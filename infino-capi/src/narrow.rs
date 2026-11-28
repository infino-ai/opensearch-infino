//! Narrowing a text lookup by the filters beside it.
//!
//! A text clause the scan cannot answer as a row predicate (a phrase, a
//! required or an excluded word) that is not the relation a statement reads
//! is written as the ids its lookup holds:
//!
//! ```sql
//! SELECT ... FROM "t"
//! WHERE osid IN (SELECT osid FROM token_match('t', 'title', '"pope leo"', 'and'))
//!   AND "time" >= '2025-05-08T15:00:00Z' AND "time" <= '2025-05-08T21:00:00Z'
//! ```
//!
//! Written so, the lookup runs over the whole table before the filters
//! beside it are applied: on a very large table it does not finish. The
//! engine skips every file a lookup's own `WHERE` rules out, so
//! [`text_subqueries`] gives each such lookup the filters of the statement
//! that holds it. A row the statement keeps passes those filters, so it is
//! in the lookup's ids exactly when it is in the narrowed ones: the answer
//! is the same, and only what is read to give it changes.

use std::ops::ControlFlow;

use datafusion::sql::sqlparser::ast::{
    BinaryOperator, Expr, FunctionArg, FunctionArgExpr, FunctionArguments, GroupByExpr, Query,
    Select, SelectItem, SetExpr, Statement, TableFactor, UnaryOperator, Visit, Visitor,
};

use crate::attached::{parse, searched, SCORE};

/// The engine's lookup of the rows a text query matches.
const LOOKUP: &str = "token_match";
/// The function that gives its first argument that is not null.
const COALESCE: &str = "coalesce";

/// The table a `FROM` item reads when it is that table alone: named, or
/// searched by one of the engine's table functions. None for a join, a
/// subquery or a qualified name.
fn table_of(factor: &TableFactor) -> Option<(String, Option<String>)> {
    let TableFactor::Table { name, args, .. } = factor else {
        return None;
    };
    let function = name.0.first()?.as_ident()?;
    if name.0.len() != 1 {
        return None;
    }
    match args {
        None => Some((function.value.clone(), None)),
        Some(args) => {
            Some((searched(&args.args)?.to_string(), Some(function.value.to_ascii_lowercase())))
        }
    }
}

/// The table one `SELECT` reads, when it reads one alone.
fn read_by(select: &Select) -> Option<(String, Option<String>)> {
    match select.from.as_slice() {
        [only] if only.joins.is_empty() => table_of(&only.relation),
        _ => None,
    }
}

/// Finds what keeps a condition from being copied into a lookup: a query of
/// its own, a qualified column (the lookup gives the table's columns under
/// no name), or the score only a ranked search gives. Also whether it names
/// any column at all.
#[derive(Default)]
struct Plain {
    names_column: bool,
}

impl Visitor for Plain {
    type Break = ();

    fn pre_visit_query(&mut self, _: &Query) -> ControlFlow<()> {
        ControlFlow::Break(())
    }

    fn pre_visit_expr(&mut self, expr: &Expr) -> ControlFlow<()> {
        match expr {
            Expr::CompoundIdentifier(_) | Expr::Subquery(_) | Expr::Exists { .. } => {
                ControlFlow::Break(())
            }
            Expr::Identifier(ident) if ident.value.eq_ignore_ascii_case(SCORE) => {
                ControlFlow::Break(())
            }
            Expr::Identifier(_) => {
                self.names_column = true;
                ControlFlow::Continue(())
            }
            _ => ControlFlow::Continue(()),
        }
    }
}

/// Whether `condition` holds of a row by that row's own columns alone, so
/// it reads the same inside a lookup of the same table.
fn is_filter(condition: &Expr) -> bool {
    let mut plain = Plain::default();
    condition.visit(&mut plain).is_continue() && plain.names_column
}

/// The conditions `selection` joins by `AND`, each without the parentheses
/// around it.
fn conjuncts<'a>(selection: &'a Expr, out: &mut Vec<&'a Expr>) {
    match selection {
        Expr::Nested(inner) => conjuncts(inner, out),
        Expr::BinaryOp {
            left,
            op: BinaryOperator::And,
            right,
        } => {
            conjuncts(left, out);
            conjuncts(right, out);
        }
        other => out.push(other),
    }
}

/// `filters` joined by `AND`, each in parentheses.
fn all_of(filters: &[Expr]) -> Option<Expr> {
    filters
        .iter()
        .map(|f| Expr::Nested(Box::new(f.clone())))
        .reduce(|left, right| Expr::BinaryOp {
            left: Box::new(left),
            op: BinaryOperator::And,
            right: Box::new(right),
        })
}

/// Whether `subquery` is a lookup of `table`'s ids under `id` with no
/// condition of its own: `SELECT id FROM token_match('table', ...)`.
fn is_bare_lookup(subquery: &Query, table: &str, id: &str) -> bool {
    if subquery.with.is_some() || subquery.order_by.is_some() || subquery.limit_clause.is_some() {
        return false;
    }
    let SetExpr::Select(select) = subquery.body.as_ref() else {
        return false;
    };
    let ids = matches!(
        select.projection.as_slice(),
        [SelectItem::UnnamedExpr(Expr::Identifier(i))] if i.value == id
    );
    let ungrouped = matches!(&select.group_by, GroupByExpr::Expressions(by, _) if by.is_empty());
    ids && ungrouped
        && select.distinct.is_none()
        && select.selection.is_none()
        && select.having.is_none()
        && read_by(select) == Some((table.to_string(), Some(LOOKUP.to_string())))
}

/// Give `filters` to each bare lookup of `table` that `condition` tests a
/// row's id against, wherever it stands under `AND`, `OR` and `NOT`. A
/// lookup inside a query of its own is left: its rows are not this
/// statement's. Whether any was narrowed.
fn narrow(condition: &mut Expr, table: &str, filters: &[Expr]) -> bool {
    match condition {
        Expr::Nested(inner) => narrow(inner, table, filters),
        Expr::UnaryOp {
            op: UnaryOperator::Not,
            expr,
        } => narrow(expr, table, filters),
        Expr::BinaryOp {
            left,
            op: BinaryOperator::And | BinaryOperator::Or,
            right,
        } => {
            // Both sides, whether or not the first held one.
            let first = narrow(left, table, filters);
            narrow(right, table, filters) || first
        }
        // A condition read as false where it is unknown, as an excluded
        // clause is written: `NOT COALESCE((...), FALSE)`.
        Expr::Function(function) if function.name.to_string().eq_ignore_ascii_case(COALESCE) => {
            let FunctionArguments::List(list) = &mut function.args else {
                return false;
            };
            let mut any = false;
            for arg in &mut list.args {
                if let FunctionArg::Unnamed(FunctionArgExpr::Expr(inner)) = arg {
                    any |= narrow(inner, table, filters);
                }
            }
            any
        }
        Expr::InSubquery { expr, subquery, .. } => {
            let Expr::Identifier(id) = expr.as_ref() else {
                return false;
            };
            if !is_bare_lookup(subquery, table, &id.value) {
                return false;
            }
            let SetExpr::Select(select) = subquery.body.as_mut() else {
                return false;
            };
            select.selection = all_of(filters);
            select.selection.is_some()
        }
        _ => false,
    }
}

/// Narrow the lookups of one `SELECT` by its own filters, or those of the
/// query it reads from when it reads one alone. Whether any was.
fn narrow_select(select: &mut Select) -> bool {
    if let [only] = select.from.as_mut_slice() {
        if let TableFactor::Derived { subquery, .. } = &mut only.relation {
            return only.joins.is_empty() && narrow_body(&mut subquery.body);
        }
    }
    let Some((table, _)) = read_by(select) else {
        return false;
    };
    let Some(selection) = select.selection.as_mut() else {
        return false;
    };
    let filters: Vec<Expr> = {
        let mut all = Vec::new();
        conjuncts(selection, &mut all);
        all.into_iter().filter(|c| is_filter(c)).cloned().collect()
    };
    !filters.is_empty() && narrow(selection, &table, &filters)
}

/// Narrow the lookups of each `SELECT` a query body is made of.
fn narrow_body(body: &mut SetExpr) -> bool {
    match body {
        SetExpr::Select(select) => narrow_select(select),
        SetExpr::Query(query) => narrow_body(&mut query.body),
        SetExpr::SetOperation { left, right, .. } => {
            let first = narrow_body(left);
            narrow_body(right) || first
        }
        _ => false,
    }
}

/// `sql` with each text lookup it tests a row's id against narrowed by the
/// filters of the statement that holds it (see the module's note). None
/// when it holds no such lookup, has no filter to give one, or is not one
/// query: it then runs as written.
pub(crate) fn text_subqueries(sql: &str) -> Option<String> {
    // Most statements hold no lookup as a subquery: those are not parsed.
    if !sql.contains(LOOKUP) || !sql.contains("IN (SELECT") {
        return None;
    }
    let mut query = parse(sql)?;
    if !narrow_body(&mut query.body) {
        return None;
    }
    Some(Statement::Query(Box::new(query)).to_string())
}

#[cfg(test)]
mod tests {
    use super::text_subqueries;

    const RANGE: &str = r#""time" >= '2025-05-08T15:00:00Z' AND "time" <= '2025-05-08T21:00:00Z'"#;
    const PHRASE: &str = r#"osid IN (SELECT osid FROM token_match('t', 'title', '"pope leo"', 'and'))"#;

    #[test]
    fn a_lookup_beside_a_range_is_given_the_range() {
        let sql = format!(r#"SELECT osid FROM "t" WHERE (1=1 AND {PHRASE} AND ({RANGE}))"#);
        let narrowed = text_subqueries(&sql).unwrap();
        assert!(
            narrowed.contains(
                r#"token_match('t', 'title', '"pope leo"', 'and') WHERE ("time" >= '2025-05-08T15:00:00Z') AND ("time" <= '2025-05-08T21:00:00Z'))"#
            ),
            "{narrowed}"
        );
        // The statement's own filters stay where they were.
        assert!(narrowed.ends_with(&format!("AND ({RANGE}))")), "{narrowed}");
    }

    #[test]
    fn a_lookup_beside_a_search_of_the_same_table_is_narrowed() {
        let sql = format!(
            r#"SELECT osid, score FROM bm25_search('t', 'title', 'conclave', 3, 'or') WHERE {PHRASE} AND "lang" = 'en' ORDER BY score DESC"#
        );
        let narrowed = text_subqueries(&sql).unwrap();
        assert!(narrowed.contains(r#"'and') WHERE ("lang" = 'en'))"#), "{narrowed}");
    }

    #[test]
    fn a_lookup_under_not_or_or_is_narrowed_by_the_filters_beside_it() {
        let sql = format!(r#"SELECT osid FROM "t" WHERE NOT ({PHRASE}) AND "hits" > 4"#);
        assert!(text_subqueries(&sql).unwrap().contains(r#"'and') WHERE ("hits" > 4))"#));
        // An excluded clause, as the plugin writes one.
        let sql = format!(
            r#"SELECT osid FROM "t" WHERE NOT COALESCE(({PHRASE}), FALSE) AND "hits" > 4"#
        );
        assert!(text_subqueries(&sql).unwrap().contains(r#"'and') WHERE ("hits" > 4))"#));
        let sql = format!(r#"SELECT osid FROM "t" WHERE ({PHRASE} OR "lang" = 'de') AND "hits" > 4"#);
        let narrowed = text_subqueries(&sql).unwrap();
        assert!(narrowed.contains(r#"'and') WHERE ("hits" > 4))"#), "{narrowed}");
    }

    #[test]
    fn each_of_two_lookups_is_narrowed() {
        let other = r#"osid IN (SELECT osid FROM token_match('t', 'title', '"leo xiv"', 'and'))"#;
        let sql = format!(r#"SELECT osid FROM "t" WHERE {PHRASE} AND {other} AND "hits" > 4"#);
        let narrowed = text_subqueries(&sql).unwrap();
        assert_eq!(narrowed.matches(r#"'and') WHERE ("hits" > 4))"#).count(), 2, "{narrowed}");
    }

    #[test]
    fn a_statement_with_nothing_to_narrow_runs_as_written() {
        for sql in [
            // No lookup as a subquery.
            format!(r#"SELECT osid FROM token_match('t', 'title', 'x', 'or') WHERE {RANGE}"#),
            // No filter beside the lookup.
            format!(r#"SELECT osid FROM "t" WHERE 1=1 AND {PHRASE}"#),
            // The filters are alternatives to the lookup, not beside it.
            format!(r#"SELECT osid FROM "t" WHERE {PHRASE} OR "hits" > 4"#),
            // A lookup that already has a condition.
            r#"SELECT osid FROM "t" WHERE osid IN (SELECT osid FROM token_match('t', 'title', 'x', 'or') WHERE "hits" > 1) AND "hits" > 4"#.to_string(),
            // A lookup of another table's rows.
            r#"SELECT osid FROM "t" WHERE osid IN (SELECT osid FROM token_match('u', 'title', 'x', 'or')) AND "hits" > 4"#.to_string(),
            // A join: a column may be either table's.
            format!(r#"SELECT a.osid FROM "t" a JOIN "u" b ON a.k = b.k WHERE {PHRASE} AND "hits" > 4"#),
        ] {
            assert_eq!(text_subqueries(&sql), None, "{sql}");
        }
    }

    #[test]
    fn a_condition_the_lookup_cannot_hold_is_not_copied() {
        // The score of a ranked search, a qualified column and a query of
        // its own stay outside; the plain filter beside them goes in alone.
        let sql = format!(
            r#"SELECT osid FROM bm25_search('t', 'title', 'x', 3, 'or') AS s WHERE {PHRASE} AND score > 1 AND s."lang" = 'en' AND "k" IN (SELECT k FROM "t") AND "hits" > 4"#
        );
        let narrowed = text_subqueries(&sql).unwrap();
        assert!(narrowed.contains(r#"'and') WHERE ("hits" > 4))"#), "{narrowed}");
    }

    #[test]
    fn a_lookup_in_each_part_of_a_union_takes_that_parts_filters() {
        let sql = format!(
            r#"SELECT osid FROM "t" WHERE {PHRASE} AND "hits" > 4 UNION ALL SELECT osid FROM "t" WHERE {PHRASE} AND "hits" < 2"#
        );
        let narrowed = text_subqueries(&sql).unwrap();
        assert!(narrowed.contains(r#"'and') WHERE ("hits" > 4))"#), "{narrowed}");
        assert!(narrowed.contains(r#"'and') WHERE ("hits" < 2))"#), "{narrowed}");
    }
}
