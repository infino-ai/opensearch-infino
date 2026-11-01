//! The read-only SQL the plugin's SQL route serves, held to the indices the
//! caller may read.
//!
//! A statement names OpenSearch indices, never the engine's tables. The
//! plugin asks which indices a statement reads ([`indices`]), the security
//! plugin authorizes the caller for each, and the plugin hands back, for
//! each one, a view: the query over its table that leaves out what the
//! caller may not see. [`rewrite`] puts each view where its index was named,
//! so the engine reads only through those views. What the walk cannot hold
//! to the views is refused rather than run: anything but one query, a
//! qualified name, a table function other than the engine's searches and
//! row generators, a `WITH` name that is also a table's.

use std::collections::{HashMap, HashSet};
use std::ops::ControlFlow;

use datafusion::sql::parser::{DFParser, Statement as DFStatement};
use datafusion::sql::sqlparser::ast::{
    Expr, FunctionArg, FunctionArgExpr, Ident, LimitClause, ObjectName, ObjectNamePart, Query,
    SetExpr, Statement, TableAlias, TableFactor, Value, Visit, VisitMut, Visitor, VisitorMut,
};

/// The engine's search functions, each naming the table it searches as its
/// first argument, a string literal.
const SEARCH_FUNCTIONS: [&str; 6] = [
    "bm25_search",
    "bm25_search_prefix",
    "vector_search",
    "token_match",
    "exact_match",
    "hybrid_search",
];
/// Table functions that generate rows and read no table.
const GENERATORS: [&str; 2] = ["generate_series", "range"];
/// The status of a statement the route does not serve as written.
pub(crate) const BAD_REQUEST: u16 = 400;
/// The status of a statement that would read around a security rule.
pub(crate) const FORBIDDEN: u16 = 403;

/// Why a statement is not run, and the HTTP status that says so.
#[derive(Debug)]
pub(crate) struct Refusal {
    pub(crate) message: String,
    pub(crate) status: u16,
}

fn refuse(message: impl Into<String>, status: u16) -> Box<Refusal> {
    Box::new(Refusal {
        message: message.into(),
        status,
    })
}

/// The indices `sql` reads, each once, in the order it names them: every
/// table it names that is not one of its own `WITH` names, and the table
/// each engine search function in it searches.
pub(crate) fn indices(sql: &str) -> Result<Vec<String>, Box<Refusal>> {
    let mut query = parse_query(sql)?;
    let mut walk = Walk::new(ctes(&query), Mode::Collect);
    if let ControlFlow::Break(refusal) = VisitMut::visit(&mut query, &mut walk) {
        return Err(refusal);
    }
    if walk.found.is_empty() {
        return Err(refuse("the statement reads no index", BAD_REQUEST));
    }
    Ok(walk.found)
}

/// `sql` with each index it names replaced by its view from `views` (the
/// query over the index's table the caller may read), and each search
/// function's index by its table from `functions`, rendered for the engine.
/// An index with no view, or a search over one with no table (the plugin
/// gives none where a rule applies, since a search reads the table whole),
/// is refused. A `WITH` name that is one of `tables` is refused too: where
/// that name is out of the `WITH`'s scope it would read the table itself.
/// The statement is held to `max_rows + 1` rows ([`limit_rows`]), so the
/// caller can tell a result that was cut.
pub(crate) fn rewrite(
    sql: &str,
    views: &HashMap<String, String>,
    functions: &HashMap<String, String>,
    tables: &[String],
    max_rows: usize,
) -> Result<String, Box<Refusal>> {
    let mut query = parse_query(sql)?;
    let ctes = ctes(&query);
    for name in &ctes {
        if tables.iter().any(|t| t.eq_ignore_ascii_case(name)) {
            return Err(refuse(
                format!("the WITH name [{name}] is the name of a table; name it otherwise"),
                BAD_REQUEST,
            ));
        }
    }
    let mut parsed = HashMap::with_capacity(views.len());
    for (index, view) in views {
        parsed.insert(index.clone(), parse_query(view)?);
    }
    let mut walk = Walk::new(
        ctes,
        Mode::Rewrite {
            views: parsed,
            functions,
        },
    );
    if let ControlFlow::Break(refusal) = VisitMut::visit(&mut query, &mut walk) {
        return Err(refusal);
    }
    limit_rows(&mut query, max_rows)?;
    Ok(Statement::Query(Box::new(query)).to_string())
}

/// The one query `sql` holds; anything else is refused.
fn parse_query(sql: &str) -> Result<Query, Box<Refusal>> {
    let mut statements =
        DFParser::parse_sql(sql).map_err(|e| refuse(e.to_string(), BAD_REQUEST))?;
    if statements.len() != 1 {
        return Err(refuse("send exactly one statement", BAD_REQUEST));
    }
    match statements.pop_front() {
        Some(DFStatement::Statement(statement)) => match *statement {
            Statement::Query(query) => Ok(*query),
            _ => Err(refuse("only a query is served", BAD_REQUEST)),
        },
        _ => Err(refuse("only a query is served", BAD_REQUEST)),
    }
}

/// Every `WITH` name the query declares, at any depth, as the engine folds
/// it: a name in quotes as written, any other in lower case.
fn ctes(query: &Query) -> HashSet<String> {
    struct Names(HashSet<String>);
    impl Visitor for Names {
        type Break = ();
        fn pre_visit_query(&mut self, query: &Query) -> ControlFlow<()> {
            if let Some(with) = &query.with {
                for cte in &with.cte_tables {
                    self.0.insert(folded(&cte.alias.name));
                }
            }
            ControlFlow::Continue(())
        }
    }
    let mut names = Names(HashSet::new());
    let _ = Visit::visit(query, &mut names);
    names.0
}

/// A name as the engine's SQL layer folds it: in quotes, as written; else
/// in lower case.
fn folded(ident: &Ident) -> String {
    if ident.quote_style.is_some() {
        ident.value.clone()
    } else {
        ident.value.to_lowercase()
    }
}

/// The one identifier of a name, or a refusal for a qualified one, which
/// would reach past the indices into the engine's catalog.
fn single(name: &ObjectName) -> Result<&Ident, Box<Refusal>> {
    match name.0.as_slice() {
        [ObjectNamePart::Identifier(ident)] => Ok(ident),
        _ => Err(refuse(
            format!(
                "[{name}] is not an index name: name an index alone, without a schema or catalog"
            ),
            BAD_REQUEST,
        )),
    }
}

/// What the walk does with each index it meets.
enum Mode<'a> {
    /// Note it.
    Collect,
    /// Put its view in its place.
    Rewrite {
        views: HashMap<String, Query>,
        functions: &'a HashMap<String, String>,
    },
}

/// The walk over a query and every query inside it.
struct Walk<'a> {
    ctes: HashSet<String>,
    mode: Mode<'a>,
    found: Vec<String>,
}

impl<'a> Walk<'a> {
    fn new(ctes: HashSet<String>, mode: Mode<'a>) -> Self {
        Self {
            ctes,
            mode,
            found: Vec::new(),
        }
    }

    fn note(&mut self, index: String) {
        if !self.found.contains(&index) {
            self.found.push(index);
        }
    }

    /// A named table: a `WITH` name stays as it is; an index is noted, or
    /// replaced by its view under the name it was given.
    fn table(&mut self, factor: &mut TableFactor) -> ControlFlow<Box<Refusal>> {
        let TableFactor::Table {
            name,
            alias,
            args: None,
            with_hints,
            version,
            partitions,
            json_path,
            sample,
            index_hints,
            ..
        } = factor
        else {
            return ControlFlow::Continue(());
        };
        let ident = match single(name) {
            Ok(ident) => ident.clone(),
            Err(refusal) => return ControlFlow::Break(refusal),
        };
        let index = folded(&ident);
        if self.ctes.contains(&index) {
            return ControlFlow::Continue(());
        }
        if !with_hints.is_empty()
            || version.is_some()
            || !partitions.is_empty()
            || json_path.is_some()
            || sample.is_some()
            || !index_hints.is_empty()
        {
            return ControlFlow::Break(refuse(
                format!("[{index}] carries a table modifier the route does not serve"),
                BAD_REQUEST,
            ));
        }
        let Mode::Rewrite { views, .. } = &self.mode else {
            self.note(index);
            return ControlFlow::Continue(());
        };
        let Some(view) = views.get(&index) else {
            return ControlFlow::Break(refuse(
                format!("no index [{index}] is readable here"),
                FORBIDDEN,
            ));
        };
        // The view takes the name the statement gave the index, so a column
        // it qualifies by that name still resolves.
        let alias = alias.clone().unwrap_or(TableAlias {
            explicit: true,
            name: ident,
            columns: Vec::new(),
            at: None,
        });
        *factor = TableFactor::Derived {
            lateral: false,
            subquery: Box::new(view.clone()),
            alias: Some(alias),
            sample: None,
        };
        ControlFlow::Continue(())
    }

    /// A table function: an engine search names its index, noted or
    /// replaced by its table; a row generator reads none; any other is
    /// refused.
    fn function(&mut self, factor: &mut TableFactor) -> ControlFlow<Box<Refusal>> {
        let TableFactor::Table {
            name,
            args: Some(args),
            ..
        } = factor
        else {
            return ControlFlow::Continue(());
        };
        let function = match single(name) {
            Ok(ident) => ident.value.to_lowercase(),
            Err(refusal) => return ControlFlow::Break(refusal),
        };
        if GENERATORS.contains(&function.as_str()) {
            return ControlFlow::Continue(());
        }
        if !SEARCH_FUNCTIONS.contains(&function.as_str()) {
            return ControlFlow::Break(refuse(
                format!("the table function {function} is not served"),
                BAD_REQUEST,
            ));
        }
        let Some(FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Value(first)))) =
            args.args.first_mut()
        else {
            return ControlFlow::Break(refuse(
                format!("{function} takes the index it searches as its first argument, a string"),
                BAD_REQUEST,
            ));
        };
        let Value::SingleQuotedString(index) = &first.value else {
            return ControlFlow::Break(refuse(
                format!("{function} takes the index it searches as its first argument, a string"),
                BAD_REQUEST,
            ));
        };
        let index = index.clone();
        match &self.mode {
            Mode::Collect => self.note(index),
            Mode::Rewrite { functions, .. } => match functions.get(&index) {
                Some(table) => first.value = Value::SingleQuotedString(table.clone()),
                None => {
                    return ControlFlow::Break(refuse(
                        format!(
                            "{function} over [{index}] is not served: it searches one table whole, so it reads \
                             only an index that is one table and under no rule for you; read [{index}] by name instead"
                        ),
                        FORBIDDEN,
                    ))
                }
            },
        }
        ControlFlow::Continue(())
    }
}

impl VisitorMut for Walk<'_> {
    type Break = Box<Refusal>;

    /// The walk starts at the query, so any statement it meets is one
    /// inside it (an insert, update, delete or merge in its body).
    fn pre_visit_statement(&mut self, _statement: &mut Statement) -> ControlFlow<Self::Break> {
        ControlFlow::Break(refuse("only a query is served", BAD_REQUEST))
    }

    fn pre_visit_query(&mut self, query: &mut Query) -> ControlFlow<Self::Break> {
        if !query.pipe_operators.is_empty() {
            return ControlFlow::Break(refuse("pipe syntax is not served", BAD_REQUEST));
        }
        check_body(&query.body)
    }

    // The view goes in after the walk has left the factor, so the walk never
    // descends into a view the plugin wrote.
    fn post_visit_table_factor(&mut self, factor: &mut TableFactor) -> ControlFlow<Self::Break> {
        match factor {
            TableFactor::Table { args: None, .. } => self.table(factor),
            TableFactor::Table { args: Some(_), .. } => self.function(factor),
            TableFactor::Derived { .. }
            | TableFactor::NestedJoin { .. }
            | TableFactor::UNNEST { .. } => ControlFlow::Continue(()),
            _ => ControlFlow::Break(refuse(
                "this kind of table expression is not served",
                BAD_REQUEST,
            )),
        }
    }
}

/// A query body reads rows only: a `SELECT` that writes no table, a set
/// operation of such, or `VALUES`.
fn check_body(body: &SetExpr) -> ControlFlow<Box<Refusal>> {
    match body {
        SetExpr::Select(select) if select.into.is_none() => ControlFlow::Continue(()),
        SetExpr::Query(_) | SetExpr::Values(_) => ControlFlow::Continue(()),
        SetExpr::SetOperation { left, right, .. } => {
            check_body(left)?;
            check_body(right)
        }
        _ => ControlFlow::Break(refuse("only a query is served", BAD_REQUEST)),
    }
}

/// Hold the query to `max_rows + 1` rows: a query that names no row count,
/// or a larger one, is given that, so the engine stops there and a result
/// that would be cut shows it by its one row too many. A count that is not
/// a number as written stays, and the result is held to the cap after.
fn limit_rows(query: &mut Query, max_rows: usize) -> Result<(), Box<Refusal>> {
    let cap = max_rows + 1;
    let within = |limit: &Expr| match limit {
        Expr::Value(v) => v
            .to_string()
            .parse::<u128>()
            .map_or(true, |n| n <= cap as u128),
        _ => true,
    };
    let keep = match &query.limit_clause {
        None => false,
        Some(LimitClause::LimitOffset { limit, .. }) => limit.as_ref().is_some_and(within),
        Some(LimitClause::OffsetCommaLimit { limit, .. }) => within(limit),
    };
    if keep {
        return Ok(());
    }
    let template = parse_query(&format!("SELECT 1 LIMIT {cap}"))?;
    let Some(LimitClause::LimitOffset {
        limit: Some(count), ..
    }) = template.limit_clause
    else {
        return Err(refuse("the row limit could not be written", BAD_REQUEST));
    };
    match &mut query.limit_clause {
        Some(LimitClause::LimitOffset { limit, .. }) => *limit = Some(count),
        Some(LimitClause::OffsetCommaLimit { limit, .. }) => *limit = count,
        clause => {
            *clause = Some(LimitClause::LimitOffset {
                limit: Some(count),
                offset: None,
                limit_by: Vec::new(),
            })
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The largest result the tests let a statement return.
    const MAX_ROWS: usize = 10;

    fn views(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    fn refusal(r: Result<impl std::fmt::Debug, Box<Refusal>>) -> Box<Refusal> {
        r.expect_err("the statement is refused")
    }

    #[test]
    fn the_indices_a_statement_reads_are_its_tables_and_its_searches() {
        let sql = "WITH recent AS (SELECT * FROM logs WHERE ts > 0) \
                   SELECT r.msg FROM recent r JOIN \"Users\" u ON r.uid = u.id \
                   WHERE r.id IN (SELECT osid FROM token_match('logs', 'msg', 'disk')) \
                   AND EXISTS (SELECT 1 FROM Audit)";
        assert_eq!(indices(sql).unwrap(), ["logs", "Users", "audit"]);
    }

    #[test]
    fn a_statement_that_reads_no_index_is_refused() {
        assert_eq!(refusal(indices("SELECT 1")).status, BAD_REQUEST);
        assert_eq!(
            refusal(indices("SELECT * FROM generate_series(1, 3)")).status,
            BAD_REQUEST
        );
    }

    #[test]
    fn anything_but_one_query_is_refused() {
        for sql in [
            "DROP TABLE logs",
            "INSERT INTO logs VALUES (1)",
            "CREATE TABLE x AS SELECT * FROM logs",
            "COPY logs TO '/tmp/out.parquet'",
            "CREATE EXTERNAL TABLE x STORED AS PARQUET LOCATION '/tmp/x'",
            "EXPLAIN SELECT * FROM logs",
            "SET datafusion.execution.batch_size = 1",
            "SELECT * FROM logs; SELECT * FROM users",
            "SELECT * INTO copied FROM logs",
            "TABLE logs",
        ] {
            assert_eq!(refusal(indices(sql)).status, BAD_REQUEST, "{sql}");
        }
    }

    #[test]
    fn a_qualified_name_or_another_table_function_is_refused() {
        for sql in [
            "SELECT * FROM public.os_index",
            "SELECT * FROM datafusion.public.os_logs",
            "SELECT * FROM information_schema.tables",
            "SELECT * FROM graph_walk('edges', 'logs', '1', 2, 10)",
            "SELECT * FROM read_parquet('/etc/passwd')",
        ] {
            assert_eq!(refusal(indices(sql)).status, BAD_REQUEST, "{sql}");
        }
    }

    #[test]
    fn each_index_is_read_through_its_view_under_the_name_it_was_given() {
        let sql = "SELECT l.msg, (SELECT count(*) FROM users) FROM logs l WHERE l.id IN (SELECT id FROM logs)";
        let out = rewrite(
            sql,
            &views(&[
                (
                    "logs",
                    "SELECT \"id\", \"msg\" FROM \"os_logs\" WHERE \"tenant\" = 'a'",
                ),
                ("users", "SELECT * FROM \"os_users\""),
            ]),
            &HashMap::new(),
            &[],
            MAX_ROWS,
        )
        .unwrap();
        assert_eq!(
            out,
            "SELECT l.msg, (SELECT count(*) FROM (SELECT * FROM \"os_users\") AS users) \
             FROM (SELECT \"id\", \"msg\" FROM \"os_logs\" WHERE \"tenant\" = 'a') l \
             WHERE l.id IN (SELECT id FROM (SELECT \"id\", \"msg\" FROM \"os_logs\" WHERE \"tenant\" = 'a') AS logs) \
             LIMIT 11"
        );
        // What the engine is handed reads as the same single query.
        assert!(parse_query(&out).is_ok());
    }

    #[test]
    fn an_index_without_a_view_is_refused() {
        let r = refusal(rewrite(
            "SELECT * FROM logs JOIN secrets ON true",
            &views(&[("logs", "SELECT * FROM \"os_logs\"")]),
            &HashMap::new(),
            &[],
            MAX_ROWS,
        ));
        assert_eq!(r.status, FORBIDDEN);
        assert!(r.message.contains("secrets"), "{}", r.message);
    }

    #[test]
    fn a_search_reads_its_table_only_where_no_rule_applies() {
        let sql = "SELECT osid FROM bm25_search('logs', 'msg', 'disk', 10)";
        let tables = HashMap::from([("logs".to_string(), "os_logs".to_string())]);
        let out = rewrite(sql, &HashMap::new(), &tables, &[], MAX_ROWS).unwrap();
        assert_eq!(
            out,
            "SELECT osid FROM bm25_search('os_logs', 'msg', 'disk', 10) LIMIT 11"
        );
        let r = refusal(rewrite(
            sql,
            &HashMap::new(),
            &HashMap::new(),
            &[],
            MAX_ROWS,
        ));
        assert_eq!(r.status, FORBIDDEN);
        assert_eq!(
            refusal(indices(
                "SELECT * FROM bm25_search(concat('lo', 'gs'), 'msg', 'x', 1)"
            ))
            .status,
            BAD_REQUEST
        );
    }

    #[test]
    fn a_with_name_stays_and_one_naming_a_table_is_refused() {
        let sql = "WITH logs AS (SELECT 1 AS x) SELECT * FROM logs";
        assert_eq!(refusal(indices(sql)).status, BAD_REQUEST); // it reads no index
        let sql = "WITH recent AS (SELECT * FROM logs) SELECT * FROM recent";
        let logs = views(&[("logs", "SELECT * FROM \"os_logs\"")]);
        let out = rewrite(
            sql,
            &logs,
            &HashMap::new(),
            &["os_logs".to_string()],
            MAX_ROWS,
        )
        .unwrap();
        assert!(out.ends_with("SELECT * FROM recent LIMIT 11"), "{out}");
        // Out of its scope the name would read the table itself.
        let sql = "SELECT * FROM (WITH os_index AS (SELECT * FROM logs) SELECT * FROM os_index) t, os_index";
        let r = refusal(rewrite(
            sql,
            &logs,
            &HashMap::new(),
            &["os_index".to_string()],
            MAX_ROWS,
        ));
        assert_eq!(r.status, BAD_REQUEST);
    }

    #[test]
    fn a_limit_within_the_cap_stands_and_a_larger_one_is_held_to_it() {
        let logs = views(&[("logs", "SELECT * FROM \"os_logs\"")]);
        let out = rewrite(
            "SELECT * FROM logs LIMIT 3",
            &logs,
            &HashMap::new(),
            &[],
            MAX_ROWS,
        )
        .unwrap();
        assert!(out.ends_with("LIMIT 3"), "{out}");
        let out = rewrite(
            "SELECT * FROM logs OFFSET 5",
            &logs,
            &HashMap::new(),
            &[],
            MAX_ROWS,
        )
        .unwrap();
        assert!(out.contains("LIMIT 11"), "{out}");
        // The engine stops at the cap rather than building a larger result first.
        let out = rewrite(
            "SELECT * FROM logs LIMIT 1000000",
            &logs,
            &HashMap::new(),
            &[],
            MAX_ROWS,
        )
        .unwrap();
        assert!(out.ends_with("LIMIT 11"), "{out}");
    }
}
