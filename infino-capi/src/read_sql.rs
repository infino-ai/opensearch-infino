//! The read-only SQL the plugin's SQL route serves, held to the indices the
//! caller may read.
//!
//! A statement names OpenSearch indices, never the engine's tables. The
//! plugin asks which indices a statement reads ([`indices`]), the security
//! plugin authorizes the caller for each, and the plugin hands back, for
//! each one, a view: the query over its table that leaves out what the
//! caller may not see. [`rewrite`] puts each view where its index was named,
//! so the engine reads only through those views. An engine search over an
//! index is put in place the same way, as the search over its table read
//! through the same rules; over several tables (an index's shards, a
//! pattern's indices), a ranked search gives their best rows together, to
//! its own row count. A graph walk returns rows of its table through that
//! table's search view, and reads its edges and seeds whole, so those are
//! taken only from an index the caller may read whole. What the walk
//! cannot hold to the views is refused rather than run: anything but one
//! query, a qualified name, a table function other than the engine's
//! searches, walks and row generators, a `WITH` name that is also a
//! table's, a search naming a column the caller may not read as it is.
//!
//! A column the caller may read only as output (masked by a rule SQL cannot
//! compute, so the plugin masks it in the result) is held to that
//! ([`output_columns`]): given as a column of the result and nothing else.

use std::collections::{HashMap, HashSet};
use std::mem::take;
use std::ops::ControlFlow;

use datafusion::sql::parser::{DFParser, Statement as DFStatement};
use datafusion::sql::sqlparser::ast::{
    Expr, FunctionArg, FunctionArgExpr, FunctionArguments, GroupByExpr, Ident, ObjectName,
    ObjectNamePart, OrderByKind, Query, Select, SelectItem, SetExpr, Statement, TableAlias,
    TableFactor, Value, Visit, VisitMut, Visitor, VisitorMut,
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
/// The engine's graph walks: `(edges, table, seed_table, seed_ids, hops,
/// k)`, the rows of `table` within `hops` edges of the seed rows of
/// `seed_table`, over the edge table `edges`. Each names three indices.
const GRAPH_FUNCTIONS: [&str; 2] = ["graph_walk", "graph_rank"];
/// Where a graph walk names its edge table, the table whose rows it
/// returns, and the table its seeds are rows of.
const GRAPH_EDGES_AT: usize = 0;
const GRAPH_TABLE_AT: usize = 1;
const GRAPH_SEEDS_AT: usize = 2;
/// The engine's ranked searches and walks, each with where its row count
/// `k` is among its arguments and the order its score ranks rows in: BM25,
/// hybrid relevance and a graph rank higher first; a vector distance and a
/// walk's hop count nearer first.
const RANKED: [(&str, usize, &str); 6] = [
    ("bm25_search", 3, "DESC"),
    ("bm25_search_prefix", 3, "DESC"),
    ("vector_search", 3, "ASC"),
    ("hybrid_search", 5, "DESC"),
    ("graph_walk", 5, "ASC"),
    ("graph_rank", 5, "DESC"),
];
/// Where each engine search names the columns it matches among its
/// arguments: the column after the table, and a hybrid search's vector
/// column after its text query. The other arguments are queries, vectors
/// and counts, which a column's name may appear in as plain text.
const SEARCH_COLUMNS: [(&str, &[usize]); 6] = [
    ("bm25_search", &[1]),
    ("bm25_search_prefix", &[1]),
    ("vector_search", &[1]),
    ("token_match", &[1]),
    ("exact_match", &[1]),
    ("hybrid_search", &[1, 3]),
];
/// The column an engine search ranks its rows by.
const SCORE: &str = "score";
/// The name a ranked search over several tables is merged under.
const MERGED: &str = "infino_merged";
/// Table functions that generate rows and read no table.
const GENERATORS: [&str; 2] = ["generate_series", "range"];
/// The table function a search view reads, with the table as its one
/// argument, where the statement's own search goes: the walk gives it the
/// search's name and the rest of its arguments. The plugin writes the same
/// name (`InfinoSql.SEARCH_PLACEHOLDER`).
pub(crate) const SEARCH_PLACEHOLDER: &str = "infino_search";
/// The one function a bare `*` may be an argument of: `count(*)` counts
/// rows and reads no column.
const COUNT: &str = "count";
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

/// What the plugin gives the walk, by the index names a statement reads.
#[derive(Default)]
pub(crate) struct Reads {
    /// The view of each: the query over its table (or tables) the caller may read.
    pub(crate) views: HashMap<String, String>,
    /// The search over each: a view reading [`SEARCH_PLACEHOLDER`] in place
    /// of the search, with each table as its argument.
    pub(crate) searches: HashMap<String, String>,
    /// The columns of each a search may not name: hidden or masked for the
    /// caller, since a search matches the stored values.
    pub(crate) unsearchable: HashMap<String, HashSet<String>>,
    /// The columns the caller may read only as output (see [`output_columns`]).
    pub(crate) output_only: HashSet<String>,
    /// The one table of each index the caller may read whole (one table, no
    /// document, field or masking rule, no filtering alias): what a graph
    /// walk may take as its edges or its seeds, since it reads them whole.
    pub(crate) whole: HashMap<String, String>,
}

/// A statement as the engine runs it, and the columns of its result the
/// plugin masks, each as (result column, column it gives).
#[derive(Debug)]
pub(crate) struct Rewritten {
    pub(crate) sql: String,
    pub(crate) masked: Vec<(String, String)>,
}

/// `sql` with each index it names replaced by its view, and each search
/// over an index by its search view, from `reads`, rendered for the engine.
/// An index with no view is refused, as is a search naming a column it may
/// not, and a use of a column the caller may read only as output other than
/// as output. A `WITH` name that is one of `tables` is refused too: where
/// that name is out of the `WITH`'s scope it would read the table itself.
pub(crate) fn rewrite(
    sql: &str,
    reads: &Reads,
    tables: &[String],
) -> Result<Rewritten, Box<Refusal>> {
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
    let masked = output_columns(&query, &reads.output_only)?;
    let mut views = HashMap::with_capacity(reads.views.len());
    for (index, view) in &reads.views {
        views.insert(index.clone(), parse_query(view)?);
    }
    let mut searches = HashMap::with_capacity(reads.searches.len());
    for (index, search) in &reads.searches {
        searches.insert(index.clone(), parse_query(search)?);
    }
    let mut walk = Walk::new(
        ctes,
        Mode::Rewrite {
            views,
            searches,
            unsearchable: &reads.unsearchable,
            whole: &reads.whole,
        },
    );
    if let ControlFlow::Break(refusal) = VisitMut::visit(&mut query, &mut walk) {
        return Err(refusal);
    }
    Ok(Rewritten {
        sql: Statement::Query(Box::new(query)).to_string(),
        masked,
    })
}

/// The columns of the result that give a column the caller may read only
/// as output (`output_only`), each as (result column, column), for the
/// plugin to mask. Such a column is given as a column of the result and
/// used for nothing else: named alone (or renamed) in the outermost
/// `SELECT`, reached by its `*`, or grouped on alone, which reads only
/// which values are equal, as their masks would. Any other use is refused:
/// in a condition, a join, an order, an expression, a subquery's column
/// list, so no value of it reaches the result except as the plugin masks
/// it.
fn output_columns(
    query: &Query,
    output_only: &HashSet<String>,
) -> Result<Vec<(String, String)>, Box<Refusal>> {
    if output_only.is_empty() {
        return Ok(Vec::new());
    }
    let mut rest = query.clone();
    let mut masked = Vec::new();
    let mut names = output_only.clone();
    let outermost = match rest.body.as_mut() {
        SetExpr::Select(select) => Some(select),
        _ => None,
    };
    if let Some(select) = outermost {
        given_as_output(select, output_only, &mut masked, &mut names)?;
        // An order by position or by all of the result orders by the values.
        if !masked.is_empty() {
            if let Some(order_by) = &rest.order_by {
                let by_position = match &order_by.kind {
                    OrderByKind::All(_) => true,
                    OrderByKind::Expressions(exprs) => {
                        exprs.iter().any(|e| matches!(e.expr, Expr::Value(_)))
                    }
                };
                if by_position {
                    return Err(only_as_output("the result's columns"));
                }
            }
        }
    }
    let mut uses = Uses {
        names,
        // Below a set operation a `*` gives the result columns no one masks.
        wildcards_pass: matches!(query.body.as_ref(), SetExpr::Select(_)),
    };
    if let ControlFlow::Break(refusal) = Visit::visit(&rest, &mut uses) {
        return Err(refusal);
    }
    Ok(masked)
}

/// Take the outermost `SELECT`'s plain uses of `output_only` columns out of
/// `select`, noting each result column they give onto `masked` and each
/// name they are given onto `names`.
fn given_as_output(
    select: &mut Select,
    output_only: &HashSet<String>,
    masked: &mut Vec<(String, String)>,
    names: &mut HashSet<String>,
) -> Result<(), Box<Refusal>> {
    for item in select.projection.iter_mut() {
        match item {
            SelectItem::UnnamedExpr(expr) => {
                if let Some(column) = bare(expr, output_only) {
                    masked.push((column.clone(), column));
                    *expr = Expr::value(Value::Null);
                }
            }
            SelectItem::ExprWithAlias { expr, alias } => {
                if let Some(column) = bare(expr, output_only) {
                    let given = folded(alias);
                    names.insert(given.clone());
                    masked.push((given, column));
                    *expr = Expr::value(Value::Null);
                }
            }
            SelectItem::Wildcard(options) | SelectItem::QualifiedWildcard(_, options) => {
                if !options.to_string().is_empty() {
                    return Err(only_as_output("a `*` that renames or replaces columns"));
                }
                for column in output_only {
                    masked.push((column.clone(), column.clone()));
                }
            }
            SelectItem::ExprWithAliases { .. } => {}
        }
    }
    if let GroupByExpr::Expressions(exprs, _) = &mut select.group_by {
        exprs.retain(|e| bare(e, output_only).is_none());
    }
    Ok(())
}

/// The column of `names` `expr` is, alone; none for anything else.
fn bare(expr: &Expr, names: &HashSet<String>) -> Option<String> {
    let column = match expr {
        Expr::Identifier(ident) => folded(ident),
        Expr::CompoundIdentifier(parts) => folded(parts.last()?),
        Expr::Nested(inner) => return bare(inner, names),
        _ => return None,
    };
    names.contains(&column).then_some(column)
}

fn only_as_output(what: &str) -> Box<Refusal> {
    refuse(
        format!(
            "{what} would read a field masked for you, which SQL gives only as a column of the result: \
             filter, join, sort or compute on other fields"
        ),
        FORBIDDEN,
    )
}

/// The check for any use of `names` left once the plain ones are taken out:
/// a reference, a join on same-named columns, a column list renaming what a
/// subquery gives, a whole row handed to a function, and (where `*` cannot
/// be followed to the result) any `*`.
struct Uses {
    names: HashSet<String>,
    wildcards_pass: bool,
}

impl Visitor for Uses {
    type Break = Box<Refusal>;

    fn pre_visit_query(&mut self, query: &Query) -> ControlFlow<Self::Break> {
        if let Some(with) = &query.with {
            if with
                .cte_tables
                .iter()
                .any(|cte| !cte.alias.columns.is_empty())
            {
                return ControlFlow::Break(only_as_output("a WITH column list"));
            }
        }
        ControlFlow::Continue(())
    }

    fn pre_visit_select(&mut self, select: &Select) -> ControlFlow<Self::Break> {
        for from in &select.from {
            for join in &from.joins {
                // The join operator has a variant per kind of join; its text
                // names a join on same-named columns the same way for all.
                let text = join.to_string();
                if text.starts_with("NATURAL ") || text.contains(" USING(") {
                    return ControlFlow::Break(only_as_output("a NATURAL or USING join"));
                }
            }
        }
        if !self.wildcards_pass
            && select.projection.iter().any(|item| {
                matches!(
                    item,
                    SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(..)
                )
            })
        {
            return ControlFlow::Break(only_as_output("a `*` below a set operation"));
        }
        ControlFlow::Continue(())
    }

    fn pre_visit_table_factor(&mut self, factor: &TableFactor) -> ControlFlow<Self::Break> {
        let alias = match factor {
            TableFactor::Table { alias, .. }
            | TableFactor::Derived { alias, .. }
            | TableFactor::NestedJoin { alias, .. } => alias.as_ref(),
            _ => None,
        };
        if alias.is_some_and(|a| !a.columns.is_empty()) {
            return ControlFlow::Break(only_as_output("a column list renaming a table's columns"));
        }
        ControlFlow::Continue(())
    }

    fn pre_visit_expr(&mut self, expr: &Expr) -> ControlFlow<Self::Break> {
        if let Some(column) = bare(expr, &self.names) {
            return ControlFlow::Break(only_as_output(&format!("[{column}]")));
        }
        if let Expr::Function(f) = expr {
            if let FunctionArguments::List(list) = &f.args {
                let counts = f.name.to_string().eq_ignore_ascii_case(COUNT);
                for arg in &list.args {
                    let (FunctionArg::Unnamed(a)
                    | FunctionArg::Named { arg: a, .. }
                    | FunctionArg::ExprNamed { arg: a, .. }) = arg;
                    let whole_row = match a {
                        FunctionArgExpr::Wildcard => !counts,
                        FunctionArgExpr::QualifiedWildcard(_)
                        | FunctionArgExpr::WildcardWithOptions(_) => true,
                        FunctionArgExpr::Expr(_) => false,
                    };
                    if whole_row {
                        return ControlFlow::Break(only_as_output("a function given a whole row"));
                    }
                }
            }
        }
        ControlFlow::Continue(())
    }
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
    /// Put its view in its place, or its search view in place of a search over it.
    Rewrite {
        views: HashMap<String, Query>,
        searches: HashMap<String, Query>,
        unsearchable: &'a HashMap<String, HashSet<String>>,
        whole: &'a HashMap<String, String>,
    },
}

/// Gives a search view the statement's own search: [`SEARCH_PLACEHOLDER`]
/// takes the search's name, and around the table it names the arguments
/// the search gives before and after its table (a walk names its edges
/// before it, its seeds and the rest after).
struct Fill {
    name: ObjectName,
    before: Vec<FunctionArg>,
    after: Vec<FunctionArg>,
    filled: bool,
}

impl VisitorMut for Fill {
    type Break = ();

    fn post_visit_table_factor(&mut self, factor: &mut TableFactor) -> ControlFlow<()> {
        if let TableFactor::Table {
            name,
            args: Some(args),
            ..
        } = factor
        {
            if name.to_string() == SEARCH_PLACEHOLDER {
                *name = self.name.clone();
                let table = take(&mut args.args);
                args.args = self.before.clone();
                args.args.extend(table);
                args.args.extend(self.after.iter().cloned());
                self.filled = true;
            }
        }
        ControlFlow::Continue(())
    }
}

/// The index a table function names at argument `at`, a string as written.
fn index_arg(args: &[FunctionArg], at: usize, function: &str) -> Result<String, Box<Refusal>> {
    match args.get(at) {
        Some(arg @ FunctionArg::Unnamed(_)) => string_arg(arg).map(str::to_string),
        _ => None,
    }
    .ok_or_else(|| {
        refuse(
            format!(
                "{function} takes an index as its argument {}, a string",
                at + 1
            ),
            BAD_REQUEST,
        )
    })
}

/// An engine table named as a table function's argument.
fn table_arg(table: &str) -> FunctionArg {
    FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::value(
        Value::SingleQuotedString(table.to_string()),
    )))
}

/// A string an argument gives, as written.
fn string_arg(arg: &FunctionArg) -> Option<&str> {
    let (FunctionArg::Unnamed(a)
    | FunctionArg::Named { arg: a, .. }
    | FunctionArg::ExprNamed { arg: a, .. }) = arg;
    match a {
        FunctionArgExpr::Expr(Expr::Value(v)) => match &v.value {
            Value::SingleQuotedString(s) => Some(s),
            _ => None,
        },
        _ => None,
    }
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
    /// replaced by its search view, read under the name the statement gave
    /// it (the function's own when it gave none); a row generator reads
    /// none; any other is refused.
    fn function(&mut self, factor: &mut TableFactor) -> ControlFlow<Box<Refusal>> {
        let TableFactor::Table {
            name,
            args: Some(args),
            alias,
            ..
        } = factor
        else {
            return ControlFlow::Continue(());
        };
        let (name, args, alias) = (name.clone(), args.clone(), alias.clone());
        let function = match single(&name) {
            Ok(ident) => ident.value.to_lowercase(),
            Err(refusal) => return ControlFlow::Break(refusal),
        };
        if GENERATORS.contains(&function.as_str()) {
            return ControlFlow::Continue(());
        }
        let graph = GRAPH_FUNCTIONS.contains(&function.as_str());
        if !graph && !SEARCH_FUNCTIONS.contains(&function.as_str()) {
            return ControlFlow::Break(refuse(
                format!("the table function {function} is not served"),
                BAD_REQUEST,
            ));
        }
        // A search names the index it searches first; a walk names its edges,
        // the index whose rows it returns, and the index of its seeds.
        let (at, named): (usize, &[usize]) = if graph {
            (
                GRAPH_TABLE_AT,
                &[GRAPH_EDGES_AT, GRAPH_TABLE_AT, GRAPH_SEEDS_AT],
            )
        } else {
            (0, &[0])
        };
        let mut indices = Vec::with_capacity(named.len());
        for &i in named {
            match index_arg(&args.args, i, &function) {
                Ok(index) => indices.push(index),
                Err(refusal) => return ControlFlow::Break(refusal),
            }
        }
        let Mode::Rewrite {
            searches,
            unsearchable,
            whole,
            ..
        } = &self.mode
        else {
            for index in indices {
                self.note(index);
            }
            return ControlFlow::Continue(());
        };
        let index = indices[named.iter().position(|&i| i == at).unwrap_or(0)].clone();
        let Some(search) = searches.get(&index) else {
            return ControlFlow::Break(refuse(
                format!("no index [{index}] is readable here"),
                FORBIDDEN,
            ));
        };
        let (before, after) = if graph {
            // A walk reads its edges and its seeds whole: each is an index
            // the caller may read whole, as its one table.
            let mut tables = Vec::with_capacity(2);
            for (i, what) in [(GRAPH_EDGES_AT, "edges"), (GRAPH_SEEDS_AT, "seeds")] {
                let named_index = &indices[named.iter().position(|&n| n == i).unwrap_or(0)];
                let Some(table) = whole.get(named_index) else {
                    return ControlFlow::Break(refuse(
                        format!(
                            "{function} reads its {what} in [{named_index}] whole: name an index you may read whole, \
                             as one table, with no document, field or masking rule over it"
                        ),
                        FORBIDDEN,
                    ));
                };
                tables.push(table_arg(table));
            }
            let seeds = tables.pop();
            let mut after: Vec<FunctionArg> = seeds.into_iter().collect();
            after.extend(args.args[GRAPH_SEEDS_AT + 1..].iter().cloned());
            (tables, after)
        } else {
            // A search matches the stored values of the columns it names, so
            // it names none the caller may not read as stored.
            if let Some(closed) = unsearchable.get(&index) {
                let at: &[usize] = SEARCH_COLUMNS
                    .iter()
                    .find(|(f, _)| *f == function)
                    .map_or(&[], |(_, at)| *at);
                for arg in at.iter().filter_map(|&i| args.args.get(i)) {
                    if let Some(column) = string_arg(arg).filter(|c| closed.contains(*c)) {
                        return ControlFlow::Break(refuse(
                            format!(
                                "[{column}] is hidden or masked for you in [{index}]; {function} over it would match its values"
                            ),
                            FORBIDDEN,
                        ));
                    }
                }
            }
            (Vec::new(), args.args[1..].to_vec())
        };
        let mut view = search.clone();
        let mut fill = Fill {
            name,
            before,
            after,
            filled: false,
        };
        let _ = VisitMut::visit(&mut view, &mut fill);
        if !fill.filled {
            return ControlFlow::Break(refuse(
                format!("the search view of [{index}] reads no search"),
                BAD_REQUEST,
            ));
        }
        // A view over several tables (shards, or the indices of a pattern)
        // runs the search over each; a ranked one gives its best `k` rows
        // together, as a search over several shards does.
        if matches!(view.body.as_ref(), SetExpr::SetOperation { .. }) {
            let ranked = RANKED.iter().find(|(f, ..)| *f == function);
            if let Some((_, at, order)) = ranked {
                if let Some(k) = args.args.get(*at) {
                    view = match parse_query(&format!(
                        "SELECT * FROM ({view}) AS {MERGED} ORDER BY \"{SCORE}\" {order} LIMIT {k}"
                    )) {
                        Ok(merged) => merged,
                        Err(refusal) => return ControlFlow::Break(refusal),
                    };
                }
            }
        }
        let alias = alias.unwrap_or(TableAlias {
            explicit: true,
            name: Ident::new(function),
            columns: Vec::new(),
            at: None,
        });
        *factor = TableFactor::Derived {
            lateral: false,
            subquery: Box::new(view),
            alias: Some(alias),
            sample: None,
        };
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

#[cfg(test)]
mod tests {
    use super::*;

    fn views(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    /// Reads with these views and nothing else.
    fn reading(pairs: &[(&str, &str)]) -> Reads {
        Reads {
            views: views(pairs),
            ..Reads::default()
        }
    }

    /// `sql` rewritten under `reads`, as the engine is handed it.
    fn rewritten(sql: &str, reads: &Reads, tables: &[String]) -> Result<String, Box<Refusal>> {
        rewrite(sql, reads, tables).map(|r| r.sql)
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
            "SELECT * FROM read_parquet('/etc/passwd')",
        ] {
            assert_eq!(refusal(indices(sql)).status, BAD_REQUEST, "{sql}");
        }
    }

    #[test]
    fn each_index_is_read_through_its_view_under_the_name_it_was_given() {
        let sql = "SELECT l.msg, (SELECT count(*) FROM users) FROM logs l WHERE l.id IN (SELECT id FROM logs)";
        let out = rewritten(
            sql,
            &reading(&[
                (
                    "logs",
                    "SELECT \"id\", \"msg\" FROM \"os_logs\" WHERE \"tenant\" = 'a'",
                ),
                ("users", "SELECT * FROM \"os_users\""),
            ]),
            &[],
        )
        .unwrap();
        assert_eq!(
            out,
            "SELECT l.msg, (SELECT count(*) FROM (SELECT * FROM \"os_users\") AS users) \
             FROM (SELECT \"id\", \"msg\" FROM \"os_logs\" WHERE \"tenant\" = 'a') l \
             WHERE l.id IN (SELECT id FROM (SELECT \"id\", \"msg\" FROM \"os_logs\" WHERE \"tenant\" = 'a') AS logs)"
        );
        // What the engine is handed reads as the same single query.
        assert!(parse_query(&out).is_ok());
    }

    #[test]
    fn an_index_without_a_view_is_refused() {
        let r = refusal(rewritten(
            "SELECT * FROM logs JOIN secrets ON true",
            &reading(&[("logs", "SELECT * FROM \"os_logs\"")]),
            &[],
        ));
        assert_eq!(r.status, FORBIDDEN);
        assert!(r.message.contains("secrets"), "{}", r.message);
    }

    #[test]
    fn a_search_is_read_through_its_search_view() {
        let sql =
            "SELECT osid, score FROM bm25_search('logs', 'msg', 'disk', 10) ORDER BY score DESC";
        let reads = Reads {
            searches: views(&[(
                "logs",
                "SELECT \"osid\", \"msg\", \"score\" FROM infino_search('os_logs') WHERE \"tenant\" = 'a'",
            )]),
            ..Reads::default()
        };
        assert_eq!(
            rewritten(sql, &reads, &[]).unwrap(),
            "SELECT osid, score FROM (SELECT \"osid\", \"msg\", \"score\" FROM bm25_search('os_logs', 'msg', 'disk', 10) \
             WHERE \"tenant\" = 'a') AS bm25_search ORDER BY score DESC"
        );
        // An index with no search view is not read.
        assert_eq!(
            refusal(rewritten(sql, &Reads::default(), &[])).status,
            FORBIDDEN
        );
        assert_eq!(
            refusal(indices(
                "SELECT * FROM bm25_search(concat('lo', 'gs'), 'msg', 'x', 1)"
            ))
            .status,
            BAD_REQUEST
        );
    }

    #[test]
    fn a_ranked_search_over_several_tables_gives_their_best_rows_together() {
        let reads = Reads {
            searches: views(&[(
                "logs",
                "SELECT \"msg\", \"score\" FROM infino_search('os_logs_shard0') \
                 UNION ALL SELECT \"msg\", \"score\" FROM infino_search('os_logs_shard1')",
            )]),
            ..Reads::default()
        };
        assert_eq!(
            rewritten("SELECT msg FROM bm25_search('logs', 'msg', 'disk', 5)", &reads, &[]).unwrap(),
            "SELECT msg FROM (SELECT * FROM (SELECT \"msg\", \"score\" FROM bm25_search('os_logs_shard0', 'msg', 'disk', 5) \
             UNION ALL SELECT \"msg\", \"score\" FROM bm25_search('os_logs_shard1', 'msg', 'disk', 5)) AS infino_merged \
             ORDER BY \"score\" DESC LIMIT 5) AS bm25_search"
        );
        // A vector distance ranks the nearest first.
        assert!(rewritten(
            "SELECT msg FROM vector_search('logs', 'emb', '0.1,0.2', 3)",
            &reads,
            &[]
        )
        .unwrap()
        .contains("ORDER BY \"score\" ASC LIMIT 3"));
        // An unranked match gives every row it matches.
        assert!(!rewritten(
            "SELECT msg FROM token_match('logs', 'msg', 'disk')",
            &reads,
            &[]
        )
        .unwrap()
        .contains(MERGED));
    }

    #[test]
    fn a_walk_returns_rows_through_its_tables_search_view_over_whole_edges_and_seeds() {
        let sql = "SELECT osid, score FROM graph_walk('links', 'logs', 'logs', [7, 9], 2, 50)";
        assert_eq!(indices(sql).unwrap(), vec!["links", "logs"]);
        let reads = Reads {
            searches: views(&[(
                "logs",
                "SELECT \"osid\", \"score\" FROM infino_search('os_logs') WHERE \"tenant\" = 'a'",
            )]),
            whole: views(&[("links", "os_links"), ("logs", "os_logs")]),
            ..Reads::default()
        };
        assert_eq!(
            rewritten(sql, &reads, &[]).unwrap(),
            "SELECT osid, score FROM (SELECT \"osid\", \"score\" FROM \
             graph_walk('os_links', 'os_logs', 'os_logs', [7, 9], 2, 50) \
             WHERE \"tenant\" = 'a') AS graph_walk"
        );
        // Edges or seeds the caller may not read whole are not walked.
        let partial = Reads {
            whole: views(&[("logs", "os_logs")]),
            ..reads
        };
        let r = refusal(rewritten(sql, &partial, &[]));
        assert_eq!(r.status, FORBIDDEN);
        assert!(r.message.contains("links"), "{}", r.message);
    }

    #[test]
    fn a_search_naming_a_column_the_caller_may_not_read_is_refused() {
        let reads = Reads {
            searches: views(&[("logs", "SELECT * FROM infino_search('os_logs')")]),
            unsearchable: HashMap::from([(
                "logs".to_string(),
                HashSet::from(["secret".to_string()]),
            )]),
            ..Reads::default()
        };
        let r = refusal(rewritten(
            "SELECT osid FROM bm25_search('logs', 'secret', 'alpha', 10)",
            &reads,
            &[],
        ));
        assert_eq!(r.status, FORBIDDEN);
        assert!(r.message.contains("secret"), "{}", r.message);
        assert!(rewritten(
            "SELECT osid FROM bm25_search('logs', 'msg', 'alpha', 10)",
            &reads,
            &[]
        )
        .is_ok());
        // The column a search names, not its query text: the word is only a word.
        assert!(rewritten(
            "SELECT osid FROM token_match('logs', 'msg', 'secret')",
            &reads,
            &[]
        )
        .is_ok());
        // A hybrid search names a vector column too.
        let r = refusal(rewritten(
            "SELECT osid FROM hybrid_search('logs', 'msg', 'alpha', 'secret', '[1.0]', 10)",
            &reads,
            &[],
        ));
        assert_eq!(r.status, FORBIDDEN);
    }

    #[test]
    fn a_column_read_only_as_output_is_given_and_nothing_else() {
        let reads = Reads {
            views: views(&[("logs", "SELECT \"ip\", \"n\" FROM \"os_logs\"")]),
            output_only: HashSet::from(["ip".to_string()]),
            ..Reads::default()
        };
        let masked = |sql: &str| rewrite(sql, &reads, &[]).map(|r| r.masked);
        let pair = |a: &str, b: &str| (a.to_string(), b.to_string());
        assert_eq!(
            masked("SELECT ip, n FROM logs WHERE n > 1 ORDER BY n").unwrap(),
            [pair("ip", "ip")]
        );
        assert_eq!(
            masked("SELECT l.ip AS addr FROM logs l").unwrap(),
            [pair("addr", "ip")]
        );
        assert_eq!(masked("SELECT * FROM logs").unwrap(), [pair("ip", "ip")]);
        // Grouping reads only which values are equal, as their masks would.
        assert_eq!(
            masked("SELECT ip, count(*) AS c FROM logs GROUP BY ip").unwrap(),
            [pair("ip", "ip")]
        );
        assert_eq!(
            masked("SELECT n FROM (SELECT * FROM logs) t WHERE n > 1").unwrap(),
            []
        );
        for sql in [
            "SELECT n FROM logs WHERE ip = '10.0.0.1'",
            "SELECT n FROM logs ORDER BY ip",
            "SELECT ip AS a FROM logs ORDER BY a",
            "SELECT ip FROM logs ORDER BY 1",
            "SELECT upper(ip) FROM logs",
            "SELECT count(DISTINCT ip) FROM logs",
            "SELECT x FROM (SELECT ip AS x FROM logs) t",
            "SELECT * FROM logs a JOIN logs b ON a.ip = b.ip",
            "SELECT * FROM logs a JOIN logs b USING (ip)",
            "SELECT * FROM logs a NATURAL JOIN logs b",
            "SELECT z FROM (SELECT * FROM logs) AS t(z, m)",
            "WITH t(z, m) AS (SELECT * FROM logs) SELECT z FROM t",
            "SELECT * FROM logs UNION ALL SELECT * FROM logs",
            "SELECT * REPLACE (upper(ip) AS ip) FROM logs",
            "SELECT array_agg(logs.*) FROM logs",
        ] {
            assert_eq!(refusal(masked(sql)).status, FORBIDDEN, "{sql}");
        }
        // With nothing read only as output, all of these are served.
        assert!(rewritten(
            "SELECT n FROM logs WHERE ip = '10.0.0.1'",
            &reading(&[("logs", "SELECT * FROM \"os_logs\"")]),
            &[]
        )
        .is_ok());
    }

    #[test]
    fn a_with_name_stays_and_one_naming_a_table_is_refused() {
        let sql = "WITH logs AS (SELECT 1 AS x) SELECT * FROM logs";
        assert_eq!(refusal(indices(sql)).status, BAD_REQUEST); // it reads no index
        let sql = "WITH recent AS (SELECT * FROM logs) SELECT * FROM recent";
        let logs = reading(&[("logs", "SELECT * FROM \"os_logs\"")]);
        let out = rewritten(sql, &logs, &["os_logs".to_string()]).unwrap();
        assert!(out.ends_with("SELECT * FROM recent"), "{out}");
        // Out of its scope the name would read the table itself.
        let sql = "SELECT * FROM (WITH os_index AS (SELECT * FROM logs) SELECT * FROM os_index) t, os_index";
        let r = refusal(rewritten(sql, &logs, &["os_index".to_string()]));
        assert_eq!(r.status, BAD_REQUEST);
    }
}
