//! Makes a small table the way a program other than the plugin makes one:
//! through the engine, with its own columns and none of the plugin's.
//!
//! The plugin's REST suites run against a node that only ever sees tables
//! the plugin wrote, so an index attached to a table made elsewhere has
//! nothing to attach to there. The suites' build runs this once before the
//! node starts, over the catalog the node will open.
//!
//!   outside_table <catalog uri> <table>...

use std::process::ExitCode;
use std::sync::Arc;

use infino::arrow_schema::{DataType, Field, Schema};
use infino::serde_json::json;
use infino::{connect_with, ConnectOptions, FtsField, IndexSpec, InfinoError};

fn make(uri: &str, table: &str) -> Result<(), InfinoError> {
    let conn = connect_with(uri, ConnectOptions::new())?;
    let schema = Arc::new(Schema::new(vec![
        Field::new("title", DataType::LargeUtf8, false),
        Field::new("lang", DataType::LargeUtf8, false),
        Field::new("hits", DataType::Int64, false),
    ]));
    // The title is searched as text; the language is compared whole.
    let spec = IndexSpec::new()
        .fts(FtsField::new("title".to_string()))
        .fts(FtsField::new("lang".to_string()));
    conn.create_table(table, schema, spec)?;
    conn.open_table(table)?.append_rows(&[
        json!({"title": "Pope Leo XIV", "lang": "en", "hits": 9}),
        json!({"title": "India national team", "lang": "en", "hits": 5}),
        json!({"title": "Papa Leone XIV", "lang": "it", "hits": 3}),
    ])?;
    Ok(())
}

fn main() -> ExitCode {
    let mut args = std::env::args().skip(1);
    let (Some(uri), tables) = (args.next(), args.collect::<Vec<_>>()) else {
        eprintln!("usage: outside_table <catalog uri> <table>...");
        return ExitCode::from(2);
    };
    if tables.is_empty() {
        eprintln!("usage: outside_table <catalog uri> <table>...");
        return ExitCode::from(2);
    }
    for table in &tables {
        if let Err(e) = make(&uri, table) {
            eprintln!("outside_table: {table}: {e}");
            return ExitCode::FAILURE;
        }
    }
    ExitCode::SUCCESS
}
