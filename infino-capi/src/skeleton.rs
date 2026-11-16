// SPDX-License-Identifier: Apache-2.0
//! The document parser, and a document's shape: what the engine's document
//! column rebuilds `source` from (see `infino::document`).
//!
//! [`parse`] reads the document's JSON text once. It builds the value tree
//! `flatten` reads (what `serde_json` would give) and, in the same walk, the
//! document's skeleton: its compact text with each scalar replaced by a
//! numbered placeholder, and for each scalar its path (as `flatten` paths
//! it: object keys joined with `.`, array entries at their array's path)
//! and its raw text. No second pass over the text.
//!
//! [`shape`] then fills the placeholders against the row the bridge stored:
//! a path holding one value is in the column of that name, a path holding
//! several in its list column (`path[]`, or the column itself when the
//! engine typed it a list), the k-th value of the path being the k-th
//! element. A value becomes a slot naming that column when the stored value
//! is exactly what the document wrote: always for an unescaped string in a
//! string column, an integer in an integer column and a boolean in a
//! boolean column, which are stored as written; for any other pairing when
//! the engine renders the stored value as the raw text, byte for byte.
//! Otherwise the value goes to the residual as written.

use std::collections::HashMap;
use std::ops::Range;

use infino::arrow_array::{Array, RecordBatch};
use infino::arrow_schema::DataType;
use infino::document::{render_value, residual, Slot};
use infino::serde_json::{self, Map, Number, Value};
use infino::{FieldId, TableSchema};

use crate::LIST_SUFFIX;

/// A parsed document's skeleton before its slots are known: its compact
/// text with a hole where each scalar goes, the scalars' raw texts in one
/// buffer, and the document's distinct paths, numbered.
pub(crate) struct Draft {
    text: String,
    /// `(offset in text, scalar)` of each hole, in order.
    holes: Vec<(usize, u32)>,
    scalars: Vec<Scalar>,
    raws: String,
    paths: Vec<String>,
}

struct Scalar {
    path: u16,
    raw: Range<usize>,
    kind: Kind,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Kind {
    /// A string with no escape: stored exactly as written in a string column.
    PlainString,
    String,
    /// An integer literal: stored exactly as written in an integer column.
    Integer,
    Number,
    Bool,
    Null,
}

/// One document's skeleton, and its residual when any value needed one.
pub(crate) struct Shape {
    pub(crate) skeleton: String,
    pub(crate) residual: Option<String>,
}

/// Where a path's values are stored in a batch: its own column and its list
/// column (`path[]`), each with the column's index in the batch and field id
/// in the table, looked up once per batch.
#[derive(Clone, Copy, Default)]
struct Columns {
    own: Option<(usize, FieldId)>,
    list: Option<(usize, FieldId)>,
}

/// The column lookups of one batch, shared by its documents (one per thread).
#[derive(Default)]
pub(crate) struct Lookups(HashMap<String, Columns>);

impl Lookups {
    fn of(&mut self, path: &str, batch: &RecordBatch, schema: &TableSchema) -> Columns {
        if let Some(found) = self.0.get(path) {
            return *found;
        }
        let at = |name: &str| {
            let index = batch.schema().index_of(name).ok()?;
            Some((index, schema.id_of(name)?))
        };
        let found = Columns {
            own: at(path),
            list: at(&format!("{path}{LIST_SUFFIX}")),
        };
        self.0.insert(path.to_owned(), found);
        found
    }
}

/// Parse `source` once: its value (the object `flatten` reads) and its draft
/// skeleton.
pub(crate) fn parse(source: &str) -> Result<(Value, Draft), String> {
    let mut p = Parser {
        bytes: source.as_bytes(),
        at: 0,
        draft: Draft {
            text: String::with_capacity(source.len() / 2),
            holes: Vec::new(),
            scalars: Vec::new(),
            raws: String::with_capacity(source.len() / 2),
            paths: Vec::new(),
        },
        path: String::new(),
    };
    p.ws();
    let value = p.value()?;
    p.ws();
    if p.at != p.bytes.len() {
        return Err("malformed JSON: text after the value".into());
    }
    Ok((value, p.draft))
}

/// The shape of the document `draft` was parsed from, stored as row `row` of
/// `batch`. With no batch (the rows could not be converted) every value is
/// kept in the residual, so the document still rebuilds exactly.
pub(crate) fn shape(
    draft: &Draft,
    row: usize,
    batch: Option<&RecordBatch>,
    schema: &TableSchema,
    lookups: &mut Lookups,
) -> Shape {
    let mut counts = vec![0u32; draft.paths.len()];
    for s in &draft.scalars {
        if s.kind != Kind::Null {
            counts[s.path as usize] += 1;
        }
    }
    let mut seen = vec![0u32; draft.paths.len()];
    let mut slots: Vec<Option<Slot>> = Vec::with_capacity(draft.scalars.len());
    let mut kept: Vec<&str> = Vec::new();
    for s in &draft.scalars {
        if s.kind == Kind::Null {
            slots.push(None);
            continue;
        }
        let k = seen[s.path as usize] as usize;
        seen[s.path as usize] += 1;
        let many = counts[s.path as usize] > 1;
        let raw = &draft.raws[s.raw.clone()];
        let slot = batch.and_then(|b| {
            let columns = lookups.of(&draft.paths[s.path as usize], b, schema);
            locate(b, columns, s.kind, raw, k, many, row)
        });
        match slot {
            Some(slot) => slots.push(Some(slot)),
            None => {
                kept.push(raw);
                slots.push(Some(Slot::Residual(kept.len() - 1)));
            }
        }
    }
    let mut skeleton = String::with_capacity(draft.text.len() + draft.holes.len() * 4);
    let mut from = 0;
    for &(at, i) in &draft.holes {
        skeleton.push_str(&draft.text[from..at]);
        match slots[i as usize] {
            Some(slot) => {
                use std::fmt::Write;
                let _ = write!(skeleton, "{slot}");
            }
            None => skeleton.push_str(&draft.raws[draft.scalars[i as usize].raw.clone()]),
        }
        from = at;
    }
    skeleton.push_str(&draft.text[from..]);
    let residual = (!kept.is_empty()).then(|| residual(kept.iter().copied()));
    Shape { skeleton, residual }
}

/// The slot of the `k`-th value of its path in row `row`, when the stored
/// value is exactly what the document wrote.
fn locate(
    batch: &RecordBatch,
    columns: Columns,
    kind: Kind,
    raw: &str,
    k: usize,
    many: bool,
    row: usize,
) -> Option<Slot> {
    // Several values: the list column, element k; or the path's own column
    // when the engine typed it a list. One value: the path's own column, or
    // its first element when that column is a list.
    let candidates: [Option<(usize, FieldId, bool)>; 2] = if many {
        [columns.list.map(|(i, f)| (i, f, true)), columns.own.map(|(i, f)| (i, f, false))]
    } else {
        [columns.own.map(|(i, f)| (i, f, false)), None]
    };
    for (index, field, is_list_column) in candidates.into_iter().flatten() {
        let array = batch.column(index);
        let listed = is_list_column || matches!(array.data_type(), DataType::List(_) | DataType::LargeList(_));
        let (slot, element) = match (many, listed) {
            (true, true) => (Slot::Element(field, k), Some(k)),
            (false, false) => (Slot::Value(field), None),
            (false, true) => (Slot::Element(field, 0), Some(0)),
            (true, false) => continue,
        };
        let value_type = match array.data_type() {
            DataType::List(f) | DataType::LargeList(f) => f.data_type().clone(),
            other => other.clone(),
        };
        let as_written = matches!(
            (kind, &value_type),
            (Kind::PlainString, DataType::LargeUtf8 | DataType::Utf8 | DataType::Utf8View)
                | (Kind::Integer, DataType::Int64 | DataType::Int32 | DataType::Int16 | DataType::Int8)
                | (Kind::Bool, DataType::Boolean)
        );
        if as_written && !array.is_null(row) {
            return Some(slot);
        }
        if render_value(array.as_ref(), row, element).as_deref() == Some(raw) {
            return Some(slot);
        }
    }
    None
}

struct Parser<'a> {
    bytes: &'a [u8],
    at: usize,
    draft: Draft,
    /// The path of the value being read, `.`-joined; extended and cut back
    /// as objects are entered and left.
    path: String,
}

impl<'a> Parser<'a> {
    fn ws(&mut self) {
        while self.at < self.bytes.len() && matches!(self.bytes[self.at], b' ' | b'\t' | b'\n' | b'\r') {
            self.at += 1;
        }
    }

    fn peek(&self) -> Option<u8> {
        self.bytes.get(self.at).copied()
    }

    fn bad(&self, what: &str) -> String {
        format!("malformed JSON: {what} at {}", self.at)
    }

    /// The input's text from `start` to here: borrowed from the input, not
    /// the parser, so the parser can go on writing while it is held.
    fn raw(&self, start: usize) -> &'a str {
        let bytes: &'a [u8] = self.bytes;
        // The input is a &str and every token starts and ends on an ASCII byte.
        std::str::from_utf8(&bytes[start..self.at]).unwrap_or("")
    }

    /// The number of the current path, numbering it when it is new.
    fn path_id(&mut self) -> u16 {
        let paths = &mut self.draft.paths;
        match paths.iter().position(|p| *p == self.path) {
            Some(i) => i as u16,
            None => {
                paths.push(self.path.clone());
                (paths.len() - 1) as u16
            }
        }
    }

    fn value(&mut self) -> Result<Value, String> {
        match self.peek() {
            Some(b'{') => self.object(),
            Some(b'[') => self.array(),
            Some(_) => self.scalar(),
            None => Err(self.bad("value expected")),
        }
    }

    fn object(&mut self) -> Result<Value, String> {
        self.at += 1;
        self.draft.text.push('{');
        let mut map = Map::new();
        self.ws();
        if self.peek() == Some(b'}') {
            self.at += 1;
            self.draft.text.push('}');
            return Ok(Value::Object(map));
        }
        loop {
            self.ws();
            let start = self.at;
            let escaped = self.string()?;
            let raw_key = self.raw(start);
            let key = if escaped {
                serde_json::from_str::<String>(raw_key).map_err(|e| format!("malformed JSON: {e}"))?
            } else {
                raw_key[1..raw_key.len() - 1].to_owned()
            };
            self.draft.text.push_str(raw_key);
            self.draft.text.push(':');
            self.ws();
            if self.peek() != Some(b':') {
                return Err(self.bad("':' expected"));
            }
            self.at += 1;
            self.ws();
            let outer = self.path.len();
            if outer > 0 {
                self.path.push('.');
            }
            self.path.push_str(&key);
            let value = self.value();
            self.path.truncate(outer);
            map.insert(key, value?);
            self.ws();
            match self.peek() {
                Some(b',') => {
                    self.at += 1;
                    self.draft.text.push(',');
                }
                Some(b'}') => {
                    self.at += 1;
                    self.draft.text.push('}');
                    return Ok(Value::Object(map));
                }
                _ => return Err(self.bad("',' or '}' expected")),
            }
        }
    }

    /// An array's entries keep the array's path, as `flatten` paths them.
    fn array(&mut self) -> Result<Value, String> {
        self.at += 1;
        self.draft.text.push('[');
        let mut items = Vec::new();
        self.ws();
        if self.peek() == Some(b']') {
            self.at += 1;
            self.draft.text.push(']');
            return Ok(Value::Array(items));
        }
        loop {
            self.ws();
            items.push(self.value()?);
            self.ws();
            match self.peek() {
                Some(b',') => {
                    self.at += 1;
                    self.draft.text.push(',');
                }
                Some(b']') => {
                    self.at += 1;
                    self.draft.text.push(']');
                    return Ok(Value::Array(items));
                }
                _ => return Err(self.bad("',' or ']' expected")),
            }
        }
    }

    /// Past one string token; whether it holds an escape.
    fn string(&mut self) -> Result<bool, String> {
        if self.peek() != Some(b'"') {
            return Err(self.bad("string expected"));
        }
        self.at += 1;
        let mut escaped = false;
        while let Some(b) = self.peek() {
            self.at += 1;
            match b {
                b'"' => return Ok(escaped),
                b'\\' => {
                    escaped = true;
                    if self.peek().is_none() {
                        break;
                    }
                    self.at += 1;
                }
                0..=0x1f => return Err(self.bad("control character in string")),
                _ => {}
            }
        }
        Err(self.bad("unterminated string"))
    }

    fn scalar(&mut self) -> Result<Value, String> {
        let start = self.at;
        let (value, kind) = if self.peek() == Some(b'"') {
            let escaped = self.string()?;
            let raw = self.raw(start);
            if escaped {
                let s = serde_json::from_str::<String>(raw).map_err(|e| format!("malformed JSON: {e}"))?;
                (Value::String(s), Kind::String)
            } else {
                (Value::String(raw[1..raw.len() - 1].to_owned()), Kind::PlainString)
            }
        } else {
            while let Some(b) = self.peek() {
                if matches!(b, b',' | b'}' | b']' | b' ' | b'\t' | b'\n' | b'\r') {
                    break;
                }
                self.at += 1;
            }
            let raw = self.raw(start);
            match raw {
                "true" => (Value::Bool(true), Kind::Bool),
                "false" => (Value::Bool(false), Kind::Bool),
                "null" => (Value::Null, Kind::Null),
                _ => number(raw).ok_or_else(|| format!("malformed JSON: bad value {raw:?}"))?,
            }
        };
        let path = self.path_id();
        let from = self.draft.raws.len();
        let raw_end = self.at;
        self.draft.raws.push_str(std::str::from_utf8(&self.bytes[start..raw_end]).unwrap_or(""));
        let index = self.draft.scalars.len() as u32;
        self.draft.holes.push((self.draft.text.len(), index));
        self.draft.scalars.push(Scalar {
            path,
            raw: from..self.draft.raws.len(),
            kind,
        });
        Ok(value)
    }
}

/// A JSON number token as `serde_json` reads it: an integer that fits in
/// `i64` or `u64` as such, anything else as `f64`.
fn number(raw: &str) -> Option<(Value, Kind)> {
    let b = raw.as_bytes();
    let mut i = 0;
    if b.first() == Some(&b'-') {
        i += 1;
    }
    let digits = |i: &mut usize| {
        let s = *i;
        while b.get(*i).is_some_and(u8::is_ascii_digit) {
            *i += 1;
        }
        *i > s
    };
    let int_start = i;
    if !digits(&mut i) || (b[int_start] == b'0' && i - int_start > 1) {
        return None;
    }
    let mut integer = true;
    if b.get(i) == Some(&b'.') {
        i += 1;
        integer = false;
        if !digits(&mut i) {
            return None;
        }
    }
    if matches!(b.get(i), Some(b'e' | b'E')) {
        i += 1;
        integer = false;
        if matches!(b.get(i), Some(b'+' | b'-')) {
            i += 1;
        }
        if !digits(&mut i) {
            return None;
        }
    }
    if i != b.len() {
        return None;
    }
    if integer {
        if let Ok(n) = raw.parse::<i64>() {
            return Some((Value::Number(n.into()), Kind::Integer));
        }
        if let Ok(n) = raw.parse::<u64>() {
            return Some((Value::Number(n.into()), Kind::Integer));
        }
    }
    let f: f64 = raw.parse().ok()?;
    Number::from_f64(f).map(|n| (Value::Number(n), Kind::Number))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A document of every shape the rebuild has to keep, written compact
    /// so the rebuilt text equals it byte for byte.
    const SHAPES: &str = concat!(
        r#"{"@timestamp":"2023-01-01T09:37:20.000Z","level":"info","#,
        r#""http.method":"GET","http":{"method":"PUT"},"#,
        r#""status":[200,404],"took":1.5,"message":"a \"quoted\" word","#,
        r#""tags":["one"],"empty":{},"none":[],"gone":null,"#,
        r#""spans":[{"name":"a","ms":3},{"ms":4},{"name":"c"}],"#,
        r#""odd":"2023-01-01T09:37:20Z"}"#
    );

    #[test]
    fn the_parse_reads_what_serde_json_reads() {
        for doc in [SHAPES, r#"{"a":[1,[2,null],{"b":-0.5e3}],"c":18446744073709551615,"d":"é"}"#] {
            let (value, _) = parse(doc).unwrap();
            assert_eq!(value, serde_json::from_str::<Value>(doc).unwrap(), "{doc}");
        }
        assert!(parse(r#"{"a":01}"#).is_err());
        assert!(parse(r#"{"a":1} x"#).is_err());
    }

    #[test]
    fn a_document_rebuilds_from_its_shape_and_its_row_exactly() {
        use infino::arrow_schema::{Field, Schema, TimeUnit};
        use std::sync::Arc;
        let list = |t: DataType| DataType::List(Arc::new(Field::new("item", t, true)));
        let schema = Schema::new(vec![
            Field::new("@timestamp", DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())), true),
            Field::new("level", DataType::LargeUtf8, true),
            Field::new("http.method", DataType::LargeUtf8, true),
            Field::new("http.method[]", list(DataType::LargeUtf8), true),
            Field::new("status", DataType::Int64, true),
            Field::new("status[]", list(DataType::Int64), true),
            Field::new("took", DataType::Float64, true),
            Field::new("message", DataType::LargeUtf8, true),
        ]);
        let conn = infino::connect("memory://").unwrap();
        conn.create_table("t", Arc::new(schema), infino::IndexSpec::new()).unwrap();
        let table_schema = conn.schema("t").unwrap();
        let row = serde_json::json!({
            "@timestamp": 1672565840000i64,
            "level": "info",
            "http.method": null,
            "http.method[]": ["GET", "PUT"],
            "status[]": [200, 404],
            "took": 1.5,
            "message": "a \"quoted\" word",
        });
        let batch = infino::rows_to_batch(&[row], &table_schema).unwrap();
        let (_, draft) = parse(SHAPES).unwrap();
        let shape = shape(&draft, 0, Some(&batch), &table_schema, &mut Lookups::default());
        // Every value a column holds is a slot; the rest are kept as written.
        assert!(!shape.skeleton.contains("PUT") && !shape.skeleton.contains("info"), "{}", shape.skeleton);
        assert!(shape.skeleton.contains(r#""http.method":#"#) && shape.skeleton.contains(r#""http":{"method":#"#), "{}", shape.skeleton);
        let rebuilt = infino::document::rebuild(&shape.skeleton, shape.residual.as_deref(), |field, element| {
            let name = table_schema.name_of(field)?.to_string();
            infino::document::render_value(batch.column_by_name(&name)?.as_ref(), 0, element)
        })
        .unwrap();
        assert_eq!(rebuilt, SHAPES);
    }

    #[test]
    fn with_no_row_every_value_is_kept_and_the_shape_too() {
        let conn = infino::connect("memory://").unwrap();
        conn.create_table(
            "t",
            std::sync::Arc::new(infino::arrow_schema::Schema::new(vec![infino::arrow_schema::Field::new(
                "a",
                DataType::Int64,
                true,
            )])),
            infino::IndexSpec::new(),
        )
        .unwrap();
        let schema = conn.schema("t").unwrap();
        let (_, draft) = parse(r#"{"a.b": 1, "c": {"d": ["x", null, {}], "e": []}, "f": null}"#).unwrap();
        let shape = shape(&draft, 0, None, &schema, &mut Lookups::default());
        assert_eq!(shape.skeleton, r#"{"a.b":#r0,"c":{"d":[#r1,null,{}],"e":[]},"f":null}"#);
        assert_eq!(shape.residual.as_deref(), Some(r#"[1,"x"]"#));
    }
}
