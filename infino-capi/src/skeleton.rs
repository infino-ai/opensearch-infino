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

use infino::arrow_array::{Array, RecordBatch};
use infino::arrow_schema::DataType;
use infino::document::{render_value, residual, Slot};
use infino::serde_json::{self, Map, Number, Value};
use infino::TableSchema;

use crate::LIST_SUFFIX;

/// A parsed document's skeleton before its slots are known.
pub(crate) struct Draft {
    /// Compact text pieces, and where each scalar goes, in order.
    pieces: Vec<Piece>,
    scalars: Vec<Scalar>,
}

enum Piece {
    Text(String),
    Scalar(usize),
}

struct Scalar {
    path: String,
    raw: String,
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

/// Parse `source` once: its value (the object `flatten` reads) and its draft
/// skeleton.
pub(crate) fn parse(source: &str) -> Result<(Value, Draft), String> {
    let mut p = Parser {
        bytes: source.as_bytes(),
        at: 0,
        draft: Draft {
            pieces: Vec::new(),
            scalars: Vec::new(),
        },
        text: String::with_capacity(source.len() / 2),
    };
    p.ws();
    let value = p.value("")?;
    p.ws();
    if p.at != p.bytes.len() {
        return Err("malformed JSON: text after the value".into());
    }
    p.flush();
    Ok((value, p.draft))
}

/// The shape of the document `draft` was parsed from, stored as row `row` of
/// `batch`. With no batch (the rows could not be converted) every value is
/// kept in the residual, so the document still rebuilds exactly.
pub(crate) fn shape(draft: &Draft, row: usize, batch: Option<&RecordBatch>, schema: &TableSchema) -> Shape {
    let mut counts: HashMap<&str, usize> = HashMap::new();
    for s in &draft.scalars {
        if s.kind != Kind::Null {
            *counts.entry(s.path.as_str()).or_insert(0) += 1;
        }
    }
    let mut seen: HashMap<&str, usize> = HashMap::new();
    let mut slots: Vec<Option<Slot>> = Vec::with_capacity(draft.scalars.len());
    let mut kept: Vec<&str> = Vec::new();
    for s in &draft.scalars {
        if s.kind == Kind::Null {
            slots.push(None);
            continue;
        }
        let k = {
            let n = seen.entry(s.path.as_str()).or_insert(0);
            *n += 1;
            *n - 1
        };
        let many = counts.get(s.path.as_str()).copied().unwrap_or(0) > 1;
        match batch.and_then(|b| locate(b, schema, s, k, many, row)) {
            Some(slot) => slots.push(Some(slot)),
            None => {
                kept.push(s.raw.as_str());
                slots.push(Some(Slot::Residual(kept.len() - 1)));
            }
        }
    }
    let mut skeleton = String::new();
    for piece in &draft.pieces {
        match piece {
            Piece::Text(text) => skeleton.push_str(text),
            Piece::Scalar(i) => match slots[*i] {
                Some(slot) => skeleton.push_str(&slot.to_string()),
                None => skeleton.push_str(&draft.scalars[*i].raw),
            },
        }
    }
    let residual = (!kept.is_empty()).then(|| residual(kept.iter().copied()));
    Shape { skeleton, residual }
}

/// The slot of the `k`-th value of its path in row `row`, when the stored
/// value is exactly what the document wrote.
fn locate(batch: &RecordBatch, schema: &TableSchema, s: &Scalar, k: usize, many: bool, row: usize) -> Option<Slot> {
    let list = format!("{}{LIST_SUFFIX}", s.path);
    for (name, is_list_column) in [(list.as_str(), true), (s.path.as_str(), false)] {
        let Some(array) = batch.column_by_name(name) else {
            continue;
        };
        let listed = is_list_column || matches!(array.data_type(), DataType::List(_) | DataType::LargeList(_));
        let (slot, element) = match (many, listed) {
            (true, true) => (Slot::Element(schema.id_of(name)?, k), Some(k)),
            (false, false) => (Slot::Value(schema.id_of(name)?), None),
            // One value in a column the engine typed a list: its first element.
            (false, true) if !is_list_column => (Slot::Element(schema.id_of(name)?, 0), Some(0)),
            _ => continue,
        };
        let value_type = match array.data_type() {
            DataType::List(f) | DataType::LargeList(f) => f.data_type().clone(),
            other => other.clone(),
        };
        let as_written = matches!(
            (s.kind, &value_type),
            (Kind::PlainString, DataType::LargeUtf8 | DataType::Utf8 | DataType::Utf8View)
                | (Kind::Integer, DataType::Int64 | DataType::Int32 | DataType::Int16 | DataType::Int8)
                | (Kind::Bool, DataType::Boolean)
        );
        if as_written && !array.is_null(row) {
            return Some(slot);
        }
        if render_value(array.as_ref(), row, element).as_deref() == Some(s.raw.as_str()) {
            return Some(slot);
        }
    }
    None
}

struct Parser<'a> {
    bytes: &'a [u8],
    at: usize,
    draft: Draft,
    /// Compact text not yet pushed as a piece.
    text: String,
}

impl Parser<'_> {
    fn flush(&mut self) {
        if !self.text.is_empty() {
            self.draft.pieces.push(Piece::Text(std::mem::take(&mut self.text)));
        }
    }

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

    fn raw(&self, start: usize) -> &str {
        // The input is a &str and every token starts and ends on an ASCII byte.
        std::str::from_utf8(&self.bytes[start..self.at]).unwrap_or("")
    }

    fn value(&mut self, path: &str) -> Result<Value, String> {
        match self.peek() {
            Some(b'{') => self.object(path),
            Some(b'[') => self.array(path),
            Some(_) => self.scalar(path),
            None => Err(self.bad("value expected")),
        }
    }

    fn object(&mut self, path: &str) -> Result<Value, String> {
        self.at += 1;
        self.text.push('{');
        let mut map = Map::new();
        self.ws();
        if self.peek() == Some(b'}') {
            self.at += 1;
            self.text.push('}');
            return Ok(Value::Object(map));
        }
        loop {
            self.ws();
            let start = self.at;
            let escaped = self.string()?;
            let raw_key = self.raw(start).to_owned();
            let key = if escaped {
                serde_json::from_str::<String>(&raw_key).map_err(|e| format!("malformed JSON: {e}"))?
            } else {
                raw_key[1..raw_key.len() - 1].to_owned()
            };
            self.text.push_str(&raw_key);
            self.text.push(':');
            self.ws();
            if self.peek() != Some(b':') {
                return Err(self.bad("':' expected"));
            }
            self.at += 1;
            self.ws();
            let child = if path.is_empty() { key.clone() } else { format!("{path}.{key}") };
            let value = self.value(&child)?;
            map.insert(key, value);
            self.ws();
            match self.peek() {
                Some(b',') => {
                    self.at += 1;
                    self.text.push(',');
                }
                Some(b'}') => {
                    self.at += 1;
                    self.text.push('}');
                    return Ok(Value::Object(map));
                }
                _ => return Err(self.bad("',' or '}' expected")),
            }
        }
    }

    /// An array's entries keep the array's path, as `flatten` paths them.
    fn array(&mut self, path: &str) -> Result<Value, String> {
        self.at += 1;
        self.text.push('[');
        let mut items = Vec::new();
        self.ws();
        if self.peek() == Some(b']') {
            self.at += 1;
            self.text.push(']');
            return Ok(Value::Array(items));
        }
        loop {
            self.ws();
            items.push(self.value(path)?);
            self.ws();
            match self.peek() {
                Some(b',') => {
                    self.at += 1;
                    self.text.push(',');
                }
                Some(b']') => {
                    self.at += 1;
                    self.text.push(']');
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

    fn scalar(&mut self, path: &str) -> Result<Value, String> {
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
        let raw = self.raw(start).to_owned();
        self.flush();
        self.draft.pieces.push(Piece::Scalar(self.draft.scalars.len()));
        self.draft.scalars.push(Scalar {
            path: path.to_owned(),
            raw,
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
        let shape = shape(&draft, 0, Some(&batch), &table_schema);
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
        let shape = shape(&draft, 0, None, &schema);
        assert_eq!(shape.skeleton, r#"{"a.b":#r0,"c":{"d":[#r1,null,{}],"e":[]},"f":null}"#);
        assert_eq!(shape.residual.as_deref(), Some(r#"[1,"x"]"#));
    }
}
