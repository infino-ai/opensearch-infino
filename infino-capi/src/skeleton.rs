// SPDX-License-Identifier: Apache-2.0
//! A document's skeleton and residual: the shape the engine's document
//! column rebuilds `source` from (see `infino::document`).
//!
//! The document's own JSON text is walked once, token by token. Every
//! scalar value is located in the row the bridge stored for it: the path
//! `flatten` gives it (nested keys and dotted keys alike), then the column
//! that path landed in. A path that holds one value is in the column of
//! that name; a path that holds several is in its list column (`path[]`,
//! or the column itself when the engine typed it a list), the k-th value
//! of the path being the k-th element. The value becomes a slot naming that
//! column when the engine renders the stored value exactly as the document
//! wrote it, byte for byte; otherwise it goes to the residual as written.
//! So the rebuilt document is the original, whitespace aside.

use std::collections::HashMap;

use infino::arrow_array::{Array, RecordBatch};
use infino::arrow_schema::DataType;
use infino::document::{render_value, residual, Slot};
use infino::serde_json;
use infino::TableSchema;

use crate::LIST_SUFFIX;

/// One document's skeleton, and its residual when any value needed one.
pub(crate) struct Shape {
    pub(crate) skeleton: String,
    pub(crate) residual: Option<String>,
}

/// The shape of `source`, the document stored as row `row` of `batch`.
/// With no batch (the rows could not be converted) every value is kept in
/// the residual, so the document still rebuilds exactly.
pub(crate) fn shape(source: &str, row: usize, batch: Option<&RecordBatch>, schema: &TableSchema) -> Result<Shape, String> {
    let bytes = source.as_bytes();
    // First pass: how many values each path holds, which decides whether
    // a path's values sit in its scalar column or in its list column.
    let mut counts: HashMap<String, usize> = HashMap::new();
    {
        let mut p = Parser { bytes, at: 0 };
        p.ws();
        p.value("", &mut |path, _, null| {
            if !null {
                *counts.entry(path.to_string()).or_insert(0) += 1;
            }
            Ok(None)
        })?;
    }
    let mut seen: HashMap<String, usize> = HashMap::new();
    let mut kept: Vec<String> = Vec::new();
    let mut skeleton = String::with_capacity(source.len() / 2);
    let mut p = Parser { bytes, at: 0 };
    p.ws();
    let mut out = Writer { skeleton: &mut skeleton };
    p.value_into("", &mut out, &mut |path, raw, null| {
        if null {
            return Ok(None);
        }
        let k = {
            let n = seen.entry(path.to_string()).or_insert(0);
            *n += 1;
            *n - 1
        };
        let many = counts.get(path).copied().unwrap_or(0) > 1;
        let slot = batch.and_then(|batch| locate(batch, schema, path, k, many, row, raw));
        Ok(Some(slot.unwrap_or_else(|| {
            kept.push(raw.to_string());
            Slot::Residual(kept.len() - 1)
        })))
    })?;
    p.ws();
    if p.at != bytes.len() {
        return Err("document: text after the value".into());
    }
    let residual = (!kept.is_empty()).then(|| residual(kept.iter().map(String::as_str)));
    Ok(Shape { skeleton, residual })
}

/// The slot of the `k`-th value of `path` in row `row`, when the stored
/// value renders as `raw`.
fn locate(batch: &RecordBatch, schema: &TableSchema, path: &str, k: usize, many: bool, row: usize, raw: &str) -> Option<Slot> {
    let list = format!("{path}{LIST_SUFFIX}");
    let candidates: [(&str, bool); 2] = [(list.as_str(), true), (path, false)];
    for (name, is_list_column) in candidates {
        let Some(array) = batch.column_by_name(name) else {
            continue;
        };
        let Some(field) = schema.id_of(name) else {
            continue;
        };
        let listed = is_list_column || matches!(array.data_type(), DataType::List(_) | DataType::LargeList(_));
        let slot = match (many, listed) {
            (true, true) => Slot::Element(field, k),
            (false, false) => Slot::Value(field),
            // One value in a column typed a list: its first element.
            (false, true) if !is_list_column => Slot::Element(field, 0),
            _ => continue,
        };
        let element = match slot {
            Slot::Element(_, i) => Some(i),
            _ => None,
        };
        if render_value(array.as_ref(), row, element).as_deref() == Some(raw) {
            return Some(slot);
        }
    }
    None
}

struct Writer<'a> {
    skeleton: &'a mut String,
}

/// What a value walk reports for each scalar: the scalar's path, its raw
/// text, and whether it is `null`; the slot to write in its place (`None`
/// writes it as it is).
type OnScalar<'f> = dyn FnMut(&str, &str, bool) -> Result<Option<Slot>, String> + 'f;

struct Parser<'a> {
    bytes: &'a [u8],
    at: usize,
}

impl Parser<'_> {
    fn ws(&mut self) {
        while self.at < self.bytes.len() && matches!(self.bytes[self.at], b' ' | b'\t' | b'\n' | b'\r') {
            self.at += 1;
        }
    }

    fn peek(&self) -> Option<u8> {
        self.bytes.get(self.at).copied()
    }

    fn expect(&mut self, b: u8) -> Result<(), String> {
        if self.peek() == Some(b) {
            self.at += 1;
            Ok(())
        } else {
            Err(format!("document: expected '{}' at {}", b as char, self.at))
        }
    }

    /// Walk one value at `path` without writing anything.
    fn value(&mut self, path: &str, on: &mut OnScalar<'_>) -> Result<(), String> {
        let mut sink = String::new();
        let mut out = Writer { skeleton: &mut sink };
        self.value_into(path, &mut out, on)
    }

    /// Walk one value at `path`, writing its compact text with each scalar
    /// replaced by the slot `on` gives it.
    fn value_into(&mut self, path: &str, out: &mut Writer<'_>, on: &mut OnScalar<'_>) -> Result<(), String> {
        match self.peek() {
            Some(b'{') => self.object(path, out, on),
            Some(b'[') => self.array(path, out, on),
            Some(_) => {
                let start = self.at;
                self.scalar()?;
                let raw = std::str::from_utf8(&self.bytes[start..self.at]).map_err(|e| e.to_string())?;
                let null = raw == "null";
                match on(path, raw, null)? {
                    Some(slot) => out.skeleton.push_str(&slot.to_string()),
                    None => out.skeleton.push_str(raw),
                }
                Ok(())
            }
            None => Err("document: value expected".into()),
        }
    }

    fn object(&mut self, path: &str, out: &mut Writer<'_>, on: &mut OnScalar<'_>) -> Result<(), String> {
        self.expect(b'{')?;
        out.skeleton.push('{');
        self.ws();
        if self.peek() == Some(b'}') {
            self.at += 1;
            out.skeleton.push('}');
            return Ok(());
        }
        let mut first = true;
        loop {
            self.ws();
            let start = self.at;
            self.string()?;
            let raw_key = std::str::from_utf8(&self.bytes[start..self.at]).map_err(|e| e.to_string())?;
            let key: String = serde_json::from_str(raw_key).map_err(|e| format!("document: key: {e}"))?;
            if !first {
                out.skeleton.push(',');
            }
            first = false;
            out.skeleton.push_str(raw_key);
            out.skeleton.push(':');
            self.ws();
            self.expect(b':')?;
            self.ws();
            let child = if path.is_empty() { key } else { format!("{path}.{key}") };
            self.value_into(&child, out, on)?;
            self.ws();
            match self.peek() {
                Some(b',') => {
                    self.at += 1;
                }
                Some(b'}') => {
                    self.at += 1;
                    out.skeleton.push('}');
                    return Ok(());
                }
                _ => return Err(format!("document: ',' or '}}' expected at {}", self.at)),
            }
        }
    }

    /// An array's entries keep the array's path: a scalar entry is one more
    /// value of the path, an object entry's members are values of the
    /// path's member paths, and an array inside is read through, as
    /// `flatten` reads them.
    fn array(&mut self, path: &str, out: &mut Writer<'_>, on: &mut OnScalar<'_>) -> Result<(), String> {
        self.expect(b'[')?;
        out.skeleton.push('[');
        self.ws();
        if self.peek() == Some(b']') {
            self.at += 1;
            out.skeleton.push(']');
            return Ok(());
        }
        let mut first = true;
        loop {
            self.ws();
            if !first {
                out.skeleton.push(',');
            }
            first = false;
            self.value_into(path, out, on)?;
            self.ws();
            match self.peek() {
                Some(b',') => {
                    self.at += 1;
                }
                Some(b']') => {
                    self.at += 1;
                    out.skeleton.push(']');
                    return Ok(());
                }
                _ => return Err(format!("document: ',' or ']' expected at {}", self.at)),
            }
        }
    }

    fn string(&mut self) -> Result<(), String> {
        self.expect(b'"')?;
        while let Some(b) = self.peek() {
            self.at += 1;
            match b {
                b'"' => return Ok(()),
                b'\\' => {
                    if self.peek().is_none() {
                        break;
                    }
                    self.at += 1;
                }
                _ => {}
            }
        }
        Err("document: unterminated string".into())
    }

    fn scalar(&mut self) -> Result<(), String> {
        match self.peek() {
            Some(b'"') => self.string(),
            Some(_) => {
                let start = self.at;
                while let Some(b) = self.peek() {
                    if matches!(b, b',' | b'}' | b']' | b' ' | b'\t' | b'\n' | b'\r') {
                        break;
                    }
                    self.at += 1;
                }
                if self.at == start {
                    Err(format!("document: value expected at {start}"))
                } else {
                    Ok(())
                }
            }
            None => Err("document: value expected".into()),
        }
    }
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
    fn a_document_rebuilds_from_its_shape_and_its_row_exactly() {
        let schema = infino::arrow_schema::Schema::new(vec![
            infino::arrow_schema::Field::new("@timestamp", DataType::Timestamp(infino::arrow_schema::TimeUnit::Millisecond, Some("UTC".into())), true),
            infino::arrow_schema::Field::new("level", DataType::LargeUtf8, true),
            infino::arrow_schema::Field::new("http.method", DataType::LargeUtf8, true),
            infino::arrow_schema::Field::new("http.method[]", DataType::List(std::sync::Arc::new(infino::arrow_schema::Field::new("item", DataType::LargeUtf8, true))), true),
            infino::arrow_schema::Field::new("status", DataType::Int64, true),
            infino::arrow_schema::Field::new("status[]", DataType::List(std::sync::Arc::new(infino::arrow_schema::Field::new("item", DataType::Int64, true))), true),
            infino::arrow_schema::Field::new("took", DataType::Float64, true),
            infino::arrow_schema::Field::new("message", DataType::LargeUtf8, true),
        ]);
        let conn = infino::connect("memory://").unwrap();
        conn.create_table("t", std::sync::Arc::new(schema), infino::IndexSpec::new()).unwrap();
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
        let shape = shape(SHAPES, 0, Some(&batch), &table_schema).unwrap();
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
            std::sync::Arc::new(infino::arrow_schema::Schema::new(vec![infino::arrow_schema::Field::new("a", DataType::Int64, true)])),
            infino::IndexSpec::new(),
        )
        .unwrap();
        let schema = conn.schema("t").unwrap();
        let doc = r#"{"a.b": 1, "c": {"d": ["x", null, {}], "e": []}, "f": null}"#;
        let shape = shape(doc, 0, None, &schema).unwrap();
        assert_eq!(shape.skeleton, r#"{"a.b":#r0,"c":{"d":[#r1,null,{}],"e":[]},"f":null}"#);
        assert_eq!(shape.residual.as_deref(), Some(r#"[1,"x"]"#));
    }
}
