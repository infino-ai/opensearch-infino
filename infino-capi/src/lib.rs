//! Thin C-ABI bridge over the Infino engine for the OpenSearch-on-Infino spike.
//! JSON-in / JSON-out.
//!
//! Dynamic schema: the per-shard table's columns are inferred from the first
//! document (`infino_create_from_source`): every scalar field becomes a typed
//! Infino column (string→LargeUtf8, int→Int64, float→Float64, bool→Boolean) so
//! SQL aggregates / filters run over real columns. Four reserved columns are
//! always present: `id` Int64, `osid` LargeUtf8 (original _id), `source`
//! LargeUtf8 (raw _source JSON, for fetch), `text` LargeUtf8 (concat of string
//! fields, the FTS column), `emb` FixedSizeList<Float32,16> (the vector column).

use std::ffi::{c_char, CStr, CString};
use std::os::raw::c_int;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::ptr;
use std::sync::Arc;

use infino::arrow_array::{
    Array, ArrayRef, BooleanArray, FixedSizeListArray, Float32Array, Float64Array, Int64Array,
    LargeStringArray, RecordBatch, StringArray, UInt64Array,
};
use infino::arrow_schema::{DataType, Field, Schema};
use infino::{connect, Bm25SearchOptions, BoolMode, Connection, IndexSpec, Metric};

const DIM: usize = 16;
/// Column names the bridge manages itself; everything else is a user field.
const RESERVED: [&str; 5] = ["id", "osid", "source", "text", "emb"];
/// Source fields preferred as the FTS `text` column (else all strings are concatenated).
const TEXT_FIELDS: [&str; 5] = ["text", "body", "content", "title", "message"];
/// Source fields treated as the vector for the `emb` column.
const VECTOR_FIELDS: [&str; 3] = ["emb", "vector", "embedding"];

unsafe fn cstr<'a>(p: *const c_char) -> Option<&'a str> {
    if p.is_null() { return None; }
    CStr::from_ptr(p).to_str().ok()
}
fn out_string(s: String) -> *mut c_char {
    CString::new(s).map(|c| c.into_raw()).unwrap_or(ptr::null_mut())
}
fn conn_ref<'a>(h: *mut Connection) -> Option<&'a Connection> {
    if h.is_null() { None } else { Some(unsafe { &*h }) }
}
fn err_json(e: &str) -> *mut c_char {
    out_string(format!("{{\"error\":{}}}", json_str(e)))
}

fn emb_field() -> Field {
    Field::new(
        "emb",
        DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), DIM as i32),
        true,
    )
}
fn emb_array(mut emb: Vec<f32>) -> Result<FixedSizeListArray, ()> {
    emb.resize(DIM, 0.0);
    let field = Arc::new(Field::new("item", DataType::Float32, true));
    FixedSizeListArray::try_new(field, DIM as i32, Arc::new(Float32Array::from(emb)), None).map_err(|_| ())
}

/// Extract the FTS text value from a _source object: a named text field, else all strings joined.
fn extract_text(obj: Option<&serde_json::Map<String, serde_json::Value>>) -> String {
    obj.and_then(|m| TEXT_FIELDS.iter().find_map(|k| m.get(*k).and_then(|x| x.as_str()).map(String::from)))
        .or_else(|| obj.map(|m| m.values().filter_map(|x| x.as_str()).collect::<Vec<_>>().join(" ")))
        .unwrap_or_default()
}
/// Extract the vector for `emb`: a named vector field (numeric array), else empty.
fn extract_emb(obj: Option<&serde_json::Map<String, serde_json::Value>>) -> Vec<f32> {
    obj.and_then(|m| VECTOR_FIELDS.iter().find_map(|k| m.get(*k).and_then(|x| x.as_array())))
        .map(|a| a.iter().filter_map(|n| n.as_f64().map(|f| f as f32)).collect())
        .unwrap_or_default()
}

/// Infer the Arrow type for one dynamic (user) field, or None to skip (arrays / objects / null).
fn infer_type(v: &serde_json::Value) -> Option<DataType> {
    match v {
        serde_json::Value::String(_) => Some(DataType::LargeUtf8),
        serde_json::Value::Bool(_) => Some(DataType::Boolean),
        serde_json::Value::Number(n) => {
            Some(if n.is_i64() || n.is_u64() { DataType::Int64 } else { DataType::Float64 })
        }
        _ => None, // arrays feed `emb`; objects/null are kept only in `source`
    }
}

#[no_mangle]
pub extern "C" fn infino_open(uri: *const c_char) -> *mut Connection {
    catch_unwind(AssertUnwindSafe(|| {
        let uri = unsafe { cstr(uri) }.unwrap_or("memory://");
        match connect(uri) {
            Ok(c) => Box::into_raw(Box::new(c)),
            Err(e) => { eprintln!("[capi] connect: {e}"); ptr::null_mut() }
        }
    }))
    .unwrap_or(ptr::null_mut())
}

#[no_mangle]
pub extern "C" fn infino_close(h: *mut Connection) {
    if !h.is_null() { unsafe { drop(Box::from_raw(h)) }; }
}

#[no_mangle]
pub extern "C" fn infino_free_string(s: *mut c_char) {
    if !s.is_null() { unsafe { drop(CString::from_raw(s)) }; }
}

/// Create the per-shard table with a schema inferred from one _source document.
/// Idempotent-ish: an existing table (AlreadyExists) is treated as success.
#[no_mangle]
pub extern "C" fn infino_create_from_source(
    h: *mut Connection, name: *const c_char, source: *const c_char,
) -> c_int {
    catch_unwind(AssertUnwindSafe(|| {
        let (Some(conn), Some(name), Some(source)) =
            (conn_ref(h), unsafe { cstr(name) }, unsafe { cstr(source) }) else { return -1 };
        let v: serde_json::Value = serde_json::from_str(source).unwrap_or(serde_json::Value::Null);
        let obj = v.as_object();

        let mut fields = vec![
            Field::new("id", DataType::Int64, false),
            Field::new("osid", DataType::LargeUtf8, true),
            Field::new("text", DataType::LargeUtf8, true),
            Field::new("source", DataType::LargeUtf8, true),
            emb_field(),
        ];
        // Dynamic columns: one per scalar user field (deterministic order).
        if let Some(m) = obj {
            let mut keys: Vec<&String> = m.keys().collect();
            keys.sort();
            for k in keys {
                if RESERVED.contains(&k.as_str()) || VECTOR_FIELDS.contains(&k.as_str()) {
                    continue;
                }
                if let Some(dt) = infer_type(&m[k]) {
                    fields.push(Field::new(k, dt, true));
                }
            }
        }
        let spec = IndexSpec::new().fts("text").vector("emb", DIM, Metric::Cosine);
        match conn.create_table(name, Arc::new(Schema::new(fields)), spec) {
            Ok(_) => 0,
            Err(infino::InfinoError::AlreadyExists(_)) => 0,
            Err(e) => { eprintln!("[capi] create_from_source: {e}"); -2 }
        }
    }))
    .unwrap_or(-99)
}

/// Columns of a table as JSON [{"name","type"}], type in {i64,f64,bool,str,vec,other}.
/// "" is returned if the table is absent.
#[no_mangle]
pub extern "C" fn infino_columns(h: *mut Connection, name: *const c_char) -> *mut c_char {
    catch_unwind(AssertUnwindSafe(|| {
        let (Some(conn), Some(name)) = (conn_ref(h), unsafe { cstr(name) }) else { return ptr::null_mut() };
        match conn.open_table(name) {
            Ok(t) => {
                let cols: Vec<String> = t.schema().fields().iter().map(|f| {
                    let ty = match f.data_type() {
                        DataType::Int64 => "i64",
                        DataType::Float64 => "f64",
                        DataType::Boolean => "bool",
                        DataType::LargeUtf8 | DataType::Utf8 => "str",
                        DataType::FixedSizeList(..) => "vec",
                        _ => "other",
                    };
                    format!("{{\"name\":{},\"type\":{}}}", json_str(f.name()), json_str(ty))
                }).collect();
                out_string(format!("[{}]", cols.join(",")))
            }
            Err(_) => out_string(String::new()),
        }
    }))
    .unwrap_or(ptr::null_mut())
}

/// Index one document, conforming to the table's (dynamic) schema. Reserved
/// columns are filled by the bridge; each other column is pulled from _source by
/// name and coerced to the column's type (null if absent).
#[no_mangle]
pub extern "C" fn infino_index_source(
    h: *mut Connection,
    name: *const c_char,
    id: i64,
    osid: *const c_char,
    source: *const c_char,
) -> c_int {
    catch_unwind(AssertUnwindSafe(|| {
        let (Some(conn), Some(name), Some(osid), Some(source)) =
            (conn_ref(h), unsafe { cstr(name) }, unsafe { cstr(osid) }, unsafe { cstr(source) })
        else { return -1 };
        let table = match conn.open_table(name) { Ok(t) => t, Err(e) => { eprintln!("[capi] open: {e}"); return -2 } };
        let schema = table.schema();
        let v: serde_json::Value = serde_json::from_str(source).unwrap_or(serde_json::Value::Null);
        let obj = v.as_object();

        let mut cols: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());
        for field in schema.fields() {
            let arr: ArrayRef = match field.name().as_str() {
                "id" => Arc::new(Int64Array::from(vec![id])),
                "osid" => Arc::new(LargeStringArray::from(vec![osid.to_string()])),
                "source" => Arc::new(LargeStringArray::from(vec![source.to_string()])),
                "text" => Arc::new(LargeStringArray::from(vec![extract_text(obj)])),
                "emb" => match emb_array(extract_emb(obj)) { Ok(a) => Arc::new(a), Err(_) => return -5 },
                other => {
                    let cell = obj.and_then(|m| m.get(other));
                    match field.data_type() {
                        DataType::Int64 => Arc::new(Int64Array::from(vec![cell.and_then(|x| x.as_i64())])),
                        DataType::Float64 => Arc::new(Float64Array::from(vec![cell.and_then(|x| x.as_f64())])),
                        DataType::Boolean => Arc::new(BooleanArray::from(vec![cell.and_then(|x| x.as_bool())])),
                        _ => Arc::new(LargeStringArray::from(vec![cell.and_then(|x| x.as_str().map(String::from))])),
                    }
                }
            };
            cols.push(arr);
        }
        let batch = match RecordBatch::try_new(schema, cols) {
            Ok(b) => b, Err(e) => { eprintln!("[capi] build batch: {e}"); return -6 } };
        match table.append(&batch) {
            Ok(_) => 0,
            Err(e) => { eprintln!("[capi] index_source: {e}"); -7 }
        }
    }))
    .unwrap_or(-99)
}

/// Index many documents in one append (schema-aware, like infino_index_source but
/// batched). Infino wants batched appends — one row per call fragments the table.
/// `ids`/`osids`/`sources` are parallel arrays of length `count`.
#[no_mangle]
pub extern "C" fn infino_index_batch(
    h: *mut Connection,
    name: *const c_char,
    ids: *const i64,
    osids: *const *const c_char,
    sources: *const *const c_char,
    count: c_int,
) -> c_int {
    catch_unwind(AssertUnwindSafe(|| {
        let (Some(conn), Some(name)) = (conn_ref(h), unsafe { cstr(name) }) else { return -1 };
        if ids.is_null() || osids.is_null() || sources.is_null() || count <= 0 { return -1; }
        let n = count as usize;
        let id_s = unsafe { std::slice::from_raw_parts(ids, n) };
        let os_p = unsafe { std::slice::from_raw_parts(osids, n) };
        let sr_p = unsafe { std::slice::from_raw_parts(sources, n) };
        let osids_v: Vec<String> = os_p.iter().map(|p| unsafe { cstr(*p) }.unwrap_or("").to_string()).collect();
        let sources_v: Vec<String> = sr_p.iter().map(|p| unsafe { cstr(*p) }.unwrap_or("{}").to_string()).collect();
        let vals: Vec<serde_json::Value> =
            sources_v.iter().map(|s| serde_json::from_str(s).unwrap_or(serde_json::Value::Null)).collect();
        let objs: Vec<Option<&serde_json::Map<String, serde_json::Value>>> =
            vals.iter().map(|v| v.as_object()).collect();

        let table = match conn.open_table(name) { Ok(t) => t, Err(e) => { eprintln!("[capi] open: {e}"); return -2 } };
        let schema = table.schema();
        let mut cols: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());
        for field in schema.fields() {
            let arr: ArrayRef = match field.name().as_str() {
                "id" => Arc::new(Int64Array::from(id_s.to_vec())),
                "osid" => Arc::new(LargeStringArray::from_iter_values(osids_v.iter())),
                "source" => Arc::new(LargeStringArray::from_iter_values(sources_v.iter())),
                "text" => Arc::new(LargeStringArray::from_iter_values(objs.iter().map(|o| extract_text(*o)))),
                "emb" => {
                    let mut flat: Vec<f32> = Vec::with_capacity(n * DIM);
                    for o in &objs { let mut e = extract_emb(*o); e.resize(DIM, 0.0); flat.extend(e); }
                    let fld = Arc::new(Field::new("item", DataType::Float32, true));
                    match FixedSizeListArray::try_new(fld, DIM as i32, Arc::new(Float32Array::from(flat)), None) {
                        Ok(a) => Arc::new(a),
                        Err(_) => return -5,
                    }
                }
                other => {
                    let get = |i: usize| objs[i].and_then(|m| m.get(other));
                    match field.data_type() {
                        DataType::Int64 => Arc::new(Int64Array::from(
                            (0..n).map(|i| get(i).and_then(|x| x.as_i64())).collect::<Vec<_>>())),
                        DataType::Float64 => Arc::new(Float64Array::from(
                            (0..n).map(|i| get(i).and_then(|x| x.as_f64())).collect::<Vec<_>>())),
                        DataType::Boolean => Arc::new(BooleanArray::from(
                            (0..n).map(|i| get(i).and_then(|x| x.as_bool())).collect::<Vec<_>>())),
                        _ => Arc::new(LargeStringArray::from(
                            (0..n).map(|i| get(i).and_then(|x| x.as_str().map(String::from))).collect::<Vec<_>>())),
                    }
                }
            };
            cols.push(arr);
        }
        let batch = match RecordBatch::try_new(schema, cols) {
            Ok(b) => b, Err(e) => { eprintln!("[capi] build batch: {e}"); return -6 } };
        match table.append(&batch) {
            Ok(_) => 0,
            Err(e) => { eprintln!("[capi] index_batch: {e}"); -7 }
        }
    }))
    .unwrap_or(-99)
}

/// Fetch the raw _source JSON for one osid, or "" if absent.
#[no_mangle]
pub extern "C" fn infino_get(h: *mut Connection, name: *const c_char, osid: *const c_char) -> *mut c_char {
    catch_unwind(AssertUnwindSafe(|| {
        let (Some(conn), Some(name), Some(osid)) =
            (conn_ref(h), unsafe { cstr(name) }, unsafe { cstr(osid) }) else { return ptr::null_mut() };
        let esc = osid.replace('\'', "''");
        let sql = format!("SELECT source FROM {name} WHERE osid = '{esc}' LIMIT 1");
        match conn.query_sql(&sql) {
            Ok(batches) => {
                for b in &batches {
                    if b.num_rows() > 0 {
                        if let Some(a) = b.column(0).as_any().downcast_ref::<LargeStringArray>() {
                            if !a.is_null(0) { return out_string(a.value(0).to_string()); }
                        }
                    }
                }
                out_string(String::new())
            }
            Err(e) => { eprintln!("[capi] get: {e}"); out_string(String::new()) }
        }
    }))
    .unwrap_or(ptr::null_mut())
}

/// Delete every row with this osid (tombstone). Used to make the write path an
/// upsert: delete-then-append gives idempotent translog replay + correct updates.
#[no_mangle]
pub extern "C" fn infino_delete_osid(h: *mut Connection, name: *const c_char, osid: *const c_char) -> c_int {
    use datafusion::prelude::{col, lit};
    catch_unwind(AssertUnwindSafe(|| {
        let (Some(conn), Some(name), Some(osid)) =
            (conn_ref(h), unsafe { cstr(name) }, unsafe { cstr(osid) }) else { return -1 };
        let table = match conn.open_table(name) { Ok(t) => t, Err(_) => return 0 }; // no table ⇒ nothing to delete
        match table.delete(col("osid").eq(lit(osid.to_string()))) {
            Ok(_) => 0,
            Err(e) => { eprintln!("[capi] delete_osid: {e}"); -2 }
        }
    }))
    .unwrap_or(-99)
}

/// Drop a table and purge its storage. A missing table is not fatal.
#[no_mangle]
pub extern "C" fn infino_drop(h: *mut Connection, name: *const c_char) -> c_int {
    catch_unwind(AssertUnwindSafe(|| {
        let (Some(conn), Some(name)) = (conn_ref(h), unsafe { cstr(name) }) else { return -1 };
        match conn.drop_table(name, true) {
            Ok(_) => 0,
            Err(e) => { eprintln!("[capi] drop_table: {e}"); -2 }
        }
    }))
    .unwrap_or(-99)
}

#[no_mangle]
pub extern "C" fn infino_sql(h: *mut Connection, sql: *const c_char) -> *mut c_char {
    catch_unwind(AssertUnwindSafe(|| {
        let (Some(conn), Some(sql)) = (conn_ref(h), unsafe { cstr(sql) }) else { return ptr::null_mut() };
        match conn.query_sql(sql) {
            Ok(b) => out_string(batches_to_json(&b)),
            Err(e) => err_json(&e.to_string()),
        }
    }))
    .unwrap_or(ptr::null_mut())
}

#[no_mangle]
pub extern "C" fn infino_bm25(
    h: *mut Connection, name: *const c_char, column: *const c_char, query: *const c_char, k: c_int,
) -> *mut c_char {
    catch_unwind(AssertUnwindSafe(|| {
        let (Some(conn), Some(name), Some(column), Some(query)) =
            (conn_ref(h), unsafe { cstr(name) }, unsafe { cstr(column) }, unsafe { cstr(query) })
        else { return ptr::null_mut() };
        let table = match conn.open_table(name) { Ok(t) => t, Err(e) => return err_json(&e.to_string()) };
        match table.bm25_search(column, query, k as usize, Bm25SearchOptions::default(), Some(&["osid", "score"])) {
            Ok(b) => out_string(batches_to_json(&b)),
            Err(e) => err_json(&e.to_string()),
        }
    }))
    .unwrap_or(ptr::null_mut())
}

#[no_mangle]
pub extern "C" fn infino_knn(
    h: *mut Connection, name: *const c_char, column: *const c_char, emb_json: *const c_char, k: c_int,
) -> *mut c_char {
    catch_unwind(AssertUnwindSafe(|| {
        let (Some(conn), Some(name), Some(column), Some(emb_json)) =
            (conn_ref(h), unsafe { cstr(name) }, unsafe { cstr(column) }, unsafe { cstr(emb_json) })
        else { return ptr::null_mut() };
        let emb = match parse_vec(emb_json) { Ok(v) => v, Err(e) => return err_json(&e) };
        let table = match conn.open_table(name) { Ok(t) => t, Err(e) => return err_json(&e.to_string()) };
        match table.vector_search(column, &emb, k as usize, None, Some(&["osid", "score"])) {
            Ok(b) => out_string(batches_to_json(&b)),
            Err(e) => err_json(&e.to_string()),
        }
    }))
    .unwrap_or(ptr::null_mut())
}

/// Unranked full-text match set: the osids of every doc matching `query` on
/// `column`. Backs query-scoped aggregations over a text query (the match set
/// becomes a SQL `osid IN (...)` predicate).
#[no_mangle]
pub extern "C" fn infino_token_match(
    h: *mut Connection, name: *const c_char, column: *const c_char, query: *const c_char,
) -> *mut c_char {
    catch_unwind(AssertUnwindSafe(|| {
        let (Some(conn), Some(name), Some(column), Some(query)) =
            (conn_ref(h), unsafe { cstr(name) }, unsafe { cstr(column) }, unsafe { cstr(query) })
        else { return ptr::null_mut() };
        let table = match conn.open_table(name) { Ok(t) => t, Err(e) => return err_json(&e.to_string()) };
        match table.token_match(column, query, BoolMode::Or, Some(&["osid"])) {
            Ok(b) => out_string(batches_to_json(&b)),
            Err(e) => err_json(&e.to_string()),
        }
    }))
    .unwrap_or(ptr::null_mut())
}

/// Count of docs matching a full-text query (the hit total for a `match`), without
/// materializing the osid list — cheaper than infino_token_match for plain search.
#[no_mangle]
pub extern "C" fn infino_token_count(
    h: *mut Connection, name: *const c_char, column: *const c_char, query: *const c_char,
) -> *mut c_char {
    catch_unwind(AssertUnwindSafe(|| {
        let (Some(conn), Some(name), Some(column), Some(query)) =
            (conn_ref(h), unsafe { cstr(name) }, unsafe { cstr(column) }, unsafe { cstr(query) })
        else { return ptr::null_mut() };
        let table = match conn.open_table(name) { Ok(t) => t, Err(e) => return err_json(&e.to_string()) };
        match table.token_match(column, query, BoolMode::Or, Some(&["osid"])) {
            Ok(b) => out_string(b.iter().map(|rb| rb.num_rows()).sum::<usize>().to_string()),
            Err(e) => err_json(&e.to_string()),
        }
    }))
    .unwrap_or(ptr::null_mut())
}

#[no_mangle]
pub extern "C" fn infino_hybrid(
    h: *mut Connection,
    name: *const c_char,
    text_column: *const c_char,
    text_query: *const c_char,
    vector_column: *const c_char,
    emb_json: *const c_char,
    k: c_int,
) -> *mut c_char {
    catch_unwind(AssertUnwindSafe(|| {
        let (Some(conn), Some(name), Some(tcol), Some(tq), Some(vcol), Some(emb_json)) = (
            conn_ref(h), unsafe { cstr(name) }, unsafe { cstr(text_column) },
            unsafe { cstr(text_query) }, unsafe { cstr(vector_column) }, unsafe { cstr(emb_json) },
        ) else { return ptr::null_mut() };
        let emb = match parse_vec(emb_json) { Ok(v) => v, Err(e) => return err_json(&e) };
        let table = match conn.open_table(name) { Ok(t) => t, Err(e) => return err_json(&e.to_string()) };
        match table.hybrid_search(tcol, tq, BoolMode::Or, vcol, &emb, k as usize, Some(&["osid", "score"])) {
            Ok(b) => out_string(batches_to_json(&b)),
            Err(e) => err_json(&e.to_string()),
        }
    }))
    .unwrap_or(ptr::null_mut())
}

/// Parse "[f32,...]" and resize to the fixed vector dim.
fn parse_vec(json: &str) -> Result<Vec<f32>, String> {
    let mut v: Vec<f32> = serde_json::from_str::<Vec<f64>>(json)
        .map_err(|e| e.to_string())?
        .into_iter()
        .map(|f| f as f32)
        .collect();
    v.resize(DIM, 0.0);
    Ok(v)
}

fn json_str(s: &str) -> String {
    serde_json::to_string(s).unwrap_or_else(|_| "\"\"".into())
}
fn batches_to_json(batches: &[RecordBatch]) -> String {
    let mut rows: Vec<String> = Vec::new();
    for batch in batches {
        let schema = batch.schema();
        for r in 0..batch.num_rows() {
            let mut cells: Vec<String> = Vec::new();
            for (c, field) in schema.fields().iter().enumerate() {
                cells.push(format!("{}:{}", json_str(field.name()), cell_json(batch.column(c).as_ref(), r)));
            }
            rows.push(format!("{{{}}}", cells.join(",")));
        }
    }
    format!("[{}]", rows.join(","))
}
fn cell_json(col: &dyn Array, r: usize) -> String {
    if col.is_null(r) { return "null".into(); }
    if let Some(a) = col.as_any().downcast_ref::<Int64Array>() { return a.value(r).to_string(); }
    if let Some(a) = col.as_any().downcast_ref::<UInt64Array>() { return a.value(r).to_string(); }
    if let Some(a) = col.as_any().downcast_ref::<Float64Array>() { return a.value(r).to_string(); }
    if let Some(a) = col.as_any().downcast_ref::<Float32Array>() { return a.value(r).to_string(); }
    if let Some(a) = col.as_any().downcast_ref::<BooleanArray>() { return a.value(r).to_string(); }
    if let Some(a) = col.as_any().downcast_ref::<LargeStringArray>() { return json_str(a.value(r)); }
    if let Some(a) = col.as_any().downcast_ref::<StringArray>() { return json_str(a.value(r)); }
    "\"<unsupported>\"".into()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dynamic_schema_roundtrip() {
        let uri = CString::new("memory://").unwrap();
        let h = infino_open(uri.as_ptr());
        assert!(!h.is_null());
        let name = CString::new("docs").unwrap();
        // first doc carries a numeric field → a typed column is inferred
        let first = CString::new("{\"text\":\"red fox\",\"price\":10}").unwrap();
        assert_eq!(infino_create_from_source(h, name.as_ptr(), first.as_ptr()), 0);

        for (id, t, p) in [(1, "red fox", 10), (2, "red dog", 20), (3, "blue fox", 30)] {
            let osid = CString::new(format!("d{id}")).unwrap();
            let src = CString::new(format!("{{\"text\":\"{t}\",\"price\":{p}}}")).unwrap();
            assert_eq!(infino_index_source(h, name.as_ptr(), id, osid.as_ptr(), src.as_ptr()), 0);
        }
        // aggregate over the dynamic column
        let sql = CString::new("SELECT SUM(price) AS s FROM docs").unwrap();
        let out = infino_sql(h, sql.as_ptr());
        let got = unsafe { CStr::from_ptr(out) }.to_str().unwrap().to_string();
        infino_free_string(out);
        println!("sum(price) -> {got}");
        assert!(got.contains("60"), "{got}");

        let col = CString::new("text").unwrap();
        let q = CString::new("fox").unwrap();
        let out = infino_bm25(h, name.as_ptr(), col.as_ptr(), q.as_ptr(), 10);
        let got = unsafe { CStr::from_ptr(out) }.to_str().unwrap().to_string();
        infino_free_string(out);
        infino_close(h);
        println!("bm25 fox -> {got}");
        assert!(got.contains("d1") && got.contains("d3"), "{got}");
    }
}
