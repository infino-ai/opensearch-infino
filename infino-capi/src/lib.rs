//! Thin C-ABI bridge over the Infino engine for the OpenSearch-on-Infino plugin.
//! JSON-in / JSON-out.
//!
//! Schema: the per-index table is declared from the OpenSearch **mapping**
//! (`infino_ensure_table`), not inferred from the first document, so a
//! mapped `date` is a timestamp column (range filters, sort and
//! `date_histogram` run on it), a mapped `text` field carries its own BM25
//! index, a mapped `keyword` is an exact column, and a mapped `knn_vector`
//! is the only thing that creates a vector index. Fields the mapping does
//! not name are typed by the engine from the documents as they arrive
//! (`Supertable::append_rows` adds unknown paths to the schema itself).
//!
//! Three reserved columns are always present: `osid` (the OpenSearch `_id`),
//! `source` (the raw `_source` JSON, served back on fetch) and `osseq` (the
//! sequence number the plugin wrote the row under).
//!
//! Documents are checked against the declared columns before they reach
//! the engine, OpenSearch-style: a value a column cannot hold fails that
//! one document and the rest of the batch goes through, instead of the
//! engine refusing the whole batch.
//!
//! # Safety
//!
//! Every `extern "C"` entry point is `unsafe`: it dereferences the handle
//! and the C strings the caller passes, and trusts the array lengths it is
//! given. The caller (the JNA binding) owns those invariants. Strings the
//! bridge returns are freed with `infino_free_string`; the handle with
//! `infino_close`.

#![deny(unsafe_op_in_unsafe_fn)]
// The safety contract is the same for every entry point and stated once above.
#![allow(clippy::missing_safety_doc)]

mod read_sql;

use std::cell::RefCell;
use std::cmp::Reverse;
use std::collections::{HashMap, HashSet};
use std::ffi::{c_char, CStr, CString};
use std::os::raw::c_int;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::ptr;
use std::sync::{Arc, OnceLock, RwLock};
use std::time::Duration;

use datafusion::arrow::compute::{concat_batches, filter_record_batch};
use datafusion::common::tree_node::TreeNode;
use datafusion::common::DFSchema;
use datafusion::optimizer::analyzer::type_coercion::TypeCoercionRewriter;
use datafusion::prelude::{col, lit, Expr, SessionContext};
use infino::arrow_array::{
    new_null_array, Array, BooleanArray, Decimal128Array, Float32Array, Float64Array, Int64Array,
    LargeStringArray, RecordBatch, StringArray, TimestampMicrosecondArray,
    TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray, UInt64Array,
};
use infino::arrow_schema::{DataType, Field, Schema, TimeUnit};
use infino::serde_json::{self, json, Map, Value};
use infino::{
    connect_with, rows_to_batch, Bm25SearchOptions, BoolMode, ColumnIndex, ConnectOptions,
    Connection, FieldPatch, FtsField, IndexSpec, InfinoError, Metric, OptimizeOptions, SchemaPatch,
    Supertable,
};
use rayon::prelude::*;

/// The OpenSearch `_id` of a row.
const OSID: &str = "osid";
/// The raw `_source` JSON of a row, served back by the fetch phase.
const SOURCE: &str = "source";
/// The routing value a row was written with, when it was written with one:
/// what rebuilding a shard's documents from its table routes them by. The
/// column appears with the first routed write.
const ROUTING: &str = "osrouting";
/// The sequence number of the write that put the row there: in proxy mode
/// the one the plugin hands out, which a conditional write is checked
/// against; in default mode the shard's own, stamped by its writer, and a
/// search over a reader that does not hold that write yet leaves the row
/// out. Null for a row written without one (it reads as 0).
const SEQ: &str = "osseq";
/// The engine's row id column, which orders rows sharing every sort value.
const ROW_ID: &str = "_id";
/// The sequence number of the operation that wrote a row, when the writer
/// records one: the highest in a table says how far into the writer's
/// history the table holds every write.
const OP: &str = "osop";
/// Which writer stamped a row's [`SEQ`] and [`OP`]: sequence numbers
/// compare only between rows of one writer's history, so a cut leaves
/// another's alone.
const GEN: &str = "osgen";
/// Separator used when a multi-valued string field lands in a scalar string
/// column: the values are joined so a text index sees every token. The
/// original array is still in `source`.
const MULTI_VALUE_SEPARATOR: &str = " ";
/// What the column holding a field's values is named after the field's own,
/// for a document that gives the field more than one: `<path>[]`, a list of
/// the field's own type, declared the first time a document needs it. The
/// field's own column is null for that document, except a string field's,
/// which keeps the values joined so its text index sees every token.
const LIST_SUFFIX: &str = "[]";
/// The name Arrow gives a list column's element.
const LIST_ITEM: &str = "item";
/// A float of this magnitude or more is past the whole numbers a float holds
/// exactly (under 2^53), so an integer column does not take it as one.
const WHOLE_FLOAT_LIMIT: f64 = 9.0e15;
/// The hosted service's trial route: the one call made without a key, since
/// it is the call that asks for one.
const TRIAL_PATH: &str = "/v1/trial";
/// How long the trial call waits: a person is waiting on it, and a service
/// that cannot provision an account in this time is a failure to report.
const TRIAL_TIMEOUT: Duration = Duration::from_secs(30);
/// What the engine's hosted client puts before the HTTP status in the message
/// of a refused call; the status is read back off it (see `status_in`).
const STATUS_MARKER: &str = "server returned ";
/// Digits of an HTTP status code.
const STATUS_DIGITS: usize = 3;
/// HTTP status of a caller's statement that is wrong: it does not parse or
/// plan, or asks for what the engine does not do.
const STATUS_BAD_REQUEST: u16 = 400;
/// HTTP status of a caller's statement naming a column or table that is not there.
const STATUS_NOT_FOUND: u16 = 404;
/// HTTP status of a caller's statement past the connection's memory budget.
const STATUS_TOO_MANY_REQUESTS: u16 = 429;
/// The mapping type a column of the engine's row ids is declared with: no
/// OpenSearch field type is the engine's `_id` type, so the plugin names it
/// so for the id columns of an edge index.
const ENGINE_ID_TYPE: &str = "infino_engine_id";
/// The engine's id type, which every `_id` column has, and so an edge
/// table's id columns.
const ID_PRECISION: u8 = 38;
const ID_SCALE: i8 = 0;
/// The first whole number past what [`ID_PRECISION`] digits hold: a row id
/// is below it.
const ROW_ID_BOUND: i128 = 10_i128.pow(ID_PRECISION as u32);
/// The columns of an edge table, as the engine's adjacency index reads them:
/// each end of an edge as the table its row is in and the row's `_id`.
const SRC_TABLE: &str = "src_table";
const SRC_ID: &str = "src_id";
const DST_TABLE: &str = "dst_table";
const DST_ID: &str = "dst_id";

thread_local! {
    /// Why the last `infino_open_with` on this thread returned null, as the
    /// error JSON `infino_open_error` hands back. A connection is opened from
    /// one thread, and the handle, not the reason, is what the open returns.
    static OPEN_ERROR: RefCell<Option<String>> = const { RefCell::new(None) };
}

/// Arrow type of a mapped `date`: epoch milliseconds in UTC, which is what
/// OpenSearch stores and what its date queries compare against.
fn date_type() -> DataType {
    DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into()))
}

/// The calendar's units, in the smaller one.
const MILLIS_PER_SECOND: i64 = 1_000;
const NANOS_PER_MILLI: i64 = 1_000_000;
const SECONDS_PER_MINUTE: i64 = 60;
const MINUTES_PER_HOUR: i64 = 60;
const HOURS_PER_DAY: i64 = 24;
const MONTHS_PER_YEAR: i64 = 12;
/// Digits of a year, and of each of the date's other parts.
const YEAR_DIGITS: usize = 4;
const PART_DIGITS: usize = 2;
/// The most digits a fraction of a second carries: nanoseconds.
const FRACTION_DIGITS: usize = 9;
/// The digits of a second's fraction a millisecond column keeps.
const MILLI_DIGITS: usize = 3;
/// The widest zone offset a date may carry, in hours.
const MAX_OFFSET_HOURS: i64 = 18;
/// Days in each month of a common year.
const DAYS_IN_MONTH: [i64; 12] = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];

/// Epoch milliseconds of `s` as OpenSearch's default date format reads it,
/// `strict_date_optional_time||epoch_millis`: a date `yyyy[-MM[-dd]]`, then
/// optionally `T` and `HH[:mm[:ss[.fraction]]]`, any of it (`2026-11-23T`
/// is a day), then optionally a zone (`Z`, `±HH`, `±HHmm`, `±HH:mm`), UTC
/// when there is none; failing that, a whole number of milliseconds. `None`
/// for any other string, which the engine's own parser then reads.
fn default_format_millis(s: &str) -> Option<i64> {
    optional_time_millis(s, true).or_else(|| {
        let digits = s.strip_prefix('-').unwrap_or(s);
        (!digits.is_empty() && digits.bytes().all(|b| b.is_ascii_digit()))
            .then(|| s.parse::<i64>().ok())
            .flatten()
    })
}

/// `strict_date_optional_time` (see [`default_format_millis`]), or with
/// `strict` false `date_optional_time`, which also takes a month, day, hour,
/// minute or second of one digit (`2026-4-30T9:05`), as OpenSearch's lenient
/// form does.
fn optional_time_millis(s: &str, strict: bool) -> Option<i64> {
    let b = s.as_bytes();
    let mut at = 0;
    let part = |at: &mut usize| match strict {
        true => fixed_digits(b, at, PART_DIGITS),
        false => leading_digits(b, at, PART_DIGITS),
    };
    let year = fixed_digits(b, &mut at, YEAR_DIGITS)?;
    let (mut month, mut day) = (1, 1);
    if skip_byte(b, &mut at, b'-') {
        month = part(&mut at)?;
        if skip_byte(b, &mut at, b'-') {
            day = part(&mut at)?;
        }
    }
    let (mut hour, mut minute, mut second, mut nanos) = (0, 0, 0, 0);
    if skip_byte(b, &mut at, b'T') && b.get(at).is_some_and(u8::is_ascii_digit) {
        hour = part(&mut at)?;
        if skip_byte(b, &mut at, b':') {
            minute = part(&mut at)?;
            if skip_byte(b, &mut at, b':') {
                second = part(&mut at)?;
                if skip_byte(b, &mut at, b'.') || skip_byte(b, &mut at, b',') {
                    let start = at;
                    while at < b.len() && b[at].is_ascii_digit() && at - start < FRACTION_DIGITS {
                        at += 1;
                    }
                    if at == start {
                        return None;
                    }
                    let fraction: i64 = s[start..at].parse().ok()?;
                    nanos = fraction * 10i64.pow((FRACTION_DIGITS - (at - start)) as u32);
                }
            }
        }
    }
    let offset_minutes = match b.get(at) {
        None => 0,
        Some(b'Z') => {
            at += 1;
            0
        }
        Some(&sign @ (b'+' | b'-')) => {
            at += 1;
            let hours = fixed_digits(b, &mut at, PART_DIGITS)?;
            let minutes =
                if skip_byte(b, &mut at, b':') || b.get(at).is_some_and(u8::is_ascii_digit) {
                    fixed_digits(b, &mut at, PART_DIGITS)?
                } else {
                    0
                };
            if hours > MAX_OFFSET_HOURS || minutes >= MINUTES_PER_HOUR {
                return None;
            }
            let offset = hours * MINUTES_PER_HOUR + minutes;
            if sign == b'-' {
                -offset
            } else {
                offset
            }
        }
        Some(_) => return None,
    };
    if at != b.len()
        || !(1..=MONTHS_PER_YEAR).contains(&month)
        || day < 1
        || day > days_in_month(year, month)
        || hour >= HOURS_PER_DAY
        || minute >= MINUTES_PER_HOUR
        || second >= SECONDS_PER_MINUTE
    {
        return None;
    }
    let seconds =
        days_from_civil(year, month, day) * HOURS_PER_DAY * MINUTES_PER_HOUR * SECONDS_PER_MINUTE
            + ((hour * MINUTES_PER_HOUR + minute) - offset_minutes) * SECONDS_PER_MINUTE
            + second;
    Some(seconds * MILLIS_PER_SECOND + nanos / NANOS_PER_MILLI)
}

/// Epoch milliseconds of `s` as a whole or fractional number of
/// `unit_millis` (1 for `epoch_millis`, [`MILLIS_PER_SECOND`] for
/// `epoch_second`), signed, as OpenSearch's epoch formats read it; `None`
/// for anything else, and for what OpenSearch refuses: a fraction finer than
/// a nanosecond, and a negative number of seconds under one (`-0`, `-0.5`).
/// The instant is read to nanoseconds and then to milliseconds toward the
/// past, as OpenSearch reads it, and a negative number of seconds keeps its
/// fraction positive, as OpenSearch's `epoch_second` does: `-1.5` is half a
/// second before the epoch, `-500`, and `-1.9995` is `-1`. A negative number
/// of milliseconds is negative throughout: `-123.5` is `-124`.
fn epoch_millis(s: &str, unit_millis: i64) -> Option<i64> {
    let (negative, digits) = match s.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, s),
    };
    let (whole, fraction) = match digits.split_once('.') {
        Some((_, "")) => return None,
        Some((whole, fraction)) => (whole, fraction),
        None => (digits, ""),
    };
    let seconds = unit_millis == MILLIS_PER_SECOND;
    let unit_nanos = i128::from(unit_millis) * i128::from(NANOS_PER_MILLI);
    // The unit's fraction to the nanosecond: nine digits of a second, six of
    // a millisecond.
    let fraction_digits = FRACTION_DIGITS - if seconds { 0 } else { MILLI_DIGITS };
    if whole.is_empty()
        || fraction.len() > fraction_digits
        || !whole.bytes().all(|b| b.is_ascii_digit())
        || !fraction.bytes().all(|b| b.is_ascii_digit())
    {
        return None;
    }
    let whole_nanos = i128::from(whole.parse::<i64>().ok()?) * unit_nanos;
    if negative && seconds && whole_nanos == 0 {
        return None;
    }
    let mut padded = fraction.to_string();
    while padded.len() < fraction_digits {
        padded.push('0');
    }
    let fraction_nanos: i128 = padded.parse().ok()?;
    let nanos = match (negative, seconds) {
        (false, _) => whole_nanos + fraction_nanos,
        (true, true) => fraction_nanos - whole_nanos,
        (true, false) => -(whole_nanos + fraction_nanos),
    };
    i64::try_from(nanos.div_euclid(i128::from(NANOS_PER_MILLI))).ok()
}

/// `value` as a date field of `formats` holds it: the first form that reads
/// it gives its epoch milliseconds, as OpenSearch tries a field's formats in
/// order (a number by its digits, so an `epoch_second` field's `893966296`
/// is a day of 1998, not of 1970). A form this bridge does not read (a
/// custom pattern) does not stop the others: only when none of them reads
/// the value is it left to [`conform`], as a field of the default format.
/// A value no form reads fails its document, as OpenSearch fails it.
fn conform_date(value: Value, formats: &[DateFormat]) -> Result<Value, String> {
    let text = match &value {
        Value::Null => return Ok(value),
        Value::String(s) => s.clone(),
        Value::Number(n) => n.to_string(),
        _ => return Err("not a date".into()),
    };
    let mut custom = false;
    for format in formats {
        let millis = match format {
            DateFormat::OptionalTime { strict } => optional_time_millis(&text, *strict),
            DateFormat::EpochMillis => epoch_millis(&text, 1),
            DateFormat::EpochSecond => epoch_millis(&text, MILLIS_PER_SECOND),
            DateFormat::Other(_) => {
                custom = true;
                None
            }
        };
        if let Some(millis) = millis {
            return Ok(json!(millis));
        }
    }
    if custom {
        return conform(value, &date_type());
    }
    let names: Vec<&str> = formats.iter().map(DateFormat::name).collect();
    Err(format!(
        "failed to parse date field [{text}] with format [{}]",
        names.join("||")
    ))
}

/// Exactly `n` ASCII digits of `b` from `at`, as a number, moving `at` past them.
fn fixed_digits(b: &[u8], at: &mut usize, n: usize) -> Option<i64> {
    let digits = b.get(*at..*at + n)?;
    if !digits.iter().all(u8::is_ascii_digit) {
        return None;
    }
    *at += n;
    Some(
        digits
            .iter()
            .fold(0, |value, d| value * 10 + i64::from(d - b'0')),
    )
}

/// One to `max` ASCII digits of `b` from `at`, as many as there are, as a
/// number, moving `at` past them.
fn leading_digits(b: &[u8], at: &mut usize, max: usize) -> Option<i64> {
    let n = b
        .get(*at..)?
        .iter()
        .take(max)
        .take_while(|d| d.is_ascii_digit())
        .count();
    if n == 0 {
        return None;
    }
    fixed_digits(b, at, n)
}

/// Whether `b` holds `byte` at `at`, moving `at` past it when it does.
fn skip_byte(b: &[u8], at: &mut usize, byte: u8) -> bool {
    let found = b.get(*at) == Some(&byte);
    if found {
        *at += 1;
    }
    found
}

fn days_in_month(year: i64, month: i64) -> i64 {
    let leap = year % 4 == 0 && (year % 100 != 0 || year % 400 == 0);
    DAYS_IN_MONTH[(month - 1) as usize] + i64::from(leap && month == 2)
}

/// Days from 1970-01-01 to the proleptic Gregorian date: Howard Hinnant's
/// `days_from_civil`, with the year starting in March so the leap day is
/// the last of it.
fn days_from_civil(year: i64, month: i64, day: i64) -> i64 {
    let y = if month <= 2 { year - 1 } else { year };
    let era = y.div_euclid(400);
    let year_of_era = y - era * 400;
    let month_from_march = (month + 9) % 12;
    let day_of_year = (153 * month_from_march + 2) / 5 + day - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    era * 146_097 + day_of_era - 719_468
}

unsafe fn cstr<'a>(p: *const c_char) -> Option<&'a str> {
    if p.is_null() {
        return None;
    }
    unsafe { CStr::from_ptr(p) }.to_str().ok()
}

fn out_string(s: String) -> *mut c_char {
    CString::new(s)
        .map(|c| c.into_raw())
        .unwrap_or(ptr::null_mut())
}

fn out_json(v: Value) -> *mut c_char {
    out_string(v.to_string())
}

/// What the C handle points at: the connection plus the table handles it
/// has opened, so a hot path does not reopen the table (and re-list its
/// manifest) on every call. A handle is dropped from the cache when the
/// table's schema changes or the table is dropped.
pub struct Bridge {
    conn: Connection,
    tables: RwLock<HashMap<String, Supertable>>,
    /// Whether the connection forwards to the hosted service (an `http(s)://`
    /// URI) rather than running the engine over storage of its own.
    hosted: bool,
    /// How each table's fields keep their values beyond their column's type
    /// ([`Kept`]: a float at less than a double's precision, a date by its
    /// own format), by path, from the mapping it was last declared from
    /// (`infino_ensure_table`, which the plugin calls before a node first
    /// writes to an index and whenever its mapping moves).
    kept: RwLock<HashMap<String, Arc<HashMap<String, Kept>>>>,
    /// The field each document of a table backing a data stream must hold
    /// one value at, from the mapping's `_data_stream_timestamp` (set with
    /// [`Bridge::kept`]).
    stamps: RwLock<HashMap<String, Arc<str>>>,
}

impl Bridge {
    /// How the fields of `name` keep their values ([`Kept`]); none for a
    /// table no mapping has declared.
    fn kept(&self, name: &str) -> Arc<HashMap<String, Kept>> {
        self.kept
            .read()
            .ok()
            .and_then(|all| all.get(name).cloned())
            .unwrap_or_default()
    }

    /// Record how the fields `mapped` declares keep their values for
    /// `name`, replacing the mapping's earlier ones.
    fn keep_rules(&self, name: &str, mapped: &[MappedField]) {
        let kept: HashMap<String, Kept> = mapped
            .iter()
            .filter_map(|f| f.kept().map(|k| (f.path.clone(), k)))
            .collect();
        if let Ok(mut all) = self.kept.write() {
            all.insert(name.to_string(), Arc::new(kept));
        }
    }

    /// The timestamp field of the data stream `name` backs, if it backs one.
    fn stamp(&self, name: &str) -> Option<Arc<str>> {
        self.stamps
            .read()
            .ok()
            .and_then(|all| all.get(name).cloned())
    }

    /// Record the timestamp field `mapping` requires for `name`, or none.
    fn stamp_rule(&self, name: &str, mapping: &Value) {
        if let Ok(mut all) = self.stamps.write() {
            match stream_timestamp(mapping) {
                Some(field) => all.insert(name.to_string(), field.into()),
                None => all.remove(name),
            };
        }
    }

    fn table(&self, name: &str) -> Result<Supertable, InfinoError> {
        if let Some(t) = self.tables.read().map_err(poisoned)?.get(name) {
            return Ok(t.clone());
        }
        let t = self.conn.open_table(name)?;
        self.tables
            .write()
            .map_err(poisoned)?
            .insert(name.to_string(), t.clone());
        Ok(t)
    }

    fn forget(&self, name: &str) {
        if let Ok(mut tables) = self.tables.write() {
            tables.remove(name);
        }
    }
}

fn poisoned<T>(_: std::sync::PoisonError<T>) -> InfinoError {
    InfinoError::Backend("the bridge's table cache lock is poisoned".into())
}

fn bridge_ref<'a>(h: *mut Bridge) -> Option<&'a Bridge> {
    if h.is_null() {
        None
    } else {
        Some(unsafe { &*h })
    }
}

/// The HTTP status a refused hosted call carries, read off the engine's
/// message: its hosted client keeps the code after `server returned ` for
/// every status, and that message is all that reaches this side.
fn status_in(message: &str) -> Option<u16> {
    let rest = message.split(STATUS_MARKER).nth(1)?;
    rest.get(..STATUS_DIGITS)?.parse().ok()
}

/// The error object every entry point answers with: the message, and the
/// HTTP status when the hosted service refused the call, so the plugin can
/// answer its own caller with the same one (a key refused, an account at a
/// limit) rather than a generic failure.
fn error_value(e: &str) -> Value {
    let mut v = json!({ "error": e });
    if let Some(status) = status_in(e) {
        v["status"] = json!(status);
    }
    v
}

fn err_json(e: &str) -> *mut c_char {
    out_json(error_value(e))
}

// ---------------------------------------------------------------------------
// Connection lifecycle
// ---------------------------------------------------------------------------

/// Whether `uri` names the hosted service rather than storage of the node's own.
fn is_hosted(uri: &str) -> bool {
    uri.starts_with("https://") || uri.starts_with("http://")
}

/// Open `uri`; `api_key` is the bearer key of a hosted URI (null or empty
/// to take none, in which case the engine reads `INFINO_API_KEY`). A hosted
/// connection is used once before it is handed back — the database's tables
/// are listed — so a key the service refuses, or a service that cannot be
/// reached, fails here where the caller can report it rather than on the
/// first write; a database the account does not have yet is registered.
///
/// `cache_dir` is where the engine keeps a local copy of what it reads from
/// the node's own storage. Without one, a table opened after a restart reads
/// its whole file from storage on every query; the process that wrote it
/// held it in memory, so the cost shows only after a restart. A hosted
/// connection keeps nothing locally and takes none.
fn open(uri: &str, api_key: Option<&str>, cache_dir: Option<&str>) -> Result<Bridge, InfinoError> {
    let mut options = ConnectOptions::new();
    if let Some(key) = api_key {
        options = options.with_api_key(key);
    }
    let hosted = is_hosted(uri);
    if let Some(dir) = cache_dir.filter(|_| !hosted) {
        options = options.with_cache_dir(dir);
    }
    let conn = connect_with(uri, options)?;
    if hosted {
        match conn.list_tables() {
            Ok(_) => {}
            Err(InfinoError::NotFound(_)) => conn.create_database()?,
            Err(e) => return Err(e),
        }
    }
    Ok(Bridge {
        conn,
        tables: RwLock::new(HashMap::new()),
        hosted,
        kept: RwLock::new(HashMap::new()),
        stamps: RwLock::new(HashMap::new()),
    })
}

#[no_mangle]
pub unsafe extern "C" fn infino_open(uri: *const c_char) -> *mut Bridge {
    unsafe { infino_open_with(uri, ptr::null(), ptr::null()) }
}

/// Open `uri` with `api_key` and `cache_dir`, each null or empty for none
/// (see `open`). Null when the open failed; the reason is then in
/// `infino_open_error`.
#[no_mangle]
pub unsafe extern "C" fn infino_open_with(
    uri: *const c_char,
    api_key: *const c_char,
    cache_dir: *const c_char,
) -> *mut Bridge {
    catch_unwind(AssertUnwindSafe(|| {
        let uri = unsafe { cstr(uri) }.unwrap_or("memory://");
        let api_key = unsafe { cstr(api_key) }.filter(|k| !k.is_empty());
        let cache_dir = unsafe { cstr(cache_dir) }.filter(|d| !d.is_empty());
        match open(uri, api_key, cache_dir) {
            Ok(bridge) => {
                OPEN_ERROR.with(|slot| *slot.borrow_mut() = None);
                Box::into_raw(Box::new(bridge))
            }
            Err(e) => {
                eprintln!("[capi] connect: {e}");
                OPEN_ERROR.with(|slot| {
                    *slot.borrow_mut() = Some(error_value(&e.to_string()).to_string())
                });
                ptr::null_mut()
            }
        }
    }))
    .unwrap_or(ptr::null_mut())
}

/// Why the last open on this thread returned null, as `{"error", "status"?}`;
/// null when it did not fail. Read once: the slot is cleared.
#[no_mangle]
pub unsafe extern "C" fn infino_open_error() -> *mut c_char {
    match OPEN_ERROR.with(|slot| slot.borrow_mut().take()) {
        Some(json) => out_string(json),
        None => ptr::null_mut(),
    }
}

/// Whether the connection forwards to the hosted service (1) or runs the
/// engine over storage of its own (0).
#[no_mangle]
pub unsafe extern "C" fn infino_hosted(h: *mut Bridge) -> c_int {
    match bridge_ref(h) {
        Some(bridge) if bridge.hosted => 1,
        _ => 0,
    }
}

/// Ask the hosted service at `base_url` for a trial account: `POST /v1/trial`
/// with `database` as the account's first database when given. Answers the
/// service's own response (`api_key`, `database`, `credit_cents`,
/// `console_url`) or `{"error", "status"}` with the status it refused with:
/// 501 when this service offers no trial, 409 when this address has had its
/// trial already. Made without a key, since it is the call that asks for one.
#[no_mangle]
pub unsafe extern "C" fn infino_trial(
    base_url: *const c_char,
    database: *const c_char,
) -> *mut c_char {
    catch_unwind(AssertUnwindSafe(|| {
        let Some(base_url) = (unsafe { cstr(base_url) }) else {
            return err_json("null argument");
        };
        let database = unsafe { cstr(database) }.filter(|d| !d.is_empty());
        let url = format!("{}{TRIAL_PATH}", base_url.trim_end_matches('/'));
        let body = match database {
            Some(database) => json!({ "database": database }),
            None => json!({}),
        };
        let agent = ureq::AgentBuilder::new().timeout(TRIAL_TIMEOUT).build();
        match agent
            .post(&url)
            .set("Accept", "application/json")
            .send_json(body)
        {
            Ok(response) => match response.into_json::<Value>() {
                Ok(v) => out_json(v),
                Err(e) => err_json(&format!("trial: parsing the response: {e}")),
            },
            Err(ureq::Error::Status(code, response)) => {
                let text = response.into_string().unwrap_or_default();
                out_json(json!({ "error": server_message(&text), "status": code }))
            }
            Err(ureq::Error::Transport(t)) => err_json(&format!("trial: {t}")),
        }
    }))
    .unwrap_or(ptr::null_mut())
}

/// The service's message out of an error body: a JSON `{"error": "..."}`
/// (or `message`), else the text as it came.
fn server_message(body: &str) -> String {
    if let Ok(Value::Object(obj)) = serde_json::from_str::<Value>(body) {
        for key in ["error", "message"] {
            if let Some(Value::String(s)) = obj.get(key) {
                return s.clone();
            }
        }
    }
    body.trim().to_string()
}

#[no_mangle]
pub unsafe extern "C" fn infino_close(h: *mut Bridge) {
    if !h.is_null() {
        unsafe { drop(Box::from_raw(h)) };
    }
}

#[no_mangle]
pub unsafe extern "C" fn infino_free_string(s: *mut c_char) {
    if !s.is_null() {
        unsafe { drop(CString::from_raw(s)) };
    }
}

// ---------------------------------------------------------------------------
// Mapping → schema
// ---------------------------------------------------------------------------

/// One field the OpenSearch mapping declares, as the table will hold it.
struct MappedField {
    path: String,
    data_type: DataType,
    fts: bool,
    vector: Option<(usize, Metric)>,
    /// A numeric or date field the mapping indexes, as OpenSearch indexes
    /// every such field for ranges unless it says `"index": false`: the
    /// table gives it a range index.
    range: bool,
    /// How OpenSearch keeps the field's values, when at less than a
    /// double's precision.
    precision: Option<Precision>,
    /// The formats a date field's mapping declares, in order; `None` for the
    /// default format, which [`default_format_millis`] reads.
    date_formats: Option<Vec<DateFormat>>,
}

impl MappedField {
    /// How the field keeps its values beyond its column's type, if it does.
    fn kept(&self) -> Option<Kept> {
        self.precision
            .map(Kept::Precision)
            .or_else(|| self.date_formats.clone().map(Kept::Date))
    }
}

/// How a mapped field's values are kept beyond its column's type, as
/// OpenSearch keeps them.
#[derive(Clone, Debug, PartialEq)]
enum Kept {
    /// A float at less than a double's precision.
    Precision(Precision),
    /// A date read by the formats its mapping declares, in order.
    Date(Vec<DateFormat>),
}

impl Kept {
    fn precision(&self) -> Option<Precision> {
        match self {
            Self::Precision(p) => Some(*p),
            Self::Date(_) => None,
        }
    }
}

/// One of the formats a date field's mapping declares (`format`, its forms
/// joined by `||`).
#[derive(Clone, Debug, PartialEq)]
enum DateFormat {
    /// `strict_date_optional_time`, or with `strict` false the lenient
    /// `date_optional_time`, as [`optional_time_millis`] reads them.
    OptionalTime { strict: bool },
    /// `epoch_millis`: milliseconds since the epoch, a fraction allowed.
    EpochMillis,
    /// `epoch_second`: seconds since the epoch, a fraction allowed.
    EpochSecond,
    /// Any other form (a custom pattern, a named format this bridge does not
    /// read); a value reaching it is read as a field of the default format.
    Other(String),
}

impl DateFormat {
    /// The forms of a mapping's `format`, in order.
    fn parse_all(format: &str) -> Vec<Self> {
        format
            .split("||")
            .map(str::trim)
            .filter(|f| !f.is_empty())
            .map(|f| match f {
                "strict_date_optional_time" => Self::OptionalTime { strict: true },
                "date_optional_time" => Self::OptionalTime { strict: false },
                "epoch_millis" => Self::EpochMillis,
                "epoch_second" => Self::EpochSecond,
                other => Self::Other(other.to_string()),
            })
            .collect()
    }

    /// The form's name, as the mapping gives it.
    fn name(&self) -> &str {
        match self {
            Self::OptionalTime { strict: true } => "strict_date_optional_time",
            Self::OptionalTime { strict: false } => "date_optional_time",
            Self::EpochMillis => "epoch_millis",
            Self::EpochSecond => "epoch_second",
            Self::Other(f) => f,
        }
    }
}

/// How OpenSearch keeps a mapped floating-point field's values when at less
/// than a double's precision: what its aggregations, sorts and range
/// queries read, while `_source` keeps each value as sent. The field's
/// column holds each value as OpenSearch reads it back ([`kept_as`]), so a
/// `half_float` price of 53.98 sums as the 53.96875 OpenSearch sums.
#[derive(Clone, Copy, Debug, PartialEq)]
enum Precision {
    /// `float`: a 32-bit float.
    Single,
    /// `half_float`: a 16-bit float, rounded from the 32-bit one OpenSearch
    /// parses the value as.
    Half,
    /// `scaled_float`: the value times its scaling factor, rounded to a long.
    Scaled(f64),
}

impl Precision {
    /// The precision a mapped field of type `ty` (with its definition `def`)
    /// keeps its values at; `None` for a type kept whole.
    fn of(ty: &str, def: &Map<String, Value>) -> Option<Self> {
        match ty {
            "float" => Some(Self::Single),
            "half_float" => Some(Self::Half),
            "scaled_float" => def
                .get(SCALING_FACTOR)
                .and_then(|f| f.as_f64().or_else(|| f.as_str()?.trim().parse().ok()))
                .map(Self::Scaled),
            _ => None,
        }
    }

    /// The mapping type, as OpenSearch names it in a refusal.
    fn type_name(self) -> &'static str {
        match self {
            Self::Single => "float",
            Self::Half => "half_float",
            Self::Scaled(_) => "scaled_float",
        }
    }
}

/// The mapping key of a `scaled_float`'s scaling factor.
const SCALING_FACTOR: &str = "scaling_factor";
/// What Java's `Math.round` adds before it floors, as OpenSearch rounds a
/// `scaled_float`: a half rounds up, toward positive infinity.
const ROUND_HALF_UP: f64 = 0.5;
/// The layout of a 32-bit float (Lucene's `HalfFloatPoint` reads it): its
/// mantissa's bits and mask, the implicit leading bit, its exponent's bias
/// and the exponent of an infinity or a NaN.
const F32_MANTISSA_BITS: u32 = 23;
const F32_MANTISSA_MASK: u32 = (1 << F32_MANTISSA_BITS) - 1;
const F32_IMPLICIT_BIT: u32 = 1 << F32_MANTISSA_BITS;
const F32_EXP_BIAS: i32 = 127;
const F32_EXP_MAX: i32 = 0xff;
/// The same of a 16-bit float, and where its sign bit sits.
const F16_MANTISSA_BITS: u32 = 10;
const F16_MANTISSA_MASK: u32 = (1 << F16_MANTISSA_BITS) - 1;
const F16_EXP_BIAS: i32 = 15;
const F16_EXP_MAX: i32 = 0x1f;
const F16_SIGN_SHIFT: u32 = 15;
/// Where a 32-bit float's sign bit sits, and a shift past a word's bits.
const F32_SIGN_SHIFT: u32 = 31;
const WORD_BITS: i32 = 32;

/// `i` divided by 2^`shift`, rounded to the nearest, ties to even
/// (Lucene's `HalfFloatPoint.roundShift`).
fn round_shift(i: u32, shift: u32) -> u32 {
    let i = i + (1 << (shift - 1));
    let i = i - ((i >> shift) & 1);
    i >> shift
}

/// The bits of the 16-bit float nearest `v`, as Lucene encodes a
/// `half_float` (`HalfFloatPoint.halfFloatToShortBits`): ties to even, a
/// 32-bit subnormal to zero, a value past the largest half to infinity.
fn half_bits(v: f32) -> u16 {
    let bits = v.to_bits();
    let sign = bits >> F32_SIGN_SHIFT;
    let mut exp = ((bits >> F32_MANTISSA_BITS) as i32) & F32_EXP_MAX;
    let mut mantissa = bits & F32_MANTISSA_MASK;
    if exp == F32_EXP_MAX {
        // An infinity or a NaN stays one.
        exp = F16_EXP_MAX;
        mantissa >>= F32_MANTISSA_BITS - F16_MANTISSA_BITS;
    } else if exp == 0 {
        // Even the largest 32-bit subnormal is below the smallest half.
        mantissa = 0;
    } else {
        exp = exp - F32_EXP_BIAS + F16_EXP_BIAS;
        if exp >= F16_EXP_MAX {
            exp = F16_EXP_MAX;
            mantissa = 0;
        } else if exp <= 0 {
            // A half subnormal.
            let shift = F32_MANTISSA_BITS as i32 - F16_MANTISSA_BITS as i32 - exp + 1;
            if shift >= WORD_BITS {
                exp = 0;
                mantissa = 0;
            } else {
                mantissa = round_shift(mantissa | F32_IMPLICIT_BIT, shift as u32);
                exp = (mantissa >> F16_MANTISSA_BITS) as i32;
                mantissa &= F16_MANTISSA_MASK;
            }
        } else {
            // A carry out of the mantissa moves the exponent up.
            mantissa = round_shift(
                ((exp as u32) << F32_MANTISSA_BITS) | mantissa,
                F32_MANTISSA_BITS - F16_MANTISSA_BITS,
            );
            exp = (mantissa >> F16_MANTISSA_BITS) as i32;
            mantissa &= F16_MANTISSA_MASK;
        }
    }
    ((sign << F16_SIGN_SHIFT) | ((exp as u32) << F16_MANTISSA_BITS) | mantissa) as u16
}

/// The 32-bit float a 16-bit float's `bits` hold, exactly
/// (`HalfFloatPoint.shortBitsToHalfFloat`).
fn half_value(bits: u16) -> f32 {
    let bits = u32::from(bits);
    let sign = bits >> F16_SIGN_SHIFT;
    let mut exp = ((bits >> F16_MANTISSA_BITS) as i32) & F16_EXP_MAX;
    let mut mantissa = bits & F16_MANTISSA_MASK;
    if exp == F16_EXP_MAX {
        exp = F32_EXP_MAX;
        mantissa <<= F32_MANTISSA_BITS - F16_MANTISSA_BITS;
    } else if mantissa != 0 || exp != 0 {
        if exp == 0 {
            // A half subnormal is a normal 32-bit float.
            let shift =
                mantissa.leading_zeros() as i32 - (WORD_BITS - F16_MANTISSA_BITS as i32 - 1);
            mantissa = (mantissa << shift) & F16_MANTISSA_MASK;
            exp = exp - shift + 1;
        }
        exp = exp + F32_EXP_BIAS - F16_EXP_BIAS;
        mantissa <<= F32_MANTISSA_BITS - F16_MANTISSA_BITS;
    }
    f32::from_bits((sign << F32_SIGN_SHIFT) | ((exp as u32) << F32_MANTISSA_BITS) | mantissa)
}

/// `value` as OpenSearch keeps a field of `precision` and reads it back, or
/// why it refuses it: a value the field's type holds only as an infinity is
/// refused, as OpenSearch refuses it.
fn kept_as(value: f64, precision: Precision) -> Result<f64, String> {
    let kept = match precision {
        Precision::Single => f64::from(value as f32),
        Precision::Half => f64::from(half_value(half_bits(value as f32))),
        // OpenSearch reads the long back divided by the factor.
        Precision::Scaled(factor) => (value * factor + ROUND_HALF_UP).floor() / factor,
    };
    if kept.is_finite() {
        Ok(kept)
    } else {
        Err(format!(
            "[{}] supports only finite values, but got [{value}]",
            precision.type_name()
        ))
    }
}

/// A conformed value of a field kept at `precision`, as OpenSearch keeps it
/// ([`kept_as`]); a null stays null.
fn keep(value: Value, precision: Option<Precision>) -> Result<Value, String> {
    match (precision, value.as_f64()) {
        (Some(precision), Some(number)) => Ok(json!(kept_as(number, precision)?)),
        _ => Ok(value),
    }
}

/// The mapping meta-field that makes an index back a data stream.
const STREAM_TIMESTAMP: &str = "_data_stream_timestamp";
/// The timestamp field a data stream names when its mapping names none.
const DEFAULT_STREAM_TIMESTAMP: &str = "@timestamp";

/// The field a data stream's documents must hold one value at, when
/// `mapping` enables [`STREAM_TIMESTAMP`]; read wherever [`properties_of`]
/// reads the properties.
fn stream_timestamp(mapping: &Value) -> Option<String> {
    let m = mapping.as_object()?;
    if let Some(meta) = m.get(STREAM_TIMESTAMP) {
        if meta.get("enabled") != Some(&Value::Bool(true)) {
            return None;
        }
        let field = meta
            .pointer("/timestamp_field/name")
            .and_then(Value::as_str);
        return Some(field.unwrap_or(DEFAULT_STREAM_TIMESTAMP).to_string());
    }
    ["_doc", "mappings"]
        .iter()
        .find_map(|k| m.get(*k))
        .and_then(stream_timestamp)
}

/// Stock's refusal of a data-stream document without one timestamp; the
/// plugin answers a reason that starts so as stock's parse failure.
fn no_timestamp(field: &str) -> String {
    format!("documents must contain a single-valued timestamp field '{field}' of date type")
}

/// The `properties` object of a mapping, whether the caller passed the
/// mapping source itself, a `{"_doc": {...}}` wrapper, or `{"properties": ...}`.
fn properties_of(mapping: &Value) -> Option<&Map<String, Value>> {
    let m = mapping.as_object()?;
    if let Some(p) = m.get("properties").and_then(|p| p.as_object()) {
        return Some(p);
    }
    if let Some(doc) = m.get("_doc") {
        return properties_of(doc);
    }
    if let Some(mappings) = m.get("mappings") {
        return properties_of(mappings);
    }
    None
}

fn vector_metric(def: &Map<String, Value>) -> Metric {
    let space = def
        .get("method")
        .and_then(|m| m.get("space_type"))
        .and_then(|s| s.as_str())
        .or_else(|| def.get("space_type").and_then(|s| s.as_str()))
        .unwrap_or("cosinesimil");
    match space {
        "l2" => Metric::L2Sq,
        "innerproduct" => Metric::NegDot,
        _ => Metric::Cosine,
    }
}

/// Walk a `properties` object, emitting one field per mapped leaf with its
/// dot path. Objects recurse; multi-fields (`fields`) are not columns, the
/// plugin maps a keyword multi-field onto its field's column. Types the engine has no
/// column for (geo, binary, alias, …) are left to `source`.
fn walk_properties(props: &Map<String, Value>, prefix: &str, out: &mut Vec<MappedField>) {
    for (name, def) in props {
        let Some(def) = def.as_object() else { continue };
        let path = if prefix.is_empty() {
            name.clone()
        } else {
            format!("{prefix}.{name}")
        };
        let ty = def.get("type").and_then(|t| t.as_str());
        let indexed = def.get("index").and_then(|i| i.as_bool()) != Some(false);
        let ranged = matches!(
            ty,
            Some("date")
                | Some("date_nanos")
                | Some("long")
                | Some("integer")
                | Some("short")
                | Some("byte")
                | Some("unsigned_long")
                | Some("double")
                | Some("float")
                | Some("half_float")
                | Some("scaled_float")
        );
        let (data_type, fts, vector) = match ty {
            Some("date") | Some("date_nanos") => (date_type(), false, None),
            Some("keyword")
            | Some("ip")
            | Some("constant_keyword")
            | Some("wildcard")
            | Some("version") => (DataType::LargeUtf8, false, None),
            Some("text") | Some("match_only_text") | Some("search_as_you_type") => {
                (DataType::LargeUtf8, true, None)
            }
            Some("long")
            | Some("integer")
            | Some("short")
            | Some("byte")
            | Some("unsigned_long") => (DataType::Int64, false, None),
            Some("double") | Some("float") | Some("half_float") | Some("scaled_float") => {
                (DataType::Float64, false, None)
            }
            Some("boolean") => (DataType::Boolean, false, None),
            Some(ENGINE_ID_TYPE) => (DataType::Decimal128(ID_PRECISION, ID_SCALE), false, None),
            Some("knn_vector") => {
                let dim = def.get("dimension").and_then(|d| d.as_u64()).unwrap_or(0) as usize;
                if dim == 0 {
                    continue;
                }
                let field = Arc::new(Field::new("item", DataType::Float32, true));
                (
                    DataType::FixedSizeList(field, dim as i32),
                    false,
                    Some((dim, vector_metric(def))),
                )
            }
            Some("object") | Some("nested") | None => {
                if let Some(p) = def.get("properties").and_then(|p| p.as_object()) {
                    walk_properties(p, &path, out);
                }
                continue;
            }
            Some(_) => continue,
        };
        let date_formats = matches!(ty, Some("date") | Some("date_nanos"))
            .then(|| def.get("format").and_then(Value::as_str))
            .flatten()
            .map(DateFormat::parse_all);
        out.push(MappedField {
            path,
            data_type,
            fts,
            vector,
            range: ranged && indexed,
            precision: ty.and_then(|ty| Precision::of(ty, def)),
            date_formats,
        });
    }
}

fn mapped_fields(mapping: &Value) -> Vec<MappedField> {
    let mut out = Vec::new();
    if let Some(props) = properties_of(mapping) {
        walk_properties(props, "", &mut out);
    }
    out
}

/// Create the table from the mapping, or bring an existing table's schema up
/// to it: a mapped field the table lacks is added with its mapped type, a
/// `text` one with the text index it would have had at creation (the
/// mapping grows as documents arrive, a dynamic field included). A mapped
/// field whose type disagrees with the column it already has, or a `text`
/// field the table already holds without a text index (an index is part of
/// a column's definition, fixed when the column is added), is a conflict the
/// mapping cannot express over this table: the call fails naming it, so the
/// index answers with the reason instead of serving the column as something
/// it is not.
fn ensure_table(bridge: &Bridge, name: &str, mapping: &Value) -> Result<Value, String> {
    let mut mapped = mapped_fields(mapping);
    // A field named like a reserved column is served from the document's
    // source, not given a column of its own.
    mapped.retain(|f| !matches!(f.path.as_str(), OSID | SOURCE | SEQ));
    bridge.keep_rules(name, &mapped);
    bridge.stamp_rule(name, mapping);
    let conn = &bridge.conn;
    // The cached handle, if any, predates whatever this changes.
    bridge.forget(name);
    match conn.open_table(name) {
        Ok(table) => {
            let schema = table.schema();
            let table_schema = conn.schema(name).map_err(|e| e.to_string())?;
            // The text index a field added now gets: the one creation gives a
            // text field, the engine's defaults (as the id column's index
            // carries them) with positions.
            let text_index = table_schema
                .fields()
                .iter()
                .find(|c| c.name == OSID)
                .and_then(|c| match &c.index {
                    Some(ColumnIndex::Fts {
                        stopwords,
                        stemmer,
                        stored,
                        bm25,
                        ..
                    }) => Some(ColumnIndex::Fts {
                        stopwords: *stopwords,
                        stemmer: *stemmer,
                        positions: true,
                        stored: *stored,
                        bm25: *bm25,
                    }),
                    _ => None,
                });
            let mut conflicts: Vec<String> = Vec::new();
            for f in &mapped {
                let present = schema.field_with_name(&f.path).is_ok();
                let indexed = table_schema
                    .fields()
                    .iter()
                    .any(|c| c.name == f.path && matches!(c.index, Some(ColumnIndex::Fts { .. })));
                // A column's index is fixed when the column is added, so a text field
                // the table holds without one cannot be served as text.
                if f.fts && present && !indexed {
                    conflicts.push(format!(
                        "[{}] is mapped as text but the table holds it without a text index",
                        f.path
                    ));
                }
                if f.fts && !present && text_index.is_none() {
                    conflicts.push(format!(
                        "[{}] is mapped as text but the table has no text index to model one on",
                        f.path
                    ));
                }
                let Ok(existing) = schema.field_with_name(&f.path) else {
                    continue;
                };
                let have = type_name(existing.data_type());
                let want = type_name(&f.data_type);
                if have != want {
                    conflicts.push(format!(
                        "[{}] is mapped as {want} but the table holds it as {have}",
                        f.path
                    ));
                }
            }
            if !conflicts.is_empty() {
                return Err(format!(
                    "the mapping of {name} cannot be applied to its table: {}; recreate the index with the mapping it needs",
                    conflicts.join("; ")
                ));
            }
            let mut patches: Vec<FieldPatch> = mapped
                .iter()
                .filter(|f| schema.field_with_name(&f.path).is_err())
                .map(|f| {
                    let patch = FieldPatch::named(f.path.clone())
                        .with_type(f.data_type.clone())
                        .with_nullable(true);
                    if f.range {
                        patch.with_index(ColumnIndex::Range)
                    } else {
                        match (&text_index, f.fts) {
                            (Some(index), true) => patch.with_index(index.clone()),
                            _ => patch,
                        }
                    }
                })
                .collect();
            // A table made before the plugin kept sequence numbers gains the
            // column; its rows read as written at 0.
            if schema.field_with_name(SEQ).is_err() {
                patches.push(
                    FieldPatch::named(SEQ.to_string())
                        .with_type(DataType::Int64)
                        .with_nullable(true),
                );
            }
            let added = patches.len();
            if added > 0 {
                table
                    .apply_schema(&SchemaPatch::new(patches), None)
                    .map_err(|e| e.to_string())?;
            }
            Ok(json!({ "created": false, "added": added }))
        }
        Err(InfinoError::NotFound(_)) => {
            let mut fields = vec![
                Field::new(OSID, DataType::LargeUtf8, false),
                Field::new(SOURCE, DataType::LargeUtf8, true),
                Field::new(SEQ, DataType::Int64, true),
            ];
            // The id column carries an FTS index: a GET by id is then a term
            // lookup (`exact_match`) instead of a scan of the column, and the
            // manifest's per-superfile term bloom skips the files that cannot
            // hold the id. Scalar columns have only min/max per superfile.
            let mut spec = IndexSpec::new().fts(FtsField::new(OSID.to_string()));
            for f in &mapped {
                fields.push(Field::new(&f.path, f.data_type.clone(), true));
                if f.fts {
                    // Positions let a quoted phrase match adjacent terms in
                    // order (match_phrase, quoted atoms of a query_string).
                    spec = spec.fts(FtsField::new(f.path.clone()).positions(true));
                }
                if let Some((dim, metric)) = f.vector {
                    spec = spec.vector(f.path.clone(), dim, metric);
                }
                if f.range {
                    spec = spec.range(f.path.clone());
                }
            }
            conn.create_table(name, Arc::new(Schema::new(fields)), spec)
                .map(|_| json!({ "created": true, "added": mapped.len() }))
                .or_else(|e| match e {
                    InfinoError::AlreadyExists(_) => Ok(json!({ "created": false, "added": 0 })),
                    other => Err(other.to_string()),
                })
        }
        Err(e) => Err(e.to_string()),
    }
}

/// Create the table from an OpenSearch mapping, or add the mapped fields an
/// existing table lacks. Returns `{"created":bool,"added":n}` or `{"error":..}`.
#[no_mangle]
pub unsafe extern "C" fn infino_ensure_table(
    h: *mut Bridge,
    name: *const c_char,
    mapping_json: *const c_char,
) -> *mut c_char {
    catch_unwind(AssertUnwindSafe(|| {
        let (Some(conn), Some(name), Some(mapping)) =
            (bridge_ref(h), unsafe { cstr(name) }, unsafe {
                cstr(mapping_json)
            })
        else {
            return err_json("null argument");
        };
        let mapping: Value = serde_json::from_str(mapping).unwrap_or(Value::Null);
        match ensure_table(conn, name, &mapping) {
            Ok(v) => out_json(v),
            Err(e) => err_json(&e),
        }
    }))
    .unwrap_or(ptr::null_mut())
}

fn type_name(dt: &DataType) -> &'static str {
    match dt {
        DataType::Int64 | DataType::Int32 | DataType::Int16 | DataType::Int8 => "i64",
        DataType::UInt64 | DataType::UInt32 | DataType::UInt16 | DataType::UInt8 => "i64",
        DataType::Float64 | DataType::Float32 => "f64",
        DataType::Boolean => "bool",
        DataType::LargeUtf8 | DataType::Utf8 | DataType::Utf8View => "str",
        DataType::Timestamp(..) => "ts",
        DataType::Date32 | DataType::Date64 => "ts",
        DataType::FixedSizeList(..) => "vec",
        DataType::List(..) | DataType::LargeList(..) => "list",
        _ => "other",
    }
}

/// Columns of a table as JSON `[{"name","type","elem","fts","vector"}]`,
/// type in {i64,f64,bool,str,ts,vec,list,other}, `elem` a list column's
/// element type (null for any other). `""` if the table is absent.
#[no_mangle]
pub unsafe extern "C" fn infino_columns(h: *mut Bridge, name: *const c_char) -> *mut c_char {
    catch_unwind(AssertUnwindSafe(|| {
        let (Some(conn), Some(name)) = (bridge_ref(h), unsafe { cstr(name) }) else {
            return ptr::null_mut();
        };
        match conn.conn.schema(name) {
            Ok(schema) => {
                let cols: Vec<Value> = schema
                    .fields()
                    .iter()
                    .map(|f| {
                        // A list column's element type: what its values compare as.
                        let elem = match &f.data_type {
                            DataType::List(item) | DataType::LargeList(item) => {
                                Some(type_name(item.data_type()))
                            }
                            _ => None,
                        };
                        json!({
                            "name": f.name,
                            "type": type_name(&f.data_type),
                            "elem": elem,
                            "fts": matches!(f.index, Some(ColumnIndex::Fts { .. })),
                            "vector": matches!(f.index, Some(ColumnIndex::Vector { .. })),
                        })
                    })
                    .collect();
                out_json(Value::Array(cols))
            }
            Err(_) => out_string(String::new()),
        }
    }))
    .unwrap_or(ptr::null_mut())
}

// ---------------------------------------------------------------------------
// Documents → rows
// ---------------------------------------------------------------------------

/// Flatten a document to dot paths, the way the engine stores it: nested
/// objects become `a.b` keys, everything else stays a leaf value. An empty
/// object carries nothing and is dropped. An array of objects (an `object`
/// or `nested` field holding several) is the multi-valued form of each
/// member path, as OpenSearch indexes it: `a: [{b: 1}, {b: 2}]` becomes
/// `a.b: [1, 2]`, so a declared `a.b` column takes the values rather than
/// the objects; scalars beside the objects stay under the array's own path.
fn flatten(obj: &Map<String, Value>, prefix: &str, out: &mut Map<String, Value>) {
    for (k, v) in obj {
        let path = if prefix.is_empty() {
            k.clone()
        } else {
            format!("{prefix}.{k}")
        };
        match v {
            Value::Object(inner) if !inner.is_empty() => flatten(inner, &path, out),
            Value::Object(_) => {}
            Value::Array(items) if items.iter().any(Value::is_object) => {
                let mut per_path: Vec<(String, Vec<Value>)> = Vec::new();
                let mut scalars = Vec::new();
                for item in items {
                    match item {
                        Value::Object(inner) => {
                            let mut member = Map::new();
                            flatten(inner, &path, &mut member);
                            for (member_path, member_value) in member {
                                let values = match member_value {
                                    Value::Array(list) => list,
                                    other => vec![other],
                                };
                                match per_path.iter_mut().find(|(p, _)| *p == member_path) {
                                    Some((_, existing)) => existing.extend(values),
                                    None => per_path.push((member_path, values)),
                                }
                            }
                        }
                        other => scalars.push(other.clone()),
                    }
                }
                for (member_path, values) in per_path {
                    out.insert(member_path, Value::Array(values));
                }
                if !scalars.is_empty() {
                    out.insert(path, Value::Array(scalars));
                }
            }
            other => {
                out.insert(path, other.clone());
            }
        }
    }
}

/// Coerce one value to what a declared column can hold, OpenSearch-style
/// (`coerce: true`): numeric strings become numbers, scalars become strings
/// for string columns, multi-valued strings are joined. Anything the column
/// cannot take fails the document with a reason.
fn conform(value: Value, dt: &DataType) -> Result<Value, String> {
    if value.is_null() {
        return Ok(value);
    }
    match dt {
        DataType::Int64 => match &value {
            Value::Number(n) if n.is_i64() => Ok(value),
            // An `unsigned_long` past the largest signed 64-bit integer.
            Value::Number(n) if n.is_u64() => Err(format!(
                "{n} is past the largest value an integer column holds"
            )),
            Value::Number(n) => match n.as_f64() {
                Some(f) if f.fract() != 0.0 => {
                    Err("a fractional number for an integer field".into())
                }
                Some(f) if f.abs() < WHOLE_FLOAT_LIMIT => Ok(json!(f as i64)),
                _ => Err(format!(
                    "{n} is past the whole numbers a float holds exactly, so it is no integer"
                )),
            },
            Value::String(s) => s
                .trim()
                .parse::<i64>()
                .map(|i| json!(i))
                .map_err(|_| format!("'{s}' is not an integer")),
            Value::Bool(_) => Err("a boolean for an integer field".into()),
            Value::Array(_) => Err("an array for an integer field".into()),
            Value::Object(_) => Err("an object for an integer field".into()),
            Value::Null => Ok(value),
        },
        DataType::Float64 => match &value {
            Value::Number(_) => Ok(value),
            Value::String(s) => s
                .trim()
                .parse::<f64>()
                .map(|f| json!(f))
                .map_err(|_| format!("'{s}' is not a number")),
            Value::Bool(_) => Err("a boolean for a float field".into()),
            Value::Array(_) => Err("an array for a float field".into()),
            Value::Object(_) => Err("an object for a float field".into()),
            Value::Null => Ok(value),
        },
        DataType::Boolean => match &value {
            Value::Bool(_) => Ok(value),
            Value::String(s) => match s.as_str() {
                "true" => Ok(json!(true)),
                "false" => Ok(json!(false)),
                _ => Err(format!("'{s}' is not a boolean")),
            },
            _ => Err("not a boolean".into()),
        },
        DataType::Timestamp(..) => match &value {
            // A string in OpenSearch's default date format is read as it
            // reads it; any other is left to the engine's parser. An
            // epoch-millisecond integer is taken as is.
            Value::String(s) => Ok(default_format_millis(s).map_or(value.clone(), |ms| json!(ms))),
            Value::Number(n) if n.is_i64() || n.is_u64() => Ok(value),
            _ => Err("not a date".into()),
        },
        DataType::LargeUtf8 | DataType::Utf8 | DataType::Utf8View => match &value {
            Value::String(_) => Ok(value),
            Value::Number(n) => Ok(Value::String(n.to_string())),
            Value::Bool(b) => Ok(Value::String(b.to_string())),
            Value::Array(items) => {
                let mut parts = Vec::with_capacity(items.len());
                string_values(items, &mut parts)?;
                Ok(Value::String(parts.join(MULTI_VALUE_SEPARATOR)))
            }
            Value::Object(_) => Err("an object for a string field".into()),
            Value::Null => Ok(value),
        },
        // A row id: a whole number, as digits in a string where it is past
        // what a JSON number holds exactly (as a SQL read gives an `_id`),
        // and no wider than the column's precision.
        DataType::Decimal128(ID_PRECISION, ID_SCALE) => match &value {
            Value::Number(n) if n.is_u64() => Ok(value),
            Value::String(s)
                if !s.trim().is_empty() && s.trim().bytes().all(|b| b.is_ascii_digit()) =>
            {
                match s.trim().parse::<i128>() {
                    Ok(id) if id < ROW_ID_BOUND => Ok(Value::String(id.to_string())),
                    _ => Err(format!(
                        "not a row id: '{}' has more than the {ID_PRECISION} digits a row's `_id` holds",
                        s.trim()
                    )),
                }
            }
            _ => Err("not a row id: a row's `_id` is a whole number, its digits".into()),
        },
        DataType::FixedSizeList(_, dim) => match &value {
            Value::Array(items)
                if items.len() == *dim as usize && items.iter().all(|x| x.is_number()) =>
            {
                Ok(value)
            }
            Value::Array(items) => Err(format!(
                "a vector of {} values for a {dim}-dimensional field",
                items.len()
            )),
            _ => Err("not a vector".into()),
        },
        _ => Ok(value),
    }
}

/// The type of the list column a field of type `dt` keeps several values in
/// ([`LIST_SUFFIX`]): a list of `dt` itself, for the scalar types a document
/// may give several of; `None` for any other type.
fn list_type(dt: &DataType) -> Option<DataType> {
    match dt {
        DataType::Int64
        | DataType::Float64
        | DataType::Boolean
        | DataType::Timestamp(..)
        | DataType::LargeUtf8
        | DataType::Utf8
        | DataType::Utf8View => Some(DataType::List(Arc::new(Field::new(
            LIST_ITEM,
            dt.clone(),
            true,
        )))),
        _ => None,
    }
}

/// The values an array gives a field, in order, onto `out`, as OpenSearch
/// indexes them: an array inside it is read through and a null is no value.
fn array_values(items: Vec<Value>, out: &mut Vec<Value>) {
    for item in items {
        match item {
            Value::Array(inner) => array_values(inner, out),
            Value::Null => {}
            other => out.push(other),
        }
    }
}

/// The values of a multi-valued string field, in order, onto `out`: an array
/// inside the array is read through, as OpenSearch flattens arrays of
/// arrays (`[["@timestamp", "desc"]]` is two values), and a null is no value.
fn string_values(items: &[Value], out: &mut Vec<String>) -> Result<(), String> {
    for item in items {
        match item {
            Value::String(s) => out.push(s.clone()),
            Value::Number(n) => out.push(n.to_string()),
            Value::Bool(b) => out.push(b.to_string()),
            Value::Array(inner) => string_values(inner, out)?,
            Value::Null => {}
            Value::Object(_) => return Err("an array of objects for a string field".into()),
        }
    }
    Ok(())
}

/// The kinds one path carries across a batch.
#[derive(Default)]
struct Seen {
    bool: bool,
    int: bool,
    float: bool,
    str: bool,
    list: bool,
}

impl Seen {
    fn note(&mut self, v: &Value) {
        match v {
            Value::Bool(_) => self.bool = true,
            Value::Number(n) if n.is_i64() || n.is_u64() => self.int = true,
            Value::Number(_) => self.float = true,
            Value::String(_) => self.str = true,
            Value::Array(items) => {
                self.list = true;
                for item in items {
                    if !item.is_array() {
                        self.note(item);
                    }
                }
            }
            _ => {}
        }
    }

    /// Scalars of more than one kind: strings win over numbers and booleans,
    /// floats over ints, since the wider column holds every value.
    fn to_string(&self) -> bool {
        (self.str || self.bool) && (self.int || self.float || (self.str && self.bool))
    }

    fn to_float(&self) -> bool {
        !self.to_string() && self.int && self.float
    }
}

fn widen_scalar(v: &Value, to_string: bool, to_float: bool) -> Value {
    match v {
        Value::Number(n) if to_string => Value::String(n.to_string()),
        Value::Bool(b) if to_string => Value::String(b.to_string()),
        Value::Number(n) if to_float => json!(n.as_f64().unwrap_or(0.0)),
        other => other.clone(),
    }
}

/// A path no column declares takes its type from the documents. Within one
/// batch the documents may disagree (`"200"` in one, `200` in the next; a
/// list in one, a scalar in the next), which the engine refuses for the
/// whole batch. So the path is widened before the append: strings win over
/// numbers and booleans, floats over ints, and if any document carries a
/// list every scalar becomes a one-element list, which is what OpenSearch
/// means by a multi-valued field anyway.
fn unify_new_paths(rows: &mut [Map<String, Value>], schema: &Schema) {
    let mut seen: HashMap<String, Seen> = HashMap::new();
    for row in rows.iter() {
        for (k, v) in row {
            if schema.field_with_name(k).is_ok() {
                continue;
            }
            seen.entry(k.clone()).or_default().note(v);
        }
    }
    for (path, s) in seen {
        let (to_string, to_float) = (s.to_string(), s.to_float());
        if !to_string && !to_float && !s.list {
            continue;
        }
        for row in rows.iter_mut() {
            let Some(v) = row.get_mut(&path) else {
                continue;
            };
            *v = match &*v {
                Value::Null => Value::Null,
                Value::Array(items) => Value::Array(
                    items
                        .iter()
                        .map(|i| widen_scalar(i, to_string, to_float))
                        .collect(),
                ),
                scalar if s.list => Value::Array(vec![widen_scalar(scalar, to_string, to_float)]),
                scalar => widen_scalar(scalar, to_string, to_float),
            };
        }
    }
}

/// Check one document against the declared columns without indexing it:
/// `""` when it conforms, else the reason it would fail. The engine returns
/// the same verdict `infino_index_batch` would report for the document, so a
/// caller can refuse it synchronously (an OpenSearch bulk item error) rather
/// than learn after the batch commits.
#[no_mangle]
pub unsafe extern "C" fn infino_check_doc(
    h: *mut Bridge,
    name: *const c_char,
    source: *const c_char,
) -> *mut c_char {
    catch_unwind(AssertUnwindSafe(|| {
        let (Some(conn), Some(name), Some(source)) =
            (bridge_ref(h), unsafe { cstr(name) }, unsafe {
                cstr(source)
            })
        else {
            return out_string("null argument".into());
        };
        let table = match conn.table(name) {
            Ok(t) => t,
            Err(e) => return out_string(e.to_string()),
        };
        let schema = table.schema();
        let stamp = conn.stamp(name);
        out_string(
            match conform_doc(source, &schema, &conn.kept(name), stamp.as_deref()) {
                Ok(_) => String::new(),
                Err(why) => why,
            },
        )
    }))
    .unwrap_or(ptr::null_mut())
}

/// Parse, flatten and conform one document to the declared columns. The
/// row it returns is what the engine is given. A declared field the document
/// gives several values goes to its list column ([`LIST_SUFFIX`]), each
/// value conformed as the field's own would be; one value in an array is
/// that value, as OpenSearch reads it. A field `kept` names holds each value
/// as OpenSearch keeps it ([`Kept`]): a float at its precision, a date read
/// by its own format. With `stamp`, the document must hold exactly one
/// non-null value at that field, as a data stream's documents must.
fn conform_doc(
    source: &str,
    schema: &Schema,
    kept: &HashMap<String, Kept>,
    stamp: Option<&str>,
) -> Result<Map<String, Value>, String> {
    let doc: Value = serde_json::from_str(source).map_err(|e| format!("malformed JSON: {e}"))?;
    let obj = doc
        .as_object()
        .ok_or_else(|| "the document is not an object".to_string())?;
    let mut flat = Map::new();
    flatten(obj, "", &mut flat);
    if let Some(field) = stamp {
        let values = match flat.get(field) {
            None | Some(Value::Null) => 0,
            Some(Value::Array(items)) => items.iter().filter(|v| !v.is_null()).count(),
            Some(_) => 1,
        };
        if values != 1 {
            return Err(no_timestamp(field));
        }
    }
    let mut lists: Vec<(String, Value)> = Vec::new();
    for (path, value) in flat.iter_mut() {
        let Ok(field) = schema.field_with_name(path) else {
            continue;
        };
        let dt = field.data_type();
        let rule = kept.get(path);
        let fit = |v: Value| match rule {
            Some(Kept::Date(formats)) if matches!(dt, DataType::Timestamp(..)) => {
                conform_date(v, formats)
            }
            _ => conform(v, dt).and_then(|v| keep(v, rule.and_then(Kept::precision))),
        };
        let why = |why: String| format!("field [{path}]: {why}");
        *value = match value.take() {
            Value::Array(items) if list_type(dt).is_some() => {
                let mut values = Vec::with_capacity(items.len());
                array_values(items, &mut values);
                if values.len() > 1 {
                    let conformed = values
                        .into_iter()
                        .map(fit)
                        .collect::<Result<Vec<Value>, String>>()
                        .map_err(why)?;
                    // A string field keeps its values joined, for its text index.
                    let own = if matches!(
                        dt,
                        DataType::LargeUtf8 | DataType::Utf8 | DataType::Utf8View
                    ) {
                        Value::String(
                            conformed
                                .iter()
                                .filter_map(Value::as_str)
                                .collect::<Vec<&str>>()
                                .join(MULTI_VALUE_SEPARATOR),
                        )
                    } else {
                        Value::Null
                    };
                    lists.push((format!("{path}{LIST_SUFFIX}"), Value::Array(conformed)));
                    own
                } else {
                    values.pop().map_or(Ok(Value::Null), fit).map_err(why)?
                }
            }
            other => fit(other).map_err(why)?,
        };
    }
    for (path, values) in lists {
        flat.insert(path, values);
    }
    Ok(flat)
}

/// Declare the list columns ([`LIST_SUFFIX`]) the rows need and the table
/// lacks, each a list of its field's own type, so a list of dates is one of
/// timestamps rather than of the integers the engine would infer. Whether
/// the schema changed; a list a batch running beside this one declared
/// first is no error.
fn ensure_list_columns(
    bridge: &Bridge,
    name: &str,
    schema: &Schema,
    rows: &[Map<String, Value>],
) -> Result<bool, String> {
    let mut wanted: Vec<String> = Vec::new();
    let mut patches: Vec<FieldPatch> = Vec::new();
    for row in rows {
        for key in row.keys() {
            let Some(path) = key.strip_suffix(LIST_SUFFIX) else {
                continue;
            };
            if schema.field_with_name(key).is_ok() || wanted.contains(key) {
                continue;
            }
            let Some(dt) = schema
                .field_with_name(path)
                .ok()
                .and_then(|f| list_type(f.data_type()))
            else {
                continue;
            };
            wanted.push(key.clone());
            patches.push(
                FieldPatch::named(key.clone())
                    .with_type(dt)
                    .with_nullable(true),
            );
        }
    }
    if patches.is_empty() {
        return Ok(false);
    }
    let table = bridge.table(name).map_err(|e| e.to_string())?;
    let applied = table.apply_schema(&SchemaPatch::new(patches), None);
    // The cached handle predates the change either way.
    bridge.forget(name);
    if let Err(e) = applied {
        let now = bridge.table(name).map_err(|e| e.to_string())?.schema();
        if wanted.iter().any(|w| now.field_with_name(w).is_err()) {
            return Err(e.to_string());
        }
    }
    Ok(true)
}

/// Append rows, isolating the ones the engine refuses: a refused batch is
/// split in halves until each refused document stands alone and is reported
/// with the engine's reason, while every accepted half is one append.
fn append_checked(table: &Supertable, rows: &[(usize, Value)], failed: &mut Vec<Value>) {
    if rows.is_empty() {
        return;
    }
    let values: Vec<Value> = rows.iter().map(|(_, v)| v.clone()).collect();
    match table.append_rows(&values) {
        Ok(()) => {}
        Err(e) if rows.len() == 1 => {
            failed.push(json!({ "i": rows[0].0, "reason": e.to_string() }));
        }
        Err(_) => {
            let (a, b) = rows.split_at(rows.len() / 2);
            append_checked(table, a, failed);
            append_checked(table, b, failed);
        }
    }
}

/// Index many documents in one append. `osids` and `sources` are parallel
/// arrays of length `count`. Returns `{"ok":n,"failed":[{"i":..,"reason":..}]}`,
/// where `i` indexes the input arrays, or `{"error":..}` when nothing ran.
#[no_mangle]
pub unsafe extern "C" fn infino_index_batch(
    h: *mut Bridge,
    name: *const c_char,
    osids: *const *const c_char,
    sources: *const *const c_char,
    count: c_int,
) -> *mut c_char {
    catch_unwind(AssertUnwindSafe(|| {
        let (Some(conn), Some(name)) = (bridge_ref(h), unsafe { cstr(name) }) else {
            return err_json("null argument");
        };
        if osids.is_null() || sources.is_null() || count <= 0 {
            return err_json("empty batch");
        }
        let n = count as usize;
        let os_p = unsafe { std::slice::from_raw_parts(osids, n) };
        let sr_p = unsafe { std::slice::from_raw_parts(sources, n) };
        let table = match conn.table(name) {
            Ok(t) => t,
            Err(e) => return err_json(&e.to_string()),
        };
        let schema = table.schema();
        let kept = conn.kept(name);
        let stamp = conn.stamp(name);

        let mut failed: Vec<Value> = Vec::new();
        let mut rows: Vec<(usize, Map<String, Value>)> = Vec::with_capacity(n);
        for i in 0..n {
            let osid = unsafe { cstr(os_p[i]) }.unwrap_or("");
            let source = unsafe { cstr(sr_p[i]) }.unwrap_or("{}");
            let mut flat = match conform_doc(source, &schema, &kept, stamp.as_deref()) {
                Ok(flat) => flat,
                Err(why) => {
                    failed.push(json!({ "i": i, "reason": why }));
                    continue;
                }
            };
            flat.insert(OSID.into(), Value::String(osid.to_string()));
            flat.insert(SOURCE.into(), Value::String(source.to_string()));
            rows.push((i, flat));
        }

        let mut maps: Vec<Map<String, Value>> = rows.iter().map(|(_, m)| m.clone()).collect();
        let (table, schema) = match ensure_list_columns(conn, name, &schema, &maps) {
            Ok(true) => match conn.table(name) {
                Ok(t) => {
                    let s = t.schema();
                    (t, s)
                }
                Err(e) => return err_json(&e.to_string()),
            },
            Ok(false) => (table, schema),
            Err(why) => return err_json(&why),
        };
        unify_new_paths(&mut maps, &schema);
        let indexed: Vec<(usize, Value)> = rows
            .iter()
            .zip(maps)
            .map(|((i, _), m)| (*i, Value::Object(m)))
            .collect();
        append_checked(&table, &indexed, &mut failed);
        let ok = n - failed.len();
        out_json(json!({ "ok": ok, "failed": failed }))
    }))
    .unwrap_or(ptr::null_mut())
}

/// Index many documents from one buffer, in one append. The buffer holds
/// `id0 source0 id1 source1 ...` as UTF-8; `id_ends[i]` is where row i's id
/// ends (its source starts there) and `src_ends[i]` where its source ends,
/// as byte offsets into the buffer. One buffer and two offset arrays cross
/// the boundary as three pointers, where an array of strings crosses as one
/// allocation per string. The documents are parsed and conformed in
/// parallel; the append is the engine's. Returns what
/// [`infino_index_batch`] returns.
#[no_mangle]
pub unsafe extern "C" fn infino_index_batch_buf(
    h: *mut Bridge,
    name: *const c_char,
    buf: *const u8,
    buf_len: usize,
    id_ends: *const u32,
    src_ends: *const u32,
    count: c_int,
) -> *mut c_char {
    catch_unwind(AssertUnwindSafe(|| {
        let (Some(conn), Some(name)) = (bridge_ref(h), unsafe { cstr(name) }) else {
            return err_json("null argument");
        };
        if buf.is_null() || id_ends.is_null() || src_ends.is_null() || count <= 0 {
            return err_json("empty batch");
        }
        let prepared = match unsafe {
            prepare(
                conn,
                name,
                buf,
                buf_len,
                id_ends,
                src_ends,
                None,
                count as usize,
            )
        } {
            Ok(p) => p,
            Err(why) => return err_json(&why),
        };
        let table = match conn.table(name) {
            Ok(t) => t,
            Err(e) => return err_json(&e.to_string()),
        };
        let n = prepared.n;
        let failed = append_prepared(&table, vec![prepared], &[], &[]);
        let failed: Vec<Value> = failed
            .into_iter()
            .map(|(_, i, reason)| json!({ "i": i, "reason": reason }))
            .collect();
        let ok = n - failed.len();
        out_json(json!({ "ok": ok, "failed": failed }))
    }))
    .unwrap_or(ptr::null_mut())
}

/// A batch parsed, conformed and converted to Arrow ahead of its append, so
/// that work runs while the previous append commits. Every row of the
/// batch is accounted for: in `batch` (or `values`, when the Arrow
/// conversion refused the batch) or in `failed`.
pub struct Prepared {
    n: usize,
    batch: Option<RecordBatch>,
    values: Vec<Value>,
    index_of: Vec<usize>,
    failed: Vec<(usize, String)>,
}

/// Each row's routing value and sequence numbers, beside the buffer of ids
/// and sources: `rt_ends[i]` is where row i's routing value ends (it follows
/// the row's source in the buffer; an empty one is none), `seqs[i]` its
/// stamp and `ops[i]` the operation that wrote it (each negative for none),
/// by writer `gen`.
#[derive(Clone, Copy)]
struct RowMeta {
    rt_ends: *const u32,
    seqs: *const i64,
    ops: *const i64,
    gen: i64,
}

/// Parse the buffer's documents in parallel and convert them to one Arrow
/// batch. With `meta`, each row also keeps its routing value in
/// [`ROUTING`], its stamp in [`SEQ`] and its operation in [`OP`] (see
/// [`RowMeta`]).
#[allow(clippy::too_many_arguments)]
unsafe fn prepare(
    conn: &Bridge,
    name: &str,
    buf: *const u8,
    buf_len: usize,
    id_ends: *const u32,
    src_ends: *const u32,
    meta: Option<RowMeta>,
    n: usize,
) -> Result<Prepared, String> {
    let bytes = unsafe { std::slice::from_raw_parts(buf, buf_len) };
    let id_ends = unsafe { std::slice::from_raw_parts(id_ends, n) };
    let src_ends = unsafe { std::slice::from_raw_parts(src_ends, n) };
    let rt_ends = meta.map(|m| unsafe { std::slice::from_raw_parts(m.rt_ends, n) });
    let seqs = meta.map(|m| unsafe { std::slice::from_raw_parts(m.seqs, n) });
    let ops = meta.map(|m| unsafe { std::slice::from_raw_parts(m.ops, n) });
    let gen = meta.map_or(0, |m| m.gen);
    let table = conn.table(name).map_err(|e| e.to_string())?;
    let table_schema = conn.conn.schema(name).map_err(|e| e.to_string())?;
    let schema = table.schema();
    let kept = conn.kept(name);
    let stamp = conn.stamp(name);

    // Each row's id, source and routing value as borrowed slices of the
    // buffer, with its stamp and operation.
    let mut rows_in: Vec<(&str, &str, &str, i64, i64)> = Vec::with_capacity(n);
    let mut start = 0usize;
    for i in 0..n {
        let (id_end, src_end) = (id_ends[i] as usize, src_ends[i] as usize);
        let rt_end = rt_ends.map_or(src_end, |ends| ends[i] as usize);
        if id_end < start || src_end < id_end || rt_end < src_end || rt_end > buf_len {
            return Err("offsets out of range".into());
        }
        let id = std::str::from_utf8(&bytes[start..id_end]).unwrap_or("");
        let src = std::str::from_utf8(&bytes[id_end..src_end]).unwrap_or("{}");
        let routing = std::str::from_utf8(&bytes[src_end..rt_end]).unwrap_or("");
        rows_in.push((
            id,
            src,
            routing,
            seqs.map_or(-1, |s| s[i]),
            ops.map_or(-1, |o| o[i]),
        ));
        start = rt_end;
    }

    let parsed: Vec<Result<Map<String, Value>, String>> = rows_in
        .par_iter()
        .map(|(id, src, routing, seq, op)| {
            conform_doc(src, &schema, &kept, stamp.as_deref()).map(|mut flat| {
                flat.insert(OSID.into(), Value::String((*id).to_string()));
                flat.insert(SOURCE.into(), Value::String((*src).to_string()));
                if !routing.is_empty() {
                    flat.insert(ROUTING.into(), Value::String((*routing).to_string()));
                }
                if *seq >= 0 {
                    flat.insert(SEQ.into(), json!(*seq));
                }
                if *op >= 0 {
                    flat.insert(OP.into(), json!(*op));
                }
                if *seq >= 0 || *op >= 0 {
                    flat.insert(GEN.into(), json!(gen));
                }
                flat
            })
        })
        .collect();
    let mut failed: Vec<(usize, String)> = Vec::new();
    let mut index_of: Vec<usize> = Vec::with_capacity(n);
    let mut rows: Vec<Map<String, Value>> = Vec::with_capacity(n);
    for (i, r) in parsed.into_iter().enumerate() {
        match r {
            Ok(flat) => {
                index_of.push(i);
                rows.push(flat);
            }
            Err(why) => failed.push((i, why)),
        }
    }
    // A field given several values may need its list column declared first;
    // the rows are then converted against the schema that holds it.
    let (table_schema, schema) = if ensure_list_columns(conn, name, &schema, &rows)? {
        (
            conn.conn.schema(name).map_err(|e| e.to_string())?,
            conn.table(name).map_err(|e| e.to_string())?.schema(),
        )
    } else {
        (table_schema, schema)
    };
    unify_new_paths(&mut rows, &schema);
    let values: Vec<Value> = rows.into_iter().map(Value::Object).collect();
    let batch = if values.is_empty() {
        None
    } else {
        rows_to_batch(&values, &table_schema).ok()
    };
    Ok(Prepared {
        n,
        batch,
        values,
        index_of,
        failed,
    })
}

/// The batch's rows whose `osid` is in `ids`, and the rest, as two batches;
/// None when the batch has no readable id column.
fn split_by_osid(batch: &RecordBatch, ids: &[String]) -> Option<(RecordBatch, RecordBatch)> {
    let column = batch.column_by_name(OSID)?;
    let wanted: std::collections::HashSet<&str> = ids.iter().map(String::as_str).collect();
    let mask: BooleanArray = if let Some(a) = column.as_any().downcast_ref::<LargeStringArray>() {
        a.iter().map(|v| v.map(|s| wanted.contains(s))).collect()
    } else if let Some(a) = column.as_any().downcast_ref::<StringArray>() {
        a.iter().map(|v| v.map(|s| wanted.contains(s))).collect()
    } else {
        return None;
    };
    let other: BooleanArray = mask.iter().map(|v| Some(!v.unwrap_or(false))).collect();
    let replacing = filter_record_batch(batch, &mask).ok()?;
    let fresh = filter_record_batch(batch, &other).ok()?;
    Some((replacing, fresh))
}

/// The batches as one, over the union of their schemas: a batch that added
/// an unmapped path has a column the others lack, and takes null there in
/// theirs. A path typed differently by two batches cannot be joined and is
/// the error. None when there is no batch.
fn join_over_union(batches: &[&RecordBatch]) -> Result<Option<RecordBatch>, String> {
    let Some(first) = batches.first() else {
        return Ok(None);
    };
    if batches.len() == 1 {
        return Ok(Some((*first).clone()));
    }
    let mut fields: Vec<Field> = Vec::new();
    for batch in batches {
        for field in batch.schema().fields() {
            match fields.iter().find(|f| f.name() == field.name()) {
                None => fields.push(field.as_ref().clone()),
                Some(seen) if seen.data_type() != field.data_type() => {
                    return Err(format!(
                        "[{}] is typed differently across the batch ({} and {})",
                        field.name(),
                        seen.data_type(),
                        field.data_type()
                    ));
                }
                Some(_) => {}
            }
        }
    }
    let schema = Arc::new(Schema::new(fields));
    let mut aligned: Vec<RecordBatch> = Vec::with_capacity(batches.len());
    for batch in batches {
        let columns: Vec<_> = schema
            .fields()
            .iter()
            .map(|f| match batch.column_by_name(f.name()) {
                Some(c) => c.clone(),
                None => new_null_array(f.data_type(), batch.num_rows()),
            })
            .collect();
        aligned.push(RecordBatch::try_new(schema.clone(), columns).map_err(|e| e.to_string())?);
    }
    concat_batches(&schema, aligned.iter())
        .map(Some)
        .map_err(|e| e.to_string())
}

/// The `osid` of every row of `batch`, in order; empty when it has no
/// readable id column.
fn osids_of(batch: &RecordBatch) -> Vec<String> {
    let Some(column) = batch.column_by_name(OSID) else {
        return Vec::new();
    };
    if let Some(a) = column.as_any().downcast_ref::<LargeStringArray>() {
        a.iter().flatten().map(str::to_owned).collect()
    } else if let Some(a) = column.as_any().downcast_ref::<StringArray>() {
        a.iter().flatten().map(str::to_owned).collect()
    } else {
        Vec::new()
    }
}

/// The documents of the converted batches of `prepared` whose id is among
/// `replaced`, as (batch index, document index). A batch that could not be
/// converted reports its own documents on the row path.
fn replaced_rows(prepared: &[Prepared], replaced: &[String]) -> Vec<(usize, usize)> {
    let mut out = Vec::new();
    for (b, p) in prepared.iter().enumerate() {
        if p.batch.is_none() {
            continue;
        }
        for (j, v) in p.values.iter().enumerate() {
            let id = v.get(OSID).and_then(Value::as_str).unwrap_or("");
            if replaced.iter().any(|r| r == id) {
                out.push((b, p.index_of[j]));
            }
        }
    }
    out
}

/// Stamp each document of a prepared batch with its sequence number,
/// `seqs[i]` for document `i` of the batch: in the row it falls back to, and
/// in its Arrow batch, where the column is replaced when the batch carries
/// it and added when it does not.
fn stamp_seqs(p: &mut Prepared, seqs: &[i64]) -> Result<(), String> {
    for (j, v) in p.values.iter_mut().enumerate() {
        if let (Some(seq), Value::Object(row)) = (seqs.get(p.index_of[j]), v) {
            row.insert(SEQ.into(), json!(seq));
        }
    }
    let Some(batch) = p.batch.take() else {
        return Ok(());
    };
    let column: Arc<dyn Array> = Arc::new(Int64Array::from(
        p.index_of
            .iter()
            .map(|&i| seqs.get(i).copied())
            .collect::<Vec<Option<i64>>>(),
    ));
    let schema = batch.schema();
    let mut fields: Vec<Arc<Field>> = schema.fields().iter().cloned().collect();
    let mut columns = batch.columns().to_vec();
    match schema.index_of(SEQ) {
        Ok(at) => columns[at] = column,
        Err(_) => {
            fields.push(Arc::new(Field::new(SEQ, DataType::Int64, true)));
            columns.push(column);
        }
    }
    let stamped = Schema::new_with_metadata(fields, schema.metadata().clone());
    p.batch = Some(RecordBatch::try_new(Arc::new(stamped), columns).map_err(|e| e.to_string())?);
    Ok(())
}

/// Append prepared batches as one commit. When `replaced` names ids, the
/// rows under those ids are replaced in the same mutation as their
/// tombstones (the engine's update), whatever shape the batches take: they
/// are joined over the union of their schemas first, so a batch that added
/// a path does not push the write onto a path that tombstones ahead of an
/// append. A replace that fails leaves the old rows and reports every
/// document it carried; an append the engine refuses falls back to the
/// halving append that names each refused document, over the documents
/// that replace nothing. A document whose batch could not be converted and
/// whose id is replaced is refused rather than appended beside the row it
/// was to replace. `seqs[b]`, when given, holds the sequence number of each
/// document of batch `b` ([`stamp_seqs`]). Returns every refused document as
/// (batch index, document index, reason).
fn append_prepared(
    table: &Supertable,
    mut prepared: Vec<Prepared>,
    replaced: &[String],
    seqs: &[Vec<i64>],
) -> Vec<(usize, usize, String)> {
    let mut failed: Vec<(usize, usize, String)> = Vec::new();
    for (b, p) in prepared.iter_mut().enumerate() {
        for (i, why) in &p.failed {
            failed.push((b, *i, why.clone()));
        }
        if let Some(seqs) = seqs.get(b) {
            if let Err(why) = stamp_seqs(p, seqs) {
                for i in &p.index_of {
                    failed.push((b, *i, format!("sequence number: {why}")));
                }
                p.batch = None;
                p.values.clear();
                p.index_of.clear();
            }
        }
    }
    let batches: Vec<&RecordBatch> = prepared.iter().filter_map(|p| p.batch.as_ref()).collect();
    let joined = match join_over_union(&batches) {
        Ok(joined) => joined,
        Err(why) => {
            for (b, p) in prepared.iter().enumerate() {
                if p.batch.is_some() {
                    for i in &p.index_of {
                        failed.push((b, *i, why.clone()));
                    }
                }
            }
            None
        }
    };
    // Whether the joined batch's rows that replace nothing landed as Arrow;
    // the rows that replace are either in by the update or reported by it.
    let mut fresh_landed = joined.is_none();
    if let Some(batch) = &joined {
        if replaced.is_empty() {
            fresh_landed = table.append(batch).is_ok();
        } else {
            match split_by_osid(batch, replaced) {
                Some((replacing, fresh)) => {
                    if replacing.num_rows() > 0 {
                        // The replace names the ids this commit carries a row for. An
                        // id in `replaced` whose document was refused keeps its old
                        // row, and naming it would make the replace match more rows
                        // than it brings, which the engine refuses whole.
                        let carried = osids_of(&replacing);
                        let pred = col(OSID)
                            .in_list(carried.iter().map(|s| lit(s.clone())).collect(), false);
                        if let Err(e) = table.update(pred, &replacing) {
                            let why = format!("replace: {e}");
                            for (b, i) in replaced_rows(&prepared, &carried) {
                                failed.push((b, i, why.clone()));
                            }
                        }
                    }
                    fresh_landed = fresh.num_rows() == 0 || table.append(&fresh).is_ok();
                }
                None => {
                    let why = "the batch carries no readable id column".to_string();
                    for (b, p) in prepared.iter().enumerate() {
                        if p.batch.is_some() {
                            for i in &p.index_of {
                                failed.push((b, *i, why.clone()));
                            }
                        }
                    }
                    fresh_landed = true;
                }
            }
        }
    }
    let joined_batches = joined.is_some();
    // What did not land as Arrow goes a document at a time: the rows of a
    // batch that could not be converted, and the fresh rows of the joined
    // batch when the engine refused the append. A document that replaces a
    // row is never appended this way: with its replace failed (reported
    // above) or its batch unconverted, appending it would put a second row
    // beside the one it was to replace.
    for (b, p) in prepared.into_iter().enumerate() {
        let converted = p.batch.is_some();
        if converted && joined_batches && fresh_landed {
            continue;
        }
        if converted && !joined_batches {
            continue; // reported as untyped above
        }
        let mut indexed: Vec<(usize, Value)> = Vec::new();
        for (j, v) in p.values.into_iter().enumerate() {
            let id = v.get(OSID).and_then(Value::as_str).unwrap_or("");
            let replaces = replaced.iter().any(|r| r == id);
            if replaces {
                if !converted {
                    failed.push((
                        b,
                        p.index_of[j],
                        "replace: the batch could not be converted".to_string(),
                    ));
                }
                // converted and replacing: landed, or reported by the replace.
                continue;
            }
            indexed.push((p.index_of[j], v));
        }
        let mut refused: Vec<Value> = Vec::new();
        append_checked(table, &indexed, &mut refused);
        for r in refused {
            let i = r["i"].as_u64().unwrap_or(0) as usize;
            failed.push((b, i, r["reason"].as_str().unwrap_or("refused").to_string()));
        }
    }
    failed
}

/// Parse and convert a batch ahead of its append; see [`Prepared`]. The
/// buffer layout is that of [`infino_index_batch_buf`]. Returns null when
/// the table is unknown or the offsets do not fit the buffer; otherwise a
/// handle that [`infino_append_prepared`] consumes or
/// [`infino_free_prepared`] releases.
#[no_mangle]
pub unsafe extern "C" fn infino_prepare_batch(
    h: *mut Bridge,
    name: *const c_char,
    buf: *const u8,
    buf_len: usize,
    id_ends: *const u32,
    src_ends: *const u32,
    count: c_int,
) -> *mut Prepared {
    catch_unwind(AssertUnwindSafe(|| {
        let (Some(conn), Some(name)) = (bridge_ref(h), unsafe { cstr(name) }) else {
            return ptr::null_mut();
        };
        if buf.is_null() || id_ends.is_null() || src_ends.is_null() || count <= 0 {
            return ptr::null_mut();
        }
        match unsafe {
            prepare(
                conn,
                name,
                buf,
                buf_len,
                id_ends,
                src_ends,
                None,
                count as usize,
            )
        } {
            Ok(p) => Box::into_raw(Box::new(p)),
            Err(why) => {
                eprintln!("[capi] prepare: {why}");
                ptr::null_mut()
            }
        }
    }))
    .unwrap_or(ptr::null_mut())
}

/// [`infino_prepare_batch`] for documents with a routing value and two
/// sequence numbers each: the buffer holds `id0 source0 routing0 id1 ...`,
/// `rt_ends[i]` is where row i's routing value ends (an empty one is none),
/// `seqs[i]` is its stamp and `ops[i]` the operation that wrote it (each
/// negative for none), by writer `gen`. A row keeps them in the
/// `osrouting`, `osseq`, `osop` and `osgen` columns.
#[no_mangle]
#[allow(clippy::too_many_arguments)]
pub unsafe extern "C" fn infino_prepare_batch_meta(
    h: *mut Bridge,
    name: *const c_char,
    buf: *const u8,
    buf_len: usize,
    id_ends: *const u32,
    src_ends: *const u32,
    rt_ends: *const u32,
    seqs: *const i64,
    ops: *const i64,
    gen: i64,
    count: c_int,
) -> *mut Prepared {
    catch_unwind(AssertUnwindSafe(|| {
        let (Some(conn), Some(name)) = (bridge_ref(h), unsafe { cstr(name) }) else {
            return ptr::null_mut();
        };
        if buf.is_null()
            || id_ends.is_null()
            || src_ends.is_null()
            || rt_ends.is_null()
            || seqs.is_null()
            || ops.is_null()
            || count <= 0
        {
            return ptr::null_mut();
        }
        match unsafe {
            prepare(
                conn,
                name,
                buf,
                buf_len,
                id_ends,
                src_ends,
                Some(RowMeta {
                    rt_ends,
                    seqs,
                    ops,
                    gen,
                }),
                count as usize,
            )
        } {
            Ok(p) => Box::into_raw(Box::new(p)),
            Err(why) => {
                eprintln!("[capi] prepare: {why}");
                ptr::null_mut()
            }
        }
    }))
    .unwrap_or(ptr::null_mut())
}

/// Append prepared batches in one commit (see [`append_prepared`]) and
/// release them; the rows whose ids are among the `replaced_count` strings
/// of `replaced` (null for none) replace the table's rows under those ids
/// in the same mutation. `seqs` (null for none) holds `seqs_count` sequence
/// numbers, one per document of every batch, batch after batch in order: the
/// number each document is written under. Returns
/// `{"ok":n,"failed":[{"b":batch,"i":doc,"reason":..}]}`, where `b` indexes
/// `prepared` and `i` the documents of that batch, or `{"error":..}` when
/// nothing ran (the handles are released either way).
#[no_mangle]
pub unsafe extern "C" fn infino_append_prepared(
    h: *mut Bridge,
    name: *const c_char,
    prepared: *const *mut Prepared,
    count: c_int,
    replaced: *const *const c_char,
    replaced_count: c_int,
    seqs: *const i64,
    seqs_count: c_int,
) -> *mut c_char {
    catch_unwind(AssertUnwindSafe(|| {
        if prepared.is_null() || count <= 0 {
            return err_json("empty batch");
        }
        let handles = unsafe { std::slice::from_raw_parts(prepared, count as usize) };
        let owned: Vec<Prepared> = handles
            .iter()
            .filter(|p| !p.is_null())
            .map(|p| *unsafe { Box::from_raw(*p) })
            .collect();
        let (Some(conn), Some(name)) = (bridge_ref(h), unsafe { cstr(name) }) else {
            return err_json("null argument");
        };
        let replaced: Vec<String> = if replaced.is_null() || replaced_count <= 0 {
            Vec::new()
        } else {
            unsafe { std::slice::from_raw_parts(replaced, replaced_count as usize) }
                .iter()
                .filter_map(|p| unsafe { cstr(*p) })
                .map(str::to_owned)
                .collect()
        };
        let table = match conn.table(name) {
            Ok(t) => t,
            Err(e) => return err_json(&e.to_string()),
        };
        let n: usize = owned.iter().map(|p| p.n).sum();
        let per_batch: Vec<Vec<i64>> = if seqs.is_null() || seqs_count <= 0 {
            Vec::new()
        } else if seqs_count as usize != n {
            return err_json(&format!("{seqs_count} sequence numbers for {n} documents"));
        } else {
            let all = unsafe { std::slice::from_raw_parts(seqs, n) };
            let mut at = 0;
            owned
                .iter()
                .map(|p| {
                    let these = all[at..at + p.n].to_vec();
                    at += p.n;
                    these
                })
                .collect()
        };
        let failed = append_prepared(&table, owned, &replaced, &per_batch);
        let ok = n.saturating_sub(failed.len());
        let failed: Vec<Value> = failed
            .into_iter()
            .map(|(b, i, reason)| json!({ "b": b, "i": i, "reason": reason }))
            .collect();
        out_json(json!({ "ok": ok, "failed": failed }))
    }))
    .unwrap_or(ptr::null_mut())
}

/// Release a prepared batch that will not be appended.
#[no_mangle]
pub unsafe extern "C" fn infino_free_prepared(p: *mut Prepared) {
    if !p.is_null() {
        drop(unsafe { Box::from_raw(p) });
    }
}

/// Tombstone every row whose `osid` is in the list, in one mutation. Backs the
/// upsert path (translog replay, updates): delete the ids of a batch, then
/// append the batch.
#[no_mangle]
pub unsafe extern "C" fn infino_delete_osids(
    h: *mut Bridge,
    name: *const c_char,
    osids: *const *const c_char,
    count: c_int,
) -> c_int {
    catch_unwind(AssertUnwindSafe(|| {
        let (Some(conn), Some(name)) = (bridge_ref(h), unsafe { cstr(name) }) else {
            return -1;
        };
        if osids.is_null() || count <= 0 {
            return 0;
        }
        let n = count as usize;
        let os_p = unsafe { std::slice::from_raw_parts(osids, n) };
        let ids: Vec<_> = os_p
            .iter()
            .filter_map(|p| unsafe { cstr(*p) })
            .map(|s| lit(s.to_string()))
            .collect();
        // No table yet means nothing to delete.
        let table = match conn.table(name) {
            Ok(t) => t,
            Err(_) => return 0,
        };
        match table.delete(col(OSID).in_list(ids, false)) {
            Ok(_) => 0,
            Err(e) => {
                eprintln!("[capi] delete_osids: {e}");
                -2
            }
        }
    }))
    .unwrap_or(-99)
}

/// The source of the row under `osid` and the sequence number it was
/// written under (0 for a row written without one), or None when the table
/// holds no such row.
fn get_row(conn: &Bridge, name: &str, osid: &str) -> Option<(String, i64)> {
    let table = conn.table(name).ok()?;
    // A term lookup through the id column's FTS index, projected to the
    // source and the sequence number. A table declared before the id column
    // carried that index refuses the lookup; it falls back to an equality on
    // the column.
    let batches = match table.exact_match(OSID, osid, Some(&[SOURCE, SEQ])) {
        Ok(batches) => batches,
        Err(_) => {
            let esc = osid.replace('\'', "''");
            let sql = format!("SELECT {SOURCE}, {SEQ} FROM {name} WHERE {OSID} = '{esc}' LIMIT 1");
            match conn.conn.query_sql(&sql) {
                Ok(batches) => batches,
                Err(e) => {
                    eprintln!("[capi] get: {e}");
                    return None;
                }
            }
        }
    };
    for b in &batches {
        if b.num_rows() == 0 {
            continue;
        }
        let Some(source) = b
            .column_by_name(SOURCE)
            .and_then(|c| c.as_any().downcast_ref::<LargeStringArray>())
        else {
            continue;
        };
        if source.is_null(0) {
            continue;
        }
        let seq = b
            .column_by_name(SEQ)
            .and_then(|c| c.as_any().downcast_ref::<Int64Array>())
            .filter(|a| !a.is_null(0))
            .map_or(0, |a| a.value(0));
        return Some((source.value(0).to_string(), seq));
    }
    None
}

/// The raw `_source` JSON for one osid, or "" if absent.
#[no_mangle]
pub unsafe extern "C" fn infino_get(
    h: *mut Bridge,
    name: *const c_char,
    osid: *const c_char,
) -> *mut c_char {
    catch_unwind(AssertUnwindSafe(|| {
        let (Some(conn), Some(name), Some(osid)) =
            (bridge_ref(h), unsafe { cstr(name) }, unsafe { cstr(osid) })
        else {
            return ptr::null_mut();
        };
        out_string(get_row(conn, name, osid).map_or_else(String::new, |(source, _)| source))
    }))
    .unwrap_or(ptr::null_mut())
}

/// Most lookups [`infino_get_many`] runs at once.
const GET_MANY_THREADS: usize = 16;

/// The raw `_source` JSON of several osids, as one JSON object from each id
/// the table holds to its source; an id it does not hold is absent.
///
/// Each id is looked up as [`infino_get`] looks it up, through the id
/// column's index. A page of a sorted search reads its sources this way once
/// its ids are chosen, and the lookups ran one call after another; here they
/// run side by side, at most [`GET_MANY_THREADS`] at once, so a page costs
/// about what its slowest lookup does.
#[no_mangle]
pub unsafe extern "C" fn infino_get_many(
    h: *mut Bridge,
    name: *const c_char,
    osids: *const *const c_char,
    count: c_int,
) -> *mut c_char {
    catch_unwind(AssertUnwindSafe(|| {
        let (Some(conn), Some(name)) = (bridge_ref(h), unsafe { cstr(name) }) else {
            return ptr::null_mut();
        };
        let ids: Vec<&str> = if osids.is_null() || count <= 0 {
            Vec::new()
        } else {
            unsafe { std::slice::from_raw_parts(osids, count as usize) }
                .iter()
                .filter_map(|p| unsafe { cstr(*p) })
                .collect()
        };
        let mut found = serde_json::Map::new();
        for chunk in ids.chunks(GET_MANY_THREADS) {
            let sources: Vec<Option<String>> = std::thread::scope(|scope| {
                let lookups: Vec<_> = chunk
                    .iter()
                    .map(|id| {
                        scope.spawn(move || get_row(conn, name, id).map(|(source, _)| source))
                    })
                    .collect();
                lookups
                    .into_iter()
                    .map(|lookup| lookup.join().ok().flatten())
                    .collect()
            });
            for (id, source) in chunk.iter().zip(sources) {
                if let Some(source) = source.filter(|s| !s.is_empty()) {
                    found.insert((*id).to_owned(), Value::String(source));
                }
            }
        }
        out_json(Value::Object(found))
    }))
    .unwrap_or(ptr::null_mut())
}

/// One osid's document as `{"source": .., "seq": n}`, its sequence number
/// what a conditional write on it is checked against; "" if absent.
#[no_mangle]
pub unsafe extern "C" fn infino_get_doc(
    h: *mut Bridge,
    name: *const c_char,
    osid: *const c_char,
) -> *mut c_char {
    catch_unwind(AssertUnwindSafe(|| {
        let (Some(conn), Some(name), Some(osid)) =
            (bridge_ref(h), unsafe { cstr(name) }, unsafe { cstr(osid) })
        else {
            return ptr::null_mut();
        };
        match get_row(conn, name, osid) {
            Some((source, seq)) => out_json(json!({ "source": source, "seq": seq })),
            None => out_string(String::new()),
        }
    }))
    .unwrap_or(ptr::null_mut())
}

/// Drop a table and purge its storage. A missing table is not fatal.
#[no_mangle]
pub unsafe extern "C" fn infino_drop(h: *mut Bridge, name: *const c_char) -> c_int {
    catch_unwind(AssertUnwindSafe(|| {
        let (Some(conn), Some(name)) = (bridge_ref(h), unsafe { cstr(name) }) else {
            return -1;
        };
        conn.forget(name);
        match conn.conn.drop_table(name, true) {
            Ok(_) => 0,
            Err(InfinoError::NotFound(_)) => 0,
            Err(e) => {
                eprintln!("[capi] drop_table: {e}");
                -2
            }
        }
    }))
    .unwrap_or(-99)
}

/// Compact the table's superfiles toward the engine's target size and collect
/// what the merge superseded. Returns 0, or -2 with the reason on stderr.
#[no_mangle]
pub unsafe extern "C" fn infino_optimize(h: *mut Bridge, name: *const c_char) -> c_int {
    unsafe { optimize_with(h, name, &OptimizeOptions::default()) }
}

/// Compact an edge table as [`infino_optimize`] does, and build the
/// adjacency index the engine's graph walks read over its rows: each row an
/// edge from the row [`SRC_ID`] of the table [`SRC_TABLE`] to the row
/// [`DST_ID`] of the table [`DST_TABLE`]. The engine builds it again only
/// when the rows changed since it last did. Returns what
/// [`infino_optimize`] returns.
#[no_mangle]
pub unsafe extern "C" fn infino_optimize_edges(h: *mut Bridge, name: *const c_char) -> c_int {
    let options = OptimizeOptions::default().with_adjacency(SRC_TABLE, SRC_ID, DST_TABLE, DST_ID);
    unsafe { optimize_with(h, name, &options) }
}

unsafe fn optimize_with(h: *mut Bridge, name: *const c_char, options: &OptimizeOptions) -> c_int {
    catch_unwind(AssertUnwindSafe(|| {
        let (Some(conn), Some(name)) = (bridge_ref(h), unsafe { cstr(name) }) else {
            return -1;
        };
        let table = match conn.table(name) {
            Ok(t) => t,
            Err(_) => return 0,
        };
        match table.optimize(options) {
            Ok(()) => 0,
            Err(e) => {
                eprintln!("[capi] optimize {name}: {e}");
                -2
            }
        }
    }))
    .unwrap_or(-99)
}

// ---------------------------------------------------------------------------
// Queries
// ---------------------------------------------------------------------------

#[no_mangle]
pub unsafe extern "C" fn infino_sql(h: *mut Bridge, sql: *const c_char) -> *mut c_char {
    catch_unwind(AssertUnwindSafe(|| {
        let (Some(conn), Some(sql)) = (bridge_ref(h), unsafe { cstr(sql) }) else {
            return ptr::null_mut();
        };
        match conn.conn.query_sql(sql) {
            Ok(b) => out_string(batches_to_json(&b)),
            Err(e) => err_json(&e.to_string()),
        }
    }))
    .unwrap_or(ptr::null_mut())
}

/// The indices a caller's read-only statement reads, as
/// `{"indices": [..]}`, for the plugin to have the caller authorized for
/// before it runs ([`infino_sql_read`]); a statement the route does not
/// serve is refused with the reason and its status.
#[no_mangle]
pub unsafe extern "C" fn infino_sql_indices(sql: *const c_char) -> *mut c_char {
    catch_unwind(AssertUnwindSafe(|| {
        let Some(sql) = (unsafe { cstr(sql) }) else {
            return ptr::null_mut();
        };
        match read_sql::indices(sql) {
            Ok(indices) => out_json(json!({ "indices": indices })),
            Err(refusal) => refused_json(&refusal),
        }
    }))
    .unwrap_or(ptr::null_mut())
}

/// Run a caller's read-only statement with each index it names read
/// through the view `views` gives for it, as `{"relations": {index: query},
/// "searches": {index: query}, "unsearchable": {index: [column]},
/// "output_only": [column], "whole": {index: table}, "max_result_bytes": n}`
/// (see [`read_sql::Reads`]). Answers
/// `{"schema": [{"name", "type"}], "rows": [..], "masked": {column: field}}`
/// (`masked`, left out when empty: the result's columns that give a field
/// read only as output, for the plugin to mask), or an error with its status: a statement the
/// route does not serve or whose rows pass `max_result_bytes` as JSON (400),
/// an index without a view or a use of a field the caller may not make
/// (403), a statement past the engine's memory budget (429).
#[no_mangle]
pub unsafe extern "C" fn infino_sql_read(
    h: *mut Bridge,
    sql: *const c_char,
    views: *const c_char,
) -> *mut c_char {
    catch_unwind(AssertUnwindSafe(|| {
        let (Some(conn), Some(sql), Some(views)) =
            (bridge_ref(h), unsafe { cstr(sql) }, unsafe { cstr(views) })
        else {
            return ptr::null_mut();
        };
        let views: Value = match serde_json::from_str(views) {
            Ok(v) => v,
            Err(e) => return err_json(&format!("views: {e}")),
        };
        let tables = match conn.conn.list_tables() {
            Ok(tables) => tables,
            Err(e) => return err_json(&e.to_string()),
        };
        let reads = read_sql::Reads {
            views: string_map(&views["relations"]),
            searches: string_map(&views["searches"]),
            unsearchable: views["unsearchable"]
                .as_object()
                .map(|m| {
                    m.iter()
                        .map(|(index, columns)| (index.clone(), string_set(columns)))
                        .collect()
                })
                .unwrap_or_default(),
            output_only: string_set(&views["output_only"]),
            whole: string_map(&views["whole"]),
        };
        let statement = match read_sql::rewrite(sql, &reads, &tables) {
            Ok(statement) => statement,
            Err(refusal) => return refused_json(&refusal),
        };
        let masked: Map<String, Value> = statement
            .masked
            .iter()
            .map(|(column, field)| (column.clone(), json!(field)))
            .collect();
        // No bound given reads as none: the plugin always sends its setting.
        let max_bytes = views["max_result_bytes"]
            .as_u64()
            .and_then(|n| usize::try_from(n).ok())
            .unwrap_or(usize::MAX);
        match conn.conn.query_sql(&statement.sql) {
            Ok(batches) => {
                let Some(rows) = batches_to_json_within(&batches, max_bytes) else {
                    return refused_json(&read_sql::Refusal {
                        message: format!(
                            "the result is larger than infino.sql.max_result_size ({max_bytes} bytes as JSON): \
                             read fewer rows or columns, or aggregate"
                        ),
                        status: read_sql::BAD_REQUEST,
                    });
                };
                let masked = if masked.is_empty() {
                    String::new()
                } else {
                    format!(",\"masked\":{}", Value::Object(masked))
                };
                out_string(format!(
                    "{{\"schema\":{},\"rows\":{rows}{masked}}}",
                    schema_json(&batches),
                ))
            }
            Err(e) => {
                let mut v = sql_error_value(&e);
                if let Some(message) = v["error"].as_str() {
                    v["error"] = json!(in_index_names(message, &reads, &tables));
                }
                out_json(v)
            }
        }
    }))
    .unwrap_or(ptr::null_mut())
}

/// A JSON array of strings as a set; anything else is empty.
fn string_set(v: &Value) -> HashSet<String> {
    v.as_array()
        .map(|a| {
            a.iter()
                .filter_map(|s| s.as_str().map(str::to_string))
                .collect()
        })
        .unwrap_or_default()
}

/// A JSON object of strings as a map; anything else is empty.
fn string_map(v: &Value) -> HashMap<String, String> {
    v.as_object()
        .map(|m| {
            m.iter()
                .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
                .collect()
        })
        .unwrap_or_default()
}

/// `message`, an engine error over a caller's statement, with each table a
/// view reads named as the index the statement read it through: the tables
/// are the plugin's, the caller named indices. A table several views read
/// is named by the first; longer table names are replaced first, so one
/// that begins another's name is never replaced inside it.
fn in_index_names(message: &str, reads: &read_sql::Reads, tables: &[String]) -> String {
    let mut by_table: Vec<(&str, &str)> = Vec::new();
    let mut indices: Vec<&String> = reads.views.keys().collect();
    indices.sort();
    for table in tables {
        // A view names its tables quoted: as an identifier, or as a search's argument.
        let (ident, literal) = (format!("\"{table}\""), format!("'{table}'"));
        if let Some(index) = indices.iter().find(|index| {
            let view = &reads.views[index.as_str()];
            view.contains(&ident) || view.contains(&literal)
        }) {
            by_table.push((table.as_str(), index.as_str()));
        }
    }
    by_table.sort_by_key(|(table, _)| Reverse(table.len()));
    let mut out = message.to_string();
    for (table, index) in by_table {
        out = out.replace(table, &format!("[{index}]"));
    }
    out
}

/// A statement refused, with the status that says why.
fn refused_json(refusal: &read_sql::Refusal) -> *mut c_char {
    out_json(json!({ "error": refusal.message, "status": refusal.status }))
}

/// The error a caller's statement failed with, under the status that tells
/// the caller's mistake from the engine's.
fn sql_error_value(e: &InfinoError) -> Value {
    let status = match e {
        InfinoError::Query(_) | InfinoError::Unsupported(_) => Some(STATUS_BAD_REQUEST),
        InfinoError::NotFound(_) => Some(STATUS_NOT_FOUND),
        InfinoError::OverBudget(_) => Some(STATUS_TOO_MANY_REQUESTS),
        _ => None,
    };
    let mut v = error_value(&e.to_string());
    if let Some(status) = status {
        v["status"] = json!(status);
    }
    v
}

/// The columns of a result, each its name and Arrow type; none for a
/// result with no batch.
fn schema_json(batches: &[RecordBatch]) -> String {
    let fields: Vec<Value> = batches
        .first()
        .map(|b| {
            b.schema()
                .fields()
                .iter()
                .map(|f| json!({ "name": f.name(), "type": f.data_type().to_string() }))
                .collect()
        })
        .unwrap_or_default();
    Value::Array(fields).to_string()
}

/// Ranked BM25 over one text column (`mode` is `"and"` or `"or"`), the top
/// `k` rows best first, each carrying `osid`, `source` and `score`: a page
/// of hits in one call, where the same page through `bm25_search` in SQL
/// would plan a statement first.
#[no_mangle]
pub unsafe extern "C" fn infino_bm25(
    h: *mut Bridge,
    name: *const c_char,
    column: *const c_char,
    query: *const c_char,
    mode: *const c_char,
    k: c_int,
) -> *mut c_char {
    catch_unwind(AssertUnwindSafe(|| {
        let (Some(conn), Some(name), Some(column), Some(query)) = (
            bridge_ref(h),
            unsafe { cstr(name) },
            unsafe { cstr(column) },
            unsafe { cstr(query) },
        ) else {
            return ptr::null_mut();
        };
        let mode = match unsafe { cstr(mode) } {
            Some("and") => BoolMode::And,
            _ => BoolMode::Or,
        };
        let table = match conn.table(name) {
            Ok(t) => t,
            Err(e) => return err_json(&e.to_string()),
        };
        match table.bm25_search(
            column,
            query,
            k.max(0) as usize,
            Bm25SearchOptions::new().with_mode(mode),
            Some(&[OSID, SOURCE, "score"]),
        ) {
            Ok(b) => out_string(batches_to_json(&b)),
            Err(e) => err_json(&e.to_string()),
        }
    }))
    .unwrap_or(ptr::null_mut())
}

/// The column the engine's sorted search fills with its own (zero) score.
const SORTED_SCORE: &str = "score";

/// The table's first `k` rows by `column`, ascending or (`descending`
/// non-zero) descending, rows sharing a value in row id order and rows
/// without one last, each carrying `osid`, `_id` and the column: the page a
/// search that keeps every row asks for when it sorts on one field, in one
/// call. No `source`: the caller looks the page's sources up by `osid`
/// afterwards. Asked for one, the engine would carry it into the statement
/// it plans when fewer than `k` rows have a value, and read every row's
/// source rather than the page's.
/// `null` when the column has no range index: the engine's sorted search
/// serves only a range-indexed column, and the caller plans the statement.
/// Also `null`, with no schema read, on a hosted connection (whose sorted
/// search forwards a statement anyway, so the schema request would only add
/// a round trip) and for a column named `score`: the engine's sorted search
/// puts its own score under that name, and the page drops it, which would
/// drop the field's values with it.
#[no_mangle]
pub unsafe extern "C" fn infino_sorted(
    h: *mut Bridge,
    name: *const c_char,
    column: *const c_char,
    descending: c_int,
    k: c_int,
) -> *mut c_char {
    catch_unwind(AssertUnwindSafe(|| {
        let (Some(conn), Some(name), Some(column)) =
            (bridge_ref(h), unsafe { cstr(name) }, unsafe {
                cstr(column)
            })
        else {
            return ptr::null_mut();
        };
        if conn.hosted || column == SORTED_SCORE {
            return out_string("null".to_string());
        }
        let range_indexed = match conn.conn.schema(name) {
            Ok(schema) => schema
                .fields()
                .iter()
                .any(|f| f.name == column && matches!(f.index, Some(ColumnIndex::Range))),
            Err(e) => return err_json(&e.to_string()),
        };
        if !range_indexed {
            return out_string("null".to_string());
        }
        let table = match conn.table(name) {
            Ok(t) => t,
            Err(e) => return err_json(&e.to_string()),
        };
        let projection = [OSID, ROW_ID, column];
        match table.sorted_search(
            column,
            descending != 0,
            k.max(0) as usize,
            Some(&projection[..]),
        ) {
            // The engine fixes every row's `score` at zero; a sorted hit has none.
            Ok(b) => out_string(batches_to_json(&without_column(&b, SORTED_SCORE))),
            Err(e) => err_json(&e.to_string()),
        }
    }))
    .unwrap_or(ptr::null_mut())
}

/// `batches` without their column `name`, when they have one.
fn without_column(batches: &[RecordBatch], name: &str) -> Vec<RecordBatch> {
    batches
        .iter()
        .map(|b| {
            let keep: Vec<usize> = (0..b.num_columns())
                .filter(|&i| b.schema().field(i).name() != name)
                .collect();
            b.project(&keep).unwrap_or_else(|_| b.clone())
        })
        .collect()
}

/// Number of rows matching a token query on one text column (`mode` is
/// `"and"` or `"or"`), without materializing them: the exact `hits.total`
/// of a `match`.
#[no_mangle]
pub unsafe extern "C" fn infino_count(
    h: *mut Bridge,
    name: *const c_char,
    column: *const c_char,
    query: *const c_char,
    mode: *const c_char,
) -> *mut c_char {
    catch_unwind(AssertUnwindSafe(|| {
        let (Some(conn), Some(name), Some(column), Some(query)) = (
            bridge_ref(h),
            unsafe { cstr(name) },
            unsafe { cstr(column) },
            unsafe { cstr(query) },
        ) else {
            return ptr::null_mut();
        };
        let mode = match unsafe { cstr(mode) } {
            Some("and") => BoolMode::And,
            _ => BoolMode::Or,
        };
        let table = match conn.table(name) {
            Ok(t) => t,
            Err(e) => return err_json(&e.to_string()),
        };
        match table.count(column, query, mode) {
            Ok(n) => out_string(n.to_string()),
            Err(e) => err_json(&e.to_string()),
        }
    }))
    .unwrap_or(ptr::null_mut())
}

/// The rows each of `predicates` holds: a JSON array of SQL boolean
/// expressions over the table's columns in, a JSON array of their counts out,
/// in order, as `COUNT(*) FILTER (WHERE p)` gives each. The engine counts a
/// range on a range-indexed column (or `1=1`, every row) from the range index
/// with no statement, and runs one aggregate otherwise.
#[no_mangle]
pub unsafe extern "C" fn infino_count_where(
    h: *mut Bridge,
    name: *const c_char,
    predicates: *const c_char,
) -> *mut c_char {
    catch_unwind(AssertUnwindSafe(|| {
        let (Some(conn), Some(name), Some(predicates)) =
            (bridge_ref(h), unsafe { cstr(name) }, unsafe {
                cstr(predicates)
            })
        else {
            return ptr::null_mut();
        };
        let predicates: Vec<String> = match serde_json::from_str(predicates) {
            Ok(p) => p,
            Err(e) => return err_json(&format!("predicates: {e}")),
        };
        let table = match conn.table(name) {
            Ok(t) => t,
            Err(e) => return err_json(&e.to_string()),
        };
        let schema = match DFSchema::try_from(table.schema().as_ref().clone()) {
            Ok(s) => s,
            Err(e) => return err_json(&e.to_string()),
        };
        // A predicate over another relation (a subquery) is no expression of
        // this table's columns alone, and the parser plans its subquery
        // against its own context, not the table's: such counts run as one
        // statement.
        // Each predicate as a statement would read it: coerced to the
        // columns' types, so a date compared with a string literal is a
        // comparison with a timestamp, which the table counts from its range
        // index rather than by reading every row.
        let Ok(exprs) = predicates
            .iter()
            .map(|p| {
                let expr = predicate_parser().parse_sql_expr(p, &schema)?;
                expr.rewrite(&mut TypeCoercionRewriter::new(&schema))
                    .map(|t| t.data)
            })
            .collect::<Result<Vec<_>, _>>()
        else {
            return count_where_by_sql(&conn.conn, name, &predicates);
        };
        if exprs.iter().any(holds_subquery) {
            return count_where_by_sql(&conn.conn, name, &predicates);
        }
        match table.count_where(&exprs) {
            Ok(counts) => out_string(json!(counts).to_string()),
            Err(e) => err_json(&e.to_string()),
        }
    }))
    .unwrap_or(ptr::null_mut())
}

/// Whether `expr` reads another relation: an `IN`, `EXISTS` or scalar
/// subquery anywhere in it.
fn holds_subquery(expr: &Expr) -> bool {
    expr.exists(|e| {
        Ok(matches!(
            e,
            Expr::InSubquery(_) | Expr::Exists(_) | Expr::ScalarSubquery(_)
        ))
    })
    .unwrap_or(true)
}

/// [`infino_count_where`]'s counts as statements over table `name`, one
/// `COUNT(*) ... WHERE p` per predicate: a FILTER clause cannot hold the
/// subquery these predicates read.
fn count_where_by_sql(conn: &Connection, name: &str, predicates: &[String]) -> *mut c_char {
    let table = name.replace('"', "\"\"");
    let mut counts = Vec::with_capacity(predicates.len());
    for p in predicates {
        let sql = format!("SELECT COUNT(*) AS c FROM \"{table}\" WHERE {p}");
        let batches = match conn.query_sql(&sql) {
            Ok(b) => b,
            Err(e) => return err_json(&e.to_string()),
        };
        let count = batches
            .iter()
            .find(|b| b.num_rows() > 0)
            .and_then(|row| row.column(0).as_any().downcast_ref::<Int64Array>())
            .map(|c| c.value(0));
        match count {
            Some(count) => counts.push(count),
            None => return err_json("count_where: a count came back as no Int64"),
        }
    }
    out_string(json!(counts).to_string())
}

/// The context [`infino_count_where`] parses predicates with: one for the
/// process, as building a context registers every function anew.
fn predicate_parser() -> &'static SessionContext {
    static PARSER: OnceLock<SessionContext> = OnceLock::new();
    PARSER.get_or_init(SessionContext::new)
}

/// A column's rows counted per value: rows of the value (under the column's
/// name, null for the rows without one) and `count(*)`, with no statement.
/// The engine reads them from the table's statistics when those count every
/// row exactly, and runs the grouped count otherwise. `null` when there are
/// more than `max_rows` of them, so a column of many values is not carried
/// across whole to a caller that would not use them.
#[no_mangle]
pub unsafe extern "C" fn infino_value_counts(
    h: *mut Bridge,
    name: *const c_char,
    column: *const c_char,
    max_rows: c_int,
) -> *mut c_char {
    catch_unwind(AssertUnwindSafe(|| {
        let (Some(conn), Some(name), Some(column)) =
            (bridge_ref(h), unsafe { cstr(name) }, unsafe {
                cstr(column)
            })
        else {
            return ptr::null_mut();
        };
        let table = match conn.table(name) {
            Ok(t) => t,
            Err(e) => return err_json(&e.to_string()),
        };
        match table.value_counts(column) {
            Ok(b) if b.num_rows() > max_rows.max(0) as usize => out_string("null".to_string()),
            Ok(b) => out_string(batches_to_json(&[b])),
            Err(e) => err_json(&e.to_string()),
        }
    }))
    .unwrap_or(ptr::null_mut())
}

/// Unranked full-text match set: the osids of every row matching `query` on
/// `column`. Backs query-scoped aggregations over a text query.
#[no_mangle]
pub unsafe extern "C" fn infino_token_match(
    h: *mut Bridge,
    name: *const c_char,
    column: *const c_char,
    query: *const c_char,
) -> *mut c_char {
    catch_unwind(AssertUnwindSafe(|| {
        let (Some(conn), Some(name), Some(column), Some(query)) = (
            bridge_ref(h),
            unsafe { cstr(name) },
            unsafe { cstr(column) },
            unsafe { cstr(query) },
        ) else {
            return ptr::null_mut();
        };
        let table = match conn.table(name) {
            Ok(t) => t,
            Err(e) => return err_json(&e.to_string()),
        };
        match table.token_match(column, query, BoolMode::Or, Some(&[OSID])) {
            Ok(b) => out_string(batches_to_json(&b)),
            Err(e) => err_json(&e.to_string()),
        }
    }))
    .unwrap_or(ptr::null_mut())
}

#[no_mangle]
pub unsafe extern "C" fn infino_knn(
    h: *mut Bridge,
    name: *const c_char,
    column: *const c_char,
    emb_json: *const c_char,
    k: c_int,
) -> *mut c_char {
    catch_unwind(AssertUnwindSafe(|| {
        let (Some(conn), Some(name), Some(column), Some(emb_json)) = (
            bridge_ref(h),
            unsafe { cstr(name) },
            unsafe { cstr(column) },
            unsafe { cstr(emb_json) },
        ) else {
            return ptr::null_mut();
        };
        let emb = match parse_vec(emb_json) {
            Ok(v) => v,
            Err(e) => return err_json(&e),
        };
        let table = match conn.table(name) {
            Ok(t) => t,
            Err(e) => return err_json(&e.to_string()),
        };
        match table.vector_search(
            column,
            &emb,
            k.max(0) as usize,
            None,
            Some(&[OSID, SOURCE, "score"]),
        ) {
            Ok(b) => out_string(batches_to_json(&b)),
            Err(e) => err_json(&e.to_string()),
        }
    }))
    .unwrap_or(ptr::null_mut())
}

#[no_mangle]
pub unsafe extern "C" fn infino_hybrid(
    h: *mut Bridge,
    name: *const c_char,
    text_column: *const c_char,
    text_query: *const c_char,
    vector_column: *const c_char,
    emb_json: *const c_char,
    k: c_int,
) -> *mut c_char {
    catch_unwind(AssertUnwindSafe(|| {
        let (Some(conn), Some(name), Some(tcol), Some(tq), Some(vcol), Some(emb_json)) = (
            bridge_ref(h),
            unsafe { cstr(name) },
            unsafe { cstr(text_column) },
            unsafe { cstr(text_query) },
            unsafe { cstr(vector_column) },
            unsafe { cstr(emb_json) },
        ) else {
            return ptr::null_mut();
        };
        let emb = match parse_vec(emb_json) {
            Ok(v) => v,
            Err(e) => return err_json(&e),
        };
        let table = match conn.table(name) {
            Ok(t) => t,
            Err(e) => return err_json(&e.to_string()),
        };
        match table.hybrid_search(
            tcol,
            tq,
            BoolMode::Or,
            vcol,
            &emb,
            k.max(0) as usize,
            Some(&[OSID, SOURCE, "score"]),
        ) {
            Ok(b) => out_string(batches_to_json(&b)),
            Err(e) => err_json(&e.to_string()),
        }
    }))
    .unwrap_or(ptr::null_mut())
}

/// Parse `"[f32,...]"`; the engine checks the width against the column.
fn parse_vec(json: &str) -> Result<Vec<f32>, String> {
    serde_json::from_str::<Vec<f64>>(json)
        .map(|v| v.into_iter().map(|f| f as f32).collect())
        .map_err(|e| e.to_string())
}

// ---------------------------------------------------------------------------
// Arrow → JSON
// ---------------------------------------------------------------------------

fn json_str(s: &str) -> String {
    serde_json::to_string(s).unwrap_or_else(|_| "\"\"".into())
}

/// Rows as a JSON array of flat objects. Timestamps are epoch milliseconds,
/// which is what OpenSearch's date formats and histogram keys take.
fn batches_to_json(batches: &[RecordBatch]) -> String {
    batches_to_json_within(batches, usize::MAX).unwrap_or_default()
}

/// [`batches_to_json`], or `None` as soon as the text passes `max_bytes`:
/// a result too large to hand over is never built whole.
fn batches_to_json_within(batches: &[RecordBatch], max_bytes: usize) -> Option<String> {
    let mut out = String::from("[");
    for batch in batches {
        let schema = batch.schema();
        for r in 0..batch.num_rows() {
            if out.len() > 1 {
                out.push(',');
            }
            out.push('{');
            for (c, field) in schema.fields().iter().enumerate() {
                if c > 0 {
                    out.push(',');
                }
                out.push_str(&json_str(field.name()));
                out.push(':');
                out.push_str(&cell_json(batch.column(c).as_ref(), r));
            }
            out.push('}');
            if out.len() > max_bytes {
                return None;
            }
        }
    }
    out.push(']');
    Some(out)
}

fn cell_json(col: &dyn Array, r: usize) -> String {
    if col.is_null(r) {
        return "null".into();
    }
    let any = col.as_any();
    if let Some(a) = any.downcast_ref::<Int64Array>() {
        return a.value(r).to_string();
    }
    if let Some(a) = any.downcast_ref::<UInt64Array>() {
        return a.value(r).to_string();
    }
    if let Some(a) = any.downcast_ref::<Float64Array>() {
        return a.value(r).to_string();
    }
    if let Some(a) = any.downcast_ref::<Float32Array>() {
        return a.value(r).to_string();
    }
    if let Some(a) = any.downcast_ref::<BooleanArray>() {
        return a.value(r).to_string();
    }
    if let Some(a) = any.downcast_ref::<LargeStringArray>() {
        return json_str(a.value(r));
    }
    if let Some(a) = any.downcast_ref::<StringArray>() {
        return json_str(a.value(r));
    }
    if let Some(a) = any.downcast_ref::<TimestampMillisecondArray>() {
        return a.value(r).to_string();
    }
    if let Some(a) = any.downcast_ref::<TimestampMicrosecondArray>() {
        return (a.value(r) / 1_000).to_string();
    }
    if let Some(a) = any.downcast_ref::<TimestampNanosecondArray>() {
        return (a.value(r) / 1_000_000).to_string();
    }
    if let Some(a) = any.downcast_ref::<TimestampSecondArray>() {
        return (a.value(r) * 1_000).to_string();
    }
    // A decimal, the engine's `_id` among them, as its digits in a string: an
    // id is past what a JSON number holds exactly.
    if let Some(a) = any.downcast_ref::<Decimal128Array>() {
        return json_str(&a.value_as_string(r));
    }
    "\"<unsupported>\"".into()
}

#[cfg(test)]
mod tests {
    use super::*;

    const MAPPING: &str = r#"{"properties":{
        "@timestamp":{"type":"date"},
        "level":{"type":"keyword"},
        "message":{"type":"text"},
        "status":{"type":"long"},
        "took":{"type":"float"},
        "http":{"properties":{"method":{"type":"keyword"}}},
        "geo":{"type":"geo_point"}
    }}"#;

    fn c(s: &str) -> CString {
        CString::new(s).unwrap()
    }

    fn take(p: *mut c_char) -> String {
        assert!(!p.is_null());
        let s = unsafe { CStr::from_ptr(p) }.to_str().unwrap().to_string();
        unsafe { infino_free_string(p) };
        s
    }

    /// A fresh catalog on local disk: `memory://` tables have no durable
    /// storage, and delete / update need one.
    fn open_logs() -> *mut Bridge {
        let dir = std::env::temp_dir().join(format!(
            "infino-capi-test-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0)
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let uri = format!("file://{}", dir.display());
        let h = unsafe { infino_open(c(&uri).as_ptr()) };
        assert!(!h.is_null());
        let out = ensure(h, MAPPING);
        assert!(out.contains("\"created\":true"), "{out}");
        h
    }

    fn close(h: *mut Bridge) {
        unsafe { infino_close(h) }
    }

    fn ensure(h: *mut Bridge, mapping: &str) -> String {
        take(unsafe { infino_ensure_table(h, c("logs").as_ptr(), c(mapping).as_ptr()) })
    }

    fn columns(h: *mut Bridge) -> Value {
        serde_json::from_str(&take(unsafe { infino_columns(h, c("logs").as_ptr()) })).unwrap()
    }

    fn index(h: *mut Bridge, docs: &[(&str, &str)]) -> Value {
        let ids: Vec<CString> = docs.iter().map(|(id, _)| c(id)).collect();
        let srcs: Vec<CString> = docs.iter().map(|(_, s)| c(s)).collect();
        let idp: Vec<*const c_char> = ids.iter().map(|x| x.as_ptr()).collect();
        let sp: Vec<*const c_char> = srcs.iter().map(|x| x.as_ptr()).collect();
        let out = take(unsafe {
            infino_index_batch(
                h,
                c("logs").as_ptr(),
                idp.as_ptr(),
                sp.as_ptr(),
                docs.len() as c_int,
            )
        });
        serde_json::from_str(&out).unwrap()
    }

    fn sql(h: *mut Bridge, q: &str) -> Value {
        let out = take(unsafe { infino_sql(h, c(q).as_ptr()) });
        serde_json::from_str(&out).unwrap_or_else(|_| panic!("{out}"))
    }

    fn check(h: *mut Bridge, doc: &str) -> String {
        take(unsafe { infino_check_doc(h, c("logs").as_ptr(), c(doc).as_ptr()) })
    }

    fn bm25(h: *mut Bridge, column: &str, query: &str) -> String {
        take(unsafe {
            infino_bm25(
                h,
                c("logs").as_ptr(),
                c(column).as_ptr(),
                c(query).as_ptr(),
                c("or").as_ptr(),
                10,
            )
        })
    }

    fn count(h: *mut Bridge, column: &str, query: &str) -> String {
        take(unsafe {
            infino_count(
                h,
                c("logs").as_ptr(),
                c(column).as_ptr(),
                c(query).as_ptr(),
                c("or").as_ptr(),
            )
        })
    }

    fn get(h: *mut Bridge, osid: &str) -> String {
        take(unsafe { infino_get(h, c("logs").as_ptr(), c(osid).as_ptr()) })
    }

    fn get_many(h: *mut Bridge, osids: &[&str]) -> Value {
        let ids: Vec<CString> = osids.iter().map(|x| c(x)).collect();
        let idp: Vec<*const c_char> = ids.iter().map(|x| x.as_ptr()).collect();
        let out = take(unsafe {
            infino_get_many(h, c("logs").as_ptr(), idp.as_ptr(), osids.len() as c_int)
        });
        serde_json::from_str(&out).unwrap_or_else(|_| panic!("{out}"))
    }

    fn delete(h: *mut Bridge, osids: &[&str]) -> c_int {
        let ids: Vec<CString> = osids.iter().map(|x| c(x)).collect();
        let idp: Vec<*const c_char> = ids.iter().map(|x| x.as_ptr()).collect();
        unsafe { infino_delete_osids(h, c("logs").as_ptr(), idp.as_ptr(), osids.len() as c_int) }
    }

    fn optimize(h: *mut Bridge) -> c_int {
        unsafe { infino_optimize(h, c("logs").as_ptr()) }
    }

    fn value_counts(h: *mut Bridge, column: &str, max_rows: c_int) -> Value {
        let out = take(unsafe {
            infino_value_counts(h, c("logs").as_ptr(), c(column).as_ptr(), max_rows)
        });
        serde_json::from_str(&out).unwrap_or_else(|_| panic!("{out}"))
    }

    /// `(value, count)` pairs of `rows`, the value under `key` and the count under `count`, sorted.
    fn counted(rows: &Value, key: &str, count: &str) -> Vec<(String, u64)> {
        let mut out: Vec<(String, u64)> = rows
            .as_array()
            .unwrap()
            .iter()
            .map(|r| (r[key].to_string(), r[count].as_u64().unwrap()))
            .collect();
        out.sort();
        out
    }

    /// The rows of several tables read as one row set, as the plugin's SQL
    /// route reads an index pattern: each column through `coalesce(.., NULL)`,
    /// a column a table lacks as null. A column read as stored carries its
    /// table's Parquet field id, and the engine refuses a union whose inputs'
    /// ids differ, as two tables' do.
    #[test]
    fn several_tables_read_as_one_row_set() {
        let h = open_logs();
        let w1 = r#"{"properties":{"n":{"type":"long"},"tag":{"type":"keyword"}}}"#;
        let w2 = r#"{"properties":{"n":{"type":"long"},"tag":{"type":"keyword"},"extra":{"type":"keyword"}}}"#;
        take(unsafe { infino_ensure_table(h, c("w1").as_ptr(), c(w1).as_ptr()) });
        take(unsafe { infino_ensure_table(h, c("w2").as_ptr(), c(w2).as_ptr()) });
        for (table, id, src) in [
            ("w1", "a", r#"{"n":1,"tag":"a"}"#),
            ("w1", "b", r#"{"n":2,"tag":"b"}"#),
            ("w2", "c", r#"{"n":3,"tag":"a","extra":"x"}"#),
        ] {
            let ids = [c(id)];
            let srcs = [c(src)];
            let idp: Vec<*const c_char> = ids.iter().map(|x| x.as_ptr()).collect();
            let sp: Vec<*const c_char> = srcs.iter().map(|x| x.as_ptr()).collect();
            take(unsafe { infino_index_batch(h, c(table).as_ptr(), idp.as_ptr(), sp.as_ptr(), 1) });
        }
        let rows = sql(
            h,
            "SELECT count(*) AS c, sum(n) AS s, count(extra) AS e FROM (\
             SELECT coalesce(n, NULL) AS n, coalesce(tag, NULL) AS tag, NULL AS extra FROM w1 UNION ALL \
             SELECT coalesce(n, NULL) AS n, coalesce(tag, NULL) AS tag, coalesce(extra, NULL) AS extra FROM w2) t",
        );
        assert_eq!(rows, json!([{ "c": 3, "s": 6, "e": 1 }]));
        close(h);
    }

    /// A graph walk as the SQL route runs it: rows of an index within some
    /// hops of a seed row, over an edge index read whole, returned through
    /// the index's search view. The edges are documents naming each end as
    /// its table and the `_id` a SQL read gives, as digits; the edge table
    /// is declared as the plugin declares an edge index's, and its adjacency
    /// index is built when it is compacted.
    #[test]
    fn a_walk_runs_through_the_routes_views() {
        let h = open_logs();
        index(
            h,
            &[
                ("a", r#"{"level":"error"}"#),
                ("b", r#"{"level":"info"}"#),
                ("c", r#"{"level":"warn"}"#),
                ("d", r#"{"level":"info"}"#),
            ],
        );
        let rows = sql(h, "SELECT osid, _id FROM logs ORDER BY osid");
        let id = |osid: &str| -> String {
            let row = rows
                .as_array()
                .unwrap()
                .iter()
                .find(|r| r["osid"] == osid)
                .unwrap();
            row["_id"].as_str().unwrap().to_string()
        };
        let edge_mapping = json!({"properties": {
            SRC_TABLE: {"type": "keyword"},
            SRC_ID: {"type": ENGINE_ID_TYPE},
            DST_TABLE: {"type": "keyword"},
            DST_ID: {"type": ENGINE_ID_TYPE},
        }})
        .to_string();
        let declared =
            take(unsafe { infino_ensure_table(h, c("edges").as_ptr(), c(&edge_mapping).as_ptr()) });
        assert!(declared.contains("\"created\":true"), "{declared}");
        // a -> b -> c; d is no row's neighbour.
        let edge = |from: &str, to: &str| {
            json!({SRC_TABLE: "logs", SRC_ID: id(from), DST_TABLE: "logs", DST_ID: id(to)})
                .to_string()
        };
        let ids = [c("ab"), c("bc"), c("bad")];
        let docs = [
            c(&edge("a", "b")),
            c(&edge("b", "c")),
            c(
                &json!({SRC_TABLE: "logs", SRC_ID: "a", DST_TABLE: "logs", DST_ID: id("d")})
                    .to_string(),
            ),
        ];
        let idp: Vec<*const c_char> = ids.iter().map(|x| x.as_ptr()).collect();
        let dp: Vec<*const c_char> = docs.iter().map(|x| x.as_ptr()).collect();
        let written: Value = serde_json::from_str(&take(unsafe {
            infino_index_batch(h, c("edges").as_ptr(), idp.as_ptr(), dp.as_ptr(), 3)
        }))
        .unwrap();
        // An edge whose end is no row id is refused, with why.
        assert_eq!(written["ok"], 2, "{written}");
        assert!(
            written["failed"][0]["reason"]
                .as_str()
                .unwrap()
                .contains("not a row id"),
            "{written}"
        );
        assert_eq!(unsafe { infino_optimize_edges(h, c("edges").as_ptr()) }, 0);
        let bridge = unsafe { &*h };

        let reads = read_sql::Reads {
            searches: HashMap::from([(
                "logs".to_string(),
                "SELECT * FROM infino_search('logs')".to_string(),
            )]),
            whole: HashMap::from([
                ("links".to_string(), "edges".to_string()),
                ("logs".to_string(), "logs".to_string()),
            ]),
            ..read_sql::Reads::default()
        };
        let statement = read_sql::rewrite(
            &format!(
                "SELECT osid FROM graph_walk('links', 'logs', 'logs', {}, 2, 10) \
                 WHERE osid <> 'a' ORDER BY score, osid",
                id("a")
            ),
            &reads,
            &[],
        )
        .unwrap();
        let out = bridge.conn.query_sql(&statement.sql).unwrap();
        let reached: Vec<String> = out
            .iter()
            .flat_map(|b| {
                let a = b
                    .column(0)
                    .as_any()
                    .downcast_ref::<LargeStringArray>()
                    .unwrap();
                (0..a.len())
                    .map(|i| a.value(i).to_string())
                    .collect::<Vec<_>>()
            })
            .collect();
        assert_eq!(reached, vec!["b", "c"]);
        close(h);
    }

    #[test]
    fn value_counts_are_the_grouped_count_up_to_the_cap() {
        // error, info and the rows without a level: three groups.
        const GROUPS: c_int = 3;
        let h = open_logs();
        index(
            h,
            &[
                ("a", r#"{"level":"error"}"#),
                ("b", r#"{"level":"info"}"#),
                ("c", r#"{"level":"info"}"#),
                ("d", r#"{"message":"no level"}"#),
            ],
        );
        let grouped_sql = "SELECT level, COUNT(*) AS n FROM logs GROUP BY level";
        assert_eq!(
            counted(&value_counts(h, "level", GROUPS), "level", "count(*)"),
            counted(&sql(h, grouped_sql), "level", "n")
        );
        assert_eq!(value_counts(h, "level", GROUPS - 1), Value::Null);
        // A delete leaves the deleted row in the table's statistics, so the
        // engine runs the grouped count, which leaves it out.
        assert_eq!(delete(h, &["a"]), 0);
        assert_eq!(
            counted(&value_counts(h, "level", GROUPS), "level", "count(*)"),
            counted(&sql(h, grouped_sql), "level", "n")
        );
        close(h);
    }

    #[test]
    fn count_where_counts_each_predicate_as_a_filtered_count() {
        let h = open_logs();
        index(
            h,
            &[
                (
                    "a",
                    r#"{"@timestamp":"2026-10-24T10:00:00Z","status":200,"took":1.5,"level":"info"}"#,
                ),
                (
                    "b",
                    r#"{"@timestamp":"2026-10-24T10:01:00Z","status":404,"took":30.0,"level":"warn"}"#,
                ),
                (
                    "c",
                    r#"{"@timestamp":"2026-10-24T10:02:00Z","status":500,"took":2.5,"level":"error"}"#,
                ),
                ("d", r#"{"message":"no status"}"#),
            ],
        );
        // Every row, ranges the index counts, two shapes it leaves to the
        // aggregate (a keyword column and an OR), and a date compared with a
        // string literal, which the count coerces to a timestamp as a
        // statement does.
        let predicates = [
            "1=1",
            r#"(1=1 AND ("status" >= 400))"#,
            r#"("took") < 10"#,
            r#""level" = 'info'"#,
            r#""status" < 300 OR "took" > 20"#,
            r#""@timestamp" >= '2026-10-24T10:01:00Z'"#,
            r#"("@timestamp" >= '2026-10-24T10:00:00Z' AND "@timestamp" < '2026-10-24T10:02:00Z')"#,
        ];
        let filtered: Vec<String> = predicates
            .iter()
            .enumerate()
            .map(|(i, p)| format!("COUNT(*) FILTER (WHERE {p}) AS c{i}"))
            .collect();
        let expected = sql(h, &format!("SELECT {} FROM logs", filtered.join(", ")));
        let expected: Vec<u64> = (0..predicates.len())
            .map(|i| expected[0][format!("c{i}")].as_u64().expect("a count"))
            .collect();
        let out = take(unsafe {
            infino_count_where(
                h,
                c("logs").as_ptr(),
                c(&json!(predicates).to_string()).as_ptr(),
            )
        });
        let counts: Vec<u64> = serde_json::from_str(&out).unwrap_or_else(|_| panic!("{out}"));
        assert_eq!(counts, expected);
        assert_eq!(counts, [4, 2, 2, 1, 2, 2, 2]);
        // A text clause joined by id, which reads the token index through a
        // subquery, beside the whole table.
        let joined = [
            r#""osid" IN (SELECT "osid" FROM token_match('logs', 'message', 'status', 'or'))"#,
            "1=1",
        ];
        let out = take(unsafe {
            infino_count_where(
                h,
                c("logs").as_ptr(),
                c(&json!(joined).to_string()).as_ptr(),
            )
        });
        let counts: Vec<u64> = serde_json::from_str(&out).unwrap_or_else(|_| panic!("{out}"));
        assert_eq!(counts, [1, 4]);
        close(h);
    }

    #[test]
    fn mapping_declares_the_columns() {
        let h = open_logs();
        let cols = columns(h);
        let by_name = |n: &str| {
            cols.as_array()
                .unwrap()
                .iter()
                .find(|cc| cc["name"] == n)
                .cloned()
                .unwrap_or_else(|| panic!("no column {n} in {cols}"))
        };
        assert_eq!(by_name("@timestamp")["type"], "ts");
        assert_eq!(by_name("level")["type"], "str");
        assert_eq!(by_name("level")["fts"], false);
        assert_eq!(by_name("message")["fts"], true);
        assert_eq!(by_name("status")["type"], "i64");
        assert_eq!(by_name("took")["type"], "f64");
        assert_eq!(by_name("http.method")["type"], "str");
        // geo_point has no column; a vector index exists only when mapped.
        assert!(cols
            .as_array()
            .unwrap()
            .iter()
            .all(|cc| cc["name"] != "geo"));
        assert!(cols
            .as_array()
            .unwrap()
            .iter()
            .all(|cc| cc["vector"] == false));
        close(h);
    }

    #[test]
    fn numeric_and_date_fields_get_a_range_index_unless_unindexed() {
        let mapping = json!({"properties": {
            "@timestamp": {"type": "date"},
            "status": {"type": "integer"},
            "took": {"type": "float"},
            "bytes": {"type": "long", "index": false},
            "level": {"type": "keyword"},
            "message": {"type": "text"},
            "ok": {"type": "boolean"},
            "http": {"properties": {"code": {"type": "short"}}}
        }});
        let mut ranged: Vec<String> = mapped_fields(&mapping)
            .into_iter()
            .filter(|f| f.range)
            .map(|f| f.path)
            .collect();
        ranged.sort();
        assert_eq!(ranged, ["@timestamp", "http.code", "status", "took"]);
    }

    #[test]
    fn an_array_of_objects_flattens_to_the_multi_valued_member_paths() {
        let doc: Value = serde_json::from_str(
            r#"{"references":[{"id":"a","type":"x"},{"id":"b"}],"tags":["t",{"k":1}],"plain":[1,2]}"#,
        )
        .unwrap();
        let mut flat = Map::new();
        flatten(doc.as_object().unwrap(), "", &mut flat);
        assert_eq!(flat["references.id"], json!(["a", "b"]));
        assert_eq!(flat["references.type"], json!(["x"]));
        assert!(flat.get("references").is_none());
        assert_eq!(flat["tags.k"], json!([1]));
        assert_eq!(flat["tags"], json!(["t"]));
        assert_eq!(flat["plain"], json!([1, 2]));
    }

    #[test]
    fn documents_are_typed_coerced_and_failed_per_item() {
        let h = open_logs();
        let out = index(
            h,
            &[
                (
                    "a",
                    r#"{"@timestamp":"2026-10-24T10:00:00Z","level":"error","message":"disk full on node-3","status":"500","took":1.5,"http":{"method":"GET"},"tags":["x","y"]}"#,
                ),
                (
                    "b",
                    r#"{"@timestamp":"2026-10-24T10:01:00Z","level":"info","message":"request served","status":200,"took":3,"http":{"method":"POST"},"tags":"z","retry":7}"#,
                ),
                (
                    "c",
                    r#"{"@timestamp":"2026-10-24T10:02:00Z","level":"warn","message":"slow disk","status":"not-a-number"}"#,
                ),
                (
                    "d",
                    r#"{"@timestamp":"2026-10-24T10:03:00Z","level":"info","message":"cache warm","status":204,"retry":"8"}"#,
                ),
            ],
        );
        assert_eq!(out["ok"], 3, "{out}");
        assert_eq!(out["failed"].as_array().unwrap().len(), 1, "{out}");
        assert_eq!(out["failed"][0]["i"], 2);
        assert!(out["failed"][0]["reason"]
            .as_str()
            .unwrap()
            .contains("status"));

        // The numeric string was coerced; the date is a timestamp the range can use;
        // `retry` arrived once as a number and once as a string, so it is a string
        // column; `tags` arrived once as a list and once as a scalar, so it is a list.
        let rows = sql(h, "SELECT osid, status FROM logs WHERE \"@timestamp\" >= '2026-10-24T10:00:30Z' ORDER BY \"@timestamp\" DESC");
        let ids: Vec<&str> = rows
            .as_array()
            .unwrap()
            .iter()
            .map(|r| r["osid"].as_str().unwrap())
            .collect();
        assert_eq!(ids, ["d", "b"]);
        let cols = columns(h);
        let col_type = |n: &str| {
            cols.as_array()
                .unwrap()
                .iter()
                .find(|cc| cc["name"] == n)
                .unwrap_or_else(|| panic!("{cols}"))["type"]
                .clone()
        };
        assert_eq!(col_type("retry"), "str", "{cols}");
        assert_eq!(col_type("tags"), "list", "{cols}");

        // A document the declared columns cannot hold is refused before it is indexed.
        let why = check(h, r#"{"status":"nope"}"#);
        assert!(why.contains("status"), "{why}");
        assert_eq!(check(h, r#"{"status":"7"}"#), "");

        // Field-scoped text search and the exact count behind hits.total.
        let hits = bm25(h, "message", "disk");
        assert!(hits.contains("\"a\""), "{hits}");
        assert!(!hits.contains("\"b\""), "{hits}");
        assert_eq!(count(h, "message", "disk"), "1");

        // A histogram over the timestamp, the shape every dashboard asks for.
        let buckets = sql(h, "SELECT date_bin(INTERVAL '2 minutes', \"@timestamp\") AS key, COUNT(*) AS doc_count FROM logs GROUP BY 1 ORDER BY 1");
        assert_eq!(buckets.as_array().unwrap().len(), 2, "{buckets}");
        assert!(buckets[0]["key"].is_number(), "{buckets}");

        // Fetch returns the document as sent.
        let src = get(h, "a");
        assert!(src.contains("\"tags\":[\"x\",\"y\"]"), "{src}");
        // Several at once: each id the table holds, as one get returns it;
        // an id it does not hold, absent.
        let many = get_many(h, &["a", "b", "no-such-id"]);
        assert_eq!(many["a"].as_str(), Some(src.as_str()), "{many}");
        assert_eq!(many["b"].as_str(), Some(get(h, "b").as_str()), "{many}");
        assert_eq!(many.as_object().map(|o| o.len()), Some(2), "{many}");
        assert_eq!(get_many(h, &[]), json!({}));
        close(h);
    }

    /// A text field the mapping adds after the table exists, as a dynamic
    /// field is, gets the text index creation would have given it; a text
    /// field the table already holds without one is still refused.
    #[test]
    fn a_text_field_mapped_late_gets_its_text_index() {
        let h = open_logs();
        let out = ensure(h, r#"{"properties":{"note":{"type":"text"}}}"#);
        assert!(out.contains("\"added\":1"), "{out}");
        index(h, &[("n1", r#"{"note":"a field mapped late"}"#)]);
        let hits = bm25(h, "note", "late");
        assert!(hits.contains("\"n1\""), "{hits}");
        // `level` is a keyword column: mapping it as text cannot add the index.
        let out = ensure(h, r#"{"properties":{"level":{"type":"text"}}}"#);
        assert!(out.contains("without a text index"), "{out}");
        close(h);
    }

    /// A field named like a reserved column, added after the table exists,
    /// leaves the column alone, as it does at creation (#23).
    #[test]
    fn a_late_field_named_like_a_reserved_column_leaves_it_alone() {
        let h = open_logs();
        let out = ensure(
            h,
            r#"{"properties":{"source":{"type":"text"},"osseq":{"type":"text"}}}"#,
        );
        assert!(out.contains("\"created\":false"), "{out}");
        index(h, &[("s1", r#"{"source":"hello"}"#)]);
        assert!(get(h, "s1").contains("hello"));
        close(h);
    }

    #[test]
    fn a_batch_lands_once() {
        let h = open_logs();
        let out = ensure(
            h,
            r#"{"properties":{"@timestamp":{"type":"date"},"host":{"type":"keyword"},"level":{"type":"keyword"},"region":{"type":"keyword"},"status":{"type":"long"},"took":{"type":"float"}}}"#,
        );
        assert!(out.contains("\"added\""), "{out}");
        let docs: Vec<(String, String)> = (1..=8)
            .map(|i| {
                (
                    i.to_string(),
                    format!(
                        r#"{{"@timestamp":"2026-10-24T10:0{}:00Z","host":"n{}","level":"info","region":"eu","status":{},"took":{}}}"#,
                        i % 10,
                        i % 3,
                        200 + i,
                        i
                    ),
                )
            })
            .collect();
        let refs: Vec<(&str, &str)> = docs.iter().map(|(a, b)| (a.as_str(), b.as_str())).collect();
        let out = index(h, &refs);
        assert_eq!(out["ok"], 8, "{out}");
        assert_eq!(out["failed"].as_array().unwrap().len(), 0, "{out}");
        let rows = sql(h, "SELECT COUNT(*) AS c FROM logs");
        assert_eq!(rows[0]["c"], 8, "{rows}");
        close(h);
    }

    /// Where an aggregation scoped by a text match spends its time on the
    /// bench's document shape, through the same C-ABI entry the plugin
    /// calls and, beside it, the engine's public API without the JSON
    /// round trip. Not a check; run with `--release --ignored --nocapture`.
    #[test]
    #[ignore]
    fn where_a_scoped_aggregation_spends_its_time() {
        use std::time::Instant;
        const DOCS: usize = 5_000;
        const BATCHES: usize = 200;
        const ITERS: usize = 10;
        let vocab = [
            "alpha", "bravo", "charlie", "delta", "echo", "foxtrot", "golf", "hotel", "india",
            "juliet", "kilo", "lima", "mike", "november", "oscar", "papa", "quebec", "romeo",
            "sierra", "tango",
        ];
        let mut seed: u64 = 42;
        let mut next = move || {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed
        };
        let mapping = r#"{"properties":{"category":{"type":"keyword"},"price":{"type":"float"},"text":{"type":"text"}}}"#;
        let h = open_logs();
        let conn = unsafe { &*h };
        take(unsafe { infino_ensure_table(h, c("bench").as_ptr(), c(mapping).as_ptr()) });
        for b in 0..BATCHES {
            let docs: Vec<(String, String)> = (0..DOCS)
                .map(|i| {
                    let words: Vec<&str> = (0..5).map(|_| vocab[(next() % 20) as usize]).collect();
                    let price = (next() % 100_000) as f64 / 100.0;
                    (
                        (b * DOCS + i).to_string(),
                        format!(
                            r#"{{"text": "{}", "category": "cat{}", "price": {}}}"#,
                            words.join(" "),
                            next() % 8,
                            price
                        ),
                    )
                })
                .collect();
            let ids: Vec<CString> = docs.iter().map(|(id, _)| c(id)).collect();
            let srcs: Vec<CString> = docs.iter().map(|(_, s)| c(s)).collect();
            let idp: Vec<*const c_char> = ids.iter().map(|x| x.as_ptr()).collect();
            let sp: Vec<*const c_char> = srcs.iter().map(|x| x.as_ptr()).collect();
            let out = take(unsafe {
                infino_index_batch(
                    h,
                    c("bench").as_ptr(),
                    idp.as_ptr(),
                    sp.as_ptr(),
                    docs.len() as c_int,
                )
            });
            assert!(out.contains("\"ok\":5000"), "{out}");
        }
        let report = |label: &str, run: &dyn Fn() -> String| {
            run();
            let mut times: Vec<f64> = (0..ITERS)
                .map(|_| {
                    let t = Instant::now();
                    run();
                    t.elapsed().as_secs_f64() * 1000.0
                })
                .collect();
            times.sort_by(|a, b| a.partial_cmp(b).unwrap());
            let sample = run();
            eprintln!(
                "{label:<52} median {:7.2} ms   min {:7.2} ms   -> {}",
                times[ITERS / 2],
                times[0],
                &sample[..sample.len().min(48)]
            );
        };
        let tm = "token_match('bench', 'text', 'alpha', 'and')";
        for phase in ["as ingested (200 commits)", "after optimize"] {
            if phase == "after optimize" {
                let t = Instant::now();
                let rc = unsafe { infino_optimize(h, c("bench").as_ptr()) };
                eprintln!(
                    "optimize: {:.0} ms -> rc {rc}",
                    t.elapsed().as_secs_f64() * 1000.0
                );
            }
            eprintln!("== {phase}");
            for (label, sql) in [
                ("C-ABI sql: SELECT 1", "SELECT 1 AS ok".to_string()),
                (
                    "C-ABI sql: count(*) FROM token_match",
                    format!("SELECT count(*) AS n FROM {tm}"),
                ),
                (
                    "C-ABI sql: sum(price) FROM token_match",
                    format!("SELECT sum(price) AS s FROM {tm}"),
                ),
                (
                    "C-ABI sql: sum(price) FROM bench",
                    "SELECT sum(price) AS s FROM bench".to_string(),
                ),
            ] {
                report(label, &|| take(unsafe { infino_sql(h, c(&sql).as_ptr()) }));
            }
            report("C-ABI count (token_match_count)", &|| {
                take(unsafe {
                    infino_count(
                        h,
                        c("bench").as_ptr(),
                        c("text").as_ptr(),
                        c("alpha").as_ptr(),
                        c("and").as_ptr(),
                    )
                })
            });
            for (label, sql) in [
                (
                    "public query_sql: count(*) FROM token_match",
                    format!("SELECT count(*) AS n FROM {tm}"),
                ),
                (
                    "public query_sql: sum(price) FROM token_match",
                    format!("SELECT sum(price) AS s FROM {tm}"),
                ),
            ] {
                report(label, &|| {
                    let b = conn.conn.query_sql(&sql).unwrap();
                    format!(
                        "{} batches, {} rows",
                        b.len(),
                        b.iter().map(|x| x.num_rows()).sum::<usize>()
                    )
                });
            }
        }
        close(h);
    }

    /// Where a batch's time goes, stage by stage, on the bench's document
    /// shape. Not a check; run with `--ignored --nocapture` and read the lines.
    #[test]
    #[ignore]
    fn where_a_batch_spends_its_time() {
        use std::time::Instant;
        const DOCS: usize = 5_000;
        const BATCHES: usize = 20;
        let vocab = [
            "alpha", "bravo", "charlie", "delta", "echo", "foxtrot", "golf", "hotel", "india",
            "juliet", "kilo", "lima", "mike", "november", "oscar", "papa", "quebec", "romeo",
            "sierra", "tango",
        ];
        let mut seed: u64 = 42;
        let mut next = move || {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed
        };
        let mut batches: Vec<Vec<(String, String)>> = Vec::new();
        for b in 0..BATCHES {
            let mut docs = Vec::with_capacity(DOCS);
            for i in 0..DOCS {
                let words: Vec<&str> = (0..5).map(|_| vocab[(next() % 20) as usize]).collect();
                let price = (next() % 100_000) as f64 / 100.0;
                docs.push((
                    (b * DOCS + i).to_string(),
                    format!(
                        r#"{{"text": "{}", "category": "cat{}", "price": {}}}"#,
                        words.join(" "),
                        next() % 8,
                        price
                    ),
                ));
            }
            batches.push(docs);
        }
        let mapping = r#"{"properties":{"category":{"type":"keyword"},"price":{"type":"float"},"text":{"type":"text"}}}"#;

        for id_index in [true, false] {
            let h = open_logs();
            let conn = unsafe { &*h };
            if id_index {
                take(unsafe { infino_ensure_table(h, c("bench").as_ptr(), c(mapping).as_ptr()) });
            } else {
                // The same columns, declared directly, with no term index on the id column.
                let fields = vec![
                    Field::new(OSID, DataType::LargeUtf8, false),
                    Field::new(SOURCE, DataType::LargeUtf8, true),
                    Field::new("category", DataType::LargeUtf8, true),
                    Field::new("price", DataType::Float64, true),
                    Field::new("text", DataType::LargeUtf8, true),
                ];
                conn.conn
                    .create_table(
                        "bench",
                        Arc::new(Schema::new(fields)),
                        IndexSpec::new().fts(FtsField::new("text".to_string())),
                    )
                    .unwrap();
            }
            let table = conn.table("bench").unwrap();
            let schema = table.schema();
            let table_schema = conn.conn.schema("bench").unwrap();
            let (mut t_conform, mut t_unify, mut t_rows_to_batch, mut t_append, mut t_capi) =
                (0.0, 0.0, 0.0, 0.0, 0.0);
            for (bi, docs) in batches.iter().enumerate() {
                // Stage timings on the even batches, the whole C-ABI call on the odd ones.
                if bi % 2 == 1 {
                    let refs: Vec<(&str, &str)> =
                        docs.iter().map(|(a, b)| (a.as_str(), b.as_str())).collect();
                    let ids: Vec<CString> = refs.iter().map(|(id, _)| c(id)).collect();
                    let srcs: Vec<CString> = refs.iter().map(|(_, s)| c(s)).collect();
                    let idp: Vec<*const c_char> = ids.iter().map(|x| x.as_ptr()).collect();
                    let sp: Vec<*const c_char> = srcs.iter().map(|x| x.as_ptr()).collect();
                    let t = Instant::now();
                    let out = take(unsafe {
                        infino_index_batch(
                            h,
                            c("bench").as_ptr(),
                            idp.as_ptr(),
                            sp.as_ptr(),
                            refs.len() as c_int,
                        )
                    });
                    t_capi += t.elapsed().as_secs_f64();
                    assert!(out.contains("\"ok\":5000"), "{out}");
                    continue;
                }
                let t = Instant::now();
                let mut rows: Vec<Map<String, Value>> = docs
                    .iter()
                    .map(|(id, src)| {
                        let mut flat =
                            conform_doc(src, &schema, &conn.kept("bench"), None).unwrap();
                        flat.insert(OSID.into(), Value::String(id.clone()));
                        flat.insert(SOURCE.into(), Value::String(src.clone()));
                        flat
                    })
                    .collect();
                t_conform += t.elapsed().as_secs_f64();
                let t = Instant::now();
                unify_new_paths(&mut rows, &schema);
                let values: Vec<Value> = rows.into_iter().map(Value::Object).collect();
                t_unify += t.elapsed().as_secs_f64();
                let t = Instant::now();
                let batch = rows_to_batch(&values, &table_schema).unwrap();
                t_rows_to_batch += t.elapsed().as_secs_f64();
                let t = Instant::now();
                table.append(&batch).unwrap();
                t_append += t.elapsed().as_secs_f64();
            }
            let per = |x: f64| x / (BATCHES as f64 / 2.0) * 1000.0;
            eprintln!(
                "id index {}: per 5000-doc batch — conform {:.1} ms, unify+wrap {:.1} ms, rows_to_batch {:.1} ms, append(commit) {:.1} ms; whole C-ABI call {:.1} ms",
                id_index,
                per(t_conform),
                per(t_unify),
                per(t_rows_to_batch),
                per(t_append),
                per(t_capi)
            );
            close(h);
        }
    }

    /// A prepared batch of ids and sources, as the plugin packs one: one buffer with end offsets.
    fn prepared(h: *mut Bridge, docs: &[(&str, &str)]) -> *mut Prepared {
        let mut buf: Vec<u8> = Vec::new();
        let mut id_ends: Vec<u32> = Vec::new();
        let mut src_ends: Vec<u32> = Vec::new();
        for (id, src) in docs {
            buf.extend_from_slice(id.as_bytes());
            id_ends.push(buf.len() as u32);
            buf.extend_from_slice(src.as_bytes());
            src_ends.push(buf.len() as u32);
        }
        let p = unsafe {
            infino_prepare_batch(
                h,
                c("logs").as_ptr(),
                buf.as_ptr(),
                buf.len(),
                id_ends.as_ptr(),
                src_ends.as_ptr(),
                docs.len() as c_int,
            )
        };
        assert!(!p.is_null());
        p
    }

    fn append_replacing(h: *mut Bridge, p: *mut Prepared, replaced: &[&str]) -> Value {
        append_with_seqs(h, &[p], replaced, &[])
    }

    #[test]
    fn a_replacing_append_replaces_in_one_mutation_and_the_id_lookup_follows() {
        let h = open_logs();
        let out = index(
            h,
            &[
                ("a1", r#"{"level":"info","status":1}"#),
                ("a2", r#"{"level":"info","status":2}"#),
            ],
        );
        assert_eq!(out["ok"], 2, "{out}");
        // One batch replacing a1 and adding a9: a1's old row goes with its new one, a9 is appended.
        let p = prepared(
            h,
            &[
                ("a1", r#"{"level":"warn","status":11}"#),
                ("a9", r#"{"level":"info","status":9}"#),
            ],
        );
        let out = append_replacing(h, p, &["a1"]);
        assert_eq!(out["ok"], 2, "{out}");
        assert!(get(h, "a1").contains("\"status\":11"), "{}", get(h, "a1"));
        assert!(get(h, "a9").contains("\"status\":9"));
        let rows = sql(h, "SELECT osid FROM logs WHERE osid = 'a1'");
        assert_eq!(rows.as_array().map(|r| r.len()), Some(1), "{rows}");
        // A second replacement of the same id, as an update after an update.
        let p = prepared(h, &[("a1", r#"{"level":"error","status":111}"#)]);
        let out = append_replacing(h, p, &["a1"]);
        assert_eq!(out["ok"], 1, "{out}");
        assert!(get(h, "a1").contains("\"status\":111"), "{}", get(h, "a1"));
        let rows = sql(h, "SELECT osid FROM logs WHERE osid = 'a1'");
        assert_eq!(rows.as_array().map(|r| r.len()), Some(1), "{rows}");
        close(h);
    }

    /// A document the columns refuse, in a batch that replaces rows, leaves
    /// its id's row as it was, and the batch's other rows are still replaced:
    /// the replace names only the ids it brings a row for. Naming the refused
    /// id too made the replace match one row more than it carried, which the
    /// engine refuses whole, so every replace beside it failed.
    #[test]
    fn a_refused_document_keeps_its_row_and_the_rest_of_the_batch_replaces() {
        let h = open_logs();
        let out = index(
            h,
            &[
                ("r1", r#"{"level":"info","status":1}"#),
                ("r2", r#"{"level":"info","status":2}"#),
            ],
        );
        assert_eq!(out["ok"], 2, "{out}");
        let p = prepared(
            h,
            &[
                ("r1", r#"{"level":"warn","status":"not a number"}"#),
                ("r2", r#"{"level":"warn","status":22}"#),
            ],
        );
        let out = append_replacing(h, p, &["r1", "r2"]);
        assert_eq!(out["ok"], 1, "{out}");
        let failed = out["failed"].as_array().cloned().unwrap_or_default();
        assert_eq!(failed.len(), 1, "{out}");
        assert_eq!(failed[0]["i"], 0, "{out}");
        assert!(
            get(h, "r1").contains("\"level\":\"info\""),
            "{}",
            get(h, "r1")
        );
        assert!(get(h, "r2").contains("\"status\":22"), "{}", get(h, "r2"));
        for id in ["r1", "r2"] {
            let rows = sql(h, &format!("SELECT osid FROM logs WHERE osid = '{id}'"));
            assert_eq!(rows.as_array().map(|r| r.len()), Some(1), "{rows}");
        }
        close(h);
    }

    fn append_replacing_many(
        h: *mut Bridge,
        handles: &[*mut Prepared],
        replaced: &[&str],
    ) -> Value {
        append_with_seqs(h, handles, replaced, &[])
    }

    /// Append prepared batches replacing `replaced`, each document under its
    /// number in `seqs` (none when empty), as the plugin's writer does.
    fn append_with_seqs(
        h: *mut Bridge,
        handles: &[*mut Prepared],
        replaced: &[&str],
        seqs: &[i64],
    ) -> Value {
        let ids: Vec<CString> = replaced.iter().map(|x| c(x)).collect();
        let idp: Vec<*const c_char> = ids.iter().map(|x| x.as_ptr()).collect();
        let out = take(unsafe {
            infino_append_prepared(
                h,
                c("logs").as_ptr(),
                handles.as_ptr(),
                handles.len() as c_int,
                if idp.is_empty() {
                    ptr::null()
                } else {
                    idp.as_ptr()
                },
                idp.len() as c_int,
                if seqs.is_empty() {
                    ptr::null()
                } else {
                    seqs.as_ptr()
                },
                seqs.len() as c_int,
            )
        });
        serde_json::from_str(&out).unwrap_or_else(|_| panic!("{out}"))
    }

    /// One id's document as `infino_get_doc` returns it; Null when it is absent.
    fn get_doc(h: *mut Bridge, id: &str) -> Value {
        let out = take(unsafe { infino_get_doc(h, c("logs").as_ptr(), c(id).as_ptr()) });
        if out.is_empty() {
            return Value::Null;
        }
        serde_json::from_str(&out).unwrap_or_else(|_| panic!("{out}"))
    }

    /// A field a document gives several values keeps them in its list column,
    /// declared the first time on either append path with the field's own
    /// type, the field's own column null for that document (a string's keeps
    /// them joined, for its text index); one value in an array is that value.
    #[test]
    fn several_values_go_to_the_field_s_list_column() {
        let h = open_logs();
        let mapping = r#"{"properties":{"n":{"type":"long"},"d":{"type":"date"},"ok":{"type":"boolean"},
            "kw":{"type":"keyword"},"items":{"properties":{"price":{"type":"double"}}}}}"#;
        let out = ensure(h, mapping);
        assert!(out.contains("\"created\":false"), "{out}");
        let out = index(
            h,
            &[
                (
                    "m1",
                    r#"{"n":[1,2,2],"d":["2026-10-01T00:00:00Z","2026-10-02T00:00:00Z"],"kw":["a","b"]}"#,
                ),
                (
                    "m2",
                    r#"{"n":[3],"d":"2026-10-03T00:00:00Z","kw":["c"],"items":[{"price":1.0}]}"#,
                ),
            ],
        );
        assert_eq!(out["ok"], 2, "{out}");
        // The prepared path declares the lists its own batch needs.
        let p = prepared(
            h,
            &[(
                "m3",
                r#"{"ok":[true,false],"items":[{"price":5.5},{"price":7.0}],"kw":"d"}"#,
            )],
        );
        let out = append_with_seqs(h, &[p], &[], &[]);
        assert_eq!(out["ok"], 1, "{out}");
        let cols = columns(h);
        let elem = |name: &str| {
            cols.as_array()
                .and_then(|all| all.iter().find(|c| c["name"] == name))
                .map(|c| c["elem"].clone())
        };
        assert_eq!(elem("n[]"), Some(json!("i64")), "{cols}");
        assert_eq!(elem("d[]"), Some(json!("ts")), "{cols}");
        assert_eq!(elem("kw[]"), Some(json!("str")), "{cols}");
        assert_eq!(elem("ok[]"), Some(json!("bool")), "{cols}");
        assert_eq!(elem("items.price[]"), Some(json!("f64")), "{cols}");
        // The answer's JSON carries no lists; their values are read as text.
        let rows = sql(
            h,
            r#"SELECT osid, n, array_to_string("n[]", ',') AS ns, cardinality("d[]") AS dates, kw,
                array_to_string("kw[]", ',') AS kws, "items.price", array_to_string("items.price[]", ',') AS prices
                FROM logs ORDER BY osid"#,
        );
        assert_eq!(rows[0]["n"], Value::Null, "{rows}");
        assert_eq!(rows[0]["ns"], "1,2,2", "{rows}");
        assert_eq!(rows[0]["dates"], 2, "{rows}");
        assert_eq!(rows[0]["kw"], "a b", "{rows}");
        assert_eq!(rows[0]["kws"], "a,b", "{rows}");
        assert_eq!(rows[1]["n"], 3, "{rows}");
        assert_eq!(rows[1]["ns"], Value::Null, "{rows}");
        assert_eq!(rows[1]["kw"], "c", "{rows}");
        assert_eq!(rows[1]["items.price"], 1.0, "{rows}");
        assert_eq!(rows[2]["items.price"], Value::Null, "{rows}");
        assert_eq!(rows[2]["prices"], "5.5,7", "{rows}");
        let hits = sql(h, r#"SELECT osid FROM logs WHERE array_has("n[]", 2)"#);
        assert_eq!(hits, json!([{ "osid": "m1" }]), "{hits}");
        close(h);
    }

    /// An `unsigned_long` past the largest signed 64-bit integer, and a
    /// whole float past those a float holds exactly, are refused by what
    /// they are rather than taken for fractions.
    #[test]
    fn an_integer_past_the_column_s_range_is_refused_by_what_it_is() {
        let past = conform(json!(u64::MAX), &DataType::Int64).expect_err("refused");
        assert!(past.contains("past the largest value"), "{past}");
        let whole = conform(json!(1.0e16), &DataType::Int64).expect_err("refused");
        assert!(
            whole.contains("whole numbers a float holds exactly"),
            "{whole}"
        );
        let fraction = conform(json!(1.5), &DataType::Int64).expect_err("refused");
        assert!(fraction.contains("fractional"), "{fraction}");
        assert_eq!(
            conform(json!(i64::MAX), &DataType::Int64).expect("held"),
            json!(i64::MAX)
        );
    }

    /// A `float`, `half_float` or `scaled_float` field holds each value as
    /// OpenSearch keeps it and reads it back, on both append paths and in
    /// its list column: a 32-bit float; a 16-bit float rounded from it, a
    /// tie to the even one; the value times the scaling factor rounded half
    /// up, over the factor. A `double` keeps the value whole, and a value a
    /// half holds only as an infinity is refused, as OpenSearch refuses it.
    #[test]
    fn a_narrow_float_field_holds_its_values_as_opensearch_keeps_them() {
        let h = open_logs();
        let mapping = r#"{"properties":{"f":{"type":"float"},"hf":{"type":"half_float"},
            "sf":{"type":"scaled_float","scaling_factor":100},"dd":{"type":"double"}}}"#;
        ensure(h, mapping);
        let out = index(
            h,
            &[
                // 0.125 and -0.125 scale to exact ties: up, toward positive infinity.
                ("p1", r#"{"f":0.1,"hf":53.98,"sf":0.125,"dd":0.1}"#),
                ("p2", r#"{"hf":[53.98,0.1],"sf":-0.125}"#),
                ("p3", r#"{"hf":70000}"#),
            ],
        );
        assert_eq!(out["ok"], 2, "{out}");
        assert!(
            out["failed"][0]["reason"]
                .as_str()
                .is_some_and(|r| r.contains("[half_float] supports only finite values")),
            "{out}"
        );
        // Halfway between two halves either side of 1: each goes to the even one.
        let p = prepared(h, &[("p4", r#"{"hf":[1.00048828125,"1.00146484375"]}"#)]);
        let out = append_with_seqs(h, &[p], &[], &[]);
        assert_eq!(out["ok"], 1, "{out}");
        let rows = sql(
            h,
            r#"SELECT osid, f, hf, sf, dd, array_to_string("hf[]", ',') AS hfs FROM logs
                WHERE osid IN ('p1', 'p2', 'p4') ORDER BY osid"#,
        );
        assert_eq!(rows[0]["f"], json!(f64::from(0.1_f32)), "{rows}");
        assert_eq!(rows[0]["hf"], 53.96875, "{rows}");
        assert_eq!(rows[0]["sf"], 0.13, "{rows}");
        assert_eq!(rows[0]["dd"], 0.1, "{rows}");
        assert_eq!(rows[1]["hfs"], "53.96875,0.0999755859375", "{rows}");
        assert_eq!(rows[1]["sf"], -0.12, "{rows}");
        assert_eq!(rows[2]["hfs"], "1,1.001953125", "{rows}");
        close(h);
    }

    /// Each document is stored under the sequence number it was appended
    /// with, batch after batch in order, whatever shape each batch takes; a
    /// replacement carries its new number; a row written without one reads as
    /// 0; and a count that does not match the documents writes nothing.
    #[test]
    fn appended_rows_keep_their_sequence_numbers() {
        let h = open_logs();
        let out = index(h, &[("s0", r#"{"level":"info","status":1}"#)]);
        assert_eq!(out["ok"], 1, "{out}");
        let first = prepared(h, &[("s1", r#"{"level":"info","status":2}"#)]);
        let second = prepared(
            h,
            &[
                ("s2", r#"{"level":"info","status":3}"#),
                ("s3", r#"{"level":"warn","status":4,"extra":"yes"}"#),
            ],
        );
        let out = append_with_seqs(h, &[first, second], &[], &[7, 8, 9]);
        assert_eq!(out["ok"], 3, "{out}");
        for (id, seq) in [("s0", 0), ("s1", 7), ("s2", 8), ("s3", 9)] {
            assert_eq!(get_doc(h, id)["seq"], seq, "{id}: {}", get_doc(h, id));
        }
        let replacing = prepared(h, &[("s1", r#"{"level":"error","status":22}"#)]);
        let out = append_with_seqs(h, &[replacing], &["s1"], &[10]);
        assert_eq!(out["ok"], 1, "{out}");
        let doc = get_doc(h, "s1");
        assert_eq!(doc["seq"], 10, "{doc}");
        assert!(
            doc["source"]
                .as_str()
                .unwrap_or_default()
                .contains("\"status\":22"),
            "{doc}"
        );
        let miscounted = prepared(h, &[("s4", r#"{"level":"info","status":5}"#)]);
        let out = append_with_seqs(h, &[miscounted], &[], &[11, 12]);
        assert!(out.get("error").is_some(), "{out}");
        assert_eq!(get_doc(h, "s4"), Value::Null);
        close(h);
    }

    /// Two batches of different shape (the second adds a path the mapping
    /// does not name) replacing an existing id go in as one mutation: the
    /// replaced row is gone, its new row and the other batch's rows are
    /// there, and nothing was tombstoned ahead of an append.
    #[test]
    fn batches_of_different_shape_replace_in_one_mutation() {
        let h = open_logs();
        let out = index(
            h,
            &[
                ("b1", r#"{"level":"info","status":1}"#),
                ("b2", r#"{"level":"info","status":2}"#),
            ],
        );
        assert_eq!(out["ok"], 2, "{out}");
        let first = prepared(h, &[("b1", r#"{"level":"warn","status":11}"#)]);
        let second = prepared(
            h,
            &[
                ("b2", r#"{"level":"warn","status":22,"extra":"yes"}"#),
                ("b3", r#"{"level":"info","status":3,"extra":"new"}"#),
            ],
        );
        let out = append_replacing_many(h, &[first, second], &["b1", "b2"]);
        assert_eq!(out["ok"], 3, "{out}");
        for id in ["b1", "b2"] {
            let rows = sql(h, &format!("SELECT osid FROM logs WHERE osid = '{id}'"));
            assert_eq!(rows.as_array().map(|r| r.len()), Some(1), "{id}: {rows}");
        }
        assert!(get(h, "b1").contains("\"status\":11"), "{}", get(h, "b1"));
        assert!(
            get(h, "b2").contains("\"extra\":\"yes\""),
            "{}",
            get(h, "b2")
        );
        assert!(
            get(h, "b3").contains("\"extra\":\"new\""),
            "{}",
            get(h, "b3")
        );
        let rows = sql(h, "SELECT COUNT(*) AS c FROM logs");
        assert_eq!(rows[0]["c"], 3, "{rows}");
        // The added path is a column now, null for the batch that lacked it.
        let rows = sql(h, "SELECT osid FROM logs WHERE extra IS NULL ORDER BY osid");
        assert_eq!(rows.as_array().map(|r| r.len()), Some(1), "{rows}");
        assert_eq!(rows[0]["osid"], "b1", "{rows}");
        close(h);
    }

    /// A path typed one way by one batch and another way by the next cannot
    /// be joined; every document of those batches is refused with the
    /// reason, and the rows they were to replace stay as they were.
    #[test]
    fn batches_that_type_a_path_differently_are_refused_whole() {
        let h = open_logs();
        let out = index(h, &[("c1", r#"{"level":"info","status":1}"#)]);
        assert_eq!(out["ok"], 1, "{out}");
        let first = prepared(h, &[("c1", r#"{"level":"warn","status":11,"extra":1}"#)]);
        let second = prepared(
            h,
            &[("c2", r#"{"level":"info","status":2,"extra":"text"}"#)],
        );
        let out = append_replacing_many(h, &[first, second], &["c1"]);
        assert_eq!(out["ok"], 0, "{out}");
        let failed = out["failed"].as_array().unwrap();
        assert_eq!(failed.len(), 2, "{out}");
        assert!(
            failed[0]["reason"]
                .as_str()
                .unwrap()
                .contains("typed differently"),
            "{out}"
        );
        assert!(get(h, "c1").contains("\"status\":1"), "{}", get(h, "c1"));
        assert!(get(h, "c2").is_empty(), "{}", get(h, "c2"));
        close(h);
    }

    #[test]
    fn a_replacing_append_over_a_prepared_table_keeps_the_id_lookup() {
        let h = open_logs();
        // The plugin's own write path: prepared batches, the first with no replacement.
        let p = prepared(
            h,
            &[
                (
                    "a1",
                    r#"{"@timestamp":"2026-10-24T10:00:00Z","level":"x","message":"wool coat","status":1}"#,
                ),
                (
                    "a2",
                    r#"{"@timestamp":"2026-10-24T10:00:00Z","level":"x","message":"coat of wool","status":3}"#,
                ),
                (
                    "a3",
                    r#"{"@timestamp":"2026-10-24T10:00:00Z","level":"y","message":"silk scarf","status":5}"#,
                ),
                (
                    "a4",
                    r#"{"@timestamp":"2026-10-24T10:00:00Z","message":"no tag here","status":7}"#,
                ),
            ],
        );
        assert_eq!(append_replacing(h, p, &[])["ok"], 4);
        let rows = sql(
            h,
            "SELECT osid, source FROM logs WHERE osid IN ('a1', 'a2')",
        );
        assert_eq!(rows.as_array().map(|r| r.len()), Some(2), "{rows}");
        let p = prepared(
            h,
            &[(
                "a1",
                r#"{"@timestamp":"2026-10-24T10:00:00Z","level":"x","message":"wool coat","status":11}"#,
            )],
        );
        assert_eq!(append_replacing(h, p, &["a1"])["ok"], 1);
        assert!(get(h, "a1").contains("\"status\":11"), "{}", get(h, "a1"));
        let p = prepared(
            h,
            &[(
                "a1",
                r#"{"@timestamp":"2026-10-24T10:00:00Z","level":"x","message":"wool coat","status":11,"extra":"yes"}"#,
            )],
        );
        assert_eq!(append_replacing(h, p, &["a1"])["ok"], 1);
        assert!(
            get(h, "a1").contains("\"extra\":\"yes\""),
            "{}",
            get(h, "a1")
        );
        close(h);
    }

    #[test]
    fn a_later_mapping_adds_columns_and_deletes_are_batched() {
        let h = open_logs();
        index(
            h,
            &[(
                "a",
                r#"{"@timestamp":"2026-10-24T10:00:00Z","level":"info","message":"one"}"#,
            )],
        );
        let out = ensure(
            h,
            r#"{"properties":{"host":{"properties":{"name":{"type":"keyword"}}},"ended":{"type":"date"}}}"#,
        );
        assert!(out.contains("\"added\":2"), "{out}");
        index(
            h,
            &[(
                "b",
                r#"{"@timestamp":"2026-10-24T10:01:00Z","level":"info","message":"two","host":{"name":"n1"},"ended":"2026-10-24T10:02:00Z"}"#,
            )],
        );
        let rows = sql(
            h,
            "SELECT osid FROM logs WHERE \"host.name\" = 'n1' AND ended > '2026-10-24T10:01:30Z'",
        );
        assert_eq!(rows.as_array().unwrap().len(), 1, "{rows}");

        assert_eq!(delete(h, &["a", "b"]), 0);
        let rows = sql(h, "SELECT COUNT(*) AS c FROM logs");
        assert_eq!(rows[0]["c"], 0, "{rows}");
        assert_eq!(optimize(h), 0);
        close(h);
    }

    /// A refused hosted call carries its status in the error object, read
    /// off the engine's message; a message without one carries none.
    #[test]
    fn a_hosted_refusal_carries_its_status() {
        let limited = error_value(
            "append: server returned 403 (the key was refused for this call): free plan row limit reached",
        );
        assert_eq!(limited["status"], 403, "{limited}");
        assert!(
            limited["error"].as_str().unwrap().contains("row limit"),
            "{limited}"
        );
        let unknown = error_value(
            "append: server returned 401 (the key was refused for this call): no such key",
        );
        assert_eq!(unknown["status"], 401, "{unknown}");
        let moved = error_value("query_sql: server returned 503: activating");
        assert_eq!(moved["status"], 503, "{moved}");
        let local = error_value("the mapping of logs cannot be applied to its table");
        assert!(local.get("status").is_none(), "{local}");
        assert_eq!(status_in("server returned abc"), None);
    }

    /// The service's refusal text comes out of a JSON body, or is the body itself.
    #[test]
    fn a_trial_refusal_is_read_off_the_body() {
        assert_eq!(
            server_message(r#"{"error":"this address has used its trial"}"#),
            "this address has used its trial"
        );
        assert_eq!(
            server_message(r#"{"message":"no trial here"}"#),
            "no trial here"
        );
        assert_eq!(server_message("  plain text  "), "plain text");
    }

    /// Bytes under `dir`, recursively.
    fn bytes_under(dir: &std::path::Path) -> u64 {
        std::fs::read_dir(dir)
            .map(|entries| {
                entries
                    .flatten()
                    .map(|e| match e.metadata() {
                        Ok(m) if m.is_dir() => bytes_under(&e.path()),
                        Ok(m) => m.len(),
                        Err(_) => 0,
                    })
                    .sum()
            })
            .unwrap_or(0)
    }

    /// A catalog reopened with a cache directory, as a node is after a
    /// restart, reads what its queries need through the cache: the bytes land
    /// there, and the answers are the ones the writing connection gave.
    #[test]
    fn a_reopened_catalog_reads_through_its_cache() {
        let root = std::env::temp_dir().join(format!(
            "infino-capi-cache-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0)
        ));
        let data = root.join("data");
        let cache = root.join("cache");
        std::fs::create_dir_all(&data).unwrap();
        let uri = c(&format!("file://{}", data.display()));
        let cache_dir = c(&cache.display().to_string());

        let h = unsafe { infino_open_with(uri.as_ptr(), ptr::null(), cache_dir.as_ptr()) };
        assert!(!h.is_null());
        assert!(ensure(h, MAPPING).contains("\"created\":true"));
        index(
            h,
            &[
                ("a", r#"{"level":"error","message":"disk full"}"#),
                ("b", r#"{"level":"info","message":"request served"}"#),
            ],
        );
        let written = sql(h, "SELECT osid FROM logs WHERE level = 'error'");
        close(h);
        // Whatever the writing connection kept goes, so only the reopened
        // connection's reads can fill the cache.
        let _ = std::fs::remove_dir_all(&cache);

        let h = unsafe { infino_open_with(uri.as_ptr(), ptr::null(), cache_dir.as_ptr()) };
        assert!(!h.is_null());
        assert_eq!(
            sql(h, "SELECT osid FROM logs WHERE level = 'error'"),
            written
        );
        assert!(
            bytes_under(&cache) > 0,
            "the reopened read went through the cache"
        );
        close(h);
        let _ = std::fs::remove_dir_all(&root);
    }

    /// A hosted URI is one that names a service rather than storage.
    #[test]
    fn hosted_uris_are_http() {
        assert!(is_hosted("https://host/db"));
        assert!(is_hosted("http://127.0.0.1:9100/db"));
        assert!(!is_hosted("memory://"));
        assert!(!is_hosted("file:///tmp/catalog"));
        assert!(!is_hosted("s3://bucket/prefix"));
    }

    /// Opening a hosted URI nobody serves fails with the reason, and the
    /// reason is read once.
    #[test]
    fn a_failed_open_reports_why_once() {
        let uri = CString::new("http://127.0.0.1:1/nobody").unwrap();
        let key = CString::new("ik_test").unwrap();
        let h = unsafe { infino_open_with(uri.as_ptr(), key.as_ptr(), ptr::null()) };
        assert!(h.is_null());
        let why = unsafe { infino_open_error() };
        assert!(!why.is_null());
        let text = unsafe { CStr::from_ptr(why) }.to_str().unwrap().to_string();
        unsafe { infino_free_string(why) };
        let why: Value = serde_json::from_str(&text).unwrap();
        assert!(
            why["error"].as_str().unwrap().contains("list_tables"),
            "{why}"
        );
        assert!(unsafe { infino_open_error() }.is_null(), "read once");
    }

    /// A result's JSON is built up to the bound and no further, and within
    /// it is the same text as unbounded.
    #[test]
    fn a_result_past_its_bound_is_never_built_whole() {
        let schema = Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, false)]));
        let batches = [RecordBatch::try_new(
            schema,
            vec![Arc::new(Int64Array::from(vec![1, 22, 333])) as Arc<dyn Array>],
        )
        .unwrap()];
        let whole = batches_to_json(&batches);
        assert_eq!(whole, r#"[{"n":1},{"n":22},{"n":333}]"#);
        assert_eq!(
            batches_to_json_within(&batches, whole.len()).as_deref(),
            Some(whole.as_str())
        );
        assert!(batches_to_json_within(&batches, whole.len() / 2).is_none());
        assert_eq!(batches_to_json(&[]), "[]");
    }

    /// A row id holds at most the column's precision in digits.
    #[test]
    fn a_row_id_wider_than_its_column_is_refused() {
        let id = DataType::Decimal128(ID_PRECISION, ID_SCALE);
        let widest = "9".repeat(ID_PRECISION as usize);
        assert_eq!(conform(json!(widest.clone()), &id), Ok(json!(widest)));
        assert_eq!(conform(json!("0042"), &id), Ok(json!("42")));
        let wider = "1".repeat(ID_PRECISION as usize + 1);
        assert!(conform(json!(wider), &id).unwrap_err().contains("digits"));
        assert!(conform(json!("9".repeat(60)), &id).is_err());
    }

    /// An engine error over a caller's statement names the indices the
    /// statement read, not the plugin's tables.
    #[test]
    fn an_engine_error_names_indices_not_tables() {
        let reads = read_sql::Reads {
            views: HashMap::from([
                ("logs".to_string(), "SELECT * FROM \"os_logs\"".to_string()),
                (
                    "logs_1".to_string(),
                    "SELECT * FROM \"os_logs_1\"".to_string(),
                ),
            ]),
            ..read_sql::Reads::default()
        };
        let tables = vec![
            "os_logs".to_string(),
            "os_logs_1".to_string(),
            "os_index".to_string(),
        ];
        assert_eq!(
            in_index_names(
                "table os_logs_1 has no column x; os_logs neither",
                &reads,
                &tables
            ),
            "table [logs_1] has no column x; [logs] neither"
        );
    }

    /// Every form OpenSearch's default date format takes is read as the
    /// instant OpenSearch indexes it at, `2026-11-23T` (a day with an empty
    /// time, as the sample flights write it) included; what it refuses is
    /// left to the engine's parser.
    #[test]
    fn a_date_is_read_as_opensearch_s_default_format_reads_it() {
        let cases = [
            ("2026-11-23T", 1_795_392_000_000),
            ("2026-11-23", 1_795_392_000_000),
            ("2026-11", 1_793_491_200_000),
            ("2026", 1_767_225_600_000),
            ("2026-11-23T10", 1_795_428_000_000),
            ("2026-11-23T10:30", 1_795_429_800_000),
            ("2026-11-23T10:30:15", 1_795_429_815_000),
            ("2026-11-23T10:30:15.250Z", 1_795_429_815_250),
            ("2026-11-23T10:30:15.250123456Z", 1_795_429_815_250),
            ("2026-11-23T10:30:15+02:00", 1_795_422_615_000),
            ("2026-11-23T10:30:15-0530", 1_795_449_615_000),
            ("2026-11-23T10:30:15+02", 1_795_422_615_000),
            ("1969-12-31T23:59:59Z", -1_000),
            ("2024-02-29", 1_709_164_800_000),
            ("1795392000000", 1_795_392_000_000),
            ("-1000", -1_000),
        ];
        for (sent, millis) in cases {
            assert_eq!(default_format_millis(sent), Some(millis), "{sent}");
        }
        for refused in [
            "2026-11-23 10:30:15",
            "2026-13-01",
            "2025-02-29",
            "2026-11-23T24:00",
            "2026-11-23T10:30:15.",
            "26-11-23",
            "2026-11-23Tnoon",
            "",
            "yesterday",
        ] {
            assert_eq!(default_format_millis(refused), None, "{refused}");
        }
        assert_eq!(
            conform(json!("2026-11-23T"), &date_type()).unwrap(),
            json!(1_795_392_000_000i64)
        );
        // A form only the engine reads is left to it, as before.
        assert_eq!(
            conform(json!("2026-11-23 10:30:15"), &date_type()).unwrap(),
            json!("2026-11-23 10:30:15")
        );
    }

    /// A date field is read by the format its mapping declares: an
    /// `epoch_second` value is seconds (the logs track's `@timestamp`), the
    /// field's forms are tried in order, and a value none of them reads is
    /// refused, as OpenSearch refuses it.
    #[test]
    fn a_date_field_is_read_by_its_own_format() {
        let seconds = DateFormat::parse_all("epoch_second");
        assert_eq!(
            conform_date(json!(893966296), &seconds),
            Ok(json!(893_966_296_000i64))
        );
        assert_eq!(
            conform_date(json!("893966296.25"), &seconds),
            Ok(json!(893_966_296_250i64))
        );
        assert_eq!(conform_date(json!(-1), &seconds), Ok(json!(-1_000i64)));
        assert_eq!(conform_date(Value::Null, &seconds), Ok(Value::Null));
        assert_eq!(
            conform_date(json!("1998-04-30T00:00:00Z"), &seconds),
            Err(
                "failed to parse date field [1998-04-30T00:00:00Z] with format [epoch_second]"
                    .into()
            )
        );
        let either = DateFormat::parse_all("strict_date_optional_time||epoch_second");
        assert_eq!(
            conform_date(json!("1998-04-30T00:00:00Z"), &either),
            Ok(json!(893_894_400_000i64))
        );
        assert_eq!(
            conform_date(json!(893966296), &either),
            Ok(json!(893_966_296_000i64))
        );
        let millis = DateFormat::parse_all("epoch_millis");
        assert_eq!(
            conform_date(json!(893966296000i64), &millis),
            Ok(json!(893_966_296_000i64))
        );
        // A custom pattern's value is read as the default format's, as before.
        let custom = DateFormat::parse_all("yyyy-MM-dd HH:mm:ss||strict_date_optional_time");
        assert_eq!(
            conform_date(json!("2026-11-23 10:30:15"), &custom),
            Ok(json!("2026-11-23 10:30:15"))
        );
        // A custom pattern does not stop the forms after it: an
        // `epoch_second` listed after one still reads a number as seconds,
        // a day of 1998 rather than of 1970.
        // Negative and fractional epochs read as stock OpenSearch 3.9.0's
        // `DateFormatter` reads them, to the millisecond toward the past: a
        // negative number of seconds keeps its fraction positive, a negative
        // number of milliseconds is negative throughout, and stock refuses a
        // negative number of seconds under one and a fraction past the
        // nanosecond.
        for (value, want) in [
            ("1.0005", Some(1_000i64)),
            ("1.123456789", Some(1_123)),
            ("-1.0005", Some(-1_000)),
            ("-1.5", Some(-500)),
            ("-2.25", Some(-1_750)),
            ("-1.999", Some(-1)),
            ("-1.9995", Some(-1)),
            ("-1.0000001", Some(-1_000)),
            ("-893966296.25", Some(-893_966_295_750)),
            ("-0", None),
            ("-0.5", None),
            ("1.1234567891", None),
        ] {
            assert_eq!(
                epoch_millis(value, MILLIS_PER_SECOND),
                want,
                "epoch_second {value}"
            );
        }
        for (value, want) in [
            ("123.5", Some(123i64)),
            ("123.999", Some(123)),
            ("-123", Some(-123)),
            ("-123.5", Some(-124)),
            ("-123.999", Some(-124)),
            ("-0.5", Some(-1)),
            ("-0", Some(0)),
            ("-0.000001", Some(-1)),
            ("1.1234567", None),
        ] {
            assert_eq!(epoch_millis(value, 1), want, "epoch_millis {value}");
        }
        let custom_then_seconds = DateFormat::parse_all("yyyy-MM-dd||epoch_second");
        assert_eq!(
            conform_date(json!(893966296), &custom_then_seconds),
            Ok(json!(893_966_296_000i64))
        );
        // `date_optional_time` is lenient: a month, day, hour or minute of
        // one digit reads as the two-digit one; the strict form refuses it,
        // and each refusal names the form the mapping declared.
        let lenient = DateFormat::parse_all("date_optional_time");
        assert_eq!(
            conform_date(json!("2026-4-30T9:05"), &lenient),
            Ok(json!(
                optional_time_millis("2026-04-30T09:05", true).expect("strict")
            ))
        );
        let strict = DateFormat::parse_all("strict_date_optional_time");
        assert_eq!(
            conform_date(json!("2026-4-30"), &strict),
            Err(
                "failed to parse date field [2026-4-30] with format [strict_date_optional_time]"
                    .into()
            )
        );
        assert_eq!(
            conform_date(json!("noon"), &lenient),
            Err("failed to parse date field [noon] with format [date_optional_time]".into())
        );
    }

    /// A string field's arrays of arrays are read through, as OpenSearch
    /// flattens them: a saved search's `sort` (`[["@timestamp", "desc"]]`)
    /// is two values, not a refusal.
    #[test]
    fn a_string_field_s_nested_arrays_are_its_values() {
        assert_eq!(
            conform(json!([["@timestamp", "desc"]]), &DataType::LargeUtf8).unwrap(),
            json!("@timestamp desc")
        );
        assert_eq!(
            conform(json!(["a", ["b", null, ["c"]], 7]), &DataType::LargeUtf8).unwrap(),
            json!("a b c 7")
        );
        assert!(conform(json!(["a", [{"b": 1}]]), &DataType::LargeUtf8).is_err());
    }

    /// A data stream's documents hold exactly one value at its timestamp
    /// field, named by the mapping however it is wrapped.
    #[test]
    fn a_stream_document_without_one_timestamp_is_refused() {
        let enabled = json!({"_data_stream_timestamp": {"enabled": true}});
        assert_eq!(stream_timestamp(&enabled).as_deref(), Some("@timestamp"));
        let named = json!({"_doc": {"_data_stream_timestamp": {"enabled": true, "timestamp_field": {"name": "at"}}}});
        assert_eq!(stream_timestamp(&named).as_deref(), Some("at"));
        assert_eq!(
            stream_timestamp(&json!({"_data_stream_timestamp": {"enabled": false}})),
            None
        );
        assert_eq!(stream_timestamp(&json!({"properties": {}})), None);

        let schema = Schema::new(vec![Field::new("message", DataType::LargeUtf8, true)]);
        let kept = HashMap::new();
        let check = |doc: &str| conform_doc(doc, &schema, &kept, Some("@timestamp"));
        assert!(check(r#"{"@timestamp": "2023-01-01T00:00:01Z"}"#).is_ok());
        assert!(check(r#"{"@timestamp": ["2023-01-01T00:00:01Z", null]}"#).is_ok());
        for doc in [
            r#"{"message": "m"}"#,
            r#"{"@timestamp": null}"#,
            r#"{"@timestamp": ["2023-01-01T00:00:01Z", "2023-01-01T00:00:02Z"]}"#,
        ] {
            assert_eq!(check(doc), Err(no_timestamp("@timestamp")), "{doc}");
        }
        assert!(conform_doc(r#"{"message": "m"}"#, &schema, &kept, None).is_ok());
    }

    /// A sorted page on a range-indexed field carries the field's values and
    /// no sources; a field named `score`, and any field on a hosted connection, is left to
    /// the hit statement (`null`) before any schema is read.
    #[test]
    fn a_sorted_page_carries_its_values_and_a_score_field_or_a_hosted_connection_is_left_to_the_statement(
    ) {
        let h = open_logs();
        let scored = r#"{"properties":{"score":{"type":"long"},"status":{"type":"long"}}}"#;
        take(unsafe { infino_ensure_table(h, c("scored").as_ptr(), c(scored).as_ptr()) });
        // `c` has no `status`, so only two of the three rows have a value:
        // the range indexes cannot answer the page and the engine plans its
        // statement.
        let ids = [c("a"), c("b"), c("c")];
        let srcs = [
            c(r#"{"score":2,"status":20}"#),
            c(r#"{"score":1,"status":10}"#),
            c(r#"{"score":3}"#),
        ];
        let idp: Vec<*const c_char> = ids.iter().map(|x| x.as_ptr()).collect();
        let sp: Vec<*const c_char> = srcs.iter().map(|x| x.as_ptr()).collect();
        take(unsafe { infino_index_batch(h, c("scored").as_ptr(), idp.as_ptr(), sp.as_ptr(), 3) });
        let sorted = move |column: &str| {
            take(unsafe { infino_sorted(h, c("scored").as_ptr(), c(column).as_ptr(), 0, 10) })
        };
        let page: Value = serde_json::from_str(&sorted("status")).unwrap();
        let rows = page.as_array().unwrap_or_else(|| panic!("{page}"));
        let got: Vec<(String, Option<i64>)> = rows
            .iter()
            .map(|r| (r[OSID].as_str().unwrap().to_string(), r["status"].as_i64()))
            .collect();
        assert_eq!(
            got,
            [
                ("b".to_string(), Some(10)),
                ("a".to_string(), Some(20)),
                ("c".to_string(), None)
            ]
        );
        // The statement's page still leaves the sources to the caller's
        // lookup by `osid`.
        assert!(rows.iter().all(|r| r.get(SOURCE).is_none()), "{page}");
        assert_eq!(sorted("score"), "null");
        unsafe { (*h).hosted = true };
        assert_eq!(sorted("status"), "null");
        unsafe { (*h).hosted = false };
        close(h);
    }
}
