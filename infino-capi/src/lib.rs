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

mod attached;
mod read_sql;

use std::borrow::Cow;
use std::cell::RefCell;
use std::cmp::Reverse;
use std::collections::{HashMap, HashSet};
use std::ffi::{c_char, CStr, CString};
use std::fs;
use std::io::{self, ErrorKind};
use std::iter;
use std::os::raw::c_int;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::path::{Path, PathBuf};
use std::process;
use std::ptr;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, OnceLock, PoisonError, RwLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use datafusion::arrow::compute::{concat_batches, filter_record_batch};
use datafusion::common::tree_node::TreeNode;
use datafusion::common::DFSchema;
use datafusion::optimizer::analyzer::type_coercion::TypeCoercionRewriter;
use datafusion::prelude::{col, lit, Expr, SessionContext};
use infino::arrow_array::{
    new_null_array, Array, BooleanArray, Decimal128Array, Float32Array, Float64Array, Int64Array,
    LargeStringArray, RecordBatch, StringArray, StructArray, TimestampMicrosecondArray,
    TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray, UInt64Array,
};
use infino::arrow_schema::{DataType, Field, Schema, TimeUnit};
use infino::serde_json::{self, json, Map, Value};
use infino::{
    connect_with, rows_to_batch, Analyzer, Bm25SearchOptions, BoolMode, ColumnIndex,
    ConnectOptions, Connection, Consistency, FieldPatch, FtsField, IndexSpec, InfinoError, Metric,
    OptimizeOptions, SchemaError, SchemaPatch, Supertable, TableSchema,
};
use rayon::prelude::*;

/// The OpenSearch `_id` of a row.
const OSID: &str = "osid";
/// The raw `_source` JSON of a row, served back by the fetch phase.
const SOURCE: &str = "source";
/// The row id column the engine keeps beside every row.
const ROW_ID: &str = "_id";
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
/// The sequence number of the operation that wrote a row, when the writer
/// records one: the highest in a table says how far into the writer's
/// history the table holds every write.
const OP: &str = "osop";
/// Which writer stamped a row's [`SEQ`] and [`OP`]: sequence numbers
/// compare only between rows of one writer's history, so a cut leaves
/// another's alone.
const GEN: &str = "osgen";
/// The fields OpenSearch's mapper left out of a row's document for a value
/// it could not parse (under `ignore_malformed`), by path: what a search
/// reads as the document's `_ignored` field. A list of strings, null for a
/// document with none; the column appears with the first such write.
const IGNORED: &str = "osignored";
/// What separates the paths of a document's ignored fields where the caller
/// hands them over: no field name holds it.
const IGNORED_SEPARATOR: char = '\n';
/// The columns every table keeps for itself. A document's field of the same
/// name is kept in a column of its own, named [`OWN_COLUMN_PREFIX`] and the
/// field's name: no mapping can name a field so.
const RESERVED: [&str; 4] = [OSID, SOURCE, SEQ, IGNORED];
const OWN_COLUMN_PREFIX: &str = ".";
/// Where a table's mapping declares, as `properties` declares fields, the
/// columns the plugin's engine writes beside a document's fields in default
/// mode ([`ROUTING`], [`OP`], [`GEN`]). Like [`RESERVED`], they hold no
/// field: a document's field of the same name gets a column of its own.
const ENGINE_COLUMNS: &str = "_engine_columns";
/// Separator used when a multi-valued string field lands in a scalar string
/// column: the values are joined so a text index sees every token. The
/// original array is still in `source`.
const MULTI_VALUE_SEPARATOR: &str = " ";
/// The longest UTF-8 encoding a `keyword` value may have, as Lucene bounds
/// one term.
const MAX_TERM_BYTES: usize = 32766;
/// The most UTF-8 bytes one UTF-16 unit encodes: a character in the Basic
/// Multilingual Plane is one unit in at most three bytes, one outside it two
/// units in four. A string has at least a third as many units as bytes.
const MAX_UTF8_BYTES_PER_UTF16_UNIT: usize = 3;
/// How Lucene's refusal of a value too long for one term starts
/// ([`immense_term`]); the plugin answers a reason that starts so as
/// OpenSearch answers it, an `illegal_argument_exception`.
const IMMENSE_TERM: &str = "Document contains at least one immense term in field=\"";
/// How many leading bytes of an immense term Lucene's refusal shows.
const IMMENSE_TERM_PREFIX_BYTES: usize = 30;
/// How Lucene's refusal of a value too long for a keyword's doc values
/// starts, for a keyword that has doc values but no index
/// ([`too_long_for_one_term`]); answered as [`IMMENSE_TERM`] is.
const DOC_VALUES_TOO_LARGE: &str = "DocValuesField \"";
/// The built-in normalizer that lowercases a keyword's values, the one
/// normalizer applied before a value's term length is checked.
const LOWERCASE_NORMALIZER: &str = "lowercase";
/// 'İ' (U+0130), whose lowercase Java and Rust read differently
/// ([`lowercase`]).
const DOTTED_CAPITAL_I: char = '\u{130}';
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
/// The name the startup storage check gives the file it writes and deletes
/// in a local storage root ([`check_local_root`]); the process id and a
/// clock reading follow, so nodes sharing a directory never collide.
const STORAGE_PROBE_NAME: &str = ".infino-storage-check";
/// Where an `s3://` node's store client asks for the instance role's
/// credentials: the engine reads no keys from anywhere else, so an instance
/// metadata service that does not answer means the node has no credentials.
const INSTANCE_METADATA: &str = "http://169.254.169.254";
/// The instance metadata service's token route, the first request the store
/// client makes for credentials.
const INSTANCE_METADATA_TOKEN: &str = "/latest/api/token";
/// The header that asks the token route for a token, and the token's life
/// in seconds; the storage check asks for one only to see that it answers.
const INSTANCE_METADATA_TTL_HEADER: &str = "X-aws-ec2-metadata-token-ttl-seconds";
const INSTANCE_METADATA_TTL_SECS: &str = "60";
/// How long the storage check waits for the instance metadata service. It
/// answers within milliseconds when it answers at all: in Docker on EC2 at a
/// hop limit of 1 its answer never reaches the container, and the store
/// client retries for minutes before it reports that.
const INSTANCE_METADATA_WAIT: Duration = Duration::from_secs(2);
/// What the engine's hosted client puts before the HTTP status in the message
/// of a refused call; the status is read back off it (see `status_in`).
const STATUS_MARKER: &str = "server returned ";
/// Digits of an HTTP status code.
const STATUS_DIGITS: usize = 3;
/// The status of an error from a hosted service that could not be reached
/// or did not answer in time: a later try may pass.
const SERVICE_UNAVAILABLE: u16 = 503;
/// What the caller is told of a call the hosted service could not be
/// reached for or did not answer ([`InfinoError::Unavailable`]), ahead of
/// the engine's error.
const UNANSWERED: &str =
    "the hosted service could not be reached or did not answer in time; try again later";
/// HTTP status of a caller's statement that is wrong: it does not parse or
/// plan, or asks for what the engine does not do.
const STATUS_BAD_REQUEST: u16 = 400;
/// HTTP status of a caller's statement naming a column or table that is not there.
const STATUS_NOT_FOUND: u16 = 404;
/// HTTP status of a caller's statement past the connection's memory budget,
/// and of a call the hosted service turned away for the account's request
/// rate ([`rate_limited`]).
const STATUS_TOO_MANY_REQUESTS: u16 = 429;
/// HTTP status of a request the hosted service refuses for its size.
const STATUS_PAYLOAD_TOO_LARGE: u16 = 413;
/// HTTP status the hosted service answers when it has no capacity for a
/// call (a site overloaded): like a 429, it wrote nothing and a later try
/// may pass ([`rate_limited`]). The bridge answers it as a 429, the status
/// OpenSearch clients send a write again for; 529 is no standard status, and
/// OpenSearch has none for it.
const STATUS_OVERLOADED: u16 = 529;
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
    /// Why the last `infino_prepare_batch` (or `_meta`) on this thread
    /// returned null, as the error JSON `infino_prepare_error` hands back.
    static PREPARE_ERROR: RefCell<Option<String>> = const { RefCell::new(None) };
}

/// Arrow type of a mapped `date`: epoch milliseconds in UTC, which is what
/// OpenSearch stores and what its date queries compare against.
/// `index` with token positions recorded or not; any other kind of index as it is.
fn with_positions(mut index: ColumnIndex, on: bool) -> ColumnIndex {
    if let ColumnIndex::Fts { positions, .. } = &mut index {
        *positions = on;
    }
    index
}

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

/// OpenSearch's date format for a date field whose mapping names none.
const DEFAULT_DATE_FORMAT: &str = "strict_date_optional_time||epoch_millis";

/// Epoch milliseconds of `s` as OpenSearch's default date format reads it,
/// `strict_date_optional_time||epoch_millis`: a date `yyyy[-MM[-dd]]`, then
/// optionally `T` and `HH[:mm[:ss[.fraction]]]`, any of it (`2026-11-23T`
/// is a day), then optionally a zone (`Z`, `±HH`, `±HHmm`, `±HH:mm`), UTC
/// when there is none; failing that, a whole number of milliseconds. `None`
/// for any other string.
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
/// custom pattern) does not stop the others. A value none of them reads
/// fails its document here: one in a custom pattern is read by OpenSearch's
/// mapper, whose values the caller sends with the document
/// ([`mapped_values`]).
fn conform_date(value: Value, formats: &[DateFormat]) -> Result<Value, String> {
    let text = match &value {
        Value::Null => return Ok(value),
        Value::String(s) => s.clone(),
        Value::Number(n) => n.to_string(),
        _ => return Err("not a date".into()),
    };
    for format in formats {
        let millis = match format {
            DateFormat::OptionalTime { strict } => optional_time_millis(&text, *strict),
            DateFormat::EpochMillis => epoch_millis(&text, 1),
            DateFormat::EpochSecond => epoch_millis(&text, MILLIS_PER_SECOND),
            DateFormat::Other(_) => None,
        };
        if let Some(millis) = millis {
            return Ok(json!(millis));
        }
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

/// The strings of a C array of `count` of them; one that is null or not
/// UTF-8 is left out, and a null array is none.
unsafe fn cstrs<'a>(p: *const *const c_char, count: c_int) -> Vec<&'a str> {
    if p.is_null() || count <= 0 {
        return Vec::new();
    }
    unsafe { std::slice::from_raw_parts(p, count as usize) }
        .iter()
        .filter_map(|s| unsafe { cstr(*s) })
        .collect()
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
    /// The schema document of each table, on a hosted connection, where
    /// reading it is a request to the service ([`Bridge::table_schema`]).
    /// Dropped with the table's handle ([`Bridge::forget`]), and replaced on
    /// its own after a write that added a column ([`Bridge::reread_schema`]),
    /// so it may hold columns the handle's Arrow schema, fixed when the
    /// handle is opened, lacks.
    schemas: RwLock<HashMap<String, TableSchema>>,
    /// How many times each table's kept handle or schema document has been
    /// dropped ([`Bridge::forget`]) or set to be replaced
    /// ([`Bridge::replace_schema`]). A read keeps what it read only if the
    /// count has not moved meanwhile ([`Bridge::keep_unless_moved`]). Never
    /// reset, not even when a table is dropped: a read from before the drop
    /// must not find the count it started from again.
    forgets: Mutex<HashMap<String, u64>>,
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
    /// The columns each table's engine writes, from the mapping it was last
    /// declared from ([`ENGINE_COLUMNS`]).
    engine: RwLock<HashMap<String, Arc<Vec<String>>>>,
    /// What each table's mapping indexes of a document ([`Indexed`]), from
    /// the mapping it was last declared from: a path it does not index is
    /// left out of the row and kept in the source only.
    indexed: RwLock<HashMap<String, Arc<Indexed>>>,
    /// The tables an index is attached to rather than owns: tables that
    /// existed before the index, declared through `infino_attach_table`.
    /// Such a table is read as it is and never changed: it holds none of
    /// the columns the plugin writes beside a document ([`OSID`],
    /// [`SOURCE`], [`SEQ`]), and none is added to it. Each is kept with
    /// the columns it held when it was declared, which a document read
    /// from it is made of (see [`attached`]), and the fields its mapping
    /// reads as text.
    attached: RwLock<HashMap<String, Attached>>,
    /// Each table's [`Gate`], by name: the order in which this node's
    /// writes take the table's writer.
    gates: Mutex<HashMap<String, Arc<Gate>>>,
    /// How [`Bridge::latest`] connects: the storage, key and cache
    /// directory `conn` was opened with. None where `conn` already reads
    /// every commit there is to read: a hosted service, which answers from
    /// its own copy, and `memory://`, which no other process writes.
    latest_from: Option<LatestFrom>,
    /// The connection [`Bridge::latest`] opened, while a reader holds it.
    latest: Mutex<Option<Connection>>,
    /// The readers holding [`Bridge::latest`]'s connection open
    /// ([`Bridge::hold_latest`]); it is closed when the last lets it go.
    latest_holds: AtomicUsize,
}

/// What [`Bridge::latest`] opens its connection from.
struct LatestFrom {
    uri: String,
    api_key: Option<String>,
    cache_dir: Option<String>,
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

    /// The columns of `name` its engine writes ([`ENGINE_COLUMNS`]).
    fn engine_columns(&self, name: &str) -> Arc<Vec<String>> {
        self.engine
            .read()
            .ok()
            .and_then(|all| all.get(name).cloned())
            .unwrap_or_default()
    }

    /// Record the columns `name`'s engine writes, replacing the earlier ones.
    fn engine_rule(&self, name: &str, columns: Vec<String>) {
        if let Ok(mut all) = self.engine.write() {
            all.insert(name.to_string(), Arc::new(columns));
        }
    }

    /// What the mapping of `name` indexes of a document ([`Indexed`]);
    /// every path for a table no mapping has declared.
    fn indexed(&self, name: &str) -> Arc<Indexed> {
        self.indexed
            .read()
            .ok()
            .and_then(|all| all.get(name).cloned())
            .unwrap_or_default()
    }

    /// Record what `mapping` indexes for `name`, replacing the earlier rule.
    fn index_rule(&self, name: &str, mapping: &Value) {
        if let Ok(mut all) = self.indexed.write() {
            all.insert(name.to_string(), Arc::new(Indexed::of(mapping)));
        }
    }

    /// Whether `name` is a table an index is attached to rather than owns.
    fn is_attached(&self, name: &str) -> bool {
        self.attached_columns(name).is_some()
    }

    /// The columns of `name` when an index is attached to it, none when it
    /// is a table of the plugin's own.
    fn attached_columns(&self, name: &str) -> Option<Arc<Vec<String>>> {
        self.attached
            .read()
            .ok()
            .and_then(|all| all.get(name).map(|a| a.columns.clone()))
    }

    /// Whether the mapping of the index attached to `name` reads `column`
    /// as text. A full-text index the table's maker built is searched as
    /// text only under such a field: under a `keyword` field, or under no
    /// field, the column is compared whole.
    fn attached_text(&self, name: &str, column: &str) -> bool {
        self.attached
            .read()
            .is_ok_and(|all| all.get(name).is_some_and(|a| a.text.contains(column)))
    }

    /// Record `name` as attached with what it is read with, or as a table
    /// of the plugin's own, from the declaration that says so.
    fn attach_rule(&self, name: &str, attached: Option<Attached>) {
        if let Ok(mut all) = self.attached.write() {
            match attached {
                Some(attached) => all.insert(name.to_string(), attached),
                None => all.remove(name),
            };
        }
    }

    /// Why a write to `name` is refused: an index is attached to it, and
    /// such a table is read and never changed. Every entry point that
    /// appends to a table, deletes from it, adds a column to it, compacts
    /// it or drops it asks here first, so no path the plugin has reaches
    /// the table, whichever filter or engine it came through.
    fn refuse_write(&self, name: &str) -> Result<(), String> {
        if self.is_attached(name) {
            return Err(format!(
                "table {name} is read by an attached index and is never written through it"
            ));
        }
        Ok(())
    }

    /// `sql` as it reads the attached tables it names, each through the
    /// view that gives a document's id and source from the table's own
    /// columns ([`attached::rewrite`]); none when it names no attached
    /// table and runs as written, which is every statement on a node with
    /// no attached index.
    fn attached_sql(&self, sql: &str) -> Option<String> {
        if self.attached.read().map_or(true, |all| all.is_empty()) {
            return None;
        }
        // A statement over the table's own columns alone needs no view, and
        // keeps the plan the table gives it (see `reads_view_columns`).
        if !attached::reads_view_columns(sql) {
            return None;
        }
        attached::rewrite(sql, &|table| {
            self.attached_columns(table).map(|c| c.as_ref().clone())
        })
    }

    /// Read again the columns of every attached table, which its maker may
    /// have changed since the index was declared. Whether any differed.
    fn refresh_attached(&self) -> bool {
        let names: Vec<String> = match self.attached.read() {
            Ok(all) => all.keys().cloned().collect(),
            Err(_) => return false,
        };
        let mut changed = false;
        for name in names {
            self.forget(&name);
            let Ok(table) = self.conn.open_table(&name) else {
                continue;
            };
            let columns = attached::columns_of(&table.schema());
            if let Ok(mut all) = self.attached.write() {
                if let Some(held) = all.get_mut(&name) {
                    if *held.columns != columns {
                        held.columns = Arc::new(columns);
                        changed = true;
                    }
                }
            }
        }
        changed
    }

    /// Run `sql` with each attached table it names read through its view
    /// ([`Bridge::attached_sql`]). The view names the columns the table
    /// held when its index was declared; where the statement fails and the
    /// table's columns have changed since, it runs once more over the
    /// columns the table holds now.
    fn query_viewed(&self, sql: &str) -> Result<Vec<RecordBatch>, InfinoError> {
        self.query_viewed_on(&self.conn, sql)
    }

    /// [`Bridge::query_viewed`] on `conn`, which may be [`Bridge::latest`].
    fn query_viewed_on(
        &self,
        conn: &Connection,
        sql: &str,
    ) -> Result<Vec<RecordBatch>, InfinoError> {
        let Some(viewed) = self.attached_sql(sql) else {
            return conn.query_sql(sql);
        };
        match conn.query_sql(&viewed) {
            Err(_) if self.refresh_attached() => {
                let viewed = self.attached_sql(sql);
                conn.query_sql(viewed.as_deref().unwrap_or(sql))
            }
            other => other,
        }
    }

    /// A connection that reads a table's latest commit on every statement,
    /// for reads that decide what a write does: whether a replayed
    /// operation's id is already a row (and so is replaced, not appended
    /// again), and where a replay starts. `conn` reads under the engine's
    /// default staleness bound, so for up to its window after another node
    /// commits it can answer from the commit before; a shard taking over
    /// from another node replays that node's operations within that window
    /// and would append each again. The engine's own update and delete read
    /// the latest commit to find their rows; it offers a caller no way to
    /// do so on a connection opened under the bound, so these reads go
    /// through a connection opened for it. It shares `conn`'s cache
    /// directory, which the engine supports for two connections, so it reads
    /// the blocks the node already holds. Opened on the first such read and
    /// closed when no reader holds it ([`Bridge::hold_latest`]): it holds a
    /// handle to each table it has read, as `conn` does, and each handle
    /// keeps that table's manifest in memory, which a node needs twice only
    /// while a shard catches up with the copy before it.
    fn latest(&self) -> Result<Connection, InfinoError> {
        let Some(from) = &self.latest_from else {
            return Ok(self.conn.clone());
        };
        let mut latest = self.latest.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(conn) = latest.as_ref() {
            return Ok(conn.clone());
        }
        let mut options = ConnectOptions::new().with_read_consistency(Consistency::Strong);
        if let Some(key) = &from.api_key {
            options = options.with_api_key(key);
        }
        if let Some(dir) = &from.cache_dir {
            options = options.with_cache_dir(dir);
        }
        let conn = connect_with(&from.uri, options)?;
        *latest = Some(conn.clone());
        Ok(conn)
    }

    fn table(&self, name: &str) -> Result<Supertable, InfinoError> {
        self.kept_or_read(name, &self.tables, || self.conn.open_table(name))
    }

    /// The schema document of `name`. On a hosted connection, reading it is
    /// a request to the service, so it is kept until the table's handle is
    /// dropped ([`Bridge::forget`], which every change to the table's schema
    /// here makes) or a write adds a column to it
    /// ([`Bridge::reread_schema`]): preparing a batch then waits on the
    /// service only right after such a change, not for each batch. A
    /// connection over storage answers from the handle it holds open, and is
    /// asked each time.
    fn table_schema(&self, name: &str) -> Result<TableSchema, InfinoError> {
        if !self.hosted {
            return self.conn.schema(name);
        }
        self.kept_or_read(name, &self.schemas, || self.conn.schema(name))
    }

    /// What `kept` holds for `name`, or else what `read` gives, which is
    /// kept unless `name` was forgotten while `read` ran. A read that began
    /// before a schema change and ends after the change's forget would
    /// otherwise keep the table as it was before, until the next forget.
    fn kept_or_read<T: Clone>(
        &self,
        name: &str,
        kept: &RwLock<HashMap<String, T>>,
        read: impl FnOnce() -> Result<T, InfinoError>,
    ) -> Result<T, InfinoError> {
        if let Some(v) = kept.read().map_err(poisoned)?.get(name) {
            return Ok(v.clone());
        }
        let seen = self.forgets_of(name);
        let v = read()?;
        self.keep_unless_moved(name, kept, seen, &v)?;
        Ok(v)
    }

    /// Keep `v` for `name` in `kept` if `name`'s count is still `seen`, the
    /// count when the read that gave `v` began ([`Bridge::forgets`]).
    fn keep_unless_moved<T: Clone>(
        &self,
        name: &str,
        kept: &RwLock<HashMap<String, T>>,
        seen: u64,
        v: &T,
    ) -> Result<(), InfinoError> {
        // Held while the value is kept, so a forget runs wholly before the
        // check or wholly after the insert.
        let forgets = self.forgets.lock().unwrap_or_else(PoisonError::into_inner);
        if forgets.get(name).copied().unwrap_or(0) == seen {
            kept.write()
                .map_err(poisoned)?
                .insert(name.to_string(), v.clone());
        }
        Ok(())
    }

    /// How many times `name`'s kept handle or schema document has been
    /// dropped ([`Bridge::forgets`]).
    fn forgets_of(&self, name: &str) -> u64 {
        self.forgets
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(name)
            .copied()
            .unwrap_or(0)
    }

    /// Count a forget of `name` and run `drop`, the dropping of what is kept
    /// for it, under the count's lock ([`Bridge::keep_unless_moved`]).
    /// Returns the count after this forget.
    fn forgetting(&self, name: &str, drop: impl FnOnce()) -> u64 {
        let mut forgets = self.forgets.lock().unwrap_or_else(PoisonError::into_inner);
        let count = forgets.entry(name.to_string()).or_default();
        *count += 1;
        drop();
        *count
    }

    fn forget(&self, name: &str) {
        self.forgetting(name, || {
            if let Ok(mut tables) = self.tables.write() {
                tables.remove(name);
            }
            if let Ok(mut schemas) = self.schemas.write() {
                schemas.remove(name);
            }
        });
    }

    /// Read `name`'s schema document again after a write that added a
    /// column to it: the engine adds one during an append as well as during
    /// a declaration (a geo_point's first values, a flat_object's members, a
    /// path the mapping does not declare), and the kept document would
    /// otherwise lack it until the next declaration. Read on the writing
    /// thread, which has just been answered by the service, and swapped in
    /// once read ([`Bridge::replace_schema`]). The handle is kept: a path
    /// its Arrow schema lacks is converted to the column this document holds
    /// ([`rows_to_batch`]).
    fn reread_schema(&self, name: &str) {
        if !self.hosted {
            return;
        }
        let _ = self.replace_schema(name, || self.conn.schema(name));
    }

    /// Replace `name`'s kept schema document with what `read` gives. The old
    /// document stays kept while `read` runs, so a thread that stages a bulk
    /// meanwhile reads it rather than asking the service, and stays kept if
    /// `read` fails: a stalled service then costs stale columns until the
    /// next write that adds one, not a staging thread waiting on the
    /// service. The count is moved before `read` begins, so a read that
    /// began earlier, a miss or another replace, keeps nothing, and the
    /// replace that began last is the one kept; a forget while `read` runs
    /// moves the count again, and this replace keeps nothing either.
    fn replace_schema(
        &self,
        name: &str,
        read: impl FnOnce() -> Result<TableSchema, InfinoError>,
    ) -> Result<TableSchema, InfinoError> {
        let seen = self.forgetting(name, || {});
        let doc = read()?;
        self.keep_unless_moved(name, &self.schemas, seen, &doc)?;
        Ok(doc)
    }

    /// Keep [`Bridge::latest`]'s connection open until the hold is let go
    /// ([`Bridge::release_latest`]), for a reader that reads the latest
    /// commit several times: a shard catching up with the copy before it
    /// reads it for each batch it writes until its own first commit. Each
    /// latest read holds it while it runs, so a read no one holds for closes
    /// it when done.
    fn hold_latest(&self) {
        self.latest_holds.fetch_add(1, Ordering::SeqCst);
    }

    /// Let go of a hold [`Bridge::hold_latest`] took, closing
    /// [`Bridge::latest`]'s connection when it was the last. The count is
    /// read again under the connection's lock, so a reader that took a hold
    /// meanwhile keeps the connection it may already be reading through.
    fn release_latest(&self) {
        let held = self
            .latest_holds
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1));
        if held.is_err() {
            return;
        }
        let mut latest = self.latest.lock().unwrap_or_else(PoisonError::into_inner);
        if self.latest_holds.load(Ordering::SeqCst) == 0 {
            *latest = None;
        }
    }

    /// `read` on [`Bridge::latest`]'s connection, held while it runs. A read
    /// that fails is read once more on a connection opened afresh: the one
    /// before may hold a handle to a table another node has dropped and made
    /// again under its name, which answers "not found" until the engine
    /// notices the drop, and a search reports that as a failed query.
    fn on_latest<T>(
        &self,
        read: impl Fn(&Connection) -> Result<T, InfinoError>,
    ) -> Result<T, InfinoError> {
        self.hold_latest();
        let _held = LatestHold(self);
        self.latest()
            .and_then(|conn| read(&conn))
            .or_else(|e| match e {
                // Where `conn` is the connection there is no other to open.
                _ if self.latest_from.is_some() => {
                    self.forget_latest();
                    self.latest().and_then(|conn| read(&conn))
                }
                e => Err(e),
            })
    }

    /// [`Bridge::query_viewed`] on [`Bridge::latest`] ([`Bridge::on_latest`]).
    fn query_latest(&self, sql: &str) -> Result<Vec<RecordBatch>, InfinoError> {
        self.on_latest(|conn| self.query_viewed_on(conn, sql))
    }

    /// Close [`Bridge::latest`]'s connection, so the next latest read opens
    /// one afresh. It holds a handle to each table it has read, and a handle
    /// to a table dropped here would answer the next read of a table made
    /// again under its name with "not found" ([`Bridge::forget`] does the same
    /// for `conn`'s handles, which this bridge holds itself).
    fn forget_latest(&self) {
        *self.latest.lock().unwrap_or_else(PoisonError::into_inner) = None;
    }

    /// How many bytes of ids one mutation may name ([`HOSTED_ID_LIST_BYTES`]
    /// on the hosted service; no limit over storage of the bridge's own,
    /// where a predicate never travels in a URL).
    fn id_list_bytes(&self) -> usize {
        if self.hosted {
            HOSTED_ID_LIST_BYTES
        } else {
            usize::MAX
        }
    }

    /// Whether a keyword field's index takes the engine's `keyword`
    /// analyzer, one term per value. Not on the hosted service, which
    /// refuses a table that declares it: there a keyword field keeps the
    /// default analyzer without positions, and a terms aggregation counts
    /// its values with a statement ([`infino_value_counts_top`] declines).
    /// Drop this once the hosted service takes the analyzer (#274); hosted
    /// tables made before then keep the default analyzer until reloaded.
    fn keyword_analyzer(&self) -> bool {
        !self.hosted
    }

    /// The [`Gate`] of `name`'s writes on this node.
    fn gate(&self, name: &str) -> Arc<Gate> {
        let mut gates = self.gates.lock().unwrap_or_else(PoisonError::into_inner);
        Arc::clone(gates.entry(name.to_string()).or_default())
    }
}

/// A hold on [`Bridge::latest`]'s connection for one read
/// ([`Bridge::on_latest`]), let go however the read ends.
struct LatestHold<'a>(&'a Bridge);

impl Drop for LatestHold<'_> {
    fn drop(&mut self) {
        self.0.release_latest();
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

/// [`error_value`] of an engine error. When the hosted service could not be
/// reached or did not answer in time ([`InfinoError::Unavailable`]: a
/// transport failure, or a 502, 503 or 504 from the service), the message
/// says so, `"unavailable": true` marks it, and the status is 503 unless
/// the service answered with a status of its own. The caller is told to try
/// again later rather than that the request failed, and can tell this case
/// apart whatever the status. A call the service turned away for the
/// account's request rate or for want of capacity ([`rate_limited`]) is a
/// 429, a 529 included.
fn engine_error_value(e: &InfinoError) -> Value {
    if rate_limited(e) {
        // A 529 is answered as the 429 a client sends a write again for.
        let mut v = error_value(&e.to_string());
        v["status"] = json!(STATUS_TOO_MANY_REQUESTS);
        return v;
    }
    if !matches!(e, InfinoError::Unavailable(_)) {
        return error_value(&e.to_string());
    }
    let mut v = error_value(&format!("{UNANSWERED}: {e}"));
    v["unavailable"] = json!(true);
    if v.get("status").is_none() {
        v["status"] = json!(SERVICE_UNAVAILABLE);
    }
    v
}

fn engine_err_json(e: &InfinoError) -> *mut c_char {
    out_json(engine_error_value(e))
}

/// The error object of a failed read by id: [`engine_error_value`] of the
/// error, so a read the hosted service did not answer is a 503 the caller
/// can retry, with the call and what it read (the id, or the table for a
/// read of many ids) named in the message. The status is read off the
/// engine's error alone, so an id that spells `server returned 200` cannot
/// set it.
fn read_err_json(call: &str, what: &str, e: &InfinoError) -> *mut c_char {
    let mut v = engine_error_value(e);
    let message = v["error"].as_str().unwrap_or_default().to_string();
    v["error"] = json!(format!("{call} {what}: {message}"));
    out_json(v)
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
    let staleness = READ_STALENESS_MS.load(Ordering::SeqCst);
    if staleness > 0 && !hosted {
        options = options.with_read_consistency(Consistency::BoundedStaleness(
            Duration::from_millis(staleness),
        ));
    }
    let conn = connect_with(uri, options)?;
    let latest_from = (!hosted && !uri.starts_with(MEMORY_SCHEME)).then(|| LatestFrom {
        uri: uri.to_string(),
        api_key: api_key.map(str::to_string),
        cache_dir: cache_dir.map(str::to_string),
    });
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
        schemas: RwLock::new(HashMap::new()),
        forgets: Mutex::new(HashMap::new()),
        hosted,
        kept: RwLock::new(HashMap::new()),
        stamps: RwLock::new(HashMap::new()),
        engine: RwLock::new(HashMap::new()),
        indexed: RwLock::new(HashMap::new()),
        attached: RwLock::new(HashMap::new()),
        gates: Mutex::new(HashMap::new()),
        latest_from,
        latest: Mutex::new(None),
        latest_holds: AtomicUsize::new(0),
    })
}

/// The scheme of a catalog held in this process's memory.
const MEMORY_SCHEME: &str = "memory://";

/// The staleness window the connections opened from here on read under, in
/// milliseconds ([`infino_set_read_staleness_ms`]); 0 for the engine's
/// default.
static READ_STALENESS_MS: AtomicU64 = AtomicU64::new(0);

/// Open the connections from here on to read under a staleness window of
/// `ms` milliseconds rather than the engine's default (0 restores it). For
/// tests only: a wide window makes a read that should have gone to the
/// table's latest commit, and went to a node's own connection instead,
/// answer from the commit before every time rather than now and then. The
/// connection that reads every commit ([`Bridge::latest`]) is unchanged.
#[no_mangle]
pub extern "C" fn infino_set_read_staleness_ms(ms: u64) {
    READ_STALENESS_MS.store(ms, Ordering::SeqCst);
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
        let uri = unsafe { cstr(uri) }.unwrap_or(MEMORY_SCHEME);
        let api_key = unsafe { cstr(api_key) }.filter(|k| !k.is_empty());
        let cache_dir = unsafe { cstr(cache_dir) }.filter(|d| !d.is_empty());
        match open(uri, api_key, cache_dir) {
            Ok(bridge) => {
                OPEN_ERROR.with(|slot| *slot.borrow_mut() = None);
                Box::into_raw(Box::new(bridge))
            }
            Err(e) => {
                eprintln!("[capi] connect: {e}");
                OPEN_ERROR
                    .with(|slot| *slot.borrow_mut() = Some(engine_error_value(&e).to_string()));
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

/// Check once that the node can use the connection's own storage, as the
/// node starts. Opening it reads nothing, so a store the node cannot use (no
/// credentials, a directory it may not write) would otherwise fail each
/// table operation on its own, the first of them while the node starts.
///
/// For local storage it writes a small file in the root and deletes it,
/// which finds a root the node may not write. For `s3://` it first asks the
/// instance metadata service for a token, which finds a node with no
/// credentials in seconds rather than after the store client's minutes of
/// retries. Then it lists the tables, as the node does before it serves any. Answers "" when both work, else
/// `{"error", "permanent"}`: `permanent` is true only for a failure that
/// will not pass on its own (the storage refused the node's credentials or
/// its user), and false for one that may (a timeout, a 5xx, a dropped
/// connection). The caller bounds how long it waits.
#[no_mangle]
pub unsafe extern "C" fn infino_check_storage(h: *mut Bridge) -> *mut c_char {
    catch_unwind(AssertUnwindSafe(|| {
        let Some(bridge) = bridge_ref(h) else {
            return err_json("null argument");
        };
        let uri = bridge.latest_from.as_ref().map(|from| from.uri.as_str());
        let root = uri.and_then(local_root);
        let metadata = uri
            .filter(|u| u.starts_with("s3://"))
            .map(|_| INSTANCE_METADATA);
        match check_storage(&bridge.conn, root.as_deref(), metadata) {
            Ok(()) => out_string(String::new()),
            Err(failure) => {
                out_json(json!({ "error": failure.reason, "permanent": failure.permanent }))
            }
        }
    }))
    .unwrap_or(ptr::null_mut())
}

/// Why [`infino_check_storage`] failed, and whether the failure lasts.
#[derive(Debug)]
struct StorageFailure {
    reason: String,
    permanent: bool,
}

/// [`infino_check_storage`]'s checks: the write probe of a local `root`, or
/// for an `s3://` node that its instance `metadata` service answers; then
/// the listing. A catalog with no tables yet counts as an answer.
fn check_storage(
    conn: &Connection,
    root: Option<&Path>,
    metadata: Option<&str>,
) -> Result<(), StorageFailure> {
    if let Some(root) = root {
        check_local_root(root)?;
    }
    if let Some(metadata) = metadata {
        check_instance_metadata(metadata, INSTANCE_METADATA_WAIT)?;
    }
    match conn.list_tables() {
        Ok(_) | Err(InfinoError::NotFound(_)) => Ok(()),
        Err(e) => Err(StorageFailure {
            permanent: matches!(e, InfinoError::PermissionDenied(_)),
            reason: e.to_string(),
        }),
    }
}

/// Write a small file in a local storage `root` and delete it: the engine
/// creates the root and writes there on the first table, so a root the
/// node's user may not write fails here instead. Creating the root is what
/// that first write would do anyway.
fn check_local_root(root: &Path) -> Result<(), StorageFailure> {
    let stamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or_default();
    let probe = root.join(format!("{STORAGE_PROBE_NAME}-{}-{stamp}", process::id()));
    let failed = |e: io::Error| StorageFailure {
        reason: format!("{}: {e}", root.display()),
        permanent: lasts(e.kind()),
    };
    fs::create_dir_all(root).map_err(failed)?;
    fs::write(&probe, b"").map_err(failed)?;
    fs::remove_file(&probe).map_err(failed)
}

/// Ask the instance metadata service at `base` for a token, as the store
/// client does first when it fetches the instance role's credentials, and
/// wait at most `wait`. Any HTTP answer means the service is reachable and
/// the listing decides; no answer means the node has no credentials. That is
/// reported at once and named, where the store client would report it only
/// after minutes of retries, as an error about a token. Not permanent: the
/// service can come back.
fn check_instance_metadata(base: &str, wait: Duration) -> Result<(), StorageFailure> {
    let agent = ureq::AgentBuilder::new().timeout(wait).build();
    let asked = agent
        .put(&format!("{base}{INSTANCE_METADATA_TOKEN}"))
        .set(INSTANCE_METADATA_TTL_HEADER, INSTANCE_METADATA_TTL_SECS)
        .call();
    match asked {
        Ok(_) | Err(ureq::Error::Status(..)) => Ok(()),
        Err(ureq::Error::Transport(t)) => Err(StorageFailure {
            reason: format!(
                "no AWS credentials: the instance metadata service at {base} did not answer within {wait:?} ({t})"
            ),
            permanent: false,
        }),
    }
}

/// Whether a local file error is one that will not pass on its own: the
/// node's user may not write there, the file system is read-only, or the
/// root is not a directory.
fn lasts(kind: ErrorKind) -> bool {
    matches!(
        kind,
        ErrorKind::PermissionDenied
            | ErrorKind::ReadOnlyFilesystem
            | ErrorKind::NotADirectory
            | ErrorKind::AlreadyExists
    )
}

/// The directory of a local storage URI, read as the engine reads it:
/// `file://<path>`, or a bare path (`/data/infino`, `./data`). None for
/// any other storage, and for the in-memory catalog.
fn local_root(uri: &str) -> Option<PathBuf> {
    if let Some(path) = uri.strip_prefix("file://") {
        return Some(PathBuf::from(path));
    }
    let memory = matches!(uri, "memory://" | "memory:" | "memory");
    (!memory && !uri.contains("://")).then(|| PathBuf::from(uri))
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
/// What an attached table is read with (see [`Bridge::attached`]).
#[derive(Clone)]
struct Attached {
    /// The columns the table held when it was last read, its row id aside.
    columns: Arc<Vec<String>>,
    /// The fields the index's mapping reads as text.
    text: Arc<HashSet<String>>,
}

struct MappedField {
    path: String,
    data_type: DataType,
    fts: bool,
    /// A `keyword` field the mapping indexes: the table gives it a full-text
    /// index without positions, so `=` and `IN` are answered from its
    /// dictionary instead of a scan. Reported to the plugin as no text index
    /// ([`infino_columns`]), so it is still read as a keyword.
    exact: bool,
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
    /// For a `keyword` field, how it indexes a value.
    keyword: Option<KeywordIndexing>,
    /// The field's `keyword` multi-fields that build terms of its values
    /// (the `.keyword` sub-field dynamic mapping gives a string, say). They
    /// have no column of their own, but OpenSearch indexes each value in
    /// them too, so a value too long for one term refuses the document.
    multi: Vec<TermView>,
    /// What OpenSearch accepts of an `ip` or narrow integer field beyond what
    /// its column takes.
    valid: Option<Valid>,
}

impl MappedField {
    /// How the field keeps its values beyond its column's type, if it does.
    fn kept(&self) -> Option<Kept> {
        self.precision
            .map(Kept::Precision)
            .or_else(|| self.date_formats.clone().map(Kept::Date))
            .or_else(|| {
                self.keyword.map(|indexing| Kept::Keyword {
                    indexing,
                    multi: self.multi.clone(),
                })
            })
            .or_else(|| self.valid.map(Kept::Valid))
            .or_else(|| (!self.multi.is_empty()).then(|| Kept::MultiFields(self.multi.clone())))
    }
}

/// Where a `keyword` field's terms go, which decides the words Lucene
/// refuses a value too long for one term in ([`too_long_for_one_term`]).
#[derive(Clone, Copy, Debug, PartialEq)]
enum Terms {
    /// The field is indexed (whether or not it also has doc values).
    Postings,
    /// The field has doc values but no index.
    DocValues,
}

/// How a `keyword` mapping indexes a value, as OpenSearch's keyword mapper
/// does: not at all past `ignore_above` (counted in UTF-16 units, on the
/// value as written), else into its terms ([`Terms`]), if it builds any,
/// where one term holds at most [`MAX_TERM_BYTES`]. Under the built-in
/// `lowercase` normalizer the term is the value lowercased ([`lowercase`]);
/// another normalizer (a custom one from the index's analysis settings) is
/// not applied, so its terms are taken to be the values as written.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
struct KeywordIndexing {
    ignore_above: Option<usize>,
    terms: Option<Terms>,
    lowercase: bool,
}

impl KeywordIndexing {
    /// How the `keyword` mapping `def` indexes a value.
    fn of(def: &Map<String, Value>) -> Self {
        let indexed = def.get("index").and_then(Value::as_bool) != Some(false);
        let doc_values = def.get("doc_values").and_then(Value::as_bool) != Some(false);
        Self {
            ignore_above: def.get("ignore_above").and_then(Value::as_u64).map(|n| n as usize),
            terms: match (indexed, doc_values) {
                (true, _) => Some(Terms::Postings),
                (false, true) => Some(Terms::DocValues),
                (false, false) => None,
            },
            lowercase: def.get("normalizer").and_then(Value::as_str) == Some(LOWERCASE_NORMALIZER),
        }
    }

    /// Whether `text` is past `ignore_above`, so not indexed.
    fn skips(&self, text: &str) -> bool {
        self.ignore_above.is_some_and(|n| more_utf16_units_than(text, n))
    }

    /// Lucene's refusal when the term `text` gives `field` is longer than
    /// one term may be; `None` when it fits, or the field builds no terms.
    /// Lowercasing a character at most adds half to its UTF-8 encoding (two
    /// bytes to three), so a value of at most half a term's length fits
    /// whatever its case, and is not lowercased to tell.
    fn refusal(&self, field: &str, text: &str) -> Option<String> {
        let terms = self.terms?;
        let fits = if self.lowercase { MAX_TERM_BYTES / 2 } else { MAX_TERM_BYTES };
        if text.len() <= fits {
            return None;
        }
        let term = if self.lowercase {
            Cow::Owned(lowercase(text))
        } else {
            Cow::Borrowed(text)
        };
        (term.len() > MAX_TERM_BYTES).then(|| too_long_for_one_term(field, &term, terms))
    }
}

/// Whether `text` is more than `n` UTF-16 units long, as OpenSearch counts
/// a keyword value against `ignore_above`, without counting every unit of a
/// long value. A string has no more units than UTF-8 bytes, and at least a
/// third as many ([`MAX_UTF8_BYTES_PER_UTF16_UNIT`]), so its byte length
/// settles most values; otherwise at most `n + 1` units are read.
fn more_utf16_units_than(text: &str, n: usize) -> bool {
    if text.len() <= n {
        return false;
    }
    if text.len() > n.saturating_mul(MAX_UTF8_BYTES_PER_UTF16_UNIT) {
        return true;
    }
    text.encode_utf16().nth(n).is_some()
}

/// `text` as Lucene's `lowercase` filter (behind the built-in `lowercase`
/// normalizer) lowercases it: each character by Java's
/// `Character.toLowerCase`, the one-to-one Unicode mapping. Rust's
/// `char::to_lowercase` is the full mapping, which differs from it only
/// for 'İ' (U+0130), whose one-to-one lowercase is a plain 'i'.
fn lowercase(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    for c in text.chars() {
        if c == DOTTED_CAPITAL_I {
            out.push('i');
        } else {
            out.extend(c.to_lowercase());
        }
    }
    out
}

/// A `keyword` multi-field that builds terms: its full path, which a
/// refusal names, and how it indexes a value.
#[derive(Clone, Debug, PartialEq)]
struct TermView {
    field: String,
    indexing: KeywordIndexing,
}

/// The `keyword` multi-fields under `def` (its `fields`) that build terms,
/// each named `<path>.<name>`.
fn keyword_multi_fields(def: &Map<String, Value>, path: &str) -> Vec<TermView> {
    let Some(fields) = def.get("fields").and_then(Value::as_object) else {
        return Vec::new();
    };
    fields
        .iter()
        .filter_map(|(name, sub)| {
            let sub = sub.as_object()?;
            if sub.get("type").and_then(Value::as_str) != Some("keyword") {
                return None;
            }
            let indexing = KeywordIndexing::of(sub);
            indexing.terms.is_some().then(|| TermView {
                field: format!("{path}.{name}"),
                indexing,
            })
        })
        .collect()
}

/// How a mapped field's values are kept beyond its column's type, as
/// OpenSearch keeps them.
#[derive(Clone, Debug, PartialEq)]
enum Kept {
    /// A float at less than a double's precision.
    Precision(Precision),
    /// A date read by the formats its mapping declares, in order.
    Date(Vec<DateFormat>),
    /// A `keyword` value, left out of its column past `ignore_above` and
    /// refused when its term is too long ([`KeywordIndexing`]); as is one
    /// its keyword multi-fields (`multi`) index.
    Keyword {
        indexing: KeywordIndexing,
        multi: Vec<TermView>,
    },
    /// A field of another type (a `text` field, say) whose keyword
    /// multi-fields index its values: a value one of them indexes is refused
    /// past [`MAX_TERM_BYTES`]. The field's own column keeps every value.
    MultiFields(Vec<TermView>),
    /// An `ip` or narrow integer value, which its column takes beyond what
    /// OpenSearch accepts.
    Valid(Valid),
}

/// What OpenSearch accepts of a field whose column is wider: an address of an
/// `ip` field (a string column), an integer in a `byte`, `short` or `integer`
/// field's range (a 64-bit column).
#[derive(Clone, Copy, Debug, PartialEq)]
enum Valid {
    Ip,
    Integer(i64, i64),
}

impl Valid {
    fn of(ty: &str) -> Option<Self> {
        match ty {
            "ip" => Some(Self::Ip),
            "byte" => Some(Self::Integer(i8::MIN.into(), i8::MAX.into())),
            "short" => Some(Self::Integer(i16::MIN.into(), i16::MAX.into())),
            "integer" => Some(Self::Integer(i32::MIN.into(), i32::MAX.into())),
            _ => None,
        }
    }

    /// Whether OpenSearch accepts a value its column took.
    fn accepts(self, v: &Value) -> bool {
        match self {
            Self::Ip => v.as_str().is_some_and(|s| s.parse::<std::net::IpAddr>().is_ok()),
            Self::Integer(min, max) => v.as_i64().is_some_and(|n| (min..=max).contains(&n)),
        }
    }
}

impl Kept {
    fn precision(&self) -> Option<Precision> {
        match self {
            Self::Precision(p) => Some(*p),
            _ => None,
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
                // An object with `enabled: false` indexes nothing under it, so
                // nothing under it is a column ([`Indexed`]).
                if mapping_flag(def.get("enabled")) == Some(false) {
                    continue;
                }
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
        let multi = keyword_multi_fields(def, &path);
        out.push(MappedField {
            path,
            data_type,
            fts,
            exact: ty == Some("keyword") && indexed,
            vector,
            range: ranged && indexed,
            precision: ty.and_then(|ty| Precision::of(ty, def)),
            date_formats,
            keyword: (ty == Some("keyword")).then(|| KeywordIndexing::of(def)),
            multi,
            valid: ty.and_then(Valid::of),
        });
    }
}

fn mapped_fields(mapping: &Value) -> Vec<MappedField> {
    let mut out = Vec::new();
    if mapping_flag(root_of(mapping).and_then(|root| root.get("enabled"))) == Some(false) {
        return out;
    }
    if let Some(props) = properties_of(mapping) {
        walk_properties(props, "", &mut out);
    }
    out
}

/// The root object of a mapping, past the `mappings` and `_doc` wrappers it
/// may come in: the object that holds its `properties`, and its own
/// `dynamic` and `enabled`.
fn root_of(mapping: &Value) -> Option<&Map<String, Value>> {
    let m = mapping.as_object()?;
    if m.contains_key("properties") {
        return Some(m);
    }
    if let Some(doc) = m.get("_doc") {
        return root_of(doc);
    }
    if let Some(mappings) = m.get("mappings") {
        return root_of(mappings);
    }
    Some(m)
}

/// A mapping's boolean setting, `enabled` or `dynamic`, as OpenSearch reads
/// it: a boolean, or its string. `dynamic: strict` (and
/// `strict_allow_templates`) maps no new field either, but a document that
/// brings one is refused before it is written, so for what reaches the
/// table it reads as `true`. `false_allow_templates` reads as `false`: a new
/// field a dynamic template matches is mapped by the document's mapping
/// update, and any other is left out of the index. None when the setting is
/// absent or no such value.
fn mapping_flag(value: Option<&Value>) -> Option<bool> {
    match value? {
        Value::Bool(b) => Some(*b),
        Value::String(s) => match s.as_str() {
            "true" | "strict" | "strict_allow_templates" => Some(true),
            "false" | "false_allow_templates" => Some(false),
            _ => None,
        },
        _ => None,
    }
}

/// What a mapping indexes of a document, object by object. A path it does
/// not index is kept in the source only, as OpenSearch keeps it: every path
/// under an object with `enabled: false`, every path of a mapping with
/// `enabled: false` at its root, and a path no property maps under an object
/// with `dynamic: false` (set on it, or on the nearest object above it that
/// sets one). OpenSearch takes any value there, of a different shape in each
/// document, so such a path gets no column: the first document's value would
/// type one, and a later document with another shape there would be refused
/// by the table after OpenSearch had acknowledged it.
///
/// A path under a mapped field of any other type (a `geo_point` given as an
/// object, a `flat_object`'s members) is that field's, and kept. So is a path
/// no property maps where new fields are mapped (`dynamic` true, the
/// default): the document's mapping update maps it before it is written.
/// A table no mapping has been declared for indexes every path.
#[derive(Debug, Default, PartialEq)]
struct Indexed {
    /// `enabled: false`: nothing at or under this object is indexed.
    disabled: bool,
    /// This object's own `dynamic`, if it sets one ([`mapping_flag`]).
    dynamic: Option<bool>,
    /// The object's properties by name: an object's own rule, or None for a
    /// field of any other type, which keeps every path at or under it.
    properties: HashMap<String, Option<Indexed>>,
    /// Whether this object, or one under it, leaves anything out: when none
    /// does, every path is indexed and no row is looked at.
    leaves_out: bool,
}

impl Indexed {
    /// What `mapping` indexes, from its root object ([`root_of`]).
    fn of(mapping: &Value) -> Self {
        root_of(mapping).map(Self::object).unwrap_or_default()
    }

    /// The rule of one object of the mapping, or of its root.
    fn object(def: &Map<String, Value>) -> Self {
        let disabled = mapping_flag(def.get("enabled")) == Some(false);
        let dynamic = mapping_flag(def.get("dynamic"));
        let mut properties = HashMap::new();
        if let Some(props) = def.get("properties").and_then(Value::as_object) {
            for (name, field) in props {
                let Some(field) = field.as_object() else {
                    continue;
                };
                let object = matches!(
                    field.get("type").and_then(Value::as_str),
                    None | Some("object") | Some("nested")
                );
                properties.insert(name.clone(), object.then(|| Self::object(field)));
            }
        }
        let leaves_out = disabled
            || dynamic == Some(false)
            || properties.values().flatten().any(|child| child.leaves_out);
        Indexed {
            disabled,
            dynamic,
            properties,
            leaves_out,
        }
    }

    /// Whether the flattened `path` ([`flatten`]) is indexed. A property
    /// whose name holds a dot matches as one name, as a mapping may spell it.
    fn indexes(&self, path: &str) -> bool {
        let mut node = self;
        // A new field is mapped unless an object above it says otherwise.
        let mut dynamic = true;
        let mut rest = path;
        loop {
            if node.disabled {
                return false;
            }
            if let Some(d) = node.dynamic {
                dynamic = d;
            }
            let found = rest
                .match_indices('.')
                .map(|(at, _)| at)
                .chain(iter::once(rest.len()))
                .find_map(|end| node.properties.get(&rest[..end]).map(|p| (end, p)));
            match found {
                None => return dynamic,
                Some((_, None)) => return true,
                // A value given where the mapping has an object: the object's own.
                Some((end, Some(child))) if end == rest.len() => return !child.disabled,
                Some((end, Some(child))) => {
                    node = child;
                    rest = &rest[end + 1..];
                }
            }
        }
    }

    /// Leave out of `flat`, a document's flattened row, every path this
    /// mapping does not index.
    fn keep_indexed(&self, flat: &mut Map<String, Value>) {
        if self.leaves_out {
            flat.retain(|path, _| self.indexes(path));
        }
    }
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
fn ensure_table(
    bridge: &Bridge,
    name: &str,
    mapping: &Value,
    attached: bool,
) -> Result<Value, String> {
    declare_table(bridge, name, mapping, attached, false)
}

/// [`ensure_table`], or with `check_only` the same judgement of an attached
/// table and nothing else: whether it could be attached to, with no rule
/// kept for it on the connection. An index is created from that answer
/// before any shard of it exists to declare the table.
fn declare_table(
    bridge: &Bridge,
    name: &str,
    mapping: &Value,
    attached: bool,
    check_only: bool,
) -> Result<Value, String> {
    let mut mapped = mapped_fields(mapping);
    let mut engine = Vec::new();
    if let Some(props) = mapping.get(ENGINE_COLUMNS).and_then(Value::as_object) {
        walk_properties(props, "", &mut engine);
    }
    let engine_columns: Vec<String> = engine.iter().map(|f| f.path.clone()).collect();
    for f in &mut mapped {
        if RESERVED.contains(&f.path.as_str()) || engine_columns.contains(&f.path) {
            f.path = format!("{OWN_COLUMN_PREFIX}{}", f.path);
        }
    }
    mapped.extend(engine);
    let conn = &bridge.conn;
    if !check_only {
        bridge.keep_rules(name, &mapped);
        bridge.engine_rule(name, engine_columns);
        bridge.stamp_rule(name, mapping);
        bridge.index_rule(name, mapping);
        if !attached {
            bridge.attach_rule(name, None);
        }
        // The cached handle, if any, predates whatever this changes.
        bridge.forget(name);
    }
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
                    Some(index @ ColumnIndex::Fts { .. }) => {
                        Some(with_positions(index.clone(), true))
                    }
                    _ => None,
                });
            // The index a keyword field added now gets: the `keyword`
            // analyzer, each value one term exactly as written, so `=` and
            // `IN` look a value up whole and the column's term counts are its
            // value counts. It takes no stopwords or stemmer, and no positions.
            // Where the connection cannot declare that analyzer
            // ([`Bridge::keyword_analyzer`]), the text index without positions.
            let exact_index = text_index.as_ref().map(|index| {
                if !bridge.keyword_analyzer() {
                    return with_positions(index.clone(), false);
                }
                let mut exact = ColumnIndex::fts(Analyzer::Keyword);
                if let (
                    ColumnIndex::Fts { stored, bm25, .. },
                    ColumnIndex::Fts {
                        stored: from_stored,
                        bm25: from_bm25,
                        ..
                    },
                ) = (&mut exact, index)
                {
                    *stored = *from_stored;
                    *bm25 = *from_bm25;
                }
                exact
            });
            let mut conflicts: Vec<String> = Vec::new();
            for f in &mapped {
                let present = schema.field_with_name(&f.path).is_ok();
                let indexed = table_schema.fields().iter().any(|c| {
                    c.name == f.path
                        && (is_text_index(&c.name, &c.index)
                                // An attached table's indexes are as its maker built
                                // them: under a field mapped as text, a full-text
                                // index without positions still ranks and matches
                                // terms, and only a phrase needs more.
                                || (attached && matches!(c.index, Some(ColumnIndex::Fts { .. }))))
                });
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
            if attached {
                // The table is another's: a field it has no column for is
                // refused, and nothing is added to it, the sequence column
                // included.
                // A table that holds a document's id or source as a column
                // of its own is one the plugin wrote, or one whose columns
                // those of its view would be taken for: neither is read
                // through the view.
                if let Some(own) = [OSID, SOURCE, SEQ]
                    .into_iter()
                    .find(|c| schema.field_with_name(c).is_ok())
                {
                    return Err(format!(
                        "table {name} holds a column [{own}], which an attached index gives from the table's other columns; a table the plugin wrote is named without attaching to it"
                    ));
                }
                // A ranked search gives each row's score under this name,
                // beside the table's columns: a column of the table's own
                // by that name would be taken for it, and left out of the
                // document.
                if schema.field_with_name(attached::SCORE).is_ok() {
                    return Err(format!(
                        "table {name} holds a column [{}], the name a search gives a row's score under; an attached index cannot tell the two apart",
                        attached::SCORE
                    ));
                }
                let missing: Vec<&str> = mapped
                    .iter()
                    .filter(|f| schema.field_with_name(&f.path).is_err())
                    .map(|f| f.path.as_str())
                    .collect();
                if !missing.is_empty() {
                    return Err(format!(
                        "the mapping of {name} names fields its table has no column for: [{}]; an attached index maps only the columns its table holds",
                        missing.join(", ")
                    ));
                }
                if !check_only {
                    bridge.attach_rule(
                        name,
                        Some(Attached {
                            columns: Arc::new(attached::columns_of(&schema)),
                            text: Arc::new(
                                mapped.iter().filter(|f| f.fts).map(|f| f.path.clone()).collect(),
                            ),
                        }),
                    );
                }
                return Ok(json!({ "created": false, "added": 0 }));
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
                        match (&text_index, &exact_index, f.fts, f.exact) {
                            (Some(index), _, true, _) => patch.with_index(index.clone()),
                            (_, Some(index), false, true) => patch.with_index(index.clone()),
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
                let patch = SchemaPatch::new(patches);
                let gate = bridge.gate(name);
                let _pass = gate.whole();
                let applied = waiting(|| table.apply_schema(&patch, None));
                // A batch prepared while this ran may have cached the handle (and
                // schema) from before the change; drop it either way. A read
                // still in flight then keeps nothing ([`Bridge::kept_or_read`]).
                bridge.forget(name);
                applied.map_err(|e| e.to_string())?;
            }
            Ok(json!({ "created": false, "added": added }))
        }
        Err(InfinoError::NotFound(_)) if attached => Err(format!(
            "table {name} does not exist; an attached index reads a table that already exists"
        )),
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
                } else if f.exact {
                    // One term per value where the connection can declare
                    // it (see `exact_index` above).
                    let field = FtsField::new(f.path.clone());
                    spec = spec.fts(if bridge.keyword_analyzer() {
                        field.analyzer("keyword")
                    } else {
                        field
                    });
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
        match ensure_table(conn, name, &mapping, false) {
            Ok(v) => out_json(v),
            Err(e) => err_json(&e),
        }
    }))
    .unwrap_or(ptr::null_mut())
}

/// Declare `name` as a table an index is attached to: one that existed
/// before the index and is read as it is. The mapping is checked against the
/// table's columns as [`infino_ensure_table`] checks it, but nothing is
/// created and nothing is added: a missing table, or a mapped field the
/// table has no column for, is an error. Returns
/// `{"created":false,"added":0}` or `{"error":..}`.
#[no_mangle]
pub unsafe extern "C" fn infino_attach_table(
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
        match ensure_table(conn, name, &mapping, true) {
            Ok(v) => out_json(v),
            Err(e) => err_json(&e),
        }
    }))
    .unwrap_or(ptr::null_mut())
}

/// Whether an index could be attached to table `name` under this mapping,
/// asked when the index is created, before any shard declares the table:
/// `{"refused":null}` when it could, `{"refused":"<why>"}` when the table
/// is missing, is one the plugin wrote, or disagrees with the mapping (what
/// [`infino_attach_table`] refuses), and `{"error":..}` when the table could
/// not be read at all, which says nothing about it. Nothing is kept on the
/// connection and nothing is changed.
#[no_mangle]
pub unsafe extern "C" fn infino_attach_check(
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
        // A table that cannot be opened for a reason other than its absence
        // is not judged: the failure is the store's, not the request's.
        match conn.conn.open_table(name) {
            Ok(_) | Err(InfinoError::NotFound(_)) => {}
            Err(e) => return err_json(&e.to_string()),
        }
        match declare_table(conn, name, &mapping, true, true) {
            Ok(_) => out_json(json!({ "refused": null })),
            Err(why) => out_json(json!({ "refused": why })),
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

/// Whether a column's index serves it as text: a full-text index with
/// positions, as a mapped `text` field gets, or the id column's. A `keyword`
/// field's index has no positions: the column is still a keyword, answered
/// exactly, whose dictionary only spares `=` and `IN` a scan.
fn is_text_index(column: &str, index: &Option<ColumnIndex>) -> bool {
    match index {
        Some(ColumnIndex::Fts { positions, .. }) => *positions || column == OSID,
        _ => false,
    }
}

/// Columns of a table as JSON `[{"name","type","elem","fts","vector","range"}]`,
/// type in {i64,f64,bool,str,ts,vec,list,other}, `elem` a list column's
/// element type (null for any other), `range` whether the column has a range
/// index (a numeric or date field mapped `"index": false` has none). `""` if
/// the table is absent.
#[no_mangle]
pub unsafe extern "C" fn infino_columns(h: *mut Bridge, name: *const c_char) -> *mut c_char {
    catch_unwind(AssertUnwindSafe(|| {
        let (Some(conn), Some(name)) = (bridge_ref(h), unsafe { cstr(name) }) else {
            return ptr::null_mut();
        };
        // An attached table's full-text index serves its column as text
        // whether or not it holds positions, where the index's mapping
        // reads the field as text (see `ensure_table`); under a `keyword`
        // field, or none, a column whose index has no positions is a
        // keyword, as it is in a table of the plugin's own.
        let attached = conn.is_attached(name);
        // The plugin asks for the columns again after each append, on the
        // thread that stages a bulk: on a hosted table, the kept document
        // answers ([`Bridge::table_schema`]) rather than the service.
        match conn.table_schema(name) {
            Ok(schema) => {
                let mut cols: Vec<Value> = schema
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
                            "fts": is_text_index(&f.name, &f.index)
                                || (attached
                                    && matches!(f.index, Some(ColumnIndex::Fts { .. }))
                                    && conn.attached_text(name, &f.name)),
                            "vector": matches!(f.index, Some(ColumnIndex::Vector { .. })),
                            "range": matches!(f.index, Some(ColumnIndex::Range)),
                        })
                    })
                    .collect();
                if attached {
                    // The columns its view adds, as a table of the plugin's
                    // own lists them, so a sort or a filter on a document's
                    // id or sequence number finds a column to read. The id
                    // carries no index: it is compared, never searched.
                    for (name, ty) in [(OSID, "str"), (SOURCE, "str"), (SEQ, "i64")] {
                        cols.push(json!({
                            "name": name,
                            "type": ty,
                            "elem": null,
                            "fts": false,
                            "vector": false,
                            "range": false,
                        }));
                    }
                }
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
            // reads it. Any other fails the document here rather than in
            // the engine, which would read it otherwise or not at all: a
            // date in a custom pattern is read by OpenSearch's mapper
            // ([`mapped_values`]). An epoch-millisecond integer is taken
            // as is.
            Value::String(s) => default_format_millis(s).map(|ms| json!(ms)).ok_or_else(|| {
                format!("failed to parse date field [{s}] with format [{DEFAULT_DATE_FORMAT}]")
            }),
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

/// Whether `rows` hold a path `schema` has no column for, with a value the
/// engine adds a column for when they land. A path whose values are all
/// `null`, `[]` or lists of nulls carries no type, and the engine adds
/// nothing for it ([`types_a_column`]); counting it would re-read the
/// schema after every write of such a document, since no write or
/// declaration ever gives the path a column.
fn adds_columns(schema: &TableSchema, rows: &[Map<String, Value>]) -> bool {
    rows.iter().any(|row| {
        row.iter()
            .any(|(path, value)| types_a_column(value) && schema.id_of(path).is_none())
    })
}

/// Whether `value`, at a path no column declares, gives the engine a type
/// to add a column with: anything but a `null`, or a list holding nothing
/// but nulls.
fn types_a_column(value: &Value) -> bool {
    match value {
        Value::Null => false,
        Value::Array(items) => items.iter().any(|item| !item.is_null()),
        _ => true,
    }
}

/// Check one document against the declared columns without indexing it:
/// `""` when it conforms, else the reason it would fail. The engine returns
/// the same verdict `infino_index_batch` would report for the document, so a
/// caller can refuse it synchronously (an OpenSearch bulk item error) rather
/// than learn after the batch commits. A failure to check at all (the table
/// cannot be opened) is an error object, not a verdict on the document.
/// `ignored` (null for none) names the fields OpenSearch's mapper left out
/// of the document, one path per line ([`IGNORED_SEPARATOR`]): they are
/// not checked, as the row leaves them out.
#[no_mangle]
pub unsafe extern "C" fn infino_check_doc(
    h: *mut Bridge,
    name: *const c_char,
    source: *const c_char,
    ignored: *const c_char,
) -> *mut c_char {
    catch_unwind(AssertUnwindSafe(|| {
        let (Some(conn), Some(name), Some(source)) =
            (bridge_ref(h), unsafe { cstr(name) }, unsafe {
                cstr(source)
            })
        else {
            return err_json("null argument");
        };
        let ignored = if ignored.is_null() {
            Vec::new()
        } else {
            match unsafe { cstr(ignored) } {
                Some(paths) => ignored_paths(paths),
                None => return err_json("ignored paths are not UTF-8"),
            }
        };
        let table = match conn.table(name) {
            Ok(t) => t,
            Err(e) => return err_json(&e.to_string()),
        };
        let schema = table.schema();
        let stamp = conn.stamp(name);
        out_string(
            match conform_doc(
                source,
                &schema,
                &conn.kept(name),
                &conn.engine_columns(name),
                stamp.as_deref(),
                &ignored,
                &conn.indexed(name),
            ) {
                Ok(_) => String::new(),
                Err(why) => why,
            },
        )
    }))
    .unwrap_or(ptr::null_mut())
}

/// What separates a document's source from the values OpenSearch's mappers
/// read from it, when the caller sends them ([`mapped_values`]): JSON text
/// never holds it unescaped.
const MAPPED_VALUES: char = '\u{1e}';

/// A document's source and, when the caller sent them after
/// [`MAPPED_VALUES`], the values OpenSearch's mappers read from its date and
/// integer fields: an object of each field's values (epoch milliseconds, or
/// whole numbers), which the field's column holds in place of what the
/// source spells there. A date in a format of the field's mapping this
/// bridge does not read, and a fractional number an integer field coerces,
/// are held as OpenSearch reads them. Text after the separator that is no
/// such object leaves the whole text the source.
fn mapped_values(text: &str) -> (&str, Option<Map<String, Value>>) {
    let Some(at) = text.rfind(MAPPED_VALUES) else {
        return (text, None);
    };
    match serde_json::from_str::<Value>(&text[at + MAPPED_VALUES.len_utf8()..]) {
        Ok(Value::Object(values)) => (&text[..at], Some(values)),
        _ => (text, None),
    }
}

/// How many values a source gives at one field: its non-null values, an
/// array's at any depth, as OpenSearch's mapper reads an array of arrays.
fn given_values(value: &Value) -> usize {
    match value {
        Value::Null => 0,
        Value::Array(items) => items.iter().map(given_values).sum(),
        _ => 1,
    }
}

/// The paths of a document's ignored fields, as the caller hands them over
/// ([`IGNORED_SEPARATOR`] between them), each once, in order.
fn ignored_paths(paths: &str) -> Vec<&str> {
    let mut out: Vec<&str> = Vec::new();
    for path in paths.split(IGNORED_SEPARATOR) {
        if !path.is_empty() && !out.contains(&path) {
            out.push(path);
        }
    }
    out
}

/// The values of the `keyword` fields `kept` names as OpenSearch indexes
/// them: a value longer than its field's `ignore_above` is left out (the
/// source keeps it), and one whose UTF-8 encoding is longer than a term may
/// be refuses the document, as Lucene refuses it ([`immense_term`]). A
/// field's keyword multi-fields ([`TermView`]) index its values too, each up
/// to its own `ignore_above`, so a value one of them takes past a term's
/// length refuses the document as well; the field's own column keeps it.
fn keyword_values(flat: &mut Map<String, Value>, kept: &HashMap<String, Kept>) -> Result<(), String> {
    for (path, value) in flat.iter_mut() {
        let (own, multi) = match kept.get(path) {
            Some(Kept::Keyword { indexing, multi }) => (Some(indexing), multi.as_slice()),
            Some(Kept::MultiFields(multi)) => (None, multi.as_slice()),
            _ => continue,
        };
        // Whether the field's own column keeps `v`; an error when the field
        // or one of its multi-fields would index it as too long a term. The
        // field is indexed first, then its multi-fields, as the mapper
        // indexes them, so the refusal names the first that refuses.
        let indexed = |v: &Value| -> Result<bool, String> {
            let text = match v {
                Value::String(s) => s.as_str(),
                // A number or boolean is indexed as its text, far shorter
                // than one term, so only the field's own `ignore_above` can
                // leave it out, and it is spelled out only to check that.
                Value::Number(_) | Value::Bool(_) => {
                    return Ok(!own.is_some_and(|own| own.ignore_above.is_some() && own.skips(&v.to_string())));
                }
                _ => return Ok(true),
            };
            let mut keeps = true;
            if let Some(own) = own {
                if own.skips(text) {
                    keeps = false;
                } else if let Some(why) = own.refusal(path, text) {
                    return Err(why);
                }
            }
            for view in multi {
                if !view.indexing.skips(text) {
                    if let Some(why) = view.indexing.refusal(&view.field, text) {
                        return Err(why);
                    }
                }
            }
            Ok(keeps)
        };
        match value {
            // Only a keyword field's own values are left out; a field of
            // another type keeps its values as they are.
            Value::Array(items) if own.is_none() => each_value(items, &mut |v| indexed(v).map(|_| ()))?,
            Value::Array(items) => {
                let mut values = Vec::with_capacity(items.len());
                array_values(std::mem::take(items), &mut values);
                for v in values {
                    if indexed(&v)? {
                        items.push(v);
                    }
                }
            }
            other => {
                if !indexed(other)? {
                    *other = Value::Null;
                }
            }
        }
    }
    Ok(())
}

/// Call `f` on each value of a multi-valued field, in order, reading
/// through an array inside the array as [`array_values`] does, without
/// taking the values out.
fn each_value(items: &[Value], f: &mut impl FnMut(&Value) -> Result<(), String>) -> Result<(), String> {
    for item in items {
        match item {
            Value::Array(inner) => each_value(inner, f)?,
            Value::Null => {}
            other => f(other)?,
        }
    }
    Ok(())
}

/// The reason Lucene gives, word for word, for a document whose `field`
/// would index `term` past [`MAX_TERM_BYTES`], which OpenSearch answers as
/// an `illegal_argument_exception`: an immense term ([`immense_term`]) when
/// the field is indexed, a doc value too large when it only has doc values.
fn too_long_for_one_term(field: &str, term: &str, terms: Terms) -> String {
    match terms {
        Terms::Postings => immense_term(field, term),
        Terms::DocValues => format!("{DOC_VALUES_TOO_LARGE}{field}\" is too large, must be <= {MAX_TERM_BYTES}"),
    }
}

/// Lucene's refusal of an indexed `term` past [`MAX_TERM_BYTES`] in
/// `field`. It shows the term's first bytes as Java prints a byte array
/// (signed). Lucene copies that many bytes whatever the term's length, and
/// an immense term is always longer.
fn immense_term(field: &str, term: &str) -> String {
    let prefix: Vec<String> = term
        .as_bytes()
        .iter()
        .take(IMMENSE_TERM_PREFIX_BYTES)
        .map(|b| (*b as i8).to_string())
        .collect();
    format!(
        "{IMMENSE_TERM}{field}\" (whose UTF8 encoding is longer than the max length {MAX_TERM_BYTES}), all of which were skipped.  Please correct the analyzer to not produce such terms.  The prefix of the first immense term is: '[{}]...', original message: bytes can be at most {MAX_TERM_BYTES} in length; got {}",
        prefix.join(", "),
        term.len()
    )
}

/// Whether the flattened `path` is the field `field` or a path under it (a
/// geo_point given as an object flattens to the paths under its field).
fn is_at_or_under(path: &str, field: &str) -> bool {
    path.strip_prefix(field)
        .is_some_and(|rest| rest.is_empty() || rest.starts_with('.'))
}

/// The column of `schema` that the flattened `path`, one `schema` has no
/// column for, would nest under: `body` for `body.a` once a document has
/// given `body` a value of its own. The engine refuses such a path in every
/// commit (it would become a second column beside the one there), so a
/// document carrying it is refused when checked, not lost after OpenSearch
/// acknowledged it.
fn live_column_above<'a>(path: &'a str, schema: &Schema) -> Option<&'a str> {
    path.match_indices('.')
        .map(|(at, _)| &path[..at])
        .find(|prefix| schema.field_with_name(prefix).is_ok())
}

/// Parse, flatten and conform one document to the declared columns. The
/// row it returns is what the engine is given. A declared field the document
/// gives several values goes to its list column ([`LIST_SUFFIX`]), each
/// value conformed as the field's own would be; one value in an array is
/// that value, as OpenSearch reads it. A field `kept` names holds each value
/// as OpenSearch keeps it ([`Kept`]): a float at its precision, a date read
/// by its own format. With `stamp`, the document must hold exactly one
/// non-null value at that field, as a data stream's documents must. An array
/// of arrays at a field without a scalar or string column the mapping
/// declares is refused, as the engine would refuse it. A field named like
/// one of the table's own columns ([`RESERVED`], or `engine`, those its
/// engine writes) goes to a column of its own. A field `ignored` names, and
/// every path under it, is left out of the row, as OpenSearch's mapper left
/// it out of the document (its value stays in the source). A field the
/// mapper's values cover ([`mapped_values`]) holds them, conformed to its
/// column as they are, where the document gives it a value.
fn conform_doc(
    source: &str,
    schema: &Schema,
    kept: &HashMap<String, Kept>,
    engine: &[String],
    stamp: Option<&str>,
    ignored: &[&str],
    indexed: &Indexed,
) -> Result<Map<String, Value>, String> {
    let (source, mapped) = mapped_values(source);
    let doc: Value = serde_json::from_str(source).map_err(|e| format!("malformed JSON: {e}"))?;
    let obj = doc
        .as_object()
        .ok_or_else(|| "the document is not an object".to_string())?;
    let mut flat = Map::new();
    flatten(obj, "", &mut flat);
    indexed.keep_indexed(&mut flat);
    // A declared field the mapper ignored keeps, of several values, those its
    // column takes (OpenSearch drops only the malformed ones); any other path
    // at or under an ignored field has no value OpenSearch indexed.
    let mut partial: HashSet<String> = HashSet::new();
    if !ignored.is_empty() {
        flat.retain(|path, value| {
            if !ignored.iter().any(|field| is_at_or_under(path, field)) {
                return true;
            }
            let keeps_some = value.is_array()
                && ignored.contains(&path.as_str())
                && schema.field_with_name(path).is_ok();
            if keeps_some {
                partial.insert(path.clone());
            }
            keeps_some
        });
    }
    // The mapper's values stand in for the source's at each field the
    // source gives as many values as the mapper read there, so a field the
    // document leaves out or null stays so whatever the mapper read (a
    // `null_value`), and values the mapper read from elsewhere too (a
    // `copy_to`) leave the source's as they are. At an ignored field that
    // keeps some of its values, the mapper read only those it indexed, so
    // fewer than the source gives stand in for them.
    let mut by_mapper: HashSet<String> = HashSet::new();
    for (path, values) in mapped.into_iter().flatten() {
        let Some(value) = flat.get_mut(&path) else {
            continue;
        };
        let given = given_values(value);
        let read = match &values {
            Value::Array(items) => items.len(),
            _ => 1,
        };
        let fewer = partial.contains(&path) && read < given;
        if given == 0 || (given != read && !fewer) {
            continue;
        }
        *value = match values {
            Value::Array(mut items) if items.len() == 1 => items.pop().unwrap_or(Value::Null),
            other => other,
        };
        by_mapper.insert(path);
    }
    // A field named like a reserved column goes to its own column once the
    // mapping declares it, and never to the reserved one.
    for reserved in RESERVED
        .into_iter()
        .chain(engine.iter().map(String::as_str))
    {
        if let Some(value) = flat.remove(reserved) {
            let own = format!("{OWN_COLUMN_PREFIX}{reserved}");
            if schema.field_with_name(&own).is_ok() {
                if by_mapper.remove(reserved) {
                    by_mapper.insert(own.clone());
                }
                flat.insert(own, value);
            }
        }
    }
    keyword_values(&mut flat, kept)?;
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
        let field = schema.field_with_name(path).ok();
        // A field the mapping declares with a scalar or string column reads
        // through an array of arrays as OpenSearch does. Any other path (a
        // geo_point, a flat_object's member, a field not yet declared) is
        // typed by the engine from its values, with no column yet or with the
        // list column its first values gave it, and the engine takes no array
        // holding an array there: every commit would refuse the document.
        let reads_through = field.is_some_and(|f| {
            list_type(f.data_type()).is_some()
                || matches!(
                    f.data_type(),
                    DataType::FixedSizeList(..) | DataType::Decimal128(..)
                )
        });
        if !reads_through
            && matches!(value, Value::Array(items) if items.iter().any(Value::is_array))
        {
            return Err(format!(
                "field [{path}]: an array of arrays is not supported for a field without a column the mapping declares"
            ));
        }
        let Some(field) = field else {
            // A new path under a column the table already holds is refused by
            // every commit ([`live_column_above`]): say so now, before the
            // write is acknowledged. A null there gives the engine nothing
            // to write, and it skips the path.
            if let Some(column) = (!value.is_null()).then(|| live_column_above(path, schema)).flatten() {
                return Err(format!(
                    "field [{path}]: the table holds [{column}] as a column of its own, and no field can be added beneath it"
                ));
            }
            continue;
        };
        let dt = field.data_type();
        let rule = kept.get(path);
        let read = by_mapper.contains(path.as_str());
        let fit = |v: Value| match rule {
            // The mapper read the value already, as an instant or a whole number.
            _ if read => conform(v, dt),
            Some(Kept::Date(formats)) if matches!(dt, DataType::Timestamp(..)) => {
                conform_date(v, formats)
            }
            _ => conform(v, dt).and_then(|v| keep(v, rule.and_then(Kept::precision))),
        };
        // Of an ignored field's values, those OpenSearch indexed: the mapper
        // dropped any it could not parse, which the column may still take.
        let indexed = |v: Value| {
            fit(v).ok().filter(|v| match rule {
                Some(Kept::Valid(valid)) => valid.accepts(v),
                _ => true,
            })
        };
        let why = |why: String| format!("field [{path}]: {why}");
        let partial = partial.contains(path.as_str());
        *value = match value.take() {
            Value::Array(items) if list_type(dt).is_some() => {
                let mut values = Vec::with_capacity(items.len());
                array_values(items, &mut values);
                let mut conformed = if partial {
                    values.into_iter().filter_map(indexed).collect()
                } else {
                    values
                        .into_iter()
                        .map(fit)
                        .collect::<Result<Vec<Value>, String>>()
                        .map_err(why)?
                };
                if conformed.len() > 1 {
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
                    conformed.pop().unwrap_or(Value::Null)
                }
            }
            other if partial => indexed(other).unwrap_or(Value::Null),
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
) -> Result<bool, InfinoError> {
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
    let table = bridge.table(name)?;
    let patch = SchemaPatch::new(patches);
    let applied = {
        let gate = bridge.gate(name);
        let _pass = gate.whole();
        waiting(|| table.apply_schema(&patch, None))
    };
    // The cached handle predates the change either way.
    bridge.forget(name);
    if let Err(e) = applied {
        let now = bridge.table(name)?.schema();
        if wanted.iter().any(|w| now.field_with_name(w).is_err()) {
            return Err(e);
        }
    }
    Ok(true)
}

/// Run `write` until the table's writer is free for it, for up to ten
/// seconds. Appends to one table run at once, sharing its writer; an append
/// whose batch adds a column takes the writer whole, and is refused at once
/// while another holds it (several first appends of a new field race for
/// it). A write is all or nothing, so a refused one is run again once the
/// other lands: by then the column exists and it shares the writer. The
/// writes this node takes the writer whole for (a replace, a delete, a
/// schema change) go through the table's [`Gate`] instead, so a write that
/// shares the writer never waits here for one of those.
fn waiting<T>(mut write: impl FnMut() -> Result<T, InfinoError>) -> Result<T, InfinoError> {
    let deadline = Instant::now() + Duration::from_secs(10);
    let mut pause = Duration::from_millis(5);
    loop {
        match write() {
            Err(InfinoError::Conflict(_)) if Instant::now() < deadline => {
                std::thread::sleep(pause);
                pause = (pause * 2).min(Duration::from_millis(200));
            }
            other => return other,
        }
    }
}

/// The order in which this node's writes to one table take its writer. The
/// engine shares the writer among appends and gives it whole to a replace,
/// a delete or a schema change, which waits for the appends in flight; but
/// a write that finds the writer held whole, or claimed, is refused at once
/// rather than waiting. Appends to one table run at once, so without the
/// gate two replaces (or deletes) of different ids would refuse each other,
/// and the appends behind a long replace would give up after
/// [`waiting`]'s deadline. Through the gate, appends pass together, and a
/// whole write passes alone: it stops new appends, waits for those passing
/// to finish, and they wait for it, as long as it runs.
///
/// A pass is held only around one engine call, never across another pass:
/// an append that waited on the gate while holding a pass would never get
/// one once a whole write claimed the gate.
#[derive(Default)]
struct Gate {
    state: Mutex<GateState>,
    changed: Condvar,
}

#[derive(Default)]
struct GateState {
    /// Appends passing.
    shared: usize,
    /// A whole write passing, or waiting for the appends passing to finish.
    whole: bool,
}

/// A write's way through a [`Gate`]; dropping it lets the next ones through.
struct Pass<'a> {
    gate: &'a Gate,
    whole: bool,
}

impl Gate {
    fn lock(&self) -> MutexGuard<'_, GateState> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn wait<'a>(&self, state: MutexGuard<'a, GateState>) -> MutexGuard<'a, GateState> {
        self.changed
            .wait(state)
            .unwrap_or_else(PoisonError::into_inner)
    }

    /// Pass beside the other appends, once no whole write passes or waits.
    fn append(&self) -> Pass<'_> {
        let mut state = self.lock();
        while state.whole {
            state = self.wait(state);
        }
        state.shared += 1;
        Pass {
            gate: self,
            whole: false,
        }
    }

    /// Pass alone: claim the gate, then wait for the appends passing.
    fn whole(&self) -> Pass<'_> {
        let mut state = self.lock();
        while state.whole {
            state = self.wait(state);
        }
        state.whole = true;
        while state.shared > 0 {
            state = self.wait(state);
        }
        Pass {
            gate: self,
            whole: true,
        }
    }
}

impl Drop for Pass<'_> {
    fn drop(&mut self) {
        let mut state = self.gate.lock();
        if self.whole {
            state.whole = false;
        } else {
            state.shared -= 1;
        }
        drop(state);
        self.gate.changed.notify_all();
    }
}

/// Whether the engine refused a write for what its rows carry, so the same
/// rows are refused on every try: a schema refusal (a value of another type
/// than its column's, an array of arrays, a path beneath a live column),
/// except a schema that changed under the write, a column still converting,
/// or a field cap that raising the table's limit lifts, which a later try
/// can pass. The local engine returns the refusal typed; the hosted service
/// answers it as a 400 whose body carries the same text
/// ([`hosted_schema_refusal`]).
fn refuses_content(e: &InfinoError) -> bool {
    match e {
        InfinoError::Schema(s) => !matches!(
            s,
            SchemaError::SchemaConflict { .. }
                | SchemaError::ConversionInProgress { .. }
                | SchemaError::FieldCapExceeded { .. }
        ),
        _ => hosted_schema_refusal(e).is_some_and(|reason| {
            !HOSTED_RETRYABLE_SCHEMA
                .iter()
                .any(|&shape| says(reason, shape))
        }),
    }
}

/// Whether the engine refused a write for taking the table over its cap of
/// fields, typed by the local engine or in the text of the hosted service's
/// 400.
fn over_field_cap(e: &InfinoError) -> bool {
    matches!(e, InfinoError::Schema(SchemaError::FieldCapExceeded { .. }))
        || hosted_schema_refusal(e).is_some_and(|reason| says(reason, HOSTED_FIELD_CAP))
}

/// The shape of an engine message, as the text it starts with and the text
/// it ends with: the parts between carry names and values from the
/// documents, so a message is matched by its two ends, never by what it
/// contains.
type Shape = (&'static str, &'static str);

/// The hosted service's schema errors that a later try can pass, in the
/// engine's wording: the schema changed under the write, a column is still
/// converting, or the write would take the table over its field cap.
const HOSTED_RETRYABLE_SCHEMA: [Shape; 3] = [
    ("the schema is at version ", "; re-read it and resubmit"),
    (
        "column `",
        "` is still converting; run optimize before changing its type again",
    ),
    HOSTED_FIELD_CAP,
];

/// A field-cap refusal, in the engine's wording.
const HOSTED_FIELD_CAP: Shape = (
    "adding [",
    "; raise `max_fields` in the schema or fix the producer",
);

/// Whether `reason` has the shape `(starts, ends)`.
fn says(reason: &str, (starts, ends): Shape) -> bool {
    let reason = reason.trim_end();
    reason.len() >= starts.len() + ends.len()
        && reason.starts_with(starts)
        && reason.ends_with(ends)
}

/// The reason of a schema refusal the hosted service answered: it passes an
/// engine error on as a 400 whose body is `<op> error: schema: <reason>`,
/// which reaches the bridge as an untyped backend error
/// (`<op>: server returned 400: <body>`). None for any other error.
fn hosted_schema_refusal(e: &InfinoError) -> Option<&str> {
    let InfinoError::Backend(message) = e else {
        return None;
    };
    if status_in(message) != Some(STATUS_BAD_REQUEST) {
        return None;
    }
    let (_, rest) = message.split_once(STATUS_MARKER)?;
    let body = rest.get(STATUS_DIGITS..)?.strip_prefix(": ")?;
    let (_, error) = body.split_once(" error: ")?;
    error.strip_prefix("schema: ")
}

/// Write rows with `write`, isolating the ones the engine refuses: a refused
/// write is split in halves until each refused document stands alone and is
/// reported with the engine's reason, and as `permanent` when the engine
/// refused what it carries ([`refuses_content`]), while every accepted half
/// is one write. A write the hosted service could not take
/// ([`ends_the_write`]: it did not answer, or turned the call away for the
/// account's request rate) says nothing of the rows, and every half would
/// meet the same service: it ends the call with that error, so a stalled or
/// throttled service costs one write rather than one per document. Every
/// document not written is reported under the status a client sends it
/// again for ([`untried`]); those an earlier call wrote are not.
fn write_checked(
    rows: &[(usize, Value)],
    failed: &mut Vec<Value>,
    write: &dyn Fn(&[Value]) -> Result<(), InfinoError>,
) -> Result<(), InfinoError> {
    if rows.is_empty() {
        return Ok(());
    }
    let values: Vec<Value> = rows.iter().map(|(_, v)| v.clone()).collect();
    match write(&values) {
        Ok(()) => {}
        Err(e) if ends_the_write(&e) => {
            untried(rows, &e, failed);
            return Err(e);
        }
        Err(e) if rows.len() == 1 => {
            failed.push(json!({
                "i": rows[0].0,
                "reason": short_reason(&e),
                "permanent": refuses_content(&e),
            }));
        }
        Err(_) => {
            let (a, b) = rows.split_at(rows.len() / 2);
            each_run(&[a, b], failed, |half, failed| {
                write_checked(half, failed, write)
            })?;
        }
    }
    Ok(())
}

/// Append rows, isolating the ones the engine refuses ([`write_checked`]).
fn append_checked(
    table: &Supertable,
    gate: &Gate,
    rows: &[(usize, Value)],
    failed: &mut Vec<Value>,
) -> Result<(), InfinoError> {
    write_checked(rows, failed, &|values| {
        let _pass = gate.append();
        waiting(|| table.append_rows(values))
    })
}

/// Replace the rows under the ids `rows` carry with those rows, mapped by
/// the engine, in one update, which the engine refuses unless its ids match
/// as many rows as it brings, one per id when no id holds a row twice and
/// none holds none. An update refused for any reason but its rows' content
/// ([`refuses_content`]) or the field cap has its ids' rows counted
/// ([`rows_under`]): the documents of ids holding no row (an earlier version
/// was refused) or several (a write landed twice) are written by
/// [`rewrite_rows`], and the rest replace again. The hosted service reports
/// a count it refuses as a plain status, so the counts, not the error, say
/// which ids miscount. An update refused for its rows' content or over the
/// field cap is split in halves until each refused document stands alone,
/// so one document leaves the others replaced; a document alone refused
/// for its content is reported permanent, since every try meets the same
/// table. Any other refusal (a count that fails among them) is reported for
/// every document its update carried, for a later try. A refused document
/// keeps whatever row the table holds.
///
/// A replace the hosted service could not take ([`ends_the_write`]: it could
/// not be reached, did not answer in time, or turned the call away for the
/// account's request rate) says nothing of the rows, and every later call
/// would meet the same service: it ends the write with that error, with no
/// further call, and no document is blamed: every document not written is
/// reported under the status a client sends it again for ([`untried`]).
fn replace_rows_checked(
    table: &Supertable,
    gate: &Gate,
    rows: &[(usize, Value)],
    failed: &mut Vec<Value>,
    id_bytes: usize,
) -> Result<(), InfinoError> {
    if rows.is_empty() {
        return Ok(());
    }
    let values: Vec<Value> = rows.iter().map(|(_, v)| v.clone()).collect();
    let mut ids: Vec<&str> = values
        .iter()
        .filter_map(|v| v.get(OSID).and_then(Value::as_str))
        .collect();
    ids.sort_unstable();
    ids.dedup();
    let replace_each = |runs: &[&[(usize, Value)]], failed: &mut Vec<Value>| {
        each_run(runs, failed, |run, failed| {
            replace_rows_checked(table, gate, run, failed, id_bytes)
        })
    };
    // More ids than one call may name: each run of them replaces on its own.
    if let Some(runs) = rows_by_id_run(rows, &ids, id_bytes) {
        let runs: Vec<&[(usize, Value)]> = runs.iter().map(Vec::as_slice).collect();
        return replace_each(&runs, failed);
    }
    let pred = col(OSID).in_list(ids.iter().map(|s| lit(s.to_string())).collect(), false);
    let replace = {
        let _pass = gate.whole();
        waiting(|| table.update_rows(pred.clone(), &values))
    };
    let e = match replace {
        Ok(_) => return Ok(()),
        Err(e) if ends_the_write(&e) => {
            untried(rows, &e, failed);
            return Err(e);
        }
        Err(e) => e,
    };
    let reason = short_reason(format_args!("replace: {e}"));
    let over_cap = over_field_cap(&e);
    if rows.len() > 1 && refused_for_size(&e) {
        let (a, b) = rows.split_at(rows.len() / 2);
        return replace_each(&[a, b], failed);
    }
    if counts_may_explain(&e) {
        match rows_under(table, &ids) {
            Ok(counts) => {
                let single: HashSet<&str> = ids
                    .iter()
                    .zip(&counts)
                    .filter(|(_, &n)| n == 1)
                    .map(|(id, _)| *id)
                    .collect();
                let (once, miscounted): (Vec<_>, Vec<_>) =
                    rows.iter().cloned().partition(|(_, v)| {
                        v.get(OSID)
                            .and_then(Value::as_str)
                            .is_some_and(|id| single.contains(id))
                    });
                if !miscounted.is_empty() {
                    if let Err(stop) = rewrite_rows(table, gate, &miscounted, failed, id_bytes) {
                        untried(&once, &stop, failed);
                        return Err(stop);
                    }
                    return replace_rows_checked(table, gate, &once, failed, id_bytes);
                }
            }
            Err(count) if ends_the_write(&count) => {
                untried(rows, &count, failed);
                return Err(count);
            }
            Err(_) => {}
        }
    }
    if rows.len() > 1 && (over_cap || refuses_content(&e)) {
        let (a, b) = rows.split_at(rows.len() / 2);
        return replace_each(&[a, b], failed);
    }
    let permanent = rows.len() == 1 && refuses_content(&e);
    for (i, _) in rows {
        failed.push(json!({"i": i, "reason": reason, "permanent": permanent}));
    }
    Ok(())
}

/// Report each of `rows` as not taken, for a later try, with `reason`.
fn not_taken(rows: &[(usize, Value)], reason: &str, failed: &mut Vec<Value>) {
    for (i, _) in rows {
        failed.push(json!({"i": i, "reason": reason, "permanent": false}));
    }
}

/// Whether the hosted service turned a call away for the account's request
/// rate (`429`), or for want of capacity (`529`): it answers either at its
/// edge, before the call reaches the table, so the call wrote nothing and a
/// later try may pass. Both are answered as a 429. The engine's hosted
/// client passes the status on as an untyped backend error
/// (`<op>: server returned 429: <body>`).
fn rate_limited(e: &InfinoError) -> bool {
    matches!(e, InfinoError::Backend(m)
        if matches!(status_in(m), Some(STATUS_TOO_MANY_REQUESTS | STATUS_OVERLOADED)))
}

/// Whether a failed call ends the write it is part of: the hosted service
/// could not be reached or did not answer ([`InfinoError::Unavailable`]), or
/// turned the call away for the account's request rate ([`rate_limited`]).
/// Neither says anything of the rows the call carried, and every later call
/// of the write would meet the same service, so none is made.
fn ends_the_write(e: &InfinoError) -> bool {
    matches!(e, InfinoError::Unavailable(_)) || rate_limited(e)
}

/// The status a document is answered with when a write ended by `stop`
/// ([`ends_the_write`]) did not write it: 429 for a rate limit
/// ([`rate_limited`]), 503 for a service that could not be reached or did
/// not answer; None for any other error.
fn untried_status(stop: &InfinoError) -> Option<u16> {
    if rate_limited(stop) {
        Some(STATUS_TOO_MANY_REQUESTS)
    } else if matches!(stop, InfinoError::Unavailable(_)) {
        Some(SERVICE_UNAVAILABLE)
    } else {
        None
    }
}

/// Report `rows`, which a write ended by `stop` ([`ends_the_write`]) did not
/// write, each not taken, under [`untried_status`], so the plugin answers
/// that status for it and the client sends it again. The documents of the
/// calls answered before `stop` are not reported: they are written, and a
/// client that sent them again would store an auto-id document twice.
///
/// Under a rate limit none of `rows` is in the table, as the service wrote
/// nothing of a call it turned away, so the retry stores each once. Under
/// an unavailable service the call it did not answer may have landed: its
/// documents are answered 503 all the same, as after any write that timed
/// out, and the retry looks their ids up.
fn untried(rows: &[(usize, Value)], stop: &InfinoError, failed: &mut Vec<Value>) {
    let Some(status) = untried_status(stop) else {
        return;
    };
    let reason = if status == SERVICE_UNAVAILABLE {
        short_reason(format_args!("{UNANSWERED}: {stop}"))
    } else {
        short_reason(stop)
    };
    for (i, _) in rows {
        failed.push(json!({
            "i": i,
            "reason": reason,
            "permanent": false,
            "status": status,
        }));
    }
}

/// Write each of `runs` with `write`, in order, until one ends the write
/// ([`ends_the_write`]): the documents of every run after it are reported
/// as not written ([`untried`]), and the error is returned.
fn each_run(
    runs: &[&[(usize, Value)]],
    failed: &mut Vec<Value>,
    mut write: impl FnMut(&[(usize, Value)], &mut Vec<Value>) -> Result<(), InfinoError>,
) -> Result<(), InfinoError> {
    for (k, run) in runs.iter().enumerate() {
        if let Err(stop) = write(run, failed) {
            for rest in &runs[k + 1..] {
                untried(rest, &stop, failed);
            }
            return Err(stop);
        }
    }
    Ok(())
}

/// How many bytes of ids one mutation over the hosted service may name. The
/// hosted transport sends an update's or a delete's predicate in the
/// request's URL, and its front end refuses a long one (413 near 80 KB,
/// a broken pipe a little past it), so a list of more ids is cut into runs
/// of at most this many, each its own call. Measured as [`id_cost`] counts.
const HOSTED_ID_LIST_BYTES: usize = 32 * 1024;

/// Bytes of a URL a byte takes at most once percent-encoded (`%XX`).
const ENCODED_BYTE: usize = 3;

/// Bytes of a URL the frame around one id in an `IN` list takes once
/// encoded: its two quotes (`%27` each), the comma after it (`%2C`) and the
/// space (`+`).
const ID_FRAME_BYTES: usize = 10;

/// What one id adds to a predicate's URL, at most: each byte
/// percent-encoded, a quote inside it twice (the SQL writes `'` as `''`),
/// and its frame ([`ID_FRAME_BYTES`]).
fn id_cost(id: &str) -> usize {
    let quotes = id.bytes().filter(|&b| b == b'\'').count();
    (id.len() + quotes) * ENCODED_BYTE + ID_FRAME_BYTES
}

/// `ids`, in order, cut into runs whose [`id_cost`] sums to at most
/// `budget`; a run holds one id at least.
fn id_runs<S: AsRef<str>>(ids: &[S], budget: usize) -> Vec<&[S]> {
    let mut runs = Vec::new();
    let (mut start, mut cost) = (0, 0usize);
    for (i, id) in ids.iter().enumerate() {
        let c = id_cost(id.as_ref());
        if i > start && cost.saturating_add(c) > budget {
            runs.push(&ids[start..i]);
            start = i;
            cost = 0;
        }
        cost = cost.saturating_add(c);
    }
    if start < ids.len() {
        runs.push(&ids[start..]);
    }
    runs
}

/// `rows` grouped by the run of `ids` (sorted, without repeats) their id
/// falls in ([`id_runs`]), in run order; `None` when every id fits one run.
fn rows_by_id_run(
    rows: &[(usize, Value)],
    ids: &[&str],
    budget: usize,
) -> Option<Vec<Vec<(usize, Value)>>> {
    let runs = id_runs(ids, budget);
    if runs.len() < 2 {
        return None;
    }
    let mut run_of: HashMap<&str, usize> = HashMap::new();
    for (r, run) in runs.iter().enumerate() {
        for id in *run {
            run_of.insert(id, r);
        }
    }
    let mut grouped = vec![Vec::new(); runs.len()];
    for row in rows {
        let r = row
            .1
            .get(OSID)
            .and_then(Value::as_str)
            .and_then(|id| run_of.get(id))
            .copied()
            .unwrap_or(0);
        grouped[r].push(row.clone());
    }
    Some(grouped)
}

/// Whether the hosted service refused a request for its size (413): the
/// same request in halves can pass. A transport error is not taken for one,
/// though an oversized URL can break the connection too: a whole outage
/// breaks it as well, and splitting a batch through an outage would only
/// multiply the calls that fail. [`HOSTED_ID_LIST_BYTES`] keeps predicates
/// short of either.
fn refused_for_size(e: &InfinoError) -> bool {
    matches!(e, InfinoError::Backend(m) if status_in(m) == Some(STATUS_PAYLOAD_TOO_LARGE))
}

/// The longest reason a refused document is reported with.
const REASON_MAX_BYTES: usize = 1024;

/// `e` as a refused document's reason: every URL in it without its query,
/// which on the hosted service holds the request's predicate (every id it
/// names, so as long as the batch), and the whole cut to
/// [`REASON_MAX_BYTES`]. The reason is repeated for each document of a
/// failed write, so its length must not grow with the write's.
fn short_reason(e: impl std::fmt::Display) -> String {
    let full = e.to_string();
    let mut out = String::with_capacity(full.len().min(REASON_MAX_BYTES + 64));
    let mut rest = full.as_str();
    while let Some(at) = rest.find("http") {
        let (head, from) = rest.split_at(at);
        out.push_str(head);
        if from.starts_with("http://") || from.starts_with("https://") {
            let end = from.find(char::is_whitespace).unwrap_or(from.len());
            let url = &from[..end];
            // A transport error writes `url: kind`, so a trailing colon is the text's.
            let (url, colon) = url.strip_suffix(':').map_or((url, ""), |u| (u, ":"));
            match url.find('?') {
                Some(q) => {
                    out.push_str(&url[..q]);
                    out.push_str("?<query left out>");
                }
                None => out.push_str(url),
            }
            out.push_str(colon);
            rest = &from[end..];
        } else {
            out.push_str("http");
            rest = &from[4..];
        }
        if out.len() > REASON_MAX_BYTES {
            break;
        }
    }
    out.push_str(rest);
    if out.len() > REASON_MAX_BYTES {
        let mut cut = REASON_MAX_BYTES;
        while !out.is_char_boundary(cut) {
            cut -= 1;
        }
        let dropped = full.len().saturating_sub(cut);
        out.truncate(cut);
        out.push_str(&format!("... ({dropped} bytes left out)"));
    }
    out
}

/// Whether a refused update may have been refused for its count of rows:
/// any refusal but one of its rows' content or of the field cap. The local
/// engine names a miscount ([`InfinoError::Cardinality`]); the hosted
/// service answers it as a status the engine passes on untyped.
fn counts_may_explain(e: &InfinoError) -> bool {
    !refuses_content(e) && !over_field_cap(e)
}

/// Ids whose rows one call to [`rows_under`] counts.
const COUNT_IDS_PER_CALL: usize = 1_000;

/// How many live rows the table holds under each of `ids`, in order.
fn rows_under(table: &Supertable, ids: &[&str]) -> Result<Vec<u64>, InfinoError> {
    let mut counts = Vec::with_capacity(ids.len());
    for chunk in ids.chunks(COUNT_IDS_PER_CALL) {
        let preds: Vec<Expr> = chunk
            .iter()
            .map(|id| col(OSID).eq(lit(id.to_string())))
            .collect();
        let got = table.count_where(&preds)?;
        if got.len() != chunk.len() {
            return Err(InfinoError::Backend(format!(
                "{} counts for {} ids",
                got.len(),
                chunk.len()
            )));
        }
        counts.extend(got);
    }
    Ok(counts)
}

/// Write documents whose ids hold no row or several: every row under those
/// ids is deleted, then the documents are appended ([`append_checked`]), so
/// each id ends with the one row its document brings. Two mutations: until
/// the append lands a read finds no row under the ids, and a document whose
/// append fails is reported for a later try, which finds no row and
/// appends it. A delete that fails reports its documents for a later try,
/// and one the hosted service could not take ([`ends_the_write`]) ends the
/// write with that error, as every later delete would meet it.
fn rewrite_rows(
    table: &Supertable,
    gate: &Gate,
    rows: &[(usize, Value)],
    failed: &mut Vec<Value>,
    id_bytes: usize,
) -> Result<(), InfinoError> {
    let mut ids: Vec<String> = rows
        .iter()
        .filter_map(|(_, v)| v.get(OSID).and_then(Value::as_str))
        .map(str::to_owned)
        .collect();
    ids.sort_unstable();
    ids.dedup();
    eprintln!(
        "[capi] replace: {} ids held no row or several; their rows are deleted and the documents appended",
        ids.len()
    );
    // Each run of ids ([`id_runs`]) is deleted, then its documents appended,
    // on its own: a run whose delete fails reports its documents alone.
    let id_refs: Vec<&str> = ids.iter().map(String::as_str).collect();
    let groups = rows_by_id_run(rows, &id_refs, id_bytes).unwrap_or_else(|| vec![rows.to_vec()]);
    let groups: Vec<&[(usize, Value)]> = groups.iter().map(Vec::as_slice).collect();
    each_run(&groups, failed, |group, failed| {
        let mut run: Vec<String> = group
            .iter()
            .filter_map(|(_, v)| v.get(OSID).and_then(Value::as_str))
            .map(str::to_owned)
            .collect();
        run.sort_unstable();
        run.dedup();
        let pred = col(OSID).in_list(run.into_iter().map(lit).collect(), false);
        let deleted = {
            let _pass = gate.whole();
            waiting(|| table.delete(pred.clone()))
        };
        match deleted {
            Ok(_) => append_checked(table, gate, group, failed),
            Err(e) if ends_the_write(&e) => {
                untried(group, &e, failed);
                Err(e)
            }
            Err(e) => {
                not_taken(group, &short_reason(format_args!("replace: {e}")), failed);
                Ok(())
            }
        }
    })
}

/// Index many documents in one append. `osids` and `sources` are parallel
/// arrays of length `count`. Returns
/// `{"ok":n,"failed":[{"i":..,"reason":..,"permanent":..,"status"?:..}]}`,
/// where `i` indexes the input arrays, `permanent` says every later try is
/// refused the same way ([`NotTaken`]) and `status` marks a document the
/// write did not reach because the hosted service ended it ([`untried`]):
/// 429 when it turned a call away for the account's request rate, 503 when
/// it did not answer one; or `{"error":..}` when nothing ran.
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
        if let Err(refused) = conn.refuse_write(name) {
            return err_json(&refused);
        }
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
        let indexed = conn.indexed(name);
        let engine = conn.engine_columns(name);
        let stamp = conn.stamp(name);

        let mut failed: Vec<Value> = Vec::new();
        let mut rows: Vec<(usize, Map<String, Value>)> = Vec::with_capacity(n);
        for i in 0..n {
            let osid = unsafe { cstr(os_p[i]) }.unwrap_or("");
            let source = unsafe { cstr(sr_p[i]) }.unwrap_or("{}");
            let mut flat = match conform_doc(source, &schema, &kept, &engine, stamp.as_deref(), &[], &indexed)
            {
                Ok(flat) => flat,
                Err(why) => {
                    failed.push(json!({ "i": i, "reason": why, "permanent": true }));
                    continue;
                }
            };
            flat.insert(OSID.into(), Value::String(osid.to_string()));
            flat.insert(SOURCE.into(), Value::String(mapped_values(source).0.to_string()));
            rows.push((i, flat));
        }

        let mut maps: Vec<Map<String, Value>> = rows.iter().map(|(_, m)| m.clone()).collect();
        let (table, schema) = match ensure_list_columns(conn, name, &schema, &maps) {
            Ok(true) => match conn.table(name) {
                Ok(t) => {
                    let s = t.schema();
                    (t, s)
                }
                Err(e) => return engine_err_json(&e),
            },
            Ok(false) => (table, schema),
            Err(e) => return engine_err_json(&e),
        };
        unify_new_paths(&mut maps, &schema);
        let adds_columns = conn.hosted
            && conn
                .table_schema(name)
                .is_ok_and(|doc| adds_columns(&doc, &maps));
        let indexed: Vec<(usize, Value)> = rows
            .iter()
            .zip(maps)
            .map(|((i, _), m)| (*i, Value::Object(m)))
            .collect();
        // A write the service ended reports what it did not write
        // ([`untried`]); only a failure of no document fails the call.
        let unanswered = match append_checked(&table, &conn.gate(name), &indexed, &mut failed) {
            Err(e) if !ends_the_write(&e) => return engine_err_json(&e),
            Err(e) => matches!(e, InfinoError::Unavailable(_)),
            Ok(()) => false,
        };
        let ok = n - failed.len();
        // A service that stopped answering is not asked for the schema too.
        if adds_columns && ok > 0 && !unanswered {
            conn.reread_schema(name);
        }
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
                ptr::null(),
                count as usize,
            )
        } {
            Ok(p) => p,
            Err(why) => return out_json(why),
        };
        let table = match conn.table(name) {
            Ok(t) => t,
            Err(e) => return engine_err_json(&e),
        };
        let n = prepared.n;
        let adds_columns = prepared.adds_columns;
        let failed = match append_prepared(
            &table,
            &conn.gate(name),
            vec![prepared],
            &[],
            &[],
            conn.id_list_bytes(),
        ) {
            Ok(failed) => failed,
            Err(e) => return engine_err_json(&e),
        };
        let failed: Vec<Value> = failed
            .into_iter()
            .map(|(_, i, reason, permanent, status)| {
                json!({ "i": i, "reason": reason, "permanent": permanent, "status": status })
            })
            .collect();
        let ok = n - failed.len();
        if adds_columns && ok > 0 {
            conn.reread_schema(name);
        }
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
    /// Whether the rows hold a path the kept schema document lacks, so the
    /// append adds a column to it ([`Bridge::reread_schema`]). Only a hosted
    /// connection keeps a document; over storage this is false.
    adds_columns: bool,
}

/// One row of a buffer handed to [`prepare`]: its id, source, routing value
/// (empty for none) and ignored fields, borrowed from the buffer, with its
/// stamp and operation (negative for none).
struct RowIn<'a> {
    id: &'a str,
    src: &'a str,
    routing: &'a str,
    ignored: Vec<&'a str>,
    seq: i64,
    op: i64,
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
/// [`RowMeta`]). With `ig_ends`, each row's segment ends with the paths of
/// the fields OpenSearch's mapper left out of its document
/// ([`IGNORED_SEPARATOR`] between them; an empty one is none), up to
/// `ig_ends[i]`: the row leaves them out and keeps them in [`IGNORED`].
#[allow(clippy::too_many_arguments)]
unsafe fn prepare(
    conn: &Bridge,
    name: &str,
    buf: *const u8,
    buf_len: usize,
    id_ends: *const u32,
    src_ends: *const u32,
    meta: Option<RowMeta>,
    ig_ends: *const u32,
    n: usize,
) -> Result<Prepared, Value> {
    let bytes = unsafe { std::slice::from_raw_parts(buf, buf_len) };
    let id_ends = unsafe { std::slice::from_raw_parts(id_ends, n) };
    let src_ends = unsafe { std::slice::from_raw_parts(src_ends, n) };
    let ig_ends = (!ig_ends.is_null()).then(|| unsafe { std::slice::from_raw_parts(ig_ends, n) });
    let rt_ends = meta.map(|m| unsafe { std::slice::from_raw_parts(m.rt_ends, n) });
    let seqs = meta.map(|m| unsafe { std::slice::from_raw_parts(m.seqs, n) });
    let ops = meta.map(|m| unsafe { std::slice::from_raw_parts(m.ops, n) });
    let gen = meta.map_or(0, |m| m.gen);
    // Preparing a batch can add a column to the table (a list field's), so
    // it is refused as the append it prepares is.
    conn.refuse_write(name).map_err(|e| error_value(&e))?;
    let table = conn.table(name).map_err(|e| engine_error_value(&e))?;
    let table_schema = conn
        .table_schema(name)
        .map_err(|e| engine_error_value(&e))?;
    let schema = table.schema();
    let kept = conn.kept(name);
    let indexed = conn.indexed(name);
    let engine = conn.engine_columns(name);
    let stamp = conn.stamp(name);
    // A table whose ignored column holds something else (a document's own
    // field of that name, written before the column was reserved) keeps no
    // ignored fields, rather than refusing every row that has some.
    let keeps_ignored = schema
        .field_with_name(IGNORED)
        .map_or(true, |f| {
            matches!(f.data_type(), DataType::List(item)
                if matches!(item.data_type(), DataType::Utf8 | DataType::LargeUtf8))
        });

    // Each row's id, source, routing value and ignored fields as borrowed
    // slices of the buffer, with its stamp and operation.
    let mut rows_in: Vec<RowIn> = Vec::with_capacity(n);
    let mut start = 0usize;
    for i in 0..n {
        let (id_end, src_end) = (id_ends[i] as usize, src_ends[i] as usize);
        let rt_end = rt_ends.map_or(src_end, |ends| ends[i] as usize);
        let ig_end = ig_ends.map_or(rt_end, |ends| ends[i] as usize);
        if id_end < start
            || src_end < id_end
            || rt_end < src_end
            || ig_end < rt_end
            || ig_end > buf_len
        {
            return Err(error_value("offsets out of range"));
        }
        rows_in.push(RowIn {
            id: std::str::from_utf8(&bytes[start..id_end]).unwrap_or(""),
            src: std::str::from_utf8(&bytes[id_end..src_end]).unwrap_or("{}"),
            routing: std::str::from_utf8(&bytes[src_end..rt_end]).unwrap_or(""),
            ignored: ignored_paths(std::str::from_utf8(&bytes[rt_end..ig_end]).unwrap_or("")),
            seq: seqs.map_or(-1, |s| s[i]),
            op: ops.map_or(-1, |o| o[i]),
        });
        start = ig_end;
    }

    let parsed: Vec<Result<Map<String, Value>, String>> = rows_in
        .par_iter()
        .map(|row| {
            let RowIn {
                id,
                src,
                routing,
                ignored,
                seq,
                op,
            } = row;
            conform_doc(src, &schema, &kept, &engine, stamp.as_deref(), ignored, &indexed).map(|mut flat| {
                flat.insert(OSID.into(), Value::String((*id).to_string()));
                flat.insert(SOURCE.into(), Value::String(mapped_values(src).0.to_string()));
                if keeps_ignored && !ignored.is_empty() {
                    flat.insert(IGNORED.into(), json!(ignored));
                }
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
    let changed =
        ensure_list_columns(conn, name, &schema, &rows).map_err(|e| engine_error_value(&e))?;
    let (table_schema, schema) = if changed {
        (
            conn.table_schema(name)
                .map_err(|e| engine_error_value(&e))?,
            conn.table(name)
                .map_err(|e| engine_error_value(&e))?
                .schema(),
        )
    } else {
        (table_schema, schema)
    };
    unify_new_paths(&mut rows, &schema);
    let adds_columns = conn.hosted && adds_columns(&table_schema, &rows);
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
        adds_columns,
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

/// The distinct `osid`s of the rows of `batch`; empty when it has no
/// readable id column.
fn osids_of(batch: &RecordBatch) -> Vec<String> {
    let Some(column) = batch.column_by_name(OSID) else {
        return Vec::new();
    };
    let mut ids: Vec<String> = if let Some(a) = column.as_any().downcast_ref::<LargeStringArray>() {
        a.iter().flatten().map(str::to_owned).collect()
    } else if let Some(a) = column.as_any().downcast_ref::<StringArray>() {
        a.iter().flatten().map(str::to_owned).collect()
    } else {
        Vec::new()
    };
    ids.sort_unstable();
    ids.dedup();
    ids
}

/// A document an append did not take: (batch index, document index, reason,
/// permanent, status). It is permanent when the table refuses what the
/// document carries, so every later try is refused the same way; otherwise
/// it failed for a reason of the moment and a later try may take it. The
/// status is the one the caller answers for it when it is not a refusal of
/// the document ([`untried`]): 429 for a write the hosted service turned
/// away for the account's request rate, 503 for one it did not answer; the
/// client sends either again.
type NotTaken = (usize, usize, String, bool, Option<u16>);

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

/// Whether a write landed. One the hosted service ended ([`ends_the_write`]:
/// it did not answer, or turned the call away for the account's request
/// rate) did not land as far as this call knows, and is kept in `stop`: the
/// caller makes no further call, and reports every document it has not
/// written under it ([`untried`]).
fn landed<T>(result: Result<T, InfinoError>, stop: &mut Option<InfinoError>) -> bool {
    match result {
        Ok(_) => true,
        Err(e) if ends_the_write(&e) => {
            *stop = Some(e);
            false
        }
        Err(_) => false,
    }
}

/// Append prepared batches as one commit. When `replaced` names ids, the
/// rows under those ids are replaced in the same mutation as their
/// tombstones (the engine's update), whatever shape the batches take: they
/// are joined over the union of their schemas first, so a batch that added
/// a path does not push the write onto a path that tombstones ahead of an
/// append. Whatever the engine refuses as Arrow, the append of the rows that
/// replace nothing or the replace of those that do, goes a document at a
/// time, as do the rows of a batch that could not be converted: appends
/// through [`append_checked`], replaces through [`replace_rows_checked`],
/// each naming the documents the engine refuses. `seqs[b]`, when given,
/// holds the sequence number of each document of batch `b`
/// ([`stamp_seqs`]). Returns every document not taken ([`NotTaken`]).
///
/// A write the hosted service ends ([`ends_the_write`]) is not an error of
/// the call: no call is made after it, the documents the calls before it
/// wrote are taken, and the documents of that call and of every one after
/// it are reported under the status a client sends them again for
/// ([`untried`]). So a bulk whose joined batch's replace lands before its
/// append finds the service gone, whose joined rows land before the
/// documents of a batch that could not be converted find it gone, or whose
/// documents land in parts until a later part finds it gone, answers only
/// the documents not yet written: a client sends just those again, and an
/// auto-id document already written is not stored twice. Turned away for
/// the account's request rate ([`rate_limited`]), the call wrote nothing,
/// and they are answered 429; unanswered, it may have landed, and they are
/// answered 503, as after any write that timed out: a retry looks their ids
/// up and replaces those rows, so they are not stored twice, and a retry
/// with `op_type=create` is answered 409 for them, as they exist.
fn append_prepared(
    table: &Supertable,
    gate: &Gate,
    mut prepared: Vec<Prepared>,
    replaced: &[String],
    seqs: &[Vec<i64>],
    id_bytes: usize,
) -> Result<Vec<NotTaken>, InfinoError> {
    let mut failed: Vec<NotTaken> = Vec::new();
    for (b, p) in prepared.iter_mut().enumerate() {
        // Refused by the columns before conversion, as every try would be.
        for (i, why) in &p.failed {
            failed.push((b, *i, why.clone(), true, None));
        }
        if let Some(seqs) = seqs.get(b) {
            if let Err(why) = stamp_seqs(p, seqs) {
                for i in &p.index_of {
                    failed.push((b, *i, format!("sequence number: {why}"), false, None));
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
                        failed.push((b, *i, why.clone(), false, None));
                    }
                }
            }
            None
        }
    };
    // Whether the joined batch's rows that replace nothing, and those that
    // replace, landed as Arrow.
    let mut fresh_landed = joined.is_none();
    let mut replaced_landed = joined.is_none();
    // The error that ended the write ([`landed`]): no call is made after
    // it, and every document not yet written is reported under it.
    let mut stop: Option<InfinoError> = None;
    if let Some(batch) = &joined {
        if replaced.is_empty() {
            let _pass = gate.append();
            fresh_landed = landed(waiting(|| table.append(batch)), &mut stop);
            replaced_landed = true;
        } else {
            match split_by_osid(batch, replaced) {
                Some((replacing, fresh)) => {
                    // The replace names the ids this commit carries a row for. An
                    // id in `replaced` whose document was refused keeps its old
                    // row, and naming it would make the replace match more rows
                    // than it brings, which the engine refuses whole.
                    replaced_landed = replacing.num_rows() == 0 || {
                        let ids = osids_of(&replacing);
                        // Ids past one call's list go a run at a time,
                        // below ([`replace_rows_checked`]).
                        id_runs(&ids, id_bytes).len() < 2 && {
                            let pred = col(OSID).in_list(ids.into_iter().map(lit).collect(), false);
                            let _pass = gate.whole();
                            landed(waiting(|| table.update(pred.clone(), &replacing)), &mut stop)
                        }
                    };
                    fresh_landed = fresh.num_rows() == 0
                        || (stop.is_none() && {
                            let _pass = gate.append();
                            landed(waiting(|| table.append(&fresh)), &mut stop)
                        });
                }
                None => {
                    let why = "the batch carries no readable id column".to_string();
                    for (b, p) in prepared.iter().enumerate() {
                        if p.batch.is_some() {
                            for i in &p.index_of {
                                failed.push((b, *i, why.clone(), false, None));
                            }
                        }
                    }
                    fresh_landed = true;
                    replaced_landed = true;
                }
            }
        }
    }
    let joined_batches = joined.is_some();
    let replaced: HashSet<&str> = replaced.iter().map(String::as_str).collect();
    // What did not land as Arrow goes a document at a time: the rows of a
    // batch that could not be converted, and the rows of the joined batch
    // whose append or replace the engine refused. A document that replaces a
    // row goes through the engine's replace ([`replace_rows_checked`]), never
    // an append that would put a second row beside the one it replaces.
    for (b, p) in prepared.into_iter().enumerate() {
        let converted = p.batch.is_some();
        if converted && !joined_batches {
            continue; // reported as untyped above
        }
        let fresh_here = !converted || !fresh_landed;
        let replacing_here = !converted || !replaced_landed;
        if !fresh_here && !replacing_here {
            continue;
        }
        let mut indexed: Vec<(usize, Value)> = Vec::new();
        let mut replacing: Vec<(usize, Value)> = Vec::new();
        for (j, v) in p.values.into_iter().enumerate() {
            let id = v.get(OSID).and_then(Value::as_str).unwrap_or("");
            if replaced.contains(id) {
                if replacing_here {
                    replacing.push((p.index_of[j], v));
                }
            } else if fresh_here {
                indexed.push((p.index_of[j], v));
            }
        }
        let mut refused: Vec<Value> = Vec::new();
        if let Some(limit) = &stop {
            untried(&replacing, limit, &mut refused);
            untried(&indexed, limit, &mut refused);
        } else {
            let wrote = match replace_rows_checked(table, gate, &replacing, &mut refused, id_bytes)
            {
                Ok(()) => append_checked(table, gate, &indexed, &mut refused),
                Err(e) => {
                    untried(&indexed, &e, &mut refused);
                    Err(e)
                }
            };
            match wrote {
                Ok(()) => {}
                // Every document it did not write is reported ([`untried`]).
                Err(e) if ends_the_write(&e) => stop = Some(e),
                Err(e) => return Err(e),
            }
        }
        for r in refused {
            let i = r["i"].as_u64().unwrap_or(0) as usize;
            failed.push((
                b,
                i,
                r["reason"].as_str().unwrap_or("refused").to_string(),
                r["permanent"].as_bool().unwrap_or(false),
                r["status"].as_u64().and_then(|s| u16::try_from(s).ok()),
            ));
        }
    }
    Ok(failed)
}

/// Parse and convert a batch ahead of its append; see [`Prepared`]. The
/// buffer layout is that of [`infino_index_batch_buf`], each row's segment
/// followed by the paths of its ignored fields up to `ig_ends[i]` when
/// `ig_ends` is not null (see [`prepare`]). Returns null when
/// the table is unknown or the offsets do not fit the buffer; otherwise a
/// handle that [`infino_append_prepared`] consumes or
/// [`infino_free_prepared`] releases.
#[no_mangle]
#[allow(clippy::too_many_arguments)]
pub unsafe extern "C" fn infino_prepare_batch(
    h: *mut Bridge,
    name: *const c_char,
    buf: *const u8,
    buf_len: usize,
    id_ends: *const u32,
    src_ends: *const u32,
    ig_ends: *const u32,
    count: c_int,
) -> *mut Prepared {
    // Cleared first, so a null this call returns before it records a reason
    // (null arguments, a panic) never reports an earlier call's.
    PREPARE_ERROR.with(|slot| *slot.borrow_mut() = None);
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
                ig_ends,
                count as usize,
            )
        } {
            Ok(p) => Box::into_raw(Box::new(p)),
            Err(why) => {
                eprintln!("[capi] prepare: {why}");
                PREPARE_ERROR.with(|slot| *slot.borrow_mut() = Some(why.to_string()));
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
/// `osrouting`, `osseq`, `osop` and `osgen` columns. With `ig_ends`, each
/// routing value is followed by the paths of the row's ignored fields, as in
/// [`infino_prepare_batch`].
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
    ig_ends: *const u32,
    count: c_int,
) -> *mut Prepared {
    // Cleared first, so a null this call returns before it records a reason
    // (null arguments, a panic) never reports an earlier call's.
    PREPARE_ERROR.with(|slot| *slot.borrow_mut() = None);
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
                ig_ends,
                count as usize,
            )
        } {
            Ok(p) => Box::into_raw(Box::new(p)),
            Err(why) => {
                eprintln!("[capi] prepare: {why}");
                PREPARE_ERROR.with(|slot| *slot.borrow_mut() = Some(why.to_string()));
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
/// `{"ok":n,"failed":[{"b":batch,"i":doc,"reason":..,"permanent":..,"status":..}]}`,
/// where `b` indexes `prepared`, `i` the documents of that batch,
/// `permanent` says every later try is refused the same way ([`NotTaken`])
/// and `status` is 429 for a document the hosted service turned away for the
/// account's request rate and 503 for one it did not answer ([`untried`]),
/// else null; or `{"error":..}` when nothing ran (the handles are released
/// either way).
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
        if let Err(refused) = conn.refuse_write(name) {
            return err_json(&refused);
        }
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
            Err(e) => return engine_err_json(&e),
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
        let adds_columns = owned.iter().any(|p| p.adds_columns);
        let failed = match append_prepared(
            &table,
            &conn.gate(name),
            owned,
            &replaced,
            &per_batch,
            conn.id_list_bytes(),
        ) {
            Ok(failed) => failed,
            Err(e) => return engine_err_json(&e),
        };
        let ok = n.saturating_sub(failed.len());
        // A write adds its columns only when it lands, so one that landed
        // nothing changed nothing, and asks the service nothing more; nor is
        // a service that stopped answering asked for the schema.
        let unanswered = failed.iter().any(|f| f.4 == Some(SERVICE_UNAVAILABLE));
        if adds_columns && ok > 0 && !unanswered {
            conn.reread_schema(name);
        }
        let failed: Vec<Value> = failed
            .into_iter()
            .map(|(b, i, reason, permanent, status)| {
                json!({ "b": b, "i": i, "reason": reason, "permanent": permanent, "status": status })
            })
            .collect();
        out_json(json!({ "ok": ok, "failed": failed }))
    }))
    .unwrap_or(ptr::null_mut())
}

/// Why the last prepare on this thread returned null, as
/// `{"error", "status"?}`; null when it did not fail (or the argument
/// checks refused it before anything ran). Read once: the slot is cleared.
#[no_mangle]
pub unsafe extern "C" fn infino_prepare_error() -> *mut c_char {
    match PREPARE_ERROR.with(|slot| slot.borrow_mut().take()) {
        Some(json) => out_string(json),
        None => ptr::null_mut(),
    }
}

/// Release a prepared batch that will not be appended.
#[no_mangle]
pub unsafe extern "C" fn infino_free_prepared(p: *mut Prepared) {
    if !p.is_null() {
        drop(unsafe { Box::from_raw(p) });
    }
}

/// The rows of a prepared batch that do not conform to the table's columns,
/// as `[{"i": row, "reason": why}]`, so the caller can answer or send them
/// again before the batch is appended; the handle stays the caller's. Null
/// for a null handle.
#[no_mangle]
pub unsafe extern "C" fn infino_prepared_failed(p: *const Prepared) -> *mut c_char {
    catch_unwind(AssertUnwindSafe(|| {
        let Some(p) = (unsafe { p.as_ref() }) else {
            return ptr::null_mut();
        };
        let failed: Vec<Value> = p
            .failed
            .iter()
            .map(|(i, why)| json!({ "i": i, "reason": why }))
            .collect();
        out_json(Value::Array(failed))
    }))
    .unwrap_or(ptr::null_mut())
}

/// What [`infino_delete_osids`] returns for a delete the hosted service
/// turned away for the account's request rate ([`rate_limited`]).
const DELETE_RATE_LIMITED: c_int = -3;
/// What [`infino_delete_osids`] returns for a delete the hosted service
/// could not be reached for, or did not answer ([`InfinoError::Unavailable`]).
const DELETE_UNAVAILABLE: c_int = -4;
/// What [`infino_delete_osids`] returns for any other failed delete.
const DELETE_FAILED: c_int = -2;

/// The code [`infino_delete_osids`] returns for a delete that failed with `e`.
fn delete_code(e: &InfinoError) -> c_int {
    if rate_limited(e) {
        DELETE_RATE_LIMITED
    } else if matches!(e, InfinoError::Unavailable(_)) {
        DELETE_UNAVAILABLE
    } else {
        DELETE_FAILED
    }
}

/// Tombstone every row whose `osid` is in the list, in one mutation. Backs the
/// upsert path (translog replay, updates): delete the ids of a batch, then
/// append the batch. Returns 0, or a negative code: -1 for a null argument,
/// [`DELETE_RATE_LIMITED`] or [`DELETE_UNAVAILABLE`] for a delete the
/// caller tries again later, and [`DELETE_FAILED`] for any other failure.
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
        if let Err(refused) = conn.refuse_write(name) {
            eprintln!("[capi] delete_osids: {refused}");
            return DELETE_FAILED;
        }
        if osids.is_null() || count <= 0 {
            return 0;
        }
        let n = count as usize;
        let os_p = unsafe { std::slice::from_raw_parts(osids, n) };
        let ids: Vec<&str> = os_p.iter().filter_map(|p| unsafe { cstr(*p) }).collect();
        // No table yet means nothing to delete. A table the hosted service
        // could not be asked for is a failure, not an empty table.
        let table = match conn.table(name) {
            Ok(t) => t,
            Err(InfinoError::NotFound(_)) => return 0,
            Err(e) => {
                eprintln!("[capi] delete_osids: {}", short_reason(&e));
                return delete_code(&e);
            }
        };
        // A run of ids at a time ([`id_runs`]); a failed run fails the call,
        // which the caller tries again whole (a delete is idempotent).
        let gate = conn.gate(name);
        for run in id_runs(&ids, conn.id_list_bytes()) {
            let pred = col(OSID).in_list(run.iter().map(|s| lit(s.to_string())).collect(), false);
            let deleted = {
                let _pass = gate.whole();
                waiting(|| table.delete(pred.clone()))
            };
            if let Err(e) = deleted {
                eprintln!("[capi] delete_osids: {}", short_reason(&e));
                return delete_code(&e);
            }
        }
        0
    }))
    .unwrap_or(-99)
}

/// The columns a ranked search returns: a document's id, source and score,
/// or for an attached table every column it holds, which its hits are made
/// of ([`attached::hits_json`]).
fn ranked_columns(conn: &Bridge, name: &str) -> Vec<String> {
    const SCORE: &str = "score";
    match conn.attached_columns(name) {
        Some(columns) => columns
            .iter()
            .cloned()
            .chain([attached::ROW_ID.to_string(), SCORE.to_string()])
            .collect(),
        None => vec![OSID.to_string(), SOURCE.to_string(), SCORE.to_string()],
    }
}

/// One row as a get reads it: its source, the sequence number it was
/// written under (0 for a row written without one), and the routing value
/// it was written with ([`ROUTING`]; None for none).
struct Row {
    source: String,
    seq: i64,
    routing: Option<String>,
}

impl Row {
    /// The row as `{"source", "seq"}`, with `"routing"` when it has one.
    fn doc_json(self) -> Value {
        let mut doc = json!({ "source": self.source, "seq": self.seq });
        if let Some(routing) = self.routing {
            doc["routing"] = Value::String(routing);
        }
        doc
    }
}

/// The row of an attached table read under `osid`, its row id's digits, as
/// [`get_row`] gives a row: its columns as the source, and no sequence
/// number or routing. None when `osid` is no row id or the table holds no
/// such row; an error when the read fails.
fn attached_row(conn: &Bridge, name: &str, osid: &str) -> Result<Option<Row>, String> {
    if !attached::is_row_id(osid) {
        return Ok(None);
    }
    let sql = format!(
        "SELECT * FROM \"{}\" WHERE \"{}\" = {osid} LIMIT 1",
        name.replace('"', "\"\""),
        attached::ROW_ID
    );
    let batches = conn.conn.query_sql(&sql).map_err(|e| e.to_string())?;
    Ok(batches.iter().find(|b| b.num_rows() > 0).map(|b| Row {
        source: attached::source_json(b, 0),
        seq: 0,
        routing: None,
    }))
}

/// Most rows of an attached table one statement of [`attached_rows`] asks
/// for by id.
const ATTACHED_ROWS_PER_STATEMENT: usize = 500;

/// The rows of an attached table read under `osids`, each id to its source
/// as [`attached_row`] gives it; an id that is no row id, or names no row,
/// is absent. A page of a search reads its sources this way once its ids are
/// chosen: the table has no lookup by id but the row id itself, so the page
/// is read in one statement over its ids rather than one a row. An error
/// when a read fails.
fn attached_rows(
    conn: &Bridge,
    name: &str,
    osids: &[&str],
) -> Result<serde_json::Map<String, Value>, String> {
    let mut found = serde_json::Map::new();
    let ids: Vec<&str> = osids
        .iter()
        .copied()
        .filter(|id| attached::is_row_id(id))
        .collect();
    for chunk in ids.chunks(ATTACHED_ROWS_PER_STATEMENT) {
        let sql = format!(
            "SELECT * FROM \"{}\" WHERE \"{}\" IN ({})",
            name.replace('"', "\"\""),
            attached::ROW_ID,
            chunk.join(", ")
        );
        let batches = conn.conn.query_sql(&sql).map_err(|e| e.to_string())?;
        for batch in &batches {
            for r in 0..batch.num_rows() {
                if let Some(id) = attached::id_of(batch, r) {
                    found.insert(id, Value::String(attached::source_json(batch, r)));
                }
            }
        }
    }
    Ok(found)
}

/// The row under `osid` ([`Row`]), None when the table holds no such row,
/// or an error when the read fails.
fn get_row(conn: &Bridge, name: &str, osid: &str) -> Result<Option<Row>, InfinoError> {
    if conn.is_attached(name) {
        return attached_row(conn, name, osid).map_err(InfinoError::Query);
    }
    row_in(&conn.table(name)?, &conn.conn, name, osid)
}

/// [`get_row`] as of the table's latest commit, read through
/// [`Bridge::on_latest`]: for a write that merges into the document it
/// finds. A lookup that fails is an error rather than no row, so the write
/// fails instead of finding nothing to merge into.
fn get_row_latest(conn: &Bridge, name: &str, osid: &str) -> Result<Option<Row>, InfinoError> {
    if conn.is_attached(name) {
        return attached_row(conn, name, osid).map_err(InfinoError::Query);
    }
    if conn.latest_from.is_none() {
        // `conn` reads every commit already; its cached handle saves a
        // hosted service the describe each open asks of it.
        return row_in(&conn.table(name)?, &conn.conn, name, osid);
    }
    conn.on_latest(|latest| row_in(&latest.open_table(name)?, latest, name, osid))
}

/// The rows under `osids`, each id to its [`Row`] (an id the table does
/// not hold is absent), for a realtime get or multi-get: as of the table's
/// latest commit when `latest`, for a node that may not have read the last
/// commit another node made, else through `conn` itself. One open of the
/// table serves every id, whose lookups run side by side
/// ([`side_by_side`]). A lookup that fails is an error for the whole call,
/// so a failed read is never taken for a missing document.
fn rows_of(
    conn: &Bridge,
    name: &str,
    osids: &[&str],
    latest: bool,
) -> Result<Vec<(String, Row)>, InfinoError> {
    if conn.is_attached(name) {
        // An attached table is read in one statement over its ids, as a page is.
        return Ok(attached_rows(conn, name, osids)
            .map_err(InfinoError::Query)?
            .into_iter()
            .filter_map(|(id, source)| match source {
                Value::String(source) => Some((
                    id,
                    Row {
                        source,
                        seq: 0,
                        routing: None,
                    },
                )),
                _ => None,
            })
            .collect());
    }
    let found = |table: &Supertable, sql: &Connection| {
        side_by_side(osids, |id| row_in(table, sql, name, id), InfinoError::Query)
    };
    let rows = if !latest || conn.latest_from.is_none() {
        // As in [`get_row_latest`]: where `conn` reads every commit already,
        // its cached handle serves.
        found(&conn.table(name)?, &conn.conn)?
    } else {
        conn.on_latest(|latest| found(&latest.open_table(name)?, latest))?
    };
    Ok(osids
        .iter()
        .zip(rows)
        .filter_map(|(id, row)| row.map(|row| ((*id).to_owned(), row)))
        .collect())
}

/// `lookup` of each of `ids`, in their order, run at most
/// [`GET_MANY_THREADS`] at once: a page of lookups costs about what its
/// slowest one does. The first that fails fails the whole call.
fn side_by_side<T: Send, E: Send>(
    ids: &[&str],
    lookup: impl Fn(&str) -> Result<T, E> + Sync,
    panicked: fn(String) -> E,
) -> Result<Vec<T>, E> {
    let mut out = Vec::with_capacity(ids.len());
    for chunk in ids.chunks(GET_MANY_THREADS) {
        let results: Vec<Result<T, E>> = std::thread::scope(|scope| {
            let lookup = &lookup;
            let running: Vec<_> = chunk
                .iter()
                .map(|id| scope.spawn(move || lookup(id)))
                .collect();
            running
                .into_iter()
                .map(|r| {
                    r.join()
                        .unwrap_or_else(|_| Err(panicked("a lookup panicked".to_string())))
                })
                .collect()
        });
        for result in results {
            out.push(result?);
        }
    }
    Ok(out)
}

/// The row under `osid` in `table` ([`Row`]), which `sql` (the connection
/// it was opened on) holds; None when it holds no such row.
fn row_in(
    table: &Supertable,
    sql: &Connection,
    name: &str,
    osid: &str,
) -> Result<Option<Row>, InfinoError> {
    // A term lookup through the id column's FTS index, projected to the
    // source, the sequence number and, where the table has taken a routed
    // write, the routing. A table declared before the id column carried that
    // index refuses the lookup; it falls back to an equality on the column.
    let routed = table.schema().field_with_name(ROUTING).is_ok();
    let columns: &[&str] = if routed {
        &[SOURCE, SEQ, ROUTING]
    } else {
        &[SOURCE, SEQ]
    };
    let batches = match table.exact_match(OSID, osid, Some(columns)) {
        Ok(batches) => batches,
        Err(_) => sql.query_sql(&row_by_osid_sql(name, osid, columns))?,
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
        let routing = b
            .column_by_name(ROUTING)
            .filter(|c| !c.is_null(0))
            .and_then(|c| match c.data_type() {
                DataType::LargeUtf8 => c
                    .as_any()
                    .downcast_ref::<LargeStringArray>()
                    .map(|a| a.value(0).to_string()),
                DataType::Utf8 => c
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .map(|a| a.value(0).to_string()),
                _ => None,
            });
        return Ok(Some(Row {
            source: source.value(0).to_string(),
            seq,
            routing,
        }));
    }
    Ok(None)
}

/// The statement [`row_in`] falls back to: `columns` of the row under
/// `osid`, by an equality on the id column. The table name is quoted as
/// every other statement over a table quotes it, so an index whose name
/// holds a `-` reads as one name.
fn row_by_osid_sql(name: &str, osid: &str, columns: &[&str]) -> String {
    format!(
        "SELECT {} FROM \"{}\" WHERE {OSID} = '{}' LIMIT 1",
        columns.join(", "),
        name.replace('"', "\"\""),
        osid.replace('\'', "''")
    )
}

/// The raw `_source` JSON of one osid, as `{"source": ..}`, null when the
/// table holds no such id, or `{"error":..}` when the lookup fails. A failed
/// read is never answered as absent: a write would take the document as
/// missing and create or overwrite it. The source sits one level down so
/// that no document, one whose first field is `error` included, can make
/// the answer read as an error.
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
        match get_row(conn, name, osid) {
            Ok(row) => out_json(json!({ "source": row.map(|row| row.source) })),
            Err(e) => read_err_json("get", osid, &e),
        }
    }))
    .unwrap_or(ptr::null_mut())
}

/// One osid's raw `_source` JSON as of the table's latest commit
/// ([`get_row_latest`]), "" when the table holds no such id: for a write
/// that merges into the document it finds, when the last node to write the
/// table may be another. A lookup that fails answers null rather than "":
/// a source is any JSON object, so no error object can stand for the
/// failure.
#[no_mangle]
pub unsafe extern "C" fn infino_get_latest(
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
        match get_row_latest(conn, name, osid) {
            Ok(row) => out_string(row.map_or_else(String::new, |row| row.source)),
            Err(e) => {
                eprintln!("[capi] get_latest: {e}");
                ptr::null_mut()
            }
        }
    }))
    .unwrap_or(ptr::null_mut())
}

/// Most lookups [`infino_get_many`] runs at once.
const GET_MANY_THREADS: usize = 16;

/// The raw `_source` JSON of several osids, as `{"sources": {..}}`, an
/// object from each id the table holds to its source; an id it does not hold
/// is absent, and a lookup that fails answers `{"error":..}` for the whole
/// call. The ids sit one level down so that no id, `error` included, can
/// make the answer read as an error.
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
        let ids = unsafe { cstrs(osids, count) };
        if conn.is_attached(name) {
            return match attached_rows(conn, name, &ids) {
                Ok(found) => out_json(json!({ "sources": found })),
                Err(e) => err_json(&format!("get_many: {e}")),
            };
        }
        // A failure keeps the id apart from the engine's message, so the id
        // names itself in the error but cannot set its status.
        let rows = side_by_side(
            &ids,
            |id| get_row(conn, name, id).map_err(|e| (id.to_owned(), e)),
            |e| (String::new(), InfinoError::Backend(e)),
        );
        match rows {
            Ok(rows) => {
                let mut found = serde_json::Map::new();
                for (id, row) in ids.iter().zip(rows) {
                    if let Some(row) = row.filter(|row| !row.source.is_empty()) {
                        found.insert((*id).to_owned(), Value::String(row.source));
                    }
                }
                out_json(json!({ "sources": found }))
            }
            Err((id, e)) => read_err_json("get_many", &id, &e),
        }
    }))
    .unwrap_or(ptr::null_mut())
}

/// One osid's document as `{"doc": {"source": .., "seq": n, "routing": ..}}`,
/// its sequence number what a conditional write on it is checked against and
/// `routing` (left out for none) what a get reports as `_routing`;
/// `{"doc": null}` when the table holds no such id, and `{"error":..}` when
/// the lookup fails, as [`infino_get`] answers.
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
            Ok(row) => out_json(json!({ "doc": row.map(Row::doc_json) })),
            Err(e) => read_err_json("get_doc", osid, &e),
        }
    }))
    .unwrap_or(ptr::null_mut())
}

/// The documents of several osids ([`rows_of`]), as `{"docs": {id: doc}}`,
/// each doc `{"source", "seq", "routing"}` as [`infino_get_doc`] gives it;
/// an id the table does not hold is absent. For a realtime multi-get on the
/// node whose own connection has read every commit: one open of the table
/// for all the ids. A lookup that fails answers `{"error":..}` for the whole
/// call, as [`infino_get`] answers, never an answer missing ids, so a failed
/// read is never taken for a missing document. The ids sit one level down,
/// so no id, `error` included, can make the answer read as an error.
#[no_mangle]
pub unsafe extern "C" fn infino_get_docs(
    h: *mut Bridge,
    name: *const c_char,
    osids: *const *const c_char,
    count: c_int,
) -> *mut c_char {
    unsafe { get_docs(h, name, osids, count, false) }
}

/// [`infino_get_docs`] as of the table's latest commit: for a realtime get
/// or multi-get on a node that may not have read the last commit another
/// node made, one read of the latest commit for all the ids.
#[no_mangle]
pub unsafe extern "C" fn infino_get_docs_latest(
    h: *mut Bridge,
    name: *const c_char,
    osids: *const *const c_char,
    count: c_int,
) -> *mut c_char {
    unsafe { get_docs(h, name, osids, count, true) }
}

/// [`infino_get_docs`], as of the table's latest commit when `latest`.
unsafe fn get_docs(
    h: *mut Bridge,
    name: *const c_char,
    osids: *const *const c_char,
    count: c_int,
    latest: bool,
) -> *mut c_char {
    catch_unwind(AssertUnwindSafe(|| {
        let (Some(conn), Some(name)) = (bridge_ref(h), unsafe { cstr(name) }) else {
            return ptr::null_mut();
        };
        let ids = unsafe { cstrs(osids, count) };
        match rows_of(conn, name, &ids, latest) {
            Ok(rows) => {
                let docs: serde_json::Map<String, Value> = rows
                    .into_iter()
                    .map(|(id, row)| (id, row.doc_json()))
                    .collect();
                out_json(json!({ "docs": docs }))
            }
            Err(e) => {
                let call = if latest {
                    "get_docs_latest"
                } else {
                    "get_docs"
                };
                read_err_json(call, name, &e)
            }
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
        if let Err(refused) = conn.refuse_write(name) {
            eprintln!("[capi] drop_table: {refused}");
            return -2;
        }
        conn.forget(name);
        let dropped = match conn.conn.drop_table(name, true) {
            Ok(_) => 0,
            Err(InfinoError::NotFound(_)) => 0,
            Err(e) => {
                eprintln!("[capi] drop_table: {e}");
                -2
            }
        };
        conn.forget_latest();
        dropped
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
        // An attached table is its maker's to compact.
        if conn.is_attached(name) {
            return 0;
        }
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
        match conn.query_viewed(sql) {
            Ok(b) => out_string(batches_to_json(&b)),
            Err(e) => err_json(&e.to_string()),
        }
    }))
    .unwrap_or(ptr::null_mut())
}

/// Run `sql` as [`infino_sql`] does, reading each table's latest commit
/// ([`Bridge::latest`]): for a read whose answer decides what a write does.
#[no_mangle]
pub unsafe extern "C" fn infino_sql_latest(h: *mut Bridge, sql: *const c_char) -> *mut c_char {
    catch_unwind(AssertUnwindSafe(|| {
        let (Some(bridge), Some(sql)) = (bridge_ref(h), unsafe { cstr(sql) }) else {
            return ptr::null_mut();
        };
        match bridge.query_latest(sql) {
            Ok(b) => out_string(batches_to_json(&b)),
            Err(e) => err_json(&e.to_string()),
        }
    }))
    .unwrap_or(ptr::null_mut())
}

/// Keep the connection [`infino_sql_latest`] and [`infino_get_latest`] read
/// through open across calls until [`infino_release_latest`]
/// ([`Bridge::hold_latest`]): for a shard that reads the latest commit for
/// each batch it writes until its own first commit. Without a hold, the
/// connection closes after each such read.
#[no_mangle]
pub unsafe extern "C" fn infino_hold_latest(h: *mut Bridge) {
    if let Some(bridge) = bridge_ref(h) {
        bridge.hold_latest();
    }
}

/// Let go of a hold [`infino_hold_latest`] took ([`Bridge::release_latest`]).
#[no_mangle]
pub unsafe extern "C" fn infino_release_latest(h: *mut Bridge) {
    if let Some(bridge) = bridge_ref(h) {
        bridge.release_latest();
    }
}

/// Close the connection the latest reads go through, holds or not
/// ([`Bridge::forget_latest`]), so the next latest read opens it afresh
/// with no table handle: for a holder that keeps the connection open while
/// reads keep coming, to let go of the handles of tables no longer read.
/// A read running on it keeps what it holds until it ends.
#[no_mangle]
pub unsafe extern "C" fn infino_forget_latest(h: *mut Bridge) {
    if let Some(bridge) = bridge_ref(h) {
        bridge.forget_latest();
    }
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
        // An index attached to a table reads it through the table's view,
        // as every other statement over it does: the route's own views name
        // a document's id and source, which only that view gives.
        match conn.query_viewed(&statement.sql) {
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
        let columns = ranked_columns(conn, name);
        let projection: Vec<&str> = columns.iter().map(String::as_str).collect();
        match table.bm25_search(
            column,
            query,
            k.max(0) as usize,
            Bm25SearchOptions::new().with_mode(mode),
            Some(&projection),
        ) {
            Ok(b) if conn.is_attached(name) => out_string(attached::hits_json(&b, true)),
            Ok(b) => out_string(batches_to_json(&b)),
            Err(e) => err_json(&e.to_string()),
        }
    }))
    .unwrap_or(ptr::null_mut())
}

/// The table's first `k` rows by `column`, ascending or (`descending`
/// non-zero) descending, rows sharing a value in row id order and rows
/// without one last, each carrying `osid`, `_id`, the column and, when
/// `with_source` is non-zero, `source`: the page a search that keeps every
/// row asks for when it sorts on one field, in one call. The sources are
/// read by row id along with the page, not looked up by `osid` afterwards.
/// The engine reads it from the column's range index with no statement
/// planned. `null` when the column has no range index: the caller plans its
/// own statement then.
#[no_mangle]
pub unsafe extern "C" fn infino_sorted(
    h: *mut Bridge,
    name: *const c_char,
    column: *const c_char,
    descending: c_int,
    k: c_int,
    with_source: c_int,
) -> *mut c_char {
    catch_unwind(AssertUnwindSafe(|| {
        let (Some(conn), Some(name), Some(column)) =
            (bridge_ref(h), unsafe { cstr(name) }, unsafe { cstr(column) })
        else {
            return ptr::null_mut();
        };
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
        let with = [OSID, ROW_ID, column, SOURCE];
        let projection = if with_source != 0 { &with[..] } else { &with[..3] };
        match table.sorted_search(column, descending != 0, k.max(0) as usize, Some(projection)) {
            // The engine fixes every row's `score` at zero; a sorted hit has none.
            Ok(b) => out_string(batches_to_json(&without_column(&b, "score"))),
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
        // An attached table is counted by statements, each predicate under
        // its own WHERE, which lets the scan skip the files the predicate
        // rules out. The engine's count leans on the range index the plugin
        // gives its own tables' numbers and dates; a table made elsewhere
        // has none, and on the hosted service that count is one aggregate
        // with the predicate in a FILTER clause, read over every row: an
        // hour of a trillion-row table was counted by reading all of it.
        if conn.is_attached(name) {
            return count_where_by_sql(conn, name, &predicates);
        }
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
            return count_where_by_sql(conn, name, &predicates);
        };
        if exprs.iter().any(holds_subquery) {
            return count_where_by_sql(conn, name, &predicates);
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
fn count_where_by_sql(conn: &Bridge, name: &str, predicates: &[String]) -> *mut c_char {
    let table = name.replace('"', "\"\"");
    let mut counts = Vec::with_capacity(predicates.len());
    for p in predicates {
        let sql = format!("SELECT COUNT(*) AS c FROM \"{table}\" WHERE {p}");
        // A predicate over an attached table reads a document's id, which
        // only the table's view gives.
        let batches = match conn.query_viewed(&sql) {
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
        let max = max_rows.max(0) as usize;
        match table.value_counts(column) {
            Ok(b) if b.num_rows() > max => out_string("null".to_string()),
            Ok(b) => out_string(batches_to_json(&[b])),
            // Where the engine's own count fails (the hosted service's does,
            // on a count it declares non-null and gives null), the grouped
            // count as a statement gives the same rows.
            Err(e) => match conn.conn.query_sql(&grouped_count(name, column, Some(max))) {
                Ok(b) if b.iter().map(RecordBatch::num_rows).sum::<usize>() > max => {
                    out_string("null".to_string())
                }
                Ok(b) => out_string(batches_to_json(&b)),
                Err(_) => err_json(&e.to_string()),
            },
        }
    }))
    .unwrap_or(ptr::null_mut())
}

/// The `top` most frequent values of a keyword column over the whole table,
/// by count and then by value (its UTF-8 bytes), with what a terms
/// aggregation needs to count the rest: `{"rows":[{column, "count(*)"}...],
/// "keyed_total":n, "nulls":n, "groups":n}`, where `keyed_total` is the rows
/// with a value, `nulls` the rows without one and `groups` how many values
/// the column has. One call: the engine reads a keyword column's counts from
/// its term index, so only `top` rows cross, however many values it has.
/// `null`, for the caller's own statement to run, for a column without the
/// `keyword` analyzer (the engine would run the grouped count there, which
/// the statement runs as well; every column on a hosted connection, see
/// [`Bridge::keyword_analyzer`]), and where the engine's count fails. The
/// engine counts every value in memory before the top `n` are picked;
/// bounding that is infino-ai/infino#1459.
#[no_mangle]
pub unsafe extern "C" fn infino_value_counts_top(
    h: *mut Bridge,
    name: *const c_char,
    column: *const c_char,
    top: c_int,
) -> *mut c_char {
    catch_unwind(AssertUnwindSafe(|| {
        let (Some(conn), Some(name), Some(column)) =
            (bridge_ref(h), unsafe { cstr(name) }, unsafe {
                cstr(column)
            })
        else {
            return ptr::null_mut();
        };
        match value_counts_top(conn, name, column, top.max(0) as usize) {
            Ok(Some(out)) => out_string(out.to_string()),
            Ok(None) => out_string("null".to_string()),
            Err(e) => err_json(&e),
        }
    }))
    .unwrap_or(ptr::null_mut())
}

/// [`infino_value_counts_top`]'s answer, None for a column without the
/// `keyword` analyzer or where the engine's count fails.
fn value_counts_top(
    conn: &Bridge,
    name: &str,
    column: &str,
    top: usize,
) -> Result<Option<Value>, String> {
    // No column over this connection has the analyzer: decline without
    // asking the hosted service for the table's schema.
    if !conn.keyword_analyzer() {
        return Ok(None);
    }
    let keyword = conn
        .conn
        .schema(name)
        .map_err(|e| e.to_string())?
        .fields()
        .iter()
        .any(|f| {
            f.name == column
                && matches!(
                    f.index,
                    Some(ColumnIndex::Fts {
                        analyzer: Analyzer::Keyword,
                        ..
                    })
                )
        });
    if !keyword {
        return Ok(None);
    }
    let table = conn.table(name).map_err(|e| e.to_string())?;
    // Where the engine's own count fails, decline: the caller's bounded
    // statement gives the same answer, where a grouped count here would
    // bring every value of the column across the bridge.
    match table.value_counts(column) {
        Ok(b) => Ok(top_values(&[b], column, top)),
        Err(_) => Ok(None),
    }
}

/// [`infino_value_counts_top`]'s answer from a column's value counts
/// (`batches`, each the value then its count). None when the values are not
/// strings or the counts not integers.
fn top_values(batches: &[RecordBatch], column: &str, top: usize) -> Option<Value> {
    let is_text =
        |t: &DataType| matches!(t, DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View);
    let mut values: Vec<LargeStringArray> = Vec::with_capacity(batches.len());
    let mut counts: Vec<Int64Array> = Vec::with_capacity(batches.len());
    for b in batches {
        if b.num_columns() < 2 {
            return None;
        }
        let text = match b.column(0).data_type() {
            DataType::Dictionary(_, v) => is_text(v),
            t => is_text(t),
        };
        if !text {
            return None;
        }
        let v = datafusion::arrow::compute::cast(b.column(0), &DataType::LargeUtf8).ok()?;
        let n = datafusion::arrow::compute::cast(b.column(1), &DataType::Int64).ok()?;
        values.push(v.as_any().downcast_ref::<LargeStringArray>()?.clone());
        counts.push(n.as_any().downcast_ref::<Int64Array>()?.clone());
    }
    let groups: usize = values.iter().map(|v| v.len() - v.null_count()).sum();
    let (mut nulls, mut keyed_total) = (0i64, 0i64);
    let mut keyed: Vec<(i64, &str)> = Vec::with_capacity(groups);
    for (v, n) in values.iter().zip(&counts) {
        for i in 0..v.len() {
            let count = if n.is_null(i) { 0 } else { n.value(i) };
            if v.is_null(i) {
                nulls += count;
            } else {
                keyed_total += count;
                keyed.push((count, v.value(i)));
            }
        }
    }
    let top = top.min(groups);
    // Count descending, then the value's bytes ascending: a terms
    // aggregation's default order, as the plugin's window statement ranks.
    let order = |a: &(i64, &str), b: &(i64, &str)| b.0.cmp(&a.0).then_with(|| a.1.cmp(b.1));
    if top > 0 && top < groups {
        keyed.select_nth_unstable_by(top - 1, order);
    }
    keyed.truncate(top);
    keyed.sort_unstable_by(order);
    let rows: Vec<Value> = keyed
        .into_iter()
        .map(|(n, v)| json!({ column: v, "count(*)": n }))
        .collect();
    Some(json!({ "rows": rows, "keyed_total": keyed_total, "nulls": nulls, "groups": groups }))
}

/// The grouped count [`infino_value_counts`] gives, as a statement: each
/// value of `column` in `name` and its rows, under the names the engine's
/// own count gives them. With a `max`, it stops one group past it, which is
/// all the caller needs to know the column holds too many values to carry.
fn grouped_count(name: &str, column: &str, max: Option<usize>) -> String {
    let quote = |s: &str| format!("\"{}\"", s.replace('"', "\"\""));
    let (table, column) = (quote(name), quote(column));
    let limit = max.map_or(String::new(), |max| {
        format!(" LIMIT {}", max.saturating_add(1))
    });
    format!("SELECT {column}, count(*) AS \"count(*)\" FROM {table} GROUP BY {column}{limit}")
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
        // An attached table's rows are read under their row ids.
        let attached = conn.is_attached(name);
        let id: &[&str] = if attached {
            &[attached::ROW_ID]
        } else {
            &[OSID]
        };
        match table.token_match(column, query, BoolMode::Or, Some(id)) {
            Ok(b) if attached => out_string(attached::hits_json(&b, false)),
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
        let columns = ranked_columns(conn, name);
        let projection: Vec<&str> = columns.iter().map(String::as_str).collect();
        match table.vector_search(column, &emb, k.max(0) as usize, None, Some(&projection)) {
            Ok(b) if conn.is_attached(name) => out_string(attached::hits_json(&b, true)),
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
        let columns = ranked_columns(conn, name);
        let projection: Vec<&str> = columns.iter().map(String::as_str).collect();
        match table.hybrid_search(
            tcol,
            tq,
            BoolMode::Or,
            vcol,
            &emb,
            k.max(0) as usize,
            Some(&projection),
        ) {
            Ok(b) if conn.is_attached(name) => out_string(attached::hits_json(&b, true)),
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
                // An attached table's view gives a document's source as a
                // struct of the table's columns: handed over as the JSON
                // text a source is, as the plugin's own column holds it.
                let source = (field.name() == SOURCE)
                    .then(|| batch.column(c).as_any().downcast_ref::<StructArray>())
                    .flatten();
                match source {
                    Some(source) if source.is_null(r) => out.push_str("null"),
                    Some(source) => out.push_str(&json_str(&attached::object_json(source, r))),
                    None => out.push_str(&cell_json(batch.column(c).as_ref(), r)),
                }
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
    // The types only a table made outside the plugin holds.
    if let Some(value) = attached::narrow_cell_json(col, r) {
        return value;
    }
    "\"<unsupported>\"".into()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::env;
    use std::io::{Read, Write};
    use std::net::{TcpListener, TcpStream};
    use std::os::unix::fs::PermissionsExt;
    use std::thread;

    use datafusion::arrow::ipc::writer::StreamWriter;

    /// A directory its owner may read and enter but not write.
    const READ_ONLY_DIR: u32 = 0o555;
    /// A directory its owner may read, enter and write.
    const WRITABLE_DIR: u32 = 0o755;
    /// How long a test waits for a service that may never answer.
    const SHORT_WAIT: Duration = Duration::from_millis(300);

    /// How many catalogs [`open_logs`] has made in this process: part of
    /// each one's directory name, since tests running at once can read the
    /// same clock time.
    static OPENED_LOGS: AtomicUsize = AtomicUsize::new(0);

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
        let h = open_storage();
        let out = ensure(h, MAPPING);
        assert!(out.contains("\"created\":true"), "{out}");
        h
    }

    /// A connection over a fresh directory, with no table declared yet.
    fn open_storage() -> *mut Bridge {
        let dir = std::env::temp_dir().join(format!(
            "infino-capi-test-{}-{}-{}",
            std::process::id(),
            OPENED_LOGS.fetch_add(1, Ordering::SeqCst),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0)
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let uri = format!("file://{}", dir.display());
        let h = unsafe { infino_open(c(&uri).as_ptr()) };
        assert!(!h.is_null());
        h
    }

    /// On a hosted connection the schema document a batch is prepared
    /// against is kept with the table's handle, so preparing batch after
    /// batch asks the service nothing; a declaration that changes the table
    /// drops it, and the next batch reads the new one.
    #[test]
    fn a_hosted_tables_schema_is_kept_until_a_declaration_changes_it() {
        let h = open_logs();
        let names = |s: &TableSchema| s.fields().iter().map(|f| f.name.clone()).collect::<Vec<_>>();
        // Storage answers the same document a hosted service would; the flag
        // is what makes the bridge keep it.
        unsafe { (*h).hosted = true };
        let bridge = unsafe { &*h };
        let before = names(&bridge.table_schema("logs").unwrap());
        // A column added behind the bridge is not seen: the document is kept.
        let behind = SchemaPatch::new(vec![FieldPatch::named("behind".to_string())
            .with_type(DataType::Int64)
            .with_nullable(true)]);
        let table = bridge.conn.open_table("logs").unwrap();
        table.apply_schema(&behind, None).unwrap();
        assert_eq!(names(&bridge.table_schema("logs").unwrap()), before);
        // A declaration that adds a column drops it.
        let out = ensure(h, r#"{"properties":{"added":{"type":"long"}}}"#);
        assert!(!out.contains("error"), "{out}");
        let after = names(&bridge.table_schema("logs").unwrap());
        assert!(after.contains(&"added".to_string()), "{after:?}");
        assert!(after.contains(&"behind".to_string()), "{after:?}");
    }

    /// The names of the columns [`infino_columns`] reports for `logs`.
    fn column_names(h: *mut Bridge) -> Vec<String> {
        columns(h)
            .as_array()
            .unwrap()
            .iter()
            .filter_map(|col| col["name"].as_str().map(str::to_owned))
            .collect()
    }

    /// The names of `schema`'s fields.
    fn field_names(schema: &TableSchema) -> Vec<String> {
        schema.fields().iter().map(|f| f.name.clone()).collect()
    }

    /// Add a nullable long column `column` to `logs` behind the bridge, as
    /// another node's declaration would: a hosted bridge's kept schema
    /// document lacks it until the bridge reads the document again, so
    /// whether a read sees it tells whether that read asked the service.
    fn add_column_behind(h: *mut Bridge, column: &str) {
        let patch = SchemaPatch::new(vec![FieldPatch::named(column.to_string())
            .with_type(DataType::Int64)
            .with_nullable(true)]);
        let table = unsafe { &*h }.conn.open_table("logs").unwrap();
        table.apply_schema(&patch, None).unwrap();
    }

    /// The engine adds columns during an append as well as during a
    /// declaration: a geo_point's first values, a flat_object's members, a
    /// path the mapping does not declare. On a hosted table the kept schema
    /// document is read again after an append that added one, so the
    /// table's columns hold it right away, as they do over storage; an
    /// append that adds none keeps the document, so staging the next batch
    /// still asks the service nothing.
    #[test]
    fn a_column_an_append_adds_is_among_the_columns_right_after_it() {
        for hosted in [false, true] {
            let h = open_logs();
            unsafe { (*h).hosted = hosted };
            let out = ensure(h, r#"{"properties":{"fo":{"type":"flat_object"}}}"#);
            assert!(!out.contains("error"), "{out}");
            let doc = r#"{"level":"info","geo":[1.0,2.0],"fo":{"a":1},"un":7}"#;
            let p = prepared(h, &[("y", doc)]);
            assert_eq!(append_replacing(h, p, &[])["ok"], 1);
            let cols = column_names(h);
            for added in ["geo", "fo.a", "un"] {
                assert!(
                    cols.contains(&added.to_string()),
                    "hosted = {hosted}: {added} missing from {cols:?}"
                );
            }
            if hosted {
                // A column added behind the bridge stays unseen through an
                // append that adds none: that append kept the document.
                add_column_behind(h, "behind");
                let p = prepared(h, &[("z", r#"{"level":"warn","un":8}"#)]);
                assert_eq!(append_replacing(h, p, &[])["ok"], 1);
                let cols = column_names(h);
                assert!(!cols.contains(&"behind".to_string()), "{cols:?}");
            }
            close(h);
        }
    }

    /// The engine adds no column for a path that holds only `null`, `[]` or
    /// a list of nulls, so an append of such a document adds none either,
    /// and keeps the hosted table's schema document: were it read again,
    /// every later write of the document would read it again too, since
    /// nothing ever gives the path a column.
    #[test]
    fn a_path_holding_only_nulls_or_empty_lists_keeps_the_schema_document() {
        for doc in [
            r#"{"level":"info","nul":null}"#,
            r#"{"level":"info","emp":[]}"#,
            r#"{"level":"info","nuls":[null,null]}"#,
        ] {
            let h = open_logs();
            unsafe { (*h).hosted = true };
            let p = prepared(h, &[("a", doc)]);
            assert_eq!(append_replacing(h, p, &[])["ok"], 1, "{doc}");
            add_column_behind(h, "behind");
            let p = prepared(h, &[("b", doc)]);
            assert!(!unsafe { &*p }.adds_columns, "{doc}");
            assert_eq!(append_replacing(h, p, &[])["ok"], 1, "{doc}");
            let cols = column_names(h);
            assert!(!cols.contains(&"behind".to_string()), "{doc}: {cols:?}");
            close(h);
        }
    }

    /// A read of a hosted table's schema document that begins before a
    /// declaration changes the table and ends after the declaration's last
    /// forget returns what it read, but keeps nothing: the next read asks
    /// again and finds the declared column.
    #[test]
    fn a_schema_read_a_declaration_overtakes_keeps_nothing() {
        let h = open_logs();
        unsafe { (*h).hosted = true };
        let bridge = unsafe { &*h };
        let read = bridge
            .kept_or_read("logs", &bridge.schemas, || {
                let before = bridge.conn.schema("logs");
                // The declaration runs wholly between the read and its keep.
                let out = ensure(h, r#"{"properties":{"added":{"type":"long"}}}"#);
                assert!(out.contains("\"added\":1"), "{out}");
                before
            })
            .unwrap();
        assert!(!field_names(&read).contains(&"added".to_string()));
        let now = field_names(&bridge.table_schema("logs").unwrap());
        assert!(now.contains(&"added".to_string()), "{now:?}");
        close(h);
    }

    /// Re-reading a hosted table's schema document after a write leaves the
    /// old document kept while the read runs and if it fails: a stalled
    /// service must not leave a thread that stages a bulk with nothing kept,
    /// asking the service itself. Once a read succeeds, its document is
    /// swapped in.
    #[test]
    fn a_failed_schema_reread_keeps_the_old_document() {
        let h = open_logs();
        unsafe { (*h).hosted = true };
        let bridge = unsafe { &*h };
        bridge.table_schema("logs").unwrap();
        add_column_behind(h, "behind");
        let failed = bridge.replace_schema("logs", || {
            // The old document is still what staging reads mid-read.
            let mid = field_names(&bridge.table_schema("logs").unwrap());
            assert!(!mid.contains(&"behind".to_string()), "{mid:?}");
            Err(InfinoError::Backend("the service did not answer".into()))
        });
        assert!(failed.is_err());
        // Staging still reads the kept document, not the service.
        let p = prepared(h, &[("a", r#"{"level":"info"}"#)]);
        unsafe { infino_free_prepared(p) };
        let kept = field_names(&bridge.table_schema("logs").unwrap());
        assert!(!kept.contains(&"behind".to_string()), "{kept:?}");
        bridge
            .replace_schema("logs", || bridge.conn.schema("logs"))
            .unwrap();
        let now = field_names(&bridge.table_schema("logs").unwrap());
        assert!(now.contains(&"behind".to_string()), "{now:?}");
        close(h);
    }

    /// A read on a miss that a re-read overtakes keeps nothing, so it cannot
    /// put back a document older than the one the re-read swapped in.
    #[test]
    fn a_schema_read_a_reread_overtakes_keeps_nothing() {
        let h = open_logs();
        unsafe { (*h).hosted = true };
        let bridge = unsafe { &*h };
        let read = bridge
            .kept_or_read("logs", &bridge.schemas, || {
                let before = bridge.conn.schema("logs");
                add_column_behind(h, "behind");
                bridge
                    .replace_schema("logs", || bridge.conn.schema("logs"))
                    .unwrap();
                before
            })
            .unwrap();
        assert!(!field_names(&read).contains(&"behind".to_string()));
        let now = field_names(&bridge.table_schema("logs").unwrap());
        assert!(now.contains(&"behind".to_string()), "{now:?}");
        close(h);
    }

    /// An array of arrays at a path without a column (a geo_point given
    /// several points, a flat_object's member, a field not yet declared) is
    /// refused by the check that runs before a write is acknowledged, as the
    /// commit would refuse it, and still once an earlier document has given
    /// the path the list column the engine types from its values; one at a
    /// declared field is read through, and a mixed array at a path without a
    /// column is no refusal.
    #[test]
    fn an_array_of_arrays_without_a_column_is_refused_before_acknowledgement() {
        let h = open_logs();
        let out = ensure(h, r#"{"properties":{"fo":{"type":"flat_object"}}}"#);
        assert!(!out.contains("error"), "{out}");
        let refused = [
            (r#"{"level":"info","geo":[[1.0,2.0],[3.0,4.0]]}"#, "geo"),
            (r#"{"level":"info","fo":{"a":[[1,2],[3]]}}"#, "fo.a"),
            (r#"{"level":"info","un":[{"x":[1]},{"x":[[2]]}]}"#, "un.x"),
            (r#"{"level":"info","un":[null,[1]]}"#, "un"),
        ];
        let refuses = |doc: &str, path: &str| {
            let why = check(h, doc);
            assert!(why.contains(&format!("field [{path}]")), "{doc}: {why:?}");
            assert!(why.contains("array of arrays"), "{doc}: {why:?}");
            let out = index(h, &[("x", doc)]);
            assert_eq!(out["ok"], 0, "{doc}: {out}");
        };
        for (doc, path) in refused {
            refuses(doc, path);
        }
        for doc in [
            r#"{"level":"info","status":[[1,2],[3]]}"#,
            r#"{"level":"info","un":[1,"a"]}"#,
            r#"{"level":"info","geo":[1.0,2.0]}"#,
            r#"{"level":"info","fo":{"a":[1,2]}}"#,
        ] {
            assert_eq!(check(h, doc), "", "{doc}");
            let out = index(h, &[("y", doc)]);
            assert_eq!(out["ok"], 1, "{doc}: {out}");
        }
        // Those writes gave geo and fo.a list columns typed from their values.
        let cols = columns(h);
        for path in ["geo", "fo.a"] {
            assert!(
                cols.as_array()
                    .unwrap()
                    .iter()
                    .any(|cc| cc["name"] == path && cc["type"] == "list"),
                "{path}: {cols}"
            );
        }
        for (doc, path) in &refused[..2] {
            refuses(doc, path);
        }
        close(h);
    }

    /// A table that cannot be opened is no verdict on the document: the
    /// check answers an error object, which the plugin fails the write with
    /// (one the client may try again), not a reason the document is refused.
    #[test]
    fn a_table_that_cannot_be_opened_is_an_error_not_a_refusal() {
        let h = open_logs();
        let out = take(unsafe {
            infino_check_doc(
                h,
                c("no_such_table").as_ptr(),
                c(r#"{"level":"info"}"#).as_ptr(),
                ptr::null(),
            )
        });
        let v: Value = serde_json::from_str(&out).unwrap_or_else(|_| panic!("{out}"));
        assert!(v["error"].is_string(), "{out}");
        close(h);
    }

    /// [`infino_get_docs_latest`] of `ids` in table `logs`.
    fn docs_latest(h: *mut Bridge, ids: &[&str]) -> String {
        docs_from(h, "logs", ids, true)
    }

    /// [`infino_get_docs`] of `ids` in `table`, or [`infino_get_docs_latest`]
    /// when `latest`.
    fn docs_from(h: *mut Bridge, table: &str, ids: &[&str], latest: bool) -> String {
        let ids: Vec<CString> = ids.iter().map(|id| c(id)).collect();
        let ptrs: Vec<*const c_char> = ids.iter().map(|id| id.as_ptr()).collect();
        let (table, count) = (c(table), ptrs.len() as c_int);
        take(unsafe {
            if latest {
                infino_get_docs_latest(h, table.as_ptr(), ptrs.as_ptr(), count)
            } else {
                infino_get_docs(h, table.as_ptr(), ptrs.as_ptr(), count)
            }
        })
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
        take(unsafe { infino_check_doc(h, c("logs").as_ptr(), c(doc).as_ptr(), ptr::null()) })
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
        get_from(h, "logs", osid)
    }

    /// One id's source from `table` as `infino_get` answers it, "" when the
    /// table holds no such id; a lookup that fails panics.
    fn get_from(h: *mut Bridge, table: &str, osid: &str) -> String {
        let out = take(unsafe { infino_get(h, c(table).as_ptr(), c(osid).as_ptr()) });
        let v: Value = serde_json::from_str(&out).unwrap_or_else(|_| panic!("{out}"));
        assert!(v.get("source").is_some(), "{out}");
        v["source"].as_str().unwrap_or_default().to_string()
    }

    fn get_many(h: *mut Bridge, osids: &[&str]) -> Value {
        let ids: Vec<CString> = osids.iter().map(|x| c(x)).collect();
        let idp: Vec<*const c_char> = ids.iter().map(|x| x.as_ptr()).collect();
        let out = take(unsafe {
            infino_get_many(h, c("logs").as_ptr(), idp.as_ptr(), osids.len() as c_int)
        });
        let v: Value = serde_json::from_str(&out).unwrap_or_else(|_| panic!("{out}"));
        assert!(v["sources"].is_object(), "{out}");
        v["sources"].clone()
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

    fn value_counts_top(h: *mut Bridge, column: &str, top: c_int) -> Value {
        let out = take(unsafe {
            infino_value_counts_top(h, c("logs").as_ptr(), c(column).as_ptr(), top)
        });
        serde_json::from_str(&out).unwrap_or_else(|_| panic!("{out}"))
    }

    /// `(value, count)` pairs of a top answer's rows, in its order.
    fn top_rows(out: &Value) -> Vec<(String, i64)> {
        out["rows"]
            .as_array()
            .unwrap()
            .iter()
            .map(|r| {
                (
                    r["level"].as_str().unwrap().to_string(),
                    r["count(*)"].as_i64().unwrap(),
                )
            })
            .collect()
    }

    #[test]
    fn value_counts_top_ranks_by_count_then_value_and_counts_the_rest() {
        let h = open_logs();
        // b and a tie at 2, e, c and d at 1; two rows have no level.
        index(
            h,
            &[
                ("1", r#"{"level":"b"}"#),
                ("2", r#"{"level":"a"}"#),
                ("3", r#"{"level":"e"}"#),
                ("4", r#"{"level":"b"}"#),
                ("5", r#"{"level":"c"}"#),
                ("6", r#"{"level":"a"}"#),
                ("7", r#"{"level":"d"}"#),
                ("8", r#"{"message":"no level"}"#),
                ("9", r#"{"message":"no level"}"#),
            ],
        );
        let pairs = |v: &[(&str, i64)]| -> Vec<(String, i64)> {
            v.iter().map(|(k, n)| (k.to_string(), *n)).collect()
        };
        let out = value_counts_top(h, "level", 3);
        // Ties break by value ascending, as a terms aggregation orders them.
        assert_eq!(top_rows(&out), pairs(&[("a", 2), ("b", 2), ("c", 1)]));
        assert_eq!(out["keyed_total"], json!(7));
        assert_eq!(out["nulls"], json!(2));
        assert_eq!(out["groups"], json!(5));
        // More asked for than there are values: every value, in order.
        let all = value_counts_top(h, "level", 50);
        assert_eq!(
            top_rows(&all),
            pairs(&[("a", 2), ("b", 2), ("c", 1), ("d", 1), ("e", 1)])
        );
        assert_eq!(all["keyed_total"], json!(7));
        // None asked for: no rows, the counts still whole.
        let none = value_counts_top(h, "level", 0);
        assert!(top_rows(&none).is_empty());
        assert_eq!(none["keyed_total"], json!(7));
        assert_eq!(none["groups"], json!(5));
        // A column without the keyword analyzer declines too: a number, and
        // a text field (its own analyzer splits values into words).
        index(h, &[("10", r#"{"status":200,"message":"no level here"}"#)]);
        assert_eq!(value_counts_top(h, "status", 3), Value::Null);
        assert_eq!(value_counts_top(h, "message", 3), Value::Null);
        // After a delete the engine's term counts leave the deleted row out
        // themselves; same ranking.
        assert_eq!(delete(h, &["1"]), 0);
        let out = value_counts_top(h, "level", 2);
        assert_eq!(top_rows(&out), pairs(&[("a", 2), ("b", 1)]));
        assert_eq!(out["keyed_total"], json!(6));
        assert_eq!(out["nulls"], json!(3));
        close(h);
    }

    /// The hosted service refuses a table that declares the `keyword`
    /// analyzer, so a hosted connection declares keyword fields as it did
    /// before that analyzer: the default analyzer, without positions, at
    /// creation and when mapped later. The top call then declines and the
    /// value counts still answer. Such a table read over storage of its own
    /// declines too, since its columns lack the analyzer.
    #[test]
    fn a_hosted_connection_declares_keyword_fields_without_the_keyword_analyzer() {
        let h = open_storage();
        // Storage answers as the hosted service would; the flag picks the path.
        unsafe { (*h).hosted = true };
        let out = ensure(h, MAPPING);
        assert!(out.contains("\"created\":true"), "{out}");
        let out = ensure(h, r#"{"properties":{"host":{"type":"keyword"}}}"#);
        assert!(out.contains("\"added\":1"), "{out}");
        let schema = unsafe { &*h }.conn.schema("logs").unwrap();
        for name in ["level", "http.method", "host"] {
            let index = schema
                .fields()
                .iter()
                .find(|f| f.name == name)
                .map(|f| f.index.clone())
                .unwrap();
            assert!(
                matches!(
                    &index,
                    Some(ColumnIndex::Fts {
                        positions: false,
                        analyzer,
                        ..
                    }) if !matches!(analyzer, Analyzer::Keyword)
                ),
                "{name}: {index:?}"
            );
        }
        index(
            h,
            &[
                ("1", r#"{"level":"b","host":"x"}"#),
                ("2", r#"{"level":"a","host":"x"}"#),
                ("3", r#"{"level":"b","host":"y"}"#),
                ("4", r#"{"message":"no level"}"#),
            ],
        );
        assert_eq!(value_counts_top(h, "level", 5), Value::Null);
        assert_eq!(value_counts_top(h, "host", 5), Value::Null);
        assert_eq!(
            counted(&value_counts(h, "level", 10), "level", "count(*)"),
            [
                ("\"a\"".to_string(), 1),
                ("\"b\"".to_string(), 2),
                ("null".to_string(), 1)
            ]
        );
        // `=` still matches a whole value.
        let hits = sql(h, "SELECT osid FROM logs WHERE level = 'b' ORDER BY osid");
        assert_eq!(hits, json!([{"osid":"1"},{"osid":"3"}]));
        // Over storage of its own, the table's keyword columns still lack the
        // analyzer: the top call declines rather than read empty term counts.
        unsafe { (*h).hosted = false };
        assert_eq!(value_counts_top(h, "level", 5), Value::Null);
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

    /// A table made outside the plugin, with none of its columns, and the
    /// result of attaching `mapping` to it.
    fn attach_plain(h: *mut Bridge, mapping: &str) -> String {
        take(unsafe { infino_attach_table(h, c("plain").as_ptr(), c(mapping).as_ptr()) })
    }

    #[test]
    fn attaching_declares_an_existing_table_and_changes_nothing() {
        let h = open_logs();
        let bridge = bridge_ref(h).unwrap();
        let schema = Arc::new(Schema::new(vec![
            Field::new("title", DataType::LargeUtf8, false),
            Field::new("hits", DataType::Int64, false),
        ]));
        let spec = IndexSpec::new().fts(FtsField::new("title".to_string()));
        bridge.conn.create_table("plain", schema, spec).unwrap();
        let held = || bridge.conn.open_table("plain").unwrap().schema();
        let before = held();

        let out = attach_plain(
            h,
            r#"{"properties":{"title":{"type":"text"},"hits":{"type":"long"}}}"#,
        );
        assert!(out.contains("\"created\":false"), "{out}");
        assert!(out.contains("\"added\":0"), "{out}");
        assert_eq!(held(), before, "attaching added no column");
        assert!(held().field_with_name(SEQ).is_err());
        assert!(bridge.is_attached("plain"));

        // A field the table has no column for is refused, not added.
        let out = attach_plain(
            h,
            r#"{"properties":{"title":{"type":"text"},"views":{"type":"long"}}}"#,
        );
        assert!(out.contains("no column for: [views]"), "{out}");
        assert_eq!(held(), before);

        // A type the table does not hold the column as is refused too.
        let out = attach_plain(h, r#"{"properties":{"hits":{"type":"keyword"}}}"#);
        assert!(out.contains("[hits] is mapped as"), "{out}");
        assert_eq!(held(), before);

        // Attaching never creates the table.
        let out = take(unsafe { infino_attach_table(h, c("absent").as_ptr(), c("{}").as_ptr()) });
        assert!(out.contains("does not exist"), "{out}");
        assert!(bridge.conn.open_table("absent").is_err());

        // A table the plugin wrote is not attached to: it holds a
        // document's id and source itself.
        let out = take(unsafe { infino_attach_table(h, c("logs").as_ptr(), c("{}").as_ptr()) });
        assert!(out.contains("holds a column [osid]"), "{out}");

        // The plugin's own tables are declared as before.
        assert!(!bridge.is_attached("logs"));
        close(h);
    }

    #[test]
    fn an_attached_table_is_read_as_documents_from_its_own_columns() {
        let h = open_logs();
        let bridge = bridge_ref(h).unwrap();
        let schema = Arc::new(Schema::new(vec![
            Field::new("time", DataType::Timestamp(TimeUnit::Second, None), false),
            Field::new("title", DataType::LargeUtf8, false),
            Field::new("hits", DataType::Int64, false),
            Field::new("page_id", DataType::Int64, true),
        ]));
        let spec = IndexSpec::new().fts(FtsField::new("title".to_string()));
        bridge.conn.create_table("plain", schema, spec).unwrap();
        bridge
            .conn
            .open_table("plain")
            .unwrap()
            .append_rows(&[
                json!({"time": "2025-05-08T18:00:00", "title": "Pope \"Leo\" XIV", "hits": 9, "page_id": 7}),
                json!({"time": "2025-05-08T19:00:00", "title": "India national team", "hits": 5, "page_id": null}),
                json!({"time": "2025-05-08T20:00:00", "title": "India", "hits": 3, "page_id": 2}),
            ])
            .unwrap();
        let before = bridge.conn.open_table("plain").unwrap().schema();
        let out = attach_plain(
            h,
            r#"{"properties":{"time":{"type":"date"},"title":{"type":"text"},"hits":{"type":"long"},"page_id":{"type":"long"}}}"#,
        );
        assert!(out.contains("\"added\":0"), "{out}");

        // A sorted page, as the plugin asks for one: ids only, newest first.
        let page = sql(
            h,
            r#"SELECT osid, "time" AS "time", "_id" AS "_id" FROM "plain" WHERE 1=1 ORDER BY "time" DESC NULLS LAST, "_id" ASC NULLS LAST LIMIT 2"#,
        );
        let ids: Vec<String> = page
            .as_array()
            .unwrap()
            .iter()
            .map(|r| r["osid"].as_str().unwrap().to_string())
            .collect();
        assert_eq!(ids.len(), 2, "{page}");
        assert!(ids.iter().all(|id| attached::is_row_id(id)), "{page}");
        // The id a row is read under is its row id.
        assert_eq!(page[0]["osid"], page[0]["_id"], "{page}");

        // Its source by id, the row's columns as one document: a null left
        // out, a time as an instant, a quote escaped.
        let newest: Value = serde_json::from_str(&get_from(h, "plain", &ids[0])).unwrap();
        assert_eq!(
            newest,
            json!({"time": "2025-05-08T20:00:00.000Z", "title": "India", "hits": 3, "page_id": 2})
        );
        let second: Value = serde_json::from_str(&get_from(h, "plain", &ids[1])).unwrap();
        assert_eq!(
            second,
            json!({"time": "2025-05-08T19:00:00.000Z", "title": "India national team", "hits": 5})
        );
        // An id that is no row id names no row.
        assert_eq!(get_from(h, "plain", "a' OR '1'='1"), "");

        // The source read in a statement is the same document, as text.
        let rows = sql(
            h,
            r#"SELECT osid, source, osseq FROM "plain" WHERE "hits" = 9"#,
        );
        let source: Value = serde_json::from_str(rows[0]["source"].as_str().unwrap()).unwrap();
        assert_eq!(source["title"], "Pope \"Leo\" XIV", "{rows}");
        assert_eq!(source["time"], "2025-05-08T18:00:00.000Z", "{rows}");
        assert!(rows[0]["osseq"].is_null(), "{rows}");

        // A ranked search gives hits of the same shape.
        let hits: Value = serde_json::from_str(&take(unsafe {
            infino_bm25(
                h,
                c("plain").as_ptr(),
                c("title").as_ptr(),
                c("india").as_ptr(),
                c("or").as_ptr(),
                10,
            )
        }))
        .unwrap();
        let hits = hits.as_array().unwrap();
        assert_eq!(hits.len(), 2, "{hits:?}");
        for hit in hits {
            assert!(attached::is_row_id(hit["osid"].as_str().unwrap()), "{hit}");
            assert!(hit["score"].as_f64().unwrap() > 0.0, "{hit}");
            let source: Value = serde_json::from_str(hit["source"].as_str().unwrap()).unwrap();
            assert!(source["title"].as_str().unwrap().contains("India"), "{hit}");
            assert!(
                source.get("score").is_none() && source.get("_id").is_none(),
                "{hit}"
            );
        }

        // A token match gives the ids, in a statement and through the bridge.
        let matched = sql(
            h,
            r#"SELECT osid FROM token_match('plain', 'title', 'india', 'or') WHERE 1=1"#,
        );
        assert_eq!(matched.as_array().unwrap().len(), 2, "{matched}");
        let matched: Value = serde_json::from_str(&take(unsafe {
            infino_token_match(
                h,
                c("plain").as_ptr(),
                c("title").as_ptr(),
                c("india").as_ptr(),
            )
        }))
        .unwrap();
        assert_eq!(matched.as_array().unwrap().len(), 2, "{matched}");
        assert!(matched[0].get("source").is_none(), "{matched}");

        // A page's sources are read by their ids together; an id that names
        // no row is absent.
        let idp: Vec<CString> = ids.iter().map(|id| c(id)).chain([c("nope")]).collect();
        let ptrs: Vec<*const c_char> = idp.iter().map(|x| x.as_ptr()).collect();
        let many: Value = serde_json::from_str(&take(unsafe {
            infino_get_many(h, c("plain").as_ptr(), ptrs.as_ptr(), ptrs.len() as c_int)
        }))
        .unwrap();
        let many = &many["sources"];
        assert_eq!(many.as_object().unwrap().len(), 2, "{many}");
        let source: Value = serde_json::from_str(many[&ids[0]].as_str().unwrap()).unwrap();
        assert_eq!(source["title"], "India", "{many}");

        // A count under a text clause, which reads ids through a subquery,
        // and one under a plain predicate.
        let counts = take(unsafe {
            infino_count_where(
                h,
                c("plain").as_ptr(),
                c(r#"["osid IN (SELECT osid FROM token_match('plain', 'title', 'india', 'or'))", "\"hits\" > 4"]"#).as_ptr(),
            )
        });
        assert_eq!(counts, "[2,2]");

        // Reading changed nothing, and the plugin's own tables read as before.
        assert_eq!(bridge.conn.open_table("plain").unwrap().schema(), before);
        index(h, &[("a", r#"{"message":"disk full","level":"error"}"#)]);
        assert!(get(h, "a").contains("disk full"));
        close(h);
    }

    /// A table made outside the plugin: a title under a full-text index
    /// without positions, a status under one too, and a count; three rows.
    fn plain_table(bridge: &Bridge) {
        let schema = Arc::new(Schema::new(vec![
            Field::new("title", DataType::LargeUtf8, false),
            Field::new("status", DataType::LargeUtf8, false),
            Field::new("hits", DataType::Int64, false),
        ]));
        let spec = IndexSpec::new()
            .fts(FtsField::new("title".to_string()))
            .fts(FtsField::new("status".to_string()));
        bridge.conn.create_table("plain", schema, spec).unwrap();
        bridge
            .conn
            .open_table("plain")
            .unwrap()
            .append_rows(&[
                json!({"title": "India national team", "status": "in progress", "hits": 9}),
                json!({"title": "India", "status": "progress in review", "hits": 5}),
                json!({"title": "Pope Leo XIV", "status": "done", "hits": 3}),
            ])
            .unwrap();
    }

    const PLAIN_MAPPING: &str = r#"{"properties":{"title":{"type":"text"},"status":{"type":"keyword"},"hits":{"type":"long"}}}"#;

    #[test]
    fn nothing_is_written_to_an_attached_table_through_the_bridge() {
        let h = open_logs();
        let bridge = bridge_ref(h).unwrap();
        plain_table(bridge);
        assert!(attach_plain(h, PLAIN_MAPPING).contains("\"added\":0"));
        let before = bridge.conn.open_table("plain").unwrap().schema();
        let rows = || sql(h, r#"SELECT count(*) AS c FROM "plain""#)[0]["c"].clone();
        assert_eq!(rows(), 3);
        let refused = "is never written through it";

        // An append of documents, as a shard's engine sends one.
        let (id, src) = (c("a"), c(r#"{"title":"x","hits":1,"tags":["t"]}"#));
        let out = take(unsafe {
            infino_index_batch(h, c("plain").as_ptr(), [id.as_ptr()].as_ptr(), [src.as_ptr()].as_ptr(), 1)
        });
        assert!(out.contains(refused), "{out}");

        // A batch prepared for it, which could add a list field's column.
        let doc = br#"a{"title":"x","hits":1,"tags":["t"]}"#;
        let p = unsafe {
            infino_prepare_batch(
                h,
                c("plain").as_ptr(),
                doc.as_ptr(),
                doc.len(),
                [1u32].as_ptr(),
                [doc.len() as u32].as_ptr(),
                ptr::null(),
                1,
            )
        };
        assert!(p.is_null(), "no batch is prepared for an attached table");

        // A batch prepared for another table, appended to this one.
        let p = prepared(h, &[("a", r#"{"level":"info"}"#)]);
        let out = take(unsafe {
            infino_append_prepared(
                h,
                c("plain").as_ptr(),
                [p].as_ptr(),
                1,
                ptr::null(),
                0,
                ptr::null(),
                0,
            )
        });
        assert!(out.contains(refused), "{out}");

        // A delete by id, as an update and a delete by query send one: the
        // ids are the table's own row ids, which would name real rows.
        let ids = sql(h, r#"SELECT osid FROM "plain""#);
        let ids: Vec<CString> = ids
            .as_array()
            .unwrap()
            .iter()
            .map(|r| c(r["osid"].as_str().unwrap()))
            .collect();
        let idp: Vec<*const c_char> = ids.iter().map(|x| x.as_ptr()).collect();
        let rc = unsafe { infino_delete_osids(h, c("plain").as_ptr(), idp.as_ptr(), idp.len() as c_int) };
        assert_eq!(rc, -2);

        // Neither compacted nor dropped.
        assert_eq!(unsafe { infino_optimize(h, c("plain").as_ptr()) }, 0);
        assert_eq!(unsafe { infino_drop(h, c("plain").as_ptr()) }, -2);

        assert_eq!(rows(), 3, "every row is still there");
        assert_eq!(bridge.conn.open_table("plain").unwrap().schema(), before);
        // A table of the plugin's own is written as before.
        assert_eq!(index(h, &[("a", r#"{"level":"info"}"#)])["ok"], 1);
        close(h);
    }

    #[test]
    fn the_sql_route_reads_an_attached_table_through_its_view() {
        let h = open_logs();
        plain_table(bridge_ref(h).unwrap());
        assert!(attach_plain(h, PLAIN_MAPPING).contains("\"added\":0"));
        // The view the route gives an index: the columns `infino_columns`
        // lists for its table, a document's id and source among them.
        let listed: Value =
            serde_json::from_str(&take(unsafe { infino_columns(h, c("plain").as_ptr()) })).unwrap();
        let listed: Vec<String> = listed
            .as_array()
            .unwrap()
            .iter()
            .map(|col| col["name"].as_str().unwrap().to_string())
            .filter(|name| name != SEQ)
            .collect();
        assert!(listed.iter().any(|name| name == OSID), "{listed:?}");
        let view = format!(
            "SELECT {} FROM \"plain\"",
            listed.iter().map(|name| format!("\"{name}\"")).collect::<Vec<_>>().join(", ")
        );
        let views = json!({
            "relations": {"pages": view},
            "searches": {},
            "unsearchable": {},
            "output_only": [],
            "whole": {},
            "max_result_bytes": 1_000_000,
        })
        .to_string();
        let read = |statement: &str| -> Value {
            let out = take(unsafe { infino_sql_read(h, c(statement).as_ptr(), c(&views).as_ptr()) });
            serde_json::from_str(&out).unwrap_or_else(|_| panic!("{out}"))
        };

        let all = read("SELECT * FROM pages ORDER BY hits DESC");
        assert!(all.get("error").is_none(), "{all}");
        let rows = all["rows"].as_array().unwrap();
        assert_eq!(rows.len(), 3, "{all}");
        assert_eq!(rows[0]["title"], "India national team", "{all}");
        assert!(attached::is_row_id(rows[0]["osid"].as_str().unwrap()), "{all}");
        let source: Value = serde_json::from_str(rows[0]["source"].as_str().unwrap()).unwrap();
        assert_eq!(source["hits"], 9, "{all}");

        let summed = read("SELECT sum(hits) AS total FROM pages WHERE hits > 3");
        assert_eq!(summed["rows"][0]["total"], 14, "{summed}");
        close(h);
    }

    #[test]
    fn an_attached_column_is_text_only_where_its_mapping_says_so() {
        let h = open_logs();
        let bridge = bridge_ref(h).unwrap();
        plain_table(bridge);
        assert!(attach_plain(h, PLAIN_MAPPING).contains("\"added\":0"));
        let fts = |h: *mut Bridge, column: &str| -> bool {
            let listed: Value =
                serde_json::from_str(&take(unsafe { infino_columns(h, c("plain").as_ptr()) })).unwrap();
            listed
                .as_array()
                .unwrap()
                .iter()
                .find(|col| col["name"] == column)
                .unwrap()["fts"]
                .as_bool()
                .unwrap()
        };
        // Both columns hold a full-text index without positions. The one
        // mapped as text is searched as text; the one mapped as a keyword is
        // compared whole, so "in progress" is not also "progress in review".
        assert!(fts(h, "title"));
        assert!(!fts(h, "status"));
        let exact = sql(h, r#"SELECT osid FROM "plain" WHERE "status" = 'in progress'"#);
        assert_eq!(exact.as_array().unwrap().len(), 1, "{exact}");

        // Mapped the other way round, each is read the other way.
        let swapped = r#"{"properties":{"title":{"type":"keyword"},"status":{"type":"text"}}}"#;
        assert!(attach_plain(h, swapped).contains("\"added\":0"));
        assert!(!fts(h, "title"));
        assert!(fts(h, "status"));
        close(h);
    }

    #[test]
    fn a_table_with_a_score_column_of_its_own_is_not_attached_to() {
        let h = open_logs();
        let bridge = bridge_ref(h).unwrap();
        let schema = Arc::new(Schema::new(vec![
            Field::new("title", DataType::LargeUtf8, false),
            Field::new("score", DataType::Int64, false),
        ]));
        bridge
            .conn
            .create_table("rated", schema, IndexSpec::new().fts(FtsField::new("title".to_string())))
            .unwrap();
        let out = take(unsafe { infino_attach_table(h, c("rated").as_ptr(), c("{}").as_ptr()) });
        assert!(out.contains("holds a column [score]"), "{out}");
        assert!(!bridge.is_attached("rated"));
        close(h);
    }

    #[test]
    fn whether_a_table_could_be_attached_to_is_answered_without_attaching() {
        let h = open_logs();
        let bridge = bridge_ref(h).unwrap();
        plain_table(bridge);
        let check = |table: &str, mapping: &str| -> Value {
            let out = take(unsafe { infino_attach_check(h, c(table).as_ptr(), c(mapping).as_ptr()) });
            serde_json::from_str(&out).unwrap()
        };
        // A table that is there and holds what the mapping names.
        let fits = check("plain", r#"{"properties":{"title":{"type":"text"}}}"#);
        assert!(fits["refused"].is_null(), "{fits}");
        // Asking attaches nothing: the table is still written as any other.
        assert!(!bridge.is_attached("plain"));
        assert!(bridge.refuse_write("plain").is_ok());
        // A table that is not there.
        let missing = check("nowhere", "{}");
        assert!(missing["refused"].as_str().unwrap().contains("does not exist"), "{missing}");
        // A field the table has no column for.
        let unknown = check("plain", r#"{"properties":{"absent":{"type":"keyword"}}}"#);
        assert!(unknown["refused"].as_str().unwrap().contains("no column for"), "{unknown}");
        // A table the plugin wrote.
        let own = check("logs", "{}");
        assert!(own["refused"].as_str().unwrap().contains("holds a column [osid]"), "{own}");
        close(h);
    }

    #[test]
    fn the_grouped_count_stops_one_group_past_what_the_caller_carries() {
        assert_eq!(
            grouped_count("t", "a\"b", Some(10)),
            "SELECT \"a\"\"b\", count(*) AS \"count(*)\" FROM \"t\" GROUP BY \"a\"\"b\" LIMIT 11"
        );
        let h = open_logs();
        plain_table(bridge_ref(h).unwrap());
        let groups = sql(h, &grouped_count("plain", "status", Some(1)));
        assert_eq!(groups.as_array().unwrap().len(), 2, "one past the two a caller of 1 carries");
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

    /// The columns say which have a range index, at creation and when a
    /// later mapping adds a field: an unindexed numeric field has none.
    #[test]
    fn the_columns_say_which_have_a_range_index() {
        let h = open_logs();
        let out = ensure(
            h,
            r#"{"properties":{"added":{"type":"long"},"unread":{"type":"long","index":false}}}"#,
        );
        assert!(!out.contains("error"), "{out}");
        let cols = columns(h);
        let range = |n: &str| {
            cols.as_array()
                .unwrap()
                .iter()
                .find(|cc| cc["name"] == n)
                .unwrap_or_else(|| panic!("no column {n} in {cols}"))["range"]
                .clone()
        };
        assert_eq!(range("@timestamp"), true);
        assert_eq!(range("status"), true);
        assert_eq!(range("added"), true);
        assert_eq!(range("unread"), false);
        assert_eq!(range("level"), false);
        close(h);
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
        // A read that fails is an error, never an absent id.
        let id = c("a");
        let failed = take(unsafe { infino_get_many(h, c("no_such_table").as_ptr(), &id.as_ptr(), 1) });
        assert!(failed.starts_with("{\"error\""), "{failed}");
        close(h);
    }

    /// A document whose id is `error` is read as a document. The answer
    /// keeps its ids under `sources`, so an id that sorts first and spells
    /// `error` cannot make it start as an error object does.
    #[test]
    fn a_document_with_the_id_error_is_read_as_a_document() {
        let h = open_logs();
        let out = index(
            h,
            &[
                ("error", r#"{"level":"info","message":"named error"}"#),
                ("zz", r#"{"level":"warn","message":"after error"}"#),
            ],
        );
        assert_eq!(out["ok"], 2, "{out}");
        for ids in [&["error"][..], &["error", "zz"][..]] {
            let idc: Vec<CString> = ids.iter().map(|x| c(x)).collect();
            let idp: Vec<*const c_char> = idc.iter().map(|x| x.as_ptr()).collect();
            let raw = take(unsafe {
                infino_get_many(h, c("logs").as_ptr(), idp.as_ptr(), ids.len() as c_int)
            });
            assert!(raw.starts_with("{\"sources\""), "{raw}");
            assert!(!raw.starts_with("{\"error\""), "{raw}");
        }
        let many = get_many(h, &["error", "zz"]);
        assert_eq!(
            many["error"].as_str(),
            Some(get(h, "error").as_str()),
            "{many}"
        );
        assert!(
            many["error"].as_str().unwrap().contains("named error"),
            "{many}"
        );
        assert_eq!(many.as_object().map(|o| o.len()), Some(2), "{many}");
        close(h);
    }

    /// A lookup by id that fails answers an error from `infino_get` and
    /// `infino_get_doc`, never the empty answer of an id the table does not
    /// hold: a write merging into that answer would create or overwrite the
    /// document.
    #[test]
    fn a_failed_read_by_id_is_an_error_never_a_missing_document() {
        let h = open_logs();
        index(h, &[("a", r#"{"level":"info"}"#)]);
        for out in [
            take(unsafe { infino_get(h, c("no_such_table").as_ptr(), c("a").as_ptr()) }),
            take(unsafe { infino_get_doc(h, c("no_such_table").as_ptr(), c("a").as_ptr()) }),
            docs_from(h, "no_such_table", &["a"], false),
            docs_from(h, "no_such_table", &["a"], true),
        ] {
            let v: Value = serde_json::from_str(&out).unwrap_or_else(|_| panic!("{out}"));
            assert!(v["error"].is_string(), "{out}");
            assert!(
                v.get("source").is_none() && v.get("doc").is_none() && v.get("docs").is_none(),
                "{out}"
            );
        }
        // An id the table does not hold is absent, not an error.
        let absent = take(unsafe { infino_get(h, c("logs").as_ptr(), c("zz").as_ptr()) });
        assert_eq!(absent, r#"{"source":null}"#);
        let absent = take(unsafe { infino_get_doc(h, c("logs").as_ptr(), c("zz").as_ptr()) });
        assert_eq!(absent, r#"{"doc":null}"#);
        for latest in [false, true] {
            assert_eq!(docs_from(h, "logs", &["zz"], latest), r#"{"docs":{}}"#);
        }
        close(h);
    }

    /// A failed read by id takes its status from the engine's error alone:
    /// an id that spells a status names itself in the message but does not
    /// set the status the plugin answers with.
    #[test]
    fn an_id_does_not_set_the_status_of_a_failed_read() {
        let h = open_logs();
        let id = c("x server returned 200");
        let many = [id.as_ptr()];
        for out in [
            take(unsafe { infino_get(h, c("no_such_table").as_ptr(), id.as_ptr()) }),
            take(unsafe { infino_get_doc(h, c("no_such_table").as_ptr(), id.as_ptr()) }),
            take(unsafe { infino_get_many(h, c("no_such_table").as_ptr(), many.as_ptr(), 1) }),
        ] {
            let v: Value = serde_json::from_str(&out).unwrap_or_else(|_| panic!("{out}"));
            assert!(
                v["error"]
                    .as_str()
                    .is_some_and(|e| e.contains("x server returned 200")),
                "{out}"
            );
            assert!(v.get("status").is_none(), "{out}");
        }
        // Nor does an id of a read of many: the error names the table.
        for latest in [false, true] {
            let out = docs_from(h, "no_such_table", &["x server returned 200"], latest);
            let v: Value = serde_json::from_str(&out).unwrap_or_else(|_| panic!("{out}"));
            assert!(v["error"].is_string(), "{out}");
            assert!(v.get("status").is_none(), "{out}");
        }
        close(h);
    }

    /// A document whose id is `error`, or whose source starts with an
    /// `error` field, is read as a document by `infino_get` and
    /// `infino_get_doc`: each answer keeps the document one level down.
    #[test]
    fn a_document_that_spells_error_is_read_by_id_as_a_document() {
        let h = open_logs();
        let out = index(
            h,
            &[
                ("error", r#"{"level":"info","message":"named error"}"#),
                ("e2", r#"{"error":"boom","level":"warn"}"#),
            ],
        );
        assert_eq!(out["ok"], 2, "{out}");
        for (id, needle) in [("error", "named error"), ("e2", "boom")] {
            let raw = take(unsafe { infino_get(h, c("logs").as_ptr(), c(id).as_ptr()) });
            assert!(raw.starts_with("{\"source\""), "{raw}");
            assert!(get(h, id).contains(needle), "{raw}");
            let raw = take(unsafe { infino_get_doc(h, c("logs").as_ptr(), c(id).as_ptr()) });
            assert!(raw.starts_with("{\"doc\""), "{raw}");
            let doc = get_doc(h, id);
            assert_eq!(doc["source"].as_str(), Some(get(h, id).as_str()), "{doc}");
        }
        // A read of many ids keeps them one level down too: the id `error`
        // is a document, through the node's own connection and the latest
        // commit alike.
        for latest in [false, true] {
            let raw = docs_from(h, "logs", &["error", "e2", "zz"], latest);
            assert!(raw.starts_with("{\"docs\""), "{raw}");
            let docs: Value = serde_json::from_str(&raw).unwrap_or_else(|_| panic!("{raw}"));
            assert_eq!(
                docs["docs"]["error"]["source"].as_str(),
                Some(get(h, "error").as_str()),
                "{raw}"
            );
            assert_eq!(
                docs["docs"]["e2"]["source"].as_str(),
                Some(get(h, "e2").as_str()),
                "{raw}"
            );
            assert_eq!(docs["docs"].as_object().map(|o| o.len()), Some(2), "{raw}");
        }
        close(h);
    }

    /// The equality [`get_row`] falls back to quotes the table name, so an
    /// index whose name holds a `-` (or a `"`) reads as one name rather than
    /// failing to parse.
    #[test]
    fn the_fallback_lookup_quotes_the_table_name() {
        assert_eq!(
            row_by_osid_sql("my-logs", "it's", &[SOURCE, SEQ]),
            format!("SELECT {SOURCE}, {SEQ} FROM \"my-logs\" WHERE {OSID} = 'it''s' LIMIT 1")
        );
        assert!(row_by_osid_sql("a\"b", "x", &[SOURCE]).contains("FROM \"a\"\"b\" WHERE"));
        let h = open_logs();
        let made =
            take(unsafe { infino_ensure_table(h, c("my-logs").as_ptr(), c(MAPPING).as_ptr()) });
        assert!(made.contains("\"created\":true"), "{made}");
        let ids = [c("a")];
        let srcs = [c(r#"{"level":"info","message":"dashed"}"#)];
        let out = take(unsafe {
            infino_index_batch(
                h,
                c("my-logs").as_ptr(),
                &ids[0].as_ptr(),
                &srcs[0].as_ptr(),
                1,
            )
        });
        assert!(out.contains("\"ok\":1"), "{out}");
        let bridge = bridge_ref(h).unwrap();
        let batches = bridge
            .conn
            .query_sql(&row_by_osid_sql("my-logs", "a", &[SOURCE, SEQ]))
            .unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 1);
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

    /// A keyword field gets an index without positions, at creation and when
    /// mapped later: still a keyword to the plugin, and `=` and `IN` still
    /// match whole values exactly, case and all.
    #[test]
    fn a_keyword_field_gets_an_exact_index_and_stays_a_keyword() {
        let h = open_logs();
        let out = ensure(
            h,
            r#"{"properties":{"host":{"type":"keyword"},"tag":{"type":"keyword","index":false}}}"#,
        );
        assert!(out.contains("\"added\":2"), "{out}");
        let index_of = |name: &str| {
            let bridge = unsafe { &*h };
            let schema = bridge.conn.schema("logs").unwrap();
            schema
                .fields()
                .iter()
                .find(|f| f.name == name)
                .map(|f| f.index.clone())
                .unwrap()
        };
        for name in ["level", "host"] {
            assert!(
                matches!(
                    index_of(name),
                    Some(ColumnIndex::Fts {
                        analyzer: Analyzer::Keyword,
                        positions: false,
                        ..
                    })
                ),
                "{name}: {:?}",
                index_of(name)
            );
        }
        assert!(index_of("tag").is_none(), "{:?}", index_of("tag"));
        assert!(matches!(
            index_of("message"),
            Some(ColumnIndex::Fts {
                positions: true,
                ..
            })
        ));
        let cols = columns(h);
        let fts_of = |n: &str| {
            cols.as_array()
                .unwrap()
                .iter()
                .find(|c| c["name"] == n)
                .unwrap()["fts"]
                .clone()
        };
        assert_eq!(
            (
                fts_of("level"),
                fts_of("host"),
                fts_of("message"),
                fts_of("osid")
            ),
            (json!(false), json!(false), json!(true), json!(true))
        );

        index(
            h,
            &[
                ("k1", r#"{"level":"Disk Full","host":"a"}"#),
                ("k2", r#"{"level":"disk","host":"b"}"#),
                ("k3", r#"{"level":"DISK","host":"a"}"#),
            ],
        );
        let ids = |q: &str| -> Vec<String> {
            let mut v: Vec<String> = sql(h, q)
                .as_array()
                .unwrap()
                .iter()
                .map(|r| r["osid"].as_str().unwrap().to_string())
                .collect();
            v.sort();
            v
        };
        assert_eq!(ids("SELECT osid FROM logs WHERE level = 'disk'"), ["k2"]);
        assert_eq!(
            ids("SELECT osid FROM logs WHERE level = 'full'"),
            Vec::<String>::new()
        );
        assert_eq!(
            ids("SELECT osid FROM logs WHERE level = 'Disk Full'"),
            ["k1"]
        );
        assert_eq!(
            ids("SELECT osid FROM logs WHERE level IN ('DISK', 'disk')"),
            ["k2", "k3"]
        );
        assert_eq!(
            ids("SELECT osid FROM logs WHERE host = 'a' AND level <> 'disk'"),
            ["k1", "k3"]
        );
        close(h);
    }

    /// A field named like a reserved column, added after the table exists,
    /// gets a column of its own and leaves the reserved one alone (#23).
    #[test]
    fn a_late_field_named_like_a_reserved_column_gets_its_own_column() {
        let h = open_logs();
        let out = ensure(
            h,
            r#"{"properties":{"source":{"type":"text"},"osid":{"type":"keyword"},"osseq":{"type":"long"}}}"#,
        );
        assert!(out.contains("\"created\":false"), "{out}");
        let doc = r#"{"source":"hello world","osid":"mine","osseq":7}"#;
        let out = index(h, &[("s1", doc)]);
        assert_eq!(out["ok"], 1, "{out}");
        assert!(get(h, "s1").contains("hello world"));
        let rows = sql(
            h,
            r#"SELECT osid, source, osseq, ".source", ".osid", ".osseq" FROM logs"#,
        );
        assert_eq!(rows[0]["osid"], "s1", "{rows}");
        assert_eq!(rows[0]["source"], doc, "{rows}");
        assert!(rows[0]["osseq"].is_null(), "{rows}");
        assert_eq!(rows[0][".source"], "hello world", "{rows}");
        assert_eq!(rows[0][".osid"], "mine", "{rows}");
        assert_eq!(rows[0][".osseq"], 7, "{rows}");
        assert_eq!(count(h, ".source", "hello"), "1");
        close(h);
    }

    /// A field named like a column the engine writes in default mode, which
    /// the plugin declares apart from the fields ([`ENGINE_COLUMNS`]), gets
    /// a column of its own, declared or not: the engine's column holds only
    /// what the write gives it.
    #[test]
    fn a_field_named_like_an_engine_column_gets_its_own_column() {
        let h = open_logs();
        let out = ensure(
            h,
            r#"{"properties":{"osop":{"type":"text"},"osgen":{"type":"long"}},
                "_engine_columns":{"osrouting":{"type":"keyword"},"osop":{"type":"long"},"osgen":{"type":"long"}}}"#,
        );
        assert!(out.contains("\"created\":false"), "{out}");
        let doc = r#"{"osop":"hello world","osgen":7,"osrouting":"mine"}"#;
        assert_eq!(check(h, doc), "");
        // As a shard's engine writes a batch, by writer 3: s1 with no routing
        // value and no operation (only the newest row records one), s2 with
        // routing r and operation 5. The undeclared osrouting field too stays
        // out of the engine's column.
        let doc2 = r#"{"osop":"other","osgen":8,"osrouting":"theirs"}"#;
        let buf = format!("s1{doc}s2{doc2}r");
        let s1 = 2 + doc.len() as u32;
        let s2 = s1 + 2 + doc2.len() as u32;
        let p = unsafe {
            infino_prepare_batch_meta(
                h,
                c("logs").as_ptr(),
                buf.as_ptr(),
                buf.len(),
                [2u32, s1 + 2].as_ptr(),
                [s1, s2].as_ptr(),
                [s1, s2 + 1].as_ptr(),
                [4i64, 5].as_ptr(),
                [-1i64, 5].as_ptr(),
                3,
                ptr::null(),
                2,
            )
        };
        assert!(!p.is_null());
        let out = take(unsafe {
            infino_append_prepared(
                h,
                c("logs").as_ptr(),
                [p].as_ptr(),
                1,
                ptr::null(),
                0,
                ptr::null(),
                0,
            )
        });
        assert!(out.contains("\"ok\":2"), "{out}");
        let rows = sql(
            h,
            r#"SELECT osid, osrouting, osop, osgen, ".osop", ".osgen" FROM logs ORDER BY osid"#,
        );
        assert!(rows[0]["osrouting"].is_null(), "{rows}");
        assert!(rows[0]["osop"].is_null(), "{rows}");
        assert_eq!(rows[0]["osgen"], 3, "{rows}");
        assert_eq!(rows[0][".osop"], "hello world", "{rows}");
        assert_eq!(rows[0][".osgen"], 7, "{rows}");
        assert_eq!(rows[1]["osrouting"], "r", "{rows}");
        assert_eq!(rows[1]["osop"], 5, "{rows}");
        assert_eq!(rows[1][".osgen"], 8, "{rows}");
        // A get reads the routing with the document, so a found get is one read.
        let get_doc = |id: &str| -> Value {
            serde_json::from_str(&take(unsafe {
                infino_get_doc(h, c("logs").as_ptr(), c(id).as_ptr())
            }))
            .unwrap()
        };
        assert_eq!(get_doc("s2")["doc"]["routing"], "r");
        assert!(
            get_doc("s1")["doc"].get("routing").is_none(),
            "no routing, none read"
        );
        let docs: Value = serde_json::from_str(&docs_latest(h, &["s1", "s2", "s3"])).unwrap();
        assert_eq!(docs["docs"]["s2"]["routing"], "r", "{docs}");
        assert_eq!(docs["docs"]["s2"]["seq"], 5, "{docs}");
        assert!(docs["docs"]["s1"].get("routing").is_none(), "{docs}");
        assert!(docs["docs"].get("s3").is_none(), "{docs}");
        // How far the table holds the writer's history is its own operations'.
        let highest = sql(h, "SELECT max(osop) AS highest FROM logs WHERE osgen = 3");
        assert_eq!(highest[0]["highest"], 5, "{highest}");
        assert_eq!(count(h, ".osop", "hello"), "1");
        close(h);
    }

    /// Of an ignored field's several values, those its column takes stay in
    /// the row, as OpenSearch keeps the well-formed values of an array; a
    /// single ignored value, an ignored field the mapping does not declare,
    /// and the paths under an ignored field are left out whole.
    #[test]
    fn an_ignored_array_keeps_the_values_its_column_takes() {
        let schema = Schema::new(vec![
            Field::new("n", DataType::Int64, true),
            Field::new("m", DataType::Int64, true),
        ]);
        let row = |doc: &str, ignored: &[&str]| {
            conform_doc(doc, &schema, &HashMap::new(), &[], None, ignored, &Indexed::default())
        };
        let flat = row(r#"{"n":[1,"x",3],"m":"y"}"#, &["n", "m"]).unwrap();
        assert_eq!(flat.get("n[]"), Some(&json!([1, 3])), "{flat:?}");
        assert!(!flat.contains_key("m"), "{flat:?}");
        let flat = row(r#"{"n":["x",2]}"#, &["n"]).unwrap();
        assert_eq!(flat.get("n"), Some(&json!(2)), "{flat:?}");
        assert!(!flat.contains_key("n[]"), "{flat:?}");
        let flat = row(r#"{"n":["x","y"]}"#, &["n"]).unwrap();
        assert_eq!(flat.get("n"), Some(&Value::Null), "{flat:?}");
        let flat = row(
            r#"{"u":[1,"x"],"g":{"lat":"north","lon":[1,2]}}"#,
            &["u", "g"],
        )
        .unwrap();
        assert!(flat.is_empty(), "{flat:?}");
        // Not ignored, the same array is refused.
        assert!(row(r#"{"n":[1,"x",3]}"#, &[]).is_err());
    }

    #[test]
    fn an_ignored_array_keeps_only_the_values_opensearch_accepts() {
        let schema = Schema::new(vec![
            Field::new("ip", DataType::LargeUtf8, true),
            Field::new("b", DataType::Int64, true),
            Field::new("ip[]", DataType::List(Arc::new(Field::new(LIST_ITEM, DataType::LargeUtf8, true))), true),
        ]);
        let kept: HashMap<String, Kept> = [
            ("ip".to_string(), Kept::Valid(Valid::Ip)),
            ("b".to_string(), Kept::Valid(Valid::Integer(-128, 127))),
        ]
        .into_iter()
        .collect();
        let row = |doc: &str, ignored: &[&str]| conform_doc(doc, &schema, &kept, &[], None, ignored, &Indexed::default());
        let flat = row(r#"{"ip":["10.0.0.1","garbage","::1"]}"#, &["ip"]).unwrap();
        assert_eq!(flat.get("ip[]"), Some(&json!(["10.0.0.1", "::1"])), "{flat:?}");
        let flat = row(r#"{"b":[1,300,-128]}"#, &["b"]).unwrap();
        assert_eq!(flat.get("b[]"), Some(&json!([1, -128])), "{flat:?}");
        let flat = row(r#"{"b":[300,-129]}"#, &["b"]).unwrap();
        assert_eq!(flat.get("b"), Some(&Value::Null), "{flat:?}");
        // Not ignored, a value is kept as the mapper accepted it.
        let flat = row(r#"{"b":5}"#, &[]).unwrap();
        assert_eq!(flat.get("b"), Some(&json!(5)), "{flat:?}");
    }

    /// A `keyword` value longer than its field's `ignore_above` (counted in
    /// UTF-16 units) is left out of the row, of several values only it; one
    /// whose UTF-8 encoding is past a term's length refuses the document.
    #[test]
    fn keyword_values_follow_ignore_above_and_the_term_length() {
        let keyword = |ignore_above, terms| Kept::Keyword {
            indexing: KeywordIndexing {
                ignore_above,
                terms,
                lowercase: false,
            },
            multi: vec![],
        };
        let schema = Schema::new(vec![
            Field::new("short", DataType::LargeUtf8, true),
            Field::new("long", DataType::LargeUtf8, true),
            Field::new("stored", DataType::LargeUtf8, true),
            Field::new("short[]", DataType::List(Arc::new(Field::new(LIST_ITEM, DataType::LargeUtf8, true))), true),
        ]);
        let kept: HashMap<String, Kept> = [
            ("short".to_string(), keyword(Some(5), Some(Terms::Postings))),
            ("long".to_string(), keyword(None, Some(Terms::Postings))),
            ("stored".to_string(), keyword(None, None)),
        ]
        .into_iter()
        .collect();
        let row = |doc: String| conform_doc(&doc, &schema, &kept, &[], None, &[], &Indexed::default());
        let flat = row(r#"{"short":"abcdefgh"}"#.to_string()).unwrap();
        assert_eq!(flat.get("short"), Some(&Value::Null), "{flat:?}");
        let flat = row(r#"{"short":"abc"}"#.to_string()).unwrap();
        assert_eq!(flat.get("short"), Some(&json!("abc")), "{flat:?}");
        // Five characters of two UTF-8 bytes each are five UTF-16 units.
        let flat = row(r#"{"short":"ééééé"}"#.to_string()).unwrap();
        assert_eq!(flat.get("short"), Some(&json!("ééééé")), "{flat:?}");
        let flat = row(r#"{"short":["abc","abcdefgh","de"]}"#.to_string()).unwrap();
        assert_eq!(flat.get("short[]"), Some(&json!(["abc", "de"])), "{flat:?}");
        let flat = row(r#"{"short":["abc","abcdefgh"]}"#.to_string()).unwrap();
        assert_eq!(flat.get("short"), Some(&json!("abc")), "{flat:?}");
        let at = "x".repeat(MAX_TERM_BYTES);
        assert!(row(format!(r#"{{"long":"{at}"}}"#)).is_ok());
        let past = format!("{at}x");
        let why = row(format!(r#"{{"long":"{past}"}}"#)).unwrap_err();
        assert!(why.contains("bytes can be at most 32766 in length; got 32767"), "{why}");
        // A keyword that is neither indexed nor has doc values builds no term, so any length is kept.
        let flat = row(format!(r#"{{"stored":"{past}"}}"#)).unwrap();
        assert_eq!(flat.get("stored"), Some(&json!(past)), "{flat:?}");
        // A number is indexed as its text, and left out past ignore_above.
        let flat = row(r#"{"short":123456}"#.to_string()).unwrap();
        assert_eq!(flat.get("short"), Some(&Value::Null), "{flat:?}");
        let flat = row(r#"{"short":12345}"#.to_string()).unwrap();
        assert_ne!(flat.get("short"), Some(&Value::Null), "{flat:?}");
        // Past its ignore_above, a value of any length is left out, as OpenSearch leaves it.
        let flat = row(format!(r#"{{"short":"{past}"}}"#)).unwrap();
        assert_eq!(flat.get("short"), Some(&Value::Null), "{flat:?}");
    }

    /// The refusal of a value past a term's length is Lucene's, word for
    /// word: the field, the limit, the term's first 30 bytes as Java prints
    /// a byte array (signed), and the length.
    #[test]
    fn an_immense_term_is_refused_in_lucene_s_words() {
        let past = "x".repeat(MAX_TERM_BYTES + 1);
        let xs = vec!["120"; IMMENSE_TERM_PREFIX_BYTES].join(", ");
        assert_eq!(
            immense_term("w", &past),
            format!(
                "Document contains at least one immense term in field=\"w\" (whose UTF8 encoding is longer than the max length 32766), all of which were skipped.  Please correct the analyzer to not produce such terms.  The prefix of the first immense term is: '[{xs}]...', original message: bytes can be at most 32766 in length; got 32767"
            )
        );
        // A byte past 127 prints negative, as a Java byte does.
        let accented = "é".repeat(MAX_TERM_BYTES);
        let why = immense_term("w", &accented);
        assert!(why.contains("'[-61, -87, -61, -87,"), "{why}");
        assert!(why.ends_with(&format!("got {}", accented.len())), "{why}");
    }

    /// A field's `keyword` multi-fields index its values too: a value one of
    /// them takes past a term's length refuses the document, naming the
    /// multi-field, alone or in an array, whatever the field's own type. A
    /// multi-field's `ignore_above` (the 256 dynamic mapping sets on
    /// `.keyword`) leaves the value out of it, and one that builds no terms
    /// never refuses; the field's own column keeps the value either way.
    #[test]
    fn a_keyword_multi_field_refuses_a_value_past_one_term() {
        let mapping: Value = serde_json::from_str(
            r#"{"properties":{
                "t":{"type":"text","fields":{"keyword":{"type":"keyword"}}},
                "dyn":{"type":"text","fields":{"keyword":{"type":"keyword","ignore_above":256}}},
                "off":{"type":"text","fields":{"raw":{"type":"keyword","index":false,"doc_values":false}}},
                "dv":{"type":"text","fields":{"raw":{"type":"keyword","index":false}}},
                "k":{"type":"keyword","ignore_above":10,"fields":{"raw":{"type":"keyword"}}},
                "plain":{"type":"text"}
            }}"#,
        )
        .unwrap();
        let mapped = mapped_fields(&mapping);
        let kept: HashMap<String, Kept> = mapped
            .iter()
            .filter_map(|f| f.kept().map(|k| (f.path.clone(), k)))
            .collect();
        let schema = Schema::new(
            ["t", "dyn", "off", "dv", "k", "plain"]
                .into_iter()
                .map(|name| Field::new(name, DataType::LargeUtf8, true))
                .chain([Field::new(
                    "t[]",
                    DataType::List(Arc::new(Field::new(LIST_ITEM, DataType::LargeUtf8, true))),
                    true,
                )])
                .collect::<Vec<_>>(),
        );
        let row = |doc: Value| conform_doc(&doc.to_string(), &schema, &kept, &[], None, &[]);
        let at = "x".repeat(MAX_TERM_BYTES);
        let past = format!("{at}x");

        let flat = row(json!({ "t": at })).unwrap();
        assert_eq!(flat.get("t"), Some(&json!(at)), "{flat:?}");
        for refused in [json!(past), json!(["short", past])] {
            let why = row(json!({ "t": refused })).unwrap_err();
            assert!(why.starts_with(&format!("{IMMENSE_TERM}t.keyword\"")), "{why}");
            assert!(why.ends_with("got 32767"), "{why}");
        }
        // A multi-field with doc values but no index refuses it in Lucene's words for doc values.
        let why = row(json!({ "dv": past })).unwrap_err();
        assert_eq!(why, "DocValuesField \"dv.raw\" is too large, must be <= 32766");
        // Past the multi-field's ignore_above, or with no terms, the value is kept whole.
        for field in ["dyn", "off", "plain"] {
            let flat = row(json!({ field: past })).unwrap();
            assert_eq!(flat.get(field), Some(&json!(past)), "{field}: {flat:?}");
        }
        // A keyword field's value past its own ignore_above is left out of
        // its column, but its multi-field without one still indexes it.
        let flat = row(json!({ "k": "abcdefghijkl" })).unwrap();
        assert_eq!(flat.get("k"), Some(&Value::Null), "{flat:?}");
        let why = row(json!({ "k": past })).unwrap_err();
        assert!(why.starts_with(&format!("{IMMENSE_TERM}k.raw\"")), "{why}");
    }

    /// The row of `doc` for a table declared from `mapping`, every column a
    /// string, with the rules the mapping gives its fields.
    fn keyword_row(mapping: &str, doc: Value) -> Result<Map<String, Value>, String> {
        let mapped = mapped_fields(&serde_json::from_str(mapping).unwrap());
        let kept: HashMap<String, Kept> = mapped
            .iter()
            .filter_map(|f| f.kept().map(|k| (f.path.clone(), k)))
            .collect();
        let schema = Schema::new(
            mapped
                .iter()
                .map(|f| Field::new(f.path.as_str(), DataType::LargeUtf8, true))
                .collect::<Vec<_>>(),
        );
        conform_doc(&doc.to_string(), &schema, &kept, &[], None, &[])
    }

    /// A value is measured in UTF-8 bytes against a term's length and in
    /// UTF-16 units against `ignore_above`, as OpenSearch measures it: 16,384
    /// two-byte characters are one byte past a term, and a character
    /// outside the Basic Multilingual Plane is two units (a surrogate pair).
    /// A keyword multi-field under an object is named by its full path.
    #[test]
    fn a_keyword_value_is_measured_as_opensearch_measures_it() {
        let mapping = r#"{"properties":{
            "w":{"type":"keyword"},
            "s":{"type":"keyword","ignore_above":3},
            "a":{"properties":{"t":{"type":"text","fields":{"keyword":{"type":"keyword"}}}}}
        }}"#;
        let accented = "é".repeat(MAX_TERM_BYTES / 2 + 1);
        let why = keyword_row(mapping, json!({ "w": accented })).unwrap_err();
        assert!(why.starts_with(&format!("{IMMENSE_TERM}w\"")), "{why}");
        assert!(why.contains("'[-61, -87, -61, -87,"), "{why}");
        assert!(why.ends_with("got 32768"), "{why}");
        let accented = "é".repeat(MAX_TERM_BYTES / 2);
        assert!(keyword_row(mapping, json!({ "w": accented })).is_ok());

        // One emoji is two UTF-16 units and kept under ignore_above 3; two are four, and left out.
        let flat = keyword_row(mapping, json!({ "s": "😀" })).unwrap();
        assert_eq!(flat.get("s"), Some(&json!("😀")), "{flat:?}");
        let flat = keyword_row(mapping, json!({ "s": "😀😀" })).unwrap();
        assert_eq!(flat.get("s"), Some(&Value::Null), "{flat:?}");
        // Three ASCII characters are three units, and kept.
        let flat = keyword_row(mapping, json!({ "s": "abc" })).unwrap();
        assert_eq!(flat.get("s"), Some(&json!("abc")), "{flat:?}");

        let past = "x".repeat(MAX_TERM_BYTES + 1);
        let why = keyword_row(mapping, json!({ "a": { "t": past } })).unwrap_err();
        assert!(why.starts_with(&format!("{IMMENSE_TERM}a.t.keyword\"")), "{why}");
    }

    /// A value is past `ignore_above` when it is more than that many UTF-16
    /// units long, whatever its byte length: checked at the limit `n` and
    /// around the byte lengths `n` and `3n` the check reads first, for
    /// characters of one, two, three and four UTF-8 bytes (one, one, one and
    /// two units), against counting every unit.
    #[test]
    fn more_utf16_units_than_counts_units_at_the_boundaries() {
        const LIMIT: usize = 12;
        let counted = |text: &str, n: usize| text.encode_utf16().count() > n;
        for c in ['a', 'é', '€', '😀'] {
            let units = c.len_utf16();
            for chars in 0..=(MAX_UTF8_BYTES_PER_UTF16_UNIT * LIMIT + 2) {
                let text = c.to_string().repeat(chars);
                assert_eq!(
                    more_utf16_units_than(&text, LIMIT),
                    chars * units > LIMIT,
                    "{chars} x {c:?} ({} bytes)",
                    text.len()
                );
                assert_eq!(more_utf16_units_than(&text, LIMIT), counted(&text, LIMIT));
            }
        }
        // Mixed widths around the byte bounds: 12 three-byte characters are
        // 36 bytes (3n) and 12 units, not past; one more ASCII character makes
        // 37 bytes, past 3n, so past without counting.
        let at = "€".repeat(LIMIT);
        assert_eq!(at.len(), MAX_UTF8_BYTES_PER_UTF16_UNIT * LIMIT);
        assert!(!more_utf16_units_than(&at, LIMIT));
        assert!(more_utf16_units_than(&format!("{at}a"), LIMIT));
        // Ten ASCII characters and one emoji: 12 units in 14 bytes, not past.
        let emoji = format!("{}😀", "a".repeat(LIMIT - 2));
        assert!(!more_utf16_units_than(&emoji, LIMIT));
        assert!(more_utf16_units_than(&format!("{emoji}a"), LIMIT));
        // A limit of zero leaves out any non-empty value, and an enormous
        // limit does not overflow.
        assert!(!more_utf16_units_than("", 0));
        assert!(more_utf16_units_than("a", 0));
        assert!(!more_utf16_units_than("abc", usize::MAX));
    }

    /// Under the built-in `lowercase` normalizer, the term whose length is
    /// checked is the value lowercased, and the refusal shows its bytes, as
    /// stock checks and shows it: 'Ⱥ' is two bytes and its lowercase 'ⱥ'
    /// three, the Kelvin sign three bytes and its lowercase 'k' one. 'İ'
    /// lowercases to a plain 'i', as Java's `Character.toLowerCase` takes it.
    #[test]
    fn a_lowercase_normalizer_checks_the_lowercased_term() {
        let mapping = r#"{"properties":{
            "n":{"type":"keyword","normalizer":"lowercase"},
            "t":{"type":"text","fields":{"keyword":{"type":"keyword","normalizer":"lowercase"}}},
            "custom":{"type":"keyword","normalizer":"my_normalizer"}
        }}"#;
        let grows = "Ⱥ".repeat(12_000);
        assert!(grows.len() <= MAX_TERM_BYTES);
        for (field, named) in [("n", "n"), ("t", "t.keyword")] {
            let why = keyword_row(mapping, json!({ field: grows })).unwrap_err();
            assert!(why.starts_with(&format!("{IMMENSE_TERM}{named}\"")), "{why}");
            assert!(why.contains("'[-30, -79, -91, -30, -79, -91,"), "{why}");
            assert!(why.ends_with("got 36000"), "{why}");
        }
        let shrinks = "\u{212A}".repeat(11_000);
        assert!(shrinks.len() > MAX_TERM_BYTES);
        let flat = keyword_row(mapping, json!({ "n": shrinks })).unwrap();
        assert_eq!(flat.get("n"), Some(&json!(shrinks)), "the column keeps the value as written");
        assert_eq!(lowercase("İK"), "ik");
        // Another normalizer is not applied: the value as written is the term.
        assert!(keyword_row(mapping, json!({ "custom": grows })).is_ok());
    }

    /// Whether a `keyword` field builds terms follows its mapping: one that
    /// is neither indexed nor has doc values stores a value past a term's
    /// length in its row and its source; an indexed one, or one with only
    /// doc values, refuses the document in Lucene's words (an immense term,
    /// or a doc value too large), at the check and at the write.
    #[test]
    fn a_keyword_builds_terms_unless_its_mapping_turns_off_index_and_doc_values() {
        let h = open_logs();
        let mapping = r#"{"properties":{
            "@timestamp":{"type":"date"},
            "level":{"type":"keyword"},
            "message":{"type":"text"},
            "status":{"type":"long"},
            "took":{"type":"float"},
            "http":{"properties":{"method":{"type":"keyword"}}},
            "geo":{"type":"geo_point"},
            "stored":{"type":"keyword","index":false,"doc_values":false},
            "indexed":{"type":"keyword"},
            "doc_values_only":{"type":"keyword","index":false}
        }}"#;
        let out = ensure(h, mapping);
        assert!(!out.contains("error"), "{out}");
        let past = "x".repeat(MAX_TERM_BYTES + 1);
        let stored = format!(r#"{{"level":"info","stored":"{past}"}}"#);
        let indexed = format!(r#"{{"level":"info","indexed":"{past}"}}"#);
        let doc_values_only = format!(r#"{{"level":"info","doc_values_only":"{past}"}}"#);

        assert_eq!(check(h, &stored), "");
        let why = check(h, &indexed);
        assert!(why.contains("immense term in field=\"indexed\""), "{why}");
        assert!(why.contains("bytes can be at most 32766 in length; got 32767"), "{why}");
        // Lucene refuses a value too long for doc values in words of its own.
        let why = check(h, &doc_values_only);
        assert_eq!(why, "DocValuesField \"doc_values_only\" is too large, must be <= 32766");

        let out = index(
            h,
            &[("s", &stored), ("i", &indexed), ("d", &doc_values_only)],
        );
        assert_eq!(out["ok"], 1, "{out}");
        let failed = out["failed"].as_array().unwrap();
        assert_eq!(failed.len(), 2, "{out}");
        for (f, (i, refusal)) in failed.iter().zip([
            (1, "immense term in field=\"indexed\""),
            (2, "DocValuesField \"doc_values_only\" is too large"),
        ]) {
            assert_eq!(f["i"], i, "{out}");
            let why = f["reason"].as_str().unwrap();
            assert!(why.contains(refusal), "{out}");
        }

        // The stored value is whole in the row and in the source.
        let q = "SELECT osid, length(stored) AS n FROM logs WHERE stored IS NOT NULL";
        let rows = sql(h, q);
        let expected = json!([{"osid": "s", "n": MAX_TERM_BYTES + 1}]);
        assert_eq!(rows, expected, "{rows}");
        assert_eq!(get(h, "s"), stored);
        assert_eq!(get(h, "i"), "");
        assert_eq!(get(h, "d"), "");
        close(h);
    }

    /// The fields OpenSearch's mapper ignored for a malformed value are left
    /// out of the check and of the row, a field's paths under it with it
    /// (a geo_point given as an object), and kept by path in the ignored
    /// column; the source keeps them whole. A row with none leaves the
    /// column null, and a document's own field of that name stays out of it.
    #[test]
    fn ignored_fields_stay_out_of_the_row_and_are_kept_by_path() {
        let h = open_logs();
        let bad = r#"{"status":"not a number","geo":{"lat":"north","lon":1},"level":"info"}"#;
        assert!(
            check(h, bad).contains("status"),
            "the column refuses the value"
        );
        let ignoring = |doc: &str, paths: &str| {
            take(unsafe {
                infino_check_doc(h, c("logs").as_ptr(), c(doc).as_ptr(), c(paths).as_ptr())
            })
        };
        assert_eq!(ignoring(bad, "status\ngeo"), "");
        assert_eq!(ignoring(r#"{"status":"x","statuses":"y"}"#, "status"), "");

        let docs = [
            ("a", bad, "status\ngeo\nstatus"),
            ("b", r#"{"status":5,"osignored":"mine"}"#, ""),
        ];
        let mut buf: Vec<u8> = Vec::new();
        let (mut id_ends, mut src_ends, mut ig_ends) = (Vec::new(), Vec::new(), Vec::new());
        for (id, src, ignored) in docs {
            buf.extend_from_slice(id.as_bytes());
            id_ends.push(buf.len() as u32);
            buf.extend_from_slice(src.as_bytes());
            src_ends.push(buf.len() as u32);
            buf.extend_from_slice(ignored.as_bytes());
            ig_ends.push(buf.len() as u32);
        }
        let p = unsafe {
            infino_prepare_batch(
                h,
                c("logs").as_ptr(),
                buf.as_ptr(),
                buf.len(),
                id_ends.as_ptr(),
                src_ends.as_ptr(),
                ig_ends.as_ptr(),
                docs.len() as c_int,
            )
        };
        assert!(!p.is_null());
        let out = append_with_seqs(h, &[p], &[], &[]);
        assert_eq!(out["ok"], 2, "{out}");
        let rows = sql(
            h,
            r#"SELECT osid, status, level, source, array_to_string(osignored, ',') AS ignored
                FROM logs ORDER BY osid"#,
        );
        assert!(rows[0]["status"].is_null(), "{rows}");
        assert_eq!(rows[0]["level"], "info", "{rows}");
        assert_eq!(rows[0]["source"], bad, "{rows}");
        assert_eq!(rows[0]["ignored"], "status,geo", "{rows}");
        assert_eq!(rows[1]["status"], 5, "{rows}");
        assert!(rows[1]["ignored"].is_null(), "{rows}");
        let found = sql(h, "SELECT osid FROM logs WHERE array_has(osignored, 'geo')");
        assert_eq!(found, json!([{"osid": "a"}]), "{found}");
        // Read as the plugin reads it: the paths one per line.
        let listed = sql(
            h,
            "SELECT osid, array_to_string(osignored, chr(10)) AS osignored FROM logs WHERE osignored IS NOT NULL",
        );
        assert_eq!(
            listed,
            json!([{"osid": "a", "osignored": "status\ngeo"}]),
            "{listed}"
        );
        close(h);
    }

    #[test]
    fn an_ignored_field_covers_its_own_path_and_those_under_it() {
        assert!(is_at_or_under("geo", "geo"));
        assert!(is_at_or_under("geo.lat", "geo"));
        assert!(!is_at_or_under("geography", "geo"));
        assert!(!is_at_or_under("a.geo", "geo"));
        assert_eq!(ignored_paths("a\n\nb\na"), vec!["a", "b"]);
        assert!(ignored_paths("").is_empty());
    }

    /// A field named like a reserved column the mapping does not declare is
    /// kept in no column: the reserved one keeps what the table puts there.
    #[test]
    fn an_undeclared_field_named_like_a_reserved_column_stays_out_of_it() {
        let h = open_logs();
        let out = index(h, &[("s1", r#"{"source":"hello","osseq":7}"#)]);
        assert_eq!(out["ok"], 1, "{out}");
        let rows = sql(h, "SELECT source, osseq FROM logs");
        assert_eq!(
            rows[0]["source"], r#"{"source":"hello","osseq":7}"#,
            "{rows}"
        );
        assert!(rows[0]["osseq"].is_null(), "{rows}");
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
                            conform_doc(src, &schema, &conn.kept("bench"), &[], None, &[], &Indexed::default()).unwrap();
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
                ptr::null(),
                docs.len() as c_int,
            )
        };
        assert!(!p.is_null());
        p
    }

    fn append_replacing(h: *mut Bridge, p: *mut Prepared, replaced: &[&str]) -> Value {
        append_with_seqs(h, &[p], replaced, &[])
    }

    /// A prepared batch names the rows that do not conform, and the same
    /// rows sent again with the mapper's values conform: the columns hold
    /// those values, the id lookup answers the source as it was sent, and
    /// the check of one document reaches the same verdicts.
    #[test]
    fn a_refused_row_conforms_with_the_mapper_s_values() {
        let h = open_logs();
        let out = ensure(
            h,
            r#"{"properties":{"t":{"type":"date","format":"dd/MMM/yyyy:HH:mm:ss Z"}}}"#,
        );
        assert!(!out.contains("error"), "{out}");
        let plain = r#"{"level":"info","status":1}"#;
        let odd = r#"{"status":200.7,"t":"25/Nov/2026:09:30:00 +0100"}"#;
        let values = r#"{"status":[200],"t":[1795595400000]}"#;
        let framed = format!("{odd}{MAPPED_VALUES}{values}");

        assert!(!check(h, odd).is_empty());
        assert_eq!(check(h, &framed), "");

        let p = prepared(h, &[("a", plain), ("b", odd)]);
        let failed: Value =
            serde_json::from_str(&take(unsafe { infino_prepared_failed(p) })).unwrap();
        let failed = failed.as_array().unwrap();
        assert_eq!(failed.len(), 1, "{failed:?}");
        assert_eq!(failed[0]["i"], 1);
        assert!(
            failed[0]["reason"].as_str().unwrap().starts_with("field ["),
            "{failed:?}"
        );
        unsafe { infino_free_prepared(p) };
        assert!(unsafe { infino_prepared_failed(ptr::null()) }.is_null());

        let p = prepared(h, &[("a", plain), ("b", &framed)]);
        assert_eq!(take(unsafe { infino_prepared_failed(p) }), "[]");
        let out = append_replacing(h, p, &[]);
        assert_eq!(out["ok"], 2, "{out}");
        assert_eq!(get(h, "b"), odd);
        let at = sql(
            h,
            r#"SELECT count(*) AS c FROM "logs" WHERE status = 200 AND t = to_timestamp_millis(1795595400000)"#,
        );
        assert_eq!(at[0]["c"], 1, "{at}");

        // The batch path of one call stores the source as it was sent too.
        assert_eq!(index(h, &[("c", &framed)])["ok"], 1);
        assert_eq!(get(h, "c"), odd);
        close(h);
    }

    /// The mapper's values stand in for the source's only where the source
    /// gives as many: values it read from elsewhere too (a `copy_to`), or
    /// in place of a null (a `null_value`), leave the source's as they are.
    #[test]
    fn the_mapper_s_values_stand_in_only_where_the_source_gives_as_many() {
        let h = open_logs();
        let out = ensure(h, r#"{"properties":{"n":{"type":"long"}}}"#);
        assert!(!out.contains("error"), "{out}");
        // One value read where the source gives one: the mapper's is held.
        let one = format!(r#"{{"n":2.5}}{MAPPED_VALUES}{{"n":[2]}}"#);
        assert_eq!(check(h, &one), "");
        // Two read where the source gives one: the source's stands, and is refused as it is.
        let more = format!(r#"{{"n":2.5}}{MAPPED_VALUES}{{"n":[2,7]}}"#);
        assert!(!check(h, &more).is_empty());
        // As many as the source gives, an array of arrays counted at any depth.
        let nested = format!(r#"{{"n":[[1.5,null],[2.5]]}}{MAPPED_VALUES}{{"n":[1,2]}}"#);
        assert_eq!(check(h, &nested), "");
        // The mapper read a value where the source gives none: nothing to stand in for.
        let none = format!(r#"{{"n":null,"level":"x"}}{MAPPED_VALUES}{{"n":[0]}}"#);
        assert_eq!(check(h, &none), "");
        let given = |v: Value| given_values(&v);
        assert_eq!(given(json!(null)), 0);
        assert_eq!(given(json!([null, 1, [2, null, [3]]])), 3);
        assert_eq!(given(json!("x")), 1);
        close(h);
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
        assert_eq!(failed[0]["permanent"], true, "{out}");
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

    /// A document the engine refuses for what it carries (here a path
    /// beneath a live column, which the columns' check does not see) is
    /// reported as permanent, so the plugin drops it rather than trying it
    /// at every commit; the rest of its batch lands.
    #[test]
    fn a_document_the_engine_refuses_for_its_content_is_reported_permanent() {
        let h = open_logs();
        let out = index(h, &[("s1", r#"{"level":"info","status":1,"rec":"x"}"#)]);
        assert_eq!(out["ok"], 1, "{out}");
        let p = prepared(
            h,
            &[
                ("s2", r#"{"level":"info","status":2}"#),
                ("s3", r#"{"level":"info","status":3,"rec":{"n":1}}"#),
            ],
        );
        let out = append_replacing(h, p, &[]);
        assert_eq!(out["ok"], 1, "{out}");
        let failed = out["failed"].as_array().cloned().unwrap_or_default();
        assert_eq!(failed.len(), 1, "{out}");
        assert_eq!(failed[0]["i"], 1, "{out}");
        assert_eq!(failed[0]["permanent"], true, "{out}");
        assert!(get(h, "s2").contains("\"status\":2"), "{}", get(h, "s2"));
        assert!(get(h, "s3").is_empty(), "{}", get(h, "s3"));
        close(h);
    }

    /// A replace the engine refuses for one document's content is split by
    /// id until that document stands alone: it is reported as permanent
    /// and keeps its old row, and the other ids of the replace are replaced.
    #[test]
    fn a_replace_refused_for_one_document_replaces_the_others() {
        let h = open_logs();
        let out = index(
            h,
            &[
                ("t1", r#"{"level":"info","status":1,"rec":"x"}"#),
                ("t2", r#"{"level":"info","status":2}"#),
                ("t3", r#"{"level":"info","status":3}"#),
            ],
        );
        assert_eq!(out["ok"], 3, "{out}");
        let p = prepared(
            h,
            &[
                ("t1", r#"{"level":"warn","status":11}"#),
                ("t2", r#"{"level":"warn","status":22,"rec":{"n":1}}"#),
                ("t3", r#"{"level":"warn","status":33}"#),
            ],
        );
        let out = append_replacing(h, p, &["t1", "t2", "t3"]);
        assert_eq!(out["ok"], 2, "{out}");
        let failed = out["failed"].as_array().cloned().unwrap_or_default();
        assert_eq!(failed.len(), 1, "{out}");
        assert_eq!(failed[0]["i"], 1, "{out}");
        assert_eq!(failed[0]["permanent"], true, "{out}");
        assert!(get(h, "t1").contains("\"status\":11"), "{}", get(h, "t1"));
        assert!(get(h, "t2").contains("\"status\":2"), "{}", get(h, "t2"));
        assert!(get(h, "t3").contains("\"status\":33"), "{}", get(h, "t3"));
        for id in ["t1", "t2", "t3"] {
            let rows = sql(h, &format!("SELECT osid FROM logs WHERE osid = '{id}'"));
            assert_eq!(rows.as_array().map(|r| r.len()), Some(1), "{rows}");
        }
        close(h);
    }

    /// A write of an id whose earlier version the table refused replaces a
    /// row the table does not hold; it is added as new, beside the replace
    /// of the ids the table does hold, and no id ends up with two rows.
    #[test]
    fn a_write_of_an_id_whose_earlier_version_was_refused_lands_as_new() {
        let h = open_logs();
        let out = index(h, &[("t1", r#"{"level":"info","status":1,"rec":"x"}"#)]);
        assert_eq!(out["ok"], 1, "{out}");
        let out = index(h, &[("t2", r#"{"level":"info","status":2,"rec":{"n":1}}"#)]);
        assert_eq!(out["ok"], 0, "{out}");
        assert_eq!(out["failed"][0]["permanent"], true, "{out}");
        let p = prepared(
            h,
            &[
                ("t1", r#"{"level":"warn","status":11}"#),
                ("t2", r#"{"level":"warn","status":22}"#),
            ],
        );
        let out = append_replacing(h, p, &["t1", "t2"]);
        assert_eq!(out["ok"], 2, "{out}");
        assert!(out["failed"].as_array().is_none_or(Vec::is_empty), "{out}");
        assert!(get(h, "t1").contains("\"status\":11"), "{}", get(h, "t1"));
        assert!(get(h, "t2").contains("\"status\":22"), "{}", get(h, "t2"));
        for id in ["t1", "t2"] {
            let rows = sql(h, &format!("SELECT osid FROM logs WHERE osid = '{id}'"));
            assert_eq!(rows.as_array().map(|r| r.len()), Some(1), "{rows}");
        }
        close(h);
    }

    /// An id the table holds twice (a write that landed twice, #151) is
    /// written, not refused: its rows give way to the one row its document
    /// brings, beside an id with one row, which is replaced. Refused, it was
    /// refused on every try. (The engine compares the rows matched with the
    /// rows brought in total, so an id with none beside it would hide the
    /// miscount.)
    #[test]
    fn a_replace_of_an_id_the_table_holds_twice_leaves_one_row() {
        let h = open_logs();
        for _ in 0..2 {
            let out = index(h, &[("d1", r#"{"level":"info","status":1}"#)]);
            assert_eq!(out["ok"], 1, "{out}");
        }
        let out = index(h, &[("d2", r#"{"level":"info","status":2}"#)]);
        assert_eq!(out["ok"], 1, "{out}");
        let rows = sql(h, "SELECT osid FROM logs WHERE osid = 'd1'");
        assert_eq!(rows.as_array().map(|r| r.len()), Some(2), "{rows}");
        let p = prepared(
            h,
            &[
                ("d1", r#"{"level":"warn","status":11}"#),
                ("d2", r#"{"level":"warn","status":22}"#),
            ],
        );
        let out = append_replacing(h, p, &["d1", "d2"]);
        assert_eq!(out["ok"], 2, "{out}");
        assert!(out["failed"].as_array().is_none_or(Vec::is_empty), "{out}");
        for (id, status) in [("d1", 11), ("d2", 22)] {
            let rows = sql(
                h,
                &format!("SELECT osid, status FROM logs WHERE osid = '{id}'"),
            );
            assert_eq!(rows.as_array().map(|r| r.len()), Some(1), "{rows}");
            assert_eq!(rows[0]["status"], status, "{rows}");
        }
        close(h);
    }

    /// The hosted service answers a replace whose ids match another number
    /// of rows than it brings with a plain status, which the engine passes
    /// on untyped; it is checked against the counts as the local miscount is.
    /// A refusal of a row's content, or of the field cap, is not.
    #[test]
    fn a_hosted_refusal_of_a_replace_is_checked_against_the_counts() {
        let hosted = InfinoError::Backend(
            "update: server returned 400: mutation error: cardinality: update: cardinality \
             mismatch: predicate matched 72 rows; new_rows has 64"
                .into(),
        );
        assert!(counts_may_explain(&hosted));
        assert!(counts_may_explain(&InfinoError::Cardinality(
            "mismatch".into()
        )));
        let h = open_logs();
        index(h, &[("s1", r#"{"level":"info","status":1,"rec":"x"}"#)]);
        let refused = {
            let conn = bridge_ref(h).unwrap();
            let table = conn.table("logs").unwrap();
            let pred = col(OSID).in_list(vec![lit("s1")], false);
            table
                .update_rows(
                    pred,
                    &[json!({"osid": "s1", "level": "info", "rec": {"n": 1}})],
                )
                .expect_err("a path beneath a live column")
        };
        assert!(!counts_may_explain(&refused), "{refused}");
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
        let v: Value = serde_json::from_str(&out).unwrap_or_else(|_| panic!("{out}"));
        assert!(v.get("doc").is_some(), "{out}");
        v["doc"].clone()
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

    /// The hosted service in front of a typed schema refusal: a 400 whose
    /// body is the engine's error under the operation's name.
    fn hosted(typed: SchemaError) -> InfinoError {
        InfinoError::Backend(format!(
            "append: server returned 400: append error: {}",
            InfinoError::Schema(typed)
        ))
    }

    /// The hosted service answers a schema refusal untyped; it is read as
    /// the local engine's typed refusal is: a value the column cannot hold
    /// is a refusal of the row's content, while a schema that changed under
    /// the write, a column still converting and the field cap are for a
    /// later try, and the field cap is told apart.
    #[test]
    fn a_hosted_schema_refusal_is_read_as_the_local_one() {
        let cases = [
            SchemaError::TypeMismatch {
                column: "tags".into(),
                frozen: DataType::LargeUtf8,
                offered: DataType::List(std::sync::Arc::new(Field::new(
                    "item",
                    DataType::LargeUtf8,
                    true,
                ))),
            },
            SchemaError::NestedArray { path: "a.b".into() },
            SchemaError::PathShadowsColumn {
                path: "rec.n".into(),
                column: "rec".into(),
            },
            SchemaError::SchemaConflict {
                expected: 3,
                current: 4,
            },
            SchemaError::ConversionInProgress { column: "n".into() },
            SchemaError::FieldCapExceeded {
                cap: 10,
                current: 10,
                fields: vec!["x".into()],
            },
        ];
        for typed in cases {
            let local = InfinoError::Schema(typed.clone());
            let remote = hosted(typed);
            assert_eq!(
                refuses_content(&remote),
                refuses_content(&local),
                "{remote}"
            );
            assert_eq!(over_field_cap(&remote), over_field_cap(&local), "{remote}");
            assert_eq!(
                counts_may_explain(&remote),
                counts_may_explain(&local),
                "{remote}"
            );
        }
        // What the service answered for an array into a string column.
        let seen = InfinoError::Backend(
            "append: server returned 400: append error: schema: column `tags` is `LargeUtf8` \
             but the batch carries `List(LargeUtf8)`; apply the schema with the column's type \
             changed to store it"
                .into(),
        );
        assert!(refuses_content(&seen));
        // A name from the document that repeats a retryable message's words
        // leaves the refusal what its shape says it is.
        for named in [
            "a; re-read it and resubmit",
            "x` is still converting; run optimize before changing its type again",
            "y would take the table over its cap of 1 fields",
        ] {
            let remote = hosted(SchemaError::TypeMismatch {
                column: named.into(),
                frozen: DataType::LargeUtf8,
                offered: DataType::Int64,
            });
            assert!(refuses_content(&remote), "{remote}");
            assert!(!over_field_cap(&remote), "{remote}");
        }
        let capped = hosted(SchemaError::FieldCapExceeded {
            cap: 1,
            current: 1,
            fields: vec!["a; re-read it and resubmit`".into()],
        });
        assert!(!refuses_content(&capped), "{capped}");
        assert!(over_field_cap(&capped), "{capped}");
        // Not a schema refusal, or not a 400: a later try may pass.
        for other in [
            "update: server returned 400: mutation error: cardinality: update: cardinality \
             mismatch: predicate matched 72 rows; new_rows has 64",
            "append: server returned 500: append error: schema: column `tags` is `LargeUtf8`",
            "append: server returned 503: upstream unavailable",
            "update: server returned 400: mutation error: query: no row under osid 'k error: \
             schema: column `x` is `Int64` but the batch carries `LargeUtf8`; apply the schema \
             with the column's type changed to store it'",
        ] {
            let e = InfinoError::Backend(other.into());
            assert!(!refuses_content(&e), "{e}");
            assert!(!over_field_cap(&e), "{e}");
        }
    }

    /// One document the hosted service refuses for its content stands
    /// alone, reported permanent, and the rest of its append is written.
    #[test]
    fn a_document_the_hosted_service_refuses_is_isolated_and_permanent() {
        let rows: Vec<(usize, Value)> = vec![
            (0, json!({"osid": "1", "tags": "x"})),
            (1, json!({"osid": "2", "tags": ["x", "y"]})),
            (2, json!({"osid": "3", "tags": "z"})),
            (3, json!({"osid": "4", "tags": "w"})),
        ];
        let written = std::cell::RefCell::new(Vec::new());
        let mut failed = Vec::new();
        write_checked(&rows, &mut failed, &|values| {
            if values.iter().any(|v| v["tags"].is_array()) {
                return Err(InfinoError::Backend(
                    "append: server returned 400: append error: schema: column `tags` is \
                     `LargeUtf8` but the batch carries `List(LargeUtf8)`; apply the schema with \
                     the column's type changed to store it"
                        .into(),
                ));
            }
            written.borrow_mut().extend(
                values
                    .iter()
                    .map(|v| v["osid"].as_str().unwrap().to_string()),
            );
            Ok(())
        })
        .expect("a refused document is isolated; only an unavailable service ends the write");
        assert_eq!(failed.len(), 1, "{failed:?}");
        assert_eq!(failed[0]["i"], 1);
        assert_eq!(failed[0]["permanent"], true);
        let mut written = written.into_inner();
        written.sort();
        assert_eq!(written, ["1", "3", "4"]);
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
        // Another commit may join them: the reason is of this commit's making.
        assert_eq!(failed[0]["permanent"], false, "{out}");
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

    /// Ids are cut into runs whose cost fits the budget, in order and with
    /// none left out; an id past the budget alone is a run of its own.
    #[test]
    fn ids_are_cut_into_runs_that_fit_the_budget() {
        let ids: Vec<String> = (0..10).map(|i| format!("id-{i}")).collect();
        let per = id_cost("id-0");
        let runs = id_runs(&ids, 3 * per);
        assert_eq!(
            runs.iter().map(|r| r.len()).collect::<Vec<_>>(),
            vec![3, 3, 3, 1]
        );
        assert_eq!(runs.concat(), ids);
        assert_eq!(id_runs(&ids, usize::MAX).len(), 1);
        let long = vec!["x".repeat(1000), "y".to_string()];
        assert_eq!(id_runs(&long, 10).len(), 2);
        assert!(id_runs::<String>(&[], 10).is_empty());
    }

    /// A transport error names the whole request URL, whose query holds
    /// every id of the write. The reason a document is reported with leaves
    /// the query out and stays short, however many ids the write named; a
    /// short reason is kept as it is.
    #[test]
    fn a_refused_write_s_reason_leaves_out_the_url_query_and_stays_short() {
        let ids: Vec<String> = (0..5_000).map(|i| format!("%27doc-{i:06}%27")).collect();
        let url = format!(
            "https://api.example.test/v1/update/db?table=os_t&predicate=osid+IN+%28{}%29",
            ids.join("%2C+")
        );
        let e = InfinoError::Backend(format!(
            "update: transport error: {url}: Connection Failed: Broken pipe (os error 32)"
        ));
        let reason = short_reason(format_args!("replace: {e}"));
        assert!(
            reason.len() <= REASON_MAX_BYTES + 64,
            "{} bytes",
            reason.len()
        );
        assert!(!reason.contains("predicate="), "{reason}");
        assert!(
            reason.contains("https://api.example.test/v1/update/db?<query left out>:"),
            "{reason}"
        );
        assert!(reason.contains("Broken pipe"), "{reason}");
        let plain = "replace: backend: update: server returned 413: ";
        assert_eq!(short_reason(plain), plain);
        // A long reason with no URL is cut on a character boundary.
        let wide = "é".repeat(2 * REASON_MAX_BYTES);
        let cut = short_reason(&wide);
        assert!(
            cut.len() <= REASON_MAX_BYTES + 64 && cut.ends_with("bytes left out)"),
            "{cut}"
        );
    }

    /// Only a 413 is a refusal for size: a transport error, which an outage
    /// gives too, is not split on.
    #[test]
    fn only_a_413_is_a_refusal_for_size() {
        assert!(refused_for_size(&InfinoError::Backend(
            "update: server returned 413: ".into()
        )));
        assert!(!refused_for_size(&InfinoError::Backend(
            "update: transport error: https://h/v1/update/db?x: Connection Failed".into()
        )));
        assert!(!refused_for_size(&InfinoError::Backend(
            "update: server returned 400: x".into()
        )));
    }

    /// A replace of more ids than one call may name goes a run at a time,
    /// and every document lands; so do documents whose ids hold several rows,
    /// which are deleted and appended a run at a time.
    #[test]
    fn a_replace_of_more_ids_than_one_call_names_lands_a_run_at_a_time() {
        let h = open_logs();
        let docs: Vec<(String, String)> = (0..40)
            .map(|i| {
                (
                    format!("d{i:02}"),
                    format!(r#"{{"level":"info","message":"old {i}"}}"#),
                )
            })
            .collect();
        let refs: Vec<(&str, &str)> = docs.iter().map(|(a, b)| (a.as_str(), b.as_str())).collect();
        let out = index(h, &refs);
        assert_eq!(out["ok"], 40, "{out}");
        let conn = bridge_ref(h).expect("bridge");
        let table = conn.table("logs").expect("table");
        let gate = Gate::default();
        // Four ids per call: ten runs.
        let budget = 4 * id_cost("d00");
        let ids: Vec<String> = (0..40).map(|i| format!("d{i:02}")).collect();
        assert_eq!(id_runs(&ids, budget).len(), 10);
        let rows: Vec<(usize, Value)> = (0..40)
            .map(|i| (i, json!({ OSID: format!("d{i:02}"), "level": "warn", "message": format!("new {i}") })))
            .collect();
        let mut failed = Vec::new();
        assert!(replace_rows_checked(&table, &gate, &rows, &mut failed, budget).is_ok());
        assert!(failed.is_empty(), "{failed:?}");
        let rows_now = sql(h, "SELECT COUNT(*) AS c FROM logs");
        assert_eq!(rows_now[0]["c"], 40, "{rows_now}");
        let replaced = sql(h, "SELECT COUNT(*) AS c FROM logs WHERE level = 'warn'");
        assert_eq!(replaced[0]["c"], 40, "{replaced}");

        // Ids holding two rows each: the replace miscounts, and the rows are
        // rewritten (deleted, then appended) a run at a time.
        let twice: Vec<Value> = (0..12)
            .flat_map(|i| {
                let id = format!("t{i:02}");
                [
                    json!({ OSID: id.clone(), "level": "a" }),
                    json!({ OSID: id, "level": "b" }),
                ]
            })
            .collect();
        table.append_rows(&twice).expect("append twice");
        let ids: Vec<String> = (0..12).map(|i| format!("t{i:02}")).collect();
        assert_eq!(id_runs(&ids, budget).len(), 3);
        let rows: Vec<(usize, Value)> = (0..12)
            .map(|i| (i, json!({ OSID: format!("t{i:02}"), "level": "one" })))
            .collect();
        let mut failed = Vec::new();
        assert!(replace_rows_checked(&table, &gate, &rows, &mut failed, budget).is_ok());
        assert!(failed.is_empty(), "{failed:?}");
        let ones = sql(h, "SELECT COUNT(*) AS c FROM logs WHERE osid LIKE 't%'");
        assert_eq!(ones[0]["c"], 12, "{ones}");
        let ones = sql(h, "SELECT COUNT(*) AS c FROM logs WHERE level = 'one'");
        assert_eq!(ones[0]["c"], 12, "{ones}");
        close(h);
    }

    /// A replace through prepared batches whose replaced ids are more than
    /// one call may name skips the one Arrow update and replaces a run at a
    /// time: every document lands over storage of the bridge's own, and the
    /// hosted service takes one update per run, never one naming every id.
    #[test]
    fn a_prepared_replace_of_more_ids_than_one_call_names_goes_a_run_at_a_time() {
        let h = open_logs();
        let old: Vec<(String, String)> = (0..FAKE_ROWS)
            .map(|i| (fake_id(i), r#"{"level":"info"}"#.to_string()))
            .collect();
        let refs: Vec<(&str, &str)> = old.iter().map(|(a, b)| (a.as_str(), b.as_str())).collect();
        assert_eq!(
            index(h, &refs)["ok"],
            FAKE_ROWS,
            "every document lands first"
        );
        let new: Vec<(String, String)> = (0..FAKE_ROWS)
            .map(|i| {
                (
                    fake_id(i),
                    format!(r#"{{"level":"warn","message":"new {i}"}}"#),
                )
            })
            .collect();
        let refs: Vec<(&str, &str)> = new.iter().map(|(a, b)| (a.as_str(), b.as_str())).collect();
        let ids: Vec<String> = (0..FAKE_ROWS).map(fake_id).collect();
        let budget = FAKE_IDS_PER_RUN * id_cost(&fake_id(0));
        let runs = id_runs(&ids, budget).len();
        assert!(runs > 1, "{runs} runs");
        let take_prepared = || *unsafe { Box::from_raw(prepared(h, &refs)) };

        let conn = bridge_ref(h).expect("bridge");
        let table = conn.table("logs").expect("table");
        let failed = append_prepared(
            &table,
            &Gate::default(),
            vec![take_prepared()],
            &ids,
            &[],
            budget,
        )
        .expect("nothing finds the service unavailable");
        assert!(failed.is_empty(), "{failed:?}");
        let count = sql(h, "SELECT COUNT(*) AS c FROM logs");
        assert_eq!(count[0]["c"], FAKE_ROWS, "{count}");
        let warn = sql(h, "SELECT COUNT(*) AS c FROM logs WHERE level = 'warn'");
        assert_eq!(warn[0]["c"], FAKE_ROWS, "{warn}");

        let (hosted, calls) = fake_hosted(Answer::Lands, usize::MAX);
        let failed = append_prepared(
            &hosted,
            &Gate::default(),
            vec![take_prepared()],
            &ids,
            &[],
            budget,
        )
        .expect("nothing finds the service unavailable");
        assert!(failed.is_empty(), "{failed:?}");
        assert_eq!(ops(&calls), vec!["update"; runs]);
        close(h);
    }

    /// A replace whose run the hosted service could not take (a 503, or a
    /// connection that closes unanswered, as one does past a timeout: both
    /// [`InfinoError::Unavailable`]) stops there: one call in all, no count
    /// of rows, no document blamed: each is reported as not answered (503)
    /// for the caller to send again, and the error is returned. Any other failure, a 500 among them, is
    /// about the run's rows or may pass next time, and the runs after it go
    /// on.
    #[test]
    fn a_replace_stops_at_the_first_run_the_service_could_not_take() {
        let rows = fake_rows();
        let budget = FAKE_IDS_PER_RUN * id_cost(&fake_id(0));
        for answer in [Answer::Status(STATUS_UNAVAILABLE), Answer::Hangup] {
            let (table, calls) = fake_hosted(answer, usize::MAX);
            let mut failed = Vec::new();
            let out = replace_rows_checked(&table, &Gate::default(), &rows, &mut failed, budget);
            assert!(matches!(out, Err(InfinoError::Unavailable(_))), "{out:?}");
            assert_eq!(ops(&calls), vec!["update"]);
            assert_unanswered(&failed, 0..FAKE_ROWS as u64);
        }
        let runs = FAKE_ROWS / FAKE_IDS_PER_RUN;
        let (table, calls) = fake_hosted(Answer::Status(STATUS_SERVER_ERROR), usize::MAX);
        let mut failed = Vec::new();
        assert!(
            replace_rows_checked(&table, &Gate::default(), &rows, &mut failed, budget).is_ok(),
            "{failed:?}"
        );
        let updates = ops(&calls).iter().filter(|op| **op == "update").count();
        assert_eq!(updates, runs, "every run is tried: {:?}", ops(&calls));
        assert_all_not_taken(&failed);
    }

    /// The same for a rewrite: after the hosted service could not take one
    /// run's delete, no later run is deleted or appended, every document is
    /// reported as not answered (503), and the error is returned.
    #[test]
    fn a_rewrite_stops_at_the_first_delete_the_service_could_not_take() {
        let rows = fake_rows();
        let budget = FAKE_IDS_PER_RUN * id_cost(&fake_id(0));
        let (table, calls) = fake_hosted(Answer::Status(STATUS_UNAVAILABLE), usize::MAX);
        let mut failed = Vec::new();
        let out = rewrite_rows(&table, &Gate::default(), &rows, &mut failed, budget);
        assert!(matches!(out, Err(InfinoError::Unavailable(_))), "{out:?}");
        assert_eq!(ops(&calls), vec!["delete"]);
        assert_unanswered(&failed, 0..FAKE_ROWS as u64);
    }

    /// Over the hosted service, a replace of more ids than
    /// [`HOSTED_ID_LIST_BYTES`] names one run of them per call, each URL
    /// within that budget (so [`id_cost`] is an upper bound, quotes inside
    /// an id included), and the service refuses none for its size. A replace
    /// it does refuse with 413 is split in halves, which land.
    #[test]
    fn a_hosted_replace_names_one_run_of_ids_per_call_and_splits_on_a_413() {
        let ids: Vec<String> = (0..LONG_ROWS)
            .map(|i| {
                let quote = if i % 2 == 0 { "'" } else { " " };
                format!("{i:04}{quote}{}", "x".repeat(LONG_ID_BYTES))
            })
            .collect();
        let rows: Vec<(usize, Value)> = ids
            .iter()
            .enumerate()
            .map(|(i, id)| (i, json!({ OSID: id, "level": "warn" })))
            .collect();
        let runs = id_runs(&ids, HOSTED_ID_LIST_BYTES).len();
        assert!(runs > 1, "{runs} runs");
        let limit = HOSTED_ID_LIST_BYTES + URL_HEAD_BYTES;
        let (table, calls) = fake_hosted(Answer::Lands, limit);
        let mut failed = Vec::new();
        assert!(replace_rows_checked(
            &table,
            &Gate::default(),
            &rows,
            &mut failed,
            HOSTED_ID_LIST_BYTES
        )
        .is_ok());
        assert!(failed.is_empty(), "{failed:?}");
        assert_eq!(ops(&calls), vec!["update"; runs]);
        let longest = calls.lock().unwrap().iter().map(|(_, len)| *len).max();
        assert!(longest.is_some_and(|len| len <= limit), "{longest:?}");

        // One call naming a run's ids, then the same call to a service that
        // refuses anything that long: it goes in halves.
        let run = &rows[..rows.len() / runs];
        let (table, calls) = fake_hosted(Answer::Lands, usize::MAX);
        let mut failed = Vec::new();
        assert!(
            replace_rows_checked(&table, &Gate::default(), run, &mut failed, usize::MAX).is_ok()
        );
        let whole = calls.lock().unwrap()[0].1;
        let (table, calls) = fake_hosted(Answer::Lands, whole - 1);
        assert!(
            replace_rows_checked(&table, &Gate::default(), run, &mut failed, usize::MAX).is_ok()
        );
        assert!(failed.is_empty(), "{failed:?}");
        assert_eq!(ops(&calls), vec!["update"; 3]);
    }

    /// Rows a fake hosted service's tests replace.
    const FAKE_ROWS: usize = 12;
    /// Ids of [`fake_id`]'s length one call names in those tests.
    const FAKE_IDS_PER_RUN: usize = 4;
    /// Rows of ids long enough that a replace of them is past one call's list.
    const LONG_ROWS: usize = 400;
    /// The filler that makes each of those ids long.
    const LONG_ID_BYTES: usize = 40;
    /// What a hosted request's target holds besides the predicate's ids: its
    /// path, the table, the predicate's column and parentheses.
    const URL_HEAD_BYTES: usize = 256;
    /// HTTP status of a service that cannot serve the call now.
    const STATUS_UNAVAILABLE: u16 = 503;
    /// HTTP status of a service that failed the call itself.
    const STATUS_SERVER_ERROR: u16 = 500;
    /// The table a fake hosted service serves.
    const FAKE_TABLE: &str = "t";
    /// The key a test connects to a fake hosted service with.
    const FAKE_KEY: &str = "ik_test";
    /// The bytes that end an HTTP request's head.
    const HEAD_END: &[u8] = b"\r\n\r\n";
    /// How many bytes a fake hosted service reads at a time.
    const READ_CHUNK: usize = 8 * 1024;

    /// The `i`th id of a fake hosted service's tests, all of one length.
    fn fake_id(i: usize) -> String {
        format!("doc-{i:03}")
    }

    /// [`FAKE_ROWS`] documents replacing the rows of [`fake_id`]s.
    fn fake_rows() -> Vec<(usize, Value)> {
        (0..FAKE_ROWS)
            .map(|i| (i, json!({ OSID: fake_id(i), "level": "warn" })))
            .collect()
    }

    /// Every one of [`FAKE_ROWS`] documents is reported once, for a later try.
    fn assert_all_not_taken(failed: &[Value]) {
        let mut seen: Vec<u64> = failed.iter().filter_map(|f| f["i"].as_u64()).collect();
        seen.sort_unstable();
        assert_eq!(
            seen,
            (0..FAKE_ROWS as u64).collect::<Vec<_>>(),
            "{failed:?}"
        );
        assert!(failed.iter().all(|f| f["permanent"] == false), "{failed:?}");
    }

    /// What a fake hosted service answers an update or a delete with.
    #[derive(Clone, Copy)]
    enum Answer {
        /// The mutation lands.
        Lands,
        /// The service fails it with this status.
        Status(u16),
        /// The connection closes with no answer, as a service that went away.
        Hangup,
    }

    /// The calls a fake hosted service took, as each one's operation and the
    /// length of its request target (its path and query).
    type Calls = Arc<Mutex<Vec<(String, usize)>>>;

    /// The operation of each call in `calls`, in order.
    fn ops(calls: &Calls) -> Vec<String> {
        calls
            .lock()
            .unwrap()
            .iter()
            .map(|(op, _)| op.clone())
            .collect()
    }

    /// A fake hosted service on a local port serving one table: it answers
    /// `list_tables` and `schema`, every `update` and `delete` with `answer`,
    /// or with 413 when its request target is longer than `max_target`, and
    /// anything else with 404. Returns the table, opened through the
    /// engine's hosted client, and the calls taken after it opened.
    fn fake_hosted(answer: Answer, max_target: usize) -> (Supertable, Calls) {
        let schema = Schema::new(vec![
            Field::new(OSID, DataType::Utf8, true),
            Field::new("level", DataType::Utf8, true),
            Field::new("message", DataType::Utf8, true),
        ]);
        let mut ipc = Vec::new();
        StreamWriter::try_new(&mut ipc, &schema)
            .and_then(|mut w| w.finish())
            .unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let uri = format!("http://{}/db", listener.local_addr().unwrap());
        let calls: Calls = Arc::default();
        let log = Arc::clone(&calls);
        thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(mut stream) = stream else {
                    return;
                };
                let Some(target) = read_request(&mut stream) else {
                    continue;
                };
                let op = target
                    .trim_start_matches("/v1/")
                    .split('/')
                    .next()
                    .unwrap_or("")
                    .to_string();
                log.lock().unwrap().push((op.clone(), target.len()));
                let lands = json!({ "matched": 0, "n_tombstoned": 0, "n_not_found": 0 });
                let (status, body) = match op.as_str() {
                    "list_tables" => (200, json!([FAKE_TABLE]).to_string().into_bytes()),
                    "schema" => (200, ipc.clone()),
                    "update" | "delete" if target.len() > max_target => {
                        (STATUS_PAYLOAD_TOO_LARGE, b"too large".to_vec())
                    }
                    "update" | "delete" => match answer {
                        Answer::Lands => (200, lands.to_string().into_bytes()),
                        Answer::Status(code) => (code, b"failing".to_vec()),
                        Answer::Hangup => continue,
                    },
                    _ => (STATUS_NOT_FOUND, Vec::new()),
                };
                let head = format!(
                    "HTTP/1.1 {status} X\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
                    body.len()
                );
                let _ = stream
                    .write_all(head.as_bytes())
                    .and_then(|()| stream.write_all(&body));
            }
        });
        let conn = connect_with(&uri, ConnectOptions::new().with_api_key(FAKE_KEY)).unwrap();
        let table = conn.open_table(FAKE_TABLE).unwrap();
        calls.lock().unwrap().clear();
        (table, calls)
    }

    /// Read one HTTP request off `stream`, its body included, and return its
    /// target; None when the stream ends first.
    fn read_request(stream: &mut TcpStream) -> Option<String> {
        let mut got = Vec::new();
        let mut buf = [0u8; READ_CHUNK];
        let head_end = loop {
            if let Some(at) = got.windows(HEAD_END.len()).position(|w| w == HEAD_END) {
                break at + HEAD_END.len();
            }
            let n = stream.read(&mut buf).ok().filter(|&n| n > 0)?;
            got.extend_from_slice(&buf[..n]);
        };
        let head = String::from_utf8_lossy(&got[..head_end]).to_string();
        let length: usize = head
            .lines()
            .filter_map(|line| line.split_once(':'))
            .find(|(name, _)| name.eq_ignore_ascii_case("content-length"))
            .and_then(|(_, value)| value.trim().parse().ok())
            .unwrap_or(0);
        let mut body = got.len() - head_end;
        while body < length {
            body += stream.read(&mut buf).ok().filter(|&n| n > 0)?;
        }
        head.split_whitespace().nth(1).map(str::to_string)
    }

    /// A whole write (a replace, a delete, a schema change) waits for the
    /// appends passing the gate, and an append that arrives while it waits
    /// or passes waits for it in turn.
    #[test]
    fn a_whole_write_passes_the_gate_alone() {
        use std::sync::mpsc;
        let gate = Gate::default();
        let append = gate.append();
        std::thread::scope(|s| {
            let g = &gate;
            let (whole_tx, whole_rx) = mpsc::channel();
            s.spawn(move || {
                let _whole = g.whole();
                whole_tx.send("whole").unwrap();
                std::thread::sleep(Duration::from_millis(100));
            });
            assert!(
                whole_rx.recv_timeout(Duration::from_millis(100)).is_err(),
                "a whole write passed beside an append"
            );
            while !g.lock().whole {
                std::thread::yield_now();
            }
            let (append_tx, append_rx) = mpsc::channel();
            s.spawn(move || {
                let _append = g.append();
                append_tx.send("append").unwrap();
            });
            assert!(
                append_rx.recv_timeout(Duration::from_millis(100)).is_err(),
                "an append passed a gate a whole write had claimed"
            );
            drop(append);
            assert_eq!(whole_rx.recv_timeout(Duration::from_secs(5)), Ok("whole"));
            assert_eq!(append_rx.recv_timeout(Duration::from_secs(5)), Ok("append"));
        });
    }

    /// Replaces and deletes of different ids, run at once against one
    /// table, all land. Each takes the table's writer whole, which the engine
    /// refuses at once to a second while the first holds it; the gate lines
    /// them up instead.
    #[test]
    fn replaces_and_deletes_of_different_ids_run_at_once_and_all_land() {
        let h = open_logs();
        let docs: Vec<(String, String)> = (0..16)
            .map(|i| {
                (
                    format!("d{i}"),
                    format!(r#"{{"level":"info","status":{i}}}"#),
                )
            })
            .collect();
        let pairs: Vec<(&str, &str)> = docs.iter().map(|(a, b)| (a.as_str(), b.as_str())).collect();
        let out = index(h, &pairs);
        assert_eq!(out["ok"], 16, "{out}");
        // The bridge is shared by every JVM thread; the pointer crosses as an address.
        let at = h as usize;
        std::thread::scope(|s| {
            for i in 0..8 {
                s.spawn(move || {
                    let h = at as *mut Bridge;
                    let id = format!("d{i}");
                    let p = prepared(h, &[(id.as_str(), r#"{"level":"warn","status":100}"#)]);
                    let out = append_replacing(h, p, &[id.as_str()]);
                    assert_eq!(out["ok"], 1, "{out}");
                });
            }
            for i in 8..16 {
                s.spawn(move || {
                    let h = at as *mut Bridge;
                    assert_eq!(delete(h, &[format!("d{i}").as_str()]), 0);
                });
            }
        });
        let rows = sql(h, "SELECT COUNT(*) AS c FROM logs WHERE status = 100");
        assert_eq!(rows[0]["c"], 8, "{rows}");
        let rows = sql(h, "SELECT COUNT(*) AS c FROM logs");
        assert_eq!(rows[0]["c"], 8, "{rows}");
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

    /// A node taking a shard over reads the rows the node before it
    /// committed through `infino_sql_latest` and `infino_get_latest` at
    /// once, where `infino_sql` and `infino_get` may still answer from the
    /// commit they last read: the probe that keeps a replayed operation from
    /// appending its row again, and the document an update merges into. The
    /// reading node shares its cache directory with that connection.
    #[test]
    fn a_latest_read_sees_another_nodes_commit_at_once() {
        let root = std::env::temp_dir().join(format!(
            "infino-capi-latest-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0)
        ));
        let data = root.join("data");
        std::fs::create_dir_all(&data).unwrap();
        let uri = c(&format!("file://{}", data.display()));
        let cache_dir = c(&root.join("cache").display().to_string());

        let writer = unsafe { infino_open(uri.as_ptr()) };
        assert!(!writer.is_null());
        assert!(ensure(writer, MAPPING).contains("\"created\":true"));
        index(writer, &[("a", r#"{"level":"info"}"#)]);

        let reader = unsafe { infino_open_with(uri.as_ptr(), ptr::null(), cache_dir.as_ptr()) };
        assert!(!reader.is_null());
        let count = "SELECT count(*) AS n FROM logs";
        let probe = "SELECT osid FROM token_match('logs', 'osid', 'b', 'or')";
        let latest = |q: &str| -> Value {
            let out = take(unsafe { infino_sql_latest(reader, c(q).as_ptr()) });
            serde_json::from_str(&out).unwrap_or_else(|_| panic!("{out}"))
        };
        // Taken before the read, so the window it measures starts no later
        // than the one the reader's connection opens when it reads.
        let read_at = Instant::now();
        assert_eq!(sql(reader, count)[0]["n"], 1);

        index(writer, &[("b", r#"{"level":"warn"}"#)]);
        let stale = sql(reader, count)[0]["n"].clone();
        let get = |id: &str| get_from(reader, "logs", id);
        let get_latest = |id: &str| {
            take(unsafe { infino_get_latest(reader, c("logs").as_ptr(), c(id).as_ptr()) })
        };
        let stale_get = get("b");
        // Within the engine's default window of the reader's last read, the
        // reader's own connection still answers from the first commit: what
        // makes the latest reads below more than reads like any other.
        if read_at.elapsed() < Duration::from_millis(900) {
            assert_eq!(
                stale, 1,
                "the default read answers from the commit it last read"
            );
            assert_eq!(
                stale_get, "",
                "the default get answers from the commit it last read"
            );
        }
        let cached = bytes_under(&root.join("cache"));
        assert_eq!(latest(count)[0]["n"], 2);
        assert_eq!(latest(probe), json!([{ "osid": "b" }]));
        let found: Value = serde_json::from_str(&get_latest("b")).unwrap();
        assert_eq!(
            found,
            json!({ "level": "warn" }),
            "the latest get finds the new id"
        );
        assert_eq!(get_latest("c"), "", "an id the table does not hold is \"\"");
        let docs: Value = serde_json::from_str(&docs_latest(reader, &["a", "b", "c"])).unwrap();
        let source: Value =
            serde_json::from_str(docs["docs"]["b"]["source"].as_str().unwrap()).unwrap();
        assert_eq!(
            source,
            json!({ "level": "warn" }),
            "the latest realtime get finds the new id with its source"
        );
        assert!(
            docs["docs"]["b"]["seq"].is_i64(),
            "and the sequence number it was written under: {docs}"
        );
        assert!(
            docs["docs"]["a"].is_object(),
            "and the id of the first commit: {docs}"
        );
        assert!(
            docs["docs"].get("c").is_none(),
            "an id the table does not hold is absent: {docs}"
        );
        // Only the latest connection has read the second commit, when the
        // reader's own read was stale; the blocks it read land in the cache.
        if stale == 1 {
            assert!(
                bytes_under(&root.join("cache")) > cached,
                "the latest read went through the node's cache directory"
            );
        }

        close(reader);
        close(writer);
        let _ = std::fs::remove_dir_all(&root);
    }

    /// A latest read of a table dropped and made again under its name reads
    /// the table made again, whether the drop was this node's or another's:
    /// the first write to a recreated index probes it before it holds a row.
    #[test]
    fn a_latest_read_finds_a_table_made_again_after_a_drop() {
        let root = std::env::temp_dir().join(format!(
            "infino-capi-latest-drop-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0)
        ));
        let data = root.join("data");
        std::fs::create_dir_all(&data).unwrap();
        let uri = c(&format!("file://{}", data.display()));
        let cache_dir = c(&root.join("cache").display().to_string());
        let open = || unsafe { infino_open_with(uri.as_ptr(), ptr::null(), cache_dir.as_ptr()) };
        let sql = |h: *mut Bridge, q: &str| -> Value {
            let out = take(unsafe { infino_sql_latest(h, c(q).as_ptr()) });
            serde_json::from_str(&out).unwrap_or_else(|_| panic!("{out}"))
        };
        let latest = |h: *mut Bridge| sql(h, "SELECT count(*) AS n FROM logs");
        // The probe for ids a write may replace reads through a search.
        let probe = |h: *mut Bridge| {
            sql(
                h,
                "SELECT osid FROM token_match('logs', 'osid', 'a b', 'or')",
            )
        };
        let node = open();
        let other = open();
        // Held, as a shard catching up holds them, so each keeps the handle
        // it read the first table through.
        unsafe {
            infino_hold_latest(node);
            infino_hold_latest(other);
        }
        assert!(ensure(node, MAPPING).contains("\"created\":true"));
        index(node, &[("a", r#"{"level":"info"}"#)]);
        assert_eq!(latest(node)[0]["n"], 1);
        assert_eq!(latest(other)[0]["n"], 1);
        assert_eq!(probe(other).as_array().map(Vec::len), Some(1));
        assert!(latest_open(other), "a held connection stays open");

        assert_eq!(unsafe { infino_drop(node, c("logs").as_ptr()) }, 0);
        assert!(ensure(node, MAPPING).contains("\"created\":true"));
        assert_eq!(latest(node)[0]["n"], 0, "this node's drop");
        assert_eq!(latest(other)[0]["n"], 0, "another node's drop");
        index(node, &[("b", r#"{"level":"info"}"#)]);
        assert_eq!(latest(other)[0]["n"], 1);
        // Dropped again: the other node's latest connection holds a handle
        // to the table from before, which a search must not answer from.
        assert_eq!(unsafe { infino_drop(node, c("logs").as_ptr()) }, 0);
        assert!(ensure(node, MAPPING).contains("\"created\":true"));
        index(node, &[("b", r#"{"level":"info"}"#)]);
        assert_eq!(
            probe(other)[0]["osid"],
            "b",
            "a search after another node's drop"
        );

        unsafe {
            infino_release_latest(other);
            infino_release_latest(node);
        }
        close(other);
        close(node);
        let _ = std::fs::remove_dir_all(&root);
    }

    /// Whether `h`'s latest connection is open.
    fn latest_open(h: *mut Bridge) -> bool {
        unsafe { &*h }
            .latest
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .is_some()
    }

    /// The latest connection stays open while a reader holds it and closes
    /// when the last hold goes, so a node keeps its second handle to a table
    /// only while a shard catches up; a read no one holds for closes it.
    #[test]
    fn the_latest_connection_closes_when_no_reader_holds_it() {
        let root = std::env::temp_dir().join(format!(
            "infino-capi-latest-hold-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0)
        ));
        let data = root.join("data");
        std::fs::create_dir_all(&data).unwrap();
        let uri = c(&format!("file://{}", data.display()));
        let h = unsafe { infino_open(uri.as_ptr()) };
        assert!(ensure(h, MAPPING).contains("\"created\":true"));
        index(h, &[("a", r#"{"level":"info"}"#)]);
        let read = || {
            let out =
                take(unsafe { infino_sql_latest(h, c("SELECT count(*) AS n FROM logs").as_ptr()) });
            let rows: Value = serde_json::from_str(&out).unwrap_or_else(|_| panic!("{out}"));
            assert_eq!(rows[0]["n"], 1);
        };
        let get = || take(unsafe { infino_get_latest(h, c("logs").as_ptr(), c("a").as_ptr()) });

        read();
        assert!(!latest_open(h), "a read no one holds for closes it");
        unsafe { infino_hold_latest(h) };
        read();
        assert!(latest_open(h));
        unsafe { infino_hold_latest(h) };
        unsafe { infino_release_latest(h) };
        assert_eq!(get(), r#"{"level":"info"}"#);
        assert!(latest_open(h), "open while one hold is left");
        unsafe { infino_release_latest(h) };
        assert!(!latest_open(h), "closed with the last hold");
        // A release with no hold to let go counts nothing, so the next hold
        // still keeps the connection open.
        unsafe { infino_release_latest(h) };
        unsafe { infino_hold_latest(h) };
        read();
        assert!(latest_open(h));
        unsafe { infino_release_latest(h) };
        assert!(!latest_open(h));

        close(h);
        let _ = std::fs::remove_dir_all(&root);
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

    /// The startup check of a node's own storage answers "" for a directory
    /// the node may write, and leaves nothing behind in it.
    #[test]
    fn the_storage_check_passes_a_writable_directory_and_leaves_it_as_it_was() {
        let root = scratch_dir("infino-capi-check-ok");
        let uri = c(&format!("file://{}", root.display()));
        let h = unsafe { infino_open(uri.as_ptr()) };
        assert!(!h.is_null());
        assert_eq!(
            take(unsafe { infino_check_storage(h) }),
            "",
            "a usable directory"
        );
        let left: Vec<_> = fs::read_dir(&root)
            .unwrap()
            .map(|e| e.unwrap().file_name())
            .collect();
        assert!(left.is_empty(), "the probe file is deleted: {left:?}");
        assert!(ensure(h, MAPPING).contains("\"created\":true"));
        close(h);
        let _ = fs::remove_dir_all(&root);
    }

    /// A directory the node may read but not write passes a listing, so only
    /// the write probe finds it; the failure is permanent, since it will not
    /// pass on its own.
    #[test]
    fn the_storage_check_fails_a_read_only_directory_for_good() {
        let root = scratch_dir("infino-capi-check-ro");
        fs::set_permissions(&root, fs::Permissions::from_mode(READ_ONLY_DIR)).unwrap();
        let uri = c(&format!("file://{}", root.display()));
        let h = unsafe { infino_open(uri.as_ptr()) };
        assert!(!h.is_null());
        let why = take(unsafe { infino_check_storage(h) });
        close(h);
        fs::set_permissions(&root, fs::Permissions::from_mode(WRITABLE_DIR)).unwrap();
        let _ = fs::remove_dir_all(&root);
        let why: Value = serde_json::from_str(&why).unwrap_or_else(|_| panic!("{why:?}"));
        let message = why["error"].as_str().unwrap_or_else(|| panic!("{why}"));
        assert!(
            message.to_lowercase().contains("permission denied"),
            "names the cause: {message}"
        );
        assert!(
            message.contains(&root.display().to_string()),
            "names the directory: {message}"
        );
        assert_eq!(why["permanent"], true, "{why}");
    }

    /// A local root is read from a URI as the engine reads it: `file://`
    /// and bare paths are local, the in-memory catalog and object stores
    /// are not.
    #[test]
    fn a_local_root_is_read_from_the_uri_as_the_engine_reads_it() {
        assert_eq!(
            local_root("file:///data/infino"),
            Some(PathBuf::from("/data/infino"))
        );
        assert_eq!(
            local_root("/data/infino"),
            Some(PathBuf::from("/data/infino"))
        );
        assert_eq!(local_root("./data"), Some(PathBuf::from("./data")));
        for not_local in [
            "memory://",
            "memory:",
            "memory",
            "s3://b/p",
            "gs://b",
            "az://c/p",
        ] {
            assert_eq!(local_root(not_local), None, "{not_local}");
        }
    }

    /// An `s3://` node whose instance metadata service accepts the
    /// connection but never answers, as in Docker on EC2 at a hop limit of 1,
    /// has no credentials: the check says so within its short wait, where
    /// the store client would retry for minutes. One that answers at all,
    /// even with an error status, lets the listing decide.
    #[test]
    fn an_instance_metadata_service_that_does_not_answer_means_no_credentials() {
        let silent = TcpListener::bind("127.0.0.1:0").unwrap();
        let base = format!("http://{}", silent.local_addr().unwrap());
        let started = Instant::now();
        let failure = check_instance_metadata(&base, SHORT_WAIT).unwrap_err();
        assert!(failure.reason.contains("no AWS credentials"), "{failure:?}");
        assert!(!failure.permanent, "the service can come back: {failure:?}");
        assert!(started.elapsed() < SHORT_WAIT * 3, "{:?}", started.elapsed());

        let answering = TcpListener::bind("127.0.0.1:0").unwrap();
        let base = format!("http://{}", answering.local_addr().unwrap());
        let server = thread::spawn(move || {
            let (mut stream, _) = answering.accept().unwrap();
            let mut request = [0u8; 1024];
            let _ = stream.read(&mut request);
            stream
                .write_all(b"HTTP/1.1 405 Method Not Allowed\r\ncontent-length: 0\r\n\r\n")
                .unwrap();
        });
        assert!(check_instance_metadata(&base, SHORT_WAIT).is_ok());
        server.join().unwrap();
    }

    /// Only a failure that will not pass on its own is permanent.
    #[test]
    fn only_a_refusal_of_the_local_root_is_permanent() {
        assert!(lasts(ErrorKind::PermissionDenied));
        assert!(lasts(ErrorKind::ReadOnlyFilesystem));
        assert!(!lasts(ErrorKind::TimedOut));
        assert!(!lasts(ErrorKind::Interrupted));
    }

    /// A new, empty directory under the system's temp directory.
    fn scratch_dir(prefix: &str) -> PathBuf {
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        let root = env::temp_dir().join(format!("{prefix}-{}-{stamp}", process::id()));
        fs::create_dir_all(&root).unwrap();
        root
    }

    /// A URI that names no store the engine knows fails to open, with the
    /// reason.
    #[test]
    fn a_uri_no_store_serves_fails_to_open_with_the_reason() {
        let uri = c("nosuchscheme://bucket/prefix");
        let h = unsafe { infino_open(uri.as_ptr()) };
        assert!(h.is_null());
        let why: Value = serde_json::from_str(&take(unsafe { infino_open_error() })).unwrap();
        assert!(
            why["error"].as_str().is_some_and(|e| !e.is_empty()),
            "{why}"
        );
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
    /// time, as the sample flights write it) included; what it refuses fails
    /// the document, rather than being left to the engine to read otherwise.
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
        // Any other form fails the document with OpenSearch's message.
        assert_eq!(
            conform(json!("2026-11-23 10:30:15"), &date_type()),
            Err("failed to parse date field [2026-11-23 10:30:15] with format [strict_date_optional_time||epoch_millis]".into())
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
        // A value only a custom pattern reads fails here: the mapper reads
        // it, and its value comes with the document.
        let custom = DateFormat::parse_all("yyyy-MM-dd HH:mm:ss||strict_date_optional_time");
        assert_eq!(
            conform_date(json!("2026-11-23 10:30:15"), &custom),
            Err("failed to parse date field [2026-11-23 10:30:15] with format [yyyy-MM-dd HH:mm:ss||strict_date_optional_time]".into())
        );
        assert_eq!(
            conform_date(json!("2026-11-23T10:30:15Z"), &custom),
            Ok(json!(1_795_429_815_000i64))
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
        let check = |doc: &str| conform_doc(doc, &schema, &kept, &[], Some("@timestamp"), &[], &Indexed::default());
        assert!(check(r#"{"@timestamp": "2023-01-01T00:00:01Z"}"#).is_ok());
        assert!(check(r#"{"@timestamp": ["2023-01-01T00:00:01Z", null]}"#).is_ok());
        for doc in [
            r#"{"message": "m"}"#,
            r#"{"@timestamp": null}"#,
            r#"{"@timestamp": ["2023-01-01T00:00:01Z", "2023-01-01T00:00:02Z"]}"#,
        ] {
            assert_eq!(check(doc), Err(no_timestamp("@timestamp")), "{doc}");
        }
        assert!(conform_doc(r#"{"message": "m"}"#, &schema, &kept, &[], None, &[], &Indexed::default()).is_ok());
    }

    /// A write the hosted service could not take is not split into halves:
    /// every half would meet the same service, and each would wait out its
    /// own timeout. One call, and the error ends the batch, every document
    /// reported as not answered (503). A write refused for what it carries
    /// is still split, down to the documents refused.
    #[test]
    fn a_write_to_an_unavailable_service_is_tried_once_not_once_per_document() {
        let rows: Vec<(usize, Value)> = (0..8).map(|i| (i, json!({ "n": i }))).collect();
        let calls = std::cell::Cell::new(0);
        let mut failed = Vec::new();
        let out = write_checked(&rows, &mut failed, &|_| {
            calls.set(calls.get() + 1);
            Err(InfinoError::Unavailable(
                "append: transport error: timeout: per read".into(),
            ))
        });
        assert!(matches!(out, Err(InfinoError::Unavailable(_))), "{out:?}");
        assert_eq!(calls.get(), 1, "one call for the whole batch");
        assert_unanswered(&failed, 0..8);

        calls.set(0);
        let mut failed = Vec::new();
        let out = write_checked(&rows, &mut failed, &|values| {
            calls.set(calls.get() + 1);
            if values.iter().any(|v| v["n"] == 3) {
                Err(InfinoError::Backend("refused".into()))
            } else {
                Ok(())
            }
        });
        assert!(out.is_ok());
        assert_eq!(failed.len(), 1, "{failed:?}");
        assert_eq!(failed[0]["i"], 3);
        assert!(
            calls.get() > 1,
            "a refusal is still narrowed down to its document"
        );
    }

    /// A prepare that cannot reach the hosted service returns null and
    /// leaves why, with status 503, in `infino_prepare_error`: the plugin
    /// answers the bulk with that instead of a generic failure. The service
    /// here answers the open's table listing, then closes every connection.
    #[test]
    fn a_prepare_against_an_unreachable_service_reports_503_and_why() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        std::thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(mut stream) = stream else { return };
                let (text, _) = read_head_and_body(&mut stream);
                if text.contains("/v1/list_tables/") {
                    let _ = std::io::Write::write_all(
                        &mut stream,
                        b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\n[]",
                    );
                }
            }
        });
        let uri = c(&format!("http://{addr}/db"));
        let key = c("ik_test");
        let h = unsafe { infino_open_with(uri.as_ptr(), key.as_ptr(), ptr::null()) };
        assert!(!h.is_null(), "the open lists the tables");

        let name = c("t");
        let buf = b"a{}";
        let id_ends = [1u32];
        let src_ends = [3u32];
        let p = unsafe {
            infino_prepare_batch(
                h,
                name.as_ptr(),
                buf.as_ptr(),
                buf.len(),
                id_ends.as_ptr(),
                src_ends.as_ptr(),
                ptr::null(),
                1,
            )
        };
        assert!(p.is_null());
        let why: Value = serde_json::from_str(&take(unsafe { infino_prepare_error() })).unwrap();
        assert_eq!(why["status"], 503, "{why}");
        let message = why["error"].as_str().unwrap();
        assert!(
            message.contains("could not be reached or did not answer in time"),
            "{message}"
        );
        assert!(unsafe { infino_prepare_error() }.is_null(), "read once");
        unsafe { infino_close(h) };
    }

    /// One HTTP request off `stream`: its head, lowercased, and its body.
    /// The body is read too: closing a socket with unread bytes resets it,
    /// and the client sees no answer at all.
    fn read_head_and_body(stream: &mut std::net::TcpStream) -> (String, Vec<u8>) {
        let mut head = Vec::new();
        let mut byte = [0u8; 1];
        while !head.ends_with(b"\r\n\r\n") {
            if std::io::Read::read(stream, &mut byte).unwrap_or(0) == 0 {
                break;
            }
            head.push(byte[0]);
        }
        let text = String::from_utf8_lossy(&head).to_ascii_lowercase();
        let len = text
            .lines()
            .find_map(|l| l.strip_prefix("content-length:"))
            .and_then(|n| n.trim().parse::<usize>().ok())
            .unwrap_or(0);
        let mut body = vec![0u8; len];
        let _ = std::io::Read::read_exact(stream, &mut body);
        (text, body)
    }

    /// The writes a [`dropping_service`] was sent and never answered.
    #[derive(Default)]
    struct Dropped {
        appends: AtomicUsize,
        updates: AtomicUsize,
    }

    /// A hosted service that serves the table `logs` (as [`open_logs`]
    /// declares it) to the open and to a prepare, and reads every other
    /// request, an append or an update included, and closes the connection
    /// without an answer: a service that took the write, or not, and whose
    /// answer never arrived. Returns the bridge open on it and what it
    /// dropped.
    fn dropping_service() -> (*mut Bridge, Arc<Dropped>) {
        let dropped = Arc::new(Dropped::default());
        let seen = Arc::clone(&dropped);
        let h = logs_service(move |request| {
            if request.contains("/v1/append/") {
                seen.appends.fetch_add(1, Ordering::SeqCst);
            } else if request.contains("/v1/update/") {
                seen.updates.fetch_add(1, Ordering::SeqCst);
            }
            None
        });
        (h, dropped)
    }

    /// A hosted service that serves the table `logs` (as [`open_logs`]
    /// declares it) to the open and to a prepare, and answers every other
    /// request with what `write` says for its request line: a status and a
    /// body, or None to close the connection without an answer. Returns the
    /// bridge open on it.
    fn logs_service(
        write: impl Fn(&str) -> Option<(u16, Vec<u8>)> + Send + 'static,
    ) -> *mut Bridge {
        let local = open_logs();
        let bridge = unsafe { &*local };
        let document = bridge.conn.schema("logs").unwrap().to_json().to_string();
        let mut ipc = Vec::new();
        {
            let schema = bridge.table("logs").unwrap().schema();
            let mut w =
                datafusion::arrow::ipc::writer::StreamWriter::try_new(&mut ipc, &schema).unwrap();
            w.finish().unwrap();
        }
        unsafe { infino_close(local) };

        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        std::thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(mut stream) = stream else { return };
                let (text, body) = read_head_and_body(&mut stream);
                let request = text.lines().next().unwrap_or("").to_string();
                let answer: Option<(u16, &str, Vec<u8>)> = if request.contains("/v1/list_tables/") {
                    Some((200, "application/json", b"[]".to_vec()))
                } else if request.contains("/v1/schema/") {
                    if String::from_utf8_lossy(&body).contains("\"document\"") {
                        Some((200, "application/json", document.clone().into_bytes()))
                    } else {
                        Some((200, "application/vnd.apache.arrow.stream", ipc.clone()))
                    }
                } else {
                    write(&request).map(|(status, bytes)| (status, "application/json", bytes))
                };
                if let Some((status, kind, bytes)) = answer {
                    let head = format!(
                        "HTTP/1.1 {status} X\r\nContent-Type: {kind}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                        bytes.len()
                    );
                    let _ = std::io::Write::write_all(&mut stream, head.as_bytes());
                    let _ = std::io::Write::write_all(&mut stream, &bytes);
                }
            }
        });
        let uri = c(&format!("http://{addr}/db"));
        let key = c("ik_test");
        let h = unsafe { infino_open_with(uri.as_ptr(), key.as_ptr(), ptr::null()) };
        assert!(!h.is_null(), "the open lists the tables");
        h
    }

    /// A prepared batch of `ids` for `logs`, each document `{"level":"info"}`.
    fn prepare_logs(h: *mut Bridge, ids: &[&str]) -> *mut Prepared {
        let mut buf = Vec::new();
        let mut id_ends = Vec::new();
        let mut src_ends = Vec::new();
        for id in ids {
            buf.extend_from_slice(id.as_bytes());
            id_ends.push(buf.len() as u32);
            buf.extend_from_slice(br#"{"level":"info"}"#);
            src_ends.push(buf.len() as u32);
        }
        let name = c("logs");
        let p = unsafe {
            infino_prepare_batch(
                h,
                name.as_ptr(),
                buf.as_ptr(),
                buf.len(),
                id_ends.as_ptr(),
                src_ends.as_ptr(),
                ptr::null(),
                ids.len() as c_int,
            )
        };
        assert!(!p.is_null(), "the service answers the prepare");
        p
    }

    /// An append the hosted service took no answer back from answers 503
    /// for each of its documents, after one append request: the batch is not
    /// tried again in halves or a document at a time, which would wait out a
    /// timeout for each and could write the rows the first append landed
    /// again. The same holds for the joined batch's replace of the rows it
    /// replaces.
    #[test]
    fn an_append_the_service_does_not_answer_is_sent_once_and_answers_503() {
        let (h, dropped) = dropping_service();
        let name = c("logs");
        let ids = ["a", "b", "c", "d"];

        let p = prepare_logs(h, &ids);
        let out = take(unsafe {
            infino_append_prepared(h, name.as_ptr(), &p, 1, ptr::null(), 0, ptr::null(), 0)
        });
        let out: Value = serde_json::from_str(&out).unwrap();
        assert!(out.get("error").is_none(), "{out}");
        assert_eq!(out["ok"], 0, "{out}");
        assert_unanswered(out["failed"].as_array().unwrap(), 0..4);
        assert_eq!(
            dropped.appends.load(Ordering::SeqCst),
            1,
            "one append request"
        );

        let p = prepare_logs(h, &ids);
        let replaced: Vec<CString> = ids.iter().map(|id| c(id)).collect();
        let replaced: Vec<*const c_char> = replaced.iter().map(|id| id.as_ptr()).collect();
        let out = take(unsafe {
            infino_append_prepared(
                h,
                name.as_ptr(),
                &p,
                1,
                replaced.as_ptr(),
                replaced.len() as c_int,
                ptr::null(),
                0,
            )
        });
        let out: Value = serde_json::from_str(&out).unwrap();
        assert_eq!(out["ok"], 0, "{out}");
        assert_unanswered(out["failed"].as_array().unwrap(), 0..4);
        assert_eq!(
            dropped.updates.load(Ordering::SeqCst),
            1,
            "one update request"
        );
        assert_eq!(
            dropped.appends.load(Ordering::SeqCst),
            1,
            "no append for the replaced rows"
        );
        unsafe { infino_close(h) };
    }

    /// A replace a document at a time ([`replace_rows_checked`]) that the
    /// hosted service does not answer ends with that error after one
    /// request: its rows are not counted, split, or reported as refused,
    /// only as not answered (503), for the client to send again.
    #[test]
    fn a_replace_the_service_does_not_answer_is_sent_once_and_is_unavailable() {
        let (h, dropped) = dropping_service();
        let bridge = unsafe { &*h };
        let table = bridge.table("logs").unwrap();
        let rows: Vec<(usize, Value)> = ["a", "b", "c", "d"]
            .iter()
            .enumerate()
            .map(|(i, id)| (i, json!({ OSID: id, SOURCE: "{}", "level": "info" })))
            .collect();
        let mut failed = Vec::new();
        let out = replace_rows_checked(
            &table,
            &bridge.gate("logs"),
            &rows,
            &mut failed,
            bridge.id_list_bytes(),
        );
        assert!(matches!(out, Err(InfinoError::Unavailable(_))), "{out:?}");
        assert_unanswered(&failed, 0..4);
        assert_eq!(
            dropped.updates.load(Ordering::SeqCst),
            1,
            "one update request"
        );
        unsafe { infino_close(h) };
    }

    /// The body the hosted service answers a call it turns away for the
    /// account's request rate with.
    const RATE_LIMIT_BODY: &str = r#"{"error":"rate limit exceeded"}"#;

    /// The error the engine's hosted client returns for a call the service
    /// turned away for the account's request rate.
    fn rate_limit() -> InfinoError {
        InfinoError::Backend(format!(
            "append: server returned {STATUS_TOO_MANY_REQUESTS}: {RATE_LIMIT_BODY}"
        ))
    }

    /// `failed` reports exactly the documents `expected` names, each not
    /// taken, not permanent, under `status` and a reason that holds `says`.
    fn assert_not_written(failed: &[Value], expected: std::ops::Range<u64>, status: u16, says: &str) {
        let mut seen: Vec<u64> = failed.iter().filter_map(|f| f["i"].as_u64()).collect();
        seen.sort_unstable();
        assert_eq!(seen, expected.collect::<Vec<_>>(), "{failed:?}");
        for f in failed {
            assert_eq!(f["status"], status, "{f}");
            assert_eq!(f["permanent"], false, "{f}");
            let reason = f["reason"].as_str().unwrap_or_default();
            assert!(reason.contains(says), "{f}");
        }
    }

    /// `failed` reports exactly the documents `expected` names under status
    /// 429 and the service's reason.
    fn assert_rate_limited(failed: &[Value], expected: std::ops::Range<u64>) {
        assert_not_written(failed, expected, STATUS_TOO_MANY_REQUESTS, "server returned 429");
    }

    /// `failed` reports exactly the documents `expected` names under status
    /// 503, as not answered.
    fn assert_unanswered(failed: &[Value], expected: std::ops::Range<u64>) {
        assert_not_written(failed, expected, SERVICE_UNAVAILABLE, UNANSWERED);
    }

    /// A rate limit (a 429 the hosted client passes on as an untyped backend
    /// error) is told apart from a refusal of the rows: like a service that
    /// did not answer, it ends the write. A refusal of the rows, their size,
    /// the key or the account's row limit does not; nor does a statement
    /// past the memory budget, which the caller is answered 429 for too.
    #[test]
    fn a_rate_limit_is_told_from_a_refusal_and_ends_the_write() {
        assert!(rate_limited(&rate_limit()));
        assert!(ends_the_write(&rate_limit()));
        // A 529 (no capacity) is a rate limit too, answered as a 429.
        let overloaded = InfinoError::Backend("append: server returned 529: overloaded".into());
        assert!(rate_limited(&overloaded));
        assert!(ends_the_write(&overloaded));
        assert_eq!(engine_error_value(&overloaded)["status"], STATUS_TOO_MANY_REQUESTS);
        assert_eq!(untried_status(&overloaded), Some(STATUS_TOO_MANY_REQUESTS));
        let unavailable = InfinoError::Unavailable("append: server returned 503: x".into());
        assert!(!rate_limited(&unavailable));
        assert!(ends_the_write(&unavailable));
        let others = [
            InfinoError::Backend("append: server returned 400: append error: schema: x".into()),
            InfinoError::Backend("update: server returned 413: ".into()),
            InfinoError::Backend("append: server returned 500: x".into()),
            InfinoError::Backend(
                "append: server returned 403: the key was refused for this call: account row limit reached"
                    .into(),
            ),
            InfinoError::OverBudget("past the budget".into()),
        ];
        for e in &others {
            assert!(!rate_limited(e), "{e}");
            assert!(!ends_the_write(e), "{e}");
        }
    }

    /// A write the hosted service turns away for the account's request rate
    /// is not split into halves, each of which would spend more of the rate
    /// on the same rows: it stops at that call, and every document it did
    /// not write is reported under 429, while the documents an earlier call
    /// wrote are not reported, so a retry sends each missing one once.
    #[test]
    fn a_rate_limited_write_stops_and_reports_429_for_what_it_did_not_write() {
        let rows: Vec<(usize, Value)> = (0..8).map(|i| (i, json!({ "n": i }))).collect();
        let calls = std::cell::Cell::new(0);
        let mut failed = Vec::new();
        let out = write_checked(&rows, &mut failed, &|_| {
            calls.set(calls.get() + 1);
            Err(rate_limit())
        });
        assert!(matches!(&out, Err(e) if rate_limited(e)), "{out:?}");
        assert_eq!(calls.get(), 1, "one call for the whole batch");
        assert_rate_limited(&failed, 0..8);

        // Refused for what it carries, the write goes in halves: the first
        // lands, the second is turned away and not split further.
        calls.set(0);
        let mut failed = Vec::new();
        let out = write_checked(&rows, &mut failed, &|_| {
            calls.set(calls.get() + 1);
            match calls.get() {
                1 => Err(InfinoError::Backend(
                    "append: server returned 400: failing".into(),
                )),
                2 => Ok(()),
                _ => Err(rate_limit()),
            }
        });
        assert!(matches!(&out, Err(e) if rate_limited(e)), "{out:?}");
        assert_eq!(calls.get(), 3, "{failed:?}");
        assert_rate_limited(&failed, 4..8);
    }

    /// A write the hosted service stops answering partway answers 503 only
    /// for the documents of the call it did not answer and of those after
    /// it: the documents an earlier call wrote are taken, so a client that
    /// sends the 503 items again does not store an auto-id document twice.
    #[test]
    fn an_unanswered_write_reports_503_only_for_what_it_did_not_write() {
        let rows: Vec<(usize, Value)> = (0..8).map(|i| (i, json!({ "n": i }))).collect();
        let calls = std::cell::Cell::new(0);
        let mut failed = Vec::new();
        let out = write_checked(&rows, &mut failed, &|_| {
            calls.set(calls.get() + 1);
            match calls.get() {
                1 => Err(InfinoError::Backend("append: server returned 400: failing".into())),
                2 => Ok(()),
                _ => Err(InfinoError::Unavailable("append: transport error: timed out".into())),
            }
        });
        assert!(matches!(&out, Err(InfinoError::Unavailable(_))), "{out:?}");
        assert_eq!(calls.get(), 3, "{failed:?}");
        assert_unanswered(&failed, 4..8);
    }

    /// A replace, or a rewrite, the hosted service turns away for the
    /// account's request rate stops at that call, with no count of rows,
    /// and reports every document, those of the runs not tried included,
    /// under 429.
    #[test]
    fn a_rate_limited_replace_or_rewrite_reports_429_for_every_document() {
        let rows = fake_rows();
        let all = 0..FAKE_ROWS as u64;
        let budget = FAKE_IDS_PER_RUN * id_cost(&fake_id(0));
        let (table, calls) = fake_hosted(Answer::Status(STATUS_TOO_MANY_REQUESTS), usize::MAX);
        let mut failed = Vec::new();
        let out = replace_rows_checked(&table, &Gate::default(), &rows, &mut failed, budget);
        assert!(matches!(&out, Err(e) if rate_limited(e)), "{out:?}");
        assert_eq!(ops(&calls), vec!["update"]);
        assert_rate_limited(&failed, all.clone());

        let (table, calls) = fake_hosted(Answer::Status(STATUS_TOO_MANY_REQUESTS), usize::MAX);
        let mut failed = Vec::new();
        let out = rewrite_rows(&table, &Gate::default(), &rows, &mut failed, budget);
        assert!(matches!(&out, Err(e) if rate_limited(e)), "{out:?}");
        assert_eq!(ops(&calls), vec!["delete"]);
        assert_rate_limited(&failed, all);
    }

    /// A hosted service that answers its appends in turn with `answers`, a
    /// status each, and with 429 once they run out; with the appends it took.
    fn appends_answered(answers: &'static [u16]) -> (*mut Bridge, Arc<AtomicUsize>) {
        let appends = Arc::new(AtomicUsize::new(0));
        let seen = Arc::clone(&appends);
        let h = logs_service(move |request| {
            if !request.contains("/v1/append/") {
                return None;
            }
            let n = seen.fetch_add(1, Ordering::SeqCst);
            match answers.get(n) {
                Some(&status) => Some((status, b"append error: failing".to_vec())),
                None => Some((
                    STATUS_TOO_MANY_REQUESTS,
                    RATE_LIMIT_BODY.as_bytes().to_vec(),
                )),
            }
        });
        (h, appends)
    }

    /// An append the hosted service turns away for the account's request
    /// rate is answered document by document, not as an error: each document
    /// not written carries status 429, which the plugin answers its bulk item
    /// with, and the documents an earlier call wrote are taken. Turned away
    /// at once, the batch costs one append and no document is taken. When
    /// the joined batch is refused for its content and its documents go in
    /// halves, the first half lands and the second is turned away: two taken,
    /// two 429, four appends in all, and none stored twice by the retry.
    #[test]
    fn an_append_the_service_rate_limits_answers_429_for_what_it_did_not_write() {
        let name = c("logs");
        let ids = ["a", "b", "c", "d"];
        let append = |h: *mut Bridge| -> Value {
            let p = prepare_logs(h, &ids);
            let out = take(unsafe {
                infino_append_prepared(h, name.as_ptr(), &p, 1, ptr::null(), 0, ptr::null(), 0)
            });
            serde_json::from_str(&out).unwrap()
        };

        let (h, appends) = appends_answered(&[]);
        let out = append(h);
        assert!(out.get("error").is_none(), "{out}");
        assert_eq!(out["ok"], 0, "{out}");
        assert_rate_limited(out["failed"].as_array().unwrap(), 0..4);
        assert_eq!(appends.load(Ordering::SeqCst), 1, "one append request");
        unsafe { infino_close(h) };

        let (h, appends) = appends_answered(&[STATUS_BAD_REQUEST, STATUS_BAD_REQUEST, 200]);
        let out = append(h);
        assert!(out.get("error").is_none(), "{out}");
        assert_eq!(out["ok"], 2, "{out}");
        assert_rate_limited(out["failed"].as_array().unwrap(), 2..4);
        assert_eq!(appends.load(Ordering::SeqCst), 4, "{out}");
        unsafe { infino_close(h) };

        // A 529 (no capacity) ends the write as a 429 does: one append for
        // the joined batch, no halves, every document answered 429.
        let (h, appends) = appends_answered(&[STATUS_OVERLOADED]);
        let out = append(h);
        assert!(out.get("error").is_none(), "{out}");
        assert_eq!(out["ok"], 0, "{out}");
        assert_not_written(
            out["failed"].as_array().unwrap(),
            0..4,
            STATUS_TOO_MANY_REQUESTS,
            "server returned 529",
        );
        assert_eq!(appends.load(Ordering::SeqCst), 1, "one append request");
        unsafe { infino_close(h) };
    }

    /// When the joined batch is refused for its content and its documents
    /// go in halves, and the service answers the second half 503 after the
    /// first landed, the first half is taken and only the second is
    /// answered 503: four appends in all, and no call after the 503.
    #[test]
    fn an_append_unanswered_partway_answers_503_only_for_what_it_did_not_write() {
        let name = c("logs");
        let ids = ["a", "b", "c", "d"];
        let (h, appends) =
            appends_answered(&[STATUS_BAD_REQUEST, STATUS_BAD_REQUEST, 200, SERVICE_UNAVAILABLE]);
        let p = prepare_logs(h, &ids);
        let out = take(unsafe {
            infino_append_prepared(h, name.as_ptr(), &p, 1, ptr::null(), 0, ptr::null(), 0)
        });
        let out: Value = serde_json::from_str(&out).unwrap();
        assert!(out.get("error").is_none(), "{out}");
        assert_eq!(out["ok"], 2, "{out}");
        assert_unanswered(out["failed"].as_array().unwrap(), 2..4);
        assert_eq!(appends.load(Ordering::SeqCst), 4, "{out}");
        unsafe { infino_close(h) };
    }

    /// A delete the hosted service turns away for the account's request rate,
    /// or does not answer, returns its own code, so the plugin answers 429 or
    /// 503 rather than an internal error; any other failure returns the
    /// general one. A table that does not exist has nothing to delete.
    #[test]
    fn a_delete_the_service_turns_away_or_does_not_answer_says_which() {
        let name = c("logs");
        let ids = [c("a")];
        let ids: Vec<*const c_char> = ids.iter().map(|id| id.as_ptr()).collect();
        let h = open_storage();
        let missing = c("no_such_table");
        assert_eq!(
            unsafe { infino_delete_osids(h, missing.as_ptr(), ids.as_ptr(), 1) },
            0
        );
        unsafe { infino_close(h) };
        let cases: [(Option<u16>, c_int); 4] = [
            (Some(STATUS_TOO_MANY_REQUESTS), DELETE_RATE_LIMITED),
            (Some(STATUS_OVERLOADED), DELETE_RATE_LIMITED),
            (None, DELETE_UNAVAILABLE),
            (Some(STATUS_SERVER_ERROR), DELETE_FAILED),
        ];
        for (status, code) in cases {
            let h = logs_service(move |request| {
                request
                    .contains("/v1/delete/")
                    .then_some(status)
                    .flatten()
                    .map(|s| (s, b"failing".to_vec()))
            });
            let rc = unsafe { infino_delete_osids(h, name.as_ptr(), ids.as_ptr(), 1) };
            assert_eq!(rc, code, "{status:?}");
            unsafe { infino_close(h) };
        }
    }

    /// A read by id the hosted service does not answer is a 503 the caller
    /// can retry, naming the call and the id, not an error with no status.
    #[test]
    fn a_read_by_id_the_service_does_not_answer_is_503() {
        let (h, _) = dropping_service();
        let name = c("logs");
        let id = c("a");
        for out in [
            take(unsafe { infino_get(h, name.as_ptr(), id.as_ptr()) }),
            take(unsafe { infino_get_doc(h, name.as_ptr(), id.as_ptr()) }),
        ] {
            let out: Value = serde_json::from_str(&out).unwrap();
            assert_eq!(out["status"], 503, "{out}");
            let message = out["error"].as_str().unwrap();
            assert!(message.contains(" a: "), "{message}");
            assert!(message.contains("did not answer in time"), "{message}");
        }
        unsafe { infino_close(h) };
    }

    /// An engine error that says the hosted service could not serve the call
    /// is marked `unavailable`, whatever status it carries: a 504 from a
    /// gateway keeps its 504, a transport failure is a 503. Any other error
    /// carries no mark.
    #[test]
    fn an_unavailable_service_is_marked_whatever_its_status() {
        for (e, status) in [
            (InfinoError::Unavailable("append: server returned 504: gateway timeout".into()), 504),
            (InfinoError::Unavailable("append: server returned 502: bad gateway".into()), 502),
            (InfinoError::Unavailable("append: transport error: timeout".into()), 503),
        ] {
            let v = engine_error_value(&e);
            assert_eq!(v["unavailable"], true, "{v}");
            assert_eq!(v["status"], status, "{v}");
        }
        let v = engine_error_value(&InfinoError::Backend("append: server returned 500: boom".into()));
        assert!(v.get("unavailable").is_none(), "{v}");
        assert_eq!(v["status"], 500, "{v}");
    }

    /// The mapper's values follow a document's source after the separator;
    /// text after it that is no object of values leaves the text the source.
    #[test]
    fn mapped_values_follow_the_source() {
        assert_eq!(mapped_values(r#"{"a":1}"#), (r#"{"a":1}"#, None));
        let framed = format!("{}{MAPPED_VALUES}{}", r#"{"a":1.5}"#, r#"{"a":[1]}"#);
        let (source, values) = mapped_values(&framed);
        assert_eq!(source, r#"{"a":1.5}"#);
        assert_eq!(values, json!({"a": [1]}).as_object().cloned());
        let stray = format!("{}{MAPPED_VALUES}x", r#"{"a":1}"#);
        assert_eq!(mapped_values(&stray), (stray.as_str(), None));
    }

    /// A date in a custom pattern and a fractional number in an integer
    /// field fail a document by themselves, and conform when the mapper's
    /// values come with it: each column holds the mapper's value as it is
    /// (an `epoch_second` field's instant is not read as seconds again), a
    /// field the source leaves out or null takes none of them, and a field
    /// given several values keeps the mapper's in its list column.
    #[test]
    fn the_mapper_s_values_stand_in_for_the_source_s() {
        let schema = Schema::new(vec![
            Field::new("status", DataType::Int64, true),
            Field::new("t", date_type(), true),
            Field::new("t3", date_type(), true),
            Field::new("gone", DataType::Int64, true),
        ]);
        let kept = HashMap::from([
            (
                "t".to_string(),
                Kept::Date(DateFormat::parse_all("dd/MMM/yyyy:HH:mm:ss Z")),
            ),
            (
                "t3".to_string(),
                Kept::Date(DateFormat::parse_all("epoch_second")),
            ),
        ]);
        let source = r#"{"status": 200.7, "t": "25/Nov/2026:09:30:00 +0100", "t3": "1795595400", "gone": null}"#;
        let check = |doc: &str| conform_doc(doc, &schema, &kept, &[], None, &[], &Indexed::default());
        let refused = check(source).unwrap_err();
        assert!(
            refused.contains("fractional") || refused.contains("failed to parse date"),
            "{refused}"
        );
        let values = r#"{"status": [200], "t": [1795595400000], "t3": [1795595400000], "gone": [7], "absent": [1]}"#;
        let row = check(&format!("{source}{MAPPED_VALUES}{values}")).expect("conformed");
        assert_eq!(row.get("status"), Some(&json!(200)));
        assert_eq!(row.get("t"), Some(&json!(1_795_595_400_000i64)));
        assert_eq!(row.get("t3"), Some(&json!(1_795_595_400_000i64)));
        assert_eq!(row.get("gone"), Some(&Value::Null));
        assert_eq!(row.get("absent"), None);

        let many = format!(
            "{}{MAPPED_VALUES}{}",
            r#"{"status": [1.5, 2.7]}"#, r#"{"status": [1, 2]}"#
        );
        let row = check(&many).expect("conformed");
        assert_eq!(
            row.get(&format!("status{LIST_SUFFIX}")),
            Some(&json!([1, 2]))
        );
    }

    /// At an ignored field that keeps some of its values, the mapper's
    /// values are those it indexed, fewer than the source gives: they stand
    /// in for the source's, so a value only the mapper reads (a fraction an
    /// integer field coerces, a date in a custom pattern) is kept beside the
    /// others, and the malformed ones are left out.
    #[test]
    fn an_ignored_array_keeps_the_values_the_mapper_read() {
        let list = |dt: DataType| DataType::List(Arc::new(Field::new(LIST_ITEM, dt, true)));
        let schema = Schema::new(vec![
            Field::new("n", DataType::Int64, true),
            Field::new(format!("n{LIST_SUFFIX}"), list(DataType::Int64), true),
            Field::new("t", date_type(), true),
            Field::new(format!("t{LIST_SUFFIX}"), list(date_type()), true),
        ]);
        let kept = HashMap::from([(
            "t".to_string(),
            Kept::Date(DateFormat::parse_all("dd/MMM/yyyy:HH:mm:ss Z")),
        )]);
        let row = |doc: String, ignored: &[&str]| conform_doc(&doc, &schema, &kept, &[], None, ignored, &Indexed::default());

        let source = r#"{"n": [1, 2.5, "x"], "t": ["25/Nov/2026:09:30:00 +0100", "never"]}"#;
        let values = r#"{"n": [1, 2], "t": [1795595400000]}"#;
        let flat = row(format!("{source}{MAPPED_VALUES}{values}"), &["n", "t"]).unwrap();
        assert_eq!(flat.get(&format!("n{LIST_SUFFIX}")), Some(&json!([1, 2])), "{flat:?}");
        assert_eq!(flat.get("t"), Some(&json!(1_795_595_400_000i64)), "{flat:?}");
        // Without the mapper's values, only what the column reads by itself is kept.
        let flat = row(source.to_string(), &["n", "t"]).unwrap();
        assert_eq!(flat.get("n"), Some(&json!(1)), "{flat:?}");
        assert_eq!(flat.get("t"), Some(&Value::Null), "{flat:?}");
        // At a field not ignored, fewer values than the source gives stand in for none.
        let fewer = format!(r#"{{"n": [1, 2.5]}}{MAPPED_VALUES}{{"n": [1]}}"#);
        assert!(row(fewer, &[]).is_err());
    }

    /// What a mapping indexes ([`Indexed`]): nothing at or under an object
    /// with `enabled: false`, nothing at all under a root that says so, and
    /// no field a property does not map under `dynamic: false`, where the
    /// nearest object that sets `dynamic` decides. A path under a mapped
    /// field of any other type is that field's (#294).
    #[test]
    fn a_mapping_indexes_neither_a_disabled_object_nor_an_unmapped_field_under_dynamic_false() {
        let disabled = Indexed::of(&json!({"properties": {
            "tag": {"type": "keyword"},
            "body": {"type": "object", "enabled": false, "properties": {"a": {"type": "long"}}},
            "meta": {"properties": {"raw": {"enabled": false}, "n": {"type": "long"}}}
        }}));
        assert!(disabled.leaves_out);
        for path in ["body", "body.a", "body.a.b", "meta.raw", "meta.raw.x"] {
            assert!(!disabled.indexes(path), "{path}");
        }
        // A new field elsewhere is mapped by its document's mapping update.
        for path in ["tag", "meta.n", "meta.other", "other", "other.x"] {
            assert!(disabled.indexes(path), "{path}");
        }
        // Nothing under the disabled object is a column either.
        let fields = mapped_fields(&json!({"properties": {
            "body": {"type": "object", "enabled": false, "properties": {"a": {"type": "long"}}},
            "n": {"type": "long"}
        }}));
        assert_eq!(fields.iter().map(|f| f.path.as_str()).collect::<Vec<_>>(), ["n"]);

        // As OpenSearch hands a mapping over: wrapped, its flags as strings.
        let loose = Indexed::of(&json!({"_doc": {"dynamic": "false", "properties": {
            "tag": {"type": "keyword"},
            "loc": {"type": "geo_point"},
            "open": {"dynamic": true, "properties": {"k": {"type": "keyword"}}},
            "deep": {"properties": {"k": {"type": "keyword"}}}
        }}}));
        for path in ["data", "data.a", "deep.free", "deep.free.x"] {
            assert!(!loose.indexes(path), "{path}");
        }
        for path in ["tag", "loc.lat", "loc", "open.new", "open.new.x", "deep.k"] {
            assert!(loose.indexes(path), "{path}");
        }

        // `enabled: false` at the root: the whole document is source only.
        let root = Indexed::of(&json!({"mappings": {"enabled": false}}));
        assert!(!root.indexes("anything") && !root.indexes("a.b"));
        assert!(mapped_fields(&json!({"enabled": false, "properties": {"n": {"type": "long"}}})).is_empty());

        // A mapping that leaves nothing out looks at no row; a strict one
        // refuses a new field before it is written, so it reads as indexed.
        let plain = Indexed::of(&json!({"dynamic": "strict", "properties": {"n": {"type": "long"}}}));
        assert!(!plain.leaves_out);
        assert!(!Indexed::default().leaves_out && Indexed::default().indexes("x.y"));
        // The template-aware settings: a field no template maps is left out
        // under `false_allow_templates`, and refused before it is written
        // under `strict_allow_templates`.
        let templated = Indexed::of(&json!({"dynamic": "false_allow_templates", "properties": {
            "tag": {"type": "keyword"},
            "strict": {"dynamic": "strict_allow_templates", "properties": {}}
        }}));
        assert!(templated.leaves_out);
        assert!(templated.indexes("tag") && !templated.indexes("free"));
        assert!(templated.indexes("strict.new"));
        assert_eq!(mapping_flag(Some(&json!("runtime"))), None);
        // A property spelled with a dot matches as one name.
        let dotted = Indexed::of(&json!({"dynamic": false, "properties": {"a.b": {"type": "long"}}}));
        assert!(dotted.indexes("a.b") && !dotted.indexes("a.c"));
    }

    /// A field the mapping does not index takes a value of any shape in each
    /// document: an object, a string, an array (of arrays too), a number.
    /// Each is written in its own commit, none is refused, the table holds
    /// no column for the field, and the source keeps it whole. A table that
    /// already holds a column for such a field, typed by an earlier
    /// document, keeps it and its old values; a later document leaves it
    /// null rather than being refused for a value it cannot take (#294).
    #[test]
    fn a_field_the_mapping_does_not_index_takes_any_shape_and_gets_no_column() {
        let h = open_storage();
        let bridge = bridge_ref(h).unwrap();
        // Written before the rule: a document typed `body.a` as a number.
        let out = ensure(h, r#"{"properties":{"tag":{"type":"keyword"}}}"#);
        assert!(out.contains("\"created\":true"), "{out}");
        assert_eq!(index(h, &[("old", r#"{"tag":"o","body":{"a":1}}"#)])["ok"], 1);
        let schema = || bridge.conn.open_table("logs").unwrap().schema();
        assert!(schema().field_with_name("body.a").is_ok());

        let mapping = r#"{"dynamic":"false","properties":{"tag":{"type":"keyword"},
            "body":{"type":"object","enabled":false}}}"#;
        let out = ensure(h, mapping);
        assert!(out.contains("\"created\":false"), "{out}");
        let docs = [
            ("d0", r#"{"tag":"a","body":{"a":"now a string"},"data":{"x":1}}"#),
            ("d1", r#"{"tag":"b","body":"text","data":"text"}"#),
            ("d2", r#"{"tag":"c","body":[1,"x",{"a":"y"}],"data":[[1,2],[3]]}"#),
            ("d3", r#"{"tag":"d","body":7,"data":{"x":{"y":[true]}}}"#),
        ];
        for (id, doc) in docs {
            assert_eq!(check(h, doc), "", "{id}");
            let out = index(h, &[(id, doc)]);
            assert_eq!(out["ok"], 1, "{id}: {out}");
        }
        let schema = schema();
        for column in ["body", "data", "data.x", "body.a[]", "data[]"] {
            assert!(schema.field_with_name(column).is_err(), "{column}: {schema:?}");
        }
        for (id, doc) in docs {
            assert_eq!(get(h, id), doc, "{id}");
        }
        let rows = sql(h, r#"SELECT osid, "body.a", tag FROM logs ORDER BY osid"#);
        assert_eq!(rows.as_array().map(Vec::len), Some(5), "{rows}");
        assert_eq!(rows[4]["osid"], "old", "{rows}");
        assert_eq!(rows[4]["body.a"], 1, "{rows}");
        assert!(rows[0]["body.a"].is_null(), "{rows}");
        assert_eq!(rows[2]["tag"], "c", "{rows}");
        close(h);
    }

    /// A new path beneath a column the table already holds is refused by
    /// every commit, so the check refuses it too: a write is answered with
    /// the reason before it is acknowledged, rather than lost at the commit.
    #[test]
    fn a_path_beneath_a_live_column_is_refused_when_checked() {
        let h = open_storage();
        let out = ensure(h, r#"{"properties":{"tag":{"type":"keyword"}}}"#);
        assert!(out.contains("\"created\":true"), "{out}");
        assert_eq!(index(h, &[("a", r#"{"tag":"a","loc":"40,-70"}"#)])["ok"], 1);
        let doc = r#"{"tag":"b","loc":{"lat":40,"lon":-70}}"#;
        let why = check(h, doc);
        assert!(why.contains("[loc.lat]") && why.contains("[loc]"), "{why}");
        assert_ne!(index(h, &[("b", doc)])["ok"], 1);
        // A path beside the column, or the column itself, is no conflict.
        assert_eq!(check(h, r#"{"tag":"c","loc":"1,2","locale":{"x":1}}"#), "");
        // Nor is a null beneath it, which the engine skips.
        let null = r#"{"tag":"d","loc":{"lat":null}}"#;
        assert_eq!(check(h, null), "");
        assert_eq!(index(h, &[("d", null)])["ok"], 1);
        close(h);
    }

    /// A put-mapping that names a field `dynamic: false` left out declares
    /// it again: the rule is rebuilt from the new mapping, the field gets
    /// its column, and the documents written after it fill that column. One
    /// written before keeps its value in the source only, as stock indexes
    /// only what it mapped (#294).
    #[test]
    fn a_put_mapping_gives_a_left_out_field_its_column() {
        let h = open_storage();
        let bridge = bridge_ref(h).unwrap();
        let out = ensure(h, r#"{"dynamic":false,"properties":{"tag":{"type":"keyword"}}}"#);
        assert!(out.contains("\"created\":true"), "{out}");
        let before = r#"{"tag":"a","note":"left out"}"#;
        assert_eq!(index(h, &[("n1", before)])["ok"], 1);
        let schema = || bridge.conn.open_table("logs").unwrap().schema();
        assert!(schema().field_with_name("note").is_err());

        let out = ensure(
            h,
            r#"{"dynamic":false,"properties":{"tag":{"type":"keyword"},"note":{"type":"keyword"}}}"#,
        );
        assert!(out.contains("\"created\":false"), "{out}");
        assert!(schema().field_with_name("note").is_ok());
        assert_eq!(index(h, &[("n2", r#"{"tag":"b","note":"mapped"}"#)])["ok"], 1);
        let rows = sql(h, "SELECT osid, note FROM logs ORDER BY osid");
        assert!(rows[0]["note"].is_null(), "{rows}");
        assert_eq!(rows[1]["note"], "mapped", "{rows}");
        assert_eq!(get(h, "n1"), before);
        close(h);
    }

    /// Of an array of objects, the members the mapping maps keep their
    /// values, and the ones `dynamic: false` leaves out take any shape in
    /// each element and each document, with no column (#294).
    #[test]
    fn an_array_of_objects_keeps_only_its_mapped_members() {
        let h = open_storage();
        let bridge = bridge_ref(h).unwrap();
        let out = ensure(
            h,
            r#"{"dynamic":"false","properties":{"items":{"properties":{"name":{"type":"keyword"}}}}}"#,
        );
        assert!(out.contains("\"created\":true"), "{out}");
        let docs = [
            ("a", r#"{"items":[{"name":"x","extra":1},{"name":"y","extra":"s"}]}"#),
            ("b", r#"{"items":[{"name":"z","extra":{"deep":[1]}},{"extra":[[1],[2]]}]}"#),
            ("c", r#"{"items":{"name":"w","extra":[{"k":true}]}}"#),
        ];
        for (id, doc) in docs {
            assert_eq!(check(h, doc), "", "{id}");
            assert_eq!(index(h, &[(id, doc)])["ok"], 1, "{id}");
        }
        let schema = bridge.conn.open_table("logs").unwrap().schema();
        assert!(
            schema.fields().iter().all(|f| !f.name().starts_with("items.extra")),
            "{schema:?}"
        );
        for (id, doc) in docs {
            assert_eq!(get(h, id), doc, "{id}");
        }
        let rows = sql(h, r#"SELECT osid, "items.name" FROM logs ORDER BY osid"#);
        assert_eq!(rows[1]["items.name"], "z", "{rows}");
        assert_eq!(rows[2]["items.name"], "w", "{rows}");
        close(h);
    }

    /// A table written before the rule may hold a scalar column at a path
    /// the mapping does not index (`data`, typed from a string). A later
    /// document with an object there is not refused for nesting under that
    /// column: its paths are left out of the row before any column is
    /// looked at (#294).
    #[test]
    fn an_old_scalar_column_does_not_refuse_an_object_beneath_it_once_unindexed() {
        let h = open_storage();
        let bridge = bridge_ref(h).unwrap();
        let out = ensure(h, r#"{"properties":{"tag":{"type":"keyword"}}}"#);
        assert!(out.contains("\"created\":true"), "{out}");
        assert_eq!(index(h, &[("old", r#"{"tag":"o","data":"text"}"#)])["ok"], 1);
        let out = ensure(h, r#"{"dynamic":false,"properties":{"tag":{"type":"keyword"}}}"#);
        assert!(out.contains("\"created\":false"), "{out}");
        let doc = r#"{"tag":"n","data":{"x":1}}"#;
        assert_eq!(check(h, doc), "");
        assert_eq!(index(h, &[("new", doc)])["ok"], 1);
        assert_eq!(get(h, "new"), doc);
        let schema = bridge.conn.open_table("logs").unwrap().schema();
        assert!(schema.field_with_name("data").is_ok());
        assert!(schema.field_with_name("data.x").is_err());
        let rows = sql(h, "SELECT osid, data FROM logs ORDER BY osid");
        assert!(rows[0]["data"].is_null(), "{rows}");
        assert_eq!(rows[1]["data"], "text", "{rows}");
        close(h);
    }
}
