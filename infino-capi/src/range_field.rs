//! Range fields (`long_range`, `integer_range`, `float_range`,
//! `double_range`, `date_range`): each value is an interval, kept in the
//! table as two hidden columns under the field's path, its lowest value
//! ([`FROM`]) and its highest ([`TO`]), both inclusive, as OpenSearch keeps
//! a range. A bound given exclusive (`gt`, `lt`) is kept as the next value
//! inside it, and a bound left out as the least or greatest value of the
//! type, so a query compares the two columns alone. `_source` keeps the
//! value as written.
//!
//! An `ip_range` has no columns: its values are kept in `_source` only, and
//! the plugin refuses a query on it.

use infino::arrow_schema::DataType;
use infino::serde_json::{json, Map, Value};

use crate::{conform_date, date_type, DateFormat, DEFAULT_DATE_FORMAT};

/// The hidden column under a range field's path holding each range's lowest
/// value, inclusive.
pub(crate) const FROM: &str = "from";
/// The hidden column under a range field's path holding each range's
/// highest value, inclusive.
pub(crate) const TO: &str = "to";

/// The largest finite double: what a `float_range` or `double_range` bound
/// left out is kept as, where OpenSearch keeps an infinity, which a JSON
/// row cannot carry.
const DOUBLE_LIMIT: f64 = f64::MAX;

/// The type of a range field, as its mapping names it.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum RangeKind {
    Long,
    Integer,
    Float,
    Double,
    /// A `date_range`, its bounds read by the formats its mapping declares.
    Date(Vec<DateFormat>),
}

impl RangeKind {
    /// The range type a field definition names, if it names one that has
    /// columns. `ip_range` is not one ([`is_range_type`]).
    pub(crate) fn of(ty: &str, def: &Map<String, Value>) -> Option<Self> {
        Some(match ty {
            "long_range" => Self::Long,
            "integer_range" => Self::Integer,
            "float_range" => Self::Float,
            "double_range" => Self::Double,
            "date_range" => Self::Date(DateFormat::parse_all(
                def.get("format")
                    .and_then(Value::as_str)
                    .unwrap_or(DEFAULT_DATE_FORMAT),
            )),
            _ => return None,
        })
    }

    /// The type of the field's two columns.
    pub(crate) fn data_type(&self) -> DataType {
        match self {
            Self::Long | Self::Integer => DataType::Int64,
            Self::Float | Self::Double => DataType::Float64,
            Self::Date(_) => date_type(),
        }
    }

    /// The type's name, as a mapping gives it.
    fn name(&self) -> &'static str {
        match self {
            Self::Long => "long_range",
            Self::Integer => "integer_range",
            Self::Float => "float_range",
            Self::Double => "double_range",
            Self::Date(_) => "date_range",
        }
    }

    /// The least value of the type: a lower bound left out.
    fn min(&self) -> Bound {
        match self {
            Self::Long | Self::Date(_) => Bound::Int(i64::MIN),
            Self::Integer => Bound::Int(i64::from(i32::MIN)),
            Self::Float | Self::Double => Bound::Float(-DOUBLE_LIMIT),
        }
    }

    /// The greatest value of the type: an upper bound left out.
    fn max(&self) -> Bound {
        match self {
            Self::Long | Self::Date(_) => Bound::Int(i64::MAX),
            Self::Integer => Bound::Int(i64::from(i32::MAX)),
            Self::Float | Self::Double => Bound::Float(DOUBLE_LIMIT),
        }
    }

    /// One bound as written, as a value of the type.
    fn parse(&self, field: &str, v: &Value) -> Result<Bound, String> {
        match self {
            Self::Long => whole(v, i64::MIN, i64::MAX).map(Bound::Int),
            Self::Integer => whole(v, i64::from(i32::MIN), i64::from(i32::MAX)).map(Bound::Int),
            Self::Float | Self::Double => float(v).map(Bound::Float),
            Self::Date(formats) => {
                let millis = conform_date(v.clone(), formats)?;
                millis
                    .as_i64()
                    .map(Bound::Int)
                    .ok_or_else(|| format!("failed to parse date field [{v}] of [{field}]"))
            }
        }
    }
}

/// Whether `ty` names a range type, `ip_range` included.
pub(crate) fn is_range_type(ty: &str) -> bool {
    matches!(
        ty,
        "long_range" | "integer_range" | "float_range" | "double_range" | "date_range" | "ip_range"
    )
}

/// The path of a range field's column `end` ([`FROM`] or [`TO`]).
pub(crate) fn column(path: &str, end: &str) -> String {
    format!("{path}.{end}")
}

/// One end of a range, as its column holds it.
#[derive(Clone, Copy, Debug, PartialEq)]
enum Bound {
    Int(i64),
    Float(f64),
}

impl Bound {
    /// The next value above this one, as OpenSearch reads a `gt` bound.
    fn next_up(self) -> Self {
        match self {
            Self::Int(i) => Self::Int(i.saturating_add(1)),
            Self::Float(f) => Self::Float(f.next_up()),
        }
    }

    /// The next value below this one, as OpenSearch reads an `lt` bound.
    fn next_down(self) -> Self {
        match self {
            Self::Int(i) => Self::Int(i.saturating_sub(1)),
            Self::Float(f) => Self::Float(f.next_down()),
        }
    }

    fn greater_than(self, other: Self) -> bool {
        match (self, other) {
            (Self::Int(a), Self::Int(b)) => a > b,
            (Self::Float(a), Self::Float(b)) => a > b,
            _ => false,
        }
    }

    fn value(self) -> Value {
        match self {
            Self::Int(i) => json!(i),
            Self::Float(f) => json!(f),
        }
    }
}

/// A whole number from `lo` to `hi`, as OpenSearch's range mapper reads one
/// under `coerce` (the default): a number, its fraction dropped, or its
/// digits in a string.
fn whole(v: &Value, lo: i64, hi: i64) -> Result<i64, String> {
    let n = match v {
        Value::Number(n) => match n.as_i64() {
            Some(i) => Some(i),
            None => n.as_f64().filter(|f| f.is_finite()).and_then(|f| {
                let t = f.trunc();
                // The bounds of an i64, exactly as a double holds them.
                (t >= i64::MIN as f64 && t < i64::MAX as f64).then_some(t as i64)
            }),
        },
        Value::String(s) => {
            let s = s.trim();
            s.parse::<i64>().ok().or_else(|| {
                s.parse::<f64>()
                    .ok()
                    .filter(|f| f.is_finite())
                    .map(f64::trunc)
                    .filter(|t| *t >= i64::MIN as f64 && *t < i64::MAX as f64)
                    .map(|t| t as i64)
            })
        }
        _ => None,
    };
    match n {
        Some(i) if (lo..=hi).contains(&i) => Ok(i),
        Some(_) => Err(format!("Value [{v}] is out of range")),
        None => Err(format!("For input string: {v}")),
    }
}

/// A number, or its digits in a string.
fn float(v: &Value) -> Result<f64, String> {
    match v {
        Value::Number(n) => n.as_f64(),
        Value::String(s) => s.trim().parse::<f64>().ok(),
        _ => None,
    }
    .ok_or_else(|| format!("For input string: {v}"))
}

/// The ranges a range field's value `v` holds, each as its lowest and
/// highest value, inclusive, in the order written: an object of bounds, or
/// an array of them. A null is no range. Anything OpenSearch's mapper
/// refuses is refused with its reason: a bound it does not know, a value
/// that is not an object, a lowest value above the highest.
pub(crate) fn ranges(
    field: &str,
    kind: &RangeKind,
    v: &Value,
) -> Result<Vec<(Value, Value)>, String> {
    let mut out = Vec::new();
    collect(field, kind, v, &mut out)?;
    Ok(out)
}

fn collect(
    field: &str,
    kind: &RangeKind,
    v: &Value,
    out: &mut Vec<(Value, Value)>,
) -> Result<(), String> {
    match v {
        Value::Null => Ok(()),
        Value::Array(items) => items
            .iter()
            .try_for_each(|item| collect(field, kind, item, out)),
        Value::Object(bounds) => {
            let mut from = kind.min();
            let mut to = kind.max();
            for (name, bound) in bounds {
                let read = |b: &Value| {
                    kind.parse(field, b).map_err(|why| {
                        format!(
                            "failed to parse field [{field}] of type [{}]: {why}",
                            kind.name()
                        )
                    })
                };
                match name.as_str() {
                    "gte" if !bound.is_null() => from = read(bound)?,
                    "gt" if !bound.is_null() => from = read(bound)?.next_up(),
                    "lte" if !bound.is_null() => to = read(bound)?,
                    "lt" if !bound.is_null() => to = read(bound)?.next_down(),
                    "gte" | "gt" | "lte" | "lt" => {}
                    other => {
                        return Err(format!(
                            "error parsing field [{field}], with unknown parameter [{other}]"
                        ))
                    }
                }
            }
            if from.greater_than(to) {
                return Err(format!(
                    "failed to parse field [{field}] of type [{}]: min value ({}) is greater than max value ({})",
                    kind.name(),
                    from.value(),
                    to.value()
                ));
            }
            out.push((from.value(), to.value()));
            Ok(())
        }
        _ => Err(format!(
            "error parsing field [{field}], expected an object but got {field}"
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn long(v: Value) -> Result<Vec<(Value, Value)>, String> {
        ranges("r", &RangeKind::Long, &v)
    }

    #[test]
    fn inclusive_bounds_are_kept_as_written() {
        assert_eq!(
            long(json!({"gte": 1, "lte": 5})).unwrap(),
            [(json!(1), json!(5))]
        );
    }

    #[test]
    fn exclusive_bounds_are_kept_as_the_next_value_inside() {
        assert_eq!(
            long(json!({"gt": 1, "lt": 5})).unwrap(),
            [(json!(2), json!(4))]
        );
        let d = ranges("r", &RangeKind::Double, &json!({"gt": 1.0})).unwrap();
        assert_eq!(d[0].0, json!(1.0_f64.next_up()));
    }

    #[test]
    fn a_bound_left_out_is_the_types_least_or_greatest_value() {
        assert_eq!(
            long(json!({"lte": 10})).unwrap(),
            [(json!(i64::MIN), json!(10))]
        );
        assert_eq!(
            long(json!({"gte": 15})).unwrap(),
            [(json!(15), json!(i64::MAX))]
        );
        let i = ranges("r", &RangeKind::Integer, &json!({})).unwrap();
        assert_eq!(i, [(json!(i32::MIN), json!(i32::MAX))]);
        assert_eq!(
            long(json!({"gte": null, "lte": 3})).unwrap(),
            [(json!(i64::MIN), json!(3))]
        );
    }

    #[test]
    fn several_ranges_keep_their_order_and_a_null_is_none() {
        assert_eq!(
            long(json!([{"gte": 5, "lte": 9}, null, {"gte": 1, "lte": 2}])).unwrap(),
            [(json!(5), json!(9)), (json!(1), json!(2))]
        );
        assert!(long(Value::Null).unwrap().is_empty());
    }

    #[test]
    fn numbers_are_coerced_as_the_mapper_coerces_them() {
        assert_eq!(
            long(json!({"gte": "7", "lte": 9.8})).unwrap(),
            [(json!(7), json!(9))]
        );
        assert!(ranges("r", &RangeKind::Integer, &json!({"gte": 3_000_000_000_i64})).is_err());
        assert!(long(json!({"gte": "x"})).is_err());
    }

    #[test]
    fn what_the_mapper_refuses_is_refused() {
        let e = long(json!({"from": 1})).unwrap_err();
        assert_eq!(e, "error parsing field [r], with unknown parameter [from]");
        let e = long(json!(5)).unwrap_err();
        assert_eq!(e, "error parsing field [r], expected an object but got r");
        let e = long(json!({"gt": 5, "lt": 6})).unwrap_err();
        assert!(
            e.ends_with("min value (6) is greater than max value (5)"),
            "{e}"
        );
    }

    #[test]
    fn a_date_range_reads_its_formats_and_an_exclusive_bound_moves_a_millisecond() {
        let kind = RangeKind::of("date_range", &Map::new()).unwrap();
        let r = ranges(
            "d",
            &kind,
            &json!({"gte": "2016-01-01", "lt": "2016-01-02"}),
        )
        .unwrap();
        assert_eq!(
            r,
            [(json!(1_451_606_400_000_i64), json!(1_451_692_799_999_i64))]
        );
        let r = ranges("d", &kind, &json!({"gt": 0})).unwrap();
        assert_eq!(r, [(json!(1), json!(i64::MAX))]);
        assert!(ranges("d", &kind, &json!({"gte": "yesterday"})).is_err());
    }

    #[test]
    fn an_ip_range_has_no_columns() {
        assert!(RangeKind::of("ip_range", &Map::new()).is_none());
        assert!(is_range_type("ip_range"));
    }
}
