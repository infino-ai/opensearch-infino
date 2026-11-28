/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import org.opensearch.common.geo.ShapeRelation;
import org.opensearch.infino.InfinoNative.Column;

import java.math.BigDecimal;
import java.time.ZoneId;
import java.util.Map;
import java.util.Set;

import static org.opensearch.infino.InfinoQueryTranslator.LIST_SUFFIX;
import static org.opensearch.infino.InfinoQueryTranslator.OSID;
import static org.opensearch.infino.InfinoQueryTranslator.quoteIdent;

/**
 * A range field ({@code long_range}, {@code integer_range},
 * {@code float_range}, {@code double_range}, {@code date_range}) as the
 * table keeps it: two hidden columns under the field's path, each range's
 * lowest value ({@link #FROM}) and its highest ({@link #TO}), both
 * inclusive, as OpenSearch keeps a range. A bound a document leaves out is
 * the least or greatest value of the type. A document with several ranges
 * keeps them in the two columns' lists, in the same order, so the n-th
 * values of the two lists are one range.
 *
 * <p>A range query (with its {@code relation}), a term or terms query (the
 * ranges holding the value) and exists are answered from the two columns; a
 * histogram or date_histogram counts a document in every bucket from its
 * lowest value's to its highest's ({@link Samples}). Any other query or
 * aggregation on the field is refused, as is any on an {@code ip_range},
 * which has no columns.
 */
final class InfinoRangeField {

    /** Every range type a mapping may name. */
    static final Set<String> TYPES = Set.of("long_range", "integer_range", "float_range", "double_range", "date_range", "ip_range");

    /** The hidden column under the field's path holding each range's lowest value. */
    static final String FROM = "from";
    /** The hidden column under the field's path holding each range's highest value. */
    static final String TO = "to";

    /** A date column's type, for a literal of epoch milliseconds. */
    private static final String UTC_MILLIS = "'Timestamp(Millisecond, Some(\"UTC\"))'";

    /** The greatest finite double: what the table keeps a float or double range's missing bound as. */
    private static final double DOUBLE_LIMIT = Double.MAX_VALUE;

    /** The aliases the subqueries over the columns' lists name their rows and values by. */
    private static final String PAIR_SOURCE = "range_source";
    private static final String PAIR_ROWS = "range_rows";
    private static final String PAIR_ID = "range_id";
    private static final String PAIR_LOW = "range_low";
    private static final String PAIR_HIGH = "range_high";
    private static final String SAMPLE_INDEX = "range_sample";

    /** What a mapping declares of a range field: its path, its type and its date format (null for the default). */
    record Declared(String path, String type, String format) {}

    /** How the field's values compare. */
    enum Kind {
        INT,
        FLOAT,
        DATE
    }

    final String field;
    final String type;
    final String format;
    final Kind kind;
    final String table;
    private final Column from;
    private final Column to;
    private final Column fromList;
    private final Column toList;

    private InfinoRangeField(String field, Declared declared, String table, Column from, Column to, Column fromList, Column toList) {
        this.field = field;
        this.type = declared.type();
        this.format = declared.format();
        this.kind = switch (declared.type()) {
            case "float_range", "double_range" -> Kind.FLOAT;
            case "date_range" -> Kind.DATE;
            default -> Kind.INT;
        };
        this.table = table;
        this.from = from;
        this.to = to;
        this.fromList = fromList;
        this.toList = toList;
    }

    /**
     * The range field {@code field} of {@code table}, or null when its
     * mapping declares no range field there. A range field the table keeps
     * no columns for (an {@code ip_range}, or one an attached table does not
     * hold) is refused, never read as having no value.
     */
    static InfinoRangeField of(String table, Map<String, Column> cols, String field) {
        Declared declared = InfinoNative.rangeField(table, field);
        if (declared == null) {
            return null;
        }
        Column from = cols.get(declared.path() + "." + FROM);
        Column to = cols.get(declared.path() + "." + TO);
        if (from == null || to == null) {
            throw new IllegalArgumentException(
                "[infino] a query on [" + field + "], a field of type [" + declared.type() + "], is not served: the table keeps it in _source only"
            );
        }
        if (from.masked() || to.masked()) {
            throw InfinoAccess.refused("a query on [" + field + "], a field masked for this user, is not served");
        }
        Column fromList = cols.get(from.name() + LIST_SUFFIX);
        Column toList = cols.get(to.name() + LIST_SUFFIX);
        return new InfinoRangeField(field, declared, table, from, to, fromList, toList);
    }

    /** Whether some document gives the field several ranges. */
    boolean multi() {
        return fromList != null && toList != null;
    }

    // ---- queries ----

    /**
     * The rows a range query on the field matches: a range of the document
     * and the query's, both inclusive once an exclusive bound is moved
     * inside, relate as {@code relation} says (intersects when null).
     * {@code format} is the query's own, else the field's. Null when the
     * test needs the columns' lists and {@code columnValuesOnly} allows only
     * column comparisons.
     */
    String range(
        Object lower,
        Object upper,
        boolean includeLower,
        boolean includeUpper,
        ShapeRelation relation,
        String format,
        ZoneId zone,
        long now,
        boolean columnValuesOnly
    ) {
        // The query builder refuses a disjoint relation before it gets here, as stock does.
        ShapeRelation r = relation == null ? ShapeRelation.INTERSECTS : relation;
        String readBy = format != null ? format : this.format;
        double[] q = bounds(lower, upper, includeLower, includeUpper, readBy, zone, now);
        if (q == null) {
            return InfinoQueryTranslator.NONE;
        }
        String lo = literal(q[0]);
        String hi = literal(q[1]);
        return pairs(columnValuesOnly, (l, h) -> switch (r) {
            case WITHIN -> "(" + l + " >= " + lo + " AND " + h + " <= " + hi + ")";
            case CONTAINS -> "(" + l + " <= " + lo + " AND " + h + " >= " + hi + ")";
            default -> "(" + l + " <= " + hi + " AND " + h + " >= " + lo + ")";
        });
    }

    /** The rows a term on the field matches: a range holding {@code value}, as stock reads it (a range from it to it). */
    String term(Object value, long now, boolean columnValuesOnly) {
        if (value == null) {
            throw new IllegalArgumentException("[infino] a term query on [" + field + "] needs a value");
        }
        return range(value, value, true, true, ShapeRelation.INTERSECTS, null, null, now, columnValuesOnly);
    }

    /** The rows where the field has a value. */
    String exists() {
        String own = quoteIdent(from.name()) + " IS NOT NULL";
        return multi() ? "(" + own + " OR " + quoteIdent(fromList.name()) + " IS NOT NULL)" : own;
    }

    /**
     * The rows any of whose ranges ({@code low}, {@code high}: SQL
     * expressions of its ends) passes {@code test}: a document's one range,
     * or, where it gave several, any of its lists' pairs, through a subquery
     * by id. Null where only column comparisons are allowed and a list must
     * be read.
     */
    String pairs(boolean columnValuesOnly, PairTest test) {
        String own = test.apply(quoteIdent(from.name()), quoteIdent(to.name()));
        if (!multi()) {
            return own;
        }
        if (columnValuesOnly) {
            return null;
        }
        String lows = PAIR_SOURCE + "." + quoteIdent(fromList.name());
        String highs = PAIR_SOURCE + "." + quoteIdent(toList.name());
        String any = OSID + " IN (SELECT " + PAIR_ROWS + "." + PAIR_ID + " FROM (SELECT " + PAIR_SOURCE + "." + OSID + " AS " + PAIR_ID + ", unnest(" + lows
            + ") AS " + PAIR_LOW + ", unnest(" + highs + ") AS " + PAIR_HIGH + " FROM " + quoteIdent(table) + " " + PAIR_SOURCE + " WHERE " + lows
            + " IS NOT NULL) " + PAIR_ROWS + " WHERE " + test.apply(PAIR_ROWS + "." + PAIR_LOW, PAIR_ROWS + "." + PAIR_HIGH) + ")";
        return "((" + quoteIdent(fromList.name()) + " IS NULL AND " + own + ") OR " + any + ")";
    }

    /** A predicate over one range, given SQL expressions of its lowest and highest values. */
    @FunctionalInterface
    interface PairTest {
        String apply(String low, String high);
    }

    /**
     * A query's bounds as the inclusive lowest and highest values they
     * admit, as stock reads them: a bound left out is the type's least or
     * greatest value, an exclusive one the next value inside it, and a date
     * is read by {@code format}, an exclusive lower bound and an inclusive
     * upper one rounded up. Null when they admit no value.
     */
    double[] bounds(Object lower, Object upper, boolean includeLower, boolean includeUpper, String format, ZoneId zone, long now) {
        switch (kind) {
            case DATE -> {
                long lo = lower == null ? Long.MIN_VALUE : InfinoQueryTranslator.dateBoundMillis(lower, format, !includeLower, zone, now);
                long hi = upper == null ? Long.MAX_VALUE : InfinoQueryTranslator.dateBoundMillis(upper, format, includeUpper, zone, now);
                if (lower != null && !includeLower) {
                    if (lo == Long.MAX_VALUE) {
                        return null;
                    }
                    lo++;
                }
                if (upper != null && !includeUpper) {
                    if (hi == Long.MIN_VALUE) {
                        return null;
                    }
                    hi--;
                }
                return lo > hi ? null : new double[] { lo, hi };
            }
            case FLOAT -> {
                double lo = lower == null ? -DOUBLE_LIMIT : decimal(lower).doubleValue();
                double hi = upper == null ? DOUBLE_LIMIT : decimal(upper).doubleValue();
                if (lower != null && !includeLower) {
                    lo = Math.nextUp(lo);
                }
                if (upper != null && !includeUpper) {
                    hi = Math.nextDown(hi);
                }
                return lo > hi ? null : new double[] { lo, hi };
            }
            default -> {
                long min = "integer_range".equals(type) ? Integer.MIN_VALUE : Long.MIN_VALUE;
                long max = "integer_range".equals(type) ? Integer.MAX_VALUE : Long.MAX_VALUE;
                long lo = lower == null ? min : whole(lower, min, max);
                long hi = upper == null ? max : whole(upper, min, max);
                if (lower != null && !includeLower) {
                    if (lo == max) {
                        return null;
                    }
                    lo++;
                }
                if (upper != null && !includeUpper) {
                    if (hi == min) {
                        return null;
                    }
                    hi--;
                }
                return lo > hi ? null : new double[] { lo, hi };
            }
        }
    }

    /** A query's number, as stock parses a range field's bound. */
    private BigDecimal decimal(Object v) {
        try {
            return v instanceof Number n ? new BigDecimal(n.toString()) : new BigDecimal(v.toString().trim());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("For input string: \"" + v + "\"");
        }
    }

    /** A whole number from {@code min} to {@code max}, as stock parses an integer range's bound (no fraction). */
    private long whole(Object v, long min, long max) {
        BigDecimal d = decimal(v);
        if (d.stripTrailingZeros().scale() > 0) {
            throw new IllegalArgumentException("Value [" + v + "] has a decimal part");
        }
        if (d.compareTo(BigDecimal.valueOf(min)) < 0 || d.compareTo(BigDecimal.valueOf(max)) > 0) {
            throw new IllegalArgumentException("Value [" + v + "] is out of range for " + ("integer_range".equals(type) ? "an integer" : "a long"));
        }
        return d.longValueExact();
    }

    /**
     * A value of the field as an SQL literal its columns compare with: a
     * whole number as its digits, a double as its decimal form, a date as
     * the timestamp of its epoch milliseconds. Bounds are carried as doubles
     * until here; a long past a double's exact range is rounded, which no
     * range a document holds tells apart from its neighbour.
     */
    String literal(double v) {
        return switch (kind) {
            case DATE -> "arrow_cast(" + exactLong(v) + ", " + UTC_MILLIS + ")";
            case INT -> Long.toString(exactLong(v));
            case FLOAT -> Double.toString(v);
        };
    }

    /** {@code v} as a long, the extremes kept as the long extremes a double rounds them away from. */
    private static long exactLong(double v) {
        if (v >= (double) Long.MAX_VALUE) {
            return Long.MAX_VALUE;
        }
        if (v <= (double) Long.MIN_VALUE) {
            return Long.MIN_VALUE;
        }
        return (long) v;
    }

    // ---- aggregations ----

    /** One of the field's columns, an end of each range, as a double: a date as its epoch milliseconds. */
    private String number(String end) {
        return kind == Kind.DATE ? "CAST(arrow_cast(" + end + ", 'Int64') AS DOUBLE)" : "CAST(" + end + " AS DOUBLE)";
    }

    /**
     * The values a histogram reads from the field: for each range, values
     * from its lowest to its highest at most {@code step} apart, its highest
     * among them, so every bucket the range reaches holds one when no bucket
     * is narrower than {@code step}. A document then counts once in each such
     * bucket, as stock counts a range. {@code low} and {@code high}, when
     * not null, cut each range to the values the histogram's bounds keep.
     * More than {@code maxSamples} values for one range is refused before
     * any is read ({@link Samples#refuseWide}).
     */
    Samples samples(String agg, double step, Double low, Double high, long maxSamples, long maxBuckets) {
        if (!(step > 0) || Double.isInfinite(step)) {
            throw new InfinoAggregationProcessor.Unsupported("aggregation [" + agg + "] has an interval of no length");
        }
        return new Samples(this, agg, step, low, high, maxSamples, maxBuckets);
    }

    /** What {@link #samples} reads; see there. */
    record Samples(InfinoRangeField range, String agg, double step, Double low, Double high, long maxSamples, long maxBuckets) {

        /** The lowest and highest value of each range of a row of {@code relation}, cut to the bounds, as {@code low} and {@code high}. */
        private String ends(String relation, String low, String high) {
            String lo;
            String hi;
            if (range.multi()) {
                String f = quoteIdent(range.from.name());
                String t = quoteIdent(range.to.name());
                String fl = quoteIdent(range.fromList.name());
                String tl = quoteIdent(range.toList.name());
                lo = "unnest(CASE WHEN " + fl + " IS NULL THEN make_array(" + f + ") ELSE " + fl + " END)";
                hi = "unnest(CASE WHEN " + tl + " IS NULL THEN make_array(" + t + ") ELSE " + tl + " END)";
            } else {
                lo = quoteIdent(range.from.name());
                hi = quoteIdent(range.to.name());
            }
            return "SELECT *, " + lo + " AS " + low + ", " + hi + " AS " + high + " FROM " + relation;
        }

        /** The lowest value of a range, cut to the bounds, as a double. */
        private String cutLow(String low) {
            String n = range.number(low);
            return this.low == null ? n : "GREATEST(" + n + ", " + this.low + ")";
        }

        /** The highest value of a range, cut to the bounds, as a double. */
        private String cutHigh(String high) {
            String n = range.number(high);
            return this.high == null ? n : "LEAST(" + n + ", " + this.high + ")";
        }

        /**
         * How many steps a range spans past its lowest value, as a double:
         * one value more is read. A range the bounds leave nothing of spans
         * fewer than none.
         */
        private String steps(String low, String high) {
            return "CEIL((" + cutHigh(high) + " - " + cutLow(low) + ") / " + step + ")";
        }

        /**
         * That a row has a range the bounds leave something of. A missing end
         * is checked on its own: GREATEST and LEAST pass over a null.
         */
        private String reaches(String low, String high) {
            return low + " IS NOT NULL AND " + high + " IS NOT NULL AND " + cutHigh(high) + " >= " + cutLow(low);
        }

        /**
         * {@code relation} with a row per value read from each of its rows
         * ({@link InfinoRangeField#samples}), the value under {@code as}; a
         * row with no range, or one the bounds leave nothing of, has none.
         * {@code layer} names the subqueries.
         */
        String relation(String relation, String as, String layer) {
            String low = quoteIdent(layer + "_low");
            String high = quoteIdent(layer + "_high");
            String index = quoteIdent(layer + "_" + SAMPLE_INDEX);
            String withEnds = "(" + ends(relation + " " + layer + "_a", low, high) + ")";
            String steps = steps(low, high);
            // A range of more steps is in no row the histogram reads: refuseWide refuses the request first.
            String series = "(SELECT *, unnest(generate_series(0, CAST(" + steps + " AS BIGINT))) AS " + index + " FROM " + withEnds + " " + layer + "_b WHERE "
                + reaches(low, high) + " AND " + steps + " <= " + maxSamples + ")";
            String value = "LEAST(" + cutLow(low) + " + " + index + " * " + step + ", " + cutHigh(high) + ")";
            String typed = range.kind == Kind.DATE ? "arrow_cast(CAST(" + value + " AS BIGINT), " + UTC_MILLIS + ")" : value;
            return "(SELECT * EXCLUDE (" + low + ", " + high + ", " + index + "), " + typed + " AS " + as + " FROM " + series + " " + layer + "_c)";
        }

        /**
         * Refuse the histogram when a range of {@code from WHERE where}
         * reaches more buckets than one request may build: stock refuses it
         * for too many buckets, and reading it would read that many values.
         */
        void refuseWide(String from, String where) {
            String low = quoteIdent("wide_low");
            String high = quoteIdent("wide_high");
            String rows = "(" + ends("(SELECT * FROM " + from + " WHERE " + where + ") wide_a", low, high) + ") wide_b";
            String sql = "SELECT 1 AS wide FROM " + rows + " WHERE " + reaches(low, high) + " AND " + steps(low, high) + " > " + maxSamples + " LIMIT 1";
            if (!InfinoNative.sqlRows(sql).isEmpty()) {
                throw new InfinoAggregationProcessor.Unsupported(
                    "aggregation ["
                        + agg
                        + "] over the range field ["
                        + range.field
                        + "] would create too many buckets: a document's range reaches more than ["
                        + maxBuckets
                        + "] of them. This limit can be set by changing the [search.max_buckets] cluster level setting."
                );
            }
        }

        /** The rows with a range reaching the bucket of values from {@code start} up to {@code end}, which it excludes. */
        String reaching(double start, double end) {
            String s = range.literal(start);
            String e = range.literal(end);
            return range.pairs(false, (l, h) -> "(" + l + " < " + e + " AND " + h + " >= " + s + ")");
        }

        /** The rows with no range. */
        String noValue() {
            String own = quoteIdent(range.from.name()) + " IS NULL";
            return range.multi() ? "(" + own + " AND " + quoteIdent(range.fromList.name()) + " IS NULL)" : own;
        }
    }
}
