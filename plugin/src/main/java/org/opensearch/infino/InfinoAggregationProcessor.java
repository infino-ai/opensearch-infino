/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.BytesRef;
import org.opensearch.common.Rounding;
import org.opensearch.common.time.DateFormatter;
import org.opensearch.index.mapper.DateFieldMapper;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.infino.InfinoNative.Column;
import org.opensearch.infino.InfinoQueryTranslator.Translation;
import org.opensearch.infino.InfinoSearch.Plan;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.BucketOrder;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.search.aggregations.InternalOrder;
import org.opensearch.search.aggregations.bucket.filter.FiltersAggregationBuilder;
import org.opensearch.search.aggregations.bucket.filter.FiltersAggregator;
import org.opensearch.search.aggregations.bucket.filter.InternalFilters;
import org.opensearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.opensearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.opensearch.search.aggregations.bucket.histogram.DateIntervalWrapper;
import org.opensearch.search.aggregations.bucket.histogram.HistogramAggregationBuilder;
import org.opensearch.search.aggregations.bucket.histogram.InternalDateHistogram;
import org.opensearch.search.aggregations.bucket.histogram.InternalHistogram;
import org.opensearch.search.aggregations.bucket.histogram.LongBounds;
import org.opensearch.search.aggregations.bucket.range.DateRangeAggregationBuilder;
import org.opensearch.search.aggregations.bucket.range.InternalDateRange;
import org.opensearch.search.aggregations.bucket.range.InternalRange;
import org.opensearch.search.aggregations.bucket.range.RangeAggregationBuilder;
import org.opensearch.search.aggregations.bucket.range.RangeAggregator;
import org.opensearch.search.aggregations.bucket.terms.LongTerms;
import org.opensearch.search.aggregations.bucket.terms.StringTerms;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregator;
import org.opensearch.search.aggregations.metrics.AvgAggregationBuilder;
import org.opensearch.search.aggregations.metrics.CardinalityAggregationBuilder;
import org.opensearch.search.aggregations.metrics.InternalAvg;
import org.opensearch.search.aggregations.metrics.InternalMax;
import org.opensearch.search.aggregations.metrics.InternalMin;
import org.opensearch.search.aggregations.metrics.InternalSum;
import org.opensearch.search.aggregations.metrics.InternalTDigestPercentiles;
import org.opensearch.search.aggregations.metrics.InternalValueCount;
import org.opensearch.search.aggregations.metrics.MaxAggregationBuilder;
import org.opensearch.search.aggregations.metrics.MinAggregationBuilder;
import org.opensearch.search.aggregations.metrics.PercentilesAggregationBuilder;
import org.opensearch.search.aggregations.metrics.PercentilesConfig;
import org.opensearch.search.aggregations.metrics.SumAggregationBuilder;
import org.opensearch.search.aggregations.metrics.TDigestState;
import org.opensearch.search.aggregations.metrics.ValueCountAggregationBuilder;
import org.opensearch.search.aggregations.support.ValuesSourceAggregationBuilder;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.opensearch.infino.InfinoQueryTranslator.literal;
import static org.opensearch.infino.InfinoQueryTranslator.quoteIdent;

/**
 * Serves aggregations from Infino SQL for Infino-backed indices, over the
 * table's typed columns, scoped by the same relation and predicate the hits
 * use (so a full-text query scopes its aggregations through the engine's
 * token index, never through a materialized id list).
 *
 * <p>Three kinds of aggregation are planned:
 * <ul>
 *   <li><b>metrics</b> ({@code value_count}, {@code sum}, {@code avg},
 *       {@code min}, {@code max}, {@code cardinality}, {@code percentiles}):
 *       aggregate columns of the query at their level;</li>
 *   <li><b>grouped buckets</b> ({@code terms}, {@code date_histogram},
 *       {@code histogram}): a chain of consecutive grouped levels is answered
 *       with one {@code GROUP BY} per depth, each level grouping by every key
 *       above it, with the per-parent size of a terms level applied by a
 *       window function, and the rows folded into nested buckets;</li>
 *   <li><b>explicit buckets</b> ({@code filters}, {@code range},
 *       {@code date_range}): one query per bucket, the bucket's predicate
 *       added to the request's, since buckets may overlap.</li>
 * </ul>
 * Pipeline aggregations need nothing here: the final reduction computes them
 * from the buckets returned. A request with an aggregation outside this
 * surface is refused with the reason, rather than answered empty.
 */
public class InfinoAggregationProcessor {

    private static final Logger log = LogManager.getLogger(InfinoAggregationProcessor.class);
    /** Buckets a terms level may return; past this the window is not worth a GROUP BY. */
    private static final int MAX_TERMS_SIZE = 10_000;
    /** Parent buckets a nested grouped level is restricted to; past this the key list is not worth a predicate. */
    private static final int MAX_PARENT_TUPLES = 1_000;
    /** How bucket keys are printed when the request names no format. */
    private static final String DEFAULT_DATE_FORMAT = "strict_date_optional_time||epoch_millis";
    /** The t-digest compression OpenSearch uses when the request names none. */
    private static final double DEFAULT_TDIGEST_COMPRESSION = 100.0;

    /** An aggregation the table cannot serve; the request fails with this reason. */
    static final class Unsupported extends IllegalArgumentException {
        Unsupported(String message) {
            super("[infino] " + message);
        }
    }

    /**
     * The requested aggregations over the plan's relation, in request order,
     * as the stock response objects. {@code total} is the request's exact
     * match count when the hit path already has it, else null.
     */
    public InternalAggregations evaluate(Collection<AggregationBuilder> builders, Plan plan, Long total) {
        Translation q = plan.query();
        List<Spec> specs = new ArrayList<>();
        int position = 0;
        for (AggregationBuilder b : builders) {
            specs.add(Spec.of(b, plan.cols(), plan.table(), position++));
        }
        Map<String, InternalAggregation> byName = new HashMap<>();
        List<Metric> rootMetrics = new ArrayList<>();
        for (Spec spec : specs) {
            if (spec instanceof Metric m) {
                rootMetrics.add(m);
                continue;
            }
            if (total == null) {
                total = InfinoSearch.count(plan.table(), q);
            }
            byName.put(spec.name(), evalBucket((BucketSpec) spec, q.from(), q.where(), total));
        }
        if (!rootMetrics.isEmpty()) {
            Map<String, Object> row = oneRow("SELECT " + metricColumns(rootMetrics) + " FROM " + q.from() + " WHERE " + q.where());
            for (Metric m : rootMetrics) {
                byName.put(m.name(), m.build(row));
            }
        }
        // Keep the request's order of aggregations.
        List<InternalAggregation> ordered = new ArrayList<>();
        for (AggregationBuilder b : builders) {
            ordered.add(byName.get(b.getName()));
        }
        log.debug("[infino] served {} aggregation(s) from {}", ordered.size(), plan.table());
        return InternalAggregations.from(ordered);
    }

    // ---- servability (shared with the hit path's plan) ----

    /** Plans every requested aggregation; throws {@link Unsupported} naming the first that cannot be served. */
    static void check(Collection<AggregationBuilder> builders, Plan plan) {
        int position = 0;
        for (AggregationBuilder b : builders) {
            Spec.of(b, plan.cols(), plan.table(), position++);
        }
    }

    // ---- specs ----

    /** One aggregation the table can serve. */
    abstract static class Spec {
        final AggregationBuilder builder;

        Spec(AggregationBuilder builder) {
            this.builder = builder;
        }

        String name() {
            return builder.getName();
        }

        /** The spec for a builder at {@code index} among its siblings, or an {@link Unsupported} reason. */
        static Spec of(AggregationBuilder b, Map<String, Column> cols, String table, int index) {
            if (Metric.handles(b)) {
                if (!b.getSubAggregations().isEmpty()) {
                    throw new Unsupported("metric aggregation [" + b.getName() + "] cannot have sub-aggregations");
                }
                return Metric.of(b, cols, index);
            }
            List<Metric> metrics = new ArrayList<>();
            BucketSpec child = null;
            int i = 0;
            for (AggregationBuilder sub : b.getSubAggregations()) {
                Spec s = of(sub, cols, table, i++);
                if (s instanceof Metric m) {
                    metrics.add(m);
                } else if (child == null) {
                    child = (BucketSpec) s;
                } else {
                    throw new Unsupported("aggregation [" + b.getName() + "] has more than one nested bucket aggregation");
                }
            }
            if (b instanceof TermsAggregationBuilder t) {
                return Grouped.terms(t, cols, metrics, child);
            }
            if (b instanceof DateHistogramAggregationBuilder dh) {
                return Grouped.dateHistogram(dh, cols, metrics, child);
            }
            if (b instanceof HistogramAggregationBuilder h) {
                return Grouped.histogram(h, cols, metrics, child);
            }
            if (b instanceof FiltersAggregationBuilder f) {
                return Explicit.filters(f, cols, table, metrics, child);
            }
            if (b instanceof RangeAggregationBuilder r) {
                return Explicit.range(r, cols, metrics, child);
            }
            if (b instanceof DateRangeAggregationBuilder dr) {
                return Explicit.dateRange(dr, cols, metrics, child);
            }
            throw new Unsupported("aggregation [" + b.getName() + "] of type [" + b.getType() + "] is not served");
        }

        static Column columnOf(ValuesSourceAggregationBuilder<?> b, Map<String, Column> cols) {
            if (b.field() == null) {
                throw new Unsupported("aggregation [" + b.getName() + "] needs a field");
            }
            Column c = InfinoSearch.columnFor(cols, b.field());
            if (c == null) {
                throw new Unsupported("aggregation [" + b.getName() + "] names field [" + b.field() + "], which the table has no column for");
            }
            return c;
        }
    }

    /** What a metric computes. */
    enum MetricKind {
        VALUE_COUNT,
        SUM,
        AVG,
        MIN,
        MAX,
        CARDINALITY,
        PERCENTILES
    }

    /** A metric over one column: the SQL aggregate columns it needs and the result it builds. */
    static final class Metric extends Spec {
        final Column column;
        final String alias;
        final MetricKind kind;
        final double[] percents;
        final boolean keyed;
        final double compression;

        private Metric(AggregationBuilder b, Column column, String alias, MetricKind kind, double[] percents, boolean keyed, double compression) {
            super(b);
            this.column = column;
            this.alias = alias;
            this.kind = kind;
            this.percents = percents;
            this.keyed = keyed;
            this.compression = compression;
        }

        static boolean handles(AggregationBuilder b) {
            return b instanceof ValueCountAggregationBuilder
                || b instanceof SumAggregationBuilder
                || b instanceof AvgAggregationBuilder
                || b instanceof MinAggregationBuilder
                || b instanceof MaxAggregationBuilder
                || b instanceof CardinalityAggregationBuilder
                || b instanceof PercentilesAggregationBuilder;
        }

        static Metric of(AggregationBuilder b, Map<String, Column> cols, int index) {
            Column c = columnOf((ValuesSourceAggregationBuilder<?>) b, cols);
            String alias = "m" + index;
            boolean numeric = c.isNumeric() || c.isTimestamp();
            if (b instanceof ValueCountAggregationBuilder) {
                return new Metric(b, c, alias, MetricKind.VALUE_COUNT, null, false, 0);
            }
            if (b instanceof CardinalityAggregationBuilder) {
                return new Metric(b, c, alias, MetricKind.CARDINALITY, null, false, 0);
            }
            if (!numeric) {
                throw new Unsupported("aggregation [" + b.getName() + "] needs a numeric or date field, [" + c.name() + "] is " + c.type());
            }
            if (b instanceof SumAggregationBuilder || b instanceof AvgAggregationBuilder) {
                if (c.isTimestamp()) {
                    throw new Unsupported("aggregation [" + b.getName() + "] cannot sum a date field");
                }
                return new Metric(b, c, alias, b instanceof SumAggregationBuilder ? MetricKind.SUM : MetricKind.AVG, null, false, 0);
            }
            if (b instanceof MinAggregationBuilder) {
                return new Metric(b, c, alias, MetricKind.MIN, null, false, 0);
            }
            if (b instanceof MaxAggregationBuilder) {
                return new Metric(b, c, alias, MetricKind.MAX, null, false, 0);
            }
            PercentilesAggregationBuilder p = (PercentilesAggregationBuilder) b;
            PercentilesConfig config = p.percentilesConfig();
            if (config != null && !(config instanceof PercentilesConfig.TDigest)) {
                throw new Unsupported("aggregation [" + b.getName() + "] uses the HDR method; only t-digest is served");
            }
            double compression = config instanceof PercentilesConfig.TDigest td ? td.getCompression() : DEFAULT_TDIGEST_COMPRESSION;
            double[] percents = p.percentiles();
            if (percents == null || percents.length == 0) {
                throw new Unsupported("aggregation [" + b.getName() + "] names no percents");
            }
            return new Metric(b, c, alias, MetricKind.PERCENTILES, percents, p.keyed(), compression);
        }

        /** The SELECT list entries for this metric. */
        String columns() {
            String f = quoteIdent(column.name());
            return switch (kind) {
                case VALUE_COUNT -> "COUNT(" + f + ") AS " + alias + "_c";
                case SUM -> "SUM(" + f + ") AS " + alias + "_s";
                case AVG -> "SUM(" + f + ") AS " + alias + "_s, COUNT(" + f + ") AS " + alias + "_c";
                case MIN -> "MIN(" + f + ") AS " + alias + "_v";
                case MAX -> "MAX(" + f + ") AS " + alias + "_v";
                case CARDINALITY -> "COUNT(DISTINCT " + f + ") AS " + alias + "_d";
                case PERCENTILES -> {
                    List<String> cols = new ArrayList<>();
                    for (int i = 0; i < percents.length; i++) {
                        cols.add("approx_percentile_cont(" + f + ", " + (percents[i] / 100.0) + ") AS " + alias + "_p" + i);
                    }
                    cols.add("MIN(" + f + ") AS " + alias + "_mn");
                    cols.add("MAX(" + f + ") AS " + alias + "_mx");
                    cols.add("COUNT(" + f + ") AS " + alias + "_c");
                    yield String.join(", ", cols);
                }
            };
        }

        /** The alias a terms order by this metric sorts on, or null when it has no single column. */
        String orderAlias() {
            return switch (kind) {
                case VALUE_COUNT -> alias + "_c";
                case SUM -> alias + "_s";
                case MIN, MAX -> alias + "_v";
                case CARDINALITY -> alias + "_d";
                case AVG, PERCENTILES -> null;
            };
        }

        InternalAggregation build(Map<String, Object> row) {
            String name = builder.getName();
            Map<String, Object> meta = builder.getMetadata();
            return switch (kind) {
                case VALUE_COUNT -> new InternalValueCount(name, asLong(row.get(alias + "_c")), meta);
                case SUM -> new InternalSum(name, asDouble(row.get(alias + "_s"), 0.0), DocValueFormat.RAW, meta);
                case AVG -> new InternalAvg(name, asDouble(row.get(alias + "_s"), 0.0), asLong(row.get(alias + "_c")), DocValueFormat.RAW, meta);
                case MIN -> new InternalMin(name, asDouble(row.get(alias + "_v"), Double.POSITIVE_INFINITY), DocValueFormat.RAW, meta);
                case MAX -> new InternalMax(name, asDouble(row.get(alias + "_v"), Double.NEGATIVE_INFINITY), DocValueFormat.RAW, meta);
                case CARDINALITY -> InfinoAggregationWire.cardinality(name, meta, asLong(row.get(alias + "_d")));
                case PERCENTILES -> percentiles(row);
            };
        }

        /**
         * A t-digest that answers the requested percents with the engine's
         * values. The digest reads quantile {@code q} at position {@code q * count}
         * and returns a single-row centroid's value when the position falls on
         * it, so each engine value goes in as a single-row centroid at the
         * position its percent names, with filler centroids carrying the rows
         * between them. The minimum and maximum are the single-row end
         * centroids the digest requires; everything else stays strictly inside.
         */
        private InternalAggregation percentiles(Map<String, Object> row) {
            TDigestState state = new TDigestState(compression);
            long count = asLong(row.get(alias + "_c"));
            if (count > 0) {
                double min = asDouble(row.get(alias + "_mn"), 0.0);
                double max = asDouble(row.get(alias + "_mx"), min);
                state.add(min, 1);
                if (count > 1) {
                    state.add(max, 1);
                }
                double innerMin = Math.nextUp(min);
                double innerMax = Math.nextDown(max);
                long interior = count - 2;
                if (interior > 0 && innerMin <= innerMax) {
                    placeInterior(state, row, count, innerMin, innerMax);
                }
            }
            return new InternalTDigestPercentiles(builder.getName(), percents, state, keyed, DocValueFormat.RAW, builder.getMetadata());
        }

        /** The percent centroids in percent order, each at its position, fillers between; the end rows stay with the caller. */
        private void placeInterior(TDigestState state, Map<String, Object> row, long count, double innerMin, double innerMax) {
            int n = percents.length;
            Integer[] order = new Integer[n];
            for (int i = 0; i < n; i++) {
                order[i] = i;
            }
            Arrays.sort(order, Comparator.comparingDouble(i -> percents[i]));
            double[] value = new double[n];
            for (int i = 0; i < n; i++) {
                value[i] = Math.min(innerMax, Math.max(innerMin, asDouble(row.get(alias + "_p" + i), innerMin)));
            }
            // Rows placed so far: the minimum is row 0, so the interior starts at 1
            // and must end before the maximum at count - 1.
            long placed = 1;
            double previous = innerMin;
            for (int k = 0; k < n; k++) {
                int i = order[k];
                long remaining = n - k;
                long position = Math.round(percents[i] / 100.0 * count);
                long at = Math.max(placed, Math.min(position, count - 1 - remaining));
                long filler = at - placed;
                if (filler > 0) {
                    state.add((previous + value[i]) / 2, (int) Math.min(Integer.MAX_VALUE, filler));
                }
                state.add(value[i], 1);
                placed = at + 1;
                previous = value[i];
            }
            long filler = count - 1 - placed;
            if (filler > 0) {
                state.add((previous + innerMax) / 2, (int) Math.min(Integer.MAX_VALUE, filler));
            }
        }
    }

    /** A bucket aggregation: the metrics under it and at most one nested bucket aggregation. */
    abstract static class BucketSpec extends Spec {
        final List<Metric> metrics;
        final BucketSpec child;

        BucketSpec(AggregationBuilder b, List<Metric> metrics, BucketSpec child) {
            super(b);
            this.metrics = metrics;
            this.child = child;
        }

        /** The sub-aggregations of a bucket: its metrics from the row, and the nested aggregation given. */
        InternalAggregations subs(Map<String, Object> row, InternalAggregation nested) {
            List<InternalAggregation> out = new ArrayList<>();
            for (Metric m : metrics) {
                out.add(m.build(row));
            }
            if (nested != null) {
                out.add(nested);
            }
            return out.isEmpty() ? InternalAggregations.EMPTY : InternalAggregations.from(out);
        }

        /** The sub-aggregations of an empty bucket. */
        InternalAggregations emptySubs() {
            return subs(Map.of(), child == null ? null : child.empty());
        }

        /** This aggregation with no buckets. */
        abstract InternalAggregation empty();
    }

    /** The type of a grouped level's key. */
    enum KeyKind {
        STR,
        BOOL,
        LONG,
        DOUBLE,
        DATE
    }

    /** How a date histogram keys its buckets. */
    record DateKey(DocValueFormat format, Rounding rounding, LongBounds bounds, boolean keyed, long offset) {}

    /** How a numeric histogram keys its buckets. */
    record HistKey(double interval, double offset, boolean keyed) {}

    /** A grouped bucket level: terms, date_histogram or histogram. */
    static final class Grouped extends BucketSpec {
        final Column column;
        final String keyExpr;
        final KeyKind keyKind;
        final boolean terms;
        final int size;
        final long minDocCount;
        final String orderTemplate;
        final DateKey date;
        final HistKey hist;

        private Grouped(
            AggregationBuilder b,
            List<Metric> metrics,
            BucketSpec child,
            Column column,
            String keyExpr,
            KeyKind keyKind,
            boolean terms,
            int size,
            long minDocCount,
            String orderTemplate,
            DateKey date,
            HistKey hist
        ) {
            super(b, metrics, child);
            this.column = column;
            this.keyExpr = keyExpr;
            this.keyKind = keyKind;
            this.terms = terms;
            this.size = size;
            this.minDocCount = minDocCount;
            this.orderTemplate = orderTemplate;
            this.date = date;
            this.hist = hist;
        }

        static Grouped terms(TermsAggregationBuilder t, Map<String, Column> cols, List<Metric> metrics, BucketSpec child) {
            Column c = columnOf(t, cols);
            if (t.size() > MAX_TERMS_SIZE) {
                throw new Unsupported("aggregation [" + t.getName() + "] asks for " + t.size() + " buckets; at most " + MAX_TERMS_SIZE + " are served");
            }
            KeyKind kind = kindOf(c);
            String order = orderClause(t.order(), metrics);
            if (order == null) {
                throw new Unsupported("aggregation [" + t.getName() + "] has an order the table cannot sort by: " + t.order());
            }
            return new Grouped(t, metrics, child, c, quoteIdent(c.name()), kind, true, t.size(), t.minDocCount(), order, null, null);
        }

        static Grouped dateHistogram(DateHistogramAggregationBuilder dh, Map<String, Column> cols, List<Metric> metrics, BucketSpec child) {
            Column c = columnOf(dh, cols);
            if (!c.isTimestamp()) {
                throw new Unsupported("aggregation [" + dh.getName() + "] needs a date field, [" + c.name() + "] is " + c.type());
            }
            ZoneId tz = dh.timeZone() == null ? ZoneOffset.UTC : dh.timeZone();
            String source = quoteIdent(c.name());
            if (!ZoneOffset.UTC.equals(tz) && !"UTC".equals(tz.getId())) {
                // Bucket boundaries fall on local time in the request's zone; the
                // epoch value of each boundary is unchanged.
                source = "arrow_cast(" + source + ", 'Timestamp(Millisecond, Some(\"" + tz.getId() + "\"))')";
            }
            String keyExpr;
            DateIntervalWrapper wrapper = new DateIntervalWrapper();
            if (dh.getFixedInterval() != null) {
                long ms = dh.getFixedInterval().estimateMillis();
                if (ms <= 0) {
                    throw new Unsupported("aggregation [" + dh.getName() + "] has an interval of no length");
                }
                String origin = dh.offset() == 0 ? "" : ", TIMESTAMP '1970-01-01T00:00:00Z' + INTERVAL '" + dh.offset() + " milliseconds'";
                keyExpr = "date_bin(INTERVAL '" + ms + " milliseconds', " + source + origin + ")";
                wrapper.fixedInterval(dh.getFixedInterval());
            } else if (dh.getCalendarInterval() != null) {
                if (dh.offset() != 0) {
                    throw new Unsupported("aggregation [" + dh.getName() + "] combines a calendar interval with an offset");
                }
                String unit = calendarUnit(dh.getCalendarInterval());
                if (unit == null) {
                    throw new Unsupported("aggregation [" + dh.getName() + "] has calendar interval [" + dh.getCalendarInterval() + "]; one unit at a time is served");
                }
                keyExpr = "date_trunc('" + unit + "', " + source + ")";
                wrapper.calendarInterval(dh.getCalendarInterval());
            } else {
                throw new Unsupported("aggregation [" + dh.getName() + "] names no interval");
            }
            Rounding rounding = wrapper.createRounding(tz, dh.offset());
            String fmt = dh.format() == null ? DEFAULT_DATE_FORMAT : dh.format();
            DocValueFormat format = new DocValueFormat.DateTime(DateFormatter.forPattern(fmt), tz, DateFieldMapper.Resolution.MILLISECONDS);
            LongBounds bounds = null;
            if (dh.extendedBounds() != null) {
                // Bounds given as epoch milliseconds (what dashboards send) are rounded to the
                // histogram's grid; bounds given as date strings are not readable from here.
                Long min = dh.extendedBounds().getMin();
                Long max = dh.extendedBounds().getMax();
                if (min == null && max == null) {
                    throw new Unsupported("aggregation [" + dh.getName() + "] gives extended_bounds as dates; epoch milliseconds are served");
                }
                Rounding.Prepared prepared = rounding.prepareForUnknown();
                bounds = new LongBounds(min == null ? null : prepared.round(min), max == null ? null : prepared.round(max));
            }
            DateKey date = new DateKey(format, rounding, bounds, dh.keyed(), dh.offset());
            return new Grouped(dh, metrics, child, c, keyExpr, KeyKind.DATE, false, Integer.MAX_VALUE, dh.minDocCount(), "{key} ASC", date, null);
        }

        static Grouped histogram(HistogramAggregationBuilder h, Map<String, Column> cols, List<Metric> metrics, BucketSpec child) {
            Column c = columnOf(h, cols);
            if (!c.isNumeric()) {
                throw new Unsupported("aggregation [" + h.getName() + "] needs a numeric field, [" + c.name() + "] is " + c.type());
            }
            if (h.interval() <= 0) {
                throw new Unsupported("aggregation [" + h.getName() + "] has an interval of no length");
            }
            String f = quoteIdent(c.name());
            String keyExpr = "floor((" + f + " - " + h.offset() + ") / " + h.interval() + ") * " + h.interval() + " + " + h.offset();
            if (h.order() != null && !InternalOrder.isKeyOrder(h.order())) {
                throw new Unsupported("aggregation [" + h.getName() + "] has an order other than by key");
            }
            String order = h.order() != null && InternalOrder.isKeyDesc(h.order()) ? "{key} DESC" : "{key} ASC";
            HistKey hist = new HistKey(h.interval(), h.offset(), h.keyed());
            return new Grouped(h, metrics, child, c, keyExpr, KeyKind.DOUBLE, false, Integer.MAX_VALUE, h.minDocCount(), order, null, hist);
        }

        private static KeyKind kindOf(Column c) {
            if (c.isString()) {
                return KeyKind.STR;
            }
            if (c.isBoolean()) {
                return KeyKind.BOOL;
            }
            if (c.isTimestamp()) {
                return KeyKind.DATE;
            }
            if ("i64".equals(c.type())) {
                return KeyKind.LONG;
            }
            if ("f64".equals(c.type())) {
                return KeyKind.DOUBLE;
            }
            throw new Unsupported("field [" + c.name() + "] of type " + c.type() + " cannot key buckets");
        }

        /** The calendar unit `date_trunc` takes, or null for one it has no name for. */
        private static String calendarUnit(DateHistogramInterval interval) {
            return switch (interval.toString()) {
                case "1m", "minute" -> "minute";
                case "1h", "hour" -> "hour";
                case "1d", "day" -> "day";
                case "1w", "week" -> "week";
                case "1M", "month" -> "month";
                case "1q", "quarter" -> "quarter";
                case "1y", "year" -> "year";
                default -> null;
            };
        }

        /** ORDER BY for this level's buckets, {@code keyAlias} standing in for its key. */
        String order(String keyAlias) {
            return orderTemplate.replace("{key}", keyAlias);
        }

        /** The key as an SQL literal, for restricting a nested level to a parent's bucket. */
        String keyLiteral(Object key) {
            return switch (keyKind) {
                case STR -> literal(key.toString());
                case BOOL -> String.valueOf(Boolean.parseBoolean(key.toString()));
                case LONG -> Long.toString(asLong(key));
                case DOUBLE -> Double.toString(asDouble(key, 0.0));
                case DATE -> literal(Instant.ofEpochMilli(asLong(key)).toString());
            };
        }

        /** Build this level's aggregation from its rows; {@code subsOf} gives each row's sub-aggregations. */
        InternalAggregation build(List<Map<String, Object>> rows, String keyAlias, Function<Map<String, Object>, InternalAggregations> subsOf, long parentTotal) {
            if (terms) {
                return buildTerms(rows, keyAlias, subsOf, parentTotal);
            }
            if (date != null) {
                return buildDateHistogram(rows, keyAlias, subsOf);
            }
            return buildHistogram(rows, keyAlias, subsOf);
        }

        @Override
        InternalAggregation empty() {
            return build(List.of(), "k0", row -> emptySubs(), 0);
        }

        private InternalAggregation buildTerms(List<Map<String, Object>> rows, String keyAlias, Function<Map<String, Object>, InternalAggregations> subsOf, long parentTotal) {
            TermsAggregationBuilder t = (TermsAggregationBuilder) builder;
            long sumTop = 0;
            List<StringTerms.Bucket> sBuckets = new ArrayList<>();
            List<LongTerms.Bucket> lBuckets = new ArrayList<>();
            List<InfinoAggregationWire.DoubleBucket> dBuckets = new ArrayList<>();
            for (Map<String, Object> row : rows) {
                Object key = row.get(keyAlias);
                if (key == null) {
                    continue; // a terms aggregation has no bucket for the missing value
                }
                long docCount = asLong(row.get("doc_count"));
                sumTop += docCount;
                InternalAggregations subs = subsOf.apply(row);
                switch (keyKind) {
                    case STR, BOOL -> sBuckets.add(new StringTerms.Bucket(new BytesRef(key.toString()), docCount, subs, false, 0L, DocValueFormat.RAW));
                    case LONG, DATE -> lBuckets.add(new LongTerms.Bucket(asLong(key), docCount, subs, false, 0L, DocValueFormat.RAW));
                    case DOUBLE -> dBuckets.add(new InfinoAggregationWire.DoubleBucket(asDouble(key, 0.0), docCount, subs));
                }
            }
            long otherDocCount = Math.max(0, parentTotal - sumTop);
            TermsAggregator.BucketCountThresholds thresholds = new TermsAggregator.BucketCountThresholds(t.minDocCount(), t.shardMinDocCount(), t.size(), t.shardSize());
            return switch (keyKind) {
                case STR, BOOL -> new StringTerms(t.getName(), t.order(), t.order(), t.getMetadata(), DocValueFormat.RAW, t.shardSize(), false, otherDocCount, sBuckets, 0L, thresholds);
                case LONG, DATE -> new LongTerms(t.getName(), t.order(), t.order(), t.getMetadata(), DocValueFormat.RAW, t.shardSize(), false, otherDocCount, lBuckets, 0L, thresholds);
                // The double bucket class is not public in every release built against; the object comes through its stream form.
                case DOUBLE -> InfinoAggregationWire.doubleTerms(t.getName(), t.getMetadata(), t.order(), t.size(), t.minDocCount(), otherDocCount, dBuckets);
            };
        }

        private InternalAggregation buildDateHistogram(List<Map<String, Object>> rows, String keyAlias, Function<Map<String, Object>, InternalAggregations> subsOf) {
            List<InternalDateHistogram.Bucket> buckets = new ArrayList<>();
            for (Map<String, Object> row : rows) {
                Object key = row.get(keyAlias);
                if (key == null) {
                    continue;
                }
                buckets.add(new InternalDateHistogram.Bucket(asLong(key), asLong(row.get("doc_count")), date.keyed(), date.format(), subsOf.apply(row)));
            }
            InternalAggregations empty = minDocCount == 0 ? emptySubs() : null;
            return InfinoAggregationWire.dateHistogram(
                builder.getName(),
                builder.getMetadata(),
                buckets,
                minDocCount,
                date.offset(),
                date.rounding(),
                empty,
                date.bounds(),
                date.format(),
                date.keyed()
            );
        }

        private InternalAggregation buildHistogram(List<Map<String, Object>> rows, String keyAlias, Function<Map<String, Object>, InternalAggregations> subsOf) {
            List<InternalHistogram.Bucket> buckets = new ArrayList<>();
            for (Map<String, Object> row : rows) {
                Object key = row.get(keyAlias);
                if (key == null) {
                    continue;
                }
                buckets.add(new InternalHistogram.Bucket(asDouble(key, 0.0), asLong(row.get("doc_count")), hist.keyed(), DocValueFormat.RAW, subsOf.apply(row)));
            }
            InternalHistogram.EmptyBucketInfo empty = minDocCount == 0
                ? new InternalHistogram.EmptyBucketInfo(hist.interval(), hist.offset(), Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, emptySubs())
                : null;
            HistogramAggregationBuilder h = (HistogramAggregationBuilder) builder;
            return new InternalHistogram(h.getName(), buckets, h.order(), minDocCount, empty, DocValueFormat.RAW, hist.keyed(), h.getMetadata());
        }
    }

    /** One explicit bucket: its key, its predicate, and its bounds for a range. */
    record ExplicitBucket(String key, String predicate, double from, double to) {}

    /** What an explicit level is. */
    enum ExplicitKind {
        FILTERS,
        RANGE,
        DATE_RANGE
    }

    /** An explicit bucket level: filters, range or date_range, each bucket its own predicate. */
    static final class Explicit extends BucketSpec {
        final ExplicitKind kind;
        final List<ExplicitBucket> buckets;
        final boolean keyed;
        final DocValueFormat format;

        private Explicit(AggregationBuilder b, List<Metric> metrics, BucketSpec child, ExplicitKind kind, List<ExplicitBucket> buckets, boolean keyed, DocValueFormat format) {
            super(b, metrics, child);
            this.kind = kind;
            this.buckets = buckets;
            this.keyed = keyed;
            this.format = format;
        }

        static Explicit filters(FiltersAggregationBuilder f, Map<String, Column> cols, String table, List<Metric> metrics, BucketSpec child) {
            List<ExplicitBucket> buckets = new ArrayList<>();
            List<String> all = new ArrayList<>();
            for (FiltersAggregator.KeyedFilter kf : f.filters()) {
                String p = InfinoQueryTranslator.predicate(kf.filter(), cols, table, System.currentTimeMillis());
                if (p == null) {
                    throw new Unsupported("aggregation [" + f.getName() + "] has a filter the table cannot answer: " + kf.filter());
                }
                buckets.add(new ExplicitBucket(kf.key(), p, Double.NaN, Double.NaN));
                all.add("(" + p + ")");
            }
            if (f.otherBucket()) {
                String other = all.isEmpty() ? InfinoQueryTranslator.ALL : "NOT (" + String.join(" OR ", all) + ")";
                buckets.add(new ExplicitBucket(f.otherBucketKey(), other, Double.NaN, Double.NaN));
            }
            return new Explicit(f, metrics, child, ExplicitKind.FILTERS, buckets, f.isKeyed(), DocValueFormat.RAW);
        }

        static Explicit range(RangeAggregationBuilder r, Map<String, Column> cols, List<Metric> metrics, BucketSpec child) {
            Column c = columnOf(r, cols);
            if (!c.isNumeric()) {
                throw new Unsupported("aggregation [" + r.getName() + "] needs a numeric field, [" + c.name() + "] is " + c.type());
            }
            List<ExplicitBucket> buckets = new ArrayList<>();
            for (RangeAggregator.Range range : r.ranges()) {
                double from = range.getFrom();
                double to = range.getTo();
                buckets.add(new ExplicitBucket(range.getKey(), between(c, from, to), from, to));
            }
            return new Explicit(r, metrics, child, ExplicitKind.RANGE, buckets, r.keyed(), DocValueFormat.RAW);
        }

        static Explicit dateRange(DateRangeAggregationBuilder r, Map<String, Column> cols, List<Metric> metrics, BucketSpec child) {
            Column c = columnOf(r, cols);
            if (!c.isTimestamp()) {
                throw new Unsupported("aggregation [" + r.getName() + "] needs a date field, [" + c.name() + "] is " + c.type());
            }
            ZoneId tz = r.timeZone() == null ? ZoneOffset.UTC : r.timeZone();
            String fmt = r.format() == null ? DEFAULT_DATE_FORMAT : r.format();
            DocValueFormat format = new DocValueFormat.DateTime(DateFormatter.forPattern(fmt), tz, DateFieldMapper.Resolution.MILLISECONDS);
            long now = System.currentTimeMillis();
            List<ExplicitBucket> buckets = new ArrayList<>();
            for (RangeAggregator.Range range : r.ranges()) {
                // Range bounds are read as given: `to` is exclusive, so it is not rounded up.
                double from = bound(range.getFromAsString(), range.getFrom(), fmt, false, tz, now, r.getName());
                double to = bound(range.getToAsString(), range.getTo(), fmt, false, tz, now, r.getName());
                buckets.add(new ExplicitBucket(range.getKey(), between(c, from, to), from, to));
            }
            return new Explicit(r, metrics, child, ExplicitKind.DATE_RANGE, buckets, r.keyed(), format);
        }

        /** A date-range bound as epoch milliseconds: the string with date math when given, else the number, else open. */
        private static double bound(String asString, double asNumber, String format, boolean roundUp, ZoneId tz, long now, String agg) {
            if (asString != null) {
                Long ms = InfinoQueryTranslator.dateMathMillis(asString, format, roundUp, tz, now);
                if (ms == null) {
                    throw new Unsupported("aggregation [" + agg + "] has a bound [" + asString + "] that is not a date");
                }
                return ms;
            }
            return asNumber;
        }

        /** {@code from <= f < to}, each side only when bounded. */
        private static String between(Column c, double from, double to) {
            String f = quoteIdent(c.name());
            List<String> parts = new ArrayList<>();
            if (!Double.isInfinite(from) && !Double.isNaN(from)) {
                parts.add(f + " >= " + bound(c, from));
            }
            if (!Double.isInfinite(to) && !Double.isNaN(to)) {
                parts.add(f + " < " + bound(c, to));
            }
            return parts.isEmpty() ? InfinoQueryTranslator.ALL : "(" + String.join(" AND ", parts) + ")";
        }

        private static String bound(Column c, double v) {
            if (c.isTimestamp()) {
                return literal(Instant.ofEpochMilli((long) v).toString());
            }
            return v == Math.rint(v) && Math.abs(v) < 9.0e15 ? Long.toString((long) v) : Double.toString(v);
        }

        /** Build from one row per bucket (doc_count and metrics) and each bucket's nested aggregation. */
        InternalAggregation build(List<Map<String, Object>> rows, List<InternalAggregation> nested) {
            String name = builder.getName();
            Map<String, Object> meta = builder.getMetadata();
            switch (kind) {
                case FILTERS -> {
                    List<InternalFilters.InternalBucket> out = new ArrayList<>();
                    for (int i = 0; i < buckets.size(); i++) {
                        out.add(new InternalFilters.InternalBucket(buckets.get(i).key(), asLong(rows.get(i).get("doc_count")), subs(rows.get(i), nested.get(i)), keyed));
                    }
                    return new InternalFilters(name, out, keyed, meta);
                }
                case RANGE -> {
                    InternalRange.Factory<InternalRange.Bucket, ?> factory = new InternalRange.Factory<>();
                    List<InternalRange.Bucket> out = new ArrayList<>();
                    for (int i = 0; i < buckets.size(); i++) {
                        ExplicitBucket b = buckets.get(i);
                        out.add(factory.createBucket(b.key(), b.from(), b.to(), asLong(rows.get(i).get("doc_count")), subs(rows.get(i), nested.get(i)), keyed, format));
                    }
                    return factory.create(name, out, format, keyed, meta);
                }
                default -> {
                    List<InternalDateRange.Bucket> out = new ArrayList<>();
                    for (int i = 0; i < buckets.size(); i++) {
                        ExplicitBucket b = buckets.get(i);
                        out.add(InternalDateRange.FACTORY.createBucket(b.key(), b.from(), b.to(), asLong(rows.get(i).get("doc_count")), subs(rows.get(i), nested.get(i)), keyed, format));
                    }
                    return InternalDateRange.FACTORY.create(name, out, format, keyed, meta);
                }
            }
        }

        @Override
        InternalAggregation empty() {
            List<Map<String, Object>> rows = new ArrayList<>();
            List<InternalAggregation> nested = new ArrayList<>();
            for (int i = 0; i < buckets.size(); i++) {
                rows.add(Map.of());
                nested.add(child == null ? null : child.empty());
            }
            return build(rows, nested);
        }
    }

    /**
     * The ORDER BY a terms order maps to ({@code {key}} stands for the key
     * alias), or null if unsupported. A metric order arrives as a compound
     * with a {@code _key asc} tiebreaker appended by the builder; each element
     * is translated in turn, and the key closes the clause so ties are stable.
     */
    private static String orderClause(BucketOrder order, List<Metric> metrics) {
        if (order == null) {
            return "doc_count DESC, {key} ASC";
        }
        List<BucketOrder> elements = order instanceof InternalOrder.CompoundOrder compound ? compound.orderElements() : List.of(order);
        List<String> terms = new ArrayList<>();
        boolean keyed = false;
        for (BucketOrder element : elements) {
            String term = orderTerm(element, metrics);
            if (term == null) {
                return null;
            }
            keyed |= term.startsWith("{key}");
            terms.add(term);
        }
        if (!keyed) {
            terms.add("{key} ASC");
        }
        return String.join(", ", terms);
    }

    /** One element of a terms order as a sort term, or null if the table cannot sort by it. */
    private static String orderTerm(BucketOrder order, List<Metric> metrics) {
        if (InternalOrder.isCountDesc(order)) {
            return "doc_count DESC";
        }
        if (order.equals(BucketOrder.count(true))) {
            return "doc_count ASC";
        }
        if (InternalOrder.isKeyAsc(order)) {
            return "{key} ASC";
        }
        if (InternalOrder.isKeyDesc(order)) {
            return "{key} DESC";
        }
        for (Metric m : metrics) {
            if (m.orderAlias() == null) {
                continue;
            }
            String name = m.builder.getName();
            if (order.equals(BucketOrder.aggregation(name, true))) {
                return m.orderAlias() + " ASC";
            }
            if (order.equals(BucketOrder.aggregation(name, false))) {
                return m.orderAlias() + " DESC";
            }
        }
        return null;
    }

    // ---- evaluation ----

    private static String metricColumns(List<Metric> metrics) {
        List<String> cols = new ArrayList<>();
        for (Metric m : metrics) {
            cols.add(m.columns());
        }
        return cols.isEmpty() ? "COUNT(*) AS n" : String.join(", ", cols);
    }

    private static Map<String, Object> oneRow(String sql) {
        List<Map<String, Object>> rows = InfinoNative.sqlRows(sql);
        return rows.isEmpty() ? Map.of() : rows.get(0);
    }

    /** Evaluate a bucket aggregation over {@code from WHERE where}; {@code total} is that relation's row count. */
    private InternalAggregation evalBucket(BucketSpec spec, String from, String where, long total) {
        if (spec instanceof Explicit e) {
            return evalExplicit(e, from, where);
        }
        return evalChain((Grouped) spec, from, where, total);
    }

    private InternalAggregation evalExplicit(Explicit e, String from, String where) {
        List<Map<String, Object>> rows = new ArrayList<>();
        List<InternalAggregation> nested = new ArrayList<>();
        for (ExplicitBucket b : e.buckets) {
            String w = "(" + where + ") AND (" + b.predicate() + ")";
            String select = "COUNT(*) AS doc_count" + (e.metrics.isEmpty() ? "" : ", " + metricColumns(e.metrics));
            Map<String, Object> row = oneRow("SELECT " + select + " FROM " + from + " WHERE " + w);
            rows.add(row);
            nested.add(e.child == null ? null : evalBucket(e.child, from, w, asLong(row.get("doc_count"))));
        }
        return e.build(rows, nested);
    }

    /**
     * Evaluate a chain of grouped levels: one GROUP BY per depth over all keys
     * above it, each level restricted to the buckets its parent kept, then
     * fold the rows into nested buckets. An explicit level under the chain's
     * leaf is evaluated per leaf bucket.
     */
    private InternalAggregation evalChain(Grouped root, String from, String where, long total) {
        List<Grouped> chain = new ArrayList<>();
        BucketSpec cursor = root;
        while (cursor instanceof Grouped g) {
            chain.add(g);
            cursor = g.child;
        }
        Explicit leafChild = (Explicit) cursor;

        List<List<Map<String, Object>>> rowsByLevel = new ArrayList<>();
        List<List<Object>> kept = null;
        for (int i = 0; i < chain.size(); i++) {
            Grouped level = chain.get(i);
            List<String> keyCols = new ArrayList<>();
            List<String> aliases = new ArrayList<>();
            for (int j = 0; j <= i; j++) {
                keyCols.add(chain.get(j).keyExpr + " AS k" + j);
                aliases.add("k" + j);
            }
            String select = String.join(", ", keyCols) + ", COUNT(*) AS doc_count" + (level.metrics.isEmpty() ? "" : ", " + metricColumns(level.metrics));
            String restriction = kept == null ? "" : " AND " + tupleRestriction(chain, kept, i);
            StringBuilder positions = new StringBuilder();
            for (int j = 1; j <= i + 1; j++) {
                positions.append(j == 1 ? "" : ", ").append(j);
            }
            String inner = "SELECT " + select + " FROM " + from + " WHERE " + where + restriction + " GROUP BY " + positions;
            if (level.minDocCount > 1) {
                inner += " HAVING COUNT(*) >= " + level.minDocCount;
            }
            String parents = String.join(", ", aliases.subList(0, i));
            String sql;
            if (level.terms) {
                String partition = i == 0 ? "" : "PARTITION BY " + parents + " ";
                sql = "SELECT * FROM (SELECT t.*, ROW_NUMBER() OVER (" + partition + "ORDER BY " + level.order("k" + i) + ") AS rn FROM (" + inner
                    + ") t) WHERE rn <= " + level.size + " ORDER BY " + (i == 0 ? "rn" : parents + ", rn");
            } else {
                sql = "SELECT * FROM (" + inner + ") t ORDER BY " + String.join(", ", aliases);
            }
            List<Map<String, Object>> rows = InfinoNative.sqlRows(sql);
            rowsByLevel.add(rows);
            kept = new ArrayList<>();
            for (Map<String, Object> row : rows) {
                List<Object> tuple = new ArrayList<>();
                for (int j = 0; j <= i; j++) {
                    tuple.add(row.get("k" + j));
                }
                kept.add(tuple);
            }
            if (rows.isEmpty()) {
                break;
            }
            if (i + 1 < chain.size() && kept.size() > MAX_PARENT_TUPLES) {
                throw new Unsupported("aggregation [" + chain.get(i + 1).name() + "] would nest under " + kept.size() + " buckets; at most " + MAX_PARENT_TUPLES + " are served");
            }
        }
        while (rowsByLevel.size() < chain.size()) {
            rowsByLevel.add(List.of());
        }
        return fold(chain, 0, List.of(), rowsByLevel, total, leafChild, from, where);
    }

    /** {@code ((k0 = a AND k1 = b) OR ...)} over the key expressions, for the tuples the parent levels kept. */
    private static String tupleRestriction(List<Grouped> chain, List<List<Object>> kept, int level) {
        List<String> ors = new ArrayList<>();
        for (List<Object> tuple : kept) {
            List<String> ands = new ArrayList<>();
            for (int j = 0; j < level; j++) {
                Object key = tuple.get(j);
                ands.add(key == null ? chain.get(j).keyExpr + " IS NULL" : chain.get(j).keyExpr + " = " + chain.get(j).keyLiteral(key));
            }
            ors.add("(" + String.join(" AND ", ands) + ")");
        }
        return "(" + String.join(" OR ", ors) + ")";
    }

    /** Build level {@code depth} for the parent bucket whose keys are {@code prefix}. */
    private InternalAggregation fold(
        List<Grouped> chain,
        int depth,
        List<Object> prefix,
        List<List<Map<String, Object>>> rowsByLevel,
        long parentTotal,
        Explicit leafChild,
        String from,
        String where
    ) {
        Grouped level = chain.get(depth);
        List<Map<String, Object>> rows = new ArrayList<>();
        for (Map<String, Object> row : rowsByLevel.get(depth)) {
            if (hasPrefix(row, prefix)) {
                rows.add(row);
            }
        }
        String keyAlias = "k" + depth;
        return level.build(rows, keyAlias, row -> {
            List<Object> keys = new ArrayList<>(prefix);
            keys.add(row.get(keyAlias));
            long docCount = asLong(row.get("doc_count"));
            InternalAggregation nested = null;
            if (depth + 1 < chain.size()) {
                nested = fold(chain, depth + 1, keys, rowsByLevel, docCount, leafChild, from, where);
            } else if (leafChild != null) {
                nested = evalExplicit(leafChild, from, "(" + where + ") AND " + tupleRestriction(chain, List.of(keys), keys.size()));
            }
            return level.subs(row, nested);
        }, parentTotal);
    }

    private static boolean hasPrefix(Map<String, Object> row, List<Object> prefix) {
        for (int j = 0; j < prefix.size(); j++) {
            if (!sameKey(row.get("k" + j), prefix.get(j))) {
                return false;
            }
        }
        return true;
    }

    private static boolean sameKey(Object a, Object b) {
        if (a == null || b == null) {
            return a == b;
        }
        if (a instanceof Number x && b instanceof Number y) {
            return x.doubleValue() == y.doubleValue();
        }
        return a.toString().equals(b.toString());
    }

    // ---- values ----

    static long asLong(Object v) {
        if (v instanceof Number n) {
            return n.longValue();
        }
        if (v instanceof String s) {
            try {
                return Long.parseLong(s);
            } catch (NumberFormatException e) {
                return 0;
            }
        }
        return 0;
    }

    static double asDouble(Object v, double dflt) {
        if (v instanceof Number n) {
            return n.doubleValue();
        }
        if (v instanceof String s) {
            try {
                return Double.parseDouble(s);
            } catch (NumberFormatException e) {
                return dflt;
            }
        }
        return dflt;
    }
}
