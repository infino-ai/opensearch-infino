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
import org.opensearch.infino.InfinoNative.Column;
import org.opensearch.infino.InfinoQueryPhaseSearcher.Plan;
import org.opensearch.infino.InfinoQueryTranslator.Translation;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.AggregationProcessor;
import org.opensearch.search.aggregations.BucketOrder;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.search.aggregations.InternalOrder;
import org.opensearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.opensearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.opensearch.search.aggregations.bucket.histogram.DateIntervalWrapper;
import org.opensearch.search.aggregations.bucket.histogram.InternalDateHistogram;
import org.opensearch.search.aggregations.bucket.histogram.LongBounds;
import org.opensearch.search.aggregations.bucket.terms.LongTerms;
import org.opensearch.search.aggregations.bucket.terms.StringTerms;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregator;
import org.opensearch.search.aggregations.metrics.AvgAggregationBuilder;
import org.opensearch.search.aggregations.metrics.InternalAvg;
import org.opensearch.search.aggregations.metrics.InternalMax;
import org.opensearch.search.aggregations.metrics.InternalMin;
import org.opensearch.search.aggregations.metrics.InternalSum;
import org.opensearch.search.aggregations.metrics.InternalValueCount;
import org.opensearch.search.aggregations.metrics.MaxAggregationBuilder;
import org.opensearch.search.aggregations.metrics.MinAggregationBuilder;
import org.opensearch.search.aggregations.metrics.SumAggregationBuilder;
import org.opensearch.search.aggregations.metrics.ValueCountAggregationBuilder;
import org.opensearch.search.aggregations.support.ValuesSourceAggregationBuilder;
import org.opensearch.search.internal.SearchContext;

import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.opensearch.infino.InfinoQueryTranslator.literal;
import static org.opensearch.infino.InfinoQueryTranslator.quoteIdent;

/**
 * Serves aggregations from Infino SQL for Infino-enabled shards, over the
 * table's typed columns, scoped by the same relation and predicate the hits
 * use (so a full-text query scopes its aggregations through the engine's
 * token index, never through a materialized id list).
 *
 * <p>Served shapes:
 * <ul>
 *   <li>metrics: {@code value_count}, {@code sum}, {@code avg}, {@code min},
 *       {@code max} over a column, at the root or under a bucket;</li>
 *   <li>{@code terms} over a string, boolean, integer or date column, with
 *       {@code size}, {@code min_doc_count} and an order by count, key or a
 *       sibling metric;</li>
 *   <li>{@code date_histogram} over a timestamp column, fixed or calendar
 *       interval, time zone, offset (fixed intervals), {@code min_doc_count}
 *       and {@code extended_bounds};</li>
 *   <li>one bucket aggregation nested under another, each with its metrics.</li>
 * </ul>
 * Each bucket level is one {@code GROUP BY}; a nested level groups by both
 * keys at once and is folded into its parent's buckets. Anything else is
 * delegated to the stock processor together with the hits.
 */
public class InfinoAggregationProcessor implements AggregationProcessor {

    private static final Logger log = LogManager.getLogger(InfinoAggregationProcessor.class);
    /** Buckets a terms level may return; past this the window is not worth a GROUP BY. */
    private static final int MAX_TERMS_SIZE = 10_000;
    /** How bucket keys are printed when the request names no format. */
    private static final String DEFAULT_DATE_FORMAT = "strict_date_optional_time||epoch_millis";

    private final AggregationProcessor delegate;

    public InfinoAggregationProcessor(AggregationProcessor delegate) {
        this.delegate = delegate;
    }

    @Override
    public void preProcess(SearchContext context) {
        if (!served(context)) {
            delegate.preProcess(context); // sets up Lucene collectors
        }
    }

    @Override
    public void postProcess(SearchContext context) {
        if (!served(context)) {
            delegate.postProcess(context);
            return;
        }
        try {
            Plan plan = InfinoQueryPhaseSearcher.plan(context);
            List<InternalAggregation> aggs = new ArrayList<>();
            Long total = null;
            for (AggregationBuilder b : builders(context)) {
                Spec spec = Spec.of(b, plan.cols());
                if (spec instanceof Metric) {
                    continue; // root metrics are read in one query below
                }
                if (total == null) {
                    total = InfinoQueryPhaseSearcher.count(plan.query());
                }
                aggs.add(bucket((Bucket) spec, plan.query(), total));
            }
            List<Metric> rootMetrics = new ArrayList<>();
            for (AggregationBuilder b : builders(context)) {
                Spec spec = Spec.of(b, plan.cols());
                if (spec instanceof Metric m) {
                    rootMetrics.add(m);
                }
            }
            if (!rootMetrics.isEmpty()) {
                String sql = "SELECT " + metricColumns(rootMetrics) + " FROM " + plan.query().from() + " WHERE " + plan.query().where();
                List<Map<String, Object>> rows = InfinoNative.sqlRows(sql);
                Map<String, Object> row = rows.isEmpty() ? Map.of() : rows.get(0);
                for (Metric m : rootMetrics) {
                    aggs.add(m.build(row));
                }
            }
            context.queryResult().aggregations(InternalAggregations.from(aggs));
            log.debug("[infino] served {} aggregation(s) from {}", aggs.size(), plan.table());
        } catch (Exception e) {
            log.warn("[infino] aggregation routing failed, delegating", e);
            delegate.postProcess(context);
        }
    }

    private boolean served(SearchContext context) {
        return !builders(context).isEmpty() && InfinoQueryPhaseSearcher.plan(context) != null;
    }

    // ---- servability (shared with the query phase's plan) ----

    /** True when every requested aggregation has a plan over the table's columns. */
    static boolean servable(SearchContext context, Plan plan) {
        for (AggregationBuilder b : builders(context)) {
            if (Spec.of(b, plan.cols()) == null) {
                log.warn("[infino] aggregation not servable, falling back to Lucene: {} [{}]", b.getName(), b.getType());
                return false;
            }
        }
        return true;
    }

    static Collection<AggregationBuilder> builders(SearchContext context) {
        try {
            if (context.request() == null || context.request().source() == null || context.request().source().aggregations() == null) {
                return List.of();
            }
            return context.request().source().aggregations().getAggregatorFactories();
        } catch (Exception e) {
            return List.of();
        }
    }

    // ---- specs ----

    /** One aggregation the table can serve. */
    sealed interface Spec permits Metric, Bucket {
        AggregationBuilder builder();

        /** The spec for a builder, or null when the table cannot serve it. */
        static Spec of(AggregationBuilder b, Map<String, Column> cols) {
            return of(b, cols, 0, 0);
        }

        private static Spec of(AggregationBuilder b, Map<String, Column> cols, int depth, int index) {
            if (b instanceof ValuesSourceAggregationBuilder<?> vs && isMetric(b)) {
                if (!b.getSubAggregations().isEmpty() || vs.field() == null) {
                    return null;
                }
                Column c = InfinoQueryPhaseSearcher.columnFor(cols, vs.field());
                if (c == null) {
                    return null;
                }
                return Metric.of(b, c, index);
            }
            if (depth >= 2) {
                return null;
            }
            List<Metric> metrics = new ArrayList<>();
            Bucket child = null;
            int i = 0;
            for (AggregationBuilder sub : b.getSubAggregations()) {
                Spec s = of(sub, cols, depth + 1, i++);
                if (s instanceof Metric m) {
                    metrics.add(m);
                } else if (s instanceof Bucket bk && child == null) {
                    child = bk;
                } else {
                    return null;
                }
            }
            if (b instanceof TermsAggregationBuilder t) {
                return Bucket.terms(t, cols, metrics, child);
            }
            if (b instanceof DateHistogramAggregationBuilder dh) {
                return Bucket.dateHistogram(dh, cols, metrics, child);
            }
            return null;
        }

        private static boolean isMetric(AggregationBuilder b) {
            return b instanceof ValueCountAggregationBuilder
                || b instanceof SumAggregationBuilder
                || b instanceof AvgAggregationBuilder
                || b instanceof MinAggregationBuilder
                || b instanceof MaxAggregationBuilder;
        }
    }

    /** A metric over one column: the SQL aggregate columns it needs and the result it builds. */
    record Metric(AggregationBuilder builder, Column column, String alias) implements Spec {
        static Metric of(AggregationBuilder b, Column c, int index) {
            boolean numeric = c.isNumeric() || c.isTimestamp();
            if (b instanceof ValueCountAggregationBuilder) {
                return new Metric(b, c, "m" + index);
            }
            if (!numeric) {
                return null;
            }
            if ((b instanceof SumAggregationBuilder || b instanceof AvgAggregationBuilder) && c.isTimestamp()) {
                return null;
            }
            return new Metric(b, c, "m" + index);
        }

        /** The SELECT list entries for this metric. */
        String columns() {
            String f = quoteIdent(column.name());
            if (builder instanceof ValueCountAggregationBuilder) {
                return "COUNT(" + f + ") AS " + alias + "_c";
            }
            if (builder instanceof SumAggregationBuilder) {
                return "SUM(" + f + ") AS " + alias + "_s";
            }
            if (builder instanceof AvgAggregationBuilder) {
                return "SUM(" + f + ") AS " + alias + "_s, COUNT(" + f + ") AS " + alias + "_c";
            }
            if (builder instanceof MinAggregationBuilder) {
                return "MIN(" + f + ") AS " + alias + "_v";
            }
            return "MAX(" + f + ") AS " + alias + "_v";
        }

        /** The alias a terms order by this metric sorts on. */
        String orderAlias() {
            if (builder instanceof ValueCountAggregationBuilder) {
                return alias + "_c";
            }
            if (builder instanceof SumAggregationBuilder) {
                return alias + "_s";
            }
            if (builder instanceof AvgAggregationBuilder) {
                return null; // an average is not a column the GROUP BY can sort on
            }
            return alias + "_v";
        }

        InternalAggregation build(Map<String, Object> row) {
            String name = builder.getName();
            Map<String, Object> meta = builder.getMetadata();
            if (builder instanceof ValueCountAggregationBuilder) {
                return new InternalValueCount(name, asLong(row.get(alias + "_c")), meta);
            }
            if (builder instanceof SumAggregationBuilder) {
                return new InternalSum(name, asDouble(row.get(alias + "_s"), 0.0), DocValueFormat.RAW, meta);
            }
            if (builder instanceof AvgAggregationBuilder) {
                return new InternalAvg(name, asDouble(row.get(alias + "_s"), 0.0), asLong(row.get(alias + "_c")), DocValueFormat.RAW, meta);
            }
            if (builder instanceof MinAggregationBuilder) {
                return new InternalMin(name, asDouble(row.get(alias + "_v"), Double.POSITIVE_INFINITY), DocValueFormat.RAW, meta);
            }
            return new InternalMax(name, asDouble(row.get(alias + "_v"), Double.NEGATIVE_INFINITY), DocValueFormat.RAW, meta);
        }
    }

    /** How a date histogram keys its buckets. */
    record DateKey(DocValueFormat format, Rounding rounding, long minDocCount, LongBounds bounds, boolean keyed, long offset) {}

    /** A bucket aggregation: its key expression, the metrics under it, and at most one nested bucket. */
    record Bucket(AggregationBuilder builder, Column column, String keyExpr, List<Metric> metrics, Bucket child, DateKey date) implements Spec {

        static Bucket terms(TermsAggregationBuilder t, Map<String, Column> cols, List<Metric> metrics, Bucket child) {
            if (t.field() == null || t.size() > MAX_TERMS_SIZE) {
                return null;
            }
            Column c = InfinoQueryPhaseSearcher.columnFor(cols, t.field());
            if (c == null || !(c.isString() || c.isBoolean() || "i64".equals(c.type()) || c.isTimestamp())) {
                return null;
            }
            if (orderClause(t.order(), metrics) == null) {
                return null;
            }
            return new Bucket(t, c, quoteIdent(c.name()), metrics, child, null);
        }

        static Bucket dateHistogram(DateHistogramAggregationBuilder dh, Map<String, Column> cols, List<Metric> metrics, Bucket child) {
            if (dh.field() == null) {
                return null;
            }
            Column c = InfinoQueryPhaseSearcher.columnFor(cols, dh.field());
            if (c == null || !c.isTimestamp()) {
                return null;
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
                    return null;
                }
                String origin = dh.offset() == 0 ? "" : ", TIMESTAMP '1970-01-01T00:00:00Z' + INTERVAL '" + dh.offset() + " milliseconds'";
                keyExpr = "date_bin(INTERVAL '" + ms + " milliseconds', " + source + origin + ")";
                wrapper.fixedInterval(dh.getFixedInterval());
            } else if (dh.getCalendarInterval() != null) {
                if (dh.offset() != 0) {
                    return null;
                }
                String unit = calendarUnit(dh.getCalendarInterval());
                if (unit == null) {
                    return null;
                }
                keyExpr = "date_trunc('" + unit + "', " + source + ")";
                wrapper.calendarInterval(dh.getCalendarInterval());
            } else {
                return null;
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
                    return null;
                }
                Rounding.Prepared prepared = rounding.prepareForUnknown();
                bounds = new LongBounds(min == null ? null : prepared.round(min), max == null ? null : prepared.round(max));
            }
            DateKey date = new DateKey(format, rounding, dh.minDocCount(), bounds, dh.keyed(), dh.offset());
            return new Bucket(dh, c, keyExpr, metrics, child, date);
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

        boolean isTerms() {
            return date == null;
        }

        int size() {
            return builder instanceof TermsAggregationBuilder t ? t.size() : Integer.MAX_VALUE;
        }

        long minDocCount() {
            if (builder instanceof TermsAggregationBuilder t) {
                return t.minDocCount();
            }
            return date.minDocCount();
        }

        /** ORDER BY for this level's buckets. */
        String order(String keyAlias) {
            if (builder instanceof TermsAggregationBuilder t) {
                return orderClause(t.order(), metrics).replace("{key}", keyAlias);
            }
            return keyAlias + " ASC";
        }
    }

    /** The ORDER BY a terms order maps to ({@code {key}} stands for the key alias), or null if unsupported. */
    private static String orderClause(BucketOrder order, List<Metric> metrics) {
        if (order == null || InternalOrder.isCountDesc(order)) {
            return "doc_count DESC, {key} ASC";
        }
        if (InternalOrder.isKeyAsc(order)) {
            return "{key} ASC";
        }
        if (InternalOrder.isKeyDesc(order)) {
            return "{key} DESC";
        }
        if (order.equals(BucketOrder.count(true))) {
            return "doc_count ASC, {key} ASC";
        }
        for (Metric m : metrics) {
            if (m.orderAlias() == null) {
                continue;
            }
            String name = m.builder().getName();
            if (order.equals(BucketOrder.aggregation(name, true))) {
                return m.orderAlias() + " ASC, {key} ASC";
            }
            if (order.equals(BucketOrder.aggregation(name, false))) {
                return m.orderAlias() + " DESC, {key} ASC";
            }
        }
        return null;
    }

    // ---- building ----

    private static String metricColumns(List<Metric> metrics) {
        List<String> cols = new ArrayList<>();
        for (Metric m : metrics) {
            cols.add(m.columns());
        }
        return cols.isEmpty() ? "COUNT(*) AS n" : String.join(", ", cols);
    }

    /**
     * Run one bucket level and build it, with its nested level folded in.
     * {@code total} is the relation's row count, for the terms "other" count.
     */
    private InternalAggregation bucket(Bucket spec, Translation q, long total) {
        String keyAlias = "k0";
        String select = spec.keyExpr() + " AS " + keyAlias + ", COUNT(*) AS doc_count";
        if (!spec.metrics().isEmpty()) {
            select += ", " + metricColumns(spec.metrics());
        }
        String sql = "SELECT " + select + " FROM " + q.from() + " WHERE " + q.where() + " GROUP BY 1";
        if (spec.minDocCount() > 1) {
            sql += " HAVING COUNT(*) >= " + spec.minDocCount();
        }
        sql += " ORDER BY " + spec.order(keyAlias);
        if (spec.isTerms()) {
            sql += " LIMIT " + spec.size();
        }
        List<Map<String, Object>> rows = InfinoNative.sqlRows(sql);

        Map<Object, List<Map<String, Object>>> childRows = Map.of();
        if (spec.child() != null && !rows.isEmpty()) {
            List<Object> keys = new ArrayList<>();
            for (Map<String, Object> r : rows) {
                keys.add(r.get(keyAlias));
            }
            childRows = childLevel(spec, q, keys);
        }

        if (spec.isTerms()) {
            return terms(spec, rows, childRows, total);
        }
        return dateHistogram(spec, rows, childRows);
    }

    /** The nested level, grouped by both keys, restricted to the parent's kept keys, and split by parent key. */
    private Map<Object, List<Map<String, Object>>> childLevel(Bucket parent, Translation q, List<Object> parentKeys) {
        Bucket child = parent.child();
        String select = parent.keyExpr() + " AS k0, " + child.keyExpr() + " AS k1, COUNT(*) AS doc_count";
        if (!child.metrics().isEmpty()) {
            select += ", " + metricColumns(child.metrics());
        }
        List<String> keyLits = new ArrayList<>();
        for (Object k : parentKeys) {
            keyLits.add(keyLiteral(parent, k));
        }
        String inner = "SELECT " + select + " FROM " + q.from() + " WHERE " + q.where() + " GROUP BY 1, 2";
        if (child.minDocCount() > 1) {
            inner += " HAVING COUNT(*) >= " + child.minDocCount();
        }
        String sql;
        if (child.isTerms()) {
            // The child's size applies per parent bucket.
            sql = "SELECT * FROM (SELECT t.*, ROW_NUMBER() OVER (PARTITION BY k0 ORDER BY " + child.order("k1") + ") AS rn FROM ("
                + inner + ") t) WHERE rn <= " + child.size() + " AND k0 IN (" + String.join(", ", keyLits) + ") ORDER BY k0, rn";
        } else {
            sql = "SELECT * FROM (" + inner + ") t WHERE k0 IN (" + String.join(", ", keyLits) + ") ORDER BY k0, k1";
        }
        Map<Object, List<Map<String, Object>>> out = new LinkedHashMap<>();
        for (Map<String, Object> row : InfinoNative.sqlRows(sql)) {
            out.computeIfAbsent(normalizeKey(row.get("k0")), k -> new ArrayList<>()).add(row);
        }
        return out;
    }

    /** A key as an SQL literal of the parent's key type. */
    private static String keyLiteral(Bucket spec, Object key) {
        if (key == null) {
            return "NULL";
        }
        if (!spec.isTerms() || spec.column().isTimestamp()) {
            // The engine reads an ISO string against a timestamp column.
            return literal(java.time.Instant.ofEpochMilli(asLong(key)).toString());
        }
        if (spec.column().isString()) {
            return literal(key.toString());
        }
        return key.toString();
    }

    /** Keys as the fold compares them: numbers as Long, strings as String. */
    private static Object normalizeKey(Object key) {
        if (key instanceof Number n) {
            return n.longValue();
        }
        return key;
    }

    private InternalAggregations subAggregations(Bucket spec, Map<String, Object> row, List<Map<String, Object>> childRows, long docCount) {
        List<InternalAggregation> subs = new ArrayList<>();
        for (Metric m : spec.metrics()) {
            subs.add(m.build(row));
        }
        if (spec.child() != null) {
            List<Map<String, Object>> rows = childRows == null ? List.of() : childRows;
            // The nested level's rows are keyed k1; present them as a level of their own.
            List<Map<String, Object>> rekeyed = new ArrayList<>(rows.size());
            for (Map<String, Object> r : rows) {
                Map<String, Object> copy = new LinkedHashMap<>(r);
                copy.put("k0", r.get("k1"));
                rekeyed.add(copy);
            }
            subs.add(spec.child().isTerms() ? terms(spec.child(), rekeyed, Map.of(), docCount) : dateHistogram(spec.child(), rekeyed, Map.of()));
        }
        return subs.isEmpty() ? InternalAggregations.EMPTY : InternalAggregations.from(subs);
    }

    private InternalAggregation terms(Bucket spec, List<Map<String, Object>> rows, Map<Object, List<Map<String, Object>>> childRows, long total) {
        TermsAggregationBuilder t = (TermsAggregationBuilder) spec.builder();
        long sumTop = 0;
        boolean longKeys = !spec.column().isString() && !spec.column().isBoolean();
        List<StringTerms.Bucket> sBuckets = new ArrayList<>();
        List<LongTerms.Bucket> lBuckets = new ArrayList<>();
        for (Map<String, Object> row : rows) {
            Object key = row.get("k0");
            if (key == null) {
                continue; // a terms aggregation has no bucket for the missing value
            }
            long docCount = asLong(row.get("doc_count"));
            sumTop += docCount;
            InternalAggregations subs = subAggregations(spec, row, childRows.get(normalizeKey(key)), docCount);
            if (longKeys) {
                lBuckets.add(new LongTerms.Bucket(asLong(key), docCount, subs, false, 0L, DocValueFormat.RAW));
            } else {
                sBuckets.add(new StringTerms.Bucket(new BytesRef(key.toString()), docCount, subs, false, 0L, DocValueFormat.RAW));
            }
        }
        long otherDocCount = Math.max(0, total - sumTop);
        TermsAggregator.BucketCountThresholds thresholds = new TermsAggregator.BucketCountThresholds(t.minDocCount(), t.shardMinDocCount(), t.size(), t.shardSize());
        if (longKeys) {
            return new LongTerms(t.getName(), t.order(), t.order(), t.getMetadata(), DocValueFormat.RAW, t.shardSize(), false, otherDocCount, lBuckets, 0L, thresholds);
        }
        return new StringTerms(t.getName(), t.order(), t.order(), t.getMetadata(), DocValueFormat.RAW, t.shardSize(), false, otherDocCount, sBuckets, 0L, thresholds);
    }

    private InternalAggregation dateHistogram(Bucket spec, List<Map<String, Object>> rows, Map<Object, List<Map<String, Object>>> childRows) {
        DateKey date = spec.date();
        List<InternalDateHistogram.Bucket> buckets = new ArrayList<>();
        for (Map<String, Object> row : rows) {
            Object key = row.get("k0");
            if (key == null) {
                continue;
            }
            long docCount = asLong(row.get("doc_count"));
            InternalAggregations subs = subAggregations(spec, row, childRows.get(normalizeKey(key)), docCount);
            buckets.add(new InternalDateHistogram.Bucket(asLong(key), docCount, date.keyed(), date.format(), subs));
        }
        InternalAggregations empty = date.minDocCount() == 0 ? subAggregations(spec, Map.of(), List.of(), 0) : null;
        return InfinoAggregationWire.dateHistogram(
            spec.builder().getName(),
            spec.builder().getMetadata(),
            buckets,
            date.minDocCount(),
            date.offset(),
            date.rounding(),
            empty,
            date.bounds(),
            date.format(),
            date.keyed()
        );
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
