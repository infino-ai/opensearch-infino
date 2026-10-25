/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.util.BytesRef;
import org.opensearch.common.Rounding;
import org.opensearch.common.lucene.search.TopDocsAndMaxScore;
import org.opensearch.common.time.DateFormatter;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.index.Index;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.mapper.DateFieldMapper;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.infino.InfinoNative.Column;
import org.opensearch.infino.InfinoQueryTranslator.Translation;
import org.opensearch.infino.InfinoSearch.Plan;
import org.opensearch.infino.InfinoScriptTranslator.Sql;
import org.opensearch.infino.InfinoScriptTranslator.SqlType;
import org.opensearch.infino.InfinoSearch.SortSpec;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.search.SearchShardTarget;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.BucketOrder;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.search.aggregations.InternalOrder;
import org.opensearch.search.aggregations.bucket.BucketUtils;
import org.opensearch.search.aggregations.bucket.composite.CompositeAggregationBuilder;
import org.opensearch.search.aggregations.bucket.composite.CompositeValuesSourceBuilder;
import org.opensearch.search.aggregations.bucket.composite.DateHistogramValuesSourceBuilder;
import org.opensearch.search.aggregations.bucket.composite.HistogramValuesSourceBuilder;
import org.opensearch.search.aggregations.bucket.composite.TermsValuesSourceBuilder;
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
import org.opensearch.search.aggregations.bucket.missing.MissingOrder;
import org.opensearch.search.aggregations.bucket.range.DateRangeAggregationBuilder;
import org.opensearch.search.aggregations.bucket.range.InternalDateRange;
import org.opensearch.search.aggregations.bucket.range.InternalRange;
import org.opensearch.search.aggregations.bucket.range.RangeAggregationBuilder;
import org.opensearch.search.aggregations.bucket.range.RangeAggregator;
import org.opensearch.search.aggregations.bucket.terms.LongTerms;
import org.opensearch.search.aggregations.bucket.terms.SignificantStringTerms;
import org.opensearch.search.aggregations.bucket.terms.SignificantTermsAggregationBuilder;
import org.opensearch.search.aggregations.bucket.terms.StringTerms;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregator;
import org.opensearch.search.aggregations.bucket.terms.heuristic.JLHScore;
import org.opensearch.search.aggregations.bucket.terms.heuristic.SignificanceHeuristic;
import org.opensearch.search.aggregations.metrics.AvgAggregationBuilder;
import org.opensearch.search.aggregations.metrics.CardinalityAggregationBuilder;
import org.opensearch.search.aggregations.metrics.InternalAvg;
import org.opensearch.search.aggregations.metrics.InternalMax;
import org.opensearch.search.aggregations.metrics.InternalMin;
import org.opensearch.search.aggregations.metrics.InternalSum;
import org.opensearch.search.aggregations.metrics.InternalTDigestPercentileRanks;
import org.opensearch.search.aggregations.metrics.InternalTDigestPercentiles;
import org.opensearch.search.aggregations.metrics.InternalTopHits;
import org.opensearch.search.aggregations.metrics.InternalValueCount;
import org.opensearch.search.aggregations.metrics.MaxAggregationBuilder;
import org.opensearch.search.aggregations.metrics.MinAggregationBuilder;
import org.opensearch.search.aggregations.metrics.PercentileRanksAggregationBuilder;
import org.opensearch.search.aggregations.metrics.PercentilesAggregationBuilder;
import org.opensearch.search.aggregations.metrics.PercentilesConfig;
import org.opensearch.search.aggregations.metrics.SumAggregationBuilder;
import org.opensearch.search.aggregations.metrics.TDigestState;
import org.opensearch.search.aggregations.metrics.TopHitsAggregationBuilder;
import org.opensearch.search.aggregations.metrics.ValueCountAggregationBuilder;
import org.opensearch.search.aggregations.support.ValuesSourceAggregationBuilder;
import org.opensearch.search.fetch.subphase.FetchSourceContext;
import org.opensearch.search.fetch.subphase.FieldAndFormat;
import org.opensearch.search.sort.SortOrder;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
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
 * <p>Four kinds of aggregation are planned:
 * <ul>
 *   <li><b>metrics</b> ({@code value_count}, {@code sum}, {@code avg},
 *       {@code min}, {@code max}, {@code cardinality}, {@code percentiles},
 *       {@code percentile_ranks}): aggregate columns of the query at their
 *       level;</li>
 *   <li><b>grouped buckets</b> ({@code terms}, {@code date_histogram},
 *       {@code histogram}): a chain of consecutive grouped levels is answered
 *       with one {@code GROUP BY} per depth, each level grouping by every key
 *       above it, with the per-parent size of a terms level applied by a
 *       window function, and the rows folded into nested buckets;
 *       {@code composite} is one such level over several keys, paged by a
 *       keyset predicate, and {@code significant_terms} one whose buckets
 *       carry a second, background count over the whole table;</li>
 *   <li><b>explicit buckets</b> ({@code filters}, {@code range},
 *       {@code date_range}): one query per bucket, the bucket's predicate
 *       added to the request's, since buckets may overlap;</li>
 *   <li><b>top hits</b> ({@code top_hits}): the first rows of a bucket in
 *       the requested order, one window statement per grouped level (the
 *       window partitioned by the level's keys) and one statement per
 *       explicit bucket.</li>
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
    /** OpenSearch's page size for top hits when the request names none. */
    private static final int DEFAULT_TOP_HITS_SIZE = 3;
    /** Hits one top_hits may ask for per bucket; past this the rows belong in a search. */
    private static final int MAX_TOP_HITS = 100;
    /** Separator of a key tuple's string form, chosen so no key contains it. */
    private static final char KEY_SEPARATOR = '\u0001';
    /** Stand-in for a missing key in a tuple's string form. */
    private static final String NULL_KEY = "\u0000";

    private final Index index;

    /** A processor whose top hits name {@code index}; null for planning only. */
    InfinoAggregationProcessor(Index index) {
        this.index = index;
    }

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
            if (spec instanceof TopHits th) {
                byName.put(spec.name(), th.build(topHitRows(th, q.from(), q.where()), total, index));
                continue;
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
            if (b instanceof TopHitsAggregationBuilder th) {
                return TopHits.of(th, cols);
            }
            List<Metric> metrics = new ArrayList<>();
            List<TopHits> hits = new ArrayList<>();
            BucketSpec child = null;
            int i = 0;
            for (AggregationBuilder sub : b.getSubAggregations()) {
                Spec s = of(sub, cols, table, i++);
                if (s instanceof Metric m) {
                    metrics.add(m);
                } else if (s instanceof TopHits t) {
                    hits.add(t);
                } else if (child == null) {
                    child = (BucketSpec) s;
                } else {
                    throw new Unsupported("aggregation [" + b.getName() + "] has more than one nested bucket aggregation");
                }
            }
            Leaves leaves = new Leaves(metrics, hits);
            if (b instanceof TermsAggregationBuilder t) {
                return Grouped.terms(t, cols, leaves, child);
            }
            if (b instanceof DateHistogramAggregationBuilder dh) {
                return Grouped.dateHistogram(dh, cols, leaves, child);
            }
            if (b instanceof HistogramAggregationBuilder h) {
                return Grouped.histogram(h, cols, leaves, child);
            }
            if (b instanceof FiltersAggregationBuilder f) {
                return Explicit.filters(f, cols, table, leaves, child);
            }
            if (b instanceof RangeAggregationBuilder r) {
                return Explicit.range(r, cols, leaves, child);
            }
            if (b instanceof DateRangeAggregationBuilder dr) {
                return Explicit.dateRange(dr, cols, leaves, child);
            }
            if (b instanceof SignificantTermsAggregationBuilder st) {
                return Significant.of(st, cols, table, leaves, child);
            }
            if (b instanceof CompositeAggregationBuilder c) {
                return Composite.of(c, cols, leaves, child);
            }
            throw new Unsupported("aggregation [" + b.getName() + "] of type [" + b.getType() + "] is not served");
        }

        /**
         * What the aggregation reads: its field's column, or its script as a
         * column expression (a value script, one given beside a field, reads
         * that field as {@code _value}).
         */
        static Source sourceOf(ValuesSourceAggregationBuilder<?> b, Map<String, Column> cols) {
            Column c = null;
            if (b.field() != null) {
                c = InfinoSearch.columnFor(cols, b.field());
                if (c == null) {
                    throw new Unsupported("aggregation [" + b.getName() + "] names field [" + b.field() + "], which the table has no column for");
                }
            }
            if (b.script() != null) {
                Sql sql = InfinoScriptTranslator.translate(b.script(), cols, c, System.currentTimeMillis());
                return new Source(sql.expr(), sql.type(), "script [" + b.script().getIdOrCode() + "]");
            }
            if (c == null) {
                throw new Unsupported("aggregation [" + b.getName() + "] needs a field or a script");
            }
            return Source.of(c);
        }
    }

    /** What an aggregation reads, as a column expression with its type; {@code label} names it in a refusal. */
    record Source(String expr, SqlType type, String label) {
        static Source of(Column c) {
            return new Source(quoteIdent(c.name()), InfinoScriptTranslator.typeOf(c), "[" + c.name() + "]");
        }

        boolean isNumeric() {
            return type.isNumeric();
        }

        boolean isTimestamp() {
            return type == SqlType.DATE;
        }

        boolean isString() {
            return type == SqlType.STRING;
        }

        boolean isBoolean() {
            return type == SqlType.BOOL;
        }

        /** The type's name for a refusal. */
        String typeName() {
            return type.name().toLowerCase(Locale.ROOT);
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
        PERCENTILES,
        PERCENTILE_RANKS
    }

    /** A metric over one column: the SQL aggregate columns it needs and the result it builds. */
    static final class Metric extends Spec {
        final Source source;
        final String alias;
        final MetricKind kind;
        final double[] percents;
        final boolean keyed;
        final double compression;

        private Metric(AggregationBuilder b, Source source, String alias, MetricKind kind, double[] percents, boolean keyed, double compression) {
            super(b);
            this.source = source;
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
                || b instanceof PercentilesAggregationBuilder
                || b instanceof PercentileRanksAggregationBuilder;
        }

        static Metric of(AggregationBuilder b, Map<String, Column> cols, int index) {
            Source c = sourceOf((ValuesSourceAggregationBuilder<?>) b, cols);
            String alias = "m" + index;
            boolean numeric = c.isNumeric() || c.isTimestamp();
            if (b instanceof ValueCountAggregationBuilder) {
                return new Metric(b, c, alias, MetricKind.VALUE_COUNT, null, false, 0);
            }
            if (b instanceof CardinalityAggregationBuilder) {
                return new Metric(b, c, alias, MetricKind.CARDINALITY, null, false, 0);
            }
            if (!numeric) {
                throw new Unsupported("aggregation [" + b.getName() + "] needs a numeric or date value, " + c.label() + " is " + c.typeName());
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
            if (b instanceof PercentileRanksAggregationBuilder pr) {
                PercentilesConfig config = pr.percentilesConfig();
                if (config != null && !(config instanceof PercentilesConfig.TDigest)) {
                    throw new Unsupported("aggregation [" + b.getName() + "] uses the HDR method; only t-digest is served");
                }
                double compression = config instanceof PercentilesConfig.TDigest td ? td.getCompression() : DEFAULT_TDIGEST_COMPRESSION;
                double[] values = pr.values();
                if (values == null || values.length == 0) {
                    throw new Unsupported("aggregation [" + b.getName() + "] names no values");
                }
                return new Metric(b, c, alias, MetricKind.PERCENTILE_RANKS, values, pr.keyed(), compression);
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
            String f = "(" + source.expr() + ")";
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
                case PERCENTILE_RANKS -> {
                    // One exact count of the rows below each value, beside the
                    // row count and bounds. The digest the value goes into reads
                    // its rank as that count plus half the value's own row, which
                    // is how stock's digest over the rows themselves reads it.
                    List<String> cols = new ArrayList<>();
                    for (int i = 0; i < percents.length; i++) {
                        cols.add("SUM(CASE WHEN " + f + " < " + percents[i] + " THEN 1 ELSE 0 END) AS " + alias + "_r" + i);
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
                case AVG, PERCENTILES, PERCENTILE_RANKS -> null;
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
                case PERCENTILE_RANKS -> percentileRanks(row);
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
            double[] values = new double[percents.length];
            for (int i = 0; i < percents.length; i++) {
                values[i] = asDouble(row.get(alias + "_p" + i), Double.NaN);
            }
            TDigestState state = digest(row, percents, values);
            return new InternalTDigestPercentiles(builder.getName(), percents, state, keyed, DocValueFormat.RAW, builder.getMetadata());
        }

        /**
         * The inverse: each requested value goes in as a single-row centroid
         * past the rows the engine counted below it, so the digest's
         * cumulative distribution reads the value's rank as those rows plus
         * half its own, which is what stock's digest over the rows reads.
         */
        private InternalAggregation percentileRanks(Map<String, Object> row) {
            double[] values = percents;
            long count = asLong(row.get(alias + "_c"));
            double[] ranks = new double[values.length];
            for (int i = 0; i < values.length; i++) {
                ranks[i] = count == 0 ? 0.0 : 100.0 * asLong(row.get(alias + "_r" + i)) / count;
            }
            TDigestState state = digest(row, ranks, values);
            return new InternalTDigestPercentileRanks(builder.getName(), values, state, keyed, DocValueFormat.RAW, builder.getMetadata());
        }

        /**
         * The digest over {@code count} rows whose quantile at each of
         * {@code percents} is the matching entry of {@code values}, built
         * from the row's count and bounds. A value outside the bounds is
         * clamped to them: the digest's ends are the minimum and maximum.
         */
        private TDigestState digest(Map<String, Object> row, double[] percents, double[] values) {
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
                    placeInterior(state, percents, values, count, innerMin, innerMax);
                }
            }
            return state;
        }

        /** The percent centroids in percent order, each at its position, fillers between; the end rows stay with the caller. */
        private static void placeInterior(TDigestState state, double[] percents, double[] values, long count, double innerMin, double innerMax) {
            int n = percents.length;
            Integer[] order = new Integer[n];
            for (int i = 0; i < n; i++) {
                order[i] = i;
            }
            Arrays.sort(order, Comparator.comparingDouble(i -> percents[i]));
            double[] value = new double[n];
            for (int i = 0; i < n; i++) {
                double v = Double.isNaN(values[i]) ? innerMin : values[i];
                value[i] = Math.min(innerMax, Math.max(innerMin, v));
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

    /**
     * The first rows of a bucket: {@code top_hits}. Ordered by the request's
     * field sorts when it has any, else by id so the page is stable; a
     * text-ranked order is not served here (the aggregation relation is the
     * match set, which carries no score), so hits score alike, as filtered
     * hits do.
     */
    static final class TopHits extends Spec {
        final int from;
        final int size;
        final List<SortSpec> sorts;
        final FetchSourceContext source;
        final List<FieldAndFormat> fields;
        final Map<String, Column> cols;

        private TopHits(TopHitsAggregationBuilder b, List<SortSpec> sorts, FetchSourceContext source, List<FieldAndFormat> fields, Map<String, Column> cols) {
            super(b);
            this.from = Math.max(0, b.from());
            this.size = b.size() < 0 ? DEFAULT_TOP_HITS_SIZE : b.size();
            this.sorts = sorts;
            this.source = source;
            this.fields = fields;
            this.cols = cols;
        }

        static TopHits of(TopHitsAggregationBuilder b, Map<String, Column> cols) {
            if (b.size() > MAX_TOP_HITS) {
                throw new Unsupported("aggregation [" + b.getName() + "] asks for " + b.size() + " hits per bucket; at most " + MAX_TOP_HITS + " are served");
            }
            // The builder's collections are null until the request names them.
            boolean scripts = b.scriptFields() != null && !b.scriptFields().isEmpty();
            if (b.highlighter() != null || b.explain() || b.version() || scripts) {
                throw new Unsupported("aggregation [" + b.getName() + "] asks for highlight, explain, version or script fields, which the table does not serve");
            }
            List<SortSpec> sorts;
            try {
                sorts = InfinoSearch.sorts(b.sorts(), cols);
            } catch (IllegalArgumentException e) {
                throw new Unsupported("aggregation [" + b.getName() + "] " + e.getMessage());
            }
            // docvalue_fields and fields are read from the hit's source; stored_fields adds nothing the source lacks.
            return new TopHits(b, sorts, b.fetchSource(), InfinoSearch.requestedFields(b.docValueFields(), b.fetchFields()), cols);
        }

        /** The columns a hit row carries beyond the id and the source. */
        List<String> rowColumns() {
            List<String> out = new ArrayList<>();
            for (SortSpec s : sorts) {
                out.add(s.expr() + " AS " + quoteIdent(s.name()));
            }
            return out;
        }

        /** ORDER BY within a bucket. */
        String order() {
            return sorts.isEmpty() ? InfinoQueryTranslator.OSID + " ASC" : String.join(", ", InfinoSearch.orderBy(sorts));
        }

        int fetch() {
            return from + size;
        }

        /** This aggregation over the bucket's rows (in order, at most {@link #fetch()} of them) and its row count; the hits name {@code index}. */
        InternalAggregation build(List<Map<String, Object>> rows, long bucketCount, Index index) {
            return InfinoSearch.topHits((TopHitsAggregationBuilder) builder, from, size, sorts, source, fields, cols, rows, bucketCount, index);
        }

        InternalAggregation empty() {
            return build(List.of(), 0, null);
        }
    }

    /** What sits under a bucket besides a nested bucket aggregation: its metrics and its top hits. */
    record Leaves(List<Metric> metrics, List<TopHits> hits) {
        static final Leaves NONE = new Leaves(List.of(), List.of());
    }

    /** A bucket aggregation: the metrics and top hits under it and at most one nested bucket aggregation. */
    abstract static class BucketSpec extends Spec {
        final List<Metric> metrics;
        final List<TopHits> hits;
        final BucketSpec child;

        BucketSpec(AggregationBuilder b, Leaves leaves, BucketSpec child) {
            super(b);
            this.metrics = leaves.metrics();
            this.hits = leaves.hits();
            this.child = child;
        }

        /**
         * The sub-aggregations of a bucket: its metrics from the row, its top
         * hits (one per top_hits spec, in order) and the nested aggregation given.
         */
        InternalAggregations subs(Map<String, Object> row, List<InternalAggregation> topHits, InternalAggregation nested) {
            List<InternalAggregation> out = new ArrayList<>();
            for (Metric m : metrics) {
                out.add(m.build(row));
            }
            out.addAll(topHits);
            if (nested != null) {
                out.add(nested);
            }
            return out.isEmpty() ? InternalAggregations.EMPTY : InternalAggregations.from(out);
        }

        /** The sub-aggregations of an empty bucket. */
        InternalAggregations emptySubs() {
            return subs(Map.of(), emptyHits(), child == null ? null : child.empty());
        }

        /** One empty top_hits per spec. */
        List<InternalAggregation> emptyHits() {
            List<InternalAggregation> out = new ArrayList<>();
            for (TopHits th : hits) {
                out.add(th.empty());
            }
            return out;
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
        final Source source;
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
            Leaves leaves,
            BucketSpec child,
            Source source,
            String keyExpr,
            KeyKind keyKind,
            boolean terms,
            int size,
            long minDocCount,
            String orderTemplate,
            DateKey date,
            HistKey hist
        ) {
            super(b, leaves, child);
            this.source = source;
            this.keyExpr = keyExpr;
            this.keyKind = keyKind;
            this.terms = terms;
            this.size = size;
            this.minDocCount = minDocCount;
            this.orderTemplate = orderTemplate;
            this.date = date;
            this.hist = hist;
        }

        static Grouped terms(TermsAggregationBuilder t, Map<String, Column> cols, Leaves leaves, BucketSpec child) {
            Source c = sourceOf(t, cols);
            if (t.size() > MAX_TERMS_SIZE) {
                throw new Unsupported("aggregation [" + t.getName() + "] asks for " + t.size() + " buckets; at most " + MAX_TERMS_SIZE + " are served");
            }
            KeyKind kind = kindOf(c);
            String order = orderClause(t.order(), leaves.metrics());
            if (order == null) {
                throw new Unsupported("aggregation [" + t.getName() + "] has an order the table cannot sort by: " + t.order());
            }
            return new Grouped(t, leaves, child, c, c.expr(), kind, true, t.size(), t.minDocCount(), order, null, null);
        }

        static Grouped dateHistogram(DateHistogramAggregationBuilder dh, Map<String, Column> cols, Leaves leaves, BucketSpec child) {
            Source c = sourceOf(dh, cols);
            ZoneId tz = dh.timeZone() == null ? ZoneOffset.UTC : dh.timeZone();
            DateExpr expr = dateExpr(dh.getName(), c, tz, dh.getFixedInterval(), dh.getCalendarInterval(), dh.offset());
            DocValueFormat format = dateFormat(dh.format(), tz);
            LongBounds bounds = null;
            if (dh.extendedBounds() != null) {
                // Bounds given as epoch milliseconds (what dashboards send) are rounded to the
                // histogram's grid; bounds given as date strings are not readable from here.
                Long min = dh.extendedBounds().getMin();
                Long max = dh.extendedBounds().getMax();
                if (min == null && max == null) {
                    throw new Unsupported("aggregation [" + dh.getName() + "] gives extended_bounds as dates; epoch milliseconds are served");
                }
                Rounding.Prepared prepared = expr.rounding().prepareForUnknown();
                bounds = new LongBounds(min == null ? null : prepared.round(min), max == null ? null : prepared.round(max));
            }
            DateKey date = new DateKey(format, expr.rounding(), bounds, dh.keyed(), dh.offset());
            return new Grouped(dh, leaves, child, c, expr.keyExpr(), KeyKind.DATE, false, Integer.MAX_VALUE, dh.minDocCount(), "{key} ASC", date, null);
        }

        static Grouped histogram(HistogramAggregationBuilder h, Map<String, Column> cols, Leaves leaves, BucketSpec child) {
            Source c = sourceOf(h, cols);
            String keyExpr = histExpr(h.getName(), c, h.interval(), h.offset());
            if (h.order() != null && !InternalOrder.isKeyOrder(h.order())) {
                throw new Unsupported("aggregation [" + h.getName() + "] has an order other than by key");
            }
            String order = h.order() != null && InternalOrder.isKeyDesc(h.order()) ? "{key} DESC" : "{key} ASC";
            HistKey hist = new HistKey(h.interval(), h.offset(), h.keyed());
            return new Grouped(h, leaves, child, c, keyExpr, KeyKind.DOUBLE, false, Integer.MAX_VALUE, h.minDocCount(), order, null, hist);
        }

        /** The SQL expression of a date bucket's key and the rounding it stands for. */
        record DateExpr(String keyExpr, Rounding rounding) {}

        /** The date bucket of {@code c} at the interval given, in the request's zone; the aggregation's name is for the refusal. */
        static DateExpr dateExpr(String agg, Source c, ZoneId tz, DateHistogramInterval fixed, DateHistogramInterval calendar, long offset) {
            if (!c.isTimestamp()) {
                throw new Unsupported("aggregation [" + agg + "] needs a date value, " + c.label() + " is " + c.typeName());
            }
            String source = "(" + c.expr() + ")";
            if (!ZoneOffset.UTC.equals(tz) && !"UTC".equals(tz.getId())) {
                // Bucket boundaries fall on local time in the request's zone; the
                // epoch value of each boundary is unchanged.
                source = "arrow_cast(" + source + ", 'Timestamp(Millisecond, Some(\"" + tz.getId() + "\"))')";
            }
            String keyExpr;
            DateIntervalWrapper wrapper = new DateIntervalWrapper();
            if (fixed != null) {
                long ms = fixed.estimateMillis();
                if (ms <= 0) {
                    throw new Unsupported("aggregation [" + agg + "] has an interval of no length");
                }
                String origin = offset == 0 ? "" : ", TIMESTAMP '1970-01-01T00:00:00Z' + INTERVAL '" + offset + " milliseconds'";
                keyExpr = "date_bin(INTERVAL '" + ms + " milliseconds', " + source + origin + ")";
                wrapper.fixedInterval(fixed);
            } else if (calendar != null) {
                if (offset != 0) {
                    throw new Unsupported("aggregation [" + agg + "] combines a calendar interval with an offset");
                }
                String unit = calendarUnit(calendar);
                if (unit == null) {
                    throw new Unsupported("aggregation [" + agg + "] has calendar interval [" + calendar + "]; one unit at a time is served");
                }
                keyExpr = "date_trunc('" + unit + "', " + source + ")";
                wrapper.calendarInterval(calendar);
            } else {
                throw new Unsupported("aggregation [" + agg + "] names no interval");
            }
            return new DateExpr(keyExpr, wrapper.createRounding(tz, offset));
        }

        /** How a date key prints: the request's format, else the default. */
        static DocValueFormat dateFormat(String format, ZoneId tz) {
            String fmt = format == null ? DEFAULT_DATE_FORMAT : format;
            return new DocValueFormat.DateTime(DateFormatter.forPattern(fmt), tz, DateFieldMapper.Resolution.MILLISECONDS);
        }

        /** The numeric bucket of {@code c} at the interval and offset given. */
        static String histExpr(String agg, Source c, double interval, double offset) {
            if (!c.isNumeric()) {
                throw new Unsupported("aggregation [" + agg + "] needs a numeric value, " + c.label() + " is " + c.typeName());
            }
            if (interval <= 0) {
                throw new Unsupported("aggregation [" + agg + "] has an interval of no length");
            }
            String f = "(" + c.expr() + ")";
            return "floor((" + f + " - " + offset + ") / " + interval + ") * " + interval + " + " + offset;
        }

        static KeyKind kindOf(Source c) {
            return switch (c.type()) {
                case STRING -> KeyKind.STR;
                case BOOL -> KeyKind.BOOL;
                case DATE -> KeyKind.DATE;
                case INT -> KeyKind.LONG;
                case FLOAT -> KeyKind.DOUBLE;
                default -> throw new Unsupported(c.label() + " of type " + c.typeName() + " cannot key buckets");
            };
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
            return keyLiteral(keyKind, key);
        }

        /** A key of {@code kind}, as a row returned it, as an SQL literal. */
        static String keyLiteral(KeyKind kind, Object key) {
            return switch (kind) {
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

        private Explicit(AggregationBuilder b, Leaves leaves, BucketSpec child, ExplicitKind kind, List<ExplicitBucket> buckets, boolean keyed, DocValueFormat format) {
            super(b, leaves, child);
            this.kind = kind;
            this.buckets = buckets;
            this.keyed = keyed;
            this.format = format;
        }

        static Explicit filters(FiltersAggregationBuilder f, Map<String, Column> cols, String table, Leaves leaves, BucketSpec child) {
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
            return new Explicit(f, leaves, child, ExplicitKind.FILTERS, buckets, f.isKeyed(), DocValueFormat.RAW);
        }

        static Explicit range(RangeAggregationBuilder r, Map<String, Column> cols, Leaves leaves, BucketSpec child) {
            Source c = sourceOf(r, cols);
            if (!c.isNumeric()) {
                throw new Unsupported("aggregation [" + r.getName() + "] needs a numeric value, " + c.label() + " is " + c.typeName());
            }
            List<ExplicitBucket> buckets = new ArrayList<>();
            for (RangeAggregator.Range range : r.ranges()) {
                double from = range.getFrom();
                double to = range.getTo();
                buckets.add(new ExplicitBucket(range.getKey(), between(c, from, to), from, to));
            }
            return new Explicit(r, leaves, child, ExplicitKind.RANGE, buckets, r.keyed(), DocValueFormat.RAW);
        }

        static Explicit dateRange(DateRangeAggregationBuilder r, Map<String, Column> cols, Leaves leaves, BucketSpec child) {
            Source c = sourceOf(r, cols);
            if (!c.isTimestamp()) {
                throw new Unsupported("aggregation [" + r.getName() + "] needs a date value, " + c.label() + " is " + c.typeName());
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
            return new Explicit(r, leaves, child, ExplicitKind.DATE_RANGE, buckets, r.keyed(), format);
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
        private static String between(Source c, double from, double to) {
            String f = "(" + c.expr() + ")";
            List<String> parts = new ArrayList<>();
            if (!Double.isInfinite(from) && !Double.isNaN(from)) {
                parts.add(f + " >= " + bound(c, from));
            }
            if (!Double.isInfinite(to) && !Double.isNaN(to)) {
                parts.add(f + " < " + bound(c, to));
            }
            return parts.isEmpty() ? InfinoQueryTranslator.ALL : "(" + String.join(" AND ", parts) + ")";
        }

        private static String bound(Source c, double v) {
            if (c.isTimestamp()) {
                return literal(Instant.ofEpochMilli((long) v).toString());
            }
            return v == Math.rint(v) && Math.abs(v) < 9.0e15 ? Long.toString((long) v) : Double.toString(v);
        }

        /** Build from one row per bucket (doc_count and metrics), each bucket's top hits and its nested aggregation. */
        InternalAggregation build(List<Map<String, Object>> rows, List<List<InternalAggregation>> topHits, List<InternalAggregation> nested) {
            String name = builder.getName();
            Map<String, Object> meta = builder.getMetadata();
            switch (kind) {
                case FILTERS -> {
                    List<InternalFilters.InternalBucket> out = new ArrayList<>();
                    for (int i = 0; i < buckets.size(); i++) {
                        out.add(new InternalFilters.InternalBucket(buckets.get(i).key(), asLong(rows.get(i).get("doc_count")), subs(rows.get(i), topHits.get(i), nested.get(i)), keyed));
                    }
                    return new InternalFilters(name, out, keyed, meta);
                }
                case RANGE -> {
                    InternalRange.Factory<InternalRange.Bucket, ?> factory = new InternalRange.Factory<>();
                    List<InternalRange.Bucket> out = new ArrayList<>();
                    for (int i = 0; i < buckets.size(); i++) {
                        ExplicitBucket b = buckets.get(i);
                        out.add(factory.createBucket(b.key(), b.from(), b.to(), asLong(rows.get(i).get("doc_count")), subs(rows.get(i), topHits.get(i), nested.get(i)), keyed, format));
                    }
                    return factory.create(name, out, format, keyed, meta);
                }
                default -> {
                    List<InternalDateRange.Bucket> out = new ArrayList<>();
                    for (int i = 0; i < buckets.size(); i++) {
                        ExplicitBucket b = buckets.get(i);
                        out.add(InternalDateRange.FACTORY.createBucket(b.key(), b.from(), b.to(), asLong(rows.get(i).get("doc_count")), subs(rows.get(i), topHits.get(i), nested.get(i)), keyed, format));
                    }
                    return InternalDateRange.FACTORY.create(name, out, format, keyed, meta);
                }
            }
        }

        @Override
        InternalAggregation empty() {
            List<Map<String, Object>> rows = new ArrayList<>();
            List<List<InternalAggregation>> topHits = new ArrayList<>();
            List<InternalAggregation> nested = new ArrayList<>();
            for (int i = 0; i < buckets.size(); i++) {
                rows.add(Map.of());
                topHits.add(emptyHits());
                nested.add(child == null ? null : child.empty());
            }
            return build(rows, topHits, nested);
        }
    }

    /**
     * {@code significant_terms}: the terms of the query's rows whose share
     * there stands out against their share of the whole table (or of the
     * background filter's rows). The level is a terms GROUP BY over the query
     * relation for the foreground counts, and a second over the table for the
     * background counts of the same terms; the scoring and the cut to size
     * are the stock reduction's, from those four counts.
     */
    static final class Significant extends BucketSpec {
        final String table;
        final Source source;
        final KeyKind keyKind;
        final String backgroundWhere;
        final int candidates;
        final long minDocCount;
        final SignificanceHeuristic heuristic;
        final TermsAggregator.BucketCountThresholds thresholds;

        private Significant(
            SignificantTermsAggregationBuilder b,
            Leaves leaves,
            BucketSpec child,
            String table,
            Source source,
            KeyKind keyKind,
            String backgroundWhere,
            int candidates,
            long minDocCount,
            SignificanceHeuristic heuristic,
            TermsAggregator.BucketCountThresholds thresholds
        ) {
            super(b, leaves, child);
            this.table = table;
            this.source = source;
            this.keyKind = keyKind;
            this.backgroundWhere = backgroundWhere;
            this.candidates = candidates;
            this.minDocCount = minDocCount;
            this.heuristic = heuristic;
            this.thresholds = thresholds;
        }

        static Significant of(SignificantTermsAggregationBuilder st, Map<String, Column> cols, String table, Leaves leaves, BucketSpec child) {
            Source c = sourceOf(st, cols);
            KeyKind kind = Grouped.kindOf(c);
            if (kind == KeyKind.DOUBLE) {
                throw new Unsupported("aggregation [" + st.getName() + "] needs a keyword, boolean, integer or date value, " + c.label() + " is " + c.typeName());
            }
            if (st.includeExclude() != null) {
                throw new Unsupported("aggregation [" + st.getName() + "] has include/exclude, which is not served");
            }
            TermsAggregator.BucketCountThresholds thresholds = new TermsAggregator.BucketCountThresholds(st.bucketCountThresholds());
            if (thresholds.getRequiredSize() > MAX_TERMS_SIZE) {
                throw new Unsupported("aggregation [" + st.getName() + "] asks for " + thresholds.getRequiredSize() + " buckets; at most " + MAX_TERMS_SIZE + " are served");
            }
            if (thresholds.getShardSize() < 0) {
                // As stock: a shard looks past the requested size, since the score reorders.
                thresholds.setShardSize(BucketUtils.suggestShardSideQueueSize(thresholds.getRequiredSize()));
            }
            int candidates = Math.min(MAX_TERMS_SIZE, Math.max(thresholds.getShardSize(), thresholds.getRequiredSize()));
            String background = InfinoQueryTranslator.ALL;
            if (st.backgroundFilter() != null) {
                background = InfinoQueryTranslator.predicate(st.backgroundFilter(), cols, table, System.currentTimeMillis());
                if (background == null) {
                    throw new Unsupported("aggregation [" + st.getName() + "] has a background filter the table cannot answer: " + st.backgroundFilter());
                }
            }
            SignificanceHeuristic heuristic = st.significanceHeuristic() == null ? new JLHScore() : st.significanceHeuristic();
            return new Significant(st, leaves, child, table, c, kind, background, candidates, thresholds.getMinDocCount(), heuristic, thresholds);
        }

        String keyExpr() {
            return "(" + source.expr() + ")";
        }

        /** One candidate term with both of its counts. */
        record Candidate(Object key, long subsetDf, long supersetDf, Map<String, Object> row) {}

        /** Build from the candidates, each with its top hits and nested aggregation, and the two population sizes. */
        InternalAggregation build(List<Candidate> candidates, List<List<InternalAggregation>> topHits, List<InternalAggregation> nested, long subsetSize, long supersetSize) {
            String name = builder.getName();
            Map<String, Object> meta = builder.getMetadata();
            // The score is left for the reduction, which computes it from the four counts.
            if (keyKind == KeyKind.STR || keyKind == KeyKind.BOOL) {
                List<SignificantStringTerms.Bucket> out = new ArrayList<>();
                for (int i = 0; i < candidates.size(); i++) {
                    Candidate c = candidates.get(i);
                    InternalAggregations subs = subs(c.row(), topHits.get(i), nested.get(i));
                    out.add(new SignificantStringTerms.Bucket(new BytesRef(c.key().toString()), c.subsetDf(), subsetSize, c.supersetDf(), supersetSize, subs, DocValueFormat.RAW, 0.0));
                }
                return new SignificantStringTerms(name, meta, DocValueFormat.RAW, subsetSize, supersetSize, heuristic, out, thresholds);
            }
            List<InfinoAggregationWire.SignificantLongBucket> out = new ArrayList<>();
            for (int i = 0; i < candidates.size(); i++) {
                Candidate c = candidates.get(i);
                InternalAggregations subs = subs(c.row(), topHits.get(i), nested.get(i));
                out.add(new InfinoAggregationWire.SignificantLongBucket(asLong(c.key()), c.subsetDf(), c.supersetDf(), subs));
            }
            return InfinoAggregationWire.significantLongTerms(name, meta, thresholds.getRequiredSize(), minDocCount, DocValueFormat.RAW, subsetSize, supersetSize, heuristic, out);
        }

        @Override
        InternalAggregation empty() {
            return build(List.of(), List.of(), List.of(), 0, 0);
        }
    }

    /** One source of a composite aggregation: a key expression and how its values order and print. */
    record CompositeSource(String name, Source source, String keyExpr, KeyKind keyKind, DocValueFormat format, boolean desc, boolean missingBucket, MissingOrder missingOrder) {
        /** Whether a missing key sorts before the present ones. */
        boolean nullsFirst() {
            return missingOrder == MissingOrder.FIRST || (missingOrder == MissingOrder.DEFAULT && !desc);
        }
    }

    /**
     * {@code composite}: one GROUP BY over every source's key, the buckets in
     * key order, the page after the request's {@code after} key by a keyset
     * predicate over the grouped keys (so no earlier page is scanned), cut
     * to {@code size}.
     */
    static final class Composite extends BucketSpec {
        final List<CompositeSource> sources;
        final int size;
        final Object[] after;

        private Composite(CompositeAggregationBuilder b, Leaves leaves, BucketSpec child, List<CompositeSource> sources, int size, Object[] after) {
            super(b, leaves, child);
            this.sources = sources;
            this.size = size;
            this.after = after;
        }

        static Composite of(CompositeAggregationBuilder c, Map<String, Column> cols, Leaves leaves, BucketSpec child) {
            if (c.size() > MAX_TERMS_SIZE) {
                throw new Unsupported("aggregation [" + c.getName() + "] asks for " + c.size() + " buckets; at most " + MAX_TERMS_SIZE + " are served");
            }
            List<CompositeSource> sources = new ArrayList<>();
            for (CompositeValuesSourceBuilder<?> s : c.sources()) {
                sources.add(source(c.getName(), s, cols));
            }
            Object[] after = null;
            Map<String, Object> afterKey = afterOf(c);
            if (afterKey != null) {
                after = new Object[sources.size()];
                for (int i = 0; i < sources.size(); i++) {
                    CompositeSource s = sources.get(i);
                    if (!afterKey.containsKey(s.name())) {
                        throw new Unsupported("aggregation [" + c.getName() + "] has an after key without [" + s.name() + "]");
                    }
                    Object v = afterKey.get(s.name());
                    if (v == null && !s.missingBucket()) {
                        throw new Unsupported("aggregation [" + c.getName() + "] has a null after key for [" + s.name() + "], which has no missing bucket");
                    }
                    after[i] = v == null ? null : afterLiteral(c.getName(), s, v);
                }
            }
            return new Composite(c, leaves, child, sources, c.size(), after);
        }

        /**
         * The request's {@code after} key, or null. The builder keeps it
         * without a getter, so it is read back from the builder's own JSON
         * form: {@code {name: {composite: {..., after: {...}}}}}.
         */
        @SuppressWarnings("unchecked")
        private static Map<String, Object> afterOf(CompositeAggregationBuilder c) {
            try (XContentBuilder xb = XContentFactory.jsonBuilder()) {
                xb.startObject();
                c.toXContent(xb, ToXContent.EMPTY_PARAMS);
                xb.endObject();
                Map<String, Object> map = XContentHelper.convertToMap(BytesReference.bytes(xb), false, MediaTypeRegistry.JSON).v2();
                Map<String, Object> agg = (Map<String, Object>) map.get(c.getName());
                Map<String, Object> body = agg == null ? null : (Map<String, Object>) agg.get(CompositeAggregationBuilder.NAME);
                return body == null ? null : (Map<String, Object>) body.get(CompositeAggregationBuilder.AFTER_FIELD_NAME.getPreferredName());
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        private static CompositeSource source(String agg, CompositeValuesSourceBuilder<?> s, Map<String, Column> cols) {
            Column column = null;
            if (s.field() != null) {
                column = InfinoSearch.columnFor(cols, s.field());
                if (column == null) {
                    throw new Unsupported("aggregation [" + agg + "] source [" + s.name() + "] names field [" + s.field() + "], which the table has no column for");
                }
            }
            Source c;
            if (s.script() != null) {
                Sql sql = InfinoScriptTranslator.translate(s.script(), cols, column, System.currentTimeMillis());
                c = new Source(sql.expr(), sql.type(), "script [" + s.script().getIdOrCode() + "]");
            } else if (column != null) {
                c = Source.of(column);
            } else {
                throw new Unsupported("aggregation [" + agg + "] source [" + s.name() + "] names no field or script");
            }
            boolean desc = s.order() == SortOrder.DESC;
            MissingOrder missing = s.missingOrder() == null ? MissingOrder.DEFAULT : s.missingOrder();
            if (s instanceof TermsValuesSourceBuilder) {
                KeyKind kind = Grouped.kindOf(c);
                DocValueFormat format = kind == KeyKind.DATE
                    ? Grouped.dateFormat(s.format(), ZoneOffset.UTC)
                    : kind == KeyKind.BOOL ? DocValueFormat.BOOLEAN : DocValueFormat.RAW;
                return new CompositeSource(s.name(), c, "(" + c.expr() + ")", kind, format, desc, s.missingBucket(), missing);
            }
            if (s instanceof DateHistogramValuesSourceBuilder dh) {
                ZoneId tz = dh.timeZone() == null ? ZoneOffset.UTC : dh.timeZone();
                Grouped.DateExpr expr = Grouped.dateExpr(agg, c, tz, dh.getIntervalAsFixed(), dh.getIntervalAsCalendar(), dh.offset());
                // As stock: the key is epoch milliseconds unless the source names a format.
                DocValueFormat format = s.format() == null ? DocValueFormat.RAW : Grouped.dateFormat(s.format(), tz);
                return new CompositeSource(s.name(), c, expr.keyExpr(), KeyKind.DATE, format, desc, s.missingBucket(), missing);
            }
            if (s instanceof HistogramValuesSourceBuilder h) {
                String keyExpr = Grouped.histExpr(agg, c, h.interval(), 0);
                return new CompositeSource(s.name(), c, keyExpr, KeyKind.DOUBLE, DocValueFormat.RAW, desc, s.missingBucket(), missing);
            }
            throw new Unsupported("aggregation [" + agg + "] source [" + s.name() + "] of type [" + s.getClass().getSimpleName() + "] is not served");
        }

        /** The after value of a source as an SQL literal, read as the source's key kind. */
        private static String afterLiteral(String agg, CompositeSource s, Object v) {
            if (s.keyKind() == KeyKind.DATE && v instanceof String text) {
                Long ms = InfinoQueryTranslator.dateMathMillis(text, null, false, ZoneOffset.UTC, System.currentTimeMillis());
                if (ms == null) {
                    throw new Unsupported("aggregation [" + agg + "] has an after key [" + text + "] for [" + s.name() + "] that is not a date");
                }
                return Grouped.keyLiteral(KeyKind.DATE, ms);
            }
            if ((s.keyKind() == KeyKind.LONG || s.keyKind() == KeyKind.DOUBLE || s.keyKind() == KeyKind.DATE) && !(v instanceof Number)) {
                throw new Unsupported("aggregation [" + agg + "] has an after key [" + v + "] for [" + s.name() + "] that is not a number");
            }
            return Grouped.keyLiteral(s.keyKind(), v);
        }

        /** ORDER BY over the key aliases: each source's direction, missing keys where the source puts them. */
        String order() {
            List<String> parts = new ArrayList<>();
            for (int i = 0; i < sources.size(); i++) {
                CompositeSource s = sources.get(i);
                parts.add("k" + i + (s.desc() ? " DESC" : " ASC") + (s.nullsFirst() ? " NULLS FIRST" : " NULLS LAST"));
            }
            return String.join(", ", parts);
        }

        /**
         * The buckets past the after key, in this order: {@code (k0 > a0) OR
         * (k0 = a0 AND k1 > a1) ...}, each comparison turned for a descending
         * source and a missing key placed where its source orders it.
         */
        String keysetPredicate() {
            List<String> alternatives = new ArrayList<>();
            List<String> equal = new ArrayList<>();
            for (int i = 0; i < sources.size(); i++) {
                CompositeSource s = sources.get(i);
                String k = "k" + i;
                String lit = (String) after[i];
                String strict;
                if (lit == null) {
                    // After the missing bucket: everything present when missing sorts first, nothing otherwise.
                    strict = s.nullsFirst() ? k + " IS NOT NULL" : InfinoQueryTranslator.NONE;
                } else {
                    strict = k + (s.desc() ? " < " : " > ") + lit;
                    if (!s.nullsFirst()) {
                        strict = "(" + strict + " OR " + k + " IS NULL)";
                    }
                }
                List<String> conj = new ArrayList<>(equal);
                conj.add(strict);
                alternatives.add("(" + String.join(" AND ", conj) + ")");
                equal.add(lit == null ? k + " IS NULL" : k + " = " + lit);
            }
            return "(" + String.join(" OR ", alternatives) + ")";
        }

        /** The key values of a row, typed as the stock object carries them. */
        Comparable<?>[] keyOf(Map<String, Object> row) {
            Comparable<?>[] key = new Comparable<?>[sources.size()];
            for (int i = 0; i < sources.size(); i++) {
                Object v = row.get("k" + i);
                if (v == null) {
                    continue;
                }
                key[i] = switch (sources.get(i).keyKind()) {
                    case STR -> new BytesRef(v.toString());
                    case BOOL -> Boolean.parseBoolean(v.toString()) ? 1L : 0L;
                    case LONG, DATE -> asLong(v);
                    case DOUBLE -> asDouble(v, 0.0);
                };
            }
            return key;
        }

        /** Build from the page's rows in order, each with its top hits and nested aggregation. */
        InternalAggregation build(List<Map<String, Object>> rows, List<List<InternalAggregation>> topHits, List<InternalAggregation> nested) {
            List<String> names = new ArrayList<>();
            List<DocValueFormat> formats = new ArrayList<>();
            int[] reverseMuls = new int[sources.size()];
            MissingOrder[] missingOrders = new MissingOrder[sources.size()];
            for (int i = 0; i < sources.size(); i++) {
                CompositeSource s = sources.get(i);
                names.add(s.name());
                formats.add(s.format());
                reverseMuls[i] = s.desc() ? -1 : 1;
                missingOrders[i] = s.missingOrder();
            }
            List<InfinoAggregationWire.CompositeBucket> buckets = new ArrayList<>();
            for (int i = 0; i < rows.size(); i++) {
                Map<String, Object> row = rows.get(i);
                buckets.add(new InfinoAggregationWire.CompositeBucket(keyOf(row), asLong(row.get("doc_count")), subs(row, topHits.get(i), nested.get(i))));
            }
            Comparable<?>[] afterKey = rows.isEmpty() ? null : keyOf(rows.get(rows.size() - 1));
            return InfinoAggregationWire.composite(builder.getName(), builder.getMetadata(), size, names, formats, reverseMuls, missingOrders, buckets, afterKey);
        }

        @Override
        InternalAggregation empty() {
            return build(List.of(), List.of(), List.of());
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
        if (spec instanceof Significant s) {
            return evalSignificant(s, from, where, total);
        }
        if (spec instanceof Composite c) {
            return evalComposite(c, from, where);
        }
        return evalChain((Grouped) spec, from, where, total);
    }

    private InternalAggregation evalExplicit(Explicit e, String from, String where) {
        List<Map<String, Object>> rows = new ArrayList<>();
        List<List<InternalAggregation>> topHits = new ArrayList<>();
        List<InternalAggregation> nested = new ArrayList<>();
        for (ExplicitBucket b : e.buckets) {
            String w = "(" + where + ") AND (" + b.predicate() + ")";
            String select = "COUNT(*) AS doc_count" + (e.metrics.isEmpty() ? "" : ", " + metricColumns(e.metrics));
            Map<String, Object> row = oneRow("SELECT " + select + " FROM " + from + " WHERE " + w);
            long docCount = asLong(row.get("doc_count"));
            rows.add(row);
            topHits.add(bucketHits(e, from, w, docCount));
            nested.add(e.child == null ? null : evalBucket(e.child, from, w, docCount));
        }
        return e.build(rows, topHits, nested);
    }

    /** The top hits of one bucket of {@code spec}, one statement per top_hits over {@code from WHERE where}. */
    private List<InternalAggregation> bucketHits(BucketSpec spec, String from, String where, long docCount) {
        List<InternalAggregation> out = new ArrayList<>();
        for (TopHits th : spec.hits) {
            out.add(th.build(topHitRows(th, from, where), docCount, index));
        }
        return out;
    }

    /** The first rows of {@code from WHERE where} in the top_hits' order, each with its id, source and sort values. */
    private static List<Map<String, Object>> topHitRows(TopHits th, String from, String where) {
        List<String> cols = new ArrayList<>(List.of(InfinoQueryTranslator.OSID, InfinoQueryTranslator.SOURCE));
        cols.addAll(th.rowColumns());
        return InfinoNative.sqlRows("SELECT " + String.join(", ", cols) + " FROM " + from + " WHERE " + where + " ORDER BY " + th.order() + " LIMIT " + th.fetch());
    }

    /**
     * {@code significant_terms}: the foreground counts are a terms GROUP BY
     * over the query relation, the most frequent terms first; the background
     * counts of those terms are a second GROUP BY over the table, and the
     * table's size (under the background filter) is the superset size.
     */
    private InternalAggregation evalSignificant(Significant s, String from, String where, long subsetSize) {
        String key = s.keyExpr();
        String select = key + " AS k0, COUNT(*) AS doc_count" + (s.metrics.isEmpty() ? "" : ", " + metricColumns(s.metrics));
        String having = s.minDocCount > 1 ? " HAVING COUNT(*) >= " + s.minDocCount : "";
        List<Map<String, Object>> foreground = InfinoNative.sqlRows(
            "SELECT * FROM (SELECT " + select + " FROM " + from + " WHERE (" + where + ") AND " + key + " IS NOT NULL GROUP BY 1" + having
                + ") t ORDER BY doc_count DESC, k0 ASC LIMIT " + s.candidates
        );
        String table = quoteIdent(s.table);
        Map<String, Object> size = oneRow("SELECT COUNT(*) AS c FROM " + table + " WHERE " + s.backgroundWhere);
        long supersetSize = asLong(size.get("c"));
        Map<String, Long> background = new HashMap<>();
        if (!foreground.isEmpty()) {
            List<String> literals = new ArrayList<>();
            for (Map<String, Object> row : foreground) {
                literals.add(Grouped.keyLiteral(s.keyKind, row.get("k0")));
            }
            for (Map<String, Object> row : InfinoNative.sqlRows(
                "SELECT " + key + " AS k0, COUNT(*) AS bg FROM " + table + " WHERE (" + s.backgroundWhere + ") AND " + key + " IN (" + String.join(", ", literals) + ") GROUP BY 1"
            )) {
                background.put(keyString(row.get("k0")), asLong(row.get("bg")));
            }
        }
        List<Significant.Candidate> candidates = new ArrayList<>();
        List<List<InternalAggregation>> topHits = new ArrayList<>();
        List<InternalAggregation> nested = new ArrayList<>();
        for (Map<String, Object> row : foreground) {
            Object k = row.get("k0");
            long subsetDf = asLong(row.get("doc_count"));
            long supersetDf = background.getOrDefault(keyString(k), 0L);
            candidates.add(new Significant.Candidate(k, subsetDf, Math.max(subsetDf, supersetDf), row));
            String w = "(" + where + ") AND " + key + " = " + Grouped.keyLiteral(s.keyKind, k);
            topHits.add(bucketHits(s, from, w, subsetDf));
            nested.add(s.child == null ? null : evalBucket(s.child, from, w, subsetDf));
        }
        return s.build(candidates, topHits, nested, subsetSize, supersetSize);
    }

    /** The composite page: the GROUP BY over every key, past the after key, in key order, cut to size. */
    private InternalAggregation evalComposite(Composite c, String from, String where) {
        List<String> keyCols = new ArrayList<>();
        List<String> present = new ArrayList<>();
        StringBuilder positions = new StringBuilder();
        for (int i = 0; i < c.sources.size(); i++) {
            CompositeSource s = c.sources.get(i);
            keyCols.add(s.keyExpr() + " AS k" + i);
            if (!s.missingBucket()) {
                present.add(s.keyExpr() + " IS NOT NULL");
            }
            positions.append(i == 0 ? "" : ", ").append(i + 1);
        }
        String select = String.join(", ", keyCols) + ", COUNT(*) AS doc_count" + (c.metrics.isEmpty() ? "" : ", " + metricColumns(c.metrics));
        String w = present.isEmpty() ? where : "(" + where + ") AND " + String.join(" AND ", present);
        String inner = "SELECT " + select + " FROM " + from + " WHERE " + w + " GROUP BY " + positions;
        String page = c.after == null ? "" : " WHERE " + c.keysetPredicate();
        List<Map<String, Object>> rows = InfinoNative.sqlRows("SELECT * FROM (" + inner + ") t" + page + " ORDER BY " + c.order() + " LIMIT " + c.size);
        List<List<InternalAggregation>> topHits = new ArrayList<>();
        List<InternalAggregation> nested = new ArrayList<>();
        for (Map<String, Object> row : rows) {
            long docCount = asLong(row.get("doc_count"));
            String bucket = "(" + where + ") AND " + bucketRestriction(c, row);
            topHits.add(bucketHits(c, from, bucket, docCount));
            nested.add(c.child == null ? null : evalBucket(c.child, from, bucket, docCount));
        }
        return c.build(rows, topHits, nested);
    }

    /** {@code (k0 = a AND k1 = b ...)} over a composite's key expressions, for one of its buckets. */
    private static String bucketRestriction(Composite c, Map<String, Object> row) {
        List<String> ands = new ArrayList<>();
        for (int i = 0; i < c.sources.size(); i++) {
            CompositeSource s = c.sources.get(i);
            Object key = row.get("k" + i);
            ands.add(key == null ? s.keyExpr() + " IS NULL" : s.keyExpr() + " = " + Grouped.keyLiteral(s.keyKind(), key));
        }
        return "(" + String.join(" AND ", ands) + ")";
    }

    /**
     * Evaluate a chain of grouped levels: one GROUP BY per depth over all keys
     * above it, each level restricted to the buckets its parent kept, then
     * fold the rows into nested buckets. A level's top hits are one window
     * statement per top_hits, partitioned by the level's keys. An explicit,
     * significant or composite level under the chain's leaf is evaluated per
     * leaf bucket.
     */
    private InternalAggregation evalChain(Grouped root, String from, String where, long total) {
        List<Grouped> chain = new ArrayList<>();
        BucketSpec cursor = root;
        while (cursor instanceof Grouped g) {
            chain.add(g);
            cursor = g.child;
        }
        BucketSpec leafChild = cursor;

        List<List<Map<String, Object>>> rowsByLevel = new ArrayList<>();
        List<Map<String, List<List<Map<String, Object>>>>> hitsByLevel = new ArrayList<>();
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
            hitsByLevel.add(levelHits(chain, i, kept, from, where));
            if (rows.isEmpty()) {
                break;
            }
            if (i + 1 < chain.size() && kept.size() > MAX_PARENT_TUPLES) {
                throw new Unsupported("aggregation [" + chain.get(i + 1).name() + "] would nest under " + kept.size() + " buckets; at most " + MAX_PARENT_TUPLES + " are served");
            }
        }
        while (rowsByLevel.size() < chain.size()) {
            rowsByLevel.add(List.of());
            hitsByLevel.add(Map.of());
        }
        return fold(chain, 0, List.of(), rowsByLevel, hitsByLevel, total, leafChild, from, where);
    }

    /**
     * The top hits of every bucket level {@code i} kept: one window statement
     * per top_hits, the window partitioned by the level's keys over the rows
     * of the kept buckets, the first {@code from + size} of each bucket in the
     * request's order. Keyed by the bucket's key tuple, one row list per
     * top_hits in spec order.
     */
    private static Map<String, List<List<Map<String, Object>>>> levelHits(List<Grouped> chain, int i, List<List<Object>> kept, String from, String where) {
        Grouped level = chain.get(i);
        Map<String, List<List<Map<String, Object>>>> out = new HashMap<>();
        if (level.hits.isEmpty() || kept.isEmpty()) {
            return out;
        }
        if (kept.size() > MAX_PARENT_TUPLES) {
            throw new Unsupported("aggregation [" + level.hits.get(0).name() + "] would run under " + kept.size() + " buckets; at most " + MAX_PARENT_TUPLES + " are served");
        }
        List<String> keyExprs = new ArrayList<>();
        List<String> keyCols = new ArrayList<>();
        List<String> aliases = new ArrayList<>();
        for (int j = 0; j <= i; j++) {
            keyExprs.add(chain.get(j).keyExpr);
            keyCols.add(chain.get(j).keyExpr + " AS k" + j);
            aliases.add("k" + j);
        }
        String restriction = tupleRestriction(chain, kept, i + 1);
        for (int h = 0; h < level.hits.size(); h++) {
            TopHits th = level.hits.get(h);
            List<String> cols = new ArrayList<>(aliases);
            cols.add(InfinoQueryTranslator.OSID);
            cols.add(InfinoQueryTranslator.SOURCE);
            cols.addAll(th.rowColumns());
            String sql = "SELECT " + String.join(", ", cols) + " FROM (SELECT t.*, " + String.join(", ", keyCols) + ", ROW_NUMBER() OVER (PARTITION BY "
                + String.join(", ", keyExprs) + " ORDER BY " + th.order() + ") AS hit_rank FROM " + from + " t WHERE (" + where + ") AND " + restriction
                + ") WHERE hit_rank <= " + th.fetch() + " ORDER BY " + String.join(", ", aliases) + ", hit_rank";
            for (Map<String, Object> row : InfinoNative.sqlRows(sql)) {
                List<Object> tuple = new ArrayList<>();
                for (int j = 0; j <= i; j++) {
                    tuple.add(row.get("k" + j));
                }
                List<List<Map<String, Object>>> perSpec = out.computeIfAbsent(keyString(tuple), k -> new ArrayList<>());
                while (perSpec.size() < level.hits.size()) {
                    perSpec.add(new ArrayList<>());
                }
                perSpec.get(h).add(row);
            }
        }
        return out;
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
        List<Map<String, List<List<Map<String, Object>>>>> hitsByLevel,
        long parentTotal,
        BucketSpec leafChild,
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
            List<InternalAggregation> topHits = new ArrayList<>();
            List<List<Map<String, Object>>> hitRows = hitsByLevel.get(depth).get(keyString(keys));
            for (int h = 0; h < level.hits.size(); h++) {
                List<Map<String, Object>> hits = hitRows == null || h >= hitRows.size() ? List.of() : hitRows.get(h);
                topHits.add(level.hits.get(h).build(hits, docCount, index));
            }
            InternalAggregation nested = null;
            if (depth + 1 < chain.size()) {
                nested = fold(chain, depth + 1, keys, rowsByLevel, hitsByLevel, docCount, leafChild, from, where);
            } else if (leafChild != null) {
                nested = evalBucket(leafChild, from, "(" + where + ") AND " + tupleRestriction(chain, List.of(keys), keys.size()), docCount);
            }
            return level.subs(row, topHits, nested);
        }, parentTotal);
    }

    /** A key tuple as one string, so tuples from two statements over the same expressions meet. */
    private static String keyString(List<Object> tuple) {
        StringBuilder sb = new StringBuilder();
        for (Object key : tuple) {
            if (sb.length() > 0) {
                sb.append(KEY_SEPARATOR);
            }
            sb.append(keyString(key));
        }
        return sb.toString();
    }

    private static String keyString(Object key) {
        if (key == null) {
            return NULL_KEY;
        }
        if (key instanceof Number n) {
            double d = n.doubleValue();
            return d == Math.rint(d) && Math.abs(d) < 9.0e15 ? Long.toString((long) d) : Double.toString(d);
        }
        return key.toString();
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
