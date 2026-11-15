/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.OpenSearchSecurityException;
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
import org.opensearch.search.aggregations.bucket.filter.FilterAggregationBuilder;
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
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

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
    /**
     * Groups a top terms level may have for its buckets to be picked here
     * rather than by a window statement: the bare GROUP BY is answered from
     * the table's statistics, the window over it is not, but every group
     * crosses the bridge to be picked here. Measured on a size 10 level, the
     * picking is ahead up to about a thousand groups and the window from
     * about fifteen hundred.
     */
    private static final int MAX_GROUPS_PICKED_HERE = 1_000;
    /** The alias the request's total takes when it is read beside the root metrics. */
    private static final String TOTAL_ALIAS = "request_total";
    /** Parent buckets a nested grouped level is restricted to; past this the key list is not worth a predicate. */
    private static final int MAX_PARENT_TUPLES = 1_000;
    /** The Arrow type a mapped date's column holds, which a date the index does not map is read as. */
    private static final String UNMAPPED_DATE_TYPE = "Timestamp(ms, \"UTC\")";
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
    /** The column of {@link #unionOf} naming which shard's table a row came from. */
    private static final String SHARD_COLUMN = "_infino_shard";
    /** The alias of {@link #unionOf}'s relation. */
    private static final String UNION_ALIAS = "_infino_shards";
    /** The alias of a significant_terms level's background rows. */
    private static final String BACKGROUND_ALIAS = "_infino_background";
    /** What separates a field's name from the name of one of its per-document figures ({@link Values#figure}). */
    private static final String PER_DOCUMENT = "#";
    /** The per-document figures a metric over a field some document gives several values reads. */
    private static final String FIGURE_SUM = "sum";
    private static final String FIGURE_COUNT = "count";
    private static final String FIGURE_MIN = "min";
    private static final String FIGURE_MAX = "max";
    /**
     * The aliases of the relations a statement over several values reads:
     * each layer of values ({@link #valuesRelation}), the rows it ends in,
     * a grouped level's bucket keys per document and the documents joined
     * back to them, and the documents with their per-document figures.
     */
    private static final String VALUE_LAYER = "value_layer_";
    private static final String VALUE_ROWS = "value_rows";
    private static final String BUCKET_KEYS = "bucket_keys";
    private static final String BUCKET_DOC = "bucket_doc";
    private static final String BUCKET_DOCS = "bucket_docs";
    private static final String METRIC_DOCS = "metric_docs";
    private static final String FIGURES = "figures_";
    private static final String FIGURE_DOC = "figure_doc";
    private static final String FIGURE_VALUE = "figure_value";

    private final Index index;
    /**
     * Whether these aggregations are one shard's of a search over several,
     * which the coordinating node reduces: a terms level then returns its
     * shard size of groups, as a stock shard does, not its final size.
     */
    private final boolean shardOfMany;
    /** The request being evaluated; set by {@link #evaluate}. */
    private Plan plan;
    /** The tables the aggregations read: the plan's, or each reached shard's (see {@link #evaluateOver}); set by {@link #evaluate}. */
    private List<String> tables = List.of();
    /**
     * Groups of a top terms level read ahead to count the request, by the
     * statement the level would run for them; a null value marks a level
     * with more groups than are picked here (see {@link #totalFromTopTerms}).
     */
    private final Map<String, List<Map<String, Object>>> readAhead = new HashMap<>();
    /** The table {@link #evaluate} reads, whose plain relation {@link #explicitCounts} counts directly. */
    private String table;

    /** A processor whose top hits name {@code index}; null for planning only. */
    InfinoAggregationProcessor(Index index) {
        this(index, false);
    }

    /** A processor for one shard of a search over {@code shardOfMany} several shards, or over the whole index. */
    InfinoAggregationProcessor(Index index, boolean shardOfMany) {
        this.index = index;
        this.shardOfMany = shardOfMany;
    }

    /**
     * An aggregation's {@code meta} as stock answers it: the request's, or
     * none when the request names none. The builder reads an empty map then,
     * which a response would print as {@code "meta": {}}.
     */
    static Map<String, Object> metadataOf(AggregationBuilder b) {
        Map<String, Object> meta = b.getMetadata();
        return meta.isEmpty() ? null : meta;
    }

    /**
     * A {@code filter} aggregation's query as a predicate, made by
     * {@code predicates}, or an {@link Unsupported} naming it.
     */
    static String filterPredicate(FilterAggregationBuilder f, Function<QueryBuilder, String> predicates) {
        String p = predicates.apply(f.getFilter());
        if (p == null) {
            throw new Unsupported("aggregation [" + f.getName() + "] has a filter the table cannot answer: " + f.getFilter());
        }
        return p;
    }

    /** An aggregation the table cannot serve; the request fails with this reason. */
    static final class Unsupported extends IllegalArgumentException {
        Unsupported(String message) {
            super("[infino] " + message);
        }
    }

    /** The aggregations of a request and its exact match count. */
    record Counted(InternalAggregations aggregations, long total) {}

    /**
     * The requested aggregations over the plan's relation, in request order,
     * as the stock response objects, with the request's exact match count.
     * {@code total} is that count when the hit path already has it; when
     * null it is counted here, beside the root metrics or a counting range's
     * buckets when there are any, so it costs no statement of its own.
     */
    Counted evaluate(Collection<AggregationBuilder> builders, Plan plan, Long total) {
        Translation q = plan.query();
        return evaluate(builders, plan, List.of(plan.table()), q.from(), q.where(), predicates(plan), () -> InfinoSearch.count(plan.table(), q), q, shardOfMany, total);
    }

    /**
     * The requested aggregations over the rows each of {@code shardPlans}
     * matches, one plan for each table of the reached shards, read as one
     * relation, with the request's exact match count over all of them. A
     * cardinality or percentiles result is built from the engine's exact
     * values, which cannot be merged from the shards' parts; the
     * coordinating node answers such an aggregation once this way (see
     * {@link InfinoExactAggregation}). {@code plan} is the first shard's,
     * which the aggregations are planned against.
     */
    Counted evaluateOver(Collection<AggregationBuilder> builders, Plan plan, List<Plan> shardPlans) {
        String from = unionOf(shardPlans);
        List<String> shardTables = new ArrayList<>();
        for (Plan p : shardPlans) {
            shardTables.add(p.table());
        }
        Function<QueryBuilder, String> predicates = qb -> perShard(qb, shardPlans);
        Supplier<Long> counter = () -> asLong(oneRow("SELECT COUNT(*) AS c FROM " + from).get("c"));
        // One answer for the whole index: each terms level at its final size.
        return evaluate(builders, plan, shardTables, from, InfinoQueryTranslator.ALL, predicates, counter, null, false, null);
    }

    /**
     * The aggregations over {@code from WHERE where}. When the count is
     * needed it comes from a top terms level's own groups if the relation is
     * the plan's own query ({@code own}, null when it is not; see
     * {@link #totalFromTopTerms}), else from {@code counter}.
     */
    private Counted evaluate(
        Collection<AggregationBuilder> builders,
        Plan plan,
        List<String> tables,
        String from,
        String where,
        Function<QueryBuilder, String> predicates,
        Supplier<Long> counter,
        Translation own,
        boolean shardSized,
        Long total
    ) {
        this.plan = plan;
        this.tables = tables;
        this.table = tables.size() == 1 ? tables.get(0) : null;
        List<Spec> specs = new ArrayList<>();
        int position = 0;
        for (AggregationBuilder b : builders) {
            specs.add(Spec.of(b, plan.cols(), plan.table(), predicates, plan.access(), position++, shardSized, plan.nowMillis()));
        }
        // A metric over a field some document gives several values reads each
        // document's figures over them, joined in once for the whole request.
        List<Source> figured = new ArrayList<>();
        multiMetricSources(specs, figured);
        String relation = figured.isEmpty() ? from : perDocument(from, figured);
        Map<String, InternalAggregation> byName = new HashMap<>();
        List<Metric> rootMetrics = new ArrayList<>();
        for (Spec spec : specs) {
            if (spec instanceof Metric m) {
                rootMetrics.add(m);
                continue;
            }
            if (total == null && spec instanceof Explicit e && countsOnly(e)) {
                // The buckets' statement counts the request's matches too.
                Map<String, Object> counted = explicitCounts(e, from, where, true);
                total = asLong(counted.get(TOTAL_ALIAS));
                byName.put(spec.name(), buildExplicitCounts(e, counted));
                continue;
            }
            if (total == null) {
                total = own == null ? null : totalFromTopTerms(spec, own);
            }
            if (total == null) {
                total = counter.get();
            }
            if (spec instanceof TopHits th) {
                byName.put(spec.name(), th.build(topHitRows(th, relation, where), total, index));
                continue;
            }
            byName.put(spec.name(), evalBucket((BucketSpec) spec, relation, where, total));
        }
        if (!rootMetrics.isEmpty()) {
            boolean counting = total == null;
            String select = (counting ? "COUNT(*) AS " + TOTAL_ALIAS + ", " : "") + metricColumns(rootMetrics);
            Map<String, Object> row = oneRow("SELECT " + select + " FROM " + relation + " WHERE " + where);
            if (counting) {
                total = asLong(row.get(TOTAL_ALIAS));
            }
            for (Metric m : rootMetrics) {
                byName.put(m.name(), m.build(row));
            }
        }
        if (total == null) {
            total = counter.get();
        }
        // Keep the request's order of aggregations.
        List<InternalAggregation> ordered = new ArrayList<>();
        for (AggregationBuilder b : builders) {
            ordered.add(byName.get(b.getName()));
        }
        log.debug("[infino] served {} aggregation(s) from {}", ordered.size(), tables);
        return new Counted(InternalAggregations.from(ordered), total);
    }

    /** Whether {@code builders} hold a metric whose result is built from exact values: cardinality, percentiles, percentile ranks. */
    static boolean needsExactValues(Collection<AggregationBuilder> builders) {
        for (AggregationBuilder b : builders) {
            if (b instanceof CardinalityAggregationBuilder || b instanceof PercentilesAggregationBuilder || b instanceof PercentileRanksAggregationBuilder) {
                return true;
            }
            if (needsExactValues(b.getSubAggregations())) {
                return true;
            }
        }
        return false;
    }

    /** How an aggregation's own query (a filters bucket) becomes a predicate over the plan's relation. */
    private static Function<QueryBuilder, String> predicates(Plan plan) {
        return qb -> InfinoQueryTranslator.predicate(qb, plan.cols(), plan.table(), plan.nowMillis());
    }

    /**
     * The rows of each plan's relation, every column any of the tables has
     * but vectors (which no aggregation reads), as one relation: a column a
     * table lacks is null in its rows, and {@link #SHARD_COLUMN} says which
     * plan a row came from.
     */
    static String unionOf(List<Plan> plans) {
        Map<String, Column> all = new LinkedHashMap<>();
        List<Map<String, Column>> each = new ArrayList<>();
        for (Plan p : plans) {
            Map<String, Column> cols = InfinoNative.columns(p.table());
            each.add(cols);
            for (Column c : cols.values()) {
                if (!c.vector()) {
                    all.putIfAbsent(c.name(), c);
                }
            }
        }
        List<String> branches = new ArrayList<>();
        for (int i = 0; i < plans.size(); i++) {
            List<String> select = new ArrayList<>();
            select.add(i + " AS " + quoteIdent(SHARD_COLUMN));
            for (String name : all.keySet()) {
                select.add(each.get(i).containsKey(name) ? quoteIdent(name) : "NULL AS " + quoteIdent(name));
            }
            Translation q = plans.get(i).query();
            branches.add("SELECT " + String.join(", ", select) + " FROM " + q.from() + " WHERE " + q.where());
        }
        return "(" + String.join(" UNION ALL ", branches) + ") AS " + quoteIdent(UNION_ALIAS);
    }

    /**
     * {@code qb} as a predicate over {@link #unionOf} the plans: one
     * predicate when it reads the same over every table, else each row
     * tested by its own table's (a full-text clause names its table).
     */
    private static String perShard(QueryBuilder qb, List<Plan> plans) {
        List<String> each = new ArrayList<>();
        for (Plan p : plans) {
            String predicate = InfinoQueryTranslator.predicate(qb, p.cols(), p.table(), p.nowMillis());
            if (predicate == null) {
                return null;
            }
            each.add(predicate);
        }
        if (each.stream().distinct().count() == 1) {
            return each.get(0);
        }
        StringBuilder out = new StringBuilder("(CASE ").append(quoteIdent(SHARD_COLUMN));
        for (int i = 0; i < each.size(); i++) {
            out.append(" WHEN ").append(i).append(" THEN (").append(each.get(i)).append(")");
        }
        return out.append(" END)").toString();
    }

    /** The sources the metrics of {@code specs} read, at any depth, that some document gives several values, onto {@code out}. */
    private static void multiMetricSources(Collection<? extends Spec> specs, List<Source> out) {
        for (Spec s : specs) {
            if (s instanceof Metric m) {
                m.multiSources(out);
            } else if (s instanceof BucketSpec b) {
                multiMetricSources(b.metrics, out);
                if (b.child instanceof Siblings siblings) {
                    multiMetricSources(siblings.children, out);
                } else if (b.child != null) {
                    multiMetricSources(List.of(b.child), out);
                }
            }
        }
    }

    /**
     * {@code from} with each document's figures over the values of every
     * field of {@code fields} joined in (their count, a keyword's distinct
     * ones; for numbers and dates their least and greatest; for numbers their
     * sum), named by {@link Values#figure}: what a metric over such a field
     * reads, each document once wherever it is counted, as stock reads a
     * multi-valued field's values per document.
     */
    static String perDocument(String from, Collection<Source> fields) {
        StringBuilder select = new StringBuilder("SELECT " + METRIC_DOCS + ".*");
        StringBuilder joins = new StringBuilder();
        List<String> done = new ArrayList<>();
        for (Source s : fields) {
            Values v = s.values();
            if (done.contains(v.field())) {
                continue;
            }
            String alias = FIGURES + done.size();
            done.add(v.field());
            // Each figure's name and how it is computed over the document's values.
            Map<String, String> figures = new LinkedHashMap<>();
            figures.put(FIGURE_COUNT, (s.isString() ? "COUNT(DISTINCT " : "COUNT(") + FIGURE_VALUE + ")");
            if (s.isNumeric()) {
                figures.put(FIGURE_SUM, "SUM(" + FIGURE_VALUE + ")");
            }
            if (s.isNumeric() || s.isTimestamp()) {
                figures.put(FIGURE_MIN, "MIN(" + FIGURE_VALUE + ")");
                figures.put(FIGURE_MAX, "MAX(" + FIGURE_VALUE + ")");
            }
            List<String> computed = new ArrayList<>();
            for (Map.Entry<String, String> figure : figures.entrySet()) {
                computed.add(figure.getValue() + " AS " + v.figure(figure.getKey()));
                select.append(", ").append(alias).append('.').append(v.figure(figure.getKey()));
            }
            String values = "SELECT " + InfinoQueryTranslator.OSID + " AS " + FIGURE_DOC + ", unnest(" + v.each(s.expr()) + ") AS " + FIGURE_VALUE + " FROM "
                + quoteIdent(v.table());
            joins.append(" LEFT JOIN (SELECT " + FIGURE_DOC + ", " + String.join(", ", computed) + " FROM (" + values + ") GROUP BY " + FIGURE_DOC + ") " + alias)
                .append(" ON " + alias + "." + FIGURE_DOC + " = " + METRIC_DOCS + "." + InfinoQueryTranslator.OSID);
        }
        return "(" + select + " FROM " + from + " " + METRIC_DOCS + joins + ")";
    }

    /**
     * {@code from} read as one row per value of each source of {@code multi}
     * (fields some document gives several values): each such field's column
     * holds one of the document's values, its own or one of its list's, and a
     * document with several stands on a row per combination, so a GROUP BY
     * over the rows keys every value a document has.
     */
    static String valuesRelation(String from, Collection<Source> multi) {
        String relation = from;
        List<String> done = new ArrayList<>();
        for (Source s : multi) {
            if (done.contains(s.expr())) {
                continue;
            }
            relation = "(SELECT * EXCLUDE (" + s.expr() + "), unnest(" + s.values().each(s.expr()) + ") AS " + s.expr() + " FROM " + relation + " " + VALUE_LAYER
                + done.size() + ")";
            done.add(s.expr());
        }
        return relation;
    }

    /**
     * The GROUP BY of {@code keyCols} ({@code expression AS kN}, in order),
     * with each group's row count and {@code metrics} (empty for none), over
     * {@code from WHERE where}, then {@code having}. When a key reads a field
     * some document gives several values ({@code multi}), the keys are read
     * per value ({@link #valuesRelation}) and kept once per document, so a
     * document counts once in each bucket one of its values falls in, and the
     * metrics are read from the documents themselves, joined back to them by
     * id, each once per bucket, as stock counts a multi-valued field.
     */
    static String groupStatement(List<String> keyCols, String metrics, String from, String where, List<Source> multi, String having) {
        StringBuilder positions = new StringBuilder();
        for (int j = 1; j <= keyCols.size(); j++) {
            positions.append(j == 1 ? "" : ", ").append(j);
        }
        String tail = metrics.isEmpty() ? "" : ", " + metrics;
        if (multi.isEmpty()) {
            return "SELECT " + String.join(", ", keyCols) + ", COUNT(*) AS doc_count" + tail + " FROM " + from + " WHERE " + where + " GROUP BY " + positions + having;
        }
        List<String> keys = new ArrayList<>();
        for (int j = 0; j < keyCols.size(); j++) {
            keys.add(BUCKET_KEYS + ".k" + j + " AS k" + j);
        }
        String perDocument = "SELECT DISTINCT " + InfinoQueryTranslator.OSID + " AS " + BUCKET_DOC + ", " + String.join(", ", keyCols) + " FROM "
            + valuesRelation(from, multi) + " " + VALUE_ROWS + " WHERE " + where;
        String documents = metrics.isEmpty()
            ? ""
            : " JOIN " + from + " " + BUCKET_DOCS + " ON " + BUCKET_DOCS + "." + InfinoQueryTranslator.OSID + " = " + BUCKET_KEYS + "." + BUCKET_DOC;
        return "SELECT " + String.join(", ", keys) + ", COUNT(*) AS doc_count" + tail + " FROM (" + perDocument + ") " + BUCKET_KEYS + documents + " GROUP BY "
            + positions + having;
    }

    // ---- servability (shared with the hit path's plan) ----

    /** Plans every requested aggregation; throws {@link Unsupported} naming the first that cannot be served. */
    static void check(Collection<AggregationBuilder> builders, Plan plan) {
        int position = 0;
        for (AggregationBuilder b : builders) {
            Spec.of(b, plan.cols(), plan.table(), predicates(plan), plan.access(), position++, false, plan.nowMillis());
        }
    }

    /** An aggregation refused because it would read a field the user sees only masked. */
    static OpenSearchSecurityException masked(AggregationBuilder b, Source c) {
        return InfinoAccess.refused(
            "aggregation [" + b.getName() + "] reads " + c.label() + ", a field masked for this user; only terms over it are served, keyed by its hash"
        );
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

        /**
         * The spec for a builder at {@code index} among its siblings, or an
         * {@link Unsupported} reason; {@code shardOfMany} as the processor's,
         * {@code now} the time {@code now} means in the request;
         * {@code predicates} turns an aggregation's own query into a
         * predicate over the relation the aggregations read.
         */
        static Spec of(
            AggregationBuilder b,
            Map<String, Column> cols,
            String table,
            Function<QueryBuilder, String> predicates,
            InfinoAccess access,
            int index,
            boolean shardOfMany,
            long now
        ) {
            if (Metric.handles(b)) {
                if (!b.getSubAggregations().isEmpty()) {
                    throw new Unsupported("metric aggregation [" + b.getName() + "] cannot have sub-aggregations");
                }
                return Metric.of(b, cols, table, index, now);
            }
            if (Metric.filterOfMetrics(b)) {
                Metric folded = Metric.filter((FilterAggregationBuilder) b, cols, table, predicates, "m" + index, null, now);
                if (folded.foldable()) {
                    return folded;
                }
                // Otherwise the filter is a level of its own, its predicate a WHERE.
            }
            if (b instanceof TopHitsAggregationBuilder th) {
                return TopHits.of(th, cols, access, now);
            }
            List<Metric> metrics = new ArrayList<>();
            List<TopHits> hits = new ArrayList<>();
            List<BucketSpec> children = new ArrayList<>();
            int i = 0;
            for (AggregationBuilder sub : b.getSubAggregations()) {
                Spec s = of(sub, cols, table, predicates, access, i++, shardOfMany, now);
                if (s instanceof Metric m) {
                    metrics.add(m);
                } else if (s instanceof TopHits t) {
                    hits.add(t);
                } else {
                    children.add((BucketSpec) s);
                }
            }
            BucketSpec child = children.isEmpty() ? null : children.size() == 1 ? children.get(0) : new Siblings(children);
            Leaves leaves = new Leaves(metrics, hits);
            if (b instanceof TermsAggregationBuilder t) {
                return Grouped.terms(t, cols, table, leaves, child, shardOfMany, now);
            }
            if (b instanceof DateHistogramAggregationBuilder dh) {
                return Grouped.dateHistogram(dh, cols, table, leaves, child, now);
            }
            if (b instanceof HistogramAggregationBuilder h) {
                return Grouped.histogram(h, cols, table, leaves, child, now);
            }
            if (b instanceof FilterAggregationBuilder f) {
                return Explicit.filter(f, predicates, leaves, child);
            }
            if (b instanceof FiltersAggregationBuilder f) {
                return Explicit.filters(f, predicates, leaves, child);
            }
            if (b instanceof RangeAggregationBuilder r) {
                return Explicit.range(r, cols, table, leaves, child, now);
            }
            if (b instanceof DateRangeAggregationBuilder dr) {
                return Explicit.dateRange(dr, cols, table, leaves, child, now);
            }
            if (b instanceof SignificantTermsAggregationBuilder st) {
                return Significant.of(st, cols, table, leaves, child, now);
            }
            if (b instanceof CompositeAggregationBuilder c) {
                return Composite.of(c, cols, table, leaves, child, now);
            }
            throw new Unsupported("aggregation [" + b.getName() + "] of type [" + b.getType() + "] is not served");
        }

        /**
         * What the aggregation reads: its field's column, or its script as a
         * column expression (a value script, one given beside a field, reads
         * that field as {@code _value}); a script's clock reads {@code now}. A
         * field the index does not map is read as no value in every document,
         * as OpenSearch reads an unmapped field, so a search over several
         * indices answers from the ones that map it; a field the index maps and
         * the table has no column for is refused, since its values exist and
         * cannot be read.
         */
        static Source sourceOf(ValuesSourceAggregationBuilder<?> b, Map<String, Column> cols, String table, long now) {
            Column c = null;
            if (b.field() != null) {
                c = InfinoSearch.columnFor(cols, b.field());
                if (c == null) {
                    if (!InfinoNative.maps(table, b.field())) {
                        return withMissing(b, Source.unmapped(b.field(), unmappedType(b)), now);
                    }
                    throw new Unsupported("aggregation [" + b.getName() + "] names field [" + b.field() + "], which the table has no column for");
                }
            }
            if (b.script() != null) {
                // A script reads values themselves, so it sees no masked column, the value script's included.
                Column valueColumn = c != null && c.masked() ? null : c;
                Sql sql = InfinoScriptTranslator.translate(b.script(), InfinoAccess.scriptColumns(cols), valueColumn, now);
                return withMissing(b, new Source(sql.expr(), sql.type(), "script [" + b.script().getIdOrCode() + "]", null), now);
            }
            if (c == null) {
                throw new Unsupported("aggregation [" + b.getName() + "] needs a field or a script");
            }
            return withMissing(b, Source.of(c, cols, table), now);
        }

        /**
         * {@code source} with the aggregation's {@code missing} value standing
         * in for a document that gives none, as OpenSearch reads it: a terms
         * bucket of its own, a value a metric counts. A field some document
         * gives several values, or one masked for the user, is refused with it.
         */
        static Source withMissing(ValuesSourceAggregationBuilder<?> b, Source source, long now) {
            if (b.missing() == null) {
                return source;
            }
            if (source.multi()) {
                throw new Unsupported("aggregation [" + b.getName() + "] has [missing] over " + source.label() + ", which documents give several values");
            }
            if (source.masked()) {
                throw new Unsupported("aggregation [" + b.getName() + "] has [missing] over " + source.label() + ", a field masked for this user");
            }
            String literal = InfinoQueryTranslator.literalForType(source.type(), b.missing(), now);
            if (literal == null) {
                throw new Unsupported("aggregation [" + b.getName() + "] has [missing: " + b.missing() + "], which " + source.label() + " cannot hold");
            }
            // No column: the fast paths that read a column's own values would skip the stand-in.
            return new Source("COALESCE(" + source.expr() + ", " + literal + ")", source.type(), source.label(), null, null, source.dateFormat(), null, source.onlyWhere());
        }

        /** The type an aggregation reads an unmapped field as: what its values would have been. */
        static SqlType unmappedType(AggregationBuilder b) {
            if (b instanceof DateHistogramAggregationBuilder || b instanceof DateRangeAggregationBuilder) {
                return SqlType.DATE;
            }
            if (b instanceof TermsAggregationBuilder || b instanceof SignificantTermsAggregationBuilder) {
                return SqlType.STRING;
            }
            return SqlType.FLOAT;
        }
    }

    /**
     * What an aggregation reads, as a column expression with its type;
     * {@code label} names it in a refusal, {@code mask} is how a key of it
     * leaves the node when the field is masked for the user (null when not),
     * {@code column} the column it reads as is (null for a script, or for a
     * field read under a filter), {@code dateFormat} is the format a date
     * field's mapping declares (null for the default one, and for anything
     * but a mapped field). {@code values} is where a field keeps its values
     * for the documents that give it several (null when none does), and
     * {@code onlyWhere} the predicate a metric under a filter reads such a
     * field's values where.
     */
    record Source(
        String expr,
        SqlType type,
        String label,
        UnaryOperator<String> mask,
        String column,
        String dateFormat,
        Values values,
        String onlyWhere
    ) {
        Source(String expr, SqlType type, String label, UnaryOperator<String> mask) {
            this(expr, type, label, mask, null, null, null, null);
        }

        /** A field's column, and the list column it keeps several values in when a document gives it some. */
        static Source of(Column c, Map<String, Column> cols, String table) {
            Column list = c.isList() ? c : InfinoQueryTranslator.listOf(cols, c);
            Values values = list == null ? null : new Values(c.name(), quoteIdent(list.name()), c.isList(), table);
            return new Source(
                quoteIdent(c.name()),
                InfinoScriptTranslator.typeOf(c.element()),
                "[" + c.name() + "]",
                c.mask(),
                c.name(),
                c.dateFormat(),
                values,
                null
            );
        }

        /** Whether some document gives the field several values. */
        boolean multi() {
            return values != null;
        }

        /**
         * Where any value of this source passes {@code test}, a predicate over
         * an expression of its type: the column's own value, or, for a document
         * that gives the field several, any of them.
         */
        String anyValue(Function<String, String> test) {
            if (values == null) {
                return test.apply(expr);
            }
            return InfinoQueryTranslator.anyValue(values.table(), values.listOnly() ? null : expr, values.list(), test);
        }

        /** The documents with no value of this source: neither the field's own column nor its list holds one. */
        String noValue() {
            if (values == null) {
                return expr + " IS NULL";
            }
            return "(" + (values.listOnly() ? "" : expr + " IS NULL AND ") + values.list() + " IS NULL)";
        }

        /** A field the index does not map: no value in any document, of the type the aggregation reads. */
        static Source unmapped(String field, SqlType type) {
            String nothing = switch (type) {
                case DATE -> "arrow_cast(NULL, '" + UNMAPPED_DATE_TYPE + "')";
                case STRING -> "CAST(NULL AS VARCHAR)";
                case BOOL -> "CAST(NULL AS BOOLEAN)";
                case INT -> "CAST(NULL AS BIGINT)";
                default -> "CAST(NULL AS DOUBLE)";
            };
            return new Source(nothing, type, "[" + field + "] (unmapped)", null);
        }

        /** This source where {@code predicate} holds and no value elsewhere: what a metric under a filter reads. */
        Source where(String predicate) {
            if (values != null) {
                // Its values are read from each document's own figures ({@link Values}), under the predicate.
                String both = onlyWhere == null ? predicate : "(" + onlyWhere + ") AND (" + predicate + ")";
                return new Source(expr, type, label, mask, null, dateFormat, values, both);
            }
            return new Source("CASE WHEN " + predicate + " THEN (" + expr + ") END", type, label, mask, null, dateFormat, null, null);
        }

        boolean masked() {
            return mask != null;
        }

        /** A key of this source as the user may see it. */
        String shown(Object key) {
            return mask == null ? key.toString() : mask.apply(key.toString());
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

    /**
     * Where a field keeps its values for the documents that give it several:
     * {@code field} its name, {@code list} its list column (quoted),
     * {@code listOnly} whether the field has no column of its own (one the
     * mapping does not name, which the engine made a list), and the
     * {@code table} a subquery over the list reads. A metric reads such a
     * field from each document's own figures over its values, which
     * {@link #perDocument} joins in under {@link #figure}.
     */
    record Values(String field, String list, boolean listOnly, String table) {
        /** A document's figure over its values, as the column {@link #perDocument} names it. */
        String figure(String what) {
            return quoteIdent(field + PER_DOCUMENT + what);
        }

        /** Every value of a document, its own or its list's, as one list. */
        String each(String own) {
            return listOnly ? list : "CASE WHEN " + list + " IS NULL THEN make_array(" + own + ") ELSE " + list + " END";
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
        PERCENTILE_RANKS,
        /** A filter whose sub-aggregations are all metrics: its count and theirs, where its predicate holds. */
        FILTER
    }

    /**
     * A metric over one column: the SQL aggregate columns it needs and the
     * result it builds. A {@code filter} aggregation whose sub-aggregations
     * are all metrics is one too: it needs no level of its own, since its
     * count and its metrics are conditional aggregates in its parent's
     * statement, so any number of them sit beside each other under one
     * bucket (as TSVB's filter ratios do).
     */
    static final class Metric extends Spec {
        final Source source;
        final String alias;
        final MetricKind kind;
        final double[] percents;
        final boolean keyed;
        final double compression;
        /** For a filter: where it holds, and the metrics under it. */
        final String predicate;
        final List<Metric> inner;

        private Metric(AggregationBuilder b, Source source, String alias, MetricKind kind, double[] percents, boolean keyed, double compression) {
            this(b, source, alias, kind, percents, keyed, compression, null, List.of());
        }

        private Metric(
            AggregationBuilder b,
            Source source,
            String alias,
            MetricKind kind,
            double[] percents,
            boolean keyed,
            double compression,
            String predicate,
            List<Metric> inner
        ) {
            super(b);
            this.source = source;
            this.alias = alias;
            this.kind = kind;
            this.percents = percents;
            this.keyed = keyed;
            this.compression = compression;
            this.predicate = predicate;
            this.inner = inner;
        }

        /** Whether {@code b} is a filter served as a metric: every aggregation under it a metric, or such a filter. */
        static boolean filterOfMetrics(AggregationBuilder b) {
            if (!(b instanceof FilterAggregationBuilder)) {
                return false;
            }
            for (AggregationBuilder sub : b.getSubAggregations()) {
                if (!(handles(sub) && sub.getSubAggregations().isEmpty()) && !filterOfMetrics(sub)) {
                    return false;
                }
            }
            return true;
        }

        /**
         * A filter of metrics ({@link #filterOfMetrics}) under {@code where}
         * (null at the top): its predicate joins {@code where}, and each
         * metric under it reads its value only where both hold.
         */
        static Metric filter(
            FilterAggregationBuilder f,
            Map<String, Column> cols,
            String table,
            Function<QueryBuilder, String> predicates,
            String alias,
            String where,
            long now
        ) {
            String p = filterPredicate(f, predicates);
            String holds = where == null ? "(" + p + ")" : "(" + where + " AND (" + p + "))";
            List<Metric> inner = new ArrayList<>();
            int i = 0;
            for (AggregationBuilder sub : f.getSubAggregations()) {
                String subAlias = alias + "_" + i++;
                inner.add(
                    sub instanceof FilterAggregationBuilder g
                        ? filter(g, cols, table, predicates, subAlias, holds, now)
                        : of(sub, cols, table, subAlias, holds, now)
                );
            }
            return new Metric(f, null, alias, MetricKind.FILTER, null, false, 0, holds, inner);
        }

        /**
         * Whether this filter's predicates can sit inside its parent's
         * aggregate expressions: none reads the token index, whose subquery
         * the engine plans only in a WHERE clause.
         */
        boolean foldable() {
            if (predicate != null && InfinoQueryTranslator.readsTextIndex(predicate)) {
                return false;
            }
            for (Metric m : inner) {
                if (!m.foldable()) {
                    return false;
                }
            }
            return true;
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

        static Metric of(AggregationBuilder b, Map<String, Column> cols, String table, int index, long now) {
            return of(b, cols, table, "m" + index, null, now);
        }

        /**
         * The metric {@code b}, its columns named after {@code alias}, reading
         * its value only where {@code where} holds (null: everywhere); a
         * script's clock reads {@code now}.
         */
        static Metric of(AggregationBuilder b, Map<String, Column> cols, String table, String alias, String where, long now) {
            Source source = sourceOf((ValuesSourceAggregationBuilder<?>) b, cols, table, now);
            if (source.masked()) {
                throw masked(b, source); // a count, sum or bound would read the values themselves
            }
            Source c = where == null ? source : source.where(where);
            // Over a field some document gives several values, a metric reads each document's
            // figures over them (its sum, count, least, greatest), which serve these kinds alone.
            boolean figured = b instanceof ValueCountAggregationBuilder
                || b instanceof SumAggregationBuilder
                || b instanceof AvgAggregationBuilder
                || b instanceof MinAggregationBuilder
                || b instanceof MaxAggregationBuilder;
            if (c.multi() && !figured) {
                throw new Unsupported("aggregation [" + b.getName() + "] of type [" + b.getType() + "] over " + c.label() + ", which documents give several values, is not served");
            }
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
            if (kind == MetricKind.FILTER) {
                List<String> cols = new ArrayList<>();
                cols.add("SUM(CASE WHEN " + predicate + " THEN 1 ELSE 0 END) AS " + alias + "_fc");
                for (Metric m : inner) {
                    cols.add(m.columns());
                }
                return String.join(", ", cols);
            }
            if (source.multi()) {
                return switch (kind) {
                    case VALUE_COUNT -> "SUM(" + figure(FIGURE_COUNT) + ") AS " + alias + "_c";
                    case SUM -> "SUM(" + figure(FIGURE_SUM) + ") AS " + alias + "_s";
                    case AVG -> "SUM(" + figure(FIGURE_SUM) + ") AS " + alias + "_s, SUM(" + figure(FIGURE_COUNT) + ") AS " + alias + "_c";
                    case MIN -> "MIN(" + figure(FIGURE_MIN) + ") AS " + alias + "_v";
                    case MAX -> "MAX(" + figure(FIGURE_MAX) + ") AS " + alias + "_v";
                    default -> throw new IllegalStateException("a " + kind + " metric is not read from figures");
                };
            }
            String f = "(" + source.expr() + ")";
            return switch (kind) {
                case FILTER -> throw new IllegalStateException("a filter's columns are its count's and its metrics'");
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

        /** A document's figure {@code what} over this metric's values, where its filter holds when it is under one. */
        private String figure(String what) {
            String figure = source.values().figure(what);
            return source.onlyWhere() == null ? figure : "CASE WHEN " + source.onlyWhere() + " THEN " + figure + " END";
        }

        /** The sources this metric (or any under it) reads that some document gives several values, onto {@code out}. */
        void multiSources(List<Source> out) {
            if (kind == MetricKind.FILTER) {
                for (Metric m : inner) {
                    m.multiSources(out);
                }
            } else if (source != null && source.multi()) {
                out.add(source);
            }
        }

        /**
         * What a terms order by this metric sorts on, over the grouped row's
         * columns, or null when the metric is no single value of them. An
         * average is its sum over its count, null for a bucket with no values,
         * which sorts as stock's NaN does: last ascending, first descending.
         */
        String orderExpr() {
            return switch (kind) {
                case VALUE_COUNT -> alias + "_c";
                case SUM -> alias + "_s";
                case MIN, MAX -> alias + "_v";
                case CARDINALITY -> alias + "_d";
                case AVG -> "(CAST(" + alias + "_s AS DOUBLE) / NULLIF(" + alias + "_c, 0))";
                case PERCENTILES, PERCENTILE_RANKS, FILTER -> null;
            };
        }

        InternalAggregation build(Map<String, Object> row) {
            String name = builder.getName();
            Map<String, Object> meta = metadataOf(builder);
            return switch (kind) {
                case VALUE_COUNT -> new InternalValueCount(name, asLong(row.get(alias + "_c")), meta);
                case SUM -> new InternalSum(name, asDouble(row.get(alias + "_s"), 0.0), DocValueFormat.RAW, meta);
                case AVG -> new InternalAvg(name, asDouble(row.get(alias + "_s"), 0.0), asLong(row.get(alias + "_c")), DocValueFormat.RAW, meta);
                case MIN -> new InternalMin(name, asDouble(row.get(alias + "_v"), Double.POSITIVE_INFINITY), DocValueFormat.RAW, meta);
                case MAX -> new InternalMax(name, asDouble(row.get(alias + "_v"), Double.NEGATIVE_INFINITY), DocValueFormat.RAW, meta);
                case CARDINALITY -> InfinoAggregationWire.cardinality(name, meta, asLong(row.get(alias + "_d")));
                case PERCENTILES -> percentiles(row);
                case PERCENTILE_RANKS -> percentileRanks(row);
                case FILTER -> {
                    List<InternalAggregation> subs = new ArrayList<>();
                    for (Metric m : inner) {
                        subs.add(m.build(row));
                    }
                    yield InfinoAggregationWire.filter(name, meta, asLong(row.get(alias + "_fc")), InternalAggregations.from(subs));
                }
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
            return new InternalTDigestPercentiles(builder.getName(), percents, state, keyed, DocValueFormat.RAW, metadataOf(builder));
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
            return new InternalTDigestPercentileRanks(builder.getName(), values, state, keyed, DocValueFormat.RAW, metadataOf(builder));
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
        final InfinoAccess access;

        private TopHits(
            TopHitsAggregationBuilder b,
            List<SortSpec> sorts,
            FetchSourceContext source,
            List<FieldAndFormat> fields,
            Map<String, Column> cols,
            InfinoAccess access
        ) {
            super(b);
            this.from = Math.max(0, b.from());
            this.size = b.size() < 0 ? DEFAULT_TOP_HITS_SIZE : b.size();
            this.sorts = sorts;
            this.source = source;
            this.fields = fields;
            this.cols = cols;
            this.access = access;
        }

        static TopHits of(TopHitsAggregationBuilder b, Map<String, Column> cols, InfinoAccess access, long now) {
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
                sorts = InfinoSearch.sorts(b.sorts(), cols, now);
            } catch (IllegalArgumentException e) {
                throw new Unsupported("aggregation [" + b.getName() + "] " + e.getMessage());
            }
            // docvalue_fields and fields are read from the hit's source; stored_fields adds nothing the source lacks.
            return new TopHits(b, sorts, b.fetchSource(), InfinoSearch.requestedFields(b.docValueFields(), b.fetchFields()), cols, access);
        }

        /** The columns a hit row carries beyond the id and the source. */
        List<String> rowColumns() {
            List<String> out = new ArrayList<>();
            for (SortSpec s : sorts) {
                if (s.name().equals(InfinoQueryTranslator.OSID)) {
                    continue; // the id is selected already
                }
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
            return InfinoSearch.topHits((TopHitsAggregationBuilder) builder, from, size, sorts, source, fields, cols, access, rows, bucketCount, index);
        }

        InternalAggregation empty() {
            return build(List.of(), 0, null);
        }
    }

    /** What sits under a bucket besides a nested bucket aggregation: its metrics and its top hits. */
    record Leaves(List<Metric> metrics, List<TopHits> hits) {
        static final Leaves NONE = new Leaves(List.of(), List.of());
    }

    /**
     * A bucket aggregation: the metrics and top hits under it and its nested
     * bucket aggregation, if any; several nested ones are one {@link Siblings}.
     */
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
         * hits (one per top_hits spec, in order) and the nested aggregations given.
         */
        InternalAggregations subs(Map<String, Object> row, List<InternalAggregation> topHits, List<InternalAggregation> nested) {
            List<InternalAggregation> out = new ArrayList<>();
            for (Metric m : metrics) {
                out.add(m.build(row));
            }
            out.addAll(topHits);
            out.addAll(nested);
            return out.isEmpty() ? InternalAggregations.EMPTY : InternalAggregations.from(out);
        }

        /** The sub-aggregations of an empty bucket. */
        InternalAggregations emptySubs() {
            return subs(Map.of(), emptyHits(), emptyChildren(child));
        }

        /** The nested aggregations of an empty bucket under {@code child}. */
        static List<InternalAggregation> emptyChildren(BucketSpec child) {
            if (child == null) {
                return List.of();
            }
            if (child instanceof Siblings s) {
                List<InternalAggregation> out = new ArrayList<>();
                for (BucketSpec c : s.children) {
                    out.add(c.empty());
                }
                return out;
            }
            return List.of(child.empty());
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

    /**
     * Several bucket aggregations under one bucket (TSVB's table puts one
     * filter per series under its pivot): no aggregation of its own, but
     * where a bucket's nested aggregation goes, each of them evaluated over
     * that bucket's rows ({@link #evalChildren}) and placed beside the others.
     */
    static final class Siblings extends BucketSpec {
        final List<BucketSpec> children;

        Siblings(List<BucketSpec> children) {
            super(children.get(0).builder, Leaves.NONE, null);
            this.children = List.copyOf(children);
        }

        @Override
        InternalAggregation empty() {
            throw new IllegalStateException("siblings have no aggregation of their own; see emptyChildren");
        }
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
            // A level's top hits are a window over the documents partitioned by its key,
            // which a document with several values would stand in once per value.
            if (source.multi() && !leaves.hits().isEmpty()) {
                throw new Unsupported("top hits under aggregation [" + b.getName() + "], which buckets " + source.label() + ", a field documents give several values, are not served");
            }
        }

        /**
         * A terms level. It keeps its shard size of groups ({@link #shardSize}),
         * and on one shard of several its groups at the shard minimum count, as
         * a stock shard does, so the reduce ranks from more than each index's
         * or shard's final top.
         */
        static Grouped terms(
            TermsAggregationBuilder t,
            Map<String, Column> cols,
            String table,
            Leaves leaves,
            BucketSpec child,
            boolean shardOfMany,
            long now
        ) {
            Source c = sourceOf(t, cols, table, now);
            if (t.includeExclude() != null) {
                // Read as Lucene reads them (its regular expressions, its partitions), which the table does not.
                throw new Unsupported("aggregation [" + t.getName() + "] has include/exclude, which is not served");
            }
            if (t.size() > MAX_TERMS_SIZE) {
                throw new Unsupported("aggregation [" + t.getName() + "] asks for " + t.size() + " buckets; at most " + MAX_TERMS_SIZE + " are served");
            }
            KeyKind kind = kindOf(c);
            if (c.masked() && kind != KeyKind.STR && kind != KeyKind.BOOL) {
                throw masked(t, c); // a numeric or date key would be the value itself
            }
            String order = orderClause(t.order(), leaves.metrics());
            if (order == null) {
                throw new Unsupported("aggregation [" + t.getName() + "] has an order the table cannot sort by: " + t.order());
            }
            long minDocCount = shardOfMany ? Math.min(t.shardMinDocCount(), t.minDocCount()) : t.minDocCount();
            return new Grouped(t, leaves, child, c, c.expr(), kind, true, shardSize(t), minDocCount, order, null, null);
        }

        /**
         * The buckets a terms level takes from each index or shard, as a stock
         * shard does: the request's shard size, else past the requested size by
         * stock's heuristic unless the order is by key (a key order merges
         * exactly), never fewer than the requested size nor more than are
         * served. The reduce keeps the requested size, and the buckets it drops
         * are what {@code sum_other_doc_count} and
         * {@code doc_count_error_upper_bound} are counted from, as they are over
         * stock's shards.
         */
        static int shardSize(TermsAggregationBuilder t) {
            int shard = t.shardSize();
            if (shard < 0) {
                shard = InternalOrder.isKeyOrder(t.order()) ? t.size() : BucketUtils.suggestShardSideQueueSize(t.size());
            }
            return Math.min(MAX_TERMS_SIZE, Math.max(shard, t.size()));
        }

        static Grouped dateHistogram(
            DateHistogramAggregationBuilder dh,
            Map<String, Column> cols,
            String table,
            Leaves leaves,
            BucketSpec child,
            long now
        ) {
            Source c = sourceOf(dh, cols, table, now);
            if (c.masked()) {
                throw masked(dh, c);
            }
            ZoneId tz = dh.timeZone() == null ? ZoneOffset.UTC : dh.timeZone();
            DateExpr expr = dateExpr(dh.getName(), c, tz, dh.getFixedInterval(), dh.getCalendarInterval(), dh.offset());
            DocValueFormat format = dateFormat(dh.format(), c, tz);
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

        static Grouped histogram(HistogramAggregationBuilder h, Map<String, Column> cols, String table, Leaves leaves, BucketSpec child, long now) {
            Source c = sourceOf(h, cols, table, now);
            if (c.masked()) {
                throw masked(h, c);
            }
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
            boolean zoned = !ZoneOffset.UTC.equals(tz) && !"UTC".equals(tz.getId());
            String zone = "'Timestamp(Millisecond, Some(\"" + tz.getId() + "\"))'";
            if (zoned) {
                // Bucket boundaries fall on local time in the request's zone; the
                // epoch value of each boundary is unchanged.
                source = "arrow_cast(" + source + ", " + zone + ")";
            }
            String keyExpr;
            DateIntervalWrapper wrapper = new DateIntervalWrapper();
            if (fixed != null) {
                long ms = fixed.estimateMillis();
                if (ms <= 0) {
                    throw new Unsupported("aggregation [" + agg + "] has an interval of no length");
                }
                String origin = offset == 0 ? "" : ", TIMESTAMP '1970-01-01T00:00:00Z' + INTERVAL '" + offset + " milliseconds'";
                if (zoned) {
                    // date_bin counts from the epoch in UTC whatever the zone; binning the
                    // wall-clock time and reading the result back in the zone puts the
                    // boundaries on local time, where the request (and the empty-bucket
                    // rounding) puts them.
                    keyExpr = "arrow_cast(date_bin(INTERVAL '" + ms + " milliseconds', to_local_time(" + source + ")" + origin + "), " + zone + ")";
                } else {
                    keyExpr = "date_bin(INTERVAL '" + ms + " milliseconds', " + source + origin + ")";
                }
                wrapper.fixedInterval(fixed);
            } else if (calendar != null) {
                String unit = calendarUnit(calendar);
                if (unit == null) {
                    throw new Unsupported("aggregation [" + agg + "] has calendar interval [" + calendar + "]; one unit at a time is served");
                }
                if (offset == 0) {
                    keyExpr = "date_trunc('" + unit + "', " + source + ")";
                } else {
                    // As stock's offset rounding: the unit's start, shifted by the offset, holds the date.
                    String shift = "INTERVAL '" + offset + " milliseconds'";
                    keyExpr = "(date_trunc('" + unit + "', " + source + " - " + shift + ") + " + shift + ")";
                }
                wrapper.calendarInterval(calendar);
            } else {
                throw new Unsupported("aggregation [" + agg + "] names no interval");
            }
            return new DateExpr(keyExpr, wrapper.createRounding(tz, offset));
        }

        /** How a date of {@code c} reads and prints: the request's format, else the one its field's mapping declares, else the default. */
        static String dateFormatName(String format, Source c) {
            return format != null ? format : c.dateFormat() != null ? c.dateFormat() : DEFAULT_DATE_FORMAT;
        }

        /** How a date key of {@code c} prints ({@link #dateFormatName}). */
        static DocValueFormat dateFormat(String format, Source c, ZoneId tz) {
            return new DocValueFormat.DateTime(DateFormatter.forPattern(dateFormatName(format, c)), tz, DateFieldMapper.Resolution.MILLISECONDS);
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

        /**
         * The rows of this level's bucket keyed {@code key}, as a row returned
         * it, for restricting a nested level to a parent's bucket. A date
         * bucket is its rounding's range over the date itself, from its key
         * to the next: a zoned histogram's key ends in a cast from wall-clock
         * time that the engine's planner rewrites wrongly inside a
         * comparison, and a range on the column is one it prunes files by.
         */
        String rowPredicate(Object key) {
            if (key == null) {
                return keyExpr + " IS NULL";
            }
            if (date != null) {
                return dateBucket(asLong(key), "(" + source.expr() + ")");
            }
            return keyExpr + " = " + keyLiteral(keyKind, key);
        }

        /**
         * The documents in this level's bucket keyed {@code key}: as
         * {@link #rowPredicate} for a field each document gives one value,
         * and a document any of whose values falls in the bucket for a field
         * some give several; for the aggregations nested under the bucket,
         * which read the documents themselves.
         */
        String docPredicate(Object key) {
            if (!source.multi()) {
                return rowPredicate(key);
            }
            if (key == null) {
                return source.noValue();
            }
            if (date != null) {
                long start = asLong(key);
                return source.anyValue(value -> dateBucket(start, value));
            }
            if (hist != null) {
                double low = asDouble(key, 0.0);
                return source.anyValue(value -> "(" + value + " >= " + low + " AND " + value + " < " + (low + hist.interval()) + ")");
            }
            String literal = keyLiteral(keyKind, key);
            return source.anyValue(value -> value + " = " + literal);
        }

        /** A date bucket from {@code start} to the rounding's next value, over the date {@code at}. */
        private String dateBucket(long start, String at) {
            long end = date.rounding().prepareForUnknown().nextRoundingValue(start);
            return "(" + at + " >= " + keyLiteral(KeyKind.DATE, start) + " AND " + at + " < " + keyLiteral(KeyKind.DATE, end) + ")";
        }

        /** A key of {@code kind}, as a row returned it, as an SQL literal. */
        static String keyLiteral(KeyKind kind, Object key) {
            return switch (kind) {
                case STR -> literal(key.toString());
                case BOOL -> String.valueOf(asBoolean(key));
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
            // As stock keys a boolean field: 1 and 0, printed true and false. A
            // masked one is keyed by its hash, as any masked field.
            boolean booleanKeys = keyKind == KeyKind.BOOL && !source.masked();
            for (Map<String, Object> row : rows) {
                Object key = row.get(keyAlias);
                if (key == null) {
                    continue; // a terms aggregation has no bucket for the missing value
                }
                long docCount = asLong(row.get("doc_count"));
                sumTop += docCount;
                InternalAggregations subs = subsOf.apply(row);
                if (booleanKeys) {
                    lBuckets.add(new LongTerms.Bucket(asBoolean(key) ? 1 : 0, docCount, subs, false, 0L, DocValueFormat.BOOLEAN));
                    continue;
                }
                switch (keyKind) {
                    case STR, BOOL -> sBuckets.add(new StringTerms.Bucket(new BytesRef(source.shown(key)), docCount, subs, false, 0L, DocValueFormat.RAW));
                    case LONG, DATE -> lBuckets.add(new LongTerms.Bucket(asLong(key), docCount, subs, false, 0L, DocValueFormat.RAW));
                    case DOUBLE -> dBuckets.add(new InfinoAggregationWire.DoubleBucket(asDouble(key, 0.0), docCount, subs));
                }
            }
            // The rows with a value under this parent (the window statement counts
            // them as keyed_total), less those in the buckets returned; the rows
            // without the field are in no bucket, as stock counts it. The rows
            // without one never reach this level, so none here means no row has
            // a value (a field no document holds, or one the index does not map).
            long keyed = rows.isEmpty() ? 0 : rows.get(0).get("keyed_total") == null ? parentTotal : asLong(rows.get(0).get("keyed_total"));
            long otherDocCount = Math.max(0, keyed - sumTop);
            // This level took `size` buckets from the index or shard, its shard
            // size (see shardSize): the reduce reads a result with fewer buckets
            // than this as holding every group, so with no error in its counts.
            TermsAggregator.BucketCountThresholds thresholds = new TermsAggregator.BucketCountThresholds(t.minDocCount(), t.shardMinDocCount(), t.size(), size);
            if (booleanKeys) {
                return new LongTerms(t.getName(), t.order(), t.order(), metadataOf(t), DocValueFormat.BOOLEAN, size, false, otherDocCount, lBuckets, 0L, thresholds);
            }
            return switch (keyKind) {
                case STR, BOOL -> new StringTerms(t.getName(), t.order(), t.order(), metadataOf(t), DocValueFormat.RAW, size, false, otherDocCount, sBuckets, 0L, thresholds);
                case LONG, DATE -> new LongTerms(t.getName(), t.order(), t.order(), metadataOf(t), DocValueFormat.RAW, size, false, otherDocCount, lBuckets, 0L, thresholds);
                // The double bucket class is not public in every release built against; the object comes through its stream form.
                case DOUBLE -> InfinoAggregationWire.doubleTerms(t.getName(), metadataOf(t), t.order(), t.size(), size, t.minDocCount(), otherDocCount, dBuckets);
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
                metadataOf(builder),
                buckets,
                minDocCount,
                date.offset(),
                // The reduce adds the offset to each empty bucket itself, as stock's aggregator expects.
                date.rounding().withoutOffset(),
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
            return new InternalHistogram(h.getName(), buckets, h.order(), minDocCount, empty, DocValueFormat.RAW, hist.keyed(), metadataOf(h));
        }
    }

    /** One explicit bucket: its key, its predicate, and its bounds for a range. */
    record ExplicitBucket(String key, String predicate, double from, double to) {}

    /** What an explicit level is. */
    enum ExplicitKind {
        /** One bucket, the documents a filter matches, answered as a single-bucket aggregation. */
        FILTER,
        FILTERS,
        RANGE,
        DATE_RANGE
    }

    /** An explicit bucket level: filter, filters, range or date_range, each bucket its own predicate. */
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

        /** A {@code filter} aggregation: the one bucket of the documents its query matches. */
        static Explicit filter(FilterAggregationBuilder f, Function<QueryBuilder, String> predicates, Leaves leaves, BucketSpec child) {
            String p = filterPredicate(f, predicates);
            List<ExplicitBucket> buckets = List.of(new ExplicitBucket(f.getName(), p, Double.NaN, Double.NaN));
            return new Explicit(f, leaves, child, ExplicitKind.FILTER, buckets, false, DocValueFormat.RAW);
        }

        static Explicit filters(FiltersAggregationBuilder f, Function<QueryBuilder, String> predicates, Leaves leaves, BucketSpec child) {
            List<ExplicitBucket> buckets = new ArrayList<>();
            List<String> all = new ArrayList<>();
            for (FiltersAggregator.KeyedFilter kf : f.filters()) {
                String p = predicates.apply(kf.filter());
                if (p == null) {
                    throw new Unsupported("aggregation [" + f.getName() + "] has a filter the table cannot answer: " + kf.filter());
                }
                buckets.add(new ExplicitBucket(kf.key(), p, Double.NaN, Double.NaN));
                all.add("(" + p + ")");
            }
            if (f.otherBucket()) {
                // A document a filter cannot judge (its field missing) is in no named
                // bucket, so it is in the other one: unknown counts as false here.
                String other = all.isEmpty() ? InfinoQueryTranslator.ALL : "NOT COALESCE((" + String.join(" OR ", all) + "), FALSE)";
                buckets.add(new ExplicitBucket(f.otherBucketKey(), other, Double.NaN, Double.NaN));
            }
            return new Explicit(f, leaves, child, ExplicitKind.FILTERS, buckets, f.isKeyed(), DocValueFormat.RAW);
        }

        static Explicit range(RangeAggregationBuilder r, Map<String, Column> cols, String table, Leaves leaves, BucketSpec child, long now) {
            Source c = sourceOf(r, cols, table, now);
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

        static Explicit dateRange(DateRangeAggregationBuilder r, Map<String, Column> cols, String table, Leaves leaves, BucketSpec child, long now) {
            Source c = sourceOf(r, cols, table, now);
            if (!c.isTimestamp()) {
                throw new Unsupported("aggregation [" + r.getName() + "] needs a date value, " + c.label() + " is " + c.typeName());
            }
            ZoneId tz = r.timeZone() == null ? ZoneOffset.UTC : r.timeZone();
            String fmt = Grouped.dateFormatName(r.format(), c);
            DocValueFormat format = Grouped.dateFormat(r.format(), c, tz);
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

        /** {@code from <= f < to}, each side only when bounded; a document any of whose values is in range, for a field some give several. */
        private static String between(Source c, double from, double to) {
            List<String> bounds = new ArrayList<>();
            if (!Double.isInfinite(from) && !Double.isNaN(from)) {
                bounds.add(" >= " + bound(c, from));
            }
            if (!Double.isInfinite(to) && !Double.isNaN(to)) {
                bounds.add(" < " + bound(c, to));
            }
            if (bounds.isEmpty()) {
                return InfinoQueryTranslator.ALL;
            }
            return c.anyValue(value -> {
                List<String> parts = new ArrayList<>(bounds.size());
                for (String b : bounds) {
                    parts.add("(" + value + ")" + b);
                }
                return "(" + String.join(" AND ", parts) + ")";
            });
        }

        private static String bound(Source c, double v) {
            if (c.isTimestamp()) {
                return literal(Instant.ofEpochMilli((long) v).toString());
            }
            return v == Math.rint(v) && Math.abs(v) < 9.0e15 ? Long.toString((long) v) : Double.toString(v);
        }

        /** Build from one row per bucket (doc_count and metrics), each bucket's top hits and its nested aggregations. */
        InternalAggregation build(List<Map<String, Object>> rows, List<List<InternalAggregation>> topHits, List<List<InternalAggregation>> nested) {
            String name = builder.getName();
            Map<String, Object> meta = metadataOf(builder);
            switch (kind) {
                case FILTER -> {
                    return InfinoAggregationWire.filter(name, meta, asLong(rows.get(0).get("doc_count")), subs(rows.get(0), topHits.get(0), nested.get(0)));
                }
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
            List<List<InternalAggregation>> nested = new ArrayList<>();
            for (int i = 0; i < buckets.size(); i++) {
                rows.add(Map.of());
                topHits.add(emptyHits());
                nested.add(emptyChildren(child));
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

        static Significant of(SignificantTermsAggregationBuilder st, Map<String, Column> cols, String table, Leaves leaves, BucketSpec child, long now) {
            Source c = sourceOf(st, cols, table, now);
            if (c.multi()) {
                throw new Unsupported("aggregation [" + st.getName() + "] over " + c.label() + ", which documents give several values, is not served");
            }
            KeyKind kind = Grouped.kindOf(c);
            if (kind == KeyKind.DOUBLE) {
                throw new Unsupported("aggregation [" + st.getName() + "] needs a keyword, boolean, integer or date value, " + c.label() + " is " + c.typeName());
            }
            if (c.masked() && kind != KeyKind.STR && kind != KeyKind.BOOL) {
                throw masked(st, c);
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
            String background = backgroundWhere(st, cols, table, now);
            SignificanceHeuristic heuristic = st.significanceHeuristic() == null ? new JLHScore() : st.significanceHeuristic();
            return new Significant(st, leaves, child, table, c, kind, background, candidates, thresholds.getMinDocCount(), heuristic, thresholds);
        }

        /** The background filter of {@code st} over {@code table} as a predicate; every row when it names none. */
        static String backgroundWhere(SignificantTermsAggregationBuilder st, Map<String, Column> cols, String table, long now) {
            if (st.backgroundFilter() == null) {
                return InfinoQueryTranslator.ALL;
            }
            String background = InfinoQueryTranslator.predicate(st.backgroundFilter(), cols, table, now);
            if (background == null) {
                throw new Unsupported("aggregation [" + st.getName() + "] has a background filter the table cannot answer: " + st.backgroundFilter());
            }
            return background;
        }

        String keyExpr() {
            return "(" + source.expr() + ")";
        }

        /** One candidate term with both of its counts. */
        record Candidate(Object key, long subsetDf, long supersetDf, Map<String, Object> row) {}

        /** Build from the candidates, each with its top hits and nested aggregations, and the two population sizes. */
        InternalAggregation build(
            List<Candidate> candidates,
            List<List<InternalAggregation>> topHits,
            List<List<InternalAggregation>> nested,
            long subsetSize,
            long supersetSize
        ) {
            String name = builder.getName();
            Map<String, Object> meta = metadataOf(builder);
            // The score is left for the reduction, which computes it from the four counts.
            if (keyKind == KeyKind.STR || keyKind == KeyKind.BOOL) {
                List<SignificantStringTerms.Bucket> out = new ArrayList<>();
                for (int i = 0; i < candidates.size(); i++) {
                    Candidate c = candidates.get(i);
                    InternalAggregations subs = subs(c.row(), topHits.get(i), nested.get(i));
                    out.add(new SignificantStringTerms.Bucket(new BytesRef(source.shown(c.key())), c.subsetDf(), subsetSize, c.supersetDf(), supersetSize, subs, DocValueFormat.RAW, 0.0));
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
    /** One source of a composite; {@code rounding} is a date histogram source's buckets, null for any other. */
    record CompositeSource(
        String name,
        Source source,
        String keyExpr,
        KeyKind keyKind,
        DocValueFormat format,
        boolean desc,
        boolean missingBucket,
        MissingOrder missingOrder,
        Rounding rounding
    ) {
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

        static Composite of(CompositeAggregationBuilder c, Map<String, Column> cols, String table, Leaves leaves, BucketSpec child, long now) {
            if (c.size() > MAX_TERMS_SIZE) {
                throw new Unsupported("aggregation [" + c.getName() + "] asks for " + c.size() + " buckets; at most " + MAX_TERMS_SIZE + " are served");
            }
            List<CompositeSource> sources = new ArrayList<>();
            for (CompositeValuesSourceBuilder<?> s : c.sources()) {
                sources.add(source(c.getName(), s, cols, table, now));
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
                    after[i] = v == null ? null : afterLiteral(c.getName(), s, v, now);
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

        private static CompositeSource source(String agg, CompositeValuesSourceBuilder<?> s, Map<String, Column> cols, String table, long now) {
            Column column = null;
            Source unmapped = null;
            if (s.field() != null) {
                column = InfinoSearch.columnFor(cols, s.field());
                if (column == null && !InfinoNative.maps(table, s.field())) {
                    // As for any aggregation: a field the index does not map has no value here.
                    SqlType type = s instanceof DateHistogramValuesSourceBuilder ? SqlType.DATE
                        : s instanceof HistogramValuesSourceBuilder ? SqlType.FLOAT : SqlType.STRING;
                    unmapped = Source.unmapped(s.field(), type);
                } else if (column == null) {
                    throw new Unsupported("aggregation [" + agg + "] source [" + s.name() + "] names field [" + s.field() + "], which the table has no column for");
                }
            }
            Source c;
            if (unmapped != null) {
                c = unmapped;
            } else if (s.script() != null) {
                // A script reads values themselves, so it sees no masked column, the value script's included.
                Column valueColumn = column != null && column.masked() ? null : column;
                Sql sql = InfinoScriptTranslator.translate(s.script(), InfinoAccess.scriptColumns(cols), valueColumn, now);
                c = new Source(sql.expr(), sql.type(), "script [" + s.script().getIdOrCode() + "]", null);
            } else if (column != null) {
                c = Source.of(column, cols, table);
            } else {
                throw new Unsupported("aggregation [" + agg + "] source [" + s.name() + "] names no field or script");
            }
            if (c.masked()) {
                // The after key would be the value itself, and the page after a hash cannot be found.
                throw InfinoAccess.refused("aggregation [" + agg + "] source [" + s.name() + "] reads " + c.label() + ", a field masked for this user; composite pages by the value and is not served over it");
            }
            boolean desc = s.order() == SortOrder.DESC;
            MissingOrder missing = s.missingOrder() == null ? MissingOrder.DEFAULT : s.missingOrder();
            if (c.multi() && !(s instanceof TermsValuesSourceBuilder)) {
                throw new Unsupported(
                    "aggregation [" + agg + "] source [" + s.name() + "] buckets " + c.label() + ", which documents give several values, by interval; only terms over it are served"
                );
            }
            if (s instanceof TermsValuesSourceBuilder) {
                KeyKind kind = Grouped.kindOf(c);
                DocValueFormat format = kind == KeyKind.DATE
                    ? Grouped.dateFormat(s.format(), c, ZoneOffset.UTC)
                    : kind == KeyKind.BOOL ? DocValueFormat.BOOLEAN : DocValueFormat.RAW;
                return new CompositeSource(s.name(), c, "(" + c.expr() + ")", kind, format, desc, s.missingBucket(), missing, null);
            }
            if (s instanceof DateHistogramValuesSourceBuilder dh) {
                ZoneId tz = dh.timeZone() == null ? ZoneOffset.UTC : dh.timeZone();
                DateHistogramInterval calendar = calendarOf(dh);
                DateHistogramInterval fixed = calendar == null ? dh.getIntervalAsFixed() : null;
                Grouped.DateExpr expr = Grouped.dateExpr(agg, c, tz, fixed, calendar, dh.offset());
                // As stock: the key is epoch milliseconds unless the source names a format.
                DocValueFormat format = s.format() == null ? DocValueFormat.RAW : Grouped.dateFormat(s.format(), c, tz);
                return new CompositeSource(s.name(), c, expr.keyExpr(), KeyKind.DATE, format, desc, s.missingBucket(), missing, expr.rounding());
            }
            if (s instanceof HistogramValuesSourceBuilder h) {
                String keyExpr = Grouped.histExpr(agg, c, h.interval(), 0);
                return new CompositeSource(s.name(), c, keyExpr, KeyKind.DOUBLE, DocValueFormat.RAW, desc, s.missingBucket(), missing, null);
            }
            throw new Unsupported("aggregation [" + agg + "] source [" + s.name() + "] of type [" + s.getClass().getSimpleName() + "] is not served");
        }

        /**
         * The calendar interval of a composite date source, or null when its interval is fixed.
         * The source does not say which kind the request wrote, and each getter throws for the
         * other kind; the calendar getter answers only for a calendar interval, so trying it first
         * picks the kind in the order stock's rounding does.
         */
        private static DateHistogramInterval calendarOf(DateHistogramValuesSourceBuilder dh) {
            try {
                return dh.getIntervalAsCalendar();
            } catch (IllegalStateException fixed) {
                return null;
            }
        }

        /** The after value of a source as an SQL literal, read as the source's key kind. */
        private static String afterLiteral(String agg, CompositeSource s, Object v, long now) {
            if (s.keyKind() == KeyKind.DATE && v instanceof String text) {
                Long ms = InfinoQueryTranslator.dateMathMillis(text, null, false, ZoneOffset.UTC, now);
                if (ms == null) {
                    throw new Unsupported("aggregation [" + agg + "] has an after key [" + text + "] for [" + s.name() + "] that is not a date");
                }
                return Grouped.keyLiteral(KeyKind.DATE, ms);
            }
            if ((s.keyKind() == KeyKind.LONG || s.keyKind() == KeyKind.DOUBLE || s.keyKind() == KeyKind.DATE) && !(v instanceof Number)) {
                throw new Unsupported("aggregation [" + agg + "] has an after key [" + v + "] for [" + s.name() + "] that is not a number");
            }
            if (s.keyKind() == KeyKind.BOOL && !(v instanceof Boolean || "true".equals(v) || "false".equals(v))) {
                throw new Unsupported("aggregation [" + agg + "] has an after key [" + v + "] for [" + s.name() + "] that is not a boolean");
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
                    case BOOL -> asBoolean(v) ? 1L : 0L;
                    case LONG, DATE -> asLong(v);
                    case DOUBLE -> asDouble(v, 0.0);
                };
            }
            return key;
        }

        /** Build from the page's rows in order, each with its top hits and nested aggregations. */
        InternalAggregation build(List<Map<String, Object>> rows, List<List<InternalAggregation>> topHits, List<List<InternalAggregation>> nested) {
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
            return InfinoAggregationWire.composite(builder.getName(), metadataOf(builder), size, names, formats, reverseMuls, missingOrders, buckets, afterKey);
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
            String expr = m.orderExpr();
            if (expr == null) {
                continue;
            }
            String name = m.builder.getName();
            if (order.equals(BucketOrder.aggregation(name, true))) {
                return expr + " ASC";
            }
            if (order.equals(BucketOrder.aggregation(name, false))) {
                return expr + " DESC";
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
        if (countsOnly(e)) {
            return buildExplicitCounts(e, explicitCounts(e, from, where, false));
        }
        List<Map<String, Object>> rows = new ArrayList<>();
        List<List<InternalAggregation>> topHits = new ArrayList<>();
        List<List<InternalAggregation>> nested = new ArrayList<>();
        for (ExplicitBucket b : e.buckets) {
            String w = "(" + where + ") AND (" + b.predicate() + ")";
            String select = "COUNT(*) AS doc_count" + (e.metrics.isEmpty() ? "" : ", " + metricColumns(e.metrics));
            Map<String, Object> row = oneRow("SELECT " + select + " FROM " + from + " WHERE " + w);
            long docCount = asLong(row.get("doc_count"));
            rows.add(row);
            topHits.add(bucketHits(e, from, w, docCount));
            nested.add(evalChildren(e.child, from, w, docCount));
        }
        return e.build(rows, topHits, nested);
    }

    /**
     * Whether {@code e} is range or date_range buckets that only count. A filters bucket may
     * hold a query the table answers through a subquery, which an aggregate's FILTER clause
     * cannot carry; range buckets are plain comparisons.
     */
    private static boolean countsOnly(Explicit e) {
        return e.kind != ExplicitKind.FILTERS && e.metrics.isEmpty() && e.hits.isEmpty() && e.child == null && !e.buckets.isEmpty();
    }

    /**
     * The counts of {@link #countsOnly} buckets, all in one call: each bucket's, and the
     * relation's own as {@link #TOTAL_ALIAS} when {@code withTotal}. Over the plain table
     * the engine counts a range-indexed field's buckets from its range index, with no
     * statement; over any other relation they are one statement of a
     * {@code COUNT(*) FILTER (WHERE ...)} per bucket, which reads the relation once.
     */
    private Map<String, Object> explicitCounts(Explicit e, String from, String where, boolean withTotal) {
        if (table != null && from.equals(InfinoQueryTranslator.quoteIdent(table))) {
            List<String> predicates = new ArrayList<>();
            if (withTotal) {
                predicates.add(where);
            }
            for (ExplicitBucket b : e.buckets) {
                predicates.add("(" + where + ") AND (" + b.predicate() + ")");
            }
            long[] counted = InfinoNative.countWhere(table, predicates);
            Map<String, Object> row = new HashMap<>();
            int next = 0;
            if (withTotal) {
                row.put(TOTAL_ALIAS, counted[next++]);
            }
            for (int i = 0; i < e.buckets.size(); i++) {
                row.put(bucketCount(i), counted[next++]);
            }
            return row;
        }
        List<String> counts = new ArrayList<>();
        if (withTotal) {
            counts.add("COUNT(*) AS " + TOTAL_ALIAS);
        }
        for (int i = 0; i < e.buckets.size(); i++) {
            counts.add("COUNT(*) FILTER (WHERE " + e.buckets.get(i).predicate() + ") AS " + bucketCount(i));
        }
        return oneRow("SELECT " + String.join(", ", counts) + " FROM " + from + " WHERE " + where);
    }

    /** The response of {@link #countsOnly} buckets from their {@link #explicitCounts} row. */
    private InternalAggregation buildExplicitCounts(Explicit e, Map<String, Object> counted) {
        List<Map<String, Object>> rows = new ArrayList<>();
        List<List<InternalAggregation>> topHits = new ArrayList<>();
        List<List<InternalAggregation>> nested = new ArrayList<>();
        for (int i = 0; i < e.buckets.size(); i++) {
            Map<String, Object> row = new HashMap<>();
            row.put("doc_count", counted.get(bucketCount(i)));
            rows.add(row);
            topHits.add(List.of());
            nested.add(List.of());
        }
        return e.build(rows, topHits, nested);
    }

    /** The column a bucket's count comes back in from {@link #explicitCounts}. */
    private static String bucketCount(int bucket) {
        return "bucket_" + bucket;
    }

    /** The nested aggregations of one bucket, {@code child} (or each of its {@link Siblings}) over the bucket's rows. */
    private List<InternalAggregation> evalChildren(BucketSpec child, String from, String where, long docCount) {
        if (child == null) {
            return List.of();
        }
        if (child instanceof Siblings s) {
            List<InternalAggregation> out = new ArrayList<>();
            for (BucketSpec c : s.children) {
                out.add(evalBucket(c, from, where, docCount));
            }
            return out;
        }
        return List.of(evalBucket(child, from, where, docCount));
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
     * counts of those terms are a second GROUP BY over the background rows
     * (see {@link #backgroundRows}), and their number is the superset size.
     */
    private InternalAggregation evalSignificant(Significant s, String from, String where, long subsetSize) {
        String key = s.keyExpr();
        String select = key + " AS k0, COUNT(*) AS doc_count" + (s.metrics.isEmpty() ? "" : ", " + metricColumns(s.metrics));
        String having = s.minDocCount > 1 ? " HAVING COUNT(*) >= " + s.minDocCount : "";
        List<Map<String, Object>> foreground = InfinoNative.sqlRows(
            "SELECT * FROM (SELECT " + select + " FROM " + from + " WHERE (" + where + ") AND " + key + " IS NOT NULL GROUP BY 1" + having
                + ") t ORDER BY doc_count DESC, k0 ASC LIMIT " + s.candidates
        );
        String backgroundRows = backgroundRows(s);
        long supersetSize = asLong(oneRow("SELECT COUNT(*) AS c FROM " + backgroundRows).get("c"));
        Map<String, Long> background = new HashMap<>();
        if (!foreground.isEmpty()) {
            List<String> literals = new ArrayList<>();
            for (Map<String, Object> row : foreground) {
                literals.add(Grouped.keyLiteral(s.keyKind, row.get("k0")));
            }
            for (Map<String, Object> row : InfinoNative.sqlRows(
                "SELECT k0, COUNT(*) AS bg FROM " + backgroundRows + " WHERE k0 IN (" + String.join(", ", literals) + ") GROUP BY 1"
            )) {
                background.put(keyString(row.get("k0")), asLong(row.get("bg")));
            }
        }
        List<Significant.Candidate> candidates = new ArrayList<>();
        List<List<InternalAggregation>> topHits = new ArrayList<>();
        List<List<InternalAggregation>> nested = new ArrayList<>();
        for (Map<String, Object> row : foreground) {
            Object k = row.get("k0");
            long subsetDf = asLong(row.get("doc_count"));
            long supersetDf = background.getOrDefault(keyString(k), 0L);
            candidates.add(new Significant.Candidate(k, subsetDf, Math.max(subsetDf, supersetDf), row));
            String w = "(" + where + ") AND " + key + " = " + Grouped.keyLiteral(s.keyKind, k);
            topHits.add(bucketHits(s, from, w, subsetDf));
            nested.add(evalChildren(s.child, from, w, subsetDf));
        }
        return s.build(candidates, topHits, nested, subsetSize, supersetSize);
    }

    /**
     * The key of every row a significant_terms level's background counts
     * run over, as a relation of one column, {@code k0}: the rows of each
     * table the aggregations read that the background filter keeps and the
     * user may see. The document-level rule applies to the background as to
     * the foreground; without it the background counts would reveal how
     * often a term occurs in documents the user may not read.
     */
    private String backgroundRows(Significant s) {
        SignificantTermsAggregationBuilder st = (SignificantTermsAggregationBuilder) s.builder;
        List<String> branches = new ArrayList<>();
        for (String t : tables) {
            Map<String, Column> full = InfinoNative.columns(t);
            String where = t.equals(s.table) ? s.backgroundWhere : Significant.backgroundWhere(st, plan.access().columns(full), t, plan.nowMillis());
            String rule = plan.access().documentsWhere(full, t, plan.nowMillis());
            if (rule != null) {
                where = "(" + where + ") AND " + rule;
            }
            branches.add("SELECT " + s.keyExpr() + " AS k0 FROM " + quoteIdent(t) + " WHERE " + where);
        }
        return "(" + String.join(" UNION ALL ", branches) + ") AS " + quoteIdent(BACKGROUND_ALIAS);
    }

    /** The composite page: the GROUP BY over every key, past the after key, in key order, cut to size. */
    private InternalAggregation evalComposite(Composite c, String from, String where) {
        List<Map<String, Object>> rows = countedDateBuckets(c, from, where);
        if (rows == null) {
            rows = groupedCompositeRows(c, from, where);
        }
        List<List<InternalAggregation>> topHits = new ArrayList<>();
        List<List<InternalAggregation>> nested = new ArrayList<>();
        for (Map<String, Object> row : rows) {
            long docCount = asLong(row.get("doc_count"));
            String bucket = "(" + where + ") AND " + bucketRestriction(c, row);
            topHits.add(bucketHits(c, from, bucket, docCount));
            nested.add(evalChildren(c.child, from, bucket, docCount));
        }
        return c.build(rows, topHits, nested);
    }

    /** Most date buckets {@link #countedDateBuckets} counts; past it one grouped statement reads them faster. */
    static final int MAX_COUNTED_DATE_BUCKETS = 64;

    /**
     * A composite's page counted bucket by bucket from the date column's
     * range index, with no statement planned, when that answers it: one date
     * histogram source over the column the request's one range bounds (the
     * range gives the buckets to count), no missing bucket, no after key, and
     * nothing in a bucket but its count. Each bucket is one interval of the
     * column ({@link InfinoNative#countWhere}), so the engine counts it from
     * the index. Null when the composite is not of that shape, or spans more
     * than {@link #MAX_COUNTED_DATE_BUCKETS} buckets.
     */
    private List<Map<String, Object>> countedDateBuckets(Composite c, String from, String where) {
        Translation q = plan.query();
        InfinoQueryTranslator.Span span = q.span();
        if (span == null
            || table == null
            || !where.equals(q.where())
            || !from.equals(q.from())
            || c.sources.size() != 1
            || c.after != null
            || !c.metrics.isEmpty()
            || !c.hits.isEmpty()
            || c.child != null) {
            return null;
        }
        CompositeSource s = c.sources.get(0);
        String column = InfinoQueryTranslator.quoteIdent(span.column());
        if (s.rounding() == null || s.missingBucket() || !s.source().expr().equals(column)) {
            return null;
        }
        Rounding.Prepared rounding = s.rounding().prepareForUnknown();
        List<Long> starts = new ArrayList<>();
        List<String> predicates = new ArrayList<>();
        long start = rounding.round(span.fromMillis());
        while (start < span.untilMillis()) {
            long next = rounding.nextRoundingValue(start);
            if (next <= start || starts.size() == MAX_COUNTED_DATE_BUCKETS) {
                return null;
            }
            long lo = Math.max(start, span.fromMillis());
            long hi = Math.min(next, span.untilMillis());
            predicates.add(column + " >= " + timestampLiteral(lo) + " AND " + column + " < " + timestampLiteral(hi));
            starts.add(start);
            start = next;
        }
        long[] counts = InfinoNative.countWhere(table, predicates);
        List<Map<String, Object>> rows = new ArrayList<>();
        for (int i = 0; i < counts.length; i++) {
            if (counts[i] > 0) {
                rows.add(Map.of("k0", starts.get(i), "doc_count", counts[i]));
            }
        }
        if (s.desc()) {
            Collections.reverse(rows);
        }
        return rows.size() > c.size ? rows.subList(0, c.size) : rows;
    }

    /**
     * An instant as a literal of a date column's type, so a comparison with
     * the column is one the engine counts from its range index as written.
     */
    static String timestampLiteral(long millis) {
        return "arrow_cast('" + Instant.ofEpochMilli(millis) + "', 'Timestamp(Millisecond, Some(\"UTC\"))')";
    }

    /** A composite's page from one statement grouping by every source's key. */
    private List<Map<String, Object>> groupedCompositeRows(Composite c, String from, String where) {
        List<String> keyCols = new ArrayList<>();
        List<String> present = new ArrayList<>();
        // The sources that read a field some document gives several values.
        List<Source> multi = new ArrayList<>();
        for (int i = 0; i < c.sources.size(); i++) {
            CompositeSource s = c.sources.get(i);
            keyCols.add(s.keyExpr() + " AS k" + i);
            if (!s.missingBucket()) {
                present.add(s.keyExpr() + " IS NOT NULL");
            }
            if (s.source().multi()) {
                multi.add(s.source());
            }
        }
        String metrics = c.metrics.isEmpty() ? "" : metricColumns(c.metrics);
        String w = present.isEmpty() ? where : "(" + where + ") AND " + String.join(" AND ", present);
        String inner = groupStatement(keyCols, metrics, from, w, multi, "");
        String page = c.after == null ? "" : " WHERE " + c.keysetPredicate();
        return InfinoNative.sqlRows("SELECT * FROM (" + inner + ") t" + page + " ORDER BY " + c.order() + " LIMIT " + c.size);
    }

    /**
     * {@code (k0 = a AND k1 = b ...)} over a composite's key expressions, for
     * one of its buckets: the documents its nested aggregations read. A
     * terms source over a field some document gives several values holds a
     * document any of whose values is the key ({@link Composite#source}
     * serves only terms over such a field).
     */
    private static String bucketRestriction(Composite c, Map<String, Object> row) {
        List<String> ands = new ArrayList<>();
        for (int i = 0; i < c.sources.size(); i++) {
            CompositeSource s = c.sources.get(i);
            Object key = row.get("k" + i);
            Source source = s.source();
            if (source.multi()) {
                String literal = key == null ? null : Grouped.keyLiteral(s.keyKind(), key);
                ands.add(literal == null ? source.noValue() : source.anyValue(value -> value + " = " + literal));
                continue;
            }
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
            List<String> aliases = new ArrayList<>();
            for (int j = 0; j <= i; j++) {
                aliases.add("k" + j);
            }
            // Over a value per row, a parent's bucket is the row's value in it.
            String restriction = kept == null ? "" : " AND " + tupleRestriction(chain, kept, i, true);
            // A terms level has no bucket for the missing value, so rows without
            // one leave before the window: they take no slot and count as other to no one.
            String inner = levelInner(chain, i, from, where, restriction, level.terms);
            String parents = String.join(", ", aliases.subList(0, i));
            List<Map<String, Object>> rows = i == 0 && level.terms ? pickTopTerms(level, inner, readAhead) : null;
            if (rows == null) {
                String sql;
                if (level.terms) {
                    String partition = i == 0 ? "" : "PARTITION BY " + parents + " ";
                    // keyed_total: the rows with a value under this parent, so what the
                    // window drops is counted exactly, without the field-less rows.
                    sql = "SELECT * FROM (SELECT t.*, ROW_NUMBER() OVER (" + partition + "ORDER BY " + level.order("k" + i) + ") AS rn, SUM(doc_count) OVER ("
                        + partition.trim() + ") AS keyed_total FROM (" + inner + ") t) WHERE rn <= " + level.size + " ORDER BY " + (i == 0 ? "rn" : parents + ", rn");
                } else {
                    sql = "SELECT * FROM (" + inner + ") t ORDER BY " + String.join(", ", aliases);
                }
                rows = InfinoNative.sqlRows(sql);
            }
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
     * The bare GROUP BY of level {@code i} of {@code chain}: the level's keys
     * and every parent's, the row count and the level's metrics, over
     * {@code from WHERE where} narrowed by {@code restriction}, without rows
     * missing the level's key when {@code present}, and with the level's
     * minimum count as a HAVING. A key over a field some document gives
     * several values is read per value ({@link #groupStatement}).
     */
    private static String levelInner(List<Grouped> chain, int i, String from, String where, String restriction, boolean present) {
        Grouped level = chain.get(i);
        List<String> keyCols = new ArrayList<>();
        // The keys of this depth that read a field some document gives several values.
        List<Source> multi = new ArrayList<>();
        for (int j = 0; j <= i; j++) {
            keyCols.add(chain.get(j).keyExpr + " AS k" + j);
            if (chain.get(j).source.multi()) {
                multi.add(chain.get(j).source);
            }
        }
        String metrics = level.metrics.isEmpty() ? "" : metricColumns(level.metrics);
        String keep = present ? " AND " + level.keyExpr + " IS NOT NULL" : "";
        String having = level.minDocCount > 1 ? " HAVING COUNT(*) >= " + level.minDocCount : "";
        return groupStatement(keyCols, metrics, from, where + restriction + keep, multi, having);
    }

    /**
     * The request's exact total read from a top terms aggregation's own
     * groups, so it costs no statement of its own: the level's GROUP BY with
     * the rows missing its key kept as one more group, whose counts add up to
     * the relation's rows. The groups with a key are kept for the level to
     * pick its buckets from (see {@link #pickTopTerms}), under the statement it
     * would have run. Null, and the count is left to its own statement, when
     * that count is not a statement (a bare text query's count is a direct
     * call), when the level's buckets are not picked from its groups, when a
     * minimum count leaves groups out of the sum, when the level reads a field
     * some document gives several values (that document counts once in each of
     * its values' groups, so the groups add up to more than the rows), or when
     * the level has more groups than are picked here.
     */
    private Long totalFromTopTerms(Spec spec, Translation q) {
        if (q.principal() != null && InfinoQueryTranslator.ALL.equals(q.where())) {
            return null;
        }
        if (!(spec instanceof Grouped level) || !level.terms || level.minDocCount > 1 || level.source.multi() || termsOrder(level) == null) {
            return null;
        }
        List<Grouped> chain = List.of(level);
        String picked = levelInner(chain, 0, q.from(), q.where(), "", true);
        List<Map<String, Object>> groups;
        if (readsValueCounts(level, q)) {
            groups = valueCounts(level, q);
        } else {
            groups = InfinoNative.sqlRows(levelInner(chain, 0, q.from(), q.where(), "", false) + " LIMIT " + (MAX_GROUPS_PICKED_HERE + 2));
        }
        // One group more than are picked here may be the missing key's.
        if (groups == null || groups.size() > MAX_GROUPS_PICKED_HERE + 1) {
            readAhead.put(picked, null);
            return null;
        }
        long total = 0;
        List<Map<String, Object>> keyed = new ArrayList<>();
        for (Map<String, Object> group : groups) {
            total += asLong(group.get("doc_count"));
            if (group.get("k0") != null) {
                keyed.add(group);
            }
        }
        if (keyed.size() > MAX_GROUPS_PICKED_HERE) {
            readAhead.put(picked, null);
            return total;
        }
        readAhead.put(picked, keyed);
        return total;
    }

    /**
     * Whether a top terms level's groups are the table's value counts: a
     * request that does not filter the table, over a column no document gives
     * several values (the counts are the column's own, not its list's), with
     * no metrics.
     */
    private static boolean readsValueCounts(Grouped level, Translation q) {
        return q.principal() == null
            && InfinoQueryTranslator.ALL.equals(q.where())
            && level.source.column() != null
            && !level.source.multi()
            && level.metrics.isEmpty();
    }

    /**
     * A top terms level's groups over the whole table, the rows missing its
     * key among them, as its GROUP BY would return them, by a direct call
     * instead of a statement (see {@link #readsValueCounts}): the engine reads
     * them from the table's statistics when those count every row exactly,
     * and runs the grouped count otherwise. Null when there are more than are
     * picked here, the missing key's group aside.
     */
    private static List<Map<String, Object>> valueCounts(Grouped level, Translation q) {
        List<Map<String, Object>> counts = InfinoNative.valueCounts(q.table(), level.source.column(), MAX_GROUPS_PICKED_HERE + 1);
        if (counts == null) {
            return null;
        }
        List<Map<String, Object>> groups = new ArrayList<>(counts.size());
        for (Map<String, Object> row : counts) {
            Map<String, Object> group = new HashMap<>();
            group.put("k0", row.get(level.source.column()));
            group.put("doc_count", row.get(InfinoNative.VALUE_COUNT));
            groups.add(group);
        }
        return groups;
    }

    /**
     * The buckets of a top terms level picked from its bare GROUP BY
     * ({@code inner}), which the engine answers from the table's statistics,
     * instead of through the window statement, which it does not: the groups
     * in the level's order, the first {@code size} kept, each carrying
     * keyed_total, the rows of every group, as the window statement's rows
     * do. Null, for the window statement to answer, when the order reads a
     * metric (a missing metric value sorts where the engine puts it) or the
     * level has more groups than are worth picking here.
     */
    private static List<Map<String, Object>> pickTopTerms(Grouped level, String inner, Map<String, List<Map<String, Object>>> readAhead) {
        Comparator<Map<String, Object>> order = termsOrder(level);
        if (order == null) {
            return null;
        }
        // The groups the request total was read from, when it was read from
        // them (see totalFromTopTerms); a null there is a level with too many.
        List<Map<String, Object>> groups;
        if (readAhead.containsKey(inner)) {
            List<Map<String, Object>> read = readAhead.get(inner);
            if (read == null) {
                return null;
            }
            groups = new ArrayList<>(read);
        } else {
            groups = new ArrayList<>(InfinoNative.sqlRows(inner + " LIMIT " + (MAX_GROUPS_PICKED_HERE + 1)));
        }
        if (groups.size() > MAX_GROUPS_PICKED_HERE) {
            return null;
        }
        long keyedTotal = 0;
        for (Map<String, Object> group : groups) {
            keyedTotal += asLong(group.get("doc_count"));
        }
        groups.sort(order);
        List<Map<String, Object>> out = new ArrayList<>();
        for (Map<String, Object> group : groups.subList(0, Math.min(level.size, groups.size()))) {
            Map<String, Object> row = new HashMap<>(group);
            row.put("keyed_total", keyedTotal);
            out.add(row);
        }
        return out;
    }

    /**
     * A top terms level's order over its group rows, as the engine's ORDER BY
     * reads it: by count, and by key in the key's own order (a string by its
     * UTF-8 bytes, false before true, numbers by value). Null when the order
     * reads anything else.
     */
    private static Comparator<Map<String, Object>> termsOrder(Grouped level) {
        Comparator<Map<String, Object>> out = null;
        for (String term : level.orderTemplate.split(", ")) {
            String[] parts = term.split(" ");
            Comparator<Map<String, Object>> c;
            if (parts[0].equals("doc_count")) {
                c = Comparator.comparingLong(row -> asLong(row.get("doc_count")));
            } else if (parts[0].equals("{key}")) {
                c = (a, b) -> compareKeys(level.keyKind, a.get("k0"), b.get("k0"));
            } else {
                return null;
            }
            if (parts[1].equals("DESC")) {
                c = c.reversed();
            }
            out = out == null ? c : out.thenComparing(c);
        }
        return out;
    }

    /** Two present keys of {@code kind} in the engine's order. */
    private static int compareKeys(KeyKind kind, Object a, Object b) {
        return switch (kind) {
            case STR -> new BytesRef(a.toString()).compareTo(new BytesRef(b.toString()));
            case BOOL -> Boolean.compare(asBoolean(a), asBoolean(b));
            case LONG, DATE -> Long.compare(asLong(a), asLong(b));
            case DOUBLE -> Double.compare(asDouble(a, 0.0), asDouble(b, 0.0));
        };
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
        String restriction = tupleRestriction(chain, kept, i + 1, true);
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

    /**
     * {@code ((k0 = a AND k1 = b) OR ...)} over the parent levels' buckets,
     * for the tuples they kept: over rows that each hold one value of a
     * field ({@code perValue}, {@link Grouped#rowPredicate}), else over the
     * documents ({@link Grouped#docPredicate}).
     */
    private static String tupleRestriction(List<Grouped> chain, List<List<Object>> kept, int level, boolean perValue) {
        List<String> ors = new ArrayList<>();
        for (List<Object> tuple : kept) {
            List<String> ands = new ArrayList<>();
            for (int j = 0; j < level; j++) {
                Grouped g = chain.get(j);
                ands.add(perValue ? g.rowPredicate(tuple.get(j)) : g.docPredicate(tuple.get(j)));
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
            List<InternalAggregation> nested = depth + 1 < chain.size()
                ? List.of(fold(chain, depth + 1, keys, rowsByLevel, hitsByLevel, docCount, leafChild, from, where))
                : evalChildren(leafChild, from, "(" + where + ") AND " + tupleRestriction(chain, List.of(keys), keys.size(), false), docCount);
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

    /**
     * A boolean key as the bridge returns it: a JSON {@code true} or
     * {@code false}, which the row parser reads as a Boolean. Anything else is
     * refused rather than read as false, which would put every key in one
     * place in a key order.
     */
    static boolean asBoolean(Object v) {
        if (v instanceof Boolean b) {
            return b;
        }
        if ("true".equals(v) || "false".equals(v)) {
            return "true".equals(v);
        }
        throw new IllegalStateException("a boolean key arrived as " + v);
    }
}
