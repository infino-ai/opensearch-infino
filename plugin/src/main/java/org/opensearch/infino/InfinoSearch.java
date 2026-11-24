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
import org.opensearch.action.OriginalIndices;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.SearchResponseSections;
import org.opensearch.action.search.ShardSearchFailure;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.MappingMetadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.document.DocumentField;
import org.opensearch.common.lucene.search.TopDocsAndMaxScore;
import org.opensearch.common.regex.Regex;
import org.opensearch.common.time.DateFormatter;
import org.opensearch.common.time.DateFormatters;
import org.opensearch.common.util.BigArrays;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.core.common.breaker.NoopCircuitBreaker;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.mapper.SeqNoFieldMapper;
import org.opensearch.index.mapper.SourceFieldMapper;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.infino.InfinoNative.Column;
import org.opensearch.infino.InfinoQueryTranslator.Bonus;
import org.opensearch.infino.InfinoQueryTranslator.Scoring;
import org.opensearch.infino.InfinoQueryTranslator.TextClause;
import org.opensearch.infino.InfinoQueryTranslator.Translation;
import org.opensearch.infino.InfinoScriptTranslator.Sql;
import org.opensearch.infino.InfinoScriptTranslator.SqlType;
import org.opensearch.script.ScriptService;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.search.SearchShardTarget;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.search.aggregations.MultiBucketConsumerService;
import org.opensearch.search.aggregations.metrics.InternalTopHits;
import org.opensearch.search.aggregations.metrics.TopHitsAggregationBuilder;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.collapse.CollapseBuilder;
import org.opensearch.search.fetch.StoredFieldsContext;
import org.opensearch.search.fetch.subphase.FetchSourceContext;
import org.opensearch.search.fetch.subphase.FieldAndFormat;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.sort.FieldSortBuilder;
import org.opensearch.search.sort.ScoreSortBuilder;
import org.opensearch.search.sort.ScriptSortBuilder;
import org.opensearch.search.sort.SortBuilder;
import org.opensearch.search.sort.SortMode;
import org.opensearch.search.sort.SortOrder;

import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

/**
 * Answers one {@code _search} for one Infino-backed index from its table,
 * in proxy mode at the action layer: no shard, no Lucene, no fetch phase.
 *
 * <p>Hits come one of three ways: an {@code infino_knn} / {@code infino_hybrid}
 * query, or the k-NN plugin's own {@code knn} query (see {@link InfinoStockKnn}),
 * runs the engine's vector search; a request sorted by a field runs an
 * ordered SQL scan over the translated filter; a request ranked by a
 * full-text clause runs the engine's BM25 table function with the filter
 * applied to the ranked candidates; anything else is a filtered scan. The hit
 * statement carries each hit's source, so nothing is looked up by id
 * afterwards. The exact total is a {@code COUNT} over the same relation, not
 * the fetched window. Aggregations run as SQL over the same relation and come
 * back as the stock response objects, reduced once so pipeline aggregations
 * are computed.
 *
 * <p>Hits and aggregations share one {@link #plan} decision so they never
 * disagree; a request the table cannot answer fails with the clause named.
 */
public final class InfinoSearch {

    private static final Logger log = LogManager.getLogger(InfinoSearch.class);
    /**
     * Candidates BM25 ranks for a page whose statement removes rows after the
     * ranking: a collapse keeps one row per value, and a text clause on
     * another column is a subquery the engine joins above the ranked search.
     * Every other filter goes to the engine with the ranked search, which
     * ranks within exactly the rows the filter keeps (as Lucene applies a
     * filter while it collects), so such a page asks for the page alone. A
     * page sorted by the score then fields ranks this many too, so hits that
     * tie on the score compete on the fields ({@link #scoredStatement}).
     */
    private static final int RANK_WINDOW = 10_000;
    /**
     * The most score bands a scored statement ranks apart ({@link #scoredStatement}):
     * past this the text clause's candidates are ranked once, over
     * {@link #RANK_WINDOW} of them, and scored after.
     */
    private static final int MAX_SCORE_BANDS = 64;
    /** OpenSearch's page size when the request names none. */
    static final int DEFAULT_SIZE = 10;
    /** The node name a hit's shard target carries; it only names the index in the response. */
    private static final String NODE = "infino";
    /** How a date in a hit's source is read back and printed when its mapping names no format: OpenSearch's default. */
    private static final String DEFAULT_DATE_FORMAT = "strict_date_optional_time||epoch_millis";
    /** The format a request names to have a date printed as its field's mapping prints it. */
    private static final String MAPPING_FORMAT = "use_field_mapping";
    /** The format that prints a date as its epoch milliseconds. */
    private static final String EPOCH_MILLIS_FORMAT = "epoch_millis";
    /** Digits of the engine's row id: its type is {@code DECIMAL(38, 0)}. */
    private static final int ROW_ID_DIGITS = 38;
    /**
     * The order a page with no order of its own is read in where the
     * engine's file order does not hold (a collapse, a read that is not a
     * plain scan of the table): the engine's row id, ascending
     * ({@link SortSpec#rowId}).
     */
    static final String ROW_ID_ORDER = orderBy(List.of(SortSpec.rowId())).get(0);
    /** The sort a scroll without one pages by: the document id, which {@link #columnFor} resolves to the id column. */
    private static final String ID_SORT_FIELD = "_id";
    /** The alias the hit statement selects a collapsed hit's own value under, unless the plan has a name of that kind ({@link #freeAlias}). */
    private static final String COLLAPSE_KEY = "collapse_key";
    /** The alias of a collapse's window rank, likewise. */
    private static final String COLLAPSE_RANK = "collapse_rank";
    /** Why a scroll sorted by {@code _score} then fields over a scored query is refused: its pages resume from a score no keyset can compare against. */
    static final String SCORE_SORT_SCROLL = "[infino] a sort mixing _score with a field is not served with scroll on a scored query";
    /**
     * Why a scroll with {@code terminate_after} is refused: its rounds are
     * read from a cursor over the table, with no shard collecting toward the
     * limit as a stock scroll's rounds do.
     */
    static final String TERMINATE_AFTER_SCROLL = "[infino] terminate_after is not served with a scroll";
    /** The column a ranked or scored statement gives each hit's score under. */
    static final String SCORE = "score";
    /**
     * The column {@link #scoredStatement} gives, under a sort by {@code _score}
     * then fields or when a step removes rows after its ranked reads
     * ({@link #narrowsAfterRanking}), the greatest score a candidate its
     * ranked windows cut off could have (null when no window was cut).
     */
    static final String SCORE_FLOOR = "infino_floor";
    /**
     * The column {@link #scoredStatement} gives, when a step removes rows
     * after its ranked reads, how many of its ranked bands kept a row: a band
     * that kept none bounds nothing its window cut off.
     */
    static final String SCORE_BANDS = "infino_bands";

    private InfinoSearch() {}

    /**
     * What one request translates to, shared by the hit and aggregation paths.
     * {@code cols} are the columns as the request may see them (see
     * {@link InfinoAccess#columns}), {@code query} carries the document-level
     * rule as part of its predicate. {@code postWhere} is the post_filter's
     * predicate (null when none): it narrows the hits and their total, never
     * the aggregations. {@code collapse} is the column hits are collapsed on
     * (null when none), {@code searchAfter} the sort values the page starts
     * after (null when none), {@code access} what the request may see of
     * each document, {@code nowMillis} the time {@code now} means in it.
     */
    record Plan(
        String table,
        Map<String, Column> cols,
        Translation query,
        List<SortSpec> sorts,
        VectorRequest vector,
        String postWhere,
        Column collapse,
        Object[] searchAfter,
        List<ScriptedField> scriptFields,
        InfinoAccess access,
        long nowMillis
    ) {
        List<String> orderBy() {
            return InfinoSearch.orderBy(sorts);
        }

        /**
         * Whether the hits' scores differ by the query's text: a ranked text
         * clause, scoring clauses that reorder it, a query string across
         * several text columns ({@link Scoring#across}), or text the table
         * cannot rank ({@link Scoring#unrankable}). Every hit of another query
         * scores the same constant ({@link #constantScore}).
         */
        boolean scored() {
            return vector == null && InfinoSearch.scored(query);
        }

        /** Whether the hits are ordered by their score beside a field: a scored query sorted by {@code _score}, then fields. */
        boolean ranksByScore() {
            return scored() && sorts.stream().anyMatch(SortSpec::isScore);
        }

        /**
         * This plan with {@code query} and {@code postWhere} in place of its
         * own, and nothing to sort, collapse, resume after or script: its hit
         * statement reads the hits ranked as an unsorted page reads them.
         */
        Plan unsorted(Translation query, String postWhere) {
            return new Plan(table, cols, query, List.of(), vector, postWhere, null, null, List.of(), access, nowMillis);
        }

        /** The predicate the hits satisfy: the query's and the post_filter's. */
        String hitWhere() {
            return postWhere == null ? query.where() : "(" + query.where() + ") AND (" + postWhere + ")";
        }

        /** This plan with {@code predicate} required of every row, the hits', the total's and the aggregations' alike. */
        Plan requiring(String predicate) {
            String where = InfinoQueryTranslator.ALL.equals(query.where()) ? predicate : "(" + query.where() + ") AND " + predicate;
            Translation narrowed = query.withWhere(where);
            return new Plan(table, cols, narrowed, sorts, vector, postWhere, collapse, searchAfter, scriptFields, access, nowMillis);
        }
    }

    /**
     * One sort of the request: a column, or a script as a column expression.
     * {@code expr} is what the statement orders by and selects, {@code name}
     * the alias its value comes back under (the column's name, or one made
     * for a script), {@code type} how a search_after value for it is written.
     * {@code nullsFirst} puts the rows missing the value before the others
     * (the request's {@code missing: _first}); by default they come last,
     * in either direction, as stock's {@code _last}. {@code noneMissing}
     * marks a sort whose expression gives every row a value (the request's
     * {@code missing} value stands in), so no search_after value for it
     * stands for a missing one. {@code asInt} marks a field mapped
     * {@code integer}, {@code short} or {@code byte}, which stock's
     * comparator reads as a 32-bit int.
     */
    record SortSpec(String expr, String name, SqlType type, boolean desc, boolean nullsFirst, boolean noneMissing, boolean asInt) {
        SortSpec(String expr, String name, SqlType type, boolean desc, boolean nullsFirst, boolean noneMissing) {
            this(expr, name, type, desc, nullsFirst, noneMissing, false);
        }

        SortSpec(String expr, String name, SqlType type, boolean desc) {
            this(expr, name, type, desc, false, false);
        }

        /** This sort on a field stock's comparator reads as a 32-bit int when {@code asInt} ({@link #standIn}). */
        SortSpec asInt(boolean asInt) {
            return new SortSpec(expr, name, type, desc, nullsFirst, noneMissing, asInt);
        }

        /** {@link #withMissing(Object, boolean, long)} on a field not mapped {@code float} or {@code half_float}. */
        SortSpec withMissing(Object missing, long nowMillis) {
            return withMissing(missing, false, nowMillis);
        }

        /**
         * This sort under the request's {@code missing}: {@code _last} (or
         * none) as it is, {@code _first} with the missing rows first, and any
         * other value standing in for a missing one, as stock sorts such a
         * row by that value and hands it back as the hit's sort value.
         * {@code asFloat} marks a field mapped {@code float} or
         * {@code half_float}, whose value stock's comparator reads as a
         * 32-bit float. A boolean's value is a long, as stock's comparator
         * reads it (a present value is 0 or 1), so the sort becomes one on
         * longs. An infinite floating-point value (or NaN, which stock's
         * comparator orders past every other) puts the missing rows past
         * every present value on its side, as {@code _first} or
         * {@code _last} does: no present value is one, since stock refuses
         * a non-finite value for a floating-point field.
         */
        SortSpec withMissing(Object missing, boolean asFloat, long nowMillis) {
            if (missing == null) {
                return this;
            }
            String text = missing instanceof BytesRef b ? b.utf8ToString() : missing.toString();
            if ("_last".equals(text)) {
                return this;
            }
            if ("_first".equals(text)) {
                return new SortSpec(expr, name, type, desc, true, false, asInt);
            }
            Object value = missing instanceof BytesRef ? text : missing;
            if (type == SqlType.FLOAT) {
                Double d = floating(value, asFloat);
                if (d == null) {
                    throw notA(text, "float");
                }
                if (!Double.isFinite(d)) {
                    boolean greatest = d != Double.NEGATIVE_INFINITY;
                    return new SortSpec(expr, name, type, desc, greatest == desc, false, asInt);
                }
                return new SortSpec("COALESCE(" + expr + ", " + Double.toString(d) + ")", name, type, desc, false, true);
            }
            if (type == SqlType.BOOL) {
                Long l = integral(value);
                if (l == null) {
                    throw notA(text, "long");
                }
                return new SortSpec("COALESCE(CAST(" + expr + " AS BIGINT), " + l + ")", name, SqlType.INT, desc, false, true);
            }
            String lit = replacement(value, nowMillis);
            if (lit == null) {
                throw notA(text, type.name().toLowerCase(Locale.ROOT));
            }
            return new SortSpec("COALESCE(" + expr + ", " + lit + ")", name, type, desc, false, true);
        }

        private IllegalArgumentException notA(String text, String what) {
            return new IllegalArgumentException("[infino] sort missing value [" + text + "] on [" + name + "] is not a " + what);
        }

        /**
         * The request's {@code missing} value as a literal of this sort's
         * type, read as stock's comparator reads it: a number as a long for
         * an integer or a date (3.5 is 3, a date's number its epoch
         * milliseconds), and text parsed as one; null when it is not one.
         */
        private String replacement(Object missing, long nowMillis) {
            return switch (type) {
                case INT -> {
                    Long l = integral(missing);
                    yield l == null ? null : Long.toString(l);
                }
                case DATE -> {
                    Long l = integral(missing);
                    yield l == null ? null : "arrow_cast(" + l + ", 'Timestamp(Millisecond, Some(\"UTC\"))')";
                }
                default -> {
                    try {
                        yield InfinoQueryTranslator.literalForType(type, missing, nowMillis);
                    } catch (NumberFormatException e) {
                        yield null;
                    }
                }
            };
        }

        /** {@code missing} as stock's long comparator reads it: a number cut to a long, text parsed as one; null when it is not one. */
        private static Long integral(Object missing) {
            try {
                return missing instanceof Number n ? n.longValue() : Long.parseLong(missing.toString());
            } catch (NumberFormatException e) {
                return null;
            }
        }

        /**
         * {@code missing} as stock's floating-point comparator reads it: a
         * 32-bit float when {@code asFloat}, else a double, from a number or
         * parsed text; null when it is not one.
         */
        private static Double floating(Object missing, boolean asFloat) {
            try {
                if (asFloat) {
                    return (double) (missing instanceof Number n ? n.floatValue() : Float.parseFloat(missing.toString()));
                }
                return missing instanceof Number n ? n.doubleValue() : Double.parseDouble(missing.toString());
            } catch (NumberFormatException e) {
                return null;
            }
        }

        /**
         * Whether {@code value}, a search_after value for this sort, is the
         * stand-in a default-mode hit hands back for a row missing the value
         * (the least or greatest long, or an infinity, whichever sorts on the
         * missing rows' side): the client sends back what the hit gave it, and
         * the page continues among the missing rows, as stock compares them.
         */
        boolean missingStandIn(Object value) {
            Object standIn = standIn();
            if (noneMissing || standIn == null) {
                return false;
            }
            return switch (type) {
                case INT, DATE -> value instanceof Number n
                    && !(value instanceof Double || value instanceof Float)
                    && n.longValue() == (Long) standIn
                    || value instanceof String t && t.equals(standIn.toString());
                default -> value instanceof Number n && n.doubleValue() == (Double) standIn
                    || value instanceof String t && t.equals(standIn.toString());
            };
        }

        /**
         * The sort value a hit missing this sort's value carries, as stock's
         * comparator stands one in, so it sorts on the missing rows' side in
         * this sort's direction: the least long (an integer or a date; the
         * least 32-bit int for a field read as one, {@link #asInt}) or double
         * (a floating-point field) when that side comes last in a descending
         * sort or first in an ascending one, else the greatest.
         * Null for a sort on any other type, whose missing value a hit
         * carries as null.
         */
        Object standIn() {
            boolean least = desc != nullsFirst;
            return switch (type) {
                case INT -> asInt ? (least ? (long) Integer.MIN_VALUE : (long) Integer.MAX_VALUE) : (least ? Long.MIN_VALUE : Long.MAX_VALUE);
                case DATE -> least ? Long.MIN_VALUE : Long.MAX_VALUE;
                case FLOAT -> least ? Double.NEGATIVE_INFINITY : Double.POSITIVE_INFINITY;
                default -> null;
            };
        }

        /** {@code value}, a hit's value for this sort, or the {@link #standIn} for a missing one. */
        Object orStandIn(Object value) {
            return value == null && !noneMissing ? standIn() : value;
        }
        static SortSpec of(Column c, boolean desc) {
            return new SortSpec(InfinoQueryTranslator.quoteIdent(c.name()), c.name(), InfinoScriptTranslator.typeOf(c), desc);
        }

        /**
         * The sort on a field some document gives several values, whose list
         * column is {@code values} (the field's own column when it is a list):
         * such a document sorts by its greatest value when {@code greatest},
         * else its least.
         */
        static SortSpec ofValues(Column c, Column values, boolean greatest, boolean desc) {
            String pick = (greatest ? "array_max(" : "array_min(") + InfinoQueryTranslator.quoteIdent(values.name()) + ")";
            String expr = c.isList()
                ? pick
                : "CASE WHEN " + InfinoQueryTranslator.quoteIdent(values.name()) + " IS NULL THEN " + InfinoQueryTranslator.quoteIdent(c.name())
                    + " ELSE " + pick + " END";
            return new SortSpec(expr, c.name(), InfinoScriptTranslator.typeOf(c.element()), desc);
        }

        /**
         * The engine's row id, which orders the rows sharing every sort value
         * of a request: the order they were written in, as stock orders them
         * by their place in the index. The engine answers a sort broken this
         * way from the first rows of each superfile's range index rather than
         * every row tied with the page's last value. Its value is the id's
         * digits ({@link #rowIdLiteral} reads it back).
         */
        static SortSpec rowId() {
            return new SortSpec(InfinoQueryTranslator.quoteIdent(InfinoSql.ROW_ID), InfinoSql.ROW_ID, SqlType.UNKNOWN, false);
        }

        /** The sort on {@code _seq_no}: the sequence column, a row written without a number at the one it reports. */
        static SortSpec seqNo(boolean desc) {
            String expr = "COALESCE(" + InfinoQueryTranslator.SEQ + ", " + InfinoDocumentVersion.UNTRACKED_SEQ_NO + ")";
            return new SortSpec(expr, SeqNoFieldMapper.NAME, SqlType.INT, desc);
        }

        /**
         * The sort on {@code _score} beside a field: no column of the table.
         * A scored query's hits carry the score their ranked read computes
         * ({@link #scoredStatement}); every hit of another query carries the
         * same constant ({@link #constantScore}), so the sort orders nothing
         * there. Every hit has a score, so none is missing.
         */
        static SortSpec score(boolean desc) {
            return new SortSpec(SCORE, ScoreSortBuilder.NAME, SqlType.FLOAT, desc, false, true);
        }

        /** Whether this is the sort on {@code _score} ({@link #score}). */
        boolean isScore() {
            return name.equals(ScoreSortBuilder.NAME) && expr.equals(SCORE);
        }
    }

    /** One scripted field of the request: its name and its expression, selected under {@code alias}. */
    record ScriptedField(String name, String expr, String alias) {}

    /**
     * A kNN or hybrid request, served by the engine's vector search instead of
     * SQL. {@code stock} marks the k-NN plugin's own {@code knn} query, which
     * keeps that query's meaning: the engine returns its k nearest and the
     * page is a window over them, so the total is how many came back.
     * {@code filter} is that query's filter as a predicate the engine narrows
     * the search with (null when it has none).
     */
    record VectorRequest(boolean hybrid, String textColumn, String textQuery, String vectorColumn, float[] vector, int k, boolean stock, String filter) {
        static VectorRequest of(boolean hybrid, String textColumn, String textQuery, String vectorColumn, float[] vector, int k) {
            return new VectorRequest(hybrid, textColumn, textQuery, vectorColumn, vector, k, false, null);
        }
    }

    /**
     * The hits and total of one index, before the response is assembled. The
     * first {@code unsourced} hits sit before the caller's window and carry no
     * source (see {@link #page(Index, Plan, SearchSourceBuilder, boolean, boolean, boolean, int)}).
     * {@code chosen} is how many rows the page's statement chose, counted
     * before any row deleted since dropped out: a page with fewer hits than it
     * asked for is the last only when it chose fewer.
     */
    record Page(List<SearchHit> hits, long total, boolean exact, float maxScore, int unsourced, int chosen) {
        Page(List<SearchHit> hits, long total, boolean exact, float maxScore, int unsourced) {
            this(hits, total, exact, maxScore, unsourced, hits.size());
        }

        Page(List<SearchHit> hits, long total, boolean exact, float maxScore) {
            this(hits, total, exact, maxScore, 0);
        }
    }

    // ---- plan ----

    /** The plan for a request the user may see whole; throws naming the first clause the table cannot serve. */
    static Plan plan(String table, SearchSourceBuilder source, long nowMillis) {
        return plan(table, source, nowMillis, InfinoAccess.NONE);
    }

    /** The plan for a request under {@code access}; throws naming the first clause the table cannot serve. */
    static Plan plan(String table, SearchSourceBuilder source, long nowMillis, InfinoAccess access) {
        if (source.suggest() != null) {
            throw new IllegalArgumentException("[infino] suggest is not served by the table");
        }
        // Each changes which documents match or how they rank, so answering without it would be a
        // wrong answer: min_score drops documents scored under it from the hits, total and
        // aggregations (a score is never negative, so one of 0 or less drops none), and rescore
        // reranks the top documents by another query. terminate_after is served once the total
        // is known (see terminated).
        if (source.minScore() != null && source.minScore() > 0) {
            throw new IllegalArgumentException("[infino] min_score is not served by the table");
        }
        if (source.rescores() != null && !source.rescores().isEmpty()) {
            if (source.collapse() != null) {
                // Stock refuses the pair before it would rescore, with this message.
                throw new IllegalArgumentException("cannot use `collapse` in conjunction with `rescore`");
            }
            throw new IllegalArgumentException("[infino] rescore is not served by the table");
        }
        Map<String, Column> fullCols = InfinoNative.columns(table);
        if (fullCols.isEmpty()) {
            throw new IllegalStateException("[infino] table " + table + " has not been declared");
        }
        // The request sees the columns its access leaves it; a field it may not
        // see is as if unmapped, a masked one carries its hash.
        Map<String, Column> cols = access.columns(fullCols);
        QueryBuilder qb = source.query();
        VectorRequest vector = null;
        Translation query;
        if (qb instanceof InfinoKnnQueryBuilder knn) {
            searched(cols, knn.field(), true, InfinoKnnQueryBuilder.NAME);
            vector = VectorRequest.of(false, null, null, knn.field(), knn.vector(), knn.k());
            query = new Translation(table, InfinoQueryTranslator.ALL, null);
        } else if (qb instanceof InfinoHybridQueryBuilder hybrid) {
            searched(cols, hybrid.textColumn(), false, InfinoHybridQueryBuilder.NAME);
            searched(cols, hybrid.vectorColumn(), true, InfinoHybridQueryBuilder.NAME);
            vector = VectorRequest.of(true, hybrid.textColumn(), hybrid.textQuery(), hybrid.vectorColumn(), hybrid.vector(), hybrid.k());
            query = new Translation(table, InfinoQueryTranslator.ALL, null);
        } else if (InfinoStockKnn.is(qb)) {
            vector = stockKnn(InfinoStockKnn.read(qb), cols, table, nowMillis);
            query = new Translation(table, InfinoQueryTranslator.ALL, null);
            if (!builders(source).isEmpty()) {
                // Stock aggregates over the k nearest; the table would aggregate over every row.
                throw new IllegalArgumentException("[infino] aggregations are not served with a knn query");
            }
            if (source.sorts() != null && source.sorts().stream().anyMatch(s -> !(s instanceof ScoreSortBuilder))) {
                // Stock orders the k nearest by the sort; the engine hands them back by distance only.
                throw new IllegalArgumentException("[infino] a sort other than _score is not served with a knn query");
            }
        } else {
            query = InfinoQueryTranslator.translate(qb, cols, table, nowMillis);
            if (query == null) {
                throw new IllegalArgumentException("[infino] query not served by the table: " + qb);
            }
        }
        // The document-level rule is one more required predicate: hits, total
        // and aggregations all run over the same relation, so one clause covers
        // them. The rule may name fields the request may not, so it is read
        // over the table's full columns.
        String rule = access.documentsWhere(fullCols, table, nowMillis);
        if (rule != null) {
            if (vector != null) {
                throw InfinoAccess.refused("a vector query under document-level security is not served; the engine's vector search takes no predicate");
            }
            String where = InfinoQueryTranslator.ALL.equals(query.where()) ? rule : "(" + query.where() + ") AND " + rule;
            query = query.withWhere(where);
        }
        log.debug("[infino] query {} -> FROM {} WHERE {}", qb, query.from(), query.where());
        List<SortSpec> sorts = sorts(source.sorts(), cols, InfinoNative.floatFields(table), InfinoNative.intFields(table), nowMillis, true);
        if (sorts.stream().anyMatch(SortSpec::isScore)) {
            checkScoreSort(source, sorts, query, vector != null);
        }
        if (sorts.isEmpty() && sortsByDocFirst(source)) {
            // Stock orders such a page by the index and scores nothing, so
            // the clauses' scores never reorder it.
            query = query.unscored();
        } else if (sorts.isEmpty() && sortsByScoreAscending(source)) {
            query = query.ascending();
        }
        String postWhere = null;
        if (source.postFilter() != null) {
            if (vector != null) {
                throw new IllegalArgumentException("[infino] post_filter is not served with a vector query");
            }
            postWhere = InfinoQueryTranslator.predicate(source.postFilter(), cols, table, nowMillis);
            if (postWhere == null) {
                throw new IllegalArgumentException("[infino] post_filter not served by the table: " + source.postFilter());
            }
        }
        Column collapse = null;
        if (source.collapse() != null) {
            CollapseBuilder cb = source.collapse();
            if (cb.getInnerHits() != null && !cb.getInnerHits().isEmpty()) {
                throw new IllegalArgumentException("[infino] collapse with inner_hits is not served by the table");
            }
            if (vector != null) {
                throw new IllegalArgumentException("[infino] collapse is not served with a vector query");
            }
            collapse = columnFor(cols, cb.getField());
            if (collapse == null) {
                throw new IllegalArgumentException("[infino] collapse names field [" + cb.getField() + "], which the table has no column for");
            }
            if (collapse.masked()) {
                throw InfinoAccess.refused("collapse on [" + cb.getField() + "], a field masked for this user, is not served");
            }
        }
        List<ScriptedField> scriptFields = new ArrayList<>();
        if (source.scriptFields() != null && !source.scriptFields().isEmpty()) {
            if (vector != null) {
                throw new IllegalArgumentException("[infino] script_fields are not served with a vector query");
            }
            // A script reads values themselves, so it sees no masked column.
            Map<String, Column> scriptCols = InfinoAccess.scriptColumns(cols);
            for (SearchSourceBuilder.ScriptField sf : source.scriptFields()) {
                Sql sql = InfinoScriptTranslator.translate(sf.script(), scriptCols, null, nowMillis);
                scriptFields.add(new ScriptedField(sf.fieldName(), sql.expr(), "sf" + scriptFields.size()));
            }
        }
        // A sorted request orders by the engine's row id last, so rows sharing
        // a sort value have one order and a page boundary falls between rows,
        // never through a value; a scroll's next page continues from it. A
        // request sorted by the document id already has one order.
        if (!sorts.isEmpty() && sorts.stream().noneMatch(s -> s.name().equals(InfinoQueryTranslator.OSID))) {
            sorts.add(SortSpec.rowId());
        }
        Object[] searchAfter = source.searchAfter();
        if (searchAfter != null) {
            if (sorts.isEmpty()) {
                throw new IllegalArgumentException("[infino] search_after needs a sort on fields");
            }
            // The request's own sorts, or those plus the id the previous page handed back.
            if (searchAfter.length != sorts.size() && searchAfter.length != sorts.size() - 1) {
                throw new IllegalArgumentException("[infino] search_after has " + searchAfter.length + " values for " + sorts.size() + " sort fields");
            }
            if (source.from() > 0) {
                throw new IllegalArgumentException("[infino] search_after cannot be combined with from");
            }
            if (vector != null) {
                throw new IllegalArgumentException("[infino] search_after is not served with a vector query");
            }
            searchAfter = searchAfter.clone();
            for (int i = 0; i < searchAfter.length; i++) {
                if (searchAfter[i] != null && sorts.get(i).missingStandIn(searchAfter[i])) {
                    searchAfter[i] = null;
                }
            }
        }
        Plan plan = new Plan(table, cols, query, sorts, vector, postWhere, collapse, searchAfter, scriptFields, access, nowMillis);
        InfinoAggregationProcessor.check(builders(source), plan);
        return plan;
    }

    /** Whether the hits of {@code query} score by its text ({@link Plan#scored}). */
    private static boolean scored(Translation query) {
        Scoring scoring = query.scoring();
        return query.ranked() != null || scoring.reorders() || scoring.unrankable() || scoring.across() != null;
    }

    /**
     * Refuse a sort mixing {@code _score} with a field that the table cannot
     * order as stock does. Under a query whose hits all score alike it orders
     * by the fields alone, which is served. A scored query is read ranked by
     * its score, best first, the fields ordering hits that tie on it
     * ({@link #scoredStatement}): the score must be the first sort, and
     * descending, and the query's score one the table ranks beside fields
     * (not a query string across several text columns, which only its own
     * ranked search scores, {@link #acrossStatement}). A collapse picks
     * each group's first by a score its statement does not compute, a
     * search_after resumes from a score no keyset can compare against, a
     * field named {@code score} would share the score's column, and a vector
     * query's hits come back by distance alone.
     */
    private static void checkScoreSort(SearchSourceBuilder source, List<SortSpec> sorts, Translation query, boolean vector) {
        String what = "[infino] a sort mixing _score with a field";
        if (vector) {
            throw new IllegalArgumentException(what + " is not served with a vector query");
        }
        if (!scored(query)) {
            return;
        }
        Scoring scoring = query.scoring();
        if (!(source.sorts().get(0) instanceof ScoreSortBuilder first)) {
            throw new IllegalArgumentException(what + " is not served unless _score is the first sort of a scored query");
        }
        if (first.order() != SortOrder.DESC) {
            throw new IllegalArgumentException(what + " is not served with _score ascending on a scored query");
        }
        if (scoring.unrankable()) {
            throw new IllegalArgumentException(what + " is not served for a query whose score the table does not rank");
        }
        if (scoring.across() != null) {
            // The summed BM25 ranks only an unsorted page, so the hits would score apart from the max_score.
            throw new IllegalArgumentException(what + " is not served for a query whose text clauses' scores are summed");
        }
        if (source.collapse() != null) {
            throw new IllegalArgumentException(what + " is not served with collapse on a scored query");
        }
        if (source.searchAfter() != null) {
            throw new IllegalArgumentException(what + " is not served with search_after on a scored query");
        }
        if (sorts.stream().anyMatch(s -> !s.isScore() && s.name().equals(SCORE))) {
            throw new IllegalArgumentException(what + " is not served beside a field named [" + SCORE + "] on a scored query");
        }
    }

    /**
     * Check the column a vector or hybrid query searches against the columns
     * the request may see: the engine's search reads the column itself, so a
     * field hidden from the user is as if unmapped, and one masked for the
     * user is refused, as a sort on it is. {@code vector} says which kind of
     * column the query needs: a vector one, else a text one.
     */
    private static void searched(Map<String, Column> cols, String field, boolean vector, String query) {
        Column c = field == null ? null : cols.get(field);
        if (c == null || (vector ? !c.vector() : !c.fts())) {
            throw new IllegalArgumentException(
                "[infino] [" + query + "] names field [" + field + "], which the table has no " + (vector ? "vector" : "text") + " column for"
            );
        }
        if (c.masked()) {
            throw InfinoAccess.refused("[" + query + "] on [" + field + "], a field masked for this user, is not served");
        }
    }

    /**
     * The vector request of the k-NN plugin's {@code knn} query. Its filter
     * becomes the predicate the engine narrows the search with, so the k
     * nearest come from the rows the filter keeps, as the k-NN plugin's
     * filtered search returns them; a filter that cannot be such a predicate
     * is refused with the clause named.
     */
    private static VectorRequest stockKnn(InfinoStockKnn.Query knn, Map<String, Column> cols, String table, long nowMillis) {
        searched(cols, knn.field(), true, InfinoStockKnn.NAME);
        Column c = cols.get(knn.field());
        String filter = null;
        if (knn.filter() != null) {
            filter = InfinoQueryTranslator.vectorFilter(knn.filter(), cols, table, nowMillis);
            if (filter == null) {
                throw new IllegalArgumentException("[infino] knn filter not served by the table: " + knn.filter());
            }
            if (InfinoQueryTranslator.ALL.equals(filter)) {
                filter = null; // a filter that keeps every row narrows nothing
            }
        }
        return new VectorRequest(false, null, null, c.name(), knn.vector(), knn.k(), true, filter);
    }

    /**
     * The statement a filtered {@code knn} query runs: the engine's
     * {@code vector_search} table function with the filter as its
     * {@code WHERE}, which the engine pushes into the search, so the k
     * nearest are ranked among the rows the filter keeps rather than cut
     * from an unfiltered top k.
     */
    static String filteredKnnStatement(String table, VectorRequest v) {
        String csv = floatsToJson(v.vector());
        csv = csv.substring(1, csv.length() - 1);
        return "SELECT " + InfinoQueryTranslator.OSID + ", " + InfinoQueryTranslator.SOURCE + ", score FROM vector_search("
            + InfinoQueryTranslator.literal(table)
            + ", "
            + InfinoQueryTranslator.literal(v.vectorColumn())
            + ", "
            + InfinoQueryTranslator.literal(csv)
            + ", "
            + v.k()
            + ") WHERE "
            + v.filter()
            + " ORDER BY score ASC LIMIT "
            + v.k();
    }

    /**
     * The field sorts over columns, in request order; {@code _score} and
     * {@code _doc} add nothing, and {@code _seq_no} orders by the sequence
     * numbers the table keeps. Throws when a sort names something the table
     * has no column for. {@code nowMillis} is what a script's clock reads.
     */
    static List<SortSpec> sorts(List<SortBuilder<?>> sorts, Map<String, Column> cols, long nowMillis) {
        return sorts(sorts, cols, Set.of(), nowMillis, false);
    }

    /**
     * {@link #sorts(List, Map, long)}, where {@code floatFields} are the
     * fields mapped {@code float} or {@code half_float}, whose sort's
     * {@code missing} value stock reads as a 32-bit float.
     */
    static List<SortSpec> sorts(List<SortBuilder<?>> sorts, Map<String, Column> cols, Set<String> floatFields, long nowMillis) {
        return sorts(sorts, cols, floatFields, nowMillis, false);
    }

    /**
     * {@link #sorts(List, Map, Set, long)}. With
     * {@code withScore} a {@code _score} sort beside a field or script sort
     * is one too ({@link SortSpec#score}), in its place: a hit then carries
     * its score among its sort values, as stock's does.
     */
    static List<SortSpec> sorts(List<SortBuilder<?>> sorts, Map<String, Column> cols, Set<String> floatFields, long nowMillis, boolean withScore) {
        return sorts(sorts, cols, floatFields, Set.of(), nowMillis, withScore);
    }

    /**
     * {@link #sorts(List, Map, Set, long, boolean)}, where {@code intFields}
     * are the fields mapped {@code integer}, {@code short} or {@code byte},
     * whose missing value stock stands in as a 32-bit int
     * ({@link SortSpec#standIn}).
     */
    static List<SortSpec> sorts(
        List<SortBuilder<?>> sorts,
        Map<String, Column> cols,
        Set<String> floatFields,
        Set<String> intFields,
        long nowMillis,
        boolean withScore
    ) {
        List<SortSpec> out = new ArrayList<>();
        if (sorts == null) {
            return out;
        }
        for (SortBuilder<?> sort : sorts) {
            if (sort instanceof ScoreSortBuilder ss) {
                if (withScore && handedOut(sort, sorts)) {
                    out.add(SortSpec.score(ss.order() == SortOrder.DESC));
                }
                continue;
            }
            if (sort instanceof ScriptSortBuilder ss) {
                // A script sort orders by the script as a column expression; a script sees no masked column.
                Sql sql = InfinoScriptTranslator.translate(ss.script(), InfinoAccess.scriptColumns(cols), null, nowMillis);
                out.add(new SortSpec(sql.expr(), "sort" + out.size(), sql.type(), ss.order() == SortOrder.DESC));
                continue;
            }
            if (!(sort instanceof FieldSortBuilder fs)) {
                throw new IllegalArgumentException("[infino] sort not served by the table: " + sort);
            }
            if (FieldSortBuilder.DOC_FIELD_NAME.equals(fs.getFieldName())) {
                continue;
            }
            if (SeqNoFieldMapper.NAME.equals(fs.getFieldName())) {
                // The job sweepers of alerting and its kin page their jobs in this order.
                out.add(SortSpec.seqNo(fs.order() == SortOrder.DESC));
                continue;
            }
            Column c = columnFor(cols, fs.getFieldName());
            if (c == null) {
                throw new IllegalArgumentException("[infino] sort names field [" + fs.getFieldName() + "], which the table has no column for");
            }
            if (c.masked()) {
                // The order of the values would show through the hashes.
                throw InfinoAccess.refused("sort on [" + fs.getFieldName() + "], a field masked for this user, is not served");
            }
            boolean desc = fs.order() == SortOrder.DESC;
            boolean asFloat = floatFields.contains(InfinoQueryTranslator.fieldOf(c.name()));
            boolean asInt = intFields.contains(InfinoQueryTranslator.fieldOf(c.name()));
            Column values = c.isList() ? c : InfinoQueryTranslator.listOf(cols, c);
            if (values == null) {
                out.add(SortSpec.of(c, desc).asInt(asInt).withMissing(fs.missing(), asFloat, nowMillis));
                continue;
            }
            SortMode mode = fs.sortMode();
            if (mode != null && mode != SortMode.MIN && mode != SortMode.MAX) {
                throw new IllegalArgumentException("[infino] sort mode [" + mode + "] on [" + fs.getFieldName() + "], which documents give several values, is not served");
            }
            // Stock's default mode is the least value ascending and the greatest
            // descending; an explicit min or max picks that value either way.
            boolean greatest = mode == null ? desc : mode == SortMode.MAX;
            out.add(SortSpec.ofValues(c, values, greatest, desc).asInt(asInt).withMissing(fs.missing(), asFloat, nowMillis));
        }
        return out;
    }

    /** ORDER BY terms for the sorts: each expression in its direction, missing values where the sort puts them. */
    static List<String> orderBy(List<SortSpec> sorts) {
        List<String> out = new ArrayList<>(sorts.size());
        for (SortSpec s : sorts) {
            if (s.isScore()) {
                continue; // a column of no row: only a scored statement orders by it
            }
            out.add(s.expr() + (s.desc() ? " DESC" : " ASC") + (s.nullsFirst() ? " NULLS FIRST" : " NULLS LAST"));
        }
        return out;
    }

    static Column columnFor(Map<String, Column> cols, String field) {
        if ("_id".equals(field)) {
            // The document id is the id column: a sort or collapse on it is served there. A field
            // named like the id column is looked up by that name instead.
            Column id = cols.get(InfinoQueryTranslator.OSID);
            return id == null || id.name().equals(InfinoQueryTranslator.OSID)
                ? id
                : new Column(InfinoQueryTranslator.OSID, "str", true, false);
        }
        Column c = cols.get(field);
        if (c == null && field.endsWith(".keyword")) {
            Column base = cols.get(field.substring(0, field.length() - ".keyword".length()));
            if (base != null && base.isString()) {
                return base;
            }
        }
        return c;
    }

    static Collection<AggregationBuilder> builders(SearchSourceBuilder source) {
        return source.aggregations() == null ? List.of() : source.aggregations().getAggregatorFactories();
    }

    // ---- one index ----

    /**
     * The total: what the query and post_filter keep, before any collapse
     * and regardless of where the page starts, counted up to {@code cap}
     * (see {@link #count(String, Translation, long)}).
     */
    private static long total(Plan plan, long cap) {
        Translation q = plan.query();
        return plan.postWhere() == null ? count(plan.table(), q, cap) : count(plan.table(), q.withWhere(plan.hitWhere()), cap);
    }

    /** A page's rows, and its exact total when it was counted before them (null otherwise). */
    record CountedRows(List<Map<String, Object>> rows, Long total) {}

    /**
     * Whether a page's exact total is counted before its rows: a scored page
     * ({@link #scoredPage}), the costliest statement to plan, when the
     * request counts an exact total anyway ({@code count} and its
     * {@code track_total_hits}).
     */
    static boolean countsFirst(SearchSourceBuilder source, boolean count, boolean scored) {
        return scored && count && trackTotal(source);
    }

    /**
     * A page's rows from {@code hits}, its total from {@code total} first
     * when {@code countFirst}: a total of 0 leaves no hit to read, and the
     * hit statement is not planned at all.
     */
    static CountedRows countedRows(boolean countFirst, LongSupplier total, Supplier<List<Map<String, Object>>> hits) {
        if (!countFirst) {
            return new CountedRows(hits.get(), null);
        }
        long counted = total.getAsLong();
        return new CountedRows(counted == 0 ? List.of() : hits.get(), counted);
    }

    /**
     * Whether a page's exact total is counted beside its rows: an exact
     * total the request counts, not counted first, for a page whose short
     * read cannot be the total ({@link #shortPageIsTotal}). Where it can, the
     * total is counted after the rows, and only when the page is full.
     */
    static boolean countsBeside(Plan plan, boolean exact, boolean count, int size, boolean firstCount) {
        return plan.vector() == null && exact && count && size > 0 && !firstCount && !shortPageIsTotal(plan);
    }

    /**
     * A total counted beside the page, never below the page's own rows: the
     * count and the hit read are separate snapshots, so an append between
     * them can show a row the count did not see.
     */
    static long besideTotal(long counted, int rows) {
        return Math.max(counted, rows);
    }

    /** How many times a page whose row lost its source between its two reads is read again. */
    static final int SOURCE_REREADS = 2;

    /**
     * The page's rows from its hit statement, with their sources when they
     * are read by id; {@code resumed} as {@link #page} takes it. How many rows
     * the statement chose goes in {@code chosen}.
     */
    private static List<Map<String, Object>> hitRows(
        Plan plan,
        SearchSourceBuilder source,
        int fetch,
        boolean seqNoAndPrimaryTerm,
        boolean resumed,
        int sourceFrom,
        int[] chosen
    ) {
        if (!seqNoAndPrimaryTerm && !resumed) {
            // Before the presence count readsUnsorted makes: the range index
            // reads rows missing the value last, in row id order, so a column
            // no row has a value in needs no count either. Its rows come with
            // their sources in the same call.
            List<Map<String, Object>> indexed = rangeIndexRows(plan, source, fetch);
            if (indexed != null) {
                chosen[0] = indexed.size();
                return indexed;
            }
        }
        Supplier<List<Map<String, Object>>> choose = () -> chosenRows(plan, source, fetch, seqNoAndPrimaryTerm, resumed);
        if (!readsSourcesById(plan, source, resumed)) {
            List<Map<String, Object>> rows = choose.get();
            chosen[0] = rows.size();
            return rows;
        }
        return sourcedRows(choose, rows -> withSources(plan.table(), rows, sourceFrom, fetch), fetch, chosen);
    }

    /**
     * The rows {@code choose} reads, each given its source by {@code source}.
     * A row deleted between the two reads has no source left and drops out:
     * the page is then read again, up to {@link #SOURCE_REREADS} times, and
     * after the last try goes without that row. How many rows the last read
     * chose, before any dropped out, goes in {@code chosen}: a page that lost
     * a row is still a full page, and only a short choice ends the hits.
     */
    static List<Map<String, Object>> sourcedRows(
        Supplier<List<Map<String, Object>>> choose,
        Function<List<Map<String, Object>>, List<Map<String, Object>>> source,
        int fetch,
        int[] chosen
    ) {
        for (int read = 0;; read++) {
            List<Map<String, Object>> rows = choose.get();
            chosen[0] = rows.size();
            List<Map<String, Object>> sourced = source.apply(rows);
            if (sourced.size() == Math.min(rows.size(), fetch) || read == SOURCE_REREADS) {
                return sourced;
            }
        }
    }

    /** The rows the page's hit statement chooses, without the sources read by id. */
    private static List<Map<String, Object>> chosenRows(
        Plan plan,
        SearchSourceBuilder source,
        int fetch,
        boolean seqNoAndPrimaryTerm,
        boolean resumed
    ) {
        if (readsUnsorted(plan, resumed)) {
            return InfinoNative.sqlRows(unsortedStatement(plan, fetch, seqNoAndPrimaryTerm, wantsSource(source)));
        }
        List<Map<String, Object>> rows;
        if (splitsMissingTail(plan)) {
            // The rows past a present first sort value come first; the
            // rows missing it follow them all, so they are read only
            // when the first part leaves the page short.
            rows = InfinoNative.sqlRows(hitStatement(plan, fetch, seqNoAndPrimaryTerm, Keyset.PRESENT));
            if (rows.size() < fetch) {
                rows = new ArrayList<>(rows);
                rows.addAll(InfinoNative.sqlRows(hitStatement(plan, fetch - rows.size(), seqNoAndPrimaryTerm, Keyset.MISSING)));
            }
        } else {
            rows = windowedRows(plan, fetch, seqNoAndPrimaryTerm, !sourcesById(plan));
        }
        return rows;
    }

    /**
     * The rows of {@code plan}'s hit statement, each with its source when
     * {@code withSource}: every candidate ranked when a ranked window could
     * have cut off one that belongs among them ({@link #windowCutsPage}), so
     * the rows are the ones stock returns.
     */
    private static List<Map<String, Object>> windowedRows(Plan plan, int fetch, boolean withSeq, boolean withSource) {
        List<Map<String, Object>> rows = InfinoNative.sqlRows(hitStatement(plan, fetch, withSeq, Keyset.WHOLE, RANK_WINDOW, withSource));
        if (windowCutsPage(plan, rows, fetch)) {
            TextClause text = rankedText(plan);
            long candidates = InfinoNative.count(plan.table(), text.column(), text.query(), text.mode());
            if (candidates > RANK_WINDOW) {
                int window = (int) Math.min(Integer.MAX_VALUE, candidates);
                rows = InfinoNative.sqlRows(hitStatement(plan, fetch, withSeq, Keyset.WHOLE, window, withSource));
            }
        }
        return rows;
    }

    /**
     * Whether the page's hits carry scores its read does not compute: those
     * of a scored query ({@link Plan#scored}) sorted by fields alone under
     * {@code track_scores}, which stock scores as it would unsorted. Such a
     * page is read in the fields' order with no score, so its hits' scores
     * are read apart ({@link #trackedScores}), and its max_score is the best
     * score of every match ({@link #bestScore}), as stock weighs it.
     */
    static boolean scoresBesideSort(Plan plan, SearchSourceBuilder source) {
        return source.trackScores() && plan.scored() && !plan.sorts().isEmpty() && !plan.ranksByScore();
    }

    /**
     * Whether the hit statement of {@code plan}, unsorted, selects each hit's
     * score: a ranked text clause, scoring clauses that reorder it, or a
     * query string ranked across several text columns ({@link #ranksAcross}).
     */
    private static boolean readsScore(Plan plan) {
        Translation q = plan.query();
        return q.ranked() != null || q.scoring().reorders() || ranksAcross(plan);
    }

    /**
     * The score of each of the page's rows from {@code start} up to
     * {@code end} by id, as the query's unsorted page reads it (less the
     * offset every hit takes alike; the rows before {@code start} sit
     * before every window and need none): one read of the query's hit statement, unsorted
     * ({@link Plan#unsorted}), its predicate narrowed to those ids. The
     * predicate keeps the document-level rule and the read's visibility cut,
     * so each id is read as the page read it; the engine ranks within the ids
     * (a plain predicate it applies before ranking), so none falls past the
     * window. Null when the unsorted page, post_filter and all, reads no score ({@link #readsScore}):
     * every hit then scores the constant, as the unsorted page's do. An id
     * deleted since the page was read has no score.
     */
    private static Map<String, Object> trackedScores(Plan plan, List<Map<String, Object>> rows, int start, int end) {
        Translation q = plan.query();
        List<String> ids = new ArrayList<>();
        for (int i = start; i < end; i++) {
            Object id = rows.get(i).get(InfinoQueryTranslator.OSID);
            if (id != null) {
                ids.add(id.toString());
            }
        }
        if (!readsScore(plan.unsorted(q, plan.postWhere()))) {
            return null;
        }
        Map<String, Object> scores = new HashMap<>();
        if (ids.isEmpty()) {
            return scores;
        }
        List<String> literals = new ArrayList<>(ids.size());
        for (String id : ids) {
            literals.add(InfinoQueryTranslator.literal(id));
        }
        String idsIn = InfinoQueryTranslator.OSID + " IN (" + String.join(", ", literals) + ")";
        String where = InfinoQueryTranslator.ALL.equals(q.where()) ? idsIn : idsIn + " AND (" + q.where() + ")";
        Plan byId = plan.unsorted(q.withWhere(where), null);
        int n = ids.size();
        for (Map<String, Object> row : InfinoNative.sqlRows(hitStatement(byId, n, false, Keyset.WHOLE, Math.max(RANK_WINDOW, n), false))) {
            Object id = row.get(InfinoQueryTranslator.OSID);
            if (id != null) {
                scores.put(id.toString(), row.get(SCORE));
            }
        }
        return scores;
    }

    /**
     * The best score of every hit of {@code plan}'s query, as its unsorted
     * page scores its first hit ({@link Plan#unsorted}): no sort, collapse or
     * search_after narrows it, as none narrows the max_score stock weighs.
     * NaN when no row matches.
     */
    private static float bestScore(Plan plan) {
        Translation q = plan.query();
        List<Map<String, Object>> best = windowedRows(plan.unsorted(q, plan.postWhere()), 1, false, false);
        if (best.isEmpty()) {
            return Float.NaN;
        }
        return best.get(0).get(SCORE) instanceof Number n ? (float) (n.doubleValue() + q.scoring().offset()) : constantScore(q.scoring());
    }

    /**
     * The page read from a range index with no statement planned, or null
     * for the hit statement: every row sorted on one range-indexed field
     * ({@link #indexSorted}). The bridge answers null when the column has no
     * range index, and the hit statement reads the page then.
     */
    private static List<Map<String, Object>> rangeIndexRows(Plan plan, SearchSourceBuilder source, int fetch) {
        boolean withSource = wantsSource(source);
        if (indexSorted(plan)) {
            SortSpec first = plan.sorts().get(0);
            return InfinoNative.sortedRows(plan.table(), first.name(), first.desc(), fetch, withSource);
        }
        return null;
    }

    /**
     * The hits of one index for the request's window, each carrying its
     * source, with the total. With {@code count} false an exact total is left
     * for the caller, which reads it beside the aggregations (the page's total
     * is then 0 until replaced).
     */
    static Page page(Index index, Plan plan, SearchSourceBuilder source, boolean count) {
        return page(index, plan, source, count, false, false);
    }

    /**
     * As {@link #page(Index, Plan, SearchSourceBuilder, boolean)}. With
     * {@code capTotal} the total is counted only as far as the request's
     * {@code track_total_hits} limit needs (see {@link #countCap}), which a
     * scroll, whose later rounds carry the first round's total, does not ask
     * for. With {@code resumed} a later page resumes after this page's last
     * hit from its sort values (a scroll's): those must then carry the row id
     * the sort is broken by, so the page is read in its sort's order even
     * when that sort orders nothing ({@link #sortsNothing}).
     */
    static Page page(Index index, Plan plan, SearchSourceBuilder source, boolean count, boolean capTotal, boolean resumed) {
        return page(index, plan, source, count, capTotal, resumed, 0);
    }

    /**
     * As {@link #page(Index, Plan, SearchSourceBuilder, boolean, boolean, boolean)}.
     * A caller whose window starts at {@code sourceFrom} (one index's or one
     * shard's search) never shows the hits before it, so when the sources are
     * read by id those hits come without one ({@link Page#unsourced}).
     */
    static Page page(Index index, Plan plan, SearchSourceBuilder source, boolean count, boolean capTotal, boolean resumed, int sourceFrom) {
        final int from = Math.max(0, source.from());
        final int size = source.size() < 0 ? DEFAULT_SIZE : source.size();
        final int fetch = from + size;
        // The rows before the caller's window, whose sources are not read.
        final int unsourcedRows = readsSourcesById(plan, source, resumed) ? Math.min(Math.max(0, sourceFrom), from) : 0;
        final Translation q = plan.query();
        List<Map<String, Object>> rows;
        boolean knnDistance = false;
        // A knn query's total is how many of its k came back, so it runs even for an empty page.
        boolean stockKnn = plan.vector() != null && plan.vector().stock();
        // A knn query's hits score as the k-NN plugin scores them, under the
        // field's space type; one the table cannot score is refused before the search.
        String space = stockKnn ? InfinoStockKnn.spaceType(mapping(index), plan.vector().vectorColumn()) : null;
        boolean seqNoAndPrimaryTerm = Boolean.TRUE.equals(source.seqNoAndPrimaryTerm());
        // A vector hit's sequence number, by id, when the request asks for it: the
        // vector entry points return ids and scores only.
        Map<String, Long> vectorSeqs = Map.of();
        boolean exact = trackTotal(source);
        // Every count of this page's total stops where track_total_hits needs it to (see countCap).
        long cap = capTotal ? countCap(source) : Long.MAX_VALUE;
        // A scored page counts its exact total before its rows (see countsFirst):
        // a total of 0 plans no page at all.
        boolean firstCount = plan.vector() == null
            && size > 0
            && !(bareRanked(plan) && !seqNoAndPrimaryTerm)
            && countsFirst(source, count, scoredPage(plan));
        // Otherwise, when a short page cannot be its own total, the exact total
        // and the page are separate engine calls, neither waiting on the other:
        // the total is counted on another thread while the page is read.
        CompletableFuture<Long> beside = countsBeside(plan, exact, count, size, firstCount)
            ? InfinoServices.beside(() -> total(plan, cap))
            : null;
        // The exact total, when it is read before the page.
        Long counted = null;
        boolean shortPage = false;
        // How many rows the hit statement chose, before any lost its source.
        int[] chosen = { 0 };
        try {
            if (size == 0 && !stockKnn) {
                rows = List.of();
            } else if (plan.vector() != null) {
                VectorRequest v = plan.vector();
                String emb = floatsToJson(v.vector());
                // The engine's top k for a knn query (the page is a window over them),
                // otherwise as many as the page reaches. Either way each hit comes
                // back with its id, score and source.
                int want = stockKnn ? v.k() : Math.max(fetch, 1);
                if (v.filter() != null) {
                    rows = InfinoNative.sqlRows(filteredKnnStatement(plan.table(), v));
                } else {
                    String json = v.hybrid()
                        ? InfinoNative.hybrid(plan.table(), v.textColumn(), v.textQuery(), v.vectorColumn(), emb, want)
                        : InfinoNative.knn(plan.table(), v.vectorColumn(), emb, want);
                    rows = InfinoNative.rows(json);
                }
                knnDistance = !v.hybrid();
                if (seqNoAndPrimaryTerm) {
                    // The vector entry points return ids, scores and sources; the page's
                    // sequence numbers come in one statement.
                    List<String> ids = new ArrayList<>();
                    for (int i = from; i < Math.min(rows.size(), fetch); i++) {
                        Object id = rows.get(i).get(InfinoQueryTranslator.OSID);
                        if (id != null) {
                            ids.add(id.toString());
                        }
                    }
                    vectorSeqs = InfinoNative.storedSeqs(plan.table(), ids);
                }
            } else if (bareRanked(plan) && !seqNoAndPrimaryTerm) {
                // A text query and nothing else: the engine's BM25 call returns
                // the page with its sources, so no statement is planned for it. It
                // returns no sequence numbers, so a request for them takes the hit statement.
                TextClause text = q.ranked();
                rows = InfinoNative.bm25Hits(plan.table(), text.column(), text.query(), text.mode(), fetch);
            } else {
                CountedRows read = countedRows(
                    firstCount,
                    () -> total(plan, cap),
                    () -> hitRows(plan, source, fetch, seqNoAndPrimaryTerm, resumed, unsourcedRows, chosen)
                );
                rows = read.rows();
                counted = read.total();
                shortPage = chosen[0] < fetch && shortPageIsTotal(plan);
            }
        } catch (RuntimeException | Error e) {
            // A page that fails waits out its count rather than leave it running past the request.
            InfinoServices.drain(beside, e);
            throw e;
        }

        long total;
        if (plan.vector() != null) {
            total = rows.size(); // kNN is inherently top-k
        } else if (exact && !count) {
            total = 0;
        } else if (exact && counted != null) {
            total = counted;
        } else if (exact && beside != null) {
            // Already counting: it is not left running past the request.
            total = besideTotal(InfinoServices.await(beside), rows.size());
        } else if (exact && shortPage) {
            total = rows.size();
        } else if (exact) {
            total = total(plan, cap);
        } else {
            total = rows.size();
        }

        boolean withSource = returnsSource(source);
        // What the mapping's _source keeps of each document: the table keeps it whole.
        List<FieldAndFormat> docValueFields = requestedFields(source.docValueFields(), null);
        List<FieldAndFormat> fetchFields = requestedFields(null, source.fetchFields());
        InfinoMappingSource stored = withSource || !fetchFields.isEmpty() ? InfinoMappingSource.of(mapping(index)) : InfinoMappingSource.WHOLE;
        boolean version = Boolean.TRUE.equals(source.version());
        SearchShardTarget target = target(index);
        List<SearchHit> hits = new ArrayList<>();
        float maxScore = Float.NaN;
        String collapseKey = plan.collapse() == null ? null : freeAlias(plan, COLLAPSE_KEY);
        // Every row up to the window's end: the window itself is applied once,
        // over the merged pages, as a coordinator applies it over its shards.
        int end = Math.min(rows.size(), fetch);
        // Under track_scores, a scored query sorted by fields alone is read
        // in the fields' order, with no score: its hits' scores are read
        // apart (see scoresBesideSort), for the rows a window can hold.
        boolean tracked = scoresBesideSort(plan, source);
        Map<String, Object> trackedScores = tracked ? trackedScores(plan, rows, unsourcedRows, end) : null;
        Map<String, List<String>> ignored = ignored(plan.query().table(), plan.cols(), rows, end);
        int unsourced = 0;
        for (int i = 0; i < end; i++) {
            Map<String, Object> row = rows.get(i);
            Object id = row.get(InfinoQueryTranslator.OSID);
            if (id == null) {
                continue;
            }
            if (i < unsourcedRows) {
                unsourced++;
            }
            List<String> ignoredFields = ignored.getOrDefault(id.toString(), List.of());
            SearchHit h = new SearchHit(i, id.toString(), Map.of(), ignoredField(ignoredFields));
            h.shard(target);
            float score = Float.NaN;
            // Only a scored or vector read ranks a score column; any other
            // read's "score" is a document field of that name.
            Object s = tracked
                ? (trackedScores == null ? null : trackedScores.get(id.toString()))
                : plan.vector() != null || plan.scored() ? row.get(SCORE) : null;
            // What the clauses every hit matches add to each hit's score alike.
            double offset = q.scoring().offset();
            if (s instanceof Number n) {
                score = n.floatValue();
                if (stockKnn) {
                    score = InfinoStockKnn.score(space, score);
                } else if (knnDistance) {
                    score = 1.0f / (1.0f + score);
                } else if (plan.vector() == null) {
                    score = (float) (n.doubleValue() + offset);
                }
            } else if (tracked ? trackedScores == null : plan.sorts().isEmpty() || plan.vector() == null && !plan.scored()) {
                score = constantScore(q.scoring());
            }
            // What max_score weighs: every hit's score; on a sorted page only
            // under track_scores or a sort led by the score, best first, as
            // stock weighs it. A sorted hit carries its score under
            // track_scores or a sort by the score, which stock scores every
            // hit for, the sort values carrying it too.
            float weighed = score;
            if (plan.vector() == null && !plan.sorts().isEmpty()) {
                SortSpec first = plan.sorts().get(0);
                boolean scores = source.trackScores() || plan.sorts().stream().anyMatch(SortSpec::isScore);
                weighed = source.trackScores() || first.isScore() && first.desc() ? score : Float.NaN;
                h.score(scores ? score : Float.NaN);
            } else {
                h.score(score);
            }
            if (!Float.isNaN(weighed)) {
                maxScore = Float.isNaN(maxScore) ? weighed : Math.max(maxScore, weighed);
            }
            // The sequence number the document was written under, and the version
            // and primary term the table reports (see InfinoDocumentVersion).
            if (version) {
                h.version(InfinoDocumentVersion.VERSION);
            }
            if (seqNoAndPrimaryTerm) {
                h.setSeqNo(
                    plan.vector() != null
                        ? vectorSeqs.getOrDefault(id.toString(), InfinoDocumentVersion.UNTRACKED_SEQ_NO)
                        : InfinoNative.seq(row.get(InfinoQueryTranslator.SEQ))
                );
                h.setPrimaryTerm(InfinoDocumentVersion.PRIMARY_TERM);
            }
            if (i >= unsourcedRows && (withSource || !docValueFields.isEmpty() || !fetchFields.isEmpty())) {
                String src = String.valueOf(row.get(InfinoQueryTranslator.SOURCE));
                // The source as the user may see it: hidden fields gone, masked values hashed.
                src = plan.access().read(id.toString(), src);
                if (src != null && !src.isEmpty()) {
                    // A disabled source is refused on the hits a response carries,
                    // not here: the page holds every row up to the window's end.
                    if (withSource) {
                        BytesReference shown = stored.shown(src, source.fetchSource());
                        if (shown != null) {
                            h.sourceRef(shown);
                        }
                    }
                    fillRequested(h, src, stored, docValueFields, fetchFields, plan.cols(), ignoredFields);
                }
            }
            if (!plan.sorts().isEmpty()) {
                Object[] values = new Object[plan.sorts().size()];
                DocValueFormat[] formats = new DocValueFormat[values.length];
                for (int k = 0; k < values.length; k++) {
                    SortSpec sort = plan.sorts().get(k);
                    // A missing value carries the stand-in stock's comparator gives it.
                    values[k] = sort.isScore() ? (Object) score : sort.orStandIn(row.get(sort.name()));
                    formats[k] = DocValueFormat.RAW;
                }
                h.sortValues(values, formats);
            }
            if (plan.collapse() != null) {
                // A collapsed hit carries its collapse value, as stock does.
                String field = plan.collapse().name();
                Object value = row.get(collapseKey);
                h.setDocumentField(field, new DocumentField(field, value == null ? List.of() : List.of(value)));
            }
            for (ScriptedField sf : plan.scriptFields()) {
                // A scripted field comes back under its name, as stock's fetch phase returns it.
                Object value = row.get(sf.alias());
                h.setDocumentField(sf.name(), new DocumentField(sf.name(), value == null ? List.of() : List.of(value)));
            }
            hits.add(h);
        }
        if (sortsByScoreAscending(source) && !source.trackScores()) {
            // Stock keeps a max_score only for a sort that leads with the score, best first.
            maxScore = Float.NaN;
        }
        if (tracked && size > 0) {
            // Stock weighs every match's score, not the page's alone.
            float best = bestScore(plan);
            maxScore = Float.isNaN(maxScore) ? best : Float.isNaN(best) ? maxScore : Math.max(maxScore, best);
        }
        return new Page(hits, total, exact || plan.vector() != null, maxScore, unsourced, Math.max(hits.size(), chosen[0]));
    }

    /**
     * The score every hit of a query that ranks nothing carries, as Lucene
     * scores it: what its scoring clauses add to each hit alike
     * ({@code offset}, a must term 1.0), else 1.0.
     */
    static float constantScore(double offset) {
        return offset > 0 ? (float) offset : 1.0f;
    }

    /** The score every hit of a query scored by {@code scoring} alike carries: 0 when no clause scores ({@link Scoring#zero}), else {@link #constantScore(double)}. */
    static float constantScore(Scoring scoring) {
        return scoring.zero() ? 0.0f : constantScore(scoring.offset());
    }

    /**
     * Whether the page's hits are the principal's BM25 ranking cut to
     * {@link #RANK_WINDOW} candidates before a step removes rows
     * ({@link #hitStatement}), so a page that step leaves short may have
     * matches past the window.
     */
    static boolean rankedInWindow(Plan plan) {
        String where = plan.hitWhere();
        return plan.searchAfter() == null
            && plan.sorts().isEmpty()
            && !scoredPage(plan)
            && plan.query().ranked() != null
            && !ranksAcross(plan)
            && (InfinoQueryTranslator.joinsAfterRanking(where) || (!InfinoQueryTranslator.ALL.equals(where) && plan.collapse() != null));
    }

    /** The text clause a ranked or scored statement ranks: the ranked principal, else the optional text clause (null when none). */
    private static TextClause rankedText(Plan plan) {
        return rankedText(plan.query());
    }

    private static TextClause rankedText(Translation q) {
        return q.ranked() != null ? q.ranked() : q.scoring().text();
    }

    /**
     * Whether {@link #scoredStatement} over {@code where} removes rows after
     * its ranked reads: the predicate, less the optional text clause it
     * ranks, or a bonus holds a clause the engine applies above a ranked
     * search ({@link InfinoQueryTranslator#joinsAfterRanking}), or a bonus
     * reads a relation joined to the ranked rows.
     */
    static boolean narrowsAfterRanking(Translation q, String where) {
        TextClause text = rankedText(q);
        if (text == null) {
            return false;
        }
        for (Bonus b : q.scoring().bonuses()) {
            if (b.join() != null || InfinoQueryTranslator.joinsAfterRanking(b.expr())) {
                return true;
            }
        }
        String match = q.ranked() != null ? null : q.textMatch(text);
        return InfinoQueryTranslator.joinsAfterRanking(match == null ? where : where.replace(match, InfinoQueryTranslator.ALL));
    }

    /** How many ranked bands {@link #scoredStatement} reads for {@code q}: one per sum its bonuses take, or one for them all past {@link #MAX_SCORE_BANDS}. */
    static int rankedBands(Translation q) {
        List<Double> sums = bonusSums(q.scoring().bonuses());
        return sums == null ? 1 : sums.size();
    }

    /**
     * Whether the page's ranked reads may have cut off a hit that belongs on
     * it. A page ranked in the window before a step removes rows
     * ({@link #rankedInWindow}) may when that step leaves it short. A page
     * read by {@link #scoredStatement}, sorted by {@code _score} then fields
     * ({@link Plan#ranksByScore}) or ranked by more than one clause
     * ({@link #scoredPage}), may when a cut-off hit could score as much as
     * its last hit (at most the statement's {@link #SCORE_FLOOR}) and win
     * the tie, or, on a page left short, be the hit that fills it. Where a
     * step removes rows after the ranked reads, the page may also when it is
     * short or empty, or when a band kept no row ({@link #SCORE_BANDS}):
     * those its window cut off are bounded by nothing it kept.
     */
    static boolean windowCutsPage(Plan plan, List<Map<String, Object>> rows, int fetch) {
        if (rankedInWindow(plan)) {
            return rows.size() < fetch;
        }
        if (!(plan.ranksByScore() || scoredPage(plan)) || rankedText(plan) == null) {
            return false;
        }
        Translation q = plan.query();
        if (narrowsAfterRanking(q, plan.hitWhere())) {
            if (rows.size() < fetch || !rows.isEmpty() && (!(rows.get(0).get(SCORE_BANDS) instanceof Number bands) || bands.intValue() < rankedBands(q))) {
                return true;
            }
        }
        if (rows.isEmpty()) {
            return false;
        }
        Map<String, Object> last = rows.get(rows.size() - 1);
        if (!(last.get(SCORE_FLOOR) instanceof Number floor)) {
            return false;
        }
        return rows.size() < fetch || last.get(SCORE) instanceof Number score && score.doubleValue() <= floor.doubleValue();
    }

    /**
     * Whether a page the hit statement could not fill holds every row its
     * predicate keeps, and that predicate is the total's, so its length is
     * the total: no keyset, collapse or ranked window narrows it. An
     * optional text clause ranks its matches in a window too
     * ({@link #scoredStatement}), though the plan has no principal.
     */
    static boolean shortPageIsTotal(Plan plan) {
        return plan.query().ranked() == null
            && plan.query().scoring().text() == null
            && plan.searchAfter() == null
            && plan.collapse() == null;
    }

    /**
     * Whether the page's hits are the BM25 ranking of the request's text
     * clause and nothing more: no other predicate (a filter, a post_filter, a
     * security rule), no sort, no search_after, no collapse and no scripted
     * field. {@link #hitStatement} would then select exactly what the engine's
     * BM25 call returns.
     */
    static boolean bareRanked(Plan plan) {
        return plan.query().ranked() != null
            && !plan.query().scoring().reorders()
            && plan.query().scoring().across() == null
            && InfinoQueryTranslator.ALL.equals(plan.hitWhere())
            && plan.sorts().isEmpty()
            && plan.searchAfter() == null
            && plan.collapse() == null
            && plan.scriptFields().isEmpty();
    }

    /**
     * Whether the page is every row sorted on one field's own column, ties
     * broken by the row id: the engine's sorted search, which reads it from
     * the column's range index. A predicate (a filter, a post_filter, a
     * security rule), a text query, a vector, a search_after, a collapse, a
     * scripted field, a sort on an expression rather than a column, or
     * missing: _first takes the hit statement.
     */
    static boolean indexSorted(Plan plan) {
        List<SortSpec> sorts = plan.sorts();
        if (plan.vector() != null
            || plan.query().principal() != null
            || !InfinoQueryTranslator.ALL.equals(plan.hitWhere())
            || plan.searchAfter() != null
            || plan.collapse() != null
            || !plan.scriptFields().isEmpty()
            || sorts.size() != 2) {
            return false;
        }
        SortSpec first = sorts.get(0);
        SortSpec tie = sorts.get(1);
        return tie.name().equals(InfinoSql.ROW_ID)
            && !tie.desc()
            && !first.name().equals(InfinoSql.ROW_ID)
            && !first.name().equals(InfinoQueryTranslator.OSID)
            && first.expr().equals(InfinoQueryTranslator.quoteIdent(first.name()))
            // The engine puts the rows missing the value last; missing: _first is the statement's (NULLS FIRST).
            && !first.nullsFirst();
    }

    /**
     * Whether the page's sources are read by id once its rows are chosen,
     * rather than in the hit statement: a sorted page without a collapse.
     * The sort weighs every row the predicate keeps, and every row that ties
     * on the first key competes on the next, so a source in the statement
     * would be read for each of those rows to keep a page's worth.
     */
    static boolean sourcesById(Plan plan) {
        return !plan.sorts().isEmpty() && plan.collapse() == null;
    }

    /**
     * Whether {@link #hitRows} reads this page's sources by id after its
     * rows: a sorted page's ({@link #sourcesById}), or a page ranked across
     * text columns, whose ranked search returns no source ({@link #ranksAcross}).
     */
    private static boolean readsSourcesById(Plan plan, SearchSourceBuilder source, boolean resumed) {
        return plan.vector() == null && !readsUnsorted(plan, resumed) && (sourcesById(plan) || ranksAcross(plan)) && wantsSource(source);
    }

    /** Whether a hit's response carries its source. */
    private static boolean returnsSource(SearchSourceBuilder source) {
        if (source.fetchSource() != null) {
            return source.fetchSource().fetchSource();
        }
        // As stock: stored_fields asked for without _source (or _none_) leave the source out.
        StoredFieldsContext stored = source.storedFields();
        if (stored == null) {
            return true;
        }
        return stored.fetchFields() && stored.fieldNames() != null && stored.fieldNames().contains(SourceFieldMapper.NAME);
    }

    /** Whether a hit needs its source: to return it, or to read requested fields from it. */
    private static boolean wantsSource(SearchSourceBuilder source) {
        return returnsSource(source) || !requestedFields(source.docValueFields(), source.fetchFields()).isEmpty();
    }

    /**
     * {@code rows} up to {@code fetch}, those from {@code sourceFrom} on each
     * with its source from the table, looked up by its id; the rows before it
     * stay as they are. A row from {@code sourceFrom} on whose id the table no
     * longer holds (deleted since the rows were chosen) is left out.
     */
    private static List<Map<String, Object>> withSources(String table, List<Map<String, Object>> rows, int sourceFrom, int fetch) {
        return withSources(rows, sourceFrom, fetch, ids -> InfinoNative.sources(table, ids));
    }

    /** As {@link #withSources(String, List, int, int)}, with the sources read by {@code lookup}. */
    static List<Map<String, Object>> withSources(
        List<Map<String, Object>> rows,
        int sourceFrom,
        int fetch,
        Function<List<String>, Map<String, String>> lookup
    ) {
        int end = Math.min(rows.size(), fetch);
        int start = Math.min(Math.max(0, sourceFrom), end);
        List<Map<String, Object>> page = rows.subList(start, end);
        List<String> ids = new ArrayList<>(page.size());
        for (Map<String, Object> row : page) {
            Object id = row.get(InfinoQueryTranslator.OSID);
            if (id != null) {
                ids.add(id.toString());
            }
        }
        Map<String, String> sources = ids.isEmpty() ? Map.of() : lookup.apply(ids);
        List<Map<String, Object>> out = new ArrayList<>(rows.subList(0, start));
        for (Map<String, Object> row : page) {
            Object id = row.get(InfinoQueryTranslator.OSID);
            if (id == null) {
                out.add(row);
                continue;
            }
            String found = sources.get(id.toString());
            if (found == null) {
                continue;
            }
            Map<String, Object> withSource = new HashMap<>(row);
            withSource.put(InfinoQueryTranslator.SOURCE, found);
            out.add(withSource);
        }
        return out;
    }

    /**
     * The statement that fetches the page's hits: each hit's id, its source
     * unless {@link #sourcesById} reads it afterwards, sort values, its
     * sequence number when {@code withSeq} (the request asks for
     * {@code seq_no_primary_term}) and, under a collapse, its collapse value.
     * A sorted request
     * orders the filtered relation; a text-ranked one orders the engine's
     * ranked candidates by score; anything else is a filtered scan with no
     * order, which the engine reads in file order under a page's
     * {@code LIMIT}, so pages at growing offsets follow one another, or,
     * where the filter is joined by id or the rows come from the token index,
     * the same rows ordered by the row id (see {@link #unsortedStatement}). A
     * search_after narrows the relation to the rows past the given sort values
     * (a keyset, so no earlier page is scanned), and a collapse keeps the first
     * row of each collapse value in the request's order, through a window.
     */
    static String hitStatement(Plan plan, int fetch, boolean withSeq) {
        return hitStatement(plan, fetch, withSeq, Keyset.WHOLE);
    }

    /** Which of a search_after's rows a hit statement reads (see {@link #splitsMissingTail}). */
    enum Keyset {
        /** Every row past the search_after values. */
        WHOLE,
        /** The rows past them whose first sort value is present. */
        PRESENT,
        /** The rows missing the first sort value, which sort after every present one. */
        MISSING
    }

    /**
     * Whether the page is read as the unsorted search reads it: its sort
     * orders nothing ({@link #sortsNothing}), so every hit's sort value is
     * missing and the page is the predicate's rows in file order (in row id
     * order where the read is not a plain scan, see {@link #unsortedStatement}),
     * as stock reads a sort on a field no document has in its index's order. The hits
     * still carry the sort. Not when a later page resumes from the hits' sort values
     * ({@code resumed}, a scroll): the unsorted read selects no row id, so
     * every value would be missing and the keyset past them would keep no
     * row; that page is read in the sort's order, the row id's.
     */
    static boolean readsUnsorted(Plan plan, boolean resumed) {
        return !resumed && sortsNothing(plan);
    }

    /**
     * The statement {@link #readsUnsorted} reads a page with: the sorted
     * statement's rows ({@link #hitStatement}, unranked, as a field sort
     * selects them) with no order, each with its source when
     * {@code withSource} (the hit returns it or reads requested fields from
     * it). A text query is neither ranked by BM25 nor scored, so the page is
     * the rows the sorted read would select, not the best-scoring ones.
     *
     * <p>A plain scan of the table has no order: the engine reads a scan
     * under a {@code LIMIT} with no order in file order, so while the
     * table's files stay the same, two pages of one search start where the
     * last one ended, and the read stops at its LIMIT, where ordering by the
     * row id would read the id of every row the filter keeps. Any other read
     * (a filter joined by id, rows from the token index) is ordered by the
     * row id ({@link #ROW_ID_ORDER}): the engine builds the join from every
     * row of the table before the first comes out, and emits them in no
     * fixed order, so the order costs a top-{@code LIMIT} sort of rows it
     * has already read and no further read.
     */
    static String unsortedStatement(Plan plan, int fetch, boolean withSeq, boolean withSource) {
        String hit = InfinoQueryTranslator.OSID
            + (withSource ? ", " + InfinoQueryTranslator.SOURCE : "")
            + (withSeq ? ", " + InfinoQueryTranslator.SEQ : "");
        Unranked read = unranked(plan.query(), plan.hitWhere());
        String order = read.scansTable(plan.query().table()) ? "" : " ORDER BY " + ROW_ID_ORDER;
        return "SELECT " + hit + " FROM " + read.from() + " WHERE " + read.where() + order + " LIMIT " + fetch;
    }

    /** The relation and predicate a page with no score reads its rows from. */
    record Unranked(String from, String where) {
        /**
         * Whether this is a plain scan of {@code table}: the table itself,
         * filtered with no subquery by id ({@link InfinoQueryTranslator#joinsById}).
         */
        boolean scansTable(String table) {
            return from.equals(InfinoQueryTranslator.quoteIdent(table)) && !InfinoQueryTranslator.joinsById(where);
        }
    }

    /**
     * What a page with no score (a sorted or an unsorted one) reads: a
     * plain-words text clause is a predicate on the table's rows
     * ({@code text_match}), so the engine reads rows in the page's order and
     * checks it on each, and a LIMIT stops the read, rather than collecting
     * every row the token index matches first. Any other text clause keeps
     * its token index relation.
     */
    static Unranked unranked(Translation q, String where) {
        String text = q.principalPredicate();
        return text == null ? new Unranked(q.from(), where) : new Unranked(InfinoQueryTranslator.quoteIdent(q.table()), "(" + where + ") AND " + text);
    }

    /**
     * Whether the page's sort orders nothing: one field's own column, then
     * the row id, from the page's start, over a column no row of the table
     * holds a value in (where every value is missing, missing first or last
     * and either direction order the same). The column's presence is counted
     * from the superfiles' statistics, with no row read.
     */
    static boolean sortsNothing(Plan plan) {
        List<SortSpec> sorts = plan.sorts();
        if (plan.vector() != null || plan.collapse() != null || plan.searchAfter() != null || !plan.scriptFields().isEmpty() || sorts.size() != 2) {
            return false;
        }
        SortSpec first = sorts.get(0);
        SortSpec tie = sorts.get(1);
        if (!tie.name().equals(InfinoSql.ROW_ID)
            || tie.desc()
            || first.name().equals(InfinoSql.ROW_ID)
            || first.name().equals(InfinoQueryTranslator.OSID)
            || !first.expr().equals(InfinoQueryTranslator.quoteIdent(first.name()))) {
            return false;
        }
        long[] present = InfinoNative.countWhere(plan.table(), List.of(first.expr() + " IS NOT NULL"));
        return present.length == 1 && present[0] == 0;
    }

    /**
     * Whether a search_after page is read in two parts: the rows past a
     * present first sort value, then, only when those leave the page short,
     * the rows missing that value. Missing values sort last, so the second
     * part follows the first whole, and the first part is a plain range on
     * the sort column, which the engine answers from its range index; one
     * statement holding both would weigh {@code s < v OR s IS NULL} on every
     * row. Not under a collapse, whose window ranks a value's rows across
     * both parts; not under {@code missing: _first}, whose missing rows
     * come before every present one; and not under a {@code missing}
     * value, which leaves no row missing the sort's value.
     */
    static boolean splitsMissingTail(Plan plan) {
        Object[] after = plan.searchAfter();
        if (after == null || after.length == 0 || after[0] == null || plan.collapse() != null) {
            return false;
        }
        SortSpec first = plan.sorts().get(0);
        return !first.name().equals(InfinoSql.ROW_ID) && !first.nullsFirst() && !first.noneMissing();
    }

    /**
     * {@code base}, or that with as many leading underscores as it takes to
     * be a name no column, sort or scripted field of the plan selects or
     * reads: an alias of the hit statement's own, which a field of the same
     * name would make ambiguous.
     */
    static String freeAlias(Plan plan, String base) {
        Set<String> taken = new HashSet<>(plan.cols().keySet());
        taken.add(InfinoQueryTranslator.OSID);
        taken.add(InfinoQueryTranslator.SOURCE);
        taken.add(InfinoQueryTranslator.SEQ);
        for (SortSpec s : plan.sorts()) {
            taken.add(s.name());
        }
        for (ScriptedField sf : plan.scriptFields()) {
            taken.add(sf.alias());
        }
        String alias = base;
        while (taken.contains(alias)) {
            alias = "_" + alias;
        }
        return alias;
    }

    static String hitStatement(Plan plan, int fetch, boolean withSeq, Keyset part) {
        return hitStatement(plan, fetch, withSeq, part, RANK_WINDOW);
    }

    /**
     * As {@link #hitStatement(Plan, int, boolean, Keyset)}, ranking
     * {@code window} candidates where a step removes rows after the ranking.
     */
    static String hitStatement(Plan plan, int fetch, boolean withSeq, Keyset part, int window) {
        return hitStatement(plan, fetch, withSeq, part, window, !sourcesById(plan));
    }

    /**
     * As {@link #hitStatement(Plan, int, boolean, Keyset, int)}, each hit
     * with its source when {@code withSource}.
     */
    static String hitStatement(Plan plan, int fetch, boolean withSeq, Keyset part, int window, boolean withSource) {
        Translation q = plan.query();
        StringBuilder hit = new StringBuilder(InfinoQueryTranslator.OSID);
        if (withSource) {
            hit.append(", ").append(InfinoQueryTranslator.SOURCE);
        }
        if (withSeq) {
            hit.append(", ").append(InfinoQueryTranslator.SEQ);
        }
        for (SortSpec s : plan.sorts()) {
            if (s.name().equals(InfinoQueryTranslator.OSID) || s.isScore()) {
                continue; // the id is selected already; the score is no column
            }
            hit.append(", ").append(s.expr()).append(" AS ").append(InfinoQueryTranslator.quoteIdent(s.name()));
        }
        if (plan.collapse() != null) {
            // Under its own alias: a sort on the column under a missing value selects the stand-in under the column's name.
            hit.append(", ")
                .append(InfinoQueryTranslator.quoteIdent(plan.collapse().name()))
                .append(" AS ")
                .append(InfinoQueryTranslator.quoteIdent(freeAlias(plan, COLLAPSE_KEY)));
        }
        for (ScriptedField sf : plan.scriptFields()) {
            hit.append(", ").append(sf.expr()).append(" AS ").append(sf.alias());
        }
        String where = plan.hitWhere();
        if (plan.searchAfter() != null) {
            where = "(" + where + ") AND " + keysetPredicate(plan, part);
        }
        boolean sorted = !plan.sorts().isEmpty();
        if (ranksAcross(plan)) {
            return acrossStatement(plan, fetch, withSeq);
        }
        if (scoredPage(plan) || plan.ranksByScore()) {
            return scoredStatement(plan, hit.toString(), where, fetch, withSeq, window, withSource);
        }
        boolean ranked = !sorted && q.ranked() != null;
        String from;
        String order;
        if (sorted) {
            // A sorted page has no score to rank by: the engine's top-k reads rows
            // in the sort column's order (see unranked).
            Unranked read = unranked(q, where);
            from = read.from();
            where = read.where();
            order = String.join(", ", plan.orderBy());
        } else if (ranked) {
            // The engine ranks within the rows the predicate keeps, so the page
            // is all it needs to rank; only a step that removes rows after the
            // ranking needs more candidates to choose from.
            boolean filtered = !InfinoQueryTranslator.ALL.equals(where);
            boolean narrowedAfterRanking = InfinoQueryTranslator.joinsAfterRanking(where) || (filtered && plan.collapse() != null);
            from = q.rankedFrom(narrowedAfterRanking ? window : fetch);
            hit.append(", score");
            // Hits that tie on the score come in the order they were written.
            // Stock orders them by segment, then by place in the segment,
            // which is the order they were written only until its segments
            // merge: a force-merge can reorder its ties, where the row id
            // keeps them in write order, a re-indexed document last. The
            // ranked search keeps that order, but a sort by the score alone
            // need not.
            order = "score DESC, " + InfinoQueryTranslator.quoteIdent(InfinoSql.ROW_ID) + " ASC";
        } else {
            // No sort and no score: a plain scan is left in the engine's file
            // order, a read joined by id is ordered by the row id (see
            // unsortedStatement).
            from = q.from();
            order = new Unranked(from, where).scansTable(q.table()) ? null : ROW_ID_ORDER;
        }
        if (plan.collapse() == null) {
            return "SELECT " + hit + " FROM " + from + " WHERE " + where + (order == null ? "" : " ORDER BY " + order) + " LIMIT " + fetch;
        }
        // One row per collapse value: the first in the request's order, or in
        // the row id's when the request names none. The window's rows come
        // out grouped by value, not in file order, so the page orders them
        // the same way, and pages at growing offsets follow one another.
        String within = order == null ? ROW_ID_ORDER : order;
        String partition = InfinoQueryTranslator.quoteIdent(plan.collapse().name());
        String rank = InfinoQueryTranslator.quoteIdent(freeAlias(plan, COLLAPSE_RANK));
        return "SELECT " + hit + " FROM (SELECT t.*, ROW_NUMBER() OVER (PARTITION BY " + partition + " ORDER BY " + within
            + ") AS " + rank + " FROM " + from + " t WHERE " + where + ") WHERE " + rank + " = 1"
            + " ORDER BY " + within + " LIMIT " + fetch;
    }

    /**
     * Whether the page is the summed BM25 of several text clauses
     * ({@link InfinoQueryTranslator.Across}: a query string across several
     * text columns, or a bool's match clauses beside its principal), in no
     * other order, read by {@link #acrossStatement}: no sort, collapse,
     * search_after or scripted field, and every other predicate one the
     * ranked searches take as their filter (none joined by id after the
     * ranking). Otherwise a ranked principal ranks the page alone.
     */
    static boolean ranksAcross(Plan plan) {
        InfinoQueryTranslator.Across across = plan.query().scoring().across();
        if (across == null
            || (across.match() == null) != (plan.query().principal() != null)
            || !plan.sorts().isEmpty()
            || plan.collapse() != null
            || plan.searchAfter() != null
            || !plan.scriptFields().isEmpty()) {
            return false;
        }
        if (across.match() != null && !plan.hitWhere().contains(across.match())) {
            return false;
        }
        String filter = acrossFilter(plan);
        if (filter == null) {
            return false;
        }
        // The summed search takes a text_match filter into the rows it ranks,
        // so beside a principal only a join by id falls after the ranking.
        return across.match() == null ? !InfinoQueryTranslator.joinsById(filter) : !InfinoQueryTranslator.joinsAfterRanking(filter);
    }

    /**
     * The filter each ranked search takes: the hits' predicate without the
     * query string's own, or, beside a principal, the hits' predicate and
     * the principal's own ({@link Translation#principalPredicate}), which
     * the summed search does not keep by itself. Null when the principal
     * has no such predicate.
     */
    private static String acrossFilter(Plan plan) {
        Translation q = plan.query();
        String where = plan.hitWhere();
        String match = q.scoring().across().match();
        if (match != null) {
            return where.replace(match, InfinoQueryTranslator.ALL);
        }
        String principal = q.principalPredicate();
        if (principal == null) {
            return null;
        }
        return InfinoQueryTranslator.ALL.equals(where) ? principal : "(" + principal + ") AND (" + where + ")";
    }

    /**
     * The page of a query string ranked across several text columns, or of
     * a bool's match clauses beside its principal, as stock scores it: the
     * engine's {@code bm25_search_fields} ranks within the rows the filter
     * keeps ({@link #acrossFilter}), each clause by its best column
     * (dis_max) and the sum over the clauses a document matches. The
     * engine's top-k is exact for any number of clauses and matches, and
     * ties go in the order the documents were written. The sources are read
     * by id after.
     */
    static String acrossStatement(Plan plan, int fetch, boolean withSeq) {
        Translation q = plan.query();
        String filter = acrossFilter(plan);
        return "SELECT " + InfinoQueryTranslator.OSID + ", score, " + InfinoQueryTranslator.quoteIdent(InfinoSql.ROW_ID) + " AS infino_row"
            + (withSeq ? ", " + InfinoQueryTranslator.SEQ : "") + " FROM " + q.rankedFrom(q.scoring().across(), fetch)
            + (InfinoQueryTranslator.ALL.equals(filter) ? "" : " WHERE " + filter) + " ORDER BY score DESC, infino_row ASC LIMIT " + fetch;
    }

    /**
     * Whether the page is ranked by more than the principal's BM25
     * ({@link Scoring#reorders}), in no other order, and read by
     * {@link #scoredStatement}.
     */
    static boolean scoredPage(Plan plan) {
        return plan.sorts().isEmpty() && plan.collapse() == null && plan.searchAfter() == null && plan.query().scoring().reorders();
    }

    /** As {@link #scoredStatement(Plan, String, String, int, boolean, int)}, ranking {@link #RANK_WINDOW} candidates. */
    static String scoredStatement(Plan plan, String hit, String where, int fetch, boolean withSeq) {
        return scoredStatement(plan, hit, where, fetch, withSeq, RANK_WINDOW);
    }

    /**
     * The statement for a page ranked by more than one clause's BM25
     * ({@link Scoring#reorders}), or by a scored query's score then fields
     * ({@link Plan#ranksByScore}): each hit scores the BM25 of the text clause
     * where it matches, plus what each other scoring clause adds where it
     * matches ({@code infino_bonus}, a SQL number per row), as Lucene sums a
     * bool's clauses; the page is the hits with the greatest sums. The
     * engine ranks by BM25 alone, so the hits are cut into bands that share
     * one bonus (the bonus takes few values: the sums of its clauses'
     * scores) and each band's best by BM25 is ranked within it, the band's
     * bounds riding with the ranked search as its filter; the page is the
     * best of those. When the text clause is optional (a should beside no
     * ranked clause), the hits it does not match score their bonus alone,
     * the best of them read by it. With no text clause at all, the bonus is
     * the score.
     *
     * <p>Sorted by the score then fields, hits that tie on the score come in
     * the fields' order, so a band's ranked search reads {@code window}
     * candidates rather than the page: the hits it cuts off score no more
     * than its last, and those tied with the page's last could still come
     * before it. Each hit carries the greatest score such a cut-off hit could
     * have, as {@link #SCORE_FLOOR}, for the caller to rank every candidate
     * when the page reaches it ({@link #windowCutsPage}).
     */
    static String scoredStatement(Plan plan, String hit, String where, int fetch, boolean withSeq, int window) {
        return scoredStatement(plan, hit, where, fetch, withSeq, window, !sourcesById(plan));
    }

    /** As {@link #scoredStatement(Plan, String, String, int, boolean, int)}, {@code hit} selecting each hit's source when {@code withSource}. */
    static String scoredStatement(Plan plan, String hit, String where, int fetch, boolean withSeq, int window, boolean withSource) {
        Translation q = plan.query();
        Scoring scoring = q.scoring();
        boolean sorted = !plan.sorts().isEmpty();
        String rowId = InfinoQueryTranslator.quoteIdent(InfinoSql.ROW_ID);
        List<String> names = new ArrayList<>();
        names.add(InfinoQueryTranslator.OSID);
        if (withSource) {
            names.add(InfinoQueryTranslator.SOURCE);
        }
        if (withSeq) {
            names.add(InfinoQueryTranslator.SEQ);
        }
        // The fields' order among hits that tie on the score, by the aliases the hits select them under.
        List<String> fieldOrder = new ArrayList<>();
        for (SortSpec s : plan.sorts()) {
            if (s.isScore()) {
                continue;
            }
            String alias = InfinoQueryTranslator.quoteIdent(s.name());
            if (!s.name().equals(InfinoQueryTranslator.OSID)) {
                names.add(alias);
            }
            fieldOrder.add(alias + (s.desc() ? " DESC" : " ASC") + (s.nullsFirst() ? " NULLS FIRST" : " NULLS LAST"));
        }
        for (ScriptedField sf : plan.scriptFields()) {
            names.add(sf.alias());
        }
        String tieOrder = sorted ? String.join(", ", fieldOrder) : "infino_row ASC";
        List<String> parts = new ArrayList<>();
        // The relations the bonuses read, joined to each branch's rows by id.
        StringBuilder joins = new StringBuilder();
        for (Bonus b : scoring.bonuses()) {
            parts.add("(" + b.expr() + ")");
            if (b.join() != null) {
                joins.append(b.join());
            }
        }
        String bonus = parts.isEmpty() ? "0.0" : String.join(" + ", parts);
        TextClause text = rankedText(q);
        // A band whose rows a step removes after its ranked read may have
        // used up its window while candidates remain past it.
        boolean afterRanking = narrowsAfterRanking(q, where);
        // Each branch's columns past the hit's; a ranked band's carry its
        // number, so its cut can be told where it matters: under a sort by
        // the score then fields, or a step after the ranking.
        boolean banded = sorted || afterRanking;
        String unbanded = banded ? ", -1 AS infino_band" : "";
        List<String> branches = new ArrayList<>();
        int k = window;
        // The greatest score a hit a band's ranked read cut off could have,
        // past the least of those it read: a band's hits share their bonus.
        String bandFloorOf = "MIN(infino_bm25 + infino_bonus) OVER (PARTITION BY infino_band)";
        if (text == null) {
            branches.add(
                "SELECT " + hit + ", 0.0 AS infino_bm25, " + bonus + " AS infino_bonus, " + rowId + " AS infino_row" + unbanded + " FROM " + q.from() + joins + " WHERE " + where
            );
        } else {
            // An optional text clause is a subquery in the predicate: true of every
            // row its ranked search returns, false of every other.
            String match = q.ranked() != null ? null : q.textMatch(text);
            String ranked = match == null ? where : where.replace(match, InfinoQueryTranslator.ALL);
            List<Double> sums = bonusSums(scoring.bonuses());
            if (sums == null) {
                // Too many bands: one ranked read of the window, as one band.
                // A hit it cut off scores no more than its least BM25 plus
                // the greatest bonus any hit can take.
                double maxBonus = 0;
                for (Bonus b : scoring.bonuses()) {
                    maxBonus += Math.max(0, b.values().stream().mapToDouble(Double::doubleValue).max().orElse(0));
                }
                bandFloorOf = "MIN(infino_bm25) OVER (PARTITION BY infino_band) + " + InfinoQueryTranslator.scoreLiteral(maxBonus);
                branches.add(
                    "SELECT " + hit + ", score AS infino_bm25, " + bonus + " AS infino_bonus, " + rowId + " AS infino_row" + (banded ? ", 0 AS infino_band" : "") + " FROM "
                        + q.rankedFrom(text, window) + joins + " WHERE " + ranked
                );
            } else {
                k = sorted || afterRanking ? window : fetch;
                for (int i = 0; i < sums.size(); i++) {
                    // The band around this sum: from halfway to the one below, to halfway to the one above.
                    List<String> band = new ArrayList<>();
                    if (!InfinoQueryTranslator.ALL.equals(ranked)) {
                        band.add("(" + ranked + ")");
                    }
                    if (i > 0) {
                        band.add("(" + bonus + ") >= " + InfinoQueryTranslator.scoreLiteral((sums.get(i - 1) + sums.get(i)) / 2));
                    }
                    if (i < sums.size() - 1) {
                        band.add("(" + bonus + ") < " + InfinoQueryTranslator.scoreLiteral((sums.get(i) + sums.get(i + 1)) / 2));
                    }
                    branches.add(
                        "SELECT " + hit + ", score AS infino_bm25, " + bonus + " AS infino_bonus, " + rowId + " AS infino_row" + (banded ? ", " + i + " AS infino_band" : "")
                            + " FROM " + q.rankedFrom(text, k) + joins + (band.isEmpty() ? "" : " WHERE " + String.join(" AND ", band))
                    );
                }
            }
            if (match != null) {
                String rest = "(" + where.replace(match, InfinoQueryTranslator.NONE) + ") AND " + InfinoQueryTranslator.not(match);
                branches.add(
                    "SELECT * FROM (SELECT " + hit + ", 0.0 AS infino_bm25, " + bonus + " AS infino_bonus, " + rowId + " AS infino_row" + unbanded + " FROM " + q.from() + joins
                        + " WHERE " + rest + " ORDER BY infino_bonus DESC, " + tieOrder + " LIMIT " + fetch + ") AS infino_rest"
                );
            }
        }
        String union = String.join(" UNION ALL ", branches);
        // Hits that tie on score come in the fields' order, else in the order
        // they were written, as stock orders them by their place in the index.
        String order = "score " + (scoring.ascending() ? "ASC" : "DESC") + ", " + tieOrder;
        if (!banded) {
            return "SELECT " + String.join(", ", names) + ", infino_bm25 + infino_bonus AS score FROM (" + union + ") AS infino_scored ORDER BY " + order + " LIMIT " + fetch;
        }
        // A band whose ranked read came back full (or was narrowed after it,
        // so may have been) cut hits scoring no more than its least. A
        // band's least kept row bounds those, as the least it read does.
        String cut = afterRanking ? "TRUE" : "COUNT(*) OVER (PARTITION BY infino_band) >= " + k;
        String bandFloor = "CASE WHEN infino_band >= 0 AND " + cut + " THEN " + bandFloorOf + " END";
        // Narrowed after the ranking, a band that kept no row bounds nothing:
        // each band that kept one counts once, by its first row.
        String seen = afterRanking
            ? ", CASE WHEN infino_band >= 0 AND infino_row = MIN(infino_row) OVER (PARTITION BY infino_band) THEN 1 ELSE 0 END AS infino_band_seen"
            : "";
        String bands = afterRanking ? ", SUM(infino_band_seen) OVER () AS " + SCORE_BANDS : "";
        return "SELECT " + String.join(", ", names) + ", score, MAX(infino_band_floor) OVER () AS " + SCORE_FLOOR + bands + " FROM (SELECT *, infino_bm25 + infino_bonus AS score, "
            + bandFloor + " AS infino_band_floor" + seen + " FROM (" + union + ") AS infino_scored) AS infino_banded ORDER BY " + order + " LIMIT " + fetch;
    }

    /**
     * Every sum the bonuses can take on a row, ascending: 0, and each sum of
     * one score from any of them. Null past {@link #MAX_SCORE_BANDS}.
     */
    static List<Double> bonusSums(List<Bonus> bonuses) {
        java.util.TreeSet<Double> sums = new java.util.TreeSet<>(List.of(0.0));
        for (Bonus b : bonuses) {
            java.util.TreeSet<Double> next = new java.util.TreeSet<>(sums);
            for (double s : sums) {
                for (double v : b.values()) {
                    next.add(s + v);
                }
            }
            sums = next;
            if (sums.size() > MAX_SCORE_BANDS) {
                return null;
            }
        }
        // Sums that differ only by rounding are one band.
        List<Double> out = new ArrayList<>();
        for (double s : sums) {
            if (out.isEmpty() || s - out.get(out.size() - 1) > 1e-9) {
                out.add(s);
            }
        }
        return out;
    }

    /**
     * The rows after the search_after values in the request's sort order:
     * {@code (s1 > v1) OR (s1 = v1 AND s2 > v2) OR ...}, each comparison
     * turned for a descending sort. By default the order puts missing values
     * last, so after a present value the missing rows still follow ({@code
     * OR s IS NULL}), and after a missing one only the deeper sorts decide.
     * Under {@code missing: _first} the missing rows come before every
     * present one, so after a missing value every present row follows ({@code
     * s IS NOT NULL}), and after a present one the missing rows are behind.
     * A missing value compares equal to itself. {@code part} keeps the rows
     * whose first sort value is present, or only those missing it.
     */
    private static String keysetPredicate(Plan plan, Keyset part) {
        if (part == Keyset.MISSING) {
            return plan.sorts().get(0).expr() + " IS NULL";
        }
        List<String> alternatives = new ArrayList<>();
        List<String> equal = new ArrayList<>();
        long now = plan.nowMillis();
        for (int i = 0; i < plan.searchAfter().length; i++) {
            SortSpec s = plan.sorts().get(i);
            String col = s.expr();
            Object value = plan.searchAfter()[i];
            String strict;
            String same;
            if (s.isScore()) {
                // Only a query whose hits all score alike resumes past a score
                // (see checkScoreSort): every row carries the one constant.
                float score = constantScore(plan.query().scoring());
                float after = value instanceof Number n ? n.floatValue() : value == null ? Float.NaN : Float.parseFloat(value.toString());
                boolean past = s.desc() ? score < after : score > after;
                strict = past ? InfinoQueryTranslator.ALL : InfinoQueryTranslator.NONE;
                same = score == after ? InfinoQueryTranslator.ALL : InfinoQueryTranslator.NONE;
            } else if (value == null) {
                strict = s.nullsFirst() ? col + " IS NOT NULL" : InfinoQueryTranslator.NONE;
                same = col + " IS NULL";
            } else {
                // A date sort's value as text is read by its field's own format, as stock reads it.
                Column sorted = plan.cols().get(s.name());
                String lit = s.name().equals(InfinoSql.ROW_ID)
                    ? rowIdLiteral(value)
                    : InfinoQueryTranslator.literalForType(s.type(), value, now, sorted == null ? null : sorted.dateFormat());
                if (lit == null) {
                    throw new IllegalArgumentException("[infino] search_after value [" + value + "] is not a " + s.type().name().toLowerCase(Locale.ROOT));
                }
                String past = col + (s.desc() ? " < " : " > ") + lit;
                strict = s.nullsFirst() || i == 0 && part == Keyset.PRESENT ? past : "(" + past + " OR " + col + " IS NULL)";
                same = col + " = " + lit;
            }
            List<String> conj = new ArrayList<>(equal);
            conj.add(strict);
            alternatives.add("(" + String.join(" AND ", conj) + ")");
            equal.add(same);
        }
        return "(" + String.join(" OR ", alternatives) + ")";
    }

    /**
     * The engine row id {@code value} (its digits, as a hit's sort values carry
     * it) as a literal of the id's type, or null when it is not one: the
     * digits as text cast, since an id is past what a SQL integer literal holds.
     */
    static String rowIdLiteral(Object value) {
        String digits = value.toString();
        if (digits.isEmpty() || digits.length() > ROW_ID_DIGITS || !digits.chars().allMatch(Character::isDigit)) {
            return null;
        }
        return "CAST('" + digits + "' AS DECIMAL(" + ROW_ID_DIGITS + ", 0))";
    }

    /** Whether the request asks for the exact total (the default, and what dashboards send). */
    private static boolean trackTotal(SearchSourceBuilder source) {
        Integer upTo = source.trackTotalHitsUpTo();
        return upTo == null || upTo != SearchContext.TRACK_TOTAL_HITS_DISABLED;
    }

    /** The index's number of shards from the cluster state, or 0 when it cannot be read there. */
    private static int shards(Index index) {
        ClusterService clusters = InfinoServices.clusterService();
        IndexMetadata md = clusters == null ? null : clusters.state().metadata().index(index);
        return md == null ? 0 : md.getNumberOfShards();
    }

    /** The index's mapping from the cluster state, or null when it cannot be read there. */
    private static MappingMetadata mapping(Index index) {
        ClusterService clusters = InfinoServices.clusterService();
        IndexMetadata md = clusters == null ? null : clusters.state().metadata().index(index);
        return md == null ? null : md.mapping();
    }

    /** The paths of the ignored fields ({@link InfinoIgnored}) of each of the first {@code end} rows that has any, by id. */
    private static Map<String, List<String>> ignored(String table, Map<String, Column> cols, List<Map<String, Object>> rows, int end) {
        if (InfinoQueryTranslator.ignoredColumn(cols) == null) {
            return Map.of();
        }
        List<String> ids = new ArrayList<>();
        for (int i = 0; i < end; i++) {
            Object id = rows.get(i).get(InfinoQueryTranslator.OSID);
            if (id != null) {
                ids.add(id.toString());
            }
        }
        return InfinoNative.ignoredOf(table, ids);
    }

    /** A hit's metadata fields: {@code _ignored} when the mapper ignored any of its fields, as stock's hit carries it. */
    private static Map<String, DocumentField> ignoredField(List<String> fields) {
        if (fields.isEmpty()) {
            return Map.of();
        }
        Map<String, DocumentField> meta = new HashMap<>();
        meta.put(InfinoQueryTranslator.IGNORED_FIELD, new DocumentField(InfinoQueryTranslator.IGNORED_FIELD, new ArrayList<>(fields)));
        return meta;
    }

    /** The shard target a hit of {@code index} carries; it names the index in the response. */
    private static SearchShardTarget target(Index index) {
        return new SearchShardTarget(NODE, new ShardId(index, 0), null, OriginalIndices.NONE);
    }

    /**
     * A {@code top_hits} result from a bucket's first rows (in order, at most
     * {@code from + size} of them, each with its id, source and sort values)
     * and the bucket's row count. The final reduction pages the rows past
     * {@code from} itself, so all of them are handed over, in order: the
     * reduction merges by score then position, and every hit scores alike.
     */
    static InternalAggregation topHits(
        TopHitsAggregationBuilder b,
        int from,
        int size,
        List<SortSpec> sorts,
        FetchSourceContext fetchSource,
        List<FieldAndFormat> docValueFields,
        List<FieldAndFormat> fetchFields,
        Map<String, Column> cols,
        InfinoAccess access,
        List<Map<String, Object>> rows,
        long bucketCount,
        Index index
    ) {
        boolean withSource = fetchSource == null || fetchSource.fetchSource();
        InfinoMappingSource stored = (withSource || !fetchFields.isEmpty()) && index != null
            ? InfinoMappingSource.of(mapping(index))
            : InfinoMappingSource.WHOLE;
        float score = sorts.isEmpty() ? 1.0f : Float.NaN;
        SearchShardTarget target = index == null ? null : target(index);
        List<SearchHit> hits = new ArrayList<>();
        for (Map<String, Object> row : rows) {
            Object id = row.get(InfinoQueryTranslator.OSID);
            if (id == null) {
                continue;
            }
            List<String> ignoredFields = InfinoIgnored.split(row.get(InfinoQueryTranslator.IGNORED));
            SearchHit h = new SearchHit(hits.size(), id.toString(), Map.of(), ignoredField(ignoredFields));
            if (target != null) {
                h.shard(target);
            }
            h.score(score);
            if (withSource || !docValueFields.isEmpty() || !fetchFields.isEmpty()) {
                String src = access.read(id.toString(), String.valueOf(row.get(InfinoQueryTranslator.SOURCE)));
                if (!src.isEmpty()) {
                    // Stock's top_hits fetches each of its from + size hits on the shard.
                    stored.refuse(index == null ? null : index.getName(), fetchSource, fetchFields);
                    if (withSource) {
                        BytesReference shown = stored.shown(src, fetchSource);
                        if (shown != null) {
                            h.sourceRef(shown);
                        }
                    }
                    fillRequested(h, src, stored, docValueFields, fetchFields, cols, ignoredFields);
                }
            }
            if (!sorts.isEmpty()) {
                Object[] values = new Object[sorts.size()];
                DocValueFormat[] formats = new DocValueFormat[values.length];
                for (int k = 0; k < values.length; k++) {
                    values[k] = sorts.get(k).orStandIn(row.get(sorts.get(k).name()));
                    formats[k] = DocValueFormat.RAW;
                }
                h.sortValues(values, formats);
            }
            hits.add(h);
        }
        ScoreDoc[] docs = new ScoreDoc[hits.size()];
        for (int i = 0; i < docs.length; i++) {
            docs[i] = new ScoreDoc(i, 1.0f);
        }
        float maxScore = hits.isEmpty() || !sorts.isEmpty() ? Float.NaN : 1.0f;
        TotalHits total = new TotalHits(bucketCount, TotalHits.Relation.EQUAL_TO);
        TopDocsAndMaxScore topDocs = new TopDocsAndMaxScore(new TopDocs(total, docs), maxScore);
        SearchHits searchHits = new SearchHits(hits.toArray(new SearchHit[0]), total, maxScore);
        return new InternalTopHits(b.getName(), from, size, topDocs, searchHits, InfinoAggregationProcessor.metadataOf(b));
    }

    /**
     * The fields a request asks for beside the source ({@code docvalue_fields}
     * and {@code fields}), each once, in the format the last request for it
     * names, as stock's later fetch overwrites the earlier; a pattern such
     * as {@code *} stays a pattern and is matched against the source's paths
     * per hit.
     */
    /** The docvalue_fields format that stands for the field's own: no format. */
    private static final String USE_FIELD_MAPPING = "use_field_mapping";

    static List<FieldAndFormat> requestedFields(List<FieldAndFormat> docValues, List<FieldAndFormat> fetch) {
        Map<String, FieldAndFormat> byField = new LinkedHashMap<>();
        for (List<FieldAndFormat> group : List.of(docValues == null ? List.<FieldAndFormat>of() : docValues, fetch == null ? List.<FieldAndFormat>of() : fetch)) {
            for (FieldAndFormat f : group) {
                byField.put(f.field, f);
            }
        }
        return new ArrayList<>(byField.values());
    }

    /**
     * The requested fields on the hit, as stock's fetch phase reads them:
     * {@code docvalue_fields} from the document as it was indexed (the doc
     * values hold every field, whatever the mapping's {@code _source}
     * keeps), then {@code fields} from the source as the mapping keeps it,
     * none when the mapping disables {@code _source} (the hits a response
     * carries are refused then, see {@link InfinoMappingSource#refuse}). A
     * field asked for both ways takes the {@code fields} value when the
     * source holds it, as stock's fields phase runs after its doc values
     * phase. A mapping that keeps the source whole reads it once for both.
     * A field the mapper ignored ({@code ignored}, by path) has no value to
     * give either way, as stock indexed none, though the source keeps it.
     */
    static void fillRequested(
        SearchHit h,
        String src,
        InfinoMappingSource stored,
        List<FieldAndFormat> docValueFields,
        List<FieldAndFormat> fetchFields,
        Map<String, Column> cols,
        List<String> ignored
    ) {
        Map<String, Object> flat = docValueFields.isEmpty() ? null : withoutIgnored(flat(src), ignored);
        if (flat != null) {
            fillFields(h, flat, docValueFields, cols, true);
        }
        if (!fetchFields.isEmpty() && stored.enabled()) {
            fillFields(h, flat != null && stored.whole() ? flat : withoutIgnored(flat(stored.kept(src)), ignored), fetchFields, cols, false);
        }
    }

    /** {@code flat} without the leaves at or under an ignored field's path. */
    private static Map<String, Object> withoutIgnored(Map<String, Object> flat, List<String> ignored) {
        if (!ignored.isEmpty()) {
            flat.keySet().removeIf(path -> ignored.stream().anyMatch(field -> path.equals(field) || path.startsWith(field + ".")));
        }
        return flat;
    }

    /**
     * Each requested field on the hit under {@code fields}, read from the
     * hit's source, given as its leaves by dotted path ({@link #flat}; the
     * table holds what was sent, so a field's value is the source's value at
     * that path): a date column in the requested format, else its mapping's,
     * everything else as it is. A field alias carries the values of the field
     * it names, as stock reads it. A missing path is left out, as stock does.
     */
    private static void fillFields(SearchHit h, Map<String, Object> flat, List<FieldAndFormat> fields, Map<String, Column> cols, boolean docValues) {
        for (FieldAndFormat f : fields) {
            for (Map.Entry<String, Object> e : flat.entrySet()) {
                if (Regex.simpleMatch(f.field, e.getKey())) {
                    fillField(h, e.getKey(), e.getValue(), cols.get(e.getKey()), f.format, docValues);
                }
            }
            for (Map.Entry<String, Column> e : cols.entrySet()) {
                Column c = e.getValue();
                String target = InfinoQueryTranslator.fieldOf(c.name());
                if (InfinoNative.isAlias(e.getKey(), c) && Regex.simpleMatch(f.field, e.getKey()) && flat.containsKey(target)) {
                    fillField(h, e.getKey(), flat.get(target), c, f.format, docValues);
                }
            }
        }
    }

    /**
     * One field on the hit under {@code name}: the source's value at its path, a date in
     * {@code format}, and a number of {@code docvalue_fields} in its decimal {@code format}, as
     * stock's doc values format gives it ({@code "#.0"} gives {@code 1} as {@code "1.0"}).
     */
    private static void fillField(SearchHit h, String name, Object value, Column c, String format, boolean docValues) {
        DocValueFormat decimal = docValues && c != null && c.element().isNumeric() && format != null && !USE_FIELD_MAPPING.equals(format)
            ? new DocValueFormat.Decimal(format)
            : null;
        List<Object> values = new ArrayList<>();
        for (Object v : leaves(value, new ArrayList<>())) {
            if (decimal != null && v instanceof Number n) {
                // A long field's doc values are longs, a double field's doubles.
                values.add("i64".equals(c.element().type()) ? decimal.format(n.longValue()) : decimal.format(n.doubleValue()));
                continue;
            }
            values.add(c != null && c.isTimestamp() ? formatDate(v, format, c.dateFormat()) : v);
        }
        h.setDocumentField(name, new DocumentField(name, values));
    }

    /** A source value's values, in order: an array inside an array is read through, as OpenSearch flattens arrays of arrays. */
    private static List<Object> leaves(Object value, List<Object> out) {
        if (value instanceof List<?> list) {
            for (Object item : list) {
                leaves(item, out);
            }
        } else if (value != null) {
            out.add(value);
        }
        return out;
    }

    /** The leaves of the source {@code src} by dotted path ({@link #flatten}). */
    private static Map<String, Object> flat(String src) {
        Map<String, Object> flat = new LinkedHashMap<>();
        flatten("", XContentHelper.convertToMap(new BytesArray(src), true, MediaTypeRegistry.JSON).v2(), flat);
        return flat;
    }

    /** The source's leaves by dotted path; a list of scalars stays one value. */
    private static void flatten(String prefix, Map<String, Object> map, Map<String, Object> out) {
        for (Map.Entry<String, Object> e : map.entrySet()) {
            String path = prefix.isEmpty() ? e.getKey() : prefix + "." + e.getKey();
            if (e.getValue() instanceof Map<?, ?> nested) {
                @SuppressWarnings("unchecked")
                Map<String, Object> inner = (Map<String, Object>) nested;
                flatten(path, inner, out);
            } else if (e.getValue() != null) {
                out.put(path, e.getValue());
            }
        }
    }

    /**
     * A date value of a field mapped with {@code mapped} (null for the
     * default format) in the requested format: epoch milliseconds, a named or
     * patterned date format, or the mapping's own when the request names
     * none. The value is read as stock indexed it, with the mapping's format,
     * the parts it leaves out (a time, a zone) at their start; an unreadable
     * value stays as it is.
     */
    private static Object formatDate(Object v, String format, String mapped) {
        DateFormatter mapping = DateFormatter.forPattern(mapped == null ? DEFAULT_DATE_FORMAT : mapped);
        Instant at;
        try {
            at = DateFormatters.from(mapping.parse(v.toString())).toInstant();
        } catch (RuntimeException e) {
            if (!(v instanceof Number n)) {
                return v;
            }
            at = Instant.ofEpochMilli(n.longValue());
        }
        if (EPOCH_MILLIS_FORMAT.equals(format)) {
            return Long.toString(at.toEpochMilli());
        }
        try {
            DateFormatter printer = format == null || format.equals(MAPPING_FORMAT) ? mapping : DateFormatter.forPattern(format);
            return printer.format(at.atZone(ZoneOffset.UTC));
        } catch (RuntimeException e) {
            return v;
        }
    }

    /** The exact number of rows the request matches. */
    static long count(String table, Translation q) {
        return count(table, q, Long.MAX_VALUE);
    }

    /**
     * How far a total needs counting for the request's {@code track_total_hits}:
     * one past its limit (the stock default when it names none), since
     * {@link #reported} turns any count past the limit into the limit, as a
     * lower bound. A default-mode shard's total reaches its coordinator the
     * same way: one past the limit stays past it when shards' totals add up.
     * One past {@code terminate_after} when that is larger, to tell whether
     * more documents match than it names (see {@link #terminated}).
     * Unlimited when the request tracks every hit or none.
     */
    static long countCap(SearchSourceBuilder source) {
        Integer upTo = source.trackTotalHitsUpTo();
        int limit = upTo == null ? SearchContext.DEFAULT_TRACK_TOTAL_HITS_UP_TO : upTo;
        if (limit == SearchContext.TRACK_TOTAL_HITS_DISABLED || limit == SearchContext.TRACK_TOTAL_HITS_ACCURATE) {
            return Long.MAX_VALUE;
        }
        return Math.max((long) limit, (long) source.terminateAfter()) + 1;
    }

    /** What a search with {@code terminate_after} reports: its total, and whether a shard stopped early. */
    record Terminated(long total, boolean early) {}

    /**
     * What a search of an index held by {@code shards} stock shards
     * reports for its {@code terminate_after}, from the page's {@code total}
     * (exact up to {@link #countCap} when the request tracks it); null when
     * the request sets none.
     *
     * <p>A stock shard stops collecting once more documents than {@code
     * terminate_after} match its query and post_filter, and says so. When no
     * more match, the answer is the full one, reported as not stopped early.
     * Past it, the shard's hits are whichever documents it collected first,
     * which only its segment order decides, so a request for hits is refused.
     * A request for no hits (size 0: a count, aggregations or both) is served
     * with the full answer: stock's totals and buckets past the limit are some
     * subset of the matches, which is all a client can count on, and its
     * count is the full one whenever Lucene counts the query from the index.
     * Such a request is reported as stopped early when one of its shards
     * holds more than {@code terminate_after} documents, which is certain
     * once more than {@code shards} times that many match, and as not stopped
     * early otherwise.
     */
    static Terminated terminated(Plan plan, SearchSourceBuilder source, long total, int shards) {
        int after = source.terminateAfter();
        if (after <= 0) {
            return null;
        }
        if (plan.vector() != null) {
            throw new IllegalArgumentException("[infino] terminate_after is not served by the table with a vector query");
        }
        long matched = trackTotal(source) ? total : total(plan, (long) after + 1);
        if (matched <= after) {
            return new Terminated(total, false);
        }
        if (source.size() != 0) {
            throw new IllegalArgumentException(
                "[infino] terminate_after is not served by the table when more than [" + after + "] documents match"
            );
        }
        long beyondEveryShard = (long) after * Math.max(shards, 1) + 1;
        return new Terminated(total, shards <= 1 || total(plan, beyondEveryShard) >= beyondEveryShard);
    }

    /**
     * The number of rows the request matches, or {@code cap} when it matches at
     * least that many. A predicate that reads the token index is counted under
     * a LIMIT, so the scan stops at the cap instead of matching every row: from
     * the token index itself when it is filters and one OR of plain-words text
     * clauses ({@link InfinoQueryTranslator#textCountStatement}). Any other
     * keeps the engine's count, which a range answers from its index.
     */
    static long count(String table, Translation q, long cap) {
        if (cap != Long.MAX_VALUE && q.principal() == null && InfinoQueryTranslator.readsTextIndex(q.where())) {
            String fromIndex = InfinoQueryTranslator.textCountStatement(q.table(), q.where(), cap);
            List<Map<String, Object>> rows = InfinoNative.sqlRows(
                fromIndex != null
                    ? fromIndex
                    : "SELECT COUNT(*) AS c FROM (SELECT 1 AS one FROM " + q.from() + " WHERE " + q.where() + " LIMIT " + cap + ") capped"
            );
            Object c = rows.isEmpty() ? null : rows.get(0).get("c");
            return c instanceof Number n ? n.longValue() : 0;
        }
        if (q.principal() != null && InfinoQueryTranslator.ALL.equals(q.where())) {
            // A bare text query: the engine counts its matches without materializing them.
            return InfinoNative.count(table, q.principal().column(), q.principal().query(), q.principal().mode());
        }
        if (q.principal() == null) {
            // A condition on the table's columns: the engine counts a range from its range index, with no statement.
            return InfinoNative.countWhere(table, List.of(q.where()))[0];
        }
        List<Map<String, Object>> rows = InfinoNative.sqlRows("SELECT COUNT(*) AS c FROM " + q.from() + " WHERE " + q.where());
        if (rows.isEmpty()) {
            return 0;
        }
        Object c = rows.get(0).get("c");
        return c instanceof Number n ? n.longValue() : 0;
    }

    private static String floatsToJson(float[] v) {
        StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < v.length; i++) {
            if (i > 0) {
                sb.append(',');
            }
            sb.append(v[i]);
        }
        return sb.append(']').toString();
    }

    // ---- the response ----

    /**
     * The response for a request over these indices: each index answers its
     * own page and aggregations, the pages merge in the request's order
     * (sort values, else score) and the aggregations reduce once, which is
     * also where pipeline aggregations are computed.
     */
    static SearchResponse search(
        List<Index> indices,
        Function<Index, String> tableOf,
        Function<Index, InfinoAccess> accessOf,
        SearchSourceBuilder source,
        ScriptService scripts,
        long startNanos
    ) {
        return search(indices, tableOf, accessOf, source, null, null, null, scripts, startNanos).response();
    }

    /** A response and, for a scroll, the cursor it stopped at: the sort values of the last hit handed out from each index. */
    record Searched(SearchResponse response, Map<Index, Object[]> after) {}

    /**
     * Refuse a page past an index's {@code index.max_result_window}, as a
     * shard refuses it: every row up to the window is read with its source,
     * once per index, so an unbounded one fills the heap. A scroll's pages
     * are bounded by their size, as a stock scroll's batches are.
     */
    static void checkResultWindow(List<Index> indices, SearchSourceBuilder source, boolean scrolling) {
        ClusterService clusters = InfinoServices.clusterService();
        if (clusters == null) {
            return;
        }
        long size = source.size() < 0 ? DEFAULT_SIZE : source.size();
        long window = scrolling ? size : Math.max(0, source.from()) + size;
        for (Index index : indices) {
            IndexMetadata md = clusters.state().metadata().index(index);
            if (md == null) {
                continue;
            }
            int max = IndexSettings.MAX_RESULT_WINDOW_SETTING.get(md.getSettings());
            if (window <= max) {
                continue;
            }
            String setting = IndexSettings.MAX_RESULT_WINDOW_SETTING.getKey();
            throw new IllegalArgumentException(
                scrolling
                    ? "Batch size is too large, size must be less than or equal to: [" + max + "] but was [" + window
                        + "]. Scroll batch sizes cost as much memory as result windows so they are controlled by the [" + setting
                        + "] index level setting."
                    : "Result window is too large, from + size must be less than or equal to: [" + max + "] but was [" + window
                        + "]. See the scroll api for a more efficient way to request large data sets. This limit can be set by changing the ["
                        + setting + "] index level setting."
            );
        }
    }

    /** The buckets one request may build, as the cluster sets {@code search.max_buckets}. */
    private static int maxBuckets() {
        ClusterService clusters = InfinoServices.clusterService();
        return clusters == null
            ? MultiBucketConsumerService.DEFAULT_MAX_BUCKETS
            : clusters.getClusterSettings().get(MultiBucketConsumerService.MAX_BUCKET_SETTING);
    }

    /**
     * As {@link #search(List, Function, Function, SearchSourceBuilder, ScriptService, long)},
     * and for a scroll: {@code after} is the cursor each index resumes from
     * (an index absent from it starts at the top; null when not scrolling),
     * {@code total} the hit count the first page established (null to count
     * now), {@code scrollId} what the response carries.
     */
    static Searched search(
        List<Index> indices,
        Function<Index, String> tableOf,
        Function<Index, InfinoAccess> accessOf,
        SearchSourceBuilder source,
        Map<Index, Object[]> after,
        TotalHits total,
        String scrollId,
        ScriptService scripts,
        long startNanos
    ) {
        checkResultWindow(indices, source, after != null);
        long now = System.currentTimeMillis();
        List<Page> pages = new ArrayList<>();
        List<InternalAggregations> aggs = new ArrayList<>();
        Collection<AggregationBuilder> builders = builders(source);
        List<SortSpec> sorts = List.of();
        boolean terminatedEarly = false;
        for (Index index : indices) {
            SearchSourceBuilder perIndex = after == null || after.get(index) == null ? source : withSearchAfter(source, after.get(index));
            Plan plan = plan(tableOf.apply(index), perIndex, now, accessOf.apply(index));
            if (after != null && plan.ranksByScore()) {
                throw new IllegalArgumentException(SCORE_SORT_SCROLL);
            }
            sorts = plan.sorts();
            // Aggregations see the query, not the post_filter, so they share the
            // total only when there is none. A request with no hits to fetch then
            // reads its total beside the aggregations rather than in a count of its own.
            boolean sharedTotal = !builders.isEmpty() && plan.vector() == null && plan.postWhere() == null && trackTotal(perIndex);
            boolean countWithAggs = sharedTotal && perIndex.size() == 0;
            // A total the aggregations take as their population is counted in
            // full: a capped one would undercount it (top_hits' total, a
            // significant_terms subset size). A scroll's next page resumes
            // from this one's last hits (the cursor below).
            // One index's window is cut from its own page (merge): the hits before it need no source.
            int sourceFrom = indices.size() == 1 ? Math.max(0, perIndex.from()) : 0;
            Page page = page(index, plan, perIndex, !countWithAggs, scrollId == null && !sharedTotal, after != null, sourceFrom);
            if (!builders.isEmpty()) {
                InfinoAggregationProcessor.Counted counted = new InfinoAggregationProcessor(index).evaluate(
                    builders,
                    plan,
                    sharedTotal && !countWithAggs ? page.total() : null
                );
                aggs.add(counted.aggregations());
                if (countWithAggs) {
                    page = new Page(page.hits(), counted.total(), page.exact(), page.maxScore(), page.unsourced(), page.chosen());
                }
            }
            // The index's table holds every shard's rows.
            Terminated terminated = terminated(plan, perIndex, page.total(), shards(index));
            if (terminated != null) {
                terminatedEarly |= terminated.early();
                page = new Page(page.hits(), terminated.total(), page.exact(), page.maxScore(), page.unsourced(), page.chosen());
            }
            pages.add(page);
        }
        SearchHits hits = merge(pages, source, sorts, total);
        refuse(indices, hits, source);
        Map<Index, Object[]> cursor = null;
        if (after != null) {
            // Where each index stopped: the sort values of its last hit on this
            // page, which carry the id as the last key, so the next page resumes
            // between rows.
            cursor = new HashMap<>(after);
            for (SearchHit h : hits.getHits()) {
                for (Index index : indices) {
                    if (index.getName().equals(h.getIndex())) {
                        cursor.put(index, h.getSortValues());
                    }
                }
            }
        }
        InternalAggregations reduced = null;
        if (!aggs.isEmpty()) {
            // The buckets are counted against search.max_buckets as a stock reduce
            // counts them, so one request cannot build more than the cluster allows.
            InternalAggregation.ReduceContext ctx = InternalAggregation.ReduceContext.forFinalReduction(
                BigArrays.NON_RECYCLING_INSTANCE,
                scripts,
                new MultiBucketConsumerService.MultiBucketConsumer(maxBuckets(), new NoopCircuitBreaker(CircuitBreaker.REQUEST)),
                source.aggregations().buildPipelineTree()
            );
            reduced = InternalAggregations.topLevelReduce(aggs, ctx);
        }
        // As stock, a search with terminate_after says whether a shard stopped early, and one without says nothing.
        Boolean early = source.terminateAfter() > 0 ? terminatedEarly : null;
        SearchResponseSections sections = new SearchResponseSections(hits, reduced, null, false, early, null, 1);
        long tookMillis = (System.nanoTime() - startNanos) / 1_000_000L;
        SearchResponse response = new SearchResponse(
            sections,
            scrollId,
            indices.size(),
            indices.size(),
            0,
            tookMillis,
            ShardSearchFailure.EMPTY_ARRAY,
            SearchResponse.Clusters.EMPTY
        );
        return new Searched(response, cursor);
    }

    /**
     * Refuses the response when one of its {@code hits} is of an index whose
     * mapping disables {@code _source} and {@code source} filters the source
     * or asks for {@code fields} ({@link InfinoMappingSource#refuse}): only
     * the hits the window keeps, as stock's fetch phase fetches only those.
     */
    private static void refuse(List<Index> indices, SearchHits hits, SearchSourceBuilder source) {
        List<FieldAndFormat> fetchFields = requestedFields(null, source.fetchFields());
        for (SearchHit h : hits.getHits()) {
            for (Index index : indices) {
                if (index.getName().equals(h.getIndex())) {
                    InfinoMappingSource.of(mapping(index)).refuse(index.getName(), source.fetchSource(), fetchFields);
                }
            }
        }
    }

    /**
     * How many sort values each hit hands the client: one per field or script
     * sort its request names, and one for {@code _score} beside them, as
     * stock gives one per sort it names ({@code _doc} gives none here, nor
     * {@code _score} alone). What the plugin adds past them,
     * the id it breaks ties on and a scroll's own order by the id, orders and
     * resumes its pages and is not handed out.
     */
    static int requestedSortValues(SearchSourceBuilder source) {
        if (source == null || source.sorts() == null) {
            return 0;
        }
        return (int) source.sorts().stream().filter(sort -> handedOut(sort, source.sorts())).count();
    }

    /**
     * {@code response} with each hit's sort values cut to the {@code count}
     * its request names ({@link #requestedSortValues}), as the client reads
     * them: the values kept as formatted, the plugin's own after them gone.
     */
    static SearchResponse handOut(SearchResponse response, int count) {
        if (response == null || response.getHits() == null) {
            return response;
        }
        for (SearchHit h : response.getHits().getHits()) {
            Object[] values = h.getSortValues();
            if (values.length > count) {
                DocValueFormat[] asFormatted = new DocValueFormat[count];
                Arrays.fill(asFormatted, DocValueFormat.RAW);
                h.sortValues(Arrays.copyOf(values, count), asFormatted);
            }
        }
        return response;
    }

    /** The request with its page starting after {@code values}, the rest as it is. */
    private static SearchSourceBuilder withSearchAfter(SearchSourceBuilder source, Object[] values) {
        return source.shallowCopy().searchAfter(values);
    }

    /**
     * Whether a scroll of {@code source} pages by the id alone: its request
     * names no sort but {@code _doc} or the score, which a scroll reads in
     * the id's order (see {@link #scrollSource}).
     */
    static boolean scrollsById(SearchSourceBuilder source) {
        return source.sorts() == null || source.sorts().stream().allMatch(InfinoSearch::ordersNothing);
    }

    /** Whether the first sort {@code source} names is {@code _doc}. */
    static boolean sortsByDocFirst(SearchSourceBuilder source) {
        return source.sorts() != null
            && !source.sorts().isEmpty()
            && source.sorts().get(0) instanceof FieldSortBuilder fs
            && FieldSortBuilder.DOC_FIELD_NAME.equals(fs.getFieldName());
    }

    /** Whether the first sort {@code source} names is the score, ascending. */
    static boolean sortsByScoreAscending(SearchSourceBuilder source) {
        return source.sorts() != null
            && !source.sorts().isEmpty()
            && source.sorts().get(0) instanceof ScoreSortBuilder ss
            && ss.order() == SortOrder.ASC;
    }

    /**
     * Whether {@code sort}, one of {@code sorts}, gives each hit a sort
     * value: a field or script sort, or the score beside one.
     */
    private static boolean handedOut(SortBuilder<?> sort, List<SortBuilder<?>> sorts) {
        return !ordersNothing(sort) || sort instanceof ScoreSortBuilder && sorts.stream().anyMatch(s -> !ordersNothing(s));
    }

    /** A sort a scroll pages past in the id's order: {@code _doc}, or the score. */
    private static boolean ordersNothing(SortBuilder<?> sort) {
        return sort instanceof ScoreSortBuilder || sort instanceof FieldSortBuilder fs && FieldSortBuilder.DOC_FIELD_NAME.equals(fs.getFieldName());
    }

    /**
     * The request a scroll's later pages run: the first page's query, sorts,
     * window and fields, without its aggregations (a scroll computes them
     * once) and without a count (the first page's total is repeated). A
     * scroll pages in a fixed order, so a request that names no sort is
     * ordered by the id; {@code _doc}, which the reindex family sends,
     * reads the same way. {@code _score} beside a field keeps its place, its
     * value among the hits' sort values.
     */
    static SearchSourceBuilder scrollSource(SearchSourceBuilder source) {
        SearchSourceBuilder out = new SearchSourceBuilder();
        out.query(source.query());
        if (source.postFilter() != null) {
            out.postFilter(source.postFilter());
        }
        out.size(source.size() < 0 ? DEFAULT_SIZE : source.size());
        out.trackScores(source.trackScores());
        if (source.sorts() != null) {
            for (SortBuilder<?> sort : source.sorts()) {
                if (handedOut(sort, source.sorts())) {
                    out.sort(sort);
                }
            }
        }
        if (scrollsById(source)) {
            out.sort(ID_SORT_FIELD);
        }
        if (source.fetchSource() != null) {
            out.fetchSource(source.fetchSource());
        }
        if (source.docValueFields() != null) {
            for (FieldAndFormat f : source.docValueFields()) {
                out.docValueField(f.field, f.format);
            }
        }
        if (source.fetchFields() != null) {
            for (FieldAndFormat f : source.fetchFields()) {
                out.fetchField(f.field, f.format);
            }
        }
        if (source.scriptFields() != null) {
            for (SearchSourceBuilder.ScriptField sf : source.scriptFields()) {
                out.scriptField(sf.fieldName(), sf.script(), sf.ignoreFailure());
            }
        }
        if (Boolean.TRUE.equals(source.version())) {
            out.version(true);
        }
        if (Boolean.TRUE.equals(source.seqNoAndPrimaryTerm())) {
            out.seqNoAndPrimaryTerm(true);
        }
        out.trackTotalHits(false);
        return out;
    }

    /**
     * The pages merged by sort values, else by score, then cut to the
     * request's window: each page carries every row up to the window's end,
     * and the window is applied once here, as a coordinator applies it over
     * its shards. {@code fixedTotal} stands in for the pages' own count when
     * given (a scroll's later pages repeat the first page's).
     */
    private static SearchHits merge(List<Page> pages, SearchSourceBuilder source, List<SortSpec> sorts, TotalHits fixedTotal) {
        long total = 0;
        boolean exact = true;
        float maxScore = Float.NaN;
        List<SearchHit> all = new ArrayList<>();
        for (Page p : pages) {
            total += p.total();
            exact &= p.exact();
            all.addAll(p.hits());
            if (!Float.isNaN(p.maxScore())) {
                maxScore = Float.isNaN(maxScore) ? p.maxScore() : Math.max(maxScore, p.maxScore());
            }
        }
        if (pages.size() > 1) {
            boolean sorted = all.stream().anyMatch(h -> h.getSortValues().length > 0);
            Comparator<SearchHit> byScore = sortsByScoreAscending(source)
                ? Comparator.comparing(SearchHit::getScore)
                : Comparator.comparing(SearchHit::getScore, Comparator.reverseOrder());
            all.sort(sorted ? bySortValues(sorts) : byScore);
        }
        int from = Math.max(0, source.from());
        int size = source.size() < 0 ? DEFAULT_SIZE : source.size();
        all = all.subList(Math.min(from, all.size()), Math.min(all.size(), from + size));
        TotalHits totalHits = fixedTotal != null ? fixedTotal : reported(total, exact, source);
        return new SearchHits(all.toArray(new SearchHit[0]), totalHits, maxScore);
    }

    /**
     * The total as a coordinator reports it under the request's {@code track_total_hits}: none when
     * disabled, else the count up to the limit (the stock default when the request names none), and
     * the limit itself, as a lower bound, past it. The pages count exactly whenever tracking is on,
     * so a default-mode shard's coordinator can apply the same rule.
     *
     * <p>A request that tracks every hit carries the largest {@code int} as its limit, which is
     * how the request says there is none: no Lucene shard holds more documents than that, but a
     * table does, and its total is reported as counted rather than cut to that number.
     */
    static TotalHits reported(long total, boolean exact, SearchSourceBuilder source) {
        Integer upTo = source.trackTotalHitsUpTo();
        int limit = upTo == null ? SearchContext.DEFAULT_TRACK_TOTAL_HITS_UP_TO : upTo;
        if (limit == SearchContext.TRACK_TOTAL_HITS_DISABLED) {
            return null;
        }
        if (exact && limit != SearchContext.TRACK_TOTAL_HITS_ACCURATE && total > limit) {
            return new TotalHits(limit, TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO);
        }
        return new TotalHits(total, exact ? TotalHits.Relation.EQUAL_TO : TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO);
    }

    /** Hits by their sort values in the plan's order: field and script sorts alike, each in its direction. */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    private static Comparator<SearchHit> bySortValues(List<SortSpec> sorts) {
        List<Boolean> desc = new ArrayList<>();
        List<Boolean> nullsFirst = new ArrayList<>();
        for (SortSpec s : sorts) {
            desc.add(s.desc());
            nullsFirst.add(s.nullsFirst());
        }
        return (a, b) -> {
            Object[] va = a.getSortValues();
            Object[] vb = b.getSortValues();
            for (int i = 0; i < Math.min(va.length, vb.length); i++) {
                if (va[i] == null || vb[i] == null) {
                    // Missing values where each index's own statement orders them
                    // (NULLS LAST, or NULLS FIRST under missing: _first): never turned
                    // by a descending sort.
                    int nulls = va[i] == null ? (vb[i] == null ? 0 : 1) : -1;
                    if (i < nullsFirst.size() && nullsFirst.get(i)) {
                        nulls = -nulls;
                    }
                    if (nulls != 0) {
                        return nulls;
                    }
                    continue;
                }
                int c;
                if (va[i] instanceof Comparable ca && vb[i].getClass() == va[i].getClass()) {
                    c = ca.compareTo(vb[i]);
                } else if (va[i] instanceof Number na && vb[i] instanceof Number nb) {
                    c = Double.compare(na.doubleValue(), nb.doubleValue());
                } else {
                    c = String.valueOf(va[i]).compareTo(String.valueOf(vb[i]));
                }
                if (c != 0) {
                    return i < desc.size() && desc.get(i) ? -c : c;
                }
            }
            return 0;
        };
    }
}
