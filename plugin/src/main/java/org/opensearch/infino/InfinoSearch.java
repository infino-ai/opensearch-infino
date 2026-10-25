/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.opensearch.action.OriginalIndices;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.SearchResponseSections;
import org.opensearch.action.search.ShardSearchFailure;
import org.opensearch.common.document.DocumentField;
import org.opensearch.common.lucene.search.TopDocsAndMaxScore;
import org.opensearch.common.regex.Regex;
import org.opensearch.common.time.DateFormatter;
import org.opensearch.common.util.BigArrays;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.support.XContentMapValues;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.infino.InfinoNative.Column;
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
import org.opensearch.search.aggregations.metrics.InternalTopHits;
import org.opensearch.search.aggregations.metrics.TopHitsAggregationBuilder;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.collapse.CollapseBuilder;
import org.opensearch.search.fetch.subphase.FetchSourceContext;
import org.opensearch.search.fetch.subphase.FieldAndFormat;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.sort.FieldSortBuilder;
import org.opensearch.search.sort.ScoreSortBuilder;
import org.opensearch.search.sort.ScriptSortBuilder;
import org.opensearch.search.sort.SortBuilder;
import org.opensearch.search.sort.SortOrder;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

/**
 * Answers one {@code _search} for one Infino-backed index from its table,
 * at the action layer: no shard, no Lucene, no fetch phase.
 *
 * <p>Hits come one of three ways: an {@code infino_knn} / {@code infino_hybrid}
 * query runs the engine's vector search; a request sorted by a field runs an
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
     * Candidates BM25 ranks before a filter narrows them, when a request has
     * both: OpenSearch's own index.max_result_window. A filter that keeps few
     * of the top-scored rows gives fewer hits than the total, as Lucene's
     * windowed collection would.
     */
    private static final int RANK_WINDOW = 10_000;
    /** OpenSearch's page size when the request names none. */
    private static final int DEFAULT_SIZE = 10;
    /** The node name a hit's shard target carries; it only names the index in the response. */
    private static final String NODE = "infino";
    /** Version reported for a hit when the request asks for one; the table keeps no per-document version. */
    private static final long UNVERSIONED = 1L;
    /** How a date in a hit's source is read back before it is printed in a requested format. */
    private static final String READ_DATE_FORMAT = "strict_date_optional_time||epoch_millis";

    private InfinoSearch() {}

    /**
     * What one request translates to, shared by the hit and aggregation paths.
     * {@code postWhere} is the post_filter's predicate (null when none): it
     * narrows the hits and their total, never the aggregations. {@code collapse}
     * is the column hits are collapsed on (null when none), {@code searchAfter}
     * the sort values the page starts after (null when none).
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
        List<ScriptedField> scriptFields
    ) {
        List<String> orderBy() {
            return InfinoSearch.orderBy(sorts);
        }

        /** The predicate the hits satisfy: the query's and the post_filter's. */
        String hitWhere() {
            return postWhere == null ? query.where() : "(" + query.where() + ") AND (" + postWhere + ")";
        }
    }

    /**
     * One sort of the request: a column, or a script as a column expression.
     * {@code expr} is what the statement orders by and selects, {@code name}
     * the alias its value comes back under (the column's name, or one made
     * for a script), {@code type} how a search_after value for it is written.
     */
    record SortSpec(String expr, String name, SqlType type, boolean desc) {
        static SortSpec of(Column c, boolean desc) {
            return new SortSpec(InfinoQueryTranslator.quoteIdent(c.name()), c.name(), InfinoScriptTranslator.typeOf(c), desc);
        }
    }

    /** One scripted field of the request: its name and its expression, selected under {@code alias}. */
    record ScriptedField(String name, String expr, String alias) {}

    /** A kNN or hybrid request, served by the engine's vector search instead of SQL. */
    record VectorRequest(boolean hybrid, String textColumn, String textQuery, String vectorColumn, float[] vector, int k) {}

    /** The hits and total of one index, before the response is assembled. */
    record Page(List<SearchHit> hits, long total, boolean exact, float maxScore) {}

    // ---- plan ----

    /** The plan for a request; throws naming the first clause the table cannot serve. */
    static Plan plan(String table, SearchSourceBuilder source, long nowMillis) {
        if (source.suggest() != null) {
            throw new IllegalArgumentException("[infino] suggest is not served by the table");
        }
        Map<String, Column> cols = InfinoNative.columns(table);
        if (cols.isEmpty()) {
            throw new IllegalStateException("[infino] table " + table + " has not been declared");
        }
        QueryBuilder qb = source.query();
        VectorRequest vector = null;
        Translation query;
        if (qb instanceof InfinoKnnQueryBuilder knn) {
            vector = new VectorRequest(false, null, null, knn.field(), knn.vector(), knn.k());
            query = new Translation(table, InfinoQueryTranslator.ALL, null);
        } else if (qb instanceof InfinoHybridQueryBuilder hybrid) {
            vector = new VectorRequest(true, hybrid.textColumn(), hybrid.textQuery(), hybrid.vectorColumn(), hybrid.vector(), hybrid.k());
            query = new Translation(table, InfinoQueryTranslator.ALL, null);
        } else {
            query = InfinoQueryTranslator.translate(qb, cols, table, nowMillis);
            if (query == null) {
                throw new IllegalArgumentException("[infino] query not served by the table: " + qb);
            }
        }
        log.debug("[infino] query {} -> FROM {} WHERE {}", qb, query.from(), query.where());
        List<SortSpec> sorts = sorts(source.sorts(), cols);
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
        }
        List<ScriptedField> scriptFields = new ArrayList<>();
        if (source.scriptFields() != null && !source.scriptFields().isEmpty()) {
            if (vector != null) {
                throw new IllegalArgumentException("[infino] script_fields are not served with a vector query");
            }
            for (SearchSourceBuilder.ScriptField sf : source.scriptFields()) {
                Sql sql = InfinoScriptTranslator.translate(sf.script(), cols, null, nowMillis);
                scriptFields.add(new ScriptedField(sf.fieldName(), sql.expr(), "sf" + scriptFields.size()));
            }
        }
        // A sorted request orders by the id last, so rows sharing a sort value
        // have one order and a page boundary falls between rows, never through
        // a value: the id is the tiebreak search_after needs, and the hit's sort
        // values carry it for the next page.
        Column idColumn = cols.get(InfinoQueryTranslator.OSID);
        if (!sorts.isEmpty() && idColumn != null && sorts.stream().noneMatch(s -> s.name().equals(InfinoQueryTranslator.OSID))) {
            sorts.add(SortSpec.of(idColumn, false));
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
        }
        Plan plan = new Plan(table, cols, query, sorts, vector, postWhere, collapse, searchAfter, scriptFields);
        InfinoAggregationProcessor.check(builders(source), plan);
        return plan;
    }

    /**
     * The field sorts over columns, in request order; {@code _score} and
     * {@code _doc} add nothing. Throws when a sort names something the table
     * has no column for.
     */
    static List<SortSpec> sorts(List<SortBuilder<?>> sorts, Map<String, Column> cols) {
        List<SortSpec> out = new ArrayList<>();
        if (sorts == null) {
            return out;
        }
        for (SortBuilder<?> sort : sorts) {
            if (sort instanceof ScoreSortBuilder) {
                continue;
            }
            if (sort instanceof ScriptSortBuilder ss) {
                // A script sort orders by the script as a column expression.
                Sql sql = InfinoScriptTranslator.translate(ss.script(), cols, null, System.currentTimeMillis());
                out.add(new SortSpec(sql.expr(), "sort" + out.size(), sql.type(), ss.order() == SortOrder.DESC));
                continue;
            }
            if (!(sort instanceof FieldSortBuilder fs)) {
                throw new IllegalArgumentException("[infino] sort not served by the table: " + sort);
            }
            if (FieldSortBuilder.DOC_FIELD_NAME.equals(fs.getFieldName())) {
                continue;
            }
            Column c = columnFor(cols, fs.getFieldName());
            if (c == null) {
                throw new IllegalArgumentException("[infino] sort names field [" + fs.getFieldName() + "], which the table has no column for");
            }
            out.add(SortSpec.of(c, fs.order() == SortOrder.DESC));
        }
        return out;
    }

    /** ORDER BY terms for the sorts: each expression in its direction, missing values last. */
    static List<String> orderBy(List<SortSpec> sorts) {
        List<String> out = new ArrayList<>(sorts.size());
        for (SortSpec s : sorts) {
            out.add(s.expr() + (s.desc() ? " DESC" : " ASC") + " NULLS LAST");
        }
        return out;
    }

    static Column columnFor(Map<String, Column> cols, String field) {
        if ("_id".equals(field)) {
            // The document id is the id column: a sort or collapse on it is served there.
            return cols.get(InfinoQueryTranslator.OSID);
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

    /** The hits of one index for the request's window, each carrying its source, with the total. */
    static Page page(Index index, Plan plan, SearchSourceBuilder source) {
        final int from = Math.max(0, source.from());
        final int size = source.size() < 0 ? DEFAULT_SIZE : source.size();
        final int fetch = from + size;
        final Translation q = plan.query();
        List<Map<String, Object>> rows;
        boolean knnDistance = false;
        if (size == 0) {
            rows = List.of();
        } else if (plan.vector() != null) {
            VectorRequest v = plan.vector();
            String emb = floatsToJson(v.vector());
            String json = v.hybrid()
                ? InfinoNative.hybrid(plan.table(), v.textColumn(), v.textQuery(), v.vectorColumn(), emb, Math.max(fetch, 1))
                : InfinoNative.knn(plan.table(), v.vectorColumn(), emb, Math.max(fetch, 1));
            rows = InfinoNative.rows(json);
            knnDistance = !v.hybrid();
            // The vector entry points return ids and scores; the page's sources come in one statement.
            List<String> ids = new ArrayList<>();
            for (int i = from; i < Math.min(rows.size(), fetch); i++) {
                Object id = rows.get(i).get(InfinoQueryTranslator.OSID);
                if (id != null) {
                    ids.add(id.toString());
                }
            }
            InfinoNative.prefetchSources(plan.table(), ids);
        } else {
            rows = InfinoNative.sqlRows(hitStatement(plan, fetch));
        }

        boolean exact = trackTotal(source);
        long total;
        if (plan.vector() != null) {
            total = rows.size(); // kNN is inherently top-k
        } else if (exact) {
            // The total counts what the query and post_filter keep, before
            // any collapse and regardless of where the page starts.
            total = plan.postWhere() == null ? count(plan.table(), q) : count(plan.table(), new Translation(q.table(), plan.hitWhere(), q.principal()));
        } else {
            total = rows.size();
        }

        FetchSourceContext sourceFilter = sourceFilter(source.fetchSource());
        boolean withSource = source.fetchSource() == null || source.fetchSource().fetchSource();
        List<FieldAndFormat> fields = requestedFields(source.docValueFields(), source.fetchFields());
        boolean version = Boolean.TRUE.equals(source.version());
        SearchShardTarget target = target(index);
        List<SearchHit> hits = new ArrayList<>();
        float maxScore = Float.NaN;
        // Every row up to the window's end: the window itself is applied once,
        // over the merged pages, as a coordinator applies it over its shards.
        int end = Math.min(rows.size(), fetch);
        for (int i = 0; i < end; i++) {
            Map<String, Object> row = rows.get(i);
            Object id = row.get(InfinoQueryTranslator.OSID);
            if (id == null) {
                continue;
            }
            SearchHit h = new SearchHit(i, id.toString(), Map.of(), Map.of());
            h.shard(target);
            float score = Float.NaN;
            Object s = row.get("score");
            if (s instanceof Number n) {
                score = n.floatValue();
                if (knnDistance) {
                    score = 1.0f / (1.0f + score);
                }
            } else if (plan.sorts().isEmpty()) {
                score = 1.0f; // a filter-only request scores every hit alike, as Lucene does
            }
            h.score(score);
            if (!Float.isNaN(score)) {
                maxScore = Float.isNaN(maxScore) ? score : Math.max(maxScore, score);
            }
            if (version) {
                h.version(UNVERSIONED);
            }
            if (withSource || !fields.isEmpty()) {
                String src = plan.vector() != null
                    ? InfinoNative.getSource(plan.table(), id.toString())
                    : String.valueOf(row.get(InfinoQueryTranslator.SOURCE));
                if (src != null && !src.isEmpty()) {
                    if (withSource) {
                        h.sourceRef(sourceFilter == null ? new BytesArray(src) : filtered(src, sourceFilter));
                    }
                    fillFields(h, src, fields, plan.cols());
                }
            }
            if (!plan.sorts().isEmpty()) {
                Object[] values = new Object[plan.sorts().size()];
                DocValueFormat[] formats = new DocValueFormat[values.length];
                for (int k = 0; k < values.length; k++) {
                    values[k] = row.get(plan.sorts().get(k).name());
                    formats[k] = DocValueFormat.RAW;
                }
                h.sortValues(values, formats);
            }
            if (plan.collapse() != null) {
                // A collapsed hit carries its collapse value, as stock does.
                String field = plan.collapse().name();
                Object value = row.get(field);
                h.setDocumentField(field, new DocumentField(field, value == null ? List.of() : List.of(value)));
            }
            for (ScriptedField sf : plan.scriptFields()) {
                // A scripted field comes back under its name, as stock's fetch phase returns it.
                Object value = row.get(sf.alias());
                h.setDocumentField(sf.name(), new DocumentField(sf.name(), value == null ? List.of() : List.of(value)));
            }
            hits.add(h);
        }
        return new Page(hits, total, exact || plan.vector() != null, maxScore);
    }

    /**
     * The statement that fetches the page's hits: each hit's id, source,
     * sort values and, under a collapse, its collapse value. A sorted request
     * orders the filtered relation; a text-ranked one orders the engine's
     * ranked candidates by score; anything else is a filtered scan. A
     * search_after narrows the relation to the rows past the given sort values
     * (a keyset, so no earlier page is scanned), and a collapse keeps the first
     * row of each collapse value in the request's order, through a window.
     */
    static String hitStatement(Plan plan, int fetch) {
        Translation q = plan.query();
        StringBuilder hit = new StringBuilder(InfinoQueryTranslator.OSID + ", " + InfinoQueryTranslator.SOURCE);
        for (SortSpec s : plan.sorts()) {
            if (s.name().equals(InfinoQueryTranslator.OSID)) {
                continue; // the id is selected already
            }
            hit.append(", ").append(s.expr()).append(" AS ").append(InfinoQueryTranslator.quoteIdent(s.name()));
        }
        if (plan.collapse() != null && plan.sorts().stream().noneMatch(s -> s.name().equals(plan.collapse().name()))) {
            hit.append(", ").append(InfinoQueryTranslator.quoteIdent(plan.collapse().name()));
        }
        for (ScriptedField sf : plan.scriptFields()) {
            hit.append(", ").append(sf.expr()).append(" AS ").append(sf.alias());
        }
        String where = plan.hitWhere();
        if (plan.searchAfter() != null) {
            where = "(" + where + ") AND " + keysetPredicate(plan);
        }
        boolean sorted = !plan.sorts().isEmpty();
        boolean ranked = !sorted && q.principal() != null;
        String from;
        String order;
        if (sorted) {
            from = q.from();
            order = String.join(", ", plan.orderBy());
        } else if (ranked) {
            boolean filtered = !InfinoQueryTranslator.ALL.equals(where);
            from = q.rankedFrom(filtered ? RANK_WINDOW : fetch);
            hit.append(", score");
            order = "score DESC";
        } else {
            from = q.from();
            order = null;
        }
        if (plan.collapse() == null) {
            return "SELECT " + hit + " FROM " + from + " WHERE " + where + (order == null ? "" : " ORDER BY " + order) + " LIMIT " + fetch;
        }
        // One row per collapse value: the first in the request's order (the
        // id's order when the request names none, so the page is stable).
        String within = order == null ? InfinoQueryTranslator.OSID + " ASC" : order;
        String partition = InfinoQueryTranslator.quoteIdent(plan.collapse().name());
        return "SELECT " + hit + " FROM (SELECT t.*, ROW_NUMBER() OVER (PARTITION BY " + partition + " ORDER BY " + within
            + ") AS collapse_rank FROM " + from + " t WHERE " + where + ") WHERE collapse_rank = 1"
            + (order == null ? "" : " ORDER BY " + order) + " LIMIT " + fetch;
    }

    /**
     * The rows after the search_after values in the request's sort order:
     * {@code (s1 > v1) OR (s1 = v1 AND s2 > v2) OR ...}, each comparison
     * turned for a descending sort. The order puts missing values last, so
     * after a present value the missing rows still follow ({@code OR s IS
     * NULL}), after a missing one only the deeper sorts decide, and a
     * missing value compares equal to itself.
     */
    private static String keysetPredicate(Plan plan) {
        List<String> alternatives = new ArrayList<>();
        List<String> equal = new ArrayList<>();
        long now = System.currentTimeMillis();
        for (int i = 0; i < plan.searchAfter().length; i++) {
            SortSpec s = plan.sorts().get(i);
            String col = s.expr();
            Object value = plan.searchAfter()[i];
            String strict;
            String same;
            if (value == null) {
                strict = InfinoQueryTranslator.NONE;
                same = col + " IS NULL";
            } else {
                String lit = InfinoQueryTranslator.literalForType(s.type(), value, now);
                if (lit == null) {
                    throw new IllegalArgumentException("[infino] search_after value [" + value + "] is not a " + s.type().name().toLowerCase(Locale.ROOT));
                }
                strict = "(" + col + (s.desc() ? " < " : " > ") + lit + " OR " + col + " IS NULL)";
                same = col + " = " + lit;
            }
            List<String> conj = new ArrayList<>(equal);
            conj.add(strict);
            alternatives.add("(" + String.join(" AND ", conj) + ")");
            equal.add(same);
        }
        return "(" + String.join(" OR ", alternatives) + ")";
    }

    /** Whether the request asks for the exact total (the default, and what dashboards send). */
    private static boolean trackTotal(SearchSourceBuilder source) {
        Integer upTo = source.trackTotalHitsUpTo();
        return upTo == null || upTo != SearchContext.TRACK_TOTAL_HITS_DISABLED;
    }

    /** The source includes/excludes of {@code ctx}, or null when the source is returned whole (or not at all). */
    private static FetchSourceContext sourceFilter(FetchSourceContext ctx) {
        if (ctx == null || !ctx.fetchSource()) {
            return null;
        }
        boolean includes = ctx.includes() != null && ctx.includes().length > 0;
        boolean excludes = ctx.excludes() != null && ctx.excludes().length > 0;
        return includes || excludes ? ctx : null;
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
        List<FieldAndFormat> fields,
        Map<String, Column> cols,
        List<Map<String, Object>> rows,
        long bucketCount,
        Index index
    ) {
        FetchSourceContext sourceFilter = sourceFilter(fetchSource);
        boolean withSource = fetchSource == null || fetchSource.fetchSource();
        float score = sorts.isEmpty() ? 1.0f : Float.NaN;
        SearchShardTarget target = index == null ? null : target(index);
        List<SearchHit> hits = new ArrayList<>();
        for (Map<String, Object> row : rows) {
            Object id = row.get(InfinoQueryTranslator.OSID);
            if (id == null) {
                continue;
            }
            SearchHit h = new SearchHit(hits.size(), id.toString(), Map.of(), Map.of());
            if (target != null) {
                h.shard(target);
            }
            h.score(score);
            if (withSource || !fields.isEmpty()) {
                String src = String.valueOf(row.get(InfinoQueryTranslator.SOURCE));
                if (!src.isEmpty()) {
                    if (withSource) {
                        h.sourceRef(sourceFilter == null ? new BytesArray(src) : filtered(src, sourceFilter));
                    }
                    fillFields(h, src, fields, cols);
                }
            }
            if (!sorts.isEmpty()) {
                Object[] values = new Object[sorts.size()];
                DocValueFormat[] formats = new DocValueFormat[values.length];
                for (int k = 0; k < values.length; k++) {
                    values[k] = row.get(sorts.get(k).name());
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
        return new InternalTopHits(b.getName(), from, size, topDocs, searchHits, b.getMetadata());
    }

    /**
     * The fields a request asks for beside the source ({@code docvalue_fields}
     * and {@code fields}), each once; a pattern such as {@code *} stays a
     * pattern and is matched against the source's paths per hit.
     */
    static List<FieldAndFormat> requestedFields(List<FieldAndFormat> docValues, List<FieldAndFormat> fetch) {
        List<FieldAndFormat> out = new ArrayList<>();
        Set<String> seen = new HashSet<>();
        for (List<FieldAndFormat> group : List.of(docValues == null ? List.<FieldAndFormat>of() : docValues, fetch == null ? List.<FieldAndFormat>of() : fetch)) {
            for (FieldAndFormat f : group) {
                if (seen.add(f.field)) {
                    out.add(f);
                }
            }
        }
        return out;
    }

    /**
     * Each requested field on the hit under {@code fields}, read from the
     * hit's source (the table holds what was sent, so a field's value is the
     * source's value at that path): a date column in the requested format,
     * everything else as it is. A missing path is left out, as stock does.
     */
    static void fillFields(SearchHit h, String src, List<FieldAndFormat> fields, Map<String, Column> cols) {
        if (fields.isEmpty()) {
            return;
        }
        Map<String, Object> flat = new LinkedHashMap<>();
        flatten("", XContentHelper.convertToMap(new BytesArray(src), true, MediaTypeRegistry.JSON).v2(), flat);
        for (FieldAndFormat f : fields) {
            for (Map.Entry<String, Object> e : flat.entrySet()) {
                if (!Regex.simpleMatch(f.field, e.getKey())) {
                    continue;
                }
                List<Object> values = new ArrayList<>();
                Column c = cols.get(e.getKey());
                for (Object v : e.getValue() instanceof List<?> list ? list : List.of(e.getValue())) {
                    values.add(c != null && c.isTimestamp() ? formatDate(v, f.format) : v);
                }
                h.setDocumentField(e.getKey(), new DocumentField(e.getKey(), values));
            }
        }
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

    /** A date value in the requested format: epoch milliseconds, or a named or patterned date format; unreadable values stay as they are. */
    private static Object formatDate(Object v, String format) {
        Instant at;
        try {
            at = v instanceof Number n
                ? Instant.ofEpochMilli(n.longValue())
                : DateFormatter.forPattern(READ_DATE_FORMAT).parse(v.toString()).query(Instant::from);
        } catch (RuntimeException e) {
            return v;
        }
        if (format == null || format.equals("use_field_mapping")) {
            return v;
        }
        if (format.equals("epoch_millis")) {
            return Long.toString(at.toEpochMilli());
        }
        try {
            return DateFormatter.forPattern(format).format(at.atZone(ZoneOffset.UTC));
        } catch (RuntimeException e) {
            return v;
        }
    }

    static BytesReference filtered(String src, FetchSourceContext filter) {
        Map<String, Object> map = XContentHelper.convertToMap(new BytesArray(src), true, MediaTypeRegistry.JSON).v2();
        try (XContentBuilder b = XContentFactory.jsonBuilder()) {
            b.map(XContentMapValues.filter(map, filter.includes(), filter.excludes()));
            return BytesReference.bytes(b);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** The exact number of rows the request matches. */
    static long count(String table, Translation q) {
        if (q.principal() != null && InfinoQueryTranslator.ALL.equals(q.where())) {
            // A bare text query: the engine counts its matches without materializing them.
            return InfinoNative.count(table, q.principal().column(), q.principal().query(), q.principal().mode());
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
    static SearchResponse search(List<Index> indices, Function<Index, String> tableOf, SearchSourceBuilder source, ScriptService scripts, long startNanos) {
        long now = System.currentTimeMillis();
        List<Page> pages = new ArrayList<>();
        List<InternalAggregations> aggs = new ArrayList<>();
        Collection<AggregationBuilder> builders = builders(source);
        List<SortSpec> sorts = List.of();
        for (Index index : indices) {
            Plan plan = plan(tableOf.apply(index), source, now);
            sorts = plan.sorts();
            Page page = page(index, plan, source);
            pages.add(page);
            if (!builders.isEmpty()) {
                // Aggregations see the query, not the post_filter, so a post-filtered
                // page's total is not theirs.
                Long total = page.exact() && plan.vector() == null && plan.postWhere() == null ? page.total() : null;
                aggs.add(new InfinoAggregationProcessor(index).evaluate(builders, plan, total));
            }
        }
        SearchHits hits = merge(pages, source, sorts);
        InternalAggregations reduced = null;
        if (!aggs.isEmpty()) {
            InternalAggregation.ReduceContext ctx = InternalAggregation.ReduceContext.forFinalReduction(
                BigArrays.NON_RECYCLING_INSTANCE,
                scripts,
                bucketCount -> {},
                source.aggregations().buildPipelineTree()
            );
            reduced = InternalAggregations.topLevelReduce(aggs, ctx);
        }
        SearchResponseSections sections = new SearchResponseSections(hits, reduced, null, false, null, null, 1);
        long tookMillis = (System.nanoTime() - startNanos) / 1_000_000L;
        return new SearchResponse(
            sections,
            null,
            indices.size(),
            indices.size(),
            0,
            tookMillis,
            ShardSearchFailure.EMPTY_ARRAY,
            SearchResponse.Clusters.EMPTY
        );
    }

    /**
     * The pages merged by sort values, else by score, then cut to the
     * request's window: each page carries every row up to the window's end,
     * and the window is applied once here, as a coordinator applies it over
     * its shards.
     */
    private static SearchHits merge(List<Page> pages, SearchSourceBuilder source, List<SortSpec> sorts) {
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
            all.sort(sorted ? bySortValues(sorts) : Comparator.comparing(SearchHit::getScore, Comparator.reverseOrder()));
        }
        int from = Math.max(0, source.from());
        int size = source.size() < 0 ? DEFAULT_SIZE : source.size();
        all = all.subList(Math.min(from, all.size()), Math.min(all.size(), from + size));
        TotalHits totalHits = new TotalHits(total, exact ? TotalHits.Relation.EQUAL_TO : TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO);
        return new SearchHits(all.toArray(new SearchHit[0]), totalHits, maxScore);
    }

    /** Hits by their sort values in the plan's order: field and script sorts alike, each in its direction. */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    private static Comparator<SearchHit> bySortValues(List<SortSpec> sorts) {
        List<Boolean> desc = new ArrayList<>();
        for (SortSpec s : sorts) {
            desc.add(s.desc());
        }
        return (a, b) -> {
            Object[] va = a.getSortValues();
            Object[] vb = b.getSortValues();
            for (int i = 0; i < Math.min(va.length, vb.length); i++) {
                int c;
                if (va[i] == null || vb[i] == null) {
                    c = va[i] == null ? (vb[i] == null ? 0 : 1) : -1; // nulls last, both directions
                } else if (va[i] instanceof Comparable ca && vb[i].getClass() == va[i].getClass()) {
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
