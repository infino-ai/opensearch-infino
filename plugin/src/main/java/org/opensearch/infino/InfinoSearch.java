/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.search.TotalHits;
import org.opensearch.action.OriginalIndices;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.SearchResponseSections;
import org.opensearch.action.search.ShardSearchFailure;
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
import org.opensearch.script.ScriptService;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.search.SearchShardTarget;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.fetch.subphase.FetchSourceContext;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.sort.FieldSortBuilder;
import org.opensearch.search.sort.ScoreSortBuilder;
import org.opensearch.search.sort.SortBuilder;
import org.opensearch.search.sort.SortOrder;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
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

    private InfinoSearch() {}

    /** What one request translates to, shared by the hit and aggregation paths. */
    record Plan(String table, Map<String, Column> cols, Translation query, List<SortSpec> sorts, VectorRequest vector) {
        List<String> orderBy() {
            List<String> out = new ArrayList<>(sorts.size());
            for (SortSpec s : sorts) {
                out.add(InfinoQueryTranslator.quoteIdent(s.column().name()) + (s.desc() ? " DESC" : " ASC") + " NULLS LAST");
            }
            return out;
        }
    }

    /** One field sort of the request, over a column. */
    record SortSpec(Column column, boolean desc) {}

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
        if (source.collapse() != null) {
            throw new IllegalArgumentException("[infino] collapse is not served by the table");
        }
        if (source.postFilter() != null) {
            throw new IllegalArgumentException("[infino] post_filter is not served by the table");
        }
        if (source.searchAfter() != null) {
            throw new IllegalArgumentException("[infino] search_after is not served by the table");
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
        List<SortSpec> sorts = sorts(source, cols);
        Plan plan = new Plan(table, cols, query, sorts, vector);
        InfinoAggregationProcessor.check(builders(source), plan);
        return plan;
    }

    /**
     * The request's field sorts over columns, in request order; {@code _score}
     * and {@code _doc} add nothing. Throws when a sort names something the
     * table has no column for.
     */
    private static List<SortSpec> sorts(SearchSourceBuilder source, Map<String, Column> cols) {
        List<SortSpec> out = new ArrayList<>();
        if (source.sorts() == null) {
            return out;
        }
        for (SortBuilder<?> sort : source.sorts()) {
            if (sort instanceof ScoreSortBuilder) {
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
            out.add(new SortSpec(c, fs.order() == SortOrder.DESC));
        }
        return out;
    }

    static Column columnFor(Map<String, Column> cols, String field) {
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
            // The hit statement carries each hit's source and sort values.
            StringBuilder hit = new StringBuilder(InfinoQueryTranslator.OSID + ", " + InfinoQueryTranslator.SOURCE);
            for (SortSpec s : plan.sorts()) {
                hit.append(", ").append(InfinoQueryTranslator.quoteIdent(s.column().name()));
            }
            String sql;
            if (!plan.sorts().isEmpty()) {
                sql = "SELECT " + hit + " FROM " + q.from() + " WHERE " + q.where() + " ORDER BY "
                    + String.join(", ", plan.orderBy()) + " LIMIT " + fetch;
            } else if (q.principal() != null) {
                boolean filtered = !InfinoQueryTranslator.ALL.equals(q.where());
                sql = "SELECT " + hit + ", score FROM " + q.rankedFrom(filtered ? RANK_WINDOW : fetch)
                    + " WHERE " + q.where() + " ORDER BY score DESC LIMIT " + fetch;
            } else {
                sql = "SELECT " + hit + " FROM " + q.from() + " WHERE " + q.where() + " LIMIT " + fetch;
            }
            rows = InfinoNative.sqlRows(sql);
        }

        boolean exact = trackTotal(source);
        long total;
        if (plan.vector() != null) {
            total = rows.size(); // kNN is inherently top-k
        } else if (exact) {
            total = count(plan.table(), q);
        } else {
            total = rows.size();
        }

        FetchSourceContext sourceFilter = sourceFilter(source);
        boolean withSource = source.fetchSource() == null || source.fetchSource().fetchSource();
        ShardId shardId = new ShardId(index, 0);
        SearchShardTarget target = new SearchShardTarget(NODE, shardId, null, OriginalIndices.NONE);
        List<SearchHit> hits = new ArrayList<>();
        float maxScore = Float.NaN;
        int end = Math.min(rows.size(), fetch);
        for (int i = from; i < end; i++) {
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
            if (withSource) {
                String src = plan.vector() != null
                    ? InfinoNative.getSource(plan.table(), id.toString())
                    : String.valueOf(row.get(InfinoQueryTranslator.SOURCE));
                if (src != null && !src.isEmpty()) {
                    h.sourceRef(sourceFilter == null ? new BytesArray(src) : filtered(src, sourceFilter));
                }
            }
            if (!plan.sorts().isEmpty()) {
                Object[] values = new Object[plan.sorts().size()];
                DocValueFormat[] formats = new DocValueFormat[values.length];
                for (int k = 0; k < values.length; k++) {
                    values[k] = row.get(plan.sorts().get(k).column().name());
                    formats[k] = DocValueFormat.RAW;
                }
                h.sortValues(values, formats);
            }
            hits.add(h);
        }
        return new Page(hits, total, exact || plan.vector() != null, maxScore);
    }

    /** Whether the request asks for the exact total (the default, and what dashboards send). */
    private static boolean trackTotal(SearchSourceBuilder source) {
        Integer upTo = source.trackTotalHitsUpTo();
        return upTo == null || upTo != SearchContext.TRACK_TOTAL_HITS_DISABLED;
    }

    /** The request's source includes/excludes, or null when the source is returned whole. */
    private static FetchSourceContext sourceFilter(SearchSourceBuilder source) {
        FetchSourceContext ctx = source.fetchSource();
        if (ctx == null || !ctx.fetchSource()) {
            return null;
        }
        boolean includes = ctx.includes() != null && ctx.includes().length > 0;
        boolean excludes = ctx.excludes() != null && ctx.excludes().length > 0;
        return includes || excludes ? ctx : null;
    }

    private static BytesReference filtered(String src, FetchSourceContext filter) {
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
        for (Index index : indices) {
            Plan plan = plan(tableOf.apply(index), source, now);
            Page page = page(index, plan, source);
            pages.add(page);
            if (!builders.isEmpty()) {
                Long total = page.exact() && plan.vector() == null ? page.total() : null;
                aggs.add(new InfinoAggregationProcessor().evaluate(builders, plan, total));
            }
        }
        SearchHits hits = merge(pages, source);
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

    /** One index's page as-is; several merged by sort values, else by score, then cut to the window. */
    private static SearchHits merge(List<Page> pages, SearchSourceBuilder source) {
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
            all.sort(sorted ? bySortValues(source) : Comparator.comparing(SearchHit::getScore, Comparator.reverseOrder()));
            int from = Math.max(0, source.from());
            int size = source.size() < 0 ? DEFAULT_SIZE : source.size();
            all = all.subList(Math.min(from, all.size()), Math.min(all.size(), from + size));
        }
        TotalHits totalHits = new TotalHits(total, exact ? TotalHits.Relation.EQUAL_TO : TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO);
        return new SearchHits(all.toArray(new SearchHit[0]), totalHits, maxScore);
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private static Comparator<SearchHit> bySortValues(SearchSourceBuilder source) {
        List<Boolean> desc = new ArrayList<>();
        for (SortBuilder<?> sort : source.sorts()) {
            if (sort instanceof FieldSortBuilder fs && !FieldSortBuilder.DOC_FIELD_NAME.equals(fs.getFieldName())) {
                desc.add(fs.order() == SortOrder.DESC);
            }
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
