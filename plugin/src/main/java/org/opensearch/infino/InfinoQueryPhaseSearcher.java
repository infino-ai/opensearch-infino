/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.opensearch.common.lucene.search.TopDocsAndMaxScore;
import org.opensearch.index.mapper.IdFieldMapper;
import org.opensearch.index.mapper.Uid;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.infino.InfinoNative.Column;
import org.opensearch.infino.InfinoQueryTranslator.Translation;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.SearchExtBuilder;
import org.opensearch.search.aggregations.AggregationProcessor;
import org.opensearch.search.internal.ContextIndexSearcher;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.query.QueryCollectorContext;
import org.opensearch.search.query.QueryPhaseSearcher;
import org.opensearch.search.query.QueryPhaseSearcherWrapper;
import org.opensearch.search.sort.FieldSortBuilder;
import org.opensearch.search.sort.ScoreSortBuilder;
import org.opensearch.search.sort.SortBuilder;
import org.opensearch.search.sort.SortOrder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Routes the native OpenSearch query phase to Infino for Infino-enabled shards.
 *
 * <p>Hits come one of three ways: an {@link InfinoVectorQuery} runs the
 * engine's kNN / hybrid search; a request sorted by a field runs an ordered
 * SQL scan over the translated filter (over the token-match set when the
 * query has a full-text clause); a request ranked by a full-text clause
 * runs the engine's BM25 table function with the filter applied to the
 * ranked candidates; anything else is a filtered scan. The exact total is a
 * {@code COUNT} over the same relation, not the fetched window. Returned
 * {@code _id}s map to Lucene docids so the fetch phase (served from Infino
 * by {@link InfinoFetchSubPhase}) loads {@code _source}.
 *
 * <p>Hits and aggregations share one {@link #plan} decision so they never
 * disagree; a request the table cannot answer is delegated to the stock
 * phase.
 */
public class InfinoQueryPhaseSearcher implements QueryPhaseSearcher {

    private static final Logger log = LogManager.getLogger(InfinoQueryPhaseSearcher.class);
    /**
     * Candidates BM25 ranks before a filter narrows them, when a request has
     * both: OpenSearch's own index.max_result_window. A filter that keeps few
     * of the top-scored rows gives fewer hits than the total, as Lucene's
     * windowed collection would.
     */
    private static final int RANK_WINDOW = 10_000;

    private final QueryPhaseSearcher fallback = new QueryPhaseSearcherWrapper();

    /** What one request translates to, shared by the hit and aggregation paths. */
    record Plan(String table, Map<String, Column> cols, Translation query, List<String> orderBy) {}

    @Override
    public boolean searchWith(
        SearchContext searchContext,
        ContextIndexSearcher searcher,
        Query query,
        LinkedList<QueryCollectorContext> collectors,
        boolean hasFilterCollector,
        boolean hasTimeout
    ) throws IOException {
        Plan plan = plan(searchContext);
        if (plan != null && fillHits(searchContext, searcher, plan)) {
            return false;
        }
        return fallback.searchWith(searchContext, searcher, query, collectors, hasFilterCollector, hasTimeout);
    }

    @Override
    public AggregationProcessor aggregationProcessor(SearchContext searchContext) {
        return new InfinoAggregationProcessor(fallback.aggregationProcessor(searchContext));
    }

    // ---- shared gate ----

    /** The plan for a request, or null when it is not an Infino shard or not translatable. */
    static Plan plan(SearchContext context) {
        if (!isInfinoShard(context)) {
            return null;
        }
        if (context.suggest() != null || context.collapse() != null) {
            return null;
        }
        String table = tableOf(context);
        Map<String, Column> cols = InfinoNative.columns(table);
        if (cols.isEmpty()) {
            return null;
        }
        Translation query = context.query() instanceof InfinoVectorQuery
            ? new Translation(table, InfinoQueryTranslator.ALL, null)
            // The shard context is frozen by now and refuses its clock; date math reads the wall clock.
            : InfinoQueryTranslator.translate(dslQuery(context), cols, table, System.currentTimeMillis());
        // Lucene holds no data for this index, so a request the table cannot answer is
        // refused with the reason rather than handed to Lucene and answered empty.
        if (query == null) {
            throw new IllegalArgumentException("[infino] query not served by the table: " + dslQuery(context));
        }
        log.debug("[infino] query {} -> FROM {} WHERE {}", dslQuery(context), query.from(), query.where());
        List<String> orderBy = orderBy(context, cols);
        if (orderBy == null) {
            throw new IllegalArgumentException("[infino] sort not served by the table: " + sorts(context));
        }
        Plan plan = new Plan(table, cols, query, orderBy);
        InfinoAggregationProcessor.check(context, plan);
        return plan;
    }

    static boolean isInfinoShard(SearchContext context) {
        try {
            return context.indexShard().indexSettings().getValue(InfinoPlugin.INFINO_ENABLED);
        } catch (Exception e) {
            return false;
        }
    }

    static String tableOf(SearchContext context) {
        return InfinoNative.tableFor(context.indexShard().shardId().getIndex(), context.indexShard().shardId().id());
    }

    /** The query as the client wrote it (carried in {@code ext}), else the shard's rewritten one. */
    static QueryBuilder dslQuery(SearchContext context) {
        try {
            if (context.request() == null || context.request().source() == null) {
                return null;
            }
            for (SearchExtBuilder ext : context.request().source().ext()) {
                if (ext instanceof InfinoQueryExt original) {
                    return original.query();
                }
            }
            return context.request().source().query();
        } catch (Exception e) {
            return null;
        }
    }

    private static List<SortBuilder<?>> sorts(SearchContext context) {
        try {
            if (context.request() == null || context.request().source() == null || context.request().source().sorts() == null) {
                return List.of();
            }
            return context.request().source().sorts();
        } catch (Exception e) {
            return List.of();
        }
    }

    /**
     * ORDER BY terms for the request's sort: field sorts over columns, in
     * request order; {@code _score} and {@code _doc} add nothing. Null when a
     * sort names something the table has no column for.
     */
    private static List<String> orderBy(SearchContext context, Map<String, Column> cols) {
        List<String> out = new ArrayList<>();
        for (SortBuilder<?> sort : sorts(context)) {
            if (sort instanceof ScoreSortBuilder) {
                continue;
            }
            if (!(sort instanceof FieldSortBuilder fs)) {
                return null;
            }
            if (FieldSortBuilder.DOC_FIELD_NAME.equals(fs.getFieldName())) {
                continue;
            }
            Column c = columnFor(cols, fs.getFieldName());
            if (c == null) {
                return null;
            }
            out.add(InfinoQueryTranslator.quoteIdent(c.name()) + (fs.order() == SortOrder.DESC ? " DESC" : " ASC") + " NULLS LAST");
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

    // ---- hits ----

    private boolean fillHits(SearchContext context, ContextIndexSearcher searcher, Plan plan) {
        final int from = Math.max(0, context.from());
        final int size = Math.max(0, context.size());
        final int fetch = from + size; // rows needed to fill the hit window
        final Translation q = plan.query();

        List<Map<String, Object>> rows;
        boolean knnDistance = false;
        long total;
        try {
            if (context.query() instanceof InfinoVectorQuery vq) {
                String emb = floatsToJson(vq.vector);
                String json = vq.hybrid
                    ? InfinoNative.hybrid(plan.table(), vq.textColumn, vq.textQuery, vq.vectorColumn, emb, Math.max(fetch, 1))
                    : InfinoNative.knn(plan.table(), vq.vectorColumn, emb, Math.max(fetch, 1));
                rows = InfinoNative.rows(json);
                knnDistance = !vq.hybrid;
                total = rows.size(); // kNN is inherently top-k
            } else {
                // The hit statement carries each hit's source: the fetch phase reads it
                // from the cache, and a lookup by id afterwards would scan the id column.
                String hit = InfinoQueryTranslator.OSID + ", " + InfinoQueryTranslator.SOURCE;
                String sql;
                if (size == 0) {
                    rows = List.of();
                } else if (!plan.orderBy().isEmpty()) {
                    sql = "SELECT " + hit + " FROM " + q.from() + " WHERE " + q.where() + " ORDER BY "
                        + String.join(", ", plan.orderBy()) + " LIMIT " + fetch;
                    rows = InfinoNative.sqlRows(sql);
                } else if (q.principal() != null) {
                    boolean filtered = !InfinoQueryTranslator.ALL.equals(q.where());
                    sql = "SELECT " + hit + ", score FROM " + q.rankedFrom(filtered ? RANK_WINDOW : fetch)
                        + " WHERE " + q.where() + " ORDER BY score DESC LIMIT " + fetch;
                    rows = InfinoNative.sqlRows(sql);
                } else {
                    sql = "SELECT " + hit + " FROM " + q.from() + " WHERE " + q.where() + " LIMIT " + fetch;
                    rows = InfinoNative.sqlRows(sql);
                }
                total = count(plan.table(), q);
                InfinoNative.keepSources(plan.table(), rows.subList(Math.min(from, rows.size()), Math.min(rows.size(), fetch)));
            }
        } catch (RuntimeException e) {
            // An engine error is the request's error; Lucene has nothing to answer with.
            throw new IllegalStateException("[infino] search failed: " + e.getMessage(), e);
        }
        try {
            List<ScoreDoc> hits = new ArrayList<>();
            int end = Math.min(rows.size(), fetch);
            for (int i = from; i < end; i++) {
                Map<String, Object> row = rows.get(i);
                int docId = lookupDocId(searcher, (String) row.get(InfinoQueryTranslator.OSID));
                if (docId < 0) {
                    continue;
                }
                float score = 1.0f;
                Object s = row.get("score");
                if (s instanceof Number n) {
                    score = n.floatValue();
                    if (knnDistance) {
                        score = 1.0f / (1.0f + score);
                    }
                }
                hits.add(new ScoreDoc(docId, score));
            }
            float maxScore = Float.NaN;
            for (ScoreDoc d : hits) {
                maxScore = Float.isNaN(maxScore) ? d.score : Math.max(maxScore, d.score);
            }
            ScoreDoc[] arr = hits.toArray(new ScoreDoc[0]);
            TopDocs topDocs = new TopDocs(new TotalHits(total, TotalHits.Relation.EQUAL_TO), arr);
            context.queryResult().topDocs(new TopDocsAndMaxScore(topDocs, maxScore), new DocValueFormat[0]);
            log.debug("[infino] query phase served {} hits (total {}) from {}", arr.length, total, plan.table());
            return true;
        } catch (IOException e) {
            throw new IllegalStateException("[infino] docid lookup failed", e);
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

    private int lookupDocId(ContextIndexSearcher searcher, String osid) throws IOException {
        if (osid == null) {
            return -1;
        }
        TermQuery q = new TermQuery(new Term(IdFieldMapper.NAME, Uid.encodeId(osid)));
        TopDocs td = searcher.search(q, 1);
        return td.scoreDocs.length == 0 ? -1 : td.scoreDocs[0].doc;
    }
}
