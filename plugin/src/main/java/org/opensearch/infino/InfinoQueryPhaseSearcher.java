package org.opensearch.infino;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;

import org.opensearch.common.lucene.search.TopDocsAndMaxScore;
import org.opensearch.index.mapper.IdFieldMapper;
import org.opensearch.index.mapper.Uid;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.aggregations.AggregationProcessor;
import org.opensearch.search.internal.ContextIndexSearcher;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.query.QueryCollectorContext;
import org.opensearch.search.query.QueryPhaseSearcher;
import org.opensearch.search.query.QueryPhaseSearcherWrapper;

/**
 * Routes the native OpenSearch query phase to Infino for Infino-enabled shards.
 *
 * <p>Three hit paths: a {@link InfinoVectorQuery} → Infino kNN / hybrid; a pure
 * filter (translated to a SQL {@code WHERE} by {@link InfinoSqlWhere}) → a
 * {@code SELECT osid WHERE p} scan; a {@code term}/{@code match} → Infino BM25.
 * Returned {@code _id}s map back to Lucene docids so the fetch phase (served from
 * Infino by {@link InfinoFetchSubPhase}) loads {@code _source}. Aggregations are
 * served — query-scoped via the same {@code WHERE} — by
 * {@link InfinoAggregationProcessor}. Anything not translatable (sorts, unknown
 * queries, BM25+aggs) is delegated to the stock phase. Hits and aggs share one
 * {@link #canServe} decision so they never disagree.
 */
public class InfinoQueryPhaseSearcher implements QueryPhaseSearcher {

    private static final Logger log = LogManager.getLogger(InfinoQueryPhaseSearcher.class);
    private static final Pattern OSID = Pattern.compile("\"osid\"\\s*:\\s*\"((?:[^\"\\\\]|\\\\.)*)\"");
    private static final Pattern SCORE = Pattern.compile("\"score\"\\s*:\\s*(-?[0-9][0-9eE.+-]*)");

    private final QueryPhaseSearcher fallback = new QueryPhaseSearcherWrapper();

    @Override
    public boolean searchWith(
        SearchContext searchContext,
        ContextIndexSearcher searcher,
        Query query,
        LinkedList<QueryCollectorContext> collectors,
        boolean hasFilterCollector,
        boolean hasTimeout
    ) throws IOException {
        if (canServe(searchContext) && fillHits(searchContext, searcher)) {
            return false;
        }
        return fallback.searchWith(searchContext, searcher, query, collectors, hasFilterCollector, hasTimeout);
    }

    @Override
    public AggregationProcessor aggregationProcessor(SearchContext searchContext) {
        return new InfinoAggregationProcessor(fallback.aggregationProcessor(searchContext));
    }

    // ---- shared gate ----

    static boolean canServe(SearchContext context) {
        if (!isInfinoShard(context)) return false;
        if (context.sort() != null || context.suggest() != null) return false;
        String table = tableOf(context);
        Map<String, String> cols = InfinoNative.columns(table);
        String where = InfinoSqlWhere.of(dslQuery(context), cols, table);
        boolean hitsOk = context.query() instanceof InfinoVectorQuery
            || bm25Translatable(context.query()) || where != null;
        if (!hitsOk) return false;
        return InfinoAggregationProcessor.aggsServable(context, cols, where);
    }

    static boolean isInfinoShard(SearchContext context) {
        try {
            return context.indexShard().indexSettings().getValue(InfinoPlugin.INFINO_ENABLED);
        } catch (Exception e) {
            return false;
        }
    }

    static String tableOf(SearchContext context) {
        return InfinoNative.tableFor(
            context.indexShard().shardId().getIndexName(), context.indexShard().shardId().id());
    }

    static QueryBuilder dslQuery(SearchContext context) {
        try {
            return context.request() != null && context.request().source() != null
                ? context.request().source().query() : null;
        } catch (Exception e) {
            return null;
        }
    }

    private static boolean bm25Translatable(Query q) {
        if (q instanceof TermQuery) return true;
        if (q instanceof BooleanQuery bq) {
            for (BooleanClause c : bq.clauses()) {
                if (c.isProhibited() || !(c.query() instanceof TermQuery)) return false;
            }
            return !bq.clauses().isEmpty();
        }
        return false;
    }

    private static String floatsToJson(float[] v) {
        StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < v.length; i++) {
            if (i > 0) sb.append(',');
            sb.append(v[i]);
        }
        return sb.append(']').toString();
    }

    // ---- hits ----

    private boolean fillHits(SearchContext context, ContextIndexSearcher searcher) {
        final String table = tableOf(context);
        final int from = Math.max(0, context.from());
        final int size = Math.max(0, context.size());
        final int fetch = from + size; // rows needed to fill the hit window

        boolean knnDistance = false;
        long total;          // accurate match count (not the fetched-row count)
        final String rowsJson; // "[]" when size == 0 (no rows needed)
        try {
            Query q = context.query();
            if (q instanceof InfinoVectorQuery vq) {
                String emb = floatsToJson(vq.vector);
                rowsJson = vq.hybrid
                    ? InfinoNative.hybrid(table, vq.textColumn, vq.textQuery, vq.vectorColumn, emb, Math.max(fetch, 1))
                    : InfinoNative.knn(table, vq.vectorColumn, emb, Math.max(fetch, 1));
                knnDistance = !vq.hybrid;
                total = -1; // kNN is inherently top-k; total set from parsed rows
            } else if (bm25Translatable(q)) {
                String terms = bm25Terms(q);
                total = InfinoNative.tokenCount(table, "text", terms); // match count, osids not materialized
                rowsJson = size == 0 ? "[]" : InfinoNative.bm25(table, "text", terms, fetch); // ranked window
            } else {
                Map<String, String> cols = InfinoNative.columns(table);
                String where = InfinoSqlWhere.of(dslQuery(context), cols, table);
                if (where == null) return false;
                total = count(InfinoNative.sql("SELECT COUNT(*) AS c FROM " + table + " WHERE " + where));
                rowsJson = size == 0 ? "[]"
                    : InfinoNative.sql("SELECT osid FROM " + table + " WHERE " + where + " LIMIT " + fetch);
            }
        } catch (RuntimeException e) {
            return false;
        }
        if (rowsJson == null || rowsJson.startsWith("{\"error\"")) {
            if (rowsJson != null) log.warn("[infino] search error, falling back: {}", rowsJson);
            return false;
        }
        try {
            List<String> ids = allMatches(OSID, rowsJson);
            List<Float> scores = new ArrayList<>();
            Matcher m = SCORE.matcher(rowsJson);
            while (m.find()) scores.add(Float.parseFloat(m.group(1)));
            boolean haveScores = scores.size() == ids.size();
            if (knnDistance) {
                for (int i = 0; i < scores.size(); i++) scores.set(i, 1.0f / (1.0f + scores.get(i)));
            }
            if (total < 0) total = ids.size();

            List<ScoreDoc> hits = new ArrayList<>();
            int end = size == 0 ? 0 : Math.min(ids.size(), fetch);
            for (int i = from; i < end; i++) {
                int docId = lookupDocId(searcher, ids.get(i));
                if (docId < 0) continue;
                hits.add(new ScoreDoc(docId, haveScores ? scores.get(i) : 1.0f));
            }
            float maxScore = Float.NaN;
            for (ScoreDoc d : hits) maxScore = Float.isNaN(maxScore) ? d.score : Math.max(maxScore, d.score);
            ScoreDoc[] arr = hits.toArray(new ScoreDoc[0]);
            TopDocs topDocs = new TopDocs(new TotalHits(total, TotalHits.Relation.EQUAL_TO), arr);
            context.queryResult().topDocs(new TopDocsAndMaxScore(topDocs, maxScore), new DocValueFormat[0]);
            log.info("[infino] query phase served {} hits (total {}) from {}", arr.length, total, table);
            return true;
        } catch (Exception e) {
            log.warn("[infino] query-phase routing failed, falling back", e);
            return false;
        }
    }

    /** The term text of a term / bool-of-terms Lucene query, space-joined. */
    private String bm25Terms(Query query) {
        if (query instanceof TermQuery tq) return tq.getTerm().text();
        BooleanQuery bq = (BooleanQuery) query;
        StringBuilder terms = new StringBuilder();
        for (BooleanClause c : bq.clauses()) {
            if (terms.length() > 0) terms.append(' ');
            terms.append(((TermQuery) c.query()).getTerm().text());
        }
        return terms.toString();
    }

    private static final Pattern FIRST_NUM = Pattern.compile("(\\d+)");

    private long count(String json) {
        if (json == null) return 0;
        Matcher m = FIRST_NUM.matcher(json);
        return m.find() ? Long.parseLong(m.group(1)) : 0;
    }

    private int lookupDocId(ContextIndexSearcher searcher, String osid) throws IOException {
        TermQuery q = new TermQuery(new Term(IdFieldMapper.NAME, Uid.encodeId(osid)));
        TopDocs td = searcher.search(q, 1);
        return td.scoreDocs.length == 0 ? -1 : td.scoreDocs[0].doc;
    }

    private static List<String> allMatches(Pattern p, String s) {
        List<String> out = new ArrayList<>();
        Matcher m = p.matcher(s);
        while (m.find()) out.add(m.group(1));
        return out;
    }
}
