/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import org.opensearch.index.query.MultiMatchQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * A full-text query naming no field, which searches every field, over real
 * tables. Over one text field and a keyword field it ranks as stock ranks
 * it: each hit scores the better of its text BM25 and the keyword match's
 * score (1), a {@code dis_max} of the two fields, whether a keyword value
 * matches or not. A multi-field the table keeps no column of is left out
 * of every field rather than failing the search.
 */
public class InfinoDefaultFieldSearchTests extends OpenSearchTestCase {

    private static final String TABLE = "value_ranking";
    /** A table whose mapping has multi-fields it keeps no column of: a text one with its own analyzer, a normalized keyword. */
    private static final String MULTI = "columnless_multi_fields";
    /** What a keyword match scores in stock: a term query there scores its boost. */
    private static final double KEYWORD_MATCH = 1.0;
    private static final String[] MESSAGES = {
        "disk full disk",
        "disk",
        "memory full",
        "cpu hot",
        "disk failure on the second volume of the storage array",
        "full",
        "disk disk disk" };
    private static final String[] LEVELS = { "info", "disk", "disk", "warn", "info", "warn", "info" };

    @Override
    public void setUp() throws Exception {
        super.setUp();
        InfinoNative.dropTable(TABLE);
        InfinoNative.ensureTable(TABLE, "{\"properties\":{\"message\":{\"type\":\"text\"},\"level\":{\"type\":\"keyword\"}}}");
        String[] ids = new String[MESSAGES.length];
        String[] sources = new String[MESSAGES.length];
        for (int i = 0; i < MESSAGES.length; i++) {
            ids[i] = Integer.toString(i);
            sources[i] = "{\"message\":\"" + MESSAGES[i] + "\",\"level\":\"" + LEVELS[i] + "\"}";
        }
        assertEquals(MESSAGES.length, InfinoNative.indexBatch(TABLE, ids, sources, MESSAGES.length).ok());
    }

    @Override
    public void tearDown() throws Exception {
        InfinoNative.dropTable(TABLE);
        InfinoNative.dropTable(MULTI);
        super.tearDown();
    }

    private static InfinoSearch.Plan plan(QueryBuilder query) {
        return InfinoSearch.plan(TABLE, new SearchSourceBuilder().query(query), System.currentTimeMillis());
    }

    /** The page of {@code query}, unsorted: id to score, best first. */
    private static Map<String, Double> page(QueryBuilder query) {
        Map<String, Double> out = new LinkedHashMap<>();
        for (Map<String, Object> row : InfinoNative.sqlRows(InfinoSearch.hitStatement(plan(query), MESSAGES.length, false))) {
            out.put(row.get(InfinoQueryTranslator.OSID).toString(), ((Number) row.get(InfinoSearch.SCORE)).doubleValue());
        }
        return out;
    }

    /**
     * Stock's page: each document the word matches on message or on level,
     * scored by its message BM25 and its level match, combined by the better
     * ({@code best}) or the sum; best first, ties in the order written.
     */
    private static List<Map.Entry<String, Double>> stock(String word, boolean best) {
        Map<String, Double> bm25 = page(QueryBuilders.matchQuery("message", word));
        List<Map.Entry<String, Double>> out = new ArrayList<>();
        for (int i = 0; i < MESSAGES.length; i++) {
            String id = Integer.toString(i);
            double text = bm25.getOrDefault(id, 0.0);
            double keyword = LEVELS[i].equals(word) ? KEYWORD_MATCH : 0.0;
            if (text > 0 || keyword > 0) {
                out.add(Map.entry(id, best ? Math.max(text, keyword) : text + keyword));
            }
        }
        out.sort(Comparator.<Map.Entry<String, Double>>comparingDouble(e -> -e.getValue()).thenComparingInt(e -> Integer.parseInt(e.getKey())));
        return out;
    }

    private static void assertRanked(List<Map.Entry<String, Double>> want, Map<String, Double> got) {
        assertEquals(want.stream().map(Map.Entry::getKey).toList(), new ArrayList<>(got.keySet()));
        for (Map.Entry<String, Double> e : want) {
            assertEquals(e.getKey(), e.getValue(), got.get(e.getKey()), 1e-6);
        }
    }

    /** A keyword value matches: the text hits still rank by their BM25, a keyword-only hit scores the keyword match. */
    public void testAKeywordMatchCompetesWithTheTextScore() {
        List<Map.Entry<String, Double>> want = stock("disk", true);
        // Document 2 is a keyword match alone; 1 matches both.
        assertTrue(want.stream().anyMatch(e -> e.getKey().equals("2")));
        assertRanked(want, page(QueryBuilders.queryStringQuery("disk")));
        assertRanked(want, page(QueryBuilders.multiMatchQuery("disk")));
        assertRanked(want, page(QueryBuilders.multiMatchQuery("disk").type(MultiMatchQueryBuilder.Type.BEST_FIELDS).lenient(true)));
        assertRanked(want, page(QueryBuilders.multiMatchQuery("disk", "*")));
        assertRanked(want, page(QueryBuilders.queryStringQuery("disk").field("message").field("level")));
        // Beside a filter, as Discover sends it: the same scores.
        assertRanked(
            want,
            page(QueryBuilders.boolQuery().must(QueryBuilders.multiMatchQuery("disk")).filter(QueryBuilders.existsQuery("level")))
        );
        // most_fields sums its fields (tie_breaker 1).
        assertRanked(stock("disk", false), page(QueryBuilders.multiMatchQuery("disk").type(MultiMatchQueryBuilder.Type.MOST_FIELDS)));
    }

    /** No keyword value matches: the hits rank by their text BM25 alone, the same order and scores as a match on the text field. */
    public void testNoKeywordMatchRanksByTheText() {
        List<Map.Entry<String, Double>> want = stock("full", true);
        assertEquals(page(QueryBuilders.matchQuery("message", "full")).keySet().stream().toList(), want.stream().map(Map.Entry::getKey).toList());
        assertRanked(want, page(QueryBuilders.queryStringQuery("full")));
        assertRanked(want, page(QueryBuilders.multiMatchQuery("full")));
    }

    /**
     * The text hits score the same whether a keyword value matches or not:
     * a write adding a matching keyword value adds its document, scored by
     * the keyword match, and leaves every text hit scored by its BM25.
     */
    public void testAWriteAddingAKeywordValueAddsOnlyItsDocument() {
        assertSameScores(page(QueryBuilders.matchQuery("message", "hot")), page(QueryBuilders.queryStringQuery("hot")));
        assertEquals(1, InfinoNative.indexBatch(TABLE, new String[] { "7" }, new String[] { "{\"message\":\"cold\",\"level\":\"hot\"}" }, 1).ok());
        Map<String, Double> after = page(QueryBuilders.queryStringQuery("hot"));
        assertEquals(KEYWORD_MATCH, after.remove("7"), 1e-6);
        assertSameScores(page(QueryBuilders.matchQuery("message", "hot")), after);
    }

    /** The same hits in the same order, each score the same to within a float's rounding. */
    private static void assertSameScores(Map<String, Double> want, Map<String, Double> got) {
        assertEquals(new ArrayList<>(want.keySet()), new ArrayList<>(got.keySet()));
        for (Map.Entry<String, Double> e : want.entrySet()) {
            assertEquals(e.getKey(), e.getValue(), got.get(e.getKey()), 1e-6);
        }
    }

    /**
     * A multi-field the table keeps no column of is left out of {@code *}
     * and of the index's default fields, so a search naming no field reads
     * the fields it can; a pattern or a name the request gives that reaches
     * one is refused, as before.
     */
    public void testEveryFieldLeavesOutAColumnlessMultiField() {
        InfinoNative.dropTable(MULTI);
        InfinoNative.ensureTable(
            MULTI,
            "{\"properties\":{\"title\":{\"type\":\"text\",\"fields\":{"
                + "\"english\":{\"type\":\"text\",\"analyzer\":\"english\"},"
                + "\"lower\":{\"type\":\"keyword\",\"normalizer\":\"lowercase\"}}},"
                + "\"level\":{\"type\":\"keyword\"}}}"
        );
        assertEquals(1, InfinoNative.indexBatch(MULTI, new String[] { "a" }, new String[] { "{\"title\":\"disk full\",\"level\":\"info\"}" }, 1).ok());
        Map<String, InfinoNative.Column> cols = InfinoNative.columns(MULTI);
        assertFalse(cols.containsKey("title.english"));
        assertFalse(cols.containsKey("title.lower"));
        long now = System.currentTimeMillis();
        for (QueryBuilder qb : List.of(
            QueryBuilders.multiMatchQuery("disk"),
            QueryBuilders.multiMatchQuery("disk", "*"),
            QueryBuilders.queryStringQuery("disk"),
            QueryBuilders.queryStringQuery("disk").defaultField("*"),
            QueryBuilders.simpleQueryStringQuery("disk")
        )) {
            String where = InfinoQueryTranslator.translate(qb, cols, MULTI, now).where();
            assertFalse(where, where.contains("title.english") || where.contains("title.lower"));
            List<Map<String, Object>> hits = InfinoNative.sqlRows(
                InfinoSearch.hitStatement(InfinoSearch.plan(MULTI, new SearchSourceBuilder().query(qb), now), 10, false)
            );
            assertEquals(qb.toString(), List.of("a"), hits.stream().map(r -> r.get(InfinoQueryTranslator.OSID).toString()).toList());
        }
        for (QueryBuilder qb : List.of(
            QueryBuilders.multiMatchQuery("disk", "title.*"),
            QueryBuilders.queryStringQuery("disk").field("title.*"),
            QueryBuilders.multiMatchQuery("disk", "title.english"),
            QueryBuilders.queryStringQuery("disk").field("title.lower")
        )) {
            IllegalArgumentException refused = expectThrows(IllegalArgumentException.class, () -> InfinoQueryTranslator.translate(qb, cols, MULTI, now));
            assertTrue(refused.getMessage(), refused.getMessage().contains("is not served"));
        }
    }
}
