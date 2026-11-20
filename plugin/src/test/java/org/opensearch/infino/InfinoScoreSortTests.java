/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.collapse.CollapseBuilder;
import org.opensearch.search.sort.ScoreSortBuilder;
import org.opensearch.search.sort.SortOrder;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

/**
 * A sort by {@code _score} beside a field, read through the engine. A
 * scored query's page is its hits by score, best first, the fields ordering
 * those that tie on it; a query whose hits all score alike is ordered by the
 * fields alone. The shapes the table cannot order as stock does are refused.
 */
public class InfinoScoreSortTests extends OpenSearchTestCase {

    private static final String TABLE = "score_field_sort";
    /** Documents indexed; document {@code i} has seq {@code DOCS - 1 - i}, so the seq order is not the order they were written in. */
    private static final int DOCS = 30;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        InfinoNative.dropTable(TABLE);
        InfinoNative.ensureTable(TABLE, "{\"properties\":{\"message\":{\"type\":\"text\"},\"seq\":{\"type\":\"long\"},\"status\":{\"type\":\"keyword\"}}}");
        String[] ids = new String[DOCS];
        String[] sources = new String[DOCS];
        for (int i = 0; i < DOCS; i++) {
            // Two of every three mention disk, once or twice: many hits tie on the score.
            String message = switch (i % 3) {
                case 0 -> "disk disk full";
                case 1 -> "disk full";
                default -> "memory full";
            };
            ids[i] = Integer.toString(i);
            sources[i] = "{\"message\":\"" + message + "\",\"seq\":" + (DOCS - 1 - i) + ",\"status\":\"" + (i % 2 == 0 ? "ok" : "bad") + "\"}";
        }
        assertEquals(DOCS, InfinoNative.indexBatch(TABLE, ids, sources, DOCS).ok());
    }

    @Override
    public void tearDown() throws Exception {
        InfinoNative.dropTable(TABLE);
        super.tearDown();
    }

    private static InfinoSearch.Plan plan(SearchSourceBuilder source) {
        return InfinoSearch.plan(TABLE, source, System.currentTimeMillis());
    }

    private static SearchSourceBuilder disk() {
        return new SearchSourceBuilder().query(QueryBuilders.matchQuery("message", "disk"));
    }

    private static int seq(Map<String, Object> row) {
        return DOCS - 1 - Integer.parseInt(row.get(InfinoQueryTranslator.OSID).toString());
    }

    /** The match's hits by score, best first, then by seq: the order stock sorts {@code [_score, seq]} in. */
    private static List<Map<String, Object>> expected() {
        List<Map<String, Object>> ranked = new ArrayList<>(InfinoNative.sqlRows(InfinoSearch.hitStatement(plan(disk()), DOCS, false)));
        ranked.sort(
            Comparator.<Map<String, Object>>comparingDouble(r -> -((Number) r.get(InfinoSearch.SCORE)).doubleValue())
                .thenComparingInt(InfinoScoreSortTests::seq)
        );
        return ranked;
    }

    /**
     * A scored query sorted by the score then a field: its hits by score,
     * best first, those that tie on it in the field's order, each with its
     * score; the written order would have put the hits with the greatest
     * seq first among each tie.
     */
    public void testAScoredQueryIsOrderedByTheScoreThenTheField() {
        InfinoSearch.Plan plan = plan(disk().sort(new ScoreSortBuilder()).sort("seq", SortOrder.ASC));
        assertTrue(plan.ranksByScore());
        List<Map<String, Object>> rows = InfinoNative.sqlRows(InfinoSearch.hitStatement(plan, 5, false));
        List<Map<String, Object>> want = expected().subList(0, 5);
        assertEquals(5, rows.size());
        for (int i = 0; i < rows.size(); i++) {
            assertEquals(want.get(i).get(InfinoQueryTranslator.OSID), rows.get(i).get(InfinoQueryTranslator.OSID));
            assertEquals(((Number) want.get(i).get(InfinoSearch.SCORE)).doubleValue(), ((Number) rows.get(i).get(InfinoSearch.SCORE)).doubleValue(), 1e-6);
            assertEquals(seq(rows.get(i)), ((Number) rows.get(i).get("seq")).intValue());
        }
        // The first tie holds every twice-mentioning hit, document 27 the one of least seq.
        assertEquals(2, seq(rows.get(0)));
        // No ranked window was cut: the table holds fewer candidates than it ranks.
        assertNull(rows.get(0).get(InfinoSearch.SCORE_FLOOR));
        assertFalse(InfinoSearch.windowCutsPage(plan, rows, 5));
    }

    /**
     * A ranked window too small for the hits tied on the page's last score
     * may have cut one that comes before it on the field: the page carries
     * the greatest score such a hit could have, and asks to be read again
     * over every candidate.
     */
    public void testAWindowCutThroughTheLastTieIsTold() {
        InfinoSearch.Plan plan = plan(disk().sort(new ScoreSortBuilder()).sort("seq", SortOrder.ASC));
        List<Map<String, Object>> cut = InfinoNative.sqlRows(InfinoSearch.hitStatement(plan, 3, false, InfinoSearch.Keyset.WHOLE, 4));
        assertEquals(3, cut.size());
        Object floor = cut.get(0).get(InfinoSearch.SCORE_FLOOR);
        assertNotNull(floor);
        assertEquals(((Number) cut.get(2).get(InfinoSearch.SCORE)).doubleValue(), ((Number) floor).doubleValue(), 1e-6);
        assertTrue(InfinoSearch.windowCutsPage(plan, cut, 3));
        // Read over every candidate, the page is stock's.
        List<Map<String, Object>> whole = InfinoNative.sqlRows(InfinoSearch.hitStatement(plan, 3, false, InfinoSearch.Keyset.WHOLE, DOCS));
        assertNull(whole.get(0).get(InfinoSearch.SCORE_FLOOR));
        for (int i = 0; i < 3; i++) {
            assertEquals(expected().get(i).get(InfinoQueryTranslator.OSID), whole.get(i).get(InfinoQueryTranslator.OSID));
        }
    }

    /**
     * Past 64 sums of the scoring clauses' shares, the text is ranked once
     * rather than in a band per sum: a window cut through it is told by the
     * greatest score a cut-off hit could have, its least BM25 plus every
     * share at its greatest.
     */
    public void testAWindowCutThroughOneRankedReadIsTold() {
        BoolQueryBuilder query = QueryBuilders.boolQuery().should(QueryBuilders.matchQuery("message", "disk"));
        for (int i = 0; i < 7; i++) {
            // Boosts 1, 2, 4, ... 64: every subset sums apart, 128 sums.
            query.should(QueryBuilders.termQuery("seq", i).boost(1 << i));
        }
        assertNull(InfinoSearch.bonusSums(plan(new SearchSourceBuilder().query(query)).query().scoring().bonuses()));
        InfinoSearch.Plan plan = plan(new SearchSourceBuilder().query(query).sort(new ScoreSortBuilder()).sort("seq", SortOrder.ASC));
        assertTrue(plan.ranksByScore());
        List<Map<String, Object>> cut = InfinoNative.sqlRows(InfinoSearch.hitStatement(plan, 3, false, InfinoSearch.Keyset.WHOLE, 4));
        assertEquals(3, cut.size());
        assertNotNull(cut.get(0).get(InfinoSearch.SCORE_FLOOR));
        assertTrue(InfinoSearch.windowCutsPage(plan, cut, 3));
        // Read over every candidate, the page is the hits by score, then seq.
        List<Map<String, Object>> want = new ArrayList<>(
            InfinoNative.sqlRows(InfinoSearch.hitStatement(plan(new SearchSourceBuilder().query(query)), DOCS, false))
        );
        want.sort(
            Comparator.<Map<String, Object>>comparingDouble(r -> -((Number) r.get(InfinoSearch.SCORE)).doubleValue())
                .thenComparingInt(InfinoScoreSortTests::seq)
        );
        List<Map<String, Object>> whole = InfinoNative.sqlRows(InfinoSearch.hitStatement(plan, 3, false, InfinoSearch.Keyset.WHOLE, DOCS));
        assertNull(whole.get(0).get(InfinoSearch.SCORE_FLOOR));
        assertFalse(InfinoSearch.windowCutsPage(plan, whole, 3));
        for (int i = 0; i < 3; i++) {
            assertEquals(want.get(i).get(InfinoQueryTranslator.OSID), whole.get(i).get(InfinoQueryTranslator.OSID));
        }
    }

    /** A query whose hits all score alike is ordered by the field alone, whichever place the score takes. */
    public void testAnUnscoredQueryIsOrderedByTheFieldAlone() {
        for (SearchSourceBuilder source : List.of(
            new SearchSourceBuilder().query(QueryBuilders.matchAllQuery()).sort(new ScoreSortBuilder()).sort("seq", SortOrder.ASC),
            new SearchSourceBuilder().query(QueryBuilders.matchAllQuery()).sort("seq", SortOrder.ASC).sort(new ScoreSortBuilder()),
            new SearchSourceBuilder().query(QueryBuilders.matchAllQuery()).sort(new ScoreSortBuilder().order(SortOrder.ASC)).sort("seq", SortOrder.ASC),
            new SearchSourceBuilder().query(QueryBuilders.boolQuery().filter(QueryBuilders.termQuery("status", "ok"))).sort(new ScoreSortBuilder()).sort("seq", SortOrder.ASC)
        )) {
            InfinoSearch.Plan plan = plan(source);
            assertFalse(plan.ranksByScore());
            assertTrue(plan.sorts().stream().anyMatch(InfinoSearch.SortSpec::isScore));
            List<Map<String, Object>> rows = InfinoNative.sqlRows(InfinoSearch.hitStatement(plan, 4, false));
            assertEquals(4, rows.size());
            for (int i = 1; i < rows.size(); i++) {
                assertTrue(seq(rows.get(i - 1)) < seq(rows.get(i)));
            }
        }
    }

    /** The shapes a scored query's score cannot order as stock orders them are refused, each named. */
    public void testTheShapesNotServedAreRefused() {
        assertRefused(disk().sort("seq", SortOrder.ASC).sort(new ScoreSortBuilder()), "unless _score is the first sort");
        assertRefused(disk().sort(new ScoreSortBuilder().order(SortOrder.ASC)).sort("seq", SortOrder.ASC), "with _score ascending");
        assertRefused(disk().sort(new ScoreSortBuilder()).sort("seq", SortOrder.ASC).collapse(new CollapseBuilder("status")), "with collapse");
        assertRefused(disk().sort(new ScoreSortBuilder()).sort("seq", SortOrder.ASC).searchAfter(new Object[] { 1.0f, 3 }), "with search_after");
        // Text clauses in should, beside no ranked one, score what the table cannot rank.
        SearchSourceBuilder unrankable = new SearchSourceBuilder().query(
            QueryBuilders.boolQuery()
                .should(QueryBuilders.matchQuery("message", "disk"))
                .should(QueryBuilders.matchQuery("message", "memory"))
                .should(QueryBuilders.termQuery("status", "ok"))
        );
        assertRefused(unrankable.sort(new ScoreSortBuilder()).sort("seq", SortOrder.ASC), "whose score the table does not rank");
        // The score alone, or beside _doc, is no sort beside a field.
        assertFalse(plan(disk().sort(new ScoreSortBuilder().order(SortOrder.ASC))).ranksByScore());
        assertTrue(plan(disk().sort(new ScoreSortBuilder()).sort("_doc")).sorts().isEmpty());
    }

    private static void assertRefused(SearchSourceBuilder source, String why) {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> plan(source));
        assertTrue(e.getMessage(), e.getMessage().startsWith("[infino] a sort mixing _score with a field is not served"));
        assertTrue(e.getMessage(), e.getMessage().contains(why));
    }
}
