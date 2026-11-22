/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import org.opensearch.core.index.Index;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.infino.InfinoScriptTranslator.SqlType;
import org.opensearch.search.SearchHit;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.collapse.CollapseBuilder;
import org.opensearch.search.sort.ScoreSortBuilder;
import org.opensearch.search.sort.SortOrder;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
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
    /** The documents tagged "keep": two that mention disk once, so rank below every twice-mentioning one. */
    private static final List<String> KEPT = List.of("1", "4");

    @Override
    public void setUp() throws Exception {
        super.setUp();
        InfinoNative.dropTable(TABLE);
        InfinoNative.ensureTable(TABLE, "{\"properties\":{\"message\":{\"type\":\"text\"},\"seq\":{\"type\":\"long\"},\"status\":{\"type\":\"keyword\"},\"tags\":{\"type\":\"keyword\"}}}");
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
            sources[i] = "{\"message\":\""
                + message
                + "\",\"seq\":"
                + (DOCS - 1 - i)
                + ",\"status\":\""
                + (i % 2 == 0 ? "ok" : "bad")
                + "\",\"tags\":[\""
                + (KEPT.contains(ids[i]) ? "keep" : "drop")
                + "\",\"t"
                + i % 3
                + "\"]}";
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

    /** The match on message, its hits kept by a term on tags, a field each document holds several values of, which the engine applies after it ranks. */
    private static BoolQueryBuilder diskKept() {
        return QueryBuilders.boolQuery().must(QueryBuilders.matchQuery("message", "disk")).filter(QueryBuilders.termQuery("tags", "keep"));
    }

    /**
     * A filter applied after the ranking may remove every hit the ranked
     * window read while matches it keeps rank past the window: the page,
     * left empty, asks to be read again over every candidate, and is then
     * the kept hits by score, then seq.
     */
    public void testAFilterAfterTheRankingThatEmptiesThePageIsTold() {
        InfinoSearch.Plan plan = plan(new SearchSourceBuilder().query(diskKept()).sort(new ScoreSortBuilder()).sort("seq", SortOrder.ASC));
        assertTrue(plan.ranksByScore());
        assertTrue(InfinoSearch.narrowsAfterRanking(plan.query(), plan.hitWhere()));
        // The four best candidates mention disk twice: the filter keeps none.
        List<Map<String, Object>> cut = InfinoNative.sqlRows(InfinoSearch.hitStatement(plan, 3, false, InfinoSearch.Keyset.WHOLE, 4));
        assertTrue(cut.toString(), cut.isEmpty());
        assertTrue(InfinoSearch.windowCutsPage(plan, cut, 3));
        List<Map<String, Object>> whole = InfinoNative.sqlRows(InfinoSearch.hitStatement(plan, 3, false, InfinoSearch.Keyset.WHOLE, DOCS));
        assertEquals(List.of("4", "1"), whole.stream().map(r -> r.get(InfinoQueryTranslator.OSID).toString()).toList());
        assertEquals(1, ((Number) whole.get(0).get(InfinoSearch.SCORE_BANDS)).intValue());
        // A page the filter left short asks too: its window may have cut what fills it.
        assertTrue(InfinoSearch.windowCutsPage(plan, whole, 3));
        // Unsorted, a page ranked by more than the match is read the same way.
        BoolQueryBuilder bonus = diskKept().should(QueryBuilders.termQuery("status", "ok"));
        InfinoSearch.Plan scored = plan(new SearchSourceBuilder().query(bonus));
        assertTrue(InfinoSearch.scoredPage(scored));
        String sql = InfinoSearch.hitStatement(scored, 3, false, InfinoSearch.Keyset.WHOLE, 4);
        assertTrue(sql, sql.contains(InfinoSearch.SCORE_BANDS));
        cut = InfinoNative.sqlRows(sql);
        assertTrue(cut.toString(), cut.isEmpty());
        assertTrue(InfinoSearch.windowCutsPage(scored, cut, 3));
        // With no step after the ranking, the statement counts no bands.
        assertFalse(InfinoSearch.hitStatement(plan(disk().sort(new ScoreSortBuilder()).sort("seq", SortOrder.ASC)), 3, false).contains(InfinoSearch.SCORE_BANDS));
    }

    /**
     * Narrowed after the ranking, a band that kept no row bounds nothing its
     * window cut off: a full page missing a band asks to be read again,
     * however its last score compares with the floor of those it kept.
     */
    public void testABandTheFilterEmptiedIsTold() {
        InfinoSearch.Plan plan = plan(
            new SearchSourceBuilder().query(diskKept().should(QueryBuilders.termQuery("status", "ok"))).sort(new ScoreSortBuilder()).sort("seq", SortOrder.ASC)
        );
        assertEquals(2, InfinoSearch.rankedBands(plan.query()));
        Map<String, Object> row = new java.util.HashMap<>();
        row.put(InfinoQueryTranslator.OSID, "4");
        row.put(InfinoSearch.SCORE, 2.0);
        row.put(InfinoSearch.SCORE_FLOOR, 1.0);
        row.put(InfinoSearch.SCORE_BANDS, 1L);
        assertTrue(InfinoSearch.windowCutsPage(plan, List.of(row), 1));
        row.put(InfinoSearch.SCORE_BANDS, 2L);
        assertFalse(InfinoSearch.windowCutsPage(plan, List.of(row), 1));
        row.put(InfinoSearch.SCORE, 1.0);
        assertTrue(InfinoSearch.windowCutsPage(plan, List.of(row), 1));
    }

    /**
     * A match in filter context scores nothing, as in stock: its hits all
     * score 0, so a sort by the score then seq orders them by seq alone.
     */
    public void testAFilterMatchIsOrderedByTheFieldAlone() {
        InfinoSearch.Plan plan = plan(
            new SearchSourceBuilder().query(QueryBuilders.boolQuery().filter(QueryBuilders.matchQuery("message", "disk")))
                .sort(new ScoreSortBuilder())
                .sort("seq", SortOrder.ASC)
        );
        assertFalse(plan.scored());
        assertFalse(plan.ranksByScore());
        assertEquals(0.0f, InfinoSearch.constantScore(plan.query().scoring()), 0);
        List<Map<String, Object>> rows = InfinoNative.sqlRows(InfinoSearch.hitStatement(plan, DOCS, false));
        assertEquals(20, rows.size());
        for (int i = 1; i < rows.size(); i++) {
            assertTrue(seq(rows.get(i - 1)) < seq(rows.get(i)));
        }
        // Beside a match that scores, the filter's text narrows its hits and adds nothing.
        InfinoSearch.Plan full = plan(
            new SearchSourceBuilder().query(
                QueryBuilders.boolQuery().must(QueryBuilders.matchQuery("message", "full")).filter(QueryBuilders.matchQuery("message", "disk"))
            ).sort(new ScoreSortBuilder()).sort("seq", SortOrder.ASC)
        );
        assertTrue(full.ranksByScore());
        rows = InfinoNative.sqlRows(InfinoSearch.hitStatement(full, DOCS, false));
        assertEquals(20, rows.size());
        // Every hit holds "full" once in a message of two or three words: those of two outrank those of three, each tie by seq.
        for (int i = 1; i < rows.size(); i++) {
            double before = ((Number) rows.get(i - 1).get(InfinoSearch.SCORE)).doubleValue();
            double after = ((Number) rows.get(i).get(InfinoSearch.SCORE)).doubleValue();
            assertTrue(before >= after);
            assertTrue(before > after || seq(rows.get(i - 1)) < seq(rows.get(i)));
        }
        // The first is the two-word message of least seq: document 28.
        assertEquals("28", rows.get(0).get(InfinoQueryTranslator.OSID).toString());
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

    /**
     * Under track_scores, a scored query sorted by a field alone is ordered
     * by the field, each hit carrying the score its unsorted page gives it,
     * and max_score the best score of every match, not of the page alone:
     * from the first hit and past it, with a bonus clause and with a filter
     * applied after the ranking.
     */
    public void testTrackScoresBesideAFieldSortScoresEachHitAsUnsorted() {
        List<SearchSourceBuilder> queries = List.of(
            disk(),
            new SearchSourceBuilder().query(
                QueryBuilders.boolQuery().must(QueryBuilders.matchQuery("message", "disk")).should(QueryBuilders.termQuery("status", "ok"))
            ),
            new SearchSourceBuilder().query(
                QueryBuilders.boolQuery().must(QueryBuilders.matchQuery("message", "disk")).filter(QueryBuilders.termQuery("tags", "keep"))
            )
        );
        for (SearchSourceBuilder query : queries) {
            InfinoSearch.Page unsorted = page(copy(query).size(DOCS));
            Map<String, Float> want = new HashMap<>();
            for (SearchHit hit : unsorted.hits()) {
                want.put(hit.getId(), hit.getScore());
            }
            assertFalse(want.isEmpty());
            float best = want.values().stream().max(Float::compare).orElseThrow();
            assertEquals(best, unsorted.maxScore(), 1e-6);
            for (int from : new int[] { 0, 1 }) {
                SearchSourceBuilder sorted = copy(query).sort("seq", SortOrder.ASC).trackScores(true).from(from).size(4);
                InfinoSearch.Plan plan = plan(sorted);
                assertTrue(InfinoSearch.scoresBesideSort(plan, sorted));
                InfinoSearch.Page page = page(sorted);
                // The page is every row up to its end; the window is cut after the merge.
                assertEquals(Math.min(from + 4, want.size()), page.hits().size());
                int lastSeq = -1;
                for (SearchHit hit : page.hits()) {
                    int seq = DOCS - 1 - Integer.parseInt(hit.getId());
                    assertTrue(lastSeq < seq);
                    lastSeq = seq;
                    assertEquals(query + " hit " + hit.getId(), want.get(hit.getId()), hit.getScore(), 1e-6);
                    assertEquals(((Number) hit.getSortValues()[0]).intValue(), seq);
                }
                assertEquals(query.toString(), best, page.maxScore(), 1e-6);
                // Without track_scores the hits carry no score, and the page no max_score.
                InfinoSearch.Page untracked = page(copy(query).sort("seq", SortOrder.ASC).from(from).size(4));
                assertTrue(Float.isNaN(untracked.hits().get(0).getScore()));
                assertTrue(Float.isNaN(untracked.maxScore()));
            }
        }
        // A query whose hits all score alike keeps scoring the constant.
        SearchSourceBuilder constant = new SearchSourceBuilder().query(QueryBuilders.matchAllQuery())
            .sort("seq", SortOrder.ASC)
            .trackScores(true)
            .size(3);
        assertFalse(InfinoSearch.scoresBesideSort(plan(constant), constant));
        InfinoSearch.Page page = page(constant);
        assertEquals(1.0f, page.hits().get(0).getScore(), 0);
        assertEquals(1.0f, page.maxScore(), 0);
    }

    /**
     * A missing sort value carries stock's stand-in: the least long (int,
     * on an integer, short or byte field) or double when the missing rows' side comes last in a descending sort or
     * first in an ascending one, else the greatest; null on any other type.
     * A search_after value equal to the stand-in resumes among the missing rows.
     */
    public void testAMissingSortValueCarriesStocksStandIn() {
        for (boolean desc : new boolean[] { false, true }) {
            for (boolean nullsFirst : new boolean[] { false, true }) {
                boolean least = desc != nullsFirst;
                InfinoSearch.SortSpec longs = new InfinoSearch.SortSpec("n", "n", SqlType.INT, desc, nullsFirst, false);
                InfinoSearch.SortSpec dates = new InfinoSearch.SortSpec("d", "d", SqlType.DATE, desc, nullsFirst, false);
                InfinoSearch.SortSpec doubles = new InfinoSearch.SortSpec("f", "f", SqlType.FLOAT, desc, nullsFirst, false);
                InfinoSearch.SortSpec strings = new InfinoSearch.SortSpec("k", "k", SqlType.STRING, desc, nullsFirst, false);
                InfinoSearch.SortSpec ints = new InfinoSearch.SortSpec("i", "i", SqlType.INT, desc, nullsFirst, false, true);
                assertEquals(least ? Long.MIN_VALUE : Long.MAX_VALUE, longs.orStandIn(null));
                // An integer, short or byte field: the 32-bit int bound, as stock's int comparator.
                assertEquals(least ? (long) Integer.MIN_VALUE : (long) Integer.MAX_VALUE, ints.orStandIn(null));
                assertEquals(least ? Long.MIN_VALUE : Long.MAX_VALUE, dates.orStandIn(null));
                assertEquals(least ? Double.NEGATIVE_INFINITY : Double.POSITIVE_INFINITY, doubles.orStandIn(null));
                assertNull(strings.orStandIn(null));
                assertEquals(7L, longs.orStandIn(7L));
                assertEquals("a", strings.orStandIn("a"));
                for (InfinoSearch.SortSpec s : List.of(longs, ints, dates, doubles)) {
                    assertTrue(s.missingStandIn(s.standIn()));
                    assertTrue(s.missingStandIn(s.standIn().toString()));
                    assertFalse(s.missingStandIn(7L));
                }
                assertFalse(strings.missingStandIn(null));
                // A sort whose missing value stands in a given value has no missing rows.
                InfinoSearch.SortSpec given = new InfinoSearch.SortSpec("n", "n", SqlType.INT, desc, nullsFirst, true);
                assertNull(given.orStandIn(null));
                assertFalse(given.missingStandIn(given.standIn()));
            }
        }
    }

    /** The hit statement leaves out each hit's source when asked to, as the score reads under track_scores do. */
    public void testAHitStatementWithoutSources() {
        InfinoSearch.Plan plan = plan(disk());
        String with = InfinoSearch.hitStatement(plan, 3, false, InfinoSearch.Keyset.WHOLE, 4, true);
        String without = InfinoSearch.hitStatement(plan, 3, false, InfinoSearch.Keyset.WHOLE, 4, false);
        assertTrue(with, with.contains(InfinoQueryTranslator.SOURCE));
        assertFalse(without, without.contains(InfinoQueryTranslator.SOURCE));
        List<Map<String, Object>> rows = InfinoNative.sqlRows(without);
        assertEquals(3, rows.size());
        assertNull(rows.get(0).get(InfinoQueryTranslator.SOURCE));
        assertTrue(rows.get(0).get(InfinoSearch.SCORE) instanceof Number);
    }

    private static InfinoSearch.Page page(SearchSourceBuilder source) {
        return InfinoSearch.page(new Index(TABLE, "uuid-" + TABLE), plan(source), source.fetchSource(false), true);
    }

    private static SearchSourceBuilder copy(SearchSourceBuilder source) {
        return new SearchSourceBuilder().query(source.query());
    }

    private static void assertRefused(SearchSourceBuilder source, String why) {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> plan(source));
        assertTrue(e.getMessage(), e.getMessage().startsWith("[infino] a sort mixing _score with a field is not served"));
        assertTrue(e.getMessage(), e.getMessage().contains(why));
    }
}
