/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import org.apache.lucene.search.TotalHits;
import org.opensearch.infino.InfinoSearch.CountedRows;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/** The hits.total a proxy search reports, as a coordinator reports it under track_total_hits. */
public class InfinoSearchTotalsTests extends OpenSearchTestCase {

    private static final int CAP = SearchContext.DEFAULT_TRACK_TOTAL_HITS_UP_TO;

    public void testASearchThatNamesNoLimitCountsUpToTheDefaultCap() {
        SearchSourceBuilder source = new SearchSourceBuilder();
        assertEquals(new TotalHits(CAP, TotalHits.Relation.EQUAL_TO), InfinoSearch.reported(CAP, true, source));
        assertEquals(new TotalHits(CAP, TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO), InfinoSearch.reported(CAP + 1, true, source));
    }

    public void testTrackingEveryHitCountsPastTheDefaultCap() {
        SearchSourceBuilder source = new SearchSourceBuilder().trackTotalHits(true);
        assertEquals(new TotalHits(CAP + 1, TotalHits.Relation.EQUAL_TO), InfinoSearch.reported(CAP + 1, true, source));
    }

    /** A hit statement's one row. */
    private static final List<Map<String, Object>> ONE_ROW = List.of(Map.of(InfinoQueryTranslator.OSID, "d1"));

    /**
     * A scored page whose exact total is 0 counts it and plans no hit
     * statement: the count is the one engine call.
     */
    public void testAScoredPageWithAnExactZeroTotalReadsNoHitStatement() {
        assertTrue(InfinoSearch.countsFirst(new SearchSourceBuilder(), true, true));
        List<String> calls = new ArrayList<>();
        CountedRows read = InfinoSearch.countedRows(true, () -> {
            calls.add("count");
            return 0;
        }, () -> {
            calls.add("hits");
            return ONE_ROW;
        });
        assertEquals(List.of("count"), calls);
        assertTrue(read.rows().isEmpty());
        assertEquals(Long.valueOf(0), read.total());
    }

    /** A scored page with hits counts its total, then reads them: the total is not counted again. */
    public void testAScoredPageWithHitsCountsThenReadsThem() {
        List<String> calls = new ArrayList<>();
        CountedRows read = InfinoSearch.countedRows(true, () -> {
            calls.add("count");
            return 3;
        }, () -> {
            calls.add("hits");
            return ONE_ROW;
        });
        assertEquals(List.of("count", "hits"), calls);
        assertEquals(ONE_ROW, read.rows());
        assertEquals(Long.valueOf(3), read.total());
    }

    /**
     * A page that counts no exact total (track_total_hits: false, a request
     * that counts none, or an unscored page) reads its hits alone, first.
     */
    public void testAPageThatCountsNoExactTotalReadsItsHitsAlone() {
        assertFalse(InfinoSearch.countsFirst(new SearchSourceBuilder().trackTotalHits(false), true, true));
        assertFalse(InfinoSearch.countsFirst(new SearchSourceBuilder(), false, true));
        assertFalse(InfinoSearch.countsFirst(new SearchSourceBuilder(), true, false));
        List<String> calls = new ArrayList<>();
        CountedRows read = InfinoSearch.countedRows(false, () -> {
            calls.add("count");
            return 0;
        }, () -> {
            calls.add("hits");
            return ONE_ROW;
        });
        assertEquals(List.of("hits"), calls);
        assertEquals(ONE_ROW, read.rows());
        assertNull(read.total());
    }

    /**
     * A total counted beside the page is never below the hits it returns:
     * an append between the count and the hit read cannot report one hit
     * with a total of 0.
     */
    public void testATotalCountedBesideThePageIsNeverBelowItsHits() {
        assertEquals(1, InfinoSearch.besideTotal(0, 1));
        assertEquals(7, InfinoSearch.besideTotal(7, 3));
        assertEquals(0, InfinoSearch.besideTotal(0, 0));
    }

    /** Rows of a page as its statement chose them: ids only. */
    private static List<Map<String, Object>> idRows(String... ids) {
        List<Map<String, Object>> rows = new ArrayList<>();
        for (String id : ids) {
            rows.add(Map.of(InfinoQueryTranslator.OSID, id));
        }
        return rows;
    }

    public void testARowDeletedBeforeItsSourceIsReadLeavesThePage() {
        // d2 was deleted between the page's two reads: it is left out, never shown with no source.
        List<Map<String, Object>> page = InfinoSearch.withSources(
            idRows("d1", "d2", "d3"),
            10,
            ids -> Map.of("d1", "{\"a\":1}", "d3", "{\"a\":3}")
        );
        assertEquals(2, page.size());
        assertEquals("d1", page.get(0).get(InfinoQueryTranslator.OSID));
        assertEquals("{\"a\":1}", page.get(0).get(InfinoQueryTranslator.SOURCE));
        assertEquals("d3", page.get(1).get(InfinoQueryTranslator.OSID));
        assertEquals("{\"a\":3}", page.get(1).get(InfinoQueryTranslator.SOURCE));
    }
}
