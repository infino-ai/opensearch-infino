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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

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

    /**
     * Tracking every hit has no limit, though the request carries the largest
     * int as one: a table holds more rows than any Lucene shard holds
     * documents, and a total past that number is reported as counted.
     */
    public void testTrackingEveryHitCountsPastTheLargestInt() {
        SearchSourceBuilder source = new SearchSourceBuilder().trackTotalHits(true);
        long justPast = (long) Integer.MAX_VALUE + 1;
        long aTrillion = 1_000_000_000_000L;
        assertEquals(new TotalHits(Integer.MAX_VALUE, TotalHits.Relation.EQUAL_TO), InfinoSearch.reported(Integer.MAX_VALUE, true, source));
        assertEquals(new TotalHits(justPast, TotalHits.Relation.EQUAL_TO), InfinoSearch.reported(justPast, true, source));
        assertEquals(new TotalHits(aTrillion, TotalHits.Relation.EQUAL_TO), InfinoSearch.reported(aTrillion, true, source));
        // A limit the request names is still a limit, however many rows there are.
        SearchSourceBuilder limited = new SearchSourceBuilder().trackTotalHitsUpTo(50_000);
        assertEquals(new TotalHits(50_000, TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO), InfinoSearch.reported(aTrillion, true, limited));
        // And a search that tracks none reports none.
        assertNull(InfinoSearch.reported(aTrillion, true, new SearchSourceBuilder().trackTotalHits(false)));
    }

    /**
     * A total is counted one past the track_total_hits limit, or one past
     * terminate_after when that is larger, so a search can tell whether more
     * documents match than terminate_after names.
     */
    public void testATotalIsCountedOnePastTheLimitOrTerminateAfter() {
        assertEquals(CAP + 1, InfinoSearch.countCap(new SearchSourceBuilder()));
        assertEquals(CAP + 1, InfinoSearch.countCap(new SearchSourceBuilder().terminateAfter(5)));
        assertEquals(100_001, InfinoSearch.countCap(new SearchSourceBuilder().terminateAfter(100_000)));
        assertEquals(Long.MAX_VALUE, InfinoSearch.countCap(new SearchSourceBuilder().trackTotalHits(true).terminateAfter(100_000)));
        assertEquals(Long.MAX_VALUE, InfinoSearch.countCap(new SearchSourceBuilder().trackTotalHits(false).terminateAfter(100_000)));
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

    /** A table a page is read from, named in the log when its count fails. */
    private static final String TABLE = "os_logs_0";

    /** A short page's rows: fewer than its size. */
    private static final int SHORT_PAGE_ROWS = 3;

    /** A count of more rows than a page holds. */
    private static final long COUNTED = 42;

    /**
     * A short page is its own total: the count that ran beside it is waited
     * out, and its failure does not fail the page, whose answer is already
     * in hand. A full page's total is the count, so its failure fails the page.
     */
    public void testAFailedCountBesideAShortPageLeavesThePageItsTotal() {
        RuntimeException failed = new IllegalStateException("count");
        assertEquals(
            SHORT_PAGE_ROWS,
            InfinoSearch.besidePageTotal(TABLE, CompletableFuture.failedFuture(failed), true, SHORT_PAGE_ROWS)
        );
        RuntimeException thrown = expectThrows(
            IllegalStateException.class,
            () -> InfinoSearch.besidePageTotal(TABLE, CompletableFuture.failedFuture(failed), false, SHORT_PAGE_ROWS)
        );
        assertSame(failed, thrown);
    }

    /** A count beside a short page is waited for but not used; beside a full page it is the total. */
    public void testACountBesideAPageIsTheTotalOnlyWhenThePageIsFull() {
        assertEquals(
            SHORT_PAGE_ROWS,
            InfinoSearch.besidePageTotal(TABLE, CompletableFuture.completedFuture(COUNTED), true, SHORT_PAGE_ROWS)
        );
        assertEquals(COUNTED, InfinoSearch.besidePageTotal(TABLE, CompletableFuture.completedFuture(COUNTED), false, SHORT_PAGE_ROWS));
    }

    /** Rows of a page as its statement chose them: ids only. */
    private static List<Map<String, Object>> idRows(String... ids) {
        List<Map<String, Object>> rows = new ArrayList<>();
        for (String id : ids) {
            rows.add(Map.of(InfinoQueryTranslator.OSID, id));
        }
        return rows;
    }

    /** A source lookup over {@code held}: the ids the table still holds, each with a source naming it. */
    private static Function<List<Map<String, Object>>, List<Map<String, Object>>> holding(
        List<String> held,
        List<List<String>> asked
    ) {
        return rows -> InfinoSearch.withSources(rows, 0, 10, ids -> {
            asked.add(List.copyOf(ids));
            Map<String, String> out = new HashMap<>();
            for (String id : ids) {
                if (held.contains(id)) {
                    out.put(id, "{\"id\":\"" + id + "\"}");
                }
            }
            return out;
        });
    }

    private static List<Object> ids(List<Map<String, Object>> rows) {
        return rows.stream().map(r -> r.get(InfinoQueryTranslator.OSID)).toList();
    }

    public void testAPageWhoseSourcesAreAllThereIsReadOnce() {
        List<List<String>> asked = new ArrayList<>();
        int[] reads = { 0 };
        int[] chosen = { 0 };
        List<Map<String, Object>> page = InfinoSearch.sourcedRows(() -> {
            reads[0]++;
            return idRows("d1", "d2");
        }, holding(List.of("d1", "d2"), asked), 10, chosen);
        assertEquals(List.of("d1", "d2"), ids(page));
        assertEquals(1, reads[0]);
        assertEquals(2, chosen[0]);
    }

    public void testARowDeletedBetweenTheReadsIsReadAgainAndTheNextRowTakesItsPlace() {
        // The first read chooses d1 d2 d3; d2 is deleted before its source is
        // read. The page is read again and chooses d1 d3 d4, all still held.
        List<List<String>> asked = new ArrayList<>();
        List<List<Map<String, Object>>> choices = new ArrayList<>(List.of(idRows("d1", "d2", "d3"), idRows("d1", "d3", "d4")));
        int[] chosen = { 0 };
        List<Map<String, Object>> page = InfinoSearch.sourcedRows(
            () -> choices.remove(0),
            holding(List.of("d1", "d3", "d4"), asked),
            3,
            chosen
        );
        assertEquals(List.of("d1", "d3", "d4"), ids(page));
        assertEquals("{\"id\":\"d4\"}", page.get(2).get(InfinoQueryTranslator.SOURCE));
        assertEquals(2, asked.size());
        assertEquals(3, chosen[0]);
    }

    public void testARowThatKeepsVanishingLeavesAFullPageShortAndStillCountsAsChosen() {
        // Every read chooses d2, which has no source: after the last re-read the
        // page goes without it, and the count of rows chosen stays 3, so the page
        // is not taken for the last one.
        List<List<String>> asked = new ArrayList<>();
        int[] chosen = { 0 };
        List<Map<String, Object>> page = InfinoSearch.sourcedRows(
            () -> idRows("d1", "d2", "d3"),
            holding(List.of("d1", "d3"), asked),
            3,
            chosen
        );
        assertEquals(List.of("d1", "d3"), ids(page));
        assertEquals(InfinoSearch.SOURCE_REREADS + 1, asked.size());
        assertEquals(3, chosen[0]);
    }

    public void testAShortChoiceIsNotReadAgain() {
        // Two rows chosen for a page of three: the table ran out, nothing vanished.
        List<List<String>> asked = new ArrayList<>();
        int[] chosen = { 0 };
        List<Map<String, Object>> page = InfinoSearch.sourcedRows(
            () -> idRows("d1", "d2"),
            holding(List.of("d1", "d2"), asked),
            3,
            chosen
        );
        assertEquals(List.of("d1", "d2"), ids(page));
        assertEquals(1, asked.size());
        assertEquals(2, chosen[0]);
    }

    public void testAScrollRoundEndsOnlyWhenTheTableRunsOut() {
        // A full page that lost a hit to a delete pages on.
        assertFalse(InfinoQueryPhaseSearcher.scrollRoundDone(1, 2, 3, 3));
        // A page that chose fewer rows than asked is the last.
        assertTrue(InfinoQueryPhaseSearcher.scrollRoundDone(1, 2, 2, 3));
        // Enough known rows ends the round.
        assertTrue(InfinoQueryPhaseSearcher.scrollRoundDone(3, 3, 3, 3));
        // A page left with no hit has nothing to page on from.
        assertTrue(InfinoQueryPhaseSearcher.scrollRoundDone(0, 0, 3, 3));
    }

    public void testADocumentWithTheIdErrorIsASource() {
        // The ids sit under "sources", so an id spelling "error" is a document, not a failed read.
        Map<String, String> sources = InfinoNative.sourcesOf("{\"sources\":{\"error\":\"{\\\"a\\\":1}\",\"zz\":\"{}\"}}");
        assertEquals(Map.of("error", "{\"a\":1}", "zz", "{}"), sources);
        assertEquals(Map.of(), InfinoNative.sourcesOf("{\"sources\":{}}"));
        expectThrows(InfinoNative.InfinoException.class, () -> InfinoNative.sourcesOf("{\"error\":\"{}\"}"));
    }

    public void testARowDeletedBeforeItsSourceIsReadLeavesThePage() {
        // d2 was deleted between the page's two reads: it is left out, never shown with no source.
        List<Map<String, Object>> page = InfinoSearch.withSources(
            idRows("d1", "d2", "d3"),
            0,
            10,
            ids -> Map.of("d1", "{\"a\":1}", "d3", "{\"a\":3}")
        );
        assertEquals(2, page.size());
        assertEquals("d1", page.get(0).get(InfinoQueryTranslator.OSID));
        assertEquals("{\"a\":1}", page.get(0).get(InfinoQueryTranslator.SOURCE));
        assertEquals("d3", page.get(1).get(InfinoQueryTranslator.OSID));
        assertEquals("{\"a\":3}", page.get(1).get(InfinoQueryTranslator.SOURCE));
    }

    public void testARowDeletedInTheWindowLeavesItAndTheRowsBeforeItStay() {
        // from 1: d1 is before the window and keeps no source; d3 was deleted between the reads.
        List<Map<String, Object>> page = InfinoSearch.withSources(idRows("d1", "d2", "d3"), 1, 10, ids -> Map.of("d2", "{}"));
        assertEquals(2, page.size());
        assertEquals("d1", page.get(0).get(InfinoQueryTranslator.OSID));
        assertFalse(page.get(0).containsKey(InfinoQueryTranslator.SOURCE));
        assertEquals("d2", page.get(1).get(InfinoQueryTranslator.OSID));
    }

    public void testOnlyTheWindowsSourcesAreRead() {
        // from 2, size 2: the rows before the window keep no source and are not looked up.
        List<List<String>> asked = new ArrayList<>();
        List<Map<String, Object>> page = InfinoSearch.withSources(idRows("d1", "d2", "d3", "d4", "d5"), 2, 4, ids -> {
            asked.add(ids);
            return Map.of("d3", "{}", "d4", "{}");
        });
        assertEquals(List.of(List.of("d3", "d4")), asked);
        assertEquals(4, page.size());
        assertFalse(page.get(0).containsKey(InfinoQueryTranslator.SOURCE));
        assertFalse(page.get(1).containsKey(InfinoQueryTranslator.SOURCE));
        assertEquals("{}", page.get(2).get(InfinoQueryTranslator.SOURCE));
        assertEquals("d4", page.get(3).get(InfinoQueryTranslator.OSID));
    }

    public void testAWindowPastThePageReadsNoSource() {
        List<Map<String, Object>> page = InfinoSearch.withSources(idRows("d1"), 5, 10, ids -> {
            throw new AssertionError("no lookup expected");
        });
        assertEquals(1, page.size());
    }
}
