/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import org.opensearch.infino.InfinoAggregationProcessor.Grouped;
import org.opensearch.infino.InfinoAggregationProcessor.Leaves;
import org.opensearch.infino.InfinoNative.Column;
import org.opensearch.search.aggregations.BucketOrder;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * An unfiltered terms level over a keyword column takes its best groups and
 * the count of the rest from one engine call, and its buckets and
 * sum_other_doc_count come out as the window statement's would.
 */
public class InfinoTopValueCountsTests extends OpenSearchTestCase {

    private static Map<String, Column> columns() {
        Map<String, Column> cols = new HashMap<>();
        cols.put("osid", new Column("osid", "str", false, false));
        cols.put("tag", new Column("tag", "str", true, false));
        cols.put("n", new Column("n", "i64", false, false));
        cols.put("secret", new Column("secret", "str", true, false, s -> "sha256(" + s + ")", null, null));
        return cols;
    }

    private static Grouped terms(TermsAggregationBuilder t) {
        return Grouped.terms(t, columns(), "t", Leaves.NONE, null, false, 0L);
    }

    /** The engine's answer: rows in its order, the keyed rows' count and the rows without a value. */
    private static Map<String, Object> answer(long keyedTotal, long nulls, Object... pairs) {
        List<Map<String, Object>> rows = new java.util.ArrayList<>();
        for (int i = 0; i < pairs.length; i += 2) {
            rows.add(Map.of("tag", pairs[i], InfinoNative.VALUE_COUNT, pairs[i + 1]));
        }
        return Map.of("rows", rows, "keyed_total", keyedTotal, "nulls", nulls, "groups", 1020);
    }

    public void testTheDefaultOrderOverAKeywordColumnTakesTheTopCall() {
        assertTrue(InfinoAggregationProcessor.takesTopValueCounts(terms(new TermsAggregationBuilder("t").field("tag").size(5))));
        // big5's keyword-terms: size 500, a shard size of 760.
        assertTrue(InfinoAggregationProcessor.takesTopValueCounts(terms(new TermsAggregationBuilder("t").field("tag").size(500))));
        // An explicit count order with the key tiebreak is the default order.
        assertTrue(
            InfinoAggregationProcessor.takesTopValueCounts(
                terms(new TermsAggregationBuilder("t").field("tag").size(5).order(BucketOrder.count(false)))
            )
        );
    }

    public void testAnyOtherShapeKeepsItsOwnReads() {
        // Another order: the engine ranks by count and then key only.
        assertFalse(
            InfinoAggregationProcessor.takesTopValueCounts(terms(new TermsAggregationBuilder("t").field("tag").order(BucketOrder.key(true))))
        );
        assertFalse(
            InfinoAggregationProcessor.takesTopValueCounts(terms(new TermsAggregationBuilder("t").field("tag").order(BucketOrder.count(true))))
        );
        // A number key does not order as its text.
        assertFalse(InfinoAggregationProcessor.takesTopValueCounts(terms(new TermsAggregationBuilder("t").field("n"))));
        // A masked key is shown hashed, not as stored.
        assertFalse(InfinoAggregationProcessor.takesTopValueCounts(terms(new TermsAggregationBuilder("t").field("secret"))));
        // A missing value stands in for the rows without one: no bare column.
        assertFalse(InfinoAggregationProcessor.takesTopValueCounts(terms(new TermsAggregationBuilder("t").field("tag").missing("none"))));
        // More groups than are picked here (a shard size past 1000).
        assertFalse(InfinoAggregationProcessor.takesTopValueCounts(terms(new TermsAggregationBuilder("t").field("tag").size(1000))));
    }

    public void testNoAnswerIsNoGroups() {
        assertNull(InfinoAggregationProcessor.topGroups(null, "tag"));
    }

    /**
     * The top groups read ahead with the keyed total: the level keeps its
     * size of them in order, each carrying the count of every keyed row, so
     * sum_other_doc_count is that less the buckets kept, past the groups the
     * engine left out.
     */
    public void testTheTopGroupsAndKeyedTotalGiveTheBucketsAndTheRest() {
        Grouped level = terms(new TermsAggregationBuilder("t").field("tag").size(2));
        // Shard size 2*1.5+10 = 13 asked of the engine; it gave its best 4 (of 1020) in order.
        Map<String, Object> top = answer(1038, 3, "k0042", 5, "k0500", 5, "k1000", 4, "k0007", 3);
        List<Map<String, Object>> groups = InfinoAggregationProcessor.topGroups(top, "tag");
        assertEquals(4, groups.size());
        assertEquals(Map.of("k0", "k0042", "doc_count", 5), groups.get(0));
        Map<String, List<Map<String, Object>>> readAhead = new HashMap<>();
        readAhead.put("inner", groups);
        List<Map<String, Object>> rows = InfinoAggregationProcessor.pickTopTerms(level, "inner", readAhead, 1038L);
        // pickTopTerms keeps the level's shard size; all 4 fit, in order, each with the keyed total.
        assertEquals(4, rows.size());
        assertEquals(List.of("k0042", "k0500", "k1000", "k0007"), rows.stream().map(r -> r.get("k0")).toList());
        for (Map<String, Object> row : rows) {
            assertEquals(1038L, row.get("keyed_total"));
        }
        // Not every group: the window statement would not have read them all either.
        assertFalse(InfinoAggregationProcessor.keptEveryGroup(level, rows));
    }

    /** Every group the column has, fewer than the level takes: the level knows it holds them all. */
    public void testFewerGroupsThanTheSizeAreEveryGroup() {
        Grouped level = terms(new TermsAggregationBuilder("t").field("tag").size(10));
        Map<String, Object> top = answer(5, 1, "a", 2, "b", 2, "c", 1);
        Map<String, List<Map<String, Object>>> readAhead = new HashMap<>();
        readAhead.put("inner", InfinoAggregationProcessor.topGroups(top, "tag"));
        List<Map<String, Object>> rows = InfinoAggregationProcessor.pickTopTerms(level, "inner", readAhead, 5L);
        assertEquals(3, rows.size());
        assertTrue(InfinoAggregationProcessor.keptEveryGroup(level, rows));
    }
}
