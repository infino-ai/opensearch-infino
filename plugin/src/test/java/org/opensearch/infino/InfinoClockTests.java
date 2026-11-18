/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.infino.InfinoNative.Column;
import org.opensearch.test.OpenSearchTestCase;

import java.util.LinkedHashMap;
import java.util.Map;

/** A translation that turns {@code now} into a time reads the clock, wherever {@code now} is written. */
public class InfinoClockTests extends OpenSearchTestCase {

    private static boolean reads(QueryBuilder qb) {
        Map<String, Column> cols = new LinkedHashMap<>();
        cols.put("osid", new Column("osid", "str", false, false));
        cols.put("at", new Column("at", "ts", false, false));
        cols.put("tag", new Column("tag", "str", false, false));
        return InfinoClock.reads(() -> InfinoQueryTranslator.predicate(qb, cols, "t_0", 0L));
    }

    public void testNowInsideQueryTextReadsTheClock() {
        assertTrue(reads(QueryBuilders.queryStringQuery("at:[* TO now]")));
        assertTrue(reads(QueryBuilders.queryStringQuery("at:>now-1h")));
        assertTrue(reads(QueryBuilders.boolQuery().filter(QueryBuilders.queryStringQuery("tag:x AND at:[now-1d TO now]"))));
        assertTrue(reads(QueryBuilders.rangeQuery("at").gte("now-1d")));
    }

    public void testFixedDatesDoNotReadTheClock() {
        assertFalse(reads(QueryBuilders.queryStringQuery("at:[2020-01-01 TO 2021-01-01]")));
        assertFalse(reads(QueryBuilders.queryStringQuery("tag:now")));
        assertFalse(reads(QueryBuilders.rangeQuery("at").gte("2020-01-01||+1d")));
        assertFalse(reads(QueryBuilders.matchAllQuery()));
    }

    public void testAnOuterWatchSeesAnInnerRead() {
        boolean[] inner = new boolean[1];
        assertTrue(InfinoClock.reads(() -> inner[0] = InfinoClock.reads(() -> InfinoClock.read(1L))));
        assertTrue(inner[0]);
    }
}
