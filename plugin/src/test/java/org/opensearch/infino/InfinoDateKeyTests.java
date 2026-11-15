/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import org.opensearch.test.OpenSearchTestCase;

public class InfinoDateKeyTests extends OpenSearchTestCase {

    private static final long HOUR = 3_600_000L;

    /**
     * date_bin takes only the dates an i64 of nanoseconds holds; the rest,
     * less an interval at each end, are floored in epoch milliseconds.
     */
    public void testDateBinOnlyWhereNanosecondsHoldTheDate() {
        String key = InfinoAggregationProcessor.Grouped.utcBin("(ts)", HOUR, 0);
        long lo = Long.MIN_VALUE / 1_000_000L + HOUR;
        long hi = Long.MAX_VALUE / 1_000_000L - HOUR;
        assertEquals(
            "CASE WHEN arrow_cast((ts), 'Int64') BETWEEN " + lo + " AND " + hi
                + " THEN date_bin(INTERVAL '3600000 milliseconds', (ts))"
                + " ELSE arrow_cast((arrow_cast((ts), 'Int64') - (0)) - (((arrow_cast((ts), 'Int64') - (0)) % 3600000) + 3600000) % 3600000 + (0),"
                + " 'Timestamp(Millisecond, Some(\"UTC\"))') END",
            key
        );
    }

    /** An offset moves the origin of both branches, and narrows date_bin's range by its size. */
    public void testAnOffsetShiftsBothBranches() {
        long offset = -6 * HOUR;
        String key = InfinoAggregationProcessor.Grouped.utcBin("(ts)", 24 * HOUR, offset);
        assertTrue(key, key.contains("date_bin(INTERVAL '86400000 milliseconds', (ts), TIMESTAMP '1970-01-01T00:00:00Z' + INTERVAL '-21600000 milliseconds')"));
        assertTrue(key, key.contains("BETWEEN " + (Long.MIN_VALUE / 1_000_000L + 30 * HOUR) + " AND "));
        assertTrue(key, key.contains("(arrow_cast((ts), 'Int64') - (-21600000))"));
        assertTrue(key, key.contains(" + (-21600000), 'Timestamp"));
    }

    /** An interval too long for any date_bin range is floored in milliseconds alone. */
    public void testAnIntervalPastTheRangeIsFlooredAlone() {
        long ms = Long.MAX_VALUE / 1_000_000L + 1;
        String key = InfinoAggregationProcessor.Grouped.utcBin("(ts)", ms, 0);
        assertFalse(key, key.contains("date_bin"));
        assertTrue(key, key.startsWith("arrow_cast((arrow_cast((ts), 'Int64') - (0))"));
    }
}
