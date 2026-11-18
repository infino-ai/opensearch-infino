/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import org.opensearch.infino.InfinoScriptTranslator.SqlType;
import org.opensearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.opensearch.test.OpenSearchTestCase;

import java.time.ZoneId;

public class InfinoDateKeyTests extends OpenSearchTestCase {

    private static final long HOUR = 3_600_000L;
    /** The source as UTC milliseconds, floored, which both branches read. */
    private static final String MILLIS = "arrow_cast(date_trunc('millisecond', (ts)), 'Timestamp(Millisecond, Some(\"UTC\"))')";
    /** The local wall-clock time of the source in the zone, in milliseconds with no zone, which both zoned branches read. */
    private static final String LOCAL = "arrow_cast(to_local_time((ts)), 'Timestamp(Millisecond, None)')";
    private static final String KOLKATA = "'Timestamp(Millisecond, Some(\"Asia/Kolkata\"))'";

    /**
     * date_bin takes only the dates an i64 of nanoseconds holds; the rest,
     * less an interval at each end, are floored in epoch milliseconds.
     */
    public void testDateBinOnlyWhereNanosecondsHoldTheDate() {
        String key = InfinoAggregationProcessor.Grouped.utcBin("(ts)", HOUR, 0);
        long lo = Long.MIN_VALUE / 1_000_000L + HOUR;
        long hi = Long.MAX_VALUE / 1_000_000L - HOUR;
        String epoch = "arrow_cast(" + MILLIS + ", 'Int64')";
        assertEquals(
            "CASE WHEN " + epoch + " BETWEEN " + lo + " AND " + hi
                + " THEN date_bin(INTERVAL '3600000 milliseconds', " + MILLIS + ")"
                + " ELSE arrow_cast((" + epoch + " - (0)) - (((" + epoch + " - (0)) % 3600000) + 3600000) % 3600000 + (0),"
                + " 'Timestamp(Millisecond, Some(\"UTC\"))') END",
            key
        );
    }

    /**
     * A column in micro- or nanoseconds is read as milliseconds before its
     * epoch is taken or binned: its own Int64 would be taken for milliseconds
     * and land in the wrong bucket.
     */
    public void testTheSourceIsReadInMillisecondsWhateverItsUnit() {
        String key = InfinoAggregationProcessor.Grouped.utcBin("(ts)", HOUR, 0);
        assertFalse(key, key.contains("arrow_cast((ts), 'Int64')"));
        assertFalse(key, key.contains("milliseconds', (ts)"));
        assertTrue(key, key.contains("arrow_cast(" + MILLIS + ", 'Int64')"));
    }

    /** An offset moves the origin of both branches, and narrows date_bin's range by its size. */
    public void testAnOffsetShiftsBothBranches() {
        long offset = -6 * HOUR;
        String key = InfinoAggregationProcessor.Grouped.utcBin("(ts)", 24 * HOUR, offset);
        assertTrue(key, key.contains("date_bin(INTERVAL '86400000 milliseconds', " + MILLIS + ", TIMESTAMP '1970-01-01T00:00:00Z' + INTERVAL '-21600000 milliseconds')"));
        assertTrue(key, key.contains("BETWEEN " + (Long.MIN_VALUE / 1_000_000L + 30 * HOUR) + " AND "));
        assertTrue(key, key.contains("(arrow_cast(" + MILLIS + ", 'Int64') - (-21600000))"));
        assertTrue(key, key.contains(" + (-21600000), 'Timestamp"));
    }

    /** An interval too long for any date_bin range is floored in milliseconds alone. */
    public void testAnIntervalPastTheRangeIsFlooredAlone() {
        long ms = Long.MAX_VALUE / 1_000_000L + 1;
        String key = InfinoAggregationProcessor.Grouped.utcBin("(ts)", ms, 0);
        assertFalse(key, key.contains("date_bin"));
        assertTrue(key, key.startsWith("arrow_cast((arrow_cast(" + MILLIS + ", 'Int64') - (0))"));
    }

    /**
     * A zoned key is guarded like a UTC one: date_bin where nanoseconds hold
     * the date, else a floor in milliseconds, and the result is read back in
     * the zone.
     */
    public void testZonedDateBinOnlyWhereNanosecondsHoldTheDate() {
        String key = InfinoAggregationProcessor.Grouped.zonedBin("(ts)", KOLKATA, HOUR, 0);
        long lo = Long.MIN_VALUE / 1_000_000L + HOUR;
        long hi = Long.MAX_VALUE / 1_000_000L - HOUR;
        String epoch = "arrow_cast(" + LOCAL + ", 'Int64')";
        assertEquals(
            "arrow_cast(CASE WHEN " + epoch + " BETWEEN " + lo + " AND " + hi
                + " THEN date_bin(INTERVAL '3600000 milliseconds', " + LOCAL + ")"
                + " ELSE arrow_cast((" + epoch + " - (0)) - (((" + epoch + " - (0)) % 3600000) + 3600000) % 3600000 + (0),"
                + " 'Timestamp(Millisecond, None)') END, " + KOLKATA + ")",
            key
        );
    }

    /**
     * The zoned fallback floors the local wall-clock milliseconds, the ones
     * date_bin bins, and keeps them local until the cast back to the zone;
     * flooring the UTC instant would put the boundary off local time by the
     * zone's offset.
     */
    public void testZonedFallbackFloorsLocalTimeNotTheInstant() {
        String key = InfinoAggregationProcessor.Grouped.zonedBin("(ts)", KOLKATA, 2 * HOUR, 0);
        assertTrue(key, key.contains("ELSE arrow_cast((arrow_cast(" + LOCAL + ", 'Int64') - (0))"));
        assertTrue(key, key.contains("+ (0), 'Timestamp(Millisecond, None)') END"));
        assertFalse(key, key.contains("arrow_cast((ts), 'Int64')"));
        assertFalse(key, key.contains("Some(\"UTC\")"));
        assertTrue(key, key.startsWith("arrow_cast(CASE WHEN "));
        assertTrue(key, key.endsWith(" END, " + KOLKATA + ")"));
    }

    /** An offset moves the origin of both zoned branches, and narrows date_bin's range by its size. */
    public void testZonedOffsetShiftsBothBranches() {
        long offset = 6 * HOUR;
        String key = InfinoAggregationProcessor.Grouped.zonedBin("(ts)", KOLKATA, 24 * HOUR, offset);
        assertTrue(key, key.contains("date_bin(INTERVAL '86400000 milliseconds', " + LOCAL + ", TIMESTAMP '1970-01-01T00:00:00Z' + INTERVAL '21600000 milliseconds')"));
        assertTrue(key, key.contains("BETWEEN " + (Long.MIN_VALUE / 1_000_000L + 30 * HOUR) + " AND " + (Long.MAX_VALUE / 1_000_000L - 30 * HOUR) + " "));
        assertTrue(key, key.contains("(arrow_cast(" + LOCAL + ", 'Int64') - (21600000))"));
        assertTrue(key, key.contains(" + (21600000), 'Timestamp(Millisecond, None)')"));
    }

    /** A zoned interval too long for any date_bin range is floored in local milliseconds alone. */
    public void testZonedIntervalPastTheRangeIsFlooredAlone() {
        long ms = Long.MAX_VALUE / 1_000_000L + 1;
        String key = InfinoAggregationProcessor.Grouped.zonedBin("(ts)", KOLKATA, ms, 0);
        assertFalse(key, key.contains("date_bin"));
        assertFalse(key, key.contains("CASE"));
        assertTrue(key, key.startsWith("arrow_cast(arrow_cast((arrow_cast(" + LOCAL + ", 'Int64') - (0))"));
        assertTrue(key, key.endsWith(", 'Timestamp(Millisecond, None)'), " + KOLKATA + ")"));
    }

    /** dateExpr sends a zoned fixed interval through the guard, reading the source in the zone first. */
    public void testDateExprGuardsAZonedFixedInterval() {
        InfinoAggregationProcessor.Source ts = new InfinoAggregationProcessor.Source("ts", SqlType.DATE, "[ts]", null);
        ZoneId tz = ZoneId.of("Asia/Kolkata");
        String key = InfinoAggregationProcessor.Grouped.dateExpr("h", ts, tz, new DateHistogramInterval("2h"), null, 0).keyExpr();
        String expected = InfinoAggregationProcessor.Grouped.zonedBin("arrow_cast((ts), " + KOLKATA + ")", KOLKATA, 2 * HOUR, 0);
        assertTrue(expected, expected.contains("CASE WHEN"));
        assertEquals(expected, key);
    }
}
