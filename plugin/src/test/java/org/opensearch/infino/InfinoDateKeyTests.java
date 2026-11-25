/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import org.opensearch.infino.InfinoNative.Column;
import org.opensearch.infino.InfinoScriptTranslator.SqlType;
import org.opensearch.common.Rounding;
import org.opensearch.search.aggregations.bucket.composite.CompositeAggregationBuilder;
import org.opensearch.search.aggregations.bucket.composite.CompositeValuesSourceBuilder;
import org.opensearch.search.aggregations.bucket.composite.DateHistogramValuesSourceBuilder;
import org.opensearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.opensearch.search.aggregations.bucket.histogram.DateIntervalWrapper;
import org.opensearch.test.OpenSearchTestCase;

import java.time.Instant;
import java.time.ZoneId;
import java.util.List;
import java.util.Map;

public class InfinoDateKeyTests extends OpenSearchTestCase {

    private static final long HOUR = 3_600_000L;
    /** The source as UTC milliseconds, floored, which both branches read. */
    private static final String MILLIS = "arrow_cast(date_trunc('millisecond', (ts)), 'Timestamp(Millisecond, Some(\"UTC\"))')";
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
     * A zoned key is worked out in epoch milliseconds: the date's local
     * time is floored, and the floor read back as an instant with the
     * offset the zone has at the start the date's own offset gives. No
     * date_bin, so no date is out of its range, and no cast from wall-clock
     * time, which fails for a local time that happens twice.
     */
    public void testZonedKeyIsWorkedOutInEpochMilliseconds() {
        String key = InfinoAggregationProcessor.Grouped.zonedBin("(ts)", KOLKATA, HOUR, 0);
        String epoch = "arrow_cast(" + MILLIS + ", 'Int64')";
        String shifted = "(" + epoch + " - (0))";
        String local = "arrow_cast(to_local_time(arrow_cast(" + shifted + ", " + KOLKATA + ")), 'Int64')";
        String past = "(((" + local + " % 3600000) + 3600000) % 3600000)";
        String start = "(" + shifted + " - " + past + ")";
        String startLocal = "arrow_cast(to_local_time(arrow_cast(" + start + ", " + KOLKATA + ")), 'Int64')";
        assertEquals("arrow_cast(" + start + " + (" + local + " - " + past + ") - " + startLocal + " + (0), " + KOLKATA + ")", key);
        assertFalse(key, key.contains("date_bin"));
        assertFalse(key, key.contains("Timestamp(Millisecond, None)"));
    }

    /** As stock's offset rounding, an offset moves the instant before it is floored, and is added back to the start. */
    public void testZonedOffsetMovesTheInstant() {
        long offset = 6 * HOUR;
        String key = InfinoAggregationProcessor.Grouped.zonedBin("(ts)", KOLKATA, 24 * HOUR, offset);
        assertTrue(key, key.contains("(arrow_cast(" + MILLIS + ", 'Int64') - (21600000))"));
        assertTrue(key, key.contains("% 86400000) + 86400000) % 86400000)"));
        assertTrue(key, key.endsWith(" + (21600000), " + KOLKATA + ")"));
    }

    /** dateExpr sends a zoned fixed interval through zonedBin, reading the source in the zone first. */
    public void testDateExprSendsAZonedFixedIntervalThroughZonedBin() {
        InfinoAggregationProcessor.Source ts = new InfinoAggregationProcessor.Source("ts", SqlType.DATE, "[ts]", null);
        ZoneId tz = ZoneId.of("Asia/Kolkata");
        String key = InfinoAggregationProcessor.Grouped.dateExpr("h", ts, tz, new DateHistogramInterval("2h"), null, 0).keyExpr();
        String expected = InfinoAggregationProcessor.Grouped.zonedBin("arrow_cast((ts), " + KOLKATA + ")", KOLKATA, 2 * HOUR, 0);
        assertEquals(expected, key);
    }

    /**
     * In Europe/London on 2026-10-25 01:00-02:00 local happens twice: an
     * hour's bucket starts at both 00:00Z (BST) and 01:00Z (GMT), as stock's;
     * the day's bucket starts at its BST midnight on both sides of the
     * change. On 2026-03-29 01:00-02:00 local does not happen: the hour after
     * 00:00Z starts at 01:00Z (02:00 BST).
     */
    public void testZonedKeysAcrossLondonsChanges() {
        ZoneId london = ZoneId.of("Europe/London");
        assertEquals(Instant.parse("2026-10-25T00:00:00Z").toEpochMilli(), key(london, "2026-10-25T00:40:00Z", HOUR, 0));
        assertEquals(Instant.parse("2026-10-25T01:00:00Z").toEpochMilli(), key(london, "2026-10-25T01:20:00Z", HOUR, 0));
        assertEquals(Instant.parse("2026-10-25T01:30:00Z").toEpochMilli(), key(london, "2026-10-25T01:50:00Z", HOUR / 2, 0));
        assertEquals(Instant.parse("2026-10-24T23:00:00Z").toEpochMilli(), key(london, "2026-10-25T00:10:00Z", 24 * HOUR, 0));
        assertEquals(Instant.parse("2026-10-24T23:00:00Z").toEpochMilli(), key(london, "2026-10-25T12:00:00Z", 24 * HOUR, 0));
        assertEquals(Instant.parse("2026-03-29T00:00:00Z").toEpochMilli(), key(london, "2026-03-29T00:50:00Z", HOUR, 0));
        assertEquals(Instant.parse("2026-03-29T01:00:00Z").toEpochMilli(), key(london, "2026-03-29T01:10:00Z", HOUR, 0));
        assertEquals(Instant.parse("2026-03-29T00:00:00Z").toEpochMilli(), key(london, "2026-03-29T12:00:00Z", 24 * HOUR, 0));
    }

    /**
     * The key's arithmetic, minute by minute for two days around each
     * change of 2026, gives stock's rounding's key: in zones whose clocks
     * move an hour at 01:00 or 02:00 local, by half an hour (Lord Howe), and
     * by an hour at midnight (Santiago), for intervals from 30m to a day,
     * with and without an offset.
     */
    public void testZonedKeysMatchStockAcrossChanges() {
        String[][] changes = {
            { "Europe/London", "2026-03-29T01:00:00Z", "2026-10-25T01:00:00Z" },
            { "America/New_York", "2026-03-08T07:00:00Z", "2026-11-01T06:00:00Z" },
            { "Australia/Lord_Howe", "2026-04-04T15:00:00Z", "2026-10-03T15:30:00Z" },
            { "America/Santiago", "2026-04-05T03:00:00Z", "2026-09-06T04:00:00Z" } };
        long[] intervals = { HOUR / 2, HOUR, 3 * HOUR, 24 * HOUR };
        long[] offsets = { 0, HOUR / 2, -2 * HOUR };
        for (String[] change : changes) {
            ZoneId tz = ZoneId.of(change[0]);
            for (int c = 1; c < change.length; c++) {
                long at = Instant.parse(change[c]).toEpochMilli();
                for (long ms : intervals) {
                    for (long offset : offsets) {
                        DateIntervalWrapper wrapper = new DateIntervalWrapper();
                        wrapper.fixedInterval(new DateHistogramInterval(ms + "ms"));
                        Rounding.Prepared stock = wrapper.createRounding(tz, offset).prepare(at - 50 * HOUR, at + 50 * HOUR);
                        for (long u = at - 48 * HOUR; u <= at + 48 * HOUR; u += 60_000L) {
                            assertEquals(change[0] + " " + Instant.ofEpochMilli(u) + " every " + ms + " offset " + offset, stock.round(u), key(tz, u, ms, offset));
                        }
                    }
                }
            }
        }
    }

    /**
     * A composite date source's after key is read with the source's format,
     * as stock reads it. A date with no offset of its own is local time in the
     * source's zone: "2017-10-20" in -02:00 is 02:00Z, so the page after it
     * leaves out that day's bucket. One with an offset is that instant. A
     * source with no format, whose key is epoch milliseconds, reads only
     * those, and date math on now is refused.
     */
    public void testCompositeAfterKeyIsReadWithTheSourcesFormat() {
        DateHistogramValuesSourceBuilder iso = new DateHistogramValuesSourceBuilder("d").field("d")
            .calendarInterval(DateHistogramInterval.DAY)
            .timeZone(ZoneId.of("-02:00"))
            .format("iso8601");
        assertEquals("'2017-10-20T02:00:00Z'", afterLiteral(iso, "2017-10-20"));
        assertEquals("'2017-10-21T02:00:00Z'", afterLiteral(iso, "2017-10-21T00:00:00.000-02:00"));
        assertEquals("'2017-10-20T02:00:00Z'", afterLiteral(iso, "2017-10-20T15:00:00||/d"));
        Exception now = expectThrows(InfinoAggregationProcessor.Unsupported.class, () -> afterLiteral(iso, "now-1d"));
        assertTrue(now.getMessage(), now.getMessage().contains("after key [now-1d] for [d] that its format does not read"));
        DateHistogramValuesSourceBuilder raw = new DateHistogramValuesSourceBuilder("d").field("d")
            .calendarInterval(DateHistogramInterval.HOUR)
            .timeZone(ZoneId.of("America/New_York"));
        assertEquals("'2017-11-05T06:00:00Z'", afterLiteral(raw, 1509861600000L));
        assertEquals("'2017-11-05T06:00:00Z'", afterLiteral(raw, "1509861600000"));
        Exception date = expectThrows(InfinoAggregationProcessor.Unsupported.class, () -> afterLiteral(raw, "2017-11-04T23:00"));
        assertTrue(date.getMessage(), date.getMessage().contains("after key [2017-11-04T23:00] for [d] that its format does not read"));
    }

    /** The after literal a composite over a date column [d] makes of {@code after} for its one source. */
    private static String afterLiteral(CompositeValuesSourceBuilder<?> source, Object after) {
        CompositeAggregationBuilder c = new CompositeAggregationBuilder("c", List.of(source)).aggregateAfter(Map.of("d", after));
        Map<String, Column> cols = Map.of("d", new Column("d", "ts", false, false));
        long now = Instant.parse("2026-01-01T00:00:00Z").toEpochMilli();
        InfinoAggregationProcessor.Composite composite = InfinoAggregationProcessor.Composite.of(c, cols, "t", InfinoAggregationProcessor.Leaves.NONE, null, now);
        return (String) composite.after[0];
    }

    private static long key(ZoneId tz, String at, long ms, long offset) {
        return key(tz, Instant.parse(at).toEpochMilli(), ms, offset);
    }

    /** {@link InfinoAggregationProcessor.Grouped#zonedBin}'s arithmetic, step by step, in Java. */
    private static long key(ZoneId tz, long epoch, long ms, long offset) {
        long shifted = epoch - offset;
        long local = localMillis(tz, shifted);
        long past = ((local % ms) + ms) % ms;
        long start = shifted - past;
        return start + (local - past) - localMillis(tz, start) + offset;
    }

    private static long localMillis(ZoneId tz, long epoch) {
        return epoch + tz.getRules().getOffset(Instant.ofEpochMilli(epoch)).getTotalSeconds() * 1000L;
    }
}
