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
    /** The source read as UTC, which a zoned key does before it reads it in the zone. */
    private static final String AS_UTC = "arrow_cast((ts), 'Timestamp(Millisecond, Some(\"UTC\"))')";

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

    /**
     * dateExpr sends a zoned fixed interval through zonedBin, reading the
     * source in the zone first: alone, in a zone that still changes its
     * clocks and with no word of where the dates fall.
     */
    public void testDateExprSendsAZonedFixedIntervalThroughZonedBin() {
        InfinoAggregationProcessor.Source ts = new InfinoAggregationProcessor.Source("ts", SqlType.DATE, "[ts]", null);
        String london = "'Timestamp(Millisecond, Some(\"Europe/London\"))'";
        String key = InfinoAggregationProcessor.Grouped.dateExpr("h", ts, ZoneId.of("Europe/London"), new DateHistogramInterval("2h"), null, 0).keyExpr();
        assertEquals(InfinoAggregationProcessor.Grouped.zonedBin("arrow_cast(" + AS_UTC + ", " + london + ")", london, 2 * HOUR, 0), key);
    }

    /**
     * A date is read as UTC before it is read in the request's zone. A
     * table written elsewhere may keep its dates with no zone, and a cast
     * straight to the zone takes such a date for local time there, which
     * moves every bucket by the zone's offset.
     */
    public void testAZonedKeyReadsTheDateAsUtcFirst() {
        InfinoAggregationProcessor.Source ts = new InfinoAggregationProcessor.Source("ts", SqlType.DATE, "[ts]", null);
        String london = "'Timestamp(Millisecond, Some(\"Europe/London\"))'";
        for (DateHistogramInterval unit : List.of(DateHistogramInterval.HOUR, DateHistogramInterval.MONTH)) {
            String key = InfinoAggregationProcessor.Grouped.dateExpr("h", ts, ZoneId.of("Europe/London"), null, unit, 0).keyExpr();
            assertTrue(key, key.contains("arrow_cast(" + AS_UTC + ", " + london + ")"));
            assertFalse(key, key.contains("arrow_cast((ts), " + london + ")"));
        }
    }

    /**
     * A zone that no longer changes its clocks keeps one offset from its
     * last change on. Dates from then on, less an interval and a day, are
     * floored in UTC with the origin moved by the offset, and read in no
     * zone; only earlier dates go through the zone. Both give a key of the
     * zone's type.
     */
    public void testAZoneThatNoLongerChangesFloorsInUtc() {
        InfinoAggregationProcessor.Source ts = new InfinoAggregationProcessor.Source("ts", SqlType.DATE, "[ts]", null);
        ZoneId tz = ZoneId.of("Asia/Kolkata");
        long lastChange = Instant.parse("1945-10-14T17:30:00Z").toEpochMilli();
        long plusFiveThirty = 19_800_000L;
        assertEquals(List.of(new InfinoAggregationProcessor.Grouped.Steady(lastChange, null, plusFiveThirty)), InfinoAggregationProcessor.Grouped.steady(tz, null, null));

        String epoch = "arrow_cast(" + MILLIS + ", 'Int64')";
        String floor = "arrow_cast(" + InfinoAggregationProcessor.Grouped.utcBin("(ts)", 2 * HOUR, -plusFiveThirty) + ", " + KOLKATA + ")";
        String zoned = InfinoAggregationProcessor.Grouped.zonedBin("arrow_cast(" + AS_UTC + ", " + KOLKATA + ")", KOLKATA, 2 * HOUR, 0);
        String fixed = InfinoAggregationProcessor.Grouped.dateExpr("h", ts, tz, new DateHistogramInterval("2h"), null, 0).keyExpr();
        assertEquals("CASE WHEN " + epoch + " >= " + (lastChange + 2 * HOUR + 24 * HOUR) + " THEN " + floor + " ELSE " + zoned + " END", fixed);

        String hour = InfinoAggregationProcessor.Grouped.dateExpr("h", ts, tz, null, DateHistogramInterval.HOUR, 0).keyExpr();
        String hourFloor = "arrow_cast(" + InfinoAggregationProcessor.Grouped.utcBin("(ts)", HOUR, -plusFiveThirty) + ", " + KOLKATA + ")";
        assertEquals(
            "CASE WHEN " + epoch + " >= " + (lastChange + HOUR + 24 * HOUR) + " THEN " + hourFloor + " ELSE date_trunc('hour', arrow_cast(" + AS_UTC + ", " + KOLKATA + ")) END",
            hour
        );
    }

    /** A zone that is an offset and nothing else has no dates to send through a zone: the key is the floor alone. */
    public void testAZoneOfOneOffsetIsTheFloorAlone() {
        InfinoAggregationProcessor.Source ts = new InfinoAggregationProcessor.Source("ts", SqlType.DATE, "[ts]", null);
        String zone = "'Timestamp(Millisecond, Some(\"-03:00\"))'";
        String key = InfinoAggregationProcessor.Grouped.dateExpr("h", ts, ZoneId.of("-03:00"), null, DateHistogramInterval.DAY, 0).keyExpr();
        assertEquals("arrow_cast(" + InfinoAggregationProcessor.Grouped.utcBin("(ts)", 24 * HOUR, 3 * HOUR) + ", " + zone + ")", key);
        assertFalse(key, key.contains("to_local_time"));
        assertFalse(key, key.contains("date_trunc('day'"));
    }

    /**
     * In a zone that still changes its clocks, the span the request gives
     * says which stretches its dates fall in: a week in one stretch floors
     * between that stretch's ends, and a span across a change floors on each
     * side of it. A week, month or year has no one length, and is always
     * read in the zone.
     */
    public void testASpanChoosesTheStretchesOfAZoneThatChanges() {
        InfinoAggregationProcessor.Source ts = new InfinoAggregationProcessor.Source("ts", SqlType.DATE, "[ts]", null);
        ZoneId tz = ZoneId.of("America/Los_Angeles");
        long from = Instant.parse("2025-12-01T08:00:00Z").toEpochMilli();
        long to = Instant.parse("2025-12-08T08:00:00Z").toEpochMilli();
        long pst = Instant.parse("2025-11-02T09:00:00Z").toEpochMilli();
        long pdt = Instant.parse("2026-03-08T10:00:00Z").toEpochMilli();
        assertEquals(List.of(new InfinoAggregationProcessor.Grouped.Steady(pst, pdt, -8 * HOUR)), InfinoAggregationProcessor.Grouped.steady(tz, from, to));
        String week = InfinoAggregationProcessor.Grouped.dateExpr("h", ts, tz, null, DateHistogramInterval.HOUR, 0, from, to).keyExpr();
        assertTrue(week, week.startsWith("CASE WHEN arrow_cast(" + MILLIS + ", 'Int64') BETWEEN " + (pst + 25 * HOUR) + " AND " + (pdt - 1) + " THEN arrow_cast("));
        assertTrue(week, week.endsWith(" ELSE date_trunc('hour', arrow_cast(" + AS_UTC + ", 'Timestamp(Millisecond, Some(\"America/Los_Angeles\"))')) END"));

        List<InfinoAggregationProcessor.Grouped.Steady> across = InfinoAggregationProcessor.Grouped.steady(tz, pst - 24 * HOUR, pst + 24 * HOUR);
        assertEquals(2, across.size());
        assertEquals(-7 * HOUR, across.get(0).offsetMillis());
        assertEquals(Long.valueOf(pst), across.get(0).until());
        assertEquals(new InfinoAggregationProcessor.Grouped.Steady(pst, pdt, -8 * HOUR), across.get(1));
        assertEquals(4, InfinoAggregationProcessor.Grouped.steady(tz, 0L, to).size());
        assertEquals(List.of(), InfinoAggregationProcessor.Grouped.steady(tz, null, null));

        String month = InfinoAggregationProcessor.Grouped.dateExpr("h", ts, tz, null, DateHistogramInterval.MONTH, 0, from, to).keyExpr();
        assertEquals("date_trunc('month', arrow_cast(" + AS_UTC + ", 'Timestamp(Millisecond, Some(\"America/Los_Angeles\"))'))", month);
    }

    /**
     * The floor gives stock's key for every date it takes: minute by minute
     * for four days around a change, with the request's span set across it,
     * in zones whose clocks move an hour in the small hours, by half an hour
     * (Lord Howe), at midnight (Santiago, Havana), and in one whose last
     * change is long past (Kolkata), for fixed intervals and for a calendar
     * minute, hour and day, with and without an offset. The dates nearest
     * the change are not floored, and most of the rest are.
     */
    public void testFlooredKeysMatchStock() {
        String[][] changes = {
            { "Europe/London", "2026-03-29T01:00:00Z", "2026-10-25T01:00:00Z" },
            { "America/Los_Angeles", "2026-03-08T10:00:00Z", "2026-11-01T09:00:00Z" },
            { "Australia/Lord_Howe", "2026-04-04T15:00:00Z", "2026-10-03T15:30:00Z" },
            { "America/Santiago", "2026-04-05T03:00:00Z", "2026-09-06T04:00:00Z" },
            { "America/Havana", "2026-03-08T05:00:00Z", "2026-11-01T05:00:00Z" },
            { "Asia/Kolkata", "1945-10-14T17:30:00Z" } };
        long[] intervals = { HOUR / 2, HOUR, 3 * HOUR, 24 * HOUR };
        DateHistogramInterval[] units = { DateHistogramInterval.MINUTE, DateHistogramInterval.HOUR, DateHistogramInterval.DAY };
        long[] unitMillis = { 60_000L, HOUR, 24 * HOUR };
        long[] offsets = { 0, HOUR / 2, -2 * HOUR };
        for (String[] change : changes) {
            ZoneId tz = ZoneId.of(change[0]);
            for (int c = 1; c < change.length; c++) {
                long at = Instant.parse(change[c]).toEpochMilli();
                long from = at - 96 * HOUR;
                long to = at + 96 * HOUR;
                for (long offset : offsets) {
                    for (int i = 0; i < intervals.length + units.length; i++) {
                        boolean fixed = i < intervals.length;
                        long ms = fixed ? intervals[i] : unitMillis[i - intervals.length];
                        DateIntervalWrapper wrapper = new DateIntervalWrapper();
                        if (fixed) {
                            wrapper.fixedInterval(new DateHistogramInterval(ms + "ms"));
                        } else {
                            wrapper.calendarInterval(units[i - intervals.length]);
                        }
                        Rounding.Prepared stock = wrapper.createRounding(tz, offset).prepare(from - 50 * HOUR, to + 50 * HOUR);
                        int floored = 0;
                        int all = 0;
                        for (long u = from; u <= to; u += 60_000L) {
                            Long key = flooredKey(tz, u, ms, offset, from, to);
                            all++;
                            if (key == null) {
                                continue;
                            }
                            floored++;
                            String what = change[0] + " " + Instant.ofEpochMilli(u) + (fixed ? " every " + ms : " calendar " + ms) + " offset " + offset;
                            assertEquals(what, stock.round(u), key.longValue());
                            assertTrue(what + " is floored within a day after the change", u < at || u >= at + 24 * HOUR);
                        }
                        assertTrue(change[0] + " floors most dates: " + floored + " of " + all, floored > all / 2);
                    }
                }
            }
        }
    }

    /**
     * {@link InfinoAggregationProcessor.Grouped#steadyBin}'s choice and
     * arithmetic in Java: the floor of {@code epoch} in the first stretch
     * that takes it, or null for a date that goes through the zone.
     */
    private static Long flooredKey(ZoneId tz, long epoch, long ms, long offset, long from, long to) {
        for (InfinoAggregationProcessor.Grouped.Steady s : InfinoAggregationProcessor.Grouped.steady(tz, from, to)) {
            Long lo = s.first(ms, offset);
            Long hi = s.last(offset);
            if ((lo == null || epoch >= lo) && (hi == null || epoch <= hi)) {
                long moved = offset - s.offsetMillis();
                long shifted = epoch - moved;
                return shifted - Math.floorMod(shifted, ms) + moved;
            }
        }
        return null;
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
