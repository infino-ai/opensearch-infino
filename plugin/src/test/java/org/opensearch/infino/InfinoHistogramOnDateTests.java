/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import com.sun.jna.Memory;
import com.sun.jna.Pointer;

import org.opensearch.infino.InfinoAggregationProcessor.Grouped;
import org.opensearch.infino.InfinoAggregationProcessor.Leaves;
import org.opensearch.infino.InfinoNative.Column;
import org.opensearch.script.Script;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.search.aggregations.MultiBucketConsumerService;
import org.opensearch.search.aggregations.bucket.composite.CompositeAggregationBuilder;
import org.opensearch.search.aggregations.bucket.composite.CompositeValuesSourceBuilder;
import org.opensearch.search.aggregations.bucket.composite.HistogramValuesSourceBuilder;
import org.opensearch.search.aggregations.bucket.histogram.DoubleBounds;
import org.opensearch.search.aggregations.bucket.histogram.HistogramAggregationBuilder;
import org.opensearch.search.aggregations.bucket.histogram.InternalHistogram;
import org.opensearch.test.OpenSearchTestCase;

import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A histogram on a date field buckets the date's epoch milliseconds, as
 * stock's does, and its keys print as dates in the field's format.
 */
public class InfinoHistogramOnDateTests extends OpenSearchTestCase {

    /** A date of the "ts" column as an Int64 of UTC epoch milliseconds. */
    private static final String TS_MILLIS = "arrow_cast(arrow_cast(date_trunc('millisecond', \"ts\"), 'Timestamp(Millisecond, Some(\"UTC\"))'), 'Int64')";
    private static final double MINUTE = 60_000;
    private static final long T0 = 1_600_000_000_000L;

    private static Map<String, Column> columns() {
        Map<String, Column> cols = new HashMap<>();
        cols.put("ts", new Column("ts", "ts", false, false));
        cols.put("fmt", new Column("fmt", "ts", false, false, null, "yyyy/MM/dd HH:mm:ss||epoch_millis", null));
        cols.put("n", new Column("n", "i64", false, false));
        cols.put("k", new Column("k", "str", false, false));
        return cols;
    }

    private static Grouped histogram(HistogramAggregationBuilder h) {
        return Grouped.histogram(h, columns(), "t", Leaves.NONE, null, 0L);
    }

    private static InternalHistogram built(Grouped g, double... keys) {
        List<Map<String, Object>> rows = new ArrayList<>();
        for (double key : keys) {
            rows.add(Map.of("k0", key, "doc_count", 1L));
        }
        return (InternalHistogram) g.build(rows, "k0", row -> InternalAggregations.EMPTY, keys.length);
    }

    public void testADateIsBucketedByItsEpochMilliseconds() {
        Grouped g = histogram(new HistogramAggregationBuilder("h").field("ts").interval(MINUTE));
        assertEquals("floor(((" + TS_MILLIS + ") - 0.0) / 60000.0) * 60000.0 + 0.0", g.keyExpr);
    }

    public void testAnOffsetMovesTheBucketsOfADate() {
        Grouped g = histogram(new HistogramAggregationBuilder("h").field("ts").interval(MINUTE).offset(30_000));
        assertEquals("floor(((" + TS_MILLIS + ") - 30000.0) / 60000.0) * 60000.0 + 30000.0", g.keyExpr);
    }

    public void testANumberIsBucketedAsItIs() {
        Grouped g = histogram(new HistogramAggregationBuilder("h").field("n").interval(5));
        assertEquals("floor(((\"n\") - 0.0) / 5.0) * 5.0 + 0.0", g.keyExpr);
    }

    public void testAKeywordIsStillRefused() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> histogram(new HistogramAggregationBuilder("h").field("k").interval(MINUTE))
        );
        assertTrue(e.getMessage(), e.getMessage().endsWith("aggregation [h] needs a numeric value, [k] is string"));
    }

    /** A date's key prints in the field's default format, in UTC. */
    public void testADatesKeyPrintsAsADate() {
        InternalHistogram.Bucket b = built(histogram(new HistogramAggregationBuilder("h").field("ts").interval(MINUTE)), T0).getBuckets().get(0);
        assertEquals(T0, ((Double) b.getKey()).longValue());
        assertEquals("2020-09-13T12:26:40.000Z", b.getKeyAsString());
    }

    /** The field's own format prints the key, unless the request names another. */
    public void testTheFieldsFormatOrTheRequestsPrintsTheKey() {
        Grouped field = histogram(new HistogramAggregationBuilder("h").field("fmt").interval(MINUTE));
        assertEquals("2020/09/13 12:26:40", built(field, T0).getBuckets().get(0).getKeyAsString());
        Grouped asked = histogram(new HistogramAggregationBuilder("h").field("fmt").interval(MINUTE).format("epoch_second"));
        assertEquals("1600000000", built(asked, T0).getBuckets().get(0).getKeyAsString());
    }

    /** A number's key prints only in a format the request gives, as stock's. */
    public void testANumbersKeyPrintsInTheRequestsFormatOnly() {
        Grouped raw = histogram(new HistogramAggregationBuilder("h").field("n").interval(5));
        assertSame(DocValueFormat.RAW, built(raw, 10).getBuckets().get(0).getFormatter());
        Grouped formatted = histogram(new HistogramAggregationBuilder("h").field("n").interval(5).format("Value is ##0.0"));
        assertEquals("Value is 10.0", built(formatted, 10).getBuckets().get(0).getKeyAsString());
    }

    /**
     * As stock: a bucket is kept when its start, before the offset is added
     * back, is inside the hard bounds. The bounds are the level's WHERE, not
     * part of its key, so the rows outside them are not grouped at all; a
     * plain column also gets a range of its own values, a bucket wider on
     * each side, that the engine can skip files by.
     */
    public void testHardBoundsKeepOnlyTheRowsInsideThemInTheWhere() {
        Grouped g = histogram(
            new HistogramAggregationBuilder("h").field("ts").interval(MINUTE).offset(1000).hardBounds(new DoubleBounds((double) T0, null))
        );
        String start = "floor(((" + TS_MILLIS + ") - 1000.0) / 60000.0) * 60000.0";
        assertEquals(start + " + 1000.0", g.keyExpr);
        assertEquals("(\"ts\" >= '2020-09-13T12:25:41Z' AND " + start + " >= 1.6E12)", g.within);
        Grouped both = histogram(new HistogramAggregationBuilder("h").field("n").interval(5).hardBounds(new DoubleBounds(2.0, 6.0)));
        String n = "floor(((\"n\") - 0.0) / 5.0) * 5.0";
        assertEquals(n + " + 0.0", both.keyExpr);
        assertEquals("(\"n\" >= -3 AND " + n + " >= 2.0 AND \"n\" < 11 AND " + n + " <= 6.0)", both.within);
        assertNull("no bounds, every row", histogram(new HistogramAggregationBuilder("h").field("n").interval(5)).within);
    }

    /**
     * Stock reads a script with no field as numbers (a date as its epoch
     * milliseconds), so its key prints as a number unless the request gives
     * a format, even when the script returns a date.
     */
    public void testAScriptWithNoFieldKeysByANumberEvenForADate() {
        Grouped g = histogram(new HistogramAggregationBuilder("h").script(new Script("doc['ts'].value")).interval(MINUTE));
        assertTrue(g.keyExpr, g.keyExpr.contains("'Int64')"));
        InternalHistogram.Bucket b = built(g, T0).getBuckets().get(0);
        assertSame(DocValueFormat.RAW, b.getFormatter());
        Grouped formatted = histogram(new HistogramAggregationBuilder("h").script(new Script("doc['ts'].value")).interval(MINUTE).format("0"));
        assertEquals("1600000000000", built(formatted, T0).getBuckets().get(0).getKeyAsString());
    }

    /** A value script over a date field keeps the field's format, as stock's. */
    public void testAValueScriptOnADateFieldKeepsTheFieldsFormat() {
        Grouped g = histogram(new HistogramAggregationBuilder("h").field("fmt").script(new Script("_value")).interval(MINUTE));
        assertEquals("2020/09/13 12:26:40", built(g, T0).getBuckets().get(0).getKeyAsString());
    }

    /**
     * A histogram reads its groups only up to one past search.max_buckets;
     * past it, it fails with stock's own error, which stock's reduce raises
     * for all of the level's buckets at once: they are counted in the engine,
     * with no rows read. A group missing its key is no bucket and is left out.
     */
    public void testAHistogramPastMaxBucketsFailsWithStocksErrorWithoutReadingMore() {
        List<String> statements = new ArrayList<>();
        AtomicInteger groups = new AtomicInteger();
        InfinoNative.useLib((InfinoLib) Proxy.newProxyInstance(InfinoLib.class.getClassLoader(), new Class<?>[] { InfinoLib.class }, (proxy, method, args) -> switch (method.getName()) {
            case "infino_open_with" -> new Memory(1);
            case "infino_sql" -> {
                String statement = (String) args[1];
                statements.add(statement);
                if (statement.startsWith("SELECT COUNT(*)")) {
                    yield string("[{\"n\":" + groups.get() + "}]");
                }
                // The engine applies the LIMIT: at most that many groups come back.
                int limit = Integer.parseInt(statement.substring(statement.lastIndexOf(' ') + 1));
                StringBuilder rows = new StringBuilder("[");
                for (int i = 0; i < Math.min(limit, groups.get()); i++) {
                    rows.append(i == 0 ? "" : ",").append("{\"k0\":").append(i).append(",\"doc_count\":1}");
                }
                yield string(rows.append("]").toString());
            }
            default -> null;
        }));
        try {
            groups.set(3);
            assertEquals(3, InfinoAggregationProcessor.boundedByMaxBuckets("SELECT g", List.of("k0"), 3).size());
            assertEquals(List.of("SELECT * FROM (SELECT g) t WHERE k0 IS NOT NULL ORDER BY k0 LIMIT 4"), statements);
            groups.set(50_000_000);
            statements.clear();
            MultiBucketConsumerService.TooManyBucketsException e = expectThrows(
                MultiBucketConsumerService.TooManyBucketsException.class,
                () -> InfinoAggregationProcessor.boundedByMaxBuckets("SELECT g", List.of("k0"), 3)
            );
            assertEquals(
                "Trying to create too many buckets. Must be less than or equal to: [3] but was [50000000]. "
                    + "This limit can be set by changing the [search.max_buckets] cluster level setting.",
                e.getMessage()
            );
            assertEquals(3, e.getMaxBuckets());
            assertEquals(List.of("SELECT * FROM (SELECT g) t WHERE k0 IS NOT NULL ORDER BY k0 LIMIT 4", "SELECT COUNT(*) AS n FROM (SELECT g) t WHERE k0 IS NOT NULL"), statements);
        } finally {
            InfinoNative.useLib(null);
        }
    }

    private static Pointer string(String s) {
        byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
        Memory m = new Memory(bytes.length + 1);
        m.write(0, bytes, 0, bytes.length);
        m.setByte(bytes.length, (byte) 0);
        return m;
    }

    /** As stock, extended bounds reaching outside the hard bounds are refused. */
    public void testExtendedBoundsOutsideHardBoundsAreRefused() {
        HistogramAggregationBuilder h = new HistogramAggregationBuilder("h").field("n")
            .interval(5)
            .hardBounds(new DoubleBounds(0.0, 10.0))
            .extendedBounds(new DoubleBounds(-5.0, 10.0));
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> histogram(h));
        assertEquals("Extended bounds have to be inside hard bounds, hard bounds: [0.0--10.0], extended bounds: [-5.0--10.0]", e.getMessage());
        // Inside them is served.
        histogram(h.extendedBounds(new DoubleBounds(0.0, 5.0)));
    }

    /** Stock keys a composite histogram source on a date by the date's text, which is not built here: still refused. */
    public void testACompositeHistogramSourceOnADateIsStillRefused() {
        List<CompositeValuesSourceBuilder<?>> sources = List.of(new HistogramValuesSourceBuilder("h").field("ts").interval(MINUTE));
        CompositeAggregationBuilder c = new CompositeAggregationBuilder("c", sources);
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> InfinoAggregationProcessor.Composite.of(c, columns(), "t", Leaves.NONE, null, 0L)
        );
        assertTrue(e.getMessage(), e.getMessage().contains("needs a numeric value, [ts] is date"));
    }
}
