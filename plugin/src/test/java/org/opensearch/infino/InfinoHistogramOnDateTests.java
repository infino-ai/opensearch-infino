/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import org.opensearch.infino.InfinoAggregationProcessor.Grouped;
import org.opensearch.infino.InfinoAggregationProcessor.Leaves;
import org.opensearch.infino.InfinoNative.Column;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.search.aggregations.bucket.composite.CompositeAggregationBuilder;
import org.opensearch.search.aggregations.bucket.composite.CompositeValuesSourceBuilder;
import org.opensearch.search.aggregations.bucket.composite.HistogramValuesSourceBuilder;
import org.opensearch.search.aggregations.bucket.histogram.DoubleBounds;
import org.opensearch.search.aggregations.bucket.histogram.HistogramAggregationBuilder;
import org.opensearch.search.aggregations.bucket.histogram.InternalHistogram;
import org.opensearch.test.OpenSearchTestCase;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
        return Grouped.histogram(h, columns(), "t", List.of("t"), Leaves.NONE, null, 0L);
    }

    private static InternalHistogram built(Grouped g, double... keys) {
        List<Map<String, Object>> rows = new java.util.ArrayList<>();
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

    /** As stock: a bucket is kept when its start, before the offset is added back, is inside the hard bounds. */
    public void testHardBoundsKeepOnlyTheBucketsInsideThem() {
        Grouped g = histogram(
            new HistogramAggregationBuilder("h").field("ts").interval(MINUTE).offset(1000).hardBounds(new DoubleBounds((double) T0, null))
        );
        String start = "floor(((" + TS_MILLIS + ") - 1000.0) / 60000.0) * 60000.0";
        assertEquals("CASE WHEN " + start + " >= 1.6E12 THEN " + start + " + 1000.0 END", g.keyExpr);
        Grouped both = histogram(new HistogramAggregationBuilder("h").field("n").interval(5).hardBounds(new DoubleBounds(2.0, 6.0)));
        String n = "floor(((\"n\") - 0.0) / 5.0) * 5.0";
        assertEquals("CASE WHEN " + n + " >= 2.0 AND " + n + " <= 6.0 THEN " + n + " + 0.0 END", both.keyExpr);
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
