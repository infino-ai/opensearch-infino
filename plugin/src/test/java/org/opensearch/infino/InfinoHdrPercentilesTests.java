/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import org.opensearch.core.index.Index;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.AggregationBuilders;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.bucket.terms.Terms;
import org.opensearch.search.aggregations.metrics.InternalHDRPercentileRanks;
import org.opensearch.search.aggregations.metrics.InternalHDRPercentiles;
import org.opensearch.search.aggregations.metrics.PercentilesConfig;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.HdrHistogram.DoubleHistogram;

/**
 * Percentiles and percentile ranks by the HDR method, read through the
 * engine, against the histogram stock builds: each value recorded into a
 * histogram per shard, in the shard's order, and the shards' histograms
 * merged as stock's reduce merges them. The values must match exactly.
 */
public class InfinoHdrPercentilesTests extends OpenSearchTestCase {

    private static final String TABLE = "hdr_percentiles";
    /** The percents every comparison asks for, beside a few random ones. */
    private static final double[] PERCENTS = { 0, 0.1, 1, 5, 25, 33.3, 50, 75, 90, 95, 99, 99.9, 100 };
    /** Shards stock would spread the values over, at most. */
    private static final int MAX_SHARDS = 5;
    /** Decades a field's values span at most: a five-digit histogram covers about 13. */
    private static final int MAX_DECADES = 12;
    /** The least decade a double field's values start at. */
    private static final int MIN_DOUBLE_DECADE = -3;
    /** Powers of two the least value of the range-limit test is drawn from, either side of one. */
    private static final int EDGE_EXPONENTS = 20;
    /** Significant decimal digits the range-limit test writes its values with: few enough to read back exactly. */
    private static final int EDGE_DIGITS = 15;
    /** Powers of two past the least value that no histogram covers: where the search for the greatest value starts. */
    private static final int BEYOND_ANY_RANGE = 80;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        resetTable();
    }

    private static void resetTable() {
        InfinoNative.dropTable(TABLE);
        InfinoNative.ensureTable(TABLE, "{\"properties\":{\"g\":{\"type\":\"keyword\"},\"l\":{\"type\":\"long\"},\"d\":{\"type\":\"double\"}}}");
    }

    @Override
    public void tearDown() throws Exception {
        InfinoNative.dropTable(TABLE);
        super.tearDown();
    }

    /** One document per entry: its group, and its long and double (null for none). */
    private record Doc(String g, Long l, Double d) {}

    private static void index(List<Doc> docs) {
        String[] ids = new String[docs.size()];
        String[] sources = new String[docs.size()];
        for (int i = 0; i < docs.size(); i++) {
            Doc doc = docs.get(i);
            ids[i] = Integer.toString(i);
            StringBuilder source = new StringBuilder("{\"g\":\"" + doc.g() + "\"");
            if (doc.l() != null) {
                source.append(",\"l\":").append(doc.l());
            }
            if (doc.d() != null) {
                source.append(",\"d\":").append(String.format(Locale.ROOT, "%s", doc.d()));
            }
            sources[i] = source.append('}').toString();
        }
        assertEquals(docs.size(), InfinoNative.indexBatch(TABLE, ids, sources, ids.length).ok());
    }

    private static InternalAggregation run(AggregationBuilder agg) {
        SearchSourceBuilder source = new SearchSourceBuilder().size(0).aggregation(agg);
        InfinoSearch.Plan plan = InfinoSearch.plan(TABLE, source, System.currentTimeMillis());
        return new InfinoAggregationProcessor(new Index(TABLE, "_na_")).evaluate(List.of(agg), plan, null).aggregations().get(agg.getName());
    }

    /**
     * Stock's histogram over {@code values}: spread over a random number of
     * shards in a random order, each shard recording its values one by one
     * into a histogram of its own, then merged as the reduce merges them.
     */
    private DoubleHistogram stock(List<Double> values, int digits) {
        List<Double> shuffled = new ArrayList<>(values);
        Collections.shuffle(shuffled, random());
        int shards = randomIntBetween(1, MAX_SHARDS);
        List<DoubleHistogram> parts = new ArrayList<>();
        for (int s = 0; s < shards; s++) {
            DoubleHistogram h = new DoubleHistogram(digits);
            h.setAutoResize(true);
            parts.add(h);
        }
        for (int i = 0; i < shuffled.size(); i++) {
            parts.get(i % shards).recordValue(shuffled.get(i));
        }
        DoubleHistogram merged = new DoubleHistogram(parts.get(0));
        merged.setAutoResize(true);
        for (DoubleHistogram h : parts) {
            merged.add(h);
        }
        return merged;
    }

    private double[] percents() {
        double[] out = new double[PERCENTS.length + 3];
        System.arraycopy(PERCENTS, 0, out, 0, PERCENTS.length);
        for (int i = PERCENTS.length; i < out.length; i++) {
            out[i] = randomDoubleBetween(0, 100, true);
        }
        return out;
    }

    /** Values to rank: some of the values themselves, a little either side of them, and the ends. */
    private double[] ranked(List<Double> values) {
        List<Double> out = new ArrayList<>();
        for (int i = 0; i < 6; i++) {
            double v = randomFrom(values);
            out.add(v);
            out.add(v * 1.0001);
            out.add(v * 0.9999);
        }
        out.add(Collections.min(values));
        out.add(Collections.max(values));
        out.add(0.0);
        return out.stream().mapToDouble(Double::doubleValue).toArray();
    }

    private void assertSameAsStock(String field, List<Double> values, int digits) {
        DoubleHistogram stock = stock(values, digits);
        double[] percents = percents();
        InternalHDRPercentiles p = (InternalHDRPercentiles) run(
            AggregationBuilders.percentiles("p").field(field).percentiles(percents).percentilesConfig(new PercentilesConfig.Hdr(digits))
        );
        assertEquals(stock.getTotalCount(), p.getState().getTotalCount());
        for (double percent : percents) {
            assertEquals(field + " digits " + digits + " p" + percent, stock.getValueAtPercentile(percent), p.percentile(percent), 0.0);
        }
        double[] ranked = ranked(values);
        InternalHDRPercentileRanks r = (InternalHDRPercentileRanks) run(
            AggregationBuilders.percentileRanks("r", ranked).field(field).percentilesConfig(new PercentilesConfig.Hdr(digits))
        );
        for (double v : ranked) {
            assertEquals(
                field + " digits " + digits + " rank of " + v,
                InternalHDRPercentileRanks.percentileRank(stock, v),
                r.percent(v),
                0.0
            );
        }
    }

    /**
     * The values of a field over many magnitudes, repeats and zeros among
     * them, spanning at most {@link #MAX_DECADES} decades: past about 2^46
     * between the least and the greatest, a five-digit histogram cannot
     * cover them and stock refuses the aggregation.
     */
    private List<Double> spread(boolean whole) {
        int n = randomIntBetween(500, 3000);
        int maxExponent = randomIntBetween(2, whole ? MAX_DECADES : MAX_DECADES + MIN_DOUBLE_DECADE);
        List<Double> out = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            double v;
            if (rarely()) {
                v = 0;
            } else if (!out.isEmpty() && rarely()) {
                v = randomFrom(out);
            } else {
                v = Math.pow(10, randomDoubleBetween(whole ? 0 : MIN_DOUBLE_DECADE, maxExponent, true));
            }
            out.add(whole ? Math.floor(v) : v);
        }
        return out;
    }

    /** Three longs, the 95th percentile at three digits: the value stock gives. */
    public void testThreeLongsAsStockReadsThem() {
        index(List.of(new Doc("a", 10L, null), new Doc("a", 20L, null), new Doc("a", 30L, null)));
        InternalHDRPercentiles p = (InternalHDRPercentiles) run(
            AggregationBuilders.percentiles("p").field("l").percentiles(95).percentilesConfig(new PercentilesConfig.Hdr(3))
        );
        assertEquals(30.0078125, p.percentile(95), 0.0);
    }

    /** Random longs and doubles over every magnitude, at each significant digit count: stock's values exactly. */
    public void testRandomValuesMatchStock() {
        List<Double> longs = spread(true);
        List<Double> doubles = spread(false);
        int n = Math.max(longs.size(), doubles.size());
        List<Doc> docs = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            Long l = i < longs.size() ? (long) (double) longs.get(i) : null;
            Double d = i < doubles.size() ? doubles.get(i) : null;
            docs.add(new Doc("a", l, d));
        }
        index(docs);
        for (int digits = 0; digits <= 5; digits++) {
            assertSameAsStock("l", longs, digits);
            assertSameAsStock("d", doubles, digits);
        }
    }

    /** Under a terms bucket each bucket's histogram is its own values', as stock's per bucket. */
    public void testPerTermsBucket() {
        List<Doc> docs = new ArrayList<>();
        Map<String, List<Double>> byGroup = new HashMap<>();
        for (int i = 0; i < 2000; i++) {
            String g = "g" + randomIntBetween(0, 4);
            double v = Math.pow(10, randomDoubleBetween(-2, 8, true));
            docs.add(new Doc(g, null, v));
            byGroup.computeIfAbsent(g, k -> new ArrayList<>()).add(v);
        }
        index(docs);
        int digits = randomIntBetween(0, 5);
        double[] percents = percents();
        Terms terms = (Terms) run(
            AggregationBuilders.terms("t")
                .field("g")
                .subAggregation(
                    AggregationBuilders.percentiles("p").field("d").percentiles(percents).percentilesConfig(new PercentilesConfig.Hdr(digits))
                )
        );
        assertEquals(byGroup.size(), terms.getBuckets().size());
        for (Terms.Bucket b : terms.getBuckets()) {
            DoubleHistogram stock = stock(byGroup.get(b.getKeyAsString()), digits);
            InternalHDRPercentiles p = b.getAggregations().get("p");
            for (double percent : percents) {
                assertEquals(b.getKeyAsString() + " p" + percent, stock.getValueAtPercentile(percent), p.percentile(percent), 0.0);
            }
        }
    }

    /** A field no document gives: no value, as stock's empty histogram, and missing stands in for it. */
    public void testNoValuesAndMissing() {
        index(List.of(new Doc("a", 5L, null), new Doc("a", 7L, null)));
        InternalHDRPercentiles none = (InternalHDRPercentiles) run(
            AggregationBuilders.percentiles("p").field("d").percentiles(50).percentilesConfig(new PercentilesConfig.Hdr(3))
        );
        assertEquals(0, none.getState().getTotalCount());
        assertTrue(Double.isNaN(none.percentile(50)));
        InternalHDRPercentiles missing = (InternalHDRPercentiles) run(
            AggregationBuilders.percentiles("p").field("d").missing(1.5).percentiles(50).percentilesConfig(new PercentilesConfig.Hdr(3))
        );
        assertEquals(2, missing.getState().getTotalCount());
        assertEquals(stock(List.of(1.5, 1.5), 3).getValueAtPercentile(50), missing.percentile(50), 0.0);
    }

    /** Values too far apart for the histogram's range are refused, as stock's histogram refuses them. */
    public void testTooWideARangeIsRefused() {
        index(List.of(new Doc("a", 1L, null), new Doc("a", 1_000_000_000_000_000L, null)));
        expectThrows(ArrayIndexOutOfBoundsException.class, () -> stock(List.of(1.0, 1e18), 5));
        expectThrows(
            ArrayIndexOutOfBoundsException.class,
            () -> run(AggregationBuilders.percentiles("p").field("l").percentiles(50).percentilesConfig(new PercentilesConfig.Hdr(5)))
        );
    }

    /** Whether one stock histogram takes {@code a} and {@code b}, recorded in either order. */
    private static boolean stockTakes(double a, double b, int digits) {
        for (double[] order : new double[][] { { a, b }, { b, a } }) {
            DoubleHistogram h = new DoubleHistogram(digits);
            h.setAutoResize(true);
            try {
                for (double v : order) {
                    h.recordValue(v);
                }
            } catch (ArrayIndexOutOfBoundsException e) {
                return false;
            }
        }
        return true;
    }

    /** The greatest double a stock histogram still takes beside {@code least}. */
    private static double greatestBeside(double least, int digits) {
        long lo = Double.doubleToLongBits(least);
        long hi = Double.doubleToLongBits(least * Math.pow(2, BEYOND_ANY_RANGE));
        // A positive double's bits order as its value does.
        while (hi - lo > 1) {
            long mid = (lo + hi) >>> 1;
            if (stockTakes(least, Double.longBitsToDouble(mid), digits)) {
                lo = mid;
            } else {
                hi = mid;
            }
        }
        return Double.longBitsToDouble(lo);
    }

    /**
     * The least value at the top edge of its histogram bucket, where the cut
     * lowers it the most, and the greatest value at the range limit stock's
     * histogram has beside it: served with stock's values, and a value just
     * past the limit refused as stock refuses it. The histogram moves its
     * range by powers of two from the least value's, and the cut keeps the
     * least value in its bucket, so it never narrows how far apart the
     * values may be.
     */
    public void testLeastValueAtABucketTopAndGreatestAtTheRangeLimit() {
        for (int digits = 0; digits <= 5; digits++) {
            // Anywhere, and just under a power of two, where log2 may round up to it.
            assertAtTheRangeLimit(Math.pow(2, randomDoubleBetween(-EDGE_EXPONENTS, EDGE_EXPONENTS, true)), digits);
            assertAtTheRangeLimit(Math.nextDown(Math.pow(2, randomIntBetween(-EDGE_EXPONENTS, EDGE_EXPONENTS))), digits);
        }
    }

    /**
     * {@code d} cut to {@link #EDGE_DIGITS} significant decimal digits,
     * rounding as {@code mode} says. A double written with that few digits
     * reads back from the table exactly; one written with all seventeen can
     * read back one unit in the last place off, which would move a value
     * off the edge it is meant to sit on.
     */
    private static double shortDecimal(double d, RoundingMode mode) {
        return new BigDecimal(d).round(new MathContext(EDGE_DIGITS, mode)).doubleValue();
    }

    /**
     * The top of {@code base}'s bucket as the least value, beside the
     * greatest value stock takes with it, then beside the least value past
     * that, each as close to its edge as a short decimal gets.
     */
    private void assertAtTheRangeLimit(double base, int digits) {
        DoubleHistogram one = new DoubleHistogram(digits);
        one.setAutoResize(true);
        one.recordValue(base);
        double least = shortDecimal(Math.nextDown(one.nextNonEquivalentValue(base)), RoundingMode.DOWN);
        assertTrue(one.valuesAreEquivalent(base, least));
        // The histogram's range ends at a power of two: the least value past it.
        double limit = Math.nextUp(greatestBeside(least, digits));
        assertEquals(limit, Math.pow(2, Math.getExponent(limit)), 0.0);
        double greatest = shortDecimal(Math.nextDown(limit), RoundingMode.DOWN);
        double past = shortDecimal(limit, RoundingMode.UP);
        assertTrue(stockTakes(least, greatest, digits));
        assertFalse(stockTakes(least, past, digits));
        assertTrue("the table reads the edge values back as written", readsBack(least, greatest, past));

        resetTable();
        index(List.of(new Doc("a", null, least), new Doc("a", null, greatest)));
        // One shard's histogram, the two values recorded as written, as
        // stock answers a one-shard index: copying a histogram this wide, as
        // a reduce over several shards starts by doing, can itself throw.
        DoubleHistogram shard = new DoubleHistogram(digits);
        shard.setAutoResize(true);
        shard.recordValue(least);
        shard.recordValue(greatest);
        double[] percents = percents();
        InternalHDRPercentiles p = (InternalHDRPercentiles) run(
            AggregationBuilders.percentiles("p").field("d").percentiles(percents).percentilesConfig(new PercentilesConfig.Hdr(digits))
        );
        assertEquals(shard.getTotalCount(), p.getState().getTotalCount());
        for (double percent : percents) {
            assertEquals("digits " + digits + " p" + percent, shard.getValueAtPercentile(percent), p.percentile(percent), 0.0);
        }
        double[] ranked = { least, greatest, one.lowestEquivalentValue(least) };
        InternalHDRPercentileRanks r = (InternalHDRPercentileRanks) run(
            AggregationBuilders.percentileRanks("r", ranked).field("d").percentilesConfig(new PercentilesConfig.Hdr(digits))
        );
        for (double v : ranked) {
            assertEquals("digits " + digits + " rank of " + v, InternalHDRPercentileRanks.percentileRank(shard, v), r.percent(v), 0.0);
        }

        resetTable();
        index(List.of(new Doc("a", null, least), new Doc("a", null, past)));
        expectThrows(
            ArrayIndexOutOfBoundsException.class,
            () -> run(AggregationBuilders.percentiles("p").field("d").percentiles(50).percentilesConfig(new PercentilesConfig.Hdr(digits)))
        );
    }

    /** Whether the table reads each of {@code values} back as written. */
    private static boolean readsBack(double... values) {
        resetTable();
        List<Doc> docs = new ArrayList<>();
        for (double v : values) {
            docs.add(new Doc("a", null, v));
        }
        index(docs);
        List<Double> read = new ArrayList<>();
        for (Map<String, Object> row : InfinoNative.sqlRows("SELECT d FROM \"" + TABLE + "\"")) {
            read.add(((Number) row.get("d")).doubleValue());
        }
        for (double v : values) {
            if (!read.contains(v)) {
                return false;
            }
        }
        return true;
    }

    /** A negative value is refused, as stock's histogram refuses it, rather than left out. */
    public void testNegativeValueIsRefused() {
        index(List.of(new Doc("a", 5L, null), new Doc("a", -7L, null)));
        expectThrows(
            ArrayIndexOutOfBoundsException.class,
            () -> run(AggregationBuilders.percentiles("p").field("l").percentiles(50).percentilesConfig(new PercentilesConfig.Hdr(3)))
        );
    }
}
