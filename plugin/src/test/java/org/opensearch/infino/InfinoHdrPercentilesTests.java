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

    @Override
    public void setUp() throws Exception {
        super.setUp();
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

    /** The values of a field over every magnitude, repeats and zeros among them. */
    private List<Double> spread(boolean whole) {
        int n = randomIntBetween(500, 3000);
        int maxExponent = randomIntBetween(2, 15);
        List<Double> out = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            double v;
            if (rarely()) {
                v = 0;
            } else if (!out.isEmpty() && rarely()) {
                v = randomFrom(out);
            } else {
                v = Math.pow(10, randomDoubleBetween(whole ? 0 : -4, maxExponent, true));
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

    /** A negative value is refused, as stock's histogram refuses it, rather than left out. */
    public void testNegativeValueIsRefused() {
        index(List.of(new Doc("a", 5L, null), new Doc("a", -7L, null)));
        expectThrows(
            ArrayIndexOutOfBoundsException.class,
            () -> run(AggregationBuilders.percentiles("p").field("l").percentiles(50).percentilesConfig(new PercentilesConfig.Hdr(3)))
        );
    }
}
