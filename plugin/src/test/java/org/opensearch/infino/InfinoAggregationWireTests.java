/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import org.apache.lucene.util.BytesRef;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.SearchModule;
import org.opensearch.search.aggregations.BucketOrder;
import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.search.aggregations.bucket.composite.CompositeAggregation;
import org.opensearch.search.aggregations.bucket.composite.InternalComposite;
import org.opensearch.search.aggregations.bucket.filter.InternalFilter;
import org.opensearch.search.aggregations.bucket.missing.MissingOrder;
import org.opensearch.search.aggregations.bucket.terms.DoubleTerms;
import org.opensearch.search.aggregations.bucket.terms.SignificantLongTerms;
import org.opensearch.search.aggregations.bucket.terms.SignificantTerms;
import org.opensearch.search.aggregations.bucket.terms.Terms;
import org.opensearch.search.aggregations.bucket.terms.heuristic.JLHScore;
import org.opensearch.search.aggregations.metrics.InternalCardinality;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;
import java.util.Map;

/** The aggregation objects built through their wire form answer what they were built from. */
public class InfinoAggregationWireTests extends OpenSearchTestCase {

    /** The relative error allowed for a unique count past the exact range; OpenSearch's own sketch is quoted at about 1%. */
    private static final double HLL_TOLERANCE = 0.01;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        InfinoServices.namedWriteableRegistry(new NamedWriteableRegistry(new SearchModule(Settings.EMPTY, List.of()).getNamedWriteables()));
    }

    public void testCardinalityIsExactInTheLinearCountingRange() {
        for (long count : new long[] { 0, 1, 2, 17, 1_000, 3_000, 3_072 }) {
            InternalCardinality c = InfinoAggregationWire.cardinality("u", Map.of(), count);
            assertEquals("count " + count, count, c.getValue());
        }
    }

    public void testDoubleTermsReadsBackItsBuckets() {
        DoubleTerms terms = InfinoAggregationWire.doubleTerms(
            "t",
            Map.of(),
            BucketOrder.count(false),
            10,
            25,
            1,
            7,
            List.of(new InfinoAggregationWire.DoubleBucket(1.5, 4, InternalAggregations.EMPTY), new InfinoAggregationWire.DoubleBucket(2.25, 2, null))
        );
        assertEquals("t", terms.getName());
        // Through the public interface: the bucket class is not public in every release.
        List<? extends Terms.Bucket> buckets = ((Terms) terms).getBuckets();
        assertEquals(2, buckets.size());
        assertEquals(1.5, buckets.get(0).getKeyAsNumber().doubleValue(), 0.0);
        assertEquals(4, buckets.get(0).getDocCount());
        assertEquals(2.25, buckets.get(1).getKeyAsNumber().doubleValue(), 0.0);
        assertEquals(7, terms.getSumOfOtherDocCounts());
    }

    public void testFilterReadsBackItsCountMetaAndSubAggregations() {
        InternalCardinality sub = InfinoAggregationWire.cardinality("u", null, 2);
        InternalFilter filter = InfinoAggregationWire.filter("f", Map.of("panel", "tsvb"), 3, InternalAggregations.from(List.of(sub)));
        assertEquals("f", filter.getName());
        assertEquals(3, filter.getDocCount());
        assertEquals(Map.of("panel", "tsvb"), filter.getMetadata());
        InternalCardinality back = filter.getAggregations().get("u");
        assertEquals(2, back.getValue());
        assertNull(back.getMetadata());
        // No meta in the request is none in the answer, as stock's.
        assertNull(InfinoAggregationWire.filter("g", null, 0, InternalAggregations.EMPTY).getMetadata());
    }

    public void testSignificantLongTermsReadsBackItsBuckets() {
        SignificantLongTerms terms = InfinoAggregationWire.significantLongTerms(
            "s",
            Map.of(),
            10,
            1,
            DocValueFormat.RAW,
            30,
            100,
            new JLHScore(),
            List.of(new InfinoAggregationWire.SignificantLongBucket(404L, 12, 15, InternalAggregations.EMPTY), new InfinoAggregationWire.SignificantLongBucket(200L, 3, 60, null))
        );
        assertEquals("s", terms.getName());
        List<? extends SignificantTerms.Bucket> buckets = ((SignificantTerms) terms).getBuckets();
        assertEquals(2, buckets.size());
        assertEquals(404L, buckets.get(0).getKeyAsNumber().longValue());
        assertEquals(12, buckets.get(0).getSubsetDf());
        assertEquals(15, buckets.get(0).getSupersetDf());
        assertEquals(30, buckets.get(0).getSubsetSize());
        assertEquals(100, buckets.get(0).getSupersetSize());
        assertEquals(200L, buckets.get(1).getKeyAsNumber().longValue());
        assertEquals(3, buckets.get(1).getDocCount());
    }

    public void testCompositeReadsBackItsBucketsAndAfterKey() {
        InternalComposite composite = InfinoAggregationWire.composite(
            "c",
            Map.of(),
            2,
            List.of("cat", "n"),
            List.of(DocValueFormat.RAW, DocValueFormat.RAW),
            new int[] { 1, -1 },
            new MissingOrder[] { MissingOrder.DEFAULT, MissingOrder.FIRST },
            List.of(
                new InfinoAggregationWire.CompositeBucket(new Comparable<?>[] { new BytesRef("coats"), 7L }, 4, InternalAggregations.EMPTY),
                new InfinoAggregationWire.CompositeBucket(new Comparable<?>[] { new BytesRef("hats"), null }, 1, null)
            ),
            new Comparable<?>[] { new BytesRef("hats"), null }
        );
        assertEquals("c", composite.getName());
        assertEquals(2, composite.getSize());
        List<? extends CompositeAggregation.Bucket> buckets = composite.getBuckets();
        assertEquals(2, buckets.size());
        assertEquals(Map.of("cat", "coats", "n", 7L), buckets.get(0).getKey());
        assertEquals(4, buckets.get(0).getDocCount());
        assertEquals("hats", buckets.get(1).getKey().get("cat"));
        assertNull(buckets.get(1).getKey().get("n"));
        assertEquals("hats", composite.afterKey().get("cat"));
        assertNull(composite.afterKey().get("n"));
    }

    public void testCardinalityTracksTheCountPastIt() {
        for (long count : new long[] { 3_073, 5_000, 11_000, 12_000, 20_000, 60_000, 81_000, 100_000, 1_000_000, 50_000_000L, 2_000_000_000L }) {
            InternalCardinality c = InfinoAggregationWire.cardinality("u", Map.of(), count);
            long value = c.getValue();
            double error = Math.abs(value - count) / (double) count;
            logger.info("cardinality {} -> {} ({}%)", count, value, String.format(java.util.Locale.ROOT, "%.3f", error * 100));
            assertTrue("count " + count + " came back as " + value, error <= HLL_TOLERANCE);
        }
    }
}
