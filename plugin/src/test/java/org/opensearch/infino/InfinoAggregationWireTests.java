/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.search.SearchModule;
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
        InfinoPlugin.namedWriteableRegistry(new NamedWriteableRegistry(new SearchModule(Settings.EMPTY, List.of()).getNamedWriteables()));
    }

    public void testCardinalityIsExactInTheLinearCountingRange() {
        for (long count : new long[] { 0, 1, 2, 17, 1_000, 3_000, 3_072 }) {
            InternalCardinality c = InfinoAggregationWire.cardinality("u", Map.of(), count);
            assertEquals("count " + count, count, c.getValue());
        }
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
