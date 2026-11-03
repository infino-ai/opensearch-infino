/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import org.opensearch.common.util.BigArrays;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.pipeline.PipelineAggregator;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;
import java.util.Map;

public class InfinoExactAggregationTests extends OpenSearchTestCase {

    private static InternalAggregation.ReduceContext finalReduce() {
        return InternalAggregation.ReduceContext.forFinalReduction(
            BigArrays.NON_RECYCLING_INSTANCE,
            null,
            buckets -> {},
            PipelineAggregator.PipelineTree.EMPTY
        );
    }

    /**
     * A routed search's record is held for its final reduce until the search
     * forgets it, as it does once it answers; a reduce after that is refused
     * by name, never answered.
     */
    public void testARecordIsHeldUntilItsSearchForgetsIt() {
        String request = "exact-" + randomAlphaOfLength(8);
        InfinoExactAggregation part = new InfinoExactAggregation("distinct", null, request, List.of());
        InfinoExactAggregation.record(request, new SearchSourceBuilder(), 0L, Map.of());
        // Held: the record is read, and its search names no such aggregation.
        IllegalStateException held = expectThrows(IllegalStateException.class, () -> part.reduce(List.of(part), finalReduce()));
        assertTrue(held.getMessage(), held.getMessage().contains("names no aggregation [distinct]"));
        InfinoExactAggregation.forget(request);
        IllegalStateException gone = expectThrows(IllegalStateException.class, () -> part.reduce(List.of(part), finalReduce()));
        assertTrue(gone.getMessage(), gone.getMessage().contains("no longer holds it"));
    }
}
