/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.SearchResponseSections;
import org.opensearch.action.search.ShardSearchFailure;
import org.opensearch.search.SearchHits;
import org.opensearch.search.internal.InternalSearchResponse;
import org.opensearch.test.OpenSearchTestCase;

/** A default-mode search some shard refused is refused whole; any other shard failure stays a partial result. */
public class InfinoSearchActionFilterTests extends OpenSearchTestCase {

    private static SearchResponse withFailures(ShardSearchFailure... failures) {
        SearchResponseSections sections = new InternalSearchResponse(SearchHits.empty(), null, null, null, false, null, 1);
        return new SearchResponse(sections, null, 3, 3 - failures.length, 0, 1, failures, SearchResponse.Clusters.EMPTY);
    }

    public void testAShardRefusalFailsTheSearch() {
        IllegalArgumentException refused = new IllegalArgumentException("[infino] sort mode [median] on [field], which documents give several values, is not served");
        RuntimeException wrapped = new RuntimeException("Query Failed [Failed to execute main query]", refused);
        IllegalArgumentException thrown = expectThrows(
            IllegalArgumentException.class,
            () -> InfinoSearchActionFilter.refuseIfAnyShardRefused(withFailures(new ShardSearchFailure(wrapped)))
        );
        assertSame(refused, thrown);
    }

    public void testOtherShardFailuresStayPartial() {
        InfinoSearchActionFilter.refuseIfAnyShardRefused(withFailures());
        InfinoSearchActionFilter.refuseIfAnyShardRefused(
            withFailures(new ShardSearchFailure(new IllegalArgumentException("Unable to retrieve the requested [fields] since _source is disabled")))
        );
        assertNull(InfinoSearchActionFilter.refusal(new IllegalStateException("node closed")));
        assertNotNull(InfinoSearchActionFilter.refusal(new IllegalStateException("unsupported: [infino] aggregation [a] is not served")));
    }
}
