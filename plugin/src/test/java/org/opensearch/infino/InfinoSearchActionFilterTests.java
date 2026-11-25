/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import org.opensearch.OpenSearchException;
import org.opensearch.action.search.SearchPhaseExecutionException;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.SearchResponseSections;
import org.opensearch.action.search.ShardSearchFailure;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.search.SearchHits;
import org.opensearch.search.internal.InternalSearchResponse;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

/**
 * A default-mode search some shard refused is refused whole; any other shard
 * failure stays a partial result. A search refused because a shard is
 * rebuilding its skeleton answers 503 with a Retry-After header.
 */
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

    /** The refusal a shard rebuilding its skeleton answers with, as the shard throws it. */
    private static InfinoNative.InfinoException rebuilding() {
        return new InfinoNative.InfinoException(InfinoQueryPhaseSearcher.SKELETON_INCOMPLETE + "[idx][0]", RestStatus.SERVICE_UNAVAILABLE);
    }

    /** The 503 a client sees: the shard's message, and how long to wait before trying again. */
    private static void assertRetryAfter(Exception e) {
        assertTrue(e.toString(), e instanceof OpenSearchException);
        OpenSearchException refused = (OpenSearchException) e;
        assertEquals(RestStatus.SERVICE_UNAVAILABLE, refused.status());
        assertTrue(refused.getMessage(), refused.getMessage().startsWith(InfinoQueryPhaseSearcher.SKELETON_INCOMPLETE));
        assertEquals(
            List.of(String.valueOf(InfinoSearchActionFilter.REBUILD_RETRY_AFTER_SECONDS)),
            refused.getHeader(InfinoSearchActionFilter.RETRY_AFTER)
        );
    }

    public void testASearchEveryShardOfWhichIsRebuildingAnswers503WithRetryAfter() {
        SearchPhaseExecutionException failed = new SearchPhaseExecutionException(
            "query",
            "all shards failed",
            new ShardSearchFailure[] { new ShardSearchFailure(new RuntimeException("Query Failed", rebuilding())) }
        );
        assertRetryAfter(InfinoSearchActionFilter.retryAfter(failed));
    }

    public void testASearchOneShardOfWhichIsRebuildingAnswers503WithRetryAfter() {
        RuntimeException thrown = expectThrows(
            RuntimeException.class,
            () -> InfinoSearchActionFilter.refuseIfAnyShardRefused(withFailures(new ShardSearchFailure(rebuilding())))
        );
        assertRetryAfter(InfinoSearchActionFilter.retryAfter(thrown));
        // As read back from another node, with the type's name before the message.
        RuntimeException remote = new RuntimeException("infino_exception: " + rebuilding().getMessage());
        assertRetryAfter(InfinoSearchActionFilter.retryAfter(remote));
    }

    public void testOtherFailuresAreLeftAsTheyAre() {
        IllegalStateException other = new IllegalStateException("node closed");
        assertSame(other, InfinoSearchActionFilter.retryAfter(other));
        SearchPhaseExecutionException failed = new SearchPhaseExecutionException(
            "query",
            "all shards failed",
            new ShardSearchFailure[] { new ShardSearchFailure(new IllegalArgumentException("[infino] aggregation [a] is not served")) }
        );
        assertSame(failed, InfinoSearchActionFilter.retryAfter(failed));
    }
}
