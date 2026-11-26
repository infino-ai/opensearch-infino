/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Which walk of a shard's table may begin on a node: a copy of the shard
 * that started again (from its own store, say) while the walk of the copy
 * before it had not ended waits for that walk rather than being taken for
 * it, which would leave its rebuild never run.
 */
public class InfinoSkeletonWalksTests extends OpenSearchTestCase {

    private static final ShardId SHARD = new ShardId(new Index("logs", "uuid"), 0);
    private static final ShardId OTHER_SHARD = new ShardId(new Index("logs", "uuid"), 1);

    public void testACopyStartedAgainWaitsForTheWalkOfTheCopyBefore() {
        InfinoSkeletonAdopter.Walks walks = new InfinoSkeletonAdopter.Walks();
        Object before = new Object();
        Object again = new Object();
        assertEquals(InfinoSkeletonAdopter.Walks.Begin.NOW, walks.begin(SHARD, before));
        assertEquals("the same copy's walk runs already", InfinoSkeletonAdopter.Walks.Begin.RUNNING, walks.begin(SHARD, before));
        assertEquals("a new copy waits, not taken for the old walk", InfinoSkeletonAdopter.Walks.Begin.LATER, walks.begin(SHARD, again));
        assertEquals("another shard's walk is its own", InfinoSkeletonAdopter.Walks.Begin.NOW, walks.begin(OTHER_SHARD, again));
        // The new copy ending nothing it does not run leaves the old walk in place.
        walks.end(SHARD, again);
        assertEquals(InfinoSkeletonAdopter.Walks.Begin.LATER, walks.begin(SHARD, again));
        walks.end(SHARD, before);
        assertEquals("once the old walk ends, the new copy's begins", InfinoSkeletonAdopter.Walks.Begin.NOW, walks.begin(SHARD, again));
    }
}
