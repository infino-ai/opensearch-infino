/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import org.opensearch.action.delete.DeleteRequest;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.test.OpenSearchTestCase;

public class InfinoDocumentVersionTests extends OpenSearchTestCase {

    private static final ShardId SHARD = new ShardId("logs", "_na_", 0);

    private static IndexRequest write(long seqNo, long term) {
        return new IndexRequest("logs").id("1").setIfSeqNo(seqNo).setIfPrimaryTerm(term);
    }

    public void testAMatchingConditionHolds() {
        assertNull(InfinoDocumentVersion.conflict(SHARD, write(8, InfinoDocumentVersion.PRIMARY_TERM), 8L));
    }

    public void testAStaleSequenceNumberConflicts() {
        assertNotNull(InfinoDocumentVersion.conflict(SHARD, write(7, InfinoDocumentVersion.PRIMARY_TERM), 8L));
        assertNotNull(InfinoDocumentVersion.conflict(SHARD, new DeleteRequest("logs", "1").setIfSeqNo(7).setIfPrimaryTerm(1), 8L));
    }

    public void testAnotherPrimaryTermConflicts() {
        assertNotNull(InfinoDocumentVersion.conflict(SHARD, write(8, 2), 8L));
    }

    public void testBothStaleConflicts() {
        assertNotNull(InfinoDocumentVersion.conflict(SHARD, write(7, 2), 8L));
    }

    public void testAMissingDocumentConflicts() {
        assertNotNull(InfinoDocumentVersion.conflict(SHARD, write(8, InfinoDocumentVersion.PRIMARY_TERM), null));
    }

    public void testAnUnconditionedWriteHasNoConflict() {
        assertNull(InfinoDocumentVersion.conflict(SHARD, new IndexRequest("logs").id("1"), 8L));
    }
}
