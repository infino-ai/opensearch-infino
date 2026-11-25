/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A primary takes a create as one of its skeleton rebuild's, and so leaves
 * the table alone for it, only when the rebuild has that id in flight with
 * the same content: whether the source reached the engine as sent or written
 * anew by a mapper, as the k-NN plugin's derived source does.
 */
public class InfinoSkeletonAdopterTests extends OpenSearchTestCase {

    private static final ShardId SHARD = new ShardId(new Index("logs", "uuid"), 0);
    private static final String ID = "a1";
    private static final String SENT = "{\"message\": \"disk full\", \"status\": 503, \"latency_ms\": 11.3, \"v\": [0.866, 0.0, 0.2887]}";

    public void testARebuildCreateIsClaimedOnce() {
        Map<String, String> inFlight = inFlight();
        InfinoSkeletonAdopter.track(SHARD, inFlight);
        try {
            assertTrue(InfinoSkeletonAdopter.claim(SHARD, ID, new BytesArray(SENT), MediaTypeRegistry.JSON));
            assertFalse("claimed once", InfinoSkeletonAdopter.claim(SHARD, ID, new BytesArray(SENT), MediaTypeRegistry.JSON));
        } finally {
            InfinoSkeletonAdopter.untrack(SHARD, inFlight);
        }
    }

    public void testASourceWrittenAnewIsStillTheRebuilds() throws IOException {
        Map<String, String> inFlight = inFlight();
        InfinoSkeletonAdopter.track(SHARD, inFlight);
        try {
            // The same document, compact and with its keys in another order.
            BytesReference rewritten = new BytesArray("{\"v\":[0.866,0.0,0.2887],\"status\":503,\"latency_ms\":11.3,\"message\":\"disk full\"}");
            assertTrue(InfinoSkeletonAdopter.claim(SHARD, ID, rewritten, MediaTypeRegistry.JSON));
        } finally {
            InfinoSkeletonAdopter.untrack(SHARD, inFlight);
        }
        assertTrue("in another content type", InfinoSkeletonAdopter.sameContent(SENT, smile(), XContentType.SMILE));
    }

    public void testAClientsWriteIsNotTheRebuilds() {
        Map<String, String> inFlight = inFlight();
        InfinoSkeletonAdopter.track(SHARD, inFlight);
        try {
            BytesReference other = new BytesArray("{\"message\": \"disk full\", \"status\": 500, \"latency_ms\": 11.3, \"v\": [0.866, 0.0, 0.2887]}");
            assertFalse("other content", InfinoSkeletonAdopter.claim(SHARD, ID, other, MediaTypeRegistry.JSON));
            assertFalse("another id", InfinoSkeletonAdopter.claim(SHARD, "a2", new BytesArray(SENT), MediaTypeRegistry.JSON));
            ShardId otherShard = new ShardId(SHARD.getIndex(), 1);
            assertFalse("another shard", InfinoSkeletonAdopter.claim(otherShard, ID, new BytesArray(SENT), MediaTypeRegistry.JSON));
            assertFalse("no source", InfinoSkeletonAdopter.claim(SHARD, ID, null, MediaTypeRegistry.JSON));
            assertFalse("not a document", InfinoSkeletonAdopter.claim(SHARD, ID, new BytesArray("not json"), MediaTypeRegistry.JSON));
            // None of them took the claim: the rebuild's own create still has it.
            assertTrue(InfinoSkeletonAdopter.claim(SHARD, ID, new BytesArray(SENT), MediaTypeRegistry.JSON));
        } finally {
            InfinoSkeletonAdopter.untrack(SHARD, inFlight);
        }
    }

    public void testNothingIsClaimedOnceTheBulkIsAnswered() {
        Map<String, String> inFlight = inFlight();
        InfinoSkeletonAdopter.track(SHARD, inFlight);
        InfinoSkeletonAdopter.untrack(SHARD, inFlight);
        assertFalse(InfinoSkeletonAdopter.claim(SHARD, ID, new BytesArray(SENT), MediaTypeRegistry.JSON));
    }

    private static Map<String, String> inFlight() {
        Map<String, String> inFlight = new ConcurrentHashMap<>();
        inFlight.put(ID, SENT);
        return inFlight;
    }

    private static BytesReference smile() throws IOException {
        try (XContentBuilder b = XContentBuilder.builder(XContentType.SMILE.xContent())) {
            b.startObject()
                .field("message", "disk full")
                .field("status", 503)
                .field("latency_ms", 11.3)
                .field("v", List.of(0.866, 0.0, 0.2887))
                .endObject();
            return BytesReference.bytes(b);
        }
    }
}
