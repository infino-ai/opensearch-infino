/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import org.opensearch.client.ResponseException;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

/**
 * A default-mode index with a replica, on a two-node cluster over one
 * catalog. A replica added after writes the primary has not flushed
 * recovers by replaying them from the primary's history, which needs the
 * source each copy keeps for recovery; a replica that takes over as primary
 * serves every write the old primary acknowledged, each once; and a write
 * reaches the table only once the copies hold it.
 */
public class InfinoEngineReplicaIT extends InfinoRestTestCase {

    /** Documents written before the replica exists, or before the primary is taken away. */
    private static final int DOCS = 40;
    /** Documents written to the new primary after it takes over. */
    private static final int LATER = 5;
    /** How long a copy may take to recover. */
    private static final String GREEN_TIMEOUT = "60s";
    /** How long a node's reads may take to see another node's commit: well past the table's read staleness window. */
    private static final long STALENESS_WAIT_SECONDS = 10;

    public void testAReplicaAddedAfterUnflushedWritesRecoversFromThePrimarysHistory() throws IOException {
        String index = "eng_rep_recover";
        createIndex(index, 0, "");
        write(index, 0, DOCS);
        call("PUT", "/" + index + "/_settings", "{\"index\":{\"number_of_replicas\":1}}");
        assertGreen(index);
        call("POST", "/" + index + "/_refresh", null);
        for (String preference : new String[] { "_primary", "_replica" }) {
            assertEquals(preference, Set.copyOf(ids(0, DOCS)), hitIds(index, preference));
        }
        assertEquals(DOCS, count(index));
        call("DELETE", "/" + index, null);
    }

    public void testAPromotedReplicaServesEveryAcknowledgedWrite() throws Exception {
        String index = "eng_rep_promote";
        createIndex(index, 1, "");
        assertGreen(index);
        write(index, 0, DOCS);
        cancelPrimary(index);
        assertGreen(index);
        write(index, DOCS, LATER);
        call("POST", "/" + index + "/_refresh", null);
        assertServesEvery(index, DOCS + LATER);
        call("DELETE", "/" + index, null);
    }

    /**
     * A primary whose replica does not hold its writes, as one cut off from
     * its replica would be, keeps them out of the table, so a search after a
     * refresh finds none; when the replica takes over, it writes them into
     * the table from its own history, as they are now the shard's.
     */
    public void testAWriteTheCopiesDoNotHoldStaysOutOfTheTableUntilOneTakesOver() throws Exception {
        String index = "eng_rep_held";
        // The copies sync their translogs only hourly, so the replica never holds
        // a write durably and the shard's global checkpoint does not move.
        createIndex(index, 1, ",\"index.translog.durability\":\"async\",\"index.translog.sync_interval\":\"1h\"");
        assertGreen(index);
        write(index, 0, DOCS);
        call("POST", "/" + index + "/_refresh", null);
        assertEquals("no write reaches the table ahead of its copies", Set.of(), hitIds(index, "_primary"));
        assertEquals(0, count(index));
        cancelPrimary(index);
        assertGreen(index);
        call("POST", "/" + index + "/_refresh", null);
        assertServesEvery(index, DOCS);
        call("DELETE", "/" + index, null);
    }

    /** Take the primary away from its node, so the replica takes over. */
    @SuppressWarnings("unchecked")
    private void cancelPrimary(String index) throws IOException {
        String primary = null;
        for (Object shard : callList("GET", "/_cat/shards/" + index + "?format=json&h=prirep,node", null)) {
            Map<String, Object> s = (Map<String, Object>) shard;
            if ("p".equals(s.get("prirep"))) {
                primary = s.get("node").toString();
            }
        }
        assertNotNull("the primary's node", primary);
        call(
            "POST",
            "/_cluster/reroute",
            "{\"commands\":[{\"cancel\":{\"index\":\"" + index + "\",\"shard\":0,\"node\":\"" + primary + "\",\"allow_primary\":true}}]}"
        );
    }

    /**
     * The primary serves documents {@code d0} to {@code d<docs - 1>}, each
     * once, by search, count and get. A count may run on the other node's
     * copy, and a get is answered by the node it is sent to, each from that
     * node's own view of the table, which takes up another node's commit
     * within the table's read staleness window; so those retry.
     */
    @SuppressWarnings("unchecked")
    private void assertServesEvery(String index, int docs) throws Exception {
        assertEquals(Set.copyOf(ids(0, docs)), hitIds(index, "_primary"));
        assertBusy(() -> {
            try {
                assertEquals(docs, count(index));
                for (int i = 0; i < docs; i++) {
                    Map<String, Object> doc = call("GET", "/" + index + "/_doc/d" + i, null);
                    assertEquals("d" + i, i, ((Map<String, Object>) doc.get("_source")).get("n"));
                }
            } catch (ResponseException e) {
                throw new AssertionError(e);
            }
        }, STALENESS_WAIT_SECONDS, TimeUnit.SECONDS);
    }

    /** A default-mode index of one shard, with {@code extra} settings after the others. */
    private void createIndex(String index, int replicas, String extra) throws IOException {
        // Refresh off: only a write's own translog sync commits it to the table.
        call(
            "PUT",
            "/" + index,
            "{\"settings\":{\"index.infino.enabled\":true,\"index.infino.mode\":\"default\",\"index.refresh_interval\":-1,"
                + "\"number_of_shards\":1,\"number_of_replicas\":"
                + replicas
                + extra
                + "},\"mappings\":{\"properties\":{\"n\":{\"type\":\"long\"}}}}"
        );
    }

    /** Write documents {@code d<first>} on, one request each, unrefreshed. */
    private void write(String index, int first, int count) throws IOException {
        for (int i = first; i < first + count; i++) {
            call("PUT", "/" + index + "/_doc/d" + i, "{\"n\":" + i + "}");
        }
    }

    private void assertGreen(String index) throws IOException {
        Map<String, Object> health = call("GET", "/_cluster/health/" + index + "?wait_for_status=green&timeout=" + GREEN_TIMEOUT, null);
        assertEquals("green: " + health, "green", health.get("status"));
    }

    private int count(String index) throws IOException {
        return ((Number) call("GET", "/" + index + "/_count", null).get("count")).intValue();
    }

    @SuppressWarnings("unchecked")
    private Set<Object> hitIds(String index, String preference) throws IOException {
        Map<String, Object> hits = (Map<String, Object>) call(
            "POST",
            "/" + index + "/_search?preference=" + preference,
            "{\"size\":" + (DOCS + LATER + 1) + "}"
        ).get("hits");
        Set<Object> ids = new HashSet<>();
        for (Object h : (List<?>) hits.get("hits")) {
            assertTrue("each document once", ids.add(((Map<String, Object>) h).get("_id")));
        }
        return ids;
    }

    private static List<String> ids(int first, int count) {
        return IntStream.range(first, first + count).mapToObj(i -> "d" + i).toList();
    }
}
