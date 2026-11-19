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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
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
    /** The retention lease a default-mode table holds on its shard's history (the engine's {@code TABLE_LEASE}). */
    private static final String TABLE_LEASE = "infino_table";
    /** Distinct users among the documents written before the refresh. */
    private static final int USERS = 20;
    /** Searches sent in turn, so that each node of the two coordinates at least one. */
    private static final int COORDINATOR_ROUNDS = 4;

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

    /**
     * Only the primary keeps a search in the shard request cache: a
     * replica reads a table another node writes, which it may see late
     * while its reader stays the same, so it answers without keeping.
     */
    public void testOnlyThePrimaryFillsTheShardRequestCache() throws Exception {
        String index = "eng_rep_cache";
        createIndex(index, 1, "");
        assertGreen(index);
        write(index, 0, DOCS);
        call("POST", "/" + index + "/_refresh", null);
        for (String preference : new String[] { "_primary", "_replica" }) {
            for (int i = 0; i < 3; i++) {
                assertEquals(preference, DOCS, total(index, preference));
            }
        }
        assertEquals("the primary: one miss, two hits", List.of(1, 2), requestCache(index, true));
        assertEquals("the replica: never kept", List.of(0, 0), requestCache(index, false));
        call("DELETE", "/" + index, null);
    }

    /** A {@code size: 0} search's total, on the copy {@code preference} names. */
    @SuppressWarnings("unchecked")
    private int total(String index, String preference) throws IOException {
        Map<String, Object> hits = (Map<String, Object>) call("POST", "/" + index + "/_search?preference=" + preference, "{\"size\":0}")
            .get("hits");
        return ((Number) ((Map<String, Object>) hits.get("total")).get("value")).intValue();
    }

    /** The shard request cache's misses and hits on the primary copy of shard 0, or on its replica. */
    @SuppressWarnings("unchecked")
    private List<Integer> requestCache(String index, boolean primary) throws IOException {
        Map<String, Object> indices = (Map<String, Object>) call("GET", "/" + index + "/_stats/request_cache?level=shards", null).get("indices");
        Map<String, Object> shards = (Map<String, Object>) ((Map<String, Object>) indices.get(index)).get("shards");
        for (Object c : (List<?>) shards.get("0")) {
            Map<String, Object> copy = (Map<String, Object>) c;
            if (primary == (Boolean) ((Map<String, Object>) copy.get("routing")).get("primary")) {
                Map<String, Object> cache = (Map<String, Object>) copy.get("request_cache");
                return List.of(((Number) cache.get("miss_count")).intValue(), ((Number) cache.get("hit_count")).intValue());
            }
        }
        throw new AssertionError("no " + (primary ? "primary" : "replica") + " copy of " + index);
    }

    /**
     * The table holds the shard's history from the first write it lacks: a
     * retention lease, moved forward as commits land, which the replica is
     * given too, so a promoted copy keeps every write it has to put in the
     * table however long commits failed before.
     */
    public void testTheTableHoldsTheShardsHistoryFromTheFirstWriteItLacks() throws Exception {
        String index = "eng_rep_lease";
        createIndex(index, 1, "");
        assertGreen(index);
        write(index, 0, DOCS);
        call("POST", "/" + index + "/_refresh", null);
        // Operations 0 to DOCS - 1 are in the table: it holds the history from DOCS.
        assertEquals(Long.valueOf(DOCS), tableLease(index, true));
        // The replica is given the lease when it is added, and its renewals at
        // the next lease sync (30 s apart): its copy is never past the
        // primary's, which only makes a takeover write more of its history.
        Long replica = tableLease(index, false);
        assertNotNull("the replica holds the table's lease", replica);
        assertTrue("the replica's lease " + replica + " is not past the primary's", replica <= DOCS);
        write(index, DOCS, LATER);
        call("POST", "/" + index + "/_refresh", null);
        assertEquals(Long.valueOf(DOCS + LATER), tableLease(index, true));
        cancelPrimary(index);
        assertGreen(index);
        call("POST", "/" + index + "/_refresh", null);
        assertServesEvery(index, DOCS + LATER);
        call("DELETE", "/" + index, null);
    }

    /**
     * #137: a lease renewal changes only the leases in memory, which reach
     * disk on the lease sync, while a flush records the history it keeps
     * from them. A flush between two syncs, then a store recovery, must not
     * load the table's lease behind that history: the shard recovers with
     * every acknowledged write. Writes run while the leases sync often, so a
     * sync can save the table's lease while a commit is in flight.
     */
    public void testAShardFlushedBetweenLeaseSyncsRecoversWithEveryWrite() throws Exception {
        String index = "eng_rep_lease_flush";
        createIndex(index, 0, ",\"index.soft_deletes.retention_lease.sync_interval\":\"200ms\"");
        assertGreen(index);
        Set<String> acked = ConcurrentHashMap.newKeySet();
        for (int round = 0; round < 3; round++) {
            call("PUT", "/" + index + "/_settings", "{\"index.soft_deletes.retention_lease.sync_interval\":\"200ms\"}");
            AtomicBoolean stop = new AtomicBoolean();
            int base = round * 1_000_000;
            Thread writer = new Thread(() -> {
                for (int i = base; !stop.get(); i++) {
                    try {
                        call("PUT", "/" + index + "/_doc/w" + i, "{\"n\":" + i + "}");
                        acked.add("w" + i);
                    } catch (IOException e) {
                        return;
                    }
                }
            });
            writer.start();
            Thread.sleep(1_500);
            call("PUT", "/" + index + "/_settings", "{\"index.soft_deletes.retention_lease.sync_interval\":\"1h\"}");
            Thread.sleep(300);
            stop.set(true);
            writer.join();
            call("POST", "/" + index + "/_flush?force=true", null);
            call("POST", "/" + index + "/_close", null);
            call("POST", "/" + index + "/_open", null);
            assertGreen(index);
        }
        assertFalse("the writer acknowledged writes", acked.isEmpty());
        call("POST", "/" + index + "/_refresh", null);
        assertBusy(() -> {
            try {
                int served = count(index);
                assertTrue("served " + served + " of " + acked.size() + " acknowledged writes", served >= acked.size());
            } catch (ResponseException e) {
                throw new AssertionError(e);
            }
        }, STALENESS_WAIT_SECONDS, TimeUnit.SECONDS);
        call("DELETE", "/" + index, null);
    }

    /**
     * OpenSearch expires a lease not renewed within the lease period, and a
     * commit renews the table's lease only when it moves. The engine renews
     * it where it is, so a shard idle for several periods keeps it.
     */
    public void testTheTablesLeaseOutlivesTheLeasePeriodOnAnIdleShard() throws Exception {
        String index = "eng_rep_lease_idle";
        createIndex(
            index,
            0,
            ",\"index.soft_deletes.retention_lease.period\":\"5s\",\"index.soft_deletes.retention_lease.sync_interval\":\"1s\""
        );
        assertGreen(index);
        write(index, 0, DOCS);
        call("POST", "/" + index + "/_refresh", null);
        assertEquals(Long.valueOf(DOCS), tableLease(index, true));
        Thread.sleep(15_000); // three lease periods with no commit; a lease sync runs every second
        assertEquals("the table's lease is kept, where it was", Long.valueOf(DOCS), tableLease(index, true));
        call("DELETE", "/" + index, null);
    }

    /**
     * An index whose two shards are on different nodes: a cardinality and a
     * count over it are the whole index's, answered once by the coordinating
     * node over both shards' tables, each read at its own shard's cut, so a
     * write no reader holds yet is in neither the hits nor the aggregation,
     * whichever node coordinates.
     */
    @SuppressWarnings("unchecked")
    public void testAnExactAggregationOverShardsOnTwoNodesIsTheWholeIndexs() throws Exception {
        String index = "eng_rep_exact";
        call(
            "PUT",
            "/" + index,
            "{\"settings\":{\"index.infino.enabled\":true,\"index.infino.mode\":\"default\",\"index.refresh_interval\":-1,"
                + "\"number_of_shards\":2,\"number_of_replicas\":0,\"index.routing.allocation.total_shards_per_node\":1},"
                + "\"mappings\":{\"properties\":{\"n\":{\"type\":\"long\"},\"user\":{\"type\":\"keyword\"}}}}"
        );
        assertGreen(index);
        Set<Object> nodes = new HashSet<>();
        for (Object shard : callList("GET", "/_cat/shards/" + index + "?format=json&h=node", null)) {
            nodes.add(((Map<String, Object>) shard).get("node"));
        }
        assertEquals("one shard on each node", 2, nodes.size());
        for (int i = 0; i < DOCS; i++) {
            call("PUT", "/" + index + "/_doc/d" + i, "{\"n\":" + i + ",\"user\":\"u" + (i % USERS) + "\"}");
        }
        call("POST", "/" + index + "/_refresh", null);
        // Written after the refresh: in the tables, in no reader, so in no answer.
        for (int i = DOCS; i < DOCS + LATER; i++) {
            call("PUT", "/" + index + "/_doc/d" + i, "{\"n\":" + i + ",\"user\":\"late" + i + "\"}");
        }
        // The client takes each node in turn, so each coordinates the search; a
        // node reads the other's table as it was within the table's read
        // staleness window, so the rounds retry until it has caught up.
        assertBusy(() -> {
            for (int round = 0; round < COORDINATOR_ROUNDS; round++) {
                Map<String, Object> answer;
                try {
                    answer = call(
                        "POST",
                        "/" + index + "/_search",
                        "{\"size\":0,\"aggs\":{\"users\":{\"cardinality\":{\"field\":\"user\"}},"
                            + "\"top\":{\"terms\":{\"field\":\"user\",\"size\":1},\"aggs\":{\"users\":{\"cardinality\":{\"field\":\"user\"}}}}}}"
                    );
                } catch (IOException e) {
                    throw new AssertionError(e);
                }
                Map<String, Object> total = (Map<String, Object>) ((Map<String, Object>) answer.get("hits")).get("total");
                assertEquals(DOCS, ((Number) total.get("value")).intValue());
                Map<String, Object> aggs = (Map<String, Object>) answer.get("aggregations");
                assertEquals(USERS, ((Number) ((Map<String, Object>) aggs.get("users")).get("value")).intValue());
                // Each user wrote DOCS / USERS documents, and is one user within its own bucket.
                Map<String, Object> bucket = (Map<String, Object>) ((List<?>) ((Map<String, Object>) aggs.get("top")).get("buckets")).get(0);
                assertEquals(DOCS / USERS, ((Number) bucket.get("doc_count")).intValue());
                assertEquals(1, ((Number) ((Map<String, Object>) bucket.get("users")).get("value")).intValue());
            }
        }, STALENESS_WAIT_SECONDS, TimeUnit.SECONDS);
        call("DELETE", "/" + index, null);
    }

    /** Where the table's lease holds the history of the index's shard on its primary (or replica), or null when that copy has none. */
    @SuppressWarnings("unchecked")
    private Long tableLease(String index, boolean primary) throws IOException {
        Map<String, Object> stats = call("GET", "/" + index + "/_stats?level=shards", null);
        Map<String, Object> shards = (Map<String, Object>) ((Map<String, Object>) ((Map<String, Object>) stats.get("indices")).get(index)).get(
            "shards"
        );
        for (Object copy : (List<?>) shards.get("0")) {
            Map<String, Object> c = (Map<String, Object>) copy;
            if (((Map<String, Object>) c.get("routing")).get("primary").equals(primary)) {
                Map<String, Object> leases = (Map<String, Object>) c.get("retention_leases");
                for (Object lease : (List<?>) leases.get("leases")) {
                    Map<String, Object> l = (Map<String, Object>) lease;
                    if (TABLE_LEASE.equals(l.get("id"))) {
                        return ((Number) l.get("retaining_seq_no")).longValue();
                    }
                }
            }
        }
        return null;
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
