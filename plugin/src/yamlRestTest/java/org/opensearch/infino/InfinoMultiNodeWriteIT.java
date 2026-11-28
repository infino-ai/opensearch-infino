/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import org.junit.After;
import org.junit.Before;
import org.opensearch.client.Node;
import org.opensearch.client.Request;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.Response;
import org.opensearch.client.ResponseException;
import org.opensearch.client.RestClient;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.test.rest.OpenSearchRestTestCase;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Writes to one Infino-backed index arriving at both nodes of a two-node
 * cluster. Each node sends its writes to the node holding the index's
 * primary, which writes the table, so the writes meet one sequence of
 * numbers and one view of which ids the table holds, as they would meet one
 * primary shard on a stock index.
 */
public class InfinoMultiNodeWriteIT extends OpenSearchRestTestCase {

    /** The admin user of the test cluster's internal user database. */
    private static final String ADMIN_BASIC = "admin:admin";
    /** The nodes of the test cluster. */
    private static final int NODES = 2;
    /** Writes sent one at a time, alternating between the nodes. */
    private static final int ALTERNATING_WRITES = 20;
    /** Ids each created from both nodes at once. */
    private static final int RACED_IDS = 8;
    /** How long a read on one node may take to see another node's commit. */
    private static final long STALENESS_WAIT_SECONDS = 10;
    /** The cache test's refresh interval: far past the wait, so only an invalidation lets the count move in time. */
    private static final String CACHE_TEST_REFRESH = "60s";
    /** The status of a write refused for a conflict. */
    private static final int CONFLICT = 409;
    /** The status of a get of a document the index does not hold. */
    private static final int NOT_FOUND = 404;
    /** Rounds of a write and a get of it on each node, in the get test. */
    private static final int GET_ROUNDS = 30;
    /** Settings that keep a write out of the table until a refresh: the table takes a write once the copies hold it durably, and the translog syncs only hourly. */
    private static final String UNSYNCED = ",\"index.translog.durability\":\"async\",\"index.translog.sync_interval\":\"1h\"";
    /** Updates sent one at a time, alternating between the nodes. */
    private static final int ALTERNATING_UPDATES = 20;
    /** Threads updating one document through each node at once. */
    private static final int UPDATERS_PER_NODE = 3;
    /** Updates each of those threads sends, one after another. */
    private static final int UPDATES_PER_UPDATER = 5;
    /** How often each of those updates may run again on a conflict, when it may. */
    private static final int RETRIES = 20;
    /** How many times the moving-primary test moves the primary to the other node. */
    private static final int MOVES = 4;
    /** The timeout given to a write whose shard has no active primary. */
    private static final String UNASSIGNED_TIMEOUT = "1s";
    /** How soon such a write is answered at most: well inside the default timeout of a minute. */
    private static final long UNASSIGNED_ANSWER_SECONDS = 20;

    /** A client per node, each reaching that node alone. */
    private final List<RestClient> nodeClients = new ArrayList<>();
    /** Each node's name, in the order of {@link #nodeClients}. */
    private final List<String> nodeNames = new ArrayList<>();

    @Override
    protected Settings restClientSettings() {
        return Settings.builder().put(super.restClientSettings()).put(ThreadContext.PREFIX + ".Authorization", adminAuthorization()).build();
    }

    /** The Authorization header of the admin user. */
    private static String adminAuthorization() {
        return "Basic " + Base64.getEncoder().encodeToString(ADMIN_BASIC.getBytes(StandardCharsets.UTF_8));
    }

    @Override
    protected boolean preserveIndicesUponCompletion() {
        // A wildcard delete would reach the security plugin's own index.
        return true;
    }

    /**
     * Writes sent one after another, each to the other node: every write is
     * given its own sequence number, each above the one before. Before, each
     * node counted on from the highest number the table held when it first
     * wrote, and the two nodes gave the same numbers out.
     */
    public void testWritesFromEitherNodeTakeOneSequenceOfNumbers() throws IOException {
        String index = "multi_seq";
        createIndex(index);
        List<Long> seqs = new ArrayList<>();
        for (int i = 0; i < ALTERNATING_WRITES; i++) {
            Request put = new Request("PUT", "/" + index + "/_doc/d" + i);
            put.setJsonEntity("{\"n\":" + i + "}");
            seqs.add(((Number) on(i % NODES, put).get("_seq_no")).longValue());
        }
        for (int i = 1; i < seqs.size(); i++) {
            assertTrue("each write's number above the one before: " + seqs, seqs.get(i) > seqs.get(i - 1));
        }
    }

    /**
     * A write conditioned on a document's sequence number, sent to one node
     * after the other node wrote the document again: it is refused, as the
     * document no longer stands at that number. Before, the second node
     * checked the number against its own view of the table, which may not
     * have seen the other node's commit yet, and the stale write replaced
     * the newer one.
     */
    public void testAConditionedWriteSeesTheOtherNodesWrite() throws Exception {
        String index = "multi_cas";
        createIndex(index);
        Request first = new Request("PUT", "/" + index + "/_doc/x");
        first.setJsonEntity("{\"v\":1}");
        Map<String, Object> written = on(0, first);
        long seq = ((Number) written.get("_seq_no")).longValue();
        long term = ((Number) written.get("_primary_term")).longValue();

        Request second = new Request("PUT", "/" + index + "/_doc/x");
        second.addParameter("if_seq_no", Long.toString(seq));
        second.addParameter("if_primary_term", Long.toString(term));
        second.setJsonEntity("{\"v\":2}");
        on(1, second);

        Request stale = new Request("PUT", "/" + index + "/_doc/x");
        stale.addParameter("if_seq_no", Long.toString(seq));
        stale.addParameter("if_primary_term", Long.toString(term));
        stale.setJsonEntity("{\"v\":3}");
        ResponseException refused = expectThrows(ResponseException.class, () -> on(0, stale));
        assertEquals(CONFLICT, refused.getResponse().getStatusLine().getStatusCode());

        Request refresh = new Request("POST", "/" + index + "/_refresh");
        client().performRequest(refresh);
        assertBusy(() -> {
            Map<String, Object> got = entityAsMap(client().performRequest(new Request("GET", "/" + index + "/_doc/x")));
            assertEquals("the newer write stands", 2, ((Map<?, ?>) got.get("_source")).get("v"));
        }, STALENESS_WAIT_SECONDS, TimeUnit.SECONDS);
    }

    /**
     * A get sent to either node right after a write is acknowledged finds
     * it, on the node without the index's primary as on the one with it, the
     * first time. Each round, each node first reads the table (a get of the
     * id about to be written), so its own view of the table is as recent as
     * it gets. Before, the node without the primary answered from that view,
     * which trails the primary's commit by up to the table's read staleness
     * window, and found nothing.
     */
    @SuppressWarnings("unchecked")
    public void testAGetSentToEitherNodeFindsTheWriteJustAcknowledged() throws IOException {
        String index = "multi_get";
        createIndex(index);
        for (int i = 0; i < GET_ROUNDS; i++) {
            String path = "/" + index + "/_doc/d" + i;
            for (int node = 0; node < NODES; node++) {
                int asked = node;
                ResponseException missing = expectThrows(ResponseException.class, () -> on(asked, new Request("GET", path)));
                assertEquals(NOT_FOUND, missing.getResponse().getStatusLine().getStatusCode());
            }
            Request put = new Request("PUT", path);
            put.setJsonEntity("{\"n\":" + i + "}");
            on(i % NODES, put);
            for (int node = 0; node < NODES; node++) {
                Map<String, Object> got = on(node, new Request("GET", path));
                assertEquals("round " + i + ", node " + node + ": " + got, true, got.get("found"));
                assertEquals("round " + i + ", node " + node, i, ((Map<String, Object>) got.get("_source")).get("n"));
            }
        }
    }

    /**
     * The same new id created from both nodes at once: one create succeeds,
     * the other is refused, and the index holds one document. Before, each
     * node looked the id up, found it absent, and appended it.
     */
    public void testACreateRacedFromBothNodesWritesOneDocument() throws Exception {
        String index = "multi_create";
        createIndex(index);
        ExecutorService pool = Executors.newFixedThreadPool(NODES);
        try {
            for (int i = 0; i < RACED_IDS; i++) {
                String id = "c" + i;
                List<Future<Integer>> raced = new ArrayList<>();
                for (int node = 0; node < NODES; node++) {
                    int n = node;
                    Callable<Integer> create = () -> {
                        Request put = new Request("PUT", "/" + index + "/_create/" + id);
                        put.setJsonEntity("{\"from\":" + n + "}");
                        try {
                            return performOn(n, put).getStatusLine().getStatusCode();
                        } catch (ResponseException e) {
                            return e.getResponse().getStatusLine().getStatusCode();
                        }
                    };
                    raced.add(pool.submit(create));
                }
                List<Integer> statuses = new ArrayList<>();
                for (Future<Integer> f : raced) {
                    statuses.add(f.get());
                }
                statuses.sort(Integer::compare);
                assertEquals(id + ": one create, one conflict", List.of(201, CONFLICT), statuses);
            }
        } finally {
            pool.shutdownNow();
        }
        client().performRequest(new Request("POST", "/" + index + "/_refresh"));
        assertBusy(() -> {
            Map<String, Object> counted = entityAsMap(client().performRequest(new Request("GET", "/" + index + "/_count")));
            assertEquals("one document per id", RACED_IDS, ((Number) counted.get("count")).intValue());
        }, STALENESS_WAIT_SECONDS, TimeUnit.SECONDS);
    }

    /**
     * A repeated count kept in each node's request cache, then a write with
     * refresh=true through that same node: a count soon includes it. One of
     * the nodes forwards its write to the other, which writes the table, and
     * reads that node's commit a moment later; until it does, what it reads
     * must not be kept. The index refreshes once a minute, which bounds a kept
     * response's life, so a response kept from before the write, or read just
     * after it and kept, is served for up to that minute.
     */
    public void testAWriteSentToAnotherNodeMovesTheCacheOfTheNodeThatTookIt() throws Exception {
        String index = "multi_cache";
        createIndex(index, ",\"refresh_interval\":\"" + CACHE_TEST_REFRESH + "\"");
        for (int node = 0; node < NODES; node++) {
            int n = node;
            long before = total(n, index);
            // The same request again: answered from the node's cache.
            assertEquals(before, total(n, index));
            Request put = new Request("PUT", "/" + index + "/_doc/w" + n);
            put.addParameter("refresh", "true");
            put.setJsonEntity("{\"n\":" + n + "}");
            on(n, put);
            assertBusy(() -> assertEquals("node " + n + " counts its own write", before + 1, total(n, index)), STALENESS_WAIT_SECONDS, TimeUnit.SECONDS);
        }
    }

    /**
     * An update sent through the node that does not hold the primary, right
     * after an acknowledged update through the node that does, which the
     * table has not taken yet: the second update is merged on the primary's
     * node into the document with the first, and both stand. The same with
     * the nodes the other way round.
     */
    public void testAnUpdateThroughTheOtherNodeKeepsTheUpdateBefore() throws Exception {
        String index = "multi_update_seq";
        createIndex(index, UNSYNCED);
        int primary = primaryNode(index);
        int other = 1 - primary;
        Request put = new Request("PUT", "/" + index + "/_doc/x");
        put.addParameter("refresh", "true");
        put.setJsonEntity("{\"a\":0,\"b\":0}");
        on(primary, put);

        assertEquals(200, status(primary, updateOf(index, "x", "{\"a\":1}", 0)));
        assertEquals(200, status(other, updateOf(index, "x", "{\"b\":1}", 0)));
        assertEquals("both updates stand", Map.of("a", 1, "b", 1), sourceAfterRefresh(index, "x"));

        assertEquals(200, status(other, updateOf(index, "x", "{\"a\":2}", 0)));
        assertEquals(200, status(primary, updateOf(index, "x", "{\"b\":2}", 0)));
        assertEquals("both updates stand", Map.of("a", 2, "b", 2), sourceAfterRefresh(index, "x"));
    }

    /**
     * Updates of one document sent one after another, each through the other
     * node and each setting a field of its own, none waiting on a refresh:
     * every one is applied, and the document holds every field.
     */
    public void testUpdatesAlternatingBetweenTheNodesAreAllApplied() throws Exception {
        String index = "multi_update_alternating";
        createIndex(index, UNSYNCED);
        Request put = new Request("PUT", "/" + index + "/_doc/x");
        put.setJsonEntity("{\"start\":1}");
        on(0, put);
        Map<String, Object> expected = new HashMap<>(Map.of("start", 1));
        for (int i = 0; i < ALTERNATING_UPDATES; i++) {
            String field = "f" + i;
            assertEquals(field + " applied", 200, status(i % NODES, updateOf(index, "x", "{\"" + field + "\":" + i + "}", 0)));
            expected.put(field, i);
        }
        assertEquals("every update stands", expected, sourceAfterRefresh(index, "x"));
    }

    /**
     * Updates of one document sent through both nodes at once, each setting
     * a field of its own and allowed to run again on a conflict: every one is
     * applied, and the document holds every field. An update whose merge read
     * the document before another write of it is refused at the shard and
     * runs again from a fresh read; it never overwrites a write its merge did
     * not see.
     */
    public void testConcurrentUpdatesThroughBothNodesThatMayRunAgainAreAllApplied() throws Exception {
        String index = "multi_update_race";
        createIndex(index, UNSYNCED);
        Map<String, Integer> statuses = raceUpdates(index, RETRIES);
        Map<?, ?> source = sourceAfterRefresh(index, "x");
        for (Map.Entry<String, Integer> e : statuses.entrySet()) {
            assertEquals(e.getKey() + " applied: " + statuses, 200, (int) e.getValue());
            assertEquals(e.getKey() + " stands: " + source, 1, source.get(e.getKey()));
        }
    }

    /**
     * The same race with no update allowed to run again: an update is
     * applied or refused with a conflict, as on a stock index, and every
     * applied one is in the document.
     */
    public void testConcurrentUpdatesThroughBothNodesKeepEveryAcknowledgedField() throws Exception {
        String index = "multi_update_race_once";
        createIndex(index, UNSYNCED);
        Map<String, Integer> statuses = raceUpdates(index, 0);
        Map<?, ?> source = sourceAfterRefresh(index, "x");
        int applied = 0;
        for (Map.Entry<String, Integer> e : statuses.entrySet()) {
            if (e.getValue() == 200) {
                applied++;
                assertEquals(e.getKey() + " acknowledged: " + source, 1, source.get(e.getKey()));
            } else {
                assertEquals(e.getKey() + " applied or refused", CONFLICT, (int) e.getValue());
                assertNull(e.getKey() + " refused, so not written: " + source, source.get(e.getKey()));
            }
        }
        assertTrue("some update is applied: " + statuses, applied > 0);
    }

    /**
     * Updates conditioned by the client on the document's sequence number,
     * sent through either node: one on the number the document stands at is
     * applied, one on an older number is refused with a conflict, as stock
     * refuses it, and one of a missing document is answered missing.
     */
    public void testAnUpdateTheClientConditionsIsCheckedThroughEitherNode() throws Exception {
        String index = "multi_update_cas";
        createIndex(index, UNSYNCED);
        int primary = primaryNode(index);
        int other = 1 - primary;
        Request put = new Request("PUT", "/" + index + "/_doc/x");
        put.setJsonEntity("{\"v\":0}");
        Map<String, Object> written = on(primary, put);
        long seq = ((Number) written.get("_seq_no")).longValue();
        long term = ((Number) written.get("_primary_term")).longValue();

        Map<String, Object> updated = on(other, conditioned(updateOf(index, "x", "{\"a\":1}", 0), seq, term));
        long next = ((Number) updated.get("_seq_no")).longValue();
        assertTrue("the update takes a later number", next > seq);
        assertEquals(CONFLICT, status(primary, conditioned(updateOf(index, "x", "{\"b\":1}", 0), seq, term)));
        assertEquals(CONFLICT, status(other, conditioned(updateOf(index, "x", "{\"b\":1}", 0), seq, term)));
        assertEquals(200, status(primary, conditioned(updateOf(index, "x", "{\"c\":1}", 0), next, term)));
        assertEquals(404, status(other, conditioned(updateOf(index, "missing", "{\"c\":1}", 0), seq, term)));
        assertEquals("the applied updates stand", Map.of("v", 0, "a", 1, "c", 1), sourceAfterRefresh(index, "x"));
    }

    /**
     * Updates of a missing document through the node that does not hold the
     * primary: a plain one is answered missing, one with an upsert creates
     * the document and the next merges into it, a doc-as-upsert creates its
     * own, and a scripted one is refused.
     */
    public void testAnUpdateOfAMissingDocumentThroughTheOtherNode() throws Exception {
        String index = "multi_update_missing";
        createIndex(index, UNSYNCED);
        int other = 1 - primaryNode(index);
        assertEquals(404, status(other, updateOf(index, "x", "{\"a\":1}", 0)));

        Request upsert = new Request("POST", "/" + index + "/_update/x");
        upsert.setJsonEntity("{\"doc\":{\"a\":1},\"upsert\":{\"a\":0,\"made\":true}}");
        assertEquals("created", on(other, upsert).get("result"));
        assertEquals("updated", on(other, upsert).get("result"));

        Request docAsUpsert = new Request("POST", "/" + index + "/_update/y");
        docAsUpsert.setJsonEntity("{\"doc\":{\"b\":1},\"doc_as_upsert\":true}");
        assertEquals("created", on(other, docAsUpsert).get("result"));

        Request scripted = new Request("POST", "/" + index + "/_update/x");
        scripted.setJsonEntity("{\"script\":{\"source\":\"ctx._source.a += 1\"}}");
        assertEquals("updated", on(other, scripted).get("result"));

        assertEquals("the upsert, the update, then the script", Map.of("a", 2, "made", true), sourceAfterRefresh(index, "x"));
        assertEquals("the doc as upserted", Map.of("b", 1), sourceAfterRefresh(index, "y"));
    }

    /**
     * Updates of one document sent through both nodes while its primary moves
     * from node to node: every update is applied or answered with a status a
     * client retries, and every applied one is in the document. An update
     * that reaches a node the primary has just left is sent on to the node
     * holding it now.
     */
    public void testUpdatesWhileThePrimaryMovesKeepEveryAcknowledgedField() throws Exception {
        String index = "multi_update_moving";
        createIndex(index);
        Request put = new Request("PUT", "/" + index + "/_doc/x");
        put.setJsonEntity("{\"start\":1}");
        on(0, put);
        AtomicBoolean moving = new AtomicBoolean(true);
        ExecutorService pool = Executors.newFixedThreadPool(NODES * UPDATERS_PER_NODE);
        List<Future<Map<String, Integer>>> updaters = new ArrayList<>();
        try {
            for (int node = 0; node < NODES; node++) {
                for (int t = 0; t < UPDATERS_PER_NODE; t++) {
                    int n = node;
                    String prefix = "n" + node + "_" + t + "_";
                    updaters.add(pool.submit(() -> {
                        Map<String, Integer> statuses = new TreeMap<>();
                        for (int i = 0; moving.get(); i++) {
                            String field = prefix + i;
                            statuses.put(field, status(n, updateOf(index, "x", "{\"" + field + "\":1}", RETRIES)));
                        }
                        return statuses;
                    }));
                }
            }
            for (int move = 0; move < MOVES; move++) {
                int from = primaryNode(index);
                Request reroute = new Request("POST", "/_cluster/reroute");
                reroute.setJsonEntity(
                    "{\"commands\":[{\"move\":{\"index\":\""
                        + index
                        + "\",\"shard\":0,\"from_node\":\""
                        + nodeNames.get(from)
                        + "\",\"to_node\":\""
                        + nodeNames.get(1 - from)
                        + "\"}}]}"
                );
                client().performRequest(reroute);
                Request health = new Request("GET", "/_cluster/health/" + index);
                health.addParameter("wait_for_no_relocating_shards", "true");
                health.addParameter("wait_for_status", "green");
                client().performRequest(health);
                assertEquals("the primary moved", 1 - from, primaryNode(index));
            }
            moving.set(false);
            Map<String, Integer> statuses = new TreeMap<>();
            for (Future<Map<String, Integer>> f : updaters) {
                statuses.putAll(f.get());
            }
            Map<?, ?> source = sourceAfterRefresh(index, "x");
            int applied = 0;
            for (Map.Entry<String, Integer> e : statuses.entrySet()) {
                int status = e.getValue();
                if (status == 200) {
                    applied++;
                    assertEquals(e.getKey() + " acknowledged: " + source, 1, source.get(e.getKey()));
                } else {
                    assertTrue(e.getKey() + " applied, or refused with a status a client retries: " + status, status == CONFLICT || status == 503);
                }
            }
            assertTrue("updates were applied while the primary moved: " + statuses, applied > 0);
        } finally {
            moving.set(false);
            pool.shutdownNow();
        }
    }

    /**
     * An update whose shard has no active primary, through either node, is
     * answered unavailable once the timeout the client gave it passes, alone
     * or inside a bulk, rather than after the default timeout.
     */
    public void testAnUpdateWithNoActivePrimaryIsAnsweredWithinItsTimeout() throws Exception {
        String index = "multi_update_unassigned";
        Request create = new Request("PUT", "/" + index);
        create.addParameter("wait_for_active_shards", "0");
        create.setJsonEntity(
            "{\"settings\":{\"index.infino.enabled\":true,\"number_of_shards\":1,\"number_of_replicas\":0,"
                + "\"index.routing.allocation.require._name\":\"no_such_node\"}}"
        );
        client().performRequest(create);
        for (int node = 0; node < NODES; node++) {
            Request update = updateOf(index, "x", "{\"a\":1}", 0);
            update.addParameter("timeout", UNASSIGNED_TIMEOUT);
            long start = System.nanoTime();
            assertEquals("node " + node + " answers the update unavailable", 503, status(node, update));
            long took = TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - start);
            assertTrue("node " + node + " answered the update within its timeout, in " + took + "s", took < UNASSIGNED_ANSWER_SECONDS);

            Request bulk = new Request("POST", "/_bulk");
            bulk.addParameter("timeout", UNASSIGNED_TIMEOUT);
            bulk.setJsonEntity("{\"update\":{\"_index\":\"" + index + "\",\"_id\":\"x\"}}\n{\"doc\":{\"a\":1}}\n");
            start = System.nanoTime();
            Map<String, Object> answer = on(node, bulk);
            took = TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - start);
            Map<?, ?> item = (Map<?, ?>) ((Map<?, ?>) ((List<?>) answer.get("items")).get(0)).get("update");
            assertEquals("node " + node + " answers the bulk's update unavailable: " + answer, 503, item.get("status"));
            assertTrue("node " + node + " answered the bulk within its timeout, in " + took + "s", took < UNASSIGNED_ANSWER_SECONDS);
        }
    }

    /**
     * Updates of one fresh document sent through both nodes at once, each
     * setting a field of its own and allowed to run again {@code retries}
     * times on a conflict; each field's status.
     */
    private Map<String, Integer> raceUpdates(String index, int retries) throws Exception {
        Request put = new Request("PUT", "/" + index + "/_doc/x");
        put.setJsonEntity("{\"start\":1}");
        on(0, put);
        ExecutorService pool = Executors.newFixedThreadPool(NODES * UPDATERS_PER_NODE);
        List<Future<Map<String, Integer>>> updaters = new ArrayList<>();
        try {
            for (int node = 0; node < NODES; node++) {
                for (int t = 0; t < UPDATERS_PER_NODE; t++) {
                    int n = node;
                    String prefix = "n" + node + "_" + t + "_";
                    updaters.add(pool.submit(() -> {
                        Map<String, Integer> statuses = new TreeMap<>();
                        for (int i = 0; i < UPDATES_PER_UPDATER; i++) {
                            String field = prefix + i;
                            statuses.put(field, status(n, updateOf(index, "x", "{\"" + field + "\":1}", retries)));
                        }
                        return statuses;
                    }));
                }
            }
            Map<String, Integer> statuses = new TreeMap<>();
            for (Future<Map<String, Integer>> f : updaters) {
                statuses.putAll(f.get());
            }
            return statuses;
        } finally {
            pool.shutdownNow();
        }
    }

    /** An update of {@code id} merging {@code doc}, which may run again {@code retries} times on a conflict. */
    private static Request updateOf(String index, String id, String doc, int retries) {
        Request update = new Request("POST", "/" + index + "/_update/" + id);
        if (retries > 0) {
            update.addParameter("retry_on_conflict", Integer.toString(retries));
        }
        update.setJsonEntity("{\"doc\":" + doc + "}");
        return update;
    }

    /** {@code request} conditioned on the document standing at {@code seq} under {@code term}. */
    private static Request conditioned(Request request, long seq, long term) {
        request.addParameter("if_seq_no", Long.toString(seq));
        request.addParameter("if_primary_term", Long.toString(term));
        return request;
    }

    /** The status node {@code node} answers {@code request} with. */
    private int status(int node, Request request) throws IOException {
        try {
            return performOn(node, request).getStatusLine().getStatusCode();
        } catch (ResponseException e) {
            return e.getResponse().getStatusLine().getStatusCode();
        }
    }

    /** {@code id}'s source once a refresh lets the table take every write and each node reads the same. */
    private Map<?, ?> sourceAfterRefresh(String index, String id) throws Exception {
        client().performRequest(new Request("POST", "/" + index + "/_refresh"));
        AtomicReference<Map<?, ?>> source = new AtomicReference<>();
        assertBusy(() -> {
            Map<?, ?> first = sourceOn(0, index, id);
            Map<?, ?> second = sourceOn(1, index, id);
            assertEquals("both nodes read the same document", first, second);
            source.set(first);
        }, STALENESS_WAIT_SECONDS, TimeUnit.SECONDS);
        return source.get();
    }

    /**
     * {@code id}'s source as node {@code node} reads it. A node that does
     * not find it yet fails the assertion rather than the test, so a wait
     * for the nodes to agree ({@link #sourceAfterRefresh}) tries again.
     */
    private Map<?, ?> sourceOn(int node, String index, String id) throws IOException {
        try {
            return (Map<?, ?>) on(node, new Request("GET", "/" + index + "/_doc/" + id)).get("_source");
        } catch (ResponseException e) {
            if (e.getResponse().getStatusLine().getStatusCode() == 404) {
                throw new AssertionError("node " + node + " does not find " + id + " yet", e);
            }
            throw e;
        }
    }

    /** The node, by its number here, that holds {@code index}'s primary. */
    @SuppressWarnings("unchecked")
    private int primaryNode(String index) throws IOException {
        Request shards = new Request("GET", "/_cat/shards/" + index);
        shards.addParameter("format", "json");
        shards.addParameter("h", "prirep,node,state");
        List<Object> rows = entityAsList(client().performRequest(shards));
        for (Object row : rows) {
            Map<String, Object> shard = (Map<String, Object>) row;
            if ("p".equals(shard.get("prirep")) && "STARTED".equals(shard.get("state"))) {
                int node = nodeNames.indexOf(String.valueOf(shard.get("node")));
                assertTrue("the primary's node is one of " + nodeNames + ": " + shard, node >= 0);
                return node;
            }
        }
        throw new AssertionError("no started primary for " + index + ": " + rows);
    }

    /** The exact count of {@code index}'s documents, asked of node {@code node} as a request its cache keeps. */
    @SuppressWarnings("unchecked")
    private long total(int node, String index) throws IOException {
        Request search = new Request("POST", "/" + index + "/_search");
        search.addParameter("request_cache", "true");
        search.setJsonEntity("{\"size\":0,\"track_total_hits\":true}");
        Map<String, Object> hits = (Map<String, Object>) on(node, search).get("hits");
        return ((Number) ((Map<String, Object>) hits.get("total")).get("value")).longValue();
    }

    private void createIndex(String index) throws IOException {
        createIndex(index, "");
    }

    /** An Infino-backed index of one shard and no replica, with {@code settings} (a JSON fragment, leading comma) added. */
    private void createIndex(String index, String settings) throws IOException {
        Request create = new Request("PUT", "/" + index);
        create.setJsonEntity("{\"settings\":{\"index.infino.enabled\":true,\"number_of_shards\":1,\"number_of_replicas\":0" + settings + "}}");
        client().performRequest(create);
        Request health = new Request("GET", "/_cluster/health/" + index);
        health.addParameter("wait_for_status", "green");
        client().performRequest(health);
    }

    /** {@code request} sent to node {@code node} of the cluster, its answer as a map. */
    private Map<String, Object> on(int node, Request request) throws IOException {
        return entityAsMap(performOn(node, request));
    }

    /**
     * {@code request} sent to node {@code node} alone, as the admin user: the
     * cluster's nodes taken in the order of their addresses, so each number
     * names the same node throughout a test.
     */
    private Response performOn(int node, Request request) throws IOException {
        RequestOptions.Builder options = request.getOptions().toBuilder();
        options.addHeader("Authorization", adminAuthorization());
        request.setOptions(options);
        assertEquals("a client per node", NODES, nodeClients.size());
        return nodeClients.get(node).performRequest(request);
    }

    /**
     * A client per node of the cluster, ordered by port. The test client
     * lists every address a node listens on (IPv4 and IPv6 alike); the one a
     * node publishes picks a single address per node.
     */
    @SuppressWarnings("unchecked")
    @Before
    public void openNodeClients() throws IOException {
        Map<String, Object> info = entityAsMap(client().performRequest(new Request("GET", "/_nodes/http")));
        Map<Integer, String> published = new HashMap<>();
        for (Object node : ((Map<String, Object>) info.get("nodes")).values()) {
            Map<String, Object> described = (Map<String, Object>) node;
            String address = String.valueOf(((Map<String, Object>) described.get("http")).get("publish_address"));
            published.put(Integer.parseInt(address.substring(address.lastIndexOf(':') + 1)), String.valueOf(described.get("name")));
        }
        Map<Integer, Node> byPort = new TreeMap<>();
        for (Node n : client().getNodes()) {
            if (published.containsKey(n.getHost().getPort())) {
                byPort.putIfAbsent(n.getHost().getPort(), n);
            }
        }
        for (Map.Entry<Integer, Node> n : byPort.entrySet()) {
            nodeClients.add(RestClient.builder(n.getValue()).build());
            nodeNames.add(published.get(n.getKey()));
        }
    }

    @After
    public void closeNodeClients() throws IOException {
        for (RestClient c : nodeClients) {
            c.close();
        }
        nodeClients.clear();
        nodeNames.clear();
    }
}
