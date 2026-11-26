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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

/**
 * Two nodes and an index with a replica, run twice: on a cluster whose
 * nodes each keep their tables in a directory of their own
 * ({@code tests.infino.storage.shared=false}), and on one whose nodes share
 * one directory. Every copy of a shard reads one table, so a copy on a node
 * that sees other storage would answer with no documents and a 200. On
 * separate directories that copy stays unassigned, the cluster explains why,
 * and the node that sees other storage refuses what it would answer from it;
 * on one shared directory every copy is allocated and every node answers.
 */
public class InfinoUnsharedStorageIT extends InfinoRestTestCase {

    /** Whether the cluster's nodes share one directory. */
    private static final boolean SHARED = Boolean.parseBoolean(System.getProperty("tests.infino.storage.shared", "true"));
    private static final String ADMIN_BASIC = "admin:admin";
    private static final int NODES = 2;
    private static final int DOCS = 50;
    /** How long the cluster may take to allocate, or to decide not to. */
    private static final long ALLOCATION_SECONDS = 60;
    private static final int UNAVAILABLE = 503;
    private static final String NOT_SHARED = "does not see the cluster's Infino storage";

    /** A client per node, each reaching that node alone, and each node's name. */
    private final List<RestClient> nodeClients = new ArrayList<>();
    private final List<String> nodeNames = new ArrayList<>();

    /** The cluster's storage token is a persistent setting: cleared between tests, the cluster would choose it again. */
    @Override
    protected boolean preserveClusterSettings() {
        return true;
    }

    /**
     * An index of one shard and one replica: on separate directories the
     * replica is never allocated, the node with no copy counts every document
     * through the primary, and its get is refused rather than answered
     * missing. Before, the replica was allocated to the other node, read that
     * node's empty directory, and answered {@code _count?preference=_replica}
     * with 0 and a 200.
     */
    public void testAReplicaReadsTheClustersStorageOrIsNotAllocated() throws Exception {
        String index = "storage_check";
        call("PUT", "/" + index, "{\"settings\":{\"number_of_shards\":1,\"number_of_replicas\":1}}");
        int primary = awaitPrimary(index);
        if (SHARED) {
            // Every copy started before the write, so the replica takes part in
            // the write's refresh: what is checked here is which storage a copy
            // reads, not how fresh a copy that finishes recovering mid-write is.
            Request started = new Request("GET", "/_cluster/health/" + index);
            started.addParameter("wait_for_status", "green");
            started.addParameter("timeout", ALLOCATION_SECONDS + "s");
            assertEquals("every copy started: ", false, on(primary, started).get("timed_out"));
        }
        StringBuilder bulk = new StringBuilder();
        for (int i = 0; i < DOCS; i++) {
            bulk.append("{\"index\":{\"_id\":\"d").append(i).append("\"}}\n{\"n\":").append(i).append("}\n");
        }
        Request write = new Request("POST", "/" + index + "/_bulk");
        write.addParameter("refresh", "true");
        write.setJsonEntity(bulk.toString());
        Map<String, Object> written = on(primary, write);
        assertEquals("bulk errors: " + written, false, written.get("errors"));

        // Every copy the cluster will allocate has started, so the counts below read every copy there is.
        Request settled = new Request("GET", "/_cluster/health/" + index);
        settled.addParameter("wait_for_no_initializing_shards", "true");
        settled.addParameter("wait_for_no_relocating_shards", "true");
        settled.addParameter("wait_for_status", SHARED ? "green" : "yellow");
        settled.addParameter("timeout", ALLOCATION_SECONDS + "s");
        Map<String, Object> health = on(primary, settled);
        assertEquals("allocation settled: " + health, false, health.get("timed_out"));
        if (SHARED) {
            assertEquals("green", health.get("status"));
        }
        for (int node = 0; node < NODES; node++) {
            // A replica that answers holds every document; with none allocated the count fails, as stock fails it.
            Request replica = new Request("GET", "/" + index + "/_count");
            replica.addParameter("preference", "_replica");
            Map<String, Object> counted;
            try {
                counted = on(node, replica);
            } catch (ResponseException e) {
                assertFalse("on shared storage the replica answers: " + e.getMessage(), SHARED);
                assertEquals(UNAVAILABLE, e.getResponse().getStatusLine().getStatusCode());
                counted = null;
            }
            if (counted != null) {
                long failed = ((Number) shards(counted).get("failed")).longValue();
                assertTrue(
                    "a replica answered " + counted + " through " + nodeNames.get(node) + ", with a 200",
                    ((Number) counted.get("count")).longValue() == DOCS || (!SHARED && failed > 0)
                );
            }
            assertEquals("count through " + nodeNames.get(node), DOCS, ((Number) on(node, new Request("GET", "/" + index + "/_count")).get("count")).longValue());
        }
        Map<String, Object> settings = call("GET", "/_cluster/settings?flat_settings=true", null);
        assertNotNull("the cluster recorded its storage: " + settings, persistent(settings).get("infino.storage.id"));
        int other = 1 - primary;
        if (SHARED) {
            for (int node = 0; node < NODES; node++) {
                assertEquals(true, on(node, new Request("GET", "/" + index + "/_doc/d1")).get("found"));
            }
            return;
        }
        assertBusy(() -> {
            Map<String, Object> explained = call("POST", "/_cluster/allocation/explain", "{\"index\":\"" + index + "\",\"shard\":0,\"primary\":false}");
            assertEquals("unassigned", explained.get("current_state"));
            String reason = refusalOn(explained, nodeNames.get(other));
            assertNotNull("the storage decider refuses " + nodeNames.get(other) + ": " + explained, reason);
            assertTrue(reason, reason.contains(NOT_SHARED) && reason.contains("file://"));
        }, ALLOCATION_SECONDS, TimeUnit.SECONDS);
        assertEquals("yellow", call("GET", "/_cluster/health/" + index, null).get("status"));
        assertEquals(true, on(primary, new Request("GET", "/" + index + "/_doc/d1")).get("found"));
        ResponseException refused = expectThrows(ResponseException.class, () -> on(other, new Request("GET", "/" + index + "/_doc/d1")));
        assertEquals(UNAVAILABLE, refused.getResponse().getStatusLine().getStatusCode());
        assertTrue(refused.getMessage(), refused.getMessage().contains(NOT_SHARED));
    }

    /**
     * A proxy-mode index, answered from the table by whichever node takes the
     * request: on separate directories the node that sees other storage
     * refuses the count, where before it counted its own empty directory.
     */
    public void testAProxyModeReadIsNotAnsweredFromOtherStorage() throws Exception {
        String index = "storage_check_proxy";
        call("PUT", "/" + index, "{\"settings\":{\"number_of_shards\":1,\"number_of_replicas\":0,\"index.infino.mode\":\"proxy\"}}");
        int primary = awaitPrimary(index);
        Request write = new Request("POST", "/" + index + "/_bulk");
        write.addParameter("refresh", "true");
        write.setJsonEntity("{\"index\":{\"_id\":\"a\"}}\n{\"n\":1}\n{\"index\":{\"_id\":\"b\"}}\n{\"n\":2}\n");
        assertEquals(false, on(primary, write).get("errors"));
        assertEquals(2L, ((Number) on(primary, new Request("GET", "/" + index + "/_count")).get("count")).longValue());
        int other = 1 - primary;
        if (SHARED) {
            assertEquals(2L, ((Number) on(other, new Request("GET", "/" + index + "/_count")).get("count")).longValue());
            return;
        }
        ResponseException refused = expectThrows(ResponseException.class, () -> on(other, new Request("GET", "/" + index + "/_count")));
        assertEquals(UNAVAILABLE, refused.getResponse().getStatusLine().getStatusCode());
        assertTrue(refused.getMessage(), refused.getMessage().contains(NOT_SHARED));
    }

    /** The node holding {@code index}'s started primary, as an index into {@link #nodeClients}. */
    @SuppressWarnings("unchecked")
    private int awaitPrimary(String index) throws Exception {
        int[] found = new int[] { -1 };
        assertBusy(() -> {
            for (Object row : callList("GET", "/_cat/shards/" + index + "?format=json&h=prirep,state,node", null)) {
                Map<String, Object> shard = (Map<String, Object>) row;
                if ("p".equals(shard.get("prirep")) && "STARTED".equals(shard.get("state"))) {
                    found[0] = nodeNames.indexOf(String.valueOf(shard.get("node")));
                }
            }
            assertTrue("a started primary of " + index, found[0] >= 0);
        }, ALLOCATION_SECONDS, TimeUnit.SECONDS);
        return found[0];
    }

    /** The storage decider's explanation of its refusal on {@code node}, from an allocation explanation; null when it did not refuse. */
    @SuppressWarnings("unchecked")
    private static String refusalOn(Map<String, Object> explained, String node) {
        Object decisions = explained.get("node_allocation_decisions");
        if (!(decisions instanceof List<?> list)) {
            return null;
        }
        for (Object o : list) {
            Map<String, Object> decision = (Map<String, Object>) o;
            if (!node.equals(decision.get("node_name")) || !(decision.get("deciders") instanceof List<?> deciders)) {
                continue;
            }
            for (Object d : deciders) {
                Map<String, Object> decider = (Map<String, Object>) d;
                if ("infino_storage".equals(decider.get("decider")) && "NO".equals(decider.get("decision"))) {
                    return String.valueOf(decider.get("explanation"));
                }
            }
        }
        return null;
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> persistent(Map<String, Object> settings) {
        return (Map<String, Object>) settings.get("persistent");
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> shards(Map<String, Object> response) {
        return (Map<String, Object>) response.get("_shards");
    }

    /** {@code request} sent to node {@code node} alone, as the admin user, its answer read. */
    private Map<String, Object> on(int node, Request request) throws IOException {
        RequestOptions.Builder options = request.getOptions().toBuilder();
        options.addHeader("Authorization", "Basic " + Base64.getEncoder().encodeToString(ADMIN_BASIC.getBytes(StandardCharsets.UTF_8)));
        request.setOptions(options);
        assertEquals("a client per node", NODES, nodeClients.size());
        Response response = nodeClients.get(node).performRequest(request);
        return entityAsMap(response);
    }

    /** A client per node of the cluster, ordered by port, as each node publishes its address. */
    @SuppressWarnings("unchecked")
    @Before
    public void openNodeClients() throws IOException {
        Map<String, Object> info = call("GET", "/_nodes/http", null);
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
