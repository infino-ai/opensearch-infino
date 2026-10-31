/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import org.junit.After;
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
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

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
    /** The status of a write refused for a conflict. */
    private static final int CONFLICT = 409;

    /** A client per node, each reaching that node alone. */
    private final List<RestClient> nodeClients = new ArrayList<>();

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

    private void createIndex(String index) throws IOException {
        assertEquals("the client reaches every node", NODES, getClusterHosts().size());
        Request create = new Request("PUT", "/" + index);
        create.setJsonEntity("{\"settings\":{\"index.infino.enabled\":true,\"number_of_shards\":1,\"number_of_replicas\":0}}");
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
        if (nodeClients.isEmpty()) {
            List<Node> nodes = new ArrayList<>(client().getNodes());
            nodes.sort(Comparator.comparing(n -> n.getHost().toString()));
            for (Node n : nodes) {
                nodeClients.add(RestClient.builder(n).build());
            }
        }
        RequestOptions.Builder options = request.getOptions().toBuilder();
        options.addHeader("Authorization", adminAuthorization());
        request.setOptions(options);
        return nodeClients.get(node % nodeClients.size()).performRequest(request);
    }

    @After
    public void closeNodeClients() throws IOException {
        for (RestClient c : nodeClients) {
            c.close();
        }
        nodeClients.clear();
    }
}
