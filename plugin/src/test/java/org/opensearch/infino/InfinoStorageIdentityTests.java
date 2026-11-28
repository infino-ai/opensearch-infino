/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import com.sun.jna.Memory;
import com.sun.jna.Pointer;

import org.junit.After;
import org.opensearch.Version;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodeRole;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.infino.InfinoStorageIdentity.Seen;
import org.opensearch.test.ClusterServiceUtils;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;

import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.opensearch.infino.InfinoStorageIdentity.CLUSTER_STORAGE_ID;

/** How a node reads which storage it sees, and what it refuses when that is not the cluster's. */
public class InfinoStorageIdentityTests extends OpenSearchTestCase {

    /** What the bridge answers for a table read that failed for another reason than a missing table. */
    private static final String READ_FAILED = "{\"error\":\"the read was throttled\",\"status\":503}";

    @After
    public void forget() {
        InfinoStorageIdentity.reset();
    }

    /** The token is the first token column in the table's order: a column a racing node added later does not move it. */
    public void testTheTokenIsTheMarkersFirstTokenColumn() {
        assertEquals("abc", InfinoStorageIdentity.tokenOf(List.of("_id", "storage_abc", "storage_def")));
        assertNull(InfinoStorageIdentity.tokenOf(List.of("_id")));
        assertNull("a bare prefix holds no token", InfinoStorageIdentity.tokenOf(List.of("_id", "storage_")));
        assertNull(InfinoStorageIdentity.tokenOf(List.of()));
    }

    /** The marker on the node's storage is created once, and every later read finds the same token, a racer's column after it included. */
    public void testTheMarkerIsCreatedOnceAndKeepsItsToken() {
        String token = InfinoStorageIdentity.markerToken();
        assertFalse(token.isEmpty());
        assertEquals(token, InfinoStorageIdentity.markerToken());
        // A node that found the marker and added a column of its own, as two nodes creating it at once can.
        InfinoNative.ensureTable(InfinoStorageIdentity.MARKER_TABLE, InfinoStorageIdentity.markerMapping("later"));
        assertEquals(token, InfinoStorageIdentity.markerToken());
    }

    /**
     * Memory is one node's own: its token names the node. A hosted account's
     * token is its database's marker, as a node's own storage's is, never its
     * URL and database name, which two accounts can share; neither the key
     * nor a password is in what the node reports.
     */
    public void testMemoryIsTheNodesOwnAndAHostedAccountHoldsAMarker() {
        Seen memory = InfinoStorageIdentity.read(new InfinoNative.Target("memory://", null, false, "memory://"), "node-a");
        assertEquals("memory node-a", memory.token());
        assertNotEquals(memory.token(), InfinoStorageIdentity.read(new InfinoNative.Target("memory://", null, false, "memory://"), "node-b").token());

        // The marker is read through the connection the node has open, here the test's own storage.
        InfinoNative.Target hosted = InfinoNative.Target.hosted("https://user:secret@platform.example/", "db", "the-key");
        Seen seen = InfinoStorageIdentity.read(hosted, "node-a");
        assertEquals(InfinoStorageIdentity.markerToken(), seen.token());
        assertFalse(seen.token().contains("platform.example"));
        assertFalse(seen.token().contains("secret") || seen.where().contains("secret"));
        assertFalse(seen.token().contains("the-key") || seen.where().contains("the-key"));
    }

    /**
     * A marker read that fails (a throttled read, a service that did not
     * answer) is no missing marker: it throws, to be tried again, and the
     * marker is neither created nor given a column. A missing table has no
     * columns. A query's lookup of the columns reads both as none, as it did.
     */
    public void testAFailedMarkerReadIsNotAMissingMarker() {
        AtomicBoolean failing = new AtomicBoolean(true);
        AtomicInteger declared = new AtomicInteger();
        InfinoNative.useLib((InfinoLib) Proxy.newProxyInstance(InfinoLib.class.getClassLoader(), new Class<?>[] { InfinoLib.class }, (proxy, method, args) -> switch (method.getName()) {
            case "infino_open_with" -> new Memory(1);
            case "infino_columns" -> InfinoStorageIdentity.MARKER_TABLE.equals(args[1]) && failing.get() ? string(READ_FAILED) : string("");
            case "infino_ensure_table" -> {
                declared.incrementAndGet();
                yield string("{\"created\":true}");
            }
            default -> null; // the frees
        }));
        try {
            InfinoNative.InfinoException e = expectThrows(InfinoNative.InfinoException.class, InfinoStorageIdentity::markerToken);
            assertEquals(RestStatus.SERVICE_UNAVAILABLE, e.status());
            assertEquals("a failed read creates nothing", 0, declared.get());
            assertTrue(InfinoNative.columnNames("absent_table").isEmpty());
            assertTrue(InfinoNative.columns(InfinoStorageIdentity.MARKER_TABLE).isEmpty());
            // A missing marker is created.
            failing.set(false);
            expectThrows(IllegalStateException.class, InfinoStorageIdentity::markerToken);
            assertEquals(1, declared.get());
        } finally {
            InfinoNative.useLib(null);
        }
    }

    private static Pointer string(String s) {
        byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
        Memory m = new Memory(bytes.length + 1);
        m.write(0, bytes, 0, bytes.length);
        m.setByte(bytes.length, (byte) 0);
        return m;
    }

    /** A node whose storage is not the cluster's refuses every table operation with 503 and the reason; one that is, or does not know yet, does not. */
    public void testANodeThatDoesNotSeeTheStorageRefusesTableOperations() {
        Seen mine = new Seen("mine", "file:///data/infino");
        InfinoStorageIdentity.assume(mine, "theirs");
        InfinoNative.InfinoException refused = expectThrows(InfinoNative.InfinoException.class, InfinoStorageIdentity::refuseIfUnshared);
        assertEquals(RestStatus.SERVICE_UNAVAILABLE, refused.status());
        // The caller learns that the storage is not shared, never where either storage is.
        assertEquals(InfinoStorageIdentity.NOT_SHARED_TO_CALLER, refused.getMessage());
        assertFalse(refused.getMessage().contains("file://") || refused.getMessage().contains("theirs") || refused.getMessage().contains("mine"));
        // The log and the allocation explanation say where.
        assertEquals(
            "[infino] this node does not see the cluster's Infino storage: expected token theirs, node has mine at file:///data/infino",
            InfinoStorageIdentity.notShared("theirs", mine)
        );
        // Any table operation, through the connection.
        expectThrows(InfinoNative.InfinoException.class, () -> InfinoNative.columns("anything"));

        InfinoStorageIdentity.assume(mine, "mine");
        InfinoStorageIdentity.refuseIfUnshared();
        InfinoStorageIdentity.assume(mine, "");
        InfinoStorageIdentity.refuseIfUnshared();
        InfinoStorageIdentity.assume(null, "theirs");
        InfinoStorageIdentity.refuseIfUnshared();
    }

    /**
     * With no token recorded, the cluster takes the one more than half of
     * all its data nodes report, not the first to arrive: a node on memory or
     * on a directory of its own answers before one reading a bucket. A node
     * that holds no data has no say.
     */
    public void testWithNoTokenTheClusterTakesOneMoreThanHalfOfItsDataNodesReport() {
        DiscoveryNodes nodes = threeDataNodesAndAManager();
        Seen x = new Seen("x", "s3://bucket");
        Seen y = new Seen("y", "file:///own");
        Seen memory = new Seen("memory c", "memory://");
        assertNull("the fastest node alone", InfinoStorageIdentity.chosen("", Map.of("c", memory), Set.of(), nodes));
        assertNull("one of three", InfinoStorageIdentity.chosen("", Map.of("c", memory, "a", x), Set.of(), nodes));
        assertEquals("two of three", "x", InfinoStorageIdentity.chosen("", Map.of("c", memory, "a", x, "b", x), Set.of(), nodes));
        assertEquals("x", InfinoStorageIdentity.chosen("", Map.of("a", x, "b", x, "c", y, "manager", y), Set.of(), nodes));
        assertNull("no majority, one yet to report", InfinoStorageIdentity.chosen("", Map.of("a", x, "b", y), Set.of(), nodes));
        // Every data node reported and none has a majority: the one most report, a tie to the smaller, never one node's memory.
        assertEquals("x", InfinoStorageIdentity.chosen("", Map.of("a", y, "b", x, "c", memory), Set.of(), nodes));
        assertEquals("y", InfinoStorageIdentity.chosen("", Map.of("a", y, "b", new Seen("memory b", "memory://"), "c", memory), Set.of(), nodes));
        assertNull("only a node with no data reported", InfinoStorageIdentity.chosen("", Map.of("manager", y), Set.of(), nodes));
        assertNull("a node no longer in the cluster", InfinoStorageIdentity.chosen("", Map.of("gone", x, "a", x), Set.of(), nodes));
        // A node's memory is the cluster's storage only when that node is the whole cluster.
        DiscoveryNodes alone = DiscoveryNodes.builder().add(node("a", true)).build();
        assertEquals("memory a", InfinoStorageIdentity.chosen("", Map.of("a", new Seen("memory a", "memory://")), Set.of(), alone));
        DiscoveryNodes withManager = DiscoveryNodes.builder().add(node("a", true)).add(node("manager", false)).build();
        assertNull(InfinoStorageIdentity.chosen("", Map.of("a", new Seen("memory a", "memory://")), Set.of(), withManager));
    }

    /**
     * Once recorded, the token is replaced only when every data node reports
     * the same other one and an administrator switched at least one of them:
     * one node's switch never moves the shards of the nodes that still see
     * the cluster's storage, and a cluster restarted on other storage with no
     * switch keeps refusing rather than serving it empty.
     */
    public void testARecordedTokenIsReplacedOnlyWhenEveryDataNodeIsSwitched() {
        DiscoveryNodes nodes = threeDataNodesAndAManager();
        Seen x = new Seen("x", "s3://bucket");
        Seen y = new Seen("y", "hosted");
        Seen memory = new Seen("memory a", "memory://");
        assertNull("one node switched", InfinoStorageIdentity.chosen("x", Map.of("a", y, "b", x, "c", x), Set.of("a"), nodes));
        assertNull("one node switched to memory", InfinoStorageIdentity.chosen("x", Map.of("a", memory, "b", x, "c", x), Set.of("a"), nodes));
        assertNull("two of three switched", InfinoStorageIdentity.chosen("x", Map.of("a", y, "b", y, "c", x), Set.of("a", "b"), nodes));
        assertNull("one has not reported", InfinoStorageIdentity.chosen("x", Map.of("a", y, "b", y), Set.of("a", "b"), nodes));
        assertNull("every node on it, none switched", InfinoStorageIdentity.chosen("x", Map.of("a", y, "b", y, "c", y), Set.of(), nodes));
        assertEquals("every node on it, one switched", "y", InfinoStorageIdentity.chosen("x", Map.of("a", y, "b", y, "c", y), Set.of("a"), nodes));
        assertNull("the token kept already", InfinoStorageIdentity.chosen("x", Map.of("a", x, "b", x, "c", x), Set.of("a"), nodes));
        DiscoveryNodes alone = DiscoveryNodes.builder().add(node("a", true)).build();
        assertEquals("the only data node switched", "y", InfinoStorageIdentity.chosen("x", Map.of("a", y), Set.of("a"), alone));
    }

    /**
     * The cluster manager, through its cluster service: a switch on one of
     * three data nodes keeps the cluster's token, and the token moves once
     * the last data node reports the switched storage. With no token yet, the
     * first report, from memory, chooses nothing; two bucket reports do.
     */
    public void testTheManagerKeepsTheTokenUntilEveryDataNodeIsSwitched() throws Exception {
        ThreadPool threadPool = new TestThreadPool(getTestName());
        ClusterService clusterService = ClusterServiceUtils.createClusterService(threadPool);
        try {
            clusterService.setRerouteService((reason, priority, listener) -> listener.onResponse(null));
            InfinoServices.bind(clusterService, threadPool);
            String local = clusterService.localNode().getId();
            ClusterState state = clusterService.state();
            ClusterServiceUtils.setState(
                clusterService,
                ClusterState.builder(state)
                    .nodes(DiscoveryNodes.builder(state.nodes()).add(node("a", true)).add(node("b", true)))
                    .metadata(Metadata.builder(state.metadata()).persistentSettings(Settings.builder().put(CLUSTER_STORAGE_ID.getKey(), "x").build()))
            );
            Seen switched = new Seen("y", "hosted database");
            InfinoStorageIdentity.received(local, new Seen("x", "s3://bucket"), false);
            InfinoStorageIdentity.received("b", new Seen("x", "s3://bucket"), false);
            InfinoStorageIdentity.received("a", switched, true);
            assertEquals("one node's switch", "x", storageId(clusterService));
            InfinoStorageIdentity.received("b", switched, true);
            assertEquals("two of three", "x", storageId(clusterService));
            InfinoStorageIdentity.received(local, switched, false);
            assertBusy(() -> assertEquals("every data node", "y", storageId(clusterService)), 10, TimeUnit.SECONDS);

            InfinoStorageIdentity.reset();
            ClusterServiceUtils.setState(
                clusterService,
                ClusterState.builder(clusterService.state()).metadata(Metadata.builder(clusterService.state().metadata()).persistentSettings(Settings.EMPTY))
            );
            InfinoStorageIdentity.received("b", new Seen("memory b", "memory://"), false);
            InfinoStorageIdentity.received("a", new Seen("bucket", "s3://bucket"), false);
            assertEquals("no token from one of three", "", storageId(clusterService));
            InfinoStorageIdentity.received(local, new Seen("bucket", "s3://bucket"), false);
            assertBusy(() -> assertEquals("bucket", storageId(clusterService)), 10, TimeUnit.SECONDS);
        } finally {
            InfinoServices.bind(null, null);
            clusterService.close();
            terminate(threadPool);
        }
    }

    private static String storageId(ClusterService clusterService) {
        return CLUSTER_STORAGE_ID.get(clusterService.state().metadata().settings());
    }

    private static DiscoveryNodes threeDataNodesAndAManager() {
        return DiscoveryNodes.builder().add(node("a", true)).add(node("b", true)).add(node("c", true)).add(node("manager", false)).build();
    }

    private static DiscoveryNode node(String id, boolean data) {
        Set<DiscoveryNodeRole> roles = data
            ? Set.of(DiscoveryNodeRole.CLUSTER_MANAGER_ROLE, DiscoveryNodeRole.DATA_ROLE)
            : Set.of(DiscoveryNodeRole.CLUSTER_MANAGER_ROLE);
        return new DiscoveryNode(id, buildNewFakeTransportAddress(), Map.of(), roles, Version.CURRENT);
    }
}
