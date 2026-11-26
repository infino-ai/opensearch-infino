/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import org.junit.After;
import org.opensearch.Version;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodeRole;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.infino.InfinoStorageIdentity.Seen;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;
import java.util.Map;
import java.util.Set;

/** How a node reads which storage it sees, and what it refuses when that is not the cluster's. */
public class InfinoStorageIdentityTests extends OpenSearchTestCase {

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

    /** Memory is one node's own: its token names the node. A hosted account's names the service and database, never the key or a password. */
    public void testMemoryIsTheNodesOwnAndAHostedAccountIsShared() {
        Seen memory = InfinoStorageIdentity.read(new InfinoNative.Target("memory://", null, false, "memory://"), "node-a");
        assertEquals("memory node-a", memory.token());
        assertNotEquals(memory.token(), InfinoStorageIdentity.read(new InfinoNative.Target("memory://", null, false, "memory://"), "node-b").token());

        InfinoNative.Target hosted = InfinoNative.Target.hosted("https://user:secret@platform.example/", "db", "the-key");
        Seen seen = InfinoStorageIdentity.read(hosted, "node-a");
        assertEquals("hosted https://***@platform.example/db", seen.token());
        assertEquals("the same account on another node", seen.token(), InfinoStorageIdentity.read(hosted, "node-b").token());
        assertFalse(seen.token().contains("secret") || seen.where().contains("secret"));
        assertFalse(seen.token().contains("the-key") || seen.where().contains("the-key"));
        assertNotEquals(seen.token(), InfinoStorageIdentity.read(InfinoNative.Target.hosted("https://platform.example", "other", "the-key"), "a").token());
    }

    /** A node whose storage is not the cluster's refuses every table operation with 503 and the reason; one that is, or does not know yet, does not. */
    public void testANodeThatDoesNotSeeTheStorageRefusesTableOperations() {
        Seen mine = new Seen("mine", "file:///data/infino");
        InfinoStorageIdentity.assume(mine, "theirs");
        InfinoNative.InfinoException refused = expectThrows(InfinoNative.InfinoException.class, InfinoStorageIdentity::refuseIfUnshared);
        assertEquals(RestStatus.SERVICE_UNAVAILABLE, refused.status());
        assertEquals(
            "[infino] this node does not see the cluster's Infino storage: expected token theirs, node has mine at file:///data/infino",
            refused.getMessage()
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

    /** With no token recorded, the cluster takes the one most data nodes report; a node that holds no data has no say. */
    public void testTheClusterTakesTheTokenMostDataNodesReport() {
        DiscoveryNodes nodes = DiscoveryNodes.builder()
            .add(node("a", true))
            .add(node("b", true))
            .add(node("c", true))
            .add(node("manager", false))
            .build();
        Seen x = new Seen("x", "s3://bucket");
        Seen y = new Seen("y", "file:///own");
        assertEquals("x", InfinoStorageIdentity.chosen(Map.of("a", x, "b", x, "c", y, "manager", y), nodes));
        assertEquals("a tie goes to the smaller token", "x", InfinoStorageIdentity.chosen(Map.of("a", y, "b", x), nodes));
        assertNull("only a node with no data reported", InfinoStorageIdentity.chosen(Map.of("manager", y), nodes));
        assertNull("a node no longer in the cluster", InfinoStorageIdentity.chosen(Map.of("gone", x), nodes));
    }

    private static DiscoveryNode node(String id, boolean data) {
        Set<DiscoveryNodeRole> roles = data
            ? Set.of(DiscoveryNodeRole.CLUSTER_MANAGER_ROLE, DiscoveryNodeRole.DATA_ROLE)
            : Set.of(DiscoveryNodeRole.CLUSTER_MANAGER_ROLE);
        return new DiscoveryNode(id, buildNewFakeTransportAddress(), Map.of(), roles, Version.CURRENT);
    }
}
