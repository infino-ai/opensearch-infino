/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import org.junit.After;
import org.opensearch.Version;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.EmptyClusterInfoService;
import org.opensearch.cluster.OpenSearchAllocationTestCase;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.IndexShardRoutingTable;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.allocation.AllocationService;
import org.opensearch.cluster.routing.allocation.allocator.BalancedShardsAllocator;
import org.opensearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.opensearch.cluster.routing.allocation.decider.Decision;
import org.opensearch.cluster.routing.allocation.decider.SameShardAllocationDecider;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.infino.InfinoStorageIdentity.Seen;
import org.opensearch.test.gateway.TestGatewayAllocator;

import java.util.List;

/** Where a shard of an Infino-backed index may go: only to a node that sees the cluster's storage. */
public class InfinoStorageDeciderTests extends OpenSearchAllocationTestCase {

    private static final String TOKEN = "cluster-token";
    private static final Seen SHARED = new Seen(TOKEN, "s3://bucket/prefix");
    private static final Seen OWN_DISK = new Seen("other-token", "file:///data/infino");

    @After
    public void forget() {
        InfinoStorageIdentity.reset();
    }

    /** A node that has not reported, or a cluster with no token yet, gets no new copy, but keeps one it holds; a different token neither. */
    public void testRefusals() {
        assertNull(InfinoStorageDecider.refusal(TOKEN, SHARED, false));
        assertNull(InfinoStorageDecider.refusal(TOKEN, SHARED, true));
        assertEquals(
            "[infino] this node does not see the cluster's Infino storage: expected token cluster-token, node has other-token at file:///data/infino",
            InfinoStorageDecider.refusal(TOKEN, OWN_DISK, false)
        );
        assertNotNull("a copy on such a node may not stay", InfinoStorageDecider.refusal(TOKEN, OWN_DISK, true));
        assertNotNull(InfinoStorageDecider.refusal(TOKEN, null, false));
        assertNull("the manager just changed and the node has not reported again", InfinoStorageDecider.refusal(TOKEN, null, true));
        assertNotNull(InfinoStorageDecider.refusal("", SHARED, false));
        assertNull(InfinoStorageDecider.refusal("", SHARED, true));
    }

    /** Two nodes, one on its own disk: the Infino index's replica stays unassigned, the other index's is allocated there. */
    public void testTheReplicaOfAnInfinoIndexStaysOffANodeOnOtherStorage() {
        InfinoStorageIdentity.assumeReported("shared", SHARED);
        InfinoStorageIdentity.assumeReported("own", OWN_DISK);
        AllocationService allocation = allocationService();
        ClusterState state = allocate(allocation, initialState(true));

        IndexShardRoutingTable infino = state.routingTable().index("infino").shard(0);
        assertTrue(infino.primaryShard().active());
        assertEquals("shared", infino.primaryShard().currentNodeId());
        assertTrue("the copy that would read other storage is not allocated", infino.replicaShards().get(0).unassigned());

        IndexShardRoutingTable lucene = state.routingTable().index("lucene").shard(0);
        assertTrue(lucene.primaryShard().active());
        assertTrue(lucene.replicaShards().get(0).active());
    }

    /** When both nodes see the storage, every copy is allocated. */
    public void testEveryCopyIsAllocatedOnSharedStorage() {
        InfinoStorageIdentity.assumeReported("shared", SHARED);
        InfinoStorageIdentity.assumeReported("own", SHARED);
        ClusterState state = allocate(allocationService(), initialState(true));
        IndexShardRoutingTable infino = state.routingTable().index("infino").shard(0);
        assertTrue(infino.primaryShard().active());
        assertTrue(infino.replicaShards().get(0).active());
    }

    /** With no node that sees the storage, the index's primary is not allocated at all: the index is red, never served from other storage. */
    public void testNoPrimaryWhereNoNodeSeesTheStorage() {
        InfinoStorageIdentity.assumeReported("shared", OWN_DISK);
        InfinoStorageIdentity.assumeReported("own", OWN_DISK);
        ClusterState state = allocate(allocationService(), initialState(true));
        assertTrue(state.routingTable().index("infino").shard(0).primaryShard().unassigned());
    }

    /** A copy already on a node that turns out to see other storage may not remain there. */
    public void testACopyOnANodeThatSeesOtherStorageMayNotRemain() {
        InfinoStorageIdentity.assumeReported("shared", SHARED);
        InfinoStorageIdentity.assumeReported("own", SHARED);
        AllocationService allocation = allocationService();
        ClusterState state = allocate(allocation, initialState(false));
        ShardRouting replica = state.routingTable().index("infino").shard(0).replicaShards().get(0);
        assertTrue(replica.active());

        InfinoStorageIdentity.assumeReported(replica.currentNodeId(), OWN_DISK);
        InfinoStorageDecider decider = new InfinoStorageDecider();
        assertEquals(
            Decision.Type.NO,
            decider.canRemain(replica, state.getRoutingNodes().node(replica.currentNodeId()), newRoutingAllocation(deciders(), state)).type()
        );
    }

    private AllocationDeciders deciders() {
        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        return new AllocationDeciders(List.of(new SameShardAllocationDecider(Settings.EMPTY, clusterSettings), new InfinoStorageDecider()));
    }

    private AllocationService allocationService() {
        return new MockAllocationService(
            deciders(),
            new TestGatewayAllocator(),
            new BalancedShardsAllocator(Settings.EMPTY),
            EmptyClusterInfoService.INSTANCE,
            SNAPSHOT_INFO_SERVICE_WITH_NO_SHARD_SIZES
        );
    }

    /** An Infino index and a Lucene one, each of one shard and one replica, on two nodes, the cluster's token recorded. */
    private ClusterState initialState(boolean withLucene) {
        Metadata.Builder metadata = Metadata.builder()
            .persistentSettings(Settings.builder().put(InfinoStorageIdentity.CLUSTER_STORAGE_ID.getKey(), TOKEN).build())
            .put(index("infino", true));
        if (withLucene) {
            metadata.put(index("lucene", false));
        }
        Metadata built = metadata.build();
        RoutingTable.Builder routing = RoutingTable.builder().addAsNew(built.index("infino"));
        if (withLucene) {
            routing.addAsNew(built.index("lucene"));
        }
        return ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
            .metadata(built)
            .routingTable(routing.build())
            .nodes(DiscoveryNodes.builder().add(newNode("shared")).add(newNode("own")))
            .build();
    }

    private static IndexMetadata.Builder index(String name, boolean infino) {
        return IndexMetadata.builder(name)
            .settings(settings(Version.CURRENT).put(InfinoPlugin.INFINO_ENABLED.getKey(), infino))
            .numberOfShards(1)
            .numberOfReplicas(1);
    }

    private ClusterState allocate(AllocationService allocation, ClusterState state) {
        return applyStartedShardsUntilNoChange(allocation.reroute(state, "test"), allocation);
    }
}
