/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.routing.RoutingNode;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.allocation.RoutingAllocation;
import org.opensearch.cluster.routing.allocation.decider.AllocationDecider;
import org.opensearch.cluster.routing.allocation.decider.Decision;

/**
 * Allocates a shard of an Infino-backed index (either mode) only to a node
 * that sees the cluster's Infino storage ({@link InfinoStorageIdentity}):
 * every copy reads the one table, so a copy on a node that sees other
 * storage would answer with no documents. Such a copy stays unassigned, and
 * {@code _cluster/allocation/explain} says why. A copy already on such a
 * node may not remain there; one on a node that has not reported yet (the
 * cluster manager just changed) stays where it is.
 */
final class InfinoStorageDecider extends AllocationDecider {

    static final String NAME = "infino_storage";

    @Override
    public Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        return decide(shardRouting, node, allocation, false);
    }

    @Override
    public Decision canRemain(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        return decide(shardRouting, node, allocation, true);
    }

    /** A primary whose last copy is on a node that does not see the storage is not forced there either: it would serve that node's. */
    @Override
    public Decision canForceAllocatePrimary(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        return decide(shardRouting, node, allocation, false);
    }

    private Decision decide(ShardRouting shard, RoutingNode node, RoutingAllocation allocation, boolean remain) {
        IndexMetadata index = allocation.metadata().index(shard.index());
        if (index == null || !index.getSettings().getAsBoolean(InfinoPlugin.INFINO_ENABLED.getKey(), false)) {
            return allocation.decision(Decision.YES, NAME, "not an Infino-backed index");
        }
        String cluster = InfinoStorageIdentity.CLUSTER_STORAGE_ID.get(allocation.metadata().settings());
        InfinoStorageIdentity.Seen seen = InfinoStorageIdentity.reported(node.nodeId());
        if (!refuses(cluster, seen, remain)) {
            return allocation.decision(Decision.YES, NAME, "this node sees the cluster's Infino storage");
        }
        // The reason is built only for an explanation: a reroute asks every shard of every node.
        return allocation.debugDecision() ? allocation.decision(Decision.NO, NAME, "%s", refusal(cluster, seen, remain)) : Decision.NO;
    }

    /** Whether {@link #refusal} has a reason, without building it. */
    static boolean refuses(String cluster, InfinoStorageIdentity.Seen seen, boolean remain) {
        if (seen == null || cluster.isEmpty()) {
            return !remain;
        }
        return InfinoStorageIdentity.mismatched(cluster, seen);
    }

    /**
     * Why a shard may not be allocated to (or, with {@code remain}, stay on) a
     * node that reported {@code seen} (null for nothing yet), in a cluster
     * whose token is {@code cluster} (empty for none yet); null when it may.
     */
    static String refusal(String cluster, InfinoStorageIdentity.Seen seen, boolean remain) {
        if (seen == null) {
            return remain
                ? null
                : "this node has not reported which Infino storage it sees yet; it does once it has read its storage (see its log),"
                    + " unless it runs a plugin version without this check, as a node not yet upgraded in a rolling upgrade does";
        }
        if (cluster.isEmpty()) {
            return remain
                ? null
                : "the cluster has not recorded its Infino storage yet; it does once more than half of the data nodes, or every one,"
                    + " has reported the storage it sees (see their logs)";
        }
        return InfinoStorageIdentity.mismatched(cluster, seen) ? InfinoStorageIdentity.notShared(cluster, seen) : null;
    }
}
