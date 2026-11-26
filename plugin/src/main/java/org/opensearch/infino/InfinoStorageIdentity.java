/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.cluster.ClusterChangedEvent;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.ClusterStateUpdateTask;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.Priority;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.transport.TransportResponse;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportException;
import org.opensearch.transport.TransportRequest;
import org.opensearch.transport.TransportResponseHandler;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Whether every node sees the cluster's Infino storage.
 *
 * <p>Every copy of a shard reads one table, and every node reads tables for
 * the requests it coordinates (a get, a proxy-mode search), so every node
 * must see the same storage. The plugin cannot tell that from a URI: the
 * same {@code file:///data/infino} on two machines is two disks. So the
 * storage carries a token, and each node reads the token from the storage
 * it sees:
 *
 * <ul>
 * <li>a node's own storage ({@code file://}, {@code s3://}, {@code gs://},
 * {@code az://}) holds a marker table, {@link #MARKER_TABLE}, created by the
 * first node that finds none, with a random token;</li>
 * <li>a hosted account is shared by construction: its token is the service
 * URL and the database, which every node naming the same account names;</li>
 * <li>{@code memory://} lives in one node's process, so it is that node's
 * alone: its token names the node, and only a single-node cluster can use it.</li>
 * </ul>
 *
 * <p>Each node reports its token to the elected cluster manager. The first
 * data node to report fixes the cluster's token, kept as the persistent
 * cluster setting {@link #CLUSTER_STORAGE_ID}, so it survives a full
 * restart; a later report never overwrites it, except from a node whose
 * storage an administrator switched through the account route. A node whose
 * token differs (or that has not reported yet) gets no shard of an Infino
 * index ({@link InfinoStorageDecider}), refuses every table operation it
 * would otherwise answer from the wrong storage ({@link #refuseIfUnshared}),
 * and logs one ERROR saying what to fix. A copy that would answer with no
 * documents stays unassigned instead, and an index whose only copy is on
 * such a node goes red rather than answering short.
 */
final class InfinoStorageIdentity {

    private static final Logger log = LogManager.getLogger(InfinoStorageIdentity.class);

    /**
     * The token of the storage the cluster's Infino indices live on, set by
     * the cluster manager from the first data node's report. Cleared by an
     * administrator whose cluster moved to other storage on purpose; the
     * manager then takes the token most data nodes report.
     */
    static final Setting<String> CLUSTER_STORAGE_ID = Setting.simpleString(
        "infino.storage.id",
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * The marker table in a node's own storage. No index maps to it: an index
     * name never starts with {@code -}, and every table behind an index starts
     * with {@code os_} followed by the index name's own characters.
     */
    static final String MARKER_TABLE = "os_-storage";
    /**
     * What the marker's token column is named with, before the token. The
     * token is the name of the marker's first such column: creating a table
     * is the one write the engine makes conditional on there being none, and
     * a column named at creation is committed with the table itself. Of two
     * nodes creating the marker at once, one creates it and the other finds
     * it; a node that adds its own column to a marker it found adds it after
     * the first, which stays the token.
     */
    static final String TOKEN_COLUMN_PREFIX = "storage_";
    /** The transport action a node reports its token to the cluster manager with. */
    static final String REPORT_ACTION = "internal:infino/storage/report";
    /** How long a node waits before it reads its token, or reports it, again after a failure. */
    static final TimeValue RETRY_DELAY = TimeValue.timeValueSeconds(10);
    /** How a token of a hosted account, and of a node's memory, starts: never a marker's column name. */
    private static final String HOSTED = "hosted ";
    private static final String MEMORY = "memory ";

    /** What a node reports: its storage's token, and where that storage is, with any password hidden. */
    record Seen(String token, String where) {}

    // ---- this node ----

    /** This node's storage, once read; null before, and while a switched storage is read again. */
    private static volatile Seen own;
    /** The cluster's token as this node's last applied state holds it; empty for none. */
    private static volatile String expected = "";
    /**
     * The cluster manager this node last reported to, by its ephemeral id: a
     * manager that restarts keeps its node id but holds no reports. Null when
     * the last report failed or was not sent.
     */
    private static volatile String reportedTo;
    /** Whether this node's storage was switched through the account route, and the cluster has not taken its token yet. */
    private static volatile boolean adopting;
    /** Whether a read of this node's storage is under way or scheduled. */
    private static final AtomicBoolean reading = new AtomicBoolean();
    /** The mismatch last logged, so each is logged once. */
    private static volatile String logged;

    // ---- the cluster manager ----

    /** Each node's report, by node id, as the elected manager holds them. */
    private static final Map<String, Seen> REPORTED = new ConcurrentHashMap<>();

    private InfinoStorageIdentity() {}

    /** Register the handler the cluster manager takes reports with. */
    static void register(TransportService transport) {
        transport.registerRequestHandler(REPORT_ACTION, ThreadPool.Names.GENERIC, Report::new, (report, channel, task) -> {
            received(report.nodeId, new Seen(report.token, report.where), report.adopt);
            channel.sendResponse(TransportResponse.Empty.INSTANCE);
        });
    }

    /** Every state a node applies: read its storage once, report to each new manager, and on the manager keep the cluster's token. */
    static void onClusterChanged(ClusterChangedEvent event) {
        ClusterState state = event.state();
        expected = CLUSTER_STORAGE_ID.get(state.metadata().settings());
        Seen seen = own;
        if (seen == null) {
            readSoon(TimeValue.ZERO);
        } else {
            if (adopting && seen.token().equals(expected)) {
                adopting = false;
            }
            DiscoveryNode manager = state.nodes().getClusterManagerNode();
            if (manager != null && !manager.getEphemeralId().equals(reportedTo)) {
                report(seen);
            }
            logMismatch(seen, expected);
        }
        if (state.nodes().isLocalNodeElectedClusterManager()) {
            REPORTED.keySet().removeIf(id -> state.nodes().get(id) == null);
            if (expected.isEmpty() && !REPORTED.isEmpty()) {
                record(null);
            }
        } else if (!REPORTED.isEmpty()) {
            // Another node manages the cluster now; what this one held may be stale by the time it is elected again.
            REPORTED.clear();
        }
    }

    /**
     * The node's storage was switched while it runs (the account route): read
     * its token again and have the cluster take it, as the administrator
     * pointed the cluster's storage there. A no-op before the node's services
     * run, when the switch is the node's own configuration at boot.
     */
    static void storageChanged() {
        if (InfinoServices.clusterService() == null) {
            return;
        }
        adopting = true;
        own = null;
        reportedTo = null;
        logged = null;
        readSoon(TimeValue.ZERO);
    }

    /**
     * Refuse a table operation on a node that does not see the cluster's
     * storage: answered from this node's storage, it would find no documents
     * or the wrong ones, with a 200. Passes while either token is unknown, and
     * while a switched storage waits for the cluster to take its token.
     */
    static void refuseIfUnshared() {
        Seen seen = own;
        String cluster = expected;
        if (seen != null && !adopting && mismatched(cluster, seen)) {
            throw new InfinoNative.InfinoException(notShared(cluster, seen), RestStatus.SERVICE_UNAVAILABLE);
        }
    }

    /** Whether the cluster has a token and {@code seen} is a different one. */
    static boolean mismatched(String cluster, Seen seen) {
        return !cluster.isEmpty() && !cluster.equals(seen.token());
    }

    // ---- reading the token ----

    /** Read this node's token on the generic pool after {@code delay}, unless a read is under way. */
    private static void readSoon(TimeValue delay) {
        ThreadPool pools = InfinoServices.threadPool();
        ClusterService clusters = InfinoServices.clusterService();
        if (pools == null || clusters == null || !reading.compareAndSet(false, true)) {
            return;
        }
        Runnable read = () -> {
            InfinoNative.Target target = InfinoNative.target();
            Seen seen;
            try {
                seen = read(target, clusters.localNode().getId());
            } catch (Exception e) {
                reading.set(false);
                log.warn("[infino] cannot read which storage this node sees; trying again in {}: {}", RETRY_DELAY, e.getMessage());
                readSoon(RETRY_DELAY);
                return;
            }
            reading.set(false);
            if (!target.equals(InfinoNative.target())) {
                // The storage was switched while it was read: read the new one.
                readSoon(TimeValue.ZERO);
                return;
            }
            own = seen;
            log.info("[infino] this node's storage at {} has token {}", seen.where(), seen.token());
            logMismatch(seen, expected);
            report(seen);
        };
        if (delay.nanos() <= 0) {
            pools.generic().execute(read);
        } else {
            pools.schedule(read, delay, ThreadPool.Names.GENERIC);
        }
    }

    /** The token of the storage {@code target} names, as node {@code nodeId} sees it; the marker is created when there is none. */
    static Seen read(InfinoNative.Target target, String nodeId) {
        String where = InfinoNative.scrubbed(target, target.name());
        if (InfinoNative.inMemory(target)) {
            return new Seen(MEMORY + nodeId, where + " (in the memory of node " + nodeId + ")");
        }
        if (target.hosted()) {
            return new Seen(HOSTED + InfinoNative.scrubbed(target, target.uri()), where);
        }
        return new Seen(markerToken(), where);
    }

    /** The token in the marker of the storage the node's connection is open to; the marker is created, with a random token, when there is none. */
    static String markerToken() {
        String token = tokenOf(InfinoNative.columnNames(MARKER_TABLE));
        if (token == null) {
            InfinoNative.ensureTable(MARKER_TABLE, markerMapping(UUID.randomUUID().toString().replace("-", "")));
            token = tokenOf(InfinoNative.columnNames(MARKER_TABLE));
        }
        if (token == null) {
            throw new IllegalStateException("the marker table " + MARKER_TABLE + " has no token column");
        }
        return token;
    }

    /** The token among a marker's columns, in the table's order: the first token column's; null when it has none. */
    static String tokenOf(List<String> columns) {
        for (String column : columns) {
            if (column.startsWith(TOKEN_COLUMN_PREFIX) && column.length() > TOKEN_COLUMN_PREFIX.length()) {
                return column.substring(TOKEN_COLUMN_PREFIX.length());
            }
        }
        return null;
    }

    /** The mapping a marker holding {@code token} is created with. */
    static String markerMapping(String token) {
        return "{\"properties\":{\"" + TOKEN_COLUMN_PREFIX + token + "\":{\"type\":\"keyword\"}}}";
    }

    // ---- reporting ----

    /** Send {@code seen} to the elected manager; on a failure, again after a delay. */
    private static void report(Seen seen) {
        ClusterService clusters = InfinoServices.clusterService();
        TransportService transport = InfinoServices.transportService();
        ThreadContext context = InfinoServices.threadContext();
        if (clusters == null || transport == null || context == null) {
            return;
        }
        DiscoveryNode manager = clusters.state().nodes().getClusterManagerNode();
        if (manager == null) {
            return; // reported when a manager is elected (onClusterChanged)
        }
        reportedTo = manager.getEphemeralId();
        Report request = new Report(clusters.localNode().getId(), seen.token(), seen.where(), adopting);
        // Sent as the node itself, whatever request this thread serves.
        try (ThreadContext.StoredContext ignored = context.stashContext()) {
            transport.sendRequest(manager, REPORT_ACTION, request, new TransportResponseHandler<TransportResponse.Empty>() {
                @Override
                public TransportResponse.Empty read(StreamInput in) {
                    return TransportResponse.Empty.INSTANCE;
                }

                @Override
                public void handleResponse(TransportResponse.Empty response) {}

                @Override
                public void handleException(TransportException e) {
                    reportedTo = null;
                    log.debug("[infino] reporting this node's storage to the cluster manager failed; trying again in {}", RETRY_DELAY, e);
                    ThreadPool pools = InfinoServices.threadPool();
                    if (pools != null) {
                        pools.schedule(() -> {
                            Seen now = own;
                            if (now != null && reportedTo == null) {
                                report(now);
                            }
                        }, RETRY_DELAY, ThreadPool.Names.GENERIC);
                    }
                }

                @Override
                public String executor() {
                    return ThreadPool.Names.SAME;
                }
            });
        }
    }

    /** On the elected manager: node {@code nodeId} sees {@code seen}; {@code adopt} when an administrator switched its storage. */
    static void received(String nodeId, Seen seen, boolean adopt) {
        Seen before = REPORTED.put(nodeId, seen);
        ClusterService clusters = InfinoServices.clusterService();
        if (clusters == null) {
            return;
        }
        String cluster = CLUSTER_STORAGE_ID.get(clusters.state().metadata().settings());
        if (adopt || cluster.isEmpty()) {
            record(adopt ? seen.token() : null);
        } else if (!seen.equals(before)) {
            reroute(clusters, "infino storage reported by node " + nodeId);
        }
    }

    /**
     * On the elected manager: keep {@code token} as the cluster's, replacing
     * whatever it was; or with null, when the cluster has none, the token most
     * data nodes reported. The decisions are taken again once it is kept.
     */
    private static void record(String token) {
        ClusterService clusters = InfinoServices.clusterService();
        if (clusters == null) {
            return;
        }
        clusters.submitStateUpdateTask("infino-storage-id", new ClusterStateUpdateTask(Priority.URGENT) {
            @Override
            public ClusterState execute(ClusterState current) {
                String now = CLUSTER_STORAGE_ID.get(current.metadata().settings());
                String chosen = token != null ? token : now.isEmpty() ? chosen(REPORTED, current.nodes()) : null;
                if (chosen == null || chosen.equals(now)) {
                    return current;
                }
                Settings persistent = Settings.builder()
                    .put(current.metadata().persistentSettings())
                    .put(CLUSTER_STORAGE_ID.getKey(), chosen)
                    .build();
                return ClusterState.builder(current).metadata(Metadata.builder(current.metadata()).persistentSettings(persistent)).build();
            }

            @Override
            public void onFailure(String source, Exception e) {
                log.warn("[infino] cannot record the cluster's storage token", e);
            }

            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                if (oldState != newState) {
                    reroute(clusters, "infino storage token recorded");
                }
            }
        });
    }

    /** The token most of the data nodes in {@code nodes} reported, of {@code reports}; null when none of them has. Ties go to the smaller token. */
    static String chosen(Map<String, Seen> reports, DiscoveryNodes nodes) {
        Map<String, Integer> votes = new HashMap<>();
        for (Map.Entry<String, Seen> e : reports.entrySet()) {
            DiscoveryNode node = nodes.get(e.getKey());
            if (node != null && node.isDataNode()) {
                votes.merge(e.getValue().token(), 1, Integer::sum);
            }
        }
        String best = null;
        for (Map.Entry<String, Integer> e : votes.entrySet()) {
            int than = best == null ? -1 : votes.get(best);
            if (e.getValue() > than || (e.getValue() == than && e.getKey().compareTo(best) < 0)) {
                best = e.getKey();
            }
        }
        return best;
    }

    private static void reroute(ClusterService clusters, String reason) {
        clusters.getRerouteService()
            .reroute(reason, Priority.NORMAL, ActionListener.wrap(r -> {}, e -> log.debug("[infino] reroute after [{}] failed", reason, e)));
    }

    /** What node {@code nodeId} reported, as the elected manager holds it; null when it has not. */
    static Seen reported(String nodeId) {
        return REPORTED.get(nodeId);
    }

    /** For tests: forget every report and this node's own token. */
    static void reset() {
        REPORTED.clear();
        own = null;
        expected = "";
        reportedTo = null;
        adopting = false;
        logged = null;
        reading.set(false);
    }

    /** For tests: what this node's storage and the cluster's token are taken to be. */
    static void assume(Seen seen, String cluster) {
        own = seen;
        expected = cluster;
    }

    /** For tests: record a report as the elected manager would. */
    static void assumeReported(String nodeId, Seen seen) {
        REPORTED.put(nodeId, seen);
    }

    // ---- messages ----

    /** Log once, as an ERROR, that this node does not see the cluster's storage, and what to do. */
    private static void logMismatch(Seen seen, String cluster) {
        if (!mismatched(cluster, seen)) {
            return;
        }
        String key = cluster + " " + seen.token();
        if (key.equals(logged)) {
            return;
        }
        logged = key;
        log.error(
            "{}. {}",
            notShared(cluster, seen),
            "Every node must see the same storage: an S3, GCS or Azure bucket, a hosted account, or one directory every node mounts;"
                + " storage on a node's own disk, or in its memory, serves a single-node cluster only. Until this node sees it, no shard"
                + " of an Infino index is allocated here and this node refuses Infino requests. If the cluster's storage moved on purpose,"
                + " clear the cluster's token: PUT _cluster/settings {\"persistent\":{\""
                + CLUSTER_STORAGE_ID.getKey()
                + "\":null}}"
        );
    }

    /** Why a node seeing {@code seen} is refused by a cluster whose token is {@code cluster}. */
    static String notShared(String cluster, Seen seen) {
        return "[infino] this node does not see the cluster's Infino storage: expected token "
            + cluster
            + ", node has "
            + seen.token()
            + " at "
            + seen.where();
    }

    /** A node's report to the cluster manager. */
    static final class Report extends TransportRequest {
        final String nodeId;
        final String token;
        final String where;
        final boolean adopt;

        Report(String nodeId, String token, String where, boolean adopt) {
            this.nodeId = Objects.requireNonNull(nodeId);
            this.token = Objects.requireNonNull(token);
            this.where = Objects.requireNonNull(where);
            this.adopt = adopt;
        }

        Report(StreamInput in) throws IOException {
            super(in);
            this.nodeId = in.readString();
            this.token = in.readString();
            this.where = in.readString();
            this.adopt = in.readBoolean();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(nodeId);
            out.writeString(token);
            out.writeString(where);
            out.writeBoolean(adopt);
        }
    }
}
