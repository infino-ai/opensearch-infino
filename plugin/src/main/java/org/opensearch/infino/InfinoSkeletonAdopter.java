/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.TermQuery;
import org.opensearch.action.DocWriteRequest;
import org.opensearch.action.bulk.BulkItemResponse;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.delete.DeleteRequest;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.action.support.replication.ReplicationResponse;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.routing.OperationRouting;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.gateway.WriteStateException;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.mapper.IdFieldMapper;
import org.opensearch.index.mapper.Uid;
import org.opensearch.index.seqno.RetentionLeaseNotFoundException;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.shard.IndexShardState;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Rebuilds the Lucene skeleton of a default-mode shard whose primary started
 * empty over a table that holds rows: an index the registry recreated
 * ({@link InfinoPlugin#INFINO_REBUILT}), or a primary allocated empty after
 * its copies were lost. A search hit names a document of the skeleton, so
 * until the rebuild is done, the shard's searches that return hits answer
 * 503 ({@link InfinoEngine#answersSearches}) rather than a page that leaves
 * out the rows written before the rebuild.
 *
 * <p>The rebuild is under way while the shard holds the rebuild's retention
 * lease ({@link InfinoEngine#REBUILD_LEASE}), which the primary adds before
 * it reads the table and which every copy is given: a replica refuses those
 * searches too, and a replica that takes over as primary, or a primary that
 * starts again from its own store, finishes the rebuild. The rebuild is done
 * once it has walked the whole table, whatever rows it had to leave out
 * (which it logs, with how many and why), and it then refreshes every copy
 * so their searches see its last documents, with refresh off as with it on.
 *
 * <p>When the shard's primary starts (or a replica takes over as primary),
 * the shard's table is read a page at a time in id order, and every row the
 * skeleton lacks is created through the cluster's own write path, from the
 * row's own source, so each of the shard's copies gets it as it gets any
 * write, routed by the routing value the row was written with when it was
 * written with one. The table already holds these rows, so the primary's
 * engine leaves the table alone for them (the request carries
 * {@link #SKELETON_ONLY}).
 * Each is a create, so a client's write of the same id wins; a row a client
 * deleted while its create was in flight is deleted again through the
 * shard, so the skeleton and the translog end where the table is. Until a
 * shard's skeleton is whole, its engine looks ids up in the table
 * ({@link InfinoEngine#skeletonComplete}).
 *
 * <p>While {@link #PAUSED} is set, a rebuild waits before it reads the
 * table: the shard holds the rebuild's lease, its searches that return hits
 * answer 503, and its engine goes on looking ids up in the table.
 */
final class InfinoSkeletonAdopter {

    private static final Logger log = LogManager.getLogger(InfinoSkeletonAdopter.class);

    /** The header a skeleton-only write carries, so the primary's engine writes Lucene alone. */
    static final String SKELETON_ONLY = "_infino_skeleton_only";
    /** Rows read from the table per page. */
    private static final int PAGE_ROWS = 10_000;
    /** The count's alias in the row-count statement. */
    private static final String COUNT_ALIAS = "n";

    /**
     * Hold every skeleton rebuild that has not begun reading its table until
     * the setting is cleared, as {@code PUT _cluster/settings} sets it: an
     * operator keeps a rebuild's writes off a busy cluster, and a test holds
     * a skeleton incomplete while it writes.
     */
    static final Setting<Boolean> PAUSED = Setting.boolSetting(
        "infino.skeleton_rebuild.paused",
        false,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );
    /** How often a paused rebuild looks whether it may go on. */
    private static final long PAUSE_POLL_MILLIS = 200;
    /** The wait before a rebuild that failed starts again; it doubles after each failure up to {@link #MAX_RESTART_MILLIS}. */
    private static final long FIRST_RESTART_MILLIS = 1_000;
    /** The longest wait before a failed rebuild starts again. */
    private static final long MAX_RESTART_MILLIS = 60_000;
    /** Pages read between two log lines of a rebuild's progress. */
    private static final int PROGRESS_PAGES = 10;
    /** How many times a finished rebuild tries to refresh the shard's copies. */
    private static final int REFRESH_TRIES = 3;
    /** Rows left out that a rebuild names in its log, beyond which it only counts them. */
    private static final int NAMED_LEFT_OUT = 10;

    /** The shards whose skeleton is being rebuilt on this node, so a second start does not run beside the first. */
    private static final Set<ShardId> RUNNING = ConcurrentHashMap.newKeySet();

    private InfinoSkeletonAdopter() {}

    /** Rebuild {@code shard}'s skeleton from {@code table} on the generic pool, unless it is being rebuilt already. */
    static void start(IndexShard shard, String table) {
        start(shard, table, FIRST_RESTART_MILLIS);
    }

    /**
     * {@link #start}, waiting {@code restart} before starting again if the
     * rebuild fails: until it is done the shard refuses searches that return
     * hits, so a rebuild that fails is not left there. It stops once the
     * shard closes or is no longer the primary here (a copy that takes over
     * finishes the rebuild).
     */
    private static void start(IndexShard shard, String table, long restart) {
        ThreadPool threadPool = InfinoServices.threadPool();
        ShardId shardId = shard.shardId();
        if (threadPool == null || !RUNNING.add(shardId)) {
            return;
        }
        threadPool.generic().execute(() -> {
            boolean again = false;
            try {
                adopt(shard, table);
            } catch (Exception e) {
                again = shard.state() != IndexShardState.CLOSED && shard.isPrimaryMode();
                log.error(
                    "[infino] the skeleton of {} could not be rebuilt from table {}{}",
                    shardId,
                    table,
                    again ? "; its searches that return hits answer 503 until it is, and it starts again in " + restart + "ms" : "",
                    e
                );
            } finally {
                RUNNING.remove(shardId);
            }
            if (again) {
                threadPool.schedule(
                    () -> start(shard, table, Math.min(restart * 2, MAX_RESTART_MILLIS)),
                    TimeValue.timeValueMillis(restart),
                    ThreadPool.Names.GENERIC
                );
            }
        });
    }

    private static void adopt(IndexShard shard, String table) throws IOException {
        ShardId shardId = shard.shardId();
        if (!holdLease(shard) || !waitWhilePaused(shard)) {
            return;
        }
        IndexMetadata md = shard.indexSettings().getIndexMetadata();
        long rows = tableRows(table);
        log.info("[infino] rebuilding the skeleton of {} from the {} rows of table {}", shardId, rows, table);
        // A row written with a routing value keeps it; a table that never
        // had one has no column for it.
        boolean routed = InfinoNative.columns(table).containsKey(InfinoQueryTranslator.ROUTING);
        Tally tally = new Tally();
        String after = null;
        for (int pages = 1;; pages++) {
            List<Map<String, Object>> page = InfinoNative.sqlRows(pageSql(table, after, routed));
            Map<String, Row> missing = new LinkedHashMap<>();
            try (Engine.Searcher searcher = shard.acquireSearcher("infino_skeleton")) {
                for (Map<String, Object> row : page) {
                    tally.walked++;
                    Object id = row.get(InfinoQueryTranslator.OSID);
                    Object source = row.get(InfinoQueryTranslator.SOURCE);
                    if (id == null) {
                        tally.noId++;
                        continue;
                    }
                    after = id.toString();
                    Object r = routed ? row.get(InfinoQueryTranslator.ROUTING) : null;
                    String routing = r == null ? null : r.toString();
                    if (OperationRouting.generateShardId(md, after, routing) != shardId.id()) {
                        // Written with a routing value before the table kept them: there
                        // is no routing that brings it back to this shard.
                        tally.leftOut("routes to another shard by its id and keeps no routing value", after);
                        continue;
                    }
                    if (searcher.count(new TermQuery(new Term(IdFieldMapper.NAME, Uid.encodeId(after)))) > 0) {
                        continue;
                    }
                    if (source == null) {
                        tally.leftOut("has no source", after);
                    } else {
                        missing.put(after, new Row(source.toString(), routing));
                    }
                }
            }
            if (!missing.isEmpty()) {
                tally.created += createMissing(md.getIndex().getName(), table, missing, tally);
            }
            if (page.size() < PAGE_ROWS) {
                break;
            }
            if (pages % PROGRESS_PAGES == 0) {
                log.info("[infino] rebuilding the skeleton of {}: walked {} of about {} rows, created {} documents", shardId, tally.walked, rows, tally.created);
            }
        }
        // The whole table is walked: the rebuild is done, whatever it left out.
        InfinoEngine.skeletonComplete(shardId);
        releaseLease(shard);
        refreshCopies(shardId);
        if (tally.leftOut > 0 || tally.noId > 0) {
            log.warn(
                "[infino] the skeleton of {} leaves out {} rows of table {}, which its searches' hits miss: {}{}",
                shardId,
                tally.leftOut + tally.noId,
                table,
                tally.named,
                tally.noId > 0 ? "; " + tally.noId + " rows have no id" : ""
            );
        }
        log.info("[infino] rebuilt the skeleton of {}: walked {} rows of table {}, created {} documents", shardId, tally.walked, table, tally.created);
    }

    /** What a rebuild walked, created and left out (and some of what it left out, by id and why). */
    private static final class Tally {
        long walked;
        long created;
        long noId;
        long leftOut;
        final List<String> named = new ArrayList<>();

        void leftOut(String why, String id) {
            leftOut++;
            if (named.size() < NAMED_LEFT_OUT) {
                named.add(id + " (" + why + ")");
            }
        }
    }

    /**
     * Add the rebuild's lease ({@link InfinoEngine#REBUILD_LEASE}) unless the
     * shard holds it already (a rebuild resumed), keep it on disk, and wait
     * until the shard's copies hold it too: from then on a copy that takes
     * over, or this one starting again from its store, finishes the rebuild.
     * False when this copy is no longer the primary: the one that is
     * finishes it.
     */
    private static boolean holdLease(IndexShard shard) {
        if (shard.getRetentionLeases().contains(InfinoEngine.REBUILD_LEASE)) {
            return true;
        }
        PlainActionFuture<ReplicationResponse> synced = PlainActionFuture.newFuture();
        try {
            if (!shard.isPrimaryMode()) {
                return false;
            }
            shard.addRetentionLease(InfinoEngine.REBUILD_LEASE, InfinoEngine.REBUILD_LEASE_RETAINING, InfinoEngine.REBUILD_LEASE_SOURCE, synced);
        } catch (AssertionError e) {
            // The shard moved off this node, or a copy took over, between the check and the add.
            log.info("[infino] the skeleton rebuild of {} stops here: this copy is no longer its primary", shard.shardId());
            return false;
        }
        synced.actionGet();
        try {
            shard.persistRetentionLeases();
        } catch (WriteStateException e) {
            throw new UncheckedIOException(e);
        }
        return true;
    }

    /**
     * Remove the rebuild's lease and wait until the shard's copies no longer
     * hold it. A copy that no longer is the primary (its shard moved, or a
     * replica took over) leaves it: the new primary finishes the rebuild.
     */
    private static void releaseLease(IndexShard shard) {
        PlainActionFuture<ReplicationResponse> synced = PlainActionFuture.newFuture();
        try {
            if (!shard.isPrimaryMode()) {
                return;
            }
            shard.removeRetentionLease(InfinoEngine.REBUILD_LEASE, synced);
        } catch (RetentionLeaseNotFoundException e) {
            return;
        } catch (AssertionError e) {
            log.info("[infino] the skeleton of {} is rebuilt; the copy that took over as primary releases its lease", shard.shardId());
            return;
        }
        synced.actionGet();
    }

    /**
     * Refresh every copy of {@code index}, so the rebuild's last documents
     * reach searches with refresh off as with it on; a copy knows its
     * rebuild has ended before this refresh opens its reader
     * ({@link InfinoEngine#answersSearches}). Tried a few times; a copy it
     * still misses answers once its next refresh runs.
     */
    private static void refreshCopies(ShardId shardId) {
        ThreadContext context = InfinoServices.threadPool().getThreadContext();
        long wait = FIRST_RESTART_MILLIS;
        for (int tries = 1;; tries++) {
            try (ThreadContext.StoredContext ignored = context.stashContext()) {
                PlainActionFuture<Void> refreshed = PlainActionFuture.newFuture();
                InfinoClient.refresh(shardId.getIndexName(), refreshed);
                refreshed.actionGet();
                return;
            } catch (RuntimeException e) {
                if (tries >= REFRESH_TRIES) {
                    log.warn(
                        "[infino] the skeleton of {} is rebuilt, but not every copy was refreshed; a copy not refreshed answers searches "
                            + "that return hits with 503 until its next refresh",
                        shardId,
                        e
                    );
                    return;
                }
            }
            try {
                Thread.sleep(wait);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
            wait *= 2;
        }
    }

    /**
     * Wait while {@link #PAUSED} is set. False when the shard closed (or
     * the node stops the wait) meanwhile: the shard's next start rebuilds it.
     */
    private static boolean waitWhilePaused(IndexShard shard) {
        boolean logged = false;
        while (paused()) {
            if (shard.state() == IndexShardState.CLOSED) {
                return false;
            }
            if (!logged) {
                log.info("[infino] the skeleton of {} waits to be rebuilt: {} is set", shard.shardId(), PAUSED.getKey());
                logged = true;
            }
            try {
                Thread.sleep(PAUSE_POLL_MILLIS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        }
        return shard.state() != IndexShardState.CLOSED;
    }

    /** Whether the cluster's settings pause rebuilds now. */
    private static boolean paused() {
        ClusterService clusterService = InfinoServices.clusterService();
        return clusterService != null && PAUSED.get(clusterService.state().metadata().settings());
    }

    /** A row the skeleton lacks: its source, and the routing value it was written with (null for none). */
    private record Row(String source, String routing) {}

    /** The table's rows. */
    static long tableRows(String table) {
        List<Map<String, Object>> out = InfinoNative.sqlRows("SELECT COUNT(*) AS " + COUNT_ALIAS + " FROM " + InfinoQueryTranslator.quoteIdent(table));
        return out.isEmpty() ? 0 : InfinoAggregationProcessor.asLong(out.get(0).get(COUNT_ALIAS));
    }

    /** One page of the table's ids, sources and (when {@code routed}) routing values in id order, after {@code after} (null for the first). */
    private static String pageSql(String table, String after, boolean routed) {
        String osid = InfinoQueryTranslator.quoteIdent(InfinoQueryTranslator.OSID);
        return "SELECT "
            + osid
            + ", "
            + InfinoQueryTranslator.quoteIdent(InfinoQueryTranslator.SOURCE)
            + (routed ? ", " + InfinoQueryTranslator.quoteIdent(InfinoQueryTranslator.ROUTING) : "")
            + " FROM "
            + InfinoQueryTranslator.quoteIdent(table)
            + (after == null ? "" : " WHERE " + osid + " > " + InfinoQueryTranslator.literal(after))
            + " ORDER BY "
            + osid
            + " LIMIT "
            + PAGE_ROWS;
    }

    /**
     * Create a skeleton document for each id, as a skeleton-only write; then
     * delete through the shard any whose row the table lost meanwhile.
     * Returns the documents created. A create that failed for a reason that
     * may pass (a full write pool, a shard not ready: 429 or a server error)
     * fails the rebuild once the rest are done, so it starts again and
     * creates it then. A create refused for any other reason but a client's
     * write of the same id winning (the mapping refusing the row's source,
     * say) leaves its row out, in {@code tally}.
     */
    private static long createMissing(String index, String table, Map<String, Row> missing, Tally tally) {
        BulkRequest creates = new BulkRequest();
        for (Map.Entry<String, Row> row : missing.entrySet()) {
            creates.add(
                new IndexRequest(index).id(row.getKey())
                    .routing(row.getValue().routing())
                    .source(row.getValue().source(), XContentType.JSON)
                    .opType(DocWriteRequest.OpType.CREATE)
            );
        }
        List<String> created = new ArrayList<>();
        BulkItemResponse.Failure passing = null;
        for (BulkItemResponse item : run(creates, true).getItems()) {
            if (!item.isFailed()) {
                created.add(item.getId());
            } else if (mayPass(item.getFailure().getStatus())) {
                passing = item.getFailure();
            } else if (item.getFailure().getStatus() != RestStatus.CONFLICT) {
                tally.leftOut("refused: " + item.getFailureMessage(), item.getId());
            }
        }
        if (!created.isEmpty()) {
            deleteLost(index, table, missing, created);
        }
        if (passing != null) {
            throw new IllegalStateException("[infino] a skeleton create failed for a reason that may pass", passing.getCause());
        }
        return created.size();
    }

    /** Whether a create that failed with {@code status} may go through when sent again: a rate limit, or a server error. */
    private static boolean mayPass(RestStatus status) {
        return status == RestStatus.TOO_MANY_REQUESTS || status.getStatus() >= RestStatus.INTERNAL_SERVER_ERROR.getStatus();
    }

    /** Delete through the shard the documents just {@code created} whose rows the table no longer holds. */
    private static void deleteLost(String index, String table, Map<String, Row> missing, List<String> created) {
        // As of the table's latest commit: a row another node deleted within the
        // engine's staleness window would otherwise read as kept, and its
        // skeleton stay.
        Set<String> kept = InfinoNative.existingOsidsLatest(table, created);
        BulkRequest deletes = new BulkRequest();
        for (String id : created) {
            if (!kept.contains(id)) {
                deletes.add(new DeleteRequest(index, id).routing(missing.get(id).routing()));
            }
        }
        if (deletes.numberOfActions() > 0) {
            run(deletes, false);
        }
    }

    /** Run {@code bulk} as the node, skeleton-only when asked, and wait for it. */
    private static BulkResponse run(BulkRequest bulk, boolean skeletonOnly) {
        ThreadContext context = InfinoServices.threadPool().getThreadContext();
        try (ThreadContext.StoredContext ignored = context.stashContext()) {
            if (skeletonOnly) {
                context.putHeader(SKELETON_ONLY, Boolean.TRUE.toString());
            }
            PlainActionFuture<BulkResponse> response = PlainActionFuture.newFuture();
            InfinoClient.bulk(bulk, response);
            return response.actionGet();
        }
    }
}
