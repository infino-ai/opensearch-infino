/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.TermQuery;
import org.opensearch.ExceptionsHelper;
import org.opensearch.OpenSearchException;
import org.opensearch.OpenSearchStatusException;
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
import org.opensearch.core.concurrency.OpenSearchRejectedExecutionException;
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
 * lease ({@link InfinoEngine#REBUILD_LEASE}), which the primary takes before
 * the shard starts and which every copy is given: a replica refuses those
 * searches too, and a replica that takes over as primary, or a primary that
 * starts again from its own store, finishes the rebuild. The rebuild is done
 * once it has walked the whole table, whatever rows it had to leave out, and
 * it then refreshes every copy so their searches see its last documents,
 * with refresh off as with it on.
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
 * shard, so the skeleton and the translog end where the table is. While a
 * shard's skeleton may lack a row, its engine looks ids up in the table
 * ({@link InfinoEngine#looksUpIds}).
 *
 * <p>A row the rebuild cannot create a document for (a source the mapping
 * now refuses, a row with no source) is left out: it is counted and logged,
 * and it no longer keeps searches back. The shard then holds the left-out
 * lease ({@link InfinoEngine#LEFT_OUT_LEASE}), which every copy is given, so
 * its writes go on looking ids up in the table and a client's write of a
 * left-out id replaces its row rather than adding a second one. Each time
 * the shard's primary starts again, it walks the table again, holding no
 * search back, and only a walk that leaves nothing out removes that lease.
 * A row that routes to another shard and keeps no routing value, or that has
 * no id, is logged too: no write of this shard can name it.
 *
 * <p>A walk that fails for a reason that may pass (see
 * {@link #retryable(Throwable)}) starts again after a wait that grows to
 * {@link #MAX_RESTART_MILLIS}, until it has failed for {@link #GIVE_UP_MILLIS}
 * with no walk ending; any other failure stops it at once. A rebuild that
 * stops logs an error, and the shard's searches that return hits answer 503
 * until the shard starts again (the index closed and opened, or the node
 * restarted).
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
    /**
     * How long a rebuild may go on failing, each time for a reason that may
     * pass, with no walk of the table ending, before it stops with an error:
     * a table that cannot be read for this long is down or refusing, not
     * busy, and a rebuild left trying would hold a generic thread with no end.
     */
    static final long GIVE_UP_MILLIS = 30 * 60_000L;
    /** Pages read between two log lines of a rebuild's progress. */
    private static final int PROGRESS_PAGES = 10;
    /** How many times a finished rebuild tries to refresh the shard's copies. */
    private static final int REFRESH_TRIES = 3;
    /** Rows of each kind a walk names in its log, beyond which it only counts them. */
    static final int NAMED_ROWS = 10;

    /** The shards whose skeleton is being rebuilt on this node, so a second start does not run beside the first. */
    private static final Set<ShardId> RUNNING = ConcurrentHashMap.newKeySet();

    private InfinoSkeletonAdopter() {}

    /**
     * Rebuild {@code shard}'s skeleton from {@code table} on the generic pool,
     * or walk the table again when the shard's rebuild left rows out, unless
     * a walk of it runs already.
     */
    static void start(IndexShard shard, String table) {
        start(shard, table, FIRST_RESTART_MILLIS, -1);
    }

    /**
     * {@link #start}, waiting {@code restart} before starting again if the
     * walk fails for a reason that may pass ({@link #retryable(Throwable)})
     * and it has failed for less than {@link #GIVE_UP_MILLIS} since
     * {@code failingSince} (in the thread pool's relative milliseconds, or -1
     * for no failure yet): until a rebuild is done the shard refuses searches
     * that return hits, so one that fails for a moment is not left there. Any
     * other failure stops it, as does the shard closing or this copy no
     * longer being the primary (a copy that takes over walks the table again).
     */
    private static void start(IndexShard shard, String table, long restart, long failingSince) {
        ThreadPool threadPool = InfinoServices.threadPool();
        ShardId shardId = shard.shardId();
        if (threadPool == null || !RUNNING.add(shardId)) {
            return;
        }
        threadPool.generic().execute(() -> {
            boolean again = false;
            long since = failingSince;
            try {
                adopt(shard, table);
            } catch (Exception e) {
                boolean primaryHere = shard.state() != IndexShardState.CLOSED && shard.isPrimaryMode();
                long now = threadPool.relativeTimeInMillis();
                since = since < 0 ? now : since;
                again = primaryHere && retryable(e) && now - since < GIVE_UP_MILLIS;
                String then;
                if (again) {
                    then = "; it starts again in " + restart + "ms";
                } else if (primaryHere && InfinoEngine.rebuilding(shardId)) {
                    then = "; it stops, and the shard's searches that return hits answer 503 until the shard starts again (close and open the "
                        + "index, or restart the node)";
                } else if (primaryHere) {
                    then = "; it stops, and the shard's writes go on looking ids up in the table until a walk leaves no row out";
                } else {
                    then = "";
                }
                log.error("[infino] the skeleton of {} could not be rebuilt from table {}{}", shardId, table, then, e);
            } finally {
                RUNNING.remove(shardId);
            }
            if (again) {
                long failing = since;
                threadPool.schedule(
                    () -> start(shard, table, Math.min(restart * 2, MAX_RESTART_MILLIS), failing),
                    TimeValue.timeValueMillis(restart),
                    ThreadPool.Names.GENERIC
                );
            }
        });
    }

    private static void adopt(IndexShard shard, String table) throws IOException {
        ShardId shardId = shard.shardId();
        // A walk again over a skeleton whose rebuild left rows out holds no
        // search back: the skeleton has every other row.
        boolean rebuild = InfinoEngine.rebuilding(shardId);
        if ((rebuild && !holdLease(shard, InfinoEngine.REBUILD_LEASE, InfinoEngine.REBUILD_LEASE_SOURCE)) || !waitWhilePaused(shard)) {
            return;
        }
        IndexMetadata md = shard.indexSettings().getIndexMetadata();
        long rows = tableRows(table);
        log.info(
            "[infino] {} the skeleton of {} from the {} rows of table {}",
            rebuild ? "rebuilding" : "walking again, for the rows its rebuild left out,",
            shardId,
            rows,
            table
        );
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
                        tally.unreachable.add("a row", "has no id");
                        continue;
                    }
                    after = id.toString();
                    Object r = routed ? row.get(InfinoQueryTranslator.ROUTING) : null;
                    String routing = r == null ? null : r.toString();
                    if (OperationRouting.generateShardId(md, after, routing) != shardId.id()) {
                        // Written with a routing value before the table kept them: there
                        // is no routing that brings it back to this shard.
                        tally.unreachable.add(after, "routes to another shard by its id and keeps no routing value");
                        continue;
                    }
                    if (searcher.count(new TermQuery(new Term(IdFieldMapper.NAME, Uid.encodeId(after)))) > 0) {
                        continue;
                    }
                    if (source == null) {
                        tally.leftOut.add(after, "has no source");
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
        finish(shard, table, rebuild, tally);
    }

    /**
     * The whole table is walked: the rebuild is done, whatever it left out,
     * so the shard's searches answer. A row left out keeps the shard's writes
     * looking ids up in the table: the left-out lease is taken before the
     * rebuild's is let go, so no write in between takes such an id as new. A
     * walk that leaves nothing out ends that. A copy that is no longer the
     * primary leaves both leases to the one that is, which walks again.
     */
    private static void finish(IndexShard shard, String table, boolean rebuild, Tally tally) {
        ShardId shardId = shard.shardId();
        boolean primary = tally.leftOut.count > 0
            ? holdLease(shard, InfinoEngine.LEFT_OUT_LEASE, InfinoEngine.LEFT_OUT_LEASE_SOURCE)
            : releaseLease(shard, InfinoEngine.LEFT_OUT_LEASE);
        if (!primary) {
            return;
        }
        InfinoEngine.rebuildWalked(shardId);
        if (rebuild) {
            releaseLease(shard, InfinoEngine.REBUILD_LEASE);
            refreshCopies(shardId);
        }
        if (tally.leftOut.count > 0) {
            log.error(
                "[infino] the skeleton of {} has no document for {} rows of table {}: {}. Searches answer, and their hits miss those rows; "
                    + "the shard's writes go on looking ids up in the table, so a write of one of those ids replaces its row. Fix the "
                    + "cause (or delete those ids): each time the shard's primary starts again (close and open the index, or restart the "
                    + "node) it walks the table again, and a walk that leaves nothing out ends the lookups",
                shardId,
                tally.leftOut.count,
                table,
                tally.leftOut
            );
        }
        if (tally.unreachable.count > 0) {
            log.warn(
                "[infino] the skeleton of {} has no document for {} rows of table {} that no write of this shard can name, which its "
                    + "searches' hits miss: {}",
                shardId,
                tally.unreachable.count,
                table,
                tally.unreachable
            );
        }
        log.info("[infino] walked {} rows of table {} for the skeleton of {}, created {} documents", tally.walked, table, shardId, tally.created);
    }

    /** What a walk of the table walked, created and left out. */
    private static final class Tally {
        long walked;
        long created;
        /** Rows of this shard the skeleton has no document for, which a write of this shard may name. */
        final Named leftOut = new Named();
        /** Rows no write of this shard can name: no id, or an id that routes to another shard. */
        final Named unreachable = new Named();
    }

    /**
     * Rows counted, the first {@link #NAMED_ROWS} by id and why: what a walk
     * left out, for its log.
     */
    static final class Named {
        long count;
        final List<String> named = new ArrayList<>();

        void add(String id, String why) {
            count++;
            if (named.size() < NAMED_ROWS) {
                named.add(id + " (" + why + ")");
            }
        }

        @Override
        public String toString() {
            long more = count - named.size();
            return String.join(", ", named) + (more > 0 ? ", and " + more + " more" : "");
        }
    }

    /**
     * Add the lease {@code id} (the rebuild's, {@link InfinoEngine#REBUILD_LEASE},
     * or the left-out one, {@link InfinoEngine#LEFT_OUT_LEASE}) unless the
     * shard holds it already (a rebuild resumed, or one its primary took
     * before the shard started), keep it on disk, and wait until the shard's
     * copies hold it too: from then on a copy that takes over, or this one
     * starting again from its store, knows of it. False when this copy is no
     * longer the primary: the one that is walks the table again.
     */
    private static boolean holdLease(IndexShard shard, String id, String source) {
        if (shard.getRetentionLeases().contains(id)) {
            return true;
        }
        PlainActionFuture<ReplicationResponse> synced = PlainActionFuture.newFuture();
        try {
            if (!shard.isPrimaryMode()) {
                return false;
            }
            shard.addRetentionLease(id, InfinoEngine.REBUILD_LEASE_RETAINING, source, synced);
        } catch (AssertionError e) {
            // The shard moved off this node, or a copy took over, between the check and the add.
            log.info("[infino] the skeleton walk of {} stops here: this copy is no longer its primary", shard.shardId());
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
     * Remove the lease {@code id} and wait until the shard's copies no longer
     * hold it. False when this copy is no longer the primary (its shard
     * moved, or a replica took over): it leaves the lease to the new primary,
     * which walks the table again.
     */
    private static boolean releaseLease(IndexShard shard, String id) {
        PlainActionFuture<ReplicationResponse> synced = PlainActionFuture.newFuture();
        try {
            if (!shard.isPrimaryMode()) {
                return false;
            }
            shard.removeRetentionLease(id, synced);
        } catch (RetentionLeaseNotFoundException e) {
            return true;
        } catch (AssertionError e) {
            log.info("[infino] the skeleton walk of {} ends; the copy that took over as primary releases its lease", shard.shardId());
            return false;
        }
        synced.actionGet();
        return true;
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
     * may pass (a full write pool, a shard not ready: see
     * {@link #retryable(RestStatus)}) fails the walk once the rest are done,
     * so it starts again and creates it then. A create refused for any other
     * reason but a client's write of the same id winning (the mapping
     * refusing the row's source, say) leaves its row out, in {@code tally}.
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
            } else if (retryable(item.getFailure().getStatus())) {
                passing = item.getFailure();
            } else if (item.getFailure().getStatus() != RestStatus.CONFLICT) {
                tally.leftOut.add(item.getId(), "refused: " + item.getFailureMessage());
            }
        }
        if (!created.isEmpty()) {
            deleteLost(index, table, missing, created);
        }
        if (passing != null) {
            throw new OpenSearchStatusException("[infino] a skeleton create failed for a reason that may pass", passing.getStatus(), passing.getCause());
        }
        return created.size();
    }

    /**
     * Whether a request answered {@code status} may go through when tried
     * again: a rate limit or a full thread pool (429), or a service, a shard
     * or a gateway not ready or not answering (502, 503, 504). Any other
     * status, a 500 among them, says the request failed, and trying it again
     * would fail it again.
     */
    static boolean retryable(RestStatus status) {
        return status == RestStatus.TOO_MANY_REQUESTS
            || status == RestStatus.BAD_GATEWAY
            || status == RestStatus.SERVICE_UNAVAILABLE
            || status == RestStatus.GATEWAY_TIMEOUT;
    }

    /**
     * Whether {@code e} may pass when its request is tried again. A bridge
     * error passes when the bridge marks it as the hosted service not reached
     * or not answering ({@link InfinoNative.InfinoException#unavailable}: a
     * transport failure, or a 502, 503 or 504), or when its status is one
     * that passes ({@link #retryable(RestStatus)}), as the service's 429 is;
     * one without a status reads as a 500 and does not. A failure of the
     * cluster passes when it is a full thread pool or carries a status that
     * passes. Anything else is a fault of the request or of the rebuild,
     * which a retry would only repeat.
     */
    static boolean retryable(Throwable e) {
        Throwable cause = ExceptionsHelper.unwrapCause(e);
        if (cause instanceof InfinoNative.InfinoException infino) {
            return infino.unavailable() || retryable(infino.status());
        }
        return cause instanceof OpenSearchRejectedExecutionException
            || (cause instanceof OpenSearchException && retryable(ExceptionsHelper.status(cause)));
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
