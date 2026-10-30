/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import com.sun.jna.Pointer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.util.BytesRef;
import org.opensearch.core.concurrency.OpenSearchRejectedExecutionException;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.engine.EngineConfig;
import org.opensearch.index.engine.EngineCreationFailureException;
import org.opensearch.index.engine.EngineException;
import org.opensearch.index.engine.FlushFailedEngineException;
import org.opensearch.index.engine.InternalEngine;
import org.opensearch.index.mapper.ParseContext.Document;
import org.opensearch.index.mapper.SourceFieldMapper;
import org.opensearch.index.translog.Translog;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

/**
 * The engine of an index in engine mode ({@code index.infino.mode: engine}):
 * OpenSearch's own write path (sequence numbers, translog, acknowledgement,
 * the refresh and flush cycle) over a Lucene index that holds only each
 * document's identity, with every document's content in the index's table.
 *
 * <p>Each write is parsed by the index's mapper as on any index, so the
 * mapping grows and a document the mapping cannot hold is refused the usual
 * way; the user fields and the stored source are then dropped from the
 * Lucene document, which keeps {@code _id}, the sequence number and the
 * version, what the engine contract and the fetch phase need.
 *
 * <p>Writes reach the table in batches, a group commit: every write waiting
 * goes in the next commit, and a write's acknowledgement waits for the
 * commit that carries it. A batch commits when the shard syncs its translog
 * for a write, so an acknowledged write is in the table on every copy's
 * behalf; on refresh; on flush, before the translog that holds it is
 * trimmed; and past its document or byte bound. Each commit first syncs the
 * translog and refreshes the skeleton's reader, so the table never holds a
 * write the translog could lose or a document a search's reader cannot
 * fetch: a crash between the two leaves writes the translog's replay looks
 * up in the table, and every search reader opened since knows each id the
 * table answers with. A commit that fails keeps its batch for the next one
 * and fails what asked for it: the write's sync, or the flush, so the
 * translog keeps the writes the table lacks. Writes wait rather than pile up
 * past a bound while commits fail.
 *
 * <p>The engine knows which writes replace an existing document (the version
 * map says so), so those rows replace their old ones in the same commit
 * without a lookup; a write replayed from the translog, which may or may not
 * have reached the table before a restart, is looked up. A write's record in
 * the batch is made under the same per-id lock as its skeleton write, so the
 * batch holds each id's writes in the order the shard applied them.
 *
 * <p>Only the primary writes the table: one table has one writer. A
 * replica's documents, the operations a recovering copy receives, and a
 * replica's own translog replay were all written by the primary that took
 * them, so on a replica the skeleton alone is written. Every copy keeps each
 * write's source as the recovery source, as stock does for a filtered
 * source, so a copy can recover from another's history; Lucene drops it
 * from merged segments once no copy can need it. A replica promoted to
 * primary finds every acknowledged write already in the table. Each shard
 * has its own table (see {@link InfinoTables}), so an index of several
 * shards has one writer per table too.
 *
 * <p>An index the registry recreated on a fresh cluster ({@link
 * InfinoPlugin#INFINO_REBUILT}) starts with an empty skeleton over a table
 * that holds rows; {@link InfinoSkeletonAdopter} creates a document for each
 * row, through the write path, with writes that leave the table alone. Until
 * it is done the skeleton cannot say which ids exist: a write the skeleton
 * takes as new is looked up in the table, and a delete the skeleton does
 * not find deletes the table's row and reports it found. A row written with
 * a routing value keeps it, so the rebuilt document is routed as it was.
 *
 * <p>A table refusal after Lucene has acknowledged a write leaves that
 * document out of the table; it is logged. The mapper refuses what the
 * mapping cannot hold first, so this is a document the table's columns
 * refuse beyond the mapping.
 */
public class InfinoEngine extends InternalEngine {
    private static final Logger log = LogManager.getLogger(InfinoEngine.class);
    /** Documents a batch holds before it commits on its own, between refreshes. */
    private static final int MAX_BATCH_DOCS = Integer.getInteger("infino.engine.batch.docs", 50_000);
    /** Source bytes a batch holds before it commits on its own, between refreshes. */
    private static final long MAX_BATCH_BYTES = Long.getLong("infino.engine.batch.bytes", 64L << 20);
    /**
     * Batches' worth of writes that may wait for a commit before a write is
     * refused: past this the commits are failing, and a write waits for the
     * table rather than piling up in memory.
     */
    private static final int MAX_PENDING_BATCHES = 4;
    /** Locks over ids that order each id's skeleton write with its record in the batch. */
    private static final int ID_LOCK_STRIPES = 1024;
    /** The refresh source of the skeleton refresh each commit runs first. */
    private static final String COMMIT_REFRESH = "infino_commit";

    /** The writes waiting for the next commit, folded by id. */
    private static final class Batch {
        /** The latest source for each id written, in arrival order. */
        final Map<String, String> writes = new LinkedHashMap<>();
        /** The routing value each id was written with, for the ids written with one. */
        final Map<String, String> routings = new HashMap<>();
        /** Ids whose row the table holds and a write replaces. */
        final Set<String> replaced = new HashSet<>();
        /** Ids replayed from the translog: whether the table holds them is looked up at commit. */
        final Set<String> replayed = new HashSet<>();
        /** Ids created in this batch, so the table has no row for them yet. */
        final Set<String> created = new HashSet<>();
        /** Ids to tombstone. */
        final Set<String> deleted = new LinkedHashSet<>();
        long bytes = 0;

        boolean isEmpty() {
            return writes.isEmpty() && deleted.isEmpty();
        }

        int size() {
            return writes.size() + deleted.size();
        }

        /** Record a write of {@code id}, routed by {@code routing} when not null. */
        void write(String id, String source, String routing) {
            writes.put(id, source);
            deleted.remove(id);
            if (routing == null) {
                routings.remove(id);
            } else {
                routings.put(id, routing);
            }
            bytes += source.length();
        }
    }

    private final String table;
    private final ShardId shardId;
    /** Whether the skeleton started empty over a table holding rows (see the class comment). */
    private final boolean rebuilt;
    private final Object batchLock = new Object();
    /** Serializes commits, so one batch reaches the table before the next. */
    private final Object commitLock = new Object();
    private final Object[] idLocks = new Object[ID_LOCK_STRIPES];
    private Batch batch = new Batch();
    /** The batch being committed, until the table has it; a read still sees its writes meanwhile. */
    private Batch committing = null;

    /**
     * A write waiting for its commit, as a read sees it before the table
     * does: the document's source, or null when the write deleted it.
     */
    record Pending(String source) {}

    /** The live engines by table, so a read sees what a shard has acknowledged and not yet committed. */
    private static final Map<String, InfinoEngine> BY_TABLE = new ConcurrentHashMap<>();

    /**
     * Whether this node's copy of each shard is its primary, as its routing
     * said when it began recovering and as it changes since (a promotion):
     * a copy replaying its own translog writes the table only as primary.
     */
    private static final Map<ShardId, Boolean> PRIMARY_COPY = new ConcurrentHashMap<>();

    /** The shards of rebuilt indices whose skeleton holds every row of the table again (see {@link InfinoSkeletonAdopter}). */
    private static final Set<ShardId> SKELETON_COMPLETE = ConcurrentHashMap.newKeySet();

    /** Record whether this node's copy of {@code shard} is its primary. */
    static void primaryCopy(ShardId shard, boolean primary) {
        PRIMARY_COPY.put(shard, primary);
    }

    /** {@code shard}'s skeleton holds a document for every row of its table: ids need no lookup. */
    static void skeletonComplete(ShardId shard) {
        SKELETON_COMPLETE.add(shard);
    }

    /** This node no longer holds a copy of {@code shard}. */
    static void forgetCopy(ShardId shard) {
        PRIMARY_COPY.remove(shard);
        SKELETON_COMPLETE.remove(shard);
    }

    /** Whether the skeleton can say which ids the table holds: not while a rebuilt index's skeleton is rebuilt. */
    private boolean skeletonKnowsIds() {
        return !rebuilt || SKELETON_COMPLETE.contains(shardId);
    }

    /** Whether the write being applied is a skeleton-only one, whose row the table holds already. */
    private boolean skeletonOnly() {
        return config().getThreadPool().getThreadContext().getHeader(InfinoSkeletonAdopter.SKELETON_ONLY) != null;
    }

    public InfinoEngine(EngineConfig config) {
        super(config);
        this.shardId = config.getShardId();
        this.rebuilt = InfinoPlugin.INFINO_REBUILT.get(config.getIndexSettings().getSettings());
        for (int i = 0; i < idLocks.length; i++) {
            idLocks[i] = new Object();
        }
        try {
            this.table = InfinoPlugin.tables().ensure(config.getIndexSettings().getIndexMetadata(), config.getShardId().id());
        } catch (Throwable t) {
            // A bridge that cannot load or a table that cannot be declared fails
            // this shard; as an Error it would take the node down.
            throw new EngineCreationFailureException(
                config.getShardId(),
                "[infino] cannot declare the table of " + config.getShardId().getIndexName(),
                t instanceof Exception e ? e : new RuntimeException(t)
            );
        }
        BY_TABLE.put(table, this);
        log.info("[infino] engine attached to table {} for {}", table, config.getShardId());
    }

    /**
     * The write to {@code id} a shard holds for {@code table} and has not
     * committed yet, or null when it holds none: what a realtime read or an
     * update's merge sees before the table does, as a stock shard serves
     * them from its translog.
     */
    static Pending pending(String table, String id) {
        InfinoEngine engine = BY_TABLE.get(table);
        if (engine == null) {
            return null;
        }
        synchronized (engine.batchLock) {
            // The newest write wins: the open batch's, else the one committing.
            for (Batch b : new Batch[] { engine.batch, engine.committing }) {
                if (b == null) {
                    continue;
                }
                String source = b.writes.get(id);
                if (source != null) {
                    return new Pending(source);
                }
                if (b.deleted.contains(id)) {
                    return new Pending(null);
                }
            }
            return null;
        }
    }

    /** {@code id}'s source as a read sees it now: a pending write's, else the table's; null when it is deleted or missing. */
    static String currentSource(String table, String id) {
        Pending p = pending(table, id);
        return p != null ? p.source() : InfinoNative.getSourceFresh(table, id);
    }

    /**
     * The routing value {@code id} was last written with, as a read sees it
     * now: a pending write's, else the table's; null when it was written with
     * none.
     */
    static String currentRouting(String table, String id) {
        InfinoEngine engine = BY_TABLE.get(table);
        if (engine != null) {
            synchronized (engine.batchLock) {
                for (Batch b : new Batch[] { engine.batch, engine.committing }) {
                    if (b != null && b.writes.containsKey(id)) {
                        return b.routings.get(id);
                    }
                }
            }
        }
        return InfinoNative.routingOf(table, id);
    }

    @Override
    public void close() throws IOException {
        BY_TABLE.remove(table, this);
        super.close();
    }

    /**
     * Whether an operation of this origin is this copy's to write to the
     * table: the primary's own writes, and its replay of its own translog
     * after a restart. A replica's operations, a recovering copy's, and a
     * replica's replay or reset all carry writes the primary that took them
     * has written.
     */
    private boolean writesTable(Operation.Origin origin) {
        return switch (origin) {
            case PRIMARY -> true;
            case LOCAL_TRANSLOG_RECOVERY -> PRIMARY_COPY.getOrDefault(shardId, false);
            default -> false;
        };
    }

    /** The lock that orders the writes of {@code id}. */
    private Object idLock(String id) {
        return idLocks[Math.floorMod(id.hashCode(), ID_LOCK_STRIPES)];
    }

    /**
     * Refuse a new write while more than {@link #MAX_PENDING_BATCHES}
     * batches' worth wait for a commit: the commits are failing, and the
     * writes would otherwise pile up in memory. A replay is not refused; it
     * restores writes the shard acknowledged.
     */
    private void refuseWhileBacklogged(Operation.Origin origin) {
        if (origin.isRecovery()) {
            return;
        }
        int waiting;
        long bytes;
        synchronized (batchLock) {
            waiting = batch.size() + (committing == null ? 0 : committing.size());
            bytes = batch.bytes + (committing == null ? 0 : committing.bytes);
        }
        if (waiting >= (long) MAX_PENDING_BATCHES * MAX_BATCH_DOCS || bytes >= MAX_PENDING_BATCHES * MAX_BATCH_BYTES) {
            throw new OpenSearchRejectedExecutionException(
                "[infino] " + waiting + " writes to table " + table + " are waiting for a commit; the write is refused until the table takes them"
            );
        }
    }

    @Override
    public IndexResult index(Index index) throws IOException {
        boolean toTable = writesTable(index.origin()) && !skeletonOnly();
        if (toTable) {
            refuseWhileBacklogged(index.origin());
        }
        String source = index.source() == null
            ? null
            : InfinoBulkActionFilter.sourceAsJson(index.source(), index.parsedDoc() == null ? null : index.parsedDoc().getMediaType());
        // Lucene keeps the document's identity only: the metadata fields the
        // engine and the fetch phase read, not the user fields or the source.
        for (Document d : index.docs()) {
            d.getFields().removeIf(f -> !f.name().startsWith("_") || SourceFieldMapper.NAME.equals(f.name()));
        }
        keepRecoverySource(index);
        String id = index.id();
        IndexResult result;
        boolean full = false;
        synchronized (idLock(id)) {
            result = super.index(index);
            if (result.getResultType() != Result.Type.SUCCESS || !toTable || source == null) {
                return result;
            }
            synchronized (batchLock) {
                Batch b = batch;
                boolean pendingCreate = b.created.contains(id);
                b.write(id, source, index.routing());
                if (index.origin().isRecovery() || (!skeletonKnowsIds() && result.isCreated() && !pendingCreate)) {
                    // The table may hold a row the skeleton does not know of.
                    b.replayed.add(id);
                } else if (result.isCreated()) {
                    b.created.add(id);
                    b.replaced.remove(id);
                } else if (!pendingCreate) {
                    b.replaced.add(id);
                }
                full = b.writes.size() >= MAX_BATCH_DOCS || b.bytes >= MAX_BATCH_BYTES;
            }
        }
        if (full) {
            commitBatch();
        }
        return result;
    }

    /**
     * Keep the write's source in the skeleton as its recovery source, as the
     * mapper does when an index filters its stored source: what a copy
     * recovering from this one replays its operations from.
     */
    private static void keepRecoverySource(Index index) {
        if (index.source() == null || index.parsedDoc() == null) {
            return;
        }
        Document root = index.parsedDoc().rootDoc();
        if (root.getField(SourceFieldMapper.RECOVERY_SOURCE_NAME) != null) {
            return; // the mapper kept it already
        }
        BytesRef ref = index.source().toBytesRef();
        root.add(new StoredField(SourceFieldMapper.RECOVERY_SOURCE_NAME, ref.bytes, ref.offset, ref.length));
        root.add(new NumericDocValuesField(SourceFieldMapper.RECOVERY_SOURCE_NAME, 1));
    }

    @Override
    public DeleteResult delete(Delete delete) throws IOException {
        boolean toTable = writesTable(delete.origin());
        if (toTable) {
            refuseWhileBacklogged(delete.origin());
        }
        String id = delete.id();
        DeleteResult result;
        synchronized (idLock(id)) {
            result = super.delete(delete);
            if (result.getResultType() != Result.Type.SUCCESS || !toTable) {
                return result;
            }
            if (result.isFound()) {
                synchronized (batchLock) {
                    Batch b = batch;
                    b.writes.remove(id);
                    b.routings.remove(id);
                    b.replaced.remove(id);
                    b.replayed.remove(id);
                    if (!b.created.remove(id)) {
                        b.deleted.add(id); // the table holds a row for it
                    }
                }
                return result;
            }
            if (skeletonKnowsIds() || !deleteTableOnly(id)) {
                return result;
            }
        }
        // The found result carries no translog location, so the delete's is
        // synced here when the index syncs for each request.
        Translog.Location location = result.getTranslogLocation();
        if (location != null && config().getIndexSettings().getTranslogDurability() == Translog.Durability.REQUEST) {
            translogManager().ensureTranslogSynced(Stream.of(location));
        }
        return new DeleteResult(result.getVersion(), result.getTerm(), result.getSeqNo(), true);
    }

    /**
     * A delete the skeleton of a rebuilt index did not find: the table may
     * hold the row from before the rebuild. When it does, the row is deleted
     * and the delete reports it found, as it would have before the rebuild.
     */
    private boolean deleteTableOnly(String id) {
        if (InfinoNative.existingOsids(table, List.of(id)).isEmpty()) {
            return false;
        }
        synchronized (batchLock) {
            batch.deleted.add(id);
        }
        return true;
    }

    /**
     * Commit what is pending before the shard syncs its translog for a write,
     * so a write is in the table before it is acknowledged (see the class
     * comment). A failure fails the sync, and so the write.
     */
    protected void commitBeforeSync() {
        if (commitLock == null) {
            return; // a sync from within the base engine's constructor: nothing is pending yet
        }
        commitBatch();
    }

    /**
     * Commit what is pending to the table; batches reach it in the order they
     * were taken. The translog is synced and the skeleton's reader refreshed
     * first (see the class comment). A failure keeps the batch for the next
     * commit and is thrown.
     */
    private void commitBatch() {
        synchronized (commitLock) {
            Batch b;
            synchronized (batchLock) {
                if (batch.isEmpty()) {
                    return;
                }
                b = batch;
                batch = new Batch();
                committing = b;
            }
            boolean committed = false;
            try {
                translogManager().syncTranslog();
                if (!b.created.isEmpty() || !b.replayed.isEmpty()) {
                    // Each of these ids was written to the skeleton before it
                    // entered the batch, so the refreshed reader has them all.
                    super.refresh(COMMIT_REFRESH);
                }
                commit(b);
                committed = true;
            } catch (IOException e) {
                throw new UncheckedIOException("[infino] syncing the translog of " + shardId + " before a commit to " + table + " failed", e);
            } finally {
                synchronized (batchLock) {
                    committing = null;
                    if (!committed) {
                        keepForNextCommit(b);
                    }
                }
            }
        }
    }

    /**
     * Fold a batch whose commit failed back under the open one, so its
     * writes go with the next commit: a write the open batch holds for the
     * same id is newer and wins. Its rows are looked up then, since a
     * failed commit may have reached the table in part.
     */
    private void keepForNextCommit(Batch failed) {
        Batch next = batch;
        for (Map.Entry<String, String> w : failed.writes.entrySet()) {
            String id = w.getKey();
            if (!next.writes.containsKey(id) && !next.deleted.contains(id)) {
                next.write(id, w.getValue(), failed.routings.get(id));
                next.replayed.add(id);
            }
        }
        for (String id : failed.deleted) {
            if (!next.writes.containsKey(id)) {
                next.deleted.add(id);
            }
        }
    }

    private void commit(Batch b) {
        long start = System.nanoTime();
        Set<String> replaced = new HashSet<>(b.replaced);
        if (!b.replayed.isEmpty()) {
            replaced.addAll(InfinoNative.existingOsids(table, b.replayed));
        }
        Set<String> deleted = new LinkedHashSet<>(b.deleted);
        if (!b.writes.isEmpty()) {
            List<String> ids = new ArrayList<>(b.writes.keySet());
            List<String> sources = new ArrayList<>(b.writes.values());
            Pointer prepared;
            if (b.routings.isEmpty()) {
                prepared = InfinoNative.prepare(table, ids, sources);
            } else {
                List<String> routings = new ArrayList<>(ids.size());
                for (String id : ids) {
                    routings.add(b.routings.get(id));
                }
                prepared = InfinoNative.prepare(table, ids, sources, routings);
            }
            if (prepared == null) {
                throw new IllegalStateException("[infino] the bridge could not prepare a batch for " + table);
            }
            InfinoNative.PreparedResult result = InfinoNative.appendPrepared(table, List.of(prepared), replaced);
            for (InfinoNative.PreparedFailure f : result.failed()) {
                log.warn("[infino] table {} refused document {} after Lucene acknowledged it: {}", table, ids.get(f.index()), f.reason());
            }
        }
        if (!deleted.isEmpty()) {
            int rc = InfinoNative.deleteOsids(table, deleted.toArray(new String[0]), deleted.size());
            if (rc != 0) {
                throw new IllegalStateException("[infino] delete of " + deleted.size() + " ids from " + table + " returned " + rc);
            }
        }
        Set<String> touched = new HashSet<>(b.writes.keySet());
        touched.addAll(deleted);
        InfinoNative.forgetSources(table, touched);
        log.debug("[infino] engine committed {} writes and {} deletes to {} in {} ms", b.writes.size(), deleted.size(), table, (System.nanoTime() - start) / 1_000_000L);
    }

    /**
     * Commit pending writes before Lucene's refresh or force merge. A failure
     * is logged and Lucene's step still runs: failing it would fail the shard
     * for writes the translog holds and a later commit takes.
     */
    private void commitBeforeLucene(String why) {
        try {
            commitBatch();
        } catch (RuntimeException e) {
            log.warn("[infino] committing to {} on {} failed", table, why, e);
        }
    }

    @Override
    public void refresh(String source) throws EngineException {
        commitBeforeLucene("refresh");
        super.refresh(source);
    }

    @Override
    public boolean maybeRefresh(String source) throws EngineException {
        commitBeforeLucene("refresh");
        return super.maybeRefresh(source);
    }

    /**
     * A flush trims the translog, so the writes it holds must be in the table
     * first: a commit that fails fails the flush, and the translog keeps
     * them for the next.
     */
    @Override
    public void flush(boolean force, boolean waitIfOngoing) throws EngineException {
        try {
            commitBatch();
        } catch (RuntimeException e) {
            throw new FlushFailedEngineException(shardId, e);
        }
        super.flush(force, waitIfOngoing);
    }

    @Override
    public void forceMerge(
        boolean flush,
        int maxNumSegments,
        boolean onlyExpungeDeletes,
        boolean upgrade,
        boolean upgradeOnlyAncientSegments,
        String forceMergeUUID
    ) throws EngineException, IOException {
        // A force-merge is the operator asking for fewer, larger files: compact the table too.
        commitBeforeLucene("force merge");
        int rc = InfinoNative.optimize(table);
        if (rc != 0) {
            log.warn("[infino] optimize of {} returned {}", table, rc);
        }
        super.forceMerge(flush, maxNumSegments, onlyExpungeDeletes, upgrade, upgradeOnlyAncientSegments, forceMergeUUID);
    }

    /**
     * The final flush trims the translog too: when the final commit fails,
     * the shard closes without it, so its translog keeps the writes the
     * table lacks and the next start replays them into the table.
     */
    @Override
    public void flushAndClose() throws IOException {
        try {
            commitBatch();
        } catch (RuntimeException e) {
            log.warn("[infino] committing to {} on close failed; closing without the final flush, so the translog keeps the writes", table, e);
            close();
            return;
        }
        super.flushAndClose();
    }
}
