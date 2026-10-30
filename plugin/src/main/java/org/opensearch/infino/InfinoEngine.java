/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import com.sun.jna.Pointer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.index.engine.EngineConfig;
import org.opensearch.index.engine.EngineCreationFailureException;
import org.opensearch.index.engine.EngineException;
import org.opensearch.index.engine.InternalEngine;
import org.opensearch.index.mapper.ParseContext.Document;
import org.opensearch.index.mapper.SourceFieldMapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

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
 * <p>Writes reach the table in batches. A batch commits on refresh, so a
 * document becomes visible in the table when Lucene's skeleton does, and on
 * flush, before the translog that holds it is trimmed; a batch past its
 * document or byte bound commits on its own. The engine knows which writes
 * replace an existing document (the version map says so), so those rows
 * replace their old ones in the same commit without a lookup; a write
 * replayed from the translog, which may or may not have reached the table
 * before a restart, is looked up. Only a primary writes the table: one
 * table has one writer, and a replica's documents are the primary's. Each
 * shard has its own table (see {@link InfinoTables}), so an index of
 * several shards has one writer per table too.
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

    /** The writes waiting for the next commit, folded by id. */
    private static final class Batch {
        /** The latest source for each id written, in arrival order. */
        final Map<String, String> writes = new LinkedHashMap<>();
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
    }

    private final String table;
    private final Object batchLock = new Object();
    /** Serializes commits, so one batch reaches the table before the next. */
    private final Object commitLock = new Object();
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

    public InfinoEngine(EngineConfig config) {
        super(config);
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

    @Override
    public void close() throws IOException {
        BY_TABLE.remove(table, this);
        super.close();
    }

    @Override
    public IndexResult index(Index index) throws IOException {
        String source = index.source() == null
            ? null
            : InfinoBulkActionFilter.sourceAsJson(index.source(), index.parsedDoc() == null ? null : index.parsedDoc().getMediaType());
        // Lucene keeps the document's identity only: the metadata fields the
        // engine and the fetch phase read, not the user fields or the source.
        for (Document d : index.docs()) {
            d.getFields().removeIf(f -> !f.name().startsWith("_") || SourceFieldMapper.NAME.equals(f.name()));
        }
        IndexResult result = super.index(index);
        if (result.getResultType() != Result.Type.SUCCESS || index.origin() == Operation.Origin.REPLICA || source == null) {
            return result;
        }
        boolean full;
        synchronized (batchLock) {
            Batch b = batch;
            String id = index.id();
            boolean pendingCreate = b.created.contains(id);
            b.writes.put(id, source);
            b.deleted.remove(id);
            if (index.origin().isRecovery()) {
                b.replayed.add(id);
            } else if (result.isCreated()) {
                b.created.add(id);
                b.replaced.remove(id);
            } else if (!pendingCreate) {
                b.replaced.add(id);
            }
            b.bytes += source.length();
            full = b.writes.size() >= MAX_BATCH_DOCS || b.bytes >= MAX_BATCH_BYTES;
        }
        if (full) {
            commitBatch();
        }
        return result;
    }

    @Override
    public DeleteResult delete(Delete delete) throws IOException {
        DeleteResult result = super.delete(delete);
        if (result.getResultType() != Result.Type.SUCCESS || delete.origin() == Operation.Origin.REPLICA || !result.isFound()) {
            return result;
        }
        synchronized (batchLock) {
            Batch b = batch;
            String id = delete.id();
            b.writes.remove(id);
            b.replaced.remove(id);
            b.replayed.remove(id);
            if (!b.created.remove(id)) {
                b.deleted.add(id); // the table holds a row for it
            }
        }
        return result;
    }

    /** Commit what is pending to the table; batches reach it in the order they were taken. */
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
            try {
                commit(b);
            } finally {
                synchronized (batchLock) {
                    committing = null;
                }
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
            Pointer prepared = InfinoNative.prepare(table, ids, sources);
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

    /** Commit pending writes before Lucene makes its skeleton visible or durable. */
    private void commitBeforeLucene(String why) {
        try {
            commitBatch();
        } catch (RuntimeException e) {
            // Lucene's own step still runs: failing it would fail the shard
            // for a write the translog holds and a later commit replays.
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

    @Override
    public void flush(boolean force, boolean waitIfOngoing) throws EngineException {
        commitBeforeLucene("flush");
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

    @Override
    public void flushAndClose() throws IOException {
        commitBeforeLucene("close");
        super.flushAndClose();
    }
}
