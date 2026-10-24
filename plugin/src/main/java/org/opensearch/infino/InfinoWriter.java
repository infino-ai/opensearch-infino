/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.DocWriteRequest;
import org.opensearch.action.DocWriteResponse;
import org.opensearch.action.bulk.BulkItemResponse;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.delete.DeleteResponse;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.action.support.replication.ReplicationResponse;
import org.opensearch.action.update.UpdateRequest;
import org.opensearch.action.update.UpdateResponse;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Writes to one table, batched across bulk requests.
 *
 * <p>Each engine append is a commit, so a bulk that became its own append
 * would commit once per request and leave a stream of small superfiles. The
 * writes of consecutive bulks accumulate instead and go to the table as one
 * append when the batch reaches a document or byte threshold, when a
 * request asks for a refresh, or when the flush delay since the first
 * pending write elapses. A bulk is acknowledged after the append that
 * carries its writes, so an acknowledged item is durable in the table; the
 * flush runs on the write pool, never on the thread that took the request.
 *
 * <p>Within a batch a later write of an id replaces an earlier one, and a
 * delete cancels a pending write. Before the append, one statement over the
 * id column's term index names the ids the table already holds; those are
 * tombstoned in one mutation, so an append of new documents (the shape of a
 * log stream) pays no lookup.
 */
public final class InfinoWriter {

    private static final Logger log = LogManager.getLogger(InfinoWriter.class);
    /** Documents per append before the batch flushes on its own. */
    private static final int MAX_BATCH_DOCS = Integer.getInteger("infino.batch.docs", 50_000);
    /** Source bytes per append before the batch flushes on its own. */
    private static final long MAX_BATCH_BYTES = Long.getLong("infino.batch.bytes", 64L << 20);
    /** How long the first pending write of a batch waits for company before the batch flushes. */
    private static final TimeValue FLUSH_DELAY = TimeValue.timeValueMillis(Long.getLong("infino.batch.delay_ms", 1_000L));
    /** Version reported for every write; the table keeps no per-document version. */
    private static final long VERSION = 1L;
    /** Sequence number and primary term reported for every write; the table keeps neither. */
    private static final long SEQ_NO = SequenceNumbers.UNASSIGNED_SEQ_NO;
    private static final long PRIMARY_TERM = SequenceNumbers.UNASSIGNED_PRIMARY_TERM;

    /** One write a bulk item asks for: the source to append (null for a delete) and whether the id may already exist. */
    record Write(String id, String source, boolean lookup) {}

    /** What one item of a bulk became: answered up front (refused), or by its write when the batch flushes. */
    record Outcome(int slot, DocWriteRequest<?> item, Write write, BulkItemResponse answered) {}

    /** One bulk's share of a batch: its outcomes and the listener that gets its response. */
    record Waiter(ShardId shardId, String indexName, List<Outcome> outcomes, BulkItemResponse[] items, long startNanos, ActionListener<BulkResponse> listener) {}

    /** What a flush did: the ids the engine refused with the reason, and the ids that already existed. */
    private record Applied(Map<String, String> refused, Set<String> existed) {}

    /** The pending batch of one table. */
    private static final class Batch {
        final Map<String, Write> writes = new LinkedHashMap<>();
        final List<Waiter> waiters = new ArrayList<>();
        long bytes = 0;
        boolean scheduled = false;
    }

    private final ThreadPool threadPool;
    private final Map<String, Batch> batches = new ConcurrentHashMap<>();

    public InfinoWriter(ThreadPool threadPool) {
        this.threadPool = threadPool;
    }

    /** The source a pending write holds for an id, so an update in the same batch merges into it. */
    String pendingSource(String table, String id) {
        Batch batch = batches.get(table);
        if (batch == null) {
            return null;
        }
        synchronized (batch) {
            Write w = batch.writes.get(id);
            return w == null ? null : w.source();
        }
    }

    /**
     * Stage one bulk's writes to a table. Items answered up front carry their
     * response; the rest are answered when the batch flushes, and the
     * listener gets the whole bulk's response then.
     */
    void submit(String table, Waiter waiter, WriteRequest.RefreshPolicy refresh) {
        Batch batch = batches.computeIfAbsent(table, t -> new Batch());
        boolean flushNow;
        synchronized (batch) {
            for (Outcome o : waiter.outcomes()) {
                if (o.write() == null) {
                    continue;
                }
                Write previous = batch.writes.put(o.write().id(), o.write());
                if (previous != null && previous.source() != null) {
                    batch.bytes -= previous.source().length();
                }
                if (o.write().source() != null) {
                    batch.bytes += o.write().source().length();
                }
            }
            batch.waiters.add(waiter);
            flushNow = refresh != WriteRequest.RefreshPolicy.NONE
                || batch.writes.size() >= MAX_BATCH_DOCS
                || batch.bytes >= MAX_BATCH_BYTES;
            if (!flushNow && !batch.scheduled) {
                batch.scheduled = true;
                threadPool.schedule(() -> flush(table), FLUSH_DELAY, ThreadPool.Names.WRITE);
            }
        }
        if (flushNow) {
            threadPool.executor(ThreadPool.Names.WRITE).execute(() -> flush(table));
        }
    }

    /** Every pending write of the table goes to it now, and every waiting bulk is answered. */
    void flush(String table) {
        Batch batch = batches.get(table);
        if (batch == null) {
            return;
        }
        Map<String, Write> writes;
        List<Waiter> waiters;
        synchronized (batch) {
            batch.scheduled = false;
            if (batch.writes.isEmpty() && batch.waiters.isEmpty()) {
                return;
            }
            writes = new LinkedHashMap<>(batch.writes);
            waiters = new ArrayList<>(batch.waiters);
            batch.writes.clear();
            batch.waiters.clear();
            batch.bytes = 0;
        }
        try {
            Applied applied = apply(table, writes);
            for (Waiter w : waiters) {
                answer(w, applied);
            }
        } catch (Exception e) {
            log.warn("[infino] flush of {} writes to {} failed", writes.size(), table, e);
            for (Waiter w : waiters) {
                w.listener().onFailure(e);
            }
        }
    }

    /** Tombstone what exists, append the rest. */
    private static Applied apply(String table, Map<String, Write> writes) {
        Set<String> lookups = new HashSet<>();
        for (Write w : writes.values()) {
            if (w.lookup()) {
                lookups.add(w.id());
            }
        }
        Set<String> existed = lookups.isEmpty() ? Set.of() : InfinoNative.existingOsids(table, lookups);
        List<String> tombstones = new ArrayList<>();
        List<String> ids = new ArrayList<>();
        List<String> sources = new ArrayList<>();
        for (Write w : writes.values()) {
            if (existed.contains(w.id())) {
                tombstones.add(w.id());
            }
            if (w.source() != null) {
                ids.add(w.id());
                sources.add(w.source());
            }
        }
        if (!tombstones.isEmpty()) {
            int rc = InfinoNative.deleteOsids(table, tombstones.toArray(new String[0]), tombstones.size());
            if (rc != 0) {
                throw new IllegalStateException("[infino] delete of " + tombstones.size() + " ids from " + table + " returned " + rc);
            }
        }
        Map<String, String> refused = new HashMap<>();
        if (!ids.isEmpty()) {
            InfinoNative.BatchResult result = InfinoNative.indexBatch(table, ids.toArray(new String[0]), sources.toArray(new String[0]), ids.size());
            for (InfinoNative.Failure f : result.failed()) {
                refused.put(ids.get(f.index()), f.reason());
            }
            log.debug("[infino] appended {} of {} documents to {} ({} tombstoned first)", result.ok(), ids.size(), table, tombstones.size());
        }
        return new Applied(refused, existed);
    }

    /** One bulk's response from what its batch did. */
    private static void answer(Waiter w, Applied applied) {
        for (Outcome o : w.outcomes()) {
            if (o.answered() != null) {
                w.items()[o.slot()] = o.answered();
                continue;
            }
            Write write = o.write();
            String why = applied.refused().get(write.id());
            if (why != null) {
                w.items()[o.slot()] = new BulkItemResponse(
                    o.slot(),
                    o.item().opType(),
                    new BulkItemResponse.Failure(w.indexName(), write.id(), new IllegalArgumentException("[infino] " + why), RestStatus.BAD_REQUEST)
                );
                continue;
            }
            boolean existed = applied.existed().contains(write.id());
            DocWriteResponse r;
            if (write.source() == null) {
                r = new DeleteResponse(w.shardId(), write.id(), SEQ_NO, PRIMARY_TERM, VERSION, existed);
            } else if (o.item() instanceof UpdateRequest) {
                r = new UpdateResponse(w.shardId(), write.id(), SEQ_NO, PRIMARY_TERM, VERSION, existed ? DocWriteResponse.Result.UPDATED : DocWriteResponse.Result.CREATED);
            } else {
                r = new IndexResponse(w.shardId(), write.id(), SEQ_NO, PRIMARY_TERM, VERSION, !existed);
            }
            r.setShardInfo(new ReplicationResponse.ShardInfo(1, 1));
            w.items()[o.slot()] = new BulkItemResponse(o.slot(), o.item().opType(), r);
        }
        w.listener().onResponse(new BulkResponse(w.items(), (System.nanoTime() - w.startNanos()) / 1_000_000L));
    }
}
