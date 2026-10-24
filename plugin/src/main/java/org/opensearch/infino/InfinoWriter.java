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
import org.opensearch.action.support.replication.ReplicationResponse;
import org.opensearch.action.update.UpdateRequest;
import org.opensearch.action.update.UpdateResponse;
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
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Writes to one table, batched across bulk requests.
 *
 * <p>Each engine append is a commit, and a bulk is acknowledged after the
 * append that carries its writes, so an acknowledged item is durable in the
 * table. To keep that without a commit per request, writes group-commit: a
 * bulk that arrives while no append is running starts one at once; bulks
 * that arrive while an append is running accumulate and go to the table as
 * the next append the moment it finishes. A client sending one bulk at a
 * time therefore waits only for its own append; clients sending in parallel
 * (every log shipper does) share appends, and the batch is bounded by a
 * document and a byte threshold. The append runs on the write pool, never
 * on the thread that took the request.
 *
 * <p>Within a batch a later write of an id replaces an earlier one, and a
 * delete cancels a pending write. Before the append, one statement over the
 * id column's term index names the ids the table already holds; those are
 * tombstoned in one mutation, so an append of new documents (the shape of a
 * log stream) pays no lookup.
 */
public final class InfinoWriter {

    private static final Logger log = LogManager.getLogger(InfinoWriter.class);
    /** Documents a batch may hold while an append runs; past this, arriving bulks wait for the next append to be taken. */
    private static final int MAX_BATCH_DOCS = Integer.getInteger("infino.batch.docs", 50_000);
    /** Source bytes a batch may hold while an append runs. */
    private static final long MAX_BATCH_BYTES = Long.getLong("infino.batch.bytes", 64L << 20);
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

    /** The pending batch of one table, and whether an append is running for it. */
    private static final class Batch {
        final Lock lock = new ReentrantLock();
        /** Signalled whenever the pending writes are taken by an append, for bulks held back by the bound. */
        final Condition taken = lock.newCondition();
        final Map<String, Write> writes = new LinkedHashMap<>();
        final List<Waiter> waiters = new ArrayList<>();
        long bytes = 0;
        boolean flushing = false;
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
        batch.lock.lock();
        try {
            Write w = batch.writes.get(id);
            return w == null ? null : w.source();
        } finally {
            batch.lock.unlock();
        }
    }

    /**
     * Stage one bulk's writes to a table. Items answered up front carry their
     * response; the rest are answered when their append commits, and the
     * listener gets the whole bulk's response then.
     */
    void submit(String table, Waiter waiter) {
        Batch batch = batches.computeIfAbsent(table, t -> new Batch());
        boolean start;
        batch.lock.lock();
        try {
            // A batch past its bound waits for the running append to take it
            // before more is staged behind it.
            while (batch.flushing && (batch.writes.size() >= MAX_BATCH_DOCS || batch.bytes >= MAX_BATCH_BYTES)) {
                try {
                    batch.taken.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    waiter.listener().onFailure(e);
                    return;
                }
            }
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
            start = !batch.flushing;
            if (start) {
                batch.flushing = true;
            }
        } finally {
            batch.lock.unlock();
        }
        if (start) {
            threadPool.executor(ThreadPool.Names.WRITE).execute(() -> flush(table, batch));
        }
    }

    /** Append what is pending and answer the bulks it carries; repeat while more arrived meanwhile. */
    private void flush(String table, Batch batch) {
        while (true) {
            Map<String, Write> writes;
            List<Waiter> waiters;
            batch.lock.lock();
            try {
                if (batch.waiters.isEmpty()) {
                    batch.flushing = false;
                    batch.taken.signalAll();
                    return;
                }
                writes = new LinkedHashMap<>(batch.writes);
                waiters = new ArrayList<>(batch.waiters);
                batch.writes.clear();
                batch.waiters.clear();
                batch.bytes = 0;
                batch.taken.signalAll();
            } finally {
                batch.lock.unlock();
            }
            try {
                Applied applied = apply(table, writes);
                for (Waiter w : waiters) {
                    answer(w, applied);
                }
            } catch (Exception e) {
                log.warn("[infino] append of {} writes to {} failed", writes.size(), table, e);
                for (Waiter w : waiters) {
                    w.listener().onFailure(e);
                }
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
