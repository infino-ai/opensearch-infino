/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import com.sun.jna.Pointer;

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
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.index.engine.VersionConflictEngineException;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
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
 * <p>A bulk's documents are parsed, conformed and converted to Arrow on the
 * thread that took the request, before they join the batch, so that work
 * overlaps the append that is running; the append itself then only
 * commits the prepared batches. When an id is written by more than one
 * bulk of a batch, or a delete targets an id a bulk of the batch writes,
 * the batch takes the slower path that folds the writes by id first.
 *
 * <p>Before the append, one statement over the id column's term index
 * names the ids the table already holds; those are tombstoned in one
 * mutation, so an append of new documents (the shape of a log stream) pays
 * no lookup.
 *
 * <p>Each write is given the next sequence number of its table when its
 * batch is written, in the order the batch staged it, and the table stores
 * the number beside the row. A table's flushes run one at a time on this
 * node, so the numbers rise in the order the writes commit. One node writes
 * a table, the one holding its index's primary shard (see
 * {@link InfinoBulkActionFilter#dispatch}); when that copy changes (the
 * primary moves, or another copy takes over) the next number is read from
 * the table again, so a number given elsewhere meanwhile is never given
 * twice. A write
 * conditioned on a sequence number is checked then, against the number the
 * table stores for its id or the one the write before it in the batch was
 * given (see {@link InfinoDocumentVersion}).
 */
public final class InfinoWriter {

    private static final Logger log = LogManager.getLogger(InfinoWriter.class);
    /** Documents a batch may hold while an append runs; past this, arriving bulks wait for the next append to be taken. */
    private static final int MAX_BATCH_DOCS = Integer.getInteger("infino.batch.docs", 50_000);
    /** Source bytes a batch may hold while an append runs. */
    private static final long MAX_BATCH_BYTES = Long.getLong("infino.batch.bytes", 64L << 20);
    /** Version reported for every write; the table keeps no per-document version. */
    private static final long VERSION = InfinoDocumentVersion.VERSION;
    /** Primary term reported for every write; the table has no primary to fail over. */
    private static final long PRIMARY_TERM = InfinoDocumentVersion.PRIMARY_TERM;
    /** A table's next sequence number before its first flush on this node has read the highest the table holds. */
    private static final long UNREAD = -1L;

    /**
     * One write a bulk item asks for: the source to append (null for a
     * delete), whether the id may already exist (looked up at flush), and
     * whether it must not ({@code op_type: create}, answered with a conflict
     * when it does).
     */
    record Write(String id, String source, boolean lookup, boolean create) {
        Write(String id, String source, boolean lookup) {
            this(id, source, lookup, false);
        }
    }

    /** What one item of a bulk became: answered up front (refused), or by its write when the batch flushes. */
    record Outcome(int slot, DocWriteRequest<?> item, Write write, BulkItemResponse answered) {}

    /** One bulk's share of a batch: its outcomes and the listener that gets its response. */
    record Waiter(ShardId shardId, String indexName, List<Outcome> outcomes, BulkItemResponse[] items, long startNanos, ActionListener<BulkResponse> listener) {}

    /** A bulk in its batch: the waiter, its prepared batch (null when it writes nothing), and the ids of the prepared rows in order. */
    private record Staged(Waiter waiter, Pointer prepared, List<String> preparedIds, long bytes) {}

    /**
     * What one write became when its batch was written: the sequence number
     * it was given, whether a document stood under its id just before it, and
     * the conflict that kept it out of the write (null when it was written).
     */
    private record Written(long seq, boolean existed, Exception conflict) {}

    /**
     * What a flush did: the ids the engine refused with the reason, what each
     * write became, and the ids whose delete failed after the append had
     * committed, with why (empty and null when every delete landed).
     */
    private record Applied(Map<String, String> refused, Map<Outcome, Written> written, Set<String> undeleted, Exception deleteFailure) {}

    /** Whether an outcome carries a row to append: its prepared batch holds these, in the bulk's order. */
    private static boolean carriesRow(Outcome o) {
        return o.write() != null && o.write().source() != null;
    }

    /** The pending batch of one table, and whether an append is running for it. */
    private static final class Batch {
        final Lock lock = new ReentrantLock();
        /** Signalled whenever the pending writes are taken by an append, for bulks held back by the bound. */
        final Condition taken = lock.newCondition();
        final List<Staged> staged = new ArrayList<>();
        /** The latest source pending for each id, for an update in the same batch to merge into. */
        final Map<String, String> pendingSources = new HashMap<>();
        int docs = 0;
        long bytes = 0;
        boolean flushing = false;
        /**
         * The sequence number the next write is given; {@link InfinoWriter#UNREAD}
         * until the first flush reads the table's highest. Only the running flush
         * touches it, and flushes take turns through {@link #flushing}, set under
         * {@link #lock}.
         */
        long nextSeq = UNREAD;
        /** The copy that wrote the table when {@link #nextSeq} was read; another copy since means it is read again. */
        String seqAllocation;
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
            return batch.pendingSources.get(id);
        } finally {
            batch.lock.unlock();
        }
    }

    /**
     * Stage one bulk's writes to a table: its documents are prepared here,
     * on the caller's thread, then join the batch. Items answered up front
     * carry their response; the rest are answered when their append
     * commits, and the listener gets the whole bulk's response then.
     */
    void submit(String table, Waiter waiter) {
        List<String> ids = new ArrayList<>();
        List<String> sources = new ArrayList<>();
        long bytes = 0;
        for (Outcome o : waiter.outcomes()) {
            if (carriesRow(o)) {
                ids.add(o.write().id());
                sources.add(o.write().source());
                bytes += o.write().source().length();
            }
        }
        Pointer prepared = ids.isEmpty() ? null : InfinoNative.prepare(table, ids, sources);
        if (!ids.isEmpty() && prepared == null) {
            waiter.listener().onFailure(new IllegalStateException("[infino] the bridge could not prepare the batch for " + table));
            return;
        }
        Staged staged = new Staged(waiter, prepared, ids, bytes);

        Batch batch = batches.computeIfAbsent(table, t -> new Batch());
        boolean start;
        batch.lock.lock();
        try {
            // A batch past its bound waits for the running append to take it
            // before more is staged behind it.
            while (batch.flushing && (batch.docs >= MAX_BATCH_DOCS || batch.bytes >= MAX_BATCH_BYTES)) {
                try {
                    batch.taken.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    InfinoNative.freePrepared(prepared);
                    waiter.listener().onFailure(e);
                    return;
                }
            }
            batch.staged.add(staged);
            batch.docs += ids.size();
            batch.bytes += bytes;
            for (int i = 0; i < ids.size(); i++) {
                batch.pendingSources.put(ids.get(i), sources.get(i));
            }
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
            List<Staged> staged;
            batch.lock.lock();
            try {
                if (batch.staged.isEmpty()) {
                    batch.flushing = false;
                    batch.taken.signalAll();
                    return;
                }
                staged = new ArrayList<>(batch.staged);
                batch.staged.clear();
                batch.pendingSources.clear();
                batch.docs = 0;
                batch.bytes = 0;
                batch.taken.signalAll();
            } finally {
                batch.lock.unlock();
            }
            // The prepared handles are owned by apply(): it hands them to the
            // engine, which releases them whatever the outcome, or releases
            // them itself when it fails before that.
            try {
                Applied applied = apply(table, batch, staged);
                for (Staged s : staged) {
                    answer(s.waiter(), applied);
                }
            } catch (Exception e) {
                log.warn("[infino] append of {} bulks to {} failed", staged.size(), table, e);
                for (Staged s : staged) {
                    s.waiter().listener().onFailure(e);
                }
            }
        }
    }

    /**
     * Write the batch: the rows of ids the table already holds replace
     * those rows in the same mutation as the append (so a failed write
     * leaves the old rows), ids whose last write is a delete are tombstoned
     * in a mutation after it (the engine replaces rows one for one, so a
     * delete with no row in its place is a mutation of its own), and a write
     * that meets a conflict (a create over a document, or a sequence-number
     * condition the document does not meet) is left out.
     *
     * <p>Owns the prepared handles. The engine releases every handle it is
     * handed, on success and on failure alike, and the folded path releases
     * them before it prepares afresh; so a handle is released here only when
     * this fails before either, on the reads of the table that come first.
     * Releasing one twice corrupts the native heap.
     */
    private static Applied apply(String table, Batch batch, List<Staged> staged) {
        Set<String> lookups = new HashSet<>();
        boolean conditioned = false;
        for (Staged s : staged) {
            for (Outcome o : s.waiter().outcomes()) {
                if (o.write() == null) {
                    continue;
                }
                if (o.write().lookup()) {
                    lookups.add(o.write().id());
                }
                conditioned |= InfinoDocumentVersion.seqConditioned(o.item());
            }
        }
        Set<String> existed;
        Map<String, Long> stored;
        try {
            String allocation = writerAllocation(staged);
            if (batch.nextSeq == UNREAD || !Objects.equals(allocation, batch.seqAllocation)) {
                batch.nextSeq = InfinoNative.maxSeq(table) + 1;
                batch.seqAllocation = allocation;
            }
            // A conditioned write is checked against the sequence numbers the table
            // stores, read in the same probe as which ids it holds; a batch without
            // one reads only which ids it holds.
            if (lookups.isEmpty()) {
                stored = Map.of();
                existed = Set.of();
            } else if (conditioned) {
                stored = InfinoNative.storedSeqs(table, lookups);
                existed = stored.keySet();
            } else {
                stored = Map.of();
                existed = InfinoNative.existingOsids(table, lookups);
            }
        } catch (RuntimeException e) {
            // The handles never reached the engine: released here, once.
            for (Staged s : staged) {
                InfinoNative.freePrepared(s.prepared());
            }
            throw e;
        }
        // Each write in the order the batch staged it, against its document as
        // the writes before it left it: a conflict is left out of the write, and
        // every other write is given the next sequence number.
        Map<Outcome, Written> written = new IdentityHashMap<>();
        // The sequence number each id's document stands at after the writes so far; null once deleted.
        Map<String, Long> standing = new HashMap<>();
        // The last write of each id that goes to the table.
        Map<String, Outcome> last = new LinkedHashMap<>();
        boolean folded = false;
        for (Staged s : staged) {
            ShardId shardId = s.waiter().shardId();
            for (Outcome o : s.waiter().outcomes()) {
                Write w = o.write();
                if (w == null) {
                    continue;
                }
                Long current = standing.containsKey(w.id())
                    ? standing.get(w.id())
                    : existed.contains(w.id()) ? stored.getOrDefault(w.id(), InfinoDocumentVersion.UNTRACKED_SEQ_NO) : null;
                Exception conflict = w.create() && current != null
                    ? new VersionConflictEngineException(shardId, w.id(), "document already exists")
                    : InfinoDocumentVersion.conflict(shardId, o.item(), current);
                if (conflict != null) {
                    written.put(o, new Written(SequenceNumbers.UNASSIGNED_SEQ_NO, current != null, conflict));
                    folded = true; // the prepared batches carry its row; only the folded path leaves it out
                    continue;
                }
                long seq = batch.nextSeq++;
                written.put(o, new Written(seq, current != null, null));
                standing.put(w.id(), w.source() == null ? null : seq);
                if (last.put(w.id(), o) != null) {
                    folded = true; // the same id twice in one batch: written twice, or written and deleted
                }
            }
        }
        List<String> replaced = new ArrayList<>();
        List<String> tombstones = new ArrayList<>();
        for (Outcome o : last.values()) {
            Write w = o.write();
            if (!existed.contains(w.id())) {
                continue; // a new id: its row is appended, and there is nothing to replace
            }
            if (w.source() == null) {
                tombstones.add(w.id());
            } else {
                replaced.add(w.id());
            }
        }
        Map<String, String> refused;
        try {
            // Both paths release every handle: the engine does on the prepared
            // path, the folded path up front. Nothing after this may release one.
            refused = folded ? appendFolded(table, staged, last.values(), written, replaced) : appendPrepared(table, staged, written, replaced);
        } finally {
            // A read after the write must see the table, not a prefetched copy.
            InfinoNative.forgetSources(table, last.keySet());
        }
        // The deletes are a mutation of their own after the append, which has
        // committed by now: when they fail, the appended writes are answered as
        // written and only the deletes as failed, so a client retries just those,
        // and a delete retried removes what it names.
        if (!tombstones.isEmpty()) {
            int rc = InfinoNative.deleteOsids(table, tombstones.toArray(new String[0]), tombstones.size());
            if (rc != 0) {
                IllegalStateException failed = new IllegalStateException("[infino] delete of " + tombstones.size() + " ids from " + table + " returned " + rc);
                log.warn("[infino] the batch's writes to {} committed, its deletes did not", table, failed);
                return new Applied(refused, written, Set.copyOf(tombstones), failed);
            }
        }
        return new Applied(refused, written, Set.of(), null);
    }

    /** The copy of the batch's index that writes its table, as this node's cluster state names it; null when unknown. */
    private static String writerAllocation(List<Staged> staged) {
        ClusterService clusters = InfinoServices.clusterService();
        if (clusters == null || staged.isEmpty()) {
            return null;
        }
        return InfinoBulkActionFilter.writerAllocation(clusters.state(), staged.get(0).waiter().shardId().getIndex());
    }

    /** The prepared batches as one commit, each row under the sequence number its write was given; a refused document is reported under its id. */
    private static Map<String, String> appendPrepared(String table, List<Staged> staged, Map<Outcome, Written> written, List<String> replaced) {
        List<Pointer> handles = new ArrayList<>();
        List<Staged> withRows = new ArrayList<>();
        int docs = 0;
        for (Staged s : staged) {
            if (s.prepared() != null) {
                handles.add(s.prepared());
                withRows.add(s);
                docs += s.preparedIds().size();
            }
        }
        Map<String, String> refused = new HashMap<>();
        if (handles.isEmpty()) {
            return refused;
        }
        long[] seqs = new long[docs];
        int at = 0;
        for (Staged s : withRows) {
            for (Outcome o : s.waiter().outcomes()) {
                if (carriesRow(o)) {
                    seqs[at++] = written.get(o).seq();
                }
            }
        }
        long start = System.nanoTime();
        InfinoNative.PreparedResult result = InfinoNative.appendPrepared(table, handles, replaced, seqs);
        long millis = (System.nanoTime() - start) / 1_000_000L;
        for (InfinoNative.PreparedFailure f : result.failed()) {
            refused.put(withRows.get(f.batch()).preparedIds().get(f.index()), f.reason());
        }
        log.debug("[infino] appended {} of {} documents to {} in {} ms ({} bulks)", result.ok(), docs, table, millis, handles.size());
        return refused;
    }

    /**
     * The batch's last write of each id, prepared afresh and written as one
     * mutation under the sequence number it was given (a delete writes no
     * row): the path for a batch in which one id appears twice, or a write
     * meets a conflict and is left out.
     */
    private static Map<String, String> appendFolded(String table, List<Staged> staged, Collection<Outcome> last, Map<Outcome, Written> written, List<String> replaced) {
        for (Staged s : staged) {
            InfinoNative.freePrepared(s.prepared());
        }
        List<String> ids = new ArrayList<>();
        List<String> sources = new ArrayList<>();
        List<Long> seqs = new ArrayList<>();
        for (Outcome o : last) {
            if (carriesRow(o)) {
                ids.add(o.write().id());
                sources.add(o.write().source());
                seqs.add(written.get(o).seq());
            }
        }
        Map<String, String> refused = new HashMap<>();
        if (ids.isEmpty()) {
            return refused;
        }
        Pointer prepared = InfinoNative.prepare(table, ids, sources);
        if (prepared == null) {
            throw new IllegalStateException("[infino] the bridge could not prepare the folded batch for " + table);
        }
        long[] numbers = seqs.stream().mapToLong(Long::longValue).toArray();
        InfinoNative.PreparedResult result = InfinoNative.appendPrepared(table, List.of(prepared), replaced, numbers);
        for (InfinoNative.PreparedFailure f : result.failed()) {
            refused.put(ids.get(f.index()), f.reason());
        }
        log.debug("[infino] appended {} of {} documents to {} (folded: an id appeared twice in the batch, or a write met a conflict)", result.ok(), ids.size(), table);
        return refused;
    }

    /** One bulk's response from what its batch did. */
    private static void answer(Waiter w, Applied applied) {
        for (Outcome o : w.outcomes()) {
            if (o.answered() != null) {
                w.items()[o.slot()] = o.answered();
                continue;
            }
            Write write = o.write();
            Written done = applied.written().get(o);
            if (done.conflict() != null) {
                // A create over a document, or a condition the document does not meet: the conflict stock reports, and no row written.
                w.items()[o.slot()] = new BulkItemResponse(
                    o.slot(),
                    o.item().opType(),
                    new BulkItemResponse.Failure(w.indexName(), write.id(), done.conflict(), RestStatus.CONFLICT)
                );
                continue;
            }
            if (write.source() == null && applied.undeleted().contains(write.id())) {
                w.items()[o.slot()] = new BulkItemResponse(
                    o.slot(),
                    o.item().opType(),
                    new BulkItemResponse.Failure(w.indexName(), write.id(), applied.deleteFailure(), RestStatus.INTERNAL_SERVER_ERROR)
                );
                continue;
            }
            String why = applied.refused().get(write.id());
            if (why != null) {
                w.items()[o.slot()] = new BulkItemResponse(
                    o.slot(),
                    o.item().opType(),
                    new BulkItemResponse.Failure(w.indexName(), write.id(), new IllegalArgumentException("[infino] " + why), RestStatus.BAD_REQUEST)
                );
                continue;
            }
            DocWriteResponse r;
            if (write.source() == null) {
                r = new DeleteResponse(w.shardId(), write.id(), done.seq(), PRIMARY_TERM, VERSION, done.existed());
            } else if (o.item() instanceof UpdateRequest) {
                r = new UpdateResponse(
                    w.shardId(),
                    write.id(),
                    done.seq(),
                    PRIMARY_TERM,
                    VERSION,
                    done.existed() ? DocWriteResponse.Result.UPDATED : DocWriteResponse.Result.CREATED
                );
            } else {
                r = new IndexResponse(w.shardId(), write.id(), done.seq(), PRIMARY_TERM, VERSION, !done.existed());
            }
            r.setShardInfo(new ReplicationResponse.ShardInfo(1, 1));
            w.items()[o.slot()] = new BulkItemResponse(o.slot(), o.item().opType(), r);
        }
        w.listener().onResponse(new BulkResponse(w.items(), (System.nanoTime() - w.startNanos()) / 1_000_000L));
    }
}
