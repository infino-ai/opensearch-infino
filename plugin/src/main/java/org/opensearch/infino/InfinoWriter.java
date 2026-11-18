/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import com.sun.jna.Pointer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.ExceptionsHelper;
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
import org.opensearch.core.concurrency.OpenSearchRejectedExecutionException;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.index.engine.DocumentMissingException;
import org.opensearch.index.engine.VersionConflictEngineException;
import org.opensearch.index.mapper.MapperParsingException;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.threadpool.ThreadPool;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

/**
 * Writes to one table, batched across bulk requests.
 *
 * <p>Each engine append is a commit, and a bulk is acknowledged after the
 * append that carries its writes, so an acknowledged item is durable in the
 * table. Up to {@code infino.batch.concurrency} appends to one table run at
 * once (the engine builds concurrent appends in parallel): a bulk that
 * arrives while a slot is free starts an append at once; bulks that arrive
 * while every slot is taken accumulate and go to the table as the next
 * append the moment one finishes, bounded by a document and a byte
 * threshold. The appends run on the write pool, never on the thread that
 * took the request.
 *
 * <p>Two appends running at once never write the same id. A bulk that names
 * an id (an index with a client id, a delete, an update) waits while a
 * running append writes that id, and every bulk staged after it waits behind
 * it, so writes to one id reach the table in the order they arrived. A
 * document with a generated id names nothing, so a stream of them (the shape
 * of a log stream) never waits.
 *
 * <p>A bulk's documents are parsed, conformed and converted to Arrow on a
 * write-pool thread before they join the batch, so that work overlaps the
 * append that is running; the append itself then only
 * commits the prepared batches. When an id is written by more than one
 * bulk of a batch, or a delete targets an id a bulk of the batch writes,
 * the batch takes the slower path that folds the writes by id first. So
 * does a batch carrying an update: an update's document is made when its
 * batch is written, merged into the write before it in the batch or into
 * the table, on the one thread that writes the table, so two updates of
 * one document, or an index and an update of it in one bulk, apply one
 * after the other as stock applies them.
 *
 * <p>Before the append, one statement over the id column's term index
 * names the ids the table already holds; those are tombstoned in one
 * mutation, so an append of new documents (the shape of a log stream) pays
 * no lookup.
 *
 * <p>Each write is given the next sequence number of its table when its
 * batch is written, in the order the batch staged it, and the table stores
 * the number beside the row. The numbers are unique per table; appends that
 * run at once may commit out of the order of their numbers, which nothing
 * reads: a condition on a sequence number compares the number stored for its
 * id, and writes to one id never run at once. One node writes
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
    /** Documents a batch may hold while an append runs; past this, arriving bulks are refused with 429 until the append takes them. */
    private static final int MAX_BATCH_DOCS = Integer.getInteger("infino.batch.docs", 50_000);
    /** Source bytes a batch may hold while an append runs. */
    private static final long MAX_BATCH_BYTES = Long.getLong("infino.batch.bytes", 64L << 20);
    /** Version reported for every write; the table keeps no per-document version. */
    private static final long VERSION = InfinoDocumentVersion.VERSION;
    /** Primary term reported for every write; the table has no primary to fail over. */
    private static final long PRIMARY_TERM = InfinoDocumentVersion.PRIMARY_TERM;
    /** Appends to one table that may run at once; 0 or less means half the write pool. */
    private static final int MAX_CONCURRENT_APPENDS = Integer.getInteger("infino.batch.concurrency", 0);
    /** A table's next sequence number before its first flush on this node has read the highest the table holds. */
    private static final long UNREAD = -1L;

    /**
     * One write a bulk item asks for: the source to append (null for a
     * delete or an update), whether the id may already exist (looked up at
     * flush), whether it must not ({@code op_type: create}, answered with a
     * conflict when it does), and for an update the request whose document
     * is merged when its batch is written.
     */
    record Write(String id, String source, boolean lookup, boolean create, UpdateRequest update) {
        Write(String id, String source, boolean lookup, boolean create) {
            this(id, source, lookup, create, null);
        }

        Write(String id, String source, boolean lookup) {
            this(id, source, lookup, false, null);
        }

        /**
         * An update. Its document is merged into the one it replaces when its
         * batch is written, on the one thread that writes the table, so it
         * merges into every write taken before it: earlier in its own bulk,
         * earlier in its batch, or in an append that has committed since.
         */
        static Write update(UpdateRequest update) {
            return new Write(update.id(), null, true, false, update);
        }

        /** Whether this write removes its id's document. */
        boolean deletes() {
            return source == null && update == null;
        }
    }

    /** What one item of a bulk became: answered up front (refused), or by its write when the batch flushes. */
    record Outcome(int slot, DocWriteRequest<?> item, Write write, BulkItemResponse answered) {}

    /** One bulk's share of a batch: its outcomes and the listener that gets its response. */
    record Waiter(ShardId shardId, String indexName, List<Outcome> outcomes, BulkItemResponse[] items, long startNanos, ActionListener<BulkResponse> listener) {}

    /**
     * A bulk in its batch: the waiter, its prepared batch (null when it writes
     * nothing), the ids of the prepared rows in order, the documents and source
     * bytes it counts toward the bound, and the ids it names (see {@link #names}).
     */
    private record Staged(Waiter waiter, Pointer prepared, List<String> preparedIds, int docs, long bytes, Set<String> named) {}

    /**
     * Whether a write names its id: a client chose it, so another write may
     * name it too. Only a generated id is known to be new, and it is never
     * looked up.
     */
    private static boolean names(Write w) {
        return w != null && w.lookup();
    }

    /**
     * What one write became when its batch was written: the sequence number
     * it was given, whether a document stood under its id just before it, and
     * what kept it out of the write (null when it was written): a conflict,
     * or for an update a missing document or a merged one the columns refuse.
     */
    private record Written(long seq, boolean existed, Exception rejection) {}

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

    /** The pending writes of one table, and the appends running for it. */
    private static final class Batch {
        final Lock lock = new ReentrantLock();
        final List<Staged> staged = new ArrayList<>();
        int docs = 0;
        long bytes = 0;
        /** Appends running for the table. */
        int running = 0;
        /** The ids the running appends name, with how many of them name each. */
        final Map<String, Integer> busy = new HashMap<>();
        /** Guards {@link #nextSeq} and {@link #seqAllocation}: appends running at once take numbers in turn. */
        final Object seqLock = new Object();
        /**
         * The sequence number the next write is given; {@link InfinoWriter#UNREAD}
         * until the first flush reads the table's highest. Read and advanced
         * under {@link #seqLock}.
         */
        long nextSeq = UNREAD;
        /** The copy that wrote the table when {@link #nextSeq} was read; another copy since means it is read again. */
        String seqAllocation;
    }

    /** Where a table's appends run: the {@code write} pool. */
    private final Supplier<Executor> flushes;
    private final int maxBatchDocs;
    private final long maxBatchBytes;
    private final int maxAppends;
    private final Map<String, Batch> batches = new ConcurrentHashMap<>();

    public InfinoWriter(ThreadPool threadPool) {
        this(
            () -> threadPool.executor(ThreadPool.Names.WRITE),
            MAX_BATCH_DOCS,
            MAX_BATCH_BYTES,
            MAX_CONCURRENT_APPENDS > 0 ? MAX_CONCURRENT_APPENDS : Math.max(1, threadPool.info(ThreadPool.Names.WRITE).getMax() / 2)
        );
    }

    InfinoWriter(Supplier<Executor> flushes, int maxBatchDocs, long maxBatchBytes) {
        this(flushes, maxBatchDocs, maxBatchBytes, 1);
    }

    /**
     * {@code maxAppends} appends to one table run at once. They run on the
     * write pool, as the bulks that feed them are prepared there, so it is
     * kept under the pool's size: half of it by default.
     */
    InfinoWriter(Supplier<Executor> flushes, int maxBatchDocs, long maxBatchBytes, int maxAppends) {
        this.flushes = flushes;
        this.maxBatchDocs = maxBatchDocs;
        this.maxBatchBytes = maxBatchBytes;
        this.maxAppends = Math.max(1, maxAppends);
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
        // An update's document is made when its batch is written; it counts toward the bound as a document.
        int updates = 0;
        Set<String> named = new HashSet<>();
        for (Outcome o : waiter.outcomes()) {
            if (names(o.write())) {
                named.add(o.write().id());
            }
            if (carriesRow(o)) {
                ids.add(o.write().id());
                sources.add(o.write().source());
                bytes += o.write().source().length();
            } else if (o.write() != null && o.write().update() != null) {
                updates++;
            }
        }
        Pointer prepared = ids.isEmpty() ? null : InfinoNative.prepare(table, ids, sources);
        if (!ids.isEmpty() && prepared == null) {
            waiter.listener().onFailure(new IllegalStateException("[infino] the bridge could not prepare the batch for " + table));
            return;
        }
        Staged staged = new Staged(waiter, prepared, ids, ids.size() + updates, bytes, named);

        Batch batch = batches.computeIfAbsent(table, t -> new Batch());
        boolean admitted;
        List<List<Staged>> starts = new ArrayList<>();
        int pending;
        batch.lock.lock();
        try {
            // A batch past its bound refuses more until a running append
            // takes it. It never waits here: this is a write-pool thread, the
            // pool the appends themselves run on, and threads waiting on a
            // stalled append would leave none to run it.
            pending = batch.docs;
            admitted = !refuse(batch.running > 0, batch.docs, batch.bytes);
            if (admitted) {
                batch.staged.add(staged);
                batch.docs += staged.docs();
                batch.bytes += bytes;
                while (batch.running < maxAppends) {
                    List<Staged> next = take(batch);
                    if (next.isEmpty()) {
                        break;
                    }
                    batch.running++;
                    starts.add(next);
                }
            }
        } finally {
            batch.lock.unlock();
        }
        if (!admitted) {
            if (prepared != null) {
                InfinoNative.freePrepared(prepared);
            }
            waiter.listener()
                .onFailure(
                    new OpenSearchRejectedExecutionException(
                        "[infino] "
                            + pending
                            + " writes to table "
                            + table
                            + " are waiting for the running appends; the bulk is refused until an append takes them"
                    )
                );
            return;
        }
        start(table, batch, starts);
    }

    /**
     * Hand appends taken under the batch's lock to the write pool. An append
     * the pool refuses (its queue is full, or the node is closing) answers
     * its bulks with the refusal, a 429 for a full queue, and nothing of them
     * is written; it gives back its slot and the ids it named, and the bulks
     * it held back are taken as {@link #flush} takes them, so none is left
     * staged with no append to take it.
     */
    private void start(String table, Batch batch, List<List<Staged>> appends) {
        Deque<List<Staged>> pending = new ArrayDeque<>(appends);
        while (!pending.isEmpty()) {
            List<Staged> next = pending.poll();
            try {
                flushes.get().execute(() -> flush(table, batch, next));
                continue;
            } catch (RejectedExecutionException e) {
                log.warn("[infino] the write pool refused an append of {} bulks to {}", next.size(), table, e);
                for (Staged s : next) {
                    if (s.prepared() != null) {
                        InfinoNative.freePrepared(s.prepared());
                    }
                    s.waiter().listener().onFailure(e);
                }
            }
            batch.lock.lock();
            try {
                release(batch, next);
                batch.running--;
                while (batch.running < maxAppends) {
                    List<Staged> more = take(batch);
                    if (more.isEmpty()) {
                        break;
                    }
                    batch.running++;
                    pending.add(more);
                }
            } finally {
                batch.lock.unlock();
            }
        }
    }

    /** Whether a bulk arriving now is refused: an append is running and the batch behind it is past its bound. */
    private boolean refuse(boolean running, int docs, long bytes) {
        return running && (docs >= maxBatchDocs || bytes >= maxBatchBytes);
    }

    /**
     * Take the next append's bulks, under the batch's lock: the staged bulks
     * in order up to the first that names an id a running append names (it
     * waits for that append, and every bulk after it waits behind it). Marks
     * the ids the taken bulks name as busy. Empty when nothing can go now.
     */
    private static List<Staged> take(Batch batch) {
        List<Staged> taken = new ArrayList<>();
        for (Staged s : batch.staged) {
            boolean blocked = false;
            for (String id : s.named()) {
                if (batch.busy.containsKey(id)) {
                    blocked = true;
                    break;
                }
            }
            if (blocked) {
                break;
            }
            taken.add(s);
        }
        if (taken.isEmpty()) {
            return taken;
        }
        batch.staged.subList(0, taken.size()).clear();
        for (Staged s : taken) {
            batch.docs -= s.docs();
            batch.bytes -= s.bytes();
            for (String id : s.named()) {
                batch.busy.merge(id, 1, Integer::sum);
            }
        }
        return taken;
    }

    /** Releases the ids an append's bulks named, under the batch's lock. */
    private static void release(Batch batch, List<Staged> staged) {
        for (Staged s : staged) {
            for (String id : s.named()) {
                batch.busy.computeIfPresent(id, (k, n) -> n == 1 ? null : n - 1);
            }
        }
    }

    /**
     * Append the bulks taken for this append and answer them; then, while
     * more can go, take the next ones and append them on this thread, and
     * start appends for any further free slots.
     */
    private void flush(String table, Batch batch, List<Staged> first) {
        List<Staged> staged = first;
        while (true) {
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
            List<List<Staged>> more = new ArrayList<>();
            batch.lock.lock();
            try {
                release(batch, staged);
                staged = take(batch);
                if (staged.isEmpty()) {
                    batch.running--;
                }
                // Releasing these ids may have unblocked bulks for slots another append left free.
                while (batch.running < maxAppends) {
                    List<Staged> next = take(batch);
                    if (next.isEmpty()) {
                        break;
                    }
                    batch.running++;
                    more.add(next);
                }
            } finally {
                batch.lock.unlock();
            }
            start(table, batch, more);
            if (staged.isEmpty()) {
                return;
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
            synchronized (batch.seqLock) {
                if (batch.nextSeq == UNREAD || !Objects.equals(allocation, batch.seqAllocation)) {
                    // Numbers this node gave to appends still running are not in
                    // the table yet: never go back below them.
                    long read = InfinoNative.maxSeq(table) + 1;
                    batch.nextSeq = batch.nextSeq == UNREAD ? read : Math.max(read, batch.nextSeq);
                    batch.seqAllocation = allocation;
                }
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
        // The document each id stands at after the writes so far, for an update to merge into; null once deleted.
        Map<String, String> documents = new HashMap<>();
        // The document each update became.
        Map<Outcome, String> merged = new IdentityHashMap<>();
        // The last write of each id that goes to the table.
        Map<String, Outcome> last = new LinkedHashMap<>();
        boolean folded = false;
        // Numbers are taken in turn with the other appends running for the table.
        // An update reads its base here too, under the lock; the ids it reads no
        // running append writes.
        synchronized (batch.seqLock) {
            try {
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
                        Exception rejection = w.create() && current != null
                            ? new VersionConflictEngineException(shardId, w.id(), "document already exists")
                            : InfinoDocumentVersion.conflict(shardId, o.item(), current);
                        if (rejection == null && w.update() != null) {
                            // The base is the write before it in this batch, else the table,
                            // which holds every append of its id before this one: no running
                            // append writes its id.
                            String base = documents.containsKey(w.id()) ? documents.get(w.id()) : current == null ? null : InfinoNative.getSourceFresh(table, w.id());
                            String document = InfinoBulkActionFilter.merge(base, w.update());
                            String why = document == null ? null : InfinoNative.checkDoc(table, document);
                            if (document == null) {
                                rejection = new DocumentMissingException(shardId, w.id());
                            } else if (why != null) {
                                rejection = refused(why);
                            } else {
                                merged.put(o, document);
                            }
                        }
                        if (rejection != null) {
                            written.put(o, new Written(SequenceNumbers.UNASSIGNED_SEQ_NO, current != null, rejection));
                            folded |= carriesRow(o); // the prepared batches carry its row; only the folded path leaves it out
                            continue;
                        }
                        String document = merged.containsKey(o) ? merged.get(o) : w.source();
                        long seq = batch.nextSeq++;
                        written.put(o, new Written(seq, current != null, null));
                        standing.put(w.id(), document == null ? null : seq);
                        documents.put(w.id(), document);
                        if (last.put(w.id(), o) != null) {
                            folded = true; // the same id twice in one batch: written twice, or written and deleted
                        }
                        folded |= w.update() != null; // its document was made here: only the folded path prepares it
                    }
                }
            } catch (RuntimeException e) {
                // An update's read of the table failed: the handles never reached the engine.
                for (Staged s : staged) {
                    InfinoNative.freePrepared(s.prepared());
                }
                throw e;
            }
        }
        List<String> replaced = new ArrayList<>();
        List<String> tombstones = new ArrayList<>();
        for (Outcome o : last.values()) {
            Write w = o.write();
            if (!existed.contains(w.id())) {
                continue; // a new id: its row is appended, and there is nothing to replace
            }
            if (w.deletes()) {
                tombstones.add(w.id());
            } else {
                replaced.add(w.id());
            }
        }
        Map<String, String> refused;
        try {
            // Both paths release every handle: the engine does on the prepared
            // path, the folded path up front. Nothing after this may release one.
            refused = folded ? appendFolded(table, staged, last.values(), merged, written, replaced) : appendPrepared(table, staged, written, replaced);
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

    /** How the bridge's reason starts when a data stream's document lacks one timestamp. */
    static final String NO_TIMESTAMP = "documents must contain a single-valued timestamp field";

    /** A document the bridge refused, answered as stock answers it: a data stream's document without one timestamp fails the parse. */
    static Exception refused(String why) {
        if (why.startsWith(NO_TIMESTAMP)) {
            return new MapperParsingException("failed to parse", new IllegalArgumentException(why));
        }
        return new IllegalArgumentException("[infino] " + why);
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
     * row; an update writes the document {@code merged} holds for it): the
     * path for a batch in which one id appears twice, a write meets a
     * conflict and is left out, or an update was merged.
     */
    private static Map<String, String> appendFolded(
        String table,
        List<Staged> staged,
        Collection<Outcome> last,
        Map<Outcome, String> merged,
        Map<Outcome, Written> written,
        List<String> replaced
    ) {
        for (Staged s : staged) {
            InfinoNative.freePrepared(s.prepared());
        }
        List<String> ids = new ArrayList<>();
        List<String> sources = new ArrayList<>();
        List<Long> seqs = new ArrayList<>();
        for (Outcome o : last) {
            String document = merged.containsKey(o) ? merged.get(o) : o.write().source();
            if (document != null) {
                ids.add(o.write().id());
                sources.add(document);
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
            if (done.rejection() != null) {
                // A create over a document, a condition the document does not meet, or an
                // update of a missing document or into one the columns refuse: the failure
                // stock reports (409, 404 or 400), and no row written.
                w.items()[o.slot()] = new BulkItemResponse(
                    o.slot(),
                    o.item().opType(),
                    new BulkItemResponse.Failure(w.indexName(), write.id(), done.rejection(), ExceptionsHelper.status(done.rejection()))
                );
                continue;
            }
            if (write.deletes() && applied.undeleted().contains(write.id())) {
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
                    new BulkItemResponse.Failure(w.indexName(), write.id(), refused(why), RestStatus.BAD_REQUEST)
                );
                continue;
            }
            DocWriteResponse r;
            if (write.deletes()) {
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
