/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import com.sun.jna.Pointer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.search.ReferenceManager;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.util.BytesRef;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.common.Randomness;
import org.opensearch.common.lucene.index.OpenSearchDirectoryReader;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.concurrency.OpenSearchRejectedExecutionException;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.index.engine.DocumentIndexWriter;
import org.opensearch.index.engine.EngineConfig;
import org.opensearch.index.engine.EngineCreationFailureException;
import org.opensearch.index.engine.EngineException;
import org.opensearch.index.engine.FlushFailedEngineException;
import org.opensearch.index.engine.InternalEngine;
import org.opensearch.index.mapper.MapperParsingException;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.mapper.ParseContext.Document;
import org.opensearch.index.mapper.SourceFieldMapper;
import org.opensearch.index.seqno.ReplicationTracker;
import org.opensearch.index.seqno.RetentionLease;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.index.shard.RefreshListeners;
import org.opensearch.index.translog.Translog;
import org.opensearch.threadpool.Scheduler;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.LongSupplier;
import java.util.function.LongUnaryOperator;
import java.util.function.Predicate;

/**
 * The engine of an index in default mode ({@code index.infino.mode: default}):
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
 * <p>Writes reach the table in batches, a group commit. A write reaches the
 * table only once every in-sync copy of the shard holds it: a commit takes
 * the writes at or below the shard's global checkpoint, and leaves the rest
 * waiting. A write the copies do not all hold may yet be undone, when a copy
 * that lacks it becomes primary, so it never reaches the table ahead of
 * them; a primary cut off from its copies, still taking writes it cannot
 * replicate, therefore never puts them in the table either. Each commit's
 * newest row records the operation that wrote it, so the table says how far
 * into the shard's history it holds every write.
 *
 * <p>A batch commits when the shard syncs its translog for a write, on
 * refresh, on flush, and past its document or byte bound. The first three
 * wait, for a bounded time, until the copies hold every operation applied
 * so far: a write's acknowledgement waits for the commit that carries it,
 * as it waits for the copies anyway; a search after a refresh finds what
 * its reader covers; and a flush commits the writes before the translog
 * that holds them is trimmed. Each commit first
 * syncs the translog, so the table never holds a write the translog could
 * lose: a crash between the two leaves writes the translog's replay looks up
 * in the table. A commit that fails keeps its batch for the next one and
 * fails what asked for it: the write's sync, or the flush, so the translog
 * keeps the writes the table lacks. A hosted service that could not be
 * reached, did not answer, or turned the commit away for the account's
 * request rate fails neither: the batch is kept for the next commit, as a
 * write the table did not take is, so the write is acknowledged and the
 * table's hold on the shard's history stays below it (see
 * {@link #commitBeforeSync} and {@link #holdHistoryThrough}). After a
 * rate limit, commits make no call until a backoff ends ({@link RateLimit}).
 * Writes wait rather than pile up past a bound while commits fail.
 *
 * <p>A search sees what the skeleton's reader sees, as on a stock index: a
 * write is visible after the refresh that follows it. A row a write creates
 * carries the write's sequence number, each refresh records the sequence
 * number its reader covers, and a search over a reader that does not cover
 * every row the table holds leaves the newer rows out (see
 * {@link #visibleSeqNo}), so its hits, total and aggregations agree. A get
 * sees a write as soon as it is acknowledged, as a stock get does.
 *
 * <p>The engine knows which writes replace an existing document (the version
 * map says so), so those rows replace their old ones in the same commit
 * without a lookup; a write replayed from the translog, which may or may not
 * have reached the table before a restart, is looked up. A write's record in
 * the batch is made under the same per-id lock as its skeleton write, so the
 * batch holds each id's writes in the order the shard applied them, and a
 * commit that takes the writes up to the checkpoint takes each id's writes
 * up to it, leaving its later ones for the next.
 *
 * <p>Only the primary writes the table: one table has one writer. A
 * replica's documents, the operations a recovering copy receives, and a
 * replica's own translog replay were all written by the primary that took
 * them, so on a replica the skeleton alone is written. Every copy keeps each
 * write's source as the recovery source, as stock does for a filtered
 * source, so a copy can recover from another's history; Lucene drops it
 * from merged segments once no copy can need it. A copy that becomes primary
 * (a promotion, or a restart of the primary) writes into the table the
 * operations of its history the table may not hold (see
 * {@link #fillSeqNoGaps}): the writes its old primary had not committed,
 * which are now the shard's. A primary that hands the shard to another node
 * has committed every acknowledged write by then, commits what it still
 * holds where the release lets it see the hand-off coming, and commits
 * nothing after it. Each
 * shard has its own table (see {@link InfinoTables}), so an index of several
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
 * <p>The table holds a retention lease on the shard's history from the first
 * operation it may not hold, which the primary moves forward as commits
 * land and which every copy is given as it is given every lease: a copy
 * keeps the history a new primary writes the table from however long
 * commits fail, and a promotion whose history is not whole from there fails
 * rather than serving without acknowledged writes.
 *
 * <p>A document the table does not take after Lucene has acknowledged it is
 * checked against the table's columns again. One they refuse (the mapper
 * refuses what the mapping cannot hold first, so a document the columns
 * refuse beyond the mapping) is refused on every try: it is logged as an
 * error and is not in the table. Any other is put back for the next commit,
 * and the lease stays below it until a commit takes it.
 *
 * <p>Not covered: a primary cut off from its copies may still commit, late,
 * writes at or below the global checkpoint it last knew, which its copies
 * held and which the new primary took over. If the new primary has written
 * a document since, such a late commit undoes that write: an index or
 * update brings back the older version, or a document the new primary
 * deleted; a delete removes a document the new primary wrote. Closing it
 * needs the table to refuse a stale primary's commit.
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
    /** Skeleton readers whose covered sequence number is remembered; a search on an older one sees no stamped row. */
    private static final int READERS_REMEMBERED = 1024;
    /** The sequence number of a row that every reader may show: no stamp. */
    private static final long UNSTAMPED = -1L;
    /**
     * Created ids awaiting a reader past which the engine refreshes on its
     * own, so that with refresh off their record stays bounded.
     */
    private static final int MAX_UNREFRESHED = 1_000_000;
    /** The refresh source of that refresh. */
    private static final String UNREFRESHED_REFRESH = "infino_unrefreshed";
    /** The seed and multiplier of the 64-bit polynomial hash an index UUID becomes a generation by. */
    private static final long GENERATION_SEED = 1125899906842597L;
    private static final long GENERATION_MULTIPLIER = 31L;
    /**
     * How long a refresh, a flush or a hand-off waits for the shard's copies
     * to hold the operations it covers before it goes on with what they hold:
     * as long as a write waits for its copies by default.
     */
    private static final long COPIES_WAIT_NANOS = TimeUnit.MILLISECONDS.toNanos(
        Long.getLong("infino.engine.copies.wait.ms", TimeUnit.MINUTES.toMillis(1))
    );
    /** The first and the longest pause between looks at what the copies hold while waiting for them. */
    private static final long COPIES_POLL_MIN_NANOS = TimeUnit.MICROSECONDS.toNanos(50);
    private static final long COPIES_POLL_MAX_NANOS = TimeUnit.MILLISECONDS.toNanos(5);
    /**
     * The first and the longest pause before a commit calls the hosted service
     * again after it turned commits away for the account's request rate (see
     * {@link RateLimit}): doubled for each commit turned away in a row.
     */
    static final long RATE_LIMIT_PAUSE_MIN_NANOS = TimeUnit.SECONDS.toNanos(1);
    static final long RATE_LIMIT_PAUSE_MAX_NANOS = TimeUnit.SECONDS.toNanos(30);
    /** The changes-snapshot source of the history a new primary writes into the table. */
    private static final String FILL_SOURCE = "infino_fill";
    /** A sequence number below every operation's: what a primary that handed its shard off may commit through. */
    private static final long NOTHING = Long.MIN_VALUE;
    /**
     * The retention lease the table holds on its shard's history (see
     * {@link #holdHistoryThrough}): from the first operation the table may
     * not hold, which is where a new primary writes the table from.
     */
    static final String TABLE_LEASE = "infino_table";
    /** What the lease names as the one holding it. */
    private static final String TABLE_LEASE_SOURCE = "infino table";
    /** Shortest wait between renewals of the table's lease where it is (see {@link #refreshTableLease}). */
    private static final long LEASE_REFRESH_MIN_MILLIS = 1_000;

    /**
     * One write of an id waiting for its commit: its source (null for a
     * delete), routing value and stamp, the operation that made it, and the
     * paths of the source's fields the mapper ignored ({@link InfinoIgnored}),
     * left out of its row. A write the copies need not hold first (a replay
     * of the shard's own history) is not gated. {@code mapped} holds the
     * values OpenSearch's mapper read from the source's date and integer
     * fields, for a document the table's columns refuse without them
     * ({@link InfinoMappedValues}); null for any other.
     */
    record Version(long seq, boolean gated, String source, String routing, long stamp, List<String> ignored, String mapped) {
        Version(long seq, boolean gated, String source, String routing, long stamp, List<String> ignored) {
            this(seq, gated, source, routing, stamp, ignored, null);
        }

        /** The source as the bridge takes it, with the mapper's values when it has them. */
        String forTable() {
            return InfinoMappedValues.framed(source, mapped);
        }

        /** Whether the table may take this write once the copies hold every operation up to {@code through}. */
        boolean takenBy(long through) {
            return !gated || seq <= through;
        }
    }

    /** What the table holds under an id before the first of its waiting writes. */
    enum Base {
        /** No row: the id is new, or was deleted. */
        ABSENT,
        /** A row, which a write replaces. */
        PRESENT,
        /** Unknown: a write looks the row up at commit. */
        UNKNOWN
    }

    /** The writes of one id waiting for a commit, oldest first. */
    static final class Entry {
        Base base;
        final List<Version> versions = new ArrayList<>(1);

        Entry(Base base) {
            this.base = base;
        }

        Version latest() {
            return versions.get(versions.size() - 1);
        }
    }

    /** The writes waiting for a commit, by id in the arrival order of each id's first. */
    static final class Batch {
        final Map<String, Entry> entries = new LinkedHashMap<>();
        int versions = 0;
        long bytes = 0;
        /** The lowest and the highest sequence number among the gated writes. */
        long lowestGated = Long.MAX_VALUE;
        long highestGated = Long.MIN_VALUE;
        /** Writes that are not gated. */
        int ungated = 0;

        boolean isEmpty() {
            return entries.isEmpty();
        }

        int size() {
            return versions;
        }

        /** Whether a commit through {@code through} takes any of these writes. */
        boolean anyTakenBy(long through) {
            return ungated > 0 || lowestGated <= through;
        }

        /** Whether a commit through {@code through} takes every one of these writes. */
        boolean allTakenBy(long through) {
            return highestGated <= through;
        }

        /** Record a write of {@code id}; {@code base} is what the table holds when the batch has no write of it yet. */
        void add(String id, Base base, Version v) {
            entries.computeIfAbsent(id, k -> new Entry(base)).versions.add(v);
            count(v);
        }

        /** The lowest sequence number among these writes; {@code Long.MAX_VALUE} when there are none. */
        long lowestSeq() {
            long lowest = Long.MAX_VALUE;
            for (Entry e : entries.values()) {
                for (Version v : e.versions) {
                    lowest = Math.min(lowest, v.seq());
                }
            }
            return lowest;
        }

        /** The newest write of {@code id}, or null when the batch has none. */
        Version latest(String id) {
            Entry e = entries.get(id);
            return e == null ? null : e.latest();
        }

        private void count(Version v) {
            versions++;
            if (v.source() != null) {
                bytes += v.source().length();
            }
            if (v.gated()) {
                lowestGated = Math.min(lowestGated, v.seq());
                highestGated = Math.max(highestGated, v.seq());
            } else {
                ungated++;
            }
        }

        private void recount() {
            versions = 0;
            bytes = 0;
            lowestGated = Long.MAX_VALUE;
            highestGated = Long.MIN_VALUE;
            ungated = 0;
            for (Entry e : entries.values()) {
                for (Version v : e.versions) {
                    count(v);
                }
            }
        }

        /**
         * Take the writes a commit through {@code through} carries: each id's
         * writes up to it, oldest first. The writes left stand on what the
         * taken ones leave in the table.
         */
        Batch take(long through) {
            Batch out = new Batch();
            Iterator<Map.Entry<String, Entry>> it = entries.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry<String, Entry> me = it.next();
                Entry e = me.getValue();
                int k = 0;
                while (k < e.versions.size() && e.versions.get(k).takenBy(through)) {
                    k++;
                }
                if (k == 0) {
                    continue;
                }
                Entry taken = new Entry(e.base);
                taken.versions.addAll(e.versions.subList(0, k));
                out.entries.put(me.getKey(), taken);
                if (k == e.versions.size()) {
                    it.remove();
                } else {
                    e.base = taken.latest().source() == null ? Base.ABSENT : Base.PRESENT;
                    e.versions.subList(0, k).clear();
                }
            }
            out.recount();
            recount();
            return out;
        }

        /**
         * Put back, ahead of the writes recorded since, the writes of a commit
         * that failed. Their rows are looked up at the next commit, since a
         * failed commit may have reached the table in part.
         */
        void putBack(Batch failed) {
            putBack(failed, true);
        }

        /**
         * Put back, ahead of the writes recorded since, the writes of a commit
         * that did not take them: with {@code lookUp}, each id's row is looked
         * up at the next commit; without, each keeps the base it carries,
         * which the commit that returned it to retry set (see {@link #write}).
         */
        void putBack(Batch failed, boolean lookUp) {
            Map<String, Entry> merged = new LinkedHashMap<>();
            for (Map.Entry<String, Entry> me : failed.entries.entrySet()) {
                Entry older = me.getValue();
                if (lookUp) {
                    older.base = Base.UNKNOWN;
                }
                Entry newer = entries.get(me.getKey());
                if (newer != null) {
                    older.versions.addAll(newer.versions);
                }
                merged.put(me.getKey(), older);
            }
            for (Map.Entry<String, Entry> me : entries.entrySet()) {
                merged.putIfAbsent(me.getKey(), me.getValue());
            }
            entries.clear();
            entries.putAll(merged);
            recount();
        }
    }

    /**
     * When a table's next commit may call the hosted service, after the
     * service turned commits away for the account's request rate: each
     * commit turned away in a row doubles the pause, from
     * {@link #RATE_LIMIT_PAUSE_MIN_NANOS} up to
     * {@link #RATE_LIMIT_PAUSE_MAX_NANOS}, and the pause taken is a random
     * part of it, between half and all, so the shards a limit stopped
     * together do not all call again at once. A commit the service takes
     * clears it. Until the pause ends, a commit makes no call: the writes
     * wait in the batch, which the translog and the table's hold on the
     * shard's history keep, and new writes are refused once too many wait
     * (see {@link #refuseWhileBacklogged}). The engine's hosted client does
     * not pass the service's {@code Retry-After} on, so the pause is this
     * backoff alone.
     */
    static final class RateLimit {
        /** Given a pause, the part of it taken. */
        private final LongUnaryOperator jitter;
        /** The current pause; 0 when the last commit was not turned away. */
        private long pause = 0;
        /** When the pause ends ({@link System#nanoTime}). */
        private long notBefore = 0;

        RateLimit() {
            this(pause -> pause / 2 + Randomness.get().nextLong(pause / 2 + 1));
        }

        RateLimit(LongUnaryOperator jitter) {
            this.jitter = jitter;
        }

        /** Whether a commit at {@code now} waits, making no call. */
        synchronized boolean waits(long now) {
            return pause > 0 && now - notBefore < 0;
        }

        /** Record a commit made at {@code now}: whether the service turned it away for the rate. */
        synchronized void after(boolean rateLimited, long now) {
            if (!rateLimited) {
                pause = 0;
                return;
            }
            pause = pause == 0 ? RATE_LIMIT_PAUSE_MIN_NANOS : Math.min(RATE_LIMIT_PAUSE_MAX_NANOS, pause * 2);
            notBefore = now + jitter.applyAsLong(pause);
        }
    }

    /**
     * Which copy of a shard this node holds: whether it is the primary, its
     * allocation, and whether it is recovering as the target of the
     * primary's move to this node.
     */
    record Copy(boolean primary, String allocationId, boolean relocationTarget) {}

    private final String table;
    private final ShardId shardId;
    /** The index's generation, which this engine's stamped rows name (see {@link #generation}). */
    private final long generation;
    /** Whether the skeleton started empty over a table holding rows (see the class comment). */
    private final boolean rebuilt;
    private final Object batchLock = new Object();
    /** Serializes commits, so one batch reaches the table before the next. */
    private final Object commitLock = new Object();
    /** Waits for hosted reads after a refresh, outside Lucene's lock, and then releases its wait_for requests. */
    private final RefreshRelease refreshRelease;
    /**
     * Held shared from a write's apply to its record in the batch, and
     * exclusively while a commit takes its writes: a commit through a
     * sequence number then finds every operation at or below it, all of
     * which are applied, in the batch.
     */
    private final ReentrantReadWriteLock recordLock = new ReentrantReadWriteLock();
    /** Held exclusively from persisting the leases to the end of a Lucene commit (see {@link #commitIndexWriter}). */
    private final ReentrantReadWriteLock leaseLock = new ReentrantReadWriteLock();
    /** The next renewal of the table's lease where it is, until the engine closes. */
    private volatile Scheduler.ScheduledCancellable leaseRefresh;
    private volatile boolean leaseRefreshStopped;

    /**
     * Whether this engine holds open the connection the latest reads go
     * through ({@link InfinoNative#holdLatest}). While a rebuilt index's
     * skeleton is rebuilt, each commit with a write the skeleton took as new,
     * and each delete it did not find, reads the table's latest commit, as
     * each commit of a replay does; without a hold, each would open the table
     * again. Under {@link #latestHoldLock}.
     */
    private boolean holdsLatest;

    /**
     * Whether an update's merge on this primary reads the table's latest
     * commit ({@link #sourceForMerge}): set as the engine opens and as a
     * replica takes over, and cleared once this engine's own commit has
     * appended or deleted. Until then this node's connection may answer from
     * a commit before the last one the copy before it made, for up to the
     * table's staleness window; its own commit brings it to the latest.
     * While set, a primary holds the latest connection open
     * ({@link #holdLatestWhileLookingUp}).
     */
    private volatile boolean readsLatest = true;

    /**
     * Whether the table holds this copy's history, so an update may merge
     * into what it reads there: true for a copy that opens as primary, for
     * a replica once its takeover has written into the table the writes its
     * old primary had not committed ({@link #fillSeqNoGaps}), and for the
     * target of a move once it has done the same after the hand-off
     * ({@link #fillAfterHandOff}).
     */
    private volatile boolean filled;

    /**
     * Whether this copy opened as the target of the primary's move to this
     * node. The primary it takes over from commits its writes as it hands
     * the shard off, and a commit that fails then leaves them to this copy,
     * so it writes its history into the table ({@link #fillAfterHandOff})
     * before its first commit as primary and before an update merges here.
     */
    private final boolean movedHere;

    /** Whether {@link #fillAfterHandOff} runs on this thread, under {@link #commitLock}. */
    private boolean fillingAfterHandOff;

    /** Whether a fill after the hand-off is queued for an update waiting on it ({@link #mergesHere}). */
    private final AtomicBoolean handOffFillQueued = new AtomicBoolean();

    /** Whether an update was refused a merge here while the table did not yet hold this copy's history, already logged. */
    private final AtomicBoolean unfilledLogged = new AtomicBoolean();

    private final Object latestHoldLock = new Object();
    private final Object[] idLocks = new Object[ID_LOCK_STRIPES];
    private Batch batch = new Batch();
    /** The batch being committed, until the table has it; a read still sees its writes meanwhile. */
    private Batch committing = null;
    /**
     * The ids this primary created that the latest skeleton reader may not
     * hold yet, with their creation's sequence number: a later write of one
     * stamps its row with it, so the row stays out of searches until a
     * reader holds the id. Guarded by {@link #batchLock}.
     */
    private final Map<String, Long> unrefreshed = new HashMap<>();
    /** The sequence number each recent skeleton reader covers, by the reader's version. */
    private final Map<Long, Long> coveredByReader = new LinkedHashMap<>(16, 0.75f, true) {
        @Override
        protected boolean removeEldestEntry(Map.Entry<Long, Long> eldest) {
            return size() > READERS_REMEMBERED;
        }
    };
    /** The sequence number the latest skeleton reader covers. */
    private volatile long latestCovered = UNSTAMPED;
    /** Whether a refresh for {@link #MAX_UNREFRESHED} is queued. */
    private final AtomicBoolean unrefreshedRefreshQueued = new AtomicBoolean();
    /** Whether the engine is closing: its final flush commits what the copies hold without waiting for more. */
    private volatile boolean closing = false;
    /** When the next commit may call the hosted service, after a rate limit. */
    private final RateLimit rateLimit = new RateLimit();
    /** Whether the shard's checkpoints could not be read, already logged. */
    private final AtomicBoolean untrackedLogged = new AtomicBoolean();

    /**
     * A write waiting for its commit, as a read sees it before the table
     * does: the document's source, or null when the write deleted it.
     */
    record Pending(String source) {}

    /** The live engines by table, so a read sees what a shard has acknowledged and not yet committed. */
    private static final Map<String, InfinoEngine> BY_TABLE = new ConcurrentHashMap<>();

    /**
     * Which copy of each shard this node holds, as its routing said when it
     * began recovering and as it changes since (a promotion): a copy
     * replaying its own translog writes the table only as primary.
     */
    private static final Map<ShardId, Copy> COPIES = new ConcurrentHashMap<>();

    /**
     * The shards whose skeleton rebuild has walked the whole table on this
     * node (see {@link InfinoSkeletonAdopter}), whatever rows it left out.
     */
    private static final Set<ShardId> REBUILD_WALKED = ConcurrentHashMap.newKeySet();

    /**
     * The shards whose skeleton rebuild has walked the whole table on this
     * node and left no row out: the skeleton holds a document for every row
     * of the table again, so ids need no lookup.
     */
    private static final Set<ShardId> SKELETON_COMPLETE = ConcurrentHashMap.newKeySet();

    /** Record which copy of {@code shard} this node holds, as {@code routing} says. */
    static void copyOf(ShardId shard, ShardRouting routing) {
        COPIES.put(
            shard,
            new Copy(routing.primary(), routing.allocationId() == null ? null : routing.allocationId().getId(), routing.isRelocationTarget())
        );
    }

    /**
     * The shards whose copy on this node has written its history into the
     * table as primary ({@link #fillSeqNoGaps}): from then on, and not
     * before, the table holds every write its skeleton reader holds.
     */
    private static final Set<ShardId> TOOK_OVER = ConcurrentHashMap.newKeySet();

    /**
     * Whether this node's copy of {@code shard} may fill the shard request
     * cache: a primary that has written its history into the table. A
     * replica becoming primary keeps its skeleton reader while it fills the
     * table, so a count it kept before the fill would be served after it
     * for that reader's life.
     */
    static boolean fillsRequestCache(ShardId shard) {
        Copy copy = COPIES.get(shard);
        return copy != null && copy.primary() && TOOK_OVER.contains(shard);
    }

    /**
     * {@code shard}'s skeleton rebuild has walked the whole table, whatever
     * it left out. Its writes go on looking ids up in the table until the
     * skeleton is complete ({@link #looksUpIds}).
     */
    static void rebuildWalked(ShardId shard) {
        REBUILD_WALKED.add(shard);
        lookUpsChanged(shard);
    }

    /** {@code shard}'s skeleton holds a document for every row of its table: ids need no lookup. */
    static void skeletonComplete(ShardId shard) {
        SKELETON_COMPLETE.add(shard);
        lookUpsChanged(shard);
    }

    /** Let this node's engine of {@code shard} go of the latest connection if its writes no longer look ids up. */
    private static void lookUpsChanged(ShardId shard) {
        for (InfinoEngine engine : BY_TABLE.values()) {
            if (engine.shardId.equals(shard)) {
                engine.holdLatestWhileLookingUp(false);
            }
        }
    }

    /** This node no longer holds a copy of {@code shard}. */
    static void forgetCopy(ShardId shard) {
        COPIES.remove(shard);
        TOOK_OVER.remove(shard);
        REBUILD_WALKED.remove(shard);
        SKELETON_COMPLETE.remove(shard);
    }

    /** Whether this node's copy of the shard is its primary. */
    private boolean primaryCopy() {
        Copy copy = COPIES.get(shardId);
        return copy != null && copy.primary();
    }

    /**
     * Whether the skeleton holds a document for every row its rebuild could
     * create one for: not until a rebuilt index's rebuild has walked the
     * table ({@link #rebuildWalked}). A row the rebuild left out has no
     * document, and is no search's hit.
     */
    private boolean skeletonKnowsIds() {
        return !rebuilt || REBUILD_WALKED.contains(shardId);
    }

    /**
     * Whether this copy's writes look ids up in the table, because its
     * skeleton may lack a document for a row the table holds: while a
     * rebuilt index's skeleton is rebuilt, and after while the rebuild left
     * rows out ({@link #skeletonComplete} not yet), so a client's write of a
     * left-out id replaces its row rather than being taken as new.
     */
    private boolean looksUpIds() {
        return !skeletonKnowsIds() || (rebuilt && !SKELETON_COMPLETE.contains(shardId));
    }

    /**
     * Hold the latest connection open ({@link #holdsLatest}) while the
     * skeleton cannot say which ids the table holds, while a primary's
     * update merges read the latest commit ({@link #readsLatest}) or, as
     * {@code lookingUp} says, the commits look ids up; let it go once none
     * holds or the engine closes.
     */
    private void holdLatestWhileLookingUp(boolean lookingUp) {
        synchronized (latestHoldLock) {
            boolean wanted = (lookingUp || looksUpIds() || (readsLatest && primaryCopy())) && !leaseRefreshStopped;
            if (wanted == holdsLatest) {
                return;
            }
            holdsLatest = wanted;
            if (wanted) {
                InfinoNative.holdLatest();
            } else {
                InfinoNative.releaseLatest();
            }
        }
    }

    public InfinoEngine(EngineConfig config) {
        super(config);
        this.shardId = config.getShardId();
        this.generation = generation(config.getIndexSettings().getUUID());
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
        // A reader's cover is recorded as the internal manager opens it, before
        // the external manager can hand it to a search (see CoverRecorder).
        CoverRecorder<?> covers = new CoverRecorder<>(getReferenceManager(SearcherScope.INTERNAL), this::getProcessedLocalCheckpoint, coveredByReader);
        getReferenceManager(SearcherScope.INTERNAL).addListener(covers.opening());
        getReferenceManager(SearcherScope.EXTERNAL).addListener(covers.publishing());
        // The shard's wait_for listeners are released by refreshRelease once a
        // refresh has returned and every read sees what it covers, so the wait
        // for hosted reads runs outside Lucene's refresh lock.
        ReferenceManager<OpenSearchDirectoryReader> readers = getReferenceManager(SearcherScope.EXTERNAL);
        RefreshListeners waiting = null;
        for (ReferenceManager.RefreshListener l : config.getExternalRefreshListener()) {
            if (l instanceof RefreshListeners r) {
                waiting = r;
                readers.removeListener(r);
            }
        }
        this.refreshRelease = new RefreshRelease(table, waiting, () -> {
            try {
                return translogManager().getTranslogLastWriteLocation();
            } catch (AlreadyClosedException e) {
                return null; // a closing shard releases its listeners as it closes
            }
        }, () -> {
            if (InfinoNative.hosted()) {
                commitBeforeLucene("refresh", appliedSoFar());
            }
        });
        readers.addListener(refreshRelease);
        Copy copy = COPIES.get(shardId);
        this.movedHere = copy != null && copy.relocationTarget();
        this.filled = primaryCopy() && !movedHere;
        BY_TABLE.put(table, this);
        scheduleLeaseRefresh();
        log.info("[infino] engine attached to table {} for {}", table, config.getShardId());
    }

    /**
     * Records what each skeleton reader searches use covers before they can
     * use it. The external reader manager publishes a reader the internal
     * one opened, so the cover is taken when the internal one opens it: the
     * operations processed when its refresh began. Recording it after the
     * external refresh returns would leave a window where a search finds
     * the reader with no cover, hides every stamped row, and the shard
     * request cache keeps that short count for the reader's life.
     *
     * <p>Only readers an external refresh may publish are recorded, so
     * refreshes no search sees (a flush's, a realtime get's) do not push
     * the readers searches use out of the bounded map: one opened while an
     * external refresh runs, and the latest one opened before it, which
     * the external refresh publishes when the internal manager has nothing
     * newer. Each external refresh records at most these two, and the
     * reader it publishes is the newest, so it stays in the map until
     * {@code READERS_REMEMBERED} newer readers have been published.
     */
    static final class CoverRecorder<R extends DirectoryReader> {
        private final ReferenceManager<R> internal;
        private final LongSupplier processed;
        private final Map<Long, Long> coveredByReader;
        /** The operations processed when the internal refresh under way began; guarded by its refresh lock. */
        private long covering = UNSTAMPED;
        /** The latest reader the internal manager opened and its cover, or -1 when there is none yet; guarded by this. */
        private long latestVersion = -1;
        private long latestCover = UNSTAMPED;
        /** Whether an external refresh is under way; guarded by this. */
        private boolean publishing = false;

        CoverRecorder(ReferenceManager<R> internal, LongSupplier processed, Map<Long, Long> coveredByReader) {
            this.internal = internal;
            this.processed = processed;
            this.coveredByReader = coveredByReader;
        }

        /** The listener for the internal manager: what each reader it opens covers. */
        ReferenceManager.RefreshListener opening() {
            return new ReferenceManager.RefreshListener() {
                @Override
                public void beforeRefresh() {
                    covering = processed.getAsLong();
                }

                @Override
                public void afterRefresh(boolean didRefresh) throws IOException {
                    // The reader a shard opens with was never opened by a refresh;
                    // the first refresh that finds it still current covers it, or
                    // a shard with no new writes would hide every stamped row.
                    if (didRefresh || openedNone()) {
                        opened(version(), covering);
                    }
                }
            };
        }

        /** The listener for the external manager: the readers it may publish are recorded before it does. */
        ReferenceManager.RefreshListener publishing() {
            return new ReferenceManager.RefreshListener() {
                @Override
                public void beforeRefresh() {
                    synchronized (CoverRecorder.this) {
                        publishing = true;
                        if (latestVersion >= 0) {
                            record(latestVersion, latestCover);
                        }
                    }
                }

                @Override
                public void afterRefresh(boolean didRefresh) {
                    synchronized (CoverRecorder.this) {
                        publishing = false;
                    }
                }
            };
        }

        private synchronized boolean openedNone() {
            return latestVersion < 0;
        }

        private synchronized void opened(long version, long cover) {
            latestVersion = version;
            latestCover = cover;
            if (publishing) {
                record(version, cover);
            }
        }

        private void record(long version, long cover) {
            synchronized (coveredByReader) {
                coveredByReader.merge(version, cover, Math::max);
            }
        }

        private long version() throws IOException {
            R reader = internal.acquire();
            try {
                return reader.getVersion();
            } finally {
                internal.release(reader);
            }
        }
    }

    /**
     * The sequence number past which {@code table}'s rows are out of a search
     * over the skeleton reader of version {@code readerVersion}, or null when
     * the search needs no such cut: every row the table may hold was written
     * by an operation that reader holds. A reader too old to be remembered
     * shows no stamped row, which keeps its hits, total and aggregations in
     * agreement at the cost of leaving rows out; so does the reader the engine
     * opened with, which was never refreshed and holds no stamped row.
     */
    static Long visibleSeqNo(String table, long readerVersion) {
        InfinoEngine engine = BY_TABLE.get(table);
        if (engine == null) {
            return null;
        }
        Long covered;
        synchronized (engine.coveredByReader) {
            covered = engine.coveredByReader.get(readerVersion);
        }
        return engine.cutAt(covered == null ? UNSTAMPED : covered);
    }

    /**
     * The generation of the index with UUID {@code indexUuid}: what its
     * stamped rows name, so a table an index outlives (a rebuilt index, or a
     * new one naming the same table) keeps the old index's rows out of the
     * new one's cut, whose sequence numbers start again.
     */
    static long generation(String indexUuid) {
        long h = GENERATION_SEED;
        for (int i = 0; i < indexUuid.length(); i++) {
            h = h * GENERATION_MULTIPLIER + indexUuid.charAt(i);
        }
        return h;
    }

    /** The cut for a reader covering {@code covered}: none while no operation past it has been applied. */
    private Long cutAt(long covered) {
        long max = getSeqNoStats(SequenceNumbers.UNASSIGNED_SEQ_NO).getMaxSeqNo();
        return max <= covered ? null : covered;
    }

    /**
     * Record that the current external skeleton reader holds every operation
     * up to {@code covered}, and stop stamping the ids it holds. What the
     * reader covers for searches was recorded before it was published (see
     * {@link CoverRecorder}).
     */
    private void rememberCovered(long covered) {
        if (covered > latestCovered) {
            latestCovered = covered;
        }
        synchronized (batchLock) {
            unrefreshed.values().removeIf(seq -> seq <= covered);
        }
    }

    /**
     * The write to {@code id} a shard holds for {@code table} and has not
     * committed yet, or null when it holds none: what a realtime read or an
     * update's merge sees before the table does, as a stock shard serves
     * them from its translog.
     */
    static Pending pending(String table, String id) {
        Version v = latestPending(table, id);
        return v == null ? null : new Pending(v.source());
    }

    /** The newest write to {@code id} a shard holds for {@code table} and has not committed: the open batch's, else the one committing. */
    private static Version latestPending(String table, String id) {
        InfinoEngine engine = BY_TABLE.get(table);
        if (engine == null) {
            return null;
        }
        synchronized (engine.batchLock) {
            for (Batch b : new Batch[] { engine.batch, engine.committing }) {
                Version v = b == null ? null : b.latest(id);
                if (v != null) {
                    return v;
                }
            }
            return null;
        }
    }

    /**
     * Whether an update to {@code table} merges here: this node holds the
     * shard's primary, which has not handed the shard off, and the table
     * holds its history ({@link #filled}). Only that copy's pending writes
     * and its reads of the table are as new as the sequence numbers its
     * shard gives. A primary that moved here and has not yet written its
     * history into the table starts doing so ({@link #fillAfterHandOff}),
     * and the update waits for it.
     */
    static boolean mergesHere(String table) {
        InfinoEngine engine = BY_TABLE.get(table);
        if (engine == null || !engine.primaryCopy()) {
            return false;
        }
        ReplicationTracker tracker = engine.config().getGlobalCheckpointSupplier() instanceof ReplicationTracker t ? t : null;
        if (tracker != null && tracker.isRelocated()) {
            return false;
        }
        if (engine.filled) {
            return true;
        }
        if (engine.unfilledLogged.compareAndSet(false, true)) {
            log.info("[infino] updates to {} wait until its table holds the history of {}, which this node now holds as primary", table, engine.shardId);
        }
        if (engine.movedHere && tracker != null && tracker.isPrimaryMode() && engine.handOffFillQueued.compareAndSet(false, true)) {
            engine.config().getThreadPool().executor(ThreadPool.Names.GENERIC).execute(() -> {
                try {
                    synchronized (engine.commitLock) {
                        engine.fillAfterHandOff();
                    }
                } catch (RuntimeException e) {
                    log.debug("[infino] the history of {} was not written into {} for a waiting update", engine.shardId, table, e);
                } finally {
                    engine.handOffFillQueued.set(false);
                }
            });
        }
        return false;
    }

    /**
     * {@code id}'s source for an update to merge into, on the primary that
     * writes {@code table} ({@link #mergesHere}); null when it is deleted or
     * missing. Read after the caller has read the document's sequence
     * number, it is at least as new as that number: a write holds the id's
     * lock from its apply to its record in the batch, so once that lock is
     * taken here, a write the number counts is in the batch, the batch
     * committing, or the table. A write the shard has not committed is read
     * from its batch; otherwise the table is read, from its latest commit
     * while another copy may have written it last ({@link #readsLatest}).
     * {@code shardHoldsId} is whether the shard found the id: when it did
     * not and the skeleton knows every id, the table holds none either.
     */
    static String sourceForMerge(String table, String id, boolean shardHoldsId) {
        InfinoEngine engine = BY_TABLE.get(table);
        if (engine == null) {
            throw new InfinoNative.InfinoException("the shard of " + table + " is not open on this node", RestStatus.SERVICE_UNAVAILABLE);
        }
        Version pending;
        synchronized (engine.idLock(id)) {
            pending = latestPending(table, id);
        }
        if (pending != null) {
            return pending.source();
        }
        if (!shardHoldsId && !engine.looksUpIds()) {
            return null;
        }
        if (engine.readsLatest) {
            engine.holdLatestWhileLookingUp(false);
            return InfinoNative.getSourceLatest(table, id);
        }
        return InfinoNative.getSourceFresh(table, id);
    }

    /** {@code id}'s source as a read sees it now: a pending write's, else the table's; null when it is deleted or missing. */
    static String currentSource(String table, String id) {
        Pending p = pending(table, id);
        return p != null ? p.source() : InfinoNative.getSourceFresh(table, id);
    }

    /**
     * {@code id}'s document as a realtime read sees it now ({@link #currentDocs});
     * null when it is deleted or missing.
     */
    static InfinoNative.Doc currentDoc(String table, String id) {
        return currentDocs(table, List.of(id)).get(id);
    }

    /**
     * The documents of {@code ids} as a realtime read sees them now, by id,
     * each with the sequence number and routing it was written with: a
     * pending write's, else the table's; an id deleted or missing is absent.
     * The ids with no pending write are read from the table in one call,
     * through this node's own connection when it has read every commit
     * ({@link #readsEveryCommit(String)}), else from the table's latest commit.
     */
    static Map<String, InfinoNative.Doc> currentDocs(String table, Collection<String> ids) {
        Map<String, InfinoNative.Doc> out = new HashMap<>();
        List<String> committed = new ArrayList<>();
        for (String id : ids) {
            Version v = latestPending(table, id);
            if (v == null) {
                committed.add(id);
            } else if (v.source() != null) {
                out.put(id, new InfinoNative.Doc(v.source(), v.seq(), v.routing()));
            }
        }
        if (committed.isEmpty()) {
            return out;
        }
        boolean latest = readsEveryCommit(table) == false;
        if (latest) {
            holdLatestForRealtime(table);
        }
        out.putAll(InfinoNative.docsOf(table, committed, latest));
        return out;
    }

    /**
     * How long the latest connection stays held after this node's last
     * realtime read through it, and how long it keeps a table's handle that
     * no realtime read has read. Unheld, each such read opens the connection
     * and the table again (a catalog read and the manifest); held, it costs
     * one pointer read.
     */
    private static final TimeValue REALTIME_LATEST_LINGER = TimeValue.timeValueSeconds(30);
    private static final Object REALTIME_HOLD_LOCK = new Object();
    /** Whether realtime reads hold the latest connection open; under {@link #REALTIME_HOLD_LOCK}. */
    private static boolean realtimeHolds;
    /**
     * When a realtime read last read each table through the latest connection
     * ({@link System#nanoTime}), while realtime reads hold it. The connection
     * keeps a handle (and the manifest) of each table it has read; once a
     * table goes {@link #REALTIME_LATEST_LINGER} unread, the connection is
     * opened afresh, so it holds only the tables read within about twice
     * that, not every table the node has served a realtime read for.
     */
    private static final Map<String, Long> REALTIME_TABLES = new ConcurrentHashMap<>();

    /**
     * Hold the latest connection open for realtime reads of {@code table}
     * until {@link #REALTIME_LATEST_LINGER} passes with none. Without a
     * thread pool to time the release (a unit test), each read opens it alone.
     */
    private static void holdLatestForRealtime(String table) {
        ThreadPool pool = InfinoServices.threadPool();
        if (pool == null) {
            return;
        }
        REALTIME_TABLES.put(table, System.nanoTime());
        synchronized (REALTIME_HOLD_LOCK) {
            if (realtimeHolds) {
                return;
            }
            InfinoNative.holdLatest();
            realtimeHolds = true;
        }
        releaseWhenIdle(pool);
    }

    /**
     * Each {@link #REALTIME_LATEST_LINGER}: forget the tables no realtime read
     * has read for that long, opening the connection afresh so it lets go of
     * their handles when realtime reads still hold it, and letting go of the
     * hold when none is left.
     */
    private static void releaseWhenIdle(ThreadPool pool) {
        try {
            pool.schedule(() -> {
                boolean idle;
                boolean forgot;
                synchronized (REALTIME_HOLD_LOCK) {
                    long now = System.nanoTime();
                    // Removed only when still that old: a read since keeps its table.
                    forgot = REALTIME_TABLES.values().removeIf(read -> now - read >= REALTIME_LATEST_LINGER.nanos());
                    idle = REALTIME_TABLES.isEmpty();
                    if (idle) {
                        realtimeHolds = false;
                    } else {
                        releaseWhenIdle(pool);
                    }
                }
                if (idle) {
                    InfinoNative.releaseLatest();
                } else if (forgot) {
                    InfinoNative.forgetLatest();
                }
            }, REALTIME_LATEST_LINGER, ThreadPool.Names.GENERIC);
        } catch (Exception e) {
            // The pool is shutting down: let go now.
            synchronized (REALTIME_HOLD_LOCK) {
                realtimeHolds = false;
                REALTIME_TABLES.clear();
            }
            InfinoNative.releaseLatest();
        }
    }

    /**
     * Whether this node's own connection has read every commit to
     * {@code table}, so a realtime read may go through it. That connection
     * reads under the engine's staleness bound: for up to its window after
     * another node commits, it may answer from the commit before. Only the
     * shard's primary, once it has committed since it began writing here,
     * knows the last commit is its own, and only until it hands the shard
     * off: the new primary commits from then on, while this copy still
     * answers reads until it closes. Every other node (a replica, a node
     * holding no copy, a primary that has just taken over or has handed off)
     * may be behind, so its realtime reads read the table's latest commit.
     * Searches are not realtime and keep the bound.
     */
    static boolean readsEveryCommit(String table) {
        InfinoEngine engine = BY_TABLE.get(table);
        InfinoWriter writer = InfinoServices.writer();
        return engine != null
            && writer != null
            && readsEveryCommit(
                COPIES.get(engine.shardId),
                handedOff(engine.config().getGlobalCheckpointSupplier()),
                allocation -> writer.caughtUp(table, allocation)
            );
    }

    /**
     * {@link #readsEveryCommit(String)} for this node's {@code copy} of the
     * shard (null for none), {@code handedOff} when that copy has handed the
     * shard off to another node ({@link #handedOff(LongSupplier)}), where
     * {@code caughtUp} says whether this node has committed to the table
     * since it began writing it as a copy ({@link InfinoWriter#caughtUp}).
     */
    static boolean readsEveryCommit(Copy copy, boolean handedOff, Predicate<String> caughtUp) {
        return copy != null && copy.primary() && handedOff == false && caughtUp.test(copy.allocationId());
    }

    /**
     * Whether the copy whose shard's checkpoints {@code checkpoints} reads
     * has handed the shard off to another node (a relocation), after which
     * the new primary writes the table. A copy whose checkpoints cannot be
     * read is taken as not handed off.
     */
    static boolean handedOff(LongSupplier checkpoints) {
        return checkpoints instanceof ReplicationTracker tracker && tracker.isRelocated();
    }

    /**
     * {@code id}'s document as the latest refresh left it, which a get with
     * {@code realtime=false} sees: the table's, unless the id was created
     * since that refresh. A pending write is not seen.
     */
    static InfinoNative.Doc refreshedDoc(String table, String id) {
        InfinoEngine engine = BY_TABLE.get(table);
        if (engine != null) {
            synchronized (engine.batchLock) {
                if (engine.unrefreshed.containsKey(id)) {
                    return null;
                }
            }
        }
        return InfinoNative.getDoc(table, id);
    }

    /** Refresh the copy of {@code table}'s shard this node holds, as a get with {@code refresh=true} does; none, nothing to do. */
    static void refreshFor(String table, String source) {
        InfinoEngine engine = BY_TABLE.get(table);
        if (engine != null) {
            engine.refresh(source);
        }
    }

    /**
     * The paths of the fields the mapper ignored in {@code id}'s document, as
     * a read sees it now: a pending write's, else the table's.
     */
    static List<String> currentIgnored(String table, String id) {
        Version v = latestPending(table, id);
        if (v != null && v.source() != null) {
            return v.ignored();
        }
        return InfinoNative.ignoredOf(table, id);
    }

    @Override
    public void close() throws IOException {
        leaseRefreshStopped = true;
        holdLatestWhileLookingUp(false);
        InfinoWriter writer = InfinoServices.writer();
        if (writer != null) {
            writer.stopReadingLatest(table);
        }
        Scheduler.ScheduledCancellable next = leaseRefresh;
        if (next != null) {
            next.cancel();
        }
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
        return writesTable(origin, primaryCopy());
    }

    /** {@link #writesTable(Operation.Origin)} for a copy that is the shard's primary or not ({@code primary}). */
    static boolean writesTable(Operation.Origin origin, boolean primary) {
        return switch (origin) {
            case PRIMARY -> true;
            case LOCAL_TRANSLOG_RECOVERY -> primary;
            default -> false;
        };
    }

    /**
     * Whether an index operation of this origin, which
     * {@link InfinoSkeletonAdopter#kind} takes for {@code kind}, is written to
     * the table by a copy that is the shard's primary or not
     * ({@code primary}). A skeleton rebuild's create never is, whether the
     * primary applies it or replays it from its translog after a restart:
     * the table holds its row already.
     */
    static boolean writesRow(Operation.Origin origin, boolean primary, InfinoSkeletonAdopter.Kind kind) {
        return writesTable(origin, primary) && kind != InfinoSkeletonAdopter.Kind.REBUILD;
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

    /** Whether the open batch is past its bounds with writes a commit would take now. */
    private boolean batchFull() {
        Batch b = batch;
        return (b.size() >= MAX_BATCH_DOCS || b.bytes >= MAX_BATCH_BYTES)
            && b.anyTakenBy(config().getGlobalCheckpointSupplier().getAsLong());
    }

    @Override
    public IndexResult index(Index index) throws IOException {
        // A skeleton rebuild's create, whose row the table holds already, is
        // written to Lucene alone. The primary knows it by content: this node's
        // rebuild of the shard has it in flight. It carries a mark in its source
        // that the translog and the shard's history keep (the primary puts it
        // back when a plugin wrote the source anew), so a replay after a restart
        // and a copy taking over leave the table alone for it too (see
        // fillTable). A primary write that carries the mark but is no rebuild's
        // is refused, so a client cannot keep a write out of the table with it.
        InfinoSkeletonAdopter.Kind kind = InfinoSkeletonAdopter.kind(shardId, index);
        if (kind == InfinoSkeletonAdopter.Kind.FORGED) {
            return new IndexResult(
                new MapperParsingException(
                    "[infino] a document may not open with " + InfinoSkeletonAdopter.MARK + ": it marks the documents a rebuild creates"
                ),
                index.version()
            );
        }
        boolean toTable = writesRow(index.origin(), primaryCopy(), kind);
        if (toTable) {
            refuseWhileBacklogged(index.origin());
        }
        String source = !toTable || index.source() == null
            ? null
            : InfinoBulkActionFilter.sourceAsJson(index.source(), index.parsedDoc() == null ? null : index.parsedDoc().getMediaType());
        // The fields the mapper ignored for a malformed value: the row leaves them out, as the shard's document does.
        List<String> ignored = source == null ? List.of() : InfinoIgnored.of(index.parsedDoc());
        // The table's columns refuse here, before the write is applied and
        // acknowledged, a document the commit would refuse: one the mapper takes
        // but a column cannot hold (an unsigned_long past the largest signed
        // 64-bit integer, a fractional number for an integer field, an edge's
        // end that is no row id). A replay carries writes already acknowledged.
        // The table is declared from the current mapping first, as the commit
        // declares it: a field mapped since (dynamically, by this very document,
        // or by a put-mapping) has its column, so the check reads the document
        // as the commit will. A failure to declare or check is no verdict on the
        // document: this write alone fails, and the bridge's failures answer 503,
        // so it may be tried again. A document the columns refuse is checked
        // again with the values the mapper read from its date and integer
        // fields (a date in a format of the field's own, a fraction an integer
        // field coerces), which its row then holds.
        String mapped = null;
        if (source != null && !index.origin().isRecovery()) {
            String why;
            try {
                InfinoPlugin.tables().ensure(config().getIndexSettings().getIndexMetadata(), config().getShardId().id());
                why = InfinoNative.checkDoc(table, source, ignored);
                if (why != null) {
                    try {
                        mapped = InfinoMappedValues.of(index.parsedDoc(), config().getMapperService());
                    } catch (RuntimeException e) {
                        // Values the mapper's document does not yield are no values: the bridge's refusal stands.
                        mapped = null;
                    }
                    if (mapped != null) {
                        why = InfinoNative.checkDoc(table, InfinoMappedValues.framed(source, mapped), ignored);
                    }
                }
            } catch (InfinoNative.InfinoException e) {
                return new IndexResult(
                    e.status() == RestStatus.INTERNAL_SERVER_ERROR ? new InfinoNative.InfinoException(e.getMessage(), RestStatus.SERVICE_UNAVAILABLE) : e,
                    index.version()
                );
            } catch (RuntimeException e) {
                return new IndexResult(e, index.version());
            }
            if (why != null) {
                return new IndexResult(InfinoWriter.refusedAtWrite(why), index.version());
            }
        }
        // Lucene keeps the document's identity only: the metadata fields the
        // engine and the fetch phase read, not the user fields or the source.
        for (Document d : index.docs()) {
            d.getFields().removeIf(f -> !f.name().startsWith("_") || SourceFieldMapper.NAME.equals(f.name()));
        }
        keepRecoverySource(index);
        if (!toTable) {
            return super.index(index);
        }
        String id = index.id();
        IndexResult result;
        boolean full = false;
        recordLock.readLock().lock();
        try {
            synchronized (idLock(id)) {
                result = super.index(index);
                if (result.getResultType() != Result.Type.SUCCESS || source == null) {
                    return result;
                }
                synchronized (batchLock) {
                    boolean recovery = index.origin().isRecovery();
                    // A row a reader may not hold the id of yet carries the sequence number that
                    // created it; a replay restores rows the readers since recovery hold.
                    long stamp = UNSTAMPED;
                    if (!recovery) {
                        if (result.isCreated()) {
                            stamp = result.getSeqNo();
                            unrefreshed.put(id, stamp);
                            if (unrefreshed.size() > MAX_UNREFRESHED && unrefreshedRefreshQueued.compareAndSet(false, true)) {
                                config().getThreadPool().executor(ThreadPool.Names.REFRESH).execute(() -> {
                                    try {
                                        maybeRefresh(UNREFRESHED_REFRESH);
                                    } finally {
                                        unrefreshedRefreshQueued.set(false);
                                    }
                                });
                            }
                        } else {
                            stamp = unrefreshed.getOrDefault(id, UNSTAMPED);
                        }
                    }
                    // What the table holds for the id, when the batch has no write of it: a
                    // replay, or a write a rebuilt skeleton takes as new, may find a row there.
                    Base base = recovery || (looksUpIds() && result.isCreated())
                        ? Base.UNKNOWN
                        : result.isCreated() ? Base.ABSENT : Base.PRESENT;
                    batch.add(id, base, new Version(result.getSeqNo(), !recovery, source, index.routing(), stamp, ignored, mapped));
                    full = batchFull();
                }
            }
        } finally {
            recordLock.readLock().unlock();
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
        if (!writesTable(delete.origin())) {
            return super.delete(delete);
        }
        refuseWhileBacklogged(delete.origin());
        String id = delete.id();
        DeleteResult result;
        recordLock.readLock().lock();
        try {
            synchronized (idLock(id)) {
                result = super.delete(delete);
                if (result.getResultType() != Result.Type.SUCCESS) {
                    return result;
                }
                boolean recovery = delete.origin().isRecovery();
                if (result.isFound()) {
                    synchronized (batchLock) {
                        unrefreshed.remove(id);
                        batch.add(id, recovery ? Base.UNKNOWN : Base.PRESENT, new Version(result.getSeqNo(), !recovery, null, null, UNSTAMPED, List.of()));
                    }
                    return result;
                }
                if (!looksUpIds() || !deleteTableOnly(id, result.getSeqNo())) {
                    return result;
                }
            }
        } finally {
            recordLock.readLock().unlock();
        }
        return new FoundInTable(result);
    }

    /**
     * A delete the skeleton did not find and the table did, reported found:
     * at the translog location and with the time of the skeleton's delete, so
     * the request syncs the translog (and so commits the table) through it
     * before it is acknowledged, as for any other delete. A bulk requires a
     * location of every write it applied. The result's setters are
     * package-private in OpenSearch 2.19 and 3.0, hence the overrides.
     */
    private static final class FoundInTable extends DeleteResult {
        private final DeleteResult skeleton;

        FoundInTable(DeleteResult skeleton) {
            super(skeleton.getVersion(), skeleton.getTerm(), skeleton.getSeqNo(), true);
            this.skeleton = skeleton;
        }

        @Override
        public Translog.Location getTranslogLocation() {
            return skeleton.getTranslogLocation();
        }

        @Override
        public long getTook() {
            return skeleton.getTook();
        }
    }

    /**
     * A delete, by operation {@code seq}, the skeleton of a rebuilt index did
     * not find: the table may hold the row from before the rebuild. When it
     * does, the row is deleted and the delete reports it found, as it would
     * have before the rebuild.
     */
    private boolean deleteTableOnly(String id, long seq) {
        holdLatestWhileLookingUp(false);
        if (InfinoNative.existingOsidsLatest(table, List.of(id)).isEmpty()) {
            return false;
        }
        synchronized (batchLock) {
            batch.add(id, Base.PRESENT, new Version(seq, true, null, null, UNSTAMPED, List.of()));
        }
        return true;
    }

    /**
     * Commit every write applied so far before the shard syncs its translog
     * for a write, waiting for the copies to hold them, so a write is in the
     * table before it is acknowledged (see the class comment). A failure
     * fails the sync, and so the write; a wait that runs out, or a commit
     * that leaves writes the table did not take, acknowledges the write all
     * the same, as its copies hold it, and leaves it to a later commit or to
     * the primary that takes over. A hosted service that could not be
     * reached or did not answer is such a commit, not a failure (see
     * {@link #write}): failing the sync with 503 would have a client retry a
     * write the next commit takes anyway, and an auto-id write retried is
     * stored twice.
     */
    protected void commitBeforeSync() {
        if (commitLock == null) {
            return; // a sync from within the base engine's constructor: nothing is pending yet
        }
        long through = getProcessedLocalCheckpoint();
        if (!commitThrough(through)) {
            log.debug("[infino] {} holds operations through {} that {} does not yet at a write's sync", shardId, through, table);
        }
    }

    /**
     * Commit every write before the shard is handed to another node, waiting
     * for the copies to hold them: no write runs while the shard drains its
     * syncs for the hand-off, and the new primary writes the table after it.
     */
    protected void commitBeforeHandoff() {
        if (commitLock == null) {
            return;
        }
        long through = getProcessedLocalCheckpoint();
        if (!commitThrough(through)) {
            log.warn("[infino] {} handed off with writes through {} that are not in {}; the new primary writes them", shardId, through, table);
        }
    }

    /**
     * The sequence number through which the shard's copies hold every
     * operation, which is how far the table may take writes: the shard's
     * global checkpoint, with this copy's own share of it brought up to what
     * it has synced, since OpenSearch records that share only after a write's
     * sync and refresh, which may be the ones asking. A primary that handed
     * its shard off commits nothing more; the new primary writes the table.
     */
    private long copiesHold() {
        LongSupplier checkpoint = config().getGlobalCheckpointSupplier();
        if (!(checkpoint instanceof ReplicationTracker tracker)) {
            if (untrackedLogged.compareAndSet(false, true)) {
                log.warn("[infino] the copies of {} cannot be read; commits to {} take what this copy has synced", shardId, table);
            }
            return getPersistedLocalCheckpoint();
        }
        if (handedOff(tracker)) {
            return NOTHING;
        }
        Copy copy = COPIES.get(shardId);
        long synced = getPersistedLocalCheckpoint();
        if (tracker.isPrimaryMode() && copy != null && copy.allocationId() != null && tracker.getGlobalCheckpoint() < synced) {
            try {
                tracker.updateLocalCheckpoint(copy.allocationId(), synced);
            } catch (AssertionError e) {
                // A hand-off in progress refuses the update where assertions run; it
                // waits for no write, so the checkpoint as recorded serves.
                log.debug("[infino] the checkpoint of {} was not brought up to {}", shardId, synced, e);
            }
        }
        return tracker.getGlobalCheckpoint();
    }

    /**
     * Commit the writes the copies hold to the table; batches reach it in the
     * order they were taken. The translog is synced first (see the class
     * comment). A failure keeps the writes for the next commit and is thrown;
     * false when the table took the commit but not every write of it, which
     * waits for the next commit (see {@link #notTaken}), or when the hosted
     * service's rate limit holds commits back ({@link RateLimit}): then no
     * call is made, and the writes wait, but at a close, which tries once.
     */
    private boolean commitBatch() {
        synchronized (commitLock) {
            if (movedHere && !filled && !fillingAfterHandOff && takesPrimaryWrites()) {
                fillAfterHandOff();
            }
            synchronized (batchLock) {
                if (batch.isEmpty()) {
                    return true;
                }
            }
            if (!closing && rateLimit.waits(System.nanoTime())) {
                return false;
            }
            try {
                translogManager().syncTranslog();
            } catch (IOException e) {
                throw new UncheckedIOException("[infino] syncing the translog of " + shardId + " before a commit to " + table + " failed", e);
            }
            long through = copiesHold();
            Batch b;
            recordLock.writeLock().lock();
            try {
                synchronized (batchLock) {
                    if (batch.allTakenBy(through)) {
                        b = batch;
                        batch = new Batch();
                    } else if (batch.anyTakenBy(through)) {
                        b = batch.take(through);
                    } else {
                        return true;
                    }
                    committing = b;
                }
            } finally {
                recordLock.writeLock().unlock();
            }
            Batch retry = null;
            try {
                retry = commit(b);
            } finally {
                synchronized (batchLock) {
                    committing = null;
                    if (retry == null) {
                        batch.putBack(b);
                    } else if (!retry.isEmpty()) {
                        batch.putBack(retry, false); // each write to retry carries its base (see write)
                    }
                }
            }
            // Every write through `through` is in the table now, but for those to retry.
            holdHistoryThrough(retry.isEmpty() ? through : Math.min(through, retry.lowestSeq() - 1));
            return retry.isEmpty();
        }
    }

    /** The table's lease on this shard's history as this copy was last given it, or null when it has none. */
    private RetentionLease tableLease() {
        if (!(config().getGlobalCheckpointSupplier() instanceof ReplicationTracker tracker)) {
            return null;
        }
        return tracker.getRetentionLeases().get(TABLE_LEASE);
    }

    /**
     * Hold the shard's history from the first operation the table may not
     * hold, {@code covered + 1}: a retention lease ({@link #TABLE_LEASE}) the
     * primary keeps and its copies are given, as they are given every lease.
     * The history a copy keeps is what a new primary writes into the table
     * from ({@link #fillTable}), so it is never trimmed past what the table
     * holds, however long commits fail. Only the primary moves the lease,
     * and only forward.
     */
    private void holdHistoryThrough(long covered) {
        if (covered == NOTHING || !(config().getGlobalCheckpointSupplier() instanceof ReplicationTracker tracker)) {
            return;
        }
        long retaining = covered + 1;
        // A Lucene commit in progress records its history from the leases it persisted; the next commit moves this one.
        if (leaseLock.isWriteLockedByCurrentThread() || !leaseLock.readLock().tryLock()) {
            return;
        }
        try {
            if (!tracker.isPrimaryMode()) {
                return;
            }
            RetentionLease held = tracker.getRetentionLeases().get(TABLE_LEASE);
            if (held == null) {
                tracker.addRetentionLease(
                    TABLE_LEASE,
                    retaining,
                    TABLE_LEASE_SOURCE,
                    ActionListener.wrap(r -> {}, e -> log.debug("[infino] the copies of {} were not given its table's lease yet", shardId, e))
                );
            } else if (held.retainingSequenceNumber() < retaining) {
                tracker.renewRetentionLease(TABLE_LEASE, retaining, TABLE_LEASE_SOURCE);
            }
        } catch (RuntimeException | AssertionError e) {
            // A hand-off in progress refuses lease changes; the new primary holds its own.
            log.debug("[infino] the lease on the history of {} was not moved to {}", shardId, retaining, e);
        } finally {
            leaseLock.readLock().unlock();
        }
    }

    /** Renew the table's lease where it is after a quarter of the lease period, read again each time. */
    private void scheduleLeaseRefresh() {
        if (leaseRefreshStopped) {
            return;
        }
        long delay = Math.max(LEASE_REFRESH_MIN_MILLIS, config().getIndexSettings().getRetentionLeaseMillis() / 4);
        try {
            Scheduler.ScheduledCancellable next = config().getThreadPool()
                .schedule(this::refreshTableLease, TimeValue.timeValueMillis(delay), ThreadPool.Names.GENERIC);
            leaseRefresh = next;
            if (leaseRefreshStopped) {
                next.cancel(); // close() ran between the check above and this schedule, and cancelled the one before
            }
        } catch (OpenSearchRejectedExecutionException e) {
            log.debug("[infino] the renewal of the table lease of {} was not scheduled", shardId, e);
        }
    }

    /**
     * Renew the table's lease where it is: OpenSearch expires a lease not
     * renewed within {@code index.soft_deletes.retention_lease.period}, and
     * {@link #holdHistoryThrough} renews only when it moves, so an idle
     * shard, or one whose commits fail, would lose the history it holds.
     */
    private void refreshTableLease() {
        try {
            if (config().getGlobalCheckpointSupplier() instanceof ReplicationTracker tracker && leaseLock.readLock().tryLock()) {
                try {
                    RetentionLease held = tracker.isPrimaryMode() ? tracker.getRetentionLeases().get(TABLE_LEASE) : null;
                    if (held != null) {
                        tracker.renewRetentionLease(TABLE_LEASE, held.retainingSequenceNumber(), TABLE_LEASE_SOURCE);
                    }
                } finally {
                    leaseLock.readLock().unlock();
                }
            }
        } catch (RuntimeException | AssertionError e) {
            // A commit moved the lease meanwhile, or a hand-off refuses lease changes.
            log.debug("[infino] the table lease of {} was not renewed", shardId, e);
        } finally {
            scheduleLeaseRefresh();
        }
    }

    /**
     * Persist the leases before the commit records the history it keeps from
     * them, and let none move until it has: a renewal changes only the leases
     * in memory, so otherwise a restart can load the table's lease behind
     * that history and {@link #fillTable} refuses the shard (#137).
     */
    @Override
    protected void commitIndexWriter(DocumentIndexWriter writer, String translogUUID) throws IOException {
        if (leaseLock == null) {
            super.commitIndexWriter(writer, translogUUID); // a commit from within the base engine's constructor
            return;
        }
        leaseLock.writeLock().lock();
        try {
            if (config().getGlobalCheckpointSupplier() instanceof ReplicationTracker tracker) {
                tracker.persistRetentionLeases(config().getStore().shardPath().getShardStatePath());
            }
            super.commitIndexWriter(writer, translogUUID);
        } finally {
            leaseLock.writeLock().unlock();
        }
    }

    /**
     * Commit the writes through operation {@code through}, waiting up to
     * {@link #COPIES_WAIT_NANOS} for the copies to hold them; true when the
     * table took every one, false when the wait ran out first or a commit
     * left writes the table did not take for the next one: committing again
     * at once meets the same table, and the writes put back would otherwise
     * hold every sync and refresh for the whole wait.
     */
    private boolean commitThrough(long through) {
        long deadline = System.nanoTime() + COPIES_WAIT_NANOS;
        long pause = COPIES_POLL_MIN_NANOS;
        while (waiting(through)) {
            if (!commitBatch()) {
                return false;
            }
            if (!waiting(through)) {
                return true;
            }
            if (System.nanoTime() - deadline >= 0) {
                return false;
            }
            LockSupport.parkNanos(pause);
            pause = Math.min(pause * 2, COPIES_POLL_MAX_NANOS);
        }
        return true;
    }

    /**
     * Whether a write through operation {@code through} still waits for the
     * table: in the open batch, or in the one committing. Every operation
     * applied by then is recorded first (see {@link #recordLock}).
     */
    private boolean waiting(long through) {
        recordLock.writeLock().lock();
        try {
            synchronized (batchLock) {
                for (Batch b : new Batch[] { batch, committing }) {
                    if (b != null && (b.ungated > 0 || b.lowestGated <= through)) {
                        return true;
                    }
                }
                return false;
            }
        } finally {
            recordLock.writeLock().unlock();
        }
    }

    /**
     * Commit {@code b}'s writes to the table; the writes the table did not
     * take and may take on a later try, to be retried (see
     * {@link #notTaken} and {@link #write}). A failure that takes nothing is
     * thrown.
     */
    private Batch commit(Batch b) {
        long start = System.nanoTime();
        Written w = write(
            table,
            generation,
            b,
            this::holdLatestWhileLookingUp,
            () -> InfinoPlugin.tables().ensure(config().getIndexSettings().getIndexMetadata(), config().getShardId().id()),
            this::withMapped
        );
        rateLimit.after(w.rateLimited(), System.nanoTime());
        InfinoNative.forgetSources(table, w.touched());
        if (!w.touched().isEmpty() && readsLatest) {
            // This node's connection has read the table at its own commit, the latest.
            readsLatest = false;
            holdLatestWhileLookingUp(false);
        }
        log.debug("[infino] engine committed {} writes and {} deletes to {} in {} ms", w.writes(), w.deletes(), table, (System.nanoTime() - start) / 1_000_000L);
        return w.retry();
    }

    /**
     * What {@link #write} did: the writes to retry, the ids it wrote or
     * deleted, how many of each, and whether the hosted service turned any
     * call away for the account's request rate.
     */
    record Written(Batch retry, Set<String> touched, int writes, int deletes, boolean rateLimited) {}

    /**
     * Write {@code b} to {@code table}: its deletes, then its rows in one
     * append. {@code holdLatest} is told, before the rows are looked up,
     * whether any is; {@code declare} runs before the rows are prepared, so
     * a field mapped since the table was declared gets its index.
     * {@code withMapped} gives a write the values OpenSearch's mapper reads
     * from its source, or null when there are none (see {@link #notTaken}).
     *
     * <p>When the hosted service could not be reached or did not answer the
     * lookup, the delete, the prepare or the append (the bridge marks the
     * error {@link InfinoNative.InfinoException#unavailable}, whether its
     * status is 502, 503 or 504), or turned one away for the account's
     * request rate (429), the whole batch is returned to retry rather than
     * the failure thrown ({@link InfinoNative.InfinoException#retryStatus}):
     * the write's sync is acknowledged and the next commit writes the batch,
     * looking each row up first since the service may have committed the
     * append and only its answer was lost, so a retried row replaces the one
     * already there rather than being added again; turned away for the
     * rate, the batch wrote nothing, so each write keeps the base it had. An
     * append the service ended partway reports the documents it did not
     * write one by one instead, as not taken ({@link #notTaken}), and only
     * those are retried. Every write to retry carries the base the next
     * commit starts from.
     */
    static Written write(
        String table,
        long generation,
        Batch b,
        Consumer<Boolean> holdLatest,
        Runnable declare,
        BiFunction<String, Version, Version> withMapped
    ) {
        List<String> ids = new ArrayList<>();
        List<Version> written = new ArrayList<>();
        List<String> sources = new ArrayList<>();
        List<String> routings = new ArrayList<>();
        List<List<String>> ignored = new ArrayList<>();
        List<Long> stamps = new ArrayList<>();
        List<Long> ops = new ArrayList<>();
        Set<String> replaced = new HashSet<>();
        Set<String> lookedUp = new HashSet<>();
        Set<String> deleted = new LinkedHashSet<>();
        for (Map.Entry<String, Entry> me : b.entries.entrySet()) {
            String id = me.getKey();
            Entry e = me.getValue();
            Version last = e.latest();
            if (last.source() == null) {
                if (e.base != Base.ABSENT) {
                    deleted.add(id); // the table may hold a row for it
                }
                continue;
            }
            ids.add(id);
            written.add(last);
            sources.add(last.forTable());
            routings.add(last.routing());
            ignored.add(last.ignored());
            stamps.add(last.stamp());
            ops.add(last.seq());
            if (e.base == Base.PRESENT) {
                replaced.add(id);
            } else if (e.base == Base.UNKNOWN) {
                lookedUp.add(id);
            }
        }
        Batch retry = new Batch();
        boolean rateLimited = false;
        try {
            // An id this node has no record of may be a row another node wrote:
            // a replayed operation, after a takeover, of a write that node
            // committed. The table's latest commit says, so the write replaces
            // the row rather than appending it again.
            holdLatest.accept(!lookedUp.isEmpty());
            if (!lookedUp.isEmpty()) {
                replaced.addAll(InfinoNative.existingOsidsLatest(table, lookedUp));
            }
            // Deletes go first: until a commit's rows are in, the highest operation the
            // table records is an earlier commit's, from which a new primary writes on.
            if (!deleted.isEmpty()) {
                int rc = InfinoNative.deleteOsids(table, deleted.toArray(new String[0]), deleted.size());
                if (rc != 0) {
                    throw InfinoNative.deleteFailed(table, deleted.size(), rc);
                }
            }
            if (!ids.isEmpty()) {
                // The newest write's row alone records its operation: the commit holds every
                // operation up to it, so the highest any row records is how far the table holds
                // every write. A later delete of that row lowers it, which only widens what a
                // new primary writes again.
                long[] seqs = new long[ids.size()];
                long[] opSeqs = new long[ids.size()];
                int newest = 0;
                for (int i = 0; i < ids.size(); i++) {
                    seqs[i] = stamps.get(i);
                    opSeqs[i] = UNSTAMPED;
                    if (ops.get(i) > ops.get(newest)) {
                        newest = i;
                    }
                }
                opSeqs[newest] = ops.get(newest);
                // The mapping may have grown since the table was declared (a dynamic field
                // is mapped before its document is applied): declared first, a new text
                // field gets its text index, where the append would add a plain column.
                declare.run();
                Pointer prepared = InfinoNative.prepare(table, ids, sources, routings, seqs, opSeqs, generation, ignored);
                // The batch carries each row's sequence number from prepare, so the append names none.
                InfinoNative.PreparedResult result = InfinoNative.appendPrepared(table, List.of(prepared), replaced, null);
                retry = notTaken(table, result.failed(), ids, written, replaced, withMapped);
                for (InfinoNative.PreparedFailure f : result.failed()) {
                    rateLimited |= f.status() == RestStatus.TOO_MANY_REQUESTS;
                }
            }
        } catch (InfinoNative.InfinoException e) {
            if (e.retryStatus() == null) {
                throw e;
            }
            rateLimited = e.retryStatus() == RestStatus.TOO_MANY_REQUESTS;
            // The hosted service could not be reached or did not answer (a 502, 503
            // or 504, or no answer at all: the bridge marks each alike), or turned a
            // call away for the account's request rate (429). The whole batch is kept
            // for the next commit, which looks each row up, since the service may
            // have committed the append and only its answer was lost.
            // The write's sync is not failed for it (see commitBeforeSync): a
            // client that retried an auto-id bulk would get its documents twice.
            // A call turned away for the rate wrote nothing, so each write keeps
            // its base; one that was not answered may have landed, so each is
            // looked up.
            log.warn("[infino] the hosted service did not take a commit of {} writes and {} deletes to {}; the next commit tries again: {}", ids.size(), deleted.size(), table, e.getMessage());
            if (!rateLimited) {
                for (Entry entry : b.entries.values()) {
                    entry.base = Base.UNKNOWN;
                }
            }
            retry = b;
        }
        Set<String> touched = new HashSet<>(ids);
        touched.addAll(deleted);
        return new Written(retry, touched, ids.size(), deleted.size(), rateLimited);
    }

    /**
     * Of the documents a commit's append did not take ({@code failed}, by
     * position in {@code ids} and {@code written}), the ones a later commit
     * may. One the table refuses for what it carries is refused the same way
     * on every try, so it is logged as lost and not retried: the engine
     * says so of its own refusal ({@link InfinoNative.PreparedFailure#permanent}),
     * and the table's columns are checked again for any other. Retrying it
     * would hold every later write to the shard behind it. Any other failed
     * for a reason of the moment (the engine's schema moved under the write,
     * storage failed), as does one the table could not be checked against
     * (once a check fails, the rest are not tried); it is put back for the
     * next commit, and the table's
     * hold on the shard's history stays below it until a commit takes it
     * (see {@link #holdHistoryThrough}), so a new primary writes it too.
     *
     * <p>A document the write did not reach because the hosted service ended
     * it carries a status ({@link InfinoNative.PreparedFailure#status}) and is
     * not checked: the service refused nothing it carries. Turned away for
     * the account's request rate (429), or refused for a column its batch was
     * typed without ({@link InfinoNative.PreparedFailure#staleSchema}), it was
     * not written, so the table holds under its id what this commit found
     * ({@code replaced} names the ids that hold a row), and the next commit
     * writes it without looking it up. Any other is looked up, since a call
     * the service did not answer may have written it. The documents put back are logged in one line.
     */
    private static Batch notTaken(
        String table,
        List<InfinoNative.PreparedFailure> failed,
        List<String> ids,
        List<Version> written,
        Set<String> replaced,
        BiFunction<String, Version, Version> withMapped
    ) {
        Batch retry = new Batch();
        boolean checkable = true;
        // The documents put back for the next commit, logged once: how many, how
        // many of them the service turned away for the rate, and one reason.
        int again = 0;
        int limited = 0;
        String why = null;
        for (InfinoNative.PreparedFailure f : failed) {
            String id = ids.get(f.index());
            Version v = written.get(f.index());
            String refused = f.permanent() ? f.reason() : null;
            // A document turned away for the account's request rate carries a status: nothing to check.
            if (refused == null && checkable && f.status() == null) {
                try {
                    refused = InfinoNative.checkDoc(table, v.forTable(), v.ignored());
                } catch (InfinoNative.InfinoException e) {
                    // No verdict on this document or the ones after it: each is tried again.
                    checkable = false;
                }
            }
            if (refused != null && checkable && v.mapped() == null) {
                // A write that reached the commit unchecked (a replay of the shard's history) may
                // need the values the mapper reads: with them, the next commit takes it.
                try {
                    Version withValues = withMapped.apply(id, v);
                    if (withValues != null && InfinoNative.checkDoc(table, withValues.forTable(), withValues.ignored()) == null) {
                        log.debug("[infino] table {} takes document {} (operation {}) at the next commit, with the values the mapper read", table, id, v.seq());
                        retry.add(id, Base.UNKNOWN, withValues);
                        continue;
                    }
                } catch (InfinoNative.InfinoException e) {
                    retry.add(id, Base.UNKNOWN, v);
                    again++;
                    why = why == null ? e.getMessage() : why;
                    continue;
                } catch (RuntimeException e) {
                    log.debug("[infino] the mapper refuses document {} of table {} on replay: {}", id, table, e.getMessage());
                }
            }
            if (refused != null) {
                log.error(
                    "[infino] table {} refuses document {} (operation {}) after OpenSearch acknowledged it; the table does not hold this version, keeps any version it held before, and no commit tries it again: {}",
                    table,
                    id,
                    v.seq(),
                    refused
                );
                continue;
            }
            if (f.status() == RestStatus.TOO_MANY_REQUESTS || f.staleSchema()) {
                retry.add(id, replaced.contains(id) ? Base.PRESENT : Base.ABSENT, v);
                limited += f.staleSchema() ? 0 : 1;
            } else {
                retry.add(id, Base.UNKNOWN, v);
            }
            again++;
            why = why == null ? f.reason() : why;
        }
        if (again > 0) {
            log.warn(
                "[infino] table {} did not take {} documents of a commit ({} turned away for the account's request rate); the next commit tries again: {}",
                table,
                again,
                limited,
                why
            );
        }
        return retry;
    }

    /**
     * {@code v} with the values the index's mapper reads from its source
     * ({@link InfinoMappedValues}); null when it has none. Throws what the
     * mapper throws for a source it refuses.
     */
    private Version withMapped(String id, Version v) {
        if (v.source() == null) {
            return null;
        }
        String mapped = InfinoMappedValues.parse(config().getMapperService(), shardId.getIndexName(), id, v.source(), v.routing());
        return mapped == null ? null : new Version(v.seq(), v.gated(), v.source(), v.routing(), v.stamp(), v.ignored(), mapped);
    }

    /**
     * Commit pending writes through {@code through} before Lucene's refresh
     * or force merge. A failure is logged and Lucene's step still runs:
     * failing it would fail the shard for writes the translog holds and a
     * later commit takes.
     */
    private void commitBeforeLucene(String why, long through) {
        try {
            if (!commitThrough(through)) {
                log.debug("[infino] {} holds operations through {} that {} does not yet at its {}", shardId, through, table, why);
            }
        } catch (RuntimeException e) {
            log.warn("[infino] committing to {} on {} failed", table, why, e);
        }
    }

    /**
     * Commit the writes through every operation applied so far, then refresh
     * the skeleton's reader and record what it covers: every operation
     * applied before the refresh began. Once Lucene's refresh lock is
     * released, and before the refresh is acknowledged, it waits until every
     * read sees the commit ({@link RefreshRelease#release}), so a search after
     * {@code refresh=true}, or one that {@code wait_for} released, finds the
     * writes.
     */
    @Override
    public void refresh(String source) throws EngineException {
        if (commitLock == null) {
            super.refresh(source); // from within the base engine's constructor: nothing is pending or recorded yet
            return;
        }
        commitBeforeLucene("refresh", getProcessedLocalCheckpoint());
        long covered = getProcessedLocalCheckpoint();
        super.refresh(source);
        rememberCovered(covered);
        refreshRelease.release();
    }

    /**
     * The local checkpoint once every write under way is recorded: it
     * reaches each operation applied before the call, as a write holds
     * {@link #recordLock} from its apply to its record.
     */
    private long appliedSoFar() {
        recordLock.writeLock().lock();
        try {
            return getProcessedLocalCheckpoint();
        } finally {
            recordLock.writeLock().unlock();
        }
    }

    @Override
    public boolean maybeRefresh(String source) throws EngineException {
        if (commitLock == null) {
            return super.maybeRefresh(source);
        }
        commitBeforeLucene("refresh", getProcessedLocalCheckpoint());
        long covered = getProcessedLocalCheckpoint();
        boolean refreshed = super.maybeRefresh(source);
        rememberCovered(covered);
        if (refreshed) {
            refreshRelease.release(); // a refresh that did not run is released by the one that did
        }
        return refreshed;
    }

    /**
     * A flush trims the translog, so the writes it holds should be in the
     * table first: a commit that fails fails the flush, and the translog
     * keeps them for the next. A write the copies do not yet hold stays out;
     * the shard's history keeps it for the primary that writes it.
     */
    @Override
    public void flush(boolean force, boolean waitIfOngoing) throws EngineException {
        try {
            if (closing) {
                commitBatch(); // a closing copy does not wait on the others; the history keeps the rest
            } else if (!commitThrough(getProcessedLocalCheckpoint())) {
                log.debug("[infino] {} flushes with writes {} does not hold yet", shardId, table);
            }
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
        commitBeforeLucene("force merge", getProcessedLocalCheckpoint());
        int rc = InfinoNative.optimize(table, config().getIndexSettings().getSettings());
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
        closing = true;
        try {
            commitBatch();
        } catch (RuntimeException e) {
            log.warn("[infino] committing to {} on close failed; closing without the final flush, so the translog keeps the writes", table, e);
            close();
            return;
        }
        super.flushAndClose();
    }

    /**
     * A copy becoming primary (a promotion, or the primary's restart) fills
     * the gaps in its history, then writes into the table the operations of
     * its history the table may not hold (see {@link #fillTable}): the writes
     * its old primary had not committed, which are now the shard's. The table
     * may hold some of them already; a write is looked up, so writing it
     * again leaves one row.
     */
    @Override
    public int fillSeqNoGaps(long primaryTerm) throws IOException {
        int gaps = super.fillSeqNoGaps(primaryTerm);
        if (commitLock != null) {
            Copy copy = COPIES.get(shardId);
            COPIES.put(shardId, new Copy(true, copy == null ? null : copy.allocationId(), false));
            readsLatest = true;
            fillTable(getSeqNoStats(SequenceNumbers.UNASSIGNED_SEQ_NO).getMaxSeqNo());
            filled = true;
            TOOK_OVER.add(shardId);
        }
        return gaps;
    }

    /**
     * Whether this copy takes the shard's writes as its primary: it is in
     * primary mode and has not handed the shard off.
     */
    private boolean takesPrimaryWrites() {
        return config().getGlobalCheckpointSupplier() instanceof ReplicationTracker tracker && tracker.isPrimaryMode() && !tracker.isRelocated();
    }

    /**
     * On a copy that moved here ({@link #movedHere}) and now takes the
     * shard's writes, write into the table the operations of its history
     * the table may not hold ({@link #fillTable}), as a promotion does: the
     * primary it took over from commits its writes as it hands the shard
     * off, but a commit that fails then leaves them out of the table. It
     * runs under {@link #commitLock} before this copy's first commit, so no
     * write of the old primary's reaches the table after a newer one of
     * this copy's. Writes arrive meanwhile, so it reads the history through
     * what this copy has processed; those after it are its own, in the
     * batch. A history that cannot be written fails the engine, as it fails
     * a promotion.
     */
    private void fillAfterHandOff() {
        assert Thread.holdsLock(commitLock);
        if (filled || fillingAfterHandOff) {
            return;
        }
        fillingAfterHandOff = true;
        try {
            readsLatest = true;
            fillTable(getProcessedLocalCheckpoint());
            filled = true;
            log.info("[infino] {} holds the history of {} after its move here; updates merge here", table, shardId);
        } catch (IOException | RuntimeException e) {
            failEngine("writing the history of " + shardId + " into " + table + " after its move failed", e);
            throw new IllegalStateException("[infino] writing the history of " + shardId + " into " + table + " after its move failed", e);
        } finally {
            fillingAfterHandOff = false;
        }
    }

    /**
     * The paths the mapper ignores in a source of the shard's history
     * ({@link InfinoIgnored#parse}); none when {@code mappers} is null. The
     * shard took the document once, so a mapper that refuses it on replay (its
     * mapping moved since) leaves the row to the table's own check.
     */
    private List<String> fillIgnored(MapperService mappers, String id, String source, String routing) {
        try {
            return InfinoIgnored.parse(mappers, shardId.getIndexName(), id, source, routing);
        } catch (RuntimeException e) {
            log.warn("[infino] {} could not parse document {} from its history again for its ignored fields: {}", shardId, id, e.getMessage());
            return List.of();
        }
    }

    /**
     * Write into the table the operations of this copy's history it may not
     * hold: from where the table's lease holds the history
     * ({@link #holdHistoryThrough}), which the copy kept for exactly this.
     * That history must be whole: when it is not, writes acknowledged to
     * clients would be missing from the table that serves them, so the
     * promotion fails rather than serving without them. A shard with no
     * lease (its table has not committed under this engine since it was
     * given one) starts past the highest operation a row records, as far as
     * its history reaches. It writes through operation {@code to}, skipping
     * an operation whose id the batch holds a write of at least as new.
     */
    private void fillTable(long to) throws IOException {
        long retained = getMinRetainedSeqNo();
        RetentionLease lease = tableLease();
        long from;
        if (lease != null) {
            from = lease.retainingSequenceNumber();
            if (from <= to && from < retained) {
                throw new IllegalStateException(
                    "[infino] "
                        + shardId
                        + " keeps its history from "
                        + retained
                        + " only, past where its table's lease holds it ("
                        + from
                        + "): acknowledged writes would be missing from "
                        + table
                );
            }
        } else {
            Long highest = InfinoNative.highestOp(table, generation);
            from = highest == null ? retained : highest + 1;
            if (from < retained) {
                log.warn("[infino] {} keeps its history from {} only; the writes from {} before it may be missing from {}", shardId, retained, from, table);
                from = retained;
            }
        }
        if (from > to) {
            return;
        }
        long start = System.nanoTime();
        int written = 0;
        // The history keeps sources, not parsed documents: an index that may ignore a malformed value parses each again.
        MapperService ignoring = InfinoIgnored.mayIgnore(config().getMapperService()) ? config().getMapperService() : null;
        try (Translog.Snapshot history = newChangesSnapshot(FILL_SOURCE, from, to, lease != null, false)) {
            Translog.Operation op;
            while ((op = history.next()) != null) {
                Version v;
                String id;
                switch (op.opType()) {
                    case INDEX, CREATE -> {
                        Translog.Index index = (Translog.Index) op;
                        if (InfinoSkeletonAdopter.isMarked(index.source())) {
                            continue; // a skeleton rebuild's create: the table holds its row
                        }
                        id = index.id();
                        String source = InfinoBulkActionFilter.sourceAsJson(
                            index.source(),
                            index.source() == null ? null : MediaTypeRegistry.xContentType(index.source())
                        );
                        v = new Version(op.seqNo(), false, source, index.routing(), UNSTAMPED, fillIgnored(ignoring, id, source, index.routing()));
                    }
                    case DELETE -> {
                        id = ((Translog.Delete) op).id();
                        v = new Version(op.seqNo(), false, null, null, UNSTAMPED, List.of());
                    }
                    default -> {
                        continue;
                    }
                }
                boolean full;
                synchronized (idLock(id)) {
                    synchronized (batchLock) {
                        // A newer write of the id waits in the batch already: this copy took it.
                        Version newer = batch.latest(id);
                        if (newer != null && newer.seq() >= op.seqNo()) {
                            continue;
                        }
                        batch.add(id, Base.UNKNOWN, v);
                        full = batchFull();
                    }
                }
                written++;
                if (full) {
                    try {
                        commitBatch();
                    } catch (RuntimeException e) {
                        // The writes stay in the batch for the next commit, as the last one's do.
                        log.warn("[infino] committing part of the history {} took over to {} failed", shardId, table, e);
                    }
                }
            }
        }
        try {
            commitBatch();
        } catch (RuntimeException e) {
            // The writes stay in the batch for the next commit; the shard takes over regardless.
            log.warn("[infino] committing the history {} took over to {} failed", shardId, table, e);
        }
        log.info(
            "[infino] {} wrote {} operations of its history from {} to {} into {} on becoming primary in {} ms",
            shardId,
            written,
            from,
            to,
            table,
            (System.nanoTime() - start) / 1_000_000L
        );
    }
}
