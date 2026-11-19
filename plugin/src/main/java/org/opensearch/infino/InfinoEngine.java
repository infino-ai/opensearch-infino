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
import org.opensearch.common.lucene.index.OpenSearchDirectoryReader;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.concurrency.OpenSearchRejectedExecutionException;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.index.engine.DocumentIndexWriter;
import org.opensearch.index.engine.EngineConfig;
import org.opensearch.index.engine.EngineCreationFailureException;
import org.opensearch.index.engine.EngineException;
import org.opensearch.index.engine.FlushFailedEngineException;
import org.opensearch.index.engine.InternalEngine;
import org.opensearch.index.mapper.MapperParsingException;
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
import java.util.function.LongSupplier;

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
 * keeps the writes the table lacks. Writes wait rather than pile up past a
 * bound while commits fail.
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
     * delete), routing value and stamp, and the operation that made it.
     * A write the copies need not hold first (a replay of the shard's own
     * history) is not gated.
     */
    private record Version(long seq, boolean gated, String source, String routing, long stamp) {
        /** Whether the table may take this write once the copies hold every operation up to {@code through}. */
        boolean takenBy(long through) {
            return !gated || seq <= through;
        }
    }

    /** What the table holds under an id before the first of its waiting writes. */
    private enum Base {
        /** No row: the id is new, or was deleted. */
        ABSENT,
        /** A row, which a write replaces. */
        PRESENT,
        /** Unknown: a write looks the row up at commit. */
        UNKNOWN
    }

    /** The writes of one id waiting for a commit, oldest first. */
    private static final class Entry {
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
    private static final class Batch {
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
            Map<String, Entry> merged = new LinkedHashMap<>();
            for (Map.Entry<String, Entry> me : failed.entries.entrySet()) {
                Entry older = me.getValue();
                older.base = Base.UNKNOWN;
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

    /** Which copy of a shard this node holds: whether it is the primary, and its allocation. */
    private record Copy(boolean primary, String allocationId) {}

    private final String table;
    private final ShardId shardId;
    /** The index's generation, which this engine's stamped rows name (see {@link #generation}). */
    private final long generation;
    /** Whether the skeleton started empty over a table holding rows (see the class comment). */
    private final boolean rebuilt;
    /** Whether the index holds a graph's edges ({@link InfinoPlugin#INFINO_EDGES}), whose columns no mapping type checks. */
    private final boolean edges;
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

    /** The shards of rebuilt indices whose skeleton holds every row of the table again (see {@link InfinoSkeletonAdopter}). */
    private static final Set<ShardId> SKELETON_COMPLETE = ConcurrentHashMap.newKeySet();

    /** Record which copy of {@code shard} this node holds, as {@code routing} says. */
    static void copyOf(ShardId shard, ShardRouting routing) {
        COPIES.put(shard, new Copy(routing.primary(), routing.allocationId() == null ? null : routing.allocationId().getId()));
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

    /** {@code shard}'s skeleton holds a document for every row of its table: ids need no lookup. */
    static void skeletonComplete(ShardId shard) {
        SKELETON_COMPLETE.add(shard);
    }

    /** This node no longer holds a copy of {@code shard}. */
    static void forgetCopy(ShardId shard) {
        COPIES.remove(shard);
        TOOK_OVER.remove(shard);
        SKELETON_COMPLETE.remove(shard);
    }

    /** Whether this node's copy of the shard is its primary. */
    private boolean primaryCopy() {
        Copy copy = COPIES.get(shardId);
        return copy != null && copy.primary();
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
        this.generation = generation(config.getIndexSettings().getUUID());
        this.rebuilt = InfinoPlugin.INFINO_REBUILT.get(config.getIndexSettings().getSettings());
        this.edges = InfinoPlugin.INFINO_EDGES.get(config.getIndexSettings().getSettings());
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

    /** {@code id}'s source as a read sees it now: a pending write's, else the table's; null when it is deleted or missing. */
    static String currentSource(String table, String id) {
        Pending p = pending(table, id);
        return p != null ? p.source() : InfinoNative.getSourceFresh(table, id);
    }

    /**
     * {@code id}'s document as a read sees it now, with the sequence number
     * it was written under: a pending write's, else the table's; null when it
     * is deleted or missing.
     */
    static InfinoNative.Doc currentDoc(String table, String id) {
        Version v = latestPending(table, id);
        if (v != null) {
            return v.source() == null ? null : new InfinoNative.Doc(v.source(), v.seq());
        }
        return InfinoNative.getDoc(table, id);
    }

    /**
     * The routing value {@code id} was last written with, as a read sees it
     * now: a pending write's, else the table's; null when it was written with
     * none.
     */
    static String currentRouting(String table, String id) {
        Version v = latestPending(table, id);
        if (v != null && v.source() != null) {
            return v.routing();
        }
        return InfinoNative.routingOf(table, id);
    }

    @Override
    public void close() throws IOException {
        leaseRefreshStopped = true;
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
        return switch (origin) {
            case PRIMARY -> true;
            case LOCAL_TRANSLOG_RECOVERY -> primaryCopy();
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

    /** Whether the open batch is past its bounds with writes a commit would take now. */
    private boolean batchFull() {
        Batch b = batch;
        return (b.size() >= MAX_BATCH_DOCS || b.bytes >= MAX_BATCH_BYTES)
            && b.anyTakenBy(config().getGlobalCheckpointSupplier().getAsLong());
    }

    @Override
    public IndexResult index(Index index) throws IOException {
        boolean toTable = writesTable(index.origin()) && !skeletonOnly();
        if (toTable) {
            refuseWhileBacklogged(index.origin());
        }
        String source = !toTable || index.source() == null
            ? null
            : InfinoBulkActionFilter.sourceAsJson(index.source(), index.parsedDoc() == null ? null : index.parsedDoc().getMediaType());
        // An edge's ends are row ids, which no mapping type holds, so the mapper
        // cannot refuse an end that is none: the table's columns refuse it here,
        // before the write is applied, as the mapper refuses what it cannot hold.
        if (edges && source != null && !index.origin().isRecovery()) {
            String why = InfinoNative.checkDoc(table, source);
            if (why != null) {
                return new IndexResult(new MapperParsingException("[infino] " + why), index.version());
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
                    Base base = recovery || (!skeletonKnowsIds() && result.isCreated())
                        ? Base.UNKNOWN
                        : result.isCreated() ? Base.ABSENT : Base.PRESENT;
                    batch.add(id, base, new Version(result.getSeqNo(), !recovery, source, index.routing(), stamp));
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
                        batch.add(id, recovery ? Base.UNKNOWN : Base.PRESENT, new Version(result.getSeqNo(), !recovery, null, null, UNSTAMPED));
                    }
                    return result;
                }
                if (skeletonKnowsIds() || !deleteTableOnly(id, result.getSeqNo())) {
                    return result;
                }
            }
        } finally {
            recordLock.readLock().unlock();
        }
        // The found result carries no translog location, so the delete's is
        // synced here when the index syncs for each request: a plain sync, as the
        // delete is not replicated yet and its commit waits for the request's own.
        if (result.getTranslogLocation() != null && config().getIndexSettings().getTranslogDurability() == Translog.Durability.REQUEST) {
            translogManager().syncTranslog();
        }
        return new DeleteResult(result.getVersion(), result.getTerm(), result.getSeqNo(), true);
    }

    /**
     * A delete, by operation {@code seq}, the skeleton of a rebuilt index did
     * not find: the table may hold the row from before the rebuild. When it
     * does, the row is deleted and the delete reports it found, as it would
     * have before the rebuild.
     */
    private boolean deleteTableOnly(String id, long seq) {
        if (InfinoNative.existingOsids(table, List.of(id)).isEmpty()) {
            return false;
        }
        synchronized (batchLock) {
            batch.add(id, Base.PRESENT, new Version(seq, true, null, null, UNSTAMPED));
        }
        return true;
    }

    /**
     * Commit every write applied so far before the shard syncs its translog
     * for a write, waiting for the copies to hold them, so a write is in the
     * table before it is acknowledged (see the class comment). A failure
     * fails the sync, and so the write; a wait that runs out acknowledges
     * the write all the same, as its copies hold it, and leaves it to a later
     * commit or to the primary that takes over.
     */
    protected void commitBeforeSync() {
        if (commitLock == null) {
            return; // a sync from within the base engine's constructor: nothing is pending yet
        }
        long through = getProcessedLocalCheckpoint();
        if (!commitThrough(through)) {
            log.debug("[infino] the copies of {} did not hold every operation through {} in time for a write's sync", shardId, through);
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
            log.warn("[infino] {} handed off with writes past {} its copies did not hold in time; they are not in {}", shardId, through, table);
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
        if (tracker.isRelocated()) {
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
     * comment). A failure keeps the writes for the next commit and is thrown.
     */
    private void commitBatch() {
        synchronized (commitLock) {
            synchronized (batchLock) {
                if (batch.isEmpty()) {
                    return;
                }
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
                        return;
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
                        batch.putBack(retry);
                    }
                }
            }
            // Every write through `through` is in the table now, but for those to retry.
            holdHistoryThrough(retry.isEmpty() ? through : Math.min(through, retry.lowestSeq() - 1));
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
     * table took every one, false when the wait ran out first.
     */
    private boolean commitThrough(long through) {
        long deadline = System.nanoTime() + COPIES_WAIT_NANOS;
        long pause = COPIES_POLL_MIN_NANOS;
        while (waiting(through)) {
            commitBatch();
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
     * {@link #notTaken}). A failure that takes nothing is thrown.
     */
    private Batch commit(Batch b) {
        long start = System.nanoTime();
        List<String> ids = new ArrayList<>();
        List<Version> written = new ArrayList<>();
        List<String> sources = new ArrayList<>();
        List<String> routings = new ArrayList<>();
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
            sources.add(last.source());
            routings.add(last.routing());
            stamps.add(last.stamp());
            ops.add(last.seq());
            if (e.base == Base.PRESENT) {
                replaced.add(id);
            } else if (e.base == Base.UNKNOWN) {
                lookedUp.add(id);
            }
        }
        if (!lookedUp.isEmpty()) {
            replaced.addAll(InfinoNative.existingOsids(table, lookedUp));
        }
        Batch retry = new Batch();
        // Deletes go first: until a commit's rows are in, the highest operation the
        // table records is an earlier commit's, from which a new primary writes on.
        if (!deleted.isEmpty()) {
            int rc = InfinoNative.deleteOsids(table, deleted.toArray(new String[0]), deleted.size());
            if (rc != 0) {
                throw new IllegalStateException("[infino] delete of " + deleted.size() + " ids from " + table + " returned " + rc);
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
            InfinoPlugin.tables().ensure(config().getIndexSettings().getIndexMetadata(), config().getShardId().id());
            Pointer prepared = InfinoNative.prepare(table, ids, sources, routings, seqs, opSeqs, generation);
            if (prepared == null) {
                throw new IllegalStateException("[infino] the bridge could not prepare a batch for " + table);
            }
            // The batch carries each row's sequence number from prepare, so the append names none.
            InfinoNative.PreparedResult result = InfinoNative.appendPrepared(table, List.of(prepared), replaced, null);
            retry = notTaken(result.failed(), ids, written);
        }
        Set<String> touched = new HashSet<>(ids);
        touched.addAll(deleted);
        InfinoNative.forgetSources(table, touched);
        log.debug("[infino] engine committed {} writes and {} deletes to {} in {} ms", ids.size(), deleted.size(), table, (System.nanoTime() - start) / 1_000_000L);
        return retry;
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
     * storage failed); it is put back for the next commit, and the table's
     * hold on the shard's history stays below it until a commit takes it
     * (see {@link #holdHistoryThrough}), so a new primary writes it too.
     */
    private Batch notTaken(List<InfinoNative.PreparedFailure> failed, List<String> ids, List<Version> written) {
        Batch retry = new Batch();
        for (InfinoNative.PreparedFailure f : failed) {
            String id = ids.get(f.index());
            Version v = written.get(f.index());
            String refused = f.permanent() ? f.reason() : InfinoNative.checkDoc(table, v.source());
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
            log.warn("[infino] table {} did not take document {} (operation {}); the next commit tries again: {}", table, id, v.seq(), f.reason());
            retry.add(id, Base.UNKNOWN, v);
        }
        return retry;
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
                log.debug("[infino] the copies of {} did not hold every operation through {} in time for its {}", shardId, through, why);
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
                log.debug("[infino] {} flushes with writes its copies do not hold yet", shardId);
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
        int filled = super.fillSeqNoGaps(primaryTerm);
        if (commitLock != null) {
            Copy copy = COPIES.get(shardId);
            COPIES.put(shardId, new Copy(true, copy == null ? null : copy.allocationId()));
            fillTable();
            TOOK_OVER.add(shardId);
        }
        return filled;
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
     * its history reaches.
     */
    private void fillTable() throws IOException {
        long retained = getMinRetainedSeqNo();
        long to = getSeqNoStats(SequenceNumbers.UNASSIGNED_SEQ_NO).getMaxSeqNo();
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
        try (Translog.Snapshot history = newChangesSnapshot(FILL_SOURCE, from, to, lease != null, false)) {
            Translog.Operation op;
            while ((op = history.next()) != null) {
                Version v;
                String id;
                switch (op.opType()) {
                    case INDEX, CREATE -> {
                        Translog.Index index = (Translog.Index) op;
                        id = index.id();
                        String source = InfinoBulkActionFilter.sourceAsJson(
                            index.source(),
                            index.source() == null ? null : MediaTypeRegistry.xContentType(index.source())
                        );
                        v = new Version(op.seqNo(), false, source, index.routing(), UNSTAMPED);
                    }
                    case DELETE -> {
                        id = ((Translog.Delete) op).id();
                        v = new Version(op.seqNo(), false, null, null, UNSTAMPED);
                    }
                    default -> {
                        continue;
                    }
                }
                boolean full;
                synchronized (batchLock) {
                    batch.add(id, Base.UNKNOWN, v);
                    full = batchFull();
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
