/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import com.sun.jna.Memory;
import com.sun.jna.Pointer;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.ReaderManager;
import org.apache.lucene.search.ReferenceManager;
import org.apache.lucene.store.Directory;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.index.seqno.ReplicationTracker;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** What a skeleton reader covers is known before a search can use it, and refreshes no search sees do not push it out. */
public class InfinoEngineTests extends OpenSearchTestCase {
    /** The ids a batch holds before it commits, as the engine reads it. */
    private static final int BATCH_DOCS = Integer.getInteger("infino.engine.batch.docs", 50_000);
    /** The source bytes a batch holds before it commits, as the engine reads it. */
    private static final long BATCH_BYTES = Long.getLong("infino.engine.batch.bytes", 64L << 20);
    /** The batches' worth of writes past which a client's write is refused. */
    private static final int PENDING_BATCHES = 4;
    /** The writes past which a client's write is refused, and a replay whose commits stall is held. */
    private static final long BOUND = (long) PENDING_BATCHES * BATCH_DOCS;
    /** Commits the hosted service leaves unanswered before it is back, in the replay tests. */
    private static final int UNANSWERED_COMMITS = 5;

    /**
     * The external reader manager as the engine's: it refreshes the internal
     * one, then publishes the internal reader it finds. {@code published}
     * holds, for each reader it publishes, the cover recorded for it then.
     */
    private static final class External extends ReferenceManager<DirectoryReader> {
        private final ReferenceManager<DirectoryReader> internal;
        private final Map<Long, Long> coveredByReader;
        private final Map<Long, Long> published = new HashMap<>();

        External(ReferenceManager<DirectoryReader> internal, Map<Long, Long> coveredByReader) throws IOException {
            this.internal = internal;
            this.coveredByReader = coveredByReader;
            current = internal.acquire();
        }

        @Override
        protected DirectoryReader refreshIfNeeded(DirectoryReader referenceToRefresh) throws IOException {
            internal.maybeRefreshBlocking();
            DirectoryReader reader = internal.acquire();
            if (reader == referenceToRefresh) {
                internal.release(reader);
                return null;
            }
            synchronized (coveredByReader) {
                published.put(reader.getVersion(), coveredByReader.get(reader.getVersion()));
            }
            return reader;
        }

        @Override
        protected void decRef(DirectoryReader reference) throws IOException {
            internal.release(reference);
        }

        @Override
        protected boolean tryIncRef(DirectoryReader reference) {
            return reference.tryIncRef();
        }

        @Override
        protected int getRefCount(DirectoryReader reference) {
            return reference.getRefCount();
        }
    }

    private static long version(ReferenceManager<DirectoryReader> readers) throws IOException {
        DirectoryReader reader = readers.acquire();
        try {
            return reader.getVersion();
        } finally {
            readers.release(reader);
        }
    }

    private static void record(ReaderManager internal, External external, AtomicLong processed, Map<Long, Long> coveredByReader) {
        InfinoEngine.CoverRecorder<DirectoryReader> covers = new InfinoEngine.CoverRecorder<>(internal, processed::get, coveredByReader);
        internal.addListener(covers.opening());
        external.addListener(covers.publishing());
    }

    public void testAReaderIsCoveredBeforeItIsPublished() throws Exception {
        try (Directory dir = newDirectory(); IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig())) {
            Map<Long, Long> coveredByReader = new HashMap<>();
            AtomicLong processed = new AtomicLong(-1);
            try (ReaderManager internal = new ReaderManager(writer); External external = new External(internal, coveredByReader)) {
                record(internal, external, processed, coveredByReader);
                for (long seq = 0; seq < 3; seq++) {
                    writer.addDocument(new Document());
                    processed.set(seq);
                    external.maybeRefreshBlocking();
                    assertEquals(Long.valueOf(seq), external.published.get(version(external)));
                }
            }
        }
    }

    public void testTheReaderAShardOpensWithIsCoveredByARefreshWithNoWrites() throws Exception {
        try (Directory dir = newDirectory(); IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig())) {
            // Rows written before a restart: the shard opens a reader that already holds them.
            for (int i = 0; i < 3; i++) {
                writer.addDocument(new Document());
            }
            writer.commit();
            Map<Long, Long> coveredByReader = new HashMap<>();
            AtomicLong processed = new AtomicLong(2);
            try (ReaderManager internal = new ReaderManager(writer); External external = new External(internal, coveredByReader)) {
                record(internal, external, processed, coveredByReader);
                long opened = version(external);
                external.maybeRefreshBlocking();
                assertEquals(opened, version(external));
                assertEquals(Long.valueOf(2), coveredByReader.get(opened));
            }
        }
    }

    public void testASearchDuringARefreshNeverMeetsAnUncoveredReader() throws Exception {
        try (Directory dir = newDirectory(); IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig())) {
            Map<Long, Long> coveredByReader = new HashMap<>();
            AtomicLong processed = new AtomicLong(-1);
            try (ReaderManager internal = new ReaderManager(writer); External external = new External(internal, coveredByReader)) {
                record(internal, external, processed, coveredByReader);
                writer.addDocument(new Document());
                processed.set(0);
                external.maybeRefreshBlocking();
                AtomicBoolean done = new AtomicBoolean();
                AtomicReference<String> uncovered = new AtomicReference<>();
                CountDownLatch started = new CountDownLatch(1);
                Thread searcher = new Thread(() -> {
                    started.countDown();
                    while (!done.get() && uncovered.get() == null) {
                        try {
                            long v = version(external);
                            synchronized (coveredByReader) {
                                if (!coveredByReader.containsKey(v)) {
                                    uncovered.set("reader " + v + " was searched before its cover was known");
                                }
                            }
                        } catch (IOException e) {
                            uncovered.set(e.toString());
                        }
                    }
                });
                searcher.start();
                started.await();
                for (long seq = 1; seq <= 200; seq++) {
                    writer.addDocument(new Document());
                    processed.set(seq);
                    external.maybeRefreshBlocking();
                }
                done.set(true);
                searcher.join();
                assertNull(uncovered.get(), uncovered.get());
            }
        }
    }

    public void testRefreshesNoSearchSeesKeepThePublishedReadersCover() throws Exception {
        try (Directory dir = newDirectory(); IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig())) {
            // As bounded as the engine's map, at a size a test reaches.
            Map<Long, Long> coveredByReader = new LinkedHashMap<>(16, 0.75f, true) {
                @Override
                protected boolean removeEldestEntry(Map.Entry<Long, Long> eldest) {
                    return size() > 4;
                }
            };
            AtomicLong processed = new AtomicLong(-1);
            try (ReaderManager internal = new ReaderManager(writer); External external = new External(internal, coveredByReader)) {
                record(internal, external, processed, coveredByReader);
                writer.addDocument(new Document());
                processed.set(0);
                external.maybeRefreshBlocking();
                long searched = version(external);
                // Refreshes of the internal manager alone (a flush's, a realtime get's) open readers no search sees.
                for (long seq = 1; seq <= 20; seq++) {
                    writer.addDocument(new Document());
                    processed.set(seq);
                    internal.maybeRefreshBlocking();
                }
                assertEquals(Long.valueOf(0), coveredByReader.get(searched));
                // The next external refresh publishes the latest of them, with its own cover.
                processed.set(99);
                external.maybeRefreshBlocking();
                assertEquals(Long.valueOf(20), external.published.get(version(external)));
            }
        }
    }

    /**
     * A realtime get reads through the node's own connection only on the
     * shard's primary, once it has committed since it began writing as that
     * copy and until it hands the shard off: then the table's last commit is
     * its own. A replica, a node with no copy, a primary that has not
     * committed since it took over, and a primary that has handed off (whose
     * new primary commits from then on) may be behind another node's commit,
     * so each reads the latest commit.
     */
    public void testARealtimeGetReadsTheLatestCommitUnlessTheLastCommitIsThisNodes() {
        String allocation = "alloc-1";
        InfinoEngine.Copy primary = new InfinoEngine.Copy(true, allocation, false);
        assertFalse("no copy here", InfinoEngine.readsEveryCommit(null, false, a -> true));
        assertFalse("no engine here", InfinoEngine.readsEveryCommit("no_such_table"));
        assertFalse("a replica", InfinoEngine.readsEveryCommit(new InfinoEngine.Copy(false, allocation, false), false, a -> true));
        assertFalse("a primary that has not committed since it took over", InfinoEngine.readsEveryCommit(primary, false, a -> false));
        assertFalse("a primary whose last commit was as another copy", InfinoEngine.readsEveryCommit(primary, false, "alloc-0"::equals));
        assertTrue("a primary that has committed as this copy", InfinoEngine.readsEveryCommit(primary, false, allocation::equals));
        assertFalse(
            "a primary that has handed the shard off, until it closes",
            InfinoEngine.readsEveryCommit(primary, true, allocation::equals)
        );
    }

    /**
     * A copy has handed its shard off once its checkpoints say it is
     * relocated: from the hand-off, not from when the copy closes. Checkpoints
     * that cannot be read say nothing of a hand-off.
     */
    public void testACopyHasHandedOffOnceItsTrackerIsRelocated() {
        ReplicationTracker tracker = mock(ReplicationTracker.class);
        when(tracker.isRelocated()).thenReturn(false);
        assertFalse(InfinoEngine.handedOff(tracker));
        when(tracker.isRelocated()).thenReturn(true);
        assertTrue(InfinoEngine.handedOff(tracker));
        assertTrue(
            "a relocated primary reads the latest commit",
            InfinoEngine.readsEveryCommit(new InfinoEngine.Copy(true, "a", false), InfinoEngine.handedOff(tracker), "a"::equals) == false
        );
        assertFalse(InfinoEngine.handedOff(() -> 0L));
    }

    /**
     * A realtime get on a node that may be behind the table's last commit (here
     * one holding no copy of its shard) reads the latest commit, in one call
     * for every id, with each document's routing; never the node's own view.
     */
    public void testARealtimeGetOnANodeThatMayBeBehindReadsTheLatestCommit() {
        List<String> calls = new CopyOnWriteArrayList<>();
        InfinoLib bridge = (InfinoLib) Proxy.newProxyInstance(InfinoLib.class.getClassLoader(), new Class<?>[] { InfinoLib.class }, (proxy, method, args) -> {
            switch (method.getName()) {
                case "infino_open_with":
                    return new Memory(1);
                case "infino_get_doc":
                case "infino_get_docs":
                    calls.add(method.getName());
                    return string("{\"docs\":{}}");
                case "infino_get_docs_latest":
                    calls.add(method.getName() + " " + List.of((String[]) args[2]));
                    return string("{\"docs\":{\"d1\":{\"source\":\"{}\",\"seq\":3,\"routing\":\"r\"}}}");
                default:
                    return null;
            }
        });
        InfinoNative.useLib(bridge);
        try {
            InfinoNative.Doc doc = InfinoEngine.currentDoc("not_held_here", "d1");
            assertEquals(new InfinoNative.Doc("{}", 3, "r"), doc);
            assertEquals(List.of("infino_get_docs_latest [d1]"), calls);
            calls.clear();
            Map<String, InfinoNative.Doc> docs = InfinoEngine.currentDocs("not_held_here", List.of("d1", "d2"));
            assertEquals(Map.of("d1", new InfinoNative.Doc("{}", 3, "r")), docs);
            assertEquals("one call for both ids", List.of("infino_get_docs_latest [d1, d2]"), calls);
        } finally {
            InfinoNative.useLib(null);
        }
    }

    private static Pointer string(String s) {
        byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
        Memory m = new Memory(bytes.length + 1);
        m.write(0, bytes, 0, bytes.length);
        m.setByte(bytes.length, (byte) 0);
        return m;
    }

    /**
     * While commits take nothing, a full batch waits before it commits
     * again: a second, doubling, and never more than thirty seconds.
     */
    public void testAFullBatchWaitsLongerAfterEachStalledCommit() {
        long second = TimeUnit.SECONDS.toNanos(1);
        assertEquals(second, InfinoEngine.stallBackoffNanos(1));
        assertEquals(2 * second, InfinoEngine.stallBackoffNanos(2));
        assertEquals(16 * second, InfinoEngine.stallBackoffNanos(5));
        assertEquals(30 * second, InfinoEngine.stallBackoffNanos(6));
        assertEquals(30 * second, InfinoEngine.stallBackoffNanos(1_000));
        assertEquals(second, InfinoEngine.stallBackoffNanos(0));
    }

    /** A replay is held at the bound a client's write is refused at: four batches' worth, in writes or in bytes. */
    public void testTheReplayBoundIsTheOneAClientWriteIsRefusedAt() {
        assertFalse(InfinoEngine.pastBound(BOUND - 1, 0));
        assertTrue(InfinoEngine.pastBound(BOUND, 0));
        assertFalse(InfinoEngine.pastBound(1, PENDING_BATCHES * BATCH_BYTES - 1));
        assertTrue(InfinoEngine.pastBound(1, PENDING_BATCHES * BATCH_BYTES));
    }

    /**
     * A commit the table took none of is a stall counted by id, not by write:
     * the table puts back one write of each id it did not take, so a batch
     * that holds two writes of an id and is refused whole is a stall. A
     * commit whose deletes landed took something.
     */
    public void testAStallIsCountedByIdSoAnIdWrittenTwiceStillStalls() {
        InfinoEngine.Batch sent = new InfinoEngine.Batch();
        sent.add("a", InfinoEngine.Base.ABSENT, write(1, "{\"v\":1}"));
        sent.add("a", InfinoEngine.Base.ABSENT, write(2, "{\"v\":2}"));
        sent.add("b", InfinoEngine.Base.ABSENT, write(3, "{\"v\":3}"));
        InfinoEngine.Batch retry = new InfinoEngine.Batch();
        retry.add("a", InfinoEngine.Base.UNKNOWN, write(2, "{\"v\":2}"));
        retry.add("b", InfinoEngine.Base.UNKNOWN, write(3, "{\"v\":3}"));
        assertTrue("three writes of two ids, both put back", InfinoEngine.tookNone(sent, retry));

        InfinoEngine.Batch tookOne = new InfinoEngine.Batch();
        tookOne.add("a", InfinoEngine.Base.UNKNOWN, write(2, "{\"v\":2}"));
        assertFalse(InfinoEngine.tookNone(sent, tookOne));
        assertFalse(InfinoEngine.tookNone(sent, new InfinoEngine.Batch()));

        InfinoEngine.Batch withDelete = new InfinoEngine.Batch();
        withDelete.add("a", InfinoEngine.Base.ABSENT, write(1, "{\"v\":1}"));
        withDelete.add("a", InfinoEngine.Base.ABSENT, write(2, "{\"v\":2}"));
        withDelete.add("d", InfinoEngine.Base.PRESENT, write(3, null));
        InfinoEngine.Batch writesBack = new InfinoEngine.Batch();
        writesBack.add("a", InfinoEngine.Base.UNKNOWN, write(2, "{\"v\":2}"));
        assertFalse("the delete landed", InfinoEngine.tookNone(withDelete, writesBack));
    }

    /**
     * A take capped at a number of ids carries the oldest writes, by
     * operation and not by the order each id was first written, and leaves
     * only newer ones: the batch's lowest operation after it, where the
     * table's hold on the history moves to, is past every write it took.
     */
    public void testATakeCappedAtItsIdsCarriesTheOldestWrites() {
        InfinoEngine.Batch batch = new InfinoEngine.Batch();
        batch.add("a", InfinoEngine.Base.ABSENT, write(1, "{}"));
        batch.add("b", InfinoEngine.Base.ABSENT, write(2, "{}"));
        batch.add("c", InfinoEngine.Base.ABSENT, write(3, "{}"));
        batch.add("a", InfinoEngine.Base.ABSENT, write(9, "{}"));
        batch.add("d", InfinoEngine.Base.ABSENT, write(4, "{}"));

        InfinoEngine.Batch first = batch.take(Long.MAX_VALUE, 2, Long.MAX_VALUE);
        assertEquals(List.of("a", "b"), List.copyOf(first.entries.keySet()));
        assertEquals("a's newer write stays", 1, first.entries.get("a").versions.size());
        assertEquals(2L, highestSeq(first));
        assertEquals(3L, batch.lowestSeq());
        assertEquals(InfinoEngine.Base.PRESENT, batch.entries.get("a").base);

        InfinoEngine.Batch second = batch.take(Long.MAX_VALUE, 2, Long.MAX_VALUE);
        assertEquals(List.of("c", "d"), List.copyOf(second.entries.keySet()));
        assertEquals(9L, batch.lowestSeq());

        InfinoEngine.Batch last = batch.take(Long.MAX_VALUE, 2, Long.MAX_VALUE);
        assertEquals(List.of("a"), List.copyOf(last.entries.keySet()));
        assertTrue(batch.isEmpty());
    }

    /** An id's writes in one take go together, oldest first, and count once against the cap. */
    public void testATakeCountsAnIdWrittenSeveralTimesOnce() {
        InfinoEngine.Batch batch = new InfinoEngine.Batch();
        batch.add("a", InfinoEngine.Base.ABSENT, write(1, "{}"));
        batch.add("a", InfinoEngine.Base.ABSENT, write(2, "{}"));
        batch.add("a", InfinoEngine.Base.ABSENT, write(3, null));
        batch.add("b", InfinoEngine.Base.ABSENT, write(4, "{}"));
        batch.add("c", InfinoEngine.Base.ABSENT, write(5, "{}"));

        InfinoEngine.Batch taken = batch.take(Long.MAX_VALUE, 2, Long.MAX_VALUE);
        assertEquals(List.of("a", "b"), List.copyOf(taken.entries.keySet()));
        assertEquals(List.of(1L, 2L, 3L), taken.entries.get("a").versions.stream().map(InfinoEngine.Version::seq).toList());
        assertEquals(4, taken.size());
        assertEquals(List.of("c"), List.copyOf(batch.entries.keySet()));
        assertEquals(1, batch.size());
    }

    /**
     * A take leaves an id's writes past {@code through}, and the writes of the
     * id it left stand on what the taken ones leave in the table; the cap
     * counts only what a commit through it carries.
     */
    public void testATakeLeavesAnIdsWritesPastThrough() {
        InfinoEngine.Batch batch = new InfinoEngine.Batch();
        batch.add("a", InfinoEngine.Base.ABSENT, gated(3, null));
        batch.add("a", InfinoEngine.Base.ABSENT, gated(8, "{}"));
        batch.add("b", InfinoEngine.Base.ABSENT, gated(4, "{}"));
        batch.add("c", InfinoEngine.Base.ABSENT, gated(7, "{}"));

        InfinoEngine.Batch taken = batch.take(5, 2, Long.MAX_VALUE);
        assertEquals(List.of("a", "b"), List.copyOf(taken.entries.keySet()));
        assertEquals(List.of(3L), taken.entries.get("a").versions.stream().map(InfinoEngine.Version::seq).toList());
        assertEquals(List.of("a", "c"), List.copyOf(batch.entries.keySet()));
        assertEquals("a delete was taken", InfinoEngine.Base.ABSENT, batch.entries.get("a").base);
        assertFalse(batch.anyTakenBy(5));
        assertTrue(batch.anyTakenBy(7));
    }

    /** A take stops once it carries about the bytes asked for, with one write at least. */
    public void testATakeIsCappedAtItsBytes() {
        InfinoEngine.Batch batch = new InfinoEngine.Batch();
        batch.add("a", InfinoEngine.Base.ABSENT, write(1, "0123456789"));
        batch.add("b", InfinoEngine.Base.ABSENT, write(2, "0123456789"));
        batch.add("c", InfinoEngine.Base.ABSENT, write(3, "0123456789"));

        assertEquals(List.of("a"), List.copyOf(batch.take(Long.MAX_VALUE, Integer.MAX_VALUE, 1).entries.keySet()));
        InfinoEngine.Batch taken = batch.take(Long.MAX_VALUE, Integer.MAX_VALUE, 15);
        assertEquals(List.of("b", "c"), List.copyOf(taken.entries.keySet()));
        assertEquals(20L, taken.bytes);
        assertTrue(batch.isEmpty());
    }

    /** A 502, 503 or 504 from the bridge is the hosted service not answering; any other failure is not. */
    public void testOnlyAServiceThatDidNotAnswerIsUnanswered() {
        for (RestStatus status : List.of(RestStatus.BAD_GATEWAY, RestStatus.SERVICE_UNAVAILABLE, RestStatus.GATEWAY_TIMEOUT)) {
            assertTrue(status.name(), InfinoEngine.unanswered(new InfinoNative.InfinoException("no answer", status)));
        }
        assertTrue(InfinoEngine.unanswered(new IllegalStateException("wrapped", new InfinoNative.InfinoException("x", RestStatus.GATEWAY_TIMEOUT))));
        for (RestStatus status : List.of(RestStatus.REQUEST_ENTITY_TOO_LARGE, RestStatus.BAD_REQUEST, RestStatus.INTERNAL_SERVER_ERROR)) {
            assertFalse(status.name(), InfinoEngine.unanswered(new InfinoNative.InfinoException("refused", status)));
        }
        assertFalse(InfinoEngine.unanswered(new IllegalStateException("the bridge could not prepare a batch")));
    }

    /**
     * While commits stall, a full batch commits after each wait; past the
     * bound it commits at once until the replay is held there.
     */
    public void testAFullBatchPastTheBoundCommitsAtOnceUntilTheReplayIsHeld() {
        AtomicLong clock = new AtomicLong();
        InfinoEngine.Stalls stalls = new InfinoEngine.Stalls(clock::get, clock::addAndGet);
        assertTrue(stalls.fullBatchMayCommit(false));
        stalls.noteStall(true, "no answer");
        assertFalse(stalls.fullBatchMayCommit(false));
        assertTrue("past the bound after one stall", stalls.fullBatchMayCommit(true));
        stalls.noteStall(true, "no answer");
        assertFalse("held from the second stall", stalls.fullBatchMayCommit(true));
        clock.addAndGet(InfinoEngine.stallBackoffNanos(2));
        assertTrue(stalls.fullBatchMayCommit(false));
        stalls.noteProgress();
        assertEquals(0, stalls.count());
        assertTrue(stalls.fullBatchMayCommit(false));
    }

    /**
     * A replay whose commits the hosted service does not answer waits at the
     * bound, tries again after each wait, and once the service answers goes
     * on to the end: the shard recovers by itself. Its writes never pass the
     * bound by more than one operation.
     */
    public void testAReplayWaitsOutAServiceThatDoesNotAnswerAndGoesOn() {
        FakeReplay replay = new FakeReplay(UNANSWERED_COMMITS, false);
        long ops = 3 * BOUND;
        replay.run(ops);
        assertEquals(ops, replay.added);
        assertEquals("every commit the service did not answer, then the one it took, then the rest", 0, replay.unansweredLeft);
        assertTrue("it waited on its own thread", replay.parks > 0);
        assertTrue("the backoff passed between tries", replay.clock.get() >= InfinoEngine.stallBackoffNanos(1));
        assertTrue("held at the bound: " + replay.most, replay.most <= BOUND + 1);
    }

    /** A replay waiting for the hosted service stops once the engine closes. */
    public void testAReplayWaitingForTheServiceStopsWhenTheEngineCloses() {
        FakeReplay replay = new FakeReplay(Integer.MAX_VALUE, false);
        replay.closeAfterParks = 3;
        IllegalStateException e = expectThrows(IllegalStateException.class, () -> replay.run(3 * BOUND));
        assertTrue(e.getMessage(), e.getMessage().contains("the engine is closing"));
        assertEquals(3, replay.parks);
        assertTrue(replay.most <= BOUND + 1);
    }

    /**
     * A replay whose writes the table refuses stops at the bound with the
     * table's reason, in the first backoff window too: it does not go on
     * adding until a second commit after the wait.
     */
    public void testAReplayTheTableRefusesStopsAtTheBound() {
        FakeReplay replay = new FakeReplay(0, true);
        IllegalStateException e = expectThrows(IllegalStateException.class, () -> replay.run(3 * BOUND));
        assertTrue(e.getMessage(), e.getMessage().contains("took none of its writes (the table refused them)"));
        assertTrue("stopped at the bound: " + replay.most, replay.most <= BOUND + 1);
        assertEquals("it never waited", 0, replay.parks);
    }

    /**
     * A replay that may not wait (it runs under the commit lock) goes on
     * while the hosted service does not answer, and its full batches commit
     * after each wait.
     */
    public void testAReplayThatMayNotWaitGoesOnWhileTheServiceDoesNotAnswer() {
        AtomicLong clock = new AtomicLong();
        InfinoEngine.Stalls stalls = new InfinoEngine.Stalls(clock::get, n -> fail("a replay that may not wait slept"));
        stalls.noteStall(true, "no answer");
        stalls.noteStall(true, "no answer");
        AtomicLong commits = new AtomicLong();
        stalls.hold(new InfinoEngine.Replay() {
            @Override
            public String name() {
                return "replaying a test history";
            }

            @Override
            public String backlog() {
                return "every write";
            }

            @Override
            public void commit() {
                commits.incrementAndGet();
            }

            @Override
            public boolean closing() {
                return false;
            }
        }, false);
        assertEquals(0, commits.get());
        assertFalse(stalls.fullBatchMayCommit(true));
    }

    /** A write of operation {@code seq} that is not gated: a replay's; {@code source} null for a delete. */
    private static InfinoEngine.Version write(long seq, String source) {
        return new InfinoEngine.Version(seq, false, source, null, -1L, List.of());
    }

    /** A primary's write of operation {@code seq}, which waits for the copies to hold it. */
    private static InfinoEngine.Version gated(long seq, String source) {
        return new InfinoEngine.Version(seq, true, source, null, -1L, List.of());
    }

    private static long highestSeq(InfinoEngine.Batch batch) {
        return batch.entries.values().stream().flatMap(e -> e.versions.stream()).mapToLong(InfinoEngine.Version::seq).max().orElseThrow();
    }

    /**
     * A replay that adds one write per operation and commits as the engine's
     * does: it is held after each write, and a full batch commits when the
     * stalls let it. A commit carries one batch's worth. The hosted service
     * leaves the first {@code unansweredLeft} commits unanswered; a table
     * that {@code refuses} takes none of them. The clock moves only while it
     * sleeps.
     */
    private static final class FakeReplay implements InfinoEngine.Replay {
        final AtomicLong clock = new AtomicLong();
        final InfinoEngine.Stalls stalls;
        final boolean refuses;
        int unansweredLeft;
        int closeAfterParks = Integer.MAX_VALUE;
        int parks = 0;
        long waiting = 0;
        long added = 0;
        long most = 0;

        FakeReplay(int unanswered, boolean refuses) {
            this.unansweredLeft = unanswered;
            this.refuses = refuses;
            this.stalls = new InfinoEngine.Stalls(clock::get, nanos -> {
                parks++;
                clock.addAndGet(nanos);
            });
        }

        void run(long ops) {
            for (long op = 0; op < ops; op++) {
                waiting++;
                added++;
                most = Math.max(most, waiting);
                stalls.hold(this, true);
                if (waiting >= BATCH_DOCS && stalls.fullBatchMayCommit(InfinoEngine.pastBound(waiting, 0))) {
                    try {
                        commit();
                    } catch (RuntimeException e) {
                        // kept for the next commit, as the engine keeps its batch
                    }
                }
            }
        }

        @Override
        public String name() {
            return "replaying a test history";
        }

        @Override
        public String backlog() {
            return InfinoEngine.pastBound(waiting, 0) ? waiting + " writes" : null;
        }

        @Override
        public void commit() {
            if (refuses) {
                stalls.noteStall(false, "the table refused them");
                return;
            }
            if (unansweredLeft > 0) {
                unansweredLeft--;
                InfinoNative.InfinoException e = new InfinoNative.InfinoException("no answer", RestStatus.SERVICE_UNAVAILABLE);
                stalls.noteStall(InfinoEngine.unanswered(e), e.getMessage());
                throw e;
            }
            waiting -= Math.min(waiting, BATCH_DOCS);
            stalls.noteProgress();
        }

        @Override
        public boolean closing() {
            return parks >= closeAfterParks;
        }
    }
}
