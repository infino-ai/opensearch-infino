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
import org.opensearch.index.seqno.ReplicationTracker;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongConsumer;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** What a skeleton reader covers is known before a search can use it, and refreshes no search sees do not push it out. */
public class InfinoEngineTests extends OpenSearchTestCase {

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
     * In default mode a commit the hosted service did not answer is not a
     * failure: the batch comes back to retry, so the write's sync is
     * acknowledged as before (a 503 there would have a client retry a write
     * the next commit takes anyway, storing an auto-id write twice). Here the
     * service committed the first append and only its answer was lost: the
     * next commit looks the ids up, finds them, and replaces their rows
     * rather than adding them again. A gateway's 502 or 504 is the same case
     * as a 503: the bridge marks each {@code unavailable}.
     */
    public void testADefaultModeCommitTheServiceDidNotAnswerIsRetriedAndLandsOnceWithTheNext() {
        for (int status : new int[] { 502, 503, 504 }) {
            retriedAndLandsOnceWithTheNext(status);
        }
    }

    /** {@link #testADefaultModeCommitTheServiceDidNotAnswerIsRetriedAndLandsOnceWithTheNext} for an unanswered append marked with {@code status}. */
    private void retriedAndLandsOnceWithTheNext(int status) {
        List<String> ids = List.of("a", "b");
        Set<String> inTable = new TreeSet<>();
        List<Set<String>> replacedByAppend = new ArrayList<>();
        AtomicInteger appends = new AtomicInteger();
        InfinoNative.useLib((InfinoLib) Proxy.newProxyInstance(InfinoLib.class.getClassLoader(), new Class<?>[] { InfinoLib.class }, (proxy, method, args) -> switch (method.getName()) {
            case "infino_open_with", "infino_prepare_batch_meta" -> new Memory(1);
            case "infino_sql_latest" -> string(
                inTable.stream().map(id -> "{\"" + InfinoQueryTranslator.OSID + "\":\"" + id + "\"}").reduce((x, y) -> x + "," + y).map(r -> "[" + r + "]").orElse("[]")
            );
            case "infino_append_prepared" -> {
                replacedByAppend.add(new TreeSet<>(Arrays.asList((String[]) args[4])));
                if (appends.incrementAndGet() == 1) {
                    inTable.addAll(ids); // committed, but the answer never came back
                    yield string(
                        "{\"error\":\"the hosted service could not be reached or did not answer in time; try again later\",\"unavailable\":true,\"status\":"
                            + status
                            + "}"
                    );
                }
                yield string("{\"ok\":" + ids.size() + ",\"failed\":[]}");
            }
            case "infino_check_doc" -> throw new AssertionError("a document was checked against a service that did not answer");
            default -> null; // the latest hold and its release, and the frees
        }));
        try {
            InfinoEngine.Batch batch = new InfinoEngine.Batch();
            for (int i = 0; i < ids.size(); i++) {
                batch.add(ids.get(i), InfinoEngine.Base.ABSENT, new InfinoEngine.Version(i, true, "{\"n\":" + i + "}", null, -1, null));
            }

            InfinoEngine.Written first = InfinoEngine.write("t", 1, batch, held -> {}, () -> {}, (id, v) -> null);

            assertEquals(1, appends.get());
            assertEquals("every write comes back to retry after a " + status, Set.copyOf(ids), first.retry().entries.keySet());

            InfinoEngine.Batch open = new InfinoEngine.Batch();
            open.putBack(first.retry()); // as the commit puts back what it did not take
            InfinoEngine.Written second = InfinoEngine.write("t", 1, open, held -> {}, () -> {}, (id, v) -> null);

            assertTrue("the next commit takes every write", second.retry().isEmpty());
            assertEquals(2, appends.get());
            assertEquals("the retried rows replace the ones the first append landed", Set.copyOf(ids), replacedByAppend.get(1));
        } finally {
            InfinoNative.useLib(null);
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

    /**
     * In default mode a commit the hosted service turned away for the
     * account's request rate is not a failure either: turned away whole (a
     * lookup or a prepare answered 429), the batch comes back to retry; with
     * its documents turned away one by one, those come back, unchecked,
     * since the service refused nothing they carry. The next commit takes
     * them, once.
     */
    public void testADefaultModeCommitTheServiceRateLimitsIsRetried() {
        String whole = "{\"error\":\"backend: append: server returned 429: rate limit exceeded\",\"status\":429}";
        String each = "{\"ok\":0,\"failed\":["
            + "{\"b\":0,\"i\":0,\"reason\":\"backend: append: server returned 429: rate limit exceeded\",\"permanent\":false,\"status\":429},"
            + "{\"b\":0,\"i\":1,\"reason\":\"backend: append: server returned 429: rate limit exceeded\",\"permanent\":false,\"status\":429}]}";
        for (String first : new String[] { whole, each }) {
            List<String> ids = List.of("a", "b");
            AtomicInteger appends = new AtomicInteger();
            InfinoNative.useLib((InfinoLib) Proxy.newProxyInstance(InfinoLib.class.getClassLoader(), new Class<?>[] { InfinoLib.class }, (proxy, method, args) -> switch (method.getName()) {
                case "infino_open_with", "infino_prepare_batch_meta" -> new Memory(1);
                case "infino_sql_latest" -> string("[]");
                case "infino_append_prepared" -> string(appends.incrementAndGet() == 1 ? first : "{\"ok\":2,\"failed\":[]}");
                case "infino_check_doc" -> throw new AssertionError("a document turned away for the rate was checked");
                default -> null; // the latest hold and its release, and the frees
            }));
            try {
                InfinoEngine.Batch batch = new InfinoEngine.Batch();
                for (int i = 0; i < ids.size(); i++) {
                    batch.add(ids.get(i), InfinoEngine.Base.ABSENT, new InfinoEngine.Version(i, true, "{\"n\":" + i + "}", null, -1, null));
                }

                InfinoEngine.Written written = InfinoEngine.write("t", 1, batch, held -> {}, () -> {}, (id, v) -> null);
                assertEquals(first, Set.copyOf(ids), written.retry().entries.keySet());

                InfinoEngine.Batch open = new InfinoEngine.Batch();
                open.putBack(written.retry());
                assertTrue(first, InfinoEngine.write("t", 1, open, held -> {}, () -> {}, (id, v) -> null).retry().isEmpty());
                assertEquals(first, 2, appends.get());
            } finally {
                InfinoNative.useLib(null);
            }
        }
    }

    /**
     * In default mode a document the hosted service turned away for the
     * account's request rate was not written, so it comes back with what the
     * commit found under its id, and the next commit writes it without
     * looking its row up again: one lookup across both commits. One whose
     * call the service did not answer (503) may have landed, so it is looked
     * up again. The commit says whether the service rate limited it, and
     * until the pause that follows ends, a commit makes no call at all.
     */
    public void testADefaultModeRateLimitKeepsEachBaseAndPausesTheNextCall() {
        String limited = "backend: append: server returned 429: rate limit exceeded";
        String unanswered = "the hosted service could not be reached or did not answer in time; try again later: unavailable: append: timeout";
        for (int status : new int[] { 429, 503 }) {
            String reason = status == 429 ? limited : unanswered;
            String each = "{\"ok\":0,\"failed\":["
                + "{\"b\":0,\"i\":0,\"reason\":\"" + reason + "\",\"permanent\":false,\"status\":" + status + "},"
                + "{\"b\":0,\"i\":1,\"reason\":\"" + reason + "\",\"permanent\":false,\"status\":" + status + "}]}";
            AtomicInteger appends = new AtomicInteger();
            AtomicInteger lookups = new AtomicInteger();
            InfinoNative.useLib((InfinoLib) Proxy.newProxyInstance(InfinoLib.class.getClassLoader(), new Class<?>[] { InfinoLib.class }, (proxy, method, args) -> switch (method.getName()) {
                case "infino_open_with", "infino_prepare_batch_meta" -> new Memory(1);
                case "infino_sql_latest" -> {
                    lookups.incrementAndGet();
                    yield string("[{\"osid\":\"old\"}]"); // "old" holds a row, "new" none
                }
                case "infino_append_prepared" -> string(appends.incrementAndGet() == 1 ? each : "{\"ok\":2,\"failed\":[]}");
                case "infino_check_doc" -> throw new AssertionError("a document the service ended the write for was checked");
                default -> null; // the latest hold and its release, and the frees
            }));
            try {
                InfinoEngine.Batch batch = new InfinoEngine.Batch();
                batch.add("new", InfinoEngine.Base.UNKNOWN, new InfinoEngine.Version(0, true, "{\"n\":0}", null, -1, null));
                batch.add("old", InfinoEngine.Base.UNKNOWN, new InfinoEngine.Version(1, true, "{\"n\":1}", null, -1, null));

                InfinoEngine.Written written = InfinoEngine.write("t", 1, batch, held -> {}, () -> {}, (id, v) -> null);
                assertEquals(status == 429, written.rateLimited());
                assertEquals(Set.of("new", "old"), written.retry().entries.keySet());
                InfinoEngine.Base absent = status == 429 ? InfinoEngine.Base.ABSENT : InfinoEngine.Base.UNKNOWN;
                InfinoEngine.Base present = status == 429 ? InfinoEngine.Base.PRESENT : InfinoEngine.Base.UNKNOWN;
                assertEquals(absent, written.retry().entries.get("new").base);
                assertEquals(present, written.retry().entries.get("old").base);
                assertEquals(1, lookups.get());

                InfinoEngine.Batch open = new InfinoEngine.Batch();
                open.putBack(written.retry(), false);
                InfinoEngine.Written again = InfinoEngine.write("t", 1, open, held -> {}, () -> {}, (id, v) -> null);
                assertTrue(again.retry().isEmpty());
                assertFalse(again.rateLimited());
                assertEquals(2, appends.get());
                assertEquals("a rate limited document is not looked up again", status == 429 ? 1 : 2, lookups.get());
            } finally {
                InfinoNative.useLib(null);
            }
        }
    }

    /**
     * After a commit the hosted service rate limited, the next commit waits:
     * the pause doubles for each commit turned away in a row, from a second
     * up to its cap, and a random part of it is taken, between half and
     * all. Until it ends a commit makes no call (the counted calls stay put);
     * a commit the service takes clears it.
     */
    public void testARateLimitPausesTheNextCommitDoublingUpToItsCap() {
        long min = InfinoEngine.RATE_LIMIT_PAUSE_MIN_NANOS;
        long max = InfinoEngine.RATE_LIMIT_PAUSE_MAX_NANOS;
        InfinoEngine.RateLimit whole = new InfinoEngine.RateLimit(pause -> pause);
        assertFalse("no limit yet", whole.waits(0));
        long now = 0;
        long expected = min;
        for (int turnedAway = 0; turnedAway < 8; turnedAway++) {
            whole.after(true, now);
            assertTrue(whole.waits(now));
            assertTrue(whole.waits(now + expected - 1));
            assertFalse(whole.waits(now + expected));
            now += expected;
            expected = Math.min(max, expected * 2);
        }
        assertEquals("capped", max, expected);
        whole.after(false, now);
        assertFalse("cleared by a commit the service takes", whole.waits(now));

        // A commit loop as the engine's: it calls the service only when the pause allows.
        InfinoEngine.RateLimit limit = new InfinoEngine.RateLimit(pause -> pause);
        AtomicInteger calls = new AtomicInteger();
        LongConsumer commitAt = at -> {
            if (!limit.waits(at)) {
                calls.incrementAndGet();
                limit.after(true, at); // turned away again
            }
        };
        commitAt.accept(0);
        commitAt.accept(1);
        commitAt.accept(min - 1);
        assertEquals("no call before the pause ends", 1, calls.get());
        commitAt.accept(min);
        assertEquals("one call once it has", 2, calls.get());
        // With the random part, the pause is at least half and at most all of it.
        for (int i = 0; i < 100; i++) {
            InfinoEngine.RateLimit random = new InfinoEngine.RateLimit();
            random.after(true, 0);
            assertTrue(random.waits(min / 2 - 1));
            assertFalse(random.waits(min));
        }
    }

    /**
     * A commit that fails for any other reason than an unanswered hosted
     * service still fails, a 503 the bridge did not mark {@code unavailable}
     * included.
     */
    public void testADefaultModeCommitThatFailsOtherwiseIsThrown() {
        for (String error : new String[] { "{\"error\":\"append: storage failed\"}", "{\"error\":\"append: storage failed\",\"status\":503}" }) {
            thrown(error);
        }
    }

    /** {@link #testADefaultModeCommitThatFailsOtherwiseIsThrown} for an append answered {@code error}. */
    private void thrown(String error) {
        InfinoNative.useLib((InfinoLib) Proxy.newProxyInstance(InfinoLib.class.getClassLoader(), new Class<?>[] { InfinoLib.class }, (proxy, method, args) -> switch (method.getName()) {
            case "infino_open_with", "infino_prepare_batch_meta" -> new Memory(1);
            case "infino_append_prepared" -> string(error);
            default -> null;
        }));
        try {
            InfinoEngine.Batch batch = new InfinoEngine.Batch();
            batch.add("a", InfinoEngine.Base.ABSENT, new InfinoEngine.Version(0, true, "{}", null, -1, null));
            InfinoNative.InfinoException e = expectThrows(
                InfinoNative.InfinoException.class,
                () -> InfinoEngine.write("t", 1, batch, held -> {}, () -> {}, (id, v) -> null)
            );
            assertTrue(e.getMessage(), e.getMessage().contains("storage failed"));
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
}
