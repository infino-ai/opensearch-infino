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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

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
     * rather than adding them again.
     */
    public void testADefaultModeCommitTheServiceDidNotAnswerIsRetriedAndLandsOnceWithTheNext() {
        List<String> ids = List.of("a", "b");
        Set<String> inTable = new TreeSet<>();
        List<Set<String>> replacedByAppend = new ArrayList<>();
        AtomicInteger appends = new AtomicInteger();
        InfinoNative.useLib((InfinoLib) Proxy.newProxyInstance(InfinoLib.class.getClassLoader(), new Class<?>[] { InfinoLib.class }, (proxy, method, args) -> switch (method.getName()) {
            case "infino_open_with", "infino_prepare_batch_meta" -> new Memory(1);
            case "infino_sql_latest" -> nativeString(
                inTable.stream().map(id -> "{\"" + InfinoQueryTranslator.OSID + "\":\"" + id + "\"}").reduce((x, y) -> x + "," + y).map(r -> "[" + r + "]").orElse("[]")
            );
            case "infino_append_prepared" -> {
                replacedByAppend.add(new TreeSet<>(Arrays.asList((String[]) args[4])));
                if (appends.incrementAndGet() == 1) {
                    inTable.addAll(ids); // committed, but the answer never came back
                    yield nativeString("{\"error\":\"the hosted service could not be reached or did not answer in time; try again later\",\"status\":503}");
                }
                yield nativeString("{\"ok\":" + ids.size() + ",\"failed\":[]}");
            }
            case "infino_check_doc" -> throw new AssertionError("a document was checked against a service that did not answer");
            default -> null; // the latest hold and its release, and the frees
        }));
        try {
            InfinoEngine.Batch batch = new InfinoEngine.Batch();
            for (int i = 0; i < ids.size(); i++) {
                batch.add(ids.get(i), InfinoEngine.Base.ABSENT, new InfinoEngine.Version(i, true, "{\"n\":" + i + "}", null, -1));
            }

            InfinoEngine.Written first = InfinoEngine.write("t", 1, batch, held -> {}, () -> {});

            assertEquals(1, appends.get());
            assertEquals("every write comes back to retry", Set.copyOf(ids), first.retry().entries.keySet());

            InfinoEngine.Batch open = new InfinoEngine.Batch();
            open.putBack(first.retry()); // as the commit puts back what it did not take
            InfinoEngine.Written second = InfinoEngine.write("t", 1, open, held -> {}, () -> {});

            assertTrue("the next commit takes every write", second.retry().isEmpty());
            assertEquals(2, appends.get());
            assertEquals("the retried rows replace the ones the first append landed", Set.copyOf(ids), replacedByAppend.get(1));
        } finally {
            InfinoNative.useLib(null);
        }
    }

    /** A commit that fails for any other reason than an unanswered hosted service still fails. */
    public void testADefaultModeCommitThatFailsOtherwiseIsThrown() {
        InfinoNative.useLib((InfinoLib) Proxy.newProxyInstance(InfinoLib.class.getClassLoader(), new Class<?>[] { InfinoLib.class }, (proxy, method, args) -> switch (method.getName()) {
            case "infino_open_with", "infino_prepare_batch_meta" -> new Memory(1);
            case "infino_append_prepared" -> nativeString("{\"error\":\"append: storage failed\"}");
            default -> null;
        }));
        try {
            InfinoEngine.Batch batch = new InfinoEngine.Batch();
            batch.add("a", InfinoEngine.Base.ABSENT, new InfinoEngine.Version(0, true, "{}", null, -1));
            InfinoNative.InfinoException e = expectThrows(
                InfinoNative.InfinoException.class,
                () -> InfinoEngine.write("t", 1, batch, held -> {}, () -> {})
            );
            assertTrue(e.getMessage(), e.getMessage().contains("storage failed"));
        } finally {
            InfinoNative.useLib(null);
        }
    }

    private static Pointer nativeString(String s) {
        byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
        Memory m = new Memory(bytes.length + 1);
        m.write(0, bytes, 0, bytes.length);
        m.setByte(bytes.length, (byte) 0);
        return m;
    }
}
