/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.ReaderManager;
import org.apache.lucene.search.ReferenceManager;
import org.apache.lucene.store.Directory;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
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
     * A realtime get reads through the node's own connection only on the
     * shard's primary, once it has committed since it began writing as that
     * copy: then the table's last commit is its own. A replica, a node with
     * no copy, and a primary that has not committed since it took over may
     * be behind another node's commit, so each reads the latest commit.
     */
    public void testARealtimeGetReadsTheLatestCommitUnlessTheLastCommitIsThisNodes() {
        String allocation = "alloc-1";
        assertFalse("no copy here", InfinoEngine.readsEveryCommit(null, a -> true));
        assertFalse("no engine here", InfinoEngine.readsEveryCommit("no_such_table"));
        assertFalse("a replica", InfinoEngine.readsEveryCommit(new InfinoEngine.Copy(false, allocation), a -> true));
        assertFalse(
            "a primary that has not committed since it took over",
            InfinoEngine.readsEveryCommit(new InfinoEngine.Copy(true, allocation), a -> false)
        );
        assertFalse(
            "a primary whose last commit was as another copy",
            InfinoEngine.readsEveryCommit(new InfinoEngine.Copy(true, allocation), "alloc-0"::equals)
        );
        assertTrue(
            "a primary that has committed as this copy",
            InfinoEngine.readsEveryCommit(new InfinoEngine.Copy(true, allocation), allocation::equals)
        );
    }
}
