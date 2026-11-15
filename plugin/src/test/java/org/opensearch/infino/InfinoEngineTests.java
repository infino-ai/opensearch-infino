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
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/** What a skeleton reader covers is known before a search can use the reader. */
public class InfinoEngineTests extends OpenSearchTestCase {

    /**
     * The external reader manager as the engine's: it refreshes the internal
     * one, then publishes the internal reader it finds. {@code published}
     * holds, for each reader it publishes, what was recorded for it then.
     */
    private static final class External extends ReferenceManager<DirectoryReader> {
        private final ReferenceManager<DirectoryReader> internal;
        private final InfinoEngine.ReaderCovers coveredByReader;
        private final Map<Long, Long> published = new HashMap<>();

        External(ReferenceManager<DirectoryReader> internal, InfinoEngine.ReaderCovers coveredByReader) throws IOException {
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
            published.put(reader.getVersion(), coveredByReader.covered(reader.getVersion()));
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

    public void testAReaderIsCoveredBeforeItIsPublished() throws Exception {
        try (Directory dir = newDirectory(); IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig())) {
            InfinoEngine.ReaderCovers coveredByReader = new InfinoEngine.ReaderCovers();
            AtomicLong processed = new AtomicLong(-1);
            try (
                ReaderManager internal = new ReaderManager(writer);
                External external = new External(internal, coveredByReader)
            ) {
                internal.addListener(new InfinoEngine.CoverRecorder<>(internal, processed::get, coveredByReader));
                for (long seq = 0; seq < 3; seq++) {
                    writer.addDocument(new Document());
                    processed.set(seq);
                    external.maybeRefreshBlocking();
                    DirectoryReader reader = external.acquire();
                    try {
                        assertEquals(Long.valueOf(seq), external.published.get(reader.getVersion()));
                    } finally {
                        external.release(reader);
                    }
                }
                // A refresh with nothing new opens no reader and records nothing.
                DirectoryReader last = external.acquire();
                try {
                    processed.set(10);
                    external.maybeRefreshBlocking();
                    assertEquals(Long.valueOf(2), coveredByReader.covered(last.getVersion()));
                } finally {
                    external.release(last);
                }
            }
        }
    }

    public void testASearchDuringARefreshNeverMeetsAnUncoveredReader() throws Exception {
        try (Directory dir = newDirectory(); IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig())) {
            InfinoEngine.ReaderCovers coveredByReader = new InfinoEngine.ReaderCovers();
            AtomicLong processed = new AtomicLong(-1);
            try (
                ReaderManager internal = new ReaderManager(writer);
                External external = new External(internal, coveredByReader)
            ) {
                internal.addListener(new InfinoEngine.CoverRecorder<>(internal, processed::get, coveredByReader));
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
                            DirectoryReader reader = external.acquire();
                            try {
                                if (coveredByReader.covered(reader.getVersion()) == null) {
                                    uncovered.set("reader " + reader.getVersion() + " was searched before its cover was known");
                                }
                            } finally {
                                external.release(reader);
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

    public void testInternalRefreshesKeepThePublishedReadersCover() throws Exception {
        try (Directory dir = newDirectory(); IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig())) {
            InfinoEngine.ReaderCovers coveredByReader = new InfinoEngine.ReaderCovers();
            AtomicLong processed = new AtomicLong(-1);
            try (
                ReaderManager internal = new ReaderManager(writer);
                External external = new External(internal, coveredByReader)
            ) {
                internal.addListener(new InfinoEngine.CoverRecorder<>(internal, processed::get, coveredByReader));
                external.addListener(new InfinoEngine.CoverPublisher<>(external, coveredByReader));
                writer.addDocument(new Document());
                processed.set(0);
                external.maybeRefreshBlocking();
                long searched;
                DirectoryReader reader = external.acquire();
                try {
                    searched = reader.getVersion();
                } finally {
                    external.release(reader);
                }
                // With external refresh off, only the engine's own refreshes open readers.
                writer.addDocument(new Document());
                processed.set(1);
                internal.maybeRefreshBlocking();
                long unpublished;
                reader = internal.acquire();
                try {
                    unpublished = reader.getVersion();
                } finally {
                    internal.release(reader);
                }
                for (long seq = 2; seq < InfinoEngine.READERS_REMEMBERED + 2; seq++) {
                    writer.addDocument(new Document());
                    processed.set(seq);
                    internal.maybeRefreshBlocking();
                }
                assertEquals(Long.valueOf(0), coveredByReader.covered(searched));
                // A reader no search was given is forgotten past the bound.
                assertNull(coveredByReader.covered(unpublished));
            }
        }
    }
}
