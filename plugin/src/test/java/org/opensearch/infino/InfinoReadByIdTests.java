/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import com.sun.jna.Memory;
import com.sun.jna.Pointer;

import org.opensearch.ExceptionsHelper;
import org.opensearch.Version;
import org.opensearch.action.DocWriteRequest;
import org.opensearch.action.bulk.BulkItemResponse;
import org.opensearch.action.update.UpdateRequest;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.index.engine.VersionConflictEngineException;
import org.opensearch.test.OpenSearchTestCase;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A read by id that fails on the native side reaches its caller as an
 * error the client may retry, never as a missing document: an update that
 * took the failure for a missing document would create or overwrite it.
 */
public class InfinoReadByIdTests extends OpenSearchTestCase {

    /** The answer the bridge gives a lookup that fails. */
    private static final String FAILED = "{\"error\":\"table os_logs could not be opened\"}";

    @Override
    public void tearDown() throws Exception {
        InfinoNative.useLib(null);
        super.tearDown();
    }

    /** A source or document is read from under its key, whatever its own fields spell; absent is null. */
    public void testAnAnswerIsReadFromUnderItsKey() {
        assertEquals("{\"error\":\"boom\"}", InfinoNative.sourceOf("{\"source\":\"{\\\"error\\\":\\\"boom\\\"}\"}"));
        assertNull(InfinoNative.sourceOf("{\"source\":null}"));
        InfinoNative.Doc doc = InfinoNative.docOf("{\"doc\":{\"seq\":7,\"source\":\"{\\\"error\\\":\\\"boom\\\"}\"}}");
        assertEquals("{\"error\":\"boom\"}", doc.source());
        assertEquals(7L, doc.seq());
        assertNull(InfinoNative.docOf("{\"doc\":null}"));
        // An answer without its key is not taken for a missing document.
        expectThrows(InfinoNative.InfinoException.class, () -> InfinoNative.sourceOf("{}"));
        expectThrows(InfinoNative.InfinoException.class, () -> InfinoNative.docOf("{}"));
    }

    /**
     * The documents of a read of many ids are read from under {@code docs}:
     * an id {@code error}, or a source whose first field is {@code error},
     * is a document; an answer without {@code docs} is not taken for ids the
     * table does not hold.
     */
    public void testADocumentIdErrorReadsAsADocumentFromManyIds() {
        Map<String, InfinoNative.Doc> docs = InfinoNative.docsFrom(
            "{\"docs\":{\"error\":{\"source\":\"{\\\"error\\\":\\\"boom\\\"}\",\"seq\":4,\"routing\":\"r\"},\"b\":{\"source\":\"{}\",\"seq\":1}}}"
        );
        assertEquals(Map.of("error", new InfinoNative.Doc("{\"error\":\"boom\"}", 4, "r"), "b", new InfinoNative.Doc("{}", 1, null)), docs);
        assertEquals(Map.of(), InfinoNative.docsFrom("{\"docs\":{}}"));
        expectThrows(InfinoNative.InfinoException.class, () -> InfinoNative.docsFrom("{}"));
    }

    /**
     * A read of many ids that fails, from the latest commit or through the
     * node's own connection, throws with 503 rather than answering the ids
     * as missing.
     */
    public void testAFailedReadOfManyIdsThrowsARetryableStatus() {
        for (String answer : new String[] { FAILED, null }) {
            for (boolean latest : new boolean[] { true, false }) {
                FakeBridge bridge = new FakeBridge(answer);
                InfinoNative.useLib(bridge.lib());
                InfinoNative.InfinoException e = expectThrows(
                    InfinoNative.InfinoException.class,
                    () -> InfinoNative.docsOf("os_logs", List.of("1", "2"), latest)
                );
                assertEquals(RestStatus.SERVICE_UNAVAILABLE, e.status());
                assertEquals(List.of(latest ? "infino_get_docs_latest" : "infino_get_docs"), bridge.reads);
            }
        }
    }

    /**
     * A realtime get in default mode on a node that may not have read the
     * last commit reads the latest commit; when that read fails the get
     * fails with 503, never 404.
     */
    public void testAFailedLatestReadIsUnavailableNotMissing() {
        FakeBridge bridge = new FakeBridge(FAILED);
        InfinoNative.useLib(bridge.lib());
        IndexMetadata md = index(InfinoPlugin.MODE_DEFAULT);
        InfinoNative.InfinoException e = expectThrows(
            InfinoNative.InfinoException.class,
            () -> InfinoGetActionFilter.docOf(md, "os_logs", "1", true)
        );
        assertEquals(RestStatus.SERVICE_UNAVAILABLE, e.status());
        assertNotEquals(RestStatus.NOT_FOUND, e.status());
        assertEquals(List.of("infino_get_docs_latest"), bridge.reads);
    }

    /**
     * A get or multi-get in proxy mode on a node that has not read the
     * writer's last commit (here no cluster names this node the writer)
     * reads the table's latest commit, realtime or not: the node's own view
     * may trail it, and miss a document written moments before or report an
     * older write's sequence number. Proxy mode has no refresh for a get
     * that is not realtime to stop at.
     */
    public void testAProxyReadOffTheWriterReadsTheLatestCommit() {
        IndexMetadata md = index(InfinoPlugin.MODE_PROXY);
        for (boolean realtime : new boolean[] { true, false }) {
            FakeBridge bridge = new FakeBridge("{\"docs\":{\"1\":{\"source\":\"{}\",\"seq\":3}}}");
            InfinoNative.useLib(bridge.lib());
            assertEquals(new InfinoNative.Doc("{}", 3, null), InfinoGetActionFilter.docOf(md, "os_logs", "1", realtime));
            assertEquals(
                Map.of("1", new InfinoNative.Doc("{}", 3, null)),
                InfinoGetActionFilter.proxyDocs(md, "os_logs", List.of("1"), InfinoAccess.NONE)
            );
            assertEquals("realtime=" + realtime, List.of("infino_get_docs_latest", "infino_get_docs_latest"), bridge.reads);
        }
    }

    /** A lookup the bridge fails, or answers with nothing at all, throws with 503 from both reads. */
    public void testAFailedLookupThrowsARetryableStatus() {
        for (String answer : new String[] { FAILED, null }) {
            InfinoNative.useLib(new FakeBridge(answer).lib());
            InfinoNative.InfinoException e = expectThrows(InfinoNative.InfinoException.class, () -> InfinoNative.getSourceFresh("os_logs", "1"));
            assertEquals(RestStatus.SERVICE_UNAVAILABLE, e.status());
            e = expectThrows(InfinoNative.InfinoException.class, () -> InfinoNative.getDoc("os_logs", "1"));
            assertEquals(RestStatus.SERVICE_UNAVAILABLE, e.status());
        }
    }

    /** A get fails rather than answering found: false, in proxy mode and in default mode, realtime or not. */
    public void testAGetFailsRatherThanAnsweringMissing() {
        InfinoNative.useLib(new FakeBridge(FAILED).lib());
        for (String mode : new String[] { InfinoPlugin.MODE_PROXY, InfinoPlugin.MODE_DEFAULT }) {
            IndexMetadata md = index(mode);
            for (boolean realtime : new boolean[] { true, false }) {
                InfinoNative.InfinoException e = expectThrows(
                    InfinoNative.InfinoException.class,
                    () -> InfinoGetActionFilter.docOf(md, "os_logs", "1", realtime)
                );
                assertEquals(mode + " realtime=" + realtime, RestStatus.SERVICE_UNAVAILABLE, e.status());
            }
        }
    }

    /**
     * An update in default mode whose document cannot be read on the primary
     * that merges it fails with 503 and writes nothing: neither a partial
     * document as the upsert ({@code doc_as_upsert}) nor the upsert body,
     * either of which would create the document or overwrite the one the
     * table holds. The source to merge into is read from the table, from
     * its latest commit, or not at all when the shard is not open here;
     * each failure is the update's answer, and the update does not run
     * again, as it would on a version conflict.
     */
    public void testAnUpdateWhoseDocumentCannotBeReadFailsRatherThanWrites() {
        InfinoNative.useLib(new FakeBridge(FAILED).lib());
        List<Exception> failedReads = List.of(
            expectThrows(InfinoNative.InfinoException.class, () -> InfinoNative.getSourceFresh("os_logs", "1")),
            expectThrows(InfinoNative.InfinoException.class, () -> InfinoNative.getSourceLatest("os_logs", "1")),
            expectThrows(InfinoNative.InfinoException.class, () -> InfinoEngine.sourceForMerge("os_logs", "1", true))
        );
        List<UpdateRequest> updates = List.of(
            new UpdateRequest("logs", "1").doc(Map.of("f", 2)).docAsUpsert(true).retryOnConflict(3),
            new UpdateRequest("logs", "2").doc(Map.of("f", 2)).upsert(Map.of("f", 1)).retryOnConflict(3)
        );
        for (Exception read : failedReads) {
            for (UpdateRequest update : updates) {
                BulkItemResponse item = InfinoBulkActionFilter.unmerged(0, update, "logs", read, new HashMap<>());
                assertNotNull(read.getMessage(), item);
                assertTrue(item.isFailed());
                assertEquals(DocWriteRequest.OpType.UPDATE, item.getOpType());
                assertEquals(RestStatus.SERVICE_UNAVAILABLE, item.status());
                assertEquals(RestStatus.SERVICE_UNAVAILABLE, ExceptionsHelper.status(item.getFailure().getCause()));
            }
        }
        // A conflict, unlike a failed read, runs the update again.
        Exception conflict = new VersionConflictEngineException(new ShardId("logs", "logs-uuid", 0), "1", "moved");
        assertNull(InfinoBulkActionFilter.unmerged(0, updates.get(0), "logs", conflict, new HashMap<>()));
    }

    private static IndexMetadata index(String mode) {
        return IndexMetadata.builder("logs")
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                    .put(IndexMetadata.SETTING_INDEX_UUID, "logs-uuid")
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                    .put(InfinoPlugin.INFINO_ENABLED.getKey(), true)
                    .put(InfinoPlugin.INFINO_MODE.getKey(), mode)
            )
            .build();
    }

    /** A bridge that opens and declares tables, and answers every read by id, of one id or many, with {@code answer} (null for none). */
    private static final class FakeBridge implements InvocationHandler {
        private final String answer;
        /** The reads by id asked of the bridge, in order. */
        final List<String> reads = new ArrayList<>();

        FakeBridge(String answer) {
            this.answer = answer;
        }

        InfinoLib lib() {
            return (InfinoLib) Proxy.newProxyInstance(InfinoLib.class.getClassLoader(), new Class<?>[] { InfinoLib.class }, this);
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) {
            switch (method.getName()) {
                case "infino_open_with":
                    return new Memory(1);
                case "infino_ensure_table":
                    return string("{}");
                case "infino_get":
                case "infino_get_doc":
                case "infino_get_docs":
                case "infino_get_docs_latest":
                    reads.add(method.getName());
                    return answer == null ? null : string(answer);
                default:
                    return null; // the frees
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
}
