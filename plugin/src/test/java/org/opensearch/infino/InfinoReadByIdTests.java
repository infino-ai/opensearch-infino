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
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.update.UpdateRequest;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.test.OpenSearchTestCase;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
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
     * An update in default mode whose document cannot be read fails with 503
     * and writes nothing: neither a partial document as the upsert
     * ({@code doc_as_upsert}) nor the upsert body, either of which would
     * create the document or overwrite the one the table holds.
     */
    public void testAnUpdateWhoseDocumentCannotBeReadFailsRatherThanWrites() {
        InfinoNative.useLib(new FakeBridge(FAILED).lib());
        IndexMetadata md = index(InfinoPlugin.MODE_DEFAULT);
        ClusterState state = ClusterState.builder(ClusterName.DEFAULT).metadata(Metadata.builder().put(md, false)).build();
        BulkRequest bulk = new BulkRequest().add(new UpdateRequest("logs", "1").doc(Map.of("f", 2)).docAsUpsert(true))
            .add(new UpdateRequest("logs", "2").doc(Map.of("f", 2)).upsert(Map.of("f", 1)));
        InfinoBulkActionFilter.EngineBulk routed = InfinoBulkActionFilter.engineUpdates(state, bulk, List.of("logs", "logs"), new InfinoTables());
        assertNotNull(routed);
        assertTrue(routed.bulk().requests().toString(), routed.bulk().requests().isEmpty());
        assertEquals(2, routed.answered().size());
        for (BulkItemResponse item : routed.answered().values()) {
            assertTrue(item.isFailed());
            assertEquals(DocWriteRequest.OpType.UPDATE, item.getOpType());
            assertEquals(RestStatus.SERVICE_UNAVAILABLE, item.status());
            assertEquals(RestStatus.SERVICE_UNAVAILABLE, ExceptionsHelper.status(item.getFailure().getCause()));
        }
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

    /** A bridge that opens and declares tables, and answers every lookup by id with {@code answer} (null for none). */
    private static final class FakeBridge implements InvocationHandler {
        private final String answer;

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
