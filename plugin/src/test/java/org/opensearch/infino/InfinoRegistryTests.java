/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import com.sun.jna.Memory;
import com.sun.jna.Pointer;

import org.opensearch.Version;
import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.IndexScopedSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.test.ClusterServiceUtils;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class InfinoRegistryTests extends OpenSearchTestCase {

    /** The index settings a node with no plugin knows. */
    private static final IndexScopedSettings KNOWN = new IndexScopedSettings(Settings.EMPTY, IndexScopedSettings.BUILT_IN_INDEX_SETTINGS);

    public void testSettingsTheClusterDoesNotKnowOrCannotTakeAreLeftOutAndNamed() {
        Settings given = Settings.builder()
            .put("index.number_of_shards", "1")
            .put("index.refresh_interval", "5s")
            // Another engine's lifecycle settings, which this cluster has no setting for.
            .put("index.lifecycle.name", "logs")
            .put("index.lifecycle.rollover_alias", "logs-write")
            // A setting this cluster knows, with a value it cannot take.
            .put("index.max_result_window", "many")
            .build();
        List<String> leftOut = new ArrayList<>();
        Settings kept = InfinoRegistry.knownSettings(given, KNOWN, leftOut);
        assertEquals("1", kept.get("index.number_of_shards"));
        assertEquals("5s", kept.get("index.refresh_interval"));
        assertNull(kept.get("index.lifecycle.name"));
        assertNull(kept.get("index.max_result_window"));
        assertTrue("nothing is archived into the request", kept.keySet().stream().noneMatch(key -> key.startsWith("archived.")));
        leftOut.sort(String::compareTo);
        assertEquals(List.of("index.lifecycle.name", "index.lifecycle.rollover_alias", "index.max_result_window"), leftOut);
    }

    public void testAKnownRowKeepsEverySettingAndNamesNone() {
        Map<String, Object> row = Map.of("settings", Map.of("index.number_of_shards", "1", "index.default_pipeline", "tag_logs"));
        List<String> leftOut = new ArrayList<>();
        CreateIndexRequest request = InfinoRegistry.requestOf("logs", row, KNOWN, leftOut);
        assertEquals("tag_logs", request.settings().get("index.default_pipeline"));
        assertEquals("1", request.settings().get("index.number_of_shards"));
        assertTrue(leftOut.isEmpty());
    }

    public void testOfTwoRowsUnderOneIdEveryReadTakesTheSameOne() {
        String older = "{\"name\":\"logs\",\"version\":3}";
        String newer = "{\"name\":\"logs\",\"version\":5}";
        String unversioned = "{\"name\":\"logs\"}";
        // The row recording the later metadata version, whichever the table returns first.
        assertEquals(newer, InfinoRegistry.later(older, newer));
        assertEquals(newer, InfinoRegistry.later(newer, older));
        // A row a loader wrote records no version: one the cluster recorded is later.
        assertEquals(older, InfinoRegistry.later(unversioned, older));
        // Two alike in version: still the same one either way round.
        String a = "{\"name\":\"logs\",\"table\":\"a\"}";
        String b = "{\"name\":\"logs\",\"table\":\"b\"}";
        assertEquals(InfinoRegistry.later(a, b), InfinoRegistry.later(b, a));
    }

    public void testAFailedRowIsTriedAgainSoonThenLessOftenUpToAMinute() {
        assertEquals(TimeValue.timeValueSeconds(1), InfinoRegistry.retryDelay(1));
        assertEquals(TimeValue.timeValueSeconds(2), InfinoRegistry.retryDelay(2));
        assertEquals(TimeValue.timeValueSeconds(4), InfinoRegistry.retryDelay(3));
        assertEquals(TimeValue.timeValueSeconds(32), InfinoRegistry.retryDelay(6));
        assertEquals(InfinoRegistry.MAX_RETRY_DELAY, InfinoRegistry.retryDelay(7));
        // However long it keeps failing, the wait stays at the most, never overflowing.
        assertEquals(InfinoRegistry.MAX_RETRY_DELAY, InfinoRegistry.retryDelay(1_000));
        assertEquals(InfinoRegistry.MAX_RETRY_DELAY, InfinoRegistry.retryDelay(Integer.MAX_VALUE));
    }

    public void testAFailedRowIsWrittenByItsScheduledRetryAsTheClusterHoldsItThen() throws Exception {
        ThreadPool threadPool = new TestThreadPool(getTestName());
        ClusterService clusterService = ClusterServiceUtils.createClusterService(threadPool);
        StubBridge bridge = new StubBridge();
        try {
            bind(clusterService, threadPool, bridge);
            IndexMetadata first = index("retried", "retried-uuid", 1);
            ClusterServiceUtils.setState(clusterService, withIndices(clusterService.state(), first));
            bridge.failing = true;
            InfinoRegistry.record(first);
            assertTrue(bridge.appended.isEmpty());
            // The service comes back, and the index moves on while its row waits.
            ClusterServiceUtils.setState(clusterService, withIndices(clusterService.state(), index("retried", "retried-uuid", 2)));
            bridge.failing = false;
            assertBusy(() -> assertEquals(1, bridge.appended.size()), 10, TimeUnit.SECONDS);
            assertEquals(2L, ((Number) InfinoNative.object(bridge.appended.get(0)).get("version")).longValue());
        } finally {
            unbind(clusterService, threadPool);
        }
    }

    public void testAScheduledRetryOfADeletedIndexWritesNothing() throws Exception {
        ThreadPool threadPool = new TestThreadPool(getTestName());
        ClusterService clusterService = ClusterServiceUtils.createClusterService(threadPool);
        StubBridge bridge = new StubBridge();
        try {
            bind(clusterService, threadPool, bridge);
            IndexMetadata deleted = index("deleted", "deleted-uuid", 1);
            IndexMetadata live = index("live", "live-uuid", 1);
            ClusterServiceUtils.setState(clusterService, withIndices(clusterService.state(), deleted, live));
            bridge.failing = true;
            // The deleted index's row fails first, so it is due by the time the live one is.
            InfinoRegistry.record(deleted);
            InfinoRegistry.record(live);
            ClusterServiceUtils.setState(
                clusterService,
                ClusterState.builder(clusterService.state()).metadata(Metadata.builder(clusterService.state().metadata()).remove("deleted"))
            );
            bridge.failing = false;
            assertBusy(() -> assertEquals(1, bridge.appended.size()), 10, TimeUnit.SECONDS);
            // A read drains what is due, after any drain under way.
            InfinoRegistry.rows();
            assertEquals(1, bridge.appended.size());
            assertEquals("live", InfinoNative.object(bridge.appended.get(0)).get("name"));
        } finally {
            unbind(clusterService, threadPool);
        }
    }

    public void testANodeThatLosesTheElectionMidDrainWritesNoFurtherRow() throws Exception {
        ThreadPool threadPool = new TestThreadPool(getTestName());
        ClusterService clusterService = ClusterServiceUtils.createClusterService(threadPool);
        StubBridge bridge = new StubBridge();
        try {
            // No pool for the registry: nothing retries on its own, the read below drains.
            bind(clusterService, null, bridge);
            IndexMetadata a = index("first_row", "first-uuid", 1);
            IndexMetadata b = index("second_row", "second-uuid", 1);
            ClusterServiceUtils.setState(clusterService, withIndices(clusterService.state(), a, b));
            bridge.failing = true;
            InfinoRegistry.record(a);
            InfinoRegistry.record(b);
            bridge.failing = false;
            // Another node is elected as soon as this one writes a row.
            DiscoveryNode other = new DiscoveryNode("other", buildNewFakeTransportAddress(), Version.CURRENT);
            bridge.onAppend = () -> ClusterServiceUtils.setState(
                clusterService,
                ClusterState.builder(clusterService.state())
                    .nodes(DiscoveryNodes.builder(clusterService.state().nodes()).add(other).clusterManagerNodeId(other.getId()))
            );
            // Both rows wait out their first delay, then one read drains them together.
            Thread.sleep(InfinoRegistry.retryDelay(1).millis() + 100);
            InfinoRegistry.rows();
            assertEquals(1, bridge.appended.size());
            assertFalse(clusterService.state().nodes().isLocalNodeElectedClusterManager());
        } finally {
            unbind(clusterService, threadPool);
        }
    }

    public void testARegistryTableWhoseDeclarationFailedIsDeclaredAgainByTheNextWrite() {
        ThreadPool threadPool = new TestThreadPool(getTestName());
        ClusterService clusterService = ClusterServiceUtils.createClusterService(threadPool);
        StubBridge bridge = new StubBridge();
        try {
            // No pool for the registry: nothing retries on its own, the calls below write.
            bind(clusterService, null, bridge);
            IndexMetadata md = index("declared_late", "declared-late-uuid", 1);
            bridge.declareFailing = true;
            InfinoRegistry.record(md);
            assertTrue(bridge.appended.isEmpty());
            // The service takes the declaration now; the next write makes it again before writing.
            bridge.declareFailing = false;
            InfinoRegistry.record(md);
            assertEquals(1, bridge.appended.size());
            assertEquals(2, bridge.declarations.get());
        } finally {
            InfinoRegistry.forget("declared_late");
            unbind(clusterService, threadPool);
        }
    }

    public void testForgettingAnIndexDoesNotWaitOutTheReadWindowButTheNextWriteDoes() {
        ThreadPool threadPool = new TestThreadPool(getTestName());
        ClusterService clusterService = ClusterServiceUtils.createClusterService(threadPool);
        StubBridge bridge = new StubBridge();
        TimeValue window = TimeValue.timeValueSeconds(1);
        InfinoNative.readWindow(window);
        try {
            bind(clusterService, null, bridge);
            // The registry table, written on the hosted service a moment ago.
            long start = System.nanoTime();
            InfinoNative.wrote(InfinoRegistry.TABLE, start);
            // A delete reads nothing back; it runs on the cluster applier thread.
            InfinoRegistry.forget("gone");
            assertTrue(System.nanoTime() - start < window.nanos());
            // A write reads the table back first (its rows under the id), so it waits.
            InfinoRegistry.record(index("kept", "kept-uuid", 1));
            assertEquals(1, bridge.appended.size());
            assertTrue(System.nanoTime() - start >= window.nanos());
        } finally {
            InfinoNative.readWindow(InfinoPlugin.PLATFORM_READ_WINDOW.get(Settings.EMPTY));
            InfinoRegistry.forget("kept");
            unbind(clusterService, threadPool);
        }
    }

    /** Stand {@code bridge} in for the library, on a node that has not declared the registry table yet. */
    private static void bind(ClusterService clusterService, ThreadPool threadPool, StubBridge bridge) {
        InfinoServices.bind(clusterService, threadPool);
        InfinoNative.useLib(bridge.lib());
        InfinoRegistry.undeclare();
    }

    private static void unbind(ClusterService clusterService, ThreadPool threadPool) {
        InfinoServices.bind(null, null);
        InfinoNative.useLib(null);
        clusterService.close();
        terminate(threadPool);
    }

    private static IndexMetadata index(String name, String uuid, long version) {
        return IndexMetadata.builder(name)
            .settings(settings(Version.CURRENT).put(IndexMetadata.SETTING_INDEX_UUID, uuid))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .version(version)
            .build();
    }

    private static ClusterState withIndices(ClusterState state, IndexMetadata... indices) {
        Metadata.Builder metadata = Metadata.builder(state.metadata());
        for (IndexMetadata md : indices) {
            metadata.put(md, false);
        }
        return ClusterState.builder(state).metadata(metadata).build();
    }

    /**
     * A bridge that answers the registry's calls, failing each write while
     * {@link #failing}, and keeps the source of every row it appends. Its
     * table is read only once a declaration of it has succeeded; each
     * declaration fails while {@link #declareFailing}.
     */
    private static final class StubBridge implements InvocationHandler {
        volatile boolean failing;
        volatile boolean declareFailing;
        final AtomicInteger declarations = new AtomicInteger();
        private volatile boolean declared;
        volatile Runnable onAppend = () -> {};
        final List<String> appended = new CopyOnWriteArrayList<>();
        private volatile String prepared;

        InfinoLib lib() {
            return (InfinoLib) Proxy.newProxyInstance(InfinoLib.class.getClassLoader(), new Class<?>[] { InfinoLib.class }, this);
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) {
            switch (method.getName()) {
                case "infino_open_with":
                    return new Memory(1);
                case "infino_ensure_table":
                    declarations.incrementAndGet();
                    if (declareFailing) {
                        return string("{\"error\":\"the service is down\"}");
                    }
                    declared = true;
                    return string("{}");
                case "infino_sql":
                    // The registry counts a row's copies before every write.
                    if (declared == false) {
                        return string("{\"error\":\"table os_index not found\"}");
                    }
                    return string(failing ? "{\"error\":\"the service is down\"}" : "[{\"held\":0}]");
                case "infino_prepare_batch": {
                    byte[] buf = (byte[]) args[2];
                    int idEnd = ((int[]) args[4])[0];
                    int srcEnd = ((int[]) args[5])[0];
                    prepared = new String(buf, idEnd, srcEnd - idEnd, StandardCharsets.UTF_8);
                    return new Memory(1);
                }
                case "infino_append_prepared":
                    appended.add(prepared);
                    onAppend.run();
                    return string("{\"ok\":1,\"failed\":[]}");
                case "infino_delete_osids":
                    return 0;
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

    public void testAWriteIsVisibleOnceTheReadWindowHasPassed() {
        long window = TimeValue.timeValueSeconds(1).nanos();
        long written = 5_000_000_000L;
        assertEquals(window, InfinoNative.untilVisible(written, window, written));
        assertEquals(window / 4, InfinoNative.untilVisible(written, window, written + window * 3 / 4));
        assertTrue(InfinoNative.untilVisible(written, window, written + window) <= 0);
        assertTrue(InfinoNative.untilVisible(written, 0, written) <= 0);
    }

    public void testARefreshOverATableThisNodeNeverWroteDoesNotWait() {
        long start = System.nanoTime();
        InfinoNative.awaitVisible("never_written");
        assertTrue(System.nanoTime() - start < TimeValue.timeValueMillis(500).nanos());
    }

    public void testARefreshAfterAHostedWriteWaitsOutTheReadWindow() {
        TimeValue window = TimeValue.timeValueMillis(300);
        InfinoNative.readWindow(window);
        try {
            long start = System.nanoTime();
            InfinoNative.wrote("written_here", start);
            InfinoNative.awaitVisible("written_here");
            assertTrue(System.nanoTime() - start >= window.nanos());
            // Once seen, the write no longer holds a refresh back.
            long again = System.nanoTime();
            InfinoNative.awaitVisible("written_here");
            assertTrue(System.nanoTime() - again < window.nanos());
        } finally {
            InfinoNative.readWindow(InfinoPlugin.PLATFORM_READ_WINDOW.get(Settings.EMPTY));
        }
    }

    public void testAnOlderWriteNotedLastDoesNotCutTheWaitForANewerOne() {
        TimeValue window = TimeValue.timeValueMillis(300);
        InfinoNative.readWindow(window);
        try {
            long start = System.nanoTime();
            InfinoNative.wrote("written_twice", start);
            // A write that completed a window earlier, noted after the newer one.
            InfinoNative.wrote("written_twice", start - window.nanos());
            InfinoNative.awaitVisible("written_twice");
            assertTrue(System.nanoTime() - start >= window.nanos());
        } finally {
            InfinoNative.readWindow(InfinoPlugin.PLATFORM_READ_WINDOW.get(Settings.EMPTY));
        }
    }

    public void testTheReadWindowDefaultsToASecond() {
        assertEquals(TimeValue.timeValueSeconds(1), InfinoPlugin.PLATFORM_READ_WINDOW.get(Settings.EMPTY));
    }

    public void testARowFromAnotherEngineIsCreatedWithoutWhatTheClusterDoesNotKnow() {
        Map<String, Object> row = Map.of("settings", Map.of("index.number_of_shards", "1", "index.lifecycle.name", "logs"));
        List<String> leftOut = new ArrayList<>();
        CreateIndexRequest request = InfinoRegistry.requestOf("logs", row, KNOWN, leftOut);
        assertNull(request.settings().get("index.lifecycle.name"));
        assertEquals(List.of("index.lifecycle.name"), leftOut);
    }
}
