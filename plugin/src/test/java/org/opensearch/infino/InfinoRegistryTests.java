/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.common.settings.IndexScopedSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

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
