/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.common.settings.IndexScopedSettings;
import org.opensearch.common.settings.Settings;
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

    public void testARowFromAnotherEngineIsCreatedWithoutWhatTheClusterDoesNotKnow() {
        Map<String, Object> row = Map.of("settings", Map.of("index.number_of_shards", "1", "index.lifecycle.name", "logs"));
        List<String> leftOut = new ArrayList<>();
        CreateIndexRequest request = InfinoRegistry.requestOf("logs", row, KNOWN, leftOut);
        assertNull(request.settings().get("index.lifecycle.name"));
        assertEquals(List.of("index.lifecycle.name"), leftOut);
    }
}
