/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import org.opensearch.common.settings.IndexScopedSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class InfinoClusterStateTests extends OpenSearchTestCase {

    public void testEachKindIsCreatedThroughItsOwnRestPath() {
        assertEquals("/_cluster/settings", InfinoClusterState.Kind.PERSISTENT_SETTINGS.uri("persistent"));
        assertEquals("/_component_template/base", InfinoClusterState.Kind.COMPONENT_TEMPLATE.uri("base"));
        assertEquals("/_index_template/logs", InfinoClusterState.Kind.INDEX_TEMPLATE.uri("logs"));
        assertEquals("/_template/legacy", InfinoClusterState.Kind.TEMPLATE.uri("legacy"));
        assertEquals("/_ingest/pipeline/tag", InfinoClusterState.Kind.INGEST_PIPELINE.uri("tag"));
        assertEquals("/_search/pipeline/rank", InfinoClusterState.Kind.SEARCH_PIPELINE.uri("rank"));
        assertEquals("/_scripts/score", InfinoClusterState.Kind.SCRIPT.uri("score"));
        assertEquals("/_plugins/_security/api/actiongroups/reads", InfinoClusterState.Kind.ACTION_GROUP.uri("reads"));
        assertEquals("/_plugins/_security/api/tenants/team", InfinoClusterState.Kind.TENANT.uri("team"));
        assertEquals("/_plugins/_security/api/roles/reader", InfinoClusterState.Kind.ROLE.uri("reader"));
        assertEquals("/_plugins/_security/api/internalusers/ana", InfinoClusterState.Kind.USER.uri("ana"));
        assertEquals("/_plugins/_security/api/rolesmapping/reader", InfinoClusterState.Kind.ROLE_MAPPING.uri("reader"));
    }

    public void testANameIsOnePathSegmentWhateverItHolds() {
        assertEquals("/_plugins/_security/api/internalusers/ana%20maria", InfinoClusterState.Kind.USER.uri("ana maria"));
        assertEquals("/_ingest/pipeline/a%2Fb", InfinoClusterState.Kind.INGEST_PIPELINE.uri("a/b"));
        assertEquals("/_scripts/x%3Fy%26z", InfinoClusterState.Kind.SCRIPT.uri("x?y&z"));
    }

    public void testWhatAnItemNamesIsCreatedBeforeIt() {
        List<InfinoClusterState.Kind> order = Arrays.asList(InfinoClusterState.Kind.values());
        // An index template composed of a component template, and the
        // users and mappings that name a role.
        assertTrue(order.indexOf(InfinoClusterState.Kind.COMPONENT_TEMPLATE) < order.indexOf(InfinoClusterState.Kind.INDEX_TEMPLATE));
        assertTrue(order.indexOf(InfinoClusterState.Kind.ACTION_GROUP) < order.indexOf(InfinoClusterState.Kind.ROLE));
        assertTrue(order.indexOf(InfinoClusterState.Kind.TENANT) < order.indexOf(InfinoClusterState.Kind.ROLE));
        assertTrue(order.indexOf(InfinoClusterState.Kind.ROLE) < order.indexOf(InfinoClusterState.Kind.USER));
        assertTrue(order.indexOf(InfinoClusterState.Kind.ROLE) < order.indexOf(InfinoClusterState.Kind.ROLE_MAPPING));
    }

    public void testAKindIsFoundByTheNameALoaderWritesAndNoOtherIs() {
        for (InfinoClusterState.Kind kind : InfinoClusterState.Kind.values()) {
            assertSame(kind, InfinoClusterState.Kind.of(kind.key));
        }
        assertNull(InfinoClusterState.Kind.of("data_stream"));
    }

    /** The index settings a node with no plugin knows. */
    private static final IndexScopedSettings KNOWN = new IndexScopedSettings(Settings.EMPTY, IndexScopedSettings.BUILT_IN_INDEX_SETTINGS);

    @SuppressWarnings("unchecked")
    public void testATemplatesSettingsTheClusterDoesNotKnowAreLeftOutAndNamed() {
        Map<String, Object> composable = Map.of(
            "index_patterns",
            List.of("logs-*"),
            "template",
            Map.of("settings", Map.of("index.lifecycle.name", "logs", "index.default_pipeline", "tag"), "mappings", Map.of())
        );
        InfinoClusterState.Item item = new InfinoClusterState.Item(
            "_cluster/index_template/logs",
            "index_template",
            InfinoClusterState.Kind.INDEX_TEMPLATE,
            "logs",
            composable
        );
        List<String> leftOut = new ArrayList<>();
        Map<String, Object> held = InfinoClusterState.heldToKnownSettings(item, KNOWN, leftOut);
        Map<String, Object> settings = (Map<String, Object>) ((Map<String, Object>) held.get("template")).get("settings");
        assertEquals(Map.of("index.default_pipeline", "tag"), settings);
        assertEquals(List.of("index.lifecycle.name"), leftOut);
        assertEquals(List.of("logs-*"), held.get("index_patterns"));

        Map<String, Object> legacy = Map.of("index_patterns", List.of("old-*"), "settings", Map.of("index.lifecycle.name", "logs"));
        List<String> legacyLeftOut = new ArrayList<>();
        Map<String, Object> heldLegacy = InfinoClusterState.heldToKnownSettings(
            new InfinoClusterState.Item("_cluster/template/old", "template", InfinoClusterState.Kind.TEMPLATE, "old", legacy),
            KNOWN,
            legacyLeftOut
        );
        assertEquals(Map.of(), heldLegacy.get("settings"));
        assertEquals(List.of("index.lifecycle.name"), legacyLeftOut);

        // A pipeline's body is not a template's: it is sent as it is.
        Map<String, Object> pipeline = Map.of("processors", List.of());
        List<String> none = new ArrayList<>();
        assertEquals(
            pipeline,
            InfinoClusterState.heldToKnownSettings(
                new InfinoClusterState.Item("_cluster/ingest_pipeline/p", "ingest_pipeline", InfinoClusterState.Kind.INGEST_PIPELINE, "p", pipeline),
                KNOWN,
                none
            )
        );
        assertTrue(none.isEmpty());
    }

    public void testOnlyAClusterStateRowIsTakenForOne() {
        assertTrue(InfinoClusterState.isClusterRow("_cluster/user/ana"));
        // No index name begins with an underscore, so an index's row never is one.
        assertFalse(InfinoClusterState.isClusterRow("logs"));
        assertFalse(InfinoClusterState.isClusterRow("cluster"));
    }
}
