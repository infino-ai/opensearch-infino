/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import org.opensearch.common.settings.IndexScopedSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
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
        InfinoClusterState.Item item = new InfinoClusterState.Item("index_template", InfinoClusterState.Kind.INDEX_TEMPLATE, "logs", composable);
        List<String> leftOut = new ArrayList<>();
        Map<String, Object> held = InfinoClusterState.heldToKnownSettings(item, KNOWN, leftOut);
        Map<String, Object> settings = (Map<String, Object>) ((Map<String, Object>) held.get("template")).get("settings");
        assertEquals(Map.of("index.default_pipeline", "tag"), settings);
        assertEquals(List.of("index.lifecycle.name"), leftOut);
        assertEquals(List.of("logs-*"), held.get("index_patterns"));

        Map<String, Object> legacy = Map.of("index_patterns", List.of("old-*"), "settings", Map.of("index.lifecycle.name", "logs"));
        List<String> legacyLeftOut = new ArrayList<>();
        Map<String, Object> heldLegacy = InfinoClusterState.heldToKnownSettings(
            new InfinoClusterState.Item("template", InfinoClusterState.Kind.TEMPLATE, "old", legacy),
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
                new InfinoClusterState.Item("ingest_pipeline", InfinoClusterState.Kind.INGEST_PIPELINE, "p", pipeline),
                KNOWN,
                none
            )
        );
        assertTrue(none.isEmpty());
    }

    public void testAUserWithNoCredentialIsCreatedWithAPasswordNoOneIsTold() {
        InfinoClusterState.Item user = new InfinoClusterState.Item(
            "user",
            InfinoClusterState.Kind.USER,
            "ana",
            Map.of("backend_roles", List.of("reader"))
        );
        Map<String, Object> sent = InfinoClusterState.sentBody(user, KNOWN, new ArrayList<>());
        assertEquals(List.of("reader"), sent.get("backend_roles"));
        String password = (String) sent.get("password");
        assertEquals(32, password.length());
        assertTrue(password.chars().anyMatch(Character::isUpperCase));
        assertTrue(password.chars().anyMatch(Character::isLowerCase));
        assertTrue(password.chars().anyMatch(Character::isDigit));
        assertTrue(password.chars().anyMatch(c -> !Character.isLetterOrDigit(c)));
        assertNotEquals(password, InfinoClusterState.sentBody(user, KNOWN, new ArrayList<>()).get("password"));
        // A user that comes with its hash is sent as it is, and no other kind is given a password.
        Map<String, Object> hashed = Map.of("hash", "$2y$12$x");
        assertEquals(
            hashed,
            InfinoClusterState.sentBody(new InfinoClusterState.Item("user", InfinoClusterState.Kind.USER, "bo", hashed), KNOWN, new ArrayList<>())
        );
        Map<String, Object> role = Map.of("cluster_permissions", List.of("cluster_monitor"));
        assertEquals(
            role,
            InfinoClusterState.sentBody(new InfinoClusterState.Item("role", InfinoClusterState.Kind.ROLE, "reader", role), KNOWN, new ArrayList<>())
        );
    }

    public void testOnlyAUserWithNoCredentialIsLookedUpBeforeItIsCreated() {
        // Applied again over a user the cluster has, it would get a new password no one is told.
        assertTrue(
            InfinoClusterState.createdWithUnknowablePassword(
                new InfinoClusterState.Item("user", InfinoClusterState.Kind.USER, "ana", Map.of("backend_roles", List.of("reader")))
            )
        );
        assertFalse(
            InfinoClusterState.createdWithUnknowablePassword(
                new InfinoClusterState.Item("user", InfinoClusterState.Kind.USER, "bo", Map.of("hash", "$2y$12$x"))
            )
        );
        assertFalse(
            InfinoClusterState.createdWithUnknowablePassword(
                new InfinoClusterState.Item("user", InfinoClusterState.Kind.USER, "cy", Map.of("password", "chosen"))
            )
        );
        assertFalse(
            InfinoClusterState.createdWithUnknowablePassword(
                new InfinoClusterState.Item("role", InfinoClusterState.Kind.ROLE, "reader", Map.of())
            )
        );
    }

    @SuppressWarnings("unchecked")
    public void testAUserTheClusterHadIsReportedPresent() throws IOException {
        XContentBuilder b = XContentFactory.jsonBuilder().startObject();
        InfinoClusterState.write(
            b,
            List.of(
                new InfinoClusterState.Applied("user", "ana", true, true, 200, null, List.of()),
                new InfinoClusterState.Applied("role", "reader", true, false, 200, null, List.of()),
                new InfinoClusterState.Applied("user", "bo", false, false, 403, "no permission", List.of())
            )
        );
        Map<String, Object> out = XContentHelper.convertToMap(BytesReference.bytes(b.endObject()), false, MediaTypeRegistry.JSON).v2();
        List<Map<String, Object>> state = (List<Map<String, Object>>) out.get("state");
        assertEquals(List.of("present", "applied", "failed"), state.stream().map(s -> s.get("result")).toList());
        assertEquals(403, state.get(2).get("status"));
    }

    public void testAStateRowListsItsItemsInTheOrderTheyAreApplied() {
        String row = "{\"items\": ["
            + "{\"kind\": \"user\", \"name\": \"ana\", \"body\": {\"hash\": \"$2y$12$x\", \"backend_roles\": [\"reader\"]}},"
            + "{\"kind\": \"data_stream\", \"name\": \"logs\", \"body\": {}},"
            + "\"not an item\","
            + "{\"kind\": \"role\", \"name\": \"reader\", \"body\": {\"cluster_permissions\": [\"cluster_monitor\"]}},"
            + "{\"kind\": \"component_template\", \"name\": \"base\", \"body\": {\"template\": {}}}"
            + "]}";
        List<InfinoClusterState.Item> items = InfinoClusterState.items(row);
        assertEquals(List.of("base", "reader", "ana", "logs"), items.stream().map(InfinoClusterState.Item::name).toList());
        // A kind this node does not know is kept, to be reported, and comes last.
        assertNull(items.get(3).kind());
        assertEquals("data_stream", items.get(3).kindKey());
        assertEquals(Map.of("hash", "$2y$12$x", "backend_roles", List.of("reader")), items.get(2).body());
        // What a pass writes back for the items that failed reads as those items.
        assertEquals(items.subList(2, 4), InfinoClusterState.items(InfinoClusterState.source(items.subList(2, 4))));
        assertEquals(List.of(), InfinoClusterState.items("{}"));
    }

    public void testOnlyAClusterStateRowIsTakenForOne() {
        assertTrue(InfinoClusterState.isClusterRow(InfinoClusterState.STATE_ROW));
        assertTrue(InfinoClusterState.isClusterRow("_cluster/user/ana"));
        // No index name begins with an underscore, so an index's row never is one.
        assertFalse(InfinoClusterState.isClusterRow("logs"));
        assertFalse(InfinoClusterState.isClusterRow("cluster"));
    }
}
