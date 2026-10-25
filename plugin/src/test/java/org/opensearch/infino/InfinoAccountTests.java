/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import org.opensearch.common.settings.SecureSetting;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.Index;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

public class InfinoAccountTests extends OpenSearchTestCase {

    public void testDatabaseNameIsTheClusterNameMappedToTheServicesRule() {
        assertEquals("opensearch", InfinoAccount.databaseNameFor("opensearch"));
        assertEquals("logs-prod", InfinoAccount.databaseNameFor("logs.prod"));
        assertEquals("my-cluster-2", InfinoAccount.databaseNameFor("my cluster 2"));
        // A leading underscore or dash is not accepted; it is dropped, not replaced.
        assertEquals("internal", InfinoAccount.databaseNameFor("_internal"));
        assertEquals("x", InfinoAccount.databaseNameFor("--x"));
        // Nothing usable maps to a fixed name, so the same cluster always lands on the same database.
        assertEquals("opensearch", InfinoAccount.databaseNameFor(""));
        assertEquals("opensearch", InfinoAccount.databaseNameFor("___"));
        String longName = "a".repeat(300);
        assertEquals(128, InfinoAccount.databaseNameFor(longName).length());
    }

    public void testBaseUrlDropsTrailingSlashes() {
        assertEquals("https://host", InfinoAccount.baseUrl("https://host/"));
        assertEquals("https://host", InfinoAccount.baseUrl("https://host///"));
        assertEquals("https://host", InfinoAccount.baseUrl("https://host"));
    }

    public void testTheNoticeSaysWhatLeavesAndHowToAccept() {
        List<String> notice = InfinoAccount.notice("https://api.example");
        String text = String.join("\n", notice);
        assertTrue(text, text.contains("every document of this cluster"));
        assertTrue(text, text.contains("the documents themselves, not names or metrics"));
        assertTrue(text, text.contains("https://api.example"));
        assertTrue(text, text.contains("no email, no password and no card"));
        assertTrue(text, text.contains("The key is returned once, to you"));
        assertTrue(text, text.contains("POST " + InfinoAccount.TRIAL_ROUTE + " {\"accept\": true}"));
        assertTrue(text, text.contains("PUT " + InfinoAccount.ROUTE));
        assertTrue(text, text.contains("Nothing is uploaded until an account is in use"));
    }

    public void testTheConfigLinesNameTheNodeSettingsAndTheKeystore() {
        String lines = String.join("\n", InfinoAccount.configLines("https://api.example", "logs"));
        assertTrue(lines, lines.contains("infino.platform.url: https://api.example"));
        assertTrue(lines, lines.contains("infino.platform.database: logs"));
        assertTrue(lines, lines.contains("opensearch-keystore add infino.platform.api_key"));
        assertFalse(lines, lines.contains("cluster"));
    }

    public void testAHostedTargetNamesTheServiceAndTheDatabaseButNeverTheKey() {
        InfinoNative.Target target = InfinoNative.Target.hosted("https://host/", "logs", "ik_x");
        assertTrue(target.hosted());
        assertEquals("https://host/logs", target.uri());
        assertEquals("ik_x", target.apiKey());
        assertFalse(target.name(), target.name().contains("ik_x"));
        assertTrue(target.name(), target.name().contains("logs"));
        // No key: the engine reads INFINO_API_KEY from the environment.
        assertNull(InfinoNative.Target.hosted("https://host", "logs", null).apiKey());
        assertFalse(InfinoNative.Target.node().hosted());
    }

    public void testTheKeyIsASecureSettingAndTheUrlIsNotDynamic() {
        // A keystore setting: never in the node's settings, never in a settings API.
        assertTrue(InfinoPlugin.PLATFORM_API_KEY instanceof SecureSetting);
        assertFalse(InfinoPlugin.PLATFORM_URL.isDynamic());
        assertFalse(InfinoPlugin.PLATFORM_DATABASE.isDynamic());
    }

    public void testATableIsNamedByItsIndexSoAFreshClusterFindsIt() {
        Index logs = new Index("logs-2026.10.25", "someUUIDaaa");
        assertEquals("os_logs-2026_10_25", InfinoNative.tableFor(logs, Settings.EMPTY));
        // The same name on another cluster, another UUID: the same table.
        assertEquals("os_logs-2026_10_25", InfinoNative.tableFor(new Index("logs-2026.10.25", "otherUUID"), Settings.EMPTY));
        assertEquals("os__kibana_1", InfinoNative.tableFor(".kibana_1"));
        // An explicit name wins, and marks the table as one to keep on delete.
        Settings explicit = Settings.builder().put(InfinoPlugin.INFINO_TABLE.getKey(), "kept").build();
        assertEquals("kept", InfinoNative.tableFor(logs, explicit));
        assertTrue(InfinoNative.tableIsExplicit(explicit));
        assertFalse(InfinoNative.tableIsExplicit(Settings.EMPTY));
    }
}
