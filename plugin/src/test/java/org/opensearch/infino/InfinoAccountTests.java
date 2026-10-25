/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import org.opensearch.common.settings.Settings;
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
        assertTrue(text, text.contains("filtered from every settings API"));
        assertTrue(text, text.contains("POST " + InfinoAccount.TRIAL_ROUTE + " {\"accept\": true}"));
        assertTrue(text, text.contains("PUT " + InfinoAccount.ROUTE));
        assertTrue(text, text.contains("Nothing is uploaded until an account is stored"));
    }

    public void testAHostedTargetNeedsTheUrlTheDatabaseAndTheKey() {
        Settings none = Settings.EMPTY;
        assertNull(InfinoNative.Target.hosted(none));
        assertFalse(InfinoNative.Target.of(none).hosted());

        Settings keyOnly = Settings.builder().put(InfinoPlugin.PLATFORM_API_KEY.getKey(), "ik_x").build();
        assertNull(InfinoNative.Target.hosted(keyOnly));

        Settings whole = Settings.builder()
            .put(InfinoPlugin.PLATFORM_URL.getKey(), "https://host/")
            .put(InfinoPlugin.PLATFORM_DATABASE.getKey(), "logs")
            .put(InfinoPlugin.PLATFORM_API_KEY.getKey(), "ik_x")
            .build();
        InfinoNative.Target target = InfinoNative.Target.of(whole);
        assertTrue(target.hosted());
        assertEquals("https://host/logs", target.uri());
        assertEquals("ik_x", target.apiKey());
        // The key is not in the name a message or a log line carries.
        assertFalse(target.name(), target.name().contains("ik_x"));
        assertTrue(target.name(), target.name().contains("logs"));
    }

    public void testTheKeyIsAFilteredSetting() {
        assertTrue(InfinoPlugin.PLATFORM_API_KEY.isFiltered());
        assertTrue(InfinoPlugin.PLATFORM_API_KEY.isDynamic());
        assertFalse(InfinoPlugin.PLATFORM_URL.isFiltered());
    }
}
