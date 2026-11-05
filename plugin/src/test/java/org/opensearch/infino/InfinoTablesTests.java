/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import org.opensearch.Version;
import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;
import java.util.Map;

/** Which index a table backs when two live indices name it, and the tables no index is served from. */
public class InfinoTablesTests extends OpenSearchTestCase {

    /** An Infino-backed index in proxy mode, created at {@code created}, with {@code extra} settings. */
    private static IndexMetadata index(String name, String uuid, long created, Settings extra) {
        return IndexMetadata.builder(name)
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                    .put(IndexMetadata.SETTING_INDEX_UUID, uuid)
                    .put(InfinoPlugin.INFINO_ENABLED.getKey(), true)
                    .put(InfinoPlugin.INFINO_MODE.getKey(), InfinoPlugin.MODE_PROXY)
                    .put(extra)
            )
            .creationDate(created)
            .build();
    }

    private static IndexMetadata index(String name, String uuid, long created) {
        return index(name, uuid, created, Settings.EMPTY);
    }

    public void testOfTwoIndicesWhoseNamesMapToOneTableTheOlderIsServed() {
        IndexMetadata dotted = index("logs.app", "u1", 1);
        IndexMetadata underscored = index("logs_app", "u2", 2);
        Map<String, List<IndexMetadata>> naming = InfinoTables.naming(List.of(underscored, dotted));
        assertEquals(dotted.getIndex(), InfinoTables.owner(naming.get("os_logs_app")));
        InfinoTables.claim("os_logs_app", dotted, naming);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> InfinoTables.claim("os_logs_app", underscored, naming));
        assertTrue(e.getMessage(), e.getMessage().contains("backs index [logs.app]"));
    }

    public void testAnIndexNamingAnotherIndexsTableExplicitlyIsRefused() {
        IndexMetadata security = index(".opendistro_security", "u1", 1);
        IndexMetadata mallory = index(
            "mallory-x",
            "u2",
            2,
            Settings.builder().put(InfinoPlugin.INFINO_TABLE.getKey(), "os__opendistro_security").build()
        );
        Map<String, List<IndexMetadata>> naming = InfinoTables.naming(List.of(security, mallory));
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> InfinoTables.claim("os__opendistro_security", mallory, naming)
        );
        assertTrue(e.getMessage(), e.getMessage().contains("backs index [.opendistro_security]"));
        InfinoTables.claim("os__opendistro_security", security, naming);
    }

    public void testTheRegistrysTableBacksNoIndex() {
        // An index named "index" maps to the registry's table by its name alone.
        IndexMetadata named = index("index", "u1", 1);
        assertEquals(InfinoRegistry.TABLE, InfinoTables.tablesOf(named).get(0));
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> InfinoTables.claim(InfinoRegistry.TABLE, named, InfinoTables.naming(List.of(named)))
        );
        assertTrue(e.getMessage(), e.getMessage().contains("holds the registry"));
    }

    public void testADefaultModeShardTableIsTakenAgainstAnotherIndexsName() {
        IndexMetadata sharded = index(
            "foo",
            "u1",
            1,
            Settings.builder()
                .put(InfinoPlugin.INFINO_MODE.getKey(), InfinoPlugin.MODE_DEFAULT)
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 2)
                .build()
        );
        IndexMetadata lookalike = index("foo_shard0", "u2", 2);
        assertEquals(List.of("os_foo_shard0", "os_foo_shard1"), InfinoTables.tablesOf(sharded));
        Map<String, List<IndexMetadata>> naming = InfinoTables.naming(List.of(lookalike, sharded));
        expectThrows(IllegalArgumentException.class, () -> InfinoTables.claim("os_foo_shard0", lookalike, naming));
    }

    public void testIndicesCreatedTogetherAreOrderedTheSameWayOnEveryNode() {
        IndexMetadata a = index("t.x", "b-uuid", 5);
        IndexMetadata b = index("t_x", "a-uuid", 5);
        assertEquals(b.getIndex(), InfinoTables.owner(List.of(a, b)));
        assertEquals(b.getIndex(), InfinoTables.owner(List.of(b, a)));
    }

    public void testAnIndexNotBackedByInfinoNamesNoTable() {
        IndexMetadata stock = IndexMetadata.builder("logs.app")
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                    .put(IndexMetadata.SETTING_INDEX_UUID, "u1")
            )
            .creationDate(1)
            .build();
        assertTrue(InfinoTables.naming(List.of(stock)).isEmpty());
    }

    public void testAnExplicitTableIsReadFromTheRequestHoweverItIsWritten() {
        assertEquals(
            "os_x",
            InfinoExplicitTableActionFilter.namedTable(
                new CreateIndexRequest("a").settings(Settings.builder().put(InfinoPlugin.INFINO_TABLE.getKey(), "os_x"))
            )
        );
        // The cluster gives an index setting written without its prefix the prefix.
        assertEquals("os_y", InfinoExplicitTableActionFilter.namedTable(new CreateIndexRequest("a").settings(Settings.builder().put("infino.table", "os_y"))));
        assertNull(InfinoExplicitTableActionFilter.namedTable(new CreateIndexRequest("a")));
    }
}
