/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import org.opensearch.Version;
import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.action.admin.indices.rollover.RolloverRequest;
import org.opensearch.action.admin.indices.shrink.ResizeRequest;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
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

    /** The settings of an index created while a live index its table could back named that table. */
    private static final Settings CREATED_REFUSED = Settings.builder().put(InfinoPlugin.INFINO_TABLE_CLAIMED.getKey(), false).build();

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

    public void testAnExplicitTableIsReadFromTheIndexAResizeOrARolloverCreates() {
        ResizeRequest resize = new ResizeRequest("target", "source");
        resize.getTargetIndexRequest().settings(Settings.builder().put(InfinoPlugin.INFINO_TABLE.getKey(), "os_z"));
        assertEquals("os_z", InfinoExplicitTableActionFilter.namedTable(resize));
        RolloverRequest rollover = new RolloverRequest("alias", null);
        rollover.getCreateIndexRequest().settings(Settings.builder().put("infino.table", "os_w"));
        assertEquals("os_w", InfinoExplicitTableActionFilter.namedTable(rollover));
        assertNull(InfinoExplicitTableActionFilter.namedTable(new ResizeRequest("target", "source")));
    }

    public void testWhetherAnIndexWasFreeToTakeItsTableIsReadFromAnyRequestThatSetsIt() {
        String claimed = InfinoPlugin.INFINO_TABLE_CLAIMED.getKey();
        Settings set = InfinoExplicitTableActionFilter.requestedSettings(new CreateIndexRequest("a").settings(Settings.builder().put("infino.table_claimed", true)));
        assertTrue(set.keySet().contains(claimed));
        ResizeRequest resize = new ResizeRequest("target", "source");
        resize.getTargetIndexRequest().settings(Settings.builder().put(claimed, true));
        assertTrue(InfinoExplicitTableActionFilter.requestedSettings(resize).keySet().contains(claimed));
    }

    public void testAnIndexCreatedBesideTheOneItsTableBacksInheritsNothingWhenThatOneIsDeleted() {
        IndexMetadata owner = index("logs.app", "u1", 1);
        IndexMetadata refused = index("logs_app", "u2", 2, CREATED_REFUSED);
        // While the owner lives, the refused index is told which index the table backs.
        Map<String, List<IndexMetadata>> both = InfinoTables.naming(List.of(owner, refused));
        assertEquals(owner.getIndex(), InfinoTables.owner(both.get("os_logs_app")));
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> InfinoTables.claim("os_logs_app", refused, both));
        assertTrue(e.getMessage(), e.getMessage().contains("backs index [logs.app]"));
        // Once the owner is gone, the table backs no index: the refused one, now the oldest, takes nothing.
        Map<String, List<IndexMetadata>> after = InfinoTables.naming(List.of(refused));
        assertNull(InfinoTables.owner(after.get("os_logs_app")));
        e = expectThrows(IllegalArgumentException.class, () -> InfinoTables.claim("os_logs_app", refused, after));
        assertTrue(e.getMessage(), e.getMessage().contains("was created while table [os_logs_app] backed another index"));
        assertFalse(new InfinoTables().owns(refused, Metadata.builder().put(refused, false).build()));
    }

    public void testAnIndexIsFreeToTakeATableNoIndexItCouldBackNames() {
        IndexMetadata owner = index("logs.app", "u1", 1);
        IndexMetadata refused = index("logs_app", "u2", 2, CREATED_REFUSED);
        Settings proxy = Settings.builder()
            .put(InfinoPlugin.INFINO_ENABLED.getKey(), true)
            .put(InfinoPlugin.INFINO_MODE.getKey(), InfinoPlugin.MODE_PROXY)
            .build();
        List<String> tables = InfinoTables.tablesOf("logs-app", proxy);
        assertEquals(List.of("os_logs-app"), tables);
        List<String> colliding = InfinoTables.tablesOf("logs_app", proxy);
        assertFalse(InfinoTables.claimable(colliding, InfinoTables.naming(List.of(owner))));
        // An index created refused beside a deleted owner holds the table for no later index.
        assertTrue(InfinoTables.claimable(colliding, InfinoTables.naming(List.of(refused))));
        assertTrue(InfinoTables.claimable(colliding, Map.of()));
        // An explicit table is taken against the index it names.
        Settings explicit = Settings.builder().put(proxy).put(InfinoPlugin.INFINO_TABLE.getKey(), "os_logs_app").build();
        assertFalse(InfinoTables.claimable(InfinoTables.tablesOf("other", explicit), InfinoTables.naming(List.of(owner))));
    }

    public void testDeletingTheOwnerKeepsTheTableForNoIndexCreatedRefusedBesideIt() {
        IndexMetadata owner = index("logs.app", "u1", 1);
        IndexMetadata refused = index("logs_app", "u2", 2, CREATED_REFUSED);
        IndexMetadata older = index("logs~app", "u0", 0);
        InfinoTables tables = new InfinoTables();
        tables.observe(Metadata.builder().put(owner, false).put(refused, false).build());
        assertFalse(tables.keptAfterDelete("os_logs_app", owner.getIndex()));
        assertTrue(tables.keptAfterDelete("os_logs_app", refused.getIndex()));
        // An index created before the setting carries none and keeps the table as it always did.
        tables.observe(Metadata.builder().put(owner, false).put(older, false).build());
        assertTrue(tables.keptAfterDelete("os_logs_app", owner.getIndex()));
    }
}
