/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import org.opensearch.OpenSearchStatusException;
import org.opensearch.Version;
import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.opensearch.action.admin.indices.rollover.RolloverRequest;
import org.opensearch.action.admin.indices.shrink.ResizeRequest;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.delete.DeleteRequest;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.update.UpdateRequest;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.metadata.MetadataCreateIndexService;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.indices.InvalidIndexNameException;
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
        IndexMetadata named = index("reg1", "u1", 1, Settings.builder().put(InfinoPlugin.INFINO_TABLE.getKey(), InfinoRegistry.TABLE).build());
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> InfinoTables.claim(InfinoRegistry.TABLE, named, InfinoTables.naming(List.of(named)))
        );
        assertTrue(e.getMessage(), e.getMessage().contains("holds the registry"));
    }

    public void testAnIndexNamedIndexIsServedFromATableNoOtherIndexNameMapsTo() {
        Settings oneShard = Settings.builder()
            .put(InfinoPlugin.INFINO_ENABLED.getKey(), true)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .build();
        for (String mode : List.of(InfinoPlugin.MODE_PROXY, InfinoPlugin.MODE_DEFAULT)) {
            Settings settings = Settings.builder().put(oneShard).put(InfinoPlugin.INFINO_MODE.getKey(), mode).build();
            assertEquals(List.of("os_-index"), InfinoTables.tablesOf("index", settings));
            InfinoTables.refuseRegistryTable("index", settings);
            // Every other name keeps the table it maps to, the registry's neighbours included.
            for (String name : List.of("logs", ".index", "index_", "index-1", "os_index", "index_shard0")) {
                String table = InfinoTables.tablesOf(name, settings).get(0);
                assertNotEquals(name, "os_-index", table);
                assertNotEquals(name, InfinoRegistry.TABLE, table);
                assertEquals(name, InfinoNative.tableFor(name), table);
            }
        }
        // A default-mode index of several shards keeps the shard tables it always had: none is the registry's.
        Settings sharded = Settings.builder()
            .put(oneShard)
            .put(InfinoPlugin.INFINO_MODE.getKey(), InfinoPlugin.MODE_DEFAULT)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 2)
            .build();
        assertEquals(List.of("os_index_shard0", "os_index_shard1"), InfinoTables.tablesOf("index", sharded));
        // Beside an older index whose name differs only by a character a table name may not hold, the index is served from its own
        // table, and no index is named for the registry's.
        IndexMetadata index = index("index", "u2", 2);
        IndexMetadata dotted = index(".index", "u1", 1);
        Map<String, List<IndexMetadata>> naming = InfinoTables.naming(List.of(dotted, index));
        assertEquals(index.getIndex(), InfinoTables.owner(naming.get("os_-index")));
        InfinoTables.claim("os_-index", index, naming);
        assertNull(naming.get(InfinoRegistry.TABLE));
        // The one name that would map to the same table is one OpenSearch refuses.
        expectThrows(InvalidIndexNameException.class, () -> MetadataCreateIndexService.validateIndexOrAliasName("-index", InvalidIndexNameException::new));
    }

    public void testAnIndexWhoseTableWouldBeTheRegistrysIsRefusedAtCreate() {
        // Only a table named explicitly can be the registry's, as a template may name it.
        for (String mode : List.of(InfinoPlugin.MODE_PROXY, InfinoPlugin.MODE_DEFAULT)) {
            Settings settings = Settings.builder()
                .put(InfinoPlugin.INFINO_MODE.getKey(), mode)
                .put(InfinoPlugin.INFINO_TABLE.getKey(), InfinoRegistry.TABLE)
                .build();
            IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> InfinoTables.refuseRegistryTable("logs", settings)
            );
            assertTrue(e.getMessage(), e.getMessage().contains("index [logs] would be served from table [os_index]"));
        }
        Settings named = Settings.builder().put(InfinoPlugin.INFINO_TABLE.getKey(), "os_other").build();
        InfinoTables.refuseRegistryTable("index", named);
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

    /** The settings of a request for an index attached to {@code table} (none when null), with {@code extra}. */
    private static Settings attaching(String table, Settings extra) {
        Settings.Builder out = Settings.builder().put(InfinoPlugin.INFINO_ATTACHED.getKey(), true).put(extra);
        if (table != null) {
            out.put(InfinoPlugin.INFINO_TABLE.getKey(), table);
        }
        return out.build();
    }

    public void testAnAttachedIndexNamesItsTableAndIsServedInProxyMode() {
        Settings proxy = Settings.builder().put(InfinoPlugin.INFINO_MODE.getKey(), InfinoPlugin.MODE_PROXY).build();
        Settings inDefaultMode = Settings.builder().put(InfinoPlugin.INFINO_MODE.getKey(), InfinoPlugin.MODE_DEFAULT).build();
        assertNull(InfinoExplicitTableActionFilter.attachRefusal(attaching("pageviews", Settings.EMPTY), "pageviews"));
        assertNull(InfinoExplicitTableActionFilter.attachRefusal(attaching("pageviews", proxy), "pageviews"));
        String unnamed = InfinoExplicitTableActionFilter.attachRefusal(attaching(null, Settings.EMPTY), null);
        assertTrue(unnamed, unnamed.contains("needs [index.infino.table]"));
        String shards = InfinoExplicitTableActionFilter.attachRefusal(attaching("pageviews", inDefaultMode), "pageviews");
        assertTrue(shards, shards.contains("to be [proxy]"));
        // A request that asks for no attached index is none of this check's.
        assertNull(InfinoExplicitTableActionFilter.attachRefusal(Settings.EMPTY, null));
        assertNull(InfinoExplicitTableActionFilter.attachRefusal(inDefaultMode, "os_logs"));
    }

    public void testEveryWriteToAnAttachedIndexIsRefusedAndNoneToAnyOther() {
        IndexMetadata attached = index("pageviews-ui", "u1", 1, attaching("pageviews", Settings.EMPTY));
        IndexMetadata owned = index("logs", "u2", 2);
        ClusterState state = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(Metadata.builder().put(attached, false).put(owned, false))
            .build();
        Map<String, Object> doc = Map.of("title", "x");
        assertEquals("pageviews-ui", attachedTarget(state, new BulkRequest().add(new IndexRequest("pageviews-ui").id("1").source(doc))));
        assertEquals("pageviews-ui", attachedTarget(state, new BulkRequest().add(new DeleteRequest("pageviews-ui", "1"))));
        assertEquals("pageviews-ui", attachedTarget(state, new BulkRequest().add(new UpdateRequest("pageviews-ui", "1").doc(doc))));
        // One item to an attached index refuses the bulk it came in.
        assertEquals(
            "pageviews-ui",
            attachedTarget(state, new BulkRequest().add(new IndexRequest("logs").id("1").source(doc)).add(new DeleteRequest("pageviews-ui", "2")))
        );
        assertNull(attachedTarget(state, new BulkRequest().add(new IndexRequest("logs").id("1").source(doc))));
        // An index the cluster does not hold yet is the bulk's to create, as before.
        assertNull(attachedTarget(state, new BulkRequest().add(new IndexRequest("fresh").id("1").source(doc))));
    }

    /** The attached index a bulk writes to as it now stands, as the filter reads it before and after the bulk's pipelines. */
    private static String attachedTarget(ClusterState state, BulkRequest bulk) {
        return InfinoBulkActionFilter.attachedTarget(state, InfinoBulkActionFilter.concreteIndices(state, bulk));
    }

    public void testADocumentAPipelineSendsToAnAttachedIndexIsRefusedThere() {
        IndexMetadata attached = index("pageviews-ui", "u1", 1, attaching("pageviews", Settings.EMPTY));
        IndexMetadata owned = index("logs", "u2", 2);
        ClusterState state = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(Metadata.builder().put(attached, false).put(owned, false))
            .build();
        // The bulk names an index of the plugin's own, and is let through as it arrives.
        IndexRequest item = new IndexRequest("logs").id("1").source(Map.of("title", "x"));
        BulkRequest bulk = new BulkRequest().add(item);
        assertNull(InfinoBulkActionFilter.attachedRefusal(state, InfinoBulkActionFilter.concreteIndices(state, bulk)));
        // Its pipeline then sets the document's index, as a script setting ctx._index does. The
        // check made again where the bulk is served from sees where the document now goes.
        item.index("pageviews-ui");
        OpenSearchStatusException refused = InfinoBulkActionFilter.attachedRefusal(state, InfinoBulkActionFilter.concreteIndices(state, bulk));
        assertNotNull(refused);
        assertEquals(RestStatus.FORBIDDEN, refused.status());
        assertTrue(refused.getMessage(), refused.getMessage().contains("[pageviews-ui] is attached to a table it reads and never writes"));
    }

    public void testATemplateCannotPutAnAttachedIndexInDefaultMode() {
        // The settings the cluster creates the index with: the request asks to attach, and a
        // template that matches its name gave the mode. Neither alone is refused.
        Settings merged = Settings.builder()
            .put(InfinoPlugin.INFINO_ENABLED.getKey(), true)
            .put(InfinoPlugin.INFINO_MODE.getKey(), InfinoPlugin.MODE_DEFAULT)
            .put(attaching("pageviews", Settings.EMPTY))
            .build();
        IllegalArgumentException mode = expectThrows(IllegalArgumentException.class, () -> InfinoPlugin.checkAttach(merged));
        assertTrue(mode.getMessage(), mode.getMessage().contains("to be [proxy]"));
        // A template that gives the attaching without the table is refused the same way.
        Settings unnamed = Settings.builder().put(InfinoPlugin.INFINO_ENABLED.getKey(), true).put(attaching(null, Settings.EMPTY)).build();
        IllegalArgumentException table = expectThrows(IllegalArgumentException.class, () -> InfinoPlugin.checkAttach(unnamed));
        assertTrue(table.getMessage(), table.getMessage().contains("needs [index.infino.table]"));
        // Settings that attach nothing, or attach as they should, pass.
        InfinoPlugin.checkAttach(Settings.builder().put(InfinoPlugin.INFINO_MODE.getKey(), InfinoPlugin.MODE_DEFAULT).build());
        InfinoPlugin.checkAttach(attaching("pageviews", Settings.EMPTY));
    }

    public void testAMappingUpdateOnAnAttachedIndexIsRefused() {
        IndexMetadata attached = index("pageviews-ui", "u1", 1, attaching("pageviews", Settings.EMPTY));
        IndexMetadata owned = index("logs", "u2", 2);
        ClusterState state = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(Metadata.builder().put(attached, false).put(owned, false))
            .build();
        assertEquals("pageviews-ui", InfinoAttachedMappingActionFilter.attachedTarget(state, null, new PutMappingRequest("pageviews-ui")));
        assertEquals("pageviews-ui", InfinoAttachedMappingActionFilter.attachedTarget(state, null, new PutMappingRequest("logs", "pageviews-ui")));
        // A dynamic update names its one index outright.
        assertEquals(
            "pageviews-ui",
            InfinoAttachedMappingActionFilter.attachedTarget(state, null, new PutMappingRequest().setConcreteIndex(attached.getIndex()))
        );
        // An index of the plugin's own takes mapping updates as before, and one the cluster does not hold is the cluster's to report.
        assertNull(InfinoAttachedMappingActionFilter.attachedTarget(state, null, new PutMappingRequest("logs")));
        assertNull(InfinoAttachedMappingActionFilter.attachedTarget(state, null, new PutMappingRequest("absent")));
    }

    public void testATableIsDeclaredAgainOverANewConnection() {
        InfinoTables tables = new InfinoTables();
        tables.forgetOver(1);
        tables.declaredForTest("pageviews", 3);
        assertTrue(tables.declaredAt("pageviews", 3));
        // The same connection keeps what was declared over it.
        tables.forgetOver(1);
        assertTrue(tables.declaredAt("pageviews", 3));
        // A new one holds no declaration, an attached table's view among them: each is made again.
        tables.forgetOver(2);
        assertFalse(tables.declaredAt("pageviews", 3));
    }

    public void testAnAttachedTableIsNeverCompacted() {
        // Answered before the bridge is loaded: the table is its owner's to compact.
        assertEquals(0, InfinoNative.optimize("pageviews", attaching("pageviews", Settings.EMPTY)));
    }
}
