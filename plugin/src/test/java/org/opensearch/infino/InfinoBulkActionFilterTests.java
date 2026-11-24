/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import org.opensearch.ExceptionsHelper;
import org.opensearch.Version;
import org.opensearch.action.DocWriteRequest;
import org.opensearch.action.bulk.BulkItemResponse;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.delete.DeleteRequest;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.support.AutoCreateIndex;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.AliasMetadata;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.index.VersionType;
import org.opensearch.indices.SystemIndices;
import org.opensearch.test.OpenSearchTestCase;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/** How a bulk's items that require their index be an alias are answered. */
public class InfinoBulkActionFilterTests extends OpenSearchTestCase {

    private static IndexRequest write(String index, boolean requireAlias) {
        return new IndexRequest(index).id("1").source(Map.of("f", 1)).setRequireAlias(requireAlias);
    }

    public void testANameAnyItemRequiresBeAnAliasIsNeverCreated() {
        BulkRequest bulk = new BulkRequest().add(write("new_index_not_created", true))
            .add(write("new_index_created", false))
            .add(write("test_require_alias", true))
            .add(write("test_require_alias", false));
        assertEquals(Set.of("new_index_not_created", "test_require_alias"), InfinoBulkActionFilter.aliasRequired(bulk));
        // One item that requires an alias keeps the name from being created for the items that do not.
        BulkRequest mixed = new BulkRequest().add(write("logs", false)).add(write("logs", true));
        assertEquals(Set.of("logs"), InfinoBulkActionFilter.aliasRequired(mixed));
        assertEquals(Set.of(), InfinoBulkActionFilter.aliasRequired(new BulkRequest().add(write("logs", false))));
    }

    public void testAnItemRequiringAnAliasFailsOnAnythingButAnAlias() {
        IndexMetadata backing = IndexMetadata.builder("backing_index")
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            )
            .putAlias(AliasMetadata.builder("test_require_alias"))
            .build();
        Metadata metadata = Metadata.builder().put(backing, false).build();

        IndexNotFoundException missing = InfinoBulkActionFilter.notAnAlias(write("new_index_not_created", true), metadata);
        assertNotNull(missing);
        assertEquals(RestStatus.NOT_FOUND, ExceptionsHelper.status(missing));
        assertEquals(
            "no such index [new_index_not_created] and [require_alias] request flag is [true] and [new_index_not_created] is not an alias",
            missing.getMessage()
        );
        // An index that exists is still no alias.
        IndexNotFoundException concrete = InfinoBulkActionFilter.notAnAlias(write("backing_index", true), metadata);
        assertNotNull(concrete);
        assertEquals(
            "no such index [backing_index] and [require_alias] request flag is [true] and [backing_index] is not an alias",
            concrete.getMessage()
        );
        assertNull(InfinoBulkActionFilter.notAnAlias(write("test_require_alias", true), metadata));
        // An item that requires nothing passes wherever it writes.
        assertNull(InfinoBulkActionFilter.notAnAlias(write("new_index_created", false), metadata));
        assertNull(InfinoBulkActionFilter.notAnAlias(write("backing_index", false), metadata));
    }

    public void testAnItemToAMissingNameAnotherItemRequiresBeAnAliasFindsNoIndex() {
        IndexMetadata base = IndexMetadata.builder("base")
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            )
            .putAlias(AliasMetadata.builder("base_alias"))
            .build();
        Metadata metadata = Metadata.builder().put(base, false).build();
        BulkRequest bulk = new BulkRequest().add(write("base", false))
            .add(write("new_index", true))
            .add(write("new_index", false))
            .add(write("base_alias", true))
            .add(write("base_alias", false))
            .add(write("other_new", false));
        Set<String> aliasRequired = InfinoBulkActionFilter.aliasRequired(bulk);

        // The flagless item to the name the flagged one requires be an alias finds no index, as stock answers it.
        IndexNotFoundException missing = InfinoBulkActionFilter.notCreated(write("new_index", false), aliasRequired, metadata, null);
        assertNotNull(missing);
        assertEquals(RestStatus.NOT_FOUND, ExceptionsHelper.status(missing));
        assertEquals("no such index [new_index]", missing.getMessage());
        // A name that exists, as an index or an alias, is written as usual.
        assertNull(InfinoBulkActionFilter.notCreated(write("base", false), aliasRequired, metadata, null));
        assertNull(InfinoBulkActionFilter.notCreated(write("base_alias", false), aliasRequired, metadata, null));
        // A missing name no item requires be an alias is created as usual.
        assertNull(InfinoBulkActionFilter.notCreated(write("other_new", false), aliasRequired, metadata, null));
    }

    private static Metadata indexNamed(String name) {
        IndexMetadata index = IndexMetadata.builder(name)
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            )
            .build();
        return Metadata.builder().put(index, false).build();
    }

    public void testAnItemAPipelineAlreadyAnsweredRequiresNoAlias() {
        BulkRequest bulk = new BulkRequest().add(write("new_index", false)).add(write("new_index", true)).add(write("other", true));
        BulkItemResponse[] answered = new BulkItemResponse[3];
        answered[1] = new BulkItemResponse(
            1,
            DocWriteRequest.OpType.INDEX,
            new BulkItemResponse.Failure("new_index", "1", new IllegalStateException("pipeline failed"))
        );
        // The flagged item a pipeline failed never reaches the index, so it keeps no name from being created.
        Set<String> aliasRequired = InfinoBulkActionFilter.aliasRequired(bulk, answered);
        assertEquals(Set.of("other"), aliasRequired);
        assertNull(InfinoBulkActionFilter.notCreated(write("new_index", false), aliasRequired, Metadata.EMPTY_METADATA, null));
        assertEquals(Set.of("new_index", "other"), InfinoBulkActionFilter.aliasRequired(bulk, new BulkItemResponse[3]));
    }

    public void testNoIndexIsCreatedForAnItemAPipelineAlreadyAnswered() {
        IndexNameExpressionResolver resolver = new IndexNameExpressionResolver(new ThreadContext(Settings.EMPTY));
        AutoCreateIndex rule = new AutoCreateIndex(
            Settings.EMPTY,
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
            resolver,
            new SystemIndices(Map.of())
        );
        ClusterState state = ClusterState.builder(ClusterName.DEFAULT).metadata(indexNamed("existing")).build();
        BulkRequest bulk = new BulkRequest().add(write("dropped_only", false))
            .add(write("kept", false))
            .add(write("existing", false))
            .add(write("flag_failed", false))
            .add(write("flag_failed", true));
        BulkItemResponse[] answered = new BulkItemResponse[5];
        answered[0] = new BulkItemResponse(
            0,
            DocWriteRequest.OpType.INDEX,
            new BulkItemResponse.Failure("dropped_only", "1", new IllegalStateException("pipeline failed"))
        );
        answered[4] = new BulkItemResponse(
            4,
            DocWriteRequest.OpType.INDEX,
            new BulkItemResponse.Failure("flag_failed", "1", new IllegalStateException("pipeline failed"))
        );
        // Only the names the items the pipelines left write to are created; a failed flagged item blocks none.
        assertEquals(List.of("kept", "flag_failed"), InfinoBulkActionFilter.missingIndices(state, rule, bulk, answered));
        assertEquals(List.of("dropped_only", "kept"), InfinoBulkActionFilter.missingIndices(state, rule, bulk, null));
    }

    /** An index that cannot be created fails the items that write to it, each in its own slot; the rest go on. */
    public void testAnIndexThatCannotBeCreatedFailsOnlyItsOwnItems() {
        BulkRequest bulk = new BulkRequest().add(write("bad", false)).add(write("good", false)).add(write("bad", false)).add(write("bad", false));
        BulkItemResponse[] answered = new BulkItemResponse[4];
        BulkItemResponse byPipeline = new BulkItemResponse(
            3,
            DocWriteRequest.OpType.INDEX,
            new BulkItemResponse.Failure("bad", "1", new IllegalStateException("pipeline failed"))
        );
        answered[3] = byPipeline;
        InfinoBulkActionFilter.failItemsOf("bad", new IllegalArgumentException("cannot create"), bulk, answered);
        for (int slot : new int[] { 0, 2 }) {
            assertTrue(answered[slot].isFailed());
            assertEquals(slot, answered[slot].getItemId());
            assertEquals("bad", answered[slot].getFailure().getIndex());
            assertEquals("cannot create", answered[slot].getFailure().getCause().getMessage());
            assertEquals(400, answered[slot].getFailure().getStatus().getStatus());
        }
        assertNull(answered[1]);
        // An item already answered keeps its answer.
        assertSame(byPipeline, answered[3]);
    }

    /**
     * Every missing index is asked for at once; one that cannot be created
     * fails its own items, and the bulk goes on once, after the last answer.
     */
    public void testMissingIndicesAreCreatedTogetherAndTheBulkGoesOnOnce() {
        BulkRequest bulk = new BulkRequest().add(write("a", false)).add(write("bad", false)).add(write("b", false)).add(write("bad", false));
        BulkItemResponse[] answered = new BulkItemResponse[4];
        Map<String, ActionListener<Void>> asked = new LinkedHashMap<>();
        AtomicInteger ran = new AtomicInteger();
        InfinoBulkActionFilter.createMissing(
            List.of("a", "bad", "b"),
            bulk,
            answered,
            ran::incrementAndGet,
            (index, timeout, listener) -> asked.put(index, listener)
        );
        // None waits on another's answer.
        assertEquals(List.of("a", "bad", "b"), List.copyOf(asked.keySet()));
        asked.get("bad").onFailure(new IllegalArgumentException("cannot create"));
        asked.get("a").onResponse(null);
        assertEquals(0, ran.get());
        asked.get("b").onResponse(null);
        assertEquals(1, ran.get());
        // A second answer to a creation already answered changes nothing.
        asked.get("a").onFailure(new IllegalStateException("late"));
        asked.get("b").onResponse(null);
        assertEquals(1, ran.get());
        assertNull(answered[0]);
        assertNull(answered[2]);
        assertEquals("cannot create", answered[1].getFailure().getCause().getMessage());
        assertEquals("cannot create", answered[3].getFailure().getCause().getMessage());
    }

    /** As the stock bulk: a delete creates no index unless it carries an external version. */
    public void testADeleteCreatesNoIndexWithoutAnExternalVersion() {
        IndexNameExpressionResolver resolver = new IndexNameExpressionResolver(new ThreadContext(Settings.EMPTY));
        AutoCreateIndex rule = new AutoCreateIndex(
            Settings.EMPTY,
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
            resolver,
            new SystemIndices(Map.of())
        );
        ClusterState state = ClusterState.builder(ClusterName.DEFAULT).metadata(Metadata.builder().build()).build();
        BulkRequest bulk = new BulkRequest().add(new DeleteRequest("plain", "1"))
            .add(new DeleteRequest("external", "1").version(3).versionType(VersionType.EXTERNAL))
            .add(new DeleteRequest("external_gte", "1").version(3).versionType(VersionType.EXTERNAL_GTE))
            .add(new DeleteRequest("written", "1"))
            .add(write("written", false));
        assertEquals(List.of("external", "external_gte", "written"), InfinoBulkActionFilter.missingIndices(state, rule, bulk, null));
    }

    public void testADateMathNameIsCheckedAsTheIndexItResolvesTo() {
        IndexNameExpressionResolver resolver = new IndexNameExpressionResolver(new ThreadContext(Settings.EMPTY));
        String expression = "<logs-{now/d}>";
        String today = resolver.resolveDateMathExpression(expression);
        assertNotEquals(expression, today);
        Set<String> aliasRequired = Set.of(expression);
        // The index the expression names today exists, so the flagless item writes it.
        assertNull(InfinoBulkActionFilter.notCreated(write(expression, false), aliasRequired, indexNamed(today), resolver));
        // Absent, it finds no index, named as the expression resolves.
        IndexNotFoundException missing = InfinoBulkActionFilter.notCreated(
            write(expression, false),
            aliasRequired,
            Metadata.EMPTY_METADATA,
            resolver
        );
        assertNotNull(missing);
        assertEquals("no such index [" + today + "]", missing.getMessage());
        // Date math that does not parse is left to the stock bulk, which reports it.
        String openEnded = "<logs-{now/d>";
        expectThrows(RuntimeException.class, () -> resolver.resolveDateMathExpression(openEnded));
        assertNull(InfinoBulkActionFilter.notCreated(write(openEnded, false), Set.of(openEnded), Metadata.EMPTY_METADATA, resolver));
    }
}
