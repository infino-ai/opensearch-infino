/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import org.opensearch.ExceptionsHelper;
import org.opensearch.Version;
import org.opensearch.action.DocWriteRequest;
import org.opensearch.action.bulk.BulkAction;
import org.opensearch.action.bulk.BulkItemResponse;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.support.ActionFilterChain;
import org.opensearch.action.support.AutoCreateIndex;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.action.update.UpdateRequest;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.AliasMetadata;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.indices.SystemIndices;
import org.opensearch.test.ClusterServiceUtils;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;

import com.sun.jna.Memory;
import com.sun.jna.Pointer;

import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/** How a bulk's items that require their index be an alias are answered. */
public class InfinoBulkActionFilterTests extends OpenSearchTestCase {

    /** A writer's batch bounds, unused by a bulk that goes on to the shards. */
    private static final int MAX_DOCS = 10;
    private static final long MAX_BYTES = 1 << 20;
    /** The name of the thread a test delivers a bulk on. */
    private static final String CALLER = "bulk-caller";
    /** How long a test waits for a step that should take moments. */
    private static final long WAIT_SECONDS = 5;

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

    /**
     * A bulk updating a document of a default-mode index reads the document
     * from the table to merge the update into it. That read can wait on the
     * hosted service, so it runs on the write pool: the thread that delivered
     * the bulk, a transport worker on a node, returns at once while the read
     * waits, and the bulk goes on to the shards once the read answers.
     */
    public void testAnUpdateWaitingOnTheTableHoldsNotTheThreadThatDeliveredIt() throws Exception {
        CountDownLatch reading = new CountDownLatch(1);
        CountDownLatch answer = new CountDownLatch(1);
        AtomicReference<String> readOn = new AtomicReference<>();
        ThreadPool threadPool = new TestThreadPool(getTestName());
        ClusterService clusterService = ClusterServiceUtils.createClusterService(threadPool);
        try {
            IndexMetadata logs = IndexMetadata.builder("logs")
                .settings(
                    settings(Version.CURRENT).put(InfinoPlugin.INFINO_ENABLED.getKey(), true)
                        .put(InfinoPlugin.INFINO_MODE.getKey(), InfinoPlugin.MODE_DEFAULT)
                        .put(IndexMetadata.SETTING_INDEX_UUID, "logs-uuid")
                )
                .numberOfShards(1)
                .numberOfReplicas(0)
                .build();
            ClusterServiceUtils.setState(
                clusterService,
                ClusterState.builder(clusterService.state()).metadata(Metadata.builder(clusterService.state().metadata()).put(logs, false))
            );
            InfinoServices.bind(clusterService, threadPool);
            InfinoNative.useLib((InfinoLib) Proxy.newProxyInstance(InfinoLib.class.getClassLoader(), new Class<?>[] { InfinoLib.class }, (proxy, method, args) -> {
                switch (method.getName()) {
                    case "infino_open_with":
                        return new Memory(1);
                    case "infino_ensure_table":
                        return nativeString("{}");
                    case "infino_get":
                        readOn.set(Thread.currentThread().getName());
                        reading.countDown();
                        answer.await();
                        return nativeString("{\"f\":1}");
                    default:
                        return null;
                }
            }));
            InfinoBulkActionFilter filter = new InfinoBulkActionFilter(
                () -> clusterService,
                new InfinoTables(),
                () -> new InfinoWriter(() -> Runnable::run, MAX_DOCS, MAX_BYTES)
            );
            BulkRequest bulk = new BulkRequest().add(new UpdateRequest("logs", "1").doc(Map.of("f", 2)));
            AtomicReference<BulkRequest> proceeded = new AtomicReference<>();
            CountDownLatch onToShards = new CountDownLatch(1);
            ActionFilterChain<BulkRequest, BulkResponse> chain = (task, action, request, listener) -> {
                proceeded.set(request);
                onToShards.countDown();
            };
            PlainActionFuture<BulkResponse> answered = new PlainActionFuture<>();
            // The thread that delivers the bulk, as a transport worker would.
            Thread caller = new Thread(() -> filter.filter(null, BulkAction.NAME, bulk, answered, chain), CALLER);
            caller.start();
            assertTrue("the read reached the bridge", reading.await(WAIT_SECONDS, TimeUnit.SECONDS));
            caller.join(TimeUnit.SECONDS.toMillis(WAIT_SECONDS));
            assertFalse("the caller returned while the read waits", caller.isAlive());
            assertNotEquals(CALLER, readOn.get());
            assertTrue(readOn.get(), readOn.get().contains("[" + ThreadPool.Names.WRITE + "]"));
            assertNull("nothing went on to the shards before the read answered", proceeded.get());
            answer.countDown();
            assertTrue(onToShards.await(WAIT_SECONDS, TimeUnit.SECONDS));
            IndexRequest merged = (IndexRequest) proceeded.get().requests().get(0);
            assertEquals(Map.of("f", 2), merged.sourceAsMap());
        } finally {
            answer.countDown();
            InfinoServices.bind(null, null);
            InfinoNative.useLib(null);
            clusterService.close();
            terminate(threadPool);
        }
    }

    private static Pointer nativeString(String s) {
        byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
        Memory m = new Memory(bytes.length + 1);
        m.write(0, bytes, 0, bytes.length);
        m.setByte(bytes.length, (byte) 0);
        return m;
    }
}
