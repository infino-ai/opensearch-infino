/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.support.ActionFilter;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.IndexScopedSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.settings.SettingsFilter;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.env.Environment;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.index.IndexModule;
import org.opensearch.index.shard.IndexEventListener;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.plugins.ActionPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.SearchPlugin;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.rest.RestController;
import org.opensearch.rest.RestHandler;
import org.opensearch.script.ScriptService;
import org.opensearch.threadpool.Scheduler;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.Client;
import org.opensearch.watcher.ResourceWatcherService;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

/**
 * Backs an index with an Infino table at the action layer: bulk, search and
 * get for an index created with {@code index.infino.enabled} are answered
 * from the table before they are routed to a shard, so no document is ever
 * written to Lucene. The index itself exists in cluster state as a name, a
 * mapping (which declares the table's columns) and its settings; its one
 * shard stays empty.
 *
 * <p>The table is {@code index.infino.table} when set, else named after the
 * index and its UUID. It is dropped when the index is deleted unless it was
 * named explicitly, so an index recreated on a fresh cluster with the same
 * setting reattaches to the rows its predecessor wrote.
 */
public class InfinoPlugin extends Plugin implements ActionPlugin, SearchPlugin {

    private static final Logger log = LogManager.getLogger(InfinoPlugin.class);

    public static final Setting<Boolean> INFINO_ENABLED = Setting.boolSetting("index.infino.enabled", false, Setting.Property.IndexScope);

    /** The table behind the index, when it is to be named explicitly (to reattach an index to its rows). */
    public static final Setting<String> INFINO_TABLE = Setting.simpleString(
        "index.infino.table",
        Setting.Property.IndexScope,
        Setting.Property.Final
    );

    /**
     * How often an index's table is compacted. Every bulk commits as its own
     * superfile, so without this a steady stream of events leaves the table
     * as thousands of small files.
     */
    public static final Setting<TimeValue> OPTIMIZE_INTERVAL = Setting.timeSetting(
        "index.infino.optimize_interval",
        TimeValue.timeValueSeconds(60),
        TimeValue.timeValueSeconds(5),
        Setting.Property.IndexScope,
        Setting.Property.Dynamic
    );

    private static volatile NamedWriteableRegistry namedWriteableRegistry;
    private static final InfinoTables TABLES = new InfinoTables();
    private ThreadPool threadPool;
    private ClusterService clusterService;
    private IndexNameExpressionResolver indexNameExpressionResolver;
    private ScriptService scriptService;
    private InfinoWriter writer;
    private final Map<ShardId, Scheduler.Cancellable> optimizers = new ConcurrentHashMap<>();

    /** The node's registry, used to read aggregation objects back from their wire form. */
    static NamedWriteableRegistry namedWriteableRegistry() {
        return namedWriteableRegistry;
    }

    /** For tests that build aggregation objects without a node. */
    static void namedWriteableRegistry(NamedWriteableRegistry registry) {
        namedWriteableRegistry = registry;
    }

    /** The tables behind the node's Infino-backed indices. */
    static InfinoTables tables() {
        return TABLES;
    }

    @Override
    public Collection<Object> createComponents(
        Client client,
        ClusterService clusterService,
        ThreadPool threadPool,
        ResourceWatcherService resourceWatcherService,
        ScriptService scriptService,
        NamedXContentRegistry xContentRegistry,
        Environment environment,
        NodeEnvironment nodeEnvironment,
        NamedWriteableRegistry namedWriteableRegistry,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Supplier<RepositoriesService> repositoriesServiceSupplier
    ) {
        InfinoPlugin.namedWriteableRegistry = namedWriteableRegistry;
        this.threadPool = threadPool;
        this.clusterService = clusterService;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.scriptService = scriptService;
        this.writer = new InfinoWriter(threadPool);
        return List.of();
    }

    @Override
    public List<ActionFilter> getActionFilters() {
        // The filters are asked for before createComponents runs; they read the
        // services lazily through the plugin.
        return List.of(
            new InfinoBulkActionFilter(() -> clusterService, TABLES, () -> writer),
            new InfinoSearchActionFilter(() -> clusterService, () -> indexNameExpressionResolver, () -> scriptService),
            new InfinoGetActionFilter(() -> clusterService, TABLES)
        );
    }

    /**
     * Declare the table when the index's shard starts (the mapping is known by
     * then) and compact it on a schedule while the shard is started as a
     * primary; drop the table when the index is deleted, unless it was named
     * explicitly. Neither fires on restart or relocation (those close the
     * shard without deleting), so the table survives those.
     */
    @Override
    public void onIndexModule(IndexModule indexModule) {
        if (!indexModule.getSettings().getAsBoolean(INFINO_ENABLED.getKey(), false)) {
            return;
        }
        indexModule.addIndexEventListener(new IndexEventListener() {
            @Override
            public void afterIndexShardStarted(IndexShard indexShard) {
                if (indexShard.routingEntry() == null || !indexShard.routingEntry().primary()) {
                    return;
                }
                try {
                    String table = TABLES.ensure(indexShard.indexSettings().getIndexMetadata());
                    scheduleOptimize(indexShard, table);
                } catch (Exception e) {
                    log.warn("[infino] table for {} could not be declared", indexShard.shardId().getIndexName(), e);
                }
            }

            @Override
            public void beforeIndexShardClosed(ShardId shardId, IndexShard indexShard, Settings indexSettings) {
                Scheduler.Cancellable task = optimizers.remove(shardId);
                if (task != null) {
                    task.cancel();
                }
            }

            @Override
            public void afterIndexShardDeleted(ShardId shardId, Settings indexSettings) {
                String table = InfinoNative.tableFor(shardId.getIndex(), indexSettings);
                TABLES.forget(table);
                if (InfinoNative.tableIsExplicit(indexSettings)) {
                    log.info("[infino] index {} deleted; its table {} was named explicitly and is kept", shardId.getIndexName(), table);
                    return;
                }
                try {
                    InfinoNative.dropTable(table);
                    log.info("[infino] dropped table {} after index delete", table);
                } catch (Throwable t) {
                    log.warn("[infino] dropTable failed for " + table, t);
                }
            }
        });
    }

    private void scheduleOptimize(IndexShard shard, String table) {
        if (threadPool == null) {
            return;
        }
        TimeValue interval = shard.indexSettings().getValue(OPTIMIZE_INTERVAL);
        ShardId shardId = shard.shardId();
        Scheduler.Cancellable task = threadPool.scheduleWithFixedDelay(() -> {
            try {
                int rc = InfinoNative.optimize(table);
                if (rc != 0) {
                    log.warn("[infino] optimize of {} returned {}", table, rc);
                }
            } catch (Throwable t) {
                log.warn("[infino] scheduled optimize of {} failed", table, t);
            }
        }, interval, ThreadPool.Names.GENERIC);
        Scheduler.Cancellable previous = optimizers.put(shardId, task);
        if (previous != null) {
            previous.cancel();
        }
    }

    @Override
    public List<Setting<?>> getSettings() {
        return List.of(INFINO_ENABLED, INFINO_TABLE, OPTIMIZE_INTERVAL);
    }

    @Override
    public List<RestHandler> getRestHandlers(
        Settings settings,
        RestController restController,
        ClusterSettings clusterSettings,
        IndexScopedSettings indexScopedSettings,
        SettingsFilter settingsFilter,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Supplier<DiscoveryNodes> nodesInCluster
    ) {
        return List.of(new InfinoSqlRestHandler());
    }

    @Override
    public List<QuerySpec<?>> getQueries() {
        return List.of(
            new QuerySpec<>(InfinoKnnQueryBuilder.NAME, InfinoKnnQueryBuilder::new, InfinoKnnQueryBuilder::fromXContent),
            new QuerySpec<>(InfinoHybridQueryBuilder.NAME, InfinoHybridQueryBuilder::new, InfinoHybridQueryBuilder::fromXContent)
        );
    }
}
