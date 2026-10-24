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
import org.opensearch.index.IndexSettings;
import org.opensearch.index.engine.EngineFactory;
import org.opensearch.index.shard.IndexEventListener;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.plugins.ActionPlugin;
import org.opensearch.plugins.EnginePlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.SearchPlugin;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.rest.RestController;
import org.opensearch.rest.RestHandler;
import org.opensearch.script.ScriptService;
import org.opensearch.search.fetch.FetchSubPhase;
import org.opensearch.search.query.QueryPhaseSearcher;
import org.opensearch.threadpool.Scheduler;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.Client;
import org.opensearch.watcher.ResourceWatcherService;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

/** Registers the Infino-backed engine, a SQL passthrough endpoint, and the Infino query phase. */
public class InfinoPlugin extends Plugin implements EnginePlugin, ActionPlugin, SearchPlugin {

    private static final Logger log = LogManager.getLogger(InfinoPlugin.class);

    public static final Setting<Boolean> INFINO_ENABLED = Setting.boolSetting("index.infino.enabled", false, Setting.Property.IndexScope);

    /**
     * How often a primary shard's Infino table is compacted. Every refresh
     * commits the pending batch as its own superfile, so without this a
     * steady stream of events leaves the table as thousands of small files.
     */
    public static final Setting<TimeValue> OPTIMIZE_INTERVAL = Setting.timeSetting(
        "index.infino.optimize_interval",
        TimeValue.timeValueSeconds(60),
        TimeValue.timeValueSeconds(5),
        Setting.Property.IndexScope,
        Setting.Property.Dynamic
    );

    private static volatile NamedWriteableRegistry namedWriteableRegistry;
    private ThreadPool threadPool;
    private ClusterService clusterService;
    private IndexNameExpressionResolver indexNameExpressionResolver;
    private final Map<ShardId, Scheduler.Cancellable> optimizers = new ConcurrentHashMap<>();

    /** The node's registry, used to read aggregation objects back from their wire form. */
    static NamedWriteableRegistry namedWriteableRegistry() {
        return namedWriteableRegistry;
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
        return List.of();
    }

    @Override
    public List<ActionFilter> getActionFilters() {
        // The filters are asked for before createComponents runs; they read the
        // cluster service lazily through the plugin.
        return List.of(
            new InfinoGetActionFilter(() -> clusterService),
            new InfinoSearchActionFilter(() -> clusterService, () -> indexNameExpressionResolver)
        );
    }

    @Override
    public List<SearchExtSpec<?>> getSearchExts() {
        return List.of(new SearchExtSpec<>(InfinoQueryExt.NAME, InfinoQueryExt::new, InfinoQueryExt::fromXContent));
    }

    /**
     * Drop the per-shard Infino table when its shard is deleted (index delete), so a
     * later same-named index starts clean; compact it on a schedule while the shard
     * is started as a primary. Neither fires on restart or relocation (those close
     * the shard without deleting), so the persisted table survives those.
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
                scheduleOptimize(indexShard);
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
                String table = InfinoNative.tableFor(shardId.getIndexName(), shardId.id());
                try {
                    InfinoNative.dropTable(table);
                    log.info("[infino] dropped table {} after shard delete", table);
                } catch (Throwable t) {
                    log.warn("[infino] dropTable failed for " + table, t);
                }
            }
        });
    }

    private void scheduleOptimize(IndexShard shard) {
        if (threadPool == null) {
            return;
        }
        TimeValue interval = shard.indexSettings().getValue(OPTIMIZE_INTERVAL);
        ShardId shardId = shard.shardId();
        Scheduler.Cancellable task = threadPool.scheduleWithFixedDelay(() -> {
            try {
                InfinoEngine engine = InfinoEngine.forShard(shardId);
                if (engine != null) {
                    engine.optimize();
                }
            } catch (Throwable t) {
                log.warn("[infino] scheduled optimize of {} failed", shardId, t);
            }
        }, interval, ThreadPool.Names.GENERIC);
        Scheduler.Cancellable previous = optimizers.put(shardId, task);
        if (previous != null) {
            previous.cancel();
        }
    }

    @Override
    public List<Setting<?>> getSettings() {
        return List.of(INFINO_ENABLED, OPTIMIZE_INTERVAL);
    }

    @Override
    public Optional<EngineFactory> getEngineFactory(IndexSettings indexSettings) {
        if (indexSettings.getValue(INFINO_ENABLED)) {
            return Optional.of(InfinoEngine::new);
        }
        return Optional.empty();
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
    public Optional<QueryPhaseSearcher> getQueryPhaseSearcher() {
        return Optional.of(new InfinoQueryPhaseSearcher());
    }

    @Override
    public List<FetchSubPhase> getFetchSubPhases(FetchPhaseConstructionContext context) {
        return List.of(new InfinoFetchSubPhase());
    }

    @Override
    public List<QuerySpec<?>> getQueries() {
        return List.of(
            new QuerySpec<>(InfinoKnnQueryBuilder.NAME, InfinoKnnQueryBuilder::new, InfinoKnnQueryBuilder::fromXContent),
            new QuerySpec<>(InfinoHybridQueryBuilder.NAME, InfinoHybridQueryBuilder::new, InfinoHybridQueryBuilder::fromXContent)
        );
    }
}
