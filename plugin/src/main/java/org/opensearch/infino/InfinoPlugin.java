/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.support.ActionFilter;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.common.lifecycle.LifecycleComponent;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.IndexScopedSettings;
import org.opensearch.common.settings.SecureSetting;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.settings.SettingsFilter;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.common.settings.SecureString;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.IndexModule;
import org.opensearch.index.shard.IndexEventListener;
import org.opensearch.index.shard.IndexSettingProvider;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.plugins.ActionPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.SearchPlugin;
import org.opensearch.rest.RestController;
import org.opensearch.rest.RestHandler;
import org.opensearch.threadpool.Scheduler;
import org.opensearch.threadpool.ThreadPool;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

/**
 * Backs an index with an Infino table at the action layer: bulk, search and
 * get for an index with {@code index.infino.enabled} are answered from the
 * table before they are routed to a shard, so no document is ever written
 * to Lucene. Every index the cluster creates gets the setting unless its
 * request or template sets it (node setting
 * {@code infino.index.default_enabled}, on by default), so the node holds
 * no index data of its own. The index itself exists in cluster state as a
 * name, a mapping (which declares the table's columns) and its settings;
 * its one shard stays empty.
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
     * How often an index's table is compacted. Every append commits as its
     * own superfile, so without this a steady stream of events leaves the
     * table as thousands of small files.
     */
    public static final Setting<TimeValue> OPTIMIZE_INTERVAL = Setting.timeSetting(
        "index.infino.optimize_interval",
        TimeValue.timeValueSeconds(60),
        TimeValue.timeValueSeconds(5),
        Setting.Property.IndexScope,
        Setting.Property.Dynamic
    );

    /**
     * Whether every index the cluster creates is Infino-backed unless its own
     * settings say otherwise: on by default, so the node keeps no index data
     * of its own, the indices other plugins create for themselves included.
     */
    public static final Setting<Boolean> INFINO_DEFAULT_ENABLED = Setting.boolSetting("infino.index.default_enabled", true, Setting.Property.NodeScope);

    /**
     * The hosted account the node's tables live on, in the node's own
     * settings (see {@link InfinoAccount}): the service URL and the database
     * in opensearch.yml, the key in the node's keystore. A node is configured
     * and started; nothing about the account lives in the cluster, so a node
     * with an empty data directory serves the account's tables as soon as it
     * is up. Empty when the node runs over its own storage.
     */
    public static final Setting<String> PLATFORM_URL = Setting.simpleString("infino.platform.url", Setting.Property.NodeScope);
    /** The account's database; the cluster's name, mapped to the service's rule, when unset. */
    public static final Setting<String> PLATFORM_DATABASE = Setting.simpleString("infino.platform.database", Setting.Property.NodeScope);
    /** The account's key, from the keystore ({@code opensearch-keystore add infino.platform.api_key}); {@code INFINO_API_KEY} in the environment is read when unset. */
    public static final Setting<SecureString> PLATFORM_API_KEY = SecureSetting.secureString("infino.platform.api_key", null);
    /** The hosted service a trial is asked from when the request names none. */
    public static final Setting<String> PLATFORM_DEFAULT_URL = Setting.simpleString(
        "infino.platform.default_url",
        InfinoAccount.DEFAULT_PLATFORM_URL,
        Setting.Property.NodeScope
    );

    private static final InfinoTables TABLES = new InfinoTables();
    private final Map<ShardId, Scheduler.Cancellable> optimizers = new ConcurrentHashMap<>();
    private final boolean defaultEnabled;
    private final InfinoAccount account;

    public InfinoPlugin(Settings settings) {
        this.defaultEnabled = INFINO_DEFAULT_ENABLED.get(settings);
        this.account = new InfinoAccount(PLATFORM_DEFAULT_URL.get(settings));
        InfinoRequestCache.configure(settings);
        String url = PLATFORM_URL.get(settings);
        if (url.isEmpty()) {
            return;
        }
        // The keystore is open only while the plugins are constructed, so the
        // key is read here and kept by the node for its life.
        String key;
        try (SecureString secure = PLATFORM_API_KEY.get(settings)) {
            key = secure == null || secure.length() == 0 ? null : secure.toString();
        }
        String database = PLATFORM_DATABASE.get(settings);
        if (database.isEmpty()) {
            database = InfinoAccount.databaseNameFor(ClusterName.CLUSTER_NAME_SETTING.get(settings).value());
        }
        account.configuredAtBoot(url, database, key != null);
        InfinoNative.configure(InfinoNative.Target.hosted(url, database, key));
    }

    /** The tables behind the node's Infino-backed indices. */
    static InfinoTables tables() {
        return TABLES;
    }

    /** The node services arrive by injection (see {@link InfinoServices}). */
    @Override
    public Collection<Class<? extends LifecycleComponent>> getGuiceServiceClasses() {
        return List.of(InfinoServices.class);
    }

    @Override
    public List<ActionFilter> getActionFilters() {
        // The filters are asked for before the services are injected; they read them lazily.
        return List.of(
            new InfinoBulkActionFilter(InfinoServices::clusterService, TABLES, InfinoServices::writer),
            new InfinoUpdateActionFilter(InfinoServices::clusterService, TABLES, InfinoServices::writer),
            new InfinoSearchActionFilter(InfinoServices::clusterService, InfinoServices::indexNameExpressionResolver, InfinoServices::scriptService),
            new InfinoScrollActionFilter(InfinoServices::clusterService, InfinoServices::scriptService),
            new InfinoGetActionFilter(InfinoServices::clusterService, InfinoServices::indexNameExpressionResolver, TABLES),
            new InfinoMultiGetActionFilter(InfinoServices::clusterService, InfinoServices::indexNameExpressionResolver, TABLES)
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
                InfinoRegistry.forget(shardId.getIndexName());
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
        ThreadPool threadPool = InfinoServices.threadPool();
        if (threadPool == null) {
            return;
        }
        // A hosted table is compacted by the service's own optimizer; the node has no say.
        if (InfinoNative.hosted()) {
            return;
        }
        TimeValue interval = shard.indexSettings().getValue(OPTIMIZE_INTERVAL);
        ShardId shardId = shard.shardId();
        Scheduler.Cancellable task = threadPool.scheduleWithFixedDelay(() -> {
            try {
                long start = System.nanoTime();
                int rc = InfinoNative.optimize(table);
                long millis = (System.nanoTime() - start) / 1_000_000L;
                if (rc != 0) {
                    log.warn("[infino] optimize of {} returned {} after {} ms", table, rc, millis);
                } else {
                    log.info("[infino] optimized {} in {} ms", table, millis);
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

    /**
     * Stamp {@code index.infino.enabled} on every index the cluster creates
     * unless the request or a template sets it. A provider's settings sit
     * below templates and requests, so an index created with the setting
     * false keeps it; without the switch the node would hold the data of
     * every index created without the setting, which is the state the
     * plugin exists to keep off the node.
     */
    @Override
    public Collection<IndexSettingProvider> getAdditionalIndexSettingProviders() {
        if (!defaultEnabled) {
            return List.of();
        }
        return List.of(new IndexSettingProvider() {
            @Override
            public Settings getAdditionalIndexSettings(String indexName, boolean isDataStreamIndex, Settings templateAndRequestSettings) {
                if (templateAndRequestSettings.hasValue(INFINO_ENABLED.getKey())) {
                    return Settings.EMPTY;
                }
                return Settings.builder().put(INFINO_ENABLED.getKey(), true).build();
            }
        });
    }

    @Override
    public List<Setting<?>> getSettings() {
        return List.of(
            INFINO_DEFAULT_ENABLED,
            INFINO_ENABLED,
            INFINO_TABLE,
            OPTIMIZE_INTERVAL,
            PLATFORM_URL,
            PLATFORM_DATABASE,
            PLATFORM_API_KEY,
            PLATFORM_DEFAULT_URL
        );
    }

    /** The account route: what the cluster's tables are served from, and how to accept or store a hosted account. */
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
        return List.of(new InfinoAccountRestHandler(account));
    }

    @Override
    public List<QuerySpec<?>> getQueries() {
        return List.of(
            new QuerySpec<>(InfinoKnnQueryBuilder.NAME, InfinoKnnQueryBuilder::new, InfinoKnnQueryBuilder::fromXContent),
            new QuerySpec<>(InfinoHybridQueryBuilder.NAME, InfinoHybridQueryBuilder::new, InfinoHybridQueryBuilder::fromXContent)
        );
    }
}
