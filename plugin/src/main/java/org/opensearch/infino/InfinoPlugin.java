/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.support.ActionFilter;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.common.lifecycle.LifecycleComponent;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.IndexScopedSettings;
import org.opensearch.common.settings.SecureSetting;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.settings.SettingsFilter;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.OpenSearchExecutors;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.common.settings.SecureString;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.env.Environment;
import org.opensearch.http.HttpTransportSettings;
import org.opensearch.index.IndexModule;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.engine.EngineFactory;
import org.opensearch.index.shard.IndexEventListener;
import org.opensearch.index.shard.IndexSettingProvider;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.shard.SearchOperationListener;
import org.opensearch.plugins.ActionPlugin;
import org.opensearch.plugins.EnginePlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.SearchPlugin;
import org.opensearch.rest.RestController;
import org.opensearch.rest.RestHandler;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.fetch.FetchSubPhase;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.query.QueryPhaseSearcher;
import org.opensearch.threadpool.ExecutorBuilder;
import org.opensearch.threadpool.FixedExecutorBuilder;
import org.opensearch.threadpool.ThreadPool;

import java.nio.file.Files;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * Backs an index with an Infino table. Every index the cluster creates gets
 * {@code index.infino.enabled} unless its request or template sets it (node
 * setting {@code infino.index.default_enabled}, on by default).
 *
 * <p>In default mode ({@code index.infino.mode: default}, the setting's
 * default) an index is served through its shards: OpenSearch's own write
 * path through {@link InfinoEngine}, over a Lucene index holding each
 * document's identity only, and each shard's query and fetch phases
 * answered from its table by the same planner
 * ({@link InfinoQueryPhaseSearcher}).
 *
 * <p>In proxy mode ({@code index.infino.mode: proxy}) bulk, search and get
 * are answered from the table at the action layer, before they are routed
 * to a shard, so no document is ever written to Lucene and the node holds
 * no index data of its own. The index itself exists in cluster state as a
 * name, a mapping (which declares the table's columns) and its settings;
 * its one shard stays empty.
 *
 * <p>The table is {@code index.infino.table} when set, else named after the
 * index. A table backs one live index (see {@link InfinoTables}). It is
 * dropped when the index is deleted unless it was named explicitly, so an
 * index recreated on a fresh cluster with the same setting reattaches to the
 * rows its predecessor wrote.
 */
public class InfinoPlugin extends Plugin implements ActionPlugin, SearchPlugin, EnginePlugin {

    private static final Logger log = LogManager.getLogger(InfinoPlugin.class);

    public static final Setting<Boolean> INFINO_ENABLED = Setting.boolSetting("index.infino.enabled", false, Setting.Property.IndexScope);

    /** {@link #INFINO_MODE}: served before any shard, from the table; no Lucene. */
    public static final String MODE_PROXY = "proxy";
    /** {@link #INFINO_MODE}: served through the shard's engine and query phase, over a Lucene skeleton. The setting's default. */
    public static final String MODE_DEFAULT = "default";

    /**
     * Where an Infino-backed index is served. {@code default} (the setting's
     * default) keeps OpenSearch's own write path through
     * {@link InfinoEngine}, whose Lucene index holds each document's identity
     * only, and answers the shard's query phase from the table
     * ({@link InfinoQueryPhaseSearcher}), a table per shard; it needs
     * {@link #DEFAULT_MODE_ENABLED} on the node. {@code proxy} answers bulk,
     * search and get before the request reaches a shard, and nothing is
     * written to Lucene.
     */
    public static final Setting<String> INFINO_MODE = new Setting<>("index.infino.mode", MODE_DEFAULT, mode -> {
        if (!MODE_PROXY.equals(mode) && !MODE_DEFAULT.equals(mode)) {
            throw new IllegalArgumentException("[index.infino.mode] must be [" + MODE_PROXY + "] or [" + MODE_DEFAULT + "], got [" + mode + "]");
        }
        return mode;
    }, Setting.Property.IndexScope, Setting.Property.Final);

    /**
     * Whether this node serves default-mode indices. On by default, as
     * default mode is every index's default. A node turns it off where
     * another search plugin needs the node's one query-phase searcher
     * (neural search's hybrid query, for one); its indices are then created
     * in proxy mode. Unset on a node with neural search installed, it is off
     * (see {@link #servesDefaultMode}).
     */
    public static final Setting<Boolean> DEFAULT_MODE_ENABLED = Setting.boolSetting("infino.default_mode.enabled", true, Setting.Property.NodeScope);

    /**
     * The OpenSearch bundle's neural search plugin, which registers a
     * query-phase searcher of its own (for its hybrid query). OpenSearch
     * starts a node with one at most.
     */
    static final String NEURAL_SEARCH_PLUGIN = "opensearch-neural-search";

    /**
     * Set by the registry on an index it recreates on a fresh cluster (see
     * {@link InfinoRegistry}): the index's table already holds rows, which
     * a default-mode index's new Lucene skeleton knows nothing of, so its
     * engine looks ids up in the table (see {@link InfinoEngine}).
     */
    public static final Setting<Boolean> INFINO_REBUILT = Setting.boolSetting(
        "index.infino.rebuilt",
        false,
        Setting.Property.IndexScope,
        Setting.Property.Final
    );

    /**
     * Whether the index holds a graph's edges, for the engine's graph walks
     * over the SQL route ({@code graph_walk}, {@code graph_rank}): each
     * document is one edge, from the row {@code src_id} of the table
     * {@code src_table} to the row {@code dst_id} of the table
     * {@code dst_table}, each row named as the engine names it, by its table
     * and its {@code _id} (as a SQL read gives them). The index's table
     * declares those columns with the engine's types, and builds the
     * adjacency index the walks read each time it is compacted, so an edge
     * is walked from the compaction after its write.
     */
    public static final Setting<Boolean> INFINO_EDGES = Setting.boolSetting(
        "index.infino.edges",
        false,
        Setting.Property.IndexScope,
        Setting.Property.Final
    );

    /** The table behind the index, when it is to be named explicitly (to reattach an index to its rows). */
    public static final Setting<String> INFINO_TABLE = Setting.simpleString(
        "index.infino.table",
        Setting.Property.IndexScope,
        Setting.Property.Final
    );

    /**
     * Whether the index is attached to a table that existed before it,
     * named in {@link #INFINO_TABLE}: one made and filled outside the
     * plugin, which holds none of the columns the plugin writes beside a
     * document. Such a table is read as it is and never changed: nothing is
     * added to it when the index is created, a mapped field it has no column
     * for is refused, and so are every write to the index and every update
     * of its mapping ({@link InfinoAttachedMappingActionFilter}). An
     * attached index is served in proxy mode, and its table stays when the
     * index is deleted, as any named table does.
     */
    public static final Setting<Boolean> INFINO_ATTACHED = Setting.boolSetting(
        "index.infino.attached",
        false,
        Setting.Property.IndexScope,
        Setting.Property.Final
    );

    /**
     * Whether the index was free to take its table when it was created: set
     * by the cluster at creation ({@link #getAdditionalIndexSettingProviders}),
     * false when a live index the table could back already named it. An
     * index created false is never served from the table, not even after
     * that index is deleted, so a younger index named onto a table never
     * inherits the rows of the one it was refused beside (see
     * {@link InfinoTables}). Private, so no request or template sets it; not
     * copied to a resized index, which is a new index. An index created
     * before the setting carries none and counts as free, as it always has.
     */
    public static final Setting<Boolean> INFINO_TABLE_CLAIMED = Setting.boolSetting(
        "index.infino.table_claimed",
        true,
        Setting.Property.IndexScope,
        Setting.Property.PrivateIndex,
        Setting.Property.Final,
        Setting.Property.NotCopyableOnResize
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

    /**
     * Where the engine keeps a local copy of what it reads from the node's own
     * storage, {@code infino-cache} under the node's first data path when
     * unset. Without one, a table opened after a restart reads its whole file
     * from storage on every query. Not used for a hosted account, which keeps
     * nothing on the node.
     */
    public static final Setting<String> CACHE_DIR = Setting.simpleString("infino.cache.dir", Setting.Property.NodeScope);

    /**
     * How long a read on the hosted service may be answered from a snapshot
     * taken before a write. A refresh waits this long after the last write it
     * covers before it returns, so the search after it sees the writes, as
     * {@code refresh=true} and {@code wait_for} promise. Zero waits for
     * nothing. The node's own storage reads every write at once and never waits.
     */
    public static final Setting<TimeValue> PLATFORM_READ_WINDOW = Setting.timeSetting(
        "infino.platform.read_window",
        TimeValue.timeValueSeconds(1),
        TimeValue.ZERO,
        Setting.Property.NodeScope
    );

    /**
     * How long one request to the hosted service may take before it fails as
     * a service that did not answer (a 503). Zero, the default, leaves the
     * engine's own bound of several minutes. The time bounds every request, a
     * write as much as a read, and a write that runs out of it may still land
     * on the service: a short one suits a node whose searches must not wait
     * and whose writes are few or none, such as one serving attached indices.
     */
    public static final Setting<TimeValue> PLATFORM_REQUEST_TIMEOUT = Setting.timeSetting(
        "infino.platform.request_timeout",
        TimeValue.ZERO,
        TimeValue.ZERO,
        Setting.Property.NodeScope
    );

    /**
     * The most a SQL result may hold, as the JSON the route answers with (see
     * {@link InfinoSql}). The engine's memory budget bounds a statement while
     * it runs, not the answer handed to the node: a statement whose result
     * passes this is refused before the answer is built whole, so one
     * authorized read cannot fill the node's heap. The largest request body
     * the node takes over HTTP when unset.
     */
    public static final Setting<ByteSizeValue> SQL_MAX_RESULT_SIZE = Setting.byteSizeSetting(
        "infino.sql.max_result_size",
        HttpTransportSettings.SETTING_HTTP_MAX_CONTENT_LENGTH,
        Setting.Property.NodeScope
    );

    /** The directory {@link #CACHE_DIR} names under the node's data path by default. */
    static final String DEFAULT_CACHE_SUBDIR = "infino-cache";

    /** The security plugin's setting naming its own index, which a search, get or SQL read reads as empty (see {@link InfinoAccess#reading}). */
    static final String SECURITY_INDEX_SETTING = "plugins.security.config_index_name";

    private static final InfinoTables TABLES = new InfinoTables();
    private final InfinoOptimizer optimizer = new InfinoOptimizer(
        (run, interval) -> InfinoServices.threadPool().scheduleWithFixedDelay(run, interval, ThreadPool.Names.GENERIC)
    );
    private final boolean defaultEnabled;
    private final boolean defaultModeEnabled;
    private final InfinoAccount account;

    public InfinoPlugin(Settings settings) {
        InfinoAccess.securityIndex(settings.get(SECURITY_INDEX_SETTING));
        this.defaultEnabled = INFINO_DEFAULT_ENABLED.get(settings);
        this.defaultModeEnabled = servesDefaultMode(settings);
        if (!defaultModeEnabled && !DEFAULT_MODE_ENABLED.exists(settings)) {
            log.warn(
                "[infino] [{}] is installed and takes the node's query-phase searcher, so this node serves its Infino-backed indices in "
                    + "proxy mode; remove it ({}) to serve them in default mode",
                NEURAL_SEARCH_PLUGIN,
                "bin/opensearch-plugin remove " + NEURAL_SEARCH_PLUGIN
            );
        }
        this.account = new InfinoAccount(PLATFORM_DEFAULT_URL.get(settings));
        InfinoNative.cacheDir(cacheDir(settings));
        InfinoRequestCache.configure(settings);
        InfinoSql.configure(settings);
        InfinoNative.readWindow(PLATFORM_READ_WINDOW.get(settings));
        InfinoNative.requestTimeout(PLATFORM_REQUEST_TIMEOUT.get(settings));
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

    /**
     * The node's cache directory: {@link #CACHE_DIR} when set, else
     * {@link #DEFAULT_CACHE_SUBDIR} under the node's first data path, as the
     * node's environment resolves it (the home's {@code data} when none is
     * set). Null without a home, as on a node built for a unit test.
     */
    static String cacheDir(Settings settings) {
        String explicit = CACHE_DIR.get(settings);
        if (!explicit.isEmpty()) {
            return explicit;
        }
        if (Environment.PATH_HOME_SETTING.get(settings).isEmpty()) {
            return null;
        }
        return new Environment(settings, null).dataFiles()[0].resolve(DEFAULT_CACHE_SUBDIR).toString();
    }

    /**
     * Whether a node with these settings serves default-mode indices:
     * {@link #DEFAULT_MODE_ENABLED} where the node sets it, else on unless
     * neural search is installed, which takes the node's one query-phase
     * searcher (the node then serves its indices in proxy mode, over the
     * same storage). Set on beside neural search, the node cannot start:
     * the two searchers would refuse each other.
     */
    static boolean servesDefaultMode(Settings settings) {
        boolean neural = neuralSearchInstalled(settings);
        if (!DEFAULT_MODE_ENABLED.exists(settings)) {
            return !neural;
        }
        boolean on = DEFAULT_MODE_ENABLED.get(settings);
        if (on && neural) {
            throw new IllegalArgumentException(
                "[infino] ["
                    + DEFAULT_MODE_ENABLED.getKey()
                    + ": true] needs the node's query-phase searcher, which ["
                    + NEURAL_SEARCH_PLUGIN
                    + "] also registers: remove it (bin/opensearch-plugin remove "
                    + NEURAL_SEARCH_PLUGIN
                    + "), or set ["
                    + DEFAULT_MODE_ENABLED.getKey()
                    + ": false] to serve this node's indices in proxy mode"
            );
        }
        return on;
    }

    /**
     * Whether neural search is installed on the node: its directory among the
     * node's plugins. False without a home, as on a node built for a unit test.
     */
    static boolean neuralSearchInstalled(Settings settings) {
        if (Environment.PATH_HOME_SETTING.get(settings).isEmpty()) {
            return false;
        }
        return Files.isDirectory(InfinoNodePaths.plugins(new Environment(settings, null)).resolve(NEURAL_SEARCH_PLUGIN));
    }

    /** The tables behind the node's Infino-backed indices. */
    static InfinoTables tables() {
        return TABLES;
    }

    /** Whether an index with these settings is Infino-backed and in proxy mode, served at the action layer. */
    static boolean inProxyMode(Settings indexSettings) {
        return indexSettings.getAsBoolean(INFINO_ENABLED.getKey(), false) && !MODE_DEFAULT.equals(INFINO_MODE.get(indexSettings));
    }

    /** Whether an index with these settings is Infino-backed and in default mode, served through its engine. */
    static boolean inDefaultMode(Settings indexSettings) {
        return indexSettings.getAsBoolean(INFINO_ENABLED.getKey(), false) && MODE_DEFAULT.equals(INFINO_MODE.get(indexSettings));
    }

    @Override
    public Optional<EngineFactory> getEngineFactory(IndexSettings indexSettings) {
        if (!inDefaultMode(indexSettings.getSettings())) {
            return Optional.empty();
        }
        if (!defaultModeEnabled) {
            // Without the engine the index would fall to the stock one and keep its data in Lucene.
            throw new IllegalArgumentException(
                "[infino] index "
                    + indexSettings.getIndex().getName()
                    + " is in default mode, which needs ["
                    + DEFAULT_MODE_ENABLED.getKey()
                    + ": true] on the node and ["
                    + NEURAL_SEARCH_PLUGIN
                    + "] not installed"
            );
        }
        // The release's own subclass hooks the translog sync (see InfinoEngine#commitBeforeSync).
        return Optional.of(InfinoSyncedEngine::new);
    }

    @Override
    public Optional<QueryPhaseSearcher> getQueryPhaseSearcher() {
        return defaultModeEnabled ? Optional.of(new InfinoQueryPhaseSearcher()) : Optional.empty();
    }

    @Override
    public List<FetchSubPhase> getFetchSubPhases(FetchPhaseConstructionContext context) {
        return defaultModeEnabled ? List.of(new InfinoFetchSubPhase()) : List.of();
    }

    @Override
    public List<SearchExtSpec<?>> getSearchExts() {
        return List.of(new SearchExtSpec<>(InfinoQueryExt.NAME, InfinoQueryExt::new, InfinoQueryExt::fromXContent));
    }

    /** A shard's part of an aggregation the coordinating node answers, as it travels to that node (see {@link InfinoExactAggregation}). */
    @Override
    public List<NamedWriteableRegistry.Entry> getNamedWriteables() {
        return List.of(new NamedWriteableRegistry.Entry(InternalAggregation.class, InfinoExactAggregation.NAME, InfinoExactAggregation::new));
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
            new InfinoMultiGetActionFilter(InfinoServices::clusterService, InfinoServices::indexNameExpressionResolver, TABLES),
            new InfinoStatsActionFilter(InfinoServices::clusterService, TABLES),
            new InfinoExplicitTableActionFilter(),
            new InfinoAttachedMappingActionFilter(InfinoServices::clusterService, InfinoServices::indexNameExpressionResolver)
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
        Index index = indexModule.getIndex();
        indexModule.addSettingsUpdateConsumer(OPTIMIZE_INTERVAL, interval -> optimizer.intervalChanged(index, interval));
        // Whether the shard request cache may keep a search is decided before it is consulted.
        indexModule.addSearchOperationListener(new SearchOperationListener() {
            @Override
            public void onPreQueryPhase(SearchContext searchContext) {
                InfinoQueryPhaseSearcher.beforeQueryPhase(searchContext);
            }
        });
        indexModule.addIndexEventListener(new IndexEventListener() {
            // Which copy of a shard this node holds, for the engine (see InfinoEngine#writesTable).
            @Override
            public void beforeIndexShardRecovery(IndexShard indexShard, IndexSettings indexSettings) {
                if (indexShard.routingEntry() != null) {
                    InfinoEngine.copyOf(indexShard.shardId(), indexShard.routingEntry());
                }
            }

            @Override
            public void shardRoutingChanged(IndexShard indexShard, ShardRouting oldRouting, ShardRouting newRouting) {
                InfinoEngine.copyOf(indexShard.shardId(), newRouting);
                if (oldRouting != null && !oldRouting.primary() && newRouting.primary() && newRouting.started()) {
                    // A replica took over: it finishes a skeleton rebuild the old primary may have left.
                    adoptIfRebuilt(indexShard);
                }
            }

            @Override
            public void afterIndexShardClosed(ShardId shardId, IndexShard indexShard, Settings indexSettings) {
                InfinoEngine.forgetCopy(shardId);
            }

            @Override
            public void afterIndexShardStarted(IndexShard indexShard) {
                if (indexShard.routingEntry() == null || !indexShard.routingEntry().primary()) {
                    return;
                }
                try {
                    String table = TABLES.ensure(indexShard.indexSettings().getIndexMetadata(), indexShard.shardId().id());
                    scheduleOptimize(indexShard, table);
                } catch (Exception e) {
                    log.warn("[infino] table for {} could not be declared", indexShard.shardId().getIndexName(), e);
                }
                adoptIfRebuilt(indexShard);
            }

            @Override
            public void beforeIndexShardClosed(ShardId shardId, IndexShard indexShard, Settings indexSettings) {
                optimizer.stop(shardId);
            }

            @Override
            public void afterIndexShardDeleted(ShardId shardId, Settings indexSettings) {
                String table = InfinoTables.shardTable(InfinoNative.tableFor(shardId.getIndex(), indexSettings), shardId.id(), indexSettings);
                InfinoRegistry.forget(shardId.getIndexName());
                if (TABLES.keptAfterDelete(table, shardId.getIndex())) {
                    // Another live index is served from the table (or it is the
                    // registry's): the rows are not this index's to drop.
                    log.info("[infino] index {} deleted; its table {} backs another index and is kept", shardId.getIndexName(), table);
                    return;
                }
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

    /** Rebuild the skeleton of a primary of a default-mode index the registry recreated (see {@link InfinoSkeletonAdopter}). */
    private static void adoptIfRebuilt(IndexShard shard) {
        Settings settings = shard.indexSettings().getSettings();
        if (!inDefaultMode(settings) || !INFINO_REBUILT.get(settings)) {
            return;
        }
        try {
            InfinoSkeletonAdopter.start(shard, TABLES.ensure(shard.indexSettings().getIndexMetadata(), shard.shardId().id()));
        } catch (Exception e) {
            log.warn("[infino] the skeleton of {} could not be rebuilt", shard.shardId(), e);
        }
    }

    private void scheduleOptimize(IndexShard shard, String table) {
        if (InfinoServices.threadPool() == null) {
            return;
        }
        // A hosted table is compacted by the service's own optimizer; the node has no say.
        if (InfinoNative.hosted()) {
            return;
        }
        TimeValue interval = shard.indexSettings().getValue(OPTIMIZE_INTERVAL);
        Settings indexSettings = shard.indexSettings().getSettings();
        optimizer.start(shard.shardId(), () -> {
            try {
                long start = System.nanoTime();
                int rc = InfinoNative.optimize(table, indexSettings);
                long millis = (System.nanoTime() - start) / 1_000_000L;
                if (rc != 0) {
                    log.warn("[infino] optimize of {} returned {} after {} ms", table, rc, millis);
                } else {
                    log.info("[infino] optimized {} in {} ms", table, millis);
                }
            } catch (Throwable t) {
                log.warn("[infino] scheduled optimize of {} failed", table, t);
            }
        }, interval);
    }

    /**
     * Refuse the settings an index is created with when they ask for an
     * attached index that cannot be served
     * ({@link InfinoExplicitTableActionFilter#attachRefusal}). These are
     * the index's templates' merged with the request's, so a template that
     * gives the mode or leaves out the table is seen here, which the create
     * request alone does not show: an attached index in default mode would
     * be served by a shard's engine, which writes to its table.
     */
    static void checkAttach(Settings templateAndRequestSettings) {
        String table = INFINO_TABLE.get(templateAndRequestSettings);
        String refused = InfinoExplicitTableActionFilter.attachRefusal(templateAndRequestSettings, table.isEmpty() ? null : table);
        if (refused != null) {
            throw new IllegalArgumentException(refused);
        }
    }

    /**
     * Stamp {@code index.infino.enabled} on every index the cluster creates
     * unless the request or a template sets it (and {@link #INFINO_DEFAULT_ENABLED}
     * is on). A provider's settings sit below templates and requests, so an
     * index created with the setting false keeps it; without the switch the
     * node would hold the data of every index created without the setting,
     * which is the state the plugin exists to keep off the node.
     *
     * <p>An Infino-backed index created without a mode is stamped with the
     * one the creating node serves: default mode, or proxy mode where
     * {@link #DEFAULT_MODE_ENABLED} is off. The mode is final, so the index
     * keeps it on every node.
     *
     * <p>Every Infino-backed index is also stamped with whether it was free
     * to take its table ({@link #INFINO_TABLE_CLAIMED}), from the indices the
     * cluster manager last applied. The provider runs on the cluster manager
     * as it creates the index; a resized index is created without providers
     * and so carries none.
     */
    @Override
    public Collection<IndexSettingProvider> getAdditionalIndexSettingProviders() {
        return List.of(new IndexSettingProvider() {
            @Override
            public Settings getAdditionalIndexSettings(String indexName, boolean isDataStreamIndex, Settings templateAndRequestSettings) {
                checkAttach(templateAndRequestSettings);
                Settings.Builder out = Settings.builder();
                boolean enabled;
                if (templateAndRequestSettings.hasValue(INFINO_ENABLED.getKey())) {
                    enabled = templateAndRequestSettings.getAsBoolean(INFINO_ENABLED.getKey(), false);
                } else {
                    enabled = defaultEnabled;
                    if (enabled) {
                        out.put(INFINO_ENABLED.getKey(), true);
                    }
                }
                if (enabled && !templateAndRequestSettings.hasValue(INFINO_MODE.getKey())) {
                    // An attached index has no shard writes to keep a Lucene skeleton of: it is served before any shard.
                    boolean attached = templateAndRequestSettings.getAsBoolean(INFINO_ATTACHED.getKey(), false);
                    out.put(INFINO_MODE.getKey(), defaultModeEnabled && !attached ? MODE_DEFAULT : MODE_PROXY);
                }
                if (enabled) {
                    // The index's tables as it will hold them: the request and its templates over the defaults above.
                    Settings index = Settings.builder().put(out.build()).put(templateAndRequestSettings).build();
                    // Fails the create (400) rather than leaving an index whose shard never starts.
                    InfinoTables.refuseRegistryTable(indexName, index);
                    out.put(INFINO_TABLE_CLAIMED.getKey(), TABLES.claimable(indexName, index));
                }
                return out.build();
            }
        });
    }

    /**
     * The pool a proxy-mode table's appends run on, apart from the
     * {@code write} pool its bulks are staged on. An append to the hosted
     * service can wait for as long as a hosted request may last (up to ten
     * minutes); on the {@code write} pool, a few appends waiting that long
     * would hold every thread of it, and every write on the node, a stock
     * index's included, would wait behind them. Here they hold this pool's
     * threads only: further bulks are staged until their table's batch is
     * full and then refused with 429, and an append its full queue turns away
     * answers its bulks with 429 too.
     */
    public static final String APPEND_POOL = "infino_append";
    /** Appends that may wait for a thread of {@link #APPEND_POOL}; past this, the next is refused with 429. */
    private static final int APPEND_QUEUE = 1_000;

    @Override
    public List<ExecutorBuilder<?>> getExecutorBuilders(Settings settings) {
        return List.of(appendPool(settings));
    }

    /**
     * {@link #APPEND_POOL}: one thread per allocated processor, the {@code write}
     * pool's default size, sized with its own settings under
     * {@code thread_pool.infino_append}. A node that sets
     * {@code thread_pool.write.size} does not carry that over: set
     * {@code thread_pool.infino_append.size} as well.
     */
    static ExecutorBuilder<?> appendPool(Settings settings) {
        return new FixedExecutorBuilder(
            settings,
            APPEND_POOL,
            OpenSearchExecutors.allocatedProcessors(settings),
            APPEND_QUEUE,
            "thread_pool." + APPEND_POOL
        );
    }

    @Override
    public List<Setting<?>> getSettings() {
        return List.of(
            INFINO_DEFAULT_ENABLED,
            INFINO_ENABLED,
            INFINO_MODE,
            DEFAULT_MODE_ENABLED,
            INFINO_REBUILT,
            InfinoSkeletonAdopter.PAUSED,
            INFINO_EDGES,
            INFINO_TABLE,
            INFINO_ATTACHED,
            InfinoSearchWindow.FIELD,
            InfinoSearchWindow.MAX,
            InfinoSearchWindow.PHRASES,
            INFINO_TABLE_CLAIMED,
            OPTIMIZE_INTERVAL,
            PLATFORM_URL,
            PLATFORM_DATABASE,
            PLATFORM_API_KEY,
            PLATFORM_DEFAULT_URL,
            CACHE_DIR,
            PLATFORM_READ_WINDOW,
            PLATFORM_REQUEST_TIMEOUT,
            SQL_MAX_RESULT_SIZE
        );
    }

    /** The plugin's own action: a caller's SQL over Infino-backed indices, authorized as a read of each (see {@link InfinoSql}). */
    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        return List.of(new ActionHandler<>(InfinoSqlAction.INSTANCE, TransportInfinoSqlAction.class));
    }

    /**
     * The account route: what the cluster's tables are served from, and how
     * to accept or store a hosted account; and the SQL route.
     */
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
        // Registered cluster state is applied through the node's own REST API.
        InfinoClusterState.bind(restController);
        return List.of(new InfinoAccountRestHandler(account), new InfinoSqlRestHandler());
    }

    @Override
    public List<QuerySpec<?>> getQueries() {
        return List.of(
            new QuerySpec<>(InfinoKnnQueryBuilder.NAME, InfinoKnnQueryBuilder::new, InfinoKnnQueryBuilder::fromXContent),
            new QuerySpec<>(InfinoHybridQueryBuilder.NAME, InfinoHybridQueryBuilder::new, InfinoHybridQueryBuilder::fromXContent)
        );
    }
}
