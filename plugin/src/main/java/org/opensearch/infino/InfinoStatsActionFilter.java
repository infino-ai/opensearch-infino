/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.admin.indices.stats.CommonStats;
import org.opensearch.action.admin.indices.stats.IndicesStatsAction;
import org.opensearch.action.admin.indices.stats.IndicesStatsRequest;
import org.opensearch.action.admin.indices.stats.IndicesStatsResponse;
import org.opensearch.action.admin.indices.stats.ShardStats;
import org.opensearch.action.support.ActionFilter;
import org.opensearch.action.support.ActionFilterChain;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.index.shard.DocsStats;
import org.opensearch.infino.InfinoQueryTranslator.Translation;
import org.opensearch.tasks.Task;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Reports the documents of Infino-backed indices in {@code _stats} (and so
 * in {@code _cat/indices} and the index pages of Dashboards), which read
 * them from the shards. The shards of such an index hold no documents, so
 * the request goes on to them as it came, and before the response is handed
 * back each copy of an Infino-backed index's first shard reports the table's
 * row count, every row live. The shards' other stats stay their own.
 */
public class InfinoStatsActionFilter implements ActionFilter {

    private static final Logger log = LogManager.getLogger(InfinoStatsActionFilter.class);
    /** The shard an Infino-backed index's documents are reported under: its table holds them all. */
    private static final int TABLE_SHARD = 0;
    /** Deleted documents reported: a deleted row no longer counts in the table. */
    private static final long NO_DELETED = 0L;

    private final Supplier<ClusterService> clusterService;
    private final InfinoTables tables;

    public InfinoStatsActionFilter(Supplier<ClusterService> clusterService, InfinoTables tables) {
        this.clusterService = clusterService;
        this.tables = tables;
    }

    @Override
    public int order() {
        // After the security plugin's checks, which run first.
        return Integer.MAX_VALUE;
    }

    @Override
    public <Request extends ActionRequest, Response extends ActionResponse> void apply(
        Task task,
        String action,
        Request request,
        ActionListener<Response> listener,
        ActionFilterChain<Request, Response> chain
    ) {
        ClusterService clusters = clusterService.get();
        if (!IndicesStatsAction.NAME.equals(action) || !(request instanceof IndicesStatsRequest stats) || !stats.docs() || clusters == null) {
            chain.proceed(task, action, request, listener);
            return;
        }
        chain.proceed(task, action, request, ActionListener.wrap(response -> {
            if (response instanceof IndicesStatsResponse indices) {
                countDocuments(clusters.state(), indices);
            }
            listener.onResponse(response);
        }, listener::onFailure));
    }

    /** Put each Infino-backed index's row count on its first shard's copies, one count per index. */
    private void countDocuments(ClusterState state, IndicesStatsResponse response) {
        Map<String, Long> counts = new HashMap<>();
        for (ShardStats shard : response.getShards()) {
            CommonStats stats = shard.getStats();
            if (stats.docs == null || shard.getShardRouting().id() != TABLE_SHARD) {
                continue;
            }
            String name = shard.getShardRouting().getIndexName();
            IndexMetadata md = state.metadata().index(name);
            if (md == null || !md.getSettings().getAsBoolean(InfinoPlugin.INFINO_ENABLED.getKey(), false)) {
                continue;
            }
            Long count = counts.computeIfAbsent(name, n -> rows(md));
            if (count != null) {
                stats.docs = new DocsStats(count, NO_DELETED, stats.docs.getTotalSizeInBytes());
            }
        }
    }

    /** The table's row count; null, with the reason logged, when the table cannot answer, so the shards' own count stands. */
    private Long rows(IndexMetadata md) {
        try {
            String table = tables.ensure(md);
            return InfinoSearch.count(table, new Translation(table, InfinoQueryTranslator.ALL, null));
        } catch (Exception e) {
            log.warn("[infino] the document count of {} could not be read from its table", md.getIndex().getName(), e);
            return null;
        }
    }
}
