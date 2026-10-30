/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.bulk.BulkItemResponse;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.support.ActionFilter;
import org.opensearch.action.support.ActionFilterChain;
import org.opensearch.action.update.UpdateAction;
import org.opensearch.action.update.UpdateRequest;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.tasks.Task;

import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Serves a single {@code _update} for an Infino-backed index. OpenSearch runs
 * a single index or delete as a one-item bulk, which the bulk filter serves,
 * but an update as its own action that reads the document from the shard
 * first; the shard holds nothing, so the update is turned into a one-item
 * bulk here and served the same way as an update inside a bulk, its item's
 * response handed back as the update's. An index the update names before
 * it exists is created first, as for a bulk. An update to an index in engine
 * mode goes to the cluster as that one-item bulk, which the bulk filter
 * merges and hands to the shard as a write of the whole document.
 */
public class InfinoUpdateActionFilter implements ActionFilter {

    private static final Logger log = LogManager.getLogger(InfinoUpdateActionFilter.class);

    private final Supplier<ClusterService> clusterService;
    private final InfinoTables tables;
    private final Supplier<InfinoWriter> writer;

    public InfinoUpdateActionFilter(Supplier<ClusterService> clusterService, InfinoTables tables, Supplier<InfinoWriter> writer) {
        this.clusterService = clusterService;
        this.tables = tables;
        this.writer = writer;
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
        if (!UpdateAction.NAME.equals(action) || !(request instanceof UpdateRequest update)) {
            chain.proceed(task, action, request, listener);
            return;
        }
        ClusterService clusters = clusterService.get();
        InfinoWriter writes = writer.get();
        if (clusters == null || writes == null) {
            chain.proceed(task, action, request, listener);
            return;
        }
        BulkRequest bulk = new BulkRequest().add(update).setRefreshPolicy(update.getRefreshPolicy());
        InfinoBulkActionFilter.createMissing(InfinoBulkActionFilter.missingIndices(clusters, bulk), ActionListener.wrap(created -> {
            Map<String, IndexMetadata> infino;
            List<String> concrete = InfinoBulkActionFilter.concreteIndices(clusters.state(), bulk);
            try {
                infino = InfinoBulkActionFilter.infinoIndices(clusters, concrete);
            } catch (Exception e) {
                listener.onFailure(e);
                return;
            }
            ActionListener<BulkResponse> answer = ActionListener.wrap(response -> {
                BulkItemResponse item = response.getItems()[0];
                if (item.isFailed()) {
                    listener.onFailure(item.getFailure().getCause());
                    return;
                }
                @SuppressWarnings("unchecked")
                Response cast = (Response) item.getResponse();
                listener.onResponse(cast);
            }, e -> {
                log.debug("[infino] update of {} in {} failed: {}", update.id(), update.index(), e.getMessage());
                listener.onFailure(e);
            });
            if (infino.isEmpty()) {
                IndexMetadata md = concrete.get(0) == null ? null : clusters.state().metadata().index(concrete.get(0));
                if (md != null && InfinoPlugin.atEngine(md.getSettings())) {
                    // The stock update reads the document's source from Lucene, which an
                    // engine-mode index keeps no copy of: as a one-item bulk, the bulk
                    // path merges it here and writes it whole through the shard.
                    InfinoClient.bulk(bulk, answer);
                    return;
                }
                chain.proceed(task, action, request, listener);
                return;
            }
            InfinoBulkActionFilter.serve(bulk, concrete, infino, writes, tables, answer);
        }, listener::onFailure));
    }
}
