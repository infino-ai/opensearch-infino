/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.get.GetResponse;
import org.opensearch.action.get.MultiGetAction;
import org.opensearch.action.get.MultiGetItemResponse;
import org.opensearch.action.get.MultiGetRequest;
import org.opensearch.action.get.MultiGetResponse;
import org.opensearch.action.support.ActionFilter;
import org.opensearch.action.support.ActionFilterChain;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.tasks.Task;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Serves {@code _mget} for Infino-backed indices from their tables: the ids of
 * each index are looked up through the id column's term index in one
 * statement, and each item comes back as a get response. An index may be
 * named through an alias. A request that also names an index the plugin
 * does not back goes on to the shards as it is. Version and sequence number
 * are not carried, as for a single get.
 */
public class InfinoMultiGetActionFilter implements ActionFilter {

    private static final Logger log = LogManager.getLogger(InfinoMultiGetActionFilter.class);

    private final Supplier<ClusterService> clusterService;
    private final Supplier<IndexNameExpressionResolver> resolver;
    private final InfinoTables tables;

    public InfinoMultiGetActionFilter(Supplier<ClusterService> clusterService, Supplier<IndexNameExpressionResolver> resolver, InfinoTables tables) {
        this.clusterService = clusterService;
        this.resolver = resolver;
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
        if (!MultiGetAction.NAME.equals(action) || !(request instanceof MultiGetRequest mget)) {
            chain.proceed(task, action, request, listener);
            return;
        }
        ClusterService clusters = clusterService.get();
        if (clusters == null || mget.getItems().isEmpty()) {
            chain.proceed(task, action, request, listener);
            return;
        }
        // Every item must be on an Infino-backed index for the table to answer the request whole.
        ClusterState state = clusters.state();
        IndexNameExpressionResolver names = resolver.get();
        Map<String, IndexMetadata> indices = new HashMap<>();
        for (MultiGetRequest.Item item : mget.getItems()) {
            IndexMetadata index = indices.computeIfAbsent(item.index(), name -> InfinoGetActionFilter.infinoIndex(state, names, name));
            if (index == null) {
                chain.proceed(task, action, request, listener);
                return;
            }
        }
        try {
            List<String> concrete = new ArrayList<>();
            for (IndexMetadata md : indices.values()) {
                concrete.add(md.getIndex().getName());
            }
            InfinoSecurity.refuseIfRestricted(InfinoServices.threadContext(), concrete, "mget");
            // One source fetch per index, over all of its ids.
            Map<String, List<String>> idsByIndex = new HashMap<>();
            for (MultiGetRequest.Item item : mget.getItems()) {
                idsByIndex.computeIfAbsent(item.index(), k -> new ArrayList<>()).add(item.id());
            }
            Map<String, String> tableOf = new HashMap<>();
            for (Map.Entry<String, List<String>> e : idsByIndex.entrySet()) {
                String table = tables.ensure(indices.get(e.getKey()));
                tableOf.put(e.getKey(), table);
                InfinoNative.prefetchSources(table, e.getValue());
            }
            MultiGetItemResponse[] responses = new MultiGetItemResponse[mget.getItems().size()];
            for (int i = 0; i < responses.length; i++) {
                MultiGetRequest.Item item = mget.getItems().get(i);
                String source = InfinoNative.getSource(tableOf.get(item.index()), item.id());
                GetResponse get = new GetResponse(InfinoGetActionFilter.result(indices.get(item.index()).getIndex().getName(), item.id(), source, item.fetchSourceContext()));
                responses[i] = new MultiGetItemResponse(get, null);
            }
            @SuppressWarnings("unchecked")
            Response response = (Response) new MultiGetResponse(responses);
            listener.onResponse(response);
        } catch (Exception e) {
            log.warn("[infino] mget of {} items failed", mget.getItems().size(), e);
            listener.onFailure(e);
        }
    }
}
