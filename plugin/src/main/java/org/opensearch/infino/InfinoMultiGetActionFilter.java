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
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.index.get.GetResult;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.tasks.Task;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Serves {@code _mget} for Infino-backed indices from their tables: the ids of
 * each index are looked up through the id column's term index in one
 * statement, and each item comes back as a get response. A request that
 * also names an index the plugin does not back goes on to the shards as it
 * is. Version and sequence number are not carried, as for a single get.
 */
public class InfinoMultiGetActionFilter implements ActionFilter {

    private static final Logger log = LogManager.getLogger(InfinoMultiGetActionFilter.class);
    /** Version reported for a document served from the table, which keeps no per-document version. */
    private static final long UNVERSIONED = 1L;

    private final Supplier<ClusterService> clusterService;
    private final InfinoTables tables;

    public InfinoMultiGetActionFilter(Supplier<ClusterService> clusterService, InfinoTables tables) {
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
        Map<String, IndexMetadata> indices = new HashMap<>();
        for (MultiGetRequest.Item item : mget.getItems()) {
            IndexMetadata index = indices.computeIfAbsent(item.index(), name -> clusters.state().metadata().index(name));
            if (index == null || !index.getSettings().getAsBoolean(InfinoPlugin.INFINO_ENABLED.getKey(), false)) {
                chain.proceed(task, action, request, listener);
                return;
            }
        }
        try {
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
                boolean exists = source != null && !source.isEmpty();
                GetResult result = new GetResult(
                    indices.get(item.index()).getIndex().getName(),
                    item.id(),
                    SequenceNumbers.UNASSIGNED_SEQ_NO,
                    SequenceNumbers.UNASSIGNED_PRIMARY_TERM,
                    exists ? UNVERSIONED : -1L,
                    exists,
                    exists ? new BytesArray(source.getBytes(StandardCharsets.UTF_8)) : null,
                    Map.of(),
                    Map.of()
                );
                responses[i] = new MultiGetItemResponse(new GetResponse(result), null);
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
