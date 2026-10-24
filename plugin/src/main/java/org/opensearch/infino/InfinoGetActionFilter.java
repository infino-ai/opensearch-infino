/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.get.GetAction;
import org.opensearch.action.get.GetRequest;
import org.opensearch.action.get.GetResponse;
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
import java.util.Map;
import java.util.function.Supplier;

/**
 * Serves {@code GET /index/_doc/id} for an Infino-backed index from the
 * table, through the id column's term index, before the request is routed to
 * the shard that holds nothing. Version and sequence number are not carried.
 */
public class InfinoGetActionFilter implements ActionFilter {

    private static final Logger log = LogManager.getLogger(InfinoGetActionFilter.class);
    /** Version reported for a document served from the table, which keeps no per-document version. */
    private static final long UNVERSIONED = 1L;

    private final Supplier<ClusterService> clusterService;
    private final InfinoTables tables;

    public InfinoGetActionFilter(Supplier<ClusterService> clusterService, InfinoTables tables) {
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
        if (!GetAction.NAME.equals(action) || !(request instanceof GetRequest get)) {
            chain.proceed(task, action, request, listener);
            return;
        }
        ClusterService clusters = clusterService.get();
        IndexMetadata index = clusters == null ? null : clusters.state().metadata().index(get.index());
        if (index == null || !index.getSettings().getAsBoolean(InfinoPlugin.INFINO_ENABLED.getKey(), false)) {
            chain.proceed(task, action, request, listener);
            return;
        }
        try {
            String table = tables.ensure(index);
            String source = InfinoNative.getSource(table, get.id());
            boolean exists = source != null && !source.isEmpty();
            GetResult result = new GetResult(
                index.getIndex().getName(),
                get.id(),
                SequenceNumbers.UNASSIGNED_SEQ_NO,
                SequenceNumbers.UNASSIGNED_PRIMARY_TERM,
                exists ? UNVERSIONED : -1L,
                exists,
                exists ? new BytesArray(source.getBytes(StandardCharsets.UTF_8)) : null,
                Map.of(),
                Map.of()
            );
            @SuppressWarnings("unchecked")
            Response response = (Response) new GetResponse(result);
            listener.onResponse(response);
        } catch (Exception e) {
            log.warn("[infino] get of {} from {} failed", get.id(), get.index(), e);
            listener.onFailure(e);
        }
    }
}
