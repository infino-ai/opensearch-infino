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
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.index.get.GetResult;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.search.fetch.subphase.FetchSourceContext;
import org.opensearch.tasks.Task;

import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Serves {@code GET /index/_doc/id} for an Infino-backed index from the
 * table, through the id column's term index, before the request is routed to
 * the shard that holds nothing. The index may be named through an alias.
 * Version and sequence number are not carried; {@code _source} includes and
 * excludes are applied.
 */
public class InfinoGetActionFilter implements ActionFilter {

    private static final Logger log = LogManager.getLogger(InfinoGetActionFilter.class);
    /** Version reported for a document served from the table, which keeps no per-document version. */
    private static final long UNVERSIONED = 1L;

    private final Supplier<ClusterService> clusterService;
    private final Supplier<IndexNameExpressionResolver> resolver;
    private final InfinoTables tables;

    public InfinoGetActionFilter(Supplier<ClusterService> clusterService, Supplier<IndexNameExpressionResolver> resolver, InfinoTables tables) {
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
        if (!GetAction.NAME.equals(action) || !(request instanceof GetRequest get)) {
            chain.proceed(task, action, request, listener);
            return;
        }
        ClusterService clusters = clusterService.get();
        IndexMetadata index = clusters == null ? null : infinoIndex(clusters.state(), resolver.get(), get.index());
        if (index == null) {
            chain.proceed(task, action, request, listener);
            return;
        }
        try {
            InfinoSecurity.refuseIfRestricted(InfinoServices.threadContext(), List.of(index.getIndex().getName()), "get");
            String table = tables.ensure(index);
            String source = InfinoNative.getSource(table, get.id());
            @SuppressWarnings("unchecked")
            Response response = (Response) new GetResponse(result(index.getIndex().getName(), get.id(), source, get.fetchSourceContext()));
            listener.onResponse(response);
        } catch (Exception e) {
            log.warn("[infino] get of {} from {} failed", get.id(), get.index(), e);
            listener.onFailure(e);
        }
    }

    /** The Infino-backed index a request names, directly or through an alias; null when it names something else. */
    static IndexMetadata infinoIndex(ClusterState state, IndexNameExpressionResolver resolver, String name) {
        String concrete = name;
        if (resolver != null) {
            try {
                String[] names = resolver.concreteIndexNames(state, IndicesOptions.strictSingleIndexNoExpandForbidClosed(), name);
                if (names.length == 1) {
                    concrete = names[0];
                }
            } catch (Exception e) {
                return null; // a missing index is reported the usual way
            }
        }
        IndexMetadata md = state.metadata().index(concrete);
        return md != null && md.getSettings().getAsBoolean(InfinoPlugin.INFINO_ENABLED.getKey(), false) ? md : null;
    }

    /** A get result from a source string (null or empty when the document is missing), with the request's source filter applied. */
    static GetResult result(String index, String id, String source, FetchSourceContext fetch) {
        boolean exists = source != null && !source.isEmpty();
        BytesReference bytes = null;
        if (exists && (fetch == null || fetch.fetchSource())) {
            boolean includes = fetch != null && fetch.includes() != null && fetch.includes().length > 0;
            boolean excludes = fetch != null && fetch.excludes() != null && fetch.excludes().length > 0;
            bytes = includes || excludes ? InfinoSearch.filtered(source, fetch) : new BytesArray(source);
        }
        return new GetResult(
            index,
            id,
            SequenceNumbers.UNASSIGNED_SEQ_NO,
            SequenceNumbers.UNASSIGNED_PRIMARY_TERM,
            exists ? UNVERSIONED : -1L,
            exists,
            bytes,
            Map.of(),
            Map.of()
        );
    }
}
