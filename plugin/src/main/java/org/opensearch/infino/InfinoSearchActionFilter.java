/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.search.SearchAction;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.support.ActionFilter;
import org.opensearch.action.support.ActionFilterChain;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.search.SearchExtBuilder;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.tasks.Task;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

/**
 * Attaches the query as written to every search over Infino-enabled indices
 * (see {@link InfinoQueryExt}), on the coordinator, before the shards
 * rewrite it against Lucene statistics that hold no data.
 */
public class InfinoSearchActionFilter implements ActionFilter {

    private final Supplier<ClusterService> clusterService;
    private final Supplier<IndexNameExpressionResolver> resolver;

    public InfinoSearchActionFilter(Supplier<ClusterService> clusterService, Supplier<IndexNameExpressionResolver> resolver) {
        this.clusterService = clusterService;
        this.resolver = resolver;
    }

    @Override
    public int order() {
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
        if (SearchAction.NAME.equals(action) && request instanceof SearchRequest search && touchesInfinoIndex(search)) {
            SearchSourceBuilder source = search.source();
            if (source != null && source.query() != null && source.ext().stream().noneMatch(e -> e instanceof InfinoQueryExt)) {
                List<SearchExtBuilder> ext = new ArrayList<>(source.ext());
                ext.add(new InfinoQueryExt(source.query()));
                source.ext(ext);
            }
        }
        chain.proceed(task, action, request, listener);
    }

    private boolean touchesInfinoIndex(SearchRequest search) {
        ClusterService clusters = clusterService.get();
        IndexNameExpressionResolver names = resolver.get();
        if (clusters == null || names == null || search.indices() == null) {
            return false;
        }
        ClusterState state = clusters.state();
        String[] concrete;
        try {
            concrete = names.concreteIndexNames(state, search);
        } catch (RuntimeException e) {
            return false; // an unresolvable expression fails later, as it would anyway
        }
        for (String name : concrete) {
            IndexMetadata index = state.metadata().index(name);
            if (index != null && index.getSettings().getAsBoolean(InfinoPlugin.INFINO_ENABLED.getKey(), false)) {
                return true;
            }
        }
        return false;
    }
}
