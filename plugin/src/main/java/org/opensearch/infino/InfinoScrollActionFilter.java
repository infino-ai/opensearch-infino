/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.search.TotalHits;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.search.ClearScrollAction;
import org.opensearch.action.search.ClearScrollRequest;
import org.opensearch.action.search.ClearScrollResponse;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.SearchScrollAction;
import org.opensearch.action.search.SearchScrollRequest;
import org.opensearch.action.support.ActionFilter;
import org.opensearch.action.support.ActionFilterChain;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.index.Index;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.infino.InfinoScrolls.Scroll;
import org.opensearch.infino.InfinoSearch.Searched;
import org.opensearch.script.ScriptService;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.PipelineAggregationBuilder;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.tasks.Task;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Serves {@code _search/scroll} and its clearing for scrolls this node
 * opened over Infino-backed indices (see {@link InfinoScrolls}): each page
 * is the scroll's search resumed after the cursor of each index, and the
 * cursor moves to where the page stopped. A scroll id another node or
 * OpenSearch itself issued goes on as it is.
 */
public class InfinoScrollActionFilter implements ActionFilter {

    private static final Logger log = LogManager.getLogger(InfinoScrollActionFilter.class);
    /** The thread-context key the security plugin keeps the authenticated user under; its string form names the scroll's owner. */
    private static final String USER_KEY = "_opendistro_security_user";
    /** The id a clear request names to close every scroll. */
    private static final String ALL_SCROLLS = "_all";

    private final Supplier<ClusterService> clusterService;
    private final Supplier<ScriptService> scripts;

    public InfinoScrollActionFilter(Supplier<ClusterService> clusterService, Supplier<ScriptService> scripts) {
        this.clusterService = clusterService;
        this.scripts = scripts;
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
        if (SearchScrollAction.NAME.equals(action) && request instanceof SearchScrollRequest scroll && InfinoScrolls.owns(scroll.scrollId())) {
            @SuppressWarnings("unchecked")
            ActionListener<SearchResponse> cast = (ActionListener<SearchResponse>) listener;
            page(scroll, cast);
            return;
        }
        if (ClearScrollAction.NAME.equals(action) && request instanceof ClearScrollRequest clear) {
            List<String> ours = new ArrayList<>();
            boolean all = false;
            for (String id : clear.getScrollIds()) {
                if (ALL_SCROLLS.equals(id)) {
                    all = true;
                } else if (InfinoScrolls.owns(id)) {
                    ours.add(id);
                }
            }
            int freed = InfinoScrolls.close(all ? List.of(ALL_SCROLLS) : ours);
            if (!all && ours.size() == clear.getScrollIds().size()) {
                // Every id was one of this node's: nothing is left for the chain.
                @SuppressWarnings("unchecked")
                Response response = (Response) new ClearScrollResponse(true, freed);
                listener.onResponse(response);
                return;
            }
            // _all, or ids of other origins beside ours: the chain clears what it holds and reports its own count.
        }
        chain.proceed(task, action, request, listener);
    }

    /** The user the current request runs as, as the security plugin records it; empty with no security plugin. */
    static String principal() {
        ThreadContext context = InfinoServices.threadContext();
        Object user = context == null ? null : context.getTransient(USER_KEY);
        return user == null ? "" : String.valueOf(user);
    }

    /**
     * Open a scroll: run its first page (the request's query, sorts, fields
     * and aggregations, counted once) and keep what the later pages need.
     * The response carries the scroll's id.
     */
    static SearchResponse open(
        SearchRequest search,
        List<Index> indices,
        Function<Index, String> tableOf,
        Map<Index, InfinoAccess> access,
        SearchSourceBuilder source,
        ScriptService scripts,
        long startNanos
    ) {
        if (source.from() > 0) {
            throw new IllegalArgumentException("[infino] a scroll starts at the top; from must be 0");
        }
        if (source.collapse() != null) {
            throw new IllegalArgumentException("[infino] collapse is not served with a scroll");
        }
        if (source.searchAfter() != null) {
            throw new IllegalArgumentException("[infino] search_after is not served with a scroll; a scroll keeps its own place");
        }
        if (source.query() instanceof InfinoKnnQueryBuilder
            || source.query() instanceof InfinoHybridQueryBuilder
            || InfinoStockKnn.is(source.query())) {
            throw new IllegalArgumentException("[infino] a vector query is not served with a scroll");
        }
        SearchSourceBuilder pages = InfinoSearch.scrollSource(source);
        SearchSourceBuilder first = pages.shallowCopy();
        if (source.aggregations() != null) {
            for (AggregationBuilder b : source.aggregations().getAggregatorFactories()) {
                first.aggregation(b);
            }
            for (PipelineAggregationBuilder b : source.aggregations().getPipelineAggregatorFactories()) {
                first.aggregation(b);
            }
        }
        if (source.trackTotalHitsUpTo() != null) {
            first.trackTotalHitsUpTo(source.trackTotalHitsUpTo());
        } else {
            first.trackTotalHits(true);
        }
        TimeValue keepAlive = search.scroll() == null ? null : search.scroll().keepAlive();
        String id = InfinoScrolls.newId();
        Searched out = InfinoSearch.search(indices, tableOf, access::get, first, Map.of(), null, id, scripts, startNanos);
        InfinoScrolls.register(
            id,
            principal(),
            indices,
            pages,
            access,
            out.response().getHits().getTotalHits(),
            out.after(),
            keepAlive
        );
        return out.response();
    }

    /** The next page of a scroll, from each index's cursor. */
    private void page(SearchScrollRequest request, ActionListener<SearchResponse> listener) {
        long start = System.nanoTime();
        try {
            TimeValue keepAlive = request.scroll() == null ? null : request.scroll().keepAlive();
            Scroll scroll = InfinoScrolls.next(request.scrollId(), principal(), keepAlive);
            ClusterService clusters = clusterService.get();
            ClusterState state = clusters.state();
            InfinoTables tables = InfinoPlugin.tables();
            Map<Index, String> tableOf = new HashMap<>();
            for (Index index : scroll.indices) {
                IndexMetadata md = state.metadata().index(index);
                if (md == null) {
                    throw new IndexNotFoundException(index);
                }
                tableOf.put(index, tables.ensure(md));
            }
            TotalHits total = scroll.total;
            Searched out = InfinoSearch.search(scroll.indices, tableOf::get, scroll.access::get, scroll.source, scroll.after, total, scroll.id, scripts.get(), start);
            scroll.after = out.after();
            listener.onResponse(out.response());
        } catch (Exception e) {
            log.debug("[infino] scroll {} failed: {}", request.scrollId(), e.getMessage());
            listener.onFailure(e);
        }
    }
}
