/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.search.SearchAction;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.ShardSearchFailure;
import org.opensearch.action.support.ActionFilter;
import org.opensearch.action.support.ActionFilterChain;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.index.Index;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.script.ScriptService;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Serves {@code _search} (and so {@code _count} and each request of an
 * {@code _msearch}) for Infino-backed indices from their tables, before the
 * request is routed to any shard. A request over other indices proceeds as
 * usual; one mixing both is refused. A scroll request opens a scroll (see
 * {@link InfinoScrolls}) and answers its first page.
 */
public class InfinoSearchActionFilter implements ActionFilter {

    private static final Logger log = LogManager.getLogger(InfinoSearchActionFilter.class);
    /**
     * The shard whose table a default-mode index's alias filters are checked
     * against: every shard's table holds the index's mapped columns, and a
     * filter the engine cannot serve is refused whatever the shard.
     */
    private static final int ALIAS_CHECK_SHARD = 0;

    private final Supplier<ClusterService> clusterService;
    private final Supplier<IndexNameExpressionResolver> resolver;
    private final Supplier<ScriptService> scripts;

    public InfinoSearchActionFilter(Supplier<ClusterService> clusterService, Supplier<IndexNameExpressionResolver> resolver, Supplier<ScriptService> scripts) {
        this.clusterService = clusterService;
        this.resolver = resolver;
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
        if (!SearchAction.NAME.equals(action) || !(request instanceof SearchRequest search)) {
            chain.proceed(task, action, request, listener);
            return;
        }
        ClusterService clusters = clusterService.get();
        IndexNameExpressionResolver names = resolver.get();
        if (clusters == null || names == null) {
            chain.proceed(task, action, request, listener);
            return;
        }
        ClusterState state = clusters.state();
        String[] concrete;
        try {
            concrete = names.concreteIndexNames(state, search);
        } catch (Exception e) {
            chain.proceed(task, action, request, listener); // a missing index is reported the usual way
            return;
        }
        List<Index> infino = new ArrayList<>();
        List<IndexMetadata> metadata = new ArrayList<>();
        int engine = 0;
        for (String name : concrete) {
            IndexMetadata md = state.metadata().index(name);
            if (md == null) {
                continue;
            }
            if (InfinoPlugin.inProxyMode(md.getSettings())) {
                infino.add(md.getIndex());
                metadata.add(md);
            } else if (InfinoPlugin.inDefaultMode(md.getSettings())) {
                engine++;
            }
        }
        if ((engine > 0 || !infino.isEmpty()) && search.source() != null && search.source().slice() != null) {
            // The table answers a search whole, never a slice of it: each slice of a
            // sliced scroll (a sliced reindex or update by query) would get every document.
            listener.onFailure(new IllegalArgumentException("[infino] a sliced search is not served by the table; send it as one slice"));
            return;
        }
        if (engine > 0) {
            if (engine != concrete.length) {
                listener.onFailure(new IllegalArgumentException("[infino] a search mixing default-mode indices with others is not served"));
                return;
            }
            // Served by the shards, which plan from the source as written (see
            // InfinoQueryExt): the cluster routes it as it routes any search,
            // a scroll included, and merges the shards' results.
            SearchSourceBuilder written = search.source() == null ? new SearchSourceBuilder() : search.source();
            String id = InfinoQueryExt.newRequestId();
            long now = System.currentTimeMillis();
            // A filtering alias the search reaches an index through is checked
            // here, before any shard reads, as proxy mode checks it: one the
            // table cannot answer is refused naming the alias. Each shard reads
            // through the filter the cluster resolves for it.
            Map<String, QueryBuilder> filters = new HashMap<>();
            try {
                for (String name : concrete) {
                    IndexMetadata md = state.metadata().index(name);
                    if (md.getAliases().isEmpty()) {
                        continue; // no alias, so none that filters
                    }
                    String table = InfinoPlugin.tables().ensure(md, ALIAS_CHECK_SHARD);
                    filters.put(name, InfinoAliasFilter.of(state, names, md, search.indices(), table, now));
                }
            } catch (Exception e) {
                listener.onFailure(e);
                return;
            }
            // A cardinality or percentiles aggregation over more than one shard is
            // answered here, once, over every reached shard's table (see
            // InfinoExactAggregation): the search is recorded for it, with what the
            // user may see of each index, read now under the user's own identity,
            // and what an alias filter keeps of it.
            boolean exactAtCoordinator = reachedShardCount(clusters, names, state, search, concrete) > 1
                && written.aggregations() != null
                && InfinoAggregationProcessor.needsExactValues(written.aggregations().getAggregatorFactories());
            // The shards' hits carry the id their table breaks ties on after the
            // request's own sort values; the client is handed the request's only,
            // as stock hands them, and so is each page of a scroll it opens.
            int sortValues = InfinoSearch.requestedSortValues(written);
            TimeValue keepAlive = search.scroll() == null ? null : search.scroll().keepAlive();
            ActionListener<Response> answered = ActionListener.map(listener, response -> {
                if (response instanceof SearchResponse r) {
                    refuseIfAnyShardRefused(r);
                    InfinoSearch.handOut(r, sortValues);
                    if (search.scroll() != null) {
                        InfinoScrolls.handsOut(r.getScrollId(), sortValues, keepAlive);
                    }
                }
                return response;
            });
            if (exactAtCoordinator) {
                Map<String, InfinoAccess> access = new HashMap<>();
                for (String name : concrete) {
                    access.put(name, InfinoAccess.searching(name).filteredBy(filters.get(name)));
                }
                InfinoExactAggregation.record(id, written, now, access);
                // Its final reduce runs before the search answers, so the record
                // goes once it has: the records held are the searches in flight.
                answered = ActionListener.runAfter(answered, () -> InfinoExactAggregation.forget(id));
            }
            // The shard request cache keys on the routed request, its ext included. A
            // search it may keep is routed with a fixed id and no clock, so a repeat
            // matches the entry its first run left; the shard reads the clock from the
            // shard request, outside the key, and does not keep a search whose
            // translation reads it (InfinoQueryPhaseSearcher#beforeQueryPhase). Any
            // other search is kept out of the cache, where each run would only add an
            // entry no search reads.
            if (shardCacheable(search, written, concrete, filters, exactAtCoordinator)) {
                search.source(InfinoQueryExt.route(written, InfinoQueryExt.CACHED_REQUEST, false, 0L));
            } else {
                if (!Boolean.FALSE.equals(search.requestCache())) {
                    search.requestCache(false);
                }
                search.source(InfinoQueryExt.route(written, id, exactAtCoordinator, now));
            }
            chain.proceed(task, action, request, answered);
            return;
        }
        if (infino.isEmpty()) {
            chain.proceed(task, action, request, listener);
            return;
        }
        if (infino.size() != concrete.length) {
            listener.onFailure(new IllegalArgumentException("[infino] a search mixing Infino-backed indices with others is not served"));
            return;
        }
        long start = System.nanoTime();
        // The table is read on the search pool, never on the thread that took the request.
        InfinoServices.fork(ThreadPool.Names.SEARCH, listener::onFailure, () -> serve(search, state, names, infino, metadata, start, listener));
    }

    /**
     * Fails a default-mode search that some shard refused. A shard refuses
     * what its table cannot answer, and some refusals turn on what that
     * shard holds: an aggregation or sort over a field some of its rows
     * give several values, or over a field one index maps and another does
     * not. The search would otherwise answer, with no error, from the
     * shards that happened to serve it. Other shard failures are left as
     * stock leaves them: a partial result naming the failed shards.
     */
    static void refuseIfAnyShardRefused(SearchResponse response) {
        for (ShardSearchFailure failure : response.getShardFailures()) {
            Throwable refusal = refusal(failure.getCause());
            if (refusal instanceof RuntimeException e) {
                throw e;
            }
            if (refusal != null) {
                throw new IllegalArgumentException(refusal.getMessage(), refusal);
            }
        }
    }

    /**
     * The plugin's refusal ({@code [infino] ...}) among a shard failure's causes, or null when it
     * is none; one read back from another node may carry its type's name before it.
     */
    static Throwable refusal(Throwable cause) {
        for (Throwable t = cause; t != null && t.getCause() != t; t = t.getCause()) {
            if (t.getMessage() != null && t.getMessage().contains(REFUSAL_PREFIX)) {
                return t;
            }
        }
        return null;
    }

    /** How every refusal of the plugin's starts. */
    private static final String REFUSAL_PREFIX = "[infino] ";

    /**
     * Whether the shard request cache may keep a default-mode search's
     * shard results under a key a repeat of it shares: a search the cache
     * keeps for a Lucene index ({@code size: 0}, no scroll, not profiled,
     * not asked to skip it), with no hits to fetch, whose answer depends on
     * nothing but the shard's rows. The routed copy's id ties a shard's hits
     * to its fetch phase and a cardinality or percentiles aggregation over
     * several shards to the search recorded for it. So none of these may be
     * in play: an exact aggregation answered here, a {@code now} this check
     * sees (the shard catches the rest), a script or a random score, a
     * filtering alias, or a rule of the security plugin narrowing what the
     * user sees, which the shard's key does not hold.
     */
    private static boolean shardCacheable(
        SearchRequest search,
        SearchSourceBuilder source,
        String[] concrete,
        Map<String, QueryBuilder> filters,
        boolean exactAtCoordinator
    ) {
        if (exactAtCoordinator || Boolean.FALSE.equals(search.requestCache()) || search.scroll() != null) {
            return false;
        }
        if (source.size() != 0 || source.profile()) {
            return false;
        }
        for (QueryBuilder filter : filters.values()) {
            if (filter != null) {
                return false;
            }
        }
        try {
            for (String name : concrete) {
                if (!InfinoAccess.searching(name).unrestricted()) {
                    return false;
                }
            }
            Map<String, Object> body = XContentHelper.convertToMap(new BytesArray(source.toString()), false, MediaTypeRegistry.JSON).v2();
            return !InfinoRequestCache.changesWithoutWrites(body);
        } catch (Exception e) {
            // An access the security plugin refuses is refused by the shards; it is not kept.
            return false;
        }
    }

    /** Answer a search over proxy-mode indices from their tables. */
    private <Response extends ActionResponse> void serve(
        SearchRequest search,
        ClusterState state,
        IndexNameExpressionResolver names,
        List<Index> infino,
        List<IndexMetadata> metadata,
        long start,
        ActionListener<Response> listener
    ) {
        try {
            SearchSourceBuilder source = search.source() == null ? new SearchSourceBuilder() : search.source();
            InfinoTables tables = InfinoPlugin.tables();
            Function<Index, String> tableOf = index -> tables.ensure(metadata.get(infino.indexOf(index)));
            // Each index is read under what the security plugin lets this user see
            // of it, and what a filtering alias the request reached it through keeps.
            long now = System.currentTimeMillis();
            Map<Index, InfinoAccess> access = new HashMap<>();
            List<InfinoAccess> rules = new ArrayList<>();
            List<QueryBuilder> filters = new ArrayList<>();
            for (int i = 0; i < infino.size(); i++) {
                Index index = infino.get(i);
                InfinoAccess own = InfinoAccess.searching(index.getName());
                QueryBuilder filter = InfinoAliasFilter.of(state, names, metadata.get(i), search.indices(), tableOf.apply(index), now);
                rules.add(own);
                filters.add(filter);
                access.put(index, own.filteredBy(filter));
            }
            // A repeated request is answered from the responses kept for it,
            // as the shard request cache answers it for a Lucene index.
            InfinoRequestCache kept = InfinoRequestCache.node();
            String key = null;
            if (kept != null) {
                List<String> tableNames = new ArrayList<>();
                for (Index index : infino) {
                    tableNames.add(tableOf.apply(index));
                }
                key = InfinoRequestCache.key(search, source, metadata, tableNames, rules, filters);
                SearchResponse hit = key == null ? null : kept.get(key, start);
                if (hit != null) {
                    @SuppressWarnings("unchecked")
                    Response cast = (Response) hit;
                    listener.onResponse(cast);
                    return;
                }
            }
            SearchResponse response = search.scroll() == null
                ? InfinoSearch.search(infino, tableOf, access::get, source, scripts.get(), start)
                : InfinoScrollActionFilter.open(search, infino, tableOf, access, source, scripts.get(), start);
            // The hits carry the id the table breaks ties on after the request's own
            // sort values; the client is handed the request's only, as stock hands them.
            InfinoSearch.handOut(response, InfinoSearch.requestedSortValues(source));
            if (key != null) {
                kept.put(key, response, InfinoRequestCache.lifetime(metadata));
            }
            @SuppressWarnings("unchecked")
            Response cast = (Response) response;
            listener.onResponse(cast);
        } catch (Exception e) {
            log.debug("[infino] search over {} failed: {}", infino, e.getMessage());
            listener.onFailure(e);
        }
    }

    /**
     * How many shards the search reaches, over every index it names, as the
     * cluster will route it (its routing values and shard preference applied).
     */
    private static int reachedShardCount(ClusterService clusters, IndexNameExpressionResolver names, ClusterState state, SearchRequest search, String[] concrete) {
        Map<String, Set<String>> routing = names.resolveSearchRouting(state, search.routing(), search.indices());
        return clusters.operationRouting().searchShards(state, concrete, routing, search.preference()).size();
    }
}
