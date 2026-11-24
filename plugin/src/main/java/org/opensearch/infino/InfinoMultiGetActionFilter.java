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
import org.opensearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

/**
 * Serves {@code _mget} for Infino-backed indices from their tables: the ids of
 * each index are looked up through the id column's term index in one
 * statement, and each item comes back as a get response. An index may be
 * named through an alias; an item whose name resolves to no single index
 * fails alone, as it does on the shards. A request that also names an index
 * the plugin does not back goes on to the shards as it is. Each document
 * carries the sequence number it was written under, as for a single get.
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
        Layout layout = layout(clusters.state(), resolver.get(), mget.getItems());
        if (layout == null) {
            chain.proceed(task, action, request, listener);
            return;
        }
        // The tables are read on the get pool, never on the thread that took the request.
        InfinoServices.fork(ThreadPool.Names.GET, listener::onFailure, () -> serve(mget, layout, listener));
    }

    /**
     * How the tables answer a multi-get: the Infino-backed index each served
     * name stands for, and by slot the failure of each item whose name
     * resolves to no single index (null for the others).
     */
    record Layout(Map<String, IndexMetadata> indices, Exception[] failed) {}

    /**
     * The layout of a multi-get's items, or null when it goes on to the
     * shards as it is: an item names an index the plugin does not back, or
     * no item names one it does. An item whose name resolves to no single
     * index, such as an alias of several or a missing index, fails alone
     * with what the resolution threw, as the stock multi-get fails it before
     * it reaches any shard; the other items are served.
     */
    static Layout layout(ClusterState state, IndexNameExpressionResolver names, List<MultiGetRequest.Item> items) {
        Map<String, IndexMetadata> indices = new HashMap<>();
        Exception[] failed = new Exception[items.size()];
        for (int i = 0; i < items.size(); i++) {
            MultiGetRequest.Item item = items.get(i);
            if (names != null) {
                try {
                    names.concreteSingleIndex(state, item);
                } catch (Exception e) {
                    failed[i] = e;
                    continue;
                }
            }
            IndexMetadata index = indices.computeIfAbsent(item.index(), name -> InfinoGetActionFilter.infinoIndex(state, names, name));
            if (index == null) {
                return null;
            }
        }
        return indices.isEmpty() ? null : new Layout(indices, failed);
    }

    /** Answer a multi-get from the tables as {@code layout} has it. */
    private <Response extends ActionResponse> void serve(MultiGetRequest mget, Layout layout, ActionListener<Response> listener) {
        Map<String, IndexMetadata> indices = layout.indices();
        Exception[] failed = layout.failed();
        try {
            // One fetch per table, over all of its ids: through the document
            // rule when the security plugin has one for this user. An index in
            // default mode with several shards has a table per shard, so each
            // id goes to its shard's; a realtime read of a default-mode table
            // counts a write its shard has not committed yet, and reads the rest
            // in one call, from the table's latest commit when this node may be
            // behind it. A read that is not realtime is read item by item.
            Map<String, InfinoAccess> accessOf = new HashMap<>();
            for (Map.Entry<String, IndexMetadata> e : indices.entrySet()) {
                accessOf.put(e.getKey(), InfinoAccess.reading(e.getValue().getIndex().getName(), "mget"));
            }
            String[] tableOfItem = new String[mget.getItems().size()];
            Map<String, List<String>> idsByTable = new HashMap<>();
            Map<String, String> indexOfTable = new HashMap<>();
            Set<String> refreshed = new HashSet<>();
            for (int i = 0; i < tableOfItem.length; i++) {
                if (failed[i] != null) {
                    continue;
                }
                MultiGetRequest.Item item = mget.getItems().get(i);
                String table = tables.ensureForDocument(indices.get(item.index()), item.id(), item.routing());
                tableOfItem[i] = table;
                if (mget.refresh() && refreshed.add(table)) {
                    InfinoGetActionFilter.refresh(indices.get(item.index()), table);
                }
                idsByTable.computeIfAbsent(table, k -> new ArrayList<>()).add(item.id());
                indexOfTable.put(table, item.index());
            }
            Map<String, Map<String, InfinoNative.Doc>> docsOf = new HashMap<>();
            for (Map.Entry<String, List<String>> e : idsByTable.entrySet()) {
                String index = indexOfTable.get(e.getKey());
                IndexMetadata md = indices.get(index);
                InfinoAccess access = accessOf.get(index);
                if (access.restrictsDocuments()) {
                    docsOf.put(e.getKey(), InfinoGetActionFilter.restrictedDocs(e.getKey(), e.getValue(), access));
                } else if (!InfinoPlugin.inDefaultMode(md.getSettings())) {
                    docsOf.put(e.getKey(), InfinoNative.docs(e.getKey(), e.getValue(), null));
                } else if (mget.realtime()) {
                    docsOf.put(e.getKey(), InfinoEngine.currentDocs(e.getKey(), e.getValue()));
                }
            }
            // The ignored fields of each table's ids, read once per table,
            // except where a realtime default-mode read sees pending writes.
            Map<String, Map<String, List<String>>> ignoredOf = new HashMap<>();
            for (Map.Entry<String, List<String>> e : idsByTable.entrySet()) {
                IndexMetadata md = indices.get(indexOfTable.get(e.getKey()));
                if (!(mget.realtime() && InfinoPlugin.inDefaultMode(md.getSettings()))) {
                    ignoredOf.put(e.getKey(), InfinoNative.ignoredOf(e.getKey(), e.getValue()));
                }
            }
            // What each index's mapping keeps of a document's _source, read once per index.
            Map<String, InfinoMappingSource> storedOf = new HashMap<>();
            MultiGetItemResponse[] responses = new MultiGetItemResponse[mget.getItems().size()];
            for (int i = 0; i < responses.length; i++) {
                MultiGetRequest.Item item = mget.getItems().get(i);
                if (failed[i] != null) {
                    responses[i] = new MultiGetItemResponse(null, new MultiGetResponse.Failure(item.index(), item.id(), failed[i]));
                    continue;
                }
                IndexMetadata md = indices.get(item.index());
                Map<String, InfinoNative.Doc> byId = docsOf.get(tableOfItem[i]);
                InfinoNative.Doc doc = byId != null ? byId.get(item.id()) : InfinoGetActionFilter.docOf(md, tableOfItem[i], item.id(), mget.realtime());
                String routing = InfinoGetActionFilter.routingOf(md, doc);
                GetResponse get = new GetResponse(
                    InfinoGetActionFilter.result(
                        md.getIndex().getName(),
                        item.id(),
                        doc,
                        accessOf.get(item.index()),
                        storedOf.computeIfAbsent(item.index(), k -> InfinoMappingSource.of(md.mapping())),
                        item.fetchSourceContext(),
                        routing,
                        doc == null ? List.of()
                            : ignoredOf.containsKey(tableOfItem[i]) ? ignoredOf.get(tableOfItem[i]).getOrDefault(item.id(), List.of())
                            : InfinoGetActionFilter.ignoredOf(md, tableOfItem[i], item.id(), doc, true)
                    )
                );
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
