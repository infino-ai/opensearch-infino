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
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

/**
 * Serves {@code _mget} for Infino-backed indices from their tables: the ids of
 * each index are looked up through the id column's term index in one
 * statement, and each item comes back as a get response. An index may be
 * named through an alias; an item whose name resolves to no single index
 * fails alone, as it does on the shards. A request that also names an index
 * the plugin does not back goes on to the shards as it is. Each document
 * carries the version, sequence number and primary term a single get
 * reports, and an item that asks for another version fails alone; the items
 * of a default-mode shard whose primary is on another node are sent on to
 * that node, as one multi-get per node.
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
        ClusterState state = clusters.state();
        // The tables are read on the get pool, never on the thread that took the request.
        InfinoServices.fork(ThreadPool.Names.GET, listener::onFailure, () -> serve(mget, state, layout, listener));
    }

    /**
     * How the tables answer a multi-get: the Infino-backed index each served
     * name stands for, and by slot the failure of each item whose name
     * resolves to no single index or that asks for a stored field (null for
     * the others).
     */
    record Layout(Map<String, IndexMetadata> indices, Exception[] failed) {}

    /**
     * The layout of a multi-get's items, or null when it goes on to the
     * shards as it is: an item names an index the plugin does not back, or
     * no item names one it does. An item whose name resolves to no single
     * index, such as an alias of several or a missing index, fails alone
     * with what the resolution threw, as the stock multi-get fails it before
     * it reaches any shard; the other items are served. So does an item asking
     * for a field its index stores apart from the source, which the table does
     * not keep.
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
            String stored = InfinoGetActionFilter.storedFieldAsked(index, item.storedFields());
            if (stored != null) {
                failed[i] = InfinoSearchChecks.storedFieldRefused(stored, index.getIndex().getName());
            }
        }
        return indices.isEmpty() ? null : new Layout(indices, failed);
    }

    /**
     * Answer a multi-get from the tables as {@code layout} has it: here, but
     * for the items of default-mode shards whose primary is on another node,
     * which go there as one multi-get per node, each item's answer filling
     * its slot.
     */
    private <Response extends ActionResponse> void serve(MultiGetRequest mget, ClusterState state, Layout layout, ActionListener<Response> listener) {
        MultiGetItemResponse[] responses = new MultiGetItemResponse[mget.getItems().size()];
        Map<DiscoveryNode, List<Integer>> elsewhere = new LinkedHashMap<>();
        boolean[] here = new boolean[responses.length];
        try {
            for (int i = 0; i < responses.length; i++) {
                MultiGetRequest.Item item = mget.getItems().get(i);
                Exception failed = layout.failed()[i];
                IndexMetadata md = failed == null ? layout.indices().get(item.index()) : null;
                if (md != null && InfinoPlugin.inDefaultMode(md.getSettings())) {
                    InfinoGetActionFilter.Route route = InfinoGetActionFilter.route(
                        state,
                        InfinoGetActionFilter.shardOf(md, item.id(), item.routing())
                    );
                    failed = route.failure();
                    if (route.node() != null) {
                        elsewhere.computeIfAbsent(route.node(), k -> new ArrayList<>()).add(i);
                        continue;
                    }
                }
                if (failed != null) {
                    responses[i] = new MultiGetItemResponse(null, new MultiGetResponse.Failure(item.index(), item.id(), failed));
                    continue;
                }
                here[i] = true;
            }
            serveHere(mget, layout, here, responses);
        } catch (Exception e) {
            log.warn("[infino] mget of {} items failed", mget.getItems().size(), e);
            listener.onFailure(e);
            return;
        }
        @SuppressWarnings("unchecked")
        ActionListener<MultiGetResponse> answer = (ActionListener<MultiGetResponse>) listener;
        gather(mget, responses, elsewhere, (node, part, partListener) -> InfinoGetActionFilter.forward(node, MultiGetAction.INSTANCE, part, partListener), answer);
    }

    /** How the items a node holds the primaries of are sent to it, as one multi-get. */
    interface Sender {
        void send(DiscoveryNode node, MultiGetRequest part, ActionListener<MultiGetResponse> listener);
    }

    /**
     * Answer {@code mget} once every node in {@code elsewhere} has answered
     * its items, sent with {@code sender} as one multi-get per node: each
     * item's answer fills its slot of {@code responses}, which holds the
     * items answered here already. A node that fails fails its own items
     * alone, each with that failure, as a stock multi-get fails the items of
     * a shard it cannot reach.
     */
    static void gather(
        MultiGetRequest mget,
        MultiGetItemResponse[] responses,
        Map<DiscoveryNode, List<Integer>> elsewhere,
        Sender sender,
        ActionListener<MultiGetResponse> answer
    ) {
        if (elsewhere.isEmpty()) {
            answer.onResponse(new MultiGetResponse(responses));
            return;
        }
        AtomicInteger parts = new AtomicInteger(elsewhere.size());
        for (Map.Entry<DiscoveryNode, List<Integer>> e : elsewhere.entrySet()) {
            List<Integer> slots = e.getValue();
            MultiGetRequest part = new MultiGetRequest().realtime(mget.realtime()).refresh(mget.refresh()).preference(mget.preference());
            for (int slot : slots) {
                part.add(mget.getItems().get(slot));
            }
            sender.send(e.getKey(), part, ActionListener.wrap(got -> {
                for (int k = 0; k < slots.size(); k++) {
                    responses[slots.get(k)] = got.getResponses()[k];
                }
                if (parts.decrementAndGet() == 0) {
                    answer.onResponse(new MultiGetResponse(responses));
                }
            }, failure -> {
                for (int slot : slots) {
                    MultiGetRequest.Item item = mget.getItems().get(slot);
                    responses[slot] = new MultiGetItemResponse(null, new MultiGetResponse.Failure(item.index(), item.id(), failure));
                }
                if (parts.decrementAndGet() == 0) {
                    answer.onResponse(new MultiGetResponse(responses));
                }
            }));
        }
    }

    /** Answer the items {@code here} marks into {@code responses}, from the tables as {@code layout} has them. */
    private void serveHere(MultiGetRequest mget, Layout layout, boolean[] here, MultiGetItemResponse[] responses) {
        Map<String, IndexMetadata> indices = layout.indices();
        // One fetch per table, over all of its ids: through the document
        // rule when the security plugin has one for this user. An index in
        // default mode with several shards has a table per shard, so each
        // id goes to its shard's, and its rows are read with the shard's own
        // get of each, as for a single get (InfinoGetActionFilter#reads): a
        // realtime read counts a write its shard has not committed yet.
        Map<String, InfinoAccess> accessOf = new HashMap<>();
        for (Map.Entry<String, IndexMetadata> e : indices.entrySet()) {
            accessOf.put(e.getKey(), InfinoAccess.reading(e.getValue().getIndex().getName(), "mget"));
        }
        String[] tableOfItem = new String[mget.getItems().size()];
        Map<String, List<String>> idsByTable = new HashMap<>();
        Map<String, String> indexOfTable = new HashMap<>();
        Set<String> defaultTables = new HashSet<>();
        Set<String> refreshed = new HashSet<>();
        for (int i = 0; i < tableOfItem.length; i++) {
            if (!here[i]) {
                continue;
            }
            MultiGetRequest.Item item = mget.getItems().get(i);
            IndexMetadata md = indices.get(item.index());
            String table = tables.ensureForDocument(md, item.id(), item.routing());
            tableOfItem[i] = table;
            if (mget.refresh() && refreshed.add(table)) {
                InfinoGetActionFilter.refresh(md, table);
            }
            idsByTable.computeIfAbsent(table, k -> new ArrayList<>()).add(item.id());
            indexOfTable.put(table, item.index());
            if (InfinoPlugin.inDefaultMode(md.getSettings())) {
                defaultTables.add(table);
            }
        }
        Map<String, Map<String, InfinoNative.Doc>> docsOf = new HashMap<>();
        Map<String, Map<String, InfinoGetActionFilter.Read>> readsOf = new HashMap<>();
        // A default-mode table that cannot be read here (its engine is not open)
        // fails its own items alone, with the reason, as a shard does.
        Map<String, Exception> tableFailed = new HashMap<>();
        for (Map.Entry<String, List<String>> e : idsByTable.entrySet()) {
            String table = e.getKey();
            InfinoAccess access = accessOf.get(indexOfTable.get(table));
            if (defaultTables.contains(table)) {
                try {
                    readsOf.put(
                        table,
                        InfinoGetActionFilter.reads(
                            InfinoEngine.shardReads(table),
                            new LinkedHashSet<>(e.getValue()),
                            mget.realtime(),
                            InfinoGetActionFilter.docReader(table, access)
                        )
                    );
                } catch (InfinoNative.InfinoException failure) {
                    tableFailed.put(table, failure);
                }
            } else {
                docsOf.put(table, InfinoGetActionFilter.proxyDocs(indices.get(indexOfTable.get(table)), table, e.getValue(), access));
            }
        }
        // The ignored fields of each proxy-mode table's ids, read once per
        // table; a default-mode read sees a pending write's (see reads).
        Map<String, Map<String, List<String>>> ignoredOf = new HashMap<>();
        for (Map.Entry<String, List<String>> e : idsByTable.entrySet()) {
            if (!defaultTables.contains(e.getKey())) {
                ignoredOf.put(e.getKey(), InfinoNative.ignoredOf(e.getKey(), e.getValue()));
            }
        }
        // What each index's mapping keeps of a document's _source, read once per index.
        Map<String, InfinoMappingSource> storedOf = new HashMap<>();
        for (int i = 0; i < responses.length; i++) {
            if (!here[i]) {
                continue;
            }
            MultiGetRequest.Item item = mget.getItems().get(i);
            IndexMetadata md = indices.get(item.index());
            InfinoGetActionFilter.Read read = readsOf.containsKey(tableOfItem[i]) ? readsOf.get(tableOfItem[i]).get(item.id()) : null;
            Exception failed = tableFailed.get(tableOfItem[i]);
            if (failed == null && read != null) {
                failed = read.failure();
            }
            InfinoNative.Doc doc = failed != null ? null : read != null ? read.doc() : docsOf.get(tableOfItem[i]).get(item.id());
            InfinoEngine.WrittenNumbers written = read != null ? read.written() : null;
            // A found document is at the version its shard reports in default
            // mode, and at the one its writes report for every document in proxy mode.
            if (failed == null && doc != null) {
                failed = InfinoGetActionFilter.versionConflict(
                    written != null ? InfinoGetActionFilter.shardOf(md, item.id(), item.routing()) : new ShardId(md.getIndex(), 0),
                    item.id(),
                    item.versionType(),
                    item.version(),
                    written != null ? written.version() : InfinoDocumentVersion.VERSION
                );
            }
            if (failed != null) {
                responses[i] = new MultiGetItemResponse(null, new MultiGetResponse.Failure(item.index(), item.id(), failed));
                continue;
            }
            String routing = InfinoGetActionFilter.routingOf(md, doc);
            GetResponse get = new GetResponse(
                InfinoGetActionFilter.result(
                    md.getIndex().getName(),
                    item.id(),
                    doc,
                    written,
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
    }
}
