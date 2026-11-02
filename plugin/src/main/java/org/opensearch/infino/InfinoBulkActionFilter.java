/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.DocWriteRequest;
import org.opensearch.action.DocWriteResponse;
import org.opensearch.action.bulk.BulkAction;
import org.opensearch.action.bulk.BulkItemResponse;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.delete.DeleteRequest;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.support.ActionFilter;
import org.opensearch.action.support.ActionFilterChain;
import org.opensearch.action.support.AutoCreateIndex;
import org.opensearch.action.update.UpdateRequest;
import org.opensearch.action.update.UpdateResponse;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.routing.OperationRouting;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.UUIDs;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.MediaType;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.index.engine.DocumentMissingException;
import org.opensearch.infino.InfinoWriter.Outcome;
import org.opensearch.infino.InfinoWriter.Waiter;
import org.opensearch.infino.InfinoWriter.Write;
import org.opensearch.tasks.Task;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

/**
 * Serves {@code _bulk} (and so single index, delete and update requests,
 * which OpenSearch runs as one-item bulks) for Infino-backed indices from
 * their tables, before the request is routed to any shard. Nothing reaches
 * Lucene: the index exists in cluster state as a name, a mapping and its
 * settings, and every row lives in the table.
 *
 * <p>An index a write names before it exists is created first, as a stock
 * bulk would create it and under the cluster's rule for that, so the write
 * lands in the table the new index is backed by rather than in the shard
 * the stock path would have created it for. A bulk over several
 * Infino-backed indices is served per index, the items answered in the
 * bulk's own order.
 *
 * <p>An update is merged here into the document it targets. The writes then
 * go to the table's {@link InfinoWriter}, which batches them across requests
 * and answers the bulk once they are committed; a document the declared
 * columns cannot hold comes back from that append as its own item with the
 * reason, as a mapping parse failure would be.
 *
 * <p>A bulk mixing Infino-backed and other indices is refused as a whole.
 */
public class InfinoBulkActionFilter implements ActionFilter {

    private static final Logger log = LogManager.getLogger(InfinoBulkActionFilter.class);

    private final Supplier<ClusterService> clusterService;
    private final InfinoTables tables;
    private final Supplier<InfinoWriter> writer;

    public InfinoBulkActionFilter(Supplier<ClusterService> clusterService, InfinoTables tables, Supplier<InfinoWriter> writer) {
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
        if (!BulkAction.NAME.equals(action) || !(request instanceof BulkRequest bulk)) {
            chain.proceed(task, action, request, listener);
            return;
        }
        ClusterService clusters = clusterService.get();
        InfinoWriter writes = writer.get();
        if (clusters == null || writes == null) {
            chain.proceed(task, action, request, listener);
            return;
        }
        @SuppressWarnings("unchecked")
        ActionListener<BulkResponse> cast = (ActionListener<BulkResponse>) listener;
        createMissing(missingIndices(clusters, bulk), ActionListener.wrap(created -> {
            Map<String, IndexMetadata> infino;
            List<String> concrete = concreteIndices(clusters.state(), bulk);
            try {
                infino = infinoIndices(clusters, concrete);
            } catch (Exception e) {
                listener.onFailure(e);
                return;
            }
            if (infino.isEmpty()) {
                EngineBulk routed = engineUpdates(clusters.state(), bulk, concrete, tables);
                if (routed == null) {
                    chain.proceed(task, action, request, listener);
                    return;
                }
                @SuppressWarnings("unchecked")
                Request next = (Request) routed.bulk();
                @SuppressWarnings("unchecked")
                ActionListener<Response> answer = (ActionListener<Response>) routed.answer(cast);
                chain.proceed(task, action, next, answer);
                return;
            }
            serve(bulk, concrete, infino, writes, tables, cast);
        }, listener::onFailure));
    }

    /**
     * A bulk over default-mode indices with its updates turned into writes of
     * the whole document: {@code bulk} goes to the shards, and
     * {@code answer} turns its response back into the one the client's bulk
     * expects. {@code slots} is each item's slot in the client's bulk,
     * {@code updates} the slots that were updates, {@code answered} the items
     * answered before the shards (in slot order).
     */
    record EngineBulk(BulkRequest bulk, int[] slots, Set<Integer> updates, Map<Integer, BulkItemResponse> answered, int size) {
        ActionListener<BulkResponse> answer(ActionListener<BulkResponse> listener) {
            return ActionListener.wrap(response -> {
                BulkItemResponse[] items = new BulkItemResponse[size];
                for (Map.Entry<Integer, BulkItemResponse> e : answered.entrySet()) {
                    items[e.getKey()] = e.getValue();
                }
                BulkItemResponse[] routed = response.getItems();
                for (int i = 0; i < routed.length; i++) {
                    int slot = slots[i];
                    items[slot] = updates.contains(slot) ? asUpdate(routed[i], slot) : reslot(routed[i], slot);
                }
                listener.onResponse(new BulkResponse(items, response.getTook().millis(), response.getIngestTookInMillis()));
            }, listener::onFailure);
        }
    }

    /**
     * The bulk to send to the shards of default-mode indices when it carries
     * updates, or null when it carries none (it goes on as it is). The stock
     * update reads the current document from Lucene, which keeps no source
     * in default mode, so each update is merged here into the document as a
     * read sees it now (the shard's write not yet committed, else the
     * table), by proxy mode's rule ({@link #merge}), and goes on as a
     * write of the whole document, carrying any sequence-number condition
     * for the shard to check. An update the table cannot serve (a script) or
     * whose document is missing is answered here.
     */
    static EngineBulk engineUpdates(ClusterState state, BulkRequest bulk, List<String> concrete, InfinoTables tables) {
        List<DocWriteRequest<?>> items = bulk.requests();
        Set<Integer> updates = new HashSet<>();
        Map<Integer, BulkItemResponse> answered = new LinkedHashMap<>();
        Map<Integer, DocWriteRequest<?>> writes = new HashMap<>();
        for (int slot = 0; slot < items.size(); slot++) {
            IndexMetadata md = concrete.get(slot) == null ? null : state.metadata().index(concrete.get(slot));
            if (!(items.get(slot) instanceof UpdateRequest update) || md == null || !InfinoPlugin.inDefaultMode(md.getSettings())) {
                continue;
            }
            updates.add(slot);
            String index = md.getIndex().getName();
            if (update.script() != null) {
                answered.put(slot, failure(slot, update, index, new IllegalArgumentException("[infino] scripted updates are not served"), RestStatus.BAD_REQUEST));
                continue;
            }
            String table = tables.ensureForDocument(md, update.id(), update.routing());
            String merged = merge(InfinoEngine.currentSource(table, update.id()), update);
            if (merged == null) {
                ShardId shardId = new ShardId(md.getIndex(), OperationRouting.generateShardId(md, update.id(), update.routing()));
                answered.put(slot, failure(slot, update, index, new DocumentMissingException(shardId, update.id()), RestStatus.NOT_FOUND));
                continue;
            }
            IndexRequest write = new IndexRequest(index).id(update.id()).routing(update.routing()).source(merged, MediaTypeRegistry.JSON);
            write.setIfSeqNo(update.ifSeqNo()).setIfPrimaryTerm(update.ifPrimaryTerm());
            writes.put(slot, write);
        }
        if (updates.isEmpty()) {
            return null;
        }
        BulkRequest routed = new BulkRequest().setRefreshPolicy(bulk.getRefreshPolicy()).timeout(bulk.timeout()).waitForActiveShards(bulk.waitForActiveShards());
        int[] slots = new int[items.size() - answered.size()];
        int at = 0;
        for (int slot = 0; slot < items.size(); slot++) {
            if (answered.containsKey(slot)) {
                continue;
            }
            routed.add(writes.getOrDefault(slot, items.get(slot)));
            slots[at++] = slot;
        }
        return new EngineBulk(routed, slots, updates, answered, items.size());
    }

    /** A routed write's response as the update it was in the client's bulk. */
    private static BulkItemResponse asUpdate(BulkItemResponse r, int slot) {
        if (r.isFailed()) {
            return new BulkItemResponse(slot, DocWriteRequest.OpType.UPDATE, r.getFailure());
        }
        DocWriteResponse w = r.getResponse();
        UpdateResponse update = new UpdateResponse(w.getShardInfo(), w.getShardId(), w.getId(), w.getSeqNo(), w.getPrimaryTerm(), w.getVersion(), w.getResult());
        return new BulkItemResponse(slot, DocWriteRequest.OpType.UPDATE, update);
    }

    /**
     * The concrete index each item writes to, by slot: the name it gives, or
     * for an alias the alias's write index, as the stock path resolves it.
     * An alias that resolves to no single write index keeps its name, so the
     * stock path reports it.
     */
    static List<String> concreteIndices(ClusterState state, BulkRequest bulk) {
        IndexNameExpressionResolver resolver = InfinoServices.indexNameExpressionResolver();
        List<String> out = new ArrayList<>(bulk.requests().size());
        for (DocWriteRequest<?> item : bulk.requests()) {
            String name = item.index();
            if (resolver != null && name != null && state.metadata().hasAlias(name)) {
                try {
                    name = resolver.concreteWriteIndex(state, item).getName();
                } catch (Exception e) {
                    log.debug("[infino] alias {} resolves to no write index: {}", name, e.getMessage());
                }
            }
            out.add(name);
        }
        return out;
    }

    /**
     * The indices the bulk's items name that do not exist and the cluster
     * would create for a stock bulk, in order; empty when none. When the
     * cluster's rule forbids creating one, nothing is listed: the stock path
     * reports the missing index.
     */
    static List<String> missingIndices(ClusterService clusters, BulkRequest bulk) {
        ClusterState state = clusters.state();
        AutoCreateIndex rule = InfinoServices.autoCreateIndex();
        Set<String> missing = new LinkedHashSet<>();
        for (DocWriteRequest<?> item : bulk.requests()) {
            String name = item.index();
            if (name == null || state.metadata().getIndicesLookup().containsKey(name) || missing.contains(name)) {
                continue;
            }
            try {
                if (rule != null && rule.shouldAutoCreate(name, state)) {
                    missing.add(name);
                }
            } catch (IndexNotFoundException e) {
                // The rule forbids creating it; the stock path answers the item.
            }
        }
        return new ArrayList<>(missing);
    }

    /** Create the indices named, one after another, then tell {@code then}. */
    static void createMissing(List<String> names, ActionListener<Void> then) {
        if (names.isEmpty()) {
            then.onResponse(null);
            return;
        }
        String first = names.get(0);
        List<String> rest = names.subList(1, names.size());
        InfinoClient.createIndex(first, ActionListener.wrap(created -> createMissing(rest, then), then::onFailure));
    }

    /**
     * The Infino-backed indices a bulk's items write to (the concrete names
     * {@link #concreteIndices} gave, one per slot), by name; empty when none
     * is. A bulk that mixes them with other indices is refused, and so is a
     * write to an index the security plugin restricts for this user by
     * document, field or mask: a write reads the document it replaces or
     * merges into and puts fields the rule hides, so under a rule it is
     * refused whole rather than served outside the rule.
     */
    static Map<String, IndexMetadata> infinoIndices(ClusterService clusters, List<String> concrete) {
        Map<String, IndexMetadata> infino = new LinkedHashMap<>();
        int infinoItems = 0;
        for (String name : concrete) {
            IndexMetadata md = name == null ? null : clusters.state().metadata().index(name);
            // An index in default mode takes its writes through its shard's engine, as any index does.
            if (md != null && InfinoPlugin.inProxyMode(md.getSettings())) {
                infino.put(name, md);
                infinoItems++;
            }
        }
        if (infinoItems == 0) {
            return Map.of();
        }
        if (infinoItems != concrete.size()) {
            throw new IllegalArgumentException("[infino] a bulk mixing Infino-backed indices with others is not served");
        }
        for (String name : infino.keySet()) {
            if (!InfinoSecurity.access(name, "write").unrestricted()) {
                throw InfinoAccess.refused("a write to " + name + " under a document, field or masking rule is not served");
            }
        }
        return infino;
    }

    /**
     * Serve a bulk over Infino-backed indices: stage the items of each index
     * and hand them to its table's writer. With several indices, each index's
     * items go to their own waiter and the bulk is answered when every waiter
     * has, each item in the bulk's own slot.
     */
    static void serve(
        BulkRequest bulk,
        List<String> concrete,
        Map<String, IndexMetadata> infino,
        InfinoWriter writes,
        InfinoTables tables,
        ActionListener<BulkResponse> listener
    ) {
        long start = System.nanoTime();
        try {
            BulkItemResponse[] items = new BulkItemResponse[bulk.requests().size()];
            if (infino.size() == 1) {
                IndexMetadata md = infino.values().iterator().next();
                String table = tables.ensure(md);
                ShardId shardId = new ShardId(md.getIndex(), 0);
                Waiter waiter = new Waiter(shardId, md.getIndex().getName(), stage(table, shardId, bulk.requests(), writes), items, start, listener);
                writes.submit(table, waiter);
                return;
            }
            Map<String, List<Integer>> slotsByIndex = new LinkedHashMap<>();
            for (int slot = 0; slot < bulk.requests().size(); slot++) {
                slotsByIndex.computeIfAbsent(concrete.get(slot), k -> new ArrayList<>()).add(slot);
            }
            AtomicInteger pending = new AtomicInteger(slotsByIndex.size());
            AtomicReference<Exception> failure = new AtomicReference<>();
            for (Map.Entry<String, List<Integer>> group : slotsByIndex.entrySet()) {
                IndexMetadata md = infino.get(group.getKey());
                List<Integer> slots = group.getValue();
                List<DocWriteRequest<?>> requests = new ArrayList<>(slots.size());
                for (int slot : slots) {
                    requests.add(bulk.requests().get(slot));
                }
                BulkItemResponse[] part = new BulkItemResponse[slots.size()];
                ActionListener<BulkResponse> partListener = ActionListener.wrap(response -> {
                    for (int i = 0; i < slots.size(); i++) {
                        items[slots.get(i)] = reslot(response.getItems()[i], slots.get(i));
                    }
                    if (pending.decrementAndGet() == 0) {
                        finish(items, failure.get(), start, listener);
                    }
                }, e -> {
                    failure.compareAndSet(null, e);
                    if (pending.decrementAndGet() == 0) {
                        finish(items, failure.get(), start, listener);
                    }
                });
                String table = tables.ensure(md);
                ShardId shardId = new ShardId(md.getIndex(), 0);
                Waiter waiter = new Waiter(shardId, md.getIndex().getName(), stage(table, shardId, requests, writes), part, start, partListener);
                writes.submit(table, waiter);
            }
        } catch (Exception e) {
            log.warn("[infino] bulk failed", e);
            listener.onFailure(e);
        }
    }

    private static void finish(BulkItemResponse[] items, Exception failure, long start, ActionListener<BulkResponse> listener) {
        if (failure != null) {
            listener.onFailure(failure);
            return;
        }
        listener.onResponse(new BulkResponse(items, (System.nanoTime() - start) / 1_000_000L));
    }

    /** An item's response under the slot it has in the whole bulk. */
    private static BulkItemResponse reslot(BulkItemResponse r, int slot) {
        return r.isFailed() ? new BulkItemResponse(slot, r.getOpType(), r.getFailure()) : new BulkItemResponse(slot, r.getOpType(), r.getResponse());
    }

    /** Check every item against the table and turn it into a write, or answer it now when it cannot be written. */
    private static List<Outcome> stage(String table, ShardId shardId, List<DocWriteRequest<?>> requests, InfinoWriter writes) {
        String indexName = shardId.getIndexName();
        List<Outcome> outcomes = new ArrayList<>(requests.size());
        for (int slot = 0; slot < requests.size(); slot++) {
            DocWriteRequest<?> item = requests.get(slot);
            // The table keeps no versions or sequence numbers: a write conditioned on the
            // constants it reports is served as unconditioned, one on anything else refused.
            String conditioned = conditioned(item);
            if (conditioned != null) {
                outcomes.add(new Outcome(slot, item, null, failure(slot, item, indexName, new IllegalArgumentException("[infino] " + conditioned), RestStatus.BAD_REQUEST)));
                continue;
            }
            if (item instanceof IndexRequest index) {
                boolean generated = index.id() == null;
                if (generated) {
                    index.id(UUIDs.base64UUID());
                }
                // The document is checked against the declared columns when its batch is
                // appended, and a refused one comes back as its own item then; checking it
                // here too would parse every document twice.
                String source = sourceAsJson(index.source(), index.getContentType());
                // A generated id is new by definition; an explicit id is looked up at flush,
                // to replace its row, or to refuse a create that would duplicate it.
                boolean create = index.opType() == DocWriteRequest.OpType.CREATE;
                outcomes.add(new Outcome(slot, item, new Write(index.id(), source, !generated, create && !generated), null));
            } else if (item instanceof DeleteRequest delete) {
                outcomes.add(new Outcome(slot, item, new Write(delete.id(), null, true), null));
            } else if (item instanceof UpdateRequest update) {
                if (update.script() != null) {
                    outcomes.add(new Outcome(slot, item, null, failure(slot, item, indexName, new IllegalArgumentException("[infino] scripted updates are not served"), RestStatus.BAD_REQUEST)));
                    continue;
                }
                // The merge base is the pending write for the id, else the table itself;
                // never a prefetched copy, which may predate an acknowledged write.
                String current = writes.pendingSource(table, update.id());
                if (current == null) {
                    current = InfinoNative.getSourceFresh(table, update.id());
                }
                String merged = merge(current, update);
                if (merged == null) {
                    outcomes.add(new Outcome(slot, item, null, failure(slot, item, indexName, new DocumentMissingException(shardId, update.id()), RestStatus.NOT_FOUND)));
                    continue;
                }
                String why = InfinoNative.checkDoc(table, merged);
                if (why != null) {
                    outcomes.add(new Outcome(slot, item, null, failure(slot, item, indexName, new IllegalArgumentException("[infino] " + why), RestStatus.BAD_REQUEST)));
                    continue;
                }
                outcomes.add(new Outcome(slot, item, new Write(update.id(), merged, true), null));
            } else {
                outcomes.add(new Outcome(slot, item, null, failure(slot, item, indexName, new IllegalArgumentException("[infino] unsupported write " + item.getClass().getSimpleName()), RestStatus.BAD_REQUEST)));
            }
        }
        return outcomes;
    }

    /** Why a write's condition cannot be served, or null when it is unconditioned or on the values the table reports (see {@link InfinoDocumentVersion}). */
    static String conditioned(DocWriteRequest<?> item) {
        return InfinoDocumentVersion.conditioned(item);
    }

    private static BulkItemResponse failure(int slot, DocWriteRequest<?> item, String index, Exception cause, RestStatus status) {
        return new BulkItemResponse(slot, item.opType(), new BulkItemResponse.Failure(index, item.id(), cause, status));
    }

    /** The source in whatever content type the client sent, as JSON for the bridge. */
    static String sourceAsJson(BytesReference source, MediaType type) {
        if (source == null) {
            return "{}";
        }
        try {
            if (type == null || type == MediaTypeRegistry.JSON) {
                return source.utf8ToString();
            }
            return XContentHelper.convertToJson(source, false, type);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * The document an update produces: the partial document merged into the
     * current one; the upsert document when there is no current one and the
     * request carries one (or asks for the partial document as the upsert);
     * null when the document is missing and nothing says what to write.
     */
    static String merge(String current, UpdateRequest update) {
        boolean exists = current != null && !current.isEmpty();
        IndexRequest doc = update.doc();
        if (!exists) {
            if (update.docAsUpsert() && doc != null) {
                return sourceAsJson(doc.source(), doc.getContentType());
            }
            if (update.upsertRequest() != null) {
                return sourceAsJson(update.upsertRequest().source(), update.upsertRequest().getContentType());
            }
            return null;
        }
        if (doc == null) {
            return current;
        }
        Map<String, Object> merged = XContentHelper.convertToMap(new BytesArray(current), true, MediaTypeRegistry.JSON).v2();
        Map<String, Object> partial = XContentHelper.convertToMap(doc.source(), true, doc.getContentType()).v2();
        XContentHelper.update(merged, partial, true);
        try (XContentBuilder b = XContentFactory.jsonBuilder()) {
            b.map(merged);
            return BytesReference.bytes(b).utf8ToString();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
