/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.OpenSearchStatusException;
import org.opensearch.action.ActionListenerResponseHandler;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.DocWriteRequest;
import org.opensearch.action.DocWriteResponse;
import org.opensearch.action.UnavailableShardsException;
import org.opensearch.action.bulk.BulkAction;
import org.opensearch.action.bulk.BulkItemResponse;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.delete.DeleteRequest;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.support.ActionFilterChain;
import org.opensearch.action.support.AutoCreateIndex;
import org.opensearch.action.support.ContextPreservingActionListener;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.action.update.UpdateRequest;
import org.opensearch.action.update.UpdateResponse;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.ClusterStateObserver;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.metadata.MetadataIndexTemplateService;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.cluster.routing.OperationRouting;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.UUIDs;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.MediaType;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.index.engine.DocumentMissingException;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.infino.InfinoWriter.Outcome;
import org.opensearch.infino.InfinoWriter.Waiter;
import org.opensearch.infino.InfinoWriter.Write;
import org.opensearch.ingest.IngestService;
import org.opensearch.node.NodeClosedException;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReferenceArray;
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
 * <p>The documents go through their ingest pipelines first (the one the
 * request names, the index's default and final pipelines), here on the node
 * as the stock bulk runs them before any shard; a document a pipeline fails
 * or drops is answered as stock answers it and not written.
 *
 * <p>An update is merged here into the document it targets. The writes then
 * go to the table's {@link InfinoWriter}, which batches them across requests
 * and answers the bulk once they are committed; a document the declared
 * columns cannot hold comes back from that append as its own item with the
 * reason, as a mapping parse failure would be.
 *
 * <p>Each Infino-backed index's table is written by one node, the one
 * holding its primary shard; a write that arrives elsewhere is sent there
 * ({@link #dispatch}). A bulk mixing Infino-backed and other indices sends
 * the others' items to the stock bulk, and answers every item in its slot.
 */
public class InfinoBulkActionFilter extends InfinoActionFilter {

    private static final Logger log = LogManager.getLogger(InfinoBulkActionFilter.class);
    /**
     * The header of a bulk sent to the node that writes its tables. Only a
     * node sets it: it is no REST header, so no client request carries it.
     */
    static final String FORWARDED = "_infino_forwarded";
    /**
     * The header of a forwarded bulk sent on once more, because the node it
     * reached no longer wrote its tables by that node's newer cluster state;
     * the node it reaches next never sends it on again (see {@link #dispatch}).
     */
    static final String FORWARDED_AGAIN = "_infino_forwarded_again";
    /** How many times a bulk may be sent on to the node that writes its tables. */
    private static final int MAX_FORWARDS = 2;
    /** The destination of the items a bulk writes to indices Infino does not back: the stock bulk on this node. */
    private static final String STOCK = "";
    /** The id stock answers a dropped document under when the request gave it none. */
    private static final String DROPPED_GENERATED_ID = "auto-generated";
    /** The primary term stock answers a dropped document with: it was never written. */
    private static final long DROPPED_PRIMARY_TERM = 0L;

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
    protected <Request extends ActionRequest, Response extends ActionResponse> void filter(
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
        route(bulk, clusters, writes, tables, cast, () -> toShards(task, action, request, listener, chain, bulk, clusters, cast));
    }

    /**
     * A bulk no item of which this node serves from a table, on to the shards:
     * as it came, unless it carries updates to default-mode indices, which are
     * turned into writes of the whole document first ({@link #engineUpdates})
     * and their responses back into the ones the client's bulk expects.
     */
    private <Request extends ActionRequest, Response extends ActionResponse> void toShards(
        Task task,
        String action,
        Request request,
        ActionListener<Response> listener,
        ActionFilterChain<Request, Response> chain,
        BulkRequest bulk,
        ClusterService clusters,
        ActionListener<BulkResponse> cast
    ) {
        EngineBulk routed;
        try {
            routed = engineUpdates(clusters.state(), bulk, concreteIndices(clusters.state(), bulk), tables);
        } catch (Exception e) {
            listener.onFailure(e);
            return;
        }
        if (routed == null) {
            chain.proceed(task, action, request, listener);
            return;
        }
        @SuppressWarnings("unchecked")
        Request next = (Request) routed.bulk();
        @SuppressWarnings("unchecked")
        ActionListener<Response> answer = (ActionListener<Response>) routed.answer(cast);
        chain.proceed(task, action, next, answer);
    }

    /**
     * Serve a bulk from the tables when its items write to Infino-backed
     * indices, as the stock bulk would serve it from the shards: its ingest
     * pipelines run first, then the indices its documents name that do not
     * exist are created, so a document a pipeline sends to another index is
     * served there and the index it named is not created for it. When no
     * item writes to an Infino-backed index, {@code stock} runs instead and
     * the bulk goes on as it came.
     */
    static void route(BulkRequest bulk, ClusterService clusters, InfinoWriter writes, InfinoTables tables, ActionListener<BulkResponse> listener, Runnable stock) {
        // As the bulk arrives: a bulk that names an attached index runs no pipeline and creates no index.
        Exception refused = attachedRefusal(clusters.state(), concreteIndices(clusters.state(), bulk));
        if (refused != null) {
            listener.onFailure(refused);
            return;
        }
        if (forwards() > 0) {
            // Its pipelines ran on the node it arrived at, which marked each document
            // resolved; they never run again here, whatever the documents carry.
            createAndServe(bulk, clusters, new BulkItemResponse[bulk.requests().size()], writes, tables, listener, stock);
            return;
        }
        if (!resolvePipelines(bulk, clusters.state())) {
            createAndServe(bulk, clusters, new BulkItemResponse[bulk.requests().size()], writes, tables, listener, stock);
            return;
        }
        // A bulk bound for no table goes on as it came, its pipelines run by the stock bulk. Not while
        // the cluster holds an attached index: a pipeline may send a document to it from an index
        // served by its shards, and the stock bulk would write it to that index's own shard. The
        // pipelines run here instead, and the bulk is judged again by where its documents then stand.
        if (!infinoBound(clusters, bulk) && !anyAttached(clusters.state())) {
            stock.run();
            return;
        }
        ingest(
            bulk,
            clusters,
            ActionListener.wrap(
                answered -> createAndServe(
                    bulk,
                    clusters,
                    answered,
                    writes,
                    tables,
                    listener,
                    // Its pipelines sent every document elsewhere: the stock bulk takes
                    // them as they now stand, past their pipelines.
                    () -> dispatch(bulk, concreteIndices(clusters.state(), bulk), Map.of(), answered, writes, tables, clusters, listener)
                ),
                listener::onFailure
            )
        );
    }

    /**
     * Create the indices the bulk's items name that do not exist, then serve
     * it from the tables with {@code answered} (the items its pipelines
     * answered), or run {@code instead} when none of its indices is
     * Infino-backed.
     */
    private static void createAndServe(
        BulkRequest bulk,
        ClusterService clusters,
        BulkItemResponse[] answered,
        InfinoWriter writes,
        InfinoTables tables,
        ActionListener<BulkResponse> listener,
        Runnable instead
    ) {
        createMissing(missingIndices(clusters, bulk, answered), ActionListener.wrap(created -> {
            Map<String, IndexMetadata> infino;
            List<String> concrete = concreteIndices(clusters.state(), bulk);
            try {
                infino = infinoIndices(clusters, concrete);
            } catch (Exception e) {
                listener.onFailure(e);
                return;
            }
            if (infino.isEmpty()) {
                instead.run();
                return;
            }
            dispatch(bulk, concrete, infino, answered, writes, tables, clusters, listener);
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
     * Send each item where it is written, and answer the bulk with every
     * item in its own slot once all are. An Infino-backed index is written
     * by one node, the one holding its primary shard, as a stock index is:
     * a table's sequence numbers and its id lookups are then one node's, so
     * two nodes never give two writes the same number, and never both append
     * an id neither found. Items for an index this node writes are served
     * here; the others go to their node as one bulk per node, past their
     * pipelines, and that node serves them without sending them on. Items
     * for an index Infino does not back (a mixed bulk, or a document a
     * pipeline sent elsewhere) go to the stock bulk on this node, past their
     * pipelines too.
     *
     * <p>A bulk forwarded here is checked against this node's own cluster
     * state, which may be newer than the sender's: the items of an index
     * whose primary has moved on are sent on once more, to the node holding
     * it now, and a bulk sent on that way that still misses the writer is
     * answered with the primary unavailable for those items rather than
     * written here, so a moving shard sends a bulk on at most twice and two
     * nodes never write one table. While an index has no active primary its
     * items wait for one, up to the bulk's timeout, as the stock bulk's do.
     *
     * <p>Each destination is a part of the bulk answered on its own: a part
     * that fails, or whose index gets no primary in time, answers each of its
     * items with the failure, as the stock bulk answers the items of a shard
     * that failed, and the other parts keep their results.
     */
    static void dispatch(
        BulkRequest bulk,
        List<String> concrete,
        Map<String, IndexMetadata> infino,
        BulkItemResponse[] answered,
        InfinoWriter writes,
        InfinoTables tables,
        ClusterService clusters,
        ActionListener<BulkResponse> listener
    ) {
        int forwards = forwards();
        ClusterState state = clusters.state();
        // Again where every bulk is served from, past its pipelines: one of them may have sent a
        // document on to an attached index that the bulk did not name when it arrived.
        Exception refused = attachedRefusal(state, concrete);
        if (refused != null) {
            listener.onFailure(refused);
            return;
        }
        String local = state.nodes().getLocalNodeId();
        long start = System.nanoTime();
        BulkItemResponse[] items = new BulkItemResponse[bulk.requests().size()];
        // Where each index's items go, and each destination's slots. An item its
        // pipeline answered keeps that answer and goes nowhere; so does one this node
        // answers now because its primary moved on after the bulk was sent on twice.
        Map<String, Destination> byIndex = new HashMap<>();
        Map<Destination, List<Integer>> slotsByDestination = new LinkedHashMap<>();
        boolean anyAnswered = false;
        Set<String> aliasRequired = aliasRequired(bulk, answered);
        for (int slot = 0; slot < bulk.requests().size(); slot++) {
            if (answered[slot] != null) {
                items[slot] = answered[slot];
                anyAnswered = true;
                continue;
            }
            String index = concrete.get(slot);
            IndexNotFoundException noAlias = notAnAlias(bulk.requests().get(slot), state.metadata());
            if (noAlias == null) {
                noAlias = notCreated(bulk.requests().get(slot), aliasRequired, state.metadata(), InfinoServices.indexNameExpressionResolver());
            }
            if (noAlias != null) {
                items[slot] = failure(slot, bulk.requests().get(slot), bulk.requests().get(slot).index(), noAlias);
                anyAnswered = true;
                continue;
            }
            Destination destination = byIndex.computeIfAbsent(index, name -> destination(state, infino.get(name), local, forwards));
            if (destination.moved()) {
                items[slot] = failure(slot, bulk.requests().get(slot), index, movedOn(infino.get(index).getIndex()));
                anyAnswered = true;
                continue;
            }
            slotsByDestination.computeIfAbsent(destination, k -> new ArrayList<>()).add(slot);
        }
        Destination here = new Destination(local, null, false);
        if (!anyAnswered && slotsByDestination.size() == 1 && slotsByDestination.containsKey(here)) {
            serve(bulk, concrete, infino, answered, writes, tables, listener);
            return;
        }
        if (slotsByDestination.isEmpty()) {
            finish(items, start, listener);
            return;
        }
        AtomicInteger pending = new AtomicInteger(slotsByDestination.size());
        for (Map.Entry<Destination, List<Integer>> group : slotsByDestination.entrySet()) {
            List<Integer> slots = group.getValue();
            BulkRequest part = new BulkRequest();
            part.setRefreshPolicy(bulk.getRefreshPolicy());
            part.timeout(bulk.timeout());
            part.waitForActiveShards(bulk.waitForActiveShards());
            List<String> partConcrete = new ArrayList<>(slots.size());
            Map<String, IndexMetadata> partInfino = new LinkedHashMap<>();
            for (int slot : slots) {
                part.add(bulk.requests().get(slot));
                partConcrete.add(concrete.get(slot));
                IndexMetadata md = infino.get(concrete.get(slot));
                if (md != null) {
                    partInfino.put(concrete.get(slot), md);
                }
            }
            ActionListener<BulkResponse> partListener = slotted(bulk, concrete, slots, items, pending, start, listener);
            Destination destination = group.getKey();
            if (destination.waitingFor() != null) {
                // Its index has no active primary: once one is, the part goes where it is.
                awaitWriter(
                    part,
                    destination.waitingFor(),
                    clusters,
                    partListener,
                    () -> dispatch(part, partConcrete, partInfino, new BulkItemResponse[slots.size()], writes, tables, clusters, partListener)
                );
            } else if (STOCK.equals(destination.node())) {
                InfinoClient.bulk(part, partListener);
            } else if (destination.equals(here)) {
                serve(part, partConcrete, partInfino, new BulkItemResponse[slots.size()], writes, tables, partListener);
            } else {
                // The write lands on another node, which this node's request cache
                // does not hear from: its responses over these tables move away here
                // before the write, and once it is answered nothing read from them is
                // kept until this node's reads are sure to see it.
                Map<String, IndexMetadata> written = new LinkedHashMap<>();
                try {
                    for (IndexMetadata md : partInfino.values()) {
                        written.put(tables.ensure(md), md);
                    }
                } catch (Exception e) {
                    partListener.onFailure(e);
                    continue;
                }
                written.keySet().forEach(InfinoRequestCache::changed);
                Runnable settle = () -> written.forEach((t, md) -> InfinoRequestCache.writtenElsewhere(t, InfinoRequestCache.lifetime(List.of(md))));
                forward(state.nodes().get(destination.node()), part, ActionListener.wrap(response -> {
                    settle.run();
                    partListener.onResponse(response);
                }, e -> {
                    settle.run();
                    partListener.onFailure(e);
                }));
            }
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
        update.setForcedRefresh(w.forcedRefresh());
        return new BulkItemResponse(slot, DocWriteRequest.OpType.UPDATE, update);
    }

    /**
     * Where a bulk's items for one index go: {@code node}, the one that
     * writes its table ({@link #STOCK} for an index Infino does not back);
     * or, while it has no active primary, {@code waitingFor} it; or nowhere
     * when it has {@code moved} on from this node after the bulk was sent on
     * as often as it may be.
     */
    private record Destination(String node, Index waitingFor, boolean moved) {}

    /** Where the items for {@code md} (null for an index Infino does not back) go from this node, the bulk sent on {@code forwards} times so far. */
    private static Destination destination(ClusterState state, IndexMetadata md, String local, int forwards) {
        if (md == null) {
            return new Destination(STOCK, null, false);
        }
        String node = writerNode(state, md.getIndex());
        if (node == null) {
            return new Destination(null, md.getIndex(), false);
        }
        return new Destination(node, null, forwards == MAX_FORWARDS && !node.equals(local));
    }

    /** What an item of {@code index} is answered with when its primary moved on while the bulk was sent on to it twice: a client retries it. */
    private static UnavailableShardsException movedOn(Index index) {
        return new UnavailableShardsException(new ShardId(index, 0), "the primary moved to another node while the bulk was sent on to this one");
    }

    /** How many times the bulk this thread serves was sent on to the node that writes its tables: 0 when it arrived here from its client. */
    private static int forwards() {
        ThreadContext context = InfinoServices.threadContext();
        if (context == null || context.getHeader(FORWARDED) == null) {
            return 0;
        }
        return context.getHeader(FORWARDED_AGAIN) == null ? 1 : MAX_FORWARDS;
    }

    /**
     * The listener of one part of a bulk, whose items sit at {@code slots}
     * of the whole: it puts the part's answers in those slots, or when the
     * part fails answers each of its items with the failure, as the stock
     * bulk answers the items of a shard that failed. The bulk is answered
     * once every part is.
     */
    private static ActionListener<BulkResponse> slotted(
        BulkRequest bulk,
        List<String> concrete,
        List<Integer> slots,
        BulkItemResponse[] items,
        AtomicInteger pending,
        long start,
        ActionListener<BulkResponse> listener
    ) {
        return ActionListener.wrap(response -> {
            for (int i = 0; i < slots.size(); i++) {
                items[slots.get(i)] = reslot(response.getItems()[i], slots.get(i));
            }
            if (pending.decrementAndGet() == 0) {
                finish(items, start, listener);
            }
        }, e -> {
            log.debug("[infino] {} items of a bulk failed", slots.size(), e);
            for (int slot : slots) {
                items[slot] = failure(slot, bulk.requests().get(slot), concrete.get(slot), e);
            }
            if (pending.decrementAndGet() == 0) {
                finish(items, start, listener);
            }
        });
    }

    /**
     * The node that writes {@code index}'s table: the one holding its active
     * primary shard; null while it has none.
     */
    static String writerNode(ClusterState state, Index index) {
        ShardRouting primary = primary(state, index);
        return primary == null ? null : primary.currentNodeId();
    }

    /**
     * Which copy writes {@code index}'s table: its active primary's
     * allocation; null while it has none. It changes whenever the primary
     * moves or another copy takes over, which a writer that hands out
     * sequence numbers needs to know (see {@link InfinoWriter}).
     */
    static String writerAllocation(ClusterState state, Index index) {
        ShardRouting primary = primary(state, index);
        return primary == null || primary.allocationId() == null ? null : primary.allocationId().getId();
    }

    private static ShardRouting primary(ClusterState state, Index index) {
        IndexRoutingTable routing = state.routingTable().index(index);
        if (routing == null || routing.shard(0) == null) {
            return null;
        }
        ShardRouting primary = routing.shard(0).primaryShard();
        return primary != null && primary.active() ? primary : null;
    }

    /**
     * Wait for {@code index} to have an active primary, then {@code retry};
     * past the bulk's timeout, answer that it has none, as the stock bulk
     * does.
     */
    private static void awaitWriter(BulkRequest bulk, Index index, ClusterService clusters, ActionListener<BulkResponse> listener, Runnable retry) {
        ThreadContext context = InfinoServices.threadContext();
        ClusterStateObserver observer = new ClusterStateObserver(clusters, bulk.timeout(), log, context);
        observer.waitForNextChange(new ClusterStateObserver.Listener() {
            @Override
            public void onNewClusterState(ClusterState state) {
                retry.run();
            }

            @Override
            public void onClusterServiceClose() {
                listener.onFailure(new NodeClosedException(clusters.localNode()));
            }

            @Override
            public void onTimeout(TimeValue timeout) {
                listener.onFailure(
                    new UnavailableShardsException(new ShardId(index, 0), "primary shard is not active Timeout: [{}]", timeout)
                );
            }
        }, state -> writerNode(state, index) != null);
    }

    /**
     * Send {@code part} to {@code node}, which writes its tables, as a bulk
     * marked forwarded (forwarded again when it arrived here forwarded), so
     * it serves it there. It goes as the caller: the node authorizes it
     * again, and the answer comes back in the caller's context.
     */
    private static void forward(DiscoveryNode node, BulkRequest part, ActionListener<BulkResponse> listener) {
        TransportService transport = InfinoServices.transportService();
        ThreadContext context = InfinoServices.threadContext();
        if (transport == null || context == null || node == null) {
            listener.onFailure(new IllegalStateException("[infino] the node that writes the table cannot be reached"));
            return;
        }
        ActionListener<BulkResponse> inCallerContext = new ContextPreservingActionListener<>(context.newRestorableContext(false), listener);
        try (ThreadContext.StoredContext ignored = context.newStoredContext(true)) {
            // A header once set stays for the request's life: one sent on again carries a second.
            context.putHeader(context.getHeader(FORWARDED) == null ? FORWARDED : FORWARDED_AGAIN, "true");
            transport.sendRequest(node, BulkAction.NAME, part, new ActionListenerResponseHandler<>(inCallerContext, BulkResponse::new));
        }
    }

    /**
     * Whether any item writes to an index served here, from its table, as
     * the bulk stands: one in proxy mode, or one that does not exist yet and
     * that the cluster would create in proxy mode. An index in default mode
     * is written by its shards, and its bulk goes on as it came.
     */
    static boolean infinoBound(ClusterService clusters, BulkRequest bulk) {
        ClusterState state = clusters.state();
        Set<String> missing = new HashSet<>(missingIndices(clusters, bulk));
        for (String name : concreteIndices(state, bulk)) {
            IndexMetadata md = name == null ? null : state.metadata().index(name);
            boolean served = md != null ? InfinoPlugin.inProxyMode(md.getSettings()) : missing.contains(name) && createdInProxyMode(clusters, name);
            if (served) {
                return true;
            }
        }
        return false;
    }

    /**
     * Whether an index the cluster created under {@code name} now would be in
     * proxy mode: Infino-backed (the plugin stamps every new index unless the
     * node turns that off) and given proxy mode, by a template or, when none
     * names a mode, by the node, whose new indices are in default mode unless
     * it turns that off; a template's own setting wins (see
     * {@link InfinoPlugin#getAdditionalIndexSettingProviders}), the template
     * found as index creation finds it: a composable one when one matches,
     * else the legacy ones.
     */
    static boolean createdInProxyMode(ClusterService clusters, String name) {
        Metadata metadata = clusters.state().metadata();
        String composable = MetadataIndexTemplateService.findV2Template(metadata, name, false);
        Settings fromTemplates = composable != null
            ? MetadataIndexTemplateService.resolveSettings(metadata, composable)
            : MetadataIndexTemplateService.resolveSettings(MetadataIndexTemplateService.findV1Templates(metadata, name, null));
        Settings node = clusters.getSettings();
        if (!fromTemplates.getAsBoolean(InfinoPlugin.INFINO_ENABLED.getKey(), InfinoPlugin.INFINO_DEFAULT_ENABLED.get(node))) {
            return false;
        }
        String nodeMode = InfinoPlugin.servesDefaultMode(node) ? InfinoPlugin.MODE_DEFAULT : InfinoPlugin.MODE_PROXY;
        return InfinoPlugin.MODE_PROXY.equals(fromTemplates.get(InfinoPlugin.INFINO_MODE.getKey(), nodeMode));
    }

    /**
     * Resolve onto each document the pipelines that apply to it (the one the
     * request names, its index's default and final pipelines), as the stock
     * bulk resolves them; whether any document has one to run.
     */
    static boolean resolvePipelines(BulkRequest bulk, ClusterState state) {
        IngestService ingest = InfinoServices.ingestService();
        if (ingest == null) {
            return false;
        }
        boolean any = false;
        for (DocWriteRequest<?> item : bulk.requests()) {
            IndexRequest doc = indexWriteRequest(item);
            if (doc != null) {
                any |= ingest.resolvePipelines(item, doc, state.metadata());
            }
        }
        return any;
    }

    /**
     * Run the resolved pipelines over the bulk's documents on the write
     * pool, as the stock bulk runs them before any shard: each document is
     * rewritten in place, and the answer stock gives an item a pipeline failed
     * or dropped is handed back under the item's slot (null for the rest).
     */
    static void ingest(BulkRequest bulk, ClusterService clusters, ActionListener<BulkItemResponse[]> then) {
        if (!clusters.localNode().isIngestNode()) {
            then.onFailure(new IllegalArgumentException("[infino] the bulk's documents go through an ingest pipeline, and this node has no ingest role"));
            return;
        }
        int n = bulk.requests().size();
        // Pipelines may answer items from several threads; the first answer for a slot stands, as in stock.
        AtomicReferenceArray<BulkItemResponse> answered = new AtomicReferenceArray<>(n);
        InfinoIngest.executeBulkRequest(
            InfinoServices.ingestService(),
            bulk,
            (slot, e) -> answered.compareAndSet(slot, null, pipelineFailure(bulk.requests().get(slot), slot, e)),
            (thread, e) -> {
                if (e != null) {
                    then.onFailure(e);
                    return;
                }
                BulkItemResponse[] out = new BulkItemResponse[n];
                for (int i = 0; i < n; i++) {
                    out[i] = answered.get(i);
                }
                then.onResponse(out);
            },
            slot -> answered.compareAndSet(slot, null, dropped(bulk.requests().get(slot), slot)),
            ThreadPool.Names.WRITE
        );
    }

    /**
     * The document an item writes, which its pipelines run over: an index
     * request's own, an update's upsert (its partial document when that is
     * the upsert); null for a delete. The stock bulk's rule.
     */
    static IndexRequest indexWriteRequest(DocWriteRequest<?> item) {
        if (item instanceof IndexRequest index) {
            return index;
        }
        if (item instanceof UpdateRequest update) {
            return update.docAsUpsert() ? update.doc() : update.upsertRequest();
        }
        return null;
    }

    /** The answer stock gives an item whose pipeline failed. */
    private static BulkItemResponse pipelineFailure(DocWriteRequest<?> item, int slot, Exception e) {
        IndexRequest doc = indexWriteRequest(item);
        return new BulkItemResponse(slot, doc.opType(), new BulkItemResponse.Failure(doc.index(), doc.id(), e));
    }

    /** The answer stock gives an item whose pipeline dropped its document: a no-op, under a placeholder id when it had none. */
    private static BulkItemResponse dropped(DocWriteRequest<?> item, int slot) {
        IndexRequest doc = indexWriteRequest(item);
        String id = doc.id() == null ? DROPPED_GENERATED_ID : doc.id();
        ShardId shard = new ShardId(doc.index(), IndexMetadata.INDEX_UUID_NA_VALUE, 0);
        return new BulkItemResponse(
            slot,
            doc.opType(),
            new UpdateResponse(shard, id, SequenceNumbers.UNASSIGNED_SEQ_NO, DROPPED_PRIMARY_TERM, doc.version(), DocWriteResponse.Result.NOOP)
        );
    }

    /**
     * The concrete index each item writes to, by slot: the name it gives, or
     * for an alias or a data stream its write index, as the stock path
     * resolves it. A name that resolves to no single write index keeps its
     * name, so the stock path reports it; so does a write stock refuses on
     * a data stream (any op but {@code create}, or a custom routing).
     */
    static List<String> concreteIndices(ClusterState state, BulkRequest bulk) {
        IndexNameExpressionResolver resolver = InfinoServices.indexNameExpressionResolver();
        List<String> out = new ArrayList<>(bulk.requests().size());
        for (DocWriteRequest<?> item : bulk.requests()) {
            String name = item.index();
            boolean appendToStream = item.opType() == DocWriteRequest.OpType.CREATE
                && item.routing() == null
                && state.metadata().dataStreams().containsKey(name);
            if (resolver != null && name != null && (state.metadata().hasAlias(name) || appendToStream)) {
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
     * The first index a bulk's items write to that is attached to a table
     * made outside the plugin ({@link InfinoPlugin#INFINO_ATTACHED}), which
     * it reads and never writes; null when none is. Every write to an
     * index comes as a bulk (a single index, update or delete, and each
     * batch of a write by query, among them), so a bulk refused here is
     * every write refused. {@code concrete} is the index of each item as it
     * now stands: read before the bulk's pipelines run it is what the bulk
     * named, and after them what they sent each document to.
     */
    static String attachedTarget(ClusterState state, List<String> concrete) {
        for (String name : concrete) {
            IndexMetadata md = name == null ? null : state.metadata().index(name);
            if (md != null && InfinoPlugin.INFINO_ENABLED.get(md.getSettings()) && InfinoPlugin.INFINO_ATTACHED.get(md.getSettings())) {
                return name;
            }
        }
        return null;
    }

    /** Whether the cluster holds an index attached to a table made outside the plugin ({@link InfinoPlugin#INFINO_ATTACHED}). */
    static boolean anyAttached(ClusterState state) {
        for (IndexMetadata md : state.metadata()) {
            if (InfinoPlugin.INFINO_ENABLED.get(md.getSettings()) && InfinoPlugin.INFINO_ATTACHED.get(md.getSettings())) {
                return true;
            }
        }
        return false;
    }

    /** The failure a bulk is answered with when one of {@code concrete} is attached ({@link #attachedTarget}); null when none is. */
    static OpenSearchStatusException attachedRefusal(ClusterState state, List<String> concrete) {
        String attached = attachedTarget(state, concrete);
        if (attached == null) {
            return null;
        }
        return new OpenSearchStatusException(
            "[infino] index [{}] is attached to a table it reads and never writes ([{}]); no document is indexed, updated or deleted through it",
            RestStatus.FORBIDDEN,
            attached,
            InfinoPlugin.INFINO_ATTACHED.getKey()
        );
    }

    /**
     * The indices the bulk's items name that do not exist and the cluster
     * would create for a stock bulk, in order; empty when none. When the
     * cluster's rule forbids creating one, nothing is listed: the stock path
     * reports the missing index. Neither is one an item requires be an alias
     * ({@link #aliasRequired}): the cluster creates no index for it, and the
     * item fails.
     */
    static List<String> missingIndices(ClusterService clusters, BulkRequest bulk) {
        return missingIndices(clusters, bulk, null);
    }

    /**
     * {@link #missingIndices(ClusterService, BulkRequest)} over the items its
     * pipelines left: an item they failed or dropped ({@code answered} holds
     * its answer under its slot; null when none is answered) is out of the
     * bulk the stock bulk creates indices for.
     */
    static List<String> missingIndices(ClusterService clusters, BulkRequest bulk, BulkItemResponse[] answered) {
        return missingIndices(clusters.state(), InfinoServices.autoCreateIndex(), bulk, answered);
    }

    /** {@link #missingIndices(ClusterService, BulkRequest, BulkItemResponse[])} against {@code state} under {@code rule}. */
    static List<String> missingIndices(ClusterState state, AutoCreateIndex rule, BulkRequest bulk, BulkItemResponse[] answered) {
        Set<String> aliasRequired = aliasRequired(bulk, answered);
        Set<String> missing = new LinkedHashSet<>();
        for (int slot = 0; slot < bulk.requests().size(); slot++) {
            if (answered != null && answered[slot] != null) {
                continue;
            }
            String name = bulk.requests().get(slot).index();
            if (name == null
                || state.metadata().getIndicesLookup().containsKey(name)
                || missing.contains(name)
                || aliasRequired.contains(name)) {
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

    /**
     * The names a bulk's items write to with {@code require_alias} set, which
     * the stock bulk never creates an index for: one such item keeps the name
     * from being created for every item that names it.
     */
    static Set<String> aliasRequired(BulkRequest bulk) {
        return aliasRequired(bulk, null);
    }

    /**
     * {@link #aliasRequired(BulkRequest)} over the items its pipelines left:
     * an item they failed or dropped ({@code answered} holds its answer under
     * its slot; null when none is answered) is out of the bulk the stock bulk
     * goes on with, so its flag keeps no name from being created.
     */
    static Set<String> aliasRequired(BulkRequest bulk, BulkItemResponse[] answered) {
        Set<String> names = new HashSet<>();
        for (int slot = 0; slot < bulk.requests().size(); slot++) {
            DocWriteRequest<?> item = bulk.requests().get(slot);
            if ((answered == null || answered[slot] == null) && item.isRequireAlias() && item.index() != null) {
                names.add(item.index());
            }
        }
        return names;
    }

    /**
     * Why an item that requires its index be an alias fails, as the stock
     * bulk fails it, when the name it writes to is no alias; null when it
     * passes or requires nothing.
     */
    static IndexNotFoundException notAnAlias(DocWriteRequest<?> item, Metadata metadata) {
        if (!item.isRequireAlias() || metadata.hasAlias(item.index())) {
            return null;
        }
        return new IndexNotFoundException(
            "[" + DocWriteRequest.REQUIRE_ALIAS + "] request flag is [true] and [" + item.index() + "] is not an alias",
            item.index()
        );
    }

    /**
     * Why an item fails, as the stock bulk fails it, when no index or alias
     * has the name it writes to and another item of the bulk requires that
     * name be an alias ({@link #aliasRequired}): the name is not created, so
     * the item finds no index. A date math name is looked up, and reported,
     * as the index {@code resolver} resolves it to, as the stock bulk writes
     * it. Null when the name exists, no item requires it be an alias, or its
     * date math does not parse (the stock bulk reports that). Failing it here
     * keeps it out of the part sent on to the stock bulk, which would create
     * the index for it.
     */
    static IndexNotFoundException notCreated(
        DocWriteRequest<?> item,
        Set<String> aliasRequired,
        Metadata metadata,
        IndexNameExpressionResolver resolver
    ) {
        String name = item.index();
        if (name == null || !aliasRequired.contains(name)) {
            return null;
        }
        String index;
        try {
            index = resolver == null ? name : resolver.resolveDateMathExpression(name);
        } catch (RuntimeException e) {
            return null;
        }
        return metadata.getIndicesLookup().containsKey(index) ? null : new IndexNotFoundException(index);
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
     * is. A write to one the security plugin restricts for this user by
     * document, field or mask is refused: a write reads the document it
     * replaces or merges into and puts fields the rule hides, so under a rule
     * it is refused whole rather than served outside the rule.
     */
    static Map<String, IndexMetadata> infinoIndices(ClusterService clusters, List<String> concrete) {
        Map<String, IndexMetadata> infino = new LinkedHashMap<>();
        for (String name : concrete) {
            IndexMetadata md = name == null ? null : clusters.state().metadata().index(name);
            // An index in default mode takes its writes through its shard's engine, as any index does.
            if (md != null && InfinoPlugin.inProxyMode(md.getSettings())) {
                infino.put(name, md);
            }
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
     * has, each item in the bulk's own slot; an index whose write fails
     * answers its own items with the failure, and the others keep their
     * results. An item {@code answered} holds an answer for (one its ingest
     * pipeline failed or dropped) is given that answer and not written. The
     * items are parsed, conformed and converted on the write pool, never on
     * the thread that delivered the bulk; a write pool whose queue is full
     * refuses the bulk with 429, as a stock bulk's is. The fields the
     * documents add to the mapping are mapped before that, as a shard maps
     * them ({@link InfinoDynamicMapping}).
     */
    static void serve(
        BulkRequest bulk,
        List<String> concrete,
        Map<String, IndexMetadata> infino,
        BulkItemResponse[] answered,
        InfinoWriter writes,
        InfinoTables tables,
        ActionListener<BulkResponse> client
    ) {
        long start = System.nanoTime();
        ActionListener<BulkResponse> listener = refreshing(bulk.getRefreshPolicy(), answered, infino.values(), tables, client);
        InfinoServices.fork(ThreadPool.Names.WRITE, listener::onFailure, () -> {
            // The fields the documents add are mapped first, as a shard maps them, so
            // each table is declared from the mapping that holds them before its rows land.
            BulkItemResponse[] items = answered.clone();
            InfinoDynamicMapping.apply(
                bulk,
                concrete,
                infino,
                items,
                ActionListener.wrap(mapped -> stageAndSubmit(bulk, concrete, mapped, items, writes, tables, listener, start), listener::onFailure)
            );
        });
    }

    /**
     * The listener of a bulk served here under {@code policy}: when it asks
     * for a refresh, the bulk is answered once every read of its tables sees
     * the writes, and with {@code refresh=true} each item it wrote says it
     * forced a refresh, as a stock shard's does. {@code answered} holds the
     * items answered before any write.
     */
    static ActionListener<BulkResponse> refreshing(
        WriteRequest.RefreshPolicy policy,
        BulkItemResponse[] answered,
        Collection<IndexMetadata> indices,
        InfinoTables tables,
        ActionListener<BulkResponse> listener
    ) {
        if (policy == WriteRequest.RefreshPolicy.NONE) {
            return listener;
        }
        List<IndexMetadata> written = List.copyOf(indices);
        return ActionListener.wrap(response -> {
            if (policy == WriteRequest.RefreshPolicy.IMMEDIATE) {
                BulkItemResponse[] items = response.getItems();
                for (int slot = 0; slot < items.length; slot++) {
                    if (answered[slot] == null && items[slot] != null && !items[slot].isFailed()) {
                        items[slot].getResponse().setForcedRefresh(true);
                    }
                }
            }
            if (!InfinoNative.hosted()) {
                listener.onResponse(response);
                return;
            }
            // The wait for hosted reads may last the read window: not on the writer's thread.
            InfinoServices.fork(ThreadPool.Names.GENERIC, listener::onFailure, () -> {
                for (IndexMetadata md : written) {
                    InfinoNative.awaitVisible(tables.ensure(md));
                }
                listener.onResponse(response);
            });
        }, listener::onFailure);
    }

    private static void stageAndSubmit(
        BulkRequest bulk,
        List<String> concrete,
        Map<String, IndexMetadata> infino,
        BulkItemResponse[] answered,
        InfinoWriter writes,
        InfinoTables tables,
        ActionListener<BulkResponse> listener,
        long start
    ) {
        try {
            BulkItemResponse[] items = new BulkItemResponse[bulk.requests().size()];
            if (infino.size() == 1) {
                IndexMetadata md = infino.values().iterator().next();
                String table = tables.ensure(md);
                ShardId shardId = new ShardId(md.getIndex(), 0);
                Waiter waiter = new Waiter(shardId, md.getIndex().getName(), stage(shardId, bulk.requests(), answered), items, start, listener);
                writes.submit(table, waiter);
                return;
            }
            Map<String, List<Integer>> slotsByIndex = new LinkedHashMap<>();
            for (int slot = 0; slot < bulk.requests().size(); slot++) {
                slotsByIndex.computeIfAbsent(concrete.get(slot), k -> new ArrayList<>()).add(slot);
            }
            AtomicInteger pending = new AtomicInteger(slotsByIndex.size());
            for (Map.Entry<String, List<Integer>> group : slotsByIndex.entrySet()) {
                IndexMetadata md = infino.get(group.getKey());
                List<Integer> slots = group.getValue();
                List<DocWriteRequest<?>> requests = new ArrayList<>(slots.size());
                BulkItemResponse[] partAnswered = new BulkItemResponse[slots.size()];
                for (int i = 0; i < slots.size(); i++) {
                    requests.add(bulk.requests().get(slots.get(i)));
                    partAnswered[i] = answered[slots.get(i)];
                }
                BulkItemResponse[] part = new BulkItemResponse[slots.size()];
                ActionListener<BulkResponse> partListener = slotted(bulk, concrete, slots, items, pending, start, listener);
                try {
                    String table = tables.ensure(md);
                    ShardId shardId = new ShardId(md.getIndex(), 0);
                    Waiter waiter = new Waiter(shardId, md.getIndex().getName(), stage(shardId, requests, partAnswered), part, start, partListener);
                    writes.submit(table, waiter);
                } catch (Exception e) {
                    log.warn("[infino] bulk items for {} failed", group.getKey(), e);
                    partListener.onFailure(e);
                }
            }
        } catch (Exception e) {
            log.warn("[infino] bulk failed", e);
            listener.onFailure(e);
        }
    }

    private static void finish(BulkItemResponse[] items, long start, ActionListener<BulkResponse> listener) {
        listener.onResponse(new BulkResponse(items, (System.nanoTime() - start) / 1_000_000L));
    }

    /** An item's response under the slot it has in the whole bulk. */
    private static BulkItemResponse reslot(BulkItemResponse r, int slot) {
        return r.isFailed() ? new BulkItemResponse(slot, r.getOpType(), r.getFailure()) : new BulkItemResponse(slot, r.getOpType(), r.getResponse());
    }

    /**
     * Turn every item into a write, or answer it now when it cannot be
     * written or {@code answered} already holds its answer (by slot, as
     * {@code requests}). A data stream's timestamp is checked where the
     * bridge parses the document, for every write path.
     */
    private static List<Outcome> stage(ShardId shardId, List<DocWriteRequest<?>> requests, BulkItemResponse[] answered) {
        String indexName = shardId.getIndexName();
        List<Outcome> outcomes = new ArrayList<>(requests.size());
        for (int slot = 0; slot < requests.size(); slot++) {
            DocWriteRequest<?> item = requests.get(slot);
            if (answered[slot] != null) {
                outcomes.add(new Outcome(slot, item, null, answered[slot]));
                continue;
            }
            // The table keeps no per-document version, so a version condition is refused here;
            // a sequence-number condition is checked when the item's batch is written.
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
                // Merged when its batch is written, in the order the batch takes the
                // writes: into the write before it in the batch, else the table as
                // the appends before it left it (see InfinoWriter#apply).
                outcomes.add(new Outcome(slot, item, Write.update(update), null));
            } else {
                outcomes.add(new Outcome(slot, item, null, failure(slot, item, indexName, new IllegalArgumentException("[infino] unsupported write " + item.getClass().getSimpleName()), RestStatus.BAD_REQUEST)));
            }
        }
        return outcomes;
    }

    /** Why a write's version condition cannot be served, or null when it has none (see {@link InfinoDocumentVersion}). */
    static String conditioned(DocWriteRequest<?> item) {
        return InfinoDocumentVersion.conditioned(item);
    }

    private static BulkItemResponse failure(int slot, DocWriteRequest<?> item, String index, Exception cause, RestStatus status) {
        return new BulkItemResponse(slot, item.opType(), new BulkItemResponse.Failure(index, item.id(), cause, status));
    }

    /** An item failed by {@code cause}, under the status the cause carries. */
    private static BulkItemResponse failure(int slot, DocWriteRequest<?> item, String index, Exception cause) {
        return new BulkItemResponse(slot, item.opType(), new BulkItemResponse.Failure(index, item.id(), cause));
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
