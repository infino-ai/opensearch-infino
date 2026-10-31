/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
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
import org.opensearch.action.support.ActionFilter;
import org.opensearch.action.support.ActionFilterChain;
import org.opensearch.action.support.AutoCreateIndex;
import org.opensearch.action.support.ContextPreservingActionListener;
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
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
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
public class InfinoBulkActionFilter implements ActionFilter {

    private static final Logger log = LogManager.getLogger(InfinoBulkActionFilter.class);
    /**
     * The header of a bulk sent to the node that writes its tables: that
     * node serves it, never sending it on.
     */
    static final String FORWARDED = "_infino_forwarded";
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
        route(bulk, clusters, writes, tables, cast, () -> chain.proceed(task, action, request, listener));
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
        if (!resolvePipelines(bulk, clusters.state())) {
            createAndServe(bulk, clusters, new BulkItemResponse[bulk.requests().size()], writes, tables, listener, stock);
            return;
        }
        if (!infinoBound(clusters, bulk)) {
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
                instead.run();
                return;
            }
            dispatch(bulk, concrete, infino, answered, writes, tables, clusters, listener);
        }, listener::onFailure));
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
     * pipelines too. A bulk forwarded here is served here whatever this node
     * now holds, so a moving shard never sends a bulk back and forth. While
     * an index has no active primary the bulk waits for one, up to its
     * timeout, as the stock bulk does.
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
        ThreadContext context = InfinoServices.threadContext();
        boolean forwarded = context != null && context.getHeader(FORWARDED) != null;
        ClusterState state = clusters.state();
        String local = state.nodes().getLocalNodeId();
        Map<String, String> writers = new HashMap<>();
        for (IndexMetadata md : infino.values()) {
            String node = forwarded ? local : writerNode(state, md.getIndex());
            if (node == null) {
                awaitWriter(bulk, md.getIndex(), clusters, listener, () -> dispatch(bulk, concrete, infino, answered, writes, tables, clusters, listener));
                return;
            }
            writers.put(md.getIndex().getName(), node);
        }
        long start = System.nanoTime();
        BulkItemResponse[] items = new BulkItemResponse[bulk.requests().size()];
        // Each slot's destination: a node's id for an Infino-backed index, STOCK for any other.
        // An item its pipeline answered keeps that answer and goes nowhere.
        Map<String, List<Integer>> slotsByDestination = new LinkedHashMap<>();
        boolean anyAnswered = false;
        for (int slot = 0; slot < bulk.requests().size(); slot++) {
            if (answered[slot] != null) {
                items[slot] = answered[slot];
                anyAnswered = true;
                continue;
            }
            String index = concrete.get(slot);
            String destination = infino.containsKey(index) ? writers.get(index) : STOCK;
            slotsByDestination.computeIfAbsent(destination, k -> new ArrayList<>()).add(slot);
        }
        if (!anyAnswered && slotsByDestination.size() == 1 && slotsByDestination.containsKey(local)) {
            serve(bulk, concrete, infino, answered, writes, tables, listener);
            return;
        }
        if (slotsByDestination.isEmpty()) {
            finish(items, null, start, listener);
            return;
        }
        AtomicInteger pending = new AtomicInteger(slotsByDestination.size());
        AtomicReference<Exception> failure = new AtomicReference<>();
        for (Map.Entry<String, List<Integer>> group : slotsByDestination.entrySet()) {
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
            String destination = group.getKey();
            if (STOCK.equals(destination)) {
                InfinoClient.bulk(part, partListener);
            } else if (destination.equals(local)) {
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
                forward(state.nodes().get(destination), part, ActionListener.wrap(response -> {
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
     * marked forwarded, so it serves it there. It goes as the caller: the
     * node authorizes it again, and the answer comes back in the caller's
     * context.
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
            context.putHeader(FORWARDED, "true");
            transport.sendRequest(node, BulkAction.NAME, part, new ActionListenerResponseHandler<>(inCallerContext, BulkResponse::new));
        }
    }

    /**
     * Whether any item writes to an Infino-backed index as the bulk stands:
     * one that exists with the setting, or one that does not exist yet and
     * that the cluster would create Infino-backed.
     */
    static boolean infinoBound(ClusterService clusters, BulkRequest bulk) {
        ClusterState state = clusters.state();
        Set<String> missing = new HashSet<>(missingIndices(clusters, bulk));
        for (String name : concreteIndices(state, bulk)) {
            IndexMetadata md = name == null ? null : state.metadata().index(name);
            boolean backed = md != null ? InfinoPlugin.INFINO_ENABLED.get(md.getSettings()) : missing.contains(name) && createdInfinoBacked(clusters, name);
            if (backed) {
                return true;
            }
        }
        return false;
    }

    /**
     * Whether an index the cluster created under {@code name} now would be
     * Infino-backed: the plugin stamps every new index unless the node turns
     * that off, and a template's own setting wins (see
     * {@link InfinoPlugin#getAdditionalIndexSettingProviders}), the template
     * found as index creation finds it: a composable one when one matches,
     * else the legacy ones.
     */
    static boolean createdInfinoBacked(ClusterService clusters, String name) {
        Metadata metadata = clusters.state().metadata();
        String composable = MetadataIndexTemplateService.findV2Template(metadata, name, false);
        Settings fromTemplates = composable != null
            ? MetadataIndexTemplateService.resolveSettings(metadata, composable)
            : MetadataIndexTemplateService.resolveSettings(MetadataIndexTemplateService.findV1Templates(metadata, name, null));
        return fromTemplates.getAsBoolean(InfinoPlugin.INFINO_ENABLED.getKey(), InfinoPlugin.INFINO_DEFAULT_ENABLED.get(clusters.getSettings()));
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
     * is. A write to one the security plugin restricts for this user by
     * document, field or mask is refused: a write reads the document it
     * replaces or merges into and puts fields the rule hides, so under a rule
     * it is refused whole rather than served outside the rule.
     */
    static Map<String, IndexMetadata> infinoIndices(ClusterService clusters, List<String> concrete) {
        Map<String, IndexMetadata> infino = new LinkedHashMap<>();
        for (String name : concrete) {
            IndexMetadata md = name == null ? null : clusters.state().metadata().index(name);
            if (md != null && md.getSettings().getAsBoolean(InfinoPlugin.INFINO_ENABLED.getKey(), false)) {
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
     * has, each item in the bulk's own slot. An item {@code answered} holds
     * an answer for (one its ingest pipeline failed or dropped) is given that
     * answer and not written.
     */
    static void serve(
        BulkRequest bulk,
        List<String> concrete,
        Map<String, IndexMetadata> infino,
        BulkItemResponse[] answered,
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
                Waiter waiter = new Waiter(shardId, md.getIndex().getName(), stage(table, shardId, bulk.requests(), answered, writes), items, start, listener);
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
                BulkItemResponse[] partAnswered = new BulkItemResponse[slots.size()];
                for (int i = 0; i < slots.size(); i++) {
                    requests.add(bulk.requests().get(slots.get(i)));
                    partAnswered[i] = answered[slots.get(i)];
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
                Waiter waiter = new Waiter(shardId, md.getIndex().getName(), stage(table, shardId, requests, partAnswered, writes), part, start, partListener);
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

    /**
     * Check every item against the table and turn it into a write, or answer
     * it now when it cannot be written or {@code answered} already holds its
     * answer (by slot, as {@code requests}).
     */
    private static List<Outcome> stage(String table, ShardId shardId, List<DocWriteRequest<?>> requests, BulkItemResponse[] answered, InfinoWriter writes) {
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

    /** Why a write's version condition cannot be served, or null when it has none (see {@link InfinoDocumentVersion}). */
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
