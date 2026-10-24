/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.DocWriteRequest;
import org.opensearch.action.bulk.BulkAction;
import org.opensearch.action.bulk.BulkItemResponse;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.delete.DeleteRequest;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.support.ActionFilter;
import org.opensearch.action.support.ActionFilterChain;
import org.opensearch.action.update.UpdateRequest;
import org.opensearch.cluster.metadata.IndexMetadata;
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
import org.opensearch.index.engine.DocumentMissingException;
import org.opensearch.infino.InfinoWriter.Outcome;
import org.opensearch.infino.InfinoWriter.Waiter;
import org.opensearch.infino.InfinoWriter.Write;
import org.opensearch.tasks.Task;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Serves {@code _bulk} (and so single index, delete and update requests,
 * which OpenSearch runs as one-item bulks) for Infino-backed indices from
 * the table, before the request is routed to any shard. Nothing reaches
 * Lucene: the index exists in cluster state as a name, a mapping and its
 * settings, and every row lives in the table.
 *
 * <p>Each item is checked here, synchronously: a document the declared
 * columns cannot hold is refused as its own item with the reason, as a
 * mapping parse failure would be; an update is merged into the document it
 * targets. The writes then go to the table's {@link InfinoWriter}, which
 * batches them across requests and answers the bulk once they are committed.
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
        // Which items target an Infino-backed index.
        Map<String, IndexMetadata> infino = new LinkedHashMap<>();
        int infinoItems = 0;
        for (DocWriteRequest<?> item : bulk.requests()) {
            IndexMetadata md = clusters.state().metadata().index(item.index());
            if (md != null && md.getSettings().getAsBoolean(InfinoPlugin.INFINO_ENABLED.getKey(), false)) {
                infino.put(item.index(), md);
                infinoItems++;
            }
        }
        if (infinoItems == 0) {
            chain.proceed(task, action, request, listener);
            return;
        }
        if (infinoItems != bulk.requests().size()) {
            listener.onFailure(new IllegalArgumentException("[infino] a bulk mixing Infino-backed indices with others is not served"));
            return;
        }
        if (infino.size() != 1) {
            listener.onFailure(new IllegalArgumentException("[infino] a bulk over more than one Infino-backed index is not served; send one bulk per index"));
            return;
        }
        long start = System.nanoTime();
        try {
            IndexMetadata md = infino.values().iterator().next();
            String table = tables.ensure(md);
            BulkItemResponse[] items = new BulkItemResponse[bulk.requests().size()];
            @SuppressWarnings("unchecked")
            ActionListener<BulkResponse> cast = (ActionListener<BulkResponse>) listener;
            ShardId shardId = new ShardId(md.getIndex(), 0);
            Waiter waiter = new Waiter(shardId, md.getIndex().getName(), stage(table, shardId, bulk.requests(), writes), items, start, cast);
            writes.submit(table, waiter, bulk.getRefreshPolicy());
        } catch (Exception e) {
            log.warn("[infino] bulk failed", e);
            listener.onFailure(e);
        }
    }

    /** Check every item against the table and turn it into a write, or answer it now when it cannot be written. */
    private static List<Outcome> stage(String table, ShardId shardId, List<DocWriteRequest<?>> requests, InfinoWriter writes) {
        String indexName = shardId.getIndexName();
        List<Outcome> outcomes = new ArrayList<>(requests.size());
        for (int slot = 0; slot < requests.size(); slot++) {
            DocWriteRequest<?> item = requests.get(slot);
            if (item instanceof IndexRequest index) {
                boolean generated = index.id() == null;
                if (generated) {
                    index.id(UUIDs.base64UUID());
                }
                String source = sourceAsJson(index.source(), index.getContentType());
                String why = InfinoNative.checkDoc(table, source);
                if (why != null) {
                    outcomes.add(new Outcome(slot, item, null, failure(slot, item, indexName, new IllegalArgumentException("[infino] " + why), RestStatus.BAD_REQUEST)));
                    continue;
                }
                // A generated id, or an explicit create, is new by definition; an index of an
                // explicit id may replace a row, so its id is looked up at flush.
                boolean lookup = !generated && index.opType() != DocWriteRequest.OpType.CREATE;
                outcomes.add(new Outcome(slot, item, new Write(index.id(), source, lookup), null));
            } else if (item instanceof DeleteRequest delete) {
                outcomes.add(new Outcome(slot, item, new Write(delete.id(), null, true), null));
            } else if (item instanceof UpdateRequest update) {
                if (update.script() != null) {
                    outcomes.add(new Outcome(slot, item, null, failure(slot, item, indexName, new IllegalArgumentException("[infino] scripted updates are not served"), RestStatus.BAD_REQUEST)));
                    continue;
                }
                String current = writes.pendingSource(table, update.id());
                if (current == null) {
                    current = InfinoNative.getSource(table, update.id());
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
