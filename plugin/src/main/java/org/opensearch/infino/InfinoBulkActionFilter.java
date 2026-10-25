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
import org.opensearch.common.lucene.uid.Versions;
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
import org.opensearch.index.VersionType;
import org.opensearch.index.engine.DocumentMissingException;
import org.opensearch.index.seqno.SequenceNumbers;
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
        Map<String, IndexMetadata> infino = infinoIndices(clusters, bulk);
        if (infino.isEmpty()) {
            chain.proceed(task, action, request, listener);
            return;
        }
        @SuppressWarnings("unchecked")
        ActionListener<BulkResponse> cast = (ActionListener<BulkResponse>) listener;
        serve(bulk, infino, writes, tables, cast);
    }

    /**
     * The Infino-backed indices a bulk's items target, by name; empty when
     * none does. A bulk that mixes them with other indices, or spans more
     * than one, is refused.
     */
    static Map<String, IndexMetadata> infinoIndices(ClusterService clusters, BulkRequest bulk) {
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
            return Map.of();
        }
        if (infinoItems != bulk.requests().size()) {
            throw new IllegalArgumentException("[infino] a bulk mixing Infino-backed indices with others is not served");
        }
        if (infino.size() != 1) {
            throw new IllegalArgumentException("[infino] a bulk over more than one Infino-backed index is not served; send one bulk per index");
        }
        return infino;
    }

    /** Serve a bulk over one Infino-backed index: stage its items and hand them to the table's writer. */
    static void serve(BulkRequest bulk, Map<String, IndexMetadata> infino, InfinoWriter writes, InfinoTables tables, ActionListener<BulkResponse> listener) {
        long start = System.nanoTime();
        try {
            IndexMetadata md = infino.values().iterator().next();
            String table = tables.ensure(md);
            BulkItemResponse[] items = new BulkItemResponse[bulk.requests().size()];
            ShardId shardId = new ShardId(md.getIndex(), 0);
            Waiter waiter = new Waiter(shardId, md.getIndex().getName(), stage(table, shardId, bulk.requests(), writes), items, start, listener);
            writes.submit(table, waiter);
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
            // The table keeps no versions or sequence numbers, so a write conditioned on
            // them cannot be checked; it is refused rather than written unconditionally.
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

    /** Why a write conditioned on a version or sequence number cannot be served, or null when it is unconditioned. */
    static String conditioned(DocWriteRequest<?> item) {
        if (item.ifSeqNo() != SequenceNumbers.UNASSIGNED_SEQ_NO || item.ifPrimaryTerm() != SequenceNumbers.UNASSIGNED_PRIMARY_TERM) {
            return "if_seq_no / if_primary_term are not served: the table keeps no sequence numbers";
        }
        // An op_type create carries the "match deleted" marker as its version; that is the create, not a condition.
        boolean unversioned = item.version() == Versions.MATCH_ANY || item.version() == Versions.MATCH_DELETED;
        if (!unversioned || item.versionType() != VersionType.INTERNAL) {
            return "version and version_type are not served: the table keeps no per-document version";
        }
        return null;
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
