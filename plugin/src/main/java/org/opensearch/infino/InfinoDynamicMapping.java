/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.DocWriteRequest;
import org.opensearch.action.bulk.BulkItemResponse;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.support.ContextPreservingActionListener;
import org.opensearch.action.update.UpdateRequest;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.ClusterStateObserver;
import org.opensearch.cluster.action.index.MappingUpdatedAction;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.MediaType;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.IndexService;
import org.opensearch.index.mapper.DocumentMapperForType;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.mapper.Mapping;
import org.opensearch.index.mapper.ObjectMapper;
import org.opensearch.index.mapper.ParsedDocument;
import org.opensearch.index.mapper.SourceToParse;
import org.opensearch.indices.IndicesService;
import org.opensearch.node.NodeClosedException;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The mapping a proxy-mode bulk's new fields add, as a stock shard adds it.
 * Proxy mode answers a bulk before any shard sees it, so OpenSearch's
 * dynamic mapping never ran: a new string field stayed out of the mapping
 * and its column got no text index. Here each document with a field the
 * mapping does not hold is parsed with the index's own document mapper,
 * which applies every rule a shard applies ({@code dynamic}, dynamic
 * templates, date and numeric detection, a string's {@code keyword}
 * subfield); the update it yields goes to the cluster manager through the
 * same action a shard sends it with, and the bulk is written once this node
 * holds the new mapping, so its table is declared from it (a text field with
 * its text index) before the rows land. A document the mapper refuses (a new
 * field under {@code dynamic: strict}, a value its field cannot take) is
 * answered with the mapper's failure, as stock answers it.
 *
 * <p>Most documents add no field: one pass over each document's fields
 * finds that, and only a document with a new field is parsed. The updates
 * of one bulk go as one update per index, so a bulk sends at most one per
 * index and round, and none once its fields are mapped.
 *
 * <p>A field the table already holds as a column the mapping does not name
 * (written before this rule, or under {@code dynamic: false}) is left as it
 * is: a column's text index is fixed when the column is added, so mapping
 * it as text now would only make the table refuse the mapping.
 */
final class InfinoDynamicMapping {

    private static final Logger log = LogManager.getLogger(InfinoDynamicMapping.class);
    /**
     * How many rounds a document's update may go in: a document whose fields are
     * still new after that is left to the table, as before this rule.
     */
    private static final int MAX_ROUNDS = 3;
    /** The id a document is parsed under when the bulk has not given it one yet: the mapping does not depend on it. */
    private static final String PLACEHOLDER_ID = "_infino_dynamic_mapping";

    private InfinoDynamicMapping() {}

    /** One document of the bulk to check: its slot, its index, its source, and whether a refusal answers its item. */
    private record Doc(int slot, String index, String id, String routing, BytesReference source, MediaType type, boolean answers) {}

    /**
     * Map the new fields of the documents {@code bulk} writes to the indices
     * in {@code infino}, then hand {@code then} those indices as the cluster
     * state now holds them, on the write pool. An item whose document the
     * mapper refuses, or whose mapping update fails, gets its answer in
     * {@code answered} and is not written. With no node services (a unit
     * test) or no local index service, nothing is mapped.
     */
    static void apply(
        BulkRequest bulk,
        List<String> concrete,
        Map<String, IndexMetadata> infino,
        BulkItemResponse[] answered,
        ActionListener<Map<String, IndexMetadata>> then
    ) {
        ClusterService clusters = InfinoServices.clusterService();
        IndicesService indices = InfinoServices.indicesService();
        MappingUpdatedAction updates = InfinoServices.mappingUpdatedAction();
        if (clusters == null || indices == null || updates == null) {
            then.onResponse(infino);
            return;
        }
        List<Doc> docs = new ArrayList<>();
        for (int slot = 0; slot < bulk.requests().size(); slot++) {
            String index = concrete.get(slot);
            if (answered[slot] != null || index == null || !infino.containsKey(index)) {
                continue;
            }
            DocWriteRequest<?> item = bulk.requests().get(slot);
            if (item instanceof IndexRequest write) {
                docs.add(new Doc(slot, index, write.id(), write.routing(), write.source(), write.getContentType(), true));
            } else if (item instanceof UpdateRequest update) {
                // The partial document and the upsert hold the fields an update adds; the
                // document they merge into was mapped when it was written. Either alone
                // may lack what the merged one has, so a refusal here answers nothing:
                // the merged document is checked when its batch is written, as before.
                if (update.doc() != null) {
                    docs.add(new Doc(slot, index, update.id(), update.routing(), update.doc().source(), update.doc().getContentType(), false));
                }
                if (update.upsertRequest() != null) {
                    IndexRequest upsert = update.upsertRequest();
                    docs.add(new Doc(slot, index, update.id(), update.routing(), upsert.source(), upsert.getContentType(), false));
                }
            }
        }
        if (docs.isEmpty()) {
            then.onResponse(infino);
            return;
        }
        round(docs, false, new HashSet<>(), new IdentityHashMap<>(), bulk, infino, answered, clusters, indices, updates, then);
    }

    /**
     * One round: parse the documents with a new field against the mapping
     * this node holds, merge their updates into one per index (a document
     * whose update conflicts with one merged before waits for the next
     * round, when the first is mapped, as it would follow it in stock),
     * send them, and once this node holds them run the next round over the
     * documents that waited. Merging two updates does not check the values
     * the documents hold, so when several documents' updates went as one,
     * those documents are parsed again in the next round, against the mapping
     * they made: one whose value the first document's field cannot take is
     * refused then, as stock refuses the later document. An index in
     * {@code single} sends one document's update per round, so a refused
     * update is answered to its own document.
     */
    private static void round(
        List<Doc> docs,
        boolean recheck,
        Set<String> single,
        Map<Doc, Integer> rounds,
        BulkRequest bulk,
        Map<String, IndexMetadata> infino,
        BulkItemResponse[] answered,
        ClusterService clusters,
        IndicesService indices,
        MappingUpdatedAction updates,
        ActionListener<Map<String, IndexMetadata>> then
    ) {
        ClusterState state = clusters.state();
        Map<String, Mapping> byIndex = new LinkedHashMap<>();
        Map<String, List<Doc>> contributors = new HashMap<>();
        Map<String, Long> versions = new HashMap<>();
        Map<String, Unmapped> unmapped = new HashMap<>();
        List<Doc> waiting = new ArrayList<>();
        for (Doc doc : docs) {
            IndexMetadata md = state.metadata().index(doc.index());
            IndexService service = md == null ? null : indices.indexService(md.getIndex());
            if (service == null) {
                // Not on the node that writes the table: nothing to parse against.
                log.debug("[infino] no index service for {} here; its new fields are typed by the table", doc.index());
                continue;
            }
            MapperService mappers = service.mapperService();
            Unmapped held = unmapped.computeIfAbsent(doc.index(), name -> Unmapped.of(md, mappers));
            Mapping update;
            try {
                update = update(doc, mappers, held, recheck);
            } catch (Exception e) {
                if (doc.answers()) {
                    answered[doc.slot()] = failure(bulk, doc, e);
                } else {
                    log.debug("[infino] the update of {} in {} adds no mapping: {}", doc.id(), doc.index(), e.getMessage());
                }
                continue;
            }
            if (update == null || rounds.getOrDefault(doc, 0) >= MAX_ROUNDS) {
                continue;
            }
            Mapping merged = byIndex.get(doc.index());
            if (merged != null && single.contains(doc.index())) {
                waiting.add(doc);
                continue;
            }
            try {
                merged = merged == null ? update : merged.merge(update, MapperService.MergeReason.MAPPING_UPDATE);
            } catch (IllegalArgumentException e) {
                waiting.add(doc);
                continue;
            }
            rounds.merge(doc, 1, Integer::sum);
            byIndex.put(doc.index(), merged);
            contributors.computeIfAbsent(doc.index(), k -> new ArrayList<>()).add(doc);
            versions.putIfAbsent(doc.index(), md.getMappingVersion());
        }
        if (byIndex.isEmpty()) {
            then.onResponse(current(clusters.state(), infino));
            return;
        }
        AtomicInteger pending = new AtomicInteger(byIndex.size());
        Map<String, Long> sent = new HashMap<>();
        for (Map.Entry<String, List<Doc>> e : contributors.entrySet()) {
            if (e.getValue().size() > 1) {
                waiting.addAll(e.getValue());
            }
        }
        Runnable sentAll = () -> awaitMappings(
            sent,
            bulk.timeout(),
            clusters,
            then,
            () -> round(waiting, true, single, rounds, bulk, infino, answered, clusters, indices, updates, then)
        );
        for (Map.Entry<String, Mapping> e : byIndex.entrySet()) {
            String index = e.getKey();
            IndexMetadata md = state.metadata().index(index);
            send(updates, md, e.getValue(), ActionListener.wrap(ok -> {
                synchronized (sent) {
                    sent.put(index, versions.get(index));
                }
                if (pending.decrementAndGet() == 0) {
                    sentAll.run();
                }
            }, failure -> {
                List<Doc> from = contributors.get(index);
                synchronized (waiting) {
                    if (from.size() > 1) {
                        // Which document's field was refused is not known: they go again
                        // (already queued) one at a time, each answered on its own.
                        log.debug("[infino] mapping update for {} refused, retried per document: {}", index, failure.getMessage());
                        single.add(index);
                    } else {
                        for (Doc doc : from) {
                            if (doc.answers()) {
                                answered[doc.slot()] = failure(bulk, doc, failure);
                            }
                        }
                    }
                }
                if (pending.decrementAndGet() == 0) {
                    sentAll.run();
                }
            }));
        }
    }

    /**
     * The mapping update {@code doc} needs, or null when it adds no field.
     * Its fields are walked first, and only a document with a field the
     * mapping does not hold is parsed by the document mapper; the fields the
     * table already holds unmapped are taken out of it first. A document
     * {@code recheck}ed after another's update mapped its fields is always
     * parsed: the walk reads names, and its values must fit that mapping.
     */
    private static Mapping update(Doc doc, MapperService mappers, Unmapped held, boolean recheck) throws IOException {
        if (doc.source() == null) {
            return null;
        }
        MediaType type = doc.type() == null ? MediaTypeRegistry.JSON : doc.type();
        Scan scan = new Scan(mappers, held);
        try (XContentParser parser = parser(doc.source(), type)) {
            boolean added = parser.nextToken() != XContentParser.Token.START_OBJECT || scan.object(parser, "");
            if (!added && !recheck) {
                return null;
            }
        } catch (IOException | RuntimeException e) {
            // Not a document the walk can read: the mapper reports it as stock does.
        }
        BytesReference source = doc.source();
        if (!held.columns().isEmpty()) {
            // The walk may have stopped before reaching them.
            source = without(source, type, held.columns());
            type = MediaTypeRegistry.JSON;
        }
        DocumentMapperForType mapper = mappers.documentMapperWithAutoCreate();
        String id = doc.id() == null ? PLACEHOLDER_ID : doc.id();
        ParsedDocument parsed = mapper.getDocumentMapper().parse(new SourceToParse(doc.index(), id, source, type, doc.routing()));
        if (mapper.getMapping() != null) {
            // The index has no mapping yet: the mapper made one, which goes with the update.
            parsed.addDynamicMappingsUpdate(mapper.getMapping());
        }
        return parsed.dynamicMappingsUpdate();
    }

    /**
     * The columns the table holds that the mapping does not name, and every
     * object path above them: a field there is left to the table as before.
     */
    private record Unmapped(Set<String> columns, Set<String> objects) {
        static Unmapped of(IndexMetadata md, MapperService mappers) {
            Set<String> columns = new HashSet<>();
            Set<String> objects = new HashSet<>();
            for (String column : InfinoNative.columns(InfinoNative.tableFor(md.getIndex(), md.getSettings())).keySet()) {
                if (mappers.fieldType(column) != null) {
                    continue;
                }
                columns.add(column);
                for (int dot = column.indexOf('.'); dot > 0; dot = column.indexOf('.', dot + 1)) {
                    objects.add(column.substring(0, dot));
                }
            }
            return new Unmapped(columns, objects);
        }
    }

    /**
     * A walk over a document's fields that finds whether any is new to the
     * mapping: a value under a path no field or object maps, or an object no
     * object maps. A field's own value (an object or array it takes, a
     * geo point, a range) and an object that is not indexed are not looked
     * into, and a null or an empty array adds nothing, as in stock.
     */
    private static final class Scan {
        private final MapperService mappers;
        private final Unmapped held;

        Scan(MapperService mappers, Unmapped held) {
            this.mappers = mappers;
            this.held = held;
        }

        /** Whether the object the parser is in (at its start) holds a new field. */
        boolean object(XContentParser parser, String prefix) throws IOException {
            XContentParser.Token token;
            while ((token = parser.nextToken()) == XContentParser.Token.FIELD_NAME) {
                String name = parser.currentName();
                String path = prefix.isEmpty() ? name : prefix + "." + name;
                if (value(parser, parser.nextToken(), path)) {
                    return true;
                }
            }
            return false;
        }

        private boolean value(XContentParser parser, XContentParser.Token token, String path) throws IOException {
            if (token == XContentParser.Token.VALUE_NULL) {
                return false;
            }
            if (mappers.fieldType(path) != null) {
                parser.skipChildren();
                return false;
            }
            if (token == XContentParser.Token.START_OBJECT) {
                ObjectMapper object = mappers.getObjectMapper(path);
                if (object != null && !object.isEnabled()) {
                    parser.skipChildren();
                    return false;
                }
                if (object == null && !held.objects().contains(path)) {
                    return true;
                }
                return object(parser, path);
            }
            if (token == XContentParser.Token.START_ARRAY) {
                while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                    if (value(parser, token, path)) {
                        return true;
                    }
                }
                return false;
            }
            if (held.columns().contains(path)) {
                return false;
            }
            return true;
        }
    }

    private static XContentParser parser(BytesReference source, MediaType type) throws IOException {
        return XContentHelper.createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, source, type);
    }

    /** {@code source} as JSON without the fields at {@code paths}; an object left empty by that goes too. */
    @SuppressWarnings("unchecked")
    static BytesReference without(BytesReference source, MediaType type, Set<String> paths) throws IOException {
        Map<String, Object> map = XContentHelper.convertToMap(source, true, type).v2();
        prune(map, "", paths);
        try (XContentBuilder b = XContentFactory.jsonBuilder()) {
            b.map(map);
            return BytesReference.bytes(b);
        }
    }

    @SuppressWarnings("unchecked")
    private static void prune(Map<String, Object> map, String prefix, Set<String> paths) {
        for (Iterator<Map.Entry<String, Object>> it = map.entrySet().iterator(); it.hasNext();) {
            Map.Entry<String, Object> e = it.next();
            String path = prefix.isEmpty() ? e.getKey() : prefix + "." + e.getKey();
            if (paths.contains(path)) {
                it.remove();
            } else if (e.getValue() instanceof Map<?, ?> inner) {
                prune((Map<String, Object>) inner, path, paths);
                if (inner.isEmpty()) {
                    it.remove();
                }
            } else if (e.getValue() instanceof List<?> list) {
                for (Object element : list) {
                    if (element instanceof Map<?, ?> inner) {
                        prune((Map<String, Object>) inner, path, paths);
                    }
                }
            }
        }
    }

    /**
     * Send {@code update} for {@code md}'s index as a shard sends a dynamic
     * update, as the node: the bulk's own user was already allowed to write
     * the index, which is the right a dynamic field needs in stock.
     */
    private static void send(MappingUpdatedAction updates, IndexMetadata md, Mapping update, ActionListener<Void> listener) {
        ThreadContext context = InfinoServices.threadContext();
        if (context == null) {
            updates.updateMappingOnClusterManager(md.getIndex(), update, listener);
            return;
        }
        ActionListener<Void> inCallerContext = new ContextPreservingActionListener<>(context.newRestorableContext(false), listener);
        try (ThreadContext.StoredContext ignored = context.stashContext()) {
            updates.updateMappingOnClusterManager(md.getIndex(), update, inCallerContext);
        }
    }

    /**
     * Wait until this node holds a mapping newer than the one each index in
     * {@code sent} was parsed against, then run {@code next} on the write
     * pool; past {@code timeout}, fail. The update is acknowledged once the
     * cluster manager has published it, not once this node applied it, so
     * the bulk waits for that as a stock shard does.
     */
    private static void awaitMappings(
        Map<String, Long> sent,
        TimeValue timeout,
        ClusterService clusters,
        ActionListener<Map<String, IndexMetadata>> then,
        Runnable next
    ) {
        Runnable onWritePool = () -> InfinoServices.fork(ThreadPool.Names.WRITE, then::onFailure, next);
        Map<String, Long> versions;
        synchronized (sent) {
            versions = Map.copyOf(sent);
        }
        if (versions.isEmpty() || applied(clusters.state(), versions)) {
            onWritePool.run();
            return;
        }
        ThreadContext context = InfinoServices.threadContext();
        ClusterStateObserver observer = new ClusterStateObserver(clusters, timeout, log, context);
        observer.waitForNextChange(new ClusterStateObserver.Listener() {
            @Override
            public void onNewClusterState(ClusterState state) {
                onWritePool.run();
            }

            @Override
            public void onClusterServiceClose() {
                then.onFailure(new NodeClosedException(clusters.localNode()));
            }

            @Override
            public void onTimeout(TimeValue waited) {
                then.onFailure(new IllegalStateException("[infino] timed out after [" + waited + "] waiting for the mapping update of " + versions.keySet()));
            }
        }, state -> applied(state, versions));
    }

    /** Whether {@code state} holds, for each index, a mapping newer than the version given (or the index is gone). */
    private static boolean applied(ClusterState state, Map<String, Long> versions) {
        for (Map.Entry<String, Long> e : versions.entrySet()) {
            IndexMetadata md = state.metadata().index(e.getKey());
            if (md != null && md.getMappingVersion() <= e.getValue()) {
                return false;
            }
        }
        return true;
    }

    /** The indices of {@code infino} as {@code state} holds them now, the ones it no longer holds as they were. */
    private static Map<String, IndexMetadata> current(ClusterState state, Map<String, IndexMetadata> infino) {
        Map<String, IndexMetadata> out = new LinkedHashMap<>();
        for (Map.Entry<String, IndexMetadata> e : infino.entrySet()) {
            IndexMetadata md = state.metadata().index(e.getKey());
            out.put(e.getKey(), md != null && md.getIndexUUID().equals(e.getValue().getIndexUUID()) ? md : e.getValue());
        }
        return out;
    }

    private static BulkItemResponse failure(BulkRequest bulk, Doc doc, Exception cause) {
        DocWriteRequest<?> item = bulk.requests().get(doc.slot());
        return new BulkItemResponse(doc.slot(), item.opType(), new BulkItemResponse.Failure(doc.index(), item.id(), cause));
    }
}
