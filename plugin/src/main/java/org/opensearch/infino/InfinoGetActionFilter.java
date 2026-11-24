/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionListenerResponseHandler;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionType;
import org.opensearch.action.UnavailableShardsException;
import org.opensearch.action.get.GetAction;
import org.opensearch.action.get.GetRequest;
import org.opensearch.action.get.GetResponse;
import org.opensearch.action.support.ActionFilter;
import org.opensearch.action.support.ActionFilterChain;
import org.opensearch.action.support.ContextPreservingActionListener;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.routing.OperationRouting;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.document.DocumentField;
import org.opensearch.common.lucene.uid.Versions;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.engine.VersionConflictEngineException;
import org.opensearch.index.get.GetResult;
import org.opensearch.index.mapper.IgnoredFieldMapper;
import org.opensearch.index.mapper.RoutingFieldMapper;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.search.fetch.subphase.FetchSourceContext;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Serves {@code GET /index/_doc/id} for an Infino-backed index from the
 * table, through the id column's term index, before the request is routed to
 * the shard that holds nothing. The index may be named through an alias.
 * {@code _source} includes and excludes are applied.
 *
 * <p>A default-mode document carries the version, sequence number and primary
 * term its shard reports for it, as a stock get does, read from the shard's
 * skeleton together with the document's row ({@link #reads}): a write
 * conditioned on what the get reported is checked against the same numbers.
 * Only the node holding the shard's primary sees the writes the table has not
 * taken yet, so a get that reaches another node is sent on to that one. A
 * proxy-mode document carries the sequence number the table stored with it,
 * at version 1 (see {@link InfinoDocumentVersion}).
 */
public class InfinoGetActionFilter implements ActionFilter {

    private static final Logger log = LogManager.getLogger(InfinoGetActionFilter.class);
    /**
     * The header of a read sent on to the node holding its shard's primary.
     * Only a node sets it: it is no REST header, so no client request carries it.
     */
    static final String FORWARDED = "_infino_read_forwarded";
    /**
     * How many times documents are read again when a write to one of them
     * lands while they are read (see {@link #reads}).
     */
    private static final int READ_TRIES = 3;

    private final Supplier<ClusterService> clusterService;
    private final Supplier<IndexNameExpressionResolver> resolver;
    private final InfinoTables tables;

    public InfinoGetActionFilter(Supplier<ClusterService> clusterService, Supplier<IndexNameExpressionResolver> resolver, InfinoTables tables) {
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
        if (!GetAction.NAME.equals(action) || !(request instanceof GetRequest get)) {
            chain.proceed(task, action, request, listener);
            return;
        }
        ClusterService clusters = clusterService.get();
        ClusterState state = clusters == null ? null : clusters.state();
        IndexMetadata index = state == null ? null : infinoIndex(state, resolver.get(), get.index());
        if (index == null) {
            chain.proceed(task, action, request, listener);
            return;
        }
        String stored = storedFieldAsked(index, get.storedFields());
        if (stored != null) {
            listener.onFailure(InfinoSearchChecks.storedFieldRefused(stored, index.getIndex().getName()));
            return;
        }
        boolean defaultMode = InfinoPlugin.inDefaultMode(index.getSettings());
        if (defaultMode) {
            Route route = route(state, shardOf(index, get.id(), get.routing()));
            if (route.failure() != null) {
                listener.onFailure(route.failure());
                return;
            }
            if (route.node() != null) {
                @SuppressWarnings("unchecked")
                ActionListener<GetResponse> answer = (ActionListener<GetResponse>) listener;
                forward(route.node(), GetAction.INSTANCE, get, answer);
                return;
            }
        }
        // The table is read on the get pool, never on the thread that took the request.
        InfinoServices.fork(ThreadPool.Names.GET, listener::onFailure, () -> {
            try {
                // The document as the security plugin lets this user see it: under a
                // document rule it is looked up through the rule, and its source is
                // cut to the fields the user may read.
                InfinoAccess access = InfinoAccess.reading(index.getIndex().getName(), "get");
                String table = tables.ensureForDocument(index, get.id(), get.routing());
                if (get.refresh()) {
                    refresh(index, table);
                }
                InfinoNative.Doc doc;
                InfinoEngine.Written written = null;
                if (defaultMode) {
                    Read read = reads(table, List.of(get.id()), skeleton(table, get.realtime()), docReader(index, table, access, get.realtime())).get(
                        get.id()
                    );
                    doc = read.doc();
                    written = read.written();
                } else {
                    doc = access.restrictsDocuments() ? restrictedDocs(table, List.of(get.id()), access).get(get.id()) : docOf(index, table, get.id(), get.realtime());
                }
                // In proxy mode a found document is at the version its writes report for
                // every document; a default-mode get is at the version its shard reports.
                if (doc != null
                    && !defaultMode
                    && get.versionType().isVersionConflictForReads(InfinoDocumentVersion.VERSION, get.version())) {
                    listener.onFailure(
                        new VersionConflictEngineException(
                            new ShardId(index.getIndex(), 0),
                            get.id(),
                            get.versionType().explainConflictForReads(InfinoDocumentVersion.VERSION, get.version())
                        )
                    );
                    return;
                }
                @SuppressWarnings("unchecked")
                Response response = (Response) new GetResponse(
                    result(
                        index.getIndex().getName(),
                        get.id(),
                        doc,
                        written,
                        access,
                        InfinoMappingSource.of(index.mapping()),
                        get.fetchSourceContext(),
                        routingOf(index, doc),
                        ignoredOf(index, table, get.id(), doc, get.realtime())
                    )
                );
                listener.onResponse(response);
            } catch (Exception e) {
                log.warn("[infino] get of {} from {} failed", get.id(), get.index(), e);
                listener.onFailure(e);
            }
        });
    }

    /** The shard of a default-mode {@code index} that holds {@code id}, written with {@code routing} (null for none). */
    static ShardId shardOf(IndexMetadata index, String id, String routing) {
        return new ShardId(index.getIndex(), OperationRouting.generateShardId(index, id, routing));
    }

    /**
     * Where this node serves a default-mode read of a shard from: here when
     * it holds the shard's primary ({@code node} and {@code failure} both
     * null), else the {@code node} that does; or the {@code failure} it is
     * answered with, while the shard has no active primary, or when the read
     * was sent here already and the primary has moved on since.
     */
    record Route(DiscoveryNode node, Exception failure) {}

    /** {@link Route} of a default-mode read of {@code shard}, by {@code state}. */
    static Route route(ClusterState state, ShardId shard) {
        String primary = InfinoBulkActionFilter.writerNode(state, shard);
        if (primary == null) {
            return new Route(null, new UnavailableShardsException(shard, "primary shard is not active"));
        }
        if (primary.equals(state.nodes().getLocalNodeId())) {
            return new Route(null, null);
        }
        DiscoveryNode node = state.nodes().get(primary);
        if (node == null || forwarded()) {
            return new Route(null, new UnavailableShardsException(shard, "the primary moved to another node while the read was sent on to this one"));
        }
        return new Route(node, null);
    }

    /** Whether the read this thread serves was sent here by another node. */
    private static boolean forwarded() {
        ThreadContext context = InfinoServices.threadContext();
        return context != null && context.getHeader(FORWARDED) != null;
    }

    /**
     * Send {@code request} to {@code node}, which holds its shard's primary,
     * marked forwarded, so it is served there and sent on no further. It goes
     * as the caller: the node authorizes it again, and the answer comes back
     * in the caller's context.
     */
    static <T extends ActionRequest, R extends ActionResponse> void forward(DiscoveryNode node, ActionType<R> action, T request, ActionListener<R> listener) {
        TransportService transport = InfinoServices.transportService();
        ThreadContext context = InfinoServices.threadContext();
        if (transport == null || context == null) {
            listener.onFailure(new IllegalStateException("[infino] the node that holds the primary cannot be reached"));
            return;
        }
        ActionListener<R> inCallerContext = new ContextPreservingActionListener<>(context.newRestorableContext(false), listener);
        try (ThreadContext.StoredContext ignored = context.newStoredContext(true)) {
            context.putHeader(FORWARDED, "true");
            transport.sendRequest(node, action.name(), request, new ActionListenerResponseHandler<>(inCallerContext, action.getResponseReader()));
        }
    }

    /**
     * A default-mode document as a get reads it: {@code written}, the
     * version, sequence number and primary term its shard's skeleton holds
     * for it, which a stock get reports, and {@code doc}, its row (each null
     * when no document is found).
     */
    record Read(InfinoEngine.Written written, InfinoNative.Doc doc) {}

    /**
     * How a default-mode get reads an id's {@link InfinoEngine.Written} from
     * the shard behind {@code table} on this node, realtime or not as the
     * request asks.
     */
    static Function<String, InfinoEngine.Written> skeleton(String table, boolean realtime) {
        return id -> InfinoEngine.written(table, id, realtime);
    }

    /**
     * How a default-mode get reads documents' rows from {@code table}, by id:
     * a realtime read counts a write the shard has not committed yet, and one
     * that is not sees what the latest refresh made visible. Under a document
     * rule the rows are read through the rule, which sees only what the table
     * holds, so a realtime read first commits a write of those ids the table
     * has not taken.
     */
    static Function<Collection<String>, Map<String, InfinoNative.Doc>> docReader(IndexMetadata index, String table, InfinoAccess access, boolean realtime) {
        if (access.restrictsDocuments()) {
            return ids -> {
                if (realtime && ids.stream().anyMatch(id -> InfinoEngine.pending(table, id) != null)) {
                    refresh(index, table);
                }
                return restrictedDocs(table, ids, access);
            };
        }
        if (realtime) {
            return ids -> InfinoEngine.currentDocs(table, ids);
        }
        return ids -> {
            Map<String, InfinoNative.Doc> out = new HashMap<>();
            for (String id : ids) {
                InfinoNative.Doc doc = InfinoEngine.refreshedDoc(table, id);
                if (doc != null) {
                    out.put(id, doc);
                }
            }
            return out;
        };
    }

    /**
     * Default-mode documents of {@code ids} from {@code table}, by id, each
     * with what its shard's skeleton holds of its latest write
     * ({@code skeleton}) and its row ({@code docs}), read on the node holding
     * the shard's primary. The skeleton is read first, then the rows once
     * every write it found is recorded for the table
     * ({@link InfinoEngine#awaitRecorded}), so a row is never older than the
     * numbers reported with it; the skeleton is read again after, and a
     * document a write reached in between is read again, so the row is the
     * one those numbers belong to. Should writes keep landing, the last read
     * stands, its row as new as its numbers or newer, which a write
     * conditioned on them meets as a conflict.
     */
    static Map<String, Read> reads(
        String table,
        Collection<String> ids,
        Function<String, InfinoEngine.Written> skeleton,
        Function<Collection<String>, Map<String, InfinoNative.Doc>> docs
    ) {
        Map<String, Read> out = new HashMap<>();
        // Null values stand for a document the skeleton does not hold.
        Map<String, InfinoEngine.Written> before = new LinkedHashMap<>();
        for (String id : ids) {
            before.put(id, skeleton.apply(id));
        }
        for (int tries = 1; !before.isEmpty(); tries++) {
            List<String> found = new ArrayList<>();
            for (Map.Entry<String, InfinoEngine.Written> e : before.entrySet()) {
                if (e.getValue() != null) {
                    InfinoEngine.awaitRecorded(table, e.getKey());
                    found.add(e.getKey());
                } else {
                    out.put(e.getKey(), new Read(null, null));
                }
            }
            Map<String, InfinoNative.Doc> rows = found.isEmpty() ? Map.of() : docs.apply(found);
            Map<String, InfinoEngine.Written> moved = new LinkedHashMap<>();
            for (String id : found) {
                boolean last = tries == READ_TRIES;
                InfinoEngine.Written after = last ? null : skeleton.apply(id);
                if (last || before.get(id).equals(after)) {
                    out.put(id, new Read(before.get(id), rows.get(id)));
                } else {
                    moved.put(id, after);
                }
            }
            before = moved;
        }
        return out;
    }

    /** The first of {@code names} that {@code index} maps with {@code store: true}, or null when none is. */
    static String storedFieldAsked(IndexMetadata index, String[] names) {
        if (names == null || names.length == 0) {
            return null;
        }
        return InfinoSearchChecks.storedField(InfinoSearchChecks.properties(index.mapping()), Arrays.asList(names));
    }

    /**
     * The documents of {@code ids} the document-level rule lets the user
     * see, by id: one statement over the table with the rule as its
     * predicate. An id the rule hides is absent, as a missing document is.
     */
    static Map<String, InfinoNative.Doc> restrictedDocs(String table, Collection<String> ids, InfinoAccess access) {
        return InfinoNative.docs(table, ids, access.documentsWhere(InfinoNative.columns(table), table, System.currentTimeMillis()));
    }

    /**
     * A document as a read sees it, with the sequence number it was written
     * under: in default mode a write its shard has acknowledged and not yet
     * committed counts when {@code realtime}, as a stock get reads the
     * translog, and otherwise only what the latest refresh made visible; in
     * proxy mode an acknowledged write is in the table, visible to searches,
     * already. Null when it is missing or deleted.
     */
    static InfinoNative.Doc docOf(IndexMetadata index, String table, String id, boolean realtime) {
        if (!InfinoPlugin.inDefaultMode(index.getSettings())) {
            return InfinoNative.getDoc(table, id);
        }
        return realtime ? InfinoEngine.currentDoc(table, id) : InfinoEngine.refreshedDoc(table, id);
    }

    /** Refresh the shard behind {@code table} before a get that asks for it; proxy mode has nothing to refresh. */
    static void refresh(IndexMetadata index, String table) {
        if (InfinoPlugin.inDefaultMode(index.getSettings())) {
            InfinoEngine.refreshFor(table, "refresh_flag_get");
        }
    }

    /** The Infino-backed index a request names, directly or through an alias; null when it names something else. */
    static IndexMetadata infinoIndex(ClusterState state, IndexNameExpressionResolver resolver, String name) {
        String concrete = name;
        if (resolver != null) {
            try {
                String[] names = resolver.concreteIndexNames(state, IndicesOptions.strictSingleIndexNoExpandForbidClosed(), name);
                if (names.length == 1) {
                    concrete = names[0];
                }
            } catch (Exception e) {
                return null; // a missing index is reported the usual way
            }
        }
        IndexMetadata md = state.metadata().index(concrete);
        return md != null && md.getSettings().getAsBoolean(InfinoPlugin.INFINO_ENABLED.getKey(), false) ? md : null;
    }

    /**
     * The routing value a found default-mode document was written with, which
     * a stock get reports as {@code _routing} and the document was read with;
     * null when it was written with none, when it was not found, or in proxy
     * mode, which routes nothing.
     */
    static String routingOf(IndexMetadata index, InfinoNative.Doc doc) {
        return doc == null || !InfinoPlugin.inDefaultMode(index.getSettings()) ? null : doc.routing();
    }

    /**
     * The paths of the fields the mapper ignored in a found document, which a
     * stock get reports as {@code _ignored}: a realtime default-mode read sees
     * a pending write's; none when it was not found or ignored none.
     */
    static List<String> ignoredOf(IndexMetadata index, String table, String id, InfinoNative.Doc doc, boolean realtime) {
        if (doc == null) {
            return List.of();
        }
        return realtime && InfinoPlugin.inDefaultMode(index.getSettings()) ? InfinoEngine.currentIgnored(table, id) : InfinoNative.ignoredOf(table, id);
    }

    /**
     * A get result from the table's document (null when it is missing): its
     * source as the user may see it (the read reported to the audit log when
     * it watches the index), with the request's source filter applied, the
     * version, sequence number and primary term {@code written} has of a
     * default-mode document, as its shard reports them (null in proxy mode,
     * where the document carries the sequence number the table stored with
     * it, at the version and primary term of {@link InfinoDocumentVersion}),
     * {@code routing} (null for none) as its {@code _routing}, and
     * {@code ignored} (empty for none) as its {@code _ignored}.
     */
    static GetResult result(
        String index,
        String id,
        InfinoNative.Doc doc,
        InfinoEngine.Written written,
        InfinoAccess access,
        InfinoMappingSource stored,
        FetchSourceContext fetch,
        String routing,
        List<String> ignored
    ) {
        String source = doc == null ? null : access.read(id, doc.source());
        boolean exists = source != null && !source.isEmpty();
        BytesReference bytes = null;
        if (exists && (fetch == null || fetch.fetchSource())) {
            bytes = stored.shown(source, fetch);
        }
        Map<String, DocumentField> meta = new HashMap<>();
        if (exists && routing != null) {
            meta.put(RoutingFieldMapper.NAME, new DocumentField(RoutingFieldMapper.NAME, List.of(routing)));
        }
        if (exists && !ignored.isEmpty()) {
            meta.put(IgnoredFieldMapper.NAME, new DocumentField(IgnoredFieldMapper.NAME, new ArrayList<>(ignored)));
        }
        if (!exists) {
            return new GetResult(index, id, SequenceNumbers.UNASSIGNED_SEQ_NO, SequenceNumbers.UNASSIGNED_PRIMARY_TERM, Versions.NOT_FOUND, false, null, Map.of(), meta);
        }
        return new GetResult(
            index,
            id,
            written != null ? written.seqNo() : doc.seq(),
            written != null ? written.primaryTerm() : InfinoDocumentVersion.PRIMARY_TERM,
            written != null ? written.version() : InfinoDocumentVersion.VERSION,
            true,
            bytes,
            Map.of(),
            meta
        );
    }
}
