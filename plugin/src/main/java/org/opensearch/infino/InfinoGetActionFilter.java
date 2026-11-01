/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.get.GetAction;
import org.opensearch.action.get.GetRequest;
import org.opensearch.action.get.GetResponse;
import org.opensearch.action.support.ActionFilter;
import org.opensearch.action.support.ActionFilterChain;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.document.DocumentField;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.index.get.GetResult;
import org.opensearch.index.mapper.RoutingFieldMapper;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.search.fetch.subphase.FetchSourceContext;
import org.opensearch.tasks.Task;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Serves {@code GET /index/_doc/id} for an Infino-backed index from the
 * table, through the id column's term index, before the request is routed to
 * the shard that holds nothing. The index may be named through an alias.
 * Version and sequence number are not carried; {@code _source} includes and
 * excludes are applied.
 */
public class InfinoGetActionFilter implements ActionFilter {

    private static final Logger log = LogManager.getLogger(InfinoGetActionFilter.class);

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
        IndexMetadata index = clusters == null ? null : infinoIndex(clusters.state(), resolver.get(), get.index());
        if (index == null) {
            chain.proceed(task, action, request, listener);
            return;
        }
        try {
            // The document as the security plugin lets this user see it: under a
            // document rule it is looked up through the rule, and its source is
            // cut to the fields the user may read.
            InfinoAccess access = InfinoSecurity.access(index.getIndex().getName(), "get");
            String table = tables.ensureForDocument(index, get.id(), get.routing());
            String source = access.restrictsDocuments()
                ? restrictedSources(table, List.of(get.id()), access).get(get.id())
                : sourceOf(index, table, get.id());
            String shown = access.read(get.id(), source);
            @SuppressWarnings("unchecked")
            Response response = (Response) new GetResponse(
                result(index.getIndex().getName(), get.id(), shown, get.fetchSourceContext(), routingOf(index, table, get.id(), shown))
            );
            listener.onResponse(response);
        } catch (Exception e) {
            log.warn("[infino] get of {} from {} failed", get.id(), get.index(), e);
            listener.onFailure(e);
        }
    }

    /**
     * The sources of {@code ids} the document-level rule lets the user see,
     * by id: one statement over the table with the rule as its predicate. An
     * id the rule hides is absent, as a missing document is.
     */
    static Map<String, String> restrictedSources(String table, List<String> ids, InfinoAccess access) {
        String rule = access.documentsWhere(InfinoNative.columns(table), table, System.currentTimeMillis());
        List<String> literals = new ArrayList<>(ids.size());
        for (String id : ids) {
            literals.add(InfinoQueryTranslator.literal(id));
        }
        String sql = "SELECT " + InfinoQueryTranslator.OSID + ", " + InfinoQueryTranslator.SOURCE + " FROM " + InfinoQueryTranslator.quoteIdent(table)
            + " WHERE " + InfinoQueryTranslator.OSID + " IN (" + String.join(", ", literals) + ") AND (" + rule + ")";
        Map<String, String> out = new HashMap<>();
        for (Map<String, Object> row : InfinoNative.sqlRows(sql)) {
            Object id = row.get(InfinoQueryTranslator.OSID);
            Object source = row.get(InfinoQueryTranslator.SOURCE);
            if (id != null && source != null) {
                out.put(id.toString(), source.toString());
            }
        }
        return out;
    }

    /**
     * A document's source as a read sees it now: in default mode a write its
     * shard has acknowledged and not yet committed counts, as a stock get
     * reads the translog; in cloud mode an acknowledged write is in the
     * table already.
     */
    static String sourceOf(IndexMetadata index, String table, String id) {
        return InfinoPlugin.inDefaultMode(index.getSettings()) ? InfinoEngine.currentSource(table, id) : InfinoNative.getSource(table, id);
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
     * a stock get reports as {@code _routing}; null when it was written with
     * none, when {@code source} says it was not found, or in cloud mode,
     * which routes nothing.
     */
    static String routingOf(IndexMetadata index, String table, String id, String source) {
        if (source == null || source.isEmpty() || !InfinoPlugin.inDefaultMode(index.getSettings())) {
            return null;
        }
        return InfinoEngine.currentRouting(table, id);
    }

    /** A get result from a source string (null or empty when the document is missing), with the request's source filter applied. */
    static GetResult result(String index, String id, String source, FetchSourceContext fetch) {
        return result(index, id, source, fetch, null);
    }

    /** {@link #result(String, String, String, FetchSourceContext)} reporting {@code routing} (null for none) as the document's {@code _routing}. */
    static GetResult result(String index, String id, String source, FetchSourceContext fetch, String routing) {
        boolean exists = source != null && !source.isEmpty();
        BytesReference bytes = null;
        if (exists && (fetch == null || fetch.fetchSource())) {
            boolean includes = fetch != null && fetch.includes() != null && fetch.includes().length > 0;
            boolean excludes = fetch != null && fetch.excludes() != null && fetch.excludes().length > 0;
            bytes = includes || excludes ? InfinoSearch.filtered(source, fetch) : new BytesArray(source);
        }
        // A found document carries the constants the table reports as its version (see InfinoDocumentVersion).
        return new GetResult(
            index,
            id,
            exists ? InfinoDocumentVersion.SEQ_NO : SequenceNumbers.UNASSIGNED_SEQ_NO,
            exists ? InfinoDocumentVersion.PRIMARY_TERM : SequenceNumbers.UNASSIGNED_PRIMARY_TERM,
            exists ? InfinoDocumentVersion.VERSION : -1L,
            exists,
            bytes,
            Map.of(),
            exists && routing != null ? Map.of(RoutingFieldMapper.NAME, new DocumentField(RoutingFieldMapper.NAME, List.of(routing))) : Map.of()
        );
    }
}
