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
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.index.get.GetResult;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.search.fetch.subphase.FetchSourceContext;
import org.opensearch.tasks.Task;

import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Serves {@code GET /index/_doc/id} for an Infino-backed index from the
 * table, through the id column's term index, before the request is routed to
 * the shard that holds nothing. The index may be named through an alias.
 * The document carries the sequence number it was written under, which a
 * conditional write on it is checked against; {@code _source} includes and
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
            String table = tables.ensure(index);
            InfinoNative.Doc doc = access.restrictsDocuments()
                ? restrictedDocs(table, List.of(get.id()), access).get(get.id())
                : InfinoNative.getDoc(table, get.id());
            @SuppressWarnings("unchecked")
            Response response = (Response) new GetResponse(result(index.getIndex().getName(), get.id(), doc, access, get.fetchSourceContext()));
            listener.onResponse(response);
        } catch (Exception e) {
            log.warn("[infino] get of {} from {} failed", get.id(), get.index(), e);
            listener.onFailure(e);
        }
    }

    /**
     * The documents of {@code ids} the document-level rule lets the user
     * see, by id: one statement over the table with the rule as its
     * predicate. An id the rule hides is absent, as a missing document is.
     */
    static Map<String, InfinoNative.Doc> restrictedDocs(String table, List<String> ids, InfinoAccess access) {
        return InfinoNative.docs(table, ids, access.documentsWhere(InfinoNative.columns(table), table, System.currentTimeMillis()));
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
     * A get result from the table's document (null when it is missing): its
     * source as the user may see it (the read reported to the audit log when
     * it watches the index), with the request's source filter applied, and
     * the sequence number it was written under.
     */
    static GetResult result(String index, String id, InfinoNative.Doc doc, InfinoAccess access, FetchSourceContext fetch) {
        String source = doc == null ? null : access.read(id, doc.source());
        boolean exists = source != null && !source.isEmpty();
        BytesReference bytes = null;
        if (exists && (fetch == null || fetch.fetchSource())) {
            boolean includes = fetch != null && fetch.includes() != null && fetch.includes().length > 0;
            boolean excludes = fetch != null && fetch.excludes() != null && fetch.excludes().length > 0;
            bytes = includes || excludes ? InfinoSearch.filtered(source, fetch) : new BytesArray(source);
        }
        // A found document carries its sequence number, and the version and primary term the table reports (see InfinoDocumentVersion).
        return new GetResult(
            index,
            id,
            exists ? doc.seq() : SequenceNumbers.UNASSIGNED_SEQ_NO,
            exists ? InfinoDocumentVersion.PRIMARY_TERM : SequenceNumbers.UNASSIGNED_PRIMARY_TERM,
            exists ? InfinoDocumentVersion.VERSION : -1L,
            exists,
            bytes,
            Map.of(),
            Map.of()
        );
    }
}
