/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import org.opensearch.OpenSearchStatusException;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.opensearch.action.support.ActionFilter;
import org.opensearch.action.support.ActionFilterChain;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.tasks.Task;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Refuses a mapping update on an index attached to a table made outside the
 * plugin ({@link InfinoPlugin#INFINO_ATTACHED}), and one that would add a
 * text field to an Infino-backed index that its table cannot analyze as the
 * mapping asks ({@link InfinoAnalyzers#refusal}).
 *
 * <p>The mapping of an attached index is checked against its table's
 * columns when the index is declared, and a field the table has no column
 * for is refused there. A mapping update is accepted by the cluster before
 * any table is read, and a mapped field is never removed: one that named a
 * field the table lacks would leave an index whose every read fails on that
 * check, with no way back but deleting it. So the mapping is fixed when the
 * index is created, and a different one is had by creating the index again,
 * which costs nothing: the index holds no data of its own.
 *
 * <p>A text field whose analyzer splits text with a pattern tokenizer would
 * be indexed by the table with the standard analyzer, and its searches would
 * miss what stock finds with no error. Creation refuses such a mapping on the
 * releases that let a plugin check it ({@link InfinoAnalyzers#validate}); an
 * update that adds such a field is refused here, on every release.
 */
public class InfinoAttachedMappingActionFilter implements ActionFilter {

    private final Supplier<ClusterService> clusterService;
    private final Supplier<IndexNameExpressionResolver> resolver;

    public InfinoAttachedMappingActionFilter(Supplier<ClusterService> clusterService, Supplier<IndexNameExpressionResolver> resolver) {
        this.clusterService = clusterService;
        this.resolver = resolver;
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
        ClusterService clusters = clusterService.get();
        if (!(request instanceof PutMappingRequest put) || clusters == null) {
            chain.proceed(task, action, request, listener);
            return;
        }
        String attached = attachedTarget(clusters.state(), resolver.get(), put);
        if (attached != null) {
            listener.onFailure(
                new OpenSearchStatusException(
                    "[infino] index [{}] is attached to a table ([{}]) and its mapping is fixed when it is created; create the index again with the mapping it needs",
                    RestStatus.BAD_REQUEST,
                    attached,
                    InfinoPlugin.INFINO_ATTACHED.getKey()
                )
            );
            return;
        }
        String unanalyzable = analyzerRefusal(clusters.state(), resolver.get(), put);
        if (unanalyzable != null) {
            listener.onFailure(new OpenSearchStatusException(unanalyzable, RestStatus.BAD_REQUEST));
            return;
        }
        chain.proceed(task, action, request, listener);
    }

    /**
     * The first index a mapping update changes that is attached to a table;
     * null when none is, or when the request names no index the cluster
     * holds, which the cluster then reports as it does for any index.
     */
    static String attachedTarget(ClusterState state, IndexNameExpressionResolver resolver, PutMappingRequest put) {
        for (IndexMetadata md : targets(state, resolver, put)) {
            if (InfinoPlugin.INFINO_ENABLED.get(md.getSettings()) && InfinoPlugin.INFINO_ATTACHED.get(md.getSettings())) {
                return md.getIndex().getName();
            }
        }
        return null;
    }

    /**
     * Why a mapping update cannot be applied to the first Infino-backed index
     * it changes: a text field it adds, or a dynamic template it holds, whose
     * analyzer (by that index's settings) the table cannot apply. Null when
     * there is none, or the request names no index the cluster holds.
     */
    static String analyzerRefusal(ClusterState state, IndexNameExpressionResolver resolver, PutMappingRequest put) {
        String source = put.source();
        if (source == null || source.isEmpty()) {
            return null;
        }
        Map<String, Object> mapping;
        try {
            mapping = XContentHelper.convertToMap(new BytesArray(source), true, MediaTypeRegistry.JSON).v2();
        } catch (Exception e) {
            // Not a mapping the cluster can read: it refuses it on its own terms.
            return null;
        }
        for (IndexMetadata md : targets(state, resolver, put)) {
            if (!InfinoPlugin.INFINO_ENABLED.get(md.getSettings())) {
                continue;
            }
            String refused = InfinoAnalyzers.refusal(mapping, md.getSettings());
            if (refused != null) {
                return refused;
            }
        }
        return null;
    }

    /** The indices the cluster holds that a mapping update changes; none when it names none it holds. */
    private static List<IndexMetadata> targets(ClusterState state, IndexNameExpressionResolver resolver, PutMappingRequest put) {
        String[] names;
        if (put.getConcreteIndex() != null) {
            names = new String[] { put.getConcreteIndex().getName() };
        } else if (resolver == null) {
            names = put.indices();
        } else {
            try {
                names = resolver.concreteIndexNames(state, put);
            } catch (Exception e) {
                return List.of();
            }
        }
        if (names == null) {
            return List.of();
        }
        List<IndexMetadata> out = new ArrayList<>();
        for (String name : names) {
            IndexMetadata md = state.metadata().index(name);
            if (md != null) {
                out.add(md);
            }
        }
        return out;
    }
}
