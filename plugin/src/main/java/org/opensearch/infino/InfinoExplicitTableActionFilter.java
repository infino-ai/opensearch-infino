/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.action.admin.indices.rollover.RolloverRequest;
import org.opensearch.action.admin.indices.shrink.ResizeRequest;
import org.opensearch.action.admin.indices.template.put.PutComponentTemplateAction;
import org.opensearch.action.admin.indices.template.put.PutComposableIndexTemplateAction;
import org.opensearch.action.admin.indices.template.put.PutIndexTemplateRequest;
import org.opensearch.action.support.ActionFilter;
import org.opensearch.action.support.ActionFilterChain;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.IndexTemplateMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.metadata.MetadataIndexTemplateService;
import org.opensearch.cluster.metadata.Template;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Holds a request that names an index's table ({@code index.infino.table})
 * to the right to manage the cluster: an index created with the setting
 * (directly, as the target of a resize, or as the new index of a rollover),
 * or from a template that sets it, is served from the table it names, so a
 * caller who may create an index but not manage the cluster would otherwise
 * choose which rows that index reads and writes. The right is asked of the
 * cluster under the caller's own identity, as the account route asks it
 * ({@link InfinoClient#requireClusterManagement}). A request with no user
 * (no security plugin, or the node's own work, such as the registry
 * bringing an index back) passes as it came.
 *
 * <p>The registry's table is refused here by name, so such a create fails
 * with the reason rather than the index coming up refused; a table another
 * live index is served from is refused when the index is served (see
 * {@link InfinoTables}), since a template's settings meet the index only
 * when it is created. Whether an index was free to take its table
 * ({@link InfinoPlugin#INFINO_TABLE_CLAIMED}) is the cluster's to say: a
 * request or template that sets it is refused, from any caller.
 */
public class InfinoExplicitTableActionFilter implements ActionFilter {

    private static final Logger log = LogManager.getLogger(InfinoExplicitTableActionFilter.class);

    /** The prefix the cluster gives an index setting written without it. */
    private static final String INDEX_SETTING_PREFIX = "index.";

    /** The one type a mapping's fields may be written under. */
    private static final String TYPE = "_doc";

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
        Settings settings = requestedSettings(request);
        if (settings != null && settings.keySet().contains(InfinoPlugin.INFINO_TABLE_CLAIMED.getKey())) {
            listener.onFailure(
                new IllegalArgumentException(
                    "[infino] [" + InfinoPlugin.INFINO_TABLE_CLAIMED.getKey() + "] is set by the cluster when an index is created, never by a request"
                )
            );
            return;
        }
        // What an index made by this request starts with. For a create that is its templates' settings
        // and mappings under the request's own, so a template that asks for the attach, names the table
        // or adds a field is judged with the request, as the cluster will merge them.
        Made made = request instanceof CreateIndexRequest create ? merged(create, settings, InfinoServices.clusterService()) : alone(request, settings);
        if (made == null) {
            chain.proceed(task, action, request, listener);
            return;
        }
        String table = namedTable(made.settings());
        // A template gives part of an index's settings: one that only asks for an attached index is
        // completed by the request that names the table, and is judged when an index is made from it.
        String refused = isTemplate(request) ? null : attachRefusal(made.settings(), table);
        if (refused != null) {
            listener.onFailure(new IllegalArgumentException(refused));
            return;
        }
        // The right to manage the cluster is asked of a caller who names a table; one a template named
        // was held to it when the template was stored.
        String named = settings == null ? null : namedTable(settings);
        if (InfinoRegistry.TABLE.equals(named)) {
            listener.onFailure(
                new IllegalArgumentException(
                    "[infino] [" + InfinoPlugin.INFINO_TABLE.getKey() + "] names table [" + named + "], which holds the registry of the cluster's indices"
                )
            );
            return;
        }
        Runnable proceed = () -> attachThenProceed(task, action, request, made, table, listener, chain);
        if (named == null || InfinoScrollActionFilter.principal().isEmpty()) {
            proceed.run();
            return;
        }
        InfinoClient.requireClusterManagement(ActionListener.wrap(allowed -> proceed.run(), listener::onFailure));
    }

    /** The settings and the mapping an index made by a request starts with; the mapping is null where the request makes no index outright. */
    record Made(Settings settings, String mapping) {}

    /** What a request other than a create gives the indices it makes, from the request alone; null when it gives no settings. */
    private static Made alone(ActionRequest request, Settings settings) {
        if (settings == null) {
            return null;
        }
        CreateIndexRequest rolled = request instanceof RolloverRequest rollover ? rollover.getCreateIndexRequest() : null;
        return new Made(settings, rolled == null ? null : mergedMapping(List.of(mappingOf(rolled))));
    }

    /**
     * What an index a create request makes starts with: the settings and
     * mappings of the templates its name matches (a composable template with
     * its components, else the legacy templates from the lowest order up)
     * under the request's own, as the cluster merges them when it creates
     * the index. Null when neither gives any setting. Templates that cannot
     * be read leave the request's own, and the index's shard to report what
     * it finds.
     */
    static Made merged(CreateIndexRequest create, Settings requested, ClusterService clusters) {
        Settings.Builder settings = Settings.builder();
        List<String> mappings = new ArrayList<>();
        if (clusters != null) {
            try {
                ClusterState state = clusters.state();
                Metadata metadata = state.metadata();
                boolean hidden = requested != null && IndexMetadata.INDEX_HIDDEN_SETTING.get(requested);
                String composable = MetadataIndexTemplateService.findV2Template(metadata, create.index(), hidden);
                if (composable != null) {
                    settings.put(MetadataIndexTemplateService.resolveSettings(metadata, composable));
                    for (var mapping : MetadataIndexTemplateService.collectMappings(state, composable, create.index())) {
                        mappings.add(mapping.string());
                    }
                } else {
                    List<IndexTemplateMetadata> legacy = MetadataIndexTemplateService.findV1Templates(metadata, create.index(), hidden);
                    for (int i = legacy.size() - 1; i >= 0; i--) {
                        settings.put(legacy.get(i).settings());
                        if (legacy.get(i).mappings() != null) {
                            mappings.add(legacy.get(i).mappings().string());
                        }
                    }
                }
            } catch (Exception e) {
                log.debug("[infino] the templates of [{}] could not be read to judge its create request", create.index(), e);
                settings = Settings.builder();
                mappings.clear();
            }
        }
        if (requested != null) {
            settings.put(requested);
        }
        mappings.add(mappingOf(create));
        Settings all = settings.normalizePrefix(INDEX_SETTING_PREFIX).build();
        return all.isEmpty() ? null : new Made(all, mergedMapping(mappings));
    }

    private static String mappingOf(CreateIndexRequest create) {
        return create.mappings() == null || create.mappings().isEmpty() ? "{}" : create.mappings();
    }

    /**
     * Mappings as one, each over those before it, as JSON: a mapping names
     * its fields under {@code properties}, with or without the {@code _doc}
     * type around them, and a later one adds to or replaces an earlier one's
     * field by field.
     */
    @SuppressWarnings("unchecked")
    static String mergedMapping(List<String> mappings) {
        Map<String, Object> out = new LinkedHashMap<>();
        for (String json : mappings) {
            Map<String, Object> mapping = InfinoNative.object(json);
            if (mapping.size() == 1 && mapping.get(TYPE) instanceof Map<?, ?> typed) {
                mapping = (Map<String, Object>) typed;
            }
            over(out, mapping);
        }
        try (XContentBuilder b = XContentFactory.jsonBuilder()) {
            b.map(out);
            return BytesReference.bytes(b).utf8ToString();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @SuppressWarnings("unchecked")
    private static void over(Map<String, Object> under, Map<String, Object> top) {
        for (Map.Entry<String, Object> e : top.entrySet()) {
            if (e.getValue() instanceof Map<?, ?> inner && under.get(e.getKey()) instanceof Map<?, ?> held) {
                Map<String, Object> both = new LinkedHashMap<>((Map<String, Object>) held);
                over(both, (Map<String, Object>) inner);
                under.put(e.getKey(), both);
            } else {
                under.put(e.getKey(), e.getValue());
            }
        }
    }

    /**
     * Pass the request on, unless it creates an attached index
     * ({@link InfinoPlugin#INFINO_ATTACHED}) over a table that cannot be
     * attached to: one that is missing, that the plugin wrote, or that
     * disagrees with the mapping the index starts with. The index would be
     * created and then fail every search, with the reason only in the
     * node's log; the request is answered with it instead. The table is
     * read off the calling thread. A caller who names the table has been
     * held to the right to manage the cluster by now, so the answer tells no
     * one else which tables exist; a table a template names is one its
     * author chose.
     */
    private static <Request extends ActionRequest, Response extends ActionResponse> void attachThenProceed(
        Task task,
        String action,
        Request request,
        Made made,
        String table,
        ActionListener<Response> listener,
        ActionFilterChain<Request, Response> chain
    ) {
        if (made.mapping() == null || table == null || !InfinoPlugin.INFINO_ATTACHED.get(made.settings())) {
            chain.proceed(task, action, request, listener);
            return;
        }
        InfinoServices.fork(ThreadPool.Names.GENERIC, listener::onFailure, () -> {
            String refused = InfinoNative.attachRefusal(table, made.mapping());
            if (refused != null) {
                listener.onFailure(new IllegalArgumentException("[infino] " + refused));
                return;
            }
            chain.proceed(task, action, request, listener);
        });
    }

    /** Whether the request stores a template, whose settings reach an index only when one is created from it. */
    private static boolean isTemplate(ActionRequest request) {
        return request instanceof PutIndexTemplateRequest
            || request instanceof PutComposableIndexTemplateAction.Request
            || request instanceof PutComponentTemplateAction.Request;
    }

    /**
     * Why the settings of a request that asks for an attached index
     * ({@link InfinoPlugin#INFINO_ATTACHED}) cannot be served, or null when
     * they can or ask for none: an attached index names the table it reads
     * and is served in proxy mode.
     */
    static String attachRefusal(Settings requested, String table) {
        if (!InfinoPlugin.INFINO_ATTACHED.get(requested)) {
            return null;
        }
        String attached = InfinoPlugin.INFINO_ATTACHED.getKey();
        if (table == null) {
            return "[infino] [" + attached + "] needs [" + InfinoPlugin.INFINO_TABLE.getKey() + "] to name the table the index is attached to";
        }
        if (requested.hasValue(InfinoPlugin.INFINO_MODE.getKey()) && !InfinoPlugin.MODE_PROXY.equals(InfinoPlugin.INFINO_MODE.get(requested))) {
            return "[infino] [" + attached + "] needs [" + InfinoPlugin.INFINO_MODE.getKey() + "] to be [" + InfinoPlugin.MODE_PROXY + "]";
        }
        return null;
    }

    /** The table a create, resize, rollover or template request names in {@code index.infino.table}; null when it names none. */
    static String namedTable(ActionRequest request) {
        Settings settings = requestedSettings(request);
        return settings == null ? null : namedTable(settings);
    }

    private static String namedTable(Settings requested) {
        String table = InfinoPlugin.INFINO_TABLE.get(requested);
        return table.isEmpty() ? null : table;
    }

    /**
     * The index settings a create, resize, rollover or template request
     * gives the indices it makes, each under its {@code index.} key; null
     * when it gives none or is no such request.
     */
    static Settings requestedSettings(ActionRequest request) {
        Settings settings = null;
        if (request instanceof CreateIndexRequest create) {
            settings = create.settings();
        } else if (request instanceof ResizeRequest resize && resize.getTargetIndexRequest() != null) {
            settings = resize.getTargetIndexRequest().settings();
        } else if (request instanceof RolloverRequest rollover && rollover.getCreateIndexRequest() != null) {
            settings = rollover.getCreateIndexRequest().settings();
        } else if (request instanceof PutIndexTemplateRequest legacy) {
            settings = legacy.settings();
        } else if (request instanceof PutComposableIndexTemplateAction.Request composable && composable.indexTemplate() != null) {
            settings = settingsOf(composable.indexTemplate().template());
        } else if (request instanceof PutComponentTemplateAction.Request component && component.componentTemplate() != null) {
            settings = settingsOf(component.componentTemplate().template());
        }
        if (settings == null || settings.isEmpty()) {
            return null;
        }
        return Settings.builder().put(settings).normalizePrefix(INDEX_SETTING_PREFIX).build();
    }

    private static Settings settingsOf(Template template) {
        return template == null ? null : template.settings();
    }
}
