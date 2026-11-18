/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.action.admin.indices.rollover.RolloverRequest;
import org.opensearch.action.admin.indices.shrink.ResizeRequest;
import org.opensearch.action.admin.indices.template.put.PutComponentTemplateAction;
import org.opensearch.action.admin.indices.template.put.PutComposableIndexTemplateAction;
import org.opensearch.action.admin.indices.template.put.PutIndexTemplateRequest;
import org.opensearch.action.support.ActionFilter;
import org.opensearch.action.support.ActionFilterChain;
import org.opensearch.cluster.metadata.Template;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.tasks.Task;

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

    /** The prefix the cluster gives an index setting written without it. */
    private static final String INDEX_SETTING_PREFIX = "index.";

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
        if (settings == null) {
            chain.proceed(task, action, request, listener);
            return;
        }
        if (settings.keySet().contains(InfinoPlugin.INFINO_TABLE_CLAIMED.getKey())) {
            listener.onFailure(
                new IllegalArgumentException(
                    "[infino] [" + InfinoPlugin.INFINO_TABLE_CLAIMED.getKey() + "] is set by the cluster when an index is created, never by a request"
                )
            );
            return;
        }
        String table = namedTable(settings);
        String refused = attachRefusal(settings, table);
        if (refused != null) {
            listener.onFailure(new IllegalArgumentException(refused));
            return;
        }
        if (table == null) {
            chain.proceed(task, action, request, listener);
            return;
        }
        if (InfinoRegistry.TABLE.equals(table)) {
            listener.onFailure(
                new IllegalArgumentException(
                    "[infino] [" + InfinoPlugin.INFINO_TABLE.getKey() + "] names table [" + table + "], which holds the registry of the cluster's indices"
                )
            );
            return;
        }
        if (InfinoScrollActionFilter.principal().isEmpty()) {
            chain.proceed(task, action, request, listener);
            return;
        }
        InfinoClient.requireClusterManagement(ActionListener.wrap(allowed -> chain.proceed(task, action, request, listener), listener::onFailure));
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
