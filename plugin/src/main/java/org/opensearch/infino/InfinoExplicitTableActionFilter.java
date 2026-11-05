/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.admin.indices.create.CreateIndexRequest;
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
 * to the right to manage the cluster: an index created with the setting, or
 * from a template that sets it, is served from the table it names, so a
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
 * when it is created.
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
        String table = namedTable(request);
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

    /** The table a create or template request names in {@code index.infino.table}; null when it names none. */
    static String namedTable(ActionRequest request) {
        Settings settings = null;
        if (request instanceof CreateIndexRequest create) {
            settings = create.settings();
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
        Settings normalized = Settings.builder().put(settings).normalizePrefix(INDEX_SETTING_PREFIX).build();
        String table = InfinoPlugin.INFINO_TABLE.get(normalized);
        return table.isEmpty() ? null : table;
    }

    private static Settings settingsOf(Template template) {
        return template == null ? null : template.settings();
    }
}
