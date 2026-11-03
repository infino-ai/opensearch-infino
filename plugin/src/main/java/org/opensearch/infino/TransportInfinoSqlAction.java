/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import org.opensearch.action.ActionRunnable;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

/**
 * Runs {@link InfinoSqlAction} on the node that took it, once the security
 * plugin has authorized it, on the search pool as a search runs.
 */
public final class TransportInfinoSqlAction extends HandledTransportAction<InfinoSqlRequest, InfinoSqlResponse> {

    @Inject
    public TransportInfinoSqlAction(TransportService transportService, ActionFilters actionFilters) {
        super(InfinoSqlAction.NAME, transportService, actionFilters, InfinoSqlRequest::new);
    }

    @Override
    protected void doExecute(Task task, InfinoSqlRequest request, ActionListener<InfinoSqlResponse> listener) {
        ThreadPool pool = InfinoServices.threadPool();
        if (pool == null) {
            listener.onFailure(new IllegalStateException("[infino] the node is not ready"));
            return;
        }
        pool.executor(ThreadPool.Names.SEARCH).execute(ActionRunnable.supply(listener, () -> new InfinoSqlResponse(InfinoSql.run(request))));
    }
}
