/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.support.ActionFilter;
import org.opensearch.action.support.ActionFilterChain;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.tasks.Task;

/**
 * One of the plugin's action filters, on OpenSearch releases before 3.4,
 * which call a filter without the request's metadata. A filter's work is its
 * {@link #filter}. The tree for 3.4 and later holds this class over the call
 * those releases make.
 */
abstract class InfinoActionFilter implements ActionFilter {
    @Override
    public final <Request extends ActionRequest, Response extends ActionResponse> void apply(
        Task task,
        String action,
        Request request,
        ActionListener<Response> listener,
        ActionFilterChain<Request, Response> chain
    ) {
        filter(task, action, request, listener, chain);
    }

    /** What the filter does with a request: answer it, or pass it on through {@code chain}. */
    protected abstract <Request extends ActionRequest, Response extends ActionResponse> void filter(
        Task task,
        String action,
        Request request,
        ActionListener<Response> listener,
        ActionFilterChain<Request, Response> chain
    );
}
