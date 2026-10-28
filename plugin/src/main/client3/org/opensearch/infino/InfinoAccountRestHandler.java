/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.transport.client.node.NodeClient;

import java.io.IOException;
import java.util.List;

/**
 * The account route (see {@link InfinoAccount}). The node client's package
 * moved in 3.0 and this handler's signature names it, so this class has a
 * copy per major; everything it does is the account's.
 */
public final class InfinoAccountRestHandler extends BaseRestHandler {

    private final InfinoAccount account;

    public InfinoAccountRestHandler(InfinoAccount account) {
        this.account = account;
    }

    @Override
    public String getName() {
        return "infino_account";
    }

    @Override
    public List<Route> routes() {
        return List.of(
            new Route(RestRequest.Method.GET, InfinoAccount.ROUTE),
            new Route(RestRequest.Method.GET, InfinoAccount.INDICES_ROUTE),
            new Route(RestRequest.Method.POST, InfinoAccount.INDICES_ROUTE),
            new Route(RestRequest.Method.POST, InfinoAccount.TRIAL_ROUTE),
            new Route(RestRequest.Method.PUT, InfinoAccount.ROUTE),
            new Route(RestRequest.Method.DELETE, InfinoAccount.ROUTE)
        );
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        // Parameters count as recognised only when read before the consumer runs.
        request.param("name");
        return channel -> account.handle(request, channel);
    }
}
