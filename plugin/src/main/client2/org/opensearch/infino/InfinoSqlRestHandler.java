/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import org.opensearch.client.node.NodeClient;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestRequest;

import java.util.List;

/**
 * The SQL route (see {@link InfinoSqlRoute}). The node client's package
 * moved in 3.0 and this handler's signature names it, so this class has a
 * copy per major; everything it does is the route's.
 */
public final class InfinoSqlRestHandler extends BaseRestHandler {

    @Override
    public String getName() {
        return "infino_sql";
    }

    @Override
    public List<Route> routes() {
        return InfinoSqlRoute.methods().stream().map(method -> new Route(method, InfinoSqlRoute.ROUTE)).toList();
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
        return channel -> InfinoSqlRoute.handle(request, channel, (sql, listener) -> client.execute(InfinoSqlAction.INSTANCE, sql, listener));
    }
}
