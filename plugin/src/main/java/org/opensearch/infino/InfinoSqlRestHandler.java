package org.opensearch.infino;

import java.util.List;

import org.opensearch.core.rest.RestStatus;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestRequest;
import org.opensearch.transport.client.node.NodeClient;

import static org.opensearch.rest.RestRequest.Method.GET;

/** Infino passthrough: /_infino/sql, /_infino/search (BM25), /_infino/knn (vector). */
public class InfinoSqlRestHandler extends BaseRestHandler {

    @Override
    public String getName() {
        return "infino_query";
    }

    @Override
    public List<Route> routes() {
        return List.of(
            new Route(GET, "/_infino/sql"),
            new Route(GET, "/_infino/search"),
            new Route(GET, "/_infino/knn"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
        final String path = request.path();
        final String table = request.param("table", "");
        final String column = request.param("column", path.endsWith("/knn") ? "emb" : "text");
        final int k = Integer.parseInt(request.param("k", "10"));
        final String q = request.param("q", "");
        final String v = request.param("v", "[]");
        final String sql = request.param("q", "SELECT 1 AS ok");
        return channel -> {
            String json;
            if (path.endsWith("/sql")) {
                json = InfinoNative.sql(sql);
            } else if (path.endsWith("/knn")) {
                json = InfinoNative.knn(table, column, v, k);
            } else {
                json = InfinoNative.bm25(table, column, q, k);
            }
            channel.sendResponse(new BytesRestResponse(RestStatus.OK, "application/json; charset=UTF-8",
                json == null ? "{\"error\":\"null\"}" : json));
        };
    }
}
