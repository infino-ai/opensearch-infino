/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestChannel;
import org.opensearch.rest.RestRequest;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

/**
 * {@code POST /_plugins/_infino/sql} with {@code {"query": "<statement>"}}:
 * one read-only SQL statement over Infino-backed indices, answered with
 * {@code {"schema": [{"name", "type"}], "rows": [..]}} (see
 * {@link InfinoSql}). The route names the indices the statement reads and
 * sends it as {@link InfinoSqlAction}, so the security plugin authorizes the
 * caller for each before anything runs. The handler that takes the request
 * is a copy per major ({@code InfinoSqlRestHandler}); this is the route.
 */
final class InfinoSqlRoute {

    private static final Logger log = LogManager.getLogger(InfinoSqlRoute.class);

    static final String ROUTE = "/_plugins/_infino/sql";
    /** The body's field holding the statement. */
    private static final String QUERY = "query";
    private static final String JSON = "application/json; charset=UTF-8";

    private InfinoSqlRoute() {}

    /** Answer {@code request} on {@code channel}, sending the statement through {@code execute} (the node client's). */
    static void handle(RestRequest request, RestChannel channel, BiConsumer<InfinoSqlRequest, ActionListener<InfinoSqlResponse>> execute) {
        InfinoSqlRequest sql;
        try {
            String statement = statement(request);
            sql = new InfinoSqlRequest(statement, InfinoNative.sqlIndices(statement).toArray(new String[0]));
        } catch (Exception e) {
            fail(channel, e);
            return;
        }
        execute.accept(sql, ActionListener.wrap(response -> channel.sendResponse(new BytesRestResponse(RestStatus.OK, JSON, response.json())), e -> fail(channel, e)));
    }

    /** The statement the body carries under {@code query}. */
    private static String statement(RestRequest request) throws IOException {
        if (!request.hasContent()) {
            throw new IllegalArgumentException("[infino] send the statement as {\"query\": \"...\"}");
        }
        Map<String, Object> body;
        try (XContentParser parser = request.contentParser()) {
            body = parser.map();
        }
        if (!(body.get(QUERY) instanceof String query) || query.isBlank() || body.size() != 1) {
            throw new IllegalArgumentException("[infino] send the statement as {\"query\": \"...\"}, and nothing else");
        }
        return query;
    }

    private static void fail(RestChannel channel, Exception e) {
        try {
            channel.sendResponse(new BytesRestResponse(channel, e));
        } catch (IOException io) {
            log.warn("[infino] the SQL route could not answer", io);
        }
    }

    /** The methods the route answers. */
    static List<RestRequest.Method> methods() {
        return List.of(RestRequest.Method.GET, RestRequest.Method.POST);
    }
}
