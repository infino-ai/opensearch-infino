/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import org.opensearch.client.Request;
import org.opensearch.client.RequestOptions;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * A skeleton rebuild of a default-mode index with a vector field, mapped as
 * the getting-started guide maps one. The k-NN plugin writes the source of
 * each write anew before the engine sees it (its derived source), which
 * drops the mark a rebuild's creates carry: the primary knows them by their
 * content instead, and puts the mark back, so the table keeps every row as
 * it was through the rebuild, a replay of the primary's translog, and its
 * history. The table is read through SQL as a user who cannot see the vector
 * field, whose view of the index leaves it out.
 */
public class InfinoRebuildKnnIT extends InfinoRestTestCase {

    /** Rows the table holds before the index is recreated. */
    private static final int ROWS = 100;
    /** How long the skeleton may take to be rebuilt. */
    private static final long REBUILD_SECONDS = 60;
    /** The table both indices name. */
    private static final String TABLE = "kemr_rows";
    /** The engine's smallest vector, as the guide's mapping has it. */
    private static final int DIMENSION = 16;
    /** What each document's vector components step by: not a value a float holds exactly. */
    private static final double STEP = 0.1;
    /** The user the table's rows are read as, and its role; its password is the test cluster's demo one. */
    private static final String READER = "kemr_reader";
    private static final String READER_ROLE = "kemr_rows_reader";
    private static final String READER_BASIC = READER + ":admin";
    /** The demo admin password's hash, which every test user's is. */
    private static final String DEMO_HASH = "$2a$12$VcCDgh2NDk07JGN0rjGbM.Ad41qVR/YFJcgHp0UGns5JDymv..TOG";
    /** How long a read waits before it compares the table's rows (see {@link #rowIds}). */
    private static final long SETTLE_MILLIS = 3_000;

    @SuppressWarnings("unchecked")
    public void testARebuildLeavesTheTableAsItIs() throws Exception {
        createReader();
        createIndex("kemr_old", false);
        StringBuilder bulk = new StringBuilder();
        for (int i = 0; i < ROWS; i++) {
            bulk.append("{\"index\":{\"_index\":\"kemr_old\",\"_id\":\"v").append(i).append("\"}}\n");
            bulk.append("{\"service\":\"api\",\"status\":")
                .append(200 + i % 5)
                .append(",\"latency_ms\":")
                .append(i * STEP)
                .append(",\"message\":\"request ")
                .append(i)
                .append("\",\"message_embedding\":")
                .append(vector(i))
                .append("}\n");
        }
        assertEquals(false, call("POST", "/_bulk?refresh=true", bulk.toString()).get("errors"));
        Map<String, Object> before = rowIds("kemr_old");
        assertEquals(ROWS, before.size());
        call("DELETE", "/kemr_old", null);

        createIndex("kemr", true);
        assertBusy(() -> {
            Map<String, Object> index = (Map<String, Object>) callList("GET", "/_cat/indices/kemr?format=json&h=docs.count", null).get(0);
            assertEquals(String.valueOf(ROWS), index.get("docs.count"));
        }, REBUILD_SECONDS, TimeUnit.SECONDS);
        assertEquals("every row keeps its engine id through the rebuild", before, rowIds("kemr"));

        // The primary started again from its own store replays the rebuild's
        // creates from its translog, then writes the table its history.
        Map<String, Object> indices = (Map<String, Object>) call("GET", "/kemr/_stats/translog", null).get("indices");
        Map<String, Object> translog = (Map<String, Object>) ((Map<String, Object>) ((Map<String, Object>) indices.get("kemr")).get("primaries")).get("translog");
        assertEquals("every create waits in the translog for the replay", ROWS, ((Number) translog.get("uncommitted_operations")).intValue());
        List<Object> shard = callList("GET", "/_cat/shards/kemr?format=json&h=node", null);
        String node = ((Map<String, Object>) shard.get(0)).get("node").toString();
        call(
            "POST",
            "/_cluster/reroute",
            "{\"commands\":[{\"cancel\":{\"index\":\"kemr\",\"shard\":0,\"node\":\"" + node + "\",\"allow_primary\":true}}]}"
        );
        call("GET", "/_cluster/health/kemr?wait_for_status=green&timeout=60s", null);
        assertEquals("and through the primary's start from its store", before, rowIds("kemr"));

        call("POST", "/kemr/_close", null);
        call("POST", "/kemr/_open?wait_for_active_shards=1", null);
        call("GET", "/_cluster/health/kemr?wait_for_status=green&timeout=60s", null);
        assertEquals("and through the index's reopening", before, rowIds("kemr"));

        Map<String, Object> hits = (Map<String, Object>) call(
            "POST",
            "/kemr/_search",
            "{\"size\":1,\"query\":{\"knn\":{\"message_embedding\":{\"vector\":" + vector(1) + ",\"k\":1}}}}"
        ).get("hits");
        assertEquals("v1", ((Map<String, Object>) ((List<?>) hits.get("hits")).get(0)).get("_id"));
        call("DELETE", "/kemr", null);
    }

    /**
     * Each row of {@code index}'s table by its document id: the engine's id
     * of the row, which a write that replaces the row changes. Read as
     * {@link #READER}, whose view of the index has no vector column.
     */
    @Override
    @SuppressWarnings("unchecked")
    protected Map<String, Object> rowIds(String index) throws IOException, InterruptedException {
        Thread.sleep(SETTLE_MILLIS);
        Request request = new Request("POST", "/_plugins/_infino/sql");
        request.setJsonEntity("{\"query\":\"SELECT osid, _id FROM " + index + "\"}");
        String token = Base64.getEncoder().encodeToString(READER_BASIC.getBytes(StandardCharsets.UTF_8));
        request.setOptions(RequestOptions.DEFAULT.toBuilder().addHeader("Authorization", "Basic " + token));
        Map<String, Object> out = entityAsMap(client().performRequest(request));
        Map<String, Object> ids = new HashMap<>();
        for (Object row : (List<?>) out.get("rows")) {
            Map<String, Object> r = (Map<String, Object>) row;
            assertNull("each document id once", ids.put(r.get("osid").toString(), r.get("_id")));
        }
        return ids;
    }

    /**
     * {@link #READER}: reads the test's indices with the vector field hidden.
     * A view of an index that has a vector field cannot be read in SQL yet,
     * since the table holds no column by the field's name.
     */
    private void createReader() throws IOException {
        call(
            "PUT",
            "/_plugins/_security/api/roles/" + READER_ROLE,
            "{\"cluster_permissions\":[\"cluster_composite_ops_ro\"],\"index_permissions\":[{\"index_patterns\":[\"kemr*\"],"
                + "\"fls\":[\"~message_embedding\"],\"allowed_actions\":[\"read\",\"indices:admin/mappings/get\"]}]}"
        );
        call("PUT", "/_plugins/_security/api/internalusers/" + READER, "{\"hash\":\"" + DEMO_HASH + "\"}");
        call("PUT", "/_plugins/_security/api/rolesmapping/" + READER_ROLE, "{\"users\":[\"" + READER + "\"]}");
    }

    /** Document {@code i}'s vector: no two the same, none all zero. */
    private static String vector(int i) {
        StringBuilder out = new StringBuilder("[");
        for (int d = 0; d < DIMENSION; d++) {
            out.append(d == 0 ? "" : ",").append(((i + d) % DIMENSION + 1) * STEP + (d == 0 ? i : 0));
        }
        return out.append(']').toString();
    }

    /** An index with the getting-started guide's mapping, over {@link #TABLE}. */
    private void createIndex(String index, boolean rebuilt) throws IOException {
        call(
            "PUT",
            "/" + index,
            "{\"settings\":{\"index.infino.enabled\":true,\"index.infino.mode\":\"default\",\"index.infino.table\":\""
                + TABLE
                + "\",\"index.infino.rebuilt\":"
                + rebuilt
                + ",\"number_of_shards\":1,\"number_of_replicas\":0,\"index.knn\":true},"
                + "\"mappings\":{\"properties\":{"
                + "\"@timestamp\":{\"type\":\"date\"},\"service\":{\"type\":\"keyword\"},\"level\":{\"type\":\"keyword\"},"
                + "\"status\":{\"type\":\"long\"},\"path\":{\"type\":\"keyword\"},\"latency_ms\":{\"type\":\"float\"},"
                + "\"host\":{\"type\":\"keyword\"},\"message\":{\"type\":\"text\"},"
                + "\"message_embedding\":{\"type\":\"knn_vector\",\"dimension\":"
                + DIMENSION
                + ",\"method\":{\"name\":\"hnsw\",\"engine\":\"lucene\",\"space_type\":\"cosinesimil\"}}}}}"
        );
    }
}
