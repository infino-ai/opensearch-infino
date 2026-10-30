/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import org.opensearch.client.ResponseException;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * An engine-mode index as the registry recreates it on a fresh cluster:
 * created over a table that already holds rows, and marked rebuilt. Its
 * new Lucene skeleton gets a document for each row, so its searches find
 * the rows written before; a rewrite of an existing id replaces its row, and
 * a delete of one deletes it and reports it found, whether the skeleton has
 * the id yet or not.
 */
public class InfinoEngineRebuildIT extends InfinoRestTestCase {

    /** Rows the table holds before the index is recreated. */
    private static final int ROWS = 300;
    /** How long the skeleton may take to be rebuilt. */
    private static final long REBUILD_SECONDS = 60;
    /** The table both indices name. */
    private static final String TABLE = "emr_rows";
    /** The id rewritten and the value it is rewritten with. */
    private static final String REWRITTEN = "r0";
    private static final int REWRITTEN_VALUE = 1_000;
    /** The id deleted. */
    private static final String DELETED = "r1";

    @SuppressWarnings("unchecked")
    public void testARebuiltIndexGetsItsRowsBackAndKeepsThemOnce() throws Exception {
        createIndex("emr_old", false);
        StringBuilder bulk = new StringBuilder();
        for (int i = 0; i < ROWS; i++) {
            bulk.append("{\"index\":{\"_index\":\"emr_old\",\"_id\":\"r").append(i).append("\"}}\n");
            bulk.append("{\"n\":").append(i).append("}\n");
        }
        Map<String, Object> written = call("POST", "/_bulk?refresh=true", bulk.toString());
        assertEquals(false, written.get("errors"));
        // The table was named explicitly, so it outlives the index.
        call("DELETE", "/emr_old", null);

        createIndex("emr", true);
        // Right away, while the skeleton may not know these ids yet.
        call("PUT", "/emr/_doc/" + REWRITTEN + "?refresh=true", "{\"n\":" + REWRITTEN_VALUE + "}");
        Map<String, Object> deleted = call("DELETE", "/emr/_doc/" + DELETED + "?refresh=true", null);
        assertEquals("the delete finds the row", "deleted", deleted.get("result"));

        // Every row but the deleted one, each once, as hits of a search.
        assertBusy(() -> {
            Map<String, Object> hits = (Map<String, Object>) call("POST", "/emr/_search", "{\"size\":" + (ROWS + 1) + "}").get("hits");
            assertEquals(ROWS - 1, ((List<?>) hits.get("hits")).size());
        }, REBUILD_SECONDS, TimeUnit.SECONDS);
        Map<String, Object> hits = (Map<String, Object>) call("POST", "/emr/_search", "{\"size\":" + (ROWS + 1) + "}").get("hits");
        Set<Object> ids = new HashSet<>();
        for (Object h : (List<?>) hits.get("hits")) {
            assertTrue("each row once", ids.add(((Map<String, Object>) h).get("_id")));
        }
        assertFalse(ids.contains(DELETED));
        assertEquals(ROWS - 1, ((Number) call("GET", "/emr/_count", null).get("count")).intValue());
        Map<String, Object> rewritten = call("GET", "/emr/_doc/" + REWRITTEN, null);
        assertEquals(REWRITTEN_VALUE, ((Map<String, Object>) rewritten.get("_source")).get("n"));
        ResponseException gone = expectThrows(ResponseException.class, () -> call("GET", "/emr/_doc/" + DELETED, null));
        assertEquals(404, gone.getResponse().getStatusLine().getStatusCode());

        // Once rebuilt, the skeleton answers for every id.
        call("PUT", "/emr/_doc/r2?refresh=true", "{\"n\":2}");
        assertEquals(ROWS - 1, ((Number) call("GET", "/emr/_count", null).get("count")).intValue());
        call("DELETE", "/emr", null);
    }

    private void createIndex(String index, boolean rebuilt) throws IOException {
        call(
            "PUT",
            "/" + index,
            "{\"settings\":{\"index.infino.enabled\":true,\"index.infino.mode\":\"engine\",\"index.infino.table\":\""
                + TABLE
                + "\",\"index.infino.rebuilt\":"
                + rebuilt
                + ",\"number_of_shards\":1,\"number_of_replicas\":0},"
                + "\"mappings\":{\"properties\":{\"n\":{\"type\":\"long\"}}}}"
        );
    }
}
