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
 * A default-mode index as the registry recreates it on a fresh cluster:
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
    /** The table of the routed test's indices, its rows and its shards. */
    private static final String ROUTED_TABLE = "emr2_rows";
    private static final int ROUTED_ROWS = 40;
    private static final int ROUTED_SHARDS = 2;
    /** The table of the held-rebuild test's indices, its rows, and the id it deletes. */
    private static final String HELD_TABLE = "emr3_rows";
    private static final int HELD_ROWS = 20;
    private static final String HELD_DELETED = "h1";
    /** The table of the untouched-rows test's indices, and its rows. */
    private static final String KEPT_TABLE = "emr6_rows";
    private static final int KEPT_ROWS = 200;
    /** The cluster setting that holds skeleton rebuilds. */
    private static final String PAUSED = "infino.skeleton_rebuild.paused";

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

    /**
     * Documents written with routing values live in the table of the shard
     * their routing picks, whatever shard their id alone would pick; a
     * rebuild routes each back by the value it was written with, so every
     * one is a hit again and a get with its routing finds it.
     */
    @SuppressWarnings("unchecked")
    public void testARebuiltIndexRoutesRowsAsTheyWereWritten() throws Exception {
        createIndex("emr2_old", false, ROUTED_TABLE, ROUTED_SHARDS);
        StringBuilder bulk = new StringBuilder();
        for (int i = 0; i < ROUTED_ROWS; i++) {
            bulk.append("{\"index\":{\"_index\":\"emr2_old\",\"_id\":\"d").append(i).append("\",\"routing\":\"").append(routing(i)).append("\"}}\n");
            bulk.append("{\"n\":").append(i).append("}\n");
        }
        Map<String, Object> written = call("POST", "/_bulk?refresh=true", bulk.toString());
        assertEquals(false, written.get("errors"));
        call("DELETE", "/emr2_old", null);

        createIndex("emr2", true, ROUTED_TABLE, ROUTED_SHARDS);
        assertBusy(() -> {
            Map<String, Object> hits = (Map<String, Object>) call("POST", "/emr2/_search", "{\"size\":" + (ROUTED_ROWS + 1) + "}").get("hits");
            assertEquals(ROUTED_ROWS, ((List<?>) hits.get("hits")).size());
        }, REBUILD_SECONDS, TimeUnit.SECONDS);
        for (int i = 0; i < ROUTED_ROWS; i++) {
            Map<String, Object> doc = call("GET", "/emr2/_doc/d" + i + "?routing=" + routing(i), null);
            assertEquals("d" + i + " is found by its routing", true, doc.get("found"));
            assertEquals(routing(i), doc.get("_routing"));
        }
        call("DELETE", "/emr2", null);
    }

    /**
     * A bulk delete of an id the table holds and a held rebuild has not put
     * in the skeleton yet: the table alone deletes the row, and the delete is
     * acknowledged as found. A bulk requires a translog location of every
     * write it applied; a result without one fails an assertion that takes
     * the node down where assertions run.
     */
    @SuppressWarnings("unchecked")
    public void testABulkDeleteWhileTheSkeletonIsHeldDeletesTheRow() throws Exception {
        createIndex("emr3_old", false, HELD_TABLE, 1);
        StringBuilder rows = new StringBuilder();
        for (int i = 0; i < HELD_ROWS; i++) {
            rows.append("{\"index\":{\"_index\":\"emr3_old\",\"_id\":\"h").append(i).append("\"}}\n");
            rows.append("{\"n\":").append(i).append("}\n");
        }
        assertEquals(false, call("POST", "/_bulk?refresh=true", rows.toString()).get("errors"));
        call("DELETE", "/emr3_old", null);

        call("PUT", "/_cluster/settings", "{\"persistent\":{\"" + PAUSED + "\":true}}");
        try {
            createIndex("emr3", true, HELD_TABLE, 1);
            // The rebuild is held: the skeleton has no document yet.
            assertEquals("the rebuild is held", 0, heldHits().size());

            String delete = "{\"delete\":{\"_index\":\"emr3\",\"_id\":\"" + HELD_DELETED + "\"}}\n";
            Map<String, Object> bulk = call("POST", "/_bulk?refresh=true", delete);
            assertEquals(false, bulk.get("errors"));
            Map<String, Object> item = (Map<String, Object>) ((Map<String, Object>) ((List<?>) bulk.get("items")).get(0)).get("delete");
            assertEquals("the delete finds the row", "deleted", item.get("result"));
            assertEquals(200, ((Number) item.get("status")).intValue());
        } finally {
            call("PUT", "/_cluster/settings", "{\"persistent\":{\"" + PAUSED + "\":null}}");
        }

        // Released, the rebuild brings back every row but the deleted one.
        assertBusy(() -> assertEquals(HELD_ROWS - 1, heldHits().size()), REBUILD_SECONDS, TimeUnit.SECONDS);
        assertEquals(HELD_ROWS - 1, ((Number) call("GET", "/emr3/_count", null).get("count")).intValue());
        ResponseException gone = expectThrows(ResponseException.class, () -> call("GET", "/emr3/_doc/" + HELD_DELETED, null));
        assertEquals(404, gone.getResponse().getStatusLine().getStatusCode());
        call("DELETE", "/emr3", null);
    }

    /**
     * A rebuild creates a document for each row and leaves the rows as they
     * are: written to the table again, each would be a replace (a lookup, a
     * delete and an append), whose memory grows with the table, and its row
     * would get a new engine id.
     */
    @SuppressWarnings("unchecked")
    public void testARebuildRewritesNoRow() throws Exception {
        createIndex("emr6_old", false, KEPT_TABLE, 1);
        StringBuilder bulk = new StringBuilder();
        for (int i = 0; i < KEPT_ROWS; i++) {
            bulk.append("{\"index\":{\"_index\":\"emr6_old\",\"_id\":\"k").append(i).append("\"}}\n");
            bulk.append("{\"n\":").append(i).append("}\n");
        }
        assertEquals(false, call("POST", "/_bulk?refresh=true", bulk.toString()).get("errors"));
        Map<String, Object> before = rowIds("emr6_old");
        assertEquals(KEPT_ROWS, before.size());
        call("DELETE", "/emr6_old", null);

        createIndex("emr6", true, KEPT_TABLE, 1);
        // The shard's own document count, which a search of a skeleton being rebuilt does not wait for.
        assertBusy(() -> {
            Map<String, Object> index = (Map<String, Object>) callList("GET", "/_cat/indices/emr6?format=json&h=docs.count", null).get(0);
            assertEquals(String.valueOf(KEPT_ROWS), index.get("docs.count"));
        }, REBUILD_SECONDS, TimeUnit.SECONDS);
        assertEquals("every row keeps its engine id through the rebuild", before, rowIds("emr6"));
        assertEquals(KEPT_ROWS, ((Number) call("GET", "/emr6/_count", null).get("count")).intValue());
        call("DELETE", "/emr6", null);
    }

    /** The hits of a search of the held-rebuild test's index, room left for every row. */
    @SuppressWarnings("unchecked")
    private List<?> heldHits() throws IOException {
        Map<String, Object> hits = (Map<String, Object>) call("POST", "/emr3/_search", "{\"size\":" + (HELD_ROWS + 1) + "}").get("hits");
        return (List<?>) hits.get("hits");
    }

    /** The routing value document {@code i} of the routed test is written with. */
    private static String routing(int i) {
        return i % 2 == 0 ? "even" : "odd";
    }

    private void createIndex(String index, boolean rebuilt) throws IOException {
        createIndex(index, rebuilt, TABLE, 1);
    }

    private void createIndex(String index, boolean rebuilt, String table, int shards) throws IOException {
        call(
            "PUT",
            "/" + index,
            "{\"settings\":{\"index.infino.enabled\":true,\"index.infino.mode\":\"default\",\"index.infino.table\":\""
                + table
                + "\",\"index.infino.rebuilt\":"
                + rebuilt
                + ",\"number_of_shards\":"
                + shards
                + ",\"number_of_replicas\":0},"
                + "\"mappings\":{\"properties\":{\"n\":{\"type\":\"long\"}}}}"
        );
    }
}
