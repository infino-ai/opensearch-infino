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
 * the id yet or not. Until the rebuild has walked the table, a search that
 * returns hits answers 503 rather than a page short of the total, while a
 * count and an aggregation, which read the table, answer in full.
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
    /** The table of the empty-primary test's indices and its rows. */
    private static final String EMPTY_TABLE = "emr4_rows";
    private static final int EMPTY_ROWS = 30;
    /** The table of the left-out-row test's indices, its rows, and the id of the row the new mapping refuses. */
    private static final String REFUSED_TABLE = "emr5_rows";
    private static final int REFUSED_ROWS = 12;
    private static final String REFUSED_ID = "x0";
    /** The table of the refresh-off test's indices and its rows. */
    private static final String UNREFRESHED_TABLE = "emr6_rows";
    private static final int UNREFRESHED_ROWS = 25;
    /** The table of the two-shard held test's indices, its rows, its shards, and the routing that puts every row in one shard. */
    private static final String SHARDED_TABLE = "emr7_rows";
    private static final int SHARDED_ROWS = 16;
    private static final int SHARDED_SHARDS = 2;
    private static final String SHARDED_ROUTING = "one";
    /** The table of the restart-from-store test's index, its rows, and the id written mid-rebuild. */
    private static final String RESTARTED_TABLE = "emr10_rows";
    private static final int RESTARTED_ROWS = 20;
    private static final String RESTARTED_ID = "t0";
    /** The leases a shard holds while its skeleton is rebuilt, and once a rebuild left rows out. */
    private static final String REBUILD_LEASE = "infino_skeleton_rebuild";
    private static final String LEFT_OUT_LEASE = "infino_skeleton_left_out";
    /** The header a 503 of a rebuilding index says how long to wait in, and the seconds it says. */
    private static final String RETRY_AFTER = "Retry-After";
    private static final String RETRY_AFTER_SECONDS = "5";
    /** The mapping every index of these tests has. */
    private static final String LONG_N = "{\"properties\":{\"n\":{\"type\":\"long\"}}}";
    /** The cluster setting that holds skeleton rebuilds. */
    private static final String PAUSED = "infino.skeleton_rebuild.paused";
    /** The status a search of an index rebuilding its skeleton answers. */
    private static final int UNAVAILABLE = 503;

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
        assertBusy(() -> assertEquals(ROWS - 1, hitsOnceRebuilt("emr", ROWS + 1).size()), REBUILD_SECONDS, TimeUnit.SECONDS);
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
        assertBusy(() -> assertEquals(ROUTED_ROWS, hitsOnceRebuilt("emr2", ROUTED_ROWS + 1).size()), REBUILD_SECONDS, TimeUnit.SECONDS);
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
     * the node down where assertions run. While the rebuild is held, a search
     * that returns hits answers 503 with a {@link #RETRY_AFTER} header, and a
     * count or an aggregation answers in full from the table.
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
            // The rebuild is held: the skeleton has no document yet, so a search
            // that returns hits answers 503 rather than leave rows out of the hits
            // its total counts, a scroll's first page included.
            assertRebuilding("POST", "/emr3/_search", "{\"size\":" + (HELD_ROWS + 1) + "}");
            assertRebuilding("POST", "/emr3/_search", "{}");
            assertRebuilding("POST", "/emr3/_search?scroll=1m", "{\"size\":5}");
            // A count, a size 0 search and an aggregation read the table, which
            // holds every row, and answer in full.
            assertEquals(HELD_ROWS, ((Number) call("GET", "/emr3/_count", null).get("count")).intValue());
            Map<String, Object> counted = call("POST", "/emr3/_search", "{\"size\":0,\"track_total_hits\":true}");
            assertEquals(HELD_ROWS, ((Map<String, Object>) ((Map<String, Object>) counted.get("hits")).get("total")).get("value"));
            Map<String, Object> aggregated = call("POST", "/emr3/_search", "{\"size\":0,\"aggs\":{\"top\":{\"max\":{\"field\":\"n\"}}}}");
            assertEquals(HELD_ROWS - 1, ((Number) ((Map<String, Object>) ((Map<String, Object>) aggregated.get("aggregations")).get("top")).get("value")).intValue());

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
        assertBusy(() -> assertEquals(HELD_ROWS - 1, hitsOnceRebuilt("emr3", HELD_ROWS + 1).size()), REBUILD_SECONDS, TimeUnit.SECONDS);
        assertEquals(HELD_ROWS - 1, ((Number) call("GET", "/emr3/_count", null).get("count")).intValue());
        ResponseException gone = expectThrows(ResponseException.class, () -> call("GET", "/emr3/_doc/" + HELD_DELETED, null));
        assertEquals(404, gone.getResponse().getStatusLine().getStatusCode());
        call("DELETE", "/emr3", null);
    }

    /**
     * A new primary over a table that already holds rows, as one allocated
     * empty after its copies were lost starts, though the index is not marked
     * rebuilt: its skeleton is rebuilt all the same, so every row is a hit,
     * and a write of an id the table holds replaces its row rather than
     * appending a second one.
     */
    public void testAnEmptyPrimaryOverATableWithRowsRebuildsItsSkeleton() throws Exception {
        createIndex("emr4_old", false, EMPTY_TABLE, 1);
        StringBuilder rows = new StringBuilder();
        for (int i = 0; i < EMPTY_ROWS; i++) {
            rows.append("{\"index\":{\"_index\":\"emr4_old\",\"_id\":\"e").append(i).append("\"}}\n");
            rows.append("{\"n\":").append(i).append("}\n");
        }
        assertEquals(false, call("POST", "/_bulk?refresh=true", rows.toString()).get("errors"));
        call("DELETE", "/emr4_old", null);

        createIndex("emr4", false, EMPTY_TABLE, 1);
        assertBusy(() -> assertEquals(EMPTY_ROWS, hitsOnceRebuilt("emr4", EMPTY_ROWS + 1).size()), REBUILD_SECONDS, TimeUnit.SECONDS);
        Map<String, Object> rewritten = call("PUT", "/emr4/_doc/e0?refresh=true", "{\"n\":" + REWRITTEN_VALUE + "}");
        assertEquals("the write finds the row", "updated", rewritten.get("result"));
        assertEquals(EMPTY_ROWS, ((Number) call("GET", "/emr4/_count", null).get("count")).intValue());
        call("DELETE", "/emr4", null);
    }

    /**
     * A row the rebuild cannot create a document for (here the new mapping
     * refuses its source) is left out, and logged: the rebuild still ends
     * once it has walked the table, so the index answers searches again,
     * with every other row a hit, rather than answering 503 until the node
     * starts again. The shard's writes go on looking ids up in the table, so
     * a client's write of the left-out id replaces its row rather than
     * adding a second one. The next start of the primary walks the table
     * again; with that id written since, it leaves nothing out, and the
     * writes stop looking ids up.
     */
    @SuppressWarnings("unchecked")
    public void testARebuildThatLeavesARowOutStillEnds() throws Exception {
        // The old mapping keeps a malformed value out of the field; the new one refuses it.
        createIndex("emr5_old", false, REFUSED_TABLE, 1, "", "{\"properties\":{\"n\":{\"type\":\"long\",\"ignore_malformed\":true}}}");
        StringBuilder rows = new StringBuilder();
        for (int i = 1; i < REFUSED_ROWS; i++) {
            rows.append("{\"index\":{\"_index\":\"emr5_old\",\"_id\":\"x").append(i).append("\"}}\n");
            rows.append("{\"n\":").append(i).append("}\n");
        }
        rows.append("{\"index\":{\"_index\":\"emr5_old\",\"_id\":\"").append(REFUSED_ID).append("\"}}\n");
        rows.append("{\"n\":\"not a number\"}\n");
        assertEquals(false, call("POST", "/_bulk?refresh=true", rows.toString()).get("errors"));
        call("DELETE", "/emr5_old", null);

        createIndex("emr5", false, REFUSED_TABLE, 1);
        assertBusy(() -> assertEquals(REFUSED_ROWS - 1, hitsOnceRebuilt("emr5", REFUSED_ROWS + 1).size()), REBUILD_SECONDS, TimeUnit.SECONDS);
        Set<Object> ids = new HashSet<>();
        Map<String, Object> hits = (Map<String, Object>) call("POST", "/emr5/_search", "{\"size\":" + (REFUSED_ROWS + 1) + "}").get("hits");
        for (Object h : (List<?>) hits.get("hits")) {
            ids.add(((Map<String, Object>) h).get("_id"));
        }
        assertFalse("the refused row has no document", ids.contains(REFUSED_ID));
        assertEquals("searches answer, and writes still look ids up", Set.of(LEFT_OUT_LEASE), rebuildLeases("emr5"));
        // A get of an id the table does not hold says so, rather than 503 for
        // as long as a row is left out.
        ResponseException missing = expectThrows(ResponseException.class, () -> call("GET", "/emr5/_doc/no-such-id", null));
        assertEquals(404, missing.getResponse().getStatusLine().getStatusCode());

        // The skeleton has no document for the id, but the write finds the
        // table's row and replaces it: the table still holds one row per id.
        call("PUT", "/emr5/_doc/" + REFUSED_ID + "?refresh=true", "{\"n\":0}");
        assertEquals(REFUSED_ROWS, ((Number) call("GET", "/emr5/_count", null).get("count")).intValue());

        // The primary starts again from its store and walks the table again:
        // nothing is left out now, so the shard no longer holds the lease.
        call("POST", "/emr5/_close", null);
        call("POST", "/emr5/_open?wait_for_active_shards=1", null);
        call("GET", "/_cluster/health/emr5?wait_for_status=green&timeout=60s", null);
        assertBusy(() -> assertEquals(Set.of(), rebuildLeases("emr5")), REBUILD_SECONDS, TimeUnit.SECONDS);
        assertEquals(REFUSED_ROWS, hitsOnceRebuilt("emr5", REFUSED_ROWS + 1).size());
        assertEquals(REFUSED_ROWS, ((Number) call("GET", "/emr5/_count", null).get("count")).intValue());
        call("DELETE", "/emr5", null);
    }

    /**
     * With refresh off, nothing but the rebuild itself makes its documents
     * visible: it refreshes once it is done, so the index answers searches
     * without a refresh by hand.
     */
    public void testARebuildWithRefreshOffAnswersOnceDone() throws Exception {
        createIndex("emr6_old", false, UNREFRESHED_TABLE, 1);
        StringBuilder rows = new StringBuilder();
        for (int i = 0; i < UNREFRESHED_ROWS; i++) {
            rows.append("{\"index\":{\"_index\":\"emr6_old\",\"_id\":\"u").append(i).append("\"}}\n");
            rows.append("{\"n\":").append(i).append("}\n");
        }
        assertEquals(false, call("POST", "/_bulk?refresh=true", rows.toString()).get("errors"));
        call("DELETE", "/emr6_old", null);

        createIndex("emr6", false, UNREFRESHED_TABLE, 1, ",\"index.refresh_interval\":-1", LONG_N);
        assertBusy(
            () -> assertEquals(UNREFRESHED_ROWS, hitsOnceRebuilt("emr6", UNREFRESHED_ROWS + 1).size()),
            REBUILD_SECONDS,
            TimeUnit.SECONDS
        );
        call("DELETE", "/emr6", null);
    }

    /**
     * An index of two shards whose rows all live in one shard's table: only
     * that shard is rebuilt. While its rebuild is held, a search that returns
     * hits answers 503 whole, rather than a 200 from the other shard alone
     * with the failed shard named in {@code _shards}; a count answers in full.
     */
    @SuppressWarnings("unchecked")
    public void testASearchOverShardsOneOfWhichIsRebuiltAnswers503Whole() throws Exception {
        createIndex("emr7_old", false, SHARDED_TABLE, SHARDED_SHARDS);
        StringBuilder rows = new StringBuilder();
        for (int i = 0; i < SHARDED_ROWS; i++) {
            rows.append("{\"index\":{\"_index\":\"emr7_old\",\"_id\":\"s").append(i).append("\",\"routing\":\"").append(SHARDED_ROUTING).append("\"}}\n");
            rows.append("{\"n\":").append(i).append("}\n");
        }
        assertEquals(false, call("POST", "/_bulk?refresh=true", rows.toString()).get("errors"));
        call("DELETE", "/emr7_old", null);

        call("PUT", "/_cluster/settings", "{\"persistent\":{\"" + PAUSED + "\":true}}");
        try {
            createIndex("emr7", false, SHARDED_TABLE, SHARDED_SHARDS);
            call("GET", "/_cluster/health/emr7?wait_for_status=green&timeout=60s", null);
            assertRebuilding("POST", "/emr7/_search", "{\"size\":" + (SHARDED_ROWS + 1) + "}");
            assertRebuilding("POST", "/emr7/_search?allow_partial_search_results=true", "{\"size\":" + (SHARDED_ROWS + 1) + "}");
            assertEquals(SHARDED_ROWS, ((Number) call("GET", "/emr7/_count", null).get("count")).intValue());
            // Sent to each shard alone, the rebuilding one answers 503 and the
            // other, whose table is empty and which is not rebuilt, answers.
            int rebuilding = shardOf("emr7", SHARDED_ROUTING);
            assertRebuilding("POST", "/emr7/_search?preference=_shards:" + rebuilding, "{\"size\":" + (SHARDED_ROWS + 1) + "}");
            Map<String, Object> other = call("POST", "/emr7/_search?preference=_shards:" + (1 - rebuilding), "{\"size\":" + (SHARDED_ROWS + 1) + "}");
            assertEquals(0, ((List<?>) ((Map<String, Object>) other.get("hits")).get("hits")).size());
            assertEquals(1, ((Number) ((Map<String, Object>) other.get("_shards")).get("successful")).intValue());
        } finally {
            call("PUT", "/_cluster/settings", "{\"persistent\":{\"" + PAUSED + "\":null}}");
        }
        assertBusy(() -> assertEquals(SHARDED_ROWS, hitsOnceRebuilt("emr7", SHARDED_ROWS + 1).size()), REBUILD_SECONDS, TimeUnit.SECONDS);
        call("DELETE", "/emr7", null);
    }

    /**
     * A primary that fails while its rebuild is held, and starts again from
     * its own store: it knows from the rebuild's lease, which it took before
     * the shard first started and kept on disk, that its skeleton is still
     * being rebuilt. So its searches
     * that return hits still answer 503, a write of an id its skeleton lacks
     * replaces the table's row rather than adding a second one, and once
     * released it finishes the rebuild.
     */
    @SuppressWarnings("unchecked")
    public void testAPrimaryStartingAgainFromItsStoreMidRebuildFinishesIt() throws Exception {
        createIndex("emr10_old", false, RESTARTED_TABLE, 1);
        StringBuilder rows = new StringBuilder();
        for (int i = 0; i < RESTARTED_ROWS; i++) {
            rows.append("{\"index\":{\"_index\":\"emr10_old\",\"_id\":\"t").append(i).append("\"}}\n");
            rows.append("{\"n\":").append(i).append("}\n");
        }
        assertEquals(false, call("POST", "/_bulk?refresh=true", rows.toString()).get("errors"));
        call("DELETE", "/emr10_old", null);

        call("PUT", "/_cluster/settings", "{\"persistent\":{\"" + PAUSED + "\":true}}");
        try {
            createIndex("emr10", false, RESTARTED_TABLE, 1);
            call("GET", "/_cluster/health/emr10?wait_for_status=green&timeout=60s", null);
            assertRebuilding("POST", "/emr10/_search", "{\"size\":" + (RESTARTED_ROWS + 1) + "}");
            failPrimary("emr10");
            assertEquals(Set.of(REBUILD_LEASE), rebuildLeases("emr10"));
            assertRebuilding("POST", "/emr10/_search", "{\"size\":" + (RESTARTED_ROWS + 1) + "}");
            call("PUT", "/emr10/_doc/" + RESTARTED_ID + "?refresh=true", "{\"n\":" + REWRITTEN_VALUE + "}");
            assertEquals("the write replaces the row", RESTARTED_ROWS, ((Number) call("GET", "/emr10/_count", null).get("count")).intValue());
        } finally {
            call("PUT", "/_cluster/settings", "{\"persistent\":{\"" + PAUSED + "\":null}}");
        }
        assertBusy(() -> assertEquals(RESTARTED_ROWS, hitsOnceRebuilt("emr10", RESTARTED_ROWS + 1).size()), REBUILD_SECONDS, TimeUnit.SECONDS);
        assertEquals(Set.of(), rebuildLeases("emr10"));
        assertEquals(RESTARTED_ROWS, ((Number) call("GET", "/emr10/_count", null).get("count")).intValue());
        assertEquals(REWRITTEN_VALUE, ((Map<String, Object>) call("GET", "/emr10/_doc/" + RESTARTED_ID, null).get("_source")).get("n"));
        call("DELETE", "/emr10", null);
    }

    /** Fail {@code index}'s one primary and wait until it is allocated again from its own store. */
    @SuppressWarnings("unchecked")
    private void failPrimary(String index) throws IOException {
        List<Object> shard = callList("GET", "/_cat/shards/" + index + "?format=json&h=node", null);
        String node = ((Map<String, Object>) shard.get(0)).get("node").toString();
        call(
            "POST",
            "/_cluster/reroute",
            "{\"commands\":[{\"cancel\":{\"index\":\"" + index + "\",\"shard\":0,\"node\":\"" + node + "\",\"allow_primary\":true}}]}"
        );
        call("GET", "/_cluster/health/" + index + "?wait_for_status=green&timeout=60s", null);
    }

    /** The rebuild's leases ({@link #REBUILD_LEASE}, {@link #LEFT_OUT_LEASE}) {@code index}'s one primary holds. */
    @SuppressWarnings("unchecked")
    private Set<String> rebuildLeases(String index) throws IOException {
        Map<String, Object> indices = (Map<String, Object>) call("GET", "/" + index + "/_stats?level=shards", null).get("indices");
        Map<String, Object> shards = (Map<String, Object>) ((Map<String, Object>) indices.get(index)).get("shards");
        Set<String> held = new HashSet<>();
        for (Object copy : (List<?>) shards.get("0")) {
            Map<String, Object> c = (Map<String, Object>) copy;
            if (Boolean.TRUE.equals(((Map<String, Object>) c.get("routing")).get("primary"))) {
                for (Object lease : (List<?>) ((Map<String, Object>) c.get("retention_leases")).get("leases")) {
                    String id = ((Map<String, Object>) lease).get("id").toString();
                    if (id.equals(REBUILD_LEASE) || id.equals(LEFT_OUT_LEASE)) {
                        held.add(id);
                    }
                }
            }
        }
        return held;
    }

    /** The shard of {@code index} a document written with {@code routing} goes to. */
    @SuppressWarnings("unchecked")
    private int shardOf(String index, String routing) throws IOException {
        List<?> groups = (List<?>) call("GET", "/" + index + "/_search_shards?routing=" + routing, null).get("shards");
        return ((Number) ((Map<String, Object>) ((List<?>) groups.get(0)).get(0)).get("shard")).intValue();
    }

    /**
     * {@code method path body} answers 503 because the index is rebuilding,
     * with a {@link #RETRY_AFTER} header.
     */
    private void assertRebuilding(String method, String path, String body) {
        ResponseException held = expectThrows(ResponseException.class, () -> call(method, path, body));
        assertEquals(path + " " + body, UNAVAILABLE, held.getResponse().getStatusLine().getStatusCode());
        assertTrue(held.getMessage(), held.getMessage().contains("rebuilding its documents"));
        assertEquals(path + " " + body, RETRY_AFTER_SECONDS, held.getResponse().getHeader(RETRY_AFTER));
    }

    /**
     * The hits of a search of {@code index}, room left for {@code size} rows;
     * a 503 while its skeleton is rebuilt fails as an assertion, so a busy
     * wait tries again.
     */
    @SuppressWarnings("unchecked")
    private List<?> hitsOnceRebuilt(String index, int size) throws IOException {
        try {
            Map<String, Object> hits = (Map<String, Object>) call("POST", "/" + index + "/_search", "{\"size\":" + size + "}").get("hits");
            return (List<?>) hits.get("hits");
        } catch (ResponseException e) {
            if (e.getResponse().getStatusLine().getStatusCode() == UNAVAILABLE) {
                throw new AssertionError("still rebuilding: " + e.getMessage(), e);
            }
            throw e;
        }
    }

    /** The routing value document {@code i} of the routed test is written with. */
    private static String routing(int i) {
        return i % 2 == 0 ? "even" : "odd";
    }

    private void createIndex(String index, boolean rebuilt) throws IOException {
        createIndex(index, rebuilt, TABLE, 1);
    }

    private void createIndex(String index, boolean rebuilt, String table, int shards) throws IOException {
        createIndex(index, rebuilt, table, shards, "", LONG_N);
    }

    /** An index over {@code table}, with {@code extra} settings after the others and the mappings {@code mappings}. */
    private void createIndex(String index, boolean rebuilt, String table, int shards, String extra, String mappings) throws IOException {
        call(
            "PUT",
            "/" + index,
            "{\"settings\":{\"index.infino.enabled\":true,\"index.infino.mode\":\"default\",\"index.infino.table\":\""
                + table
                + "\",\"index.infino.rebuilt\":"
                + rebuilt
                + ",\"number_of_shards\":"
                + shards
                + ",\"number_of_replicas\":0"
                + extra
                + "},\"mappings\":"
                + mappings
                + "}"
        );
    }
}
