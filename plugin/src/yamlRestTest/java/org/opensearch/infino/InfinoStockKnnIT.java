/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import org.opensearch.client.Request;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.test.rest.OpenSearchRestTestCase;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;

/**
 * The stock {@code knn} query where a YAML suite cannot reach: each space
 * type's scores against the k-NN plugin's own (YAML has no tolerance
 * assertion), and a filter over a corpus too large for a YAML suite.
 */
public class InfinoStockKnnIT extends OpenSearchRestTestCase {

    /** The admin user of the test cluster's internal user database. */
    private static final String ADMIN_BASIC = "admin:admin";
    /** The engine's smallest vector. */
    private static final int DIMENSION = 16;
    /** The query vector's leading components; the rest are zero. */
    private static final float[] QUERY = { 1.0f, 0.5f };
    /**
     * The twins' documents, by leading components (the rest zero). Under the
     * query each sits at its own distance in every space type, so no order
     * has ties, and four have a negative inner product with it, so both of
     * the k-NN plugin's inner-product score branches are exercised.
     */
    private static final float[][] TWIN_DOCS = {
        { 1.0f, 0.0f, 0.0f, 0.0f },
        { 0.9f, 0.3f, 0.0f, 0.0f },
        { 0.5f, 0.5f, 0.2f, 0.0f },
        { 0.0f, 1.0f, 0.0f, 0.0f },
        { 0.0f, 0.0f, 1.0f, 0.0f },
        { -0.5f, 0.0f, 0.0f, 0.3f },
        { -1.0f, -0.5f, 0.0f, 0.0f },
        { 0.2f, 0.1f, 0.0f, 1.5f },
        { -0.2f, -0.2f, 0.4f, 0.0f },
        { 0.0f, -0.3f, 0.0f, 0.7f } };
    /** Every twin document: the whole order is compared. */
    private static final int TWIN_K = TWIN_DOCS.length;
    /** How far a score may sit from the k-NN plugin's: float arithmetic on both sides. */
    private static final double SCORE_TOLERANCE = 1e-4;
    /** The space types the table serves, each twin-checked. */
    private static final String[] SPACE_TYPES = { "l2", "innerproduct", "cosinesimil" };

    /** Bulks of the large corpus, each committed as its own file. */
    private static final int BULKS = 11;
    /** Documents near the query per bulk, none of which the filter keeps. */
    private static final int NEAR_PER_BULK = 10_000;
    /** Documents far from the query: one per bulk, the only ones the filter keeps. */
    private static final int FAR = BULKS;
    /** Spread of the near documents' components, so they are distinct. */
    private static final int NEAR_SPREAD = 997;
    /** A k smaller than the far set. */
    private static final int K = 10;
    /** A k larger than the far set. */
    private static final int WIDE_K = 20;

    @Override
    protected Settings restClientSettings() {
        String token = Base64.getEncoder().encodeToString(ADMIN_BASIC.getBytes(StandardCharsets.UTF_8));
        return Settings.builder().put(super.restClientSettings()).put(ThreadContext.PREFIX + ".Authorization", "Basic " + token).build();
    }

    @Override
    protected boolean preserveIndicesUponCompletion() {
        // A wildcard delete would reach the security plugin's own index.
        return true;
    }

    /**
     * For each space type, the same documents in an Infino-backed index and a
     * Lucene twin: the k nearest come back in the same order with the same
     * scores, as the k-NN plugin scores them.
     */
    @SuppressWarnings("unchecked")
    public void testScoresAreTheKnnPluginsInEverySpaceType() throws IOException {
        for (String space : SPACE_TYPES) {
            String infino = "twin_inf_" + space;
            String lucene = "twin_luc_" + space;
            createIndex(infino, true, space);
            createIndex(lucene, false, space);
            for (String index : new String[] { infino, lucene }) {
                StringBuilder body = new StringBuilder();
                for (int i = 0; i < TWIN_DOCS.length; i++) {
                    body.append("{\"index\":{\"_index\":\"").append(index).append("\",\"_id\":\"d").append(i).append("\"}}\n");
                    body.append("{\"emb\":").append(vector(TWIN_DOCS[i])).append(",\"tag\":\"t\"}\n");
                }
                bulk(body.toString());
            }
            String query = "{\"size\":" + TWIN_K + ",\"query\":{\"knn\":{\"emb\":{\"vector\":" + vector(QUERY) + ",\"k\":" + TWIN_K + "}}}}";
            List<Map<String, Object>> expected = hits(lucene, query);
            List<Map<String, Object>> actual = hits(infino, query);
            assertEquals(space + ": every document", TWIN_K, expected.size());
            assertEquals(space + ": the same hits as the k-NN plugin", ids(expected), ids(actual));
            for (int i = 0; i < expected.size(); i++) {
                double want = ((Number) expected.get(i).get("_score")).doubleValue();
                double got = ((Number) actual.get(i).get("_score")).doubleValue();
                assertEquals(space + ": the score of " + expected.get(i).get("_id"), want, got, SCORE_TOLERANCE);
            }
        }
    }

    /**
     * A filtered knn whose matching documents all sit farther from the query
     * than every other document: the k nearest the filter keeps, as the k-NN
     * plugin returns them, however many nearer documents it rejects.
     *
     * <p>The engine's {@code vector_search} takes the filter as a predicate
     * on a column with no index. It checks the predicate on its k nearest
     * and, when fewer than k survive, evaluates it over the table's rows and
     * searches within exactly the rows it keeps. Before that, it widened the
     * search until k survived, up to a ceiling (10,000 hits for any k below
     * 157), and the matching documents past it were never reached: the cases
     * past 10,000 nearer documents came back empty.
     */
    public void testAFilteredKnnIsExactHoweverManyNearerDocumentsItRejects() throws IOException {
        List<String> measured = new ArrayList<>();
        // Below the engine's old widening ceiling: 5,000 nearer documents, 11 matching.
        String small = "knn_scale_small";
        createIndex(small, true, "l2");
        StringBuilder smallBody = new StringBuilder();
        appendNear(smallBody, small, 0, NEAR_PER_BULK / 2);
        for (int b = 0; b < FAR; b++) {
            appendFar(smallBody, small, b);
        }
        bulk(smallBody.toString());
        measured.add(measure(small, "5k nearer", K, FAR));
        measured.add(measure(small, "5k nearer", WIDE_K, FAR));

        String index = "knn_scale";
        createIndex(index, true, "l2");
        for (int b = 0; b < BULKS; b++) {
            StringBuilder body = new StringBuilder();
            appendNear(body, index, b * NEAR_PER_BULK, NEAR_PER_BULK);
            appendFar(body, index, b);
            bulk(body.toString());
            if (b == 0) {
                measured.add(measure(index, "10k nearer", K, 1));
            }
        }
        measured.add(measure(index, "110k nearer", K, FAR));
        measured.add(measure(index, "110k nearer", WIDE_K, FAR));
        String report = String.join("; ", measured);
        assertEquals(
            report,
            List.of(
                "5k nearer k=10: 10 of 10",
                "5k nearer k=20: 11 of 11",
                // Past the old ceiling: these came back empty.
                "10k nearer k=10: 1 of 1",
                "110k nearer k=10: 10 of 10",
                "110k nearer k=20: 11 of 11"
            ),
            measured
        );
    }

    /** {@code count} documents near the query, ids from {@code first}; the filter keeps none of them. */
    private static void appendNear(StringBuilder body, String index, int first, int count) {
        for (int n = first; n < first + count; n++) {
            float[] near = { QUERY[0], QUERY[1], (n % NEAR_SPREAD) / (float) NEAR_SPREAD / 10f, (n / NEAR_SPREAD) / 1000f };
            body.append("{\"index\":{\"_index\":\"").append(index).append("\",\"_id\":\"n").append(n).append("\"}}\n");
            body.append("{\"emb\":").append(vector(near)).append(",\"tag\":\"near\"}\n");
        }
    }

    /** The {@code b}th document far from the query, one the filter keeps. */
    private static void appendFar(StringBuilder body, String index, int b) {
        float[] far = { -QUERY[0], -QUERY[1], b / 10f, 0f };
        body.append("{\"index\":{\"_index\":\"").append(index).append("\",\"_id\":\"f").append(b).append("\"}}\n");
        body.append("{\"emb\":").append(vector(far)).append(",\"tag\":\"far\"}\n");
    }

    /** {@code label k=K: <far hits> of <expected>}: how many of the filter's nearest came back. */
    private String measure(String index, String label, int k, int matching) throws IOException {
        String query = "{\"size\":"
            + k
            + ",\"query\":{\"knn\":{\"emb\":{\"vector\":"
            + vector(QUERY)
            + ",\"k\":"
            + k
            + ",\"filter\":{\"term\":{\"tag\":\"far\"}}}}}}";
        List<Map<String, Object>> page = hits(index, query);
        long far = page.stream().filter(h -> h.get("_id").toString().startsWith("f")).count();
        assertEquals(label + ": every hit is a far document", far, page.size());
        return label + " k=" + k + ": " + far + " of " + Math.min(k, matching);
    }

    private void createIndex(String index, boolean infino, String space) throws IOException {
        Request create = new Request("PUT", "/" + index);
        create.setJsonEntity(
            "{\"settings\":{\"index.infino.enabled\":"
                + infino
                + ",\"index.knn\":true,\"number_of_shards\":1,\"number_of_replicas\":0},"
                + "\"mappings\":{\"properties\":{\"emb\":{\"type\":\"knn_vector\",\"dimension\":"
                + DIMENSION
                + ",\"method\":{\"name\":\"hnsw\",\"engine\":\"lucene\",\"space_type\":\""
                + space
                + "\"}},\"tag\":{\"type\":\"keyword\"}}}}"
        );
        client().performRequest(create);
    }

    private void bulk(String body) throws IOException {
        Request bulk = new Request("POST", "/_bulk");
        bulk.addParameter("refresh", "true");
        bulk.setJsonEntity(body);
        Map<String, Object> out = entityAsMap(client().performRequest(bulk));
        assertEquals("every document indexed", false, out.get("errors"));
    }

    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> hits(String index, String query) throws IOException {
        Request search = new Request("POST", "/" + index + "/_search");
        search.setJsonEntity(query);
        Map<String, Object> hits = (Map<String, Object>) entityAsMap(client().performRequest(search)).get("hits");
        return (List<Map<String, Object>>) hits.get("hits");
    }

    private static List<Object> ids(List<Map<String, Object>> hits) {
        return hits.stream().map(h -> h.get("_id")).toList();
    }

    /** {@code leading} padded with zeros to the engine's smallest vector, as JSON. */
    private static String vector(float[] leading) {
        StringBuilder out = new StringBuilder("[");
        for (int i = 0; i < DIMENSION; i++) {
            if (i > 0) {
                out.append(',');
            }
            out.append(i < leading.length ? leading[i] : 0f);
        }
        return out.append(']').toString();
    }
}
