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
 * A text search under a filter applied after the ranking (a term on a field
 * some documents hold several values of) is the hits the filter keeps, by
 * score, however far below the unfiltered ranking they sit: in each mode,
 * the page is the one a Lucene index of the same documents returns, sorted
 * by the score then a field, or by the score alone. The corpus is too large
 * for a YAML suite: the kept documents rank below more stronger matches than
 * the plugin ranks in one window.
 */
public class InfinoScoreFieldSortIT extends OpenSearchRestTestCase {

    /** The admin user of the test cluster's internal user database. */
    private static final String ADMIN_BASIC = "admin:admin";
    /** The Lucene index, and the two the plugin serves, each with the same documents. */
    private static final String LUCENE = "score_sort_window_luc";
    private static final List<String> SERVED = List.of("score_sort_window_proxy", "score_sort_window_default");
    /** Stronger matches than any kept one: more than the plugin ranks in one window. */
    private static final int STRONG = 10_040;
    /** The documents the filter keeps, each a weaker match than every strong one. */
    private static final int KEPT = 12;
    private static final int PAGE = 5;

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

    public void testAFilterAfterTheRankingKeepsTheHitsLuceneKeeps() throws IOException {
        create(LUCENE, "\"index.infino.enabled\":false");
        create(SERVED.get(0), "\"index.infino.enabled\":true,\"index.infino.mode\":\"proxy\"");
        create(SERVED.get(1), "\"index.infino.enabled\":true");
        for (String index : List.of(LUCENE, SERVED.get(0), SERVED.get(1))) {
            load(index);
        }

        String must = "{\"match\":{\"message\":\"disk\"}}";
        String keep = "{\"term\":{\"tags\":\"keep\"}}";
        String bySeq = "[{\"_score\":\"desc\"},{\"seq\":\"asc\"}]";
        // The shape the review reported: sorted by the score then seq.
        String kept = "{\"bool\":{\"must\":[" + must + "],\"filter\":[" + keep + "]}}";
        assertSamePage(kept, bySeq, 0);
        assertSamePage(kept, bySeq, PAGE);
        // Ranked by more than the match, by the score then seq and by the score alone.
        String bonus = "{\"bool\":{\"must\":[" + must + "],\"should\":[{\"term\":{\"status\":\"ok\"}}],\"filter\":[" + keep + "]}}";
        assertSamePage(bonus, bySeq, 0);
        assertSamePage(bonus, null, 0);
        // The match's ranking alone.
        assertSamePage(kept, null, 0);
        // The filter in must_not.
        String notDropped = "{\"bool\":{\"must\":[" + must + "],\"must_not\":[{\"term\":{\"tags\":\"drop\"}}]}}";
        assertSamePage(notDropped, bySeq, 0);
    }

    private void create(String index, String settings) throws IOException {
        Request create = new Request("PUT", "/" + index);
        create.setJsonEntity(
            "{\"settings\":{"
                + settings
                + ",\"number_of_shards\":1,\"number_of_replicas\":0},"
                + "\"mappings\":{\"properties\":{\"message\":{\"type\":\"text\"},\"seq\":{\"type\":\"long\"},"
                + "\"status\":{\"type\":\"keyword\"},\"tags\":{\"type\":\"keyword\"}}}}"
        );
        client().performRequest(create);
    }

    /**
     * "disk disk" (the term twice in two words) outranks "disk failed on the
     * node now" (once in six or more) under BM25, so every kept document
     * ranks below every strong one. Each kept document is a word longer than
     * the one before, so no two tie on the score, and Lucene keeps each
     * length exactly (under 24 words). Each document holds two tags, so the
     * term on them is applied after the ranking.
     */
    private void load(String index) throws IOException {
        StringBuilder body = new StringBuilder();
        for (int i = 0; i < STRONG + KEPT; i++) {
            boolean kept = i >= STRONG;
            body.append("{\"index\":{\"_index\":\"").append(index).append("\",\"_id\":\"").append(kept ? "k" : "s").append(i).append("\"}}\n");
            body.append("{\"message\":\"")
                .append(kept ? "disk failed on the node now" + " again".repeat(i - STRONG) : "disk disk")
                .append("\",\"seq\":")
                .append((i * 7919L) % (STRONG + KEPT))
                .append(",\"status\":\"")
                .append(i % 2 == 0 ? "ok" : "bad")
                .append("\",\"tags\":[\"")
                .append(kept ? "keep" : "drop")
                .append("\",\"t")
                .append(i % 3)
                .append("\"]}\n");
        }
        Request bulk = new Request("POST", "/_bulk");
        bulk.addParameter("refresh", "true");
        bulk.setJsonEntity(body.toString());
        Map<String, Object> out = entityAsMap(client().performRequest(bulk));
        assertEquals(index + ": every document indexed", false, out.get("errors"));
    }

    /** The page of {@code query} sorted by {@code sort} (by score alone when null) from {@code from}: the same in each served index as in Lucene's. */
    private void assertSamePage(String query, String sort, int from) throws IOException {
        Page want = search(LUCENE, query, sort, from);
        assertEquals(query, KEPT, want.total);
        assertEquals(query, Math.min(PAGE, KEPT - from), want.ids.size());
        for (String index : SERVED) {
            Page got = search(index, query, sort, from);
            String what = index + " " + query + " sort " + sort + " from " + from;
            assertEquals(what, want.total, got.total);
            assertEquals(what, want.ids, got.ids);
            assertEquals(what, want.sorts.size(), got.sorts.size());
            for (int i = 0; i < want.scores.size(); i++) {
                assertEquals(what + " score of " + want.ids.get(i), want.scores.get(i), got.scores.get(i), 1e-4);
            }
            if (sort != null) {
                for (int i = 0; i < want.sorts.size(); i++) {
                    assertEquals(what, want.sorts.get(i).get(1), got.sorts.get(i).get(1));
                }
            }
        }
    }

    private record Page(int total, List<String> ids, List<Double> scores, List<List<Object>> sorts) {}

    @SuppressWarnings("unchecked")
    private Page search(String index, String query, String sort, int from) throws IOException {
        Request search = new Request("POST", "/" + index + "/_search");
        search.setJsonEntity(
            "{\"from\":" + from + ",\"size\":" + PAGE + ",\"track_total_hits\":true,\"query\":" + query + (sort == null ? "" : ",\"sort\":" + sort) + "}"
        );
        Map<String, Object> hits = (Map<String, Object>) entityAsMap(client().performRequest(search)).get("hits");
        int total = ((Number) ((Map<String, Object>) hits.get("total")).get("value")).intValue();
        List<String> ids = new ArrayList<>();
        List<Double> scores = new ArrayList<>();
        List<List<Object>> sorts = new ArrayList<>();
        for (Map<String, Object> hit : (List<Map<String, Object>>) hits.get("hits")) {
            ids.add(hit.get("_id").toString());
            scores.add(((Number) hit.get("_score")).doubleValue());
            if (hit.get("sort") != null) {
                sorts.add((List<Object>) hit.get("sort"));
            }
        }
        return new Page(total, ids, scores, sorts);
    }
}
