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
import java.util.Base64;
import java.util.List;
import java.util.Map;

/**
 * A text search under a filter is the top hits among the documents the filter
 * keeps, as Lucene applies a filter while it collects, however far below the
 * unfiltered ranking those documents sit. The corpus is too large for a YAML
 * suite: the matching documents rank below more stronger matches than any
 * candidate window the plugin or the engine ever drew from.
 */
public class InfinoRankedFilterIT extends OpenSearchRestTestCase {

    /** The admin user of the test cluster's internal user database. */
    private static final String ADMIN_BASIC = "admin:admin";
    private static final String INDEX = "ranked_filter";
    /**
     * Bulks, each committed as its own file holding one matching document, so
     * no file is skipped by its statistics before the ranking.
     */
    private static final int BULKS = 11;
    /**
     * Stronger matches per bulk: across the bulks, more than the 100,000 hits
     * the engine widened a filtered ranking to before it ranked within the
     * filter's rows, and far more than the plugin's old 10,000 candidates.
     */
    private static final int STRONG_PER_BULK = 10_000;
    /** The matching documents: one per bulk. */
    private static final int MATCHING = BULKS;
    /** A page smaller than the matching set. */
    private static final int PAGE = 10;
    /** A page larger than the matching set. */
    private static final int WIDE_PAGE = 20;
    /** Where a later page starts. */
    private static final int LATER_FROM = 5;
    /** The strong documents' date, long before any "now" a date-math filter reads. */
    private static final String STRONG_DATE = "2020-01-01T00:00:00Z";

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

    public void testFilterAppliesBeforeTheRanking() throws IOException {
        Request create = new Request("PUT", "/" + INDEX);
        create.setJsonEntity(
            "{\"settings\":{\"index.infino.enabled\":true,\"number_of_shards\":1,\"number_of_replicas\":0},"
                + "\"mappings\":{\"properties\":{\"message\":{\"type\":\"text\"},\"level\":{\"type\":\"keyword\"},"
                + "\"status\":{\"type\":\"long\"},\"host\":{\"type\":\"keyword\"},\"at\":{\"type\":\"date\"}}}}"
        );
        client().performRequest(create);

        // "disk disk" (the term twice in a two-word message) outranks "disk
        // failed on the node now" (once in six words) under BM25, so every
        // matching document ranks below every strong one. Only the matching
        // documents have a host, and only they are dated today.
        long now = System.currentTimeMillis();
        for (int b = 0; b < BULKS; b++) {
            StringBuilder body = new StringBuilder();
            for (int i = 0; i < STRONG_PER_BULK; i++) {
                body.append("{\"index\":{\"_index\":\"").append(INDEX).append("\",\"_id\":\"s").append(b).append('-').append(i).append("\"}}\n");
                body.append("{\"message\":\"disk disk\",\"level\":\"info\",\"status\":200,\"at\":\"").append(STRONG_DATE).append("\"}\n");
            }
            body.append("{\"index\":{\"_index\":\"").append(INDEX).append("\",\"_id\":\"m").append(b).append("\"}}\n");
            body.append("{\"message\":\"disk failed on the node now\",\"level\":\"error\",\"status\":500,\"host\":\"db-7\",\"at\":")
                .append(now)
                .append("}\n");
            Request bulk = new Request("POST", "/_bulk");
            if (b == BULKS - 1) {
                bulk.addParameter("refresh", "true");
            }
            bulk.setJsonEntity(body.toString());
            Map<String, Object> out = entityAsMap(client().performRequest(bulk));
            assertEquals("bulk " + b + " indexed every document", false, out.get("errors"));
        }

        // A keyword term, the shape of every log view's filter.
        String byLevel = "{\"bool\":{\"must\":[{\"match\":{\"message\":\"disk\"}}],\"filter\":[{\"term\":{\"level\":\"error\"}}]}}";
        assertMatchingPage(byLevel, 0, PAGE, PAGE);
        assertMatchingPage(byLevel, 0, WIDE_PAGE, MATCHING);
        assertMatchingPage(byLevel, LATER_FROM, PAGE, MATCHING - LATER_FROM);
        // A numeric range.
        String byStatus = "{\"bool\":{\"must\":[{\"match\":{\"message\":\"disk\"}}],\"filter\":[{\"range\":{\"status\":{\"gte\":500}}}]}}";
        assertMatchingPage(byStatus, 0, PAGE, PAGE);

        // Every other shape the translator writes a filter as rides with the
        // ranked search the same way: the engine evaluates the whole WHERE
        // over the rows it ranks within, not only a column comparison.
        StringBuilder ids = new StringBuilder();
        for (int b = 0; b < BULKS; b++) {
            ids.append(b == 0 ? "" : ",").append("\"m").append(b).append('"');
        }
        List<String> filters = List.of(
            "{\"terms\":{\"level\":[\"error\",\"fatal\"]}}",
            "{\"regexp\":{\"level\":\"err.*\"}}",
            "{\"wildcard\":{\"level\":\"e*r\"}}",
            "{\"prefix\":{\"level\":\"err\"}}",
            "{\"exists\":{\"field\":\"host\"}}",
            "{\"bool\":{\"should\":[{\"term\":{\"level\":\"fatal\"}},{\"range\":{\"status\":{\"gte\":500}}}],\"minimum_should_match\":1}}",
            "{\"bool\":{\"must_not\":[{\"term\":{\"level\":\"info\"}}]}}",
            "{\"range\":{\"at\":{\"gte\":\"now-1d\"}}}",
            "{\"script\":{\"script\":{\"source\":\"doc['status'].value >= 500\",\"lang\":\"painless\"}}}",
            "{\"query_string\":{\"query\":\"level:error AND status:[500 TO *]\"}}",
            "{\"ids\":{\"values\":[" + ids + "]}}"
        );
        for (String filter : filters) {
            assertMatchingPage("{\"bool\":{\"must\":[{\"match\":{\"message\":\"disk\"}}],\"filter\":[" + filter + "]}}", 0, PAGE, PAGE);
        }
        // A must_not beside the ranked clause rather than inside its filter.
        String notInfo = "{\"bool\":{\"must\":[{\"match\":{\"message\":\"disk\"}}],\"must_not\":[{\"term\":{\"level\":\"info\"}}]}}";
        assertMatchingPage(notInfo, 0, WIDE_PAGE, MATCHING);
    }

    /**
     * One page of {@code query} from {@code from}, {@code size} long: the total
     * counts every matching document, the page holds {@code expectedHits} of
     * them, and every hit is one of them.
     */
    @SuppressWarnings("unchecked")
    private void assertMatchingPage(String query, int from, int size, int expectedHits) throws IOException {
        Request search = new Request("POST", "/" + INDEX + "/_search");
        search.setJsonEntity("{\"from\":" + from + ",\"size\":" + size + ",\"track_total_hits\":true,\"query\":" + query + "}");
        Map<String, Object> hits = (Map<String, Object>) entityAsMap(client().performRequest(search)).get("hits");
        Map<String, Object> total = (Map<String, Object>) hits.get("total");
        assertEquals(query, MATCHING, ((Number) total.get("value")).intValue());
        List<Map<String, Object>> page = (List<Map<String, Object>>) hits.get("hits");
        assertEquals(query + " from " + from + " size " + size, expectedHits, page.size());
        for (Map<String, Object> hit : page) {
            Map<String, Object> source = (Map<String, Object>) hit.get("_source");
            assertEquals(hit.toString(), "error", source.get("level"));
            assertTrue(hit.toString(), hit.get("_id").toString().startsWith("m"));
        }
    }
}
