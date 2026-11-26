/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * Scripted updates of one document from clients running at once lose none
 * of the updates, in either mode and on a Lucene twin alike: each update
 * runs its script against the document as the last write left it, and one
 * that meets a write it did not see runs again ({@code retry_on_conflict}).
 * Too many requests for a YAML suite, which also cannot send two at once.
 */
public class InfinoScriptedUpdateIT extends InfinoRestTestCase {

    /** The clients that update the counter at once. */
    private static final int CLIENTS = 2;
    /** The updates each client sends, one after another. */
    private static final int UPDATES_PER_CLIENT = 100;
    /** How often an update may run again after a conflict: far more than two clients make. */
    private static final int RETRIES = 100;
    /** How long the clients may take, together. */
    private static final long TIMEOUT_MINUTES = 5;

    public void testConcurrentScriptedUpdatesLoseNoUpdateOnTheTwin() throws Exception {
        assertCounterReachesEveryUpdate("scripted_counter_luc", "\"index.infino.enabled\":false");
    }

    public void testConcurrentScriptedUpdatesLoseNoUpdateInDefaultMode() throws Exception {
        assertCounterReachesEveryUpdate("scripted_counter_def", "\"index.infino.enabled\":true");
    }

    public void testConcurrentScriptedUpdatesLoseNoUpdateInProxyMode() throws Exception {
        assertCounterReachesEveryUpdate("scripted_counter_prx", "\"index.infino.enabled\":true,\"index.infino.mode\":\"proxy\"");
    }

    private void assertCounterReachesEveryUpdate(String index, String settings) throws Exception {
        call(
            "PUT",
            "/" + index,
            "{\"settings\":{" + settings + ",\"number_of_shards\":1,\"number_of_replicas\":0},"
                + "\"mappings\":{\"properties\":{\"count\":{\"type\":\"long\"}}}}"
        );
        call("PUT", "/" + index + "/_doc/c?refresh=true", "{\"count\":0}");
        String update = "/" + index + "/_update/c?retry_on_conflict=" + RETRIES;
        String body = "{\"script\":{\"lang\":\"painless\",\"source\":\"ctx._source.count += params.by\",\"params\":{\"by\":1}}}";
        ExecutorService clients = Executors.newFixedThreadPool(CLIENTS);
        try {
            List<Future<?>> running = new ArrayList<>();
            for (int c = 0; c < CLIENTS; c++) {
                running.add(clients.submit(() -> {
                    for (int i = 0; i < UPDATES_PER_CLIENT; i++) {
                        Map<String, Object> answer = call("POST", update, body);
                        assertEquals(answer.toString(), "updated", answer.get("result"));
                    }
                    return null;
                }));
            }
            for (Future<?> f : running) {
                f.get(TIMEOUT_MINUTES, TimeUnit.MINUTES);
            }
        } finally {
            clients.shutdownNow();
        }
        Map<String, Object> got = call("GET", "/" + index + "/_doc/c", null);
        @SuppressWarnings("unchecked")
        Map<String, Object> source = (Map<String, Object>) got.get("_source");
        assertEquals("every update counted, none lost", CLIENTS * UPDATES_PER_CLIENT, ((Number) source.get("count")).intValue());
        call("DELETE", "/" + index, null);
    }
}
