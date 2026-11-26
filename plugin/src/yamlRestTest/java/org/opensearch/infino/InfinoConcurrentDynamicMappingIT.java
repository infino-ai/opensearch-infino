/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * First writes from clients running at once into a new proxy-mode index
 * with dynamic mapping leave the index healthy: every document a bulk
 * acknowledged is stored and counted, and a new string field is searched as
 * the text field the mapping makes it. The documents bring fields under
 * {@code x} whose names both clients pick from a few and whose values
 * change type (a string, a number, an object), so the clients' mapping
 * updates keep landing between each other's and some documents are
 * refused, as stock refuses them; some documents bring a sparse string
 * field. Too many requests, sent at once, for a YAML suite.
 */
public class InfinoConcurrentDynamicMappingIT extends InfinoRestTestCase {

    /** The clients that write at once. */
    private static final int CLIENTS = 2;
    /** Fresh indices written, one after another: a broken one shows in some, not all. */
    private static final int TRIALS = 8;
    /** The bulks each client sends to an index. */
    private static final int BULKS_PER_CLIENT = 40;
    /** The documents in each bulk. */
    private static final int DOCS_PER_BULK = 20;
    /** The objects under x a document's field goes in. */
    private static final int GROUPS = 5;
    /** The names a field in one of those objects takes. */
    private static final int NAMES = 10;
    /** Below this draw a field's value is a string, below {@link #NUMBER_BELOW} a number, else an object. */
    private static final double STRING_BELOW = 0.6;
    /** See {@link #STRING_BELOW}. */
    private static final double NUMBER_BELOW = 0.8;
    /** The share of documents that carry the sparse string field. */
    private static final double SPARSE_SHARE = 0.2;
    /** Values a generated number or suffix is drawn below. */
    private static final int VALUES = 1000;
    /** How long the clients may take on one index, together. */
    private static final long TIMEOUT_MINUTES = 5;

    public void testConcurrentFirstWritesLeaveTheIndexHealthy() throws Exception {
        for (int trial = 0; trial < TRIALS; trial++) {
            assertHealthyAfterConcurrentFirstWrites("dyn_race_" + trial, trial);
        }
    }

    private void assertHealthyAfterConcurrentFirstWrites(String index, int trial) throws Exception {
        call(
            "PUT",
            "/" + index,
            "{\"settings\":{\"index.infino.enabled\":true,\"index.infino.mode\":\"proxy\",\"number_of_shards\":1,\"number_of_replicas\":0},"
                + "\"mappings\":{\"dynamic\":true}}"
        );
        AtomicInteger acked = new AtomicInteger();
        AtomicInteger sparseAcked = new AtomicInteger();
        ExecutorService clients = Executors.newFixedThreadPool(CLIENTS);
        try {
            List<Future<?>> running = new ArrayList<>();
            for (int c = 0; c < CLIENTS; c++) {
                int client = c;
                Random random = new Random(trial * CLIENTS + client);
                running.add(clients.submit(() -> {
                    for (int b = 0; b < BULKS_PER_CLIENT; b++) {
                        StringBuilder bulk = new StringBuilder();
                        List<Boolean> sparse = new ArrayList<>();
                        for (int d = 0; d < DOCS_PER_BULK; d++) {
                            boolean withText = random.nextDouble() < SPARSE_SHARE;
                            sparse.add(withText);
                            bulk.append("{\"index\":{\"_index\":\"").append(index).append("\",\"_id\":\"c").append(client).append('_').append(b);
                            bulk.append('_').append(d).append("\"}}\n");
                            bulk.append("{\"x\":{\"h").append(random.nextInt(GROUPS)).append("\":{\"k").append(random.nextInt(NAMES)).append("\":");
                            double kind = random.nextDouble();
                            if (kind < STRING_BELOW) {
                                bulk.append("\"silk scarf ").append(random.nextInt(VALUES)).append('"');
                            } else if (kind < NUMBER_BELOW) {
                                bulk.append(random.nextInt(VALUES));
                            } else {
                                bulk.append("{\"z\":\"deep ").append(random.nextInt(VALUES)).append("\"}");
                            }
                            bulk.append("}}");
                            if (withText) {
                                bulk.append(",\"t_a\":\"wool coat ").append(random.nextInt(VALUES)).append('"');
                            }
                            bulk.append("}\n");
                        }
                        Map<String, Object> answer = call("POST", "/_bulk", bulk.toString());
                        @SuppressWarnings("unchecked")
                        List<Map<String, Object>> items = (List<Map<String, Object>>) answer.get("items");
                        for (int i = 0; i < items.size(); i++) {
                            @SuppressWarnings("unchecked")
                            Map<String, Object> item = (Map<String, Object>) items.get(i).get("index");
                            if (item.get("error") == null) {
                                acked.incrementAndGet();
                                if (sparse.get(i)) {
                                    sparseAcked.incrementAndGet();
                                }
                            }
                        }
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
        call("POST", "/" + index + "/_refresh", null);
        // A table the mapping cannot be applied to answers every request with a 500.
        Map<String, Object> count = call("GET", "/" + index + "/_count", null);
        assertEquals(index + ": every acknowledged document is stored", acked.get(), ((Number) count.get("count")).intValue());
        if (sparseAcked.get() > 0) {
            Map<String, Object> found = call("POST", "/" + index + "/_count", "{\"query\":{\"match\":{\"t_a\":\"wool\"}}}");
            assertEquals(index + ": the sparse field is searched as text", sparseAcked.get(), ((Number) found.get("count")).intValue());
        }
        call("DELETE", "/" + index, null);
    }
}
