/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
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

    /** The clients that write at once; {@code -Dtests.infino.race.clients} raises it for a stress run. */
    private static final int CLIENTS = Integer.getInteger("tests.infino.race.clients", 2);
    /** Fresh indices written, one after another: a broken one shows in some, not all; {@code -Dtests.infino.race.trials} for more. */
    private static final int TRIALS = Integer.getInteger("tests.infino.race.trials", 8);
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
    /** In the array test, below this draw a field's value is an array instead. */
    private static final double ARRAY_BELOW = 0.4;
    /** See {@link #STRING_BELOW}. */
    private static final double NUMBER_BELOW = 0.8;
    /** The share of documents that carry the sparse string field. */
    private static final double SPARSE_SHARE = 0.2;
    /** Values a generated number or suffix is drawn below. */
    private static final int VALUES = 1000;
    /**
     * The length of the name each run gives its indices: a run repeated in
     * one cluster ({@code -Dtests.iters}) never meets an index an earlier run
     * left behind.
     */
    private static final int RUN_NAME = 8;
    /** How long the clients may take on one index, together. */
    private static final long TIMEOUT_MINUTES = 5;

    public void testConcurrentFirstWritesLeaveTheIndexHealthy() throws Exception {
        String run = randomAlphaOfLength(RUN_NAME).toLowerCase(Locale.ROOT);
        for (int trial = 0; trial < TRIALS; trial++) {
            assertHealthyAfterConcurrentFirstWrites("dyn_race_" + run + "_" + trial, trial, false);
        }
    }

    /**
     * As {@link #testConcurrentFirstWritesLeaveTheIndexHealthy}, where some
     * new fields' first values are arrays of strings or of numbers: each is
     * written once the mapping declares it, as one value per document with its
     * values listed beside it, never as a list column of the table's own
     * typing that the mapping could not then be applied over.
     */
    public void testConcurrentNewArrayFieldsLeaveTheIndexHealthy() throws Exception {
        String run = randomAlphaOfLength(RUN_NAME).toLowerCase(Locale.ROOT);
        for (int trial = 0; trial < TRIALS; trial++) {
            assertHealthyAfterConcurrentFirstWrites("dyn_arrays_" + run + "_" + trial, trial, true);
        }
    }

    /**
     * A field a scripted update adds is mapped before the document is
     * written, as a stock shard maps it: it is searched as text, and a later
     * document that brings the same field is written to a table that holds it
     * as the mapping does, rather than to one that typed it from the update's
     * document and so can no longer take the mapping.
     */
    public void testAFieldAScriptedUpdateAddsIsMappedFirst() throws Exception {
        String index = "dyn_script_" + randomAlphaOfLength(RUN_NAME).toLowerCase(Locale.ROOT);
        call(
            "PUT",
            "/" + index,
            "{\"settings\":{\"index.infino.enabled\":true,\"index.infino.mode\":\"proxy\",\"number_of_shards\":1,\"number_of_replicas\":0},"
                + "\"mappings\":{\"dynamic\":true}}"
        );
        call("PUT", "/" + index + "/_doc/1?refresh=true", "{\"n\":1}");
        call("POST", "/" + index + "/_update/1", "{\"script\":{\"source\":\"ctx._source.note = params.v\",\"params\":{\"v\":\"wool coat\"}}}");
        call("PUT", "/" + index + "/_doc/2", "{\"n\":2,\"note\":\"wool hat\"}");
        call("POST", "/" + index + "/_refresh", null);
        Map<String, Object> found = call("POST", "/" + index + "/_count", "{\"query\":{\"match\":{\"note\":\"wool\"}}}");
        assertEquals(2, ((Number) found.get("count")).intValue());
        call("DELETE", "/" + index, null);
    }

    private void assertHealthyAfterConcurrentFirstWrites(String index, int trial, boolean arrays) throws Exception {
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
                            if (arrays && kind < ARRAY_BELOW) {
                                // An array of strings below half the share, else of numbers.
                                if (kind < ARRAY_BELOW / 2) {
                                    bulk.append("[\"silk ").append(random.nextInt(VALUES)).append("\",\"wool\"]");
                                } else {
                                    bulk.append('[').append(random.nextInt(VALUES)).append(",999]");
                                }
                            } else if (kind < STRING_BELOW) {
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
