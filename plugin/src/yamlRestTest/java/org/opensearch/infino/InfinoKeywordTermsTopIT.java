/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * An unfiltered terms aggregation in the default order over a keyword field
 * of more than 1,001 values answers as a Lucene index of the same documents
 * does, on one shard and on three, in proxy and default mode: the buckets,
 * {@code sum_other_doc_count}, {@code doc_count_error_upper_bound} and the
 * total. On three shards a default-mode index answers as a Lucene index of
 * three shards: each shard picks its effective {@code shard_size} (more than
 * {@code size}) and the reduce adds up the shards' answers, so the error
 * bounds come from each shard's own last bucket. A proxy index keeps one
 * table whatever its shard count and counts exactly, so it answers as a
 * Lucene index of one shard. The same holds
 * after deletes (the only document of a value, and documents from the top
 * buckets), with an empty string counted as its own bucket, and when a
 * document gives the field two values, which the engine's value counts do
 * not describe.
 *
 * <p>One difference is allowed. Once a segment has deletes, a one-shard
 * Lucene index collects the buckets instead of reading term frequencies and
 * reports its last collected bucket's count as the error bound, where it
 * reported 0 before. An index served from one table counts every value
 * exactly and reports 0 either way.
 */
public class InfinoKeywordTermsTopIT extends InfinoRestTestCase {

    /** The sizes asked for: inside a tie of counts at 5 and at 50, and one whose three-shard shard_size is past what the plugin picks in one call. */
    private static final List<Integer> SIZES = List.of(5, 50, 700);

    /**
     * The values and how many documents carry each. Ten heavy values whose
     * counts tie across the fifth bucket (12, 12, 12 in places 4 to 6), 80
     * at 3 so the fiftieth bucket sits inside a tie, the empty string, and
     * 3,300 values once each: 3,391 values in all, so each of three shards
     * holds more than the 1,001 a table's value counts carry.
     */
    private static Map<String, Integer> counts() {
        Map<String, Integer> counts = new LinkedHashMap<>();
        int[] heavy = { 20, 20, 15, 12, 12, 12, 9, 9, 8, 8 };
        for (int i = 0; i < heavy.length; i++) {
            counts.put(String.format(Locale.ROOT, "a%02d", i), heavy[i]);
        }
        for (int i = 0; i < 80; i++) {
            counts.put(String.format(Locale.ROOT, "b%03d", i), 3);
        }
        counts.put("", 4);
        for (int i = 0; i < 3300; i++) {
            counts.put(String.format(Locale.ROOT, "c%04d", i), 1);
        }
        return counts;
    }

    private static final String ERROR = "doc_count_error_upper_bound";

    /** Documents without the field: in the total, in no bucket. */
    private static final int WITHOUT = 5;

    public void testOneShard() throws IOException {
        assertParity("kt1", 1, false);
    }

    public void testThreeShards() throws IOException {
        assertParity("kt3", 3, false);
    }

    public void testAMultiValuedDocumentOnOneShard() throws IOException {
        assertParity("ktm1", 1, true);
    }

    public void testAMultiValuedDocumentOnThreeShards() throws IOException {
        assertParity("ktm3", 3, true);
    }

    /**
     * Loads the documents into a Lucene index and the two the plugin serves,
     * each with {@code shards} shards, compares every size, deletes, and
     * compares again. With {@code multi}, one more document gives the field
     * two values.
     */
    private void assertParity(String prefix, int shards, boolean multi) throws IOException {
        String lucene = prefix + "_luc";
        String exact = shards == 1 ? lucene : prefix + "_luc1";
        String proxy = prefix + "_proxy";
        String dflt = prefix + "_default";
        // Each served index and the Lucene index it answers as.
        Map<String, String> twins = Map.of(proxy, exact, dflt, lucene);
        // The served indices that count over one table.
        Set<String> oneTable = shards == 1 ? Set.of(proxy, dflt) : Set.of(proxy);
        List<String> all = new ArrayList<>(List.of(lucene, proxy, dflt));
        create(lucene, "\"index.infino.enabled\":false", shards);
        if (shards > 1) {
            create(exact, "\"index.infino.enabled\":false", 1);
            all.add(exact);
        }
        create(proxy, "\"index.infino.enabled\":true,\"index.infino.mode\":\"proxy\"", shards);
        create(dflt, "\"index.infino.enabled\":true", shards);
        List<String> docs = documents(multi);
        for (String index : all) {
            bulk(index, docs);
        }
        assertSameTerms(twins, Set.of(), "loaded");
        if (shards > 1) {
            // The reduce's error bound is in play: some shard left a value out.
            assertTrue(((Number) terms(search(lucene, body(5))).get(ERROR)).longValue() > 0);
        }
        // The empty string is a value of its own, inside the first 50.
        assertTrue(buckets(search(lucene, body(50))).stream().anyMatch(b -> "".equals(((Map<?, ?>) b).get("key"))));

        // The only document of c0005, three of a00's and one of a01's, so
        // the top buckets' order changes; then one of the empty string's.
        List<String> gone = new ArrayList<>(List.of(idOf(docs, "c0005", 0)));
        for (int i = 0; i < 3; i++) {
            gone.add(idOf(docs, "a00", i));
        }
        gone.add(idOf(docs, "a01", 0));
        gone.add(idOf(docs, "", 0));
        for (String index : all) {
            StringBuilder body = new StringBuilder();
            for (String id : gone) {
                body.append("{\"delete\":{\"_index\":\"").append(index).append("\",\"_id\":\"").append(id).append("\"}}\n");
            }
            Map<String, Object> out = call("POST", "/_bulk?refresh=true", body.toString());
            assertEquals(index + ": every delete applied", false, out.get("errors"));
        }
        assertSameTerms(twins, oneTable, "after deletes");
    }

    private void create(String index, String settings, int shards) throws IOException {
        call(
            "PUT",
            "/" + index,
            "{\"settings\":{"
                + settings
                + ",\"number_of_shards\":"
                + shards
                + ",\"number_of_replicas\":0},\"mappings\":{\"properties\":{\"tag\":{\"type\":\"keyword\"},\"n\":{\"type\":\"long\"}}}}"
        );
    }

    /**
     * Each document's source, its id {@code d<i>}. The values are written in
     * an interleaved order so a value's documents spread over the shards.
     */
    private static List<String> documents(boolean multi) {
        List<String> values = new ArrayList<>();
        for (Map.Entry<String, Integer> e : counts().entrySet()) {
            for (int i = 0; i < e.getValue(); i++) {
                values.add(e.getKey());
            }
        }
        List<String> docs = new ArrayList<>();
        int stride = 389;
        for (int i = 0; i < values.size(); i++) {
            String value = values.get((int) ((long) i * stride % values.size()));
            docs.add("{\"tag\":\"" + value + "\",\"n\":" + i + "}");
        }
        for (int i = 0; i < WITHOUT; i++) {
            docs.add("{\"n\":" + docs.size() + "}");
        }
        if (multi) {
            docs.add("{\"tag\":[\"a02\",\"b000\"],\"n\":" + docs.size() + "}");
        }
        return docs;
    }

    /** The id of the {@code nth} document whose tag is {@code value}. */
    private static String idOf(List<String> docs, String value, int nth) {
        String source = "{\"tag\":\"" + value + "\",";
        for (int i = 0, seen = 0; i < docs.size(); i++) {
            if (docs.get(i).startsWith(source) && seen++ == nth) {
                return "d" + i;
            }
        }
        throw new AssertionError("no document " + nth + " of " + value);
    }

    private void bulk(String index, List<String> docs) throws IOException {
        StringBuilder body = new StringBuilder();
        for (int i = 0; i < docs.size(); i++) {
            body.append("{\"index\":{\"_index\":\"").append(index).append("\",\"_id\":\"d").append(i).append("\"}}\n");
            body.append(docs.get(i)).append('\n');
        }
        Map<String, Object> out = call("POST", "/_bulk?refresh=true", body.toString());
        assertEquals(index + ": every document indexed", false, out.get("errors"));
    }

    /**
     * Each size's aggregation, and the total, are the same in each served
     * index as in its Lucene twin. For an index in {@code exact}, an error
     * bound of 0 stands for the one its one-shard twin reports.
     */
    private void assertSameTerms(Map<String, String> twins, Set<String> exact, String when) throws IOException {
        for (int size : SIZES) {
            for (Map.Entry<String, String> twin : twins.entrySet()) {
                String what = twin.getKey() + " " + when + " size " + size;
                Map<String, Object> want = search(twin.getValue(), body(size));
                assertFalse(what + ": Lucene has buckets", buckets(want).isEmpty());
                Map<String, Object> got = search(twin.getKey(), body(size));
                if (exact.contains(twin.getKey())) {
                    assertEquals(what + ": counted exactly", 0L, ((Number) terms(got).get(ERROR)).longValue());
                    terms(got).put(ERROR, terms(want).get(ERROR));
                }
                assertEquals(what, want, got);
            }
        }
    }

    private static String body(int size) {
        return "{\"size\":0,\"track_total_hits\":true,\"aggs\":{\"t\":{\"terms\":{\"field\":\"tag\",\"size\":" + size + "}}}}";
    }

    /** The parts of a search's answer the aggregation decides: the total and the aggregations. */
    @SuppressWarnings("unchecked")
    private Map<String, Object> search(String index, String body) throws IOException {
        Map<String, Object> out = call("POST", "/" + index + "/_search", body);
        Map<String, Object> hits = (Map<String, Object>) out.get("hits");
        Map<String, Object> kept = new LinkedHashMap<>();
        kept.put("total", hits.get("total"));
        kept.put("aggregations", out.get("aggregations"));
        return kept;
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> terms(Map<String, Object> answer) {
        return (Map<String, Object>) ((Map<String, Object>) answer.get("aggregations")).get("t");
    }

    @SuppressWarnings("unchecked")
    private static List<Object> buckets(Map<String, Object> answer) {
        return (List<Object>) terms(answer).get("buckets");
    }
}
