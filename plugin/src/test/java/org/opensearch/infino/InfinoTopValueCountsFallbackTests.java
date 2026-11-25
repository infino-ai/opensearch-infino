/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import com.sun.jna.Pointer;

import org.opensearch.core.index.Index;
import org.opensearch.search.aggregations.AggregationBuilders;
import org.opensearch.search.aggregations.bucket.terms.StringTerms;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Set;

/**
 * A terms aggregation over a keyword column whose top values the bridge
 * declines to give (a column without the keyword analyzer, as every column
 * on a hosted connection is) runs its own reads and answers exactly as the
 * one-call path does. The bridge is the real one, with that one call made
 * to decline on demand.
 */
public class InfinoTopValueCountsFallbackTests extends OpenSearchTestCase {

    private static final String TABLE = "top_value_counts_fallback";
    /** Documents without the field, counted in the total but in no bucket. */
    private static final int WITHOUT_TAG = 3;
    /** How many distinct counts the values cycle through, so many values tie. */
    private static final int COUNT_CYCLE = 7;

    /** Whether the top call declines, as a hosted connection's bridge does. */
    private volatile boolean declineTop;
    /** The bridge calls made, by name, in order. */
    private final List<String> calls = Collections.synchronizedList(new ArrayList<>());
    /** Answers this stub made itself, which the bridge must not free. */
    private final Set<Pointer> ours = Collections.synchronizedSet(Collections.newSetFromMap(new IdentityHashMap<>()));

    @Override
    public void setUp() throws Exception {
        super.setUp();
        // The library as the plugin loads it (the same -Dinfino.lib), wrapped below.
        InfinoNative.useLib(null);
        InfinoLib real = InfinoNative.lib();
        InfinoNative.useLib((InfinoLib) Proxy.newProxyInstance(InfinoLib.class.getClassLoader(), new Class<?>[] { InfinoLib.class }, (p, m, a) -> {
            calls.add(m.getName());
            if (declineTop && m.getName().equals("infino_value_counts_top")) {
                Pointer answer = InfinoNativeTests.cString("null");
                ours.add(answer);
                return answer;
            }
            if (m.getName().equals("infino_free_string") && ours.remove((Pointer) a[0])) {
                return null;
            }
            try {
                return m.invoke(real, a);
            } catch (InvocationTargetException e) {
                throw e.getCause();
            }
        }));
        InfinoNative.dropTable(TABLE);
        InfinoNative.ensureTable(TABLE, "{\"properties\":{\"tag\":{\"type\":\"keyword\"}}}");
    }

    @Override
    public void tearDown() throws Exception {
        try {
            InfinoNative.dropTable(TABLE);
        } finally {
            InfinoNative.useLib(null);
            super.tearDown();
        }
    }

    /** {@code distinct} values, value i in 1 + (i mod 7) documents, and a few documents without the field. */
    private static void index(int distinct) {
        List<String> sources = new ArrayList<>();
        for (int i = 0; i < distinct; i++) {
            for (int n = 0; n <= i % COUNT_CYCLE; n++) {
                sources.add("{\"tag\":\"v" + String.format(Locale.ROOT, "%05d", i) + "\"}");
            }
        }
        for (int i = 0; i < WITHOUT_TAG; i++) {
            sources.add("{\"other\":\"x\"}");
        }
        String[] ids = new String[sources.size()];
        for (int i = 0; i < ids.length; i++) {
            ids[i] = Integer.toString(i);
        }
        assertEquals(ids.length, InfinoNative.indexBatch(TABLE, ids, sources.toArray(new String[0]), ids.length).ok());
    }

    /** What a terms aggregation answers: total, each bucket's key and count, the rest and the error bound. */
    private record Answer(long total, List<String> buckets, long other, long error) {}

    private Answer run(int size) {
        TermsAggregationBuilder agg = AggregationBuilders.terms("t").field("tag").size(size);
        SearchSourceBuilder source = new SearchSourceBuilder().size(0).aggregation(agg);
        InfinoSearch.Plan plan = InfinoSearch.plan(TABLE, source, System.currentTimeMillis());
        InfinoAggregationProcessor.Counted counted = new InfinoAggregationProcessor(new Index(TABLE, "_na_")).evaluate(List.of(agg), plan, null);
        StringTerms terms = (StringTerms) counted.aggregations().get("t");
        List<String> buckets = new ArrayList<>();
        for (StringTerms.Bucket b : terms.getBuckets()) {
            buckets.add(b.getKeyAsString() + "=" + b.getDocCount());
        }
        return new Answer(counted.total(), buckets, terms.getSumOfOtherDocCounts(), terms.getDocCountError());
    }

    /** The answer with the top call taken, then declined, and the calls each made. */
    private void assertTheDeclinedCallAnswersTheSame(int distinct, int size) {
        index(distinct);
        calls.clear();
        Answer taken = run(size);
        List<String> takenCalls = new ArrayList<>(calls);
        declineTop = true;
        calls.clear();
        Answer declined = run(size);
        List<String> declinedCalls = new ArrayList<>(calls);
        // Over storage of the node's own the column has the keyword analyzer:
        // one call answers, and the value counts are not read.
        assertTrue(takenCalls.toString(), takenCalls.contains("infino_value_counts_top"));
        assertFalse(takenCalls.toString(), takenCalls.contains("infino_value_counts"));
        // Declined, the level reads the value counts itself, as before the call existed.
        assertTrue(declinedCalls.toString(), declinedCalls.contains("infino_value_counts"));
        assertEquals(taken, declined);
        // A shard keeps more buckets than the size (its shard size); the reduce cuts them.
        assertTrue(declined.buckets().toString(), declined.buckets().size() >= size);
    }

    /** More values than the value counts carry: the declined level runs its window statement. */
    public void testManyValuesAnswerTheSameWhenTheTopCallDeclines() {
        assertTheDeclinedCallAnswersTheSame(1_100, 50);
    }

    /** Few values: the declined level picks its buckets from the value counts. */
    public void testFewValuesAnswerTheSameWhenTheTopCallDeclines() {
        assertTheDeclinedCallAnswersTheSame(40, 10);
    }
}
