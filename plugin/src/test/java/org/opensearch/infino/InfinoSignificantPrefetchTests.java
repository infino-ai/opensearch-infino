/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.opensearch.common.logging.Loggers;
import org.opensearch.core.index.Index;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.AggregationBuilders;
import org.opensearch.search.aggregations.bucket.terms.SignificantTermsAggregationBuilder;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.List;

/**
 * significant_terms under a terms parent, read through the engine: every
 * parent bucket's foreground is one statement while parent buckets times
 * candidates stays within the rows one bucket's own statement may return,
 * and one statement per bucket past that. Both give the same buckets, so
 * the statements the node logs are what tells them apart.
 */
public class InfinoSignificantPrefetchTests extends OpenSearchTestCase {

    /** The statements {@link InfinoNative#sql} logs while it is attached. */
    private static final class Statements extends AbstractAppender {
        final List<String> sql = new ArrayList<>();

        Statements() {
            super("infino-statements", null, null, false, null);
            start();
        }

        @Override
        public void append(LogEvent event) {
            String message = event.getMessage().getFormattedMessage();
            if (message.startsWith("[infino] sql: ")) {
                sql.add(message);
            }
        }
    }

    private static final String TABLE = "significant_prefetch";

    @Override
    public void setUp() throws Exception {
        super.setUp();
        InfinoNative.dropTable(TABLE);
        InfinoNative.ensureTable(TABLE, "{\"properties\":{\"p\":{\"type\":\"keyword\"},\"tag\":{\"type\":\"keyword\"}}}");
        String[] ids = { "1", "2", "3", "4" };
        String[] sources = {
            "{\"p\":\"a\",\"tag\":\"x\"}",
            "{\"p\":\"a\",\"tag\":\"x\"}",
            "{\"p\":\"b\",\"tag\":\"y\"}",
            "{\"p\":\"b\",\"tag\":\"y\"}" };
        assertEquals(4, InfinoNative.indexBatch(TABLE, ids, sources, ids.length).ok());
    }

    @Override
    public void tearDown() throws Exception {
        InfinoNative.dropTable(TABLE);
        super.tearDown();
    }

    /** The foreground statements a terms(p) → significant_terms(tag) request with this shard_size runs. */
    private static List<String> foregrounds(int shardSize) {
        SignificantTermsAggregationBuilder sig = AggregationBuilders.significantTerms("sig").field("tag").minDocCount(1).shardSize(shardSize);
        AggregationBuilder parent = AggregationBuilders.terms("p").field("p").subAggregation(sig);
        SearchSourceBuilder source = new SearchSourceBuilder().size(0).aggregation(parent);
        Statements statements = new Statements();
        Logger logger = LogManager.getLogger(InfinoNative.class);
        Level was = logger.getLevel();
        Loggers.addAppender(logger, statements);
        Loggers.setLevel(logger, Level.DEBUG);
        try {
            InfinoSearch.Plan plan = InfinoSearch.plan(TABLE, source, System.currentTimeMillis());
            new InfinoAggregationProcessor(new Index(TABLE, "_na_")).evaluate(List.of(parent), plan, null);
        } finally {
            Loggers.removeAppender(logger, statements);
            Loggers.setLevel(logger, was);
        }
        List<String> out = new ArrayList<>();
        for (String s : statements.sql) {
            // The batched read ranks within each parent; a bucket's own read cuts its own top.
            if (s.contains("ROW_NUMBER() OVER (PARTITION BY p0 ORDER BY doc_count DESC, k0 ASC)")
                || s.contains("ORDER BY doc_count DESC, k0 ASC LIMIT ")) {
                out.add(s);
            }
        }
        return out;
    }

    /** Two parents times 5,000 candidates is the bound: one statement reads both foregrounds. */
    public void testAtTheBoundBothBucketsAreReadInOneStatement() {
        List<String> read = foregrounds(5_000);
        assertEquals(read.toString(), 1, read.size());
        assertTrue(read.get(0), read.get(0).contains("PARTITION BY p0"));
    }

    /** Two parents times 5,001 candidates is past it: each bucket reads its own foreground. */
    public void testPastTheBoundEachBucketIsReadOnItsOwn() {
        List<String> read = foregrounds(5_001);
        assertEquals(read.toString(), 2, read.size());
        for (String s : read) {
            assertTrue(s, s.contains("LIMIT 5001"));
        }
    }
}
