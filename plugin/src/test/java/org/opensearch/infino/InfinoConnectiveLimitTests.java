/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import org.opensearch.core.rest.RestStatus;
import org.opensearch.infino.InfinoNative.InfinoException;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.List;

/**
 * A statement past the engine's limit of AND / OR connectives, read through
 * the engine: refused with a 400 that names the limit, not the engine's
 * internal error, as it is too large a query rather than a fault.
 */
public class InfinoConnectiveLimitTests extends OpenSearchTestCase {

    private static final String TABLE = "connective_limit";

    @Override
    public void setUp() throws Exception {
        super.setUp();
        InfinoNative.dropTable(TABLE);
        InfinoNative.ensureTable(TABLE, "{\"properties\":{\"node\":{\"type\":\"keyword\"},\"fileId\":{\"type\":\"long\"}}}");
        String[] ids = { "1", "2" };
        String[] sources = { "{\"node\":\"n1\",\"fileId\":1}", "{\"node\":\"n2\",\"fileId\":2}" };
        assertEquals(ids.length, InfinoNative.indexBatch(TABLE, ids, sources, ids.length).ok());
    }

    @Override
    public void tearDown() throws Exception {
        InfinoNative.dropTable(TABLE);
        super.tearDown();
    }

    /** A count of rows matching {@code pairs} (node, file) pairs, each its own {@code (a AND b)}, joined by OR. */
    private static String pairs(int pairs) {
        List<String> or = new ArrayList<>();
        for (int i = 1; i <= pairs; i++) {
            or.add("(\"node\" = 'n" + i + "' AND \"fileId\" = " + i + ")");
        }
        return "SELECT COUNT(*) AS n FROM \"" + TABLE + "\" WHERE " + String.join(" OR ", or);
    }

    public void testAStatementWithinTheLimitIsAnswered() {
        // 341 pairs: 341 ANDs and 340 ORs, 681 connectives.
        assertEquals(2L, ((Number) InfinoNative.sqlRows(pairs(341)).get(0).get("n")).longValue());
    }

    public void testAStatementPastTheLimitIsA400NamingIt() {
        // 600 pairs: 1,199 connectives.
        InfinoException e = expectThrows(InfinoException.class, () -> InfinoNative.sql(pairs(600)));
        assertEquals(RestStatus.BAD_REQUEST, e.status());
        assertTrue(e.getMessage(), e.getMessage().contains("more than " + InfinoNative.MAX_CONNECTIVES + " AND/OR connectives"));
    }

    public void testAnyOtherEngineErrorKeepsItsStatus() {
        InfinoException e = expectThrows(InfinoException.class, () -> InfinoNative.sql("SELECT no_such_column FROM \"" + TABLE + "\""));
        assertNotEquals(RestStatus.BAD_REQUEST, e.status());
        assertFalse(e.getMessage(), e.getMessage().contains(InfinoNative.CONNECTIVE_LIMIT));
    }
}
