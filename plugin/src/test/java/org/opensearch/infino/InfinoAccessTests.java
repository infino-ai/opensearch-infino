/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import org.apache.lucene.search.join.ScoreMode;
import org.opensearch.OpenSearchSecurityException;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.index.query.MatchNoneQueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.infino.InfinoNative.Column;
import org.opensearch.test.OpenSearchTestCase;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.UnaryOperator;

/** The security plugin's rules as the table applies them: columns, sources and the document predicate. */
public class InfinoAccessTests extends OpenSearchTestCase {

    private static final UnaryOperator<String> HASH = s -> "h(" + s + ")";

    private static Map<String, Column> cols() {
        Map<String, Column> cols = new LinkedHashMap<>();
        cols.put("osid", new Column("osid", "str", false, false));
        cols.put("source", new Column("source", "str", false, false));
        cols.put("tag", new Column("tag", "str", false, false));
        cols.put("secret", new Column("secret", "str", false, false));
        cols.put("ip", new Column("ip", "str", false, false));
        cols.put("n", new Column("n", "i64", false, false));
        return cols;
    }

    public void testUnrestrictedAccessChangesNothing() {
        Map<String, Column> cols = cols();
        assertSame(cols, InfinoAccess.NONE.columns(cols));
        String src = "{\"tag\":\"x\",\"secret\":\"a\"}";
        assertSame(src, InfinoAccess.NONE.source(src));
        assertNull(InfinoAccess.NONE.documentsWhere(cols, "t", 0));
        assertTrue(InfinoAccess.NONE.unrestricted());
    }

    public void testHiddenFieldLeavesTheColumnsAndTheSource() {
        // A rule hiding every field named secret and, as the security plugin's
        // recursive check does, everything under a hidden object.
        InfinoAccess access = new InfinoAccess(
            null,
            field -> !(field.equals("secret") || field.startsWith("secret.") || field.endsWith(".secret") || field.contains(".secret.")),
            null
        );
        Map<String, Column> cols = access.columns(cols());
        assertFalse(cols.containsKey("secret"));
        assertTrue(cols.containsKey("tag"));
        assertTrue(cols.containsKey("osid"));
        assertTrue(cols.containsKey("source"));
        String shown = access.source("{\"tag\":\"x\",\"secret\":{\"a\":1},\"nested\":{\"secret\":2,\"keep\":3},\"list\":[{\"secret\":1},{\"keep\":2}]}");
        // A visible object stays, with whatever of it is visible, an empty one included.
        assertEquals("{\"tag\":\"x\",\"nested\":{\"keep\":3},\"list\":[{},{\"keep\":2}]}", shown);
    }

    public void testIncludedObjectKeepsItsMembers() {
        // An include rule on a.b shows a.b.c; a itself is shown for what it holds.
        InfinoAccess access = new InfinoAccess(null, field -> field.equals("a.b") || field.startsWith("a.b."), null);
        String shown = access.source("{\"a\":{\"b\":{\"c\":1,\"d\":2},\"e\":3},\"f\":4}");
        assertEquals("{\"a\":{\"b\":{\"c\":1,\"d\":2}}}", shown);
    }

    public void testMaskedFieldHashesValuesAndMarksTheColumn() {
        InfinoAccess access = new InfinoAccess(null, null, field -> field.equals("ip") ? HASH : null);
        Map<String, Column> cols = access.columns(cols());
        assertTrue(cols.get("ip").masked());
        assertFalse(cols.get("tag").masked());
        assertFalse(InfinoAccess.scriptColumns(cols).containsKey("ip"));
        assertTrue(InfinoAccess.scriptColumns(cols).containsKey("tag"));
        String shown = access.source("{\"ip\":\"10.0.0.1\",\"ips\":[\"a\",\"b\"],\"tag\":\"x\",\"ip2\":5}");
        assertEquals("{\"ip\":\"h(10.0.0.1)\",\"ips\":[\"a\",\"b\"],\"tag\":\"x\",\"ip2\":5}", shown);
    }

    public void testDocumentRuleBecomesOnePredicate() {
        Map<String, Column> cols = cols();
        InfinoAccess one = new InfinoAccess(List.of(QueryBuilders.termQuery("tag", "x")), null, null);
        assertEquals("\"tag\" = 'x'", one.documentsWhere(cols, "t", 0));
        InfinoAccess either = new InfinoAccess(List.of(QueryBuilders.termQuery("tag", "x"), QueryBuilders.termQuery("n", 3)), null, null);
        assertEquals("(\"tag\" = 'x' OR \"n\" = 3)", either.documentsWhere(cols, "t", 0));
        InfinoAccess none = new InfinoAccess(List.of(new MatchNoneQueryBuilder()), null, null);
        assertEquals(InfinoQueryTranslator.NONE, none.documentsWhere(cols, "t", 0));
    }

    public void testAnAliasFilterNarrowsEveryDocumentRule() {
        Map<String, Column> cols = cols();
        assertSame(InfinoAccess.NONE, InfinoAccess.NONE.filteredBy(null));
        // No rule: the filter is the rule.
        assertEquals("\"n\" = 3", InfinoAccess.NONE.filteredBy(QueryBuilders.termQuery("n", 3)).documentsWhere(cols, "t", 0));
        // A rule: a row is kept when the rule and the filter both keep it.
        InfinoAccess either = new InfinoAccess(List.of(QueryBuilders.termQuery("tag", "x"), QueryBuilders.termQuery("tag", "y")), null, null);
        String where = either.filteredBy(QueryBuilders.termQuery("n", 3)).documentsWhere(cols, "t", 0);
        assertTrue(where, where.contains("\"tag\" = 'x'") && where.contains("\"tag\" = 'y'") && where.contains("\"n\" = 3"));
        assertTrue(where, where.startsWith("(") && where.contains(" OR "));
    }

    public void testDocumentRuleTheTableCannotAnswerRefuses() {
        InfinoAccess access = new InfinoAccess(List.of(QueryBuilders.nestedQuery("p", QueryBuilders.matchAllQuery(), ScoreMode.None)), null, null);
        OpenSearchSecurityException e = expectThrows(OpenSearchSecurityException.class, () -> access.documentsWhere(cols(), "t", 0));
        assertEquals(RestStatus.FORBIDDEN, e.status());
    }
}
