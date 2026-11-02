/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import org.opensearch.OpenSearchSecurityException;
import org.opensearch.rest.RestRequest;
import org.opensearch.test.OpenSearchTestCase;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.UnaryOperator;

public class InfinoSqlTests extends OpenSearchTestCase {

    private static final String IP = "\"ip\"";

    public void testANamedHashIsThatDigestInHex() {
        assertEquals("encode(digest(CAST(\"ip\" AS VARCHAR), 'sha256'), 'hex')", InfinoSql.masked(IP, "ip::SHA-256", null));
        assertEquals("encode(digest(CAST(\"ip\" AS VARCHAR), 'md5'), 'hex')", InfinoSql.masked(IP, "ip::md5", null));
        // A rule that names no algorithm takes the node's default.
        assertEquals("encode(digest(CAST(\"ip\" AS VARCHAR), 'sha512'), 'hex')", InfinoSql.masked(IP, "ip", "SHA-512"));
    }

    public void testARegularExpressionMaskIsItsReplacementsInOrder() {
        assertEquals(
            "regexp_replace(regexp_replace(CAST(\"ip\" AS VARCHAR), '\\.[0-9]+$', '.0', 'g'), '^10', 'ten', 'g')",
            InfinoSql.masked(IP, "ip::/\\.[0-9]+$/::.0::/^10/::ten", null)
        );
    }

    public void testAJavaReplacementReadsAsTheEnginesRegularExpressionsTakeIt() {
        assertEquals("${1}-${2}x", InfinoSql.replacement("$1-$2x", "r"));
        assertEquals("$$5 ${name}", InfinoSql.replacement("\\$5 ${name}", "r"));
    }

    public void testSeveralIndicesReadAsOneRowSetColumnForColumn() {
        InfinoSql.Part a = new InfinoSql.Part("os_a", new LinkedHashMap<>(Map.of("n", "\"n\"")), true, null);
        Map<String, String> bColumns = new LinkedHashMap<>();
        bColumns.put("n", "\"n\"");
        bColumns.put("ip", InfinoSql.masked(IP, "ip::SHA-256", null));
        InfinoSql.Part b = new InfinoSql.Part("os_b", bColumns, false, "\"tag\" = 'x'");
        // Each row also gives its name in the engine: its _id and its table.
        assertEquals(
            "SELECT coalesce(\"n\", NULL) AS \"n\", coalesce(\"_id\", NULL) AS \"_id\", coalesce('os_a', NULL) AS \"_table\", NULL AS \"ip\" "
                + "FROM \"os_a\" UNION ALL "
                + "SELECT coalesce(\"n\", NULL) AS \"n\", coalesce(\"_id\", NULL) AS \"_id\", coalesce('os_b', NULL) AS \"_table\", "
                + "coalesce(encode(digest(CAST(\"ip\" AS VARCHAR), 'sha256'), 'hex'), NULL) AS \"ip\" "
                + "FROM \"os_b\" WHERE \"tag\" = 'x'",
            InfinoSql.view(List.of(a, b), false)
        );
        // One index read whole is its table as stored, _id among its columns, with the table's name.
        assertEquals("SELECT *, 'os_a' AS \"_table\" FROM \"os_a\"", InfinoSql.view(List.of(a), false));
    }

    public void testASearchReadsTheSearchOverEachTableWithItsScore() {
        InfinoSql.Part a = new InfinoSql.Part("os_a", new LinkedHashMap<>(Map.of("n", "\"n\"")), true, null);
        InfinoSql.Part b = new InfinoSql.Part("os_b", new LinkedHashMap<>(Map.of("n", "\"n\"")), false, "\"tag\" = 'x'");
        assertEquals("SELECT *, 'os_a' AS \"_table\" FROM infino_search('os_a')", InfinoSql.view(List.of(a), true));
        assertEquals(
            "SELECT \"n\", \"_id\", 'os_b' AS \"_table\", \"score\" FROM infino_search('os_b') WHERE \"tag\" = 'x'",
            InfinoSql.view(List.of(b), true)
        );
        assertEquals(
            "SELECT coalesce(\"n\", NULL) AS \"n\", coalesce(\"_id\", NULL) AS \"_id\", coalesce('os_a', NULL) AS \"_table\", "
                + "coalesce(\"score\", NULL) AS \"score\" FROM infino_search('os_a') UNION ALL "
                + "SELECT coalesce(\"n\", NULL) AS \"n\", coalesce(\"_id\", NULL) AS \"_id\", coalesce('os_b', NULL) AS \"_table\", "
                + "coalesce(\"score\", NULL) AS \"score\" FROM infino_search('os_b') WHERE \"tag\" = 'x'",
            InfinoSql.view(List.of(a, b), true)
        );
    }

    public void testUnderAFieldRuleTheSourceIsNeitherReadNorSearched() {
        Map<String, InfinoNative.Column> columns = new LinkedHashMap<>();
        for (String name : List.of("osid", "source", "tag", "secret")) {
            columns.put(name, new InfinoNative.Column(name, "str", false, false));
        }
        InfinoAccess fls = new InfinoAccess(null, field -> !field.equals("secret"), null);
        InfinoSql.Part part = InfinoSql.part("os_a", columns, fls, null, 0);
        assertFalse(part.columns().containsKey("source"));
        // The source holds the hidden field as written: no search may match on it.
        assertTrue(part.unsearchable().contains("source"));
        assertTrue(part.unsearchable().contains("secret"));
        // Read whole, the source is a column like any other.
        InfinoSql.Part whole = InfinoSql.part("os_a", columns, InfinoAccess.NONE, null, 0);
        assertFalse(whole.unsearchable().contains("source"));
    }

    public void testTheEnginesBookkeepingColumnsAreNoColumnOfAView() {
        Map<String, InfinoNative.Column> columns = new LinkedHashMap<>();
        for (String name : List.of("osid", "tag", "osrouting", "osseq", "osop", "osgen")) {
            columns.put(name, new InfinoNative.Column(name, "str", false, false));
        }
        InfinoSql.Part part = InfinoSql.part("os_a", columns, InfinoAccess.NONE, null, 0);
        assertEquals(List.of("osid", "tag"), List.copyOf(part.columns().keySet()));
        assertFalse(part.whole());
    }

    public void testTheRouteAnswersPostOnly() {
        assertEquals(List.of(RestRequest.Method.POST), InfinoSqlRoute.methods());
    }

    public void testAHashSqlCannotGiveIsLeftToTheResult() {
        // The default: a BLAKE2b keyed with the cluster's salt.
        assertNull(InfinoSql.masked(IP, "ip", null));
        assertNull(InfinoSql.masked(IP, "ip", "BLAKE2B_LEGACY_DEFAULT"));
        assertNull(InfinoSql.masked(IP, "ip::SHA-1", null));
        // A rule the security plugin would not read is refused.
        expectThrows(OpenSearchSecurityException.class, () -> InfinoSql.masked(IP, "ip::\\.x::y", null));
    }

    public void testAResultColumnGivenAsOutputIsMaskedByItsField() {
        UnaryOperator<String> mask = v -> "h(" + v + ")";
        String answer = "{\"schema\":[{\"name\":\"addr\",\"type\":\"Int64\"},{\"name\":\"n\",\"type\":\"Int64\"}],"
            + "\"rows\":[{\"addr\":7,\"n\":1},{\"addr\":null,\"n\":2}],\"masked\":{\"addr\":\"ip\"}}";
        assertEquals(
            "{\"schema\":[{\"name\":\"addr\",\"type\":\"Utf8\"},{\"name\":\"n\",\"type\":\"Int64\"}],"
                + "\"rows\":[{\"addr\":\"h(7)\",\"n\":1},{\"addr\":null,\"n\":2}]}",
            InfinoSql.maskResult(answer, Map.of("ip", mask))
        );
        // A column the bridge names for a field with no mask is refused rather than served.
        expectThrows(OpenSearchSecurityException.class, () -> InfinoSql.maskResult(answer, Map.of("other", mask)));
    }
}
