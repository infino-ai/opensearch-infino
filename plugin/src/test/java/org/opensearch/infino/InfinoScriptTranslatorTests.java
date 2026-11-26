/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import org.opensearch.infino.InfinoNative.Column;
import org.opensearch.infino.InfinoScriptTranslator.Sql;
import org.opensearch.infino.InfinoScriptTranslator.SqlType;
import org.opensearch.script.Script;
import org.opensearch.script.ScriptType;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Map;

/** Painless scripts become column expressions; what cannot be read is refused with the script named. */
public class InfinoScriptTranslatorTests extends OpenSearchTestCase {

    private static final Map<String, Column> COLS = Map.of(
        "price",
        new Column("price", "f64", false, false),
        "qty",
        new Column("qty", "i64", false, false),
        "name",
        new Column("name", "str", false, false),
        "active",
        new Column("active", "bool", false, false),
        "@timestamp",
        new Column("@timestamp", "ts", false, false)
    );
    private static final long NOW = 1_700_000_000_000L;

    private static Sql sql(String code) {
        return sql(code, Map.of());
    }

    private static Sql sql(String code, Map<String, Object> params) {
        return InfinoScriptTranslator.translate(new Script(ScriptType.INLINE, "painless", code, params), COLS, null, NOW);
    }

    public void testArithmeticOverDocValues() {
        Sql s = sql("doc['price'].value * doc['qty'].value");
        assertEquals("(\"price\" * \"qty\")", s.expr());
        assertEquals(SqlType.FLOAT, s.type());
        // Integer division stays integer, as in Painless.
        assertEquals(SqlType.INT, sql("doc['qty'].value / 2").type());
        assertEquals(SqlType.FLOAT, sql("doc['qty'].value / 2.0").type());
        assertEquals("(-\"qty\")", sql("-doc['qty'].value").expr());
    }

    public void testComparisonsAndLogic() {
        Sql s = sql("doc['price'].value > 10 && !(doc['qty'].value == 0) || doc['name'].value != null");
        assertEquals(SqlType.BOOL, s.type());
        assertEquals("(((\"price\" > 10) AND NOT ((\"qty\" = 0))) OR (\"name\" IS NOT NULL))", s.expr());
        assertEquals("(\"price\" <> 5.5)", sql("doc['price'].value != 5.5").expr());
    }

    public void testParamsAreLiterals() {
        Sql s = sql("doc['price'].value * params.rate > params.min && doc['name'].value == params.who", Map.of("rate", 2, "min", 1.5, "who", "a'b"));
        assertEquals("(((\"price\" * 2) > 1.5) AND (\"name\" = 'a''b'))", s.expr());
        assertEquals("NULL", sql("params.missing").expr());
        assertEquals("'x'", sql("params['who']", Map.of("who", "x")).expr());
    }

    public void testStringsConcatenateWithPlus() {
        Sql s = sql("doc['name'].value + '-' + doc['qty'].value");
        assertEquals(SqlType.STRING, s.type());
        assertEquals("((\"name\" || '-') || CAST(\"qty\" AS VARCHAR))", s.expr());
        assertEquals("lower(\"name\")", sql("doc['name'].value.toLowerCase()").expr());
        assertEquals("(strpos(\"name\", 'ab') > 0)", sql("doc['name'].value.contains('ab')").expr());
        assertEquals("substr(\"name\", 1 + 1, 3 - 1)", sql("doc['name'].value.substring(1, 3)").expr());
        assertEquals("length(\"name\")", sql("doc['name'].value.length()").expr());
    }

    public void testDocSizeAndEmptyGuardMissingValues() {
        assertEquals("CASE WHEN \"price\" IS NULL THEN 0 ELSE 1 END", sql("doc['price'].size()").expr());
        assertEquals("(\"price\" IS NULL)", sql("doc['price'].empty").expr());
        Sql s = sql("doc['price'].size() > 0 ? doc['price'].value : 0");
        assertEquals("CASE WHEN (CASE WHEN \"price\" IS NULL THEN 0 ELSE 1 END > 0) THEN \"price\" ELSE 0 END", s.expr());
        assertEquals(SqlType.FLOAT, s.type());
        // A field the table lacks is NULL, never an error.
        assertEquals("NULL", sql("doc['nope'].value").expr());
    }

    public void testDocSizeAndEmptyCountAListColumnsValues() {
        // A document that gave the field several values holds them in its list column, its own column null.
        Map<String, Column> cols = Map.of("tags", new Column("tags", "str", false, false), "tags[]", Column.list("tags[]", "str"));
        Script size = new Script(ScriptType.INLINE, "painless", "doc['tags'].size()", Map.of());
        assertEquals(
            "CASE WHEN \"tags[]\" IS NOT NULL THEN CAST(cardinality(\"tags[]\") AS BIGINT) WHEN \"tags\" IS NOT NULL THEN 1 ELSE 0 END",
            InfinoScriptTranslator.translate(size, cols, null, NOW).expr()
        );
        Script empty = new Script(ScriptType.INLINE, "painless", "doc['tags'].empty", Map.of());
        assertEquals("(\"tags\" IS NULL AND \"tags[]\" IS NULL)", InfinoScriptTranslator.translate(empty, cols, null, NOW).expr());
    }

    public void testStatementsBecomeOneExpression() {
        Sql s = sql("def t = doc['price'].value * params.rate; return t > params.min;", Map.of("rate", 2, "min", 200));
        assertEquals("((\"price\" * 2) > 200)", s.expr());
        s = sql("if (doc['price'].value >= 100) { return 'high'; } else if (doc['price'].value >= 50) { return 'mid'; } return 'low';");
        assertEquals("CASE WHEN (\"price\" >= 100) THEN 'high' ELSE CASE WHEN (\"price\" >= 50) THEN 'mid' ELSE 'low' END END", s.expr());
        assertEquals(SqlType.STRING, s.type());
        s = sql("long x = 42; if (x > 0) return x; return 0");
        assertEquals("CASE WHEN (42 > 0) THEN 42 ELSE 0 END", s.expr());
    }

    public void testDashboardsFilterHelperForm() {
        Sql s = sql(
            "boolean compare(Supplier s, def v) {return s.get() == v;}compare(() -> { return doc['name'].value }, params.value);",
            Map.of("value", "hats")
        );
        assertEquals("(\"name\" = 'hats')", s.expr());
        s = sql(
            "boolean gte(Supplier s, def v) {return s.get() >= v;}boolean lte(Supplier s, def v) {return s.get() <= v;}"
                + "gte(() -> { doc['price'].value * doc['qty'].value }, params.gte) && lte(() -> { doc['price'].value * doc['qty'].value }, params.lte)",
            Map.of("gte", 100, "lte", 150)
        );
        assertEquals("(((\"price\" * \"qty\") >= 100) AND ((\"price\" * \"qty\") <= 150))", s.expr());
        assertEquals(SqlType.BOOL, s.type());
    }

    public void testDatesAndMath() {
        assertEquals("CAST(date_part('hour', \"@timestamp\") AS BIGINT)", sql("doc['@timestamp'].value.getHour()").expr());
        // Painless's getter shortcut, as the Dashboards sample flights' hour_of_day scripted field reads it.
        assertEquals("CAST(date_part('hour', \"@timestamp\") AS BIGINT)", sql("doc['@timestamp'].value.hour").expr());
        assertEquals(sql("doc['@timestamp'].value.getDayOfWeekEnum().getValue()").expr(), sql("doc['@timestamp'].value.dayOfWeekEnum.value").expr());
        assertEquals("CAST(date_part('month', \"@timestamp\") AS BIGINT)", sql("doc['@timestamp'].value.monthValue").expr());
        assertEquals("arrow_cast(\"@timestamp\", 'Int64')", sql("doc['@timestamp'].value.toInstant().toEpochMilli()").expr());
        assertEquals("arrow_cast(\"@timestamp\", 'Int64')", sql("doc['@timestamp'].value.getMillis()").expr());
        assertEquals("arrow_cast(\"@timestamp\", 'Int64')", sql("new Date(doc['@timestamp'].value).getTime()").expr());
        assertEquals("(((CAST(date_part('dow', \"@timestamp\") AS BIGINT) + 6) % 7) + 1)", sql("doc['@timestamp'].value.getDayOfWeekEnum().getValue()").expr());
        Sql days = sql("(doc['@timestamp'].value.toInstant().toEpochMilli() - params.t) / (1000.0 * 60 * 60 * 24)", Map.of("t", 0));
        assertEquals(SqlType.FLOAT, days.type());
        assertEquals("((arrow_cast(\"@timestamp\", 'Int64') - 0) / (((1000.0 * 60) * 60) * 24))", days.expr());
        assertEquals("((" + NOW + " - arrow_cast(\"@timestamp\", 'Int64')) / 86400000)", sql("ChronoUnit.DAYS.between(doc['@timestamp'].value, Instant.ofEpochMilli(System.currentTimeMillis()))").expr());
        assertEquals("(arrow_cast(\"@timestamp\", 'Int64') > 5)", sql("doc['@timestamp'].value > 5").expr());
        assertEquals("CAST(round(\"price\") AS BIGINT)", sql("Math.round(doc['price'].value)").expr());
        assertEquals("greatest(\"price\", 1)", sql("Math.max(doc['price'].value, 1)").expr());
        assertEquals("ln(\"price\")", sql("Math.log(doc['price'].value)").expr());
        assertEquals("CAST(trunc(\"price\") AS BIGINT)", sql("(long) doc['price'].value").expr());
        assertEquals("CAST(\"name\" AS BIGINT)", sql("Integer.parseInt(doc['name'].value)").expr());
    }

    public void testValueScriptReadsTheField() {
        Sql s = InfinoScriptTranslator.translate(new Script(ScriptType.INLINE, "painless", "_value / 10", Map.of()), COLS, COLS.get("price"), NOW);
        assertEquals("(\"price\" / 10)", s.expr());
    }

    public void testWhatIsNotReadIsRefusedWithTheScriptNamed() {
        for (String code : new String[] {
            "for (int i = 0; i < 3; i++) { x += i; }",
            "doc['name'].value.split(',')",
            "new HashMap()",
            "ctx._source.x",
            "doc['price'].values",
            "def x = 1; x = 2; return x;",
            "params._source.nope.deeper",
            "Math.random()" }) {
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> sql(code));
            assertTrue(code + " -> " + e.getMessage(), e.getMessage().startsWith("[infino] script ["));
        }
        IllegalArgumentException stored = expectThrows(
            IllegalArgumentException.class,
            () -> InfinoScriptTranslator.translate(new Script(ScriptType.STORED, null, "my-script", Map.of()), COLS, null, NOW)
        );
        assertTrue(stored.getMessage().contains("stored"));
    }
}
