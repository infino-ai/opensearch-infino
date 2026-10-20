package org.opensearch.infino;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.MatchAllQueryBuilder;
import org.opensearch.index.query.MatchQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.RangeQueryBuilder;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.index.query.TermsQueryBuilder;

/**
 * Translates an OpenSearch query-DSL builder tree into an Infino SQL {@code WHERE}
 * predicate, reading the parsed builders (introspectable, unlike the byte-encoded
 * Lucene {@code PointRangeQuery}). Supports {@code match_all}, {@code term} /
 * {@code terms} / {@code range} over dynamic columns, and {@code bool} combining
 * them, and {@code match} (full-text) — resolved to the matching {@code osid}s
 * via Infino {@code token_match} and spliced in as {@code osid IN (...)}, so a
 * text query can scope aggregations. Returns {@code null} when the query is not
 * translatable over known columns — the caller then uses a ranking path or
 * delegates to Lucene. (The {@code IN}-list scales with the match set; a
 * production path would push the text predicate into the engine instead.)
 *
 * <p>The same predicate is used for hits ({@code SELECT osid ... WHERE p}) and for
 * query-scoped aggregations ({@code SELECT agg ... WHERE p}), so both honor the
 * filter identically.
 */
final class InfinoSqlWhere {
    private InfinoSqlWhere() {}

    /** SQL boolean expression, or null if not translatable over known columns. */
    static String of(QueryBuilder qb, Map<String, String> cols, String table) {
        if (qb == null || qb instanceof MatchAllQueryBuilder) return "1=1";
        if (qb instanceof TermQueryBuilder t) return term(t.fieldName(), t.value(), cols);
        if (qb instanceof TermsQueryBuilder ts) return terms(ts, cols);
        if (qb instanceof RangeQueryBuilder r) return range(r, cols);
        if (qb instanceof MatchQueryBuilder m) return match(m, table);
        if (qb instanceof BoolQueryBuilder b) return bool(b, cols, table);
        return null;
    }

    /** Full-text match → osid IN (...) via token_match (over the FTS `text` column). */
    private static String match(MatchQueryBuilder m, String table) {
        if (m.value() == null) return null;
        List<String> osids = InfinoNative.tokenMatchOsids(table, "text", m.value().toString());
        if (osids.isEmpty()) return "1=0"; // matches nothing
        List<String> lits = new ArrayList<>(osids.size());
        for (String id : osids) lits.add("'" + id.replace("'", "''") + "'");
        return "osid IN (" + String.join(", ", lits) + ")";
    }

    private static String term(String field, Object value, Map<String, String> cols) {
        String ty = cols.get(field);
        if (ty == null || value == null) return null;
        return field + " = " + literal(value, ty);
    }

    private static String terms(TermsQueryBuilder ts, Map<String, String> cols) {
        String field = ts.fieldName();
        String ty = cols.get(field);
        List<Object> values = ts.values();
        if (ty == null || values == null || values.isEmpty()) return null;
        List<String> lits = new ArrayList<>();
        for (Object v : values) {
            String lit = literal(v, ty);
            if (lit == null) return null;
            lits.add(lit);
        }
        return field + " IN (" + String.join(", ", lits) + ")";
    }

    private static String range(RangeQueryBuilder r, Map<String, String> cols) {
        String field = r.fieldName();
        String ty = cols.get(field);
        if (!"i64".equals(ty) && !"f64".equals(ty)) return null; // numeric ranges only
        List<String> parts = new ArrayList<>();
        if (r.from() != null) {
            String n = number(r.from());
            if (n == null) return null;
            parts.add(field + (r.includeLower() ? " >= " : " > ") + n);
        }
        if (r.to() != null) {
            String n = number(r.to());
            if (n == null) return null;
            parts.add(field + (r.includeUpper() ? " <= " : " < ") + n);
        }
        if (parts.isEmpty()) return "1=1";
        return "(" + String.join(" AND ", parts) + ")";
    }

    private static String bool(BoolQueryBuilder b, Map<String, String> cols, String table) {
        List<String> and = new ArrayList<>();
        for (QueryBuilder c : b.must()) and.add(of(c, cols, table));
        for (QueryBuilder c : b.filter()) and.add(of(c, cols, table));
        for (QueryBuilder c : b.mustNot()) {
            String p = of(c, cols, table);
            if (p == null) return null;
            and.add("NOT (" + p + ")");
        }
        if (and.contains(null)) return null;
        // `should` filters only when there is no must/filter/mustNot (then min_should_match=1 ⇒ OR).
        if (and.isEmpty() && !b.should().isEmpty()) {
            List<String> or = new ArrayList<>();
            for (QueryBuilder c : b.should()) {
                String p = of(c, cols, table);
                if (p == null) return null;
                or.add(p);
            }
            return "(" + String.join(" OR ", or) + ")";
        }
        if (and.isEmpty()) return "1=1";
        return "(" + String.join(" AND ", and) + ")";
    }

    private static String literal(Object value, String ty) {
        if ("str".equals(ty)) return "'" + value.toString().replace("'", "''") + "'";
        if ("bool".equals(ty)) return Boolean.parseBoolean(value.toString()) ? "true" : "false";
        if ("i64".equals(ty) || "f64".equals(ty)) return number(value);
        return null;
    }

    private static String number(Object v) {
        try {
            return Double.toString(Double.parseDouble(v.toString()));
        } catch (NumberFormatException e) {
            return null;
        }
    }
}
