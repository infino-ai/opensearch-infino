/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import org.opensearch.common.time.DateFormatter;
import org.opensearch.common.time.DateMathParser;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.ExistsQueryBuilder;
import org.opensearch.index.query.IdsQueryBuilder;
import org.opensearch.index.query.MatchAllQueryBuilder;
import org.opensearch.index.query.MatchNoneQueryBuilder;
import org.opensearch.index.query.MatchPhraseQueryBuilder;
import org.opensearch.index.query.MatchQueryBuilder;
import org.opensearch.index.query.MultiMatchQueryBuilder;
import org.opensearch.index.query.Operator;
import org.opensearch.index.query.PrefixQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryStringQueryBuilder;
import org.opensearch.index.query.RangeQueryBuilder;
import org.opensearch.index.query.RegexpQueryBuilder;
import org.opensearch.index.query.SimpleQueryStringBuilder;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.index.query.TermsQueryBuilder;
import org.opensearch.index.query.WildcardQueryBuilder;
import org.opensearch.infino.InfinoNative.Column;

import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Translates an OpenSearch query-DSL builder tree into the SQL the Infino
 * table answers it with: a {@code FROM} source and a {@code WHERE} predicate.
 *
 * <p>Filters (term / terms / range / exists / prefix / wildcard / regexp /
 * ids, combined by bool) become a predicate over the table's columns. A
 * mapped date is a timestamp column, so a date range (with date math such
 * as {@code now-15m}) is a timestamp comparison. A field named with a
 * {@code .keyword} suffix resolves to the string column itself.
 *
 * <p>Full-text clauses (match / match_phrase / multi_match / query_string
 * / simple_query_string on a text-indexed column) are answered by the
 * engine's own token index: the first such clause in a required position
 * becomes the {@code FROM} source (the {@code token_match} table function,
 * or {@code bm25_search} when hits are ranked by it), every other one is
 * an {@code osid IN (SELECT ...)} subquery over the same function. A phrase
 * goes to the engine quoted, which it matches as adjacent terms in order; a
 * query string's {@code +}, {@code -}, quotes and {@code AND} / {@code OR}
 * / {@code NOT} go through as the engine's own syntax, and its
 * {@code field:term} clauses become predicates on those columns. Nothing
 * materializes a match set in Java.
 *
 * <p>A query the table cannot answer translates to {@code null}; the
 * caller then falls back to the stock query phase.
 */
final class InfinoQueryTranslator {
    /** The OpenSearch {@code _id} column. */
    static final String OSID = "osid";
    /** The reserved column holding each document's JSON source. */
    static final String SOURCE = "source";
    /** Mapping suffix OpenSearch clients use for the exact form of a text field. */
    private static final String KEYWORD_SUFFIX = ".keyword";
    /** How a date range or histogram bound is read when the request names no format. */
    private static final String DEFAULT_DATE_FORMAT = "strict_date_optional_time||epoch_millis";
    /** Predicate that keeps every row. */
    static final String ALL = "1=1";
    /** Predicate that keeps no row. */
    static final String NONE = "1=0";

    /** One full-text clause on a text-indexed column. */
    record TextClause(String column, String query, String mode) {}

    /** What a query translates to. */
    record Translation(String table, String where, TextClause principal) {
        /** The relation the predicate applies to: the table, or its token-match set. */
        String from() {
            if (principal == null) {
                return quoteIdent(table);
            }
            return "token_match("
                + literal(table)
                + ", "
                + literal(principal.column())
                + ", "
                + literal(principal.query())
                + ", "
                + literal(principal.mode())
                + ")";
        }

        /** The ranked relation for hits: BM25 over the principal clause, {@code k} candidates. */
        String rankedFrom(int k) {
            return "bm25_search("
                + literal(table)
                + ", "
                + literal(principal.column())
                + ", "
                + literal(principal.query())
                + ", "
                + k
                + ", "
                + literal(principal.mode())
                + ")";
        }
    }

    private final Map<String, Column> cols;
    private final String table;
    private final long nowMillis;
    private TextClause principal;

    private InfinoQueryTranslator(Map<String, Column> cols, String table, long nowMillis) {
        this.cols = cols;
        this.table = table;
        this.nowMillis = nowMillis;
    }

    /** Translate, or {@code null} when the query is not answerable over the table's columns. */
    static Translation translate(QueryBuilder qb, Map<String, Column> cols, String table, long nowMillis) {
        InfinoQueryTranslator t = new InfinoQueryTranslator(cols, table, nowMillis);
        String where = t.predicate(qb, true);
        if (where == null) {
            return null;
        }
        return new Translation(table, where, t.principal);
    }

    /**
     * A query as a predicate alone, never as a FROM source: for a filter
     * inside an aggregation, where text clauses become subqueries over the
     * table's token index. Null when not answerable.
     */
    static String predicate(QueryBuilder qb, Map<String, Column> cols, String table, long nowMillis) {
        InfinoQueryTranslator t = new InfinoQueryTranslator(cols, table, nowMillis);
        return t.predicate(qb, false);
    }

    /** Epoch milliseconds of a date, epoch number or date-math expression, or null when it is not a date. */
    static Long dateMathMillis(String text, String format, boolean roundUp, ZoneId zone, long nowMillis) {
        DateMathParser parser = DateFormatter.forPattern(format == null ? DEFAULT_DATE_FORMAT : format).toDateMathParser();
        try {
            return parser.parse(text, () -> nowMillis, roundUp, zone).toEpochMilli();
        } catch (RuntimeException e) {
            return null;
        }
    }

    // ---- columns ----

    /** The column a query names, resolving {@code name.keyword} to the string column {@code name}. */
    Column column(String field) {
        if (field == null) {
            return null;
        }
        Column c = cols.get(field);
        if (c == null && field.endsWith(KEYWORD_SUFFIX)) {
            Column base = cols.get(field.substring(0, field.length() - KEYWORD_SUFFIX.length()));
            if (base != null && base.isString()) {
                return base;
            }
        }
        return c;
    }

    static String quoteIdent(String name) {
        return "\"" + name.replace("\"", "\"\"") + "\"";
    }

    static String literal(String s) {
        return "'" + s.replace("'", "''") + "'";
    }

    // ---- predicates ----

    /**
     * SQL boolean expression for a builder, or null if not translatable.
     * {@code positive} is true where a full-text clause may become the FROM
     * source: at the root and down must / filter chains, never under should
     * or must_not.
     */
    private String predicate(QueryBuilder qb, boolean positive) {
        if (qb == null || qb instanceof MatchAllQueryBuilder) {
            return ALL;
        }
        if (qb instanceof MatchNoneQueryBuilder) {
            return NONE;
        }
        if (qb instanceof TermQueryBuilder t) {
            return equality(t.fieldName(), t.value());
        }
        if (qb instanceof TermsQueryBuilder ts) {
            return inList(ts.fieldName(), ts.values());
        }
        if (qb instanceof RangeQueryBuilder r) {
            return range(r);
        }
        if (qb instanceof ExistsQueryBuilder e) {
            Column c = column(e.fieldName());
            return c == null ? NONE : quoteIdent(c.name()) + " IS NOT NULL";
        }
        if (qb instanceof PrefixQueryBuilder p) {
            return like(p.fieldName(), escapeLike(p.value()) + "%");
        }
        if (qb instanceof WildcardQueryBuilder w) {
            return like(w.fieldName(), wildcardToLike(w.value()));
        }
        if (qb instanceof RegexpQueryBuilder re) {
            Column c = column(re.fieldName());
            if (c == null || !c.isString()) {
                return c == null ? NONE : null;
            }
            // A Lucene regexp is anchored to the whole value.
            return "regexp_like(" + quoteIdent(c.name()) + ", " + literal("^(?:" + re.value() + ")$") + ")";
        }
        if (qb instanceof IdsQueryBuilder ids) {
            if (ids.ids().isEmpty()) {
                return NONE;
            }
            List<String> lits = new ArrayList<>();
            for (String id : ids.ids()) {
                lits.add(literal(id));
            }
            return OSID + " IN (" + String.join(", ", lits) + ")";
        }
        if (qb instanceof MatchQueryBuilder m) {
            return text(m.fieldName(), String.valueOf(m.value()), m.operator(), positive);
        }
        if (qb instanceof MatchPhraseQueryBuilder mp) {
            // A quoted query is a phrase to the engine: the terms adjacent, in order.
            return text(mp.fieldName(), phrase(String.valueOf(mp.value())), Operator.AND, positive);
        }
        if (qb instanceof MultiMatchQueryBuilder mm) {
            String value = String.valueOf(mm.value());
            if (mm.type() == MultiMatchQueryBuilder.Type.PHRASE) {
                value = phrase(value);
            }
            return textOnFields(new ArrayList<>(mm.fields().keySet()), value, mm.operator(), positive);
        }
        if (qb instanceof QueryStringQueryBuilder qs) {
            return queryString(qs.queryString(), qs.defaultField(), new ArrayList<>(qs.fields().keySet()), qs.defaultOperator(), positive);
        }
        if (qb instanceof SimpleQueryStringBuilder sqs) {
            return queryString(sqs.value(), null, new ArrayList<>(sqs.fields().keySet()), sqs.defaultOperator(), positive);
        }
        if (qb instanceof BoolQueryBuilder b) {
            return bool(b, positive);
        }
        return null;
    }

    private String bool(BoolQueryBuilder b, boolean positive) {
        List<String> and = new ArrayList<>();
        for (QueryBuilder c : b.must()) {
            and.add(predicate(c, positive));
        }
        for (QueryBuilder c : b.filter()) {
            and.add(predicate(c, positive));
        }
        for (QueryBuilder c : b.mustNot()) {
            String p = predicate(c, false);
            if (p == null) {
                return null;
            }
            and.add("NOT (" + p + ")");
        }
        if (and.contains(null)) {
            return null;
        }
        // `should` filters when nothing else does, or when minimum_should_match asks
        // for at least one; otherwise it only affects scoring and is not a filter.
        boolean shouldRequired = !b.should().isEmpty() && (and.isEmpty() || minimumShouldMatch(b.minimumShouldMatch()) > 0);
        if (shouldRequired) {
            List<String> or = new ArrayList<>();
            for (QueryBuilder c : b.should()) {
                String p = predicate(c, false);
                if (p == null) {
                    return null;
                }
                or.add(p);
            }
            and.add("(" + String.join(" OR ", or) + ")");
        }
        if (and.isEmpty()) {
            return ALL;
        }
        return "(" + String.join(" AND ", and) + ")";
    }

    /** A minimum_should_match of "N", "N%" or "-N" taken as "at least one"; absent is 0. */
    private static int minimumShouldMatch(String msm) {
        if (msm == null || msm.isEmpty()) {
            return 0;
        }
        try {
            return Integer.parseInt(msm.replace("%", "").replace("-", "")) > 0 ? 1 : 0;
        } catch (NumberFormatException e) {
            return 1;
        }
    }

    // ---- full text ----

    private String text(String field, String value, Operator op, boolean positive) {
        Column c = column(field);
        if (c == null) {
            return NONE;
        }
        if (!c.fts()) {
            // A match on an exact column is an equality.
            return c.isString() ? equality(field, value) : null;
        }
        return textClause(c.name(), value, op == Operator.AND ? "and" : "or", positive);
    }

    private String textClause(String column, String query, String mode, boolean positive) {
        if (positive && principal == null) {
            principal = new TextClause(column, query, mode);
            return ALL;
        }
        return OSID
            + " IN (SELECT "
            + OSID
            + " FROM token_match("
            + literal(table)
            + ", "
            + literal(column)
            + ", "
            + literal(query)
            + ", "
            + literal(mode)
            + "))";
    }

    private String textOnFields(List<String> fields, String value, Operator op, boolean positive) {
        List<String> indexed = new ArrayList<>();
        for (String f : fields) {
            Column c = column(f);
            if (c != null && c.fts()) {
                indexed.add(c.name());
            }
        }
        if (indexed.isEmpty()) {
            return NONE;
        }
        String mode = op == Operator.AND ? "and" : "or";
        if (indexed.size() == 1) {
            return textClause(indexed.get(0), value, mode, positive);
        }
        List<String> or = new ArrayList<>();
        for (String column : indexed) {
            or.add(textClause(column, value, mode, false));
        }
        return "(" + String.join(" OR ", or) + ")";
    }

    /** The query that matches {@code value} as a phrase: the engine reads a quoted run as adjacent terms in order. */
    static String phrase(String value) {
        return "\"" + value.replace("\"", " ") + "\"";
    }

    /** One atom of a query string: its polarity, the field it names (null for the default fields) and its text. */
    private record Atom(char sigil, String field, String text, boolean quoted) {}

    /**
     * A query string, read for the syntax the engine shares with Lucene and
     * the part the plugin resolves itself. {@code +term}, {@code -term} and
     * {@code "a phrase"} go to the engine as written; {@code AND}, {@code OR}
     * and {@code NOT} become that polarity and mode; {@code field:term} and
     * {@code field:"a phrase"} are clauses on that column, a text column's
     * through its token index, an exact column's as an equality (with
     * {@code *} and {@code ?} as a pattern); {@code _exists_:field} is a null
     * check. {@code *} alone matches everything. Grouping with parentheses,
     * ranges, fuzziness and boosts are not read; such a string is not served.
     */
    private String queryString(String query, String defaultField, List<String> fields, Operator op, boolean positive) {
        String q = query == null ? "" : query.trim();
        if (q.isEmpty() || "*".equals(q) || "*:*".equals(q)) {
            return ALL;
        }
        List<Atom> atoms = readAtoms(q, op);
        if (atoms == null) {
            return null;
        }
        List<String> targets = new ArrayList<>(fields);
        if (targets.isEmpty() && defaultField != null && !"*".equals(defaultField)) {
            targets.add(defaultField);
        }
        if (targets.isEmpty() || targets.contains("*")) {
            targets.clear();
            for (Column c : cols.values()) {
                if (c.fts()) {
                    targets.add(c.name());
                }
            }
        }
        boolean anyOr = atoms.stream().anyMatch(a -> a.sigil() == '|');
        Operator mode = anyOr ? Operator.OR : op;
        // The atoms on the default fields go to the engine as one query in its own syntax.
        StringBuilder text = new StringBuilder();
        List<String> musts = new ArrayList<>();
        List<String> shoulds = new ArrayList<>();
        List<String> nots = new ArrayList<>();
        for (Atom a : atoms) {
            if (a.field() == null) {
                if (text.length() > 0) {
                    text.append(' ');
                }
                if (a.sigil() == '+' || a.sigil() == '-') {
                    text.append(a.sigil());
                }
                text.append(a.quoted() ? phrase(a.text()) : a.text());
                continue;
            }
            String p = fieldAtom(a);
            if (p == null) {
                return null;
            }
            switch (a.sigil()) {
                case '-' -> nots.add(p);
                case '+' -> musts.add(p);
                default -> (mode == Operator.AND ? musts : shoulds).add(p);
            }
        }
        List<String> and = new ArrayList<>();
        if (text.length() > 0) {
            String t = textOnFields(targets, text.toString(), mode, positive && musts.isEmpty() && shoulds.isEmpty());
            if (t == null) {
                return null;
            }
            if (mode == Operator.AND || shoulds.isEmpty()) {
                and.add(t);
            } else {
                shoulds.add(t);
            }
        }
        and.addAll(musts);
        if (!shoulds.isEmpty()) {
            and.add("(" + String.join(" OR ", shoulds) + ")");
        }
        for (String n : nots) {
            and.add("NOT (" + n + ")");
        }
        if (and.isEmpty()) {
            return ALL;
        }
        return "(" + String.join(" AND ", and) + ")";
    }

    /**
     * The atoms of a query string in order, or null when it uses syntax that
     * is not read. {@code AND} marks its neighbours required, {@code OR} marks
     * them optional (sigil {@code |}), {@code NOT} negates the next atom.
     */
    private static List<Atom> readAtoms(String q, Operator op) {
        List<String> tokens = new ArrayList<>();
        StringBuilder cur = new StringBuilder();
        boolean inQuote = false;
        for (int i = 0; i < q.length(); i++) {
            char ch = q.charAt(i);
            if (ch == '"') {
                inQuote = !inQuote;
                cur.append(ch);
            } else if (Character.isWhitespace(ch) && !inQuote) {
                if (cur.length() > 0) {
                    tokens.add(cur.toString());
                    cur.setLength(0);
                }
            } else {
                cur.append(ch);
            }
        }
        if (inQuote) {
            return null;
        }
        if (cur.length() > 0) {
            tokens.add(cur.toString());
        }
        List<Atom> atoms = new ArrayList<>();
        char pending = 0;
        for (String token : tokens) {
            if (token.indexOf('(') >= 0 || token.indexOf(')') >= 0 || token.indexOf('[') >= 0 || token.indexOf('{') >= 0
                || token.indexOf('~') >= 0 || token.indexOf('^') >= 0) {
                return null;
            }
            switch (token) {
                case "AND", "&&" -> {
                    if (!atoms.isEmpty() && atoms.get(atoms.size() - 1).sigil() == 0) {
                        Atom last = atoms.remove(atoms.size() - 1);
                        atoms.add(new Atom('+', last.field(), last.text(), last.quoted()));
                    }
                    pending = '+';
                    continue;
                }
                case "OR", "||" -> {
                    if (!atoms.isEmpty() && atoms.get(atoms.size() - 1).sigil() == 0) {
                        Atom last = atoms.remove(atoms.size() - 1);
                        atoms.add(new Atom('|', last.field(), last.text(), last.quoted()));
                    }
                    pending = '|';
                    continue;
                }
                case "NOT", "!" -> {
                    pending = '-';
                    continue;
                }
                default -> {
                }
            }
            char sigil = pending;
            pending = 0;
            String rest = token;
            if (rest.startsWith("+") || rest.startsWith("-")) {
                sigil = rest.charAt(0);
                rest = rest.substring(1);
            }
            String field = null;
            int colon = rest.indexOf(':');
            if (colon > 0 && !rest.startsWith("\"")) {
                field = rest.substring(0, colon);
                rest = rest.substring(colon + 1);
            }
            boolean quoted = rest.length() >= 2 && rest.startsWith("\"") && rest.endsWith("\"");
            if (quoted) {
                rest = rest.substring(1, rest.length() - 1);
            }
            if (rest.isEmpty()) {
                return null;
            }
            atoms.add(new Atom(sigil, field, rest, quoted));
        }
        return atoms;
    }

    /** The predicate of a field-scoped atom, or null when the column cannot take it. */
    private String fieldAtom(Atom a) {
        if ("_exists_".equals(a.field())) {
            Column c = column(a.text());
            return c == null ? NONE : quoteIdent(c.name()) + " IS NOT NULL";
        }
        if ("_id".equals(a.field())) {
            return OSID + " = " + literal(a.text());
        }
        Column c = column(a.field());
        if (c == null) {
            return NONE;
        }
        if (c.fts()) {
            return textClause(c.name(), a.quoted() ? phrase(a.text()) : a.text(), "and", false);
        }
        if (!a.quoted() && (a.text().indexOf('*') >= 0 || a.text().indexOf('?') >= 0)) {
            return c.isString() ? quoteIdent(c.name()) + " LIKE " + literal(wildcardToLike(a.text())) : null;
        }
        return equality(a.field(), a.text());
    }

    // ---- scalars ----

    private String equality(String field, Object value) {
        Column c = column(field);
        if (c == null || value == null) {
            return c == null ? NONE : null;
        }
        String lit = scalar(c, value, false);
        return lit == null ? null : quoteIdent(c.name()) + " = " + lit;
    }

    private String inList(String field, List<Object> values) {
        Column c = column(field);
        if (c == null) {
            return NONE;
        }
        if (values == null || values.isEmpty()) {
            return NONE;
        }
        List<String> lits = new ArrayList<>();
        for (Object v : values) {
            String lit = scalar(c, v, false);
            if (lit == null) {
                return null;
            }
            lits.add(lit);
        }
        return quoteIdent(c.name()) + " IN (" + String.join(", ", lits) + ")";
    }

    private String range(RangeQueryBuilder r) {
        Column c = column(r.fieldName());
        if (c == null) {
            return NONE;
        }
        if (!(c.isNumeric() || c.isTimestamp() || c.isString())) {
            return null;
        }
        List<String> parts = new ArrayList<>();
        ZoneId zone = r.timeZone() == null ? null : ZoneId.of(r.timeZone());
        if (r.from() != null) {
            // An exclusive lower date bound rounds up (gt 2026-10-24 means after that day).
            String lit = c.isTimestamp() ? dateLiteral(r.from(), r.format(), !r.includeLower(), zone) : scalar(c, r.from(), false);
            if (lit == null) {
                return null;
            }
            parts.add(quoteIdent(c.name()) + (r.includeLower() ? " >= " : " > ") + lit);
        }
        if (r.to() != null) {
            String lit = c.isTimestamp() ? dateLiteral(r.to(), r.format(), r.includeUpper(), zone) : scalar(c, r.to(), false);
            if (lit == null) {
                return null;
            }
            parts.add(quoteIdent(c.name()) + (r.includeUpper() ? " <= " : " < ") + lit);
        }
        if (parts.isEmpty()) {
            return ALL;
        }
        return "(" + String.join(" AND ", parts) + ")";
    }

    private String like(String field, String pattern) {
        Column c = column(field);
        if (c == null) {
            return NONE;
        }
        if (!c.isString()) {
            return null;
        }
        return quoteIdent(c.name()) + " LIKE " + literal(pattern);
    }

    private static String escapeLike(String s) {
        return s.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_");
    }

    private static String wildcardToLike(String s) {
        StringBuilder out = new StringBuilder();
        for (int i = 0; i < s.length(); i++) {
            char ch = s.charAt(i);
            switch (ch) {
                case '*' -> out.append('%');
                case '?' -> out.append('_');
                case '%', '_', '\\' -> out.append('\\').append(ch);
                default -> out.append(ch);
            }
        }
        return out.toString();
    }

    /** A literal of {@code value} for the column's type, or null when it cannot be one: for a value a client hands back. */
    static String literalFor(Column c, Object value, long nowMillis) {
        return new InfinoQueryTranslator(Map.of(), "", nowMillis).scalar(c, value, false);
    }

    /** A literal for the column's type, or null when the value cannot be one. */
    private String scalar(Column c, Object value, boolean roundUp) {
        if (c.isString()) {
            return literal(value.toString());
        }
        if (c.isBoolean()) {
            return Boolean.parseBoolean(value.toString()) ? "true" : "false";
        }
        if (c.isNumeric()) {
            return number(value);
        }
        if (c.isTimestamp()) {
            return dateLiteral(value, null, roundUp, null);
        }
        return null;
    }

    private static String number(Object v) {
        if (v instanceof Number n) {
            double d = n.doubleValue();
            return d == Math.rint(d) && Math.abs(d) < 9.0e15 ? Long.toString((long) d) : Double.toString(d);
        }
        try {
            return number(Double.parseDouble(v.toString()));
        } catch (NumberFormatException e) {
            return null;
        }
    }

    /** A timestamp literal from a date, epoch milliseconds, or date math, in the engine's ISO form. */
    private String dateLiteral(Object value, String format, boolean roundUp, ZoneId zone) {
        Instant at;
        if (value instanceof Number n) {
            at = Instant.ofEpochMilli(n.longValue());
        } else {
            DateMathParser parser = DateFormatter.forPattern(format == null ? DEFAULT_DATE_FORMAT : format).toDateMathParser();
            try {
                at = parser.parse(value.toString(), () -> nowMillis, roundUp, zone);
            } catch (RuntimeException e) {
                return null;
            }
        }
        // The column holds milliseconds; a rounded-up bound ends in .999.
        return literal(Instant.ofEpochMilli(at.toEpochMilli()).toString());
    }
}
