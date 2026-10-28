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
import org.opensearch.index.query.ScriptQueryBuilder;
import org.opensearch.index.query.SimpleQueryStringBuilder;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.index.query.TermsQueryBuilder;
import org.opensearch.index.query.WildcardQueryBuilder;
import org.opensearch.infino.InfinoLuceneQuery.All;
import org.opensearch.infino.InfinoLuceneQuery.Bool;
import org.opensearch.infino.InfinoLuceneQuery.Node;
import org.opensearch.infino.InfinoLuceneQuery.Range;
import org.opensearch.infino.InfinoLuceneQuery.Regex;
import org.opensearch.infino.InfinoLuceneQuery.Term;
import org.opensearch.infino.InfinoLuceneQuery.Wildcard;
import org.opensearch.infino.InfinoNative.Column;
import org.opensearch.infino.InfinoScriptTranslator.Sql;
import org.opensearch.infino.InfinoScriptTranslator.SqlType;

import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;

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
 * query string is read as Lucene reads it ({@link InfinoLuceneQuery}): its
 * terms and phrases on the default fields go to the engine with the
 * clause occurrences as {@code +} / {@code -} sigils, and its field
 * clauses, ranges, wildcards and regular expressions become predicates on
 * those columns. Nothing materializes a match set in Java.
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
    /** How a text clause other than the ranked one joins the relation: by id, through a subquery over its token index. */
    private static final String ID_SUBQUERY = " IN (SELECT ";

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
    /**
     * Whether a terms lookup may be read from its index's table directly:
     * true for a security rule, which is the security plugin's own; false
     * for a user's query, whose lookup would pass no authorization here.
     */
    private final boolean trusted;
    private TextClause principal;

    private InfinoQueryTranslator(Map<String, Column> cols, String table, long nowMillis, boolean trusted) {
        this.cols = cols;
        this.table = table;
        this.nowMillis = nowMillis;
        this.trusted = trusted;
    }

    /** Translate, or {@code null} when the query is not answerable over the table's columns. */
    static Translation translate(QueryBuilder qb, Map<String, Column> cols, String table, long nowMillis) {
        InfinoQueryTranslator t = new InfinoQueryTranslator(cols, table, nowMillis, false);
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
        InfinoQueryTranslator t = new InfinoQueryTranslator(cols, table, nowMillis, false);
        return t.predicate(qb, false);
    }

    /** A security plugin rule as a predicate: as {@link #predicate}, with a terms lookup read from its table. */
    static String rulePredicate(QueryBuilder qb, Map<String, Column> cols, String table, long nowMillis) {
        InfinoQueryTranslator t = new InfinoQueryTranslator(cols, table, nowMillis, true);
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

    /**
     * Whether a predicate holds a text clause on a column other than the
     * ranked one: a subquery the engine joins above a ranked search, so the
     * rows it removes go after the ranking. A string literal that happens to
     * hold the same text only costs that search a wider candidate window.
     */
    static boolean joinsAfterRanking(String predicate) {
        return predicate.contains(ID_SUBQUERY);
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
            // A term on a text column is one indexed term, as in Lucene; on an exact column the value.
            Column c = column(t.fieldName());
            if (c != null && c.fts()) {
                return textClause(c.name(), String.valueOf(t.value()), "and", false);
            }
            return equality(t.fieldName(), t.value());
        }
        if (qb instanceof TermsQueryBuilder ts) {
            if (ts.termsLookup() != null) {
                if (!trusted) {
                    throw new IllegalArgumentException("[infino] a terms lookup into [" + ts.termsLookup().index() + "] is not served by the table");
                }
                return inList(ts.fieldName(), InfinoTermsLookup.values(ts.termsLookup()));
            }
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
            // On a text column a prefix is a term's prefix; on an exact column the value's.
            Column c = column(p.fieldName());
            if (c != null && c.fts()) {
                return termPattern(c, wildcardToRegex(p.value()), true);
            }
            return like(p.fieldName(), escapeLike(p.value()) + "%");
        }
        if (qb instanceof WildcardQueryBuilder w) {
            Column c = column(w.fieldName());
            if (c != null && c.fts()) {
                return termPattern(c, wildcardToRegex(w.value()), w.value().endsWith("*"));
            }
            return like(w.fieldName(), wildcardToLike(w.value()));
        }
        if (qb instanceof RegexpQueryBuilder re) {
            Column c = column(re.fieldName());
            if (c == null || !c.isString()) {
                return c == null ? NONE : null;
            }
            if (c.fts()) {
                return termPattern(c, "(?:" + re.value() + ")", false);
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
            return text(mp.fieldName(), String.valueOf(mp.value()), Operator.AND, positive, true);
        }
        if (qb instanceof MultiMatchQueryBuilder mm) {
            boolean asPhrase = mm.type() == MultiMatchQueryBuilder.Type.PHRASE;
            return textOnFields(new ArrayList<>(mm.fields().keySet()), String.valueOf(mm.value()), mm.operator(), positive, asPhrase, true);
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
        if (qb instanceof ScriptQueryBuilder sq) {
            // The script as a column expression; it must yield a boolean to filter.
            Sql s = InfinoScriptTranslator.translate(sq.script(), cols, null, nowMillis);
            if (s.type() != SqlType.BOOL && s.type() != SqlType.UNKNOWN) {
                throw new InfinoScriptTranslator.Unsupported("[" + sq.script().getIdOrCode() + "] yields a " + s.type().name().toLowerCase(Locale.ROOT) + ", not the boolean a script query needs");
            }
            return "(" + s.expr() + ")";
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
        if (and.contains(null)) {
            return null;
        }
        // `should` filters when no must or filter clause does, or when
        // minimum_should_match asks for some; otherwise it only affects scoring.
        boolean nothingRequired = b.must().isEmpty() && b.filter().isEmpty();
        int required = minimumShouldMatch(b.minimumShouldMatch(), b.should().size(), nothingRequired);
        if (!b.should().isEmpty() && required > 0) {
            List<String> or = new ArrayList<>();
            for (QueryBuilder c : b.should()) {
                String p = predicate(c, false);
                if (p == null) {
                    return null;
                }
                or.add(p);
            }
            and.add(atLeast(or, required));
        }
        for (QueryBuilder c : b.mustNot()) {
            String p = predicate(c, false);
            if (p == null) {
                return null;
            }
            and.add(not(p));
        }
        if (and.isEmpty()) {
            return ALL;
        }
        return "(" + String.join(" AND ", and) + ")";
    }

    /**
     * The negation of a predicate as a filter: a row the predicate does not
     * decide (a missing value, which SQL reads as NULL) is kept, as a
     * must_not keeps the documents without the field.
     */
    static String not(String predicate) {
        return "NOT COALESCE((" + predicate + "), FALSE)";
    }

    /** At least {@code required} of the predicates true: a plain OR for one, a count of the true ones past that. */
    private static String atLeast(List<String> predicates, int required) {
        if (required <= 1) {
            return "(" + String.join(" OR ", predicates) + ")";
        }
        List<String> counted = new ArrayList<>();
        for (String p : predicates) {
            counted.add("CASE WHEN " + p + " THEN 1 ELSE 0 END");
        }
        return "((" + String.join(" + ", counted) + ") >= " + Math.min(required, predicates.size()) + ")";
    }

    /**
     * How many of {@code n} should clauses must match: the request's
     * minimum_should_match read as OpenSearch reads it ({@code N}, {@code -N},
     * {@code P%}, {@code -P%}; a combination such as {@code 3<90%} is taken
     * as one), else one when nothing else filters, else none.
     */
    private static int minimumShouldMatch(String msm, int n, boolean nothingRequired) {
        if (msm == null || msm.isEmpty()) {
            return nothingRequired ? 1 : 0;
        }
        String s = msm.trim();
        try {
            if (s.endsWith("%")) {
                int percent = Integer.parseInt(s.substring(0, s.length() - 1));
                int magnitude = percent < 0 ? -percent : percent;
                int share = magnitude * n / 100;
                return Math.max(0, percent < 0 ? n - share : share);
            }
            int count = Integer.parseInt(s);
            return Math.max(0, count < 0 ? n + count : count);
        } catch (NumberFormatException e) {
            return 1;
        }
    }

    // ---- full text ----

    private String text(String field, String value, Operator op, boolean positive) {
        return text(field, value, op, positive, false);
    }

    /**
     * A match or, with {@code asPhrase}, a match_phrase on one field: on a
     * text column the engine's token query (the phrase quoted, so its terms
     * match adjacent and in order), on an exact column an equality of the
     * value as given, which is what a phrase over a keyword means.
     */
    private String text(String field, String value, Operator op, boolean positive, boolean asPhrase) {
        Column c = column(field);
        if (c == null) {
            return NONE;
        }
        if (!c.fts()) {
            // A match on an exact column is an equality, whatever the column's type.
            return equality(field, value);
        }
        return textClause(c.name(), asPhrase ? phrase(value) : value, op == Operator.AND ? "and" : "or", positive);
    }

    private String textClause(String column, String query, String mode, boolean positive) {
        if (positive && principal == null) {
            principal = new TextClause(column, query, mode);
            return ALL;
        }
        if (positive && principal != null && principal.column().equals(column) && "and".equals(mode) && "and".equals(principal.mode())) {
            // A second required clause on the same column joins the ranked query, so
            // the score reflects both; under all-terms mode the join reads the same.
            principal = new TextClause(column, principal.query() + " " + query, mode);
            return ALL;
        }
        return OSID
            + ID_SUBQUERY
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

    /**
     * A clause over several {@code fields}: any of the text columns matching
     * the query (as a phrase when {@code asPhrase}), or, with
     * {@code exactToo}, any of the exact columns equal to the value as
     * given (a multi_match or a bare query-string term reaches keyword
     * fields that way; a composed engine query does not). A field the table
     * has no column for matches nothing.
     */
    private String textOnFields(List<String> fields, String value, Operator op, boolean positive, boolean asPhrase, boolean exactToo) {
        List<String> indexed = new ArrayList<>();
        List<String> exact = new ArrayList<>();
        for (String f : fields) {
            Column c = column(f);
            if (c == null) {
                continue;
            }
            if (c.fts()) {
                indexed.add(c.name());
            } else if (exactToo) {
                exact.add(equality(f, value));
            }
        }
        if (indexed.isEmpty() && exact.isEmpty()) {
            return NONE;
        }
        String mode = op == Operator.AND ? "and" : "or";
        String query = asPhrase ? phrase(value) : value;
        if (indexed.size() == 1 && exact.isEmpty()) {
            return textClause(indexed.get(0), query, mode, positive);
        }
        List<String> or = new ArrayList<>(exact);
        for (String column : indexed) {
            or.add(textClause(column, query, mode, false));
        }
        return or.size() == 1 ? or.get(0) : "(" + String.join(" OR ", or) + ")";
    }

    /** The query that matches {@code value} as a phrase: the engine reads a quoted run as adjacent terms in order. */
    static String phrase(String value) {
        return "\"" + value.replace("\"", " ") + "\"";
    }

    /** One atom of a query string: its polarity, the field it names (null for the default fields) and its text. */
    private record Atom(char sigil, String field, String text, boolean quoted) {}

    /**
     * A query string, read as Lucene reads it ({@link InfinoLuceneQuery}) and
     * served clause by clause: a clause on the default fields goes to the
     * engine's token index, and a group whose clauses are all plain terms or
     * phrases on the default fields goes as one engine query with the
     * {@code +} / {@code -} sigils Lucene's clause list maps to; a
     * {@code field:} clause is a predicate on that column (text columns
     * through the token index, exact columns as an equality, a pattern or a
     * range); a wildcard or regular expression on a text column matches a
     * term of its text; {@code _exists_:field} is a null check; a range or
     * comparison on a date column takes date math. Boosts change nothing
     * here and are dropped. Fuzziness and proximity have no counterpart in
     * the token index and are refused with the clause named, as is a range
     * without a field.
     */
    private String queryString(String query, String defaultField, List<String> fields, Operator op, boolean positive) {
        String q = query == null ? "" : query.trim();
        if (q.isEmpty() || "*".equals(q) || "*:*".equals(q)) {
            return ALL;
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
        return node(InfinoLuceneQuery.parse(q, op), targets, positive, q, op);
    }

    /** The predicate of one node of a query string's tree, or null when the table cannot take it; {@code op} reads a bare term's words. */
    private String node(Node n, List<String> targets, boolean positive, String query, Operator op) {
        if (n instanceof All) {
            return ALL;
        }
        if (n instanceof Bool b) {
            String engine = engineQuery(b);
            if (engine != null) {
                return textOnFields(targets, engine, Operator.OR, positive, false, false);
            }
            // Lucene's clause list: the required clauses match on their own, the
            // optional ones only when nothing is required, the prohibited never.
            List<String> and = new ArrayList<>();
            for (Node m : b.must()) {
                String p = node(m, targets, positive, query, op);
                if (p == null) {
                    return null;
                }
                and.add(p);
            }
            if (b.must().isEmpty() && !b.should().isEmpty()) {
                List<String> or = new ArrayList<>();
                for (Node s : b.should()) {
                    String p = node(s, targets, false, query, op);
                    if (p == null) {
                        return null;
                    }
                    or.add(p);
                }
                and.add("(" + String.join(" OR ", or) + ")");
            }
            for (Node x : b.mustNot()) {
                String p = node(x, targets, false, query, op);
                if (p == null) {
                    return null;
                }
                and.add(not(p));
            }
            return and.isEmpty() ? ALL : "(" + String.join(" AND ", and) + ")";
        }
        if (n instanceof Term t) {
            if (t.fuzzy() > 0) {
                throw new InfinoLuceneQuery.Malformed("[" + query + "] asks for fuzziness on [" + t.text() + "], which the token index does not serve");
            }
            if (t.quoted() && t.slop() > 0) {
                throw new InfinoLuceneQuery.Malformed("[" + query + "] asks for proximity on [\"" + t.text() + "\"], which the token index does not serve; a phrase matches adjacent terms");
            }
            if (t.field() == null) {
                return textOnFields(targets, t.text(), op, positive, t.quoted(), true);
            }
            return fieldAtom(new Atom((char) 0, t.field(), t.text(), t.quoted()));
        }
        if (n instanceof Wildcard w) {
            if (w.field() == null) {
                return anyOf(targets, c -> termPattern(c, wildcardToRegex(w.pattern()), w.pattern().endsWith("*")));
            }
            Column c = column(w.field());
            if (c == null) {
                return NONE;
            }
            if (c.fts()) {
                return termPattern(c, wildcardToRegex(w.pattern()), w.pattern().endsWith("*"));
            }
            return c.isString() ? quoteIdent(c.name()) + " LIKE " + literal(wildcardToLike(w.pattern())) : null;
        }
        if (n instanceof Regex r) {
            if (r.field() == null) {
                return anyOf(targets, c -> termPattern(c, "(?:" + r.pattern() + ")", false));
            }
            Column c = column(r.field());
            if (c == null) {
                return NONE;
            }
            if (c.fts()) {
                return termPattern(c, "(?:" + r.pattern() + ")", false);
            }
            // A Lucene regexp is anchored to the whole value.
            return c.isString() ? "regexp_like(" + quoteIdent(c.name()) + ", " + literal("^(?:" + r.pattern() + ")$") + ")" : null;
        }
        if (n instanceof Range r) {
            if (r.field() == null) {
                throw new InfinoLuceneQuery.Malformed("[" + query + "] has a range with no field; a range needs field:[a TO b]");
            }
            Column c = column(r.field());
            if (c == null) {
                return NONE;
            }
            if (r.from() == null && r.to() == null) {
                return quoteIdent(c.name()) + " IS NOT NULL";
            }
            RangeQueryBuilder rb = new RangeQueryBuilder(r.field());
            if (r.from() != null) {
                rb.from(r.from(), r.includeFrom());
            }
            if (r.to() != null) {
                rb.to(r.to(), r.includeTo());
            }
            return range(rb);
        }
        return null;
    }

    /**
     * A clause list whose clauses are all plain terms or phrases on the
     * default fields, as one engine query: required clauses with {@code +},
     * prohibited with {@code -}, optional bare, read in the engine's
     * any-of mode so the sigils alone decide. Null when a clause needs the
     * plugin's own translation, or when nothing is required or optional (a
     * purely negative query excludes rows from everything, which is a
     * predicate, not a search).
     */
    private static String engineQuery(Bool b) {
        if (b.must().isEmpty() && b.should().isEmpty()) {
            return null;
        }
        StringBuilder out = new StringBuilder();
        for (List<Node> group : List.of(b.must(), b.should(), b.mustNot())) {
            String sigil = group == b.must() ? "+" : group == b.mustNot() ? "-" : "";
            for (Node n : group) {
                if (!(n instanceof Term t) || t.field() != null || t.fuzzy() > 0 || t.slop() > 0 || t.text().isBlank()) {
                    return null;
                }
                if (out.length() > 0) {
                    out.append(' ');
                }
                out.append(sigil).append(t.quoted() ? phrase(t.text()) : t.text());
            }
        }
        return out.toString();
    }

    /** The predicate over the text columns named, any of them matching; none matches nothing. */
    private String anyOf(List<String> targets, Function<Column, String> perColumn) {
        List<String> or = new ArrayList<>();
        for (String f : targets) {
            Column c = column(f);
            if (c != null && c.fts()) {
                String p = perColumn.apply(c);
                if (p == null) {
                    return null;
                }
                or.add(p);
            }
        }
        if (or.isEmpty()) {
            return NONE;
        }
        return or.size() == 1 ? or.get(0) : "(" + String.join(" OR ", or) + ")";
    }

    /**
     * A term of a text column's text matching {@code regex}: the pattern is
     * bounded at a word boundary on the left and, unless it ends open, on the
     * right, and read without regard to case, since the index lowercases
     * its terms.
     */
    private static String termPattern(Column c, String regex, boolean openEnd) {
        String bounded = "\\b" + regex + (openEnd ? "" : "\\b");
        return "regexp_like(" + quoteIdent(c.name()) + ", " + literal(bounded) + ", 'i')";
    }

    /** A Lucene wildcard as a regular expression: {@code *} any run within a term, {@code ?} one character, the rest literal. */
    private static String wildcardToRegex(String s) {
        StringBuilder out = new StringBuilder();
        for (int i = 0; i < s.length(); i++) {
            char ch = s.charAt(i);
            switch (ch) {
                case '*' -> out.append("[^\\s]*");
                case '?' -> out.append("[^\\s]");
                default -> {
                    if ("\\.[]{}()<>+-=!?^$|".indexOf(ch) >= 0) {
                        out.append('\\');
                    }
                    out.append(ch);
                }
            }
        }
        return out.toString();
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
        return new InfinoQueryTranslator(Map.of(), "", nowMillis, false).scalar(c, value, false);
    }

    /** A literal of {@code value} for an expression's type, or null when it cannot be one. */
    static String literalForType(SqlType type, Object value, long nowMillis) {
        return switch (type) {
            case STRING -> literal(value.toString());
            case BOOL -> Boolean.parseBoolean(value.toString()) ? "true" : "false";
            case INT, FLOAT -> number(value);
            case DATE -> new InfinoQueryTranslator(Map.of(), "", nowMillis, false).dateLiteral(value, null, false, null);
            default -> null;
        };
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
