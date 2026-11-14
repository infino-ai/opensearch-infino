/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import org.opensearch.OpenSearchParseException;
import org.opensearch.common.time.DateFormatter;
import org.opensearch.common.time.DateMathParser;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.ExistsQueryBuilder;
import org.opensearch.index.query.IdsQueryBuilder;
import org.opensearch.index.query.MatchAllQueryBuilder;
import org.opensearch.index.query.MatchNoneQueryBuilder;
import org.opensearch.index.query.MatchPhraseQueryBuilder;
import org.opensearch.index.query.MatchQueryBuilder;
import org.opensearch.index.query.MultiMatchQueryBuilder;
import org.opensearch.index.query.NestedQueryBuilder;
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

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.ToIntFunction;
import java.util.regex.Pattern;

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
 * <p>A nested query (an entry of an array of objects matching, every
 * clause on that one entry) is checked entry by entry: the table's columns
 * hold each member field's values flattened across the entries, so they
 * narrow the documents to those that may match, and each of those
 * document's source is read to find an entry that does. See
 * {@link #nested}.
 *
 * <p>A query the table cannot answer translates to {@code null}; the
 * caller then falls back to the stock query phase.
 */
final class InfinoQueryTranslator {
    /** The OpenSearch {@code _id} column. */
    static final String OSID = "osid";
    /** The reserved column holding each document's JSON source. */
    static final String SOURCE = "source";
    /** The reserved column holding the routing value a default-mode document was written with, when it was. */
    static final String ROUTING = "osrouting";
    /**
     * The reserved column holding the sequence number each document was written under: the plugin's in proxy mode
     * (see {@link InfinoDocumentVersion}), the shard's own in default mode, stamped by its writer.
     */
    static final String SEQ = "osseq";
    /** The reserved column holding the sequence number of the operation that wrote each default-mode commit's newest row. */
    static final String OP = "osop";
    /**
     * The reserved column naming the index whose write stamped a row's {@link #SEQ} and {@link #OP}: sequence
     * numbers compare within one index's history only.
     */
    static final String GEN = "osgen";
    /**
     * What the bridge adds to a field's column name for the list column that
     * holds the field's values for a document that gives it several; the
     * field's own column is then null for that document (a string field's
     * keeps the values joined, for its text index).
     */
    static final String LIST_SUFFIX = "[]";
    /**
     * The names a list's subquery gives a document's id and each of its
     * values, and the aliases of the table it reads and of its rows. The
     * engine plans the subquery only with both relations aliased.
     */
    private static final String LIST_ID = "list_id";
    private static final String LIST_VALUE = "list_value";
    private static final String LIST_SOURCE = "list_source";
    private static final String LIST_ROWS = "list_rows";
    /** Mapping suffix OpenSearch clients use for the exact form of a text field. */
    private static final String KEYWORD_SUFFIX = ".keyword";
    /** How a date range or histogram bound is read when the request names no format. */
    private static final String DEFAULT_DATE_FORMAT = "strict_date_optional_time||epoch_millis";
    /**
     * How a clause joins the relation by id, through a subquery: a text
     * clause other than the ranked one over its token index, or a field's
     * values over its list column.
     */
    private static final String ID_SUBQUERY = " IN (SELECT ";
    /** How a text clause that is not the FROM source reads the token index: a subquery over its ids. */
    private static final String TEXT_SUBQUERY = OSID + ID_SUBQUERY + OSID + " FROM token_match(";
    /** Predicate that keeps every row. */
    static final String ALL = "1=1";
    /** Predicate that keeps no row. */
    static final String NONE = "1=0";
    /**
     * How many documents a nested query's columns may leave to be checked
     * entry by entry, each by its source; past this the query is refused
     * rather than read whole. {@code -Dinfino.nested.candidates.max}.
     */
    static final int NESTED_CANDIDATES_MAX = Integer.getInteger("infino.nested.candidates.max", 10_000);

    /** One full-text clause on a text-indexed column. */
    record TextClause(String column, String query, String mode) {}

    /**
     * The rows of a date column from {@code fromMillis}, inclusive, to
     * {@code untilMillis}, exclusive: what a query that is one range on that
     * column keeps, read from the range's own bounds.
     */
    record Span(String column, long fromMillis, long untilMillis) {}

    /**
     * What a query translates to. {@code span} is set when the query is one
     * range on a date column with both bounds, and is then exactly the rows
     * {@code where} keeps; null otherwise.
     */
    record Translation(String table, String where, TextClause principal, Span span) {
        Translation(String table, String where, TextClause principal) {
            this(table, where, principal, null);
        }

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
    /** The span of the last range translated over a date column with both bounds ({@link Span}). */
    private Span span;
    /**
     * Whether a terms lookup may be read from its index's table directly:
     * true for a security rule, which is the security plugin's own; false
     * for a user's query, whose lookup would pass no authorization here.
     */
    private final boolean trusted;
    /**
     * Whether the predicate may compare column values only: true for a knn
     * query's filter, which the engine pushes into its vector search, where
     * a text clause's subquery over the token index cannot go.
     */
    private final boolean columnValuesOnly;
    /** How the translator reads rows of the table: a nested query's candidates, with their sources. */
    private final Function<String, List<Map<String, Object>>> rows;
    private TextClause principal;

    private InfinoQueryTranslator(Map<String, Column> cols, String table, long nowMillis, boolean trusted) {
        this(cols, table, nowMillis, trusted, false);
    }

    private InfinoQueryTranslator(Map<String, Column> cols, String table, long nowMillis, boolean trusted, boolean columnValuesOnly) {
        this(cols, table, nowMillis, trusted, columnValuesOnly, InfinoNative::sqlRows);
    }

    private InfinoQueryTranslator(
        Map<String, Column> cols,
        String table,
        long nowMillis,
        boolean trusted,
        boolean columnValuesOnly,
        Function<String, List<Map<String, Object>>> rows
    ) {
        this.cols = cols;
        this.table = table;
        this.nowMillis = nowMillis;
        this.trusted = trusted;
        this.columnValuesOnly = columnValuesOnly;
        this.rows = rows;
    }

    /** Translate, or {@code null} when the query is not answerable over the table's columns. */
    static Translation translate(QueryBuilder qb, Map<String, Column> cols, String table, long nowMillis) {
        return translate(qb, cols, table, nowMillis, InfinoNative::sqlRows);
    }

    /** {@link #translate(QueryBuilder, Map, String, long)}, reading the table's rows through {@code rows}. */
    static Translation translate(QueryBuilder qb, Map<String, Column> cols, String table, long nowMillis, Function<String, List<Map<String, Object>>> rows) {
        InfinoQueryTranslator t = new InfinoQueryTranslator(cols, table, nowMillis, false, false, rows);
        String where = t.predicate(qb, true);
        if (where == null) {
            return null;
        }
        return new Translation(table, where, t.principal, qb instanceof RangeQueryBuilder ? t.span : null);
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

    /**
     * A knn query's filter as the predicate the engine narrows its vector
     * search with (the k nearest among the rows it keeps): as
     * {@link #predicate}, over column values only. A full-text clause would
     * be a subquery over the token index, which the engine does not push
     * into the vector search, so one is refused with the clause named.
     * Null when not answerable.
     */
    static String vectorFilter(QueryBuilder qb, Map<String, Column> cols, String table, long nowMillis) {
        InfinoQueryTranslator t = new InfinoQueryTranslator(cols, table, nowMillis, false, true);
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

    /**
     * The column a query compares the values of, as {@link #resolve} finds
     * it. A masked field's column holds its clear values, so a user's query
     * comparing them (a term, a range, a pattern, a text match) would tell
     * the user what the mask hides: refused, as a sort on it is. A security
     * rule or an alias filter, which the user did not write, compares them.
     */
    Column column(String field) {
        Column c = resolve(field);
        if (c != null && c.masked() && !trusted) {
            throw InfinoAccess.refused("a query on [" + field + "], a field masked for this user, is not served");
        }
        return c;
    }

    /**
     * The column a query names, resolving {@code name.keyword} to the string
     * column {@code name}. A field's list column ({@link #LIST_SUFFIX}) is no
     * field of its own: a query naming one names an unmapped field. A
     * {@code name.keyword} over a text field is that field's whole value, as
     * a keyword subfield holds it: compared exactly, never through the text
     * field's terms ({@code term dept.keyword: Sales} matches "Sales", not
     * "Pre-Sales Ops").
     */
    Column resolve(String field) {
        if (field == null || field.endsWith(LIST_SUFFIX)) {
            return null;
        }
        Column c = cols.get(field);
        if (c == null && field.endsWith(KEYWORD_SUFFIX)) {
            Column base = cols.get(field.substring(0, field.length() - KEYWORD_SUFFIX.length()));
            if (base != null && base.isString()) {
                return base.exact();
            }
        }
        return c;
    }

    /** The field a column holds: its own name, or a list column's field ({@link #LIST_SUFFIX}). */
    static String fieldOf(String column) {
        return column.endsWith(LIST_SUFFIX) ? column.substring(0, column.length() - LIST_SUFFIX.length()) : column;
    }

    /** The list column holding a field's values for the documents that give it several; null when no document has. */
    static Column listOf(Map<String, Column> cols, Column c) {
        return c.isList() ? null : cols.get(c.name() + LIST_SUFFIX);
    }

    /**
     * The rows where any value of a field passes {@code test}, a predicate
     * over a column expression of the field's own type: a document's own
     * value, or, where it gave the field several, any of the values its list
     * column holds, through a subquery over the list's values by id. Null
     * when {@code test} is (a value with no literal), and for a list in a
     * vector search's filter, which compares column values only.
     */
    private String anyValue(Column c, Function<String, String> test) {
        Column values = c.isList() ? c : listOf(cols, c);
        if (values == null) {
            return test.apply(quoteIdent(c.name()));
        }
        if (columnValuesOnly) {
            return null;
        }
        return anyValue(table, c.isList() ? null : quoteIdent(c.name()), quoteIdent(values.name()), test);
    }

    /**
     * {@link #anyValue(Column, Function)} over named columns: {@code own} the
     * field's own column (null when the field is its list), {@code list} its
     * list column, both quoted, in {@code table}. Null when {@code test} is.
     */
    static String anyValue(String table, String own, String list, Function<String, String> test) {
        String mine = own == null ? null : test.apply(own);
        String each = test.apply(LIST_ROWS + "." + LIST_VALUE);
        if (each == null || (own != null && mine == null)) {
            return null;
        }
        String read = LIST_SOURCE + "." + list;
        String any = OSID + ID_SUBQUERY + LIST_ROWS + "." + LIST_ID + " FROM (SELECT " + LIST_SOURCE + "." + OSID + " AS " + LIST_ID + ", unnest(" + read
            + ") AS " + LIST_VALUE + " FROM " + quoteIdent(table) + " " + LIST_SOURCE + " WHERE " + read + " IS NOT NULL) " + LIST_ROWS + " WHERE " + each
            + ")";
        return own == null ? any : "((" + list + " IS NULL AND " + mine + ") OR " + any + ")";
    }

    /**
     * The rows where a field has a value. A field that is an object holds
     * its values in the columns under it, so it exists where any of them
     * holds one, as Lucene expands {@code exists} on an object over its
     * sub-fields; a pattern ({@code *}) names every column it matches. The
     * reserved columns are never a document's field.
     */
    private String exists(String field) {
        // Whether a field has a value tells nothing of a masked one's: its hash is shown.
        Column c = resolve(field);
        if (c != null) {
            Column values = listOf(cols, c);
            String own = quoteIdent(c.name()) + " IS NOT NULL";
            return values == null ? own : "(" + own + " OR " + quoteIdent(values.name()) + " IS NOT NULL)";
        }
        String under = field + ".";
        // OpenSearch's own field-pattern matcher, named in full: this class's Regex is a query node.
        boolean pattern = org.opensearch.common.regex.Regex.isSimpleMatchPattern(field);
        List<String> present = new ArrayList<>();
        for (Column sub : cols.values()) {
            String name = sub.name();
            if (name.equals(OSID) || name.equals(SOURCE) || name.equals(SEQ)) {
                continue;
            }
            if (name.startsWith(under) || (pattern && org.opensearch.common.regex.Regex.simpleMatch(field, name))) {
                present.add(quoteIdent(name) + " IS NOT NULL");
            }
        }
        return present.isEmpty() ? NONE : "(" + String.join(" OR ", present) + ")";
    }

    /**
     * Whether a predicate holds a subquery by id (a text clause on a column
     * other than the ranked one, or a field's list values): one the engine
     * joins above a ranked search, so the rows it removes go after the
     * ranking. A string literal that happens to hold the same text only
     * costs that search a wider candidate window.
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
                String terms = plainTerms(String.valueOf(t.value()));
                return terms.isEmpty() ? NONE : textClause(c.name(), terms, "and", false);
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
            return exists(e.fieldName());
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
            if (c == null || !c.element().isString()) {
                return c == null ? NONE : null;
            }
            if (c.fts()) {
                return termPattern(c, "(?:" + re.value() + ")", false);
            }
            // A Lucene regexp is anchored to the whole value.
            String anchored = literal("^(?:" + re.value() + ")$");
            return anyValue(c, v -> "regexp_like(" + v + ", " + anchored + ")");
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
        if (qb instanceof NestedQueryBuilder n) {
            return nested(n);
        }
        if (qb instanceof ScriptQueryBuilder sq) {
            // The script as a column expression; it must yield a boolean to filter.
            // A script reads values themselves, so a user's sees no masked column.
            Sql s = InfinoScriptTranslator.translate(sq.script(), trusted ? cols : InfinoAccess.scriptColumns(cols), null, nowMillis);
            if (s.type() != SqlType.BOOL && s.type() != SqlType.UNKNOWN) {
                throw new InfinoScriptTranslator.Unsupported("[" + sq.script().getIdOrCode() + "] yields a " + s.type().name().toLowerCase(Locale.ROOT) + ", not the boolean a script query needs");
            }
            return "(" + s.expr() + ")";
        }
        if (qb instanceof InfinoKnnQueryBuilder || qb instanceof InfinoHybridQueryBuilder || InfinoStockKnn.is(qb)) {
            // The engine's vector search answers a whole request; as one clause
            // among others it would have to be a predicate, which it is not.
            throw new IllegalArgumentException("[infino] [" + qb.getWriteableName() + "] is served as the whole query only, not as a clause inside another");
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

    // ---- nested ----

    /**
     * A nested query: the documents with an entry of the array of objects at
     * its path on which the inner query holds, every clause of it on that
     * one entry. Dashboards' {@code has_reference} is one: a saved object
     * whose references hold an entry with both the id and the type asked
     * for, not the id in one entry and the type in another.
     *
     * <p>The table flattens an array of objects into one column per member
     * field, holding the values of every entry ({@code references.id},
     * {@code references.type}), which loses which values share an entry. So
     * the columns only narrow the documents to those that may match: the
     * inner query read over the flattened values, its negations left out
     * ({@link #mayMatch}). Each of those is then checked by its source, entry
     * by entry ({@link #entryTest}), and the clause becomes the ids of the
     * documents that hold a matching entry. A clause the check cannot read
     * refuses the query with the clause named, as does a narrowing that
     * leaves more than {@link #NESTED_CANDIDATES_MAX} documents: never an
     * answer read over the flattened values.
     */
    private String nested(NestedQueryBuilder n) {
        String path = n.path();
        if (n.innerHit() != null) {
            throw new IllegalArgumentException("[infino] inner_hits on the nested query on [" + path + "] is not served by the table");
        }
        Predicate<Map<String, Object>> test = entryTest(n.query(), path, path);
        // An entry with no value at all matches only a query that needs none
        // (match_all, a must_not); then no column narrows the documents, and
        // each one's entries are read from its source alone.
        boolean emptyEntryMatches = test.test(Map.of());
        String present = exists(path);
        if (NONE.equals(present) && !emptyEntryMatches) {
            return NONE;
        }
        // The narrowing reads every column's values, as a predicate the engine runs here, not in a vector search.
        InfinoQueryTranslator wide = columnValuesOnly ? new InfinoQueryTranslator(cols, table, nowMillis, trusted, false, rows) : this;
        String narrowed = emptyEntryMatches ? ALL : and(List.of(present, wide.mayMatch(n.query())));
        List<Map<String, Object>> candidates = rows.apply(
            "SELECT " + OSID + ", " + SOURCE + " FROM " + quoteIdent(table) + " WHERE " + narrowed + " LIMIT " + (NESTED_CANDIDATES_MAX + 1)
        );
        if (candidates.size() > NESTED_CANDIDATES_MAX) {
            throw new IllegalArgumentException(
                "[infino] the nested query on ["
                    + path
                    + "] leaves more than "
                    + NESTED_CANDIDATES_MAX
                    + " documents to check entry by entry, which the table does not serve: "
                    + n.query()
            );
        }
        List<String> ids = new ArrayList<>();
        for (Map<String, Object> row : candidates) {
            Object id = row.get(OSID);
            Object source = row.get(SOURCE);
            List<Map<String, Object>> entries = source == null
                ? List.of()
                : entries(XContentHelper.convertToMap(new BytesArray(source.toString()), false, MediaTypeRegistry.JSON).v2(), path);
            if (entries.isEmpty() && !emptyEntryMatches) {
                // Its columns hold values under the path its source does not: its entries cannot be read, so neither can the answer.
                throw new IllegalArgumentException(
                    "[infino] the nested query on [" + path + "] is not served: document [" + id + "] has values under [" + path + "] its source does not hold"
                );
            }
            for (Map<String, Object> entry : entries) {
                if (test.test(entry)) {
                    ids.add(literal(String.valueOf(id)));
                    break;
                }
            }
        }
        return ids.isEmpty() ? NONE : OSID + " IN (" + String.join(", ", ids) + ")";
    }

    /** The predicates all holding: the one there is, or their conjunction; every row for none. */
    private static String and(List<String> predicates) {
        List<String> kept = new ArrayList<>();
        for (String p : predicates) {
            if (!ALL.equals(p)) {
                kept.add(p);
            }
        }
        if (kept.isEmpty()) {
            return ALL;
        }
        return kept.size() == 1 ? kept.get(0) : "(" + String.join(" AND ", kept) + ")";
    }

    /**
     * A predicate over the flattened columns that holds wherever a nested
     * query's inner query may hold on an entry: each clause on the values of
     * every entry together. A negation, or a clause the columns cannot read,
     * narrows nothing, so every row it would decide stays a candidate.
     */
    private String mayMatch(QueryBuilder qb) {
        if (qb instanceof BoolQueryBuilder b) {
            List<String> and = new ArrayList<>();
            for (QueryBuilder c : b.must()) {
                and.add(mayMatch(c));
            }
            for (QueryBuilder c : b.filter()) {
                and.add(mayMatch(c));
            }
            boolean nothingRequired = b.must().isEmpty() && b.filter().isEmpty();
            if (!b.should().isEmpty() && minimumShouldMatch(b.minimumShouldMatch(), b.should().size(), nothingRequired) > 0) {
                List<String> or = new ArrayList<>();
                for (QueryBuilder c : b.should()) {
                    or.add(mayMatch(c));
                }
                if (!or.contains(ALL)) {
                    and.add("(" + String.join(" OR ", or) + ")");
                }
            }
            return and(and);
        }
        if (qb instanceof NestedQueryBuilder inner) {
            return mayMatch(inner.query());
        }
        if (qb instanceof TermQueryBuilder
            || qb instanceof TermsQueryBuilder
            || qb instanceof RangeQueryBuilder
            || qb instanceof ExistsQueryBuilder
            || qb instanceof MatchQueryBuilder
            || qb instanceof MatchNoneQueryBuilder) {
            String p = predicate(qb, false);
            return p == null ? ALL : p;
        }
        return ALL;
    }

    /**
     * Whether an entry of the nested query on {@code path} matches {@code qb},
     * its fields read from the entry, which sits at {@code scope} (the path
     * itself, or a deeper nested query's). A clause on a field outside the
     * scope matches nothing, as in stock, whose nested documents hold only
     * their own fields; a field the table has no column for (unmapped, or
     * not visible to the request) matches nothing either. Throws naming the
     * clause the check cannot read.
     */
    private Predicate<Map<String, Object>> entryTest(QueryBuilder qb, String path, String scope) {
        if (qb == null || qb instanceof MatchAllQueryBuilder) {
            return e -> true;
        }
        if (qb instanceof MatchNoneQueryBuilder) {
            return e -> false;
        }
        if (qb instanceof TermQueryBuilder t) {
            Object q = t.value();
            return entryValue(t.fieldName(), qb, path, scope, (c, v) -> sameValue(c, v, q));
        }
        if (qb instanceof TermsQueryBuilder ts) {
            if (ts.termsLookup() != null) {
                throw notInNested(qb, path, "a terms lookup");
            }
            List<Object> qs = ts.values() == null ? List.of() : ts.values();
            return entryValue(ts.fieldName(), qb, path, scope, (c, v) -> {
                for (Object q : qs) {
                    if (sameValue(c, v, q)) {
                        return true;
                    }
                }
                return false;
            });
        }
        if (qb instanceof MatchQueryBuilder m) {
            // A match on an exact field is an equality, as the translator reads it elsewhere.
            Object q = m.value();
            return entryValue(m.fieldName(), qb, path, scope, (c, v) -> sameValue(c, v, q));
        }
        if (qb instanceof RangeQueryBuilder r) {
            return entryValue(r.fieldName(), qb, path, scope, (c, v) -> inRange(c, v, r));
        }
        if (qb instanceof ExistsQueryBuilder e) {
            return entryExists(e, path, scope);
        }
        if (qb instanceof BoolQueryBuilder b) {
            List<Predicate<Map<String, Object>>> required = new ArrayList<>();
            for (QueryBuilder c : b.must()) {
                required.add(entryTest(c, path, scope));
            }
            for (QueryBuilder c : b.filter()) {
                required.add(entryTest(c, path, scope));
            }
            List<Predicate<Map<String, Object>>> optional = new ArrayList<>();
            for (QueryBuilder c : b.should()) {
                optional.add(entryTest(c, path, scope));
            }
            List<Predicate<Map<String, Object>>> prohibited = new ArrayList<>();
            for (QueryBuilder c : b.mustNot()) {
                prohibited.add(entryTest(c, path, scope));
            }
            boolean nothingRequired = required.isEmpty();
            int atLeast = optional.isEmpty() ? 0 : Math.min(minimumShouldMatch(b.minimumShouldMatch(), optional.size(), nothingRequired), optional.size());
            return e -> {
                for (Predicate<Map<String, Object>> p : required) {
                    if (!p.test(e)) {
                        return false;
                    }
                }
                for (Predicate<Map<String, Object>> p : prohibited) {
                    if (p.test(e)) {
                        return false;
                    }
                }
                int matched = 0;
                for (Predicate<Map<String, Object>> p : optional) {
                    if (matched >= atLeast) {
                        break;
                    }
                    if (p.test(e)) {
                        matched++;
                    }
                }
                return matched >= atLeast;
            };
        }
        if (qb instanceof NestedQueryBuilder inner) {
            if (inner.innerHit() != null) {
                throw new IllegalArgumentException("[infino] inner_hits on the nested query on [" + inner.path() + "] is not served by the table");
            }
            String sub = relative(inner.path(), scope);
            if (sub == null) {
                throw notInNested(qb, path, "a nested query on [" + inner.path() + "], which is not under [" + scope + "]");
            }
            Predicate<Map<String, Object>> test = entryTest(inner.query(), path, inner.path());
            return e -> {
                for (Map<String, Object> entry : entries(e, sub)) {
                    if (test.test(entry)) {
                        return true;
                    }
                }
                return false;
            };
        }
        throw notInNested(qb, path, null);
    }

    /** The refusal of a clause inside a nested query the entry check does not read. */
    private static IllegalArgumentException notInNested(QueryBuilder qb, String path, String what) {
        return new IllegalArgumentException(
            "[infino] ["
                + qb.getWriteableName()
                + "]"
                + (what == null ? "" : " on " + what)
                + " inside the nested query on ["
                + path
                + "] is not served by the table; inside one it serves term, terms, match, range and exists on keyword,"
                + " numeric and boolean fields, combined by bool or a deeper nested query"
        );
    }

    /**
     * An entry holding a value of {@code field} that passes {@code test}, the
     * field read through its column (so a field masked for the user is
     * refused, and one the user may not see matches nothing). A text field
     * would be matched by its analyzed terms and a date by its parsed
     * instant, which the check does not reproduce: refused.
     */
    private Predicate<Map<String, Object>> entryValue(
        String field,
        QueryBuilder clause,
        String path,
        String scope,
        BiPredicate<Column, Object> test
    ) {
        Column c = column(field);
        if (c == null) {
            return e -> false;
        }
        if (c.fts()) {
            throw notInNested(clause, path, "the text field [" + field + "]");
        }
        if (!(c.isString() || c.isNumeric() || c.isBoolean())) {
            throw notInNested(clause, path, "the " + (c.isTimestamp() ? "date" : c.type()) + " field [" + field + "]");
        }
        String rel = relative(c.name(), scope);
        if (rel == null) {
            return e -> false;
        }
        return e -> {
            for (Object v : valuesAt(e, rel)) {
                if (test.test(c, v)) {
                    return true;
                }
            }
            return false;
        };
    }

    /** An entry with a value of the field, or of any field under it when it is an object. */
    private Predicate<Map<String, Object>> entryExists(ExistsQueryBuilder e, String path, String scope) {
        String field = e.fieldName();
        if (org.opensearch.common.regex.Regex.isSimpleMatchPattern(field)) {
            throw notInNested(e, path, "the pattern [" + field + "]");
        }
        List<String> rels = new ArrayList<>();
        Column c = resolve(field);
        if (c != null) {
            String rel = relative(c.name(), scope);
            if (rel != null) {
                rels.add(rel);
            }
        } else {
            for (Column sub : cols.values()) {
                String rel = relative(sub.name(), scope);
                if (rel != null && !sub.isList() && sub.name().startsWith(field + ".") && !rels.contains(rel)) {
                    rels.add(rel);
                }
            }
        }
        return entry -> {
            for (String rel : rels) {
                if (!valuesAt(entry, rel).isEmpty()) {
                    return true;
                }
            }
            return false;
        };
    }

    /** {@code name} relative to {@code scope}, or null when it is not under it. */
    private static String relative(String name, String scope) {
        return name.startsWith(scope + ".") ? name.substring(scope.length() + 1) : null;
    }

    /** The entries of the array of objects at {@code path} in a document or entry: one object is one entry. */
    @SuppressWarnings("unchecked")
    static List<Map<String, Object>> entries(Map<String, Object> doc, String path) {
        List<Map<String, Object>> out = new ArrayList<>();
        for (Object o : valuesAt(doc, path)) {
            if (o instanceof Map<?, ?> m) {
                out.add((Map<String, Object>) m);
            }
        }
        return out;
    }

    /** Every non-null value at {@code path} under {@code node}, arrays read through, a key holding dots read as its path. */
    static List<Object> valuesAt(Object node, String path) {
        List<Object> out = new ArrayList<>();
        collect(node, path.split("\\."), 0, out);
        return out;
    }

    private static void collect(Object node, String[] parts, int at, List<Object> out) {
        if (node instanceof List<?> list) {
            for (Object o : list) {
                collect(o, parts, at, out);
            }
            return;
        }
        if (at == parts.length) {
            if (node != null) {
                out.add(node);
            }
            return;
        }
        if (node instanceof Map<?, ?> map) {
            StringBuilder key = new StringBuilder();
            for (int j = at; j < parts.length; j++) {
                if (j > at) {
                    key.append('.');
                }
                key.append(parts[j]);
                if (map.containsKey(key.toString())) {
                    collect(map.get(key.toString()), parts, j + 1, out);
                }
            }
        }
    }

    /**
     * Whether a source value equals a query value as the field indexes both:
     * a keyword its text, a long its integer part (a long field indexes 5.7
     * as 5), a double its number, a boolean its truth.
     */
    private static boolean sameValue(Column c, Object v, Object q) {
        if (v == null || q == null) {
            return false;
        }
        if (c.isString()) {
            return v.toString().equals(q.toString());
        }
        if (c.isNumeric()) {
            BigDecimal a = indexed(c, v);
            BigDecimal b = decimal(q);
            return a != null && b != null && a.compareTo(b) == 0;
        }
        if (c.isBoolean()) {
            Boolean a = truth(v);
            return a != null && a.equals(truth(q));
        }
        return false;
    }

    /** Whether a source value lies within a range, compared as the field indexes it (a keyword by its UTF-8 bytes). */
    private static boolean inRange(Column c, Object v, RangeQueryBuilder r) {
        if (v == null) {
            return false;
        }
        if (c.isString()) {
            byte[] x = v.toString().getBytes(StandardCharsets.UTF_8);
            return within(r, b -> Arrays.compareUnsigned(x, b.toString().getBytes(StandardCharsets.UTF_8)));
        }
        if (c.isNumeric()) {
            BigDecimal x = indexed(c, v);
            if (x == null) {
                return false;
            }
            return within(r, b -> {
                BigDecimal bound = decimal(b);
                if (bound == null) {
                    throw new IllegalArgumentException("[infino] the range on [" + r.fieldName() + "] has the bound [" + b + "], which is not a number");
                }
                return x.compareTo(bound);
            });
        }
        return false;
    }

    /** Whether a value, compared to each bound by {@code compareTo} (the value's order against the bound), lies within the range. */
    private static boolean within(RangeQueryBuilder r, ToIntFunction<Object> compareTo) {
        if (r.from() != null) {
            int cmp = compareTo.applyAsInt(r.from());
            if (r.includeLower() ? cmp < 0 : cmp <= 0) {
                return false;
            }
        }
        if (r.to() != null) {
            int cmp = compareTo.applyAsInt(r.to());
            if (r.includeUpper() ? cmp > 0 : cmp >= 0) {
                return false;
            }
        }
        return true;
    }

    /** A source value of a numeric field as the field indexes it: a long's integer part, a double whole. */
    private static BigDecimal indexed(Column c, Object v) {
        BigDecimal d = decimal(v);
        return d != null && "i64".equals(c.type()) ? d.setScale(0, RoundingMode.DOWN) : d;
    }

    private static BigDecimal decimal(Object v) {
        if (v instanceof Map<?, ?> || v instanceof List<?> || v instanceof Boolean) {
            return null;
        }
        try {
            return new BigDecimal(v.toString().trim());
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private static Boolean truth(Object v) {
        if (v instanceof Boolean b) {
            return b;
        }
        String s = v.toString();
        return "true".equals(s) ? Boolean.TRUE : "false".equals(s) ? Boolean.FALSE : null;
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
        String query = asPhrase ? phrase(value) : plainTerms(value);
        return query.isEmpty() ? NONE : textClause(c.name(), query, op == Operator.AND ? "and" : "or", positive);
    }

    private String textClause(String column, String query, String mode, boolean positive) {
        if (columnValuesOnly) {
            throw new IllegalArgumentException(
                "[infino] a knn filter matching text on [" + column + "] (" + query + ") is not served: the engine narrows its vector search by column values, and a text match is a search of its own"
            );
        }
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
        return TEXT_SUBQUERY + literal(table) + ", " + literal(column) + ", " + literal(query) + ", " + literal(mode) + "))";
    }

    /**
     * Whether {@code predicate} reads the token index through a subquery,
     * which the engine plans in a WHERE clause but not inside an aggregate's
     * expression.
     */
    static boolean readsTextIndex(String predicate) {
        return predicate.contains(TEXT_SUBQUERY);
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
        return textOnFields(fields, value, op, positive, asPhrase, exactToo, false);
    }

    /**
     * As {@link #textOnFields(List, String, Operator, boolean, boolean, boolean)};
     * with {@code composed}, {@code value} is an engine query the translator
     * built itself (a query string's clauses with their sigils), sent as it is.
     */
    private String textOnFields(List<String> fields, String value, Operator op, boolean positive, boolean asPhrase, boolean exactToo, boolean composed) {
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
        String query = composed ? value : asPhrase ? phrase(value) : plainTerms(value);
        if (query.isEmpty()) {
            // No term to match on a text column: only an exact column's equality can.
            indexed.clear();
            if (exact.isEmpty()) {
                return NONE;
            }
        }
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

    /** What separates the runs of an engine text query: Unicode white space, as the engine splits it. */
    private static final Pattern QUERY_WHITESPACE = Pattern.compile("(?U)\\s+");

    /**
     * {@code text} as an engine text query of plain terms. The engine reads a
     * run's leading {@code +} or {@code -} as a required or an excluded clause
     * and a double quote as the bound of a phrase; a match's text means none
     * of that ({@code -error timeout} is the two words, not "timeout and not
     * error"), and an id holding one ({@code +15551234}) is just the id.
     * Neither character is part of a term, so the terms are those of the
     * text. Empty when the text holds no term at all.
     */
    static String plainTerms(String text) {
        StringBuilder out = new StringBuilder();
        for (String run : QUERY_WHITESPACE.split(text.replace('"', ' '))) {
            int start = 0;
            while (start < run.length() && (run.charAt(start) == '+' || run.charAt(start) == '-')) {
                start++;
            }
            if (start < run.length()) {
                if (out.length() > 0) {
                    out.append(' ');
                }
                out.append(run, start, run.length());
            }
        }
        return out.toString();
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
            // Every text field once: an alias names a column already here. The id
            // column carries a term index but is no field of the documents, and a
            // field masked for the user is left out rather than matched by its
            // clear values.
            for (Map.Entry<String, Column> entry : cols.entrySet()) {
                Column c = entry.getValue();
                if (c.fts() && !InfinoNative.isAlias(entry.getKey(), c) && !c.name().equals(OSID) && (trusted || !c.masked())) {
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
                return textOnFields(targets, engine, Operator.OR, positive, false, false, true);
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
                if (!(n instanceof Term t) || t.field() != null || t.fuzzy() > 0 || t.slop() > 0) {
                    return null;
                }
                // The parser has read the clause's own sigil; its text is the words alone.
                String words = t.quoted() ? phrase(t.text()) : plainTerms(t.text());
                if (words.isBlank() || (!t.quoted() && words.indexOf(' ') >= 0)) {
                    // Several words under one sigil are no single engine clause.
                    return null;
                }
                if (out.length() > 0) {
                    out.append(' ');
                }
                out.append(sigil).append(words);
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
            return exists(a.text());
        }
        if ("_id".equals(a.field())) {
            return OSID + " = " + literal(a.text());
        }
        Column c = column(a.field());
        if (c == null) {
            return NONE;
        }
        if (c.fts()) {
            String query = a.quoted() ? phrase(a.text()) : plainTerms(a.text());
            return query.isEmpty() ? NONE : textClause(c.name(), query, "and", false);
        }
        if (!a.quoted() && (a.text().indexOf('*') >= 0 || a.text().indexOf('?') >= 0)) {
            return like(c, wildcardToLike(a.text()));
        }
        return equality(a.field(), a.text());
    }

    // ---- scalars ----

    private String equality(String field, Object value) {
        Column c = column(field);
        if (c == null || value == null) {
            return c == null ? NONE : null;
        }
        String lit = scalar(c.element(), value, false);
        return lit == null ? null : anyValue(c, v -> v + " = " + lit);
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
            String lit = scalar(c.element(), v, false);
            if (lit == null) {
                return null;
            }
            lits.add(lit);
        }
        String in = " IN (" + String.join(", ", lits) + ")";
        return anyValue(c, v -> v + in);
    }

    private String range(RangeQueryBuilder r) {
        Column c = column(r.fieldName());
        if (c == null) {
            return NONE;
        }
        Column typed = c.element();
        if (!(typed.isNumeric() || typed.isTimestamp() || typed.isString())) {
            return null;
        }
        // Each bound as a comparison and its literal, applied to whichever values the field has.
        List<String> bounds = new ArrayList<>();
        ZoneId zone = r.timeZone() == null ? null : ZoneId.of(r.timeZone());
        // A date bound is read as OpenSearch reads it: in the request's format, else the field's own.
        String format = r.format() != null ? r.format() : typed.dateFormat();
        if (r.from() != null) {
            // An exclusive lower date bound rounds up (gt 2026-10-24 means after that day).
            String lit = typed.isTimestamp() ? dateBound(r.from(), format, !r.includeLower(), zone) : scalar(typed, r.from(), false);
            if (lit == null) {
                return null;
            }
            bounds.add((r.includeLower() ? " >= " : " > ") + lit);
        }
        if (r.to() != null) {
            String lit = typed.isTimestamp() ? dateBound(r.to(), format, r.includeUpper(), zone) : scalar(typed, r.to(), false);
            if (lit == null) {
                return null;
            }
            bounds.add((r.includeUpper() ? " <= " : " < ") + lit);
        }
        if (bounds.isEmpty()) {
            return ALL;
        }
        if (typed.isTimestamp() && !c.isList() && r.from() != null && r.to() != null) {
            // A bound rounded up ends in .999: past it (gt) starts a millisecond later, through it (lte) ends a millisecond later.
            long from = epochMillis(bounds.get(0)) + (r.includeLower() ? 0 : 1);
            long until = epochMillis(bounds.get(1)) + (r.includeUpper() ? 1 : 0);
            span = new Span(c.name(), from, until);
        }
        return anyValue(c, v -> {
            List<String> parts = new ArrayList<>(bounds.size());
            for (String bound : bounds) {
                parts.add(v + bound);
            }
            return "(" + String.join(" AND ", parts) + ")";
        });
    }

    /** The epoch milliseconds of a bound {@link #range} wrote: a comparison and a quoted ISO instant. */
    private static long epochMillis(String bound) {
        int open = bound.indexOf('\'');
        return Instant.parse(bound.substring(open + 1, bound.lastIndexOf('\''))).toEpochMilli();
    }

    private String like(String field, String pattern) {
        Column c = column(field);
        if (c == null) {
            return NONE;
        }
        return like(c, pattern);
    }

    /** The rows where a value of a string field matches a LIKE pattern; null for a field of another type. */
    private String like(Column c, String pattern) {
        if (!c.element().isString()) {
            return null;
        }
        return anyValue(c, v -> v + " LIKE " + literal(pattern));
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
        return literalForType(type, value, nowMillis, null);
    }

    /**
     * {@link #literalForType(SqlType, Object, long)} for a value of a field whose date format
     * is {@code dateFormat} (null for the default), read as stock reads a search_after value:
     * a number is epoch milliseconds, as a hit's sort values give it, and text is read by the
     * field's format (an {@code epoch_second} field's {@code "897436800"} is seconds).
     */
    static String literalForType(SqlType type, Object value, long nowMillis, String dateFormat) {
        return switch (type) {
            case STRING -> literal(value.toString());
            case BOOL -> Boolean.parseBoolean(value.toString()) ? "true" : "false";
            case INT, FLOAT -> number(value);
            case DATE -> new InfinoQueryTranslator(Map.of(), "", nowMillis, false).dateLiteral(
                value,
                value instanceof Number ? null : dateFormat,
                false,
                null
            );
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
            return dateLiteral(value, c.dateFormat(), roundUp, null);
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

    /**
     * A range's date bound as {@link #dateLiteral}; a bound {@code format} cannot read is refused
     * as OpenSearch refuses it, rather than matching nothing.
     */
    private String dateBound(Object value, String format, boolean roundUp, ZoneId zone) {
        String lit = dateLiteral(value, format, roundUp, zone);
        if (lit == null) {
            throw new OpenSearchParseException(
                "failed to parse date field [{}] with format [{}]",
                value,
                format == null ? DEFAULT_DATE_FORMAT : format
            );
        }
        return lit;
    }

    /**
     * A timestamp literal from a date, epoch milliseconds, or date math, in the engine's ISO form.
     * A number is epoch milliseconds in the default format and read by {@code format} when one is
     * given, as OpenSearch reads a number by the field's format (an {@code epoch_second} field's
     * {@code 893966296} is seconds).
     */
    private String dateLiteral(Object value, String format, boolean roundUp, ZoneId zone) {
        Instant at;
        if (value instanceof Number n && format == null) {
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
