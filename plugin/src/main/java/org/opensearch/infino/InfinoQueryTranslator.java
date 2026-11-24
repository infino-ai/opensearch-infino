/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.search.join.ScoreMode;
import org.opensearch.OpenSearchParseException;
import org.opensearch.OpenSearchSecurityException;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.lucene.search.Queries;
import org.opensearch.common.network.InetAddresses;
import org.opensearch.common.time.DateFormatter;
import org.opensearch.common.time.DateMathParser;
import org.opensearch.common.unit.Fuzziness;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.index.Index;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.XContentBuilder;
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
import org.opensearch.index.query.QueryShardException;
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

import java.io.IOException;
import java.io.StringReader;
import java.io.UncheckedIOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
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
 * those columns. None of these materializes a match set in Java; only a
 * nested query reads rows into Java, its candidates' sources, up to
 * {@link #NESTED_CANDIDATES_MAX}.
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
     * The reserved list column holding the paths of a document's fields that OpenSearch's mapper ignored for a
     * malformed value ({@link InfinoIgnored}), served as {@code _ignored}; null for a document with none.
     */
    static final String IGNORED = "osignored";
    /** The metadata field listing a document's ignored fields. */
    static final String IGNORED_FIELD = "_ignored";
    /**
     * What the bridge adds to a field's column name for the list column that
     * holds the field's values for a document that gives it several; the
     * field's own column is then null for that document (a string field's
     * keeps the values joined, for its text index).
     */
    static final String LIST_SUFFIX = "[]";
    /**
     * What the bridge puts before the name of a field named like {@link #OSID}, {@link #SOURCE}, {@link #SEQ} or
     * {@link #IGNORED} for the column holding it: no mapping names a field so.
     */
    static final String OWN_COLUMN_PREFIX = ".";
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
    /**
     * How a text clause of plain terms that is not the FROM source reads the
     * token index: the engine's row predicate, which the scan answers from
     * the term dictionary and the postings of a {@code standard} column (as
     * the bridge declares every text column), so the clause neither collects
     * every matching id before the outer query starts nor joins on them.
     */
    private static final String TEXT_PREDICATE = "text_match(";
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
     * One clause's share of a hit's score beyond a text clause's BM25:
     * {@code expr} a SQL number per row (0 where the clause does not match),
     * {@code values} the scores it takes where it does. {@code join} is
     * the relation {@code expr} reads, joined to the hits' rows by id (null
     * for none): a clause whose predicate holds a subquery by id, which the
     * engine plans only in a filter, matches where the join finds the row.
     */
    record Bonus(String expr, List<Double> values, String join) {
        Bonus(String expr, List<Double> values) {
            this(expr, values, null);
        }
    }

    /**
     * A query string's or a multi_match's BM25 over several text columns, as
     * stock scores it: each of {@code clauses} scores the best of its
     * columns' BM25, each times its column's weight in {@code weights} (a
     * {@code dis_max} with tie_breaker 0, one {@link TextClause} per column),
     * and a hit scores the sum over the clauses it matches (a bool's should
     * clauses). Under {@code mode} {@code or} a hit matches any clause and
     * any of a clause's words; under {@code and}, one clause alone, all its
     * words on one column. A column not in {@code weights} weighs 1.
     * {@code match} is the predicate the query translated to, every row the
     * clauses match; the ranked clauses read the same rows. {@code match} is
     * null for a bool's match clauses beside a ranked principal, each on one
     * column ({@link #summedClauses}): the translation's predicate and the
     * principal's own keep the rows they match, and the clauses only score them.
     */
    record Across(List<List<TextClause>> clauses, String match, String mode, Map<String, Float> weights) {
        /** The columns of a clause share its query, every clause reads {@code mode}, and {@code and} holds one clause. */
        Across {
            if ("and".equals(mode) ? clauses.size() != 1 : !"or".equals(mode)) {
                throw new IllegalArgumentException("ranked clauses under mode [" + mode + "]: " + clauses);
            }
            for (List<TextClause> clause : clauses) {
                for (TextClause c : clause) {
                    if (!c.query().equals(clause.get(0).query()) || !mode.equals(c.mode())) {
                        throw new IllegalArgumentException("a ranked clause's columns differ in query or mode: " + clause);
                    }
                }
            }
            weights = Map.copyOf(weights);
        }

        /** Clauses that match any of their words, every column weighing 1. */
        Across(List<List<TextClause>> clauses, String match) {
            this(clauses, match, "or", Map.of());
        }

        /**
         * The weights as {@code bm25_search_fields} reads them, a JSON object
         * of column to factor, sorted by column name; null when every column weighs 1.
         */
        String weightsJson() {
            if (weights.isEmpty()) {
                return null;
            }
            try (XContentBuilder b = XContentFactory.jsonBuilder()) {
                b.startObject();
                for (Map.Entry<String, Float> w : new java.util.TreeMap<>(weights).entrySet()) {
                    b.field(w.getKey(), w.getValue());
                }
                b.endObject();
                return BytesReference.bytes(b).utf8ToString();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        /**
         * The clauses as {@code bm25_search_fields} reads them: a JSON array
         * of {@code {"query", "fields"}}, one per clause, its columns in
         * {@code fields}. The columns of a clause share its query.
         */
        String clausesJson() {
            try (XContentBuilder b = XContentFactory.jsonBuilder()) {
                b.startArray();
                for (List<TextClause> clause : clauses) {
                    b.startObject().field("query", clause.get(0).query()).startArray("fields");
                    for (TextClause c : clause) {
                        b.value(c.column());
                    }
                    b.endArray().endObject();
                }
                b.endArray();
                return BytesReference.bytes(b).utf8ToString();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    /**
     * How a hit scores, as Lucene sums a bool's scoring clauses: the BM25 of
     * a text clause, {@code offset} from the clauses every hit matches with
     * one score (a term, terms, range or exists in must scores its boost),
     * and each {@code bonuses} entry where its clause matches (such a clause
     * in should, or a nested clause, whose score is its entries'). The text
     * clause is the principal when there is one; otherwise {@code text}, a
     * text clause in should, whose BM25 counts where it matches.
     * {@code ascending} when the request sorts by the score ascending: the
     * least sums come first. {@code unrankable} when the query scores its
     * hits apart in stock but no one ranked read here can: a should clause
     * the table cannot score, or several text clauses with no principal. Its
     * hits then all carry {@code offset}. {@code zero} when no clause of the
     * query scores at all (a bool of filter and must_not clauses alone,
     * {@link #scoresNothing}): every hit scores 0, as in stock.
     * {@code across} when a required query string over several text columns
     * is the one scoring clause, or when a ranked principal shares the score
     * with other match clauses: its hits rank by the sum of the clauses'
     * BM25 ({@link Across}), beside {@code offset}.
     */
    record Scoring(TextClause text, double offset, List<Bonus> bonuses, boolean ascending, boolean unrankable, boolean zero, Across across) {
        static final Scoring NONE = new Scoring(null, 0, List.of());
        static final Scoring ZERO = new Scoring(null, 0, List.of(), false, false, true, null);

        Scoring(TextClause text, double offset, List<Bonus> bonuses) {
            this(text, offset, bonuses, false, false, false, null);
        }

        Scoring(TextClause text, double offset, List<Bonus> bonuses, boolean ascending) {
            this(text, offset, bonuses, ascending, false, false, null);
        }

        Scoring(TextClause text, double offset, List<Bonus> bonuses, boolean ascending, boolean unrankable) {
            this(text, offset, bonuses, ascending, unrankable, false, null);
        }

        Scoring(TextClause text, double offset, List<Bonus> bonuses, boolean ascending, boolean unrankable, boolean zero) {
            this(text, offset, bonuses, ascending, unrankable, zero, null);
        }

        /** Whether hits rank by more than the principal's BM25 alone: then {@code offset} only shifts every score alike. */
        boolean reorders() {
            return text != null || !bonuses.isEmpty();
        }
    }

    /**
     * What a query translates to. {@code principal} is the text clause the
     * rows are read through ({@link #from}), and {@code ranks} whether its
     * BM25 scores the hits: false for a clause in filter context, which
     * scores nothing in stock ({@link #ranked}). {@code span} is set when
     * the query is one range on a date column with both bounds, and is then
     * exactly the rows {@code where} keeps; null otherwise.
     */
    record Translation(String table, String where, TextClause principal, boolean ranks, Scoring scoring, Span span) {
        Translation(String table, String where, TextClause principal) {
            this(table, where, principal, true, Scoring.NONE, null);
        }

        /** The principal when its BM25 scores the hits, else null. */
        TextClause ranked() {
            return ranks ? principal : null;
        }

        /** This translation with {@code where} as its predicate, scored as before; no longer one span. */
        Translation withWhere(String where) {
            return new Translation(table, where, principal, ranks, scoring, null);
        }

        /** This translation with no clause but the principal's BM25 scoring the hits. */
        Translation unscored() {
            return new Translation(table, where, principal, ranks, scoring.zero() ? Scoring.ZERO : Scoring.NONE, span);
        }

        /**
         * This translation with its hits in ascending score. Only the bonuses
         * of a query with no text clause are summed over every hit; a text
         * clause's BM25 ranks its best hits only, so such a query is unscored.
         */
        Translation ascending() {
            if (ranked() != null || scoring.text() != null || scoring.across() != null) {
                return unscored();
            }
            Scoring up = new Scoring(null, scoring.offset(), scoring.bonuses(), true, scoring.unrankable(), scoring.zero());
            return new Translation(table, where, principal, ranks, up, span);
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

        /**
         * The principal clause as a predicate on the table's rows, for a page
         * that needs no score: {@code text_match} on its column when it is plain
         * words ({@link #plainQuery}), null when there is none or it needs the
         * token index (a phrase, a required or an excluded word).
         */
        String principalPredicate() {
            return principal == null || !plainQuery(principal.query()) ? null : textPredicate(table, principal);
        }

        /** The ranked relation for hits: BM25 over the ranked principal clause ({@link #ranked}), {@code k} candidates. */
        String rankedFrom(int k) {
            return rankedFrom(ranked(), k);
        }

        /** The ranked relation for hits: BM25 over {@code text}, {@code k} candidates. */
        String rankedFrom(TextClause text, int k) {
            return "bm25_search("
                + literal(table)
                + ", "
                + literal(text.column())
                + ", "
                + literal(text.query())
                + ", "
                + k
                + ", "
                + literal(text.mode())
                + ")";
        }

        /**
         * The ranked relation for a query across several text columns: the
         * engine's {@code bm25_search_fields}, which scores each clause by its
         * best weighted column and sums the clauses a row matches, {@code k}
         * candidates.
         */
        String rankedFrom(Across across, int k) {
            String weights = across.weightsJson();
            return "bm25_search_fields("
                + literal(table)
                + ", "
                + literal(across.clausesJson())
                + ", "
                + k
                + ", "
                + literal(across.mode())
                + (weights == null ? "" : ", " + literal(weights))
                + ")";
        }

        /**
         * The predicate {@code text} is as a clause other than the ranked one,
         * exactly as the translator wrote it into {@link #where} (see
         * {@link #textPredicate}).
         */
        String textMatch(TextClause text) {
            return textPredicate(table, text);
        }
    }

    /**
     * The predicate a text clause other than the ranked one is: the engine's
     * {@code text_match} on the column for plain words ({@link #plainQuery}),
     * otherwise the ids its token match holds, as a subquery over the token
     * index of {@code table}.
     */
    private static String textPredicate(String table, TextClause text) {
        if (plainQuery(text.query())) {
            return TEXT_PREDICATE + quoteIdent(text.column()) + ", " + literal(text.query()) + ", " + literal(text.mode()) + ")";
        }
        return TEXT_SUBQUERY + literal(table) + ", " + literal(text.column()) + ", " + literal(text.query()) + ", " + literal(text.mode()) + "))";
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
    /** Whether {@link #principal} came from a clause that scores, not one in filter context. */
    private boolean principalRanks;
    /** How many filter clauses enclose the clause being translated: within any, a clause scores nothing. */
    private int filtering;
    /**
     * Whether a text clause in filter context is a predicate rather than the
     * principal: so when another text clause scores the hits, which must
     * then be the ranked one, read with the filter's restriction on its rows.
     */
    private boolean filterTextIsPredicate;
    /** A required query string's ranking across its columns ({@link #across(QueryStringQueryBuilder)}), found while scoring. */
    private Across across;
    /** Each nested clause's matching documents with their scores, read once whether it filters, scores or both. */
    private final Map<NestedQueryBuilder, Map<String, Double>> nestedScores = new java.util.IdentityHashMap<>();
    /** Each BM25-scored term clause's score ({@link #bm25TermScore}), read once per clause. */
    private final Map<QueryBuilder, Double> termScores = new java.util.IdentityHashMap<>();

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
        t.filterTextIsPredicate = t.scoresText(qb);
        String where = t.predicate(qb, true);
        if (where == null) {
            return null;
        }
        return new Translation(table, where, t.principal, t.principalRanks, t.scoring(qb), qb instanceof RangeQueryBuilder ? t.span : null);
    }

    /**
     * Whether a text clause scores the hits of {@code qb}: one reached
     * through must and should clauses alone (filter and must_not clauses
     * score nothing) that reads the token index as a principal would.
     */
    private boolean scoresText(QueryBuilder qb) {
        if (qb instanceof BoolQueryBuilder b) {
            for (QueryBuilder c : b.must()) {
                if (scoresText(c)) {
                    return true;
                }
            }
            for (QueryBuilder c : b.should()) {
                if (scoresText(c)) {
                    return true;
                }
            }
            return false;
        }
        if (!(qb instanceof MatchQueryBuilder
            || qb instanceof MatchPhraseQueryBuilder
            || qb instanceof MultiMatchQueryBuilder
            || qb instanceof QueryStringQueryBuilder
            || qb instanceof SimpleQueryStringBuilder)) {
            return false;
        }
        InfinoQueryTranslator alone = new InfinoQueryTranslator(cols, table, nowMillis, trusted, columnValuesOnly, rows);
        try {
            alone.predicate(qb, true);
        } catch (RuntimeException e) {
            // A clause the table cannot read is refused when the query is translated.
            return false;
        }
        return alone.principal != null;
    }

    /**
     * Whether no clause of {@code qb} scores: a bool whose clauses are all
     * filter and must_not ones, or must and should ones that score nothing
     * themselves. Lucene scores its hits 0.
     */
    static boolean scoresNothing(QueryBuilder qb) {
        if (!(qb instanceof BoolQueryBuilder b) || !b.hasClauses()) {
            return false;
        }
        for (QueryBuilder c : b.should()) {
            if (!scoresNothing(c)) {
                return false;
            }
        }
        for (QueryBuilder c : b.must()) {
            if (!scoresNothing(c)) {
                return false;
            }
        }
        return true;
    }

    // ---- scoring ----

    /**
     * How the hits of {@code root} score, as Lucene scores them, beyond the
     * principal's BM25: what the clauses every hit matches add, and what the
     * optional ones add where they match. See {@link Scoring}.
     */
    private Scoring scoring(QueryBuilder root) {
        double[] offset = { 0 };
        List<Bonus> bonuses = new ArrayList<>();
        List<TextClause> texts = new ArrayList<>();
        List<TextClause> summed = new ArrayList<>();
        boolean[] unscored = { false };
        boolean[] stray = { false };
        if (scoresNothing(root)) {
            return Scoring.ZERO;
        }
        required(root, offset, bonuses, texts, summed, unscored, stray);
        TextClause ranked = principalRanks ? principal : null;
        Across clauses = ranked == null || across != null || stray[0] || !bonuses.isEmpty() ? null : summedClauses(summed);
        if (clauses != null) {
            // The principal's BM25 and every other match clause's, summed where each matches.
            return new Scoring(null, offset[0], List.of(), false, false, false, clauses);
        }
        if (across != null) {
            if (principal == null && !unscored[0] && texts.isEmpty() && bonuses.isEmpty()) {
                // The query string's BM25 across its columns is the only score beside the offset.
                return new Scoring(null, offset[0], List.of(), false, false, false, across);
            }
            // Beside another scoring clause the sum is no longer the query string's alone.
            unscored[0] = true;
        }
        if (ranked == null && (unscored[0] || texts.size() > 1)) {
            // A should clause the table cannot score (or a second text clause,
            // which would add its BM25 beside the first) would leave some of the
            // hits it matches at 0, under hits it outscores in stock: with no
            // ranked clause, the hits keep the one score they all had.
            return new Scoring(null, offset[0], List.of(), false, true);
        }
        // Beside a ranked clause a should clause the table cannot score adds
        // nothing, as before; the others add theirs.
        TextClause text = ranked == null && texts.size() == 1 ? texts.get(0) : null;
        return offset[0] == 0 && bonuses.isEmpty() && text == null ? Scoring.NONE : new Scoring(text, offset[0], List.copyOf(bonuses));
    }

    /**
     * A clause every hit matches: the root, or a must clause of a bool every
     * hit matches. {@code summed} gathers the text clauses whose BM25 the
     * hits may sum ({@link #summedClauses}), required and optional, null for
     * one that scores otherwise than plain BM25 ({@link #summable});
     * {@code stray} is set by a clause that scores and is no text clause,
     * which no sum of them holds.
     */
    private void required(
        QueryBuilder qb,
        double[] offset,
        List<Bonus> bonuses,
        List<TextClause> texts,
        List<TextClause> summed,
        boolean[] unscored,
        boolean[] stray
    ) {
        // A bool with no clause is a match_all: it scores its boost (constantScore).
        if (qb instanceof BoolQueryBuilder b && b.hasClauses()) {
            if (b.boost() != 1f) {
                // A boosted bool weighs its clauses' sum, which no sum of them keeps.
                summed.add(null);
            }
            // A filter or must_not clause scores nothing.
            for (QueryBuilder c : b.must()) {
                required(c, offset, bonuses, texts, summed, unscored, stray);
            }
            for (QueryBuilder c : b.should()) {
                int known = texts.size();
                if (!optional(c, bonuses, texts)) {
                    unscored[0] = true;
                    stray[0] = true;
                } else if (texts.size() == known + 1) {
                    // An optional clause scores where it matches: all its words
                    // under operator and, which a sum over any of them would not keep.
                    TextClause t = texts.get(known);
                    summed.add(summable(c) && "or".equals(t.mode()) ? t : null);
                } else if (texts.size() > known) {
                    // A clause that reads as several text clauses scores as their
                    // combination, which no plain sum of them keeps.
                    summed.add(null);
                }
            }
            return;
        }
        Double w = constantScore(qb);
        if (w != null) {
            offset[0] += w;
        } else if (qb instanceof NestedQueryBuilder n && !entriesScoredApart(n.query())) {
            Bonus nb = nestedBonus(n);
            if (nb != null) {
                bonuses.add(nb);
            }
        } else {
            // A text clause here ranks the hits when it is the principal; any
            // other (a text clause on a second column, a query string over
            // several, a script) scores nothing here, where stock scores it.
            TextClause t = textOnly(qb);
            if (t != null) {
                summed.add(summable(qb) ? t : null);
            } else {
                stray[0] = true;
            }
            if (t == null || !principalRanks || !t.equals(principal)) {
                // A query string or a multi_match over several columns ranks across them when it is the one text clause.
                boolean ranks = principal == null && across == null;
                if (ranks && qb instanceof MultiMatchQueryBuilder mm) {
                    refuseUnscorable(mm);
                }
                Across a = ranks ? across(qb) : null;
                if (a != null) {
                    across = a;
                } else {
                    unscored[0] = true;
                }
            }
        }
    }

    /**
     * How the hits rank when a bool's match clauses each score, as Lucene
     * sums a bool's scoring clauses: the ranked principal and the other
     * text clauses in {@code summed} (required ones, which the predicate
     * keeps, and optional ones, which score where they match), each on its
     * one column ({@link Across}). Null when the principal's BM25 alone is
     * the score: no other text clause, one that scores otherwise
     * ({@link #summable}), one past plain words or the principal's own
     * predicate ({@link #plainQuery}), or past {@link #MAX_ACROSS_SEARCHES}.
     * A clause scores in {@code or}: within the rows a required clause's
     * predicate keeps, all of its words, the sum is the same.
     */
    private Across summedClauses(List<TextClause> summed) {
        List<TextClause> others = new ArrayList<>(summed);
        if (others.contains(null) || !others.remove(principal) || others.isEmpty() || summed.size() > MAX_ACROSS_SEARCHES) {
            return null;
        }
        others.add(0, principal);
        List<List<TextClause>> clauses = new ArrayList<>();
        for (TextClause t : others) {
            if (!plainQuery(t.query())) {
                return null;
            }
            clauses.add(List.of(new TextClause(t.column(), t.query(), "or")));
        }
        return new Across(List.copyOf(clauses), null);
    }

    /**
     * Whether {@code qb} scores as the plain BM25 of its words: a match with
     * boost 1 and no option that changes which words it reads or how they
     * score (minimum_should_match, fuzziness, an analyzer).
     */
    private static boolean summable(QueryBuilder qb) {
        return qb instanceof MatchQueryBuilder m
            && m.boost() == 1f
            && m.minimumShouldMatch() == null
            && m.fuzziness() == null
            && m.analyzer() == null;
    }

    /** How {@code qb} ranks across its text columns ({@link Across}): a query string's or a multi_match's, else null. */
    private Across across(QueryBuilder qb) {
        if (qb instanceof QueryStringQueryBuilder qs) {
            return across(qs);
        }
        if (qb instanceof MultiMatchQueryBuilder mm) {
            return across(mm);
        }
        return null;
    }

    /**
     * How stock ranks {@code mm} across its text columns ({@link Across}), or
     * null when the plugin does not rank it that way: it reaches one text
     * column (then it is the principal), a field that is a pattern or no
     * text column, or it sets an option that changes which words it reads.
     *
     * <p>Stock scores each field's match of the words, times the field's
     * boost and the query's, and combines the fields as a {@code dis_max}
     * with the query's tie_breaker ({@link #tieBreaker}): the best field's
     * score plus tie_breaker times the others'. Tie_breaker 0 is one clause
     * over every column, scored by the best; 1 is a clause per column,
     * summed. Under operator and every field must match all the words, which
     * one clause in mode {@code and} reads; a sum of such fields is past the
     * engine's modes, and is left unranked. A phrase is one run of words, so
     * the operator changes nothing for it, as in stock.
     */
    private Across across(MultiMatchQueryBuilder mm) {
        MultiMatchQueryBuilder.Type type = mm.type();
        if ((type != MultiMatchQueryBuilder.Type.BEST_FIELDS
            && type != MultiMatchQueryBuilder.Type.MOST_FIELDS
            && type != MultiMatchQueryBuilder.Type.PHRASE)
            || mm.minimumShouldMatch() != null
            || mm.analyzer() != null
            || mm.slop() != 0
            || mm.cutoffFrequency() != null) {
            return null;
        }
        float tieBreaker = tieBreaker(mm);
        boolean summed = tieBreaker == 1f;
        boolean all = type != MultiMatchQueryBuilder.Type.PHRASE && mm.operator() == Operator.AND;
        if ((!summed && tieBreaker != 0f) || (summed && all)) {
            return null;
        }
        // The predicate the multi_match is; one that set a principal is ranked by it.
        InfinoQueryTranslator alone = new InfinoQueryTranslator(cols, table, nowMillis, trusted, columnValuesOnly, rows);
        String match;
        try {
            match = alone.predicate(mm, true);
        } catch (IllegalArgumentException | OpenSearchSecurityException e) {
            return null;
        }
        if (match == null || alone.principal != null || ALL.equals(match) || NONE.equals(match)) {
            return null;
        }
        List<String> columns = new ArrayList<>();
        Map<String, Float> weights = new LinkedHashMap<>();
        try {
            for (Map.Entry<String, Float> f : mm.fields().entrySet()) {
                Column c = column(f.getKey());
                if (c == null && f.getKey().indexOf('*') < 0) {
                    continue; // stock's dis_max has no score from a field the mapping does not have
                }
                if (c == null || !c.fts() || columns.contains(c.name())) {
                    return null; // a pattern, a column with no text index, or one reached twice
                }
                float weight = f.getValue() * mm.boost();
                if (!Float.isFinite(weight) || weight <= 0) {
                    return null;
                }
                columns.add(c.name());
                if (weight != 1f) {
                    weights.put(c.name(), weight);
                }
            }
        } catch (IllegalArgumentException | OpenSearchSecurityException e) {
            return null; // a field the table serves no column of, or one masked for the user
        }
        if (columns.size() < 2 || columns.size() > MAX_ACROSS_SEARCHES) {
            return null;
        }
        String value = String.valueOf(mm.value());
        String query = type == MultiMatchQueryBuilder.Type.PHRASE ? phrase(value) : plainTerms(value);
        String mode = all ? "and" : "or";
        List<TextClause> each = new ArrayList<>();
        for (String c : columns) {
            each.add(new TextClause(c, query, mode));
        }
        List<List<TextClause>> clauses = new ArrayList<>();
        if (summed) {
            for (TextClause c : each) {
                clauses.add(List.of(c));
            }
        } else {
            clauses.add(List.copyOf(each));
        }
        return new Across(List.copyOf(clauses), match, mode, weights);
    }

    /**
     * The tie_breaker stock combines a multi_match's fields with: the one it
     * names, else its type's (0 for best_fields and the phrase types, 1 for
     * most_fields, the sum of the fields' scores).
     */
    private static float tieBreaker(MultiMatchQueryBuilder mm) {
        return mm.tieBreaker() != null ? mm.tieBreaker() : mm.type().tieBreaker();
    }

    /**
     * Refuses a scored multi_match over several text columns whose score the
     * table cannot give, rather than leave its hits unranked: cross_fields,
     * which scores each word as if the fields were one, blending their term
     * statistics, and a tie_breaker between 0 and 1, which adds that share
     * of the other fields' scores to the best field's. The engine scores a
     * clause by its best column or sums the columns, nothing between.
     */
    private void refuseUnscorable(MultiMatchQueryBuilder mm) {
        if (textColumns(mm) < 2) {
            return;
        }
        if (mm.type() == MultiMatchQueryBuilder.Type.CROSS_FIELDS) {
            throw crossFieldsRefused(mm);
        }
        float tieBreaker = tieBreaker(mm);
        if (tieBreaker != 0f && tieBreaker != 1f) {
            throw new IllegalArgumentException(
                "[infino] multi_match on ["
                    + String.join(", ", mm.fields().keySet())
                    + "] with tie_breaker ["
                    + tieBreaker
                    + "] is not served: the table scores several text fields by the best one (tie_breaker 0) or by their sum (tie_breaker 1)"
            );
        }
    }

    /** A cross_fields multi_match over several text fields, refused: the table scores and matches each field apart. */
    private static IllegalArgumentException crossFieldsRefused(MultiMatchQueryBuilder mm) {
        return new IllegalArgumentException(
            "[infino] multi_match of type [cross_fields] on ["
                + String.join(", ", mm.fields().keySet())
                + "] is not served: the table matches and scores each text field apart, not as one field"
        );
    }

    /** How many distinct text columns the fields of {@code mm} name, a pattern ({@code *}) naming every one it matches. */
    private int textColumns(MultiMatchQueryBuilder mm) {
        List<String> text = new ArrayList<>();
        for (String f : mm.fields().keySet()) {
            Column c = resolve(f);
            if (c != null && c.fts() && !text.contains(c.name())) {
                text.add(c.name());
            } else if (c == null && org.opensearch.common.regex.Regex.isSimpleMatchPattern(f)) {
                for (Column each : cols.values()) {
                    if (each.fts() && !text.contains(each.name()) && org.opensearch.common.regex.Regex.simpleMatch(f, fieldOf(each.name()))) {
                        text.add(each.name());
                    }
                }
            }
        }
        return text.size();
    }

    /** How a query string's JSON names its default type, best_fields: the builder has no getter for it. */
    private static final Pattern BEST_FIELDS = Pattern.compile("\"type\"\\s*:\\s*\"best_fields\"");

    /**
     * The most BM25 searches (clauses times columns) a query string ranks
     * across before it is left unranked: the engine runs one search per
     * clause and column, so this bounds the work one query string asks for.
     */
    static final int MAX_ACROSS_SEARCHES = 64;

    /**
     * How stock ranks {@code qs} across its text columns ({@link Across}), or
     * null when the plugin does not rank it that way: it is on one column
     * (then it is the principal), or its form is outside what is read here.
     *
     * <p>Stock's parser (Lucene's, not splitting on whitespace) makes a
     * clause of each {@code field:word}, and one clause of each run of words
     * with no operator between them, on the default fields; a word before a
     * {@code field:} or an {@code OR} ends the run, and the word right after
     * an {@code OR} is a clause of its own, the run starting at the word
     * after it ({@code a OR b c d} is {@code a}, {@code b} and {@code c d}).
     * A field the mapping does not have adds nothing. Over several fields a
     * clause is a {@code dis_max} of each field's BM25 for its words
     * (best_fields, tie_breaker 0), and the clauses are summed. Read here:
     * plain words (one token each, no word twice in a run), {@code field:word}
     * on a text column and {@code OR}, under the default operator OR, every
     * mapped field a text column with boost 1, and no option that changes
     * the scoring.
     */
    private Across across(QueryStringQueryBuilder qs) {
        if (qs.defaultOperator() != Operator.OR
            || qs.minimumShouldMatch() != null
            || !BEST_FIELDS.matcher(qs.toString()).find()
            || (qs.tieBreaker() != null && qs.tieBreaker() != 0f)
            || qs.analyzer() != null
            || qs.boost() != 1f
            || qs.queryString() == null) {
            return null;
        }
        for (float b : qs.fields().values()) {
            if (b != 1f) {
                return null;
            }
        }
        // The predicate the query string is; one that set a principal is ranked by it.
        InfinoQueryTranslator alone = new InfinoQueryTranslator(cols, table, nowMillis, trusted, columnValuesOnly, rows);
        String match;
        try {
            match = alone.predicate(qs, true);
        } catch (IllegalArgumentException | OpenSearchSecurityException e) {
            return null;
        }
        if (match == null || alone.principal != null || ALL.equals(match) || NONE.equals(match)) {
            return null;
        }
        List<String> defaults = new ArrayList<>();
        try {
            for (String f : queryTargets(new ArrayList<>(qs.fields().keySet()), qs.defaultField())) {
                Column c = column(f);
                if (c == null && f.indexOf('*') < 0) {
                    continue; // stock's dis_max has no clause for a field the mapping does not have
                }
                if (c == null || !c.fts()) {
                    return null; // a pattern, or a column with no text index
                }
                if (!defaults.contains(c.name())) {
                    defaults.add(c.name());
                }
            }
        } catch (IllegalArgumentException | OpenSearchSecurityException e) {
            return null; // a field the table serves no column of, or one masked for the user
        }
        List<String> tokens = new ArrayList<>();
        for (String piece : QUERY_WHITESPACE.split(qs.queryString().trim())) {
            // A colon is a token of its own, with or without spaces around it.
            int start = 0;
            for (int i = 0; i < piece.length(); i++) {
                if (piece.charAt(i) == ':') {
                    if (i > start) {
                        tokens.add(piece.substring(start, i));
                    }
                    tokens.add(":");
                    start = i + 1;
                }
            }
            if (start < piece.length()) {
                tokens.add(piece.substring(start));
            }
        }
        List<List<TextClause>> clauses = new ArrayList<>();
        int searches = 0;
        boolean afterOr = true; // nothing may start or follow an OR but a clause
        for (int i = 0; i < tokens.size();) {
            String t = tokens.get(i);
            if ("OR".equals(t)) {
                if (afterOr) {
                    return null;
                }
                afterOr = true;
                i++;
                continue;
            }
            boolean single = afterOr && i > 0; // the token before this one is an OR
            afterOr = false;
            if (i + 1 < tokens.size() && ":".equals(tokens.get(i + 1))) {
                // field:word, one clause on that field's column.
                if (!fieldName(t) || i + 2 >= tokens.size() || !plainWord(tokens.get(i + 2))
                    || (i + 3 < tokens.size() && ":".equals(tokens.get(i + 3)))) {
                    return null;
                }
                Column c = column(t);
                if (c != null && (!c.fts() || (!trusted && c.masked()))) {
                    return null;
                }
                if (c != null) {
                    // A field the mapping does not have matches nothing and adds nothing.
                    clauses.add(List.of(new TextClause(c.name(), tokens.get(i + 2), "or")));
                    searches++;
                }
                i += 3;
                continue;
            }
            if (!plainWord(t)) {
                return null;
            }
            // A run: the words that follow while the word after each is no
            // field's colon or OR. The word right after an OR is alone.
            List<String> run = new ArrayList<>(List.of(t));
            int j = i + 1;
            while (!single && j < tokens.size() && plainWord(tokens.get(j))
                && (j + 1 >= tokens.size() || !(":".equals(tokens.get(j + 1)) || "OR".equals(tokens.get(j + 1))))) {
                run.add(tokens.get(j));
                j++;
            }
            if (run.stream().map(w -> w.toLowerCase(Locale.ROOT)).distinct().count() < run.size() || defaults.isEmpty()) {
                return null;
            }
            List<TextClause> perColumn = new ArrayList<>();
            for (String c : defaults) {
                perColumn.add(new TextClause(c, String.join(" ", run), "or"));
            }
            clauses.add(List.copyOf(perColumn));
            searches += perColumn.size();
            i = j;
        }
        if (afterOr || clauses.isEmpty() || searches < 2 || searches > MAX_ACROSS_SEARCHES) {
            // One search alone is a principal's ranking, which the translation would have set.
            return null;
        }
        return new Across(List.copyOf(clauses), match);
    }

    /**
     * A word both analyzers keep as one token and the query syntax reads as a
     * word: ASCII letters and digits only (the engine's segmenter splits some
     * other scripts where Lucene's does not), and no operator.
     */
    private static boolean plainWord(String w) {
        String lower = w.toLowerCase(Locale.ROOT);
        return !w.isEmpty()
            && w.length() <= 255
            && w.chars().allMatch(c -> (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9'))
            && !lower.equals("and")
            && !lower.equals("or")
            && !lower.equals("not")
            && !lower.equals("to");
    }

    /** A field name as a query string names one: no pattern, no escape. */
    private static boolean fieldName(String f) {
        return !f.isEmpty() && f.codePoints().allMatch(c -> Character.isLetterOrDigit(c) || c == '.' || c == '_' || c == '-');
    }

    /**
     * A should clause of a bool every hit matches: it adds its score to the
     * hits it matches. Beside a must or filter clause it does not narrow the
     * hits, so it was never read as a predicate: one the table cannot read
     * (or that names a field masked for the user, whose values a score would
     * tell) is left unscored, and the query is served as before.
     */
    private boolean optional(QueryBuilder qb, List<Bonus> bonuses, List<TextClause> texts) {
        try {
            return scoreOptional(qb, bonuses, texts);
        } catch (IllegalArgumentException | OpenSearchSecurityException e) {
            return false;
        }
    }

    /** Scores a should clause; false when the table cannot score it as stock does. */
    private boolean scoreOptional(QueryBuilder qb, List<Bonus> bonuses, List<TextClause> texts) {
        Double w = constantScore(qb);
        if (w != null) {
            String p = predicate(qb, false);
            if (p == null) {
                return false;
            }
            if (joinsAfterRanking(p)) {
                // The rows it matches, read in a filter and joined to the hits by id.
                String alias = "infino_match" + bonuses.size();
                String join = " LEFT JOIN (SELECT " + OSID + " AS infino_hit FROM " + quoteIdent(table) + " WHERE " + p + ") AS " + alias + " ON " + OSID
                    + " = " + alias + ".infino_hit";
                bonuses.add(new Bonus("CASE WHEN " + alias + ".infino_hit IS NOT NULL THEN " + scoreLiteral(w) + " ELSE 0.0 END", List.of(w), join));
            } else if (!NONE.equals(p)) {
                bonuses.add(new Bonus("CASE WHEN " + p + " THEN " + scoreLiteral(w) + " ELSE 0.0 END", List.of(w)));
            }
            return true;
        }
        if (qb instanceof NestedQueryBuilder n) {
            if (entriesScoredApart(n.query())) {
                return false;
            }
            Bonus nb = nestedBonus(n);
            if (nb != null) {
                bonuses.add(nb);
            }
            return true;
        }
        TextClause t = textOnly(qb);
        if (t == null) {
            return false;
        }
        texts.add(t);
        return true;
    }

    /**
     * The score Lucene gives every document a clause matches, or null when it
     * scores documents apart: a terms, range, exists, ids or match_all query scores its
     * boost (a constant-score query in Lucene), as do a term, prefix,
     * wildcard or regexp query on a field other than a text one, and a match
     * or match_phrase on one, which is a term query there. A term, match or
     * match_phrase on a boolean field, or on a keyword field with
     * {@code use_similarity}, Lucene scores by BM25 instead: the same score
     * on every document it matches ({@link #bm25TermScore}) while each
     * document gives the field one value, and apart (null) once some give
     * several.
     */
    private Double constantScore(QueryBuilder qb) {
        if (qb instanceof TermsQueryBuilder
            || qb instanceof RangeQueryBuilder
            || qb instanceof ExistsQueryBuilder
            || qb instanceof IdsQueryBuilder
            || qb instanceof MatchAllQueryBuilder
            || qb instanceof BoolQueryBuilder b && !b.hasClauses()) {
            // A bool with no clause is a match_all in stock, which scores its boost.
            return (double) qb.boost();
        }
        String field = null;
        if (qb instanceof TermQueryBuilder t) {
            field = t.fieldName();
        } else if (qb instanceof MatchQueryBuilder m) {
            field = m.fieldName();
        } else if (qb instanceof MatchPhraseQueryBuilder mp) {
            field = mp.fieldName();
        } else if (qb instanceof PrefixQueryBuilder px) {
            field = px.fieldName();
        } else if (qb instanceof WildcardQueryBuilder w) {
            field = w.fieldName();
        } else if (qb instanceof RegexpQueryBuilder re) {
            field = re.fieldName();
        }
        if (field == null) {
            return null;
        }
        Column c = resolve(field);
        if (c != null && c.fts()) {
            return null;
        }
        if (c != null && c.scoresTermsByBm25() && isTermQuery(qb)) {
            return c.isList() || listOf(cols, c) != null ? null : bm25TermScore(qb, c);
        }
        return (double) qb.boost();
    }

    /** Whether Lucene reads {@code qb} on a field other than a text one as one term query. */
    private static boolean isTermQuery(QueryBuilder qb) {
        return qb instanceof TermQueryBuilder || qb instanceof MatchQueryBuilder || qb instanceof MatchPhraseQueryBuilder;
    }

    /** BM25's {@code k1} and {@code b}, Lucene's defaults, which an index keeps unless its similarity says otherwise. */
    private static final float BM25_K1 = 1.2f;
    private static final float BM25_B = 0.75f;

    /**
     * What Lucene's BM25 gives each document a term on {@code c} matches,
     * when every document gives the field one value. The field has no
     * norms, so every document's length is 1, as is the average, and the
     * score is {@code boost * idf * 1 / (1 + k1)}, with
     * {@code idf = ln(1 + (N - n + 0.5) / (n + 0.5))}: {@code N} the documents
     * holding the field, {@code n} those the term matches. Both are counted
     * over the whole table, as Lucene reads them from the shard's index
     * whatever a document-level rule hides. Computed in floats, as Lucene does.
     */
    private Double bm25TermScore(QueryBuilder qb, Column c) {
        Double known = termScores.get(qb);
        if (known != null) {
            return known;
        }
        String p = predicate(qb, false);
        if (p == null) {
            return null;
        }
        double score = qb.boost();
        if (!NONE.equals(p)) {
            List<Map<String, Object>> counted = rows.apply(
                "SELECT COUNT(" + quoteIdent(c.name()) + ") AS infino_docs, SUM(CASE WHEN " + p + " THEN 1 ELSE 0 END) AS infino_hits FROM "
                    + quoteIdent(table)
            );
            long docs = counted.isEmpty() ? 0 : asLong(counted.get(0).get("infino_docs"));
            long hits = counted.isEmpty() ? 0 : asLong(counted.get(0).get("infino_hits"));
            if (hits > 0) {
                float idf = (float) Math.log(1 + (docs - hits + 0.5D) / (hits + 0.5D));
                float weight = qb.boost() * idf;
                float normInverse = 1f / (BM25_K1 * ((1 - BM25_B) + BM25_B));
                score = weight - weight / (1f + normInverse);
            }
        }
        termScores.put(qb, score);
        return score;
    }

    private static long asLong(Object v) {
        return v instanceof Number n ? n.longValue() : 0;
    }

    /**
     * Whether the entries of a nested query score apart, as the table cannot
     * score them: a term on a boolean field or a keyword field with
     * {@code use_similarity} in a scoring place (a must or should clause, at
     * any depth), which Lucene scores by BM25 over the nested documents.
     */
    private boolean entriesScoredApart(QueryBuilder qb) {
        if (qb instanceof BoolQueryBuilder b) {
            for (QueryBuilder c : b.must()) {
                if (entriesScoredApart(c)) {
                    return true;
                }
            }
            for (QueryBuilder c : b.should()) {
                if (entriesScoredApart(c)) {
                    return true;
                }
            }
            return false;
        }
        if (qb instanceof NestedQueryBuilder n) {
            return entriesScoredApart(n.query());
        }
        String field = qb instanceof TermQueryBuilder t ? t.fieldName()
            : qb instanceof MatchQueryBuilder m ? m.fieldName()
            : qb instanceof MatchPhraseQueryBuilder mp ? mp.fieldName()
            : null;
        Column c = field == null ? null : resolve(field);
        return c != null && c.scoresTermsByBm25();
    }

    /**
     * The text clause {@code qb} is when it is one alone (a match or a
     * match_phrase on a text field), as it would rank: null for any other clause.
     */
    private TextClause textOnly(QueryBuilder qb) {
        if (!(qb instanceof MatchQueryBuilder || qb instanceof MatchPhraseQueryBuilder)) {
            return null;
        }
        InfinoQueryTranslator alone = new InfinoQueryTranslator(cols, table, nowMillis, trusted, columnValuesOnly, rows);
        String p = alone.predicate(qb, true);
        return ALL.equals(p) ? alone.principal : null;
    }

    /**
     * What a nested clause adds to the documents it matches: their entries'
     * scores read by its score_mode ({@link #nestedScores}), by id. Null when
     * it matches none, or scores them all 0.
     */
    private Bonus nestedBonus(NestedQueryBuilder n) {
        Map<String, Double> scores = nestedScores(n);
        Map<Double, List<String>> byScore = new java.util.TreeMap<>();
        for (Map.Entry<String, Double> e : scores.entrySet()) {
            if (e.getValue() != 0) {
                byScore.computeIfAbsent(e.getValue(), k -> new ArrayList<>()).add(literal(e.getKey()));
            }
        }
        if (byScore.isEmpty()) {
            return null;
        }
        StringBuilder expr = new StringBuilder("CASE");
        for (Map.Entry<Double, List<String>> e : byScore.entrySet()) {
            expr.append(" WHEN ").append(OSID).append(" IN (").append(String.join(", ", e.getValue())).append(") THEN ").append(scoreLiteral(e.getKey()));
        }
        expr.append(" ELSE 0.0 END");
        return new Bonus(expr.toString(), List.copyOf(byScore.keySet()));
    }

    /** A score as a SQL number: plain digits, never an exponent. */
    static String scoreLiteral(double v) {
        return BigDecimal.valueOf(v).toPlainString();
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
            return parser.parse(text, InfinoClock.of(nowMillis), roundUp, zone).toEpochMilli();
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
        refuseColumnless(field);
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

    /**
     * The field a column holds: its own name, a list column's field ({@link #LIST_SUFFIX}), or the field named like
     * a reserved column ({@link #OWN_COLUMN_PREFIX}).
     */
    static String fieldOf(String column) {
        String field = column.endsWith(LIST_SUFFIX) ? column.substring(0, column.length() - LIST_SUFFIX.length()) : column;
        return field.startsWith(OWN_COLUMN_PREFIX) ? field.substring(OWN_COLUMN_PREFIX.length()) : field;
    }

    /** The name a column is looked up by: its field's, except a list column's, which is its own. */
    static String keyOf(String column) {
        return column.endsWith(LIST_SUFFIX) ? column : fieldOf(column);
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
        String meta = metadataExists(field);
        if (meta != null) {
            return meta;
        }
        // Whether a field has a value tells nothing of a masked one's: its hash is shown.
        Column c = resolve(field);
        if (c != null) {
            Column values = listOf(cols, c);
            String own = quoteIdent(c.name()) + " IS NOT NULL";
            return values == null ? own : "(" + own + " OR " + quoteIdent(values.name()) + " IS NOT NULL)";
        }
        refuseColumnless(field);
        String under = field + ".";
        // OpenSearch's own field-pattern matcher, named in full: this class's Regex is a query node.
        boolean pattern = org.opensearch.common.regex.Regex.isSimpleMatchPattern(field);
        List<String> present = new ArrayList<>();
        for (Column sub : cols.values()) {
            String name = sub.name();
            if (name.equals(OSID) || name.equals(SOURCE) || name.equals(SEQ) || name.equals(IGNORED)) {
                continue;
            }
            String held = fieldOf(name);
            if (held.startsWith(under) || (pattern && org.opensearch.common.regex.Regex.simpleMatch(field, held))) {
                present.add(quoteIdent(name) + " IS NOT NULL");
            }
        }
        return present.isEmpty() ? NONE : "(" + String.join(" OR ", present) + ")";
    }

    /**
     * The rows an exists on a metadata field matches, as stock's: every
     * document has an {@code _id}, {@code _index}, {@code _seq_no} and
     * {@code _version}; {@code _routing} is the documents written with a
     * routing value, which only a default-mode table keeps; {@code _ignored}
     * is the documents with a field the mapper ignored. {@code _source}
     * cannot be searched, and stock says so. Null for any other field.
     */
    private String metadataExists(String field) {
        switch (field) {
            case "_id", "_index", "_seq_no", "_version" -> {
                return ALL;
            }
            case "_routing" -> {
                if (cols.get(ROUTING) == null) {
                    throw new IllegalArgumentException("[infino] exists on [_routing] is not served: the table keeps no routing values");
                }
                return quoteIdent(ROUTING) + " IS NOT NULL";
            }
            case IGNORED_FIELD -> {
                return ignoredColumn(cols) == null ? NONE : quoteIdent(IGNORED) + " IS NOT NULL";
            }
            case "_source" -> throw new QueryShardException((Index) null, "The _source field is not searchable", (Throwable) null);
            default -> {
                return null;
            }
        }
    }

    /**
     * Refuses a query on a field the index maps but the table keeps no
     * column of (a multi-field of another type or with a normalizer), or
     * whose values the mapping normalizes: answered over the raw values or
     * as unmapped, it would be a wrong answer.
     */
    private void refuseColumnless(String field) {
        String why = InfinoNative.unservedField(table, field);
        if (why != null) {
            throw new IllegalArgumentException("[infino] a query on [" + field + "], " + why + ", is not served");
        }
    }

    /**
     * Whether a predicate holds a subquery by id (a text clause on a column
     * other than the ranked one, or a field's list values) or a text_match
     * on such a column: one the engine applies above a ranked search, so the
     * rows it removes go after the ranking. A string literal that happens to
     * hold the same text only costs that search a wider candidate window.
     * The summed search of several text clauses ({@link Across}) takes a
     * text_match into the rows it ranks; only {@link #joinsById} falls after it.
     */
    static boolean joinsAfterRanking(String predicate) {
        return joinsById(predicate) || predicate.contains(TEXT_PREDICATE);
    }

    /**
     * Whether a predicate holds a subquery by id (a text clause the token
     * index answers, or a field's list values), which the engine plans as a
     * join that reads the whole table before it returns a row, and joins to
     * the rows above any search it ranks. A string literal that happens to
     * hold the same text only costs a page an order by row id.
     */
    static boolean joinsById(String predicate) {
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
            if (t.caseInsensitive() && c != null && t.value() != null) {
                String lit = literal(String.valueOf(t.value()));
                return anyValue(caseless(c, "term"), v -> "lower(" + v + ") = lower(" + lit + ")");
            }
            return equality(t.fieldName(), t.value());
        }
        if (qb instanceof TermsQueryBuilder ts) {
            refuseBitmap(ts);
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
            if (p.caseInsensitive() && c != null) {
                return caselessLike(caseless(c, "prefix"), escapeLike(p.value()) + "%");
            }
            return like(p.fieldName(), escapeLike(p.value()) + "%");
        }
        if (qb instanceof WildcardQueryBuilder w) {
            Column c = column(w.fieldName());
            if (c != null && c.fts()) {
                return termPattern(c, wildcardToRegex(w.value(), true), w.value().endsWith("*") && !w.value().endsWith("\\*"));
            }
            // A backslash escapes the character after it, as Lucene's wildcard reads it.
            String pattern = escapedWildcardToLike(w.value());
            if (w.caseInsensitive() && c != null) {
                return caselessLike(caseless(c, "wildcard"), pattern);
            }
            return like(w.fieldName(), pattern);
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
            String flags = re.caseInsensitive() ? ", 'i'" : "";
            return anyValue(c, v -> "regexp_like(" + v + ", " + anchored + flags + ")");
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
            unfuzzy("match", m.fieldName(), m.fuzziness());
            return text(m.fieldName(), String.valueOf(m.value()), m.operator(), positive);
        }
        if (qb instanceof MatchPhraseQueryBuilder mp) {
            // A quoted query is a phrase to the engine: the terms adjacent, in order.
            return text(mp.fieldName(), String.valueOf(mp.value()), Operator.AND, positive, true);
        }
        if (qb instanceof MultiMatchQueryBuilder mm) {
            if (mm.type() == MultiMatchQueryBuilder.Type.BOOL_PREFIX || mm.type() == MultiMatchQueryBuilder.Type.PHRASE_PREFIX) {
                // Their last term is a prefix, which the table's text match does not read; as
                // match_bool_prefix and match_phrase_prefix, they are refused, not answered as a match.
                throw new IllegalArgumentException(
                    "[infino] multi_match of type [" + mm.type().parseField().getPreferredName() + "] is not served"
                );
            }
            unfuzzy("multi_match", String.join(", ", mm.fields().keySet()), mm.fuzziness());
            if (mm.type() == MultiMatchQueryBuilder.Type.CROSS_FIELDS && mm.operator() == Operator.AND && textColumns(mm) >= 2) {
                // Stock needs each word in some field; each field matching all of them keeps fewer rows.
                throw crossFieldsRefused(mm);
            }
            boolean asPhrase = mm.type() == MultiMatchQueryBuilder.Type.PHRASE;
            return textOnFields(new ArrayList<>(mm.fields().keySet()), String.valueOf(mm.value()), mm.operator(), positive, asPhrase, true);
        }
        if (qb instanceof QueryStringQueryBuilder qs) {
            return queryString(qs.queryString(), qs.defaultField(), new ArrayList<>(qs.fields().keySet()), qs.defaultOperator(), qs.minimumShouldMatch(), false, positive);
        }
        if (qb instanceof SimpleQueryStringBuilder sqs) {
            return queryString(sqs.value(), null, new ArrayList<>(sqs.fields().keySet()), sqs.defaultOperator(), sqs.minimumShouldMatch(), true, positive);
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
            filtering++;
            try {
                and.add(predicate(c, positive));
            } finally {
                filtering--;
            }
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
     * by entry ({@link #entryScore}), and the clause becomes the ids of the
     * documents that hold a matching entry. A clause the check cannot read
     * refuses the query with the clause named, as does a narrowing that
     * leaves more than {@link #NESTED_CANDIDATES_MAX} documents: never an
     * answer read over the flattened values.
     */
    private String nested(NestedQueryBuilder n) {
        Map<String, Double> scores = nestedScores(n);
        if (scores.isEmpty()) {
            return NONE;
        }
        List<String> ids = new ArrayList<>();
        for (String id : scores.keySet()) {
            ids.add(literal(id));
        }
        return OSID + " IN (" + String.join(", ", ids) + ")";
    }

    /**
     * The documents a nested query matches ({@link #nested}), each with its
     * score as Lucene gives it: the scores of its matching entries
     * ({@link #entryScore}) read by the query's score_mode (their average by
     * default, their sum, the greatest, the least, or 0 for none), times the
     * query's boost. Read once per clause.
     */
    private Map<String, Double> nestedScores(NestedQueryBuilder n) {
        Map<String, Double> known = nestedScores.get(n);
        if (known != null) {
            return known;
        }
        String path = n.path();
        if (n.innerHit() != null) {
            throw new IllegalArgumentException("[infino] inner_hits on the nested query on [" + path + "] is not served by the table");
        }
        EntryScore score = entryScore(n.query(), path, path);
        // An entry with no value at all matches only a query that needs none
        // (match_all, a must_not); then no column narrows the documents, and
        // each one's entries are read from its source alone.
        boolean emptyEntryMatches = !Double.isNaN(score.of(Map.of()));
        String present = exists(path);
        Map<String, Double> matched = new java.util.LinkedHashMap<>();
        if (NONE.equals(present) && !emptyEntryMatches) {
            nestedScores.put(n, matched);
            return matched;
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
            double s = combined(n.scoreMode(), entries, score);
            if (!Double.isNaN(s)) {
                matched.put(String.valueOf(id), s * n.boost());
            }
        }
        nestedScores.put(n, matched);
        return matched;
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
     * The score of one entry under a nested query, as Lucene scores the
     * nested document: NaN when the query does not hold on it.
     */
    @FunctionalInterface
    private interface EntryScore {
        double of(Map<String, Object> entry);
    }

    /**
     * The scores of the {@code entries} that match read by {@code mode}, as a
     * nested query reads them for their document: NaN when none matches.
     */
    private static double combined(ScoreMode mode, List<Map<String, Object>> entries, EntryScore score) {
        int matched = 0;
        double sum = 0;
        double max = Double.NEGATIVE_INFINITY;
        double min = Double.POSITIVE_INFINITY;
        for (Map<String, Object> entry : entries) {
            double s = score.of(entry);
            if (Double.isNaN(s)) {
                continue;
            }
            matched++;
            sum += s;
            max = Math.max(max, s);
            min = Math.min(min, s);
        }
        if (matched == 0) {
            return Double.NaN;
        }
        return switch (mode) {
            case None -> 0;
            case Total -> sum;
            case Max -> max;
            case Min -> min;
            case Avg -> sum / matched;
        };
    }

    /** {@code test} as an entry score: {@code boost} where it holds, NaN where it does not. */
    private static EntryScore scoring(Predicate<Map<String, Object>> test, float boost) {
        return e -> test.test(e) ? boost : Double.NaN;
    }

    /**
     * How an entry of the nested query on {@code path} scores under
     * {@code qb}, its fields read from the entry, which sits at {@code scope}
     * (the path itself, or a deeper nested query's): NaN where {@code qb}
     * does not hold on it. A term, terms, match, range, exists or match_all
     * scores its boost, as their queries score on a keyword, numeric or
     * boolean field in Lucene; a bool the sum of its must clauses' scores and
     * its matching should clauses', times its boost (a filter or must_not
     * scores nothing); a deeper nested query its entries' scores by its
     * score_mode, times its boost. A clause on a field outside the scope
     * matches nothing, as in stock, whose nested documents hold only their
     * own fields; a field the table has no column for (unmapped, or not
     * visible to the request) matches nothing either. Throws naming the
     * clause the check cannot read.
     */
    private EntryScore entryScore(QueryBuilder qb, String path, String scope) {
        if (qb == null) {
            return e -> 1.0;
        }
        if (qb instanceof MatchAllQueryBuilder) {
            return scoring(e -> true, qb.boost());
        }
        if (qb instanceof MatchNoneQueryBuilder) {
            return e -> Double.NaN;
        }
        if (qb instanceof TermQueryBuilder t) {
            Object q = t.value();
            return scoring(entryValue(t.fieldName(), qb, path, scope, (c, v) -> sameValue(c, v, q)), qb.boost());
        }
        if (qb instanceof TermsQueryBuilder ts) {
            refuseBitmap(ts);
            if (ts.termsLookup() != null) {
                throw notInNested(qb, path, "a terms lookup");
            }
            List<Object> qs = ts.values() == null ? List.of() : ts.values();
            return scoring(entryValue(ts.fieldName(), qb, path, scope, (c, v) -> {
                for (Object q : qs) {
                    if (sameValue(c, v, q)) {
                        return true;
                    }
                }
                return false;
            }), qb.boost());
        }
        if (qb instanceof MatchQueryBuilder m) {
            // A match on an exact field is an equality, as the translator reads it elsewhere.
            unfuzzy("match", m.fieldName(), m.fuzziness());
            Object q = m.value();
            return scoring(entryValue(m.fieldName(), qb, path, scope, (c, v) -> sameValue(c, v, q)), qb.boost());
        }
        if (qb instanceof RangeQueryBuilder r) {
            return scoring(entryValue(r.fieldName(), qb, path, scope, (c, v) -> inRange(c, v, r)), qb.boost());
        }
        if (qb instanceof ExistsQueryBuilder e) {
            return scoring(entryExists(e, path, scope), qb.boost());
        }
        if (qb instanceof BoolQueryBuilder b && !b.hasClauses()) {
            // Stock reads a bool with no clause as a match_all: every entry scores its boost.
            return scoring(e -> true, qb.boost());
        }
        if (qb instanceof BoolQueryBuilder b) {
            List<EntryScore> scored = new ArrayList<>();
            for (QueryBuilder c : b.must()) {
                scored.add(entryScore(c, path, scope));
            }
            List<EntryScore> filters = new ArrayList<>();
            for (QueryBuilder c : b.filter()) {
                filters.add(entryScore(c, path, scope));
            }
            List<EntryScore> optional = new ArrayList<>();
            for (QueryBuilder c : b.should()) {
                optional.add(entryScore(c, path, scope));
            }
            List<EntryScore> prohibited = new ArrayList<>();
            for (QueryBuilder c : b.mustNot()) {
                prohibited.add(entryScore(c, path, scope));
            }
            boolean nothingRequired = scored.isEmpty() && filters.isEmpty();
            int atLeast = optional.isEmpty() ? 0 : Math.min(minimumShouldMatch(b.minimumShouldMatch(), optional.size(), nothingRequired), optional.size());
            float boost = b.boost();
            return e -> {
                double sum = 0;
                for (EntryScore s : scored) {
                    double v = s.of(e);
                    if (Double.isNaN(v)) {
                        return Double.NaN;
                    }
                    sum += v;
                }
                for (EntryScore s : filters) {
                    if (Double.isNaN(s.of(e))) {
                        return Double.NaN;
                    }
                }
                for (EntryScore s : prohibited) {
                    if (!Double.isNaN(s.of(e))) {
                        return Double.NaN;
                    }
                }
                int matched = 0;
                for (EntryScore s : optional) {
                    double v = s.of(e);
                    if (!Double.isNaN(v)) {
                        matched++;
                        sum += v;
                    }
                }
                return matched >= atLeast ? sum * boost : Double.NaN;
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
            EntryScore score = entryScore(inner.query(), path, inner.path());
            ScoreMode mode = inner.scoreMode();
            float boost = inner.boost();
            return e -> combined(mode, entries(e, sub), score) * boost;
        }
        throw notInNested(qb, path, null);
    }

    /** Refuse a terms query whose one value is a serialized bitmap of the terms, which the table does not decode. */
    private static void refuseBitmap(TermsQueryBuilder ts) {
        if (ts.valueType() == TermsQueryBuilder.ValueType.BITMAP) {
            throw new IllegalArgumentException("[infino] a terms query on [" + ts.fieldName() + "] with value_type [bitmap] is not served by the table");
        }
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
        if (isCidr(c, q)) {
            return inBlock(v.toString(), q.toString());
        }
        if (c.isString()) {
            return v.toString().equals(q.toString());
        }
        if (c.isNumeric() && c.fieldType() != null && c.fieldType().rounds()) {
            Double a = kept(c, v);
            Double b = kept(c, q);
            return a != null && b != null && a.doubleValue() == b.doubleValue();
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
        if (c.isNumeric() && c.fieldType() != null && c.fieldType().rounds()) {
            Double x = kept(c, v);
            return x != null
                && (r.from() == null || x >= c.fieldType().lower(r.from(), r.includeLower()))
                && (r.to() == null || x <= c.fieldType().upper(r.to(), r.includeUpper()));
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

    /** A source value as a float, half_float or scaled_float field keeps it; null for one that is not a number. */
    private static Double kept(Column c, Object v) {
        if (v instanceof Map<?, ?> || v instanceof List<?> || v instanceof Boolean) {
            return null;
        }
        try {
            return c.fieldType().kept(v);
        } catch (IllegalArgumentException e) {
            return null;
        }
    }

    /** Whether {@code value}, in a term on {@code c}, is a CIDR block: on an ip field, text with a prefix length. */
    private static boolean isCidr(Column c, Object value) {
        InfinoNative.FieldType type = c.element().fieldType();
        return type != null && type.isIp() && value != null && value.toString().indexOf('/') >= 0;
    }

    /**
     * The test a CIDR block in a term on the ip field {@code field} makes of
     * a value of its column, which holds each address as written: whether
     * it lies in the block, as stock reads such a term (the addresses the
     * block covers, its host bits ignored). An IPv4 address is written as
     * four decimal octets, so the block is a prefix of that text: its whole
     * octets as written, then a partial octet as the values it allows. An
     * IPv6 block is refused: an IPv6 address has several textual forms
     * (zeros compressed or not, an embedded IPv4 tail), so no pattern over
     * the text finds every address in the block.
     */
    static Function<String, String> cidr(String field, String block) {
        // A malformed block is refused with stock's message.
        Tuple<InetAddress, Integer> parsed = InetAddresses.parseCidr(block);
        if (!(parsed.v1() instanceof Inet4Address)) {
            throw new IllegalArgumentException("[infino] a term with the IPv6 CIDR block [" + block + "] on [" + field + "] is not served");
        }
        byte[] address = parsed.v1().getAddress();
        int bits = parsed.v2();
        if (bits == 0) {
            return v -> v + " NOT LIKE '%:%'";
        }
        StringBuilder whole = new StringBuilder();
        for (int i = 0; i < bits / 8; i++) {
            whole.append(address[i] & 0xff).append('.');
        }
        if (bits == 32) {
            String lit = literal(whole.substring(0, whole.length() - 1));
            return v -> v + " = " + lit;
        }
        if (bits % 8 == 0) {
            String lit = literal(whole + "%");
            return v -> v + " LIKE " + lit;
        }
        int octet = bits / 8;
        int free = 8 - bits % 8;
        int first = (address[octet] & 0xff) >> free << free;
        List<String> allowed = new ArrayList<>(1 << free);
        for (int i = 0; i < 1 << free; i++) {
            allowed.add(Integer.toString(first + i));
        }
        String pattern = "^" + whole.toString().replace(".", "\\.") + "(?:" + String.join("|", allowed) + ")" + (octet < 3 ? "\\." : "$");
        String lit = literal(pattern);
        return v -> "regexp_like(" + v + ", " + lit + ")";
    }

    /** Whether the address {@code address} lies in the CIDR block {@code block}; false for text that is not an address. */
    private static boolean inBlock(String address, String block) {
        Tuple<InetAddress, Integer> parsed = InetAddresses.parseCidr(block);
        byte[] net = parsed.v1().getAddress();
        byte[] ip;
        try {
            ip = InetAddresses.forString(address).getAddress();
        } catch (IllegalArgumentException e) {
            return false;
        }
        if (ip.length != net.length) {
            return false;
        }
        int bits = parsed.v2();
        for (int i = 0; i < net.length && bits > 0; i++, bits -= 8) {
            int mask = bits >= 8 ? 0xff : 0xff << (8 - bits) & 0xff;
            if ((ip[i] & mask) != (net[i] & mask)) {
                return false;
            }
        }
        return true;
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
        // A clause in filter context scores nothing: it may still be the
        // principal the rows are read through, unranked, unless another text
        // clause scores the hits.
        boolean scores = filtering == 0;
        if (positive && principal == null && (scores || !filterTextIsPredicate)) {
            principal = new TextClause(column, query, mode);
            principalRanks = scores;
            return ALL;
        }
        if (positive
            && principal != null
            && principalRanks == scores
            && principal.column().equals(column)
            && "and".equals(mode)
            && "and".equals(principal.mode())) {
            // A second required clause on the same column joins the principal, so
            // a ranked one's score reflects both; under all-terms mode the join
            // reads the same. A clause scoring otherwise than the principal stays apart.
            principal = new TextClause(column, principal.query() + " " + query, mode);
            return ALL;
        }
        return textPredicate(table, new TextClause(column, query, mode));
    }

    /**
     * Whether {@code query} is plain words, which {@code text_match} takes: no
     * phrase (a double quote) and no run the engine reads as a required or an
     * excluded clause (a leading {@code +} or {@code -}). Those keep the
     * {@code token_match} subquery, which reads them.
     */
    static boolean plainQuery(String query) {
        if (query.indexOf('"') >= 0) {
            return false;
        }
        for (String run : QUERY_WHITESPACE.split(query)) {
            if (!run.isEmpty() && (run.charAt(0) == '+' || run.charAt(0) == '-')) {
                return false;
            }
        }
        return true;
    }

    /**
     * Whether {@code predicate} reads the token index through a subquery,
     * which the engine plans in a WHERE clause but not inside an aggregate's
     * expression.
     */
    static boolean readsTextIndex(String predicate) {
        // text_match is a predicate the scan answers from the index; inside an
        // aggregate's expression it would be evaluated row by row instead.
        return predicate.contains(TEXT_SUBQUERY) || predicate.contains(TEXT_PREDICATE);
    }

    /** One {@code text_match} call exactly as {@link #textPredicate} writes it: column, plain words, mode. */
    private static final Pattern TEXT_MATCH_CALL = Pattern.compile("text_match\\(\"((?:[^\"]|\"\")*)\", '((?:[^']|'')*)', '(and|or)'\\)");
    /** The SQL keywords whose own AND would be read as a conjunction by {@link #junction}. */
    private static final Pattern AMBIGUOUS_AND = Pattern.compile("(?i)\\b(BETWEEN|CASE)\\b");

    /**
     * The statement that counts the rows of {@code table} matching {@code where}
     * up to {@code cap} from the token index, or null when {@code where} is not
     * filters joined by AND to one OR of plain-words {@code text_match} calls.
     * Each column's clauses become one {@code token_match} part (the words of
     * its any-word clauses, and of its one-word all-words clauses, merged into
     * one any-word match; another all-words clause a part of its own), each
     * under the filters. A row counts in the first part that holds it only:
     * every later part leaves out the rows an earlier part's clause matches, so
     * the parts are disjoint and their rows add up with no set of ids to keep,
     * and the cap stops the count. The scan the predicate would run instead
     * reads the rows the filters keep and matches each against every clause.
     */
    static String textCountStatement(String table, String where, long cap) {
        List<String> conjuncts = new ArrayList<>();
        if (!flatten(where, "AND", conjuncts)) {
            return null;
        }
        List<String> filters = new ArrayList<>();
        String text = null;
        for (String c : conjuncts) {
            if (!readsTextIndex(c) && !c.contains("token_match(") && !c.toUpperCase(Locale.ROOT).contains("SELECT")) {
                filters.add("(" + c + ")");
            } else if (text == null) {
                text = c;
            } else {
                return null;
            }
        }
        List<String> calls = new ArrayList<>();
        if (text == null || !flatten(text, "OR", calls)) {
            return null;
        }
        // Per column: the words any of which a row may hold, then each clause needing all of its words.
        Map<String, List<String>> anyWords = new LinkedHashMap<>();
        Map<String, List<String>> allWords = new LinkedHashMap<>();
        for (String call : calls) {
            java.util.regex.Matcher m = TEXT_MATCH_CALL.matcher(call);
            if (!m.matches()) {
                return null;
            }
            String column = m.group(1).replace("\"\"", "\"");
            String words = m.group(2).replace("''", "'");
            if (!plainQuery(words)) {
                return null;
            }
            boolean any = "or".equals(m.group(3)) || oneToken(words);
            (any ? anyWords : allWords).computeIfAbsent(column, k -> new ArrayList<>()).add(words);
            (any ? allWords : anyWords).computeIfAbsent(column, k -> new ArrayList<>());
        }
        List<TextClause> parts = new ArrayList<>();
        for (Map.Entry<String, List<String>> e : anyWords.entrySet()) {
            if (!e.getValue().isEmpty()) {
                parts.add(new TextClause(e.getKey(), String.join(" ", e.getValue()), "or"));
            }
            for (String words : allWords.get(e.getKey())) {
                parts.add(new TextClause(e.getKey(), words, "and"));
            }
        }
        List<String> selects = new ArrayList<>();
        for (int i = 0; i < parts.size(); i++) {
            TextClause part = parts.get(i);
            List<String> conditions = new ArrayList<>(filters);
            for (TextClause earlier : parts.subList(0, i)) {
                // A row an earlier part holds; a NULL there holds no word.
                conditions.add(not(textPredicate(table, earlier)));
            }
            selects.add(
                "SELECT 1 AS one FROM token_match("
                    + literal(table)
                    + ", "
                    + literal(part.column())
                    + ", "
                    + literal(part.query())
                    + ", "
                    + literal(part.mode())
                    + ")"
                    + (conditions.isEmpty() ? "" : " WHERE " + String.join(" AND ", conditions))
            );
        }
        if (selects.size() == 1) {
            return "SELECT COUNT(*) AS c FROM (" + selects.get(0) + " LIMIT " + cap + ") capped";
        }
        return "SELECT COUNT(*) AS c FROM (SELECT one FROM (" + String.join(" UNION ALL ", selects) + ") matched LIMIT " + cap + ") capped";
    }

    /**
     * Adds to {@code out} the parts {@code expr} joins with {@code op} at any
     * depth of parentheses holding that operator only, each with its outer
     * parentheses removed; false when a level mixes AND with OR, or holds the
     * other operator where {@code op} is asked for (an OR inside the ANDs is one
     * part: it is the OR flattened next), or a keyword whose AND is its own.
     */
    private static boolean flatten(String expr, String op, List<String> out) {
        String s = unwrap(expr.trim());
        List<String> parts = new ArrayList<>();
        String found = junction(s, parts);
        if (found == null) {
            return false;
        }
        if (found.isEmpty() || (!found.equals(op) && "AND".equals(op))) {
            if (found.isEmpty() && AMBIGUOUS_AND.matcher(s).find()) {
                return false;
            }
            out.add(s);
            return true;
        }
        if (!found.equals(op)) {
            return false;
        }
        for (String p : parts) {
            if (!flatten(p, op, out)) {
                return false;
            }
        }
        return true;
    }

    /** {@code s} without parentheses enclosing the whole of it. */
    private static String unwrap(String s) {
        while (s.length() > 1 && s.charAt(0) == '(' && closing(s, 0) == s.length() - 1) {
            s = s.substring(1, s.length() - 1).trim();
        }
        return s;
    }

    /** The index of the parenthesis closing the one at {@code open}, outside quotes; -1 when none. */
    private static int closing(String s, int open) {
        int depth = 0;
        for (int i = open; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == '\'' || c == '"') {
                i = quoteEnd(s, i);
                if (i < 0) {
                    return -1;
                }
            } else if (c == '(') {
                depth++;
            } else if (c == ')' && --depth == 0) {
                return i;
            }
        }
        return -1;
    }

    /** The index of the quote closing the one at {@code start} (a doubled quote is part of the text); -1 when none. */
    private static int quoteEnd(String s, int start) {
        char q = s.charAt(start);
        for (int i = start + 1; i < s.length(); i++) {
            if (s.charAt(i) == q) {
                if (i + 1 < s.length() && s.charAt(i + 1) == q) {
                    i++;
                } else {
                    return i;
                }
            }
        }
        return -1;
    }

    /**
     * The operator joining {@code s}'s parts outside quotes and parentheses
     * ("AND" or "OR", the parts added to {@code parts}), "" when there is none,
     * null when both join them or the quotes and parentheses do not balance.
     */
    private static String junction(String s, List<String> parts) {
        String op = "";
        int depth = 0;
        int start = 0;
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == '\'' || c == '"') {
                i = quoteEnd(s, i);
                if (i < 0) {
                    return null;
                }
            } else if (c == '(') {
                depth++;
            } else if (c == ')') {
                if (--depth < 0) {
                    return null;
                }
            } else if (depth == 0 && Character.isWhitespace(c)) {
                String here = s.regionMatches(true, i, " AND ", 0, 5) ? "AND" : s.regionMatches(true, i, " OR ", 0, 4) ? "OR" : null;
                if (here != null) {
                    if (!op.isEmpty() && !op.equals(here)) {
                        return null;
                    }
                    op = here;
                    parts.add(s.substring(start, i));
                    i += here.length() + 1;
                    start = i + 1;
                }
            }
        }
        if (depth != 0) {
            return null;
        }
        if (!op.isEmpty()) {
            parts.add(s.substring(start));
        }
        return op;
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

    /** A closed phrase of a simple_query_string with no escape in it, whose words are no operators in either syntax. */
    private static final Pattern SIMPLE_PHRASE = Pattern.compile("\"[^\"\\\\]*\"");

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
     * without a field. {@code minimumShouldMatch} applies to the optional
     * clauses of the top level only, as stock applies it to the query the
     * string parses to when that query is a clause list; where the plugin
     * cannot tell that list's clauses as stock counts them, it is refused
     * (see {@link #refuseUncountedClauses}). {@code simple} is a
     * simple_query_string, which has no default field of its own.
     */
    private String queryString(
        String query,
        String defaultField,
        List<String> fields,
        Operator op,
        String minimumShouldMatch,
        boolean simple,
        boolean positive
    ) {
        String q = query == null ? "" : query.trim();
        if (q.isEmpty() || "*".equals(q) || "*:*".equals(q)) {
            return ALL;
        }
        List<String> targets = queryTargets(fields, defaultField);
        InfinoLuceneQuery.Parsed top = InfinoLuceneQuery.parseTop(q, op);
        Node parsed = top.node();
        if (minimumShouldMatch == null) {
            return node(parsed, targets, positive, q, op);
        }
        refuseUncountedClauses(top, q, minimumShouldMatch, simple, oneField(fields, simple ? null : defaultField), targets);
        // Stock applies the value only to a clause list; a lone clause with +
        // is a list of that one required clause, with no optional one.
        Bool b = top.requiredAlone() ? new Bool(List.of(parsed), List.of(), List.of()) : parsed instanceof Bool list ? list : null;
        if (b == null) {
            return node(parsed, targets, positive, q, op);
        }
        // Stock's analysis makes no clause of a blank phrase on text, so it is not counted.
        List<Node> counted = b.should().stream().filter(s -> !tokenless(s, targets)).toList();
        if (counted.size() < b.should().size()) {
            b = new Bool(b.must(), counted, b.mustNot());
        }
        int required;
        try {
            required = Queries.calculateMinShouldMatch(b.should().size(), minimumShouldMatch);
        } catch (RuntimeException e) {
            // Stock's reader fails on a malformed form such as "2<" or "two", and
            // stock reports that as it reports any query it cannot build.
            throw new QueryShardException((Index) null, "failed to create query: {}", e, e.getMessage());
        }
        if (required > b.should().size()) {
            // More than there are, or any where none is optional (default_operator
            // AND, or only prohibited clauses): stock matches nothing. Stock builds
            // every clause first, so one the table cannot take is still refused.
            return node(parsed, targets, false, q, op) == null ? null : NONE;
        }
        if (required > 1 || (required == 1 && !b.must().isEmpty())) {
            // One optional clause, with none required, is what the list already asks.
            return atLeastOptional(b, required, targets, positive, q, op);
        }
        return node(parsed, targets, positive, q, op);
    }

    /** The fields a query string's clauses without a field search: those named, the default field, or every text field. */
    private List<String> queryTargets(List<String> fields, String defaultField) {
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
                    targets.add(entry.getKey());
                }
            }
        }
        return targets;
    }

    /** Whether stock reads a query string over exactly one field: one named, with no pattern. */
    private static boolean oneField(List<String> fields, String defaultField) {
        String only = fields.size() == 1 ? fields.get(0) : fields.isEmpty() ? defaultField : null;
        return only != null && only.indexOf('*') < 0;
    }

    /**
     * Refuses a {@code minimum_should_match} whose count the plugin cannot
     * make over the clauses stock counts. Stock (Lucene's parser, splitting
     * on operators only) analyzes a run of words with no operator between
     * them as one text: over one field each word the analyzer makes of the
     * run is a clause, over several fields the run is one clause joining the
     * fields with dis_max, and a lone word that analyzes to several words
     * ({@code foo-bar}) is itself the clause list. The plugin's tree has a
     * clause per word as written and does not run the analyzer, so where
     * those differ it cannot count as stock does. simple_query_string is
     * read here in the query_string syntax, which differs from the simple
     * one wherever the string holds an operator ({@code -gamma} there is an
     * optional clause matching the documents without gamma).
     */
    private void refuseUncountedClauses(InfinoLuceneQuery.Parsed top, String q, String spec, boolean simple, boolean oneField, List<String> targets) {
        Node parsed = top.node();
        String asked = "[" + q + "] has minimum_should_match [" + spec + "]";
        if (simple) {
            String operator = simpleSyntax(q);
            if (operator != null) {
                throw new InfinoLuceneQuery.Malformed(
                    "(simple_query_string) "
                        + asked
                        + " and the operator in ["
                        + operator
                        + "], which the plugin reads in the query_string syntax: stock's simple syntax makes other clauses of it, so the count would not be stock's"
                );
            }
            if (parsed instanceof Term t && (!oneField || (!t.quoted() && maybeSeveralWords(t.text())))) {
                throw new InfinoLuceneQuery.Malformed(
                    "(simple_query_string) "
                        + asked
                        + " on a lone clause, which stock counts over its fields or the words the analyzer makes of it; the plugin does not run the analyzer"
                );
            }
            return;
        }
        if (top.requiredAlone()) {
            // One required clause and no optional one, whatever it holds.
            return;
        }
        if (parsed instanceof Term t) {
            if (!t.quoted() && textTerm(t, targets) && maybeSeveralWords(t.text())) {
                throw new InfinoLuceneQuery.Malformed(
                    asked
                        + " on the lone term ["
                        + t.text()
                        + "], which stock counts over the words the analyzer makes of it; the plugin does not run the analyzer"
                );
            }
            return;
        }
        if (!(parsed instanceof Bool b)) {
            return;
        }
        int bare = 0;
        for (List<Node> clauses : List.of(b.must(), b.should())) {
            boolean optional = clauses == b.should();
            for (Node n : clauses) {
                if (n instanceof Term t && !t.quoted()) {
                    if (t.field() == null) {
                        bare++;
                    }
                    if (optional && textTerm(t, targets) && maybeSeveralWords(t.text())) {
                        throw new InfinoLuceneQuery.Malformed(
                            asked
                                + " and the optional term ["
                                + t.text()
                                + "], which stock counts as one clause or as each word the analyzer makes of it, by where it sits; the plugin does not run the analyzer"
                        );
                    }
                }
            }
        }
        if (bare >= 2 && !textTargets(targets)) {
            throw new InfinoLuceneQuery.Malformed(
                asked
                    + " on keyword fields only: stock reads a run of words there as one value, which the plugin does not tell apart from a clause per word; give each word its field"
            );
        }
        if (!oneField && bare >= 2) {
            throw new InfinoLuceneQuery.Malformed(
                asked
                    + " over more than one field: stock reads a run of words there as one clause over the fields (dis_max), which the plugin does not tell apart from a clause per word; name one field, or give each word its field"
            );
        }
    }

    /**
     * A blank phrase ({@code ""}, {@code " "}) on text columns only: the
     * analyzer makes no token of it, so stock drops the clause. On a keyword
     * column it is the empty value, a clause like any other.
     */
    private boolean tokenless(Node n, List<String> targets) {
        if (!(n instanceof Term t) || !t.quoted() || t.slop() > 0 || t.fuzzy() > 0 || !t.text().isBlank()) {
            return false;
        }
        List<String> fields = t.field() == null ? targets : List.of(t.field());
        if (fields.isEmpty()) {
            return false;
        }
        for (String f : fields) {
            Column c = column(f);
            if (c == null || !c.fts()) {
                return false;
            }
        }
        return true;
    }

    /** A term matched as text: on a text column, or on default fields of which one is text. */
    private boolean textTerm(Term t, List<String> targets) {
        if (t.field() == null) {
            return textTargets(targets);
        }
        Column c = column(t.field());
        return c != null && c.fts();
    }

    /**
     * Whether the default fields may read a term as text: false only when
     * every one is a keyword column, which matches the term as one exact
     * value, as stock's keyword analyzer keeps it whole.
     */
    private boolean textTargets(List<String> targets) {
        if (targets.isEmpty()) {
            return true;
        }
        for (String f : targets) {
            Column c = column(f);
            if (c == null || c.fts()) {
                return true;
            }
        }
        return false;
    }

    /**
     * Whether an analyzer may make more than one word of {@code text}: it
     * holds a character that is not a letter or digit, or an ideograph or
     * kana, which the standard analyzer makes a word each, or the standard
     * tokenizer does not keep it as one token (a change of script such as
     * {@code ไทยabc}, or a word longer than its longest token).
     */
    private static boolean maybeSeveralWords(String text) {
        return text.codePoints()
            .anyMatch(
                c -> !Character.isLetterOrDigit(c)
                    || Character.isIdeographic(c)
                    || Character.UnicodeScript.of(c) == Character.UnicodeScript.HIRAGANA
                    || Character.UnicodeScript.of(c) == Character.UnicodeScript.KATAKANA
            ) || !oneStandardToken(text);
    }

    /** Whether the standard tokenizer makes exactly one token of the whole of {@code text}. */
    private static boolean oneStandardToken(String text) {
        try (StandardTokenizer tokenizer = new StandardTokenizer()) {
            CharTermAttribute term = tokenizer.addAttribute(CharTermAttribute.class);
            tokenizer.setReader(new StringReader(text));
            tokenizer.reset();
            boolean one = tokenizer.incrementToken() && term.length() == text.length() && !tokenizer.incrementToken();
            tokenizer.end();
            return one;
        } catch (IOException e) {
            // A string reader does not fail.
            throw new UncheckedIOException(e);
        }
    }

    /**
     * The first word of a simple_query_string that holds an operator of
     * either syntax ({@code -} leading a word, {@code +}, {@code |},
     * parentheses, {@code *}, {@code ~}, a field prefix, a range, a regular
     * expression, an escape, a boost, or AND / OR / NOT) outside a phrase,
     * or null when the string is plain words and phrases, which both syntaxes
     * read alike. A phrase is checked as its bounds only, a word of its own,
     * so what follows it ({@code ~2}, or {@code OR} in {@code "alpha"OR})
     * is still an operator.
     */
    private static String simpleSyntax(String q) {
        for (String word : QUERY_WHITESPACE.split(SIMPLE_PHRASE.matcher(q.trim()).replaceAll(" \"\" ").trim())) {
            if (word.startsWith("-") || word.equals("AND") || word.equals("OR") || word.equals("NOT")) {
                return word;
            }
            for (int i = 0; i < word.length(); i++) {
                if ("+|()*~!&:[]{}/\\^<>".indexOf(word.charAt(i)) >= 0) {
                    return word;
                }
            }
        }
        return null;
    }

    /**
     * A query string's top clause list with at least {@code required} of its
     * optional clauses matching: its required clauses, the count over its
     * optional ones, and none of its prohibited ones. When the list is one
     * engine query, that query still ranks the rows (BM25 over every clause,
     * as stock scores the clauses that match), and the count narrows them.
     */
    private String atLeastOptional(Bool b, int required, List<String> targets, boolean positive, String query, Operator op) {
        List<String> optional = new ArrayList<>();
        for (Node s : b.should()) {
            String p = node(s, targets, false, query, op);
            if (p == null) {
                return null;
            }
            optional.add(p);
        }
        String count = atLeast(optional, required);
        String engine = engineQuery(b);
        if (engine != null) {
            String ranked = textOnFields(targets, engine, Operator.OR, positive, false, false, true);
            return ranked == null ? null : "(" + ranked + " AND " + count + ")";
        }
        List<String> and = new ArrayList<>();
        for (Node m : b.must()) {
            String p = node(m, targets, positive, query, op);
            if (p == null) {
                return null;
            }
            and.add(p);
        }
        and.add(count);
        for (Node x : b.mustNot()) {
            String p = node(x, targets, false, query, op);
            if (p == null) {
                return null;
            }
            and.add(not(p));
        }
        return "(" + String.join(" AND ", and) + ")";
    }

    /**
     * {@code clauses}, optional clauses of one group, as one text clause
     * matching any of their words, when each is a single plain word that is
     * one analyzed token (no phrase, wildcard, fuzziness, proximity or
     * {@code disk-full}, see {@link #oneToken}) and all reach the same one
     * text column: a field clause naming it, or a bare word whose default
     * fields hold that text column and no other field the word could match.
     * Null otherwise, and the clauses are read one by one.
     */
    private String oneColumnAnyWord(List<Node> clauses, List<String> targets, boolean positive) {
        String column = null;
        List<String> words = new ArrayList<>();
        for (Node n : clauses) {
            if (!(n instanceof Term t) || t.quoted() || t.fuzzy() > 0 || t.slop() > 0) {
                return null;
            }
            String word = plainTerms(t.text());
            if (!oneToken(word)) {
                return null;
            }
            String reached = t.field() == null ? onlyTextColumn(targets) : onlyTextColumn(List.of(t.field()));
            if (reached == null || (column != null && !column.equals(reached))) {
                return null;
            }
            column = reached;
            words.add(word);
        }
        return column == null ? null : textClause(column, String.join(" ", words), "or", positive);
    }

    /**
     * The longest token both {@code standard} analyzers keep whole: Lucene's
     * counts UTF-16 units and the engine's code points, and a longer word is
     * split into pieces. For the ASCII words {@link #oneToken} takes, the two
     * counts are the same.
     */
    static final int MAX_TOKEN_CHARS = 255;

    /**
     * Whether the engine's {@code standard} analyzer surely reads {@code word}
     * as one token: ASCII letters and digits only, and no longer than
     * {@link #MAX_TOKEN_CHARS}. Any other word may be several tokens, which its
     * own clause requires together and an any-word clause would not:
     * {@code disk-full} and {@code a*} split at the punctuation, a longer word
     * into pieces, and the engine's UAX #29 segmenter gives a token per
     * character to Han, Hiragana and the scripts Lucene reads as one
     * South-East Asian run ({@code ไทย} is three). An allow-list rather than a
     * list of the scripts it splits, so a script left off keeps its own clause,
     * which is always right, only not merged.
     */
    static boolean oneToken(String word) {
        if (word.isEmpty() || word.length() > MAX_TOKEN_CHARS) {
            return false;
        }
        for (int i = 0; i < word.length(); i++) {
            char c = word.charAt(i);
            if (!((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9'))) {
                return false;
            }
        }
        return true;
    }

    /** The one text column {@code fields} name, when they name exactly one field the table has and it is a text column. */
    private String onlyTextColumn(List<String> fields) {
        String only = null;
        for (String f : fields) {
            Column c = column(f);
            if (c == null) {
                continue;
            }
            if (!c.fts() || (only != null && !only.equals(c.name()))) {
                return null;
            }
            only = c.name();
        }
        return only;
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
            // Optional clauses alone that are each one plain word on the same
            // text column (`message:a b c` over a single text column) are that
            // column matching any of the words: one text clause, which can be
            // the query's ranked source, rather than one clause per word.
            if (b.must().isEmpty() && b.mustNot().isEmpty()) {
                String one = oneColumnAnyWord(b.should(), targets, positive);
                if (one != null) {
                    return one;
                }
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
        return wildcardToRegex(s, false);
    }

    /** {@link #wildcardToRegex(String)}, a backslash escaping the character after it when {@code escapes}, as a wildcard query reads it. */
    private static String wildcardToRegex(String s, boolean escapes) {
        StringBuilder out = new StringBuilder();
        for (int i = 0; i < s.length(); i++) {
            char ch = s.charAt(i);
            if (escapes && ch == '\\' && i + 1 < s.length()) {
                ch = s.charAt(++i);
                if ("\\.[]{}()+-?^$|*".indexOf(ch) >= 0) {
                    out.append('\\');
                }
                out.append(ch);
                continue;
            }
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

    /**
     * Refuses a match that asks for fuzziness: the table matches a query's
     * terms as written, so a misspelled term would find nothing where stock
     * finds its near matches. A fuzziness of 0 asks for none.
     */
    private static void unfuzzy(String query, String fields, Fuzziness fuzziness) {
        if (fuzziness != null && !Fuzziness.ZERO.asString().equals(fuzziness.asString())) {
            throw new IllegalArgumentException(
                "[infino] " + query + " on [" + fields + "] asks for fuzziness [" + fuzziness.asString() + "], which is not served"
            );
        }
    }

    // ---- scalars ----

    private String equality(String field, Object value) {
        if ("_id".equals(field)) {
            return value == null ? NONE : OSID + " = " + literal(String.valueOf(value));
        }
        if (IGNORED_FIELD.equals(field)) {
            return value == null ? NONE : ignoredHas(" = " + literal(String.valueOf(value)));
        }
        Column c = column(field);
        if (c == null || value == null) {
            return c == null ? NONE : null;
        }
        if (isCidr(c, value)) {
            return anyValue(c, cidr(field, value.toString()));
        }
        String lit = scalar(c.element(), value, false);
        return lit == null ? null : anyValue(c, v -> v + " = " + lit);
    }

    private String inList(String field, List<Object> values) {
        if ("_id".equals(field) && values != null && !values.isEmpty()) {
            List<String> ids = new ArrayList<>();
            for (Object v : values) {
                ids.add(literal(String.valueOf(v)));
            }
            return OSID + " IN (" + String.join(", ", ids) + ")";
        }
        if (IGNORED_FIELD.equals(field)) {
            if (values == null || values.isEmpty()) {
                return NONE;
            }
            List<String> paths = new ArrayList<>();
            for (Object v : values) {
                paths.add(literal(String.valueOf(v)));
            }
            return ignoredHas(" IN (" + String.join(", ", paths) + ")");
        }
        Column c = column(field);
        if (c == null) {
            return NONE;
        }
        if (values == null || values.isEmpty()) {
            return NONE;
        }
        if (values.stream().anyMatch(v -> isCidr(c, v))) {
            // Each value its own test, a block's addresses or one address, any of them a match.
            List<Function<String, String>> tests = new ArrayList<>();
            for (Object v : values) {
                if (isCidr(c, v)) {
                    tests.add(cidr(field, v.toString()));
                } else if (v != null) {
                    String lit = scalar(c.element(), v, false);
                    tests.add(x -> x + " = " + lit);
                }
            }
            return anyValue(c, x -> {
                List<String> any = new ArrayList<>(tests.size());
                for (Function<String, String> test : tests) {
                    any.add(test.apply(x));
                }
                return "(" + String.join(" OR ", any) + ")";
            });
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

    /**
     * The rows whose ignored fields include a path {@code test} (an SQL
     * comparison of it) holds for; null, as for any list column, where the
     * clause may only compare column values ({@link #columnValuesOnly}).
     */
    private String ignoredHas(String test) {
        if (ignoredColumn(cols) == null) {
            return NONE;
        }
        return columnValuesOnly ? null : anyValue(table, null, quoteIdent(IGNORED), v -> v + test);
    }

    /**
     * The table's {@link #IGNORED} column, or null when no document has had a
     * field ignored: the column is added with the first. A document field of
     * that name is not it ({@link #OWN_COLUMN_PREFIX}), nor is a column that
     * holds no list.
     */
    static Column ignoredColumn(Map<String, Column> cols) {
        Column c = cols.get(IGNORED);
        return c != null && c.name().equals(IGNORED) && c.isList() ? c : null;
    }

    /** The select item reading the {@link #IGNORED} column as one string, a path per line ({@link InfinoIgnored#SEPARATOR}). */
    static String ignoredPaths() {
        String column = quoteIdent(IGNORED);
        return "array_to_string(" + column + ", chr(10)) AS " + column;
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
        InfinoNative.FieldType rounded = typed.isNumeric() && typed.fieldType() != null && typed.fieldType().rounds() ? typed.fieldType() : null;
        if (rounded != null) {
            // Each bound as the kept value it admits, inclusive, as stock reads a float, half_float
            // or scaled_float range; an infinite one admits every value or none.
            double lower;
            double upper;
            try {
                lower = r.from() == null ? Double.NEGATIVE_INFINITY : rounded.lower(r.from(), r.includeLower());
                upper = r.to() == null ? Double.POSITIVE_INFINITY : rounded.upper(r.to(), r.includeUpper());
            } catch (IllegalArgumentException e) {
                // A bound that is not a number, as for any other numeric field.
                return null;
            }
            if (r.from() != null) {
                if (lower == Double.POSITIVE_INFINITY) {
                    return NONE;
                }
                if (lower != Double.NEGATIVE_INFINITY) {
                    bounds.add(" >= " + number(lower));
                }
            }
            if (r.to() != null) {
                if (upper == Double.NEGATIVE_INFINITY) {
                    return NONE;
                }
                if (upper != Double.POSITIVE_INFINITY) {
                    bounds.add(" <= " + number(upper));
                }
            }
            if (bounds.isEmpty() && (r.from() != null || r.to() != null)) {
                // Bounds past every finite value still ask for a value.
                bounds.add(" IS NOT NULL");
            }
        }
        if (rounded == null && r.from() != null) {
            // An exclusive lower date bound rounds up (gt 2026-10-24 means after that day).
            String lit = typed.isTimestamp() ? dateBound(r.from(), format, !r.includeLower(), zone) : scalar(typed, r.from(), false);
            if (lit == null) {
                return null;
            }
            bounds.add((r.includeLower() ? " >= " : " > ") + lit);
        }
        if (rounded == null && r.to() != null) {
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

    /**
     * A string column a case-insensitive {@code query} compares by its
     * lowercased values; any other refuses, as stock refuses such a query
     * on a field that is not a string.
     */
    private Column caseless(Column c, String query) {
        if (!c.element().isString()) {
            throw new IllegalArgumentException("[infino] a case_insensitive " + query + " on [" + fieldOf(c.name()) + "], which is not a string field, is not served");
        }
        return c;
    }

    /** The rows where a value of a string field matches a LIKE pattern, both lowercased. */
    private String caselessLike(Column c, String pattern) {
        String lit = literal(pattern);
        return anyValue(c, v -> "lower(" + v + ") LIKE lower(" + lit + ")");
    }

    /**
     * A Lucene wildcard as a LIKE pattern: {@code *} any run, {@code ?} one
     * character, a backslash escaping the character after it (a trailing
     * one is itself), the rest literal.
     */
    static String escapedWildcardToLike(String s) {
        StringBuilder out = new StringBuilder();
        for (int i = 0; i < s.length(); i++) {
            char ch = s.charAt(i);
            if (ch == '\\' && i + 1 < s.length()) {
                ch = s.charAt(++i);
                if (ch == '%' || ch == '_' || ch == '\\') {
                    out.append('\\');
                }
                out.append(ch);
                continue;
            }
            switch (ch) {
                case '*' -> out.append('%');
                case '?' -> out.append('_');
                case '%', '_', '\\' -> out.append('\\').append(ch);
                default -> out.append(ch);
            }
        }
        return out.toString();
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
            if (c.fieldType() != null && c.fieldType().rounds()) {
                // A float, half_float or scaled_float field compares the value as it keeps it, rounded.
                Double kept = kept(c, value);
                return kept == null ? null : number(kept);
            }
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
        return literal(Instant.ofEpochMilli(dateBoundMillis(value, format, roundUp, zone, nowMillis)).toString());
    }

    /**
     * A range's date bound as epoch milliseconds, read as {@link #range}
     * reads it into the statement: {@code format} is the range's own, else
     * the field's mapped one, {@code roundUp} is set for an exclusive lower
     * bound and an inclusive upper one, and a bound the format cannot read
     * is refused as OpenSearch refuses it. Anything that judges a range
     * before it is sent reads its bounds here, so it judges what is read.
     */
    static long dateBoundMillis(Object value, String format, boolean roundUp, ZoneId zone, long nowMillis) {
        Long at = dateMillis(value, format, roundUp, zone, nowMillis);
        if (at == null) {
            throw new OpenSearchParseException(
                "failed to parse date field [{}] with format [{}]",
                value,
                format == null ? DEFAULT_DATE_FORMAT : format
            );
        }
        return at;
    }

    /**
     * A date, epoch number or date math as epoch milliseconds, or null when
     * {@code format} cannot read it. A number is epoch milliseconds in the
     * default format and read by {@code format} when one is given, as
     * OpenSearch reads a number by the field's format (an
     * {@code epoch_second} field's {@code 893966296} is seconds).
     */
    private static Long dateMillis(Object value, String format, boolean roundUp, ZoneId zone, long nowMillis) {
        if (value instanceof Number n && format == null) {
            return n.longValue();
        }
        return dateMathMillis(value.toString(), format, roundUp, zone, nowMillis);
    }

    /**
     * A timestamp literal from a date, epoch milliseconds, or date math, in the engine's ISO form.
     * A number is epoch milliseconds in the default format and read by {@code format} when one is
     * given, as OpenSearch reads a number by the field's format (an {@code epoch_second} field's
     * {@code 893966296} is seconds).
     */
    private String dateLiteral(Object value, String format, boolean roundUp, ZoneId zone) {
        Long at = dateMillis(value, format, roundUp, zone, nowMillis);
        // The column holds milliseconds; a rounded-up bound ends in .999.
        return at == null ? null : literal(Instant.ofEpochMilli(at).toString());
    }
}
