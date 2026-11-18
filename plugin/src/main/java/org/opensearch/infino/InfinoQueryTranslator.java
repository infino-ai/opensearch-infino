/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.search.join.ScoreMode;
import org.opensearch.OpenSearchParseException;
import org.opensearch.OpenSearchSecurityException;
import org.opensearch.common.lucene.search.Queries;
import org.opensearch.common.time.DateFormatter;
import org.opensearch.common.time.DateMathParser;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.index.Index;
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
     * A query string's BM25 over several text columns, as stock scores it:
     * each of {@code clauses} scores the best of its columns' BM25 (a
     * {@code dis_max} with tie_breaker 0, one {@link TextClause} per column),
     * and a hit scores the sum over the clauses it matches (a bool's should
     * clauses). {@code match} is the predicate the query string translated
     * to, every row any clause matches; the ranked clauses read the same rows.
     */
    record Across(List<List<TextClause>> clauses, String match) {}

    /**
     * How a hit scores, as Lucene sums a bool's scoring clauses: the BM25 of
     * a text clause, {@code offset} from the clauses every hit matches with
     * one score (a term, terms, range or exists in must scores its boost),
     * and each {@code bonuses} entry where its clause matches (such a clause
     * in should, or a nested clause, whose score is its entries'). The text
     * clause is the principal when there is one; otherwise {@code text}, a
     * text clause in should, whose BM25 counts where it matches.
     * {@code ascending} when the request sorts by the score ascending: the
     * least sums come first.
     */
    record Scoring(TextClause text, double offset, List<Bonus> bonuses, boolean ascending, Across across) {
        static final Scoring NONE = new Scoring(null, 0, List.of());

        Scoring(TextClause text, double offset, List<Bonus> bonuses) {
            this(text, offset, bonuses, false);
        }

        Scoring(TextClause text, double offset, List<Bonus> bonuses, boolean ascending) {
            this(text, offset, bonuses, ascending, null);
        }

        /** Whether hits rank by more than the principal's BM25 alone: then {@code offset} only shifts every score alike. */
        boolean reorders() {
            return text != null || !bonuses.isEmpty();
        }
    }

    /**
     * What a query translates to. {@code span} is set when the query is one
     * range on a date column with both bounds, and is then exactly the rows
     * {@code where} keeps; null otherwise.
     */
    record Translation(String table, String where, TextClause principal, Scoring scoring, Span span) {
        Translation(String table, String where, TextClause principal) {
            this(table, where, principal, Scoring.NONE, null);
        }

        /** This translation with {@code where} as its predicate, scored as before; no longer one span. */
        Translation withWhere(String where) {
            return new Translation(table, where, principal, scoring, null);
        }

        /** This translation with no clause but the principal's BM25 scoring the hits. */
        Translation unscored() {
            return new Translation(table, where, principal, Scoring.NONE, span);
        }

        /**
         * This translation with its hits in ascending score. Only the bonuses
         * of a query with no text clause are summed over every hit; a text
         * clause's BM25 ranks its best hits only, so such a query is unscored.
         */
        Translation ascending() {
            if (principal != null || scoring.text() != null || scoring.across() != null) {
                return unscored();
            }
            return new Translation(table, where, principal, new Scoring(null, scoring.offset(), scoring.bonuses(), true), span);
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
            return rankedFrom(principal, k);
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

        /** The predicate {@code text} is as a clause other than the ranked one: the ids its token match holds. */
        String textMatch(TextClause text) {
            return textSubquery(table, text);
        }
    }

    /** The ids a text clause matches, as a subquery over the token index of {@code table}. */
    private static String textSubquery(String table, TextClause text) {
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
        String where = t.predicate(qb, true);
        if (where == null) {
            return null;
        }
        return new Translation(table, where, t.principal, t.scoring(qb), qb instanceof RangeQueryBuilder ? t.span : null);
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
        boolean[] unscored = { false };
        required(root, offset, bonuses, texts, unscored);
        if (across != null) {
            if (principal == null && !unscored[0] && texts.isEmpty() && bonuses.isEmpty()) {
                // The query string's BM25 across its columns is the only score beside the offset.
                return new Scoring(null, offset[0], List.of(), false, across);
            }
            // Beside another scoring clause the sum is no longer the query string's alone.
            unscored[0] = true;
        }
        if (principal == null && (unscored[0] || texts.size() > 1)) {
            // A should clause the table cannot score (or a second text clause,
            // which would add its BM25 beside the first) would leave some of the
            // hits it matches at 0, under hits it outscores in stock: with no
            // ranked clause, the hits keep the one score they all had.
            return offset[0] == 0 ? Scoring.NONE : new Scoring(null, offset[0], List.of());
        }
        // Beside a ranked clause a should clause the table cannot score adds
        // nothing, as before; the others add theirs.
        TextClause text = principal == null && texts.size() == 1 ? texts.get(0) : null;
        return offset[0] == 0 && bonuses.isEmpty() && text == null ? Scoring.NONE : new Scoring(text, offset[0], List.copyOf(bonuses));
    }

    /** A clause every hit matches: the root, or a must clause of a bool every hit matches. */
    private void required(QueryBuilder qb, double[] offset, List<Bonus> bonuses, List<TextClause> texts, boolean[] unscored) {
        // A bool with no clause is a match_all: it scores its boost (constantScore).
        if (qb instanceof BoolQueryBuilder b && b.hasClauses()) {
            // A filter or must_not clause scores nothing.
            for (QueryBuilder c : b.must()) {
                required(c, offset, bonuses, texts, unscored);
            }
            for (QueryBuilder c : b.should()) {
                if (!optional(c, bonuses, texts)) {
                    unscored[0] = true;
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
            if (t == null || !t.equals(principal)) {
                // A query string over several columns ranks across them when it is the one text clause.
                Across a = principal == null && across == null && qb instanceof QueryStringQueryBuilder qs ? across(qs) : null;
                if (a != null) {
                    across = a;
                } else {
                    unscored[0] = true;
                }
            }
        }
    }

    /** How a query string's JSON names its default type, best_fields: the builder has no getter for it. */
    private static final Pattern BEST_FIELDS = Pattern.compile("\"type\"\\s*:\\s*\"best_fields\"");

    /** The most BM25 searches (clauses times columns) a query string ranks across before it is left unranked. */
    static final int MAX_ACROSS_SEARCHES = 64;

    /**
     * How stock ranks {@code qs} across its text columns ({@link Across}), or
     * null when the plugin does not rank it that way: it is on one column
     * (then it is the principal), or its form is outside what is read here.
     *
     * <p>Stock's parser (Lucene's, not splitting on whitespace) makes a
     * clause of each {@code field:word}, and one clause of each run of words
     * with no operator between them, on the default fields; a word before a
     * {@code field:} or an {@code OR} ends the run. Over several fields a
     * clause is a {@code dis_max} of each field's BM25 for its words
     * (best_fields, tie_breaker 0), and the clauses are summed. Read here:
     * plain words (one token each, no word twice in a run), {@code field:word}
     * on a text column and {@code OR}, under the default operator OR, every
     * field a text column with boost 1, and no option that changes the
     * scoring.
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
        for (String f : queryTargets(new ArrayList<>(qs.fields().keySet()), qs.defaultField())) {
            Column c = column(f);
            if (c == null || !c.fts()) {
                return null;
            }
            if (!defaults.contains(c.name())) {
                defaults.add(c.name());
            }
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
            afterOr = false;
            if (i + 1 < tokens.size() && ":".equals(tokens.get(i + 1))) {
                // field:word, one clause on that field's column.
                if (!fieldName(t) || i + 2 >= tokens.size() || !plainWord(tokens.get(i + 2))
                    || (i + 3 < tokens.size() && ":".equals(tokens.get(i + 3)))) {
                    return null;
                }
                Column c = column(t);
                if (c == null || !c.fts() || (!trusted && c.masked())) {
                    return null;
                }
                clauses.add(List.of(new TextClause(c.name(), tokens.get(i + 2), "or")));
                searches++;
                i += 3;
                continue;
            }
            if (!plainWord(t)) {
                return null;
            }
            // A run: the words that follow while the word after each is no field's colon or OR.
            List<String> run = new ArrayList<>(List.of(t));
            int j = i + 1;
            while (j < tokens.size() && plainWord(tokens.get(j))
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
        return textSubquery(table, new TextClause(column, query, mode));
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
                    targets.add(c.name());
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
                at = parser.parse(value.toString(), InfinoClock.of(nowMillis), roundUp, zone);
            } catch (RuntimeException e) {
                return null;
            }
        }
        // The column holds milliseconds; a rounded-up bound ends in .999.
        return literal(Instant.ofEpochMilli(at.toEpochMilli()).toString());
    }
}
