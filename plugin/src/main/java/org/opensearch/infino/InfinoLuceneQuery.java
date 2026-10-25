/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import org.opensearch.index.query.Operator;

import java.util.ArrayList;
import java.util.List;

/**
 * Reads the Lucene query-string syntax into a boolean tree: terms and
 * phrases (with {@code ~slop}), wildcards ({@code *}, {@code ?}),
 * regular expressions ({@code /…/}), ranges ({@code [a TO b]},
 * {@code {a TO b}}, {@code >=n} and the other comparisons), field prefixes
 * ({@code field:term}, {@code field:(a OR b)}), {@code AND}/{@code OR}/
 * {@code NOT} (also {@code &&}, {@code ||}, {@code !}), the {@code +} and
 * {@code -} sigils, parentheses, {@code ^boost}, {@code ~fuzziness} and
 * backslash escapes. The tree says what the query means; the translator
 * decides what the table can serve of it.
 *
 * <p>Clauses combine as Lucene's classic parser combines them: a group is
 * a flat list of clauses, each required, optional or prohibited. A clause
 * introduced by {@code AND} is required and makes the clause before it
 * required; one introduced by {@code OR} is optional and, under a default
 * operator of AND, makes the clause before it optional; {@code +} and
 * {@code -} (or {@code NOT}) fix a clause regardless; a clause with no
 * operator takes the default operator. A group with any required clause
 * matches on its required clauses alone, its optional clauses then only
 * affecting the score; a group with none needs at least one optional
 * clause. That is not operator precedence, and it is what the syntax has
 * always meant.
 */
final class InfinoLuceneQuery {
    private InfinoLuceneQuery() {}

    /** A syntax error; the request fails with the text named. */
    static final class Malformed extends IllegalArgumentException {
        Malformed(String message) {
            super("[infino] query_string " + message);
        }
    }

    sealed interface Node permits Term, Wildcard, Regex, Range, All, Bool {}

    /** A term or phrase; {@code field} is null for the default fields. {@code fuzzy} is the edit distance asked for (0 when none). */
    record Term(String field, String text, boolean quoted, int slop, int fuzzy) implements Node {}

    /** A term with {@code *} or {@code ?} in it. */
    record Wildcard(String field, String pattern) implements Node {}

    /** A {@code /pattern/}. */
    record Regex(String field, String pattern) implements Node {}

    /** A range; a null bound is open. */
    record Range(String field, String from, String to, boolean includeFrom, boolean includeTo) implements Node {}

    /** {@code *} or {@code *:*}: everything. */
    record All() implements Node {}

    /** A clause list: the required, the optional and the prohibited clauses. */
    record Bool(List<Node> must, List<Node> should, List<Node> mustNot) implements Node {}

    /** The tree of {@code query}; {@code op} is the operator for clauses with none between them. */
    static Node parse(String query, Operator op) {
        Parser p = new Parser(query, op);
        Node n = p.group();
        if (p.pos < p.tokens.size() - 1) {
            throw new Malformed("[" + query + "] has text after the end of the query: [" + p.tokens.get(p.pos).text() + "]");
        }
        return n;
    }

    // ---- tokens ----

    private enum Kind {
        WORD,
        PHRASE,
        REGEX,
        LPAREN,
        RPAREN,
        LBRACKET, // [ or {, text says which
        RBRACKET, // ] or }
        AND,
        OR,
        NOT,
        PLUS,
        MINUS,
        COLON,
        TO,
        END
    }

    /** A token; {@code suffix} carries the {@code ~n} / {@code ^n} modifiers that followed it, if any. */
    private record Token(Kind kind, String text, String suffix) {}

    private static boolean isSpecial(char c) {
        return c == '(' || c == ')' || c == '[' || c == ']' || c == '{' || c == '}' || c == '"' || c == ':' || Character.isWhitespace(c);
    }

    private static List<Token> tokenize(String q) {
        List<Token> out = new ArrayList<>();
        int i = 0;
        int n = q.length();
        while (i < n) {
            char c = q.charAt(i);
            if (Character.isWhitespace(c)) {
                i++;
                continue;
            }
            switch (c) {
                case '(' -> {
                    out.add(new Token(Kind.LPAREN, "(", null));
                    i++;
                    continue;
                }
                case ')' -> {
                    i++;
                    out.add(new Token(Kind.RPAREN, ")", modifiers(q, i)));
                    i = skipModifiers(q, i);
                    continue;
                }
                case '[', '{' -> {
                    out.add(new Token(Kind.LBRACKET, String.valueOf(c), null));
                    i++;
                    continue;
                }
                case ']', '}' -> {
                    i++;
                    out.add(new Token(Kind.RBRACKET, String.valueOf(c), modifiers(q, i)));
                    i = skipModifiers(q, i);
                    continue;
                }
                case ':' -> {
                    out.add(new Token(Kind.COLON, ":", null));
                    i++;
                    continue;
                }
                case '"' -> {
                    StringBuilder sb = new StringBuilder();
                    i++;
                    while (i < n && q.charAt(i) != '"') {
                        if (q.charAt(i) == '\\' && i + 1 < n) {
                            i++;
                        }
                        sb.append(q.charAt(i));
                        i++;
                    }
                    if (i >= n) {
                        throw new Malformed("[" + q + "] has an unterminated phrase");
                    }
                    i++;
                    out.add(new Token(Kind.PHRASE, sb.toString(), modifiers(q, i)));
                    i = skipModifiers(q, i);
                    continue;
                }
                case '/' -> {
                    StringBuilder sb = new StringBuilder();
                    i++;
                    while (i < n && q.charAt(i) != '/') {
                        if (q.charAt(i) == '\\' && i + 1 < n && q.charAt(i + 1) == '/') {
                            i++;
                        }
                        sb.append(q.charAt(i));
                        i++;
                    }
                    if (i >= n) {
                        throw new Malformed("[" + q + "] has an unterminated regular expression");
                    }
                    i++;
                    out.add(new Token(Kind.REGEX, sb.toString(), modifiers(q, i)));
                    i = skipModifiers(q, i);
                    continue;
                }
                default -> {
                }
            }
            // A sigil at the start of a clause.
            if ((c == '+' || c == '-' || c == '!') && i + 1 < n && !Character.isWhitespace(q.charAt(i + 1))) {
                out.add(new Token(c == '+' ? Kind.PLUS : c == '-' ? Kind.MINUS : Kind.NOT, String.valueOf(c), null));
                i++;
                continue;
            }
            // A word: up to whitespace or a special character; a backslash escapes the next character.
            StringBuilder sb = new StringBuilder();
            while (i < n && !isSpecial(q.charAt(i))) {
                char ch = q.charAt(i);
                if (ch == '\\' && i + 1 < n) {
                    sb.append(q.charAt(i + 1));
                    i += 2;
                    continue;
                }
                if ((ch == '~' || ch == '^') && sb.length() > 0) {
                    break;
                }
                sb.append(ch);
                i++;
            }
            String word = sb.toString();
            String suffix = modifiers(q, i);
            i = skipModifiers(q, i);
            switch (word) {
                case "AND", "&&" -> out.add(new Token(Kind.AND, word, null));
                case "OR", "||" -> out.add(new Token(Kind.OR, word, null));
                case "NOT" -> out.add(new Token(Kind.NOT, word, null));
                case "TO" -> out.add(new Token(Kind.TO, word, null));
                default -> {
                    if (word.isEmpty()) {
                        throw new Malformed("[" + q + "] has a modifier with nothing before it at offset " + i);
                    }
                    out.add(new Token(Kind.WORD, word, suffix));
                }
            }
        }
        out.add(new Token(Kind.END, "", null));
        return out;
    }

    /** The {@code ~n} / {@code ^n} modifiers at {@code i}, as text, or null. */
    private static String modifiers(String q, int i) {
        int end = skipModifiers(q, i);
        return end > i ? q.substring(i, end) : null;
    }

    private static int skipModifiers(String q, int i) {
        int n = q.length();
        while (i < n && (q.charAt(i) == '~' || q.charAt(i) == '^')) {
            i++;
            while (i < n && (Character.isDigit(q.charAt(i)) || q.charAt(i) == '.')) {
                i++;
            }
        }
        return i;
    }

    // ---- parser ----

    /** How a clause sits in its group. */
    private enum Occur {
        MUST,
        SHOULD,
        MUST_NOT
    }

    /** One clause of a group with how it sits, settled once the next clause's operator is known. */
    private static final class Clause {
        final Node node;
        Occur occur;

        Clause(Node node, Occur occur) {
            this.node = node;
            this.occur = occur;
        }
    }

    private static final class Parser {
        final List<Token> tokens;
        final Operator op;
        final String query;
        int pos;

        Parser(String query, Operator op) {
            this.query = query;
            this.tokens = tokenize(query);
            this.op = op;
        }

        private Token peek() {
            return tokens.get(pos);
        }

        private boolean at(Kind k) {
            return peek().kind() == k;
        }

        private Token take() {
            return tokens.get(pos++);
        }

        private Malformed malformed(String what) {
            return new Malformed("[" + query + "] " + what);
        }

        /** A clause list up to the end or a closing parenthesis, combined as Lucene's parser combines it. */
        Node group() {
            List<Clause> clauses = new ArrayList<>();
            while (!at(Kind.END) && !at(Kind.RPAREN)) {
                Kind conj = null;
                if (at(Kind.AND) || at(Kind.OR)) {
                    conj = take().kind();
                    if (at(Kind.END) || at(Kind.RPAREN)) {
                        throw malformed("ends with an operator");
                    }
                }
                Kind mod = null;
                while (at(Kind.PLUS) || at(Kind.MINUS) || at(Kind.NOT)) {
                    Kind k = take().kind();
                    mod = k == Kind.PLUS ? Kind.PLUS : (mod == Kind.NOT ? null : Kind.NOT);
                }
                Node value = clauseValue();
                // The operator before this clause settles the clause before it.
                if (!clauses.isEmpty() && conj == Kind.AND) {
                    Clause last = clauses.get(clauses.size() - 1);
                    if (last.occur != Occur.MUST_NOT) {
                        last.occur = Occur.MUST;
                    }
                }
                if (!clauses.isEmpty() && op == Operator.AND && conj == Kind.OR) {
                    Clause last = clauses.get(clauses.size() - 1);
                    if (last.occur != Occur.MUST_NOT) {
                        last.occur = Occur.SHOULD;
                    }
                }
                Occur occur;
                if (mod == Kind.NOT) {
                    occur = Occur.MUST_NOT;
                } else if (op == Operator.OR) {
                    occur = conj == Kind.AND || mod == Kind.PLUS ? Occur.MUST : Occur.SHOULD;
                } else {
                    occur = conj == Kind.OR && mod != Kind.PLUS ? Occur.SHOULD : Occur.MUST;
                }
                clauses.add(new Clause(value, occur));
            }
            if (clauses.isEmpty()) {
                return new All();
            }
            if (clauses.size() == 1 && clauses.get(0).occur != Occur.MUST_NOT) {
                return clauses.get(0).node;
            }
            List<Node> must = new ArrayList<>();
            List<Node> should = new ArrayList<>();
            List<Node> mustNot = new ArrayList<>();
            for (Clause c : clauses) {
                switch (c.occur) {
                    case MUST -> must.add(c.node);
                    case SHOULD -> should.add(c.node);
                    case MUST_NOT -> mustNot.add(c.node);
                }
            }
            return new Bool(must, should, mustNot);
        }

        /** A field prefix, then a group, range, phrase, regex or term. */
        private Node clauseValue() {
            String field = null;
            if (at(Kind.WORD) && tokens.get(pos + 1).kind() == Kind.COLON) {
                field = take().text();
                take();
            }
            Token t = take();
            switch (t.kind()) {
                case LPAREN -> {
                    Node inner = group();
                    if (!at(Kind.RPAREN)) {
                        throw malformed("has an unclosed parenthesis");
                    }
                    take();
                    return field == null ? inner : scoped(inner, field);
                }
                case LBRACKET -> {
                    boolean includeFrom = t.text().equals("[");
                    String from = bound();
                    if (!at(Kind.TO)) {
                        throw malformed("has a range without TO");
                    }
                    take();
                    String to = bound();
                    if (!at(Kind.RBRACKET)) {
                        throw malformed("has an unclosed range");
                    }
                    boolean includeTo = take().text().equals("]");
                    return new Range(field, from, to, includeFrom, includeTo);
                }
                case PHRASE -> {
                    return new Term(field, t.text(), true, slop(t.suffix()), 0);
                }
                case REGEX -> {
                    return new Regex(field, t.text());
                }
                case WORD -> {
                    String w = t.text();
                    if (w.equals("*")) {
                        // `*` matches everything; `field:*` the rows with a value.
                        return field == null ? new All() : new Range(field, null, null, true, true);
                    }
                    if (w.startsWith(">=")) {
                        return new Range(field, w.substring(2), null, true, true);
                    }
                    if (w.startsWith("<=")) {
                        return new Range(field, null, w.substring(2), true, true);
                    }
                    if (w.startsWith(">")) {
                        return new Range(field, w.substring(1), null, false, true);
                    }
                    if (w.startsWith("<")) {
                        return new Range(field, null, w.substring(1), true, false);
                    }
                    if (w.indexOf('*') >= 0 || w.indexOf('?') >= 0) {
                        return new Wildcard(field, w);
                    }
                    return new Term(field, w, false, 0, fuzzy(t.suffix()));
                }
                case END -> throw malformed("ends where a value should be");
                default -> throw malformed("reads [" + t.text() + "] where a value should be");
            }
        }

        /** One bound of a range: a word, a phrase, or {@code *} for open. */
        private String bound() {
            Token t = take();
            if (t.kind() == Kind.WORD || t.kind() == Kind.PHRASE) {
                return t.text().equals("*") ? null : t.text();
            }
            throw malformed("has a range bound that is not a value: [" + t.text() + "]");
        }

        /** The field pushed down to every leaf of a group: {@code field:(a OR b)}. */
        private Node scoped(Node n, String field) {
            if (n instanceof Term t) {
                return new Term(field, t.text(), t.quoted(), t.slop(), t.fuzzy());
            }
            if (n instanceof Wildcard w) {
                return new Wildcard(field, w.pattern());
            }
            if (n instanceof Regex r) {
                return new Regex(field, r.pattern());
            }
            if (n instanceof Range r) {
                return new Range(field, r.from(), r.to(), r.includeFrom(), r.includeTo());
            }
            if (n instanceof Bool b) {
                return new Bool(scopedAll(b.must(), field), scopedAll(b.should(), field), scopedAll(b.mustNot(), field));
            }
            return n;
        }

        private List<Node> scopedAll(List<Node> nodes, String field) {
            List<Node> out = new ArrayList<>();
            for (Node c : nodes) {
                out.add(scoped(c, field));
            }
            return out;
        }

        private static int slop(String suffix) {
            int tilde = suffix == null ? -1 : suffix.indexOf('~');
            if (tilde < 0) {
                return 0;
            }
            String digits = suffix.substring(tilde + 1).split("\\^")[0];
            return digits.isEmpty() ? 0 : (int) Double.parseDouble(digits);
        }

        private static int fuzzy(String suffix) {
            int tilde = suffix == null ? -1 : suffix.indexOf('~');
            if (tilde < 0) {
                return 0;
            }
            String digits = suffix.substring(tilde + 1).split("\\^")[0];
            return digits.isEmpty() ? 2 : (int) Double.parseDouble(digits);
        }
    }
}
