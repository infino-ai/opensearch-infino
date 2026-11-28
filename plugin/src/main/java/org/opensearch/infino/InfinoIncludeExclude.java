/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import org.apache.lucene.document.InetAddressPoint;
import org.apache.lucene.util.automaton.Automata;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.Operations;
import org.apache.lucene.util.automaton.RegExp;
import org.apache.lucene.util.automaton.TooComplexToDeterminizeException;
import org.opensearch.common.network.InetAddresses;
import org.opensearch.infino.InfinoAggregationProcessor.Unsupported;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.aggregations.AggregationExecutionException;
import org.opensearch.search.aggregations.bucket.terms.IncludeExclude;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.function.UnaryOperator;

import static org.opensearch.infino.InfinoQueryTranslator.literal;

/**
 * A terms aggregation's {@code include} and {@code exclude} as a test over
 * its bucket key, read as stock reads them (its {@code IncludeExclude}), so
 * the level keeps only the keys the test passes, before its top-N cut: a key
 * that fails is no bucket and no part of {@code sum_other_doc_count}, as in
 * stock, where such a term is never collected.
 *
 * <p>Two forms are served. An array of exact values is the key in (or not in)
 * that set, each value read as the field reads it: a keyword's text as
 * written, an ip's address (any way it is written), a number's value, a
 * boolean's truth. A string is a Lucene regular expression over a keyword's
 * whole value, translated to the engine's regular expressions construct by
 * construct. Stock's own automaton for the pattern, built by the Lucene of
 * the running release, is the reference: an include that keeps every string
 * is no test at all, and a translation whose language differs from it (a
 * construct this release's Lucene reads in a way the translation does not,
 * such as {@code ~} on a release where it is a complement) is refused, never
 * approximated. The partitioned form ({@code partition}, {@code num_partitions}) splits
 * terms by a hash the engine has no function for, and is refused.
 */
final class InfinoIncludeExclude {

    private InfinoIncludeExclude() {}

    /**
     * The most characters a pattern may stand for once its repetitions are
     * written out ({@code (a{100}){100}} stands for 10000), a class counting
     * as one character per range it keeps ({@code \W} as five): the engine
     * compiles each repetition as a copy, and past this its compiled pattern
     * nears the engine's size limit.
     */
    static final long MAX_EXPANDED_SIZE = 10_000;

    /** How much work Lucene may spend determinizing a pattern's automaton, as stock's include spends. */
    private static final int DETERMINIZE_WORK_LIMIT = Operations.DEFAULT_DETERMINIZE_WORK_LIMIT;

    /**
     * How deep the translation of a pattern may nest its groups (each
     * repetition is one too): the engine refuses a pattern nested past a
     * limit of its own, which a group and its repetition count against
     * twice, so this keeps well inside it.
     */
    static final int MAX_GROUP_DEPTH = 100;

    /** The most addresses an ip include or exclude may list: each address is a pattern of every way it may be written. */
    static final int MAX_IP_VALUES = 64;

    /** The groups of 16 bits an IPv6 address has. */
    private static final int IPV6_GROUPS = 8;

    /** The hex digits a group of an IPv6 address is written with, at most. */
    private static final int MAX_GROUP_DIGITS = 4;

    /** The first group of the last two, which an IPv6 address may write as an IPv4 address's four octets. */
    private static final int IPV4_TAIL_GROUP = 6;

    /** The group holding {@code ffff} in an IPv4-mapped IPv6 address ({@code ::ffff:a.b.c.d}). */
    private static final int MAPPED_MARKER_GROUP = 5;

    /** A group of all ones: an IPv4-mapped address's marker group. */
    private static final int ALL_ONES_GROUP = 0xffff;

    /** The largest Unicode code point. */
    private static final int MAX_CODE_POINT = Character.MAX_CODE_POINT;

    /** How a level's key is read when an include or exclude value is compared to it. */
    enum KeyType {
        KEYWORD,
        IP,
        LONG,
        DOUBLE,
        BOOLEAN,
        DATE
    }

    /**
     * What a terms level's include/exclude makes of its key. {@code test} is a
     * SQL predicate over a key expression, true where the key is kept; null
     * when every key is kept, so the level reads as one with no
     * include/exclude. {@code strings} is stock's own filter over a keyword
     * key's bytes, for keys read outside a statement; null for keys of other
     * types.
     */
    record KeyFilter(UnaryOperator<String> test, IncludeExclude.StringFilter strings) {
        /** Every key kept: no test at all. */
        static final KeyFilter ALL = new KeyFilter(null, null);
    }

    /**
     * What {@code body}'s include and exclude make of a key of {@code type},
     * for aggregation {@code agg} ({@link KeyFilter}). {@code body} is the
     * terms aggregation's own JSON body.
     */
    static KeyFilter keyTest(String agg, Map<String, Object> body, KeyType type) {
        Object include = body.get("include");
        Object exclude = body.get("exclude");
        if (include instanceof Map<?, ?>) {
            throw new Unsupported(
                "aggregation [" + agg + "] has an include with partition and num_partitions, which splits terms by a hash the table cannot compute; it is not served"
            );
        }
        if (include instanceof String || exclude instanceof String) {
            if (type != KeyType.KEYWORD) {
                // Stock's own exception, so its status (500) too.
                throw new AggregationExecutionException(regexOnNonString(agg, type));
            }
            return regexTest(agg, (String) include, (String) exclude);
        }
        List<?> in = (List<?>) include;
        List<?> out = (List<?>) exclude;
        // As stock: an empty list of numbers, booleans or dates is no list at
        // all (its long filter keeps every value), while an empty include of
        // strings or addresses keeps no term. An empty exclude drops none.
        if (in != null && in.isEmpty() && type != KeyType.KEYWORD && type != KeyType.IP) {
            in = null;
        }
        if (out != null && out.isEmpty()) {
            out = null;
        }
        if (in == null && out == null) {
            return KeyFilter.ALL;
        }
        if (type == KeyType.DATE) {
            throw new Unsupported("aggregation [" + agg + "] has include/exclude values on a date field, which are not served");
        }
        UnaryOperator<String> inTest = in == null ? null : valueTest(agg, in, type);
        UnaryOperator<String> outTest = out == null ? null : valueTest(agg, out, type);
        IncludeExclude.StringFilter strings = type == KeyType.KEYWORD
            ? new IncludeExclude(texts(in), texts(out)).convertToStringFilter(DocValueFormat.RAW)
            : null;
        return new KeyFilter(key -> {
            List<String> ands = new ArrayList<>();
            if (inTest != null) {
                ands.add(inTest.apply(key));
            }
            if (outTest != null) {
                ands.add("NOT " + outTest.apply(key));
            }
            return "(" + String.join(" AND ", ands) + ")";
        }, strings);
    }

    /** {@code values} as text, as stock reads an include or exclude value; null for none. */
    private static String[] texts(List<?> values) {
        if (values == null) {
            return null;
        }
        String[] out = new String[values.size()];
        for (int i = 0; i < out.length; i++) {
            out[i] = String.valueOf(values.get(i));
        }
        return out;
    }

    /**
     * A regular expression include and exclude over a keyword key. Stock's
     * automaton for them, built as stock builds it by this release's Lucene,
     * decides: a pattern it refuses is refused with its own exception, an
     * include that keeps every string is no test, and the translation is used
     * only where its language is exactly that automaton's.
     */
    private static KeyFilter regexTest(String agg, String include, String exclude) {
        Automaton stock;
        try {
            stock = stockAutomaton(include, exclude);
        } catch (TooComplexToDeterminizeException e) {
            throw new Unsupported("aggregation [" + agg + "] has an include/exclude too complex to read: " + e.getMessage());
        }
        if (Operations.isEmpty(Operations.minus(Automata.makeAnyString(), stock, DETERMINIZE_WORK_LIMIT))) {
            // Dashboards' suggestion request for an empty box (".*"): every key is kept.
            return KeyFilter.ALL;
        }
        Reading in = include == null ? null : read(include);
        Reading out = exclude == null ? null : read(exclude);
        Automaton ours = in == null ? Automata.makeAnyString() : in.automaton();
        try {
            if (out != null) {
                ours = Operations.minus(ours, out.automaton(), DETERMINIZE_WORK_LIMIT);
            }
            if (sameLanguage(ours, stock) == false) {
                throw new Unsupported(
                    "aggregation ["
                        + agg
                        + "] has an include/exclude regular expression that this release's Lucene reads differently from the table's"
                        + " regular expressions (such as ~ as a complement, or a range from an escaped character in a class), which is not served"
                );
            }
        } catch (TooComplexToDeterminizeException e) {
            throw new Unsupported("aggregation [" + agg + "] has an include/exclude too complex to read: " + e.getMessage());
        }
        String inc = in == null ? null : in.anchored();
        String exc = out == null ? null : out.anchored();
        IncludeExclude.StringFilter strings = new IncludeExclude(include, exclude).convertToStringFilter(DocValueFormat.RAW, 0);
        return new KeyFilter(key -> {
            List<String> ands = new ArrayList<>();
            if (inc != null) {
                ands.add("regexp_like(" + key + ", " + literal(inc) + ")");
            }
            if (exc != null) {
                ands.add("NOT regexp_like(" + key + ", " + literal(exc) + ")");
            }
            return "(" + String.join(" AND ", ands) + ")";
        }, strings);
    }

    /**
     * Stock's automaton for an include and exclude pattern, as its
     * {@code IncludeExclude} builds it: the include (every string when there
     * is none) less the exclude, determinized.
     */
    static Automaton stockAutomaton(String include, String exclude) {
        Automaton a = include == null ? Automata.makeAnyString() : new RegExp(include).toAutomaton();
        if (exclude != null) {
            a = Operations.minus(a, new RegExp(exclude).toAutomaton(), DETERMINIZE_WORK_LIMIT);
        }
        return a.isDeterministic() ? a : Operations.determinize(a, DETERMINIZE_WORK_LIMIT);
    }

    /** Whether two automata accept the same strings. */
    static boolean sameLanguage(Automaton a, Automaton b) {
        return Operations.isEmpty(Operations.minus(a, b, DETERMINIZE_WORK_LIMIT))
            && Operations.isEmpty(Operations.minus(b, a, DETERMINIZE_WORK_LIMIT));
    }

    /** Stock's refusal of a regular expression on a field whose terms are not strings. */
    static String regexOnNonString(String agg, KeyType type) {
        String use = type == KeyType.IP
            ? "Use an array of values for include/exclude clauses"
            : "Use an array of numeric values for include/exclude clauses used to filter numeric fields";
        return "Aggregation ["
            + agg
            + "] cannot support regular expression style include/exclude settings as they can only be applied to string fields. "
            + use;
    }

    /**
     * Whether a key is one of {@code values}, each read as stock reads an
     * include or exclude value on a field of {@code type}: a malformed one is
     * refused with stock's own exception (a number that does not parse, an ip
     * that is not an address, a boolean that is neither true nor false).
     */
    private static UnaryOperator<String> valueTest(String agg, List<?> values, KeyType type) {
        if (values.isEmpty()) {
            return key -> "FALSE";
        }
        List<String> texts = new ArrayList<>(values.size());
        for (Object v : values) {
            texts.add(String.valueOf(v));
        }
        if (type == KeyType.IP) {
            if (texts.size() > MAX_IP_VALUES) {
                throw new Unsupported("aggregation [" + agg + "] lists " + texts.size() + " addresses to include or exclude; at most " + MAX_IP_VALUES + " are served");
            }
            List<String> forms = new ArrayList<>();
            for (String text : texts) {
                forms.add(ipForms(InetAddresses.forString(text)));
            }
            String pattern = literal("^(?:" + String.join("|", forms) + ")$");
            return key -> "regexp_like(" + key + ", " + pattern + ")";
        }
        List<String> lits = new ArrayList<>(texts.size());
        for (String text : texts) {
            lits.add(switch (type) {
                case KEYWORD -> literal(text);
                case LONG -> Long.toString(parseLong(text));
                case DOUBLE -> doubleLiteral(agg, Double.parseDouble(text));
                case BOOLEAN -> Boolean.toString(parseBoolean(text));
                default -> throw new IllegalStateException("no values of " + type);
            });
        }
        String list = String.join(", ", lits);
        // A double key is compared as a double, as stock compares it, whatever width the column keeps it in.
        return type == KeyType.DOUBLE ? key -> "CAST(" + key + " AS DOUBLE) IN (" + list + ")" : key -> key + " IN (" + list + ")";
    }

    /** A long field's include or exclude value as stock's raw format reads it: a long, else a number rounded down. */
    static long parseLong(String text) {
        try {
            return Long.parseLong(text);
        } catch (NumberFormatException e) {
            // As stock: retried as a double, rounded down.
        }
        return Math.round(Math.floor(Double.parseDouble(text)));
    }

    /** A boolean field's include or exclude value as stock's boolean format reads it. */
    static boolean parseBoolean(String text) {
        return switch (text) {
            case "true" -> true;
            case "false" -> false;
            default -> throw new IllegalArgumentException("Cannot parse boolean [" + text + "], expected either [true] or [false]");
        };
    }

    private static String doubleLiteral(String agg, double d) {
        if (Double.isFinite(d) == false) {
            throw new Unsupported("aggregation [" + agg + "] has the include/exclude value [" + d + "], which is not served");
        }
        return Double.toString(d);
    }

    // ---- an ip address, every way it may be written ----

    /**
     * A pattern matching every text an ip column may hold for {@code address}:
     * the column keeps each address as written, which the bridge accepts only
     * as Rust's address parser reads it, while stock compares the addresses.
     * An IPv4 address is four decimal octets with no leading zeros, and is the
     * same address as its IPv4-mapped IPv6 form ({@code ::ffff:a.b.c.d}). An
     * IPv6 address is eight groups of one to four hex digits in either case,
     * leading zeros allowed, any one run of zero groups written as
     * {@code ::}, and its last two groups written as an IPv4 address or not.
     */
    static String ipForms(InetAddress address) {
        byte[] bytes = InetAddressPoint.encode(address);
        int[] groups = new int[IPV6_GROUPS];
        for (int i = 0; i < IPV6_GROUPS; i++) {
            groups[i] = (bytes[2 * i] & 0xff) << 8 | bytes[2 * i + 1] & 0xff;
        }
        List<String> forms = new ArrayList<>();
        boolean mapped = groups[MAPPED_MARKER_GROUP] == ALL_ONES_GROUP;
        for (int i = 0; i < MAPPED_MARKER_GROUP && mapped; i++) {
            mapped = groups[i] == 0;
        }
        String dotted = dotted(bytes);
        if (mapped) {
            forms.add(dotted);
        }
        // No group left out: eight hex groups, or six and the IPv4 tail.
        forms.add(hexGroups(groups, 0, IPV6_GROUPS));
        forms.add(hexGroups(groups, 0, IPV4_TAIL_GROUP) + ":" + dotted);
        // A run of zero groups from `start`, `length` long, written `::`.
        for (int start = 0; start < IPV6_GROUPS; start++) {
            for (int end = start + 1; end <= IPV6_GROUPS && groups[end - 1] == 0; end++) {
                String head = hexGroups(groups, 0, start);
                forms.add(head + "::" + hexGroups(groups, end, IPV6_GROUPS));
                if (end <= IPV4_TAIL_GROUP) {
                    String tail = hexGroups(groups, end, IPV4_TAIL_GROUP);
                    forms.add(head + "::" + (tail.isEmpty() ? "" : tail + ":") + dotted);
                }
            }
        }
        return String.join("|", forms);
    }

    /** Groups {@code from} (inclusive) to {@code to} (exclusive), each as its hex digits may be written, joined by colons. */
    private static String hexGroups(int[] groups, int from, int to) {
        List<String> out = new ArrayList<>();
        for (int i = from; i < to; i++) {
            out.add(hexGroup(groups[i]));
        }
        return String.join(":", out);
    }

    /** One group: its hex digits in either case after any leading zeros, at most four digits in all. */
    private static String hexGroup(int group) {
        if (group == 0) {
            return "0{1," + MAX_GROUP_DIGITS + "}";
        }
        String hex = Integer.toHexString(group);
        StringBuilder out = new StringBuilder();
        if (hex.length() < MAX_GROUP_DIGITS) {
            out.append("0{0,").append(MAX_GROUP_DIGITS - hex.length()).append('}');
        }
        for (char ch : hex.toCharArray()) {
            if (Character.isLetter(ch)) {
                out.append('[').append(ch).append(Character.toUpperCase(ch)).append(']');
            } else {
                out.append(ch);
            }
        }
        return out.toString();
    }

    /** The last four bytes as an IPv4 address's octets, the dots escaped. */
    private static String dotted(byte[] bytes) {
        int at = bytes.length - Integer.BYTES;
        return (bytes[at] & 0xff) + "\\." + (bytes[at + 1] & 0xff) + "\\." + (bytes[at + 2] & 0xff) + "\\." + (bytes[at + 3] & 0xff);
    }

    // ---- a Lucene regular expression in the engine's dialect ----

    /**
     * A Lucene regular expression as the translation reads it: the engine's
     * pattern for it, anchored at both ends, and the automaton of the strings
     * that pattern matches, to compare with Lucene's own reading.
     */
    record Reading(String anchored, Automaton automaton) {}

    /** {@code lucene} as the translation reads it ({@link Reading}); a construct with no counterpart is refused by name. */
    static Reading read(String lucene) {
        Node node;
        try {
            node = new LuceneRegex(lucene).translate();
        } catch (Unsupported e) {
            throw e;
        } catch (IllegalArgumentException e) {
            // Only reached for a pattern this release's Lucene reads (stock's own reading refuses the rest first).
            throw new Unsupported("the regular expression [" + lucene + "] cannot be read as this release's Lucene reads it (" + e.getMessage() + "), which is not served");
        }
        // Every character the translation writes is escaped, so a bare parenthesis is one of its groups.
        int depth = 0;
        for (int i = 0; i < node.re().length(); i++) {
            char ch = node.re().charAt(i);
            depth += ch == '(' ? 1 : 0;
            if (depth > MAX_GROUP_DEPTH) {
                throw new Unsupported("the regular expression [" + lucene + "] nests groups and repetitions more than " + MAX_GROUP_DEPTH + " deep, which is not served");
            }
            depth -= ch == ')' ? 1 : 0;
        }
        return new Reading("^(?:" + node.re() + ")$", node.automaton());
    }

    /** A Lucene regular expression over a whole value, as the engine's pattern for the same strings, anchored at both ends. */
    static String anchored(String lucene) {
        return read(lucene).anchored();
    }

    /**
     * One construct of a pattern: the engine's pattern for it ({@code re}), the
     * automaton of what that matches, and {@code size}, how many characters,
     * classes and wildcards it stands for once its repetitions are written out.
     */
    private record Node(String re, Automaton automaton, long size) {}

    /**
     * A recursive-descent reading of Lucene's {@code RegExp} syntax with its
     * default flags (as {@code IncludeExclude} builds it), mirroring the newer
     * Lucene parser step for step, quirks included: an operator character
     * where no operator can stand is that character ({@code *a} is the text
     * {@code *a}), {@code ^} and {@code $} are plain characters, and {@code \}
     * before a character other than a letter or backslash is that character.
     * It writes the same language in the engine's syntax: every character as
     * a code-point escape, every group non-capturing, {@code .} matching any
     * character, newlines included, as Lucene's does. Intersection
     * ({@code &}), the empty language ({@code #}) and named automata or
     * numeric intervals ({@code <...>}) have no counterpart there and are
     * refused. Where a release's Lucene reads a pattern otherwise (an older
     * one reads {@code ~} as a complement), the automaton this builds differs
     * from Lucene's and {@link #regexTest} refuses the pattern.
     */
    static final class LuceneRegex {
        private final String s;
        private int pos;

        LuceneRegex(String s) {
            this.s = s;
        }

        Node translate() {
            if (s.isEmpty()) {
                return new Node("", Automata.makeEmptyString(), 0);
            }
            Node e = union();
            if (pos < s.length()) {
                throw new IllegalArgumentException("end-of-string expected at position " + pos);
            }
            return e;
        }

        private Node union() {
            List<Node> alts = new ArrayList<>();
            alts.add(inter());
            while (match('|')) {
                alts.add(inter());
            }
            if (alts.size() == 1) {
                return alts.get(0);
            }
            List<String> res = new ArrayList<>();
            List<Automaton> automata = new ArrayList<>();
            long size = 0;
            for (Node alt : alts) {
                res.add(alt.re());
                automata.add(alt.automaton());
                size += alt.size();
            }
            return node("(?:" + String.join("|", res) + ")", Operations.union(automata), size);
        }

        private Node inter() {
            Node e = concat();
            if (match('&')) {
                throw new Unsupported("the regular expression [" + s + "] has an intersection (&), which is not served");
            }
            return e;
        }

        private Node concat() {
            List<Node> parts = new ArrayList<>();
            parts.add(repeat());
            while (more() && !peek(")|&")) {
                parts.add(repeat());
            }
            if (parts.size() == 1) {
                return parts.get(0);
            }
            StringBuilder re = new StringBuilder();
            List<Automaton> automata = new ArrayList<>();
            long size = 0;
            for (Node part : parts) {
                re.append(part.re());
                automata.add(part.automaton());
                size += part.size();
            }
            return node(re.toString(), Operations.concatenate(automata), size);
        }

        private Node repeat() {
            Node e = charClassExp();
            while (peek("?*+{")) {
                if (match('?')) {
                    e = node("(?:" + e.re() + ")?", Operations.optional(e.automaton()), e.size());
                } else if (match('*')) {
                    e = node("(?:" + e.re() + ")*", Operations.repeat(e.automaton()), e.size());
                } else if (match('+')) {
                    e = node("(?:" + e.re() + ")+", Operations.repeat(e.automaton(), 1), e.size());
                } else if (match('{')) {
                    int n = count();
                    int m;
                    if (match(',')) {
                        m = peek("0123456789") ? count() : -1;
                    } else {
                        m = n;
                    }
                    if (!match('}')) {
                        throw new IllegalArgumentException("expected '}' at position " + pos);
                    }
                    if (m != -1 && n > m) {
                        throw new IllegalArgumentException("invalid repetition range(out of order): " + n + ".." + m);
                    }
                    // Checked before the automaton is built: the engine writes {n,} as n copies and a star, {n,m} as m copies.
                    long size = e.size() * (m == -1 ? n + 1L : m);
                    checkSize(size);
                    Automaton a = m == -1 ? Operations.repeat(e.automaton(), n) : Operations.repeat(e.automaton(), n, m);
                    e = node("(?:" + e.re() + "){" + n + (m == n ? "" : m == -1 ? "," : "," + m) + "}", a, size);
                }
            }
            return e;
        }

        /** The digits of a repetition count. */
        private int count() {
            int start = pos;
            while (peek("0123456789")) {
                next();
            }
            if (start == pos) {
                throw new IllegalArgumentException("integer expected at position " + pos);
            }
            String digits = s.substring(start, pos);
            // Compared as text first: a count past an int is past the limit too.
            if (digits.length() > Long.toString(MAX_EXPANDED_SIZE).length() || Integer.parseInt(digits) > MAX_EXPANDED_SIZE) {
                checkSize(MAX_EXPANDED_SIZE + 1);
            }
            return Integer.parseInt(digits);
        }

        private void checkSize(long size) {
            if (size > MAX_EXPANDED_SIZE) {
                throw new Unsupported(
                    "the regular expression ["
                        + s
                        + "] stands for more than "
                        + MAX_EXPANDED_SIZE
                        + " characters once its repetitions are written out, which is not served"
                );
            }
        }

        /** A construct, its size checked against {@link #MAX_EXPANDED_SIZE}. */
        private Node node(String re, Automaton automaton, long size) {
            checkSize(size);
            return new Node(re, automaton, size);
        }

        private Node charClassExp() {
            if (!match('[')) {
                return simple();
            }
            boolean negate = match('^');
            List<int[]> ranges = new ArrayList<>();
            do {
                if (match('\\')) {
                    if (peek("\\ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz")) {
                        predefined(ranges);
                    } else {
                        int c = next();
                        ranges.add(new int[] { c, c });
                    }
                } else {
                    int c = charExp();
                    if (match('-')) {
                        ranges.add(range(c, charExp()));
                    } else {
                        ranges.add(new int[] { c, c });
                    }
                }
            } while (more() && !peek("]"));
            if (!match(']')) {
                throw new IllegalArgumentException("expected ']' at position " + pos);
            }
            return charClass(ranges, negate);
        }

        private static int[] range(int from, int to) {
            if (from > to) {
                throw new IllegalArgumentException("invalid range: from (" + from + ") cannot be > to (" + to + ")");
            }
            return new int[] { from, to };
        }

        /** Lucene's predefined classes after a backslash ({@code \d \D \s \S \w \W}, and {@code \\}); any other letter is an error. */
        private void predefined(List<int[]> ranges) {
            int c = next();
            switch (c) {
                case '\\' -> ranges.add(new int[] { '\\', '\\' });
                case 'd' -> ranges.add(new int[] { '0', '9' });
                case 'D' -> {
                    ranges.add(new int[] { Character.MIN_CODE_POINT, '0' - 1 });
                    ranges.add(new int[] { '9' + 1, MAX_CODE_POINT });
                }
                case 's' -> {
                    ranges.add(new int[] { '\t', '\n' });
                    ranges.add(new int[] { '\r', '\r' });
                    ranges.add(new int[] { ' ', ' ' });
                }
                case 'S' -> {
                    ranges.add(new int[] { Character.MIN_CODE_POINT, '\t' - 1 });
                    ranges.add(new int[] { '\n' + 1, '\r' - 1 });
                    ranges.add(new int[] { '\r' + 1, ' ' - 1 });
                    ranges.add(new int[] { ' ' + 1, MAX_CODE_POINT });
                }
                case 'w' -> {
                    ranges.add(new int[] { '0', '9' });
                    ranges.add(new int[] { 'A', 'Z' });
                    ranges.add(new int[] { '_', '_' });
                    ranges.add(new int[] { 'a', 'z' });
                }
                case 'W' -> {
                    ranges.add(new int[] { Character.MIN_CODE_POINT, '0' - 1 });
                    ranges.add(new int[] { '9' + 1, 'A' - 1 });
                    ranges.add(new int[] { 'Z' + 1, '_' - 1 });
                    ranges.add(new int[] { '_' + 1, 'a' - 1 });
                    ranges.add(new int[] { 'z' + 1, MAX_CODE_POINT });
                }
                default -> throw new IllegalArgumentException("invalid character class \\" + Character.toString(c));
            }
        }

        private Node simple() {
            if (match('.')) {
                return node("(?s:.)", Automata.makeAnyChar(), 1);
            }
            if (match('#')) {
                throw new Unsupported("the regular expression [" + s + "] has the empty language (#), which is not served");
            }
            if (match('@')) {
                return node("(?s:.*)", Automata.makeAnyString(), 1);
            }
            if (match('"')) {
                StringBuilder re = new StringBuilder();
                StringBuilder text = new StringBuilder();
                while (more() && !peek("\"")) {
                    int c = next();
                    re.append(escape(c));
                    text.appendCodePoint(c);
                }
                if (!match('"')) {
                    throw new IllegalArgumentException("expected '\"' at position " + pos);
                }
                return node("(?:" + re + ")", Automata.makeString(text.toString()), text.codePointCount(0, text.length()));
            }
            if (match('(')) {
                if (match(')')) {
                    return node("(?:)", Automata.makeEmptyString(), 0);
                }
                Node e = union();
                if (!match(')')) {
                    throw new IllegalArgumentException("expected ')' at position " + pos);
                }
                return node("(?:" + e.re() + ")", e.automaton(), e.size());
            }
            if (match('<')) {
                throw new Unsupported("the regular expression [" + s + "] has a named automaton or numeric interval (<...>), which is not served");
            }
            // As Lucene: the backslash is taken here whatever follows it; only a
            // letter or a backslash after it is a predefined class.
            if (match('\\') && peek("\\ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz")) {
                List<int[]> ranges = new ArrayList<>();
                predefined(ranges);
                return charClass(ranges, false);
            }
            int c = charExp();
            return node(escape(c), Automata.makeChar(c), 1);
        }

        private int charExp() {
            match('\\');
            return next();
        }

        private Node charClass(List<int[]> ranges, boolean negate) {
            StringBuilder out = new StringBuilder("[");
            if (negate) {
                out.append('^');
            }
            for (int[] r : ranges) {
                out.append(escape(r[0]));
                if (r[1] != r[0]) {
                    out.append('-').append(escape(r[1]));
                }
            }
            List<int[]> kept = negate ? complement(ranges) : ranges;
            List<Automaton> automata = new ArrayList<>();
            for (int[] r : kept) {
                automata.add(Automata.makeCharRange(r[0], r[1]));
            }
            // A class compiles to a branch per range it keeps, so it counts as that many characters.
            return node(out.append(']').toString(), automata.isEmpty() ? Automata.makeEmpty() : Operations.union(automata), Math.max(1, kept.size()));
        }

        /** The code points none of {@code ranges} holds, as ranges. */
        private static List<int[]> complement(List<int[]> ranges) {
            List<int[]> sorted = new ArrayList<>(ranges);
            sorted.sort(Comparator.comparingInt(r -> r[0]));
            List<int[]> out = new ArrayList<>();
            int next = Character.MIN_CODE_POINT;
            for (int[] r : sorted) {
                if (r[0] > next) {
                    out.add(new int[] { next, r[0] - 1 });
                }
                next = Math.max(next, r[1] + 1);
            }
            if (next <= MAX_CODE_POINT) {
                out.add(new int[] { next, MAX_CODE_POINT });
            }
            return out;
        }

        /** One character as the engine's pattern writes it: a letter or digit as itself, anything else as a code-point escape. */
        private static String escape(int c) {
            if (c < 0x80 && Character.isLetterOrDigit(c)) {
                return Character.toString(c);
            }
            if (c <= Character.MAX_VALUE && Character.isSurrogate((char) c)) {
                throw new Unsupported("a regular expression with an unpaired surrogate is not served");
            }
            return "\\x{" + Integer.toHexString(c) + "}";
        }

        private boolean peek(String chars) {
            return more() && chars.indexOf(s.codePointAt(pos)) != -1;
        }

        private boolean match(int c) {
            if (pos >= s.length()) {
                return false;
            }
            if (s.codePointAt(pos) == c) {
                pos += Character.charCount(c);
                return true;
            }
            return false;
        }

        private boolean more() {
            return pos < s.length();
        }

        private int next() {
            if (!more()) {
                throw new IllegalArgumentException("unexpected end-of-string");
            }
            int ch = s.codePointAt(pos);
            pos += Character.charCount(ch);
            return ch;
        }
    }
}
