/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import org.apache.lucene.document.InetAddressPoint;
import org.opensearch.common.network.InetAddresses;
import org.opensearch.infino.InfinoAggregationProcessor.Unsupported;
import org.opensearch.search.aggregations.AggregationExecutionException;

import java.net.InetAddress;
import java.util.ArrayList;
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
 * construct; one it cannot translate exactly is refused, never approximated.
 * The partitioned form ({@code partition}, {@code num_partitions}) splits
 * terms by a hash the engine has no function for, and is refused.
 */
final class InfinoIncludeExclude {

    private InfinoIncludeExclude() {}

    /** Stock's default {@code index.max_regex_length}, the longest include or exclude pattern it reads. */
    static final int MAX_REGEX_LENGTH = 1000;

    /** The largest count a pattern's {@code {n,m}} repetition may give; past it the engine's compiled pattern grows too large. */
    static final int MAX_REPEAT = 1000;

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
     * The test {@code body}'s include and exclude make of a key expression of
     * {@code type}, for aggregation {@code agg}: a SQL predicate true where the
     * key is kept. {@code body} is the terms aggregation's own JSON body.
     */
    static UnaryOperator<String> keyTest(String agg, Map<String, Object> body, KeyType type) {
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
            String inc = include == null ? null : anchored((String) include);
            String exc = exclude == null ? null : anchored((String) exclude);
            return key -> {
                List<String> ands = new ArrayList<>();
                if (inc != null) {
                    ands.add("regexp_like(" + key + ", " + literal(inc) + ")");
                }
                if (exc != null) {
                    ands.add("NOT regexp_like(" + key + ", " + literal(exc) + ")");
                }
                return "(" + String.join(" AND ", ands) + ")";
            };
        }
        if (type == KeyType.DATE) {
            throw new Unsupported("aggregation [" + agg + "] has include/exclude values on a date field, which are not served");
        }
        UnaryOperator<String> in = include == null ? null : valueTest(agg, (List<?>) include, type);
        UnaryOperator<String> out = exclude == null ? null : valueTest(agg, (List<?>) exclude, type);
        return key -> {
            List<String> ands = new ArrayList<>();
            if (in != null) {
                ands.add(in.apply(key));
            }
            if (out != null) {
                ands.add("NOT " + out.apply(key));
            }
            return "(" + String.join(" AND ", ands) + ")";
        };
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

    /** A Lucene regular expression over a whole value, as the engine's pattern for the same strings, anchored at both ends. */
    static String anchored(String lucene) {
        if (lucene.length() > MAX_REGEX_LENGTH) {
            // Stock's own refusal, at its default limit.
            throw new IllegalArgumentException(
                "The length of regex ["
                    + lucene.length()
                    + "] used in the request has exceeded the allowed maximum of ["
                    + MAX_REGEX_LENGTH
                    + "]. This maximum can be set by changing the [index.max_regex_length] index level setting."
            );
        }
        String translated = new LuceneRegex(lucene).translate();
        // Every character the translation writes is escaped, so a bare parenthesis is one of its groups.
        int depth = 0;
        for (int i = 0; i < translated.length(); i++) {
            char ch = translated.charAt(i);
            depth += ch == '(' ? 1 : 0;
            if (depth > MAX_GROUP_DEPTH) {
                throw new Unsupported("the regular expression [" + lucene + "] nests groups and repetitions more than " + MAX_GROUP_DEPTH + " deep, which is not served");
            }
            depth -= ch == ')' ? 1 : 0;
        }
        return "^(?:" + translated + ")$";
    }

    /**
     * A recursive-descent reading of Lucene's {@code RegExp} syntax with its
     * default flags (as {@code IncludeExclude} builds it), mirroring Lucene's
     * parser step for step, so a pattern reads as Lucene reads it, quirks
     * included: an operator character where no operator can stand is that
     * character ({@code *a} is the text {@code *a}), {@code ^} and {@code $}
     * are plain characters, and {@code \} before a character other than a
     * letter or backslash is that character. It writes the same language in
     * the engine's syntax: every character as a code-point escape, every
     * group non-capturing, {@code .} matching any character, newlines
     * included, as Lucene's does. Intersection ({@code &}), the empty
     * language ({@code #}) and named automata or numeric intervals
     * ({@code <...>}) have no counterpart there and are refused; a malformed
     * pattern is refused with Lucene's own message.
     */
    static final class LuceneRegex {
        private final String s;
        private int pos;

        LuceneRegex(String s) {
            this.s = s;
        }

        String translate() {
            if (s.isEmpty()) {
                return "";
            }
            String e = union();
            if (pos < s.length()) {
                throw new IllegalArgumentException("end-of-string expected at position " + pos);
            }
            return e;
        }

        private String union() {
            List<String> alts = new ArrayList<>();
            alts.add(inter());
            while (match('|')) {
                alts.add(inter());
            }
            return alts.size() == 1 ? alts.get(0) : "(?:" + String.join("|", alts) + ")";
        }

        private String inter() {
            String e = concat();
            if (match('&')) {
                throw new Unsupported("the regular expression [" + s + "] has an intersection (&), which is not served");
            }
            return e;
        }

        private String concat() {
            StringBuilder out = new StringBuilder(repeat());
            while (more() && !peek(")|&")) {
                out.append(repeat());
            }
            return out.toString();
        }

        private String repeat() {
            String e = charClassExp();
            while (peek("?*+{")) {
                if (match('?')) {
                    e = "(?:" + e + ")?";
                } else if (match('*')) {
                    e = "(?:" + e + ")*";
                } else if (match('+')) {
                    e = "(?:" + e + ")+";
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
                    e = "(?:" + e + "){" + n + (m == n ? "" : m == -1 ? "," : "," + m) + "}";
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
            if (digits.length() > Integer.toString(MAX_REPEAT).length() || Integer.parseInt(digits) > MAX_REPEAT) {
                throw new Unsupported("the regular expression [" + s + "] repeats more than " + MAX_REPEAT + " times, which is not served");
            }
            return Integer.parseInt(digits);
        }

        private String charClassExp() {
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

        private String simple() {
            if (match('.')) {
                return "(?s:.)";
            }
            if (match('#')) {
                throw new Unsupported("the regular expression [" + s + "] has the empty language (#), which is not served");
            }
            if (match('@')) {
                return "(?s:.*)";
            }
            if (match('"')) {
                StringBuilder out = new StringBuilder();
                while (more() && !peek("\"")) {
                    out.append(escape(next()));
                }
                if (!match('"')) {
                    throw new IllegalArgumentException("expected '\"' at position " + pos);
                }
                return "(?:" + out + ")";
            }
            if (match('(')) {
                if (match(')')) {
                    return "(?:)";
                }
                String e = union();
                if (!match(')')) {
                    throw new IllegalArgumentException("expected ')' at position " + pos);
                }
                return "(?:" + e + ")";
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
            return escape(charExp());
        }

        private int charExp() {
            match('\\');
            return next();
        }

        private static String charClass(List<int[]> ranges, boolean negate) {
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
            return out.append(']').toString();
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
