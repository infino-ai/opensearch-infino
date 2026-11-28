/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import org.apache.lucene.document.InetAddressPoint;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.automaton.CharacterRunAutomaton;
import org.apache.lucene.util.automaton.Operations;
import org.apache.lucene.util.automaton.RegExp;
import org.opensearch.common.network.InetAddresses;
import org.opensearch.infino.InfinoAggregationProcessor.Unsupported;
import org.opensearch.infino.InfinoIncludeExclude.KeyFilter;
import org.opensearch.infino.InfinoIncludeExclude.KeyType;
import org.opensearch.search.aggregations.AggregationExecutionException;
import org.opensearch.test.OpenSearchTestCase;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.UnaryOperator;
import java.util.regex.Pattern;

public class InfinoIncludeExcludeTests extends OpenSearchTestCase {

    /** Patterns read the same by Lucene and by the translation, quirks included. */
    private static final List<String> PATTERNS = List.of(
        "",
        ".*",
        "ap.*",
        "a\\.b.*",
        "err.*|warn",
        "a|b|c",
        "(ab)+c?",
        "[a-c]x",
        "[^a-c]x",
        "[]a]",
        "[\\]]",
        "[a\\-z]",
        "a{2}",
        "a{1,3}",
        "a{2,}",
        "a{0}",
        "a**",
        "a*?",
        "a+?b",
        "*a",
        "|a",
        "a||b",
        "(|a)",
        "()",
        "a()b",
        "^a$",
        "\\^",
        "\\d+",
        "[\\d.]+",
        "\\w\\W\\s\\S\\D",
        "@",
        "a@",
        "\"a.b\"c",
        "~a",
        "{",
        "}",
        "]",
        "a{1",
        "[ab",
        "(a",
        "a)",
        "a|",
        "\\",
        "\\q",
        "[b-a]",
        "a{3,1}",
        ".",
        "é.",
        "[\\\\]",
        "\\\\.*"
    );

    /** Strings the patterns are matched against. */
    private static final List<String> SUBJECTS = List.of(
        "",
        "a",
        "b",
        "c",
        "ab",
        "abc",
        "ababc",
        "aa",
        "aaa",
        "aaaa",
        "api",
        "apigw",
        "ap",
        "xap",
        "a.b",
        "a.bc",
        "axb",
        "err",
        "error",
        "warn",
        "warning",
        "ax",
        "bx",
        "dx",
        "]",
        "-",
        "z",
        "^a$",
        "^",
        "123",
        "1.2",
        "a_ \tZ",
        "a\n",
        "\n",
        "a.bc",
        "~a",
        "|a",
        "*a",
        "{",
        "}",
        "é",
        "éa",
        "\\",
        "\\x",
        "😀"
    );

    private static boolean lucene(String pattern, String subject) {
        return new CharacterRunAutomaton(Operations.determinize(new RegExp(pattern).toAutomaton(), Operations.DEFAULT_DETERMINIZE_WORK_LIMIT)).run(
            subject
        );
    }

    /** Whether Lucene reads {@code pattern}, as stock's include does; false where it refuses it. */
    private static boolean luceneReads(String pattern) {
        try {
            new RegExp(pattern).toAutomaton();
            return true;
        } catch (IllegalArgumentException e) {
            return false;
        }
    }

    private static void assertSameLanguage(String pattern, List<String> subjects) {
        if (luceneReads(pattern) == false) {
            // Lucene refuses it, so stock answers a 400; the translation refuses it too.
            expectThrows(IllegalArgumentException.class, () -> InfinoIncludeExclude.anchored(pattern));
            return;
        }
        String translated;
        try {
            translated = InfinoIncludeExclude.anchored(pattern);
        } catch (Unsupported e) {
            return; // a construct the engine's patterns have no counterpart for, refused by name
        }
        // The engine's dialect and Java's agree on everything the translation writes.
        Pattern p = Pattern.compile(translated);
        for (String subject : subjects) {
            assertEquals("[" + pattern + "] as [" + translated + "] on [" + subject + "]", lucene(pattern, subject), p.matcher(subject).matches());
        }
        // The key test serves it, with stock's own filter keeping what Lucene keeps.
        KeyFilter f = filter(Map.of("include", pattern), KeyType.KEYWORD);
        for (String subject : subjects) {
            boolean kept = f.test() == null || f.strings().accept(new BytesRef(subject));
            assertEquals("[" + pattern + "] on [" + subject + "]", lucene(pattern, subject), kept);
        }
    }

    public void testRegularExpressionsMatchWhatLuceneMatches() {
        for (String pattern : PATTERNS) {
            assertSameLanguage(pattern, SUBJECTS);
        }
    }

    public void testDashboardsSuggestionPatterns() {
        String suggest = InfinoIncludeExclude.anchored("ap.*");
        assertTrue(Pattern.compile(suggest).matcher("api").matches());
        assertTrue(Pattern.compile(suggest).matcher("apigw").matches());
        assertFalse(Pattern.compile(suggest).matcher("xapi").matches());
        assertTrue(Pattern.compile(InfinoIncludeExclude.anchored(".*")).matcher("anything\nat all").matches());
    }

    public void testRandomRegularExpressionsMatchWhatLuceneMatches() {
        String alphabet = "ab.*+?|()[]^-{},\\\"@~d10";
        for (int i = 0; i < 2000; i++) {
            StringBuilder pattern = new StringBuilder();
            int length = between(1, 8);
            for (int j = 0; j < length; j++) {
                pattern.append(alphabet.charAt(between(0, alphabet.length() - 1)));
            }
            List<String> subjects = new ArrayList<>(SUBJECTS);
            for (int j = 0; j < 20; j++) {
                subjects.add(randomFrom("a", "b", "ab", "ba", ".", "*", "-", "d", "\"", "]", "")
                    + randomFrom("", "a", "b", "aa", "\n", "^", "@", "~", "ab", "{1}"));
            }
            assertSameLanguage(pattern.toString(), subjects);
        }
    }

    public void testConstructsWithoutACounterpartAreRefusedByName() {
        Unsupported and = expectThrows(Unsupported.class, () -> InfinoIncludeExclude.anchored("a.*&.*b"));
        assertTrue(and.getMessage(), and.getMessage().contains("intersection"));
        expectThrows(Unsupported.class, () -> InfinoIncludeExclude.anchored("#"));
        expectThrows(Unsupported.class, () -> InfinoIncludeExclude.anchored("<1-10>"));
        expectThrows(Unsupported.class, () -> InfinoIncludeExclude.anchored("(".repeat(150) + "a" + ")".repeat(150)));
    }

    public void testRepetitionsAreBoundedByWhatTheyStandForWrittenOut() {
        // Each repetition alone is small; written out they are a million characters.
        Unsupported nested = expectThrows(Unsupported.class, () -> InfinoIncludeExclude.anchored("(a{1000}){1000}"));
        assertTrue(nested.getMessage(), nested.getMessage().contains("once its repetitions are written out"));
        expectThrows(Unsupported.class, () -> InfinoIncludeExclude.anchored("(ab{100}){100}"));
        expectThrows(Unsupported.class, () -> InfinoIncludeExclude.anchored("a{10001}"));
        expectThrows(Unsupported.class, () -> InfinoIncludeExclude.anchored("a{99999999999}"));
        expectThrows(Unsupported.class, () -> InfinoIncludeExclude.anchored("(a{100}){100,}"));
        assertTrue(Pattern.compile(InfinoIncludeExclude.anchored("(a{100}){99}")).matcher("a".repeat(9900)).matches());
        assertTrue(Pattern.compile(InfinoIncludeExclude.anchored("a{1001}")).matcher("a".repeat(1001)).matches());
        // A class counts once per range it keeps: \W keeps five.
        expectThrows(Unsupported.class, () -> InfinoIncludeExclude.anchored("(\\W{100}){100}"));
        expectThrows(Unsupported.class, () -> InfinoIncludeExclude.anchored("[^ac]{5000}"));
        assertTrue(Pattern.compile(InfinoIncludeExclude.anchored("\\W{2000}")).matcher("-".repeat(2000)).matches());
    }

    /**
     * A pattern is served only where this release's Lucene reads it as the
     * translation does. Two readings changed between Lucene releases the
     * plugin builds for: {@code ~} is a complement on Lucene 9 and a plain
     * character on Lucene 10, and an escaped character before {@code -} in a
     * class starts a range up to Lucene 10.1 and is one character after it.
     */
    public void testAPatternThisReleaseReadsOtherwiseIsRefused() {
        boolean complement = lucene("~a", "b");
        if (complement) {
            Unsupported refused = expectThrows(Unsupported.class, () -> filter(Map.of("include", "~(ap.*)"), KeyType.KEYWORD));
            assertTrue(refused.getMessage(), refused.getMessage().contains("reads differently"));
        } else {
            KeyFilter f = filter(Map.of("include", "~(ap.*)"), KeyType.KEYWORD);
            assertTrue(f.strings().accept(new BytesRef("~apple")));
            assertFalse(f.strings().accept(new BytesRef("banana")));
        }
        String escapedRange = "[\\.-_]+";
        if (lucene(escapedRange, "A1")) {
            expectThrows(Unsupported.class, () -> filter(Map.of("include", escapedRange), KeyType.KEYWORD));
        } else {
            KeyFilter f = filter(Map.of("include", escapedRange), KeyType.KEYWORD);
            assertTrue(f.strings().accept(new BytesRef("-._")));
            assertFalse(f.strings().accept(new BytesRef("A1")));
        }
    }

    public void testAnIncludeOfEveryStringIsNoTest() {
        // Dashboards' suggestion request for an empty box.
        assertSame(KeyFilter.ALL, filter(Map.of("include", ".*"), KeyType.KEYWORD));
        assertSame(KeyFilter.ALL, filter(Map.of("include", "@"), KeyType.KEYWORD));
        assertSame(KeyFilter.ALL, filter(Map.of("include", "(.|a)*"), KeyType.KEYWORD));
        assertNotNull(filter(Map.of("include", ".*", "exclude", "a"), KeyType.KEYWORD).test());
        assertNotNull(filter(Map.of("include", ".+"), KeyType.KEYWORD).test());
    }

    // ---- ip addresses ----

    private static Pattern ipPattern(String address) {
        return Pattern.compile("^(?:" + InfinoIncludeExclude.ipForms(InetAddresses.forString(address)) + ")$");
    }

    /** One random way of writing {@code address} that Rust's address parser reads. */
    private static String randomForm(InetAddress address) {
        byte[] bytes = InetAddressPoint.encode(address);
        int[] g = new int[8];
        for (int i = 0; i < 8; i++) {
            g[i] = (bytes[2 * i] & 0xff) << 8 | bytes[2 * i + 1] & 0xff;
        }
        boolean mapped = g[0] == 0 && g[1] == 0 && g[2] == 0 && g[3] == 0 && g[4] == 0 && g[5] == 0xffff;
        String dotted = (bytes[12] & 0xff) + "." + (bytes[13] & 0xff) + "." + (bytes[14] & 0xff) + "." + (bytes[15] & 0xff);
        if (mapped && randomBoolean()) {
            return dotted;
        }
        boolean v4tail = randomBoolean();
        int groups = v4tail ? 6 : 8;
        List<String> parts = new ArrayList<>();
        for (int i = 0; i < groups; i++) {
            String hex = Integer.toHexString(g[i]);
            hex = "0".repeat(between(0, 4 - hex.length())) + hex;
            parts.add(randomBoolean() ? hex.toUpperCase(Locale.ROOT) : hex);
        }
        if (v4tail) {
            parts.add(dotted);
        }
        // Compress a random run of zero groups, if any and if wanted.
        List<int[]> runs = new ArrayList<>();
        for (int s = 0; s < groups; s++) {
            for (int e = s + 1; e <= groups && g[e - 1] == 0; e++) {
                runs.add(new int[] { s, e });
            }
        }
        if (runs.isEmpty() || randomBoolean()) {
            return String.join(":", parts);
        }
        int[] run = randomFrom(runs);
        return String.join(":", parts.subList(0, run[0])) + "::" + String.join(":", parts.subList(run[1], parts.size()));
    }

    public void testIpFormsMatchEveryWritingOfTheAddress() {
        List<String> addresses = List.of(
            "0.0.0.0",
            "127.0.0.1",
            "10.1.2.3",
            "::1",
            "::",
            "2001:db8::1",
            "2001:db8:0:0:1:0:0:1",
            "fe80::abcd:0:12",
            "::ffff:192.168.0.1",
            "::1.2.3.4",
            "1:2:3:4:5:6:7:8"
        );
        for (String address : addresses) {
            InetAddress a = InetAddresses.forString(address);
            Pattern p = ipPattern(address);
            for (int i = 0; i < 200; i++) {
                String form = randomForm(a);
                assertEquals(form + " is " + address, a, InetAddresses.forString(form));
                assertTrue(form + " should match " + address + " via " + p, p.matcher(form).matches());
            }
            for (String other : addresses) {
                InetAddress b = InetAddresses.forString(other);
                if (b.equals(a)) {
                    continue;
                }
                for (int i = 0; i < 50; i++) {
                    String form = randomForm(b);
                    assertFalse(form + " should not match " + address, p.matcher(form).matches());
                }
            }
        }
        // The two writings of an IPv4 address an application meets most.
        assertTrue(ipPattern("0.0.0.0").matcher("::ffff:0.0.0.0").matches());
        assertTrue(ipPattern("::FFFF:0:0").matcher("0.0.0.0").matches());
        assertFalse(ipPattern("0.0.0.0").matcher("::").matches());
    }

    // ---- the key test ----

    private static KeyFilter filter(Map<String, Object> body, KeyType type) {
        return InfinoIncludeExclude.keyTest("t", body, type);
    }

    private static UnaryOperator<String> test(Map<String, Object> body, KeyType type) {
        return filter(body, type).test();
    }

    public void testValuesAreReadAsTheFieldReadsThem() {
        assertEquals("(k IN ('a', 'b''c'))", test(Map.of("include", List.of("a", "b'c")), KeyType.KEYWORD).apply("k"));
        assertEquals("(NOT k IN (1, 2))", test(Map.of("exclude", List.of("1", "2.7")), KeyType.LONG).apply("k"));
        assertEquals("(CAST(k AS DOUBLE) IN (1.5))", test(Map.of("include", List.of("1.5")), KeyType.DOUBLE).apply("k"));
        assertEquals("(k IN (true) AND NOT k IN (false))", test(Map.of("include", List.of("true"), "exclude", List.of("false")), KeyType.BOOLEAN).apply("k"));
        assertEquals("(FALSE)", test(Map.of("include", List.of()), KeyType.KEYWORD).apply("k"));
        assertEquals("(FALSE)", test(Map.of("include", List.of()), KeyType.IP).apply("k"));
        assertFalse(filter(Map.of("include", List.of()), KeyType.KEYWORD).strings().accept(new BytesRef("a")));
        assertTrue(filter(Map.of("include", List.of("a")), KeyType.KEYWORD).strings().accept(new BytesRef("a")));
        assertFalse(filter(Map.of("exclude", List.of("a")), KeyType.KEYWORD).strings().accept(new BytesRef("a")));
        assertTrue(test(Map.of("exclude", List.of("0.0.0.0")), KeyType.IP).apply("k").startsWith("(NOT regexp_like(k, '^(?:"));
        String both = test(Map.of("include", "a.*", "exclude", "ab"), KeyType.KEYWORD).apply("k");
        assertEquals("(regexp_like(k, '^(?:a(?:(?s:.))*)$') AND NOT regexp_like(k, '^(?:ab)$'))", both);
    }

    public void testAnEmptyListOfNumbersIsNoList() {
        // Stock's long filter keeps every value when given no values to include.
        for (KeyType type : List.of(KeyType.LONG, KeyType.DOUBLE, KeyType.BOOLEAN, KeyType.DATE)) {
            assertSame(type.toString(), KeyFilter.ALL, filter(Map.of("include", List.of()), type));
        }
        for (KeyType type : KeyType.values()) {
            assertSame(type.toString(), KeyFilter.ALL, filter(Map.of("exclude", List.of()), type));
        }
        assertEquals("(NOT k IN (1))", test(Map.of("include", List.of(), "exclude", List.of(1)), KeyType.LONG).apply("k"));
    }

    public void testMalformedValuesAreRefusedAsStockRefusesThem() {
        expectThrows(NumberFormatException.class, () -> test(Map.of("include", List.of("x")), KeyType.LONG));
        expectThrows(NumberFormatException.class, () -> test(Map.of("include", List.of("x")), KeyType.DOUBLE));
        IllegalArgumentException bool = expectThrows(IllegalArgumentException.class, () -> test(Map.of("include", List.of("yes")), KeyType.BOOLEAN));
        assertTrue(bool.getMessage(), bool.getMessage().contains("expected either [true] or [false]"));
        expectThrows(IllegalArgumentException.class, () -> test(Map.of("include", List.of("not-an-ip")), KeyType.IP));
    }

    public void testWhatIsNotServedIsRefused() {
        Unsupported partition = expectThrows(Unsupported.class, () -> test(Map.of("include", Map.of("partition", 0, "num_partitions", 2)), KeyType.KEYWORD));
        assertTrue(partition.getMessage(), partition.getMessage().contains("partition"));
        AggregationExecutionException regexOnIp = expectThrows(AggregationExecutionException.class, () -> test(Map.of("exclude", "127.*"), KeyType.IP));
        assertTrue(regexOnIp.getMessage(), regexOnIp.getMessage().contains("cannot support regular expression style include/exclude"));
        expectThrows(AggregationExecutionException.class, () -> test(Map.of("include", "1.*"), KeyType.LONG));
        expectThrows(Unsupported.class, () -> test(Map.of("include", List.of("2020-01-01")), KeyType.DATE));
    }
}
