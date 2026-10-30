/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.test.OpenSearchTestCase;

import java.util.function.Function;

/** How the plugin calls into the security plugin by reflection, and when a folded document rule is honoured. */
public class InfinoSecurityCallsTests extends OpenSearchTestCase {

    /** A rule object as the security plugin's look: a generic interface (so a bridge method) and an overload. */
    public static final class Rule implements Function<String, String> {
        @Override
        public String apply(String value) {
            return "string:" + value;
        }

        public String take(Object value) {
            return "object";
        }

        public String take(CharSequence value) {
            return "chars";
        }
    }

    /** Two overloads neither of which is more specific for a String argument. */
    public static final class Tied {
        public String take(CharSequence value) {
            return "chars";
        }

        public String take(Comparable<?> value) {
            return "comparable";
        }
    }

    /** A rule base that is not public, as the security plugin's rule-based privileges are. */
    abstract static class HiddenBase {
        public String restriction(String index) {
            return "rule for " + index;
        }
    }

    /** Its public subclass: the inherited method is public only through the compiler's visibility bridge. */
    public static final class Visible extends HiddenBase {}

    public void testAMethodReachedOnlyThroughAVisibilityBridgeIsCalled() throws Exception {
        assertEquals("rule for logs", InfinoSecurityCalls.call(new Visible(), "restriction", "logs"));
    }

    public void testTheBridgeMethodIsNotACandidate() throws Exception {
        // apply(String) and the compiler's apply(Object) both fit a String; only the declared one is called.
        assertEquals("string:x", InfinoSecurityCalls.call(new Rule(), "apply", "x"));
    }

    public void testTheMostSpecificOverloadIsCalledWhateverTheOrder() throws Exception {
        assertEquals("chars", InfinoSecurityCalls.call(new Rule(), "take", "x"));
        assertEquals("object", InfinoSecurityCalls.call(new Rule(), "take", 1));
    }

    public void testOverloadsWithNoMostSpecificAreRefused() {
        NoSuchMethodException e = expectThrows(NoSuchMethodException.class, () -> InfinoSecurityCalls.call(new Tied(), "take", "x"));
        assertTrue(e.getMessage(), e.getMessage().contains("more than one overload"));
    }

    public void testAFoldedDocumentRuleIsHonouredForASearchOnly() {
        ThreadContext context = new ThreadContext(Settings.EMPTY);
        assertFalse(InfinoSecurityCalls.documentRuleFolded(context, InfinoSecurityCalls.SEARCH));
        context.putHeader(InfinoSecurityCalls.FILTER_LEVEL_DLS_DONE, "true");
        assertTrue(InfinoSecurityCalls.documentRuleFolded(context, InfinoSecurityCalls.SEARCH));
        // A get or multi-get has no query the rule could have been folded into.
        assertFalse(InfinoSecurityCalls.documentRuleFolded(context, "get"));
        assertFalse(InfinoSecurityCalls.documentRuleFolded(context, "mget"));
    }

    public void testAFailingReadReportDoesNotFailTheRead() {
        InfinoAccess access = new InfinoAccess(null, null, null, (id, shown) -> { throw new IllegalArgumentException("unreadable"); });
        assertEquals("{\"a\":1}", access.read("d1", "{\"a\":1}"));
    }
}
