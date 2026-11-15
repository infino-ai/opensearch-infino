/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;
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

    /** A user as the security plugin's: a plugin user or not. */
    public static final class User {
        private final boolean plugin;

        User(boolean plugin) {
            this.plugin = plugin;
        }

        public boolean isPluginUser() {
            return plugin;
        }
    }

    /** Admin certificates that name one user. */
    public static final class AdminDns {
        private final Object admin;

        AdminDns(Object admin) {
            this.admin = admin;
        }

        public boolean isAdmin(User user) {
            return user == admin;
        }
    }

    public void testTheSecurityPluginRunsWhenInstalledAndNeitherDisabledNorSslOnly() {
        List<String> installed = List.of("org.opensearch.infino.InfinoPlugin", InfinoSecurityCalls.SECURITY_PLUGIN);
        assertTrue(InfinoSecurityCalls.runs(Settings.EMPTY, installed));
        assertTrue(InfinoSecurityCalls.runs(Settings.builder().put(InfinoSecurityCalls.SECURITY_DISABLED, false).build(), installed));
        assertFalse(InfinoSecurityCalls.runs(Settings.builder().put(InfinoSecurityCalls.SECURITY_DISABLED, true).build(), installed));
        assertFalse(InfinoSecurityCalls.runs(Settings.builder().put(InfinoSecurityCalls.SECURITY_SSL_ONLY, true).build(), installed));
        assertFalse(InfinoSecurityCalls.runs(Settings.EMPTY, List.of("org.opensearch.infino.InfinoPlugin")));
    }

    public void testTheSecurityIndexIsReadByAnAdminCertificateOrAConfigurationReadOnly() throws Exception {
        User admin = new User(false);
        AdminDns dns = new AdminDns(admin);
        ThreadContext context = new ThreadContext(Settings.EMPTY);
        // No user: a node-internal request reads it as empty, as the security plugin's reader shows it.
        assertFalse(InfinoSecurityCalls.readsSecurityIndex(context, dns));
        try (ThreadContext.StoredContext ignored = context.stashContext()) {
            context.putTransient(InfinoSecurityCalls.USER, admin);
            assertTrue(InfinoSecurityCalls.readsSecurityIndex(context, dns));
            // Admin certificates the plugin could not read lift nothing.
            assertFalse(InfinoSecurityCalls.readsSecurityIndex(context, null));
        }
        try (ThreadContext.StoredContext ignored = context.stashContext()) {
            context.putTransient(InfinoSecurityCalls.USER, new User(false));
            assertFalse(InfinoSecurityCalls.readsSecurityIndex(context, dns));
        }
        try (ThreadContext.StoredContext ignored = context.stashContext()) {
            // A plugin user lifts document rules, not the security index.
            context.putTransient(InfinoSecurityCalls.USER, new User(true));
            assertFalse(InfinoSecurityCalls.readsSecurityIndex(context, dns));
        }
        try (ThreadContext.StoredContext ignored = context.stashContext()) {
            context.putHeader(InfinoSecurityCalls.CONF_REQUEST_HEADER, "true");
            assertTrue(InfinoSecurityCalls.readsSecurityIndex(context, dns));
        }
        try (ThreadContext.StoredContext ignored = context.stashContext()) {
            context.putHeader(InfinoSecurityCalls.CONF_REQUEST_HEADER, "false");
            assertFalse(InfinoSecurityCalls.readsSecurityIndex(context, dns));
        }
    }

    public void testAFailingReadReportDoesNotFailTheRead() {
        InfinoAccess access = new InfinoAccess(null, null, null, (id, shown) -> { throw new IllegalArgumentException("unreadable"); });
        assertEquals("{\"a\":1}", access.read("d1", "{\"a\":1}"));
    }
}
