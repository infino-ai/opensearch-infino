/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.OpenSearchSecurityException;
import org.opensearch.SpecialPermission;
import org.opensearch.common.inject.Injector;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.query.AbstractQueryBuilder;
import org.opensearch.index.query.QueryBuilder;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;

import static org.opensearch.infino.InfinoSecurityCalls.ADMIN_DNS;
import static org.opensearch.infino.InfinoSecurityCalls.USER;
import static org.opensearch.infino.InfinoSecurityCalls.call;
import static org.opensearch.infino.InfinoSecurityCalls.component;
import static org.opensearch.infino.InfinoSecurityCalls.documentRuleFolded;
import static org.opensearch.infino.InfinoSecurityCalls.reason;
import static org.opensearch.infino.InfinoSecurityCalls.restrictedUser;

/**
 * The security plugin's document-level security, field-level security and
 * field masking for a request, as the table enforces them (2.x). That
 * security plugin evaluates the rules after its index-level check and
 * hands them to the shards as three thread-context headers, one per kind,
 * each a serialized map from index pattern to the rules for it: the
 * document queries as JSON, the field patterns, the masked field
 * patterns. The plugin reads them off the request through the security
 * plugin's own helpers (see {@link InfinoSecurityCalls} for how those are
 * reached): its deserializer, its pattern matcher for the index names and
 * the field patterns, and its masked-field class for the hash, so the
 * semantics are its own. The document queries are parsed with the node's
 * content registry, as the shard would parse them.
 */
final class InfinoSecurity {
    private static final Logger log = LogManager.getLogger(InfinoSecurity.class);
    /** The headers the security plugin sets when a rule applies to the request, each a serialized map by index pattern. */
    private static final String DLS_HEADER = "_opendistro_security_dls_query";
    private static final String FLS_HEADER = "_opendistro_security_fls_fields";
    private static final String MASKED_HEADER = "_opendistro_security_masked_fields";
    /** The security plugin's serializer for those headers. */
    private static final String BASE64_HELPER = "org.opensearch.security.support.Base64Helper";
    /** The security plugin's pattern matcher: wildcards, and regular expressions between slashes. */
    private static final String WILDCARD_MATCHER = "org.opensearch.security.support.WildcardMatcher";
    /** A masked field as the security plugin masks it: a name pattern, a hash algorithm or regular-expression replacements. */
    private static final String MASKED_FIELD = "org.opensearch.security.configuration.MaskedField";
    /** The salt the hashes are keyed with, from the node settings. */
    private static final String SALT = "org.opensearch.security.configuration.Salt";
    /** The node setting naming the default hash algorithm for masked fields; unset means the security plugin's own default. */
    private static final String MASKING_ALGORITHM_SETTING = "plugins.security.masked_fields.algorithm.default";
    /** A field pattern starting with this excludes the field instead of including it. */
    private static final String EXCLUDE_PREFIX = "~";

    /** The security plugin's helpers, resolved once from the class loader its user object came with. */
    private record Helpers(Object adminDns, Method deserialize, Method matcherFrom, Constructor<?> maskedField, Object salt, String algorithm) {}

    private static volatile Injector injector;
    private static volatile Helpers helpers;
    /** The security plugin's matchers by pattern, and its masked fields by specification, built once. */
    private static final Map<String, Object> MATCHERS = new ConcurrentHashMap<>();
    private static final Map<String, Object> MASKED = new ConcurrentHashMap<>();

    private InfinoSecurity() {}

    /** Keep the node's injector; the security plugin's helpers are taken from it when the first user arrives. */
    static void bind(Injector injector) {
        InfinoSecurity.injector = injector;
    }

    /** The helpers, resolved through the class loader of {@code user}; null when the security plugin lacks one. */
    private static synchronized Helpers helpers(Object user) {
        Helpers found = helpers;
        if (found != null) {
            return found;
        }
        Injector injector = InfinoSecurity.injector;
        if (injector == null) {
            return null;
        }
        ClassLoader loader = loaderOf(user);
        try {
            Object dns = component(injector, loader, ADMIN_DNS);
            Method deserialize = Class.forName(BASE64_HELPER, false, loader).getMethod("deserializeObject", String.class);
            Method matcherFrom = Class.forName(WILDCARD_MATCHER, false, loader).getMethod("from", String.class);
            Settings settings = injector.getInstance(Settings.class);
            // The masking helpers are resolved on their own: without them only
            // a request under a masking rule is refused, not every request
            // under a rule. An unset default algorithm is passed as null, which
            // the masked field reads as its built-in hash, as the security
            // plugin's own reader does.
            Constructor<?> maskedField = null;
            Object salt = null;
            try {
                Class<?> saltClass = Class.forName(SALT, false, loader);
                salt = saltClass.getMethod("from", Settings.class).invoke(null, settings);
                maskedField = Class.forName(MASKED_FIELD, false, loader).getConstructor(String.class, saltClass, String.class);
            } catch (ReflectiveOperationException | RuntimeException e) {
                log.warn("[infino] this security plugin lacks the masked-field helper the plugin hashes with ({}); requests under a masking rule are refused", reason(e));
            }
            found = new Helpers(dns, deserialize, matcherFrom, maskedField, salt, settings.get(MASKING_ALGORITHM_SETTING));
        } catch (ReflectiveOperationException | RuntimeException e) {
            log.warn("[infino] this security plugin lacks a helper the plugin reads its rules with ({}); requests under a rule are refused", reason(e));
            return null;
        }
        helpers = found;
        return found;
    }

    /**
     * The class loader the security plugin's object came with, which loads
     * its classes by name. Asking another plugin's object for its loader is
     * a privileged call under the security manager 2.x still runs, granted
     * to this plugin by its policy.
     */
    @SuppressWarnings("removal")
    private static ClassLoader loaderOf(Object securityObject) {
        SpecialPermission.check();
        return AccessController.doPrivileged((PrivilegedAction<ClassLoader>) () -> securityObject.getClass().getClassLoader());
    }

    /** What the request may see of {@code index}; {@code what} names the request in a refusal. */
    static InfinoAccess access(String index, String what) {
        ThreadContext context = InfinoServices.threadContext();
        if (context == null) {
            return InfinoAccess.NONE;
        }
        Object anyUser = context.getTransient(USER);
        if (anyUser == null) {
            return InfinoAccess.NONE;
        }
        Helpers security = helpers(anyUser);
        try {
            if (restrictedUser(context, security == null ? null : security.adminDns()) == null) {
                return InfinoAccess.NONE;
            }
            Set<String> queries = documentRuleFolded(context, what) ? Set.of() : rulesFor(security, context, DLS_HEADER, index, what);
            Set<String> fields = rulesFor(security, context, FLS_HEADER, index, what);
            Set<String> masked = rulesFor(security, context, MASKED_HEADER, index, what);
            if (log.isDebugEnabled()) {
                log.debug("[infino] {} over [{}]: documents {}, fields {}, masking {}", what, index, queries, fields, masked);
            }
            List<QueryBuilder> documents = null;
            if (!queries.isEmpty()) {
                documents = new ArrayList<>();
                for (String json : queries) {
                    documents.add(parse(json));
                }
            }
            return new InfinoAccess(documents, fields.isEmpty() ? null : visibleBy(security, fields), masked.isEmpty() ? null : maskedBy(security, masked));
        } catch (ReflectiveOperationException | IOException e) {
            throw refused(what, index, "its document- and field-level rules could not be evaluated: " + reason(e));
        }
    }

    /**
     * The rules of one kind that apply to {@code index}: the union over the
     * header's entries whose index pattern matches it. Empty when the
     * header is absent; refused when it is present and cannot be read.
     */
    private static Set<String> rulesFor(Helpers security, ThreadContext context, String header, String index, String what)
        throws ReflectiveOperationException {
        String value = context.getHeader(header);
        if (value == null || value.isEmpty()) {
            return Set.of();
        }
        if (security == null) {
            throw refused(what, index, "this security plugin's rules cannot be read");
        }
        Object map = security.deserialize().invoke(null, value);
        if (!(map instanceof Map<?, ?> byPattern)) {
            throw refused(what, index, "the security plugin's rules arrived in a shape the plugin does not read");
        }
        Set<String> out = new LinkedHashSet<>();
        for (Map.Entry<?, ?> e : byPattern.entrySet()) {
            if (!matches(security, String.valueOf(e.getKey()), index)) {
                continue;
            }
            // A rule that applies and cannot be read refuses, as an unreadable header does.
            if (!(e.getValue() instanceof Iterable<?> rules)) {
                throw refused(what, index, "the security plugin's rules for [" + e.getKey() + "] arrived in a shape the plugin does not read");
            }
            for (Object rule : rules) {
                out.add(String.valueOf(rule));
            }
        }
        return out;
    }

    /** Whether the security plugin's pattern matches {@code name}. */
    private static boolean matches(Helpers security, String pattern, String name) throws ReflectiveOperationException {
        Object matcher = MATCHERS.get(pattern);
        if (matcher == null) {
            matcher = security.matcherFrom().invoke(null, pattern);
            MATCHERS.put(pattern, matcher);
        }
        return (Boolean) call(matcher, "test", name);
    }

    /** Whether a pattern matches the field or an object it sits in. */
    private static boolean matchesPath(Helpers security, Iterable<String> patterns, String field) throws ReflectiveOperationException {
        for (String pattern : patterns) {
            String path = field;
            while (true) {
                if (matches(security, pattern, path)) {
                    return true;
                }
                int dot = path.lastIndexOf('.');
                if (dot < 0) {
                    break;
                }
                path = path.substring(0, dot);
            }
        }
        return false;
    }

    /**
     * The field rule as the security plugin's shard reader applies it: when
     * any pattern excludes, a field is visible unless an excluding pattern
     * matches it or an object it sits in, and the including patterns are
     * ignored; otherwise a field is visible when an including pattern
     * matches it or an object it sits in.
     */
    private static Predicate<String> visibleBy(Helpers security, Set<String> patterns) {
        List<String> excludes = new ArrayList<>();
        List<String> includes = new ArrayList<>();
        for (String p : patterns) {
            if (p.startsWith(EXCLUDE_PREFIX)) {
                excludes.add(p.substring(EXCLUDE_PREFIX.length()));
            } else {
                includes.add(p);
            }
        }
        return field -> {
            try {
                return excludes.isEmpty() ? matchesPath(security, includes, field) : !matchesPath(security, excludes, field);
            } catch (ReflectiveOperationException e) {
                throw InfinoAccess.refused("the field-level rule could not be evaluated: " + reason(e));
            }
        };
    }

    /** The masks as the security plugin's masked fields apply them: a field takes the first specification whose name pattern matches it. */
    private static Function<String, UnaryOperator<String>> maskedBy(Helpers security, Set<String> specifications) throws ReflectiveOperationException {
        if (security.maskedField() == null) {
            throw InfinoAccess.refused("a masking rule applies, and this security plugin has no masked-field helper the plugin can hash with");
        }
        List<Object> masks = new ArrayList<>();
        for (String spec : specifications) {
            Object mask = MASKED.get(spec);
            if (mask == null) {
                mask = security.maskedField().newInstance(spec, security.salt(), security.algorithm());
                MASKED.put(spec, mask);
            }
            masks.add(mask);
        }
        return field -> {
            try {
                for (Object mask : masks) {
                    if (matches(security, (String) call(mask, "getName"), field)) {
                        return value -> {
                            try {
                                return (String) call(mask, "mask", value);
                            } catch (ReflectiveOperationException e) {
                                throw InfinoAccess.refused("the masking rule could not be applied: " + reason(e));
                            }
                        };
                    }
                }
                return null;
            } catch (ReflectiveOperationException e) {
                throw InfinoAccess.refused("the masking rule could not be evaluated: " + reason(e));
            }
        };
    }

    /** A document rule's query, written as JSON by the security plugin, as the shard would read it. */
    private static QueryBuilder parse(String json) throws IOException {
        try (XContentParser parser = JsonXContent.jsonXContent.createParser(InfinoServices.namedXContentRegistry(), LoggingDeprecationHandler.INSTANCE, json)) {
            return AbstractQueryBuilder.parseInnerQueryBuilder(parser);
        }
    }

    private static OpenSearchSecurityException refused(String what, String index, String why) {
        return InfinoAccess.refused(what + " over [" + index + "] is refused: " + why);
    }
}
