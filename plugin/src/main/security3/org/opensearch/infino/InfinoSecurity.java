/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.OpenSearchSecurityException;
import org.opensearch.common.inject.Binding;
import org.opensearch.common.inject.Injector;
import org.opensearch.common.inject.TypeLiteral;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.security.configuration.AdminDNs;
import org.opensearch.security.privileges.DocumentAllowList;
import org.opensearch.security.privileges.PrivilegesEvaluationContext;
import org.opensearch.security.privileges.PrivilegesEvaluationException;
import org.opensearch.security.privileges.PrivilegesEvaluator;
import org.opensearch.security.privileges.dlsfls.DlsFlsProcessedConfig;
import org.opensearch.security.privileges.dlsfls.DlsRestriction;
import org.opensearch.security.privileges.dlsfls.DocumentPrivileges.RenderedDlsQuery;
import org.opensearch.security.privileges.dlsfls.FieldMasking;
import org.opensearch.security.privileges.dlsfls.FieldPrivileges;
import org.opensearch.security.support.ConfigConstants;
import org.opensearch.security.user.User;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;

/**
 * The security plugin's document-level security, field-level security and
 * field masking for a request, as the table enforces them (3.x). The
 * security plugin keeps the evaluated rules in its own objects rather than
 * the thread context, so the plugin extends it and asks those objects: the
 * user's evaluation context from its privileges evaluator, the rules for an
 * index from its processed roles configuration. The checks that lift the
 * rules are its own too: no user (a node-internal request), a plugin user,
 * an admin certificate, or the configuration-request header its own
 * configuration reads carry.
 *
 * <p>The objects are reached through the one component the security plugin
 * registers for injection that holds both, which exists from 3.6; on an
 * earlier 3.x there is nothing to evaluate with, and a request from a user
 * is refused rather than served around rules that may apply.
 */
final class InfinoSecurity {
    private static final Logger log = LogManager.getLogger(InfinoSecurity.class);
    /** The security plugin's component holding the privileges evaluator and the processed DLS/FLS rules (3.6 and later). */
    private static final String PRIVILEGES_CONFIGURATION = "org.opensearch.security.privileges.PrivilegesConfiguration";
    /** The thread-context key the security plugin keeps a request's evaluation context under; shared so one evaluation serves both. */
    private static final String EVALUATION_CONTEXT = "tmp_dls_fls_ctx";
    /** The id the security plugin's document allow list uses for "every document of the index" (a constant of its own only from 3.9). */
    private static final String ANY_DOCUMENT = "*";

    private static volatile AdminDNs adminDns;
    private static volatile Object privilegesConfiguration;
    private static volatile Method evaluatorOf;
    private static volatile Method rulesOf;

    private InfinoSecurity() {}

    /** Take the security plugin's components from the node's injector; none bound means the plugin is not running. */
    static void bind(Injector injector) {
        AdminDNs dns = component(injector, AdminDNs.class);
        if (dns == null) {
            log.info("[infino] the security plugin is not running; requests are served unrestricted");
            return;
        }
        adminDns = dns;
        try {
            Class<?> cls = Class.forName(PRIVILEGES_CONFIGURATION, false, InfinoSecurity.class.getClassLoader());
            Object configuration = component(injector, cls);
            if (configuration == null) {
                log.warn("[infino] the security plugin registered no privileges configuration; requests from users are refused");
                return;
            }
            evaluatorOf = cls.getMethod("privilegesEvaluator");
            rulesOf = cls.getMethod("dlsFlsProcessedConfig");
            privilegesConfiguration = configuration;
        } catch (ClassNotFoundException | NoSuchMethodException e) {
            log.warn("[infino] this security plugin keeps no evaluation the plugin can read (3.6 or later does); requests from users are refused");
        }
    }

    private static <T> T component(Injector injector, Class<T> type) {
        List<Binding<T>> bindings = injector.findBindingsByType(TypeLiteral.get(type));
        return bindings.isEmpty() ? null : bindings.get(0).getProvider().get();
    }

    /** What the request may see of {@code index}; {@code what} names the request in a refusal. */
    static InfinoAccess access(String index, String what) {
        AdminDNs dns = adminDns;
        ThreadContext context = InfinoServices.threadContext();
        if (dns == null || context == null) {
            return InfinoAccess.NONE;
        }
        User user = context.getTransient(ConfigConstants.OPENDISTRO_SECURITY_USER);
        if (user == null || user.isPluginUser() || dns.isAdmin(user)) {
            return InfinoAccess.NONE;
        }
        if ("true".equals(context.getHeader(ConfigConstants.OPENDISTRO_SECURITY_CONF_REQUEST_HEADER))) {
            return InfinoAccess.NONE;
        }
        Object configuration = privilegesConfiguration;
        if (configuration == null) {
            throw refused(what, index, "this security plugin keeps no evaluation the plugin can read; 3.6 or later evaluates document- and field-level rules");
        }
        try {
            PrivilegesEvaluationContext ctx = context.getTransient(EVALUATION_CONTEXT);
            if (ctx == null) {
                PrivilegesEvaluator evaluator = (PrivilegesEvaluator) evaluatorOf.invoke(configuration);
                ctx = evaluator.createContext(user, null);
                context.putTransient(EVALUATION_CONTEXT, ctx);
            }
            DlsFlsProcessedConfig rules = (DlsFlsProcessedConfig) rulesOf.invoke(configuration);
            List<QueryBuilder> documents = null;
            // When the security plugin has already folded the document rule into
            // the request (its filter-level mode), it is in the query that arrives.
            if (context.getHeader(ConfigConstants.OPENDISTRO_SECURITY_FILTER_LEVEL_DLS_DONE) == null) {
                DlsRestriction dls = rules.getDocumentPrivileges().getRestriction(ctx, index);
                boolean lifted = DocumentAllowList.get(context).isAllowed(index, ANY_DOCUMENT);
                if (!dls.isUnrestricted() && !lifted) {
                    documents = new ArrayList<>();
                    for (RenderedDlsQuery q : dls.getQueries()) {
                        documents.add(q.getQueryBuilder());
                    }
                }
            }
            FieldPrivileges.FlsRule fls = rules.getFieldPrivileges().getRestriction(ctx, index);
            FieldMasking.FieldMaskingRule masking = rules.getFieldMasking().getRestriction(ctx, index);
            Predicate<String> visible = fls.isUnrestricted() ? null : fls::isAllowedRecursive;
            Function<String, UnaryOperator<String>> maskOf = masking.isUnrestricted() ? null : field -> {
                FieldMasking.FieldMaskingRule.Field f = masking.get(field);
                return f == null ? null : f::apply;
            };
            return new InfinoAccess(documents, visible, maskOf);
        } catch (PrivilegesEvaluationException | ReflectiveOperationException e) {
            throw refused(what, index, "its document- and field-level rules could not be evaluated: " + e.getMessage());
        }
    }

    private static OpenSearchSecurityException refused(String what, String index, String why) {
        return InfinoAccess.refused(what + " over [" + index + "] is refused: " + why);
    }
}
