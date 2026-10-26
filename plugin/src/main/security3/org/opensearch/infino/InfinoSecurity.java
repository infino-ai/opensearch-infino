/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.OpenSearchSecurityException;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Injector;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.index.query.QueryBuilder;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;

import static org.opensearch.infino.InfinoSecurityCalls.ADMIN_DNS;
import static org.opensearch.infino.InfinoSecurityCalls.USER;
import static org.opensearch.infino.InfinoSecurityCalls.call;
import static org.opensearch.infino.InfinoSecurityCalls.component;
import static org.opensearch.infino.InfinoSecurityCalls.has;
import static org.opensearch.infino.InfinoSecurityCalls.reason;
import static org.opensearch.infino.InfinoSecurityCalls.restrictedUser;

/**
 * The security plugin's document-level security, field-level security and
 * field masking for a request, as the table enforces them (3.x). The
 * security plugin keeps the evaluated rules in its own objects rather than
 * the thread context, so the plugin asks those objects (see
 * {@link InfinoSecurityCalls} for how they are reached): the user's
 * evaluation context from its privileges evaluator, the rules for an index
 * from its processed roles configuration.
 *
 * <p>Where the two objects live moved in 3.6: from there one component,
 * the privileges configuration, holds both; before it the evaluator is a
 * component of its own and the processed rules sit inside the DLS/FLS
 * valve, which is not one, so the plugin builds the same rules from the
 * roles configuration the security plugin's repository holds. A security
 * plugin offering neither leaves nothing to evaluate with, and a request
 * from a user is then refused rather than served around rules that may
 * apply.
 *
 * <p>The security plugin's audit log records reads of the indices its
 * compliance configuration watches from inside the shard, which the plugin
 * bypasses; where the audit log is a component (3.6 and later) the plugin
 * reports every document it hands out to it instead.
 */
final class InfinoSecurity {
    private static final Logger log = LogManager.getLogger(InfinoSecurity.class);
    /** The component holding the privileges evaluator and the processed DLS/FLS rules (3.6 and later). */
    private static final String PRIVILEGES_CONFIGURATION = "org.opensearch.security.privileges.PrivilegesConfiguration";
    /** The privileges evaluator, a component of its own before 3.6. */
    private static final String PRIVILEGES_EVALUATOR = "org.opensearch.security.privileges.PrivilegesEvaluator";
    /** The repository holding the security configuration, a component on every release; the roles come from it before 3.6. */
    private static final String CONFIGURATION_REPOSITORY = "org.opensearch.security.configuration.ConfigurationRepository";
    /** The configuration types, whose {@code ROLES} names the roles configuration. */
    private static final String CONFIG_TYPE = "org.opensearch.security.securityconf.impl.CType";
    private static final String ROLES_TYPE = "ROLES";
    /** The factory that merges the built-in static roles into a roles configuration, as it does for the valve. */
    private static final String DYNAMIC_CONFIG_FACTORY = "org.opensearch.security.securityconf.DynamicConfigFactory";
    private static final String ADD_STATICS = "addStatics";
    /** The processed rules, built from a roles configuration; the valve keeps one, and before 3.6 the plugin builds its own. */
    private static final String PROCESSED_CONFIG = "org.opensearch.security.privileges.dlsfls.DlsFlsProcessedConfig";
    /** Its constructor's arguments: the roles, the indices lookup, the content registry, the node settings, the masking configuration. */
    private static final int PROCESSED_CONFIG_ARGUMENTS = 5;
    /** The field-masking configuration (hash algorithm, salt), read from the node settings as the valve reads it. */
    private static final String FIELD_MASKING_CONFIG = "org.opensearch.security.privileges.dlsfls.FieldMasking$Config";
    /** The per-request allow list of documents the security plugin's own reads may see regardless of a rule. */
    private static final String DOCUMENT_ALLOW_LIST = "org.opensearch.security.privileges.DocumentAllowList";
    /** The audit log as the security plugin binds it when auditing is on (a component from 3.6). */
    private static final String AUDIT_LOG = "org.opensearch.security.auditlog.impl.AuditLogImpl";
    /** The header saying the security plugin already folded the document rule into the query (its filter-level mode). */
    private static final String FILTER_LEVEL_DLS_DONE = "_opendistro_security_filter_level_dls_done";
    /** The thread-context key the security plugin keeps a request's evaluation context under; shared so one evaluation serves both. */
    private static final String EVALUATION_CONTEXT = "tmp_dls_fls_ctx";
    /** The id the security plugin's document allow list uses for "every document of the index" (a constant of its own only from 3.9). */
    private static final String ANY_DOCUMENT = "*";
    /** The one shard an Infino-backed index has, which a read is reported against. */
    private static final int THE_SHARD = 0;
    /** The user's own word on whether an API token authenticated the request (3.9 and later). */
    private static final String API_TOKEN_REQUEST = "isApiTokenRequest";

    /** The security plugin's evaluator and its processed rules, wherever this release keeps them. */
    private interface Evaluation {
        Object evaluator() throws ReflectiveOperationException;

        Object rules() throws ReflectiveOperationException;
    }

    /** The security plugin's components, resolved once from the class loader its user object came with. */
    private record Components(Object adminDns, Class<?> documentAllowList, Evaluation evaluation, Object auditLog) {}

    private static volatile Injector injector;
    private static volatile Components components;

    private InfinoSecurity() {}

    /** Keep the node's injector; the security plugin's components are taken from it when the first user arrives. */
    static void bind(Injector injector) {
        InfinoSecurity.injector = injector;
    }

    /** The components, resolved through the class loader of {@code user}; null when the security plugin registered none. */
    private static synchronized Components components(Object user) {
        Components found = components;
        if (found != null) {
            return found;
        }
        Injector injector = InfinoSecurity.injector;
        if (injector == null) {
            return null;
        }
        // The loader the security plugin's classes come from; no security manager
        // guards the question on 3.x.
        ClassLoader loader = user.getClass().getClassLoader();
        Object dns = component(injector, loader, ADMIN_DNS);
        if (dns == null) {
            log.warn("[infino] the security plugin registers no admin certificates the plugin can read; requests from users are refused");
            return null;
        }
        Class<?> allowList;
        try {
            allowList = Class.forName(DOCUMENT_ALLOW_LIST, false, loader);
        } catch (ClassNotFoundException e) {
            log.warn("[infino] this security plugin has no document allow list the plugin can read; requests from users are refused");
            return null;
        }
        Evaluation evaluation = fromPrivilegesConfiguration(injector, loader);
        if (evaluation == null) {
            evaluation = fromRolesConfiguration(injector, loader);
        }
        if (evaluation == null) {
            log.warn("[infino] this security plugin registers neither a privileges configuration nor its evaluator and configuration repository; requests from users are refused");
            return null;
        }
        Object auditLog = component(injector, loader, AUDIT_LOG);
        if (auditLog == null) {
            log.info("[infino] this security plugin registers no audit log the plugin can reach; reads of Infino-backed indices are not in its read history");
        }
        found = new Components(dns, allowList, evaluation, auditLog);
        components = found;
        return found;
    }

    /** 3.6 and later: one component holds the evaluator and the processed rules. */
    private static Evaluation fromPrivilegesConfiguration(Injector injector, ClassLoader loader) {
        Object configuration = component(injector, loader, PRIVILEGES_CONFIGURATION);
        if (configuration == null) {
            return null;
        }
        return new Evaluation() {
            @Override
            public Object evaluator() throws ReflectiveOperationException {
                return call(configuration, "privilegesEvaluator");
            }

            @Override
            public Object rules() throws ReflectiveOperationException {
                return call(configuration, "dlsFlsProcessedConfig");
            }
        };
    }

    /**
     * 3.0 to 3.5: the evaluator and the configuration repository are
     * components of their own, and the processed rules live inside the
     * DLS/FLS valve, which is not one. So the plugin builds the same
     * processed rules the valve builds, from the roles configuration the
     * repository holds, with the valve's own ingredients (the indices
     * lookup, the content registry, the node settings and the masking
     * configuration), keeps them following cluster state as the valve
     * does, and rebuilds them when the repository hands out a new roles
     * configuration.
     */
    private static Evaluation fromRolesConfiguration(Injector injector, ClassLoader loader) {
        Object evaluator = component(injector, loader, PRIVILEGES_EVALUATOR);
        Object repository = component(injector, loader, CONFIGURATION_REPOSITORY);
        if (evaluator == null || repository == null) {
            return null;
        }
        Object rolesType;
        Object maskingConfig;
        Constructor<?> processed;
        Method addStatics;
        Settings settings;
        NamedXContentRegistry registry;
        try {
            rolesType = Class.forName(CONFIG_TYPE, false, loader).getField(ROLES_TYPE).get(null);
            addStatics = null;
            for (Method m : Class.forName(DYNAMIC_CONFIG_FACTORY, false, loader).getMethods()) {
                if (m.getName().equals(ADD_STATICS) && m.getParameterCount() == 1) {
                    addStatics = m;
                }
            }
            if (addStatics == null) {
                return null;
            }
            settings = injector.getInstance(Settings.class);
            registry = injector.getInstance(NamedXContentRegistry.class);
            maskingConfig = Class.forName(FIELD_MASKING_CONFIG, false, loader).getMethod("fromSettings", Settings.class).invoke(null, settings);
            processed = null;
            for (Constructor<?> c : Class.forName(PROCESSED_CONFIG, false, loader).getConstructors()) {
                if (c.getParameterCount() == PROCESSED_CONFIG_ARGUMENTS) {
                    processed = c;
                }
            }
            if (processed == null) {
                return null;
            }
        } catch (ReflectiveOperationException | RuntimeException e) {
            log.warn("[infino] the security plugin's processed rules cannot be built here: {}", reason(e));
            return null;
        }
        Constructor<?> constructor = processed;
        Method withStatics = addStatics;
        return new Evaluation() {
            /** The roles configuration the current rules were built from, and those rules. */
            private Object builtFrom;
            private Object built;

            @Override
            public Object evaluator() {
                return evaluator;
            }

            @Override
            public synchronized Object rules() throws ReflectiveOperationException {
                Object roles = call(repository, "getConfiguration", rolesType);
                if (roles == null) {
                    throw new IllegalStateException("the security plugin has not loaded its roles yet");
                }
                if (roles != builtFrom) {
                    // The repository holds the configured roles alone; the built-in
                    // static ones (all_access among them) are merged in by the
                    // security plugin before its valve sees them, on a copy here too.
                    Object merged = withStatics.invoke(null, call(roles, "deepClone"));
                    ClusterService clusters = InfinoServices.clusterService();
                    Object rules = constructor.newInstance(merged, clusters.state().metadata().getIndicesLookup(), registry, settings, maskingConfig);
                    call(rules, "updateClusterStateMetadataAsync", clusters, InfinoServices.threadPool());
                    if (built != null) {
                        call(built, "shutdown");
                    }
                    built = rules;
                    builtFrom = roles;
                }
                return built;
            }
        };
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
        Components security = components(anyUser);
        if (security == null) {
            throw refused(what, index, "this security plugin keeps no evaluation the plugin can read, so its document- and field-level rules cannot be applied");
        }
        BiConsumer<String, String> onRead = readAudit(security.auditLog(), index);
        try {
            Object user = restrictedUser(context, security.adminDns());
            // A user authenticated by one of the security plugin's API tokens
            // (3.9 and later) is governed by the token's own permissions; the
            // security plugin evaluates no document, field or masking rule for
            // such a request, and neither does the plugin.
            if (user == null || (has(user, API_TOKEN_REQUEST) && (Boolean) call(user, API_TOKEN_REQUEST))) {
                return onRead == null ? InfinoAccess.NONE : new InfinoAccess(null, null, null, onRead);
            }
            Object ctx = context.getTransient(EVALUATION_CONTEXT);
            if (ctx == null) {
                ctx = call(security.evaluation().evaluator(), "createContext", user, null);
                context.putTransient(EVALUATION_CONTEXT, ctx);
            }
            Object rules = security.evaluation().rules();
            List<QueryBuilder> documents = null;
            if (context.getHeader(FILTER_LEVEL_DLS_DONE) == null) {
                Object dls = call(call(rules, "getDocumentPrivileges"), "getRestriction", ctx, index);
                Object allowList = security.documentAllowList().getMethod("get", ThreadContext.class).invoke(null, context);
                boolean lifted = (Boolean) call(allowList, "isAllowed", index, ANY_DOCUMENT);
                if (!(Boolean) call(dls, "isUnrestricted") && !lifted) {
                    documents = new ArrayList<>();
                    for (Object q : (Iterable<?>) call(dls, "getQueries")) {
                        documents.add((QueryBuilder) call(q, "getQueryBuilder"));
                    }
                }
            }
            Object fls = call(call(rules, "getFieldPrivileges"), "getRestriction", ctx, index);
            Object masking = call(call(rules, "getFieldMasking"), "getRestriction", ctx, index);
            if (log.isDebugEnabled()) {
                log.debug(
                    "[infino] {} over [{}] by roles {}: documents {}, fields {}, masking {}",
                    what,
                    index,
                    call(ctx, "getMappedRoles"),
                    documents,
                    fls,
                    masking
                );
            }
            Predicate<String> visible = (Boolean) call(fls, "isUnrestricted") ? null : field -> {
                try {
                    return (Boolean) call(fls, "isAllowedRecursive", field);
                } catch (ReflectiveOperationException e) {
                    throw refused(what, index, "its field-level rule could not be evaluated: " + reason(e));
                }
            };
            Function<String, UnaryOperator<String>> maskOf = (Boolean) call(masking, "isUnrestricted") ? null : field -> {
                try {
                    Object f = call(masking, "get", field);
                    return f == null ? null : value -> {
                        try {
                            return (String) call(f, "apply", value);
                        } catch (ReflectiveOperationException e) {
                            throw refused(what, index, "its masking rule could not be applied: " + reason(e));
                        }
                    };
                } catch (ReflectiveOperationException e) {
                    throw refused(what, index, "its masking rule could not be evaluated: " + reason(e));
                }
            };
            return new InfinoAccess(documents, visible, maskOf, onRead);
        } catch (ReflectiveOperationException e) {
            throw refused(what, index, "its document- and field-level rules could not be evaluated: " + reason(e));
        }
    }

    /**
     * What tells the audit log of each document a request is handed, when
     * its compliance configuration watches {@code index} for reads; null
     * when it does not, or the audit log is out of reach on this release.
     */
    private static BiConsumer<String, String> readAudit(Object auditLog, String index) {
        if (auditLog == null) {
            return null;
        }
        try {
            Object compliance = call(auditLog, "getComplianceConfig");
            if (compliance == null || !(Boolean) call(compliance, "isEnabled") || !(Boolean) call(compliance, "readHistoryEnabledForIndex", index)) {
                return null;
            }
        } catch (ReflectiveOperationException e) {
            log.debug("[infino] the audit log's compliance configuration could not be read: {}", reason(e));
            return null;
        }
        IndexMetadata md = InfinoServices.clusterService().state().metadata().index(index);
        if (md == null) {
            return null;
        }
        ShardId shard = new ShardId(md.getIndex(), THE_SHARD);
        return (id, shown) -> {
            try {
                call(auditLog, "logDocumentRead", index, id, shard, InfinoAccess.fields(shown));
            } catch (ReflectiveOperationException e) {
                log.warn("[infino] a read of {} from {} could not be reported to the audit log: {}", id, index, reason(e));
            }
        };
    }

    private static OpenSearchSecurityException refused(String what, String index, String why) {
        return InfinoAccess.refused(what + " over [" + index + "] is refused: " + why);
    }
}
