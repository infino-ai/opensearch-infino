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
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.index.query.QueryBuilder;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;

/**
 * The security plugin's document-level security, field-level security and
 * field masking for a request, as the table enforces them (3.x). The
 * security plugin keeps the evaluated rules in its own objects rather than
 * the thread context, so the plugin asks those objects: the user's
 * evaluation context from its privileges evaluator, the rules for an index
 * from its processed roles configuration. The checks that lift the rules
 * are its own too: no user (a node-internal request), a plugin user, an
 * admin certificate, or the configuration-request header its own
 * configuration reads carry.
 *
 * <p>The objects are the components the security plugin registers with the
 * node's injector, found there by class name and called through reflection:
 * the plugin neither compiles against the security plugin nor extends it
 * (the 3.0 security plugin cannot be extended at all, and extending binds
 * the plugin's install to the security plugin's), so one class serves every
 * 3.x release and a node without the security plugin. The classes are
 * loaded through the security plugin's own class loader, which the first
 * request from a user hands over with its user object; until then there is
 * no user to restrict. Where the two objects live moved in 3.6: from there
 * one component, the privileges configuration, holds both; before it the
 * evaluator is a component of its own and the processed rules sit inside
 * the DLS/FLS valve, which is not one, so the plugin builds the same rules
 * from the roles configuration the security plugin's repository holds. A
 * security plugin offering neither leaves
 * nothing to evaluate with, and a request from a user is then refused
 * rather than served around rules that may apply.
 */
final class InfinoSecurity {
    private static final Logger log = LogManager.getLogger(InfinoSecurity.class);
    /** The security plugin's admin certificates; bound whenever the security plugin runs. */
    private static final String ADMIN_DNS = "org.opensearch.security.configuration.AdminDNs";
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
    /** The thread-context transient the security plugin keeps the authenticated user under. */
    private static final String USER = "_opendistro_security_user";
    /** The header the security plugin's own configuration reads carry. */
    private static final String CONF_REQUEST_HEADER = "_opendistro_security_conf_request";
    /** The header saying the security plugin already folded the document rule into the query (its filter-level mode). */
    private static final String FILTER_LEVEL_DLS_DONE = "_opendistro_security_filter_level_dls_done";
    /** The thread-context key the security plugin keeps a request's evaluation context under; shared so one evaluation serves both. */
    private static final String EVALUATION_CONTEXT = "tmp_dls_fls_ctx";
    /** The id the security plugin's document allow list uses for "every document of the index" (a constant of its own only from 3.9). */
    private static final String ANY_DOCUMENT = "*";

    /** The security plugin's evaluator and its processed rules, wherever this release keeps them. */
    private interface Evaluation {
        Object evaluator() throws ReflectiveOperationException;

        Object rules() throws ReflectiveOperationException;
    }

    /** The security plugin's components, resolved once from the class loader its user object came with. */
    private record Components(Object adminDns, Class<?> documentAllowList, Evaluation evaluation) {}

    private static volatile Injector injector;
    private static volatile Components components;
    /** Public methods by class, name and arity, looked up once. */
    private static final Map<String, Method> METHODS = new ConcurrentHashMap<>();

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
        found = new Components(dns, allowList, evaluation);
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

    /** The component bound under the class of that name, or null when the class or a binding for it is absent. */
    private static Object component(Injector injector, ClassLoader loader, String className) {
        try {
            Class<?> cls = Class.forName(className, false, loader);
            List<? extends Binding<?>> bindings = injector.findBindingsByType(TypeLiteral.get(cls));
            return bindings.isEmpty() ? null : bindings.get(0).getProvider().get();
        } catch (ClassNotFoundException e) {
            return null;
        }
    }

    /**
     * Call the public method of {@code name} on {@code target} whose
     * parameters take {@code args} (a null argument fits any reference
     * parameter); the method is looked up once per class, name and arity.
     */
    private static Object call(Object target, String name, Object... args) throws ReflectiveOperationException {
        Class<?> cls = target.getClass();
        String key = cls.getName() + '#' + name + '/' + args.length;
        Method method = METHODS.get(key);
        if (method == null) {
            for (Method m : cls.getMethods()) {
                if (m.getName().equals(name) && m.getParameterCount() == args.length && accepts(m, args)) {
                    method = m;
                    break;
                }
            }
            if (method == null) {
                throw new NoSuchMethodException(cls.getName() + "." + name + " taking " + args.length + " arguments");
            }
            METHODS.put(key, method);
        }
        return method.invoke(target, args);
    }

    private static boolean accepts(Method m, Object[] args) {
        Class<?>[] params = m.getParameterTypes();
        for (int i = 0; i < args.length; i++) {
            if (args[i] == null ? params[i].isPrimitive() : !params[i].isInstance(args[i])) {
                return false;
            }
        }
        return true;
    }

    /** What the request may see of {@code index}; {@code what} names the request in a refusal. */
    static InfinoAccess access(String index, String what) {
        ThreadContext context = InfinoServices.threadContext();
        if (context == null) {
            return InfinoAccess.NONE;
        }
        Object user = context.getTransient(USER);
        if (user == null) {
            return InfinoAccess.NONE;
        }
        if ("true".equals(context.getHeader(CONF_REQUEST_HEADER))) {
            return InfinoAccess.NONE;
        }
        Components security = components(user);
        if (security == null) {
            throw refused(what, index, "this security plugin keeps no evaluation the plugin can read, so its document- and field-level rules cannot be applied");
        }
        try {
            if ((Boolean) call(user, "isPluginUser") || (Boolean) call(security.adminDns(), "isAdmin", user)) {
                return InfinoAccess.NONE;
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
            return new InfinoAccess(documents, visible, maskOf);
        } catch (ReflectiveOperationException e) {
            throw refused(what, index, "its document- and field-level rules could not be evaluated: " + reason(e));
        }
    }

    /** The security plugin's own message when a call threw inside it, the reflective failure otherwise. */
    private static String reason(Exception e) {
        Throwable cause = e instanceof InvocationTargetException ite && ite.getCause() != null ? ite.getCause() : e;
        return cause.getMessage() == null ? cause.toString() : cause.getMessage();
    }

    private static OpenSearchSecurityException refused(String what, String index, String why) {
        return InfinoAccess.refused(what + " over [" + index + "] is refused: " + why);
    }
}
