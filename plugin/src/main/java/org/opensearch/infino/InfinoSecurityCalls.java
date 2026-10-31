/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import org.opensearch.common.inject.Binding;
import org.opensearch.common.inject.Injector;
import org.opensearch.common.inject.TypeLiteral;
import org.opensearch.common.util.concurrent.ThreadContext;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * How the plugin reaches the security plugin on every release: its
 * components come off the node's injector by class name, loaded through
 * the class loader the request's user object comes with, and are called
 * by reflection on their public methods. The plugin neither compiles
 * against the security plugin nor extends it (the 3.0 security plugin
 * cannot be extended at all, and extending ties the plugin's install to
 * the security plugin's), so one build serves a node with or without it.
 * The checks that lift the security plugin's rules are shared here too:
 * no user (a node-internal request), a plugin user, an admin certificate,
 * or the configuration-request header its own configuration reads carry.
 */
final class InfinoSecurityCalls {
    /** The security plugin's admin certificates; bound whenever the security plugin runs. */
    static final String ADMIN_DNS = "org.opensearch.security.configuration.AdminDNs";
    /** The thread-context transient the security plugin keeps the authenticated user under. */
    static final String USER = "_opendistro_security_user";
    /** The header the security plugin's own configuration reads carry. */
    static final String CONF_REQUEST_HEADER = "_opendistro_security_conf_request";
    /** The header saying the security plugin already folded the document rule into the request's query (its filter-level mode). */
    static final String FILTER_LEVEL_DLS_DONE = "_opendistro_security_filter_level_dls_done";
    /** The request a search filter asks access for; the only kind with a query the document rule can be folded into. */
    static final String SEARCH = "search";

    /** Public methods by class, name and argument classes, looked up once. */
    private static final Map<String, Method> METHODS = new ConcurrentHashMap<>();
    /** Whether a class has a method of a name, looked up once. */
    private static final Map<String, Boolean> HAS = new ConcurrentHashMap<>();

    private InfinoSecurityCalls() {}

    /**
     * The user a request is restricted as: the authenticated user, unless
     * the request is one the security plugin lifts its rules for (no user,
     * a plugin user, an admin certificate, a configuration read), in which
     * case null.
     */
    static Object restrictedUser(ThreadContext context, Object adminDns) throws ReflectiveOperationException {
        Object user = context.getTransient(USER);
        if (user == null || "true".equals(context.getHeader(CONF_REQUEST_HEADER))) {
            return null;
        }
        if ((Boolean) call(user, "isPluginUser") || (adminDns != null && (Boolean) call(adminDns, "isAdmin", user))) {
            return null;
        }
        return user;
    }

    /**
     * Whether the security plugin already folded the document rule into this
     * request's query, so the rule must not be applied twice. Only a search
     * has a query to fold it into: the security plugin's filter-level mode
     * answers a get or multi-get with a search of its own, and a get that
     * still reaches the plugin has nothing that carries the rule, so the
     * header never lifts the rule from one.
     */
    static boolean documentRuleFolded(ThreadContext context, String what) {
        return SEARCH.equals(what) && context.getHeader(FILTER_LEVEL_DLS_DONE) != null;
    }

    /**
     * The component bound under the class of that name, or null when the
     * class or a binding for it is absent. A failure while asking the
     * injector is not an absence and propagates: the caller refuses that
     * request, and since nothing is kept from a failed lookup the next
     * request asks again.
     */
    static Object component(Injector injector, ClassLoader loader, String className) {
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
     * parameter), chosen as Java would choose it: of the methods that fit,
     * the one whose parameters are each at least as specific as every
     * other's. Two that fit with neither more specific (an overload a later
     * release could add) are refused rather than one of them picked. A
     * compiler bridge stands in for its method only when it is the one public
     * way to it (a method inherited from a class that is not public is
     * reached through such a bridge); beside a declared method of the same
     * parameters it is dropped. The method is looked up once per class, name
     * and argument classes.
     */
    static Object call(Object target, String name, Object... args) throws ReflectiveOperationException {
        Class<?> cls = target.getClass();
        StringBuilder key = new StringBuilder(cls.getName()).append('#').append(name);
        for (Object arg : args) {
            key.append('/').append(arg == null ? "null" : arg.getClass().getName());
        }
        Method method = METHODS.get(key.toString());
        if (method == null) {
            Map<List<Class<?>>, Method> bySignature = new LinkedHashMap<>();
            for (Method m : cls.getMethods()) {
                if (m.getName().equals(name) && m.getParameterCount() == args.length && accepts(m, args)) {
                    bySignature.merge(List.of(m.getParameterTypes()), m, (kept, next) -> kept.isBridge() ? next : kept);
                }
            }
            List<Method> fitting = new ArrayList<>(bySignature.values());
            if (fitting.isEmpty()) {
                throw new NoSuchMethodException(cls.getName() + "." + name + " taking " + args.length + " arguments");
            }
            for (Method m : fitting) {
                if (fitting.stream().allMatch(other -> atLeastAsSpecific(m, other))) {
                    method = m;
                    break;
                }
            }
            if (method == null) {
                throw new NoSuchMethodException(cls.getName() + "." + name + " has more than one overload taking these arguments: " + fitting);
            }
            METHODS.put(key.toString(), method);
        }
        return method.invoke(target, args);
    }

    /** Whether each of {@code m}'s parameters is the same type as, or a subtype of, {@code other}'s. */
    private static boolean atLeastAsSpecific(Method m, Method other) {
        Class<?>[] mine = m.getParameterTypes();
        Class<?>[] theirs = other.getParameterTypes();
        for (int i = 0; i < mine.length; i++) {
            if (!theirs[i].isAssignableFrom(mine[i])) {
                return false;
            }
        }
        return true;
    }

    /** Whether {@code target}'s class has a public method of that name taking no arguments (a method that arrived in a later release). */
    static boolean has(Object target, String name) {
        String key = target.getClass().getName() + '?' + name;
        Boolean known = HAS.get(key);
        if (known == null) {
            known = false;
            for (Method m : target.getClass().getMethods()) {
                if (m.getName().equals(name) && m.getParameterCount() == 0) {
                    known = true;
                    break;
                }
            }
            HAS.put(key, known);
        }
        return known;
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

    /** The security plugin's own message when a call threw inside it, the reflective failure otherwise. */
    static String reason(Exception e) {
        Throwable cause = e instanceof InvocationTargetException ite && ite.getCause() != null ? ite.getCause() : e;
        return cause.getMessage() == null ? cause.toString() : cause.getMessage();
    }
}
