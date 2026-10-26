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

    /** Public methods by class, name and arity, looked up once. */
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

    /** The component bound under the class of that name, or null when the class or a binding for it is absent. */
    static Object component(Injector injector, ClassLoader loader, String className) {
        try {
            Class<?> cls = Class.forName(className, false, loader);
            List<? extends Binding<?>> bindings = injector.findBindingsByType(TypeLiteral.get(cls));
            return bindings.isEmpty() ? null : bindings.get(0).getProvider().get();
        } catch (ClassNotFoundException e) {
            return null;
        }
    }

    /** The public method of {@code name} with that many parameters on the class of that name, or null when absent. */
    static Method method(ClassLoader loader, String className, String name, int arity) {
        try {
            for (Method m : Class.forName(className, false, loader).getMethods()) {
                if (m.getName().equals(name) && m.getParameterCount() == arity) {
                    return m;
                }
            }
        } catch (ClassNotFoundException e) {
            return null;
        }
        return null;
    }

    /**
     * Call the public method of {@code name} on {@code target} whose
     * parameters take {@code args} (a null argument fits any reference
     * parameter); the method is looked up once per class, name and arity.
     */
    static Object call(Object target, String name, Object... args) throws ReflectiveOperationException {
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
