/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import org.opensearch.index.engine.EngineConfig;
import org.opensearch.index.translog.TranslogManager;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;

/**
 * {@link InfinoEngine} with the translog sync hooked, as 3.x syncs: a shard
 * syncs through its engine's translog manager, so the manager the engine
 * hands out commits the pending batch before it syncs. The manager's
 * interface grows from release to release, so every other call goes to the
 * engine's own manager unchanged; 2.x syncs through the engine itself and
 * has a copy of this class of its own.
 */
final class InfinoSyncedEngine extends InfinoEngine {
    /** The name of the call a shard syncs its translog for a write through. */
    private static final String SYNC_CALL = "ensureTranslogSynced";

    private volatile TranslogManager wrapped;
    private volatile TranslogManager wrappedOf;

    InfinoSyncedEngine(EngineConfig config) {
        super(config);
    }

    @Override
    public TranslogManager translogManager() {
        TranslogManager inner = super.translogManager();
        if (inner == null) {
            return null;
        }
        TranslogManager w = wrapped;
        if (w != null && wrappedOf == inner) {
            return w;
        }
        w = (TranslogManager) Proxy.newProxyInstance(
            TranslogManager.class.getClassLoader(),
            new Class<?>[] { TranslogManager.class },
            (proxy, method, args) -> {
                if (SYNC_CALL.equals(method.getName())) {
                    commitBeforeSync();
                }
                try {
                    return method.invoke(inner, args);
                } catch (InvocationTargetException e) {
                    throw e.getCause();
                }
            }
        );
        wrappedOf = inner;
        wrapped = w;
        return w;
    }
}
