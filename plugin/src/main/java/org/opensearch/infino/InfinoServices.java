/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.common.lifecycle.AbstractLifecycleComponent;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.script.ScriptService;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;

/**
 * The node services the plugin's filters need, taken by injection rather
 * than through {@code Plugin.createComponents}, whose client parameter
 * changed package between OpenSearch 2.x and 3.x; this way one source tree
 * builds against both.
 */
public final class InfinoServices extends AbstractLifecycleComponent {

    private static volatile ClusterService clusterService;
    private static volatile ThreadPool threadPool;
    private static volatile ScriptService scriptService;
    private static volatile NamedWriteableRegistry namedWriteableRegistry;
    private static volatile IndexNameExpressionResolver indexNameExpressionResolver;
    private static volatile InfinoWriter writer;

    @Inject
    public InfinoServices(
        ClusterService clusterService,
        ThreadPool threadPool,
        ScriptService scriptService,
        NamedWriteableRegistry namedWriteableRegistry,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        InfinoServices.clusterService = clusterService;
        InfinoServices.threadPool = threadPool;
        InfinoServices.scriptService = scriptService;
        InfinoServices.namedWriteableRegistry = namedWriteableRegistry;
        InfinoServices.indexNameExpressionResolver = indexNameExpressionResolver;
        InfinoServices.writer = new InfinoWriter(threadPool);
    }

    static ClusterService clusterService() {
        return clusterService;
    }

    static ThreadPool threadPool() {
        return threadPool;
    }

    /** The current request's thread context, or null before the services are injected. */
    static ThreadContext threadContext() {
        return threadPool == null ? null : threadPool.getThreadContext();
    }

    static ScriptService scriptService() {
        return scriptService;
    }

    static NamedWriteableRegistry namedWriteableRegistry() {
        return namedWriteableRegistry;
    }

    /** For tests that build aggregation objects without a node. */
    static void namedWriteableRegistry(NamedWriteableRegistry registry) {
        namedWriteableRegistry = registry;
    }

    static IndexNameExpressionResolver indexNameExpressionResolver() {
        return indexNameExpressionResolver;
    }

    static InfinoWriter writer() {
        return writer;
    }

    @Override
    protected void doStart() {}

    @Override
    protected void doStop() {}

    @Override
    protected void doClose() throws IOException {}
}
