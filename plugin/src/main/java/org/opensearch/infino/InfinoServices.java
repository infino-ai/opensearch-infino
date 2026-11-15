/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import org.opensearch.action.support.AutoCreateIndex;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.inject.Injector;
import org.opensearch.common.settings.IndexScopedSettings;
import org.opensearch.common.util.concurrent.AbstractRunnable;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.common.lifecycle.AbstractLifecycleComponent;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.indices.IndicesService;
import org.opensearch.ingest.IngestService;
import org.opensearch.script.ScriptService;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.function.Consumer;

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
    private static volatile NamedXContentRegistry namedXContentRegistry;
    private static volatile IndexNameExpressionResolver indexNameExpressionResolver;
    private static volatile InfinoWriter writer;
    private static volatile AutoCreateIndex autoCreateIndex;
    private static volatile IngestService ingestService;
    private static volatile IndexScopedSettings indexScopedSettings;
    private static volatile TransportService transportService;
    private static volatile IndicesService indicesService;

    @Inject
    public InfinoServices(
        ClusterService clusterService,
        ThreadPool threadPool,
        ScriptService scriptService,
        NamedWriteableRegistry namedWriteableRegistry,
        NamedXContentRegistry namedXContentRegistry,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Injector injector
    ) {
        InfinoServices.clusterService = clusterService;
        InfinoServices.threadPool = threadPool;
        InfinoServices.scriptService = scriptService;
        InfinoServices.namedWriteableRegistry = namedWriteableRegistry;
        InfinoServices.namedXContentRegistry = namedXContentRegistry;
        InfinoServices.indexNameExpressionResolver = indexNameExpressionResolver;
        InfinoServices.writer = new InfinoWriter(threadPool);
        // The node's own rule for creating an index a write names before it exists.
        InfinoServices.autoCreateIndex = injector.getInstance(AutoCreateIndex.class);
        // The node's ingest pipelines, which a write to the table runs as a stock bulk runs them.
        InfinoServices.ingestService = injector.getInstance(IngestService.class);
        // The index settings this node knows, which a registered index is created with.
        InfinoServices.indexScopedSettings = injector.getInstance(IndexScopedSettings.class);
        // A write to a table another node writes is sent there (see InfinoBulkActionFilter).
        InfinoServices.transportService = injector.getInstance(TransportService.class);
        // The shards on this node, whose shard request cache an account switch clears (see InfinoRequestCache).
        InfinoServices.indicesService = injector.getInstance(IndicesService.class);
        InfinoClient.bind(injector);
        // The security plugin's components, when it runs, come from the same injector.
        InfinoSecurity.bind(injector);
        // Registered cluster state is sent as REST requests, whose bodies the
        // node parses with this, each in the empty context the node starts
        // with (no request has run on this thread yet).
        InfinoClusterState.bind(namedXContentRegistry, threadPool.getThreadContext().newStoredContext(false));
        // The indices come back from the tables on a fresh cluster, and every
        // index's shape is recorded as it moves (see InfinoRegistry).
        clusterService.addListener(InfinoRegistry::onClusterChanged);
        // Which index each table backs, for the tables declared on the applier
        // thread too, where the state may not be read (see InfinoTables).
        clusterService.addListener(event -> InfinoPlugin.tables().observe(event.state().metadata()));
    }

    /** Whether the cluster would create an index a write names before it exists, as it decides for a stock bulk. */
    static AutoCreateIndex autoCreateIndex() {
        return autoCreateIndex;
    }

    /** The node's ingest service, or null before the services are injected. */
    static IngestService ingestService() {
        return ingestService;
    }

    /** The index settings this node knows, or null before the services are injected. */
    static IndexScopedSettings indexScopedSettings() {
        return indexScopedSettings;
    }

    static ClusterService clusterService() {
        return clusterService;
    }

    /** The node's indices, or null before the services are injected. */
    static IndicesService indicesService() {
        return indicesService;
    }

    /** The node's transport, or null before the services are injected. */
    static TransportService transportService() {
        return transportService;
    }

    static ThreadPool threadPool() {
        return threadPool;
    }

    /**
     * Run {@code work} on the node's {@code pool} ({@code search}, {@code get},
     * {@code write}) rather than on the thread that delivered the request.
     * An action filter runs on that thread, an HTTP or transport worker, and
     * an engine call can take seconds; a worker held that long stops reading
     * its other channels, the cluster's health checks among them. The pool
     * runs it under the request's own thread context, and a pool that turns
     * it away (its queue full) hands {@code onFailure} the rejection, which
     * answers 429. Inline when no thread pool is bound, as in a unit test.
     */
    static void fork(String pool, Consumer<Exception> onFailure, Runnable work) {
        ThreadPool pools = threadPool;
        if (pools == null) {
            work.run();
            return;
        }
        pools.executor(pool).execute(new AbstractRunnable() {
            @Override
            protected void doRun() {
                work.run();
            }

            @Override
            public void onFailure(Exception e) {
                onFailure.accept(e);
            }
        });
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

    /** The node's content registry: what a query written as JSON is parsed with. */
    static NamedXContentRegistry namedXContentRegistry() {
        return namedXContentRegistry;
    }

    static IndexNameExpressionResolver indexNameExpressionResolver() {
        return indexNameExpressionResolver;
    }

    static InfinoWriter writer() {
        return writer;
    }

    @Override
    protected void doStart() {
        InfinoScrolls.start(threadPool);
    }

    @Override
    protected void doStop() {
        InfinoScrolls.stop();
    }

    @Override
    protected void doClose() throws IOException {}
}
