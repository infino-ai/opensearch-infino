/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import org.opensearch.ExceptionsHelper;
import org.opensearch.action.bulk.BulkItemResponse;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.concurrency.OpenSearchRejectedExecutionException;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/** How the writer admits a bulk while an append for its table is running. */
public class InfinoWriterTests extends OpenSearchTestCase {

    /** A bulk with no rows to append: it stages without the native bridge. */
    private static InfinoWriter.Waiter emptyBulk(ActionListener<BulkResponse> listener) {
        return new InfinoWriter.Waiter(new ShardId("i", "_na_", 0), "i", List.of(), new BulkItemResponse[0], System.nanoTime(), listener);
    }

    /**
     * A bulk that arrives while an append is running and the batch behind it
     * is past its bound is refused with 429 at once. Before, the calling
     * thread (a transport worker) waited for the append, without a timeout, so
     * an append that never returned left the node reading no requests at all.
     */
    public void testABulkPastTheBoundWhileAnAppendRunsIsRefusedWith429() throws Exception {
        // Appends are handed to an executor that never runs them: the first
        // bulk's append stays running for the rest of the test, as a stalled
        // one would.
        AtomicInteger appendsStarted = new AtomicInteger();
        InfinoWriter writer = new InfinoWriter(() -> task -> appendsStarted.incrementAndGet(), 0, Long.MAX_VALUE);

        writer.submit("t", emptyBulk(ActionListener.wrap(r -> fail("the stalled append answered"), e -> fail(e.toString()))));
        assertEquals("the first bulk starts an append", 1, appendsStarted.get());

        AtomicReference<Exception> refused = new AtomicReference<>();
        ExecutorService caller = Executors.newSingleThreadExecutor();
        try {
            Future<?> second = caller.submit(
                () -> writer.submit("t", emptyBulk(ActionListener.wrap(r -> fail("a refused bulk answered"), refused::set)))
            );
            second.get(5, TimeUnit.SECONDS);
        } finally {
            caller.shutdownNow();
        }

        Exception e = refused.get();
        assertNotNull("the second bulk was neither staged nor refused", e);
        assertTrue(e.toString(), e instanceof OpenSearchRejectedExecutionException);
        assertEquals("the REST layer answers it with 429", RestStatus.TOO_MANY_REQUESTS, ExceptionsHelper.status(e));
        assertTrue(e.getMessage(), e.getMessage().contains("table t"));
        assertEquals("a refused bulk starts no append", 1, appendsStarted.get());
    }

    /** Under its bound, a bulk joins the batch behind a running append, and starts no second one. */
    public void testABulkUnderTheBoundJoinsTheRunningAppendsBatch() {
        AtomicInteger appendsStarted = new AtomicInteger();
        InfinoWriter writer = new InfinoWriter(() -> task -> appendsStarted.incrementAndGet(), 10, Long.MAX_VALUE);
        CompletableFuture<Exception> failure = new CompletableFuture<>();

        writer.submit("t", emptyBulk(ActionListener.wrap(r -> {}, failure::complete)));
        writer.submit("t", emptyBulk(ActionListener.wrap(r -> {}, failure::complete)));

        assertFalse("a bulk under the bound was refused", failure.isDone());
        assertEquals(1, appendsStarted.get());
    }
}
