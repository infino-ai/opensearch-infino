/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import org.opensearch.ExceptionsHelper;
import org.opensearch.action.bulk.BulkItemResponse;
import org.opensearch.action.delete.DeleteRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.concurrency.OpenSearchRejectedExecutionException;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/** How the writer admits a bulk while appends for its table are running. */
public class InfinoWriterTests extends OpenSearchTestCase {

    /** A bulk with no rows to append: it stages without the native bridge. */
    private static InfinoWriter.Waiter emptyBulk(ActionListener<BulkResponse> listener) {
        return new InfinoWriter.Waiter(new ShardId("i", "_na_", 0), "i", List.of(), new BulkItemResponse[0], System.nanoTime(), listener);
    }

    /** A bulk that deletes these ids: it names them, and stages without the native bridge (a delete carries no row). */
    private static InfinoWriter.Waiter deletes(ActionListener<BulkResponse> listener, String... ids) {
        List<InfinoWriter.Outcome> outcomes = new ArrayList<>();
        for (int slot = 0; slot < ids.length; slot++) {
            InfinoWriter.Write delete = new InfinoWriter.Write(ids[slot], null, true);
            outcomes.add(new InfinoWriter.Outcome(slot, new DeleteRequest("i", ids[slot]), delete, null));
        }
        return new InfinoWriter.Waiter(
            new ShardId("i", "_na_", 0),
            "i",
            outcomes,
            new BulkItemResponse[ids.length],
            System.nanoTime(),
            listener
        );
    }

    private static ActionListener<BulkResponse> pending() {
        return ActionListener.wrap(r -> fail("a stalled append answered"), e -> fail(e.toString()));
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

    /** Bulks that name no ids each start an append of their own, up to the limit; past it they wait for a slot. */
    public void testAppendsToOneTableRunConcurrentlyUpToTheLimit() {
        AtomicInteger appendsStarted = new AtomicInteger();
        InfinoWriter writer = new InfinoWriter(() -> task -> appendsStarted.incrementAndGet(), 10, Long.MAX_VALUE, 2);

        writer.submit("t", emptyBulk(pending()));
        writer.submit("t", emptyBulk(pending()));
        assertEquals("two bulks run as two appends at once", 2, appendsStarted.get());

        writer.submit("t", emptyBulk(pending()));
        assertEquals("a third waits for a slot, staged", 2, appendsStarted.get());
    }

    /** Appends of different ids run at once. */
    public void testBulksNamingDifferentIdsAppendConcurrently() {
        AtomicInteger appendsStarted = new AtomicInteger();
        InfinoWriter writer = new InfinoWriter(() -> task -> appendsStarted.incrementAndGet(), 10, Long.MAX_VALUE, 4);

        writer.submit("t", deletes(pending(), "x"));
        writer.submit("t", deletes(pending(), "y"));

        assertEquals(2, appendsStarted.get());
    }

    /**
     * A bulk that names an id a running append writes waits for that append,
     * and a bulk staged after it waits behind it even when it names nothing,
     * so writes to one id reach the table in the order they arrived.
     */
    public void testABulkNamingABusyIdWaitsAndBulksAfterItWaitBehindIt() {
        AtomicInteger appendsStarted = new AtomicInteger();
        InfinoWriter writer = new InfinoWriter(() -> task -> appendsStarted.incrementAndGet(), 10, Long.MAX_VALUE, 4);

        writer.submit("t", deletes(pending(), "x"));
        writer.submit("t", deletes(pending(), "y", "x"));
        assertEquals("the second names x, which the first is writing", 1, appendsStarted.get());

        writer.submit("t", emptyBulk(pending()));
        assertEquals("a bulk after a waiting one waits behind it", 1, appendsStarted.get());

        writer.submit("u", deletes(pending(), "x"));
        assertEquals("another table's ids are its own", 2, appendsStarted.get());
    }

    /**
     * An append the write pool refuses answers its bulk with the refusal (a
     * 429) and gives back its slot and the ids it named, so a later bulk
     * naming them starts its own append. Before, the bulk was never answered,
     * and its ids and slot stayed taken for good.
     */
    public void testAnAppendThePoolRefusesAnswersItsBulkAndFreesItsIdsAndSlot() {
        AtomicInteger refusals = new AtomicInteger(1);
        AtomicInteger appendsStarted = new AtomicInteger();
        Executor pool = task -> {
            if (refusals.getAndDecrement() > 0) {
                throw new OpenSearchRejectedExecutionException("the write queue is full");
            }
            appendsStarted.incrementAndGet();
        };
        InfinoWriter writer = new InfinoWriter(() -> pool, 10, Long.MAX_VALUE, 1);

        AtomicReference<Exception> refused = new AtomicReference<>();
        writer.submit("t", deletes(ActionListener.wrap(r -> fail("a refused append answered as written"), refused::set), "x"));
        Exception e = refused.get();
        assertNotNull("the bulk whose append the pool refused was never answered", e);
        assertEquals("the REST layer answers it with 429", RestStatus.TOO_MANY_REQUESTS, ExceptionsHelper.status(e));

        writer.submit("t", deletes(pending(), "x"));
        assertEquals("x and the table's only slot are free again", 1, appendsStarted.get());
    }
}
