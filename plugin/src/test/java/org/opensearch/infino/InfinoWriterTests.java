/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import org.opensearch.ExceptionsHelper;
import org.opensearch.action.bulk.BulkItemResponse;
import org.opensearch.action.delete.DeleteRequest;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.update.UpdateRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.concurrency.OpenSearchRejectedExecutionException;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.index.engine.DocumentMissingException;
import org.opensearch.index.engine.VersionConflictEngineException;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.common.settings.Settings;

import com.sun.jna.Memory;
import com.sun.jna.Pointer;

import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
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

    /** How long a test waits for a step that should take moments. */
    private static final long WAIT_SECONDS = 5;

    /** A bulk with no rows to append: it stages without the native bridge. */
    private static InfinoWriter.Waiter emptyBulk(ActionListener<BulkResponse> listener) {
        return new InfinoWriter.Waiter(new ShardId("i", "_na_", 0), "i", List.of(), new BulkItemResponse[0], System.nanoTime(), listener);
    }

    /** A bulk that deletes these ids: it names them, and stages without the native bridge (a delete carries no row). */
    private static InfinoWriter.Waiter deletes(ActionListener<BulkResponse> listener, String... ids) {
        return deletesFrom("_na_", listener, ids);
    }

    /** {@link #deletes} from the index of this UUID, under the same name as every other. */
    private static InfinoWriter.Waiter deletesFrom(String uuid, ActionListener<BulkResponse> listener, String... ids) {
        List<InfinoWriter.Outcome> outcomes = new ArrayList<>();
        for (int slot = 0; slot < ids.length; slot++) {
            InfinoWriter.Write delete = new InfinoWriter.Write(ids[slot], null, true);
            outcomes.add(new InfinoWriter.Outcome(slot, new DeleteRequest("i", ids[slot]), delete, null));
        }
        return new InfinoWriter.Waiter(
            new ShardId("i", uuid, 0),
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

    /**
     * A table's appends run on the plugin's append pool, not the write pool.
     * An append to the hosted service can wait for minutes, and appends held
     * on the write pool took every thread of it, so a write to any index on
     * the node, a stock one included, waited behind them.
     */
    public void testAnAppendRunsOnTheAppendPoolNotTheWritePool() throws Exception {
        ThreadPool threadPool = new TestThreadPool(getTestName(), InfinoPlugin.appendPool(Settings.EMPTY));
        // A bridge that opens and answers nothing else: the append has no rows to write.
        InfinoNative.useLib(
            (InfinoLib) Proxy.newProxyInstance(
                InfinoLib.class.getClassLoader(),
                new Class<?>[] { InfinoLib.class },
                (proxy, method, args) -> "infino_open_with".equals(method.getName()) ? new Memory(1) : null
            )
        );
        try {
            InfinoWriter writer = new InfinoWriter(threadPool);
            CompletableFuture<String> answeredOn = new CompletableFuture<>();
            writer.submit(
                "t",
                emptyBulk(
                    ActionListener.wrap(
                        r -> answeredOn.complete(Thread.currentThread().getName()),
                        e -> answeredOn.complete(Thread.currentThread().getName())
                    )
                )
            );
            String thread = answeredOn.get(WAIT_SECONDS, TimeUnit.SECONDS);
            assertTrue(thread, thread.contains("[" + InfinoPlugin.APPEND_POOL + "]"));
        } finally {
            InfinoNative.useLib(null);
            terminate(threadPool);
        }
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
     * Appends running at once write one index. A bulk to an index created
     * again under the table's name waits while appends of the old index run,
     * and a bulk staged behind it waits too, even one to the old index: the
     * sequence numbers of the appends running are read for one index.
     */
    public void testABulkToAnotherIndexOfTheTableWaitsForTheRunningAppendsOfTheOld() {
        AtomicInteger appendsStarted = new AtomicInteger();
        InfinoWriter writer = new InfinoWriter(() -> task -> appendsStarted.incrementAndGet(), 10, Long.MAX_VALUE, 4);

        writer.submit("t", deletesFrom("old", pending(), "x"));
        writer.submit("t", deletesFrom("old", pending(), "y"));
        assertEquals("two appends of one index run at once", 2, appendsStarted.get());

        writer.submit("t", deletesFrom("new", pending(), "z"));
        assertEquals("a bulk of the index created again waits for the old one's appends", 2, appendsStarted.get());

        writer.submit("t", deletesFrom("old", pending(), "w"));
        assertEquals("a bulk staged behind it waits behind it", 2, appendsStarted.get());

        writer.submit("u", deletesFrom("new", pending(), "z"));
        assertEquals("another table's appends are its own", 3, appendsStarted.get());
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

    /** A table's next number is read afresh for a new index of its name, and never goes back for the same index. */
    public void testTheNextSequenceNumberStartsAgainForANewIndexOnly() {
        assertEquals(0L, InfinoWriter.nextSeq(-1L, 0L, false));
        assertEquals(0L, InfinoWriter.nextSeq(-1L, 0L, true));
        // The copy writing the same index moved: numbers given to running appends stay taken.
        assertEquals(7L, InfinoWriter.nextSeq(7L, 3L, true));
        assertEquals(9L, InfinoWriter.nextSeq(7L, 9L, true));
        // The index was deleted and created again under the table's name: its first write is 0.
        assertEquals(0L, InfinoWriter.nextSeq(7L, 0L, false));
    }

    /** A node that has not written a table, or whose copy closed, has not caught up with its latest commit. */
    public void testANodeThatHasNotCommittedToATableIsNotCaughtUp() {
        InfinoWriter writer = new InfinoWriter(() -> task -> {}, 10, Long.MAX_VALUE);
        assertFalse(writer.caughtUp("t", "alloc-1"));
        assertFalse(writer.caughtUp("t", null));
        writer.stopReadingLatest("t");
        assertFalse(writer.caughtUp("t", "alloc-1"));
    }

    /**
     * A node catches up with a table once an append it began as a copy has
     * committed, and only as that copy: a promotion, which starts writing the
     * table as another, reads the latest commit again until its own first
     * commit, and so does a copy that closed. An append begun before the
     * setting changed does not clear it.
     */
    public void testANodeIsCaughtUpOnlyAfterItsOwnCommitAsTheSameCopy() {
        InfinoWriter writer = new InfinoWriter(() -> task -> {}, 10, Long.MAX_VALUE);
        long first = writer.readingLatestAs("t", "alloc-1");
        assertFalse("before its first commit", writer.caughtUp("t", "alloc-1"));
        writer.committedUnder("t", first);
        assertTrue("after its first commit", writer.caughtUp("t", "alloc-1"));
        assertFalse("as another copy", writer.caughtUp("t", "alloc-2"));

        long promoted = writer.readingLatestAs("t", "alloc-2");
        assertFalse("the old copy after a promotion", writer.caughtUp("t", "alloc-1"));
        assertFalse("the new copy before its first commit", writer.caughtUp("t", "alloc-2"));
        writer.committedUnder("t", first);
        assertFalse("an append begun before the promotion commits", writer.caughtUp("t", "alloc-2"));
        writer.committedUnder("t", promoted);
        assertTrue("the new copy after its first commit", writer.caughtUp("t", "alloc-2"));

        writer.stopReadingLatest("t");
        assertFalse("once the copy closes", writer.caughtUp("t", "alloc-2"));
    }

    private static final ShardId SHARD = new ShardId("i", "_na_", 0);

    private static InfinoWriter.Outcome update(String id, long ifSeqNo) {
        UpdateRequest request = new UpdateRequest("i", id).doc("{\"a\":1}", MediaTypeRegistry.JSON);
        if (ifSeqNo >= 0) {
            request.setIfSeqNo(ifSeqNo).setIfPrimaryTerm(1);
        }
        return new InfinoWriter.Outcome(0, request, InfinoWriter.Write.update(request), null);
    }

    private static InfinoWriter.Outcome index(String id, long ifSeqNo, boolean create) {
        IndexRequest request = new IndexRequest("i").id(id).source("{\"a\":1}", MediaTypeRegistry.JSON).create(create);
        if (ifSeqNo >= 0) {
            request.setIfSeqNo(ifSeqNo).setIfPrimaryTerm(1);
        }
        return new InfinoWriter.Outcome(0, request, new InfinoWriter.Write(id, "{\"a\":1}", true, create), null);
    }

    private static InfinoWriter.Outcome delete(String id, long ifSeqNo) {
        DeleteRequest request = new DeleteRequest("i", id);
        if (ifSeqNo >= 0) {
            request.setIfSeqNo(ifSeqNo).setIfPrimaryTerm(1);
        }
        return new InfinoWriter.Outcome(0, request, new InfinoWriter.Write(id, null, true), null);
    }

    /** A conditioned update of no document is missing (404), as stock reads the document before it checks the condition. */
    public void testAConditionedUpdateOfNoDocumentIsMissingNotAConflict() {
        Exception e = InfinoWriter.rejection(SHARD, update("1", 1), null);
        assertTrue(e.toString(), e instanceof DocumentMissingException);
        assertEquals(RestStatus.NOT_FOUND, ExceptionsHelper.status(e));
    }

    /** An index or a delete of no document conditioned on a sequence number conflicts (409), as stock answers them. */
    public void testAConditionedIndexOrDeleteOfNoDocumentConflicts() {
        for (InfinoWriter.Outcome o : List.of(index("1", 1, false), delete("1", 1))) {
            Exception e = InfinoWriter.rejection(SHARD, o, null);
            assertTrue(e.toString(), e instanceof VersionConflictEngineException);
            assertEquals(RestStatus.CONFLICT, ExceptionsHelper.status(e));
        }
    }

    /** A document at sequence number 0 meets a condition on 0 and conflicts with one on any other number. */
    public void testADocumentAtSequenceNumberZeroIsCheckedAsAnyOther() {
        assertNull(InfinoWriter.rejection(SHARD, update("1", 0), 0L));
        assertNull(InfinoWriter.rejection(SHARD, index("1", 0, false), 0L));
        assertNull(InfinoWriter.rejection(SHARD, delete("1", 0), 0L));
        Exception e = InfinoWriter.rejection(SHARD, update("1", 234), 0L);
        assertTrue(e.toString(), e instanceof VersionConflictEngineException);
    }

    /** Without a condition an update of no document is left to its merge (an upsert makes one), and a create over a document conflicts. */
    public void testAnUnconditionedUpdateGoesOnAndACreateOverADocumentConflicts() {
        assertNull(InfinoWriter.rejection(SHARD, update("1", -1), null));
        Exception e = InfinoWriter.rejection(SHARD, index("1", -1, true), 0L);
        assertTrue(e.toString(), e instanceof VersionConflictEngineException);
        assertNull(InfinoWriter.rejection(SHARD, index("1", -1, true), null));
    }

    /**
     * A batch's updates read the documents they merge into in one call: the
     * updates of ids the table holds, and nothing else. An id the read no
     * longer finds stands at no document, so its update meets no base.
     */
    public void testABatchReadsItsUpdatesBasesInOneCall() {
        UpdateRequest update = new UpdateRequest("i", "u").doc("{\"a\":1}", MediaTypeRegistry.JSON);
        List<InfinoWriter.Write> writes = List.of(
            new InfinoWriter.Write("u1", null, true, false, update, List.of()),
            new InfinoWriter.Write("u2", null, true, false, update, List.of()),
            new InfinoWriter.Write("gone", null, true, false, update, List.of()),
            new InfinoWriter.Write("new", null, true, false, update, List.of()),
            new InfinoWriter.Write("replaced", "{}", true, false)
        );
        List<Set<String>> reads = new ArrayList<>();
        Map<String, String> bases = InfinoWriter.updateBases(writes, Set.of("u1", "u2", "gone", "replaced"), ids -> {
            reads.add(Set.copyOf(ids));
            return Map.of("u1", new InfinoNative.Doc("{\"b\":1}", 1, null), "u2", new InfinoNative.Doc("{\"b\":2}", 2, null));
        });
        assertEquals(List.of(Set.of("u1", "u2", "gone")), reads);
        assertEquals("{\"b\":1}", bases.get("u1"));
        assertEquals("{\"b\":2}", bases.get("u2"));
        assertTrue(bases.containsKey("gone"));
        assertNull(bases.get("gone"));
        assertFalse(bases.containsKey("new"));
        assertFalse(bases.containsKey("replaced"));
    }

    /** A batch with no update of a document the table holds reads no base at all. */
    public void testABatchWithoutUpdatesReadsNoBases() {
        List<InfinoWriter.Write> writes = List.of(new InfinoWriter.Write("d", "{}", true, false));
        Map<String, String> bases = InfinoWriter.updateBases(writes, Set.of("d"), ids -> {
            throw new AssertionError("read " + ids);
        });
        assertTrue(bases.isEmpty());
    }

    /**
     * A bulk whose batch cannot be prepared because the hosted service could
     * not be reached or did not answer in time is answered with 503 and the
     * bridge's reason, which a client retries; before, it was a 500 that
     * named no cause.
     */
    public void testABulkTheUnreachableServiceCannotPrepareIsAnswered503WithTheCause() {
        String reason = "the hosted service could not be reached or did not answer in time; try again later: "
            + "unavailable: schema(t): schema: transport error: timeout: per read";
        InfinoNative.useLib((InfinoLib) Proxy.newProxyInstance(InfinoLib.class.getClassLoader(), new Class<?>[] { InfinoLib.class }, (proxy, method, args) -> switch (method.getName()) {
            case "infino_open_with" -> new Memory(1);
            case "infino_prepare_error" -> nativeString("{\"error\":\"" + reason + "\",\"status\":503}");
            default -> null; // the prepare itself returns null, and the frees
        }));
        try {
            InfinoWriter writer = new InfinoWriter(() -> task -> fail("an unprepared bulk started an append"), 10, Long.MAX_VALUE);
            AtomicReference<Exception> failure = new AtomicReference<>();
            InfinoWriter.Write index = new InfinoWriter.Write("d1", "{\"n\":1}", false, false);
            InfinoWriter.Waiter bulk = new InfinoWriter.Waiter(
                new ShardId("i", "_na_", 0),
                "i",
                List.of(new InfinoWriter.Outcome(0, new IndexRequest("i").id("d1"), index, null)),
                new BulkItemResponse[1],
                System.nanoTime(),
                ActionListener.wrap(r -> fail("an unprepared bulk answered"), failure::set)
            );

            writer.submit("t", bulk);

            Exception e = failure.get();
            assertNotNull("the bulk was not answered", e);
            assertEquals(RestStatus.SERVICE_UNAVAILABLE, ExceptionsHelper.status(e));
            assertTrue(e.getMessage(), e.getMessage().contains("the batch for t could not be prepared"));
            assertTrue(e.getMessage(), e.getMessage().contains("did not answer in time"));
        } finally {
            InfinoNative.useLib(null);
        }
    }

    /**
     * An append the hosted service did not answer fails as a whole with 503,
     * which a client retries, and not as documents refused with 400. (That
     * the bridge stops at the first such write, without splitting the batch,
     * is checked by the bridge's own tests.)
     */
    public void testAnAppendTheServiceDidNotAnswerFailsWhole503() {
        String reason = "the hosted service could not be reached or did not answer in time; try again later: "
            + "unavailable: append(t): transport error: timeout: per read";
        InfinoNative.useLib((InfinoLib) Proxy.newProxyInstance(InfinoLib.class.getClassLoader(), new Class<?>[] { InfinoLib.class }, (proxy, method, args) -> switch (method.getName()) {
            case "infino_open_with" -> new Memory(1);
            case "infino_append_prepared" -> nativeString("{\"error\":\"" + reason + "\",\"status\":503}");
            default -> null; // the frees
        }));
        try {
            InfinoNative.InfinoException e = expectThrows(
                InfinoNative.InfinoException.class,
                () -> InfinoNative.appendPrepared("t", List.of(new Memory(1)), List.of(), null)
            );
            assertEquals(RestStatus.SERVICE_UNAVAILABLE, ExceptionsHelper.status(e));
            assertTrue(e.getMessage(), e.getMessage().contains("did not answer in time"));
        } finally {
            InfinoNative.useLib(null);
        }
    }

    private static Pointer nativeString(String s) {
        byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
        Memory m = new Memory(bytes.length + 1);
        m.write(0, bytes, 0, bytes.length);
        m.setByte(bytes.length, (byte) 0);
        return m;
    }
}
