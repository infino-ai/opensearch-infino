/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import org.opensearch.ExceptionsHelper;
import org.opensearch.action.DocWriteRequest;
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
import org.opensearch.index.mapper.MapperParsingException;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.common.settings.Settings;

import com.sun.jna.Memory;
import com.sun.jna.Pointer;

import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
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
     * A bulk whose batch cannot be prepared because the hosted service could
     * not be reached or did not answer in time has each write answered with
     * 503 and the bridge's reason, which a client retries; before, it was a
     * 500 that named no cause.
     */
    public void testABulkTheUnreachableServiceCannotPrepareIsAnswered503WithTheCause() {
        String reason = "the hosted service could not be reached or did not answer in time; try again later: "
            + "unavailable: schema(t): schema: transport error: timeout: per read";
        InfinoNative.useLib((InfinoLib) Proxy.newProxyInstance(InfinoLib.class.getClassLoader(), new Class<?>[] { InfinoLib.class }, (proxy, method, args) -> switch (method.getName()) {
            case "infino_open_with" -> new Memory(1);
            case "infino_prepare_error" -> nativeString("{\"error\":\"" + reason + "\",\"unavailable\":true,\"status\":503}");
            default -> null; // the prepare itself returns null, and the frees
        }));
        try {
            InfinoWriter writer = new InfinoWriter(() -> task -> fail("an unprepared bulk started an append"), 10, Long.MAX_VALUE);
            AtomicReference<BulkResponse> answered = new AtomicReference<>();
            InfinoWriter.Write index = new InfinoWriter.Write("d1", "{\"n\":1}", false, false);
            InfinoWriter.Waiter bulk = new InfinoWriter.Waiter(
                new ShardId("i", "_na_", 0),
                "i",
                List.of(new InfinoWriter.Outcome(0, new IndexRequest("i").id("d1"), index, null)),
                new BulkItemResponse[1],
                System.nanoTime(),
                ActionListener.wrap(answered::set, e -> fail("the bulk failed whole: " + e))
            );

            writer.submit("t", bulk);

            assertNotNull("the bulk was not answered", answered.get());
            BulkItemResponse item = answered.get().getItems()[0];
            assertTrue(item.isFailed());
            assertEquals(RestStatus.SERVICE_UNAVAILABLE, item.status());
            assertTrue(item.getFailureMessage(), item.getFailureMessage().contains("the batch for t could not be prepared"));
            assertTrue(item.getFailureMessage(), item.getFailureMessage().contains("did not answer in time"));
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

    /** The bridge's reason for a document the hosted service turned away for the account's request rate. */
    private static final String RATE_LIMITED = "backend: append: server returned 429: rate limit exceeded";

    /** A generated-id document written to slot {@code slot} of a bulk: it names no id, so nothing is looked up. */
    private static InfinoWriter.Outcome generated(int slot, String id) {
        IndexRequest request = new IndexRequest("i").id(id).source("{\"n\":1}", MediaTypeRegistry.JSON);
        return new InfinoWriter.Outcome(slot, request, new InfinoWriter.Write(id, "{\"n\":1}", false, false), null);
    }

    /**
     * A bulk of three generated-id documents (slots 0, 1 and 3) and an item
     * refused up front with 400 (slot 2), written through the writer by an
     * append the bridge answers with {@code append}: the bulk's response, or
     * null when it failed whole, with the failure in {@code failed}.
     */
    private static BulkResponse bulkAnswered(String append, AtomicReference<Exception> failed) {
        InfinoNative.useLib((InfinoLib) Proxy.newProxyInstance(InfinoLib.class.getClassLoader(), new Class<?>[] { InfinoLib.class }, (proxy, method, args) -> switch (method.getName()) {
            case "infino_open_with", "infino_prepare_batch" -> new Memory(1);
            case "infino_prepared_failed", "infino_sql_latest" -> nativeString("[]"); // every row converts; the table is empty
            case "infino_append_prepared" -> nativeString(append);
            default -> null; // the latest hold and its release, and the frees
        }));
        try {
            InfinoWriter writer = new InfinoWriter(() -> Runnable::run, 100, Long.MAX_VALUE);
            BulkItemResponse refusedUpFront = new BulkItemResponse(
                2,
                DocWriteRequest.OpType.INDEX,
                new BulkItemResponse.Failure("i", "bad", new IllegalArgumentException("[infino] scripted updates are not served"), RestStatus.BAD_REQUEST)
            );
            List<InfinoWriter.Outcome> outcomes = List.of(
                generated(0, "g1"),
                generated(1, "g2"),
                new InfinoWriter.Outcome(2, new IndexRequest("i").id("bad"), null, refusedUpFront),
                generated(3, "g3")
            );
            AtomicReference<BulkResponse> answered = new AtomicReference<>();
            writer.submit("t", new InfinoWriter.Waiter(SHARD, "i", outcomes, new BulkItemResponse[4], System.nanoTime(), ActionListener.wrap(answered::set, failed::set)));
            assertTrue("the bulk was not answered", answered.get() != null || failed.get() != null);
            return answered.get();
        } finally {
            InfinoNative.useLib(null);
        }
    }

    /**
     * A document of a bulk the hosted service turned away for the account's
     * request rate is answered 429, which Logstash, Beats, Fluent Bit and
     * Data Prepper send again, and not 400, which they drop as a bad
     * document. A document the table refuses is still a 400, the bulk says
     * {@code errors: true}, the documents written are 201 and the item
     * refused up front keeps its answer.
     */
    public void testADocumentTheServiceRateLimitsIs429AndARefusedOneStays400() {
        AtomicReference<Exception> failed = new AtomicReference<>();
        BulkResponse response = bulkAnswered(
            "{\"ok\":1,\"failed\":["
                + "{\"b\":0,\"i\":1,\"reason\":\"" + RATE_LIMITED + "\",\"permanent\":false,\"status\":429},"
                + "{\"b\":0,\"i\":2,\"reason\":\"field [n] takes no string\",\"permanent\":true,\"status\":null}]}",
            failed
        );
        assertNull(String.valueOf(failed.get()), failed.get());
        assertTrue("errors: true", response.hasFailures());
        BulkItemResponse[] items = response.getItems();
        assertFalse(items[0].getFailureMessage(), items[0].isFailed());
        assertEquals(RestStatus.CREATED, items[0].status());
        assertEquals(RestStatus.TOO_MANY_REQUESTS, items[1].status());
        assertTrue(items[1].getFailure().getCause() instanceof OpenSearchRejectedExecutionException);
        assertTrue(items[1].getFailureMessage(), items[1].getFailureMessage().contains("rate limit exceeded"));
        assertEquals("the item refused up front keeps its answer", "bad", items[2].getId());
        assertEquals(RestStatus.BAD_REQUEST, items[2].status());
        assertEquals(RestStatus.BAD_REQUEST, items[3].status());
        assertTrue(items[3].getFailure().getCause() instanceof IllegalArgumentException);
        assertTrue(items[3].getFailureMessage(), items[3].getFailureMessage().contains("takes no string"));
    }

    /**
     * An append the hosted service did not answer (no answer at all, or a
     * 502, 503 or 504, which the bridge marks {@code unavailable}) has each
     * of its writes answered 503, and one it turned away whole for the
     * account's request rate each answered 429: item by item, so the bulk
     * says {@code errors: true}, the item refused up front keeps its 400 and
     * a client sends again only the writes. Any other failure still fails
     * the bulk whole.
     */
    public void testAnAppendTheServiceDidNotTakeAnswersEachWrite503Or429() {
        String unavailable = "{\"error\":\"the hosted service could not be reached or did not answer in time; try again later: "
            + "unavailable: append(t): transport error: timeout\",\"unavailable\":true}";
        Map<String, RestStatus> cases = new LinkedHashMap<>();
        cases.put(unavailable, RestStatus.SERVICE_UNAVAILABLE);
        for (int status : new int[] { 502, 503, 504 }) {
            cases.put(
                "{\"error\":\"the hosted service could not be reached or did not answer in time; try again later: "
                    + "unavailable: append: server returned " + status + ": x\",\"unavailable\":true,\"status\":" + status + "}",
                RestStatus.SERVICE_UNAVAILABLE
            );
        }
        cases.put("{\"error\":\"" + RATE_LIMITED + "\",\"status\":429}", RestStatus.TOO_MANY_REQUESTS);
        for (Map.Entry<String, RestStatus> c : cases.entrySet()) {
            AtomicReference<Exception> failed = new AtomicReference<>();
            BulkResponse response = bulkAnswered(c.getKey(), failed);
            assertNull(c.getKey() + ": " + failed.get(), failed.get());
            assertTrue(c.getKey(), response.hasFailures());
            BulkItemResponse[] items = response.getItems();
            for (int slot : new int[] { 0, 1, 3 }) {
                assertEquals(c.getKey(), c.getValue(), items[slot].status());
            }
            assertEquals(c.getKey(), RestStatus.BAD_REQUEST, items[2].status());
        }

        AtomicReference<Exception> failed = new AtomicReference<>();
        assertNull(bulkAnswered("{\"error\":\"append: storage failed\"}", failed));
        assertEquals(RestStatus.INTERNAL_SERVER_ERROR, ExceptionsHelper.status(failed.get()));
    }

    /**
     * A document refused for a column its batch was typed without is
     * answered 503 for a later try, but the service answered that append:
     * it ended nothing, so the bulk's deletes are still sent. Only a status
     * the service ended the append with, a rate limit or no answer, holds
     * them back.
     */
    public void testAStaleSchemaRefusalDoesNotEndTheAppend() {
        InfinoNative.PreparedFailure stale = new InfinoNative.PreparedFailure(
            0,
            0,
            "column `@timestamp` was added since the batch was typed",
            false,
            RestStatus.SERVICE_UNAVAILABLE,
            true
        );
        InfinoNative.PreparedFailure limited = new InfinoNative.PreparedFailure(
            0,
            1,
            RATE_LIMITED,
            false,
            RestStatus.TOO_MANY_REQUESTS,
            false
        );
        assertNull(InfinoWriter.endedBy(Map.of("a", stale)));
        Map<String, InfinoNative.PreparedFailure> both = new LinkedHashMap<>();
        both.put("a", stale);
        both.put("b", limited);
        assertEquals(RestStatus.TOO_MANY_REQUESTS, InfinoWriter.endedBy(both));
    }

    /**
     * An append the hosted service ended partway, by not answering a call
     * (503) or turning one away for the account's request rate (429),
     * answers only the documents it did not write with that status: the one
     * an earlier call wrote is 201, so a client that sends the failed items
     * again does not store it twice. The bulk's delete is not sent to the
     * same service after it; it is answered with the same status, for the
     * client to send again.
     */
    public void testAnAppendTheServiceEndedPartwayAnswersOnlyWhatItDidNotWrite() {
        String unanswered = "the hosted service could not be reached or did not answer in time; try again later: unavailable: append: timeout";
        for (RestStatus status : new RestStatus[] { RestStatus.SERVICE_UNAVAILABLE, RestStatus.TOO_MANY_REQUESTS }) {
            String reason = status == RestStatus.SERVICE_UNAVAILABLE ? unanswered : RATE_LIMITED;
            String append = "{\"ok\":1,\"failed\":[{\"b\":0,\"i\":1,\"reason\":\"" + reason + "\",\"permanent\":false,\"status\":"
                + status.getStatus() + "}]}";
            AtomicInteger deletes = new AtomicInteger();
            InfinoNative.useLib((InfinoLib) Proxy.newProxyInstance(InfinoLib.class.getClassLoader(), new Class<?>[] { InfinoLib.class }, (proxy, method, args) -> switch (method.getName()) {
                case "infino_open_with", "infino_prepare_batch" -> new Memory(1);
                case "infino_prepared_failed" -> nativeString("[]");
                case "infino_sql", "infino_sql_latest" -> nativeString("[{\"osid\":\"old\"}]"); // the deleted id holds a row
                case "infino_append_prepared" -> nativeString(append);
                case "infino_delete_osids" -> {
                    deletes.incrementAndGet();
                    yield 0;
                }
                default -> null; // the latest hold and its release, and the frees
            }));
            try {
                InfinoWriter writer = new InfinoWriter(() -> Runnable::run, 100, Long.MAX_VALUE);
                List<InfinoWriter.Outcome> outcomes = List.of(
                    generated(0, "g1"),
                    generated(1, "g2"),
                    new InfinoWriter.Outcome(2, new DeleteRequest("i", "old"), new InfinoWriter.Write("old", null, true), null)
                );
                AtomicReference<BulkResponse> answered = new AtomicReference<>();
                AtomicReference<Exception> failed = new AtomicReference<>();
                writer.submit("t", new InfinoWriter.Waiter(SHARD, "i", outcomes, new BulkItemResponse[3], System.nanoTime(), ActionListener.wrap(answered::set, failed::set)));
                assertNull(String.valueOf(failed.get()), failed.get());
                BulkItemResponse[] items = answered.get().getItems();
                assertEquals(status + ": written by an earlier call", RestStatus.CREATED, items[0].status());
                assertEquals(status, items[1].status());
                assertTrue(items[1].getFailureMessage(), items[1].getFailureMessage().contains(status == RestStatus.SERVICE_UNAVAILABLE ? "did not answer" : "rate limit exceeded"));
                assertEquals(status + ": the delete is answered with the append's status", status, items[2].status());
                assertEquals(status + ": the delete is not sent", 0, deletes.get());
            } finally {
                InfinoNative.useLib(null);
            }
        }
    }

    /**
     * A delete the hosted service turned away for the account's request
     * rate is a 429, one it did not answer a 503, and any other failure a
     * 500, as each delete of the bulk is answered.
     */
    public void testADeleteTheServiceDidNotTakeIs429Or503() {
        assertEquals(RestStatus.TOO_MANY_REQUESTS, ExceptionsHelper.status(InfinoNative.deleteFailed("t", 2, InfinoNative.DELETE_RATE_LIMITED)));
        RuntimeException unavailable = InfinoNative.deleteFailed("t", 2, InfinoNative.DELETE_UNAVAILABLE);
        assertEquals(RestStatus.SERVICE_UNAVAILABLE, ExceptionsHelper.status(unavailable));
        assertTrue(((InfinoNative.InfinoException) unavailable).unavailable());
        assertEquals(RestStatus.INTERNAL_SERVER_ERROR, ExceptionsHelper.status(InfinoNative.deleteFailed("t", 2, -2)));
    }

    private static Pointer nativeString(String s) {
        byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
        Memory m = new Memory(bytes.length + 1);
        m.write(0, bytes, 0, bytes.length);
        m.setByte(bytes.length, (byte) 0);
        return m;
    }

    /** Lucene's refusal of a keyword value past one term, as the bridge gives it. */
    private static final String IMMENSE =
        "Document contains at least one immense term in field=\"w\" (whose UTF8 encoding is longer than the max length 32766), all of which were skipped.";
    /** Lucene's refusal of a value past one term in a keyword with doc values only. */
    private static final String DOC_VALUES = "DocValuesField \"v\" is too large, must be <= 32766";

    /**
     * A keyword value too long for one term fails as stock fails it, an illegal argument with Lucene's
     * reason as it stands, in both modes; any other refusal keeps its own answer.
     */
    public void testAValueTooLongForOneTermIsRefusedWithLucenesReasonInBothModes() {
        for (String why : List.of(IMMENSE, DOC_VALUES)) {
            assertTrue(why, InfinoWriter.tooLongForOneTerm(why));
            for (Exception e : List.of(InfinoWriter.refused(why), InfinoWriter.refusedAtWrite(why))) {
                assertEquals(IllegalArgumentException.class, e.getClass());
                assertEquals(why, e.getMessage());
                assertEquals(RestStatus.BAD_REQUEST, ExceptionsHelper.status(e));
            }
        }
        String other = "field [n]: not a number";
        assertFalse(InfinoWriter.tooLongForOneTerm(other));
        assertEquals("[infino] " + other, InfinoWriter.refused(other).getMessage());
        assertEquals(IllegalArgumentException.class, InfinoWriter.refused(other).getClass());
        Exception atWrite = InfinoWriter.refusedAtWrite(other);
        assertEquals(MapperParsingException.class, atWrite.getClass());
        assertEquals("[infino] " + other, atWrite.getMessage());
        // A reason that only mentions the words further in is not Lucene's refusal.
        assertFalse(InfinoWriter.tooLongForOneTerm("field [w]: " + IMMENSE));
    }
}
