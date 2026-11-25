/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.index.Term;
import org.opensearch.action.DocWriteRequest;
import org.opensearch.action.UnavailableShardsException;
import org.opensearch.action.bulk.BulkItemResponse;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.delete.DeleteRequest;
import org.opensearch.action.delete.DeleteResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.common.lucene.uid.Versions;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.concurrency.OpenSearchRejectedExecutionException;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.MediaType;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.VersionType;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.engine.VersionConflictEngineException;
import org.opensearch.index.mapper.IdFieldMapper;
import org.opensearch.index.mapper.ParseContext;
import org.opensearch.index.mapper.ParsedDocument;
import org.opensearch.index.mapper.SeqNoFieldMapper;
import org.opensearch.index.mapper.Uid;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.RemoteTransportException;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * The mark a skeleton rebuild's creates carry in their source: it is found
 * where it is and nowhere else, and the document reads as if it were not
 * there, so a shard's mapper, its translog replay and its history all take
 * the same document the table holds. The claim: a primary takes a create as
 * the rebuild's only when the rebuild has that id in flight with the same
 * content, whether its source reached the engine as sent or written anew by
 * a plugin, and puts the mark back on a source written anew. The creates of
 * a page: each item's answer counts as the rebuild's, a client's, one to
 * send again or a refusal. And the retry of the rebuild's requests: one that
 * fails for a reason that passes is tried again until it goes through or
 * the failures go on too long, a refusal stops the rebuild, and a closed
 * shard or a stopping node ends the wait.
 */
public class InfinoSkeletonAdopterTests extends OpenSearchTestCase {

    private static final String ROW = "{\"n\":1,\"msg\":\"a /*not a mark*/ value\",\"tags\":[\"x\"]}";
    /** What a request answers once it goes through. */
    private static final String ANSWER = "rows";
    /** Failures in a row in the test of the wait's cap: enough for the doubling to pass the cap. */
    private static final int MANY_FAILURES = 20;
    /** Waits a stopping test lets pass before the shard closes. */
    private static final int WAITS_BEFORE_CLOSE = 3;
    /** The give-up time of the tests of it: a few waits at the longest. */
    private static final long GIVE_UP_MILLIS = 5 * 60_000L;
    /** How long a test's run of failures lasts, under its give-up time. */
    private static final long SHORT_FAILING_MILLIS = 2 * 60_000L;

    private static final ShardId SHARD = new ShardId(new Index("logs", "uuid"), 0);
    private static final String ID = "a1";
    private static final String SENT = "{\"message\": \"disk full\", \"status\": 503, \"latency_ms\": 11.3, \"v\": [0.866, 0.0, 0.2887]}";
    /** The same document as a plugin writes it anew: compact, with its keys in another order. */
    private static final String REWRITTEN = "{\"v\":[0.866,0.0,0.2887],\"status\":503,\"latency_ms\":11.3,\"message\":\"disk full\"}";

    public void testAMarkedSourceReadsAsTheRowItWasMadeFrom() {
        String marked = InfinoSkeletonAdopter.marked(ROW);
        assertEquals("{" + InfinoSkeletonAdopter.MARK + ROW.substring(1), marked);
        BytesArray bytes = new BytesArray(marked);
        assertEquals("the content type is still found from the leading brace", XContentType.JSON, MediaTypeRegistry.xContentType(bytes));
        Map<String, Object> read = XContentHelper.convertToMap(bytes, false, XContentType.JSON).v2();
        assertEquals(XContentHelper.convertToMap(new BytesArray(ROW), false, XContentType.JSON).v2(), read);
    }

    public void testOnlyASourceOpeningWithTheMarkIsMarked() {
        assertTrue(InfinoSkeletonAdopter.isMarked(new BytesArray(InfinoSkeletonAdopter.marked(ROW))));
        assertTrue("leading blanks are dropped first", InfinoSkeletonAdopter.isMarked(new BytesArray(InfinoSkeletonAdopter.marked("  \n" + ROW))));
        assertFalse(InfinoSkeletonAdopter.isMarked(new BytesArray(ROW)));
        assertFalse("the mark inside a value is no mark", InfinoSkeletonAdopter.isMarked(new BytesArray("{\"m\":\"" + InfinoSkeletonAdopter.MARK + "\"}")));
        assertFalse("nor one after a blank", InfinoSkeletonAdopter.isMarked(new BytesArray(" {" + InfinoSkeletonAdopter.MARK + "\"n\":1}")));
        assertFalse(InfinoSkeletonAdopter.isMarked(new BytesArray("{")));
        assertFalse(InfinoSkeletonAdopter.isMarked(null));
        assertNull("a row that is no JSON object is not marked", InfinoSkeletonAdopter.marked("[1]"));
    }

    public void testARateLimitedRequestIsTriedAgainUntilItGoesThrough() {
        Fake fake = new Fake();
        AtomicInteger tries = new AtomicInteger();
        String out = fake.retrier.call("reading a page", failingFirst(tries, 1, () -> hosted("server returned 429 Too Many Requests", RestStatus.TOO_MANY_REQUESTS)));
        assertEquals(ANSWER, out);
        assertEquals(2, tries.get());
        assertEquals(1, fake.retrier.retried());
        long waited = fake.now.get();
        assertTrue("waited " + waited, waited >= InfinoSkeletonAdopter.FIRST_BACKOFF_MILLIS / 2 && waited <= InfinoSkeletonAdopter.FIRST_BACKOFF_MILLIS);
    }

    public void testATransportErrorIsTriedAgainUntilItGoesThrough() {
        Fake fake = new Fake();
        AtomicInteger tries = new AtomicInteger();
        // The bridge marks a transport error as the service not answering.
        String out = fake.retrier.call("reading a page", failingFirst(tries, 2, InfinoSkeletonAdopterTests::unavailable));
        assertEquals(ANSWER, out);
        assertEquals(3, tries.get());
        assertEquals(2, fake.retrier.retried());
        assertFalse(fake.stopped.get());
    }

    public void testARefusalStopsTheRebuildAtOnce() {
        for (RestStatus refusal : List.of(RestStatus.BAD_REQUEST, RestStatus.UNAUTHORIZED, RestStatus.FORBIDDEN, RestStatus.NOT_FOUND)) {
            Fake fake = new Fake();
            AtomicInteger tries = new AtomicInteger();
            InfinoNative.InfinoException thrown = expectThrows(
                InfinoNative.InfinoException.class,
                () -> fake.retrier.call("reading a page", failingFirst(tries, 1, () -> hosted("server returned " + refusal.getStatus(), refusal)))
            );
            assertEquals(refusal, thrown.status());
            assertEquals("a refusal is not tried again: " + refusal, 1, tries.get());
            assertEquals(0, fake.retrier.retried());
            assertEquals("nor waited for", 0, fake.now.get());
        }
        Fake fake = new Fake();
        expectThrows(IllegalStateException.class, () -> fake.retrier.call("reading a page", () -> {
            throw new IllegalStateException("a fault of the rebuild, with no status");
        }));
        assertEquals(0, fake.retrier.retried());
        // A bridge error the bridge gives no status and does not mark as the
        // service not answering (a statement that fails, a read of a corrupt
        // file) reads as a 500: it would fail again.
        AtomicInteger tries = new AtomicInteger();
        expectThrows(
            InfinoNative.InfinoException.class,
            () -> fake.retrier.call("reading a page", failingFirst(tries, 1, () -> new InfinoNative.InfinoException("the statement failed")))
        );
        assertEquals(1, tries.get());
        assertEquals(0, fake.retrier.retried());
    }

    public void testAClosedShardEndsTheWait() {
        Fake fake = new Fake();
        AtomicInteger tries = new AtomicInteger();
        fake.closeAfterWaits(WAITS_BEFORE_CLOSE);
        expectThrows(
            InfinoSkeletonAdopter.Stopped.class,
            () -> fake.retrier.call("reading a page", failingFirst(tries, Integer.MAX_VALUE, () -> hosted("server returned 503", RestStatus.SERVICE_UNAVAILABLE)))
        );
        assertEquals("it stops within one wait of the shard closing", WAITS_BEFORE_CLOSE, fake.waits.size());
        assertTrue(fake.waits.stream().allMatch(w -> w <= InfinoSkeletonAdopter.FIRST_BACKOFF_MILLIS));
    }

    public void testAnInterruptedWaitStopsTheRebuild() {
        AtomicLong now = new AtomicLong();
        InfinoSkeletonAdopter.Retrier retrier = new InfinoSkeletonAdopter.Retrier("[test][0]", () -> false, millis -> {
            throw new InterruptedException("the node is stopping");
        }, now::get);
        try {
            expectThrows(
                InfinoSkeletonAdopter.Stopped.class,
                () -> retrier.call("reading a page", failingFirst(new AtomicInteger(), 1, () -> hosted("server returned 429", RestStatus.TOO_MANY_REQUESTS)))
            );
            assertTrue("the interrupt is kept for the pool", Thread.currentThread().isInterrupted());
        } finally {
            Thread.interrupted();
        }
    }

    public void testNoRequestIsSentOnceTheShardClosed() {
        Fake fake = new Fake();
        fake.stopped.set(true);
        AtomicInteger tries = new AtomicInteger();
        expectThrows(InfinoSkeletonAdopter.Stopped.class, () -> fake.retrier.call("reading a page", failingFirst(tries, 0, () -> null)));
        assertEquals(0, tries.get());
    }

    public void testTheWaitDoublesUpToItsCap() {
        Fake fake = new Fake();
        AtomicInteger tries = new AtomicInteger();
        List<Long> waitedBefore = new ArrayList<>();
        AtomicLong last = new AtomicLong();
        String out = fake.retrier.call("reading a page", () -> {
            waitedBefore.add(fake.now.get() - last.get());
            last.set(fake.now.get());
            if (tries.incrementAndGet() <= MANY_FAILURES) {
                throw hosted("server returned 429", RestStatus.TOO_MANY_REQUESTS);
            }
            return ANSWER;
        });
        assertEquals(ANSWER, out);
        assertEquals(MANY_FAILURES, fake.retrier.retried());
        long longest = waitedBefore.stream().mapToLong(Long::longValue).max().orElse(0);
        assertTrue("longest wait " + longest, longest <= InfinoSkeletonAdopter.MAX_BACKOFF_MILLIS);
        assertTrue("the last waits reach the cap's lower half: " + longest, longest >= InfinoSkeletonAdopter.MAX_BACKOFF_MILLIS / 2);
    }

    public void testWhatFailsInAWayThatPasses() {
        assertTrue(InfinoSkeletonAdopter.retryable(hosted("server returned 429", RestStatus.TOO_MANY_REQUESTS)));
        assertTrue(InfinoSkeletonAdopter.retryable(hosted("server returned 502", RestStatus.BAD_GATEWAY)));
        assertTrue("the service not reached or not answering", InfinoSkeletonAdopter.retryable(unavailable()));
        assertFalse("a bridge error with no status", InfinoSkeletonAdopter.retryable(new InfinoNative.InfinoException("transport error")));
        assertFalse(InfinoSkeletonAdopter.retryable(hosted("server returned 500", RestStatus.INTERNAL_SERVER_ERROR)));
        assertTrue("a full write pool", InfinoSkeletonAdopter.retryable(new OpenSearchRejectedExecutionException("rejected")));
        assertTrue("a shard not ready", InfinoSkeletonAdopter.retryable(new UnavailableShardsException(SHARD, "primary shard is not active")));
        assertTrue(InfinoSkeletonAdopter.retryable(RestStatus.TOO_MANY_REQUESTS));
        assertTrue(InfinoSkeletonAdopter.retryable(RestStatus.SERVICE_UNAVAILABLE));
        assertFalse(InfinoSkeletonAdopter.retryable(RestStatus.INTERNAL_SERVER_ERROR));
        assertFalse(InfinoSkeletonAdopter.retryable(RestStatus.CONFLICT));
        assertFalse(InfinoSkeletonAdopter.retryable(RestStatus.BAD_REQUEST));
        assertTrue(
            "a remote failure is read for its cause",
            InfinoSkeletonAdopter.retryable(
                new RemoteTransportException("remote", hosted("server returned 429", RestStatus.TOO_MANY_REQUESTS))
            )
        );
        assertFalse(
            "a client's write of the id won",
            InfinoSkeletonAdopter.retryable(new VersionConflictEngineException(new ShardId("i", "_na_", 0), "1", "document already exists"))
        );
        assertFalse(InfinoSkeletonAdopter.retryable(hosted("server returned 403", RestStatus.FORBIDDEN)));
        assertFalse(InfinoSkeletonAdopter.retryable(new IllegalStateException("no status")));
    }

    public void testFailuresThatGoOnPastTheGiveUpTimeStopTheRebuild() {
        Fake fake = new Fake(GIVE_UP_MILLIS);
        AtomicInteger tries = new AtomicInteger();
        InfinoSkeletonAdopter.GaveUp gaveUp = expectThrows(
            InfinoSkeletonAdopter.GaveUp.class,
            () -> fake.retrier.call("reading a page", failingFirst(tries, Integer.MAX_VALUE, () -> hosted("server returned 429", RestStatus.TOO_MANY_REQUESTS)))
        );
        assertEquals(RestStatus.TOO_MANY_REQUESTS, ((InfinoNative.InfinoException) gaveUp.getCause()).status());
        assertTrue("gave up at " + fake.now.get(), fake.now.get() >= GIVE_UP_MILLIS);
        assertTrue("within a wait of the give-up time: " + fake.now.get(), fake.now.get() < GIVE_UP_MILLIS + InfinoSkeletonAdopter.MAX_BACKOFF_MILLIS);
    }

    public void testARequestThatGoesThroughStartsTheGiveUpTimeAgain() {
        Fake fake = new Fake(GIVE_UP_MILLIS);
        // Runs of failures, each shorter than the give-up time but together far
        // longer, with a request going through between them.
        for (int run = 0; run < 5; run++) {
            long until = fake.now.get() + SHORT_FAILING_MILLIS;
            assertEquals(ANSWER, fake.retrier.call("reading a page", () -> {
                if (fake.now.get() < until) {
                    throw hosted("server returned 503", RestStatus.SERVICE_UNAVAILABLE);
                }
                return ANSWER;
            }));
        }
        assertTrue(fake.now.get() > GIVE_UP_MILLIS);
    }

    public void testARebuildCreateIsClaimedOnceAndNotAfterItsAnswer() {
        Map<String, String> inFlight = inFlight();
        InfinoSkeletonAdopter.track(SHARD, inFlight);
        try {
            assertTrue(InfinoSkeletonAdopter.claim(SHARD, ID, new BytesArray(SENT), MediaTypeRegistry.JSON));
            assertFalse("claimed once", InfinoSkeletonAdopter.claim(SHARD, ID, new BytesArray(SENT), MediaTypeRegistry.JSON));
        } finally {
            InfinoSkeletonAdopter.untrack(SHARD, inFlight);
        }
        Map<String, String> answered = inFlight();
        InfinoSkeletonAdopter.track(SHARD, answered);
        InfinoSkeletonAdopter.untrack(SHARD, answered);
        assertFalse("a stale create, once its bulk is answered", InfinoSkeletonAdopter.claim(SHARD, ID, new BytesArray(SENT), MediaTypeRegistry.JSON));
    }

    public void testASourceWrittenAnewIsStillTheRebuilds() throws IOException {
        Map<String, String> inFlight = inFlight();
        InfinoSkeletonAdopter.track(SHARD, inFlight);
        try {
            assertTrue(InfinoSkeletonAdopter.claim(SHARD, ID, new BytesArray(REWRITTEN), MediaTypeRegistry.JSON));
        } finally {
            InfinoSkeletonAdopter.untrack(SHARD, inFlight);
        }
        assertTrue("in another content type", InfinoSkeletonAdopter.sameContent(SENT, smile(), XContentType.SMILE));
        assertTrue("sent with the mark", InfinoSkeletonAdopter.sameContent(InfinoSkeletonAdopter.marked(SENT), new BytesArray(REWRITTEN), null));
    }

    public void testAClientsWriteIsNotTheRebuilds() {
        Map<String, String> inFlight = inFlight();
        InfinoSkeletonAdopter.track(SHARD, inFlight);
        try {
            BytesReference other = new BytesArray("{\"message\": \"disk full\", \"status\": 500, \"latency_ms\": 11.3, \"v\": [0.866, 0.0, 0.2887]}");
            assertFalse("other content", InfinoSkeletonAdopter.claim(SHARD, ID, other, MediaTypeRegistry.JSON));
            assertFalse("another id", InfinoSkeletonAdopter.claim(SHARD, "a2", new BytesArray(SENT), MediaTypeRegistry.JSON));
            ShardId otherShard = new ShardId(SHARD.getIndex(), 1);
            assertFalse("another shard", InfinoSkeletonAdopter.claim(otherShard, ID, new BytesArray(SENT), MediaTypeRegistry.JSON));
            assertFalse("no source", InfinoSkeletonAdopter.claim(SHARD, ID, null, MediaTypeRegistry.JSON));
            assertFalse("not a document", InfinoSkeletonAdopter.claim(SHARD, ID, new BytesArray("not json"), MediaTypeRegistry.JSON));
            // None of them took the claim: the rebuild's own create still has it.
            assertTrue(InfinoSkeletonAdopter.claim(SHARD, ID, new BytesArray(SENT), MediaTypeRegistry.JSON));
        } finally {
            InfinoSkeletonAdopter.untrack(SHARD, inFlight);
        }
    }

    public void testThePrimaryPutsTheMarkBackOnASourceWrittenAnew() {
        String sent = InfinoSkeletonAdopter.marked(SENT);
        Map<String, String> inFlight = new ConcurrentHashMap<>(Map.of(ID, sent));
        InfinoSkeletonAdopter.track(SHARD, inFlight);
        try {
            // A plugin wrote the source anew before the engine saw it, dropping the mark.
            Engine.Index create = primaryCreate(ID, REWRITTEN);
            assertEquals(InfinoSkeletonAdopter.Kind.REBUILD, InfinoSkeletonAdopter.kind(SHARD, create));
            assertTrue("the translog and the history keep the mark", InfinoSkeletonAdopter.isMarked(create.source()));
            assertEquals(
                XContentHelper.convertToMap(new BytesArray(REWRITTEN), false, XContentType.JSON).v2(),
                XContentHelper.convertToMap(create.source(), false, XContentType.JSON).v2()
            );
            assertEquals("claimed once", InfinoSkeletonAdopter.Kind.CLIENT, InfinoSkeletonAdopter.kind(SHARD, primaryCreate(ID, REWRITTEN)));
        } finally {
            InfinoSkeletonAdopter.untrack(SHARD, inFlight);
        }
    }

    public void testWhoseWriteTheEngineTakesEachFor() {
        String sent = InfinoSkeletonAdopter.marked(SENT);
        Map<String, String> inFlight = new ConcurrentHashMap<>(Map.of(ID, sent));
        InfinoSkeletonAdopter.track(SHARD, inFlight);
        try {
            assertEquals("a forged mark", InfinoSkeletonAdopter.Kind.FORGED, InfinoSkeletonAdopter.kind(SHARD, primaryCreate("a2", sent)));
            assertEquals("a client's write", InfinoSkeletonAdopter.Kind.CLIENT, InfinoSkeletonAdopter.kind(SHARD, primaryCreate("a2", SENT)));
            assertEquals(
                "an index of the id that carries the mark, not a create, is refused",
                InfinoSkeletonAdopter.Kind.FORGED,
                InfinoSkeletonAdopter.kind(SHARD, operation(ID, sent, Engine.Operation.Origin.PRIMARY, Versions.MATCH_ANY))
            );
            Engine.Index create = primaryCreate(ID, sent);
            assertEquals(InfinoSkeletonAdopter.Kind.REBUILD, InfinoSkeletonAdopter.kind(SHARD, create));
            assertEquals("a source that kept its mark is left as sent", sent, create.source().utf8ToString());
        } finally {
            InfinoSkeletonAdopter.untrack(SHARD, inFlight);
        }
        // A replay and a replica take the mark as the primary left it, with nothing in flight.
        assertEquals(InfinoSkeletonAdopter.Kind.REBUILD, InfinoSkeletonAdopter.kind(SHARD, replayed(ID, sent)));
        assertEquals(InfinoSkeletonAdopter.Kind.CLIENT, InfinoSkeletonAdopter.kind(SHARD, replayed(ID, SENT)));
    }

    public void testACreateRunsNoPipeline() throws Exception {
        IndexRequest create = InfinoSkeletonAdopter.createOf("logs", ID, null, InfinoSkeletonAdopter.marked(SENT));
        assertEquals("_none", create.getPipeline());
        assertEquals("not the index's final pipeline either", "_none", create.getFinalPipeline());
        assertTrue("so the bulk does not read them from the index's settings", create.isPipelineResolved());
        assertEquals(DocWriteRequest.OpType.CREATE, create.opType());
        Method system = systemPipelineGetter();
        if (system != null) {
            assertEquals("nor a system one", "_none", system.invoke(create));
        }
    }

    public void testAPageOfCreatesCountsEachItemsAnswer() {
        Map<String, InfinoSkeletonAdopter.Row> missing = new LinkedHashMap<>();
        for (String id : List.of("ok", "busy", "refused", "client", "gone", "notjson")) {
            missing.put(id, new InfinoSkeletonAdopter.Row(id.equals("notjson") ? "[1]" : "{\"n\":\"" + id + "\"}", null));
        }
        Bulks bulks = new Bulks();
        // The first bulk: one each of what an item may answer.
        bulks.answer(Map.of("ok", 201, "busy", 429, "refused", 400, "client", 409, "gone", 201));
        // The busy one, sent again on its own.
        bulks.answer(Map.of("busy", 201));
        // The deletes of the rows the table lost meanwhile.
        bulks.answer(Map.of("gone", 200));
        List<List<String>> asked = new ArrayList<>();
        InfinoSkeletonAdopter.Created out = InfinoSkeletonAdopter.createMissing(SHARD, "t", missing, new Fake().retrier, bulks, ids -> {
            asked.add(List.copyOf(ids));
            return Set.of("ok", "busy");
        });
        assertEquals("created, each once", 3, out.created());
        assertEquals("the refused row and the one that is no JSON object", 2, out.refused());
        assertNotNull(out.firstRefusal());
        assertEquals(List.of(List.of("ok", "busy", "refused", "client", "gone"), List.of("busy"), List.of("gone")), bulks.ids);
        assertTrue(bulks.sent.get(2).requests().stream().allMatch(r -> r instanceof DeleteRequest));
        assertEquals("a client's write that got there first is not checked against the table", List.of(List.of("ok", "busy", "gone")), asked);
        assertTrue("every send was the rebuild's to claim", bulks.claimed);
    }

    public void testACreateSentAgainAfterALostAnswerCountsAsCreated() {
        Map<String, InfinoSkeletonAdopter.Row> missing = new LinkedHashMap<>();
        missing.put("a", new InfinoSkeletonAdopter.Row("{\"n\":1}", null));
        missing.put("b", new InfinoSkeletonAdopter.Row("{\"n\":2}", null));
        Bulks bulks = new Bulks();
        // The creates applied, but their answer was lost; sent again, each meets itself.
        bulks.fail(new OpenSearchRejectedExecutionException("the answer was lost"));
        bulks.answer(Map.of("a", 409, "b", 409));
        bulks.answer(Map.of("b", 200));
        List<List<String>> asked = new ArrayList<>();
        InfinoSkeletonAdopter.Created out = InfinoSkeletonAdopter.createMissing(SHARD, "t", missing, new Fake().retrier, bulks, ids -> {
            asked.add(List.copyOf(ids));
            return Set.of("a");
        });
        assertEquals(2, out.created());
        assertEquals(0, out.refused());
        assertEquals("checked against the table, so a row deleted meanwhile leaves no document", List.of(List.of("a", "b")), asked);
        assertEquals(List.of("b"), bulks.ids.get(bulks.ids.size() - 1));
    }

    public void testItemsThatKeepFailingGiveUpRatherThanLoop() {
        Map<String, InfinoSkeletonAdopter.Row> missing = Map.of("a", new InfinoSkeletonAdopter.Row("{\"n\":1}", null));
        Fake fake = new Fake(GIVE_UP_MILLIS);
        Function<BulkRequest, BulkResponse> busy = bulk -> response(bulk, Map.of("a", 429));
        expectThrows(
            InfinoSkeletonAdopter.GaveUp.class,
            () -> InfinoSkeletonAdopter.createMissing(SHARD, "t", missing, fake.retrier, busy, ids -> Set.copyOf(ids))
        );
    }

    private static Map<String, String> inFlight() {
        Map<String, String> inFlight = new ConcurrentHashMap<>();
        inFlight.put(ID, SENT);
        return inFlight;
    }

    private static BytesReference smile() throws IOException {
        try (XContentBuilder b = XContentBuilder.builder(XContentType.SMILE.xContent())) {
            b.startObject()
                .field("message", "disk full")
                .field("status", 503)
                .field("latency_ms", 11.3)
                .field("v", List.of(0.866, 0.0, 0.2887))
                .endObject();
            return BytesReference.bytes(b);
        }
    }

    private static Engine.Index primaryCreate(String id, String source) {
        return operation(id, source, Engine.Operation.Origin.PRIMARY, Versions.MATCH_DELETED);
    }

    private static Engine.Index replayed(String id, String source) {
        return operation(id, source, Engine.Operation.Origin.LOCAL_TRANSLOG_RECOVERY, 1);
    }

    private static Engine.Index operation(String id, String source, Engine.Operation.Origin origin, long version) {
        boolean primary = origin == Engine.Operation.Origin.PRIMARY;
        ParsedDocument doc = new ParsedDocument(
            new NumericDocValuesField("_version", 0),
            SeqNoFieldMapper.SequenceIDFields.emptySeqID(),
            id,
            null,
            List.of(new ParseContext.Document()),
            new BytesArray(source),
            MediaTypeRegistry.JSON,
            null
        );
        return new Engine.Index(
            new Term(IdFieldMapper.NAME, Uid.encodeId(id)),
            doc,
            primary ? SequenceNumbers.UNASSIGNED_SEQ_NO : 1,
            1,
            version,
            primary ? VersionType.INTERNAL : null,
            origin,
            0,
            -1,
            false,
            SequenceNumbers.UNASSIGNED_SEQ_NO,
            0
        );
    }

    private static Method systemPipelineGetter() {
        try {
            return IndexRequest.class.getMethod("getSystemIngestPipeline");
        } catch (NoSuchMethodException e) {
            return null;
        }
    }

    /** The answer to {@code bulk}: each id's status from {@code statuses} (2xx goes through). */
    private static BulkResponse response(BulkRequest bulk, Map<String, Integer> statuses) {
        List<DocWriteRequest<?>> requests = bulk.requests();
        BulkItemResponse[] items = new BulkItemResponse[requests.size()];
        for (int i = 0; i < items.length; i++) {
            DocWriteRequest<?> r = requests.get(i);
            RestStatus status = RestStatus.fromCode(statuses.get(r.id()));
            if (status.getStatus() < RestStatus.MULTIPLE_CHOICES.getStatus()) {
                items[i] = new BulkItemResponse(
                    i,
                    r.opType(),
                    r instanceof DeleteRequest ? new DeleteResponse(SHARD, r.id(), 1, 1, 2, true) : new IndexResponse(SHARD, r.id(), 1, 1, 1, true)
                );
            } else {
                Exception cause = new IllegalStateException("answered " + status.getStatus());
                items[i] = new BulkItemResponse(i, r.opType(), new BulkItemResponse.Failure(SHARD.getIndexName(), r.id(), cause, status));
            }
        }
        return new BulkResponse(items, 1);
    }

    /**
     * The cluster's answers to the rebuild's bulks, in turn: each records the
     * ids it was sent, and claims each create as the primary's engine would.
     */
    private static final class Bulks implements Function<BulkRequest, BulkResponse> {
        private final Deque<Function<BulkRequest, BulkResponse>> answers = new ArrayDeque<>();
        final List<BulkRequest> sent = new ArrayList<>();
        final List<List<String>> ids = new ArrayList<>();
        boolean claimed = true;

        void answer(Map<String, Integer> statuses) {
            answers.add(bulk -> response(bulk, statuses));
        }

        void fail(RuntimeException e) {
            answers.add(bulk -> {
                throw e;
            });
        }

        @Override
        public BulkResponse apply(BulkRequest bulk) {
            sent.add(bulk);
            List<String> these = new ArrayList<>();
            for (DocWriteRequest<?> r : bulk.requests()) {
                these.add(r.id());
                if (r instanceof IndexRequest create) {
                    claimed &= InfinoSkeletonAdopter.claim(SHARD, create.id(), create.source(), (MediaType) create.getContentType());
                }
            }
            ids.add(these);
            return answers.remove().apply(bulk);
        }
    }

    /** A bridge error as the bridge marks the hosted service not reached or not answering. */
    private static InfinoNative.InfinoException unavailable() {
        return new InfinoNative.InfinoException(
            "the hosted service could not be reached or did not answer in time; try again later: transport error",
            RestStatus.SERVICE_UNAVAILABLE,
            true
        );
    }

    private static InfinoNative.InfinoException hosted(String message, RestStatus status) {
        return new InfinoNative.InfinoException(message, status);
    }

    /** A request that fails {@code failures} times with {@code failure} and then answers {@link #ANSWER}, counting its tries. */
    private static Supplier<String> failingFirst(AtomicInteger tries, int failures, Supplier<RuntimeException> failure) {
        return () -> {
            if (tries.incrementAndGet() <= failures) {
                throw failure.get();
            }
            return ANSWER;
        };
    }

    /** A retrier over a clock that moves only as it waits, and a shard a test closes. */
    private static final class Fake {
        final AtomicLong now = new AtomicLong();
        final AtomicBoolean stopped = new AtomicBoolean();
        final List<Long> waits = new ArrayList<>();
        int closeAfter = Integer.MAX_VALUE;
        final InfinoSkeletonAdopter.Retrier retrier;

        Fake() {
            this(InfinoSkeletonAdopter.GIVE_UP_MILLIS);
        }

        Fake(long giveUpMillis) {
            retrier = new InfinoSkeletonAdopter.Retrier("[test][0] from table t", stopped::get, millis -> {
                waits.add(millis);
                now.addAndGet(millis);
                if (waits.size() >= closeAfter) {
                    stopped.set(true);
                }
            }, now::get, giveUpMillis);
        }

        void closeAfterWaits(int n) {
            closeAfter = n;
        }
    }
}
