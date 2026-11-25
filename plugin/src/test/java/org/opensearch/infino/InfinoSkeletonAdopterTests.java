/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.concurrency.OpenSearchRejectedExecutionException;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.index.engine.VersionConflictEngineException;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.RemoteTransportException;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

/**
 * The mark a skeleton rebuild's creates carry in their source: it is found
 * where it is and nowhere else, and the document reads as if it were not
 * there, so a shard's mapper, its translog replay and its history all take
 * the same document the table holds. And the retry of the rebuild's
 * requests: one that fails for a reason that passes is tried again until it
 * goes through, a refusal stops the rebuild, and a closed shard or a
 * stopping node ends the wait.
 */
public class InfinoSkeletonAdopterTests extends OpenSearchTestCase {

    private static final String ROW = "{\"n\":1,\"msg\":\"a /*not a mark*/ value\",\"tags\":[\"x\"]}";
    /** What a request answers once it goes through. */
    private static final String ANSWER = "rows";
    /** Failures in a row in the test of the wait's cap: enough for the doubling to pass the cap. */
    private static final int MANY_FAILURES = 20;
    /** Waits a stopping test lets pass before the shard closes. */
    private static final int WAITS_BEFORE_CLOSE = 3;

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
        // The bridge answers a transport error with no status, which reads as a 500.
        String out = fake.retrier.call(
            "reading a page",
            failingFirst(tries, 2, () -> new InfinoNative.InfinoException("error sending request for url (https://service/sql): transport error"))
        );
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
        assertTrue(InfinoSkeletonAdopter.retryable(new InfinoNative.InfinoException("transport error")));
        assertTrue("a full write pool", InfinoSkeletonAdopter.retryable(new OpenSearchRejectedExecutionException("rejected")));
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
        final InfinoSkeletonAdopter.Retrier retrier = new InfinoSkeletonAdopter.Retrier("[test][0] from table t", stopped::get, millis -> {
            waits.add(millis);
            now.addAndGet(millis);
            if (waits.size() >= closeAfter) {
                stopped.set(true);
            }
        }, now::get);

        void closeAfterWaits(int n) {
            closeAfter = n;
        }
    }
}
