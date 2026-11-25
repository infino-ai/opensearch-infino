/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import org.opensearch.test.OpenSearchTestCase;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

/** Work a request starts beside its own, how a failed request lets go of it, and what starting the services checks. */
public class InfinoServicesTests extends OpenSearchTestCase {

    /** Starting the services checks the node's storage: a directory the node may not write stops the start. */
    public void testStartingTheServicesChecksTheStorage() {
        String uri = "file:///var/lib/infino/data";
        String answer = "{\"error\":\"Permission denied (os error 13)\",\"permanent\":true}";
        InfinoNativeTests.withBridge(
            new InfinoNative.Target(uri, null, false, uri),
            Map.of("infino_check_storage", () -> InfinoNativeTests.cString(answer)),
            () -> {
                IllegalStateException e = expectThrows(IllegalStateException.class, () -> new InfinoServices().start());
                assertTrue(e.getMessage(), e.getMessage().contains("cannot use its storage at " + uri));
            }
        );
    }

    /** A failed caller returns only once the started work has finished, and its own failure is the one kept. */
    public void testDrainWaitsForTheStartedWorkAndKeepsTheCallersFailure() throws Exception {
        CompletableFuture<Long> started = new CompletableFuture<>();
        AtomicBoolean finished = new AtomicBoolean();
        // The work finishes only once someone waits on it: a drain that does not wait returns first.
        Thread work = new Thread(() -> {
            while (started.getNumberOfDependents() == 0 && started.isDone() == false) {
                Thread.onSpinWait();
            }
            finished.set(true);
            started.complete(7L);
        });
        work.start();
        try {
            RuntimeException failure = new IllegalArgumentException("page");
            InfinoServices.drain(started, failure);
            assertTrue(finished.get());
            assertEquals(0, failure.getSuppressed().length);
        } finally {
            started.complete(0L);
            work.join();
        }
    }

    /** The started work's own failure is kept beside the caller's, not in place of it. */
    public void testDrainSuppressesTheStartedWorksFailure() {
        RuntimeException count = new IllegalStateException("count");
        RuntimeException failure = new IllegalArgumentException("page");
        InfinoServices.drain(CompletableFuture.failedFuture(count), failure);
        assertArrayEquals(new Throwable[] { count }, failure.getSuppressed());
    }

    /** Settling waits for the started work, returns its failure rather than throwing it, and null when it succeeded. */
    public void testSettleReturnsTheStartedWorksFailure() {
        RuntimeException count = new IllegalStateException("count");
        assertSame(count, InfinoServices.settle(CompletableFuture.failedFuture(count)));
        assertNull(InfinoServices.settle(CompletableFuture.completedFuture(7L)));
        assertNull(InfinoServices.settle(null));
    }

    /** Nothing started, nothing to wait for. */
    public void testDrainOfNothingStartedLeavesTheFailureAlone() {
        RuntimeException failure = new IllegalArgumentException("page");
        InfinoServices.drain(null, failure);
        assertEquals(0, failure.getSuppressed().length);
    }
}
