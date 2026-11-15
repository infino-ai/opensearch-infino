/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import org.opensearch.test.OpenSearchTestCase;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

/** Work a request starts beside its own, and how a failed request lets go of it. */
public class InfinoServicesTests extends OpenSearchTestCase {

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

    /** Nothing started, nothing to wait for. */
    public void testDrainOfNothingStartedLeavesTheFailureAlone() {
        RuntimeException failure = new IllegalArgumentException("page");
        InfinoServices.drain(null, failure);
        assertEquals(0, failure.getSuppressed().length);
    }
}
