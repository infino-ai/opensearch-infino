/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import org.apache.logging.log4j.LogManager;
import org.opensearch.common.metrics.MeanMetric;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.index.shard.RefreshListeners;
import org.opensearch.index.translog.Translog;
import org.opensearch.test.OpenSearchTestCase;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class RefreshReleaseTests extends OpenSearchTestCase {

    private static RefreshListeners listeners() {
        RefreshListeners l = new RefreshListeners(
            () -> 100,
            () -> { throw new AssertionError("no forced refresh expected"); },
            LogManager.getLogger(RefreshReleaseTests.class),
            new ThreadContext(Settings.EMPTY),
            new MeanMetric()
        );
        l.setCurrentRefreshLocationSupplier(() -> { throw new AssertionError("the release pins the location"); });
        return l;
    }

    private static Translog.Location at(long offset) {
        return new Translog.Location(1, offset, 10);
    }

    /**
     * The refresh itself, under Lucene's lock, does not wait; the release
     * after it waits out the read window before a wait_for request goes, and
     * a request that comes in between is held too rather than let through.
     */
    public void testWaitForIsReleasedOnlyAfterTheReadWindowAndTheLockIsNotHeldForIt() throws Exception {
        TimeValue window = TimeValue.timeValueMillis(400);
        InfinoNative.readWindow(window);
        try {
            String table = "release_waits";
            RefreshListeners waiting = listeners();
            AtomicInteger commits = new AtomicInteger();
            RefreshRelease release = new RefreshRelease(table, waiting, () -> at(100), commits::incrementAndGet);

            AtomicBoolean first = new AtomicBoolean();
            assertFalse(waiting.addOrNotify(at(50), forced -> first.set(true)));

            long written = System.nanoTime();
            InfinoNative.wrote(table, written);
            release.beforeRefresh();
            release.afterRefresh(true);
            assertTrue("the refresh lock is not held for the read window", System.nanoTime() - written < window.nanos() / 2);
            assertFalse(first.get());

            AtomicBoolean second = new AtomicBoolean();
            assertFalse("a request the refresh covers waits for the release", waiting.addOrNotify(at(80), forced -> second.set(true)));

            release.release();
            assertTrue("the release waits out the read window", System.nanoTime() - written >= window.nanos());
            assertEquals(1, commits.get());
            assertTrue(first.get());
            assertTrue(second.get());

            AtomicBoolean third = new AtomicBoolean();
            assertTrue("once released, a covered request goes at once", waiting.addOrNotify(at(90), forced -> third.set(true)));
            assertTrue(third.get());
            AtomicBoolean beyond = new AtomicBoolean();
            assertFalse("a request past the refresh waits for the next", waiting.addOrNotify(at(150), forced -> beyond.set(true)));
            assertFalse(beyond.get());
        } finally {
            InfinoNative.readWindow(InfinoPlugin.PLATFORM_READ_WINDOW.get(Settings.EMPTY));
        }
    }

    /** A release that finishes after a later one does not take the shard's location back. */
    public void testAnEarlierReleaseDoesNotUndoALaterOne() throws Exception {
        RefreshListeners waiting = listeners();
        AtomicReference<Translog.Location> end = new AtomicReference<>(at(100));
        RefreshRelease release = new RefreshRelease("release_order", waiting, end::get, () -> {});
        release.beforeRefresh();
        release.afterRefresh(true);
        release.release();
        // A refresh over an empty translog leaves the released location as it was.
        end.set(null);
        release.beforeRefresh();
        release.afterRefresh(true);
        release.release();
        AtomicBoolean covered = new AtomicBoolean();
        assertTrue(waiting.addOrNotify(at(100), forced -> covered.set(true)));
        assertTrue(covered.get());
    }
}
