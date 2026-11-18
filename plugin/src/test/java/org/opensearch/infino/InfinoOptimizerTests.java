/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.Scheduler;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

public class InfinoOptimizerTests extends OpenSearchTestCase {

    /** A task the fake scheduler was given, and whether it was cancelled. */
    private static final class Scheduled implements Scheduler.Cancellable {
        final Runnable run;
        final TimeValue interval;
        boolean cancelled;

        Scheduled(Runnable run, TimeValue interval) {
            this.run = run;
            this.interval = interval;
        }

        @Override
        public boolean cancel() {
            cancelled = true;
            return true;
        }

        @Override
        public boolean isCancelled() {
            return cancelled;
        }
    }

    private final List<Scheduled> scheduled = new ArrayList<>();
    private final InfinoOptimizer optimizer = new InfinoOptimizer((run, interval) -> {
        Scheduled s = new Scheduled(run, interval);
        scheduled.add(s);
        return s;
    });

    private static final Index LOGS = new Index("logs", "uuid-logs");
    private static final Index METRICS = new Index("metrics", "uuid-metrics");
    private static final TimeValue HOUR = TimeValue.timeValueHours(1);
    private static final TimeValue MINUTE = TimeValue.timeValueMinutes(1);

    public void testAnIntervalChangeReschedulesTheIndexsStartedShards() {
        ShardId logs0 = new ShardId(LOGS, 0);
        ShardId logs1 = new ShardId(LOGS, 1);
        AtomicInteger logs0Runs = new AtomicInteger();
        AtomicInteger logs1Runs = new AtomicInteger();
        optimizer.start(logs0, logs0Runs::incrementAndGet, HOUR);
        optimizer.start(logs1, logs1Runs::incrementAndGet, HOUR);

        optimizer.intervalChanged(LOGS, MINUTE);

        assertEquals(MINUTE, optimizer.interval(logs0));
        assertEquals(MINUTE, optimizer.interval(logs1));
        assertEquals(4, scheduled.size());
        assertTrue(scheduled.get(0).cancelled);
        assertTrue(scheduled.get(1).cancelled);
        assertEquals(MINUTE, scheduled.get(2).interval);
        assertFalse(scheduled.get(2).cancelled);
        assertEquals(MINUTE, scheduled.get(3).interval);
        assertFalse(scheduled.get(3).cancelled);

        // The map's order decides which replacement is which shard's, so run
        // one once and the other twice: each must reach a different callback.
        Scheduled first = scheduled.get(2);
        Scheduled second = scheduled.get(3);
        first.run.run();
        second.run.run();
        second.run.run();
        assertEquals(List.of(1, 2), Stream.of(logs0Runs.get(), logs1Runs.get()).sorted().toList());
    }

    public void testAnIntervalChangeLeavesOtherIndicesAlone() {
        ShardId metrics0 = new ShardId(METRICS, 0);
        optimizer.start(metrics0, () -> {}, HOUR);

        optimizer.intervalChanged(LOGS, MINUTE);

        assertEquals(HOUR, optimizer.interval(metrics0));
        assertEquals(1, scheduled.size());
        assertFalse(scheduled.get(0).cancelled);
    }

    public void testTheSameIntervalKeepsTheTask() {
        ShardId logs0 = new ShardId(LOGS, 0);
        optimizer.start(logs0, () -> {}, HOUR);

        optimizer.intervalChanged(LOGS, HOUR);

        assertEquals(1, scheduled.size());
        assertFalse(scheduled.get(0).cancelled);
    }

    public void testAStoppedShardIsNotRescheduled() {
        ShardId logs0 = new ShardId(LOGS, 0);
        optimizer.start(logs0, () -> {}, HOUR);
        optimizer.stop(logs0);

        optimizer.intervalChanged(LOGS, MINUTE);

        assertNull(optimizer.interval(logs0));
        assertEquals(1, scheduled.size());
        assertTrue(scheduled.get(0).cancelled);
    }

    public void testARestartReplacesTheShardsTask() {
        ShardId logs0 = new ShardId(LOGS, 0);
        optimizer.start(logs0, () -> {}, HOUR);
        optimizer.start(logs0, () -> {}, MINUTE);

        assertTrue(scheduled.get(0).cancelled);
        assertEquals(MINUTE, optimizer.interval(logs0));
    }

    /** An optimize whose first call blocks until released, counting every call. */
    private static final class SlowFirstRun implements Runnable {
        final CountDownLatch entered = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);
        final AtomicInteger calls = new AtomicInteger();

        @Override
        public void run() {
            if (calls.incrementAndGet() == 1) {
                entered.countDown();
                try {
                    release.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    public void testAReplacementThatFiresDuringARunIsSkipped() throws Exception {
        ShardId logs0 = new ShardId(LOGS, 0);
        SlowFirstRun optimize = new SlowFirstRun();
        optimizer.start(logs0, optimize, HOUR);
        Thread first = new Thread(scheduled.get(0).run);
        first.start();
        assertTrue(optimize.entered.await(10, TimeUnit.SECONDS));

        try {
            optimizer.intervalChanged(LOGS, MINUTE);
            scheduled.get(1).run.run();
            assertEquals("a run overlapped the one in flight", 1, optimize.calls.get());
        } finally {
            optimize.release.countDown();
            first.join(10_000);
        }
        scheduled.get(1).run.run();
        assertEquals(2, optimize.calls.get());
    }

    public void testAStopDuringARunLetsItFinish() throws Exception {
        ShardId logs0 = new ShardId(LOGS, 0);
        SlowFirstRun optimize = new SlowFirstRun();
        optimizer.start(logs0, optimize, HOUR);
        Thread first = new Thread(scheduled.get(0).run);
        first.start();
        assertTrue(optimize.entered.await(10, TimeUnit.SECONDS));

        optimizer.stop(logs0);
        optimize.release.countDown();
        first.join(10_000);

        assertFalse(first.isAlive());
        assertNull(optimizer.interval(logs0));
        assertTrue(scheduled.get(0).cancelled);
    }
}
