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

public class InfinoOptimizerTests extends OpenSearchTestCase {

    /** A task the fake scheduler was given, and whether it was cancelled. */
    private static final class Scheduled implements Scheduler.Cancellable {
        final TimeValue interval;
        boolean cancelled;

        Scheduled(TimeValue interval) {
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
        Scheduled s = new Scheduled(interval);
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
        optimizer.start(logs0, () -> {}, HOUR);
        optimizer.start(logs1, () -> {}, HOUR);

        optimizer.intervalChanged(LOGS, MINUTE);

        assertEquals(MINUTE, optimizer.interval(logs0));
        assertEquals(MINUTE, optimizer.interval(logs1));
        assertEquals(4, scheduled.size());
        assertTrue(scheduled.get(0).cancelled);
        assertTrue(scheduled.get(1).cancelled);
        assertEquals(MINUTE, scheduled.get(2).interval);
        assertFalse(scheduled.get(2).cancelled);
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
}
