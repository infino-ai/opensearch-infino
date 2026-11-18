/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.threadpool.Scheduler;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;

/**
 * The compaction task of each primary shard this node holds, run every
 * {@code index.infino.optimize_interval}. The interval is dynamic: a change
 * to it reschedules the index's started shards at once, rather than at their
 * next start.
 *
 * <p>Cancelling a task does not stop a run in progress, so a rescheduled task
 * can fire while the old one is still compacting. Each shard's runnable is
 * therefore wrapped once, at start, in a guard that a reschedule keeps: a run
 * that finds the previous one still going is skipped rather than waiting, as
 * the run in flight is already compacting the table and waiting would hold a
 * pool thread for as long as that takes.
 */
final class InfinoOptimizer {

    private record Task(Runnable run, TimeValue interval, Scheduler.Cancellable handle) {}

    private final BiFunction<Runnable, TimeValue, Scheduler.Cancellable> schedule;
    private final Map<ShardId, Task> tasks = new HashMap<>();

    /** {@code schedule} runs a task every interval and returns its handle. */
    InfinoOptimizer(BiFunction<Runnable, TimeValue, Scheduler.Cancellable> schedule) {
        this.schedule = schedule;
    }

    /** Run {@code optimize} for {@code shard} every {@code interval}, replacing any task it had. */
    synchronized void start(ShardId shard, Runnable optimize, TimeValue interval) {
        Runnable run = oneAtATime(optimize);
        Task previous = tasks.put(shard, new Task(run, interval, schedule.apply(run, interval)));
        if (previous != null) {
            previous.handle().cancel();
        }
    }

    /** Stop the task of {@code shard}, if it has one. */
    synchronized void stop(ShardId shard) {
        Task task = tasks.remove(shard);
        if (task != null) {
            task.handle().cancel();
        }
    }

    /** Reschedule every started shard of {@code index} at {@code interval}. */
    synchronized void intervalChanged(Index index, TimeValue interval) {
        for (Map.Entry<ShardId, Task> entry : tasks.entrySet()) {
            Task task = entry.getValue();
            if (!entry.getKey().getIndex().equals(index) || task.interval().equals(interval)) {
                continue;
            }
            task.handle().cancel();
            entry.setValue(new Task(task.run(), interval, schedule.apply(task.run(), interval)));
        }
    }

    /** {@code optimize}, skipped when called while a previous call is still running. */
    private static Runnable oneAtATime(Runnable optimize) {
        AtomicBoolean running = new AtomicBoolean();
        return () -> {
            if (!running.compareAndSet(false, true)) {
                return;
            }
            try {
                optimize.run();
            } finally {
                running.set(false);
            }
        };
    }

    /** The interval {@code shard} is compacted at, or null when it has no task. */
    synchronized TimeValue interval(ShardId shard) {
        Task task = tasks.get(shard);
        return task == null ? null : task.interval();
    }
}
