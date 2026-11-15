/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import java.util.function.LongSupplier;

/**
 * The time {@code now} means in a search, read through here so a caller can
 * tell whether translating a search read it: every place a translation
 * turns {@code now} into a time (date math, a script's clock) asks this
 * class for it. A shard keeps a search's result in the shard request cache
 * only when its translation read no clock, as a stock shard keeps one only
 * when parsing it never asked for the time (see
 * {@link InfinoQueryPhaseSearcher#beforeQueryPhase}).
 */
final class InfinoClock {

    /** Whether the translation watched on this thread has read the clock, or null when none is watched. */
    private static final ThreadLocal<boolean[]> WATCHED = new ThreadLocal<>();

    private InfinoClock() {}

    /** {@code nowMillis}, noted as read by the translation watched on this thread. */
    static long read(long nowMillis) {
        boolean[] watched = WATCHED.get();
        if (watched != null) {
            watched[0] = true;
        }
        return nowMillis;
    }

    /** {@code nowMillis} as date math asks for it, noted as read when it does. */
    static LongSupplier of(long nowMillis) {
        return () -> read(nowMillis);
    }

    /** Whether {@code translation} read the clock. */
    static boolean reads(Runnable translation) {
        boolean[] outer = WATCHED.get();
        boolean[] watched = new boolean[1];
        WATCHED.set(watched);
        try {
            translation.run();
        } finally {
            WATCHED.set(outer);
            if (outer != null && watched[0]) {
                outer[0] = true;
            }
        }
        return watched[0];
    }
}
