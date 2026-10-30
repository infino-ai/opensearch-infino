/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import org.opensearch.index.engine.EngineConfig;
import org.opensearch.index.translog.Translog;

import java.io.IOException;
import java.util.stream.Stream;

/**
 * {@link InfinoEngine} with the translog sync hooked, as 2.x syncs: a shard
 * syncs its translog for a write through the engine itself, so the engine
 * commits the pending batch before it syncs. 3.x syncs through the engine's
 * translog manager and has a copy of this class of its own.
 */
final class InfinoSyncedEngine extends InfinoEngine {

    InfinoSyncedEngine(EngineConfig config) {
        super(config);
    }

    @Override
    public boolean ensureTranslogSynced(Stream<Translog.Location> locations) throws IOException {
        commitBeforeSync();
        return super.ensureTranslogSynced(locations);
    }
}
