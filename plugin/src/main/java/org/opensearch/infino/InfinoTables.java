/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.MappingMetadata;
import org.opensearch.cluster.routing.OperationRouting;
import org.opensearch.common.settings.Settings;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The table behind each Infino-backed index, declared from the index
 * mapping and re-declared whenever the mapping moves (a field added), so
 * columns carry their mapped types. Fields the mapping does not name are
 * typed by the engine from the documents.
 *
 * <p>An index at the action layer has one table whatever its shard count:
 * nothing is routed to a shard. An index in engine mode is written by each
 * primary's engine, and one table has one writer, so an index of several
 * shards keeps a table per shard; one of a single shard keeps the action
 * layer's name for its one table.
 */
public final class InfinoTables {

    private static final Logger log = LogManager.getLogger(InfinoTables.class);
    /** What a shard's table adds to the index's table name, before the shard's number. */
    private static final String SHARD_SUFFIX = "_shard";

    /** The mapping version each table was last declared from, by table. */
    private final Map<String, Long> declared = new ConcurrentHashMap<>();

    /** The index's table, declared from its current mapping if it has not been. */
    public String ensure(IndexMetadata md) {
        return declare(InfinoNative.tableFor(md.getIndex(), md.getSettings()), md);
    }

    /** The table holding one shard's rows, declared from the index's current mapping if it has not been. */
    public String ensure(IndexMetadata md, int shard) {
        return declare(shardTable(InfinoNative.tableFor(md.getIndex(), md.getSettings()), shard, md.getSettings()), md);
    }

    /** The table holding the document {@code id} (routed by {@code routing} when given), as its shard is chosen for a write. */
    public String ensureForDocument(IndexMetadata md, String id, String routing) {
        if (!InfinoPlugin.atEngine(md.getSettings()) || md.getNumberOfShards() == 1) {
            return ensure(md);
        }
        return ensure(md, OperationRouting.generateShardId(md, id, routing));
    }

    /** The name of one shard's table, from the index's table name and settings. */
    static String shardTable(String indexTable, int shard, Settings indexSettings) {
        int shards = IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.get(indexSettings);
        return InfinoPlugin.atEngine(indexSettings) && shards > 1 ? indexTable + SHARD_SUFFIX + shard : indexTable;
    }

    private String declare(String table, IndexMetadata md) {
        long version = md.getMappingVersion();
        Long last = declared.get(table);
        if (last != null && last == version) {
            return table;
        }
        synchronized (this) {
            last = declared.get(table);
            if (last != null && last == version) {
                return table;
            }
            MappingMetadata mapping = md.mapping();
            String json = mapping == null ? "{}" : mapping.source().string();
            InfinoNative.ensureTable(table, json);
            declared.put(table, version);
            log.info("[infino] table {} declared from mapping version {} of {}", table, version, md.getIndex().getName());
        }
        return table;
    }

    /** The table is gone; a later index under the same name declares afresh. */
    public void forget(String table) {
        declared.remove(table);
    }
}
