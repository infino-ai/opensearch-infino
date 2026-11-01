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
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The table behind each Infino-backed index, declared from the index
 * mapping and re-declared whenever the mapping moves (a field added), so
 * columns carry their mapped types. Fields the mapping does not name are
 * typed by the engine from the documents.
 *
 * <p>An index in cloud mode has one table whatever its shard count: nothing
 * is routed to a shard. An index in default mode is written by each
 * primary's engine, and one table has one writer, so an index of several
 * shards keeps a table per shard; one of a single shard keeps cloud mode's
 * name for its one table.
 */
public final class InfinoTables {

    private static final Logger log = LogManager.getLogger(InfinoTables.class);
    /** What a shard's table adds to the index's table name, before the shard's number. */
    private static final String SHARD_SUFFIX = "_shard";
    /** Mapping keys and field types the default-mode columns are declared with. */
    private static final String PROPERTIES = "properties";
    private static final String TYPE = "type";
    private static final String KEYWORD = "keyword";
    private static final String LONG = "long";

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

    /** Every table holding the index's rows, each declared as {@link #ensure(IndexMetadata, int)} declares it: one per shard in engine mode, else the one. */
    public List<String> ensureAll(IndexMetadata md) {
        if (!InfinoPlugin.atEngine(md.getSettings()) || md.getNumberOfShards() == 1) {
            return List.of(ensure(md));
        }
        List<String> out = new ArrayList<>(md.getNumberOfShards());
        for (int shard = 0; shard < md.getNumberOfShards(); shard++) {
            out.add(ensure(md, shard));
        }
        return out;
    }

    /** The table holding the document {@code id} (routed by {@code routing} when given), as its shard is chosen for a write. */
    public String ensureForDocument(IndexMetadata md, String id, String routing) {
        if (!InfinoPlugin.inDefaultMode(md.getSettings()) || md.getNumberOfShards() == 1) {
            return ensure(md);
        }
        return ensure(md, OperationRouting.generateShardId(md, id, routing));
    }

    /** The name of one shard's table, from the index's table name and settings. */
    static String shardTable(String indexTable, int shard, Settings indexSettings) {
        int shards = IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.get(indexSettings);
        return InfinoPlugin.inDefaultMode(indexSettings) && shards > 1 ? indexTable + SHARD_SUFFIX + shard : indexTable;
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
            InfinoNative.ensureTable(table, tableMapping(md));
            declared.put(table, version);
            log.info("[infino] table {} declared from mapping version {} of {}", table, version, md.getIndex().getName());
        }
        return table;
    }

    /**
     * The mapping a table is declared from: the index's, and for an index in
     * default mode the columns its engine writes beside the documents' fields
     * (a row's routing value, the sequence numbers it is stamped and was
     * written with, and their generation), declared so that every batch is
     * converted against declared columns rather than typed from its
     * documents.
     */
    @SuppressWarnings("unchecked")
    private static String tableMapping(IndexMetadata md) {
        MappingMetadata mapping = md.mapping();
        if (!InfinoPlugin.inDefaultMode(md.getSettings())) {
            return mapping == null ? "{}" : mapping.source().string();
        }
        Map<String, Object> properties = new LinkedHashMap<>();
        Object declared = mapping == null ? null : mapping.sourceAsMap().get(PROPERTIES);
        if (declared instanceof Map<?, ?> m) {
            properties.putAll((Map<String, Object>) m);
        }
        properties.put(InfinoQueryTranslator.ROUTING, Map.of(TYPE, KEYWORD));
        properties.put(InfinoQueryTranslator.SEQ, Map.of(TYPE, LONG));
        properties.put(InfinoQueryTranslator.OP, Map.of(TYPE, LONG));
        properties.put(InfinoQueryTranslator.GEN, Map.of(TYPE, LONG));
        try (XContentBuilder b = XContentFactory.jsonBuilder()) {
            b.map(Map.of(PROPERTIES, properties));
            return BytesReference.bytes(b).utf8ToString();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** The table is gone; a later index under the same name declares afresh. */
    public void forget(String table) {
        declared.remove(table);
    }
}
