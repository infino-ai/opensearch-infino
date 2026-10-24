/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.MappingMetadata;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The table behind each Infino-backed index, declared from the index
 * mapping and re-declared whenever the mapping moves (a field added), so
 * columns carry their mapped types. Fields the mapping does not name are
 * typed by the engine from the documents.
 */
public final class InfinoTables {

    private static final Logger log = LogManager.getLogger(InfinoTables.class);

    /** The mapping version each table was last declared from, by table. */
    private final Map<String, Long> declared = new ConcurrentHashMap<>();

    /** The index's table, declared from its current mapping if it has not been. */
    public String ensure(IndexMetadata md) {
        String table = InfinoNative.tableFor(md.getIndex(), md.getSettings());
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
