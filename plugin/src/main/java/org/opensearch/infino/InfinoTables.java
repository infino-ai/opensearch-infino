/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.MappingMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.routing.OperationRouting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.index.Index;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.mapper.DataStreamFieldMapper;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The table behind each Infino-backed index, declared from the index
 * mapping and re-declared whenever the mapping moves (a field added), so
 * columns carry their mapped types. Fields the mapping does not name are
 * typed by the engine from the documents.
 *
 * <p>An index in proxy mode has one table whatever its shard count: nothing
 * is routed to a shard. An index in default mode is written by each
 * primary's engine, and one table has one writer, so an index of several
 * shards keeps a table per shard; one of a single shard keeps proxy mode's
 * name for its one table.
 *
 * <p>A table backs one index. Two live indices can name the same table: an
 * index's name maps to its table with every character a table name may not
 * hold turned into {@code _} ({@code logs.app} and {@code logs_app} both
 * read {@code os_logs_app}), and {@code index.infino.table} names any table
 * at all. Of the live indices naming a table, the oldest is served from it
 * and every other one is refused, so an index never reads or writes rows
 * that belong to another, whatever the security plugin lets its user do
 * with the index itself. No index is served from the registry's table.
 *
 * <p>An index created while a live index the table could back already named
 * it is refused for its whole life ({@link InfinoPlugin#INFINO_TABLE_CLAIMED}),
 * and counts for nothing when the table's index is chosen. So deleting the
 * index a table backs hands its rows to none of the indices refused beside
 * it: the table is dropped as if no other index named it (or kept, when it
 * was named explicitly), and an index created after the delete may take it.
 */
public final class InfinoTables {

    private static final Logger log = LogManager.getLogger(InfinoTables.class);
    /** What a shard's table adds to the index's table name, before the shard's number. */
    private static final String SHARD_SUFFIX = "_shard";
    /** Where an index's settings name its default similarity. */
    private static final String DEFAULT_SIMILARITY_PREFIX = "index.similarity.default.";
    /** A similarity's setting naming its type. */
    private static final String SIMILARITY_TYPE = "type";
    /** Mapping keys and field types the default-mode columns are declared with. */
    private static final String PROPERTIES = "properties";
    /** Where a table's mapping declares the columns its engine writes, which the bridge keeps apart from the fields. */
    private static final String ENGINE_COLUMNS = "_engine_columns";
    private static final String TYPE = "type";
    private static final String KEYWORD = "keyword";
    private static final String LONG = "long";
    /**
     * The type the bridge declares a column of the engine's row ids with, the
     * engine's {@code _id} type, which no field type of a mapping is (the
     * bridge's {@code ENGINE_ID_TYPE}).
     */
    private static final String ENGINE_ID = "infino_engine_id";
    /** An edge index's columns (see {@link InfinoPlugin#INFINO_EDGES}): each end of an edge as its row's table and {@code _id}. */
    private static final List<String> EDGE_TABLES = List.of("src_table", "dst_table");
    private static final List<String> EDGE_IDS = List.of("src_id", "dst_id");

    /** The mapping version each table was last declared from, by table. */
    private final Map<String, Long> declared = new ConcurrentHashMap<>();

    /** The connection {@link #declared} was filled over, as {@link InfinoNative#opens} counts them. */
    private volatile long declaredOver = -1;

    /** The live Infino-backed indices naming each table, as one cluster metadata holds them. */
    private record Naming(Metadata metadata, Map<String, List<IndexMetadata>> byTable) {}

    /**
     * The naming of the metadata the node last applied, taken from a cluster
     * state listener ({@link #observe}): a table is declared on the applier
     * thread too (a shard starting), where the cluster state may not be read.
     */
    private volatile Naming naming;

    /** The index's table, declared from its current mapping if it has not been. */
    public String ensure(IndexMetadata md) {
        return declare(InfinoNative.tableFor(md.getIndex(), md.getSettings()), md);
    }

    /** The table holding one shard's rows, declared from the index's current mapping if it has not been. */
    public String ensure(IndexMetadata md, int shard) {
        return declare(shardTable(InfinoNative.tableFor(md.getIndex(), md.getSettings()), shard, md.getSettings()), md);
    }

    /** Every table holding the index's rows, each declared as {@link #ensure(IndexMetadata, int)} declares it: one per shard in default mode, else the one. */
    public List<String> ensureAll(IndexMetadata md) {
        List<String> out = new ArrayList<>();
        for (String table : tablesOf(md)) {
            out.add(declare(table, md));
        }
        return out;
    }

    /** Every table holding an index's rows: one per shard in default mode, else the one. */
    static List<String> tablesOf(IndexMetadata md) {
        return tablesOf(md.getIndex().getName(), md.getSettings());
    }

    /** Every table that would hold the rows of an index of this name and these settings, as {@link #tablesOf(IndexMetadata)} names them. */
    static List<String> tablesOf(String name, Settings settings) {
        String table = InfinoNative.tableFor(name, settings);
        int shards = IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.get(settings);
        if (!InfinoPlugin.inDefaultMode(settings) || shards == 1) {
            return List.of(table);
        }
        List<String> out = new ArrayList<>(shards);
        for (int shard = 0; shard < shards; shard++) {
            out.add(shardTable(table, shard, settings));
        }
        return out;
    }

    /** Whether the index may be served from its tables at all: it was not created beside a live index they could back. */
    static boolean free(IndexMetadata md) {
        return InfinoPlugin.INFINO_TABLE_CLAIMED.get(md.getSettings());
    }

    /** The Infino-backed indices of those given that name each table, by table. */
    static Map<String, List<IndexMetadata>> naming(Iterable<IndexMetadata> indices) {
        Map<String, List<IndexMetadata>> out = new HashMap<>();
        for (IndexMetadata md : indices) {
            if (!md.getSettings().getAsBoolean(InfinoPlugin.INFINO_ENABLED.getKey(), false)) {
                continue;
            }
            for (String table : tablesOf(md)) {
                out.computeIfAbsent(table, t -> new ArrayList<>(1)).add(md);
            }
        }
        return out;
    }

    /**
     * The index a table backs, of the live indices naming it: the oldest (by
     * creation, then by UUID, so every node picks the same one) of those not
     * created refused (see {@link #free}); null when none is.
     */
    static Index owner(List<IndexMetadata> naming) {
        IndexMetadata oldest = null;
        for (IndexMetadata md : naming) {
            if (!free(md)) {
                continue;
            }
            if (oldest == null) {
                oldest = md;
                continue;
            }
            int c = Long.compare(md.getCreationDate(), oldest.getCreationDate());
            if (c < 0 || (c == 0 && md.getIndexUUID().compareTo(oldest.getIndexUUID()) < 0)) {
                oldest = md;
            }
        }
        return oldest == null ? null : oldest.getIndex();
    }

    /** Refuse an index of this name and these settings when one of its tables would be the registry's. */
    static void refuseRegistryTable(String name, Settings settings) {
        for (String table : tablesOf(name, settings)) {
            if (InfinoRegistry.TABLE.equals(table)) {
                throw registryTable(name, table);
            }
        }
    }

    private static IllegalArgumentException registryTable(String name, String table) {
        return new IllegalArgumentException(
            "[infino] index [" + name + "] would be served from table [" + table + "], which holds the registry of the cluster's indices"
        );
    }

    /**
     * Refuse to serve {@code md} from {@code table} when the table is not
     * its own: the registry's, one an older live index of {@code naming}
     * (see {@link #naming}) is served from, or one {@code md} was created
     * refused from (see {@link #free}).
     */
    static void claim(String table, IndexMetadata md, Map<String, List<IndexMetadata>> naming) {
        String name = md.getIndex().getName();
        if (InfinoRegistry.TABLE.equals(table)) {
            throw registryTable(name, table);
        }
        Index owner = owner(naming.getOrDefault(table, List.of()));
        if (owner != null && !owner.equals(md.getIndex())) {
            throw new IllegalArgumentException(
                "[infino] index [" + name + "] would be served from table [" + table + "], which backs index [" + owner.getName() + "]"
            );
        }
        if (!free(md)) {
            throw new IllegalArgumentException(
                "[infino] index [" + name + "] was created while table [" + table + "] backed another index, and is never served from it"
            );
        }
    }

    /**
     * Whether an index created now under {@code name} with {@code settings}
     * would be free to take its tables: no live index they could back names
     * any of them, as the metadata last applied holds them. The cluster
     * manager stamps the answer on the index as it creates it
     * ({@link InfinoPlugin#INFINO_TABLE_CLAIMED}).
     */
    boolean claimable(String name, Settings settings) {
        Naming known = naming;
        return claimable(tablesOf(name, settings), known == null ? Map.of() : known.byTable());
    }

    /** Whether no live index of {@code naming} that {@code tables} could back names any of them. */
    static boolean claimable(List<String> tables, Map<String, List<IndexMetadata>> naming) {
        for (String table : tables) {
            if (owner(naming.getOrDefault(table, List.of())) != null) {
                return false;
            }
        }
        return true;
    }

    /** Take the metadata the node applied; called from a cluster state listener, after the state is applied. */
    void observe(Metadata metadata) {
        namingOf(metadata);
    }

    /** Whether every table of {@code md} is its own, as {@code metadata} names them (see {@link #claim}). */
    boolean owns(IndexMetadata md, Metadata metadata) {
        if (!free(md)) {
            return false;
        }
        Map<String, List<IndexMetadata>> byTable = namingOf(metadata);
        for (String table : tablesOf(md)) {
            if (InfinoRegistry.TABLE.equals(table) || !md.getIndex().equals(owner(byTable.getOrDefault(table, List.of())))) {
                return false;
            }
        }
        return true;
    }

    /**
     * Whether deleting {@code gone} must leave {@code table}: it is the
     * registry's, or a live index other than {@code gone} that the table
     * could back names it, as the metadata last applied holds them (the
     * delete runs on the applier, before the state without {@code gone} is
     * the node's). An index created refused beside {@code gone} keeps
     * nothing: the table's rows were never its own.
     */
    boolean keptAfterDelete(String table, Index gone) {
        if (InfinoRegistry.TABLE.equals(table)) {
            return true;
        }
        Naming known = naming;
        if (known == null) {
            return false;
        }
        for (IndexMetadata md : known.byTable().getOrDefault(table, List.of())) {
            if (!md.getIndex().equals(gone) && free(md)) {
                return true;
            }
        }
        return false;
    }

    private Map<String, List<IndexMetadata>> namingOf(Metadata metadata) {
        Naming known = naming;
        if (known != null && known.metadata() == metadata) {
            return known.byTable();
        }
        Map<String, List<IndexMetadata>> byTable = naming(metadata.indices().values());
        naming = new Naming(metadata, byTable);
        return byTable;
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
        Naming known = naming;
        claim(table, md, known == null ? Map.of() : known.byTable());
        long version = md.getMappingVersion();
        forgetOver(InfinoNative.opens());
        Long last = declared.get(table);
        if (last != null && last == version) {
            return table;
        }
        synchronized (this) {
            // Declaring opens the connection if none is open, which counts as one more.
            InfinoNative.init();
            forgetOver(InfinoNative.opens());
            last = declared.get(table);
            if (last != null && last == version) {
                return table;
            }
            InfinoNative.ensureTable(table, tableMapping(md), defaultBm25(md.getSettings()), InfinoPlugin.INFINO_ATTACHED.get(md.getSettings()));
            declared.put(table, version);
            log.info("[infino] table {} declared from mapping version {} of {}", table, version, md.getIndex().getName());
        }
        return table;
    }

    /**
     * Whether the index's default similarity is the default BM25: it names
     * none, or names BM25 with no parameters of its own.
     */
    static boolean defaultBm25(Settings indexSettings) {
        Settings similarity = indexSettings.getByPrefix(DEFAULT_SIMILARITY_PREFIX);
        return similarity.isEmpty() || similarity.keySet().equals(Set.of(SIMILARITY_TYPE)) && InfinoNative.BM25.equals(similarity.get(SIMILARITY_TYPE));
    }

    /**
     * The mapping a table is declared from: the index's, and for an index in
     * default mode the columns its engine writes beside the documents' fields
     * (a row's routing value, the operation that wrote it, and its
     * generation), declared so that every batch is converted against
     * declared columns rather than typed from its documents. They are
     * declared apart from the fields ({@link #ENGINE_COLUMNS}): a field of
     * the same name keeps a column of its own, as a field named like the
     * sequence number column every table has does. An edge index's table
     * declares its edge columns as the engine's graph walks read them,
     * whatever the mapping says of them: the tables as strings, the ids as
     * the engine's id type.
     */
    @SuppressWarnings("unchecked")
    private static String tableMapping(IndexMetadata md) {
        MappingMetadata mapping = md.mapping();
        boolean stamped = InfinoPlugin.inDefaultMode(md.getSettings());
        boolean edges = InfinoPlugin.INFINO_EDGES.get(md.getSettings());
        if (!stamped && !edges) {
            return mapping == null ? "{}" : mapping.source().string();
        }
        Map<String, Object> properties = new LinkedHashMap<>();
        Object declared = mapping == null ? null : mapping.sourceAsMap().get(PROPERTIES);
        if (declared instanceof Map<?, ?> m) {
            properties.putAll((Map<String, Object>) m);
        }
        if (edges) {
            EDGE_TABLES.forEach(column -> properties.put(column, Map.of(TYPE, KEYWORD)));
            EDGE_IDS.forEach(column -> properties.put(column, Map.of(TYPE, ENGINE_ID)));
        }
        Map<String, Object> table = new LinkedHashMap<>();
        table.put(PROPERTIES, properties);
        if (stamped) {
            Map<String, Object> engine = new LinkedHashMap<>();
            engine.put(InfinoQueryTranslator.ROUTING, Map.of(TYPE, KEYWORD));
            engine.put(InfinoQueryTranslator.OP, Map.of(TYPE, LONG));
            engine.put(InfinoQueryTranslator.GEN, Map.of(TYPE, LONG));
            table.put(ENGINE_COLUMNS, engine);
        }
        // A data stream's timestamp field, which the bridge requires of each document.
        Object stream = mapping == null ? null : mapping.sourceAsMap().get(DataStreamFieldMapper.NAME);
        if (stream != null) {
            table.put(DataStreamFieldMapper.NAME, stream);
        }
        try (XContentBuilder b = XContentFactory.jsonBuilder()) {
            b.map(table);
            return BytesReference.bytes(b).utf8ToString();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Forget every declaration made over a connection other than the
     * {@code opens}-th: the connection holds what a table was declared
     * with, an attached table's view among it, and a new one (another
     * account, new credentials) holds none, so each table is declared again
     * on its next use.
     */
    void forgetOver(long opens) {
        if (declaredOver == opens) {
            return;
        }
        synchronized (this) {
            if (declaredOver != opens) {
                declared.clear();
                declaredOver = opens;
            }
        }
    }

    /** Note the table as declared from mapping {@code version}, as {@link #ensure} does once it has declared it; for tests. */
    void declaredForTest(String table, long version) {
        declared.put(table, version);
    }

    /** Whether the table is declared from mapping {@code version} over the current connection. */
    boolean declaredAt(String table, long version) {
        Long last = declared.get(table);
        return last != null && last == version;
    }

    /** The table is gone; a later index under the same name declares afresh. */
    public void forget(String table) {
        declared.remove(table);
    }
}
