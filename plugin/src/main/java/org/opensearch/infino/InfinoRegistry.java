/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import com.sun.jna.Pointer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.admin.indices.alias.Alias;
import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.cluster.ClusterChangedEvent;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.AliasMetadata;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.MappingMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.gateway.GatewayService;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * The indices, kept beside their tables so a fresh cluster gets them back.
 *
 * <p>A node is stateless: nothing it needs survives in the cluster. The
 * tables are on the hosted service or the customer's bucket, and so is this
 * registry, one row per Infino-backed index with its settings, mapping and
 * aliases as the cluster last held them. When a cluster comes up with no
 * index of its own, the elected cluster manager reads the registry and
 * creates every index it names, with the same shape; its table, named by the
 * index, is found by name, so the rows are there before the first request.
 * Cluster state is a cache of this, rebuilt at boot, never the source.
 *
 * <p>An engine-mode index comes back marked {@link InfinoPlugin#INFINO_REBUILT}:
 * its Lucene skeleton lived on the old node, and the new one starts empty
 * over a table that already holds rows.
 *
 * <p>The manager records an index whenever its metadata moves (created, a
 * field mapped, an alias added); the node that deletes an index deletes its
 * row. The security plugin's configuration index and Dashboards' saved
 * objects come back the same way, so a fresh node needs neither
 * {@code securityadmin} nor a saved-objects migration.
 */
final class InfinoRegistry {

    private static final Logger log = LogManager.getLogger(InfinoRegistry.class);

    /** The registry table, in the same database as the indices' tables. */
    static final String TABLE = "os_index";

    /** Index settings the cluster assigns at creation and refuses on a create request. */
    private static final Set<String> CLUSTER_OWNED_SETTINGS = Set.of(
        IndexMetadata.SETTING_INDEX_UUID,
        IndexMetadata.SETTING_CREATION_DATE,
        IndexMetadata.SETTING_VERSION_CREATED,
        IndexMetadata.SETTING_VERSION_UPGRADED,
        IndexMetadata.SETTING_INDEX_PROVIDED_NAME,
        IndexMetadata.SETTING_HISTORY_UUID
    );

    /** The metadata version of each index as last recorded, so a state that moved nothing writes nothing. */
    private static final Map<String, Long> recorded = new ConcurrentHashMap<>();
    /** Indices whose metadata moved and are not yet recorded, by name; the latest metadata wins. */
    private static final Map<String, IndexMetadata> pending = new ConcurrentHashMap<>();
    /** Whether the registry table has been declared on the connection. */
    private static final AtomicBoolean declared = new AtomicBoolean();
    /** Whether this node has rebuilt the indices from the registry since it started. */
    private static final AtomicBoolean rebuilt = new AtomicBoolean();

    private InfinoRegistry() {}

    /**
     * Every state the elected manager applies: once the state is recovered,
     * rebuild what the registry names and the cluster lacks (once per node
     * life), then record every Infino-backed index whose metadata moved.
     * The engine is written on the generic pool, never on the applier.
     */
    static void onClusterChanged(ClusterChangedEvent event) {
        ClusterState state = event.state();
        if (!state.nodes().isLocalNodeElectedClusterManager() || state.blocks().hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK)) {
            return;
        }
        ThreadPool threadPool = InfinoServices.threadPool();
        if (threadPool == null) {
            return;
        }
        boolean first = rebuilt.compareAndSet(false, true);
        if (first) {
            threadPool.generic().execute(() -> rebuild(state));
        } else if (!event.metadataChanged()) {
            return;
        }
        boolean any = false;
        for (IndexMetadata md : state.metadata().indices().values()) {
            if (!md.getSettings().getAsBoolean(InfinoPlugin.INFINO_ENABLED.getKey(), false)) {
                continue;
            }
            Long last = recorded.get(md.getIndex().getName());
            if (last == null || last != md.getVersion()) {
                pending.put(md.getIndex().getName(), md);
                any = true;
            }
        }
        if (any) {
            threadPool.generic().execute(InfinoRegistry::drain);
        }
    }

    /**
     * Write every index whose metadata moved since it was last recorded.
     * Called from the generic pool after a state applies, and by every read
     * of the registry first, so a read sees the state the manager has seen.
     */
    private static synchronized void drain() {
        for (String name : new ArrayList<>(pending.keySet())) {
            IndexMetadata md = pending.remove(name);
            if (md != null) {
                record(md);
            }
        }
    }

    /** Write the index's row: its settings, mapping and aliases as the cluster holds them, replacing the row it had. */
    static void record(IndexMetadata md) {
        String name = md.getIndex().getName();
        try {
            declare();
            // A row the registry holds under this name is replaced in the same
            // mutation as the append; a first row is appended. The engine's
            // update replaces only what it matches, so the row is looked up
            // first, as the writer looks ids up.
            List<String> replaced = InfinoNative.existingOsids(TABLE, List.of(name)).isEmpty() ? List.of() : List.of(name);
            Pointer prepared = InfinoNative.prepare(TABLE, List.of(name), List.of(rowOf(md)));
            if (prepared == null) {
                throw new IllegalStateException("the registry row could not be prepared");
            }
            InfinoNative.PreparedResult result = InfinoNative.appendPrepared(TABLE, List.of(prepared), replaced);
            if (!result.failed().isEmpty()) {
                throw new IllegalStateException(result.failed().get(0).reason());
            }
            recorded.put(name, md.getVersion());
        } catch (Exception e) {
            log.warn("[infino] index {} could not be recorded in the registry", name, e);
        }
    }

    /** The index is gone: so is its row. */
    static void forget(String name) {
        pending.remove(name);
        recorded.remove(name);
        try {
            declare();
            InfinoNative.deleteOsids(TABLE, new String[] { name }, 1);
        } catch (Exception e) {
            log.warn("[infino] index {} could not be removed from the registry", name, e);
        }
    }

    /** The registry's rows: one object per recorded index, by name, after what moved is written. */
    static List<Map<String, Object>> rows() {
        drain();
        declare();
        Map<String, Map<String, Object>> byName = new LinkedHashMap<>();
        for (Map<String, Object> row : InfinoNative.sqlRows(
            "SELECT " + InfinoQueryTranslator.OSID + ", " + InfinoQueryTranslator.SOURCE + " FROM " + InfinoQueryTranslator.quoteIdent(TABLE)
        )) {
            Object source = row.get(InfinoQueryTranslator.SOURCE);
            Object name = row.get(InfinoQueryTranslator.OSID);
            if (source != null && name != null) {
                byName.put(name.toString(), InfinoNative.object(source.toString()));
            }
        }
        return new ArrayList<>(byName.values());
    }

    /** Create every index the registry names that the cluster lacks, one after another. */
    static void rebuild(ClusterState state) {
        List<Map<String, Object>> rows;
        try {
            rows = rows();
        } catch (Exception e) {
            log.warn("[infino] the registry could not be read; the indices are not rebuilt", e);
            return;
        }
        List<CreateIndexRequest> missing = new ArrayList<>();
        for (Map<String, Object> row : rows) {
            Object name = row.get("name");
            if (name instanceof String n && !state.metadata().hasIndex(n) && !state.metadata().hasAlias(n)) {
                try {
                    missing.add(requestOf(n, row));
                } catch (Exception e) {
                    log.warn("[infino] index {} could not be rebuilt from its registry row", n, e);
                }
            }
        }
        if (missing.isEmpty()) {
            log.info("[infino] the registry names {} indices, all present", rows.size());
            return;
        }
        log.info("[infino] rebuilding {} of the {} indices the registry names", missing.size(), rows.size());
        createNext(missing, 0);
    }

    private static void createNext(List<CreateIndexRequest> requests, int at) {
        if (at == requests.size()) {
            log.info("[infino] rebuilt {} indices from the registry", requests.size());
            return;
        }
        CreateIndexRequest request = requests.get(at);
        InfinoClient.createIndex(request, ActionListener.wrap(created -> createNext(requests, at + 1), e -> {
            log.warn("[infino] index {} could not be rebuilt", request.index(), e);
            createNext(requests, at + 1);
        }));
    }

    private static void declare() {
        if (declared.compareAndSet(false, true)) {
            InfinoNative.ensureTable(TABLE, "{}");
        }
    }

    /** One row: the index as a create request would describe it, plus its table. */
    static String rowOf(IndexMetadata md) {
        try (XContentBuilder b = XContentFactory.jsonBuilder()) {
            b.startObject();
            b.field("name", md.getIndex().getName());
            b.field("table", InfinoNative.tableFor(md.getIndex(), md.getSettings()));
            Settings.Builder settings = Settings.builder().put(md.getSettings());
            for (String owned : CLUSTER_OWNED_SETTINGS) {
                settings.remove(owned);
            }
            b.startObject("settings");
            settings.build().toXContent(b, ToXContent.EMPTY_PARAMS);
            b.endObject();
            MappingMetadata mapping = md.mapping();
            b.field("mappings", mapping == null ? "{}" : mapping.source().string());
            b.startObject("aliases");
            for (Map.Entry<String, AliasMetadata> alias : md.getAliases().entrySet()) {
                AliasMetadata meta = alias.getValue();
                b.startObject(alias.getKey());
                if (meta.filter() != null) {
                    b.field("filter", meta.filter().string());
                }
                if (meta.indexRouting() != null) {
                    b.field("index_routing", meta.indexRouting());
                }
                if (meta.searchRouting() != null) {
                    b.field("search_routing", meta.searchRouting());
                }
                if (meta.writeIndex() != null) {
                    b.field("is_write_index", meta.writeIndex());
                }
                if (meta.isHidden() != null) {
                    b.field("is_hidden", meta.isHidden());
                }
                b.endObject();
            }
            b.endObject();
            b.endObject();
            return BytesReference.bytes(b).utf8ToString();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** The create request a row describes. */
    @SuppressWarnings("unchecked")
    static CreateIndexRequest requestOf(String name, Map<String, Object> row) {
        CreateIndexRequest request = new CreateIndexRequest(name);
        Object settings = row.get("settings");
        if (settings instanceof Map<?, ?> s) {
            request.settings((Map<String, ?>) s);
        }
        if (InfinoPlugin.atEngine(request.settings())) {
            // Its new Lucene skeleton starts empty over a table that holds rows.
            request.settings(Settings.builder().put(request.settings()).put(InfinoPlugin.INFINO_REBUILT.getKey(), true));
        }
        Object mappings = row.get("mappings");
        if (mappings instanceof String m && !m.isEmpty() && !"{}".equals(m)) {
            request.mapping(m);
        }
        Object aliases = row.get("aliases");
        if (aliases instanceof Map<?, ?> all) {
            for (Map.Entry<?, ?> entry : all.entrySet()) {
                Alias alias = new Alias(entry.getKey().toString());
                if (entry.getValue() instanceof Map<?, ?> meta) {
                    Object filter = meta.get("filter");
                    if (filter instanceof String f) {
                        alias.filter(f);
                    }
                    Object indexRouting = meta.get("index_routing");
                    if (indexRouting instanceof String r) {
                        alias.indexRouting(r);
                    }
                    Object searchRouting = meta.get("search_routing");
                    if (searchRouting instanceof String r) {
                        alias.searchRouting(r);
                    }
                    Object write = meta.get("is_write_index");
                    if (write instanceof Boolean w) {
                        alias.writeIndex(w);
                    }
                    Object hidden = meta.get("is_hidden");
                    if (hidden instanceof Boolean h) {
                        alias.isHidden(h);
                    }
                }
                request.alias(alias);
            }
        }
        return request;
    }
}
