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
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.AbstractScopedSettings;
import org.opensearch.common.settings.IndexScopedSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
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
import java.util.Collections;
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
 * <p>A default-mode index comes back marked {@link InfinoPlugin#INFINO_REBUILT}:
 * its Lucene skeleton lived on the old node, and the new one starts empty
 * over a table that already holds rows.
 *
 * <p>The manager records an index whenever its metadata moves (created, a
 * field mapped, an alias added); the node that deletes an index deletes its
 * row. The security plugin's configuration index and Dashboards' saved
 * objects come back the same way, so a fresh node needs neither
 * {@code securityadmin} nor a saved-objects migration.
 *
 * <p>A row can also come from outside the cluster: a loader that writes a
 * table in this shape writes its index's row beside it (a load of a Lucene
 * index's snapshot into such a table does), and
 * {@code POST /_plugins/_infino/account/indices} creates it on a running
 * cluster, as a fresh one would at boot ({@link #restore}). Such a loader
 * may register the cluster state its source held besides the indices too,
 * one row per item, which that call applies first ({@link InfinoClusterState}).
 */
final class InfinoRegistry {

    private static final Logger log = LogManager.getLogger(InfinoRegistry.class);

    /** The registry table, in the same database as the indices' tables. */
    static final String TABLE = "os_index";

    /**
     * Index settings the cluster assigns at creation and refuses on a create
     * request. Whether an index was free to take its table is among them: an
     * index brought back is stamped afresh against the indices then live.
     */
    private static final Set<String> CLUSTER_OWNED_SETTINGS = Set.of(
        IndexMetadata.SETTING_INDEX_UUID,
        IndexMetadata.SETTING_CREATION_DATE,
        IndexMetadata.SETTING_VERSION_CREATED,
        IndexMetadata.SETTING_VERSION_UPGRADED,
        IndexMetadata.SETTING_INDEX_PROVIDED_NAME,
        IndexMetadata.SETTING_HISTORY_UUID,
        InfinoPlugin.INFINO_TABLE_CLAIMED.getKey()
    );

    /** The key of a row that records the metadata version of its index as the cluster held it. */
    private static final String VERSION = "version";
    /** The version of a row that records none (a loader's may not). */
    private static final long UNVERSIONED = -1L;

    /** The metadata version of each index as last recorded, so a state that moved nothing writes nothing. */
    private static final Map<String, Long> recorded = new ConcurrentHashMap<>();
    /** Indices whose metadata moved and are not yet recorded, by name; the latest metadata wins. */
    private static final Map<String, IndexMetadata> pending = new ConcurrentHashMap<>();
    /** How many writes of each index's row have failed in a row, for how long to wait before the next. */
    private static final Map<String, Integer> failures = new ConcurrentHashMap<>();
    /** When each row whose write failed may be tried again ({@link System#nanoTime}); a drain before then leaves it pending. */
    private static final Map<String, Long> retryAt = new ConcurrentHashMap<>();
    /** The rows with a retry scheduled: one each, however often a row fails meanwhile. */
    private static final Set<String> scheduled = ConcurrentHashMap.newKeySet();
    /** The wait before a failed row is written again; each further failure doubles it. */
    static final TimeValue FIRST_RETRY_DELAY = TimeValue.timeValueSeconds(1);
    /** The longest wait between tries of a row that keeps failing. */
    static final TimeValue MAX_RETRY_DELAY = TimeValue.timeValueMinutes(1);
    /**
     * Failures in a row before one is logged as a warning: on the hosted
     * service the first tries after the registry table is created can miss
     * it for a moment, and the next try finds it.
     */
    private static final int WARN_AFTER_FAILURES = 3;
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
            if (!InfinoPlugin.tables().owns(md, state.metadata())) {
                // Named onto a table another index backs (or the registry's own): it is
                // refused, and a fresh cluster must not bring it back over that table.
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
     * Write every index whose metadata moved since it was last recorded, as
     * the cluster holds it now; a row waiting out a failed write's delay
     * stays pending until then. Called from the generic pool after a state
     * applies, and by every read of the registry first, so a read sees the
     * state the manager has seen.
     */
    private static synchronized void drain() {
        long now = System.nanoTime();
        for (String name : new ArrayList<>(pending.keySet())) {
            if (!electedManager()) {
                // The elected manager records every index from its own state; what
                // this node queued while it was the manager may be older than that.
                // Asked before each row, as the node can lose the election mid-drain.
                pending.clear();
                failures.clear();
                retryAt.clear();
                return;
            }
            Long at = retryAt.get(name);
            if (at != null && at - now > 0) {
                continue;
            }
            IndexMetadata md = pending.remove(name);
            if (md == null) {
                continue;
            }
            IndexMetadata current = current(md);
            if (current == null) {
                failures.remove(name);
                retryAt.remove(name);
                log.debug("[infino] index {} is gone; its registry row is not written", name);
                continue;
            }
            record(current);
        }
    }

    /**
     * Write the index's row: its settings, mapping and aliases as the cluster
     * holds them, replacing the row it had. A write that fails is tried again
     * later ({@link #retryDelay}) for as long as the index lives: a node that
     * starts from the registry would otherwise come up without the index.
     */
    static void record(IndexMetadata md) {
        String name = md.getIndex().getName();
        try {
            write(name, rowOf(md));
            recorded.put(name, md.getVersion());
            failures.remove(name);
            retryAt.remove(name);
        } catch (Exception e) {
            int failed = failures.merge(name, 1, Integer::sum);
            TimeValue delay = retryDelay(failed);
            retryAt.put(name, System.nanoTime() + delay.nanos());
            // Metadata queued since this write began is newer, and wins.
            pending.putIfAbsent(name, md);
            if (failed < WARN_AFTER_FAILURES) {
                log.debug("[infino] index {} could not be recorded in the registry; trying again in {}", name, delay, e);
            } else {
                log.warn("[infino] index {} could not be recorded in the registry ({} tries); trying again in {}", name, failed, delay, e);
            }
            retryIn(name, delay);
        }
    }

    /**
     * How long to wait before writing a row again after {@code failed}
     * failures in a row: {@link #FIRST_RETRY_DELAY}, doubled for each failure
     * after the first, at most {@link #MAX_RETRY_DELAY}.
     */
    static TimeValue retryDelay(int failed) {
        long first = FIRST_RETRY_DELAY.millis();
        long max = MAX_RETRY_DELAY.millis();
        int doublings = Math.min(Math.max(failed - 1, 0), 30);
        return TimeValue.timeValueMillis(Math.min(max, first << doublings));
    }

    /**
     * Try {@code name}'s row again after {@code delay}, on the generic pool,
     * unless a retry of it is already scheduled.
     */
    private static void retryIn(String name, TimeValue delay) {
        ThreadPool threadPool = InfinoServices.threadPool();
        if (threadPool == null) {
            return; // no node: the next state applied, or the next read of the registry, writes it
        }
        if (scheduled.add(name)) {
            threadPool.schedule(() -> retry(name), delay, ThreadPool.Names.GENERIC);
        }
    }

    /** A scheduled retry of {@code name}'s row: write what is due, and schedule the row again if it is still waiting. */
    private static void retry(String name) {
        scheduled.remove(name);
        drain();
        Long at = retryAt.get(name);
        if (at != null && pending.containsKey(name)) {
            retryIn(name, TimeValue.timeValueNanos(Math.max(0, at - System.nanoTime())));
        }
    }

    /** Whether this node is the elected cluster manager, the one node that writes the registry's rows; true with no node. */
    private static boolean electedManager() {
        ClusterService clusterService = InfinoServices.clusterService();
        return clusterService == null || clusterService.state().nodes().isLocalNodeElectedClusterManager();
    }

    /**
     * {@code md}'s index as the cluster holds it now, or null once it is gone
     * (or another index has since been created under its name).
     */
    private static IndexMetadata current(IndexMetadata md) {
        ClusterService clusterService = InfinoServices.clusterService();
        if (clusterService == null) {
            return md;
        }
        IndexMetadata now = clusterService.state().metadata().index(md.getIndex().getName());
        return now != null && now.getIndexUUID().equals(md.getIndexUUID()) ? now : null;
    }

    /**
     * Write the registry row {@code id} with {@code source}. A row the
     * registry holds under the id is replaced in the same mutation as the
     * append; a first row is appended. The engine's update replaces only what
     * it matches, one row for one, so the rows are counted first. Two writers
     * registering at once (a loader's parts) can leave two rows, which a
     * replace of one refuses: those rows go, and one is appended.
     */
    static void write(String id, String source) {
        declare();
        long held = heldRows(id);
        if (held > 1) {
            InfinoNative.deleteOsids(TABLE, new String[] { id }, 1);
        }
        List<String> replaced = held == 1 ? List.of(id) : List.of();
        Pointer prepared = InfinoNative.prepare(TABLE, List.of(id), List.of(source));
        if (prepared == null) {
            throw new IllegalStateException("the registry row could not be prepared");
        }
        // The registry's rows are the node's own bookkeeping: no client conditions a write on them.
        InfinoNative.PreparedResult result = InfinoNative.appendPrepared(TABLE, List.of(prepared), replaced, null);
        if (!result.failed().isEmpty()) {
            throw new IllegalStateException(result.failed().get(0).reason());
        }
    }

    /** How many rows the registry holds under {@code name}. */
    private static long heldRows(String name) {
        List<Map<String, Object>> counted = InfinoNative.sqlRows(
            "SELECT count(*) AS held FROM "
                + InfinoQueryTranslator.quoteIdent(TABLE)
                + " WHERE "
                + InfinoQueryTranslator.OSID
                + " = "
                + InfinoQueryTranslator.literal(name)
        );
        return counted.isEmpty() || !(counted.get(0).get("held") instanceof Number n) ? 0 : n.longValue();
    }

    /** The index is gone: so is its row. */
    static void forget(String name) {
        pending.remove(name);
        recorded.remove(name);
        failures.remove(name);
        retryAt.remove(name);
        try {
            declare();
            InfinoNative.deleteOsids(TABLE, new String[] { name }, 1);
        } catch (Exception e) {
            log.warn("[infino] index {} could not be removed from the registry", name, e);
        }
    }

    /** The registry's rows: one object per recorded index, by name, after what moved is written. */
    static List<Map<String, Object>> rows() {
        List<Map<String, Object>> rows = new ArrayList<>();
        for (Map.Entry<String, String> row : sources().entrySet()) {
            // The cluster state a loader registered beside the indices is no index.
            if (!InfinoClusterState.isClusterRow(row.getKey())) {
                rows.add(InfinoNative.object(row.getValue()));
            }
        }
        return rows;
    }

    /**
     * The source of every row of the registry by its id, one per id, after
     * what moved is written. Two writers registering one id at once (a
     * loader's parts) can leave two rows under it until the next write of the
     * row makes them one; of two, every read takes the same one
     * ({@link #later}), whatever order the table returns them in.
     */
    static Map<String, String> sources() {
        Map<String, String> byId = new LinkedHashMap<>();
        for (Map<String, Object> row : rawRows()) {
            Object source = row.get(InfinoQueryTranslator.SOURCE);
            Object id = row.get(InfinoQueryTranslator.OSID);
            if (source != null && id != null) {
                byId.merge(id.toString(), source.toString(), InfinoRegistry::later);
            }
        }
        return byId;
    }

    /**
     * Of two rows under one id, the one recording the later metadata version
     * of its index, and of two recording the same (or none), the greater
     * source: a choice every read makes alike.
     */
    static String later(String a, String b) {
        long va = version(a);
        long vb = version(b);
        if (va != vb) {
            return va > vb ? a : b;
        }
        return a.compareTo(b) >= 0 ? a : b;
    }

    /** The metadata version a row records, or {@link #UNVERSIONED}. */
    private static long version(String source) {
        return InfinoNative.object(source).get(VERSION) instanceof Number n ? n.longValue() : UNVERSIONED;
    }

    /** Every row of the registry table, each its id and source, after what moved is written. */
    static List<Map<String, Object>> rawRows() {
        drain();
        declare();
        return InfinoNative.sqlRows(
            "SELECT " + InfinoQueryTranslator.OSID + ", " + InfinoQueryTranslator.SOURCE + " FROM " + InfinoQueryTranslator.quoteIdent(TABLE)
        );
    }

    /** What became of a registered index when the registry was restored. */
    enum Outcome {
        /** The cluster lacked it, and it was created from its row. */
        CREATED,
        /** The cluster already has an index or an alias of that name. */
        PRESENT,
        /** Its row could not be read as a create request, or the create failed. */
        FAILED
    }

    /** One registered index, what became of it, and why when it failed. */
    record Restored(String name, Outcome outcome, String reason) {}

    /** Create every index the registry names that the cluster lacks, one after another: once per node life, at boot. */
    static void rebuild(ClusterState state) {
        restore(state, null, ActionListener.wrap(restored -> {
            long created = restored.stream().filter(r -> r.outcome() == Outcome.CREATED).count();
            if (created == 0 && restored.stream().noneMatch(r -> r.outcome() == Outcome.FAILED)) {
                log.info("[infino] the registry names {} indices, all present", restored.size());
            } else {
                log.info("[infino] rebuilt {} of the {} indices the registry names", created, restored.size());
            }
        }, e -> log.warn("[infino] the registry could not be read; the indices are not rebuilt", e)));
    }

    /**
     * Create every index the registry names that {@code state} lacks, now,
     * as {@link #rebuild} does at boot; with {@code only}, that one index
     * alone. What brings up an index whose table and row a loader wrote
     * beside the others while the cluster runs (a snapshot load into the
     * plugin's table), with no restart. The listener gets every registered
     * index considered, with what became of it; a row that fails is
     * reported and the rest still created. Reads the engine: never call it
     * on a transport or applier thread.
     */
    static void restore(ClusterState state, String only, ActionListener<List<Restored>> listener) {
        List<Map<String, Object>> rows;
        try {
            rows = rows();
        } catch (Exception e) {
            listener.onFailure(e);
            return;
        }
        List<Restored> restored = Collections.synchronizedList(new ArrayList<>());
        List<Pending> missing = new ArrayList<>();
        for (Map<String, Object> row : rows) {
            if (!(row.get("name") instanceof String name) || (only != null && !only.equals(name))) {
                continue;
            }
            if (state.metadata().hasIndex(name) || state.metadata().hasAlias(name)) {
                restored.add(new Restored(name, Outcome.PRESENT, null));
                continue;
            }
            try {
                List<String> leftOut = new ArrayList<>();
                missing.add(new Pending(requestOf(name, row, InfinoServices.indexScopedSettings(), leftOut), leftOut));
            } catch (Exception e) {
                log.warn("[infino] index {} could not be rebuilt from its registry row", name, e);
                restored.add(new Restored(name, Outcome.FAILED, "its registry row is not a create request: " + e.getMessage()));
            }
        }
        createNext(missing, 0, restored, () -> listener.onResponse(List.copyOf(restored)));
    }

    /** An index to create, and the settings of its row the cluster does not know, left out of its request. */
    private record Pending(CreateIndexRequest request, List<String> leftOut) {}

    /**
     * {@code given} without the settings {@code known} does not know or
     * finds invalid, each named onto {@code leftOut}, as the cluster sets
     * them aside when it restores an index; {@code given} as it is with no
     * {@code known} to hold it to.
     */
    static Settings knownSettings(Settings given, IndexScopedSettings known, List<String> leftOut) {
        if (known == null) {
            return given;
        }
        Settings archived = known.archiveUnknownOrInvalidSettings(
            given,
            unknown -> leftOut.add(unknown.getKey()),
            (invalid, why) -> leftOut.add(invalid.getKey())
        );
        return archived.filter(key -> !key.startsWith(AbstractScopedSettings.ARCHIVED_SETTINGS_PREFIX));
    }

    private static void createNext(List<Pending> pending, int at, List<Restored> restored, Runnable done) {
        if (at == pending.size()) {
            done.run();
            return;
        }
        CreateIndexRequest request = pending.get(at).request();
        List<String> leftOut = pending.get(at).leftOut();
        InfinoClient.createIndex(request, ActionListener.wrap(created -> {
            String note = leftOut.isEmpty() ? null : "created without the settings this cluster does not know: " + String.join(", ", leftOut);
            if (note != null) {
                log.warn("[infino] index {} was {}", request.index(), note);
            }
            restored.add(new Restored(request.index(), Outcome.CREATED, note));
            createNext(pending, at + 1, restored, done);
        }, e -> {
            log.warn("[infino] index {} could not be rebuilt", request.index(), e);
            restored.add(new Restored(request.index(), Outcome.FAILED, e.getMessage()));
            createNext(pending, at + 1, restored, done);
        }));
    }

    private static void declare() {
        if (declared.compareAndSet(false, true)) {
            InfinoNative.ensureTable(TABLE, "{}");
            // The first write reads the table back (heldRows), and on the
            // hosted service a read can miss a table this new for a moment:
            // wait out the read window once per node life. A write that still
            // misses it is tried again (record).
            InfinoNative.awaitVisible(TABLE);
        }
    }

    /** One row: the index as a create request would describe it, plus its table and the metadata version it was recorded at. */
    static String rowOf(IndexMetadata md) {
        try (XContentBuilder b = XContentFactory.jsonBuilder()) {
            b.startObject();
            b.field("name", md.getIndex().getName());
            b.field("table", InfinoNative.tableFor(md.getIndex(), md.getSettings()));
            b.field(VERSION, md.getVersion());
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

    /**
     * The create request a row describes, without the settings
     * {@code known} (the cluster's index settings; null for none to hold
     * them to) does not know or cannot take as given, each named onto
     * {@code leftOut}: the cluster refuses a create request carrying such a
     * setting, where a restore sets it aside. A row a loader wrote from
     * another cluster's snapshot can carry one, from a plugin this cluster
     * does not run or from another engine altogether.
     */
    @SuppressWarnings("unchecked")
    static CreateIndexRequest requestOf(String name, Map<String, Object> row, IndexScopedSettings known, List<String> leftOut) {
        CreateIndexRequest request = new CreateIndexRequest(name);
        Object settings = row.get("settings");
        if (settings instanceof Map<?, ?> s) {
            request.settings(knownSettings(Settings.builder().loadFromMap((Map<String, ?>) s).build(), known, leftOut));
        }
        if (InfinoPlugin.inDefaultMode(request.settings())) {
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
