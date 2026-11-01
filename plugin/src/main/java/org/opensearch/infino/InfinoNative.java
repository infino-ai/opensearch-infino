/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import com.sun.jna.Native;
import com.sun.jna.Pointer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.OpenSearchException;
import org.opensearch.common.cache.Cache;
import org.opensearch.common.cache.CacheBuilder;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.index.Index;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.UnaryOperator;
import java.util.regex.Pattern;

/**
 * Loads the Infino cdylib once per node and holds a single connection.
 *
 * <p>The connection goes to the hosted service when the node is configured
 * with an account (see {@link InfinoAccount}): every table operation then
 * forwards there through the engine's own hosted client, and the node holds
 * no table data. Without one it goes to the node's own URI. The node's own
 * settings decide at boot; the account route can change it for the node's
 * life, and a change reopens the connection.
 *
 * <p>The engine and the bridge are thread-safe, so nothing here serializes
 * callers: a batch append and the queries on the node run concurrently.
 * Column descriptions are cached per table and dropped whenever the table's
 * schema may have moved (a mapping applied, a batch appended, a drop).
 */
public final class InfinoNative {
    private static final Logger log = LogManager.getLogger(InfinoNative.class);
    // Path to the native bridge, set with -Dinfino.lib=/abs/path (see scripts/run.sh);
    // the bare name only resolves if it is on java.library.path.
    private static final String LIB = System.getProperty("infino.lib", "libinfino_capi.so");
    // Where the tables live when the cluster has no hosted account: the node's own
    // URI, -Dinfino.uri=file:///abs/path or s3://bucket/prefix for tables that
    // survive restart. The default is in-process and non-persistent.
    private static final String NODE_URI = System.getProperty("infino.uri", "memory://");

    /**
     * What a connection is opened to: the URI, its bearer key (null for none:
     * the engine then reads {@code INFINO_API_KEY}), and how to name it in a
     * message, which never carries the key.
     */
    record Target(String uri, String apiKey, boolean hosted, String name) {
        /** The node's own URI, with no key. */
        static Target node() {
            return new Target(NODE_URI, null, false, NODE_URI);
        }

        /** A hosted account: the service's base URL, the database, and the key. */
        static Target hosted(String url, String database, String apiKey) {
            String base = InfinoAccount.baseUrl(url);
            return new Target(base + "/" + database, apiKey, true, base + " database " + database);
        }
    }

    private static volatile InfinoLib lib;
    /** Where the engine caches what it reads from the node's own storage (see {@link InfinoPlugin#CACHE_DIR}); null for none. */
    private static volatile String cacheDir;
    private static volatile Pointer conn;
    /** The target {@link #conn} was opened to. */
    private static volatile Target opened;
    /** The target the node is configured for: its own URI until the settings or the account route say otherwise. */
    private static volatile Target wanted = Target.node();
    private static final Map<String, Map<String, Column>> COLUMNS = new ConcurrentHashMap<>();
    /** The count's key in {@link #valueCounts}'s rows, as the engine names it. */
    static final String VALUE_COUNT = "count(*)";
    /** How long a prefetched source waits for its fetch phase. */
    private static final TimeValue SOURCE_PREFETCH_TTL = TimeValue.timeValueSeconds(30);
    /** Bytes of prefetched sources held at once across requests. */
    private static final long SOURCE_PREFETCH_BYTES = 64L << 20;
    /**
     * Sources the query phase fetched for its page, read back by the fetch
     * phase one hit at a time: one statement per page instead of one per hit.
     * Keyed by table and id; a source does not change under an id except
     * through an update, which replays as its own write, so a stale entry
     * can only be served inside the TTL.
     */
    private static final Cache<String, String> SOURCES = CacheBuilder.<String, String>builder()
        .setExpireAfterWrite(SOURCE_PREFETCH_TTL)
        .setMaximumWeight(SOURCE_PREFETCH_BYTES)
        .weigher((key, value) -> key.length() + value.length())
        .build();

    /**
     * One column of a table, as the bridge describes it. {@code mask} is the
     * security plugin's field masking for the request, when the field is
     * masked for the user: how a value of the column is hashed before it
     * leaves the node (null when it is not).
     */
    public record Column(String name, String type, boolean fts, boolean vector, UnaryOperator<String> mask) {
        public Column(String name, String type, boolean fts, boolean vector) {
            this(name, type, fts, vector, null);
        }

        /** This column as the request sees it, its values masked by {@code mask}. */
        Column masked(UnaryOperator<String> mask) {
            return new Column(name, type, fts, vector, mask);
        }

        public boolean masked() {
            return mask != null;
        }

        public boolean isString() {
            return "str".equals(type);
        }

        public boolean isNumeric() {
            return "i64".equals(type) || "f64".equals(type);
        }

        public boolean isTimestamp() {
            return "ts".equals(type);
        }

        public boolean isBoolean() {
            return "bool".equals(type);
        }
    }

    /** One document the bridge refused, with the index it had in the batch. */
    public record Failure(int index, String reason) {}

    /** Outcome of a batch append. */
    public record BatchResult(int ok, List<Failure> failed) {}

    /**
     * The bridge answered with an error object. Carries the HTTP status a
     * hosted refusal came with (a key refused, an account at a limit), so the
     * caller is answered with the same one and the service's own message;
     * anything else is an internal error.
     */
    public static final class InfinoException extends OpenSearchException {
        private final RestStatus status;

        InfinoException(String message) {
            this(message, RestStatus.INTERNAL_SERVER_ERROR);
        }

        InfinoException(String message, RestStatus status) {
            super(message);
            this.status = status;
        }

        @Override
        public RestStatus status() {
            return status;
        }
    }

    private InfinoNative() {}

    /** The bridge library, loaded once per node. */
    private static InfinoLib lib() {
        InfinoLib loaded = lib;
        if (loaded == null) {
            synchronized (InfinoNative.class) {
                loaded = lib;
                if (loaded == null) {
                    loaded = Native.load(LIB, InfinoLib.class);
                    lib = loaded;
                }
            }
        }
        return loaded;
    }

    /**
     * Open the connection the node is configured for, if it is not the one
     * open. A connection in use is not closed when another replaces it: a
     * call in flight on another thread still holds it, and an account
     * changes a few times in a node's life at most.
     */
    public static void init() {
        Target target = wanted;
        if (conn != null && target.equals(opened)) {
            return;
        }
        synchronized (InfinoNative.class) {
            if (conn != null && target.equals(opened)) {
                return;
            }
            InfinoLib loaded = lib();
            Pointer p = loaded.infino_open_with(target.uri(), target.apiKey(), target.hosted() ? null : cacheDir);
            if (p == null) {
                throw openFailure(loaded, target);
            }
            if (conn != null) {
                log.info("[infino] tables are now served from {} (were from {})", target.name(), opened.name());
            } else {
                log.info("[infino] tables are served from {}", target.name());
            }
            COLUMNS.clear();
            SOURCES.invalidateAll();
            InfinoRequestCache.changedAll();
            conn = p;
            opened = target;
        }
    }

    /** Why an open returned null, as the exception the caller gets. */
    private static InfinoException openFailure(InfinoLib loaded, Target target) {
        String why = take(loaded, loaded.infino_open_error());
        if (why == null) {
            return new InfinoException("cannot open " + target.name());
        }
        Map<String, Object> error = object(why);
        return new InfinoException("cannot open " + target.name() + ": " + error.get("error"), statusOf(error));
    }

    /**
     * Take {@code target} as what the node is configured for: from the node's
     * settings at boot, or from the account route for the node's life. A
     * connection already in use is reopened at once so the next call finds
     * the new one; a failure there is reported to the caller and the next
     * call retries.
     */
    static void configure(Target target) {
        if (target.equals(wanted)) {
            return;
        }
        wanted = target;
        if (conn != null) {
            init();
        }
    }

    /** Take {@code dir} as the node's cache directory, at boot, before the first open; null for none. */
    static void cacheDir(String dir) {
        cacheDir = dir;
    }

    /** The target the node is configured for. */
    static Target target() {
        return wanted;
    }

    /** Whether the tables are served from the hosted service rather than the node's own storage. */
    public static boolean hosted() {
        return wanted.hosted();
    }

    /** How the tables are served, for a status report. */
    static String serving() {
        return wanted.name();
    }

    /**
     * Ask the hosted service at {@code baseUrl} for a trial account, with
     * {@code database} as its first database: the service's response. A
     * refusal is thrown with the service's status and message.
     */
    static Map<String, Object> trial(String baseUrl, String database) {
        InfinoLib loaded = lib();
        String out = take(loaded, loaded.infino_trial(baseUrl, database));
        if (out == null) {
            throw new InfinoException("the trial call returned nothing");
        }
        Map<String, Object> response = object(out);
        if (response.containsKey("error")) {
            throw new InfinoException(String.valueOf(response.get("error")), statusOf(response));
        }
        return response;
    }

    /**
     * Open {@code database} on the hosted service with {@code apiKey} once and
     * close it again: a key the service refuses fails here, with its status.
     */
    static void probe(String baseUrl, String database, String apiKey) {
        InfinoLib loaded = lib();
        String uri = InfinoAccount.baseUrl(baseUrl) + "/" + database;
        Pointer p = loaded.infino_open_with(uri, apiKey, null);
        if (p == null) {
            throw openFailure(loaded, new Target(uri, apiKey, true, baseUrl + " database " + database));
        }
        loaded.infino_close(p);
    }

    /** The HTTP status an error object carries, else an internal error. */
    private static RestStatus statusOf(Map<String, Object> error) {
        Object status = error.get("status");
        RestStatus known = status instanceof Number n ? RestStatus.fromCode(n.intValue()) : null;
        return known == null ? RestStatus.INTERNAL_SERVER_ERROR : known;
    }

    /**
     * The Infino table of an index: the one named by {@code index.infino.table}
     * when set, else the index name. Named by the index and not by its UUID,
     * so an index that comes back on a fresh node (see {@link InfinoRegistry})
     * finds the rows its predecessor wrote; an index deleted on purpose drops
     * its table, so a recreated one starts empty.
     */
    public static String tableFor(Index index, Settings indexSettings) {
        String explicit = InfinoPlugin.INFINO_TABLE.get(indexSettings);
        if (!explicit.isEmpty()) {
            return explicit;
        }
        return tableFor(index.getName());
    }

    /** The table an index of this name is served from when none is named explicitly. */
    public static String tableFor(String indexName) {
        // An index name may carry characters a table name may not (the security
        // plugin's and Dashboards' own indices start with a dot); each becomes '_'.
        // Index names are lower case already, as the engine's SQL layer folds them.
        return TABLE_PREFIX + TABLE_NAME_OTHER.matcher(indexName).replaceAll("_");
    }

    /** What the engine does not take in a table name: anything outside letters, digits, '_' and '-'. */
    private static final Pattern TABLE_NAME_OTHER = Pattern.compile("[^A-Za-z0-9_-]");
    /** Every table behind an index starts with this, so the registry and the tables of other users of a database stay apart. */
    private static final String TABLE_PREFIX = "os_";

    /** Whether the table name came from {@code index.infino.table}, in which case deleting the index leaves it. */
    public static boolean tableIsExplicit(Settings indexSettings) {
        return !InfinoPlugin.INFINO_TABLE.get(indexSettings).isEmpty();
    }

    private static String take(Pointer p) {
        return take(lib, p);
    }

    private static String take(InfinoLib loaded, Pointer p) {
        if (p == null) {
            return null;
        }
        String s = p.getString(0, "UTF-8");
        loaded.infino_free_string(p);
        return s;
    }

    private static String takeOrThrow(Pointer p) {
        String s = take(p);
        if (s == null) {
            throw new InfinoException("the bridge returned null");
        }
        if (s.startsWith("{\"error\"")) {
            Map<String, Object> error = object(s);
            Object message = error.get("error");
            throw new InfinoException(message == null ? s : message.toString(), statusOf(error));
        }
        return s;
    }

    // ---- JSON ----

    @SuppressWarnings("unchecked")
    static Map<String, Object> object(String json) {
        try (XContentParser parser = parser(json)) {
            return parser.map();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @SuppressWarnings("unchecked")
    static List<Map<String, Object>> rows(String json) {
        try (XContentParser parser = parser(json)) {
            List<Object> list = parser.list();
            List<Map<String, Object>> out = new ArrayList<>(list.size());
            for (Object o : list) {
                out.add((Map<String, Object>) o);
            }
            return out;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static XContentParser parser(String json) throws IOException {
        return JsonXContent.jsonXContent.createParser(
            NamedXContentRegistry.EMPTY,
            DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
            json
        );
    }

    // ---- schema ----

    /** Create the table from a mapping, or add the mapped fields it lacks. */
    public static void ensureTable(String name, String mappingJson) {
        init();
        COLUMNS.remove(name);
        InfinoRequestCache.changed(name);
        try {
            takeOrThrow(lib.infino_ensure_table(conn, name, mappingJson));
        } finally {
            InfinoRequestCache.changed(name);
        }
    }

    /** Column name → description; empty if the table does not exist yet. */
    public static Map<String, Column> columns(String name) {
        init();
        Map<String, Column> cached = COLUMNS.get(name);
        if (cached != null) {
            return cached;
        }
        String json = take(lib.infino_columns(conn, name));
        if (json == null || json.isEmpty()) {
            return Collections.emptyMap();
        }
        Map<String, Column> out = new LinkedHashMap<>();
        for (Map<String, Object> c : rows(json)) {
            String column = (String) c.get("name");
            out.put(
                column,
                new Column(column, (String) c.get("type"), Boolean.TRUE.equals(c.get("fts")), Boolean.TRUE.equals(c.get("vector")))
            );
        }
        Map<String, Column> view = Collections.unmodifiableMap(out);
        COLUMNS.put(name, view);
        return view;
    }

    public static void forgetColumns(String name) {
        COLUMNS.remove(name);
    }

    public static int dropTable(String name) {
        init();
        COLUMNS.remove(name);
        InfinoRequestCache.changed(name);
        try {
            return lib.infino_drop(conn, name);
        } finally {
            InfinoRequestCache.changed(name);
        }
    }

    /**
     * Compact the table; for an edge index's ({@link InfinoPlugin#INFINO_EDGES}),
     * also build the adjacency index the engine's graph walks read over its rows.
     */
    public static int optimize(String name, Settings indexSettings) {
        init();
        boolean edges = InfinoPlugin.INFINO_EDGES.get(indexSettings);
        InfinoRequestCache.changed(name);
        try {
            return edges ? lib.infino_optimize_edges(conn, name) : lib.infino_optimize(conn, name);
        } finally {
            InfinoRequestCache.changed(name);
        }
    }

    // ---- documents ----

    /** The reason one document would be refused by the declared columns, or null if it conforms. */
    public static String checkDoc(String name, String source) {
        init();
        String why = take(lib.infino_check_doc(conn, name, source));
        return why == null || why.isEmpty() ? null : why;
    }

    /**
     * The ids and sources of a batch as one UTF-8 buffer with the end offset
     * of each id and each source; with routing values, each row's follows its
     * source (empty for none) and {@code rtEnds} holds where it ends.
     */
    private record Packed(byte[] buf, int[] idEnds, int[] srcEnds, int[] rtEnds) {
        static Packed of(List<String> osids, List<String> sources) {
            return of(osids, sources, null);
        }

        static Packed of(List<String> osids, List<String> sources, List<String> routings) {
            int n = osids.size();
            int[] idEnds = new int[n];
            int[] srcEnds = new int[n];
            int[] rtEnds = routings == null ? null : new int[n];
            byte[][] idBytes = new byte[n][];
            byte[][] srcBytes = new byte[n][];
            byte[][] rtBytes = new byte[n][];
            int total = 0;
            for (int i = 0; i < n; i++) {
                idBytes[i] = osids.get(i).getBytes(StandardCharsets.UTF_8);
                srcBytes[i] = sources.get(i).getBytes(StandardCharsets.UTF_8);
                String routing = routings == null ? null : routings.get(i);
                rtBytes[i] = routing == null ? new byte[0] : routing.getBytes(StandardCharsets.UTF_8);
                total += idBytes[i].length;
                idEnds[i] = total;
                total += srcBytes[i].length;
                srcEnds[i] = total;
                if (rtEnds != null) {
                    total += rtBytes[i].length;
                    rtEnds[i] = total;
                }
            }
            byte[] buf = new byte[total];
            int at = 0;
            for (int i = 0; i < n; i++) {
                System.arraycopy(idBytes[i], 0, buf, at, idBytes[i].length);
                at += idBytes[i].length;
                System.arraycopy(srcBytes[i], 0, buf, at, srcBytes[i].length);
                at += srcBytes[i].length;
                if (rtEnds != null) {
                    System.arraycopy(rtBytes[i], 0, buf, at, rtBytes[i].length);
                    at += rtBytes[i].length;
                }
            }
            return new Packed(buf, idEnds, srcEnds, rtEnds);
        }
    }

    /** One refused document of a prepared batch: which batch, which document, why. */
    public record PreparedFailure(int batch, int index, String reason) {}

    /** Outcome of appending prepared batches. */
    public record PreparedResult(int ok, List<PreparedFailure> failed) {}

    /**
     * Parse and convert a batch ahead of its append, so the work overlaps the
     * append that is running. The handle must go to {@link #appendPrepared}
     * or {@link #freePrepared}. Null when the table is unknown.
     */
    public static Pointer prepare(String name, List<String> osids, List<String> sources) {
        init();
        Packed p = Packed.of(osids, sources);
        return lib.infino_prepare_batch(conn, name, p.buf(), p.buf().length, p.idEnds(), p.srcEnds(), osids.size());
    }

    /**
     * {@link #prepare} with each row's routing value (null for none), stamp
     * and the operation that wrote it (each negative for none), by writer
     * {@code gen}, kept in the {@link InfinoQueryTranslator#ROUTING},
     * {@link InfinoQueryTranslator#SEQ}, {@link InfinoQueryTranslator#OP} and
     * {@link InfinoQueryTranslator#GEN} columns.
     */
    public static Pointer prepare(
        String name,
        List<String> osids,
        List<String> sources,
        List<String> routings,
        long[] seqs,
        long[] ops,
        long gen
    ) {
        init();
        Packed p = Packed.of(osids, sources, routings);
        return lib.infino_prepare_batch_meta(
            conn,
            name,
            p.buf(),
            p.buf().length,
            p.idEnds(),
            p.srcEnds(),
            p.rtEnds(),
            seqs,
            ops,
            gen,
            osids.size()
        );
    }

    /**
     * The highest operation writer {@code gen} recorded on a row of table
     * {@code name}, or null when no row carries one of its operations.
     */
    public static Long highestOp(String name, long gen) {
        if (!columns(name).containsKey(InfinoQueryTranslator.OP)) {
            return null;
        }
        String op = InfinoQueryTranslator.quoteIdent(InfinoQueryTranslator.OP);
        String sql = "SELECT max(" + op + ") AS highest FROM " + InfinoQueryTranslator.quoteIdent(name) + " WHERE "
            + InfinoQueryTranslator.quoteIdent(InfinoQueryTranslator.GEN) + " = " + gen;
        for (Map<String, Object> row : sqlRows(sql)) {
            Object highest = row.get("highest");
            if (highest instanceof Number n) {
                return n.longValue();
            }
        }
        return null;
    }

    /**
     * Append prepared batches in one commit; the handles are released by the
     * call. The rows of {@code replaced} ids replace the table's rows under
     * those ids in the same mutation, so a failed write leaves the old rows.
     */
    @SuppressWarnings("unchecked")
    public static PreparedResult appendPrepared(String name, List<Pointer> prepared, Collection<String> replaced) {
        init();
        COLUMNS.remove(name); // the batch may have added paths to the schema
        String[] ids = replaced.toArray(new String[0]);
        InfinoRequestCache.changed(name);
        Map<String, Object> out;
        try {
            out = object(takeOrThrow(lib.infino_append_prepared(conn, name, prepared.toArray(new Pointer[0]), prepared.size(), ids, ids.length)));
        } finally {
            InfinoRequestCache.changed(name);
        }
        List<PreparedFailure> failed = new ArrayList<>();
        for (Map<String, Object> f : (List<Map<String, Object>>) out.get("failed")) {
            failed.add(new PreparedFailure(((Number) f.get("b")).intValue(), ((Number) f.get("i")).intValue(), (String) f.get("reason")));
        }
        return new PreparedResult(((Number) out.get("ok")).intValue(), failed);
    }

    public static void freePrepared(Pointer prepared) {
        if (prepared != null) {
            lib.infino_free_prepared(prepared);
        }
    }

    /**
     * Index many documents in one append (parallel arrays of length n). The
     * ids and sources cross to the bridge as one UTF-8 buffer with end
     * offsets, so the call marshals three arrays rather than one native
     * string per document.
     */
    @SuppressWarnings("unchecked")
    public static BatchResult indexBatch(String name, String[] osids, String[] sources, int n) {
        init();
        COLUMNS.remove(name); // the batch may have added paths to the schema
        Packed p = Packed.of(List.of(osids).subList(0, n), List.of(sources).subList(0, n));
        InfinoRequestCache.changed(name);
        Map<String, Object> out;
        try {
            out = object(takeOrThrow(lib.infino_index_batch_buf(conn, name, p.buf(), p.buf().length, p.idEnds(), p.srcEnds(), n)));
        } finally {
            InfinoRequestCache.changed(name);
        }
        List<Failure> failed = new ArrayList<>();
        for (Map<String, Object> f : (List<Map<String, Object>>) out.get("failed")) {
            failed.add(new Failure(((Number) f.get("i")).intValue(), (String) f.get("reason")));
        }
        return new BatchResult(((Number) out.get("ok")).intValue(), failed);
    }

    /** Tombstone every row with one of these ids, in one mutation. */
    public static int deleteOsids(String name, String[] osids, int n) {
        init();
        InfinoRequestCache.changed(name);
        try {
            return lib.infino_delete_osids(conn, name, osids, n);
        } finally {
            InfinoRequestCache.changed(name);
        }
    }

    /** The source of one id: the page prefetch when the query phase ran one, else a lookup. */
    public static String getSource(String name, String osid) {
        init();
        String prefetched = SOURCES.get(name + "\u0000" + osid);
        if (prefetched != null) {
            return prefetched;
        }
        return take(lib.infino_get(conn, name, osid));
    }

    /** The source of one id from the table, never from the prefetch: for a write that merges into it. */
    public static String getSourceFresh(String name, String osid) {
        init();
        return take(lib.infino_get(conn, name, osid));
    }

    /** Drop the prefetched sources of ids a write just changed, so a read after the write sees the table. */
    public static void forgetSources(String name, Collection<String> osids) {
        for (String id : osids) {
            SOURCES.invalidate(name + "\u0000" + id);
        }
    }

    // ---- queries ----

    public static String sql(String q) {
        init();
        String out = takeOrThrow(lib.infino_sql(conn, q));
        log.debug("[infino] sql: {} -> {} chars", q, out.length());
        return out;
    }

    /**
     * The indices a caller's read-only statement reads, each once, in the
     * order it names them (see {@link InfinoSql}); a statement the SQL route
     * does not serve throws with the status that says why.
     */
    static List<String> sqlIndices(String sql) {
        init();
        Object listed = object(takeOrThrow(lib.infino_sql_indices(sql))).get("indices");
        List<String> out = new ArrayList<>();
        if (listed instanceof List<?> names) {
            for (Object name : names) {
                out.add(String.valueOf(name));
            }
        }
        return out;
    }

    /**
     * A caller's read-only statement, each index it reads replaced by its
     * view in {@code views} (see {@link InfinoSql}): its schema and rows as
     * JSON, or a throw with the status that says why it was not run.
     */
    static String sqlRead(String sql, String views) {
        init();
        return takeOrThrow(lib.infino_sql_read(conn, sql, views));
    }

    public static List<Map<String, Object>> sqlRows(String q) {
        return rows(sql(q));
    }

    /**
     * The top {@code k} rows of a full-text query ({@code mode} "and" / "or")
     * by BM25, best first, each with its id, source and score: a page of hits
     * in one engine call, with no statement to plan.
     */
    public static List<Map<String, Object>> bm25Hits(String n, String c, String q, String mode, int k) {
        init();
        String out = takeOrThrow(lib.infino_bm25(conn, n, c, q, mode, k));
        log.debug("[infino] bm25: {} {} {} k={} -> {} chars", n, c, q, k, out.length());
        return rows(out);
    }

    /** Count of rows matching a full-text query ({@code mode} "and" / "or"), without materializing them. */
    public static long count(String n, String c, String q, String mode) {
        init();
        return Long.parseLong(takeOrThrow(lib.infino_count(conn, n, c, q, mode)).trim());
    }

    /**
     * A column's rows counted per value, without a statement: one row per
     * value (under the column's name, null for the rows without one) with its
     * count under {@link #VALUE_COUNT}. The engine reads them from the table's
     * statistics when those count every row exactly, and runs the grouped
     * count otherwise. Null when there are more than {@code maxRows}.
     */
    public static List<Map<String, Object>> valueCounts(String n, String column, int maxRows) {
        init();
        String out = takeOrThrow(lib.infino_value_counts(conn, n, column, maxRows)).trim();
        return out.equals("null") ? null : rows(out);
    }

    /**
     * Sources for one page of ids in one statement, kept for the fetch phase
     * that follows the query phase. The table has no index on ids, so this
     * scans the id column; the SQL hit paths carry the source in the hit
     * statement instead and call {@link #keepSources}. Used by the vector
     * paths, whose entry points return ids and scores only.
     */
    public static void prefetchSources(String name, List<String> osids) {
        if (osids.isEmpty()) {
            return;
        }
        List<String> lits = new ArrayList<>(osids.size());
        for (String id : osids) {
            lits.add(InfinoQueryTranslator.literal(id));
        }
        String sql = "SELECT " + InfinoQueryTranslator.OSID + ", " + InfinoQueryTranslator.SOURCE + " FROM "
            + InfinoQueryTranslator.quoteIdent(name) + " WHERE " + InfinoQueryTranslator.OSID + " IN (" + String.join(", ", lits) + ")";
        keepSources(name, sqlRows(sql));
    }

    /** Ids per statement when probing which of a batch's ids the table already holds. */
    private static final int PROBE_IDS_PER_STATEMENT = 1_000;

    /**
     * Which of these ids the table already holds, through the id column's
     * term index: one statement per thousand ids, a union of their postings,
     * then an exact comparison, since an id with punctuation indexes as
     * several terms and the union may name ids that only share one.
     */
    public static Set<String> existingOsids(String name, Collection<String> osids) {
        Set<String> wanted = new HashSet<>(osids);
        Set<String> found = new HashSet<>();
        List<String> batch = new ArrayList<>(PROBE_IDS_PER_STATEMENT);
        for (String id : wanted) {
            batch.add(id);
            if (batch.size() == PROBE_IDS_PER_STATEMENT) {
                probe(name, batch, wanted, found);
                batch.clear();
            }
        }
        if (!batch.isEmpty()) {
            probe(name, batch, wanted, found);
        }
        return found;
    }

    private static void probe(String name, List<String> ids, Set<String> wanted, Set<String> found) {
        String terms = InfinoQueryTranslator.literal(String.join(" ", ids));
        String sql = "SELECT " + InfinoQueryTranslator.OSID + " FROM token_match(" + InfinoQueryTranslator.literal(name) + ", '"
            + InfinoQueryTranslator.OSID + "', " + terms + ", 'or')";
        for (Map<String, Object> row : sqlRows(sql)) {
            Object id = row.get(InfinoQueryTranslator.OSID);
            if (id != null && wanted.contains(id.toString())) {
                found.add(id.toString());
            }
        }
    }

    /**
     * The routing value the table's row for {@code id} was written with,
     * through the id column's term index; null when the row was written with
     * none, when there is no row, or when the table never took a routed write.
     */
    public static String routingOf(String name, String id) {
        if (!columns(name).containsKey(InfinoQueryTranslator.ROUTING)) {
            return null;
        }
        String sql = "SELECT " + InfinoQueryTranslator.OSID + ", " + InfinoQueryTranslator.quoteIdent(InfinoQueryTranslator.ROUTING)
            + " FROM token_match(" + InfinoQueryTranslator.literal(name) + ", '" + InfinoQueryTranslator.OSID + "', "
            + InfinoQueryTranslator.literal(id) + ", 'and')";
        for (Map<String, Object> row : sqlRows(sql)) {
            Object rowId = row.get(InfinoQueryTranslator.OSID);
            Object routing = row.get(InfinoQueryTranslator.ROUTING);
            if (rowId != null && id.equals(rowId.toString())) {
                return routing == null ? null : routing.toString();
            }
        }
        return null;
    }

    /** Keep the sources of rows a hit statement returned (id and source columns), for the fetch phase. */
    public static void keepSources(String name, List<Map<String, Object>> rows) {
        for (Map<String, Object> row : rows) {
            Object id = row.get(InfinoQueryTranslator.OSID);
            Object source = row.get(InfinoQueryTranslator.SOURCE);
            if (id != null && source != null) {
                SOURCES.put(name + "\u0000" + id, source.toString());
            }
        }
    }

    /** Osids of every row matching a full-text query (unranked), for query-scoped aggs. */
    public static List<String> tokenMatchOsids(String n, String c, String q) {
        init();
        List<String> out = new ArrayList<>();
        for (Map<String, Object> row : rows(takeOrThrow(lib.infino_token_match(conn, n, c, q)))) {
            out.add((String) row.get(InfinoQueryTranslator.OSID));
        }
        return out;
    }

    public static String knn(String n, String c, String v, int k) {
        init();
        return takeOrThrow(lib.infino_knn(conn, n, c, v, k));
    }

    public static String hybrid(String n, String tc, String tq, String vc, String v, int k) {
        init();
        return takeOrThrow(lib.infino_hybrid(conn, n, tc, tq, vc, v, k));
    }
}
