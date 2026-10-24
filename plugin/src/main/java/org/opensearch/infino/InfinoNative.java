/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import com.sun.jna.Native;
import com.sun.jna.Pointer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Loads the Infino cdylib once per node and holds a single connection.
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
    // Catalog backend: one catalog, one table per index-shard (see tableFor). The
    // default is in-process and non-persistent; set -Dinfino.uri=file:///abs/path
    // or s3://bucket/prefix for a table that survives restart in lockstep with Lucene.
    private static final String URI = System.getProperty("infino.uri", "memory://");

    private static volatile InfinoLib lib;
    private static volatile Pointer conn;
    private static final Map<String, Map<String, Column>> COLUMNS = new ConcurrentHashMap<>();

    /** One column of a table, as the bridge describes it. */
    public record Column(String name, String type, boolean fts, boolean vector) {
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

    /** The bridge answered with an error object. */
    public static final class InfinoException extends RuntimeException {
        InfinoException(String message) {
            super(message);
        }
    }

    private InfinoNative() {}

    public static void init() {
        if (lib == null) {
            synchronized (InfinoNative.class) {
                if (lib == null) {
                    InfinoLib loaded = Native.load(LIB, InfinoLib.class);
                    Pointer opened = loaded.infino_open(URI);
                    if (opened == null) {
                        throw new IllegalStateException("infino_open(" + URI + ") returned null");
                    }
                    conn = opened;
                    lib = loaded;
                }
            }
        }
    }

    /** Per-shard Infino table name, e.g. os_myindex_0. Must match on write and query. */
    public static String tableFor(String index, int shard) {
        return "os_" + index + "_" + shard;
    }

    private static String take(Pointer p) {
        if (p == null) {
            return null;
        }
        String s = p.getString(0, "UTF-8");
        lib.infino_free_string(p);
        return s;
    }

    private static String takeOrThrow(Pointer p) {
        String s = take(p);
        if (s == null) {
            throw new InfinoException("the bridge returned null");
        }
        if (s.startsWith("{\"error\"")) {
            Object error = object(s).get("error");
            throw new InfinoException(error == null ? s : error.toString());
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
        takeOrThrow(lib.infino_ensure_table(conn, name, mappingJson));
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
        return lib.infino_drop(conn, name);
    }

    public static int optimize(String name) {
        init();
        return lib.infino_optimize(conn, name);
    }

    // ---- documents ----

    /** The reason one document would be refused by the declared columns, or null if it conforms. */
    public static String checkDoc(String name, String source) {
        init();
        String why = take(lib.infino_check_doc(conn, name, source));
        return why == null || why.isEmpty() ? null : why;
    }

    /** Index many documents in one append (parallel arrays of length n). */
    @SuppressWarnings("unchecked")
    public static BatchResult indexBatch(String name, String[] osids, String[] sources, int n) {
        init();
        COLUMNS.remove(name); // the batch may have added paths to the schema
        Map<String, Object> out = object(takeOrThrow(lib.infino_index_batch(conn, name, osids, sources, n)));
        List<Failure> failed = new ArrayList<>();
        for (Map<String, Object> f : (List<Map<String, Object>>) out.get("failed")) {
            failed.add(new Failure(((Number) f.get("i")).intValue(), (String) f.get("reason")));
        }
        return new BatchResult(((Number) out.get("ok")).intValue(), failed);
    }

    /** Tombstone every row with one of these ids, in one mutation. */
    public static int deleteOsids(String name, String[] osids, int n) {
        init();
        return lib.infino_delete_osids(conn, name, osids, n);
    }

    public static String getSource(String name, String osid) {
        init();
        return take(lib.infino_get(conn, name, osid));
    }

    // ---- queries ----

    public static String sql(String q) {
        init();
        String out = takeOrThrow(lib.infino_sql(conn, q));
        log.debug("[infino] sql: {} -> {} chars", q, out.length());
        return out;
    }

    public static List<Map<String, Object>> sqlRows(String q) {
        return rows(sql(q));
    }

    public static String bm25(String n, String c, String q, int k) {
        init();
        return takeOrThrow(lib.infino_bm25(conn, n, c, q, k));
    }

    /** Count of rows matching a full-text query, without materializing them. */
    public static long count(String n, String c, String q) {
        init();
        return Long.parseLong(takeOrThrow(lib.infino_count(conn, n, c, q)).trim());
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
