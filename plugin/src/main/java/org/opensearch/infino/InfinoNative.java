package org.opensearch.infino;

import com.sun.jna.Native;
import com.sun.jna.Pointer;

/** Loads the Infino cdylib once per node and holds a single Connection. */
public final class InfinoNative {
    // Path to libinfino_capi.dylib — set with -Dinfino.lib=/abs/path (see scripts/run.sh);
    // the bare name only resolves if it is on java.library.path.
    private static final String LIB = System.getProperty("infino.lib", "libinfino_capi.dylib");
    // Catalog backend. One catalog, one table per shard (see tableFor); tables are
    // keyed per shard so delete/recreate can drop cleanly. Default is in-process
    // (non-persistent); set -Dinfino.uri=file:///abs/path for a persistent on-disk
    // catalog that survives restart in lockstep with Lucene.
    private static final String URI = System.getProperty("infino.uri", "memory://");

    private static volatile InfinoLib lib;
    private static volatile Pointer conn;

    private InfinoNative() {}

    public static synchronized void init() {
        if (lib == null) {
            lib = Native.load(LIB, InfinoLib.class);
            conn = lib.infino_open(URI);
            if (conn == null) throw new IllegalStateException("infino_open(" + URI + ") returned null");
        }
    }

    /** Per-shard Infino table name, e.g. os_myindex_0. Must match on write and query. */
    public static String tableFor(String index, int shard) {
        return "os_" + index + "_" + shard;
    }

    /** Create the per-shard table with a schema inferred from one _source doc (idempotent). */
    public static synchronized int createFromSource(String name, String source) {
        init();
        return lib.infino_create_from_source(conn, name, source);
    }

    /** Column name → type ({i64,f64,bool,str,vec}); empty if the table does not exist yet. */
    public static synchronized java.util.Map<String, String> columns(String name) {
        init();
        String json = take(lib.infino_columns(conn, name));
        java.util.Map<String, String> out = new java.util.LinkedHashMap<>();
        if (json == null || json.isEmpty()) return out;
        java.util.regex.Matcher m = java.util.regex.Pattern
            .compile("\\{\"name\":\"((?:[^\"\\\\]|\\\\.)*)\",\"type\":\"([a-z0-9]+)\"\\}").matcher(json);
        while (m.find()) out.put(m.group(1), m.group(2));
        return out;
    }

    public static synchronized int dropTable(String name) {
        init();
        return lib.infino_drop(conn, name);
    }

    /** Delete all rows with this osid (for upsert / idempotent replay). */
    public static synchronized int deleteOsid(String name, String osid) {
        init();
        return lib.infino_delete_osid(conn, name, osid);
    }

    public static synchronized int indexSource(String name, long id, String osid, String source) {
        init();
        return lib.infino_index_source(conn, name, id, osid, source);
    }

    /** Index many docs in one Infino append (parallel arrays of length n). */
    public static synchronized int indexBatch(String name, long[] ids, String[] osids, String[] sources, int n) {
        init();
        return lib.infino_index_batch(conn, name, ids, osids, sources, n);
    }

    private static String take(Pointer p) {
        if (p == null) return null;
        String s = p.getString(0);
        lib.infino_free_string(p);
        return s;
    }

    public static synchronized String getSource(String name, String osid) { init(); return take(lib.infino_get(conn, name, osid)); }
    public static synchronized String sql(String q)       { init(); return take(lib.infino_sql(conn, q)); }
    public static synchronized String bm25(String n, String c, String q, int k) { init(); return take(lib.infino_bm25(conn, n, c, q, k)); }

    /** Count of docs matching a full-text query, without materializing the osids. */
    public static synchronized long tokenCount(String n, String c, String q) {
        init();
        String s = take(lib.infino_token_count(conn, n, c, q));
        if (s == null) return 0;
        java.util.regex.Matcher m = java.util.regex.Pattern.compile("\\d+").matcher(s);
        return m.find() ? Long.parseLong(m.group()) : 0;
    }

    /** Osids of every doc matching a full-text query (unranked) — for query-scoped aggs. */
    public static synchronized java.util.List<String> tokenMatchOsids(String n, String c, String q) {
        init();
        String json = take(lib.infino_token_match(conn, n, c, q));
        java.util.List<String> out = new java.util.ArrayList<>();
        if (json == null) return out;
        java.util.regex.Matcher m = java.util.regex.Pattern
            .compile("\"osid\":\"((?:[^\"\\\\]|\\\\.)*)\"").matcher(json);
        while (m.find()) out.add(m.group(1));
        return out;
    }
    public static synchronized String knn(String n, String c, String v, int k)  { init(); return take(lib.infino_knn(conn, n, c, v, k)); }
    public static synchronized String hybrid(String n, String tc, String tq, String vc, String v, int k) { init(); return take(lib.infino_hybrid(conn, n, tc, tq, vc, v, k)); }
}
