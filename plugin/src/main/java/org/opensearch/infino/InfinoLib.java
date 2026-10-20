package org.opensearch.infino;

import com.sun.jna.Library;
import com.sun.jna.Pointer;

/** JNA mapping of the infino-capi C ABI (libinfino_capi.dylib). */
public interface InfinoLib extends Library {
    Pointer infino_open(String uri);
    void infino_close(Pointer conn);
    int infino_create_from_source(Pointer conn, String name, String source);
    Pointer infino_columns(Pointer conn, String name);
    int infino_drop(Pointer conn, String name);
    int infino_delete_osid(Pointer conn, String name, String osid);
    int infino_index_source(Pointer conn, String name, long id, String osid, String source);
    int infino_index_batch(Pointer conn, String name, long[] ids, String[] osids, String[] sources, int count);
    Pointer infino_get(Pointer conn, String name, String osid);
    Pointer infino_sql(Pointer conn, String sql);
    Pointer infino_bm25(Pointer conn, String name, String column, String query, int k);
    Pointer infino_token_match(Pointer conn, String name, String column, String query);
    Pointer infino_token_count(Pointer conn, String name, String column, String query);
    Pointer infino_knn(Pointer conn, String name, String column, String embJson, int k);
    Pointer infino_hybrid(Pointer conn, String name, String textColumn, String textQuery, String vectorColumn, String embJson, int k);
    void infino_free_string(Pointer s);
}
