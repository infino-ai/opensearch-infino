/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import com.sun.jna.Library;
import com.sun.jna.Pointer;

/** JNA mapping of the infino-capi C ABI (libinfino_capi.so / .dylib). */
public interface InfinoLib extends Library {
    Pointer infino_open(String uri);

    /**
     * Open {@code uri} with the bearer key of a hosted URI (null for none) and
     * the directory the engine caches the node's own storage in (null for
     * none; a hosted URI takes none). A hosted connection is used once before
     * it is handed back, so a refused key fails here; null then, with the
     * reason in {@link #infino_open_error()}.
     */
    Pointer infino_open_with(String uri, String apiKey, String cacheDir);

    /** Why the last open on this thread returned null, as {@code {"error", "status"?}}; null when it did not fail. */
    Pointer infino_open_error();

    /** Whether the connection forwards to the hosted service (1) or runs the engine over storage of its own (0). */
    int infino_hosted(Pointer h);

    /**
     * Ask the hosted service for a trial account, made without a key: the
     * service's response ({@code api_key}, {@code database}, {@code credit_cents},
     * {@code console_url}) or {@code {"error", "status"}}.
     */
    Pointer infino_trial(String baseUrl, String database);

    void infino_close(Pointer h);

    Pointer infino_ensure_table(Pointer h, String name, String mappingJson);

    Pointer infino_columns(Pointer h, String name);

    Pointer infino_check_doc(Pointer h, String name, String source);

    Pointer infino_index_batch(Pointer h, String name, String[] osids, String[] sources, int count);

    /** One buffer of ids and sources with their end offsets: three native arrays instead of one allocation per string. */
    Pointer infino_index_batch_buf(Pointer h, String name, byte[] buf, long bufLen, int[] idEnds, int[] srcEnds, int count);

    /** Parse and convert a batch ahead of its append; null when the table is unknown. */
    Pointer infino_prepare_batch(Pointer h, String name, byte[] buf, long bufLen, int[] idEnds, int[] srcEnds, int count);

    /**
     * {@link #infino_prepare_batch} with each row's routing value after its source (empty for none), ending
     * at {@code rtEnds}, its stamp in {@code seqs} and the operation that wrote it in {@code ops} (each
     * negative for none), by writer {@code gen}.
     */
    Pointer infino_prepare_batch_meta(
        Pointer h,
        String name,
        byte[] buf,
        long bufLen,
        int[] idEnds,
        int[] srcEnds,
        int[] rtEnds,
        long[] seqs,
        long[] ops,
        long gen,
        int count
    );

    /**
     * Append prepared batches in one commit and release them; the rows whose ids
     * are in {@code replaced} replace the table's rows under those ids in the
     * same mutation (null or zero for a plain append).
     */
    Pointer infino_append_prepared(Pointer h, String name, Pointer[] prepared, int count, String[] replaced, int replacedCount);

    void infino_free_prepared(Pointer prepared);

    int infino_delete_osids(Pointer h, String name, String[] osids, int count);

    Pointer infino_get(Pointer h, String name, String osid);

    int infino_drop(Pointer h, String name);

    int infino_optimize(Pointer h, String name);

    int infino_optimize_edges(Pointer h, String name);

    Pointer infino_sql(Pointer h, String sql);

    /** The indices a caller's read-only statement reads, as {@code {"indices": [..]}}, or an error with its status. */
    Pointer infino_sql_indices(String sql);

    /** A caller's read-only statement run through the views {@code views} gives each index it reads (see {@link InfinoSql}). */
    Pointer infino_sql_read(Pointer h, String sql, String views);

    Pointer infino_bm25(Pointer h, String name, String column, String query, String mode, int k);

    Pointer infino_count(Pointer h, String name, String column, String query, String mode);

    Pointer infino_value_counts(Pointer h, String name, String column, int maxRows);

    Pointer infino_token_match(Pointer h, String name, String column, String query);

    Pointer infino_knn(Pointer h, String name, String column, String embJson, int k);

    Pointer infino_hybrid(Pointer h, String name, String textColumn, String textQuery, String vectorColumn, String embJson, int k);

    void infino_free_string(Pointer s);
}
