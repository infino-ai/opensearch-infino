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

    Pointer infino_attach_table(Pointer h, String name, String mappingJson);

    /** Whether an index could be attached to the table: {@code {"refused": null | "<why>"}}, or {@code {"error"}} when the table could not be read. */
    Pointer infino_attach_check(Pointer h, String name, String mappingJson);

    Pointer infino_columns(Pointer h, String name);

    Pointer infino_check_doc(Pointer h, String name, String source);

    Pointer infino_index_batch(Pointer h, String name, String[] osids, String[] sources, int count);

    /** One buffer of ids and sources with their end offsets: three native arrays instead of one allocation per string. */
    Pointer infino_index_batch_buf(Pointer h, String name, byte[] buf, long bufLen, int[] idEnds, int[] srcEnds, int count);

    /** Parse and convert a batch ahead of its append; null when it could not be, with the reason in {@link #infino_prepare_error()}. */
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

    /** Why the last prepare on this thread returned null, as {@code {"error", "status"?}}; null when it did not fail. */
    Pointer infino_prepare_error();

    /**
     * Append prepared batches in one commit and release them; the rows whose ids
     * are in {@code replaced} replace the table's rows under those ids in the
     * same mutation (null or zero for a plain append). {@code seqs} holds the
     * sequence number of every document, batch after batch in order (null or
     * zero for none).
     */
    Pointer infino_append_prepared(Pointer h, String name, Pointer[] prepared, int count, String[] replaced, int replacedCount, long[] seqs, int seqsCount);

    void infino_free_prepared(Pointer prepared);

    int infino_delete_osids(Pointer h, String name, String[] osids, int count);

    /** One id's source as {@code {"source": ..}}, null when the table holds no such id; {@code {"error": ..}} when the lookup fails. */
    Pointer infino_get(Pointer h, String name, String osid);

    /** One id's raw source as of the table's latest commit; "" when the table holds no such id, null when the lookup fails. */
    Pointer infino_get_latest(Pointer h, String name, String osid);

    /** The sources of several osids at once, as a JSON object from each id the table holds to its source. */
    Pointer infino_get_many(Pointer h, String name, String[] osids, int count);

    /** One id's document as {@code {"doc": {"source", "seq", "routing"}}} ({@code routing} left out for none), null when the table holds no such id; {@code {"error": ..}} when the lookup fails. */
    Pointer infino_get_doc(Pointer h, String name, String osid);

    /**
     * The documents of several osids, as {@code {"docs": {id: doc}}} with each
     * doc {@code {"source", "seq", "routing"}} as {@link #infino_get_doc} gives
     * it; an id the table does not hold is absent. {@code {"error": ..}} when a
     * lookup fails.
     */
    Pointer infino_get_docs(Pointer h, String name, String[] osids, int count);

    /** {@link #infino_get_docs} as of the table's latest commit. */
    Pointer infino_get_docs_latest(Pointer h, String name, String[] osids, int count);

    int infino_drop(Pointer h, String name);

    int infino_optimize(Pointer h, String name);

    int infino_optimize_edges(Pointer h, String name);

    Pointer infino_sql(Pointer h, String sql);

    /** {@link #infino_sql} reading each table's latest commit: for a read whose answer decides what a write does. */
    Pointer infino_sql_latest(Pointer h, String sql);

    /** Keep the connection the latest reads go through open until {@link #infino_release_latest}; without a hold it closes after each. */
    void infino_hold_latest(Pointer h);

    /** Let go of a hold {@link #infino_hold_latest} took. */
    void infino_release_latest(Pointer h);

    /** Close the connection the latest reads go through, held or not, so the next opens it afresh without the table handles it held. */
    void infino_forget_latest(Pointer h);

    /** For tests: open the connections from here on under a read staleness window of {@code ms} milliseconds (0 for the engine's default). */
    void infino_set_read_staleness_ms(long ms);

    /** The indices a caller's read-only statement reads, as {@code {"indices": [..]}}, or an error with its status. */
    Pointer infino_sql_indices(String sql);

    /** A caller's read-only statement run through the views {@code views} gives each index it reads (see {@link InfinoSql}). */
    Pointer infino_sql_read(Pointer h, String sql, String views);

    Pointer infino_bm25(Pointer h, String name, String column, String query, String mode, int k);

    Pointer infino_sorted(Pointer h, String name, String column, int descending, int k, int withSource);

    Pointer infino_count(Pointer h, String name, String column, String query, String mode);

    Pointer infino_count_where(Pointer h, String name, String predicatesJson);

    Pointer infino_value_counts(Pointer h, String name, String column, int maxRows);

    Pointer infino_token_match(Pointer h, String name, String column, String query);

    Pointer infino_knn(Pointer h, String name, String column, String embJson, int k);

    Pointer infino_hybrid(Pointer h, String name, String textColumn, String textQuery, String vectorColumn, String embJson, int k);

    void infino_free_string(Pointer s);
}
