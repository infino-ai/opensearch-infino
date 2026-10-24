/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import com.sun.jna.Library;
import com.sun.jna.Pointer;

/** JNA mapping of the infino-capi C ABI (libinfino_capi.so / .dylib). */
public interface InfinoLib extends Library {
    Pointer infino_open(String uri);

    void infino_close(Pointer h);

    Pointer infino_ensure_table(Pointer h, String name, String mappingJson);

    Pointer infino_columns(Pointer h, String name);

    Pointer infino_check_doc(Pointer h, String name, String source);

    Pointer infino_index_batch(Pointer h, String name, String[] osids, String[] sources, int count);

    /** One buffer of ids and sources with their end offsets: three native arrays instead of one allocation per string. */
    Pointer infino_index_batch_buf(Pointer h, String name, byte[] buf, long bufLen, int[] idEnds, int[] srcEnds, int count);

    int infino_delete_osids(Pointer h, String name, String[] osids, int count);

    Pointer infino_get(Pointer h, String name, String osid);

    int infino_drop(Pointer h, String name);

    int infino_optimize(Pointer h, String name);

    Pointer infino_sql(Pointer h, String sql);

    Pointer infino_bm25(Pointer h, String name, String column, String query, int k);

    Pointer infino_count(Pointer h, String name, String column, String query, String mode);

    Pointer infino_token_match(Pointer h, String name, String column, String query);

    Pointer infino_knn(Pointer h, String name, String column, String embJson, int k);

    Pointer infino_hybrid(Pointer h, String name, String textColumn, String textQuery, String vectorColumn, String embJson, int k);

    void infino_free_string(Pointer s);
}
