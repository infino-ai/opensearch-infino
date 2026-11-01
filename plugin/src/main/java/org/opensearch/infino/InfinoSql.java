/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.AliasMetadata;
import org.opensearch.cluster.metadata.IndexAbstraction;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.infino.InfinoNative.Column;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A caller's read-only SQL over Infino-backed indices, held to what the
 * security plugin lets the caller see.
 *
 * <p>A statement names indices, never tables. The bridge names the indices
 * it reads ({@link InfinoNative#sqlIndices}); the route sends them in the
 * request, so the security plugin authorizes the caller for each as for a
 * search ({@link InfinoSqlAction}); then each index is replaced, in the
 * parsed statement, by a view of its table: the columns the caller may see
 * (the document's source left out under a field rule, since it holds every
 * field) and the rows the document rule keeps. The engine reads only those
 * views, so no statement reaches a table it does not name as an index the
 * caller may read: not the registry, not the security plugin's own index,
 * not another index's table.
 *
 * <p>Refused rather than served around a rule: an index with masked fields
 * (SQL cannot give a value as its hash), one whose reads the audit log
 * records document by document, an engine search over an index under any
 * rule (it searches the whole table), a filtered alias, a system index, an
 * index Infino does not back. See the bridge's {@code read_sql} for what the
 * statement itself may hold.
 */
final class InfinoSql {

    /** The most rows a statement returns: OpenSearch's own default cap on a page of hits ({@code index.max_result_window}). */
    static final int MAX_ROWS = 10_000;
    /** What a refusal calls the request. */
    private static final String WHAT = "a SQL read";

    private InfinoSql() {}

    /** Run the request's statement as its caller may read it: the result as JSON. */
    static String run(InfinoSqlRequest request) {
        ClusterState state = InfinoServices.clusterService().state();
        List<String> named = InfinoNative.sqlIndices(request.query());
        // The indices the security plugin authorized, as concrete indices: the
        // statement may read no other, whatever a caller put in the request.
        Set<String> authorized = new HashSet<>();
        for (String name : request.indices()) {
            authorized.add(index(state, name).getIndex().getName());
        }
        long now = System.currentTimeMillis();
        Map<String, String> views = new LinkedHashMap<>();
        Map<String, String> searchable = new LinkedHashMap<>();
        for (String name : named) {
            IndexMetadata md = index(state, name);
            String index = md.getIndex().getName();
            if (!authorized.contains(index)) {
                throw InfinoAccess.refused("the statement reads [" + name + "], which the request was not authorized for");
            }
            InfinoAccess access = InfinoSecurity.access(index, WHAT);
            if (access.masks()) {
                throw InfinoAccess.refused("[" + name + "] has fields masked for you, and SQL cannot give a value as its hash");
            }
            if (access.onRead() != null) {
                throw InfinoAccess.refused("reads of [" + name + "] are audited document by document, which a SQL result cannot report");
            }
            String table = InfinoPlugin.tables().ensure(md);
            Map<String, Column> columns = InfinoNative.columns(table);
            if (columns.isEmpty()) {
                throw new IllegalStateException("[infino] table " + table + " has not been declared");
            }
            views.put(name, view(table, columns, access, now));
            if (access.unrestricted()) {
                searchable.put(name, table);
            }
        }
        return InfinoNative.sqlRead(request.query(), viewsJson(views, searchable), MAX_ROWS);
    }

    /**
     * The index a statement names: an Infino-backed index, or an alias to one
     * that filters nothing. Anything else is refused: a missing name, an
     * alias to several indices or with a filter (the view would have to carry
     * it), a data stream, a system index.
     */
    static IndexMetadata index(ClusterState state, String name) {
        IndexAbstraction found = state.metadata().getIndicesLookup().get(name);
        if (found == null) {
            throw new IndexNotFoundException(name);
        }
        if (found.getType() == IndexAbstraction.Type.DATA_STREAM || found.getIndices().size() != 1) {
            throw new IllegalArgumentException("[infino] [" + name + "] is not one index; name one index or an alias to one");
        }
        IndexMetadata md = found.getIndices().get(0);
        if (found.getType() == IndexAbstraction.Type.ALIAS) {
            AliasMetadata alias = md.getAliases().get(name);
            if (alias != null && alias.filteringRequired()) {
                throw new IllegalArgumentException("[infino] [" + name + "] is a filtered alias, which SQL does not read; name the index");
            }
        }
        if (md.isSystem() || md.getIndex().getName().startsWith(".")) {
            throw InfinoAccess.refused("[" + name + "] is a system index, which SQL does not read");
        }
        if (!md.getSettings().getAsBoolean(InfinoPlugin.INFINO_ENABLED.getKey(), false)) {
            throw new IllegalArgumentException("[infino] [" + name + "] is not backed by Infino; SQL reads Infino-backed indices only");
        }
        return md;
    }

    /**
     * The view of {@code table} the caller reads: every column when no field
     * rule applies, else the visible ones without the source, which holds
     * every field; the rows the document rule keeps.
     */
    static String view(String table, Map<String, Column> columns, InfinoAccess access, long nowMillis) {
        String select = "*";
        if (access.restrictsFields()) {
            Set<String> visible = new LinkedHashSet<>();
            for (Column c : access.columns(columns).values()) {
                if (!c.name().equals(InfinoQueryTranslator.SOURCE)) {
                    visible.add(InfinoQueryTranslator.quoteIdent(c.name()));
                }
            }
            select = String.join(", ", visible);
        }
        String where = access.documentsWhere(columns, table, nowMillis);
        return "SELECT " + select + " FROM " + InfinoQueryTranslator.quoteIdent(table) + (where == null ? "" : " WHERE " + where);
    }

    /** The views and searchable tables as the bridge takes them. */
    private static String viewsJson(Map<String, String> views, Map<String, String> searchable) {
        try (XContentBuilder b = XContentFactory.jsonBuilder()) {
            b.startObject();
            b.field("relations", views);
            b.field("functions", searchable);
            b.endObject();
            return BytesReference.bytes(b).utf8ToString();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
