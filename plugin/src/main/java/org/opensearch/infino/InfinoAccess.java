/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.OpenSearchSecurityException;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.infino.InfinoNative.Column;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;

/**
 * What the security plugin lets one request see of one index, as the
 * table enforces it: {@code documents} are the document-level rule's
 * queries, any one of which a row must match (null when the rule is absent;
 * a match-none query when it keeps nothing); {@code fieldVisible} says
 * which fields the request may name or receive (null when all);
 * {@code maskOf} gives the hash a masked field's values leave with (null
 * when none is masked). The three come from the security plugin's own
 * evaluation of the user's roles, so the semantics are its: a hidden field
 * is as if unmapped, a masked field is read and aggregated by its hash, and
 * the document rule narrows hits, totals, aggregations and gets alike.
 * {@code onRead} is told of every document the request is handed, by id
 * and as shown, where the security plugin's audit log watches the index
 * for reads (null when it does not). {@code aliasFilter} is what the
 * filtering aliases the request reached the index through keep (null when
 * it reads the index whole, see {@link InfinoAliasFilter}): not a security
 * rule, but a part of the search the user is held to as to their own query.
 */
record InfinoAccess(
    List<QueryBuilder> documents,
    Predicate<String> fieldVisible,
    Function<String, UnaryOperator<String>> maskOf,
    BiConsumer<String, String> onRead,
    QueryBuilder aliasFilter
) {

    private static final Logger log = LogManager.getLogger(InfinoAccess.class);

    /** No restriction: the request sees the index whole. */
    static final InfinoAccess NONE = new InfinoAccess(null, null, null, null, null);

    InfinoAccess(List<QueryBuilder> documents, Predicate<String> fieldVisible, Function<String, UnaryOperator<String>> maskOf) {
        this(documents, fieldVisible, maskOf, null, null);
    }

    InfinoAccess(
        List<QueryBuilder> documents,
        Predicate<String> fieldVisible,
        Function<String, UnaryOperator<String>> maskOf,
        BiConsumer<String, String> onRead
    ) {
        this(documents, fieldVisible, maskOf, onRead, null);
    }

    /** Whether the request reads only some of the index's rows: under a document rule, or through a filtering alias. */
    boolean restrictsDocuments() {
        return documents != null || aliasFilter != null;
    }

    boolean restrictsFields() {
        return fieldVisible != null;
    }

    boolean masks() {
        return maskOf != null;
    }

    boolean unrestricted() {
        return !restrictsDocuments() && !restrictsFields() && !masks();
    }

    /**
     * This access with its rows narrowed to what {@code filter} keeps too, as
     * a filtering alias narrows a search: a row is kept when the document
     * rule keeps it and so does the filter. Null keeps this access as it is.
     */
    InfinoAccess filteredBy(QueryBuilder filter) {
        if (filter == null) {
            return this;
        }
        QueryBuilder narrowed = aliasFilter == null ? filter : QueryBuilders.boolQuery().filter(aliasFilter).filter(filter);
        return new InfinoAccess(documents, fieldVisible, maskOf, onRead, narrowed);
    }

    /**
     * The document {@code id} as this access shows it (see {@link #source}),
     * the read reported to the audit log when it watches the index.
     */
    String read(String id, String source) {
        String shown = source(source);
        if (onRead != null && shown != null && !shown.isEmpty()) {
            // Reporting a read never fails it.
            try {
                onRead.accept(id, shown);
            } catch (RuntimeException e) {
                log.warn("[infino] the read of {} could not be reported: {}", id, e.toString());
            }
        }
        return shown;
    }

    /** A document's fields by dotted path, each value as text: what the audit log records of a read. */
    static Map<String, String> fields(String source) {
        Map<String, String> out = new LinkedHashMap<>();
        Map<String, Object> map = XContentHelper.convertToMap(new BytesArray(source), true, MediaTypeRegistry.JSON).v2();
        flatten("", map, out);
        return out;
    }

    private static void flatten(String prefix, Map<String, Object> map, Map<String, String> out) {
        for (Map.Entry<String, Object> e : map.entrySet()) {
            String path = prefix.isEmpty() ? e.getKey() : prefix + "." + e.getKey();
            if (e.getValue() instanceof Map<?, ?> nested) {
                @SuppressWarnings("unchecked")
                Map<String, Object> inner = (Map<String, Object>) nested;
                flatten(path, inner, out);
            } else {
                out.put(path, String.valueOf(e.getValue()));
            }
        }
    }

    /** A request refused under a restriction the table cannot enforce as it stands, named. */
    static OpenSearchSecurityException refused(String what) {
        return new OpenSearchSecurityException("[infino] " + what, RestStatus.FORBIDDEN);
    }

    /**
     * The columns a plan may name: a hidden column is gone (the id and the
     * source stay, as metadata), a masked one carries its hash. A query,
     * sort, aggregation or script naming a hidden field then finds no
     * column, as it would for an unmapped field.
     */
    Map<String, Column> columns(Map<String, Column> cols) {
        if (!restrictsFields() && !masks()) {
            return cols;
        }
        Map<String, Column> out = new LinkedHashMap<>();
        for (Column c : cols.values()) {
            boolean reserved = c.name().equals(InfinoQueryTranslator.OSID) || c.name().equals(InfinoQueryTranslator.SOURCE);
            if (!reserved && restrictsFields() && !fieldVisible.test(c.name())) {
                continue;
            }
            UnaryOperator<String> mask = reserved || !masks() ? null : maskOf.apply(c.name());
            out.put(c.name(), mask == null ? c : c.masked(mask));
        }
        return Collections.unmodifiableMap(out);
    }

    /** The columns a script may read: the visible ones that are not masked, since a script would read the value itself. */
    static Map<String, Column> scriptColumns(Map<String, Column> cols) {
        Map<String, Column> out = new LinkedHashMap<>();
        for (Column c : cols.values()) {
            if (!c.masked()) {
                out.put(c.name(), c);
            }
        }
        return out.size() == cols.size() ? cols : Collections.unmodifiableMap(out);
    }

    /**
     * The predicate the document rule and the alias filter add to a plan;
     * null when there is neither. The rule is read over the table's full
     * columns (it may name fields the request may not), and a rule the table
     * cannot answer refuses the request. The alias filter is read as the
     * user's own query is, over the columns the user may name: a field
     * hidden from the user is as if unmapped there and matches nothing, as
     * on a stock index, where the filter runs over the user's reader.
     */
    String documentsWhere(Map<String, Column> fullCols, String table, long nowMillis) {
        String rules = rulesWhere(fullCols, table, nowMillis);
        if (aliasFilter == null) {
            return rules;
        }
        String alias = InfinoQueryTranslator.rulePredicate(aliasFilter, columns(fullCols), table, nowMillis);
        if (alias == null) {
            throw new IllegalArgumentException(
                "[infino] a filtering alias the search reads through uses a query an Infino-backed index does not serve in an alias filter: "
                    + aliasFilter
            );
        }
        return rules == null ? alias : "(" + rules + ") AND (" + alias + ")";
    }

    /** The predicate the document rule adds to a plan (see {@link #documentsWhere}); null when there is no rule. */
    private String rulesWhere(Map<String, Column> fullCols, String table, long nowMillis) {
        if (documents == null) {
            return null;
        }
        List<String> any = new ArrayList<>();
        for (QueryBuilder qb : documents) {
            String p = InfinoQueryTranslator.rulePredicate(qb, fullCols, table, nowMillis);
            if (p == null) {
                throw refused("the document-level security rule " + qb + " is not served by the table; the request is refused rather than served without it");
            }
            any.add(p);
        }
        if (any.isEmpty()) {
            return InfinoQueryTranslator.NONE;
        }
        return any.size() == 1 ? any.get(0) : "(" + String.join(" OR ", any) + ")";
    }

    /**
     * A document's source as this access shows it: hidden fields removed,
     * masked values replaced by their hash. The string itself when nothing
     * applies, so the unrestricted path copies nothing.
     */
    String source(String src) {
        if (src == null || src.isEmpty() || (!restrictsFields() && !masks())) {
            return src;
        }
        Map<String, Object> map = XContentHelper.convertToMap(new BytesArray(src), true, MediaTypeRegistry.JSON).v2();
        try (XContentBuilder b = XContentFactory.jsonBuilder()) {
            b.map(object("", map));
            return BytesReference.bytes(b).utf8ToString();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * An object at {@code prefix} with the rules applied: a leaf stays when
     * its path is visible, an object stays whole when its path is, else
     * with whatever of it is visible, and nothing of it when that is empty.
     */
    private Map<String, Object> object(String prefix, Map<String, Object> map) {
        Map<String, Object> out = new LinkedHashMap<>();
        for (Map.Entry<String, Object> e : map.entrySet()) {
            String path = prefix.isEmpty() ? e.getKey() : prefix + "." + e.getKey();
            Object kept = value(path, e.getValue());
            if (kept != null) {
                out.put(e.getKey(), kept);
            }
        }
        return out;
    }

    /** A value at {@code path} with the rules applied, or null when nothing of it is visible. */
    @SuppressWarnings("unchecked")
    private Object value(String path, Object v) {
        boolean visible = !restrictsFields() || fieldVisible.test(path);
        if (v instanceof Map<?, ?> nested) {
            Map<String, Object> inner = object(path, (Map<String, Object>) nested);
            return visible || !inner.isEmpty() ? inner : null;
        }
        if (v instanceof List<?> list) {
            List<Object> out = new ArrayList<>(list.size());
            for (Object item : list) {
                Object kept = value(path, item);
                if (kept != null) {
                    out.add(kept);
                }
            }
            return visible || !out.isEmpty() ? out : null;
        }
        if (!visible) {
            return null;
        }
        UnaryOperator<String> mask = masks() ? maskOf.apply(path) : null;
        return mask == null || v == null ? v : mask.apply(String.valueOf(v));
    }
}
