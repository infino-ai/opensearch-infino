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
import org.opensearch.index.query.MatchNoneQueryBuilder;
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
 * for reads (null when it does not). {@code maskRuleOf} gives the masked-field
 * expression a masked field is masked by, as the role writes it
 * ({@code ip}, {@code ip::SHA-256}, {@code ip::/regex/::replacement}), and
 * {@code maskAlgorithm} the node's default hash algorithm for one that names
 * none (null when unset): what SQL needs to mask a value as {@code maskOf}
 * does (see {@link InfinoSql}); both null when nothing is masked.
 * {@code aliasFilter} is what the filtering aliases the request reached the
 * index through keep (null when it reads the index whole, see
 * {@link InfinoAliasFilter}): not a security rule, but a part of the search
 * the user is held to as to their own query.
 */
record InfinoAccess(
    List<QueryBuilder> documents,
    Predicate<String> fieldVisible,
    Function<String, UnaryOperator<String>> maskOf,
    BiConsumer<String, String> onRead,
    Function<String, String> maskRuleOf,
    String maskAlgorithm,
    QueryBuilder aliasFilter
) {

    private static final Logger log = LogManager.getLogger(InfinoAccess.class);

    /** No restriction: the request sees the index whole. */
    static final InfinoAccess NONE = new InfinoAccess(null, null, null, null);

    /** The security plugin's index when its {@code plugins.security.config_index_name} is unset. */
    static final String DEFAULT_SECURITY_INDEX = ".opendistro_security";

    /** A document rule that keeps no row. */
    private static final List<QueryBuilder> NO_ROW = List.of(new MatchNoneQueryBuilder());

    /** A read of the security plugin's own index: no row of it (see {@link #reading}). */
    static final InfinoAccess NO_ROWS = new InfinoAccess(NO_ROW, null, null);

    private static volatile String securityIndex = DEFAULT_SECURITY_INDEX;

    /** Whether the security plugin runs on this node, and so hides its own index from a read (see {@link #reading}). */
    private static volatile boolean securityRuns = true;

    /** The security plugin's own index on this node, from its {@code plugins.security.config_index_name}. */
    static void securityIndex(String name) {
        securityIndex = name == null || name.isEmpty() ? DEFAULT_SECURITY_INDEX : name;
    }

    /** Whether the security plugin runs on this node (see {@link InfinoSecurityCalls#runs}). */
    static void securityRuns(boolean runs) {
        securityRuns = runs;
    }

    /** What a search may read of {@code index} (see {@link #reading}). */
    static InfinoAccess searching(String index) {
        return reading(index, InfinoSecurityCalls.SEARCH);
    }

    /**
     * What a search, get or SQL read may read of {@code index}; {@code what}
     * names the request in a refusal. The security plugin's own index (its
     * users, password hashes and roles) reads as holding no row, as that
     * plugin shows it on a Lucene index: a search through its reader, and a
     * get too, which it turns non-realtime so the same reader answers it. The
     * table would otherwise hand the index to anyone whose role reads
     * {@code *}. As there, only while the security plugin runs, and not for an
     * admin certificate or one of its own configuration reads, which load the
     * configuration by id, from the table (see
     * {@link InfinoSecurityCalls#readsSecurityIndex}).
     */
    static InfinoAccess reading(String index, String what) {
        if (securityRuns && index.equals(securityIndex) && !InfinoSecurity.readsSecurityIndex()) {
            return NO_ROWS;
        }
        return InfinoSecurity.access(index, what);
    }

    /** Whether the request reads no row of the index (see {@link #reading}). */
    boolean readsNothing() {
        return documents == NO_ROW;
    }

    InfinoAccess(List<QueryBuilder> documents, Predicate<String> fieldVisible, Function<String, UnaryOperator<String>> maskOf) {
        this(documents, fieldVisible, maskOf, null);
    }

    InfinoAccess(
        List<QueryBuilder> documents,
        Predicate<String> fieldVisible,
        Function<String, UnaryOperator<String>> maskOf,
        BiConsumer<String, String> onRead
    ) {
        this(documents, fieldVisible, maskOf, onRead, null, null);
    }

    /** A security plugin's rules, read through no filtering alias. */
    InfinoAccess(
        List<QueryBuilder> documents,
        Predicate<String> fieldVisible,
        Function<String, UnaryOperator<String>> maskOf,
        BiConsumer<String, String> onRead,
        Function<String, String> maskRuleOf,
        String maskAlgorithm
    ) {
        this(documents, fieldVisible, maskOf, onRead, maskRuleOf, maskAlgorithm, null);
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
        return new InfinoAccess(documents, fieldVisible, maskOf, onRead, maskRuleOf, maskAlgorithm, narrowed);
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
     * The columns a plan may name: a hidden column is gone (the id, the
     * sequence number and the ignored fields stay, as metadata), a masked one
     * carries its hash. A
     * query, sort, aggregation or script naming a hidden field then finds no
     * column, as it would for an unmapped field. A field alias goes with
     * the field it names: hidden, masked or visible as that field is, and so
     * does the list column holding a field's several values. The column of
     * raw sources goes too: it holds every field, the hidden and masked ones
     * among them, so a query, aggregation or script over it would read them.
     * A hit's source is read by the planner itself and cut by
     * {@link #source}, never through a column the request names.
     */
    Map<String, Column> columns(Map<String, Column> cols) {
        if (!restrictsFields() && !masks()) {
            return cols;
        }
        Map<String, Column> out = new LinkedHashMap<>();
        for (Map.Entry<String, Column> entry : cols.entrySet()) {
            Column c = entry.getValue();
            String name = c.name();
            if (name.equals(InfinoQueryTranslator.SOURCE)) {
                continue;
            }
            boolean reserved = name.equals(InfinoQueryTranslator.OSID)
                || name.equals(InfinoQueryTranslator.SEQ)
                || name.equals(InfinoQueryTranslator.IGNORED);
            String field = InfinoQueryTranslator.fieldOf(name);
            if (!reserved && restrictsFields() && !fieldVisible.test(field)) {
                continue;
            }
            UnaryOperator<String> mask = reserved || !masks() ? null : maskOf.apply(field);
            out.put(entry.getKey(), mask == null ? c : c.masked(mask));
        }
        return Collections.unmodifiableMap(out);
    }

    /** The columns a script may read: the visible ones that are not masked, since a script would read the value itself. */
    static Map<String, Column> scriptColumns(Map<String, Column> cols) {
        Map<String, Column> out = new LinkedHashMap<>();
        for (Map.Entry<String, Column> entry : cols.entrySet()) {
            if (!entry.getValue().masked()) {
                out.put(entry.getKey(), entry.getValue());
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
