/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import org.opensearch.action.support.IndicesOptions;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.AliasMetadata;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.index.query.AbstractQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.infino.InfinoNative.Column;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.UnaryOperator;

/**
 * A caller's read-only SQL over Infino-backed indices, held to what the
 * security plugin lets the caller see.
 *
 * <p>A statement names indices (an index, an alias, a pattern in quotes),
 * never tables. The bridge names the expressions it reads
 * ({@link InfinoNative#sqlIndices}); the route sends them in the request, so
 * the security plugin authorizes the caller for each as for a search
 * ({@link InfinoSqlAction}); then each expression is replaced, in the parsed
 * statement, by a view: for each Infino-backed index it resolves to that the
 * caller may read, its table's columns as the caller sees them (a hidden
 * field gone, a masked one as its mask, the source left out under either
 * rule, since it holds every field as written) and the rows the document
 * rule and any filtering alias keep, the indices' rows one row set, column
 * for column ({@link #view}). The
 * engine reads only those views, so no statement reaches a table it does not
 * name as an index the caller may read: not the registry, not the security
 * plugin's own index, not another index's table.
 *
 * <p>A masked field is masked in SQL as the security plugin masks it where
 * SQL has the same function: a regular-expression mask as its replacements,
 * a named algorithm (MD5, the SHA-2 family) as that digest in hex, so the
 * statement filters, groups and joins on the mask. A mask SQL has no
 * function for (the security plugin's default, a BLAKE2b keyed with the
 * cluster's salt, or another named algorithm) is applied to the result by
 * the security plugin's own function, as a search's source is masked; the
 * field is then read only as a column of the result, and a statement using
 * it otherwise is refused (see the bridge's {@code output_columns}).
 *
 * <p>An engine search over an index reads it the same way: the search over
 * each table the index resolves to, its columns and rows as the view's (see
 * {@link #view}). A search naming a column hidden or masked for the caller
 * is refused, since it matches the stored values.
 *
 * <p>Also refused: an index whose reads the audit log records document by
 * document, a system index, an index Infino does not back.
 */
final class InfinoSql {

    /** The most rows a statement returns: OpenSearch's own default cap on a page of hits ({@code index.max_result_window}). */
    static final int MAX_ROWS = 10_000;
    /**
     * How a name in a statement resolves, as a search resolves its indices:
     * a pattern to the open indices it matches (not the hidden ones), an
     * alias to every index it points at, a name that is none of these
     * refused.
     */
    static final IndicesOptions OPTIONS = IndicesOptions.strictExpandOpenAndForbidClosed();
    /** What a refusal calls the request. */
    private static final String WHAT = "a SQL read";
    /** Joins the parts of the indices one name resolves to, row set after row set. */
    private static final String UNION = " UNION ALL ";
    /**
     * The table function a search view reads, with the table as its one
     * argument, where the bridge puts the statement's own search (the
     * bridge's {@code SEARCH_PLACEHOLDER}).
     */
    static final String SEARCH_PLACEHOLDER = "infino_search";
    /** The column an engine search ranks its rows by, beside the table's own. */
    private static final String SCORE = "score";
    /**
     * The keys of the bridge's answer: its columns (each a name and an Arrow
     * type), its rows, and the result columns it names to mask.
     */
    private static final String SCHEMA = "schema";
    private static final String NAME = "name";
    private static final String TYPE = "type";
    private static final String ROWS = "rows";
    private static final String MASKED = "masked";
    /** The Arrow type of a masked column in the answer: its mask is text, whatever the field's type. */
    private static final String TEXT_TYPE = "Utf8";
    /** What separates the parts of a masked-field expression, as the security plugin splits it. */
    private static final String MASK_SEPARATOR = "::";
    /** What a regular expression in a masked-field expression is written between. */
    private static final String REGEX_DELIMITER = "/";
    /** The security plugin's name for its earlier default hash, which a node may set as the default. */
    private static final String LEGACY_DEFAULT_ALGORITHM = "BLAKE2B_LEGACY_DEFAULT";
    /** The digests SQL gives as the security plugin's named algorithms do, by the algorithm's name without its dashes. */
    private static final Map<String, String> DIGESTS = Map.of(
        "MD5",
        "md5",
        "SHA224",
        "sha224",
        "SHA256",
        "sha256",
        "SHA384",
        "sha384",
        "SHA512",
        "sha512"
    );

    private InfinoSql() {}

    /** Run the request's statement as its caller may read it: the result as JSON. */
    static String run(InfinoSqlRequest request) {
        ClusterState state = InfinoServices.clusterService().state();
        IndexNameExpressionResolver resolver = InfinoServices.indexNameExpressionResolver();
        List<String> named = InfinoNative.sqlIndices(request.query());
        // The indices the security plugin authorized, as concrete indices: the
        // statement reads no other, whatever a caller put in the request.
        Set<String> authorized = new LinkedHashSet<>();
        for (String expression : request.indices()) {
            authorized.addAll(Arrays.asList(resolver.concreteIndexNames(state, OPTIONS, expression)));
        }
        long now = System.currentTimeMillis();
        Map<String, String> views = new LinkedHashMap<>();
        Map<String, String> searches = new LinkedHashMap<>();
        Map<String, Set<String>> unsearchable = new LinkedHashMap<>();
        // The fields masked in the result, each with the security plugin's mask
        // (one rule per field, as the result does not say which index a value
        // came from), and those masked in SQL: one field is not masked both ways.
        Map<String, UnaryOperator<String>> onOutput = new LinkedHashMap<>();
        Map<String, String> outputRules = new LinkedHashMap<>();
        Set<String> inSql = new LinkedHashSet<>();
        for (String name : named) {
            List<Part> parts = new ArrayList<>();
            String[] concrete = resolver.concreteIndexNames(state, OPTIONS, name);
            for (String index : concrete) {
                if (!authorized.contains(index)) {
                    continue; // as a search over a pattern reads only the indices it may
                }
                IndexMetadata md = infinoIndex(state, name, index);
                InfinoAccess access = InfinoSecurity.access(index, WHAT);
                if (access.onRead() != null) {
                    throw InfinoAccess.refused("reads of [" + index + "] are audited document by document, which a SQL result cannot report");
                }
                String table = InfinoPlugin.tables().ensure(md);
                Map<String, Column> columns = InfinoNative.columns(table);
                if (columns.isEmpty()) {
                    throw new IllegalStateException("[infino] table " + table + " has not been declared");
                }
                String aliasFilter = aliasFilter(state, resolver, md, name, columns, table, now);
                Part part = part(table, columns, access, aliasFilter, now);
                parts.add(part);
                for (Map.Entry<String, UnaryOperator<String>> e : part.onOutput().entrySet()) {
                    String rule = access.maskRuleOf().apply(e.getKey());
                    if (!rule.equals(outputRules.getOrDefault(e.getKey(), rule))) {
                        throw InfinoAccess.refused("[" + e.getKey() + "] is masked for you by different rules in the indices the statement reads");
                    }
                    outputRules.put(e.getKey(), rule);
                    onOutput.putIfAbsent(e.getKey(), e.getValue());
                }
                part.masked().stream().filter(c -> !part.onOutput().containsKey(c)).forEach(inSql::add);
            }
            if (parts.isEmpty()) {
                throw InfinoAccess.refused("the statement reads [" + name + "], and no index it names was authorized for the request");
            }
            views.put(name, view(parts, false));
            searches.put(name, view(parts, true));
            Set<String> closed = new LinkedHashSet<>();
            parts.forEach(p -> closed.addAll(p.unsearchable()));
            if (!closed.isEmpty()) {
                unsearchable.put(name, closed);
            }
        }
        for (String field : onOutput.keySet()) {
            if (inSql.contains(field)) {
                throw InfinoAccess.refused("[" + field + "] is masked for you by rules SQL reads two ways in the indices the statement reads");
            }
        }
        String answer = InfinoNative.sqlRead(request.query(), readsJson(views, searches, unsearchable, onOutput.keySet()), MAX_ROWS);
        return onOutput.isEmpty() ? answer : maskResult(answer, onOutput);
    }

    /**
     * The bridge's answer with each result column it names under
     * {@code masked} masked by its field's mask, as the security plugin masks
     * a search's source: each value as text through the mask, null left null,
     * the column typed as text.
     */
    @SuppressWarnings("unchecked")
    static String maskResult(String answer, Map<String, UnaryOperator<String>> masks) {
        Map<String, Object> result;
        try (XContentParser parser = JsonXContent.jsonXContent.createParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, answer)) {
            result = parser.mapOrdered();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        Object given = result.remove(MASKED);
        if (given instanceof Map<?, ?> columns && result.get(ROWS) instanceof List<?> rows) {
            if (result.get(SCHEMA) instanceof List<?> schema) {
                for (Object field : schema) {
                    Map<String, Object> f = (Map<String, Object>) field;
                    if (columns.containsKey(f.get(NAME))) {
                        f.put(TYPE, TEXT_TYPE);
                    }
                }
            }
            for (Object row : rows) {
                Map<String, Object> values = (Map<String, Object>) row;
                for (Map.Entry<?, ?> column : columns.entrySet()) {
                    UnaryOperator<String> mask = masks.get(String.valueOf(column.getValue()));
                    if (mask == null) {
                        throw InfinoAccess.refused("[" + column.getValue() + "] is read only as a masked column, and no mask was found for it");
                    }
                    String name = String.valueOf(column.getKey());
                    Object value = values.get(name);
                    if (value != null) {
                        values.put(name, mask.apply(String.valueOf(value)));
                    }
                }
            }
        }
        try (XContentBuilder b = XContentFactory.jsonBuilder()) {
            b.map(result);
            return BytesReference.bytes(b).utf8ToString();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** {@code index}, which {@code name} resolved to, when SQL reads it: Infino-backed, and not a system index. */
    private static IndexMetadata infinoIndex(ClusterState state, String name, String index) {
        IndexMetadata md = state.metadata().index(index);
        if (md == null) {
            throw new IndexNotFoundException(index);
        }
        if (md.isSystem() || index.startsWith(".")) {
            throw InfinoAccess.refused("[" + index + "] is a system index, which SQL does not read");
        }
        if (!md.getSettings().getAsBoolean(InfinoPlugin.INFINO_ENABLED.getKey(), false)) {
            throw new IllegalArgumentException("[infino] [" + index + "] (from [" + name + "]) is not backed by Infino; SQL reads Infino-backed indices only");
        }
        return md;
    }

    /**
     * The rows the aliases {@code name} reached {@code md} through keep, as a
     * search through them keeps them (any one alias's filter); null when the
     * index is read whole, as when it is named itself.
     */
    private static String aliasFilter(
        ClusterState state,
        IndexNameExpressionResolver resolver,
        IndexMetadata md,
        String name,
        Map<String, Column> columns,
        String table,
        long now
    ) {
        String index = md.getIndex().getName();
        Set<String> resolved = resolver.resolveExpressions(state, name);
        String[] filtering = resolver.filteringAliases(state, index, resolved);
        if (filtering == null || filtering.length == 0) {
            return null;
        }
        List<String> any = new ArrayList<>();
        for (String alias : filtering) {
            AliasMetadata am = md.getAliases().get(alias);
            if (am == null || am.filter() == null) {
                return null; // one alias that filters nothing reads the index whole
            }
            QueryBuilder filter = parse(am.filter().string());
            String p = InfinoQueryTranslator.rulePredicate(filter, columns, table, now);
            if (p == null) {
                throw new IllegalArgumentException("[infino] the filter of alias [" + alias + "] is not served by the table");
            }
            any.add(p);
        }
        return any.size() == 1 ? any.get(0) : "(" + String.join(" OR ", any) + ")";
    }

    private static QueryBuilder parse(String json) {
        try (XContentParser parser = JsonXContent.jsonXContent.createParser(InfinoServices.namedXContentRegistry(), LoggingDeprecationHandler.INSTANCE, json)) {
            return AbstractQueryBuilder.parseInnerQueryBuilder(parser);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * One index's part of a view: the expression each column the caller
     * reads is given by, by column name ({@code whole} when that is every
     * column as stored), and the rows it keeps (null for all); the masked
     * columns, those of them masked in the result rather than in SQL (given
     * as stored, with the mask), and the columns a search may not name (the
     * masked ones and the hidden ones).
     */
    record Part(
        String table,
        Map<String, String> columns,
        boolean whole,
        String where,
        Set<String> masked,
        Map<String, UnaryOperator<String>> onOutput,
        Set<String> unsearchable
    ) {
        Part(String table, Map<String, String> columns, boolean whole, String where) {
            this(table, columns, whole, where, Set.of(), Map.of(), Set.of());
        }
    }

    /**
     * The part of {@code table} the caller reads: every column when no field
     * or masking rule applies; else the visible ones, each masked one as its
     * mask (or as stored, to be masked in the result, where SQL has no
     * function for the mask), without the source; the rows the document rule
     * and {@code aliasFilter} keep.
     */
    static Part part(String table, Map<String, Column> columns, InfinoAccess access, String aliasFilter, long nowMillis) {
        boolean whole = !access.restrictsFields() && !access.masks();
        Map<String, Column> visible = whole ? columns : access.columns(columns);
        Map<String, String> projected = new LinkedHashMap<>();
        Set<String> masked = new LinkedHashSet<>();
        Map<String, UnaryOperator<String>> onOutput = new LinkedHashMap<>();
        Set<String> unsearchable = new LinkedHashSet<>();
        for (Column c : columns.values()) {
            if (!visible.containsKey(c.name())) {
                unsearchable.add(c.name());
            }
        }
        for (Column c : visible.values()) {
            String quoted = InfinoQueryTranslator.quoteIdent(c.name());
            if (projected.containsKey(c.name()) || (!whole && c.name().equals(InfinoQueryTranslator.SOURCE))) {
                continue;
            }
            if (!c.masked()) {
                projected.put(c.name(), quoted);
                continue;
            }
            masked.add(c.name());
            unsearchable.add(c.name());
            String rule = access.maskRuleOf() == null ? null : access.maskRuleOf().apply(c.name());
            if (rule == null) {
                throw InfinoAccess.refused("[" + c.name() + "] is masked for you by a rule SQL cannot read");
            }
            String expr = masked(quoted, rule, access.maskAlgorithm());
            if (expr != null) {
                projected.put(c.name(), expr);
                continue;
            }
            onOutput.put(c.name(), c.mask());
            projected.put(c.name(), quoted);
        }
        String where = access.documentsWhere(columns, table, nowMillis);
        if (aliasFilter != null) {
            where = where == null ? aliasFilter : "(" + where + ") AND (" + aliasFilter + ")";
        }
        return new Part(table, projected, whole, where, masked, onOutput, unsearchable);
    }

    /**
     * The view the parts make: one part as it is; several as one row set, each
     * part giving every column any of them has, in one order, a column it
     * lacks as null. A column read as stored carries its table's Parquet
     * field id, and the engine refuses a union whose inputs' ids differ (as
     * two indices' tables do), so in a union each column is read through
     * {@code coalesce(.., NULL)}, which is the value without the id.
     *
     * <p>With {@code search}, the view of an engine search over the same
     * indices: each part reads {@link #SEARCH_PLACEHOLDER} over its table in
     * place of the table, which the bridge makes the statement's own search,
     * and gives the search's score beside its columns.
     */
    static String view(List<Part> parts, boolean search) {
        String score = InfinoQueryTranslator.quoteIdent(SCORE);
        if (parts.size() == 1) {
            Part p = parts.get(0);
            String list = p.whole() ? "*" : String.join(", ", aliased(p.columns()).values()) + (search ? ", " + score : "");
            return select(p, list, search);
        }
        Set<String> names = new LinkedHashSet<>();
        for (Part p : parts) {
            names.addAll(p.columns().keySet());
        }
        if (search) {
            names.add(SCORE);
        }
        List<String> selects = new ArrayList<>();
        for (Part p : parts) {
            List<String> list = new ArrayList<>(names.size());
            for (String name : names) {
                String expr = search && name.equals(SCORE) ? score : p.columns().get(name);
                String quoted = InfinoQueryTranslator.quoteIdent(name);
                list.add((expr == null ? "NULL" : "coalesce(" + expr + ", NULL)") + " AS " + quoted);
            }
            selects.add(select(p, String.join(", ", list), search));
        }
        return String.join(UNION, selects);
    }

    /** Each column's expression under the column's own name. */
    private static Map<String, String> aliased(Map<String, String> columns) {
        Map<String, String> out = new LinkedHashMap<>();
        columns.forEach((name, expr) -> {
            String quoted = InfinoQueryTranslator.quoteIdent(name);
            out.put(name, expr.equals(quoted) ? quoted : expr + " AS " + quoted);
        });
        return out;
    }

    private static String select(Part p, String list, boolean search) {
        String from = search
            ? SEARCH_PLACEHOLDER + "(" + InfinoQueryTranslator.literal(p.table()) + ")"
            : InfinoQueryTranslator.quoteIdent(p.table());
        return "SELECT " + list + " FROM " + from + (p.where() == null ? "" : " WHERE " + p.where());
    }

    /**
     * {@code column} masked as the security plugin masks it by {@code rule}
     * (its masked-field expression), in SQL: a regular-expression mask as its
     * replacements in order; a hash as the digest its algorithm names, or
     * {@code defaultAlgorithm} when it names none, in lower-case hex. Null
     * for a rule SQL has no equal for, which is masked in the result: the
     * default salted BLAKE2b, and an algorithm outside MD5 and the SHA-2
     * family. A rule the security plugin would not read is refused.
     */
    static String masked(String column, String rule, String defaultAlgorithm) {
        List<String> parts = Arrays.asList(rule.split(MASK_SEPARATOR, -1));
        String text = "CAST(" + column + " AS VARCHAR)";
        if (parts.size() >= 3 && parts.size() % 2 == 1) {
            String out = text;
            for (int i = 1; i < parts.size(); i += 2) {
                String regex = parts.get(i);
                if (regex.length() < 2 || !regex.startsWith(REGEX_DELIMITER) || !regex.endsWith(REGEX_DELIMITER)) {
                    throw InfinoAccess.refused("the masking rule [" + rule + "] is not one the security plugin reads");
                }
                String pattern = regex.substring(1, regex.length() - 1);
                out = "regexp_replace(" + out + ", " + InfinoQueryTranslator.literal(pattern) + ", " + InfinoQueryTranslator.literal(
                    replacement(parts.get(i + 1), rule)
                ) + ", 'g')";
            }
            return out;
        }
        if (parts.size() > 2) {
            throw InfinoAccess.refused("the masking rule [" + rule + "] is not one the security plugin reads");
        }
        String algorithm = parts.size() == 2 ? parts.get(1) : defaultAlgorithm;
        if (algorithm == null || algorithm.isEmpty() || algorithm.equalsIgnoreCase(LEGACY_DEFAULT_ALGORITHM)) {
            return null; // the default, a BLAKE2b keyed with the cluster's salt
        }
        String digest = DIGESTS.get(algorithm.replace("-", "").toUpperCase(Locale.ROOT));
        return digest == null ? null : "encode(digest(" + text + ", '" + digest + "'), 'hex')";
    }

    /**
     * A Java regular-expression replacement as the engine's regular
     * expressions take it: a group as {@code ${n}}, an escaped character as
     * itself ({@code $} doubled).
     */
    static String replacement(String java, String rule) {
        StringBuilder out = new StringBuilder();
        for (int i = 0; i < java.length(); i++) {
            char c = java.charAt(i);
            if (c == '\\' && i + 1 < java.length()) {
                char next = java.charAt(++i);
                out.append(next == '$' ? "$$" : String.valueOf(next));
            } else if (c == '$') {
                int end = i + 1;
                if (end < java.length() && java.charAt(end) == '{') {
                    int close = java.indexOf('}', end);
                    if (close < 0) {
                        throw InfinoAccess.refused("the masking rule [" + rule + "] is not one the security plugin reads");
                    }
                    out.append(java, i, close + 1);
                    i = close;
                    continue;
                }
                while (end < java.length() && Character.isDigit(java.charAt(end))) {
                    end++;
                }
                if (end == i + 1) {
                    throw InfinoAccess.refused("the masking rule [" + rule + "] is not one the security plugin reads");
                }
                out.append("${").append(java, i + 1, end).append('}');
                i = end - 1;
            } else {
                out.append(c);
            }
        }
        return out.toString();
    }

    /** What the statement may read, as the bridge takes it (its {@code Reads}). */
    private static String readsJson(
        Map<String, String> views,
        Map<String, String> searches,
        Map<String, Set<String>> unsearchable,
        Set<String> outputOnly
    ) {
        try (XContentBuilder b = XContentFactory.jsonBuilder()) {
            b.startObject();
            b.field("relations", views);
            b.field("searches", searches);
            b.field("unsearchable", unsearchable);
            b.field("output_only", outputOnly);
            b.endObject();
            return BytesReference.bytes(b).utf8ToString();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
