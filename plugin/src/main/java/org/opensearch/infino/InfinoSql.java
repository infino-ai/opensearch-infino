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
 * <p>A masked field is masked in SQL as the security plugin masks it: a
 * regular-expression mask as its replacements, a named algorithm (MD5, the
 * SHA-2 family) as that digest in hex. The security plugin's default hash, a
 * BLAKE2b keyed with the cluster's salt, has no SQL function to give it, so
 * a field masked by it is refused rather than served unmasked.
 *
 * <p>Also refused: an index whose reads the audit log records document by
 * document, an engine search over anything but one index under no rule (it
 * searches the whole table), a system index, an index Infino does not back.
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
        Map<String, String> searchable = new LinkedHashMap<>();
        for (String name : named) {
            List<Part> parts = new ArrayList<>();
            boolean restricted = false;
            String onlyTable = null;
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
                parts.add(part(table, columns, access, aliasFilter, now));
                restricted |= !access.unrestricted() || aliasFilter != null;
                onlyTable = table;
            }
            if (parts.isEmpty()) {
                throw InfinoAccess.refused("the statement reads [" + name + "], and no index it names was authorized for the request");
            }
            views.put(name, view(parts));
            if (parts.size() == 1 && !restricted) {
                searchable.put(name, onlyTable);
            }
        }
        return InfinoNative.sqlRead(request.query(), viewsJson(views, searchable), MAX_ROWS);
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
     * column as stored), and the rows it keeps (null for all).
     */
    record Part(String table, Map<String, String> columns, boolean whole, String where) {}

    /**
     * The part of {@code table} the caller reads: every column when no field
     * or masking rule applies; else the visible ones, each masked one as its
     * mask, without the source; the rows the document rule and
     * {@code aliasFilter} keep.
     */
    static Part part(String table, Map<String, Column> columns, InfinoAccess access, String aliasFilter, long nowMillis) {
        boolean whole = !access.restrictsFields() && !access.masks();
        Map<String, String> projected = new LinkedHashMap<>();
        for (Column c : (whole ? columns : access.columns(columns)).values()) {
            String quoted = InfinoQueryTranslator.quoteIdent(c.name());
            if (projected.containsKey(c.name()) || (!whole && c.name().equals(InfinoQueryTranslator.SOURCE))) {
                continue;
            }
            if (!c.masked()) {
                projected.put(c.name(), quoted);
                continue;
            }
            String rule = access.maskRuleOf() == null ? null : access.maskRuleOf().apply(c.name());
            if (rule == null) {
                throw InfinoAccess.refused("[" + c.name() + "] is masked for you by a rule SQL cannot read");
            }
            projected.put(c.name(), masked(quoted, rule, access.maskAlgorithm()));
        }
        String where = access.documentsWhere(columns, table, nowMillis);
        if (aliasFilter != null) {
            where = where == null ? aliasFilter : "(" + where + ") AND (" + aliasFilter + ")";
        }
        return new Part(table, projected, whole, where);
    }

    /**
     * The view the parts make: one part as it is; several as one row set, each
     * part giving every column any of them has, in one order, a column it
     * lacks as null. A column read as stored carries its table's Parquet
     * field id, and the engine refuses a union whose inputs' ids differ (as
     * two indices' tables do), so in a union each column is read through
     * {@code coalesce(.., NULL)}, which is the value without the id.
     */
    static String view(List<Part> parts) {
        if (parts.size() == 1) {
            Part p = parts.get(0);
            return select(p, p.whole() ? "*" : String.join(", ", aliased(p.columns()).values()));
        }
        Set<String> names = new LinkedHashSet<>();
        for (Part p : parts) {
            names.addAll(p.columns().keySet());
        }
        List<String> selects = new ArrayList<>();
        for (Part p : parts) {
            List<String> list = new ArrayList<>(names.size());
            for (String name : names) {
                String expr = p.columns().get(name);
                String quoted = InfinoQueryTranslator.quoteIdent(name);
                list.add((expr == null ? "NULL" : "coalesce(" + expr + ", NULL)") + " AS " + quoted);
            }
            selects.add(select(p, String.join(", ", list)));
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

    private static String select(Part p, String list) {
        return "SELECT " + list + " FROM " + InfinoQueryTranslator.quoteIdent(p.table()) + (p.where() == null ? "" : " WHERE " + p.where());
    }

    /**
     * {@code column} masked as the security plugin masks it by {@code rule}
     * (its masked-field expression), in SQL: a regular-expression mask as its
     * replacements in order; a hash as the digest its algorithm names, or
     * {@code defaultAlgorithm} when it names none, in lower-case hex. A
     * rule SQL has no equal for is refused: the default salted BLAKE2b, and
     * an algorithm outside MD5 and the SHA-2 family.
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
        String algorithm = parts.size() == 2 ? parts.get(1) : defaultAlgorithm;
        if (parts.size() > 2 || algorithm == null || algorithm.isEmpty() || algorithm.equalsIgnoreCase(LEGACY_DEFAULT_ALGORITHM)) {
            throw InfinoAccess.refused(
                "the field masked by [" + rule + "] is hashed by the security plugin's default, a BLAKE2b keyed with the cluster's salt, which SQL has no function to give"
            );
        }
        String digest = DIGESTS.get(algorithm.replace("-", "").toUpperCase(Locale.ROOT));
        if (digest == null) {
            throw InfinoAccess.refused("the field masked by [" + rule + "] is hashed with " + algorithm + ", which SQL has no function to give");
        }
        return "encode(digest(" + text + ", '" + digest + "'), 'hex')";
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
