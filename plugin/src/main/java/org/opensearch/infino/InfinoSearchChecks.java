/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.MappingMetadata;
import org.opensearch.common.regex.Regex;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.BoostingQueryBuilder;
import org.opensearch.index.query.ConstantScoreQueryBuilder;
import org.opensearch.index.query.DisMaxQueryBuilder;
import org.opensearch.index.query.FuzzyQueryBuilder;
import org.opensearch.index.query.NestedQueryBuilder;
import org.opensearch.index.query.PrefixQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.RangeQueryBuilder;
import org.opensearch.index.query.RegexpQueryBuilder;
import org.opensearch.index.query.ScriptQueryBuilder;
import org.opensearch.index.query.TermsQueryBuilder;
import org.opensearch.index.query.WildcardQueryBuilder;
import org.opensearch.index.query.functionscore.FunctionScoreQueryBuilder;
import org.opensearch.index.query.functionscore.ScriptScoreQueryBuilder;
import org.opensearch.search.SearchService;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.rescore.RescorerBuilder;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The checks a shard makes of a search as written, made for the indices the
 * plugin serves, whose shards never see it as written: the index's limits on
 * the rescore window, docvalue and script fields and a terms query's terms,
 * and the queries {@code search.allow_expensive_queries: false} refuses.
 * Each is refused with stock's message, as a bad request.
 */
final class InfinoSearchChecks {
    private InfinoSearchChecks() {}

    /** The mapped types whose prefix, wildcard, regexp, fuzzy and range queries are expensive. */
    private static final Set<String> STRING_TYPES = Set.of("text", "match_only_text", "keyword");

    /** Refuse {@code source} over {@code indices} as a shard of each would. */
    static void check(SearchSourceBuilder source, List<IndexMetadata> indices, boolean allowExpensive) {
        // The one check a search with no source can fail: it lists documents over every time there is.
        for (IndexMetadata index : indices) {
            InfinoSearchWindow.check(source, index, System::currentTimeMillis);
        }
        if (source == null) {
            return;
        }
        for (IndexMetadata index : indices) {
            Settings settings = index.getSettings();
            checkRescores(source, settings);
            checkFields(source, settings);
            int maxTerms = IndexSettings.MAX_TERMS_COUNT_SETTING.get(settings);
            Map<String, Object> properties = properties(index.mapping());
            checkQuery(source.query(), maxTerms, allowExpensive, properties);
            checkQuery(source.postFilter(), maxTerms, allowExpensive, properties);
        }
    }

    private static void checkRescores(SearchSourceBuilder source, Settings settings) {
        if (source.rescores() == null) {
            return;
        }
        int max = IndexSettings.MAX_RESCORE_WINDOW_SETTING.get(settings);
        for (RescorerBuilder<?> rescore : source.rescores()) {
            Integer window = rescore.windowSize();
            if (window != null && window > max) {
                throw new IllegalArgumentException(
                    "Rescore window [" + window + "] is too large. It must be less than [" + max + "]. This prevents allocating massive heaps for storing the results "
                        + "to be rescored. This limit can be set by changing the [" + IndexSettings.MAX_RESCORE_WINDOW_SETTING.getKey() + "] index level setting."
                );
            }
        }
    }

    private static void checkFields(SearchSourceBuilder source, Settings settings) {
        if (source.docValueFields() != null) {
            int max = IndexSettings.MAX_DOCVALUE_FIELDS_SEARCH_SETTING.get(settings);
            // A pattern counts the fields it matches; only named fields are counted here.
            long named = source.docValueFields().stream().filter(f -> !Regex.isSimpleMatchPattern(f.field)).count();
            if (named > max) {
                throw new IllegalArgumentException(
                    "Trying to retrieve too many docvalue_fields. Must be less than or equal to: [" + max + "] but was [" + named + "]. This limit can be set by changing the ["
                        + IndexSettings.MAX_DOCVALUE_FIELDS_SEARCH_SETTING.getKey() + "] index level setting."
                );
            }
        }
        if (source.scriptFields() != null && source.size() != 0) {
            int max = IndexSettings.MAX_SCRIPT_FIELDS_SETTING.get(settings);
            if (source.scriptFields().size() > max) {
                throw new IllegalArgumentException(
                    "Trying to retrieve too many script_fields. Must be less than or equal to: [" + max + "] but was [" + source.scriptFields().size()
                        + "]. This limit can be set by changing the [" + IndexSettings.MAX_SCRIPT_FIELDS_SETTING.getKey() + "] index level setting."
                );
            }
        }
    }

    private static void checkQuery(QueryBuilder qb, int maxTerms, boolean allowExpensive, Map<String, Object> properties) {
        if (qb == null) {
            return;
        }
        if (qb instanceof TermsQueryBuilder terms && terms.values() != null && terms.values().size() > maxTerms) {
            throw new IllegalArgumentException(
                "The number of terms [" + terms.values().size() + "] used in the Terms Query request has exceeded the allowed maximum of [" + maxTerms + "]. "
                    + "This maximum can be set by changing the [" + IndexSettings.MAX_TERMS_COUNT_SETTING.getKey() + "] index level setting."
            );
        }
        if (!allowExpensive) {
            String refused = expensive(qb, properties);
            if (refused != null) {
                throw new IllegalArgumentException(refused + " cannot be executed when '" + SearchService.ALLOW_EXPENSIVE_QUERIES.getKey() + "' is set to false."
                    + (qb instanceof PrefixQueryBuilder ? " For optimised prefix queries on text fields please enable [index_prefixes]." : ""));
            }
        }
        for (QueryBuilder inner : inner(qb)) {
            checkQuery(inner, maxTerms, allowExpensive, properties);
        }
    }

    /** What stock names a query {@code search.allow_expensive_queries: false} refuses, or null when it does not refuse it. */
    private static String expensive(QueryBuilder qb, Map<String, Object> properties) {
        if (qb instanceof ScriptQueryBuilder) {
            return "[script] queries";
        }
        if (qb instanceof ScriptScoreQueryBuilder) {
            return "[script score] queries";
        }
        if (qb instanceof NestedQueryBuilder) {
            return "[joining] queries";
        }
        if (qb instanceof PrefixQueryBuilder p) {
            Map<String, Object> field = field(properties, p.fieldName());
            return stringField(field) && !field.containsKey("index_prefixes") ? "[prefix] queries" : null;
        }
        if (qb instanceof WildcardQueryBuilder w) {
            return stringField(field(properties, w.fieldName())) ? "[wildcard] queries" : null;
        }
        if (qb instanceof RegexpQueryBuilder r) {
            return stringField(field(properties, r.fieldName())) ? "[regexp] queries" : null;
        }
        if (qb instanceof FuzzyQueryBuilder f) {
            return stringField(field(properties, f.fieldName())) ? "[fuzzy] queries" : null;
        }
        if (qb instanceof RangeQueryBuilder r) {
            return stringField(field(properties, r.fieldName())) ? "[range] queries on [text] or [keyword] fields" : null;
        }
        return null;
    }

    /** The queries a compound query holds. */
    static List<QueryBuilder> inner(QueryBuilder qb) {
        if (qb instanceof BoolQueryBuilder b) {
            List<QueryBuilder> all = new ArrayList<>(b.must());
            all.addAll(b.filter());
            all.addAll(b.should());
            all.addAll(b.mustNot());
            return all;
        }
        if (qb instanceof ConstantScoreQueryBuilder c) {
            return List.of(c.innerQuery());
        }
        if (qb instanceof BoostingQueryBuilder b) {
            return List.of(b.positiveQuery(), b.negativeQuery());
        }
        if (qb instanceof DisMaxQueryBuilder d) {
            return d.innerQueries();
        }
        if (qb instanceof FunctionScoreQueryBuilder f) {
            return List.of(f.query());
        }
        if (qb instanceof ScriptScoreQueryBuilder s) {
            return List.of(s.query());
        }
        if (qb instanceof NestedQueryBuilder n) {
            return List.of(n.query());
        }
        return List.of();
    }

    private static boolean stringField(Map<String, Object> field) {
        return field != null && STRING_TYPES.contains(String.valueOf(field.get("type")));
    }

    @SuppressWarnings("unchecked")
    static Map<String, Object> properties(MappingMetadata mapping) {
        Object properties = mapping == null ? null : mapping.sourceAsMap().get("properties");
        return properties instanceof Map<?, ?> m ? (Map<String, Object>) m : Map.of();
    }

    /** A field's mapping by its full name, through objects and multi-fields, an alias followed; null when it is not mapped. */
    static Map<String, Object> field(Map<String, Object> properties, String name) {
        Map<String, Object> field = lookup(properties, name);
        if (field != null && "alias".equals(field.get("type")) && field.get("path") instanceof String path) {
            return lookup(properties, path);
        }
        return field;
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> lookup(Map<String, Object> properties, String name) {
        if (properties == null || name == null) {
            return null;
        }
        if (properties.get(name) instanceof Map<?, ?> direct) {
            return (Map<String, Object>) direct;
        }
        for (int dot = name.indexOf('.'); dot > 0; dot = name.indexOf('.', dot + 1)) {
            if (properties.get(name.substring(0, dot)) instanceof Map<?, ?> parent) {
                String rest = name.substring(dot + 1);
                for (String children : List.of("properties", "fields")) {
                    if (parent.get(children) instanceof Map<?, ?> m) {
                        Map<String, Object> found = lookup((Map<String, Object>) m, rest);
                        if (found != null) {
                            return found;
                        }
                    }
                }
            }
        }
        return null;
    }
}
