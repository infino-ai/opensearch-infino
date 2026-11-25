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
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.bucket.filter.FilterAggregationBuilder;
import org.opensearch.search.aggregations.bucket.filter.FiltersAggregationBuilder;
import org.opensearch.search.aggregations.bucket.filter.FiltersAggregator;
import org.opensearch.search.aggregations.bucket.terms.SignificantTermsAggregationBuilder;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.opensearch.search.rescore.QueryRescorerBuilder;
import org.opensearch.search.rescore.RescorerBuilder;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * The checks a shard makes of a search as written, made for the indices the
 * plugin serves, whose shards never see it as written: the index's limits on
 * the rescore window, docvalue and script fields and a terms query's terms,
 * the queries {@code search.allow_expensive_queries: false} refuses,
 * wherever in the search a query sits, {@code stored_fields: _none_}
 * alongside the source or {@code fields}, and a {@code token_count} field
 * without doc values in {@code docvalue_fields}.
 * Each is refused with stock's message, as a bad request.
 */
final class InfinoSearchChecks {
    private InfinoSearchChecks() {}

    /** The mapped types whose prefix, wildcard, regexp, fuzzy and range queries are expensive. */
    private static final Set<String> STRING_TYPES = Set.of("text", "match_only_text", "keyword");

    /** Refuse {@code source} over {@code indices} as a shard of each would. */
    static void check(SearchSourceBuilder source, List<IndexMetadata> indices, boolean allowExpensive) {
        for (IndexMetadata index : indices) {
            Settings settings = index.getSettings();
            // The mapping is read once for an index, for every check that needs its fields.
            Map<String, Object> properties = properties(index.mapping());
            // The one check a search with no source can fail: it lists documents over every time there is.
            InfinoSearchWindow.check(source, index, properties, System::currentTimeMillis);
            if (source == null) {
                continue;
            }
            checkRescores(source, settings);
            checkFields(source, settings);
            checkTokenCountDocValues(source, properties);
            if (source.storedFields() != null) {
                checkNoStoredFields(source);
                String stored = storedField(properties, source.storedFields().fieldNames());
                if (stored != null) {
                    throw storedFieldRefused(stored, index.getIndex().getName());
                }
            }
            int maxTerms = IndexSettings.MAX_TERMS_COUNT_SETTING.get(settings);
            for (QueryBuilder query : queries(source)) {
                checkQuery(query, maxTerms, allowExpensive, properties);
            }
            for (QueryBuilder query : highlightQueries(source)) {
                checkQuery(query, maxTerms, allowExpensive, properties);
            }
        }
    }

    /**
     * Every query the search holds, each of which a shard builds and checks:
     * its query, its post filter, the query of a filter or filters
     * aggregation and a significant terms aggregation's background filter at
     * any depth, and a rescore's query.
     */
    static List<QueryBuilder> queries(SearchSourceBuilder source) {
        List<QueryBuilder> all = new ArrayList<>();
        all.add(source.query());
        all.add(source.postFilter());
        if (source.aggregations() != null) {
            aggregationQueries(source.aggregations().getAggregatorFactories(), all);
        }
        if (source.rescores() != null) {
            for (RescorerBuilder<?> rescore : source.rescores()) {
                if (rescore instanceof QueryRescorerBuilder query) {
                    all.add(query.getRescoreQuery());
                }
            }
        }
        all.removeIf(Objects::isNull);
        return all;
    }

    /** The queries a highlight asks to be built, which a shard checks as it checks the search's own. */
    private static List<QueryBuilder> highlightQueries(SearchSourceBuilder source) {
        HighlightBuilder highlight = source.highlighter();
        if (highlight == null) {
            return List.of();
        }
        List<QueryBuilder> all = new ArrayList<>();
        all.add(highlight.highlightQuery());
        for (HighlightBuilder.Field field : highlight.fields()) {
            all.add(field.highlightQuery());
        }
        all.removeIf(Objects::isNull);
        return all;
    }

    private static void aggregationQueries(Collection<AggregationBuilder> aggregations, List<QueryBuilder> all) {
        for (AggregationBuilder agg : aggregations) {
            if (agg instanceof FilterAggregationBuilder filter) {
                all.add(filter.getFilter());
            } else if (agg instanceof FiltersAggregationBuilder filters) {
                for (FiltersAggregator.KeyedFilter keyed : filters.filters()) {
                    all.add(keyed.filter());
                }
            } else if (agg instanceof SignificantTermsAggregationBuilder significant) {
                all.add(significant.backgroundFilter());
            }
            aggregationQueries(agg.getSubAggregations(), all);
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

    /**
     * A {@code docvalue_fields} entry, by name or pattern, naming a
     * {@code token_count} field without doc values ({@link InfinoTokenCounts}):
     * stock has no field data to read its counts from. Stock keeps a token count as an
     * {@code integer} field, so its message names that type.
     */
    static void checkTokenCountDocValues(SearchSourceBuilder source, Map<String, Object> properties) {
        if (source.docValueFields() == null || source.docValueFields().isEmpty()) {
            return;
        }
        for (Map.Entry<String, Boolean> e : InfinoTokenCounts.fields(properties).entrySet()) {
            if (!e.getValue() && source.docValueFields().stream().anyMatch(f -> Regex.simpleMatch(f.field, e.getKey()))) {
                throw new IllegalArgumentException(
                    "Can't load fielddata on [" + e.getKey() + "] because fielddata is unsupported on fields of type [integer]. Use doc values instead."
                );
            }
        }
    }

    /** {@code stored_fields: _none_} leaves a hit nothing to show, so it cannot ask for the source or for fields. */
    static void checkNoStoredFields(SearchSourceBuilder source) {
        if (source.storedFields() == null || source.storedFields().fetchFields()) {
            return;
        }
        if (source.fetchSource() != null && source.fetchSource().fetchSource()) {
            throw new IllegalArgumentException("[stored_fields] cannot be disabled if [_source] is requested");
        }
        if (source.fetchFields() != null) {
            throw new IllegalArgumentException("[stored_fields] cannot be disabled when using the [fields] option");
        }
    }

    private static void checkQuery(QueryBuilder qb, int maxTerms, boolean allowExpensive, Map<String, Object> properties) {
        if (qb == null) {
            return;
        }
        // A shard answers a terms query on a field its index does not map with nothing, before it counts the terms.
        if (qb instanceof TermsQueryBuilder terms && terms.values() != null && terms.values().size() > maxTerms && mapped(properties, terms.fieldName())) {
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

    /** Whether the index maps the field: in its mapping, or one of the metadata fields every index has. */
    private static boolean mapped(Map<String, Object> properties, String name) {
        return name != null && (name.startsWith("_") || field(properties, name) != null);
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

    /**
     * The first of {@code names} that {@code properties} maps with {@code store: true},
     * or null when none is. A pattern is not matched: a request for every stored
     * field, as a dashboard's search sends, reads the source as it always has.
     */
    static String storedField(Map<String, Object> properties, List<String> names) {
        if (names == null) {
            return null;
        }
        for (String name : names) {
            Map<String, Object> field = Regex.isSimpleMatchPattern(name) ? null : field(properties, name);
            if (field != null && Boolean.parseBoolean(String.valueOf(field.get("store")))) {
                return name;
            }
        }
        return null;
    }

    /** The refusal of a request for the values of {@code field}, which {@code index} stores apart from the source. */
    static IllegalArgumentException storedFieldRefused(String field, String index) {
        // The table keeps the source, not the values a stored field holds apart from it.
        return new IllegalArgumentException("[infino] the stored field [" + field + "] of [" + index + "] is not served by the table; read it from _source");
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
