/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.regex.Regex;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.ConstantScoreQueryBuilder;
import org.opensearch.index.query.MatchPhrasePrefixQueryBuilder;
import org.opensearch.index.query.MatchPhraseQueryBuilder;
import org.opensearch.index.query.MultiMatchQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryStringQueryBuilder;
import org.opensearch.index.query.RangeQueryBuilder;
import org.opensearch.index.query.SimpleQueryStringBuilder;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.metrics.AvgAggregationBuilder;
import org.opensearch.search.aggregations.metrics.MaxAggregationBuilder;
import org.opensearch.search.aggregations.metrics.MinAggregationBuilder;
import org.opensearch.search.aggregations.metrics.StatsAggregationBuilder;
import org.opensearch.search.aggregations.metrics.SumAggregationBuilder;
import org.opensearch.search.aggregations.metrics.ValueCountAggregationBuilder;
import org.opensearch.search.builder.SearchSourceBuilder;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.LongSupplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * What one search of an index may ask of its table, for a table too large
 * to read whole on a visitor's request: the widest span of time
 * ({@link #FIELD}, {@link #MAX}), and whether a phrase ({@link #PHRASES}).
 *
 * <p>A table of this size answers a count or a sum over any span quickly,
 * and a listing of documents or a breakdown into groups only over a short
 * one: past that the statement runs for minutes, long after the client that
 * asked has given up, and holds a search thread of the node the whole time,
 * so a handful of them stop the node answering anything. With the limit
 * set, such a search is refused at once with the span it may ask for, and
 * the table is never sent the statement.
 *
 * <p>Held to the span: a search that returns hits, and a search whose
 * aggregations are anything but plain numbers over the matching rows. Not
 * held: a count, and a sum, average, minimum, maximum, value count or stats
 * with no hits beside them, which the table answers over any span.
 *
 * <p>A span is measured as the statement will read it: each end of a range
 * through {@link InfinoQueryTranslator#dateBoundMillis}, the reading the
 * translator sends, in the range's format or the field's, rounded as an
 * inclusive or exclusive end is.
 */
final class InfinoSearchWindow {
    private InfinoSearchWindow() {}

    /** The date field a search's span of time is read from. Empty, the default, for no limit. */
    static final Setting<String> FIELD = Setting.simpleString("index.infino.search.window.field", Setting.Property.IndexScope, Setting.Property.Dynamic);

    /** The widest span of {@link #FIELD} a search for documents or a breakdown may read. Negative, the default, for no limit. */
    static final Setting<TimeValue> MAX = Setting.timeSetting(
        "index.infino.search.window.max",
        TimeValue.MINUS_ONE,
        TimeValue.MINUS_ONE,
        Setting.Property.IndexScope,
        Setting.Property.Dynamic
    );

    /**
     * Whether a search may ask for a phrase on a text field. On by default.
     * The table answers words as a filter beside a search's other clauses,
     * its range of time among them, and a phrase through a lookup of its own
     * over every row, which a range does not narrow: on a table too large to
     * read whole that lookup does not finish. Off, a search that asks for a
     * phrase is refused at once, a count included.
     */
    static final Setting<Boolean> PHRASES = Setting.boolSetting("index.infino.search.phrases", true, Setting.Property.IndexScope, Setting.Property.Dynamic);

    /** The mapped types whose values are searched as text, a phrase among the ways. */
    private static final Set<String> TEXT_TYPES = Set.of("text", "match_only_text");

    /** A rounding of an inclusive upper end that fills no more than this is the end's own precision, not a wider span. */
    private static final long PRECISION_MILLIS = 1000;

    /** A field named just before a quoted run of a query string, as {@code title:"..."}. */
    private static final Pattern NAMED = Pattern.compile("([\\w.*@-]+)\\s*:\\s*\\(?\\s*$");

    /** As {@link #check(SearchSourceBuilder, IndexMetadata, Map, LongSupplier)}, reading the index's mapping here. */
    static void check(SearchSourceBuilder source, IndexMetadata index, LongSupplier now) {
        check(source, index, InfinoSearchChecks.properties(index.mapping()), now);
    }

    /**
     * Refuse {@code source} over {@code index} when it asks its table for
     * more than the index allows one search: a phrase where the index serves
     * none, or a wider span of time than its limit. {@code properties} is
     * the index's mapped fields.
     */
    static void check(SearchSourceBuilder source, IndexMetadata index, Map<String, Object> properties, LongSupplier now) {
        Settings settings = index.getSettings();
        String name = index.getIndex().getName();
        if (!PHRASES.get(settings) && source != null && phrase(source, properties)) {
            throw new IllegalArgumentException(
                "[infino] index ["
                    + name
                    + "] does not search for a phrase: its table looks one up across every row, whatever else the search asks; search for the words without the quotes ["
                    + PHRASES.getKey()
                    + "]"
            );
        }
        String field = FIELD.get(settings);
        TimeValue max = MAX.get(settings);
        if (field.isEmpty() || max.millis() < 0 || !limited(source)) {
            return;
        }
        Map<String, Object> mapped = InfinoSearchChecks.field(properties, field);
        String format = mapped != null && mapped.get("format") instanceof String f ? f : null;
        long[] span = { Long.MIN_VALUE, Long.MAX_VALUE };
        if (source != null) {
            long at = now.getAsLong();
            narrow(source.query(), field, format, at, span);
            narrow(source.postFilter(), field, format, at, span);
        }
        if (span[0] == Long.MIN_VALUE || span[1] == Long.MAX_VALUE) {
            throw new IllegalArgumentException(
                "[infino] index ["
                    + name
                    + "] lists documents and breaks them into groups over at most ["
                    + max
                    + "] of ["
                    + field
                    + "] in one search, and this search names no such range; add a range on ["
                    + field
                    + "] with both ends, or ask for a count or a total alone ["
                    + MAX.getKey()
                    + "]"
            );
        }
        long asked;
        try {
            asked = Math.subtractExact(span[1], span[0]);
        } catch (ArithmeticException e) {
            asked = Long.MAX_VALUE; // ends too far apart to measure are too far apart to read
        }
        if (asked > max.millis()) {
            throw new IllegalArgumentException(
                "[infino] index ["
                    + name
                    + "] lists documents and breaks them into groups over at most ["
                    + max
                    + "] of ["
                    + field
                    + "] in one search, and this search asks for ["
                    + (asked == Long.MAX_VALUE ? "more than can be measured" : TimeValue.timeValueMillis(asked).toString())
                    + "]; narrow the range, or ask for a count or a total alone ["
                    + MAX.getKey()
                    + "]"
            );
        }
    }

    /**
     * Whether the search is one the span holds: it returns hits, or one of
     * its aggregations is more than a plain number over the matching rows.
     * A search with no source returns the default page of hits.
     */
    static boolean limited(SearchSourceBuilder source) {
        if (source == null) {
            return true;
        }
        if (source.size() != 0) { // unset (-1) is the default page of hits
            return true;
        }
        if (source.aggregations() == null) {
            return false;
        }
        for (AggregationBuilder agg : source.aggregations().getAggregatorFactories()) {
            boolean plain = agg instanceof SumAggregationBuilder
                || agg instanceof AvgAggregationBuilder
                || agg instanceof MinAggregationBuilder
                || agg instanceof MaxAggregationBuilder
                || agg instanceof ValueCountAggregationBuilder
                || agg instanceof StatsAggregationBuilder;
            if (!plain || !agg.getSubAggregations().isEmpty()) {
                return true;
            }
        }
        return false;
    }

    /**
     * Narrow {@code span} (its start and end, in milliseconds) by every range
     * on {@code field} that each matching document must satisfy: the query
     * itself, and the clauses a bool query requires or filters by, at any
     * depth. A clause that only may match (a should, a must_not) bounds
     * nothing. {@code format} is the field's mapped format, which a range
     * that names none of its own is read in.
     */
    private static void narrow(QueryBuilder query, String field, String format, long now, long[] span) {
        if (query instanceof BoolQueryBuilder bool) {
            for (QueryBuilder must : bool.must()) {
                narrow(must, field, format, now, span);
            }
            for (QueryBuilder filter : bool.filter()) {
                narrow(filter, field, format, now, span);
            }
        } else if (query instanceof ConstantScoreQueryBuilder constant) {
            narrow(constant.innerQuery(), field, format, now, span);
        } else if (query instanceof RangeQueryBuilder range && field.equals(range.fieldName())) {
            String read = range.format() != null ? range.format() : format;
            ZoneId zone = range.timeZone() == null ? null : ZoneId.of(range.timeZone());
            if (range.from() != null) {
                // An exclusive lower end rounds up, as the statement's does.
                span[0] = Math.max(span[0], InfinoQueryTranslator.dateBoundMillis(range.from(), read, !range.includeLower(), zone, now));
            }
            if (range.to() != null) {
                span[1] = Math.min(span[1], end(range, read, zone, now));
            }
        }
    }

    /**
     * Where a range's upper end falls, as the statement reads it. An
     * inclusive end is rounded up to the last instant of what it names: of
     * the year for {@code now/y}, of the month for {@code 2025-05} read as a
     * month, and the span is measured to there. An end written to the second
     * rounds up by under a second, which is its own precision and no wider a
     * span: midnight to midnight, both ends inclusive, is a day.
     */
    private static long end(RangeQueryBuilder range, String format, ZoneId zone, long now) {
        long exact = InfinoQueryTranslator.dateBoundMillis(range.to(), format, false, zone, now);
        if (!range.includeUpper()) {
            return exact;
        }
        long rounded = InfinoQueryTranslator.dateBoundMillis(range.to(), format, true, zone, now);
        return rounded - exact < PRECISION_MILLIS ? exact : rounded;
    }

    /**
     * Whether the search asks for a phrase on a text field anywhere the
     * table would look one up: any query it holds (see
     * {@link InfinoSearchChecks#queries}).
     */
    static boolean phrase(SearchSourceBuilder source, Map<String, Object> properties) {
        return InfinoSearchChecks.queries(source).stream().anyMatch(q -> phrase(q, properties));
    }

    /**
     * Whether the query asks for a phrase on a text field anywhere in it: a
     * phrase query, a multi-field match of the phrase kinds, or a query
     * string holding a quoted run.
     *
     * <p>Only over a field mapped as text. A phrase query on any other field
     * is a comparison with the whole value, which is how a filter on a
     * keyword, a number or a date is commonly written, and the table answers
     * it as a comparison. So is a quoted run of a query string that names
     * such a field before it ({@code host:"web-01"}); one that names no
     * field searches the index's default ones, text among them.
     */
    static boolean phrase(QueryBuilder query, Map<String, Object> properties) {
        if (query == null) {
            return false;
        }
        if (query instanceof MatchPhraseQueryBuilder match) {
            return text(properties, match.fieldName());
        }
        if (query instanceof MatchPhrasePrefixQueryBuilder match) {
            return text(properties, match.fieldName());
        }
        if (query instanceof MultiMatchQueryBuilder multi) {
            boolean phrases = multi.type() == MultiMatchQueryBuilder.Type.PHRASE || multi.type() == MultiMatchQueryBuilder.Type.PHRASE_PREFIX;
            // With no fields named it searches the index's default ones, text among them.
            return phrases && (multi.fields().isEmpty() || multi.fields().keySet().stream().anyMatch(f -> text(properties, f)));
        }
        if (query instanceof QueryStringQueryBuilder string) {
            return string.queryString() != null && quotesText(string.queryString(), properties);
        }
        if (query instanceof SimpleQueryStringBuilder simple) {
            // Its text names no fields; the fields it searches are given beside it.
            boolean reachesText = simple.fields().isEmpty() || simple.fields().keySet().stream().anyMatch(f -> text(properties, f));
            return simple.value() != null && simple.value().indexOf('"') >= 0 && reachesText;
        }
        for (QueryBuilder inner : InfinoSearchChecks.inner(query)) {
            if (phrase(inner, properties)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Whether a query string holds a quoted run that reaches a text field:
     * one with no field named before it, or with a text field named.
     */
    private static boolean quotesText(String query, Map<String, Object> properties) {
        int open = query.indexOf('"');
        while (open >= 0) {
            int close = query.indexOf('"', open + 1);
            Matcher named = NAMED.matcher(query.substring(0, open));
            if (!named.find() || text(properties, named.group(1))) {
                return true;
            }
            if (close < 0) {
                return false;
            }
            open = query.indexOf('"', close + 1);
        }
        return false;
    }

    /**
     * Whether the index maps {@code field} as text, whose phrases the table
     * looks up in its token index; for a name with a wildcard, whether it
     * maps any text field the pattern matches.
     */
    private static boolean text(Map<String, Object> properties, String field) {
        if (field.indexOf('*') >= 0) {
            List<String> texts = new ArrayList<>();
            textFields("", properties, texts);
            return texts.stream().anyMatch(name -> Regex.simpleMatch(field, name));
        }
        Map<String, Object> mapped = InfinoSearchChecks.field(properties, field);
        return mapped != null && TEXT_TYPES.contains(String.valueOf(mapped.get("type")));
    }

    /** The full names of the text fields under {@code properties}: object fields' own, and a field's sub-fields. */
    @SuppressWarnings("unchecked")
    private static void textFields(String prefix, Map<String, Object> properties, List<String> into) {
        for (Map.Entry<String, Object> e : properties.entrySet()) {
            if (!(e.getValue() instanceof Map<?, ?> raw)) {
                continue;
            }
            Map<String, Object> mapped = (Map<String, Object>) raw;
            String name = prefix + e.getKey();
            if (TEXT_TYPES.contains(String.valueOf(mapped.get("type")))) {
                into.add(name);
            }
            for (String nested : List.of("properties", "fields")) {
                if (mapped.get(nested) instanceof Map<?, ?> inner) {
                    textFields(name + ".", (Map<String, Object>) inner, into);
                }
            }
        }
    }
}
