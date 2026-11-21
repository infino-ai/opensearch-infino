/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.time.DateFormatter;
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
import java.time.ZoneOffset;
import java.util.function.LongSupplier;

/**
 * The widest span of time one search of an index may read, for an index
 * whose table is too large to read whole on a visitor's request
 * ({@link #FIELD}, {@link #MAX}).
 *
 * <p>A table of this size answers a count or a sum over any span quickly,
 * and a listing of documents or a breakdown into groups only over a short
 * one: past that the statement runs for minutes, long after the client that
 * asked has given up, and holds a search thread of the node the whole time,
 * so a handful of them stop the node answering anything. With the limit
 * set, such a search is refused at once with the span it may ask for, and
 * the table is never sent the statement.
 *
 * <p>Held to the limit: a search that returns hits, and a search whose
 * aggregations are anything but plain numbers over the matching rows. Not
 * held: a count, and a sum, average, minimum, maximum, value count or stats
 * with no hits beside them, which the table answers over any span.
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

    /** A range's bounds are read as a date field reads them unless the range names a format of its own. */
    private static final String DEFAULT_FORMAT = "strict_date_optional_time||epoch_millis";

    /** Refuse {@code source} over {@code index} when it reads a wider span of time than the index allows one search. */
    static void check(SearchSourceBuilder source, IndexMetadata index, LongSupplier now) {
        Settings settings = index.getSettings();
        String field = FIELD.get(settings);
        TimeValue max = MAX.get(settings);
        if (field.isEmpty() || max.millis() < 0) {
            return;
        }
        String name = index.getIndex().getName();
        if (source != null && (phrase(source.query()) || phrase(source.postFilter()))) {
            // Whatever else the search asks for, a count included: the phrase is looked up first.
            throw new IllegalArgumentException(
                "[infino] index ["
                    + name
                    + "] limits the span of ["
                    + field
                    + "] one search reads, and a phrase is looked up across the whole table whatever range the search names; search for the words without the quotes ["
                    + MAX.getKey()
                    + "]"
            );
        }
        if (!limited(source)) {
            return;
        }
        long[] span = { Long.MIN_VALUE, Long.MAX_VALUE };
        if (source != null) {
            narrow(source.query(), field, now, span);
            narrow(source.postFilter(), field, now, span);
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
        long asked = span[1] - span[0];
        if (asked > max.millis()) {
            throw new IllegalArgumentException(
                "[infino] index ["
                    + name
                    + "] lists documents and breaks them into groups over at most ["
                    + max
                    + "] of ["
                    + field
                    + "] in one search, and this search asks for ["
                    + TimeValue.timeValueMillis(asked)
                    + "]; narrow the range, or ask for a count or a total alone ["
                    + MAX.getKey()
                    + "]"
            );
        }
    }

    /**
     * Whether the query asks for a phrase anywhere in it: a phrase query, a
     * multi-field match of the phrase kinds, or a query string holding a
     * quoted run. The table answers words as a filter beside the search's
     * other clauses, its range of time among them, but a phrase through a
     * lookup of its own over every row, which a range does not narrow: on a
     * table that needs a limit on its span, that lookup does not finish.
     */
    static boolean phrase(QueryBuilder query) {
        if (query == null) {
            return false;
        }
        if (query instanceof MatchPhraseQueryBuilder || query instanceof MatchPhrasePrefixQueryBuilder) {
            return true;
        }
        if (query instanceof MultiMatchQueryBuilder multi) {
            return multi.type() == MultiMatchQueryBuilder.Type.PHRASE || multi.type() == MultiMatchQueryBuilder.Type.PHRASE_PREFIX;
        }
        if (query instanceof QueryStringQueryBuilder string) {
            return string.queryString() != null && string.queryString().indexOf('"') >= 0;
        }
        if (query instanceof SimpleQueryStringBuilder simple) {
            return simple.value() != null && simple.value().indexOf('"') >= 0;
        }
        for (QueryBuilder inner : InfinoSearchChecks.inner(query)) {
            if (phrase(inner)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Whether the search is one the limit holds: it returns hits, or one of
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
     * nothing.
     */
    private static void narrow(QueryBuilder query, String field, LongSupplier now, long[] span) {
        if (query instanceof BoolQueryBuilder bool) {
            for (QueryBuilder must : bool.must()) {
                narrow(must, field, now, span);
            }
            for (QueryBuilder filter : bool.filter()) {
                narrow(filter, field, now, span);
            }
        } else if (query instanceof ConstantScoreQueryBuilder constant) {
            narrow(constant.innerQuery(), field, now, span);
        } else if (query instanceof RangeQueryBuilder range && field.equals(range.fieldName())) {
            // Each end as the instant it names, not rounded out to the end of its unit: a range
            // from one midnight to the next is a day, whichever of its ends is inclusive.
            Long from = instant(range.from(), range, now);
            Long to = instant(range.to(), range, now);
            if (from != null) {
                span[0] = Math.max(span[0], from);
            }
            if (to != null) {
                span[1] = Math.min(span[1], to);
            }
        }
    }

    /** One end of a range as milliseconds, or null when the range leaves it open or it cannot be read as a time. */
    private static Long instant(Object bound, RangeQueryBuilder range, LongSupplier now) {
        if (bound == null) {
            return null;
        }
        if (bound instanceof Number n) {
            return n.longValue();
        }
        try {
            ZoneId zone = range.timeZone() == null ? ZoneOffset.UTC : ZoneId.of(range.timeZone());
            DateFormatter format = DateFormatter.forPattern(range.format() == null ? DEFAULT_FORMAT : range.format());
            return format.toDateMathParser().parse(bound.toString(), now, false, zone).toEpochMilli();
        } catch (RuntimeException e) {
            return null;
        }
    }
}
