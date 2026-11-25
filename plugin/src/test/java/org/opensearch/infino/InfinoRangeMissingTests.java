/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import org.opensearch.infino.InfinoAggregationProcessor.Explicit;
import org.opensearch.infino.InfinoAggregationProcessor.ExplicitBucket;
import org.opensearch.infino.InfinoAggregationProcessor.Source;
import org.opensearch.infino.InfinoAggregationProcessor.Spec;
import org.opensearch.infino.InfinoScriptTranslator.SqlType;
import org.opensearch.search.aggregations.bucket.range.DateRangeAggregationBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;

public class InfinoRangeMissingTests extends OpenSearchTestCase {

    private static final String MILLIS = "'Timestamp(Millisecond, Some(\"UTC\"))'";
    private static final long NOW = Instant.parse("2024-05-01T12:00:00Z").toEpochMilli();

    private static Source date(String format) {
        return new Source("\"d\"", SqlType.DATE, "[d]", null, "d", format, null, null);
    }

    private static long millis(String iso) {
        return Instant.parse(iso).toEpochMilli();
    }

    /** As stock, an epoch_second field's missing value is seconds: 0 is the epoch, 200000 is 200000 seconds. */
    public void testMissingIsReadInTheFieldsFormat() {
        DateRangeAggregationBuilder b = new DateRangeAggregationBuilder("r").field("d").missing("200000");
        assertEquals("arrow_cast(200000000, " + MILLIS + ")", Spec.missingDate(b, date("epoch_second"), NOW));
        assertEquals("arrow_cast(0, " + MILLIS + ")", Spec.missingDate(b.missing("0"), date("epoch_second"), NOW));
    }

    /** The aggregation's format and time zone win over the field's, and date math is read against now. */
    public void testMissingIsReadInTheAggregationsFormatAndZoneWithDateMath() {
        DateRangeAggregationBuilder zoned = new DateRangeAggregationBuilder("r").field("d")
            .format("yyyy-MM-dd'T'HH:mm")
            .timeZone(ZoneOffset.ofHours(1))
            .missing("2020-01-01T00:30");
        assertEquals("arrow_cast(" + millis("2019-12-31T23:30:00Z") + ", " + MILLIS + ")", Spec.missingDate(zoned, date("epoch_second"), NOW));
        DateRangeAggregationBuilder math = new DateRangeAggregationBuilder("r").field("d").missing("now-1d/d");
        assertEquals("arrow_cast(" + millis("2024-04-30T00:00:00Z") + ", " + MILLIS + ")", Spec.missingDate(math, date(null), NOW));
    }

    /** A missing value that is not a date has no value, which the aggregation refuses by name. */
    public void testMissingThatIsNotADateIsRefused() {
        DateRangeAggregationBuilder b = new DateRangeAggregationBuilder("r").field("d").missing("not a date");
        assertNull(Spec.missingDate(b, date(null), NOW));
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> Spec.withMissing(b, date(null), NOW));
        assertTrue(e.getMessage(), e.getMessage().contains("[missing: not a date]"));
    }

    /** The stand-in is a typed timestamp under COALESCE, which does not cast text to one. */
    public void testMissingStandsInAsATimestamp() {
        DateRangeAggregationBuilder b = new DateRangeAggregationBuilder("r").field("d").missing("2020-01-01");
        assertEquals("COALESCE(\"d\", arrow_cast(" + millis("2020-01-01T00:00:00Z") + ", " + MILLIS + "))", Spec.withMissing(b, date(null), NOW).expr());
    }

    /** A bound is text where a nanosecond timestamp holds it, and the epoch value outside 1677-2262. */
    public void testBoundsOutsideNanosecondsAreEpochValues() {
        assertEquals("'2020-01-01T00:00:00Z'", Explicit.bound(date(null), millis("2020-01-01T00:00:00Z")));
        long ancient = millis("1000-01-01T00:00:00Z");
        assertEquals("arrow_cast(" + ancient + ", " + MILLIS + ")", Explicit.bound(date(null), ancient));
        assertEquals("arrow_cast(946713600000000, " + MILLIS + ")", Explicit.bound(date(null), 946713600000000d));
    }

    /** As stock, range buckets are ordered by from, then to, ties as requested. */
    public void testBucketsAreOrderedByTheirBounds() {
        List<ExplicitBucket> buckets = new ArrayList<>(List.of(
            new ExplicitBucket("y", "", 315561600000d, 946713600000d),
            new ExplicitBucket("x", "", 200000d, 315561600000d),
            new ExplicitBucket("other", "", Double.NEGATIVE_INFINITY, 200000d),
            new ExplicitBucket("x2", "", 200000d, 315561600000d),
            new ExplicitBucket("open", "", 200000d, Double.POSITIVE_INFINITY)
        ));
        buckets.sort(Explicit.BY_BOUNDS);
        assertEquals(List.of("other", "x", "x2", "open", "y"), buckets.stream().map(ExplicitBucket::key).toList());
    }
}
