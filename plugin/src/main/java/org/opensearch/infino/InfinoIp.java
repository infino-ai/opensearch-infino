/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import org.apache.lucene.document.InetAddressPoint;
import org.apache.lucene.util.BytesRef;
import org.opensearch.common.network.InetAddresses;
import org.opensearch.common.network.NetworkAddress;

import java.net.InetAddress;
import java.util.HexFormat;
import java.util.Locale;

/**
 * The order of an {@code ip} field's addresses. The table holds each address
 * as the text it was written in, and text compares character by character:
 * {@code 1.0.179.100} before {@code 1.0.6.192}. Stock orders addresses as
 * numbers, by their 16-byte form (an IPv4 address as its IPv4-mapped IPv6
 * address, {@code ::ffff:a.b.c.d}), and every range, sort and key order on
 * the field follows that.
 *
 * <p>{@link #key} turns the text into that order in SQL: the 16 bytes as 32
 * lowercase hex digits, which compare as text in the order the bytes compare
 * as numbers. A query's address becomes the same digits ({@link #keyLiteral}),
 * and a key read back becomes the address as stock prints it ({@link #shown}).
 */
final class InfinoIp {
    private InfinoIp() {}

    /** What an IPv4 address's key starts with: the IPv4-mapped prefix {@code ::ffff:0:0/96}. */
    private static final String MAPPED_PREFIX = "00000000000000000000ffff";

    /** The groups of an IPv6 address written in full. */
    private static final int GROUPS = 8;

    /** The hex digits of one address's 16 bytes. */
    private static final int KEY_DIGITS = 32;

    private static final HexFormat HEX = HexFormat.of();

    /**
     * The key of the address text {@code v} (an SQL expression), as an SQL
     * expression: 32 hex digits, or null where {@code v} is null. The text is
     * any form the table holds an address in: IPv4 dotted decimal, or IPv6 in
     * any case, with or without leading zeros, {@code ::} or an IPv4 tail.
     */
    static String key(String v) {
        return "CASE WHEN strpos(" + v + ", ':') = 0 THEN " + v4Key(v) + " ELSE " + v6Key("lower(" + v + ")") + " END";
    }

    /** The key of IPv4 text {@code t}: the mapped prefix, then the address as 8 hex digits. */
    private static String v4Key(String t) {
        String number = "(" + octet(t, 1) + " * 16777216 + " + octet(t, 2) + " * 65536 + " + octet(t, 3) + " * 256 + " + octet(t, 4) + ")";
        return "('" + MAPPED_PREFIX + "' || lpad(to_hex(" + number + "), 8, '0'))";
    }

    /**
     * Octet {@code i} (from 1) of the dotted-decimal text {@code t}, as a
     * number. A try: the engine may evaluate a CASE arm over rows the arm
     * does not answer for, where the text has no such octet.
     */
    private static String octet(String t, int i) {
        return "TRY_CAST(split_part(" + t + ", '.', " + i + ") AS BIGINT)";
    }

    /**
     * The key of lowercase IPv6 text {@code a}, in four steps: an IPv4 tail
     * becomes two hex groups; a leading or trailing {@code ::} gets a zero
     * group beside it, so {@code ::} always sits between two groups; the
     * {@code ::} becomes as many zero groups as the address lacks; and each
     * of the eight groups is padded to four digits and the colons dropped.
     * Text is joined with {@code ||}, which keeps a null address null, where
     * {@code concat} would read it as empty text.
     */
    private static String v6Key(String a) {
        String tail = "regexp_replace(" + a + ", '^.*:', '')";
        String high = "(" + octet(tail, 1) + " * 256 + " + octet(tail, 2) + ")";
        String low = "(" + octet(tail, 3) + " * 256 + " + octet(tail, 4) + ")";
        String hexTail = "CASE WHEN strpos(" + a + ", '.') = 0 THEN " + a + " ELSE (regexp_replace(" + a + ", '[0-9.]+$', '') || to_hex(" + high
            + ") || ':' || to_hex(" + low + ")) END";
        String inner = "regexp_replace(regexp_replace(" + hexTail + ", '^::', '0::'), '::$', '::0')";
        String colons = "(length(" + inner + ") - length(replace(" + inner + ", ':', '')))";
        String full = "CASE WHEN strpos(" + inner + ", '::') = 0 THEN " + inner + " ELSE replace(" + inner + ", '::', (':' || repeat('0:', " + GROUPS
            + " - " + colons + "))) END";
        return "regexp_replace(regexp_replace((':' || " + full + "), ':', ':000', 'g'), ':[0-9a-f]*([0-9a-f]{4})', '\\1', 'g')";
    }

    /**
     * The key of the address {@code value} as an SQL literal, read as stock
     * reads an address in a query; one that is not an address is refused
     * with stock's message.
     */
    static String keyLiteral(Object value) {
        return "'" + keyOf(value) + "'";
    }

    /** The key of the address {@code value}: the hex digits of its 16-byte form. */
    static String keyOf(Object value) {
        return HEX.formatHex(bytesOf(value));
    }

    /**
     * The key of the least address a range's lower bound {@code value}
     * admits, as stock reads the bound: the address itself when
     * {@code inclusive}, else the one after it; null when it admits none
     * (past the greatest address).
     */
    static String lowerKey(Object value, boolean inclusive) {
        InetAddress address = InetAddresses.forString(text(value));
        if (inclusive) {
            return keyOf(address);
        }
        return address.equals(InetAddressPoint.MAX_VALUE) ? null : keyOf(InetAddressPoint.nextUp(address));
    }

    /** As {@link #lowerKey}, the greatest address a range's upper bound admits; null before the least address. */
    static String upperKey(Object value, boolean inclusive) {
        InetAddress address = InetAddresses.forString(text(value));
        if (inclusive) {
            return keyOf(address);
        }
        return address.equals(InetAddressPoint.MIN_VALUE) ? null : keyOf(InetAddressPoint.nextDown(address));
    }

    private static String keyOf(InetAddress address) {
        return HEX.formatHex(InetAddressPoint.encode(address));
    }

    /**
     * The 16-byte form of the address {@code value} (text, as a query or a
     * hit writes it), the bytes stock sorts and buckets an ip field by.
     */
    static BytesRef encoded(Object value) {
        return new BytesRef(bytesOf(value));
    }

    /** The 16-byte form of the address a key holds. */
    static BytesRef encodedKey(Object key) {
        return new BytesRef(HEX.parseHex(checked(key)));
    }

    /** The address a key holds, as stock prints it ({@code 1.2.3.4}, or IPv6 compressed). */
    static String shown(Object key) {
        return NetworkAddress.format(InetAddressPoint.decode(HEX.parseHex(checked(key))));
    }

    /** The address in {@code encoded}, an address's 16-byte form, as stock prints it. */
    static String shownEncoded(BytesRef encoded) {
        return NetworkAddress.format(InetAddressPoint.decode(BytesRef.deepCopyOf(encoded).bytes));
    }

    private static byte[] bytesOf(Object value) {
        return InetAddressPoint.encode(InetAddresses.forString(text(value)));
    }

    private static String text(Object value) {
        return value instanceof BytesRef b ? b.utf8ToString() : value.toString();
    }

    private static String checked(Object key) {
        String text = key.toString().toLowerCase(Locale.ROOT);
        if (text.length() != KEY_DIGITS) {
            throw new IllegalStateException("[infino] [" + key + "] is not an address key");
        }
        return text;
    }
}
