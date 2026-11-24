/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import org.opensearch.common.lucene.uid.Versions;
import org.opensearch.index.get.GetResult;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * A default-mode get reports the version, sequence number and primary term
 * the shard's skeleton holds for the document, as a stock get does, with the
 * row those numbers belong to: never the sequence number the table stored
 * beside the row, which a replace leaves at the first write's.
 */
public class InfinoGetVersionTests extends OpenSearchTestCase {

    private static final String INDEX = "logs";
    private static final String TABLE = "os_logs";
    private static final String ID = "d5";
    /** The primary term a fresh shard writes under. */
    private static final long TERM = 1L;
    /** The sequence number of the write that replaced the document, as the shard gave it. */
    private static final long REPLACED_SEQ = 300_000L;
    /** The version that replace took the document to. */
    private static final long REPLACED_VERSION = 2L;
    /** The sequence number the table stores beside a row a replace rewrote: the stamp of the first write, or none. */
    private static final long STORED_SEQ = 0L;

    /** What the skeleton holds of a document's latest write at {@code seq} and {@code version}. */
    private static InfinoEngine.Written found(long seq, long version) {
        return new InfinoEngine.Written(version, seq, TERM);
    }

    /** A row of the document holding {@code source}, with the sequence number the table stored beside it. */
    private static InfinoNative.Doc row(String source) {
        return new InfinoNative.Doc(source, STORED_SEQ, null);
    }

    /** The skeleton's answers for the document, one per read, the last repeated once they run out; null for none. */
    private static Function<String, InfinoEngine.Written> shardAnswering(InfinoEngine.Written... answers) {
        List<InfinoEngine.Written> left = new ArrayList<>(Arrays.asList(answers));
        return id -> left.size() > 1 ? left.remove(0) : left.get(0);
    }

    /** Reads of the document's row, one source per call, the last repeated; each call's ids are kept in {@code asked}. */
    private static Function<Collection<String>, Map<String, InfinoNative.Doc>> rowsAnswering(List<List<String>> asked, String... sources) {
        Deque<String> left = new ArrayDeque<>(List.of(sources));
        return ids -> {
            asked.add(new ArrayList<>(ids));
            String source = left.size() > 1 ? left.poll() : left.peek();
            Map<String, InfinoNative.Doc> out = new HashMap<>();
            for (String id : ids) {
                out.put(id, row(source));
            }
            return out;
        };
    }

    /** The get result a read answers with, from what the skeleton holds and its row. */
    private static GetResult answer(InfinoGetActionFilter.Read read) {
        return InfinoGetActionFilter.result(INDEX, ID, read.doc(), read.written(), InfinoAccess.NONE, InfinoMappingSource.WHOLE, null, null, List.of());
    }

    /**
     * After a replace, the get reported version 1 and sequence number 0, the
     * numbers stored beside the row, so a write conditioned on them
     * conflicted. It reports the skeleton's.
     */
    public void testAReplacedDocumentReportsTheShardsVersionAndSequenceNumber() {
        List<List<String>> asked = new ArrayList<>();
        InfinoGetActionFilter.Read read = InfinoGetActionFilter.reads(
            TABLE,
            List.of(ID),
            shardAnswering(found(REPLACED_SEQ, REPLACED_VERSION)),
            rowsAnswering(asked, "{\"n\":10}")
        ).get(ID);
        GetResult got = answer(read);
        assertTrue(got.isExists());
        assertEquals("{\"n\":10}", got.sourceAsString());
        assertEquals(REPLACED_SEQ, got.getSeqNo());
        assertEquals(REPLACED_VERSION, got.getVersion());
        assertEquals(TERM, got.getPrimaryTerm());
        assertEquals(List.of(List.of(ID)), asked);
    }

    /** A proxy-mode document has no skeleton: it carries the sequence number the table stored, at version 1. */
    public void testAProxyDocumentReportsTheStoredSequenceNumber() {
        GetResult got = InfinoGetActionFilter.result(INDEX, ID, row("{\"n\":1}"), null, InfinoAccess.NONE, InfinoMappingSource.WHOLE, null, null, List.of());
        assertEquals(STORED_SEQ, got.getSeqNo());
        assertEquals(InfinoDocumentVersion.VERSION, got.getVersion());
        assertEquals(InfinoDocumentVersion.PRIMARY_TERM, got.getPrimaryTerm());
    }

    /** A document the skeleton does not hold is missing, and its row is not read. */
    public void testADocumentTheShardDoesNotHoldIsMissing() {
        List<List<String>> asked = new ArrayList<>();
        InfinoGetActionFilter.Read read = InfinoGetActionFilter.reads(TABLE, List.of(ID), shardAnswering((InfinoEngine.Written) null), rowsAnswering(asked, "{}"))
            .get(ID);
        assertNull(read.doc());
        GetResult got = answer(read);
        assertFalse(got.isExists());
        assertEquals(SequenceNumbers.UNASSIGNED_SEQ_NO, got.getSeqNo());
        assertEquals(Versions.NOT_FOUND, got.getVersion());
        assertEquals(List.of(), asked);
    }

    /**
     * A write that lands between the skeleton's read and the row's read: the row
     * read is the newer write's, so the document is read again, and the
     * numbers reported are the ones that row belongs to.
     */
    public void testAWriteLandingWhileTheDocumentIsReadIsReadAgain() {
        List<List<String>> asked = new ArrayList<>();
        InfinoGetActionFilter.Read read = InfinoGetActionFilter.reads(
            TABLE,
            List.of(ID),
            shardAnswering(found(5, 2), found(6, 3)),
            rowsAnswering(asked, "{\"n\":3}")
        ).get(ID);
        GetResult got = answer(read);
        assertEquals(6, got.getSeqNo());
        assertEquals(3, got.getVersion());
        assertEquals("{\"n\":3}", got.sourceAsString());
        assertEquals(2, asked.size());
    }

    /**
     * Writes that keep landing: the last read stands, its row read after its
     * numbers, so never older than them; a write conditioned on them meets
     * the newer write as a conflict.
     */
    public void testWritesThatKeepLandingLeaveTheLastRead() {
        List<List<String>> asked = new ArrayList<>();
        InfinoGetActionFilter.Read read = InfinoGetActionFilter.reads(
            TABLE,
            List.of(ID),
            shardAnswering(found(5, 2), found(6, 3), found(7, 4), found(8, 5)),
            rowsAnswering(asked, "{\"n\":3}", "{\"n\":4}", "{\"n\":5}")
        ).get(ID);
        assertEquals(7, read.written().seqNo());
        assertEquals("{\"n\":5}", read.doc().source());
        assertEquals(3, asked.size());
    }

}
