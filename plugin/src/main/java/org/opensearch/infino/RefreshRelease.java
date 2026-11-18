/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import org.apache.lucene.search.ReferenceManager;
import org.opensearch.index.shard.RefreshListeners;
import org.opensearch.index.translog.Translog;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.function.Supplier;

/**
 * Holds a shard's {@code wait_for} listeners back from a refresh until every
 * read of its table sees what the refresh covers, and waits for that outside
 * Lucene's refresh lock, so concurrent refreshes of one shard do not queue
 * behind the wait.
 *
 * <p>In place of the shard's {@link RefreshListeners} on the external reader,
 * it notes, under the lock, where the translog ends as a refresh begins.
 * {@link #release} runs once the refresh has returned: it commits, waits on
 * the read window ({@link InfinoNative#awaitVisible}), and only then hands
 * {@link RefreshListeners} the location the latest refreshed reader holds,
 * which releases the {@code wait_for} requests it covers and lets later ones
 * through at once.
 */
final class RefreshRelease implements ReferenceManager.RefreshListener {

    private final String table;
    private final RefreshListeners waiting;
    private final Supplier<Translog.Location> lastWrite;
    private final Runnable commit;

    /** Where the translog ended as the refresh under way began; under the refresh lock. */
    private Translog.Location starting;
    /** Where the translog ended as the latest finished refresh began: what its reader holds. */
    private volatile Translog.Location refreshed;
    /** The location last handed to {@link #waiting}. */
    private Translog.Location released;

    /**
     * @param waiting the shard's listeners, or null when it has none
     * @param lastWrite where the shard's translog ends
     * @param commit commits the writes applied so far, ahead of the wait
     */
    RefreshRelease(String table, RefreshListeners waiting, Supplier<Translog.Location> lastWrite, Runnable commit) {
        this.table = table;
        this.waiting = waiting;
        this.lastWrite = lastWrite;
        this.commit = commit;
    }

    @Override
    public void beforeRefresh() {
        starting = lastWrite.get();
    }

    @Override
    public void afterRefresh(boolean didRefresh) {
        Translog.Location at = starting;
        if (at != null && (refreshed == null || refreshed.compareTo(at) < 0)) {
            refreshed = at;
        }
    }

    /**
     * Wait, after a refresh has returned and before it is acknowledged, until
     * every read sees the writes the latest refreshed reader holds, then
     * release the {@code wait_for} requests they cover. Those writes were in
     * the translog before the wait began, so the commit and the wait cover
     * them; a later refresh's release covers anything newer.
     */
    void release() {
        Translog.Location through = refreshed;
        commit.run();
        InfinoNative.awaitVisible(table);
        if (waiting == null) {
            return;
        }
        synchronized (this) {
            if (released != null && (through == null || through.compareTo(released) < 0)) {
                through = released; // a later release went further: keep its location
            }
            Translog.Location at = through;
            try {
                waiting.setCurrentRefreshLocationSupplier(() -> at);
                waiting.beforeRefresh();
                waiting.afterRefresh(true);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            released = at;
        }
    }
}
