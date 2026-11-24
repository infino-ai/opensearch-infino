/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import com.sun.jna.Library;
import com.sun.jna.Native;
import com.sun.jna.Pointer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.OpenSearchException;
import org.opensearch.common.cache.Cache;
import org.opensearch.common.cache.CacheBuilder;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.index.Index;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.seqno.SequenceNumbers;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BooleanSupplier;
import java.util.function.UnaryOperator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Loads the Infino cdylib once per node and holds a single connection.
 *
 * <p>The connection goes to the hosted service when the node is configured
 * with an account (see {@link InfinoAccount}): every table operation then
 * forwards there through the engine's own hosted client, and the node holds
 * no table data. Without one it goes to the node's own URI. The node's own
 * settings decide at boot; the account route can change it for the node's
 * life, and a change reopens the connection.
 *
 * <p>The engine and the bridge are thread-safe, so nothing here serializes
 * callers: a batch append and the queries on the node run concurrently.
 * Column descriptions are cached per table and dropped whenever the table's
 * schema may have moved (a mapping applied, a batch appended, a drop).
 */
public final class InfinoNative {
    private static final Logger log = LogManager.getLogger(InfinoNative.class);
    // Path to the native bridge, set with -Dinfino.lib=/abs/path (see scripts/run.sh);
    // the bare name only resolves if it is on java.library.path.
    private static final String LIB = System.getProperty("infino.lib", "libinfino_capi.so");
    // Where the tables live when the cluster has no hosted account: the node's own
    // URI, -Dinfino.uri=file:///abs/path or s3://bucket/prefix for tables that
    // survive restart. The default is in-process and non-persistent.
    private static final String NODE_URI = System.getProperty("infino.uri", "memory://");
    /** The URIs of a catalog held in the node's memory, as the engine reads them: there is no storage to check. */
    private static final Set<String> MEMORY_URIS = Set.of("memory://", "memory:", "memory");
    /** The scheme of local storage; a URI with no scheme at all is a local path too. */
    private static final String FILE_SCHEME = "file://";
    /** What separates a URI's scheme from the rest. */
    private static final String SCHEME_END = "://";
    /**
     * How long the node's start waits for its storage check ({@link #checkStorage}),
     * the hosted open included: a storage that has not answered by then is reported
     * and the node keeps starting, rather than waiting out the client's own retries
     * and timeouts, which can run for minutes.
     */
    static final TimeValue STORAGE_CHECK_TIMEOUT = TimeValue.timeValueSeconds(30);
    /** What a user and password in a URI, or the key, are shown as in a message. */
    private static final String HIDDEN = "***";
    /** A user, and a password, between a URI's scheme and its host. */
    private static final Pattern URI_USER = Pattern.compile("://[^/@\\s]*@");
    /**
     * For tests only: the read staleness window the node's connections read
     * under, in milliseconds, in place of the engine's default (0 for that).
     * A wide window makes a read that should have read the table's latest
     * commit, and read the node's own connection instead, answer from the
     * commit before every time rather than now and then.
     */
    private static final long READ_STALENESS_MS = Long.getLong("infino.engine.read.staleness.ms", 0L);

    /**
     * What a connection is opened to: the URI, its bearer key (null for none:
     * the engine then reads {@code INFINO_API_KEY}), and how to name it in a
     * message, which never carries the key.
     */
    record Target(String uri, String apiKey, boolean hosted, String name) {
        /** The node's own URI, with no key. */
        static Target node() {
            return new Target(NODE_URI, null, false, NODE_URI);
        }

        /** A hosted account: the service's base URL, the database, and the key. */
        static Target hosted(String url, String database, String apiKey) {
            String base = InfinoAccount.baseUrl(url);
            return new Target(base + "/" + database, apiKey, true, base + " database " + database);
        }
    }

    /**
     * The status a read by id that fails is answered with: 503, as a stock
     * read that no copy of its shard could serve is, so the client may try
     * it again. Never the answer of a missing document.
     */
    private static final RestStatus READ_FAILED = RestStatus.SERVICE_UNAVAILABLE;

    private static volatile InfinoLib lib;
    /** Where the engine caches what it reads from the node's own storage (see {@link InfinoPlugin#CACHE_DIR}); null for none. */
    private static volatile String cacheDir;
    private static volatile Pointer conn;
    /** See {@link #opens()}; written under the class lock that opens a connection. */
    private static volatile long opens;
    /** The target {@link #conn} was opened to. */
    private static volatile Target opened;
    /** The target the node is configured for: its own URI until the settings or the account route say otherwise. */
    private static volatile Target wanted = Target.node();
    /** How long a read on the hosted service may trail a write ({@link InfinoPlugin#PLATFORM_READ_WINDOW}), in nanoseconds. */
    private static volatile long readWindowNanos = InfinoPlugin.PLATFORM_READ_WINDOW.get(Settings.EMPTY).nanos();
    /** When each table was last written on the hosted service ({@link System#nanoTime}), until a wait sees the window pass. */
    private static final Map<String, Long> WRITTEN = new ConcurrentHashMap<>();
    private static final Map<String, Map<String, Column>> COLUMNS = new ConcurrentHashMap<>();
    /** What each table's mapping declares, from the mapping it was last declared with. */
    private static final Map<String, Mapping> MAPPINGS = new ConcurrentHashMap<>();
    /** A mapping's key for a field's multi-fields. */
    private static final String MULTI_FIELDS = "fields";
    /** A mapping's keys for its fields, a field's type, an alias's target, and a date's format. */
    private static final String PROPERTIES = "properties";
    private static final String TYPE = "type";
    private static final String ALIAS_TYPE = "alias";
    private static final String ALIAS_PATH = "path";
    private static final String FORMAT = "format";
    /** The mapping types whose values are dates. */
    private static final Set<String> DATE_TYPES = Set.of("date", "date_nanos");
    /** The mapping types whose values a sort compares as 32-bit floats (stock's float comparator). */
    private static final Set<String> FLOAT_TYPES = Set.of("float", "half_float");
    /** The integral field types narrower than a long, whose sort stock compares as 32-bit ints. */
    private static final Set<String> INT_TYPES = Set.of("integer", "short", "byte");
    /** The type of a multi-field that holds its field's value whole, the exact form of a text field. */
    private static final String KEYWORD_TYPE = "keyword";
    /** The other type that holds a value whole: a wildcard field compares the value as a keyword one does. */
    private static final String WILDCARD_TYPE = "wildcard";
    /** A keyword or wildcard field's normalizer, applied to its values and to the terms a query compares them with. */
    private static final String NORMALIZER = "normalizer";
    private static final String USE_SIMILARITY = "use_similarity";
    /** A field's own similarity, which overrides the index's default. */
    private static final String SIMILARITY = "similarity";
    /** The similarity a field scores by unless it or its index names another. */
    static final String BM25 = "BM25";
    /** The count's key in {@link #valueCounts}'s rows, as the engine names it. */
    static final String VALUE_COUNT = "count(*)";
    /** How long a prefetched source waits for its fetch phase. */
    private static final TimeValue SOURCE_PREFETCH_TTL = TimeValue.timeValueSeconds(30);
    /** Bytes of prefetched sources held at once across requests. */
    private static final long SOURCE_PREFETCH_BYTES = 64L << 20;
    /**
     * Sources the query phase fetched for its page, read back by the fetch
     * phase one hit at a time: one statement per page instead of one per hit.
     * Keyed by table and id; a source does not change under an id except
     * through an update, which replays as its own write, so a stale entry
     * can only be served inside the TTL.
     */
    private static final Cache<String, String> SOURCES = CacheBuilder.<String, String>builder()
        .setExpireAfterWrite(SOURCE_PREFETCH_TTL)
        .setMaximumWeight(SOURCE_PREFETCH_BYTES)
        .weigher((key, value) -> key.length() + value.length())
        .build();

    /**
     * One column of a table, as the bridge describes it. {@code mask} is the
     * security plugin's field masking for the request, when the field is
     * masked for the user: how a value of the column is hashed before it
     * leaves the node (null when it is not). {@code dateFormat} is the format
     * a date column's mapping declares (null for the default one). {@code elem}
     * is a list column's element type (null for any other column).
     * {@code similarity} is whether its mapping is a keyword field with
     * {@code use_similarity}, whose terms Lucene scores by BM25.
     * {@code otherSimilarity} is whether its field scores by a similarity
     * other than the default BM25: its mapping names one, or the index's
     * default is another.
     */
    public record Column(
        String name,
        String type,
        boolean fts,
        boolean vector,
        UnaryOperator<String> mask,
        String dateFormat,
        String elem,
        boolean similarity,
        boolean otherSimilarity
    ) {
        public Column(
            String name,
            String type,
            boolean fts,
            boolean vector,
            UnaryOperator<String> mask,
            String dateFormat,
            String elem,
            boolean similarity
        ) {
            this(name, type, fts, vector, mask, dateFormat, elem, similarity, false);
        }

        public Column(String name, String type, boolean fts, boolean vector) {
            this(name, type, fts, vector, null, null, null, false);
        }

        public Column(String name, String type, boolean fts, boolean vector, UnaryOperator<String> mask, String dateFormat, String elem) {
            this(name, type, fts, vector, mask, dateFormat, elem, false);
        }

        /** A list column of {@code elem} values. */
        static Column list(String name, String elem) {
            return new Column(name, "list", false, false, null, null, elem);
        }

        /** This column as the request sees it, its values masked by {@code mask}. */
        Column masked(UnaryOperator<String> mask) {
            return new Column(name, type, fts, vector, mask, dateFormat, elem, similarity, otherSimilarity);
        }

        /** This column with the date format its mapping declares. */
        Column dated(String dateFormat) {
            return new Column(name, type, fts, vector, mask, dateFormat, elem, similarity, otherSimilarity);
        }

        /** This column as a keyword field with {@code use_similarity} maps it. */
        Column scoredBySimilarity() {
            return new Column(name, type, fts, vector, mask, dateFormat, elem, true, otherSimilarity);
        }

        /** This column as a field scored by a similarity other than the default BM25. */
        Column scoredByOtherSimilarity() {
            return new Column(name, type, fts, vector, mask, dateFormat, elem, similarity, true);
        }

        /**
         * Whether Lucene scores a term on this field by BM25 rather than at a
         * constant: a boolean field, whose term query is a plain one, and a
         * keyword field with {@code use_similarity}, under the default BM25.
         * Under another similarity such a term keeps its boost as its score:
         * exactly stock's under {@code boolean}, which scores a matching term
         * its boost, and the table's score from before for the others, which
         * it does not compute.
         */
        boolean scoresTermsByBm25() {
            return (isBoolean() || similarity) && !otherSimilarity;
        }

        /**
         * This column read by its whole value, as a keyword multi-field of its
         * field reads it: no text index, so a term is the value, not one of its
         * words. Its {@code use_similarity} and similarity are its own
         * mapping's, not its field's: neither here.
         */
        Column exact() {
            return fts || similarity || otherSimilarity ? new Column(name, type, false, vector, mask, dateFormat, elem, false, false) : this;
        }

        /** A list column as one of its values: the column typed as its element, for a literal or a comparison. */
        Column element() {
            return isList() ? new Column(name, elem, false, false, mask, dateFormat, null, similarity, otherSimilarity) : this;
        }

        public boolean isList() {
            return "list".equals(type);
        }

        public boolean masked() {
            return mask != null;
        }

        public boolean isString() {
            return "str".equals(type);
        }

        public boolean isNumeric() {
            return "i64".equals(type) || "f64".equals(type);
        }

        public boolean isTimestamp() {
            return "ts".equals(type);
        }

        public boolean isBoolean() {
            return "bool".equals(type);
        }
    }

    /** One document the bridge refused, with the index it had in the batch. */
    public record Failure(int index, String reason) {}

    /** Outcome of a batch append. */
    public record BatchResult(int ok, List<Failure> failed) {}

    /**
     * The bridge answered with an error object. Carries the HTTP status a
     * hosted refusal came with (a key refused, an account at a limit), so the
     * caller is answered with the same one and the service's own message;
     * anything else is an internal error. Says too whether the bridge marked
     * the error as the hosted service not answering ({@link #unavailable}).
     */
    public static final class InfinoException extends OpenSearchException {
        private final RestStatus status;
        private final boolean unavailable;

        InfinoException(String message) {
            this(message, RestStatus.INTERNAL_SERVER_ERROR);
        }

        InfinoException(String message, RestStatus status) {
            this(message, status, false);
        }

        InfinoException(String message, RestStatus status, boolean unavailable) {
            super(message);
            this.status = status;
            this.unavailable = unavailable;
        }

        /**
         * {@code message} with the status of the bridge's error object
         * {@code error}, else {@code fallback}, and its {@code unavailable}
         * mark.
         */
        static InfinoException of(String message, Map<String, Object> error, RestStatus fallback) {
            return new InfinoException(message, statusOf(error, fallback), Boolean.TRUE.equals(error.get("unavailable")));
        }

        /**
         * Whether the hosted service could not be reached or did not answer:
         * a transport failure, or a 502, 503 or 504 from it, whatever status
         * the error carries. A write that met it may have landed or not.
         */
        public boolean unavailable() {
            return unavailable;
        }

        @Override
        public RestStatus status() {
            return status;
        }
    }

    private InfinoNative() {}

    /** The bridge library, loaded once per node. */
    private static InfinoLib lib() {
        InfinoLib loaded = lib;
        if (loaded == null) {
            synchronized (InfinoNative.class) {
                loaded = lib;
                if (loaded == null) {
                    // Strings go to the engine as UTF-8 whatever the node's locale: JNA's
                    // default is the platform encoding, which under LANG=C turns every
                    // non-ASCII character of a statement into '?'.
                    loaded = Native.load(LIB, InfinoLib.class, Map.of(Library.OPTION_STRING_ENCODING, StandardCharsets.UTF_8.name()));
                    if (READ_STALENESS_MS > 0) {
                        log.warn("[infino] connections read under a staleness window of {} ms, for tests", READ_STALENESS_MS);
                        loaded.infino_set_read_staleness_ms(READ_STALENESS_MS);
                    }
                    lib = loaded;
                }
            }
        }
        return loaded;
    }

    /**
     * For tests that stand a bridge of their own in for the library (null to
     * load the library again): the next call opens its connection through it.
     */
    static synchronized void useLib(InfinoLib bridge) {
        lib = bridge;
        conn = null;
        opened = null;
    }

    /**
     * Open the connection the node is configured for, if it is not the one
     * open. A connection in use is not closed when another replaces it: a
     * call in flight on another thread still holds it, and an account
     * changes a few times in a node's life at most.
     */
    public static void init() {
        Target target = wanted;
        if (conn != null && target.equals(opened)) {
            return;
        }
        synchronized (InfinoNative.class) {
            if (conn != null && target.equals(opened)) {
                return;
            }
            InfinoLib loaded = lib();
            Pointer p = loaded.infino_open_with(target.uri(), target.apiKey(), target.hosted() ? null : cacheDir);
            if (p == null) {
                throw openFailure(loaded, target);
            }
            if (conn != null) {
                log.info("[infino] tables are now served from {} (were from {})", target.name(), opened.name());
            } else {
                log.info("[infino] tables are served from {}", target.name());
            }
            boolean switched = conn != null;
            opens++;
            COLUMNS.clear();
            SOURCES.invalidateAll();
            InfinoRequestCache.changedAll();
            conn = p;
            opened = target;
            if (switched) {
                InfinoRequestCache.clearShardCaches();
            }
        }
    }

    /**
     * How many connections the node has opened. A connection holds what
     * each table was declared with (how its fields keep their values, and
     * for an attached table that it is one and what it is read with), and a
     * new one holds none of it: whoever remembers that a table is declared
     * ({@link InfinoTables}) declares it again when this has moved.
     */
    static long opens() {
        return opens;
    }

    /** Why an open returned null, as the exception the caller gets. */
    private static InfinoException openFailure(InfinoLib loaded, Target target) {
        String why = take(loaded, loaded.infino_open_error());
        if (why == null) {
            return new InfinoException("cannot open " + target.name());
        }
        Map<String, Object> error = object(why);
        return new InfinoException("cannot open " + target.name() + ": " + error.get("error"), statusOf(error));
    }

    /**
     * Take {@code target} as what the node is configured for: from the node's
     * settings at boot, or from the account route for the node's life. A
     * connection already in use is reopened at once so the next call finds
     * the new one; a failure there is reported to the caller and the next
     * call retries.
     */
    static void configure(Target target) {
        if (target.equals(wanted)) {
            return;
        }
        wanted = target;
        if (conn != null) {
            init();
        }
    }

    /** Take {@code dir} as the node's cache directory, at boot, before the first open; null for none. */
    static void cacheDir(String dir) {
        cacheDir = dir;
    }

    /** The target the node is configured for. */
    static Target target() {
        return wanted;
    }

    /** Why the node's storage check failed, and whether the failure will not pass on its own. */
    record StorageFailure(String reason, boolean permanent) {}

    /**
     * Open the storage the node is configured for and check once that the
     * node can use it, as the node starts. Opening the node's own storage
     * reads nothing, so a store it cannot use would otherwise fail each table
     * operation on its own, the security plugin's index first, and the node
     * would never become ready with nothing in its log naming the cause.
     *
     * <p>Waits at most {@link #STORAGE_CHECK_TIMEOUT}. On a failure it logs one
     * ERROR naming the storage, the reason and what to check. It stops the node
     * (throws) only for a failure that will not pass on its own: the key was
     * refused, the storage refused the node's credentials or its user. On any
     * other failure (a timeout, a 5xx, a dropped connection) the node keeps
     * starting and each index tries the storage again when it is used. While
     * a hosted open that outlived the wait is still running, it holds the lock
     * {@link #init} opens under, so an index's {@code init()} waits for that
     * open to return (up to the client's read timeout) rather than trying
     * again at once. A node in memory has nothing to check.
     *
     * <p>The bridge library is loaded here, on the starting thread, before the
     * check: a library that cannot be loaded (a wrong {@code -Dinfino.lib})
     * stops the node with the loader's own error, as an {@link Error} raised
     * on the check's thread does.
     *
     * @return the ERROR it logged and the node did not stop for; null when the storage is usable
     */
    static String checkStorage() {
        return checkStorage(STORAGE_CHECK_TIMEOUT);
    }

    static String checkStorage(TimeValue timeout) {
        lib();
        Target target = wanted;
        if (MEMORY_URIS.contains(target.uri())) {
            return null;
        }
        StorageFailure failure = within(timeout, () -> probeStorage(target));
        if (failure == null) {
            log.info("[infino] the storage at {} is usable", scrubbed(target, target.name()));
            return null;
        }
        String message = unusableStorage(target, failure);
        log.error(message);
        if (failure.permanent()) {
            throw new IllegalStateException(message);
        }
        return message;
    }

    /**
     * Run {@code check} on a thread of its own and wait at most {@code timeout}
     * for it: the hosted open and a store's own retries can take minutes, and
     * the node's start should not. A check still running is left to finish.
     * An {@link Error} the check raised (a library that does not link, say) is
     * thrown again here: no retry fixes it, so it stops the node.
     */
    private static StorageFailure within(TimeValue timeout, Callable<StorageFailure> check) {
        FutureTask<StorageFailure> task = new FutureTask<>(check);
        Thread thread = new Thread(task, "infino-storage-check");
        thread.setDaemon(true);
        thread.start();
        try {
            return task.get(timeout.millis(), TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            return new StorageFailure("it did not answer within " + timeout, false);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return new StorageFailure("the check was interrupted", false);
        } catch (ExecutionException e) {
            if (e.getCause() instanceof Error error) {
                throw error;
            }
            return new StorageFailure(String.valueOf(e.getCause()), false);
        }
    }

    /**
     * Open {@code target} and, for the node's own storage, run the bridge's
     * check of it. For a hosted account the open is the check: it lists the
     * database's tables before it hands the connection back.
     */
    private static StorageFailure probeStorage(Target target) {
        try {
            init();
        } catch (InfinoException e) {
            String opening = "cannot open " + target.name() + ": ";
            String why = e.getMessage().startsWith(opening) ? e.getMessage().substring(opening.length()) : e.getMessage();
            return new StorageFailure(why, refused(e.status()));
        }
        if (target.hosted()) {
            return null;
        }
        InfinoLib loaded = lib();
        return storageFailure(take(loaded, loaded.infino_check_storage(conn)));
    }

    /**
     * The failure in the bridge's answer to {@code infino_check_storage}: none
     * for "", the reason for {@code {"error", "permanent"}}. No answer at all
     * (the bridge failed) is a failure too, though not one that stops the node.
     */
    static StorageFailure storageFailure(String answer) {
        if (answer == null) {
            return new StorageFailure("the bridge returned no answer", false);
        }
        if (answer.isEmpty()) {
            return null;
        }
        Map<String, Object> error = object(answer);
        Object reason = error.get("error");
        return new StorageFailure(reason == null ? answer : reason.toString(), Boolean.TRUE.equals(error.get("permanent")));
    }

    /** Whether the service refused the key: it will not take it however often it is sent. */
    private static boolean refused(RestStatus status) {
        return status == RestStatus.UNAUTHORIZED || status == RestStatus.FORBIDDEN;
    }

    /**
     * The message the node logs when it cannot use its storage: the storage,
     * the reason, what to check for its kind, and whether the node stops.
     * Never shows a user or password in a URI, or the key.
     */
    static String unusableStorage(Target target, StorageFailure failure) {
        String then = failure.permanent()
            ? " The node stops: every Infino-backed index it serves, the security plugin's included, lives there."
            : " The node keeps starting, and each index tries the storage again when it is used,"
                + " after any open still in progress returns.";
        return scrubbed(
            target,
            "[infino] this node cannot use its storage at "
                + target.name()
                + ": "
                + failure.reason()
                + ". "
                + storageHint(target, failure)
                + then
        );
    }

    /** What to check for a storage of {@code target}'s kind that failed with {@code failure}. */
    static String storageHint(Target target, StorageFailure failure) {
        String uri = target.uri();
        if (target.hosted()) {
            if (failure.permanent()) {
                return "The service refused the key. A key belongs to one platform URL: check that "
                    + InfinoPlugin.PLATFORM_URL.getKey()
                    + " is the one the key was made on.";
            }
            return "Check that the node can reach the service.";
        }
        if (uri.startsWith("s3://")) {
            return "Check the node's AWS credentials and that they may list the bucket. On Docker on EC2 with an instance role, "
                + "set the instance metadata hop limit to 2: at 1 the container gets no credentials.";
        }
        if (local(uri)) {
            return "Check that the directory exists and that the user the node runs as may read and write it.";
        }
        return "Check the node's credentials for this storage and that they may list it.";
    }

    /** Whether {@code uri} names local storage: {@code file://}, or a path with no scheme. */
    private static boolean local(String uri) {
        return uri.startsWith(FILE_SCHEME) || (!uri.contains(SCHEME_END) && !MEMORY_URIS.contains(uri));
    }

    /** {@code text} without any user or password a URI in it carries, or {@code target}'s key. */
    static String scrubbed(Target target, String text) {
        String out = URI_USER.matcher(text).replaceAll(Matcher.quoteReplacement("://" + HIDDEN + "@"));
        String key = target.apiKey();
        return key == null || key.isEmpty() ? out : out.replace(key, HIDDEN);
    }

    /** Whether the tables are served from the hosted service rather than the node's own storage. */
    public static boolean hosted() {
        return wanted.hosted();
    }

    /** Take {@code window} as how long a hosted read may trail a write, at boot. */
    static void readWindow(TimeValue window) {
        readWindowNanos = window.nanos();
    }

    /**
     * Note a write to {@code name}. On the hosted service a read may be
     * answered from a snapshot taken before it, for up to the read window;
     * the node's own storage answers every read with what was written.
     */
    private static void wrote(String name) {
        if (hosted() && readWindowNanos > 0) {
            wrote(name, System.nanoTime());
        }
    }

    /**
     * Note a write to {@code name} that completed at {@code at}. Of two
     * writes noted at once, the later stays, whichever is noted last, so a
     * wait never ends before the later one is visible.
     */
    static void wrote(String name, long at) {
        WRITTEN.merge(name, at, (a, b) -> b - a > 0 ? b : a);
    }

    /**
     * Wait until every read of {@code name} sees what this node has written
     * to it: until the read window has passed since its last write. Returns
     * at once when the window has passed, or the table is not hosted, or
     * this node has not written it. What a refresh waits on, so that a
     * search after it ({@code refresh=true}, {@code wait_for}) finds the
     * writes it covered. An interrupt does not cut the wait short, as the
     * writes would then be released unseen; it is kept for the caller.
     */
    public static void awaitVisible(String name) {
        Long at = WRITTEN.get(name);
        if (at == null) {
            return;
        }
        boolean interrupted = false;
        long left;
        while ((left = untilVisible(at, readWindowNanos, System.nanoTime())) > 0) {
            try {
                TimeUnit.NANOSECONDS.sleep(left);
            } catch (InterruptedException e) {
                interrupted = true;
            }
        }
        WRITTEN.remove(name, at);
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    /** Nanoseconds from {@code now} until a write at {@code writtenAt} is visible to every read; zero or less once it is. */
    static long untilVisible(long writtenAt, long window, long now) {
        return writtenAt + window - now;
    }

    /** How the tables are served, for a status report. */
    static String serving() {
        return wanted.name();
    }

    /**
     * Ask the hosted service at {@code baseUrl} for a trial account, with
     * {@code database} as its first database: the service's response. A
     * refusal is thrown with the service's status and message.
     */
    static Map<String, Object> trial(String baseUrl, String database) {
        InfinoLib loaded = lib();
        String out = take(loaded, loaded.infino_trial(baseUrl, database));
        if (out == null) {
            throw new InfinoException("the trial call returned nothing");
        }
        Map<String, Object> response = object(out);
        if (response.containsKey("error")) {
            throw new InfinoException(String.valueOf(response.get("error")), statusOf(response));
        }
        return response;
    }

    /**
     * Open {@code database} on the hosted service with {@code apiKey} once and
     * close it again: a key the service refuses fails here, with its status.
     */
    static void probe(String baseUrl, String database, String apiKey) {
        InfinoLib loaded = lib();
        String uri = InfinoAccount.baseUrl(baseUrl) + "/" + database;
        Pointer p = loaded.infino_open_with(uri, apiKey, null);
        if (p == null) {
            throw openFailure(loaded, new Target(uri, apiKey, true, baseUrl + " database " + database));
        }
        loaded.infino_close(p);
    }

    /** The HTTP status an error object carries, else an internal error. */
    private static RestStatus statusOf(Map<String, Object> error) {
        return statusOf(error, RestStatus.INTERNAL_SERVER_ERROR);
    }

    /** The status an error object carries, else {@code fallback}. */
    private static RestStatus statusOf(Map<String, Object> error, RestStatus fallback) {
        Object status = error.get("status");
        RestStatus known = status instanceof Number n ? RestStatus.fromCode(n.intValue()) : null;
        return known == null ? fallback : known;
    }

    /**
     * The Infino table of an index: the one named by {@code index.infino.table}
     * when set, else the index name. Named by the index and not by its UUID,
     * so an index that comes back on a fresh node (see {@link InfinoRegistry})
     * finds the rows its predecessor wrote; an index deleted on purpose drops
     * its table, so a recreated one starts empty.
     */
    public static String tableFor(Index index, Settings indexSettings) {
        return tableFor(index.getName(), indexSettings);
    }

    /** The Infino table of an index of this name and these settings, as {@link #tableFor(Index, Settings)} names it. */
    public static String tableFor(String indexName, Settings indexSettings) {
        String explicit = InfinoPlugin.INFINO_TABLE.get(indexSettings);
        if (!explicit.isEmpty()) {
            return explicit;
        }
        return tableFor(indexName);
    }

    /** The table an index of this name is served from when none is named explicitly. */
    public static String tableFor(String indexName) {
        // An index name may carry characters a table name may not (the security
        // plugin's and Dashboards' own indices start with a dot); each becomes '_'.
        // Index names are lower case already, as the engine's SQL layer folds them.
        return TABLE_PREFIX + TABLE_NAME_OTHER.matcher(indexName).replaceAll("_");
    }

    /** What the engine does not take in a table name: anything outside letters, digits, '_' and '-'. */
    private static final Pattern TABLE_NAME_OTHER = Pattern.compile("[^A-Za-z0-9_-]");
    /** Every table behind an index starts with this, so the registry and the tables of other users of a database stay apart. */
    private static final String TABLE_PREFIX = "os_";

    /** Whether the table name came from {@code index.infino.table}, in which case deleting the index leaves it. */
    public static boolean tableIsExplicit(Settings indexSettings) {
        return !InfinoPlugin.INFINO_TABLE.get(indexSettings).isEmpty();
    }

    private static String take(Pointer p) {
        return take(lib, p);
    }

    private static String take(InfinoLib loaded, Pointer p) {
        if (p == null) {
            return null;
        }
        String s = p.getString(0, "UTF-8");
        loaded.infino_free_string(p);
        return s;
    }

    private static String takeOrThrow(Pointer p) {
        return takeOrThrow(p, RestStatus.INTERNAL_SERVER_ERROR);
    }

    /** The bridge's answer; an error object is thrown with its status, else {@code fallback}. */
    private static String takeOrThrow(Pointer p, RestStatus fallback) {
        String s = take(p);
        if (s == null) {
            throw new InfinoException("the bridge returned null", fallback);
        }
        if (s.startsWith("{\"error\"")) {
            Map<String, Object> error = object(s);
            Object message = error.get("error");
            throw InfinoException.of(message == null ? s : message.toString(), error, fallback);
        }
        return s;
    }

    // ---- JSON ----

    @SuppressWarnings("unchecked")
    static Map<String, Object> object(String json) {
        try (XContentParser parser = parser(json)) {
            return parser.map();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @SuppressWarnings("unchecked")
    static List<Map<String, Object>> rows(String json) {
        try (XContentParser parser = parser(json)) {
            List<Object> list = parser.list();
            List<Map<String, Object>> out = new ArrayList<>(list.size());
            for (Object o : list) {
                out.add((Map<String, Object>) o);
            }
            return out;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static XContentParser parser(String json) throws IOException {
        return JsonXContent.jsonXContent.createParser(
            NamedXContentRegistry.EMPTY,
            DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
            json
        );
    }

    // ---- schema ----

    /** Create the table from a mapping, or add the mapped fields it lacks, under the default BM25 similarity. */
    public static void ensureTable(String name, String mappingJson) {
        ensureTable(name, mappingJson, true);
    }

    /**
     * Create the table from a mapping, or add the mapped fields it lacks.
     * {@code defaultBm25} is whether the index's default similarity is the
     * default BM25 ({@link Mapping}).
     */
    public static void ensureTable(String name, String mappingJson, boolean defaultBm25) {
        ensureTable(name, mappingJson, defaultBm25, false);
    }

    /**
     * Why an index with this mapping cannot be attached to table
     * {@code name} ({@link InfinoPlugin#INFINO_ATTACHED}), or null when it
     * can: the table is missing, is one the plugin wrote, or disagrees with
     * the mapping. Asked when the index is created, so the request is
     * answered with the reason; nothing is declared and nothing changes.
     * Null too when the table could not be read at all, which says nothing
     * about it: the index's shard declares the table when it starts and
     * reports what it finds then.
     */
    public static String attachRefusal(String name, String mappingJson) {
        init();
        try {
            Object refused = object(takeOrThrow(lib.infino_attach_check(conn, name, mappingJson))).get("refused");
            return refused == null ? null : refused.toString();
        } catch (InfinoException e) {
            log.warn("[infino] table {} could not be read to check an attach to it: {}", name, e.getMessage());
            return null;
        }
    }

    /**
     * As {@link #ensureTable(String, String, boolean)}, or for an
     * {@code attached} index ({@link InfinoPlugin#INFINO_ATTACHED}) declare
     * the table it reads without creating or changing it: a missing table,
     * or a mapped field the table has no column for, is refused.
     */
    public static void ensureTable(String name, String mappingJson, boolean defaultBm25, boolean attached) {
        init();
        COLUMNS.remove(name);
        InfinoRequestCache.changed(name);
        try {
            takeOrThrow(attached ? lib.infino_attach_table(conn, name, mappingJson) : lib.infino_ensure_table(conn, name, mappingJson));
        } finally {
            InfinoRequestCache.changed(name);
            wrote(name);
        }
        MAPPINGS.put(name, Mapping.of(mappingJson, defaultBm25));
        // A lookup while the table was declared cached its columns without what the mapping adds.
        COLUMNS.remove(name);
    }

    /**
     * What a mapping declares beyond the columns: every field path, of any
     * type, a multi-field and an alias included ({@link #maps}); each field
     * alias's dotted path to the path it points at, since an alias has no
     * column of its own and OpenSearch reads it as the field it names; each
     * keyword multi-field's dotted path to its field's, since the field's
     * column holds the value the multi-field indexes whole (a log mapping's
     * {@code request.raw} under the text field {@code request}, as a dynamic
     * mapping's {@code name.keyword}); each date field's declared format;
     * each {@code float} or {@code half_float} field ({@code floatFields});
     * and each {@code integer}, {@code short} or {@code byte} field
     * ({@code intFields}).
     * A wildcard multi-field reads as a keyword one. {@code multiFields} are
     * every multi-field's path, and {@code normalized} the keyword and
     * wildcard fields with a normalizer, whose values the table keeps as
     * written: a query on either is refused ({@link #unservedField}).
     */
    record Mapping(
        Set<String> paths,
        Map<String, String> aliases,
        Map<String, String> keywordFields,
        Map<String, String> dateFormats,
        Set<String> similarityFields,
        Set<String> otherSimilarityFields,
        Set<String> floatFields,
        Set<String> intFields,
        Set<String> multiFields,
        Set<String> normalized,
        boolean defaultBm25
    ) {
        /** {@link #of(String, boolean)} under the default BM25 similarity. */
        static Mapping of(String mappingJson) {
            return of(mappingJson, true);
        }

        /**
         * The mapping {@code mappingJson} declares, its fields' similarity
         * read under an index whose default is the default BM25 when
         * {@code defaultBm25}: each field scoring by another similarity is in
         * {@code otherSimilarityFields}.
         */
        @SuppressWarnings("unchecked")
        static Mapping of(String mappingJson, boolean defaultBm25) {
            Mapping out = new Mapping(
                new HashSet<>(),
                new LinkedHashMap<>(),
                new LinkedHashMap<>(),
                new LinkedHashMap<>(),
                new HashSet<>(),
                new HashSet<>(),
                new HashSet<>(),
                new HashSet<>(),
                new HashSet<>(),
                new HashSet<>(),
                defaultBm25
            );
            Object root = object(mappingJson);
            // The mapping as the cluster keeps it sits under its type name.
            while (root instanceof Map<?, ?> m && !m.containsKey(PROPERTIES) && m.size() == 1) {
                root = m.values().iterator().next();
            }
            if (root instanceof Map<?, ?> m && m.get(PROPERTIES) instanceof Map<?, ?> properties) {
                out.walk((Map<String, Object>) properties, "");
            }
            return out;
        }

        @SuppressWarnings("unchecked")
        private void walk(Map<String, Object> properties, String prefix) {
            for (Map.Entry<String, Object> entry : properties.entrySet()) {
                if (!(entry.getValue() instanceof Map<?, ?> field)) {
                    continue;
                }
                String path = prefix.isEmpty() ? entry.getKey() : prefix + "." + entry.getKey();
                paths.add(path);
                if (ALIAS_TYPE.equals(field.get(TYPE)) && field.get(ALIAS_PATH) instanceof String target) {
                    aliases.put(path, target);
                }
                // An object field has no type.
                if (field.get(TYPE) instanceof String type && DATE_TYPES.contains(type) && field.get(FORMAT) instanceof String format) {
                    dateFormats.put(path, format);
                }
                if (field.get(TYPE) instanceof String type && FLOAT_TYPES.contains(type)) {
                    floatFields.add(path);
                }
                if (field.get(TYPE) instanceof String type && INT_TYPES.contains(type)) {
                    intFields.add(path);
                }
                if (scoresBySimilarity(field)) {
                    similarityFields.add(path);
                }
                if (scoresByOtherSimilarity(field)) {
                    otherSimilarityFields.add(path);
                }
                if (normalizes(field)) {
                    normalized.add(path);
                }
                if (field.get(MULTI_FIELDS) instanceof Map<?, ?> multi) {
                    for (Map.Entry<?, ?> sub : multi.entrySet()) {
                        String subPath = path + "." + sub.getKey();
                        paths.add(subPath);
                        multiFields.add(subPath);
                        if (sub.getValue() instanceof Map<?, ?> subField && normalizes(subField)) {
                            normalized.add(subPath);
                        } else if (sub.getValue() instanceof Map<?, ?> subField
                            && (KEYWORD_TYPE.equals(subField.get(TYPE)) || WILDCARD_TYPE.equals(subField.get(TYPE)))) {
                            keywordFields.put(subPath, path);
                            if (scoresBySimilarity(subField)) {
                                similarityFields.add(subPath);
                            }
                            if (scoresByOtherSimilarity(subField)) {
                                otherSimilarityFields.add(subPath);
                            }
                        }
                    }
                }
                if (field.get(PROPERTIES) instanceof Map<?, ?> inner) {
                    walk((Map<String, Object>) inner, path);
                }
            }
        }

        /**
         * Whether {@code field} scores by a similarity other than the default
         * BM25: the one its mapping names, or the index's default when it
         * names none. A named similarity is the default BM25 only by that
         * name; any other, a custom BM25 with its own parameters included,
         * scores otherwise.
         */
        private boolean scoresByOtherSimilarity(Map<?, ?> field) {
            Object named = field.get(SIMILARITY);
            return named == null ? !defaultBm25 : !BM25.equals(String.valueOf(named));
        }

        /** Whether {@code field} is a keyword or wildcard field with a normalizer. */
        private static boolean normalizes(Map<?, ?> field) {
            Object type = field.get(TYPE);
            return (KEYWORD_TYPE.equals(type) || WILDCARD_TYPE.equals(type)) && field.get(NORMALIZER) != null;
        }

        /** Whether {@code field} is a keyword field with {@code use_similarity}, as the mapping keeps it (a boolean or its string). */
        private static boolean scoresBySimilarity(Map<?, ?> field) {
            return KEYWORD_TYPE.equals(field.get(TYPE)) && "true".equals(String.valueOf(field.get(USE_SIMILARITY)));
        }
    }

    /**
     * Whether {@code table}'s index maps {@code field} ({@link Mapping}). A
     * field it does not map has no value in any of its documents, as
     * OpenSearch reads an unmapped field. True for a table no mapping has
     * declared yet, whose fields are unknown.
     */
    static boolean maps(String table, String field) {
        Mapping mapping = MAPPINGS.get(table);
        return mapping == null || mapping.paths().contains(field);
    }

    /**
     * Why a query on {@code field} of {@code table} cannot be answered from
     * its columns, or null when it can: a keyword or wildcard field with a
     * normalizer, whose column holds the values as written, not as the
     * normalizer leaves them, or a multi-field the table keeps no column of
     * (one of another type than keyword or wildcard, or under a field that
     * is not a string). Either, answered from the columns, would be a wrong
     * answer: unnormalized values, or none at all.
     */
    static String unservedField(String table, String field) {
        Mapping mapping = MAPPINGS.get(table);
        if (mapping == null || field == null) {
            return null;
        }
        if (mapping.normalized().contains(field)) {
            return "whose values its normalizer changes";
        }
        if (mapping.multiFields().contains(field) && !columns(table).containsKey(field)) {
            return "a multi-field the table keeps no column of";
        }
        return null;
    }

    /**
     * The fields of {@code table} its mapping declares {@code float} or
     * {@code half_float}, which a sort compares as 32-bit floats; empty for
     * a table no mapping has declared yet.
     */
    static Set<String> floatFields(String table) {
        Mapping mapping = MAPPINGS.get(table);
        return mapping == null ? Set.of() : mapping.floatFields();
    }

    /**
     * The fields of {@code table} its mapping declares {@code integer},
     * {@code short} or {@code byte}, which a sort compares as 32-bit ints;
     * empty for a table no mapping has declared yet.
     */
    static Set<String> intFields(String table) {
        Mapping mapping = MAPPINGS.get(table);
        return mapping == null ? Set.of() : mapping.intFields();
    }

    /**
     * Column name → description; empty if the table does not exist yet. A
     * field alias is a name of its target's column, as a keyword multi-field
     * is of its string field's, and a date column carries the format its
     * mapping declares.
     */
    public static Map<String, Column> columns(String name) {
        init();
        Map<String, Column> cached = COLUMNS.get(name);
        if (cached != null) {
            return cached;
        }
        String json = take(lib.infino_columns(conn, name));
        if (json == null || json.isEmpty()) {
            return Collections.emptyMap();
        }
        Mapping mapping = MAPPINGS.get(name);
        Map<String, Column> out = new LinkedHashMap<>();
        for (Map<String, Object> c : rows(json)) {
            String column = (String) c.get("name");
            Column described = new Column(
                column,
                (String) c.get("type"),
                Boolean.TRUE.equals(c.get("fts")),
                Boolean.TRUE.equals(c.get("vector")),
                null,
                null,
                (String) c.get("elem")
            );
            String key = InfinoQueryTranslator.keyOf(column);
            // A list column of a mapped field reads its dates as the field's do.
            String format = mapping == null ? null : mapping.dateFormats().get(InfinoQueryTranslator.fieldOf(column));
            if (mapping != null && mapping.similarityFields().contains(key)) {
                described = described.scoredBySimilarity();
            }
            if (mapping != null && mapping.otherSimilarityFields().contains(InfinoQueryTranslator.fieldOf(column))) {
                described = described.scoredByOtherSimilarity();
            }
            Column kept = format == null ? described : described.dated(format);
            // A field named like a reserved column is looked up by its name, in place of the reserved column.
            if (key.equals(column)) {
                out.putIfAbsent(key, kept);
            } else {
                out.put(key, kept);
            }
        }
        for (Map.Entry<String, String> alias : (mapping == null ? Map.<String, String>of() : mapping.aliases()).entrySet()) {
            Column target = out.get(alias.getValue());
            if (target != null) {
                out.putIfAbsent(alias.getKey(), target);
            }
        }
        for (Map.Entry<String, String> sub : (mapping == null ? Map.<String, String>of() : mapping.keywordFields()).entrySet()) {
            Column field = out.get(sub.getValue());
            if (field != null && field.isString()) {
                Column exact = field.exact();
                Column scored = mapping.similarityFields().contains(sub.getKey()) ? exact.scoredBySimilarity() : exact;
                if (mapping.otherSimilarityFields().contains(sub.getKey())) {
                    scored = scored.scoredByOtherSimilarity();
                }
                out.putIfAbsent(sub.getKey(), scored);
            }
        }
        Map<String, Column> view = Collections.unmodifiableMap(out);
        COLUMNS.put(name, view);
        return view;
    }

    /** Whether {@code key} names {@code column} by an alias rather than by the column's own name. */
    static boolean isAlias(String key, Column column) {
        return !key.equals(InfinoQueryTranslator.keyOf(column.name()));
    }

    public static void forgetColumns(String name) {
        COLUMNS.remove(name);
    }

    public static int dropTable(String name) {
        init();
        COLUMNS.remove(name);
        InfinoRequestCache.changed(name);
        try {
            return lib.infino_drop(conn, name);
        } finally {
            InfinoRequestCache.changed(name);
            WRITTEN.remove(name); // nothing of it is left to see
        }
    }

    /**
     * Compact the table; for an edge index's ({@link InfinoPlugin#INFINO_EDGES}),
     * also build the adjacency index the engine's graph walks read over its rows.
     */
    public static int optimize(String name, Settings indexSettings) {
        if (InfinoPlugin.INFINO_ATTACHED.get(indexSettings)) {
            // An attached index reads a table it does not own: compacting it is its owner's to do.
            return 0;
        }
        init();
        boolean edges = InfinoPlugin.INFINO_EDGES.get(indexSettings);
        InfinoRequestCache.changed(name);
        try {
            return edges ? lib.infino_optimize_edges(conn, name) : lib.infino_optimize(conn, name);
        } finally {
            InfinoRequestCache.changed(name);
        }
    }

    // ---- documents ----

    /**
     * The reason one document would be refused by the declared columns, or
     * null if it conforms. Throws {@link InfinoException} (503 unless the
     * error carries its own status) when the document could not be checked,
     * because the table could not be opened or the bridge failed while
     * checking it: that says nothing about the document, so the write may be
     * tried again, and an acknowledged document the commit checks is kept
     * for the next commit rather than dropped.
     */
    public static String checkDoc(String name, String source) {
        return checkDoc(name, source, List.of());
    }

    /**
     * {@link #checkDoc(String, String)} with the fields at {@code ignored}
     * left out of the document, as OpenSearch's mapper leaves out a field
     * with a malformed value it ignores.
     */
    public static String checkDoc(String name, String source, List<String> ignored) {
        init();
        Pointer p = lib.infino_check_doc(conn, name, source, ignored.isEmpty() ? null : InfinoIgnored.join(ignored));
        if (p == null) {
            throw new InfinoException("the bridge failed while checking the document", RestStatus.SERVICE_UNAVAILABLE);
        }
        String why = takeOrThrow(p, RestStatus.SERVICE_UNAVAILABLE);
        return why.isEmpty() ? null : why;
    }

    /**
     * The ids and sources of a batch as one UTF-8 buffer with the end offset
     * of each id and each source; with routing values, each row's follows its
     * source (empty for none) and {@code rtEnds} holds where it ends. With
     * ignored fields, the paths of each row's follow ({@link InfinoIgnored#join},
     * empty for none) and {@code igEnds} holds where they end; null when no
     * row has any.
     */
    private record Packed(byte[] buf, int[] idEnds, int[] srcEnds, int[] rtEnds, int[] igEnds) {
        static Packed of(List<String> osids, List<String> sources, List<List<String>> ignored) {
            return of(osids, sources, null, ignored);
        }

        static Packed of(List<String> osids, List<String> sources, List<String> routings, List<List<String>> ignored) {
            int n = osids.size();
            int[] idEnds = new int[n];
            int[] srcEnds = new int[n];
            int[] rtEnds = routings == null ? null : new int[n];
            boolean anyIgnored = ignored != null && ignored.stream().anyMatch(paths -> paths != null && paths.isEmpty() == false);
            int[] igEnds = anyIgnored ? new int[n] : null;
            byte[][] idBytes = new byte[n][];
            byte[][] srcBytes = new byte[n][];
            byte[][] rtBytes = new byte[n][];
            byte[][] igBytes = new byte[n][];
            int total = 0;
            for (int i = 0; i < n; i++) {
                idBytes[i] = osids.get(i).getBytes(StandardCharsets.UTF_8);
                srcBytes[i] = sources.get(i).getBytes(StandardCharsets.UTF_8);
                String routing = routings == null ? null : routings.get(i);
                rtBytes[i] = routing == null ? new byte[0] : routing.getBytes(StandardCharsets.UTF_8);
                List<String> paths = anyIgnored ? ignored.get(i) : null;
                igBytes[i] = paths == null || paths.isEmpty() ? new byte[0] : InfinoIgnored.join(paths).getBytes(StandardCharsets.UTF_8);
                total += idBytes[i].length;
                idEnds[i] = total;
                total += srcBytes[i].length;
                srcEnds[i] = total;
                if (rtEnds != null) {
                    total += rtBytes[i].length;
                    rtEnds[i] = total;
                }
                if (igEnds != null) {
                    total += igBytes[i].length;
                    igEnds[i] = total;
                }
            }
            byte[] buf = new byte[total];
            int at = 0;
            for (int i = 0; i < n; i++) {
                System.arraycopy(idBytes[i], 0, buf, at, idBytes[i].length);
                at += idBytes[i].length;
                System.arraycopy(srcBytes[i], 0, buf, at, srcBytes[i].length);
                at += srcBytes[i].length;
                if (rtEnds != null) {
                    System.arraycopy(rtBytes[i], 0, buf, at, rtBytes[i].length);
                    at += rtBytes[i].length;
                }
                if (igEnds != null) {
                    System.arraycopy(igBytes[i], 0, buf, at, igBytes[i].length);
                    at += igBytes[i].length;
                }
            }
            return new Packed(buf, idEnds, srcEnds, rtEnds, igEnds);
        }
    }

    /**
     * One refused document of a prepared batch: which batch, which document,
     * why, and whether the table refuses what it carries ({@code permanent}),
     * so every later try is refused the same way. One not permanent failed
     * for a reason of the moment and a later try may take it.
     */
    public record PreparedFailure(int batch, int index, String reason, boolean permanent) {}

    /** Outcome of appending prepared batches. */
    public record PreparedResult(int ok, List<PreparedFailure> failed) {}

    /**
     * Parse and convert a batch ahead of its append, so the work overlaps the
     * append that is running. The handle must go to {@link #appendPrepared}
     * or {@link #freePrepared}. Throws {@link InfinoException} with the
     * bridge's reason when the batch could not be prepared (see
     * {@link #prepareFailure}).
     */
    public static Pointer prepare(String name, List<String> osids, List<String> sources) {
        return prepare(name, osids, sources, (List<List<String>>) null);
    }

    /**
     * {@link #prepare(String, List, List)} with the paths of each row's
     * fields that OpenSearch's mapper ignored (null, or an empty list, for
     * none): the row leaves them out and keeps them in the
     * {@link InfinoQueryTranslator#IGNORED} column.
     */
    public static Pointer prepare(String name, List<String> osids, List<String> sources, List<List<String>> ignored) {
        init();
        Packed p = Packed.of(osids, sources, ignored);
        return prepared(name, lib.infino_prepare_batch(conn, name, p.buf(), p.buf().length, p.idEnds(), p.srcEnds(), p.igEnds(), osids.size()));
    }

    /** The prepared handle, or the reason there is none thrown. */
    private static Pointer prepared(String name, Pointer handle) {
        if (handle == null) {
            throw prepareFailure(name);
        }
        return handle;
    }

    /**
     * Why a prepare returned null, as the exception the caller gets: the
     * bridge's reason, with the status it gives. A hosted service that could
     * not be reached or did not answer in time is a 503, which a client
     * retries; anything else is an internal error.
     */
    private static InfinoException prepareFailure(String name) {
        String why = take(lib.infino_prepare_error());
        if (why == null) {
            return new InfinoException("[infino] the bridge could not prepare the batch for " + name);
        }
        Map<String, Object> error = object(why);
        return InfinoException.of("[infino] the batch for " + name + " could not be prepared: " + error.get("error"), error, RestStatus.INTERNAL_SERVER_ERROR);
    }

    /**
     * {@link #prepare} with each row's routing value (null for none), stamp
     * and the operation that wrote it (each negative for none), by writer
     * {@code gen}, kept in the {@link InfinoQueryTranslator#ROUTING},
     * {@link InfinoQueryTranslator#SEQ}, {@link InfinoQueryTranslator#OP} and
     * {@link InfinoQueryTranslator#GEN} columns, and the paths of its
     * ignored fields as {@link #prepare(String, List, List, List)} keeps them.
     */
    public static Pointer prepare(
        String name,
        List<String> osids,
        List<String> sources,
        List<String> routings,
        long[] seqs,
        long[] ops,
        long gen,
        List<List<String>> ignored
    ) {
        init();
        Packed p = Packed.of(osids, sources, routings, ignored);
        return prepared(name, lib.infino_prepare_batch_meta(
            conn,
            name,
            p.buf(),
            p.buf().length,
            p.idEnds(),
            p.srcEnds(),
            p.rtEnds(),
            seqs,
            ops,
            gen,
            p.igEnds(),
            osids.size()
        ));
    }

    /**
     * The highest operation writer {@code gen} recorded on a row of table
     * {@code name}, or null when no row carries one of its operations; read
     * from the table's latest commit ({@link #latestRows}), since a replay
     * starts after it.
     */
    public static Long highestOp(String name, long gen) {
        if (!columns(name).containsKey(InfinoQueryTranslator.OP)) {
            return null;
        }
        String op = InfinoQueryTranslator.quoteIdent(InfinoQueryTranslator.OP);
        String sql = "SELECT max(" + op + ") AS highest FROM " + InfinoQueryTranslator.quoteIdent(name) + " WHERE "
            + InfinoQueryTranslator.quoteIdent(InfinoQueryTranslator.GEN) + " = " + gen;
        for (Map<String, Object> row : latestRows(sql)) {
            Object highest = row.get("highest");
            if (highest instanceof Number n) {
                return n.longValue();
            }
        }
        return null;
    }

    /**
     * Append prepared batches in one commit; the handles are released by the
     * call. The rows of {@code replaced} ids replace the table's rows under
     * those ids in the same mutation, so a failed write leaves the old rows.
     * {@code seqs} (null for none) is the sequence number of every document,
     * batch after batch in order, stored beside its row.
     */
    @SuppressWarnings("unchecked")
    public static PreparedResult appendPrepared(String name, List<Pointer> prepared, Collection<String> replaced, long[] seqs) {
        init();
        COLUMNS.remove(name); // the batch may have added paths to the schema
        String[] ids = replaced.toArray(new String[0]);
        int seqCount = seqs == null ? 0 : seqs.length;
        InfinoRequestCache.changed(name);
        Map<String, Object> out;
        try {
            out = object(
                takeOrThrow(lib.infino_append_prepared(conn, name, prepared.toArray(new Pointer[0]), prepared.size(), ids, ids.length, seqs, seqCount))
            );
        } finally {
            InfinoRequestCache.changed(name);
            wrote(name);
        }
        List<PreparedFailure> failed = new ArrayList<>();
        for (Map<String, Object> f : (List<Map<String, Object>>) out.get("failed")) {
            failed.add(
                new PreparedFailure(
                    ((Number) f.get("b")).intValue(),
                    ((Number) f.get("i")).intValue(),
                    (String) f.get("reason"),
                    Boolean.TRUE.equals(f.get("permanent"))
                )
            );
        }
        return new PreparedResult(((Number) out.get("ok")).intValue(), failed);
    }

    public static void freePrepared(Pointer prepared) {
        if (prepared != null) {
            lib.infino_free_prepared(prepared);
        }
    }

    /**
     * The rows of a prepared batch the table refuses, each by its place in
     * the batch and why; empty for a null handle. The handle stays the
     * caller's.
     */
    public static List<Failure> preparedFailed(Pointer prepared) {
        if (prepared == null) {
            return List.of();
        }
        init();
        List<Failure> failed = new ArrayList<>();
        for (Map<String, Object> f : rows(takeOrThrow(lib.infino_prepared_failed(prepared)))) {
            failed.add(new Failure(((Number) f.get("i")).intValue(), (String) f.get("reason")));
        }
        return failed;
    }

    /**
     * Index many documents in one append (parallel arrays of length n). The
     * ids and sources cross to the bridge as one UTF-8 buffer with end
     * offsets, so the call marshals three arrays rather than one native
     * string per document.
     */
    @SuppressWarnings("unchecked")
    public static BatchResult indexBatch(String name, String[] osids, String[] sources, int n) {
        init();
        COLUMNS.remove(name); // the batch may have added paths to the schema
        Packed p = Packed.of(List.of(osids).subList(0, n), List.of(sources).subList(0, n), null);
        InfinoRequestCache.changed(name);
        Map<String, Object> out;
        try {
            out = object(takeOrThrow(lib.infino_index_batch_buf(conn, name, p.buf(), p.buf().length, p.idEnds(), p.srcEnds(), n)));
        } finally {
            InfinoRequestCache.changed(name);
            wrote(name);
        }
        List<Failure> failed = new ArrayList<>();
        for (Map<String, Object> f : (List<Map<String, Object>>) out.get("failed")) {
            failed.add(new Failure(((Number) f.get("i")).intValue(), (String) f.get("reason")));
        }
        return new BatchResult(((Number) out.get("ok")).intValue(), failed);
    }

    /** Tombstone every row with one of these ids, in one mutation. */
    public static int deleteOsids(String name, String[] osids, int n) {
        init();
        InfinoRequestCache.changed(name);
        try {
            return lib.infino_delete_osids(conn, name, osids, n);
        } finally {
            InfinoRequestCache.changed(name);
            wrote(name);
        }
    }

    /**
     * The source of one id: the page prefetch when the query phase ran one,
     * else a lookup; null when the table holds no such id. A lookup that
     * fails throws ({@link #READ_FAILED}), so a failed read is never taken
     * for a missing document.
     */
    public static String getSource(String name, String osid) {
        init();
        String prefetched = SOURCES.get(name + "\u0000" + osid);
        if (prefetched != null) {
            return prefetched;
        }
        return getSourceFresh(name, osid);
    }

    /**
     * The source of one id from the table, never from the prefetch: for a
     * write that merges into it. Null when the table holds no such id; a
     * lookup that fails throws, so the write fails rather than creating or
     * overwriting the document.
     */
    public static String getSourceFresh(String name, String osid) {
        init();
        return sourceOf(takeOrThrow(lib.infino_get(conn, name, osid), READ_FAILED));
    }

    /**
     * The source of a {@code get} answer, {@code {"source": ..}}; null when
     * the table holds no such id. The source sits under {@code source},
     * never at the top, so a document whose first field is {@code error}
     * cannot make the answer read as an error object.
     */
    static String sourceOf(String json) {
        Map<String, Object> answer = object(json);
        if (!answer.containsKey("source")) {
            throw new InfinoException("the bridge answered no source", RestStatus.INTERNAL_SERVER_ERROR);
        }
        Object source = answer.get("source");
        return source == null ? null : source.toString();
    }

    /**
     * {@link #getSourceFresh} as of the table's latest commit, as
     * {@link #latestRows} reads: for a write that merges into the document
     * when the last node to write the table may be another. A lookup that
     * fails throws rather than answering that the table holds no such id.
     */
    public static String getSourceLatest(String name, String osid) {
        init();
        Pointer p = lib.infino_get_latest(conn, name, osid);
        if (p == null) {
            throw new InfinoException("reading " + osid + " from the latest commit of " + name + " failed", RestStatus.SERVICE_UNAVAILABLE);
        }
        return take(p);
    }

    /**
     * Keep the connection the latest reads ({@link #latestRows},
     * {@link #getSourceLatest}) go through open until {@link #releaseLatest},
     * for a reader that reads the latest commit for each batch it writes.
     * The connection holds a handle to each table it has read, each keeping
     * that table's manifest in memory beside the one the node's own
     * connection holds, so it closes after each read no one holds it for.
     */
    public static void holdLatest() {
        init();
        lib.infino_hold_latest(conn);
    }

    /** Let go of a hold {@link #holdLatest} took. */
    public static void releaseLatest() {
        init();
        lib.infino_release_latest(conn);
    }

    /**
     * One document as the table holds it: its source, the sequence number it
     * was written under, and the routing value it was written with (null for
     * none), which a get reports as {@code _routing}.
     */
    public record Doc(String source, long seq, String routing) {}

    /**
     * One id's document from the table, never from the prefetch; null when
     * the table holds no such id. A lookup that fails throws
     * ({@link #READ_FAILED}), so a failed read is never taken for a missing
     * document.
     */
    public static Doc getDoc(String name, String osid) {
        init();
        return docOf(takeOrThrow(lib.infino_get_doc(conn, name, osid), READ_FAILED));
    }

    /**
     * The documents of these ids, by id, for a realtime get or multi-get: as
     * of the table's latest commit when {@code latest} ({@link #latestRows}),
     * for a node that may not have read the last commit another node made,
     * else through the node's own connection. One open of the table serves
     * every id. An id the table does not hold is absent; a lookup that fails
     * throws ({@link #READ_FAILED}) rather than answering that the table
     * holds no such id.
     */
    public static Map<String, Doc> docsOf(String name, Collection<String> osids, boolean latest) {
        if (osids.isEmpty()) {
            return new HashMap<>();
        }
        init();
        String[] ids = osids.toArray(new String[0]);
        Pointer p = latest ? lib.infino_get_docs_latest(conn, name, ids, ids.length) : lib.infino_get_docs(conn, name, ids, ids.length);
        return docsFrom(takeOrThrow(p, READ_FAILED));
    }

    /** Close the connection the latest reads go through, so the next opens it afresh without the table handles it held. */
    public static void forgetLatest() {
        init();
        lib.infino_forget_latest(conn);
    }

    /**
     * The document of a {@code get_doc} answer, {@code {"doc": {"source": .., "seq": n, "routing": ..}}};
     * null when the table holds no such id. The document sits under {@code doc},
     * never at the top, so no document can make the answer read as an error.
     */
    static Doc docOf(String json) {
        Map<String, Object> answer = object(json);
        if (!answer.containsKey("doc")) {
            throw new InfinoException("the bridge answered no document", RestStatus.INTERNAL_SERVER_ERROR);
        }
        return answer.get("doc") instanceof Map<?, ?> doc ? doc(doc) : null;
    }

    /**
     * The documents of a {@code get_docs} answer, {@code {"docs": {id: doc}}},
     * by id; an id the table does not hold is absent. The ids sit under
     * {@code docs}, never at the top, so an id such as {@code error} reads as
     * a document.
     */
    static Map<String, Doc> docsFrom(String json) {
        if (!(object(json).get("docs") instanceof Map<?, ?> byId)) {
            throw new InfinoException("the bridge answered no documents", RestStatus.INTERNAL_SERVER_ERROR);
        }
        Map<String, Doc> out = new HashMap<>();
        for (Map.Entry<?, ?> e : byId.entrySet()) {
            if (e.getValue() instanceof Map<?, ?> doc) {
                out.put(e.getKey().toString(), doc(doc));
            }
        }
        return out;
    }

    /** The document of one {@code {"source", "seq", "routing"}} answer. */
    private static Doc doc(Map<?, ?> doc) {
        Object source = doc.get("source");
        Object routing = doc.get("routing");
        return new Doc(source == null ? null : source.toString(), seq(doc.get("seq")), routing == null ? null : routing.toString());
    }

    /**
     * The documents of these ids, by id, in one statement, narrowed by
     * {@code where} (null for none): an id the table does not hold, or the
     * predicate drops, is absent. The table has no index on ids, so this
     * scans the id column, as {@link #prefetchSources} does.
     */
    public static Map<String, Doc> docs(String name, Collection<String> osids, String where) {
        Map<String, Doc> out = new HashMap<>();
        if (osids.isEmpty()) {
            return out;
        }
        String columns = InfinoQueryTranslator.OSID + ", " + InfinoQueryTranslator.SOURCE + ", " + InfinoQueryTranslator.SEQ;
        if (columns(name).containsKey(InfinoQueryTranslator.ROUTING)) {
            columns += ", " + InfinoQueryTranslator.quoteIdent(InfinoQueryTranslator.ROUTING);
        }
        for (Map<String, Object> row : sqlRows(byIds(name, columns, osids, where))) {
            Object id = row.get(InfinoQueryTranslator.OSID);
            Object source = row.get(InfinoQueryTranslator.SOURCE);
            Object routing = row.get(InfinoQueryTranslator.ROUTING);
            if (id != null && source != null) {
                out.put(id.toString(), new Doc(source.toString(), seq(row.get(InfinoQueryTranslator.SEQ)), routing == null ? null : routing.toString()));
            }
        }
        return out;
    }

    /**
     * The sources of these ids, by id, each looked up through the id
     * column's index: a page's worth costs what its ids do, whatever the
     * table holds, where one statement over the ids would read the whole id
     * column. An id the table does not hold is absent; a lookup that fails
     * throws, so a failed read is never taken for a deleted document.
     */
    public static Map<String, String> sources(String name, Collection<String> osids) {
        Map<String, String> out = new HashMap<>();
        if (osids.isEmpty()) {
            return out;
        }
        // One call for the page: the bridge runs its lookups side by side.
        init();
        String json = takeOrThrow(lib.infino_get_many(conn, name, osids.toArray(new String[0]), osids.size()));
        return sourcesOf(json);
    }

    /**
     * The sources of a {@code get_many} answer, {@code {"sources":{id: source}}}.
     * The ids sit under {@code sources}, never at the top, so an id such as
     * {@code error} cannot make the answer read as an error object.
     */
    static Map<String, String> sourcesOf(String json) {
        Object found = object(json).get("sources");
        if (!(found instanceof Map<?, ?> byId)) {
            throw new InfinoException("the bridge answered no sources", RestStatus.INTERNAL_SERVER_ERROR);
        }
        Map<String, String> out = new HashMap<>();
        for (Map.Entry<?, ?> entry : byId.entrySet()) {
            if (entry.getValue() instanceof String source && !source.isEmpty()) {
                out.put(entry.getKey().toString(), source);
            }
        }
        return out;
    }

    /** A sequence number as a row holds it: a row written without one reads as {@link InfinoDocumentVersion#UNTRACKED_SEQ_NO}. */
    static long seq(Object value) {
        return value instanceof Number n ? n.longValue() : InfinoDocumentVersion.UNTRACKED_SEQ_NO;
    }

    /**
     * The highest sequence number the table holds as of its latest commit
     * ({@link #latestRows}; see {@link #highestSeq}):
     * {@link SequenceNumbers#NO_OPS_PERFORMED} when it holds no row, so its
     * first write is given 0, as stock gives a new shard's first operation.
     */
    public static long maxSeq(String name) {
        String table = InfinoQueryTranslator.quoteIdent(name);
        String sql = "SELECT MAX(" + InfinoQueryTranslator.SEQ + ") AS " + InfinoQueryTranslator.SEQ + " FROM " + table;
        List<Map<String, Object>> rows = latestRows(sql);
        Object max = rows.isEmpty() ? null : rows.get(0).get(InfinoQueryTranslator.SEQ);
        return highestSeq(max, () -> !latestRows("SELECT " + InfinoQueryTranslator.OSID + " FROM " + table + " LIMIT 1").isEmpty());
    }

    /**
     * The highest sequence number of a table whose rows' highest stored one
     * is {@code max} (null when no row stores one): that number, else
     * {@link InfinoDocumentVersion#UNTRACKED_SEQ_NO} when the table holds rows
     * written without one (those report it, so the next write is given a
     * number past it), else {@link SequenceNumbers#NO_OPS_PERFORMED}.
     */
    static long highestSeq(Object max, BooleanSupplier holdsRows) {
        if (max instanceof Number n) {
            return n.longValue();
        }
        return holdsRows.getAsBoolean() ? InfinoDocumentVersion.UNTRACKED_SEQ_NO : SequenceNumbers.NO_OPS_PERFORMED;
    }

    /** Drop the prefetched sources of ids a write just changed, so a read after the write sees the table. */
    public static void forgetSources(String name, Collection<String> osids) {
        for (String id : osids) {
            SOURCES.invalidate(name + "\u0000" + id);
        }
    }

    // ---- queries ----

    public static String sql(String q) {
        init();
        String out = takeOrThrow(lib.infino_sql(conn, q));
        log.debug("[infino] sql: {} -> {} chars", q, out.length());
        return out;
    }

    /**
     * The indices a caller's read-only statement reads, each once, in the
     * order it names them (see {@link InfinoSql}); a statement the SQL route
     * does not serve throws with the status that says why.
     */
    static List<String> sqlIndices(String sql) {
        init();
        Object listed = object(takeOrThrow(lib.infino_sql_indices(sql))).get("indices");
        List<String> out = new ArrayList<>();
        if (listed instanceof List<?> names) {
            for (Object name : names) {
                out.add(String.valueOf(name));
            }
        }
        return out;
    }

    /**
     * A caller's read-only statement, each index it reads replaced by its
     * view in {@code views} (see {@link InfinoSql}): its schema and rows as
     * JSON, or a throw with the status that says why it was not run.
     */
    static String sqlRead(String sql, String views) {
        init();
        return takeOrThrow(lib.infino_sql_read(conn, sql, views));
    }

    public static List<Map<String, Object>> sqlRows(String q) {
        return rows(sql(q));
    }

    /**
     * The rows of {@code q} as of each table's latest commit, for a read whose
     * answer decides what a write does: whether an id is already a row, so a
     * write replaces it rather than appending it again, or where a replay or
     * a new writer's numbering starts. {@link #sqlRows} reads under the
     * engine's staleness bound and, for up to its window after another node
     * commits, may answer from the commit before, which is what a shard
     * taking over from that node reads first. The engine's own update and
     * delete read the latest commit to find their rows but give a caller no
     * way to on a connection opened under the bound, so the bridge reads
     * these through a connection of its own that does.
     */
    static List<Map<String, Object>> latestRows(String q) {
        init();
        String out = takeOrThrow(lib.infino_sql_latest(conn, q));
        log.debug("[infino] latest sql: {} -> {} chars", q, out.length());
        return rows(out);
    }

    /**
     * The top {@code k} rows of a full-text query ({@code mode} "and" / "or")
     * by BM25, best first, each with its id, source and score: a page of hits
     * in one engine call, with no statement to plan.
     */
    public static List<Map<String, Object>> bm25Hits(String n, String c, String q, String mode, int k) {
        init();
        String out = takeOrThrow(lib.infino_bm25(conn, n, c, q, mode, k));
        log.debug("[infino] bm25: {} {} {} k={} -> {} chars", n, c, q, k, out.length());
        return rows(out);
    }

    /**
     * The first {@code k} rows of table {@code n} by {@code column},
     * descending when {@code desc}, rows sharing a value in row id order and
     * rows without one last, each with its id, row id, the column's value
     * and, when {@code withSource}, its source: a sorted page in one engine
     * call, read from the column's range index with no statement to plan,
     * its sources read by row id along with it. Null when the column has no
     * range index, for the caller to plan the statement.
     */
    public static List<Map<String, Object>> sortedRows(String n, String column, boolean desc, int k, boolean withSource) {
        init();
        String out = takeOrThrow(lib.infino_sorted(conn, n, column, desc ? 1 : 0, k, withSource ? 1 : 0)).trim();
        log.debug("[infino] sorted: {} {} {} k={} -> {} chars", n, column, desc ? "desc" : "asc", k, out.length());
        return out.equals("null") ? null : rows(out);
    }

    /** Count of rows matching a full-text query ({@code mode} "and" / "or"), without materializing them. */
    public static long count(String n, String c, String q, String mode) {
        init();
        return Long.parseLong(takeOrThrow(lib.infino_count(conn, n, c, q, mode)).trim());
    }

    /**
     * The rows each of {@code predicates} (SQL conditions over the table's
     * columns) holds, in order, as {@code COUNT(*) FILTER (WHERE p)} counts
     * each. The engine counts a range on a range-indexed column, or every row,
     * from its range index with no statement, and runs one aggregate otherwise.
     */
    public static long[] countWhere(String n, List<String> predicates) {
        init();
        String out = takeOrThrow(lib.infino_count_where(conn, n, jsonArray(predicates)));
        try (XContentParser parser = parser(out)) {
            List<Object> list = parser.list();
            long[] counts = new long[list.size()];
            for (int i = 0; i < counts.length; i++) {
                counts[i] = ((Number) list.get(i)).longValue();
            }
            return counts;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** {@code values} as a JSON array of strings. */
    private static String jsonArray(List<String> values) {
        try (XContentBuilder b = XContentFactory.jsonBuilder()) {
            b.startArray();
            for (String v : values) {
                b.value(v);
            }
            b.endArray();
            return BytesReference.bytes(b).utf8ToString();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * A column's rows counted per value, without a statement: one row per
     * value (under the column's name, null for the rows without one) with its
     * count under {@link #VALUE_COUNT}. The engine reads them from the table's
     * statistics when those count every row exactly, and runs the grouped
     * count otherwise. Null when there are more than {@code maxRows}.
     */
    public static List<Map<String, Object>> valueCounts(String n, String column, int maxRows) {
        init();
        String out = takeOrThrow(lib.infino_value_counts(conn, n, column, maxRows)).trim();
        return out.equals("null") ? null : rows(out);
    }

    /**
     * Sources for one page of ids in one statement, kept for the fetch phase
     * that follows the query phase. The table has no index on ids, so this
     * scans the id column; the SQL hit paths carry the source in the hit
     * statement instead and call {@link #keepSources}. Used by the vector
     * paths, whose entry points return ids and scores only.
     */
    public static void prefetchSources(String name, List<String> osids) {
        if (osids.isEmpty()) {
            return;
        }
        keepSources(name, sqlRows(byIds(name, InfinoQueryTranslator.OSID + ", " + InfinoQueryTranslator.SOURCE, osids, null)));
    }

    /** The statement selecting {@code columns} of the rows under these ids, narrowed by {@code where} (null for none). */
    private static String byIds(String name, String columns, Collection<String> osids, String where) {
        List<String> lits = new ArrayList<>(osids.size());
        for (String id : osids) {
            lits.add(InfinoQueryTranslator.literal(id));
        }
        return "SELECT " + columns + " FROM " + InfinoQueryTranslator.quoteIdent(name) + " WHERE " + InfinoQueryTranslator.OSID + " IN ("
            + String.join(", ", lits) + ")" + (where == null ? "" : " AND (" + where + ")");
    }

    /** Ids per statement when probing which of a batch's ids the table already holds. */
    private static final int PROBE_IDS_PER_STATEMENT = 1_000;

    /**
     * Which of these ids the table already holds, through the id column's
     * term index: one statement per thousand ids, a union of their postings
     * (asked as plain terms, see {@link #idTerms}), then an exact comparison,
     * since an id with punctuation indexes as several terms and the union may
     * name ids that only share one. An id with no term is compared by value.
     */
    public static Set<String> existingOsids(String name, Collection<String> osids) {
        return probe(name, osids, false, false).keySet();
    }

    /**
     * {@link #existingOsids} as of the table's latest commit
     * ({@link #latestRows}): for ids whose write replaces their row only if
     * the table holds one, when the last node to write the table may be
     * another.
     */
    public static Set<String> existingOsidsLatest(String name, Collection<String> osids) {
        return probe(name, osids, false, true).keySet();
    }

    /**
     * The sequence number of each of these ids the table holds, by id, the
     * same probe as {@link #existingOsids} reading the sequence column too:
     * an id the table does not hold is absent.
     */
    public static Map<String, Long> storedSeqs(String name, Collection<String> osids) {
        return probe(name, osids, true, false);
    }

    /** {@link #storedSeqs} as of the table's latest commit ({@link #latestRows}). */
    public static Map<String, Long> storedSeqsLatest(String name, Collection<String> osids) {
        return probe(name, osids, true, true);
    }

    private static Map<String, Long> probe(String name, Collection<String> osids, boolean withSeqs, boolean latest) {
        Set<String> wanted = new HashSet<>(osids);
        Map<String, Long> found = new HashMap<>();
        List<String> batch = new ArrayList<>(PROBE_IDS_PER_STATEMENT);
        // A latest probe's statements share one connection.
        if (latest) {
            holdLatest();
        }
        try {
            for (String id : wanted) {
                batch.add(id);
                if (batch.size() == PROBE_IDS_PER_STATEMENT) {
                    probe(name, batch, withSeqs, latest, wanted, found);
                    batch.clear();
                }
            }
            if (!batch.isEmpty()) {
                probe(name, batch, withSeqs, latest, wanted, found);
            }
        } finally {
            if (latest) {
                releaseLatest();
            }
        }
        return found;
    }

    private static void probe(
        String name,
        List<String> ids,
        boolean withSeqs,
        boolean latest,
        Set<String> wanted,
        Map<String, Long> found
    ) {
        String columns = withSeqs ? InfinoQueryTranslator.OSID + ", " + InfinoQueryTranslator.SEQ : InfinoQueryTranslator.OSID;
        // An id with no letter or digit indexes as no term, so the term index
        // cannot find it: those are compared by value over the id column.
        List<String> termed = new ArrayList<>(ids.size());
        List<String> bare = new ArrayList<>();
        for (String id : ids) {
            (hasTerm(id) ? termed : bare).add(id);
        }
        List<String> statements = new ArrayList<>(2);
        if (!termed.isEmpty()) {
            statements.add(
                "SELECT " + columns + " FROM token_match(" + InfinoQueryTranslator.literal(name) + ", '" + InfinoQueryTranslator.OSID + "', "
                    + InfinoQueryTranslator.literal(idTerms(termed)) + ", 'or')"
            );
        }
        if (!bare.isEmpty()) {
            statements.add(byIds(name, columns, bare, null));
        }
        for (String sql : statements) {
            for (Map<String, Object> row : latest ? latestRows(sql) : sqlRows(sql)) {
                Object id = row.get(InfinoQueryTranslator.OSID);
                if (id != null && wanted.contains(id.toString())) {
                    found.put(id.toString(), seq(row.get(InfinoQueryTranslator.SEQ)));
                }
            }
        }
    }

    /**
     * Ids as an engine text query of plain terms
     * ({@link InfinoQueryTranslator#plainTerms}): an id holding a leading
     * {@code +} or {@code -} (a phone number such as {@code +15551234}, a
     * negative number) would otherwise be a required or excluded clause, and
     * with a required clause in it the union finds only the rows holding
     * that one id.
     */
    static String idTerms(Collection<String> ids) {
        StringBuilder out = new StringBuilder();
        for (String id : ids) {
            String terms = InfinoQueryTranslator.plainTerms(id);
            if (!terms.isEmpty()) {
                if (out.length() > 0) {
                    out.append(' ');
                }
                out.append(terms);
            }
        }
        return out.toString();
    }

    /** Whether an id holds a letter or a digit, and so indexes as at least one term of the id column's term index. */
    static boolean hasTerm(String id) {
        return id.codePoints().anyMatch(Character::isLetterOrDigit);
    }

    /** The paths of the fields the mapper ignored in {@code id}'s document, as the table holds them ({@link InfinoIgnored}); none when it ignored none. */
    public static List<String> ignoredOf(String name, String id) {
        return ignoredOf(name, List.of(id)).getOrDefault(id, List.of());
    }

    /**
     * {@link #ignoredOf(String, String)} for each of these ids that has any,
     * by id: through the id column's term index as {@link #existingOsids}
     * finds ids, and only over rows that hold the column; none when the
     * table holds no such column.
     */
    public static Map<String, List<String>> ignoredOf(String name, Collection<String> ids) {
        if (ids.isEmpty() || InfinoQueryTranslator.ignoredColumn(columns(name)) == null) {
            return Map.of();
        }
        Set<String> wanted = new HashSet<>(ids);
        String columns = InfinoQueryTranslator.OSID + ", " + InfinoQueryTranslator.ignoredPaths();
        String held = InfinoQueryTranslator.quoteIdent(InfinoQueryTranslator.IGNORED) + " IS NOT NULL";
        List<String> termed = new ArrayList<>(wanted.size());
        List<String> bare = new ArrayList<>();
        for (String id : wanted) {
            (hasTerm(id) ? termed : bare).add(id);
        }
        List<String> statements = new ArrayList<>(2);
        for (int from = 0; from < termed.size(); from += PROBE_IDS_PER_STATEMENT) {
            List<String> batch = termed.subList(from, Math.min(termed.size(), from + PROBE_IDS_PER_STATEMENT));
            statements.add(
                "SELECT " + columns + " FROM token_match(" + InfinoQueryTranslator.literal(name) + ", '" + InfinoQueryTranslator.OSID + "', "
                    + InfinoQueryTranslator.literal(idTerms(batch)) + ", 'or') WHERE " + held
            );
        }
        if (!bare.isEmpty()) {
            statements.add(byIds(name, columns, bare, held));
        }
        Map<String, List<String>> out = new HashMap<>();
        for (String sql : statements) {
            for (Map<String, Object> row : sqlRows(sql)) {
                Object id = row.get(InfinoQueryTranslator.OSID);
                List<String> paths = InfinoIgnored.split(row.get(InfinoQueryTranslator.IGNORED));
                if (id != null && wanted.contains(id.toString()) && !paths.isEmpty()) {
                    out.put(id.toString(), paths);
                }
            }
        }
        return out;
    }

    /** Keep the sources of rows a hit statement returned (id and source columns), for the fetch phase. */
    public static void keepSources(String name, List<Map<String, Object>> rows) {
        for (Map<String, Object> row : rows) {
            Object id = row.get(InfinoQueryTranslator.OSID);
            Object source = row.get(InfinoQueryTranslator.SOURCE);
            if (id != null && source != null) {
                SOURCES.put(name + "\u0000" + id, source.toString());
            }
        }
    }

    /** Osids of every row matching a full-text query (unranked), for query-scoped aggs. */
    public static List<String> tokenMatchOsids(String n, String c, String q) {
        init();
        List<String> out = new ArrayList<>();
        for (Map<String, Object> row : rows(takeOrThrow(lib.infino_token_match(conn, n, c, q)))) {
            out.add((String) row.get(InfinoQueryTranslator.OSID));
        }
        return out;
    }

    public static String knn(String n, String c, String v, int k) {
        init();
        return takeOrThrow(lib.infino_knn(conn, n, c, v, k));
    }

    public static String hybrid(String n, String tc, String tq, String vc, String v, int k) {
        init();
        return takeOrThrow(lib.infino_hybrid(conn, n, tc, tq, vc, v, k));
    }
}
