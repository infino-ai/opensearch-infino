/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.support.ContextPreservingActionListener;
import org.opensearch.common.settings.IndexScopedSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.http.HttpChannel;
import org.opensearch.http.HttpRequest;
import org.opensearch.http.HttpResponse;
import org.opensearch.rest.AbstractRestChannel;
import org.opensearch.rest.RestController;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.RestResponse;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * The cluster's own state a loader registers beside the indices it loads:
 * what a snapshot of another cluster held besides its indices - its
 * persistent settings, index and component templates, ingest and search
 * pipelines and stored scripts - and the security plugin's action groups,
 * tenants, roles, users and role mappings. A user comes without its password
 * hash, which stays on the cluster it came from: it is created with a
 * password no one is told, and signs in once an administrator sets one
 * ({@link #sentBody}). One row of the indices' registry
 * ({@link InfinoRegistry#TABLE}) holds all of it ({@link #STATE_ROW}, an id
 * beginning with {@link #ROW_PREFIX}, which no index name can): each item's
 * kind, its name, and the body the cluster's own REST API takes to create
 * it. One row is written in one mutation, so the registry never holds part
 * of a state, or none while a new one replaces the old.
 *
 * <p>{@code POST /_plugins/_infino/account/indices} applies every registered
 * item before it creates the indices, so an index comes up with its
 * templates and pipelines in place, as on the cluster it came from. Each
 * item is sent to this node's own REST API as the caller's request, with the
 * caller's headers: the cluster parses and checks it exactly as it would the
 * caller sending it, and the security plugin allows it only when the caller
 * may make it. An item applied leaves the registry, so a later call never
 * applies it again over a change made since; one that fails stays, reported
 * with the cluster's answer, for the next call to retry. A cluster coming up
 * on its own applies nothing: there is no caller to apply it as.
 */
final class InfinoClusterState {

    private static final Logger log = LogManager.getLogger(InfinoClusterState.class);

    /**
     * What the id of a cluster-state row in the registry begins with: an
     * index name never begins with an underscore, so no index's row is one.
     */
    static final String ROW_PREFIX = "_cluster/";
    /** The id of the registry row that holds the cluster state. */
    static final String STATE_ROW = ROW_PREFIX + "state";

    /** The key of the state row's source that lists its items, and the keys of an item. */
    private static final String ITEMS = "items";
    private static final String KIND = "kind";
    private static final String NAME = "name";
    private static final String BODY = "body";
    /** Where an index or component template keeps its settings, and the settings' own key. */
    private static final String TEMPLATE_BODY = "template";
    private static final String SETTINGS = "settings";
    /** Settings written by their dotted names, as a template's body takes them. */
    private static final ToXContent.Params FLAT_SETTINGS = new ToXContent.MapParams(Map.of("flat_settings", "true"));

    /** The security plugin's REST API, which each of its kinds is created through. */
    private static final String SECURITY_API = "/_plugins/_security/api/";
    /** The keys a user's body gives its credential by, one of which the security plugin creates a user with. */
    private static final String USER_HASH = "hash";
    private static final String USER_PASSWORD = "password";
    /** The character classes, each drawn from at least once, and the length of a password no one is told. */
    private static final List<String> PASSWORD_CLASSES = List.of(
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ",
        "abcdefghijklmnopqrstuvwxyz",
        "0123456789",
        "!#%*+-=?@^_~"
    );
    private static final int PASSWORD_LENGTH = 32;
    private static final SecureRandom RANDOM = new SecureRandom();
    /** The longest part of the cluster's answer a failed item reports. */
    private static final int MAX_REASON_CHARS = 2_000;
    /** The address a request is said to come from when the caller's channel names none. */
    private static final InetSocketAddress LOOPBACK = new InetSocketAddress(InetAddress.getLoopbackAddress(), 0);

    /**
     * Each kind of item, in the order they are applied, and the path that
     * creates one: a component template before the index templates composed
     * of it, a role before the users and mappings that name it.
     */
    enum Kind {
        PERSISTENT_SETTINGS("persistent_settings", "/_cluster/settings", false),
        COMPONENT_TEMPLATE("component_template", "/_component_template/", true),
        INDEX_TEMPLATE("index_template", "/_index_template/", true),
        TEMPLATE("template", "/_template/", true),
        INGEST_PIPELINE("ingest_pipeline", "/_ingest/pipeline/", true),
        SEARCH_PIPELINE("search_pipeline", "/_search/pipeline/", true),
        SCRIPT("script", "/_scripts/", true),
        ACTION_GROUP("action_group", SECURITY_API + "actiongroups/", true),
        TENANT("tenant", SECURITY_API + "tenants/", true),
        ROLE("role", SECURITY_API + "roles/", true),
        USER("user", SECURITY_API + "internalusers/", true),
        ROLE_MAPPING("role_mapping", SECURITY_API + "rolesmapping/", true);

        final String key;
        private final String path;
        /** Whether the item's name ends the path. */
        private final boolean named;

        Kind(String key, String path, boolean named) {
            this.key = key;
            this.path = path;
            this.named = named;
        }

        String uri(String name) {
            return named ? path + URLEncoder.encode(name, StandardCharsets.UTF_8).replace("+", "%20") : path;
        }

        static Kind of(String key) {
            for (Kind kind : values()) {
                if (kind.key.equals(key)) {
                    return kind;
                }
            }
            return null;
        }
    }

    /** One registered item: its kind (null for one this node does not know), its name and its body. */
    record Item(String kindKey, Kind kind, String name, Map<String, Object> body) {}

    /** What became of an item: applied, or failed with the cluster's status and answer. */
    record Applied(String kind, String name, boolean applied, int status, String reason, List<String> leftOut) {}

    private static volatile RestController controller;
    private static volatile NamedXContentRegistry xContentRegistry;
    /**
     * A thread context holding nothing, taken as the node starts: what a
     * request arriving on the node's network threads begins with. An item
     * is sent in it, never in a context stashed from the caller's request,
     * since a stash keeps the security plugin's record of the caller
     * (a persistent header), which a request authenticating afresh refuses
     * to find already set.
     */
    private static volatile ThreadContext.StoredContext emptyContext;

    private InfinoClusterState() {}

    /** The node's REST controller, which every item is sent through. */
    static void bind(RestController restController) {
        controller = restController;
    }

    /** The registry the node parses request bodies with, and the empty context items are sent in. */
    static void bind(NamedXContentRegistry registry, ThreadContext.StoredContext empty) {
        xContentRegistry = registry;
        emptyContext = empty;
    }

    /**
     * The registered items, in the order they are applied: those of the
     * state row. Two loader parts registering together can leave the row
     * twice, each the same state, so the first is read. Reads the engine:
     * never on a transport or applier thread.
     */
    static List<Item> registered() {
        for (Map<String, Object> row : InfinoRegistry.rawRows()) {
            Object source = row.get(InfinoQueryTranslator.SOURCE);
            if (STATE_ROW.equals(row.get(InfinoQueryTranslator.OSID)) && source != null) {
                return items(source.toString());
            }
        }
        return List.of();
    }

    /** The items a state row's source lists, in the order they are applied. */
    static List<Item> items(String source) {
        List<Item> items = new ArrayList<>();
        Object listed = InfinoNative.object(source).get(ITEMS);
        for (Object entry : listed instanceof List<?> list ? list : List.of()) {
            if (!(entry instanceof Map<?, ?> item)) {
                continue;
            }
            String kindKey = String.valueOf(item.get(KIND));
            Object body = item.get(BODY);
            @SuppressWarnings("unchecked")
            Map<String, Object> fields = body instanceof Map<?, ?> m ? (Map<String, Object>) m : Map.of();
            items.add(new Item(kindKey, Kind.of(kindKey), String.valueOf(item.get(NAME)), fields));
        }
        items.sort(
            Comparator.comparingInt((Item item) -> item.kind() == null ? Kind.values().length : item.kind().ordinal())
                .thenComparing(Item::name)
        );
        return items;
    }

    /** The source of a state row listing {@code items}, as a loader writes one. */
    static String source(List<Item> items) {
        try (XContentBuilder b = XContentFactory.jsonBuilder()) {
            b.startObject().startArray(ITEMS);
            for (Item item : items) {
                b.startObject().field(KIND, item.kindKey()).field(NAME, item.name()).field(BODY, item.body()).endObject();
            }
            return b.endArray().endObject().toString();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** Whether a registry row with id {@code osid} is an item of cluster state rather than an index. */
    static boolean isClusterRow(String osid) {
        return osid.startsWith(ROW_PREFIX);
    }

    /**
     * Apply every registered item as {@code caller}, in order, and answer
     * what became of each; an applied item leaves the registry. Completes
     * on the generic pool, in the caller's context.
     */
    static void apply(RestRequest caller, ActionListener<List<Applied>> listener) {
        ThreadPool threadPool = InfinoServices.threadPool();
        RestController rest = controller;
        if (threadPool == null || rest == null || xContentRegistry == null || emptyContext == null) {
            listener.onFailure(new IllegalStateException("[infino] the node is not ready"));
            return;
        }
        ThreadContext context = threadPool.getThreadContext();
        ActionListener<List<Applied>> done = new ContextPreservingActionListener<>(context.newRestorableContext(false), listener);
        List<Item> items;
        try {
            items = registered();
        } catch (Exception e) {
            done.onFailure(e);
            return;
        }
        if (items.isEmpty()) {
            done.onResponse(List.of());
            return;
        }
        Map<String, List<String>> headers = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        headers.putAll(caller.getHeaders());
        // The body each item sends is JSON of its own length.
        headers.remove("Content-Length");
        headers.remove("Transfer-Encoding");
        headers.put("Content-Type", List.of("application/json"));
        HttpChannel callerChannel = caller.getHttpChannel();
        InetSocketAddress local = callerChannel == null ? LOOPBACK : callerChannel.getLocalAddress();
        InetSocketAddress remote = callerChannel == null ? LOOPBACK : callerChannel.getRemoteAddress();
        new Run(threadPool, rest, items, headers, new Loopback(local, remote), done).next(0);
    }

    /** One pass over the registered items, each sent once the one before it is answered. */
    private record Run(
        ThreadPool threadPool,
        RestController rest,
        List<Item> items,
        Map<String, List<String>> headers,
        Loopback channel,
        ActionListener<List<Applied>> done,
        List<Applied> applied,
        List<Item> failed
    ) {
        Run(
            ThreadPool threadPool,
            RestController rest,
            List<Item> items,
            Map<String, List<String>> headers,
            Loopback channel,
            ActionListener<List<Applied>> done
        ) {
            this(threadPool, rest, items, headers, channel, done, new ArrayList<>(), new ArrayList<>());
        }

        /** Send item {@code at} on the generic pool, or end the pass. */
        void next(int at) {
            threadPool.generic().execute(() -> {
                if (at == items.size()) {
                    finish();
                    return;
                }
                Item item = items.get(at);
                if (item.kind() == null) {
                    note(item, RestStatus.BAD_REQUEST.getStatus(), "no kind [" + item.kindKey() + "] is known to this node", List.of());
                    next(at + 1);
                    return;
                }
                try {
                    send(item, at);
                } catch (Exception e) {
                    note(item, RestStatus.INTERNAL_SERVER_ERROR.getStatus(), e.toString(), List.of());
                    next(at + 1);
                }
            });
        }

        private void send(Item item, int at) throws IOException {
            List<String> leftOut = new ArrayList<>();
            BytesReference body;
            try (XContentBuilder b = XContentFactory.jsonBuilder()) {
                body = BytesReference.bytes(b.map(sentBody(item, InfinoServices.indexScopedSettings(), leftOut)));
            }
            HttpRequest http = new Sent(RestRequest.Method.PUT, item.kind().uri(item.name()), body, headers);
            RestRequest request = RestRequest.request(xContentRegistry, http, channel);
            Answer answer = new Answer(request, response -> {
                note(item, response.status().getStatus(), response.content() == null ? "" : response.content().utf8ToString(), leftOut);
                next(at + 1);
            });
            ThreadContext context = threadPool.getThreadContext();
            // An empty context, so the node authenticates the item from the
            // caller's headers alone, as it would the caller's own request;
            // the context this thread had comes back once it is sent.
            try (ThreadContext.StoredContext ignored = context.newStoredContext(false)) {
                emptyContext.restore();
                rest.dispatchRequest(request, answer, context);
            }
        }

        private synchronized void note(Item item, int status, String answer, List<String> leftOut) {
            boolean ok = status < RestStatus.MULTIPLE_CHOICES.getStatus();
            String reason = ok ? null : answer.length() > MAX_REASON_CHARS ? answer.substring(0, MAX_REASON_CHARS) : answer;
            applied.add(new Applied(item.kindKey(), item.name(), ok, status, reason, List.copyOf(leftOut)));
            if (ok) {
                if (!leftOut.isEmpty()) {
                    log.warn("[infino] the registered {} [{}] was applied without the settings this cluster does not know: {}", item.kindKey(), item.name(), leftOut);
                }
            } else {
                failed.add(item);
                log.warn("[infino] the registered {} [{}] was not applied: {} {}", item.kindKey(), item.name(), status, reason);
            }
        }

        /** The state row keeps only the items that failed, in one mutation; none left, it goes. */
        private void finish() {
            try {
                if (failed.isEmpty()) {
                    InfinoNative.deleteOsids(InfinoRegistry.TABLE, new String[] { STATE_ROW }, 1);
                } else if (failed.size() < items.size()) {
                    InfinoRegistry.write(STATE_ROW, source(failed));
                }
            } catch (Exception e) {
                log.warn("[infino] the applied cluster state could not be removed from the registry; a later call applies it again", e);
            }
            done.onResponse(List.copyOf(applied));
        }
    }

    /** The channel an item's answer comes back on. */
    private static final class Answer extends AbstractRestChannel {
        private final Answered then;

        Answer(RestRequest request, Answered then) {
            super(request, true);
            this.then = then;
        }

        @Override
        public void sendResponse(RestResponse response) {
            then.with(response);
        }
    }

    /** What to do with an item's answer. */
    @FunctionalInterface
    private interface Answered {
        void with(RestResponse response);
    }

    /** An item as the request the caller would have sent for it. */
    private static final class Sent implements HttpRequest {
        private final RestRequest.Method method;
        private final String uri;
        private final BytesReference content;
        private final Map<String, List<String>> headers;

        Sent(RestRequest.Method method, String uri, BytesReference content, Map<String, List<String>> headers) {
            this.method = method;
            this.uri = uri;
            this.content = content;
            this.headers = headers;
        }

        @Override
        public RestRequest.Method method() {
            return method;
        }

        @Override
        public String uri() {
            return uri;
        }

        @Override
        public BytesReference content() {
            return content;
        }

        @Override
        public Map<String, List<String>> getHeaders() {
            return headers;
        }

        @Override
        public List<String> strictCookies() {
            return List.of();
        }

        @Override
        public HttpVersion protocolVersion() {
            return HttpVersion.HTTP_1_1;
        }

        @Override
        public HttpRequest removeHeader(String header) {
            Map<String, List<String>> kept = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
            kept.putAll(headers);
            kept.remove(header);
            return new Sent(method, uri, content, kept);
        }

        @Override
        public HttpResponse createResponse(RestStatus status, BytesReference content) {
            return new HttpResponse() {
                @Override
                public void addHeader(String name, String value) {}

                @Override
                public boolean containsHeader(String name) {
                    return false;
                }
            };
        }

        @Override
        public Exception getInboundException() {
            return null;
        }

        @Override
        public void release() {}

        @Override
        public HttpRequest releaseAndCopy() {
            return this;
        }
    }

    /** The connection an item is said to arrive on: the caller's addresses, never a socket. */
    private record Loopback(InetSocketAddress local, InetSocketAddress remote) implements HttpChannel {
        @Override
        public void sendResponse(HttpResponse response, ActionListener<Void> listener) {
            listener.onResponse(null);
        }

        @Override
        public InetSocketAddress getLocalAddress() {
            return local;
        }

        @Override
        public InetSocketAddress getRemoteAddress() {
            return remote;
        }

        @Override
        public void close() {}

        @Override
        public void addCloseListener(ActionListener<Void> listener) {}

        @Override
        public boolean isOpen() {
            return true;
        }
    }

    /** Write {@code applied} as the {@code state} array of a response. */
    static void write(XContentBuilder b, List<Applied> applied) {
        try {
            b.startArray("state");
            for (Applied a : applied) {
                b.startObject();
                b.field("kind", a.kind());
                b.field("name", a.name());
                b.field("result", a.applied() ? "applied" : "failed");
                if (!a.applied()) {
                    b.field("status", a.status());
                    b.field("reason", a.reason());
                }
                if (!a.leftOut().isEmpty()) {
                    b.field("left_out", a.leftOut());
                }
                b.endObject();
            }
            b.endArray();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * {@code item}'s body as it is sent: a template's settings held to the
     * ones this node knows ({@link #heldToKnownSettings}), and a user that
     * arrives with no credential - a loader leaves every user's password
     * hash on the cluster it came from - given a random password no one is
     * told, since the security plugin creates a user only with one. The user
     * signs in once an administrator sets its password.
     */
    static Map<String, Object> sentBody(Item item, IndexScopedSettings known, List<String> leftOut) {
        Map<String, Object> body = heldToKnownSettings(item, known, leftOut);
        if (item.kind() == Kind.USER && !body.containsKey(USER_HASH) && !body.containsKey(USER_PASSWORD)) {
            body.put(USER_PASSWORD, unknowablePassword());
        }
        return body;
    }

    /**
     * A password drawn at random, of every character class a password rule
     * may ask for, never stored or shown: what a user without one is created
     * with.
     */
    static String unknowablePassword() {
        List<Character> chars = new ArrayList<>();
        for (String required : PASSWORD_CLASSES) {
            chars.add(required.charAt(RANDOM.nextInt(required.length())));
        }
        String any = String.join("", PASSWORD_CLASSES);
        while (chars.size() < PASSWORD_LENGTH) {
            chars.add(any.charAt(RANDOM.nextInt(any.length())));
        }
        Collections.shuffle(chars, RANDOM);
        StringBuilder password = new StringBuilder(PASSWORD_LENGTH);
        chars.forEach(password::append);
        return password.toString();
    }

    /**
     * {@code item}'s body with a template's settings held to {@code known},
     * the index settings this node knows, each one it does not (another
     * engine's, a plugin's this cluster does not run) named onto
     * {@code leftOut}: the cluster refuses a template carrying one, as it
     * refuses such an index. Every other kind's body as it is.
     */
    @SuppressWarnings("unchecked")
    static Map<String, Object> heldToKnownSettings(Item item, IndexScopedSettings known, List<String> leftOut) {
        Map<String, Object> body = new LinkedHashMap<>(item.body());
        Map<String, Object> holder = body;
        if (item.kind() == Kind.INDEX_TEMPLATE || item.kind() == Kind.COMPONENT_TEMPLATE) {
            if (!(body.get(TEMPLATE_BODY) instanceof Map<?, ?> inner)) {
                return body;
            }
            holder = new LinkedHashMap<>((Map<String, Object>) inner);
            body.put(TEMPLATE_BODY, holder);
        } else if (item.kind() != Kind.TEMPLATE) {
            return body;
        }
        if (!(holder.get(SETTINGS) instanceof Map<?, ?> settings)) {
            return body;
        }
        Settings kept = InfinoRegistry.knownSettings(Settings.builder().loadFromMap((Map<String, ?>) settings).build(), known, leftOut);
        try (XContentBuilder b = XContentFactory.jsonBuilder()) {
            b.startObject();
            kept.toXContent(b, FLAT_SETTINGS);
            b.endObject();
            holder.put(SETTINGS, InfinoNative.object(BytesReference.bytes(b).utf8ToString()));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return body;
    }
}
