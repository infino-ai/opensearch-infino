/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestChannel;
import org.opensearch.rest.RestRequest;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * The cluster's hosted account: where its tables live when they live on the
 * hosted service, who accepted that, and how to accept it.
 *
 * <p>What is on the node and what is in the cloud is deliberate. On the node,
 * in cluster state and nowhere else: the index names, mappings and settings,
 * and this account — the service's URL, the database, the console URL, and
 * the key, as a filtered persistent setting that no settings API returns and
 * every node reads. In the cloud: every document, the account itself, its
 * credit and its limits. Nothing is kept in any shard.
 *
 * <p>An account is created only when a person accepts. The acceptance is a
 * request to this route carrying {@code "accept": true}, and the key is
 * stored through a cluster-settings update under the caller's own identity,
 * so whoever may not change cluster settings cannot accept, and nothing that
 * runs on its own can. The notice says what leaves the cluster in the words
 * that matter: the documents, not names or metrics.
 *
 * <p>The service, not the node, enforces the account's limits: at a ceiling
 * or out of credit it refuses a write with a message naming the console
 * where billing is added, and the node passes that refusal through with the
 * same status. Nothing here takes a card.
 *
 * <pre>
 * GET    /_plugins/_infino/account         what the tables are served from; the notice when no account is stored
 * POST   /_plugins/_infino/account/trial   {"accept": true, "platform"?, "database"?}: create a free account and store it
 * PUT    /_plugins/_infino/account         {"platform", "api_key", "database"?, "console_url"?}: store an account you have
 * DELETE /_plugins/_infino/account         forget the account; the tables stay on the service
 * </pre>
 */
public final class InfinoAccount {

    private static final Logger log = LogManager.getLogger(InfinoAccount.class);

    public static final String ROUTE = "/_plugins/_infino/account";
    public static final String TRIAL_ROUTE = ROUTE + "/trial";

    /** The hosted service a trial is asked from when neither the request nor the node settings name one. */
    static final String DEFAULT_PLATFORM_URL = "https://api.platform.infino.ws";

    /** The service's rule for a database name: these characters, not starting with an underscore, at most this long. */
    private static final Pattern DATABASE_NAME_OTHER = Pattern.compile("[^A-Za-z0-9_-]");
    private static final Pattern DATABASE_NAME_LEADING = Pattern.compile("^[_-]+");
    private static final int MAX_DATABASE_NAME = 128;
    /** The database name when the cluster's name maps to nothing usable. */
    private static final String FALLBACK_DATABASE_NAME = "opensearch";

    /** The service does not offer a trial: not a fault, a deployment decision. */
    private static final RestStatus NO_TRIAL = RestStatus.NOT_IMPLEMENTED;
    /** This cluster's address has had its trial already. */
    private static final RestStatus TRIAL_USED = RestStatus.CONFLICT;

    private final String defaultPlatformUrl;

    public InfinoAccount(String defaultPlatformUrl) {
        this.defaultPlatformUrl = defaultPlatformUrl;
    }

    /** The service's base URL, with no trailing slash and no database segment. */
    static String baseUrl(String url) {
        return url.replaceAll("/+$", "");
    }

    /**
     * The database name for a cluster: its name, with everything the service
     * does not accept in an identifier replaced, so a person recognises the
     * cluster in a list of databases. A name that maps to nothing usable
     * falls back to a fixed one rather than a generated one, so the same
     * cluster always lands on the same database.
     */
    static String databaseNameFor(String clusterName) {
        String mapped = DATABASE_NAME_LEADING.matcher(DATABASE_NAME_OTHER.matcher(clusterName).replaceAll("-")).replaceAll("");
        if (mapped.length() > MAX_DATABASE_NAME) {
            mapped = mapped.substring(0, MAX_DATABASE_NAME);
        }
        return mapped.isEmpty() ? FALLBACK_DATABASE_NAME : mapped;
    }

    /** What is disclosed before anything leaves the cluster, written to be read once by someone who did not go looking for it. */
    static List<String> notice(String platform) {
        return List.of(
            "The hosted service holds every document of this cluster.",
            "With an account stored, every index this cluster creates is served from a table on " + platform
                + ": the documents themselves, not names or metrics, leave the cluster on every write, and nothing is kept in any shard.",
            "This cluster has no account, so one will be created for it: no email, no password and no card, with free credit to start on."
                + " Its key is stored in the cluster's persistent settings, filtered from every settings API, and read by every node.",
            "When the credit runs out, writes stop and say so; adding billing details then is a choice, not a renewal you have agreed to.",
            "Nothing is uploaded until an account is stored. To accept: POST " + TRIAL_ROUTE + " {\"accept\": true}."
                + " To use an account you already have: PUT " + ROUTE + " {\"platform\": \"https://host\", \"api_key\": \"...\"}."
        );
    }

    /** Answer {@code request} on {@code channel}. */
    void handle(RestRequest request, RestChannel channel) throws IOException {
        Map<String, Object> body = Map.of();
        if (request.hasContent()) {
            try (XContentParser parser = request.contentParser()) {
                body = parser.map();
            }
        }
        boolean trial = request.path().endsWith("/trial");
        switch (request.method()) {
            case GET -> status(channel);
            case POST -> {
                if (trial) {
                    trial(body, channel);
                } else {
                    notAllowed(request, channel);
                }
            }
            case PUT -> store(body, channel);
            case DELETE -> forget(channel);
            default -> notAllowed(request, channel);
        }
    }

    private static void notAllowed(RestRequest request, RestChannel channel) {
        channel.sendResponse(new BytesRestResponse(RestStatus.METHOD_NOT_ALLOWED, request.method() + " " + request.path()));
    }

    private static Settings persistent() {
        ClusterService clusters = InfinoServices.clusterService();
        return clusters == null ? Settings.EMPTY : clusters.state().metadata().persistentSettings();
    }

    private static boolean configured(Settings persistent) {
        return !InfinoPlugin.PLATFORM_URL.get(persistent).isEmpty() && !InfinoPlugin.PLATFORM_API_KEY.get(persistent).isEmpty();
    }

    /** GET: the account's non-secret facts, or the notice. Never the key. */
    private void status(RestChannel channel) throws IOException {
        Settings s = persistent();
        boolean configured = configured(s);
        XContentBuilder b = channel.newBuilder().startObject();
        b.field("configured", configured);
        b.field("serving", InfinoNative.serving());
        if (configured) {
            b.field("platform", InfinoPlugin.PLATFORM_URL.get(s));
            b.field("database", InfinoPlugin.PLATFORM_DATABASE.get(s));
            field(b, "console_url", InfinoPlugin.PLATFORM_CONSOLE_URL.get(s));
            field(b, "accepted_at", InfinoPlugin.PLATFORM_ACCEPTED_AT.get(s));
            field(b, "accepted_by", InfinoPlugin.PLATFORM_ACCEPTED_BY.get(s));
            b.field("key_stored", true);
        } else {
            b.field("notice", notice(defaultPlatformUrl));
        }
        channel.sendResponse(new BytesRestResponse(RestStatus.OK, b.endObject()));
    }

    private static void field(XContentBuilder b, String name, String value) throws IOException {
        if (!value.isEmpty()) {
            b.field(name, value);
        }
    }

    /**
     * POST trial: with {@code accept} true, ask the service for a free
     * account and store it; without it, the notice and nothing else. The
     * service answers 501 when it offers no trial and 409 when this cluster's
     * address has had one; each is passed on with the next step named.
     */
    private void trial(Map<String, Object> body, RestChannel channel) throws IOException {
        if (!Boolean.TRUE.equals(body.get("accept"))) {
            XContentBuilder b = channel.newBuilder().startObject();
            b.field("created", false);
            b.field("notice", notice(platformOf(body)));
            channel.sendResponse(new BytesRestResponse(RestStatus.BAD_REQUEST, b.endObject()));
            return;
        }
        Settings s = persistent();
        if (configured(s)) {
            throw new IllegalArgumentException(
                "[infino] this cluster already has an account on " + InfinoPlugin.PLATFORM_URL.get(s) + "; DELETE " + ROUTE + " first to start over"
            );
        }
        String platform = platformOf(body);
        String database = databaseOf(body);
        String by = principal();
        // The trial call waits on the service; never on the thread that took the request.
        onGeneric(channel, () -> {
            Map<String, Object> response;
            try {
                response = InfinoNative.trial(platform, database);
            } catch (InfinoNative.InfinoException e) {
                throw refused(e, platform);
            }
            Object apiKey = response.get("api_key");
            if (!(apiKey instanceof String key) || key.isEmpty()) {
                throw new InfinoNative.InfinoException("the trial response from " + platform + " carried no key");
            }
            Object named = response.get("database");
            String registered = named instanceof String n && !n.isEmpty() ? n : database;
            Object console = response.get("console_url");
            String consoleUrl = console instanceof String c ? c : "";
            Object credit = response.get("credit_cents");
            long creditCents = credit instanceof Number n ? n.longValue() : 0L;
            Settings stored = Settings.builder()
                .put(InfinoPlugin.PLATFORM_URL.getKey(), platform)
                .put(InfinoPlugin.PLATFORM_DATABASE.getKey(), registered)
                .put(InfinoPlugin.PLATFORM_API_KEY.getKey(), key)
                .put(InfinoPlugin.PLATFORM_CONSOLE_URL.getKey(), consoleUrl)
                .put(InfinoPlugin.PLATFORM_ACCEPTED_AT.getKey(), Instant.now().toString())
                .put(InfinoPlugin.PLATFORM_ACCEPTED_BY.getKey(), by)
                .build();
            InfinoClient.updateSettings(stored, ActionListener.wrap(ignored -> {
                log.info("[infino] a free account was created on {} for database {} and stored, accepted by {}", platform, registered, by);
                XContentBuilder b = channel.newBuilder().startObject();
                b.field("created", true);
                b.field("platform", platform);
                b.field("database", registered);
                b.field("credit_cents", creditCents);
                field(b, "console_url", consoleUrl);
                b.field("accepted_by", by);
                channel.sendResponse(new BytesRestResponse(RestStatus.OK, b.endObject()));
            }, e -> fail(channel, e)));
        });
    }

    /** Why a trial was not granted, as the next step rather than a status code. */
    private static InfinoNative.InfinoException refused(InfinoNative.InfinoException e, String platform) {
        if (e.status() == NO_TRIAL) {
            return new InfinoNative.InfinoException(
                platform + " does not offer free accounts. With a key from its operator: PUT " + ROUTE + " {\"platform\": \"" + platform + "\", \"api_key\": \"...\"}",
                NO_TRIAL
            );
        }
        if (e.status() == TRIAL_USED) {
            return new InfinoNative.InfinoException(
                "this cluster's network address has already used its free account on " + platform + ". Store that account's key instead: PUT " + ROUTE
                    + " {\"platform\": \"" + platform + "\", \"api_key\": \"...\"}",
                TRIAL_USED
            );
        }
        return new InfinoNative.InfinoException("could not get an account from " + platform + ": " + e.getMessage(), e.status());
    }

    /**
     * PUT: store an account the caller already has. The key is used once
     * before it is stored — the database is opened on the service — so a key
     * the service refuses fails here, in front of the person who can fix it,
     * and nothing is stored.
     */
    private void store(Map<String, Object> body, RestChannel channel) {
        Object key = body.get("api_key");
        if (!(key instanceof String apiKey) || apiKey.isEmpty()) {
            throw new IllegalArgumentException("[infino] api_key is required: the account's bearer key");
        }
        String platform = platformOf(body);
        String database = databaseOf(body);
        Object console = body.get("console_url");
        String consoleUrl = console instanceof String c ? c : "";
        String by = principal();
        onGeneric(channel, () -> {
            try {
                InfinoNative.probe(platform, database, apiKey);
            } catch (InfinoNative.InfinoException e) {
                throw new InfinoNative.InfinoException(platform + " refused that key for database " + database + "; nothing was stored: " + e.getMessage(), e.status());
            }
            Settings stored = Settings.builder()
                .put(InfinoPlugin.PLATFORM_URL.getKey(), platform)
                .put(InfinoPlugin.PLATFORM_DATABASE.getKey(), database)
                .put(InfinoPlugin.PLATFORM_API_KEY.getKey(), apiKey)
                .put(InfinoPlugin.PLATFORM_CONSOLE_URL.getKey(), consoleUrl)
                .put(InfinoPlugin.PLATFORM_ACCEPTED_AT.getKey(), Instant.now().toString())
                .put(InfinoPlugin.PLATFORM_ACCEPTED_BY.getKey(), by)
                .build();
            InfinoClient.updateSettings(stored, ActionListener.wrap(ignored -> {
                log.info("[infino] an account on {} for database {} was stored by {}", platform, database, by);
                XContentBuilder b = channel.newBuilder().startObject();
                b.field("stored", true);
                b.field("platform", platform);
                b.field("database", database);
                field(b, "console_url", consoleUrl);
                b.field("accepted_by", by);
                channel.sendResponse(new BytesRestResponse(RestStatus.OK, b.endObject()));
            }, e -> fail(channel, e)));
        });
    }

    /** DELETE: forget the account. The tables stay on the service; storing the key again reaches them. */
    private void forget(RestChannel channel) {
        Settings s = persistent();
        String platform = InfinoPlugin.PLATFORM_URL.get(s);
        Settings cleared = Settings.builder()
            .putNull(InfinoPlugin.PLATFORM_URL.getKey())
            .putNull(InfinoPlugin.PLATFORM_DATABASE.getKey())
            .putNull(InfinoPlugin.PLATFORM_API_KEY.getKey())
            .putNull(InfinoPlugin.PLATFORM_CONSOLE_URL.getKey())
            .putNull(InfinoPlugin.PLATFORM_ACCEPTED_AT.getKey())
            .putNull(InfinoPlugin.PLATFORM_ACCEPTED_BY.getKey())
            .build();
        InfinoClient.updateSettings(cleared, ActionListener.wrap(ignored -> {
            XContentBuilder b = channel.newBuilder().startObject();
            b.field("forgotten", !platform.isEmpty());
            if (!platform.isEmpty()) {
                b.field("note", "the tables stay on " + platform + "; store the account's key again to reach them");
            }
            channel.sendResponse(new BytesRestResponse(RestStatus.OK, b.endObject()));
        }, e -> fail(channel, e)));
    }

    private String platformOf(Map<String, Object> body) {
        Object platform = body.get("platform");
        String url = platform instanceof String p && !p.isEmpty() ? p : defaultPlatformUrl;
        if (!url.startsWith("https://") && !url.startsWith("http://")) {
            throw new IllegalArgumentException("[infino] platform must be an http(s) URL naming the service, e.g. https://host");
        }
        return baseUrl(url);
    }

    private static String databaseOf(Map<String, Object> body) {
        Object database = body.get("database");
        if (database instanceof String d && !d.isEmpty()) {
            if (DATABASE_NAME_OTHER.matcher(d).find() || DATABASE_NAME_LEADING.matcher(d).find() || d.length() > MAX_DATABASE_NAME) {
                throw new IllegalArgumentException("[infino] database must be [A-Za-z0-9_-], not starting with _ or -, at most " + MAX_DATABASE_NAME + " characters");
            }
            return d;
        }
        ClusterService clusters = InfinoServices.clusterService();
        return databaseNameFor(clusters == null ? "" : clusters.getClusterName().value());
    }

    /** Who is accepting or storing, as the security plugin names the caller; "an unauthenticated caller" without it. */
    private static String principal() {
        String user = InfinoScrollActionFilter.principal();
        return user.isEmpty() ? "an unauthenticated caller" : user;
    }

    /** Run {@code work} on the generic pool, answering the channel with any failure. */
    private static void onGeneric(RestChannel channel, Runnable work) {
        ThreadPool threadPool = InfinoServices.threadPool();
        if (threadPool == null) {
            fail(channel, new IllegalStateException("[infino] the node is not ready"));
            return;
        }
        threadPool.generic().execute(() -> {
            try {
                work.run();
            } catch (Exception e) {
                fail(channel, e);
            }
        });
    }

    private static void fail(RestChannel channel, Exception e) {
        try {
            channel.sendResponse(new BytesRestResponse(channel, e));
        } catch (IOException io) {
            log.warn("[infino] the account response could not be sent", io);
        }
    }
}
