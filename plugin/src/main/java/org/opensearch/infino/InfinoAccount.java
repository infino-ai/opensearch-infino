/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.cluster.service.ClusterService;
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
import java.util.Locale;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * The node's hosted account: where its tables live when they live on the
 * hosted service, who set that up, and how to.
 *
 * <p>A node is stateless. The account is node configuration — the service
 * URL and the database in opensearch.yml, the key in the node's keystore —
 * read at boot; nothing about it lives in the cluster, and a node with an
 * empty data directory serves the account's tables as soon as it is up,
 * its indices rebuilt from the registry kept beside the tables (see
 * {@link InfinoRegistry}). In the cloud: every document, the account, its
 * credit and its limits. Nothing is kept in any shard.
 *
 * <p>An account is created only when a person accepts, and only one the
 * cluster lets manage it: every request here that points the node at a
 * service, or away from one, is first authorized by the cluster as a
 * cluster-management action under the caller's own identity, so a user
 * without that right is refused and nothing that runs on its own can do
 * it. The acceptance is a request carrying {@code "accept": true}; the service answers
 * with the key, exactly once, and this node uses the account from then on,
 * for its life. The response carries the key and the configuration lines
 * that make it the node's own after a restart; nothing here stores it. The
 * notice says what leaves the cluster in the words that matter: the
 * documents, not names or metrics.
 *
 * <p>The service, not the node, enforces the account's limits: at a ceiling
 * or out of credit it refuses a write with a message naming the console
 * where billing is added, and the node passes that refusal through with the
 * same status. Nothing here takes a card.
 *
 * <pre>
 * GET    /_plugins/_infino/account          what the tables are served from; the notice when no account is set
 * GET    /_plugins/_infino/account/indices  the indices the registry would bring back on a fresh cluster, and the cluster state registered with them
 * POST   /_plugins/_infino/account/indices  bring them back now: apply the registered cluster state, then create every index the cluster lacks (with ?name=, that one)
 * POST   /_plugins/_infino/account/trial    {"accept": true, "platform"?, "database"?}: create a trial account and use it
 * PUT    /_plugins/_infino/account          {"platform", "api_key", "database"?}: use an account you have
 * DELETE /_plugins/_infino/account          stop using the account on this node; the tables stay on the service
 * </pre>
 */
public final class InfinoAccount {

    private static final Logger log = LogManager.getLogger(InfinoAccount.class);

    public static final String ROUTE = "/_plugins/_infino/account";
    public static final String TRIAL_ROUTE = ROUTE + "/trial";
    public static final String INDICES_ROUTE = ROUTE + "/indices";

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

    /** How the node came by its account. */
    private static final String FROM_SETTINGS = "node settings";
    private static final String FROM_ROUTE = "this route, for this node's life; the settings in `config` make it last";

    /** The account in use, as this route reports it. Never the key. */
    private record Live(String platform, String database, String consoleUrl, String since, String by, String source) {}

    private final String defaultPlatformUrl;
    private volatile Live live;

    public InfinoAccount(String defaultPlatformUrl) {
        this.defaultPlatformUrl = defaultPlatformUrl;
    }

    /** The node's settings named an account at boot. */
    void configuredAtBoot(String url, String database, boolean keyInKeystore) {
        live = new Live(baseUrl(url), database, "", "", keyInKeystore ? "the keystore" : "the environment", FROM_SETTINGS);
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
            "With an account in use, every index this cluster creates is served from a table on " + platform
                + ": the documents themselves, not names or metrics, leave the cluster on every write, and nothing is kept in any shard.",
            "This node has no account, so one will be created for it: no email, no password and no card, with starting credit."
                + " The key is returned once, to you; the node uses it for its life, and the configuration lines in the response make it the node's own after a restart.",
            "When the credit runs out, writes stop and say so; adding billing details then is a choice, not a renewal you have agreed to.",
            "Nothing is uploaded until an account is in use. To accept: POST " + TRIAL_ROUTE + " {\"accept\": true}."
                + " To use an account you already have: PUT " + ROUTE + " {\"platform\": \"https://host\", \"api_key\": \"...\"}."
        );
    }

    /** The lines that make {@code platform} and {@code database} the node's own account after a restart. */
    static List<String> configLines(String platform, String database) {
        return List.of(
            "opensearch.yml: infino.platform.url: " + platform,
            "opensearch.yml: infino.platform.database: " + database,
            "keystore: opensearch-keystore add infino.platform.api_key (or INFINO_API_KEY in the node's environment)"
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
        boolean indices = request.path().endsWith("/indices");
        switch (request.method()) {
            case GET -> {
                if (indices) {
                    // What the registry would bring back names every index, its
                    // aliases and their filters, and the users, roles and role
                    // mappings a loader registered: the cluster's management,
                    // as bringing them back is.
                    String name = request.param("name");
                    asClusterAdmin(channel, () -> indices(name, channel));
                } else {
                    status(channel);
                }
            }
            case POST -> {
                if (trial) {
                    Map<String, Object> accepted = body;
                    asClusterAdmin(channel, () -> trial(accepted, channel));
                } else if (indices) {
                    String name = request.param("name");
                    asClusterAdmin(channel, () -> restore(name, request, channel));
                } else {
                    notAllowed(request, channel);
                }
            }
            case PUT -> {
                Map<String, Object> given = body;
                asClusterAdmin(channel, () -> store(given, channel));
            }
            case DELETE -> asClusterAdmin(channel, () -> forget(channel));
            default -> notAllowed(request, channel);
        }
    }

    /**
     * Run {@code then} only for a caller the cluster lets manage it.
     * Pointing the node's data plane at a service is cluster management, so
     * the same right gates it: the cluster is asked, under the caller's own
     * identity, and a refusal (403 from the security plugin) is the answer.
     * Without the security plugin every caller has the right.
     */
    private static void asClusterAdmin(RestChannel channel, Work then) {
        InfinoClient.requireClusterManagement(ActionListener.wrap(allowed -> {
            try {
                then.run();
            } catch (Exception e) {
                fail(channel, e);
            }
        }, e -> fail(channel, e)));
    }

    private static void notAllowed(RestRequest request, RestChannel channel) {
        channel.sendResponse(new BytesRestResponse(RestStatus.METHOD_NOT_ALLOWED, request.method() + " " + request.path()));
    }

    /** GET: the account's non-secret facts, or the notice. Never the key. */
    private void status(RestChannel channel) throws IOException {
        Live account = live;
        XContentBuilder b = channel.newBuilder().startObject();
        b.field("configured", account != null);
        b.field("serving", InfinoNative.serving());
        if (account != null) {
            b.field("platform", account.platform());
            b.field("database", account.database());
            field(b, "console_url", account.consoleUrl());
            field(b, "since", account.since());
            field(b, "by", account.by());
            b.field("source", account.source());
            b.field("config", configLines(account.platform(), account.database()));
        } else {
            b.field("notice", notice(defaultPlatformUrl));
        }
        channel.sendResponse(new BytesRestResponse(RestStatus.OK, b.endObject()));
    }

    /**
     * GET indices: what the registry would bring back on a fresh cluster;
     * with {@code name}, whether that one index is registered and how.
     */
    private void indices(String name, RestChannel channel) {
        onGeneric(channel, () -> {
            List<Map<String, Object>> rows = InfinoRegistry.rows();
            XContentBuilder b = channel.newBuilder().startObject();
            b.field("serving", InfinoNative.serving());
            if (name != null) {
                Map<String, Object> found = null;
                for (Map<String, Object> row : rows) {
                    if (name.equals(row.get("name"))) {
                        found = row;
                    }
                }
                b.field("name", name);
                b.field("registered", found != null);
                if (found != null) {
                    index(b, found);
                }
            } else {
                b.startArray("indices");
                for (Map<String, Object> row : rows) {
                    b.startObject();
                    b.field("name", row.get("name"));
                    index(b, row);
                    b.endObject();
                }
                b.endArray();
                // The cluster state the next POST applies before the indices.
                b.startArray("state");
                for (InfinoClusterState.Item item : InfinoClusterState.registered()) {
                    b.startObject();
                    b.field("kind", item.kindKey());
                    b.field("name", item.name());
                    b.endObject();
                }
                b.endArray();
            }
            channel.sendResponse(new BytesRestResponse(RestStatus.OK, b.endObject()));
        });
    }

    /**
     * POST indices: create every index the registry names that the cluster
     * lacks, now, as a fresh cluster does at boot; with {@code name}, that
     * one. What serves an index whose table and registry row a loader wrote
     * while the cluster runs (a snapshot load into the plugin's table),
     * with no restart. Creating indices is cluster management, so the
     * caller needs that right. The cluster state a loader registered with
     * them (templates, pipelines, scripts, settings, the security plugin's
     * users and roles) is applied first, as the caller, so each index comes
     * up with its templates and pipelines in place ({@link InfinoClusterState}).
     * Answers with each registered item and index and what became of it:
     * applied, created, present, or failed with the reason.
     */
    private void restore(String name, RestRequest request, RestChannel channel) {
        ClusterService clusters = InfinoServices.clusterService();
        if (clusters == null) {
            throw new IllegalStateException("[infino] the node is not ready");
        }
        onGeneric(channel, () -> InfinoClusterState.apply(request, ActionListener.wrap(applied -> restore(name, applied, clusters, channel), e -> fail(channel, e))));
    }

    /** Create the registered indices once {@code applied} is the cluster state applied before them. */
    private void restore(String name, List<InfinoClusterState.Applied> applied, ClusterService clusters, RestChannel channel) {
        InfinoRegistry.restore(clusters.state(), name, ActionListener.wrap(restored -> {
            XContentBuilder b = channel.newBuilder().startObject();
            b.field("serving", InfinoNative.serving());
            InfinoClusterState.write(b, applied);
            if (name != null) {
                b.field("name", name);
                b.field("registered", !restored.isEmpty());
            }
            b.startArray("indices");
            for (InfinoRegistry.Restored r : restored) {
                b.startObject();
                b.field("name", r.name());
                b.field("result", r.outcome().name().toLowerCase(Locale.ROOT));
                if (r.reason() != null) {
                    b.field("reason", r.reason());
                }
                b.endObject();
            }
            b.endArray();
            channel.sendResponse(new BytesRestResponse(RestStatus.OK, b.endObject()));
        }, e -> fail(channel, e)));
    }

    private static void index(XContentBuilder b, Map<String, Object> row) throws IOException {
        b.field("table", row.get("table"));
        Object aliases = row.get("aliases");
        if (aliases instanceof Map<?, ?> all) {
            b.field("aliases", all.keySet().stream().map(Object::toString).sorted().toList());
        }
    }

    private static void field(XContentBuilder b, String name, String value) throws IOException {
        if (value != null && !value.isEmpty()) {
            b.field(name, value);
        }
    }

    /**
     * POST trial: with {@code accept} true, ask the service for a free
     * account and use it; without it, the notice and nothing else. The
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
        Live account = live;
        if (account != null) {
            throw new IllegalArgumentException(
                "[infino] this node already uses an account on " + account.platform() + "; DELETE " + ROUTE + " first to start over"
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
            use(platform, registered, key, consoleUrl, by);
            log.info("[infino] a trial account was created on {} for database {} and is in use, accepted by {}", platform, registered, by);
            XContentBuilder b = channel.newBuilder().startObject();
            b.field("created", true);
            b.field("platform", platform);
            b.field("database", registered);
            b.field("credit_cents", creditCents);
            field(b, "console_url", consoleUrl);
            b.field("accepted_by", by);
            // Returned exactly once by the service and once here: it is not
            // recoverable later, and nothing on the node keeps it past a restart.
            b.field("api_key", key);
            b.field("config", configLines(platform, registered));
            channel.sendResponse(new BytesRestResponse(RestStatus.OK, b.endObject()));
        });
    }

    /** Why a trial was not granted, as the next step rather than a status code. */
    private static InfinoNative.InfinoException refused(InfinoNative.InfinoException e, String platform) {
        if (e.status() == NO_TRIAL) {
            return new InfinoNative.InfinoException(
                platform + " does not offer trial accounts. With a key from its operator: PUT " + ROUTE + " {\"platform\": \"" + platform + "\", \"api_key\": \"...\"}",
                NO_TRIAL
            );
        }
        if (e.status() == TRIAL_USED) {
            return new InfinoNative.InfinoException(
                "this cluster's network address has already used its trial account on " + platform + ". Use that account's key instead: PUT " + ROUTE
                    + " {\"platform\": \"" + platform + "\", \"api_key\": \"...\"}",
                TRIAL_USED
            );
        }
        return new InfinoNative.InfinoException("could not get an account from " + platform + ": " + e.getMessage(), e.status());
    }

    /**
     * PUT: use an account the caller already has. The key is used once
     * before it is taken — the database is opened on the service — so a key
     * the service refuses fails here, in front of the person who can fix it.
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
                throw new InfinoNative.InfinoException(platform + " refused that key for database " + database + "; nothing changed: " + e.getMessage(), e.status());
            }
            use(platform, database, apiKey, consoleUrl, by);
            log.info("[infino] an account on {} for database {} is in use, set by {}", platform, database, by);
            XContentBuilder b = channel.newBuilder().startObject();
            b.field("stored", true);
            b.field("platform", platform);
            b.field("database", database);
            field(b, "console_url", consoleUrl);
            b.field("accepted_by", by);
            b.field("config", configLines(platform, database));
            channel.sendResponse(new BytesRestResponse(RestStatus.OK, b.endObject()));
        });
    }

    /** Take the account into use on this node: the connection reopens to it. */
    private void use(String platform, String database, String apiKey, String consoleUrl, String by) {
        InfinoNative.configure(InfinoNative.Target.hosted(platform, database, apiKey));
        live = new Live(platform, database, consoleUrl, Instant.now().toString(), by, FROM_ROUTE);
    }

    /** DELETE: stop using the account on this node. The tables stay on the service; a node configured with it keeps it after a restart. */
    private void forget(RestChannel channel) throws IOException {
        Live account = live;
        live = null;
        InfinoNative.configure(InfinoNative.Target.node());
        XContentBuilder b = channel.newBuilder().startObject();
        b.field("forgotten", account != null);
        if (account != null) {
            b.field("note", "the tables stay on " + account.platform() + "; use the account's key again to reach them");
        }
        channel.sendResponse(new BytesRestResponse(RestStatus.OK, b.endObject()));
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
    private static void onGeneric(RestChannel channel, Work work) {
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

    /** Work that answers a channel and may fail with a checked exception. */
    @FunctionalInterface
    private interface Work {
        void run() throws Exception;
    }

    private static void fail(RestChannel channel, Exception e) {
        try {
            channel.sendResponse(new BytesRestResponse(channel, e));
        } catch (IOException io) {
            log.warn("[infino] the account response could not be sent", io);
        }
    }
}
