# Infino for OpenSearch

![Status: beta](https://img.shields.io/badge/status-beta-yellow) [![License: Apache-2.0](https://img.shields.io/badge/license-Apache--2.0-blue)](LICENSE)

An OpenSearch plugin that stores your indices in [Infino](https://github.com/infino-ai/infino) tables instead of Lucene. Infino is an Apache-2.0 retrieval engine, written in Rust, that keeps its tables on object storage and runs SQL, full-text and vector search over them. The documents live on storage you choose (a local disk, an S3 or GCS bucket, or a hosted Infino account), and OpenSearch's REST API, Dashboards, security plugin and cluster management work unchanged.

- **The same APIs.** Clients and Dashboards talk to the node through OpenSearch's own REST API. An index keeps its name, mapping, settings and aliases.
- **One engine for every query.** Full-text search (BM25), vector search (kNN and hybrid) and SQL run on the same tables.
- **Stateless nodes.** A node with an empty data directory brings every index back from storage, with its mapping and aliases.
- **Not a fork.** The plugin installs into a released OpenSearch: 2.19.6, 3.0.0, 3.3.2, 3.6.0 or 3.9.0.

**Status: beta.** Until general availability, settings and behaviour may change between versions without a migration path: try it on data you can rebuild before relying on it. A query the table does not serve yet is refused with an error that names it, though some searches it does serve can still answer differently from stock OpenSearch with no error. OpenSearch's own YAML REST suite runs against the plugin in both modes, and the tests it does not pass yet are listed, each with its reason, in `plugin/src/yamlRestTest/core-denylist.txt`. The plugin is tested on the releases above with local storage and Dashboards; cloud buckets are not tested yet. CI runs `gradle check` on 3.9.0 only; the other releases are checked by running `check-all.sh` by hand.

**Contents:** [Quick start](#quick-start) · [Getting started guide](docs/getting-started.md) · [How it works](#how-it-works) · [Install on a cluster](#install-on-a-cluster) · [Hosted account and stateless nodes](#hosted-account-and-stateless-nodes) · [Settings](#settings) · [Configuration](docs/configuration.md) · [SQL](#sql) · [What is supported](#what-is-supported) · [Writes](#writes) · [Build and test](#build-and-test) · [Repository layout](#repository-layout) · [License](#license) · [Contributing and security](#contributing-and-security)

## Quick start

There are two ways to run a node with the plugin:

- **a) Docker Compose:** builds the plugin and runs OpenSearch in a container. You need only Docker.
- **b) Local build:** builds the plugin on your machine and installs it into an OpenSearch distribution you've downloaded.

### a) Docker Compose

Docker builds the bridge and the plugin inside the image, so you need neither Rust nor a JDK. Copy `env.example` to `.env` and fill it in before the first start:

```bash
cp env.example .env           # set OPENSEARCH_ADMIN_PASSWORD, and INFINO_API_KEY for an Infino Cloud account
docker compose up --build -d  # the first build compiles the bridge: about 15 minutes
until curl -sk -o /dev/null -w '%{http_code}' -u "admin:<password>" https://localhost:9200/_cluster/health | grep -q 200; do sleep 5; done
curl -sk -u "admin:<password>" https://localhost:9200/_plugins/_infino/account
```

The node answers `OpenSearch Security not initialized` for its first seconds, while the security plugin reads its configuration from the table; the loop waits that out.

`INFINO_STORAGE` in `.env` picks where the tables live: `local` (the default, a Docker volume, or a bucket named by `INFINO_URI`), `cloud` (an [Infino account](#hosted-account-and-stateless-nodes), with `INFINO_API_KEY`) or `memory`. `OPENSEARCH_VERSION` picks the release; `env.example` lists every option. The node serves HTTPS with the security plugin's demo certificates, hence `-k`; set `TLS_CERT` and `TLS_KEY` in `.env` to serve your own certificate instead, on both OpenSearch and Dashboards. `docker compose --profile dashboards up` adds Dashboards on port 5601.

The node's data volume belongs to the storage it was first started with, so to change `INFINO_STORAGE` (or the account, database or URI), run `docker compose down -v` first. This deletes the local volumes, including tables kept on `infino-data`. Use it between `memory` runs too.

### b) Local build

You need a Rust toolchain (the Docker build uses 1.95.0), a JDK 21, Gradle 9, and an [OpenSearch 3.9.0 distribution](https://opensearch.org/downloads/) to run it in. For another release, set `OPENSEARCH_VERSION` and use the Gradle that release wants (see [Build and test](#build-and-test)).

**Using the full OpenSearch bundle.** The standard 3.9.0 download includes the security plugin. With an empty `opensearch.yml`, the security plugin stops the node at startup with `No SSL configuration found`. For a local node that answers plain HTTP, turn security off in `$DISTRO/config/opensearch.yml`:

```yaml
plugins.security.disabled: true
```

To keep security on, run `$DISTRO/opensearch-tar-install.sh` once with `OPENSEARCH_INITIAL_ADMIN_PASSWORD` set. It installs demo certificates, and requests then need `-k -u admin:<password> https://localhost:9200/...`, as in the guide.

The bundle also includes neural search, so the node creates its indices in proxy mode (see [Neural search](#neural-search)).

```bash
./scripts/build.sh                                      # the native bridge and the plugin zip
DISTRO=/path/to/opensearch-3.9.0 ./scripts/run.sh       # install the plugin and start the node
```

OpenSearch 3.3 and later need Gradle 9.4.1 or later: an older Gradle stops with `Gradle 9.4.1+ is required`. [Build and test](#build-and-test) lists the Gradle for older releases.

`run.sh` keeps the tables in `data/` at the repository's root. Set `INFINO_URI=s3://bucket/prefix` or `gs://bucket/prefix` to keep them in a bucket instead; see [where the tables live](#install-on-a-cluster) for the credentials it uses.

### Try it

[Getting started](docs/getting-started.md) walks through the rest: create an index, load 100,000 sample log events (or your own data), and search them with full-text queries, aggregations, vector and hybrid search, and SQL. It keeps the tables on a hosted Infino account; to keep them on the node's own storage instead, skip its step 2. Every index the cluster creates is backed by Infino, so an index needs no setting of its own.

## How it works

The plugin has two parts: a Java plugin (`plugin/`) and a native bridge (`infino-capi/`) over the [`infino`](https://crates.io/crates/infino) crate, which it calls through JNA. Each index is backed by a table, which the bridge declares from the index's mapping:

| Mapping | Column |
|---|---|
| `date` | a timestamp column, with a range index unless the mapping says `"index": false` |
| numeric types | a numeric column, with a range index unless the mapping says `"index": false` |
| `text` | a string column with its own BM25 index |
| `keyword` | a string column compared by exact value, with a term index that keeps each value whole (the engine's `keyword` analyzer), which counts its values for a terms aggregation |
| `knn_vector` | a vector column, searched by the space type the mapping names |

The document id is a column too, with a term index.

A document that the declared columns cannot hold is refused as its own bulk item, with the reason.

An index is served in one of two modes, fixed when it is created:

| | **Default mode** (`index.infino.mode: default`, the default) | **Proxy mode** (`index.infino.mode: proxy`) |
|---|---|---|
| Writes | OpenSearch's own write path: each primary shard writes its documents to its shard's table | Bulk requests are answered before they are routed to a shard |
| Searches | Each shard's query phase is answered from its table | Search, `_count`, each request of an `_msearch`, and get are answered from the table before any shard |
| Lucene | Holds each document's id only | Holds nothing; the index's one shard stays empty |

A node with `infino.default_mode.enabled: false` creates its indices in proxy mode. A search plugin that needs the node's query phase for itself, such as neural search, needs this (see [below](#neural-search)).

Writes are group-committed, and each table is compacted every 60 seconds (`index.infino.optimize_interval`), so a steady stream of events doesn't leave it as thousands of small files.

## Install on a cluster

Build the two artifacts first (`./scripts/build.sh`):

- the native bridge: `infino-capi/target/release/libinfino_capi.so` (`.dylib` on macOS);
- the plugin zip for one OpenSearch release: `plugin/build/distributions/infino-engine.zip`. It installs only into the release it was built for (`OPENSEARCH_VERSION`, default 3.9.0).

Then, on every node:

**1. Install the plugin.**

```bash
bin/opensearch-plugin install -b file:///path/infino-engine.zip
```

**2. Point the JVM at the bridge**, in `config/jvm.options` or `OPENSEARCH_JAVA_OPTS`:

```
-Dinfino.lib=/path/libinfino_capi.so
```

**3. Choose where the tables live.** Every option serves the same OpenSearch APIs, and none writes anything to Lucene.

| Where | How | |
|---|---|---|
| In memory | nothing to set | Lost on restart; for trying things out. |
| On local disk | `-Dinfino.uri=file:///abs/path` | The node that owns the path serves the tables. |
| In your bucket | `-Dinfino.uri=s3://bucket/prefix` or `gs://bucket/prefix` | Every node pointed at it serves the same tables. The node uses the machine's own identity: an EC2 instance role for S3, with the bucket in `us-east-1`, or the GCE service account (or the node user's gcloud application-default credentials) for GCS. Keys in environment variables are not read, and `az://` is not supported by the plugin yet. |
| On an Infino account | `infino.platform.url` in `opensearch.yml`, the key in the keystore | The hosted service runs the engine and manages the tables. See [Hosted account](#hosted-account-and-stateless-nodes). |

When both a URI and an account are set, the account is used. A key belongs to one platform: set `infino.platform.url` to the one that issued it.

In Docker on EC2, the container reaches the instance role only when the instance's metadata hop limit is 2 (`aws ec2 modify-instance-metadata-options --http-put-response-hop-limit 2`). With the default of 1 the node finds no credentials.

When the node starts, it checks once that it can use its storage, and waits at most 30 seconds, a hosted account included. For a local directory it writes a small file there and deletes it. For `s3://` it first asks the instance metadata service for a token, waiting 2 seconds, so a node with no credentials (the hop limit of 1 above) says so at once. For any storage it then lists the tables. If the check fails, the node logs one `ERROR` that names the storage, the reason and what to check. The node stops only when the failure will not pass on its own: the key or the credentials were refused, or the directory may not be written. On a timeout, a server error or a dropped connection it keeps starting, and each index tries the storage again when it is used. If a hosted open is still running when the 30 seconds are up, an index first waits for that open to return, which can take as long as the client's read timeout, and then tries again. The check does not yet catch a bucket or container that does not exist, or object-store credentials that may read but not write.

**4. Start the node.** Every index is Infino-backed unless its settings say `index.infino.enabled: false`. To make the default off, set `infino.index.default_enabled: false` on the node. Give an index one shard and no replicas: the engine scales reads itself.

### Neural search

The standard OpenSearch bundle includes the neural search plugin, which takes the node's one query-phase searcher for its hybrid query; default mode needs that searcher too. With neural search installed, the node creates its indices in proxy mode and says so in its log. To use default mode, remove it:

```bash
bin/opensearch-plugin remove opensearch-neural-search
```

Setting `infino.default_mode.enabled: true` while neural search is installed stops the node, naming both ways out.

Neural search's queries run on Lucene's vectors, which an Infino-backed index does not keep. Use `infino_knn`, `infino_hybrid` or the k-NN plugin's `knn` query instead. To embed documents as they are indexed, use ml-commons' `ml_inference` ingest processor in place of neural search's `text_embedding`. The model is the same; you map its input and output to your fields:

```json
PUT _ingest/pipeline/embed
{"processors": [{"ml_inference": {
  "model_id": "<model id>",
  "input_map": [{"<model input>": "message"}],
  "output_map": [{"message_embedding": "<model output>"}]
}}]}
```

### Upgrading and downgrading

Upgrading to this version is one way for any index that gains a `keyword` field under it. A table's schema then carries the engine's `keyword` analyzer, which only engines from the release that carries [infino-ai/infino#1454](https://github.com/infino-ai/infino/pull/1454) can read. That covers a new index and also a `keyword` field added to an existing one, which a dynamic mapping does whenever a document brings a new string field (its `.keyword` sub-field). In practice, most live indices gain one soon after the upgrade.

- **No downgrade.** A plugin build whose engine predates that release cannot open these tables. Rolling back leaves them unreadable, not only slower.
- **No mixed-version cluster while such indices are written.** During a rolling upgrade, a primary on an upgraded node can declare a column that a node still on the old build cannot read. Upgrade every node, or stop writes to Infino-backed indices until every node runs the new build.

Indices whose `keyword` fields were all declared before the upgrade keep their tables as they are, and terms aggregations on them run as before.

## Hosted account and stateless nodes

With an Infino account, the tables live on the hosted service. It runs the engine, compacts the tables, and serves Infino's own SQL and search APIs over the same data. The account stores the tables in an Infino bucket or in your own bucket, with customer-managed encryption keys (CMEK) supported. The node holds no table data.

[Getting started](docs/getting-started.md) walks through it end to end on the Docker node: an account, data loaded and searched, and a new node with an empty data volume bringing every index back. This section is the reference.

The account is node configuration, read at startup:

```yaml
# opensearch.yml
infino.platform.url: https://host
infino.platform.database: logs        # optional: the cluster's name when unset
```

```bash
bin/opensearch-keystore add infino.platform.api_key    # or INFINO_API_KEY in the node's environment
```

**Getting an account.** Start the node without these settings and ask for a trial account. Sending `"accept": true` accepts Infino's [Terms of Service](https://www.infino.ai/terms), and from then on every document the cluster writes is sent to the hosted service:

```bash
curl -XPOST localhost:9200/_plugins/_infino/account/trial -H 'Content-Type: application/json' -d '{"accept": true}'
```

An account is created only when the request says `"accept": true`. The trial creates an account with starting credit, with no email, password or card. It registers one database, named after the cluster (or `database` in the body), and answers with the key, exactly once, along with the configuration lines above. The node uses the account from then on, and those lines keep it after a restart. If the service gives out no trial accounts, it answers 501; if the cluster's address has already had one, it answers 409. Either way the response names the next step ([errors](docs/getting-started.md#7-limits-and-errors)). `infino.platform.default_url` names the service a trial is asked from.

| Request | |
|---|---|
| `GET /_plugins/_infino/account` | What the tables are served from |
| `POST /_plugins/_infino/account/trial` | Create a trial account (`{"accept": true}`) |
| `PUT /_plugins/_infino/account` | Use an account you have: `{"api_key": "..."}`, with optional `platform` (`infino.platform.default_url` when unset) and `database` (the cluster's name when unset). The key is tried once first, so a wrong key fails here. |
| `DELETE /_plugins/_infino/account` | Stop using the account on this node; the tables stay on the service |
| `GET /_plugins/_infino/account/indices` | What would come back on a fresh cluster (`?name=<index>` for one) |
| `POST /_plugins/_infino/account/indices` | Bring it back now: the cluster state a loader registered (templates, pipelines, scripts, settings, users and roles), then every index the cluster lacks (`?name=<index>` for one) |

**Limits.** The service enforces the account's limits. At a ceiling or out of credit, it refuses a write with a message naming the console where billing is added, and the bulk fails with that status and message.

**Stateless nodes.** Nothing a node needs lives only in the cluster. A registry kept beside the tables holds one row per index: its settings, mapping and aliases, written by the elected cluster manager as the index changes and deleted with it. A node with an empty data directory boots, reads the registry, and creates every index it names. The tables are named by the index, so the rows are there before the first request. Cluster state is a cache of the registry, rebuilt at boot. The same holds for a `file://` or bucket URI of the node's own.

The security plugin's index is Infino-backed too, so its roles, users and password hashes live in a table under the same credentials as the data, and a fresh node reads them from there without a `securityadmin` run. To keep that index in Lucene instead, put an index template on `.opendistro_security` with `index.infino.enabled: false`; such a node then needs `securityadmin` (or the demo initialization) after a restart with an empty data directory. Dashboards' saved objects come back the same way.

## Settings

None of the settings has to change for a default run: each has a default that works as it is, and the quick start and the [getting-started guide](docs/getting-started.md) don't ask you to set any. [Configuration](docs/configuration.md) lists every index setting, node setting and JVM property, and covers [attaching an index to an existing table](docs/configuration.md#attaching-to-an-existing-table) and [limits for very large tables](docs/configuration.md#very-large-tables).

## SQL

`POST /_plugins/_infino/sql` runs one read-only SQL statement over Infino-backed indices:

```bash
curl -XPOST localhost:9200/_plugins/_infino/sql -H 'Content-Type: application/json' -d '{
  "query": "SELECT service, count(*) AS n FROM errors GROUP BY service ORDER BY n DESC"}'
```

`errors` is the index the [guide](docs/getting-started.md#sql) makes. The answer is `{"schema": [{"name", "type"}], "rows": [...]}`. A statement names indices (an index, an alias, or a pattern in quotes), never tables. The security plugin authorizes the caller for each index as it would for a search, and the statement sees each index as the caller does: hidden fields left out, masked fields masked, and only the rows the caller's document rules and filtering aliases allow. System indices are refused.

An index with `index.infino.edges: true` holds a graph's edges: each document is one edge, `{src_table, src_id, dst_table, dst_id}`, naming two rows by the `_table` and `_id` a SQL read gives each row. `graph_walk` and `graph_rank` walk the edges; each compaction (and, in default mode, a force merge) builds the index they read.

## What is supported

**Queries:** `match_all`, `term`, `terms`, `range` (numbers and dates, with date math and time zones), `exists`, `prefix`, `wildcard`, `regexp`, `ids`, `match`, `match_phrase`, `multi_match`, `query_string` and `simple_query_string` (Lucene's classic syntax; fuzzy and proximity are refused), `bool` (with `minimum_should_match`), `nested` (see below), `script` (an expression subset of Painless), `infino_knn`, `infino_hybrid`, and the k-NN plugin's `knn`. A field named `x.keyword` reads the string column `x` by its whole value.

A `nested` query (Dashboards' `has_reference` on saved objects' `references` is one) matches a document only when one entry of the array holds every clause, as stock does. The table flattens an array of objects into a column per member field, which loses which values share an entry, so the columns only narrow the documents to those that may match; each of those is then checked entry by entry from its source. Inside a nested query, `term`, `terms`, `match`, `range` and `exists` on keyword, numeric and boolean fields are served, combined by `bool` or a deeper `nested` query. A text or date field, any other clause, `inner_hits`, or a query that leaves more than 10,000 documents to check (`-Dinfino.nested.candidates.max`) is refused with a 400 that names it. The entries are read from the document as it was indexed, whatever the mapping's `_source` says, so a nested query is served on an index with `_source` disabled or with the path excluded from it. A nested query scores as stock scores it: each matching entry scores as its query does on it, and the entries' scores are read by `score_mode` (`avg` by default), times the query's `boost` (see Scoring).

**Scoring:** a `bool` scores as stock sums its clauses. The BM25 of one text clause (a `match` or `match_phrase` on a text field) counts on the hits it matches, in `must` or in `should`. A `terms`, `range`, `exists`, `ids`, `prefix`, `wildcard`, `regexp` or `match_all`, and a `term`, `match` or `match_phrase` on any other field but a text one, adds its `boost` (1.0 by default) on the hits it matches, as stock's constant-score queries do. A `term`, `match` or `match_phrase` on a boolean field, or on a keyword field with `use_similarity`, adds its BM25, as stock scores it: such a field has no norms, so every hit it matches scores `boost × idf / 2.2`, with the idf counted over the table's documents that hold the field. A `nested` clause adds its entries' score. These hold in `must` and in `should` alike. A `filter` or `must_not` clause adds nothing. When `should` or `nested` clauses reorder the hits, hits that tie on score come in the order they were written. A page ranked by a text clause alone keeps its ties in the order the engine's ranked search returns them. A request sorted by `_doc` first is not reordered by its other clauses' scores: with no text clause its hits come in the order they were written, as stock's do. A request sorted by `_score` ascending first, with no text clause, gets its least scores first, as stock's does. Where the score differs from stock:

- Only one text clause is scored. A second text clause in `must`, or a text clause in `should` beside one in `must`, adds nothing to the score. With no text clause in `must`, two text clauses in `should`, or any `should` or `must` clause the table does not score (a `term` on a text field, a `multi_match` over several fields, a `query_string`, a `bool` inside it), leave every hit the same score: 1.0, or what the `must` clauses above add. So does a `constant_score` clause in `should`, a `term` on a boolean or `use_similarity` keyword field that some document gives several values, and a `nested` clause whose entries such a `term` scores.
- A text clause in `filter` scores its BM25, where stock scores it 0. A `bool` of `filter` clauses alone scores every hit 1.0, where stock scores 0.
- A `boost` on a text clause or on a `bool` is not applied.
- Sorted by `_doc` first, a page with a text clause is still ranked by that clause's BM25, where stock returns it in index order.
- Sorted by `_score` ascending first, a page with a text clause is scored as if sorted by `_doc` (above): a text clause in `must` ranks it by BM25, greatest first, where stock returns the least scores first.
- When the scoring clauses' scores can sum to more than 64 different values, or the query holds a clause the engine applies after ranking (on a field some document gives several values, or a text clause on a second field), the hits the text clause matches are chosen from its 10,000 best by BM25, so a hit past them that the other clauses lift can be missed.
- Under `collapse`, `should` clauses and `nested` clauses add nothing to the score, and a text clause in `should` is not scored: with no text clause in `must`, every hit scores the same, 1.0 or what the `must` clauses above add.

**Search:** sorting by fields, with `missing` as `_last` (the default), `_first` or a value; `from` and `size` up to `index.max_result_window`; `search_after`, `post_filter`, `collapse` and scroll; `_source` includes and excludes; exact `hits.total`. A hit, a `get` and an `mget` carry `_source` as the mapping's `_source` keeps it, as stock does: none when it is disabled, and only what its `includes` and `excludes` keep (the table still holds the document as it was indexed). A search that filters `_source` on an index whose mapping disables it is refused with a 400, as stock refuses it. The `fields` API reads the source as the mapping keeps it, so it leaves out what the mapping excludes, and asking for `fields` on an index whose mapping disables `_source` is refused with stock's 400; `docvalue_fields` read the document as it was indexed, as stock's doc values hold every field. As in stock, only a hit the response carries is refused, so a page past that index's hits is answered and the total counts them. When one index of a search over several refuses it this way, stock and default mode fail only that index's shards and answer from the rest; proxy mode answers the search as one, so the whole search fails with the 400 when the page carries a hit of that index. One difference from stock: a page that continues from a hit's sort values (`search_after` or a scroll) reads a numeric value equal to the one a missing row hands back on that side (the least or greatest long, or an infinity) as missing, so on a sort with no `missing` value it skips the remaining rows that really hold that value. Matching stock would mean ordering every numeric sort by its value or the stand-in, which gives up the engine's range-index top-k. A search over several Infino-backed indices merges their pages by sort values or score and reduces their aggregations together, under `search.max_buckets`. `get` and `mget` read the table too.

**Aggregations:** `value_count`, `sum`, `avg`, `min`, `max`, `cardinality` (exact), `percentiles` (t-digest), `percentile_ranks`; `terms` over string, boolean, numeric and date columns, with `size` and `min_doc_count`, ordered by count, key or a sibling metric; `date_histogram` (fixed or calendar intervals, time zone, a fixed offset, `min_doc_count`, numeric `extended_bounds`); `histogram` (interval and offset); `filters` (with `other_bucket`); `range`; `date_range`; `composite`; `top_hits`; `significant_terms`; bucket aggregations nested to any depth; pipeline aggregations.

**Vector search.** `knn_vector` is the k-NN plugin's own mapping type, so an index with vectors runs beside the k-NN plugin, and its `knn` query is answered from the table just as it would be on a Lucene index:

```json
{"query": {"knn": {"<field>": {"vector": [...], "k": 10, "filter": {...}}}}}
```

- **Results.** The engine returns the k nearest documents, and the page is a window over them.
- **Filters.** A `filter` is applied before the k nearest are picked, as the k-NN plugin's filtered search does. When too few of the nearest documents match, the engine searches within exactly the documents the filter keeps. A filter takes the term-level queries above; a full-text query inside one is refused.
- **Scoring.** Hits are scored as the k-NN plugin scores them for the field's `space_type`: `l2`, `innerproduct` or `cosinesimil`. Any other space type is refused. Name the `space_type` in the mapping: a vector field mapped without one is treated as cosine.
- **Where `knn` goes.** `knn` is the whole query, not a clause inside `bool`.
- **Refused, with the parameter named:** radial search (`max_distance`, `min_score`), `method_parameters`, `rescore`, `expand_nested_docs`, `ignore_unmapped`, a boost or query name. With a `knn` query, aggregations, field sorts, `post_filter`, `collapse`, `search_after` and scroll are refused too.

**Not supported:** HDR percentiles; `fuzzy`, `function_score` and geo queries; `suggest`; sliced search (sliced scroll, reindex or update by query); and a search across Infino-backed and other indices together. A request using any of these fails with a 400 that names the clause (`[infino] query not served by the table: ...`); it does not return an empty answer instead.

## Writes

**Default mode.** Writes take OpenSearch's own path: each primary shard indexes the document, writes it to the translog and replicates it as usual, and adds it to the shard's batch. The batch is committed to the table when the translog syncs, on a refresh or a flush, or when it reaches `infino.engine.batch.docs` or `infino.engine.batch.bytes`, after the shard's in-sync copies hold the operations it covers. Until a commit takes them, the translog keeps the writes, so a node that stops replays them.

**Proxy mode.** Each engine append is a commit, and a bulk is acknowledged only after the append carrying its writes, so an acknowledged write is durable.

- **Visibility.** Every acknowledged write is refreshed. `index.refresh_interval` does not delay it, and a get with `realtime=false` finds it as a search does; stock would wait for the next refresh. On the hosted service a read may trail a write by `infino.platform.read_window`; with `refresh=true` or `wait_for`, the write is acknowledged only after that window, so the next search sees it.
- **Grouped commits.** A bulk that arrives while no append is running starts one at once. Bulks that arrive during an append go into the next one.
- **Replace, delete, update.** Indexing an existing id replaces it, and a delete removes it. An update merges into the document (partial `doc`, `doc_as_upsert`, `upsert`); update scripts are refused. Updates apply in order, including two updates of one document in the same bulk.
- **No lookup for new ids.** Which ids exist is answered by the id column's term index, so a stream of new documents pays no lookup.
- **Several indices.** A bulk over several indices is applied index by index, and its items are answered in order.

In proxy mode, a batch's writes and deletes are two mutations, with the deletes after the append. If a node stops between them, the batch is not acknowledged; the client's retry applies both again, and both are idempotent. If a delete fails after its append committed, only the delete items fail (500).

## Build and test

```bash
./scripts/build.sh          # the bridge and the plugin zip, for OPENSEARCH_VERSION (default 3.9.0)
cd plugin && gradle check   # precommit, unit tests, and the REST suites on a test cluster with the bridge
./scripts/check-all.sh      # the same check on every supported release
```

`gradle check` loads the bridge as `libinfino_capi.so`; on macOS pass `-Dinfino.lib=$PWD/../infino-capi/target/release/libinfino_capi.dylib`. `check-all.sh` reads the bridge from `INFINO_LIB` and the releases from `RELEASES` (default `3.9.0 3.6.0 3.3.2 3.0.0 2.19.6`).

OpenSearch's own YAML REST suites (index, get, bulk, update, delete, search, aggregations, scroll and the rest of the document and search APIs, from the release's `rest-api-spec` artifact) run in tasks of their own, outside `check`. CI does not run them, so run them before a change to how documents are written or searched:

```bash
cd plugin
gradle coreYamlRestTest         # every index in default mode
gradle coreYamlRestTestProxy    # every index in proxy mode
gradle coreYamlRestTestLucene   # the same cluster without the plugin: the baseline
```

A test that fails on the baseline fails because of the test cluster (the security plugin, the single node), not because of Infino.

Tests known not to pass are skipped, so a run reports 0 failures and any failure is a regression. They are listed in `plugin/src/yamlRestTest/core-denylist.txt`, one per line, each tagged and with its reason: `[gap]` for a test that needs a feature Infino does not have yet, `[bug]` for any other failure (a wrong answer, a 500, or a refusal of what an existing feature should handle), `[by-design]` for a test that asserts stock behaviour Infino improves on purpose (the first three narrowed to one mode as `[gap:proxy]`, `[by-design:proxy]` or `[bug:default]`), and `[cluster]` for a test that fails on the baseline too. `-Dinfino.core.skip` chooses how much is skipped:

```bash
gradle coreYamlRestTest -Dinfino.core.skip=all    # the default: every entry for the mode
gradle coreYamlRestTest -Dinfino.core.skip=gaps   # all but [bug]: the known bugs run and fail
gradle coreYamlRestTest -Dinfino.core.skip=none   # nothing: shows which entries pass now
```

`scripts/core-denylist.py default|proxy|lucene` reads a run's results and prints the failures no entry covers, as lines to tag and add; the entries whose tests that ran all passed, which can go; and the `[bug]` entries a wider entry of another tag already skips, which `skip=gaps` cannot run. It exits 1 when a failure is not covered, including one outside any YAML test, such as a suite whose cluster failed during setup. It reads the XML the task left, so check the task itself ran.

No OpenSearch checkout is needed: the plugin builds against the release named by `OPENSEARCH_VERSION`, pulled from Maven. `gradle check` builds against one release. The security integration takes a different path on 2.x, on 3.0 to 3.5, and on 3.6 and later, so run `check-all.sh` before relying on any release but the default. Each release's build tools want their own Gradle:

| Releases | Gradle | `check-all.sh` reads it from |
|---|---|---|
| 2.19.x | 8.12.1 | `GRADLE_8_12` |
| 3.0.x | 8.14 | `GRADLE_8_14` |
| 3.3 and later | 9.4.1 or later | `GRADLE` |

## Repository layout

| Path | |
|---|---|
| `plugin/` | The OpenSearch plugin (`org.opensearch.infino`): the engine and query phase of default mode, the bulk, search and get filters of proxy mode, the query translator, aggregations, the batching writer, kNN and hybrid queries, and the SQL route. |
| `infino-capi/` | The native bridge, a Rust `cdylib` over the `infino` crate: JSON in, JSON out, with every document checked against the declared columns. |
| `scripts/` | `build.sh`, `run.sh` (install into a distribution and start a node) and `check-all.sh`. |
| `bench/` | `bench.py`: indexing and query latency against stock Lucene. `local-nodes.sh` starts a stock, a default-mode and a proxy-mode node side by side, and `big5.py` loads big5 documents into them and times big5's searches against stock's answers (both Linux only). |

## License

Apache-2.0; see [LICENSE](LICENSE). The bridge and the plugin use only the published `infino` crate, which is Apache-2.0 too.

## Contributing and security

Bugs and feature requests go in [the issues](https://github.com/infino-ai/opensearch-infino/issues), and pull requests are welcome: [CONTRIBUTING.md](CONTRIBUTING.md) covers the build, the checks a pull request must pass, and the sign-off each commit needs. Everyone taking part follows the [code of conduct](CODE_OF_CONDUCT.md). Report a security problem privately, as [SECURITY.md](SECURITY.md) describes, not in a public issue.
