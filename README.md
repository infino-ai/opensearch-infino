# opensearch-infino

Back **OpenSearch** with the **Infino** engine for storage, full-text (BM25),
vector (kNN / hybrid) and analytics (SQL aggregations), delivered as an
**OpenSearch plugin**, not a fork. The OpenSearch REST API and cluster
framework stay intact; an index keeps its name, mapping and settings in
cluster state, and every document lives in an Infino table on the hosted
service or on storage of your own (`file://`, `s3://`, `gs://`, `az://`).
Nothing is written to Lucene.

The plugin answers at the **action layer**: bulk, search (so also `_count`
and each request of an `_msearch`) and get for an Infino-backed index are
served from the table before they are routed to any shard. The index's one
shard stays empty. Nodes are stateless: a node with an empty data directory
boots, reads a registry kept beside the tables and brings every index back
with its mapping and aliases; the rows are where they were.

The original spike and its findings are in [HANDOVER.md](HANDOVER.md). Since
then the plugin has been shaped for log and event streams: the table is
declared from the index **mapping** (dates are timestamp columns, text fields
carry their own BM25 index, keywords are exact columns, the id column carries
a term index), search serves date ranges with date math, field sorts,
field-scoped text search with filters, and the aggregations dashboards ask
for (`date_histogram`, `terms`, `histogram`, `filters`, `range`, metrics
including `cardinality` and `percentiles`, nested to any depth), writes group
commit across bulk requests and the table is compacted on a schedule, and a
document the mapping cannot hold fails as its own bulk item.

## What's here

| path | what |
|---|---|
| `infino-capi/` | Rust **C-ABI bridge** (`cdylib`) over the published `infino` crate. JSON in, JSON out; per-document checks against the declared columns. |
| `plugin/` | The **OpenSearch Java plugin** (`org.opensearch.infino`): bulk, search and get action filters, the query translator, the aggregation processor, the batching writer, custom kNN/hybrid queries, SQL passthrough. Talks to the bridge via JNA. Builds out of tree against a released OpenSearch. |
| `bench/` | `bench.py`: indexing + query-latency comparison vs stock Lucene. |
| `scripts/` | `build.sh` (bridge + plugin zip), `run.sh` (install into a distribution + start a node). |

## Build

Needs a Rust toolchain, a JDK 21 and Gradle. No OpenSearch checkout: the
plugin builds against the release named by `OPENSEARCH_VERSION` (default
3.9.0), pulled from Maven, and the zip installs only into that exact version.
One source tree builds for 2.19 through 3.9; the full check passes on
2.19.6, 3.0.0, 3.3.2, 3.6.0 and 3.9.0 (the build tools of 2.19 want Gradle
8.12.1, those of 3.0 want 8.14, 3.3 and later take 9.x).

```bash
./scripts/build.sh                      # infino-capi/target/release/libinfino_capi.{so,dylib} + plugin/build/distributions/infino-engine-<ver>.zip
cd plugin && gradle check               # precommit gates, unit tests, YAML REST suites on a test cluster with the bridge
```

## Install and use

There is no published release yet, so the two artifacts come from the build
above: the native bridge (`infino-capi/target/release/libinfino_capi.so`, or
`.dylib` on macOS) and the plugin zip for one OpenSearch version
(`plugin/build/distributions/infino-engine-<version>.zip`).

**1. Install the plugin on every node**, into the exact OpenSearch version
the zip was built for:

```bash
bin/opensearch-plugin install -b file:///path/infino-engine-3.9.0.zip
```

**2. Point the JVM at the bridge**, in `config/jvm.options` or
`OPENSEARCH_JAVA_OPTS`:

```
-Dinfino.lib=/path/libinfino_capi.so
```

**3. Choose where the tables live.** Four choices; the same OpenSearch
APIs on top of each, and nothing in Lucene with any of them:

- *In memory*: the default when nothing is set. Fast and gone on restart;
  for trying things out.
- *On disk*: `-Dinfino.uri=file:///abs/path` beside the bridge option. One
  node's own disk; the node that owns the path serves the tables.
- *In a bucket*: `-Dinfino.uri=s3://bucket/prefix` (`gs://`, `az://` too).
  Your storage, your credentials, the way any client reaches the bucket;
  any node pointed at it serves the same tables.
- *On an Infino account*: the hosted service runs the engine and manages
  the tables, with the enterprise features on top (compaction, the usage
  and billing console, the agent and search APIs over the same data). The
  account stores either in an Infino bucket or in your own bucket, and
  customer-managed encryption keys (CMEK) are supported. In
  `config/opensearch.yml`

  ```yaml
  infino.platform.url: https://host
  infino.platform.database: logs        # optional: the cluster's name when unset
  ```
  and the key in the node's keystore, `bin/opensearch-keystore add
  infino.platform.api_key` (or `INFINO_API_KEY` in the node's environment).
  With no account yet, start the node without these and ask for one:
  `POST /_plugins/_infino/account/trial {"accept": true}` answers with the
  key, exactly once, and these lines; the node uses the account from then
  on, and the lines make it the node's own after a restart. See "Hosted
  account, and stateless nodes" below. When both a URI and an account are
  set, the account wins.

**4. Start the node and use OpenSearch as usual.** Every index the cluster
creates is Infino-backed (`index.infino.enabled: false` on an index keeps
that one off; `infino.index.default_enabled: false` on the node turns the
default off). Index, bulk, get, search, aggregations, scroll and Dashboards
work through the same APIs; nothing reaches Lucene. Give an index one shard
and no replicas, since the shard holds nothing and the engine scales reads
itself. A fresh node on the same storage brings the indices back by itself.

For a development node, `scripts/run.sh` does steps 1 to 4 on a
distribution:

```bash
DISTRO=/path/to/opensearch-3.9.0 INFINO_URI=s3://bucket/prefix ./scripts/run.sh
```

Two things would make this a one-step install and are not done: a published
zip per OpenSearch version, and the bridge inside the zip (loaded from the
plugin's own directory, per platform) so `-Dinfino.lib` goes away.

```bash
curl -XPUT localhost:9200/logs -H 'Content-Type: application/json' -d '{
  "settings": {"index.infino.enabled": true, "number_of_shards": 1, "number_of_replicas": 0},
  "mappings": {"properties": {
    "@timestamp": {"type": "date"}, "level": {"type": "keyword"},
    "message": {"type": "text"}, "status": {"type": "long"}}}}'
curl -XPOST localhost:9200/_bulk -H 'Content-Type: application/x-ndjson' --data-binary $'{"index":{"_index":"logs","_id":"1"}}\n{"@timestamp":"2026-10-24T10:00:00Z","level":"error","message":"disk full on node-3","status":500}\n'
curl localhost:9200/logs/_search -H 'Content-Type: application/json' -d '{
  "query": {"bool": {"must": [{"match": {"message": "disk"}}],
                     "filter": [{"range": {"@timestamp": {"gte": "now-1d"}}}]}},
  "sort": [{"@timestamp": "desc"}],
  "aggs": {"over_time": {"date_histogram": {"field": "@timestamp", "fixed_interval": "1h"},
                         "aggs": {"by_level": {"terms": {"field": "level"}}}}}}'
```

Settings: `index.infino.table` names the table explicitly (set it to reattach
a recreated index to its rows; such a table is kept when the index is
deleted); `index.infino.optimize_interval` (default 60s) compacts the table on
a schedule. JVM properties `infino.batch.docs` (50000) and
`infino.batch.bytes` (64 MiB) bound the writes a batch may hold while an
append runs.

## Hosted account, and stateless nodes

Instead of its own storage, a node's tables can live on the hosted service:
every table operation then goes there through the engine's own hosted
client, and the node holds no table data. The account is node configuration,
read at boot:

```yaml
# opensearch.yml
infino.platform.url: https://host          # the service
infino.platform.database: logs             # the cluster's name, mapped, when unset
```
```bash
opensearch-keystore add infino.platform.api_key   # or INFINO_API_KEY in the node's environment
```

Nothing about the account lives in the cluster, and nothing a node needs
survives in it: the indices come back from a registry kept beside the tables
(one row per index with its settings, mapping and aliases, written by the
elected cluster manager as each index moves, deleted with the index). A node
with an empty data directory boots, reads the registry and creates every
index it names; the tables are named by the index, so the rows are there
before the first request. Cluster state is a cache of this, rebuilt at boot,
never the source. The security plugin's configuration index and Dashboards'
saved objects come back the same way. The same holds for a `file://` or
`s3://` catalog of the node's own.

The security plugin's index is Infino-backed like every other, so roles,
users and password hashes live in a table under the same credentials as the
data (the account's key, or the bucket's), and a fresh node reads them from
there rather than from a `securityadmin` run. The table is the only copy,
written on every change, so it cannot be staler than the cluster was. An
operator who wants that index in Lucene instead puts an index template on
`.opendistro_security` with `index.infino.enabled: false`; templates sit
above the plugin's default, and such a node then needs `securityadmin` (or
the demo initialization) after a restart with an empty data directory. In the cloud: every document, the account,
its credit and its limits. Nothing is kept in any shard either way.

```bash
curl localhost:9200/_plugins/_infino/account           # what the tables are served from; the notice when no account is in use
curl localhost:9200/_plugins/_infino/account/indices   # what the registry would bring back on a fresh cluster (?name=<index> for one)
curl -XPOST localhost:9200/_plugins/_infino/account/trial -H 'Content-Type: application/json' -d '{"accept": true}'
curl -XPUT  localhost:9200/_plugins/_infino/account -H 'Content-Type: application/json' -d '{"platform": "https://host", "api_key": "..."}'
curl -XDELETE localhost:9200/_plugins/_infino/account  # stop using the account on this node; the tables stay on the service
```

An account is created only when a person accepts: the trial request must
carry `"accept": true`. The trial creates a free account with credit, no
email, no password and no card, registers one database named after the
cluster (`database` in the body names another), answers with the key exactly
once and the configuration lines above, and this node uses the account from
then on, for its life; after a restart the node reads it from its
configuration. `PUT` does the same for an account you already have, after
using the key once so a refused key fails there. The service answers 501 when
it offers no trial and 409 when the cluster's address has had one; the route
passes each on with the next step. `infino.platform.default_url` (node
setting) is the service a trial is asked from when the body names none.

The service enforces the account's limits: at a ceiling or out of credit it
refuses a write with a message naming the console where billing is added,
and the bulk or request fails with that status and message. The node's own
optimize schedule is off for hosted tables; the service compacts them.

## Writes

Each engine append is a commit, and a bulk is acknowledged after the append
that carries its writes, so an acknowledged item is durable in the table.
Writes group commit: a bulk that arrives while no append is running starts
one at once; bulks that arrive during an append go to the table as the next
append the moment it finishes. A client sending one bulk at a time waits only
for its own append; clients sending in parallel share appends. An index of an
existing id replaces it, a delete removes it and an update merges into it
(partial `doc`, `doc_as_upsert`, `upsert`; scripts are refused); which ids
exist is answered by the id column's term index in one statement, so a
stream of new documents pays no lookup. A document the declared columns
cannot hold is refused as its own item with the reason. A bulk must address
one Infino-backed index.

## What the table answers

Query DSL: `match_all`, `term`, `terms`, `range` (numbers and dates, with date
math and time zones), `exists`, `prefix`, `wildcard`, `regexp`, `ids`, `match`,
`match_phrase` (as all-terms), `multi_match`, `query_string` /
`simple_query_string` (as the terms they hold; Lucene syntax inside the string
is not interpreted), `bool` (must / filter / must_not / should with
minimum_should_match as "at least one"), `infino_knn`, `infino_hybrid`. A field
named `x.keyword` resolves to the string column `x`. Sorting by fields;
`from` / `size` paging; `_source` includes and excludes; exact `hits.total`.
A search over several Infino-backed indices merges their pages by sort values
or score and reduces their aggregations once.

Aggregations: `value_count`, `sum`, `avg`, `min`, `max`, `cardinality`
(exact, `COUNT(DISTINCT)`), `percentiles` (t-digest, from the engine's
`approx_percentile_cont`); `terms` over string, boolean, integer, float or date
columns with size, min_doc_count and order by count, key or a sibling metric;
`date_histogram` with fixed or calendar intervals, time zone, offset (fixed),
min_doc_count and numeric extended_bounds; `histogram` with interval and
offset; `filters` (keyed and anonymous, with `other_bucket`), `range`,
`date_range`; bucket aggregations nested to any depth, each level a `GROUP BY`
over the parent's kept buckets; pipeline aggregations, computed in the final
reduction from the buckets returned.

A request outside this surface fails with `400` and a reason naming the
clause (`[infino] query not served by the table: ...`), never a silently
empty answer. Not served: `composite`, `top_hits`, `percentile_ranks`, HDR
percentiles, `significant_terms`; `fuzzy`, `nested`, `function_score`,
`script` and geo queries; `scroll`, `search_after`, `post_filter`,
`collapse`, `suggest`; `mget`; a search or bulk mixing Infino-backed indices
with others.

## License

Apache-2.0. The bridge and plugin bind only the published `infino` crate.
