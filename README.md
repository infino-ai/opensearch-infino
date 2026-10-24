# opensearch-infino

Back **OpenSearch** with the **Infino** engine instead of Lucene for storage,
full-text (BM25), vector (kNN / hybrid) and analytics (SQL aggregations),
delivered as an **OpenSearch plugin**, not a fork. The OpenSearch REST API and
its cluster framework stay intact; the per-shard storage + query engine is
swapped to Infino, and an index's data lives in an Infino table on local disk
or object storage (`file://`, `s3://`, `gs://`, `az://`).

The original spike and its findings are in [HANDOVER.md](HANDOVER.md). Since
then the plugin has been shaped for log and event streams: the table is
declared from the index **mapping** (dates are timestamp columns, text fields
carry their own BM25 index, keywords are exact columns), the query phase
serves date ranges with date math, field sorts, field-scoped text search with
filters, and the aggregations dashboards ask for (`date_histogram`, `terms`,
`histogram`, `filters`, `range`, metrics including `cardinality` and
`percentiles`, nested to any depth), writes are batched by size and compacted on a
schedule, and a document the mapping cannot hold fails as its own bulk item.

## What's here

| path | what |
|---|---|
| `infino-capi/` | Rust **C-ABI bridge** (`cdylib`) over the published `infino` crate. JSON in, JSON out; per-document checks against the declared columns. |
| `plugin/` | The **OpenSearch Java plugin** (`org.opensearch.infino`): engine, query phase, aggregation processor, fetch sub-phase, GET filter, custom kNN/hybrid queries, SQL passthrough. Talks to the bridge via JNA. Builds out of tree against a released OpenSearch. |
| `bench/` | `bench.py`: indexing + query-latency comparison vs stock Lucene. |
| `scripts/` | `build.sh` (bridge + plugin zip), `run.sh` (install into a distribution + start a node). |

## Build

Needs a Rust toolchain, a JDK 21 and Gradle. No OpenSearch checkout: the
plugin builds against the release named by `OPENSEARCH_VERSION` (default
3.9.0), pulled from Maven, and the zip installs only into that exact version.

```bash
./scripts/build.sh                      # infino-capi/target/release/libinfino_capi.{so,dylib} + plugin/build/distributions/infino-engine-<ver>.zip
cd plugin && gradle check               # precommit gates, unit tests, YAML REST suite on a test cluster with the bridge
```

## Run

```bash
DISTRO=/path/to/opensearch-3.9.0 INFINO_URI=s3://bucket/prefix ./scripts/run.sh
```

The bridge and the catalog are JVM system properties: `-Dinfino.lib` (path to
the native library) and `-Dinfino.uri` (`file:///abs/path`, `s3://bucket/prefix`,
...). An index opts in with `index.infino.enabled: true`; one shard per index
is the natural setting, since the engine scales reads itself.

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

Settings: `index.infino.optimize_interval` (default 60s) compacts a primary's
table on a schedule; `_forcemerge` compacts it on demand. JVM properties
`infino.batch.docs` (50000) and `infino.batch.bytes` (64 MiB) bound a batch
between refreshes.

## What the table answers

Query DSL: `match_all`, `term`, `terms`, `range` (numbers and dates, with date
math and time zones), `exists`, `prefix`, `wildcard`, `regexp`, `ids`, `match`,
`match_phrase` (as all-terms), `multi_match`, `query_string` /
`simple_query_string` (as the terms they hold; Lucene syntax inside the string
is not interpreted), `bool` (must / filter / must_not / should with
minimum_should_match as "at least one"), `infino_knn`, `infino_hybrid`. A field
named `x.keyword` resolves to the string column `x`. Sorting by fields;
exact `hits.total`.

Aggregations: `value_count`, `sum`, `avg`, `min`, `max`, `cardinality`
(exact, `COUNT(DISTINCT)`), `percentiles` (t-digest, from the engine's
`approx_percentile_cont`); `terms` over string, boolean, integer, float or date
columns with size, min_doc_count and order by count, key or a sibling metric;
`date_histogram` with fixed or calendar intervals, time zone, offset (fixed),
min_doc_count and numeric extended_bounds; `histogram` with interval and
offset; `filters` (keyed and anonymous, with `other_bucket`), `range`,
`date_range`; bucket aggregations nested to any depth, each level a `GROUP BY`
over the parent's kept buckets.

A request outside this surface fails with `400` and a reason naming the
clause (`[infino] query not served: fuzzy on message`), never a silently empty
answer: the stock query phase holds no data for an Infino index. Not served:
`composite`, `top_hits`, `percentile_ranks`, HDR percentiles,
`significant_terms`, pipeline aggregations; `fuzzy`, `nested`,
`function_score`, `script` and geo queries; `mget`.

## License

Apache-2.0. The bridge and plugin bind only the published `infino` crate.
