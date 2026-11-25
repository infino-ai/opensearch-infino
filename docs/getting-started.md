# Getting started with an Infino account

This guide takes you from nothing to searching your own data, with the tables kept on a hosted Infino account. You run one OpenSearch node in Docker, ask it for an account, load 100,000 sample log events (or your own), search them with OpenSearch queries and with SQL, and then throw the node away and watch a new one bring every index back from the account.

It takes about half an hour, most of it the first image build.

**What the outputs are.** The commands in steps 1 and 3 to 6 were run, in this order, on a Docker Compose node of OpenSearch 3.9.0 built from this repository, with the tables on the node's own storage (step 6 as its last part describes), and the outputs shown are theirs, trimmed where marked `...`; where the node in the guide is using an account, a `serving` line shows what such a node answers instead. The responses in step 2, and the account's status after it, are shown in the shape the node returns them, with placeholders where your account's own values go. The plugin is in beta: see the [README](../README.md) for its status, and try it on data you can rebuild.

**Contents:** [Before you start](#before-you-start) · [1. Start a node](#1-start-a-node) · [2. Get an account](#2-get-an-account) · [3. Create an index](#3-create-an-index) · [4. Load data](#4-load-data) · [5. Search](#5-search) · [6. Your data lives on the account](#6-your-data-lives-on-the-account) · [7. Limits and errors](#7-limits-and-errors) · [Cleaning up](#cleaning-up)

## Before you start

You need:

- Docker with Compose v2 and BuildKit (Docker Desktop has both). The image build uses BuildKit cache mounts, so a Docker without BuildKit stops with `the --mount option requires BuildKit`.
- About 4 GB of memory for Docker, and `curl` and `python3` on your machine.
- This repository: `git clone https://github.com/infino-ai/opensearch-infino.git && cd opensearch-infino`.

Every command below runs from the repository's root.

**Without an account.** Steps 3 to 6 work the same on the node's own storage: leave `INFINO_STORAGE=local` and skip step 2 (step 6 notes the one difference). On a node you built and started yourself ([Local build](../README.md#b-local-build)) with security off, use `http://localhost:9200` and drop `-k -u ...` from each command.

## 1. Start a node

Copy the example settings and pick a password for the `admin` user. It needs an upper-case letter, a lower-case letter, a digit and a symbol other than `$`, which Compose reads as a variable in `.env`.

```bash
cp env.example .env
```

Set `OPENSEARCH_ADMIN_PASSWORD=` in `.env`, and the same password in your shell, which the commands below read:

```bash
export OPENSEARCH_ADMIN_PASSWORD='<password>'
```

Leave `INFINO_STORAGE=local` for now: the node needs to be running to ask for an account. Build the image and start the node:

```bash
docker compose up --build -d   # the first build compiles the native bridge: about 15 minutes
until curl -sk -o /dev/null -w '%{http_code}' -u "admin:$OPENSEARCH_ADMIN_PASSWORD" https://localhost:9200/_cluster/health | grep -q 200; do sleep 5; done
```

The node serves HTTPS with the security plugin's demo certificate, hence `-k`. For its first seconds it answers `OpenSearch Security not initialized`; the loop waits that out. If the loop has not ended a few minutes after the build, `docker compose logs opensearch` says why: the node exits at once on a configuration it cannot use, and a wrong password in your shell answers 401 for ever.

Ask the node what its tables are served from:

```bash
curl -sk -u "admin:$OPENSEARCH_ADMIN_PASSWORD" https://localhost:9200/_plugins/_infino/account
```

```json
{"configured":false,"serving":"file:///usr/share/opensearch/infino/data","notice":["The hosted service holds every document of this cluster.", ...]}
```

## 2. Get an account

Trial accounts are not open yet, so this step answers 501 for now. Until they are, skip this step and go on to step 3, which keeps the tables on the node's own storage (see [Without an account](#before-you-start)).

The trial creates an account with starting credit. It asks for no email, password or card. Sending `"accept": true` accepts Infino's [Terms of Service](https://www.infino.ai/terms); from then on, every document the cluster writes is sent to the hosted service.

```bash
curl -sk -u "admin:$OPENSEARCH_ADMIN_PASSWORD" -XPOST https://localhost:9200/_plugins/_infino/account/trial \
  -H 'Content-Type: application/json' -d '{"accept": true}'
```

```json
{"created": true,
 "platform": "https://api.platform.infino.ws",
 "database": "opensearch-infino",
 "credit_cents": <the account's starting credit>,
 "console_url": "<where the account's usage and billing are>",
 "accepted_by": "admin",
 "api_key": "<the account's key>",
 "config": ["opensearch.yml: infino.platform.url: https://api.platform.infino.ws", ...]}
```

**Keep `api_key`.** The service hands it out exactly once, and the node does not keep it past a restart. `database` is the cluster's name unless you passed `"database"` in the body.

The node uses the account at once, but only until it stops. To keep it, put the key in `.env` and switch the node to cloud storage:

```bash
INFINO_STORAGE=cloud
INFINO_API_KEY=<api_key from the response>
```

If the response's `database` is not the cluster's name (`CLUSTER_NAME` in `.env`, `opensearch-infino` by default), set `INFINO_DATABASE` to it too.

The node's data volume belongs to the storage it was first started with, and the node refuses to start on another, so start it over with empty volumes:

```bash
docker compose down -v   # deletes this node's local volumes; nothing on the account
docker compose up -d
until curl -sk -o /dev/null -w '%{http_code}' -u "admin:$OPENSEARCH_ADMIN_PASSWORD" https://localhost:9200/_cluster/health | grep -q 200; do sleep 5; done
curl -sk -u "admin:$OPENSEARCH_ADMIN_PASSWORD" https://localhost:9200/_plugins/_infino/account
```

```json
{"configured": true,
 "serving": "https://api.platform.infino.ws database opensearch-infino",
 "platform": "https://api.platform.infino.ws",
 "database": "opensearch-infino",
 "source": "node settings",
 "config": [...]}
```

`"source": "node settings"` says the account comes from the node's configuration, so it survives restarts.

On a node you installed yourself rather than in Docker, the same three settings go in `opensearch.yml` and the keystore; see [Hosted account and stateless nodes](../README.md#hosted-account-and-stateless-nodes).

## 3. Create an index

An index on this node is backed by a table on the account; it needs no setting of its own. The mapping declares the table's columns. This one fits the sample events, with a 16-dimension vector for each message:

```bash
curl -sk -u "admin:$OPENSEARCH_ADMIN_PASSWORD" -XPUT https://localhost:9200/events -H 'Content-Type: application/json' -d '{
  "settings": {"number_of_shards": 1, "number_of_replicas": 0, "index.knn": true},
  "mappings": {"properties": {
    "@timestamp": {"type": "date"},
    "service":    {"type": "keyword"},
    "level":      {"type": "keyword"},
    "status":     {"type": "long"},
    "path":       {"type": "keyword"},
    "latency_ms": {"type": "float"},
    "host":       {"type": "keyword"},
    "message":    {"type": "text"},
    "message_embedding": {"type": "knn_vector", "dimension": 16,
      "method": {"name": "hnsw", "engine": "lucene", "space_type": "cosinesimil"}}}}}'
```

```json
{"acknowledged":true,"shards_acknowledged":true,"index":"events"}
```

- A `text` field gets a BM25 index; a `keyword` is compared by its whole value; dates and numbers get range indices.
- A `knn_vector` field is searched by the mapping's `space_type`: `l2`, `innerproduct` or `cosinesimil`. The engine's smallest vector has 16 dimensions.
- Map the fields you will search by. A document that the declared columns cannot hold is refused as its own bulk item, with the reason.

## 4. Load data

Generate the sample: 100,000 log events from the last seven days, about 28 MB of NDJSON, one JSON document per line.

```bash
python3 docs/sample-events.py > events.ndjson
head -1 events.ndjson
```

```json
{"@timestamp": "2026-11-23T19:02:26Z", "service": "auth", "level": "error", "status": 500, "path": "/logout", "latency_ms": 9.4, "host": "node-6", "message": "deadlock detected, retrying", "message_embedding": [0.0, 0.0, 0.0, 0.0, 0.0, 0.8944, 0.0, 0.0, 0.0, 0.0, 0.4472, 0.0, 0.0, 0.0, 0.0, 0.0]}
```

**Your own data.** Any file with one JSON document per line loads the same way: create an index whose mapping names its fields (step 3), and use its name in place of `events` below.

The `_bulk` API takes an action line before each document. Add them, and split the result into requests of 5,000 documents (10,000 lines) each:

```bash
awk '{print "{\"index\":{}}"; print}' events.ndjson | split -a 4 -l 10000 - bulk-
for f in bulk-????; do
  printf '%s ' "$f"
  curl -sk -u "admin:$OPENSEARCH_ADMIN_PASSWORD" -XPOST https://localhost:9200/events/_bulk \
    -H 'Content-Type: application/x-ndjson' --data-binary @"$f" -o "$f.json" -w '%{http_code} '
  grep -o '"errors":[a-z]*' "$f.json" || echo
done
```

Each request should print its file, `200` and `"errors":false`:

```
bulk-aaaa 200 "errors":false
bulk-aaab 200 "errors":false
...
```

`"errors":true` means the request wrote its other documents and refused at least one; that item in `bulk-....json` gives the reason. Don't send the whole file again: its documents have no `_id`, so the ones already written would be written twice. A status other than `200` means the whole request was refused, and `bulk-....json` says why.

**Batch size.** A request of a few thousand documents, a few megabytes, is a good size; OpenSearch refuses a request body over `http.max_content_length` (100 MB by default). Every document sent to the account counts against it, so start with a slice of a large file.

Make the writes visible to search with one refresh at the end, rather than `?refresh=true` on every request. On a hosted account a refresh waits out `infino.platform.read_window` (one second by default) after the last write it covers, so the search after it sees every write.

```bash
curl -sk -u "admin:$OPENSEARCH_ADMIN_PASSWORD" -XPOST https://localhost:9200/events/_refresh
curl -sk -u "admin:$OPENSEARCH_ADMIN_PASSWORD" https://localhost:9200/events/_count
```

```json
{"count":100000,"_shards":{"total":1,"successful":1,"skipped":0,"failed":0}}
```

On a laptop, the 20 requests took about 6 seconds with the tables on the node's own storage. On a hosted account each request also crosses the network to the service.

**Copying an existing index.** `_reindex` copies an index the cluster already holds into an Infino-backed one, through the same write path:

```bash
curl -sk -u "admin:$OPENSEARCH_ADMIN_PASSWORD" -XPOST 'https://localhost:9200/_reindex?refresh=true' -H 'Content-Type: application/json' -d '{
  "source": {"index": "events", "query": {"term": {"level": "error"}}},
  "dest":   {"index": "errors"}}'
```

```json
{"took":1483,"timed_out":false,"total":6047,"created":6047,"batches":7,"version_conflicts":0,"failures":[], ...}
```

`dest` is created from the cluster's templates if it does not exist; create it with a mapping first, as in step 3, to choose its columns. A sliced `_reindex` (`slices`) is not supported and fails with a 400. Reindexing from another cluster (`source.remote`) needs that cluster in the node's `reindex.remote.allowlist`, which the Compose file does not set; it is not covered here.

## 5. Search

### Full text, filters and sorting

Error events in the last day that mention a disk, newest first:

```bash
curl -sk -u "admin:$OPENSEARCH_ADMIN_PASSWORD" https://localhost:9200/events/_search -H 'Content-Type: application/json' -d '{
  "size": 3,
  "_source": ["@timestamp", "service", "host", "message"],
  "query": {"bool": {
    "must":   [{"match": {"message": "disk"}}],
    "filter": [{"term": {"level": "error"}}, {"range": {"@timestamp": {"gte": "now-1d"}}}]}},
  "sort": [{"@timestamp": "desc"}]}'
```

```json
{"hits": {"total": {"value": 112, "relation": "eq"}, "hits": [
  {"_source": {"@timestamp": "2026-11-24T09:05:58Z", "service": "inventory", "host": "node-6", "message": "disk full on volume /var/data"}, ...},
  {"_source": {"@timestamp": "2026-11-24T09:00:27Z", "service": "inventory", "host": "node-9", "message": "disk full on volume /var/data"}, ...},
  {"_source": {"@timestamp": "2026-11-24T08:58:22Z", "service": "search", "host": "node-5", "message": "disk full on volume /var/data"}, ...}]}}
```

### Aggregations

Errors per service, and each service's 95th-percentile latency, per day:

```bash
curl -sk -u "admin:$OPENSEARCH_ADMIN_PASSWORD" https://localhost:9200/events/_search -H 'Content-Type: application/json' -d '{
  "size": 0,
  "query": {"range": {"status": {"gte": 500}}},
  "aggs": {"by_service": {"terms": {"field": "service"},
    "aggs": {"p95_latency": {"percentiles": {"field": "latency_ms", "percents": [95]}},
             "per_day": {"date_histogram": {"field": "@timestamp", "calendar_interval": "day"}}}}}}'
```

```json
{"hits": {"total": {"value": 6047, "relation": "eq"}, ...},
 "aggregations": {"by_service": {"buckets": [
   {"key": "inventory", "doc_count": 1233,
    "p95_latency": {"values": {"95.0": 124.47}},
    "per_day": {"buckets": [{"key_as_string": "2026-11-17T00:00:00.000Z", "doc_count": 98},
                            {"key_as_string": "2026-11-18T00:00:00.000Z", "doc_count": 168}, ...]}},
   {"key": "checkout", "doc_count": 1225, ...}, ...]}}}
```

### Vector search

A vector query needs the query's embedding, made the same way as the documents'. The sample's toy embedding comes from the same script:

```bash
VEC=$(python3 docs/sample-events.py --embed "volume is out of disk space")
echo "$VEC"
```

The k-NN plugin's `knn` query is answered from the table. It is the whole query (not a clause inside `bool`), and its `filter` is applied before the nearest are picked:

```bash
curl -sk -u "admin:$OPENSEARCH_ADMIN_PASSWORD" https://localhost:9200/events/_search -H 'Content-Type: application/json' -d '{
  "size": 3, "_source": ["service", "message"],
  "query": {"knn": {"message_embedding": {"vector": '"$VEC"', "k": 10,
    "filter": {"term": {"service": "inventory"}}}}}}'
```

```json
{"hits": {"total": {"value": 10, "relation": "eq"}, "max_score": 0.8162332, "hits": [
  {"_score": 0.8162332, "_source": {"service": "inventory", "message": "disk full on volume /var/data"}, ...},
  {"_score": 0.8162332, "_source": {"service": "inventory", "message": "disk full on volume /var/data"}, ...},
  {"_score": 0.8162332, "_source": {"service": "inventory", "message": "disk full on volume /var/data"}, ...}]}}
```

### Hybrid search

`infino_hybrid` ranks by BM25 on a text field and by nearness on a vector field together, fusing the two lists by rank:

```bash
curl -sk -u "admin:$OPENSEARCH_ADMIN_PASSWORD" https://localhost:9200/events/_search -H 'Content-Type: application/json' -d '{
  "size": 3, "_source": ["service", "message"],
  "query": {"infino_hybrid": {"text_column": "message", "query": "timed out",
    "vector_column": "message_embedding", "vector": '"$VEC"', "k": 10}}}'
```

```json
{"hits": {"total": {"value": 3, "relation": "eq"}, "max_score": 0.016393442, "hits": [
  {"_score": 0.016393442, "_source": {"service": "search", "message": "upstream timed out after 30s"}, ...},
  {"_score": 0.016393442, "_source": {"service": "auth", "message": "disk full on volume /var/data"}, ...},
  {"_score": 0.016129032, "_source": {"service": "auth", "message": "upstream timed out after 30s"}, ...}]}}
```

The plugin's own `infino_knn` query (`{"field": ..., "vector": [...], "k": 10}`) runs the same nearest-neighbour search.

### SQL

`POST /_plugins/_infino/sql` runs one read-only SQL statement over the same tables. A statement names indices as tables, and the answer is `{"schema": [...], "rows": [...]}`. These read `errors`, the copy made in step 4:

```bash
curl -sk -u "admin:$OPENSEARCH_ADMIN_PASSWORD" -XPOST https://localhost:9200/_plugins/_infino/sql -H 'Content-Type: application/json' -d '{
  "query": "SELECT service, count(*) AS errors, avg(latency_ms) AS avg_ms FROM errors GROUP BY service ORDER BY errors DESC"}'
```

```json
{"schema": [{"name": "service", "type": "LargeUtf8"}, {"name": "errors", "type": "Int64"}, {"name": "avg_ms", "type": "Float64"}],
 "rows": [{"service": "inventory", "errors": 1233, "avg_ms": 45.54},
          {"service": "checkout", "errors": 1225, "avg_ms": 48.33},
          {"service": "payments", "errors": 1223, "avg_ms": 46.28},
          {"service": "auth", "errors": 1189, "avg_ms": 46.60},
          {"service": "search", "errors": 1177, "avg_ms": 46.93}]}
```

`bm25_search(index, column, query, k)` is a table of the k best BM25 matches of `query` on a text column, each row with its `score`, higher first:

```bash
curl -sk -u "admin:$OPENSEARCH_ADMIN_PASSWORD" -XPOST https://localhost:9200/_plugins/_infino/sql -H 'Content-Type: application/json' -d '{
  "query": "SELECT service, message, score FROM bm25_search('"'errors'"', '"'message'"', '"'card declined'"', 3)"}'
```

```json
{"schema": [{"name": "service", "type": "LargeUtf8"}, {"name": "message", "type": "LargeUtf8"}, {"name": "score", "type": "Float32"}],
 "rows": [{"service": "payments", "message": "card declined by issuer", "score": 1.8610585},
          {"service": "auth", "message": "card declined by issuer", "score": 1.8610585},
          {"service": "search", "message": "card declined by issuer", "score": 1.8610585}]}
```

## 6. Your data lives on the account

The node holds none of the tables. Beside them, the account keeps a registry with one row per index: its settings, mapping and aliases. Ask what a fresh node would bring back:

```bash
curl -sk -u "admin:$OPENSEARCH_ADMIN_PASSWORD" https://localhost:9200/_plugins/_infino/account/indices
```

```json
{"serving": "https://api.platform.infino.ws database opensearch-infino",
 "indices": [{"name": "events", "table": "os_events", "aliases": []},
             {"name": "errors", "table": "os_errors", "aliases": []},
             {"name": ".opendistro_security", "table": "os__opendistro_security", "aliases": []}, ...],
 "state": []}
```

`state` lists the cluster state kept beside the indices (templates, pipelines, scripts, settings, users and roles); this node has none of its own yet.

Now throw the node away, data volume and all, and start a new one with the same `.env`:

```bash
docker compose down -v
docker compose up -d
until curl -sk -o /dev/null -w '%{http_code}' -u "admin:$OPENSEARCH_ADMIN_PASSWORD" https://localhost:9200/_cluster/health | grep -q 200; do sleep 5; done
curl -sk -u "admin:$OPENSEARCH_ADMIN_PASSWORD" 'https://localhost:9200/_cat/indices/events,errors?v&h=index,health,docs.count'
curl -sk -u "admin:$OPENSEARCH_ADMIN_PASSWORD" https://localhost:9200/events/_count
```

```
index  health docs.count
errors yellow       6047
events green      100000
{"count":100000,"_shards":{"total":1,"successful":1,"skipped":0,"failed":0}}
```

The node answered within seconds of starting. `docs.count` can read low for a few seconds while the node rebuilds its own record of each index's ids from the table; `_count` and searches read the table and are complete at once. `errors` is yellow because `_reindex` created it from the default template, with one replica that a single node cannot place.

The new node read the registry at boot and created each index, with its mapping and aliases, over the table already on the account. Every search above runs again unchanged. The security plugin's users and roles live in a table on the account too, so they come back the same way: the same `admin` password works on the new node.

**On the node's own storage.** The tables live on the `infino-data` volume, which `down -v` deletes too. To see the same thing without an account, remove only the node's data volume:

```bash
docker compose rm -sf opensearch
docker volume rm opensearch-infino_opensearch-data
docker compose up -d
```

## 7. Limits and errors

**Trial limits.** The account comes with starting credit (`credit_cents` in the trial's response). The service enforces the account's limits: at a ceiling or out of credit, it refuses a write with a message naming the console where billing is added, and the bulk request fails with that status and message. Searches of what is already stored keep working.

**Errors you may see:**

| Status | When | What to do |
|---|---|---|
| 400 | A trial request without `"accept": true` | The response carries the notice; send it again with `{"accept": true}` |
| 400 | A trial on a node that already uses an account | `DELETE /_plugins/_infino/account` first, or keep the account you have |
| 501 | The service gives out no trial accounts | With a key from the service's operator: `PUT /_plugins/_infino/account {"api_key": "..."}` |
| 409 | This cluster's network address has already had a trial account | Use that account's key: `PUT /_plugins/_infino/account {"api_key": "..."}`, or `INFINO_API_KEY` in `.env` |
| 429 | `cluster_block_exception` naming the flood-stage disk watermark: the disk Docker keeps volumes on is over 95% full | Free space for Docker (`docker system df` shows what can go), then retry |
| 400 | A query clause the table does not serve (`[infino] query not served by the table: ...`), or a sliced search | See [What is supported](../README.md#what-is-supported) |

A node started with `INFINO_STORAGE=cloud` and no `INFINO_API_KEY` exits at once and prints how to get one. A node whose data volume was used with other storage exits too; `docker compose down -v` starts it clean.

## Cleaning up

```bash
curl -sk -u "admin:$OPENSEARCH_ADMIN_PASSWORD" -XDELETE https://localhost:9200/events,errors   # deletes the indices and their tables
docker compose down -v                                                                        # deletes the node and its local volumes
rm -f events.ndjson bulk-*
```

`DELETE /_plugins/_infino/account` stops a running node from using the account; the tables stay on the service. To go back to local storage, set `INFINO_STORAGE=local` in `.env` and run `docker compose down -v` before starting again.
