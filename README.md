# opensearch-infino

A spike: back **OpenSearch** with the **Infino** engine instead of Lucene for
storage, full-text (BM25), vector (kNN / hybrid), and analytics (SQL
aggregations) — delivered as an **OpenSearch plugin**, not a fork. The
OpenSearch REST API and its distributed (cluster/shard scatter-gather) framework
stay intact; only the per-shard storage + query engine is swapped to Infino.

Status: **working proof of concept**, benchmarked at 1M docs on a laptop. See
[HANDOVER.md](HANDOVER.md) for the full picture — architecture, what works, the
Lucene-coupling walls, benchmark results, known limitations, and next steps.

## What's here

| path | what |
|---|---|
| `infino-capi/` | Rust **C-ABI bridge** (`cdylib`) over the public `infino` crate. JSON/array in, JSON out. |
| `plugin/` | The **OpenSearch Java plugin** (`org.opensearch.infino`): engine, query phase, agg processor, fetch sub-phase, custom kNN/hybrid queries, REST passthrough. Talks to the cdylib via JNA. |
| `bench/` | `bench.py` — indexing + query-latency comparison vs stock Lucene. |
| `scripts/` | `build.sh` (cdylib + plugin), `run.sh` (install + start node). |

## Expected layout

The code assumes three sibling directories under one workspace folder:

```
<ws>/infino/            public Apache-2.0 infino crate   (path dep of infino-capi)
<ws>/OpenSearch/        an OpenSearch 3.10.x source checkout (build target + host for the plugin)
<ws>/opensearch-infino/ this repo
```

`infino-capi/Cargo.toml` path-depends on `../../infino`. The plugin is built
inside the OpenSearch checkout (dropped into `plugins/examples/infino-engine`).

## Quickstart

```bash
# 1. build the cdylib + plugin (needs Rust, JDK 21, an OpenSearch checkout)
./scripts/build.sh

# 2. install into a built OpenSearch distribution + start the node
#    (pass the dylib path + catalog dir via env; see the script header)
./scripts/run.sh

# 3. create an Infino-backed index and use the normal OpenSearch API
curl -XPUT localhost:9200/demo -H 'Content-Type: application/json' -d '{
  "settings": {"index.infino.enabled": true, "number_of_shards": 1, "number_of_replicas": 0},
  "mappings": {"_source": {"enabled": false},
    "properties": {"title": {"type": "text", "fielddata": true},
                   "category": {"type": "keyword"}, "price": {"type": "float"}}}}'
curl -XPUT localhost:9200/demo/_doc/1 -H 'Content-Type: application/json' \
  -d '{"title":"red widget","category":"tools","price":10.5}'
curl -XPOST localhost:9200/demo/_refresh
curl localhost:9200/demo/_search -H 'Content-Type: application/json' \
  -d '{"query":{"match":{"title":"widget"}},"aggs":{"by_cat":{"terms":{"field":"category"}}}}'
```

An index opts in with `index.infino.enabled: true`. Everything else is the
standard OpenSearch API.

## License

The bridge and plugin are intended to be Apache-2.0 (they bind only the public
`infino` crate). This repo is **private for now** — see HANDOVER.md §Strategy.
