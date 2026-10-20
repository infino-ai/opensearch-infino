# OpenSearch-on-Infino — Handover

A working proof of concept that backs OpenSearch shards with the Infino engine
instead of Lucene, as a **plugin** (no fork of OpenSearch core). This document is
written to let someone pick the work up cold.

Built/tested against **OpenSearch 3.10.0-SNAPSHOT** (Lucene 10.5.2) on macOS
(arm64), JDK 21. The engine is the public Apache-2.0 `infino` crate, reached from
the JVM through a small Rust C-ABI library via JNA.

---

## 1. Goal

Swap OpenSearch's per-shard storage + query engine from Lucene to Infino, while
keeping (a) the OpenSearch REST API and (b) its distributed cluster/shard
scatter-gather framework. "Let's see what breaks." Explicit non-goals: not a
fork; runs locally for now.

Outcome: native OpenSearch `_search` serves **storage, BM25, kNN, hybrid, fetch,
metric + terms aggregations, range/term/bool filters, and query-scoped
aggregations** entirely from Infino — with dynamic typed columns, idempotent
batched writes, persistent per-shard storage, and user data no longer stored in
Lucene. Results are bit-for-bit identical to stock Lucene at 1M docs.

---

## 2. Architecture

```
          OpenSearch REST API  (unchanged)
                     │
        coordinator / scatter-gather  (unchanged — distributed framework intact)
                     │  per-shard
   ┌─────────────────┴───────────────────────────────────────┐
   │ InfinoPlugin  (SPI hooks)                                 │
   │   EnginePlugin          → InfinoEngine  (write path)      │
   │   SearchPlugin                                            │
   │     getQueryPhaseSearcher → InfinoQueryPhaseSearcher      │  hits
   │     (QueryPhaseSearcher.aggregationProcessor)             │    → InfinoAggregationProcessor  (aggs)
   │     getFetchSubPhases    → InfinoFetchSubPhase  (_source) │
   │     getQueries           → infino_knn / infino_hybrid     │
   │   onIndexModule          → drop table on shard delete     │
   │   ActionPlugin           → /_infino/* REST passthrough    │
   └─────────────────┬───────────────────────────────────────┘
                     │ JNA
             libinfino_capi.dylib   (Rust C-ABI, infino-capi/)
                     │
                public `infino` crate   (file:// catalog, one table per shard)
```

**Plugin, not fork:** we build against stock OpenSearch and install into a
distribution. The only extension points used are public SPIs (`EnginePlugin`,
`SearchPlugin`, `Plugin.onIndexModule`). No OpenSearch core files are modified.

**One Infino table per shard**, named `os_<index>_<shard>`, in a persistent
`file://` catalog (default `<repo>/data`, override `-Dinfino.uri`). The engine is
attached only to indices created with `index.infino.enabled: true`.

---

## 3. Code map

### Rust bridge — `infino-capi/src/lib.rs`
Thin C-ABI over `infino`. JSON/array in, JSON out. Key exports:
- `infino_open/close/free_string` — connection lifecycle.
- `infino_create_from_source(name, source)` — infer the table schema from the
  first document (dynamic mapping) and create it (idempotent).
- `infino_columns(name)` — `[{"name","type"}]`, type ∈ {i64,f64,bool,str,vec}.
- `infino_index_source(name, id, osid, source)` — index one doc (schema-aware).
- `infino_index_batch(name, ids[], osids[], sources[], count)` — index many in
  one append (the scale path; one append per doc fragments the table).
- `infino_delete_osid(name, osid)` — tombstone delete (for upsert).
- `infino_sql(sql)` — DataFusion SQL (aggregations, filters).
- `infino_bm25 / infino_knn / infino_hybrid` — ranked search; project `osid`+`score`.
- `infino_token_match(name, col, q)` / `infino_token_count(...)` — the full
  (unranked) match set / its size, for query-scoped aggs over a text query.
- `infino_get(name, osid)` — raw `_source` JSON for one doc (fetch path).

Fixed/reserved columns per table: `id` (Int64, hash of _id), `osid` (LargeUtf8,
original `_id`), `text` (LargeUtf8, FTS concat of string fields), `source`
(LargeUtf8, raw `_source` JSON), `emb` (FixedSizeList<Float32,16>, vector).
Dynamic columns: one per scalar user field (string→LargeUtf8, int→Int64,
float→Float64, bool→Boolean), inferred from the first document.

### Java plugin — `plugin/src/main/java/org/opensearch/infino/`
- **InfinoPlugin** — registers all the SPI hooks + the `index.infino.enabled` setting.
- **InfinoLib** (JNA interface) / **InfinoNative** (loads the dylib once, one
  static `Connection`, Java-friendly wrappers). Loads from `-Dinfino.lib`,
  connects to `-Dinfino.uri`.
- **InfinoEngine** `extends InternalEngine` — the write path (see §5).
- **InfinoQueryPhaseSearcher** — routes the per-shard query phase to Infino (§6).
- **InfinoAggregationProcessor** — serves aggregations from Infino SQL (§6).
- **InfinoFetchSubPhase** — serves `_source` from Infino at fetch (§6).
- **InfinoSqlWhere** — translates a query-DSL builder tree → SQL `WHERE` (§6).
- **InfinoVectorQuery** + **InfinoKnnQueryBuilder** / **InfinoHybridQueryBuilder**
  — custom `infino_knn` / `infino_hybrid` query types (§6).
- **InfinoSqlRestHandler** — `/_infino/sql|search|knn` debug passthrough.

---

## 4. What works (feature matrix)

| capability | how | path |
|---|---|---|
| Storage | dual-write; Lucene user data stripped (skeleton only) | InfinoEngine |
| Dynamic field mapping | schema inferred from first doc → typed Infino columns | create_from_source |
| BM25 full-text | `match`/`term` → Infino `bm25_search` | QueryPhaseSearcher |
| kNN vector | `infino_knn` query → `vector_search` | QueryBuilder + QueryPhaseSearcher |
| Hybrid | `infino_hybrid` query → `hybrid_search` | QueryBuilder + QueryPhaseSearcher |
| Fetch `_source` | from Infino (Lucene stores none) | FetchSubPhase |
| Aggs: value_count/terms | SQL `COUNT` / `GROUP BY` | AggregationProcessor |
| Aggs: sum/avg/min/max | SQL over dynamic numeric columns | AggregationProcessor |
| Filters: range/term/terms/bool | DSL → SQL `WHERE` | SqlWhere + QueryPhaseSearcher |
| Query-scoped aggs | same `WHERE` drives hits + aggs | SqlWhere + AggregationProcessor |
| Query-scoped aggs over text | `token_match` → `osid IN (...)` | SqlWhere |
| Idempotent writes | upsert (delete-then-append) on replay/update | InfinoEngine |
| Lifecycle | table dropped on shard delete | onIndexModule |

Correctness validated at 1M: a `match` query returns the same hit count (226,814)
and the same scoped/whole-table aggregation values as stock Lucene.

---

## 5. Write path (InfinoEngine)

Each `index(Index)`:
1. **Stop dual-write of user data:** strip every user field from the Lucene
   document (`d.getFields().removeIf(f -> !f.name().startsWith("_"))`) so Lucene
   stores only the `_id`+seqno skeleton. Infino is the sole content store, fed
   from `index.source()` (full JSON, present even with `_source` disabled).
2. `super.index(index)` — Lucene seqno/translog/version/ack (skeleton only).
3. Create the Infino table lazily on the first doc (`createFromSource`).
4. **Upsert gate:** `boolean upsert = index.origin().isRecovery() || !r.isCreated()`.
   - new primary create → **batch** (accumulate; flush at `MAX_BATCH`=10000
     (`-Dinfino.batch.max`) or on `refresh()`/`flush()`).
   - update / recovery replay → flush pending batch, then `delete_osid` +
     single `index_source` (idempotent).

`refresh()` and `flush()` are overridden to flush the pending Infino batch first,
so Infino rows become visible exactly when Lucene's do, and are durable before
the translog is trimmed.

**Why batching:** one Infino append per document creates a superfile per doc —
pathological. Batching gives Infino proper multi-row appends (1M docs → ~100
superfiles at 10k batch).

**Why upsert:** Infino's append path has no key semantics, so re-applying an op
(translog replay after a crash, or a document update) would duplicate rows. The
delete-then-append upsert makes both idempotent. (`delete` is a cheap tombstone.)

---

## 6. Read paths

### Query phase — InfinoQueryPhaseSearcher
`searchWith` is the per-shard hook. A single `canServe(context)` decision gates
both hits and aggs so they never disagree (which would leave Lucene collectors
unrun). Three hit paths:
- `InfinoVectorQuery` → Infino kNN / hybrid (kNN distance mapped to a
  higher-is-better `_score`).
- a pure **filter** (DSL → non-null `WHERE` via `InfinoSqlWhere`) → `SELECT osid
  … WHERE p LIMIT k`.
- `term` / `match` → Infino BM25 (ranked).

Infino returns the original `_id` (`osid`); each is mapped back to a Lucene docid
via a `_id` term lookup, so the fetch phase (served from Infino) loads `_source`.
Exact total-hits come from a `COUNT` (filters) or `token_count` (BM25), not a
capped LIMIT. Anything not translatable (sorts, unknown shapes) is delegated to
the stock query phase.

### Aggregations — InfinoAggregationProcessor
`value_count`→`COUNT(*)`, `terms` on a string column→`GROUP BY`, `sum`/`avg`/
`min`/`max` on numeric columns. Each agg's field must be a real Infino column of
the right type (checked via `infino_columns`). **Query-scoped:** every agg SQL
gets the same `WHERE p` the hits use (match_all → `1=1`). `preProcess` skips the
Lucene collectors when Infino will serve; `postProcess` builds `InternalAggregations`
from the SQL result; otherwise it delegates to the stock processor.

### Full-text-scoped aggs — InfinoSqlWhere + token_match
A `match` clause is resolved to its full match set via `token_match` and spliced
in as `osid IN ('id', …)`, so a text query can scope aggregations (and composes
with range/term filters). Hits still use BM25 ranking; the IN-set only scopes aggs.

### Fetch — InfinoFetchSubPhase
For an Infino shard, reads each hit's `_source` from Infino by `_id`
(`infino_get`) and sets `hit.sourceRef(...)`, so `_source.enabled:false` still
returns full source. Non-Infino shards use stock Lucene source loading.

---

## 7. The Lucene-coupling walls (why not 100% Lucene-free)

These are the hard parts; they bound what a plugin (vs a fork) can do:
- **Engine contract is Lucene.** `Engine.getReferenceManager()` must return a
  Lucene `DirectoryReader`; seqno/translog/version/recovery all assume Lucene
  segments. So a Lucene doc per op is unavoidable — we reduce it to the `_id`+
  seqno skeleton but cannot remove it. Truly Lucene-free = reimplement `Engine`
  (fork-level).
- **Query/agg validation happens before the query phase.** OpenSearch builds the
  query/aggregator against the mapper first: `index:false` → "cannot search on
  field"; a `terms`/metric agg on a `text` field → "no fielddata". Workarounds:
  keep fields searchable in the mapping (so the match query builds, then we
  intercept it) and set `text` `fielddata:true` (passes the agg front door; the
  fielddata is never actually loaded because we skip the Lucene collectors).
- **Fetch loads `_source` by docid.** Solved with a FetchSubPhase that re-injects
  source from Infino, but it's a re-injection, not a clean "source provider" SPI.

The upstream-friendly fix for all three is **hardening the `Engine`/fetch SPI to
be storage-agnostic** — valuable independent of Infino.

---

## 8. Build / run / test

See `scripts/build.sh` and `scripts/run.sh` (headers document the env vars).
Prerequisites: Rust toolchain, JDK 21 (`JAVA21_HOME`), and an OpenSearch 3.10.x
source checkout as a sibling (used both to build the plugin and as the run
target — a `localDistro` build works; prebuilt tarballs 403'd during the spike).

```bash
./scripts/build.sh          # cdylib + plugin zip
./scripts/run.sh            # install plugin, start node with -Dinfino.lib/-Dinfino.uri
python3 bench/bench.py 1000000   # compare vs stock Lucene (creates bench_lucene + bench_infino)
```

**Config — library + catalog location:** `InfinoNative.java` reads `-Dinfino.lib`
(path to `libinfino_capi.dylib`) and `-Dinfino.uri` (catalog backend). Defaults
are a bare lib name and `memory://` (non-persistent) — `run.sh` sets both to
real values (the built dylib + `file://<repo>/data`). On any machine, set those
two properties; persistence needs a `file://` (or `s3://` / `gs://` / `az://`) uri.

`plugin/build.gradle` disables the strict plugin-dev gates (license/thirdPartyAudit/
forbiddenApis/etc.) and `test.enabled=false` to iterate fast — re-enable before
any productionization.

---

## 9. Benchmark (1M docs, single node, laptop)

Identical data in both indices; Infino index has `_source` disabled. Numbers are
indicative (laptop, single shard), not a tuned result.

| | stock Lucene | Infino-backed |
|---|--:|--:|
| ingest | ~81k docs/s | ~59k docs/s (dual-write; was ~48k before stripping Lucene user data) |
| Lucene store | 50 MB | 37 MB (skeleton only) |
| Infino store | — | 122 MB (100 un-compacted superfiles; incl. always-on 16-dim emb zeros) |

Query latency (server `took`, ms):

| query | Lucene | Infino |
|---|--:|--:|
| terms(category) | ~1–3 | ~1–3 (**tie**) |
| sum+avg metrics | ~1–3 | ~1–3 (**tie**) |
| match + exact total | ~2–5 | ~120–215 |
| range + exact total | ~6 | ~27–37 |

**Reading it:** Infino is competitive-to-winning on **aggregations** (columnar
DataFusion), and currently slower on **full-text / range latency**. Causes, all
addressable (not architectural):
1. **No compaction wired** → every query fans out over ~100 superfiles (biggest lever).
2. Exact total-hits scans the full match set (vs Lucene block-max WAND early-termination).
3. Per-hit `_id`→docid lookup.
4. JNA + JSON marshalling per call.

---

## 10. Known limitations

- Schema is fixed at the first document (fields that appear only in later docs
  get no column). A production path would read the OpenSearch mapping and/or
  support schema evolution.
- `terms` only on string columns (numeric `terms` → `LongTerms` not wired).
- Aggregations over a very large text match set fall back to Lucene (the
  `osid IN (...)` list scales with the match set — a production path would push
  the text predicate into Infino rather than materialize ids).
- `bool` `should` with `minimum_should_match > 1` is treated as 1.
- A query that falls back to Lucene now returns empty for user data (Lucene no
  longer stores it) — the Infino path is effectively mandatory for enabled indices.
- `emb` column always present (16-dim zeros even without vectors) — wasteful;
  should be conditional.
- Single-node / single-shard tested. Replicas and peer recovery are handled in
  principle (upsert on recovery origin) but not exercised.

---

## 11. Roadmap (highest-impact first)

1. **Wire Infino compaction/optimize** (on OpenSearch `_forcemerge`, and/or
   periodically) to collapse the superfile fan-out — the top latency lever.
2. **Respect `track_total_hits`** — stop scanning the full match set for exact
   totals when the request only wants the default 10k cap.
3. **Eliminate the per-hit docid lookup** — e.g. store the Lucene docid mapping,
   or fetch entirely from Infino and bypass docid.
4. Portability: remove hardcoded paths; make `infino.lib`/`infino.uri` config.
5. Conditional `emb` column; numeric `terms`; schema evolution; mapping-driven
   schema instead of first-doc inference.
6. Upstream-track: SPI-hardening PRs to OpenSearch (storage-agnostic Engine/fetch).

---

## 12. Strategy — why private for now

The core `infino` engine is Apache-2.0 open source, so there's **no licensing
barrier** to this being public eventually, and the k-NN-plugin precedent (an OSS
plugin over an OSS native library) fits directly. The reasons to keep it private
in the org for now:
- It's an early spike; a public repo would signal an official OpenSearch-
  integration direction before the team has decided on one.
- It isn't release-shaped (lives inside an OpenSearch checkout, hardcoded paths,
  no CI, strict gates disabled).
- Private lets iteration continue without public-hygiene constraints.

Going public later is clean: the code binds only the public `infino` crate and
references no private/internal repositories. The pragmatic path discussed:
ship eventually as an **external** public Apache-2.0 plugin (fast iteration,
release-cadence control), and in parallel contribute the **SPI-hardening** work
upstream — the real blocker to upstreaming the engine plugin is SPI maturity
(§7), not licensing.
