# big5 100M race, 2026-11-17

Branch `repro/big5-100m-2026-11-17` is named the same in `infino-ai/opensearch-infino` (this branch) and `infino-ai/infino` (the engine). It holds exactly what the race ran, so the numbers below can be rebuilt and rerun.

## Results

These numbers are from `ekechi-dev-cpu` (GCE us-east1-b, 16 vCPU, 32 GB, 1.9 TB balanced persistent disk). Stock OpenSearch 3.9.0 and the proxy node ran side by side on that host. The proxy's catalog was in `gs://infino-bench-big5/big5-100m-h`, and both nodes held 100,000,000 big5 documents.

`bench/race-big.sh` made 30 alternating calls per search with `request_cache=false`, ran two rounds after a warmup, and reported the median in ms. Round 2 ran 2026-11-17 00:07–00:12 UTC:

- `sort_keyword_can_match_shortcut` and `sort_keyword_no_can_match_shortcut` were 8.57× and 8.40× stock.
- Every other search was 2.07× stock or less. 14 of the 36 that answered were faster than stock. In round 1 the same two searches were 8.86× and 8.77×, and every other one was 2.08× or less.
- `range-auto-date-histo`, `range-auto-date-histo-with-metrics` and `multi_terms-keyword` returned errors on the proxy.

Full tables: [round 1](big5-100m-2026-11-17.round1.txt), [round 2](big5-100m-2026-11-17.round2.txt).

## What the build holds

**Plugin**
- Commit `81676b0`: main at `f09b99e` with open PR branches cherry-picked on top.
- One commit is not on any other branch: `bb347c1` "experiment: SQL statements in place of the engine's statement-free calls". It is controlled by `-Dinfino.sql_only`, which defaults to `true`. The race ran with `false`, so the plugin used its normal path.

**Bridge** (`infino-capi`)
- The Rust source is the same as `81676b0`'s.
- The build had two changes that this branch commits:
  - The engine dependency is `0.11.31`, through `[patch.crates-io]`. At build time it was a local path; here it is the engine branch's git rev.
  - The `Cargo.lock` that goes with it.
- The release profile is thin LTO with `codegen-units = 1`.

**Engine** (`infino-ai/infino`, branch `repro/big5-100m-2026-11-17`)
- `85bc14d` is main at `2134164` with these open PRs merged in: #1032, #1037, #1184, #1190, #1191, #1212, #1213, #1265, #1298, #1299, #1308, #1309, #1311, #1315, #1317, #1319–#1321, #1323, #1324, #1326–#1334, and #1336–#1343.
- On top of that sit commits cherry-picked from these branches: `perf/range-top-k-text`, `perf/head-where`, `perf/range-counts-keys-once`, `perf/sorted-search-where` and `feat/document-column`. Some of those commits have since landed on main.
- `5639ad5` adds timing prints in the scan and top-k paths. They were uncommitted in the build, and they print only when `INFINO_TIMING` is set. It was not set during the race.

**Bench scripts:** `bench/` is taken from `bench/big5-100m-bucket` at `6ffb651`, the scripts the race ran.

## Settings beyond the scripts' defaults

| Setting | Value |
|---|---|
| Heap, both nodes | 3g (`local-nodes.sh` defaults to 2g) |
| Proxy `infino.yaml` | `memory.decoded_column_cache_bytes: 4294967296` (4 GiB) |
| Proxy JVM | `-Dinfino.sql_only=false` |
| Proxy catalog | `-Dinfino.uri=gs://infino-bench-big5/big5-100m-h` |
| `opensearch.yml`, from `local-nodes.sh` | disk watermark off, security off, proxy `infino.default_mode.enabled: false` |

## Rerunning

```bash
bench/repro/big5-100m-2026-11-17.sh build
bench/repro/big5-100m-2026-11-17.sh race-fresh   # loads 100M docs into new nodes first
```

`race-fresh` loads into `CATALOG_URI`, so set it to a prefix of your own. `race-reuse` restarts only the proxy, over nodes that are already loaded (`NODES`), and races them as the original run did.

Run the build somewhere other than the race host: a build beside a loaded node can get the node killed for memory.
