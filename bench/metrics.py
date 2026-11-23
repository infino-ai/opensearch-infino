#!/usr/bin/env python3
"""Load OpenTelemetry metrics, shaped as Data Prepper's otel_metrics
processor writes them, into the nodes bench/local-nodes.sh starts, then time
the searches a metrics dashboard sends and compare each answer with stock's.

    python3 bench/metrics.py gen --hosts 200 --points 1080 --dir ./points
    python3 bench/metrics.py load --dir ./points --threads 8
    python3 bench/metrics.py run --calls 20 --out result.json
    python3 bench/metrics.py sizes --nodes ./nodes

Three indices hold the same points:

    stock      metrics     on the stock node (Lucene)
    infino     metrics     on the default-mode node
    infino-ts  metrics-ts  on the default-mode node, index.infino.timeseries

Each host reports every metric every 10 seconds, so the points arrive in
time order with every series interleaved, as a collector sends them. A
point is one document: the metric's name, kind and unit, the service and
host it came from, its attributes, its time and its value, under the field
names Data Prepper gives them (`metric.attributes.*`, `resource.attributes.*`
with `@` for a dot in a key).

`run` sends each search once as a warmup, then `--calls` times, with the
shard request cache off so each call computes its answer, and reports
the median and 95th percentile of the time the whole HTTP call took. Each
answer is compared with stock's: totals exactly, aggregation keys exactly,
and aggregated values to a relative 1e-6, or 2% for percentiles: both
estimate them with a t-digest, whose answer depends on the order it reads
the values in.

`sizes` reports what each index stores: stock's store size after a force
merge to one segment, and the bytes of each Infino table's files on disk.
"""

import argparse
import datetime
import glob
import json
import math
import os
import random
import statistics
import sys
import time
import urllib.error
import urllib.request

INTERVAL_S = 10
START = datetime.datetime(2026, 1, 1, tzinfo=datetime.timezone.utc)
SERVICES = [f"svc-{i:02d}" for i in range(20)]
# (name, kind, unit, the attribute values each host reports it with)
METRICS = [
    ("system.cpu.utilization", "GAUGE", "1", ["user", "system", "idle", "iowait"]),
    ("system.memory.utilization", "GAUGE", "1", ["used", "free"]),
    ("system.disk.io", "SUM", "By", ["read", "write"]),
    ("system.network.io", "SUM", "By", ["receive", "transmit"]),
    ("http.server.request.duration", "GAUGE", "s", ["GET", "POST"]),
    ("http.server.active_requests", "GAUGE", "{request}", [None]),
    ("process.runtime.jvm.gc.duration", "GAUGE", "s", [None]),
    ("db.client.connections.usage", "GAUGE", "{connection}", ["idle", "used"]),
]
TARGETS = {"stock": ("stock", "metrics"), "infino": ("default", "metrics"), "infino-ts": ("default", "metrics-ts")}
PORTS = {"default": 0, "stock": 1, "proxy": 2}
BULK_TRIES = 20

MAPPING = {
    "properties": {
        "time": {"type": "date"},
        "startTime": {"type": "date"},
        "name": {"type": "keyword"},
        "kind": {"type": "keyword"},
        "unit": {"type": "keyword"},
        "description": {"type": "keyword"},
        "serviceName": {"type": "keyword"},
        "value": {"type": "double"},
        "isMonotonic": {"type": "boolean"},
        "aggregationTemporality": {"type": "keyword"},
        "flags": {"type": "long"},
        "instrumentationScope": {"properties": {"name": {"type": "keyword"}, "version": {"type": "keyword"}}},
        "metric": {"properties": {"attributes": {"properties": {"state": {"type": "keyword"}}}}},
        "resource": {"properties": {"attributes": {"properties": {
            "host@name": {"type": "keyword"},
            "service@name": {"type": "keyword"},
        }}}},
    }
}


def call(port, method, path, body=None, timeout=600, ndjson=False):
    data = None
    headers = {}
    if body is not None:
        data = body if isinstance(body, bytes) else json.dumps(body).encode()
        headers["content-type"] = "application/x-ndjson" if ndjson else "application/json"
    req = urllib.request.Request(f"http://localhost:{port}{path}", data=data, method=method, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, json.loads(r.read() or b"{}")
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read() or b"{}")


def port_of(base, node):
    return base + PORTS[node]


def iso(ts):
    return ts.strftime("%Y-%m-%dT%H:%M:%S.000Z")


def series_of(hosts):
    """Every series: (service, host, metric, attribute), in a fixed order."""
    out = []
    for h in range(hosts):
        host = f"host-{h:04d}"
        service = SERVICES[h % len(SERVICES)]
        for metric in METRICS:
            for attr in metric[3]:
                out.append((service, host, metric, attr))
    return out


def points(hosts, per_series, seed):
    """The documents in arrival order: at each tick, one point per series."""
    rng = random.Random(seed)
    series = series_of(hosts)
    level = [rng.random() for _ in series]
    total = [0.0 for _ in series]
    for tick in range(per_series):
        at = START + datetime.timedelta(seconds=tick * INTERVAL_S)
        for i, (service, host, (name, kind, unit, _), attr) in enumerate(series):
            if kind == "SUM":
                total[i] += rng.randint(0, 50_000)
                value = total[i]
            else:
                level[i] = min(1.0, max(0.0, level[i] + rng.gauss(0, 0.02)))
                value = round(level[i] * (100 if unit != "1" else 1), 4)
            doc = {
                "kind": kind,
                "name": name,
                "unit": unit,
                "description": "",
                "serviceName": service,
                "time": iso(at),
                "startTime": iso(START),
                "value": float(value),
                "isMonotonic": kind == "SUM",
                "aggregationTemporality": "AGGREGATION_TEMPORALITY_CUMULATIVE" if kind == "SUM" else "AGGREGATION_TEMPORALITY_UNSPECIFIED",
                "flags": 0,
                "instrumentationScope.name": "io.opentelemetry.hostmetrics",
                "instrumentationScope.version": "1.0.0",
                "resource.attributes.host@name": host,
                "resource.attributes.service@name": service,
            }
            if attr is not None:
                doc["metric.attributes.state"] = attr
            yield doc


def create(base, node, index, timeseries):
    port = port_of(base, node)
    call(port, "DELETE", f"/{index}")
    settings = {"number_of_shards": 1, "number_of_replicas": 0, "refresh_interval": "30s"}
    if node != "stock":
        settings["index.infino.enabled"] = True
        if timeseries:
            settings["index.infino.timeseries"] = True
    status, body = call(port, "PUT", f"/{index}", {"settings": settings, "mappings": MAPPING})
    if status != 200:
        sys.exit(f"create {index} on {node}: {status} {body}")


def bulk(port, index, docs):
    if isinstance(docs, bytes):
        body = docs
    else:
        lines = []
        for doc in docs:
            lines.append(b'{"index":{}}')
            lines.append(json.dumps(doc, separators=(",", ":")).encode())
        body = b"\n".join(lines) + b"\n"
    for _ in range(BULK_TRIES):
        status, resp = call(port, "POST", f"/{index}/_bulk", body, ndjson=True)
        if status == 429:
            time.sleep(1)
            continue
        if status != 200 or resp.get("errors"):
            item = next((it for it in resp.get("items", []) if it["index"].get("error")), resp)
            sys.exit(f"bulk into {index}: {status} {json.dumps(item)[:400]}")
        return
    sys.exit(f"bulk into {index}: still 429 after {BULK_TRIES} tries")


def gen(args):
    """Write the points as _bulk bodies of `--batch` points each, in arrival
    order, so every index is loaded with the same bytes."""
    os.makedirs(args.dir, exist_ok=True)
    batch, n, total = [], 0, 0
    def flush():
        nonlocal batch, n
        with open(os.path.join(args.dir, f"{n:06d}.ndjson"), "wb") as f:
            for doc in batch:
                f.write(b'{"index":{}}\n')
                f.write(json.dumps(doc, separators=(",", ":")).encode() + b"\n")
        n += 1
        batch = []
    for doc in points(args.hosts, args.points, args.seed):
        batch.append(doc)
        total += 1
        if len(batch) == args.batch:
            flush()
    if batch:
        flush()
    meta = {"hosts": args.hosts, "points": args.points, "series": len(series_of(args.hosts)), "total": total}
    with open(os.path.join(args.dir, "meta.json"), "w") as f:
        json.dump(meta, f)
    print(f"wrote {total} points in {n} bodies", file=sys.stderr)


def load(args):
    """Create each index and load every body into it from `--threads`
    clients at once, one index after another, timing each: the seconds from
    the first body sent to the last point searchable."""
    import concurrent.futures
    bodies = sorted(glob.glob(os.path.join(args.dir, "*.ndjson")))
    meta = json.load(open(os.path.join(args.dir, "meta.json")))
    rates = {}
    for target, (node, index) in TARGETS.items():
        create(args.base_port, node, index, target == "infino-ts")
        port = port_of(args.base_port, node)
        started = time.time()
        with concurrent.futures.ThreadPoolExecutor(args.threads) as pool:
            list(pool.map(lambda path: bulk(port, index, open(path, "rb").read()), bodies))
        call(port, "POST", f"/{index}/_refresh")
        _, count = call(port, "GET", f"/{index}/_count")
        took = time.time() - started
        rates[target] = {"points": count.get("count"), "seconds": round(took, 1), "points_per_s": round(meta["total"] / took)}
        print(f"{target:10s} {count.get('count')} points in {took:.1f}s = {meta['total'] / took:,.0f} points/s", file=sys.stderr)
    with open(args.meta, "w") as f:
        json.dump(dict(meta, load=rates), f)


def searches(meta):
    end = START + datetime.timedelta(seconds=(meta["points"] - 1) * INTERVAL_S)
    last = lambda minutes: {"range": {"time": {"gte": iso(end - datetime.timedelta(minutes=minutes)), "lte": iso(end)}}}
    term = lambda field, value: {"term": {field: value}}
    host = "host-0007"
    return {
        # One series over the last hour, a point a minute: a single-line panel.
        "one-series-1h": {"size": 0, "query": {"bool": {"filter": [
            term("name", "system.cpu.utilization"), term("resource.attributes.host@name", host),
            term("metric.attributes.state", "user"), last(60)]}},
            "aggs": {"t": {"date_histogram": {"field": "time", "fixed_interval": "1m"}, "aggs": {"v": {"avg": {"field": "value"}}}}}},
        # The ten busiest hosts over the last hour: a top-N table.
        "top-hosts-1h": {"size": 0, "query": {"bool": {"filter": [
            term("name", "system.cpu.utilization"), term("metric.attributes.state", "user"), last(60)]}},
            "aggs": {"h": {"terms": {"field": "resource.attributes.host@name", "size": 10, "order": {"v": "desc"}},
                           "aggs": {"v": {"avg": {"field": "value"}}}}}},
        # One line per host for 20 hosts over 30 minutes: a multi-series panel.
        "per-host-30m": {"size": 0, "query": {"bool": {"filter": [
            term("name", "system.memory.utilization"), term("metric.attributes.state", "used"), last(30)]}},
            "aggs": {"h": {"terms": {"field": "resource.attributes.host@name", "size": 20, "order": {"_key": "asc"}},
                           "aggs": {"t": {"date_histogram": {"field": "time", "fixed_interval": "1m"},
                                          "aggs": {"v": {"avg": {"field": "value"}}}}}}}},
        # A metric over the whole range, 5-minute buckets, mean and peak.
        "fleet-all-5m": {"size": 0, "query": {"bool": {"filter": [term("name", "http.server.active_requests")]}},
            "aggs": {"t": {"date_histogram": {"field": "time", "fixed_interval": "5m"},
                           "aggs": {"avg": {"avg": {"field": "value"}}, "max": {"max": {"field": "value"}}}}}},
        # One service's peak per minute over the last 6 hours.
        "service-max-6h": {"size": 0, "query": {"bool": {"filter": [
            term("name", "http.server.request.duration"), term("serviceName", "svc-03"), last(360)]}},
            "aggs": {"t": {"date_histogram": {"field": "time", "fixed_interval": "1m"}, "aggs": {"v": {"max": {"field": "value"}}}}}},
        # Latency percentiles of a metric over the last 6 hours.
        "percentiles-6h": {"size": 0, "query": {"bool": {"filter": [
            term("name", "http.server.request.duration"), last(360)]}},
            "aggs": {"p": {"percentiles": {"field": "value", "percents": [50, 95, 99]}}}},
        # How many points arrived in the last 15 minutes.
        "count-15m": {"size": 0, "track_total_hits": True, "query": {"bool": {"filter": [last(15)]}}},
        # The last value of one series: the newest point.
        "latest-point": {"size": 1, "sort": [{"time": "desc"}], "_source": ["time", "value"], "query": {"bool": {"filter": [
            term("name", "system.cpu.utilization"), term("resource.attributes.host@name", host),
            term("metric.attributes.state", "user")]}}},
    }


def strip(answer):
    """What an answer is compared on: hits.total, the hits' source, and the
    aggregations, without timing or shard fields."""
    hits = answer.get("hits", {})
    return {
        "total": hits.get("total"),
        "hits": [h.get("_source") for h in hits.get("hits", [])],
        "aggs": answer.get("aggregations"),
    }


# Relative tolerance of a search's numbers, where 1e-6 is too tight.
TOLERANCE = {"percentiles-6h": 0.02}


def same(a, b, path="$", tol=1e-6):
    """The first place `a` and `b` differ, or None. Numbers match to a
    relative `tol`: an average summed in another order can differ in its last
    bits."""
    if isinstance(a, dict) and isinstance(b, dict):
        for k in sorted(set(a) | set(b)):
            if k not in a or k not in b:
                return f"{path}.{k}: present on one side only"
            d = same(a[k], b[k], f"{path}.{k}", tol)
            if d:
                return d
        return None
    if isinstance(a, list) and isinstance(b, list):
        if len(a) != len(b):
            return f"{path}: {len(a)} items against {len(b)}"
        for i, (x, y) in enumerate(zip(a, b)):
            d = same(x, y, f"{path}[{i}]", tol)
            if d:
                return d
        return None
    if isinstance(a, (int, float)) and isinstance(b, (int, float)) and not isinstance(a, bool):
        if a == b or math.isclose(a, b, rel_tol=tol, abs_tol=1e-9):
            return None
        return f"{path}: {a} against {b}"
    return None if a == b else f"{path}: {json.dumps(a)[:80]} against {json.dumps(b)[:80]}"


def run(args):
    meta = json.load(open(args.meta))
    report = {"meta": meta, "calls": args.calls, "searches": {}}
    for name, body in searches(meta).items():
        if args.searches and name not in args.searches.split(","):
            continue
        row = {}
        answers = {}
        for target, (node, index) in TARGETS.items():
            port = port_of(args.base_port, node)
            status, answer = call(port, "POST", f"/{index}/_search?request_cache=false", body)
            if status != 200:
                row[target] = {"error": f"{status} {json.dumps(answer)[:300]}"}
                continue
            answers[target] = strip(answer)
            times = []
            for _ in range(args.calls):
                t0 = time.perf_counter()
                status, answer = call(port, "POST", f"/{index}/_search?request_cache=false", body)
                times.append((time.perf_counter() - t0) * 1000)
                if status != 200:
                    row[target] = {"error": f"{status} {json.dumps(answer)[:300]}"}
                    break
            else:
                times.sort()
                row[target] = {
                    "p50_ms": round(statistics.median(times), 2),
                    "p95_ms": round(times[max(0, math.ceil(0.95 * len(times)) - 1)], 2),
                }
        for target in ("infino", "infino-ts"):
            if target in answers and "stock" in answers:
                diff = same(answers["stock"], answers[target], tol=TOLERANCE.get(name, 1e-6))
                row[target]["matches_stock"] = diff is None
                if diff:
                    row[target]["differs"] = diff
        report["searches"][name] = row
        cells = "  ".join(
            f"{t}={row[t].get('p50_ms', 'ERR')}/{row[t].get('p95_ms', '')}ms"
            + ("" if row[t].get("matches_stock", True) else " (DIFFERS)")
            for t in TARGETS if t in row
        )
        print(f"{name:16s} {cells}", file=sys.stderr)
    if args.out:
        with open(args.out, "w") as f:
            json.dump(report, f, indent=2)


def du(path):
    total = 0
    for root, _, files in os.walk(path):
        for f in files:
            try:
                total += os.path.getsize(os.path.join(root, f))
            except OSError:
                pass
    return total


def sizes(args):
    port = port_of(args.base_port, "stock")
    call(port, "POST", "/metrics/_forcemerge?max_num_segments=1", timeout=3600)
    _, stats = call(port, "GET", "/metrics/_stats/store")
    out = {"stock": stats["indices"]["metrics"]["primaries"]["store"]["size_in_bytes"]}
    for target in ("infino", "infino-ts"):
        index = TARGETS[target][1]
        # Each table's files sit in a directory named after its table, which
        # carries the index's name and uuid.
        _, settings = call(port_of(args.base_port, "default"), "GET", f"/{index}/_settings")
        uuid = settings[index]["settings"]["index"]["uuid"]
        dirs = [d for d in glob.glob(os.path.join(args.nodes, "default", "**", f"*{uuid}*"), recursive=True) if os.path.isdir(d)]
        out[target] = sum(du(d) for d in dirs)
        out[target + "_dirs"] = dirs
    meta = json.load(open(args.meta))
    for k in ("stock", "infino", "infino-ts"):
        print(f"{k:10s} {out[k] / 2**20:9.1f} MiB  {out[k] / meta['total']:6.1f} bytes/point", file=sys.stderr)
    if args.out:
        with open(args.out, "w") as f:
            json.dump(out, f, indent=2)


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--base-port", type=int, default=9250, help="http port of the default-mode node (stock is one above)")
    p.add_argument("--meta", default="metrics-meta.json", help="where load records what it loaded, for run and sizes")
    p.add_argument("--infino-node", choices=["default", "proxy"], default="default",
                   help="the node that holds the Infino indices: the default-mode one, or the proxy-mode one two above it")
    sub = p.add_subparsers(dest="cmd", required=True)
    g = sub.add_parser("gen")
    g.add_argument("--hosts", type=int, default=200)
    g.add_argument("--points", type=int, default=1080, help="points per series, 10 seconds apart")
    g.add_argument("--batch", type=int, default=5000)
    g.add_argument("--seed", type=int, default=1)
    g.add_argument("--dir", default="points")
    l = sub.add_parser("load")
    l.add_argument("--dir", default="points")
    l.add_argument("--threads", type=int, default=8)
    r = sub.add_parser("run")
    r.add_argument("--calls", type=int, default=20)
    r.add_argument("--searches", default="")
    r.add_argument("--out", default="")
    s = sub.add_parser("sizes")
    s.add_argument("--nodes", default="./nodes")
    s.add_argument("--out", default="")
    args = p.parse_args()
    for target in ("infino", "infino-ts"):
        TARGETS[target] = (args.infino_node, TARGETS[target][1])
    {"gen": gen, "load": load, "run": run, "sizes": sizes}[args.cmd](args)


if __name__ == "__main__":
    main()
