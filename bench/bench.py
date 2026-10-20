#!/usr/bin/env python3
import json, sys, time, urllib.request, random, statistics

H = "http://localhost:9200"
N = int(sys.argv[1]) if len(sys.argv) > 1 else 20000
CHUNK = 5000
VOCAB = "alpha bravo charlie delta echo foxtrot golf hotel india juliet kilo lima mike november oscar papa quebec romeo sierra tango".split()
CATS = [f"cat{i}" for i in range(8)]
random.seed(1)

def req(method, path, body=None, raw=False):
    data = body.encode() if isinstance(body, str) else (json.dumps(body).encode() if body is not None else None)
    r = urllib.request.Request(H + path, data=data, method=method,
                               headers={"Content-Type": "application/json" if not raw else "application/x-ndjson"})
    with urllib.request.urlopen(r) as resp:
        return json.loads(resp.read())

def make_index(name, infino):
    try: req("DELETE", "/" + name)
    except Exception: pass
    settings = {"number_of_shards": 1, "number_of_replicas": 0, "refresh_interval": "-1"}
    mappings = {"properties": {"category": {"type": "keyword"}, "price": {"type": "float"}}}
    if infino:
        settings["index.infino.enabled"] = True
        mappings["_source"] = {"enabled": False}
        mappings["properties"]["text"] = {"type": "text", "fielddata": True}
    else:
        mappings["properties"]["text"] = {"type": "text"}
    req("PUT", "/" + name, {"settings": settings, "mappings": mappings})

def gen(i):
    words = " ".join(random.choice(VOCAB) for _ in range(5))
    return {"text": words, "category": random.choice(CATS), "price": round(random.uniform(1, 1000), 2)}

def bulk_index(name, n):
    random.seed(42)  # identical data for every index
    t0 = time.time()
    for start in range(0, n, CHUNK):
        lines = []
        for i in range(start, min(start + CHUNK, n)):
            lines.append(json.dumps({"index": {"_index": name, "_id": str(i)}}))
            lines.append(json.dumps(gen(i)))
        body = "\n".join(lines) + "\n"
        resp = req("POST", "/_bulk", body, raw=True)
        if resp.get("errors"):
            item = next((it for it in resp["items"] if it["index"].get("error")), None)
            raise RuntimeError("bulk error: " + json.dumps(item)[:300])
    dt = time.time() - t0
    req("POST", f"/{name}/_refresh")
    return dt

QUERIES = {
    "match(text:alpha)":        {"track_total_hits": True, "query": {"match": {"text": "alpha"}}},
    "terms(category)":          {"size": 0, "aggs": {"c": {"terms": {"field": "category"}}}},
    "metrics(sum,avg price)":   {"size": 0, "aggs": {"s": {"sum": {"field": "price"}}, "a": {"avg": {"field": "price"}}}},
    "range(price>500)":         {"track_total_hits": True, "query": {"range": {"price": {"gt": 500}}}},
    "match+sum(scoped)":        {"size": 0, "query": {"match": {"text": "alpha"}}, "aggs": {"s": {"sum": {"field": "price"}}}},
}

def validate():
    out = {}
    for name in ("bench_lucene", "bench_infino"):
        r = req("GET", f"/{name}/_search", {"size": 0, "track_total_hits": True,
                                            "query": {"match": {"text": "alpha"}},
                                            "aggs": {"s": {"sum": {"field": "price"}}}})
        out[name] = (r["hits"]["total"]["value"], round(r["aggregations"]["s"]["value"], 1))
    wt = {}
    for name in ("bench_lucene", "bench_infino"):
        r = req("GET", f"/{name}/_search", {"size": 0, "aggs": {"s": {"sum": {"field": "price"}}}})
        wt[name] = round(r["aggregations"]["s"]["value"], 1)
    print("  validate match(alpha): lucene(hits,sum)=%s infino=%s" % (out["bench_lucene"], out["bench_infino"]))
    print("  validate whole sum:    lucene=%s infino=%s" % (wt["bench_lucene"], wt["bench_infino"]))

def latency(name, body, iters=20):
    xs = []
    for _ in range(iters):
        t0 = time.time()
        try:
            req("GET", f"/{name}/_search", body)
        except Exception as e:
            return None
        xs.append((time.time() - t0) * 1000)
    return statistics.median(xs)

def main():
    print(f"=== scale N={N} ===")
    for name, infino in [("bench_lucene", False), ("bench_infino", True)]:
        make_index(name, infino)
    rates = {}
    for name, infino in [("bench_lucene", False), ("bench_infino", True)]:
        dt = bulk_index(name, N)
        rates[name] = N / dt
        print(f"  index {name:14s}: {dt:7.1f}s  {N/dt:9.0f} docs/s")
    cnt_l = req("GET", "/bench_lucene/_count")["count"]
    cnt_i = req("GET", "/bench_infino/_count")["count"]
    print(f"  counts: lucene={cnt_l} infino={cnt_i}")
    print(f"  {'query':26s} {'lucene ms':>10s} {'infino ms':>10s}")
    for q, body in QUERIES.items():
        ml = latency("bench_lucene", body)
        mi = latency("bench_infino", body)
        sl = f"{ml:10.1f}" if ml is not None else "       ERR"
        si = f"{mi:10.1f}" if mi is not None else "       ERR"
        print(f"  {q:26s} {sl} {si}")
    validate()

if __name__ == "__main__":
    main()
