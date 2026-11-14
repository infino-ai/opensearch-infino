#!/usr/bin/env python3
"""Load big5 documents into the nodes bench/local-nodes.sh starts, then time
big5's searches (bench/big5-searches.json, from Elastic's big5 track) on each
and record how far each one raises the node's resident memory.

    python3 bench/big5.py --nodes ./nodes load --data '~/.rally/benchmarks/data/big5_1m/logs-[0-9].ndjson'
    python3 bench/big5.py --nodes ./nodes run --calls 20 --out result.json [--searches term,range]

`load` creates the index `big5` (bench/big5-index.json) on each node and bulk
loads every line of the data files. `run` sends each search once as a
warmup, then `--calls` times, sampling the node's RSS every 20 ms while a call
runs. A node that dies mid-search is reported as such and left out of the
searches after it. Each answer is compared with stock's: hits.total, the
set of top hit ids (scored searches may order ties differently), and the
aggregations as JSON.

Linux only: a node's resident memory is read from /proc.
"""

import argparse
import glob
import json
import os
import statistics
import sys
import threading
import time
import urllib.error
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
INDEX = "big5"
SEARCHES = {s["name"]: s["body"] for s in json.load(open(os.path.join(HERE, "big5-searches.json")))}
PORTS = {"default": 0, "stock": 1, "proxy": 2}


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


def nodes(args):
    out = {}
    for name, off in PORTS.items():
        pidfile = os.path.join(args.nodes, name, "pid")
        if os.path.exists(pidfile):
            out[name] = (args.base_port + off, int(open(pidfile).read().strip()))
    return out


def rss_kb(pid):
    try:
        for line in open(f"/proc/{pid}/status"):
            if line.startswith("VmRSS:"):
                return int(line.split()[1])
    except FileNotFoundError:
        return None
    return None


def load(args):
    files = sorted(glob.glob(os.path.expanduser(args.data)))
    if not files:
        sys.exit(f"no data files match {args.data}")
    mapping = json.load(open(os.path.join(HERE, "big5-index.json")))
    for name, (port, _) in nodes(args).items():
        if args.only and name not in args.only.split(","):
            continue
        call(port, "DELETE", f"/{INDEX}")
        status, body = call(port, "PUT", f"/{INDEX}", mapping)
        if status != 200:
            sys.exit(f"{name}: create {INDEX} returned {status}: {body}")

        def batches():
            buf, n = [], 0
            for f in files:
                with open(f, "rb") as fh:
                    for line in fh:
                        buf.append(b'{"index":{}}\n' + line.rstrip(b"\n") + b"\n")
                        n += 1
                        if len(buf) == args.batch:
                            yield b"".join(buf)
                            buf = []
                        if args.docs and n >= args.docs:
                            if buf:
                                yield b"".join(buf)
                            return
            if buf:
                yield b"".join(buf)

        lock = threading.Lock()
        it = batches()
        errors = [0]
        start = time.time()

        def worker():
            while True:
                with lock:
                    b = next(it, None)
                if b is None:
                    return
                for _ in range(20):
                    status, body = call(port, "POST", f"/{INDEX}/_bulk", b, ndjson=True)
                    if status == 429:
                        time.sleep(1)
                        continue
                    if body.get("errors"):
                        errors[0] += sum(1 for i in body["items"] if i["index"].get("error"))
                    break

        threads = [threading.Thread(target=worker) for _ in range(args.clients)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        call(port, "POST", f"/{INDEX}/_refresh")
        _, count = call(port, "GET", f"/{INDEX}/_count")
        print(f"{name}: {count.get('count')} docs in {time.time() - start:.0f} s, {errors[0]} item errors", flush=True)


def run(args):
    ns = nodes(args)
    dead = set()
    results = {}
    stock_ids = {}
    answers = {}
    order = ["stock"] + [n for n in ns if n != "stock"]
    wanted = args.searches.split(",") if args.searches else [s for s in SEARCHES if s not in args.skip.split(",")]
    for search in wanted:
        body = SEARCHES[search]
        for name in order:
            if name not in ns or name in dead:
                continue
            port, pid = ns[name]
            base = rss_kb(pid)
            if base is None:
                dead.add(name)
                continue
            times, peak = [], base
            status, res = None, {}
            for i in range(args.calls + 1):
                stop = threading.Event()
                peak_box = [base]

                def sample():
                    while not stop.is_set():
                        r = rss_kb(pid)
                        if r is None:
                            return
                        peak_box[0] = max(peak_box[0], r)
                        time.sleep(0.02)

                t = threading.Thread(target=sample)
                t.start()
                t0 = time.perf_counter()
                try:
                    status, res = call(port, "POST", f"/{INDEX}/_search?request_cache=false", body, timeout=args.timeout)
                except Exception as e:  # the node died or stopped answering
                    status, res = None, {"error": repr(e)}
                elapsed = (time.perf_counter() - t0) * 1000
                stop.set()
                t.join()
                peak = max(peak, peak_box[0])
                if status != 200:
                    break
                if i > 0:
                    times.append(elapsed)
            alive = rss_kb(pid) is not None
            if not alive:
                dead.add(name)
            ids = sorted(h["_id"] for h in res.get("hits", {}).get("hits", [])) if status == 200 else None
            total = res.get("hits", {}).get("total") if status == 200 else None
            aggs = json.dumps(res.get("aggregations"), sort_keys=True) if status == 200 else None
            answer = (ids, json.dumps(total, sort_keys=True), aggs)
            if name == "stock":
                stock_ids[search] = answer
            row = {
                "status": status if alive else "node died",
                "p50_ms": round(statistics.median(times), 1) if times else None,
                "rss_before_gb": round(base / 1048576, 2),
                "rss_peak_gb": round(peak / 1048576, 2),
                "rss_rise_gb": round((peak - base) / 1048576, 2),
                "hits_total": total,
                "same_as_stock": None if name == "stock" or ids is None else {
                    k: a == b for k, a, b in zip(("top_ids", "total", "aggs"), answer, stock_ids.get(search, (None,) * 3))
                },
                "error": None if status == 200 else str(res)[:300],
            }
            results.setdefault(search, {})[name] = row
            if args.answers and status == 200:
                res.pop("took", None)
                answers.setdefault(search, {})[name] = res
            print(f"{search:45s} {name:8s} status={row['status']} p50={row['p50_ms']} ms "
                  f"rss {row['rss_before_gb']}->{row['rss_peak_gb']} GB (+{row['rss_rise_gb']}) "
                  f"same_as_stock={row['same_as_stock']}", flush=True)
    if args.out:
        json.dump(results, open(args.out, "w"), indent=1)
    if args.answers:
        json.dump(answers, open(args.answers, "w"), indent=1, sort_keys=True)


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--nodes", default="./nodes")
    p.add_argument("--base-port", type=int, default=9250)
    sub = p.add_subparsers(dest="cmd", required=True)
    lo = sub.add_parser("load")
    lo.add_argument("--data", required=True)
    lo.add_argument("--docs", type=int, default=0, help="stop after this many documents (0: all)")
    lo.add_argument("--batch", type=int, default=5000)
    lo.add_argument("--clients", type=int, default=4)
    lo.add_argument("--only", help="comma-separated node names to load (default: every node)")
    ru = sub.add_parser("run")
    ru.add_argument("--calls", type=int, default=20)
    ru.add_argument("--timeout", type=int, default=300)
    ru.add_argument("--out")
    ru.add_argument("--answers", help="write each node's last response per search here, to diff two builds")
    ru.add_argument("--searches", help="comma-separated search names (default: all)")
    ru.add_argument("--skip", default="scroll", help="comma-separated search names to leave out (default: scroll)")
    args = p.parse_args()
    args.nodes = os.path.abspath(args.nodes)
    {"load": load, "run": run}[args.cmd](args)


if __name__ == "__main__":
    main()
