#!/usr/bin/env python3
"""Sample data for the getting-started guide: application log events, each with
a toy 16-dimension embedding of its message.

    python3 docs/sample-events.py > events.ndjson          # 100,000 events over the last 7 days
    python3 docs/sample-events.py --embed "disk is full"   # the same embedding for a query

The embedding hashes each word into one of 16 buckets and normalizes the result:
a stand-in for a real model, so the guide runs with nothing but Python.
"""
import datetime as dt
import json
import math
import random
import sys
import zlib


def embed(text):
    v = [0.0] * 16
    for word in text.lower().split():
        v[zlib.crc32(word.encode()) % 16] += 1.0
    n = math.sqrt(sum(x * x for x in v)) or 1.0
    return [round(x / n, 4) for x in v]


if sys.argv[1:2] == ["--embed"]:
    print(json.dumps(embed(" ".join(sys.argv[2:]))))
    sys.exit()

random.seed(7)
now = dt.datetime.now(dt.timezone.utc)
paths = {
    "checkout": ["/cart", "/checkout", "/order"],
    "search": ["/search", "/suggest"],
    "auth": ["/login", "/logout", "/token"],
    "inventory": ["/sku", "/stock"],
    "payments": ["/charge", "/refund"],
}
errors = [
    "connection reset by peer",
    "upstream timed out after 30s",
    "disk full on volume /var/data",
    "card declined by issuer",
    "session token expired",
    "deadlock detected, retrying",
    "out of memory while sorting results",
]
for _ in range(100_000):
    service = random.choice(list(paths))
    status = random.choices([200, 201, 304, 400, 404, 500, 503], [70, 8, 6, 5, 5, 4, 2])[0]
    level = "error" if status >= 500 else "warn" if status >= 400 else "info"
    path = random.choice(paths[service])
    if level == "error":
        message = random.choice(errors)
    else:
        message = f"{random.choice(['GET', 'POST'])} {path} returned {status}"
    when = now - dt.timedelta(seconds=random.randint(0, 7 * 86400))
    print(json.dumps({
        "@timestamp": when.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "service": service,
        "level": level,
        "status": status,
        "path": path,
        "latency_ms": round(random.lognormvariate(3.5, 0.8), 1),
        "host": f"node-{random.randint(1, 12)}",
        "message": message,
        "message_embedding": embed(message),
    }))
