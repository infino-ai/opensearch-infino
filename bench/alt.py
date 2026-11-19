import json, sys, time, http.client, statistics
names = sys.argv[2].split(',') if len(sys.argv) > 2 else None
srch = json.load(open(sys.argv[1]))
ports = {'stock': 9251, 'proxy': 9252}
conns = {k: http.client.HTTPConnection('localhost', p) for k, p in ports.items()}
def call(k, body):
    c = conns[k]; t = time.perf_counter()
    c.request('POST', '/big5/_search?request_cache=false', body=body, headers={'content-type': 'application/json'})
    r = c.getresponse(); r.read(); return (time.perf_counter() - t) * 1e3, r.status
for s in srch:
    if names and s['name'] not in names: continue
    body = json.dumps(s['body']); ts = {k: [] for k in ports}; bad = set()
    for k in ports: call(k, body); call(k, body)
    for i in range(30):
        for k in ports:
            ms, code = call(k, body); ts[k].append(ms)
            if code != 200: bad.add(k)
    if bad:  # an error answer's time is not a search time
        cell = lambda k: '  error' if k in bad else f"{statistics.median(ts[k]):7.2f}"
        print(f"{s['name']:40} stock {cell('stock')}  proxy {cell('proxy')}  ratio     -", flush=True); continue
    st, px = statistics.median(ts['stock']), statistics.median(ts['proxy'])
    print(f"{s['name']:40} stock {st:7.2f}  proxy {px:7.2f}  ratio {px/st:5.2f}", flush=True)
