#!/bin/bash
# Fresh stock and proxy nodes in $NODES, the proxy's catalog at $CATALOG_URI
# (an object store), the first $DOCS big5 documents streamed into each from
# Elastic's server, then every search raced proxy against stock (alt.py:
# 30 alternating calls, request_cache=false), two rounds after a warmup,
# and one answer check.
#   ZIP=... LIB=... NODES=... CATALOG_URI=gs://bucket/prefix DOCS=100000000 OUT=dir bench/race-big.sh
# CLIENTS is how many bulks the loader keeps in flight (default 4), BATCH
# the documents in each bulk (default 5000).
# START names the nodes started fresh and LOAD the nodes loaded (both default
# to stock and proxy), so a run whose proxy load failed can redo just that.
# Nothing else should compile or load on the host meanwhile: a build beside
# a loading node can get the node killed for memory.
set -u
cd "$(dirname "$0")/.."
: "${ZIP:?}" "${LIB:?}" "${NODES:?}" "${CATALOG_URI:?}" "${DOCS:?}" "${OUT:?}"
TARBALL=${TARBALL:-/home/ubuntu/osnodes/dl/opensearch-3.9.0-linux-x64.tar.gz}
mkdir -p "$OUT" "$NODES"
log() { echo "== $(date +%T) $*"; }
# RACE_ONLY=1 skips starting and loading: it races the nodes already up.
if [ -z "${RACE_ONLY:-}" ]; then
# Any node on the race ports, wherever it lives, stops first.
START=${START:-stock proxy}; LOAD=${LOAD:-proxy stock}
ports=""; for n in $START; do case $n in stock) ports="$ports or sport = :9251" ;; proxy) ports="$ports or sport = :9252" ;; esac; done
on_ports() { ss -ltnpH "( ${ports# or } )" | grep -o 'pid=[0-9]*' | cut -d= -f2 | sort -u; }
for p in $(on_ports); do kill "$p"; done
until [ -z "$(on_ports)" ]; do sleep 1; done
log "old nodes stopped: $START"
HEAP=${HEAP:-3g} ONLY="$START" TARBALL=$TARBALL ZIP=$ZIP LIB=$LIB NODES=$NODES CATALOG_URI=$CATALOG_URI \
  bash bench/local-nodes.sh > "$OUT/nodes.log" 2>&1
for port in 9251 9252; do
  for i in $(seq 150); do curl -s -m 2 localhost:$port/_cluster/health | grep -q '"status":"\(green\|yellow\)"' && break; sleep 2; done
  curl -s -m 2 localhost:$port/_cluster/health | grep -q '"status":"\(green\|yellow\)"' || { log "node $port not healthy"; tail -5 "$OUT/nodes.log"; exit 1; }
done
grep -h "infino.uri" "$NODES/proxy/opensearch-3.9.0/config/jvm.options"
log "nodes up"
for n in $LOAD; do
  t0=$(date +%s)
  python3 bench/big5.py --nodes "$NODES" load --only $n --data "rally:$DOCS" --clients "${CLIENTS:-4}" --batch "${BATCH:-5000}" > "$OUT/load-$n.log" 2>&1
  log "$n load $(( $(date +%s) - t0 )) s: $(tail -1 "$OUT/load-$n.log")"
  df -h / | tail -1; free -g | sed -n 2p
done
sleep 300   # merges and compaction settle
fi
for port in 9251 9252; do echo "$port $(curl -s localhost:$port/big5/_count)"; done
du -sh "$NODES/stock" "$NODES/proxy"
python3 bench/big5.py --nodes "$NODES" run --calls 3 --searches default > /dev/null 2>&1
python3 bench/alt.py bench/big5-searches.json > /dev/null 2>&1
for r in 1 2; do python3 bench/alt.py bench/big5-searches.json > "$OUT/race-r$r.txt" 2>&1; log "round $r done"; done
python3 bench/big5.py --nodes "$NODES" run --calls 1 --out "$OUT/run.json" --answers "$OUT/answers.json" > "$OUT/run.log" 2>&1
log RACEDONE
