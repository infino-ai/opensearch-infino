#!/usr/bin/env bash
# Start three single-node OpenSearch clusters side by side, for comparing
# answers, latency and memory on one machine:
#   stock    Lucene, no plugin                         port BASE_PORT+1
#   default  the plugin, default mode, tables on disk  port BASE_PORT
#   proxy    the plugin, proxy mode, tables on disk    port BASE_PORT+2
#
# Env:
#   TARBALL    opensearch-<V>-linux-x64.tar.gz (required)
#   ZIP        the plugin zip built for that version (required)
#   LIB        the native bridge, libinfino_capi.so (required)
#   NODES      where the nodes live (default ./nodes); wiped per node
#   BASE_PORT  http port of the default-mode node (default 9250)
#   HEAP       JVM heap of each node (default 2g)
#   ONLY       space-separated subset of "stock default proxy" to start
#   KEEP       1: keep each node's data and tables, only stop it, reinstall
#              the plugin from ZIP and LIB, and start it again (to compare
#              two builds over the same loaded index)
#
# Stop them with: for p in $NODES/*/pid; do kill "$(cat "$p")"; done
set -euo pipefail
: "${TARBALL:?set TARBALL}" "${ZIP:?set ZIP}" "${LIB:?set LIB}"
NODES=$(realpath -m "${NODES:-./nodes}")
BASE_PORT=${BASE_PORT:-9250}
HEAP=${HEAP:-2g}
ONLY=${ONLY:-stock default proxy}
V=$(basename "$TARBALL" | sed -E 's/^opensearch-([0-9.]+)-.*/\1/')

restart() {  # name: reinstall the plugin into a node that keeps its data
  local name=$1 home=$NODES/$1/opensearch-$V
  if [ -f "$NODES/$name/pid" ]; then
    local pid; pid=$(cat "$NODES/$name/pid")
    kill "$pid" 2>/dev/null || true
    for _ in $(seq 1 60); do kill -0 "$pid" 2>/dev/null || break; sleep 1; done
  fi
  if [ "$name" != stock ]; then
    "$home/bin/opensearch-plugin" remove infino-engine >/dev/null
    "$home/bin/opensearch-plugin" install --batch "file://$(realpath "$ZIP")" >/dev/null
    sed -i "s#^-Dinfino.lib=.*#-Dinfino.lib=$(realpath "$LIB")#" "$home/config/jvm.options"
  fi
  (cd "$home" && nohup bin/opensearch -d -p "$NODES/$name/pid" > "$NODES/$name/start.log" 2>&1)
}

node() {  # name http transport
  local name=$1 http=$2 tport=$3 home=$NODES/$1/opensearch-$V
  if [ "${KEEP:-0}" = 1 ]; then restart "$name"; return; fi
  rm -rf "${NODES:?}/$name"; mkdir -p "$NODES/$name"
  tar -xzf "$TARBALL" -C "$NODES/$name"
  sed -i "s/^-Xms.*/-Xms$HEAP/; s/^-Xmx.*/-Xmx$HEAP/" "$home/config/jvm.options"
  cat >> "$home/config/opensearch.yml" <<YML
cluster.name: local-$name
node.name: $name
http.port: $http
transport.port: $tport
discovery.type: single-node
plugins.security.disabled: true
# A shared dev disk is often past the default watermarks; they would block index creation.
cluster.routing.allocation.disk.threshold_enabled: false
path.data: $NODES/$name/data
YML
  if [ "$name" != stock ]; then
    "$home/bin/opensearch-plugin" remove opensearch-neural-search >/dev/null
    "$home/bin/opensearch-plugin" install --batch "file://$(realpath "$ZIP")" >/dev/null
    printf -- '-Dinfino.lib=%s\n-Dinfino.uri=file://%s\n' "$(realpath "$LIB")" "$NODES/$name/catalog" >> "$home/config/jvm.options"
    [ "$name" = proxy ] && echo "infino.default_mode.enabled: false" >> "$home/config/opensearch.yml"
  fi
  (cd "$home" && nohup bin/opensearch -d -p "$NODES/$name/pid" > "$NODES/$name/start.log" 2>&1)
}

for n in $ONLY; do
  case $n in
    default) node default "$BASE_PORT" $((BASE_PORT + 100)) ;;
    stock)   node stock $((BASE_PORT + 1)) $((BASE_PORT + 101)) ;;
    proxy)   node proxy $((BASE_PORT + 2)) $((BASE_PORT + 102)) ;;
  esac
done
for n in $ONLY; do
  case $n in default) p=$BASE_PORT ;; stock) p=$((BASE_PORT + 1)) ;; proxy) p=$((BASE_PORT + 2)) ;; esac
  for _ in $(seq 1 90); do curl -s "localhost:$p" >/dev/null && break; sleep 2; done
  echo "$n :$p pid $(cat "$NODES/$n/pid") $(curl -s "localhost:$p/_cat/plugins?h=component" | grep -c infino) infino plugin(s)"
done
