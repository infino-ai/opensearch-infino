#!/bin/bash
# Rebuild and rerun the big5 100M race of 2026-11-17 (stock OpenSearch 3.9.0
# against the Infino proxy node, both on one host). See README.md beside this
# script for what the build holds and the numbers it gave.
#
#   bench/repro/big5-100m-2026-11-17.sh build       # bridge + plugin zip into $WORK
#   bench/repro/big5-100m-2026-11-17.sh race-fresh  # new nodes, load 100M docs into each, race
#   bench/repro/big5-100m-2026-11-17.sh race-reuse  # restart the proxy on nodes already loaded, race
#
# WORK      where builds, nodes and results go (default: ./repro-work)
# TARBALL   opensearch-3.9.0-linux-x64.tar.gz
# JDK, GRADLE   a JDK 21 home and a Gradle 9.4.1 home
# CATALOG_URI   the proxy's catalog. The original run used
#               gs://infino-bench-big5/big5-100m-h; a fresh load needs a
#               prefix of your own.
# Linux only. Nothing else should compile or load on the host during a race.
set -eu
cd "$(dirname "$0")/../.."
REPO=$PWD
WORK=$(realpath -m "${WORK:-$REPO/repro-work}")
TARBALL=${TARBALL:-/home/ubuntu/osnodes/dl/opensearch-3.9.0-linux-x64.tar.gz}
JDK=${JDK:-$HOME/tools/jdk-21.0.13+9}
GRADLE=${GRADLE:-$HOME/tools/gradle-9.4.1}
CATALOG_URI=${CATALOG_URI:-gs://infino-bench-big5/big5-100m-h}
LIB=$WORK/lib/libinfino_capi.so
ZIP=$REPO/plugin/build/distributions/infino-engine.zip
NODES=${NODES:-$WORK/nodes100m}
mkdir -p "$WORK"

# The settings the 2026-11-17 race ran with.
export HEAP=3g                    # both nodes
export COLUMN_CACHE=4294967296    # proxy infino.yaml: decoded_column_cache_bytes (4 GiB)
# This build carries an experiment flag whose default (true) swaps the
# engine's statement-free calls for SQL statements. The race ran with false.
SQL_ONLY_OPTS="-Dinfino.sql_only=false"

build() {
  # The bridge pins the engine by git rev in infino-capi/Cargo.toml.
  (cd infino-capi && CARGO_TARGET_DIR=$WORK/target-bridge cargo build --release -j 4)
  mkdir -p "$(dirname "$LIB")"
  cp "$WORK/target-bridge/release/libinfino_capi.so" "$LIB"
  (cd plugin && JAVA_HOME=$JDK PATH=$JDK/bin:$GRADLE/bin:$PATH \
     gradle assemble -Dopensearch.version=3.9.0 --max-workers=2 --console=plain -q)
  ls -la "$LIB" "$ZIP"
}

case "${1:-}" in
  build) build ;;
  race-fresh)
    OPENSEARCH_JAVA_OPTS="$SQL_ONLY_OPTS" TARBALL=$TARBALL ZIP=$ZIP LIB=$LIB NODES=$NODES \
      CATALOG_URI=$CATALOG_URI DOCS=100000000 OUT=$WORK/race-$(date +%Y%m%d-%H%M) \
      bash bench/race-big.sh ;;
  race-reuse)
    OPENSEARCH_JAVA_OPTS="$SQL_ONLY_OPTS" KEEP=1 ONLY=proxy TARBALL=$TARBALL NODES=$NODES \
      ZIP=$ZIP LIB=$LIB CATALOG_URI=$CATALOG_URI bash bench/local-nodes.sh 2>&1 | tail -2
    for _ in $(seq 120); do
      curl -s -m3 localhost:9252/_cluster/health | grep -q '"status":"\(green\|yellow\)"' && break; sleep 5
    done
    curl -s localhost:9252/big5/_count; echo
    RACE_ONLY=1 ZIP=x LIB=x NODES=$NODES CATALOG_URI=$CATALOG_URI DOCS=100000000 \
      OUT=$WORK/race-$(date +%Y%m%d-%H%M) bash bench/race-big.sh ;;
  *) sed -n '2,20p' "$0"; exit 2 ;;
esac
