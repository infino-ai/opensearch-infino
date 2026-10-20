#!/usr/bin/env bash
# Build the Infino cdylib bridge and the OpenSearch plugin.
#
# Layout assumed (siblings under one workspace dir):
#   <ws>/infino/              public Apache-2.0 infino crate (path dep of infino-capi)
#   <ws>/OpenSearch/          an OpenSearch source checkout (tag/branch 3.10.x)
#   <ws>/opensearch-infino/   THIS repo
#
# Env you may override:
#   OPENSEARCH_SRC   path to the OpenSearch checkout (default ../OpenSearch)
#   JAVA21_HOME      JDK 21 home (OpenSearch build requires it)
set -euo pipefail
HERE="$(cd "$(dirname "$0")/.." && pwd)"
WS="$(cd "$HERE/.." && pwd)"
OPENSEARCH_SRC="${OPENSEARCH_SRC:-$WS/OpenSearch}"
export JAVA21_HOME="${JAVA21_HOME:-/opt/homebrew/opt/openjdk@21}"
export JAVA_HOME="$JAVA21_HOME"; export PATH="$JAVA_HOME/bin:$PATH"

echo "==> building cdylib (infino-capi)"
( cd "$HERE/infino-capi" && cargo build --release )
echo "    dylib: $HERE/infino-capi/target/release/libinfino_capi.dylib"

echo "==> syncing plugin sources into the OpenSearch checkout"
PLUGIN_DST="$OPENSEARCH_SRC/plugins/examples/infino-engine"
mkdir -p "$PLUGIN_DST/src/main/java/org/opensearch/infino"
cp "$HERE"/plugin/build.gradle "$PLUGIN_DST/build.gradle"
cp "$HERE"/plugin/src/main/java/org/opensearch/infino/*.java \
   "$PLUGIN_DST/src/main/java/org/opensearch/infino/"

echo "==> building the plugin zip"
( cd "$OPENSEARCH_SRC" && ./gradlew :example-plugins:infino-engine:bundlePlugin )
echo "    zip: $PLUGIN_DST/build/distributions/infino-engine-*.zip"
