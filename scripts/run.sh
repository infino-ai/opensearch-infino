#!/usr/bin/env bash
# Install the plugin into a built OpenSearch distribution and (re)start the node.
#
# Env you may override:
#   DISTRO      path to a built OpenSearch install (bin/opensearch inside)
#   INFINO_LIB  path to libinfino_capi.dylib  (-Dinfino.lib)
#   INFINO_URI  Infino catalog URI            (-Dinfino.uri, e.g. file:///abs/path)
set -euo pipefail
HERE="$(cd "$(dirname "$0")/.." && pwd)"
WS="$(cd "$HERE/.." && pwd)"
DISTRO="${DISTRO:-$WS/OpenSearch/distribution/archives/darwin-arm64-tar/build/install/opensearch-3.10.0-SNAPSHOT}"
INFINO_LIB="${INFINO_LIB:-$HERE/infino-capi/target/release/libinfino_capi.dylib}"
INFINO_URI="${INFINO_URI:-file://$HERE/data}"
export JAVA_HOME="${JAVA21_HOME:-/opt/homebrew/opt/openjdk@21}"
export OPENSEARCH_JAVA_HOME="$JAVA_HOME"; export PATH="$JAVA_HOME/bin:$PATH"

ZIP=$(ls "$WS"/OpenSearch/plugins/examples/infino-engine/build/distributions/infino-engine-*.zip | head -1)
echo "==> (re)installing plugin: $ZIP"
"$DISTRO/bin/opensearch-plugin" remove infino-engine 2>/dev/null || true
"$DISTRO/bin/opensearch-plugin" install -b "file://$ZIP"

# Pass the dylib + catalog location as JVM system properties.
export OPENSEARCH_JAVA_OPTS="-Dinfino.lib=$INFINO_LIB -Dinfino.uri=$INFINO_URI"
echo "==> starting node  (infino.lib=$INFINO_LIB  infino.uri=$INFINO_URI)"
mkdir -p "$HERE/data"
exec "$DISTRO/bin/opensearch"
