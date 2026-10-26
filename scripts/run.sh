#!/usr/bin/env bash
# Install the plugin into an OpenSearch distribution and (re)start the node.
#
# Env you may override:
#   DISTRO              path to an OpenSearch install of the version the zip was built for
#   OPENSEARCH_VERSION  the version the zip was built for (default 3.9.0)
#   INFINO_LIB          path to the native bridge         (-Dinfino.lib)
#   INFINO_URI          Infino catalog URI                (-Dinfino.uri: file:///abs/path, s3://bucket/prefix, ...)
set -euo pipefail
HERE="$(cd "$(dirname "$0")/.." && pwd)"
OPENSEARCH_VERSION="${OPENSEARCH_VERSION:-3.9.0}"
DISTRO="${DISTRO:?set DISTRO to an OpenSearch $OPENSEARCH_VERSION install}"
case "$(uname -s)" in
  Darwin) DEFAULT_LIB="$HERE/infino-capi/target/release/libinfino_capi.dylib" ;;
  *)      DEFAULT_LIB="$HERE/infino-capi/target/release/libinfino_capi.so" ;;
esac
INFINO_LIB="${INFINO_LIB:-$DEFAULT_LIB}"
INFINO_URI="${INFINO_URI:-file://$HERE/data}"

ZIP="$HERE/plugin/build/distributions/infino-engine-$OPENSEARCH_VERSION.zip"
echo "==> (re)installing plugin: $ZIP"
"$DISTRO/bin/opensearch-plugin" remove infino-engine 2>/dev/null || true
"$DISTRO/bin/opensearch-plugin" install -b "file://$ZIP"

# The bridge and the catalog location are JVM system properties.
export OPENSEARCH_JAVA_OPTS="${OPENSEARCH_JAVA_OPTS:-} -Dinfino.lib=$INFINO_LIB -Dinfino.uri=$INFINO_URI"
echo "==> starting node  (infino.lib=$INFINO_LIB  infino.uri=$INFINO_URI)"
mkdir -p "$HERE/data"
exec "$DISTRO/bin/opensearch"
