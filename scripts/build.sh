#!/usr/bin/env bash
# Build the Infino native bridge and the OpenSearch plugin zip.
#
# No OpenSearch checkout is needed: the plugin builds out of tree against a
# released OpenSearch, pulled from Maven by the build. The zip installs only
# into that exact version.
#
# Env you may override:
#   OPENSEARCH_VERSION  release to build against (default 3.9.0)
#   JAVA_HOME           a JDK 21
#   GRADLE              gradle launcher (default: gradle on PATH)
set -euo pipefail
HERE="$(cd "$(dirname "$0")/.." && pwd)"
OPENSEARCH_VERSION="${OPENSEARCH_VERSION:-3.9.0}"
GRADLE="${GRADLE:-gradle}"

echo "==> building the native bridge (infino-capi)"
( cd "$HERE/infino-capi" && cargo build --release )
case "$(uname -s)" in
  Darwin) LIB="$HERE/infino-capi/target/release/libinfino_capi.dylib" ;;
  *)      LIB="$HERE/infino-capi/target/release/libinfino_capi.so" ;;
esac
echo "    bridge: $LIB"

echo "==> building the plugin zip for OpenSearch $OPENSEARCH_VERSION"
( cd "$HERE/plugin" && "$GRADLE" -Dopensearch.version="$OPENSEARCH_VERSION" bundlePlugin )
echo "    zip: $HERE/plugin/build/distributions/infino-engine.zip (for OpenSearch $OPENSEARCH_VERSION)"
