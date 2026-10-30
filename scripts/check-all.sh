#!/usr/bin/env bash
# The plugin's full check (precommit, unit tests, the REST suites on a test
# cluster) on every OpenSearch release it supports.
#
# `gradle check` alone builds against one release, 3.9.0 by default. The
# security integration takes a different path on 2.x, on 3.0 through 3.5
# and from 3.6 on, and a path is exercised only by a check built against a
# release that takes it. Run this before relying on any release other than
# the default.
#
# Each release's build tools want their own Gradle. Env you may override:
#   GRADLE_8_12  launcher for 2.19.x, Gradle 8.12.1 (default: gradle on PATH)
#   GRADLE_8_14  launcher for 3.0.x, Gradle 8.14 (default: gradle on PATH)
#   GRADLE       launcher for 3.3 and later, Gradle 9 (default: gradle on PATH)
#   JAVA_HOME    a JDK 21
#   INFINO_LIB   the native bridge to test with (default: the one
#                scripts/build.sh builds)
#   RELEASES     the releases to check, space-separated (default: all five)
set -uo pipefail
HERE="$(cd "$(dirname "$0")/.." && pwd)"
GRADLE="${GRADLE:-gradle}"
GRADLE_8_12="${GRADLE_8_12:-gradle}"
GRADLE_8_14="${GRADLE_8_14:-gradle}"
RELEASES="${RELEASES:-3.9.0 3.6.0 3.3.2 3.0.0 2.19.6}"
LIB_ARG=()
if [ -n "${INFINO_LIB:-}" ]; then
  LIB_ARG=("-Dinfino.lib=$INFINO_LIB")
fi

failed=()
for v in $RELEASES; do
  case "$v" in
    2.19.*) launcher="$GRADLE_8_12" ;;
    3.0.*) launcher="$GRADLE_8_14" ;;
    *) launcher="$GRADLE" ;;
  esac
  echo "==> OpenSearch $v (gradle: $launcher)"
  if ( cd "$HERE/plugin" && "$launcher" --no-daemon -Dopensearch.version="$v" "${LIB_ARG[@]}" clean check ); then
    echo "    $v: green"
  else
    echo "    $v: FAILED"
    failed+=("$v")
  fi
done

if [ "${#failed[@]}" -gt 0 ]; then
  echo "==> failed on: ${failed[*]}"
  exit 1
fi
echo "==> green on: $RELEASES"
