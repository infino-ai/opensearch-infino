#!/bin/bash
# Maps the INFINO_* environment onto the plugin's JVM properties and node
# settings, then hands off to the stock OpenSearch entrypoint (which still
# handles OPENSEARCH_INITIAL_ADMIN_PASSWORD, the demo security config and
# dotted env vars such as cluster.name).
#
#   INFINO_STORAGE       cloud (default) | local | memory
#   INFINO_API_KEY       cloud: the account key (read by the plugin from the env)
#   INFINO_PLATFORM_URL  cloud: the hosted service (default https://api.platform.infino.ws)
#   INFINO_DATABASE      cloud: the account's database (default: the cluster name)
#   INFINO_URI           local: catalog URI (default file:///usr/share/opensearch/infino/data;
#                        s3://, gs:// or az:// also work)
#   INFINO_LOG_LEVEL     optional: logger level for org.opensearch.infino (e.g. DEBUG)
set -euo pipefail

INFINO_HOME=/usr/share/opensearch/infino
INFINO_STORAGE="${INFINO_STORAGE:-cloud}"
INFINO_DATA="$INFINO_HOME/data"

java_opts="-Dinfino.lib=$INFINO_HOME/lib/libinfino_capi.so"
extra_args=()

case "$INFINO_STORAGE" in
  cloud)
    if [ -z "${INFINO_API_KEY:-}" ]; then
      echo "infino: INFINO_STORAGE=cloud needs INFINO_API_KEY." >&2
      echo "infino: no account yet? Start once with INFINO_STORAGE=local and run" >&2
      echo "infino:   POST /_plugins/_infino/account/trial {\"accept\": true}" >&2
      echo "infino: then put the key it returns in INFINO_API_KEY." >&2
      exit 1
    fi
    extra_args+=("-Einfino.platform.url=${INFINO_PLATFORM_URL:-https://api.platform.infino.ws}")
    if [ -n "${INFINO_DATABASE:-}" ]; then
      extra_args+=("-Einfino.platform.database=$INFINO_DATABASE")
    fi
    echo "infino: storage=cloud url=${INFINO_PLATFORM_URL:-https://api.platform.infino.ws} database=${INFINO_DATABASE:-<cluster name>}"
    ;;
  local)
    # Keep the key out of the plugin's reach so a local run never talks to the service.
    unset INFINO_API_KEY
    uri="${INFINO_URI:-file://$INFINO_DATA}"
    java_opts="$java_opts -Dinfino.uri=$uri"
    if [[ "$uri" == file://* ]]; then
      # The plugin's security policy grants file access by this property.
      INFINO_DATA="${uri#file://}"
      mkdir -p "$INFINO_DATA"
      java_opts="$java_opts -Dinfino.data=$INFINO_DATA"
    fi
    echo "infino: storage=local uri=$uri"
    ;;
  memory)
    unset INFINO_API_KEY
    echo "infino: storage=memory (tables are lost on restart)"
    ;;
  *)
    echo "infino: INFINO_STORAGE must be cloud, local or memory (got '$INFINO_STORAGE')" >&2
    exit 1
    ;;
esac

if [ -n "${INFINO_LOG_LEVEL:-}" ]; then
  extra_args+=("-Elogger.org.opensearch.infino=$INFINO_LOG_LEVEL")
fi

export OPENSEARCH_JAVA_OPTS="$java_opts ${OPENSEARCH_JAVA_OPTS:-}"

# Same argument convention as the stock entrypoint: no args or flags -> opensearch.
if [ $# -eq 0 ] || [ "${1:0:1}" = '-' ]; then
  set -- opensearch "$@"
fi
if [ "$1" = "opensearch" ]; then
  exec /usr/share/opensearch/opensearch-docker-entrypoint.sh "$@" "${extra_args[@]}"
fi
exec "$@"
