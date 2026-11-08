#!/bin/bash
# Maps the INFINO_* environment onto the plugin's JVM properties and node
# settings, then hands off to the stock OpenSearch entrypoint (which still
# handles OPENSEARCH_INITIAL_ADMIN_PASSWORD, the demo security config and
# dotted env vars such as cluster.name).
#
#   INFINO_STORAGE       cloud (default) | local | memory
#   INFINO_API_KEY       cloud: the account key (read by the plugin from the env)
#   INFINO_PLATFORM_URL  the hosted service (default https://api.platform.infino.ws);
#                        in local/memory, where a trial account is asked from
#   INFINO_DATABASE      cloud: the account's database (default: the cluster name)
#   INFINO_URI           local: catalog URI (default file:///usr/share/opensearch/infino/data;
#                        s3://, gs:// or az:// also work)
#   INFINO_LOG_LEVEL     optional: logger level for org.opensearch.infino (e.g. DEBUG)
set -euo pipefail

INFINO_HOME=/usr/share/opensearch/infino
INFINO_STORAGE="${INFINO_STORAGE:-cloud}"
INFINO_DATA="$INFINO_HOME/data"
PLATFORM_URL="${INFINO_PLATFORM_URL:-https://api.platform.infino.ws}"
# Records which storage this node's data volume belongs to (see below).
STORAGE_MARKER=/usr/share/opensearch/data/.infino-storage

java_opts="-Dinfino.lib=$INFINO_HOME/lib/libinfino_capi.so"
extra_args=()

case "$INFINO_STORAGE" in
  cloud)
    if [ -z "${INFINO_API_KEY:-}" ]; then
      echo "infino: INFINO_STORAGE=cloud needs INFINO_API_KEY." >&2
      echo "infino: no account yet? Start once with INFINO_STORAGE=local and run" >&2
      echo "infino:   POST /_plugins/_infino/account/trial {\"accept\": true}" >&2
      echo "infino: then put the key it returns in INFINO_API_KEY, and set INFINO_DATABASE" >&2
      echo "infino: to the database it returns if that is not the cluster name." >&2
      echo "infino: Run 'docker compose down -v' before starting again with INFINO_STORAGE=cloud." >&2
      exit 1
    fi
    extra_args+=("-Einfino.platform.url=$PLATFORM_URL")
    if [ -n "${INFINO_DATABASE:-}" ]; then
      extra_args+=("-Einfino.platform.database=$INFINO_DATABASE")
    fi
    storage="cloud $PLATFORM_URL ${INFINO_DATABASE:-cluster:$(printenv cluster.name || true)}"
    echo "infino: storage=cloud url=$PLATFORM_URL database=${INFINO_DATABASE:-<cluster name>}"
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
    storage="local $uri"
    echo "infino: storage=local uri=$uri"
    ;;
  memory)
    unset INFINO_API_KEY
    storage="memory"
    echo "infino: storage=memory (tables are lost on restart; use 'docker compose down -v' between runs)"
    ;;
  *)
    echo "infino: INFINO_STORAGE must be cloud, local or memory (got '$INFINO_STORAGE')" >&2
    exit 1
    ;;
esac

# Without an account, a trial is asked of the same service cloud mode would use.
if [ "$INFINO_STORAGE" != cloud ] && [ -n "${INFINO_PLATFORM_URL:-}" ]; then
  extra_args+=("-Einfino.platform.default_url=$INFINO_PLATFORM_URL")
fi

# The data volume holds cluster state naming indices whose tables live in the
# storage it was first used with. Started over different storage, those
# indices would point at tables that are not there, so refuse instead.
if [ -f "$STORAGE_MARKER" ]; then
  previous="$(cat "$STORAGE_MARKER")"
  if [ "$previous" != "$storage" ]; then
    echo "infino: this node's data volume was used with storage [$previous]," >&2
    echo "infino: but it is starting with [$storage]. Its indices' tables are not there." >&2
    echo "infino: Run 'docker compose down -v' to start clean (this deletes the local volumes)," >&2
    echo "infino: or switch the INFINO_* settings back." >&2
    exit 1
  fi
else
  printf '%s' "$storage" > "$STORAGE_MARKER"
fi

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
