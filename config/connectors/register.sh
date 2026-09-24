#!/bin/sh
# Register (or update) the Debezium connector from its config file, then wait until it and its
# task report RUNNING. PUT /connectors/<name>/config is idempotent: it creates the connector
# the first time and replaces its config afterwards, so this runs on every `up`.
set -eu

CONNECT=http://connect:8083
NAME=olist-postgres

curl -fsS -X PUT -H 'Content-Type: application/json' \
  --data @/config/olist-postgres.json \
  "$CONNECT/connectors/$NAME/config" >/dev/null
echo "register: $NAME config applied"

i=0
while [ $i -lt 60 ]; do
  status=$(curl -fsS "$CONNECT/connectors/$NAME/status" || true)
  running=$(printf '%s' "$status" | grep -o '"state":"RUNNING"' | wc -l)
  if [ "$running" -ge 2 ]; then   # the connector itself and its one task
    echo "register: $NAME and its task are RUNNING"
    exit 0
  fi
  if printf '%s' "$status" | grep -q '"state":"FAILED"'; then
    echo "register: CONNECT-FAILED $status" >&2
    exit 1
  fi
  i=$((i + 1))
  sleep 2
done
echo "register: CONNECT-TIMEOUT $NAME not RUNNING after 120 s: $status" >&2
exit 1
