#!/usr/bin/env bash
set -euo pipefail
HOST="$1"
PORT="$2"
TIMEOUT="${3:-120}"
for _ in $(seq 1 "$TIMEOUT"); do
  if nc -z "$HOST" "$PORT" >/dev/null 2>&1; then
    exit 0
  fi
  sleep 1
done

echo "Timed out waiting for ${HOST}:${PORT}" >&2
exit 1
