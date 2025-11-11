#!/usr/bin/env bash

set -euo pipefail

HOST="${REDIS_HOST:-localhost}"
PORT="${REDIS_PORT:-6379}"
STREAM_KEY="${REDIS_STREAM_KEY:-data-stream-1}"
COUNT="${MESSAGE_COUNT:-10}"

echo "Producing ${COUNT} test messages to stream '${STREAM_KEY}'..."
for i in $(seq 1 "${COUNT}"); do
    timestamp=$(date -u +%Y-%m-%dT%H:%M:%SZ)
    payload=$(printf '{"id":%d,"message":"Test message %d","timestamp":"%s"}' "$i" "$i" "$timestamp")
    redis-cli -h "${HOST}" -p "${PORT}" XADD "${STREAM_KEY}" '*' value "${payload}"
    sleep 0.1
done
echo 'Done producing messages'