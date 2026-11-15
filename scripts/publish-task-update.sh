#!/usr/bin/env sh
# Publish a task update straight onto the broker, bypassing the worker, through
# RabbitMQ's own management HTTP API. Use it to reproduce duplicates, stale /
# out-of-order updates and poison messages by hand.
#
#   scripts/publish-task-update.sh <task_id> <in_progress|done|failed> [message_id]
#   scripts/publish-task-update.sh --raw '<any body, e.g. not json>'
#
# Re-using a message_id re-sends the *same* update (duplicate delivery).
set -eu

cd "$(dirname "$0")/.."
[ -f .env ] || { echo "no .env: run 'make up' first" >&2; exit 1; }
set -a; . ./.env; set +a

json_escape() { printf '%s' "$1" | sed -e 's/\\/\\\\/g' -e 's/"/\\"/g'; }

if [ "${1:-}" = "--raw" ]; then
  [ $# -eq 2 ] || { echo "usage: $0 --raw '<body>'" >&2; exit 64; }
  ROUTING_KEY="task_update.raw"
  MESSAGE_ID="raw"
  PAYLOAD=$2
else
  [ $# -ge 2 ] || { sed -n '6,7p' "$0" >&2; exit 64; }
  TASK_ID=$1
  STATUS=$2
  MESSAGE_ID=${3:-$(python3 -c 'import uuid; print(uuid.uuid4())' 2>/dev/null || uuidgen | tr 'A-Z' 'a-z')}
  ROUTING_KEY="task_update.$STATUS"
  NOW=$(date -u +%Y-%m-%dT%H:%M:%S.000Z)
  PAYLOAD=$(printf '{"message_id":"%s","task_id":"%s","status":"%s","occurred_at":"%s"}' \
    "$MESSAGE_ID" "$TASK_ID" "$STATUS" "$NOW")
fi

BODY=$(printf '{"properties":{"message_id":"%s","delivery_mode":2,"content_type":"application/json"},"routing_key":"%s","payload":"%s","payload_encoding":"string"}' \
  "$MESSAGE_ID" "$ROUTING_KEY" "$(json_escape "$PAYLOAD")")

echo "publishing: $PAYLOAD"
curl -fsS -u "$RABBITMQ_USER:$RABBITMQ_PASSWORD" -H 'content-type: application/json' \
  -X POST "http://localhost:${RABBITMQ_MANAGEMENT_PORT:-15672}/api/exchanges/%2F/control_plane.task_updates/publish" \
  -d "$BODY"
echo
