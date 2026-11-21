#!/usr/bin/env bash
# Step 7: WebSocket flood (MAX_WS_CLIENTS=1000, slow-consumer backpressure, batch snapshot/stream)
# plus live HTTP/SMS/Email/Push/Schedule/Idempotency boundary tests, verified by invariants & ratecheck.
set -euo pipefail

API_URL="${API_URL:-http://localhost:8080}"
MOCK_URL="${MOCK_URL:-http://localhost:8090}"
DIR="$(cd "$(dirname "$0")" && pwd)"

if [[ "$(curl -s -o /dev/null -w '%{http_code}' "${API_URL}/health/ready")" != "200" ]]; then
  echo "stack is not ready at ${API_URL}; start it with 'docker compose up -d' first" >&2
  exit 2
fi

curl -s -X POST "${MOCK_URL}/reset" >/dev/null
SINCE="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
echo "=== Step 7 started at ${SINCE} ==="

go run ./cmd/wsflood -url "${API_URL}" "$@"

# Wait briefly for any boundary notifications (1000-item boundary batch + singles) to reach terminal state.
echo "=== Waiting for all Step 7 notifications to settle ==="
if [[ -n "${COMPOSE:-}" ]]; then
  read -r -a COMPOSE_CMD <<<"${COMPOSE}"
elif docker compose version >/dev/null 2>&1; then
  COMPOSE_CMD=(docker compose)
else
  COMPOSE_CMD=(docker-compose)
fi

deadline=$(($(date +%s) + 180))
while (($(date +%s) < deadline)); do
  active="$("${COMPOSE_CMD[@]}" exec -T postgres psql -U notif -d notif -Atc \
    "SELECT COUNT(*) FROM notifications WHERE status IN ('pending','processing','retrying');" | tr -d '\r' | head -n1)"
  [[ "${active:-1}" == "0" ]] && break
  sleep 2
done

echo
"$DIR/prom-snapshot.sh" once
echo
"$DIR/invariants.sh" --since="$SINCE"
echo
"$DIR/ratecheck.sh" --since="$SINCE" --channels=1

