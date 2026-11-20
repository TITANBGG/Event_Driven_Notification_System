#!/usr/bin/env bash
# WHAT: End-to-end smoke script executed against the running Docker Compose stack.
# WHY: Proves core HTTP health and provider contracts stay healthy after every phase change;
#      notification create/batch/cancel/retry steps are appended in Phase 3+.
# EXPECTED OUTPUT EXAMPLE:
#   [smoke] GET /health/live -> 200
#   [smoke] POST mockprovider / -> 202

set -euo pipefail

API_URL="${API_URL:-http://localhost:8080}"
MOCK_URL="${MOCK_URL:-http://localhost:8090}"

echo "[smoke] GET ${API_URL}/health/live"
curl -fsS -w "\nHTTP %{http_code}\n" "${API_URL}/health/live"

echo "[smoke] POST ${MOCK_URL}/"
curl -fsS -w "\nHTTP %{http_code}\n" -X POST "${MOCK_URL}/" \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: smoke-phase1-1" \
  -H "X-Correlation-ID: smoke-corr-1" \
  -d '{"to":"+905551234567","channel":"sms","content":"hi"}'

echo "[smoke] GET ${MOCK_URL}/stats"
curl -fsS "${MOCK_URL}/stats"
echo ""

