#!/usr/bin/env bash
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
