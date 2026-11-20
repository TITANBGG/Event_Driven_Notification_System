#!/usr/bin/env bash
set -euo pipefail

API_URL="${API_URL:-http://localhost:8080}"
MOCK_URL="${MOCK_URL:-http://localhost:8090}"
RUN_TAG="$(date +%s)-$$"

run_req() {
  local label="$1"
  shift
  echo "=== ${label} ==="
  "$@" -i -sS -w "\n--- HTTP %{http_code} ---\n\n"
}

run_req "GET /health/live" \
  curl "${API_URL}/health/live"

run_req "GET /openapi.yaml (headers + status check)" \
  curl -o /dev/null -D - "${API_URL}/openapi.yaml"

run_req "POST /api/v1/notifications (Turkish SMS, high priority)" \
  curl -X POST "${API_URL}/api/v1/notifications" \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: smoke-single-${RUN_TAG}" \
  -H "X-Correlation-ID: smoke-corr-${RUN_TAG}" \
  -d '{"recipient":"+905551234567","channel":"sms","content":"Siparişiniz kargoya verildi","priority":"high"}'

run_req "POST /api/v1/notifications (idempotent replay)" \
  curl -X POST "${API_URL}/api/v1/notifications" \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: smoke-single-${RUN_TAG}" \
  -H "X-Correlation-ID: smoke-replay-${RUN_TAG}" \
  -d '{"priority":"high","content":"Siparişiniz kargoya verildi","channel":"sms","recipient":"+905551234567"}'

SINGLE_JSON="$(curl -fsS -X POST "${API_URL}/api/v1/notifications" \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: smoke-cancel-${RUN_TAG}" \
  -H "X-Correlation-ID: smoke-cancel-${RUN_TAG}" \
  -d '{"recipient":"+905551234568","channel":"sms","content":"Smoke cancel target"}')"
SINGLE_ID="$(printf '%s' "${SINGLE_JSON}" | sed -n 's/.*"id":"\([^"]*\)".*/\1/p')"
echo "Created notification id=${SINGLE_ID}"

run_req "GET /api/v1/notifications/${SINGLE_ID}" \
  curl "${API_URL}/api/v1/notifications/${SINGLE_ID}"

run_req "GET /api/v1/notifications?status=pending&limit=5" \
  curl "${API_URL}/api/v1/notifications?status=pending&limit=5"

run_req "POST /api/v1/notifications/${SINGLE_ID}/cancel" \
  curl -X POST "${API_URL}/api/v1/notifications/${SINGLE_ID}/cancel"

run_req "POST /api/v1/notifications/${SINGLE_ID}/retry (expect 409 because cancelled)" \
  curl -X POST "${API_URL}/api/v1/notifications/${SINGLE_ID}/retry" || true

BATCH_JSON="$(curl -fsS -X POST "${API_URL}/api/v1/notifications/batch" \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: smoke-batch-${RUN_TAG}" \
  -H "X-Correlation-ID: smoke-batch-${RUN_TAG}" \
  -d '{"notifications":[{"recipient":"+905551234567","channel":"sms","content":"Batch item 1"},{"recipient":"smoke@example.com","channel":"email","subject":"Smoke","content":"Batch item 2"}]}')"
BATCH_ID="$(printf '%s' "${BATCH_JSON}" | sed -n 's/.*"batchId":"\([^"]*\)".*/\1/p')"
echo "Created batch batchId=${BATCH_ID}"

run_req "GET /api/v1/batches/${BATCH_ID}" \
  curl "${API_URL}/api/v1/batches/${BATCH_ID}"

run_req "POST /api/v1/batches/${BATCH_ID}/cancel" \
  curl -X POST "${API_URL}/api/v1/batches/${BATCH_ID}/cancel"

run_req "POST ${MOCK_URL}/ (mockprovider EXT-1 sanity)" \
  curl -X POST "${MOCK_URL}/" \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: smoke-mock-${RUN_TAG}" \
  -H "X-Correlation-ID: smoke-mock-${RUN_TAG}" \
  -d '{"to":"+905551234567","channel":"sms","content":"hi"}'

run_req "GET ${MOCK_URL}/stats" \
  curl "${MOCK_URL}/stats"
