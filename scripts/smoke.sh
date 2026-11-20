#!/usr/bin/env bash
set -euo pipefail

API_URL="${API_URL:-http://localhost:8080}"
MOCK_URL="${MOCK_URL:-http://localhost:8090}"
RUN_TAG="$(date +%s)-$$"

run_req() {
  local label="$1"
  local expected_pattern="$2"
  shift 2
  echo "=== ${label} (expect HTTP ${expected_pattern}) ==="
  local tmp_out
  tmp_out="$(mktemp)"
  local code
  code="$("$@" -i -sS -o "${tmp_out}" -w "%{http_code}")"
  cat "${tmp_out}"
  rm -f "${tmp_out}"
  echo -e "\n--- HTTP ${code} ---\n"
  if ! [[ "${code}" =~ ^(${expected_pattern})$ ]]; then
    echo "ERROR: ${label} returned HTTP ${code}, expected ${expected_pattern}" >&2
    exit 1
  fi
}

run_req "GET /health/live" "200" \
  curl "${API_URL}/health/live"

run_req "GET /health/ready" "200" \
  curl "${API_URL}/health/ready"

run_req "GET /health (alias for /health/ready)" "200" \
  curl "${API_URL}/health"

run_req "GET /metrics (Prometheus scrape endpoint)" "200" \
  curl "${API_URL}/metrics"

run_req "GET /openapi.yaml (OpenAPI specification)" "200" \
  curl "${API_URL}/openapi.yaml"

run_req "POST /api/v1/notifications (Turkish SMS, high priority)" "202" \
  curl -X POST "${API_URL}/api/v1/notifications" \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: smoke-single-${RUN_TAG}" \
  -H "X-Correlation-ID: smoke-corr-${RUN_TAG}" \
  -d '{"recipient":"+905551234567","channel":"sms","content":"Sipari\u015finiz kargoya verildi","priority":"high"}'

run_req "POST /api/v1/notifications (idempotent replay)" "202" \
  curl -X POST "${API_URL}/api/v1/notifications" \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: smoke-single-${RUN_TAG}" \
  -H "X-Correlation-ID: smoke-replay-${RUN_TAG}" \
  -d '{"priority":"high","content":"Sipari\u015finiz kargoya verildi","channel":"sms","recipient":"+905551234567"}'

SINGLE_JSON="$(curl -fsS -X POST "${API_URL}/api/v1/notifications" \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: smoke-cancel-${RUN_TAG}" \
  -H "X-Correlation-ID: smoke-cancel-${RUN_TAG}" \
  -d '{"recipient":"+905551234568","channel":"sms","content":"Smoke lookup and cancel target"}')"
SINGLE_ID="$(printf '%s' "${SINGLE_JSON}" | sed -n 's/.*"id":"\([^"]*\)".*/\1/p')"
echo "Created notification id=${SINGLE_ID}"

run_req "GET /api/v1/notifications/${SINGLE_ID}" "200" \
  curl "${API_URL}/api/v1/notifications/${SINGLE_ID}"

run_req "GET /api/v1/notifications?limit=5" "200" \
  curl "${API_URL}/api/v1/notifications?limit=5"

run_req "POST /api/v1/notifications/${SINGLE_ID}/cancel (200 if queued or 409 if already sent)" "200|409" \
  curl -X POST "${API_URL}/api/v1/notifications/${SINGLE_ID}/cancel"

run_req "POST /api/v1/notifications/${SINGLE_ID}/retry (expect 409)" "409" \
  curl -X POST "${API_URL}/api/v1/notifications/${SINGLE_ID}/retry"

echo "=== POST /api/v1/notifications/batch (10 mixed-channel notifications -> verify end-to-end delivery) ==="
E2E_BATCH_JSON="$(curl -fsS -X POST "${API_URL}/api/v1/notifications/batch" \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: smoke-e2e-batch-${RUN_TAG}" \
  -H "X-Correlation-ID: smoke-e2e-${RUN_TAG}" \
  -d '{"notifications":[
    {"recipient":"+905551234501","channel":"sms","priority":"high","content":"Smoke 1"},
    {"recipient":"+905551234502","channel":"sms","priority":"normal","content":"Smoke 2"},
    {"recipient":"+905551234503","channel":"sms","priority":"low","content":"Smoke 3"},
    {"recipient":"+905551234504","channel":"sms","priority":"high","content":"Sipari\u015finiz haz\u0131rlan\u0131yor"},
    {"recipient":"u1@example.com","channel":"email","priority":"high","subject":"Subj 1","content":"Email 1"},
    {"recipient":"u2@example.com","channel":"email","priority":"normal","subject":"Subj 2","content":"Email 2"},
    {"recipient":"u3@example.com","channel":"email","priority":"low","content":"Email 3"},
    {"recipient":"tok_smoke_1","channel":"push","priority":"high","title":"Push 1","content":"Body 1"},
    {"recipient":"tok_smoke_2","channel":"push","priority":"normal","content":"Body 2"},
    {"recipient":"tok_smoke_3","channel":"push","priority":"low","content":"Body 3"}
  ]}')"
E2E_BATCH_ID="$(printf '%s' "${E2E_BATCH_JSON}" | sed -n 's/.*"batchId":"\([^"]*\)".*/\1/p')"
echo "Created 10-item batchId=${E2E_BATCH_ID}"

ALL_SENT=0
for _ in $(seq 1 40); do
  SUMMARY_JSON="$(curl -fsS "${API_URL}/api/v1/batches/${E2E_BATCH_ID}")"
  if printf '%s' "${SUMMARY_JSON}" | grep -q '"countsByStatus":{"sent":10}'; then
    ALL_SENT=1
    break
  fi
  sleep 0.25
done
if [ "${ALL_SENT}" -ne 1 ]; then
  echo "ERROR: batch ${E2E_BATCH_ID} did not reach sent=10 within 10s"
  curl -sS "${API_URL}/api/v1/batches/${E2E_BATCH_ID}"
  echo ""
  exit 1
fi

run_req "GET /api/v1/batches/${E2E_BATCH_ID} (all 10 sent)" "200" \
  curl "${API_URL}/api/v1/batches/${E2E_BATCH_ID}"

run_req "POST /api/v1/batches/${E2E_BATCH_ID}/cancel (after completion: cancelled=0, notCancellable=10)" "200" \
  curl -X POST "${API_URL}/api/v1/batches/${E2E_BATCH_ID}/cancel"

run_req "GET /api/v1/metrics (real-time JSON observability snapshot)" "200" \
  curl "${API_URL}/api/v1/metrics"

run_req "GET ${MOCK_URL}/stats" "200" \
  curl "${MOCK_URL}/stats"
