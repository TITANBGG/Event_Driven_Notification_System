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

run_req "GET /demo (interactive WebSocket demo page)" "200" \
  curl "${API_URL}/demo"

run_req "GET /api/v1/ws (without filter -> expect HTTP 400 missing_ws_filter)" "400" \
  curl "${API_URL}/api/v1/ws"

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

echo "=== Scheduled notification (B-2): schedule 30s ahead -> verify status=scheduled -> cancel before due ==="
SCHED_AT="$(date -u -d '+30 seconds' '+%Y-%m-%dT%H:%M:%SZ' 2>/dev/null || date -u -v+30S '+%Y-%m-%dT%H:%M:%SZ')"
SCHED_JSON="$(curl -fsS -X POST "${API_URL}/api/v1/notifications" \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: smoke-sched-30s-${RUN_TAG}" \
  -d "{\"recipient\":\"+905551234599\",\"channel\":\"sms\",\"priority\":\"high\",\"content\":\"Scheduled 30s ahead\",\"scheduledAt\":\"${SCHED_AT}\"}")"
SCHED_ID="$(printf '%s' "${SCHED_JSON}" | sed -n 's/.*"id":"\([^"]*\)".*/\1/p')"
echo "Scheduled 30s notification id=${SCHED_ID} scheduledAt=${SCHED_AT}"

run_req "GET /api/v1/notifications?status=scheduled&limit=5" "200" \
  curl "${API_URL}/api/v1/notifications?status=scheduled&limit=5"

run_req "POST /api/v1/notifications/${SCHED_ID}/cancel (cancel scheduled notification before due)" "200" \
  curl -X POST "${API_URL}/api/v1/notifications/${SCHED_ID}/cancel"

echo "=== Template system (B-3): create template -> preview -> batch of 3 personalized SMS ==="
# Non-ASCII bodies go through stdin: native Windows curl re-encodes argv with the ANSI code page,
# which the API (correctly) rejects as invalid UTF-8.
TMPL_JSON="$(curl -fsS -X POST "${API_URL}/api/v1/templates" \
  -H "Content-Type: application/json" \
  --data-binary @- <<JSON
{"name":"otp-tr-${RUN_TAG}","channel":"sms","body":"Merhaba {{name}}, doğrulama kodunuz: {{code}}","variables":["name","code"]}
JSON
)"
TMPL_ID="$(printf '%s' "${TMPL_JSON}" | sed -n 's/.*"id":"\([^"]*\)".*/\1/p')"
echo "Created template id=${TMPL_ID}"

run_req "POST /api/v1/templates/${TMPL_ID}/preview" "200" \
  curl -X POST "${API_URL}/api/v1/templates/${TMPL_ID}/preview" \
  -H "Content-Type: application/json" \
  --data-binary @- <<'JSON'
{"variables":{"name":"Ayşe","code":"4821","unusedVar":"extra"}}
JSON

TMPL_BATCH_JSON="$(curl -fsS -X POST "${API_URL}/api/v1/notifications/batch" \
  -H "Content-Type: application/json" \
  --data-binary @- <<JSON
{"notifications":[
  {"recipient":"+905551111111","channel":"sms","templateId":"${TMPL_ID}","templateVariables":{"name":"Ayşe","code":"4821"}},
  {"recipient":"+905552222222","channel":"sms","templateId":"${TMPL_ID}","templateVariables":{"name":"Mehmet","code":"9035"}},
  {"recipient":"+905553333333","channel":"sms","templateId":"${TMPL_ID}","templateVariables":{"name":"Zeynep","code":7142}}
]}
JSON
)"
TMPL_BATCH_ID="$(printf '%s' "${TMPL_BATCH_JSON}" | sed -n 's/.*"batchId":"\([^"]*\)".*/\1/p')"
echo "Created template batchId=${TMPL_BATCH_ID}"

TMPL_ALL_SENT=0
for _ in $(seq 1 40); do
  TSUM_JSON="$(curl -fsS "${API_URL}/api/v1/batches/${TMPL_BATCH_ID}")"
  if printf '%s' "${TSUM_JSON}" | grep -q '"countsByStatus":{"sent":3}'; then
    TMPL_ALL_SENT=1
    break
  fi
  sleep 0.25
done
if [ "${TMPL_ALL_SENT}" -ne 1 ]; then
  echo "ERROR: template batch ${TMPL_BATCH_ID} did not reach sent=3 within 10s" >&2
  exit 1
fi

run_req "GET /api/v1/batches/${TMPL_BATCH_ID} (all 3 personalized template SMS sent)" "200" \
  curl "${API_URL}/api/v1/batches/${TMPL_BATCH_ID}"

run_req "GET /api/v1/metrics (real-time JSON observability snapshot)" "200" \
  curl "${API_URL}/api/v1/metrics"

run_req "GET ${MOCK_URL}/stats" "200" \
  curl "${MOCK_URL}/stats"
