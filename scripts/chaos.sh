#!/usr/bin/env bash
# Combined chaos under sustained load. A background generator submits one 1,000-item mixed-channel batch
# every 5 seconds (~200/s) for LOAD_SECONDS while a fixed timeline kills workers, restarts and flushes
# Redis, stops PostgreSQL and degrades the provider. Afterwards it waits until every accepted notification
# is terminal and checks: nothing lost or stuck, invariants (duplicates allowed only as at-least-once after
# worker kills), and the per-channel rate limit from both clocks.
#
#   ./scripts/chaos.sh                  # full run (~8 min of chaos + drain)
#   LOAD_SECONDS=240 ./scripts/chaos.sh # shorter load window (timeline events past it are skipped)
set -euo pipefail

API_URL="${API_URL:-http://localhost:8080}"
MOCK_URL="${MOCK_URL:-http://localhost:8090}"
read -r -a COMPOSE <<<"${COMPOSE:-docker compose}"
LOAD_SECONDS="${LOAD_SECONDS:-480}"
DRAIN_TIMEOUT="${DRAIN_TIMEOUT:-1200}"
BATCH_SIZE=1000
BATCH_EVERY=5
RUN_TAG="$(date +%s)"
WORK="$(mktemp -d)"
DIR="$(cd "$(dirname "$0")" && pwd)"

log() { printf '[%s] %s\n' "$(date +%H:%M:%S)" "$*"; }
# psql_q retries: the chaos itself (and, once, Docker Desktop running out of memory) can make a single
# `docker compose exec` fail, and with set -e that used to abort the run before its summary.
psql_q() {
  local out try
  for try in 1 2 3 4 5; do
    if out="$("${COMPOSE[@]}" exec -T postgres psql -U notif -d notif -qAt -F ' ' -c "$1" 2>/dev/null)"; then
      printf '%s\n' "$out" | tr -d '\r'
      return 0
    fi
    log "psql attempt ${try} failed; retrying" >&2
    sleep $((try * 2))
  done
  return 1
}
mock_config() { curl -s --max-time 10 -X POST "${MOCK_URL}/config" -H 'Content-Type: application/json' -d "$1" >/dev/null || true; }

# shellcheck disable=SC2317 # invoked by the EXIT trap
cleanup() {
  local rc=$?
  if [[ -n "${LOAD_PID:-}" ]]; then kill "$LOAD_PID" 2>/dev/null || true; fi
  if [[ -n "${WATCH_PID:-}" ]]; then kill "$WATCH_PID" 2>/dev/null || true; fi
  mock_config '{"latencyMs":0,"latencyJitterMs":0,"failureRate":0,"rate429":0,"permanentRate":0}'
  "${COMPOSE[@]}" start postgres redis worker-1 worker-2 >/dev/null 2>&1 || true
  # Keep the submissions log and metrics of a failed run for diagnosis.
  if ((rc == 0)); then rm -rf "$WORK"; else echo "run artifacts kept in ${WORK} (exit ${rc})" >&2; fi
}
trap cleanup EXIT

# batch_body <seq>: 1,000 items cycling sms / email / push, priorities 20/60/20.
batch_body() {
  local s="$1" i items="" ch rcpt pr
  for ((i = 0; i < BATCH_SIZE; i++)); do
    case $((i % 3)) in
      0) ch=sms rcpt="+90555$(printf '%07d' $(((s * BATCH_SIZE + i) % 10000000)))" ;;
      1) ch=email rcpt="chaos${RUN_TAG}.${s}.${i}@example.com" ;;
      2) ch=push rcpt="chaos-token-${RUN_TAG}-${s}-${i}" ;;
    esac
    case $((i % 5)) in 0) pr=high ;; 4) pr=low ;; *) pr=normal ;; esac
    items+="${items:+,}{\"recipient\":\"${rcpt}\",\"channel\":\"${ch}\",\"priority\":\"${pr}\",\"content\":\"chaos ${RUN_TAG} ${s}-${i}\"}"
  done
  printf '{"notifications":[%s]}' "$items"
}

# load_loop: one batch every BATCH_EVERY seconds; records "<seq> <http_code> <batchId>" per submission.
load_loop() {
  local s=0 start next code out
  start=$(date +%s)
  while (($(date +%s) - start < LOAD_SECONDS)); do
    next=$((start + (s + 1) * BATCH_EVERY))
    out="$(batch_body "$s" | curl -s --max-time 30 -w '\n%{http_code}' -X POST "${API_URL}/api/v1/notifications/batch" \
      -H 'Content-Type: application/json' -H "Idempotency-Key: chaos-${RUN_TAG}-${s}" --data-binary @- || true)"
    code="$(printf '%s' "$out" | tail -n1)"
    echo "$s ${code:-000} $(printf '%s' "$out" | grep -oE '"batchId":"[^"]+"' | cut -d'"' -f4)" >> "$WORK/submissions.txt"
    s=$((s + 1))
    while (($(date +%s) < next)); do sleep 0.2; done
  done
}

at() { # at <second> <description> <command...>: run when the load has been going for <second> seconds
  local t="$1" what="$2"
  shift 2
  ((t >= LOAD_SECONDS)) && { log "skip t=${t}s ${what} (past LOAD_SECONDS)"; return; }
  while (($(date +%s) - T0 < t)); do sleep 0.5; done
  log "t=${t}s ${what}"
  "$@" >/dev/null 2>&1 || log "  (command failed: $*)"
}

# ---------------------------------------------------------------------------------------------

if [[ "$(curl -s -o /dev/null -w '%{http_code}' "${API_URL}/health/ready")" != "200" ]]; then
  echo "stack is not ready at ${API_URL}" >&2
  exit 2
fi
if ! curl -fs -X POST "${MOCK_URL}/config" -H 'Content-Type: application/json' -d '{}' >/dev/null; then
  echo "mock provider has no POST /config; rebuild it (docker compose up -d --build mockprovider)" >&2
  exit 2
fi
mock_config '{"latencyMs":0,"latencyJitterMs":0,"failureRate":0,"rate429":0,"permanentRate":0}'
curl -s -X POST "${MOCK_URL}/reset" >/dev/null
SINCE="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
log "chaos run ${RUN_TAG}: ${LOAD_SECONDS}s of load (${BATCH_SIZE} every ${BATCH_EVERY}s), since=${SINCE}"

"$DIR/prom-snapshot.sh" watch 10 "$WORK/metrics.csv" >/dev/null 2>&1 &
WATCH_PID=$!
T0=$(date +%s)
load_loop &
LOAD_PID=$!

at 30 "SIGKILL worker-1" "${COMPOSE[@]}" kill worker-1
at 45 "start worker-1" "${COMPOSE[@]}" start worker-1
at 60 "SIGKILL worker-2" "${COMPOSE[@]}" kill worker-2
at 75 "start worker-2" "${COMPOSE[@]}" start worker-2
at 90 "restart Redis" "${COMPOSE[@]}" restart redis
at 120 "Redis FLUSHALL" "${COMPOSE[@]}" exec -T redis redis-cli FLUSHALL
at 150 "stop PostgreSQL" "${COMPOSE[@]}" stop postgres
at 170 "start PostgreSQL" "${COMPOSE[@]}" start postgres
at 200 "provider slow (2s +- 0.5s)" mock_config '{"latencyMs":2000,"latencyJitterMs":500}'
at 240 "provider 30% 500s" mock_config '{"latencyMs":0,"latencyJitterMs":0,"failureRate":0.3}'
at 280 "provider 50% 429s" mock_config '{"failureRate":0,"rate429":0.5}'
at 300 "SIGKILL worker-1 during the 429 storm" "${COMPOSE[@]}" kill worker-1
at 315 "start worker-1" "${COMPOSE[@]}" start worker-1
at 360 "provider healthy" mock_config '{"rate429":0}'

wait "$LOAD_PID" || true
LOAD_PID=""
log "load finished; waiting for every accepted notification to become terminal (timeout ${DRAIN_TIMEOUT}s)"

accepted_batches="$(awk '$2 == 202 {print $3}' "$WORK/submissions.txt" | sort -u)"
accepted="$(($(printf '%s\n' "$accepted_batches" | grep -c . || true) * BATCH_SIZE))"
rejected="$(awk '$2 != 202' "$WORK/submissions.txt" | wc -l | tr -d ' ')"
# One UUID per line; build a SQL IN list ('a','b',...). UUIDs contain no quotes or spaces.
batch_list="$(printf '%s\n' "$accepted_batches" | awk -v q="'" 'NF {printf "%s%s%s%s", (n++ ? "," : ""), q, $1, q}')"
drain_start=$(date +%s)
while :; do
  open="$(psql_q "SELECT COUNT(*) FROM notifications WHERE batch_id IN (${batch_list}) AND status NOT IN ('sent','failed','cancelled');")"
  [[ "$open" == "0" ]] && break
  if (($(date +%s) - drain_start > DRAIN_TIMEOUT)); then log "drain timeout with ${open} open"; break; fi
  sleep 5
done
drain_s=$(($(date +%s) - drain_start))
kill "$WATCH_PID" 2>/dev/null || true
WATCH_PID=""

read -r rows sent failed open < <(psql_q "SELECT COUNT(*), COUNT(*) FILTER (WHERE status='sent'), COUNT(*) FILTER (WHERE status='failed'),
  COUNT(*) FILTER (WHERE status NOT IN ('sent','failed','cancelled')) FROM notifications WHERE batch_id IN (${batch_list});")
reasons="$(psql_q "SELECT COALESCE(last_error,'?') || ' x' || n FROM (SELECT last_error, COUNT(*) AS n FROM notifications
  WHERE batch_id IN (${batch_list}) AND status='failed' GROUP BY last_error ORDER BY n DESC LIMIT 5) r;" | paste -sd ';' -)"

echo
echo "=== chaos summary (since ${SINCE}) ==="
echo "submissions: $(wc -l < "$WORK/submissions.txt" | tr -d ' ') batches, ${rejected} rejected (e.g. 503 while PostgreSQL was down), ${accepted} notifications accepted"
echo "in PostgreSQL: rows=${rows} sent=${sent} failed=${failed} open=${open}; drained ${drain_s}s after the load ended"
[[ "$failed" != "0" ]] && echo "failed reasons: ${reasons}"
echo "peaks: $(awk -F, 'NR>1 {if ($11+0>q) q=$11; if ($21+0>r) r=$21; if ($19+0>c) c=$19} END {printf "queue_ready=%d redis_bytes=%d pg_conns=%d", q, r, c}' "$WORK/metrics.csv" 2>/dev/null || echo n/a)"
echo
status=0
if [[ "$rows" != "$accepted" || "$open" != "0" ]]; then
  echo "FAIL: accepted=${accepted} but rows=${rows}, open=${open}"
  status=1
fi
"$DIR/invariants.sh" --since="$SINCE" --allow-duplicates || status=1
echo
"$DIR/ratecheck.sh" --since="$SINCE" --channels=3 || status=1
exit "$status"
