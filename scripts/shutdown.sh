#!/usr/bin/env bash
# Graceful shutdown under load. While batches stream in and ~50 sends are in flight (provider latency
# 300 +- 200ms), SIGTERM is sent to worker-1, then worker-2, then both at once, then the API. Unlike a
# SIGKILL, a graceful stop must leave nothing for the reconciler to rescue:
#   - every stop finishes within SHUTDOWN_TIMEOUT (15s) and logs a clean stop
#   - the stopped worker's heartbeat key and inflight list are gone
#   - no 'abandoned' attempt and no duplicate delivery (invariants without --allow-duplicates)
#   - every batch the API accepted (202) is complete (all-or-nothing) despite the API restart
#
#   ./scripts/shutdown.sh             the four stops above (~4 min)
#   ./scripts/shutdown.sh --rolling   rolling restarts: worker-1 and worker-2 alternately get
#                                     `compose restart` every 5s for 2 minutes under the same load; every
#                                     restart must drain, and the run must end with 0 abandoned attempts,
#                                     0 duplicates and no "lease lost" log line (~5 min)
# shellcheck disable=SC2317,SC2329 # functions run indirectly through `at` and the EXIT trap
set -euo pipefail

API_URL="${API_URL:-http://localhost:8080}"
MOCK_URL="${MOCK_URL:-http://localhost:8090}"
read -r -a COMPOSE <<<"${COMPOSE:-docker compose}"
MODE="stops"
for arg in "$@"; do
  case "$arg" in
    --rolling) MODE="rolling" ;;
    *) echo "usage: $0 [--rolling]" >&2; exit 2 ;;
  esac
done
ROLLING_SECONDS=120
ROLLING_EVERY=5
LOAD_SECONDS=100
if [[ "$MODE" == rolling ]]; then LOAD_SECONDS=$((ROLLING_SECONDS + 10)); fi
BATCH_SIZE=1000
BATCH_EVERY=2
STOP_LIMIT_S=16
RUN_TAG="$(date +%s)"
WORK="$(mktemp -d)"
DIR="$(cd "$(dirname "$0")" && pwd)"
FAILURES=0
RESULTS=()

log() { printf '[%s] %s\n' "$(date +%H:%M:%S)" "$*"; }
record() {
  RESULTS+=("$1|$2|$3")
  if [[ "$2" == "FAIL" ]]; then FAILURES=$((FAILURES + 1)); fi
  log "$1: $2 - $3"
}
psql_q() { "${COMPOSE[@]}" exec -T postgres psql -U notif -d notif -qAt -F ' ' -c "$1" | tr -d '\r'; }
redis_q() { "${COMPOSE[@]}" exec -T redis redis-cli --raw "$@" | tr -d '\r'; }
mock_config() { curl -s --max-time 10 -X POST "${MOCK_URL}/config" -H 'Content-Type: application/json' -d "$1" >/dev/null || true; }

# shellcheck disable=SC2317,SC2329 # invoked by the EXIT trap
cleanup() {
  if [[ -n "${LOAD_PID:-}" ]]; then kill "$LOAD_PID" 2>/dev/null || true; fi
  mock_config '{"latencyMs":0,"latencyJitterMs":0}'
  "${COMPOSE[@]}" start api worker-1 worker-2 >/dev/null 2>&1 || true
  rm -rf "$WORK"
}
trap cleanup EXIT

# batch_body <seq>: built in awk, which is ~100x faster than a bash loop; the bash version needed
# several seconds per batch, so the first stop hit an idle worker and proved nothing.
batch_body() {
  awk -v s="$1" -v n="$BATCH_SIZE" -v tag="$RUN_TAG" 'BEGIN {
    printf "{\"notifications\":["
    for (i = 0; i < n; i++) {
      if (i % 3 == 0) { ch = "sms"; r = sprintf("+90556%07d", (s * n + i) % 10000000) }
      else if (i % 3 == 1) { ch = "email"; r = sprintf("stop%s.%d.%d@example.com", tag, s, i) }
      else { ch = "push"; r = sprintf("stop-token-%s-%d-%d", tag, s, i) }
      printf "%s{\"recipient\":\"%s\",\"channel\":\"%s\",\"content\":\"shutdown %s %d-%d\"}", (i ? "," : ""), r, ch, tag, s, i
    }
    printf "]}"
  }'
}

load_loop() {
  local s=0 start next out code
  start=$(date +%s)
  while (($(date +%s) - start < LOAD_SECONDS)); do
    next=$((start + (s + 1) * BATCH_EVERY))
    out="$(batch_body "$s" | curl -s --max-time 30 -w '\n%{http_code}' -X POST "${API_URL}/api/v1/notifications/batch" \
      -H 'Content-Type: application/json' -H "Idempotency-Key: shutdown-${RUN_TAG}-${s}" --data-binary @- || true)"
    code="$(printf '%s' "$out" | tail -n1)"
    echo "$s ${code:-000} $(printf '%s' "$out" | grep -oE '"batchId":"[^"]+"' | cut -d'"' -f4)" >> "$WORK/submissions.txt"
    s=$((s + 1))
    while (($(date +%s) < next)); do sleep 0.2; done
  done
}

# graceful_stop <service...>: SIGTERM via `compose stop` (SIGKILL only after 30s), timed; then checks
# that each stopped worker removed its heartbeat and inflight list and logged a clean stop.
graceful_stop() {
  local name="stop $*" t0 t1 elapsed svc wid since hb inflight clean timeout_hit requeued drained
  since="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  t0=$(date +%s%3N)
  "${COMPOSE[@]}" stop -t 30 "$@" >/dev/null 2>&1
  t1=$(date +%s%3N)
  elapsed=$(((t1 - t0) / 1000))
  local detail="SIGTERM -> stopped in $((t1 - t0))ms" bad=0
  for svc in "$@"; do
    [[ "$svc" == api ]] && continue
    wid="$svc"
    hb="$(redis_q EXISTS "{notif}:hb:${wid}")"
    inflight="$(redis_q LLEN "{notif}:inflight:${wid}")"
    clean="$("${COMPOSE[@]}" logs --no-log-prefix --since "$since" "$svc" 2>/dev/null | grep -c 'worker stopped cleanly' || true)"
    timeout_hit="$("${COMPOSE[@]}" logs --no-log-prefix --since "$since" "$svc" 2>/dev/null | grep -c 'shutdown timeout elapsed' || true)"
    requeued="$("${COMPOSE[@]}" logs --no-log-prefix --since "$since" "$svc" 2>/dev/null | grep -oE '"msg":"requeued inflight items on shutdown"[^}]*"count":[0-9]+' | grep -oE '[0-9]+$' || echo 0)"
    # Sends that completed between the shutdown signal and the clean stop: the drain actually had work.
    drained="$("${COMPOSE[@]}" logs --no-log-prefix --since "$since" "$svc" 2>/dev/null |
      awk '/worker shutdown signal received/ {on = 1; next} on && /notification sent/ {c++} /worker stopped cleanly/ {on = 0} END {print c + 0}')"
    detail+="; ${wid}: drained_sends=${drained} heartbeat=${hb} inflight=${inflight} clean_stop_log=${clean} timeout_hit=${timeout_hit} requeued=${requeued}"
    if [[ "$hb" != 0 || "$inflight" != 0 || "$clean" -lt 1 || "$timeout_hit" != 0 ]]; then bad=1; fi
    # A stop with nothing in flight proves nothing about draining: count it as a failure, not a pass.
    if [[ "$drained" == 0 ]]; then bad=1; fi
  done
  if ((elapsed > STOP_LIMIT_S)); then bad=1; fi
  if ((bad)); then record "$name" FAIL "$detail"; else record "$name" PASS "$detail"; fi
}

# rolling_restarts: restart worker-1 and worker-2 alternately, one every ROLLING_EVERY seconds after the
# previous restart finished, so at least one worker is always up and every restart drains live sends.
rolling_restarts() {
  local end=$((T0 + 5 + ROLLING_SECONDS)) i=0 svc since t0 t1 clean drained timeout_hit restarts=0 undrained=0 unclean=0
  while (($(date +%s) < end)); do
    svc="worker-$((i % 2 + 1))"
    since="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    t0=$(date +%s%3N)
    "${COMPOSE[@]}" restart -t 30 "$svc" >/dev/null 2>&1
    t1=$(date +%s%3N)
    clean="$("${COMPOSE[@]}" logs --no-log-prefix --since "$since" "$svc" 2>/dev/null | grep -c 'worker stopped cleanly' || true)"
    timeout_hit="$("${COMPOSE[@]}" logs --no-log-prefix --since "$since" "$svc" 2>/dev/null | grep -c 'shutdown timeout elapsed' || true)"
    drained="$("${COMPOSE[@]}" logs --no-log-prefix --since "$since" "$svc" 2>/dev/null |
      awk '/worker shutdown signal received/ {on = 1; next} on && /notification sent/ {c++} /worker stopped cleanly/ {on = 0} END {print c + 0}')"
    log "restart ${i}: ${svc} in $((t1 - t0))ms, drained_sends=${drained} clean_stop_log=${clean} timeout_hit=${timeout_hit}"
    restarts=$((restarts + 1))
    if [[ "$drained" == 0 ]]; then undrained=$((undrained + 1)); fi
    if [[ "$clean" -lt 1 || "$timeout_hit" != 0 ]]; then unclean=$((unclean + 1)); fi
    i=$((i + 1))
    sleep "$ROLLING_EVERY"
  done
  # A restart that found nothing in flight proves nothing; with 1,000 items every 2s it should not happen.
  if ((restarts >= 10 && undrained == 0 && unclean == 0)); then
    record "rolling restarts" PASS "${restarts} restarts, each drained live sends and logged a clean stop"
  else
    record "rolling restarts" FAIL "${restarts} restarts (need >= 10), ${undrained} without drained sends, ${unclean} unclean"
  fi
}

at() {
  local t="$1"
  shift
  while (($(date +%s) - T0 < t)); do sleep 0.5; done
  "$@"
}

# ---------------------------------------------------------------------------------------------

if [[ "$(curl -s -o /dev/null -w '%{http_code}' "${API_URL}/health/ready")" != "200" ]]; then
  echo "stack is not ready at ${API_URL}" >&2
  exit 2
fi
mock_config '{"latencyMs":300,"latencyJitterMs":200,"failureRate":0,"rate429":0,"permanentRate":0}'
curl -s -X POST "${MOCK_URL}/reset" >/dev/null
SINCE="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
log "graceful shutdown run ${RUN_TAG}, since=${SINCE}"
T0=$(date +%s)
load_loop &
LOAD_PID=$!

if [[ "$MODE" == rolling ]]; then
  at 5 rolling_restarts
else
  at 10 graceful_stop worker-1
  at 25 "${COMPOSE[@]}" start worker-1
  at 35 graceful_stop worker-2
  at 50 "${COMPOSE[@]}" start worker-2
  at 60 graceful_stop worker-1 worker-2
  at 72 "${COMPOSE[@]}" start worker-1 worker-2
  at 80 graceful_stop api
  at 88 "${COMPOSE[@]}" up -d --wait api
fi

wait "$LOAD_PID" || true
LOAD_PID=""
mock_config '{"latencyMs":0,"latencyJitterMs":0}'

# All-or-nothing across the API restart: every 202 batch has exactly BATCH_SIZE rows.
accepted="$(awk '$2 == 202 && $3 != "" {print $3}' "$WORK/submissions.txt" | sort -u)"
n_accepted="$(printf '%s\n' "$accepted" | grep -c . || true)"
codes="$(awk '{print $2}' "$WORK/submissions.txt" | sort | uniq -c | awk '{printf "%s x%s ", $2, $1}')"
batch_list="$(printf '%s\n' "$accepted" | awk -v q="'" 'NF {printf "%s%s%s%s", (n++ ? "," : ""), q, $1, q}')"
incomplete="$(psql_q "SELECT COUNT(*) FROM (SELECT batch_id FROM notifications WHERE batch_id IN (${batch_list})
  GROUP BY batch_id HAVING COUNT(*) <> ${BATCH_SIZE}) x;")"
missing="$(psql_q "SELECT ${n_accepted} - COUNT(DISTINCT batch_id) FROM notifications WHERE batch_id IN (${batch_list});")"
if [[ "$incomplete" == 0 && "$missing" == 0 ]]; then
  record "batches all-or-nothing" PASS "submissions: ${codes}; all ${n_accepted} accepted batches have ${BATCH_SIZE} rows"
else
  record "batches all-or-nothing" FAIL "submissions: ${codes}; incomplete=${incomplete} missing=${missing}"
fi

log "waiting for every accepted notification to become terminal"
deadline=$(($(date +%s) + 600))
while :; do
  open="$(psql_q "SELECT COUNT(*) FROM notifications WHERE batch_id IN (${batch_list}) AND status NOT IN ('sent','failed','cancelled');")"
  [[ "$open" == 0 ]] && break
  (($(date +%s) > deadline)) && break
  sleep 5
done
read -r sent failed abandoned < <(psql_q "SELECT COUNT(*) FILTER (WHERE status='sent'), COUNT(*) FILTER (WHERE status='failed'),
  (SELECT COUNT(*) FROM delivery_attempts a WHERE a.notification_id = ANY(ARRAY(SELECT id FROM notifications WHERE batch_id IN (${batch_list}))) AND a.outcome='abandoned')
  FROM notifications WHERE batch_id IN (${batch_list});")
if [[ "$open" == 0 && "$failed" == 0 && "$abandoned" == 0 ]]; then
  record "nothing left for the reconciler" PASS "$((n_accepted * BATCH_SIZE)) accepted: sent=${sent} failed=${failed} open=0; abandoned attempts=0"
else
  record "nothing left for the reconciler" FAIL "sent=${sent} failed=${failed} open=${open} abandoned=${abandoned}"
fi

# Fencing would make a stale write harmless, but a graceful restart must not need it at all.
lease_lost="$("${COMPOSE[@]}" logs --no-log-prefix --since "$SINCE" worker-1 worker-2 2>/dev/null | grep -c 'worker lease lost' || true)"
if [[ "$lease_lost" == 0 ]]; then
  record "no lease lost" PASS "0 'worker lease lost' log lines since ${SINCE}"
else
  record "no lease lost" FAIL "${lease_lost} 'worker lease lost' log lines since ${SINCE}"
fi

echo
inv_status=0
"$DIR/invariants.sh" --since="$SINCE" || inv_status=1
if ((inv_status)); then record "invariants (no duplicates allowed)" FAIL "see report above"; else record "invariants (no duplicates allowed)" PASS "0 duplicates, Redis and PostgreSQL consistent"; fi

echo
echo "=== graceful shutdown summary (${MODE}) ==="
for r in "${RESULTS[@]}"; do
  IFS='|' read -r check status detail <<<"$r"
  printf '%-34s %-5s %s\n' "$check" "$status" "$detail"
done
exit $((FAILURES > 0 ? 1 : 0))
