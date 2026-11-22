#!/usr/bin/env bash
# Live failure drills against the running docker compose stack (README section 5).
# Each drill breaks one dependency, checks the documented behavior, restores the stack and reports
# PASS/FAIL with measured times. Needs only bash, curl, grep, awk and docker compose.
#
#   ./scripts/drills.sh            # all drills (about 6-8 minutes)
#   ./scripts/drills.sh 3 5        # selected drills
# shellcheck disable=SC2317,SC2329 # predicate functions are invoked indirectly through wait_until
set -euo pipefail

API_URL="${API_URL:-http://localhost:8080}"
# API key for /api/v1 (docker-compose.yml enables this local-dev admin key by default).
API_KEY="${API_KEY:-local-dev-key-change-me}"
AUTH=(-H "Authorization: Bearer ${API_KEY}")
MOCK_URL="${MOCK_URL:-http://localhost:8090}"
PROM_URL="${PROM_URL:-http://localhost:9090}"
read -r -a COMPOSE <<<"${COMPOSE:-docker compose}"
RUN_TAG="$(date +%s)"
FAILURES=0
RESULTS=()

log() { printf '[%s] %s\n' "$(date +%H:%M:%S)" "$*"; }
now() { date +%s; }

record() { # record <drill> <PASS|FAIL> <detail>
  RESULTS+=("$1|$2|$3")
  if [[ "$2" == "FAIL" ]]; then FAILURES=$((FAILURES + 1)); fi
  log "$1: $2 - $3"
}

# wait_until <timeout-seconds> <command...>: polls every second; returns 1 on timeout.
wait_until() {
  local timeout="$1"
  shift
  local deadline=$(($(now) + timeout))
  until "$@" >/dev/null 2>&1; do
    if (($(now) >= deadline)); then return 1; fi
    sleep 1
  done
}

json_get() { grep -o "\"$1\":\"[^\"]*\"" | head -n1 | cut -d'"' -f4; }
http_code() { curl -s -o /dev/null -w '%{http_code}' --max-time "${2:-10}" "$1" || true; }

# count_status <batchId> <status> -> number of notifications of the batch in that status
count_status() {
  curl "${AUTH[@]}" -s --max-time 10 "${API_URL}/api/v1/batches/$1?limit=1" |
    { grep -o '"countsByStatus":{[^}]*}' || true; } | { grep -o "\"$2\":[0-9]*" || true; } |
    cut -d: -f2 | awk '{s+=$1} END {print s+0}'
}
all_terminal() { # all_terminal <batchId> <total>
  local finished
  finished=$(($(count_status "$1" sent) + $(count_status "$1" failed) + $(count_status "$1" cancelled)))
  ((finished == $2))
}

create_sms() { # create_sms <recipient-suffix> -> prints "<http-code> <id>"
  local out code id
  out="$(curl "${AUTH[@]}" -s --max-time 30 -w '\n%{http_code}' -X POST "${API_URL}/api/v1/notifications" \
    -H "Content-Type: application/json" \
    -d "{\"recipient\":\"+90555${1}\",\"channel\":\"sms\",\"content\":\"drill ${RUN_TAG}\"}" || true)"
  code="$(printf '%s' "$out" | tail -n1)"
  id="$(printf '%s' "$out" | sed '$d' | json_get id || true)"
  echo "${code} ${id}"
}

create_sms_batch() { # create_sms_batch <count> <recipient-base-7-digits> -> prints batchId
  local n="$1" base="$2" items="" i
  for ((i = 0; i < n; i++)); do
    items+="${items:+,}{\"recipient\":\"+90555$((base + i))\",\"channel\":\"sms\",\"content\":\"drill ${RUN_TAG} ${i}\"}"
  done
  # Body via stdin: a 500-item batch exceeds the Windows command-line length limit as an argument.
  printf '{"notifications":[%s]}' "$items" |
    curl "${AUTH[@]}" -s --max-time 30 -X POST "${API_URL}/api/v1/notifications/batch" \
      -H "Content-Type: application/json" --data-binary @- | json_get batchId
}

api_ready() { [[ "$(http_code "${API_URL}/health/ready")" == "200" ]]; }
notification_status() { curl "${AUTH[@]}" -s --max-time 10 "${API_URL}/api/v1/notifications/$1" | json_get status; }
is_status() { [[ "$(notification_status "$1")" == "$2" ]]; }

mock_env() { # mock_env [VAR=value ...]: recreate the mock provider with the given settings
  env "$@" "${COMPOSE[@]}" up -d --wait mockprovider >/dev/null 2>&1
  curl -s -X POST "${MOCK_URL}/reset" >/dev/null
}

# mock_duplicates -> provider requests whose Idempotency-Key was received more than once
mock_duplicates() {
  curl -s "${MOCK_URL}/stats" | { grep -oE '"[0-9a-f-]{36}":[0-9]+' || true; } | awk -F: '$2 > 1 {d += $2 - 1} END {print d + 0}'
}

prom_breaker_state_is() { # prom_breaker_state_is <0|1|2>: max SMS breaker state across workers
  curl -s -G "${PROM_URL}/api/v1/query" --data-urlencode 'query=max(circuit_breaker_state{channel="sms"})' |
    grep -q "\"value\":\[[0-9.]*,\"$1\"\]"
}
prom_breaker_open() { prom_breaker_state_is 2; }
prom_breaker_closed() { prom_breaker_state_is 0; }

restore_stack() {
  "${COMPOSE[@]}" start redis postgres >/dev/null 2>&1 || true
  "${COMPOSE[@]}" up -d --wait >/dev/null 2>&1 || true
  curl -s -X POST "${MOCK_URL}/reset" >/dev/null 2>&1 || true
}
trap restore_stack EXIT

# ---------------------------------------------------------------------------------------------

drill_1_redis_down_during_create() {
  local name="1 Redis down during create" t0 code id rest
  log "== ${name}"
  "${COMPOSE[@]}" stop redis >/dev/null 2>&1
  t0=$(now)
  read -r code id rest < <(create_sms "1$((RUN_TAG % 1000000))")
  local ready_out ready ready_status
  ready_out="$(curl -s --max-time 10 -w '\n%{http_code}' "${API_URL}/health/ready" || true)"
  ready="$(printf '%s' "$ready_out" | tail -n1)"
  ready_status="$(printf '%s' "$ready_out" | sed '$d' | json_get status || true)"
  log "POST -> ${code} in $(($(now) - t0))s, /health/ready -> ${ready} (${ready_status})"
  "${COMPOSE[@]}" start redis >/dev/null 2>&1
  t0=$(now)
  if [[ "$code" == "202" && "$ready" == "200" && "$ready_status" == "degraded" ]] && wait_until 120 is_status "$id" sent; then
    record "$name" PASS "202 accepted while Redis was down, /health/ready=200 (${ready_status}); sent $(($(now) - t0))s after Redis restart (reconciler step a)"
  else
    record "$name" FAIL "POST=${code} ready=${ready}(${ready_status}) final status=$(notification_status "$id")"
  fi
}

drill_2_postgres_down() {
  local name="2 PostgreSQL down" code id rest ready t0
  log "== ${name}"
  "${COMPOSE[@]}" stop postgres >/dev/null 2>&1
  read -r code id rest < <(create_sms "2$((RUN_TAG % 1000000))")
  ready="$(http_code "${API_URL}/health/ready")"
  log "POST -> ${code}, /health/ready -> ${ready}"
  "${COMPOSE[@]}" start postgres >/dev/null 2>&1
  wait_until 60 api_ready || true
  t0=$(now)
  local code2 id2
  read -r code2 id2 rest < <(create_sms "3$((RUN_TAG % 1000000))")
  if [[ "$code" == "503" && "$ready" == "503" && "$code2" == "202" ]] && wait_until 60 is_status "$id2" sent; then
    record "$name" PASS "POST=503 and /health/ready=503 while down; after restart a new notification was sent in $(($(now) - t0))s"
  else
    record "$name" FAIL "POST while down=${code} ready=${ready}; after restart POST=${code2} status=$(notification_status "$id2")"
  fi
}

drill_3_worker_sigkill() {
  local name="3 Worker SIGKILL mid-batch" batch t0 dups since
  log "== ${name}"
  mock_env MOCK_LATENCY_MS=200
  batch="$(create_sms_batch 300 4000000)"
  sleep 1
  since="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  "${COMPOSE[@]}" kill worker-1 >/dev/null 2>&1
  t0=$(now)
  log "killed worker-1 with SIGKILL; batch ${batch}"
  if wait_until 240 all_terminal "$batch" 300; then
    local elapsed=$(($(now) - t0)) sent orphan crashed
    sent="$(count_status "$batch" sent)"
    dups="$(mock_duplicates)"
    orphan="$("${COMPOSE[@]}" logs --no-log-prefix --since "$since" worker-2 2>/dev/null | grep -c 'requeued orphaned inflight items' || true)"
    crashed="$("${COMPOSE[@]}" logs --no-log-prefix --since "$since" worker-2 2>/dev/null | grep -c 'worker likely crashed' || true)"
    if ((sent == 300)); then
      record "$name" PASS "300/300 sent ${elapsed}s after the kill; orphan-requeue log lines=${orphan}, abandoned-recovery log lines=${crashed}; duplicate provider calls=${dups} (at-least-once)"
    else
      record "$name" FAIL "only ${sent}/300 sent"
    fi
  else
    record "$name" FAIL "batch not terminal after 240s: sent=$(count_status "$batch" sent)"
  fi
  # `start`, not `up`: worker-1 depends on the mock provider, and `up` would recreate it with the
  # default settings, wiping the in-memory /stats the invariants cross-check needs. The main loop
  # restores the mock provider only after that check.
  "${COMPOSE[@]}" start worker-1 >/dev/null 2>&1
}

drill_4_provider_outage() {
  local name="4 Provider outage (all 500s)" batch t0 opened failed
  log "== ${name}"
  # Start from a closed breaker; one left open by an earlier run would make the timing meaningless.
  wait_until 120 prom_breaker_closed || true
  mock_env MOCK_FAILURE_RATE=1
  batch="$(create_sms_batch 40 4100000)"
  t0=$(now)
  if wait_until 90 prom_breaker_open; then opened=$(($(now) - t0)); else opened=-1; fi
  failed="$(count_status "$batch" failed)"
  log "breaker open after ${opened}s, failed so far=${failed}"
  mock_env
  t0=$(now)
  if ((opened >= 0 && failed == 0)) && wait_until 240 all_terminal "$batch" 40 &&
    (($(count_status "$batch" sent) == 40)); then
    record "$name" PASS "breaker open (circuit_breaker_state=2) ${opened}s into the outage, 0 failed; 40/40 sent $(($(now) - t0))s after recovery"
  else
    record "$name" FAIL "opened=${opened}s failed=${failed} sent=$(count_status "$batch" sent)/40"
  fi
}

drill_5_redis_flushall() {
  local name="5 Redis FLUSHALL mid-batch" batch t0 dups
  log "== ${name}"
  mock_env
  batch="$(create_sms_batch 500 4200000)"
  sleep 1
  "${COMPOSE[@]}" exec -T redis redis-cli FLUSHALL >/dev/null
  t0=$(now)
  log "flushed Redis; batch ${batch} sent so far=$(count_status "$batch" sent)"
  if wait_until 240 all_terminal "$batch" 500 && (($(count_status "$batch" sent) == 500)); then
    dups="$(mock_duplicates)"
    record "$name" PASS "500/500 sent $(($(now) - t0))s after FLUSHALL (reconciler rebuilt queues from PostgreSQL); duplicate provider calls=${dups}"
  else
    record "$name" FAIL "sent=$(count_status "$batch" sent)/500 after 240s"
  fi
}

# ---------------------------------------------------------------------------------------------

if [[ "$(http_code "${API_URL}/health/ready")" != "200" ]]; then
  echo "stack is not ready at ${API_URL}; start it with 'docker compose up -d' first" >&2
  exit 2
fi

selected=("$@")
if ((${#selected[@]} == 0)); then selected=(1 2 3 4 5); fi
for d in "${selected[@]}"; do
  drill_start="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  case "$d" in
    1) drill_1_redis_down_during_create ;;
    2) drill_2_postgres_down ;;
    3) drill_3_worker_sigkill ;;
    4) drill_4_provider_outage ;;
    5) drill_5_redis_flushall ;;
    *) echo "unknown drill $d" >&2; exit 2 ;;
  esac
  # Check invariants before restore_stack: recreating the mock provider would reset its /stats.
  # Drills 3 and 5 interrupt in-flight sends, so at-least-once duplicates are expected there.
  inv_args=("--since=${drill_start}")
  if [[ "$d" == "3" || "$d" == "5" ]]; then inv_args+=(--allow-duplicates); fi
  if inv_out="$(COMPOSE="${COMPOSE[*]}" "$(dirname "$0")/invariants.sh" "${inv_args[@]}" 2>&1)"; then
    record "${d} invariants" PASS "$(printf '%s\n' "$inv_out" | grep -E '^6\. ' | sed 's/^6\. Duplicate deliveries *PASS *//')"
  else
    printf '%s\n' "$inv_out"
    record "${d} invariants" FAIL "post-drill invariants failed (report printed above)"
  fi
  restore_stack
done

echo
echo "=== drill summary ==="
for r in "${RESULTS[@]}"; do
  IFS='|' read -r drill status detail <<<"$r"
  printf '%-32s %-5s %s\n' "$drill" "$status" "$detail"
done
exit $((FAILURES > 0 ? 1 : 0))
