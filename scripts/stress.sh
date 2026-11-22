#!/usr/bin/env bash
# Load under constrained resources: run against the stress profile, where the API and workers have
# 0.5 CPU / 256 MB and Redis is capped at 64 MB with noeviction:
#
#   docker compose -f docker-compose.yml -f docker-compose.stress.yml up -d --wait
#   ./scripts/stress.sh
#
# Phase A: a 50,000-notification flash sale (fits in Redis). Phase B: 200,000 at once, more than 64 MB of
# queue metadata, so Redis must refuse writes (OOM). PostgreSQL is the source of truth: the API still
# answers 202 for every committed batch, and the reconciler re-enqueues the untracked rows as the queues
# drain.
#
# PASS requires: no container OOM-killed or restarted, batch request p99 < 3s in each phase, every
# accepted notification sent, invariants and rate check passing, and (to prove phase B did what it is
# for) Redis reaching its memory limit with the API logging failed enqueues.
set -euo pipefail
# shellcheck source=scripts/lib.sh
source "$(dirname "$0")/lib.sh"

PHASE_A="${PHASE_A:-50}"
PHASE_B="${PHASE_B:-200}"
PARALLEL="${PARALLEL:-4}"
P99_MAX_MS="${P99_MAX_MS:-3000}"
DRAIN_TIMEOUT="${DRAIN_TIMEOUT:-3600}"
WORK="$(mktemp -d)"
# The watcher has usually exited already; under set -e a failing kill would turn a pass into exit 1.
trap 'if [[ -n "${WATCH_PID:-}" ]]; then kill "$WATCH_PID" 2>/dev/null || true; fi; rm -rf "$WORK"' EXIT
SERVICES=(api worker-1 worker-2 redis postgres mockprovider)

require_stack
maxmem="$(redis_q CONFIG GET maxmemory | sed -n 2p)"
if [[ "$maxmem" == "0" || -z "$maxmem" ]]; then
  echo "Redis has no maxmemory: start the stress profile first (see the header of $0)" >&2
  exit 2
fi
mock_healthy
START="$(now_utc)"
log "stress run ${RUN_TAG} since ${START}; Redis maxmemory $((maxmem / 1024 / 1024)) MB"

restarts_before="$(for s in "${SERVICES[@]}"; do docker inspect -f '{{.RestartCount}}' "$("${COMPOSE[@]}" ps -q "$s")"; done | awk '{s += $1} END {print s + 0}')"

# submit_phase <name> <batches>: <batches> 1,000-item batches, round-robin over the channels, PARALLEL
# at a time; one line per request: "<http code> <seconds> <batchId>".
submit_phase() {
  local name="$1" n="$2" i ch
  local chans=(sms email push) pids=()
  : >"$WORK/$name.txt"
  # Wait on these PIDs only: a bare `wait` would also wait for the Redis watcher and never return.
  for ((i = 0; i < n; i++)); do
    ch="${chans[i % 3]}"
    # One file per request: parallel jobs appending to one file interleave their partial writes.
    {
      batch_body "$ch" normal 1000 "$i" | curl "${AUTH[@]}" -s --max-time 60 -o "$WORK/$name.$i.body" -w '%{http_code} %{time_total}' \
        -X POST "${API_URL}/api/v1/notifications/batch" -H 'Content-Type: application/json' --data-binary @- || true
      printf ' %s\n' "$(grep -oE '"batchId":"[^"]+"' "$WORK/$name.$i.body" 2>/dev/null | cut -d'"' -f4)"
    } >"$WORK/$name.$i.line" &
    pids+=($!)
    if ((${#pids[@]} == PARALLEL)); then
      wait "${pids[@]}"
      pids=()
    fi
  done
  if ((${#pids[@]})); then wait "${pids[@]}"; fi
  for ((i = 0; i < n; i++)); do cat "$WORK/$name.$i.line"; done >"$WORK/$name.txt"
}

# phase_report <name>: checks the request latency and records accepted batches in $WORK/<name>.ids.
phase_report() {
  local name="$1" total accepted p50 p99
  total="$(grep -c . "$WORK/$name.txt")"
  awk '$1 == 202 && $3 != "" {print $3}' "$WORK/$name.txt" >"$WORK/$name.ids"
  accepted="$(grep -c . "$WORK/$name.ids" || true)"
  read -r p50 p99 <<<"$(awk '{print $2 * 1000}' "$WORK/$name.txt" | sort -n |
    awk '{v[NR] = $1} END {i50 = int(NR * 0.50); i99 = int(NR * 0.99); if (i50 < 1) i50 = 1; if (i99 < 1) i99 = 1; printf "%d %d", v[i50], v[i99]}')"
  check "${name}_accepted" "$((accepted == total ? 0 : 1))" "${accepted}/${total} batches 202 ($(awk '{print $1}' "$WORK/$name.txt" | sort | uniq -c | awk '{printf "%s x%s ", $2, $1}'))"
  check "${name}_batch_p99" "$((p99 < P99_MAX_MS ? 0 : 1))" "batch request p50 ${p50}ms p99 ${p99}ms (need p99 < ${P99_MAX_MS}ms)"
}

# watch_redis <seconds-or-until-file>: samples Redis memory and queue depth every 5s into redis.csv.
watch_redis() {
  while [[ ! -e "$WORK/stop_watch" ]]; do
    printf '%s %s %s\n' "$(date +%s)" "$(redis_q INFO memory | awk -F: '/^used_memory:/ {print $2}')" "$(redis_q HLEN '{notif}:msg')" >>"$WORK/redis.csv"
    sleep 5
  done
}
watch_redis &
WATCH_PID=$!

wait_drained() { # wait_drained <ids-file> <timeout>
  wait_open_zero "batch_id IN ($(sql_in_list "$1"))" "$2"
}

log "phase A: ${PHASE_A} x 1,000"
submit_phase phase_a "$PHASE_A"
phase_report phase_a
t0=$(date +%s)
wait_drained "$WORK/phase_a.ids" "$DRAIN_TIMEOUT" || true
log "phase A drained in $(($(date +%s) - t0))s"

log "phase B: ${PHASE_B} x 1,000"
since_b="$(now_utc)"
submit_phase phase_b "$PHASE_B"
phase_report phase_b
t0=$(date +%s)
wait_drained "$WORK/phase_b.ids" "$DRAIN_TIMEOUT" || true
log "phase B drained in $(($(date +%s) - t0))s"
touch "$WORK/stop_watch"
wait "$WATCH_PID" || true

cat "$WORK/phase_a.ids" "$WORK/phase_b.ids" >"$WORK/all.ids"
all="$(sql_in_list "$WORK/all.ids")"
read -r rows sent open <<<"$(psql_q "SELECT COUNT(*), COUNT(*) FILTER (WHERE status = 'sent'),
  COUNT(*) FILTER (WHERE status NOT IN ('sent','failed','cancelled')) FROM notifications WHERE batch_id IN (${all});")"
check "all_sent" "$((rows > 0 && sent == rows ? 0 : 1))" "${sent}/${rows} sent, ${open} still open"

peak="$(awk '{print $2}' "$WORK/redis.csv" | sort -n | tail -n 1)"
peak_tracked="$(awk '{print $3}' "$WORK/redis.csv" | sort -n | tail -n 1)"
oom_logs="$("${COMPOSE[@]}" logs --no-log-prefix --since "$since_b" api 2>/dev/null | grep -ci 'oom' || true)"
check "redis_reached_limit" "$((peak * 100 >= maxmem * 95 ? 0 : 1))" \
  "peak used_memory $((peak / 1024 / 1024)) MB of $((maxmem / 1024 / 1024)) MB, peak tracked IDs ${peak_tracked}"
check "api_saw_oom" "$((oom_logs > 0 ? 0 : 1))" "${oom_logs} API log lines mentioning OOM during phase B"

oomkilled="$(for s in "${SERVICES[@]}"; do docker inspect -f '{{.State.OOMKilled}}' "$("${COMPOSE[@]}" ps -q "$s")"; done | grep -c true || true)"
restarts_after="$(for s in "${SERVICES[@]}"; do docker inspect -f '{{.RestartCount}}' "$("${COMPOSE[@]}" ps -q "$s")"; done | awk '{s += $1} END {print s + 0}')"
check "no_container_oom_killed" "$((oomkilled == 0 && restarts_after == restarts_before ? 0 : 1))" \
  "OOMKilled=${oomkilled}, restarts ${restarts_before} -> ${restarts_after}"
# Dequeue must keep working while Redis is full: it is memory-neutral, and a full Redis that cannot
# dequeue can never free memory again (see internal/queue/lua/dequeue.lua).
dequeue_oom="$("${COMPOSE[@]}" logs --no-log-prefix --since "$since_b" worker-1 worker-2 2>/dev/null | grep 'failed dequeuing' | grep -c 'OOM' || true)"
check "dequeue_works_under_oom" "$((dequeue_oom == 0 ? 0 : 1))" "${dequeue_oom} worker dequeue failures with OOM during phase B"
repaired="$(curl -s --max-time 10 "${PROM_URL}/api/v1/query" \
  --data-urlencode "query=sum(increase(reconciler_actions_total{action=\"stale_pending\"}[$(($(date +%s) - $(date -d "$since_b" +%s) + 60))s]))" |
  { grep -oE '"value":\[[0-9.]+,"[0-9.]+"' || true; } | sed -E 's/.*,"([0-9.]+)"/\1/' | cut -d. -f1)"
log "reconciler re-enqueued ~${repaired:-?} untracked pending rows during phase B (Prometheus estimate)"

if "${LIB_DIR}/ratecheck.sh" --since="$START" >"$WORK/rate" 2>&1; then rc=0; else rc=1; fi
cat "$WORK/rate"
check "rate_limit" "$rc" "ratecheck.sh --since=${START}"
if "${LIB_DIR}/invariants.sh" --since="$START" >"$WORK/inv" 2>&1; then rc=0; else rc=1; fi
tail -n 15 "$WORK/inv"
check "invariants" "$rc" "invariants.sh --since=${START}"
cp "$WORK/redis.csv" "${STRESS_CSV:-/dev/null}" 2>/dev/null || true

summary "Constrained resources (${RUN_TAG})"
