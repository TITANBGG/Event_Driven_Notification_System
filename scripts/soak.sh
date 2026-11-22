#!/usr/bin/env bash
# Long-running endurance (soak) test: sustains steady mixed-channel, mixed-priority load plus
# periodic scheduled, idempotent, metrics, and list probes, then checks for goroutine/FD/heap/Redis
# leaks, container restarts, invariants, and per-channel rate limits.
#
# Recommended setup for a 60-minute soak so Jaeger stays well inside its memory budget:
#   OTEL_SAMPLER_RATIO=0.1 docker compose up -d --force-recreate --wait
#   API_URL=http://127.0.0.1:8080 MOCK_URL=http://127.0.0.1:8090 PROM_URL=http://127.0.0.1:9090 ./scripts/soak.sh
#
# Environment overrides:
#   SOAK_SECONDS     Load duration in seconds (default: 3600 = 60 minutes)
#   BATCH_SIZE       Items per mixed batch (default: 1000)
#   BATCH_EVERY      Seconds between batches (default: 4 -> 250/s mixed across 3 channels)
#   PROBE_EVERY      Run scheduled/idempotency/metrics/list probes every N batches (default: 15)
#   SAMPLE_INTERVAL  Prometheus/DB/Redis sample cadence in seconds (default: 30)
#   FAILURE_RATE     Mock transient failure rate during soak (default: 0.005)
#   DRAIN_TIMEOUT    Seconds to wait for terminal completion after load stops (default: 900)
#   SOAK_REPORT      Optional path to keep metrics.csv after a passing run
#   OUTAGE_EVERY     Seconds between provider outages (default: 600; 0 disables them)
#   OUTAGE_OFFSET    First outage this many seconds in (default: 300, so every 10-minute slice has one)
#   OUTAGE_SECONDS   Length of each outage: every provider call answers 500 (default: 30)
#   WS_CLIENTS       WebSocket readers held open for the whole load window (default: 50; 0 disables)
#
# Besides leaks, the run fails if: any WebSocket reader is dropped or starves; the ready queue is not
# back to its pre-outage level (+2,000) before the next outage (RECOVERY_SECONDS after it ended); the
# outages never opened a breaker (they would have tested nothing); or end-to-end p99 in the last
# 10-minute slice is over twice the first.
#
# Why not 3 minutes: at 250/s against ~290/s of capacity only ~40/s is spare. In the 2026-11-21 run each
# 30s outage (plus the breaker's open state) built a backlog of 6,500-8,700 that took 4.4-5.7 minutes
# to drain. So the check is "no accumulation from one outage to the next", the property that matters.
set -euo pipefail
# shellcheck source=scripts/lib.sh
source "$(dirname "$0")/lib.sh"

SOAK_SECONDS="${SOAK_SECONDS:-3600}"
BATCH_SIZE="${BATCH_SIZE:-1000}"
BATCH_EVERY="${BATCH_EVERY:-4}"
PROBE_EVERY="${PROBE_EVERY:-15}"
SAMPLE_INTERVAL="${SAMPLE_INTERVAL:-30}"
FAILURE_RATE="${FAILURE_RATE:-0.005}"
DRAIN_TIMEOUT="${DRAIN_TIMEOUT:-900}"
P99_MAX_MS="${P99_MAX_MS:-3000}"
OUTAGE_EVERY="${OUTAGE_EVERY:-600}"
OUTAGE_OFFSET="${OUTAGE_OFFSET:-300}"
OUTAGE_SECONDS="${OUTAGE_SECONDS:-30}"
WS_CLIENTS="${WS_CLIENTS:-50}"
RECOVERY_SECONDS="${RECOVERY_SECONDS:-540}"

WORK="$(mktemp -d)"
SERVICES=(api worker-1 worker-2 redis postgres mockprovider jaeger prometheus)

# shellcheck disable=SC2317,SC2329 # invoked by EXIT trap
cleanup() {
  local rc=$?
  if [[ -n "${WATCH_PID:-}" ]]; then kill "$WATCH_PID" 2>/dev/null || true; fi
  docker rm -f "soak-ws-${RUN_TAG}" >/dev/null 2>&1 || true
  mock_healthy || true
  if [[ -n "${SOAK_REPORT:-}" && -f "$WORK/metrics.csv" ]]; then
    cp "$WORK/metrics.csv" "$SOAK_REPORT" 2>/dev/null || true
  fi
  if ((rc == 0)); then
    rm -rf "$WORK"
  else
    echo "soak artifacts kept in ${WORK} (exit ${rc})" >&2
  fi
}
trap cleanup EXIT

# soak_batch_body <seq>: mixed channels (sms/email/push) and priorities (20/60/20) generated in awk
# (~80ms per 1,000 items so sustained 4s pacing never drifts).
soak_batch_body() {
  awk -v n="$BATCH_SIZE" -v s="$1" -v tag="$RUN_TAG" 'BEGIN {
    printf "{\"notifications\":["
    for (i = 0; i < n; i++) {
      m = i % 3
      if (m == 0) {
        ch = "sms"
        r = sprintf("+90558%07d", (s * n + i) % 10000000)
      } else if (m == 1) {
        ch = "email"
        r = sprintf("soak%s.%d.%d@example.com", tag, s, i)
      } else {
        ch = "push"
        r = sprintf("soak-token-%s-%d-%d", tag, s, i)
      }
      p = i % 5
      if (p == 0) pr = "high"
      else if (p == 4) pr = "low"
      else pr = "normal"
      printf "%s{\"recipient\":\"%s\",\"channel\":\"%s\",\"priority\":\"%s\",\"content\":\"soak %s %d-%d\"}",
        (i ? "," : ""), r, ch, pr, tag, s, i
    }
    printf "]}"
  }'
}

utc_plus_seconds() {
  local epoch=$(($(date +%s) + $1))
  date -u -d "@${epoch}" +%Y-%m-%dT%H:%M:%SZ 2>/dev/null || date -u -r "${epoch}" +%Y-%m-%dT%H:%M:%SZ
}

prom_line() {
  "$LIB_DIR/prom-snapshot.sh" watch 1 "$WORK/one.csv" >/dev/null 2>&1 &
  local pid=$!
  while [[ ! -s "$WORK/one.csv" || $(wc -l < "$WORK/one.csv") -lt 2 ]]; do
    sleep 0.2
  done
  kill "$pid" 2>/dev/null || true
  wait "$pid" 2>/dev/null || true
  tail -n 1 "$WORK/one.csv"
  rm -f "$WORK/one.csv"
}

run_probes() {
  local seq="$1" due idem_key code1 code2 id1 id2 body_file="$WORK/probe.body"
  local mcode lcode hcode

  # 1. Scheduled notification due in 5s
  due="$(utc_plus_seconds 5)"
  code1="$(curl "${AUTH[@]}" -s --max-time 10 -o "$body_file" -w '%{http_code}' \
    -X POST "${API_URL}/api/v1/notifications" -H 'Content-Type: application/json' \
    -d "{\"recipient\":\"+90558999$(printf '%04d' $((seq % 10000)))\",\"channel\":\"sms\",\"priority\":\"high\",\"content\":\"soak-sched ${RUN_TAG} ${seq}\",\"scheduledAt\":\"${due}\"}" || echo "000")"
  if [[ "$code1" == "202" ]]; then
    PROBE_SINGLES=$((PROBE_SINGLES + 1))
  else
    PROBE_ERRORS=$((PROBE_ERRORS + 1))
    log "probe scheduled seq=${seq} failed HTTP ${code1}"
  fi

  # 2. Idempotent create + replay
  idem_key="soak-idem-${RUN_TAG}-${seq}"
  local idem_payload="{\"recipient\":\"soak-idem-${seq}@example.com\",\"channel\":\"email\",\"priority\":\"normal\",\"content\":\"soak-idem ${RUN_TAG} ${seq}\"}"
  code1="$(curl "${AUTH[@]}" -s --max-time 10 -o "$body_file" -w '%{http_code}' \
    -X POST "${API_URL}/api/v1/notifications" -H 'Content-Type: application/json' \
    -H "Idempotency-Key: ${idem_key}" -d "$idem_payload" || echo "000")"
  id1="$(grep -oE '"id":"[^"]+"' "$body_file" 2>/dev/null | head -n1 | cut -d'"' -f4 || true)"

  code2="$(curl "${AUTH[@]}" -s --max-time 10 -o "$body_file" -w '%{http_code}' \
    -X POST "${API_URL}/api/v1/notifications" -H 'Content-Type: application/json' \
    -H "Idempotency-Key: ${idem_key}" -d "$idem_payload" || echo "000")"
  id2="$(grep -oE '"id":"[^"]+"' "$body_file" 2>/dev/null | head -n1 | cut -d'"' -f4 || true)"

  if [[ "$code1" == "202" && ( "$code2" == "200" || "$code2" == "202" ) && -n "$id1" && "$id1" == "$id2" ]]; then
    PROBE_SINGLES=$((PROBE_SINGLES + 1))
  else
    PROBE_ERRORS=$((PROBE_ERRORS + 1))
    log "probe idempotency seq=${seq} failed c1=${code1}(${id1}) c2=${code2}(${id2})"
  fi

  # 3. API read/health endpoints
  hcode="$(curl -s --max-time 10 -o /dev/null -w '%{http_code}' "${API_URL}/health/ready" || echo "000")"
  mcode="$(curl "${AUTH[@]}" -s --max-time 10 -o /dev/null -w '%{http_code}' "${API_URL}/api/v1/metrics" || echo "000")"
  lcode="$(curl "${AUTH[@]}" -s --max-time 10 -o /dev/null -w '%{http_code}' "${API_URL}/api/v1/notifications?limit=50" || echo "000")"
  if [[ "$hcode" != "200" || "$mcode" != "200" || "$lcode" != "200" ]]; then
    PROBE_ERRORS=$((PROBE_ERRORS + 1))
    log "probe read seq=${seq} failed health=${hcode} metrics=${mcode} list=${lcode}"
  fi
  PROBE_RUNS=$((PROBE_RUNS + 1))
}

require_stack
mock_healthy
mock_config "{\"failureRate\":${FAILURE_RATE}}"
mock_reset

restarts_before="$(for s in "${SERVICES[@]}"; do docker inspect -f '{{.RestartCount}}' "$("${COMPOSE[@]}" ps -q "$s")"; done | awk '{s += $1} END {print s + 0}')"
START="$(now_utc)"
log "soak run ${RUN_TAG}: duration=${SOAK_SECONDS}s, batch=${BATCH_SIZE} every ${BATCH_EVERY}s, failureRate=${FAILURE_RATE}, since=${START}"

# Capture baseline row before starting Prom watcher
baseline_line="$(prom_line)"
printf '%s\n' "timestamp,goroutines_api,goroutines_w1,goroutines_w2,alloc_bytes_api,alloc_bytes_w1,alloc_bytes_w2,open_fds_api,open_fds_w1,open_fds_w2,queue_ready,queue_delayed,queue_dlq,queue_inflight,db_pool_acq_api,db_pool_acq_w1,db_pool_acq_w2,db_pool_empty_acquire_sum,pg_conns_total,pg_conns_active,redis_used_memory_bytes" >"$WORK/metrics.csv"
printf '%s\n' "$baseline_line" >>"$WORK/metrics.csv"

"$LIB_DIR/prom-snapshot.sh" watch "$SAMPLE_INTERVAL" "$WORK/metrics.csv" >/dev/null 2>&1 &
WATCH_PID=$!

# WebSocket readers run in the pinned golang image on the Compose network, so the host needs no Go.
WS_CONTAINER=""
if ((WS_CLIENTS > 0)); then
  WS_CONTAINER="soak-ws-${RUN_TAG}"
  ws_net="$(docker inspect -f '{{range $k, $v := .NetworkSettings.Networks}}{{$k}} {{end}}' "$("${COMPOSE[@]}" ps -q api)" | awk '{print $1}')"
  src="$(cd "$LIB_DIR/.." && (pwd -W 2>/dev/null || pwd))"
  MSYS_NO_PATHCONV=1 docker run -d --name "$WS_CONTAINER" --network "$ws_net" -v "${src}:/src" -w /src \
    -v notif-gomod:/go/pkg/mod -v notif-gobuild:/root/.cache/go-build golang:1.27.4 \
    go run ./cmd/wsflood -url http://api:8080 -hold "${SOAK_SECONDS}s" -hold-clients "$WS_CLIENTS" >/dev/null
  log "WebSocket readers: ${WS_CLIENTS} in container ${WS_CONTAINER} on ${ws_net}"
fi

ready_total() { echo $(($(queue_depth sms) + $(queue_depth email) + $(queue_depth push))); }
next_outage=$(( $(date +%s) + OUTAGE_OFFSET ))
outage_end=0
OUTAGES=0
RECOVERY_DUE=()
RECOVERY_LIMIT=()
RECOVERY_FAILS=0
RECOVERY_LOG=""

t_start=$(date +%s)
next_progress=$((t_start + 300))
seq=0
ACCEPTED_BATCHES=0
TOTAL_BATCHES=0
PROBE_SINGLES=0
PROBE_ERRORS=0
PROBE_RUNS=0
: >"$WORK/batches.txt"

while (($(date +%s) - t_start < SOAK_SECONDS)); do
  tick=$(($(date +%s) + BATCH_EVERY))
  out="$(soak_batch_body "$seq" | curl "${AUTH[@]}" -s --max-time 30 -o "$WORK/batch.body" -w '%{http_code} %{time_total}' \
    -X POST "${API_URL}/api/v1/notifications/batch" -H 'Content-Type: application/json' --data-binary @- || echo "000 30.0")"
  code="${out%% *}"
  lat="${out#* }"
  bid="$(grep -oE '"batchId":"[^"]+"' "$WORK/batch.body" 2>/dev/null | cut -d'"' -f4 || true)"
  printf '%d %s %s %s\n' "$seq" "$code" "$lat" "${bid:-none}" >>"$WORK/batches.txt"
  TOTAL_BATCHES=$((TOTAL_BATCHES + 1))
  if [[ "$code" == "202" && -n "$bid" ]]; then
    ACCEPTED_BATCHES=$((ACCEPTED_BATCHES + 1))
  fi

  if ((seq == 0 || (seq + 1) % PROBE_EVERY == 0)); then
    run_probes "$seq"
  fi

  now_s=$(date +%s)
  # Provider outage: every call answers 500 for OUTAGE_SECONDS. Only started while its recovery check
  # still falls inside the load window (after the load stops, the queue drains trivially).
  if ((OUTAGE_EVERY > 0 && outage_end == 0 && now_s >= next_outage && now_s - t_start < SOAK_SECONDS - OUTAGE_SECONDS - RECOVERY_SECONDS)); then
    before="$(ready_total)"
    mock_config '{"failureRate":1}'
    outage_end=$((now_s + OUTAGE_SECONDS))
    OUTAGES=$((OUTAGES + 1))
    RECOVERY_DUE+=($((outage_end + RECOVERY_SECONDS)))
    RECOVERY_LIMIT+=($((before + 2000)))
    log "provider outage ${OUTAGES} for ${OUTAGE_SECONDS}s (ready queue before: ${before})"
    next_outage=$((next_outage + OUTAGE_EVERY))
  elif ((outage_end > 0 && now_s >= outage_end)); then
    mock_config "{\"failureRate\":${FAILURE_RATE}}"
    outage_end=0
    log "provider outage over"
  fi
  for i in "${!RECOVERY_DUE[@]}"; do
    if ((RECOVERY_DUE[i] > 0 && now_s >= RECOVERY_DUE[i])); then
      after="$(ready_total)"
      RECOVERY_LOG+="#$((i + 1)): ${after} (limit ${RECOVERY_LIMIT[i]}) "
      if ((after > RECOVERY_LIMIT[i])); then RECOVERY_FAILS=$((RECOVERY_FAILS + 1)); fi
      log "outage $((i + 1)) recovery: ready queue ${after} ${RECOVERY_SECONDS}s after it ended (limit ${RECOVERY_LIMIT[i]})"
      RECOVERY_DUE[i]=0
    fi
  done

  if ((now_s >= next_progress)); then
    elapsed=$((now_s - t_start))
    last_row="$(tail -n 1 "$WORK/metrics.csv")"
    IFS=',' read -r _ ga gw1 gw2 _ _ _ fa fw1 fw2 qr qd qdlq qi _ _ _ _ pgt _ rmem <<<"$last_row"
    jaeger_mem="$(docker stats --no-stream --format '{{.MemUsage}}' "$("${COMPOSE[@]}" ps -q jaeger)" 2>/dev/null | awk '{print $1}' || echo "?")"
    log "progress ${elapsed}/${SOAK_SECONDS}s: batches=${ACCEPTED_BATCHES}/${TOTAL_BATCHES} goroutines=(${ga},${gw1},${gw2}) fds=(${fa},${fw1},${fw2}) q=(r:${qr},d:${qd},i:${qi},dlq:${qdlq}) pg_conns=${pgt} redisB=${rmem} jaeger=${jaeger_mem}"
    next_progress=$((now_s + 300))
  fi

  seq=$((seq + 1))
  while (($(date +%s) < tick)); do
    if (($(date +%s) - t_start >= SOAK_SECONDS)); then
      break
    fi
    sleep 0.2
  done
done

log "load window complete (${ACCEPTED_BATCHES}/${TOTAL_BATCHES} batches 202, ${PROBE_SINGLES} probe singles, ${PROBE_ERRORS} probe errors); waiting for drain"
mock_healthy
wait_open_zero "created_at >= '${START}'::timestamptz" "$DRAIN_TIMEOUT" || true
sleep 2

kill "$WATCH_PID" 2>/dev/null || true
wait "$WATCH_PID" 2>/dev/null || true
WATCH_PID=""

final_line="$(prom_line)"
printf '%s\n' "$final_line" >>"$WORK/metrics.csv"

expected_total=$((ACCEPTED_BATCHES * BATCH_SIZE + PROBE_SINGLES))
read -r total_rows sent_rows failed_rows open_rows <<<"$(psql_q "
  SELECT COUNT(*),
         COUNT(*) FILTER (WHERE status = 'sent'),
         COUNT(*) FILTER (WHERE status = 'failed'),
         COUNT(*) FILTER (WHERE status NOT IN ('sent','failed','cancelled'))
  FROM notifications WHERE created_at >= '${START}'::timestamptz;")"

read -r p50 p99 <<<"$(awk '{print $3 * 1000}' "$WORK/batches.txt" | sort -n |
  awk '{v[NR] = $1} END {i50 = int(NR * 0.50); i99 = int(NR * 0.99); if (i50 < 1) i50 = 1; if (i99 < 1) i99 = 1; printf "%d %d", v[i50], v[i99]}')"

check "batches_202" "$((ACCEPTED_BATCHES == TOTAL_BATCHES && TOTAL_BATCHES > 0 ? 0 : 1))" \
  "${ACCEPTED_BATCHES}/${TOTAL_BATCHES} batches 202 (batch p50 ${p50}ms, p99 ${p99}ms)"
check "batch_p99" "$((p99 < P99_MAX_MS ? 0 : 1))" "p99 ${p99}ms (need < ${P99_MAX_MS}ms)"
check "probes_ok" "$((PROBE_ERRORS == 0 && PROBE_RUNS > 0 ? 0 : 1))" \
  "${PROBE_RUNS} probe cycles, ${PROBE_ERRORS} errors, ${PROBE_SINGLES} singles created"
check "all_sent" "$((total_rows == expected_total && sent_rows == expected_total && failed_rows == 0 && open_rows == 0 ? 0 : 1))" \
  "sent=${sent_rows}/${expected_total} (rows=${total_rows}, failed=${failed_rows}, open=${open_rows})"

oomkilled="$(for s in "${SERVICES[@]}"; do docker inspect -f '{{.State.OOMKilled}}' "$("${COMPOSE[@]}" ps -q "$s")"; done | grep -c true || true)"
restarts_after="$(for s in "${SERVICES[@]}"; do docker inspect -f '{{.RestartCount}}' "$("${COMPOSE[@]}" ps -q "$s")"; done | awk '{s += $1} END {print s + 0}')"
check "no_container_restarts" "$((oomkilled == 0 && restarts_after == restarts_before ? 0 : 1))" \
  "OOMKilled=${oomkilled}, restarts ${restarts_before} -> ${restarts_after}"

# Analyze metrics.csv for goroutine, FD, and heap leaks (baseline -> early -> late -> final).
leak_report="$(awk -F, '
NR == 1 { next }
$2 == "NaN" || $3 == "NaN" || $4 == "NaN" || $5 == "NaN" || $8 == "NaN" { next }
{
  n++
  rows[n] = $0
  if (n == 1) {
    base_ga=$2+0; base_gw1=$3+0; base_gw2=$4+0
    base_aa=$5+0; base_aw1=$6+0; base_aw2=$7+0
    base_fa=$8+0; base_fw1=$9+0; base_fw2=$10+0
    base_rmem=$21+0
  }
}
END {
  if (n < 2) {
    printf "0 0 0 samples=%d insufficient_samples\n", n
    exit 0
  }
  # rows[1] is baseline, rows[n] is post-drain final; steady window is 2..n-1 (fallback to all if short)
  s_start = 2; s_end = n - 1
  if (s_end < s_start) { s_start = 1; s_end = n }
  span = s_end - s_start + 1
  q = int(span * 0.25); if (q < 1) q = 1
  e_end = s_start + q - 1; if (e_end > s_end) e_end = s_end
  l_start = s_end - q + 1; if (l_start < s_start) l_start = s_start

  e_ga=0; e_gw1=0; e_gw2=0; e_fa=0; e_fw1=0; e_fw2=0; e_aa=0; e_aw1=0; e_aw2=0; ec=0
  for (i = s_start; i <= e_end; i++) {
    split(rows[i], f, ",")
    e_ga += f[2]; e_gw1 += f[3]; e_gw2 += f[4]
    e_aa += f[5]; e_aw1 += f[6]; e_aw2 += f[7]
    e_fa += f[8]; e_fw1 += f[9]; e_fw2 += f[10]
    ec++
  }
  l_ga=0; l_gw1=0; l_gw2=0; l_fa=0; l_fw1=0; l_fw2=0; l_aa=0; l_aw1=0; l_aw2=0; lc=0
  for (i = l_start; i <= s_end; i++) {
    split(rows[i], f, ",")
    l_ga += f[2]; l_gw1 += f[3]; l_gw2 += f[4]
    l_aa += f[5]; l_aw1 += f[6]; l_aw2 += f[7]
    l_fa += f[8]; l_fw1 += f[9]; l_fw2 += f[10]
    lc++
  }
  split(rows[n], fin, ",")

  e_ga/=ec; e_gw1/=ec; e_gw2/=ec; e_aa/=ec; e_aw1/=ec; e_aw2/=ec; e_fa/=ec; e_fw1/=ec; e_fw2/=ec
  l_ga/=lc; l_gw1/=lc; l_gw2/=lc; l_aa/=lc; l_aw1/=lc; l_aw2/=lc; l_fa/=lc; l_fw1/=lc; l_fw2/=lc

  # Post-drain values are compared with the larger of the cold baseline and the early steady average:
  # pools (go-redis keeps idle connections for 30 minutes) legitimately grow from a cold start under
  # load, which is warm-up, not a leak. A leak keeps growing, so late vs early catches it.
  r_ga = (e_ga > base_ga) ? e_ga : base_ga; r_gw1 = (e_gw1 > base_gw1) ? e_gw1 : base_gw1; r_gw2 = (e_gw2 > base_gw2) ? e_gw2 : base_gw2
  r_fa = (e_fa > base_fa) ? e_fa : base_fa; r_fw1 = (e_fw1 > base_fw1) ? e_fw1 : base_fw1; r_fw2 = (e_fw2 > base_fw2) ? e_fw2 : base_fw2
  g_ok = (fin[2] <= r_ga + 20 && fin[3] <= r_gw1 + 20 && fin[4] <= r_gw2 + 20 && l_ga <= e_ga + 30 && l_gw1 <= e_gw1 + 30 && l_gw2 <= e_gw2 + 30) ? 1 : 0
  fd_ok = (fin[8] <= r_fa + 20 && fin[9] <= r_fw1 + 20 && fin[10] <= r_fw2 + 20 && l_fa <= e_fa + 30 && l_fw1 <= e_fw1 + 30 && l_fw2 <= e_fw2 + 30) ? 1 : 0

  lim_aa = (e_aa * 2 > e_aa + 16*1024*1024) ? e_aa * 2 : e_aa + 16*1024*1024
  lim_aw1 = (e_aw1 * 2 > e_aw1 + 16*1024*1024) ? e_aw1 * 2 : e_aw1 + 16*1024*1024
  lim_aw2 = (e_aw2 * 2 > e_aw2 + 16*1024*1024) ? e_aw2 * 2 : e_aw2 + 16*1024*1024
  h_ok = (fin[5] < 128*1024*1024 && fin[6] < 128*1024*1024 && fin[7] < 128*1024*1024 && l_aa <= lim_aa && l_aw1 <= lim_aw1 && l_aw2 <= lim_aw2) ? 1 : 0

  printf "%d %d %d samples=%d goroutines(base:%.0f/%.0f/%.0f early:%.0f/%.0f/%.0f late:%.0f/%.0f/%.0f final:%.0f/%.0f/%.0f) fds(base:%.0f/%.0f/%.0f late:%.0f/%.0f/%.0f final:%.0f/%.0f/%.0f) heapMB(early:%.1f/%.1f/%.1f late:%.1f/%.1f/%.1f final:%.1f/%.1f/%.1f) redisMB(base:%.2f final:%.2f)\n",
    g_ok, fd_ok, h_ok, n,
    base_ga, base_gw1, base_gw2, e_ga, e_gw1, e_gw2, l_ga, l_gw1, l_gw2, fin[2], fin[3], fin[4],
    base_fa, base_fw1, base_fw2, l_fa, l_fw1, l_fw2, fin[8], fin[9], fin[10],
    e_aa/1048576, e_aw1/1048576, e_aw2/1048576, l_aa/1048576, l_aw1/1048576, l_aw2/1048576, fin[5]/1048576, fin[6]/1048576, fin[7]/1048576,
    base_rmem/1048576, fin[21]/1048576
}' "$WORK/metrics.csv")"

read -r g_ok fd_ok h_ok trend_detail <<<"$leak_report"
log "trend: ${trend_detail}"
check "no_goroutine_leak" "$((g_ok == 1 ? 0 : 1))" "$trend_detail"
check "no_fd_leak" "$((fd_ok == 1 ? 0 : 1))" "$trend_detail"
check "no_heap_leak" "$((h_ok == 1 ? 0 : 1))" "$trend_detail"

if [[ -n "$WS_CONTAINER" ]]; then
  ws_rc="$(docker wait "$WS_CONTAINER" 2>/dev/null || echo 1)"
  ws_out="$(docker logs "$WS_CONTAINER" 2>&1 | grep 'ws-hold' | tail -n 2 | tr '\n' ' ')"
  check "websocket_readers_held" "$((ws_rc == 0 ? 0 : 1))" "${WS_CLIENTS} readers for ${SOAK_SECONDS}s: ${ws_out:-no output}"
fi

if ((OUTAGE_EVERY > 0)); then
  breaker_defer="$(psql_q "SELECT COUNT(*) FROM delivery_attempts a JOIN notifications n ON n.id = a.notification_id
    WHERE n.created_at >= '${START}'::timestamptz AND a.outcome = 'circuit_open';")"
  evaluated=$(( ${#RECOVERY_DUE[@]} ))
  for d in "${RECOVERY_DUE[@]}"; do ((d > 0)) && evaluated=$((evaluated - 1)); done
  check "outages_opened_breaker" "$((OUTAGES > 0 && breaker_defer > 0 ? 0 : 1))" \
    "${OUTAGES} outages of ${OUTAGE_SECONDS}s, ${breaker_defer} circuit_open deferrals"
  check "recovered_after_outages" "$((evaluated == OUTAGES && RECOVERY_FAILS == 0 ? 0 : 1))" \
    "ready queue ${RECOVERY_SECONDS}s after each outage: ${RECOVERY_LOG:-none evaluated} (${evaluated}/${OUTAGES} evaluated)"
fi

# End-to-end latency per slice of creation time: the last slice must not be over twice the first.
slice=$((SOAK_SECONDS >= 1200 ? 600 : SOAK_SECONDS / 2))
psql_q "SELECT FLOOR(EXTRACT(EPOCH FROM created_at - '${START}'::timestamptz) / ${slice})::int,
    ROUND(1000 * EXTRACT(EPOCH FROM percentile_cont(0.50) WITHIN GROUP (ORDER BY sent_at - created_at))),
    ROUND(1000 * EXTRACT(EPOCH FROM percentile_cont(0.99) WITHIN GROUP (ORDER BY sent_at - created_at)))
  FROM notifications WHERE created_at >= '${START}'::timestamptz AND status = 'sent'
    AND created_at < '${START}'::timestamptz + make_interval(secs => ${SOAK_SECONDS} - ${SOAK_SECONDS} % ${slice})
  GROUP BY 1 ORDER BY 1;" >"$WORK/slices"
slices_txt="$(awk -v s="$slice" '{printf "%d-%dmin p50 %sms p99 %sms; ", $1 * s / 60, ($1 + 1) * s / 60, $2, $3}' "$WORK/slices")"
read -r first_p99 last_p99 <<<"$(awk 'NR == 1 {f = $3} {l = $3} END {print f + 0, l + 0}' "$WORK/slices")"
log "end-to-end latency by ${slice}s slice: ${slices_txt}"
check "latency_trend" "$((first_p99 > 0 && last_p99 <= 2 * first_p99 ? 0 : 1))" \
  "p99 first slice ${first_p99}ms, last ${last_p99}ms (need last <= 2x first); ${slices_txt}"

jaeger_final="$(docker stats --no-stream --format '{{.MemUsage}}' "$("${COMPOSE[@]}" ps -q jaeger)" 2>/dev/null || echo "unknown")"
log "jaeger final container memory: ${jaeger_final}"

if "${LIB_DIR}/ratecheck.sh" --since="$START" --channels=3 >"$WORK/rate" 2>&1; then rc=0; else rc=1; fi
cat "$WORK/rate"
check "rate_limit" "$rc" "ratecheck.sh --since=${START}"

if "${LIB_DIR}/invariants.sh" --since="$START" >"$WORK/inv" 2>&1; then rc=0; else rc=1; fi
tail -n 15 "$WORK/inv"
check "invariants" "$rc" "invariants.sh --since=${START}"

summary "Endurance soak (${SOAK_SECONDS}s, ${RUN_TAG})"
