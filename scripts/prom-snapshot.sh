#!/usr/bin/env bash
# Captures Go runtime, FD, Redis, PostgreSQL connection, and queue depth snapshots
# from Prometheus, PostgreSQL, and Redis during stress/chaos tests.
#
# Usage:
#   ./scripts/prom-snapshot.sh once
#   ./scripts/prom-snapshot.sh watch <interval_sec> <output.csv>

set -euo pipefail

if [[ -n "${COMPOSE:-}" ]]; then
  read -r -a COMPOSE_CMD <<<"${COMPOSE}"
elif docker compose version >/dev/null 2>&1; then
  COMPOSE_CMD=(docker compose)
else
  COMPOSE_CMD=(docker-compose)
fi

PROM_URL="${PROM_URL:-http://localhost:9090}"
POSTGRES_USER="${POSTGRES_USER:-notif}"
POSTGRES_DB="${POSTGRES_DB:-notif}"

prom_val() {
  local expr="$1"
  local resp val
  resp="$(curl -sG --max-time 5 "${PROM_URL}/api/v1/query" --data-urlencode "query=${expr}" || true)"
  val="$(printf '%s' "$resp" | grep -oE '"value":\[[0-9.]+,"[^"]+"\]' | head -n1 | awk -F'"' '{print $4}')"
  echo "${val:-NaN}"
}

collect_metrics() {
  local ts goroutines_api goroutines_w1 goroutines_w2
  local alloc_api alloc_w1 alloc_w2 fd_api fd_w1 fd_w2
  local q_ready q_delayed q_dlq q_inflight pg_active pg_total redis_mem_bytes

  ts="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  goroutines_api="$(prom_val 'go_goroutines{job="notification-api"}')"
  goroutines_w1="$(prom_val 'go_goroutines{instance="worker-1:9091"}')"
  goroutines_w2="$(prom_val 'go_goroutines{instance="worker-2:9091"}')"

  alloc_api="$(prom_val 'go_memstats_alloc_bytes{job="notification-api"}')"
  alloc_w1="$(prom_val 'go_memstats_alloc_bytes{instance="worker-1:9091"}')"
  alloc_w2="$(prom_val 'go_memstats_alloc_bytes{instance="worker-2:9091"}')"

  fd_api="$(prom_val 'process_open_fds{job="notification-api"}')"
  fd_w1="$(prom_val 'process_open_fds{instance="worker-1:9091"}')"
  fd_w2="$(prom_val 'process_open_fds{instance="worker-2:9091"}')"

  q_ready="$(prom_val 'sum(queue_depth)')"
  q_delayed="$(prom_val 'delayed_depth')"
  q_dlq="$(prom_val 'dlq_depth')"
  q_inflight="$(prom_val 'inflight_depth')"

  pool_acq_api="$(prom_val 'db_pool_acquired_conns{job="notification-api"}')"
  pool_acq_w1="$(prom_val 'db_pool_acquired_conns{instance="worker-1:9091"}')"
  pool_acq_w2="$(prom_val 'db_pool_acquired_conns{instance="worker-2:9091"}')"
  pool_empty_sum="$(prom_val 'sum(db_pool_empty_acquire_total)')"

  pg_total="$("${COMPOSE_CMD[@]}" exec -T postgres psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -Atc "SELECT COUNT(*) FROM pg_stat_activity WHERE datname = current_database();" 2>/dev/null | tr -d '\r' | head -n1 || echo "NaN")"
  pg_active="$("${COMPOSE_CMD[@]}" exec -T postgres psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -Atc "SELECT COUNT(*) FROM pg_stat_activity WHERE datname = current_database() AND state = 'active';" 2>/dev/null | tr -d '\r' | head -n1 || echo "NaN")"
  redis_mem_bytes="$("${COMPOSE_CMD[@]}" exec -T redis redis-cli INFO memory 2>/dev/null | awk -F: '/^used_memory:/ {gsub(/\r/,"",$2); print $2; exit}' || echo "NaN")"
  redis_mem_bytes="${redis_mem_bytes:-NaN}"

  printf '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n' \
    "$ts" "$goroutines_api" "$goroutines_w1" "$goroutines_w2" \
    "$alloc_api" "$alloc_w1" "$alloc_w2" \
    "$fd_api" "$fd_w1" "$fd_w2" \
    "$q_ready" "$q_delayed" "$q_dlq" "$q_inflight" \
    "$pool_acq_api" "$pool_acq_w1" "$pool_acq_w2" "$pool_empty_sum" \
    "$pg_total" "$pg_active" "$redis_mem_bytes"
}

HEADER="timestamp,goroutines_api,goroutines_w1,goroutines_w2,alloc_bytes_api,alloc_bytes_w1,alloc_bytes_w2,open_fds_api,open_fds_w1,open_fds_w2,queue_ready,queue_delayed,queue_dlq,queue_inflight,db_pool_acq_api,db_pool_acq_w1,db_pool_acq_w2,db_pool_empty_acquire_sum,pg_conns_total,pg_conns_active,redis_used_memory_bytes"

mode="${1:-once}"
case "$mode" in
  once)
    line="$(collect_metrics)"
    IFS=',' read -r ts ga gw1 gw2 aa aw1 aw2 fa fw1 fw2 qr qd qdlq qi paq_a paq_w1 paq_w2 pemp pgt pga rmem <<<"$line"
    echo "=== Prometheus / Stack Snapshot (${ts}) ==="
    echo "Goroutines   : api=${ga}, worker-1=${gw1}, worker-2=${gw2}"
    echo "Heap Alloc B : api=${aa}, worker-1=${aw1}, worker-2=${aw2}"
    echo "Open FDs     : api=${fa}, worker-1=${fw1}, worker-2=${fw2}"
    echo "Queues       : ready=${qr}, delayed=${qd}, dlq=${qdlq}, inflight=${qi}"
    echo "DB Pool (Prom): acquired(api=${paq_a}, w1=${paq_w1}, w2=${paq_w2}), empty_acquire_total=${pemp}"
    echo "DB Conns (PG) : total=${pgt}, active=${pga}"
    echo "Redis Memory : used_memory_bytes=${rmem}"
    ;;
  watch)
    interval="${2:-5}"
    outfile="${3:-prom-snapshots.csv}"
    if [[ ! -f "$outfile" ]]; then
      printf '%s\n' "$HEADER" > "$outfile"
    fi
    echo "Recording Prometheus/DB/Redis snapshots every ${interval}s to ${outfile} (Ctrl-C to stop)..."
    while true; do
      collect_metrics >> "$outfile"
      sleep "$interval"
    done
    ;;
  *)
    echo "Usage: $0 {once|watch <interval_sec> <output.csv>}" >&2
    exit 2
    ;;
esac

