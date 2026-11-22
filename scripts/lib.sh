#!/usr/bin/env bash
# Shared helpers for the scenario scripts (isolation.sh, priority.sh, provider-timeout.sh, shutdown.sh
# --rolling). Source it; it defines functions and defaults only and runs nothing.
# shellcheck disable=SC2034 # variables are used by the sourcing scripts

API_URL="${API_URL:-http://localhost:8080}"
# API key for /api/v1 (docker-compose.yml enables this local-dev admin key by default).
API_KEY="${API_KEY:-local-dev-key-change-me}"
AUTH=(-H "Authorization: Bearer ${API_KEY}")
MOCK_URL="${MOCK_URL:-http://localhost:8090}"
PROM_URL="${PROM_URL:-http://localhost:9090}"
read -r -a COMPOSE <<<"${COMPOSE:-docker compose}"
RUN_TAG="${RUN_TAG:-$(date +%s)}"
LIB_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
FAILURES=0
RESULTS=()

log() { printf '[%s] %s\n' "$(date +%H:%M:%S)" "$*"; }

record() { # record <check> <PASS|FAIL> <detail>
  RESULTS+=("$1|$2|$3")
  if [[ "$2" == "FAIL" ]]; then FAILURES=$((FAILURES + 1)); fi
  log "$1: $2 - $3"
}

check() { # check <check> <0 = pass, else fail> <detail>
  if [[ "$2" -eq 0 ]]; then record "$1" PASS "$3"; else record "$1" FAIL "$3"; fi
}

summary() { # summary <title>: prints all results and returns 1 if any failed
  echo
  echo "=== $1 ==="
  local r check status detail
  for r in "${RESULTS[@]}"; do
    IFS='|' read -r check status detail <<<"$r"
    printf '%-40s %-5s %s\n' "$check" "$status" "$detail"
  done
  ((FAILURES == 0))
}

# </dev/null: `compose exec` reads stdin, which would swallow the input of a `while read` loop around it.
psql_q() { "${COMPOSE[@]}" exec -T postgres psql -U notif -d notif -qAt -F ' ' -c "$1" </dev/null | tr -d '\r'; }
redis_q() { "${COMPOSE[@]}" exec -T redis redis-cli --raw "$@" < /dev/null | tr -d '\r'; }

# mock_config <json>: changes the mock provider's fault profile at runtime (its /stats survive).
mock_config() { curl -s --max-time 10 -X POST "${MOCK_URL}/config" -H 'Content-Type: application/json' -d "$1" >/dev/null; }
mock_healthy() { mock_config '{"latencyMs":0,"latencyJitterMs":0,"failureRate":0,"rate429":0,"permanentRate":0}'; }
mock_reset() { curl -s --max-time 10 -X POST "${MOCK_URL}/reset" >/dev/null; }

require_stack() {
  if [[ "$(curl -s -o /dev/null -m 5 -w '%{http_code}' "${API_URL}/health/ready")" != "200" ]]; then
    echo "stack is not ready at ${API_URL}" >&2
    exit 2
  fi
  if ! curl -fs -m 5 -X POST "${MOCK_URL}/config" -H 'Content-Type: application/json' -d '{}' >/dev/null; then
    echo "mock provider has no POST /config; rebuild it (docker compose up -d --build mockprovider)" >&2
    exit 2
  fi
}

# batch_body <channel> <priority> <count> <seq> [scheduledAt]: a batch request body, built in awk
# (a bash loop needs seconds per 1,000 items, too slow to keep a load steady).
batch_body() {
  awk -v ch="$1" -v pr="$2" -v n="$3" -v s="$4" -v due="${5:-}" -v tag="$RUN_TAG" 'BEGIN {
    printf "{\"notifications\":["
    for (i = 0; i < n; i++) {
      if (ch == "sms") r = sprintf("+90559%07d", (s * n + i) % 10000000)
      else if (ch == "email") r = sprintf("lib%s.%d.%d@example.com", tag, s, i)
      else r = sprintf("lib-token-%s-%d-%d", tag, s, i)
      printf "%s{\"recipient\":\"%s\",\"channel\":\"%s\",\"priority\":\"%s\",\"content\":\"%s %s %d-%d\"%s}",
        (i ? "," : ""), r, ch, pr, ch, tag, s, i, (due != "" ? ",\"scheduledAt\":\"" due "\"" : "")
    }
    printf "]}"
  }'
}

# submit_batch <channel> <priority> <count> <seq>: prints the batchId ("" if the API rejected it).
submit_batch() {
  batch_body "$@" | curl "${AUTH[@]}" -s --max-time 60 -X POST "${API_URL}/api/v1/notifications/batch" \
    -H 'Content-Type: application/json' --data-binary @- | { grep -oE '"batchId":"[^"]+"' || true; } | cut -d'"' -f4
}

# sql_in_list <file>: one UUID per line -> 'a','b',... for a SQL IN (...) clause.
sql_in_list() { awk -v q="'" 'NF {printf "%s%s%s%s", (n++ ? "," : ""), q, $1, q}' "$1"; }

# wait_open_zero <sql-where> <timeout-s>: waits until no row matching the condition is non-terminal.
wait_open_zero() {
  local deadline=$(($(date +%s) + $2)) open
  while (($(date +%s) < deadline)); do
    open="$(psql_q "SELECT COUNT(*) FROM notifications WHERE ($1) AND status NOT IN ('sent','failed','cancelled');")"
    [[ "$open" == "0" ]] && return 0
    sleep 3
  done
  log "still open after $2s: ${open}"
  return 1
}

# queue_depth <channel>: ready items across the channel's three priority lists.
queue_depth() {
  local total=0 pr n
  for pr in high normal low; do
    n="$(redis_q LLEN "{notif}:q:$1:${pr}")"
    total=$((total + ${n:-0}))
  done
  echo "$total"
}

now_utc() { date -u +%Y-%m-%dT%H:%M:%SZ; }
