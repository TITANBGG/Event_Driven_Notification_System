#!/usr/bin/env bash
# Race-condition stress against the running compose stack. Each scenario fires many concurrent requests
# at one resource and checks the outcome from three sides: the API responses, PostgreSQL and the
# provider (mock /stats). Needs bash, curl (>= 7.84 for --parallel and %header{}), awk and docker compose.
#
#   ./scripts/races.sh            # all scenarios (~3 minutes)
#   ./scripts/races.sh 1 3        # selected scenarios
#
#   1  per-notification cancel vs worker   1000 SMS in flight, 1000 concurrent cancels
#   2  batch cancel storm                   20 concurrent cancels of the same 1000-item batch
#   3  idempotency storm                    200 concurrent POSTs, same key (same body, then different bodies)
#   4  manual retry storm                   20 failed notifications x 50 concurrent retries each
# shellcheck disable=SC2028 # curl config strings carry a literal backslash-n that curl expands in write-out
set -euo pipefail

API_URL="${API_URL:-http://localhost:8080}"
MOCK_URL="${MOCK_URL:-http://localhost:8090}"
read -r -a COMPOSE <<<"${COMPOSE:-docker compose}"
RUN_TAG="$(date +%s)"
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"; restore_mock' EXIT
FAILURES=0
RESULTS=()

log() { printf '[%s] %s\n' "$(date +%H:%M:%S)" "$*"; }
record() { # record <scenario> <PASS|FAIL> <detail>
  RESULTS+=("$1|$2|$3")
  if [[ "$2" == "FAIL" ]]; then FAILURES=$((FAILURES + 1)); fi
  log "$1: $2 - $3"
}
check() { # check <scenario> <condition-result 0/1> <detail>
  if [[ "$2" -eq 0 ]]; then record "$1" PASS "$3"; else record "$1" FAIL "$3"; fi
}

# par_requests <config>: runs every transfer of a curl config file concurrently from ONE curl process
# (up to 200 connections at once). Spawning one curl per request with xargs is slow, especially on
# Windows, and spread a "concurrent" storm over tens of seconds, so the races never actually happened.
# Relative output paths in the config resolve inside $WORK.
par_requests() { (cd "$WORK" && curl --parallel --parallel-immediate --parallel-max 200 --max-time 60 -K "$1"); }

psql_q() { "${COMPOSE[@]}" exec -T postgres psql -U notif -d notif -qAt -F ' ' -c "$1" | tr -d '\r'; }
mock_env() { env "$@" "${COMPOSE[@]}" up -d --wait mockprovider >/dev/null 2>&1; curl -s -X POST "${MOCK_URL}/reset" >/dev/null; }
restore_mock() { "${COMPOSE[@]}" up -d --wait mockprovider >/dev/null 2>&1 || true; }
# mock_count <id> -> provider requests received for that Idempotency-Key since the last reset
mock_counts() { curl -s "${MOCK_URL}/stats" | { grep -oE '"[0-9a-f-]{36}":[0-9]+' || true; } | tr -d '"' | tr ':' ' '; }

# create_batch <count> <recipient-base> -> prints "<batchId>" and writes notification IDs to $WORK/ids
create_batch() {
  local n="$1" base="$2" items="" i out
  for ((i = 0; i < n; i++)); do
    items+="${items:+,}{\"recipient\":\"+90555$((base + i))\",\"channel\":\"sms\",\"content\":\"race ${RUN_TAG} ${i}\"}"
  done
  out="$(printf '{"notifications":[%s]}' "$items" |
    curl -s --max-time 60 -X POST "${API_URL}/api/v1/notifications/batch" -H 'Content-Type: application/json' --data-binary @-)"
  printf '%s' "$out" | grep -oE '[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}' | tail -n +2 > "$WORK/ids"
  printf '%s' "$out" | grep -oE '"batchId":"[^"]+"' | cut -d'"' -f4
}

wait_terminal() { # wait_terminal <batchId> <timeout-s>
  local deadline=$(($(date +%s) + $2)) open
  while (($(date +%s) < deadline)); do
    open="$(psql_q "SELECT COUNT(*) FROM notifications WHERE batch_id = '$1' AND status NOT IN ('sent','failed','cancelled');")"
    [[ "$open" == "0" ]] && return 0
    sleep 1
  done
  return 1
}

# ---------------------------------------------------------------------------------------------

race_1_cancel_vs_worker() {
  local name="1 cancel vs worker" batch ok bad
  log "== ${name}"
  # 300ms provider latency keeps ~50 sends in flight, so cancels hit pending, processing and sent rows.
  mock_env MOCK_LATENCY_MS=300
  batch="$(create_batch 1000 5000000)"
  sleep 2
  {
    echo 'silent'
    echo 'request = "POST"'
    echo 'write-out = "%{url} %{http_code}\n"'
    while read -r id; do printf 'url = "%s/api/v1/notifications/%s/cancel"\noutput = "c_%s"\n' "$API_URL" "$id" "$id"; done < "$WORK/ids"
  } > "$WORK/cancel.cfg"
  par_requests cancel.cfg | sed -E 's#.*/notifications/([0-9a-f-]{36})/cancel ([0-9]+)$#\1 \2#' > "$WORK/cancel.txt"
  wait_terminal "$batch" 120 || true

  awk '{print $1, $2}' "$WORK/cancel.txt" | sort > "$WORK/cancel_sorted.txt"
  psql_q "SELECT id::text, status FROM notifications WHERE batch_id = '${batch}';" | sort > "$WORK/final.txt"
  mock_counts | sort > "$WORK/mock.txt"
  # One line per notification: id cancel_http final_status provider_calls
  join "$WORK/cancel_sorted.txt" "$WORK/final.txt" | join -a1 -e 0 -o '0,1.2,1.3,2.2' - "$WORK/mock.txt" > "$WORK/joined.txt"

  local c200 c409 other
  c200="$(awk '$2 == 200' "$WORK/joined.txt" | wc -l | tr -d ' ')"
  c409="$(awk '$2 == 409' "$WORK/joined.txt" | wc -l | tr -d ' ')"
  other="$(awk '$2 != 200 && $2 != 409' "$WORK/joined.txt" | wc -l | tr -d ' ')"
  # Every 200 must end cancelled and never reach the provider; every 409 must end sent (it was
  # processing or already sent when the cancel lost the race), and be sent exactly once.
  ok=$(awk '$2 == 200 && $3 == "cancelled" && $4 == 0' "$WORK/joined.txt" | wc -l | tr -d ' ')
  bad=$(awk '($2 == 200 && ($3 != "cancelled" || $4 != 0)) || ($2 == 409 && ($3 != "sent" || $4 != 1))' "$WORK/joined.txt" | wc -l | tr -d ' ')
  # Without both winners and losers the cancels never overlapped the worker: report that as a failure
  # instead of a vacuous pass.
  check "$name" $((bad > 0 || other > 0 || c200 + c409 != 1000 || c200 == 0 || c409 == 0 ? 1 : 0)) \
    "1000 cancels: ${c200} won (all cancelled, 0 provider calls: ${ok}/${c200}), ${c409} lost to the worker (sent exactly once); violations=${bad}, unexpected codes=${other}"
  restore_mock
}

race_2_batch_cancel_storm() {
  local name="2 batch cancel storm" batch sum_cancelled db_cancelled db_sent leaks responses
  log "== ${name}"
  mock_env MOCK_LATENCY_MS=300
  batch="$(create_batch 1000 5100000)"
  sleep 2
  {
    echo 'silent'
    echo 'request = "POST"'
    for i in $(seq 1 20); do printf 'url = "%s/api/v1/batches/%s/cancel"\noutput = "b_%s"\n' "$API_URL" "$batch" "$i"; done
  } > "$WORK/bcancel.cfg"
  par_requests bcancel.cfg
  for i in $(seq 1 20); do cat "$WORK/b_$i"; echo; done > "$WORK/bcancel.txt"
  wait_terminal "$batch" 120 || true

  responses="$(grep -oE '"cancelled":[0-9]+' "$WORK/bcancel.txt" | wc -l | tr -d ' ')"
  sum_cancelled="$(grep -oE '"cancelled":[0-9]+' "$WORK/bcancel.txt" | cut -d: -f2 | awk '{s+=$1} END {print s+0}')"
  db_cancelled="$(psql_q "SELECT COUNT(*) FROM notifications WHERE batch_id = '${batch}' AND status = 'cancelled';")"
  db_sent="$(psql_q "SELECT COUNT(*) FROM notifications WHERE batch_id = '${batch}' AND status = 'sent';")"
  mock_counts | sort > "$WORK/mock.txt"
  psql_q "SELECT id::text FROM notifications WHERE batch_id = '${batch}' AND status = 'cancelled';" | sort > "$WORK/bc_ids.txt"
  leaks="$(join "$WORK/bc_ids.txt" "$WORK/mock.txt" | wc -l | tr -d ' ')"
  # The batch cancel is one atomic UPDATE, so 20 concurrent calls must split the rows between them
  # without double counting: the sum reported by all responses equals the rows cancelled in the DB.
  check "$name" $((responses != 20 || sum_cancelled != db_cancelled || leaks != 0 || db_cancelled + db_sent != 1000 ? 1 : 0)) \
    "20/20 responses; sum of reported cancelled=${sum_cancelled}, cancelled in DB=${db_cancelled}, sent=${db_sent}; cancelled rows that reached the provider=${leaks}"
  restore_mock
}

race_3_idempotency_storm() {
  local name="3 idempotency storm" key rows codes replayed fresh ids
  log "== ${name}"
  key="race-${RUN_TAG}-same"
  printf '{"recipient":"+905551112233","channel":"sms","content":"idempotency storm %s"}' "$RUN_TAG" > "$WORK/body.json"
  {
    echo 'silent'
    echo 'request = "POST"'
    echo 'header = "Content-Type: application/json"'
    echo "header = \"Idempotency-Key: ${key}\""
    echo 'data-binary = "@body.json"'
    echo 'write-out = "%{http_code} %header{idempotent-replayed}\n"'
    for i in $(seq 1 200); do printf 'url = "%s/api/v1/notifications"\noutput = "r_%s.json"\n' "$API_URL" "$i"; done
  } > "$WORK/idem.cfg"
  par_requests idem.cfg > "$WORK/idem.txt"
  codes="$(awk '{print $1}' "$WORK/idem.txt" | sort | uniq -c | awk '{printf "%s x%s ", $2, $1}')"
  replayed="$(grep -c ' true$' "$WORK/idem.txt" || true)"
  fresh="$(awk '$1 == 202 && $2 != "true"' "$WORK/idem.txt" | wc -l | tr -d ' ')"
  ids="$(cat "$WORK"/r_*.json | grep -oE '"id":"[^"]+"' | sort -u | wc -l | tr -d ' ')"
  rows="$(psql_q "SELECT COUNT(*) FROM notifications WHERE idempotency_key = '${key}';")"
  check "$name (same body)" $((rows != 1 || fresh != 1 || replayed != 199 || ids != 1 ? 1 : 0)) \
    "200 concurrent POSTs: ${codes}; 1 created + ${replayed} replayed (Idempotent-Replayed: true), ${ids} distinct id(s) returned, ${rows} row(s) in DB"

  key="race-${RUN_TAG}-diff"
  # A different body per transfer needs `next` between blocks, and `next` resets every option.
  for i in $(seq 1 200); do
    ((i > 1)) && echo 'next'
    printf 'url = "%s/api/v1/notifications"\nsilent\nrequest = "POST"\n' "$API_URL"
    printf 'header = "Content-Type: application/json"\nheader = "Idempotency-Key: %s"\n' "$key"
    printf 'data-binary = "{\\"recipient\\":\\"+905551112233\\",\\"channel\\":\\"sms\\",\\"content\\":\\"body %s\\"}"\n' "$i"
    printf 'output = "d_%s.json"\nwrite-out = "%%{http_code}\\n"\n' "$i"
  done > "$WORK/idem2.cfg"
  par_requests idem2.cfg > "$WORK/idem2.txt"
  local c202 c409
  c202="$(grep -c '^202$' "$WORK/idem2.txt" || true)"
  c409="$(grep -c '^409$' "$WORK/idem2.txt" || true)"
  rows="$(psql_q "SELECT COUNT(*) FROM notifications WHERE idempotency_key = '${key}';")"
  check "$name (different bodies)" $((rows != 1 || c202 != 1 || c409 != 199 ? 1 : 0)) \
    "200 concurrent POSTs, 200 different bodies: 202 x${c202}, 409 idempotency_conflict x${c409}; ${rows} row(s) in DB"
}

race_4_manual_retry_storm() {
  local name="4 manual retry storm" batch failed per_ok bad
  log "== ${name}"
  # Make 20 notifications fail permanently (400), then storm each with 50 concurrent manual retries.
  mock_env MOCK_PERMANENT_RATE=1
  batch="$(create_batch 20 5200000)"
  wait_terminal "$batch" 60 || true
  failed="$(psql_q "SELECT COUNT(*) FROM notifications WHERE batch_id = '${batch}' AND status = 'failed';")"
  mock_env
  {
    echo 'silent'
    echo 'request = "POST"'
    echo 'write-out = "%{url} %{http_code}\n"'
    while read -r id; do
      for k in $(seq 1 50); do printf 'url = "%s/api/v1/notifications/%s/retry"\noutput = "t_%s_%s"\n' "$API_URL" "$id" "$id" "$k"; done
    done < "$WORK/ids"
  } > "$WORK/retry.cfg"
  par_requests retry.cfg | sed -E 's#.*/notifications/([0-9a-f-]{36})/retry ([0-9]+)$#\1 \2#' > "$WORK/retry.txt"
  wait_terminal "$batch" 60 || true

  mock_counts | sort > "$WORK/mock.txt"
  # Per notification: exactly one 200 (failed -> pending), 49 x 409, final status sent, one provider call.
  awk '{c[$1" "$2]++} END {for (k in c) print k, c[k]}' "$WORK/retry.txt" | sort > "$WORK/retry_counts.txt"
  per_ok="$(awk '$2 == 200 && $3 == 1' "$WORK/retry_counts.txt" | wc -l | tr -d ' ')"
  psql_q "SELECT id::text, status FROM notifications WHERE batch_id = '${batch}';" | sort > "$WORK/final.txt"
  bad="$(join -a1 -e 0 -o '0,1.2,2.2' "$WORK/final.txt" "$WORK/mock.txt" | awk '$2 != "sent" || $3 != 1' | wc -l | tr -d ' ')"
  local dlq
  dlq="$(while read -r id; do "${COMPOSE[@]}" exec -T redis redis-cli --raw LPOS '{notif}:dlq' "$id" < /dev/null | tr -d '\r'; done < "$WORK/ids" | grep -c '[0-9]' || true)"
  check "$name" $((failed != 20 || per_ok != 20 || bad != 0 || dlq != 0 ? 1 : 0)) \
    "20 failed x 50 concurrent retries: ${per_ok}/20 accepted exactly once (rest 409); ${bad} not sent exactly once afterwards; ${dlq} still in the DLQ list"
}

# ---------------------------------------------------------------------------------------------

if [[ "$(curl -s -o /dev/null -w '%{http_code}' "${API_URL}/health/ready")" != "200" ]]; then
  echo "stack is not ready at ${API_URL}; start it with 'docker compose up -d' first" >&2
  exit 2
fi

selected=("$@")
if ((${#selected[@]} == 0)); then selected=(1 2 3 4); fi
for s in "${selected[@]}"; do
  case "$s" in
    1) race_1_cancel_vs_worker ;;
    2) race_2_batch_cancel_storm ;;
    3) race_3_idempotency_storm ;;
    4) race_4_manual_retry_storm ;;
    *) echo "unknown scenario $s" >&2; exit 2 ;;
  esac
done

echo
echo "=== race summary ==="
for r in "${RESULTS[@]}"; do
  IFS='|' read -r scenario status detail <<<"$r"
  printf '%-34s %-5s %s\n' "$scenario" "$status" "$detail"
done
exit $((FAILURES > 0 ? 1 : 0))
