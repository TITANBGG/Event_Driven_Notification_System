#!/usr/bin/env bash
# A provider slower than PROVIDER_TIMEOUT, then a provider that rejects everything.
#
# Part 1: the mock answers after 12s, beyond the 10s PROVIDER_TIMEOUT, for 90s while 300 SMS are sent.
# Timeouts are transient failures, so the per-channel breaker must open and later sends must be deferred
# without burning attempts (delivery_attempts.outcome = 'circuit_open', counted = false). After the
# provider recovers, half-open probes close the breaker and every notification is delivered.
#
# Part 2: the mock answers every request with a permanent 4xx. Each notification must get exactly one
# attempt, end 'failed' and sit in the DLQ list; permanent errors are breaker successes, so no deferral.
#
#   ./scripts/provider-timeout.sh     (stack running with the default PROVIDER_TIMEOUT=10s)
#
# A timed-out request may still be accepted by the provider afterwards (the mock sleeps, then answers
# 202), so duplicates by Idempotency-Key are expected here and only reported (at-least-once).
set -euo pipefail
# shellcheck source=scripts/lib.sh
source "$(dirname "$0")/lib.sh"

SLOW_TOTAL="${SLOW_TOTAL:-300}"
SLOW_SECONDS="${SLOW_SECONDS:-90}"
PERM_TOTAL="${PERM_TOTAL:-20}"
WORK="$(mktemp -d)"
trap 'mock_healthy || true; rm -rf "$WORK"' EXIT

prom_max() { # prom_max <promql>: the scalar value of an instant query ("" if none)
  curl -s --max-time 10 "${PROM_URL}/api/v1/query" --data-urlencode "query=$1" |
    { grep -oE '"value":\[[0-9.]+,"[^"]+"\]' || true; } | head -n 1 | sed -E 's/.*,"([^"]+)"\]/\1/'
}

require_stack
START="$(now_utc)"
t0=$(date +%s)
log "provider-timeout run ${RUN_TAG} since ${START}"

# --- Part 1: slow provider -------------------------------------------------------------------------
mock_config '{"latencyMs":12000,"latencyJitterMs":0,"failureRate":0,"rate429":0,"permanentRate":0}'
slow_id="$(submit_batch sms normal "$SLOW_TOTAL" 0)"
[[ -n "$slow_id" ]] || { echo "slow batch rejected" >&2; exit 1; }
log "batch ${slow_id}: provider latency 12s for ${SLOW_SECONDS}s"
while (($(date +%s) < t0 + SLOW_SECONDS)); do sleep 5; done
mock_healthy
log "provider healthy again; waiting for the batch to finish"
wait_open_zero "batch_id = '${slow_id}'" 300 || true

where="notification_id IN (SELECT id FROM notifications WHERE batch_id = '${slow_id}')"
read -r timeouts deferrals deferrals_counted <<<"$(psql_q "
  SELECT COUNT(*) FILTER (WHERE outcome = 'transient' AND http_status IS NULL),
         COUNT(*) FILTER (WHERE outcome = 'circuit_open'),
         COUNT(*) FILTER (WHERE outcome = 'circuit_open' AND counted)
  FROM delivery_attempts WHERE ${where};")"
read -r sent failed max_attempts mismatched <<<"$(psql_q "
  SELECT COUNT(*) FILTER (WHERE n.status = 'sent'), COUNT(*) FILTER (WHERE n.status = 'failed'),
         COALESCE(MAX(n.attempts), 0),
         COUNT(*) FILTER (WHERE n.attempts <> (SELECT COUNT(*) FROM delivery_attempts a
                                               WHERE a.notification_id = n.id AND a.counted))
  FROM notifications n WHERE n.batch_id = '${slow_id}';")"
breaker_peak="$(prom_max "max(max_over_time(circuit_breaker_state{channel=\"sms\"}[$(($(date +%s) - t0 + 30))s]))")"

check "timeouts_happened" "$((timeouts > 0 ? 0 : 1))" "${timeouts} provider timeouts recorded as transient"
check "breaker_opened" "$([[ "$breaker_peak" == "2" ]] && echo 0 || echo 1)" "peak circuit_breaker_state{channel=sms} = ${breaker_peak:-none} (2 = open)"
check "deferrals_not_counted" "$((deferrals > 0 && deferrals_counted == 0 ? 0 : 1))" "${deferrals} circuit_open deferrals, ${deferrals_counted} counted as attempts"
check "attempts_match_counted_rows" "$((mismatched == 0 ? 0 : 1))" "${mismatched} notifications whose attempts differ from their counted rows; max attempts ${max_attempts}"
check "slow_all_sent" "$((sent == SLOW_TOTAL && failed == 0 ? 0 : 1))" "${sent}/${SLOW_TOTAL} sent, ${failed} failed"
breaker_now="$(prom_max 'max(circuit_breaker_state{channel="sms"})')"
check "breaker_closed_again" "$([[ "$breaker_now" == "0" ]] && echo 0 || echo 1)" "circuit_breaker_state{channel=sms} = ${breaker_now:-none}"

# --- Part 2: permanent errors ----------------------------------------------------------------------
mock_config '{"latencyMs":0,"latencyJitterMs":0,"failureRate":0,"rate429":0,"permanentRate":1}'
perm_id="$(submit_batch sms normal "$PERM_TOTAL" 1)"
[[ -n "$perm_id" ]] || { echo "permanent batch rejected" >&2; exit 1; }
wait_open_zero "batch_id = '${perm_id}'" 60 || true
mock_healthy
read -r pfailed pone pperm pdefer <<<"$(psql_q "
  SELECT COUNT(*) FILTER (WHERE n.status = 'failed'),
         COUNT(*) FILTER (WHERE n.attempts = 1),
         (SELECT COUNT(*) FROM delivery_attempts a JOIN notifications m ON m.id = a.notification_id
           WHERE m.batch_id = '${perm_id}' AND a.outcome = 'permanent' AND a.counted),
         (SELECT COUNT(*) FROM delivery_attempts a JOIN notifications m ON m.id = a.notification_id
           WHERE m.batch_id = '${perm_id}' AND a.outcome <> 'permanent')
  FROM notifications n WHERE n.batch_id = '${perm_id}';")"
psql_q "SELECT id FROM notifications WHERE batch_id = '${perm_id}';" | sort >"$WORK/perm_ids"
redis_q LRANGE '{notif}:dlq' 0 -1 | sort >"$WORK/dlq"
in_dlq="$(comm -12 "$WORK/perm_ids" "$WORK/dlq" | grep -c . || true)"
check "permanent_failed_once" "$((pfailed == PERM_TOTAL && pone == PERM_TOTAL && pperm == PERM_TOTAL ? 0 : 1))" \
  "${pfailed}/${PERM_TOTAL} failed, ${pone} with exactly 1 attempt, ${pperm} permanent attempt rows"
check "permanent_no_retry_or_deferral" "$((pdefer == 0 ? 0 : 1))" "${pdefer} non-permanent attempt rows"
check "permanent_in_dlq" "$((in_dlq == PERM_TOTAL ? 0 : 1))" "${in_dlq}/${PERM_TOTAL} in {notif}:dlq"

# Expected duplicates (timed-out requests the mock accepted later) are reported, not failed.
if "${LIB_DIR}/invariants.sh" --since="$START" --allow-duplicates >"$WORK/inv" 2>&1; then rc=0; else rc=1; fi
tail -n 15 "$WORK/inv"
check "invariants" "$rc" "invariants.sh --since=${START} --allow-duplicates"

summary "Provider timeout and permanent errors (${RUN_TAG})"
