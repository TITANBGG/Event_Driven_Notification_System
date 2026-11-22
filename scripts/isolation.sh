#!/usr/bin/env bash
# Channel isolation: a saturated SMS channel must not slow down email or push.
#
# 30,000 SMS are submitted at once (5 minutes of work at 100/s), and while that backlog drains, email
# and push each receive a 100-item batch every 2 seconds (~50/s, half of their own limit) for 2 minutes.
# Workers run separate consumers per channel and the GCRA limiter is keyed per channel, so email and push
# should be delivered as if SMS were idle.
#
#   ./scripts/isolation.sh            (stack running, mock provider healthy)
#
# PASS requires: email and push p95 end-to-end < 2s, SMS busiest 1s window <= limit+1, and (to prove the
# condition actually happened) the SMS ready queue never below 5,000 while email/push were measured.
set -euo pipefail
# shellcheck source=scripts/lib.sh
source "$(dirname "$0")/lib.sh"

SMS_TOTAL="${SMS_TOTAL:-30000}"
SIDE_SECONDS="${SIDE_SECONDS:-120}"
SIDE_BATCH="${SIDE_BATCH:-100}"
P95_MAX_MS="${P95_MAX_MS:-2000}"
MIN_SMS_BACKLOG="${MIN_SMS_BACKLOG:-5000}"
WORK="$(mktemp -d)"
trap 'mock_healthy || true; rm -rf "$WORK"' EXIT

require_stack
mock_healthy
START="$(now_utc)"
log "isolation run ${RUN_TAG} since ${START}"

log "submitting ${SMS_TOTAL} SMS"
for ((s = 0; s < SMS_TOTAL / 1000; s++)); do
  id="$(submit_batch sms normal 1000 "$s")"
  [[ -n "$id" ]] || { echo "SMS batch ${s} rejected" >&2; exit 1; }
  echo "$id" >>"$WORK/sms"
done

log "email and push: ${SIDE_BATCH} each every 2s for ${SIDE_SECONDS}s, sampling the SMS queue"
min_backlog=999999999
end=$(($(date +%s) + SIDE_SECONDS))
seq=0
while (($(date +%s) < end)); do
  tick=$(($(date +%s) + 2))
  submit_batch email normal "$SIDE_BATCH" "$seq" >>"$WORK/side" &
  submit_batch push normal "$SIDE_BATCH" "$seq" >>"$WORK/side" &
  depth="$(queue_depth sms)"
  ((depth < min_backlog)) && min_backlog=$depth
  wait
  seq=$((seq + 1))
  while (($(date +%s) < tick)); do sleep 0.2; done
done
# One more sample after the last side batch was accepted: the backlog must still be there.
depth="$(queue_depth sms)"
((depth < min_backlog)) && min_backlog=$depth
side_batches="$(grep -c . "$WORK/side" || true)"
log "side batches accepted: ${side_batches} of $((seq * 2)); smallest SMS backlog seen: ${min_backlog}"

[[ -s "$WORK/side" ]] || { echo "no email/push batch was accepted" >&2; exit 1; }
side_ids="$(sql_in_list "$WORK/side")"
wait_open_zero "batch_id IN (${side_ids})" 120 || true

check "sms_backlog_held" "$((min_backlog >= MIN_SMS_BACKLOG ? 0 : 1))" \
  "smallest SMS ready queue ${min_backlog} (need >= ${MIN_SMS_BACKLOG})"
check "side_batches_accepted" "$((side_batches == seq * 2 ? 0 : 1))" "${side_batches}/$((seq * 2))"

for ch in email push; do
  read -r n sent p50 p95 p99 <<<"$(psql_q "
    SELECT COUNT(*), COUNT(sent_at),
      COALESCE(ROUND(1000 * EXTRACT(EPOCH FROM percentile_cont(0.50) WITHIN GROUP (ORDER BY sent_at - created_at))), -1),
      COALESCE(ROUND(1000 * EXTRACT(EPOCH FROM percentile_cont(0.95) WITHIN GROUP (ORDER BY sent_at - created_at))), -1),
      COALESCE(ROUND(1000 * EXTRACT(EPOCH FROM percentile_cont(0.99) WITHIN GROUP (ORDER BY sent_at - created_at))), -1)
    FROM notifications WHERE batch_id IN (${side_ids}) AND channel = '${ch}';")"
  check "${ch}_all_sent" "$((n > 0 && n == sent ? 0 : 1))" "${sent}/${n} sent"
  check "${ch}_p95_e2e" "$((p95 >= 0 && p95 < P95_MAX_MS ? 0 : 1))" "p50 ${p50}ms p95 ${p95}ms p99 ${p99}ms (need p95 < ${P95_MAX_MS}ms)"
done

sms_open="$(psql_q "SELECT COUNT(*) FROM notifications WHERE batch_id IN ($(sql_in_list "$WORK/sms")) AND status NOT IN ('sent','failed','cancelled');")"
log "SMS still open after the side traffic: ${sms_open}; waiting for the backlog to drain"
wait_open_zero "batch_id IN ($(sql_in_list "$WORK/sms"))" 600 || true
sms_sent="$(psql_q "SELECT COUNT(*) FROM notifications WHERE batch_id IN ($(sql_in_list "$WORK/sms")) AND status = 'sent';")"
check "sms_all_sent" "$((sms_sent == SMS_TOTAL ? 0 : 1))" "${sms_sent}/${SMS_TOTAL}"

if "${LIB_DIR}/ratecheck.sh" --since="$START" >"$WORK/rate" 2>&1; then rc=0; else rc=1; fi
cat "$WORK/rate"
check "rate_limit" "$rc" "ratecheck.sh --since=${START}"
if "${LIB_DIR}/invariants.sh" --since="$START" >"$WORK/inv" 2>&1; then rc=0; else rc=1; fi
tail -n 15 "$WORK/inv"
check "invariants" "$rc" "invariants.sh --since=${START}"

summary "Channel isolation (${RUN_TAG})"
