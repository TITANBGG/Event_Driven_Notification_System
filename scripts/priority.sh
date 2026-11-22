#!/usr/bin/env bash
# Priority and starvation (FR-2.3): a low-priority backlog keeps moving while high-priority traffic
# saturates the channel.
#
# 1,000 low SMS are queued first; then high SMS arrive at ~200/s for 3 minutes, twice what the SMS
# channel may send, so the high queue never empties. The weighted 6:3:1 schedule (normal slots fall
# through to high when normal is empty) must still give low about 10% of the sends until low is drained.
#
#   ./scripts/priority.sh             (stack running, mock provider healthy)
#
# PASS requires, in every full 10s slice between the first high send and the last low send: low share
# >= 8% (10% target, minus sampling slack at slice edges); at least 3 such slices; the high queue never
# empty while sampled; every notification sent; high p50 end-to-end latency below low p50.
set -euo pipefail
# shellcheck source=scripts/lib.sh
source "$(dirname "$0")/lib.sh"

LOW_TOTAL="${LOW_TOTAL:-1000}"
HIGH_SECONDS="${HIGH_SECONDS:-180}"
HIGH_PER_2S="${HIGH_PER_2S:-400}"
MIN_LOW_SHARE_PCT="${MIN_LOW_SHARE_PCT:-8}"
WORK="$(mktemp -d)"
trap 'mock_healthy || true; rm -rf "$WORK"' EXIT

require_stack
mock_healthy
START="$(now_utc)"
log "priority run ${RUN_TAG} since ${START}"

id="$(submit_batch sms low "$LOW_TOTAL" 0)"
[[ -n "$id" ]] || { echo "low batch rejected" >&2; exit 1; }
echo "$id" >"$WORK/low"

log "high SMS: ${HIGH_PER_2S} every 2s for ${HIGH_SECONDS}s"
end=$(($(date +%s) + HIGH_SECONDS))
seq=1
while (($(date +%s) < end)); do
  tick=$(($(date +%s) + 2))
  id="$(submit_batch sms high "$HIGH_PER_2S" "$seq")"
  [[ -n "$id" ]] && echo "$id" >>"$WORK/high"
  # The first samples are taken before the first high batch is dequeued at all; skip 4s of warm-up.
  if ((seq > 2)); then redis_q LLEN '{notif}:q:sms:high' >>"$WORK/high_depth"; fi
  seq=$((seq + 1))
  while (($(date +%s) < tick)); do sleep 0.2; done
done
high_batches="$(grep -c . "$WORK/high" || true)"
min_high="$(sort -n "$WORK/high_depth" | head -n 1)"
log "high batches accepted: ${high_batches} of $((seq - 1)); smallest high queue seen: ${min_high}"

low_id="$(cat "$WORK/low")"
high_ids="$(sql_in_list "$WORK/high")"
all_ids="'${low_id}',${high_ids}"
wait_open_zero "batch_id IN (${all_ids})" 900 || true

read -r total sent <<<"$(psql_q "SELECT COUNT(*), COUNT(*) FILTER (WHERE status = 'sent') FROM notifications WHERE batch_id IN (${all_ids});")"
expected=$((LOW_TOTAL + high_batches * HIGH_PER_2S))
check "all_sent" "$((total == expected && sent == expected ? 0 : 1))" "${sent}/${total} sent, ${expected} submitted"
check "high_queue_never_empty" "$((min_high > 0 ? 0 : 1))" "smallest high queue ${min_high}"

# Full 10s slices from the first high send up to the slice that holds the last low send (excluded: it is
# partly empty of low by definition).
psql_q "
  WITH r AS (SELECT priority, sent_at FROM notifications WHERE batch_id IN (${all_ids}) AND status = 'sent'),
  b AS (SELECT (SELECT MIN(sent_at) FROM r WHERE priority = 'high') AS t0,
               (SELECT MAX(sent_at) FROM r WHERE priority = 'low') AS t1)
  SELECT FLOOR(EXTRACT(EPOCH FROM r.sent_at - b.t0) / 10)::int AS slice,
         COUNT(*) FILTER (WHERE priority = 'low'), COUNT(*)
  FROM r, b
  WHERE r.sent_at >= b.t0 AND r.sent_at < b.t0 + FLOOR(EXTRACT(EPOCH FROM b.t1 - b.t0) / 10) * INTERVAL '10 seconds'
  GROUP BY 1 ORDER BY 1;" >"$WORK/slices"
printf '%-6s %6s %6s %7s\n' slice low total low_%
slices=0 worst=100
while read -r slice low tot; do
  pct=$((100 * low / tot))
  printf '%-6s %6s %6s %6s%%\n' "$slice" "$low" "$tot" "$pct"
  slices=$((slices + 1))
  ((pct < worst)) && worst=$pct
done <"$WORK/slices"
check "contended_slices" "$((slices >= 3 ? 0 : 1))" "${slices} full 10s slices with high and low both queued (need >= 3)"
check "low_share_per_slice" "$((slices > 0 && worst >= MIN_LOW_SHARE_PCT ? 0 : 1))" "lowest low share ${worst}% (need >= ${MIN_LOW_SHARE_PCT}%)"

# Overtaking: the first LOW_TOTAL high notifications were created after every low one, so without
# priority they would finish last. Comparing all high against all low would measure the overload instead
# (high arrives at 2x capacity, so its own FIFO backlog keeps growing for the whole run).
read -r p50_first_high p50_low last_first_high last_low p50_all_high <<<"$(psql_q "
  WITH s AS (SELECT priority, created_at, sent_at, id FROM notifications WHERE batch_id IN (${all_ids}) AND status = 'sent'),
  fh AS (SELECT * FROM s WHERE priority = 'high' ORDER BY created_at, id LIMIT ${LOW_TOTAL}),
  lo AS (SELECT * FROM s WHERE priority = 'low')
  SELECT (SELECT ROUND(1000 * EXTRACT(EPOCH FROM percentile_cont(0.5) WITHIN GROUP (ORDER BY sent_at - created_at))) FROM fh),
         (SELECT ROUND(1000 * EXTRACT(EPOCH FROM percentile_cont(0.5) WITHIN GROUP (ORDER BY sent_at - created_at))) FROM lo),
         (SELECT ROUND(EXTRACT(EPOCH FROM MAX(sent_at) - (SELECT MIN(created_at) FROM lo))) FROM fh),
         (SELECT ROUND(EXTRACT(EPOCH FROM MAX(sent_at) - MIN(created_at))) FROM lo),
         (SELECT ROUND(1000 * EXTRACT(EPOCH FROM percentile_cont(0.5) WITHIN GROUP (ORDER BY sent_at - created_at))) FROM s WHERE priority = 'high');")"
check "high_overtakes_older_low" "$((p50_first_high < p50_low && last_first_high < last_low ? 0 : 1))" \
  "first ${LOW_TOTAL} high (created after all low): p50 ${p50_first_high}ms, all sent by +${last_first_high}s; low: p50 ${p50_low}ms, all sent by +${last_low}s (all high p50 ${p50_all_high}ms, backlog-bound)"

if "${LIB_DIR}/ratecheck.sh" --since="$START" --channels=1 >"$WORK/rate" 2>&1; then rc=0; else rc=1; fi
cat "$WORK/rate"
check "rate_limit" "$rc" "ratecheck.sh --since=${START} --channels=1"
if "${LIB_DIR}/invariants.sh" --since="$START" >"$WORK/inv" 2>&1; then rc=0; else rc=1; fi
tail -n 15 "$WORK/inv"
check "invariants" "$rc" "invariants.sh --since=${START}"

summary "Priority and starvation (${RUN_TAG})"
