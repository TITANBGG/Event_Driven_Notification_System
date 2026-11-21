#!/usr/bin/env bash
# Measures the per-channel send rate (FR-2.2) of everything sent since a given time, from two clocks:
#   system side   delivery_attempts.started_at (taken right before the provider HTTP call), per channel
#   provider side the mock provider's own arrival timestamps (/stats), all channels combined
# The figure that matters is the busiest sliding 1-second window: CLAUDE.md allows at most
# RATE_LIMIT_PER_SEC sends in any such window (+1 at an exact bucket boundary).
#
#   ./scripts/ratecheck.sh --since=2026-11-21T10:00:00Z [--limit=100] [--channels=1]
#
# --channels is how many channels the run used. The provider side cannot tell channels apart, so its
# limit is limit*channels; run a single-channel load (--channels=1) for an exact provider-side check.
set -euo pipefail

if [[ -n "${COMPOSE:-}" ]]; then read -r -a COMPOSE_CMD <<<"${COMPOSE}"; else COMPOSE_CMD=(docker compose); fi
MOCK_URL="${MOCK_URL:-http://localhost:8090}"
SINCE="" LIMIT=100 CHANNELS=3
for arg in "$@"; do
  case "$arg" in
    --since=*) SINCE="${arg#--since=}" ;;
    --limit=*) LIMIT="${arg#--limit=}" ;;
    --channels=*) CHANNELS="${arg#--channels=}" ;;
    *) echo "unknown argument: $arg" >&2; exit 2 ;;
  esac
done
# SINCE is interpolated into SQL, so accept only an RFC3339 timestamp.
if [[ ! "$SINCE" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9:.]+(Z|[+-][0-9:]+)$ ]]; then
  echo "usage: $0 --since=<RFC3339> [--limit=100] [--channels=1|2|3]" >&2
  exit 2
fi
[[ "$LIMIT" =~ ^[0-9]+$ && "$CHANNELS" =~ ^[1-3]$ ]] || { echo "invalid --limit or --channels" >&2; exit 2; }

psql_in() { "${COMPOSE_CMD[@]}" exec -T postgres psql -U notif -d notif -qAt -F ' ' "$@" | tr -d '\r'; }

# RANGE ... PRECEDING counts every send in (t - 1s, t] for each send time t; the maximum over all t is
# the busiest half-open 1-second window.
WINDOW_SQL="COUNT(*) OVER (PARTITION BY grp ORDER BY ts RANGE BETWEEN INTERVAL '999999 microseconds' PRECEDING AND CURRENT ROW)"

FAILURES=0
echo "=== Rate check since ${SINCE} (limit ${LIMIT}/s per channel) ==="
printf '%-9s %-8s %8s %9s %10s %12s  %s\n' side channel sends span_s avg_per_s max_1s_window verdict

while read -r ch sends span avg maxw; do
  [[ -z "${ch:-}" ]] && continue
  verdict=PASS
  if ((maxw > LIMIT + 1)); then verdict=FAIL; FAILURES=$((FAILURES + 1)); fi
  printf '%-9s %-8s %8s %9s %10s %12s  %s\n' system "$ch" "$sends" "$span" "$avg" "$maxw" "$verdict"
done < <(psql_in -c "
  WITH s AS (
    SELECT n.channel AS grp, a.started_at AS ts
    FROM delivery_attempts a JOIN notifications n ON n.id = a.notification_id
    WHERE a.outcome IN ('sent', 'transient', 'permanent') AND a.started_at >= '${SINCE}'::timestamptz
  ), w AS (SELECT grp, ts, ${WINDOW_SQL} AS c FROM s)
  SELECT grp, COUNT(*),
         ROUND(EXTRACT(EPOCH FROM MAX(ts) - MIN(ts))::numeric, 1),
         ROUND((COUNT(*) / GREATEST(EXTRACT(EPOCH FROM MAX(ts) - MIN(ts)), 0.001))::numeric, 1),
         MAX(c)
  FROM w GROUP BY grp ORDER BY grp;")

# Provider side: load the mock's arrival timestamps into a temp table and run the same window query.
provider_limit=$((LIMIT * CHANNELS))
stamps="$(curl -fsS --max-time 30 "${MOCK_URL}/stats" | grep -oE '"timestamps":\[[^]]*\]' | grep -oE '[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9:.]+Z' || true)"
if [[ -z "$stamps" ]]; then
  echo "provider  (all)    no timestamps in mock /stats (reset since the run?)"
else
  read -r sends span avg maxw < <({
    echo "CREATE TEMP TABLE t(ts timestamptz);"
    echo "COPY t FROM STDIN;"
    printf '%s\n' "$stamps"
    echo '\.'
    echo "WITH s AS (SELECT 'all'::text AS grp, ts FROM t WHERE ts >= '${SINCE}'::timestamptz),
               w AS (SELECT ts, ${WINDOW_SQL} AS c FROM s)
          SELECT COUNT(*), ROUND(EXTRACT(EPOCH FROM MAX(ts) - MIN(ts))::numeric, 1),
                 ROUND((COUNT(*) / GREATEST(EXTRACT(EPOCH FROM MAX(ts) - MIN(ts)), 0.001))::numeric, 1),
                 COALESCE(MAX(c), 0) FROM w;"
  } | psql_in)
  verdict=PASS
  if ((maxw > provider_limit + 1)); then verdict=FAIL; FAILURES=$((FAILURES + 1)); fi
  printf '%-9s %-8s %8s %9s %10s %12s  %s (limit %s = %s x %s channel(s))\n' \
    provider all "$sends" "$span" "$avg" "$maxw" "$verdict" "$provider_limit" "$LIMIT" "$CHANNELS"
fi

if ((FAILURES > 0)); then echo "RATE CHECK FAILED (${FAILURES})" >&2; exit 1; fi
echo "RATE CHECK PASSED"
