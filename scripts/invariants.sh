#!/usr/bin/env bash
# Shared post-test system invariant verifier for stress and chaos suites.
# Fails immediately (exit 1) if any PostgreSQL, Redis, or MockProvider invariant is violated.
#
# Usage:
#   ./scripts/invariants.sh [--allow-duplicates] [--allow-scheduled] [--since=<RFC3339>]
#
#   --since limits the duplicate check (6) to notifications created at or after that time, so a
#   duplicate allowed in an earlier drill does not fail later ones. Every other check always covers
#   the full history, because a stuck row or a delivered cancellation is never legitimate.
#
# Environment overrides:
#   COMPOSE       Compose command (default: auto-detected docker compose / docker-compose)
#   POSTGRES_USER PostgreSQL user (default: notif)
#   POSTGRES_DB   PostgreSQL database (default: notif)
#   MOCK_URL      MockProvider base URL (default: http://localhost:8090)

set -euo pipefail

if [[ -n "${COMPOSE:-}" ]]; then
  read -r -a COMPOSE_CMD <<<"${COMPOSE}"
elif docker compose version >/dev/null 2>&1; then
  COMPOSE_CMD=(docker compose)
else
  COMPOSE_CMD=(docker-compose)
fi

POSTGRES_USER="${POSTGRES_USER:-notif}"
POSTGRES_DB="${POSTGRES_DB:-notif}"
MOCK_URL="${MOCK_URL:-http://localhost:8090}"

ALLOW_DUPLICATES=0
ALLOW_SCHEDULED=0
SINCE=""

for arg in "$@"; do
  case "$arg" in
    --allow-duplicates) ALLOW_DUPLICATES=1 ;;
    --allow-scheduled)  ALLOW_SCHEDULED=1 ;;
    --since=*)
      SINCE="${arg#--since=}"
      # Interpolated into SQL below, so accept only timestamp characters.
      if [[ ! "$SINCE" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9:.]+(Z|[+-][0-9:]+)$ ]]; then
        echo "invalid --since (want RFC3339, e.g. 2026-11-21T10:00:00Z): $SINCE" >&2
        exit 2
      fi
      ;;
    -h|--help)
      sed -n '2,17p' "$0"
      exit 0
      ;;
    *)
      echo "unknown argument: $arg" >&2
      exit 2
      ;;
  esac
done

FAILURES=0
RESULTS=()

record() {
  local name="$1" status="$2" detail="$3"
  RESULTS+=("${name}|${status}|${detail}")
  if [[ "$status" == "FAIL" ]]; then
    FAILURES=$((FAILURES + 1))
  fi
}

pg_scalar() {
  "${COMPOSE_CMD[@]}" exec -T postgres psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -Atc "$1" | tr -d '\r' | head -n1
}

pg_list() {
  "${COMPOSE_CMD[@]}" exec -T postgres psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -Atc "$1" | tr -d '\r'
}

redis_cmd() {
  "${COMPOSE_CMD[@]}" exec -T redis redis-cli --raw "$@" | tr -d '\r'
}

# Pre-flight: dependencies must be reachable so we never silently pass against dead containers.
if ! pg_scalar "SELECT 1;" >/dev/null 2>&1; then
  echo "ERROR: PostgreSQL is unreachable via ${COMPOSE_CMD[*]} exec -T postgres" >&2
  exit 2
fi
if [[ "$(redis_cmd PING 2>/dev/null || true)" != "PONG" ]]; then
  echo "ERROR: Redis is unreachable via ${COMPOSE_CMD[*]} exec -T redis" >&2
  exit 2
fi

TMP_DIR="$(mktemp -d)"
trap 'rm -rf "$TMP_DIR"' EXIT

# Download to a file instead of capturing into a shell variable: at ~900k keys /stats is ~40 MB,
# and MSYS2/Git Bash command-substitution buffering makes curl hit --max-time before finishing.
curl -fsS --max-time 60 -o "${TMP_DIR}/stats.json" "${MOCK_URL}/stats" || {
  echo "ERROR: MockProvider /stats unreachable at ${MOCK_URL}/stats" >&2
  exit 2
}

# 1. No stuck pending / processing / retrying / overdue scheduled notifications.
stuck_active="$(pg_scalar "SELECT COUNT(*) FROM notifications WHERE status IN ('pending', 'processing', 'retrying');")"
if [[ "$ALLOW_SCHEDULED" -eq 1 ]]; then
  stuck_scheduled="$(pg_scalar "SELECT COUNT(*) FROM notifications WHERE status = 'scheduled' AND scheduled_at <= now() - interval '5 seconds';")"
  sched_label="overdue scheduled (>5s)"
else
  stuck_scheduled="$(pg_scalar "SELECT COUNT(*) FROM notifications WHERE status = 'scheduled';")"
  sched_label="scheduled"
fi
if [[ "$stuck_active" -eq 0 && "$stuck_scheduled" -eq 0 ]]; then
  record "1. Stuck notifications" "PASS" "pending/processing/retrying=0, ${sched_label}=0"
else
  breakdown="$(pg_list "SELECT status || ':' || COUNT(*) FROM notifications WHERE status IN ('pending','processing','retrying','scheduled') GROUP BY status ORDER BY status;" | paste -sd ',' -)"
  record "1. Stuck notifications" "FAIL" "active=${stuck_active}, ${sched_label}=${stuck_scheduled} (${breakdown:-none})"
fi

# 2. attempts must never exceed max_attempts (or go negative).
bad_attempts="$(pg_scalar "SELECT COUNT(*) FROM notifications WHERE attempts > max_attempts OR attempts < 0;")"
if [[ "$bad_attempts" -eq 0 ]]; then
  record "2. Attempts bounds" "PASS" "0 rows with attempts > max_attempts or attempts < 0"
else
  record "2. Attempts bounds" "FAIL" "${bad_attempts} rows violate 0 <= attempts <= max_attempts"
fi

# 3. Every sent row has provider_message_id and sent_at populated.
incomplete_sent="$(pg_scalar "SELECT COUNT(*) FROM notifications WHERE status = 'sent' AND (provider_message_id IS NULL OR provider_message_id = '' OR sent_at IS NULL);")"
sent_total="$(pg_scalar "SELECT COUNT(*) FROM notifications WHERE status = 'sent';")"
if [[ "$incomplete_sent" -eq 0 ]]; then
  record "3. Sent metadata complete" "PASS" "${sent_total} sent rows all have provider_message_id and sent_at"
else
  record "3. Sent metadata complete" "FAIL" "${incomplete_sent}/${sent_total} sent rows missing provider_message_id or sent_at"
fi

# Parse MockProvider /stats byKey entries: "<uuid> <count>" per line. grep -o is linear; the former awk
# loop re-copied the rest of the single-line JSON after every match, which took 20+ minutes at 500k keys.
{ grep -oE '"[0-9a-fA-F-]{36}":[0-9]+' "${TMP_DIR}/stats.json" || true; } \
  | tr -d '"' | awk -F: '$2 + 0 > 0 { print $1 " " $2 }' > "${TMP_DIR}/mock_by_key.txt"

mock_window="$(wc -l < "${TMP_DIR}/mock_by_key.txt" | tr -d ' ')"

# Provider calls the system itself recorded, per notification. Only these outcomes mean an HTTP
# request reached (or may have reached) the provider; circuit_open and abandoned rows never sent one.
# Format: "<uuid> <recorded_calls>".
pg_list "SELECT notification_id::text || ' ' || COUNT(*) FROM delivery_attempts
         WHERE outcome IN ('sent', 'transient', 'permanent') GROUP BY notification_id;" \
  | sort -k1,1 > "${TMP_DIR}/pg_recorded_calls.txt" || true

# Provider calls the system did NOT record: mock count minus recorded attempts, per ID, never negative.
# A positive value means a worker reached the provider and then died before InsertAttempt (SIGKILL),
# i.e. a real at-least-once duplicate risk. A negative value only means the mock was reset (its
# /stats are in memory) after some attempts, so it is clamped to zero. Format: "<uuid> <unrecorded>".
sort -k1,1 "${TMP_DIR}/mock_by_key.txt" \
  | join -a1 -e 0 -o '0,1.2,2.2' - "${TMP_DIR}/pg_recorded_calls.txt" \
  | awk '{ d = $2 - $3; if (d > 0) print $1 " " d }' > "${TMP_DIR}/unrecorded_calls.txt"

pg_list "SELECT id::text FROM notifications WHERE status = 'cancelled';" | grep -E '^[0-9a-fA-F-]+$' | sort -u > "${TMP_DIR}/pg_cancelled.txt" || true
cancelled_total="$(wc -l < "${TMP_DIR}/pg_cancelled.txt" | tr -d ' ')"

# 4. No cancelled notification was ever delivered.
#    Primary (complete history): PostgreSQL has no 'sent' attempt for a cancelled row. Cancelled rows
#    may legitimately have earlier transient attempts (retrying -> cancelled), so only 'sent' counts.
#    Secondary (mock window only): no cancelled ID received a provider call the system did not record.
cancelled_sent_pg="$(pg_scalar "SELECT COUNT(DISTINCT n.id) FROM notifications n JOIN delivery_attempts a ON a.notification_id = n.id
                                WHERE n.status = 'cancelled' AND a.outcome = 'sent';")"
cut -d' ' -f1 "${TMP_DIR}/unrecorded_calls.txt" | sort -u | comm -12 - "${TMP_DIR}/pg_cancelled.txt" > "${TMP_DIR}/cancelled_unrecorded.txt"
cancelled_unrecorded="$(wc -l < "${TMP_DIR}/cancelled_unrecorded.txt" | tr -d ' ')"
if [[ "$mock_window" -eq 0 ]]; then
  mock_note="provider cross-check skipped: mock /stats is empty (reset since the last send)"
else
  mock_note="provider cross-check over ${mock_window} IDs: ${cancelled_unrecorded} unrecorded calls to cancelled IDs"
fi
if [[ "$cancelled_sent_pg" -eq 0 && "$cancelled_unrecorded" -eq 0 ]]; then
  record "4. Cancelled never sent" "PASS" "0/${cancelled_total} cancelled rows have a 'sent' attempt; ${mock_note}"
else
  sample="$(head -n 5 "${TMP_DIR}/cancelled_unrecorded.txt" | paste -sd ',' -)"
  record "4. Cancelled never sent" "FAIL" "pg_sent_attempts_on_cancelled=${cancelled_sent_pg}; ${mock_note}${sample:+ (sample: ${sample})}"
fi

# 5. Redis <-> PostgreSQL state consistency.
redis_mismatches=0
mismatch_notes=()

# 5a. Ready queues must be drained when system is settled.
ready_nonempty=0
for ch in sms email push; do
  for pr in high normal low; do
    qkey="{notif}:q:${ch}:${pr}"
    qlen="$(redis_cmd LLEN "$qkey")"
    if [[ "$qlen" -gt 0 ]]; then
      ready_nonempty=$((ready_nonempty + qlen))
    fi
  done
done
if [[ "$ready_nonempty" -gt 0 ]]; then
  redis_mismatches=$((redis_mismatches + ready_nonempty))
  mismatch_notes+=("ready_queues=${ready_nonempty}")
fi

# 5b. Inflight lists must be drained.
inflight_nonempty=0
while IFS= read -r ikey; do
  [[ -z "$ikey" ]] && continue
  ilen="$(redis_cmd LLEN "$ikey")"
  if [[ "$ilen" -gt 0 ]]; then
    inflight_nonempty=$((inflight_nonempty + ilen))
  fi
done < <(redis_cmd KEYS '{notif}:inflight:*')
if [[ "$inflight_nonempty" -gt 0 ]]; then
  redis_mismatches=$((redis_mismatches + inflight_nonempty))
  mismatch_notes+=("inflight_items=${inflight_nonempty}")
fi

# 5c. {notif}:msg hash must not hold terminal IDs.
redis_cmd HKEYS '{notif}:msg' | grep -E '^[0-9a-fA-F-]+$' | sort -u > "${TMP_DIR}/redis_msg_ids.txt" || true
pg_list "SELECT id::text FROM notifications WHERE status IN ('sent', 'failed', 'cancelled');" | grep -E '^[0-9a-fA-F-]+$' | sort -u > "${TMP_DIR}/pg_terminal.txt" || true
stale_msg_ids="$(comm -12 "${TMP_DIR}/redis_msg_ids.txt" "${TMP_DIR}/pg_terminal.txt" | wc -l | tr -d ' ')"
if [[ "$stale_msg_ids" -gt 0 ]]; then
  redis_mismatches=$((redis_mismatches + stale_msg_ids))
  mismatch_notes+=("stale_msg_hash_ids=${stale_msg_ids}")
fi

# 5d. {notif}:delayed ZSET and PG status='scheduled' must match exactly.
redis_cmd ZRANGE '{notif}:delayed' 0 -1 | grep -E '^[0-9a-fA-F-]+$' | sort -u > "${TMP_DIR}/redis_delayed.txt" || true
pg_list "SELECT id::text FROM notifications WHERE status = 'scheduled';" | grep -E '^[0-9a-fA-F-]+$' | sort -u > "${TMP_DIR}/pg_scheduled.txt" || true
delayed_only_redis="$(comm -23 "${TMP_DIR}/redis_delayed.txt" "${TMP_DIR}/pg_scheduled.txt" | wc -l | tr -d ' ')"
delayed_only_pg="$(comm -13 "${TMP_DIR}/redis_delayed.txt" "${TMP_DIR}/pg_scheduled.txt" | wc -l | tr -d ' ')"
if [[ "$delayed_only_redis" -gt 0 || "$delayed_only_pg" -gt 0 ]]; then
  redis_mismatches=$((redis_mismatches + delayed_only_redis + delayed_only_pg))
  mismatch_notes+=("delayed_diff(redis_only=${delayed_only_redis},pg_only=${delayed_only_pg})")
fi

# 5e. Every ID in {notif}:dlq must still have status='failed' in PostgreSQL.
redis_cmd LRANGE '{notif}:dlq' 0 -1 | grep -E '^[0-9a-fA-F-]+$' | sort -u > "${TMP_DIR}/redis_dlq.txt" || true
pg_list "SELECT id::text FROM notifications WHERE status = 'failed';" | grep -E '^[0-9a-fA-F-]+$' | sort -u > "${TMP_DIR}/pg_failed.txt" || true
dlq_not_failed="$(comm -23 "${TMP_DIR}/redis_dlq.txt" "${TMP_DIR}/pg_failed.txt" | wc -l | tr -d ' ')"
if [[ "$dlq_not_failed" -gt 0 ]]; then
  redis_mismatches=$((redis_mismatches + dlq_not_failed))
  mismatch_notes+=("dlq_non_failed=${dlq_not_failed}")
fi

if [[ "$redis_mismatches" -eq 0 ]]; then
  record "5. Redis-PG consistency" "PASS" "0 mismatches (ready=0, inflight=0, stale_msg=0, delayed_diff=0, dlq_bad=0)"
else
  joined_notes="$(IFS=', '; echo "${mismatch_notes[*]}")"
  record "5. Redis-PG consistency" "FAIL" "${redis_mismatches} mismatches (${joined_notes})"
fi

# 6. Duplicate deliveries.
#    Retries after a 5xx/timeout are expected provider calls and are NOT duplicates; a duplicate is a
#    second delivery of a message the provider already accepted. Two sources:
#    a) PostgreSQL (complete history): more than one 'sent' attempt for the same notification.
#    b) Mock window: provider calls the system never recorded (worker died after the provider accepted).
if [[ -n "$SINCE" ]]; then
  since_filter="AND n.created_at >= '${SINCE}'::timestamptz"
  scope_note="notifications created since ${SINCE}"
else
  since_filter=""
  scope_note="all notifications"
fi
dup_sent_pg="$(pg_scalar "SELECT COALESCE(SUM(c - 1), 0) FROM (SELECT COUNT(*) AS c FROM delivery_attempts a
                          JOIN notifications n ON n.id = a.notification_id
                          WHERE a.outcome = 'sent' ${since_filter} GROUP BY a.notification_id HAVING COUNT(*) > 1) d;")"
unrecorded_total="$(awk '{ s += $2 } END { print s + 0 }' "${TMP_DIR}/unrecorded_calls.txt")"
unrecorded_ids="$(wc -l < "${TMP_DIR}/unrecorded_calls.txt" | tr -d ' ')"
abandoned="$(pg_scalar "SELECT COUNT(*) FROM delivery_attempts a JOIN notifications n ON n.id = a.notification_id
                        WHERE a.outcome = 'abandoned' ${since_filter};")"
if [[ "$mock_window" -eq 0 ]]; then
  dup_mock_note="unrecorded calls: not checked (mock /stats empty)"
else
  dup_mock_note="unrecorded provider calls=${unrecorded_total} across ${unrecorded_ids} ID(s) in a ${mock_window}-ID mock window"
fi
dup_detail="${scope_note}: pg double-sent=${dup_sent_pg}, abandoned attempts=${abandoned}; ${dup_mock_note}"
if [[ "$dup_sent_pg" -eq 0 && "$unrecorded_total" -eq 0 ]]; then
  record "6. Duplicate deliveries" "PASS" "$dup_detail"
elif [[ "$ALLOW_DUPLICATES" -eq 1 ]]; then
  record "6. Duplicate deliveries" "PASS" "${dup_detail} (allowed: at-least-once after worker crash)"
else
  sample_dups="$(head -n 5 "${TMP_DIR}/unrecorded_calls.txt" | tr ' ' '=' | paste -sd ',' -)"
  record "6. Duplicate deliveries" "FAIL" "${dup_detail}${sample_dups:+ (sample: ${sample_dups})}"
fi
if [[ "$mock_window" -eq 0 ]]; then
  echo "WARNING: MockProvider /stats is empty, so provider-side cross-checks (4, 6) used PostgreSQL only." >&2
fi

echo "=== System Invariants Report ==="
for row in "${RESULTS[@]}"; do
  IFS='|' read -r name status detail <<<"$row"
  printf '%-28s %-5s %s\n' "$name" "$status" "$detail"
done

if [[ "$FAILURES" -gt 0 ]]; then
  echo "INVARIANTS FAILED (${FAILURES} check(s))" >&2
  exit 1
fi
echo "ALL INVARIANTS PASSED"

