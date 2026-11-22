-- name: InsertNotification :one
INSERT INTO notifications (
  id, batch_id, channel, priority, status, recipient, subject, title, content,
  idempotency_key, max_attempts, scheduled_at, correlation_id, traceparent,
  template_id, template_vars
) VALUES (
  $1, $2, $3, $4, $5, $6, $7, $8, $9,
  $10, $11, $12, $13, $14,
  $15, $16
)
RETURNING *;

-- name: CopyNotifications :copyfrom
INSERT INTO notifications (
  id, batch_id, channel, priority, status, recipient, subject, title, content,
  idempotency_key, max_attempts, scheduled_at, correlation_id, traceparent,
  template_id, template_vars
) VALUES (
  $1, $2, $3, $4, $5, $6, $7, $8, $9,
  $10, $11, $12, $13, $14,
  $15, $16
);

-- name: GetNotification :one
SELECT *
FROM notifications
WHERE id = $1;

-- name: ListNotifications :many
SELECT *
FROM notifications
WHERE (sqlc.narg(statuses)::text[] IS NULL OR status = ANY(sqlc.narg(statuses)::text[]))
  AND (sqlc.narg(channel)::text IS NULL OR channel = sqlc.narg(channel)::text)
  AND (sqlc.narg(priority)::text IS NULL OR priority = sqlc.narg(priority)::text)
  AND (sqlc.narg(from_time)::timestamptz IS NULL OR created_at >= sqlc.narg(from_time)::timestamptz)
  AND (sqlc.narg(to_time)::timestamptz IS NULL OR created_at < sqlc.narg(to_time)::timestamptz)
  AND (
    sqlc.narg(cursor_created_at)::timestamptz IS NULL
    OR (created_at, id) < (sqlc.narg(cursor_created_at)::timestamptz, sqlc.narg(cursor_id)::uuid)
  )
ORDER BY created_at DESC, id DESC
LIMIT sqlc.arg(page_limit)::int;

-- Skew tolerance bridges minor clock drift between the worker delayed-mover and PostgreSQL now().
-- name: Claim :one
UPDATE notifications
SET status = 'processing',
    attempts = attempts + 1,
    processing_started_at = now(),
    updated_at = now()
WHERE id = sqlc.arg(id)
  AND (
    status = 'pending'
    OR (status = 'retrying' AND next_attempt_at <= now() + make_interval(secs => sqlc.arg(skew_secs)::double precision))
    OR (status = 'scheduled' AND scheduled_at <= now() + make_interval(secs => sqlc.arg(skew_secs)::double precision))
  )
RETURNING *;

-- Fenced on status='processing' AND attempts=$claimed_attempts so slow workers cannot overwrite reclaimed rows.
-- name: MarkSent :one
UPDATE notifications
SET status = 'sent',
    provider_message_id = sqlc.narg(provider_message_id),
    last_error = NULL,
    sent_at = now(),
    updated_at = now()
WHERE id = sqlc.arg(id)
  AND status = 'processing'
  AND attempts = sqlc.arg(claimed_attempts)
RETURNING *;

-- name: ScheduleRetry :one
UPDATE notifications
SET status = 'retrying',
    next_attempt_at = sqlc.arg(next_attempt_at),
    last_error = sqlc.narg(last_error),
    updated_at = now()
WHERE id = sqlc.arg(id)
  AND status = 'processing'
  AND attempts = sqlc.arg(claimed_attempts)
RETURNING *;

-- Circuit-open deferrals roll back the claim increment so breaker outages never exhaust retry budgets.
-- name: DeferCircuitOpen :one
UPDATE notifications
SET status = 'retrying',
    attempts = GREATEST(attempts - 1, 0),
    deferrals = deferrals + 1,
    next_attempt_at = sqlc.arg(next_attempt_at),
    last_error = sqlc.narg(last_error),
    updated_at = now()
WHERE id = sqlc.arg(id)
  AND status = 'processing'
  AND attempts = sqlc.arg(claimed_attempts)
RETURNING *;

-- name: MarkFailed :one
UPDATE notifications
SET status = 'failed',
    last_error = sqlc.narg(last_error),
    failed_at = now(),
    updated_at = now()
WHERE id = sqlc.arg(id)
  AND status = 'processing'
  AND attempts = sqlc.arg(claimed_attempts)
RETURNING *;

-- name: Cancel :one
UPDATE notifications
SET status = 'cancelled',
    cancelled_at = now(),
    updated_at = now()
WHERE id = $1
  AND status IN ('scheduled', 'pending', 'retrying')
RETURNING *;

-- name: ManualRetry :one
UPDATE notifications
SET status = 'pending',
    attempts = 0,
    deferrals = 0,
    last_error = NULL,
    failed_at = NULL,
    next_attempt_at = NULL,
    updated_at = now()
WHERE id = $1
  AND status = 'failed'
RETURNING *;

-- Bulk DLQ replay: resets at most page_limit failed rows (oldest failure first) in one statement.
-- SKIP LOCKED lets two concurrent replays split the work instead of blocking; the outer
-- status = 'failed' guard mirrors ManualRetry's allowed_from set.
-- name: ManualRetryFailedBulk :many
UPDATE notifications
SET status = 'pending',
    attempts = 0,
    deferrals = 0,
    last_error = NULL,
    failed_at = NULL,
    next_attempt_at = NULL,
    updated_at = now()
WHERE id IN (
    SELECT n.id
    FROM notifications n
    WHERE n.status = 'failed'
      AND (sqlc.narg(channel)::text IS NULL OR n.channel = sqlc.narg(channel)::text)
      AND (sqlc.narg(failed_from)::timestamptz IS NULL OR n.failed_at >= sqlc.narg(failed_from)::timestamptz)
      AND (sqlc.narg(failed_to)::timestamptz IS NULL OR n.failed_at < sqlc.narg(failed_to)::timestamptz)
    ORDER BY n.failed_at ASC, n.id ASC
    LIMIT sqlc.arg(page_limit)::int
    FOR UPDATE SKIP LOCKED
  )
  AND status = 'failed'
RETURNING *;

-- Lease extension after a long rate-limit wait, fenced like every post-claim update: zero rows means
-- the reconciler already reclaimed the row and the worker must not send.
-- name: ExtendProcessingLease :execrows
UPDATE notifications
SET processing_started_at = now(),
    updated_at = now()
WHERE id = sqlc.arg(id)
  AND status = 'processing'
  AND attempts = sqlc.arg(claimed_attempts);

-- name: FindStalePending :many
SELECT *
FROM notifications
WHERE status = 'pending'
  AND updated_at < now() - make_interval(secs => sqlc.arg(stale_secs)::double precision)
  AND (sqlc.narg(after_id)::uuid IS NULL OR id > sqlc.narg(after_id)::uuid)
ORDER BY id ASC
LIMIT sqlc.arg(batch_limit)::int;

-- name: FindDelayedDueSoon :many
SELECT *
FROM notifications
WHERE status IN ('retrying', 'scheduled')
  AND updated_at < now() - make_interval(secs => sqlc.arg(stale_secs)::double precision)
  AND COALESCE(next_attempt_at, scheduled_at) <= now() + make_interval(secs => sqlc.arg(lookahead_secs)::double precision)
  AND (sqlc.narg(after_id)::uuid IS NULL OR id > sqlc.narg(after_id)::uuid)
ORDER BY id ASC
LIMIT sqlc.arg(batch_limit)::int;

-- name: FindAbandonedProcessing :many
SELECT *
FROM notifications
WHERE status = 'processing'
  AND processing_started_at < now() - make_interval(secs => sqlc.arg(visibility_secs)::double precision)
  AND (sqlc.narg(after_id)::uuid IS NULL OR id > sqlc.narg(after_id)::uuid)
ORDER BY id ASC
LIMIT sqlc.arg(batch_limit)::int;

-- name: RequeueAbandoned :one
UPDATE notifications
SET status = 'pending',
    processing_started_at = NULL,
    updated_at = now()
WHERE id = sqlc.arg(id)
  AND status = 'processing'
  AND attempts = sqlc.arg(claimed_attempts)
RETURNING *;

-- name: FailAbandoned :one
UPDATE notifications
SET status = 'failed',
    last_error = sqlc.narg(last_error),
    failed_at = now(),
    updated_at = now()
WHERE id = sqlc.arg(id)
  AND status = 'processing'
  AND attempts = sqlc.arg(claimed_attempts)
RETURNING *;
