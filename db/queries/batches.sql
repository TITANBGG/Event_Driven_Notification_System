-- WHAT: Batch management SQL queries for creation, summary aggregation, keyset listing, and cancellation.
-- WHY: Computing batch counts via GROUP BY status and cancelling items in a single atomic UPDATE
--      avoids N+1 queries and prevents cancel-vs-worker races across up to 1000 items.
-- EXPECTED OUTPUT EXAMPLE:
--   GetBatchSummary($batchID) -> rows: [{status: "pending", count: 10}, {status: "sent", count: 990}]

-- name: InsertBatch :one
INSERT INTO batches (id, total, correlation_id)
VALUES ($1, $2, $3)
RETURNING *;

-- name: GetBatch :one
SELECT *
FROM batches
WHERE id = $1;

-- name: GetBatchSummary :many
SELECT status, count(*)::bigint AS count
FROM notifications
WHERE batch_id = $1
GROUP BY status;

-- name: ListBatchItems :many
SELECT *
FROM notifications
WHERE batch_id = sqlc.arg(batch_id)::uuid
  AND (
    sqlc.narg(cursor_created_at)::timestamptz IS NULL
    OR (created_at, id) < (sqlc.narg(cursor_created_at)::timestamptz, sqlc.narg(cursor_id)::uuid)
  )
ORDER BY created_at DESC, id DESC
LIMIT sqlc.arg(page_limit)::int;

-- name: CancelBatch :many
UPDATE notifications
SET status = 'cancelled',
    cancelled_at = now(),
    updated_at = now()
WHERE batch_id = $1
  AND status IN ('scheduled', 'pending', 'retrying')
RETURNING id;

