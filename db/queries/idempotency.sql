-- WHAT: Idempotency key queries for single/batch API deduplication and batched reconciler expiration cleanup.
-- WHY: Inserting with ON CONFLICT DO NOTHING in the same transaction serializes concurrent duplicate
--      requests at PostgreSQL row lock level without race conditions.
-- EXPECTED OUTPUT EXAMPLE:
--   InsertIdempotencyKey(...) -> returns inserted row on first call, or pgx.ErrNoRows on duplicate key conflict.

-- name: InsertIdempotencyKey :one
INSERT INTO idempotency_keys (
  key, request_hash, resource_type, resource_id, response_status, expires_at
) VALUES (
  $1, $2, $3, $4, $5, $6
)
ON CONFLICT (key) DO NOTHING
RETURNING *;

-- name: GetIdempotencyKey :one
SELECT *
FROM idempotency_keys
WHERE key = $1
  AND expires_at > now();

-- name: GetIdempotencyKeysByKeys :many
SELECT *
FROM idempotency_keys
WHERE key = ANY(sqlc.arg(keys)::text[])
  AND expires_at > now();

-- name: DeleteExpiredIdempotencyKeys :execrows
DELETE FROM idempotency_keys
WHERE key IN (
  SELECT key
  FROM idempotency_keys
  WHERE expires_at < now()
  LIMIT sqlc.arg(batch_limit)::int
);

