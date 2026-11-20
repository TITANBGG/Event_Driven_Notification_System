-- Reclaim expired keys atomically on conflict; active keys return 0 rows so the handler can replay or return 409.

-- name: InsertIdempotencyKey :one
INSERT INTO idempotency_keys (
  key, request_hash, resource_type, resource_id, response_status, created_at, expires_at
) VALUES (
  $1, $2, $3, $4, $5, now(), $6
)
ON CONFLICT (key) DO UPDATE
SET request_hash    = EXCLUDED.request_hash,
    resource_type   = EXCLUDED.resource_type,
    resource_id     = EXCLUDED.resource_id,
    response_status = EXCLUDED.response_status,
    created_at      = now(),
    expires_at      = EXCLUDED.expires_at
WHERE idempotency_keys.expires_at <= now()
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

-- Batched delete avoids holding long table locks during reconciler cleanup.
-- name: DeleteExpiredIdempotencyKeys :execrows
DELETE FROM idempotency_keys
WHERE key IN (
  SELECT key
  FROM idempotency_keys
  WHERE expires_at <= now()
  LIMIT sqlc.arg(batch_limit)::int
);
