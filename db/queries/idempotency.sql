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

-- Batch item keys in one statement (was one INSERT round trip per item). All rows share one
-- resource_type/response_status/expires_at; a key that is still active is skipped, so the returned
-- keys are exactly the ones claimed and the caller treats any missing key as a conflict.
-- name: InsertIdempotencyKeys :many
INSERT INTO idempotency_keys (
  key, request_hash, resource_type, resource_id, response_status, created_at, expires_at
)
SELECT k.key, k.request_hash, sqlc.arg(resource_type)::text, k.resource_id, sqlc.arg(response_status)::int, now(), sqlc.arg(expires_at)::timestamptz
FROM (
  -- Set-returning functions in one select list advance in lockstep (zip), PostgreSQL >= 10.
  SELECT unnest(sqlc.arg(keys)::text[]) AS key,
         unnest(sqlc.arg(request_hashes)::text[]) AS request_hash,
         unnest(sqlc.arg(resource_ids)::uuid[]) AS resource_id
) AS k
ON CONFLICT (key) DO UPDATE
SET request_hash    = EXCLUDED.request_hash,
    resource_type   = EXCLUDED.resource_type,
    resource_id     = EXCLUDED.resource_id,
    response_status = EXCLUDED.response_status,
    created_at      = now(),
    expires_at      = EXCLUDED.expires_at
WHERE idempotency_keys.expires_at <= now()
RETURNING key;

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
-- = ANY(ARRAY(...)) instead of IN (SELECT ...): with IN, PostgreSQL hash-joins the batch against a
-- sequential scan of the whole table (378 ms at 2.25M keys, growing with the table); the array form
-- probes the primary key for each expired key (29 ms at any size).
-- name: DeleteExpiredIdempotencyKeys :execrows
DELETE FROM idempotency_keys
WHERE key = ANY(ARRAY(
  SELECT key
  FROM idempotency_keys
  WHERE expires_at <= now()
  LIMIT sqlc.arg(batch_limit)::int
));
