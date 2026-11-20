-- name: InsertTemplate :one
INSERT INTO templates (
  id, name, channel, subject, title, body, variables
) VALUES (
  $1, $2, $3, $4, $5, $6, $7
)
RETURNING *;

-- name: GetTemplate :one
SELECT *
FROM templates
WHERE id = $1
  AND deleted_at IS NULL;

-- name: GetTemplatesByIDs :many
SELECT *
FROM templates
WHERE id = ANY(sqlc.arg(ids)::uuid[])
  AND deleted_at IS NULL;

-- name: ListTemplates :many
SELECT *
FROM templates
WHERE deleted_at IS NULL
  AND (sqlc.narg(channel)::text IS NULL OR channel = sqlc.narg(channel)::text)
  AND (
    sqlc.narg(cursor_created_at)::timestamptz IS NULL
    OR (created_at, id) < (sqlc.narg(cursor_created_at)::timestamptz, sqlc.narg(cursor_id)::uuid)
  )
ORDER BY created_at DESC, id DESC
LIMIT sqlc.arg(page_limit)::int;

-- name: UpdateTemplate :one
UPDATE templates
SET name = $2,
    channel = $3,
    subject = $4,
    title = $5,
    body = $6,
    variables = $7,
    updated_at = now()
WHERE id = $1
  AND deleted_at IS NULL
RETURNING *;

-- name: SoftDeleteTemplate :one
UPDATE templates
SET deleted_at = now(),
    updated_at = now()
WHERE id = $1
  AND deleted_at IS NULL
RETURNING id;

