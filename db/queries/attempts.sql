-- WHAT: Audit log queries recording every delivery attempt, circuit deferral, and visibility abandonment.
-- WHY: Separating delivery_attempts from notifications preserves full retry history (HTTP status,
--      duration, worker ID, error) even after manual replay resets attempts=0.
-- EXPECTED OUTPUT EXAMPLE:
--   ListAttempts($notifID) -> [{attempt_no: 1, outcome: "transient", http_status: 500}, {attempt_no: 2, outcome: "sent", http_status: 202}]

-- name: InsertAttempt :one
INSERT INTO delivery_attempts (
  notification_id, attempt_no, counted, started_at, duration_ms,
  http_status, outcome, error, provider_message_id, worker_id
) VALUES (
  $1, $2, $3, $4, $5,
  $6, $7, $8, $9, $10
)
RETURNING *;

-- name: ListAttempts :many
SELECT *
FROM delivery_attempts
WHERE notification_id = $1
ORDER BY attempt_no ASC, id ASC;

