-- WHAT: Initial PostgreSQL schema, updated_at trigger, and query-specific B-tree/partial indexes.
-- WHY: PostgreSQL acts as the durable source of truth for all notifications, batch counts,
--      delivery attempts, and idempotency keys; text + CHECK avoids PG ENUM ALTER TYPE caveats,
--      and partial indexes keep reconciler/metrics scans fast without indexing terminal rows.
-- EXPECTED OUTPUT EXAMPLE (after `migrate up`):
--   Tables: batches, notifications, delivery_attempts, idempotency_keys
--   Trigger: trg_notifications_set_updated_at on notifications

CREATE TABLE batches (
  id uuid PRIMARY KEY,
  total int NOT NULL CHECK (total BETWEEN 1 AND 1000),
  correlation_id text,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE notifications (
  id uuid PRIMARY KEY,
  batch_id uuid NULL REFERENCES batches(id),
  channel text NOT NULL CHECK (channel IN ('sms', 'email', 'push')),
  priority text NOT NULL CHECK (priority IN ('high', 'normal', 'low')),
  status text NOT NULL CHECK (status IN ('scheduled', 'pending', 'processing', 'retrying', 'sent', 'failed', 'cancelled')),
  recipient text NOT NULL,
  subject text NULL,
  title text NULL,
  content text NOT NULL,
  idempotency_key text NULL, -- informational copy; source of truth is idempotency_keys
  attempts int NOT NULL DEFAULT 0,
  max_attempts int NOT NULL CHECK (max_attempts >= 1),
  deferrals int NOT NULL DEFAULT 0,
  scheduled_at timestamptz NULL,
  next_attempt_at timestamptz NULL,
  processing_started_at timestamptz NULL,
  last_error text NULL,
  provider_message_id text NULL,
  correlation_id text NULL,
  traceparent text NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  sent_at timestamptz NULL,
  failed_at timestamptz NULL,
  cancelled_at timestamptz NULL
);

CREATE TABLE delivery_attempts (
  id bigserial PRIMARY KEY,
  notification_id uuid NOT NULL REFERENCES notifications(id) ON DELETE CASCADE,
  attempt_no int NOT NULL,
  counted boolean NOT NULL DEFAULT true,
  started_at timestamptz NOT NULL,
  duration_ms int NOT NULL,
  http_status int NULL,
  outcome text NOT NULL CHECK (outcome IN ('sent', 'transient', 'permanent', 'circuit_open', 'abandoned')),
  error text NULL,
  provider_message_id text NULL,
  worker_id text NULL
);

CREATE TABLE idempotency_keys (
  key text PRIMARY KEY,
  request_hash text NOT NULL,
  resource_type text NOT NULL CHECK (resource_type IN ('notification', 'batch')),
  resource_id uuid NOT NULL,
  response_status int NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  expires_at timestamptz NOT NULL
);

-- Maintain updated_at automatically on any UPDATE (even manual SQL) so stale detection stays accurate.
CREATE OR REPLACE FUNCTION set_updated_at()
RETURNS trigger AS $$
BEGIN
  NEW.updated_at = now();
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_notifications_set_updated_at
  BEFORE UPDATE ON notifications
  FOR EACH ROW
  EXECUTE FUNCTION set_updated_at();

-- Keyset pagination & filtered listing indexes (ORDER BY created_at DESC, id DESC).
CREATE INDEX idx_notifications_created_id
  ON notifications (created_at DESC, id DESC);

CREATE INDEX idx_notifications_status_created_id
  ON notifications (status, created_at DESC, id DESC);

CREATE INDEX idx_notifications_channel_created_id
  ON notifications (channel, created_at DESC, id DESC);

CREATE INDEX idx_notifications_batch_created_id
  ON notifications (batch_id, created_at DESC, id DESC)
  WHERE batch_id IS NOT NULL;

-- Partial index covering only active/non-terminal rows so reconciler scans remain tiny at millions of rows.
CREATE INDEX idx_notifications_reconciler
  ON notifications (status, updated_at)
  WHERE status IN ('pending', 'processing', 'retrying', 'scheduled');

-- Partial indexes for 1m/5m/15m metrics windows & percentile_cont latency queries.
CREATE INDEX idx_notifications_sent_at
  ON notifications (sent_at)
  WHERE status = 'sent';

CREATE INDEX idx_notifications_failed_at
  ON notifications (failed_at)
  WHERE status = 'failed';

-- Fast attempt timeline lookups for GET /api/v1/notifications/{id}.
CREATE INDEX idx_delivery_attempts_notif_attempt
  ON delivery_attempts (notification_id, attempt_no);

-- Fast batched cleanup of expired idempotency keys in reconciler step (e).
CREATE INDEX idx_idempotency_keys_expires_at
  ON idempotency_keys (expires_at);

