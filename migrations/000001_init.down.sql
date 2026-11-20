DROP TRIGGER IF EXISTS trg_notifications_set_updated_at ON notifications;
DROP FUNCTION IF EXISTS set_updated_at();

DROP TABLE IF EXISTS delivery_attempts;
DROP TABLE IF EXISTS idempotency_keys;
DROP TABLE IF EXISTS notifications;
DROP TABLE IF EXISTS batches;
