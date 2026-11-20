-- WHAT: Complete rollback of 000001_init.up.sql.
-- WHY: Guarantees `make migrate-down` and CI up->down->up checks leave zero leftover tables,
--      indexes, triggers, or trigger functions.
-- EXPECTED OUTPUT EXAMPLE (after `migrate down -all`):
--   \dt -> Did not find any relations.

DROP TRIGGER IF EXISTS trg_notifications_set_updated_at ON notifications;
DROP FUNCTION IF EXISTS set_updated_at();

DROP TABLE IF EXISTS delivery_attempts;
DROP TABLE IF EXISTS idempotency_keys;
DROP TABLE IF EXISTS notifications;
DROP TABLE IF EXISTS batches;

