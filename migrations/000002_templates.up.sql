-- Phase 9 (B-3): Message templates with variable substitution and soft-delete support.

CREATE TABLE templates (
  id uuid PRIMARY KEY,
  name text NOT NULL,
  channel text NOT NULL CHECK (channel IN ('sms', 'email', 'push')),
  subject text NULL,
  title text NULL,
  body text NOT NULL,
  variables text[] NOT NULL DEFAULT '{}',
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  deleted_at timestamptz NULL
);

CREATE UNIQUE INDEX templates_name_active
  ON templates (name)
  WHERE deleted_at IS NULL;

CREATE INDEX idx_templates_created_id
  ON templates (created_at DESC, id DESC)
  WHERE deleted_at IS NULL;

CREATE TRIGGER trg_templates_set_updated_at
  BEFORE UPDATE ON templates
  FOR EACH ROW
  EXECUTE FUNCTION set_updated_at();

ALTER TABLE notifications
  ADD COLUMN template_id uuid NULL REFERENCES templates(id),
  ADD COLUMN template_vars jsonb NULL;

