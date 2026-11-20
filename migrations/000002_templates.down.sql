ALTER TABLE notifications
  DROP COLUMN IF EXISTS template_vars,
  DROP COLUMN IF EXISTS template_id;

DROP TRIGGER IF EXISTS trg_templates_set_updated_at ON templates;
DROP INDEX IF EXISTS idx_templates_created_id;
DROP INDEX IF EXISTS templates_name_active;
DROP TABLE IF EXISTS templates;

