-- ONIX Feed module: recipients, export jobs, export job items.
-- Run after main schema (tenants, works, editions must exist).
-- Uses existing touch_updated_at() from database.sql.

BEGIN;

CREATE TABLE IF NOT EXISTS onix_recipients (
  id                  uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id            uuid NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
  name                 text NOT NULL,
  protocol             text NOT NULL DEFAULT 'sftp',
  host                 text NOT NULL DEFAULT '',
  port                 integer NOT NULL DEFAULT 22,
  username             text NOT NULL DEFAULT '',
  auth_type            text NOT NULL DEFAULT 'password',
  remote_path          text NOT NULL DEFAULT '',
  filename_pattern     text NOT NULL DEFAULT '',
  secret_arn           text NOT NULL DEFAULT '',
  is_active            boolean NOT NULL DEFAULT true,
  extras               jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_by_user_id   uuid NULL,
  created_at           timestamptz NOT NULL DEFAULT now(),
  updated_at           timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_onix_recipients_tenant ON onix_recipients(tenant_id);
CREATE INDEX IF NOT EXISTS idx_onix_recipients_tenant_active ON onix_recipients(tenant_id, is_active);
CREATE UNIQUE INDEX IF NOT EXISTS ux_onix_recipients_tenant_name ON onix_recipients(tenant_id, name);

DROP TRIGGER IF EXISTS trg_touch_onix_recipients ON onix_recipients;
CREATE TRIGGER trg_touch_onix_recipients
BEFORE UPDATE ON onix_recipients
FOR EACH ROW EXECUTE FUNCTION touch_updated_at();


CREATE TABLE IF NOT EXISTS onix_export_jobs (
  id                   uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id            uuid NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
  requested_by_user_id uuid NULL,
  recipient_id         uuid NULL REFERENCES onix_recipients(id) ON DELETE SET NULL,
  export_mode          text NOT NULL,
  export_scope         text NOT NULL DEFAULT 'selected',
  product_count        integer NOT NULL DEFAULT 0,
  selected_isbns       jsonb NOT NULL DEFAULT '[]'::jsonb,
  selected_edition_ids jsonb NOT NULL DEFAULT '[]'::jsonb,
  filters_json         jsonb NOT NULL DEFAULT '{}'::jsonb,
  xml_s3_key           text NOT NULL DEFAULT '',
  xml_checksum         text NOT NULL DEFAULT '',
  status               text NOT NULL DEFAULT 'pending',
  transfer_status      text NOT NULL DEFAULT '',
  transfer_error       text NOT NULL DEFAULT '',
  started_at           timestamptz NULL,
  completed_at         timestamptz NULL,
  created_at           timestamptz NOT NULL DEFAULT now(),
  extras               jsonb NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX IF NOT EXISTS idx_onix_export_jobs_tenant ON onix_export_jobs(tenant_id);
CREATE INDEX IF NOT EXISTS idx_onix_export_jobs_tenant_created ON onix_export_jobs(tenant_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_onix_export_jobs_tenant_status ON onix_export_jobs(tenant_id, status);


CREATE TABLE IF NOT EXISTS onix_export_job_items (
  id               uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  export_job_id    uuid NOT NULL REFERENCES onix_export_jobs(id) ON DELETE CASCADE,
  tenant_id        uuid NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
  work_id          uuid NULL REFERENCES works(id) ON DELETE SET NULL,
  edition_id       uuid NULL REFERENCES editions(id) ON DELETE SET NULL,
  isbn13           text NOT NULL DEFAULT '',
  record_reference text NOT NULL DEFAULT '',
  title            text NOT NULL DEFAULT '',
  product_form     text NOT NULL DEFAULT '',
  status           text NOT NULL DEFAULT 'included',
  validation_json  jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at       timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_onix_export_job_items_job ON onix_export_job_items(export_job_id);
CREATE INDEX IF NOT EXISTS idx_onix_export_job_items_tenant_isbn ON onix_export_job_items(tenant_id, isbn13);
CREATE INDEX IF NOT EXISTS idx_onix_export_job_items_edition ON onix_export_job_items(edition_id);

COMMIT;
