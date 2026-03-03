-- Tenant settings tables for /api/tenants/{slug}/settings/organization and /settings/email.
-- Run once: psql $DATABASE_URL -f docs/SCHEMA_tenant_settings.sql
-- (Or from backend root: python -c "import os; from pathlib import Path; exec(open(Path(os.environ.get('INKSUTE_BACKEND_ROOT', '.')) / 'docs/SCHEMA_tenant_settings.sql').read())"  → use psql instead)

-- Organization profile (company name, address, EIN) — one row per tenant.
CREATE TABLE IF NOT EXISTS tenant_org_profile (
  tenant_slug TEXT PRIMARY KEY,
  company_name TEXT NOT NULL DEFAULT '',
  company_address TEXT NOT NULL DEFAULT '',
  ein TEXT NOT NULL DEFAULT '',
  updated_at TIMESTAMP WITH TIME ZONE DEFAULT now()
);

-- Email/SMTP settings for sending (e.g. invites) — one row per tenant.
CREATE TABLE IF NOT EXISTS tenant_email_settings (
  tenant_slug TEXT PRIMARY KEY,
  provider TEXT NOT NULL DEFAULT 'custom',
  from_name TEXT NOT NULL DEFAULT '',
  from_email TEXT NOT NULL DEFAULT '',
  smtp_host TEXT NOT NULL DEFAULT '',
  smtp_port INT NOT NULL DEFAULT 587,
  tls_mode TEXT NOT NULL DEFAULT 'starttls',
  smtp_username TEXT NOT NULL DEFAULT '',
  smtp_secret_id TEXT NOT NULL DEFAULT '',
  is_enabled BOOLEAN NOT NULL DEFAULT false,
  last_test_status TEXT NOT NULL DEFAULT 'never',
  last_test_at TIMESTAMP WITH TIME ZONE,
  last_test_error TEXT NOT NULL DEFAULT '',
  updated_at TIMESTAMP WITH TIME ZONE DEFAULT now()
);

-- Optional: add comment so future migrations know what these are for
COMMENT ON TABLE tenant_org_profile IS 'Tenant organization profile (company name, address, EIN) — filled from Settings UI';
COMMENT ON TABLE tenant_email_settings IS 'Tenant SMTP/email settings for sending — filled from Settings UI';
