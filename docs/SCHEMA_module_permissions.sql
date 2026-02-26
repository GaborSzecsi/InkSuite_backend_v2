-- Add module_permissions for editor granular access.
-- Run once: psql $DATABASE_URL -f docs/SCHEMA_module_permissions.sql

ALTER TABLE memberships
  ADD COLUMN IF NOT EXISTS module_permissions JSONB DEFAULT '{}'::jsonb;

ALTER TABLE invites
  ADD COLUMN IF NOT EXISTS module_permissions JSONB DEFAULT '{}'::jsonb;

-- Optional: allow role 'editor' (already valid if you use it in app)
-- COMMENT ON COLUMN memberships.role IS 'tenant_admin | editor';
