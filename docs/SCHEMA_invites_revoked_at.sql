-- Add revoked_at for soft-revoked invites (optional).
-- Run once: psql $DATABASE_URL -f docs/SCHEMA_invites_revoked_at.sql

ALTER TABLE invites
  ADD COLUMN IF NOT EXISTS revoked_at TIMESTAMP WITH TIME ZONE DEFAULT NULL;
