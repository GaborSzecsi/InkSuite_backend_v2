-- Reviewed existing editions schema before authoring: no archive columns.
-- Keep internal lifecycle status and ONIX publishing status unchanged.
BEGIN;
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';
ALTER TABLE public.editions
    ADD COLUMN IF NOT EXISTS archived_at timestamptz,
    ADD COLUMN IF NOT EXISTS archived_by uuid,
    ADD COLUMN IF NOT EXISTS archive_reason text NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS archive_history jsonb NOT NULL DEFAULT '[]'::jsonb;
CREATE INDEX IF NOT EXISTS editions_archived_by_tenant
    ON public.editions (tenant_id, archived_at, id) WHERE archived_at IS NOT NULL;
COMMIT;
