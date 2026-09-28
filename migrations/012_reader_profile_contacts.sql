BEGIN;
ALTER TABLE public.marketplace_profiles ADD COLUMN IF NOT EXISTS contact_details jsonb NOT NULL DEFAULT '{}'::jsonb;
COMMIT;
