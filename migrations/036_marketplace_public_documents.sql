-- Publisher catalogs have no title; keep using the canonical publishing_assets table.
BEGIN;
SET LOCAL lock_timeout='5s';
SET LOCAL statement_timeout='60s';
ALTER TABLE public.publishing_assets ALTER COLUMN work_id DROP NOT NULL;
ALTER TABLE public.publishing_assets ADD CONSTRAINT publishing_assets_title_or_catalog
 CHECK (work_id IS NOT NULL OR lower(asset_type) IN ('catalog','catalogue'));
CREATE INDEX publishing_assets_public_downloads ON public.publishing_assets(tenant_id,asset_type,work_id,created_at DESC,id)
 WHERE is_public AND is_active;
COMMIT;
