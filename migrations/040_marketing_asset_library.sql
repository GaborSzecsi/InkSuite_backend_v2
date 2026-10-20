BEGIN;
-- Existing posts keep their media in the library until explicitly changed.
ALTER TABLE social_media_assets ADD COLUMN IF NOT EXISTS library_visible boolean NOT NULL DEFAULT true;
ALTER TABLE social_posts ADD COLUMN IF NOT EXISTS keep_media_in_library boolean NOT NULL DEFAULT true;
CREATE INDEX IF NOT EXISTS marketing_asset_library ON social_media_assets(tenant_id,library_visible,created_at DESC);
COMMIT;
