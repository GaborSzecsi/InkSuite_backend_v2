BEGIN;
ALTER TABLE video_asset_metadata ADD COLUMN IF NOT EXISTS is_finished_video boolean NOT NULL DEFAULT false;
UPDATE video_asset_metadata SET is_finished_video=true WHERE project_id IS NOT NULL AND NOT is_finished_video;
ALTER TABLE video_asset_metadata ADD COLUMN IF NOT EXISTS working_project_id uuid;
DO $$ BEGIN
 IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname='video_working_asset_project_fk' AND conrelid='video_asset_metadata'::regclass) THEN
  ALTER TABLE video_asset_metadata ADD CONSTRAINT video_working_asset_project_fk FOREIGN KEY(working_project_id,tenant_id) REFERENCES video_projects(id,tenant_id) ON DELETE SET NULL (working_project_id);
 END IF;
END $$;
CREATE INDEX IF NOT EXISTS video_working_assets_project ON video_asset_metadata(tenant_id,working_project_id) WHERE working_project_id IS NOT NULL;
COMMIT;
