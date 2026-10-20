BEGIN;
ALTER TABLE video_render_jobs ALTER COLUMN project_id DROP NOT NULL;
ALTER TABLE video_render_jobs ADD COLUMN IF NOT EXISTS normalize_asset_id uuid;
ALTER TABLE video_render_jobs ADD COLUMN IF NOT EXISTS job_kind text NOT NULL DEFAULT 'composition' CHECK(job_kind IN ('composition','normalize'));
CREATE UNIQUE INDEX IF NOT EXISTS video_one_running_per_tenant ON video_render_jobs(tenant_id) WHERE status='running';
COMMIT;
