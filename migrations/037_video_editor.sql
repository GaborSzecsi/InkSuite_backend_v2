BEGIN;
CREATE TABLE IF NOT EXISTS video_projects (
 id uuid PRIMARY KEY, tenant_id uuid NOT NULL REFERENCES tenants(id), name text NOT NULL,
 document jsonb NOT NULL, revision integer NOT NULL DEFAULT 1, source_asset_id uuid,
 created_by uuid REFERENCES users(id), created_at timestamptz NOT NULL DEFAULT now(), updated_at timestamptz NOT NULL DEFAULT now(),
 UNIQUE(id,tenant_id), FOREIGN KEY(source_asset_id,tenant_id) REFERENCES social_media_assets(id,tenant_id) ON DELETE SET NULL (source_asset_id)
);
CREATE TABLE IF NOT EXISTS video_render_jobs (
 id uuid PRIMARY KEY, tenant_id uuid NOT NULL REFERENCES tenants(id), project_id uuid NOT NULL,
 project_revision integer NOT NULL, document jsonb NOT NULL, resolution integer NOT NULL CHECK(resolution IN (720,1080)),
 status text NOT NULL DEFAULT 'queued' CHECK(status IN ('queued','running','completed','failed','cancelled')),
 progress integer NOT NULL DEFAULT 0, cancel_requested boolean NOT NULL DEFAULT false,
 output_asset_id uuid, error text, created_by uuid REFERENCES users(id), created_at timestamptz NOT NULL DEFAULT now(),
 started_at timestamptz, heartbeat_at timestamptz, completed_at timestamptz,
 FOREIGN KEY(project_id,tenant_id) REFERENCES video_projects(id,tenant_id),
 FOREIGN KEY(output_asset_id,tenant_id) REFERENCES social_media_assets(id,tenant_id) ON DELETE SET NULL (output_asset_id)
);
CREATE TABLE IF NOT EXISTS video_asset_metadata (
 asset_id uuid PRIMARY KEY, tenant_id uuid NOT NULL, project_id uuid, codec_info jsonb NOT NULL DEFAULT '{}', music_license jsonb,
 FOREIGN KEY(asset_id,tenant_id) REFERENCES social_media_assets(id,tenant_id) ON DELETE CASCADE,
 FOREIGN KEY(project_id,tenant_id) REFERENCES video_projects(id,tenant_id) ON DELETE SET NULL (project_id)
);
CREATE INDEX IF NOT EXISTS video_projects_tenant ON video_projects(tenant_id,updated_at DESC);
CREATE INDEX IF NOT EXISTS video_jobs_queue ON video_render_jobs(status,created_at);
COMMIT;
