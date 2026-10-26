BEGIN;
CREATE TABLE IF NOT EXISTS marketing_upload_sessions (
    id uuid PRIMARY KEY,
    tenant_id uuid NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    user_id uuid NOT NULL,
    campaign_id uuid,
    asset_id uuid NOT NULL UNIQUE,
    filename text NOT NULL,
    filesize_bytes bigint NOT NULL CHECK (filesize_bytes > 0 AND filesize_bytes <= 524288000),
    s3_bucket text NOT NULL,
    staging_key text NOT NULL UNIQUE,
    final_key text NOT NULL UNIQUE,
    upload_id text NOT NULL,
    part_size integer NOT NULL,
    status text NOT NULL DEFAULT 'pending' CHECK (status IN ('pending','completed','aborted')),
    created_at timestamptz NOT NULL DEFAULT now(),
    expires_at timestamptz NOT NULL
);
CREATE INDEX IF NOT EXISTS marketing_upload_expiry ON marketing_upload_sessions(expires_at) WHERE status = 'pending';
COMMIT;
