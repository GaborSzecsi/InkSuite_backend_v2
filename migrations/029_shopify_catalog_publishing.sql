ALTER TABLE distribution_shopify_variants ADD COLUMN IF NOT EXISTS active boolean NOT NULL DEFAULT true;
ALTER TABLE distribution_shopify_variants ADD COLUMN IF NOT EXISTS seen_job_id uuid;
CREATE TABLE IF NOT EXISTS distribution_shopify_publish_jobs (
 id uuid PRIMARY KEY DEFAULT gen_random_uuid(), tenant_id uuid NOT NULL,
 installation_id uuid NOT NULL, work_id uuid NOT NULL, connection_id uuid NOT NULL,
 selection jsonb NOT NULL, status text NOT NULL DEFAULT 'QUEUED', message text,
 created_at timestamptz NOT NULL DEFAULT now(), updated_at timestamptz NOT NULL DEFAULT now(),
 FOREIGN KEY(tenant_id,installation_id) REFERENCES distribution_shopify_installations(tenant_id,id) ON DELETE CASCADE,
 FOREIGN KEY(tenant_id,connection_id) REFERENCES distribution_connections(tenant_id,id)
);
CREATE UNIQUE INDEX IF NOT EXISTS distribution_publish_pending ON distribution_shopify_publish_jobs(installation_id,work_id) WHERE status IN ('QUEUED','RUNNING','REVIEW_REQUIRED');
