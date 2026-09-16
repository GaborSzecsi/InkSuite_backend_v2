-- Additive Marketing schema. Run explicitly; never applied during API startup.
BEGIN;
CREATE TABLE IF NOT EXISTS marketing_campaigns (
 id uuid PRIMARY KEY, tenant_id uuid NOT NULL REFERENCES tenants(id),
 name text NOT NULL CHECK(length(trim(name))>0), description text NOT NULL DEFAULT '',
 association_type text NOT NULL CHECK(association_type IN ('single_work','multi_work','publisher')),
 start_date date, end_date date, status text NOT NULL DEFAULT 'draft' CHECK(status IN ('draft','active','completed','archived')),
 created_by uuid NOT NULL REFERENCES users(id), created_at timestamptz NOT NULL DEFAULT now(),
 updated_at timestamptz NOT NULL DEFAULT now(), archived_at timestamptz,
 CHECK(end_date IS NULL OR start_date IS NULL OR end_date>=start_date), UNIQUE(id,tenant_id)
);
CREATE TABLE IF NOT EXISTS marketing_campaign_works (
 id uuid PRIMARY KEY, tenant_id uuid NOT NULL REFERENCES tenants(id), campaign_id uuid NOT NULL,
 work_id uuid NOT NULL REFERENCES works(id), created_at timestamptz NOT NULL DEFAULT now(),
 FOREIGN KEY(campaign_id,tenant_id) REFERENCES marketing_campaigns(id,tenant_id) ON DELETE CASCADE,
 UNIQUE(campaign_id,work_id)
);
CREATE TABLE IF NOT EXISTS social_accounts (
 id uuid PRIMARY KEY, tenant_id uuid NOT NULL REFERENCES tenants(id), provider text NOT NULL,
 provider_account_id text NOT NULL, provider_parent_account_id text, display_name text NOT NULL,
 username text, account_type text NOT NULL, profile_image_url text,
 access_token_encrypted text NOT NULL, refresh_token_encrypted text,
 token_expires_at timestamptz, refresh_token_expires_at timestamptz,
 scopes jsonb NOT NULL DEFAULT '[]', provider_metadata jsonb NOT NULL DEFAULT '{}',
 status text NOT NULL DEFAULT 'connected', connected_by uuid NOT NULL REFERENCES users(id),
 connected_at timestamptz NOT NULL DEFAULT now(), updated_at timestamptz NOT NULL DEFAULT now(), disconnected_at timestamptz,
 UNIQUE(tenant_id,provider,provider_account_id), UNIQUE(id,tenant_id)
);
CREATE TABLE IF NOT EXISTS social_posts (
 id uuid PRIMARY KEY, tenant_id uuid NOT NULL REFERENCES tenants(id), campaign_id uuid,
 title text NOT NULL DEFAULT '', content_text text NOT NULL DEFAULT '', scheduled_at timestamptz,
 timezone text NOT NULL DEFAULT 'UTC', publish_mode text NOT NULL DEFAULT 'schedule',
 status text NOT NULL DEFAULT 'draft' CHECK(status IN ('draft','scheduled','partially_published','published','failed','cancelled')),
 created_by uuid NOT NULL REFERENCES users(id), updated_by uuid REFERENCES users(id), scheduled_by uuid REFERENCES users(id),
 scheduling_timestamp timestamptz, created_at timestamptz NOT NULL DEFAULT now(), updated_at timestamptz NOT NULL DEFAULT now(),
 published_at timestamptz, cancelled_at timestamptz,
 FOREIGN KEY(campaign_id,tenant_id) REFERENCES marketing_campaigns(id,tenant_id), UNIQUE(id,tenant_id)
);
CREATE TABLE IF NOT EXISTS social_post_works (
 id uuid PRIMARY KEY, tenant_id uuid NOT NULL, post_id uuid NOT NULL, work_id uuid NOT NULL REFERENCES works(id),
 FOREIGN KEY(post_id,tenant_id) REFERENCES social_posts(id,tenant_id) ON DELETE CASCADE, UNIQUE(post_id,work_id)
);
CREATE TABLE IF NOT EXISTS social_post_targets (
 id uuid PRIMARY KEY, tenant_id uuid NOT NULL, post_id uuid NOT NULL, social_account_id uuid NOT NULL,
 provider text NOT NULL, provider_payload jsonb NOT NULL DEFAULT '{}', provider_status text NOT NULL DEFAULT 'pending',
 provider_post_id text, provider_post_url text, provider_state jsonb NOT NULL DEFAULT '{}',
 attempt_count integer NOT NULL DEFAULT 0, last_attempt_at timestamptz, published_at timestamptz,
 last_error_code text, last_error_message text, created_at timestamptz NOT NULL DEFAULT now(), updated_at timestamptz NOT NULL DEFAULT now(),
 FOREIGN KEY(post_id,tenant_id) REFERENCES social_posts(id,tenant_id) ON DELETE CASCADE,
 FOREIGN KEY(social_account_id,tenant_id) REFERENCES social_accounts(id,tenant_id), UNIQUE(post_id,social_account_id), UNIQUE(id,tenant_id)
);
CREATE TABLE IF NOT EXISTS social_media_assets (
 id uuid PRIMARY KEY, tenant_id uuid NOT NULL REFERENCES tenants(id), campaign_id uuid, work_id uuid REFERENCES works(id),
 source_type text NOT NULL CHECK(source_type IN ('title_public_asset','publisher_asset','campaign_asset','generated_derivative')),
 source_asset_id uuid, s3_bucket text NOT NULL, s3_key text NOT NULL, public_url text,
 filename text NOT NULL, display_name text, mime_type text NOT NULL, media_type text NOT NULL,
 width integer, height integer, duration_seconds numeric, filesize_bytes bigint, alt_text text,
 created_by uuid REFERENCES users(id), created_at timestamptz NOT NULL DEFAULT now(),
 FOREIGN KEY(campaign_id,tenant_id) REFERENCES marketing_campaigns(id,tenant_id), UNIQUE(id,tenant_id),
 FOREIGN KEY(source_asset_id,tenant_id) REFERENCES social_media_assets(id,tenant_id), UNIQUE(tenant_id,s3_bucket,s3_key)
);
CREATE TABLE IF NOT EXISTS social_post_assets (
 id uuid PRIMARY KEY, tenant_id uuid NOT NULL, post_id uuid NOT NULL, asset_id uuid NOT NULL,
 sort_order integer NOT NULL DEFAULT 0, role text NOT NULL DEFAULT 'media',
 FOREIGN KEY(post_id,tenant_id) REFERENCES social_posts(id,tenant_id) ON DELETE CASCADE,
 FOREIGN KEY(asset_id,tenant_id) REFERENCES social_media_assets(id,tenant_id), UNIQUE(post_id,asset_id)
);
CREATE TABLE IF NOT EXISTS social_publish_jobs (
 id uuid PRIMARY KEY, tenant_id uuid NOT NULL, post_target_id uuid NOT NULL,
 scheduled_at timestamptz NOT NULL, status text NOT NULL DEFAULT 'pending', attempt_count integer NOT NULL DEFAULT 0,
 locked_at timestamptz, locked_by text, next_retry_at timestamptz, last_error text,
 created_at timestamptz NOT NULL DEFAULT now(), updated_at timestamptz NOT NULL DEFAULT now(), completed_at timestamptz,
 FOREIGN KEY(post_target_id,tenant_id) REFERENCES social_post_targets(id,tenant_id), UNIQUE(post_target_id)
);
CREATE TABLE IF NOT EXISTS marketing_oauth_states (
 state_hash text PRIMARY KEY, tenant_id uuid NOT NULL REFERENCES tenants(id), user_id uuid NOT NULL REFERENCES users(id),
 provider text NOT NULL, expires_at timestamptz NOT NULL, used_at timestamptz
);
CREATE TABLE IF NOT EXISTS marketing_audit (
 id uuid PRIMARY KEY, tenant_id uuid NOT NULL REFERENCES tenants(id), actor_id uuid REFERENCES users(id),
 entity_id uuid NOT NULL, action text NOT NULL, detail jsonb NOT NULL DEFAULT '{}', created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS marketing_campaign_tenant ON marketing_campaigns(tenant_id,status,start_date);
CREATE INDEX IF NOT EXISTS marketing_calendar ON social_posts(tenant_id,scheduled_at,status);
CREATE INDEX IF NOT EXISTS marketing_work_campaign ON marketing_campaign_works(tenant_id,work_id);
CREATE INDEX IF NOT EXISTS marketing_work_posts ON social_post_works(tenant_id,work_id);
CREATE INDEX IF NOT EXISTS marketing_jobs_due ON social_publish_jobs(status,scheduled_at,next_retry_at);
-- Existing works may lack a composite unique key. Enforce tenant ownership without altering the catalog.
CREATE OR REPLACE FUNCTION marketing_check_work_tenant() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF NEW.work_id IS NOT NULL AND NOT EXISTS(SELECT 1 FROM works WHERE id=NEW.work_id AND tenant_id=NEW.tenant_id) THEN
  RAISE EXCEPTION 'Work does not belong to tenant' USING ERRCODE='23503';
 END IF;
 RETURN NEW;
END $$;
DROP TRIGGER IF EXISTS marketing_campaign_work_tenant ON marketing_campaign_works;
CREATE TRIGGER marketing_campaign_work_tenant BEFORE INSERT OR UPDATE ON marketing_campaign_works FOR EACH ROW EXECUTE FUNCTION marketing_check_work_tenant();
DROP TRIGGER IF EXISTS marketing_post_work_tenant ON social_post_works;
CREATE TRIGGER marketing_post_work_tenant BEFORE INSERT OR UPDATE ON social_post_works FOR EACH ROW EXECUTE FUNCTION marketing_check_work_tenant();
DROP TRIGGER IF EXISTS marketing_asset_work_tenant ON social_media_assets;
CREATE TRIGGER marketing_asset_work_tenant BEFORE INSERT OR UPDATE ON social_media_assets FOR EACH ROW EXECUTE FUNCTION marketing_check_work_tenant();
COMMIT;
