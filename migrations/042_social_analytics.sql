-- Reviewed against the live public schema on 2026-10-27. Apply explicitly.
-- No existing social/publishing records or credentials are changed.
BEGIN;
SET LOCAL lock_timeout='5s';
SET LOCAL statement_timeout='60s';
CREATE TABLE social_analytics_definitions (
 provider text NOT NULL, metric text NOT NULL, version text NOT NULL,
 label text NOT NULL, unit text NOT NULL, aggregation text NOT NULL,
 description text NOT NULL, PRIMARY KEY(provider,metric,version)
);
CREATE TABLE social_analytics_media (
 id uuid PRIMARY KEY, tenant_id uuid NOT NULL REFERENCES tenants(id),
 account_id uuid NOT NULL, external_id text NOT NULL, media_type text NOT NULL,
 title text NOT NULL DEFAULT '', permalink text, thumbnail_url text,
 published_at timestamptz, updated_at timestamptz NOT NULL DEFAULT now(),
 FOREIGN KEY(account_id,tenant_id) REFERENCES social_accounts(id,tenant_id) ON DELETE CASCADE,
 UNIQUE(account_id,tenant_id,external_id), UNIQUE(id,tenant_id)
);
CREATE TABLE social_analytics_observations (
 id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
 tenant_id uuid NOT NULL REFERENCES tenants(id), account_id uuid NOT NULL,
 provider text NOT NULL, subject text NOT NULL DEFAULT '',
 metric text NOT NULL, definition_version text NOT NULL, api_version text NOT NULL,
 value numeric CHECK(value>=0), availability text NOT NULL CHECK(availability IN
 ('available','unsupported','permission_missing','temporary_failure','not_returned')),
 scope text NOT NULL CHECK(scope IN ('day','snapshot','lifetime')),
 period_start date NOT NULL, period_end date NOT NULL CHECK(period_end>=period_start),
 observed_at timestamptz NOT NULL, run_id uuid NOT NULL,
 FOREIGN KEY(account_id,tenant_id) REFERENCES social_accounts(id,tenant_id) ON DELETE CASCADE,
 FOREIGN KEY(provider,metric,definition_version) REFERENCES social_analytics_definitions(provider,metric,version),
 CHECK((availability='available' AND value IS NOT NULL) OR (availability<>'available' AND value IS NULL)),
 UNIQUE(account_id,tenant_id,subject,metric,definition_version,scope,period_start,period_end,run_id)
);
CREATE INDEX social_analytics_reporting ON social_analytics_observations
 (tenant_id,account_id,subject,period_start,metric,observed_at DESC);
CREATE INDEX social_analytics_observed ON social_analytics_observations(tenant_id,account_id,observed_at DESC);
CREATE INDEX social_analytics_media_date ON social_analytics_media(tenant_id,published_at DESC);
CREATE INDEX social_targets_analytics_lookup ON social_post_targets(tenant_id,social_account_id,provider_post_id)
 WHERE provider_post_id IS NOT NULL;
CREATE TABLE social_analytics_sync (
 account_id uuid PRIMARY KEY, tenant_id uuid NOT NULL REFERENCES tenants(id),
 status text NOT NULL DEFAULT 'pending' CHECK(status IN
 ('pending','processing','success','partial','permission_missing','failed','disconnected')),
 last_attempt_at timestamptz, last_success_at timestamptz,
 next_run_at timestamptz NOT NULL DEFAULT now(), locked_at timestamptz,
 run_id uuid, failures integer NOT NULL DEFAULT 0, error_category text,
 requested_at timestamptz, last_collected_through date, backfill_before date,
 discovery_cursors jsonb NOT NULL DEFAULT '{}',
 FOREIGN KEY(account_id,tenant_id) REFERENCES social_accounts(id,tenant_id) ON DELETE CASCADE
);
CREATE INDEX social_analytics_due ON social_analytics_sync(next_run_at) WHERE status<>'disconnected';
COMMIT;
