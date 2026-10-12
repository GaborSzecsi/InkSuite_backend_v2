BEGIN;
CREATE TABLE IF NOT EXISTS platform_activity (
 id uuid PRIMARY KEY DEFAULT gen_random_uuid(), actor_id uuid NOT NULL,
 tenant_id uuid, tenant_slug text, action text NOT NULL, module text NOT NULL,
 occurred_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS platform_activity_date_idx ON platform_activity(occurred_at DESC);
CREATE INDEX IF NOT EXISTS platform_activity_tenant_idx ON platform_activity(tenant_id, occurred_at DESC);
CREATE TABLE IF NOT EXISTS platform_support_sessions (
 id uuid PRIMARY KEY DEFAULT gen_random_uuid(), actor_id uuid NOT NULL,
 tenant_id uuid NOT NULL, tenant_slug text NOT NULL,
 reason text NOT NULL CHECK (length(reason) BETWEEN 10 AND 500),
 started_at timestamptz NOT NULL DEFAULT now(),
 expires_at timestamptz NOT NULL DEFAULT now() + interval '30 minutes', ended_at timestamptz
);
CREATE INDEX IF NOT EXISTS platform_support_actor_idx ON platform_support_sessions(actor_id, expires_at);
COMMIT;
