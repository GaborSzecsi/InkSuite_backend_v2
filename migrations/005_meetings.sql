-- Meetings: user-owned resources, durable booking jobs, and existing bell data.
BEGIN;
CREATE TABLE IF NOT EXISTS meeting_profiles (
 tenant_id uuid NOT NULL REFERENCES tenants(id), user_id uuid NOT NULL REFERENCES users(id),
 timezone text NOT NULL DEFAULT 'America/Los_Angeles', display_name text NOT NULL DEFAULT '',
 company_email text NOT NULL DEFAULT '', sender_connection_id uuid,
 signature_text text NOT NULL DEFAULT '', signature_links jsonb NOT NULL DEFAULT '[]',
 availability jsonb NOT NULL DEFAULT '{}', updated_at timestamptz NOT NULL DEFAULT now(),
 PRIMARY KEY (tenant_id,user_id)
);
CREATE TABLE IF NOT EXISTS meeting_connections (
 id uuid PRIMARY KEY, tenant_id uuid NOT NULL, user_id uuid NOT NULL,
 provider text NOT NULL CHECK(provider IN ('google','microsoft')), account_id text NOT NULL,
 email text NOT NULL, secret_id text NOT NULL, status text NOT NULL DEFAULT 'connected',
 calendars jsonb NOT NULL DEFAULT '[]', created_at timestamptz NOT NULL DEFAULT now(),
 FOREIGN KEY(tenant_id,user_id) REFERENCES meeting_profiles(tenant_id,user_id),
 UNIQUE(tenant_id,user_id,provider,account_id), UNIQUE(id,tenant_id,user_id)
);
ALTER TABLE meeting_profiles DROP CONSTRAINT IF EXISTS meeting_profiles_sender_fk;
ALTER TABLE meeting_profiles ADD CONSTRAINT meeting_profiles_sender_fk FOREIGN KEY(sender_connection_id,tenant_id,user_id) REFERENCES meeting_connections(id,tenant_id,user_id);
CREATE TABLE IF NOT EXISTS meeting_types (
 id uuid PRIMARY KEY, tenant_id uuid NOT NULL, user_id uuid NOT NULL,
 slug text UNIQUE NOT NULL, config jsonb NOT NULL, active boolean NOT NULL DEFAULT true,
 created_at timestamptz NOT NULL DEFAULT now(), updated_at timestamptz NOT NULL DEFAULT now(),
 FOREIGN KEY(tenant_id,user_id) REFERENCES meeting_profiles(tenant_id,user_id), UNIQUE(id,tenant_id,user_id)
);
CREATE TABLE IF NOT EXISTS meeting_bookings (
 id uuid PRIMARY KEY, tenant_id uuid NOT NULL, user_id uuid NOT NULL, type_id uuid NOT NULL,
 guest jsonb NOT NULL, start_at timestamptz NOT NULL, end_at timestamptz NOT NULL,
 busy_start timestamptz NOT NULL, busy_end timestamptz NOT NULL,
 host_timezone text NOT NULL, status text NOT NULL DEFAULT 'pending', version integer NOT NULL DEFAULT 1,
 snapshot jsonb NOT NULL, sender_email_snapshot text NOT NULL,
 manage_hash text UNIQUE NOT NULL, manage_secret_id text NOT NULL,
 external_event_id text, external_connection_id uuid, external_calendar_id text,
 error text, created_at timestamptz NOT NULL DEFAULT now(), updated_at timestamptz NOT NULL DEFAULT now(),
 idempotency_key text NOT NULL, UNIQUE(type_id,idempotency_key),
 FOREIGN KEY(type_id,tenant_id,user_id) REFERENCES meeting_types(id,tenant_id,user_id),
 FOREIGN KEY(external_connection_id,tenant_id,user_id) REFERENCES meeting_connections(id,tenant_id,user_id),
 CHECK(end_at>start_at), CHECK(busy_end>busy_start)
);
CREATE INDEX IF NOT EXISTS meeting_upcoming ON meeting_bookings(tenant_id,user_id,status,start_at);
-- Host-row locking serializes writes; no optional btree_gist extension required.
CREATE TABLE IF NOT EXISTS meeting_jobs (
 id uuid PRIMARY KEY, booking_id uuid NOT NULL REFERENCES meeting_bookings(id), version integer NOT NULL,
 kind text NOT NULL, recipient text NOT NULL, due_at timestamptz NOT NULL,
 status text NOT NULL DEFAULT 'pending', attempts integer NOT NULL DEFAULT 0,
 sent_at timestamptz, error text, sender_email_snapshot text, updated_at timestamptz NOT NULL DEFAULT now(),
 UNIQUE(booking_id,version,kind,recipient)
);
CREATE INDEX IF NOT EXISTS meeting_jobs_due ON meeting_jobs(due_at) WHERE status IN ('pending','retry');
CREATE TABLE IF NOT EXISTS user_notifications (
 id uuid PRIMARY KEY, tenant_id uuid NOT NULL REFERENCES tenants(id), user_id uuid NOT NULL REFERENCES users(id),
 dedupe_key text NOT NULL, kind text NOT NULL, title text NOT NULL, message text NOT NULL,
 booking_id uuid REFERENCES meeting_bookings(id), read_at timestamptz, created_at timestamptz NOT NULL DEFAULT now(),
 UNIQUE(tenant_id,user_id,dedupe_key)
);
CREATE INDEX IF NOT EXISTS user_notifications_recent ON user_notifications(tenant_id,user_id,created_at DESC);
CREATE TABLE IF NOT EXISTS meeting_oauth_states (
 state_hash text PRIMARY KEY, tenant_id uuid NOT NULL, user_id uuid NOT NULL, provider text NOT NULL,
 verifier_secret_id text NOT NULL, expires_at timestamptz NOT NULL,
 FOREIGN KEY(tenant_id,user_id) REFERENCES meeting_profiles(tenant_id,user_id)
);
CREATE TABLE IF NOT EXISTS meeting_rate_limits (key text PRIMARY KEY, window_at timestamptz NOT NULL, hits integer NOT NULL);
CREATE TABLE IF NOT EXISTS meeting_audit (
 id bigserial PRIMARY KEY, booking_id uuid REFERENCES meeting_bookings(id), event text NOT NULL,
 created_at timestamptz NOT NULL DEFAULT now()
);
COMMIT;
