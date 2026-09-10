BEGIN;
CREATE TABLE IF NOT EXISTS meeting_calendar_preferences (
 tenant_id uuid NOT NULL, user_id uuid NOT NULL,
 share_busy boolean NOT NULL DEFAULT false,
 PRIMARY KEY(tenant_id,user_id),
 FOREIGN KEY(tenant_id,user_id) REFERENCES meeting_profiles(tenant_id,user_id)
);
CREATE TABLE IF NOT EXISTS meeting_calendar_events (
 id uuid PRIMARY KEY, tenant_id uuid NOT NULL, user_id uuid NOT NULL,
 title text NOT NULL, start_at timestamptz NOT NULL, end_at timestamptz NOT NULL,
 created_at timestamptz NOT NULL DEFAULT now(),
 FOREIGN KEY(tenant_id,user_id) REFERENCES meeting_profiles(tenant_id,user_id),
 CHECK(end_at>start_at)
);
CREATE INDEX IF NOT EXISTS meeting_calendar_event_range ON meeting_calendar_events(tenant_id,user_id,start_at,end_at);
COMMIT;
