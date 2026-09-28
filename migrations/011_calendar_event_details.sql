BEGIN;
CREATE TABLE IF NOT EXISTS meeting_event_details (
 tenant_id uuid NOT NULL REFERENCES tenants(id), user_id uuid NOT NULL REFERENCES users(id),
 event_id uuid NOT NULL, location text NOT NULL DEFAULT '', attendees jsonb NOT NULL DEFAULT '[]',
 version integer NOT NULL DEFAULT 1, PRIMARY KEY(tenant_id,user_id,event_id)
);
CREATE TABLE IF NOT EXISTS meeting_event_mail (
 id uuid PRIMARY KEY, tenant_id uuid NOT NULL REFERENCES tenants(id), user_id uuid NOT NULL REFERENCES users(id),
 event_id uuid NOT NULL, recipient text NOT NULL, payload jsonb NOT NULL,
 status text NOT NULL DEFAULT 'pending', error text, created_at timestamptz NOT NULL DEFAULT now(),
 updated_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS meeting_event_mail_pending ON meeting_event_mail(created_at) WHERE status='pending';
COMMIT;
