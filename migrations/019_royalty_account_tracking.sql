-- Additive account tracking. Existing statements and payments are not changed.
BEGIN;
CREATE TABLE IF NOT EXISTS royalty_account_settings (
 tenant_id uuid NOT NULL REFERENCES tenants(id),
 work_id uuid NOT NULL REFERENCES works(id),
 party text NOT NULL CHECK (party IN ('author','illustrator')),
 minimum_payout numeric(14,2) NOT NULL DEFAULT 100 CHECK (minimum_payout >= 0),
 reserve_percent numeric(7,4) NOT NULL DEFAULT 0 CHECK (reserve_percent BETWEEN 0 AND 100),
 reserve_held numeric(14,2) NOT NULL DEFAULT 0 CHECK (reserve_held >= 0),
 currency text NOT NULL DEFAULT 'USD',
 version integer NOT NULL DEFAULT 1,
 updated_at timestamptz NOT NULL DEFAULT now(),
 PRIMARY KEY (tenant_id,work_id,party)
);
CREATE TABLE IF NOT EXISTS royalty_account_events (
 id uuid PRIMARY KEY,
 tenant_id uuid NOT NULL REFERENCES tenants(id),
 work_id uuid NOT NULL REFERENCES works(id),
 party text NOT NULL CHECK (party IN ('author','illustrator')),
 period_id uuid NOT NULL REFERENCES royalty_periods(id),
 event_type text NOT NULL CHECK (event_type IN ('settings','payment')),
 actor text NOT NULL,
 reason text NOT NULL,
 request_payload jsonb NOT NULL,
 before_values jsonb NOT NULL DEFAULT '{}'::jsonb,
 after_values jsonb NOT NULL,
 created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS royalty_account_events_account_idx
 ON royalty_account_events(tenant_id,work_id,party,created_at DESC);
COMMIT;
