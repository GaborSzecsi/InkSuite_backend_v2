-- Apply after 020. No destructive data processing runs in this migration.
BEGIN;
-- Protect future writes even if an older application process is still running.
-- NOT VALID permits separately reviewed cleanup of any pre-existing rows.
ALTER TABLE distribution_orders ADD CONSTRAINT distribution_shopify_no_contact_data
 CHECK(source <> 'SHOPIFY' OR (recipient = '{}'::jsonb AND delivery_instructions = '')) NOT VALID;
CREATE TABLE distribution_privacy_requests (
 id uuid PRIMARY KEY DEFAULT gen_random_uuid(), tenant_id uuid NOT NULL REFERENCES tenants(id),
 installation_id uuid NOT NULL, shop text, delivery_id text NOT NULL,
 topic text NOT NULL CHECK(topic IN ('customers/data_request','customers/redact','shop/redact')),
 payload jsonb NOT NULL DEFAULT '{}', status text NOT NULL DEFAULT 'PENDING',
 error_code text, created_at timestamptz NOT NULL DEFAULT now(),
 due_at timestamptz NOT NULL DEFAULT now()+interval '30 days', completed_at timestamptz,
 UNIQUE(installation_id,delivery_id)
);
CREATE INDEX distribution_privacy_due ON distribution_privacy_requests(status,due_at);
CREATE TABLE distribution_privacy_suppressions (
 tenant_id uuid NOT NULL REFERENCES tenants(id), fingerprint text NOT NULL,
 expires_at timestamptz NOT NULL DEFAULT now()+interval '30 days',
 PRIMARY KEY(tenant_id,fingerprint)
);
CREATE TABLE distribution_access_audit (
 id uuid PRIMARY KEY DEFAULT gen_random_uuid(), tenant_id uuid NOT NULL REFERENCES tenants(id),
 actor text NOT NULL, action text NOT NULL, resource_id uuid,
 created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX distribution_audit_retention ON distribution_access_audit(created_at);
-- The application defensively hides legacy delivery data. Run the separately
-- reviewed cleanup command to remove existing values after backup review.
COMMIT;
