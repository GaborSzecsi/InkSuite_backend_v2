BEGIN;
CREATE TABLE distribution_shopify_link_codes (
 code_hash text PRIMARY KEY, tenant_id uuid NOT NULL REFERENCES tenants(id),
 user_id uuid NOT NULL REFERENCES users(id), expires_at timestamptz NOT NULL,
 used_at timestamptz
);
CREATE TABLE distribution_shopify_users (
 installation_id uuid NOT NULL REFERENCES distribution_shopify_installations(id) ON DELETE CASCADE,
 subject text NOT NULL, user_id uuid NOT NULL REFERENCES users(id),
 PRIMARY KEY(installation_id,subject)
);
COMMIT;
