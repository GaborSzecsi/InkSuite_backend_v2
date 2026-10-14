BEGIN;
ALTER TABLE distribution_connections ADD COLUMN archived_at timestamptz;
ALTER TABLE distribution_connections ADD COLUMN shopify_installation_id uuid;
ALTER TABLE distribution_connections ADD CONSTRAINT distribution_connection_store_owner
 FOREIGN KEY(tenant_id,shopify_installation_id) REFERENCES distribution_shopify_installations(tenant_id,id) ON DELETE SET NULL (shopify_installation_id);
-- Existing connections have no reliable creation provenance; do not guess ownership.
COMMIT;
