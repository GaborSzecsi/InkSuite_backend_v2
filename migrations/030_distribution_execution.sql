-- Explicit deployment migration. Existing connections and stores remain inactive.
BEGIN;
ALTER TABLE distribution_connections ADD COLUMN inventory_as_of timestamptz;
ALTER TABLE distribution_connections ADD COLUMN next_sync_at timestamptz NOT NULL DEFAULT now();
ALTER TABLE distribution_connections ADD COLUMN orders_enabled boolean NOT NULL DEFAULT false;
ALTER TABLE distribution_connections ADD COLUMN orders_enabled_at timestamptz;
ALTER TABLE distribution_connections ADD COLUMN inventory_enabled boolean NOT NULL DEFAULT false;
ALTER TABLE distribution_shopify_installations ADD COLUMN inventory_push_enabled boolean NOT NULL DEFAULT false;
ALTER TABLE distribution_orders ADD COLUMN shopify_fulfillment_reconciled boolean NOT NULL DEFAULT false;
ALTER TABLE distribution_jobs ADD COLUMN lease_id uuid;
ALTER TABLE distribution_jobs DROP CONSTRAINT distribution_jobs_kind_check;
ALTER TABLE distribution_jobs ADD CONSTRAINT distribution_jobs_kind_check CHECK(kind IN ('INVENTORY','TEST','SUBMIT','TRACKING','PUBLISH'));
CREATE TABLE distribution_simulated_orders (
 tenant_id uuid NOT NULL, connection_id uuid NOT NULL, order_id uuid NOT NULL,
 items jsonb NOT NULL, status text NOT NULL DEFAULT 'ACCEPTED' CHECK(status IN ('ACCEPTED','SHIPPED')),
 created_at timestamptz NOT NULL DEFAULT now(),
 PRIMARY KEY(tenant_id,connection_id,order_id),
 FOREIGN KEY(tenant_id,connection_id) REFERENCES distribution_connections(tenant_id,id),
 FOREIGN KEY(tenant_id,order_id) REFERENCES distribution_orders(tenant_id,id) ON DELETE CASCADE
);
CREATE INDEX distribution_connections_schedule ON distribution_connections(next_sync_at) WHERE enabled;
COMMIT;
