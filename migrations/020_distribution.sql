-- Commerce only. Does not alter financial inventory movements or royalty imports.
-- Apply separately after review; local development must not auto-migrate AWS.
BEGIN;
CREATE UNIQUE INDEX IF NOT EXISTS editions_distribution_tenant_id ON editions(tenant_id,id);
CREATE TABLE distribution_connections (
 id uuid PRIMARY KEY DEFAULT gen_random_uuid(), tenant_id uuid NOT NULL REFERENCES tenants(id),
 display_name text NOT NULL, adapter text NOT NULL,
 enabled boolean NOT NULL DEFAULT false,
 mode text NOT NULL DEFAULT 'preview' CHECK(mode IN ('preview','live')),
 status text NOT NULL DEFAULT 'NOT_CONFIGURED' CHECK(status IN ('NOT_CONFIGURED','CONNECTED','ERROR','DISABLED')),
 configuration jsonb NOT NULL DEFAULT '{}', secret_reference text,
 safety_buffer integer NOT NULL DEFAULT 5 CHECK(safety_buffer>=0),
 last_inventory_sync_at timestamptz, last_successful_connection_at timestamptz,
 last_error text, created_at timestamptz NOT NULL DEFAULT now(), updated_at timestamptz NOT NULL DEFAULT now(),
 UNIQUE(tenant_id,id)
);
CREATE TABLE distribution_inventory (
 tenant_id uuid NOT NULL, connection_id uuid NOT NULL, edition_id uuid NOT NULL,
 distributor_quantity integer NOT NULL DEFAULT 0 CHECK(distributor_quantity>=0),
 sync_status text NOT NULL DEFAULT 'NOT_SYNCED', last_synced_at timestamptz,
 marketplace_enabled boolean NOT NULL DEFAULT false,
 PRIMARY KEY(tenant_id,connection_id,edition_id),
 FOREIGN KEY(tenant_id,connection_id) REFERENCES distribution_connections(tenant_id,id),
 FOREIGN KEY(tenant_id,edition_id) REFERENCES editions(tenant_id,id)
);
CREATE TABLE distribution_orders (
 id uuid PRIMARY KEY DEFAULT gen_random_uuid(), tenant_id uuid NOT NULL,
 connection_id uuid NOT NULL, source text NOT NULL CHECK(source IN ('SHOPIFY','MARKETPLACE')),
 source_account text NOT NULL, external_order_id text NOT NULL, reference text NOT NULL,
 request_hash text NOT NULL, recipient jsonb NOT NULL, shipping_method text NOT NULL,
 delivery_instructions text NOT NULL DEFAULT '',
 mode text NOT NULL DEFAULT 'preview' CHECK(mode IN ('preview','live')),
 status text NOT NULL DEFAULT 'RECEIVED' CHECK(status IN ('RECEIVED','RESERVED','QUEUED','SUBMITTING','SUBMITTED','ACCEPTED','SHIPPED','FAILED','CANCELLED','REVIEW_REQUIRED')),
 distributor_order_id text, tracking jsonb NOT NULL DEFAULT '[]', error_code text,
 created_at timestamptz NOT NULL DEFAULT now(), updated_at timestamptz NOT NULL DEFAULT now(),
 submitted_at timestamptz, accepted_at timestamptz, shipped_at timestamptz,
 UNIQUE(tenant_id,id), UNIQUE(tenant_id,source,source_account,external_order_id,connection_id),
 FOREIGN KEY(tenant_id,connection_id) REFERENCES distribution_connections(tenant_id,id)
);
CREATE TABLE distribution_order_items (
 tenant_id uuid NOT NULL, order_id uuid NOT NULL, edition_id uuid NOT NULL,
 isbn text NOT NULL, quantity integer NOT NULL CHECK(quantity>0),
 PRIMARY KEY(tenant_id,order_id,edition_id),
 FOREIGN KEY(tenant_id,order_id) REFERENCES distribution_orders(tenant_id,id),
 FOREIGN KEY(tenant_id,edition_id) REFERENCES editions(tenant_id,id)
);
CREATE TABLE distribution_reservations (
 tenant_id uuid NOT NULL, connection_id uuid NOT NULL, edition_id uuid NOT NULL, order_id uuid NOT NULL,
 quantity integer NOT NULL CHECK(quantity>0),
 status text NOT NULL CHECK(status IN ('RESERVED','SUBMITTED','FULFILLED','RELEASED','CANCELLED')),
 created_at timestamptz NOT NULL DEFAULT now(), updated_at timestamptz NOT NULL DEFAULT now(),
 PRIMARY KEY(tenant_id,order_id,edition_id),
 FOREIGN KEY(tenant_id,connection_id,edition_id) REFERENCES distribution_inventory(tenant_id,connection_id,edition_id),
 FOREIGN KEY(tenant_id,order_id) REFERENCES distribution_orders(tenant_id,id)
);
CREATE TABLE distribution_jobs (
 id uuid PRIMARY KEY DEFAULT gen_random_uuid(), tenant_id uuid NOT NULL, connection_id uuid NOT NULL,
 order_id uuid, kind text NOT NULL CHECK(kind IN ('INVENTORY','TEST','SUBMIT','TRACKING')),
 status text NOT NULL DEFAULT 'QUEUED' CHECK(status IN ('QUEUED','RUNNING','DONE','FAILED','REVIEW_REQUIRED')),
 attempts integer NOT NULL DEFAULT 0, due_at timestamptz NOT NULL DEFAULT now(),
 started_at timestamptz, finished_at timestamptz, error_code text,
 created_at timestamptz NOT NULL DEFAULT now(),
 FOREIGN KEY(tenant_id,connection_id) REFERENCES distribution_connections(tenant_id,id),
 FOREIGN KEY(tenant_id,order_id) REFERENCES distribution_orders(tenant_id,id)
);
CREATE INDEX distribution_jobs_due ON distribution_jobs(due_at) WHERE status='QUEUED';
CREATE UNIQUE INDEX distribution_jobs_pending ON distribution_jobs(tenant_id,connection_id,kind,COALESCE(order_id,'00000000-0000-0000-0000-000000000000'::uuid)) WHERE status IN ('QUEUED','RUNNING');
CREATE TABLE distribution_events (
 id uuid PRIMARY KEY DEFAULT gen_random_uuid(), tenant_id uuid NOT NULL REFERENCES tenants(id),
 connection_id uuid NOT NULL, order_id uuid, actor text NOT NULL, event text NOT NULL,
 details jsonb NOT NULL DEFAULT '{}', created_at timestamptz NOT NULL DEFAULT now(),
 FOREIGN KEY(tenant_id,connection_id) REFERENCES distribution_connections(tenant_id,id),
 FOREIGN KEY(tenant_id,order_id) REFERENCES distribution_orders(tenant_id,id)
);
CREATE INDEX distribution_events_order ON distribution_events(tenant_id,order_id,created_at);
CREATE TABLE distribution_shopify_installations (
 id uuid PRIMARY KEY DEFAULT gen_random_uuid(), tenant_id uuid NOT NULL REFERENCES tenants(id),
 shop text NOT NULL UNIQUE, secret_reference text NOT NULL, active boolean NOT NULL DEFAULT true,
 location_id text, created_at timestamptz NOT NULL DEFAULT now(), UNIQUE(tenant_id,id)
);
CREATE TABLE distribution_shopify_variants (
 tenant_id uuid NOT NULL, installation_id uuid NOT NULL, variant_id text NOT NULL,
 sku text NOT NULL DEFAULT '', title text NOT NULL DEFAULT '', inventory_item_id text,
 edition_id uuid, connection_id uuid,
 PRIMARY KEY(tenant_id,installation_id,variant_id),
 FOREIGN KEY(tenant_id,installation_id) REFERENCES distribution_shopify_installations(tenant_id,id),
 FOREIGN KEY(tenant_id,edition_id) REFERENCES editions(tenant_id,id),
 FOREIGN KEY(tenant_id,connection_id) REFERENCES distribution_connections(tenant_id,id)
);
CREATE TABLE distribution_webhooks (
 id uuid PRIMARY KEY DEFAULT gen_random_uuid(), installation_id uuid NOT NULL,
 tenant_id uuid NOT NULL, delivery_id text NOT NULL, topic text NOT NULL,
 payload jsonb NOT NULL, status text NOT NULL DEFAULT 'RECEIVED', error_code text,
 created_at timestamptz NOT NULL DEFAULT now(), UNIQUE(installation_id,delivery_id),
 FOREIGN KEY(tenant_id,installation_id) REFERENCES distribution_shopify_installations(tenant_id,id)
);
CREATE TABLE distribution_shopify_jobs (
 id uuid PRIMARY KEY DEFAULT gen_random_uuid(), tenant_id uuid NOT NULL, installation_id uuid NOT NULL,
 status text NOT NULL DEFAULT 'QUEUED', cursor text, attempts integer NOT NULL DEFAULT 0,
 error_code text, created_at timestamptz NOT NULL DEFAULT now(), updated_at timestamptz NOT NULL DEFAULT now(),
 FOREIGN KEY(tenant_id,installation_id) REFERENCES distribution_shopify_installations(tenant_id,id)
);
CREATE UNIQUE INDEX distribution_shopify_jobs_pending ON distribution_shopify_jobs(installation_id) WHERE status='QUEUED';
CREATE TABLE distribution_oauth_states (
 state_hash text PRIMARY KEY, tenant_id uuid NOT NULL REFERENCES tenants(id),
 user_id uuid NOT NULL REFERENCES users(id), shop text NOT NULL,
 expires_at timestamptz NOT NULL, used_at timestamptz
);
CREATE VIEW distribution_availability AS
 SELECT i.*, greatest(0,i.distributor_quantity - c.safety_buffer - COALESCE(r.reserved,0))::integer AS calculated_quantity,
 CASE WHEN c.enabled AND i.sync_status='OK' AND i.last_synced_at > now()-interval '24 hours'
 THEN greatest(0,i.distributor_quantity-c.safety_buffer-COALESCE(r.reserved,0))::integer ELSE 0 END AS sellable_quantity,
 COALESCE(r.reserved,0)::integer AS reserved_quantity
 FROM distribution_inventory i JOIN distribution_connections c ON c.tenant_id=i.tenant_id AND c.id=i.connection_id
 LEFT JOIN LATERAL (SELECT SUM(quantity) AS reserved FROM distribution_reservations r
 WHERE r.tenant_id=i.tenant_id AND r.connection_id=i.connection_id AND r.edition_id=i.edition_id
 AND r.status IN ('RESERVED','SUBMITTED','FULFILLED')) r ON true;
-- FULFILLED reservations stay counted until an inventory adapter can reconcile their
-- shipment against the distributor snapshot watermark. Never release on blind refresh.
COMMIT;
