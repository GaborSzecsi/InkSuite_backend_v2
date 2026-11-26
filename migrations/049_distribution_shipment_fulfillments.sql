BEGIN;
CREATE TABLE distribution_shipment_fulfillments (
    tenant_id uuid NOT NULL,
    shipment_key text NOT NULL,
    connection_id uuid NOT NULL,
    actor_id uuid NOT NULL,
    status text NOT NULL DEFAULT 'PENDING' CHECK (status IN ('PENDING','REJECTED','SUCCEEDED')),
    shopify_fulfillment_id text,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, shipment_key),
    FOREIGN KEY (tenant_id, connection_id) REFERENCES distribution_connections(tenant_id, id)
);
COMMIT;
