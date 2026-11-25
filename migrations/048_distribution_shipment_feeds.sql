BEGIN;
CREATE TABLE distribution_shipment_feeds (
    tenant_id uuid NOT NULL,
    connection_id uuid NOT NULL,
    credential_id uuid NOT NULL DEFAULT gen_random_uuid() UNIQUE,
    credentials_saved boolean NOT NULL DEFAULT false,
    configuration jsonb NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, connection_id),
    FOREIGN KEY (tenant_id, connection_id) REFERENCES distribution_connections(tenant_id, id) ON DELETE CASCADE
);
COMMIT;
