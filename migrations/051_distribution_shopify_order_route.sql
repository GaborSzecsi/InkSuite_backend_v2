BEGIN;
ALTER TABLE distribution_shopify_installations
    ADD COLUMN outbound_inventory_connection_id uuid,
    ADD COLUMN outbound_order_connection_id uuid,
    ADD CHECK ((outbound_inventory_connection_id IS NULL) = (outbound_order_connection_id IS NULL)),
    ADD FOREIGN KEY (tenant_id, outbound_inventory_connection_id) REFERENCES distribution_connections(tenant_id,id),
    ADD FOREIGN KEY (tenant_id, outbound_order_connection_id) REFERENCES distribution_connections(tenant_id,id);
COMMIT;
