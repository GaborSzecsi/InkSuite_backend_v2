-- Operational stock ownership only: no orders, customer details or stock history.
CREATE TABLE IF NOT EXISTS distribution_shopify_managed_stock (
 tenant_id uuid NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
 installation_id uuid NOT NULL REFERENCES distribution_shopify_installations(id) ON DELETE CASCADE,
 connection_id uuid NOT NULL,
 shop text NOT NULL,
 inventory_item_id text NOT NULL,
 location_id text NOT NULL,
 PRIMARY KEY (tenant_id,shop,inventory_item_id,location_id)
);
INSERT INTO distribution_shopify_managed_stock
 (tenant_id,installation_id,connection_id,shop,inventory_item_id,location_id)
SELECT DISTINCT v.tenant_id,i.id,v.connection_id,i.shop,v.inventory_item_id,i.location_id
 FROM distribution_shopify_variants v
 JOIN distribution_shopify_installations i ON i.id=v.installation_id AND i.tenant_id=v.tenant_id
 WHERE i.active AND v.active AND v.connection_id IS NOT NULL
 AND v.inventory_item_id IS NOT NULL AND i.location_id IS NOT NULL
 AND (i.inventory_push_enabled OR i.simulated_inventory_connection_id=v.connection_id)
ON CONFLICT DO NOTHING;
