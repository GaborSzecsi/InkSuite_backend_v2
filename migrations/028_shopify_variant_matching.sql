-- Additive metadata; existing edition/distributor mappings remain unchanged.
ALTER TABLE distribution_shopify_variants ADD COLUMN IF NOT EXISTS catalog_metadata jsonb NOT NULL DEFAULT '{}'::jsonb;
ALTER TABLE distribution_shopify_jobs ADD COLUMN IF NOT EXISTS connection_id uuid REFERENCES distribution_connections(id);
