BEGIN;
-- Retain inactive installations for privacy handling, but release store ownership.
ALTER TABLE distribution_shopify_installations
 DROP CONSTRAINT IF EXISTS distribution_shopify_installations_shop_key;
CREATE UNIQUE INDEX IF NOT EXISTS distribution_shopify_active_shop
 ON distribution_shopify_installations(shop) WHERE active;
COMMIT;
