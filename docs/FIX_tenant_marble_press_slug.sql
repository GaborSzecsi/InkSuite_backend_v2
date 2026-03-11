-- Align tenant slug so "marble-press" resolves to the tenant that owns your works.
-- Works use tenant_id b9e9efbd-a2fc-4506-a79d-4dd2e12910b5; the catalog looks up by slug.
-- Run once: psql $DATABASE_URL -f docs/FIX_tenant_marble_press_slug.sql

BEGIN;

-- 1. If another row has slug 'marble-press', free it so we can assign to the works' tenant
UPDATE tenants
SET slug = 'marble-press-reserved'
WHERE lower(trim(slug)) = 'marble-press'
  AND id != 'b9e9efbd-a2fc-4506-a79d-4dd2e12910b5';

-- 2. Ensure the tenant that owns the works has slug 'marble-press'
UPDATE tenants
SET slug = 'marble-press',
    name = COALESCE(NULLIF(trim(name), ''), 'Marble Press')
WHERE id = 'b9e9efbd-a2fc-4506-a79d-4dd2e12910b5';

-- 3. If that tenant doesn't exist yet (e.g. works were imported without tenant row), insert it
INSERT INTO tenants (id, name, slug, created_at)
SELECT 'b9e9efbd-a2fc-4506-a79d-4dd2e12910b5', 'Marble Press', 'marble-press', now()
WHERE NOT EXISTS (SELECT 1 FROM tenants WHERE id = 'b9e9efbd-a2fc-4506-a79d-4dd2e12910b5');

COMMIT;

-- Verify: this should return one row with slug = marble-press
-- SELECT id, name, slug FROM tenants WHERE id = 'b9e9efbd-a2fc-4506-a79d-4dd2e12910b5';
