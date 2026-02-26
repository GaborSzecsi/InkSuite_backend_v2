-- Ensure tenant "marble-press" exists so /api/tenants/marble-press/members and /invites work.
-- Run once: psql $DATABASE_URL -f docs/SEED_tenant_marble_press.sql

INSERT INTO tenants (id, name, slug, created_at)
SELECT gen_random_uuid(), 'Marble Press', 'marble-press', now()
WHERE NOT EXISTS (SELECT 1 FROM tenants WHERE lower(slug) = 'marble-press');
