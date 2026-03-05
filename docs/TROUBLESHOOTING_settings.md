# Settings API: local “table missing” and production 403

## 1. Localhost: “relation tenant_org_profile does not exist”

Your **local** backend uses the database in your **local** `.env` (`DATABASE_URL`). The tables exist only in the DB where you ran the migration (e.g. RDS). You must create them in the **same** database your local backend uses.

**Fix (Windows, from backend repo root):**

```powershell
cd C:\Users\szecs\Documents\InkSuite_backend_v2
# Ensure .env has DATABASE_URL pointing to your LOCAL Postgres (e.g. 127.0.0.1:5433)
python scripts/run_tenant_settings_migration.py
```

- If your **local** backend uses **local Postgres** (e.g. `postgresql://inksuite_app:...@127.0.0.1:5433/inksuite`), keep that in `.env` and run the script once. It will create the tables in that local DB.
- If you want to use **RDS** from your machine, set `DATABASE_URL` to your RDS URL (and ensure the machine can reach RDS, e.g. VPN/tunnel). Then run the script; the tables will be created on RDS. Your local backend will then use RDS and the tables will exist there.

After the script runs successfully, restart the local backend and try again.

---

## 2. Production (www.inksuite.io): 403 Forbidden on `/api/tenants/marble-press/settings/organization`

The settings endpoints require **Tenant Admin** for that tenant. 403 means the backend rejected the request because the current user is not a tenant admin for `marble-press`.

**Check:**

1. **Who is logged in?** Only users with role **tenant_admin** for the tenant `marble-press` can access Settings (organization and email).
2. **Grant tenant_admin (if needed):** If you have one main user who should manage settings, ensure that user has `tenant_admin` in the `memberships` table for the marble-press tenant.

**Fix (run on the same DB your AWS backend uses, e.g. RDS via psql or a SQL client):**

```sql
-- 1) See who is in the tenant and their roles
SELECT u.email, m.role
FROM memberships m
JOIN users u ON u.id = m.user_id
JOIN tenants t ON t.id = m.tenant_id
WHERE t.slug = 'marble-press';

-- 2) Make one user the tenant admin (replace with the email you sign in with and your tenant slug)
UPDATE memberships m
SET role = 'tenant_admin'
FROM users u, tenants t
WHERE m.user_id = u.id AND m.tenant_id = t.id
  AND lower(u.email) = lower('your-email@example.com')
  AND lower(t.slug) = 'marble-press';
```

If that user has **no row** in `memberships` yet (they’re not in the tenant at all), add them first:

```sql
-- Find your user id and tenant id
SELECT id FROM users WHERE lower(email) = lower('your-email@example.com');
SELECT id FROM tenants WHERE lower(slug) = 'marble-press';

-- Insert membership as tenant_admin (use the UUIDs from above)
INSERT INTO memberships (tenant_id, user_id, role)
VALUES ('tenant-uuid-here', 'user-uuid-here', 'tenant_admin')
ON CONFLICT (tenant_id, user_id) DO UPDATE SET role = 'tenant_admin';
```

After the signed-in user has `role = 'tenant_admin'` for that tenant, Settings will stop returning 403.
