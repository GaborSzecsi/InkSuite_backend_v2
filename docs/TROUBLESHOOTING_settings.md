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

**Example (run on the DB that production uses, e.g. RDS):**

```sql
-- List current members and their roles for marble-press
SELECT u.email, m.role
FROM memberships m
JOIN users u ON u.id = m.user_id
JOIN tenants t ON t.id = m.tenant_id
WHERE t.slug = 'marble-press';

-- If you need to make a user tenant_admin (replace USER_ID and TENANT_ID with real UUIDs from the tables above):
-- UPDATE memberships SET role = 'tenant_admin' WHERE tenant_id = 'TENANT_ID' AND user_id = 'USER_ID';
```

After the user has `tenant_admin`, they can open Settings without getting 403.
