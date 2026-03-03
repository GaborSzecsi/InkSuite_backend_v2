# Tenant settings tables (fix 503 on /api/tenants/.../settings/*)

If you get **503** when calling `/api/tenants/{tenantSlug}/settings/email` or `/api/tenants/{tenantSlug}/settings/organization`, the backend is likely hitting missing Postgres tables.

**Fix:** create the tables once.

### Option A: From Windows (no psql needed)

From the backend repo root (with `.env` containing `DATABASE_URL`):

```powershell
cd C:\Users\szecs\Documents\InkSuite_backend_v2
python scripts/run_tenant_settings_migration.py
```

Uses the same `DATABASE_URL` and `psycopg` as the backend.

### Option B: From EC2/Ubuntu (psql)

```bash
# Connect (set PGPASSWORD or use .pgpass)
psql "host=... port=5432 dbname=postgres user=inksuite_app sslmode=require"
```

Then paste and run the SQL from `docs/SCHEMA_tenant_settings.sql`, or copy the file to the server and run:

```bash
psql "host=... dbname=postgres user=inksuite_app sslmode=require" -f SCHEMA_tenant_settings.sql
```

Use the same `dbname` as in your backend `DATABASE_URL`.

After this, the Settings UI (organization profile and email settings) can store and edit data in Postgres in the same place as user/tenant data.
