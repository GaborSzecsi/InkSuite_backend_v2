import os
import sys
from dotenv import load_dotenv
load_dotenv()

def get_env(name: str, default: str | None = None) -> str | None:
    v = os.getenv(name)
    return v if v not in (None, "") else default

def build_conn_info():
    # If you DO use DATABASE_URL in some environments, support it
    database_url = get_env("DATABASE_URL")

    # Otherwise try common split-var patterns
    host = get_env("POSTGRES_HOST") or get_env("DB_HOST") or get_env("PGHOST") or "127.0.0.1"
    port = get_env("POSTGRES_PORT") or get_env("DB_PORT") or get_env("PGPORT") or "5432"
    db   = get_env("POSTGRES_DB")   or get_env("DB_NAME") or get_env("PGDATABASE") or "inksuite"
    user = get_env("POSTGRES_USER") or get_env("DB_USER") or get_env("PGUSER") or "inksuite_app"
    pwd  = get_env("POSTGRES_PASSWORD") or get_env("DB_PASSWORD") or get_env("PGPASSWORD")

    return database_url, host, port, db, user, pwd

def main():
    database_url, host, port, db, user, pwd = build_conn_info()

    try:
        import psycopg2
    except Exception:
        print("psycopg2 is not installed in this environment.")
        print("Run: pip install psycopg2-binary")
        sys.exit(1)

    if database_url:
        dsn = database_url
        safe = database_url
        if "://" in safe and "@" in safe:
            # crude redact
            safe = safe.split("://", 1)[0] + "://***:***@" + safe.split("@", 1)[1]
        print(f"Using DATABASE_URL: {safe}")
        conn = psycopg2.connect(dsn)
    else:
        if not pwd:
            print("No DATABASE_URL found and no password env var found (POSTGRES_PASSWORD/DB_PASSWORD/PGPASSWORD).")
            print("Set one of those and rerun.")
            sys.exit(1)
        print(f"Using split vars: host={host} port={port} db={db} user={user}")
        conn = psycopg2.connect(host=host, port=int(port), dbname=db, user=user, password=pwd)

    conn.autocommit = True
    cur = conn.cursor()

    cur.execute("SELECT current_database(), inet_server_addr(), inet_server_port();")
    print("Connected to:", cur.fetchone())

    cur.execute("""
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema='public'
          AND table_name='memberships'
          AND column_name='module_permissions';
    """)
    has_memberships = cur.fetchone() is not None

    cur.execute("""
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema='public'
          AND table_name='invites'
          AND column_name='module_permissions';
    """)
    has_invites = cur.fetchone() is not None

    print("memberships.module_permissions exists:", has_memberships)
    print("invites.module_permissions exists:", has_invites)

    if not has_memberships:
        print("Adding memberships.module_permissions...")
        cur.execute("""
            ALTER TABLE public.memberships
              ADD COLUMN IF NOT EXISTS module_permissions JSONB NOT NULL DEFAULT '{}'::jsonb;
        """)

    if not has_invites:
        print("Adding invites.module_permissions...")
        cur.execute("""
            ALTER TABLE public.invites
              ADD COLUMN IF NOT EXISTS module_permissions JSONB NOT NULL DEFAULT '{}'::jsonb;
        """)

    # Re-check
    cur.execute("""
        SELECT column_name, data_type, is_nullable, column_default
        FROM information_schema.columns
        WHERE table_schema='public'
          AND table_name='memberships'
          AND column_name='module_permissions';
    """)
    print("memberships column:", cur.fetchone())

    cur.execute("""
        SELECT column_name, data_type, is_nullable, column_default
        FROM information_schema.columns
        WHERE table_schema='public'
          AND table_name='invites'
          AND column_name='module_permissions';
    """)
    print("invites column:", cur.fetchone())

    cur.close()
    conn.close()
    print("Done.")

if __name__ == "__main__":
    main()