"""
Seed tenant "marble-press" so /api/tenants/marble-press/members and /invites return 200 instead of 404.
Run from project root: python scripts/seed_marble_press.py
Requires: DATABASE_URL in .env
"""
import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from dotenv import load_dotenv
load_dotenv(PROJECT_ROOT / ".env")

from app.core.db import db_conn

def main():
    with db_conn() as conn, conn.cursor() as cur:
        try:
            cur.execute(
                """
                INSERT INTO tenants (id, name, slug, created_at)
                SELECT gen_random_uuid(), 'Marble Press', 'marble-press', now()
                WHERE NOT EXISTS (SELECT 1 FROM tenants WHERE lower(slug) = 'marble-press')
                RETURNING id::text, slug
                """
            )
        except Exception:
            cur.execute(
                """
                INSERT INTO tenants (id, name, slug)
                SELECT gen_random_uuid(), 'Marble Press', 'marble-press'
                WHERE NOT EXISTS (SELECT 1 FROM tenants WHERE lower(slug) = 'marble-press')
                RETURNING id::text, slug
                """
            )
        row = cur.fetchone()
    if row:
        print("Tenant 'marble-press' created:", row)
    else:
        print("Tenant 'marble-press' already exists.")
    return 0

if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as e:
        print("Error:", e)
        sys.exit(1)
