"""
Run the module_permissions migration using the same .env as the backend.
Use this so the migration runs against the exact DB the app uses.

  python scripts/run_module_permissions_migration.py

Run from backend project root (InkSuite_backend_v2).
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

if __name__ == "__main__":
    root = Path(__file__).resolve().parent.parent
    if str(root) not in sys.path:
        sys.path.insert(0, str(root))

    from dotenv import load_dotenv
    env_path = root / ".env"
    if not env_path.exists():
        env_path = root.parent / ".env"
    load_dotenv(dotenv_path=env_path, override=False)

    raw = (os.environ.get("DATABASE_URL") or "").strip()
    if not raw:
        print("ERROR: DATABASE_URL is not set in .env")
        print("  Backend uses:", env_path)
        sys.exit(1)

    # Use same db module as the app
    from app.core.db import db_conn

    sql_path = root / "docs" / "SCHEMA_module_permissions.sql"
    if not sql_path.exists():
        print("ERROR: not found", sql_path)
        sys.exit(1)
    sql = sql_path.read_text()

    print("Using same .env as backend:", env_path)
    print("Running migration: ADD COLUMN module_permissions to memberships and invites ...")
    try:
        with db_conn() as conn, conn.cursor() as cur:
            for stmt in sql.split(";"):
                stmt = stmt.strip()
                if not stmt or stmt.startswith("--"):
                    continue
                cur.execute(stmt)
        print("OK: Migration completed. Restart the backend is not required; try Save again.")
    except Exception as e:
        print("ERROR:", e)
        sys.exit(1)
