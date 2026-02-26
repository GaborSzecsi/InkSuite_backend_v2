# Run from backend project root:  python scripts/check_db.py
# Uses the same .env and db config as the app (main.py).
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
        print("  Add DATABASE_URL=postgresql://user:password@host:port/dbname to", env_path)
        sys.exit(1)

    # Redact password for display
    try:
        from urllib.parse import urlparse
        u = urlparse(raw)
        if u.password:
            safe = raw.replace(u.password, "***", 1)
        else:
            safe = raw
    except Exception:
        safe = raw[:50] + "..." if len(raw) > 50 else raw
    print("DATABASE_URL (masked):", safe)
    print()

    try:
        from app.core.db import db_conn
        with db_conn() as conn, conn.cursor() as cur:
            cur.execute("SELECT 1")
            cur.fetchone()
        print("OK: Database connection succeeded.")
        sys.exit(0)
    except Exception as e:
        print("ERROR: Connection failed:")
        print(" ", type(e).__name__, str(e))
        print()
        print("Checks:")
        print("  1. Is PostgreSQL running? (Or is your tunnel to the DB running?)")
        print("  2. Host/port in DATABASE_URL correct? For a tunnel, use the local host/port the tunnel listens on.")
        print("  3. If using a tunnel or local Postgres without SSL, add to DATABASE_URL:  ?sslmode=disable")
        print("  4. Run this script from the same machine and folder where you run: python -m uvicorn main:app")
        sys.exit(1)
