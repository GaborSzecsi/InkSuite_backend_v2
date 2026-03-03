#!/usr/bin/env python3
"""
Run tenant settings table migration from Windows (no psql needed).
Loads .env from repo root, then executes docs/SCHEMA_tenant_settings.sql.
Usage: from backend root, with Python env that has psycopg and python-dotenv:
  python scripts/run_tenant_settings_migration.py
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

# Repo root = parent of scripts/
REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

# Load .env so DATABASE_URL is set
from dotenv import load_dotenv
env_path = REPO_ROOT / ".env"
if env_path.exists():
    load_dotenv(dotenv_path=env_path, override=False)
else:
    load_dotenv(dotenv_path=REPO_ROOT.parent / ".env", override=False)

DATABASE_URL = os.environ.get("DATABASE_URL", "").strip()
if not DATABASE_URL:
    print("ERROR: DATABASE_URL not set. Set it in .env or the environment.")
    sys.exit(1)

sql_file = REPO_ROOT / "docs" / "SCHEMA_tenant_settings.sql"
if not sql_file.exists():
    print(f"ERROR: {sql_file} not found.")
    sys.exit(1)

sql = sql_file.read_text(encoding="utf-8")
# Drop comment-only lines and split into statements by semicolon
statements = [
    s.strip() for s in sql.split(";")
    if s.strip() and not s.strip().startswith("--")
]

import psycopg
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

def _normalize_database_url(raw: str) -> str:
    raw = (raw or "").strip()
    if raw.startswith("postgresql+psycopg://"):
        raw = "postgresql://" + raw[len("postgresql+psycopg://"):]
    u = urlparse(raw)
    q = dict(parse_qsl(u.query, keep_blank_values=True))
    q.setdefault("sslmode", "prefer")
    q.setdefault("connect_timeout", "30")
    return urlunparse((u.scheme, u.netloc, u.path, u.params, urlencode(q), u.fragment))

dsn = _normalize_database_url(DATABASE_URL)
print("Connecting to database...")
with psycopg.connect(dsn) as conn:
    with conn.cursor() as cur:
        for stmt in statements:
            if stmt:
                cur.execute(stmt + ";")
    conn.commit()
print("Done. Tables tenant_org_profile and tenant_email_settings are ready.")
print("Migration completed successfully.")
