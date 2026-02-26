# app/core/db.py
from __future__ import annotations

import os
from contextlib import contextmanager
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

import psycopg


def _normalize_database_url(raw: str) -> str:
    raw = (raw or "").strip()
    if not raw:
        raise RuntimeError("DATABASE_URL is not set")
    if raw.startswith("postgresql+psycopg://"):
        raw = "postgresql://" + raw[len("postgresql+psycopg://") :]
    u = urlparse(raw)
    q = dict(parse_qsl(u.query, keep_blank_values=True))
    # Use "prefer" so tunnel/local Postgres without SSL still connect; set ?sslmode=require in URL for strict SSL.
    q.setdefault("sslmode", "prefer")
    # Avoid hanging if DB is unreachable (e.g. wrong host, firewall); use 30s for slow tunnels.
    q.setdefault("connect_timeout", "30")
    return urlunparse((u.scheme, u.netloc, u.path, u.params, urlencode(q), u.fragment))


@contextmanager
def db_conn():
    dsn = _normalize_database_url(os.environ.get("DATABASE_URL", ""))
    with psycopg.connect(dsn) as conn:
        conn.autocommit = True
        yield conn
