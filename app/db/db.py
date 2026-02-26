# app/core/db.py  (COMPLETE DROP-IN)
from __future__ import annotations

import os
from contextlib import contextmanager
from typing import Tuple
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

import psycopg


def _is_local_host(netloc: str) -> bool:
    """
    netloc may look like:
      user:pass@host:5432
      host:5432
      user@host
    We'll extract host conservatively.
    """
    if not netloc:
        return True
    hostpart = netloc.rsplit("@", 1)[-1]  # drop creds if present
    host = hostpart.split(":", 1)[0].strip().lower()
    return host in ("localhost", "127.0.0.1", "::1")


def _normalize_database_url(raw: str) -> str:
    """
    Accept SQLAlchemy-style URLs (postgresql+psycopg://) and convert to psycopg (postgresql://).

    SSL behavior:
      - If sslmode is explicitly provided in DATABASE_URL, we respect it.
      - Otherwise:
          * localhost/127.0.0.1/::1 -> sslmode=disable
          * anything else            -> sslmode=require

    Also ensures connect_timeout is set (default 5s) unless explicitly provided.
    """
    raw = (raw or "").strip()
    if not raw:
        raise RuntimeError("DATABASE_URL is not set")

    if raw.startswith("postgresql+psycopg://"):
        raw = "postgresql://" + raw[len("postgresql+psycopg://") :]

    u = urlparse(raw)
    q = dict(parse_qsl(u.query, keep_blank_values=True))

    # Default sslmode only if not provided
    if "sslmode" not in q:
        q["sslmode"] = "disable" if _is_local_host(u.netloc) else "require"

    # Ensure a sane connection timeout unless user provided one
    if "connect_timeout" not in q:
        q["connect_timeout"] = "5"

    return urlunparse((u.scheme, u.netloc, u.path, u.params, urlencode(q), u.fragment))


@contextmanager
def db_conn():
    """
    Context manager that returns an autocommit psycopg connection.
    """
    dsn = _normalize_database_url(os.environ.get("DATABASE_URL", ""))
    with psycopg.connect(dsn) as conn:
        conn.autocommit = True
        yield conn


def db_ping() -> Tuple[bool, str]:
    """
    Returns (ok, message) so callers can show useful diagnostics.
    """
    try:
        dsn = _normalize_database_url(os.environ.get("DATABASE_URL", ""))
    except Exception as e:
        return False, f"{type(e).__name__}: {e}"

    try:
        with psycopg.connect(dsn) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                cur.fetchone()
        return True, "ok"
    except Exception as e:
        return False, f"{type(e).__name__}: {e}"