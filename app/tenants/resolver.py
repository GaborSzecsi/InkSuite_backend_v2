# app/tenants/resolver.py
from __future__ import annotations

import os
from typing import Optional

try:
    from app.core.db import db_conn
except Exception:
    db_conn = None  # local/dev without DB


def _db_available() -> bool:
    if db_conn is None:
        return False
    return bool((os.environ.get("DATABASE_URL") or "").strip())


def resolve_tenant(slug: str) -> Optional[dict]:
    """
    Resolve tenant by slug. Return dict with id, slug, name or None.
    Uses DB when available; otherwise returns None (so callers can 404 cleanly).
    """
    if not slug or not slug.strip():
        return None
    slug = slug.strip().lower()

    if not _db_available():
        # In production you WANT this to be None so endpoints don't silently allow access.
        return None

    with db_conn() as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT id, slug, name FROM tenants WHERE lower(slug) = lower(%s)",
            (slug,),
        )
        row = cur.fetchone()
        if not row:
            return None
        tenant_id, tenant_slug, tenant_name = row
        return {"id": tenant_id, "slug": tenant_slug, "name": tenant_name}