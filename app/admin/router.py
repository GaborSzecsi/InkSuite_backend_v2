# app/admin/router.py — superadmin-only: list/create tenants
from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel

from app.auth.dependencies import require_superadmin

router = APIRouter(prefix="/admin", tags=["Admin"])


class CreateTenantBody(BaseModel):
    name: str
    slug: str


@router.get("/tenants")
def list_tenants(
    _user: dict = Depends(require_superadmin),
):
    """List all tenants. Superadmin only."""
    from app.core.db import db_conn

    with db_conn() as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT id::text, name, slug, created_at FROM tenants ORDER BY slug"
        )
        rows = cur.fetchall()
    return {
        "ok": True,
        "tenants": [
            {"id": r[0], "name": r[1], "slug": r[2], "created_at": r[3].isoformat() if r[3] else None}
            for r in rows
        ],
    }


@router.post("/tenants")
def create_tenant(
    payload: CreateTenantBody,
    _user: dict = Depends(require_superadmin),
):
    """Create a new tenant. Superadmin only."""
    name = (payload.name or "").strip()
    slug = (payload.slug or "").strip().lower()
    if not slug:
        raise HTTPException(status_code=400, detail="slug required")
    from app.core.db import db_conn

    with db_conn() as conn, conn.cursor() as cur:
        cur.execute(
            "INSERT INTO tenants (name, slug) VALUES (%s, %s) ON CONFLICT (slug) DO NOTHING RETURNING id::text, name, slug",
            (name or slug, slug),
        )
        row = cur.fetchone()
    if not row:
        raise HTTPException(status_code=409, detail="Tenant with this slug already exists")
    return {"ok": True, "tenant": {"id": row[0], "name": row[1], "slug": row[2]}}
