# app/tenants/router.py
from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, EmailStr

from app.tenants.dependencies import require_tenant_access, require_role
from app.auth.service import (
    TENANT_ADMIN,
    TENANT_EDITOR,
    create_invite,
)

router = APIRouter(prefix="/tenants", tags=["Tenants"])


# -------------------------------------------------
# Schemas
# -------------------------------------------------
class InviteCreate(BaseModel):
    email: EmailStr
    role: str  # tenant_admin | tenant_editor
    ttl_hours: int | None = 72


# -------------------------------------------------
# List tenant members (ADMIN ONLY)
# -------------------------------------------------
@router.get("/{tenant_slug}/members")
def list_members(
    ctx: dict = Depends(require_role([TENANT_ADMIN])),
):
    """
    List members of a tenant.
    Admin-only.
    """
    tenant = ctx["tenant"]

    from app.core.db import db_conn

    with db_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                u.id::text,
                u.email,
                u.platform_role,
                m.role,
                u.created_at
            FROM memberships m
            JOIN users u ON u.id = m.user_id
            WHERE m.tenant_id = %s
            ORDER BY u.email
            """,
            (tenant["id"],),
        )
        rows = cur.fetchall()

    return {
        "ok": True,
        "tenant": tenant["slug"],
        "members": [
            {
                "user_id": r[0],
                "email": r[1],
                "platform_role": r[2],
                "tenant_role": r[3],
                "created_at": r[4].isoformat() if r[4] else None,
            }
            for r in rows
        ],
    }


# -------------------------------------------------
# Create invite (ADMIN ONLY)
# -------------------------------------------------
@router.post("/{tenant_slug}/invites")
def invite_user(
    payload: InviteCreate,
    ctx: dict = Depends(require_role([TENANT_ADMIN])),
):
    """
    Invite a user to the tenant.
    Admin-only.
    """
    tenant = ctx["tenant"]
    user = ctx["user"]

    try:
        invite = create_invite(
            invited_by_user_id=user["id"],
            tenant_slug=tenant["slug"],
            email=str(payload.email),
            role=payload.role,
            ttl_hours=int(payload.ttl_hours or 72),
        )
    except ValueError as e:
        msg = str(e)
        if msg == "invalid_role":
            raise HTTPException(status_code=400, detail="Invalid role")
        if msg == "tenant_not_found":
            raise HTTPException(status_code=404, detail="Tenant not found")
        raise HTTPException(status_code=400, detail="Bad request")

    # For now: token is returned for copy/paste testing
    return {
        "ok": True,
        "invite": invite,
    }