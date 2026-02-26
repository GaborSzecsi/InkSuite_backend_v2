# app/tenants/router.py
from __future__ import annotations

import json
import os
import secrets
from datetime import datetime, timedelta, timezone
from typing import Any, Dict

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, EmailStr

from app.tenants.dependencies import require_role
from app.auth.service import (
    TENANT_ADMIN,
    TENANT_EDITOR,
    EDITOR,
    create_invite,
)

# SES mailer (expects invite_link, not token)
from app.email.ses_mailer import send_invite_email

router = APIRouter(prefix="/tenants", tags=["Tenants"])


# -------------------------------------------------
# Config helpers
# -------------------------------------------------
def _app_origin() -> str:
    """
    Public origin used to build invite links.
    Set PUBLIC_APP_ORIGIN for prod + local.

    Examples:
      http://localhost:3000
      https://www.inksuite.io
    """
    origin = (os.environ.get("PUBLIC_APP_ORIGIN") or "").strip()
    if origin:
        return origin.rstrip("/")
    return "http://localhost:3000"


def _invite_link(token: str) -> str:
    return f"{_app_origin()}/accept-invite/{token}"


def _normalize_role(role: str) -> str:
    """
    Accept legacy 'tenant_editor' and normalize to 'editor' for storage.
    """
    r = (role or "").strip().lower()
    if r == TENANT_EDITOR:
        return EDITOR
    return r


def _safe_bool_dict(d: dict | None) -> dict:
    if not isinstance(d, dict):
        return {}
    out: dict = {}
    for k, v in d.items():
        out[str(k)] = bool(v)
    return out


# -------------------------------------------------
# Schemas
# -------------------------------------------------
class InviteCreate(BaseModel):
    email: EmailStr
    role: str  # tenant_admin | tenant_editor | editor
    ttl_hours: int | None = 72
    module_permissions: dict | None = None  # for editors: {"royalty": true, "books": true}


class UpdateRoleBody(BaseModel):
    role: str  # tenant_admin | editor | tenant_editor


class UpdatePermissionsBody(BaseModel):
    module_permissions: dict  # e.g. {"royalty": false, "books": true}


# -------------------------------------------------
# List tenant members (ADMIN ONLY)
# -------------------------------------------------
@router.get("/{tenant_slug}/members")
def list_members(ctx: dict = Depends(require_role([TENANT_ADMIN]))):
    tenant = ctx["tenant"]

    from app.core.db import db_conn

    with db_conn() as conn, conn.cursor() as cur:
        try:
            cur.execute(
                """
                SELECT
                    u.id::text,
                    u.email,
                    u.platform_role,
                    m.role,
                    u.created_at,
                    m.module_permissions
                FROM memberships m
                JOIN users u ON u.id = m.user_id
                WHERE m.tenant_id = %s
                ORDER BY u.email
                """,
                (tenant["id"],),
            )
            rows = cur.fetchall()
        except Exception:
            # Back-compat if module_permissions not present
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
            rows = [r + (None,) for r in cur.fetchall()]

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
                "module_permissions": (r[5] or {}) if len(r) > 5 else {},
            }
            for r in rows
        ],
    }


# -------------------------------------------------
# Create invite (ADMIN ONLY) + SEND EMAIL
#
# IMPORTANT:
# - We send the email here (router layer) only.
# - app.auth.service.create_invite() MUST NOT send email.
#   (Remove any email sending inside create_invite to avoid duplicates.)
# -------------------------------------------------
@router.post("/{tenant_slug}/invites")
def invite_user(payload: InviteCreate, ctx: dict = Depends(require_role([TENANT_ADMIN]))):
    tenant = ctx["tenant"]
    user = ctx["user"]

    role = _normalize_role(payload.role)
    if role not in (TENANT_ADMIN, EDITOR):
        raise HTTPException(status_code=400, detail="Invalid role")

    ttl_hours = int(payload.ttl_hours or 72)
    if ttl_hours <= 0 or ttl_hours > (24 * 30):
        # keep it sane: up to 30 days
        raise HTTPException(status_code=400, detail="Invalid ttl_hours")

    module_permissions = _safe_bool_dict(payload.module_permissions) if role == EDITOR else {}

    try:
        invite = create_invite(
            invited_by_user_id=user["id"],
            tenant_slug=tenant["slug"],
            email=str(payload.email),
            role=role,
            ttl_hours=ttl_hours,
            module_permissions=module_permissions,
        )
    except ValueError as e:
        msg = str(e)
        if msg == "invalid_role":
            raise HTTPException(status_code=400, detail="Invalid role")
        if msg == "tenant_not_found":
            raise HTTPException(status_code=404, detail="Tenant not found")
        if msg == "company_required":
            # in case your service enforces a tenant slug
            raise HTTPException(status_code=400, detail="Tenant is required")
        raise HTTPException(status_code=400, detail="Bad request")

    token = invite.get("token")
    link = _invite_link(token) if token else None

    # Send email best-effort; do not fail invite creation if SES fails.
    email_error: str | None = None
    if link:
        try:
            # SES Sandbox only delivers to VERIFIED recipient emails
            send_invite_email(
                to_email=invite["email"],
                invite_link=link,
                tenant_slug=tenant["slug"],
                role=invite.get("role", ""),
                invited_by_email=user.get("email", ""),
            )
        except Exception as e:
            email_error = str(e)

    resp: Dict[str, Any] = {"ok": True, "invite": invite, "invite_link": link}
    if email_error:
        resp["email_error"] = email_error
    return resp


# -------------------------------------------------
# List pending invites (ADMIN ONLY)
# -------------------------------------------------
@router.get("/{tenant_slug}/invites")
def list_pending_invites(ctx: dict = Depends(require_role([TENANT_ADMIN]))):
    tenant = ctx["tenant"]
    from app.core.db import db_conn

    with db_conn() as conn, conn.cursor() as cur:
        try:
            cur.execute(
                """
                SELECT
                    i.id::text,
                    i.email,
                    i.role,
                    i.expires_at,
                    i.created_at,
                    i.module_permissions
                FROM invites i
                WHERE i.tenant_id = %s
                  AND i.accepted_at IS NULL
                  AND i.revoked_at IS NULL
                ORDER BY i.created_at DESC
                """,
                (tenant["id"],),
            )
            rows = cur.fetchall()
        except Exception:
            cur.execute(
                """
                SELECT
                    i.id::text,
                    i.email,
                    i.role,
                    i.expires_at,
                    i.created_at
                FROM invites i
                WHERE i.tenant_id = %s
                  AND i.accepted_at IS NULL
                ORDER BY i.created_at DESC
                """,
                (tenant["id"],),
            )
            rows = [r + (None,) for r in cur.fetchall()]

    invites_list = []
    for r in rows:
        invites_list.append(
            {
                "invite_id": r[0],
                "email": r[1],
                "role": r[2],
                "expires_at": r[3].isoformat() if r[3] else None,
                "created_at": r[4].isoformat() if r[4] else None,
                "module_permissions": r[5] or {},
            }
        )

    return {"ok": True, "tenant": tenant["slug"], "invites": invites_list}


# -------------------------------------------------
# Update user role (ADMIN ONLY)
# -------------------------------------------------
@router.put("/{tenant_slug}/users/{user_id}/role")
def update_user_role(
    user_id: str,
    payload: UpdateRoleBody,
    ctx: dict = Depends(require_role([TENANT_ADMIN])),
):
    tenant = ctx["tenant"]
    role = _normalize_role(payload.role)

    if role not in (TENANT_ADMIN, EDITOR):
        raise HTTPException(status_code=400, detail="Invalid role")

    from app.core.db import db_conn

    with db_conn() as conn, conn.cursor() as cur:
        # Ensure membership exists and update role in one statement.
        cur.execute(
            """
            INSERT INTO memberships (tenant_id, user_id, role)
            VALUES (%s, %s, %s)
            ON CONFLICT (tenant_id, user_id)
            DO UPDATE SET role = EXCLUDED.role
            RETURNING tenant_id::text, user_id::text, role
            """,
            (tenant["id"], user_id, role),
        )
        row = cur.fetchone()
        conn.commit()

    if not row:
        # Should never happen, but keep it safe.
        raise HTTPException(status_code=500, detail="Failed to update role")

    return {"ok": True, "user_id": row[1], "role": row[2]}

# -------------------------------------------------
# Update editor module permissions (ADMIN ONLY)
# -------------------------------------------------
def _ensure_module_permissions_column(conn) -> None:
    """Add module_permissions column if missing (self-healing migration)."""
    with conn.cursor() as cur:
        cur.execute("""
            ALTER TABLE memberships
            ADD COLUMN IF NOT EXISTS module_permissions JSONB DEFAULT '{}'::jsonb
        """)
        cur.execute("""
            ALTER TABLE invites
            ADD COLUMN IF NOT EXISTS module_permissions JSONB DEFAULT '{}'::jsonb
        """)


@router.put("/{tenant_slug}/users/{user_id}/permissions")
def update_user_permissions(
    user_id: str,
    payload: UpdatePermissionsBody,
    ctx: dict = Depends(require_role([TENANT_ADMIN])),
):
    tenant = ctx["tenant"]
    perm = _safe_bool_dict(payload.module_permissions)

    from app.core.db import db_conn

    import psycopg.errors as pg_errors

    with db_conn() as conn, conn.cursor() as cur:
        try:
            cur.execute(
                """
                UPDATE memberships
                SET module_permissions = %s::jsonb
                WHERE tenant_id = %s AND user_id = %s
                RETURNING tenant_id
                """,
                (json.dumps(perm), str(tenant["id"]), str(user_id)),
            )
        except pg_errors.UndefinedColumn:
            conn.rollback()
            _ensure_module_permissions_column(conn)
            # Retry the UPDATE
            cur.execute(
                """
                UPDATE memberships
                SET module_permissions = %s::jsonb
                WHERE tenant_id = %s AND user_id = %s
                RETURNING tenant_id
                """,
                (json.dumps(perm), str(tenant["id"]), str(user_id)),
            )
        except Exception as e:
            # Return 400 with actual error so the UI can show it (e.g. type error, constraint)
            raise HTTPException(status_code=400, detail=f"Update failed: {e}")

        if not cur.fetchone():
            raise HTTPException(status_code=404, detail="User not found in this tenant")

        conn.commit()

    return {"ok": True, "user_id": user_id, "module_permissions": perm}


# -------------------------------------------------
# Revoke access: remove user from tenant (ADMIN ONLY)
# -------------------------------------------------
@router.delete("/{tenant_slug}/users/{user_id}")
def revoke_user_access(user_id: str, ctx: dict = Depends(require_role([TENANT_ADMIN]))):
    tenant = ctx["tenant"]
    from app.core.db import db_conn

    with db_conn() as conn, conn.cursor() as cur:
        cur.execute(
            "DELETE FROM memberships WHERE tenant_id = %s AND user_id = %s RETURNING tenant_id",
            (tenant["id"], user_id),
        )
        if not cur.fetchone():
            raise HTTPException(status_code=404, detail="User not found in this tenant")
        conn.commit()

    return {"ok": True, "user_id": user_id}


# -------------------------------------------------
# Cancel invite (ADMIN ONLY)
# -------------------------------------------------
@router.delete("/{tenant_slug}/invites/{invite_id}")
def cancel_invite(invite_id: str, ctx: dict = Depends(require_role([TENANT_ADMIN]))):
    tenant = ctx["tenant"]
    from app.core.db import db_conn

    with db_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            DELETE FROM invites
            WHERE id = %s AND tenant_id = %s AND accepted_at IS NULL
            RETURNING id
            """,
            (invite_id, tenant["id"]),
        )
        if not cur.fetchone():
            raise HTTPException(status_code=404, detail="Invite not found or already accepted")
        conn.commit()

    return {"ok": True, "invite_id": invite_id}


# -------------------------------------------------
# Resend invite: new token + SEND EMAIL (ADMIN ONLY)
# -------------------------------------------------
@router.post("/{tenant_slug}/invites/{invite_id}/resend")
def resend_invite(invite_id: str, ctx: dict = Depends(require_role([TENANT_ADMIN]))):
    tenant = ctx["tenant"]
    user = ctx["user"]
    from app.core.db import db_conn

    with db_conn() as conn, conn.cursor() as cur:
        # Prefer filtering by revoked_at when column exists; fallback for older schema
        try:
            cur.execute(
                """
                SELECT id, email, role
                FROM invites
                WHERE id = %s
                  AND tenant_id = %s
                  AND accepted_at IS NULL
                  AND revoked_at IS NULL
                """,
                (invite_id, tenant["id"]),
            )
        except Exception:
            conn.rollback()
            cur.execute(
                """
                SELECT id, email, role
                FROM invites
                WHERE id = %s
                  AND tenant_id = %s
                  AND accepted_at IS NULL
                """,
                (invite_id, tenant["id"]),
            )
        row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Invite not found or already accepted")

        inv_id, email, role = row
        new_token = secrets.token_urlsafe(32)
        expires_at = datetime.now(timezone.utc) + timedelta(hours=72)

        # Store as naive UTC timestamp if your schema is timestamp-without-time-zone
        cur.execute(
            "UPDATE invites SET token = %s, expires_at = %s WHERE id = %s",
            (new_token, expires_at.replace(tzinfo=None), inv_id),
        )
        conn.commit()

    link = _invite_link(new_token)

    email_result = send_invite_email(
        to_email=str(email),
        invite_link=link,
        tenant_slug=tenant["slug"],
        role=str(role or ""),
        invited_by_email=user.get("email", ""),
    )
    email_error: str | None = email_result.error if (not email_result.ok and email_result.error) else None

    resp: Dict[str, Any] = {
        "ok": True,
        "invite_id": str(invite_id),
        "token": new_token,
        "expires_at": expires_at.isoformat(),
        "invite_link": link,
    }
    if email_error:
        resp["email_error"] = email_error
    return resp