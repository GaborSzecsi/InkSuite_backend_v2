# app/routers/contract_invites.py
from __future__ import annotations

import hashlib
import os
import secrets
import smtplib
from datetime import datetime, timedelta, timezone
from email.message import EmailMessage
from typing import Any, Optional, Tuple

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel, EmailStr

from app.core.db import db_conn
from app.tenants.dependencies import require_role

# Reuse your existing draft lookup from the big contract module
from routers.contract_docs import _find_draft

# Try to allow contract-manager / tenant_user flows.
# If your project doesn't have TENANT_USER, this will fall back safely.
try:
    from app.auth.service import TENANT_USER  # type: ignore
except Exception:  # pragma: no cover
    TENANT_USER = None  # type: ignore

try:
    from app.auth.service import TENANT_ADMIN  # type: ignore
except Exception:  # pragma: no cover
    TENANT_ADMIN = None  # type: ignore


router = APIRouter(prefix="/contracts", tags=["Contracts"])


class AgentInviteIn(BaseModel):
    name: str = ""
    email: EmailStr


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _expires_at(days: int = 7) -> datetime:
    return _now_utc() + timedelta(days=days)


def _token_hash(raw_token: str) -> str:
    return hashlib.sha256(raw_token.encode("utf-8")).hexdigest()


def _frontend_base_url() -> str:
    # Local dev: set FRONTEND_BASE_URL=http://localhost:3000
    return (os.getenv("FRONTEND_BASE_URL") or "https://www.inksuite.io").rstrip("/")


def _resolve_ctx_tenant_slug(ctx: Any) -> str:
    tenant = (ctx or {}).get("tenant") or {}
    return (tenant.get("slug") or "").strip()


def _resolve_ctx_user_id(ctx: Any) -> Optional[str]:
    user = (ctx or {}).get("user") or {}
    uid = user.get("id") or user.get("user_id") or user.get("sub")
    return str(uid) if uid else None


def _can_invite(ctx: Any) -> bool:
    """
    Minimal gate:
    - If module_permissions exists, require contracts permission not False.
    - Otherwise allow tenant_admin/contract_manager if membership_role exists.
    """
    mp = (ctx or {}).get("module_permissions") or {}
    if isinstance(mp, dict) and mp.get("contracts") is False:
        return False

    role = (ctx or {}).get("membership_role") or (ctx or {}).get("role") or ""
    role = str(role).lower()
    if role in {"tenant_admin", "contract_manager", "contracts_manager"}:
        return True

    # If require_role already enforced a tenant role, allow by default.
    return True


def _find_draft_or_404(draft_id: str) -> dict:
    """
    Uses routers.contract_docs._find_draft(draft_id) and raises 404 if missing.
    """
    it = _find_draft(draft_id)
    if not it:
        raise HTTPException(status_code=404, detail="Draft not found")
    return it


def _get_sender_defaults(tenant_slug: str) -> Tuple[str, str]:
    """
    Reads from tenant_email_settings:
      from_name, from_email
    """
    from_name = ""
    from_email = ""
    with db_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT from_name, from_email
            FROM tenant_email_settings
            WHERE tenant_slug = %s
            """,
            (tenant_slug,),
        )
        row = cur.fetchone()
        if row:
            from_name = row[0] or ""
            from_email = row[1] or ""
    return from_name, from_email


def _smtp_config() -> Optional[dict]:
    host = os.getenv("SMTP_HOST") or ""
    port = int(os.getenv("SMTP_PORT") or "0")
    user = os.getenv("SMTP_USER") or ""
    password = os.getenv("SMTP_PASS") or ""
    tls_mode = (os.getenv("SMTP_TLS_MODE") or "starttls").lower()  # starttls | ssl

    if not host or not port:
        return None

    return {"host": host, "port": port, "user": user, "password": password, "tls_mode": tls_mode}


def _send_invite_email(
    *,
    to_email: str,
    to_name: str,
    from_email: str,
    from_name: str,
    subject: str,
    review_link: str,
    expires_at: datetime,
) -> Optional[str]:
    """
    Returns None on success, or a warning string if email could not be sent.
    """
    cfg = _smtp_config()
    if not cfg:
        return "SMTP not configured (SMTP_HOST/SMTP_PORT). Invite was created but email was not sent."

    msg = EmailMessage()
    msg["To"] = f"{to_name} <{to_email}>" if to_name else to_email
    msg["From"] = f"{from_name} <{from_email}>" if from_name and from_email else (from_email or "noreply@example.com")
    msg["Subject"] = subject

    exp_str = expires_at.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    body = (
        f"Hello {to_name or ''}".strip()
        + "\n\n"
        + "Please review the draft contract at the link below:\n\n"
        + f"{review_link}\n\n"
        + "You can add comments and suggest edits directly in the document.\n\n"
        + f"This link expires on {exp_str}. Please do not forward this link.\n\n"
        + "Marble Press Support Team,\n"
        + (from_name or "").strip()
    )
    msg.set_content(body)

    try:
        if cfg["tls_mode"] == "ssl":
            with smtplib.SMTP_SSL(cfg["host"], cfg["port"]) as server:
                if cfg["user"] and cfg["password"]:
                    server.login(cfg["user"], cfg["password"])
                server.send_message(msg)
        else:
            with smtplib.SMTP(cfg["host"], cfg["port"]) as server:
                server.ehlo()
                server.starttls()
                server.ehlo()
                if cfg["user"] and cfg["password"]:
                    server.login(cfg["user"], cfg["password"])
                server.send_message(msg)
        return None
    except Exception as e:  # pragma: no cover
        return f"Email send failed: {e}"


def _insert_invite(
    *,
    tenant_slug: str,
    draft_id: str,
    invitee_name: str,
    invitee_email: str,
    token_hash: str,
    expires_at: datetime,
    created_by_user_id: Optional[str],
) -> str:
    with db_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO contract_draft_invites
              (tenant_slug, draft_id, invitee_name, invitee_email, token_hash, status, expires_at, created_by_user_id, last_sent_at)
            VALUES
              (%s, %s, %s, %s, %s, 'sent', %s, %s, now())
            RETURNING id
            """,
            (tenant_slug, draft_id, invitee_name, invitee_email, token_hash, expires_at, created_by_user_id),
        )
        row = cur.fetchone()
        conn.commit()
        return str(row[0])


def _get_invite_by_token_hash(token_hash: str) -> Optional[dict]:
    with db_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT id, tenant_slug, draft_id, invitee_name, invitee_email, status, expires_at
            FROM contract_draft_invites
            WHERE token_hash = %s
            """,
            (token_hash,),
        )
        row = cur.fetchone()
        if not row:
            return None
        return {
            "id": str(row[0]),
            "tenant_slug": row[1],
            "draft_id": row[2],
            "invitee_name": row[3] or "",
            "invitee_email": row[4],
            "status": row[5],
            "expires_at": row[6],
        }


def _mark_expired_if_needed(invite_id: str, expires_at: datetime, status: str) -> str:
    if status == "expired":
        return "expired"
    if expires_at <= _now_utc():
        with db_conn() as conn:
            cur = conn.cursor()
            cur.execute(
                """
                UPDATE contract_draft_invites
                SET status = 'expired', updated_at = now()
                WHERE id = %s AND status NOT IN ('revoked','expired')
                """,
                (invite_id,),
            )
            conn.commit()
        return "expired"
    return status


def _auth_dep():
    if TENANT_USER:
        return require_role(TENANT_USER)
    if TENANT_ADMIN:
        return require_role(TENANT_ADMIN)
    return require_role("TENANT_USER")


@router.get("/draft-contracts/{draft_id}/invite-defaults")
def invite_defaults_for_draft(
    draft_id: str,
    ctx=Depends(_auth_dep()),
):
    if not _can_invite(ctx):
        raise HTTPException(status_code=403, detail="Not permitted to invite reviewers")

    it = _find_draft_or_404(draft_id)

    tenant_slug = _resolve_ctx_tenant_slug(ctx)
    if not tenant_slug:
        raise HTTPException(status_code=400, detail="Could not resolve tenant")

    from_name, from_email = _get_sender_defaults(tenant_slug)
    title = it.get("title") or it.get("filename") or "Draft Contract"

    return {
        "from_name": from_name,
        "from_email": from_email,
        "subject": f"Action Required: Review Draft Contract ({title})",
        "message": "Please review the draft contract at the link below. You can add comments and suggest edits directly in the document.",
    }


@router.post("/draft-contracts/{draft_id}/invites")
def create_invite_for_draft(
    draft_id: str,
    payload: AgentInviteIn,
    request: Request,
    ctx=Depends(_auth_dep()),
):
    if not _can_invite(ctx):
        raise HTTPException(status_code=403, detail="Not permitted to invite reviewers")

    _ = _find_draft_or_404(draft_id)

    tenant_slug = _resolve_ctx_tenant_slug(ctx)
    if not tenant_slug:
        raise HTTPException(status_code=400, detail="Could not resolve tenant")

    raw_token = secrets.token_urlsafe(32)
    th = _token_hash(raw_token)
    exp = _expires_at(days=int(os.getenv("INVITE_EXPIRES_DAYS") or "7"))
    created_by = _resolve_ctx_user_id(ctx)

    invite_id = _insert_invite(
        tenant_slug=tenant_slug,
        draft_id=draft_id,
        invitee_name=(payload.name or "").strip(),
        invitee_email=str(payload.email).strip().lower(),
        token_hash=th,
        expires_at=exp,
        created_by_user_id=created_by,
    )

    review_url = f"{_frontend_base_url()}/app/contracts/invites/{raw_token}"

    from_name, from_email = _get_sender_defaults(tenant_slug)
    warn = _send_invite_email(
        to_email=str(payload.email),
        to_name=(payload.name or "").strip(),
        from_email=from_email or os.getenv("DEFAULT_FROM_EMAIL", "noreply@marblepress.com"),
        from_name=from_name or os.getenv("DEFAULT_FROM_NAME", "Marble Press Support Team"),
        subject="Action Required: Review Draft Contract",
        review_link=review_url,
        expires_at=exp,
    )

    resp = {
        "ok": True,
        "invite_id": invite_id,
        "tenant_slug": tenant_slug,
        "draft_id": draft_id,
        "invitee_email": str(payload.email),
        "invite_url": review_url,
    }
    if warn:
        resp["warning"] = warn
    return resp


# NOTE: Removed the invalid "/../tenants/..." alias routes.
# If you want tenant-scoped routes later, we add a second APIRouter with prefix="/tenants/{tenant_slug}/contracts".


@router.get("/invites/{token}")
def resolve_invite_token(token: str):
    th = _token_hash(token)
    inv = _get_invite_by_token_hash(th)
    if not inv:
        raise HTTPException(status_code=404, detail="Invite not found")

    status = _mark_expired_if_needed(invite_id=inv["id"], expires_at=inv["expires_at"], status=inv["status"])
    if status in {"revoked", "expired"}:
        raise HTTPException(status_code=410, detail=f"Invite {status}")

    _ = _find_draft_or_404(inv["draft_id"])

    return {
        "ok": True,
        "invite_id": inv["id"],
        "tenant_slug": inv["tenant_slug"],
        "draft_id": inv["draft_id"],
        "invitee_name": inv["invitee_name"],
        "invitee_email": inv["invitee_email"],
        "status": status,
        "expires_at": inv["expires_at"],
        # Next step:
        # "collabora": {"editorUrl": "...", "wopiSrc": "...", "accessToken": "..."}
    }