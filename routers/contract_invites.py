# app/routers/contract_invites.py
from __future__ import annotations

import hashlib
import json
import os
import secrets
import smtplib
from datetime import datetime, timedelta, timezone
from email.message import EmailMessage
from typing import Any, Optional, Dict

import boto3
from botocore.exceptions import BotoCoreError, ClientError
from fastapi import APIRouter, Depends, HTTPException, Request, Query
from pydantic import BaseModel, EmailStr

from app.core.db import db_conn
from app.email.templates import render_invite_agent_email
from routers.contract_docs import _find_draft  # keep your existing import style

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
    return (os.getenv("FRONTEND_BASE_URL") or "https://www.inksuite.io").rstrip("/")


def _token_from_request(request: Request) -> Optional[str]:
    auth = (request.headers.get("Authorization") or "").strip()
    if auth.lower().startswith("bearer "):
        t = auth[7:].strip()
        if t:
            return t

    cookie = request.headers.get("Cookie") or ""
    for part in cookie.split(";"):
        part = part.strip()
        if part.lower().startswith("access_token="):
            return part[13:].strip() or None
    return None


def _ctx_from_bearer(request: Request):
    """
    Resolve user claims from request token so _load_user_and_membership_or_403 can use tenant_slug from query.
    Matches your existing approach.
    """
    claims = getattr(request.state, "user_claims", None)
    if claims:
        return claims

    token = _token_from_request(request)
    if not token:
        raise HTTPException(status_code=401, detail="Not authenticated")

    try:
        from app.auth.service import get_current_user_from_token
        claims = get_current_user_from_token(token)
    except Exception:
        claims = None

    if not claims:
        raise HTTPException(status_code=401, detail="Not authenticated")

    return claims


def _resolve_ctx_cognito_sub(ctx: Any) -> Optional[str]:
    if not ctx:
        return None
    if isinstance(ctx, dict):
        if ctx.get("sub"):
            return str(ctx.get("sub"))
        user = ctx.get("user") or {}
        if isinstance(user, dict):
            for k in ("sub", "cognito_sub", "id", "user_id"):
                if user.get(k):
                    return str(user.get(k))
    try:
        sub = getattr(ctx, "sub", None)
        if sub:
            return str(sub)
    except Exception:
        pass
    return None


def _find_draft_or_404(draft_id: str) -> dict:
    it = _find_draft(draft_id)
    if not it:
        raise HTTPException(status_code=404, detail="Draft not found")
    return it


def _load_smtp_secret(secret_id: str) -> tuple[str, str]:
    """
    Same behavior as your settings router:
    Secrets Manager SecretString must be JSON containing username/password keys.
    """
    if not secret_id:
        raise ValueError("SMTP secret id is not configured.")

    client = boto3.client("secretsmanager")
    try:
        resp = client.get_secret_value(SecretId=secret_id)
    except (ClientError, BotoCoreError) as e:
        raise ValueError(f"Failed to read SMTP secret from Secrets Manager: {e}")

    raw = resp.get("SecretString")
    if not raw and "SecretBinary" in resp:
        raw = resp["SecretBinary"]
        if isinstance(raw, (bytes, bytearray)):
            raw = raw.decode("utf-8", errors="ignore")

    if not raw:
        raise ValueError("SMTP secret is empty.")

    try:
        data = json.loads(raw)
    except Exception:
        raise ValueError("SMTP secret is not valid JSON.")

    username = data.get("username") or data.get("user") or data.get("smtp_username") or ""
    password = data.get("password") or data.get("pass") or data.get("smtp_password") or ""

    if not username or not password:
        raise ValueError("SMTP secret must contain username and password.")

    return str(username), str(password)


def _load_tenant_email_settings_or_400(tenant_slug: str) -> Dict[str, Any]:
    with db_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT from_name, from_email,
                   smtp_host, smtp_port, tls_mode,
                   smtp_username, smtp_secret_id, is_enabled
            FROM tenant_email_settings
            WHERE tenant_slug = %s
            """,
            (tenant_slug,),
        )
        row = cur.fetchone()

    if not row:
        raise HTTPException(status_code=400, detail="Email settings not configured for this tenant")

    from_name, from_email, smtp_host, smtp_port, tls_mode, smtp_username, smtp_secret_id, is_enabled = row

    if not is_enabled:
        raise HTTPException(status_code=400, detail="Email sending is disabled for this tenant")

    from_name = (from_name or "").strip()
    from_email = (from_email or "").strip()
    smtp_host = (smtp_host or "").strip()
    smtp_username = (smtp_username or "").strip()
    smtp_secret_id = (smtp_secret_id or "").strip()
    tls_mode = (tls_mode or "starttls").strip().lower()

    problems: list[str] = []
    if not from_email:
        problems.append("From email is not configured.")
    if not smtp_host:
        problems.append("SMTP host is not configured.")
    if not smtp_port:
        problems.append("SMTP port is not configured.")
    if not smtp_secret_id:
        problems.append("SMTP secret id is not configured.")
    if problems:
        raise HTTPException(status_code=400, detail=" ".join(problems))

    return {
        "from_name": from_name or from_email,
        "from_email": from_email,
        "smtp_host": smtp_host,
        "smtp_port": int(smtp_port or 587),
        "tls_mode": tls_mode,
        "smtp_secret_id": smtp_secret_id,
        # smtp_username in table is informational; actual login comes from secret (consistent with test route)
        "smtp_username_hint": smtp_username,
    }


def _send_email_smtp(
    *,
    smtp_host: str,
    smtp_port: int,
    tls_mode: str,
    username: str,
    password: str,
    from_email: str,
    from_name: str,
    to_email: str,
    to_name: str,
    subject: str,
    body_text: str,
) -> None:
    msg = EmailMessage()
    msg["To"] = f"{to_name} <{to_email}>" if to_name else to_email
    msg["From"] = f"{from_name} <{from_email}>" if from_name else from_email
    msg["Subject"] = subject
    msg.set_content(body_text)

    if tls_mode == "ssl":
        with smtplib.SMTP_SSL(smtp_host, smtp_port) as smtp:
            smtp.login(username, password)
            smtp.send_message(msg)
    else:
        with smtplib.SMTP(smtp_host, smtp_port) as smtp:
            smtp.ehlo()
            smtp.starttls()
            smtp.ehlo()
            smtp.login(username, password)
            smtp.send_message(msg)


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


def _load_user_and_membership_or_403(*, tenant_slug: str, ctx: Any) -> Dict[str, Any]:
    sub = _resolve_ctx_cognito_sub(ctx)
    if not sub:
        raise HTTPException(status_code=401, detail="Not authenticated")

    with db_conn() as conn:
        cur = conn.cursor()

        cur.execute("SELECT id FROM users WHERE cognito_sub = %s", (sub,))
        urow = cur.fetchone()
        if not urow:
            raise HTTPException(status_code=401, detail="User not found")
        user_id = str(urow[0])

        cur.execute("SELECT id FROM tenants WHERE slug = %s", (tenant_slug,))
        trow = cur.fetchone()
        if not trow:
            raise HTTPException(status_code=404, detail="Tenant not found")
        tenant_id = str(trow[0])

        cur.execute(
            """
            SELECT role, module_permissions
            FROM memberships
            WHERE tenant_id = %s AND user_id = %s
            """,
            (tenant_id, user_id),
        )
        mrow = cur.fetchone()
        if not mrow:
            raise HTTPException(status_code=403, detail="Not a member of this tenant")

        membership_role = (mrow[0] or "").strip()
        module_permissions = mrow[1] or {}

    return {
        "tenant_slug": tenant_slug,
        "tenant_id": tenant_id,
        "user_id": user_id,
        "membership_role": membership_role,
        "module_permissions": module_permissions,
    }


def _can_invite(mctx: Dict[str, Any]) -> bool:
    mp = mctx.get("module_permissions") or {}
    if isinstance(mp, dict) and mp.get("contracts") is False:
        return False

    role = (mctx.get("membership_role") or "").lower()
    if role in {"tenant_admin", "contract_manager", "contracts_manager"}:
        return True

    return True


@router.get("/draft-contracts/{draft_id}/invite-defaults")
def invite_defaults_for_draft(
    draft_id: str,
    tenant_slug: str = Query(..., description="Tenant slug, e.g. marble-press"),
    ctx=Depends(_ctx_from_bearer),
):
    mctx = _load_user_and_membership_or_403(tenant_slug=tenant_slug, ctx=ctx)
    if not _can_invite(mctx):
        raise HTTPException(status_code=403, detail="Not permitted to invite reviewers")

    it = _find_draft_or_404(draft_id)
    s = _load_tenant_email_settings_or_400(tenant_slug)
    title = it.get("title") or it.get("filename") or "Draft Contract"

    return {
        "from_name": s["from_name"],
        "from_email": s["from_email"],
        "subject": f"Action Required: Review Draft Contract ({title})",
        "message": "Enter the agent’s name and email address, then Send.",
    }


@router.post("/draft-contracts/{draft_id}/invites")
def create_invite_for_draft(
    draft_id: str,
    payload: AgentInviteIn,
    tenant_slug: str = Query(..., description="Tenant slug, e.g. marble-press"),
    ctx=Depends(_ctx_from_bearer),
):
    mctx = _load_user_and_membership_or_403(tenant_slug=tenant_slug, ctx=ctx)
    if not _can_invite(mctx):
        raise HTTPException(status_code=403, detail="Not permitted to invite reviewers")

    _ = _find_draft_or_404(draft_id)

    settings = _load_tenant_email_settings_or_400(tenant_slug)

    raw_token = secrets.token_urlsafe(32)
    th = _token_hash(raw_token)
    exp = _expires_at(days=int(os.getenv("INVITE_EXPIRES_DAYS") or "7"))

    invite_id = _insert_invite(
        tenant_slug=tenant_slug,
        draft_id=draft_id,
        invitee_name=(payload.name or "").strip(),
        invitee_email=str(payload.email).strip().lower(),
        token_hash=th,
        expires_at=exp,
        created_by_user_id=mctx["user_id"],
    )

    review_url = f"{_frontend_base_url()}/review/contracts/{raw_token}"

    subject, body_text = render_invite_agent_email(
        reviewer_name=(payload.name or "").strip() or "Reviewer",
        review_link=review_url,
        expires_at=exp,
        signature=settings["from_name"],  # Option 1: signature comes from DB from_name
    )

    try:
        username, password = _load_smtp_secret(settings["smtp_secret_id"])
        _send_email_smtp(
            smtp_host=settings["smtp_host"],
            smtp_port=settings["smtp_port"],
            tls_mode=settings["tls_mode"],
            username=username,
            password=password,
            from_email=settings["from_email"],
            from_name=settings["from_name"],
            to_email=str(payload.email).strip().lower(),
            to_name=(payload.name or "").strip(),
            subject=subject,
            body_text=body_text,
        )
    except ValueError as e:
        # Configuration/secret errors -> 400 so UI can show fixable message
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        # Send errors -> 500
        raise HTTPException(status_code=500, detail=f"Email send failed: {e}")

    return {
        "ok": True,
        "email_sent": True,
        "invite_id": invite_id,
        "tenant_slug": tenant_slug,
        "draft_id": draft_id,
        "invitee_email": str(payload.email),
        "invite_url": review_url,
        "expires_at": exp,
    }


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
    }