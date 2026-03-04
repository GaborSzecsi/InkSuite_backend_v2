# app/settings/router.py
# Extra tenant info (org profile, email settings) in the same DB as auth/users. Tables created on first use.
from __future__ import annotations

from datetime import datetime
from email.message import EmailMessage
import json
import smtplib
from typing import Literal, Optional

import boto3
from botocore.exceptions import BotoCoreError, ClientError
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, EmailStr, Field, field_validator
from app.auth.dependencies import get_current_user

from app.tenants.dependencies import require_role
from app.auth.service import TENANT_ADMIN
from app.core.db import db_conn

router = APIRouter(prefix="/tenants/{tenant_slug}/settings", tags=["Settings"])


_settings_tables_ensured = False


def _ensure_settings_tables():
    """Create tenant_org_profile and tenant_email_settings if missing. One DB, same as auth."""
    global _settings_tables_ensured
    if _settings_tables_ensured:
        return
    with db_conn() as conn:
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS tenant_org_profile (
                tenant_slug TEXT PRIMARY KEY,
                company_name TEXT NOT NULL DEFAULT '',
                company_address TEXT NOT NULL DEFAULT '',
                ein TEXT NOT NULL DEFAULT '',
                updated_at TIMESTAMP WITH TIME ZONE DEFAULT now()
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS tenant_email_settings (
                tenant_slug TEXT PRIMARY KEY,
                provider TEXT NOT NULL DEFAULT 'custom',
                from_name TEXT NOT NULL DEFAULT '',
                from_email TEXT NOT NULL DEFAULT '',
                smtp_host TEXT NOT NULL DEFAULT '',
                smtp_port INT NOT NULL DEFAULT 587,
                tls_mode TEXT NOT NULL DEFAULT 'starttls',
                smtp_username TEXT NOT NULL DEFAULT '',
                smtp_secret_id TEXT NOT NULL DEFAULT '',
                is_enabled BOOLEAN NOT NULL DEFAULT false,
                last_test_status TEXT NOT NULL DEFAULT 'never',
                last_test_at TIMESTAMP WITH TIME ZONE,
                last_test_error TEXT NOT NULL DEFAULT '',
                updated_at TIMESTAMP WITH TIME ZONE DEFAULT now()
            )
        """)
        conn.commit()
    _settings_tables_ensured = True


Provider = Literal["bluehost", "gmail", "microsoft", "custom"]
TlsMode = Literal["starttls", "ssl"]


# ----------------------------
# Models
# ----------------------------
class OrgProfileOut(BaseModel):
    company_name: str = ""
    company_address: str = ""
    ein: str = ""


class OrgProfileIn(BaseModel):
    company_name: str = ""
    company_address: str = ""
    ein: str = ""


class EmailSettingsOut(BaseModel):
    provider: Provider = "custom"
    from_name: str = ""
    from_email: str = ""
    smtp_host: str = ""
    smtp_port: int = 587
    tls_mode: TlsMode = "starttls"
    smtp_username: str = ""
    smtp_secret_id: str = ""
    is_enabled: bool = False
    last_test_status: Literal["never", "ok", "failed"] = "never"
    last_test_at: Optional[datetime] = None
    last_test_error: str = ""


class EmailSettingsIn(BaseModel):
    provider: Provider = "custom"
    from_name: str = ""
    # Accept empty string, but if not empty it must be a valid email
    from_email: str = ""
    smtp_host: str = ""
    smtp_port: int = 587
    tls_mode: TlsMode = "starttls"
    smtp_username: str = ""
    smtp_password: Optional[str] = Field(default=None, description="write-only; DO NOT store in DB")
    is_enabled: bool = False

    @field_validator("from_email")
    @classmethod
    def validate_from_email(cls, v: str) -> str:
        v = (v or "").strip()
        if v == "":
            return ""
        # validate as EmailStr when provided
        EmailStr._validate(v)  # raises if invalid (works in pydantic v2)
        return v


# ----------------------------
# Row helpers
# ----------------------------
def _row_to_org(row) -> OrgProfileOut:
    if not row:
        return OrgProfileOut()
    if isinstance(row, dict):
        return OrgProfileOut(
            company_name=row.get("company_name") or "",
            company_address=row.get("company_address") or "",
            ein=row.get("ein") or "",
        )
    return OrgProfileOut(company_name=row[0] or "", company_address=row[1] or "", ein=row[2] or "")


def _row_to_email(row) -> EmailSettingsOut:
    if not row:
        return EmailSettingsOut()
    if isinstance(row, dict):
        return EmailSettingsOut(
            provider=row.get("provider") or "custom",
            from_name=row.get("from_name") or "",
            from_email=row.get("from_email") or "",
            smtp_host=row.get("smtp_host") or "",
            smtp_port=int(row.get("smtp_port") or 587),
            tls_mode=row.get("tls_mode") or "starttls",
            smtp_username=row.get("smtp_username") or "",
            smtp_secret_id=row.get("smtp_secret_id") or "",
            is_enabled=bool(row.get("is_enabled") or False),
            last_test_status=row.get("last_test_status") or "never",
            last_test_at=row.get("last_test_at") or None,
            last_test_error=row.get("last_test_error") or "",
        )
    return EmailSettingsOut(
        provider=row[0] or "custom",
        from_name=row[1] or "",
        from_email=row[2] or "",
        smtp_host=row[3] or "",
        smtp_port=int(row[4] or 587),
        tls_mode=row[5] or "starttls",
        smtp_username=row[6] or "",
        smtp_secret_id=row[7] or "",
        is_enabled=bool(row[8]),
        last_test_status=row[9] or "never",
        last_test_at=row[10] if row[10] else None,
        last_test_error=row[11] or "",
    )


def _load_smtp_secret(secret_id: str) -> tuple[str, str]:
    """
    Load SMTP username/password from AWS Secrets Manager.
    Expects SecretString to be JSON with keys like username/user/smtp_username and password/pass/smtp_password.
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

    username = (
        data.get("username")
        or data.get("user")
        or data.get("smtp_username")
        or ""
    )
    password = (
        data.get("password")
        or data.get("pass")
        or data.get("smtp_password")
        or ""
    )

    if not username or not password:
        raise ValueError("SMTP secret must contain username and password.")

    return str(username), str(password)


# ----------------------------
# Org routes
# ----------------------------
@router.get("/organization", response_model=OrgProfileOut)
def get_org_profile(
    tenant_slug: str,
    _=Depends(require_role(TENANT_ADMIN)),
):
    _ensure_settings_tables()
    with db_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT company_name, company_address, ein
            FROM tenant_org_profile
            WHERE tenant_slug = %s
            """,
            (tenant_slug,),
        )
        row = cur.fetchone()
    return _row_to_org(row)


@router.put("/organization", response_model=OrgProfileOut)
def upsert_org_profile(
    tenant_slug: str,
    body: OrgProfileIn,
    _=Depends(require_role(TENANT_ADMIN)),
):
    _ensure_settings_tables()
    with db_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO tenant_org_profile (tenant_slug, company_name, company_address, ein, updated_at)
            VALUES (%s, %s, %s, %s, now())
            ON CONFLICT (tenant_slug)
            DO UPDATE SET
              company_name = EXCLUDED.company_name,
              company_address = EXCLUDED.company_address,
              ein = EXCLUDED.ein,
              updated_at = now()
            RETURNING company_name, company_address, ein
            """,
            (tenant_slug, body.company_name or "", body.company_address or "", body.ein or ""),
        )
        row = cur.fetchone()
        conn.commit()
    return _row_to_org(row)


# ----------------------------
# Email routes
# ----------------------------
@router.get("/email", response_model=EmailSettingsOut)
def get_email_settings(
    tenant_slug: str,
    _=Depends(require_role(TENANT_ADMIN)),
):
    _ensure_settings_tables()
    with db_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT provider, from_name, from_email, smtp_host, smtp_port, tls_mode,
                   smtp_username, smtp_secret_id, is_enabled,
                   last_test_status, last_test_at, last_test_error
            FROM tenant_email_settings
            WHERE tenant_slug = %s
            """,
            (tenant_slug,),
        )
        row = cur.fetchone()
    return _row_to_email(row)


@router.put("/email", response_model=EmailSettingsOut)
def upsert_email_settings(
    tenant_slug: str,
    body: EmailSettingsIn,
    _=Depends(require_role(TENANT_ADMIN)),
):
    """
    NOTES:
    - smtp_password is write-only and not stored in Postgres.
    - Until Secrets Manager is wired, preserve existing smtp_secret_id so UI edits
      don't wipe it.
    """
    _ensure_settings_tables()
    with db_conn() as conn:
        cur = conn.cursor()

        cur.execute(
            "SELECT smtp_secret_id FROM tenant_email_settings WHERE tenant_slug = %s",
            (tenant_slug,),
        )
        existing = cur.fetchone()
        existing_secret_id = ""
        if existing:
            if isinstance(existing, dict):
                existing_secret_id = existing.get("smtp_secret_id") or ""
            else:
                existing_secret_id = existing[0] or ""

        # If later you wire secrets:
        # if body.smtp_password: existing_secret_id = upsert_secret(...)

        cur.execute(
            """
            INSERT INTO tenant_email_settings (
              tenant_slug, provider, from_name, from_email,
              smtp_host, smtp_port, tls_mode, smtp_username, smtp_secret_id,
              is_enabled, updated_at
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s, now())
            ON CONFLICT (tenant_slug)
            DO UPDATE SET
              provider = EXCLUDED.provider,
              from_name = EXCLUDED.from_name,
              from_email = EXCLUDED.from_email,
              smtp_host = EXCLUDED.smtp_host,
              smtp_port = EXCLUDED.smtp_port,
              tls_mode = EXCLUDED.tls_mode,
              smtp_username = EXCLUDED.smtp_username,
              smtp_secret_id = EXCLUDED.smtp_secret_id,
              is_enabled = EXCLUDED.is_enabled,
              updated_at = now()
            RETURNING provider, from_name, from_email, smtp_host, smtp_port, tls_mode,
                      smtp_username, smtp_secret_id, is_enabled,
                      last_test_status, last_test_at, last_test_error
            """,
            (
                tenant_slug,
                body.provider,
                body.from_name or "",
                body.from_email or "",
                body.smtp_host or "",
                int(body.smtp_port or 587),
                body.tls_mode,
                body.smtp_username or "",
                existing_secret_id,
                bool(body.is_enabled),
            ),
        )
        row = cur.fetchone()
        conn.commit()

    return _row_to_email(row)


@router.post("/email/test", response_model=EmailSettingsOut)
def send_test_email(
    tenant_slug: str,
    ctx=Depends(require_role(TENANT_ADMIN)),  # <-- get ctx so we can read current user
):
    """
    Send a test email using the tenant's SMTP settings and AWS Secrets Manager secret.

    - Sends TO the current user's email (fallback: from_email).
    - Updates last_test_status/last_test_at/last_test_error in Postgres.
    """
    _ensure_settings_tables()

    # Load current settings (including secret id)
    with db_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT provider, from_name, from_email, smtp_host, smtp_port, tls_mode,
                   smtp_username, smtp_secret_id, is_enabled,
                   last_test_status, last_test_at, last_test_error
            FROM tenant_email_settings
            WHERE tenant_slug = %s
            """,
            (tenant_slug,),
        )
        row = cur.fetchone()

    settings = _row_to_email(row)

    # Determine recipient
    user = (ctx or {}).get("user") or {}
    recipient = (user.get("email") or user.get("username") or "").strip()
    if not recipient:
        recipient = (settings.from_email or "").strip()

    problems: list[str] = []
    if not recipient:
        problems.append("Cannot determine recipient email for the current user (no email/username in auth payload).")
    if not settings.from_email:
        problems.append("From email is not configured.")
    if not settings.smtp_host:
        problems.append("SMTP host is not configured.")
    if not settings.smtp_port:
        problems.append("SMTP port is not configured.")
    if not settings.smtp_secret_id:
        problems.append("SMTP secret id is not configured.")
    if not settings.is_enabled:
        problems.append("Email sending is disabled for this tenant.")

    error_msg: str | None = None
    new_status = "never"

    if problems:
        new_status = "failed"
        error_msg = " ".join(problems)
    else:
        try:
            username, password = _load_smtp_secret(settings.smtp_secret_id)

            msg = EmailMessage()
            if settings.from_name:
                msg["From"] = f"{settings.from_name} <{settings.from_email}>"
            else:
                msg["From"] = settings.from_email

            msg["To"] = recipient  # <-- key change
            msg["Subject"] = f"[InkSuite] SMTP test for {tenant_slug}"
            msg.set_content(
                "This is a test email from InkSuite using your custom SMTP settings.\n\n"
                "If you received this message, your SMTP configuration is working."
            )

            if settings.tls_mode == "ssl":
                with smtplib.SMTP_SSL(settings.smtp_host, settings.smtp_port) as smtp:
                    smtp.login(username, password)
                    smtp.send_message(msg)
            else:
                with smtplib.SMTP(settings.smtp_host, settings.smtp_port) as smtp:
                    smtp.ehlo()
                    smtp.starttls()
                    smtp.ehlo()
                    smtp.login(username, password)
                    smtp.send_message(msg)

            new_status = "ok"
            error_msg = ""
        except Exception as e:
            new_status = "failed"
            error_msg = str(e)[:2000]

    with db_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            UPDATE tenant_email_settings
            SET last_test_status = %s,
                last_test_at = now(),
                last_test_error = %s,
                updated_at = now()
            WHERE tenant_slug = %s
            RETURNING provider, from_name, from_email, smtp_host, smtp_port, tls_mode,
                      smtp_username, smtp_secret_id, is_enabled,
                      last_test_status, last_test_at, last_test_error
            """,
            (new_status, error_msg or "", tenant_slug),
        )
        updated = cur.fetchone()
        conn.commit()

    return _row_to_email(updated)