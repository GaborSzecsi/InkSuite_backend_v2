# app/settings/router.py
from __future__ import annotations

from datetime import datetime
from typing import Literal, Optional

from fastapi import APIRouter, Depends
from pydantic import BaseModel

from app.tenants.dependencies import require_role
from app.auth.service import TENANT_ADMIN
from app.core.db import db_conn

router = APIRouter(prefix="/tenants/{tenant_slug}/settings", tags=["Settings"])

Provider = Literal["bluehost", "gmail", "microsoft", "custom"]
TlsMode = Literal["starttls", "ssl"]


# ----------------------------
# Schemas
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
    smtp_secret_id: str = ""  # stored in DB (Secrets Manager secret name/ARN)
    is_enabled: bool = False
    last_test_status: Literal["never", "ok", "failed"] = "never"
    last_test_at: Optional[datetime] = None
    last_test_error: str = ""


class EmailSettingsIn(BaseModel):
    provider: Provider = "custom"
    from_name: str = ""
    from_email: str = ""
    smtp_host: str = ""
    smtp_port: int = 587
    tls_mode: TlsMode = "starttls"
    smtp_username: str = ""
    smtp_password: Optional[str] = None  # write-only; DO NOT store in DB
    is_enabled: bool = False


# ----------------------------
# DB helpers
# ----------------------------
def _row_to_org(row) -> OrgProfileOut:
    if not row:
        return OrgProfileOut()

    # tuple ordering from SELECT company_name, company_address, ein
    return OrgProfileOut(
        company_name=(row[0] or ""),
        company_address=(row[1] or ""),
        ein=(row[2] or ""),
    )


def _row_to_email(row) -> EmailSettingsOut:
    if not row:
        return EmailSettingsOut()

    # tuple ordering from SELECT provider, from_name, from_email, smtp_host, smtp_port, tls_mode,
    #                      smtp_username, smtp_secret_id, is_enabled, last_test_status, last_test_at, last_test_error
    return EmailSettingsOut(
        provider=(row[0] or "custom"),
        from_name=(row[1] or ""),
        from_email=(row[2] or ""),
        smtp_host=(row[3] or ""),
        smtp_port=int(row[4] or 587),
        tls_mode=(row[5] or "starttls"),
        smtp_username=(row[6] or ""),
        smtp_secret_id=(row[7] or ""),
        is_enabled=bool(row[8]),
        last_test_status=(row[9] or "never"),
        last_test_at=(row[10] or None),
        last_test_error=(row[11] or ""),
    )


# ----------------------------
# Routes
# ----------------------------
@router.get("/organization", response_model=OrgProfileOut)
def get_org_profile(
    tenant_slug: str,
    _=Depends(require_role(TENANT_ADMIN)),
):
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
            (
                tenant_slug,
                body.company_name or "",
                body.company_address or "",
                body.ein or "",
            ),
        )
        row = cur.fetchone()
        conn.commit()

    return _row_to_org(row)


@router.get("/email", response_model=EmailSettingsOut)
def get_email_settings(
    tenant_slug: str,
    _=Depends(require_role(TENANT_ADMIN)),
):
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
    NOTE:
    - smtp_password is intentionally NOT stored in Postgres.
    - For now, we preserve smtp_secret_id if it already exists.
      Later, when you wire Secrets Manager, you will:
        - create/update secret when smtp_password is provided
        - store secret id/arn in smtp_secret_id
    """

    with db_conn() as conn:
        cur = conn.cursor()

        # Preserve existing smtp_secret_id so saving settings doesn't wipe it out.
        cur.execute(
            "SELECT smtp_secret_id FROM tenant_email_settings WHERE tenant_slug=%s",
            (tenant_slug,),
        )
        existing = cur.fetchone()
        existing_secret_id = (existing[0] if existing else "") or ""

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
                existing_secret_id,  # preserve until Secrets Manager wiring
                bool(body.is_enabled),
            ),
        )
        row = cur.fetchone()
        conn.commit()

    return _row_to_email(row)