# app/settings/router.py
from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, EmailStr, Field
from typing import Literal, Optional

from app.tenants.dependencies import require_role
from app.auth.service import TENANT_ADMIN
from app.core.db import db_conn

router = APIRouter(prefix="/tenants", tags=["Settings"])

Provider = Literal["bluehost", "gmail", "microsoft", "custom"]
TlsMode = Literal["starttls", "ssl"]


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
    last_test_at: Optional[str] = None
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