# app/auth/router.py  (COMPLETE DROP-IN)
from __future__ import annotations

import logging
import os

from fastapi import APIRouter, Depends, HTTPException, Response, status
from pydantic import BaseModel, EmailStr

from app.auth.dependencies import get_current_user
from app.auth.service import (
    validate_login,
    get_me_payload_from_claims,
    get_invite_by_token,
    accept_invite,
    register_and_accept_invite,
)

log = logging.getLogger("auth")

# IMPORTANT:
# This router is designed to be mounted with prefix="/api" in main.py
# so the final URLs become:
#   POST /api/auth/login
#   POST /api/auth/logout
#   GET  /api/auth/me
#   GET  /api/auth/invites/{token}
#   POST /api/auth/invites/accept
#   POST /api/auth/invites/register
router = APIRouter(prefix="/auth", tags=["auth"])


# -----------------------------
# Schemas
# -----------------------------
class LoginBody(BaseModel):
    email: EmailStr
    password: str
    companyName: str | None = None  # tenant slug; required for sign-in (superadmin: "inksuite")
    tenant_slug: str | None = None  # alias for companyName (back compat)


class LoginResponse(BaseModel):
    access_token: str
    refresh_token: str | None = None
    token_type: str = "Bearer"
    user: dict


class AcceptInviteBody(BaseModel):
    token: str


class RegisterAcceptInviteBody(BaseModel):
    token: str
    email: EmailStr
    password: str
    full_name: str
    phone: str = ""
    username: str | None = None


# -----------------------------
# Helpers
# -----------------------------
def _is_db_down_error(detail: str) -> bool:
    d = (detail or "").lower()
    return "database" in d or "db" in d or "connection" in d or "timeout" in d


def _cookie_secure_default() -> bool:
    """
    If you're on https (prod), secure cookies should be True.
    For localhost http, secure must be False or the browser won't set them.
    """
    # If you explicitly set COOKIE_SECURE, honor it.
    v = (os.environ.get("COOKIE_SECURE") or "").strip().lower()
    if v in ("1", "true", "yes"):
        return True
    if v in ("0", "false", "no"):
        return False

    origin = (os.environ.get("PUBLIC_APP_ORIGIN") or "").strip().lower()
    if origin.startswith("https://"):
        return True
    return False


def _cookie_samesite_default() -> str:
    """
    If frontend and backend are on different sites, use 'none' + secure.
    If same-site, 'lax' is fine. Default to lax for local dev.
    """
    v = (os.environ.get("COOKIE_SAMESITE") or "").strip().lower()
    if v in ("lax", "strict", "none"):
        return v
    origin = (os.environ.get("PUBLIC_APP_ORIGIN") or "").strip().lower()
    if origin.startswith("https://"):
        # Many setups still work with Lax; but if you ever use cross-site cookies, switch to "none".
        return "lax"
    return "lax"


def _set_auth_cookies(response: Response, access_token: str, refresh_token: str | None) -> None:
    """
    Optional: if you later want backend-set cookies for local/dev testing.
    Your current architecture is Next BFF sets cookies; you can leave this unused.
    """
    secure = _cookie_secure_default()
    samesite = _cookie_samesite_default()

    response.set_cookie(
        key="access_token",
        value=access_token,
        httponly=True,
        secure=secure,
        samesite=samesite,
        path="/",
        max_age=60 * 60,  # 1 hour
    )
    if refresh_token:
        response.set_cookie(
            key="refresh_token",
            value=refresh_token,
            httponly=True,
            secure=secure,
            samesite=samesite,
            path="/",
            max_age=60 * 60 * 24 * 30,  # 30 days
        )


# -----------------------------
# Auth endpoints
# -----------------------------
@router.post("/login", response_model=LoginResponse)
async def login(body: LoginBody):
    company = (body.companyName or body.tenant_slug or "").strip()
    if not company:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Company name is required. Use your company slug (e.g. marble-press) or 'inksuite' for platform admin.",
        )

    try:
        result = validate_login(str(body.email), body.password, tenant_slug=company)
    except ValueError as e:
        msg = str(e)
        if msg == "company_required":
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Company name is required.")
        # e.g. NEW_PASSWORD_REQUIRED, MFA setup, etc.
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=msg)
    except RuntimeError as e:
        # If your validate_login raises 503 conditions
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=str(e))
    except Exception as e:
        # If DB is down, your service may throw a psycopg error; surface as 503 with a stable message
        msg = str(e)
        log.exception("Login failed unexpectedly: %s", msg)
        if _is_db_down_error(msg):
            raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="Service temporarily unavailable. Check database connection.")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Unexpected server error")

    if not result:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid login credentials. Please check your email, password, and company name.",
        )

    return LoginResponse(
        access_token=result["access_token"],
        refresh_token=result.get("refresh_token"),
        token_type=result.get("token_type", "Bearer"),
        user=result.get("user", {}),
    )


@router.post("/logout")
async def logout(response: Response):
    """
    Backend logout notes:
    - If your frontend stores tokens in localStorage/sessionStorage, clear them client-side.
    - If your Next BFF stores cookies, clear them in the BFF route.
    - If you ever set cookies from the backend directly, you can delete them here.
    """
    # Safe to delete (no harm if not present)
    response.delete_cookie(key="access_token", path="/")
    response.delete_cookie(key="refresh_token", path="/")
    return {"ok": True}


@router.get("/me")
async def me(user: dict = Depends(get_current_user)):
    return get_me_payload_from_claims(user)


# -----------------------------
# Public invite lookup (NO AUTH)
# GET /api/auth/invites/{token}
# Used by Next route: /api/invites/by-token -> backend
# -----------------------------
@router.get("/invites/{token}")
def invite_by_token(token: str):
    inv = get_invite_by_token(token)
    if not inv:
        raise HTTPException(status_code=404, detail="Invalid or expired invite")
    return {"ok": True, "invite": inv}


# -----------------------------
# Accept invite (AUTH REQUIRED)
# POST /api/auth/invites/accept
# Body: { token }
# -----------------------------
@router.post("/invites/accept")
def accept_invite_route(body: AcceptInviteBody, user: dict = Depends(get_current_user)):
    try:
        res = accept_invite(claims=user, token=body.token, require_email_match=True)
        return {"ok": True, **res}
    except ValueError as e:
        msg = str(e)
        # Map known invite errors to clean HTTP codes
        if msg in ("unauthorized",):
            raise HTTPException(status_code=401, detail="Unauthorized")
        if msg in ("invalid_token",):
            raise HTTPException(status_code=404, detail="Invalid invite")
        if msg in ("expired",):
            raise HTTPException(status_code=410, detail="Invite expired")
        if msg in ("already_accepted",):
            raise HTTPException(status_code=409, detail="Invite already accepted")
        if msg in ("email_mismatch",):
            raise HTTPException(status_code=403, detail="Invite email does not match your login email")
        raise HTTPException(status_code=400, detail=msg)
    except Exception as e:
        msg = str(e)
        if _is_db_down_error(msg):
            raise HTTPException(status_code=503, detail="Service temporarily unavailable. Check database connection.")
        raise HTTPException(status_code=500, detail="Unexpected server error")


# -----------------------------
# Register + accept invite (PUBLIC)
# POST /api/auth/invites/register
# -----------------------------
@router.post("/invites/register")
def register_and_accept_invite_route(body: RegisterAcceptInviteBody):
    try:
        res = register_and_accept_invite(
            token=body.token,
            email=str(body.email),
            password=body.password,
            full_name=body.full_name,
            phone=body.phone or "",
            username=body.username,
        )
        return res
    except ValueError as e:
        msg = str(e)
        # Clean mapping
        if msg in ("invalid_request",):
            raise HTTPException(status_code=400, detail="Invalid request")
        if msg in ("invalid_token",):
            raise HTTPException(status_code=404, detail="Invalid invite")
        if msg in ("expired",):
            raise HTTPException(status_code=410, detail="Invite expired")
        if msg in ("already_accepted",):
            raise HTTPException(status_code=409, detail="Invite already accepted")
        if msg in ("email_mismatch",):
            raise HTTPException(status_code=403, detail="Invite email mismatch")
        if msg in ("username_exists",):
            raise HTTPException(status_code=409, detail="User already exists")
        if msg in ("invalid_password",):
            raise HTTPException(status_code=400, detail="Password does not meet requirements")
        raise HTTPException(status_code=400, detail=msg)
    except RuntimeError as e:
        # Cognito not configured or DB not configured
        raise HTTPException(status_code=503, detail=str(e))
    except Exception as e:
        msg = str(e)
        if _is_db_down_error(msg):
            raise HTTPException(status_code=503, detail="Service temporarily unavailable. Check database connection.")
        raise HTTPException(status_code=500, detail="Unexpected server error")