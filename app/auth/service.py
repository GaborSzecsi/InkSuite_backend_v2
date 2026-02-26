# app/auth/service.py 
from __future__ import annotations

import base64
import hashlib
import hmac
import os
import secrets
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional

import boto3
import requests
from botocore.exceptions import ClientError
from jose import JWTError, jwt

from app.core.config import settings

# DB helper (psycopg v3)
try:
    from app.core.db import db_conn
except Exception:
    db_conn = None  # allows local/dev runs without DB


# -----------------------------
# Cognito client (password auth)
# -----------------------------
def _cognito() -> Any:
    return boto3.client("cognito-idp", region_name=settings.cognito_region)


def _secret_hash(username: str) -> str | None:
    """
    Only needed if your Cognito App Client has a client secret.
    If no secret is set, return None and do not include SECRET_HASH.
    """
    secret = getattr(settings, "cognito_client_secret", "") or ""
    client_id = settings.cognito_client_id
    if not secret:
        return None
    msg = (username + client_id).encode("utf-8")
    key = secret.encode("utf-8")
    dig = hmac.new(key, msg, hashlib.sha256).digest()
    return base64.b64encode(dig).decode("utf-8")


# Company name for platform superadmin (rescue/operations). Only this value allowed for superadmin.
SUPERADMIN_COMPANY = "inksuite"

# Generic message for any login failure (tenant isolation: do not leak why it failed).
INVALID_CREDENTIALS_MSG = "Invalid login credentials. Please check your email, password, and company name."


def _lower(s: str | None) -> str:
    return (s or "").strip().lower()


def _normalize_company(company: str) -> str:
    """Normalize company name to slug form: lower, spaces -> hyphens (e.g. 'Marble Press' -> 'marble-press')."""
    return (company or "").strip().lower().replace(" ", "-")


def _db_available() -> bool:
    """
    Lightweight check: configured + env present.
    (We do NOT attempt a live connection here to keep auth fast and avoid masking real DB errors.)
    """
    if db_conn is None:
        return False
    return bool((os.environ.get("DATABASE_URL") or "").strip())


# -----------------------------
# Tenant roles + invitations
# -----------------------------
TENANT_ADMIN = "tenant_admin"
TENANT_EDITOR = "tenant_editor"  # legacy alias
EDITOR = "editor"  # canonical editor role
TENANT_ALLOWED = {TENANT_ADMIN, TENANT_EDITOR, EDITOR}


def is_superadmin(platform_role: str | None) -> bool:
    return _lower(platform_role) == "superadmin"


def get_user_db_record_from_claims(claims: dict) -> Optional[dict]:
    """
    Ensures a users row exists, then returns {id, email, platform_role}.
    """
    if not _db_available():
        return None

    sub = (claims or {}).get("sub")
    if not sub:
        return None

    token_email = _lower((claims or {}).get("email"))

    with db_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO users (cognito_sub, email)
            VALUES (%s, COALESCE(NULLIF(%s,''), 'unknown'))
            ON CONFLICT (cognito_sub)
            DO UPDATE SET
                email = CASE
                    WHEN EXCLUDED.email <> 'unknown' THEN EXCLUDED.email
                    ELSE users.email
                END
            """,
            (sub, token_email),
        )
        cur.execute("SELECT id, email, platform_role FROM users WHERE cognito_sub = %s", (sub,))
        row = cur.fetchone()
        if not row:
            return None
        return {"id": row[0], "email": row[1], "platform_role": row[2]}


def get_memberships_for_user(user_id) -> list[dict]:
    """
    Returns list[{tenant_id, tenant_slug, role, module_permissions}]
    """
    if not _db_available():
        return []
    with db_conn() as conn, conn.cursor() as cur:
        try:
            cur.execute(
                """
                SELECT t.id, t.slug, m.role, m.module_permissions
                FROM memberships m
                JOIN tenants t ON t.id = m.tenant_id
                WHERE m.user_id = %s
                ORDER BY t.slug
                """,
                (user_id,),
            )
            rows = cur.fetchall()
        except Exception:
            cur.execute(
                """
                SELECT t.id, t.slug, m.role
                FROM memberships m
                JOIN tenants t ON t.id = m.tenant_id
                WHERE m.user_id = %s
                ORDER BY t.slug
                """,
                (user_id,),
            )
            rows = [(r[0], r[1], r[2], None) for r in cur.fetchall()]

        return [
            {
                "tenant_id": r[0],
                "tenant_slug": r[1],
                "role": r[2],
                "module_permissions": r[3] if len(r) > 3 and r[3] is not None else {},
            }
            for r in rows
        ]


def validate_login(email: str, password: str, tenant_slug: str | None = None) -> Optional[dict]:
    """
    Returns dict with access_token (+ refresh_token if returned) on success, else None.
    Raises ValueError for challenge flows (e.g. NEW_PASSWORD_REQUIRED) or "company_required".

    STRICT tenant isolation:
    - Superadmin: ONLY allowed when company normalizes to "inksuite" AND user.platform_role == 'superadmin'.
    - Other users: ONLY allowed when company normalizes to a tenant slug they have membership in.
    """
    if not (settings.cognito_user_pool_id and settings.cognito_client_id):
        raise RuntimeError("Cognito not configured: set COGNITO_USER_POOL_ID and COGNITO_CLIENT_ID")

    auth_params: Dict[str, str] = {"USERNAME": email.strip(), "PASSWORD": password}
    sh = _secret_hash(email)
    if sh:
        auth_params["SECRET_HASH"] = sh

    try:
        resp = _cognito().initiate_auth(
            AuthFlow="USER_PASSWORD_AUTH",
            ClientId=settings.cognito_client_id,
            AuthParameters=auth_params,
        )
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code", "")
        if code in ("NotAuthorizedException", "UserNotFoundException"):
            return None
        if code in ("PasswordResetRequiredException", "UserNotConfirmedException"):
            raise ValueError(code)
        raise

    if resp.get("ChallengeName"):
        raise ValueError(resp["ChallengeName"])

    ar = resp.get("AuthenticationResult") or {}
    access_token = ar.get("AccessToken")
    if not access_token:
        return None

    refresh_token = ar.get("RefreshToken")
    token_type = ar.get("TokenType", "Bearer")

    user_claims = get_current_user_from_token(access_token) or {}

    company_raw = (tenant_slug or "").strip()
    if not company_raw:
        # Your UI/BFF should provide this; we keep it strict to prevent cross-tenant access.
        raise ValueError("company_required")

    company = _normalize_company(company_raw)

    # If DB isn't configured, we can ONLY allow superadmin with company=inksuite (optional behavior).
    # Everyone else must wait until DB is configured because we can't verify tenant membership.
    if not _db_available():
        # If you prefer "allow login but no tenant access", you can return tokens here instead.
        if company == _lower(SUPERADMIN_COMPANY):
            # We cannot confirm platform_role without DB; safest is to deny.
            return None
        return None

    # DB is available: load/create user record and enforce tenant access
    user = get_user_db_record_from_claims(user_claims)
    if not user:
        return None

    # Strict: superadmin ONLY with company "inksuite"
    if company == _lower(SUPERADMIN_COMPANY):
        if not is_superadmin(user.get("platform_role")):
            return None
        return {
            "access_token": access_token,
            "refresh_token": refresh_token,
            "token_type": token_type,
            "user": user_claims,
        }

    # Strict: non-superadmin ONLY with a tenant they belong to
    from app.tenants.resolver import resolve_tenant

    tenant = resolve_tenant(company)
    if not tenant:
        return None

    memberships = get_memberships_for_user(user["id"])
    if not any(_normalize_company(m.get("tenant_slug") or "") == company for m in memberships):
        return None

    return {
        "access_token": access_token,
        "refresh_token": refresh_token,
        "token_type": token_type,
        "user": user_claims,
    }


# -----------------------------
# JWT verification (API auth)
# -----------------------------
_JWKS_CACHE: dict | None = None
_JWKS_CACHE_AT: float = 0.0
_JWKS_TTL_SECONDS = 3600


def _issuer() -> str:
    iss = getattr(settings, "cognito_issuer", "") or ""
    if iss:
        return iss.rstrip("/")
    return f"https://cognito-idp.{settings.cognito_region}.amazonaws.com/{settings.cognito_user_pool_id}"


def _jwks_url() -> str:
    url = getattr(settings, "jwks_url", "") or ""
    if url:
        return url
    return f"{_issuer()}/.well-known/jwks.json"


def _fetch_jwks() -> dict:
    r = requests.get(_jwks_url(), timeout=5)
    r.raise_for_status()
    return r.json()


def _get_jwks(force_refresh: bool = False) -> dict:
    global _JWKS_CACHE, _JWKS_CACHE_AT
    now = time.time()
    if not force_refresh and _JWKS_CACHE and (now - _JWKS_CACHE_AT) < _JWKS_TTL_SECONDS:
        return _JWKS_CACHE

    _JWKS_CACHE = _fetch_jwks()
    _JWKS_CACHE_AT = now
    return _JWKS_CACHE


def _get_signing_key_for_kid(kid: str) -> dict | None:
    jwks = _get_jwks(force_refresh=False)
    key = next((k for k in jwks.get("keys", []) if k.get("kid") == kid), None)
    if key:
        return key

    jwks = _get_jwks(force_refresh=True)
    return next((k for k in jwks.get("keys", []) if k.get("kid") == kid), None)


def get_current_user_from_token(token: str | None) -> Optional[dict]:
    """
    Strictly verifies a Cognito ACCESS token.
    """
    if not token:
        return None

    try:
        header = jwt.get_unverified_header(token)
        kid = header.get("kid")
        if not kid:
            return None

        key = _get_signing_key_for_kid(kid)
        if not key:
            return None

        claims = jwt.decode(
            token,
            key,
            algorithms=["RS256"],
            issuer=_issuer(),
            options={
                "verify_signature": True,
                "verify_iss": True,
                "verify_exp": True,
                "verify_aud": False,
            },
        )

        if claims.get("token_use") != "access":
            return None

        expected_client_id = settings.cognito_client_id
        token_client_id = claims.get("client_id")
        token_aud = claims.get("aud")

        if token_client_id != expected_client_id and token_aud != expected_client_id:
            return None

        return claims

    except (JWTError, Exception):
        return None


# -----------------------------
# DB-backed "me" enrichment
# -----------------------------
def get_me_payload_from_claims(claims: dict) -> dict:
    """
    Stable response for /api/auth/me.
    """
    sub = (claims or {}).get("sub")
    token_email = _lower((claims or {}).get("email"))

    out = {
        "loggedIn": True,
        "user": dict(claims or {}),
        "tenants": [],
    }

    if not sub or not _db_available():
        out["user"].setdefault("platform_role", "user")
        return out

    try:
        with db_conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO users (cognito_sub, email)
                VALUES (%s, COALESCE(NULLIF(%s,''), 'unknown'))
                ON CONFLICT (cognito_sub)
                DO UPDATE SET
                    email = CASE
                        WHEN EXCLUDED.email <> 'unknown' THEN EXCLUDED.email
                        ELSE users.email
                    END
                """,
                (sub, token_email),
            )

            cur.execute(
                "SELECT id, email, platform_role FROM users WHERE cognito_sub = %s",
                (sub,),
            )
            row = cur.fetchone()
            if not row:
                out["user"].setdefault("platform_role", "user")
                return out

            user_id, db_email, platform_role = row

            try:
                cur.execute(
                    """
                    SELECT t.slug, m.role, m.module_permissions
                    FROM memberships m
                    JOIN tenants t ON t.id = m.tenant_id
                    WHERE m.user_id = %s
                    ORDER BY t.slug
                    """,
                    (user_id,),
                )
                tenants = [
                    {"tenant_slug": r[0], "role": r[1], "module_permissions": r[2] or {}}
                    for r in cur.fetchall()
                ]
            except Exception:
                cur.execute(
                    """
                    SELECT t.slug, m.role
                    FROM memberships m
                    JOIN tenants t ON t.id = m.tenant_id
                    WHERE m.user_id = %s
                    ORDER BY t.slug
                    """,
                    (user_id,),
                )
                tenants = [{"tenant_slug": r[0], "role": r[1], "module_permissions": {}} for r in cur.fetchall()]

        out["user"]["email"] = db_email or out["user"].get("email") or "unknown"
        out["user"]["platform_role"] = platform_role or out["user"].get("platform_role") or "user"
        out["tenants"] = tenants
        return out

    except Exception as e:
        try:
            import logging

            logging.getLogger("auth").exception("DB-backed /auth/me failed: %s", e)
        except Exception:
            pass

        out["user"].setdefault("platform_role", "user")
        return out


# -----------------------------
# Tenant auth enforcement
# -----------------------------
def require_tenant_role(
    *,
    claims: dict,
    tenant_slug: str,
    allowed_roles: set[str],
) -> dict:
    """
    Enforce tenant-scoped authorization.
    """
    if not claims or not claims.get("sub"):
        raise ValueError("unauthorized")

    user = get_user_db_record_from_claims(claims)
    if not user:
        raise ValueError("unauthorized")

    if is_superadmin(user.get("platform_role")):
        return {
            "user": user,
            "memberships": [],
            "tenant": {"id": None, "slug": tenant_slug, "role": "superadmin"},
        }

    memberships = get_memberships_for_user(user["id"])
    slug = _lower(tenant_slug)
    allowed = {_lower(r) for r in allowed_roles}

    for m in memberships:
        if _lower(m["tenant_slug"]) == slug and _lower(m["role"]) in allowed:
            return {
                "user": user,
                "memberships": memberships,
                "tenant": {"id": m["tenant_id"], "slug": m["tenant_slug"], "role": m["role"]},
            }

    raise ValueError("forbidden")


# -----------------------------
# Invitations
# -----------------------------
def create_invite(
    *,
    invited_by_user_id,
    tenant_slug: str,
    email: str,
    role: str,
    ttl_hours: int = 72,
    module_permissions: dict | None = None,
) -> dict:
    """
    Creates an invite row and returns:
      {invite_id, token, expires_at, tenant_slug, email, role, module_permissions}

    IMPORTANT:
    - This function DOES NOT send email.
      Email sending belongs in the tenants router layer, so it can be best-effort,
      non-duplicated, and consistently use invite_link.
    """
    if not _db_available():
        raise RuntimeError("DB not configured")

    email_lc = _lower(email)
    role_lc = _lower(role)
    if role_lc not in TENANT_ALLOWED:
        raise ValueError("invalid_role")

    token = secrets.token_urlsafe(32)
    expires_at = datetime.now(timezone.utc) + timedelta(hours=int(ttl_hours or 72))
    mod_perm = module_permissions if isinstance(module_permissions, dict) else {}

    import json

    mod_perm_json = json.dumps(mod_perm)

    with db_conn() as conn, conn.cursor() as cur:
        cur.execute("SELECT id FROM tenants WHERE lower(slug)=lower(%s)", (tenant_slug,))
        tr = cur.fetchone()
        if not tr:
            raise ValueError("tenant_not_found")
        tenant_id = tr[0]

        try:
            cur.execute(
                """
                INSERT INTO invites (tenant_id, email, role, token, invited_by, expires_at, module_permissions)
                VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb)
                RETURNING id
                """,
                (
                    tenant_id,
                    email_lc,
                    role_lc,
                    token,
                    invited_by_user_id,
                    expires_at.replace(tzinfo=None),
                    mod_perm_json,
                ),
            )
        except Exception:
            cur.execute(
                """
                INSERT INTO invites (tenant_id, email, role, token, invited_by, expires_at)
                VALUES (%s, %s, %s, %s, %s, %s)
                RETURNING id
                """,
                (tenant_id, email_lc, role_lc, token, invited_by_user_id, expires_at.replace(tzinfo=None)),
            )

        invite_id = cur.fetchone()[0]
        conn.commit()

    return {
        "invite_id": str(invite_id),
        "token": token,
        "expires_at": expires_at.isoformat(),
        "tenant_slug": tenant_slug,
        "email": email_lc,
        "role": role_lc,
        "module_permissions": mod_perm,
    }


def accept_invite(*, claims: dict, token: str, require_email_match: bool = True) -> dict:
    """
    Accepts an invite for the authenticated user.
    """
    if not _db_available():
        raise RuntimeError("DB not configured")

    user = get_user_db_record_from_claims(claims)
    if not user:
        raise ValueError("unauthorized")

    token = (token or "").strip()
    if not token:
        raise ValueError("invalid_token")

    token_email = _lower((claims or {}).get("email"))
    now = datetime.utcnow()

    with db_conn() as conn, conn.cursor() as cur:
        try:
            cur.execute(
                """
                SELECT i.id, i.tenant_id, i.email, i.role, i.expires_at, i.accepted_at, t.slug, i.module_permissions
                FROM invites i
                JOIN tenants t ON t.id = i.tenant_id
                WHERE i.token = %s
                """,
                (token,),
            )
        except Exception:
            cur.execute(
                """
                SELECT i.id, i.tenant_id, i.email, i.role, i.expires_at, i.accepted_at, t.slug
                FROM invites i
                JOIN tenants t ON t.id = i.tenant_id
                WHERE i.token = %s
                """,
                (token,),
            )
        row = cur.fetchone()
        if not row:
            raise ValueError("invalid_token")

        invite_mod_perm = row[7] if len(row) > 7 else {}
        if not isinstance(invite_mod_perm, dict):
            invite_mod_perm = {}

        invite_id, tenant_id, invite_email, role, expires_at, accepted_at, tenant_slug = row[:7]

        if accepted_at is not None:
            raise ValueError("already_accepted")
        if expires_at is not None and now > expires_at:
            raise ValueError("expired")

        if require_email_match:
            if not token_email or _lower(invite_email) != token_email:
                raise ValueError("email_mismatch")

        import json

        mod_perm_json = json.dumps(invite_mod_perm)
        try:
            cur.execute(
                """
                INSERT INTO memberships (tenant_id, user_id, role, module_permissions)
                VALUES (%s, %s, %s, %s::jsonb)
                ON CONFLICT (tenant_id, user_id)
                DO UPDATE SET role = EXCLUDED.role, module_permissions = EXCLUDED.module_permissions
                """,
                (tenant_id, user["id"], role, mod_perm_json),
            )
        except Exception:
            cur.execute(
                """
                INSERT INTO memberships (tenant_id, user_id, role)
                VALUES (%s, %s, %s)
                ON CONFLICT (tenant_id, user_id)
                DO UPDATE SET role = EXCLUDED.role
                """,
                (tenant_id, user["id"], role),
            )

        cur.execute("UPDATE invites SET accepted_at = now() WHERE id = %s", (invite_id,))
        conn.commit()

    return {"tenant_slug": tenant_slug, "role": role}


def get_invite_by_token(token: str) -> Optional[dict]:
    """
    Public: get invite details by token (for accept-invite page).
    Returns { tenant_name, email, role, expires_at } or None if invalid/expired/accepted.
    """
    if not _db_available():
        return None
    token = (token or "").strip()
    if not token:
        return None
    now = datetime.now(timezone.utc)
    with db_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT i.email, i.role, i.expires_at, i.accepted_at, t.name AS tenant_name
            FROM invites i
            JOIN tenants t ON t.id = i.tenant_id
            WHERE i.token = %s
            """,
            (token,),
        )
        row = cur.fetchone()
    if not row:
        return None
    email, role, expires_at, accepted_at, tenant_name = row
    if accepted_at is not None:
        return None
    if expires_at is not None:
        exp = expires_at.replace(tzinfo=timezone.utc) if expires_at.tzinfo is None else expires_at
        if now > exp:
            return None
    return {
        "tenant_name": tenant_name or "",
        "email": email or "",
        "role": role or "",
        "expires_at": expires_at.isoformat() if expires_at else None,
    }


def register_and_accept_invite(
    *,
    token: str,
    email: str,
    password: str,
    full_name: str,
    phone: str = "",
    username: str | None = None,
) -> dict:
    """
    Public: validate invite token, create Cognito user using ADMIN APIs (works when self sign-up is disabled),
    create users row and membership, mark invite accepted.
    Returns { ok, access_token?, refresh_token?, token_type?, user?, tenant_slug, role }.
    """
    if not (settings.cognito_user_pool_id and settings.cognito_client_id):
        raise RuntimeError("Cognito not configured")
    if not _db_available():
        raise RuntimeError("DB not configured")

    token = (token or "").strip()
    email_lc = _lower(email)
    if not token or not email_lc or not password:
        raise ValueError("invalid_request")

    # ---- Load + validate invite ----
    with db_conn() as conn, conn.cursor() as cur:
        try:
            cur.execute(
                """
                SELECT i.id, i.tenant_id, i.email, i.role, i.expires_at, i.accepted_at, t.slug, i.module_permissions
                FROM invites i
                JOIN tenants t ON t.id = i.tenant_id
                WHERE i.token = %s
                """,
                (token,),
            )
        except Exception:
            cur.execute(
                """
                SELECT i.id, i.tenant_id, i.email, i.role, i.expires_at, i.accepted_at, t.slug
                FROM invites i
                JOIN tenants t ON t.id = i.tenant_id
                WHERE i.token = %s
                """,
                (token,),
            )
        row = cur.fetchone()

    if not row:
        raise ValueError("invalid_token")

    invite_id, tenant_id, invite_email, role, expires_at, accepted_at, tenant_slug = row[:7]
    invite_mod_perm = row[7] if len(row) > 7 and row[7] is not None else {}
    if not isinstance(invite_mod_perm, dict):
        invite_mod_perm = {}

    if accepted_at is not None:
        raise ValueError("already_accepted")

    now = datetime.now(timezone.utc)
    exp = expires_at.replace(tzinfo=timezone.utc) if expires_at and expires_at.tzinfo is None else expires_at
    if expires_at is not None and now > exp:
        raise ValueError("expired")

    if _lower(invite_email) != email_lc:
        raise ValueError("email_mismatch")

    # Username for Cognito + login
    login_name = email_lc

    # ---- Build Cognito attributes ----
    display_name = (full_name or "").strip() or email_lc

    attrs = [
        {"Name": "email", "Value": email_lc},
        {"Name": "email_verified", "Value": "true"},
        {"Name": "name", "Value": display_name},
    ]

    # Phone: Cognito expects E.164 (+###########). If not valid, skip to avoid hard failure.
    phone_clean = (phone or "").strip()
    if phone_clean:
        import re

        if re.fullmatch(r"\+\d{8,15}", phone_clean):
            attrs.append({"Name": "phone_number", "Value": phone_clean})
        # else: ignore invalid phone input safely

    client = _cognito()

    def _get_sub_from_admin_get_user(username_for_lookup: str) -> str | None:
        try:
            gu = client.admin_get_user(
                UserPoolId=settings.cognito_user_pool_id,
                Username=username_for_lookup,
            )
        except ClientError as e:
            code = e.response.get("Error", {}).get("Code", "")
            if code in ("UserNotFoundException",):
                return None
            raise

        for a in gu.get("UserAttributes", []) or []:
            if a.get("Name") == "sub" and a.get("Value"):
                return a["Value"]
        return None

    # ---- Create or reuse Cognito user (ADMIN path; works when sign-up disabled) ----
    user_sub = None
    user_exists = False

    try:
        user_sub = _get_sub_from_admin_get_user(login_name)
        user_exists = user_sub is not None
    except ClientError as e:
        # bubble unexpected errors
        raise

    if not user_exists:
        try:
            resp = client.admin_create_user(
                UserPoolId=settings.cognito_user_pool_id,
                Username=login_name,
                UserAttributes=attrs,
                MessageAction="SUPPRESS",  # do not send Cognito email
            )
            # Extract sub from response (if present)
            created = (resp.get("User") or {}).get("Attributes") or []
            for a in created:
                if a.get("Name") == "sub" and a.get("Value"):
                    user_sub = a["Value"]
                    break
        except ClientError as e:
            code = e.response.get("Error", {}).get("Code", "")
            if code == "UsernameExistsException":
                # Race condition: created between our check and create
                user_sub = _get_sub_from_admin_get_user(login_name)
                user_exists = True
            elif code == "InvalidPasswordException":
                raise ValueError("invalid_password")
            elif code == "InvalidParameterException":
                # often caused by invalid phone_number or attributes
                raise ValueError("invalid_request")
            elif code == "NotAuthorizedException":
                # IAM permissions missing for admin APIs
                raise ValueError("cognito_admin_not_authorized")
            else:
                raise

    if not user_sub:
        # If still missing, fetch it (covers cases where create response omitted it)
        user_sub = _get_sub_from_admin_get_user(login_name)

    if not user_sub:
        raise RuntimeError("Could not determine Cognito user sub")

    # Set password permanently (works for both new and existing users)
    try:
        client.admin_set_user_password(
            UserPoolId=settings.cognito_user_pool_id,
            Username=login_name,
            Password=password,
            Permanent=True,
        )
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code", "")
        if code == "InvalidPasswordException":
            raise ValueError("invalid_password")
        if code == "NotAuthorizedException":
            raise ValueError("cognito_admin_not_authorized")
        raise

    # Optional: ensure email_verified stays true (especially if user existed)
    # (safe to ignore failures)
    try:
        client.admin_update_user_attributes(
            UserPoolId=settings.cognito_user_pool_id,
            Username=login_name,
            UserAttributes=[
                {"Name": "email", "Value": email_lc},
                {"Name": "email_verified", "Value": "true"},
                {"Name": "name", "Value": display_name},
            ],
        )
    except ClientError:
        pass

    # ---- DB: upsert user, membership, accept invite ----
    import json

    mod_perm_json = json.dumps(invite_mod_perm)
    with db_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO users (cognito_sub, email)
            VALUES (%s, %s)
            ON CONFLICT (cognito_sub) DO UPDATE SET email = EXCLUDED.email
            RETURNING id
            """,
            (user_sub, email_lc),
        )
        user_id = cur.fetchone()[0]

        try:
            cur.execute(
                """
                INSERT INTO memberships (tenant_id, user_id, role, module_permissions)
                VALUES (%s, %s, %s, %s::jsonb)
                ON CONFLICT (tenant_id, user_id) DO UPDATE
                    SET role = EXCLUDED.role, module_permissions = EXCLUDED.module_permissions
                """,
                (tenant_id, user_id, _lower(role), mod_perm_json),
            )
        except Exception:
            cur.execute(
                """
                INSERT INTO memberships (tenant_id, user_id, role)
                VALUES (%s, %s, %s)
                ON CONFLICT (tenant_id, user_id) DO UPDATE SET role = EXCLUDED.role
                """,
                (tenant_id, user_id, _lower(role)),
            )

        cur.execute("UPDATE invites SET accepted_at = now() WHERE id = %s", (invite_id,))
        conn.commit()

    # ---- Return immediate login tokens if possible ----
    result = validate_login(login_name, password, tenant_slug=tenant_slug)
    if not result:
        return {
            "ok": True,
            "message": "Account created. Please sign in.",
            "tenant_slug": tenant_slug,
            "role": role,
        }

    return {
        "ok": True,
        "access_token": result["access_token"],
        "refresh_token": result.get("refresh_token"),
        "token_type": result.get("token_type", "Bearer"),
        "user": result.get("user", {}),
        "tenant_slug": tenant_slug,
        "role": role,
    }