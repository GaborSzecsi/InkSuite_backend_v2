# app/auth/service.py  (COMPLETE DROP-IN)
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


def validate_login(email: str, password: str, tenant_slug: str | None = None) -> Optional[dict]:
    """
    Returns dict with access_token (+ refresh_token if returned) on success, else None.
    Raises ValueError for challenge flows (e.g. NEW_PASSWORD_REQUIRED).
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

    # Verified claims (strict)
    user_claims = get_current_user_from_token(access_token) or {}

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
    # First try cached JWKS
    jwks = _get_jwks(force_refresh=False)
    key = next((k for k in jwks.get("keys", []) if k.get("kid") == kid), None)
    if key:
        return key

    # Key rotation hardening: refresh once immediately
    jwks = _get_jwks(force_refresh=True)
    return next((k for k in jwks.get("keys", []) if k.get("kid") == kid), None)


def get_current_user_from_token(token: str | None) -> Optional[dict]:
    """
    Strictly verifies a Cognito ACCESS token.

    Enforces:
      - signature via JWKS (kid-matched key)
      - iss
      - exp
      - token_use == "access"
      - client id matches (via client_id claim; aud fallback if present)

    Returns claims dict if valid, else None.
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

        # For Cognito ACCESS tokens, aud is not always reliable across configs.
        # Verify signature/iss/exp, then enforce client_id ourselves.
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

        # Accept if either claim matches our configured client id
        if token_client_id != expected_client_id and token_aud != expected_client_id:
            return None

        return claims

    except (JWTError, Exception):
        return None


# -----------------------------
# DB-backed "me" enrichment
# -----------------------------
def _db_available() -> bool:
    if db_conn is None:
        return False
    return bool((os.environ.get("DATABASE_URL") or "").strip())


def _lower(s: str | None) -> str:
    return (s or "").strip().lower()


def get_me_payload_from_claims(claims: dict) -> dict:
    """
    Stable response for /api/auth/me:
      - loggedIn: True
      - user: claims + platform_role (+ canonical email if present in DB)
      - tenants: list[{tenant_slug, role}]
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
            # Insert email if present; on conflict don't overwrite existing with blank.
            # NOTE: Your schema requires users.email NOT NULL. If token lacks email,
            # we set to 'unknown' on insert only, and we do not overwrite a real email later.
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
            tenants = [{"tenant_slug": r[0], "role": r[1]} for r in cur.fetchall()]

        out["user"]["email"] = db_email or out["user"].get("email") or "unknown"
        out["user"]["platform_role"] = platform_role or out["user"].get("platform_role") or "user"
        out["tenants"] = tenants
        return out

    except Exception as e:
        # Never break /me. Log for debugging.
        try:
            import logging

            logging.getLogger("auth").exception("DB-backed /auth/me failed: %s", e)
        except Exception:
            pass

        out["user"].setdefault("platform_role", "user")
        return out


# -----------------------------
# Tenant roles + invitations
# -----------------------------
TENANT_ADMIN = "tenant_admin"
TENANT_EDITOR = "tenant_editor"
TENANT_ALLOWED = {TENANT_ADMIN, TENANT_EDITOR}


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
    Returns list[{tenant_id, tenant_slug, role}]
    """
    if not _db_available():
        return []
    with db_conn() as conn, conn.cursor() as cur:
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
        return [{"tenant_id": r[0], "tenant_slug": r[1], "role": r[2]} for r in cur.fetchall()]


def require_tenant_role(
    *,
    claims: dict,
    tenant_slug: str,
    allowed_roles: set[str],
) -> dict:
    """
    Enforce tenant-scoped authorization.

    Returns:
      {
        "user": {id,email,platform_role},
        "memberships": [...],
        "tenant": {id,slug,role}
      }

    Superadmin bypasses membership requirement.
    Raises ValueError("unauthorized") or ValueError("forbidden").
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


def create_invite(
    *,
    invited_by_user_id,
    tenant_slug: str,
    email: str,
    role: str,
    ttl_hours: int = 72,
) -> dict:
    """
    Creates an invite and returns:
      {invite_id, token, expires_at, tenant_slug, email, role}
    """
    if not _db_available():
        raise RuntimeError("DB not configured")

    email_lc = _lower(email)
    role_lc = _lower(role)
    if role_lc not in TENANT_ALLOWED:
        raise ValueError("invalid_role")

    token = secrets.token_urlsafe(32)
    expires_at = datetime.now(timezone.utc) + timedelta(hours=int(ttl_hours or 72))

    with db_conn() as conn, conn.cursor() as cur:
        cur.execute("SELECT id FROM tenants WHERE lower(slug)=lower(%s)", (tenant_slug,))
        tr = cur.fetchone()
        if not tr:
            raise ValueError("tenant_not_found")
        tenant_id = tr[0]

        cur.execute(
            """
            INSERT INTO invites (tenant_id, email, role, token, invited_by, expires_at)
            VALUES (%s, %s, %s, %s, %s, %s)
            RETURNING id
            """,
            (tenant_id, email_lc, role_lc, token, invited_by_user_id, expires_at.replace(tzinfo=None)),
        )
        invite_id = cur.fetchone()[0]

    return {
        "invite_id": str(invite_id),
        "token": token,
        "expires_at": expires_at.isoformat(),
        "tenant_slug": tenant_slug,
        "email": email_lc,
        "role": role_lc,
    }


def accept_invite(*, claims: dict, token: str, require_email_match: bool = True) -> dict:
    """
    Accepts an invite for the authenticated user:
      - token exists, not expired, not accepted
      - optionally require invite email matches Cognito email claim
      - upserts membership role
      - marks invite accepted

    Returns: {tenant_slug, role}
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

        invite_id, tenant_id, invite_email, role, expires_at, accepted_at, tenant_slug = row

        if accepted_at is not None:
            raise ValueError("already_accepted")
        if expires_at is not None and now > expires_at:
            raise ValueError("expired")

        if require_email_match:
            if not token_email or _lower(invite_email) != token_email:
                raise ValueError("email_mismatch")

        # Upsert membership (requires UNIQUE (tenant_id, user_id) or PK)
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

    return {"tenant_slug": tenant_slug, "role": role}