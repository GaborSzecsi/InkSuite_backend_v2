from __future__ import annotations

import base64
import hashlib
import hmac
import time
from typing import Any, Dict, Optional

import boto3
import requests
from botocore.exceptions import ClientError
from jose import JWTError, jwt

from app.core.config import settings

# NEW: DB helper
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
# NEW: DB-backed "me" enrichment
# -----------------------------
def _db_available() -> bool:
    if db_conn is None:
        return False
    return bool((os.environ.get("DATABASE_URL") or "").strip())


def get_me_payload_from_claims(claims: dict) -> dict:
    """
    Returns a stable response for /api/auth/me:
      - loggedIn: True
      - user: Cognito claims (as before) PLUS platform_role
      - tenants: list[{tenant_slug, role}]
    If DB isn't configured yet, returns tenants=[] and platform_role='user' (unless present in DB later).
    """
    sub = (claims or {}).get("sub")
    email = (claims or {}).get("email") or "unknown"

    # Default/fallback shape (keeps UI working even during migration)
    out = {
        "loggedIn": True,
        "user": dict(claims or {}),
        "tenants": [],
    }

    if not sub or not _db_available():
        # Keep claims-only behavior (migration-safe)
        out["user"].setdefault("platform_role", "user")
        return out

    try:
        with db_conn() as conn, conn.cursor() as cur:
            # Upsert user (email is best-effort; access tokens often don't include email)
            cur.execute(
                """
                INSERT INTO users (cognito_sub, email)
                VALUES (%s, %s)
                ON CONFLICT (cognito_sub)
                DO UPDATE SET email = EXCLUDED.email
                """,
                (sub, email),
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
        out["user"]["platform_role"] = platform_role or "user"
        out["tenants"] = tenants
        return out

    except Exception:
        # Never break /me during migration—fallback to claims-only
        out["user"].setdefault("platform_role", "user")
        return out