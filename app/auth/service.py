# app/auth/service.py
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
        # Wrong creds / user not found
        if code in ("NotAuthorizedException", "UserNotFoundException"):
            return None
        # Typical challenge flows
        if code in ("PasswordResetRequiredException", "UserNotConfirmedException"):
            raise ValueError(code)
        raise

    # Challenge?
    if resp.get("ChallengeName"):
        raise ValueError(resp["ChallengeName"])

    ar = resp.get("AuthenticationResult") or {}
    access_token = ar.get("AccessToken")
    if not access_token:
        return None

    refresh_token = ar.get("RefreshToken")
    token_type = ar.get("TokenType", "Bearer")

    # Decode (verified) access token to provide user claims in response
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
    # If config computed jwks/issuer, use them
    iss = getattr(settings, "cognito_issuer", "") or ""
    if iss:
        return iss
    return f"https://cognito-idp.{settings.cognito_region}.amazonaws.com/{settings.cognito_user_pool_id}"


def _jwks_url() -> str:
    url = getattr(settings, "jwks_url", "") or ""
    if url:
        return url
    return f"{_issuer()}/.well-known/jwks.json"


def _get_jwks() -> dict:
    global _JWKS_CACHE, _JWKS_CACHE_AT
    now = time.time()
    if _JWKS_CACHE and (now - _JWKS_CACHE_AT) < _JWKS_TTL_SECONDS:
        return _JWKS_CACHE

    r = requests.get(_jwks_url(), timeout=5)
    r.raise_for_status()
    _JWKS_CACHE = r.json()
    _JWKS_CACHE_AT = now
    return _JWKS_CACHE


def get_current_user_from_token(token: str | None) -> Optional[dict]:
    """
    Strictly verifies Cognito ACCESS token.
    Returns claims dict if valid, else None.
    """
    if not token:
        return None

    try:
        header = jwt.get_unverified_header(token)
        kid = header.get("kid")
        if not kid:
            return None

        jwks = _get_jwks()
        key = next((k for k in jwks.get("keys", []) if k.get("kid") == kid), None)
        if not key:
            return None

        claims = jwt.decode(
            token,
            key,
            algorithms=["RS256"],
            audience=settings.cognito_client_id,
            issuer=_issuer(),
            options={"verify_aud": True, "verify_iss": True, "verify_exp": True},
        )

        # Only allow access tokens to authenticate API calls
        if claims.get("token_use") != "access":
            return None

        return claims
    except (JWTError, Exception):
        return None
