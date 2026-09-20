"""Marketplace identity service; transactions and mutation boundaries live here."""

from . import identity_repository as _repository
from time import monotonic
from threading import Lock
from collections import defaultdict, deque
from uuid import UUID
from typing import Literal
from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel, Field, EmailStr
from botocore.exceptions import ClientError
from app.auth import service as auth
from app.auth.dependencies import require_session
from app.core.config import settings
from .core import transaction, one, rows, required, current_user, public_actor
from .schemas import Profile

router = APIRouter()
_attempts = defaultdict(deque)
_lock = Lock()


class Credentials(BaseModel):
    email: EmailStr
    password: str = Field(min_length=1, max_length=256)
    account_type: Literal["reader", "bookstore"] | None = None
    company_name: str = Field(default="", max_length=100)


class Confirmation(BaseModel):
    email: EmailStr
    code: str = Field(min_length=1, max_length=64)


class EmailBody(BaseModel):
    email: EmailStr


def resend(body: EmailBody, request: Request):
    throttle(request)
    args = {"ClientId": settings.cognito_client_id, "Username": str(body.email)}
    secret = auth._secret_hash(str(body.email))
    if secret:
        args["SecretHash"] = secret
    try:
        auth._cognito().resend_confirmation_code(**args)
    except ClientError:
        # Avoid distinguishing missing or already-confirmed accounts.
        pass
    return {
        "ok": True,
        "message": "If this account needs confirmation, a new code has been sent.",
    }


def throttle(request):
    # Basic auth-endpoint protection; uses peer address, never spoofable forwarded headers.
    key = request.client.host if request.client else "unknown"
    with _lock:
        now = monotonic()
        for old in list(_attempts):
            while _attempts[old] and _attempts[old][0] < now - 60:
                _attempts[old].popleft()
            if not _attempts[old]:
                del _attempts[old]
        bucket = _attempts[key]
        if len(bucket) >= 10:
            raise HTTPException(429, "Too many attempts. Please try again in a minute.")
        bucket.append(now)


def login(body: Credentials, request: Request):
    throttle(request)
    params = {"USERNAME": str(body.email).strip(), "PASSWORD": body.password}
    secret = auth._secret_hash(params["USERNAME"])
    if secret:
        params["SECRET_HASH"] = secret
    try:
        response = auth._cognito().initiate_auth(
            AuthFlow="USER_PASSWORD_AUTH",
            ClientId=settings.cognito_client_id,
            AuthParameters=params,
        )
    except ClientError:
        raise HTTPException(
            401, "Sign-in failed. Check your details and confirm your email."
        ) from None
    if response.get("ChallengeName"):
        raise HTTPException(
            409,
            "This account requires an additional sign-in step. Complete it through InkSuite company sign-in.",
        )
    tokens = response.get("AuthenticationResult", {})
    claims = auth.get_current_user_from_token(tokens.get("AccessToken"))
    if not claims:
        raise HTTPException(401, "Sign-in failed.")
    # Cognito authenticates the same human; no tenant is created or required.
    user = auth.get_user_db_record_from_claims(claims)
    if body.account_type:
        with transaction() as cur:
            if not _repository.login_query_2(cur)["ready"]:
                raise HTTPException(
                    503,
                    "Invitation setup is pending. Apply migration 008_marketplace_invitations.sql.",
                )
            grant = _repository.login_query_1(cur, user)
            allowed = grant and (
                grant["access_type"] == body.account_type
                or (
                    body.account_type == "reader"
                    and grant["access_type"] == "librarian"
                )
            )
            if not allowed:
                raise HTTPException(
                    403,
                    "This account does not have approved access for the selected account type.",
                )
            if (
                body.account_type == "bookstore"
                and " ".join(body.company_name.split()).casefold()
                != " ".join(grant["name"].split()).casefold()
            ):
                raise HTTPException(
                    403, "Bookstore name does not match your approved invitation."
                )
    return {
        "access_token": tokens["AccessToken"],
        "refresh_token": tokens.get("RefreshToken"),
    }


def register(body: Credentials, request: Request):
    throttle(request)
    raise HTTPException(403, "Reader and librarian registration is coming soon.")


def confirm(body: Confirmation, request: Request):
    throttle(request)
    args = {
        "ClientId": settings.cognito_client_id,
        "Username": str(body.email),
        "ConfirmationCode": body.code,
    }
    secret = auth._secret_hash(str(body.email))
    if secret:
        args["SecretHash"] = secret
    try:
        auth._cognito().confirm_sign_up(**args)
    except ClientError:
        raise HTTPException(
            422, "The confirmation code is invalid or expired."
        ) from None
    return {"ok": True}


def me(claims=Depends(require_session)):
    if not claims:
        return {
            "authenticated": False,
            "actors": [],
            "organizations": [],
            "tenants": [],
        }
    user = auth.get_user_db_record_from_claims(claims)
    with transaction() as cur:
        profile = _repository.me_query_1(cur, user)
        tenants = _repository.me_query_2(cur, user)
        if user.get("platform_role") == "superadmin":
            tenants = _repository.me_query_5(cur)
        orgs = _repository.me_query_3(cur)
        allowed = []
        for o in orgs:
            member = next(
                (t for t in tenants if str(t["id"]) == str(o["tenant_id"])), None
            )
            if not member or o["status"] == "suspended":
                continue
            admin = member["role"] == "tenant_admin"
            if not admin and not member["module_permissions"].get("marketplace"):
                continue
            allowed.append(
                dict(
                    o,
                    admin=admin,
                    can_message=admin
                    or member["module_permissions"].get("marketplace_messages", False),
                )
            )
        own = _repository.me_query_4(cur, user)
        actors = (
            [dict(public_actor(cur, own["id"], True), can_message=True)] if own else []
        )
        actors += [
            dict(
                public_actor(cur, o["actor_id"], True),
                organization_id=o["id"],
                admin=o["admin"],
                can_message=o["can_message"],
            )
            for o in allowed
        ]
        if profile and own:
            profile["messaging_preference"] = own["messaging_preference"]
        return {
            "authenticated": True,
            "can_review_invitations": auth.is_superadmin(user.get("platform_role")),
            "profile": profile,
            "actors": actors,
            "organizations": allowed,
            "tenants": [
                {"id": t["id"], "name": t["name"], "slug": t["slug"]}
                for t in tenants
                if t["role"] == "tenant_admin"
            ],
        }


def save_profile(body: Profile, user=Depends(current_user)):
    with transaction() as cur:
        existing = _repository.save_profile_query_1(cur, user)
        if existing and existing["status"] != "active":
            raise HTTPException(403, "This Marketplace profile is unavailable.")
        _repository.save_profile_query_2(cur, user, body)
        result = _repository.save_profile_query_3(cur, user, body)
        return public_actor(cur, result["id"], True)


def profile(username: str):
    with transaction() as cur:
        p = required(_repository.profile_query_1(cur, username))
        return dict(
            public_actor(cur, p["id"]),
            location_text=p["location_text"],
            followers=_repository.profile_query_2(cur, p)["n"],
        )
