"""Marketplace invitations service; transactions and mutation boundaries live here."""

from . import invitations_repository as _repository

"""Manually approved Marketplace access; no tenant memberships are granted."""

import hashlib
import os
import secrets
from datetime import datetime, timezone
from typing import Literal
from urllib.parse import urlsplit
from uuid import UUID

from botocore.exceptions import BotoCoreError, ClientError
from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel, EmailStr, Field, ConfigDict

from app.auth import service as auth
from app.core.config import settings
from .core import transaction, one, rows, current_user
from .identity import throttle

router = APIRouter()
APPROVER = "szecsi.gabor@gmail.com"


class AccessRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)
    name: str = Field(min_length=1, max_length=100)
    email: EmailStr
    access_type: Literal["reader", "bookstore"]


class Decision(BaseModel):
    action: Literal["approve", "reject"]


class InviteToken(BaseModel):
    token: str = Field(min_length=40, max_length=128)


class Acceptance(InviteToken):
    password: str = Field(min_length=8, max_length=256)


def approver(user=Depends(current_user)):
    # The notification mailbox is not an authorization identity. Reuse the
    # platform administrator permission already used throughout InkSuite.
    if not auth.is_superadmin(user.get("platform_role")):
        raise HTTPException(
            403,
            "This signed-in account does not have InkSuite platform-administrator permission. "
            "Sign in with your platform administrator account to review requests.",
        )
    return user


def ready(cur):
    if not _repository.ready_query_1(cur)["ready"]:
        raise HTTPException(
            503,
            "Invitation setup is pending. Apply migration 008_marketplace_invitations.sql.",
        )


def frontend():
    value = os.getenv("MARKETPLACE_FRONTEND_URL", "http://localhost:3000").rstrip("/")
    url = urlsplit(value)
    if (
        url.username
        or url.password
        or url.query
        or url.fragment
        or url.path
        or not url.hostname
    ):
        raise HTTPException(
            503, "The invitation website URL is not configured correctly."
        )
    if url.scheme != "https" and not (
        url.scheme == "http" and url.hostname in ("localhost", "127.0.0.1")
    ):
        raise HTTPException(503, "The invitation website URL must use HTTPS.")
    if settings.node_env == "production" and url.hostname in ("localhost", "127.0.0.1"):
        raise HTTPException(
            503,
            "Configure MARKETPLACE_FRONTEND_URL before enabling public invitations.",
        )
    return value


def mail(to, subject, text):
    # Use precisely the same settings, secret loader and sender as bookdev questionnaires.
    from routers.contract_invites import (
        _load_tenant_email_settings_or_400,
        _load_smtp_secret,
        _send_email_smtp,
    )
    import logging

    try:
        config = _load_tenant_email_settings_or_400(
            os.getenv("MARKETPLACE_MAIL_TENANT", "marble-press")
        )
        username, password = _load_smtp_secret(config["smtp_secret_id"])
        message_id = _send_email_smtp(
            smtp_host=config["smtp_host"],
            smtp_port=config["smtp_port"],
            tls_mode=config["tls_mode"],
            username=username,
            password=password,
            from_email=config["from_email"],
            from_name=config["from_name"],
            to_email=to,
            to_name="",
            subject=subject,
            body_text=text,
        )
        logging.getLogger(__name__).info("Marketplace invitation submitted to SMTP")
    except Exception as exc:
        logging.getLogger(__name__).warning(
            "Marketplace invitation SMTP delivery failed (%s)", type(exc).__name__
        )
        raise HTTPException(
            503,
            "Invitation email could not be delivered using the configured questionnaire email service. Please retry or contact InkSuite.",
        ) from None


def request_access(body: AccessRequest, request: Request):
    throttle(request)
    email = str(body.email).lower()
    with transaction() as cur:
        ready(cur)
        _repository.request_access_query_1(cur, email)
        existing = _repository.request_access_query_2(cur, email)
        if (
            existing
            and existing["status"] == "approved"
            and existing["expires_at"] < datetime.now(timezone.utc)
        ):
            _repository.request_access_query_3(cur, existing)
            existing = None
        if not existing:
            recent = _repository.request_access_query_4(cur, email)
            if recent:
                return {
                    "ok": True,
                    "message": "Your request is awaiting review. You will receive a signup invitation after approval.",
                }
            existing = _repository.request_access_query_5(cur, email, body)
        if existing["status"] == "approved":
            return {
                "ok": True,
                "message": "This request is already approved. If the signup email is missing, contact InkSuite to resend your invitation.",
            }
        if existing["notified_at"] is not None and not existing.get(
            "can_notify", False
        ):
            return {
                "ok": True,
                "message": "Your request is awaiting review. A notification was recently submitted to the mail server; no duplicate email was sent. You can retry after 10 minutes.",
            }
        if existing["notified_at"] is None or existing.get("can_notify", False):
            send_approval_link(cur, existing)
    return {
        "ok": True,
        "message": "Your request is saved. The notification was submitted to the mail server for review; inbox delivery is not yet confirmed.",
    }


def review_queue(user=Depends(approver)):
    with transaction() as cur:
        ready(cur)
        return {"items": _repository.review_queue_query_1(cur)}


def review(request_id: UUID, user=Depends(approver)):
    with transaction() as cur:
        ready(cur)
        row = _repository.review_query_1(cur, request_id)
        if not row:
            raise HTTPException(404, "Request not found.")
        return row


def decide(request_id: UUID, body: Decision, user=Depends(approver)):
    with transaction() as cur:
        ready(cur)
        row = _repository.decide_query_1(cur, request_id)
        return apply_decision(cur, row, body.action, user["id"])


def apply_decision(cur, row, action, reviewer_id=None):
    if not row or row["status"] not in ("pending", "approved"):
        raise HTTPException(409, "This request is no longer awaiting approval.")
    if action == "reject":
        _repository.apply_decision_query_2(cur, reviewer_id, row)
        return {"ok": True, "message": "Request declined."}
    token = secrets.token_urlsafe(32)
    # Fragment tokens are not sent in HTTP URLs, server logs, or referrers.
    link = frontend() + "/marketplace/invitation#token=" + token
    mail(
        row["email"],
        "Your InkSuite invitation",
        f"Hello {row['name']},\n\nYour request for {row['access_type']} access has been approved.\n\nCreate your account here: {link}\n\nThis link expires in 7 days and can be used once. If you already have an InkSuite account, sign in before accepting.\n\nInkSuite",
    )
    _repository.apply_decision_query_1(cur, reviewer_id, row, hashlib, token)
    return {"ok": True, "message": "Approved. The signup invitation has been emailed."}


def invitation(cur, token):
    ready(cur)
    row = _repository.invitation_query_1(cur, hashlib, token)
    if not row:
        raise HTTPException(
            410,
            "This invitation is invalid, expired, or already used. Request a new invitation.",
        )
    return row


def inspect(body: InviteToken, request: Request):
    throttle(request)
    with transaction() as cur:
        row = invitation(cur, body.token)
        return {
            "name": row["name"],
            "email": row["email"],
            "access_type": row["access_type"],
        }


def grant(cur, row, user_id):
    actor = _repository.grant_query_1(cur, user_id)
    if actor and actor["status"] != "active":
        raise HTTPException(
            403, "This Marketplace account is unavailable. Contact InkSuite."
        )
    from .usernames import name_username, available_username
    username = available_username(cur, name_username(row["name"]), user_id, lock=True)
    _repository.grant_query_2(cur, user_id, username, row)
    _repository.grant_query_3(cur, user_id)
    _repository.grant_query_4(cur, user_id, row)
    _repository.grant_query_5(cur, user_id, row)


def accept_existing(body: InviteToken, request: Request, user=Depends(current_user)):
    throttle(request)
    with transaction() as cur:
        row = invitation(cur, body.token)
        if user.get("email", "").lower() != row["email"]:
            raise HTTPException(
                403, "Sign in with the email address this invitation was sent to."
            )
        grant(cur, row, user["id"])
    return {"ok": True}


def accept(body: Acceptance, request: Request):
    throttle(request)
    client = auth._cognito()
    created_username = None
    committed = False
    try:
        with transaction() as cur:
            row = invitation(cur, body.token)
            # Never reset an existing account's password through an invitation.
            existing = _repository.accept_query_1(cur, row)
            if existing:
                raise HTTPException(
                    409,
                    "An account already exists. Sign in with the invited email and accept this invitation.",
                )
            response = client.admin_create_user(
                UserPoolId=settings.cognito_user_pool_id,
                Username=row["email"],
                MessageAction="SUPPRESS",
                UserAttributes=[
                    {"Name": "email", "Value": row["email"]},
                    {"Name": "email_verified", "Value": "true"},
                    {"Name": "name", "Value": row["name"]},
                ],
            )
            created_username = response["User"]["Username"]
            sub = next(
                a["Value"] for a in response["User"]["Attributes"] if a["Name"] == "sub"
            )
            client.admin_set_user_password(
                UserPoolId=settings.cognito_user_pool_id,
                Username=created_username,
                Password=body.password,
                Permanent=True,
            )
            user = _repository.accept_query_2(cur, sub, row)
            grant(cur, row, user["id"])
        committed = True
        return {"ok": True, "message": "Account created. You can now sign in."}
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code")
        if code == "UsernameExistsException":
            raise HTTPException(
                409,
                "An account already exists. Sign in with the invited email and accept this invitation.",
            ) from None
        if code == "InvalidPasswordException":
            raise HTTPException(
                422, "Choose a password that meets InkSuite's password requirements."
            ) from None
        raise HTTPException(
            503, "Account setup could not finish. Please retry or contact InkSuite."
        ) from None
    except BotoCoreError:
        raise HTTPException(
            503, "Account setup could not reach the identity service. Please retry."
        ) from None
    finally:
        if created_username and not committed:
            # Only compensate the new identity created by THIS attempt, never an existing user.
            try:
                client.admin_delete_user(
                    UserPoolId=settings.cognito_user_pool_id, Username=created_username
                )
            except (BotoCoreError, ClientError):
                pass


class ApprovalDecision(InviteToken):
    action: Literal["approve", "reject"]


def send_approval_link(cur, row):
    token = secrets.token_urlsafe(32)
    link = frontend() + "/marketplace/approval#token=" + token
    mail(
        APPROVER,
        "Approve an InkSuite access request",
        f"Name: {row['name']}\nEmail: {row['email']}\nRequested access: {row['access_type']}\n\nReview and approve: {link}\n\nNo login is needed. This private link expires in 48 hours and can be used once. Opening it does not approve access; click Approve on the page. Do not forward this approval link.",
    )
    _repository.send_approval_link_query_1(cur, row, hashlib, token)


def pending_approval(cur, token):
    ready(cur)
    row = _repository.pending_approval_query_1(cur, hashlib, token)
    if not row:
        raise HTTPException(
            410, "This approval link has expired, been replaced, or already been used."
        )
    return row


def inspect_approval(body: InviteToken, request: Request):
    throttle(request)
    with transaction() as cur:
        row = pending_approval(cur, body.token)
        return {
            "name": row["name"],
            "email": row["email"],
            "access_type": row["access_type"],
        }


def decide_approval(body: ApprovalDecision, request: Request):
    throttle(request)
    with transaction() as cur:
        row = pending_approval(cur, body.token)
        return apply_decision(cur, row, body.action)


def resend_approval_link(request_id: UUID, request: Request):
    # Old email links can request replacement ONLY to the configured approver.
    throttle(request)
    with transaction() as cur:
        ready(cur)
        row = _repository.resend_approval_link_query_1(cur, request_id)
        if row and row["can_send"]:
            send_approval_link(cur, row)
    return {
        "message": "If this request is pending, a secure approval link has been sent to the approver. Allow two minutes between requests."
    }
