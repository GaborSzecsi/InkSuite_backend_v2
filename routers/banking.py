"""InkSuite secure banking API.

Drop-in replacement for the JSON-backed banking.py prototype.
Dependencies: boto3, cryptography, psycopg[binary]
Required env: DATABASE_URL, BANKING_KMS_KEY_ID, BANKING_TOKEN_PEPPER,
              PUBLIC_APP_URL (eg https://www.inksuite.io)
Optional env: AWS_REGION

IMPORTANT: mount this router behind the same authenticated API used by InkSuite.
The /public/* endpoints are intentionally token-gated and do not require a user session.
"""
from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
import secrets
import uuid
from datetime import datetime, timedelta, timezone
from typing import Literal

import boto3
import psycopg
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from fastapi import APIRouter, Header, HTTPException, Query, Request
from pydantic import BaseModel, Field

from routers.contract_invites import (
    _load_smtp_secret,
    _load_tenant_email_settings_or_400,
    _send_email_smtp,
)

router = APIRouter(prefix="/banking", tags=["Banking"])

DATABASE_URL = os.environ["DATABASE_URL"]
KMS_KEY_ID = os.environ["BANKING_KMS_KEY_ID"]
TOKEN_PEPPER = os.environ["BANKING_TOKEN_PEPPER"].encode("utf-8")
PUBLIC_APP_URL = os.environ.get("PUBLIC_APP_URL", "https://www.inksuite.io").rstrip("/")
AWS_REGION = os.environ.get("AWS_REGION", "us-east-2")
kms = boto3.client("kms", region_name=AWS_REGION)

RecipientType = Literal["CONTRIBUTOR", "AGENT", "AGENCY"]
PaymentMethod = Literal["ACH", "DOMESTIC_WIRE", "INTERNATIONAL_WIRE"]


def db():
    return psycopg.connect(DATABASE_URL)


def now():
    return datetime.now(timezone.utc)


def digest(value: str) -> str:
    return hmac.new(TOKEN_PEPPER, value.encode("utf-8"), hashlib.sha256).hexdigest()


def tenant_from_request(request: Request, tenant_slug: str | None = None) -> str:
    # Replace this resolver with your Cognito membership resolver when available.
    # Never accept an arbitrary tenant for authenticated commercial traffic.
    header_tenant = (request.headers.get("X-Tenant") or "").strip()
    tenant = header_tenant or (tenant_slug or "").strip()
    if not tenant:
        raise HTTPException(400, "Missing tenant context")
    return tenant


def actor(request: Request) -> tuple[str | None, str | None]:
    # Compatible with middleware that places Cognito claims on request.state.
    claims = getattr(request.state, "claims", None) or getattr(request.state, "user", None) or {}
    if isinstance(claims, dict):
        return str(claims.get("sub") or "") or None, str(claims.get("email") or "") or None
    return None, None


def audit(cur, tenant: str, action: str, request: Request, **kwargs):
    actor_id, actor_email = actor(request)
    metadata = kwargs.pop("metadata", {})
    cur.execute(
        """INSERT INTO secure_payments.payment_audit_events
           (tenant_id, actor_id, actor_email, payment_profile_id, payment_account_id, request_id, action, metadata)
           VALUES (%s,%s,%s,%s,%s,%s,%s,%s::jsonb)""",
        (tenant, actor_id, actor_email, kwargs.get("payment_profile_id"), kwargs.get("payment_account_id"),
         kwargs.get("request_id"), action, json.dumps(metadata)),
    )


def send_email(tenant_slug: str, to: str, subject: str, text: str, to_name: str = ""):
    """Send through the tenant's existing InkSuite SMTP configuration.

    This intentionally reuses the same sender infrastructure as Book Development
    requests instead of introducing a separate SES sender.
    """
    settings = _load_tenant_email_settings_or_400(tenant_slug)
    username, password = _load_smtp_secret(settings["smtp_secret_id"])

    _send_email_smtp(
        smtp_host=settings["smtp_host"],
        smtp_port=settings["smtp_port"],
        tls_mode=settings["tls_mode"],
        username=username,
        password=password,
        from_email=settings["from_email"],
        from_name=settings["from_name"],
        to_email=to,
        to_name=to_name,
        subject=subject,
        body_text=text,
    )


def aad(tenant: str, account_id: str) -> bytes:
    return json.dumps(
        {"tenant_id": tenant, "payment_account_id": account_id, "purpose": "payment-account"},
        separators=(",", ":"), sort_keys=True,
    ).encode("utf-8")


def encrypt_payload(tenant: str, account_id: str, payload: dict) -> tuple[bytes, bytes, bytes]:
    context = {"tenant_id": tenant, "payment_account_id": account_id, "purpose": "payment-account"}
    key = kms.generate_data_key(KeyId=KMS_KEY_ID, KeySpec="AES_256", EncryptionContext=context)
    plaintext_key = bytearray(key["Plaintext"])
    try:
        nonce = secrets.token_bytes(12)
        ciphertext = AESGCM(bytes(plaintext_key)).encrypt(
            nonce, json.dumps(payload, separators=(",", ":")).encode("utf-8"), aad(tenant, account_id)
        )
        return ciphertext, key["CiphertextBlob"], nonce
    finally:
        for i in range(len(plaintext_key)):
            plaintext_key[i] = 0


class BankingRequestCreate(BaseModel):
    recipient_type: RecipientType
    recipient_id: str = Field(min_length=1, max_length=200)
    recipient_name: str | None = Field(default=None, max_length=300)
    recipient_email: str = Field(min_length=3, max_length=320)
    requester_email: str | None = Field(default=None, max_length=320)


class OtpVerify(BaseModel):
    code: str = Field(pattern=r"^\d{6}$")


class BankSubmission(BaseModel):
    payment_method: PaymentMethod = "ACH"
    legal_account_holder_name: str = Field(min_length=1, max_length=300)
    country_code: str = Field(default="US", min_length=2, max_length=2)
    bank_name: str | None = Field(default=None, max_length=300)
    bank_address: str | None = Field(default=None, max_length=500)
    routing_number: str | None = Field(default=None, pattern=r"^\d{9}$")
    account_number: str | None = Field(default=None, min_length=4, max_length=34)
    account_type: Literal["checking", "savings", "other"] | None = "checking"
    swift_bic: str | None = Field(default=None, max_length=11)
    iban: str | None = Field(default=None, max_length=34)
    currency: str = Field(default="USD", min_length=3, max_length=3)


@router.get("/status")
def payment_status(request: Request, recipient_type: RecipientType, recipient_id: str,
                   tenant_slug: str | None = Query(default=None)):
    tenant = tenant_from_request(request, tenant_slug)
    with db() as conn, conn.cursor() as cur:
        cur.execute("""SELECT p.status, a.bank_name, a.account_last4, a.payment_method, a.verification_status, a.updated_at
                       FROM secure_payments.payment_profiles p
                       LEFT JOIN secure_payments.payment_accounts a ON a.payment_profile_id=p.id AND a.is_active
                       WHERE p.tenant_id=%s AND p.recipient_type=%s AND p.recipient_id=%s""",
                    (tenant, recipient_type, recipient_id))
        row = cur.fetchone()
    if not row:
        return {"status": "MISSING"}
    return {"status": row[0], "bank_name": row[1], "account_last4": row[2], "payment_method": row[3],
            "verification_status": row[4], "updated_at": row[5]}


@router.post("/requests")
def create_banking_request(payload: BankingRequestCreate, request: Request,
                           tenant_slug: str | None = Query(default=None)):
    tenant = tenant_from_request(request, tenant_slug)
    token = secrets.token_urlsafe(32)
    request_id = str(uuid.uuid4())
    expires = now() + timedelta(days=7)
    with db() as conn, conn.cursor() as cur:
        cur.execute("""INSERT INTO secure_payments.payment_requests
          (id,tenant_id,recipient_type,recipient_id,recipient_name,recipient_email,requester_email,token_hash,expires_at)
          VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
          (request_id, tenant, payload.recipient_type, payload.recipient_id, payload.recipient_name,
           str(payload.recipient_email).lower(), str(payload.requester_email).lower() if payload.requester_email else None,
           digest(token), expires))
        cur.execute("""INSERT INTO secure_payments.payment_profiles(id,tenant_id,recipient_type,recipient_id,status)
                       VALUES(%s,%s,%s,%s,'PENDING')
                       ON CONFLICT (tenant_id,recipient_type,recipient_id) DO NOTHING""",
                    (uuid.uuid4(), tenant, payload.recipient_type, payload.recipient_id))
        audit(cur, tenant, "BANKING_REQUEST_CREATED", request, request_id=request_id,
              metadata={"recipient_type": payload.recipient_type, "recipient_id": payload.recipient_id})
        conn.commit()
    url = f"{PUBLIC_APP_URL}/banking#request={token}"
    send_email(tenant, str(payload.recipient_email), "Secure banking information request",
               f"Please submit your payment information securely through InkSuite.\n\n{url}\n\nThis link expires in 7 days. Do not send banking information by email.")
    return {"ok": True, "expires_at": expires}


def load_public_request(cur, token: str):
    cur.execute("""SELECT id,tenant_id,recipient_type,recipient_id,recipient_name,recipient_email,expires_at,used_at,revoked_at,requester_email
                   FROM secure_payments.payment_requests WHERE token_hash=%s""", (digest(token),))
    row = cur.fetchone()
    if not row or row[7] or row[8] or row[6] <= now():
        raise HTTPException(404, "This banking request is invalid or expired")
    return row


@router.get("/public/request")
def public_request(x_banking_request_token: str = Header(..., alias="X-Banking-Request-Token")):
    token = x_banking_request_token
    with db() as conn, conn.cursor() as cur:
        row = load_public_request(cur, token)
    return {"recipient_name": row[4], "expires_at": row[6], "requires_email_verification": True}


@router.post("/public/otp")
def send_otp(x_banking_request_token: str = Header(..., alias="X-Banking-Request-Token")):
    token = x_banking_request_token
    code = f"{secrets.randbelow(1_000_000):06d}"
    with db() as conn, conn.cursor() as cur:
        row = load_public_request(cur, token)
        cur.execute("UPDATE secure_payments.payment_requests SET otp_hash=%s,otp_expires_at=%s,otp_attempts=0 WHERE id=%s",
                    (digest(f"{token}:{code}"), now()+timedelta(minutes=10), row[0]))
        conn.commit()
    send_email(row[1], row[5], "InkSuite verification code", f"Your InkSuite banking verification code is {code}. It expires in 10 minutes.")
    return {"ok": True}


@router.post("/public/verify")
def verify_otp(payload: OtpVerify, x_banking_request_token: str = Header(..., alias="X-Banking-Request-Token")):
    token = x_banking_request_token
    submit = secrets.token_urlsafe(32)
    with db() as conn, conn.cursor() as cur:
        row = load_public_request(cur, token)
        cur.execute("SELECT otp_hash,otp_expires_at,otp_attempts FROM secure_payments.payment_requests WHERE id=%s FOR UPDATE", (row[0],))
        otp_hash, otp_expires, attempts = cur.fetchone()
        if attempts >= 5:
            raise HTTPException(429, "Too many verification attempts")
        cur.execute("UPDATE secure_payments.payment_requests SET otp_attempts=otp_attempts+1 WHERE id=%s", (row[0],))
        if not otp_hash or not otp_expires or otp_expires <= now() or not hmac.compare_digest(otp_hash, digest(f"{token}:{payload.code}")):
            conn.commit(); raise HTTPException(400, "Invalid or expired verification code")
        cur.execute("""UPDATE secure_payments.payment_requests
                       SET submit_token_hash=%s,submit_token_expires_at=%s,otp_hash=NULL,otp_expires_at=NULL
                       WHERE id=%s""", (digest(submit), now()+timedelta(minutes=20), row[0]))
        conn.commit()
    return {"submit_token": submit, "expires_in": 1200}


@router.put("/public/bank-account")
def submit_bank_account(payload: BankSubmission,
                        x_banking_request_token: str = Header(..., alias="X-Banking-Request-Token"),
                        x_banking_submit_token: str = Header(..., alias="X-Banking-Submit-Token")):
    token = x_banking_request_token
    if payload.payment_method == "ACH" and (not payload.routing_number or not payload.account_number):
        raise HTTPException(422, "ACH requires routing number and account number")
    if payload.payment_method == "INTERNATIONAL_WIRE" and not (payload.iban or payload.account_number):
        raise HTTPException(422, "International wire requires IBAN or account number")

    with db() as conn, conn.cursor() as cur:
        req = load_public_request(cur, token)
        cur.execute("SELECT submit_token_hash,submit_token_expires_at FROM secure_payments.payment_requests WHERE id=%s FOR UPDATE", (req[0],))
        sh, se = cur.fetchone()
        if not sh or not se or se <= now() or not hmac.compare_digest(sh, digest(x_banking_submit_token)):
            raise HTTPException(401, "Verification required")

        tenant, recipient_type, recipient_id = req[1], req[2], req[3]
        cur.execute("SELECT id FROM secure_payments.payment_profiles WHERE tenant_id=%s AND recipient_type=%s AND recipient_id=%s FOR UPDATE",
                    (tenant, recipient_type, recipient_id))
        found = cur.fetchone()
        profile_id = found[0] if found else uuid.uuid4()
        if not found:
            cur.execute("INSERT INTO secure_payments.payment_profiles(id,tenant_id,recipient_type,recipient_id,status,default_currency) VALUES(%s,%s,%s,%s,'PENDING',%s)",
                        (profile_id, tenant, recipient_type, recipient_id, payload.currency.upper()))

        cur.execute("SELECT id FROM secure_payments.payment_accounts WHERE payment_profile_id=%s AND is_active FOR UPDATE", (profile_id,))
        old = cur.fetchone()
        account_id = str(uuid.uuid4())
        sensitive = {
            "legal_account_holder_name": payload.legal_account_holder_name,
            "routing_number": payload.routing_number,
            "account_number": payload.account_number,
            "bank_address": payload.bank_address,
            "swift_bic": payload.swift_bic,
            "iban": payload.iban,
        }
        ciphertext, encrypted_key, nonce = encrypt_payload(tenant, account_id, sensitive)
        raw_account = payload.iban or payload.account_number or ""
        last4 = raw_account[-4:] if raw_account else None
        # Existing accounts remain active until a finance user approves the replacement.
        # This prevents an email-account takeover from silently redirecting payments.
        activate_now = not bool(old)
        cur.execute("""INSERT INTO secure_payments.payment_accounts
          (id,payment_profile_id,payment_method,country_code,bank_name,account_last4,account_type,currency,
           verification_status,encrypted_payload,encrypted_data_key,nonce,is_active)
          VALUES(%s,%s,%s,%s,%s,%s,%s,%s,'UNVERIFIED',%s,%s,%s,%s)""",
          (account_id, profile_id, payload.payment_method, payload.country_code.upper(), payload.bank_name, last4,
           payload.account_type, payload.currency.upper(), ciphertext, encrypted_key, nonce, activate_now))
        new_status = "CHANGE_PENDING" if old else "COMPLETE"
        cur.execute("UPDATE secure_payments.payment_profiles SET status=%s,default_currency=%s,updated_at=now() WHERE id=%s",
                    (new_status, payload.currency.upper(), profile_id))
        cur.execute("UPDATE secure_payments.payment_requests SET used_at=now(),submit_token_hash=NULL,submit_token_expires_at=NULL WHERE id=%s", (req[0],))
        cur.execute("""INSERT INTO secure_payments.payment_audit_events
          (tenant_id,payment_profile_id,payment_account_id,request_id,action,metadata)
          VALUES(%s,%s,%s,%s,%s,%s::jsonb)""",
          (tenant, profile_id, account_id, req[0], "BANK_ACCOUNT_SUBMITTED", json.dumps({"replacement": bool(old), "last4": last4})))
        conn.commit()

    # Completion notifications contain no bank name, account number, routing number,
    # IBAN, or other payment credentials.
    requester_email = req[9] if len(req) > 9 else None
    if requester_email:
        try:
            recipient_label = req[4] or "the recipient"
            send_email(
                tenant,
                requester_email,
                "InkSuite banking information completed",
                f"{recipient_label} completed the secure banking information request in InkSuite.\n\n"
                "No banking details are included in this email. Sign in to InkSuite to view the payment profile status.",
            )
        except Exception:
            # The banking submission must not fail merely because a completion
            # notification could not be delivered. Delivery failures belong in
            # operational email monitoring, never in the public response.
            pass

    return {"ok": True, "status": new_status, "account_last4": last4}
