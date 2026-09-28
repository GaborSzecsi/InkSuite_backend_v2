"""Tenant-scoped, audited step-up access to existing encrypted payment accounts."""

import base64
import hashlib
import hmac
import io
import json
import secrets
import struct
import time
from datetime import timedelta
from urllib.parse import quote, urlencode
from uuid import UUID

from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import JSONResponse
from psycopg.rows import dict_row
from pydantic import BaseModel, Field

from app.auth.dependencies import get_current_user
from routers import banking

router = APIRouter(prefix="/financials/banking", tags=["Banking access"])


class Verification(BaseModel):
    code: str = Field(min_length=6, max_length=64)


def reply(data):
    return JSONResponse(
        data, headers={"Cache-Control": "private, no-store", "Pragma": "no-cache"}
    )


def authorized(
    tenant_slug: str = Query(..., min_length=1, max_length=100),
    claims=Depends(get_current_user),
):
    # Header/query tenant selection is never trusted without a DB membership check.
    with banking.db() as conn, conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """SELECT u.id AS user_id, u.email, t.id AS tenant_id, t.slug,
                       m.role, m.module_permissions
                       FROM public.users u JOIN public.memberships m ON m.user_id=u.id
                       JOIN public.tenants t ON t.id=m.tenant_id
                       WHERE u.cognito_sub=%s AND t.slug=%s""",
            (claims.get("sub"), tenant_slug),
        )
        row = cur.fetchone()
    if not row or not (
        row["role"] == "tenant_admin"
        or (row["module_permissions"] or {}).get("banking") is True
    ):
        raise HTTPException(
            403,
            "Banking access requires tenant administrator or explicit banking permission.",
        )
    return {**row, "claims": claims}


def ready(cur):
    cur.execute("SELECT to_regclass('secure_payments.banking_authenticators') AS ready")
    if not cur.fetchone()["ready"]:
        raise HTTPException(
            503,
            "Banking access setup is pending. Apply migration 014_banking_access.sql.",
        )


def context(user):
    return {
        "tenant_id": str(user["tenant_id"]),
        "user_id": str(user["user_id"]),
        "purpose": "banking-authenticator",
    }


def audit(cur, user, action, account_id=None):
    cur.execute(
        """INSERT INTO secure_payments.banking_access_audit
                   (tenant_id,user_id,account_id,action) VALUES(%s,%s,%s,%s)""",
        (user["tenant_id"], user["user_id"], account_id, action),
    )


def totp(secret, step):
    key = base64.b32decode(secret)
    mac = hmac.new(key, struct.pack(">Q", step), hashlib.sha1).digest()
    offset = mac[-1] & 15
    return str(
        (struct.unpack(">I", mac[offset : offset + 4])[0] & 0x7FFFFFFF) % 1000000
    ).zfill(6)


def matching_step(secret, code, last_step, timestamp=None):
    if len(code) != 6 or not code.isascii() or not code.isdigit():
        return None
    step = int(time.time() if timestamp is None else timestamp) // 30
    for candidate in (step, step - 1, step + 1):
        if (
            candidate >= 0
            and candidate > last_step
            and hmac.compare_digest(totp(secret, candidate), code)
        ):
            return candidate
    return None


def fresh_login(user):
    age = time.time() - int(user["claims"].get("auth_time") or 0)
    if age < -60 or age > 600:
        raise HTTPException(
            403,
            "For authenticator setup, sign out and sign in again, then return within 10 minutes.",
        )


def load_authenticator(cur, user):
    ready(cur)
    cur.execute(
        """SELECT * FROM secure_payments.banking_authenticators
                   WHERE tenant_id=%s AND user_id=%s FOR UPDATE""",
        (user["tenant_id"], user["user_id"]),
    )
    return cur.fetchone()


def verify(conn, cur, user, row, code, pending=False, recovery=False):
    if not row or (not pending and not row["confirmed_at"]):
        raise HTTPException(403, "Set up your authenticator first.")
    if pending and (row["confirmed_at"] or row["setup_expires_at"] <= banking.now()):
        raise HTTPException(400, "Authenticator setup expired or is already complete.")
    if row["locked_until"] and row["locked_until"] > banking.now():
        raise HTTPException(429, "Too many incorrect codes. Try again in 15 minutes.")
    secret = banking.kms.decrypt(
        CiphertextBlob=bytes(row["secret_ciphertext"]), EncryptionContext=context(user)
    )["Plaintext"].decode()
    step = matching_step(secret, code.strip(), row["last_step"])
    hashes = list(row["recovery_hashes"] or [])
    recovery_hash = banking.digest(
        f"banking-recovery:{user['tenant_id']}:{user['user_id']}:{code.strip().upper()}"
    )
    recovered = recovery and any(
        hmac.compare_digest(recovery_hash, value) for value in hashes
    )
    if step is None and not recovered:
        attempts = (0 if row["locked_until"] else row["failed_attempts"]) + 1
        cur.execute(
            """UPDATE secure_payments.banking_authenticators SET failed_attempts=%s,locked_until=%s
                       WHERE tenant_id=%s AND user_id=%s""",
            (
                attempts,
                banking.now() + timedelta(minutes=15) if attempts >= 5 else None,
                user["tenant_id"],
                user["user_id"],
            ),
        )
        audit(cur, user, "VERIFICATION_FAILED")
        conn.commit()  # Persist failed attempts even though the request returns an error.
        raise HTTPException(
            400,
            "Invalid or already-used code. Enter a new code from your authenticator.",
        )
    if recovered:
        hashes.remove(recovery_hash)
    cur.execute(
        """UPDATE secure_payments.banking_authenticators SET last_step=%s,
                   failed_attempts=0,locked_until=NULL,recovery_hashes=%s::jsonb
                   WHERE tenant_id=%s AND user_id=%s""",
        (
            step if step is not None else row["last_step"],
            json.dumps(hashes),
            user["tenant_id"],
            user["user_id"],
        ),
    )


def provision(cur, user):
    secret = base64.b32encode(secrets.token_bytes(20)).decode()
    encrypted = banking.kms.encrypt(
        KeyId=banking.KMS_KEY_ID,
        Plaintext=secret.encode(),
        EncryptionContext=context(user),
    )["CiphertextBlob"]
    cur.execute(
        """INSERT INTO secure_payments.banking_authenticators
                   (tenant_id,user_id,secret_ciphertext,setup_expires_at) VALUES(%s,%s,%s,%s)
                   ON CONFLICT (tenant_id,user_id) DO UPDATE SET
                   secret_ciphertext=EXCLUDED.secret_ciphertext,setup_expires_at=EXCLUDED.setup_expires_at,
                   confirmed_at=NULL,last_step=-1,recovery_hashes='[]'::jsonb""",
        (
            user["tenant_id"],
            user["user_id"],
            encrypted,
            banking.now() + timedelta(minutes=10),
        ),
    )
    import qrcode
    import qrcode.image.svg

    uri = (
        "otpauth://totp/"
        + quote(f"InkSuite:{user['email']} ({user['slug']})", safe="")
        + "?"
        + urlencode(
            {
                "secret": secret,
                "issuer": "InkSuite",
                "algorithm": "SHA1",
                "digits": 6,
                "period": 30,
            }
        )
    )
    image = qrcode.make(uri, image_factory=qrcode.image.svg.SvgPathImage)
    output = io.BytesIO()
    image.save(output)
    audit(cur, user, "AUTHENTICATOR_SETUP_STARTED")
    return {
        "secret": secret,
        "qr": "data:image/svg+xml;base64,"
        + base64.b64encode(output.getvalue()).decode(),
    }


@router.get("/accounts")
def accounts(user=Depends(authorized), offset: int = Query(0, ge=0)):
    with banking.db() as conn, conn.cursor(row_factory=dict_row) as cur:
        ready(cur)
        cur.execute(
            """SELECT a.id, a.bank_name,a.account_last4,a.payment_method,p.recipient_type,p.status,
                       COALESCE(r.recipient_name,r.recipient_email,p.recipient_id) AS name
                       FROM secure_payments.payment_profiles p
                       JOIN secure_payments.payment_accounts a ON a.payment_profile_id=p.id AND a.is_active
                       LEFT JOIN LATERAL (SELECT recipient_name,recipient_email FROM secure_payments.payment_requests
                         WHERE tenant_id=p.tenant_id AND recipient_type=p.recipient_type AND recipient_id=p.recipient_id
                         ORDER BY used_at DESC NULLS LAST,expires_at DESC LIMIT 1) r ON true
                       WHERE p.tenant_id=%s ORDER BY name,a.id LIMIT 51 OFFSET %s""",
            (user["slug"], offset),
        )
        rows = cur.fetchall()
        cur.execute(
            "SELECT confirmed_at IS NOT NULL AS enabled FROM secure_payments.banking_authenticators WHERE tenant_id=%s AND user_id=%s",
            (user["tenant_id"], user["user_id"]),
        )
        mfa = cur.fetchone()
    return reply(
        {
            "items": [{**row, "id": str(row["id"])} for row in rows[:50]],
            "has_more": len(rows) > 50,
            "authenticator_enabled": bool(mfa and mfa["enabled"]),
        }
    )


@router.post("/authenticator/setup")
def setup(user=Depends(authorized)):
    fresh_login(user)
    with banking.db() as conn, conn.cursor(row_factory=dict_row) as cur:
        # Serialize first-time setup too, before a row exists.
        cur.execute(
            "SELECT pg_advisory_xact_lock(hashtextextended(%s,0))",
            (str(user["tenant_id"]) + str(user["user_id"]),),
        )
        row = load_authenticator(cur, user)
        if row and row["confirmed_at"]:
            raise HTTPException(
                409,
                "An authenticator is already configured. Use recovery to replace it.",
            )
        if row and row["locked_until"] and row["locked_until"] > banking.now():
            raise HTTPException(429, "Too many incorrect codes. Try again later.")
        result = provision(cur, user)
    return reply(result)


@router.post("/authenticator/confirm")
def confirm(body: Verification, user=Depends(authorized)):
    with banking.db() as conn, conn.cursor(row_factory=dict_row) as cur:
        row = load_authenticator(cur, user)
        verify(conn, cur, user, row, body.code, pending=True)
        codes = [secrets.token_hex(8).upper() for _ in range(10)]
        hashes = [
            banking.digest(
                f"banking-recovery:{user['tenant_id']}:{user['user_id']}:{code}"
            )
            for code in codes
        ]
        cur.execute(
            """UPDATE secure_payments.banking_authenticators SET confirmed_at=now(),recovery_hashes=%s::jsonb
                       WHERE tenant_id=%s AND user_id=%s""",
            (json.dumps(hashes), user["tenant_id"], user["user_id"]),
        )
        audit(cur, user, "AUTHENTICATOR_CONFIRMED")
    return reply({"recovery_codes": codes})


@router.post("/authenticator/recover")
def recover(body: Verification, user=Depends(authorized)):
    fresh_login(user)
    with banking.db() as conn, conn.cursor(row_factory=dict_row) as cur:
        row = load_authenticator(cur, user)
        verify(conn, cur, user, row, body.code, recovery=True)
        audit(cur, user, "AUTHENTICATOR_REPLACED")
        result = provision(cur, user)
    return reply(result)


@router.post("/accounts/{account_id}/reveal")
def reveal(account_id: UUID, body: Verification, user=Depends(authorized)):
    with banking.db() as conn, conn.cursor(row_factory=dict_row) as cur:
        row = load_authenticator(cur, user)
        cur.execute(
            """SELECT a.encrypted_payload,a.encrypted_data_key,a.nonce
                       FROM secure_payments.payment_accounts a JOIN secure_payments.payment_profiles p ON p.id=a.payment_profile_id
                       WHERE a.id=%s AND p.tenant_id=%s AND a.is_active""",
            (account_id, user["slug"]),
        )
        account = cur.fetchone()
        if not account:
            raise HTTPException(404, "Banking record not found.")
        verify(conn, cur, user, row, body.code)
        # Match the encryption context and AES-GCM AAD used by the collection form.
        key = bytearray(
            banking.kms.decrypt(
                CiphertextBlob=bytes(account["encrypted_data_key"]),
                EncryptionContext={
                    "tenant_id": user["slug"],
                    "payment_account_id": str(account_id),
                    "purpose": "payment-account",
                },
            )["Plaintext"]
        )
        try:
            fields = json.loads(
                AESGCM(bytes(key)).decrypt(
                    bytes(account["nonce"]),
                    bytes(account["encrypted_payload"]),
                    banking.aad(user["slug"], str(account_id)),
                )
            )
        finally:
            for i in range(len(key)):
                key[i] = 0
        audit(cur, user, "BANKING_DETAILS_REVEALED", account_id)
    return reply({"fields": fields, "expires_in": 60})
