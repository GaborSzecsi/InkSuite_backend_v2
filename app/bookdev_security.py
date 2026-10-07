"""Durable, request-scoped recipient verification. No application account is granted."""
from __future__ import annotations

import hashlib
import hmac
import os
import re
import secrets
from datetime import datetime, timedelta, timezone
from fastapi import HTTPException, Request
from psycopg.rows import dict_row
from app.core.db import db_conn

CODE_LIFETIME = timedelta(minutes=10)
SESSION_LIFETIME = timedelta(hours=1)
MAX_ATTEMPTS = 5
MAX_SENDS_PER_HOUR = 5
TOKEN_PATTERN = re.compile(r"^[A-Za-z0-9_-]{32,128}$")
PUBLIC_SUFFIXES = {"verification-code", "verify", "contributor-info", "media-questionnaire", "marketing-profile", "sales-information", "photo", "photo-upload"}


def public_request_endpoint(path: str, method: str) -> bool:
    parts = path.rstrip("/").split("/")
    prefix = ["", "api", "project-management", "book-development", "requests"]
    return (parts[:5] == prefix and len(parts) in (6, 7) and bool(TOKEN_PATTERN.fullmatch(parts[5])) and
            ((len(parts) == 6 and method == "GET") or (len(parts) == 7 and method == "POST" and parts[6] in PUBLIC_SUFFIXES)))


def now():
    return datetime.now(timezone.utc)


def hash_session(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


def hash_code(request_id: str, code: str) -> str:
    key = os.environ.get("BOOKDEV_VERIFICATION_SECRET", "")
    if len(key.encode()) < 32:
        raise HTTPException(503, "Recipient verification is not configured. Please contact the publisher.")
    return hmac.new(key.encode(), f"{request_id}:{code}".encode(), hashlib.sha256).hexdigest()


def ensure_pending(row):
    if row["status"] in {"completed", "revoked", "expired"} or row["expires_at"] <= now():
        raise HTTPException(410, "This request has expired or has already been submitted. Please request a new link.")


def verification_valid(record, session: str) -> bool:
    return bool(record and session and record.get("session_hash") and record.get("session_expires_at") and
                record["session_expires_at"] > now() and
                hmac.compare_digest(record["session_hash"], hash_session(session)))


def is_verified(row, request: Request) -> bool:
    session = request.headers.get("x-bookdev-session", "")
    if not session or len(session) > 128:
        return False
    with db_conn() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute("SELECT session_hash, session_expires_at FROM bookdev_request_verification WHERE request_id = %s::uuid", (row["id"],))
            return verification_valid(cur.fetchone(), session)


def require_verified(row, request: Request):
    ensure_pending(row)
    if not is_verified(row, request):
        raise HTTPException(403, "Verify the code sent to the original recipient's email before continuing.")


def lock_request(cur, row, request: Request, *, verified=True):
    """Hold this lock through data writes and completion, preventing parallel reuse."""
    cur.execute("SELECT id, status, expires_at FROM bookdev_requests WHERE id = %s::uuid FOR UPDATE", (row["id"],))
    current = cur.fetchone()
    if not current:
        raise HTTPException(404, "Request not found")
    ensure_pending(current)
    if verified:
        cur.execute("SELECT session_hash, session_expires_at FROM bookdev_request_verification WHERE request_id = %s::uuid", (row["id"],))
        if not verification_valid(cur.fetchone(), request.headers.get("x-bookdev-session", "")):
            raise HTTPException(403, "Email verification has expired. Please verify again.")


def send_code(row, request: Request, send_email):
    ensure_pending(row)
    code = f"{secrets.randbelow(100_000_000):08d}"
    digest = hash_code(row["id"], code)
    timestamp = now()
    with db_conn() as conn:
        with conn.transaction():
            with conn.cursor(row_factory=dict_row) as cur:
                lock_request(cur, row, request, verified=False)
                cur.execute("INSERT INTO bookdev_request_verification (request_id) VALUES (%s::uuid) ON CONFLICT DO NOTHING", (row["id"],))
                cur.execute("SELECT * FROM bookdev_request_verification WHERE request_id = %s::uuid FOR UPDATE", (row["id"],))
                state = cur.fetchone()
                if state["last_sent_at"] and timestamp < state["last_sent_at"] + timedelta(seconds=60):
                    raise HTTPException(429, "Please wait 60 seconds before requesting another code.")
                new_window = timestamp >= state["window_started_at"] + timedelta(hours=1)
                sends = 0 if new_window else state["sends_in_window"]
                if sends >= MAX_SENDS_PER_HOUR:
                    raise HTTPException(429, "Too many verification emails. Please try again in an hour.")
                # Never accept a recipient address from the public client.
                send_email(row, code)
                cur.execute("""UPDATE bookdev_request_verification SET code_hash = %s, code_expires_at = %s,
                    failed_attempts = 0, last_sent_at = %s, window_started_at = %s, sends_in_window = %s,
                    session_hash = NULL, session_expires_at = NULL WHERE request_id = %s::uuid""",
                    (digest, timestamp + CODE_LIFETIME, timestamp, timestamp if new_window else state["window_started_at"], sends + 1, row["id"]))
    return {"ok": True, "message": "A code has been sent to the original recipient's email.", "retry_after": 60}


def verify_code(row, request: Request, code: str):
    ensure_pending(row)
    if not re.fullmatch(r"[0-9]{8}", code):
        raise HTTPException(400, "Enter the eight-digit code from your email.")
    digest = hash_code(row["id"], code)
    failure = None
    session = secrets.token_urlsafe(32)
    expires = min(now() + SESSION_LIFETIME, row["expires_at"])
    with db_conn() as conn:
        with conn.transaction():
            with conn.cursor(row_factory=dict_row) as cur:
                lock_request(cur, row, request, verified=False)
                cur.execute("SELECT * FROM bookdev_request_verification WHERE request_id = %s::uuid FOR UPDATE", (row["id"],))
                state = cur.fetchone()
                if not state or not state["code_hash"] or state["code_expires_at"] <= now():
                    failure = HTTPException(400, "This code has expired. Request a new code.")
                elif state["failed_attempts"] >= MAX_ATTEMPTS:
                    failure = HTTPException(429, "Too many incorrect codes. Request a new code.")
                elif not hmac.compare_digest(state["code_hash"], digest):
                    cur.execute("UPDATE bookdev_request_verification SET failed_attempts = failed_attempts + 1 WHERE request_id = %s::uuid", (row["id"],))
                    failure = HTTPException(400, "Incorrect code. Please try again.")
                else:
                    cur.execute("""UPDATE bookdev_request_verification SET code_hash = NULL, code_expires_at = NULL,
                        session_hash = %s, session_expires_at = %s, verified_at = %s WHERE request_id = %s::uuid""",
                        (hash_session(session), expires, now(), row["id"]))
    # Raise after committing so failed-attempt counters cannot be rolled back.
    if failure:
        raise failure
    return {"ok": True, "session_token": session, "session_expires_at": expires.isoformat()}


def bound_contributor(cur, row, *, selected_party_id="") -> str:
    payload = row.get("payload_json") or {}
    party_id = selected_party_id or payload.get("contributor_party_id")
    if not party_id:
        raise HTTPException(409, "This request has no saved contributor identity. Ask the publisher for a new link.")
    cur.execute("""SELECT party_id::text AS party_id FROM work_contributors
        WHERE tenant_id = %s::uuid AND work_id = %s::uuid AND party_id = %s::uuid LIMIT 1""",
        (row["tenant_id"], row["work_id"], party_id))
    if not cur.fetchone():
        raise HTTPException(409, "The contributor is no longer assigned to this book.")
    return str(party_id)
