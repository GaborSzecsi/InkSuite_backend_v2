# app/wopi/tokens.py
# HMAC-signed WOPI access token. Collabora sends this; we verify and extract file_id/user_id.
from __future__ import annotations

import base64
import hashlib
import hmac
import json
import time
from typing import Any, Dict

from fastapi import HTTPException


def make_wopi_token(file_id: str, user_id: str, ttl: int, secret: str) -> str:
    payload = {
        "file_id": file_id,
        "user_id": user_id,
        "exp": int(time.time()) + ttl,
    }
    raw = json.dumps(payload, separators=(",", ":"), sort_keys=True)
    sig = hmac.new(secret.encode(), raw.encode(), hashlib.sha256).digest()
    return base64.urlsafe_b64encode(raw.encode() + b"." + sig).decode()


def verify_wopi_token(token: str, secret: str) -> Dict[str, Any]:
    if not token or not token.strip():
        raise HTTPException(status_code=401, detail="Invalid WOPI token")
    try:
        decoded = base64.urlsafe_b64decode(token.encode())
    except Exception:
        raise HTTPException(status_code=401, detail="Invalid WOPI token")
    if b"." not in decoded:
        raise HTTPException(status_code=401, detail="Invalid WOPI token")
    raw, sig = decoded.rsplit(b".", 1)
    expected = hmac.new(secret.encode(), raw, hashlib.sha256).digest()
    if not hmac.compare_digest(sig, expected):
        raise HTTPException(status_code=401, detail="Invalid WOPI token")
    try:
        payload = json.loads(raw.decode())
    except Exception:
        raise HTTPException(status_code=401, detail="Invalid WOPI token")
    if payload.get("exp", 0) < time.time():
        raise HTTPException(status_code=401, detail="Expired WOPI token")
    return payload
