# app/wopi/tokens.py
from __future__ import annotations

import base64
import hashlib
import hmac
import json
import time
from typing import Any, Dict

from fastapi import HTTPException


def _b64url_encode(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).decode("ascii").rstrip("=")


def _b64url_decode(data: str) -> bytes:
    s = data.strip()
    pad = "=" * (-len(s) % 4)
    return base64.urlsafe_b64decode((s + pad).encode("ascii"))


def make_wopi_token(file_id: str, user_id: str, ttl: int, secret: str) -> str:
    payload = {
        "file_id": str(file_id),
        "user_id": str(user_id),
        "exp": int(time.time()) + int(ttl),
    }

    raw = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")
    sig = hmac.new(secret.encode("utf-8"), raw, hashlib.sha256).digest()

    # Safe format: base64url(payload) + "." + base64url(signature)
    return f"{_b64url_encode(raw)}.{_b64url_encode(sig)}"


def verify_wopi_token(token: str, secret: str) -> Dict[str, Any]:
    if not token or not token.strip():
        raise HTTPException(status_code=401, detail="Invalid WOPI token")

    token = token.strip()

    # New safe format: "<payload_b64>.<sig_b64>"
    if "." in token:
        try:
            raw_b64, sig_b64 = token.split(".", 1)
            raw = _b64url_decode(raw_b64)
            sig = _b64url_decode(sig_b64)

            expected = hmac.new(secret.encode("utf-8"), raw, hashlib.sha256).digest()
            if not hmac.compare_digest(sig, expected):
                raise HTTPException(status_code=401, detail="Invalid WOPI token")

            payload = json.loads(raw.decode("utf-8"))
            if payload.get("exp", 0) < time.time():
                raise HTTPException(status_code=401, detail="Expired WOPI token")
            return payload
        except HTTPException:
            raise
        except Exception:
            raise HTTPException(status_code=401, detail="Invalid WOPI token")

    # Backward-compat fallback for old tokens:
    # base64url(raw + b"." + sig)
    try:
        decoded = _b64url_decode(token)
    except Exception:
        raise HTTPException(status_code=401, detail="Invalid WOPI token")

    if b"." not in decoded:
        raise HTTPException(status_code=401, detail="Invalid WOPI token")

    raw, sig = decoded.rsplit(b".", 1)
    expected = hmac.new(secret.encode("utf-8"), raw, hashlib.sha256).digest()
    if not hmac.compare_digest(sig, expected):
        raise HTTPException(status_code=401, detail="Invalid WOPI token")

    try:
        payload = json.loads(raw.decode("utf-8"))
    except Exception:
        raise HTTPException(status_code=401, detail="Invalid WOPI token")

    if payload.get("exp", 0) < time.time():
        raise HTTPException(status_code=401, detail="Expired WOPI token")

    return payload