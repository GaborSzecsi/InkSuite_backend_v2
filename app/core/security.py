# Password hashing and token signing (for session cookie when not using Cognito JWT only).
from __future__ import annotations

import hashlib
import hmac
import os
import secrets
from typing import Optional


def hash_password(plain: str) -> str:
    """Simple hash for dev; replace with bcrypt/argon2 for production."""
    salt = secrets.token_hex(16)
    h = hashlib.pbkdf2_hmac("sha256", plain.encode("utf-8"), salt.encode("utf-8"), 100000)
    return f"{salt}${h.hex()}"


def verify_password(plain: str, hashed: str) -> bool:
    if "$" not in hashed:
        return False
    salt, rest = hashed.split("$", 1)
    h = hashlib.pbkdf2_hmac("sha256", plain.encode("utf-8"), salt.encode("utf-8"), 100000)
    return hmac.compare_digest(h.hex(), rest)


def secure_random_token(bytes_size: int = 32) -> str:
    return secrets.token_urlsafe(bytes_size)
