# Set/clear HttpOnly cookies (for BFF or backend-set cookies).
from __future__ import annotations

from typing import Optional


def cookie_header(
    name: str,
    value: str,
    *,
    max_age: int = 60 * 60 * 24 * 7,
    path: str = "/",
    secure: bool = True,
    same_site: str = "Lax",
    http_only: bool = True,
) -> str:
    """Build Set-Cookie header value."""
    parts = [f"{name}={value}", f"Path={path}", f"Max-Age={max_age}", "HttpOnly" if http_only else "", f"SameSite={same_site}"]
    if secure:
        parts.append("Secure")
    return "; ".join(p for p in parts if p)


def clear_cookie_header(name: str, path: str = "/") -> str:
    """Build Set-Cookie to clear cookie."""
    return f"{name}=; Path={path}; Max-Age=0; HttpOnly"
