# require_user(), require_platform_admin() — deny by default with 401.
from __future__ import annotations

from typing import Annotated

from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer, OAuth2PasswordBearer

from app.auth.service import get_current_user_from_token

# Prefer Bearer token (frontend sends after login).
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/auth/login", auto_error=False)
http_bearer = HTTPBearer(auto_error=False)


def _token_from_header(credentials: HTTPAuthorizationCredentials | None, token: str | None) -> str | None:
    if credentials and credentials.credentials:
        return credentials.credentials
    return token if token else None


async def get_current_user(
    credentials: Annotated[HTTPAuthorizationCredentials | None, Depends(http_bearer)],
    token: Annotated[str | None, Depends(oauth2_scheme)],
) -> dict:
    """Dependency: require valid auth; else 401."""
    t = _token_from_header(credentials, token)
    user = get_current_user_from_token(t) if t else None
    if not user:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Not authenticated")
    return user


def require_platform_admin(user: dict) -> None:
    """Raise 403 if user is not in inksuite_master_admin group."""
    groups = user.get("cognito:groups") or []
    if "inksuite_master_admin" not in groups:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Platform admin required")
