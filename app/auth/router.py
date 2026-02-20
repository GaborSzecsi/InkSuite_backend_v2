# app/auth/router.py
from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Response, status
from pydantic import BaseModel

from app.auth.dependencies import get_current_user
from app.auth.service import validate_login, get_me_payload_from_claims

# IMPORTANT:
# This router is designed to be mounted with prefix="/api" in main.py
# so the final URLs become:
#   POST /api/auth/login
#   POST /api/auth/logout
#   GET  /api/auth/me
router = APIRouter(prefix="/auth", tags=["auth"])


class LoginBody(BaseModel):
    email: str
    password: str
    tenant_slug: str | None = None


class LoginResponse(BaseModel):
    access_token: str
    refresh_token: str | None = None
    token_type: str = "Bearer"
    user: dict


@router.post("/login", response_model=LoginResponse)
async def login(body: LoginBody):
    try:
        result = validate_login(body.email, body.password, body.tenant_slug)
    except ValueError as e:
        # e.g. NEW_PASSWORD_REQUIRED, MFA setup, etc.
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(e))
    except RuntimeError as e:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=str(e))

    if not result:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid email or password")

    return LoginResponse(
        access_token=result["access_token"],
        refresh_token=result.get("refresh_token"),
        token_type=result.get("token_type", "Bearer"),
        user=result.get("user", {}),
    )


@router.post("/logout")
async def logout(response: Response):
    """
    Backend logout notes:
    - If your frontend stores tokens in localStorage/sessionStorage, YOU MUST clear them client-side.
    - If you store refresh_token in an HttpOnly cookie, uncomment the delete_cookie line below.
    """
    # If you use a refresh_token cookie, enable this:
    # response.delete_cookie(key="refresh_token", path="/", secure=True, httponly=True, samesite="none")
    return {"ok": True}


@router.get("/me")
async def me(user: dict = Depends(get_current_user)):
    return get_me_payload_from_claims(user)
