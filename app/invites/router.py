# app/invites/router.py — public endpoints for invite flow
from __future__ import annotations

from fastapi import APIRouter, HTTPException, status
from pydantic import BaseModel, EmailStr

from app.auth.service import get_invite_by_token, register_and_accept_invite

router = APIRouter(prefix="/invites", tags=["Invites"])


@router.get("/by-token")
def invite_by_token(token: str):
    """
    Public: get invite details by token for the accept-invite page.
    Returns tenant_name, email, role, expires_at.
    """
    invite = get_invite_by_token(token)
    if not invite:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Invalid or expired invite")
    return invite


class AcceptInviteBody(BaseModel):
    token: str
    email: EmailStr
    password: str
    full_name: str
    phone: str = ""
    username: str | None = None


@router.post("/accept")
def accept_invite_register(body: AcceptInviteBody):
    """
    Public: accept invite by registering (Cognito sign-up + user + membership).
    On success returns tokens for immediate login or message to sign in.
    """
    try:
        result = register_and_accept_invite(
            token=body.token,
            email=str(body.email),
            password=body.password,
            full_name=body.full_name,
            phone=body.phone or "",
            username=body.username,
        )
    except ValueError as e:
        msg = str(e)
        if msg == "invalid_token":
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Invalid or expired invite")
        if msg == "already_accepted":
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Invite already accepted")
        if msg == "expired":
            raise HTTPException(status_code=status.HTTP_410_GONE, detail="Invite expired")
        if msg == "email_mismatch":
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Email does not match invite")
        if msg == "username_exists":
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="An account with this email already exists")
        if msg == "invalid_password":
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Password does not meet requirements")
        if msg == "invalid_request":
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Token, email and password required")
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=msg)
    except RuntimeError as e:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=str(e))
    return result
