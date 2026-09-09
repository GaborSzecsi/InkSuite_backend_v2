"""Public Cognito password recovery; passwords and codes are never logged."""
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, EmailStr, SecretStr
from botocore.exceptions import ClientError, BotoCoreError
from app.auth.service import _cognito, _secret_hash
from app.core.config import settings

router = APIRouter()

class RecoveryRequest(BaseModel):
    email: EmailStr

class ConfirmRecoveryRequest(RecoveryRequest):
    code: SecretStr
    password: SecretStr


def _recover(body: RecoveryRequest, confirm: bool):
    if not settings.cognito_client_id:
        raise HTTPException(503, "Password recovery is temporarily unavailable.")
    username = str(body.email).strip()
    params = {"ClientId": settings.cognito_client_id, "Username": username}
    secret = _secret_hash(username)
    if secret:
        params["SecretHash"] = secret
    if confirm:
        code = body.code.get_secret_value().strip()
        password = body.password.get_secret_value()
        if not code or not password or len(code) > 2048 or len(password) > 256:
            raise HTTPException(400, "Enter the reset code and a new password of at most 256 characters.")
        params.update(ConfirmationCode=code, Password=password)
    try:
        client = _cognito()
        if confirm:
            client.confirm_forgot_password(**params)
        else:
            client.forgot_password(**params)
    except ClientError as exc:
        error = exc.response.get("Error", {}).get("Code", "")
        if not confirm and error in {"UserNotFoundException", "InvalidParameterException", "NotAuthorizedException"}:
            return {"ok": True}
        if error in {"TooManyRequestsException", "LimitExceededException", "TooManyFailedAttemptsException"}:
            raise HTTPException(429, "Too many attempts. Please wait before trying again.") from None
        messages = {
            "CodeMismatchException": "The reset code is incorrect. Use the latest code you received.",
            "ExpiredCodeException": "The reset code has expired. Request a new code.",
            "InvalidPasswordException": "The password does not meet the account password requirements. Try a longer password with uppercase, lowercase, numbers, and symbols.",
            "PasswordHistoryPolicyViolationException": "Choose a password you have not used before.",
            "UserNotFoundException": "Unable to reset the password. Check your email and code.",
            "NotAuthorizedException": "Unable to reset the password. Request a new code.",
            "InvalidParameterException": "Unable to reset the password. Check your entries or request a new code.",
        }
        if error in messages:
            raise HTTPException(400, messages[error]) from None
        raise HTTPException(503, "Password recovery is temporarily unavailable. Please try again later.") from None
    except BotoCoreError:
        raise HTTPException(503, "Password recovery is temporarily unavailable. Please try again later.") from None
    return {"ok": True}

@router.post("/forgot-password")
def forgot_password(body: RecoveryRequest):
    return _recover(body, False)

@router.post("/confirm-forgot-password")
def confirm_forgot_password(body: ConfirmRecoveryRequest):
    return _recover(body, True)
