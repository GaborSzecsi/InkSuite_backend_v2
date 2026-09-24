from .observability import TimedRoute

"""HTTP adapters; service functions also support non-HTTP callers."""
from .identity_service import *
from . import identity_service as service

router = APIRouter(route_class=TimedRoute)


@router.post("/auth/resend")
def resend(body: EmailBody, request: Request):
    return service.resend(body, request)


@router.post("/auth/login")
def login(body: Credentials, request: Request):
    return service.login(body, request)


@router.post("/auth/register")
def register(body: Credentials, request: Request):
    return service.register(body, request)


@router.post("/auth/confirm")
def confirm(body: Confirmation, request: Request):
    return service.confirm(body, request)


@router.get("/me")
def me(claims=Depends(require_session)):
    return service.me(claims)


@router.put("/profile")
def save_profile(body: Profile, user=Depends(current_user)):
    return service.save_profile(body, user)


@router.get("/users/{username}")
def profile(username: str):
    return service.profile(username)


class EmailChange(BaseModel):
    email: EmailStr


class EmailVerification(BaseModel):
    code: str = Field(min_length=1, max_length=32)


def account_token(request):
    value = request.headers.get("authorization", "")
    if not value.lower().startswith("bearer "):
        raise HTTPException(401, "Please sign in again.")
    return value.split(" ", 1)[1]


def email_error(exc):
    code = exc.response.get("Error", {}).get("Code", "")
    if code in ("CodeMismatchException", "ExpiredCodeException"):
        return HTTPException(422, "The code is invalid or expired. Request a new code and try again.")
    if code == "AliasExistsException":
        return HTTPException(409, "This email cannot be used. Choose another address.")
    if code in ("TooManyRequestsException", "LimitExceededException"):
        return HTTPException(429, "Too many attempts. Please try again later.")
    return HTTPException(409, "Email changes are unavailable. Please ask your administrator to check Cognito email verification and app permissions.")


@router.post("/profile/email")
def change_email(body: EmailChange, request: Request, user=Depends(current_user)):
    client = auth._cognito()
    try:
        pool = client.describe_user_pool(UserPoolId=settings.cognito_user_pool_id)["UserPool"]
        required = pool.get("UserAttributeUpdateSettings", {}).get("AttributesRequireVerificationBeforeUpdate", [])
        if "email" not in required:
            raise HTTPException(409, "An administrator must enable verification before email updates in Cognito. Your current sign-in email has not changed.")
        client.update_user_attributes(AccessToken=account_token(request), UserAttributes=[{"Name":"email", "Value":str(body.email)}])
    except ClientError as exc:
        raise email_error(exc) from None
    return {"ok": True}


@router.post("/profile/email/verify")
def verify_email(body: EmailVerification, request: Request, user=Depends(current_user)):
    client = auth._cognito()
    token = account_token(request)
    try:
        client.verify_user_attribute(AccessToken=token, AttributeName="email", Code=body.code)
        attributes = {a["Name"]:a["Value"] for a in client.get_user(AccessToken=token)["UserAttributes"]}
    except ClientError as exc:
        raise email_error(exc) from None
    if attributes.get("email_verified") != "true" or not attributes.get("email"):
        raise HTTPException(409, "Email verification is not complete.")
    email = attributes["email"]
    with service.transaction() as cur:
        service._repository.save_verified_email(cur, user["id"], email)
    return {"ok": True, "email": email}


@router.get("/profile/username")
def suggest_username(name: str, user=Depends(current_user)):
    from .usernames import name_username, available_username
    if len(name)>100:
        raise HTTPException(422, "Use a name of up to 100 characters.")
    with service.transaction() as cur:
        return {"username":available_username(cur,name_username(name),user["id"])}
