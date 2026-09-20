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
