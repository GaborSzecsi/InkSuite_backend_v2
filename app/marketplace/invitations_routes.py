from .observability import TimedRoute

"""HTTP adapters; service functions also support non-HTTP callers."""
from .invitations_service import *
from . import invitations_service as service

router = APIRouter(route_class=TimedRoute)


@router.post("/invitations/request")
def request_access(body: AccessRequest, request: Request):
    return service.request_access(body, request)


@router.get("/invitations/review")
def review_queue(user=Depends(approver)):
    return service.review_queue(user)


@router.get("/invitations/review/{request_id}")
def review(request_id: UUID, user=Depends(approver)):
    return service.review(request_id, user)


@router.post("/invitations/review/{request_id}")
def decide(request_id: UUID, body: Decision, user=Depends(approver)):
    return service.decide(request_id, body, user)


@router.post("/invitations/inspect")
def inspect(body: InviteToken, request: Request):
    return service.inspect(body, request)


@router.post("/invitations/accept-existing")
def accept_existing(body: InviteToken, request: Request, user=Depends(current_user)):
    return service.accept_existing(body, request, user)


@router.post("/invitations/accept")
def accept(body: Acceptance, request: Request):
    return service.accept(body, request)


@router.post("/invitations/approval/inspect")
def inspect_approval(body: InviteToken, request: Request):
    return service.inspect_approval(body, request)


@router.post("/invitations/approval/decide")
def decide_approval(body: ApprovalDecision, request: Request):
    return service.decide_approval(body, request)


@router.post("/invitations/review/{request_id}/email-link")
def resend_approval_link(request_id: UUID, request: Request):
    # Old email links can request replacement ONLY to the configured approver.
    return service.resend_approval_link(request_id, request)
