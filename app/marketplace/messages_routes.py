from .observability import TimedRoute

"""HTTP adapters; service functions also support non-HTTP callers."""
from .messages_service import *
from . import messages_service as service

router = APIRouter(route_class=TimedRoute)


@router.post("/conversations")
def start(body: Relationship, user=Depends(current_user)):
    return service.start(body, user)


@router.get("/conversations")
def inbox(
    actor_id: UUID, offset: int = Query(0, ge=0, le=10000), user=Depends(current_user)
):
    return service.inbox(actor_id, offset, user)


@router.get("/unread")
def unread(actor_id: UUID, user=Depends(current_user)):
    return service.unread(actor_id, user)


@router.get("/conversations/{conversation_id}/messages")
def history(
    conversation_id: UUID,
    actor_id: UUID,
    before: UUID | None = None,
    user=Depends(current_user),
):
    return service.history(conversation_id, actor_id, before, user)


@router.post("/conversations/{conversation_id}/messages")
def send(conversation_id: UUID, body: Message, user=Depends(current_user)):
    return service.send(conversation_id, body, user)


@router.put("/conversations/{conversation_id}/read/{message_id}")
def mark_read(
    conversation_id: UUID, message_id: UUID, actor_id: UUID, user=Depends(current_user)
):
    return service.mark_read(conversation_id, message_id, actor_id, user)


@router.delete("/messages/{message_id}")
def delete_message(message_id: UUID, actor_id: UUID, user=Depends(current_user)):
    return service.delete_message(message_id, actor_id, user)
