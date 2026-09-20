from .observability import TimedRoute

"""HTTP adapters; service functions also support non-HTTP callers."""
from .social_service import *
from . import social_service as service

router = APIRouter(route_class=TimedRoute)


@router.get("/feed")
def feed(
    actor_id: UUID | None = None,
    author_id: UUID | None = None,
    scope: str = "discover",
    offset: int = Query(0, ge=0, le=10000),
    claims=Depends(require_session),
):
    return service.feed(actor_id, author_id, scope, offset, claims)


@router.get("/media")
def media(
    actor_id: UUID, offset: int = Query(0, ge=0, le=10000), user=Depends(current_user)
):
    return service.media(actor_id, offset, user)


@router.post("/posts")
def create_post(body: Post, user=Depends(current_user)):
    return service.create_post(body, user)


@router.delete("/posts/{post_id}")
def delete_post(post_id: UUID, actor_id: UUID, user=Depends(current_user)):
    return service.delete_post(post_id, actor_id, user)


@router.get("/posts/{post_id}/comments")
def comments(
    post_id: UUID,
    actor_id: UUID | None = None,
    parent_comment_id: UUID | None = None,
    offset: int = Query(0, ge=0, le=10000),
    claims=Depends(require_session),
):
    return service.comments(post_id, actor_id, parent_comment_id, offset, claims)


@router.post("/posts/{post_id}/comments")
def create_comment(post_id: UUID, body: Comment, user=Depends(current_user)):
    return service.create_comment(post_id, body, user)


@router.put("/comments/{comment_id}/like")
def like_comment(comment_id: UUID, actor_id: UUID, user=Depends(current_user)):
    return service.like_comment(comment_id, actor_id, user)


@router.delete("/comments/{comment_id}/like")
def unlike_comment(comment_id: UUID, actor_id: UUID, user=Depends(current_user)):
    return service.unlike_comment(comment_id, actor_id, user)


@router.delete("/comments/{comment_id}")
def delete_comment(comment_id: UUID, actor_id: UUID, user=Depends(current_user)):
    return service.delete_comment(comment_id, actor_id, user)


@router.put("/posts/{post_id}/like")
def like(post_id: UUID, actor_id: UUID, user=Depends(current_user)):
    return service.like(post_id, actor_id, user)


@router.delete("/posts/{post_id}/like")
def unlike(post_id: UUID, actor_id: UUID, user=Depends(current_user)):
    return service.unlike(post_id, actor_id, user)


@router.get("/network")
def network(
    actor_id: UUID, offset: int = Query(0, ge=0, le=10000), user=Depends(current_user)
):
    return service.network(actor_id, offset, user)


@router.get("/relationship")
def relationship(actor_id: UUID, target_id: UUID, user=Depends(current_user)):
    return service.relationship(actor_id, target_id, user)


@router.put("/follow")
def follow(body: Relationship, user=Depends(current_user)):
    return service.follow(body, user)


@router.delete("/follow/{target_id}")
def unfollow(target_id: UUID, actor_id: UUID, user=Depends(current_user)):
    return service.unfollow(target_id, actor_id, user)


@router.post("/connections")
def connect(
    body: Relationship, background_tasks: BackgroundTasks, user=Depends(current_user)
):
    return service.connect(body, background_tasks, user)


@router.put("/connections/{connection_id}")
def connection_action(
    connection_id: UUID, body: ConnectionAction, user=Depends(current_user)
):
    return service.connection_action(connection_id, body, user)


@router.put("/block")
def block(body: Relationship, user=Depends(current_user)):
    return service.block(body, user)


@router.delete("/block/{target_id}")
def unblock(target_id: UUID, actor_id: UUID, user=Depends(current_user)):
    return service.unblock(target_id, actor_id, user)


@router.post("/reports")
def report(body: Report, user=Depends(current_user)):
    return service.report(body, user)
