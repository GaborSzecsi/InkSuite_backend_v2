"""Marketplace social service; transactions and mutation boundaries live here."""

from . import social_repository as _repository
from uuid import UUID
from fastapi import APIRouter, Depends, HTTPException, Query, BackgroundTasks
from app.auth.dependencies import require_session
from app.auth.service import get_user_db_record_from_claims
from .core import *
from .schemas import Post, Comment, Relationship, ConnectionAction, Report
from .feed_service import hydrate
from .catalog import book_view, BOOK_JOIN, PUBLIC_BOOK

router = APIRouter()


def post_access(cur, post_id, active=None):
    p = required(_repository.post_access_query_2(cur, post_id))
    author = _repository.post_access_query_1(cur, p)
    if author["status"] != "active" or author["org_status"] not in (None, "active"):
        raise HTTPException(404, "Post not found.")
    if active and blocked(cur, active, p["author_actor_id"]):
        raise HTTPException(404, "Post not found.")
    if (
        p["visibility"] != "public"
        and str(active) != str(p["author_actor_id"])
        and (not active or not connected(cur, active, p["author_actor_id"]))
    ):
        raise HTTPException(404, "Post not found.")
    return p


def content_author(cur, actor_id, viewer=None):
    # Content visibility is checked separately; attribution does not open a private profile.
    author = public_actor(cur, actor_id, owner=str(actor_id) == str(viewer))
    profile = _repository.content_author_query_1(cur, actor_id)
    if profile:
        author["name"] = profile["display_name"]
    return author


def comment_interactions_ready(cur, required=False):
    ready = bool(_repository.comment_interactions_ready_query_1(cur)["t"])
    if required and not ready:
        raise HTTPException(
            503,
            "Comment likes and replies need migration 009_marketplace_comment_interactions.sql.",
        )
    return ready


def comment_access(cur, comment_id, active):
    c = required(_repository.comment_access_query_1(cur, comment_id))
    post_access(cur, c["post_id"], active)
    if active and blocked(cur, active, c["author_actor_id"]):
        raise HTTPException(404, "Comment not found.")
    return c


def post_view(cur, p, active=None):
    from . import native_repository, native_storage

    native = [
        native_storage.describe(x)
        for x in native_repository.attachments(cur, [p["id"]])
    ]
    links = _repository.post_view_query_1(cur, PUBLIC_BOOK, BOOK_JOIN, p)
    return {
        "id": p["id"],
        "body": p["body"],
        "created_at": p["published_at"],
        "author": content_author(cur, p["author_actor_id"], active),
        "image": image_ref(p["media_asset_ref"]),
        "media": native,
        "books": [book_view(cur, b["id"]) for b in links],
        "likes": _repository.post_view_query_2(cur, p)["n"],
        "comments": _repository.post_view_query_3(cur, p)["n"],
        "liked": bool(active and _repository.post_view_query_4(cur, active, p)),
        "can_delete": str(active) == str(p["author_actor_id"]),
    }


def feed(
    actor_id: UUID | None = None,
    author_id: UUID | None = None,
    scope: str = "discover",
    offset: int = Query(0, ge=0, le=10000),
    claims=Depends(require_session),
):
    with transaction() as cur:
        if actor_id:
            if not claims:
                raise HTTPException(401, "Please sign in.")
            actor(cur, get_user_db_record_from_claims(claims), actor_id)
        from .feed_repository import page

        data = page(cur, actor_id, author_id, scope, offset)
        return {
            "items": hydrate(cur, data[:20], actor_id),
            "has_more": len(data) > 20,
        }


def media(
    actor_id: UUID, offset: int = Query(0, ge=0, le=10000), user=Depends(current_user)
):
    with transaction() as cur:
        a = actor(cur, user, actor_id)
        if not a["organization_id"]:
            return {"items": []}
        o = organization_permission(cur, user, a["organization_id"])
        data = _repository.media_query_1(cur, offset, o)
        return {
            "items": [
                {
                    "id": x["id"],
                    "filename": x["filename"],
                    "url": safe_url(x["public_url"]),
                }
                for x in data[:100]
                if safe_url(x["public_url"])
            ],
            "has_more": len(data) > 100,
        }


def create_post(body: Post, user=Depends(current_user)):
    with transaction() as cur:
        a = actor(cur, user, body.actor_id)
        rate_limit(cur, user["id"], "marketplace_posts", 6)
        if a["organization_id"]:
            org = organization_permission(cur, user, a["organization_id"])
            if org["status"] != "active":
                raise HTTPException(
                    409, "Publish your storefront before posting as the organization."
                )
        media = None
        if body.media_ids:
            from .native_service import ready

            ready(cur)
        if body.media_asset_id or body.media_key:
            from .media import validated_reference
            from psycopg.types.json import Jsonb

            media = Jsonb(
                validated_reference(cur, user, a, body.media_asset_id, body.media_key)
            )
        for book_id in body.book_ids:
            b = book_view(cur, book_id)
            if a["organization_id"] and str(b["organization_id"]) != str(
                a["organization_id"]
            ):
                raise HTTPException(403, "Select books from your own storefront.")
        p = _repository.create_post_query_1(cur, media, body, user)
        for i, b in enumerate(dict.fromkeys(body.book_ids)):
            _repository.create_post_query_2(cur, b, i, p)
        if body.media_ids:
            from .native_service import attach

            attach(cur, p["id"], body.media_ids, body.actor_id)
        return post_view(cur, p, body.actor_id)


def delete_post(post_id: UUID, actor_id: UUID, user=Depends(current_user)):
    with transaction() as cur:
        actor(cur, user, actor_id)
        required(_repository.delete_post_query_1(cur, post_id, actor_id))
        return {"ok": True}


def comments(
    post_id: UUID,
    actor_id: UUID | None = None,
    parent_comment_id: UUID | None = None,
    offset: int = Query(0, ge=0, le=10000),
    claims=Depends(require_session),
):
    with transaction() as cur:
        if actor_id:
            if not claims:
                raise HTTPException(401, "Please sign in.")
            actor(cur, get_user_db_record_from_claims(claims), actor_id)
        post_access(cur, post_id, actor_id)
        ready = comment_interactions_ready(cur, required=bool(parent_comment_id))
        where = "c.post_id=%s AND c.status='published'"
        params = [post_id]
        if ready:
            if parent_comment_id:
                parent = required(
                    _repository.comments_query_2(cur, parent_comment_id, post_id)
                )
                if actor_id and blocked(cur, actor_id, parent["author_actor_id"]):
                    raise HTTPException(404, "Comment not found.")
                where += " AND c.parent_comment_id=%s"
                params.append(parent_comment_id)
            else:
                where = """c.post_id=%s AND c.parent_comment_id IS NULL AND
                    (c.status='published' OR (c.status='deleted' AND EXISTS(
                      SELECT 1 FROM marketplace_comments r WHERE r.parent_comment_id=c.id AND r.status='published')))"""
        if actor_id:
            where += """ AND NOT EXISTS(SELECT 1 FROM marketplace_blocks b WHERE
                (b.blocker_actor_id=%s AND b.blocked_actor_id=c.author_actor_id) OR
                (b.blocked_actor_id=%s AND b.blocker_actor_id=c.author_actor_id))"""
            params += [actor_id, actor_id]
        data = _repository.comments_query_1(cur, where, params, offset)
        result = []
        for c in data[:30]:
            deleted = c["status"] == "deleted"
            result.append(
                {
                    "id": c["id"],
                    "body": "Comment deleted." if deleted else c["body"],
                    "created_at": c["created_at"],
                    "author": content_author(cur, c["author_actor_id"], actor_id),
                    "can_delete": not deleted
                    and str(actor_id) == str(c["author_actor_id"]),
                    "deleted": deleted,
                    "parent_comment_id": c.get("parent_comment_id"),
                    "likes": (
                        _repository.comments_query_3(cur, c)["n"]
                        if ready and not deleted
                        else 0
                    ),
                    "liked": bool(
                        ready
                        and actor_id
                        and not deleted
                        and _repository.comments_query_4(cur, actor_id, c)
                    ),
                    "replies": (
                        _repository.comments_query_5(cur, c)["n"] if ready else 0
                    ),
                }
            )
        return {
            "items": result,
            "has_more": len(data) > 30,
            "interactions_available": ready,
        }


def create_comment(post_id: UUID, body: Comment, user=Depends(current_user)):
    with transaction() as cur:
        actor(cur, user, body.actor_id)
        post_access(cur, post_id, body.actor_id)
        rate_limit(cur, user["id"], "marketplace_comments")
        if body.parent_comment_id:
            comment_interactions_ready(cur, required=True)
            parent = comment_access(cur, body.parent_comment_id, body.actor_id)
            if str(parent["post_id"]) != str(post_id):
                raise HTTPException(422, "Reply must belong to the same post.")
            # Replies to replies stay in the original thread.
            root_id = parent["parent_comment_id"] or parent["id"]
            if parent["parent_comment_id"]:
                comment_access(cur, root_id, body.actor_id)
            return _repository.create_comment_query_2(cur, post_id, root_id, body, user)
        return _repository.create_comment_query_1(cur, post_id, body, user)


def like_comment(comment_id: UUID, actor_id: UUID, user=Depends(current_user)):
    with transaction() as cur:
        actor(cur, user, actor_id)
        comment_access(cur, comment_id, actor_id)
        comment_interactions_ready(cur, required=True)
        _repository.like_comment_query_1(cur, comment_id, actor_id)
        return {"ok": True}


def unlike_comment(comment_id: UUID, actor_id: UUID, user=Depends(current_user)):
    with transaction() as cur:
        actor(cur, user, actor_id)
        comment_access(cur, comment_id, actor_id)
        comment_interactions_ready(cur, required=True)
        _repository.unlike_comment_query_1(cur, comment_id, actor_id)
        return {"ok": True}


def delete_comment(comment_id: UUID, actor_id: UUID, user=Depends(current_user)):
    with transaction() as cur:
        actor(cur, user, actor_id)
        required(_repository.delete_comment_query_1(cur, comment_id, actor_id))
        return {"ok": True}


def like(post_id: UUID, actor_id: UUID, user=Depends(current_user)):
    with transaction() as cur:
        actor(cur, user, actor_id)
        post_access(cur, post_id, actor_id)
        _repository.like_query_1(cur, actor_id, post_id)
        return {"ok": True}


def unlike(post_id: UUID, actor_id: UUID, user=Depends(current_user)):
    with transaction() as cur:
        actor(cur, user, actor_id)
        _repository.unlike_query_1(cur, actor_id, post_id)
        return {"ok": True}


def network(
    actor_id: UUID, offset: int = Query(0, ge=0, le=10000), user=Depends(current_user)
):
    with transaction() as cur:
        actor(cur, user, actor_id)
        following = _repository.network_query_1(cur, actor_id, offset)
        connections = _repository.network_query_2(cur, actor_id, offset)
        blocks = _repository.network_query_3(cur, actor_id, offset)
        for c in connections:
            c["incoming"] = str(c["recipient_actor_id"]) == str(actor_id)
            c["other"] = public_actor(
                cur,
                c["requester_actor_id"] if c["incoming"] else c["recipient_actor_id"],
                viewer=actor_id,
            )
            c.pop("requester_actor_id")
            c.pop("recipient_actor_id")
        return {
            "following": [public_actor(cur, x["id"]) for x in following[:30]],
            "connections": connections[:30],
            "blocks": [public_actor(cur, x["id"]) for x in blocks[:30]],
            "has_more": any(
                len(items) > 30 for items in (following, connections, blocks)
            ),
        }


def relationship(actor_id: UUID, target_id: UUID, user=Depends(current_user)):
    with transaction() as cur:
        actor(cur, user, actor_id)
        connection = _repository.relationship_query_1(cur, actor_id, target_id)
        following = _repository.relationship_query_2(cur, actor_id, target_id)
        return {
            "following": bool(following),
            "status": connection["status"] if connection else None,
            "incoming": bool(
                connection and str(connection["recipient_actor_id"]) == str(actor_id)
            ),
        }


def follow(body: Relationship, user=Depends(current_user)):
    with transaction() as cur:
        actor(cur, user, body.actor_id)
        target = public_actor(cur, body.target_id)
        if not target["href"] or blocked(cur, body.actor_id, body.target_id):
            raise HTTPException(404, "Profile unavailable.")
        _repository.follow_query_1(cur, body)
        return {"ok": True}


def unfollow(target_id: UUID, actor_id: UUID, user=Depends(current_user)):
    with transaction() as cur:
        actor(cur, user, actor_id)
        _repository.unfollow_query_1(cur, actor_id, target_id)
        return {"ok": True}


def connect(
    body: Relationship, background_tasks: BackgroundTasks, user=Depends(current_user)
):
    with transaction() as cur:
        actor(cur, user, body.actor_id)
        pair_lock(cur, body.actor_id, body.target_id)
        if (
            body.actor_id == body.target_id
            or not public_actor(cur, body.target_id)["href"]
            or blocked(cur, body.actor_id, body.target_id)
        ):
            raise HTTPException(403, "Connection unavailable.")
        old = _repository.connect_query_1(cur, body)
        if old and old["status"] in ("pending", "accepted"):
            return {"id": old["id"], "status": old["status"]}
        if old:
            result = _repository.connect_query_2(cur, body, old)
        else:
            result = _repository.connect_query_3(cur, body)
    from .notifications import email_connection_request

    background_tasks.add_task(email_connection_request, result["id"])
    return result


def connection_action(
    connection_id: UUID, body: ConnectionAction, user=Depends(current_user)
):
    with transaction() as cur:
        actor(cur, user, body.actor_id)
        c = required(_repository.connection_action_query_2(cur, connection_id))
        mine = str(body.actor_id)
        incoming = mine == str(c["recipient_actor_id"])
        outgoing = mine == str(c["requester_actor_id"])
        allowed = (
            (
                body.action in ("accept", "decline")
                and incoming
                and c["status"] == "pending"
            )
            or (body.action == "cancel" and outgoing and c["status"] == "pending")
            or (
                body.action == "remove"
                and (incoming or outgoing)
                and c["status"] == "accepted"
            )
        )
        if not allowed:
            raise HTTPException(403, "This connection action is unavailable.")
        if body.action == "accept" and blocked(
            cur, c["requester_actor_id"], c["recipient_actor_id"]
        ):
            raise HTTPException(403, "This connection action is unavailable.")
        status = {
            "accept": "accepted",
            "decline": "declined",
            "cancel": "cancelled",
            "remove": "cancelled",
        }[body.action]
        _repository.connection_action_query_1(cur, status, connection_id)
        return {"ok": True}


def block(body: Relationship, user=Depends(current_user)):
    with transaction() as cur:
        actor(cur, user, body.actor_id)
        pair_lock(cur, body.actor_id, body.target_id)
        _repository.block_query_1(cur, body)
        _repository.block_query_2(cur, body)
        _repository.block_query_3(cur, body)
        return {"ok": True}


def unblock(target_id: UUID, actor_id: UUID, user=Depends(current_user)):
    with transaction() as cur:
        actor(cur, user, actor_id)
        pair_lock(cur, actor_id, target_id)
        _repository.unblock_query_1(cur, actor_id, target_id)
        return {"ok": True}


def report(body: Report, user=Depends(current_user)):
    with transaction() as cur:
        actor(cur, user, body.actor_id, messaging=body.target_type == "message")
        if body.target_type == "post":
            post_access(cur, body.target_id, body.actor_id)
        elif body.target_type == "comment":
            c = required(_repository.report_query_2(cur, body))
            post_access(cur, c["post_id"], body.actor_id)
        elif body.target_type == "message":
            required(_repository.report_query_3(cur, body))
        else:
            public_actor(cur, body.target_id)
        # Column selected only from the validated Literal enum.
        return _repository.report_query_1(cur, body)
