"""Marketplace messages service; transactions and mutation boundaries live here."""

from . import messages_repository as _repository
from uuid import UUID
from fastapi import APIRouter, Depends, Query, HTTPException
from .core import *
from .schemas import Relationship, Message

router = APIRouter()


def presence(cur, other, viewer):
    if not connected(cur,viewer,other) or blocked(cur,viewer,other):
        return False
    if not one(cur,"SELECT to_regclass('public.marketplace_messaging_sessions') AS ready")['ready']:
        return False
    return bool(one(cur,"SELECT 1 FROM marketplace_messaging_sessions WHERE actor_id=%s AND connected AND heartbeat_at>now()-interval '90 seconds' LIMIT 1",(other,)))


def require_messaging_schema(cur):
    from .realtime_routes import ready
    ready(cur)


def require_connection(cur, sender, recipient):
    if not connected(cur,sender,recipient):
        raise HTTPException(403,"Messaging requires an accepted Marketplace connection.")


def thread(cur, user, conversation_id, actor_id):
    actor(cur, user, actor_id, messaging=True)
    return required(_repository.thread_query_1(cur, conversation_id, actor_id))


def start(body: Relationship, user=Depends(current_user)):
    with transaction() as cur:
        actor(cur, user, body.actor_id, messaging=True)
        if body.actor_id == body.target_id:
            raise HTTPException(422, "Choose another identity.")
        pair_lock(cur, body.actor_id, body.target_id)
        require_connection(cur, body.actor_id, body.target_id)
        message_allowed(cur, body.actor_id, body.target_id)
        low, high = sorted([body.actor_id, body.target_id])
        return _repository.start_query_1(cur, low, high)


def inbox(
    actor_id: UUID, offset: int = 0, user=Depends(current_user), q: str = "", unread_only: bool = False, search: bool = False
):
    with transaction() as cur:
        actor(cur, user, actor_id, messaging=True)
        require_messaging_schema(cur)
        data = _repository.inbox_query_1(cur, actor_id, offset, q, unread_only, search)
        for c in data:
            c["other"] = public_actor(
                cur,
                (
                    c["actor_high_id"]
                    if str(c["actor_low_id"]) == str(actor_id)
                    else c["actor_low_id"]
                ),
                viewer=actor_id,
            )
            c["other"]["online"] = presence(cur,c["other"]["id"],actor_id)
            c.pop("actor_high_id")
            c.pop("actor_low_id")
        return {"items": data[:30] if search else data, "has_more": search and len(data) > 30}


def unread(actor_id: UUID, user=Depends(current_user)):
    with transaction() as cur:
        actor(cur, user, actor_id, messaging=True)
        require_messaging_schema(cur)
        return _repository.unread_query_1(cur, actor_id)


def history(
    conversation_id: UUID,
    actor_id: UUID,
    before: UUID | None = None,
    user=Depends(current_user),
):
    with transaction() as cur:
        c = thread(cur, user, conversation_id, actor_id)
        params = [conversation_id]
        where = ""
        if before:
            marker = required(_repository.history_query_2(cur, before, conversation_id))
            where = " AND (created_at,id)<(%s,%s)"
            params += [marker["created_at"], marker["id"]]
        data = _repository.history_query_1(cur, where, params)
        other = (
            c["actor_high_id"]
            if str(c["actor_low_id"]) == str(actor_id)
            else c["actor_low_id"]
        )
        participant = public_actor(cur, other, viewer=actor_id)
        participant["online"] = presence(cur,other,actor_id)
        return {
            "items": list(reversed(data[:50])),
            "has_more": len(data) > 50,
            "other": participant,
        }


def send(conversation_id: UUID, body: Message, user=Depends(current_user)):
    with transaction() as cur:
        c = thread(cur, user, conversation_id, body.actor_id)
        other = (
            c["actor_high_id"]
            if str(c["actor_low_id"]) == str(body.actor_id)
            else c["actor_low_id"]
        )
        # Lock order: pair, then per-user rate limit. Blocks acquire the same pair lock.
        pair_lock(cur, body.actor_id, other)
        require_connection(cur, body.actor_id, other)
        message_allowed(cur, body.actor_id, other)
        require_messaging_schema(cur)
        existing = _repository.existing_message(cur,conversation_id,body)
        if existing:
            return existing
        rate_limit(cur, user["id"], "marketplace_messages")
        return _repository.send_query_1(cur, conversation_id, body, user)


def mark_read(
    conversation_id: UUID, message_id: UUID, actor_id: UUID, user=Depends(current_user)
):
    with transaction() as cur:
        thread(cur, user, conversation_id, actor_id)
        require_messaging_schema(cur)
        m = required(_repository.mark_read_query_2(cur, message_id, conversation_id))
        _repository.mark_read_query_1(cur, conversation_id, actor_id, m)
        return {"ok": True}


def delete_message(message_id: UUID, actor_id: UUID, user=Depends(current_user)):
    with transaction() as cur:
        actor(cur, user, actor_id, messaging=True)
        required(_repository.delete_message_query_1(cur, message_id, actor_id))
        return {"ok": True}
