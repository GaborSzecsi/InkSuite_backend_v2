"""Marketplace messages service; transactions and mutation boundaries live here."""

from . import messages_repository as _repository
from uuid import UUID
from fastapi import APIRouter, Depends, Query, HTTPException
from .core import *
from .schemas import Relationship, Message

router = APIRouter()


def thread(cur, user, conversation_id, actor_id):
    actor(cur, user, actor_id, messaging=True)
    return required(_repository.thread_query_1(cur, conversation_id, actor_id))


def start(body: Relationship, user=Depends(current_user)):
    with transaction() as cur:
        actor(cur, user, body.actor_id, messaging=True)
        if body.actor_id == body.target_id:
            raise HTTPException(422, "Choose another identity.")
        pair_lock(cur, body.actor_id, body.target_id)
        message_allowed(cur, body.actor_id, body.target_id)
        low, high = sorted([body.actor_id, body.target_id])
        return _repository.start_query_1(cur, low, high)


def inbox(
    actor_id: UUID, offset: int = Query(0, ge=0, le=10000), user=Depends(current_user)
):
    with transaction() as cur:
        actor(cur, user, actor_id, messaging=True)
        data = _repository.inbox_query_1(cur, actor_id, offset)
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
            c.pop("actor_high_id")
            c.pop("actor_low_id")
        return {"items": data[:30], "has_more": len(data) > 30}


def unread(actor_id: UUID, user=Depends(current_user)):
    with transaction() as cur:
        actor(cur, user, actor_id, messaging=True)
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
        return {
            "items": list(reversed(data[:50])),
            "has_more": len(data) > 50,
            "other": public_actor(cur, other, viewer=actor_id),
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
        message_allowed(cur, body.actor_id, other)
        rate_limit(cur, user["id"], "marketplace_messages")
        return _repository.send_query_1(cur, conversation_id, body, user)


def mark_read(
    conversation_id: UUID, message_id: UUID, actor_id: UUID, user=Depends(current_user)
):
    with transaction() as cur:
        thread(cur, user, conversation_id, actor_id)
        m = required(_repository.mark_read_query_2(cur, message_id, conversation_id))
        _repository.mark_read_query_1(cur, conversation_id, actor_id, m)
        return {"ok": True}


def delete_message(message_id: UUID, actor_id: UUID, user=Depends(current_user)):
    with transaction() as cur:
        actor(cur, user, actor_id, messaging=True)
        required(_repository.delete_message_query_1(cur, message_id, actor_id))
        return {"ok": True}
