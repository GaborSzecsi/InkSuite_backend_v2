"""Marketplace notifications service; transactions and mutation boundaries live here."""

from . import notifications_repository as _repository

"""Connection notifications are derived from pending requests, including older requests."""

import logging
import os
from fastapi import APIRouter, Depends
from .core import transaction, rows, one, current_user, public_actor

router = APIRouter()


def request_sender(cur, actor_id):
    # A connection request identifies its sender to its intended recipient only.
    sender = public_actor(cur, actor_id)
    profile = _repository.request_sender_query_1(cur, actor_id)
    if profile:
        sender["name"] = profile["display_name"]
    return sender


def connection_notifications(user=Depends(current_user)):
    with transaction() as cur:
        pending = _repository.connection_notifications_query_1(cur, user)
        total = pending[0]["total"] if pending else 0
        for item in pending:
            item.pop("total")
            other_id = (
                item["requester_actor_id"]
                if item["kind"] == "request"
                else item["recipient_actor_id"]
            )
            item["sender"] = request_sender(cur, other_id)
            item["notification_key"] = (
                f"{item['id']}:{item['kind']}:{item['responded_at'] or item['created_at']}"
            )
            item.pop("requester_actor_id")
        from . import arc_repository

        if arc_repository.ready(cur):
            for notice in arc_repository.notices(cur, user):
                own = notice["own"]
                message = {
                    "expired": "Your ARC access has expired.",
                    "expiring": "Your ARC access expires within seven days.",
                }.get(
                    notice["notice_status"], f"Your ARC request was {notice['status']}."
                )
                pending.append(
                    {
                        "id": notice["id"],
                        "kind": "arc",
                        "sender": {"name": notice["title"]},
                        "message": (
                            message if own else "A reader requested an advance copy."
                        ),
                        "href": (
                            (
                                f"/marketplace/library/{notice['book_id']}/read"
                                if notice["notice_status"] == "approved"
                                else "/marketplace/library"
                            )
                            if own
                            else "/marketplace/arc-requests"
                        ),
                        "notification_key": f"arc:{notice['id']}:{notice['notice_status']}:{notice['decided_at']}",
                    }
                )
        return {
            "items": pending,
            "count": total + sum(x["kind"] == "arc" for x in pending),
        }


def email_connection_request(connection_id):
    """Called after commit, once per new request; no notification on duplicate Connect clicks."""
    from .invitations import frontend
    from routers.contract_invites import (
        _load_tenant_email_settings_or_400,
        _load_smtp_secret,
        _send_email_smtp,
    )

    try:
        with transaction() as cur:
            c = _repository.email_connection_request_query_1(cur, connection_id)
            if not c:
                return
            sender = request_sender(cur, c["requester_actor_id"])["name"]
            if c["tenant_id"]:
                recipients = _repository.email_connection_request_query_2(cur, c)
            else:
                recipients = _repository.email_connection_request_query_3(cur, c)
        config = _load_tenant_email_settings_or_400(
            c["slug"] or os.getenv("MARKETPLACE_MAIL_TENANT", "marble-press")
        )
        username, password = _load_smtp_secret(config["smtp_secret_id"])
        for recipient in recipients:
            email = (recipient["email"] or "").strip()
            if "@" not in email:
                continue
            _send_email_smtp(
                smtp_host=config["smtp_host"],
                smtp_port=config["smtp_port"],
                tls_mode=config["tls_mode"],
                username=username,
                password=password,
                from_email=config["from_email"],
                from_name=config["from_name"],
                to_email=email,
                to_name="",
                subject="New InkSuite connection request",
                body_text=f"{sender} would like to connect with {c['recipient_name']}.\n\nSign in to InkSuite and open the notification bell to accept or decline:\n{frontend()}/marketplace\n\nThis email does not approve the connection automatically.",
            )
    except Exception as exc:
        logging.getLogger(__name__).warning(
            "Connection request %s email failed (%s); the request remains available in the bell.",
            connection_id,
            type(exc).__name__,
        )
