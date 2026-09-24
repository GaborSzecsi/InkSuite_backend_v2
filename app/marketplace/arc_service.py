from datetime import datetime, timezone
from uuid import uuid4
from pathlib import PurePath
from collections import OrderedDict
from threading import Lock
from time import monotonic
import hashlib

from fastapi import HTTPException

from .core import (
    transaction,
    required,
    actor,
    organization_permission,
    safe_url,
)
from . import arc_repository as repo
from . import arc_epub


# Bounded process-memory cache only.
# Authorization is always checked before and after resource access.
_archive_cache = OrderedDict()
_archive_lock = Lock()
_CACHE_BYTES = 96 * 1024 * 1024


def now():
    return datetime.now(timezone.utc)


def date(value):
    return (
        datetime.fromisoformat(value.replace("Z", "+00:00"))
        if isinstance(value, str)
        else value
    )


def available(item):
    return (
        item.get("asset_status", item.get("status")) == "active"
        and item.get("organization_status", "active") == "active"
        and item.get("marketplace_status", "public") == "public"
        and not item.get("revoked_at")
        and all(
            not item.get(k) or date(item[k]) > now()
            for k in ("expires_at", "available_until")
        )
        and (
            not item.get("available_from")
            or date(item["available_from"]) <= now()
        )
    )


def check_ready(cur):
    if not repo.ready(cur):
        raise HTTPException(
            503,
            "ARC Library requires database migration 013.",
        )


def owned(cur, user, aid):
    a = required(repo.asset(cur, aid))

    organization_permission(
        cur,
        user,
        a["organization_id"],
        admin=True,
    )

    if a["organization_status"] != "active":
        raise HTTPException(
            403,
            "Publisher is not active.",
        )

    return a


def access(cur, user, bid):
    check_ready(cur)

    ent = required(
        repo.entitlement(
            cur,
            user["id"],
            bid,
        ),
        "ARC access not found.",
    )

    if not available(ent):
        raise HTTPException(
            403,
            "This ARC has expired, been revoked, or is unavailable.",
        )

    return ent


def storage():
    from routers.uploads import _s3_client, S3_BUCKET

    return _s3_client(), S3_BUCKET


def _delete_uploaded_object(client, bucket, key):
    """
    Best-effort cleanup when S3 persistence succeeds but the
    corresponding ARC database operation fails.

    Cleanup failure must never hide the original application error.
    """
    try:
        client.delete_object(
            Bucket=bucket,
            Key=key,
        )
    except Exception:
        pass


def upload(
    eid,
    user,
    data,
    filename,
    mime,
    replace,
    start,
    end,
    enabled,
):
    if not filename.lower().endswith(".epub") or mime not in (
        "application/epub+zip",
        "application/octet-stream",
        "application/zip",
    ):
        raise HTTPException(
            422,
            "Choose an EPUB file.",
        )

    if (
        any(d and d.tzinfo is None for d in (start, end))
        or (
            start
            and end
            and end <= start
        )
    ):
        raise HTTPException(
            422,
            "Use valid availability dates including a time zone.",
        )

    client = None
    bucket = None
    key = None

    try:
        # Authorize before parsing untrusted bytes or writing storage.
        with transaction() as cur:
            check_ready(cur)

            e = required(
                repo.edition(
                    cur,
                    eid,
                )
            )

            organization_permission(
                cur,
                user,
                e["organization_id"],
                admin=True,
            )

            if e["organization_status"] != "active":
                raise HTTPException(
                    403,
                    "Publisher is not active.",
                )

            old = repo.existing(
                cur,
                eid,
            )

            if old and not replace:
                raise HTTPException(
                    409,
                    "Confirm replacement of the existing ARC.",
                )

            # Validate and inspect the complete EPUB before S3 write.
            package = arc_epub.inspect_epub(data)

            aid = old["id"] if old else uuid4()
            revision = old["revision"] + 1 if old else 1

            # Use the existing PRIVATE title root.
            # Do not use Marketplace/social-media storage.
            key = (
                f"tenants/{e['slug']}/data/uploads/"
                f"{e['uid']}/arc/{aid}/"
                f"{revision}-{uuid4().hex}.epub"
            )

            client, bucket = storage()

            client.put_object(
                Bucket=bucket,
                Key=key,
                Body=data,
                ContentType="application/epub+zip",
                ServerSideEncryption="AES256",
            )

            repo.upload(
                cur,
                aid,
                eid,
                revision,
                key,
                PurePath(filename).name[:200],
                data,
                package,
                user,
                start,
                end,
                enabled,
            )

            repo.audit(
                cur,
                aid,
                user,
                "replaced" if old else "uploaded",
                {
                    "revision": revision,
                },
            )

            result = {
                "id": aid,
                "revision": revision,
            }

        # Transaction successfully committed.
        return result

    except Exception:
        # If S3 was written but the DB operation or commit failed,
        # remove the newly uploaded object on a best-effort basis.
        if client is not None and bucket and key:
            _delete_uploaded_object(
                client,
                bucket,
                key,
            )

        raise


def edition_state(eid, user):
    with transaction() as cur:
        check_ready(cur)

        e = required(
            repo.edition(
                cur,
                eid,
            )
        )

        organization_permission(
            cur,
            user,
            e["organization_id"],
            admin=True,
        )

        a = repo.existing(
            cur,
            eid,
        )

        return {
            "asset": (
                {
                    k: a[k]
                    for k in (
                        "id",
                        "revision",
                        "request_enabled",
                        "available_from",
                        "available_until",
                    )
                }
                if a
                else None
            )
        }


def options(bid, user):
    with transaction() as cur:
        check_ready(cur)

        return {
            "items": repo.book_assets(
                cur,
                bid,
                user["id"],
            )
        }


def request(aid, body, user):
    with transaction() as cur:
        check_ready(cur)

        # Validates that the authenticated user is authorized to act
        # as either the personal actor or organization actor supplied.
        actor(
            cur,
            user,
            body.actor_id,
        )

        a = required(
            repo.asset(
                cur,
                aid,
            )
        )

        if (
            not a["book_id"]
            or not available(a)
            or not a["request_enabled"]
        ):
            raise HTTPException(
                409,
                "This ARC is not accepting requests.",
            )

        existing_library = repo.library_asset(
            cur,
            user["id"],
            a["book_id"],
        )

        if existing_library:
            raise HTTPException(
                409,
                "Your Library already has an ARC for this title.",
            )

        previous = repo.request(
            cur,
            aid,
            user["id"],
        )

        if previous:
            if previous["status"] == "pending":
                raise HTTPException(
                    409,
                    "Your ARC request is already pending.",
                )

            if previous["status"] == "approved":
                raise HTTPException(
                    409,
                    "This ARC request has already been approved.",
                )

            if previous["status"] in (
                "rejected",
                "revoked",
            ):
                result = repo.reopen_request(
                    cur,
                    previous["id"],
                    body.actor_id,
                    body.message,
                )

                if not result:
                    raise HTTPException(
                        409,
                        "This ARC request could not be reopened.",
                    )

                repo.audit(
                    cur,
                    aid,
                    user,
                    "re_requested",
                    {
                        "request_id": str(previous["id"]),
                    },
                )

                return result

            raise HTTPException(
                409,
                "An ARC request already exists for your account.",
            )

        result = repo.new_request(
            cur,
            aid,
            body.actor_id,
            user,
            body.message,
        )

        repo.audit(
            cur,
            aid,
            user,
            "requested",
        )

        return result


def queue(oid, status, user):
    with transaction() as cur:
        check_ready(cur)

        organization_permission(
            cur,
            user,
            oid,
            admin=True,
        )

        return {
            "items": repo.queue(
                cur,
                oid,
                status,
            )
        }


def decide(rid, status, reason, user):
    with transaction() as cur:
        check_ready(cur)

        r = required(
            repo.request_by_id(
                cur,
                rid,
            )
        )

        a = owned(
            cur,
            user,
            r["asset_id"],
        )

        if (
            (
                status == "revoked"
                and r["status"] != "approved"
            )
            or (
                status != "revoked"
                and r["status"] != "pending"
            )
        ):
            raise HTTPException(
                409,
                "This request has already been decided.",
            )

        if status == "approved":
            if (
                not a["book_id"]
                or not available(a)
            ):
                raise HTTPException(
                    409,
                    "The ARC is unavailable.",
                )

            existing = repo.library_asset(
                cur,
                r["requested_by"],
                a["book_id"],
            )

            if (
                existing
                and str(existing["arc_asset_id"])
                != str(a["id"])
            ):
                raise HTTPException(
                    409,
                    "This reader already has another edition "
                    "of this title in Library.",
                )

            repo.grant(
                cur,
                r["requested_by"],
                a["book_id"],
                a["id"],
                uuid4().hex.upper(),
                a["available_until"],
            )

        elif status == "revoked":
            revoked = repo.revoke(
                cur,
                r["requested_by"],
                a["id"],
            )

            if not revoked:
                raise HTTPException(
                    409,
                    "ARC entitlement was not found.",
                )

        repo.decide(
            cur,
            r,
            status,
            user,
            reason,
        )

        repo.audit(
            cur,
            a["id"],
            user,
            status,
            {
                "request_id": str(rid),
            },
        )

        return {
            "ok": True,
        }


def library(user):
    with transaction() as cur:
        check_ready(cur)

        items = repo.library(
            cur,
            user,
        )

        for item in items:
            item["cover_url"] = safe_url(
                item.get("cover_url")
            )

            item["can_read"] = (
                bool(item["arc_asset_id"])
                and available(item)
            )

            if (
                item["arc_asset_id"]
                and item["reading_revision"]
                != item["revision"]
            ):
                item["reading_progress"] = 0
                item["last_location"] = ""

            # User identity is already known from authentication.
            # Never expose it unnecessarily through Library responses.
            item.pop(
                "user_id",
                None,
            )

        return {
            "items": items,
        }


def start(bid, user):
    with transaction() as cur:
        e = access(
            cur,
            user,
            bid,
        )

        s = repo.create_session(
            cur,
            user["id"],
            bid,
            e["revision"],
        )

        affiliation = (
            f" • {e['verified_organization']}"
            if e.get("verified_organization")
            else ""
        )

        watermark = (
            f"ADVANCE READING COPY • "
            f"{e['display_name']}"
            f"{affiliation} • "
            f"ARC {e['arc_access_code'][:12]}"
        )

        return {
            **s,
            "revision": e["revision"],
            "title": e["title"],
            "location": (
                e["last_location"]
                if e["reading_revision"]
                == e["revision"]
                else ""
            ),
            "watermark": watermark,
            "package_path": e["package"]["opf"],
        }


def session_access(cur, user, sid):
    check_ready(cur)

    s = required(
        repo.session(
            cur,
            sid,
            user["id"],
        ),
        "Reading session expired. Reopen the book.",
    )

    e = access(
        cur,
        user,
        s["marketplace_book_id"],
    )

    if e["revision"] != s["revision"]:
        raise HTTPException(
            409,
            "A revised ARC is available. Reopen the book.",
        )

    return e


def resource(sid, path, user):
    # Authorization before storage access.
    with transaction() as cur:
        e = session_access(
            cur,
            user,
            sid,
        )

    if (
        path != e["package"]["opf"]
        and path not in e["package"]["resources"]
    ):
        raise HTTPException(
            404,
            "Resource not found.",
        )

    cache_key = (
        e["storage_key"],
        e["sha256"],
    )

    with _archive_lock:
        for key in list(_archive_cache):
            if _archive_cache[key][0] < monotonic():
                del _archive_cache[key]

        cached = _archive_cache.get(
            cache_key
        )

        data = (
            cached[1]
            if cached
            else None
        )

    if data is None:
        client, bucket = storage()

        body = client.get_object(
            Bucket=bucket,
            Key=e["storage_key"],
        )["Body"]

        try:
            data = body.read()
        finally:
            body.close()

        if (
            hashlib.sha256(data).hexdigest()
            != e["sha256"]
        ):
            raise HTTPException(
                503,
                "ARC storage integrity check failed.",
            )

        with _archive_lock:
            _archive_cache[cache_key] = (
                monotonic() + 120,
                data,
            )

            while (
                sum(
                    len(v[1])
                    for v in _archive_cache.values()
                )
                > _CACHE_BYTES
            ):
                _archive_cache.popitem(
                    last=False
                )

    result = arc_epub.resource(
        data,
        e["package"],
        path,
    )

    # Recheck after S3/cache work. Access might have been revoked
    # while the resource was being loaded.
    with transaction() as cur:
        session_access(
            cur,
            user,
            sid,
        )

    return result


def progress(bid, body, user):
    with transaction() as cur:
        e = access(
            cur,
            user,
            bid,
        )

        if e["revision"] != body.revision:
            raise HTTPException(
                409,
                "ARC revision changed.",
            )

        repo.progress(
            cur,
            user["id"],
            bid,
            body.revision,
            body.location,
            body.percent,
        )

        return {
            "ok": True,
        }


def bookmarks(
    bid,
    user,
    body=None,
    delete_id=None,
):
    with transaction() as cur:
        e = access(
            cur,
            user,
            bid,
        )

        if body:
            if body.revision != e["revision"]:
                raise HTTPException(
                    409,
                    "ARC revision changed.",
                )

            return repo.add_bookmark(
                cur,
                user["id"],
                bid,
                e["revision"],
                body,
            )

        if delete_id:
            deleted = repo.delete_bookmark(
                cur,
                user["id"],
                bid,
                delete_id,
            )

            if not deleted:
                raise HTTPException(
                    404,
                    "Bookmark not found.",
                )

        return {
            "items": repo.bookmarks(
                cur,
                user["id"],
                bid,
                e["revision"],
            )
        }


def preview(aid, user):
    with transaction() as cur:
        check_ready(cur)

        a = owned(
            cur,
            user,
            aid,
        )

        if (
            not a["book_id"]
            or not available(a)
        ):
            raise HTTPException(
                409,
                "Publish this edition in your Marketplace "
                "catalog before previewing it in Library.",
            )

        existing = repo.library_asset(
            cur,
            user["id"],
            a["book_id"],
        )

        if (
            existing
            and str(existing["arc_asset_id"])
            != str(aid)
        ):
            raise HTTPException(
                409,
                "Your Library already contains another "
                "ARC edition of this title.",
            )

        entitlement = repo.entitlement(
            cur,
            user["id"],
            a["book_id"],
        )

        if (
            not entitlement
            or not available(entitlement)
        ):
            repo.grant(
                cur,
                user["id"],
                a["book_id"],
                aid,
                uuid4().hex.upper(),
                a["available_until"],
            )

            repo.audit(
                cur,
                aid,
                user,
                "publisher_preview_granted",
            )

        return {
            "book_id": a["book_id"],
        }


def session_status(sid, user):
    with transaction() as cur:
        session_access(
            cur,
            user,
            sid,
        )

        return {
            "ok": True,
        }