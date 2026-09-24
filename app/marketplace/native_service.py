"""Upload authorization and durable processing scheduling; no binary API uploads."""

import os
from uuid import uuid4
from fastapi import HTTPException
from botocore.exceptions import ClientError, BotoCoreError
from .core import transaction, actor, required
from . import native_repository as repo, native_storage as storage

MIMES = {
    "image/jpeg": "image",
    "image/png": "image",
    "image/webp": "image",
    "video/mp4": "video",
    "video/quicktime": "video",
}


def limits():
    return {
        "image": int(os.getenv("MARKETPLACE_IMAGE_MAX_BYTES", 20 * 1024**2)),
        "video": int(os.getenv("MARKETPLACE_VIDEO_MAX_BYTES", 500 * 1024**2)),
        "quota": int(os.getenv("MARKETPLACE_MEDIA_QUOTA_BYTES", 1024**3)),
    }


def ready(cur):
    if not repo.available(cur):
        raise HTTPException(
            503, "Native media needs migration 010_marketplace_native_media.sql."
        )
    storage.config()
    storage.check_delivery()


def capabilities():
    try:
        with transaction() as cur:
            ready(cur)
        return {"enabled": True, "limits": limits()}
    except HTTPException as exc:
        if exc.status_code != 503:
            raise
        return {"enabled": False, "message": exc.detail, "limits": limits()}


def initiate(body, user):
    settings = limits()
    kind = MIMES.get(body.content_type)
    if not kind or body.file_size > settings[kind]:
        raise HTTPException(422, "Unsupported media format or file too large.")
    # Reserve space for derivatives as well as originals; worker verifies the cap.
    reserve = body.file_size + (16 * 1024**2 if kind == "image" else 256 * 1024**2)
    with transaction() as cur:
        ready(cur)
        owner_actor = actor(cur, user, body.actor_id)
        repo.lock_owner(cur, body.actor_id)
        if owner_actor.get("user_id") and int(repo.usage(cur, body.actor_id)) + reserve > settings["quota"]:
            raise HTTPException(
                413,
                "Social storage is full. Delete older unused assets before uploading.",
            )
        media_id = uuid4()
        row = repo.insert(
            cur,
            (
                media_id,
                body.actor_id,
                user["id"],
                kind,
                storage.config()[0],
                f"originals/{body.actor_id}/{media_id}/source",
                body.filename,
                body.content_type,
                body.file_size,
                reserve,
            ),
        )
        try:
            form = storage.upload_form(row)
        except (ClientError, BotoCoreError):
            raise HTTPException(
                503, "Media upload authorization is unavailable. Please retry."
            ) from None
    return {"id": media_id, "upload": form, "status": "pending_upload"}


def complete(media_id, owner, user):
    with transaction() as cur:
        ready(cur)
        actor(cur, user, owner)
        row = required(repo.owned(cur, media_id, owner))
        if row["status"] != "pending_upload":
            return storage.describe(row)
        try:
            head = storage.s3().head_object(
                Bucket=row["bucket"], Key=row["original_key"]
            )
        except (ClientError, BotoCoreError):
            raise HTTPException(
                409, "Upload not found yet. Retry after uploading."
            ) from None
        if (
            head["ContentLength"] != row["file_size"]
            or not head.get("VersionId")
            or head["VersionId"] == "null"
        ):
            raise HTTPException(422, "Upload size or bucket versioning is invalid.")
        # Pin the exact version. A replayed presigned POST cannot replace the processed source.
        row = repo.complete(cur, row, head["VersionId"])
    return storage.describe(row)


def inspect(media_id, owner, user):
    with transaction() as cur:
        ready(cur)
        actor(cur, user, owner)
        return storage.describe(required(repo.owned(cur, media_id, owner)))


def list_media(owner, offset, user):
    with transaction() as cur:
        ready(cur)
        owner_actor = actor(cur, user, owner)
        data = repo.list_owned(cur, owner, offset)
        return {
            "items": [storage.describe(x) for x in data[:30]],
            "has_more": len(data) > 30,
            "used_bytes": repo.usage(cur, owner),
            "quota_bytes": limits()["quota"] if owner_actor.get("user_id") else None,
        }


def remove(media_id, owner, user):
    with transaction() as cur:
        ready(cur)
        actor(cur, user, owner)
        row = required(repo.owned(cur, media_id, owner))
        if repo.referenced(cur, row):
            raise HTTPException(
                409, "Delete posts using this asset before removing it."
            )
        repo.remove(cur, row)
    return {"ok": True}


def attach(cur, post, ids, owner):
    assets = repo.lock_assets(cur, ids)
    if len(assets) != len(ids) or any(
        str(x["owner_actor_id"]) != str(owner) or x["status"] != "ready" for x in assets
    ):
        raise HTTPException(422, "Select ready media belonging to this identity.")
    if any(x["media_type"] == "video" for x in assets) and len(assets) != 1:
        raise HTTPException(422, "Choose up to ten images or one video.")
    repo.attach(cur, post, ids)


def retry(media_id, owner, user):
    with transaction() as cur:
        ready(cur)
        actor(cur, user, owner)
        row = required(repo.owned(cur, media_id, owner))
        if (
            row["status"] != "failed"
            or row["attempts"] >= 6
            or not row["original_version"]
        ):
            raise HTTPException(
                409, "This asset cannot be retried. Remove it and select another file."
            )
        repo.complete(cur, row, row["original_version"])
    return {"ok": True}
