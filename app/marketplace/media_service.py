"""Marketplace media service; transactions and mutation boundaries live here."""

from . import media_repository as _repository

"""Reference existing publisher assets; reuse S3 for bounded personal images."""

import io, re
from uuid import UUID, uuid4
from fastapi import APIRouter, Depends, File, UploadFile, HTTPException, Query
from psycopg.types.json import Jsonb
from PIL import Image, ImageOps
from botocore.exceptions import BotoCoreError, ClientError
from .core import (
    transaction,
    one,
    rows,
    required,
    current_user,
    actor,
    organization_permission,
)

router = APIRouter()
MAX_FILE = 8 * 1024 * 1024


def storage():
    from routers.storage_s3 import s3
    from app.marketing.assets import bucket

    return s3(), bucket()


def personal_prefix(user_id):
    return f"marketplace/users/{UUID(str(user_id))}/images/"


def validated_reference(cur, user, a, asset_id=None, key=None):
    if asset_id and key:
        raise HTTPException(422, "Choose one image.")
    if asset_id:
        if not a["organization_id"]:
            raise HTTPException(403, "This image belongs to an organization.")
        org = organization_permission(cur, user, a["organization_id"])
        _repository.validated_reference_query_1(cur, org)
        asset = required(_repository.validated_reference_query_3(cur, asset_id, org))
        tenant = required(_repository.validated_reference_query_4(cur, org))
        from app.marketing.domain import public_key

        work = (
            _repository.validated_reference_query_5(cur, asset, org)
            if asset["work_id"]
            else None
        )
        try:
            public_key(
                tenant["slug"],
                asset["s3_key"],
                title_uid=(
                    str(work["uid"])
                    if work and asset["source_type"] == "title_public_asset"
                    else None
                ),
            )
        except ValueError:
            raise HTTPException(
                403, "This asset is not eligible for public use."
            ) from None
        return {
            "asset_id": str(asset_id),
            "key": asset["s3_key"],
            "bucket": asset["s3_bucket"],
        }
    if key:
        if not a["user_id"] or not re.fullmatch(
            re.escape(personal_prefix(user["id"])) + r"[0-9a-f-]{36}\.jpg", key
        ):
            raise HTTPException(
                403, "This image is unavailable for the selected identity."
            )
        _repository.validated_reference_query_2(cur, user)
        client, bucket = storage()
        try:
            client.head_object(Bucket=bucket, Key=key)
        except (ClientError, BotoCoreError):
            raise HTTPException(404, "Image not found.") from None
        return {"key": key, "bucket": bucket}
    return None


def signed_image(ref):
    if not ref or not ref.get("key") or not ref.get("bucket"):
        return ""
    client, bucket = storage()
    if ref["bucket"] != bucket:
        return ""
    # Keys stored only by validated_reference; never accept arbitrary references from clients.
    return client.generate_presigned_url(
        "get_object", Params={"Bucket": bucket, "Key": ref["key"]}, ExpiresIn=900
    )


async def upload_image(
    actor_id: UUID, file: UploadFile = File(...), user=Depends(current_user)
):
    data = await file.read(MAX_FILE + 1)
    if not data or len(data) > MAX_FILE:
        raise HTTPException(413, "Choose an image smaller than 8 MB.")
    try:
        with Image.open(io.BytesIO(data)) as im:
            if (
                im.format not in ("JPEG", "PNG", "WEBP")
                or im.width * im.height > 20_000_000
            ):
                raise ValueError()
            im = ImageOps.exif_transpose(im)
            im.thumbnail((1600, 1600))
            result = Image.new("RGB", im.size, "white")
            if im.mode == "RGBA":
                result.paste(im, mask=im.getchannel("A"))
            else:
                result.paste(im.convert("RGB"))
            out = io.BytesIO()
            result.save(out, "JPEG", quality=90)
            data = out.getvalue()
    except Exception:
        raise HTTPException(
            422, "Use a valid JPEG, PNG or WebP image up to 20 megapixels."
        ) from None
    with transaction() as cur:
        a = actor(cur, user, actor_id)
        try:
            if a["organization_id"]:
                o = organization_permission(cur, user, a["organization_id"])
                tenant = required(_repository.upload_image_query_2(cur, o))
                from app.marketing.assets import upload

                value = upload(
                    cur,
                    {"tenant": tenant, "user": user},
                    "marketplace.jpg",
                    data,
                    None,
                    "general",
                )
                return {"asset_id": value["id"]}
            # Use the existing S3 client/bucket, without creating a second asset catalog.
            _repository.upload_image_query_1(cur, user)
            client, bucket = storage()
            prefix = personal_prefix(user["id"])
            used = sum(
                int(x["Size"])
                for page in client.get_paginator("list_objects_v2").paginate(
                    Bucket=bucket, Prefix=prefix
                )
                for x in page.get("Contents", [])
            )
            if used + len(data) > 1_000_000_000:
                raise HTTPException(
                    413,
                    "Your image storage is full. Delete unused images before uploading.",
                )
            key = prefix + str(uuid4()) + ".jpg"
            client.put_object(
                Bucket=bucket, Key=key, Body=data, ContentType="image/jpeg"
            )
            return {"key": key}
        except (ClientError, BotoCoreError):
            raise HTTPException(
                503, "Image storage is temporarily unavailable."
            ) from None


def personal_images(
    actor_id: UUID, offset: int = Query(0, ge=0, le=10000), user=Depends(current_user)
):
    with transaction() as cur:
        a = actor(cur, user, actor_id)
        if not a["user_id"]:
            raise HTTPException(422, "Use the publisher media library.")
        client, bucket = storage()
        try:
            data = [
                x
                for page in client.get_paginator("list_objects_v2").paginate(
                    Bucket=bucket, Prefix=personal_prefix(user["id"])
                )
                for x in page.get("Contents", [])
            ]
        except (ClientError, BotoCoreError):
            raise HTTPException(
                503, "Image storage is temporarily unavailable."
            ) from None
        data.sort(key=lambda x: x["LastModified"], reverse=True)
        return {
            "items": [
                {
                    "key": x["Key"],
                    "url": signed_image({"key": x["Key"], "bucket": bucket}),
                }
                for x in data[offset : offset + 100]
            ],
            "has_more": len(data) > offset + 100,
            "used_bytes": sum(x["Size"] for x in data),
            "limit_bytes": 1_000_000_000,
        }


def delete_image(image_id: UUID, actor_id: UUID, user=Depends(current_user)):
    with transaction() as cur:
        a = actor(cur, user, actor_id)
        if not a["user_id"]:
            raise HTTPException(
                403, "Use Marketing storage management for publisher assets."
            )
        _repository.delete_image_query_1(cur, user)
        key = personal_prefix(user["id"]) + str(image_id) + ".jpg"
        if _repository.delete_image_query_2(
            cur, key
        ) or _repository.delete_image_query_3(cur, key):
            raise HTTPException(
                409, "Remove this image from your profile or post before deleting it."
            )
        client, bucket = storage()
        try:
            client.delete_object(Bucket=bucket, Key=key)
        except (ClientError, BotoCoreError):
            raise HTTPException(
                503, "Image storage is temporarily unavailable."
            ) from None
        return {"ok": True}


def identity_image(
    actor_id: UUID,
    purpose: str,
    asset_id: UUID | None = None,
    key: str | None = None,
    user=Depends(current_user),
):
    with transaction() as cur:
        a = actor(cur, user, actor_id)
        if a["organization_id"]:
            organization_permission(cur, user, a["organization_id"], admin=True)
        ref = validated_reference(cur, user, a, asset_id, key)
        if a["user_id"] and purpose == "avatar":
            _repository.identity_image_query_1(cur, ref, user, Jsonb)
        elif a["organization_id"] and purpose in ("logo", "banner"):
            _repository.identity_image_query_2(cur, purpose, ref, a, Jsonb)
        else:
            raise HTTPException(422, "Choose a valid image purpose.")
        return {"ok": True}
