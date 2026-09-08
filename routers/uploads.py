# marble_app/routers/uploads.py
import os, io, time, pathlib, mimetypes, re
from typing import Literal, Optional, Any, Dict, List
from urllib.parse import urlparse

from fastapi import APIRouter, UploadFile, File, Form, HTTPException, Query, Body
from pydantic import BaseModel
from PIL import Image
from psycopg.rows import dict_row

from app.core.db import db_conn

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError, EndpointConnectionError, NoCredentialsError

# IMPORTANT:
# This router is the ONLY one that should own:
#   GET  /api/uploads/book-assets
#   GET  /api/uploads/health
#   POST /api/uploads/
router = APIRouter(prefix="/uploads", tags=["Uploads"])

# ----------------------------
# Local storage (existing uploads behavior)
# ----------------------------
DATA_UPLOAD_DIR = os.environ.get("DATA_UPLOAD_DIR", "./data/uploads")
pathlib.Path(DATA_UPLOAD_DIR).mkdir(parents=True, exist_ok=True)

UploadKind = Literal[
    "author_contract",
    "illustrator_contract",
    "w9",
    "book_cover",
    "back_cover",
    "interior_image",
    "full_pdf",
    "video",
    "audio",
    "public_other",
    "public_resource",
    "author_photo",
    "illustrator_photo",
    "other",
]

ALLOWED = {
    "author_contract": {"application/pdf"},
    "illustrator_contract": {"application/pdf"},
    "w9": {"application/pdf"},
    "book_cover": {"image/jpeg", "image/png", "image/webp", "image/jpg"},
    "back_cover": {"image/jpeg", "image/png", "image/webp", "image/jpg"},
    "interior_image": {"image/jpeg", "image/png", "image/webp", "image/jpg"},
    "full_pdf": {"application/pdf"},
    "video": {"video/mp4"},
    "audio": {"audio/mpeg", "audio/mp3"},
    "public_other": {
        "application/pdf",
        "image/jpeg", "image/png", "image/webp", "image/jpg",
        "video/mp4",
        "audio/mpeg", "audio/mp3",
        "application/zip",
        "text/plain",
        "text/html",
    },
    "public_resource": set(),
    "author_photo": {"image/jpeg", "image/png", "image/webp", "image/jpg"},
    "illustrator_photo": {"image/jpeg", "image/png", "image/webp", "image/jpg"},
    "other": {"application/pdf", "image/jpeg", "image/png", "image/webp"},
}

class UploadResponse(BaseModel):
    ok: bool
    url: str
    filename: str
    mime: str
    size: int
    width: int | None = None
    height: int | None = None
    dpi: tuple[int, int] | None = None
    key: str | None = None
    bookUid: str | None = None
    workId: str | None = None
    editionId: str | None = None
    isbn13: str | None = None
    resourceId: str | None = None
    resourceVersionId: str | None = None

def _clean_name(name: str) -> str:
    return "".join(c for c in (name or "") if c.isalnum() or c in ("-", "_", ".")).strip("._")


PUBLIC_IMAGE_KINDS = {"book_cover", "back_cover", "interior_image"}
PUBLIC_RESOURCE_KINDS = PUBLIC_IMAGE_KINDS | {"full_pdf", "video", "audio", "public_other"}


def _clean_isbn13(value: str) -> str:
    digits = "".join(ch for ch in (value or "") if ch.isdigit())
    if len(digits) != 13:
        raise HTTPException(status_code=400, detail="A valid 13-digit ISBN is required for public resources")
    return digits


def _public_url_for_key(key: str) -> str:
    # Public-resource SQL must store a stable URL, never an expiring presigned URL.
    return f"https://{S3_BUCKET}.s3.{AWS_REGION}.amazonaws.com/{key}"


def _public_storage_key(uid: str, filename: str) -> str:
    """All retailer/catalog assets live in the title's public S3 folder."""
    return f"{UPLOADS_S3_PREFIX}/{uid}/public/{filename}"


def _resolve_edition(work_id: str, edition_id: str = "", isbn13: str = "") -> Dict[str, Any]:
    edition_id = (edition_id or "").strip()
    isbn = _clean_isbn13(isbn13)
    with db_conn() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            if edition_id:
                cur.execute(
                    """
                    SELECT id::text AS id, tenant_id::text AS tenant_id,
                           work_id::text AS work_id, isbn13,
                           product_form, product_form_detail
                    FROM editions
                    WHERE id = %s::uuid
                      AND work_id = %s::uuid
                    LIMIT 1
                    """,
                    (edition_id, work_id),
                )
            else:
                cur.execute(
                    """
                    SELECT id::text AS id, tenant_id::text AS tenant_id,
                           work_id::text AS work_id, isbn13,
                           product_form, product_form_detail
                    FROM editions
                    WHERE work_id = %s::uuid
                      AND isbn13 = %s
                    LIMIT 1
                    """,
                    (work_id, isbn),
                )
            row = cur.fetchone()
    if not row:
        raise HTTPException(status_code=404, detail=f"Edition not found for ISBN {isbn}")
    if (row.get("isbn13") or "").strip() != isbn:
        raise HTTPException(status_code=400, detail="editionId and isbn13 do not match")
    return row


def _normalize_public_image(data: bytes) -> tuple[bytes, int, int, tuple[int, int]]:
    try:
        img = Image.open(io.BytesIO(data))
        width, height = img.size
        if max(width, height) < 2600:
            raise HTTPException(
                status_code=400,
                detail="Image must be at least 2600 px on the longest side",
            )
        rgb = img.convert("RGB")
        out = io.BytesIO()
        rgb.save(out, format="JPEG", quality=72, dpi=(300, 300))
        return out.getvalue(), width, height, (300, 300)
    except HTTPException:
        raise
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid image file")


def _validate_cover_shape(edition: Dict[str, Any], width: int, height: int) -> None:
    form = (
        f"{edition.get('product_form') or ''} "
        f"{edition.get('product_form_detail') or ''}"
    ).strip().lower()
    is_audio = "audio" in form
    if is_audio and width != height:
        raise HTTPException(status_code=400, detail="Audiobook cover must be square")
    if not is_audio and width == height:
        raise HTTPException(
            status_code=400,
            detail="Print/e-book cover must be portrait; square artwork is reserved for audiobook editions",
        )


def _next_public_sequence(uid: str, isbn13: str, stem: str) -> int:
    prefix = f"{UPLOADS_S3_PREFIX}/{uid}/public/{isbn13}_{stem}_"
    s3 = _s3_client()
    token = None
    used = set()
    while True:
        kwargs = {"Bucket": S3_BUCKET, "Prefix": prefix, "MaxKeys": 1000}
        if token:
            kwargs["ContinuationToken"] = token
        resp = s3.list_objects_v2(**kwargs)
        for obj in resp.get("Contents") or []:
            name = (obj.get("Key") or "").split("/")[-1]
            m = re.search(r"_(\d{2})\.[^.]+$", name)
            if m:
                used.add(int(m.group(1)))
        if not resp.get("IsTruncated"):
            break
        token = resp.get("NextContinuationToken")
        if not token:
            break
    n = 1
    while n in used:
        n += 1
    return n


def _canonical_public_filename(
    kind: str,
    isbn13: str,
    source_name: str,
    uid: str,
    custom_name: str = "",
) -> tuple[str, str]:
    if kind == "book_cover":
        return f"{isbn13}_RetailCover_RetailAndCatalog.jpg", "image/jpeg"
    if kind == "back_cover":
        return f"{isbn13}_RetailCover_BackCover.jpg", "image/jpeg"
    if kind == "interior_image":
        seq = _next_public_sequence(uid, isbn13, "RetailImages_Interior")
        return f"{isbn13}_RetailImages_Interior_{seq:02d}.jpg", "image/jpeg"
    if kind == "full_pdf":
        return f"{isbn13}_FULLPDF.pdf", "application/pdf"
    if kind == "video":
        seq = _next_public_sequence(uid, isbn13, "Video")
        return f"{isbn13}_Video_{seq:02d}.mp4", "video/mp4"
    if kind == "audio":
        seq = _next_public_sequence(uid, isbn13, "Audio")
        return f"{isbn13}_Audio_{seq:02d}.mp3", "audio/mpeg"

    guessed_mime = mimetypes.guess_type(source_name or "")[0] or ""
    ext, mime = _ext_for_upload(source_name, guessed_mime)

    stem = _clean_name(custom_name)
    if not stem:
        raise HTTPException(
            status_code=400,
            detail="Enter a descriptive file name for this Resource Content Type",
        )

    if guessed_mime.startswith("image/"):
        ext, mime = ".jpg", "image/jpeg"

    return f"{isbn13}_{stem}{ext}", mime


def _resource_defaults_from_filename(name: str) -> tuple[str, str, str]:
    n = (name or "").lower()
    if "_retailcover_retailandcatalog." in n:
        return "01", "03", "book_cover"
    if "_retailcover_backcover." in n:
        return "02", "03", "back_cover"
    if "_retailimages_interior_" in n:
        return "20", "03", "interior_image"
    if "_fullpdf.pdf" in n:
        return "32", "06", "full_pdf"
    if "_video_" in n:
        return "22", "04", "video"
    if "_audio_" in n:
        return "15", "02", "audio"
    return "99", "06", "public_other"


def _resource_kind_from_content_type(code: str) -> str:
    code = (code or "").strip()

    if code == "01":
        return "book_cover"
    if code == "02":
        return "back_cover"
    if code == "20":
        return "interior_image"
    if code == "32":
        return "full_pdf"
    if code in {"22", "23"}:
        return "video"
    if code in {"13", "14", "15", "25"}:
        return "audio"

    return "public_other"


def _resource_mode_from_mime(mime: str) -> str:
    mime = (mime or "").lower()
    if mime.startswith("audio/"):
        return "02"
    if mime.startswith("image/"):
        return "03"
    if mime.startswith("video/"):
        return "04"
    return "01"


def _replace_isbn_prefix(filename: str, source_isbn: str, target_isbn: str) -> str:
    name = pathlib.Path(filename or "").name
    source_isbn = _clean_isbn13(source_isbn)
    target_isbn = _clean_isbn13(target_isbn)

    if name.startswith(source_isbn):
        return target_isbn + name[len(source_isbn):]

    if name:
        return f"{target_isbn}_{name}"

    raise HTTPException(status_code=400, detail="Source resource has no usable filename")


def _upsert_supporting_resource(
    *,
    tenant_id: str,
    edition_id: str,
    resource_content_type: str,
    resource_mode: str,
    resource_form: str,
    storage_key: str,
    resource_link: str,
    filename: str,
    mime: str,
    size_bytes: int,
    width_pixels: int | None = None,
    height_pixels: int | None = None,
    caption: str = "",
    credit: str = "",
    territory_countries: str = "",
    cloned_from_resource_id: str | None = None,
) -> tuple[str, str]:
    with db_conn() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                """
                SELECT
                    r.id::text AS resource_id,
                    v.id::text AS version_id
                FROM edition_supporting_resources r
                JOIN edition_supporting_resource_versions v
                  ON v.resource_id = r.id
                 AND v.tenant_id = r.tenant_id
                WHERE r.tenant_id = %s::uuid
                  AND r.edition_id = %s::uuid
                  AND v.storage_key = %s
                LIMIT 1
                """,
                (tenant_id, edition_id, storage_key),
            )
            existing = cur.fetchone()

            if existing:
                resource_id = existing["resource_id"]
                version_id = existing["version_id"]
                cur.execute(
                    """
                    UPDATE edition_supporting_resources
                    SET resource_content_type = %s,
                        content_audience = COALESCE(NULLIF(content_audience, ''), '00'),
                        resource_mode = %s,
                        caption = CASE WHEN %s <> '' THEN %s ELSE caption END,
                        credit = CASE WHEN %s <> '' THEN %s ELSE credit END,
                        territory_countries = CASE
                            WHEN %s <> '' THEN %s
                            ELSE territory_countries
                        END,
                        updated_at = now()
                    WHERE id = %s::uuid
                    """,
                    (
                        resource_content_type,
                        resource_mode,
                        caption, caption,
                        credit, credit,
                        territory_countries, territory_countries,
                        resource_id,
                    ),
                )
                cur.execute(
                    """
                    UPDATE edition_supporting_resource_versions
                    SET resource_form = %s,
                        resource_link = %s,
                        storage_key = %s,
                        filename = %s,
                        file_format = %s,
                        file_size_bytes = %s,
                        width_pixels = %s,
                        height_pixels = %s,
                        updated_at = now()
                    WHERE id = %s::uuid
                    """,
                    (
                        resource_form,
                        resource_link,
                        storage_key,
                        filename,
                        mime,
                        size_bytes,
                        width_pixels,
                        height_pixels,
                        version_id,
                    ),
                )
            else:
                cur.execute(
                    """
                    INSERT INTO edition_supporting_resources (
                        tenant_id, edition_id,
                        resource_content_type, content_audience,
                        territory_countries, caption, credit,
                        resource_mode, cloned_from_resource_id,
                        item_order
                    )
                    VALUES (
                        %s::uuid, %s::uuid,
                        %s, '00',
                        %s, %s, %s,
                        %s, NULLIF(%s, '')::uuid,
                        COALESCE((
                            SELECT max(item_order) + 1
                            FROM edition_supporting_resources
                            WHERE tenant_id = %s::uuid
                              AND edition_id = %s::uuid
                        ), 1)
                    )
                    RETURNING id::text
                    """,
                    (
                        tenant_id, edition_id,
                        resource_content_type,
                        territory_countries, caption, credit,
                        resource_mode, cloned_from_resource_id or "",
                        tenant_id, edition_id,
                    ),
                )
                resource_id = cur.fetchone()["id"]

                cur.execute(
                    """
                    INSERT INTO edition_supporting_resource_versions (
                        tenant_id, resource_id,
                        resource_form, resource_link, storage_key,
                        filename, file_format, file_size_bytes,
                        width_pixels, height_pixels,
                        item_order
                    )
                    VALUES (
                        %s::uuid, %s::uuid,
                        %s, %s, %s,
                        %s, %s, %s,
                        %s, %s,
                        1
                    )
                    RETURNING id::text
                    """,
                    (
                        tenant_id, resource_id,
                        resource_form, resource_link, storage_key,
                        filename, mime, size_bytes,
                        width_pixels, height_pixels,
                    ),
                )
                version_id = cur.fetchone()["id"]

        conn.commit()
    return resource_id, version_id


def _read_public_resources(tenant_id: str, edition_id: str) -> List[Dict[str, Any]]:
    with db_conn() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                """
                SELECT
                    r.id::text AS resource_id,
                    r.resource_content_type,
                    r.content_audience,
                    r.territory_countries,
                    r.caption,
                    r.credit,
                    r.resource_mode,
                    r.cloned_from_resource_id::text AS cloned_from_resource_id,
                    r.item_order AS resource_order,
                    v.id::text AS version_id,
                    v.resource_form,
                    v.resource_link,
                    v.storage_key,
                    v.filename,
                    v.file_format,
                    v.file_size_bytes,
                    v.width_pixels,
                    v.height_pixels,
                    v.duration_seconds,
                    v.language_code,
                    v.content_date_role,
                    v.content_date,
                    v.content_date_format,
                    v.content_date_text,
                    v.item_order AS version_order
                FROM edition_supporting_resources r
                LEFT JOIN edition_supporting_resource_versions v
                  ON v.resource_id = r.id
                 AND v.tenant_id = r.tenant_id
                WHERE r.tenant_id = %s::uuid
                  AND r.edition_id = %s::uuid
                ORDER BY r.item_order, r.created_at, v.item_order, v.created_at
                """,
                (tenant_id, edition_id),
            )
            rows = cur.fetchall() or []

            version_ids = [
                str(row.get("version_id"))
                for row in rows
                if row.get("version_id")
            ]
            features_by_version: Dict[str, Dict[str, Any]] = {}
            if version_ids:
                cur.execute(
                    """
                    SELECT
                        resource_version_id::text AS resource_version_id,
                        feature_type,
                        feature_value,
                        feature_note,
                        item_order
                    FROM edition_supporting_resource_features
                    WHERE tenant_id = %s::uuid
                      AND resource_version_id = ANY(%s::uuid[])
                    ORDER BY resource_version_id, item_order, created_at, id
                    """,
                    (tenant_id, version_ids),
                )
                for feature_row in cur.fetchall() or []:
                    # The card currently exposes one editable version feature pair.
                    # Preserve the first persisted row for card hydration.
                    features_by_version.setdefault(
                        str(feature_row.get("resource_version_id") or ""),
                        feature_row,
                    )

    grouped: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        rid = row["resource_id"]
        item = grouped.setdefault(
            rid,
            {
                "id": rid,
                "resource_content_type": row.get("resource_content_type") or "",
                "resourceContentType": row.get("resource_content_type") or "",
                "content_audience": row.get("content_audience") or "00",
                "contentAudience": row.get("content_audience") or "00",
                "territory_countries": [
                    x for x in re.split(r"[\s,;]+", row.get("territory_countries") or "") if x
                ],
                "territoryCountries": [
                    x for x in re.split(r"[\s,;]+", row.get("territory_countries") or "") if x
                ],
                "caption": row.get("caption") or "",
                "credit": row.get("credit") or "",
                "resource_mode": row.get("resource_mode") or "",
                "resourceMode": row.get("resource_mode") or "",
                "cloned_from_resource_id": row.get("cloned_from_resource_id") or "",
                "versions": [],
                "resource_versions": [],
            },
        )
        if row.get("version_id"):
            v = {
                "id": row["version_id"],
                "resource_form": row.get("resource_form") or "01",
                "resourceForm": row.get("resource_form") or "01",
                "resource_link": row.get("resource_link") or "",
                "resourceLink": row.get("resource_link") or "",
                "url": row.get("resource_link") or "",
                "storage_key": row.get("storage_key") or "",
                "storageKey": row.get("storage_key") or "",
                "filename": row.get("filename") or "",
                "file_format": row.get("file_format") or "",
                "fileFormat": row.get("file_format") or "",
                "file_size_bytes": int(row.get("file_size_bytes") or 0),
                "fileSizeBytes": int(row.get("file_size_bytes") or 0),
                "file_size_kb": str(round(int(row.get("file_size_bytes") or 0) / 1024, 2))
                    if row.get("file_size_bytes") else "",
                "fileSizeKb": str(round(int(row.get("file_size_bytes") or 0) / 1024, 2))
                    if row.get("file_size_bytes") else "",
                "width_pixels": row.get("width_pixels") or "",
                "widthPixels": row.get("width_pixels") or "",
                "height_pixels": row.get("height_pixels") or "",
                "heightPixels": row.get("height_pixels") or "",
                "duration_seconds": row.get("duration_seconds") or "",
                "durationSeconds": row.get("duration_seconds") or "",
                "language_code": row.get("language_code") or "",
                "languageCode": row.get("language_code") or "",
                "content_date_role": row.get("content_date_role") or "",
                "contentDateRole": row.get("content_date_role") or "",
                "content_date": (
                    row.get("content_date").isoformat()
                    if hasattr(row.get("content_date"), "isoformat")
                    else str(row.get("content_date") or "")
                ),
                "contentDate": (
                    row.get("content_date").isoformat()
                    if hasattr(row.get("content_date"), "isoformat")
                    else str(row.get("content_date") or "")
                ),
                "content_date_format": row.get("content_date_format") or "",
                "contentDateFormat": row.get("content_date_format") or "",
                "content_date_text": row.get("content_date_text") or "",
                "contentDateText": row.get("content_date_text") or "",
            }
            feature = features_by_version.get(str(row["version_id"])) or {}
            v["resource_version_feature_type"] = feature.get("feature_type") or ""
            v["resourceVersionFeatureType"] = feature.get("feature_type") or ""
            v["feature_type"] = feature.get("feature_type") or ""
            v["featureType"] = feature.get("feature_type") or ""
            v["resource_version_feature_value"] = feature.get("feature_value") or ""
            v["resourceVersionFeatureValue"] = feature.get("feature_value") or ""
            v["feature_value"] = feature.get("feature_value") or ""
            v["featureValue"] = feature.get("feature_value") or ""
            v["feature_note"] = feature.get("feature_note") or ""
            v["featureNote"] = feature.get("feature_note") or ""
            item["versions"].append(v)
            item["resource_versions"].append(v)

    return list(grouped.values())


def _reconcile_s3_public_resources(
    work: Dict[str, Any],
    edition: Dict[str, Any],
) -> None:
    uid = (work.get("uid") or "").strip()
    isbn = _clean_isbn13(edition.get("isbn13") or "")
    prefix = f"{UPLOADS_S3_PREFIX}/{uid}/public/{isbn}_"
    s3 = _s3_client()
    token = None

    while True:
        kwargs = {"Bucket": S3_BUCKET, "Prefix": prefix, "MaxKeys": 1000}
        if token:
            kwargs["ContinuationToken"] = token
        resp = s3.list_objects_v2(**kwargs)

        for obj in resp.get("Contents") or []:
            key = obj.get("Key") or ""
            if not key or key.endswith("/"):
                continue
            name = key.split("/")[-1]
            code, mode, _kind = _resource_defaults_from_filename(name)
            mime = mimetypes.guess_type(name)[0] or "application/octet-stream"
            width = height = None

            if mime.startswith("image/"):
                try:
                    body = s3.get_object(Bucket=S3_BUCKET, Key=key)["Body"].read()
                    img = Image.open(io.BytesIO(body))
                    width, height = img.size
                except Exception:
                    width = height = None

            _upsert_supporting_resource(
                tenant_id=edition["tenant_id"],
                edition_id=edition["id"],
                resource_content_type=code,
                resource_mode=mode,
                resource_form="01",
                storage_key=key,
                resource_link=_public_url_for_key(key),
                filename=name,
                mime=mime,
                size_bytes=int(obj.get("Size") or 0),
                width_pixels=width,
                height_pixels=height,
            )

        if not resp.get("IsTruncated"):
            break
        token = resp.get("NextContinuationToken")
        if not token:
            break

# ----------------------------
# S3 config for LISTING + PRESIGN
# ----------------------------
AWS_REGION = (os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION") or "us-east-2").strip()
S3_BUCKET = (os.environ.get("S3_BUCKET") or "inksuite-data").strip()

# s3://inksuite-data/tenants/marble-press/data/uploads/<uid>/<uid>__cover.jpg
UPLOADS_S3_PREFIX = (
    os.environ.get("UPLOADS_S3_PREFIX")
    or os.environ.get("UPLOADS_PREFIX")
    or "tenants/marble-press/data/uploads"
).strip().rstrip("/")

USE_UPLOADS_S3 = os.environ.get("USE_UPLOADS_S3", "1").strip().lower() not in ("0", "false", "no")

# If bucket is private, presign MUST be on (recommended)
UPLOADS_USE_PRESIGNED = os.environ.get("UPLOADS_USE_PRESIGNED", "1").strip().lower() not in ("0", "false", "no")
UPLOADS_PRESIGN_EXPIRES = int(os.environ.get("UPLOADS_PRESIGN_EXPIRES", "3600"))

def _s3_client():
    return boto3.client(
        "s3",
        region_name=AWS_REGION,
        endpoint_url=f"https://s3.{AWS_REGION}.amazonaws.com",
        config=Config(
            signature_version="s3v4",
            retries={"max_attempts": 5, "mode": "standard"},
            s3={"addressing_style": "virtual"},
        ),
    )

def _guess_kind(filename: str) -> str:
    f = (filename or "").lower()
    if "__cover" in f or "cover" in f:
        return "cover"
    if "author_contract" in f or "__author_contract" in f:
        return "author_contract"
    if "illustrator_contract" in f or "__illustrator_contract" in f:
        return "illustrator_contract"
    if "author_photo" in f or "__author_photo" in f:
        return "author_photo"
    if "illustrator_photo" in f or "__illustrator_photo" in f:
        return "illustrator_photo"
    if "w9" in f:
        return "w9"
    return "other"

def _is_cover(name: str) -> bool:
    n = (name or "").lower()
    if "__cover." in n:
        return n.endswith((".jpg", ".jpeg", ".png", ".webp", ".gif"))
    if "cover" in n:
        return n.endswith((".jpg", ".jpeg", ".png", ".webp", ".gif"))
    return False

def _s3_url_for_key(key: str) -> str:
    if not UPLOADS_USE_PRESIGNED:
        return f"https://{S3_BUCKET}.s3.{AWS_REGION}.amazonaws.com/{key}"

    s3 = _s3_client()
    return s3.generate_presigned_url(
        "get_object",
        Params={"Bucket": S3_BUCKET, "Key": key},
        ExpiresIn=UPLOADS_PRESIGN_EXPIRES,
        HttpMethod="GET",
    )

def _ext_for_upload(filename: str, mime: str) -> tuple[str, str]:
    fn = (filename or "").lower()

    if mime in ("image/jpeg", "image/jpg") or fn.endswith((".jpg", ".jpeg")):
        return ".jpg", "image/jpeg"
    if mime == "image/png" or fn.endswith(".png"):
        return ".png", "image/png"
    if mime == "image/webp" or fn.endswith(".webp"):
        return ".webp", "image/webp"
    if mime == "application/pdf" or fn.endswith(".pdf"):
        return ".pdf", "application/pdf"

    ext = pathlib.Path(filename or "").suffix.lower()
    return (ext or ".bin"), (mime or "application/octet-stream")

def _resolve_work(candidate: str) -> Dict[str, Any]:
    val = (candidate or "").strip()
    if not val:
        raise HTTPException(status_code=400, detail="bookUid, workId, or book_key is required")

    sql = """
        SELECT id::text AS id, uid::text AS uid, title
        FROM works
        WHERE uid::text = %s OR id::text = %s
        LIMIT 1
    """
    with db_conn() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(sql, (val, val))
            row = cur.fetchone()

    if not row:
        raise HTTPException(status_code=404, detail=f"Work not found for '{val}'")

    return row

def _update_edition_cover(edition_id: str, s3_key: str, content_type: str) -> None:
    """Store the canonical cover pointer on the product/edition, never on works."""
    sql = """
        UPDATE editions
        SET cover_image_link = %s,
            cover_image_format = %s,
            updated_at = now()
        WHERE id = %s::uuid
    """
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (s3_key, content_type, edition_id))
        conn.commit()

# ----------------------------
# Health + listing endpoints
# ----------------------------
@router.get("/health")
def uploads_health():
    return {
        "ok": True,
        "localDir": str(pathlib.Path(DATA_UPLOAD_DIR).resolve()),
        "useS3": USE_UPLOADS_S3,
        "region": AWS_REGION,
        "bucket": S3_BUCKET,
        "uploadsPrefix": UPLOADS_S3_PREFIX,
        "presigned": UPLOADS_USE_PRESIGNED,
        "presignExpires": UPLOADS_PRESIGN_EXPIRES,
        "endpoint": f"https://s3.{AWS_REGION}.amazonaws.com",
        "addressingStyle": "virtual",
    }

@router.get("/book-assets")
def list_book_assets(bookUid: str = Query(...)):
    uid = (bookUid or "").strip()
    if not uid:
        raise HTTPException(status_code=400, detail="bookUid is required")

    if USE_UPLOADS_S3:
        if not S3_BUCKET or not UPLOADS_S3_PREFIX:
            raise HTTPException(
                status_code=500,
                detail="Uploads S3 config missing (need S3_BUCKET and UPLOADS_S3_PREFIX/UPLOADS_PREFIX)",
            )

        prefix = f"{UPLOADS_S3_PREFIX}/{uid}/"
        s3 = _s3_client()

        try:
            files: List[Dict[str, Any]] = []
            cover: Optional[Dict[str, Any]] = None
            token: Optional[str] = None

            while True:
                kwargs: Dict[str, Any] = {"Bucket": S3_BUCKET, "Prefix": prefix, "MaxKeys": 1000}
                if token:
                    kwargs["ContinuationToken"] = token

                resp = s3.list_objects_v2(**kwargs)

                for obj in (resp.get("Contents") or []):
                    key = obj.get("Key") or ""
                    if not key or key.endswith("/"):
                        continue

                    name = key.split("/")[-1]
                    size = int(obj.get("Size") or 0)

                    url = _s3_url_for_key(key)
                    item = {
                        "name": name,
                        "filename": name,
                        "kind": _guess_kind(name),
                        "url": url,
                        "size": size,
                    }

                    if _is_cover(name) and cover is None:
                        cover = item
                    else:
                        files.append(item)

                if resp.get("IsTruncated"):
                    token = resp.get("NextContinuationToken")
                    if not token:
                        break
                else:
                    break

            return {"bookUid": uid, "cover": cover, "files": files}

        except (EndpointConnectionError, NoCredentialsError) as e:
            raise HTTPException(status_code=500, detail=f"S3 auth/connection error: {e}")
        except ClientError as e:
            code = e.response.get("Error", {}).get("Code", "")
            msg = e.response.get("Error", {}).get("Message", "")
            raise HTTPException(status_code=500, detail=f"S3 client error {code}: {msg or str(e)}")
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

    local_dir = pathlib.Path(DATA_UPLOAD_DIR) / uid
    if local_dir.exists() and local_dir.is_dir():
        files: List[Dict[str, Any]] = []
        cover: Optional[Dict[str, Any]] = None

        for p in sorted(local_dir.rglob("*")):
            if not p.is_file():
                continue
            rel = p.relative_to(pathlib.Path(DATA_UPLOAD_DIR)).as_posix()
            name = p.name
            url = f"/static/uploads/{rel}"
            size = int(p.stat().st_size)

            if _is_cover(name) and cover is None:
                cover = {"name": name, "url": url, "size": size}
            else:
                files.append({"name": name, "filename": name, "kind": _guess_kind(name), "url": url, "size": size})

        return {"bookUid": uid, "cover": cover, "files": files}

    raise HTTPException(
        status_code=500,
        detail=f"Uploads not available. S3 disabled and local folder missing: {str(local_dir)}",
    )




@router.get("/public-resources")
def list_public_resources(
    bookUid: str = Query(...),
    editionId: str = Query(""),
    isbn13: str = Query(...),
    includeEditions: bool = Query(True),
):
    work = _resolve_work(bookUid)
    edition = _resolve_edition(str(work["id"]), editionId, isbn13)

    if USE_UPLOADS_S3:
        _reconcile_s3_public_resources(work, edition)

    payload = {
        "bookUid": str(work.get("uid") or ""),
        "workId": str(work.get("id") or ""),
        "editionId": edition["id"],
        "isbn13": edition["isbn13"],
        "resources": _read_public_resources(edition["tenant_id"], edition["id"]),
    }

    if includeEditions:
        with db_conn() as conn:
            with conn.cursor(row_factory=dict_row) as cur:
                cur.execute(
                    """
                    SELECT
                        id::text AS id,
                        isbn13,
                        product_form,
                        product_form_detail
                    FROM editions
                    WHERE tenant_id = %s::uuid
                      AND work_id = %s::uuid
                      AND isbn13 IS NOT NULL
                      AND trim(isbn13) <> ''
                    ORDER BY created_at, id
                    """,
                    (edition["tenant_id"], work["id"]),
                )
                other_editions = cur.fetchall() or []

        payload["availableEditions"] = [
            {
                "editionId": row["id"],
                "isbn13": row.get("isbn13") or "",
                "productForm":
                    row.get("product_form")
                    or row.get("product_form_detail")
                    or "",
            }
            for row in other_editions
            if row["id"] != edition["id"]
        ]


    return payload


@router.post("/public-resources/metadata")
def save_public_resource_metadata(payload: Dict[str, Any] = Body(...)):
    resource_id = str(payload.get("resourceId") or payload.get("id") or "").strip()
    if not resource_id:
        raise HTTPException(status_code=400, detail="resourceId is required")

    territory = payload.get("territoryCountries") or payload.get("territory_countries") or []
    if isinstance(territory, list):
        territory_text = " ".join(str(x).strip().upper() for x in territory if str(x).strip())
    else:
        territory_text = str(territory or "").strip()

    versions = payload.get("versions") or payload.get("resource_versions") or []
    if not isinstance(versions, list):
        versions = []

    with db_conn() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                """
                UPDATE edition_supporting_resources
                SET resource_content_type = COALESCE(NULLIF(%s, ''), resource_content_type),
                    content_audience = COALESCE(NULLIF(%s, ''), content_audience),
                    territory_countries = %s,
                    caption = %s,
                    credit = %s,
                    resource_mode = COALESCE(NULLIF(%s, ''), resource_mode),
                    updated_at = now()
                WHERE id = %s::uuid
                RETURNING edition_id::text AS edition_id, tenant_id::text AS tenant_id
                """,
                (
                    str(payload.get("resourceContentType") or payload.get("resource_content_type") or ""),
                    str(payload.get("contentAudience") or payload.get("content_audience") or ""),
                    territory_text,
                    str(payload.get("caption") or ""),
                    str(payload.get("credit") or ""),
                    str(payload.get("resourceMode") or payload.get("resource_mode") or ""),
                    resource_id,
                ),
            )
            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Supporting resource not found")

            tenant_id = str(row["tenant_id"])

            for version in versions:
                if not isinstance(version, dict):
                    continue
                version_id = str(version.get("id") or "").strip()
                if not version_id:
                    continue

                duration_seconds = str(
                    version.get("duration_seconds")
                    or version.get("durationSeconds")
                    or ""
                ).strip()
                language_code = str(
                    version.get("language_code")
                    or version.get("languageCode")
                    or ""
                ).strip()
                content_date_role = str(
                    version.get("content_date_role")
                    or version.get("contentDateRole")
                    or ""
                ).strip()
                content_date = str(
                    version.get("content_date")
                    or version.get("contentDate")
                    or ""
                ).strip()
                content_date_format = str(
                    version.get("content_date_format")
                    or version.get("contentDateFormat")
                    or ("00" if content_date else "")
                ).strip()
                content_date_text = str(
                    version.get("content_date_text")
                    or version.get("contentDateText")
                    or (content_date.replace("-", "") if content_date else "")
                ).strip()

                cur.execute(
                    """
                    UPDATE edition_supporting_resource_versions
                    SET duration_seconds = NULLIF(%s, '')::numeric,
                        language_code = NULLIF(%s, ''),
                        content_date_role = NULLIF(%s, ''),
                        content_date = NULLIF(%s, '')::date,
                        content_date_format = NULLIF(%s, ''),
                        content_date_text = NULLIF(%s, ''),
                        updated_at = now()
                    WHERE tenant_id = %s::uuid
                      AND resource_id = %s::uuid
                      AND id = %s::uuid
                    """,
                    (
                        duration_seconds,
                        language_code,
                        content_date_role,
                        content_date,
                        content_date_format,
                        content_date_text,
                        tenant_id,
                        resource_id,
                        version_id,
                    ),
                )

                feature_type = str(
                    version.get("resource_version_feature_type")
                    or version.get("resourceVersionFeatureType")
                    or version.get("feature_type")
                    or version.get("featureType")
                    or ""
                ).strip()
                feature_value = str(
                    version.get("resource_version_feature_value")
                    or version.get("resourceVersionFeatureValue")
                    or version.get("feature_value")
                    or version.get("featureValue")
                    or ""
                ).strip()
                feature_note = str(
                    version.get("feature_note")
                    or version.get("featureNote")
                    or ""
                ).strip()

                cur.execute(
                    """
                    DELETE FROM edition_supporting_resource_features
                    WHERE tenant_id = %s::uuid
                      AND resource_version_id = %s::uuid
                    """,
                    (tenant_id, version_id),
                )

                if feature_type or feature_value or feature_note:
                    cur.execute(
                        """
                        INSERT INTO edition_supporting_resource_features (
                            tenant_id,
                            resource_version_id,
                            feature_type,
                            feature_value,
                            feature_note,
                            item_order
                        )
                        VALUES (%s::uuid, %s::uuid, %s, %s, %s, 1)
                        """,
                        (
                            tenant_id,
                            version_id,
                            feature_type,
                            feature_value,
                            feature_note,
                        ),
                    )

        conn.commit()

    return {"ok": True, "resourceId": resource_id}



def _sync_edition_cover_from_resources(tenant_id: str, edition_id: str) -> None:
    """
    Keep editions.cover_image_* aligned with the currently stored front-cover
    supporting resource. Prefer InkSuite-managed storage keys; fall back to a
    stable external URL only when no managed front cover exists.
    """
    with db_conn() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                """
                SELECT
                    NULLIF(trim(v.storage_key), '') AS storage_key,
                    NULLIF(trim(v.resource_link), '') AS resource_link,
                    NULLIF(trim(v.file_format), '') AS file_format
                FROM edition_supporting_resources r
                JOIN edition_supporting_resource_versions v
                  ON v.resource_id = r.id
                 AND v.tenant_id = r.tenant_id
                WHERE r.tenant_id = %s::uuid
                  AND r.edition_id = %s::uuid
                  AND r.resource_content_type = '01'
                  AND (
                      NULLIF(trim(v.storage_key), '') IS NOT NULL
                      OR NULLIF(trim(v.resource_link), '') IS NOT NULL
                  )
                ORDER BY
                    CASE
                        WHEN NULLIF(trim(v.storage_key), '') IS NOT NULL THEN 0
                        ELSE 1
                    END,
                    r.item_order,
                    v.item_order,
                    r.created_at,
                    v.created_at
                LIMIT 1
                """,
                (tenant_id, edition_id),
            )
            row = cur.fetchone()

            if row:
                pointer = (
                    str(row.get("storage_key") or "").strip()
                    or str(row.get("resource_link") or "").strip()
                )
                file_format = str(row.get("file_format") or "").strip()
            else:
                pointer = ""
                file_format = ""

            cur.execute(
                """
                UPDATE editions
                SET cover_image_link = %s,
                    cover_image_format = %s,
                    updated_at = now()
                WHERE tenant_id = %s::uuid
                  AND id = %s::uuid
                """,
                (pointer, file_format, tenant_id, edition_id),
            )
        conn.commit()


def _delete_owned_public_s3_key(key: str, uid: str) -> bool:
    """
    Delete only objects that belong to this work's InkSuite public folder.
    External ONIX URLs never have a storage_key and are therefore untouched.
    """
    key = str(key or "").strip()
    uid = str(uid or "").strip()
    if not key or not uid or not USE_UPLOADS_S3:
        return False

    expected_prefix = f"{UPLOADS_S3_PREFIX}/{uid}/public/"
    if not key.startswith(expected_prefix):
        return False

    try:
        _s3_client().delete_object(Bucket=S3_BUCKET, Key=key)
        return True
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code", "")
        if code in {"NoSuchKey", "404"}:
            return True
        raise HTTPException(
            status_code=500,
            detail=f"S3 delete failed for {key}: {exc}",
        )
    except (EndpointConnectionError, NoCredentialsError) as exc:
        raise HTTPException(
            status_code=500,
            detail=f"S3 auth/connection error while deleting {key}: {exc}",
        )


@router.delete("/public-resources/{resource_id}")
def delete_public_resource(
    resource_id: str,
    bookUid: str = Query(...),
):
    """
    Delete one normalized SupportingResource.

    - InkSuite-managed files (version.storage_key present) are removed from the
      work's public S3 folder.
    - Imported/external resources have no storage_key, so only InkSuite's SQL
      metadata is removed. The external remote file is never touched.
    """
    work = _resolve_work(bookUid)
    work_id = str(work.get("id") or "").strip()
    uid = str(work.get("uid") or "").strip()

    with db_conn() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                """
                SELECT
                    r.id::text AS resource_id,
                    r.tenant_id::text AS tenant_id,
                    r.edition_id::text AS edition_id,
                    r.resource_content_type,
                    v.id::text AS version_id,
                    v.storage_key
                FROM edition_supporting_resources r
                JOIN editions e
                  ON e.id = r.edition_id
                 AND e.tenant_id = r.tenant_id
                LEFT JOIN edition_supporting_resource_versions v
                  ON v.resource_id = r.id
                 AND v.tenant_id = r.tenant_id
                WHERE r.id = %s::uuid
                  AND e.work_id = %s::uuid
                ORDER BY v.item_order, v.created_at
                """,
                (resource_id, work_id),
            )
            rows = cur.fetchall() or []

            if not rows:
                raise HTTPException(
                    status_code=404,
                    detail="Supporting resource not found for this work",
                )

            tenant_id = str(rows[0]["tenant_id"])
            edition_id = str(rows[0]["edition_id"])
            resource_type = str(rows[0].get("resource_content_type") or "")
            version_ids = [
                str(row["version_id"])
                for row in rows
                if row.get("version_id")
            ]
            storage_keys = [
                str(row.get("storage_key") or "").strip()
                for row in rows
                if str(row.get("storage_key") or "").strip()
            ]

            if version_ids:
                cur.execute(
                    """
                    DELETE FROM edition_supporting_resource_features
                    WHERE tenant_id = %s::uuid
                      AND resource_version_id = ANY(%s::uuid[])
                    """,
                    (tenant_id, version_ids),
                )

            cur.execute(
                """
                DELETE FROM edition_supporting_resource_parent_features
                WHERE tenant_id = %s::uuid
                  AND resource_id = %s::uuid
                """,
                (tenant_id, resource_id),
            )

            cur.execute(
                """
                DELETE FROM edition_supporting_resource_versions
                WHERE tenant_id = %s::uuid
                  AND resource_id = %s::uuid
                """,
                (tenant_id, resource_id),
            )

            cur.execute(
                """
                DELETE FROM edition_supporting_resources
                WHERE tenant_id = %s::uuid
                  AND id = %s::uuid
                """,
                (tenant_id, resource_id),
            )

        conn.commit()

    deleted_keys = []
    for key in storage_keys:
        if _delete_owned_public_s3_key(key, uid):
            deleted_keys.append(key)

    if resource_type == "01":
        _sync_edition_cover_from_resources(tenant_id, edition_id)

    return {
        "ok": True,
        "resourceId": resource_id,
        "editionId": edition_id,
        "externalOnly": len(storage_keys) == 0,
        "deletedStorageKeys": deleted_keys,
    }


@router.post("/public-resources/{resource_id}/replace", response_model=UploadResponse)
async def replace_public_resource(
    resource_id: str,
    file: UploadFile = File(...),
    bookUid: str = Form(...),
    editionId: str = Form(""),
    isbn13: str = Form(""),
    versionId: str = Form(""),
):
    """
    Replace the physical file for an existing SupportingResource while keeping
    the resource-level ONIX metadata and resource identity.

    Imported external resources are converted into InkSuite-managed public
    resources. Their old remote URL is not touched.
    """
    work = _resolve_work(bookUid)
    uid = str(work.get("uid") or "").strip()
    work_id = str(work.get("id") or "").strip()
    if not uid:
        raise HTTPException(status_code=400, detail="Work UID is required")

    edition = _resolve_edition(work_id, editionId, isbn13)

    with db_conn() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                """
                SELECT
                    r.id::text AS resource_id,
                    r.tenant_id::text AS tenant_id,
                    r.edition_id::text AS edition_id,
                    r.resource_content_type,
                    r.resource_mode,
                    r.caption,
                    v.id::text AS version_id,
                    v.resource_form,
                    v.storage_key AS old_storage_key,
                    v.filename AS old_filename,
                    v.file_format AS old_file_format
                FROM edition_supporting_resources r
                JOIN editions e
                  ON e.id = r.edition_id
                 AND e.tenant_id = r.tenant_id
                LEFT JOIN edition_supporting_resource_versions v
                  ON v.resource_id = r.id
                 AND v.tenant_id = r.tenant_id
                WHERE r.id = %s::uuid
                  AND e.work_id = %s::uuid
                  AND r.edition_id = %s::uuid
                  AND (
                      %s = ''
                      OR v.id = %s::uuid
                  )
                ORDER BY v.item_order, v.created_at
                LIMIT 1
                """,
                (
                    resource_id,
                    work_id,
                    edition["id"],
                    versionId.strip(),
                    versionId.strip() or None,
                ),
            )
            existing = cur.fetchone()

    if not existing:
        raise HTTPException(
            status_code=404,
            detail="Supporting resource/version not found for this edition",
        )

    resource_type = str(existing.get("resource_content_type") or "99").strip()
    effective_kind = _resource_kind_from_content_type(resource_type)

    mime = (file.content_type or mimetypes.guess_type(file.filename or "")[0] or "").lower()
    data = await file.read()
    if not data:
        raise HTTPException(status_code=400, detail="Empty file")

    width = height = None
    dpi_tuple = None
    upload_bytes = data
    normalized_mime = mime or "application/octet-stream"

    # Use the resource type as the canonical naming basis. For generic ONIX
    # resources, derive a stable descriptive name from the content-type label
    # code rather than retaining a foreign provider's filename.
    custom_name = f"SupportingResource_{resource_type}"
    canonical_name, canonical_mime = _canonical_public_filename(
        effective_kind,
        _clean_isbn13(edition.get("isbn13") or isbn13),
        file.filename or "",
        uid,
        custom_name,
    )
    normalized_mime = canonical_mime or normalized_mime

    if mime.startswith("video/") and mime != "video/mp4":
        raise HTTPException(status_code=400, detail="Video files must be MP4")
    if mime.startswith("audio/") and mime not in {"audio/mpeg", "audio/mp3"}:
        raise HTTPException(status_code=400, detail="Audio files must be MP3")
    if effective_kind == "full_pdf" and mime != "application/pdf":
        raise HTTPException(status_code=400, detail="Full content must be uploaded as PDF")

    if mime.startswith("image/"):
        upload_bytes, width, height, dpi_tuple = _normalize_public_image(data)
        normalized_mime = "image/jpeg"
        if effective_kind == "book_cover":
            _validate_cover_shape(edition, int(width or 0), int(height or 0))

    size = len(upload_bytes)
    new_key = _public_storage_key(uid, canonical_name)
    old_key = str(existing.get("old_storage_key") or "").strip()

    try:
        _s3_client().put_object(
            Bucket=S3_BUCKET,
            Key=new_key,
            Body=upload_bytes,
            ContentType=normalized_mime,
        )
    except (EndpointConnectionError, NoCredentialsError) as exc:
        raise HTTPException(status_code=500, detail=f"S3 auth/connection error: {exc}")
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code", "")
        msg = exc.response.get("Error", {}).get("Message", "")
        raise HTTPException(
            status_code=500,
            detail=f"S3 client error {code}: {msg or str(exc)}",
        )

    try:
        with db_conn() as conn:
            with conn.cursor(row_factory=dict_row) as cur:
                target_version_id = str(existing.get("version_id") or "").strip()

                if target_version_id:
                    cur.execute(
                        """
                        UPDATE edition_supporting_resource_versions
                        SET resource_form = COALESCE(NULLIF(resource_form, ''), '01'),
                            resource_link = %s,
                            storage_key = %s,
                            filename = %s,
                            file_format = %s,
                            file_size_bytes = %s,
                            width_pixels = %s,
                            height_pixels = %s,
                            updated_at = now()
                        WHERE tenant_id = %s::uuid
                          AND id = %s::uuid
                          AND resource_id = %s::uuid
                        """,
                        (
                            _public_url_for_key(new_key),
                            new_key,
                            canonical_name,
                            normalized_mime,
                            size,
                            width,
                            height,
                            edition["tenant_id"],
                            target_version_id,
                            resource_id,
                        ),
                    )
                else:
                    cur.execute(
                        """
                        INSERT INTO edition_supporting_resource_versions (
                            tenant_id, resource_id,
                            resource_form, resource_link, storage_key,
                            filename, file_format, file_size_bytes,
                            width_pixels, height_pixels, item_order
                        )
                        VALUES (
                            %s::uuid, %s::uuid,
                            '01', %s, %s,
                            %s, %s, %s,
                            %s, %s, 1
                        )
                        RETURNING id::text
                        """,
                        (
                            edition["tenant_id"],
                            resource_id,
                            _public_url_for_key(new_key),
                            new_key,
                            canonical_name,
                            normalized_mime,
                            size,
                            width,
                            height,
                        ),
                    )
                    target_version_id = cur.fetchone()["id"]

                # Keep mode aligned with the replacement file.
                cur.execute(
                    """
                    UPDATE edition_supporting_resources
                    SET resource_mode = %s,
                        updated_at = now()
                    WHERE tenant_id = %s::uuid
                      AND id = %s::uuid
                    """,
                    (
                        _resource_mode_from_mime(normalized_mime),
                        edition["tenant_id"],
                        resource_id,
                    ),
                )

            conn.commit()
    except Exception:
        if new_key != old_key:
            try:
                _delete_owned_public_s3_key(new_key, uid)
            except Exception:
                pass
        raise

    if old_key and old_key != new_key:
        _delete_owned_public_s3_key(old_key, uid)

    if resource_type == "01":
        _sync_edition_cover_from_resources(
            str(edition["tenant_id"]),
            str(edition["id"]),
        )

    return UploadResponse(
        ok=True,
        url=_public_url_for_key(new_key),
        filename=canonical_name,
        mime=normalized_mime,
        size=size,
        width=width,
        height=height,
        dpi=dpi_tuple,
        key=new_key,
        bookUid=uid,
        workId=work_id,
        editionId=str(edition["id"]),
        isbn13=_clean_isbn13(edition.get("isbn13") or isbn13),
        resourceId=resource_id,
        resourceVersionId=target_version_id,
    )


@router.post("/public-resources/clone")
def clone_public_resource(payload: Dict[str, Any] = Body(...)):
    book_uid = str(payload.get("bookUid") or "").strip()
    source_resource_id = str(payload.get("sourceResourceId") or "").strip()
    source_storage_key = str(payload.get("sourceStorageKey") or "").strip()
    target_edition_id = str(payload.get("targetEditionId") or "").strip()
    target_isbn = _clean_isbn13(str(payload.get("targetIsbn13") or ""))

    if not book_uid or not source_resource_id or not source_storage_key:
        raise HTTPException(
            status_code=400,
            detail="bookUid, sourceResourceId and sourceStorageKey are required",
        )

    work = _resolve_work(book_uid)
    uid = str(work["uid"] or "").strip()
    target_edition = _resolve_edition(
        str(work["id"]),
        target_edition_id,
        target_isbn,
    )

    with db_conn() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                """
                SELECT
                    r.id::text AS resource_id,
                    r.resource_content_type,
                    r.content_audience,
                    r.territory_countries,
                    r.caption,
                    r.credit,
                    r.resource_mode,
                    v.resource_form,
                    v.storage_key,
                    v.filename,
                    v.file_format,
                    v.file_size_bytes,
                    v.width_pixels,
                    v.height_pixels,
                    v.duration_seconds,
                    v.language_code,
                    e.isbn13 AS source_isbn
                FROM edition_supporting_resources r
                JOIN edition_supporting_resource_versions v
                  ON v.resource_id = r.id
                 AND v.tenant_id = r.tenant_id
                JOIN editions e
                  ON e.id = r.edition_id
                 AND e.tenant_id = r.tenant_id
                WHERE r.id = %s::uuid
                  AND v.storage_key = %s
                LIMIT 1
                """,
                (source_resource_id, source_storage_key),
            )
            source = cur.fetchone()

    if not source:
        raise HTTPException(status_code=404, detail="Source supporting resource not found")

    source_filename = (
        source.get("filename")
        or pathlib.Path(source_storage_key).name
    )
    target_filename = _replace_isbn_prefix(
        source_filename,
        source.get("source_isbn") or "",
        target_isbn,
    )
    target_key = _public_storage_key(uid, target_filename)
    resource_type = str(source.get("resource_content_type") or "99")
    source_mime = str(
        source.get("file_format")
        or mimetypes.guess_type(source_filename)[0]
        or "application/octet-stream"
    )

    try:
        s3 = _s3_client()
        source_obj = s3.get_object(Bucket=S3_BUCKET, Key=source_storage_key)
        source_bytes = source_obj["Body"].read()

        if resource_type == "01":
            img = Image.open(io.BytesIO(source_bytes))
            width, height = img.size
            _validate_cover_shape(target_edition, width, height)

        s3.put_object(
            Bucket=S3_BUCKET,
            Key=target_key,
            Body=source_bytes,
            ContentType=source_mime,
        )
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Resource clone failed: {exc}")

    resource_id, version_id = _upsert_supporting_resource(
        tenant_id=target_edition["tenant_id"],
        edition_id=target_edition["id"],
        resource_content_type=resource_type,
        resource_mode=str(
            source.get("resource_mode")
            or _resource_mode_from_mime(source_mime)
        ),
        resource_form=str(source.get("resource_form") or "01"),
        storage_key=target_key,
        resource_link=_public_url_for_key(target_key),
        filename=target_filename,
        mime=source_mime,
        size_bytes=len(source_bytes),
        width_pixels=source.get("width_pixels"),
        height_pixels=source.get("height_pixels"),
        caption=str(source.get("caption") or ""),
        credit=str(source.get("credit") or ""),
        territory_countries=str(source.get("territory_countries") or ""),
        cloned_from_resource_id=source_resource_id,
    )

    return {
        "ok": True,
        "editionId": target_edition["id"],
        "isbn13": target_isbn,
        "resourceId": resource_id,
        "resourceVersionId": version_id,
        "key": target_key,
        "url": _public_url_for_key(target_key),
        "filename": target_filename,
    }


@router.delete("")
@router.delete("/")
def delete_upload(
    bookKey: str = Query(""),
    url: str | None = Query(None),
    filename: str | None = Query(None),
):
    """
    Delete an uploaded asset for a book.
    - When S3 is enabled, delete the object from S3.
    - When using local disk, delete from DATA_UPLOAD_DIR.
    The frontend sends either a full S3 URL (url) or a filename.
    """
    candidate = (bookKey or "").strip()
    if not candidate:
        raise HTTPException(status_code=400, detail="bookKey is required")

    if USE_UPLOADS_S3:
        # Derive S3 key from full URL when provided.
        if url:
            parsed = urlparse(url)
            key = parsed.path.lstrip("/")
        else:
            # Fallback: assume standard key layout under uploads prefix.
            fn = (filename or "").strip()
            if not fn:
                raise HTTPException(status_code=400, detail="filename or url is required")
            key = f"{UPLOADS_S3_PREFIX}/{candidate}/{fn}"

        try:
            s3 = _s3_client()
            s3.delete_object(Bucket=S3_BUCKET, Key=key)
        except (EndpointConnectionError, NoCredentialsError) as e:
            raise HTTPException(status_code=500, detail=f"S3 auth/connection error: {e}")
        except ClientError as e:
            code = e.response.get("Error", {}).get("Code", "")
            msg = e.response.get("Error", {}).get("Message", "")
            # If already gone, treat as success.
            if code not in ("NoSuchKey", "404"):
                raise HTTPException(status_code=500, detail=f"S3 client error {code}: {msg or str(e)}")

        return {"ok": True}

    # Local disk fallback
    safe_dir = pathlib.Path(DATA_UPLOAD_DIR) / _clean_name(candidate or "_")
    if not safe_dir.exists():
        return {"ok": True}

    target: pathlib.Path | None = None
    if filename:
        cand = safe_dir / filename
        if cand.exists() and cand.is_file():
            target = cand
    elif url:
        # URL like /static/uploads/<key>/<filename>
        parsed = urlparse(url)
        name = pathlib.Path(parsed.path).name
        cand = safe_dir / name
        if cand.exists() and cand.is_file():
            target = cand

    if target and target.exists():
        try:
            target.unlink()
        except Exception:
            pass

    return {"ok": True}

# ----------------------------
# Upload endpoint
# ----------------------------
# ----------------------------
# Upload endpoint
# ----------------------------
@router.post("", response_model=UploadResponse)
@router.post("/", response_model=UploadResponse)
async def upload_file(
    file: UploadFile = File(...),
    kind: UploadKind = Form(...),
    book_key: str = Form(""),
    bookUid: str = Form(""),
    workId: str = Form(""),
    editionId: str = Form(""),
    isbn13: str = Form(""),
    resourceContentType: str = Form(""),
    resourceMode: str = Form(""),
    resourceForm: str = Form("01"),
    caption: str = Form(""),
    customName: str = Form(""),
):
    allowed = ALLOWED.get(kind, set())
    mime = (file.content_type or "").strip().lower()
    if allowed and mime not in allowed:
        raise HTTPException(status_code=400, detail=f"Unsupported type for {kind}: {mime}")

    data = await file.read()
    size = len(data)
    if size == 0:
        raise HTTPException(status_code=400, detail="Empty file")

    width = height = None
    dpi_tuple = None
    if mime.startswith("image/"):
        try:
            img = Image.open(io.BytesIO(data))
            width, height = img.size
            if "dpi" in img.info and isinstance(img.info["dpi"], tuple):
                dpi_tuple = tuple(int(round(x)) for x in img.info["dpi"])
            # Public images are normalized below to RGB JPEG with 300 DPI metadata.
        except HTTPException:
            raise
        except Exception:
            raise HTTPException(status_code=400, detail="Invalid image file")

    candidate = (bookUid or workId or book_key or "").strip()
    if not candidate:
        raise HTTPException(status_code=400, detail="bookUid, workId, or book_key is required")

    if kind in (PUBLIC_RESOURCE_KINDS | {"public_resource"}) and USE_UPLOADS_S3:
        if not resourceContentType:
            raise HTTPException(status_code=400, detail="Resource Content Type is required")

        effective_kind = (
            _resource_kind_from_content_type(resourceContentType)
            if kind == "public_resource"
            else kind
        )

        work = _resolve_work(candidate)
        uid = (work["uid"] or "").strip()
        resolved_work_id = (work["id"] or "").strip()
        edition = _resolve_edition(resolved_work_id, editionId, isbn13)
        resolved_isbn = _clean_isbn13(edition.get("isbn13") or isbn13)

        if not uid:
            raise HTTPException(status_code=500, detail="Resolved work has empty uid")

        canonical_name, normalized_mime = _canonical_public_filename(
            effective_kind,
            resolved_isbn,
            file.filename or "",
            uid,
            customName,
        )

        if mime.startswith("video/") and mime != "video/mp4":
            raise HTTPException(status_code=400, detail="Video files must be MP4")
        if mime.startswith("audio/") and mime not in {"audio/mpeg", "audio/mp3"}:
            raise HTTPException(status_code=400, detail="Audio files must be MP3")

        if effective_kind == "video" and mime != "video/mp4":
            raise HTTPException(status_code=400, detail="This Resource Content Type requires an MP4 file")
        if effective_kind == "audio" and mime not in {"audio/mpeg", "audio/mp3"}:
            raise HTTPException(status_code=400, detail="This Resource Content Type requires an MP3 file")
        if effective_kind == "full_pdf" and mime != "application/pdf":
            raise HTTPException(status_code=400, detail="Full content must be uploaded as PDF")

        upload_bytes = data
        if mime.startswith("image/"):
            upload_bytes, width, height, dpi_tuple = _normalize_public_image(data)
            normalized_mime = "image/jpeg"
            size = len(upload_bytes)

            if effective_kind == "book_cover":
                _validate_cover_shape(edition, int(width or 0), int(height or 0))

        s3_key = _public_storage_key(uid, canonical_name)

        try:
            s3 = _s3_client()
            s3.put_object(
                Bucket=S3_BUCKET,
                Key=s3_key,
                Body=upload_bytes,
                ContentType=normalized_mime,
            )
        except (EndpointConnectionError, NoCredentialsError) as e:
            raise HTTPException(status_code=500, detail=f"S3 auth/connection error: {e}")
        except ClientError as e:
            code = e.response.get("Error", {}).get("Code", "")
            msg = e.response.get("Error", {}).get("Message", "")
            raise HTTPException(status_code=500, detail=f"S3 client error {code}: {msg or str(e)}")

        default_code, _default_mode, _ = _resource_defaults_from_filename(canonical_name)
        automatic_mode = _resource_mode_from_mime(normalized_mime or mime)

        resource_id, version_id = _upsert_supporting_resource(
            tenant_id=edition["tenant_id"],
            edition_id=edition["id"],
            resource_content_type=(resourceContentType or default_code).strip(),
            resource_mode=(resourceMode or automatic_mode).strip(),
            resource_form=(resourceForm or "01").strip(),
            storage_key=s3_key,
            resource_link=_public_url_for_key(s3_key),
            filename=canonical_name,
            mime=normalized_mime,
            size_bytes=size,
            width_pixels=width,
            height_pixels=height,
            caption=(caption or "").strip(),
        )

        if effective_kind == "book_cover":
            _update_edition_cover(str(edition["id"]), s3_key, normalized_mime)

        return UploadResponse(
            ok=True,
            url=_public_url_for_key(s3_key),
            filename=canonical_name,
            mime=normalized_mime,
            size=size,
            width=width,
            height=height,
            dpi=dpi_tuple,
            key=s3_key,
            bookUid=uid,
            workId=resolved_work_id,
            editionId=edition["id"],
            isbn13=resolved_isbn,
            resourceId=resource_id,
            resourceVersionId=version_id,
        )

    # Book covers, interior images/pages, and full PDFs/ARCs are handled above
    # as public resources and must never fall through to the title-private folder.

    # Upload contracts and W-9 to the title-private S3 folder when S3 is enabled.
    if kind in ("author_contract", "illustrator_contract", "w9") and USE_UPLOADS_S3:
        work = _resolve_work(candidate)
        uid = (work["uid"] or "").strip()
        resolved_work_id = (work["id"] or "").strip()

        if not uid:
            raise HTTPException(status_code=500, detail="Resolved work has empty uid")

        ext, normalized_mime = _ext_for_upload(file.filename or "", mime)
        if kind == "author_contract":
            fname = f"{uid}__author_contract{ext}"
        elif kind == "illustrator_contract":
            fname = f"{uid}__illustrator_contract{ext}"
        else:  # w9
            fname = f"{uid}__w9{ext}"

        s3_key = f"{UPLOADS_S3_PREFIX}/{uid}/{fname}"

        try:
            s3 = _s3_client()
            s3.put_object(
                Bucket=S3_BUCKET,
                Key=s3_key,
                Body=data,
                ContentType=normalized_mime,
            )
            url = _s3_url_for_key(s3_key)
        except (EndpointConnectionError, NoCredentialsError) as e:
            raise HTTPException(status_code=500, detail=f"S3 auth/connection error: {e}")
        except ClientError as e:
            code = e.response.get("Error", {}).get("Code", "")
            msg = e.response.get("Error", {}).get("Message", "")
            raise HTTPException(status_code=500, detail=f"S3 client error {code}: {msg or str(e)}")
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

        return UploadResponse(
            ok=True,
            url=url,
            filename=file.filename or fname,
            mime=normalized_mime,
            size=size,
            width=width,
            height=height,
            dpi=dpi_tuple,
            key=s3_key,
            bookUid=uid,
            workId=resolved_work_id,
        )

    # Upload author/illustrator photos to the title-private S3 folder when S3 is enabled.
    if kind in ("author_photo", "illustrator_photo") and USE_UPLOADS_S3:
        work = _resolve_work(candidate)
        uid = (work["uid"] or "").strip()
        resolved_work_id = (work["id"] or "").strip()

        if not uid:
            raise HTTPException(status_code=500, detail="Resolved work has empty uid")

        ext, normalized_mime = _ext_for_upload(file.filename or "", mime)
        if kind == "author_photo":
            fname = f"{uid}__author_photo{ext}"
        else:
            fname = f"{uid}__illustrator_photo{ext}"

        s3_key = f"{UPLOADS_S3_PREFIX}/{uid}/{fname}"

        try:
            s3 = _s3_client()
            s3.put_object(
                Bucket=S3_BUCKET,
                Key=s3_key,
                Body=data,
                ContentType=normalized_mime,
            )
            url = _s3_url_for_key(s3_key)
        except (EndpointConnectionError, NoCredentialsError) as e:
            raise HTTPException(status_code=500, detail=f"S3 auth/connection error: {e}")
        except ClientError as e:
            code = e.response.get("Error", {}).get("Code", "")
            msg = e.response.get("Error", {}).get("Message", "")
            raise HTTPException(status_code=500, detail=f"S3 client error {code}: {msg or str(e)}")
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

        return UploadResponse(
            ok=True,
            url=url,
            filename=file.filename or fname,
            mime=normalized_mime,
            size=size,
            width=width,
            height=height,
            dpi=dpi_tuple,
            key=s3_key,
            bookUid=uid,
            workId=resolved_work_id,
        )

    safe_dir = os.path.join(DATA_UPLOAD_DIR, _clean_name(candidate) or "_")
    pathlib.Path(safe_dir).mkdir(parents=True, exist_ok=True)

    ext = pathlib.Path(file.filename or "upload.bin").suffix or ""
    safe_name = f"{int(time.time() * 1000)}_{_clean_name(file.filename or 'file')}{ext}"
    out_path = os.path.join(safe_dir, safe_name)
    with open(out_path, "wb") as f:
        f.write(data)

    url = f"/static/uploads/{_clean_name(candidate) or '_'}/{safe_name}"

    return UploadResponse(
        ok=True,
        url=url,
        filename=file.filename or safe_name,
        mime=mime,
        size=size,
        width=width,
        height=height,
        dpi=dpi_tuple,
        key=None,
        bookUid=bookUid or None,
        workId=workId or None,
    )