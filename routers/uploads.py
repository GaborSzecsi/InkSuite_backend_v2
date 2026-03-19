# marble_app/routers/uploads.py
import os, io, time, pathlib, mimetypes
from typing import Literal, Optional, Any, Dict, List
from urllib.parse import urlparse

from fastapi import APIRouter, UploadFile, File, Form, HTTPException, Query
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
    "author_photo",
    "illustrator_photo",
    "other",
]

ALLOWED = {
    "author_contract": {"application/pdf"},
    "illustrator_contract": {"application/pdf"},
    "w9": {"application/pdf"},
    "book_cover": {"image/jpeg", "image/png", "image/webp", "image/jpg"},
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

def _clean_name(name: str) -> str:
    return "".join(c for c in (name or "") if c.isalnum() or c in ("-", "_", ".")).strip("._")

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
        SELECT id::text AS id, uid::text AS uid, title, cover_image_link, cover_image_format
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

def _update_work_cover(work_id: str, s3_key: str, content_type: str) -> None:
    sql = """
        UPDATE works
        SET cover_image_link = %s,
            cover_image_format = %s,
            updated_at = now()
        WHERE id = %s::uuid
    """
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (s3_key, content_type, work_id))
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
                    item = {"name": name, "url": url, "size": size}

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
                files.append({"name": name, "url": url, "size": size})

        return {"bookUid": uid, "cover": cover, "files": files}

    raise HTTPException(
        status_code=500,
        detail=f"Uploads not available. S3 disabled and local folder missing: {str(local_dir)}",
    )


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
            if kind == "book_cover":
                if not dpi_tuple or dpi_tuple[0] < 300 or dpi_tuple[1] < 300:
                    raise HTTPException(status_code=400, detail="Cover must be at least 300 DPI")
        except HTTPException:
            raise
        except Exception:
            raise HTTPException(status_code=400, detail="Invalid image file")

    candidate = (bookUid or workId or book_key or "").strip()
    if not candidate:
        raise HTTPException(status_code=400, detail="bookUid, workId, or book_key is required")

    if kind == "book_cover" and USE_UPLOADS_S3:
        work = _resolve_work(candidate)
        uid = (work["uid"] or "").strip()
        resolved_work_id = (work["id"] or "").strip()

        if not uid:
            raise HTTPException(status_code=500, detail="Resolved work has empty uid")

        ext, normalized_mime = _ext_for_upload(file.filename or "", mime)
        s3_key = f"{UPLOADS_S3_PREFIX}/{uid}/{uid}__cover{ext}"

        try:
            s3 = _s3_client()
            s3.put_object(
                Bucket=S3_BUCKET,
                Key=s3_key,
                Body=data,
                ContentType=normalized_mime,
            )
            _update_work_cover(resolved_work_id, s3_key, normalized_mime)
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
            filename=file.filename or f"{uid}__cover{ext}",
            mime=normalized_mime,
            size=size,
            width=width,
            height=height,
            dpi=dpi_tuple,
            key=s3_key,
            bookUid=uid,
            workId=resolved_work_id,
        )

    # Upload contracts and W-9 to S3 alongside cover when S3 is enabled.
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

    # Upload author/illustrator photos to S3 alongside cover when S3 is enabled.
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