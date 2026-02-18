# marble_app/routers/uploads.py
import os, io, time, pathlib, mimetypes
from typing import Literal, Optional, Any, Dict, List

from fastapi import APIRouter, UploadFile, File, Form, HTTPException, Query
from pydantic import BaseModel
from PIL import Image

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError, EndpointConnectionError, NoCredentialsError

# IMPORTANT:
# This router is the ONLY one that should own:
#   GET  /api/uploads/book-assets
#   GET  /api/uploads/health
#   POST /api/uploads/
#
# If you also include uploads_read.py, you may be hitting the wrong implementation.
router = APIRouter(prefix="/api/uploads", tags=["Uploads"])

# ----------------------------
# Local storage (existing uploads behavior)
# ----------------------------
DATA_UPLOAD_DIR = os.environ.get("DATA_UPLOAD_DIR", "./data/uploads")
pathlib.Path(DATA_UPLOAD_DIR).mkdir(parents=True, exist_ok=True)

UploadKind = Literal["author_contract", "illustrator_contract", "w9", "book_cover", "other"]

ALLOWED = {
    "author_contract": {"application/pdf"},
    "illustrator_contract": {"application/pdf"},
    "w9": {"application/pdf"},
    "book_cover": {"image/jpeg", "image/png"},
    "other": {"application/pdf", "image/jpeg", "image/png"},
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
    """
    CRITICAL:
    - Force regional endpoint for non-us-east-1 buckets.
    - Force SigV4.
    - Force virtual hosted addressing so URLs match:
        https://<bucket>.s3.<region>.amazonaws.com/<key>?X-Amz-...
      (This is ideal for Next/Image allowlisting.)
    """
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
    # accept __cover.<ext> and also common "cover" variants
    if "__cover." in n:
        return n.endswith((".jpg", ".jpeg", ".png", ".webp", ".gif"))
    if "cover" in n:
        return n.endswith((".jpg", ".jpeg", ".png", ".webp", ".gif"))
    return False

def _s3_url_for_key(key: str) -> str:
    if not UPLOADS_USE_PRESIGNED:
        # Only works if object is public (or you front it with CloudFront)
        return f"https://{S3_BUCKET}.s3.{AWS_REGION}.amazonaws.com/{key}"

    s3 = _s3_client()
    return s3.generate_presigned_url(
        "get_object",
        Params={"Bucket": S3_BUCKET, "Key": key},
        ExpiresIn=UPLOADS_PRESIGN_EXPIRES,
        HttpMethod="GET",
    )

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

    # S3 listing first (prod architecture)
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

            # match frontend expectation
            return {"bookUid": uid, "cover": cover, "files": files}

        except (EndpointConnectionError, NoCredentialsError) as e:
            raise HTTPException(status_code=500, detail=f"S3 auth/connection error: {e}")
        except ClientError as e:
            code = e.response.get("Error", {}).get("Code", "")
            msg = e.response.get("Error", {}).get("Message", "")
            raise HTTPException(status_code=500, detail=f"S3 client error {code}: {msg or str(e)}")
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

    # Local fallback listing (dev)
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

# ----------------------------
# Existing upload endpoint (still local disk)
# ----------------------------
@router.post("/", response_model=UploadResponse)
async def upload_file(
    file: UploadFile = File(...),
    kind: UploadKind = Form(...),
    book_key: str = Form(""),
):
    allowed = ALLOWED.get(kind, set())
    mime = file.content_type or ""
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

    safe_dir = os.path.join(DATA_UPLOAD_DIR, _clean_name(book_key) or "_")
    pathlib.Path(safe_dir).mkdir(parents=True, exist_ok=True)

    ext = pathlib.Path(file.filename or "upload.bin").suffix or ""
    safe_name = f"{int(time.time() * 1000)}_{_clean_name(file.filename or 'file')}{ext}"
    out_path = os.path.join(safe_dir, safe_name)
    with open(out_path, "wb") as f:
        f.write(data)

    url = f"/static/uploads/{_clean_name(book_key) or '_'}/{safe_name}"

    return UploadResponse(
        ok=True,
        url=url,
        filename=file.filename or safe_name,
        mime=mime,
        size=size,
        width=width,
        height=height,
        dpi=dpi_tuple,
    )
