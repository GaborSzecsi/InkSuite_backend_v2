import os, io, time, pathlib, mimetypes
from typing import Literal, Optional, Any, Dict, List
from fastapi import APIRouter, UploadFile, File, Form, HTTPException, Query
from pydantic import BaseModel
from PIL import Image

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError, EndpointConnectionError, NoCredentialsError

router = APIRouter()

# ----------------------------
# Local storage (existing)
# ----------------------------
DATA_UPLOAD_DIR = os.environ.get("DATA_UPLOAD_DIR", "./data/uploads")
pathlib.Path(DATA_UPLOAD_DIR).mkdir(parents=True, exist_ok=True)

UploadKind = Literal["author_contract","illustrator_contract","w9","book_cover","other"]

ALLOWED = {
    "author_contract": {"application/pdf"},
    "illustrator_contract": {"application/pdf"},
    "w9": {"application/pdf"},
    "book_cover": {"image/jpeg","image/png"},
    "other": {"application/pdf","image/jpeg","image/png"},
}

class UploadResponse(BaseModel):
    ok: bool
    url: str
    filename: str
    mime: str
    size: int
    width: int | None = None
    height: int | None = None
    dpi: tuple[int,int] | None = None

def _clean_name(name: str) -> str:
    return "".join(c for c in name if c.isalnum() or c in ("-","_",".")).strip("._")

# ----------------------------
# S3 config for LISTING assets
# ----------------------------
AWS_REGION = os.environ.get("AWS_REGION", "us-east-2").strip()
S3_BUCKET = os.environ.get("S3_BUCKET", "inksuite-data").strip()

# Your real uploads location per examples:
# s3://inksuite-data/tenants/marble-press/data/uploads/<uid>/<uid>__cover.jpg
UPLOADS_S3_PREFIX = os.environ.get("UPLOADS_S3_PREFIX", "tenants/marble-press/data/uploads").strip().rstrip("/")

# Toggle off S3 listing in local dev if you want
USE_UPLOADS_S3 = os.environ.get("USE_UPLOADS_S3", "1").strip().lower() not in ("0","false","no")

# If bucket is private, return presigned URLs (recommended)
UPLOADS_USE_PRESIGNED = os.environ.get("UPLOADS_USE_PRESIGNED", "1").strip().lower() not in ("0","false","no")
UPLOADS_PRESIGN_EXPIRES = int(os.environ.get("UPLOADS_PRESIGN_EXPIRES", "3600"))

def _s3_client():
    return boto3.client(
        "s3",
        region_name=AWS_REGION,
        config=Config(retries={"max_attempts": 5, "mode": "standard"}),
    )

def _guess_kind(filename: str) -> str:
    f = (filename or "").lower()
    if "__cover" in f or "cover" in f:
        return "book_cover"
    if "author_contract" in f or "__author_contract" in f:
        return "author_contract"
    if "illustrator_contract" in f or "__illustrator_contract" in f:
        return "illustrator_contract"
    if "w9" in f:
        return "w9"
    return "other"

def _s3_url_for_key(key: str) -> str:
    if not UPLOADS_USE_PRESIGNED:
        return f"https://{S3_BUCKET}.s3.{AWS_REGION}.amazonaws.com/{key}"
    s3 = _s3_client()
    return s3.generate_presigned_url(
        "get_object",
        Params={"Bucket": S3_BUCKET, "Key": key},
        ExpiresIn=UPLOADS_PRESIGN_EXPIRES,
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
    }

@router.get("/book-assets")
def list_book_assets(bookUid: str = Query(...)):
    uid = (bookUid or "").strip()
    if not uid:
        raise HTTPException(status_code=400, detail="bookUid is required")

    # 1) Try S3 listing first (this matches your production architecture)
    if USE_UPLOADS_S3:
        if not S3_BUCKET or not UPLOADS_S3_PREFIX:
            # This is the error you are currently seeing
            raise HTTPException(status_code=500, detail="Uploads S3 config missing (need S3_BUCKET and UPLOADS_S3_PREFIX)")

        prefix = f"{UPLOADS_S3_PREFIX}/{uid}/"
        s3 = _s3_client()

        try:
            items: List[Dict[str, Any]] = []
            token: Optional[str] = None

            while True:
                kwargs = {"Bucket": S3_BUCKET, "Prefix": prefix, "MaxKeys": 1000}
                if token:
                    kwargs["ContinuationToken"] = token

                resp = s3.list_objects_v2(**kwargs)

                for obj in resp.get("Contents", []) or []:
                    key = obj.get("Key") or ""
                    if not key or key.endswith("/"):
                        continue
                    filename = key.split("/")[-1]
                    items.append({
                        "filename": filename,
                        "key": key,
                        "url": _s3_url_for_key(key),
                        "size": obj.get("Size", 0),
                        "lastModified": (obj.get("LastModified").isoformat() if obj.get("LastModified") else None),
                        "kind": _guess_kind(filename),
                        "mime": mimetypes.guess_type(filename)[0] or "application/octet-stream",
                        "source": "s3",
                    })

                if resp.get("IsTruncated"):
                    token = resp.get("NextContinuationToken")
                    if not token:
                        break
                else:
                    break

            # If no objects, return [] (do NOT 500)
            return items

        except (EndpointConnectionError, NoCredentialsError) as e:
            s3_err = str(e)
        except ClientError as e:
            s3_err = str(e)
        except Exception as e:
            s3_err = str(e)
    else:
        s3_err = "S3 listing disabled via USE_UPLOADS_S3"

    # 2) Local fallback listing (for dev)
    local_dir = pathlib.Path(DATA_UPLOAD_DIR) / uid
    if local_dir.exists() and local_dir.is_dir():
        out: List[Dict[str, Any]] = []
        for p in sorted(local_dir.rglob("*")):
            if not p.is_file():
                continue
            rel = p.relative_to(pathlib.Path(DATA_UPLOAD_DIR)).as_posix()
            filename = p.name
            out.append({
                "filename": filename,
                "key": rel,
                "url": f"/static/uploads/{rel}",
                "size": p.stat().st_size,
                "lastModified": None,
                "kind": _guess_kind(filename),
                "mime": mimetypes.guess_type(filename)[0] or "application/octet-stream",
                "source": "local",
            })
        return out

    raise HTTPException(
        status_code=500,
        detail=f"Uploads not available. S3 failed ({s3_err}) and local folder missing: {str(local_dir)}",
    )

# ----------------------------
# Existing upload endpoint (unchanged)
# ----------------------------
@router.post("/", response_model=UploadResponse)
async def upload_file(
    file: UploadFile = File(...),
    kind: UploadKind = Form(...),
    book_key: str = Form("")
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
    safe_name = f"{int(time.time()*1000)}_{_clean_name(file.filename or 'file')}{ext}"
    out_path = os.path.join(safe_dir, safe_name)
    with open(out_path, "wb") as f:
        f.write(data)

    url = f"/static/uploads/{_clean_name(book_key) or '_'}/{safe_name}"

    return UploadResponse(
        ok=True, url=url, filename=file.filename or safe_name,
        mime=mime, size=size, width=width, height=height, dpi=dpi_tuple
    )
