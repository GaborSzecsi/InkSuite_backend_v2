# marble_app/routers/uploads_read.py
import os
from typing import Any, Dict, List, Optional

import boto3
from fastapi import APIRouter, HTTPException, Query
from botocore.config import Config
from botocore.exceptions import ClientError, EndpointConnectionError, NoCredentialsError

router = APIRouter(prefix="/api/uploads", tags=["Uploads"])

# ----------------------- Config -----------------------
AWS_REGION = (os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION") or "us-east-2").strip()

S3_BUCKET = (os.getenv("S3_BUCKET") or "inksuite-data").strip()

UPLOADS_PREFIX = (
    os.getenv("UPLOADS_S3_PREFIX")  # preferred
    or os.getenv("UPLOADS_PREFIX")  # backward compatible
    or "tenants/marble-press/data/uploads"
).strip().rstrip("/")

PRESIGN_EXPIRES = int(os.getenv("UPLOADS_PRESIGN_EXPIRES", "3600"))

# Optional: turn on a bit more debugging in responses (safe)
UPLOADS_DEBUG = (os.getenv("UPLOADS_DEBUG") or "0").strip().lower() in ("1", "true", "yes", "on")


def _endpoint_url() -> str:
    # Force regional endpoint. This is the key to avoiding "global host" presign issues.
    return f"https://s3.{AWS_REGION}.amazonaws.com"


def s3():
    """
    Critical:
      - endpoint_url forces regional signing host: s3.<region>.amazonaws.com
      - addressing_style=virtual yields URLs like:
          https://<bucket>.s3.<region>.amazonaws.com/<key>?X-Amz-...
        (this is the form you allow in next.config remotePatterns)
      - signature_version s3v4 is required
    """
    return boto3.client(
        "s3",
        region_name=AWS_REGION,
        endpoint_url=_endpoint_url(),
        config=Config(
            signature_version="s3v4",
            retries={"max_attempts": 5, "mode": "standard"},
            s3={"addressing_style": "virtual"},
        ),
    )


def _is_cover(name: str) -> bool:
    n = (name or "").lower()
    return n.endswith(("__cover.jpg", "__cover.jpeg", "__cover.png", "__cover.webp"))


@router.get("/health")
def uploads_health():
    out = {
        "ok": True,
        "region": AWS_REGION,
        "bucket": S3_BUCKET,
        "uploadsPrefix": UPLOADS_PREFIX,
        "presignExpires": PRESIGN_EXPIRES,
        "endpointUrl": _endpoint_url(),
    }
    return out


@router.get("/book-assets")
def list_book_assets(bookUid: str = Query(...)):
    book_uid = (bookUid or "").strip()
    if not book_uid:
        raise HTTPException(status_code=400, detail="bookUid is required")

    if not S3_BUCKET:
        raise HTTPException(status_code=500, detail="S3_BUCKET env var is empty")

    prefix = f"{UPLOADS_PREFIX}/{book_uid}/"
    client = s3()

    try:
        items: List[Dict[str, Any]] = []
        cover: Optional[Dict[str, Any]] = None
        token: Optional[str] = None

        while True:
            kwargs: Dict[str, Any] = {"Bucket": S3_BUCKET, "Prefix": prefix, "MaxKeys": 1000}
            if token:
                kwargs["ContinuationToken"] = token

            resp = client.list_objects_v2(**kwargs)

            for obj in (resp.get("Contents") or []):
                key = obj.get("Key") or ""
                if not key or key.endswith("/"):
                    continue

                name = key.split("/")[-1]

                # Presign using the *same client* (regional endpoint + virtual addressing)
                url = client.generate_presigned_url(
                    "get_object",
                    Params={"Bucket": S3_BUCKET, "Key": key},
                    ExpiresIn=PRESIGN_EXPIRES,
                )

                item = {
                    "name": name,
                    "url": url,
                    "size": int(obj.get("Size") or 0),
                }

                if _is_cover(name):
                    # Prefer the first "cover" found; if multiple, last one wins (fine either way)
                    cover = item
                else:
                    items.append(item)

            if resp.get("IsTruncated"):
                token = resp.get("NextContinuationToken")
                if not token:
                    break
            else:
                break

        payload: Dict[str, Any] = {"bookUid": book_uid, "cover": cover, "files": items}

        if UPLOADS_DEBUG:
            payload["debug"] = {
                "prefix": prefix,
                "endpointUrl": _endpoint_url(),
                "region": AWS_REGION,
                "bucket": S3_BUCKET,
                "count": len(items) + (1 if cover else 0),
            }

        return payload

    except (NoCredentialsError, EndpointConnectionError) as e:
        raise HTTPException(status_code=500, detail=f"S3 auth/connection error: {e}")
    except ClientError as e:
        err = e.response.get("Error", {}) if hasattr(e, "response") else {}
        code = err.get("Code", "")
        msg = err.get("Message", "")
        raise HTTPException(status_code=500, detail=f"S3 client error {code}: {msg or str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
