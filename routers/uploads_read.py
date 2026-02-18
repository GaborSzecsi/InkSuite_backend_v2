# marble_app/routers/uploads_read.py
import os
from typing import Any, Dict, List, Optional

import boto3
from fastapi import APIRouter, Query, HTTPException
from botocore.config import Config
from botocore.exceptions import ClientError, NoCredentialsError, EndpointConnectionError

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

# ----------------------- S3 Client -----------------------
def s3():
    """
    IMPORTANT:
    - Force the regional endpoint so presigned URLs do NOT use bucket.s3.amazonaws.com (global),
      which can 403 for non-us-east-1 buckets.
    - With endpoint_url set, presigned URLs should validate correctly for AWS_REGION.
    """
    return boto3.client(
        "s3",
        region_name=AWS_REGION,
        endpoint_url=f"https://s3.{AWS_REGION}.amazonaws.com",
        config=Config(
            signature_version="s3v4",
            retries={"max_attempts": 5, "mode": "standard"},
        ),
    )

def _is_cover(name: str) -> bool:
    n = (name or "").lower()
    return (
        n.endswith("__cover.jpg")
        or n.endswith("__cover.jpeg")
        or n.endswith("__cover.png")
        or n.endswith("__cover.webp")
    )

# ----------------------- Routes -----------------------
@router.get("/health")
def uploads_health():
    return {
        "ok": True,
        "region": AWS_REGION,
        "bucket": S3_BUCKET,
        "uploadsPrefix": UPLOADS_PREFIX,
        "presignExpires": PRESIGN_EXPIRES,
    }

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

                url = client.generate_presigned_url(
                    "get_object",
                    Params={"Bucket": S3_BUCKET, "Key": key},
                    ExpiresIn=PRESIGN_EXPIRES,
                )

                item = {"name": name, "url": url, "size": int(obj.get("Size") or 0)}

                if _is_cover(name):
                    # pick the first cover encountered (or overwrite; your choice)
                    cover = item
                else:
                    items.append(item)

            if resp.get("IsTruncated"):
                token = resp.get("NextContinuationToken")
                if not token:
                    break
            else:
                break

        return {"bookUid": book_uid, "cover": cover, "files": items}

    except (NoCredentialsError, EndpointConnectionError) as e:
        raise HTTPException(status_code=500, detail=f"S3 auth/connection error: {e}")
    except ClientError as e:
        # include useful error code without dumping huge XML
        code = e.response.get("Error", {}).get("Code", "")
        msg = e.response.get("Error", {}).get("Message", "")
        raise HTTPException(status_code=500, detail=f"S3 client error {code}: {msg or str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
