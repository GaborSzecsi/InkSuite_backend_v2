# marble_app/routers/uploads_read.py
import os
import boto3
from fastapi import APIRouter, Query, HTTPException
from botocore.config import Config
from botocore.exceptions import ClientError, NoCredentialsError, EndpointConnectionError

router = APIRouter(prefix="/api/uploads", tags=["Uploads"])

AWS_REGION = os.getenv("AWS_REGION", "us-east-2").strip()

# Your bucket
S3_BUCKET = os.getenv("S3_BUCKET", "inksuite-data").strip()

# Accept either env var name, with correct default
UPLOADS_PREFIX = (
    os.getenv("UPLOADS_S3_PREFIX")  # preferred
    or os.getenv("UPLOADS_PREFIX")  # backward compatible
    or "tenants/marble-press/data/uploads"
).strip().rstrip("/")

PRESIGN_EXPIRES = int(os.getenv("UPLOADS_PRESIGN_EXPIRES", "3600"))

def s3():
    return boto3.client(
        "s3",
        region_name=AWS_REGION,
        config=Config(signature_version="s3v4", retries={"max_attempts": 5, "mode": "standard"}),
    )

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
        # handle pagination (some books might have many files)
        items = []
        cover = None
        token = None

        while True:
            kwargs = {"Bucket": S3_BUCKET, "Prefix": prefix, "MaxKeys": 1000}
            if token:
                kwargs["ContinuationToken"] = token

            resp = client.list_objects_v2(**kwargs)

            for obj in resp.get("Contents", []) or []:
                key = obj.get("Key") or ""
                if not key or key.endswith("/"):
                    continue
                name = key.split("/")[-1]

                url = client.generate_presigned_url(
                    "get_object",
                    Params={"Bucket": S3_BUCKET, "Key": key},
                    ExpiresIn=PRESIGN_EXPIRES,
                )

                item = {"name": name, "url": url, "size": obj.get("Size", 0)}

                if name.endswith("__cover.jpg") or name.endswith("__cover.png"):
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
        raise HTTPException(status_code=500, detail=f"S3 client error: {e}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
