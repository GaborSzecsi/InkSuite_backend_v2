# Users/szecs/Documents/marble_app/routers/storage_s3.py
from __future__ import annotations

import os
from typing import List, Optional, Dict, Any
import boto3
from botocore.exceptions import ClientError

def _bucket() -> str:
    b = (os.getenv("TENANT_BUCKET") or os.getenv("S3_BUCKET") or "").strip()
    if not b:
        raise RuntimeError("Missing TENANT_BUCKET (or S3_BUCKET).")
    return b

def _tenant_prefix() -> str:
    # You can set TENANT_PREFIX=tenants/marble-press
    p = (os.getenv("TENANT_PREFIX") or "").strip().strip("/")
    if not p:
        # allow non-tenant mode, but your setup is tenant-based
        p = "tenants/marble-press"
    return p

def tenant_data_prefix(*parts: str) -> str:
    # tenant_data_prefix("data", "Templates") -> tenants/marble-press/data/Templates
    base = _tenant_prefix()
    suffix = "/".join([p.strip("/") for p in parts if p and p.strip("/")])
    return f"{base}/{suffix}".strip("/")

_s3 = None
def s3():
    global _s3
    if _s3 is None:
        _s3 = boto3.client("s3", region_name=os.getenv("AWS_REGION") or None)
    return _s3

def list_keys(prefix: str) -> List[str]:
    b = _bucket()
    out: List[str] = []
    token = None
    while True:
        kwargs: Dict[str, Any] = {"Bucket": b, "Prefix": prefix}
        if token:
            kwargs["ContinuationToken"] = token
        resp = s3().list_objects_v2(**kwargs)
        for item in resp.get("Contents", []) or []:
            k = item.get("Key")
            if k and not k.endswith("/"):
                out.append(k)
        if resp.get("IsTruncated"):
            token = resp.get("NextContinuationToken")
        else:
            break
    return out

def get_bytes(key: str) -> bytes:
    b = _bucket()
    resp = s3().get_object(Bucket=b, Key=key)
    return resp["Body"].read()

def put_bytes(key: str, data: bytes, content_type: Optional[str] = None) -> None:
    b = _bucket()
    kwargs: Dict[str, Any] = {"Bucket": b, "Key": key, "Body": data}
    if content_type:
        kwargs["ContentType"] = content_type
    s3().put_object(**kwargs)

def delete_key(key: str) -> None:
    b = _bucket()
    s3().delete_object(Bucket=b, Key=key)

def presign_get(key: str, expires: int = 3600) -> str:
    b = _bucket()
    return s3().generate_presigned_url(
        "get_object",
        Params={"Bucket": b, "Key": key},
        ExpiresIn=expires,
    )