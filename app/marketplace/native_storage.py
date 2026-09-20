"""Dedicated social storage. Never fall back to a tenant bucket or public URL."""

import os
from datetime import datetime, timedelta, timezone
from functools import lru_cache
from pathlib import Path
from urllib.parse import urlsplit, quote
from botocore.signers import CloudFrontSigner
from fastapi import HTTPException
from routers.storage_s3 import s3


def config():
    bucket = os.getenv("MARKETPLACE_MEDIA_BUCKET", "")
    origin = os.getenv("MARKETPLACE_MEDIA_CDN_URL", "").rstrip("/")
    parsed = urlsplit(origin)
    key_id = os.getenv("MARKETPLACE_MEDIA_KEY_PAIR_ID", "")
    key_file = os.getenv("MARKETPLACE_MEDIA_PRIVATE_KEY_FILE", "")
    environment = os.getenv("MARKETPLACE_MEDIA_ENV", "development")
    if (
        not bucket
        or environment not in ("development", "production")
        or (
            environment == "development"
            and "-dev" not in bucket
        )
        or (
            environment == "production"
            and "-prod" not in bucket
        )
        or bucket
        in {
            os.getenv("S3_BUCKET", "inksuite-data"),
            os.getenv("TENANT_BUCKET", "inksuite-data"),
        }
        or parsed.scheme != "https"
        or not parsed.hostname
        or parsed.path
        or parsed.query
        or parsed.fragment
        or parsed.username
        or not key_id
        or not key_file
    ):
        raise HTTPException(503, "Native media storage is not configured yet.")
    return bucket, origin, key_id, key_file


@lru_cache(maxsize=2)
def signing_key(filename, modified):
    from cryptography.hazmat.primitives.serialization import load_pem_private_key
    from cryptography.hazmat.primitives.asymmetric.rsa import RSAPrivateKey

    private = load_pem_private_key(Path(filename).read_bytes(), password=None)
    if not isinstance(private, RSAPrivateKey) or private.key_size != 2048:
        raise ValueError("CloudFront requires the configured RSA 2048 signing key")
    return private


def check_delivery():
    filename = config()[3]
    try:
        signing_key(filename, Path(filename).stat().st_mtime_ns)
    except (OSError, ValueError, TypeError):
        raise HTTPException(
            503, "Native media delivery signing is not configured yet."
        ) from None


def delivery(key):
    if not key.startswith("processed/") or ".." in key.split("/"):
        raise ValueError("Invalid derivative key")
    _, origin, key_id, filename = config()
    from cryptography.hazmat.primitives import hashes
    from cryptography.hazmat.primitives.asymmetric import padding

    private = signing_key(filename, Path(filename).stat().st_mtime_ns)
    signer = CloudFrontSigner(
        key_id, lambda msg: private.sign(msg, padding.PKCS1v15(), hashes.SHA1())
    )
    expires = datetime.now(timezone.utc).replace(second=0, microsecond=0) + timedelta(
        minutes=5
    )
    return signer.generate_presigned_url(
        origin + "/" + quote(key, safe="/"), date_less_than=expires
    )


def upload_form(row):
    return s3().generate_presigned_post(
        Bucket=row["bucket"],
        Key=row["original_key"],
        Fields={"Content-Type": row["declared_mime"]},
        Conditions=[
            {"Content-Type": row["declared_mime"]},
            ["content-length-range", row["file_size"], row["file_size"]],
        ],
        ExpiresIn=600,
    )


def describe(row):
    return {
        k: row.get(k)
        for k in (
            "id",
            "media_type",
            "status",
            "original_filename",
            "width",
            "height",
            "duration",
            "error_code",
        )
    } | {
        "variants": (
            [dict(v, url=delivery(v["key"])) for v in row["variants"]]
            if row["status"] == "ready"
            else []
        )
    }
