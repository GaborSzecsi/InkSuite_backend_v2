# marble_app/services/s3_books.py
from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Dict, List, Optional

try:
    import boto3  # type: ignore
except Exception:
    boto3 = None  # allows local dev without boto3 installed


def _env(name: str, default: Optional[str] = None) -> Optional[str]:
    v = os.getenv(name)
    if v is None or str(v).strip() == "":
        return default
    return v


def _repo_root() -> Path:
    # This file lives at marble_app/services/s3_books.py -> repo root is 2 parents up from marble_app
    return Path(__file__).resolve().parents[2]


def _default_books_disk_path() -> Path:
    # common locations in your repos
    root = _repo_root()
    candidates = [
        root / "book_data" / "books.json",
        root / "Book_data" / "books.json",
        root / "books.json",
        root / "data" / "books.json",
    ]
    for p in candidates:
        if p.exists():
            return p
    # fallback (even if missing) so errors are explicit
    return root / "book_data" / "books.json"


def load_books_from_disk(path: Optional[str] = None) -> List[Dict[str, Any]]:
    p = Path(path) if path else _default_books_disk_path()
    if not p.exists():
        # keep behavior predictable in dev
        return []
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        # if file is huge/has weird encoding, try a safer read
        with p.open("r", encoding="utf-8", errors="replace") as f:
            return json.load(f)


def load_books_from_s3(
    *,
    bucket: Optional[str] = None,
    key: Optional[str] = None,
    region: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """
    Loads books.json from S3 (bucket/key), based on env:
      S3_BUCKET, BOOKS_S3_KEY, AWS_REGION
    """
    bucket = bucket or _env("S3_BUCKET")
    key = key or _env("BOOKS_S3_KEY")
    region = region or _env("AWS_REGION") or _env("AWS_DEFAULT_REGION")

    if not bucket or not key:
        raise RuntimeError("Missing S3_BUCKET or BOOKS_S3_KEY env vars")

    if boto3 is None:
        raise RuntimeError("boto3 not installed (pip install boto3)")

    s3 = boto3.client("s3", region_name=region) if region else boto3.client("s3")
    obj = s3.get_object(Bucket=bucket, Key=key)
    raw = obj["Body"].read()
    # raw may be bytes
    text = raw.decode("utf-8", errors="replace") if isinstance(raw, (bytes, bytearray)) else str(raw)
    data = json.loads(text)
    if isinstance(data, list):
        return data
    # If someone stored {"books":[...]} style, support it
    if isinstance(data, dict) and "books" in data and isinstance(data["books"], list):
        return data["books"]
    raise ValueError("Unexpected books.json shape (expected list or {'books': [...]})")


def load_books() -> List[Dict[str, Any]]:
    """
    Unified loader:
    - If S3_BUCKET + BOOKS_S3_KEY are set -> load from S3
    - else -> load from disk
    """
    bucket = _env("S3_BUCKET")
    key = _env("BOOKS_S3_KEY")
    if bucket and key:
        return load_books_from_s3(bucket=bucket, key=key)
    return load_books_from_disk()


def health() -> Dict[str, Any]:
    """
    Simple diagnostic info to help debug environments.
    """
    bucket = _env("S3_BUCKET")
    key = _env("BOOKS_S3_KEY")
    region = _env("AWS_REGION") or _env("AWS_DEFAULT_REGION")
    mode = "s3" if (bucket and key) else "disk"
    return {
        "mode": mode,
        "bucket": bucket,
        "key": key,
        "region": region,
        "disk_path": str(_default_books_disk_path()),
        "boto3_available": boto3 is not None,
    }
