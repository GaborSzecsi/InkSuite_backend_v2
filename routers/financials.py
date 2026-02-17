# marble_app/routers/financials.py
from __future__ import annotations

import json
import os
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError, EndpointConnectionError, NoCredentialsError
from fastapi import APIRouter, HTTPException, Query

router = APIRouter(prefix="/api/financials", tags=["Financials"])

# =========
# ENV
# =========
AWS_REGION = os.getenv("AWS_REGION", "us-east-2")

S3_BUCKET = os.getenv("S3_BUCKET", "inksuite-data").strip()
TENANT_PREFIX = os.getenv("TENANT_PREFIX", "tenants/marble-press").strip().rstrip("/")

# S3 keys live under book_data
FINANCIALS_KEY = os.getenv(
    "FINANCIALS_S3_KEY",
    f"{TENANT_PREFIX}/book_data/financials.json",
).strip()
BOOKS_KEY = os.getenv(
    "BOOKS_S3_KEY",
    f"{TENANT_PREFIX}/book_data/books.json",
).strip()

# Local fallbacks (make local dev robust; also helps EC2 if S3 is temporarily broken)
LOCAL_FINANCIALS_PATH = Path(
    os.getenv("LOCAL_FINANCIALS_PATH", "./book_data/financials.json")
).resolve()
LOCAL_BOOKS_PATH = Path(
    os.getenv("LOCAL_BOOKS_PATH", "./book_data/books.json")
).resolve()

# Toggle: in local dev you may want to skip S3 entirely
USE_S3 = os.getenv("USE_S3", "1").strip().lower() not in ("0", "false", "no")

_PERIOD_RE = re.compile(r"^\d{4}-\d{2}$")

# =========
# S3 helpers
# =========
def _s3_client():
    # If EC2 has an instance role, boto3 will auto-discover creds.
    return boto3.client(
        "s3",
        region_name=AWS_REGION,
        config=Config(retries={"max_attempts": 5, "mode": "standard"}),
    )

def _read_local_json(path: Path) -> Any:
    if not path.exists():
        raise FileNotFoundError(str(path))
    raw = path.read_text(encoding="utf-8")
    return json.loads(raw)

def _load_json_from_s3(key: str) -> Any:
    if not S3_BUCKET:
        raise HTTPException(status_code=500, detail="S3_BUCKET env var is empty")
    if not key:
        raise HTTPException(status_code=500, detail="S3 key is empty")

    s3 = _s3_client()
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
        raw = obj["Body"].read().decode("utf-8")
        return json.loads(raw)
    except ClientError as e:
        # preserve NoSuchKey vs permissions vs other
        raise HTTPException(status_code=500, detail=f"S3 ClientError for s3://{S3_BUCKET}/{key}: {e}")
    except (EndpointConnectionError, NoCredentialsError) as e:
        raise HTTPException(status_code=500, detail=f"S3 connection/credentials error for s3://{S3_BUCKET}/{key}: {e}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to read s3://{S3_BUCKET}/{key}: {e}")

def _load_with_fallback(kind: str, s3_key: str, local_path: Path) -> Tuple[Any, str]:
    """
    Return (data, source) where source is 's3' or 'local'.
    """
    s3_err: Optional[str] = None

    if USE_S3:
        try:
            return _load_json_from_s3(s3_key), "s3"
        except HTTPException as e:
            s3_err = str(e.detail)

    # local fallback
    try:
        return _read_local_json(local_path), "local"
    except Exception as le:
        # If both fail, give a very explicit error
        detail = (
            f"Failed to load {kind}. "
            f"Tried S3 key s3://{S3_BUCKET}/{s3_key}"
            + (f" (error: {s3_err})" if s3_err else " (skipped S3)")
            + f" and local path {str(local_path)} (error: {le})."
        )
        raise HTTPException(status_code=500, detail=detail)

def _load_financials() -> Tuple[Dict[str, Any], str]:
    data, src = _load_with_fallback("financials.json", FINANCIALS_KEY, LOCAL_FINANCIALS_PATH)
    if not isinstance(data, dict):
        raise HTTPException(status_code=500, detail=f"financials.json loaded from {src} is not a JSON object")
    return data, src

def _load_books() -> Tuple[List[Dict[str, Any]], str]:
    data, src = _load_with_fallback("books.json", BOOKS_KEY, LOCAL_BOOKS_PATH)
    if isinstance(data, list):
        return [b for b in data if isinstance(b, dict)], src
    # tolerate non-list by returning empty list
    return [], src

# =========
# coercion helpers
# =========
def to_num(v: Any) -> float:
    try:
        n = float(v)
        return n if n == n else 0.0
    except Exception:
        return 0.0

def to_int(v: Any) -> int:
    try:
        return int(float(v))
    except Exception:
        return 0

def has_inventory(v: Any) -> bool:
    if v is None:
        return False
    try:
        float(v)
        return True
    except Exception:
        return False

def friendly_format_label(fmt: str) -> str:
    u = (fmt or "").upper()
    return {
        "HC": "Hardcover",
        "PB": "Paperback",
        "BB": "Board Book",
        "EBK": "Ebook",
        "AUD": "Audiobook",
        "LP": "Large Print",
    }.get(u, u or "")

# =========
# Book key resolution
# =========
def resolve_book_keys(input_key: str, books: List[Dict[str, Any]]) -> List[str]:
    """
    Your financials.json might key by uid or id (or sometimes other variants).
    This returns candidate keys to try in byBook[...].
    """
    target = (input_key or "").strip()
    if not target:
        return []

    candidates = set()

    for b in books:
        possible = [
            b.get("uid"),
            b.get("id"),
            b.get("book_uid"),
            b.get("slug"),
        ]
        possible = [p for p in possible if isinstance(p, str) and p.strip()]

        if target in possible:
            for p in possible:
                candidates.add(p)
            candidates.add(target)
            break

    if not candidates:
        candidates.add(target)

    return list(candidates)

def period_year(period_key: str) -> Optional[int]:
    if not isinstance(period_key, str) or not _PERIOD_RE.match(period_key):
        return None
    try:
        return int(period_key[:4])
    except Exception:
        return None

def _normalize_month(month: Any) -> Optional[str]:
    if month is None:
        return None
    try:
        mm = int(str(month).strip())
        if 1 <= mm <= 12:
            return f"{mm:02d}"
        return None
    except Exception:
        return None

def build_period_keys(mode: str, year: int, month: Optional[Any], season: Optional[str]) -> List[str]:
    mode_u = (mode or "MONTH").upper()

    if mode_u == "MONTH":
        mm = _normalize_month(month)
        if not mm:
            mm = f"{datetime.utcnow().month:02d}"
        return [f"{year:04d}-{mm}"]

    s = (season or "").upper()
    if s not in ("SPRING", "FALL"):
        raise HTTPException(status_code=400, detail="For mode=SEASON, season must be SPRING or FALL")

    months = ["01", "02", "03", "04", "05", "06"] if s == "SPRING" else ["07", "08", "09", "10", "11", "12"]
    return [f"{year:04d}-{m}" for m in months]

# =========
# Routes
# =========
@router.get("/health")
def financials_health():
    fin_ok = LOCAL_FINANCIALS_PATH.exists() or bool(FINANCIALS_KEY)
    books_ok = LOCAL_BOOKS_PATH.exists() or bool(BOOKS_KEY)
    return {
        "ok": True,
        "useS3": USE_S3,
        "region": AWS_REGION,
        "bucket": S3_BUCKET,
        "tenantPrefix": TENANT_PREFIX,
        "financialsKey": FINANCIALS_KEY,
        "booksKey": BOOKS_KEY,
        "localFinancialsPath": str(LOCAL_FINANCIALS_PATH),
        "localBooksPath": str(LOCAL_BOOKS_PATH),
        "localFinancialsExists": LOCAL_FINANCIALS_PATH.exists(),
        "localBooksExists": LOCAL_BOOKS_PATH.exists(),
        "sanity": {"financialsConfigured": fin_ok, "booksConfigured": books_ok},
    }

@router.get("/book-kpis")
def get_book_kpis(
    bookUid: str = Query(...),
    mode: str = Query("MONTH"),
    year: int = Query(default_factory=lambda: datetime.utcnow().year),
    month: Optional[Any] = Query(None),
    season: Optional[str] = Query(None),
    format: str = Query("ALL"),
):
    financials, fin_src = _load_financials()
    books, books_src = _load_books()
    candidate_keys = resolve_book_keys(bookUid, books)

    keys = build_period_keys(mode, year, month, season)
    fmt_filter = (format or "ALL").upper()

    total_units = 0
    total_returns = 0
    total_free = 0
    total_fraser_units = 0
    total_fraser_dollars = 0.0

    latest_inv_key = ""
    latest_inv_sum: Optional[int] = None

    for period_key in keys:
        period = financials.get(period_key) or {}
        by_book = period.get("byBook") or {}
        if not isinstance(by_book, dict):
            continue

        book_entry = None
        for bk in candidate_keys:
            be = by_book.get(bk)
            if isinstance(be, dict) and isinstance(be.get("formats"), dict):
                book_entry = be
                break
        if not book_entry:
            continue

        formats = book_entry.get("formats") or {}
        if not isinstance(formats, dict):
            continue

        if fmt_filter == "ALL":
            rows = [r for r in formats.values() if isinstance(r, dict)]
        else:
            r = formats.get(fmt_filter)
            rows = [r] if isinstance(r, dict) else []

        if not rows:
            continue

        period_inv_sum = 0
        has_any_inv = False

        for row in rows:
            sales = row.get("sales") if isinstance(row.get("sales"), dict) else {}
            us = sales.get("us") if isinstance(sales.get("us"), dict) else {}
            fr = sales.get("fraser") if isinstance(sales.get("fraser"), dict) else {}

            us_net = us.get("unitsNet")
            us_ret = us.get("unitsReturns")

            fr_net = fr.get("unitsNet")
            fr_dollars = fr.get("dollars")

            total_units += to_int(us_net if us_net is not None else row.get("unitsSold"))
            total_returns += to_int(us_ret if us_ret is not None else row.get("returns"))
            total_free += to_int(row.get("freeCopies"))

            total_fraser_units += to_int(fr_net if fr_net is not None else row.get("fraserShipments"))
            total_fraser_dollars += to_num(fr_dollars if fr_dollars is not None else row.get("fraserDollars"))

            inv_end = row.get("inventoryEnd")
            if has_inventory(inv_end):
                period_inv_sum += to_int(inv_end)
                has_any_inv = True

        if has_any_inv and (not latest_inv_key or period_key > latest_inv_key):
            latest_inv_key = period_key
            latest_inv_sum = period_inv_sum

    return {
        "unitsSold": total_units,
        "returns": total_returns,
        "freeCopies": total_free,
        "inventoryEnd": latest_inv_sum if latest_inv_sum is not None else 0,
        "fraserShipments": total_fraser_units,
        "fraserDollars": round(total_fraser_dollars, 2),
        "periods": keys,
        "_source": {"financials": fin_src, "books": books_src},
    }

@router.get("/book-format-stats")
def get_book_format_stats(
    bookUid: str = Query(...),
    year: int = Query(default_factory=lambda: datetime.utcnow().year),
):
    financials, fin_src = _load_financials()
    books, books_src = _load_books()
    candidate_keys = resolve_book_keys(bookUid, books)

    period_keys = sorted([k for k in financials.keys() if isinstance(k, str) and _PERIOD_RE.match(k)])

    per_fmt: Dict[str, Dict[str, Any]] = {}
    latest_period_with_data: Optional[str] = None

    for pk in period_keys:
        period = financials.get(pk) or {}
        by_book = period.get("byBook") or {}
        if not isinstance(by_book, dict):
            continue

        py = period_year(pk)
        if py is None:
            continue

        book_entry = None
        for bk in candidate_keys:
            be = by_book.get(bk)
            if isinstance(be, dict) and isinstance(be.get("formats"), dict):
                book_entry = be
                break
        if not book_entry:
            continue

        if latest_period_with_data is None or pk > latest_period_with_data:
            latest_period_with_data = pk

        formats = book_entry.get("formats") or {}
        if not isinstance(formats, dict):
            continue

        for fmt_code_raw, row in formats.items():
            if not isinstance(row, dict):
                continue
            fmt = (fmt_code_raw or "").upper()
            if not fmt:
                continue

            if fmt not in per_fmt:
                per_fmt[fmt] = {
                    "label": friendly_format_label(fmt),
                    "totalPrinted": 0,
                    "lifetimeSold": 0,
                    "ytdSold": 0,
                    "lifetimeFraserShipments": 0,
                    "ytdFraserShipments": 0,
                    "inventoryEnd": 0,
                    "_lastInvPk": "",
                }

            agg = per_fmt[fmt]

            inv = row.get("inventory")
            if isinstance(inv, dict):
                agg["totalPrinted"] += sum(to_int(v) for v in inv.values())

            units_fallback = to_int(row.get("unitsSold"))
            sales = row.get("sales") if isinstance(row.get("sales"), dict) else {}
            us = sales.get("us") if isinstance(sales.get("us"), dict) else {}
            units_net = to_int(us.get("unitsNet")) if us.get("unitsNet") is not None else units_fallback

            agg["lifetimeSold"] += units_net
            if py == year:
                agg["ytdSold"] += units_net

            fr = sales.get("fraser") if isinstance(sales.get("fraser"), dict) else {}
            fraser_units = to_int(fr.get("unitsNet")) if fr.get("unitsNet") is not None else to_int(row.get("fraserShipments"))

            agg["lifetimeFraserShipments"] += fraser_units
            if py == year:
                agg["ytdFraserShipments"] += fraser_units

            inv_end = row.get("inventoryEnd")
            if (not agg["_lastInvPk"] or pk > agg["_lastInvPk"]) and has_inventory(inv_end):
                agg["_lastInvPk"] = pk
                agg["inventoryEnd"] = to_int(inv_end)

    formats_out: Dict[str, Any] = {}
    for fmt, agg in per_fmt.items():
        agg2 = dict(agg)
        agg2.pop("_lastInvPk", None)
        formats_out[fmt] = agg2

    return {
        "bookUid": bookUid,
        "year": year,
        "asOf": latest_period_with_data,
        "formats": formats_out,
        "_source": {"financials": fin_src, "books": books_src},
    }
