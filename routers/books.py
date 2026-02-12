# marble_app/routers/books.py

import os
import re
import math
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from fastapi import APIRouter, HTTPException, Request, status
from marble_app.services.file_ops import load_json, save_json

# Router (main.py likely uses: app.include_router(books.router, prefix="/api"))
router = APIRouter()

# ----------------------- Config -----------------------
# Keep flat fields (author_street/author_city/etc.) in responses for the current UI.
KEEP_FLAT_MIRRORS: bool = True

# Set True briefly and hit GET /api/books once if you want to rewrite the file on GET.
PERSIST_NORMALIZED_ON_GET: bool = False

MODULE_VERSION = "addr-v5"

# ----------------------- Utils ------------------------
def _clean_nan(obj: Any) -> Any:
    if isinstance(obj, dict):
        return {k: _clean_nan(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_clean_nan(x) for x in obj]
    if isinstance(obj, float) and math.isnan(obj):
        return None
    return obj

def _books_path() -> Path:
    env = os.getenv("BOOKS_JSON")
    if env:
        p = Path(env).expanduser()
        p.parent.mkdir(parents=True, exist_ok=True)
        return p
    here = Path(__file__).resolve()
    for candidate in [
        here.parents[1] / "Book_data" / "books.json",
        here.parents[1] / "data" / "books.json",
    ]:
        candidate.parent.mkdir(parents=True, exist_ok=True)
        return candidate
    fallback = here.parent / "books.json"
    fallback.parent.mkdir(parents=True, exist_ok=True)
    return fallback

DATA_PATH = _books_path()

def _make_key(b: Dict[str, Any]) -> str:
    t = (b.get("title") or "").strip().lower()
    a = (b.get("author") or "").strip().lower()
    return f"{t}__{a}" if (t or a) else ""

def _find_index(books: List[Dict[str, Any]], *, key: Optional[str] = None, book_id: Optional[str] = None) -> int:
    for i, b in enumerate(books):
        if book_id and b.get("id") == book_id:
            return i
        if key and _make_key(b) == key:
            return i
    return -1

# ---------------- Address normalization ----------------
_US_STATE_MAP: Dict[str, str] = {
    "AL":"Alabama","AK":"Alaska","AZ":"Arizona","AR":"Arkansas","CA":"California","CO":"Colorado","CT":"Connecticut",
    "DE":"Delaware","FL":"Florida","GA":"Georgia","HI":"Hawaii","ID":"Idaho","IL":"Illinois","IN":"Indiana","IA":"Iowa",
    "KS":"Kansas","KY":"Kentucky","LA":"Louisiana","ME":"Maine","MD":"Maryland","MA":"Massachusetts","MI":"Michigan",
    "MN":"Minnesota","MS":"Mississippi","MO":"Missouri","MT":"Montana","NE":"Nebraska","NV":"Nevada",
    "NH":"New Hampshire","NJ":"New Jersey","NM":"New Mexico","NY":"New York","NC":"North Carolina","ND":"North Dakota",
    "OH":"Ohio","OK":"Oklahoma","OR":"Oregon","PA":"Pennsylvania","RI":"Rhode Island","SC":"South Carolina",
    "SD":"South Dakota","TN":"Tennessee","TX":"Texas","UT":"Utah","VT":"Vermont","VA":"Virginia","WA":"Washington",
    "WV":"West Virginia","WI":"Wisconsin","WY":"Wyoming","DC":"District of Columbia",
}
STREET_SUFFIXES = {
    "ave","avenue","blvd","boulevard","cir","circle","ct","court","dr","drive","hwy","highway",
    "ln","lane","pkwy","parkway","pl","place","rd","road","st","street","ter","terrace","trl","trail","way"
}

def _to_full_state(state: str) -> str:
    s = (state or "").strip()
    if not s:
        return ""
    return _US_STATE_MAP.get(s.upper(), s)

def _fix_country_name(country: str) -> str:
    c = (country or "").strip()
    if not c:
        return ""
    if c.lower() in {"united sates","united sate","united state"} or c.upper() == "USA":
        return "United States"
    return c

def _split_line1_street_city_state(line1: str, fallback_state: str = "") -> Tuple[str, str, str]:
    """Split '129 Morro Ave Shell Beach CA' -> street, city, full-state."""
    s = (line1 or "").strip()
    if not s:
        return "", "", _to_full_state(fallback_state)
    tokens = s.split()
    # Peel off trailing 2-letter state if present
    if tokens and len(tokens[-1]) == 2 and tokens[-1].isalpha():
        st = tokens[-1]
        tokens = tokens[:-1]
        idx = -1
        for i, t in enumerate(tokens):
            if t.lower().strip(".,") in STREET_SUFFIXES:
                idx = i
        if idx != -1:
            street = " ".join(tokens[:idx+1]).strip()
            city = " ".join(tokens[idx+1:]).strip()
            return street, city, _to_full_state(st)
        return s, "", _to_full_state(st)
    # Fallback: use given state, still try to split street/city
    if fallback_state:
        idx = -1
        for i, t in enumerate(tokens):
            if t.lower().strip(".,") in STREET_SUFFIXES:
                idx = i
        if idx != -1:
            street = " ".join(tokens[:idx+1]).strip()
            city = " ".join(tokens[idx+1:]).strip()
            return street, city, _to_full_state(fallback_state)
        return s, "", _to_full_state(fallback_state)
    return s, "", ""

def _parse_legacy_address(addr_str: str) -> Dict[str, str]:
    """Parse legacy 'Street\\nCity, ST ZIP[, Country]' into structured pieces."""
    out = {"street":"", "city":"", "state":"", "zip":"", "country":""}
    s = (addr_str or "").strip()
    if not s:
        return out

    # Strip trailing country if present
    m = re.search(r"\s*,\s*(USA|United States|United Sates|United State|Canada)$", s, re.I)
    if m:
        out["country"] = _fix_country_name(m.group(1))
        s = re.sub(r"\s*,\s*(USA|United States|United Sates|United State|Canada)$", "", s, flags=re.I)

    lines = [ln.strip() for ln in s.splitlines() if ln.strip()]
    if not lines:
        return out

    # Single line "... ST ZIP"
    if len(lines) == 1:
        single = lines[0]
        m = re.match(r"^(.*?)(?:,)?\s+([A-Za-z]{2})\s+(\d{5}(?:-\d{4})?)$", single)
        if m:
            before, st, z = m.group(1), m.group(2), m.group(3)
            street, city, state_full = _split_line1_street_city_state(before, fallback_state=st)
            out.update({"street": street, "city": city, "state": state_full, "zip": z})
            return out
        out["street"] = single
        return out

    # 2+ lines
    line1 = lines[0]
    tail = lines[-1]

    # ST ZIP or ST, ZIP
    m = re.match(r"^([A-Za-z]{2}),?\s+(\d{5}(?:-\d{4})?)$", tail)
    if m:
        st, z = m.group(1), m.group(2)
        street, city, state_full = _split_line1_street_city_state(line1, fallback_state=st)
        out.update({"street": street, "city": city, "state": state_full, "zip": z})
        return out

    # City, ST ZIP
    m = re.match(r"^(.+?),\s*([A-Za-z]{2})\s+(\d{5}(?:-\d{4})?)$", tail)
    if m:
        out["street"] = line1
        out["city"] = m.group(1).strip()
        out["state"] = _to_full_state(m.group(2).strip())
        out["zip"] = m.group(3).strip()
        return out

    # City, ST
    m = re.match(r"^(.+?),\s*([A-Za-z]{2})$", tail)
    if m:
        out["street"] = line1
        out["city"] = m.group(1).strip()
        out["state"] = _to_full_state(m.group(2).strip())
        return out

    # City ZIP
    m = re.match(r"^(.+?)\s+(\d{5}(?:-\d{4})?)$", tail)
    if m:
        out["street"] = line1
        out["city"] = m.group(1).strip()
        out["zip"] = m.group(2).strip()
        return out

    # Fallback: line1 as street, tail as city
    out["street"] = line1
    out["city"] = tail
    return out

def _address_from_sources(*,
                          address: Any = None,
                          street: Optional[str] = None,
                          city: Optional[str] = None,
                          state: Optional[str] = None,
                          zip_code: Optional[str] = None,
                          country: Optional[str] = None) -> Dict[str, str]:
    # Prefer structured dict
    if isinstance(address, dict):
        return {
            "street": (address.get("street") or street or "").strip(),
            "city": (address.get("city") or city or "").strip(),
            "state": _to_full_state(address.get("state") or state or ""),
            "zip": (address.get("zip") or zip_code or "").strip(),
            "country": _fix_country_name(address.get("country") or country or ""),
        }
    # Legacy string
    if isinstance(address, str):
        parsed = _parse_legacy_address(address)
        parsed["state"] = _to_full_state(parsed["state"])
        parsed["country"] = _fix_country_name(parsed["country"])
        return parsed
    # Fallback fields only
    if street or city or state or zip_code or country:
        return {
            "street": (street or "").strip(),
            "city": (city or "").strip(),
            "state": _to_full_state(state or ""),
            "zip": (zip_code or "").strip(),
            "country": _fix_country_name(country or ""),
        }
    return {"street":"", "city":"", "state":"", "zip":"", "country":""}

# ---------------- Formats canonicalization ----------------
def _to_num(x):
    try:
        if x is None: return None
        if isinstance(x, (int, float)): return x
        s = str(x).strip()
        if s == "": return None
        f = float(s)
        return int(f) if f.is_integer() else f
    except Exception:
        return None

# target (lower-case) -> accepted raw keys (various cases)
_FORMAT_KEY_MAP = {
    "format": {"format","Format"},
    "pub_date": {"pub_date","PubDate","publication_date","PublicationDate"},
    "isbn": {"isbn","ISBN"},
    "price_us": {"price_us","Price_us","Price US","Price","price"},
    "price_can": {"price_can","Price_can","Price CAN"},
    "loc_number": {"loc_number","LOC","Loc","loc"},
    "pages": {"pages","Pages"},
    "tall": {"tall","Tall"},
    "wide": {"wide","Wide"},
    "spine": {"spine","Spine"},
    "weight": {"weight","Weight"},
}

_ALIAS_LOWER = {a.lower() for s in _FORMAT_KEY_MAP.values() for a in s}

def _canon_format_row(row: Any) -> Dict[str, Any]:
    """Return a dict with one canonical, lower-case set of keys only."""
    if not isinstance(row, dict):
        return {}
    out: Dict[str, Any] = {}
    # Build reverse map raw_key_lower -> target
    rev = {}
    for target, raws in _FORMAT_KEY_MAP.items():
        for rk in raws:
            rev[rk.lower()] = target

    # Map known aliases to their target key
    for k, v in row.items():
        key_lower = str(k).lower()
        target = rev.get(key_lower)
        if not target:
            # keep unknown keys that are not aliases
            if key_lower not in _ALIAS_LOWER:
                out[k] = v
            continue
        if target in {"pages","tall","wide","spine","weight","price_us","price_can"}:
            nv = _to_num(v)
            prev = out.get(target)
            # prefer non-null
            out[target] = nv if nv is not None else prev
        else:
            out[target] = v
    return out

def _format_row_for_response(row: Dict[str, Any]) -> Dict[str, Any]:
    """
    Build Title Case response row with a single set of keys.
    Input should already be canonical lower-case (use _canon_format_row first).
    """
    r = _canon_format_row(row or {})
    return {
        "Format": r.get("format",""),
        "ISBN": r.get("isbn",""),
        "PubDate": r.get("pub_date",""),
        "Price US": _to_num(r.get("price_us")) or 0,
        "Price CAN": _to_num(r.get("price_can")) or 0,
        "LOC": r.get("loc_number",""),
        "Pages": _to_num(r.get("pages")) or 0,
        "Tall": _to_num(r.get("tall")) or 0,
        "Wide": _to_num(r.get("wide")) or 0,
        "Spine": _to_num(r.get("spine")) or 0,
        "Weight": _to_num(r.get("weight")) or 0,
    }

# ---------------- Book normalization -------------------
def _normalize_book_nested_only(b: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(b)

    # Ensure id
    if not out.get("id"):
        out["id"] = str(uuid.uuid4())

    # Clean formats: drop row-level "US School Grade" and canonicalize keys
    if isinstance(out.get("formats"), list):
        cleaned = []
        for row in out["formats"]:
            if isinstance(row, dict):
                r = dict(row)
                r.pop("US School Grade", None)
                r.pop("us_school_grade", None)
                r = _canon_format_row(r)  # <- single lower-case set only
                cleaned.append(r)
            else:
                cleaned.append(_canon_format_row({}))
        out["formats"] = cleaned

    # Top-level "US School Grade" -> "us_grade"
    if isinstance(out.get("US School Grade"), str) and not out.get("us_grade"):
        out["us_grade"] = out.pop("US School Grade")

    # series None -> ""
    if out.get("series") is None:
        out["series"] = ""
    # series_volume from marble_app.volume_number if provided
    if out.get("volume_number") is not None and out.get("series_volume") is None:
        try:
            out["series_volume"] = int(out["volume_number"])
        except Exception:
            pass

    # Author address
    out["author_address"] = _address_from_sources(
        address=out.get("author_address"),
        street=out.get("author_street") or out.get("author_address_street"),
        city=out.get("author_city") or out.get("author_address_city"),
        state=out.get("author_state") or out.get("author_address_state"),
        zip_code=out.get("author_zip") or out.get("author_address_zip"),
        country=out.get("author_country") or out.get("author_address_country"),
    )

    # Author agent
    ag = out.get("author_agent") or {}
    if not isinstance(ag, dict):
        ag = {}
    ag["address"] = _address_from_sources(
        address=ag.get("address"),
        street=ag.get("address_street") or out.get("agency_street"),
        city=ag.get("address_city") or out.get("agency_city"),
        state=ag.get("address_state") or out.get("agency_state"),
        zip_code=ag.get("address_zip") or out.get("agency_zip"),
        country=ag.get("address_country") or out.get("agency_country"),
    )
    # Drop any flat mirrors inside agent
    for k in ["address_str","address_street","address_city","address_state","address_zip","address_country"]:
        ag.pop(k, None)
    out["author_agent"] = ag

    # Illustrator
    ill = out.get("illustrator") or {}
    if not isinstance(ill, dict):
        ill = {}
    ill["address"] = _address_from_sources(
        address=ill.get("address"),
        street=ill.get("address_street"),
        city=ill.get("address_city"),
        state=ill.get("address_state"),
        zip_code=ill.get("address_zip"),
        country=ill.get("address_country"),
    )
    for k in ["address_str","address_street","address_city","address_state","address_zip","address_country"]:
        ill.pop(k, None)

    # Illustrator agent
    ill_ag = ill.get("agent") or {}
    if isinstance(ill_ag, dict):
        ill_ag["address"] = _address_from_sources(
            address=ill_ag.get("address"),
            street=ill_ag.get("address_street"),
            city=ill_ag.get("address_city"),
            state=ill_ag.get("address_state"),
            zip_code=ill_ag.get("address_zip"),
            country=ill_ag.get("address_country"),
        )
        for k in ["address_str","address_street","address_city","address_state","address_zip","address_country"]:
            ill_ag.pop(k, None)
        ill["agent"] = ill_ag

    out["illustrator"] = ill

    # Drop root-level flat mirrors (disk stays clean)
    for k in [
        "author_street","author_city","author_state","author_zip","author_country",
        "author_address_street","author_address_city","author_address_state","author_address_zip","author_address_country",
        "agency_street","agency_city","agency_state","agency_zip","agency_country",
    ]:
        out.pop(k, None)

    return out

def _add_flat_mirrors(b: Dict[str, Any]) -> Dict[str, Any]:
    if not KEEP_FLAT_MIRRORS:
        return b
    out = dict(b)
    aa = out.get("author_address") or {}
    out["author_street"] = aa.get("street","")
    out["author_city"] = aa.get("city","")
    out["author_state"] = aa.get("state","")
    out["author_zip"] = aa.get("zip","")
    out["author_country"] = aa.get("country","")

    ag = out.get("author_agent") or {}
    ag_addr = ag.get("address") or {}
    out["agency_street"] = ag_addr.get("street","")
    out["agency_city"] = ag_addr.get("city","")
    out["agency_state"] = ag_addr.get("state","")
    out["agency_zip"] = ag_addr.get("zip","")
    out["agency_country"] = ag_addr.get("country","")
    return out

def _normalize_book_for_response(b: Dict[str, Any]) -> Dict[str, Any]:
    """
    Ensure addresses are structured, flat mirrors added, and
    formats are emitted with a single Title Case key set (no dupes).
    """
    nested = _normalize_book_nested_only(b)
    out = _add_flat_mirrors(nested)

    # Canonicalize formats for response (Title Case only)
    fmt_rows = out.get("formats") or []
    out["formats"] = [_format_row_for_response(_canon_format_row(r)) for r in fmt_rows]
    return out

# ----------------------- Routes -----------------------
@router.get("/books/health")
def books_health():
    return {"status": "ok", "path": str(DATA_PATH), "version": MODULE_VERSION}

# One-time maintenance endpoint to rewrite books.json in nested address shape
@router.post("/books/normalize")
def normalize_books():
    books = load_json(str(DATA_PATH), default=[])
    nested_only = [_normalize_book_nested_only(b) for b in books]
    save_json(str(DATA_PATH), _clean_nan(nested_only))
    return {"ok": True, "count": len(nested_only)}

@router.get("/books", response_model=List[Dict[str, Any]])
def get_books():
    books = load_json(str(DATA_PATH), default=[])
    normalized = [_normalize_book_for_response(b) for b in books]
    if PERSIST_NORMALIZED_ON_GET:
        nested_only = [_normalize_book_nested_only(b) for b in books]
        save_json(str(DATA_PATH), _clean_nan(nested_only))
    return _clean_nan(normalized)

@router.post("/books", response_model=Dict[str, Any])
async def save_book(request: Request):
    """
    Upsert a book by (title, author) or id.
    We SAVE nested-only (with lower-case canonical format keys) to disk,
    and RETURN with flat mirrors + Title Case formats for UI.
    """
    try:
        body = await request.json()
        if not isinstance(body, dict):
            raise HTTPException(status_code=400, detail="Request body must be a JSON object")

        books: List[Dict[str, Any]] = load_json(str(DATA_PATH), default=[])

        # Normalize to nested-only BEFORE merging/saving
        incoming = _normalize_book_nested_only(body)

        incoming_id = str(incoming.get("id") or "")
        key = _make_key(incoming)

        if incoming_id:
            idx = _find_index(books, book_id=incoming_id)
        else:
            idx = _find_index(books, key=key) if key else -1

        if idx >= 0:
            merged = {**books[idx], **incoming}
            books[idx] = _normalize_book_nested_only(merged)
            saved = books[idx]
        else:
            books.append(incoming)
            saved = incoming

        save_json(str(DATA_PATH), _clean_nan(books))
        return _clean_nan(_normalize_book_for_response(saved))
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.delete("/books", status_code=status.HTTP_204_NO_CONTENT)
def delete_book(title: str, author: str):
    target = f"{(title or '').strip().lower()}__{(author or '').strip().lower()}"
    books: List[Dict[str, Any]] = load_json(str(DATA_PATH), default=[])
    remaining = [b for b in books if _make_key(b) != target]
    if len(remaining) == len(books):
        raise HTTPException(status_code=404, detail="Book not found")
    save_json(str(DATA_PATH), _clean_nan(remaining))
    return
