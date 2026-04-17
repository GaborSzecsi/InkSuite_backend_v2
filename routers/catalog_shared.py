from __future__ import annotations

from datetime import date
from typing import Any, Dict, List, Optional


def _safe_str(v: Any) -> str:
    return str(v).strip() if v is not None else ""


def _safe_name(v: Any) -> str:
    return v.strip() if isinstance(v, str) else ""


def _parse_date_or_none(v: Any) -> Optional[date]:
    if v is None:
        return None
    s = str(v).strip()
    if not s:
        return None
    try:
        return date.fromisoformat(s)
    except Exception:
        return None


def _to_float_or_none(v: Any) -> Optional[float]:
    if v is None or v == "":
        return None
    try:
        if isinstance(v, str):
            s = v.strip().replace(",", "")
            if not s:
                return None
            return float(s)
        return float(v)
    except Exception:
        return None


def _to_int_or_none(v: Any) -> Optional[int]:
    if v is None or v == "":
        return None
    try:
        if isinstance(v, str):
            s = v.strip().replace(",", "")
            if not s:
                return None
            return int(float(s))
        return int(float(v))
    except Exception:
        return None


def _normalize_isbn13(val: Any) -> str:
    if val is None:
        return ""
    s = str(val).strip().replace("-", "").replace(" ", "").upper()
    return s[:17] if s else ""


def _is_blank_row(d: Optional[Dict[str, Any]]) -> bool:
    if not d:
        return True
    return not any(_safe_str(v) for v in d.values())


def _contributor_input(payload: Dict[str, Any], scope: str) -> Dict[str, Any]:
    obj = payload.get(scope)
    if not isinstance(obj, dict):
        obj = {}

    name = _safe_name(obj.get("name"))
    email = _safe_str(obj.get("email"))
    address = obj.get("address") if isinstance(obj.get("address"), dict) else {}

    if scope == "author":
        flat_name = _safe_name(payload.get("author_name") or payload.get("author"))
    else:
        flat_name = _safe_name(payload.get("illustrator_name") or payload.get("illustrator"))

    return {
        "name": name or flat_name,
        "email": email or _safe_str(payload.get(f"{scope}_email")),
        "address": address
        if address
        else (
            payload.get(f"{scope}_address")
            if isinstance(payload.get(f"{scope}_address"), dict)
            else {}
        ),
        "website": _safe_str(obj.get("website") or payload.get(f"{scope}_website")),
        "phone_country_code": _safe_str(
            obj.get("phone_country_code") or payload.get(f"{scope}_phone_country_code")
        ),
        "phone_number": _safe_str(
            obj.get("phone_number") or payload.get(f"{scope}_phone_number")
        ),
    }


def _has_real_contributor(payload: Dict[str, Any], scope: str) -> bool:
    info = _contributor_input(payload, scope)
    address = info.get("address") or {}
    return bool(
        _safe_str(info.get("name"))
        or _safe_str(info.get("email"))
        or any(
            _safe_str(address.get(k))
            for k in ("street", "city", "state", "zip", "country")
        )
        or _safe_str(payload.get(f"{scope}_bio"))
        or _safe_str(payload.get(f"{scope}_book_bio"))
        or _safe_str(payload.get(f"{scope}_website_bio"))
    )


def _first_non_empty_dict_list(
    payload: Dict[str, Any], keys: List[str]
) -> List[Dict[str, Any]]:
    for key in keys:
        raw = payload.get(key)
        if isinstance(raw, list) and raw:
            return [x for x in raw if isinstance(x, dict)]
    return []


def _agency_payload_from_input(payload: Dict[str, Any], scope: str) -> Dict[str, Any]:
    direct = payload.get(f"{scope}_agency")
    if isinstance(direct, dict) and not _is_blank_row(direct):
        return dict(direct)

    nested = payload.get(scope)
    if isinstance(nested, dict):
        ag = nested.get("agency")
        if isinstance(ag, dict) and not _is_blank_row(ag):
            return dict(ag)

    out = {
        "agency": _safe_str(payload.get(f"{scope}_agency_name")),
        "agent": _safe_str(payload.get(f"{scope}_agent_name")),
        "contact": _safe_str(payload.get(f"{scope}_agent_name")),
        "email": _safe_str(payload.get(f"{scope}_agent_email")),
        "phone": _safe_str(payload.get(f"{scope}_agent_phone")),
        "website": _safe_str(payload.get(f"{scope}_agency_website")),
        "addressLines": [],
    }
    return out if not _is_blank_row(out) else {}


def _format_phone(phone_country_code: Any, phone_number: Any) -> str:
    cc = _safe_str(phone_country_code)
    num = _safe_str(phone_number)

    if not num:
        return ""

    if num.startswith("+"):
        return num

    cc = cc.lstrip("+").strip()
    if cc and not num.startswith(f"+{cc}"):
        return f"+{cc} {num}".strip()

    return num


def _normalize_scope(scope: str) -> str:
    s = (scope or "").strip().lower()
    if s in ("illustrator", "a12", "artist", "illustration"):
        return "illustrator"
    return "author"


def _normalize_contributor_role(role: str) -> str:
    r = (role or "").strip().lower()
    mapping = {
        "a01": "author",
        "author": "author",
        "primary author": "author",
        "writer": "author",
        "a12": "illustrator",
        "illustrator": "illustrator",
        "artist": "illustrator",
        "illustration": "illustrator",
    }
    return mapping.get(r, r)


def _role_to_scope(role: str) -> str:
    normalized = _normalize_contributor_role(role)
    if normalized == "illustrator":
        return "illustrator"
    return "author"


def _is_author_role(role: str) -> bool:
    return _normalize_contributor_role(role) == "author"


def _is_illustrator_role(role: str) -> bool:
    return _normalize_contributor_role(role) == "illustrator"