# routers/catalog.py — Catalog API: list works, full work payload (legacy-compatible shape), resolve by ISBN.
# Read/write: works, editions, parties, work_contributors, work_party_preferences,
# party_socials, party_addresses, contributor_* tables, royalty_*, onix_raw_products.

from __future__ import annotations

import json
import uuid
import ast
from typing import Any, Dict, List, Optional, Tuple
from datetime import date, datetime

from fastapi import APIRouter, HTTPException, Query, Request
from psycopg.rows import dict_row

from app.core.db import db_conn

router = APIRouter(prefix="/catalog", tags=["Catalog"])


def _jsonable(v: Any) -> Any:
    if isinstance(v, (datetime, date)):
        return v.isoformat()
    return v


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


def _to_int_or_none(v: Any) -> Optional[int]:
    if v is None or v == "":
        return None
    try:
        return int(v)
    except Exception:
        return None


def _to_float_or_none(v: Any) -> Optional[float]:
    if v is None or v == "":
        return None
    try:
        return float(v)
    except Exception:
        return None


def _safe_str(v: Any) -> str:
    return str(v).strip() if v is not None else ""

def _safe_name(v: Any) -> str:
    return v.strip() if isinstance(v, str) else ""


def _clean_display_name(v: Any) -> str:
    """
    Protect the read path from bad ingests where a whole nested contributor object
    was accidentally serialized into parties.display_name.
    """
    s = _safe_str(v)
    if not s:
        return ""

    if s.startswith("{") and s.endswith("}"):
        try:
            parsed = ast.literal_eval(s)
            if isinstance(parsed, dict):
                candidate = _safe_str(parsed.get("name"))
                if candidate:
                    return candidate
        except Exception:
            pass

    if s.startswith("[") and s.endswith("]"):
        return ""

    return s
def _dm_merge_conditions(rows: List[dict]) -> List[dict]:
    out: List[dict] = []
    used = set()

    for i, row in enumerate(rows):
        if i in used:
            continue

        kind = _safe_str(row.get("kind"))
        comp = _safe_str(row.get("comparator"))
        val = row.get("value")

        if comp in (">=", ">"):
            for j, other in enumerate(rows):
                if j == i or j in used:
                    continue
                if _safe_str(other.get("kind")) == kind and _safe_str(other.get("comparator")) in ("<=", "<"):
                    low = float(val)
                    high = float(other.get("value"))
                    out.append({"kind": kind, "comparator": "between", "value": [low, high]})
                    used.add(i)
                    used.add(j)
                    break

        if i not in used:
            out.append(row)
            used.add(i)

    return out


def _hydrate_dealmemo_royalties(cur, draft_id: str) -> dict:
    cur.execute(
        """
        SELECT *
        FROM deal_memo_royalty_rules
        WHERE deal_memo_draft_id = %s
        ORDER BY party ASC, rights_type ASC, created_at ASC, id ASC
        """,
        (draft_id,),
    )
    rules = cur.fetchall() or []

    cur.execute(
        """
        SELECT *
        FROM deal_memo_royalty_tiers
        WHERE rule_id IN (
          SELECT id
          FROM deal_memo_royalty_rules
          WHERE deal_memo_draft_id = %s
        )
        ORDER BY rule_id ASC, tier_order ASC, id ASC
        """,
        (draft_id,),
    )
    tier_rows = cur.fetchall() or []

    cur.execute(
        """
        SELECT c.*
        FROM deal_memo_royalty_tier_conditions c
        WHERE c.tier_id IN (
          SELECT t.id
          FROM deal_memo_royalty_tiers t
          WHERE t.rule_id IN (
            SELECT r.id
            FROM deal_memo_royalty_rules r
            WHERE r.deal_memo_draft_id = %s
          )
        )
        ORDER BY c.tier_id ASC, c.created_at ASC, c.id ASC
        """,
        (draft_id,),
    )
    cond_rows = cur.fetchall() or []

    conds_by_tier: Dict[str, List[dict]] = {}
    for c in cond_rows:
        conds_by_tier.setdefault(str(c["tier_id"]), []).append(
            {
                "kind": _safe_str(c.get("kind")),
                "comparator": _safe_str(c.get("comparator")),
                "value": float(c["value"]) if c.get("value") is not None else 0,
            }
        )

    tiers_by_rule: Dict[str, List[dict]] = {}
    for t in tier_rows:
        tier_id = str(t["id"])
        tiers_by_rule.setdefault(str(t["rule_id"]), []).append(
            {
                "rate_percent": float(t["rate_percent"]) if t.get("rate_percent") is not None else 0,
                "base": _safe_str(t.get("base")) or "list_price",
                "note": _safe_str(t.get("note")),
                "conditions": _dm_merge_conditions(conds_by_tier.get(tier_id, [])),
            }
        )

    out = {
        "author": {"first_rights": [], "subrights": []},
        "illustrator": {"first_rights": [], "subrights": []},
    }

    for r in rules:
        party = _safe_str(r.get("party")).lower() or "author"
        rights_type = _safe_str(r.get("rights_type")) or "first_rights"

        if party not in out:
            continue

        rule_obj = {
            "format": _safe_str(r.get("format_label")),
            "name": _safe_str(r.get("subrights_name")),
            "mode": _safe_str(r.get("mode")) or ("tiered" if rights_type == "first_rights" else "fixed"),
            "base": _safe_str(r.get("base")) or "list_price",
            "escalating": bool(r.get("escalating") or False),
            "flat_rate_percent": float(r["flat_rate_percent"]) if r.get("flat_rate_percent") is not None else None,
            "percent": float(r["percent"]) if r.get("percent") is not None else None,
            "note": _safe_str(r.get("notes")),
            "tiers": tiers_by_rule.get(str(r["id"]), []),
        }
        out[party].setdefault(rights_type, []).append(rule_obj)

    out["author"].setdefault("subrights", [])
    out["illustrator"].setdefault("subrights", [])
    return out


def _empty_royalties() -> dict:
    return {
        "author": {"first_rights": [], "subrights": []},
        "illustrator": {"first_rights": [], "subrights": []},
    }

def _normalize_contact_category_name(category: str, scope: str) -> str:
    c = _safe_str(category).strip().lower()
    s = _safe_str(scope).strip().lower()

    prefixes = (
        f"{s}_",
        "author_",
        "illustrator_",
    )
    for prefix in prefixes:
        if c.startswith(prefix):
            c = c[len(prefix):]
            break

    return c

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


def _is_blank_row(d: Optional[Dict[str, Any]]) -> bool:
    if not d:
        return True
    return not any(_safe_str(v) for v in d.values())


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


def _extract_name_email_address_from_obj(obj: Any) -> Tuple[str, str, Dict[str, Any]]:
    if not isinstance(obj, dict):
        return "", "", {}

    name = _safe_str(obj.get("name"))
    email = _safe_str(obj.get("email"))
    addr = obj.get("address") if isinstance(obj.get("address"), dict) else {}
    return name, email, addr


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
        "address": address if address else (
            payload.get(f"{scope}_address")
            if isinstance(payload.get(f"{scope}_address"), dict)
            else {}
        ),
        "website": _safe_str(obj.get("website") or payload.get(f"{scope}_website")),
        "phone_country_code": _safe_str(obj.get("phone_country_code") or payload.get(f"{scope}_phone_country_code")),
        "phone_number": _safe_str(obj.get("phone_number") or payload.get(f"{scope}_phone_number")),
    }

def _has_real_contributor(payload: Dict[str, Any], scope: str) -> bool:
    info = _contributor_input(payload, scope)
    return bool(
        _safe_str(info.get("name"))
        or _safe_str(info.get("email"))
        or any(_safe_str((info.get("address") or {}).get(k)) for k in ("street", "city", "state", "zip", "country"))
        or _safe_str(payload.get(f"{scope}_bio"))
        or _safe_str(payload.get(f"{scope}_book_bio"))
        or _safe_str(payload.get(f"{scope}_website_bio"))
    )


def _get_tenant_id_from_slug(cur, tenant_slug: str) -> str:
    cur.execute(
        "SELECT id FROM tenants WHERE lower(slug) = lower(%s) LIMIT 1",
        (tenant_slug.strip(),),
    )
    row = cur.fetchone()
    if not row:
        cur.execute("SELECT id FROM tenants ORDER BY id LIMIT 1")
        row = cur.fetchone()
    if not row:
        raise HTTPException(status_code=404, detail=f"Unknown tenant_slug: {tenant_slug} (and no tenants in DB)")
    return str(row["id"])


def tenant_id_from_slug(conn, tenant_slug: str) -> str:
    with conn.cursor(row_factory=dict_row) as cur:
        return _get_tenant_id_from_slug(cur, tenant_slug)


def _work_row_to_list_item(row: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "id": str(row["id"]),
        "uid": str(row["uid"]) if row.get("uid") else str(row["id"]),
        "title": row.get("title") or "",
        "subtitle": row.get("subtitle") or "",
        "author": "",
        "series": row.get("series_title") or "",
        "publishing_year": row.get("publishing_year"),
        "publication_date": _jsonable(row.get("publication_date")),
        "publisher_or_imprint": row.get("publisher_or_imprint") or row.get("imprint_name") or row.get("publisher_name") or "",
        "language": row.get("language") or "",
        "rights": row.get("rights") or "",
        "cover_image_link": row.get("cover_image_link") or "",
        "publishing_status": row.get("publishing_status") or "",
        "updated_at": _jsonable(row.get("updated_at")),
        "created_at": _jsonable(row.get("created_at")),
    }


def _first_non_empty_list(*vals: Any) -> List[Dict[str, Any]]:
    for v in vals:
        if isinstance(v, list) and v:
            return v
    return []


def _category_rows(contact_categories: Dict[str, List[Dict[str, Any]]], *aliases: str) -> List[Dict[str, Any]]:
    for name in aliases:
        rows = contact_categories.get(name)
        if isinstance(rows, list) and rows:
            return rows
    return []


def _fetch_party_core(cur, tenant_id: str, party_id: str) -> Dict[str, Any]:
    cur.execute(
        """
        SELECT
            id,
            tenant_id,
            party_type,
            display_name,
            email,
            website,
            phone_country_code,
            phone_number
        FROM parties
        WHERE tenant_id = %s
          AND id = %s
        LIMIT 1
        """,
        (tenant_id, party_id),
    )
    r = cur.fetchone()
    if not r:
        return {}
    return {
        "id": str(r.get("id")),
        "party_type": _safe_str(r.get("party_type")),
        "display_name": _clean_display_name(r.get("display_name")),
        "email": _safe_str(r.get("email")),
        "website": _safe_str(r.get("website")),
        "phone_country_code": _safe_str(r.get("phone_country_code")),
        "phone_number": _safe_str(r.get("phone_number")),
    }


def _fetch_party_summary(cur, tenant_id: str, party_id: str) -> Dict[str, Any]:
    cur.execute(
        """
        SELECT
            display_name,
            email,
            website,
            phone_country_code,
            phone_number,
            short_bio,
            long_bio,
            birth_date,
            birth_city,
            birth_country,
            citizenship
        FROM parties
        WHERE tenant_id = %s
          AND id = %s
        LIMIT 1
        """,
        (tenant_id, party_id),
    )
    r = cur.fetchone()
    if not r:
        return {}
    return {
        "display_name": _clean_display_name(r.get("display_name")),
        "email": _safe_str(r.get("email")),
        "website": _safe_str(r.get("website")),
        "phone_country_code": _safe_str(r.get("phone_country_code")),
        "phone_number": _safe_str(r.get("phone_number")),
        "short_bio": _safe_str(r.get("short_bio")),
        "long_bio": _safe_str(r.get("long_bio")),
        "birth_date": _jsonable(r.get("birth_date")),
        "birth_city": _safe_str(r.get("birth_city")),
        "birth_country": _safe_str(r.get("birth_country")),
        "citizenship": _safe_str(r.get("citizenship")),
    }


def _fetch_party_address_lines(cur, tenant_id: str, party_id: str) -> List[str]:
    try:
        try:
            cur.execute(
                """
                SELECT street, city, state, postal_code, country
                FROM party_addresses
                WHERE tenant_id = %s
                  AND party_id = %s
                ORDER BY label = 'primary' DESC, id ASC
                LIMIT 1
                """,
                (tenant_id, party_id),
            )
            r = cur.fetchone()
        except Exception:
            cur.execute(
                """
                SELECT street, city, state, zip, country
                FROM party_addresses
                WHERE tenant_id = %s
                  AND party_id = %s
                ORDER BY id ASC
                LIMIT 1
                """,
                (tenant_id, party_id),
            )
            r = cur.fetchone()

        if not r:
            return []

        street = _safe_str(r.get("street"))
        city = _safe_str(r.get("city"))
        state = _safe_str(r.get("state"))
        postal = _safe_str(r.get("postal_code") or r.get("zip"))
        country = _safe_str(r.get("country"))

        line1 = street
        line2 = ", ".join([x for x in (city, state, postal, country) if x])
        return [x for x in (line1, line2) if x]
    except Exception:
        return []


def _fetch_party_address(cur, tenant_id: str, party_id: str) -> Dict[str, Any]:
    try:
        try:
            cur.execute(
                """
                SELECT street, city, state, postal_code, country
                FROM party_addresses
                WHERE tenant_id = %s
                  AND party_id = %s
                ORDER BY label = 'primary' DESC, id ASC
                LIMIT 1
                """,
                (tenant_id, party_id),
            )
            r = cur.fetchone()
            if not r:
                return {}
            return {
                "street": _safe_str(r.get("street")),
                "city": _safe_str(r.get("city")),
                "state": _safe_str(r.get("state")),
                "zip": _safe_str(r.get("postal_code")),
                "country": _safe_str(r.get("country")),
            }
        except Exception:
            cur.execute(
                """
                SELECT street, city, state, zip, country
                FROM party_addresses
                WHERE tenant_id = %s
                  AND party_id = %s
                ORDER BY id ASC
                LIMIT 1
                """,
                (tenant_id, party_id),
            )
            r = cur.fetchone()
            if not r:
                return {}
            return {
                "street": _safe_str(r.get("street")),
                "city": _safe_str(r.get("city")),
                "state": _safe_str(r.get("state")),
                "zip": _safe_str(r.get("zip")),
                "country": _safe_str(r.get("country")),
            }
    except Exception:
        return {}


def _fetch_editions(cur, tenant_id: str, work_id: str) -> List[Dict[str, Any]]:
    try:
        cur.execute(
            """
            SELECT
                e.id,
                e.isbn13,
                e.status,
                e.product_form,
                e.product_form_detail,
                e.publication_date,
                e.number_of_pages,
                e.height,
                e.width,
                e.thickness,
                e.unit_weight,
                e.created_at,
                e.updated_at,
                (
                    SELECT ep.price_amount
                    FROM edition_supply_details sd
                    JOIN edition_prices ep ON ep.supply_detail_id = sd.id
                    WHERE sd.tenant_id = e.tenant_id
                      AND sd.edition_id = e.id
                      AND upper(coalesce(ep.currency_code, '')) = 'USD'
                    ORDER BY ep.id
                    LIMIT 1
                ) AS price_us,
                (
                    SELECT ep.price_amount
                    FROM edition_supply_details sd
                    JOIN edition_prices ep ON ep.supply_detail_id = sd.id
                    WHERE sd.tenant_id = e.tenant_id
                      AND sd.edition_id = e.id
                      AND upper(coalesce(ep.currency_code, '')) IN ('CAD', 'CAN')
                    ORDER BY ep.id
                    LIMIT 1
                ) AS price_can
            FROM editions e
            WHERE e.tenant_id = %s
              AND e.work_id = %s
            ORDER BY e.created_at ASC, e.id ASC
            """,
            (tenant_id, work_id),
        )
        rows = cur.fetchall() or []
    except Exception:
        try:
            cur.execute(
                """
                SELECT
                    id, isbn13, status, product_form, product_form_detail,
                    publication_date, number_of_pages, height, width, thickness,
                    unit_weight, created_at, updated_at
                FROM editions
                WHERE tenant_id = %s
                  AND work_id = %s
                ORDER BY created_at ASC, id ASC
                """,
                (tenant_id, work_id),
            )
            rows = cur.fetchall() or []
        except Exception:
            return []

    out: List[Dict[str, Any]] = []
    for r in rows:
        out.append({
            "id": str(r["id"]),
            "isbn": r.get("isbn13") or "",
            "isbn13": r.get("isbn13") or "",
            "status": r.get("status") or "",
            "format": r.get("product_form_detail") or r.get("product_form") or "",
            "pub_date": _jsonable(r.get("publication_date")) or "",
            "price_us": float(r["price_us"]) if r.get("price_us") is not None else 0,
            "price_can": float(r["price_can"]) if r.get("price_can") is not None else 0,
            "pages": r.get("number_of_pages") or 0,
            "tall": float(r["height"]) if r.get("height") is not None else 0,
            "wide": float(r["width"]) if r.get("width") is not None else 0,
            "spine": float(r["thickness"]) if r.get("thickness") is not None else 0,
            "weight": float(r["unit_weight"]) if r.get("unit_weight") is not None else 0,
            "created_at": _jsonable(r.get("created_at")),
            "updated_at": _jsonable(r.get("updated_at")),
        })
    return out

def _normalize_date_string(v: Any) -> Optional[str]:
    if v in (None, "", "null"):
        return None
    if isinstance(v, (datetime, date)):
        return v.isoformat()[:10]
    s = str(v).strip()
    if not s:
        return None
    try:
        return date.fromisoformat(s[:10]).isoformat()
    except Exception:
        return None


def _fetch_foreign_rights_sold(cur, tenant_id: str, work_id: str) -> List[Dict[str, Any]]:
    try:
        cur.execute(
            """
            SELECT
                id,
                country,
                agency,
                sold_date,
                expiration_date,
                notes,
                created_at,
                updated_at
            FROM work_foreign_rights_sold
            WHERE tenant_id = %s
              AND work_id = %s
            ORDER BY country ASC, sold_date ASC NULLS LAST, created_at ASC, id ASC
            """,
            (tenant_id, work_id),
        )
        rows = cur.fetchall() or []
    except Exception:
        return []

    out: List[Dict[str, Any]] = []
    for r in rows:
        out.append(
            {
                "id": str(r["id"]),
                "country": _safe_str(r.get("country")),
                "agency": _safe_str(r.get("agency")),
                "sold_date": _jsonable(r.get("sold_date")),
                "expiration_date": _jsonable(r.get("expiration_date")),
                "date": _jsonable(r.get("sold_date")),              # compatibility for current UI
                "expiration": _jsonable(r.get("expiration_date")),  # compatibility for current UI
                "notes": _safe_str(r.get("notes")),
                "created_at": _jsonable(r.get("created_at")),
                "updated_at": _jsonable(r.get("updated_at")),
            }
        )
    return out


def _clean_foreign_rights_sold_rows(rows: Any) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for row in (rows or []):
        if not isinstance(row, dict):
            continue

        country = _safe_str(row.get("country"))
        agency = _safe_str(row.get("agency"))
        sold_date = _normalize_date_string(row.get("sold_date") or row.get("date"))
        expiration_date = _normalize_date_string(row.get("expiration_date") or row.get("expiration"))
        notes = _safe_str(row.get("notes"))

        if not any([country, agency, sold_date, expiration_date, notes]):
            continue

        out.append(
            {
                "country": country,
                "agency": agency,
                "sold_date": sold_date,
                "expiration_date": expiration_date,
                "notes": notes,
            }
        )
    return out


def _replace_foreign_rights_sold(cur, tenant_id: str, work_id: str, payload: Dict[str, Any]) -> None:
    rows = _clean_foreign_rights_sold_rows(payload.get("foreign_rights_sold") or payload.get("foreignRightsSold") or [])

    cur.execute(
        "DELETE FROM work_foreign_rights_sold WHERE tenant_id = %s AND work_id = %s",
        (tenant_id, work_id),
    )

    for row in rows:
        cur.execute(
            """
            INSERT INTO work_foreign_rights_sold (
                tenant_id,
                work_id,
                country,
                agency,
                sold_date,
                expiration_date,
                notes
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            (
                tenant_id,
                work_id,
                row["country"],
                row["agency"],
                _parse_date_or_none(row["sold_date"]),
                _parse_date_or_none(row["expiration_date"]),
                row["notes"],
            ),
        )

def _fetch_contributor_contact_categories(cur, tenant_id: str, party_id: str, scope: str) -> Dict[str, List[Dict[str, Any]]]:
    out: Dict[str, List[Dict[str, Any]]] = {}

    try:
        cur.execute(
            """
            SELECT
                ccl.category,
                ccl.link_type,
                ccl.item_order,
                ccl.personal_contact,
                ccl.relationship_note,
                cd.contact_type,
                cd.name,
                cd.company_or_outlet,
                cd.position,
                cd.email,
                cd.phone,
                cd.website,
                cd.street,
                cd.city,
                cd.state,
                cd.zip,
                cd.country,
                cd.social_handle,
                cd.notes
            FROM contributor_contact_links ccl
            JOIN contact_directory cd
              ON cd.id = ccl.contact_id
            WHERE ccl.tenant_id = %s
              AND ccl.party_id = %s
              AND lower(ccl.scope) = %s
            ORDER BY ccl.category ASC, ccl.item_order ASC, ccl.id ASC
            """,
            (tenant_id, party_id, scope.lower()),
        )
        rows = cur.fetchall() or []
    except Exception:
        return out

    for r in rows:
        raw_category = _safe_str(r.get("category"))
        if not raw_category:
            continue

        normalized_category = _normalize_contact_category_name(raw_category, scope)

        label = _safe_str(r.get("name")) or _safe_str(r.get("company_or_outlet"))

        item = {
            "name": label,
            "outlet": _safe_str(r.get("company_or_outlet")),
            "company": _safe_str(r.get("company_or_outlet")),
            "contact": _safe_str(r.get("name")),
            "position": _safe_str(r.get("position")),
            "email": _safe_str(r.get("email")),
            "phone": _safe_str(r.get("phone")),
            "url": _safe_str(r.get("website")),
            "city": _safe_str(r.get("city")),
            "state": _safe_str(r.get("state")),
            "zip": _safe_str(r.get("zip")),
            "country": _safe_str(r.get("country")),
            "social_handle": _safe_str(r.get("social_handle")),
            "personal_contact": bool(r.get("personal_contact") or False),
            "relationship": _safe_str(r.get("relationship_note")),
            "connection": _safe_str(r.get("relationship_note")),
            "notes": _safe_str(r.get("notes")),
            "link_type": _safe_str(r.get("link_type")),
            "contact_type": _safe_str(r.get("contact_type")),
        }

        out.setdefault(raw_category, []).append(item)
        if normalized_category and normalized_category != raw_category:
            out.setdefault(normalized_category, []).append(item)

    return out


def _fetch_contributor_marketing_profile(cur, tenant_id: str, party_id: str, scope: str) -> Dict[str, Any]:
    try:
        cur.execute(
            """
            SELECT *
            FROM contributor_marketing_profiles
            WHERE tenant_id = %s
              AND party_id = %s
              AND lower(scope) = %s
            LIMIT 1
            """,
            (tenant_id, party_id, scope.lower()),
        )
        r = cur.fetchone()
        if not r:
            return {}

        keys = (
            "website_bio",
            "book_bio",
            "contact_pref_rank1",
            "contact_pref_rank2",
            "media_best_times",
            "media_press_share",
            "us_travel_plans",
            "travel_dates",
            "additional_notes",
            "photo_credit",
            "present_position",
            "former_positions",
            "degrees_honors",
            "professional_honors",
        )
        out: Dict[str, Any] = {}
        for k in keys:
            if k in r:
                out[k] = r.get(k)
        return out
    except Exception:
        return {}


def _fetch_party_extras_block(cur, tenant_id: str, party_id: str, work_id: str, scope: str) -> Dict[str, Any]:
    pref: Dict[str, Any] = {}
    profile = _fetch_contributor_marketing_profile(cur, tenant_id, party_id, scope)
    if profile:
        pref = dict(profile)
    else:
        try:
            cur.execute(
                """
                SELECT
                    contact_pref_rank1,
                    contact_pref_rank2,
                    media_best_times,
                    media_press_share,
                    us_travel_plans,
                    travel_dates
                FROM work_party_preferences
                WHERE tenant_id = %s
                  AND party_id = %s
                  AND work_id = %s
                LIMIT 1
                """,
                (tenant_id, party_id, work_id),
            )
            r = cur.fetchone()
            if r:
                pref = {
                    "contact_pref_rank1": r.get("contact_pref_rank1"),
                    "contact_pref_rank2": r.get("contact_pref_rank2"),
                    "media_best_times": r.get("media_best_times"),
                    "media_press_share": r.get("media_press_share"),
                    "us_travel_plans": r.get("us_travel_plans"),
                    "travel_dates": r.get("travel_dates"),
                }
        except Exception:
            pass

    try:
        cur.execute(
            """
            SELECT platform, url
            FROM party_socials
            WHERE tenant_id = %s
              AND party_id = %s
            ORDER BY platform ASC, id ASC
            """,
            (tenant_id, party_id),
        )
        socials = [
            {
                "platform": _safe_str(r.get("platform")),
                "url": _safe_str(r.get("url")),
                "handle": "",
            }
            for r in (cur.fetchall() or [])
        ]
    except Exception:
        socials = []

    try:
        cur.execute(
            """
            SELECT *
            FROM contributor_published_books
            WHERE tenant_id = %s
              AND party_id = %s
              AND lower(scope) = %s
            ORDER BY item_order ASC, title ASC, id ASC
            """,
            (tenant_id, party_id, scope.lower()),
        )
        pubs = [
            {
                "title": _safe_str(r.get("title")),
                "isbn": _safe_str(r.get("isbn")),
                "publisher": _safe_str(r.get("publisher")),
                "year": _safe_str(r.get("publication_year")),
                "approx_sold": _safe_str(r.get("approx_sold")),
            }
            for r in (cur.fetchall() or [])
        ]
    except Exception:
        pubs = []

    try:
        cur.execute(
            """
            SELECT *
            FROM contributor_media_appearances
            WHERE tenant_id = %s
              AND party_id = %s
              AND lower(scope) = %s
            ORDER BY item_order ASC, id ASC
            """,
            (tenant_id, party_id, scope.lower()),
        )
        media = [
            {
                "title": _safe_str(r.get("title")),
                "venue": _safe_str(r.get("venue")),
                "date": _safe_str(r.get("date_text")),
                "appearance_date": _safe_str(r.get("date_text")),
                "link": _safe_str(r.get("link")),
                "notes": _safe_str(r.get("notes")),
            }
            for r in (cur.fetchall() or [])
        ]
    except Exception:
        media = []

    try:
        cur.execute(
            """
            SELECT *
            FROM contributor_other_publications
            WHERE tenant_id = %s
              AND party_id = %s
            ORDER BY id ASC
            """,
            (tenant_id, party_id),
        )
        other_pubs = [
            {
                "title": _safe_str(r.get("title")),
                "publication_name": _safe_str(r.get("publication_name")),
                "publication_type": _safe_str(r.get("publication_type")),
                "publication_date": _jsonable(r.get("publication_date")),
                "notes": _safe_str(r.get("notes")),
            }
            for r in (cur.fetchall() or [])
        ]
    except Exception:
        other_pubs = []

    try:
        cur.execute(
            """
            SELECT *
            FROM contributor_media_contacts
            WHERE tenant_id = %s
              AND party_id = %s
              AND lower(scope) = %s
            ORDER BY item_order ASC, id ASC
            """,
            (tenant_id, party_id, scope.lower()),
        )
        media_contacts = [
            {
                "company": _safe_str(r.get("company")),
                "name": _safe_str(r.get("name")),
                "position": _safe_str(r.get("position")),
                "phone": _safe_str(r.get("phone")),
                "email": _safe_str(r.get("email")),
            }
            for r in (cur.fetchall() or [])
        ]
    except Exception:
        media_contacts = []

    try:
        cur.execute(
            """
            SELECT
                id,
                scope,
                item_order,
                outlet_or_title,
                contact,
                relationship_note,
                notes,
                source_category
            FROM contributor_previous_publicity
            WHERE tenant_id = %s
              AND party_id = %s
              AND lower(scope) = %s
            ORDER BY item_order ASC, id ASC
            """,
            (tenant_id, party_id, scope.lower()),
        )
        previous_publicity = [
            {
                "outlet_or_title": _safe_str(r.get("outlet_or_title")),
                "contact": _safe_str(r.get("contact")),
                "relationship_note": _safe_str(r.get("relationship_note")),
                "relationship": _safe_str(r.get("relationship_note")),
                "notes": _safe_str(r.get("notes")),
                "source_category": _safe_str(r.get("source_category")),
            }
            for r in (cur.fetchall() or [])
        ]
    except Exception:
        previous_publicity = []

    try:
        cur.execute(
            """
            SELECT *
            FROM contributor_niche_publicity_targets
            WHERE tenant_id = %s
              AND party_id = %s
            ORDER BY id ASC
            """,
            (tenant_id, party_id),
        )
        niche_targets = [
            {
                "target_name": _safe_str(r.get("target_name")),
                "notes": _safe_str(r.get("notes")),
            }
            for r in (cur.fetchall() or [])
        ]
    except Exception:
        niche_targets = []

    return {
        "preferences": pref,
        "profile": profile,
        "socials": socials,
        "published_books": pubs,
        "media_appearances": media,
        "other_publications": other_pubs,
        "media_contacts": media_contacts,
        "previous_publicity": previous_publicity,
        "niche_publicity_targets": niche_targets,
    }


def _fetch_agent_for_party(
    cur,
    tenant_id: str,
    represented_party_id: str,
    work_id: str
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    agency_card: Dict[str, Any] = {}

    try:
        cur.execute(
            """
            SELECT pr.agent_party_id
            FROM party_representations pr
            WHERE pr.tenant_id = %s
              AND pr.represented_party_id = %s
              AND (pr.work_id = %s OR pr.work_id IS NULL)
            ORDER BY (pr.work_id IS NOT NULL) DESC, pr.id ASC
            LIMIT 1
            """,
            (tenant_id, represented_party_id, work_id),
        )
        rep_row = cur.fetchone()
        if not rep_row:
            return [], {}

        linked_party_id = str(rep_row["agent_party_id"])
        linked_party = _fetch_party_core(cur, tenant_id, linked_party_id)
        if not linked_party:
            return [], {}

        agency_party: Dict[str, Any] = {}
        primary_agent_party: Dict[str, Any] = {}
        linked_agents: List[Dict[str, Any]] = []

        cur.execute(
            """
            SELECT
                l.agent_party_id,
                l.is_primary,
                l.role_label,
                ag.display_name AS agent_name,
                ag.email AS agent_email,
                ag.website AS agent_website,
                ag.phone_country_code AS agent_phone_country_code,
                ag.phone_number AS agent_phone_number
            FROM agency_agent_links l
            JOIN parties ag
              ON ag.id = l.agent_party_id
             AND ag.tenant_id = l.tenant_id
            WHERE l.tenant_id = %s
              AND l.agency_party_id = %s
            ORDER BY l.is_primary DESC, ag.display_name ASC, l.id ASC
            """,
            (tenant_id, linked_party_id),
        )
        rows_as_agency = cur.fetchall() or []

        if rows_as_agency:
            agency_party = linked_party
            for r in rows_as_agency:
                linked_agents.append({
                    "agent_name": _clean_display_name(r.get("agent_name")),
                    "agent_email": _safe_str(r.get("agent_email")),
                    "agent_phone_country_code": _safe_str(r.get("agent_phone_country_code")),
                    "agent_phone_number": _safe_str(r.get("agent_phone_number")),
                    "agent_phone": _format_phone(r.get("agent_phone_country_code"), r.get("agent_phone_number")),
                    "is_primary": bool(r.get("is_primary")),
                    "role_label": _safe_str(r.get("role_label")),
                })
            primary_agent_party = {
                "display_name": _clean_display_name(rows_as_agency[0].get("agent_name")),
                "email": _safe_str(rows_as_agency[0].get("agent_email")),
                "website": _safe_str(rows_as_agency[0].get("agent_website")),
                "phone_country_code": _safe_str(rows_as_agency[0].get("agent_phone_country_code")),
                "phone_number": _safe_str(rows_as_agency[0].get("agent_phone_number")),
            }
        else:
            cur.execute(
                """
                SELECT
                    l.agency_party_id,
                    l.is_primary,
                    l.role_label
                FROM agency_agent_links l
                WHERE l.tenant_id = %s
                  AND l.agent_party_id = %s
                ORDER BY l.is_primary DESC, l.id ASC
                LIMIT 1
                """,
                (tenant_id, linked_party_id),
            )
            reverse_link = cur.fetchone()

            if reverse_link:
                agency_party_id = str(reverse_link["agency_party_id"])
                agency_party = _fetch_party_core(cur, tenant_id, agency_party_id)
                primary_agent_party = linked_party

                cur.execute(
                    """
                    SELECT
                        l.agent_party_id,
                        l.is_primary,
                        l.role_label,
                        ag.display_name AS agent_name,
                        ag.email AS agent_email,
                        ag.website AS agent_website,
                        ag.phone_country_code AS agent_phone_country_code,
                        ag.phone_number AS agent_phone_number
                    FROM agency_agent_links l
                    JOIN parties ag
                      ON ag.id = l.agent_party_id
                     AND ag.tenant_id = l.tenant_id
                    WHERE l.tenant_id = %s
                      AND l.agency_party_id = %s
                    ORDER BY l.is_primary DESC, ag.display_name ASC, l.id ASC
                    """,
                    (tenant_id, agency_party_id),
                )
                rows_for_agency = cur.fetchall() or []

                for r in rows_for_agency:
                    linked_agents.append({
                        "agent_name": _clean_display_name(r.get("agent_name")),
                        "agent_email": _safe_str(r.get("agent_email")),
                        "agent_phone_country_code": _safe_str(r.get("agent_phone_country_code")),
                        "agent_phone_number": _safe_str(r.get("agent_phone_number")),
                        "agent_phone": _format_phone(r.get("agent_phone_country_code"), r.get("agent_phone_number")),
                        "is_primary": bool(r.get("is_primary")),
                        "role_label": _safe_str(r.get("role_label")),
                    })
            else:
                primary_agent_party = linked_party
                agency_party = {}

        out = linked_agents

        agency_name = _clean_display_name(agency_party.get("display_name"))
        agency_email = _safe_str(agency_party.get("email"))
        agency_website = _safe_str(agency_party.get("website"))
        agency_phone = _format_phone(agency_party.get("phone_country_code"), agency_party.get("phone_number"))

        primary_agent_name = _clean_display_name(primary_agent_party.get("display_name"))
        primary_agent_email = _safe_str(primary_agent_party.get("email"))
        primary_agent_phone = _format_phone(primary_agent_party.get("phone_country_code"), primary_agent_party.get("phone_number"))

        address_lines: List[str] = []
        if agency_party.get("id"):
            address_lines = _fetch_party_address_lines(cur, tenant_id, str(agency_party["id"]))
        if not address_lines and primary_agent_party.get("id"):
            address_lines = _fetch_party_address_lines(cur, tenant_id, str(primary_agent_party["id"]))

        agency_card = {
            "agency": agency_name,
            "agent": primary_agent_name,
            "contact": primary_agent_name,
            "email": primary_agent_email or agency_email,
            "phone": agency_phone or primary_agent_phone,
            "website": agency_website,
            "addressLines": address_lines,
        }

        if _is_blank_row(agency_card):
            return out, {}

        return out, agency_card

    except Exception:
        return [], {}


def _fetch_contributors(cur, tenant_id: str, work_id: str) -> List[Dict[str, Any]]:
    cur.execute(
        """
        SELECT
            wc.party_id,
            wc.contributor_role,
            wc.sequence_number,
            p.display_name
        FROM work_contributors wc
        JOIN parties p ON p.id = wc.party_id
        WHERE wc.tenant_id = %s
          AND wc.work_id = %s
          AND p.tenant_id = %s
        ORDER BY wc.sequence_number ASC, wc.id ASC
        """,
        (tenant_id, work_id, tenant_id),
    )
    rows = cur.fetchall() or []
    return [
        {
            "party_id": str(r["party_id"]),
            "role": _safe_str(r.get("contributor_role")),
            "scope": _role_to_scope(_safe_str(r.get("contributor_role"))),
            "sequence_number": r.get("sequence_number") or 0,
            "display_name": _clean_display_name(r.get("display_name")),
        }
        for r in rows
    ]


def _fetch_royalties_graph(cur, tenant_id: str, work_id: str) -> Dict[str, Any]:
    cur.execute(
        """
        SELECT id, version, is_active, source_json
        FROM royalty_sets
        WHERE tenant_id = %s
          AND work_id = %s
        ORDER BY is_active DESC, version DESC
        LIMIT 1
        """,
        (tenant_id, work_id),
    )
    set_row = cur.fetchone()
    if not set_row:
        return {"author": {"first_rights": [], "subrights": []}, "illustrator": {"first_rights": [], "subrights": []}}

    royalty_set_id = set_row["id"]

    try:
        cur.execute(
            """
            SELECT
                id, party, rights_type, format_label, subrights_name,
                mode, base, escalating, flat_rate_percent, percent, notes
            FROM royalty_rules
            WHERE tenant_id = %s
              AND royalty_set_id = %s
            ORDER BY party ASC, rights_type ASC, id ASC
            """,
            (tenant_id, royalty_set_id),
        )
        rules = cur.fetchall() or []
    except Exception:
        rules = []

    try:
        cur.execute(
            """
            SELECT
                t.id AS tier_id,
                t.rule_id,
                t.tier_order,
                t.rate_percent,
                t.base,
                t.note
            FROM royalty_tiers t
            WHERE t.tenant_id = %s
              AND t.rule_id IN (
                  SELECT id
                  FROM royalty_rules
                  WHERE tenant_id = %s
                    AND royalty_set_id = %s
              )
            ORDER BY t.rule_id ASC, t.tier_order ASC, t.id ASC
            """,
            (tenant_id, tenant_id, royalty_set_id),
        )
        tiers = cur.fetchall() or []
    except Exception:
        tiers = []

    try:
        cur.execute(
            """
            SELECT tier_id, kind, comparator, value
            FROM royalty_tier_conditions
            WHERE tenant_id = %s
              AND tier_id IN (
                  SELECT id
                  FROM royalty_tiers
                  WHERE tenant_id = %s
                    AND rule_id IN (
                        SELECT id
                        FROM royalty_rules
                        WHERE tenant_id = %s
                          AND royalty_set_id = %s
                    )
              )
            ORDER BY tier_id ASC
            """,
            (tenant_id, tenant_id, tenant_id, royalty_set_id),
        )
        conds = cur.fetchall() or []
    except Exception:
        conds = []

    tiers_by_rule: Dict[str, List[Dict[str, Any]]] = {}
    for t in tiers:
        tiers_by_rule.setdefault(str(t["rule_id"]), []).append({
            "tier_order": t.get("tier_order") or 0,
            "rate_percent": float(t.get("rate_percent") or 0),
            "base": t.get("base") or "",
            "note": t.get("note") or "",
            "conditions": [],
            "_tier_id": t["tier_id"],
        })

    conds_by_tier: Dict[str, List[Dict[str, Any]]] = {}
    for c in conds:
        conds_by_tier.setdefault(str(c["tier_id"]), []).append({
            "kind": c.get("kind"),
            "comparator": c.get("comparator"),
            "value": c.get("value"),
        })

    for _, tier_list in tiers_by_rule.items():
        for t in tier_list:
            tid = t.pop("_tier_id")
            t["conditions"] = conds_by_tier.get(str(tid), [])

    out = {
        "author": {"first_rights": [], "subrights": []},
        "illustrator": {"first_rights": [], "subrights": []},
    }

    for r in rules:
        rule_obj = {
            "format": r.get("format_label") or "",
            "name": r.get("subrights_name") or "",
            "mode": r.get("mode") or "",
            "base": r.get("base") or "",
            "escalating": bool(r.get("escalating") or False),
            "flat_rate_percent": float(r["flat_rate_percent"]) if r.get("flat_rate_percent") is not None else None,
            "percent": float(r["percent"]) if r.get("percent") is not None else None,
            "note": r.get("notes") or "",
            "tiers": tiers_by_rule.get(str(r["id"]), []),
        }
        party = (r.get("party") or "").lower()
        rights_type = r.get("rights_type") or ""
        if party not in out:
            continue
        if rights_type == "first_rights":
            out[party]["first_rights"].append(rule_obj)
        else:
            out[party]["subrights"].append(rule_obj)

    return out


def _fetch_onix_raw_by_isbns(cur, tenant_id: str, isbns: List[str], limit_each: int = 1) -> Dict[str, Any]:
    if not isbns:
        return {}
    clean_isbns = [i for i in dict.fromkeys(isbns) if i]
    out: Dict[str, Any] = {}
    for isbn in clean_isbns:
        cur.execute(
            """
            SELECT record_reference, isbn13, product_xml, created_at
            FROM onix_raw_products
            WHERE tenant_id = %s
              AND isbn13 = %s
            ORDER BY created_at DESC
            LIMIT %s
            """,
            (tenant_id, isbn, limit_each),
        )
        rows = cur.fetchall() or []
        if rows:
            out[isbn] = [{
                "record_reference": r.get("record_reference") or "",
                "isbn13": r.get("isbn13") or "",
                "created_at": _jsonable(r.get("created_at")),
                "product_xml": r.get("product_xml") or "",
            } for r in rows]
    return out


def _resolve_work_id_param(cur, tenant_id: str, work_id: str) -> Optional[str]:
    if not work_id or not str(work_id).strip():
        return None
    raw = str(work_id).strip()
    cur.execute(
        "SELECT id FROM works WHERE tenant_id = %s AND id = %s LIMIT 1",
        (tenant_id, raw),
    )
    row = cur.fetchone()
    if row:
        return str(row["id"])
    try:
        uuid.UUID(raw)
    except (ValueError, TypeError):
        return None
    cur.execute(
        "SELECT id FROM works WHERE tenant_id = %s AND uid = %s LIMIT 1",
        (tenant_id, raw),
    )
    row = cur.fetchone()
    return str(row["id"]) if row else None


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


def _upsert_party_minimal(cur, tenant_id: str, display_name: str, party_type: str = "person", email: str = "") -> Optional[str]:
    name = _safe_str(display_name)
    email = _safe_str(email).lower()

    if not name and not email:
        return None

    if email:
        cur.execute(
            """
            SELECT id
            FROM parties
            WHERE tenant_id = %s
              AND lower(coalesce(email, '')) = %s
            LIMIT 1
            """,
            (tenant_id, email),
        )
        row = cur.fetchone()
        if row:
            cur.execute(
                """
                UPDATE parties
                SET
                    display_name = CASE
                        WHEN coalesce(trim(display_name), '') = '' THEN %s
                        ELSE display_name
                    END,
                    updated_at = now()
                WHERE tenant_id = %s
                  AND id = %s
                """,
                (name, tenant_id, row["id"]),
            )
            return str(row["id"])

    if name:
        cur.execute(
            """
            SELECT id
            FROM parties
            WHERE tenant_id = %s
              AND display_name = %s
            LIMIT 1
            """,
            (tenant_id, name),
        )
        row = cur.fetchone()
        if row:
            return str(row["id"])

    pid = str(uuid.uuid4())
    cur.execute(
        """
        INSERT INTO parties (
            id, tenant_id, party_type, display_name,
            names_before_key, key_names, person_name_inverted, corporate_name, email
        )
        VALUES (%s, %s, %s, %s, '', '', '', '', %s)
        """,
        (pid, tenant_id, party_type, name or email or "Unknown", email),
    )
    return pid


def _replace_party_representation(cur, tenant_id: str, represented_party_id: str, work_id: str, agency_payload: Dict[str, Any]) -> None:
    if not agency_payload:
        return

    agency_name = _safe_str(agency_payload.get("agency"))
    agent_name = _safe_str(agency_payload.get("agent") or agency_payload.get("contact"))
    email = _safe_str(agency_payload.get("email"))
    website = _safe_str(agency_payload.get("website"))
    phone = _safe_str(agency_payload.get("phone"))

    agency_party_id: Optional[str] = None
    agent_party_id: Optional[str] = None

    try:
        if agency_name:
            agency_party_id = _upsert_party_minimal(cur, tenant_id, agency_name, party_type="organization", email=email)
            if agency_party_id:
                cur.execute(
                    """
                    UPDATE parties
                    SET
                        email = COALESCE(NULLIF(%s, ''), email),
                        website = COALESCE(NULLIF(%s, ''), website),
                        updated_at = now()
                    WHERE tenant_id = %s AND id = %s
                    """,
                    (email, website, tenant_id, agency_party_id),
                )

        if agent_name:
            agent_party_id = _upsert_party_minimal(cur, tenant_id, agent_name, party_type="person", email=email)
            if agent_party_id:
                cur.execute(
                    """
                    UPDATE parties
                    SET
                        email = COALESCE(NULLIF(%s, ''), email),
                        website = COALESCE(NULLIF(%s, ''), website),
                        phone_number = COALESCE(NULLIF(%s, ''), phone_number),
                        updated_at = now()
                    WHERE tenant_id = %s AND id = %s
                    """,
                    (email, website, phone, tenant_id, agent_party_id),
                )

        representation_target = agency_party_id or agent_party_id
        if not representation_target:
            return

        try:
            cur.execute(
                "DELETE FROM party_representations WHERE tenant_id = %s AND represented_party_id = %s AND (work_id = %s OR work_id IS NULL)",
                (tenant_id, represented_party_id, work_id),
            )
        except Exception:
            pass

        try:
            cur.execute(
                """
                INSERT INTO party_representations (tenant_id, represented_party_id, agent_party_id, work_id)
                VALUES (%s, %s, %s, %s)
                """,
                (tenant_id, represented_party_id, representation_target, work_id),
            )
        except Exception:
            pass

        if agency_party_id and agent_party_id:
            try:
                cur.execute(
                    "DELETE FROM agency_agent_links WHERE tenant_id = %s AND agency_party_id = %s",
                    (tenant_id, agency_party_id),
                )
            except Exception:
                pass

            try:
                cur.execute(
                    """
                    INSERT INTO agency_agent_links (tenant_id, agency_party_id, agent_party_id, is_primary, role_label)
                    VALUES (%s, %s, %s, true, 'agent')
                    """,
                    (tenant_id, agency_party_id, agent_party_id),
                )
            except Exception:
                pass
    except Exception:
        pass

def _apply_contact_category_aliases(doc: Dict[str, Any], scope: str, contact_categories: Dict[str, List[Dict[str, Any]]]) -> None:
    endorsers = _category_rows(
        contact_categories,
        "marketing_endorsers",
        "publicity_endorsers_blurbers",
        "endorsers",
        "blurbers",
    )
    big_mouth = _category_rows(
        contact_categories,
        "marketing_big_mouth_list",
        "publicity_big_mouth_list",
        "big_mouth_list",
    )
    review_copy = _category_rows(
        contact_categories,
        "marketing_review_copy_wishlist",
        "publicity_review_copy_wishlist",
        "review_copy_wishlist",
    )
    local_media = _category_rows(
        contact_categories,
        "marketing_local_media",
        "publicity_local_media",
        "local_media",
    )
    alumni_orgs = _category_rows(
        contact_categories,
        "marketing_alumni_org_publications",
        "publicity_alumni_org_publications",
        "alumni_org_publications",
    )
    targeted_sites = _category_rows(
        contact_categories,
        "marketing_targeted_sites",
        "publicity_target_sites",
        "targeted_sites",
        "target_sites",
    )
    bloggers = _category_rows(
        contact_categories,
        "marketing_bloggers",
        "publicity_bloggers_genre",
        "bloggers",
        "bloggers_genre",
    )

    nontrade = _category_rows(
        contact_categories,
        "sales_nontrade_outlets",
        "nontrade_outlets",
        "sales_nontrade",
    )
    museums = _category_rows(
        contact_categories,
        "sales_museums_parks",
        "museums_parks",
        "museum_park_outlets",
    )
    local_bookstores = _category_rows(
        contact_categories,
        "sales_local_bookstores",
        "local_bookstores",
        "honor_local_bookstores",
    )

    doc[f"{scope}_marketing_endorsers"] = endorsers
    doc[f"{scope}_publicity_endorsers_blurbers"] = endorsers

    doc[f"{scope}_marketing_big_mouth_list"] = big_mouth
    doc[f"{scope}_publicity_big_mouth_list"] = big_mouth

    doc[f"{scope}_marketing_review_copy_wishlist"] = review_copy
    doc[f"{scope}_publicity_review_copy_wishlist"] = review_copy

    doc[f"{scope}_marketing_local_media"] = local_media
    doc[f"{scope}_publicity_local_media"] = local_media

    doc[f"{scope}_marketing_alumni_org_publications"] = alumni_orgs
    doc[f"{scope}_publicity_alumni_org_publications"] = alumni_orgs

    doc[f"{scope}_marketing_targeted_sites"] = targeted_sites
    doc[f"{scope}_publicity_target_sites"] = targeted_sites

    doc[f"{scope}_marketing_bloggers"] = bloggers
    doc[f"{scope}_publicity_bloggers_genre"] = bloggers

    doc[f"{scope}_sales_nontrade_outlets"] = nontrade
    doc[f"{scope}_sales_museums_parks"] = museums
    doc[f"{scope}_sales_local_bookstores"] = local_bookstores

    nontrade = _category_rows(
        contact_categories,
        "sales_nontrade_outlets",
        "nontrade_outlets",
        "sales_nontrade",
    )
    museums = _category_rows(
        contact_categories,
        "sales_museums_parks",
        "museums_parks",
        "museum_park_outlets",
        "honor_local_bookstores",
    )

    doc[f"{scope}_sales_nontrade_outlets"] = nontrade
    doc[f"{scope}_sales_museums_parks"] = museums


def _build_full_work_payload(cur, tenant_id: str, work_id: str) -> Dict[str, Any]:
    resolved = _resolve_work_id_param(cur, tenant_id, work_id)
    if not resolved:
        raise HTTPException(status_code=404, detail="Work not found")
    work_id = resolved

    cur.execute(
        """
        SELECT *
        FROM works
        WHERE tenant_id = %s
          AND id = %s
        LIMIT 1
        """,
        (tenant_id, work_id),
    )
    w = cur.fetchone()
    if not w:
        raise HTTPException(status_code=404, detail="Work not found")

    doc: Dict[str, Any] = {
        "id": str(w["id"]),
        "uid": str(w["uid"]) if w.get("uid") else str(w["id"]),
        "title": _safe_str(w.get("title")),
        "subtitle": _safe_str(w.get("subtitle")) or None,
        "series": _safe_str(w.get("series_title")),
        "volume_number": w.get("series_number") or 0,
        "ages": _safe_str(w.get("ages")),
        "us_grade": _safe_str(w.get("us_grade")),
        "language": _safe_str(w.get("language")),
        "rights": _safe_str(w.get("rights")),
        "editor_name": _safe_str(w.get("editor_name")),
        "art_director_name": _safe_str(w.get("art_director_name")),
        "publisher_or_imprint": _safe_str(w.get("publisher_or_imprint")),
        "publishing_year": w.get("publishing_year"),
        "publication_date": _jsonable(w.get("publication_date")),
        "publishing_status": _safe_str(w.get("publishing_status")),
        "city_of_publication": _safe_str(w.get("city_of_publication")),
        "country_of_publication": _safe_str(w.get("country_of_publication")),
        "copyright_year": int(w.get("copyright_year") or 0),
        "main_description": _safe_str(w.get("main_description")),
        "table_of_contents": _safe_str(w.get("table_of_contents")),
        "back_cover_copy": _safe_str(w.get("back_cover_copy")),
        "biographical_note": _safe_str(w.get("biographical_note")),
        "cover_image_link": _safe_str(w.get("cover_image_link")),
        "cover_image_format": _safe_str(w.get("cover_image_format")),
        "cover_image_caption": _safe_str(w.get("cover_image_caption")),
        "about_summary": _safe_str(w.get("about_summary")),
        "about_bookstore_shelf": _safe_str(w.get("about_bookstore_shelf")),
        "about_audience": _safe_str(w.get("about_audience")),
        "about_dates_holidays": _safe_str(w.get("about_dates_holidays")),
        "loc_number": _safe_str(w.get("loc_number")),
    }

    editions = _fetch_editions(cur, tenant_id, work_id)
    doc["formats"] = [{
        "format": e.get("format") or "",
        "isbn": e.get("isbn") or "",
        "pub_date": e.get("pub_date") or "",
        "price_us": e.get("price_us") or 0,
        "price_can": e.get("price_can") or 0,
        "pages": e.get("pages") or 0,
        "tall": e.get("tall") or 0,
        "wide": e.get("wide") or 0,
        "spine": e.get("spine") or 0,
        "weight": e.get("weight") or 0,
    } for e in editions]
    doc["_editions"] = editions
    doc["foreign_rights_sold"] = _fetch_foreign_rights_sold(cur, tenant_id, work_id)

    contributors = _fetch_contributors(cur, tenant_id, work_id)
    doc["_contributors"] = contributors

    author_party_id: Optional[str] = None
    illustrator_party_id: Optional[str] = None
    author_name = ""
    illustrator_name = ""

    for c in contributors:
        role = c.get("role") or ""
        scope = c.get("scope") or ""

        if (_is_author_role(role) or scope == "author") and not author_party_id:
            author_party_id = str(c["party_id"])
            author_name = c.get("display_name") or ""

        if (_is_illustrator_role(role) or scope == "illustrator") and not illustrator_party_id:
            illustrator_party_id = str(c["party_id"])
            illustrator_name = c.get("display_name") or ""

    def _set_contributor_flat(scope: str, party_id: Optional[str], display_name: str) -> None:
        if not party_id:
            return

        party_summary = _fetch_party_summary(cur, tenant_id, party_id)
        profile = _fetch_contributor_marketing_profile(cur, tenant_id, party_id, scope)
        block = _fetch_party_extras_block(cur, tenant_id, party_id, work_id, scope)
        pref = block.get("preferences") or {}
        agents, agency_card = _fetch_agent_for_party(cur, tenant_id, party_id, work_id)
        address = _fetch_party_address(cur, tenant_id, party_id)
        contact_categories = _fetch_contributor_contact_categories(cur, tenant_id, party_id, scope)

        raw_prefixed = {
            "marketing_endorsers": contact_categories.get(f"{scope}_marketing_endorsers", []),
            "publicity_endorsers_blurbers": contact_categories.get(f"{scope}_publicity_endorsers_blurbers", []),
            "marketing_big_mouth_list": contact_categories.get(f"{scope}_marketing_big_mouth_list", []),
            "publicity_big_mouth_list": contact_categories.get(f"{scope}_publicity_big_mouth_list", []),
            "marketing_review_copy_wishlist": contact_categories.get(f"{scope}_marketing_review_copy_wishlist", []),
            "publicity_review_copy_wishlist": contact_categories.get(f"{scope}_publicity_review_copy_wishlist", []),
            "marketing_local_media": contact_categories.get(f"{scope}_marketing_local_media", []),
            "publicity_local_media": contact_categories.get(f"{scope}_publicity_local_media", []),
            "marketing_alumni_org_publications": contact_categories.get(f"{scope}_marketing_alumni_org_publications", []),
            "publicity_alumni_org_publications": contact_categories.get(f"{scope}_publicity_alumni_org_publications", []),
            "marketing_targeted_sites": contact_categories.get(f"{scope}_marketing_targeted_sites", []),
            "publicity_target_sites": contact_categories.get(f"{scope}_publicity_target_sites", []),
            "marketing_bloggers": contact_categories.get(f"{scope}_marketing_bloggers", []),
            "publicity_bloggers_genre": contact_categories.get(f"{scope}_publicity_bloggers_genre", []),
            "sales_local_bookstores": contact_categories.get(f"{scope}_sales_local_bookstores", []),
            "sales_nontrade_outlets": contact_categories.get(f"{scope}_sales_nontrade_outlets", []),
            "sales_museums_parks": contact_categories.get(f"{scope}_sales_museums_parks", []),
        }

        social_obj: Dict[str, str] = {}
        for s in (block.get("socials") or []):
            platform = _safe_str(s.get("platform")).lower()
            url = _safe_str(s.get("url"))
            if platform and url:
                social_obj[platform] = url

        line1 = _safe_str(address.get("street"))
        line2 = " ".join([
            _safe_str(address.get("city")),
            _safe_str(address.get("state")),
            _safe_str(address.get("zip")),
            _safe_str(address.get("country")),
        ]).strip()
        address_lines = [x for x in (line1, line2) if x]

        full_phone = _format_phone(
            party_summary.get("phone_country_code"),
            party_summary.get("phone_number"),
        )

        short_bio = _safe_str(profile.get("book_bio") or party_summary.get("short_bio"))
        long_bio = _safe_str(profile.get("website_bio") or party_summary.get("long_bio"))
        clean_name = display_name or party_summary.get("display_name") or ""

        doc[scope] = {
            "name": clean_name,
            "email": party_summary.get("email") or "",
            "website": party_summary.get("website") or "",
            "phone": full_phone,
            "phone_country_code": party_summary.get("phone_country_code") or "",
            "phone_number": party_summary.get("phone_number") or "",
            "birthDate": party_summary.get("birth_date"),
            "birthCity": party_summary.get("birth_city") or "",
            "birthCountry": party_summary.get("birth_country") or "",
            "citizenship": party_summary.get("citizenship") or "",
            "bio": short_bio,
            "long_bio": long_bio,
            "book_bio": short_bio,
            "website_bio": long_bio,
            "social": social_obj,
            "socials": block.get("socials") or [],
            "address": address,
            "addressLines": address_lines,
            "photo": "",
            "agent": agents,
            "agency": agency_card,
            "books_published": block.get("published_books") or [],
            "published_books": block.get("published_books") or [],
            "media_appearances": block.get("media_appearances") or [],
            "other_publications": block.get("other_publications") or [],
            "media_contacts": block.get("media_contacts") or [],
            "previous_publicity": block.get("previous_publicity") or [],
            "niche_publicity_targets": block.get("niche_publicity_targets") or [],
            "contact_pref_rank1": _safe_str(pref.get("contact_pref_rank1")),
            "contact_pref_rank2": _safe_str(pref.get("contact_pref_rank2")),
            "media_best_times": _safe_str(pref.get("media_best_times")),
            "media_press_share": bool(pref.get("media_press_share") or False),
            "us_travel_plans": _safe_str(pref.get("us_travel_plans")),
            "travel_dates": _safe_str(pref.get("travel_dates")),
            "present_position": _safe_str(profile.get("present_position")),
            "former_positions": _safe_str(profile.get("former_positions")),
            "degrees_honors": _safe_str(profile.get("degrees_honors")),
            "professional_honors": _safe_str(profile.get("professional_honors")),
            "photo_credit": _safe_str(profile.get("photo_credit")),
            "additional_notes": _safe_str(profile.get("additional_notes")),
            "contact_categories": contact_categories,
            "sales_local_bookstores": [],
            "sales_nontrade_outlets": [],
            "sales_museums_parks": [],
        }

        doc[f"{scope}_name"] = clean_name
        doc[f"{scope}_email"] = party_summary.get("email") or ""
        doc[f"{scope}_website"] = party_summary.get("website") or ""
        doc[f"{scope}_phone_country_code"] = party_summary.get("phone_country_code") or ""
        doc[f"{scope}_phone_number"] = party_summary.get("phone_number") or ""
        doc[f"{scope}_phone"] = full_phone
        doc[f"{scope}_address"] = address
        doc[f"{scope}_birth_city"] = party_summary.get("birth_city") or ""
        doc[f"{scope}_birth_country"] = party_summary.get("birth_country") or ""
        doc[f"{scope}_birth_date"] = party_summary.get("birth_date")
        doc[f"{scope}_citizenship"] = party_summary.get("citizenship") or ""

        doc[f"{scope}_bio"] = short_bio
        doc[f"{scope}_long_bio"] = long_bio
        doc[f"{scope}_book_bio"] = short_bio
        doc[f"{scope}_website_bio"] = long_bio
        doc[f"{scope}_photo_credit"] = _safe_str(profile.get("photo_credit"))
        doc[f"{scope}_present_position"] = _safe_str(profile.get("present_position"))
        doc[f"{scope}_former_positions"] = _safe_str(profile.get("former_positions"))
        doc[f"{scope}_degrees_honors"] = _safe_str(profile.get("degrees_honors"))
        doc[f"{scope}_professional_honors"] = _safe_str(profile.get("professional_honors"))
        doc[f"{scope}_additional_notes"] = _safe_str(profile.get("additional_notes"))

        doc[f"{scope}_socials"] = block.get("socials") or []
        doc[f"{scope}_books_published"] = block.get("published_books") or []
        doc[f"{scope}_published_books"] = block.get("published_books") or []
        doc[f"{scope}_media_appearances"] = block.get("media_appearances") or []
        doc[f"{scope}_other_publications"] = block.get("other_publications") or []
        doc[f"{scope}_media_contacts"] = block.get("media_contacts") or []
        doc[f"{scope}_previous_publicity"] = block.get("previous_publicity") or []
        doc[f"{scope}_marketing_previous_book_publicity"] = block.get("previous_publicity") or []
        doc[f"{scope}_publicity_previous_book_publicity"] = block.get("previous_publicity") or []
        doc[f"{scope}_niche_publicity_targets"] = block.get("niche_publicity_targets") or []

        doc[f"{scope}_contact_pref_rank1"] = _safe_str(pref.get("contact_pref_rank1"))
        doc[f"{scope}_contact_pref_rank2"] = _safe_str(pref.get("contact_pref_rank2"))
        doc[f"{scope}_media_best_times"] = _safe_str(pref.get("media_best_times"))
        doc[f"{scope}_media_press_share"] = bool(pref.get("media_press_share") or False)
        doc[f"{scope}_us_travel_plans"] = _safe_str(pref.get("us_travel_plans"))
        doc[f"{scope}_travel_dates"] = _safe_str(pref.get("travel_dates"))

        doc[f"{scope}_agent"] = agents
        doc[f"{scope}_agency"] = agency_card
        doc[f"{scope}_agent_list"] = agents
        doc[f"{scope}_agency_name"] = _safe_str(agency_card.get("agency"))
        doc[f"{scope}_agent_name"] = _safe_str(agency_card.get("agent") or agency_card.get("contact"))
        doc[f"{scope}_agent_email"] = _safe_str(agency_card.get("email"))
        doc[f"{scope}_agent_phone"] = _safe_str(agency_card.get("phone"))
        doc[f"{scope}_agency_website"] = _safe_str(agency_card.get("website"))
        doc[f"{scope}_has_agency"] = bool(agency_card)

        if scope == "author":
            doc["author_agency"] = agency_card
        else:
            doc["illustrator_agency"] = agency_card

        doc[f"{scope}_contact_categories"] = contact_categories
        for category_name, items in contact_categories.items():
            doc[f"{scope}_{category_name}"] = items

        _apply_contact_category_aliases(doc, scope, contact_categories)

        for suffix, rows in raw_prefixed.items():
            if rows:
                doc[f"{scope}_{suffix}"] = rows

        doc[scope]["sales_local_bookstores"] = doc.get(f"{scope}_sales_local_bookstores", []) or []
        doc[scope]["sales_nontrade_outlets"] = doc.get(f"{scope}_sales_nontrade_outlets", []) or []
        doc[scope]["sales_museums_parks"] = doc.get(f"{scope}_sales_museums_parks", []) or []

    _set_contributor_flat("author", author_party_id, author_name)
    _set_contributor_flat("illustrator", illustrator_party_id, illustrator_name)

    for scope in ("author", "illustrator"):
        if scope not in doc:
            doc[scope] = {
                "name": "",
                "email": "",
                "website": "",
                "phone": "",
                "phone_country_code": "",
                "phone_number": "",
                "address": {},
                "addressLines": [],
                "birthDate": "",
                "birthCity": "",
                "birthCountry": "",
                "citizenship": "",
                "bio": "",
                "long_bio": "",
                "book_bio": "",
                "website_bio": "",
                "social": {},
                "socials": [],
                "agent": [],
                "agency": {},
                "books_published": [],
                "published_books": [],
                "media_appearances": [],
                "other_publications": [],
                "media_contacts": [],
                "previous_publicity": [],
                "niche_publicity_targets": [],
                "contact_categories": {},
                "sales_local_bookstores": [],
                "sales_nontrade_outlets": [],
                "sales_museums_parks": [],
            }

        for key in (
            "name", "email", "website", "phone", "phone_country_code", "phone_number", "address",
            "birth_city", "birth_country", "birth_date", "citizenship",
            "bio", "long_bio", "book_bio", "website_bio",
            "photo_credit", "present_position", "former_positions",
            "degrees_honors", "professional_honors", "additional_notes",
            "socials", "books_published", "published_books", "media_appearances",
            "other_publications", "media_contacts", "previous_publicity",
            "niche_publicity_targets", "contact_pref_rank1", "contact_pref_rank2",
            "media_best_times", "media_press_share", "us_travel_plans",
            "travel_dates", "agent", "agency", "agent_list", "agency_name",
            "agent_name", "agent_email", "agent_phone", "agency_website", "has_agency",
            "marketing_previous_book_publicity", "publicity_previous_book_publicity",
            "marketing_endorsers", "publicity_endorsers_blurbers",
            "marketing_big_mouth_list", "publicity_big_mouth_list",
            "marketing_review_copy_wishlist", "publicity_review_copy_wishlist",
            "marketing_local_media", "publicity_local_media",
            "marketing_alumni_org_publications", "publicity_alumni_org_publications",
            "marketing_targeted_sites", "publicity_target_sites",
            "marketing_bloggers", "publicity_bloggers_genre",
            "sales_local_bookstores", "sales_nontrade_outlets", "sales_museums_parks",
            "contact_categories",
        ):
            full_key = f"{scope}_{key}"
            if full_key not in doc:
                if key in (
                    "socials", "books_published", "published_books", "media_appearances",
                    "other_publications", "media_contacts", "previous_publicity",
                    "niche_publicity_targets", "agent", "agent_list",
                    "marketing_previous_book_publicity", "publicity_previous_book_publicity",
                    "marketing_endorsers", "publicity_endorsers_blurbers",
                    "marketing_big_mouth_list", "publicity_big_mouth_list",
                    "marketing_review_copy_wishlist", "publicity_review_copy_wishlist",
                    "marketing_local_media", "publicity_local_media",
                    "marketing_alumni_org_publications", "publicity_alumni_org_publications",
                    "marketing_targeted_sites", "publicity_target_sites",
                    "marketing_bloggers", "publicity_bloggers_genre",
                    "sales_local_bookstores", "sales_nontrade_outlets", "sales_museums_parks",
                ):
                    doc[full_key] = []
                elif key in ("address", "agency", "contact_categories"):
                    doc[full_key] = {}
                elif key in ("media_press_share", "has_agency"):
                    doc[full_key] = False
                else:
                    doc[full_key] = ""

    doc["royalties"] = _fetch_royalties_graph(cur, tenant_id, work_id)
    isbns = [e["isbn13"] for e in editions if e.get("isbn13")]
    doc["_onix_raw_products_by_isbn13"] = _fetch_onix_raw_by_isbns(cur, tenant_id, isbns, limit_each=1)

    return doc


@router.get("/works")
def list_works(
    tenant_slug: str = Query(...),
    q: str = Query(""),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    exclude_draft_contracts: bool = Query(True, description="Ignored; kept for API compat"),
):
    with db_conn() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            tenant_id = _get_tenant_id_from_slug(cur, tenant_slug)

            q_like = f"%{q.strip()}%" if q else None
            if q_like:
                cur.execute(
                    """
                    SELECT w.*
                    FROM works w
                    WHERE w.tenant_id = %s
                      AND (
                        w.title ILIKE %s
                        OR EXISTS (
                          SELECT 1
                          FROM work_contributors wc
                          JOIN parties p ON p.id = wc.party_id
                          WHERE wc.work_id = w.id
                            AND p.display_name ILIKE %s
                        )
                        OR EXISTS (
                          SELECT 1
                          FROM editions e
                          WHERE e.tenant_id = w.tenant_id
                            AND e.work_id = w.id
                            AND e.isbn13 ILIKE %s
                        )
                      )
                    ORDER BY w.updated_at DESC NULLS LAST, w.created_at DESC
                    LIMIT %s OFFSET %s
                    """,
                    (tenant_id, q_like, q_like, q_like, limit, offset),
                )
            else:
                cur.execute(
                    """
                    SELECT w.*
                    FROM works w
                    WHERE w.tenant_id = %s
                    ORDER BY w.updated_at DESC NULLS LAST, w.created_at DESC
                    LIMIT %s OFFSET %s
                    """,
                    (tenant_id, limit, offset),
                )

            rows = cur.fetchall() or []
            work_ids = [str(r["id"]) for r in rows if r.get("id") is not None]
            author_by_work: Dict[str, str] = {}

            if work_ids:
                cur.execute(
                    """
                    SELECT
                        wc.work_id,
                        p.display_name AS author,
                        wc.sequence_number,
                        wc.id
                    FROM work_contributors wc
                    JOIN parties p ON p.id = wc.party_id
                    WHERE wc.work_id::text = ANY(%s)
                    ORDER BY wc.work_id, wc.sequence_number, wc.id
                    """,
                    (work_ids,),
                )
                all_rows = cur.fetchall() or []

                grouped: Dict[str, List[Dict[str, Any]]] = {}
                for r in all_rows:
                    grouped.setdefault(str(r["work_id"]), []).append(r)

                for wid, grp in grouped.items():
                    preferred = None
                    for r in grp:
                        if _is_author_role(_safe_str(r.get("contributor_role"))):
                            preferred = r
                            break
                    if preferred is None and grp:
                        preferred = grp[0]
                    if preferred and preferred.get("author"):
                        author_by_work[wid] = _clean_display_name(preferred.get("author"))

            items = []
            for r in rows:
                it = _work_row_to_list_item(r)
                wid = str(r.get("id", ""))
                if wid in author_by_work:
                    it["author"] = author_by_work[wid]
                items.append(it)

            if q_like:
                cur.execute(
                    """
                    SELECT COUNT(*) AS n
                    FROM works w
                    WHERE w.tenant_id = %s
                      AND (
                        w.title ILIKE %s
                        OR EXISTS (
                          SELECT 1
                          FROM work_contributors wc
                          JOIN parties p ON p.id = wc.party_id
                          WHERE wc.work_id = w.id
                            AND p.display_name ILIKE %s
                        )
                        OR EXISTS (
                          SELECT 1
                          FROM editions e
                          WHERE e.tenant_id = w.tenant_id
                            AND e.work_id = w.id
                            AND e.isbn13 ILIKE %s
                        )
                      )
                    """,
                    (tenant_id, q_like, q_like, q_like),
                )
            else:
                cur.execute(
                    "SELECT COUNT(*) AS n FROM works w WHERE w.tenant_id = %s",
                    (tenant_id,),
                )

            total = int((cur.fetchone() or {}).get("n") or 0)

            return {
                "ok": True,
                "tenant_slug": tenant_slug,
                "total": total,
                "limit": limit,
                "offset": offset,
                "items": items,
            }
        
@router.post("/works/from-dealmemo")
def create_work_from_dealmemo(
    uid: str = Query(...),
    tenant_slug: str = Query(...),
):
    with db_conn() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            tenant_id = _get_tenant_id_from_slug(cur, tenant_slug)

            cur.execute(
                """
                SELECT *
                FROM deal_memo_drafts
                WHERE tenant_id = %s
                  AND uid = %s
                LIMIT 1
                """,
                (tenant_id, uid),
            )
            dm = cur.fetchone()
            if not dm:
                raise HTTPException(status_code=404, detail="Deal memo not found")

            contributor_role = _safe_str(
                dm.get("contributor_role") or dm.get("contributorRole")
            ).lower() or "author"
            is_illustrator = contributor_role == "illustrator"

            title = _safe_str(dm.get("title"))
            if not title:
                raise HTTPException(status_code=400, detail="Deal memo is missing title")

            author_name = _safe_str(dm.get("author"))
            illustrator_name = _safe_str(dm.get("illustrator_name"))

            author_block = {
                "name": author_name,
                "email": _safe_str(dm.get("author_email")),
                "website": _safe_str(dm.get("author_website")),
                "phone_country_code": _safe_str(dm.get("author_phone_country_code")),
                "phone_number": _safe_str(dm.get("author_phone_number")),
                "address": {
                    "street": _safe_str(dm.get("author_street")),
                    "city": _safe_str(dm.get("author_city")),
                    "state": _safe_str(dm.get("author_state")),
                    "zip": _safe_str(dm.get("author_zip")),
                    "country": _safe_str(dm.get("author_country")),
                },
            }

            illustrator_block = {
                "name": illustrator_name,
                "email": _safe_str(dm.get("illustrator_email")),
                "website": _safe_str(dm.get("illustrator_website")),
                "phone_country_code": _safe_str(dm.get("illustrator_phone_country_code")),
                "phone_number": _safe_str(dm.get("illustrator_phone_number")),
                "address": {
                    "street": _safe_str(dm.get("illustrator_street")),
                    "city": _safe_str(dm.get("illustrator_city")),
                    "state": _safe_str(dm.get("illustrator_state")),
                    "zip": _safe_str(dm.get("illustrator_zip")),
                    "country": _safe_str(dm.get("illustrator_country")),
                },
            }

            author_agency = {
                "agency": _safe_str(dm.get("author_agency_name") or dm.get("agency_name")),
                "agent": _safe_str(dm.get("author_agent_name") or dm.get("agent_name")),
                "contact": _safe_str(dm.get("author_agent_name") or dm.get("agent_name")),
                "email": _safe_str(dm.get("author_agent_email") or dm.get("agent_email")),
                "website": _safe_str(dm.get("author_agency_website") or dm.get("agency_website")),
                "phone": _format_phone(
                    _safe_str(dm.get("author_agent_phone_country_code") or dm.get("authors_agent_phone_country_code")),
                    _safe_str(dm.get("author_agent_phone_number") or dm.get("authors_agent_phone_number")),
                ),
                "address": {
                    "street": _safe_str(dm.get("author_agency_street") or dm.get("agency_street")),
                    "city": _safe_str(dm.get("author_agency_city") or dm.get("agency_city")),
                    "state": _safe_str(dm.get("author_agency_state") or dm.get("agency_state")),
                    "zip": _safe_str(dm.get("author_agency_zip") or dm.get("agency_zip")),
                    "country": _safe_str(dm.get("author_agency_country") or dm.get("agency_country")),
                },
            }

            illustrator_agency = {
                "agency": _safe_str(dm.get("illustrator_agency_name")),
                "agent": _safe_str(dm.get("illustrator_agent_name")),
                "contact": _safe_str(dm.get("illustrator_agent_name")),
                "email": _safe_str(dm.get("illustrator_agent_email")),
                "website": _safe_str(dm.get("illustrator_agency_website")),
                "phone": _format_phone(
                    _safe_str(dm.get("illustrator_agent_phone_country_code")),
                    _safe_str(dm.get("illustrator_agent_phone_number")),
                ),
                "address": {
                    "street": _safe_str(dm.get("illustrator_agency_street")),
                    "city": _safe_str(dm.get("illustrator_agency_city")),
                    "state": _safe_str(dm.get("illustrator_agency_state")),
                    "zip": _safe_str(dm.get("illustrator_agency_zip")),
                    "country": _safe_str(dm.get("illustrator_agency_country")),
                },
            }

            def _has_agency_data(block: Dict[str, Any]) -> bool:
                addr = block.get("address") or {}
                return any([
                    _safe_str(block.get("agency")),
                    _safe_str(block.get("agent")),
                    _safe_str(block.get("email")),
                    _safe_str(block.get("website")),
                    _safe_str(block.get("phone")),
                    _safe_str(addr.get("street")),
                    _safe_str(addr.get("city")),
                    _safe_str(addr.get("state")),
                    _safe_str(addr.get("zip")),
                    _safe_str(addr.get("country")),
                ])

            author_agency = author_agency if _has_agency_data(author_agency) else {}
            illustrator_agency = illustrator_agency if _has_agency_data(illustrator_agency) else {}

            royalties = _hydrate_dealmemo_royalties(cur, str(dm["id"]))
            if not isinstance(royalties, dict):
                royalties = _empty_royalties()

            payload: Dict[str, Any] = {
                "uid": uid,
                "title": title,
                "subtitle": "",
                "series": _safe_str(dm.get("series_title")) if dm.get("series") else "",
                "volume_number": int(dm.get("number_of_books") or 0) if dm.get("series") else 0,
                "description": _safe_str(dm.get("short_description") or dm.get("shortDescription")),
                "publisher_or_imprint": "",
                "publishing_year": None,
                "language": "",
                "rights": _safe_str(dm.get("territories_rights") or dm.get("territoriesRights")),
                "editor_name": "",
                "art_director_name": "",
                "ages": "",
                "us_grade": "",
                "loc_number": "",

                "author": author_block,
                "author_name": author_name,
                "author_email": _safe_str(dm.get("author_email")),
                "author_website": _safe_str(dm.get("author_website")),
                "author_phone_country_code": _safe_str(dm.get("author_phone_country_code")),
                "author_phone_number": _safe_str(dm.get("author_phone_number")),
                "author_address": author_block["address"],
                "author_birth_date": _jsonable(dm.get("author_birth_date")),
                "author_birth_city": _safe_str(dm.get("author_birth_city")),
                "author_birth_country": _safe_str(dm.get("author_birth_country")),
                "author_citizenship": _safe_str(dm.get("author_citizenship")),

                "illustrator": illustrator_block,
                "illustrator_name": illustrator_name,
                "illustrator_email": _safe_str(dm.get("illustrator_email")),
                "illustrator_website": _safe_str(dm.get("illustrator_website")),
                "illustrator_phone_country_code": _safe_str(dm.get("illustrator_phone_country_code")),
                "illustrator_phone_number": _safe_str(dm.get("illustrator_phone_number")),
                "illustrator_address": illustrator_block["address"],
                "illustrator_birth_date": _jsonable(dm.get("illustrator_birth_date")),
                "illustrator_birth_city": _safe_str(dm.get("illustrator_birth_city")),
                "illustrator_birth_country": _safe_str(dm.get("illustrator_birth_country")),
                "illustrator_citizenship": _safe_str(dm.get("illustrator_citizenship")),

                "author_agency": author_agency,
                "author_has_agency": bool(author_agency),
                "author_agency_name": _safe_str(author_agency.get("agency")),
                "author_agent_name": _safe_str(author_agency.get("agent")),
                "author_agent_email": _safe_str(author_agency.get("email")),
                "author_agent_phone": _safe_str(author_agency.get("phone")),
                "author_agency_website": _safe_str(author_agency.get("website")),

                "illustrator_agency": illustrator_agency,
                "illustrator_has_agency": bool(illustrator_agency),
                "illustrator_agency_name": _safe_str(illustrator_agency.get("agency")),
                "illustrator_agent_name": _safe_str(illustrator_agency.get("agent")),
                "illustrator_agent_email": _safe_str(illustrator_agency.get("email")),
                "illustrator_agent_phone": _safe_str(illustrator_agency.get("phone")),
                "illustrator_agency_website": _safe_str(illustrator_agency.get("website")),

                "formats": [],
                "foreign_rights_sold": [],
                "royalties": royalties,
            }

            work_id = _upsert_work_from_payload(conn, cur, tenant_id, payload)

            saved_contributor_party_id = _safe_str(dm.get("contributor_party_id"))
            saved_agent_party_id = _safe_str(dm.get("agent_party_id"))

            represented_party_id = saved_contributor_party_id
            if not represented_party_id:
                target_role = "ILLUSTRATOR" if is_illustrator else "AUTHOR"
                cur.execute(
                    """
                    SELECT wc.party_id
                    FROM work_contributors wc
                    WHERE wc.work_id = %s
                      AND wc.contributor_role = %s
                    ORDER BY wc.sequence_number ASC
                    LIMIT 1
                    """,
                    (work_id, target_role),
                )
                rep_row = cur.fetchone()
                if rep_row:
                    represented_party_id = str(rep_row["party_id"])

            if represented_party_id and saved_agent_party_id:
                cur.execute(
                    """
                    INSERT INTO party_representations (
                        tenant_id,
                        represented_party_id,
                        agent_party_id,
                        work_id,
                        is_primary,
                        role_label,
                        notes
                    )
                    VALUES (%s, %s, %s, %s, %s, 'agent', '')
                    ON CONFLICT (represented_party_id, agent_party_id, work_id)
                    DO UPDATE SET
                        is_primary = EXCLUDED.is_primary,
                        updated_at = now()
                    """,
                    (
                        tenant_id,
                        represented_party_id,
                        saved_agent_party_id,
                        work_id,
                        True,
                    ),
                )

            cur.execute(
                """
                UPDATE deal_memo_drafts
                SET work_id = %s,
                    contributor_party_id = %s,
                    updated_at = now()
                WHERE tenant_id = %s
                  AND uid = %s
                """,
                (work_id, represented_party_id or None, tenant_id, uid),
            )

            conn.commit()

            full_payload = _build_full_work_payload(cur, tenant_id, work_id)
            return {
                "ok": True,
                "tenant_slug": tenant_slug,
                "work_id": work_id,
                "uid": _safe_str(full_payload.get("uid")),
                "work": full_payload,
            }

@router.get("/works/{work_id}")
def get_work_full(
    work_id: str,
    tenant_slug: str = Query(...),
):
    with db_conn() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            tenant_id = _get_tenant_id_from_slug(cur, tenant_slug)
            resolved_id = _resolve_work_id_param(cur, tenant_id, work_id)
            if not resolved_id:
                raise HTTPException(status_code=404, detail="Work not found")
            payload = _build_full_work_payload(cur, tenant_id, resolved_id)
            return {"ok": True, "tenant_slug": tenant_slug, "work_id": resolved_id, "work": payload}


@router.get("/resolve")
def resolve_by_isbn(
    tenant_slug: str = Query(...),
    isbn13: str = Query(...),
):
    with db_conn() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            tenant_id = _get_tenant_id_from_slug(cur, tenant_slug)
            cur.execute(
                """
                SELECT w.id AS work_id
                FROM editions e
                JOIN works w ON w.id = e.work_id
                WHERE e.tenant_id = %s
                  AND e.isbn13 = %s
                LIMIT 1
                """,
                (tenant_id, isbn13.strip()),
            )
            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="ISBN not found")
            wid = str(row["work_id"])
            payload = _build_full_work_payload(cur, tenant_id, wid)
            return {"ok": True, "tenant_slug": tenant_slug, "work_id": wid, "work": payload}


def _normalize_isbn13(val: Any) -> str:
    if val is None:
        return ""
    s = str(val).strip().replace("-", "").replace(" ", "").upper()
    return s[:17] if s else ""


def _resolve_work_id(cur, tenant_id: str, body: Dict[str, Any]) -> Optional[str]:
    body_id = body.get("id")
    if body_id is not None and str(body_id).strip():
        cur.execute(
            "SELECT id FROM works WHERE tenant_id = %s AND id = %s LIMIT 1",
            (tenant_id, str(body_id).strip()),
        )
        row = cur.fetchone()
        if row:
            return str(row["id"])

    uid_raw = body.get("uid")
    if uid_raw is not None and str(uid_raw).strip():
        try:
            u = uuid.UUID(str(uid_raw).strip())
        except (ValueError, TypeError):
            pass
        else:
            cur.execute(
                "SELECT id FROM works WHERE tenant_id = %s AND uid = %s LIMIT 1",
                (tenant_id, u),
            )
            row = cur.fetchone()
            if row:
                return str(row["id"])
    return None


def _resolve_work_id_by_title_author(cur, tenant_id: str, title: str, author: str) -> Optional[str]:
    title = _safe_str(title)
    author = _safe_str(author)
    if not title and not author:
        return None
    cur.execute(
        """
        SELECT w.id
        FROM works w
        WHERE w.tenant_id = %s
          AND trim(lower(w.title)) = trim(lower(%s))
          AND EXISTS (
              SELECT 1
              FROM work_contributors wc
              JOIN parties p ON p.id = wc.party_id AND p.tenant_id = w.tenant_id
              WHERE wc.work_id = w.id
                AND trim(lower(coalesce(p.display_name, ''))) = trim(lower(%s))
          )
        LIMIT 1
        """,
        (tenant_id, title, author),
    )
    row = cur.fetchone()
    return str(row["id"]) if row else None


def _get_or_create_party(cur, tenant_id: str, display_name: str, email: str = "", party_type: str = "person") -> str:
    display_name = _safe_name(display_name)
    email = _safe_str(email).lower()

    if email:
        cur.execute(
            """
            SELECT id
            FROM parties
            WHERE tenant_id = %s
              AND lower(coalesce(email, '')) = %s
            LIMIT 1
            """,
            (tenant_id, email),
        )
        row = cur.fetchone()
        if row:
            if display_name:
                cur.execute(
                    """
                    UPDATE parties
                    SET
                        display_name = CASE
                            WHEN coalesce(trim(display_name), '') = '' THEN %s
                            ELSE display_name
                        END,
                        updated_at = now()
                    WHERE tenant_id = %s
                      AND id = %s
                    """,
                    (display_name, tenant_id, row["id"]),
                )
            return str(row["id"])

    if display_name:
        cur.execute(
            """
            SELECT id
            FROM parties
            WHERE tenant_id = %s
              AND display_name = %s
            LIMIT 1
            """,
            (tenant_id, display_name),
        )
        row = cur.fetchone()
        if row:
            return str(row["id"])

    party_id = uuid.uuid4()
    cur.execute(
        """
        INSERT INTO parties (
            id, tenant_id, party_type, display_name,
            names_before_key, key_names, person_name_inverted, corporate_name, email
        )
        VALUES (%s, %s, %s, %s, '', '', '', '', %s)
        """,
        (party_id, tenant_id, party_type, display_name or email or "Unknown", email),
    )
    return str(party_id)


def _insert_party_address(cur, tenant_id: str, party_id: str, address_obj: Dict[str, Any]) -> None:
    if not any(_safe_str(address_obj.get(k)) for k in ("street", "city", "state", "zip", "country")):
        return

    try:
        cur.execute(
            """
            INSERT INTO party_addresses (tenant_id, party_id, street, city, state, postal_code, country, label)
            VALUES (%s, %s, %s, %s, %s, %s, %s, 'primary')
            """,
            (
                tenant_id,
                party_id,
                _safe_str(address_obj.get("street")),
                _safe_str(address_obj.get("city")),
                _safe_str(address_obj.get("state")),
                _safe_str(address_obj.get("zip")),
                _safe_str(address_obj.get("country")),
            ),
        )
    except Exception:
        cur.execute(
            """
            INSERT INTO party_addresses (tenant_id, party_id, street, city, state, zip, country, label)
            VALUES (%s, %s, %s, %s, %s, %s, %s, 'primary')
            """,
            (
                tenant_id,
                party_id,
                _safe_str(address_obj.get("street")),
                _safe_str(address_obj.get("city")),
                _safe_str(address_obj.get("state")),
                _safe_str(address_obj.get("zip")),
                _safe_str(address_obj.get("country")),
            ),
        )


def _upsert_party_core(cur, tenant_id: str, party_id: str, payload: Dict[str, Any], scope: str) -> None:
    info = _contributor_input(payload, scope)
    email = _safe_str(info.get("email"))
    website = _safe_str(info.get("website"))
    phone_country_code = _safe_str(info.get("phone_country_code"))
    phone_number = _safe_str(info.get("phone_number"))
    birth_city = _safe_str(payload.get(f"{scope}_birth_city"))
    birth_country = _safe_str(payload.get(f"{scope}_birth_country"))
    birth_date = _parse_date_or_none(payload.get(f"{scope}_birth_date"))
    citizenship = _safe_str(payload.get(f"{scope}_citizenship"))

    short_bio = _safe_str(
        payload.get(f"{scope}_book_bio")
        or payload.get(f"{scope}_bio")
    )
    long_bio = _safe_str(
        payload.get(f"{scope}_website_bio")
        or payload.get(f"{scope}_long_bio")
    )

    contributor_name = _safe_name(info.get("name"))

    cur.execute(
        """
        UPDATE parties
        SET
            display_name = CASE
                WHEN %s <> '' THEN %s
                ELSE display_name
            END,
            email = %s,
            website = %s,
            phone_country_code = %s,
            phone_number = %s,
            birth_city = %s,
            birth_country = %s,
            birth_date = %s,
            citizenship = %s,
            short_bio = %s,
            long_bio = %s,
            updated_at = now()
        WHERE tenant_id = %s
          AND id = %s
        """,
        (
            contributor_name, contributor_name,
            email, website, phone_country_code, phone_number,
            birth_city, birth_country, birth_date, citizenship,
            short_bio, long_bio, tenant_id, party_id,
        ),
    )

    address_obj = info.get("address") if isinstance(info.get("address"), dict) else {}
    if isinstance(address_obj, dict):
        try:
            cur.execute(
                "DELETE FROM party_addresses WHERE tenant_id = %s AND party_id = %s",
                (tenant_id, party_id),
            )
            _insert_party_address(cur, tenant_id, party_id, address_obj)
        except Exception:
            pass


def _upsert_contributor_profile(cur, tenant_id: str, party_id: str, scope: str, payload: Dict[str, Any]) -> None:
    cur.execute(
        """
        INSERT INTO contributor_marketing_profiles (
            tenant_id, party_id, scope,
            website_bio, book_bio,
            contact_pref_rank1, contact_pref_rank2,
            media_best_times, media_press_share,
            us_travel_plans, travel_dates,
            additional_notes,
            photo_credit,
            present_position,
            former_positions,
            degrees_honors,
            professional_honors
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (tenant_id, party_id, scope)
        DO UPDATE SET
            website_bio = EXCLUDED.website_bio,
            book_bio = EXCLUDED.book_bio,
            contact_pref_rank1 = EXCLUDED.contact_pref_rank1,
            contact_pref_rank2 = EXCLUDED.contact_pref_rank2,
            media_best_times = EXCLUDED.media_best_times,
            media_press_share = EXCLUDED.media_press_share,
            us_travel_plans = EXCLUDED.us_travel_plans,
            travel_dates = EXCLUDED.travel_dates,
            additional_notes = EXCLUDED.additional_notes,
            photo_credit = EXCLUDED.photo_credit,
            present_position = EXCLUDED.present_position,
            former_positions = EXCLUDED.former_positions,
            degrees_honors = EXCLUDED.degrees_honors,
            professional_honors = EXCLUDED.professional_honors,
            updated_at = now()
        """,
        (
            tenant_id,
            party_id,
            scope,
            _safe_str(payload.get(f"{scope}_website_bio") or payload.get(f"{scope}_long_bio")),
            _safe_str(payload.get(f"{scope}_book_bio") or payload.get(f"{scope}_bio")),
            _safe_str(payload.get(f"{scope}_contact_pref_rank1")),
            _safe_str(payload.get(f"{scope}_contact_pref_rank2")),
            _safe_str(payload.get(f"{scope}_media_best_times")),
            bool(payload.get(f"{scope}_media_press_share") or False),
            _safe_str(payload.get(f"{scope}_us_travel_plans")),
            _safe_str(payload.get(f"{scope}_travel_dates")),
            _safe_str(payload.get(f"{scope}_additional_notes") or payload.get(f"{scope}_marketing_additional_notes") or payload.get(f"{scope}_publicity_additional_notes")),
            _safe_str(payload.get(f"{scope}_photo_credit")),
            _safe_str(payload.get(f"{scope}_present_position")),
            _safe_str(payload.get(f"{scope}_former_positions")),
            _safe_str(payload.get(f"{scope}_degrees_honors")),
            _safe_str(payload.get(f"{scope}_professional_honors")),
        ),
    )


def _replace_party_socials(cur, tenant_id: str, party_id: str, payload: Dict[str, Any], scope: str) -> None:
    socials = payload.get(f"{scope}_socials") or []
    cur.execute("DELETE FROM party_socials WHERE tenant_id = %s AND party_id = %s", (tenant_id, party_id))
    for s in socials:
        if not isinstance(s, dict):
            continue
        platform = _safe_str(s.get("platform"))
        url = _safe_str(s.get("url"))
        if not platform and not url:
            continue
        cur.execute(
            """
            INSERT INTO party_socials (tenant_id, party_id, platform, url)
            VALUES (%s, %s, %s, %s)
            """,
            (tenant_id, party_id, platform, url),
        )


def _replace_contributor_published_books(cur, tenant_id: str, party_id: str, payload: Dict[str, Any], scope: str) -> None:
    rows = payload.get(f"{scope}_books_published") or payload.get(f"{scope}_published_books") or []
    cur.execute(
        "DELETE FROM contributor_published_books WHERE tenant_id = %s AND party_id = %s AND lower(scope) = %s",
        (tenant_id, party_id, scope.lower()),
    )
    for idx, b in enumerate(rows, start=1):
        if not isinstance(b, dict):
            continue
        cur.execute(
            """
            INSERT INTO contributor_published_books (
                tenant_id, party_id, scope, item_order,
                title, isbn, publisher, publication_year, approx_sold
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                tenant_id,
                party_id,
                scope,
                idx,
                _safe_str(b.get("title")),
                _safe_str(b.get("isbn")),
                _safe_str(b.get("publisher")),
                _safe_str(b.get("year") or b.get("publication_year")),
                _safe_str(b.get("approx_sold")),
            ),
        )


def _replace_contributor_media_appearances(cur, tenant_id: str, party_id: str, payload: Dict[str, Any], scope: str) -> None:
    rows = payload.get(f"{scope}_media_appearances") or []
    cur.execute(
        "DELETE FROM contributor_media_appearances WHERE tenant_id = %s AND party_id = %s AND lower(scope) = %s",
        (tenant_id, party_id, scope.lower()),
    )
    for idx, m in enumerate(rows, start=1):
        if not isinstance(m, dict):
            continue
        cur.execute(
            """
            INSERT INTO contributor_media_appearances (
                tenant_id, party_id, scope, item_order,
                title, venue, date_text, link, notes
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                tenant_id,
                party_id,
                scope,
                idx,
                _safe_str(m.get("title")),
                _safe_str(m.get("venue")),
                _safe_str(m.get("appearance_date") or m.get("date")),
                _safe_str(m.get("link")),
                _safe_str(m.get("notes")),
            ),
        )


def _replace_contributor_media_contacts(cur, tenant_id: str, party_id: str, payload: Dict[str, Any], scope: str) -> None:
    rows = payload.get(f"{scope}_media_contacts") or []
    cur.execute(
        "DELETE FROM contributor_media_contacts WHERE tenant_id = %s AND party_id = %s AND lower(scope) = %s",
        (tenant_id, party_id, scope.lower()),
    )
    for idx, m in enumerate(rows, start=1):
        if not isinstance(m, dict):
            continue
        cur.execute(
            """
            INSERT INTO contributor_media_contacts (
                tenant_id, party_id, scope, item_order,
                company, name, position, phone, email
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                tenant_id,
                party_id,
                scope,
                idx,
                _safe_str(m.get("company")),
                _safe_str(m.get("name")),
                _safe_str(m.get("position")),
                _safe_str(m.get("phone")),
                _safe_str(m.get("email")),
            ),
        )


def _replace_contributor_other_publications(cur, tenant_id: str, party_id: str, rows: List[Dict[str, Any]]) -> None:
    cur.execute(
        "DELETE FROM contributor_other_publications WHERE tenant_id = %s AND party_id = %s",
        (tenant_id, party_id),
    )
    for item in rows or []:
        if not isinstance(item, dict):
            continue
        try:
            cur.execute(
                """
                INSERT INTO contributor_other_publications (
                    tenant_id, party_id, title, publication_name,
                    publication_type, publication_date, notes
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    tenant_id,
                    party_id,
                    _safe_str(item.get("title")),
                    _safe_str(item.get("publication_name")),
                    _safe_str(item.get("publication_type")),
                    _parse_date_or_none(item.get("publication_date")),
                    _safe_str(item.get("notes")),
                ),
            )
        except Exception:
            pass


def _replace_contributor_previous_publicity(cur, tenant_id: str, party_id: str, scope: str, rows: List[Dict[str, Any]]) -> None:
    cur.execute(
        "DELETE FROM contributor_previous_publicity WHERE tenant_id = %s AND party_id = %s AND lower(scope) = %s",
        (tenant_id, party_id, scope.lower()),
    )
    for idx, item in enumerate(rows or [], start=1):
        if not isinstance(item, dict):
            continue
        try:
            cur.execute(
                """
                INSERT INTO contributor_previous_publicity (
                    tenant_id, party_id, scope, item_order,
                    outlet_or_title, contact, relationship_note, notes, source_category
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    tenant_id,
                    party_id,
                    scope,
                    idx,
                    _safe_str(item.get("outlet_or_title") or item.get("publicity_name")),
                    _safe_str(item.get("contact")),
                    _safe_str(item.get("relationship_note") or item.get("relationship")),
                    _safe_str(item.get("notes")),
                    _safe_str(item.get("source_category")),
                ),
            )
        except Exception:
            pass


def _replace_contributor_niche_targets(cur, tenant_id: str, party_id: str, rows: List[Dict[str, Any]]) -> None:
    cur.execute(
        "DELETE FROM contributor_niche_publicity_targets WHERE tenant_id = %s AND party_id = %s",
        (tenant_id, party_id),
    )
    for item in rows or []:
        if not isinstance(item, dict):
            continue
        try:
            cur.execute(
                """
                INSERT INTO contributor_niche_publicity_targets (tenant_id, party_id, target_name, notes)
                VALUES (%s, %s, %s, %s)
                """,
                (
                    tenant_id,
                    party_id,
                    _safe_str(item.get("target_name")),
                    _safe_str(item.get("notes")),
                ),
            )
        except Exception:
            pass


def _replace_editions(cur, tenant_id: str, work_id: str, payload: Dict[str, Any]) -> None:
    cur.execute("DELETE FROM editions WHERE tenant_id = %s AND work_id = %s", (tenant_id, work_id))
    for fmt in (payload.get("formats") or []):
        if not isinstance(fmt, dict):
            continue

        isbn13 = _normalize_isbn13(fmt.get("ISBN") or fmt.get("isbn") or fmt.get("isbn13"))
        product_form = _safe_str(fmt.get("format"))
        publication_date = _parse_date_or_none(fmt.get("pub_date"))
        number_of_pages = _to_int_or_none(fmt.get("pages"))
        height = _to_float_or_none(fmt.get("tall"))
        width = _to_float_or_none(fmt.get("wide"))
        thickness = _to_float_or_none(fmt.get("spine"))
        unit_weight = _to_float_or_none(fmt.get("weight"))

        # Insert basic edition row
        cur.execute(
            """
            INSERT INTO editions (
                tenant_id, work_id, isbn13, status, product_form, publication_date,
                number_of_pages, height, height_unit, width, width_unit,
                thickness, thickness_unit, unit_weight, unit_weight_unit
            )
            VALUES (%s, %s, %s, 'planned', %s, %s, %s, %s, 'in', %s, 'in', %s, 'in', %s, 'lb')
            RETURNING id
            """,
            (
                tenant_id,
                work_id,
                isbn13,
                product_form,
                publication_date,
                number_of_pages,
                height,
                width,
                thickness,
                unit_weight,
            ),
        )
        edition_row = cur.fetchone()
        edition_id = edition_row and edition_row.get("id")

        # Best-effort: attach prices via edition_supply_details / edition_prices when schema supports it
        price_us = _to_float_or_none(fmt.get("price_us"))
        price_can = _to_float_or_none(fmt.get("price_can"))
        if edition_id and (price_us is not None or price_can is not None):
            try:
                cur.execute(
                    """
                    INSERT INTO edition_supply_details (tenant_id, edition_id)
                    VALUES (%s, %s)
                    RETURNING id
                    """,
                    (tenant_id, edition_id),
                )
                sd_row = cur.fetchone()
                supply_detail_id = sd_row and sd_row.get("id")
                if supply_detail_id:
                    if price_us is not None:
                        cur.execute(
                            """
                            INSERT INTO edition_prices (
                                tenant_id, supply_detail_id, price_amount, currency_code
                            )
                            VALUES (%s, %s, %s, 'USD')
                            """,
                            (tenant_id, supply_detail_id, price_us),
                        )
                    if price_can is not None:
                        cur.execute(
                            """
                            INSERT INTO edition_prices (
                                tenant_id, supply_detail_id, price_amount, currency_code
                            )
                            VALUES (%s, %s, %s, 'CAD')
                            """,
                            (tenant_id, supply_detail_id, price_can),
                        )
            except Exception:
                # If the supply/price schema is missing or incompatible, skip prices but keep editions
                pass


def _upsert_work_from_payload(conn, cur, tenant_id: str, body: Dict[str, Any]) -> str:
    work_id = _resolve_work_id(cur, tenant_id, body)
    payload = dict(body)

    uid_val = payload.get("uid")
    try:
        uid_uuid = uuid.UUID(str(uid_val)) if uid_val else uuid.uuid4()
    except (ValueError, TypeError):
        uid_uuid = uuid.uuid4()

    title = _safe_str(payload.get("title"))
    subtitle = _safe_str(payload.get("subtitle"))
    series = _safe_str(payload.get("series") or payload.get("series_title"))
    series_num = int(payload.get("volume_number") or payload.get("series_number") or 0)
    publisher_or_imprint = _safe_str(payload.get("publisher_or_imprint"))
    pub_year = _to_int_or_none(payload.get("publishing_year"))
    language = _safe_str(payload.get("language"))
    rights = _safe_str(payload.get("rights"))
    main_desc = _safe_str(payload.get("main_description") or payload.get("description"))
    cover_link = _safe_str(payload.get("cover_image_link"))
    editor_name = _safe_str(payload.get("editor_name"))
    art_director = _safe_str(payload.get("art_director_name"))
    ages = _safe_str(payload.get("ages"))
    us_grade = _safe_str(payload.get("us_grade"))
    loc_number = _safe_str(payload.get("loc_number"))

    if work_id:
        cur.execute(
            """
            UPDATE works
            SET
                uid = %s,
                title = %s,
                subtitle = %s,
                series_title = %s,
                series_number = %s,
                publisher_or_imprint = %s,
                publishing_year = %s,
                language = %s,
                rights = %s,
                main_description = %s,
                cover_image_link = %s,
                editor_name = %s,
                art_director_name = %s,
                ages = %s,
                us_grade = %s,
                loc_number = %s,
                updated_at = now()
            WHERE tenant_id = %s
              AND id = %s
            """,
            (
                uid_uuid, title, subtitle, series, series_num,
                publisher_or_imprint, pub_year, language, rights,
                main_desc, cover_link, editor_name, art_director,
                ages, us_grade, loc_number, tenant_id, work_id,
            ),
        )
    else:
        work_id = str(uuid.uuid4())
        cur.execute(
            """
            INSERT INTO works (
                id, tenant_id, uid, title, subtitle, series_title, series_number,
                publisher_or_imprint, publishing_year, language, rights, main_description,
                cover_image_link, editor_name, art_director_name, ages, us_grade, loc_number
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                work_id, tenant_id, uid_uuid, title, subtitle, series, series_num,
                publisher_or_imprint, pub_year, language, rights, main_desc,
                cover_link, editor_name, art_director, ages, us_grade, loc_number,
            ),
        )

    _replace_editions(cur, tenant_id, work_id, payload)
    _replace_foreign_rights_sold(cur, tenant_id, work_id, payload)

    author_info = _contributor_input(payload, "author")
    illustrator_info = _contributor_input(payload, "illustrator")

    author_name = _safe_str(author_info.get("name"))
    if not author_name:
        author_name = "Unknown"

    author_party_id = _get_or_create_party(
        cur,
        tenant_id,
        author_name,
        email=_safe_str(author_info.get("email")),
        party_type="person",
    )

    illustrator_party_id: Optional[str] = None
    if _has_real_contributor(payload, "illustrator"):
        illustrator_party_id = _get_or_create_party(
            cur,
            tenant_id,
            _safe_str(illustrator_info.get("name")) or "Unknown",
            email=_safe_str(illustrator_info.get("email")),
            party_type="person",
        )

    cur.execute("DELETE FROM work_contributors WHERE tenant_id = %s AND work_id = %s", (tenant_id, work_id))

    cur.execute(
        """
        INSERT INTO work_contributors (tenant_id, work_id, party_id, contributor_role, sequence_number)
        VALUES (%s, %s, %s, 'AUTHOR', 1)
        """,
        (tenant_id, work_id, author_party_id),
    )

    if illustrator_party_id:
        cur.execute(
            """
            INSERT INTO work_contributors (tenant_id, work_id, party_id, contributor_role, sequence_number)
            VALUES (%s, %s, %s, 'ILLUSTRATOR', 2)
            """,
            (tenant_id, work_id, illustrator_party_id),
        )

    contributor_targets: List[Tuple[str, str]] = [(author_party_id, "author")]
    if illustrator_party_id:
        contributor_targets.append((illustrator_party_id, "illustrator"))

    for party_id, scope in contributor_targets:
        _upsert_party_core(cur, tenant_id, party_id, payload, scope)
        _upsert_contributor_profile(cur, tenant_id, party_id, scope, payload)
        _replace_party_socials(cur, tenant_id, party_id, payload, scope)
        _replace_contributor_published_books(cur, tenant_id, party_id, payload, scope)
        _replace_contributor_media_appearances(cur, tenant_id, party_id, payload, scope)
        _replace_contributor_media_contacts(cur, tenant_id, party_id, payload, scope)

        if scope == "author":
            _replace_contributor_other_publications(cur, tenant_id, party_id, payload.get("author_other_publications") or [])
        else:
            _replace_contributor_other_publications(cur, tenant_id, party_id, payload.get("illustrator_other_publications") or [])

        _replace_contributor_previous_publicity(
            cur,
            tenant_id,
            party_id,
            scope,
            payload.get(f"{scope}_previous_publicity")
            or payload.get(f"{scope}_marketing_previous_book_publicity")
            or payload.get(f"{scope}_publicity_previous_book_publicity")
            or [],
        )
        _replace_contributor_niche_targets(cur, tenant_id, party_id, payload.get(f"{scope}_niche_publicity_targets") or [])

        cur.execute(
            """
            INSERT INTO work_party_preferences (
                tenant_id, work_id, party_id,
                contact_pref_rank1, contact_pref_rank2,
                media_best_times, media_press_share,
                us_travel_plans, travel_dates
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (work_id, party_id)
            DO UPDATE SET
                contact_pref_rank1 = EXCLUDED.contact_pref_rank1,
                contact_pref_rank2 = EXCLUDED.contact_pref_rank2,
                media_best_times = EXCLUDED.media_best_times,
                media_press_share = EXCLUDED.media_press_share,
                us_travel_plans = EXCLUDED.us_travel_plans,
                travel_dates = EXCLUDED.travel_dates
            """,
            (
                tenant_id,
                work_id,
                party_id,
                _safe_str(payload.get(f"{scope}_contact_pref_rank1")),
                _safe_str(payload.get(f"{scope}_contact_pref_rank2")),
                _safe_str(payload.get(f"{scope}_media_best_times")),
                bool(payload.get(f"{scope}_media_press_share") or False),
                _safe_str(payload.get(f"{scope}_us_travel_plans")),
                _safe_str(payload.get(f"{scope}_travel_dates")),
            ),
        )

        agency_payload = _agency_payload_from_input(payload, scope)
        if agency_payload:
            _replace_party_representation(cur, tenant_id, party_id, work_id, agency_payload)

    cur.execute("DELETE FROM royalty_sets WHERE tenant_id = %s AND work_id = %s", (tenant_id, work_id))
    royalties = payload.get("royalties") or {}
    if not isinstance(royalties, dict):
        royalties = {}
    author_roy = royalties.get("author") or {}
    illustrator_roy = royalties.get("illustrator") or {}
    if not isinstance(author_roy, dict):
        author_roy = {}
    if not isinstance(illustrator_roy, dict):
        illustrator_roy = {}

    set_id = uuid.uuid4()
    cur.execute(
        """
        INSERT INTO royalty_sets (id, tenant_id, work_id, version, is_active, source_json)
        VALUES (%s, %s, %s, 1, true, %s::jsonb)
        """,
        (set_id, tenant_id, work_id, json.dumps(royalties)),
    )

    def _insert_royalty_rule(party: str, rights_type: str, rule_obj: Dict[str, Any]) -> None:
        mode = (_safe_str(rule_obj.get("mode")) or "tiered").lower()
        if mode not in ("fixed", "tiered"):
            mode = "tiered"

        base = (_safe_str(rule_obj.get("base")) or "list_price").lower().replace(" ", "_")
        if base not in ("list_price", "net_receipts"):
            base = "list_price"

        format_label = _safe_str(rule_obj.get("format") or rule_obj.get("format_label"))
        subrights_name = _safe_str(rule_obj.get("name") or rule_obj.get("subrights_name"))
        escalating = bool(rule_obj.get("escalating") or False)

        flat_rate = rule_obj.get("flat_rate_percent")
        try:
            flat_rate = float(flat_rate) if flat_rate is not None else None
        except Exception:
            flat_rate = None

        percent = rule_obj.get("percent")
        try:
            percent = float(percent) if percent is not None else None
        except Exception:
            percent = None

        notes = _safe_str(rule_obj.get("note") or rule_obj.get("notes"))

        cur.execute(
            """
            INSERT INTO royalty_rules (
                tenant_id, royalty_set_id, party, rights_type, format_label,
                subrights_name, mode, base, escalating,
                flat_rate_percent, percent, notes
            )
            VALUES (%s, %s, %s::roy_party, %s::roy_rights_type, %s, %s, %s::roy_mode, %s::roy_base, %s, %s, %s, %s)
            RETURNING id
            """,
            (tenant_id, set_id, party, rights_type, format_label, subrights_name, mode, base, escalating, flat_rate, percent, notes),
        )
        rule_row = cur.fetchone()
        if not rule_row:
            return
        rule_id = rule_row["id"]

        for t_idx, tier in enumerate(rule_obj.get("tiers") or [], start=1):
            if not isinstance(tier, dict):
                continue
            try:
                rate = float(tier.get("rate_percent") or tier.get("percent") or 0)
            except Exception:
                rate = 0.0

            tier_base = (_safe_str(tier.get("base")) or "list_price").lower().replace(" ", "_")
            if tier_base not in ("list_price", "net_receipts"):
                tier_base = "list_price"

            tier_note = _safe_str(tier.get("note"))

            cur.execute(
                """
                INSERT INTO royalty_tiers (tenant_id, rule_id, tier_order, rate_percent, base, note)
                VALUES (%s, %s, %s, %s, %s::roy_base, %s)
                RETURNING id
                """,
                (tenant_id, rule_id, t_idx, rate, tier_base, tier_note),
            )
            tier_row = cur.fetchone()
            if not tier_row:
                continue
            tier_id = tier_row["id"]

            for cond in (tier.get("conditions") or []):
                if not isinstance(cond, dict):
                    continue
                kind = (_safe_str(cond.get("kind")) or "units").lower()
                if kind not in ("units", "discount"):
                    kind = "units"

                comp = _safe_str(cond.get("comparator")) or "<"
                if comp not in ("<", "<=", ">", ">=", "=", "!="):
                    comp = "<"

                try:
                    val = float(cond.get("value") or 0)
                except Exception:
                    val = 0.0

                cur.execute(
                    """
                    INSERT INTO royalty_tier_conditions (tenant_id, tier_id, kind, comparator, value)
                    VALUES (%s, %s, %s::roy_condition_kind, %s::roy_comparator, %s)
                    """,
                    (tenant_id, tier_id, kind, comp, val),
                )

    for r in (author_roy.get("first_rights") or []):
        if isinstance(r, dict):
            _insert_royalty_rule("author", "first_rights", r)
    for r in (author_roy.get("subrights") or []):
        if isinstance(r, dict):
            _insert_royalty_rule("author", "subrights", r)
    for r in (illustrator_roy.get("first_rights") or []):
        if isinstance(r, dict):
            _insert_royalty_rule("illustrator", "first_rights", r)
    for r in (illustrator_roy.get("subrights") or []):
        if isinstance(r, dict):
            _insert_royalty_rule("illustrator", "subrights", r)

    conn.commit()
    return work_id


@router.post("/works")
async def post_work(request: Request, tenant_slug: str = Query(...)):
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="JSON body required")
    if not isinstance(body, dict):
        body = {}

    with db_conn() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            tenant_id = _get_tenant_id_from_slug(cur, tenant_slug)
            work_id = _upsert_work_from_payload(conn, cur, tenant_id, body)
            payload = _build_full_work_payload(cur, tenant_id, work_id)
            return {"ok": True, "tenant_slug": tenant_slug, "work_id": work_id, "work": payload}
        
@router.delete("/works/{work_id}")
def delete_work(work_id: str, tenant_slug: str = Query(...)):
    with db_conn() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            tenant_id = _get_tenant_id_from_slug(cur, tenant_slug)
            cur.execute("SELECT id FROM works WHERE tenant_id = %s AND id = %s LIMIT 1", (tenant_id, work_id))
            if not cur.fetchone():
                raise HTTPException(status_code=404, detail="Work not found")
            cur.execute("DELETE FROM works WHERE tenant_id = %s AND id = %s", (tenant_id, work_id))
            conn.commit()
            return {"ok": True, "tenant_slug": tenant_slug, "work_id": work_id, "deleted": True}


@router.delete("/works")
def delete_work_by_title_author(
    tenant_slug: str = Query(...),
    title: str = Query(""),
    author: str = Query(""),
):
    with db_conn() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            tenant_id = _get_tenant_id_from_slug(cur, tenant_slug)
            work_id = _resolve_work_id_by_title_author(cur, tenant_id, title, author)
            if not work_id:
                raise HTTPException(status_code=404, detail="Work not found for given title and author")
            cur.execute("DELETE FROM works WHERE tenant_id = %s AND id = %s", (tenant_id, work_id))
            conn.commit()
            return {"ok": True, "tenant_slug": tenant_slug, "work_id": work_id, "deleted": True}