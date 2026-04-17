# routers/catalog.py — Catalog API: list works, full work payload (legacy-compatible shape), resolve by ISBN.
# Read-side routing and payload assembly only. Write logic lives in catalog_write.py.

from __future__ import annotations

import ast
from datetime import date, datetime
from typing import Any, Dict, List, Optional, Tuple

from fastapi import APIRouter, HTTPException, Query, Request
from psycopg.rows import dict_row

from app.core.db import db_conn

from .catalog_shared import (
    _safe_str,
    _role_to_scope,
    _is_author_role,
    _is_illustrator_role,
    _format_phone,
    _is_blank_row,
)
from .catalog_royalties import (
    _fetch_royalties_graph,
)
from .catalog_write import _upsert_work_from_payload
# catalog.py
from .catalog_dealmemo import (_upsert_work_from_deal_memo,)

router = APIRouter(prefix="/catalog", tags=["Catalog"])


def _jsonable(v: Any) -> Any:
    if isinstance(v, (datetime, date)):
        return v.isoformat()
    return v

def _clean_display_name(val: Optional[str]) -> str:
    if not val:
        return ""
    return str(val).strip()

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
        raise HTTPException(
            status_code=404,
            detail=f"Unknown tenant_slug: {tenant_slug} (and no tenants in DB)",
        )
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
        "publisher_or_imprint": (
            row.get("publisher_or_imprint")
            or row.get("imprint_name")
            or row.get("publisher_name")
            or ""
        ),
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


def _category_rows(
    contact_categories: Dict[str, List[Dict[str, Any]]], *aliases: str
) -> List[Dict[str, Any]]:
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
        out.append(
            {
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
            }
        )
    return out


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
                "date": _jsonable(r.get("sold_date")),
                "expiration": _jsonable(r.get("expiration_date")),
                "notes": _safe_str(r.get("notes")),
                "created_at": _jsonable(r.get("created_at")),
                "updated_at": _jsonable(r.get("updated_at")),
            }
        )
    return out


def _fetch_contributor_contact_categories(
    cur, tenant_id: str, party_id: str, scope: str
) -> Dict[str, List[Dict[str, Any]]]:
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
        category_lc = raw_category.lower()

        name = _safe_str(r.get("name"))
        company = _safe_str(r.get("company_or_outlet"))
        website = _safe_str(r.get("website"))
        phone = _safe_str(r.get("phone"))
        email = _safe_str(r.get("email"))
        notes = _safe_str(r.get("notes"))
        rel_note = _safe_str(r.get("relationship_note"))

        if category_lc.endswith("_marketing_bloggers"):
            item = {
                "name": name,
                "url": website,
                "contact": rel_note,
                "notes": notes,
                "relationship": rel_note,
                "connection": rel_note,
                "phone": phone,
                "email": email,
                "outlet": company,
                "company": company,
                "position": _safe_str(r.get("position")),
                "city": _safe_str(r.get("city")),
                "state": _safe_str(r.get("state")),
                "zip": _safe_str(r.get("zip")),
                "country": _safe_str(r.get("country")),
                "social_handle": _safe_str(r.get("social_handle")),
                "personal_contact": bool(r.get("personal_contact") or False),
                "link_type": _safe_str(r.get("link_type")),
                "contact_type": _safe_str(r.get("contact_type")),
            }

        elif category_lc.endswith("_marketing_endorsers"):
            item = {
                "name": name,
                "contact": company or phone or email or website or rel_note,
                "notes": notes,
                "relationship": rel_note,
                "connection": rel_note,
                "url": website,
                "phone": phone,
                "email": email,
                "outlet": company,
                "company": company,
                "position": _safe_str(r.get("position")),
                "city": _safe_str(r.get("city")),
                "state": _safe_str(r.get("state")),
                "zip": _safe_str(r.get("zip")),
                "country": _safe_str(r.get("country")),
                "social_handle": _safe_str(r.get("social_handle")),
                "personal_contact": bool(r.get("personal_contact") or False),
                "link_type": _safe_str(r.get("link_type")),
                "contact_type": _safe_str(r.get("contact_type")),
            }

        elif category_lc.endswith("_marketing_review_copy_wishlist"):
            item = {
                "outlet": company,
                "company": company,
                "contact": name,
                "name": name,
                "connection": rel_note,
                "relationship": rel_note,
                "notes": notes,
                "url": website,
                "phone": phone,
                "email": email,
                "position": _safe_str(r.get("position")),
                "city": _safe_str(r.get("city")),
                "state": _safe_str(r.get("state")),
                "zip": _safe_str(r.get("zip")),
                "country": _safe_str(r.get("country")),
                "social_handle": _safe_str(r.get("social_handle")),
                "personal_contact": bool(r.get("personal_contact") or False),
                "link_type": _safe_str(r.get("link_type")),
                "contact_type": _safe_str(r.get("contact_type")),
            }

        elif category_lc.endswith("_marketing_local_media"):
            item = {
                "outlet": company,
                "company": company,
                "contact": name,
                "name": name,
                "notes": notes,
                "relationship": rel_note,
                "connection": rel_note,
                "url": website,
                "phone": phone,
                "email": email,
                "position": _safe_str(r.get("position")),
                "city": _safe_str(r.get("city")),
                "state": _safe_str(r.get("state")),
                "zip": _safe_str(r.get("zip")),
                "country": _safe_str(r.get("country")),
                "social_handle": _safe_str(r.get("social_handle")),
                "personal_contact": bool(r.get("personal_contact") or False),
                "link_type": _safe_str(r.get("link_type")),
                "contact_type": _safe_str(r.get("contact_type")),
            }

        elif category_lc.endswith("_marketing_alumni_org_publications"):
            item = {
                "outlet": company,
                "company": company,
                "contact": name,
                "name": name,
                "notes": notes,
                "relationship": rel_note,
                "connection": rel_note,
                "url": website,
                "phone": phone,
                "email": email,
                "position": _safe_str(r.get("position")),
                "city": _safe_str(r.get("city")),
                "state": _safe_str(r.get("state")),
                "zip": _safe_str(r.get("zip")),
                "country": _safe_str(r.get("country")),
                "social_handle": _safe_str(r.get("social_handle")),
                "personal_contact": bool(r.get("personal_contact") or False),
                "link_type": _safe_str(r.get("link_type")),
                "contact_type": _safe_str(r.get("contact_type")),
            }

        elif category_lc.endswith("_marketing_big_mouth_list"):
            item = {
                "name": name,
                "contact": phone or email or website or rel_note,
                "relationship": rel_note,
                "connection": rel_note,
                "notes": notes,
                "url": website,
                "phone": phone,
                "email": email,
                "outlet": company,
                "company": company,
                "position": _safe_str(r.get("position")),
                "city": _safe_str(r.get("city")),
                "state": _safe_str(r.get("state")),
                "zip": _safe_str(r.get("zip")),
                "country": _safe_str(r.get("country")),
                "social_handle": _safe_str(r.get("social_handle")),
                "personal_contact": bool(r.get("personal_contact") or False),
                "link_type": _safe_str(r.get("link_type")),
                "contact_type": _safe_str(r.get("contact_type")),
            }

        elif category_lc.endswith("_marketing_endorsers"):
            item = {
                "name": name,
                "contact": phone or email or website or rel_note,
                "notes": notes,
                "relationship": rel_note,
                "connection": rel_note,
                "url": website,
                "phone": phone,
                "email": email,
                "outlet": company,
                "company": company,
                "position": _safe_str(r.get("position")),
                "city": _safe_str(r.get("city")),
                "state": _safe_str(r.get("state")),
                "zip": _safe_str(r.get("zip")),
                "country": _safe_str(r.get("country")),
                "social_handle": _safe_str(r.get("social_handle")),
                "personal_contact": bool(r.get("personal_contact") or False),
                "link_type": _safe_str(r.get("link_type")),
                "contact_type": _safe_str(r.get("contact_type")),
            }
        
        elif category_lc.endswith("_sales_local_bookstores"):
            item = {
                "name": name,
                "kind": _safe_str(r.get("position")),
                "chain_name": company,
                "city": _safe_str(r.get("city")),
                "state": _safe_str(r.get("state")),
                "contact": rel_note,
                "notes": notes,
                "url": website,
                "phone": phone,
                "email": email,
                "outlet": company,
                "company": company,
                "personal_contact": bool(r.get("personal_contact") or False),
                "link_type": _safe_str(r.get("link_type")),
                "contact_type": _safe_str(r.get("contact_type")),
            }
        
        elif category_lc.endswith("_sales_schools_libraries"):
            item = {
                "name": name,
                "kind": _safe_str(r.get("position") or r.get("company_or_outlet")),
                "city": _safe_str(r.get("city")),
                "state": _safe_str(r.get("state")),
                "contact": rel_note,
                "notes": notes,
                "url": website,
                "phone": phone,
                "email": email,
                "outlet": company,
                "company": company,
                "personal_contact": bool(r.get("personal_contact") or False),
                "link_type": _safe_str(r.get("link_type")),
                "contact_type": _safe_str(r.get("contact_type")),
            }
        
        elif category_lc.endswith("_sales_societies_orgs_conf"):
            item = {
                "name": name,
                "kind": _safe_str(r.get("position")),
                "contact": rel_note,
                "personal_contact": bool(r.get("personal_contact") or False),
                "notes": notes,
                "url": website,
                "phone": phone,
                "email": email,
                "outlet": company,
                "company": company,
                "city": _safe_str(r.get("city")),
                "state": _safe_str(r.get("state")),
                "link_type": _safe_str(r.get("link_type")),
                "contact_type": _safe_str(r.get("contact_type")),
            }
        
        elif category_lc.endswith("_sales_nontrade_outlets"):
            item = {
                "name": name,
                "category": _safe_str(r.get("position")),
                "contact": rel_note,
                "notes": notes,
                "url": website,
                "phone": phone,
                "email": email,
                "outlet": company,
                "company": company,
                "city": _safe_str(r.get("city")),
                "state": _safe_str(r.get("state")),
                "personal_contact": bool(r.get("personal_contact") or False),
                "link_type": _safe_str(r.get("link_type")),
                "contact_type": _safe_str(r.get("contact_type")),
            }

        elif category_lc.endswith("_sales_museums_parks"):
            item = {
                "name": name,
                "kind": _safe_str(r.get("position")),
                "connection": company,
                "contact": rel_note,
                "personal_contact": bool(r.get("personal_contact") or False),
                "notes": notes,
                "url": website,
                "phone": phone,
                "email": email,
                "outlet": company,
                "company": company,
                "city": _safe_str(r.get("city")),
                "state": _safe_str(r.get("state")),
                "link_type": _safe_str(r.get("link_type")),
                "contact_type": _safe_str(r.get("contact_type")),
            }
    

        else:
            label = name or company
            item = {
                "name": label,
                "outlet": company,
                "company": company,
                "contact": name,
                "position": _safe_str(r.get("position")),
                "email": email,
                "phone": phone,
                "url": website,
                "city": _safe_str(r.get("city")),
                "state": _safe_str(r.get("state")),
                "zip": _safe_str(r.get("zip")),
                "country": _safe_str(r.get("country")),
                "social_handle": _safe_str(r.get("social_handle")),
                "personal_contact": bool(r.get("personal_contact") or False),
                "relationship": rel_note,
                "connection": rel_note,
                "notes": notes,
                "link_type": _safe_str(r.get("link_type")),
                "contact_type": _safe_str(r.get("contact_type")),
            }

        out.setdefault(raw_category, []).append(item)
        if normalized_category and normalized_category != raw_category:
            out.setdefault(normalized_category, []).append(item)

    return out


def _fetch_contributor_marketing_profile(
    cur, tenant_id: str, party_id: str, scope: str
) -> Dict[str, Any]:
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


def _fetch_party_extras_block(
    cur, tenant_id: str, party_id: str, work_id: str, scope: str
) -> Dict[str, Any]:
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
            SELECT
                id,
                scope,
                item_order,
                title,
                publication,
                date_text,
                notes
            FROM contributor_other_publications
            WHERE tenant_id = %s
              AND party_id = %s
              AND lower(scope) = %s
            ORDER BY item_order ASC, id ASC
            """,
            (tenant_id, party_id, scope.lower()),
        )
        other_pubs = [
            {
                "title": _safe_str(r.get("title")),
                "publication": _safe_str(r.get("publication")),
                "date": _safe_str(r.get("date_text")),
                "date_text": _safe_str(r.get("date_text")),
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
            SELECT
                id,
                scope,
                item_order,
                target_area,
                notes,
                source_category
            FROM contributor_niche_publicity_targets
            WHERE tenant_id = %s
              AND party_id = %s
              AND lower(scope) = %s
            ORDER BY item_order ASC, id ASC
            """,
            (tenant_id, party_id, scope.lower()),
        )
        niche_targets = [
            {
                "target_name": _safe_str(r.get("target_area")),
                "target_area": _safe_str(r.get("target_area")),
                "area": _safe_str(r.get("target_area")),
                "notes": _safe_str(r.get("notes")),
                "source_category": _safe_str(r.get("source_category")),
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
    cur, tenant_id: str, represented_party_id: str, work_id: str
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
                linked_agents.append(
                    {
                        "agent_name": _clean_display_name(r.get("agent_name")),
                        "agent_email": _safe_str(r.get("agent_email")),
                        "agent_phone_country_code": _safe_str(
                            r.get("agent_phone_country_code")
                        ),
                        "agent_phone_number": _safe_str(r.get("agent_phone_number")),
                        "agent_phone": _format_phone(
                            r.get("agent_phone_country_code"),
                            r.get("agent_phone_number"),
                        ),
                        "is_primary": bool(r.get("is_primary")),
                        "role_label": _safe_str(r.get("role_label")),
                    }
                )
            primary_agent_party = {
                "display_name": _clean_display_name(rows_as_agency[0].get("agent_name")),
                "email": _safe_str(rows_as_agency[0].get("agent_email")),
                "website": _safe_str(rows_as_agency[0].get("agent_website")),
                "phone_country_code": _safe_str(
                    rows_as_agency[0].get("agent_phone_country_code")
                ),
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
                    linked_agents.append(
                        {
                            "agent_name": _clean_display_name(r.get("agent_name")),
                            "agent_email": _safe_str(r.get("agent_email")),
                            "agent_phone_country_code": _safe_str(
                                r.get("agent_phone_country_code")
                            ),
                            "agent_phone_number": _safe_str(r.get("agent_phone_number")),
                            "agent_phone": _format_phone(
                                r.get("agent_phone_country_code"),
                                r.get("agent_phone_number"),
                            ),
                            "is_primary": bool(r.get("is_primary")),
                            "role_label": _safe_str(r.get("role_label")),
                        }
                    )
            else:
                primary_agent_party = linked_party
                agency_party = {}

        out = linked_agents

        agency_name = _clean_display_name(agency_party.get("display_name"))
        agency_email = _safe_str(agency_party.get("email"))
        agency_website = _safe_str(agency_party.get("website"))
        agency_phone = _format_phone(
            agency_party.get("phone_country_code"), agency_party.get("phone_number")
        )

        primary_agent_name = _clean_display_name(primary_agent_party.get("display_name"))
        primary_agent_email = _safe_str(primary_agent_party.get("email"))
        primary_agent_phone = _format_phone(
            primary_agent_party.get("phone_country_code"),
            primary_agent_party.get("phone_number"),
        )

        address_lines: List[str] = []
        if agency_party.get("id"):
            address_lines = _fetch_party_address_lines(
                cur, tenant_id, str(agency_party["id"])
            )
        if not address_lines and primary_agent_party.get("id"):
            address_lines = _fetch_party_address_lines(
                cur, tenant_id, str(primary_agent_party["id"])
            )

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


def _fetch_onix_raw_by_isbns(
    cur, tenant_id: str, isbns: List[str], limit_each: int = 1
) -> Dict[str, Any]:
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
            out[isbn] = [
                {
                    "record_reference": r.get("record_reference") or "",
                    "isbn13": r.get("isbn13") or "",
                    "created_at": _jsonable(r.get("created_at")),
                    "product_xml": r.get("product_xml") or "",
                }
                for r in rows
            ]
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
        import uuid

        uuid.UUID(raw)
    except (ValueError, TypeError):
        return None
    cur.execute(
        "SELECT id FROM works WHERE tenant_id = %s AND uid = %s LIMIT 1",
        (tenant_id, raw),
    )
    row = cur.fetchone()
    return str(row["id"]) if row else None


def _apply_contact_category_aliases(
    doc: Dict[str, Any],
    scope: str,
    contact_categories: Dict[str, List[Dict[str, Any]]],
) -> None:
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
    local_bookstores = _category_rows(
        contact_categories,
        "sales_local_bookstores",
        "local_bookstores",
        "honor_local_bookstores",
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

    doc[f"{scope}_sales_local_bookstores"] = local_bookstores
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

        # rebuild arrays from scalar DB columns
        "about_promotable_points": [
            v for v in [
                _safe_str(w.get("about_promotable_point_1")),
                _safe_str(w.get("about_promotable_point_2")),
                _safe_str(w.get("about_promotable_point_3")),
            ] if v
        ],
        "about_diff_competitors": [
            v for v in [
                _safe_str(w.get("about_diff_competitor_1")),
                _safe_str(w.get("about_diff_competitor_2")),
                _safe_str(w.get("about_diff_competitor_3")),
            ] if v
        ],
        "about_extra": _safe_str(w.get("about_extra")),

        "loc_number": _safe_str(w.get("loc_number")),
    }

    doc["about_promotable_points"] = [x for x in doc["about_promotable_points"] if x]
    doc["about_diff_competitors"] = [x for x in doc["about_diff_competitors"] if x]

    editions = _fetch_editions(cur, tenant_id, work_id)
    doc["formats"] = [
        {
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
        }
        for e in editions
    ]
    doc["_editions"] = editions
    doc["foreign_rights_sold"] = _fetch_foreign_rights_sold(cur, tenant_id, work_id)

    contributors = _fetch_contributors(cur, tenant_id, work_id)
    doc["_contributors"] = contributors

    author_party_id: Optional[str] = None
    illustrator_party_id: Optional[str] = None
    author_name = ""
    illustrator_name = ""

    work_contributors: List[Dict[str, Any]] = []

    for c in contributors:
        party_id = str(c["party_id"]) if c.get("party_id") else None
        role = c.get("role") or ""
        scope = c.get("scope") or ""
        display_name = c.get("display_name") or ""
        email = c.get("email") or ""

        normalized_role = ""
        if _is_author_role(role) or scope == "author":
            normalized_role = "AUTHOR"
            if not author_party_id:
                author_party_id = party_id
                author_name = display_name

        elif _is_illustrator_role(role) or scope == "illustrator":
            normalized_role = "ILLUSTRATOR"
            if not illustrator_party_id:
                illustrator_party_id = party_id
                illustrator_name = display_name

        work_contributors.append(
            {
                "party_id": party_id,
                "contributor_role": normalized_role or role,
                "sequence_number": c.get("sequence_number"),
                "display_name": display_name,
                "email": email,
            }
        )

    doc["work_contributors"] = work_contributors
    doc["author_party_id"] = author_party_id
    doc["illustrator_party_id"] = illustrator_party_id

    # Keep existing contributor names if they were not already set elsewhere
    if author_name and not doc.get("author"):
        doc["author"] = author_name
    if illustrator_name and not doc.get("illustrator"):
        doc["illustrator"] = illustrator_name

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
        line2 = " ".join(
            [
                _safe_str(address.get("city")),
                _safe_str(address.get("state")),
                _safe_str(address.get("zip")),
                _safe_str(address.get("country")),
            ]
        ).strip()
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
        doc[f"{scope}_marketing_niche_publicity"] = [
            {
                "area": _safe_str(r.get("area") or r.get("target_area") or r.get("target_name")),
                "notes": _safe_str(r.get("notes")),
            }
            for r in (block.get("niche_publicity_targets") or [])
            if _safe_str(r.get("source_category")) == "niche_publicity"
            and _safe_str(r.get("area") or r.get("target_area") or r.get("target_name"))
            and not _safe_str(r.get("notes")).startswith("Contact:")
        ]
        doc[f"{scope}_publicity_niche_marketing"] = doc[f"{scope}_marketing_niche_publicity"]

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
        doc[f"{scope}_agent_name"] = _safe_str(
            agency_card.get("agent") or agency_card.get("contact")
        )
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

        doc[scope]["sales_local_bookstores"] = (
            doc.get(f"{scope}_sales_local_bookstores", []) or []
        )
        doc[scope]["sales_nontrade_outlets"] = (
            doc.get(f"{scope}_sales_nontrade_outlets", []) or []
        )
        doc[scope]["sales_museums_parks"] = (
            doc.get(f"{scope}_sales_museums_parks", []) or []
        )

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
            "name",
            "email",
            "website",
            "phone",
            "phone_country_code",
            "phone_number",
            "address",
            "birth_city",
            "birth_country",
            "birth_date",
            "citizenship",
            "bio",
            "long_bio",
            "book_bio",
            "website_bio",
            "photo_credit",
            "present_position",
            "former_positions",
            "degrees_honors",
            "professional_honors",
            "additional_notes",
            "socials",
            "books_published",
            "published_books",
            "media_appearances",
            "other_publications",
            "media_contacts",
            "previous_publicity",
            "niche_publicity_targets",
            "contact_pref_rank1",
            "contact_pref_rank2",
            "media_best_times",
            "media_press_share",
            "us_travel_plans",
            "travel_dates",
            "agent",
            "agency",
            "agent_list",
            "agency_name",
            "agent_name",
            "agent_email",
            "agent_phone",
            "agency_website",
            "has_agency",
            "marketing_previous_book_publicity",
            "publicity_previous_book_publicity",
            "marketing_endorsers",
            "publicity_endorsers_blurbers",
            "marketing_big_mouth_list",
            "publicity_big_mouth_list",
            "marketing_review_copy_wishlist",
            "publicity_review_copy_wishlist",
            "marketing_local_media",
            "publicity_local_media",
            "marketing_alumni_org_publications",
            "publicity_alumni_org_publications",
            "marketing_targeted_sites",
            "publicity_target_sites",
            "marketing_bloggers",
            "publicity_bloggers_genre",
            "sales_local_bookstores",
            "sales_nontrade_outlets",
            "sales_museums_parks",
            "contact_categories",
        ):
            full_key = f"{scope}_{key}"
            if full_key not in doc:
                if key in (
                    "socials",
                    "books_published",
                    "published_books",
                    "media_appearances",
                    "other_publications",
                    "media_contacts",
                    "previous_publicity",
                    "niche_publicity_targets",
                    "agent",
                    "agent_list",
                    "marketing_previous_book_publicity",
                    "publicity_previous_book_publicity",
                    "marketing_endorsers",
                    "publicity_endorsers_blurbers",
                    "marketing_big_mouth_list",
                    "publicity_big_mouth_list",
                    "marketing_review_copy_wishlist",
                    "publicity_review_copy_wishlist",
                    "marketing_local_media",
                    "publicity_local_media",
                    "marketing_alumni_org_publications",
                    "publicity_alumni_org_publications",
                    "marketing_targeted_sites",
                    "publicity_target_sites",
                    "marketing_bloggers",
                    "publicity_bloggers_genre",
                    "sales_local_bookstores",
                    "sales_nontrade_outlets",
                    "sales_museums_parks",
                ):
                    doc[full_key] = []
                elif key in ("address", "agency", "contact_categories"):
                    doc[full_key] = {}
                elif key in ("media_press_share", "has_agency"):
                    doc[full_key] = False
                else:
                    doc[full_key] = ""

    doc["royalties"] = _fetch_royalties_graph(cur, tenant_id, work_id)
    doc["author_advance"] = (
    (doc.get("royalties") or {}).get("author", {}).get("advance")
)
    doc["illustrator_advance"] = (
        (doc.get("royalties") or {}).get("illustrator", {}).get("advance")
    )
    isbns = [e["isbn13"] for e in editions if e.get("isbn13")]
    doc["_onix_raw_products_by_isbn13"] = _fetch_onix_raw_by_isbns(
        cur, tenant_id, isbns, limit_each=1
    )

    return doc


@router.get("/works")
def list_works(
    tenant_slug: str = Query(...),
    q: str = Query(""),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    exclude_draft_contracts: bool = Query(
        True, description="Ignored; kept for API compat"
    ),
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
                        wc.contributor_role,
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
            return {
                "ok": True,
                "tenant_slug": tenant_slug,
                "work_id": resolved_id,
                "work": payload,
            }


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
            return {
                "ok": True,
                "tenant_slug": tenant_slug,
                "work_id": wid,
                "work": payload,
            }


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
            return {
                "ok": True,
                "tenant_slug": tenant_slug,
                "work_id": work_id,
                "work": payload,
            }


@router.delete("/works/{work_id}")
def delete_work(work_id: str, tenant_slug: str = Query(...)):
    with db_conn() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            tenant_id = _get_tenant_id_from_slug(cur, tenant_slug)
            cur.execute(
                "SELECT id FROM works WHERE tenant_id = %s AND id = %s LIMIT 1",
                (tenant_id, work_id),
            )
            if not cur.fetchone():
                raise HTTPException(status_code=404, detail="Work not found")
            cur.execute(
                "DELETE FROM works WHERE tenant_id = %s AND id = %s",
                (tenant_id, work_id),
            )
            conn.commit()
            return {
                "ok": True,
                "tenant_slug": tenant_slug,
                "work_id": work_id,
                "deleted": True,
            }
@router.post("/works/from-dealmemo")
def create_work_from_dealmemo(
    uid: str = Query(...),
    tenant_slug: str = Query(...),
):
    with db_conn() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            tenant_id = _get_tenant_id_from_slug(cur, tenant_slug)
            work_id = _upsert_work_from_deal_memo(cur, tenant_id, uid)

            payload = _build_full_work_payload(cur, tenant_id, work_id)
            return {
                "ok": True,
                "tenant_slug": tenant_slug,
                "work_id": work_id,
                "work": payload,
            }