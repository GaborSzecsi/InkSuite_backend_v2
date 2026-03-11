# routers/catalog.py — Catalog API: list works, full work payload (legacy shape), resolve by ISBN.
# Read/write: works, editions, parties, work_contributors, work_list_items, work_party_preferences,
# party_socials, party_published_books, party_media_appearances, royalty_*, onix_raw_products.
from __future__ import annotations

import json
import uuid
from typing import Any, Dict, List, Optional, Tuple
from datetime import date, datetime
from fastapi import APIRouter, HTTPException, Query, Request

from app.core.db import db_conn
from psycopg.rows import dict_row

router = APIRouter(prefix="/catalog", tags=["Catalog"])


def _jsonable(v: Any) -> Any:
    if isinstance(v, (datetime, date)):
        return v.isoformat()
    return v


def _deep_merge(base: Dict[str, Any], overlay: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(base or {})
    for k, v in (overlay or {}).items():
        if isinstance(v, dict) and isinstance(out.get(k), dict):
            out[k] = _deep_merge(out[k], v)
        else:
            out[k] = v
    return out


def _get_tenant_id_from_slug(cur, tenant_slug: str) -> str:
    cur.execute(
        "SELECT id FROM tenants WHERE lower(slug) = lower(%s) LIMIT 1",
        (tenant_slug.strip(),),
    )
    row = cur.fetchone()
    if not row:
        # Fallback for single-tenant DBs: use the first tenant so catalog still returns works
        cur.execute("SELECT id FROM tenants ORDER BY id LIMIT 1")
        row = cur.fetchone()
    if not row:
        raise HTTPException(status_code=404, detail=f"Unknown tenant_slug: {tenant_slug} (and no tenants in DB)")
    return str(row["id"])


def tenant_id_from_slug(conn, tenant_slug: str) -> str:
    """
    Backwards-compatible helper that delegates to _get_tenant_id_from_slug.

    This keeps older endpoints (that use a raw connection instead of an
    explicit cursor) consistent with the newer catalog behavior:
    - case-insensitive slug matching
    - fallback to the first tenant if no slug match (single-tenant setups)
    """
    with conn.cursor(row_factory=dict_row) as cur:
        return _get_tenant_id_from_slug(cur, tenant_slug)


def _work_row_to_list_item(row: Dict[str, Any]) -> Dict[str, Any]:
    extras = row.get("internal_extras") or {}
    if isinstance(extras, str):
        try:
            extras = json.loads(extras) if extras else {}
        except Exception:
            extras = {}
    author = extras.get("author") if isinstance(extras, dict) else ""
    return {
        "id": str(row["id"]),
        "uid": str(row["uid"]) if row.get("uid") else str(row["id"]),
        "title": row.get("title") or "",
        "subtitle": row.get("subtitle") or "",
        "author": author or "",
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


def _fetch_editions(cur, tenant_id: str, work_id: str) -> List[Dict[str, Any]]:
    cur.execute(
        """
        SELECT id, isbn13, status, source_extras, created_at, updated_at
        FROM editions
        WHERE tenant_id=%s AND work_id=%s
        ORDER BY created_at ASC
        """,
        (tenant_id, work_id),
    )
    rows = cur.fetchall() or []
    out: List[Dict[str, Any]] = []
    for r in rows:
        se = r.get("source_extras") or {}
        if isinstance(se, str):
            try:
                se = json.loads(se) if se else {}
            except Exception:
                se = {}
        out.append({
            "id": str(r["id"]),
            "isbn": r.get("isbn13") or se.get("isbn13") or se.get("isbn") or "",
            "isbn13": r.get("isbn13") or "",
            "status": r.get("status") or "",
            "format": se.get("format") or se.get("Format") or se.get("binding") or se.get("product_form") or "",
            "pub_date": se.get("pub_date") or se.get("publication_date") or "",
            "price_us": se.get("price_us") or se.get("Price US") or 0,
            "price_can": se.get("price_can") or se.get("Price CAN") or 0,
            "loc_number": se.get("loc_number") or se.get("Library of Congress number") or "",
            "pages": se.get("pages") or se.get("Length (pages)") or 0,
            "tall": se.get("tall") or 0,
            "wide": se.get("wide") or 0,
            "spine": se.get("spine") or 0,
            "weight": se.get("weight") or 0,
            "source_extras": se,
            "created_at": _jsonable(r.get("created_at")),
            "updated_at": _jsonable(r.get("updated_at")),
        })
    return out


def _fetch_party_summary(cur, party_id: str) -> Dict[str, Any]:
    cur.execute(
        """
        SELECT display_name, email, website, phone_country_code, phone_number, short_bio, long_bio
        FROM parties WHERE id=%s LIMIT 1
        """,
        (party_id,),
    )
    r = cur.fetchone()
    if not r:
        return {}
    return {
        "display_name": (r.get("display_name") or "").strip(),
        "email": (r.get("email") or "").strip(),
        "website": (r.get("website") or "").strip(),
        "phone_country_code": (r.get("phone_country_code") or "").strip(),
        "phone_number": (r.get("phone_number") or "").strip(),
        "short_bio": (r.get("short_bio") or "").strip(),
        "long_bio": (r.get("long_bio") or "").strip(),
    }


def _fetch_contributors(cur, tenant_id: str, work_id: str) -> List[Dict[str, Any]]:
    cur.execute(
        """
        SELECT
          wc.party_id,
          wc.contributor_role,
          wc.sequence_number,
          p.display_name,
          p.extras
        FROM work_contributors wc
        JOIN parties p ON p.id = wc.party_id
        WHERE wc.work_id=%s AND p.tenant_id=%s
        ORDER BY wc.sequence_number ASC
        """,
        (work_id, tenant_id),
    )
    rows = cur.fetchall() or []
    out: List[Dict[str, Any]] = []
    for r in rows:
        out.append({
            "party_id": str(r["party_id"]),
            "role": r.get("contributor_role") or "",
            "sequence_number": r.get("sequence_number") or 0,
            "display_name": r.get("display_name") or "",
            "extras": r.get("extras") or {},
        })
    return out


def _fetch_party_extras_block(cur, tenant_id: str, party_id: str, work_id: str) -> Dict[str, Any]:
    cur.execute(
        """
        SELECT contact_pref_rank1, contact_pref_rank2,
               media_best_times, media_press_share,
               us_travel_plans, travel_dates,
               extras
        FROM work_party_preferences
        WHERE tenant_id=%s AND party_id=%s AND work_id=%s
        LIMIT 1
        """,
        (tenant_id, party_id, work_id),
    )
    pref = cur.fetchone() or {}

    cur.execute(
        """
        SELECT platform, url, handle, extras
        FROM party_socials
        WHERE tenant_id=%s AND party_id=%s
        ORDER BY platform ASC
        """,
        (tenant_id, party_id),
    )
    socials = cur.fetchall() or []

    cur.execute(
        """
        SELECT title, isbn, publisher, year, approx_sold, extras
        FROM party_published_books
        WHERE tenant_id=%s AND party_id=%s
        ORDER BY title ASC
        """,
        (tenant_id, party_id),
    )
    pubs = cur.fetchall() or []

    cur.execute(
        """
        SELECT title, venue, appearance_date, link, extras
        FROM party_media_appearances
        WHERE tenant_id=%s AND party_id=%s
        ORDER BY appearance_date DESC NULLS LAST
        """,
        (tenant_id, party_id),
    )
    media = cur.fetchall() or []

    return {
        "preferences": pref or {},
        "socials": socials,
        "published_books": pubs,
        "media_appearances": media,
    }


def _fetch_work_list_items(cur, tenant_id: str, work_id: str) -> List[Dict[str, Any]]:
    cur.execute(
        """
        SELECT
          party_id, scope, category, item_order,
          name, outlet, company, relationship, kind, chain_name, area, city, state,
          url, contact, position, phone, email, connection, personal_contact, notes,
          extras
        FROM work_list_items
        WHERE tenant_id=%s AND work_id=%s
        ORDER BY scope ASC, category ASC, item_order ASC
        """,
        (tenant_id, work_id),
    )
    return cur.fetchall() or []


def _fetch_royalties_graph(cur, tenant_id: str, work_id: str) -> Dict[str, Any]:
    cur.execute(
        """
        SELECT id, version, is_active, source_json
        FROM royalty_sets
        WHERE tenant_id=%s AND work_id=%s
        ORDER BY is_active DESC, version DESC
        LIMIT 1
        """,
        (tenant_id, work_id),
    )
    set_row = cur.fetchone()
    if not set_row:
        return {"author": {"first_rights": [], "subrights": []}, "illustrator": {"first_rights": [], "subrights": []}}

    royalty_set_id = set_row["id"]

    cur.execute(
        """
        SELECT
          id,
          party, rights_type,
          format_label, subrights_name,
          mode, base, escalating, flat_rate_percent, percent,
          notes, extras
        FROM royalty_rules
        WHERE tenant_id=%s AND royalty_set_id=%s
        ORDER BY party ASC, rights_type ASC
        """,
        (tenant_id, royalty_set_id),
    )
    rules = cur.fetchall() or []

    cur.execute(
        """
        SELECT
          t.id AS tier_id,
          t.rule_id,
          t.tier_order,
          t.rate_percent,
          t.base,
          t.note,
          t.extras
        FROM royalty_tiers t
        WHERE t.tenant_id=%s
          AND t.rule_id IN (SELECT id FROM royalty_rules WHERE tenant_id=%s AND royalty_set_id=%s)
        ORDER BY t.rule_id ASC, t.tier_order ASC
        """,
        (tenant_id, tenant_id, royalty_set_id),
    )
    tiers = cur.fetchall() or []

    cur.execute(
        """
        SELECT tier_id, kind, comparator, value, extras
        FROM royalty_tier_conditions
        WHERE tenant_id=%s
          AND tier_id IN (SELECT id FROM royalty_tiers WHERE tenant_id=%s AND rule_id IN
              (SELECT id FROM royalty_rules WHERE tenant_id=%s AND royalty_set_id=%s))
        ORDER BY tier_id ASC
        """,
        (tenant_id, tenant_id, tenant_id, royalty_set_id),
    )
    conds = cur.fetchall() or []

    tiers_by_rule: Dict[str, List[Dict[str, Any]]] = {}
    for t in tiers:
        tiers_by_rule.setdefault(str(t["rule_id"]), []).append({
            "tier_order": t.get("tier_order") or 0,
            "rate_percent": float(t.get("rate_percent") or 0),
            "base": t.get("base") or "",
            "note": t.get("note") or "",
            "conditions": [],
            "extras": t.get("extras") or {},
            "_tier_id": t["tier_id"],
        })

    conds_by_tier: Dict[str, List[Dict[str, Any]]] = {}
    for c in conds:
        conds_by_tier.setdefault(str(c["tier_id"]), []).append({
            "kind": c.get("kind"),
            "comparator": c.get("comparator"),
            "value": c.get("value"),
            "extras": c.get("extras") or {},
        })

    for rule_id, tier_list in tiers_by_rule.items():
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
            "extras": r.get("extras") or {},
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
            WHERE tenant_id=%s AND isbn13=%s
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
    """Resolve path param (works.id or works.uid) to internal works.id for this tenant."""
    if not work_id or not str(work_id).strip():
        return None
    raw = str(work_id).strip()
    cur.execute(
        "SELECT id FROM works WHERE tenant_id=%s AND id=%s LIMIT 1",
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
        "SELECT id FROM works WHERE tenant_id=%s AND uid=%s LIMIT 1",
        (tenant_id, raw),
    )
    row = cur.fetchone()
    return str(row["id"]) if row else None


def _build_full_work_payload(cur, tenant_id: str, work_id: str) -> Dict[str, Any]:
    resolved = _resolve_work_id_param(cur, tenant_id, work_id)
    if not resolved:
        raise HTTPException(status_code=404, detail="Work not found")
    work_id = resolved
    cur.execute(
        """
        SELECT *
        FROM works
        WHERE tenant_id=%s AND id=%s
        LIMIT 1
        """,
        (tenant_id, work_id),
    )
    w = cur.fetchone()
    if not w:
        raise HTTPException(status_code=404, detail="Work not found")

    base: Dict[str, Any] = w.get("internal_extras") or {}
    if isinstance(base, str):
        try:
            base = json.loads(base) if base else {}
        except Exception:
            base = {}
    if not isinstance(base, dict):
        base = {}

    overlay_work = {
        "id": str(w["id"]),
        "uid": str(w["uid"]) if w.get("uid") else str(w["id"]),
        "title": w.get("title") or base.get("title") or "",
        "subtitle": w.get("subtitle") or base.get("subtitle") or "",
        "series": w.get("series_title") or base.get("series") or "",
        "volume_number": w.get("series_number") or base.get("volume_number") or 0,
        "ages": w.get("ages") or base.get("ages") or "",
        "us_grade": w.get("us_grade") or base.get("us_grade") or "",
        "language": w.get("language") or base.get("language") or "",
        "rights": w.get("rights") or base.get("rights") or "",
        "editor_name": w.get("editor_name") or base.get("editor_name") or "",
        "art_director_name": w.get("art_director_name") or base.get("art_director_name") or "",
        "publisher_or_imprint": w.get("publisher_or_imprint") or base.get("publisher_or_imprint") or "",
        "publishing_year": w.get("publishing_year") or base.get("publishing_year"),
        "publication_date": _jsonable(w.get("publication_date")) or base.get("publication_date") or "",
        "publishing_status": w.get("publishing_status") or base.get("publishing_status") or "",
        "city_of_publication": w.get("city_of_publication") or base.get("city_of_publication") or "",
        "country_of_publication": w.get("country_of_publication") or base.get("country_of_publication") or "",
        "copyright_year": w.get("copyright_year") or base.get("copyright_year") or 0,
        "main_description": w.get("main_description") or base.get("main_description") or "",
        "table_of_contents": w.get("table_of_contents") or base.get("table_of_contents") or "",
        "back_cover_copy": w.get("back_cover_copy") or base.get("back_cover_copy") or "",
        "biographical_note": w.get("biographical_note") or base.get("biographical_note") or "",
        "cover_image_link": w.get("cover_image_link") or base.get("cover_image_link") or "",
        "cover_image_format": w.get("cover_image_format") or base.get("cover_image_format") or "",
        "cover_image_caption": w.get("cover_image_caption") or base.get("cover_image_caption") or "",
        "about_summary": w.get("about_summary") or base.get("about_summary") or "",
        "about_bookstore_shelf": w.get("about_bookstore_shelf") or base.get("about_bookstore_shelf") or "",
        "about_audience": w.get("about_audience") or base.get("about_audience") or "",
        "about_dates_holidays": w.get("about_dates_holidays") or base.get("about_dates_holidays") or "",
        "about_extra": w.get("about_extra") or base.get("about_extra") or "",
    }

    doc = _deep_merge(base, overlay_work)

    editions = _fetch_editions(cur, tenant_id, work_id)
    doc["formats"] = [{
        "format": e.get("format") or "",
        "isbn": e.get("isbn") or "",
        "pub_date": e.get("pub_date") or "",
        "price_us": e.get("price_us") or 0,
        "price_can": e.get("price_can") or 0,
        "loc_number": e.get("loc_number") or "",
        "pages": e.get("pages") or 0,
        "tall": e.get("tall") or 0,
        "wide": e.get("wide") or 0,
        "spine": e.get("spine") or 0,
        "weight": e.get("weight") or 0,
    } for e in editions]

    doc["_editions"] = editions

    contributors = _fetch_contributors(cur, tenant_id, work_id)
    doc["_contributors"] = contributors

    author_party_id: Optional[str] = None
    illustrator_party_id: Optional[str] = None
    for c in contributors:
        r = (c.get("role") or "").upper()
        if (r == "AUTHOR" or "A01" in r) and not author_party_id:
            author_party_id = str(c["party_id"])
            name = c.get("display_name") or (doc.get("author") if isinstance(doc.get("author"), str) else (doc.get("author") or {}).get("name")) or ""
            party_summary = _fetch_party_summary(cur, author_party_id)
            base_author = doc.get("author") if isinstance(doc.get("author"), dict) else {}
            doc["author"] = {
                **base_author,
                "name": name or party_summary.get("display_name") or "",
                "email": party_summary.get("email") or base_author.get("email") or "",
                "website": party_summary.get("website") or base_author.get("website") or "",
                "phone_country_code": party_summary.get("phone_country_code") or "",
                "phone_number": party_summary.get("phone_number") or "",
                "address": base_author.get("address") or {},
            }
        if (r == "ILLUSTRATOR" or "A12" in r) and not illustrator_party_id:
            illustrator_party_id = str(c["party_id"])
            ill_name = c.get("display_name") or ""
            party_summary = _fetch_party_summary(cur, illustrator_party_id)
            base_ill = doc.get("illustrator") if isinstance(doc.get("illustrator"), dict) else {}
            doc["illustrator"] = {
                **base_ill,
                "name": ill_name or party_summary.get("display_name") or "",
                "email": party_summary.get("email") or base_ill.get("email") or "",
                "website": party_summary.get("website") or base_ill.get("website") or "",
                "phone_country_code": party_summary.get("phone_country_code") or "",
                "phone_number": party_summary.get("phone_number") or "",
                "address": base_ill.get("address") or {},
                "agent": base_ill.get("agent") or {},
            }

    if author_party_id:
        a_block = _fetch_party_extras_block(cur, tenant_id, author_party_id, work_id)
        doc["author_socials"] = [{"platform": s.get("platform"), "url": s.get("url"), "handle": s.get("handle", ""), **(s.get("extras") or {})} for s in a_block["socials"]]
        doc["author_books_published"] = [s.get("extras") or {"title": s.get("title"), "isbn": s.get("isbn")} for s in a_block["published_books"]]
        doc["author_media_appearances"] = [m.get("extras") or {"title": m.get("title"), "venue": m.get("venue"), "date": m.get("appearance_date"), "link": m.get("link")} for m in a_block["media_appearances"]]
        pref = a_block["preferences"] or {}
        doc["author_contact_pref_rank1"] = pref.get("contact_pref_rank1") or doc.get("author_contact_pref_rank1") or ""
        doc["author_contact_pref_rank2"] = pref.get("contact_pref_rank2") or doc.get("author_contact_pref_rank2") or ""
        doc["author_media_best_times"] = pref.get("media_best_times") or doc.get("author_media_best_times") or ""
        doc["author_media_press_share"] = bool(pref.get("media_press_share") or doc.get("author_media_press_share") or False)
        doc["author_us_travel_plans"] = pref.get("us_travel_plans") or doc.get("author_us_travel_plans") or ""
        doc["author_travel_dates"] = pref.get("travel_dates") or doc.get("author_travel_dates") or ""

    if illustrator_party_id:
        i_block = _fetch_party_extras_block(cur, tenant_id, illustrator_party_id, work_id)
        doc["illustrator_socials"] = [{"platform": s.get("platform"), "url": s.get("url"), "handle": s.get("handle", ""), **(s.get("extras") or {})} for s in i_block["socials"]]
        doc["illustrator_books_published"] = [s.get("extras") or {"title": s.get("title"), "isbn": s.get("isbn")} for s in i_block["published_books"]]
        doc["illustrator_media_appearances"] = [m.get("extras") or {"title": m.get("title"), "venue": m.get("venue"), "date": m.get("appearance_date"), "link": m.get("link")} for m in i_block["media_appearances"]]
        pref = i_block["preferences"] or {}
        doc["illustrator_contact_pref_rank1"] = pref.get("contact_pref_rank1") or doc.get("illustrator_contact_pref_rank1") or ""
        doc["illustrator_contact_pref_rank2"] = pref.get("contact_pref_rank2") or doc.get("illustrator_contact_pref_rank2") or ""
        doc["illustrator_media_best_times"] = pref.get("media_best_times") or doc.get("illustrator_media_best_times") or ""
        doc["illustrator_media_press_share"] = bool(pref.get("media_press_share") or doc.get("illustrator_media_press_share") or False)
        doc["illustrator_us_travel_plans"] = pref.get("us_travel_plans") or doc.get("illustrator_us_travel_plans") or ""
        doc["illustrator_travel_dates"] = pref.get("travel_dates") or doc.get("illustrator_travel_dates") or ""

    list_rows = _fetch_work_list_items(cur, tenant_id, work_id)
    grouped: Dict[Tuple[str, str], List[Dict[str, Any]]] = {}
    for r in list_rows:
        scope = r.get("scope") or ""
        category = r.get("category") or ""
        item = r.get("extras") or {
            "name": r.get("name"),
            "outlet": r.get("outlet"),
            "company": r.get("company"),
            "relationship": r.get("relationship"),
            "kind": r.get("kind"),
            "chain_name": r.get("chain_name"),
            "area": r.get("area"),
            "city": r.get("city"),
            "state": r.get("state"),
            "url": r.get("url"),
            "contact": r.get("contact"),
            "position": r.get("position"),
            "phone": r.get("phone"),
            "email": r.get("email"),
            "connection": r.get("connection"),
            "personal_contact": bool(r.get("personal_contact") or False),
            "notes": r.get("notes") or "",
        }
        grouped.setdefault((scope, category), []).append(item)

    for (scope, category), items in grouped.items():
        if category:
            doc[category] = items

    doc.setdefault("royalties", {"author": {"first_rights": [], "subrights": []}, "illustrator": {"first_rights": [], "subrights": []}})
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
    exclude_draft_contracts: bool = Query(True, description="Exclude works marked as from_draft_contract"),
):
    with db_conn() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            tenant_id = _get_tenant_id_from_slug(cur, tenant_slug)

            draft_filter = ""
            if exclude_draft_contracts:
                draft_filter = " AND (w.internal_extras->>'from_draft_contract' IS NULL OR (w.internal_extras->>'from_draft_contract')::text NOT IN ('true', '1'))"

            q_like = f"%{q.strip()}%" if q else None
            if q_like:
                cur.execute(
                    """
                    SELECT w.*
                    FROM works w
                    WHERE w.tenant_id=%s
                      AND (
                        w.title ILIKE %s
                        OR (w.internal_extras->>'author') ILIKE %s
                        OR EXISTS (
                          SELECT 1 FROM editions e
                          WHERE e.tenant_id=w.tenant_id AND e.work_id=w.id AND e.isbn13 ILIKE %s
                        )
                      )
                      """ + draft_filter + """
                    ORDER BY w.updated_at DESC NULLS LAST, w.created_at DESC
                    LIMIT %s OFFSET %s
                    """,
                    (tenant_id, q_like, q_like, q_like, limit, offset),
                )
            else:
                cur.execute(
                    """
                    SELECT *
                    FROM works w
                    WHERE w.tenant_id=%s
                    """ + draft_filter + """
                    ORDER BY w.updated_at DESC NULLS LAST, w.created_at DESC
                    LIMIT %s OFFSET %s
                    """,
                    (tenant_id, limit, offset),
                )

            rows = cur.fetchall() or []
            items = [_work_row_to_list_item(r) for r in rows]

            cur.execute("SELECT COUNT(*) AS n FROM works w WHERE w.tenant_id=%s" + draft_filter, (tenant_id,))
            total = int((cur.fetchone() or {}).get("n") or 0)

            return {"ok": True, "tenant_slug": tenant_slug, "total": total, "limit": limit, "offset": offset, "items": items}


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
                JOIN works w ON w.id=e.work_id
                WHERE e.tenant_id=%s AND e.isbn13=%s
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
    """Return existing work id if body matches an existing work by id or uid; else None."""
    body_id = body.get("id")
    if body_id is not None and str(body_id).strip():
        cur.execute(
            "SELECT id FROM works WHERE tenant_id=%s AND id=%s LIMIT 1",
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
                "SELECT id FROM works WHERE tenant_id=%s AND uid=%s LIMIT 1",
                (tenant_id, u),
            )
            row = cur.fetchone()
            if row:
                return str(row["id"])
    return None


def _resolve_work_id_by_title_author(cur, tenant_id: str, title: str, author: str) -> Optional[str]:
    title = (title or "").strip()
    author = (author or "").strip()
    if not title and not author:
        return None
    cur.execute(
        """
        SELECT id FROM works
        WHERE tenant_id=%s
          AND trim(lower(title)) = trim(lower(%s))
          AND trim(lower(internal_extras->>'author')) = trim(lower(%s))
        LIMIT 1
        """,
        (tenant_id, title, author),
    )
    row = cur.fetchone()
    return str(row["id"]) if row else None


def _get_or_create_party(cur, conn, tenant_id: str, display_name: str) -> str:
    display_name = (display_name or "").strip() or "Unknown"
    cur.execute(
        "SELECT id FROM parties WHERE tenant_id=%s AND display_name=%s LIMIT 1",
        (tenant_id, display_name),
    )
    row = cur.fetchone()
    if row:
        return str(row["id"])
    party_id = uuid.uuid4()
    cur.execute(
        """
        INSERT INTO parties (id, tenant_id, party_type, display_name, names_before_key, key_names, person_name_inverted, corporate_name)
        VALUES (%s, %s, 'person', %s, '', '', '', '')
        """,
        (party_id, tenant_id, display_name),
    )
    conn.commit()
    return str(party_id)


def _upsert_work_from_payload(conn, cur, tenant_id: str, body: Dict[str, Any]) -> str:
    """Upsert work + editions + parties + contributors + royalties + list items + party extras. Returns work_id."""
    work_id = _resolve_work_id(cur, tenant_id, body)
    payload = dict(body)
    uid_val = payload.get("uid")
    try:
        uid_uuid = uuid.UUID(str(uid_val)) if uid_val else uuid.uuid4()
    except (ValueError, TypeError):
        uid_uuid = uuid.uuid4()

    title = (payload.get("title") or "").strip() or ""
    subtitle = (payload.get("subtitle") or "").strip() or ""
    series = (payload.get("series") or payload.get("series_title") or "").strip()
    series_num = int(payload.get("volume_number") or payload.get("series_number") or 0)
    publisher_or_imprint = (payload.get("publisher_or_imprint") or "").strip()
    pub_year = payload.get("publishing_year")
    if pub_year is not None and pub_year != "":
        try:
            pub_year = int(pub_year)
        except (TypeError, ValueError):
            pub_year = None
    language = (payload.get("language") or "").strip()
    rights = (payload.get("rights") or "").strip()
    main_desc = (payload.get("main_description") or payload.get("description") or "").strip()
    cover_link = (payload.get("cover_image_link") or "").strip()
    editor_name = (payload.get("editor_name") or "").strip()
    art_director = (payload.get("art_director_name") or "").strip()
    ages = (payload.get("ages") or "").strip()
    us_grade = (payload.get("us_grade") or (payload.get("US School Grade") or "").strip())
    internal_extras = json.dumps(payload) if isinstance(payload, dict) else "{}"

    if work_id:
        cur.execute(
            """
            UPDATE works SET
              uid=%s, title=%s, subtitle=%s, series_title=%s, series_number=%s,
              publisher_or_imprint=%s, publishing_year=%s, language=%s, rights=%s,
              main_description=%s, cover_image_link=%s, editor_name=%s, art_director_name=%s,
              ages=%s, us_grade=%s, internal_extras=%s::jsonb
            WHERE tenant_id=%s AND id=%s
            """,
            (
                uid_uuid, title, subtitle, series, series_num,
                publisher_or_imprint, pub_year, language, rights,
                main_desc, cover_link, editor_name, art_director,
                ages, us_grade, internal_extras,
                tenant_id, work_id,
            ),
        )
        conn.commit()
    else:
        work_id = str(uuid.uuid4())
        cur.execute(
            """
            INSERT INTO works (
              id, tenant_id, uid, title, subtitle, series_title, series_number,
              publisher_or_imprint, publishing_year, language, rights, main_description,
              cover_image_link, editor_name, art_director_name, ages, us_grade, internal_extras
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
            """,
            (
                work_id, tenant_id, uid_uuid, title, subtitle, series, series_num,
                publisher_or_imprint, pub_year, language, rights, main_desc,
                cover_link, editor_name, art_director, ages, us_grade, internal_extras,
            ),
        )
        conn.commit()

    # Editions: replace all for this work
    cur.execute("DELETE FROM editions WHERE tenant_id=%s AND work_id=%s", (tenant_id, work_id))
    for idx, fmt in enumerate(payload.get("formats") or []):
        if not isinstance(fmt, dict):
            continue
        isbn13 = _normalize_isbn13(fmt.get("ISBN") or fmt.get("isbn") or fmt.get("isbn13"))
        source_extras = json.dumps(fmt) if isinstance(fmt, dict) else "{}"
        cur.execute(
            """
            INSERT INTO editions (tenant_id, work_id, isbn13, source_extras, status)
            VALUES (%s, %s, %s, %s::jsonb, 'planned')
            """,
            (tenant_id, work_id, isbn13 or "", source_extras),
        )
    conn.commit()

    # Parties: author and illustrator
    author_name = (payload.get("author") or "").strip() or "Unknown"
    ill = payload.get("illustrator")
    ill_name = "Unknown"
    if isinstance(ill, dict):
        ill_name = (ill.get("name") or "").strip() or "Unknown"
    elif isinstance(ill, str):
        ill_name = (ill or "").strip() or "Unknown"
    author_party_id = _get_or_create_party(cur, conn, tenant_id, author_name)
    illustrator_party_id = _get_or_create_party(cur, conn, tenant_id, ill_name)

    cur.execute("DELETE FROM work_contributors WHERE tenant_id=%s AND work_id=%s", (tenant_id, work_id))
    cur.execute(
        """
        INSERT INTO work_contributors (tenant_id, work_id, party_id, contributor_role, sequence_number)
        VALUES (%s, %s, %s, 'A01', 1), (%s, %s, %s, 'A12', 2)
        """,
        (tenant_id, work_id, author_party_id, tenant_id, work_id, illustrator_party_id),
    )
    conn.commit()

    # Work party preferences
    for party_id, prefs_key in [(author_party_id, "author"), (illustrator_party_id, "illustrator")]:
        pref_rank1 = (payload.get(f"{prefs_key}_contact_pref_rank1") or "").strip()
        pref_rank2 = (payload.get(f"{prefs_key}_contact_pref_rank2") or "").strip()
        media_times = (payload.get(f"{prefs_key}_media_best_times") or "").strip()
        media_share = bool(payload.get(f"{prefs_key}_media_press_share") or False)
        travel = (payload.get(f"{prefs_key}_us_travel_plans") or "").strip()
        travel_dates = (payload.get(f"{prefs_key}_travel_dates") or "").strip()
        cur.execute(
            """
            INSERT INTO work_party_preferences (tenant_id, work_id, party_id, contact_pref_rank1, contact_pref_rank2, media_best_times, media_press_share, us_travel_plans, travel_dates)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (work_id, party_id) DO UPDATE SET
              contact_pref_rank1=EXCLUDED.contact_pref_rank1, contact_pref_rank2=EXCLUDED.contact_pref_rank2,
              media_best_times=EXCLUDED.media_best_times, media_press_share=EXCLUDED.media_press_share,
              us_travel_plans=EXCLUDED.us_travel_plans, travel_dates=EXCLUDED.travel_dates
            """,
            (tenant_id, work_id, party_id, pref_rank1, pref_rank2, media_times, media_share, travel, travel_dates),
        )
    conn.commit()

    # Party socials
    for party_id, key in [(author_party_id, "author_socials"), (illustrator_party_id, "illustrator_socials")]:
        cur.execute("DELETE FROM party_socials WHERE tenant_id=%s AND party_id=%s", (tenant_id, party_id))
        for s in (payload.get(key) or []):
            if not isinstance(s, dict):
                continue
            platform = (s.get("platform") or "unknown").strip() or "unknown"
            url = (s.get("url") or "").strip()
            if not url:
                continue
            handle = (s.get("handle") or "").strip()
            cur.execute(
                "INSERT INTO party_socials (tenant_id, party_id, platform, url, handle) VALUES (%s, %s, %s, %s, %s) ON CONFLICT (party_id, platform, url) DO NOTHING",
                (tenant_id, party_id, platform, url, handle),
            )
    conn.commit()

    # Party published_books and media_appearances
    for party_id, books_key, media_key in [
        (author_party_id, "author_books_published", "author_media_appearances"),
        (illustrator_party_id, "illustrator_books_published", "illustrator_media_appearances"),
    ]:
        cur.execute("DELETE FROM party_published_books WHERE tenant_id=%s AND party_id=%s", (tenant_id, party_id))
        for b in (payload.get(books_key) or []):
            if not isinstance(b, dict):
                continue
            cur.execute(
                """
                INSERT INTO party_published_books (tenant_id, party_id, title, isbn, publisher, year, approx_sold, extras)
                VALUES (%s, %s, %s, %s, %s, %s, %s, '{}'::jsonb)
                """,
                (
                    tenant_id, party_id,
                    (b.get("title") or "").strip(),
                    (b.get("isbn") or "").strip(),
                    (b.get("publisher") or "").strip(),
                    (b.get("year") or "").strip(),
                    (b.get("approx_sold") or "").strip(),
                ),
            )
        cur.execute("DELETE FROM party_media_appearances WHERE tenant_id=%s AND party_id=%s", (tenant_id, party_id))
        for m in (payload.get(media_key) or []):
            if not isinstance(m, dict):
                continue
            cur.execute(
                """
                INSERT INTO party_media_appearances (tenant_id, party_id, title, venue, appearance_date, link, extras)
                VALUES (%s, %s, %s, %s, %s, %s, '{}'::jsonb)
                """,
                (
                    tenant_id, party_id,
                    (m.get("title") or "").strip(),
                    (m.get("venue") or "").strip(),
                    (m.get("appearance_date") or m.get("date") or "").strip(),
                    (m.get("link") or "").strip(),
                ),
            )
    conn.commit()

    # Work list items: any key that is a list of objects and matches author_* / illustrator_* (category names)
    cur.execute("DELETE FROM work_list_items WHERE tenant_id=%s AND work_id=%s", (tenant_id, work_id))
    list_categories = (
        "author_marketing_endorsers", "author_sales_local_bookstores", "author_publicity_contacts",
        "illustrator_marketing_endorsers", "illustrator_sales_local_bookstores", "illustrator_publicity_contacts",
    )
    for key in list_categories:
        items = payload.get(key)
        if not isinstance(items, list) or not items:
            continue
        scope = "author" if key.startswith("author_") else "illustrator"
        party_id = author_party_id if scope == "author" else illustrator_party_id
        for idx, it in enumerate(items):
            if not isinstance(it, dict):
                continue
            cur.execute(
                """
                INSERT INTO work_list_items (tenant_id, work_id, party_id, scope, category, item_order, name, outlet, company, relationship, kind, chain_name, area, city, state, url, contact, position, phone, email, connection, personal_contact, notes, extras)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
                """,
                (
                    tenant_id, work_id, party_id, scope, key, idx + 1,
                    (it.get("name") or "").strip(),
                    (it.get("outlet") or "").strip(),
                    (it.get("company") or "").strip(),
                    (it.get("relationship") or "").strip(),
                    (it.get("kind") or "").strip(),
                    (it.get("chain_name") or "").strip(),
                    (it.get("area") or "").strip(),
                    (it.get("city") or "").strip(),
                    (it.get("state") or "").strip(),
                    (it.get("url") or "").strip(),
                    (it.get("contact") or "").strip(),
                    (it.get("position") or "").strip(),
                    (it.get("phone") or "").strip(),
                    (it.get("email") or "").strip(),
                    (it.get("connection") or "").strip(),
                    bool(it.get("personal_contact") or False),
                    (it.get("notes") or "").strip(),
                    json.dumps(it),
                ),
            )
    conn.commit()

    # Royalties: one set per work, source_json = payload.royalties; then rules + tiers + conditions
    cur.execute("DELETE FROM royalty_sets WHERE tenant_id=%s AND work_id=%s", (tenant_id, work_id))
    royalties = payload.get("royalties") or {}
    if not isinstance(royalties, dict):
        royalties = {}
    author_roy = royalties.get("author") or {}
    illustrator_roy = royalties.get("illustrator") or {}
    if not isinstance(author_roy, dict):
        author_roy = {}
    if not isinstance(illustrator_roy, dict):
        illustrator_roy = {}
    source_json = json.dumps(royalties)
    set_id = uuid.uuid4()
    cur.execute(
        "INSERT INTO royalty_sets (id, tenant_id, work_id, version, is_active, source_json) VALUES (%s, %s, %s, 1, true, %s::jsonb)",
        (set_id, tenant_id, work_id, source_json),
    )
    conn.commit()

    def _insert_royalty_rule(party: str, rights_type: str, rule_obj: Dict[str, Any]) -> None:
        mode = (rule_obj.get("mode") or "tiered").lower()
        if mode not in ("fixed", "tiered"):
            mode = "tiered"
        base = (rule_obj.get("base") or "list_price").lower().replace(" ", "_")
        if base not in ("list_price", "net_receipts"):
            base = "list_price"
        format_label = (rule_obj.get("format") or rule_obj.get("format_label") or "").strip()
        subrights_name = (rule_obj.get("name") or rule_obj.get("subrights_name") or "").strip()
        escalating = bool(rule_obj.get("escalating") or False)
        flat_rate = rule_obj.get("flat_rate_percent")
        if flat_rate is not None:
            try:
                flat_rate = float(flat_rate)
            except (TypeError, ValueError):
                flat_rate = None
        percent = rule_obj.get("percent")
        if percent is not None:
            try:
                percent = float(percent)
            except (TypeError, ValueError):
                percent = None
        notes = (rule_obj.get("note") or rule_obj.get("notes") or "").strip()
        extras = rule_obj.get("extras") or {}
        extras_json = json.dumps(extras) if isinstance(extras, dict) else "{}"
        cur.execute(
            """
            INSERT INTO royalty_rules (tenant_id, royalty_set_id, party, rights_type, format_label, subrights_name, mode, base, escalating, flat_rate_percent, percent, notes, extras)
            VALUES (%s, %s, %s::roy_party, %s::roy_rights_type, %s, %s, %s::roy_mode, %s::roy_base, %s, %s, %s, %s, %s::jsonb)
            RETURNING id
            """,
            (tenant_id, set_id, party, rights_type, format_label, subrights_name, mode, base, escalating, flat_rate, percent, notes, extras_json),
        )
        rule_row = cur.fetchone()
        if not rule_row:
            return
        rule_id = rule_row["id"]
        for t_idx, tier in enumerate(rule_obj.get("tiers") or []):
            if not isinstance(tier, dict):
                continue
            rate = float(tier.get("rate_percent") or tier.get("percent") or 0)
            tier_base = (tier.get("base") or "list_price").lower().replace(" ", "_")
            if tier_base not in ("list_price", "net_receipts"):
                tier_base = "list_price"
            tier_note = (tier.get("note") or "").strip()
            tier_extras = tier.get("extras") or {}
            cur.execute(
                """
                INSERT INTO royalty_tiers (tenant_id, rule_id, tier_order, rate_percent, base, note, extras)
                VALUES (%s, %s, %s, %s, %s::roy_base, %s, %s::jsonb)
                RETURNING id
                """,
                (tenant_id, rule_id, t_idx + 1, rate, tier_base, tier_note, json.dumps(tier_extras)),
            )
            tier_row = cur.fetchone()
            if not tier_row:
                continue
            tier_id = tier_row["id"]
            for cond in (tier.get("conditions") or []):
                if not isinstance(cond, dict):
                    continue
                kind = (cond.get("kind") or "units").lower()
                if kind not in ("units", "discount"):
                    kind = "units"
                comp = (cond.get("comparator") or "<").strip()
                if comp not in ("<", "<=", ">", ">=", "=", "!="):
                    comp = "<"
                try:
                    val = float(cond.get("value") or 0)
                except (TypeError, ValueError):
                    val = 0.0
                cur.execute(
                    """
                    INSERT INTO royalty_tier_conditions (tenant_id, tier_id, kind, comparator, value, extras)
                    VALUES (%s, %s, %s::roy_condition_kind, %s::roy_comparator, %s, '{}'::jsonb)
                    """,
                    (tenant_id, tier_id, kind, comp, val),
                )
        conn.commit()

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
            cur.execute("SELECT id FROM works WHERE tenant_id=%s AND id=%s LIMIT 1", (tenant_id, work_id))
            if not cur.fetchone():
                raise HTTPException(status_code=404, detail="Work not found")
            cur.execute("DELETE FROM works WHERE tenant_id=%s AND id=%s", (tenant_id, work_id))
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
            cur.execute("DELETE FROM works WHERE tenant_id=%s AND id=%s", (tenant_id, work_id))
            conn.commit()
            return {"ok": True, "tenant_slug": tenant_slug, "work_id": work_id, "deleted": True}
