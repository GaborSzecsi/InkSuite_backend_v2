# routers/catalog.py — Catalog API: list works, full work payload (legacy-compatible shape), resolve by ISBN.
# Read-side routing and payload assembly only. Write logic lives in catalog_write.py.

from __future__ import annotations

import ast
import re
from datetime import date, datetime
from typing import Any, Dict, List, Optional, Tuple

from fastapi import APIRouter, Body, HTTPException, Query, Request
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
from .catalog_write import (
    _upsert_work_from_payload,
    create_first_work_edition,
    update_edition_product_identity,
    update_work_titles_collections,
    update_edition_descriptive_content,
    update_edition_subjects_audience,
    update_edition_publishing_dates,
    update_edition_product_details,
    update_edition_supply_pricing,
    update_edition_rights_restrictions,
    update_edition_related_products,
    update_edition_awards,
    update_edition_cited_content,
    upsert_bookdev_task_assignment,
    update_metadata_assistant_question_state,
    add_work_contributor,
    unlink_work_contributor,
    delete_contributor_party,
)
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
    normalized_pub_date = _safe_str(row.get("normalized_publication_date"))
    normalized_year = row.get("normalized_publishing_year")
    if normalized_year is None:
        digits = re.sub(r"[^0-9]", "", normalized_pub_date)
        normalized_year = int(digits[:4]) if len(digits) >= 4 else None

    return {
        "id": str(row["id"]),
        "uid": str(row["uid"]) if row.get("uid") else str(row["id"]),
        "title": row.get("title") or "",
        "subtitle": row.get("subtitle") or "",
        "author": "",
        "series": row.get("series_title") or "",
        # Compatibility fields derived from edition_publishing_dates role 01.
        "publishing_year": normalized_year,
        "publication_date": normalized_pub_date or None,
        "publisher_name": row.get("normalized_publisher_name") or "",
        "publisher": row.get("normalized_publisher_name") or "",
        "publisher_or_imprint": (
            row.get("normalized_publisher_name")
            or row.get("imprint_name")
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
            death_date,
            birth_city,
            birth_country,
            citizenship,

            titles_before_names,
            names_before_key,
            prefix_to_key,
            key_names,
            suffix_to_key,
            letters_after_names,
            person_name_inverted,
            pen_name,
            corporate_name,
            language_code,
            country_code,
            region_code
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
        "death_date": _jsonable(r.get("death_date")),
        "birth_city": _safe_str(r.get("birth_city")),
        "birth_country": _safe_str(r.get("birth_country")),
        "citizenship": _safe_str(r.get("citizenship")),

        "titles_before_names": _safe_str(r.get("titles_before_names")),
        "names_before_key": _safe_str(r.get("names_before_key")),
        "prefix_to_key": _safe_str(r.get("prefix_to_key")),
        "key_names": _safe_str(r.get("key_names")),
        "suffix_to_key": _safe_str(r.get("suffix_to_key")),
        "letters_after_names": _safe_str(r.get("letters_after_names")),
        "person_name_inverted": _safe_str(r.get("person_name_inverted")),
        "pen_name": _safe_str(r.get("pen_name")),
        "corporate_name": _safe_str(r.get("corporate_name")),
        "language_code": _safe_str(r.get("language_code")),
        "country_code": _safe_str(r.get("country_code")),
        "region_code": _safe_str(r.get("region_code")),
    }


def _party_address_postal_column(cur) -> str:
    """Return the deployed postal column without triggering a failed query."""
    cur.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'party_addresses'
          AND column_name IN ('postal_code', 'zip')
        ORDER BY CASE WHEN column_name = 'postal_code' THEN 0 ELSE 1 END
        LIMIT 1
        """
    )
    row = cur.fetchone()
    return _safe_str(row.get("column_name")) if row else ""


def _fetch_party_address(cur, tenant_id: str, party_id: str) -> Dict[str, Any]:
    try:
        postal_column = _party_address_postal_column(cur)
        postal_select = f"{postal_column} AS postal_value" if postal_column else "'' AS postal_value"

        cur.execute(
            f"""
            SELECT street, city, state, {postal_select}, country
            FROM party_addresses
            WHERE tenant_id = %s
              AND party_id = %s
            ORDER BY
                CASE WHEN COALESCE(label, '') = 'primary' THEN 0 ELSE 1 END,
                id ASC
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
            "zip": _safe_str(r.get("postal_value")),
            "postal_code": _safe_str(r.get("postal_value")),
            "country": _safe_str(r.get("country")),
        }
    except Exception:
        return {}


def _fetch_party_address_lines(cur, tenant_id: str, party_id: str) -> List[str]:
    address = _fetch_party_address(cur, tenant_id, party_id)
    if not address:
        return []

    line1 = _safe_str(address.get("street"))
    city_state_postal = " ".join(
        x
        for x in (
            _safe_str(address.get("city")),
            _safe_str(address.get("state")),
            _safe_str(address.get("zip")),
        )
        if x
    )
    country = _safe_str(address.get("country"))
    line2 = ", ".join(x for x in (city_state_postal, country) if x)
    return [x for x in (line1, line2) if x]




def _fetch_edition_texts(
    cur,
    tenant_id: str,
    edition_id: str,
) -> List[Dict[str, Any]]:
    try:
        cur.execute(
            """
            SELECT id, text_type, text_value, source_corporate, source_title, source_title_type,
                   source_url, author, audience, content_audience, text_format,
                   language_code, item_order, created_at, updated_at
            FROM edition_texts
            WHERE tenant_id = %s AND edition_id = %s
            ORDER BY item_order ASC, created_at ASC, id ASC
            """,
            (tenant_id, edition_id),
        )
        rows = cur.fetchall() or []
    except Exception:
        return []

    result: List[Dict[str, Any]] = []

    for row in rows:
        text_id = str(row.get("id") or "")

        try:
            cur.execute(
                """
                SELECT id, content_date_role, date_format, date_text, item_order
                FROM edition_text_content_dates
                WHERE tenant_id = %s AND edition_id = %s AND edition_text_id = %s
                ORDER BY item_order ASC, created_at ASC, id ASC
                """,
                (tenant_id, edition_id, text_id),
            )
            date_rows = cur.fetchall() or []
        except Exception:
            date_rows = []

        content_dates = [
            {
                "id": str(d.get("id") or ""),
                "content_date_role": _safe_str(d.get("content_date_role")),
                "contentDateRole": _safe_str(d.get("content_date_role")),
                "date_role": _safe_str(d.get("content_date_role")),
                "dateRole": _safe_str(d.get("content_date_role")),
                "date_format": _safe_str(d.get("date_format")) or "00",
                "dateFormat": _safe_str(d.get("date_format")) or "00",
                "date_text": _safe_str(d.get("date_text")),
                "dateText": _safe_str(d.get("date_text")),
                "date_value": _safe_str(d.get("date_text")),
                "dateValue": _safe_str(d.get("date_text")),
                "date": _safe_str(d.get("date_text")),
                "item_order": int(d.get("item_order") or 0),
                "sequence_number": int(d.get("item_order") or 0),
                "sequenceNumber": int(d.get("item_order") or 0),
            }
            for d in date_rows
        ]

        result.append(
            {
                "id": text_id,
                "text_type": _safe_str(row.get("text_type")),
                "textType": _safe_str(row.get("text_type")),
                "text": _safe_str(row.get("text_value")),
                "text_value": _safe_str(row.get("text_value")),
                "textValue": _safe_str(row.get("text_value")),
                "text_content": _safe_str(row.get("text_value")),
                "textContent": _safe_str(row.get("text_value")),
                "source_corporate": _safe_str(row.get("source_corporate")),
                "sourceCorporate": _safe_str(row.get("source_corporate")),
                "source_name": _safe_str(row.get("source_corporate")),
                "sourceName": _safe_str(row.get("source_corporate")),
                "source_title": _safe_str(row.get("source_title")),
                "sourceTitle": _safe_str(row.get("source_title")),
                "source_title_type": _safe_str(row.get("source_title_type")),
                "sourceTitleType": _safe_str(row.get("source_title_type")),
                "source_url": _safe_str(row.get("source_url")),
                "sourceUrl": _safe_str(row.get("source_url")),
                "author": _safe_str(row.get("author")),
                "text_author": _safe_str(row.get("author")),
                "textAuthor": _safe_str(row.get("author")),
                "audience": _safe_str(row.get("audience")),
                "content_audience": _safe_str(row.get("content_audience")),
                "contentAudience": _safe_str(row.get("content_audience")),
                "text_format": _safe_str(row.get("text_format")) or "06",
                "textFormat": _safe_str(row.get("text_format")) or "06",
                "language_code": _safe_str(row.get("language_code")),
                "languageCode": _safe_str(row.get("language_code")),
                "content_dates": content_dates,
                "contentDates": content_dates,
                "item_order": int(row.get("item_order") or 0),
                "sequence_number": int(row.get("item_order") or 0),
                "sequenceNumber": int(row.get("item_order") or 0),
            }
        )

    return result

def _row_value(
    row: Dict[str, Any],
    *keys: str,
) -> Any:
    for key in keys:
        if key in row and row.get(key) is not None:
            return row.get(key)
    return None


def _fetch_edition_subjects(
    cur,
    tenant_id: str,
    edition_id: str,
) -> List[Dict[str, Any]]:
    try:
        cur.execute(
            """
            SELECT
                id,
                scheme_id,
                scheme_name,
                subject_code,
                heading_text,
                region_code,
                scheme_version,
                keywords,
                is_main,
                item_order
            FROM edition_subjects
            WHERE tenant_id = %s
              AND edition_id = %s
            ORDER BY item_order ASC, created_at ASC, id ASC
            """,
            (tenant_id, edition_id),
        )
        rows = cur.fetchall() or []
    except Exception:
        return []

    result: List[Dict[str, Any]] = []

    for index, row in enumerate(rows):
        scheme_id = _safe_str(row.get("scheme_id"))
        keywords = _safe_str(row.get("keywords"))

        # The current card uses Subject Heading Text as its internal keyword
        # editor value. Expose keywords there only for scheme 20 so reload
        # renders the Keywords UI rather than the generic subject UI.
        heading_text = (
            keywords
            if scheme_id == "20"
            else _safe_str(row.get("heading_text"))
        )

        item_order = int(row.get("item_order") or index + 1)

        result.append(
            {
                "id": str(row.get("id") or ""),
                "scheme_id": scheme_id,
                "scheme_name": _safe_str(row.get("scheme_name")),
                "subject_scheme_name": _safe_str(row.get("scheme_name")),
                "subjectSchemeName": _safe_str(row.get("scheme_name")),
                "schemeName": _safe_str(row.get("scheme_name")),
                "subject_scheme_identifier": scheme_id,
                "subjectSchemeIdentifier": scheme_id,
                "schemeIdentifier": scheme_id,

                "scheme_version": _safe_str(row.get("scheme_version")),
                "subject_scheme_version": _safe_str(row.get("scheme_version")),
                "subjectSchemeVersion": _safe_str(row.get("scheme_version")),
                "schemeVersion": _safe_str(row.get("scheme_version")),

                "subject_code": _safe_str(row.get("subject_code")),
                "subjectCode": _safe_str(row.get("subject_code")),

                "heading_text": heading_text,
                "subject_heading_text": heading_text,
                "subjectHeadingText": heading_text,

                "keywords": keywords,
                "region_code": _safe_str(row.get("region_code")),

                "is_main": bool(row.get("is_main")),
                "main_subject": bool(row.get("is_main")),
                "mainSubject": bool(row.get("is_main")),

                "item_order": item_order,
                "sequence_number": item_order,
                "sequenceNumber": item_order,
            }
        )

    return result



def _fetch_edition_audience(
    cur,
    tenant_id: str,
    edition_id: str,
) -> Dict[str, Any]:
    result: Dict[str, Any] = {
        "audience_codes": [],
        "audience_ranges": [],
    }

    try:
        cur.execute(
            """
            SELECT
                id,
                onix_audience_code,
                audience_range_qualifier,
                range_precision_1,
                range_value_1,
                range_precision_2,
                range_value_2,
                complexity_scheme_identifier,
                complexity_code,
                item_order
            FROM edition_audience
            WHERE tenant_id = %s
              AND edition_id = %s
            ORDER BY item_order ASC, created_at ASC, id ASC
            """,
            (tenant_id, edition_id),
        )
        rows = cur.fetchall() or []
    except Exception:
        return result

    codes: List[Dict[str, Any]] = []
    ranges: List[Dict[str, Any]] = []

    for row in rows:
        code = _safe_str(row.get("onix_audience_code"))
        qualifier = _safe_str(row.get("audience_range_qualifier"))

        if code:
            codes.append(
                {
                    "id": str(row.get("id") or ""),
                    "onix_audience_code": code,
                    "audience_code_type": _safe_str(row.get("audience_code_type")) or "01",
                    "audienceCodeType": _safe_str(row.get("audience_code_type")) or "01",
                    "audience_code_type_name": _safe_str(row.get("audience_code_type_name")),
                    "audienceCodeTypeName": _safe_str(row.get("audience_code_type_name")),
                    "audience_code": code,
                    "audienceCode": code,
                    "code": code,
                    "item_order": int(row.get("item_order") or 0),
                }
            )

        if qualifier:
            precision1 = _safe_str(row.get("range_precision_1"))
            value1 = _safe_str(row.get("range_value_1"))
            precision2 = _safe_str(row.get("range_precision_2"))
            value2 = _safe_str(row.get("range_value_2"))

            ranges.append(
                {
                    "id": str(row.get("id") or ""),
                    "audience_range_qualifier": qualifier,
                    "audienceRangeQualifier": qualifier,
                    "qualifier": qualifier,

                    "range_precision_1": precision1,
                    "audience_range_precision": precision1,
                    "audienceRangePrecision": precision1,
                    "precision": precision1,

                    "range_value_1": value1,
                    "audience_range_value": value1,
                    "audienceRangeValue": value1,
                    "value": value1,

                    "range_precision_2": precision2,
                    "audience_range_precision_2": precision2,
                    "audienceRangePrecision2": precision2,
                    "precision2": precision2,

                    "range_value_2": value2,
                    "audience_range_value_2": value2,
                    "audienceRangeValue2": value2,
                    "value2": value2,

                    "item_order": int(row.get("item_order") or 0),
                }
            )

    result["audience_codes"] = codes
    result["audience_ranges"] = ranges
    return result





def _fetch_edition_publishing_dates(
    cur,
    tenant_id: str,
    edition_id: str,
) -> List[Dict[str, Any]]:
    try:
        cur.execute(
            """
            SELECT id, date_role, date_value, date_text, date_format, note,
                   item_order, created_at, updated_at
            FROM edition_publishing_dates
            WHERE tenant_id = %s AND edition_id = %s
            ORDER BY item_order ASC, created_at ASC, id ASC
            """,
            (tenant_id, edition_id),
        )
        rows = cur.fetchall() or []
    except Exception:
        return []

    result: List[Dict[str, Any]] = []

    for row in rows:
        lexical = _safe_str(row.get("date_text"))
        if not lexical and row.get("date_value") is not None:
            lexical = (_jsonable(row.get("date_value")) or "").replace("-", "")

        result.append(
            {
                "id": str(row.get("id") or ""),
                "date_role": _safe_str(row.get("date_role")),
                "dateRole": _safe_str(row.get("date_role")),
                "publishing_date_role": _safe_str(row.get("date_role")),
                "publishingDateRole": _safe_str(row.get("date_role")),
                "date_text": lexical,
                "dateText": lexical,
                "date_value": lexical,
                "dateValue": lexical,
                "date": lexical,
                "display_date": lexical,
                "displayDate": lexical,
                "date_format": _safe_str(row.get("date_format")) or "00",
                "dateFormat": _safe_str(row.get("date_format")) or "00",
                "note": _safe_str(row.get("note")),
                "date_note": _safe_str(row.get("note")),
                "dateNote": _safe_str(row.get("note")),
                "item_order": int(row.get("item_order") or 0),
                "sequence_number": int(row.get("item_order") or 0),
                "sequenceNumber": int(row.get("item_order") or 0),
            }
        )

    return result

def _fetch_edition_form_details(cur, tenant_id: str, edition_id: str) -> List[str]:
    try:
        cur.execute(
            """
            SELECT form_detail_code
            FROM edition_form_details
            WHERE tenant_id = %s AND edition_id = %s
            ORDER BY item_order ASC, created_at ASC, id ASC
            """,
            (tenant_id, edition_id),
        )
        return [_safe_str(r.get("form_detail_code")) for r in (cur.fetchall() or []) if _safe_str(r.get("form_detail_code"))]
    except Exception:
        return []


def _fetch_edition_content_types(cur, tenant_id: str, edition_id: str) -> List[str]:
    try:
        cur.execute(
            """
            SELECT content_type_code
            FROM edition_content_types
            WHERE tenant_id = %s AND edition_id = %s
            ORDER BY is_primary DESC, item_order ASC, created_at ASC, id ASC
            """,
            (tenant_id, edition_id),
        )
        return [_safe_str(r.get("content_type_code")) for r in (cur.fetchall() or []) if _safe_str(r.get("content_type_code"))]
    except Exception:
        return []


def _fetch_edition_measurements(cur, tenant_id: str, edition_id: str) -> List[Dict[str, Any]]:
    try:
        cur.execute(
            """
            SELECT id, measure_type, measurement, measure_unit_code, item_order
            FROM edition_measurements
            WHERE tenant_id = %s AND edition_id = %s
            ORDER BY item_order ASC, created_at ASC, id ASC
            """,
            (tenant_id, edition_id),
        )
        rows = cur.fetchall() or []
    except Exception:
        return []

    return [
        {
            "id": str(r.get("id") or ""),
            "measure_type": _safe_str(r.get("measure_type")),
            "measureType": _safe_str(r.get("measure_type")),
            "measurement_type": _safe_str(r.get("measure_type")),
            "measurementType": _safe_str(r.get("measure_type")),
            "measurement": _jsonable(r.get("measurement")),
            "measurement_value": _jsonable(r.get("measurement")),
            "measurementValue": _jsonable(r.get("measurement")),
            "value": _jsonable(r.get("measurement")),
            "measure_unit_code": _safe_str(r.get("measure_unit_code")),
            "measureUnitCode": _safe_str(r.get("measure_unit_code")),
            "unit_code": _safe_str(r.get("measure_unit_code")),
            "unitCode": _safe_str(r.get("measure_unit_code")),
            "unit": _safe_str(r.get("measure_unit_code")),
            "item_order": int(r.get("item_order") or 0),
        }
        for r in rows
    ]


def _fetch_edition_extents(cur, tenant_id: str, edition_id: str) -> List[Dict[str, Any]]:
    try:
        cur.execute(
            """
            SELECT id, extent_type, extent_value, extent_unit, item_order
            FROM edition_extents
            WHERE tenant_id = %s AND edition_id = %s
            ORDER BY item_order ASC, created_at ASC, id ASC
            """,
            (tenant_id, edition_id),
        )
        rows = cur.fetchall() or []
    except Exception:
        return []

    return [
        {
            "id": str(r.get("id") or ""),
            "extent_type": _safe_str(r.get("extent_type")),
            "extentType": _safe_str(r.get("extent_type")),
            "extent_value": _jsonable(r.get("extent_value")),
            "extentValue": _jsonable(r.get("extent_value")),
            "value": _jsonable(r.get("extent_value")),
            "extent_unit": _safe_str(r.get("extent_unit")),
            "extentUnit": _safe_str(r.get("extent_unit")),
            "unit_code": _safe_str(r.get("extent_unit")),
            "unitCode": _safe_str(r.get("extent_unit")),
            "unit": _safe_str(r.get("extent_unit")),
            "item_order": int(r.get("item_order") or 0),
        }
        for r in rows
    ]



def _fetch_edition_supply_pricing(
    cur,
    tenant_id: str,
    edition_id: str,
) -> List[Dict[str, Any]]:
    """
    Read the EXISTING edition_supply_details and
    edition_prices rows. This deliberately uses SELECT *
    so legacy prices remain visible even before optional
    ONIX extension columns are added.
    """

    try:
        cur.execute(
            """
            SELECT *
            FROM edition_supply_details
            WHERE tenant_id = %s
              AND edition_id = %s
            ORDER BY id ASC
            """,
            (tenant_id, edition_id),
        )
        supplies = cur.fetchall() or []
    except Exception:
        return []

    # Check optional supplier identifier table once.
    try:
        cur.execute(
            """
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = 'public'
              AND table_name =
                  'edition_supplier_identifiers'
            LIMIT 1
            """
        )
        has_supplier_identifier_table = (
            cur.fetchone() is not None
        )
    except Exception:
        has_supplier_identifier_table = False

    out: List[Dict[str, Any]] = []

    for supply in supplies:
        supply_id = str(
            supply.get("id") or ""
        )

        identifiers: List[
            Dict[str, Any]
        ] = []

        if has_supplier_identifier_table:
            try:
                cur.execute(
                    """
                    SELECT *
                    FROM edition_supplier_identifiers
                    WHERE tenant_id = %s
                      AND supply_detail_id = %s
                    ORDER BY id ASC
                    """,
                    (
                        tenant_id,
                        supply_id,
                    ),
                )
                identifiers = [
                    {
                        "id": str(
                            row.get("id")
                            or ""
                        ),
                        "supplier_id_type":
                            _safe_str(
                                row.get(
                                    "supplier_id_type"
                                )
                            ),
                        "supplierIdType":
                            _safe_str(
                                row.get(
                                    "supplier_id_type"
                                )
                            ),
                        "identifier_type":
                            _safe_str(
                                row.get(
                                    "supplier_id_type"
                                )
                            ),
                        "identifierType":
                            _safe_str(
                                row.get(
                                    "supplier_id_type"
                                )
                            ),
                        "supplier_id_type_name":
                            _safe_str(
                                row.get(
                                    "supplier_id_type_name"
                                )
                            ),
                        "supplierIdTypeName":
                            _safe_str(
                                row.get(
                                    "supplier_id_type_name"
                                )
                            ),
                        "id_type_name":
                            _safe_str(
                                row.get(
                                    "supplier_id_type_name"
                                )
                            ),
                        "idTypeName":
                            _safe_str(
                                row.get(
                                    "supplier_id_type_name"
                                )
                            ),
                        "identifier_type_name":
                            _safe_str(
                                row.get(
                                    "supplier_id_type_name"
                                )
                            ),
                        "identifierTypeName":
                            _safe_str(
                                row.get(
                                    "supplier_id_type_name"
                                )
                            ),
                        "id_value":
                            _safe_str(
                                row.get(
                                    "id_value"
                                )
                            ),
                        "idValue":
                            _safe_str(
                                row.get(
                                    "id_value"
                                )
                            ),
                        "identifier_value":
                            _safe_str(
                                row.get(
                                    "id_value"
                                )
                            ),
                        "identifierValue":
                            _safe_str(
                                row.get(
                                    "id_value"
                                )
                            ),
                    }
                    for row
                    in (cur.fetchall() or [])
                ]
            except Exception:
                identifiers = []

        # Existing prices: no new table, no copy.
        try:
            cur.execute(
                """
                SELECT *
                FROM edition_prices
                WHERE tenant_id = %s
                  AND supply_detail_id = %s
                ORDER BY id ASC
                """,
                (
                    tenant_id,
                    supply_id,
                ),
            )
            price_rows = (
                cur.fetchall()
                or []
            )
        except Exception:
            price_rows = []

        prices: List[
            Dict[str, Any]
        ] = []

        for row in price_rows:
            price_type = _safe_str(
                row.get("price_type_code")
                or row.get("price_type")
            )
            amount = _jsonable(
                row.get("price_amount")
            )
            currency = _safe_str(
                row.get("currency_code")
            )
            tax_code = _safe_str(
                row.get("tax_rate_code")
            )
            tax_rate_percent = _jsonable(
                row.get("tax_rate_percent")
            )
            taxable_amount = _jsonable(
                row.get("taxable_amount")
            )
            tax_amount = _jsonable(
                row.get("tax_amount")
            )
            country = _safe_str(
                row.get(
                    "territory_country_included"
                )
                or row.get(
                    "country_included"
                )
            )
            region = _safe_str(
                row.get(
                    "territory_region_included"
                )
                or row.get(
                    "region_included"
                )
            )
            status = _safe_str(
                row.get("price_status")
            )
            effective_from = (
                _jsonable(
                    row.get(
                        "price_effective_from"
                    )
                )
                or ""
            )
            effective_until = (
                _jsonable(
                    row.get(
                        "price_effective_until"
                    )
                )
                or ""
            )
            discount_code = _safe_str(
                row.get("discount_code")
            )
            minimum_order_quantity = (
                row.get(
                    "minimum_order_quantity"
                )
            )
            note = _safe_str(
                row.get("price_note")
            )

            prices.append(
                {
                    "id": str(
                        row.get("id")
                        or ""
                    ),

                    "price_type":
                        price_type,
                    "priceType":
                        price_type,
                    "type":
                        price_type,

                    "price_amount":
                        amount,
                    "priceAmount":
                        amount,
                    "amount":
                        amount,
                    "price":
                        amount,

                    "currency_code":
                        currency,
                    "currencyCode":
                        currency,
                    "currency":
                        currency,

                    "tax_type":
                        tax_code,
                    "taxType":
                        tax_code,
                    "tax_rate_code":
                        tax_code,
                    "taxRateCode":
                        tax_code,
                    "tax_rate_percent":
                        tax_rate_percent,
                    "taxRatePercent":
                        tax_rate_percent,
                    "taxable_amount":
                        taxable_amount,
                    "taxableAmount":
                        taxable_amount,
                    "tax_amount":
                        tax_amount,
                    "taxAmount":
                        tax_amount,

                    "country_code":
                        country,
                    "countryCode":
                        country,
                    "country":
                        country,

                    "territory":
                        region,
                    "region_code":
                        region,
                    "regionCode":
                        region,

                    "price_status":
                        status,
                    "priceStatus":
                        status,
                    "status":
                        status,

                    "price_effective_from":
                        effective_from,
                    "priceEffectiveFrom":
                        effective_from,

                    "price_effective_until":
                        effective_until,
                    "priceEffectiveUntil":
                        effective_until,

                    "discount_code":
                        discount_code,
                    "discountCode":
                        discount_code,

                    "minimum_order_quantity":
                        minimum_order_quantity,
                    "minimumOrderQuantity":
                        minimum_order_quantity,

                    "price_note":
                        note,
                    "priceNote":
                        note,
                    "note":
                        note,
                }
            )

        supplier_name = _safe_str(
            supply.get("supplier_name")
        )
        supplier_role = _safe_str(
            supply.get("supplier_role")
        )
        supplier_email = _safe_str(
            supply.get("supplier_email")
        )
        supplier_telephone = _safe_str(supply.get("supplier_telephone"))
        supplier_fax = _safe_str(supply.get("supplier_fax"))
        availability = _safe_str(
            supply.get(
                "product_availability"
            )
        )
        order_time_days = supply.get(
            "order_time_days"
        )
        returns_code_type = _safe_str(
            supply.get("returns_code_type")
        )
        returns_code = _safe_str(
            supply.get("returns_code")
        )
        returns_note = _safe_str(
            supply.get("returns_note")
        )
        pack_quantity = supply.get(
            "pack_quantity"
        )
        carton_quantity = supply.get(
            "carton_quantity"
        )
        stock_quantity = supply.get(
            "stock_on_hand"
        )
        expected_ship_date = (
            _jsonable(
                supply.get(
                    "expected_ship_date"
                )
            )
            or ""
        )
        supply_note = _safe_str(
            supply.get("supply_note")
        )

        item = {
            "id": supply_id,

            "supplier_name":
                supplier_name,
            "supplierName":
                supplier_name,

            "supplier_role":
                supplier_role,
            "supplierRole":
                supplier_role,

            "supplier_email": supplier_email, "supplierEmail": supplier_email,
            "supplier_telephone": supplier_telephone, "supplierTelephone": supplier_telephone,
            "supplier_fax": supplier_fax, "supplierFax": supplier_fax,

            "product_availability":
                availability,
            "productAvailability":
                availability,
            "availability_code":
                availability,
            "availabilityCode":
                availability,

            "order_time_days":
                order_time_days,
            "orderTimeDays":
                order_time_days,

            "returns_code_type":
                returns_code_type,
            "returnsCodeType":
                returns_code_type,

            "returns_code":
                returns_code,
            "returnsCode":
                returns_code,

            "returns_note":
                returns_note,
            "returnsNote":
                returns_note,

            "pack_quantity":
                pack_quantity,
            "packQuantity":
                pack_quantity,

            "carton_quantity":
                carton_quantity,
            "cartonQuantity":
                carton_quantity,

            "stock_quantity":
                stock_quantity,
            "stockQuantity":
                stock_quantity,

            "expected_ship_date":
                expected_ship_date,
            "expectedShipDate":
                expected_ship_date,

            "supplier_identifiers":
                identifiers,
            "supplierIdentifiers":
                identifiers,

            "prices":
                prices,
            "product_prices":
                prices,
            "productPrices":
                prices,

            "supply_note":
                supply_note,
            "supplyNote":
                supply_note,
            "note":
                supply_note,
        }

        item["supplier"] = {
            "supplier_name":
                supplier_name,
            "supplierName":
                supplier_name,
            "name":
                supplier_name,
            "supplier_role":
                supplier_role,
            "supplierRole":
                supplier_role,
            "email_address":
                supplier_email,
            "emailAddress":
                supplier_email,
            "email":
                supplier_email,
            "supplier_identifiers":
                identifiers,
            "supplierIdentifiers":
                identifiers,
        }

        out.append(item)

    return out




def _fetch_edition_rights_restrictions(
    cur,
    tenant_id: str,
    edition_id: str,
) -> Dict[str, Any]:
    def _split_codes(value: Any) -> List[str]:
        raw = _safe_str(value)
        if not raw:
            return []
        return [
            item
            for item in re.split(r"[\s,;]+", raw)
            if item
        ]

    out: Dict[str, Any] = {}

    try:
        cur.execute(
            """
            SELECT *
            FROM edition_rights
            WHERE tenant_id = %s
              AND edition_id = %s
            ORDER BY item_order ASC NULLS LAST, id ASC
            LIMIT 1
            """,
            (tenant_id, edition_id),
        )
        row = cur.fetchone()
    except Exception:
        row = None

    if row:
        countries_included = _split_codes(
            row.get("countries_included")
            or row.get("exclusive_rights_country")
        )
        countries_excluded = _split_codes(
            row.get("countries_excluded")
        )

        stored_regions_included = _safe_str(
            row.get("regions_included")
            or row.get("exclusive_rights_territory")
        )
        worldwide = stored_regions_included.upper() == "WORLD"

        regions_included = (
            ""
            if worldwide
            else stored_regions_included
        )
        regions_excluded = _safe_str(
            row.get("regions_excluded")
        )

        territory = {
            "worldwide": worldwide,
            "countries_included": countries_included,
            "countriesIncluded": countries_included,
            "countries_excluded": countries_excluded,
            "countriesExcluded": countries_excluded,
            "regions_included": regions_included,
            "regionsIncluded": regions_included,
            "regions_excluded": regions_excluded,
            "regionsExcluded": regions_excluded,
        }

        copyright_type = _safe_str(
            row.get("copyright_type")
        ) or "C"
        holder = _safe_str(
            row.get("copyright_holder")
        )
        notice = _safe_str(
            row.get("copyright_notice")
        )
        public_domain = bool(
            row.get("public_domain")
        )
        notes = _safe_str(
            row.get("notes")
        )

        out.update(
            {
                "sales_rights_type": _safe_str(
                    row.get("sales_rights_type")
                ),
                "salesRightsType": _safe_str(
                    row.get("sales_rights_type")
                ),
                "sales_rights_territory": territory,
                "salesRightsTerritory": territory,
                "sales_worldwide": worldwide,
                "salesWorldwide": worldwide,
                "sales_countries_included": countries_included,
                "salesCountriesIncluded": countries_included,
                "sales_countries_excluded": countries_excluded,
                "salesCountriesExcluded": countries_excluded,
                "sales_regions_included": regions_included,
                "salesRegionsIncluded": regions_included,
                "sales_regions_excluded": regions_excluded,
                "salesRegionsExcluded": regions_excluded,
                "copyright_type": copyright_type,
                "copyrightType": copyright_type,
                "copyright_owner": holder,
                "copyrightOwner": holder,
                "copyright_holder": holder,
                "copyrightHolder": holder,
                "copyright_notice": notice,
                "copyrightNotice": notice,
                "public_domain": public_domain,
                "publicDomain": public_domain,
                "rights_note": notes,
                "rightsNote": notes,
                "notes": notes,
            }
        )

    restrictions: List[Dict[str, Any]] = []
    try:
        cur.execute(
            """
            SELECT *
            FROM edition_sales_restrictions
            WHERE tenant_id = %s
              AND edition_id = %s
            ORDER BY item_order ASC, created_at ASC, id ASC
            """,
            (tenant_id, edition_id),
        )
        rows = cur.fetchall() or []
    except Exception:
        rows = []

    for row in rows:
        stored_regions_included = _safe_str(
            row.get("regions_included")
        )
        worldwide = stored_regions_included.upper() == "WORLD"

        territory = {
            "worldwide": worldwide,
            "countries_included": _split_codes(
                row.get("countries_included")
            ),
            "countries_excluded": _split_codes(
                row.get("countries_excluded")
            ),
            "regions_included": (
                ""
                if worldwide
                else stored_regions_included
            ),
            "regions_excluded": _safe_str(
                row.get("regions_excluded")
            ),
        }

        restrictions.append(
            {
                "id": str(row.get("id") or ""),
                "restriction_type": _safe_str(
                    row.get("restriction_type")
                ),
                "restrictionType": _safe_str(
                    row.get("restriction_type")
                ),
                "sales_restriction_type": _safe_str(
                    row.get("restriction_type")
                ),
                "salesRestrictionType": _safe_str(
                    row.get("restriction_type")
                ),
                "restriction_detail": _safe_str(
                    row.get("restriction_detail")
                ),
                "restrictionDetail": _safe_str(
                    row.get("restriction_detail")
                ),
                "territory": territory,
                **territory,
                "start_date": _jsonable(
                    row.get("start_date")
                ) or "",
                "startDate": _jsonable(
                    row.get("start_date")
                ) or "",
                "end_date": _jsonable(
                    row.get("end_date")
                ) or "",
                "endDate": _jsonable(
                    row.get("end_date")
                ) or "",
                "restriction_note": _safe_str(
                    row.get("note")
                ),
                "restrictionNote": _safe_str(
                    row.get("note")
                ),
                "note": _safe_str(
                    row.get("note")
                ),
            }
        )

    constraints: List[Dict[str, Any]] = []
    try:
        cur.execute(
            """
            SELECT *
            FROM edition_usage_constraints
            WHERE tenant_id = %s
              AND edition_id = %s
            ORDER BY item_order ASC, created_at ASC, id ASC
            """,
            (tenant_id, edition_id),
        )
        rows = cur.fetchall() or []
    except Exception:
        rows = []

    for row in rows:
        constraints.append(
            {
                "id": str(row.get("id") or ""),
                "usage_type": _safe_str(row.get("usage_type")),
                "usageType": _safe_str(row.get("usage_type")),
                "constraint_type": _safe_str(row.get("usage_type")),
                "constraintType": _safe_str(row.get("usage_type")),
                "usage_status": _safe_str(row.get("usage_status")),
                "usageStatus": _safe_str(row.get("usage_status")),
                "status": _safe_str(row.get("usage_status")),
                "quantity": _jsonable(row.get("quantity")),
                "unit_code": _safe_str(row.get("unit_code")),
                "unitCode": _safe_str(row.get("unit_code")),
                "usage_unit": _safe_str(row.get("unit_code")),
                "usageUnit": _safe_str(row.get("unit_code")),
                "usage_note": _safe_str(row.get("usage_note")),
                "usageNote": _safe_str(row.get("usage_note")),
                "note": _safe_str(row.get("usage_note")),
            }
        )

    out["sales_restrictions"] = restrictions
    out["salesRestrictions"] = restrictions
    out["usage_constraints"] = constraints
    out["usageConstraints"] = constraints
    out["epub_usage_constraints"] = constraints
    out["epubUsageConstraints"] = constraints

    return out





def _fetch_edition_awards(
    cur,
    tenant_id: str,
    edition_id: str,
) -> List[Dict[str, Any]]:
    try:
        cur.execute(
            """
            SELECT
                id,
                prize_name,
                prize_year,
                prize_country,
                prize_code,
                prize_jury,
                award_type,
                award_status,
                award_date,
                award_category,
                award_level,
                language_code,
                recipient_name,
                recipient_role,
                award_position,
                sequence_number,
                award_website,
                award_note
            FROM edition_prizes
            WHERE tenant_id = %s
              AND edition_id = %s
            ORDER BY sequence_number ASC NULLS LAST, id ASC
            """,
            (tenant_id, edition_id),
        )
        rows = cur.fetchall() or []
    except Exception:
        return []

    awards: List[Dict[str, Any]] = []

    for index, row in enumerate(rows):
        award_name = _safe_str(row.get("prize_name"))
        award_year = _safe_str(row.get("prize_year"))
        country_code = _safe_str(row.get("prize_country"))
        award_code = _safe_str(row.get("prize_code"))
        awarding_body = _safe_str(row.get("prize_jury"))

        award_type = _safe_str(row.get("award_type"))
        award_status = _safe_str(row.get("award_status"))
        award_date = _jsonable(row.get("award_date")) or ""
        category = _safe_str(row.get("award_category"))
        level = _safe_str(row.get("award_level"))
        language_code = _safe_str(row.get("language_code"))
        recipient_name = _safe_str(row.get("recipient_name"))
        recipient_role = _safe_str(row.get("recipient_role"))
        position = _safe_str(row.get("award_position"))

        sequence_number = _safe_str(
            row.get("sequence_number") or index + 1
        )

        website = _safe_str(row.get("award_website"))
        note = _safe_str(row.get("award_note"))

        awards.append(
            {
                "id": str(row["id"]),

                "award_name": award_name,
                "awardName": award_name,
                "prize_name": award_name,
                "prizeName": award_name,
                "name": award_name,

                "award_code": award_code,
                "awardCode": award_code,
                "prize_code": award_code,
                "prizeCode": award_code,
                "code": award_code,

                "award_type": award_type,
                "awardType": award_type,
                "type": award_type,

                "award_status": award_status,
                "awardStatus": award_status,
                "status": award_status,

                "award_year": award_year,
                "awardYear": award_year,
                "prize_year": award_year,
                "prizeYear": award_year,
                "year": award_year,

                "award_date": award_date,
                "awardDate": award_date,
                "date": award_date,

                "award_category": category,
                "awardCategory": category,
                "category": category,

                "award_level": level,
                "awardLevel": level,
                "level": level,

                "country_code": country_code,
                "countryCode": country_code,
                "prize_country": country_code,
                "prizeCountry": country_code,
                "country": country_code,

                "language_code": language_code,
                "languageCode": language_code,
                "language": language_code,

                "awarding_body": awarding_body,
                "awardingBody": awarding_body,
                "award_organization": awarding_body,
                "awardOrganization": awarding_body,
                "organization": awarding_body,
                "prize_jury": awarding_body,
                "prizeJury": awarding_body,

                "recipient_name": recipient_name,
                "recipientName": recipient_name,
                "recipient": recipient_name,

                "recipient_role": recipient_role,
                "recipientRole": recipient_role,

                "award_position": position,
                "awardPosition": position,
                "position": position,

                "sequence_number": sequence_number,
                "sequenceNumber": sequence_number,
                "sequence": sequence_number,

                "award_website": website,
                "awardWebsite": website,
                "website": website,
                "url": website,

                "award_note": note,
                "awardNote": note,
                "note": note,
            }
        )

    return awards

def _fetch_edition_product_contacts(cur, tenant_id: str, edition_id: str) -> List[Dict[str, Any]]:
    try:
        cur.execute("SELECT id, product_contact_role, product_contact_name, contact_name, email_address, item_order FROM edition_product_contacts WHERE tenant_id=%s AND edition_id=%s ORDER BY item_order,id",(tenant_id,edition_id))
        rows=cur.fetchall() or []
    except Exception: return []
    return [{"id":str(r.get("id") or ""),"product_contact_role":_safe_str(r.get("product_contact_role")),"productContactRole":_safe_str(r.get("product_contact_role")),"product_contact_name":_safe_str(r.get("product_contact_name")),"productContactName":_safe_str(r.get("product_contact_name")),"contact_name":_safe_str(r.get("contact_name")),"contactName":_safe_str(r.get("contact_name")),"email_address":_safe_str(r.get("email_address")),"emailAddress":_safe_str(r.get("email_address")),"item_order":int(r.get("item_order") or 0)} for r in rows]

def _fetch_edition_cited_content(cur, tenant_id: str, edition_id: str) -> List[Dict[str, Any]]:
    try:
        cur.execute("SELECT id,cited_content_type,content_audience,source_type,source_title,citation_note,citation_note_text_format,resource_link,list_name,position_on_list,item_order FROM edition_cited_content WHERE tenant_id=%s AND edition_id=%s ORDER BY item_order,id",(tenant_id,edition_id)); rows=cur.fetchall() or []
    except Exception: return []
    out=[]
    for r in rows:
        cid=r.get("id")
        try:
            cur.execute("SELECT id,content_date_role,date_format,date_text,item_order FROM edition_cited_content_dates WHERE tenant_id=%s AND edition_id=%s AND cited_content_id=%s ORDER BY item_order,id",(tenant_id,edition_id,cid)); ds=cur.fetchall() or []
        except Exception: ds=[]
        dates=[{"id":str(d.get("id") or ""),"content_date_role":_safe_str(d.get("content_date_role")),"contentDateRole":_safe_str(d.get("content_date_role")),"date_format":_safe_str(d.get("date_format")) or "00","dateFormat":_safe_str(d.get("date_format")) or "00","date_text":_safe_str(d.get("date_text")),"dateText":_safe_str(d.get("date_text")),"item_order":int(d.get("item_order") or 0)} for d in ds]
        out.append({"id":str(cid),"cited_content_type":_safe_str(r.get("cited_content_type")),"citedContentType":_safe_str(r.get("cited_content_type")),"content_audience":_safe_str(r.get("content_audience")),"contentAudience":_safe_str(r.get("content_audience")),"source_type":_safe_str(r.get("source_type")),"sourceType":_safe_str(r.get("source_type")),"source_title":_safe_str(r.get("source_title")),"sourceTitle":_safe_str(r.get("source_title")),"citation_note":_safe_str(r.get("citation_note")),"citationNote":_safe_str(r.get("citation_note")),"citation_note_text_format":_safe_str(r.get("citation_note_text_format")) or "05","citationNoteTextFormat":_safe_str(r.get("citation_note_text_format")) or "05","resource_link":_safe_str(r.get("resource_link")),"resourceLink":_safe_str(r.get("resource_link")),"list_name":_safe_str(r.get("list_name")),"listName":_safe_str(r.get("list_name")),"position_on_list":_safe_str(r.get("position_on_list")),"positionOnList":_safe_str(r.get("position_on_list")),"content_dates":dates,"contentDates":dates,"item_order":int(r.get("item_order") or 0)})
    return out

def _fetch_edition_related_works(
    cur,
    tenant_id: str,
    edition_id: str,
) -> List[Dict[str, Any]]:
    try:
        cur.execute(
            """
            SELECT id, work_relation_code, work_id_type,
                   id_type_name, id_value, note, item_order
            FROM edition_related_works
            WHERE tenant_id = %s
              AND edition_id = %s
            ORDER BY item_order ASC, created_at ASC, id ASC
            """,
            (tenant_id, edition_id),
        )
        rows = cur.fetchall() or []
    except Exception:
        return []

    return [
        {
            "id": str(row.get("id") or ""),
            "work_relation_code": _safe_str(row.get("work_relation_code")),
            "workRelationCode": _safe_str(row.get("work_relation_code")),
            "relation_code": _safe_str(row.get("work_relation_code")),
            "relationCode": _safe_str(row.get("work_relation_code")),
            "work_id_type": _safe_str(row.get("work_id_type")),
            "workIdType": _safe_str(row.get("work_id_type")),
            "identifier_type": _safe_str(row.get("work_id_type")),
            "identifierType": _safe_str(row.get("work_id_type")),
            "id_type_name": _safe_str(row.get("id_type_name")),
            "idTypeName": _safe_str(row.get("id_type_name")),
            "id_value": _safe_str(row.get("id_value")),
            "idValue": _safe_str(row.get("id_value")),
            "identifier_value": _safe_str(row.get("id_value")),
            "identifierValue": _safe_str(row.get("id_value")),
            "note": _safe_str(row.get("note")),
            "item_order": int(row.get("item_order") or 0),
        }
        for row in rows
    ]


def _fetch_edition_related_products(
    cur,
    tenant_id: str,
    edition_id: str,
) -> List[Dict[str, Any]]:
    try:
        cur.execute(
            """
            SELECT
                id,
                relation_code,
                related_isbn13,
                related_product_form,
                related_product_form_detail,
                title,
                subtitle,
                proprietary_id,
                publisher_name,
                publication_date,
                product_url,
                identifiers,
                note,
                item_order
            FROM edition_related_products
            WHERE tenant_id = %s
              AND edition_id = %s
            ORDER BY item_order ASC, id ASC
            """,
            (tenant_id, edition_id),
        )
        rows = cur.fetchall() or []
    except Exception:
        return []

    out: List[Dict[str, Any]] = []

    for row in rows:
        raw_identifiers = row.get("identifiers") or []
        if isinstance(raw_identifiers, str):
            try:
                raw_identifiers = ast.literal_eval(raw_identifiers)
            except Exception:
                raw_identifiers = []
        if not isinstance(raw_identifiers, list):
            raw_identifiers = []

        identifiers: List[Dict[str, Any]] = []
        for index, identifier in enumerate(raw_identifiers):
            if not isinstance(identifier, dict):
                continue
            id_type = _safe_str(
                identifier.get("product_id_type")
                or identifier.get("productIdType")
                or identifier.get("identifier_type")
                or identifier.get("identifierType")
                or identifier.get("type")
            )
            id_value = _safe_str(
                identifier.get("id_value")
                or identifier.get("idValue")
                or identifier.get("identifier_value")
                or identifier.get("identifierValue")
                or identifier.get("value")
            )
            identifiers.append(
                {
                    "id": f"{row['id']}-identifier-{index + 1}",
                    "product_id_type": id_type,
                    "productIdType": id_type,
                    "identifier_type": id_type,
                    "identifierType": id_type,
                    "id_value": id_value,
                    "idValue": id_value,
                    "identifier_value": id_value,
                    "identifierValue": id_value,
                    "value": id_value,
                }
            )

        isbn = _safe_str(row.get("related_isbn13"))
        proprietary_id = _safe_str(row.get("proprietary_id"))

        if isbn and not any(
            _safe_str(item.get("product_id_type")) == "15"
            for item in identifiers
        ):
            identifiers.insert(
                0,
                {
                    "id": f"{row['id']}-isbn13",
                    "product_id_type": "15",
                    "productIdType": "15",
                    "identifier_type": "15",
                    "identifierType": "15",
                    "id_value": isbn,
                    "idValue": isbn,
                    "identifier_value": isbn,
                    "identifierValue": isbn,
                    "value": isbn,
                },
            )

        if proprietary_id and not any(
            _safe_str(item.get("product_id_type")) == "01"
            for item in identifiers
        ):
            identifiers.insert(
                0,
                {
                    "id": f"{row['id']}-proprietary",
                    "product_id_type": "01",
                    "productIdType": "01",
                    "identifier_type": "01",
                    "identifierType": "01",
                    "id_value": proprietary_id,
                    "idValue": proprietary_id,
                    "identifier_value": proprietary_id,
                    "identifierValue": proprietary_id,
                    "value": proprietary_id,
                },
            )

        relation_code = _safe_str(row.get("relation_code"))
        product_form = _safe_str(row.get("related_product_form"))
        product_form_detail = _safe_str(
            row.get("related_product_form_detail")
        )
        publication_date = _jsonable(row.get("publication_date")) or ""

        out.append(
            {
                "id": str(row["id"]),
                "product_relation_code": relation_code,
                "productRelationCode": relation_code,
                "relation_code": relation_code,
                "relationCode": relation_code,
                "relationship_type": relation_code,
                "relationshipType": relation_code,

                "title": _safe_str(row.get("title")),
                "product_title": _safe_str(row.get("title")),
                "productTitle": _safe_str(row.get("title")),

                "subtitle": _safe_str(row.get("subtitle")),
                "product_subtitle": _safe_str(row.get("subtitle")),
                "productSubtitle": _safe_str(row.get("subtitle")),

                "related_product_form": product_form,
                "product_form": product_form,
                "productForm": product_form,
                "format_code": product_form,
                "formatCode": product_form,

                "related_product_form_detail": product_form_detail,
                "product_form_detail": product_form_detail,
                "productFormDetail": product_form_detail,

                "related_isbn13": isbn,
                "isbn13": isbn,
                "isbn_13": isbn,
                "isbn": isbn,

                "proprietary_id": proprietary_id,
                "proprietaryId": proprietary_id,

                "publisher_name": _safe_str(row.get("publisher_name")),
                "publisherName": _safe_str(row.get("publisher_name")),
                "publisher": _safe_str(row.get("publisher_name")),

                "publication_date": publication_date,
                "publicationDate": publication_date,

                "product_url": _safe_str(row.get("product_url")),
                "productUrl": _safe_str(row.get("product_url")),
                "url": _safe_str(row.get("product_url")),
                "link": _safe_str(row.get("product_url")),

                "product_identifiers": identifiers,
                "productIdentifiers": identifiers,
                "identifiers": identifiers,

                "relationship_note": _safe_str(row.get("note")),
                "relationshipNote": _safe_str(row.get("note")),
                "note": _safe_str(row.get("note")),

                "item_order": int(row.get("item_order") or 0),
            }
        )

    return out

def _fetch_bookdev_task_assignments(
    cur,
    tenant_id: str,
    work_id: str,
    edition_id: str,
) -> List[Dict[str, Any]]:
    try:
        cur.execute(
            """
            SELECT
                id,
                task_key,
                responsible_person,
                deadline,
                created_at,
                updated_at
            FROM bookdev_task_assignments
            WHERE tenant_id = %s
              AND work_id = %s
              AND edition_id = %s
            ORDER BY task_key ASC, id ASC
            """,
            (tenant_id, work_id, edition_id),
        )
        rows = cur.fetchall() or []
    except Exception:
        return []

    return [
        {
            "id": str(row.get("id") or ""),
            "task_key": _safe_str(row.get("task_key")),
            "taskKey": _safe_str(row.get("task_key")),
            "responsible_person": _safe_str(row.get("responsible_person")),
            "responsiblePerson": _safe_str(row.get("responsible_person")),
            "deadline": _jsonable(row.get("deadline")) or "",
            "created_at": _jsonable(row.get("created_at")) or "",
            "updated_at": _jsonable(row.get("updated_at")) or "",
        }
        for row in rows
    ]


def _fetch_edition_product_form_features(
    cur,
    tenant_id: str,
    edition_id: str,
) -> List[Dict[str, Any]]:
    try:
        cur.execute(
            """
            SELECT
                id,
                feature_type,
                feature_value,
                feature_description,
                item_order
            FROM edition_product_form_features
            WHERE tenant_id = %s
              AND edition_id = %s
            ORDER BY item_order ASC, created_at ASC, id ASC
            """,
            (tenant_id, edition_id),
        )
        rows = cur.fetchall() or []
    except Exception:
        return []

    return [
        {
            "id": str(row.get("id") or ""),
            "feature_type": _safe_str(row.get("feature_type")),
            "featureType": _safe_str(row.get("feature_type")),
            "product_form_feature_type": _safe_str(row.get("feature_type")),
            "productFormFeatureType": _safe_str(row.get("feature_type")),
            "feature_value": _safe_str(row.get("feature_value")),
            "featureValue": _safe_str(row.get("feature_value")),
            "product_form_feature_value": _safe_str(row.get("feature_value")),
            "productFormFeatureValue": _safe_str(row.get("feature_value")),
            "feature_description": _safe_str(row.get("feature_description")),
            "featureDescription": _safe_str(row.get("feature_description")),
            "product_form_feature_description": _safe_str(row.get("feature_description")),
            "productFormFeatureDescription": _safe_str(row.get("feature_description")),
            "item_order": int(row.get("item_order") or index + 1),
        }
        for index, row in enumerate(rows)
    ]


def _fetch_edition_ancillary_content(
    cur,
    tenant_id: str,
    edition_id: str,
) -> List[Dict[str, Any]]:
    try:
        cur.execute(
            """
            SELECT
                id,
                ancillary_content_type,
                description,
                description_text_format,
                number,
                item_order
            FROM edition_ancillary_content
            WHERE tenant_id = %s
              AND edition_id = %s
            ORDER BY item_order ASC, created_at ASC, id ASC
            """,
            (tenant_id, edition_id),
        )
        rows = cur.fetchall() or []
    except Exception:
        return []

    return [
        {
            "id": str(row.get("id") or ""),
            "ancillary_content_type": _safe_str(row.get("ancillary_content_type")),
            "ancillaryContentType": _safe_str(row.get("ancillary_content_type")),
            "content_type": _safe_str(row.get("ancillary_content_type")),
            "contentType": _safe_str(row.get("ancillary_content_type")),
            "description": _safe_str(row.get("description")),
            "ancillary_content_description": _safe_str(row.get("description")),
            "ancillaryContentDescription": _safe_str(row.get("description")),
            "description_text_format": _safe_str(row.get("description_text_format")) or "05",
            "descriptionTextFormat": _safe_str(row.get("description_text_format")) or "05",
            "number": row.get("number"),
            "ancillary_content_number": row.get("number"),
            "ancillaryContentNumber": row.get("number"),
            "item_order": int(row.get("item_order") or index + 1),
        }
        for index, row in enumerate(rows)
    ]


def _fetch_editions(cur, tenant_id: str, work_id: str) -> List[Dict[str, Any]]:
    try:
        cur.execute(
            """
            SELECT
                e.id,
                e.isbn13,
                e.product_form,
                e.product_form_detail,
                e.onix_product_form,
                e.onix_product_form_detail,
                e.notification_type,
                e.product_composition,
                e.primary_content_type,
                e.barcode_type,
                e.barcode_position_on_product,
                e.product_packaging,
                e.record_reference,
                e.record_source_type,
                e.publishing_status,
                e.copyright_year,
                e.market_publishing_status,
                e.market_date_role,
                e.market_date_format,
                e.market_date_text,
                e.promotion_contact,
                e.promotion_contact_text_format,
                e.initial_print_run, e.initial_print_run_text_format,
                e.promotion_campaign, e.promotion_campaign_text_format,
                e.audience_description,
                e.duration,
                e.duration_unit,
                e.file_size,
                e.file_size_unit,
                e.edition_number,
                e.edition_statement,
                e.illustrations_number,
                e.illustrations_desc,
                e.color_content,
                e.color_pages,
                e.number_of_pieces,
                e.trade_category,
                e.country_of_manufacture,
                e.product_form_description,
                e.technical_protection,
                e.epub_version,
                e.file_format,
                e.product_details_note,
                COALESCE(
                    NULLIF(BTRIM(e.cover_image_link), ''),
                    (
                        SELECT erv.resource_link
                        FROM edition_supporting_resources esr
                        JOIN edition_supporting_resource_versions erv
                          ON erv.resource_id = esr.id
                         AND erv.tenant_id = esr.tenant_id
                        WHERE esr.tenant_id = e.tenant_id
                          AND esr.edition_id = e.id
                          AND esr.resource_content_type = '01'
                          AND NULLIF(BTRIM(erv.resource_link), '') IS NOT NULL
                        ORDER BY
                            CASE WHEN esr.is_primary THEN 0 ELSE 1 END,
                            esr.item_order,
                            erv.item_order,
                            erv.created_at
                        LIMIT 1
                    )
                ) AS cover_image_link,
                e.cover_image_format,
                e.cover_image_caption,
                e.created_at,
                e.updated_at,
                (
                    SELECT p.publisher_name
                    FROM edition_publishers p
                    WHERE p.tenant_id = e.tenant_id
                      AND p.edition_id = e.id
                      AND p.publishing_role = '01'
                    ORDER BY p.item_order ASC, p.created_at ASC, p.id ASC
                    LIMIT 1
                ) AS publisher_name,
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
        return []

    out: List[Dict[str, Any]] = []
    for r in rows:
        format_label = r.get("product_form_detail") or r.get("product_form") or ""

        identifiers: List[Dict[str, Any]] = []
        try:
            cur.execute(
                """
                SELECT
                    id,
                    id_type,
                    id_type_name,
                    id_value,
                    item_order
                FROM edition_identifiers
                WHERE tenant_id = %s
                  AND edition_id = %s
                  AND id_type <> '15'
                ORDER BY item_order ASC, id ASC
                """,
                (tenant_id, r["id"]),
            )
            identifiers = [
                {
                    "id": str(row["id"]),
                    "id_type": _safe_str(row.get("id_type")),
                    "id_type_name": _safe_str(row.get("id_type_name")),
                    "id_value": _safe_str(row.get("id_value")),
                    "identifier_type": _safe_str(row.get("id_type")),
                    "identifier_type_name": _safe_str(row.get("id_type_name")),
                    "identifier_value": _safe_str(row.get("id_value")),
                    "value": _safe_str(row.get("id_value")),
                    "item_order": int(row.get("item_order") or 0),
                }
                for row in (cur.fetchall() or [])
            ]
        except Exception:
            identifiers = []

        descriptive_texts = _fetch_edition_texts(
            cur,
            tenant_id,
            str(r["id"]),
        )

        subjects = _fetch_edition_subjects(
            cur,
            tenant_id,
            str(r["id"]),
        )

        audience = _fetch_edition_audience(
            cur,
            tenant_id,
            str(r["id"]),
        )

        publishing_dates = _fetch_edition_publishing_dates(
            cur,
            tenant_id,
            str(r["id"]),
        )

        # Canonical publication date is ONIX PublishingDateRole 01.
        primary_publication_date = next(
            (
                _safe_str(row.get("date_text") or row.get("date_value"))
                for row in publishing_dates
                if _safe_str(row.get("date_role")) == "01"
                and _safe_str(row.get("date_text") or row.get("date_value"))
            ),
            "",
        )

        form_details = _fetch_edition_form_details(cur, tenant_id, str(r["id"]))
        content_types = _fetch_edition_content_types(cur, tenant_id, str(r["id"]))
        measurements = _fetch_edition_measurements(cur, tenant_id, str(r["id"]))
        extents = _fetch_edition_extents(cur, tenant_id, str(r["id"]))

        # Legacy-compatible display fields are derived from normalized ONIX data.
        # MeasureType: 01 height, 02 width, 03 thickness, 08 unit weight.
        measurement_by_type = {
            _safe_str(row.get("measure_type")): row
            for row in measurements
            if _safe_str(row.get("measure_type"))
        }
        height_measure = measurement_by_type.get("01", {})
        width_measure = measurement_by_type.get("02", {})
        thickness_measure = measurement_by_type.get("03", {})
        weight_measure = measurement_by_type.get("08", {})

        page_extent = next(
            (
                row for row in extents
                if _safe_str(row.get("extent_type")) == "00"
                and _safe_str(row.get("extent_unit")) == "03"
            ),
            {},
        )
        normalized_pages = int(float(page_extent.get("extent_value") or 0))

        def _measurement_value(row: Dict[str, Any]) -> float:
            try:
                return float(row.get("measurement") or 0)
            except (TypeError, ValueError):
                return 0
        product_form_features = _fetch_edition_product_form_features(
            cur, tenant_id, str(r["id"])
        )
        ancillary_content = _fetch_edition_ancillary_content(
            cur, tenant_id, str(r["id"])
        )

        supply_details = _fetch_edition_supply_pricing(
            cur,
            tenant_id,
            str(r["id"]),
        )

        rights_restrictions = _fetch_edition_rights_restrictions(
            cur,
            tenant_id,
            str(r["id"]),
        )

        product_contacts = _fetch_edition_product_contacts(cur, tenant_id, str(r["id"]))
        cited_content = _fetch_edition_cited_content(cur, tenant_id, str(r["id"]))

        related_works = _fetch_edition_related_works(
            cur,
            tenant_id,
            str(r["id"]),
        )

        related_products = _fetch_edition_related_products(
            cur,
            tenant_id,
            str(r["id"]),
        )

        awards = _fetch_edition_awards(
            cur,
            tenant_id,
            str(r["id"]),
        )

        task_assignments = _fetch_bookdev_task_assignments(
            cur,
            tenant_id,
            work_id,
            str(r["id"]),
        )

        out.append(
            {
                "id": str(r["id"]),
                "edition_id": str(r["id"]),
                "isbn": r.get("isbn13") or "",
                "isbn13": r.get("isbn13") or "",
                "format": format_label,
                "format_label": format_label,
                "product_form": r.get("product_form") or "",
                "product_form_detail": r.get("product_form_detail") or "",
                "onix_product_form": r.get("onix_product_form") or "",
                "onix_product_form_detail": r.get("onix_product_form_detail") or "",
                "notification_type": r.get("notification_type") or "",
                "product_composition": r.get("product_composition") or "",
                "primary_content_type": r.get("primary_content_type") or "",
                "barcode_type": r.get("barcode_type") or "",
                "barcode_position_on_product": r.get("barcode_position_on_product") or "",
                "barcodePositionOnProduct": r.get("barcode_position_on_product") or "",
                "position_on_product": r.get("barcode_position_on_product") or "",
                "positionOnProduct": r.get("barcode_position_on_product") or "",
                "product_packaging": r.get("product_packaging") or "",
                "productPackaging": r.get("product_packaging") or "",
                "record_reference": r.get("record_reference") or "",
                "recordReference": r.get("record_reference") or "",
                "record_source_type": r.get("record_source_type") or "",
                "recordSourceType": r.get("record_source_type") or "",
                "publishing_status": r.get("publishing_status") or "",
                "publishingStatus": r.get("publishing_status") or "",

                "copyright_year": (
                    int(r.get("copyright_year"))
                    if r.get("copyright_year") is not None
                    else None
                ),
                "copyrightYear": (
                    int(r.get("copyright_year"))
                    if r.get("copyright_year") is not None
                    else None
                ),

                "market_publishing_status":
                    r.get("market_publishing_status") or "",
                "marketPublishingStatus":
                    r.get("market_publishing_status") or "",
                "market_date_role":
                    r.get("market_date_role") or "",
                "marketDateRole":
                    r.get("market_date_role") or "",
                "market_date_format":
                    r.get("market_date_format") or "00",
                "marketDateFormat":
                    r.get("market_date_format") or "00",
                "market_date":
                    r.get("market_date_text") or "",
                "marketDate":
                    r.get("market_date_text") or "",
                "market_date_text":
                    r.get("market_date_text") or "",
                "marketDateText":
                    r.get("market_date_text") or "",
                "promotion_contact":
                    r.get("promotion_contact") or "",
                "promotionContact":
                    r.get("promotion_contact") or "",
                "promotion_contact_text_format":
                    r.get("promotion_contact_text_format") or "05",
                "promotionContactTextFormat":
                    r.get("promotion_contact_text_format") or "05",
                "initial_print_run": r.get("initial_print_run") or "", "initialPrintRun": r.get("initial_print_run") or "",
                "initial_print_run_text_format": r.get("initial_print_run_text_format") or "05", "initialPrintRunTextFormat": r.get("initial_print_run_text_format") or "05",
                "promotion_campaign": r.get("promotion_campaign") or "", "promotionCampaign": r.get("promotion_campaign") or "",
                "promotion_campaign_text_format": r.get("promotion_campaign_text_format") or "05", "promotionCampaignTextFormat": r.get("promotion_campaign_text_format") or "05",
                "product_contacts": product_contacts, "productContacts": product_contacts,

                "audience_description": (
                    r.get("audience_description")
                    or r.get("target_audience")
                    or ""
                ),
                "audienceDescription": (
                    r.get("audience_description")
                    or r.get("target_audience")
                    or ""
                ),
                "notificationType": r.get("notification_type") or "",
                "productComposition": r.get("product_composition") or "",
                "productForm": r.get("onix_product_form") or "",
                "productFormCode": r.get("onix_product_form") or "",
                "productFormDetail": r.get("onix_product_form_detail") or "",
                "productFormDetailCode": r.get("onix_product_form_detail") or "",
                "productContentType": r.get("primary_content_type") or "",
                "barcodeType": r.get("barcode_type") or "",
                "barcodePositionOnProduct": r.get("barcode_position_on_product") or "",
                "productPackaging": r.get("product_packaging") or "",
                "recordReference": r.get("record_reference") or "",
                "recordSourceType": r.get("record_source_type") or "",

                # Canonical Publisher for this edition comes from
                # edition_publishers (PublishingRole 01), selected above.
                "publisher_name": r.get("publisher_name") or "",
                "publisherName": r.get("publisher_name") or "",
                "publisher": r.get("publisher_name") or "",

                "product_identifiers": identifiers,
                "productIdentifiers": identifiers,

                "descriptive_texts": descriptive_texts,
                "descriptiveTexts": descriptive_texts,
                "text_contents": descriptive_texts,
                "textContents": descriptive_texts,
                "descriptions": descriptive_texts,

                "subjects": subjects,
                "book_subjects": subjects,
                "bookSubjects": subjects,
                "onix_subjects": subjects,
                "onixSubjects": subjects,

                "audience_codes": audience.get("audience_codes") or [],
                "audienceCodes": audience.get("audience_codes") or [],
                "audiences": audience.get("audience_codes") or [],

                "audience_ranges": audience.get("audience_ranges") or [],
                "audienceRanges": audience.get("audience_ranges") or [],

                "publishing_dates": publishing_dates,
                "publishingDates": publishing_dates,
                "publication_dates": publishing_dates,
                "publicationDates": publishing_dates,

                "short_description": next(
                    (
                        _safe_str(row.get("text"))
                        for row in descriptive_texts
                        if _safe_str(row.get("text_type")) == "02"
                        and _safe_str(row.get("text"))
                    ),
                    "",
                ),
                "long_description": next(
                    (
                        _safe_str(row.get("text"))
                        for row in descriptive_texts
                        if _safe_str(row.get("text_type")) == "03"
                        and _safe_str(row.get("text"))
                    ),
                    "",
                ),
                "table_of_contents": next(
                    (
                        _safe_str(row.get("text"))
                        for row in descriptive_texts
                        if _safe_str(row.get("text_type")) == "04"
                        and _safe_str(row.get("text"))
                    ),
                    "",
                ),
                "promotional_headline": next(
                    (
                        _safe_str(row.get("text"))
                        for row in descriptive_texts
                        if _safe_str(row.get("text_type")) == "10"
                        and _safe_str(row.get("text"))
                    ),
                    "",
                ),
                "excerpt": next(
                    (
                        _safe_str(row.get("text"))
                        for row in descriptive_texts
                        if _safe_str(row.get("text_type")) == "14"
                        and _safe_str(row.get("text"))
                    ),
                    "",
                ),

                "pub_date": primary_publication_date,
                "publication_date": primary_publication_date,
                "price_us": float(r["price_us"]) if r.get("price_us") is not None else 0,
                "price_can": float(r["price_can"]) if r.get("price_can") is not None else 0,
                "pages": normalized_pages,
                "number_of_pages": normalized_pages,

                "tall": _measurement_value(height_measure),
                "height": _measurement_value(height_measure),
                "height_unit": _safe_str(height_measure.get("measure_unit_code")),

                "wide": _measurement_value(width_measure),
                "width": _measurement_value(width_measure),
                "width_unit": _safe_str(width_measure.get("measure_unit_code")),

                "spine": _measurement_value(thickness_measure),
                "thickness": _measurement_value(thickness_measure),
                "thickness_unit": _safe_str(thickness_measure.get("measure_unit_code")),

                "weight": _measurement_value(weight_measure),
                "unit_weight": _measurement_value(weight_measure),
                "unit_weight_unit": _safe_str(weight_measure.get("measure_unit_code")),

                "duration": _jsonable(r.get("duration")),
                "duration_unit": r.get("duration_unit") or "",
                "file_size": _jsonable(r.get("file_size")),
                "file_size_unit": r.get("file_size_unit") or "",

                "edition_number": r.get("edition_number") or "",
                "editionNumber": r.get("edition_number") or "",
                "edition_statement": r.get("edition_statement") or "",
                "editionStatement": r.get("edition_statement") or "",

                "illustrations_number": r.get("illustrations_number"),
                "illustration_count": r.get("illustrations_number"),
                "illustrationCount": r.get("illustrations_number"),
                "illustrations_desc": r.get("illustrations_desc") or "",
                "illustration_note": r.get("illustrations_desc") or "",
                "illustrationNote": r.get("illustrations_desc") or "",

                "color_content": r.get("color_content") or "",
                "colorContent": r.get("color_content") or "",
                "color_pages": r.get("color_pages"),
                "colorPages": r.get("color_pages"),

                "number_of_pieces": r.get("number_of_pieces"),
                "numberOfPieces": r.get("number_of_pieces"),
                "trade_category": r.get("trade_category") or "",
                "tradeCategory": r.get("trade_category") or "",
                "country_of_manufacture": r.get("country_of_manufacture") or "",
                "countryOfManufacture": r.get("country_of_manufacture") or "",
                "product_form_description": r.get("product_form_description") or "",
                "productFormDescription": r.get("product_form_description") or "",

                "technical_protection": r.get("technical_protection") or "",
                "technicalProtection": r.get("technical_protection") or "",
                "epub_version": r.get("epub_version") or "",
                "epubVersion": r.get("epub_version") or "",
                "file_format": r.get("file_format") or "",
                "fileFormat": r.get("file_format") or "",

                "product_details_note": r.get("product_details_note") or "",
                "productDetailsNote": r.get("product_details_note") or "",

                "product_form_details": form_details,
                "productFormDetails": form_details,
                "form_details": form_details,
                "formDetails": form_details,

                "product_content_types": content_types,
                "productContentTypes": content_types,
                "content_types": content_types,
                "contentTypes": content_types,

                "measurements": measurements,
                "product_measurements": measurements,
                "productMeasurements": measurements,

                "extents": extents,
                "product_extents": extents,
                "productExtents": extents,

                "product_form_features": product_form_features,
                "productFormFeatures": product_form_features,

                "ancillary_content": ancillary_content,
                "ancillaryContent": ancillary_content,

                "supply_details": supply_details,
                "supplyDetails": supply_details,
                "supplies": supply_details,
                "prices": [
                    price
                    for supply in supply_details
                    for price in (supply.get("prices") or [])
                ],
                "product_prices": [
                    price
                    for supply in supply_details
                    for price in (supply.get("prices") or [])
                ],
                "productPrices": [
                    price
                    for supply in supply_details
                    for price in (supply.get("prices") or [])
                ],

                "cited_content": cited_content,
                "citedContent": cited_content,

                "related_works": related_works,
                "relatedWorks": related_works,

                "related_products": related_products,
                "relatedProducts": related_products,
                "related_items": related_products,
                "relatedItems": related_products,

                "awards": awards,
                "prizes": awards,
                "award_records": awards,
                "awardRecords": awards,

                "task_assignments": task_assignments,
                "taskAssignments": task_assignments,
                "workflow_assignments": task_assignments,
                "workflowAssignments": task_assignments,

                **rights_restrictions,

                "created_at": _jsonable(r.get("created_at")),
                "updated_at": _jsonable(r.get("updated_at")),
            }
        )
    return out



def _fetch_work_titles(cur, tenant_id: str, work_id: str) -> Dict[str, Any]:
    result: Dict[str, Any] = {
        "title_prefix": "",
        "title_without_prefix": "",
        "title_element_level": "01",
        "no_prefix": False,
        "title_part_number": "",
        "alternative_titles": [],
    }

    try:
        cur.execute(
            """
            SELECT id, title_type, title_element_level, title_prefix,
                   title_without_prefix, subtitle, part_number, no_prefix,
                   year_of_annual, language_code, is_primary, item_order
            FROM work_titles
            WHERE tenant_id = %s AND work_id = %s
            ORDER BY is_primary DESC, item_order ASC, created_at ASC, id ASC
            """,
            (tenant_id, work_id),
        )
        rows = cur.fetchall() or []
    except Exception:
        return result

    primary = next((row for row in rows if bool(row.get("is_primary"))), None)
    if primary:
        result["title_prefix"] = _safe_str(primary.get("title_prefix"))
        result["title_without_prefix"] = _safe_str(primary.get("title_without_prefix"))
        result["title_element_level"] = _safe_str(primary.get("title_element_level")) or "01"
        result["titleElementLevel"] = result["title_element_level"]
        result["no_prefix"] = bool(primary.get("no_prefix"))
        result["noPrefix"] = result["no_prefix"]
        result["title_part_number"] = _safe_str(primary.get("part_number"))
        result["titlePartNumber"] = result["title_part_number"]

    alternatives: List[Dict[str, Any]] = []
    for row in rows:
        if bool(row.get("is_primary")):
            continue

        prefix = _safe_str(row.get("title_prefix"))
        body = _safe_str(row.get("title_without_prefix"))
        title_text = " ".join([part for part in [prefix, body] if part]).strip()

        alternatives.append(
            {
                "id": str(row["id"]),
                "title_type": _safe_str(row.get("title_type")),
                "titleType": _safe_str(row.get("title_type")),
                "title_element_level": _safe_str(row.get("title_element_level")) or "01",
                "titleElementLevel": _safe_str(row.get("title_element_level")) or "01",
                "title_prefix": prefix,
                "titlePrefix": prefix,
                "title_without_prefix": body,
                "titleWithoutPrefix": body,
                "no_prefix": bool(row.get("no_prefix")),
                "noPrefix": bool(row.get("no_prefix")),
                "part_number": _safe_str(row.get("part_number")),
                "partNumber": _safe_str(row.get("part_number")),
                "title": title_text,
                "subtitle": _safe_str(row.get("subtitle")),
                "language_code": _safe_str(row.get("language_code")),
                "languageCode": _safe_str(row.get("language_code")),
                "item_order": int(row.get("item_order") or 0),
            }
        )

    result["alternative_titles"] = alternatives
    result["alternativeTitles"] = alternatives
    return result


def _fetch_work_collections(cur, tenant_id: str, work_id: str) -> List[Dict[str, Any]]:
    try:
        cur.execute(
            """
            SELECT id, parent_collection_id, collection_type, title_type,
                   title_element_level, no_prefix, collection_title,
                   collection_subtitle, collection_number, volume_number,
                   part_number, sequence_type, sequence_number, is_primary,
                   item_order
            FROM work_collections
            WHERE tenant_id = %s AND work_id = %s
            ORDER BY is_primary DESC, item_order ASC, created_at ASC, id ASC
            """,
            (tenant_id, work_id),
        )
        rows = cur.fetchall() or []
    except Exception:
        return []

    return [
        {
            "id": str(row["id"]),
            "parent_collection_id": str(row["parent_collection_id"]) if row.get("parent_collection_id") else None,
            "collection_type": _safe_str(row.get("collection_type")),
            "collectionType": _safe_str(row.get("collection_type")),
            "title_type": _safe_str(row.get("title_type")) or "01",
            "titleType": _safe_str(row.get("title_type")) or "01",
            "title_element_level": _safe_str(row.get("title_element_level")) or "02",
            "titleElementLevel": _safe_str(row.get("title_element_level")) or "02",
            "no_prefix": bool(row.get("no_prefix")),
            "noPrefix": bool(row.get("no_prefix")),
            "title": _safe_str(row.get("collection_title")),
            "collection_title": _safe_str(row.get("collection_title")),
            "collectionTitle": _safe_str(row.get("collection_title")),
            "subtitle": _safe_str(row.get("collection_subtitle")),
            "collection_subtitle": _safe_str(row.get("collection_subtitle")),
            "collectionSubtitle": _safe_str(row.get("collection_subtitle")),
            "collection_number": _safe_str(row.get("collection_number")),
            "collectionNumber": _safe_str(row.get("collection_number")),
            "volume_number": _safe_str(row.get("volume_number")),
            "volumeNumber": _safe_str(row.get("volume_number")),
            "part_number": _safe_str(row.get("part_number")),
            "partNumber": _safe_str(row.get("part_number")),
            "sequence_type": _safe_str(row.get("sequence_type")),
            "sequence_number": _safe_str(row.get("sequence_number")),
            "is_primary": bool(row.get("is_primary")),
            "item_order": int(row.get("item_order") or 0),
        }
        for row in rows
    ]

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
        item = {
            "company_or_outlet": _safe_str(r.get("company_or_outlet")),
            "name": _safe_str(r.get("name")),
            "position": _safe_str(r.get("position")),
            "phone": _safe_str(r.get("phone")),
            "email": _safe_str(r.get("email")),
            "website": _safe_str(r.get("website")),
            "street": _safe_str(r.get("street")),
            "city": _safe_str(r.get("city")),
            "state": _safe_str(r.get("state")),
            "zip": _safe_str(r.get("zip")),
            "country": _safe_str(r.get("country")),
            "relationship_note": _safe_str(r.get("relationship_note")),
            "personal_contact": bool(r.get("personal_contact") or False),
            "notes": _safe_str(r.get("notes")),
            "social_handle": _safe_str(r.get("social_handle")),
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
    
    # Awards may live in one of several schema-compatible tables. Detect the
    # actual table and columns before querying so a missing table does not abort
    # the surrounding PostgreSQL transaction.
    awards: List[Dict[str, Any]] = []
    try:
        award_table = ""
        award_columns: set[str] = set()

        for candidate in ("party_awards", "party_honors", "contributor_awards"):
            cur.execute(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = 'public'
                  AND table_name = %s
                """,
                (candidate,),
            )
            candidate_columns = {
                str(row["column_name"]) for row in (cur.fetchall() or [])
            }
            if {"tenant_id", "party_id"}.issubset(candidate_columns):
                award_table = candidate
                award_columns = candidate_columns
                break

        if award_table:
            name_column = next(
                (
                    column
                    for column in ("award_name", "honor_name", "name", "title")
                    if column in award_columns
                ),
                "",
            )
            year_column = next(
                (
                    column
                    for column in ("award_year", "year_text", "year", "date_text")
                    if column in award_columns
                ),
                "",
            )
            result_column = next(
                (
                    column
                    for column in ("award_result", "result", "status")
                    if column in award_columns
                ),
                "",
            )
            organization_column = next(
                (
                    column
                    for column in ("organization", "awarding_body")
                    if column in award_columns
                ),
                "",
            )
            notes_column = "notes" if "notes" in award_columns else ""
            order_column = next(
                (
                    column
                    for column in ("item_order", "sequence_number")
                    if column in award_columns
                ),
                "",
            )

            if name_column:
                selected_columns = ["id"] if "id" in award_columns else []
                for column in (
                    name_column,
                    year_column,
                    result_column,
                    organization_column,
                    notes_column,
                    order_column,
                ):
                    if column and column not in selected_columns:
                        selected_columns.append(column)

                order_parts = []
                if order_column:
                    order_parts.append(f"{order_column} ASC")
                if "id" in award_columns:
                    order_parts.append("id ASC")
                order_sql = ", ".join(order_parts) or f"{name_column} ASC"

                cur.execute(
                    f"""
                    SELECT {", ".join(selected_columns)}
                    FROM {award_table}
                    WHERE tenant_id = %s
                      AND party_id = %s
                    ORDER BY {order_sql}
                    """,
                    (tenant_id, party_id),
                )

                for row in (cur.fetchall() or []):
                    awards.append(
                        {
                            "id": str(row.get("id")) if row.get("id") else "",
                            "name": _safe_str(row.get(name_column)),
                            "award": _safe_str(row.get(name_column)),
                            "honor": _safe_str(row.get(name_column)),
                            "year": _safe_str(row.get(year_column)) if year_column else "",
                            "result": _safe_str(row.get(result_column)) if result_column else "",
                            "organization": (
                                _safe_str(row.get(organization_column))
                                if organization_column
                                else ""
                            ),
                            "notes": _safe_str(row.get(notes_column)) if notes_column else "",
                        }
                    )
    except Exception:
        awards = []

    # Contributor identifiers may live in either party_identifiers or
    # contributor_identifiers. Detect the actual deployed table and columns,
    # matching both the normal Book Management writer and Deal Memo generation.
    identifiers: List[Dict[str, Any]] = []
    try:
        identifier_table = ""
        identifier_columns: set[str] = set()

        for candidate in ("party_identifiers", "contributor_identifiers"):
            cur.execute(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = 'public'
                  AND table_name = %s
                """,
                (candidate,),
            )
            candidate_columns = {
                str(row["column_name"]) for row in (cur.fetchall() or [])
            }
            if {"tenant_id", "party_id"}.issubset(candidate_columns):
                identifier_table = candidate
                identifier_columns = candidate_columns
                break

        if identifier_table:
            type_column = next(
                (
                    column
                    for column in ("identifier_type", "identifier_type_code", "type")
                    if column in identifier_columns
                ),
                "",
            )
            value_column = next(
                (
                    column
                    for column in ("identifier_value", "identifier", "value")
                    if column in identifier_columns
                ),
                "",
            )
            order_column = next(
                (
                    column
                    for column in ("item_order", "sequence_number")
                    if column in identifier_columns
                ),
                "",
            )

            if type_column and value_column:
                selected_columns = ["id"] if "id" in identifier_columns else []
                for column in (type_column, value_column, order_column):
                    if column and column not in selected_columns:
                        selected_columns.append(column)

                order_parts = []
                if order_column:
                    order_parts.append(f"{order_column} ASC")
                if "id" in identifier_columns:
                    order_parts.append("id ASC")
                order_sql = ", ".join(order_parts) or f"{type_column} ASC"

                cur.execute(
                    f"""
                    SELECT {", ".join(selected_columns)}
                    FROM {identifier_table}
                    WHERE tenant_id = %s
                      AND party_id = %s
                    ORDER BY {order_sql}
                    """,
                    (tenant_id, party_id),
                )

                for row in (cur.fetchall() or []):
                    identifier_type = _safe_str(row.get(type_column))
                    identifier_value = _safe_str(row.get(value_column))
                    identifiers.append(
                        {
                            "id": str(row.get("id")) if row.get("id") else "",
                            "type": identifier_type,
                            "identifier_type": identifier_type,
                            "identifierType": identifier_type,
                            "value": identifier_value,
                            "identifier_value": identifier_value,
                            "identifierValue": identifier_value,
                        }
                    )
    except Exception:
        identifiers = []

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
        "awards": awards,
        "identifiers": identifiers,
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


def _fetch_contributor_repeat_rows(
    cur,
    tenant_id: str,
    party_id: str,
) -> Dict[str, List[Dict[str, Any]]]:
    """Fetch ONIX repeat composites for one reusable contributor profile."""

    def q(sql: str, params: Tuple[Any, ...]) -> List[Dict[str, Any]]:
        cur.execute(sql, params)
        return [dict(row) for row in (cur.fetchall() or [])]

    identifiers = q(
        """
        SELECT name_id_type, id_type_name, id_value, item_order
        FROM party_name_identifiers
        WHERE tenant_id = %s AND party_id = %s
        ORDER BY item_order, id
        """,
        (tenant_id, party_id),
    )
    alternative_names = q(
        """
        SELECT name_type, display_name, person_name_inverted,
               names_before_key, key_names, corporate_name, item_order
        FROM party_alternative_names
        WHERE tenant_id = %s AND party_id = %s
        ORDER BY item_order, id
        """,
        (tenant_id, party_id),
    )
    websites = q(
        """
        SELECT website_role, website_description, website_link, item_order
        FROM party_websites
        WHERE tenant_id = %s AND party_id = %s
        ORDER BY item_order, id
        """,
        (tenant_id, party_id),
    )
    places = q(
        """
        SELECT contributor_place_relator, country_code, region_code,
               location_name, item_order
        FROM party_contributor_places
        WHERE tenant_id = %s AND party_id = %s
        ORDER BY item_order, id
        """,
        (tenant_id, party_id),
    )
    dates = q(
        """
        SELECT contributor_date_role, date_value, item_order
        FROM party_contributor_dates
        WHERE tenant_id = %s AND party_id = %s
        ORDER BY item_order, id
        """,
        (tenant_id, party_id),
    )
    for row in dates:
        row["date_value"] = _jsonable(row.get("date_value")) or ""

    affiliations = q(
        """
        SELECT professional_position, affiliation, affiliation_id_type,
               affiliation_id_type_name, affiliation_id_value, item_order
        FROM party_professional_affiliations
        WHERE tenant_id = %s AND party_id = %s
        ORDER BY item_order, id
        """,
        (tenant_id, party_id),
    )

    return {
        "name_identifiers": identifiers,
        "alternative_names": alternative_names,
        "websites": websites,
        "contributor_places": places,
        "contributor_dates": dates,
        "professional_affiliations": affiliations,
    }


def _fetch_contributors(cur, tenant_id: str, work_id: str) -> List[Dict[str, Any]]:
    """Return every work contributor with reusable party data and ONIX repeats."""
    cur.execute(
        """
        SELECT
            wc.id AS work_contributor_id,
            wc.party_id,
            wc.contributor_role,
            wc.sequence_number,
            wc.from_language_codes,
            wc.to_language_codes,
            wc.contributor_description,
            p.party_type,
            p.display_name,
            p.email,
            p.website,
            p.phone_country_code,
            p.phone_number,
            p.short_bio,
            p.long_bio,
            p.notes,
            p.birth_date,
            p.death_date,
            p.birth_city,
            p.birth_country,
            p.citizenship,
            p.titles_before_names,
            p.names_before_key,
            p.prefix_to_key,
            p.key_names,
            p.suffix_to_key,
            p.letters_after_names,
            p.person_name_inverted,
            p.pen_name,
            p.corporate_name,
            p.language_code,
            p.country_code,
            p.region_code
        FROM work_contributors wc
        JOIN parties p
          ON p.id = wc.party_id
         AND p.tenant_id = wc.tenant_id
        WHERE wc.tenant_id = %s
          AND wc.work_id = %s
        ORDER BY wc.sequence_number ASC NULLS LAST, wc.id ASC
        """,
        (tenant_id, work_id),
    )
    rows = cur.fetchall() or []

    out: List[Dict[str, Any]] = []
    for r in rows:
        role = _safe_str(r.get("contributor_role"))
        party_id = str(r["party_id"])
        address = _fetch_party_address(cur, tenant_id, party_id)
        repeats = _fetch_contributor_repeat_rows(cur, tenant_id, party_id)

        try:
            cur.execute(
                """
                SELECT platform, url
                FROM party_socials
                WHERE tenant_id = %s AND party_id = %s
                ORDER BY platform ASC, id ASC
                """,
                (tenant_id, party_id),
            )
            socials = [
                {"platform": _safe_str(s.get("platform")), "url": _safe_str(s.get("url"))}
                for s in (cur.fetchall() or [])
            ]
        except Exception:
            socials = []

        payload = {
            "work_contributor_id": str(r.get("work_contributor_id") or ""),
            "party_id": party_id,
            "role": role,
            "contributor_role": role,
            "role_code": role,
            "scope": _role_to_scope(role),
            "sequence_number": r.get("sequence_number") or 0,
            "from_languages": list(r.get("from_language_codes") or []),
            "fromLanguages": list(r.get("from_language_codes") or []),
            "to_languages": list(r.get("to_language_codes") or []),
            "toLanguages": list(r.get("to_language_codes") or []),
            "contributor_description": _safe_str(r.get("contributor_description")),
            "contributorDescription": _safe_str(r.get("contributor_description")),
            "party_type": _safe_str(r.get("party_type")),
            "display_name": _clean_display_name(r.get("display_name")),
            "name": _clean_display_name(r.get("display_name")),
            "email": _safe_str(r.get("email")),
            "website": _safe_str(r.get("website")),
            "phone_country_code": _safe_str(r.get("phone_country_code")),
            "phone_number": _safe_str(r.get("phone_number")),
            "phone": _format_phone(r.get("phone_country_code"), r.get("phone_number")),
            "short_bio": _safe_str(r.get("short_bio")),
            "bio": _safe_str(r.get("short_bio")),
            "long_bio": _safe_str(r.get("long_bio")),
            "notes": _safe_str(r.get("notes")),
            "birth_date": _jsonable(r.get("birth_date")) or "",
            "death_date": _jsonable(r.get("death_date")) or "",
            "birth_city": _safe_str(r.get("birth_city")),
            "birth_country": _safe_str(r.get("birth_country")),
            "citizenship": _safe_str(r.get("citizenship")),
            "titles_before_names": _safe_str(r.get("titles_before_names")),
            "names_before_key": _safe_str(r.get("names_before_key")),
            "prefix_to_key": _safe_str(r.get("prefix_to_key")),
            "key_names": _safe_str(r.get("key_names")),
            "suffix_to_key": _safe_str(r.get("suffix_to_key")),
            "letters_after_names": _safe_str(r.get("letters_after_names")),
            "person_name_inverted": _safe_str(r.get("person_name_inverted")),
            "pen_name": _safe_str(r.get("pen_name")),
            "corporate_name": _safe_str(r.get("corporate_name")),
            "language_code": _safe_str(r.get("language_code")),
            "country_code": _safe_str(r.get("country_code")),
            "region_code": _safe_str(r.get("region_code")),
            "address": address,
            "address_street": _safe_str(address.get("street")),
            "address_city": _safe_str(address.get("city")),
            "address_state": _safe_str(address.get("state")),
            "address_zip": _safe_str(address.get("zip")),
            "address_country": _safe_str(address.get("country")),
            "street": _safe_str(address.get("street")),
            "city": _safe_str(address.get("city")),
            "state": _safe_str(address.get("state")),
            "zip": _safe_str(address.get("zip")),
            "postal_code": _safe_str(address.get("zip")),
            "country": _safe_str(address.get("country")),
            "socials": socials,
        }
        payload.update(repeats)
        payload["nameIdentifiers"] = repeats["name_identifiers"]
        payload["alternativeNames"] = repeats["alternative_names"]
        payload["contributorPlaces"] = repeats["contributor_places"]
        payload["contributorDates"] = repeats["contributor_dates"]
        payload["professionalAffiliations"] = repeats["professional_affiliations"]
        out.append(payload)

    return out



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
        "original_language": _safe_str(w.get("original_language")),
        "rights": _safe_str(w.get("rights")),
        "editor_name": _safe_str(w.get("editor_name")),
        "art_director_name": _safe_str(w.get("art_director_name")),

        # Product Identity / publishing identity.
        # Publisher aliases are filled below from canonical edition_publishers.
        # Imprint remains work-level temporarily until its separate migration.
        "publisher_name": "",
        "publisher": "",
        "imprint_name": _safe_str(w.get("imprint_name")),
        "imprint": _safe_str(w.get("imprint_name")),
        "publisher_or_imprint": _safe_str(w.get("imprint_name")),

        # Filled below from normalized edition publishing dates.
        "publishing_year": None,
        "publication_date": None,
        "publishing_status": _safe_str(w.get("publishing_status")),
        "short_title": _safe_str(w.get("short_title")),
        "shortTitle": _safe_str(w.get("short_title")),
        "city_of_publication": _safe_str(w.get("city_of_publication")),
        "country_of_publication": _safe_str(w.get("country_of_publication")),
        "countryOfPublication": _safe_str(w.get("country_of_publication")),
        "originalLanguage": _safe_str(w.get("original_language")),
        "copyright_year": int(w.get("copyright_year") or 0),
        "main_description": _safe_str(w.get("main_description")),
        # Compatibility alias is filled below from canonical edition_texts TextType 04.
        "table_of_contents": "",
        "tableOfContents": "",
        "back_cover_copy": _safe_str(w.get("back_cover_copy")),
        "biographical_note": _safe_str(w.get("biographical_note")),

        # Compatibility aliases filled below from edition-level cover metadata.
        "cover_image_link": "",
        "cover_image_format": "",
        "cover_image_caption": "",

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

    # Table of Contents is canonical edition-level ONIX TextContent (TextType 04).
    # Keep the work-level aliases only as a compatibility view for Book Management.
    canonical_table_of_contents = next(
        (
            _safe_str(edition.get("table_of_contents"))
            for edition in editions
            if _safe_str(edition.get("table_of_contents"))
        ),
        "",
    )
    doc["table_of_contents"] = canonical_table_of_contents
    doc["tableOfContents"] = canonical_table_of_contents

    # Existing modules may still consume a convenient work-level cover alias,
    # but its source is now strictly edition-level metadata.
    primary_cover_edition = next(
        (
            edition
            for edition in editions
            if _safe_str(edition.get("cover_image_link"))
        ),
        None,
    )
    if primary_cover_edition:
        doc["cover_image_link"] = _safe_str(primary_cover_edition.get("cover_image_link"))
        doc["cover_image_format"] = _safe_str(primary_cover_edition.get("cover_image_format"))
        doc["cover_image_caption"] = _safe_str(primary_cover_edition.get("cover_image_caption"))

    # Compatibility aliases are derived from normalized edition_publishers.
    primary_publisher = next(
        (_safe_str(e.get("publisher_name")) for e in editions if _safe_str(e.get("publisher_name"))),
        "",
    )
    doc["publisher_name"] = primary_publisher
    doc["publisher"] = primary_publisher
    doc["publisher_or_imprint"] = primary_publisher or doc.get("imprint_name", "")

    # Existing modules still expect these work-level API aliases. Derive them
    # from the first edition with ONIX PublishingDateRole 01 rather than storing
    # duplicate work-level publication metadata.
    primary_work_publication_date = next(
        (
            _safe_str(edition.get("publication_date") or edition.get("pub_date"))
            for edition in editions
            if _safe_str(edition.get("publication_date") or edition.get("pub_date"))
        ),
        "",
    )
    doc["publication_date"] = primary_work_publication_date or None
    _pub_digits = re.sub(r"[^0-9]", "", primary_work_publication_date)
    doc["publishing_year"] = int(_pub_digits[:4]) if len(_pub_digits) >= 4 else None

    # `formats` used to be reduced to a legacy summary and therefore discarded
    # the persisted edition id, ONIX Product Classification fields, and
    # edition_identifiers.  The rich edition rows already contain every legacy
    # summary key (`format`, `isbn`, `pub_date`, prices, pages, dimensions) so
    # returning them directly remains backward compatible while allowing
    # Product Identity to round-trip all saved fields.
    doc["formats"] = editions
    doc["editions"] = editions
    doc["_editions"] = editions

    title_rows = _fetch_work_titles(cur, tenant_id, work_id)
    doc["title_prefix"] = title_rows.get("title_prefix") or ""
    doc["titlePrefix"] = doc["title_prefix"]
    doc["title_without_prefix"] = title_rows.get("title_without_prefix") or ""
    doc["titleWithoutPrefix"] = doc["title_without_prefix"]
    doc["title_element_level"] = title_rows.get("title_element_level") or "01"
    doc["titleElementLevel"] = doc["title_element_level"]
    doc["no_prefix"] = bool(title_rows.get("no_prefix"))
    doc["noPrefix"] = doc["no_prefix"]
    doc["title_part_number"] = title_rows.get("title_part_number") or ""
    doc["titlePartNumber"] = doc["title_part_number"]
    doc["alternative_titles"] = title_rows.get("alternative_titles") or []
    doc["alternativeTitles"] = doc["alternative_titles"]

    collections = _fetch_work_collections(cur, tenant_id, work_id)
    doc["collections"] = collections
    doc["collection_memberships"] = collections
    doc["collectionMemberships"] = collections

    primary_collection = next(
        (row for row in collections if row.get("is_primary")),
        collections[0] if collections else None,
    )
    if primary_collection:
        doc["collection_title"] = primary_collection.get("collection_title") or ""
        doc["collectionTitle"] = doc["collection_title"]
        doc["collection_number"] = primary_collection.get("collection_number") or ""
        doc["collectionNumber"] = doc["collection_number"]
        doc["volume_number"] = primary_collection.get("volume_number") or ""
        doc["volumeNumber"] = doc["volume_number"]
        doc["part_number"] = primary_collection.get("part_number") or ""
        doc["partNumber"] = doc["part_number"]
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

        # Preserve the legacy author/illustrator role aliases, but include the
        # complete current party payload for every ONIX contributor role.
        contributor_payload = dict(c)
        contributor_payload.update(
            {
                "party_id": party_id,
                "contributor_role": normalized_role or role,
                "role": role,
                "role_code": role,
                "sequence_number": c.get("sequence_number"),
                "display_name": display_name,
                "name": display_name,
                "email": email,
            }
        )
        work_contributors.append(contributor_payload)

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
            "awards": block.get("awards") or [],
            "honors": block.get("awards") or [],
            "identifiers": block.get("identifiers") or [],
            "contributor_identifiers": block.get("identifiers") or [],
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
            "party_id": party_id,

            "titles_before_names": party_summary.get("titles_before_names") or "",
            "names_before_key": party_summary.get("names_before_key") or "",
            "prefix_to_key": party_summary.get("prefix_to_key") or "",
            "key_names": party_summary.get("key_names") or "",
            "suffix_to_key": party_summary.get("suffix_to_key") or "",
            "letters_after_names": party_summary.get("letters_after_names") or "",
            "person_name_inverted": party_summary.get("person_name_inverted") or "",
            "pen_name": party_summary.get("pen_name") or "",
            "corporate_name": party_summary.get("corporate_name") or "",
            "language_code": party_summary.get("language_code") or "",
            "country_code": party_summary.get("country_code") or "",
            "region_code": party_summary.get("region_code") or "",

            "birth_date": party_summary.get("birth_date") or "",
            "death_date": party_summary.get("death_date") or "",
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
        doc[f"{scope}_titles_before_names"] = (
            party_summary.get("titles_before_names") or ""
        )
        doc[f"{scope}_names_before_key"] = (
            party_summary.get("names_before_key") or ""
        )
        doc[f"{scope}_prefix_to_key"] = (
            party_summary.get("prefix_to_key") or ""
        )
        doc[f"{scope}_key_names"] = (
            party_summary.get("key_names") or ""
        )
        doc[f"{scope}_suffix_to_key"] = (
            party_summary.get("suffix_to_key") or ""
        )
        doc[f"{scope}_letters_after_names"] = (
            party_summary.get("letters_after_names") or ""
        )
        doc[f"{scope}_person_name_inverted"] = (
            party_summary.get("person_name_inverted") or ""
        )
        doc[f"{scope}_pen_name"] = party_summary.get("pen_name") or ""
        doc[f"{scope}_corporate_name"] = (
            party_summary.get("corporate_name") or ""
        )
        doc[f"{scope}_language_code"] = (
            party_summary.get("language_code") or ""
        )
        doc[f"{scope}_country_code"] = (
            party_summary.get("country_code") or ""
        )
        doc[f"{scope}_region_code"] = (
            party_summary.get("region_code") or ""
        )
        doc[scope]["socials"] = block.get("socials") or []
        doc[scope]["awards"] = block.get("awards") or []
        doc[scope]["identifiers"] = block.get("identifiers") or []

        doc[f"{scope}_socials"] = block.get("socials") or []
        doc[f"{scope}_awards"] = block.get("awards") or []
        doc[f"{scope}_honors"] = block.get("awards") or []
        doc[f"{scope}_identifiers"] = block.get("identifiers") or []
        doc[f"{scope}_contributor_identifiers"] = block.get("identifiers") or []

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
                    SELECT
                        w.*,
                        (
                            SELECT COALESCE(NULLIF(pd.date_text, ''), to_char(pd.date_value, 'YYYYMMDD'))
                            FROM editions e2
                            JOIN edition_publishing_dates pd
                              ON pd.tenant_id = e2.tenant_id
                             AND pd.edition_id = e2.id
                            WHERE e2.tenant_id = w.tenant_id
                              AND e2.work_id = w.id
                              AND pd.date_role = '01'
                            ORDER BY e2.created_at ASC, pd.item_order ASC, pd.created_at ASC, pd.id ASC
                            LIMIT 1
                        ) AS normalized_publication_date,
                        (
                            SELECT EXTRACT(YEAR FROM pd.date_value)::int
                            FROM editions e2
                            JOIN edition_publishing_dates pd
                              ON pd.tenant_id = e2.tenant_id
                             AND pd.edition_id = e2.id
                            WHERE e2.tenant_id = w.tenant_id
                              AND e2.work_id = w.id
                              AND pd.date_role = '01'
                              AND pd.date_value IS NOT NULL
                            ORDER BY e2.created_at ASC, pd.item_order ASC, pd.created_at ASC, pd.id ASC
                            LIMIT 1
                        ) AS normalized_publishing_year,
                        (
                            SELECT ep.publisher_name
                            FROM editions e2
                            JOIN edition_publishers ep
                              ON ep.tenant_id = e2.tenant_id
                             AND ep.edition_id = e2.id
                            WHERE e2.tenant_id = w.tenant_id
                              AND e2.work_id = w.id
                              AND ep.publishing_role = '01'
                            ORDER BY e2.created_at ASC, ep.item_order ASC, ep.created_at ASC, ep.id ASC
                            LIMIT 1
                        ) AS normalized_publisher_name
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
                    SELECT
                        w.*,
                        (
                            SELECT COALESCE(NULLIF(pd.date_text, ''), to_char(pd.date_value, 'YYYYMMDD'))
                            FROM editions e2
                            JOIN edition_publishing_dates pd
                              ON pd.tenant_id = e2.tenant_id
                             AND pd.edition_id = e2.id
                            WHERE e2.tenant_id = w.tenant_id
                              AND e2.work_id = w.id
                              AND pd.date_role = '01'
                            ORDER BY e2.created_at ASC, pd.item_order ASC, pd.created_at ASC, pd.id ASC
                            LIMIT 1
                        ) AS normalized_publication_date,
                        (
                            SELECT EXTRACT(YEAR FROM pd.date_value)::int
                            FROM editions e2
                            JOIN edition_publishing_dates pd
                              ON pd.tenant_id = e2.tenant_id
                             AND pd.edition_id = e2.id
                            WHERE e2.tenant_id = w.tenant_id
                              AND e2.work_id = w.id
                              AND pd.date_role = '01'
                              AND pd.date_value IS NOT NULL
                            ORDER BY e2.created_at ASC, pd.item_order ASC, pd.created_at ASC, pd.id ASC
                            LIMIT 1
                        ) AS normalized_publishing_year,
                        (
                            SELECT ep.publisher_name
                            FROM editions e2
                            JOIN edition_publishers ep
                              ON ep.tenant_id = e2.tenant_id
                             AND ep.edition_id = e2.id
                            WHERE e2.tenant_id = w.tenant_id
                              AND e2.work_id = w.id
                              AND ep.publishing_role = '01'
                            ORDER BY e2.created_at ASC, ep.item_order ASC, ep.created_at ASC, ep.id ASC
                            LIMIT 1
                        ) AS normalized_publisher_name
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








@router.post("/works/{work_id}/editions/{edition_id}/rights-restrictions")
def save_edition_rights_restrictions(
    work_id: str,
    edition_id: str,
    payload: Dict[str, Any] = Body(...),
    tenant_slug: str = Query("marble-press"),
):
    with db_conn() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            tenant_id = _get_tenant_id_from_slug(
                cur,
                tenant_slug,
            )

            try:
                result = update_edition_rights_restrictions(
                    cur,
                    tenant_id,
                    work_id,
                    edition_id,
                    payload,
                )
            except ValueError as exc:
                message = str(exc)
                raise HTTPException(
                    status_code=404 if message == "Edition not found" else 400,
                    detail=message,
                )

            editions = _fetch_editions(
                cur,
                tenant_id,
                work_id,
            )

            edition = next(
                (
                    row
                    for row in editions
                    if str(row.get("id")) == str(edition_id)
                ),
                None,
            )

        conn.commit()

    return {
        **result,
        "edition": edition,
    }


@router.get("/works/{work_id}/editions/{edition_id}/task-assignments")
def get_edition_task_assignments(
    work_id: str,
    edition_id: str,
    tenant_slug: str = Query("marble-press"),
):
    with db_conn() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            tenant_id = _get_tenant_id_from_slug(cur, tenant_slug)

            cur.execute(
                """
                SELECT 1
                FROM editions
                WHERE tenant_id = %s
                  AND work_id = %s
                  AND id = %s
                LIMIT 1
                """,
                (tenant_id, work_id, edition_id),
            )
            if not cur.fetchone():
                raise HTTPException(status_code=404, detail="Edition not found")

            items = _fetch_bookdev_task_assignments(
                cur,
                tenant_id,
                work_id,
                edition_id,
            )

    return {
        "ok": True,
        "tenant_slug": tenant_slug,
        "work_id": work_id,
        "edition_id": edition_id,
        "items": items,
        "assignments": items,
    }


@router.post("/works/{work_id}/editions/{edition_id}/task-assignments")
def save_edition_task_assignment(
    work_id: str,
    edition_id: str,
    payload: Dict[str, Any] = Body(...),
    tenant_slug: str = Query("marble-press"),
):
    with db_conn() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            tenant_id = _get_tenant_id_from_slug(cur, tenant_slug)

            try:
                result = upsert_bookdev_task_assignment(
                    cur,
                    tenant_id,
                    work_id,
                    edition_id,
                    payload,
                )
            except ValueError as exc:
                message = str(exc)
                raise HTTPException(
                    status_code=404 if message == "Edition not found" else 400,
                    detail=message,
                )

            items = _fetch_bookdev_task_assignments(
                cur,
                tenant_id,
                work_id,
                edition_id,
            )

        conn.commit()

    return {
        **result,
        "items": items,
        "assignments": items,
    }


@router.post("/works/{work_id}/editions/{edition_id}/cited-content")
def save_edition_cited_content(work_id: str, edition_id: str, payload: Dict[str, Any] = Body(...), tenant_slug: str = Query("marble-press")):
    with db_conn() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            tenant_id=_get_tenant_id_from_slug(cur,tenant_slug)
            try: result=update_edition_cited_content(cur,tenant_id,work_id,edition_id,payload)
            except ValueError as exc:
                message=str(exc); raise HTTPException(status_code=404 if message=="Edition not found" else 400, detail=message)
            editions=_fetch_editions(cur,tenant_id,work_id); edition=next((row for row in editions if str(row.get("id"))==str(edition_id)),None)
        conn.commit()
    return {**result,"edition":edition}


@router.post("/works/{work_id}/editions/{edition_id}/awards")
def save_edition_awards(
    work_id: str,
    edition_id: str,
    payload: Dict[str, Any] = Body(...),
    tenant_slug: str = Query("marble-press"),
):
    with db_conn() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            tenant_id = _get_tenant_id_from_slug(
                cur,
                tenant_slug,
            )

            try:
                result = update_edition_awards(
                    cur,
                    tenant_id,
                    work_id,
                    edition_id,
                    payload,
                )
            except ValueError as exc:
                message = str(exc)
                raise HTTPException(
                    status_code=404
                    if message == "Edition not found"
                    else 400,
                    detail=message,
                )

            editions = _fetch_editions(
                cur,
                tenant_id,
                work_id,
            )

            edition = next(
                (
                    row
                    for row in editions
                    if str(row.get("id"))
                    == str(edition_id)
                ),
                None,
            )

        conn.commit()

    return {
        **result,
        "edition": edition,
    }


@router.post("/works/{work_id}/editions/{edition_id}/related-products")
def save_edition_related_products(
    work_id: str,
    edition_id: str,
    payload: Dict[str, Any] = Body(...),
    tenant_slug: str = Query("marble-press"),
):
    with db_conn() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            tenant_id = _get_tenant_id_from_slug(
                cur,
                tenant_slug,
            )

            try:
                result = update_edition_related_products(
                    cur,
                    tenant_id,
                    work_id,
                    edition_id,
                    payload,
                )
            except ValueError as exc:
                message = str(exc)
                raise HTTPException(
                    status_code=404
                    if message == "Edition not found"
                    else 400,
                    detail=message,
                )

            editions = _fetch_editions(
                cur,
                tenant_id,
                work_id,
            )

            edition = next(
                (
                    row
                    for row in editions
                    if str(row.get("id"))
                    == str(edition_id)
                ),
                None,
            )

        conn.commit()

    return {
        **result,
        "edition": edition,
    }


@router.post("/works/{work_id}/editions/{edition_id}/supply-pricing")
def save_edition_supply_pricing(
    work_id: str,
    edition_id: str,
    payload: Dict[str, Any] = Body(...),
    tenant_slug: str = Query("marble-press"),
):
    with db_conn() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            tenant_id = _get_tenant_id_from_slug(
                cur,
                tenant_slug,
            )

            try:
                result = update_edition_supply_pricing(
                    cur,
                    tenant_id,
                    work_id,
                    edition_id,
                    payload,
                )
            except ValueError as exc:
                message = str(exc)
                raise HTTPException(
                    status_code=404 if message == "Edition not found" else 400,
                    detail=message,
                )

            editions = _fetch_editions(
                cur,
                tenant_id,
                work_id,
            )

            edition = next(
                (
                    row
                    for row in editions
                    if str(row.get("id")) == str(edition_id)
                ),
                None,
            )

        conn.commit()

    return {
        **result,
        "edition": edition,
    }


@router.post("/works/{work_id}/editions/{edition_id}/product-details")
def save_edition_product_details(
    work_id: str,
    edition_id: str,
    payload: Dict[str, Any] = Body(...),
    tenant_slug: str = Query("marble-press"),
):
    with db_conn() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            tenant_id = _get_tenant_id_from_slug(cur, tenant_slug)

            try:
                result = update_edition_product_details(
                    cur, tenant_id, work_id, edition_id, payload
                )
            except ValueError as exc:
                message = str(exc)
                raise HTTPException(
                    status_code=404 if message == "Edition not found" else 400,
                    detail=message,
                )

            editions = _fetch_editions(cur, tenant_id, work_id)
            edition = next(
                (row for row in editions if str(row.get("id")) == str(edition_id)),
                None,
            )

        conn.commit()

    return {**result, "edition": edition}


@router.post("/works/{work_id}/editions/{edition_id}/publishing-dates")
def save_edition_publishing_dates(
    work_id: str,
    edition_id: str,
    payload: Dict[str, Any] = Body(...),
    tenant_slug: str = Query("marble-press"),
):
    with db_conn() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            tenant_id = _get_tenant_id_from_slug(
                cur,
                tenant_slug,
            )

            try:
                result = update_edition_publishing_dates(
                    cur,
                    tenant_id,
                    work_id,
                    edition_id,
                    payload,
                )
            except ValueError as exc:
                message = str(exc)
                status_code = (
                    404
                    if message == "Edition not found"
                    else 400
                )
                raise HTTPException(
                    status_code=status_code,
                    detail=message,
                )

            editions = _fetch_editions(
                cur,
                tenant_id,
                work_id,
            )

            edition = next(
                (
                    row
                    for row in editions
                    if str(row.get("id"))
                    == str(edition_id)
                ),
                None,
            )

        conn.commit()

    return {
        **result,
        "edition": edition,
    }


@router.post("/works/{work_id}/editions/{edition_id}/subjects-audience")
def save_edition_subjects_audience(
    work_id: str,
    edition_id: str,
    payload: Dict[str, Any] = Body(...),
    tenant_slug: str = Query("marble-press"),
):
    with db_conn() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            tenant_id = _get_tenant_id_from_slug(
                cur,
                tenant_slug,
            )

            try:
                result = update_edition_subjects_audience(
                    cur,
                    tenant_id,
                    work_id,
                    edition_id,
                    payload,
                )
            except ValueError as exc:
                message = str(exc)
                status_code = (
                    404
                    if message == "Edition not found"
                    else 400
                )
                raise HTTPException(
                    status_code=status_code,
                    detail=message,
                )

            editions = _fetch_editions(
                cur,
                tenant_id,
                work_id,
            )

            edition = next(
                (
                    row
                    for row in editions
                    if str(row.get("id"))
                    == str(edition_id)
                ),
                None,
            )

        conn.commit()

    return {
        **result,
        "edition": edition,
    }


@router.get("/works/{work_id}/editions/{edition_id}/descriptive-content")
def get_edition_descriptive_content(
    work_id: str,
    edition_id: str,
    tenant_slug: str = Query("marble-press"),
):
    """
    Return canonical edition-level descriptive content directly from edition_texts.

    This endpoint intentionally bypasses work-level compatibility aliases and
    frontend book mappers so every persisted ONIX field (including
    source_title_type) round-trips exactly to the Descriptive Content card.
    """
    with db_conn() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            tenant_id = _get_tenant_id_from_slug(cur, tenant_slug)

            cur.execute(
                """
                SELECT 1
                FROM editions
                WHERE tenant_id = %s
                  AND work_id = %s
                  AND id = %s
                LIMIT 1
                """,
                (tenant_id, work_id, edition_id),
            )
            if not cur.fetchone():
                raise HTTPException(status_code=404, detail="Edition not found")

            descriptive_texts = _fetch_edition_texts(
                cur,
                tenant_id,
                edition_id,
            )

    return {
        "work_id": work_id,
        "edition_id": edition_id,
        "descriptive_texts": descriptive_texts,
        "descriptiveTexts": descriptive_texts,
    }


@router.post("/works/{work_id}/editions/{edition_id}/descriptive-content")
def save_edition_descriptive_content(
    work_id: str,
    edition_id: str,
    payload: Dict[str, Any] = Body(...),
    tenant_slug: str = Query("marble-press"),
):
    with db_conn() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            tenant_id = _get_tenant_id_from_slug(cur, tenant_slug)

            try:
                result = update_edition_descriptive_content(
                    cur,
                    tenant_id,
                    work_id,
                    edition_id,
                    payload,
                )
            except ValueError as exc:
                message = str(exc)
                status_code = 404 if message == "Edition not found" else 400
                raise HTTPException(status_code=status_code, detail=message)

            editions = _fetch_editions(cur, tenant_id, work_id)
            edition = next(
                (
                    row
                    for row in editions
                    if str(row.get("id")) == str(edition_id)
                ),
                None,
            )

        conn.commit()

    return {
        **result,
        "edition": edition,
    }


@router.post("/works/{work_id}/titles-collections")
def save_titles_collections(
    work_id: str,
    payload: Dict[str, Any] = Body(...),
    tenant_slug: str = Query("marble-press"),
):
    with db_conn() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            tenant_id = _get_tenant_id_from_slug(cur, tenant_slug)

            try:
                result = update_work_titles_collections(
                    cur,
                    tenant_id,
                    work_id,
                    payload,
                )
            except ValueError as exc:
                message = str(exc)
                status_code = 404 if message == "Existing work not found" else 400
                raise HTTPException(status_code=status_code, detail=message)

            work = _build_full_work_payload(cur, tenant_id, work_id)

        conn.commit()

    return {
        **result,
        "work": work,
    }


@router.post("/works/{work_id}/editions/{edition_id}/product-identity")
def save_product_identity(
    work_id: str,
    edition_id: str,
    payload: Dict[str, Any] = Body(...),
    tenant_slug: str = Query("marble-press"),
):
    with db_conn() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            tenant_id = _get_tenant_id_from_slug(cur, tenant_slug)

            try:
                result = update_edition_product_identity(
                    cur,
                    tenant_id,
                    work_id,
                    edition_id,
                    payload,
                )
            except ValueError as exc:
                message = str(exc)
                status_code = 404 if message == "Edition not found" else 400
                raise HTTPException(status_code=status_code, detail=message)

            work = _build_full_work_payload(cur, tenant_id, work_id)
            editions = _fetch_editions(cur, tenant_id, work_id)
            edition = next(
                (row for row in editions if str(row.get("id")) == str(edition_id)),
                None,
            )

        conn.commit()

    return {
        **result,
        "edition": edition,
        "work": work,
    }


@router.post("/works/{work_id}/first-edition")
def create_first_edition(
    work_id: str,
    payload: Dict[str, Any] = Body(...),
    tenant_slug: str = Query("marble-press"),
):
    with db_conn() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            tenant_id = _get_tenant_id_from_slug(cur, tenant_slug)

            try:
                result = create_first_work_edition(
                    cur,
                    tenant_id,
                    work_id,
                    payload,
                )
            except ValueError as exc:
                message = str(exc)
                status_code = (
                    404
                    if message == "Existing work not found"
                    else 409
                    if "already has an edition" in message
                    else 400
                )
                raise HTTPException(status_code=status_code, detail=message)

        conn.commit()

    return result


@router.post("/works/{work_id}/contributors")
def create_work_contributor(
    work_id: str,
    payload: Dict[str, Any] = Body(...),
    tenant_slug: str = Query("marble-press"),
):
    with db_conn() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            tenant_id = _get_tenant_id_from_slug(
                cur,
                tenant_slug,
            )

            try:
                result = add_work_contributor(
                    cur,
                    tenant_id,
                    work_id,
                    payload,
                )
            except ValueError as exc:
                message = str(exc)

                if message == "Existing work not found":
                    raise HTTPException(
                        status_code=404,
                        detail=message,
                    )

                raise HTTPException(
                    status_code=400,
                    detail=message,
                )

        conn.commit()

    return result

@router.delete("/works/{work_id}/contributors/{party_id}")
def unlink_contributor_from_work(
    work_id: str,
    party_id: str,
    role_code: str = Query(""),
    tenant_slug: str = Query("marble-press"),
):
    with db_conn() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            tenant_id = _get_tenant_id_from_slug(cur, tenant_slug)

            try:
                result = unlink_work_contributor(
                    cur,
                    tenant_id,
                    work_id,
                    party_id,
                    role_code=role_code,
                )
            except ValueError as exc:
                message = str(exc)
                status_code = (
                    404
                    if message in (
                        "Contributor assignment not found",
                        "Contributor not found",
                    )
                    else 409
                    if "cannot be unlinked" in message.lower()
                    else 400
                )
                raise HTTPException(
                    status_code=status_code,
                    detail=message,
                )

        conn.commit()

    return result


@router.delete("/contributors/{party_id}")
def delete_contributor(
    party_id: str,
    tenant_slug: str = Query("marble-press"),
):
    with db_conn() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            tenant_id = _get_tenant_id_from_slug(cur, tenant_slug)

            try:
                result = delete_contributor_party(
                    cur,
                    tenant_id,
                    party_id,
                )
            except ValueError as exc:
                message = str(exc)
                status_code = (
                    404
                    if message == "Contributor not found"
                    else 409
                    if "cannot be deleted" in message.lower()
                    else 400
                )
                raise HTTPException(
                    status_code=status_code,
                    detail=message,
                )

        conn.commit()

    return result
@router.get("/contributors/search")
def search_contributors(
    query: str = Query(..., min_length=2),
    limit: int = Query(10, ge=1, le=25),
    tenant_slug: str = Query("marble-press"),
):
    search = _safe_str(query)

    with db_conn() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            tenant_id = _get_tenant_id_from_slug(cur, tenant_slug)

            cur.execute(
                """
                SELECT
                    p.id::text AS party_id,
                    p.party_type,
                    p.display_name,
                    p.email,
                    p.website,
                    p.phone_country_code,
                    p.phone_number,
                    p.titles_before_names,
                    p.names_before_key,
                    p.prefix_to_key,
                    p.key_names,
                    p.suffix_to_key,
                    p.letters_after_names,
                    p.person_name_inverted,
                    p.corporate_name,
                    p.language_code,
                    p.short_bio,
                    p.long_bio,
                    p.notes,
                    p.birth_date,
                    p.death_date
                FROM parties p
                WHERE p.tenant_id = %s
                  AND (
                      p.display_name ILIKE %s
                      OR coalesce(p.email, '') ILIKE %s
                  )
                  AND coalesce(trim(p.display_name), '') <> ''
                ORDER BY
                    CASE
                        WHEN lower(p.display_name) = lower(%s) THEN 0
                        WHEN lower(p.display_name) LIKE lower(%s) THEN 1
                        ELSE 2
                    END,
                    p.display_name
                LIMIT %s
                """,
                (
                    tenant_id,
                    f"%{search}%",
                    f"%{search}%",
                    search,
                    f"{search}%",
                    limit,
                ),
            )

            rows = cur.fetchall() or []
            items = []

            for row in rows:
                address = _fetch_party_address(
                    cur,
                    tenant_id,
                    str(row.get("party_id") or ""),
                )

                phone_country_code = _safe_str(
                    row.get("phone_country_code")
                )
                phone_number = _safe_str(row.get("phone_number"))
                phone = " ".join(
                    value
                    for value in (phone_country_code, phone_number)
                    if value
                ).strip()

                repeats = _fetch_contributor_repeat_rows(
                    cur,
                    tenant_id,
                    str(row.get("party_id") or ""),
                )

                items.append(
                    {
                        "party_id": _safe_str(row.get("party_id")),
                        "party_type": _safe_str(row.get("party_type")) or "person",
                        "display_name": _safe_str(row.get("display_name")),
                        "email": _safe_str(row.get("email")),
                        "website": _safe_str(row.get("website")),
                        "phone_country_code": phone_country_code,
                        "phone_number": phone_number,
                        "phone": phone,
                        "titles_before_names": _safe_str(row.get("titles_before_names")),
                        "names_before_key": _safe_str(row.get("names_before_key")),
                        "prefix_to_key": _safe_str(row.get("prefix_to_key")),
                        "key_names": _safe_str(row.get("key_names")),
                        "suffix_to_key": _safe_str(row.get("suffix_to_key")),
                        "letters_after_names": _safe_str(row.get("letters_after_names")),
                        "person_name_inverted": _safe_str(row.get("person_name_inverted")),
                        "corporate_name": _safe_str(row.get("corporate_name")),
                        "language_code": _safe_str(row.get("language_code")),
                        "short_bio": _safe_str(row.get("short_bio")),
                        "long_bio": _safe_str(row.get("long_bio")),
                        "notes": _safe_str(row.get("notes")),
                        "birth_date": _jsonable(row.get("birth_date")) or "",
                        "death_date": _jsonable(row.get("death_date")) or "",
                        "street": _safe_str(address.get("street")),
                        "city": _safe_str(address.get("city")),
                        "state": _safe_str(address.get("state")),
                        "zip": _safe_str(address.get("zip")),
                        "country": _safe_str(address.get("country")),
                        **repeats,
                    }
                )

    return {
        "ok": True,
        "query": search,
        "items": items,
    }

def _fetch_metadata_assistant_question_state(
    cur,
    tenant_id: str,
    work_id: str,
    edition_id: str,
) -> List[Dict[str, Any]]:
    cur.execute(
        """
        SELECT
            question_key,
            status,
            created_at,
            updated_at
        FROM metadata_assistant_question_state
        WHERE tenant_id = %s
          AND work_id = %s
          AND edition_id = %s
        ORDER BY updated_at ASC, question_key ASC
        """,
        (
            tenant_id,
            work_id,
            edition_id,
        ),
    )

    return [
        {
            "question_key": _safe_str(
                row.get("question_key")
            ),
            "questionKey": _safe_str(
                row.get("question_key")
            ),
            "status": _safe_str(
                row.get("status")
            ),
            "created_at": _jsonable(
                row.get("created_at")
            ),
            "updated_at": _jsonable(
                row.get("updated_at")
            ),
        }
        for row in (cur.fetchall() or [])
    ]


@router.get(
    "/works/{work_id}/editions/{edition_id}/metadata-assistant-state"
)
def get_metadata_assistant_question_state(
    work_id: str,
    edition_id: str,
    tenant_slug: str = Query("marble-press"),
):
    with db_conn() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            tenant_id = _get_tenant_id_from_slug(
                cur,
                tenant_slug,
            )

            cur.execute(
                """
                SELECT 1
                FROM editions
                WHERE tenant_id = %s
                  AND work_id = %s
                  AND id = %s
                LIMIT 1
                """,
                (
                    tenant_id,
                    work_id,
                    edition_id,
                ),
            )
            if not cur.fetchone():
                raise HTTPException(
                    status_code=404,
                    detail="Edition not found",
                )

            items = (
                _fetch_metadata_assistant_question_state(
                    cur,
                    tenant_id,
                    work_id,
                    edition_id,
                )
            )

    return {
        "ok": True,
        "work_id": work_id,
        "edition_id": edition_id,
        "items": items,
    }


@router.post(
    "/works/{work_id}/editions/{edition_id}/metadata-assistant-state"
)
def save_metadata_assistant_question_state(
    work_id: str,
    edition_id: str,
    payload: Dict[str, Any] = Body(...),
    tenant_slug: str = Query("marble-press"),
):
    with db_conn() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            tenant_id = _get_tenant_id_from_slug(
                cur,
                tenant_slug,
            )

            try:
                result = (
                    update_metadata_assistant_question_state(
                        cur,
                        tenant_id,
                        work_id,
                        edition_id,
                        payload,
                    )
                )
            except ValueError as exc:
                message = str(exc)
                raise HTTPException(
                    status_code=404
                    if message == "Edition not found"
                    else 400,
                    detail=message,
                )

            items = (
                _fetch_metadata_assistant_question_state(
                    cur,
                    tenant_id,
                    work_id,
                    edition_id,
                )
            )

        conn.commit()

    return {
        **result,
        "items": items,
    }



# ============================================================================
# ONIX expansion readback overrides: Titles & Collections / Descriptive Content /
# Publishing & Dates. Kept after the contributor-expanded implementation.
# ============================================================================

def _fetch_text_content_dates(
    cur,
    tenant_id: str,
    edition_id: str,
    edition_text_id: str,
) -> List[Dict[str, Any]]:
    try:
        cur.execute(
            """
            SELECT id, content_date_role, date_format, date_text, item_order
            FROM edition_text_content_dates
            WHERE tenant_id = %s AND edition_id = %s AND edition_text_id = %s
            ORDER BY item_order, id
            """,
            (tenant_id, edition_id, edition_text_id),
        )
        rows = cur.fetchall() or []
    except Exception:
        return []
    return [
        {
            "id": str(row.get("id") or ""),
            "content_date_role": _safe_str(row.get("content_date_role")),
            "contentDateRole": _safe_str(row.get("content_date_role")),
            "date_format": _safe_str(row.get("date_format")),
            "dateFormat": _safe_str(row.get("date_format")),
            "date_text": _safe_str(row.get("date_text")),
            "dateText": _safe_str(row.get("date_text")),
            "date_value": _safe_str(row.get("date_text")),
            "dateValue": _safe_str(row.get("date_text")),
            "date": _safe_str(row.get("date_text")),
            "item_order": int(row.get("item_order") or 0),
        }
        for row in rows
    ]




def _fetch_edition_publishing_dates(cur, tenant_id: str, edition_id: str) -> List[Dict[str, Any]]:
    try:
        cur.execute(
            """
            SELECT id, date_role, date_value, date_text, date_format, note,
                   item_order, created_at, updated_at
            FROM edition_publishing_dates
            WHERE tenant_id = %s AND edition_id = %s
            ORDER BY item_order, created_at, id
            """,
            (tenant_id, edition_id),
        )
        rows = cur.fetchall() or []
    except Exception:
        return []

    result = []
    for row in rows:
        lexical = _safe_str(row.get("date_text"))
        if not lexical and row.get("date_value"):
            lexical = str(_jsonable(row.get("date_value")) or "").replace("-", "")
        result.append(
            {
                "id": str(row.get("id") or ""),
                "date_role": _safe_str(row.get("date_role")),
                "dateRole": _safe_str(row.get("date_role")),
                "publishing_date_role": _safe_str(row.get("date_role")),
                "publishingDateRole": _safe_str(row.get("date_role")),
                "date_value": lexical,
                "dateValue": lexical,
                "date": lexical,
                "display_date": lexical,
                "displayDate": lexical,
                "date_text": lexical,
                "dateText": lexical,
                "date_format": _safe_str(row.get("date_format")),
                "dateFormat": _safe_str(row.get("date_format")),
                "note": _safe_str(row.get("note")),
                "date_note": _safe_str(row.get("note")),
                "dateNote": _safe_str(row.get("note")),
                "item_order": int(row.get("item_order") or 0),
                "sequence_number": int(row.get("item_order") or 0),
                "sequenceNumber": int(row.get("item_order") or 0),
            }
        )
    return result


def _fetch_work_titles(cur, tenant_id: str, work_id: str) -> Dict[str, Any]:
    result = {
        "title_prefix": "",
        "title_without_prefix": "",
        "title_element_level": "01",
        "no_prefix": False,
        "title_part_number": "",
        "alternative_titles": [],
    }
    try:
        cur.execute(
            """
            SELECT id, title_type, title_element_level, title_prefix,
                   title_without_prefix, subtitle, language_code,
                   no_prefix, part_number, is_primary, item_order
            FROM work_titles
            WHERE tenant_id = %s AND work_id = %s
            ORDER BY is_primary DESC, item_order, id
            """,
            (tenant_id, work_id),
        )
        rows = cur.fetchall() or []
    except Exception:
        return result

    alternatives = []
    for row in rows:
        if row.get("is_primary"):
            result["title_prefix"] = _safe_str(row.get("title_prefix"))
            result["title_without_prefix"] = _safe_str(row.get("title_without_prefix"))
            result["title_element_level"] = _safe_str(row.get("title_element_level")) or "01"
            result["no_prefix"] = bool(row.get("no_prefix"))
            result["title_part_number"] = _safe_str(row.get("part_number"))
            continue
        prefix = _safe_str(row.get("title_prefix"))
        body = _safe_str(row.get("title_without_prefix"))
        title_text = " ".join(part for part in (prefix, body) if part).strip()

        alternatives.append(
            {
                "id": str(row["id"]),
                "title_type": _safe_str(row.get("title_type")),
                "titleType": _safe_str(row.get("title_type")),
                "title_element_level": _safe_str(row.get("title_element_level")),
                "titleElementLevel": _safe_str(row.get("title_element_level")),
                "title_prefix": prefix,
                "titlePrefix": prefix,
                "title_without_prefix": body,
                "titleWithoutPrefix": body,
                "no_prefix": bool(row.get("no_prefix")),
                "noPrefix": bool(row.get("no_prefix")),
                "part_number": _safe_str(row.get("part_number")),
                "partNumber": _safe_str(row.get("part_number")),
                "title": title_text,
                "subtitle": _safe_str(row.get("subtitle")),
                "language_code": _safe_str(row.get("language_code")),
                "languageCode": _safe_str(row.get("language_code")),
                "item_order": int(row.get("item_order") or 0),
            }
        )
    result["alternative_titles"] = alternatives
    return result


def _fetch_work_collections(cur, tenant_id: str, work_id: str) -> List[Dict[str, Any]]:
    try:
        cur.execute(
            """
            SELECT id, parent_collection_id, collection_type, title_type,
                   title_element_level, no_prefix, collection_title,
                   collection_subtitle, collection_number, volume_number,
                   part_number, sequence_type, sequence_number, is_primary,
                   item_order
            FROM work_collections
            WHERE tenant_id = %s AND work_id = %s
            ORDER BY is_primary DESC, item_order, created_at, id
            """,
            (tenant_id, work_id),
        )
        rows = cur.fetchall() or []
    except Exception:
        return []

    return [
        {
            "id": str(row["id"]),
            "parent_collection_id": str(row["parent_collection_id"]) if row.get("parent_collection_id") else None,
            "collection_type": _safe_str(row.get("collection_type")),
            "collectionType": _safe_str(row.get("collection_type")),
            "title_type": _safe_str(row.get("title_type")),
            "titleType": _safe_str(row.get("title_type")),
            "title_element_level": _safe_str(row.get("title_element_level")),
            "titleElementLevel": _safe_str(row.get("title_element_level")),
            "no_prefix": bool(row.get("no_prefix")),
            "noPrefix": bool(row.get("no_prefix")),
            "title": _safe_str(row.get("collection_title")),
            "collection_title": _safe_str(row.get("collection_title")),
            "collectionTitle": _safe_str(row.get("collection_title")),
            "subtitle": _safe_str(row.get("collection_subtitle")),
            "collection_subtitle": _safe_str(row.get("collection_subtitle")),
            "collectionSubtitle": _safe_str(row.get("collection_subtitle")),
            "collection_number": _safe_str(row.get("collection_number")),
            "collectionNumber": _safe_str(row.get("collection_number")),
            "volume_number": _safe_str(row.get("volume_number")),
            "volumeNumber": _safe_str(row.get("volume_number")),
            "part_number": _safe_str(row.get("part_number")),
            "partNumber": _safe_str(row.get("part_number")),
            "sequence_type": _safe_str(row.get("sequence_type")),
            "sequence_number": _safe_str(row.get("sequence_number")),
            "is_primary": bool(row.get("is_primary")),
            "item_order": int(row.get("item_order") or 0),
        }
        for row in rows
    ]
