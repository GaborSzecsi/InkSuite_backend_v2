# app/onix/assembly.py
# Canonical ONIX assembly from editions + works + edition_* tables. No raw XML as source.

from __future__ import annotations

from typing import Any, Dict, List, Optional

from app.core.db import db_conn
from psycopg.rows import dict_row


def _norm(v: Any) -> str:
    if v is None:
        return ""
    return str(v).strip()


def _norm_isbn13(val: Any) -> str:
    if val is None:
        return ""
    s = str(val).strip().replace("-", "").replace(" ", "").upper()
    return s[:17] if s else ""


def _json_obj(val: Any) -> Dict[str, Any]:
    if isinstance(val, dict):
        return val
    if not val:
        return {}
    try:
        import json
        parsed = json.loads(val) if isinstance(val, str) else {}
        return parsed if isinstance(parsed, dict) else {}
    except Exception:
        return {}


def _get_tenant_id_from_slug(cur, tenant_slug: str) -> str:
    cur.execute("SELECT id FROM tenants WHERE lower(slug) = lower(%s) LIMIT 1", (tenant_slug.strip(),))
    row = cur.fetchone()
    if not row:
        cur.execute("SELECT id FROM tenants ORDER BY id LIMIT 1")
        row = cur.fetchone()
    if not row:
        raise ValueError(f"Unknown tenant_slug: {tenant_slug}")
    return str(row["id"])


def _contributor_display_name(raw: Any) -> str:
    """Use only plain name strings; skip values that look like serialized dicts/objects."""
    s = _norm(raw)
    if not s:
        return ""
    if s.startswith("{") or s.startswith("["):
        return ""
    if "'name'" in s or '"name"' in s or "': '" in s:
        return ""
    if len(s) > 200:
        return s[:200].strip()
    return s


def _contributors_summary_for_work(cur, tenant_id: str, work_id: str) -> str:
    cur.execute(
        """
        SELECT p.display_name, wc.contributor_role
        FROM work_contributors wc
        JOIN parties p ON p.id = wc.party_id
        WHERE wc.work_id = %s AND p.tenant_id = %s
        ORDER BY wc.sequence_number ASC
        LIMIT 5
        """,
        (work_id, tenant_id),
    )
    rows = cur.fetchall() or []
    names = [_contributor_display_name(r.get("display_name")) for r in rows]
    return "; ".join(n for n in names if n)


def _latest_raw_import_at(cur, tenant_id: str, isbn13_norm: str) -> Optional[str]:
    if not isbn13_norm:
        return None
    cur.execute(
        """
        SELECT MAX(created_at) AS ts
        FROM onix_raw_products
        WHERE tenant_id = %s AND normalize_isbn(isbn13) = normalize_isbn(%s)
        """,
        (tenant_id, isbn13_norm),
    )
    row = cur.fetchone()
    ts = row.get("ts") if row else None
    return ts.isoformat() if ts else None


def _display_title_for_listing(work_title: str, series_title: str, subtitle: str) -> str:
    """
    Listing/display rule:
    - series books: Series Title: Subtitle
    - standalone books: Title
    """
    wt = _norm(work_title)
    st = _norm(series_title)
    sub = _norm(subtitle)

    if st and sub:
        return f"{st}: {sub}"
    if wt:
        return wt
    if sub:
        return sub
    return st


def _publication_date_from_row(ed: Dict[str, Any], se: Dict[str, Any]) -> str:
    """
    Prefer normalized edition.publication_date, then legacy source_extras.pub_date,
    then work publication date.
    """
    pub_date = ed.get("publication_date")
    if pub_date:
        try:
            return pub_date.isoformat()
        except Exception:
            return _norm(pub_date)

    legacy_pub = _norm(se.get("pub_date") or se.get("publication_date") or "")
    if legacy_pub:
        return legacy_pub

    work_pub = ed.get("work_pub_date")
    if work_pub:
        try:
            return work_pub.isoformat()
        except Exception:
            return _norm(work_pub)

    return ""


def _map_legacy_format_to_onix_product_form(fmt: str) -> str:
    """
    Conservative mapping from legacy/source format strings to ONIX ProductForm.
    """
    f = _norm(fmt).lower()
    if not f:
        return ""

    if "ebook" in f or "e-book" in f or "epub" in f or "kindle" in f or "digital" in f:
        return "DG"
    if "audiobook" in f or "audio book" in f or "audio" in f:
        return "AJ"
    if "hardcover" in f or "hardback" in f:
        return "BB"
    if "paperback" in f or "softcover" in f or "soft cover" in f or "trade paper" in f or "large print" in f:
        return "BC"
    if "board" in f:
        return "BB"

    return ""


def _is_digital_product(product_form: str, legacy_format: str) -> bool:
    pf = _norm(product_form).upper()
    lf = _norm(legacy_format).lower()

    if pf in {"DG", "AJ"}:
        return True

    if any(x in lf for x in ["ebook", "e-book", "epub", "kindle", "digital", "audiobook", "audio book", "audio"]):
        return True

    return False


def _title_fields_for_payload(ed: Dict[str, Any]) -> Dict[str, Any]:
    """
    Preserve both standalone-book and series-book semantics.

    Standalone books:
      title = works.title
      subtitle = works.subtitle (optional)

    Series books:
      title = works.title
      subtitle = works.subtitle
      series_title = works.series_title
      series_number = works.series_number
    """
    work_title = _norm(ed.get("work_title") or "")
    work_subtitle = _norm(ed.get("work_subtitle") or "")
    series_title = _norm(ed.get("series_title") or "")
    series_number = int(ed.get("series_number") or 0)

    return {
        "title": work_title,
        "subtitle": work_subtitle,
        "series_title": series_title,
        "series_number": series_number,
    }


def _legacy_price_rows(se: Dict[str, Any]) -> List[Dict[str, Any]]:
    prices: List[Dict[str, Any]] = []

    price_us = _norm(se.get("price_us") or "")
    price_can = _norm(se.get("price_can") or "")

    try:
        if price_us not in ("", "0", "0.0", "0.00"):
            prices.append({
                "price_type_code": "01",
                "price_amount": float(price_us),
                "currency_code": "USD",
            })
    except Exception:
        pass

    try:
        if price_can not in ("", "0", "0.0", "0.00"):
            prices.append({
                "price_type_code": "01",
                "price_amount": float(price_can),
                "currency_code": "CAD",
            })
    except Exception:
        pass

    return prices


def list_exportable_products(
    tenant_slug: str,
    q: Optional[str] = None,
    isbn: Optional[str] = None,
    title: Optional[str] = None,
    contributor: Optional[str] = None,
    format_filter: Optional[str] = None,
    status_filter: Optional[str] = None,
    page: int = 1,
    page_size: int = 50,
    sort: str = "title",
) -> Dict[str, Any]:
    """List exportable products (one row per edition/ISBN)."""
    with db_conn() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            tenant_id = _get_tenant_id_from_slug(cur, tenant_slug)
            offset = (page - 1) * page_size

            conditions = ["e.tenant_id = %s"]
            params: List[Any] = [tenant_id]

            if isbn:
                conditions.append("normalize_isbn(e.isbn13) = normalize_isbn(%s)")
                params.append(_norm_isbn13(isbn))

            if title:
                conditions.append("(w.title ILIKE %s OR w.subtitle ILIKE %s OR w.series_title ILIKE %s)")
                t = f"%{_norm(title)}%"
                params.extend([t, t, t])

            if contributor:
                conditions.append(
                    "EXISTS (SELECT 1 FROM work_contributors wc "
                    "JOIN parties p ON p.id = wc.party_id "
                    "WHERE wc.work_id = w.id AND p.tenant_id = w.tenant_id AND p.display_name ILIKE %s)"
                )
                params.append(f"%{_norm(contributor)}%")

            if format_filter:
                conditions.append(
                    "(e.product_form ILIKE %s OR e.product_form_detail ILIKE %s OR e.source_extras->>'format' ILIKE %s)"
                )
                ff = f"%{_norm(format_filter)}%"
                params.extend([ff, ff, ff])

            if status_filter:
                conditions.append("e.status = %s")
                params.append(_norm(status_filter))

            if q:
                conditions.append(
                    "("
                    "w.title ILIKE %s OR "
                    "w.subtitle ILIKE %s OR "
                    "w.series_title ILIKE %s OR "
                    "e.isbn13 ILIKE %s OR "
                    "EXISTS (SELECT 1 FROM work_contributors wc "
                    "JOIN parties p ON p.id = wc.party_id "
                    "WHERE wc.work_id = w.id AND p.tenant_id = w.tenant_id AND p.display_name ILIKE %s)"
                    ")"
                )
                ql = f"%{_norm(q)}%"
                params.extend([ql, ql, ql, ql, ql])

            where_sql = " AND ".join(conditions)

            if sort == "isbn":
                order_sql = "e.isbn13"
            elif sort == "pub_date":
                order_sql = "e.publication_date DESC NULLS LAST, e.isbn13"
            elif sort == "updated":
                order_sql = "e.updated_at DESC NULLS LAST, e.isbn13"
            else:
                order_sql = (
                    "CASE WHEN COALESCE(w.series_title, '') = '' THEN COALESCE(w.title, '') ELSE COALESCE(w.series_title, '') END, "
                    "NULLIF(w.series_number, 0) NULLS LAST, "
                    "COALESCE(w.subtitle, ''), "
                    "e.isbn13"
                )

            cur.execute(
                f"""
                SELECT
                    e.id AS edition_id,
                    e.work_id,
                    e.isbn13,
                    e.record_reference,
                    e.product_form,
                    e.product_form_detail,
                    e.publication_date,
                    e.publishing_status,
                    e.status,
                    e.inventory_number,
                    e.source_extras,
                    e.updated_at,
                    w.title,
                    w.subtitle,
                    w.series_title,
                    w.series_number,
                    w.publisher_or_imprint,
                    w.publisher_name,
                    w.imprint_name
                FROM editions e
                JOIN works w
                  ON w.id = e.work_id
                 AND w.tenant_id = e.tenant_id
                WHERE {where_sql}
                ORDER BY {order_sql}
                LIMIT %s OFFSET %s
                """,
                params + [page_size, offset],
            )
            rows = cur.fetchall() or []

            cur.execute(
                f"""
                SELECT COUNT(*) AS n
                FROM editions e
                JOIN works w
                  ON w.id = e.work_id
                 AND w.tenant_id = e.tenant_id
                WHERE {where_sql}
                """,
                params,
            )
            total = int((cur.fetchone() or {}).get("n") or 0)

            items: List[Dict[str, Any]] = []
            for r in rows:
                se = _json_obj(r.get("source_extras"))
                contrib_summary = _contributors_summary_for_work(cur, tenant_id, str(r["work_id"]))

                pub_date = ""
                if r.get("publication_date"):
                    try:
                        pub_date = r["publication_date"].isoformat()
                    except Exception:
                        pub_date = _norm(r.get("publication_date"))
                if not pub_date:
                    pub_date = _norm(se.get("pub_date") or se.get("publication_date") or "")

                product_form = _norm(r.get("product_form") or "")
                if not product_form:
                    product_form = _map_legacy_format_to_onix_product_form(se.get("format"))

                work_title = _norm(r.get("title") or "")
                work_subtitle = _norm(r.get("subtitle") or "")
                work_series_title = _norm(r.get("series_title") or "")

                is_digital = _is_digital_product(product_form, _norm(se.get("format") or ""))
                inventory_number = ""
                if not is_digital:
                    inventory_number = _norm(r.get("inventory_number") or se.get("loc_number") or "0") or "0"

                items.append({
                    "edition_id": str(r["edition_id"]),
                    "work_id": str(r["work_id"]),
                    "isbn13": _norm(r.get("isbn13") or se.get("isbn13") or ""),
                    "record_reference": _norm(r.get("record_reference") or ""),
                    "title": work_title,
                    "subtitle": work_subtitle,
                    "series_title": work_series_title,
                    "series_number": int(r.get("series_number") or 0),
                    "display_title": _display_title_for_listing(work_title, work_series_title, work_subtitle),
                    "contributors_summary": contrib_summary,
                    "product_form": product_form or _norm(se.get("format") or ""),
                    "product_form_detail": _norm(r.get("product_form_detail") or ""),
                    "publisher_or_imprint": _norm(
                        r.get("publisher_or_imprint") or r.get("publisher_name") or r.get("imprint_name") or ""
                    ),
                    "publication_date": pub_date or None,
                    "publishing_status": _norm(r.get("publishing_status") or ""),
                    "inventory_number": inventory_number,
                    "status": _norm(r.get("status") or ""),
                    "updated_at": r.get("updated_at").isoformat() if r.get("updated_at") else None,
                    "latest_raw_import_at": _latest_raw_import_at(cur, tenant_id, _norm_isbn13(r.get("isbn13") or "")),
                })

            return {
                "items": items,
                "total": total,
                "page": page,
                "page_size": page_size,
            }


def get_exportable_product_by_isbn(tenant_slug: str, isbn13: str) -> Optional[Dict[str, Any]]:
    """Return full exportable product payload for one ISBN, or None."""
    norm = _norm_isbn13(isbn13)
    if not norm:
        return None

    with db_conn() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            tenant_id = _get_tenant_id_from_slug(cur, tenant_slug)
            cur.execute(
                """
                SELECT e.id AS edition_id, e.work_id
                FROM editions e
                WHERE e.tenant_id = %s
                  AND normalize_isbn(e.isbn13) = normalize_isbn(%s)
                LIMIT 1
                """,
                (tenant_id, norm),
            )
            row = cur.fetchone()
            if not row:
                return None
            return build_onix_product_payload(tenant_id, str(row["edition_id"]), cur=cur)


def build_onix_product_payload(
    tenant_id: str,
    edition_id: str,
    cur=None,
) -> Dict[str, Any]:
    """Build canonical ONIX product dict for one edition (for XML serialization)."""
    import json

    if cur is None:
        with db_conn() as conn:
            with conn.cursor(row_factory=dict_row) as cur2:
                return _build_one(cur2, tenant_id, edition_id, json)

    return _build_one(cur, tenant_id, edition_id, json)


def _build_one(cur, tenant_id: str, edition_id: str, json_mod) -> Dict[str, Any]:
    cur.execute(
        """
        SELECT
            e.*,
            w.title AS work_title,
            w.subtitle AS work_subtitle,
            w.series_title,
            w.series_number,
            w.publisher_name,
            w.imprint_name,
            w.publisher_or_imprint,
            w.language,
            w.publication_date AS work_pub_date,
            w.main_description,
            w.biographical_note,
            w.cover_image_link AS work_cover_link
        FROM editions e
        JOIN works w
          ON w.id = e.work_id
         AND w.tenant_id = e.tenant_id
        WHERE e.tenant_id = %s
          AND e.id = %s
        LIMIT 1
        """,
        (tenant_id, edition_id),
    )
    ed = cur.fetchone()
    if not ed:
        return {}

    work_id = str(ed["work_id"])
    se = _json_obj(ed.get("source_extras"))

    title_fields = _title_fields_for_payload(ed)
    title = title_fields["title"]
    subtitle = title_fields["subtitle"]
    series_title = title_fields["series_title"]
    series_number = title_fields["series_number"]

    publisher = _norm(
        ed.get("publisher_or_imprint") or ed.get("publisher_name") or ed.get("imprint_name") or ""
    )
    language = _norm(ed.get("language") or "")
    isbn13 = _norm_isbn13(ed.get("isbn13") or "")
    record_reference = _norm(ed.get("record_reference") or "")

    product_form = _norm(ed.get("product_form") or "")
    if not product_form:
        product_form = _map_legacy_format_to_onix_product_form(se.get("format"))
    if not product_form:
        product_form = "BC"

    product_form_detail = _norm(ed.get("product_form_detail") or "")
    publication_date = _publication_date_from_row(ed, se)
    publishing_status = _norm(ed.get("publishing_status") or "")
    if not publishing_status:
        publishing_status = "04"

    legacy_format = _norm(se.get("format") or "")
    is_digital = _is_digital_product(product_form, legacy_format)

    inventory_number = ""
    if not is_digital:
        inventory_number = _norm(ed.get("inventory_number") or se.get("loc_number") or "0") or "0"

    identifiers: List[Dict[str, str]] = []
    if isbn13:
        identifiers.append({
            "id_type": "15",
            "id_type_name": "ISBN-13",
            "id_value": isbn13,
        })

    # Only physical products get inventory / locator / proprietary number
    if inventory_number:
        identifiers.append({
            "id_type": "01",
            "id_type_name": "Proprietary",
            "id_value": inventory_number,
        })

    cur.execute(
        """
        SELECT id_type, id_type_name, id_value
        FROM edition_identifiers
        WHERE tenant_id = %s AND edition_id = %s
        """,
        (tenant_id, edition_id),
    )
    for r in (cur.fetchall() or []):
        id_value = _norm(r.get("id_value") or "")
        if id_value:
            identifiers.append({
                "id_type": _norm(r.get("id_type") or ""),
                "id_type_name": _norm(r.get("id_type_name") or ""),
                "id_value": id_value,
            })

    contributors: List[Dict[str, Any]] = []
    cur.execute(
        """
        SELECT wc.contributor_role, wc.sequence_number, p.display_name, p.person_name_inverted
        FROM work_contributors wc
        JOIN parties p ON p.id = wc.party_id
        WHERE wc.work_id = %s AND p.tenant_id = %s
        ORDER BY wc.sequence_number ASC
        """,
        (work_id, tenant_id),
    )
    for r in (cur.fetchall() or []):
        role = _norm(r.get("contributor_role") or "")
        role_upper = role.upper()

        if role_upper in ("A01", "AUTHOR"):
            role = "A01"
        elif role_upper in ("A12", "ILLUSTRATOR"):
            role = "A12"

        raw_name = r.get("display_name") or r.get("person_name_inverted") or ""
        if isinstance(raw_name, dict):
            name = _contributor_display_name(raw_name.get("display_name") or raw_name.get("name") or "")
        else:
            name = _contributor_display_name(str(raw_name).strip())

        if not name:
            continue

        contributors.append({
            "role": role or "A01",
            "sequence_number": int(r.get("sequence_number") or 1),
            "name": name,
        })

    subjects: List[Dict[str, str]] = []
    cur.execute(
        """
        SELECT scheme_id, subject_code, heading_text
        FROM edition_subjects
        WHERE tenant_id = %s AND edition_id = %s
        """,
        (tenant_id, edition_id),
    )
    for r in (cur.fetchall() or []):
        heading_text = _norm(r.get("heading_text") or "")
        subject_code = _norm(r.get("subject_code") or "")
        scheme_id = _norm(r.get("scheme_id") or "")
        if heading_text or subject_code:
            subjects.append({
                "scheme_id": scheme_id,
                "subject_code": subject_code,
                "heading_text": heading_text,
            })

    texts: List[Dict[str, str]] = []
    cur.execute(
        """
        SELECT text_type, text_value
        FROM edition_texts
        WHERE tenant_id = %s AND edition_id = %s
        """,
        (tenant_id, edition_id),
    )
    for r in (cur.fetchall() or []):
        text_type = _norm(r.get("text_type") or "")
        text_value = _norm(r.get("text_value") or "")
        if text_value:
            texts.append({
                "text_type": text_type,
                "text_value": text_value,
            })

    main_description = _norm(ed.get("main_description") or "")
    if main_description and not any(_norm(t.get("text_type")) for t in texts):
        texts.insert(0, {"text_type": "Main Description", "text_value": main_description})

    bio_note = _norm(ed.get("biographical_note") or "")
    if bio_note and not any(_norm(t.get("text_type")) == "Biographical Note" for t in texts):
        texts.append({"text_type": "Biographical Note", "text_value": bio_note})

    supply_details: List[Dict[str, Any]] = []
    cur.execute(
        """
        SELECT id, supplier_name, product_availability, on_sale_date
        FROM edition_supply_details
        WHERE tenant_id = %s AND edition_id = %s
        """,
        (tenant_id, edition_id),
    )
    for sd in (cur.fetchall() or []):
        sd_id = sd.get("id")
        prices: List[Dict[str, Any]] = []

        cur.execute(
            """
            SELECT price_type_code, price_amount, currency_code
            FROM edition_prices
            WHERE supply_detail_id = %s
            """,
            (sd_id,),
        )
        for pr in (cur.fetchall() or []):
            prices.append({
                "price_type_code": _norm(pr.get("price_type_code") or "01"),
                "price_amount": float(pr["price_amount"]) if pr.get("price_amount") is not None else None,
                "currency_code": _norm(pr.get("currency_code") or "USD"),
            })

        # Fallback to legacy source_extras prices if edition_prices is empty
        if not prices:
            prices = _legacy_price_rows(se)

        supply_details.append({
            "supplier_name": _norm(sd.get("supplier_name") or ""),
            "product_availability": _norm(sd.get("product_availability") or ""),
            "on_sale_date": sd.get("on_sale_date").isoformat() if sd.get("on_sale_date") else "",
            "prices": prices,
        })

    # If there are no edition_supply_details rows at all, still emit a minimal supply block from legacy prices.
    if not supply_details:
        legacy_prices = _legacy_price_rows(se)
        if legacy_prices:
            supply_details.append({
                "supplier_name": "",
                "product_availability": "",
                "on_sale_date": "",
                "prices": legacy_prices,
            })

    # Keep current DB-based cover fallback, but prefer edition over work.
    cover_link = _norm(ed.get("cover_image_link") or ed.get("work_cover_link") or "")

    return {
        "record_reference": record_reference or (isbn13 or edition_id),
        "identifiers": identifiers,
        "title": title,
        "subtitle": subtitle,
        "series_title": series_title,
        "series_number": series_number,
        "contributors": contributors,
        "publisher_name": publisher,
        "language": language or "eng",
        "product_form": product_form,
        "product_form_detail": product_form_detail,
        "publication_date": publication_date,
        "publishing_status": publishing_status,
        "subjects": subjects,
        "texts": texts,
        "supply_details": supply_details,
        "cover_image_link": cover_link,
        "number_of_pages": ed.get("number_of_pages"),
        "inventory_number": inventory_number,
        "edition_id": edition_id,
        "work_id": work_id,
        "extras": se,
    }


def build_onix_message_payload(tenant_id: str, edition_ids: List[str], cur=None) -> Dict[str, Any]:
    """Build ONIX message with multiple products."""
    import json

    products: List[Dict[str, Any]] = []

    if cur is None:
        with db_conn() as conn:
            with conn.cursor(row_factory=dict_row) as cur2:
                for eid in edition_ids:
                    p = _build_one(cur2, tenant_id, eid, json)
                    if p:
                        products.append(p)
    else:
        for eid in edition_ids:
            p = _build_one(cur, tenant_id, eid, json)
            if p:
                products.append(p)

    return {
        "release": "3.0",
        "products": products,
    }