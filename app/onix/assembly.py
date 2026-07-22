# app/onix/assembly.py
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
        raise ValueError(f"Unknown tenant_slug: {tenant_slug}")
    return str(row["id"])


def _contributor_display_name(raw: Any) -> str:
    s = _norm(raw)
    if not s:
        return ""
    if s.startswith("{") or s.startswith("["):
        return ""
    if "'name'" in s or '"name"' in s or "': '" in s:
        return ""
    return s[:200].strip() if len(s) > 200 else s


def _contributors_summary_for_work(cur, tenant_id: str, work_id: str) -> str:
    cur.execute(
        """
        SELECT p.display_name, wc.contributor_role
        FROM work_contributors wc
        JOIN parties p
          ON p.id = wc.party_id
         AND p.tenant_id = wc.tenant_id
        WHERE wc.tenant_id = %s
          AND wc.work_id = %s
        ORDER BY wc.sequence_number ASC
        LIMIT 5
        """,
        (tenant_id, work_id),
    )
    rows = cur.fetchall() or []
    names = [_contributor_display_name(r.get("display_name")) for r in rows]
    return "; ".join(n for n in names if n)


def _latest_raw_import_at(cur, tenant_id: str, isbn13_norm: str) -> Optional[str]:
    if not isbn13_norm:
        return None

    try:
        cur.execute(
            """
            SELECT MAX(created_at) AS ts
            FROM onix_raw_products
            WHERE tenant_id = %s
              AND normalize_isbn(isbn13) = normalize_isbn(%s)
            """,
            (tenant_id, isbn13_norm),
        )
        row = cur.fetchone()
        ts = row.get("ts") if row else None
        return ts.isoformat() if ts else None
    except Exception:
        return None


def _display_title_for_listing(work_title: str, series_title: str, subtitle: str) -> str:
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


def _publication_date_from_row(ed: Dict[str, Any]) -> str:
    pub_date = ed.get("publication_date")
    if pub_date:
        try:
            return pub_date.isoformat()
        except Exception:
            return _norm(pub_date)

    work_pub = ed.get("work_pub_date")
    if work_pub:
        try:
            return work_pub.isoformat()
        except Exception:
            return _norm(work_pub)

    return ""


def _map_format_to_onix_product_form(product_form: str, product_form_detail: str) -> str:
    pf = _norm(product_form)
    if pf:
        return pf

    f = _norm(product_form_detail).lower()
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


def _is_digital_product(product_form: str, product_form_detail: str) -> bool:
    pf = _norm(product_form).upper()
    detail = _norm(product_form_detail).lower()

    if pf in {"DG", "AJ"}:
        return True

    return any(
        x in detail
        for x in ["ebook", "e-book", "epub", "kindle", "digital", "audiobook", "audio book", "audio"]
    )


def _title_fields_for_payload(ed: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "title": _norm(ed.get("work_title") or ""),
        "subtitle": _norm(ed.get("work_subtitle") or ""),
        "series_title": _norm(ed.get("series_title") or ""),
        "series_number": int(ed.get("series_number") or 0),
    }


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
    """List exportable products, one row per edition/ISBN, from normalized tables."""
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
                conditions.append(
                    "(w.title ILIKE %s OR w.subtitle ILIKE %s OR w.series_title ILIKE %s)"
                )
                t = f"%{_norm(title)}%"
                params.extend([t, t, t])

            if contributor:
                conditions.append(
                    """
                    EXISTS (
                        SELECT 1
                        FROM work_contributors wc
                        JOIN parties p
                          ON p.id = wc.party_id
                         AND p.tenant_id = wc.tenant_id
                        WHERE wc.tenant_id = w.tenant_id
                          AND wc.work_id = w.id
                          AND p.display_name ILIKE %s
                    )
                    """
                )
                params.append(f"%{_norm(contributor)}%")

            if format_filter:
                conditions.append(
                    "(e.product_form ILIKE %s OR e.product_form_detail ILIKE %s)"
                )
                ff = f"%{_norm(format_filter)}%"
                params.extend([ff, ff])

            if status_filter:
                conditions.append("e.status = %s")
                params.append(_norm(status_filter))

            if q:
                conditions.append(
                    """
                    (
                        w.title ILIKE %s OR
                        w.subtitle ILIKE %s OR
                        w.series_title ILIKE %s OR
                        e.isbn13 ILIKE %s OR
                        EXISTS (
                            SELECT 1
                            FROM work_contributors wc
                            JOIN parties p
                              ON p.id = wc.party_id
                             AND p.tenant_id = wc.tenant_id
                            WHERE wc.tenant_id = w.tenant_id
                              AND wc.work_id = w.id
                              AND p.display_name ILIKE %s
                        )
                    )
                    """
                )
                ql = f"%{_norm(q)}%"
                params.extend([ql, ql, ql, ql, ql])

            where_sql = " AND ".join(conditions)

            if sort == "isbn":
                order_sql = "e.isbn13"
            elif sort == "pub_date":
                order_sql = "COALESCE(e.publication_date, w.publication_date) DESC NULLS LAST, e.isbn13"
            elif sort == "updated":
                order_sql = "COALESCE(e.updated_at, w.updated_at) DESC NULLS LAST, e.isbn13"
            else:
                order_sql = (
                    "CASE WHEN COALESCE(w.series_title, '') = '' "
                    "THEN COALESCE(w.title, '') ELSE COALESCE(w.series_title, '') END, "
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
                    e.updated_at,
                    w.title,
                    w.subtitle,
                    w.series_title,
                    w.series_number,
                    w.publisher_or_imprint,
                    w.publisher_name,
                    w.imprint_name,
                    w.publication_date AS work_pub_date,
                    w.updated_at AS work_updated_at
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
                contrib_summary = _contributors_summary_for_work(cur, tenant_id, str(r["work_id"]))

                pub_date = ""
                if r.get("publication_date"):
                    try:
                        pub_date = r["publication_date"].isoformat()
                    except Exception:
                        pub_date = _norm(r.get("publication_date"))
                if not pub_date and r.get("work_pub_date"):
                    try:
                        pub_date = r["work_pub_date"].isoformat()
                    except Exception:
                        pub_date = _norm(r.get("work_pub_date"))

                product_form = _map_format_to_onix_product_form(
                    r.get("product_form"),
                    r.get("product_form_detail"),
                )

                work_title = _norm(r.get("title") or "")
                work_subtitle = _norm(r.get("subtitle") or "")
                work_series_title = _norm(r.get("series_title") or "")

                is_digital = _is_digital_product(product_form, _norm(r.get("product_form_detail") or ""))
                inventory_number = ""
                if not is_digital:
                    inventory_number = _norm(r.get("inventory_number") or "0") or "0"

                items.append(
                    {
                        "edition_id": str(r["edition_id"]),
                        "work_id": str(r["work_id"]),
                        "isbn13": _norm(r.get("isbn13") or ""),
                        "record_reference": _norm(r.get("record_reference") or ""),
                        "title": work_title,
                        "subtitle": work_subtitle,
                        "series_title": work_series_title,
                        "series_number": int(r.get("series_number") or 0),
                        "display_title": _display_title_for_listing(
                            work_title, work_series_title, work_subtitle
                        ),
                        "contributors_summary": contrib_summary,
                        "product_form": product_form or _norm(r.get("product_form_detail") or ""),
                        "product_form_detail": _norm(r.get("product_form_detail") or ""),
                        "publisher_or_imprint": _norm(
                            r.get("publisher_or_imprint")
                            or r.get("publisher_name")
                            or r.get("imprint_name")
                            or ""
                        ),
                        "publication_date": pub_date or None,
                        "publishing_status": _norm(r.get("publishing_status") or ""),
                        "inventory_number": inventory_number,
                        "status": _norm(r.get("status") or ""),
                        "validation_status": "unvalidated",
                        "updated_at": r.get("updated_at").isoformat() if r.get("updated_at") else None,
                        "latest_raw_import_at": _latest_raw_import_at(
                            cur, tenant_id, _norm_isbn13(r.get("isbn13") or "")
                        ),
                    }
                )

            return {
                "items": items,
                "total": total,
                "page": page,
                "page_size": page_size,
            }


def get_exportable_product_by_isbn(tenant_slug: str, isbn13: str) -> Optional[Dict[str, Any]]:
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
    if cur is None:
        with db_conn() as conn:
            with conn.cursor(row_factory=dict_row) as cur2:
                return _build_one(cur2, tenant_id, edition_id)

    return _build_one(cur, tenant_id, edition_id)


def _build_one(cur, tenant_id: str, edition_id: str) -> Dict[str, Any]:
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

    title_fields = _title_fields_for_payload(ed)
    title = title_fields["title"]
    subtitle = title_fields["subtitle"]
    series_title = title_fields["series_title"]
    series_number = title_fields["series_number"]

    publisher = _norm(
        ed.get("publisher_or_imprint")
        or ed.get("publisher_name")
        or ed.get("imprint_name")
        or ""
    )
    language = _norm(ed.get("language") or "")
    isbn13 = _norm_isbn13(ed.get("isbn13") or "")
    record_reference = _norm(ed.get("record_reference") or "")

    product_form = _map_format_to_onix_product_form(
        ed.get("product_form"),
        ed.get("product_form_detail"),
    )
    if not product_form:
        product_form = "BC"

    product_form_detail = _norm(ed.get("product_form_detail") or "")
    publication_date = _publication_date_from_row(ed)
    publishing_status = _norm(ed.get("publishing_status") or "")
    if not publishing_status:
        publishing_status = "04"

    is_digital = _is_digital_product(product_form, product_form_detail)

    inventory_number = ""
    if not is_digital:
        inventory_number = _norm(ed.get("inventory_number") or "0") or "0"

    identifiers: List[Dict[str, str]] = []
    if isbn13:
        identifiers.append(
            {
                "id_type": "15",
                "id_type_name": "ISBN-13",
                "id_value": isbn13,
            }
        )

    if inventory_number:
        identifiers.append(
            {
                "id_type": "01",
                "id_type_name": "Proprietary",
                "id_value": inventory_number,
            }
        )

    try:
        cur.execute(
            """
            SELECT id_type, id_type_name, id_value
            FROM edition_identifiers
            WHERE tenant_id = %s AND edition_id = %s
            """,
            (tenant_id, edition_id),
        )
        for r in cur.fetchall() or []:
            id_value = _norm(r.get("id_value") or "")
            if id_value:
                identifiers.append(
                    {
                        "id_type": _norm(r.get("id_type") or ""),
                        "id_type_name": _norm(r.get("id_type_name") or ""),
                        "id_value": id_value,
                    }
                )
    except Exception:
        pass

    contributors: List[Dict[str, Any]] = []
    cur.execute(
        """
        SELECT wc.contributor_role, wc.sequence_number, p.display_name, p.person_name_inverted
        FROM work_contributors wc
        JOIN parties p
          ON p.id = wc.party_id
         AND p.tenant_id = wc.tenant_id
        WHERE wc.tenant_id = %s
          AND wc.work_id = %s
        ORDER BY wc.sequence_number ASC
        """,
        (tenant_id, work_id),
    )
    for r in cur.fetchall() or []:
        role = _norm(r.get("contributor_role") or "")
        role_upper = role.upper()

        if role_upper in ("A01", "AUTHOR"):
            role = "A01"
        elif role_upper in ("A12", "ILLUSTRATOR"):
            role = "A12"

        name = _contributor_display_name(
            r.get("display_name") or r.get("person_name_inverted") or ""
        )
        if not name:
            continue

        contributors.append(
            {
                "role": role or "A01",
                "sequence_number": int(r.get("sequence_number") or 1),
                "name": name,
            }
        )

    subjects: List[Dict[str, str]] = []
    try:
        cur.execute(
            """
            SELECT scheme_id, subject_code, heading_text
            FROM edition_subjects
            WHERE tenant_id = %s AND edition_id = %s
            """,
            (tenant_id, edition_id),
        )
        for r in cur.fetchall() or []:
            heading_text = _norm(r.get("heading_text") or "")
            subject_code = _norm(r.get("subject_code") or "")
            scheme_id = _norm(r.get("scheme_id") or "")
            if heading_text or subject_code:
                subjects.append(
                    {
                        "scheme_id": scheme_id,
                        "subject_code": subject_code,
                        "heading_text": heading_text,
                    }
                )
    except Exception:
        pass

    texts: List[Dict[str, str]] = []
    try:
        cur.execute(
            """
            SELECT text_type, text_value
            FROM edition_texts
            WHERE tenant_id = %s AND edition_id = %s
            """,
            (tenant_id, edition_id),
        )
        for r in cur.fetchall() or []:
            text_type = _norm(r.get("text_type") or "")
            text_value = _norm(r.get("text_value") or "")
            if text_value:
                texts.append(
                    {
                        "text_type": text_type,
                        "text_value": text_value,
                    }
                )
    except Exception:
        pass

    main_description = _norm(ed.get("main_description") or "")
    if main_description and not texts:
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
    for sd in cur.fetchall() or []:
        sd_id = sd.get("id")
        prices: List[Dict[str, Any]] = []

        cur.execute(
            """
            SELECT price_type_code, price_amount, currency_code
            FROM edition_prices
            WHERE tenant_id = %s
              AND supply_detail_id = %s
            """,
            (tenant_id, sd_id),
        )
        for pr in cur.fetchall() or []:
            prices.append(
                {
                    "price_type_code": _norm(pr.get("price_type_code") or "01"),
                    "price_amount": float(pr["price_amount"]) if pr.get("price_amount") is not None else None,
                    "currency_code": _norm(pr.get("currency_code") or "USD"),
                }
            )

        supply_details.append(
            {
                "supplier_name": _norm(sd.get("supplier_name") or ""),
                "product_availability": _norm(sd.get("product_availability") or ""),
                "on_sale_date": sd.get("on_sale_date").isoformat() if sd.get("on_sale_date") else "",
                "prices": prices,
            }
        )

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
        "extras": {},
    }


def build_onix_message_payload(tenant_id: str, edition_ids: List[str], cur=None) -> Dict[str, Any]:
    products: List[Dict[str, Any]] = []

    if cur is None:
        with db_conn() as conn:
            with conn.cursor(row_factory=dict_row) as cur2:
                for eid in edition_ids:
                    product = _build_one(cur2, tenant_id, eid)
                    if product:
                        products.append(product)
    else:
        for eid in edition_ids:
            product = _build_one(cur, tenant_id, eid)
            if product:
                products.append(product)

    return {
        "release": "3.0",
        "products": products,
    }