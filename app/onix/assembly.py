# app/onix/assembly.py
# InkSuite normalized metadata -> canonical ONIX product/message payload.
#
# Design rule:
#   - metadata values come from the database
#   - no product metadata defaults are invented here
#   - optional normalized tables are discovered safely
#   - top-level compatibility aliases are retained for the existing router/UI
#     while the authoritative export structure lives in the ONIX composites

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
import re
from typing import Any, Dict, Iterable, List, Optional
from uuid import UUID

from app.core.db import db_conn
from psycopg.rows import dict_row


def _norm(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _norm_isbn(value: Any) -> str:
    return _norm(value).replace("-", "").replace(" ", "").upper()


def _jsonable(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, dict):
        return {str(k): _jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable(v) for v in value]
    return value


def _clean_row(row: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    if not row:
        return {}
    return {str(k): _jsonable(v) for k, v in dict(row).items()}


def _get_tenant_id_from_slug(cur, tenant_slug: str) -> str:
    cur.execute(
        "SELECT id FROM tenants WHERE lower(slug) = lower(%s) LIMIT 1",
        (tenant_slug.strip(),),
    )
    row = cur.fetchone()
    if not row:
        raise ValueError(f"Unknown tenant_slug: {tenant_slug}")
    return str(row["id"])


def _table_exists(cur, table_name: str) -> bool:
    cur.execute(
        """
        SELECT EXISTS (
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = 'public'
              AND table_name = %s
        ) AS ok
        """,
        (table_name,),
    )
    row = cur.fetchone() or {}
    return bool(row.get("ok"))


def _column_exists(cur, table_name: str, column_name: str) -> bool:
    cur.execute(
        """
        SELECT EXISTS (
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name = %s
              AND column_name = %s
        ) AS ok
        """,
        (table_name, column_name),
    )
    row = cur.fetchone() or {}
    return bool(row.get("ok"))


def _fetch_rows(
    cur,
    table_name: str,
    *,
    tenant_id: str,
    key_column: str,
    key_value: Any,
    extra_where: str = "",
    extra_params: Optional[List[Any]] = None,
) -> List[Dict[str, Any]]:
    if not _table_exists(cur, table_name):
        return []

    order_parts = []
    if _column_exists(cur, table_name, "item_order"):
        order_parts.append("item_order")
    if _column_exists(cur, table_name, "sequence_number"):
        order_parts.append("sequence_number")
    if _column_exists(cur, table_name, "created_at"):
        order_parts.append("created_at")
    if _column_exists(cur, table_name, "id"):
        order_parts.append("id")

    order_sql = f" ORDER BY {', '.join(order_parts)}" if order_parts else ""
    sql = (
        f"SELECT * FROM {table_name} "
        f"WHERE tenant_id = %s AND {key_column} = %s"
    )
    params: List[Any] = [tenant_id, key_value]

    if extra_where:
        sql += f" AND ({extra_where})"
        params.extend(extra_params or [])

    sql += order_sql
    cur.execute(sql, params)
    return [_clean_row(row) for row in (cur.fetchall() or [])]


def _fetch_one_optional(
    cur,
    table_name: str,
    *,
    tenant_id: str,
    key_column: str,
    key_value: Any,
) -> Dict[str, Any]:
    rows = _fetch_rows(
        cur,
        table_name,
        tenant_id=tenant_id,
        key_column=key_column,
        key_value=key_value,
    )
    return rows[0] if rows else {}


def _first_nonempty(*values: Any) -> str:
    for value in values:
        text = _norm(value)
        if text:
            return text
    return ""


def _display_title_for_listing(work: Dict[str, Any]) -> str:
    title = _norm(work.get("title"))
    subtitle = _norm(work.get("subtitle"))
    series = _norm(work.get("series_title"))
    if series and subtitle:
        return f"{series}: {subtitle}"
    return title or subtitle or series


def _product_form_code(edition: Dict[str, Any]) -> str:
    # Prefer the explicitly stored ONIX ProductForm code.
    explicit = _norm(edition.get("onix_product_form")).upper()
    if explicit:
        return explicit

    # A two-character product_form is already an ONIX code.
    product_form = _norm(edition.get("product_form")).upper()
    if len(product_form) == 2 and product_form.isalnum():
        return product_form

    # Do not infer / manufacture a code from prose during export.
    return ""


def _product_form_detail_code(edition: Dict[str, Any]) -> str:
    """
    Export only an actual ONIX ProductFormDetail code.

    Never serialize friendly UI labels such as "Hardcover" as
    <ProductFormDetail>. Prefer the dedicated ONIX field. Legacy scalar
    product_form_detail is accepted only when it is already code-shaped.
    """
    explicit = _norm(edition.get("onix_product_form_detail")).upper()
    if explicit:
        return explicit

    legacy = _norm(edition.get("product_form_detail")).upper()
    if re.fullmatch(r"[A-Z][A-Z0-9]{2,5}", legacy):
        return legacy

    return ""


def _publication_date_for_listing(publishing_dates: List[Dict[str, Any]]) -> Optional[str]:
    for row in publishing_dates or []:
        if _norm(row.get("date_role")) != "01":
            continue
        value = _norm(row.get("date_text") or row.get("date_value") or "")
        if value:
            return value
    return None


def _contributors_summary(cur, tenant_id: str, work_id: str) -> str:
    if not _table_exists(cur, "work_contributors") or not _table_exists(cur, "parties"):
        return ""
    cur.execute(
        """
        SELECT p.display_name
        FROM work_contributors wc
        JOIN parties p
          ON p.id = wc.party_id
         AND p.tenant_id = wc.tenant_id
        WHERE wc.tenant_id = %s
          AND wc.work_id = %s
        ORDER BY wc.sequence_number NULLS LAST, wc.id
        LIMIT 8
        """,
        (tenant_id, work_id),
    )
    return "; ".join(
        _norm(row.get("display_name"))
        for row in (cur.fetchall() or [])
        if _norm(row.get("display_name"))
    )


def _cover_for_listing(cur, tenant_id: str, edition_id: str, edition: Dict[str, Any], work: Dict[str, Any]) -> str:
    # Cover art is product/edition-level metadata. Never fall back to works.
    direct = _norm(edition.get("cover_image_link"))
    if direct:
        return direct

    if not (
        _table_exists(cur, "edition_supporting_resources")
        and _table_exists(cur, "edition_supporting_resource_versions")
    ):
        return ""

    cur.execute(
        """
        SELECT erv.resource_link
        FROM edition_supporting_resources esr
        JOIN edition_supporting_resource_versions erv
          ON erv.resource_id = esr.id
         AND erv.tenant_id = esr.tenant_id
        WHERE esr.tenant_id = %s
          AND esr.edition_id = %s
          AND esr.resource_content_type = '01'
          AND NULLIF(erv.resource_link, '') IS NOT NULL
        ORDER BY
          CASE WHEN esr.is_primary THEN 0 ELSE 1 END,
          esr.item_order,
          erv.item_order,
          erv.created_at
        LIMIT 1
        """,
        (tenant_id, edition_id),
    )
    row = cur.fetchone() or {}
    return _norm(row.get("resource_link"))


def list_exportable_products(
    tenant_slug: str,
    q: Optional[str] = None,
    isbn: Optional[str] = None,
    title: Optional[str] = None,
    contributor: Optional[str] = None,
    format_filter: Optional[str] = None,
    status_filter: Optional[str] = None,
    publication_from: Optional[str] = None,
    publication_to: Optional[str] = None,
    page: int = 1,
    page_size: int = 50,
    sort: str = "title",
) -> Dict[str, Any]:
    """
    FAST catalogue-list query for the ONIX UI.

    This function intentionally does NOT build canonical ONIX products.
    Full normalized metadata assembly belongs only to detail / preview /
    download / transfer operations.

    The listing is produced with:
      - one count query
      - one result query
    and no per-row database round trips.
    """
    page = max(int(page or 1), 1)
    page_size = max(min(int(page_size or 50), 200), 1)
    offset = (page - 1) * page_size

    with db_conn() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            tenant_id = _get_tenant_id_from_slug(cur, tenant_slug)

            conditions = ["e.tenant_id = %s"]
            params: List[Any] = [tenant_id]

            if isbn:
                conditions.append(
                    "regexp_replace(coalesce(e.isbn13,''), '[^0-9Xx]', '', 'g') = "
                    "regexp_replace(%s, '[^0-9Xx]', '', 'g')"
                )
                params.append(_norm_isbn(isbn))

            if title:
                like = f"%{_norm(title)}%"
                conditions.append(
                    "(w.title ILIKE %s OR w.subtitle ILIKE %s OR w.series_title ILIKE %s)"
                )
                params.extend([like, like, like])

            if contributor:
                conditions.append(
                    """
                    EXISTS (
                        SELECT 1
                        FROM work_contributors wcq
                        JOIN parties pq
                          ON pq.id = wcq.party_id
                         AND pq.tenant_id = wcq.tenant_id
                        WHERE wcq.tenant_id = e.tenant_id
                          AND wcq.work_id = e.work_id
                          AND pq.display_name ILIKE %s
                    )
                    """
                )
                params.append(f"%{_norm(contributor)}%")

            if format_filter:
                fmt = _norm(format_filter).upper()
                aliases = {
                    "BB": [
                        "BB",
                        "HARDCOVER",
                        "HARDBACK",
                        "PAPER OVER BOARDS",
                        "PAPER OVER BOARD",
                        "CASEBOUND",
                        "CASE BOUND",
                    ],
                    "BC": [
                        "BC",
                        "PAPERBACK",
                        "SOFTCOVER",
                        "SOFT COVER",
                        "TRADE PAPER",
                    ],
                    "BH": ["BH", "BOARD BOOK"],
                    "EA": [
                        "EA",
                        "EB",
                        "EC",
                        "ED",
                        "EBOOK",
                        "E-BOOK",
                        "E BOOK",
                        "EPUB",
                        "DIGITAL BOOK",
                    ],
                    "AJ": [
                        "AJ",
                        "AC",
                        "AUDIOBOOK",
                        "AUDIO BOOK",
                        "DOWNLOADABLE AUDIO",
                    ],
                }.get(fmt, [fmt])

                format_clauses: List[str] = []
                for alias in aliases:
                    format_clauses.append(
                        "("
                        "upper(coalesce(e.onix_product_form,'')) = %s "
                        "OR upper(coalesce(e.product_form,'')) = %s "
                        "OR upper(coalesce(e.product_form_detail,'')) LIKE %s"
                        ")"
                    )
                    params.extend([alias, alias, f"%{alias}%"])
                conditions.append("(" + " OR ".join(format_clauses) + ")")

            if status_filter:
                conditions.append("e.status = %s")
                params.append(_norm(status_filter))

            if publication_from:
                conditions.append(
                    "(SELECT pd.date_value FROM edition_publishing_dates pd WHERE pd.tenant_id = e.tenant_id AND pd.edition_id = e.id AND pd.date_role = '01' ORDER BY pd.item_order, pd.created_at, pd.id LIMIT 1) >= %s::date"
                )
                params.append(_norm(publication_from))

            if publication_to:
                conditions.append(
                    "(SELECT pd.date_value FROM edition_publishing_dates pd WHERE pd.tenant_id = e.tenant_id AND pd.edition_id = e.id AND pd.date_role = '01' ORDER BY pd.item_order, pd.created_at, pd.id LIMIT 1) <= %s::date"
                )
                params.append(_norm(publication_to))

            if q:
                like = f"%{_norm(q)}%"
                conditions.append(
                    """
                    (
                        w.title ILIKE %s
                        OR w.subtitle ILIKE %s
                        OR w.series_title ILIKE %s
                        OR e.isbn13 ILIKE %s
                        OR EXISTS (
                            SELECT 1
                            FROM work_contributors wcq
                            JOIN parties pq
                              ON pq.id = wcq.party_id
                             AND pq.tenant_id = wcq.tenant_id
                            WHERE wcq.tenant_id = e.tenant_id
                              AND wcq.work_id = e.work_id
                              AND pq.display_name ILIKE %s
                        )
                    )
                    """
                )
                params.extend([like, like, like, like, like])

            where_sql = " AND ".join(conditions)

            if sort == "isbn":
                order_sql = "e.isbn13 NULLS LAST, e.id"
            elif sort == "pub_date":
                order_sql = (
                    "(SELECT pd.date_value FROM edition_publishing_dates pd WHERE pd.tenant_id = e.tenant_id AND pd.edition_id = e.id AND pd.date_role = '01' ORDER BY pd.item_order, pd.created_at, pd.id LIMIT 1) "
                    "DESC NULLS LAST, e.isbn13 NULLS LAST, e.id"
                )
            elif sort == "updated":
                order_sql = (
                    "COALESCE(e.updated_at, w.updated_at) "
                    "DESC NULLS LAST, e.isbn13 NULLS LAST, e.id"
                )
            else:
                order_sql = (
                    "COALESCE(w.series_title, w.title, ''), "
                    "NULLIF(w.series_number, 0) NULLS LAST, "
                    "COALESCE(w.subtitle, ''), "
                    "e.isbn13 NULLS LAST, e.id"
                )

            # Count is independent of all rich ONIX tables.
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

            # Select the requested page FIRST, then aggregate contributors and
            # locate covers only for those selected rows.
            cur.execute(
                f"""
                WITH selected AS (
                    SELECT
                        e.id AS edition_id,
                        e.work_id,
                        e.isbn13,
                        e.record_reference,
                        e.product_form,
                        e.product_form_detail,
                        e.onix_product_form,
                        e.onix_product_form_detail,
                        e.cover_image_link AS edition_cover_image_link,
                        (
                            SELECT ep.publisher_name
                            FROM edition_publishers ep
                            WHERE ep.tenant_id = e.tenant_id
                              AND ep.edition_id = e.id
                              AND ep.publishing_role = '01'
                            ORDER BY ep.item_order, ep.created_at, ep.id
                            LIMIT 1
                        ) AS edition_publisher_name,
                        (
                            SELECT COALESCE(NULLIF(pd.date_text, ''), to_char(pd.date_value, 'YYYYMMDD'))
                            FROM edition_publishing_dates pd
                            WHERE pd.tenant_id = e.tenant_id
                              AND pd.edition_id = e.id
                              AND pd.date_role = '01'
                            ORDER BY pd.item_order, pd.created_at, pd.id
                            LIMIT 1
                        ) AS normalized_publication_date,
                        e.publishing_status,
                        e.status,
                        e.updated_at AS edition_updated_at,

                        w.title,
                        w.subtitle,
                        w.series_title,
                        w.series_number,
                        w.updated_at AS work_updated_at,

                        to_jsonb(w)->>'imprint_name'
                            AS work_imprint_name,

                        ROW_NUMBER() OVER (
                            ORDER BY {order_sql}
                        ) AS sort_order
                    FROM editions e
                    JOIN works w
                      ON w.id = e.work_id
                     AND w.tenant_id = e.tenant_id
                    WHERE {where_sql}
                    ORDER BY {order_sql}
                    LIMIT %s OFFSET %s
                ),
                contributor_summary AS (
                    SELECT
                        wc.work_id,
                        string_agg(
                            p.display_name,
                            '; '
                            ORDER BY
                                wc.sequence_number NULLS LAST,
                                wc.id
                        ) FILTER (
                            WHERE NULLIF(trim(p.display_name), '') IS NOT NULL
                        ) AS contributors_summary
                    FROM work_contributors wc
                    JOIN parties p
                      ON p.id = wc.party_id
                     AND p.tenant_id = wc.tenant_id
                    JOIN (
                        SELECT DISTINCT work_id
                        FROM selected
                    ) sw
                      ON sw.work_id = wc.work_id
                    WHERE wc.tenant_id = %s
                    GROUP BY wc.work_id
                ),
                selected_cover AS (
                    SELECT DISTINCT ON (esr.edition_id)
                        esr.edition_id,
                        erv.resource_link
                    FROM edition_supporting_resources esr
                    JOIN edition_supporting_resource_versions erv
                      ON erv.resource_id = esr.id
                     AND erv.tenant_id = esr.tenant_id
                    JOIN selected s
                      ON s.edition_id = esr.edition_id
                    WHERE esr.tenant_id = %s
                      AND esr.resource_content_type = '01'
                      AND NULLIF(trim(erv.resource_link), '') IS NOT NULL
                    ORDER BY
                        esr.edition_id,
                        CASE WHEN esr.is_primary THEN 0 ELSE 1 END,
                        esr.item_order,
                        erv.item_order,
                        erv.created_at
                )
                SELECT
                    s.edition_id,
                    s.work_id,
                    s.isbn13,
                    s.record_reference,
                    s.title,
                    s.subtitle,
                    s.series_title,
                    s.series_number,
                    cs.contributors_summary,

                    COALESCE(
                        NULLIF(trim(s.onix_product_form), ''),
                        CASE
                            WHEN upper(trim(coalesce(s.product_form, '')))
                                 ~ '^[A-Z0-9]{{2}}$'
                            THEN upper(trim(s.product_form))
                            ELSE NULL
                        END,
                        NULLIF(trim(s.product_form), '')
                    ) AS product_form,

                    COALESCE(
                        NULLIF(trim(s.onix_product_form_detail), ''),
                        NULLIF(trim(s.product_form_detail), '')
                    ) AS product_form_detail,

                    COALESCE(
                        NULLIF(trim(s.edition_cover_image_link), ''),
                        NULLIF(trim(sc.resource_link), '')
                    ) AS cover_image_link,

                    COALESCE(
                        NULLIF(trim(s.edition_publisher_name), ''),
                        NULLIF(trim(s.work_imprint_name), '')
                    ) AS publisher_or_imprint,

                    s.normalized_publication_date AS publication_date,

                    s.publishing_status,
                    s.status,

                    COALESCE(
                        s.edition_updated_at,
                        s.work_updated_at
                    ) AS updated_at,

                    s.sort_order
                FROM selected s
                LEFT JOIN contributor_summary cs
                  ON cs.work_id = s.work_id
                LEFT JOIN selected_cover sc
                  ON sc.edition_id = s.edition_id
                ORDER BY s.sort_order
                """,
                params + [page_size, offset, tenant_id, tenant_id],
            )

            items = []
            for row in cur.fetchall() or []:
                item = _clean_row(row)
                item.pop("sort_order", None)

                # Friendly display_title remains a UI-only value.
                work_stub = {
                    "title": item.get("title"),
                    "subtitle": item.get("subtitle"),
                    "series_title": item.get("series_title"),
                }
                item["display_title"] = _display_title_for_listing(work_stub)
                items.append(item)

            return {
                "items": items,
                "total": total,
                "page": page,
                "page_size": page_size,
            }

def _dedupe_identifiers(rows: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    seen = set()
    out = []
    for row in rows:
        id_type = _first_nonempty(
            row.get("id_type"),
            row.get("product_id_type"),
            row.get("identifier_type"),
        )
        id_value = _first_nonempty(
            row.get("id_value"),
            row.get("identifier_value"),
            row.get("value"),
        )
        if not id_value:
            continue
        key = (id_type, _norm(row.get("id_type_name")), id_value)
        if key in seen:
            continue
        seen.add(key)
        out.append(
            {
                "id_type": id_type,
                "id_type_name": _norm(row.get("id_type_name")),
                "id_value": id_value,
            }
        )
    return out


def _build_contributors(cur, tenant_id: str, work_id: str) -> List[Dict[str, Any]]:
    if not (_table_exists(cur, "work_contributors") and _table_exists(cur, "parties")):
        return []

    cur.execute(
        """
        SELECT
            to_jsonb(wc) AS assignment,
            to_jsonb(p) AS party
        FROM work_contributors wc
        JOIN parties p
          ON p.id = wc.party_id
         AND p.tenant_id = wc.tenant_id
        WHERE wc.tenant_id = %s
          AND wc.work_id = %s
        ORDER BY wc.sequence_number NULLS LAST, wc.id
        """,
        (tenant_id, work_id),
    )

    contributors: List[Dict[str, Any]] = []
    for row in cur.fetchall() or []:
        assignment = _clean_row(row.get("assignment") or {})
        party = _clean_row(row.get("party") or {})
        party_id = _norm(party.get("id"))

        contributor = {
            "assignment": assignment,
            "party": party,
            "role": _norm(assignment.get("contributor_role")),

            # Canonical reusable contributor biography. parties.short_bio is
            # the ONIX Contributor/BiographicalNote source.
            "short_bio": _norm(party.get("short_bio")),
            "biographical_note": _norm(party.get("short_bio")),
            "biography": _norm(party.get("short_bio")),
            "long_bio": _norm(party.get("long_bio")),

            "sequence_number": assignment.get("sequence_number"),
            "from_language_codes": assignment.get("from_language_codes") or [],
            "to_language_codes": assignment.get("to_language_codes") or [],
            "contributor_description": _norm(assignment.get("contributor_description")),
            "name_identifiers": _fetch_rows(
                cur, "party_name_identifiers",
                tenant_id=tenant_id, key_column="party_id", key_value=party_id,
            ),
            "alternative_names": _fetch_rows(
                cur, "party_alternative_names",
                tenant_id=tenant_id, key_column="party_id", key_value=party_id,
            ),
            "websites": _fetch_rows(
                cur, "party_websites",
                tenant_id=tenant_id, key_column="party_id", key_value=party_id,
            ),
            "places": _fetch_rows(
                cur, "party_contributor_places",
                tenant_id=tenant_id, key_column="party_id", key_value=party_id,
            ),
            "dates": _fetch_rows(
                cur, "party_contributor_dates",
                tenant_id=tenant_id, key_column="party_id", key_value=party_id,
            ),
            "professional_affiliations": _fetch_rows(
                cur, "party_professional_affiliations",
                tenant_id=tenant_id, key_column="party_id", key_value=party_id,
            ),
            "metadata": _fetch_rows(
                cur, "work_contributor_metadata",
                tenant_id=tenant_id,
                key_column="work_contributor_id",
                key_value=_norm(assignment.get("id")),
            ),
        }
        contributors.append(contributor)

    return contributors


def _build_texts(cur, tenant_id: str, edition_id: str) -> List[Dict[str, Any]]:
    texts = _fetch_rows(
        cur, "edition_texts",
        tenant_id=tenant_id, key_column="edition_id", key_value=edition_id,
    )

    # Historical InkSuite records may contain an author's biography as
    # CollateralDetail/TextContent type 12. For a single-author title, the
    # canonical source is parties.short_bio and the exporter must emit it as
    # Contributor/BiographicalNote instead. Keep the database rows untouched.
    try:
        cur.execute(
            """
            SELECT e.work_id
            FROM editions e
            WHERE e.tenant_id = %s
              AND e.id = %s
            LIMIT 1
            """,
            (tenant_id, edition_id),
        )
        edition_row = cur.fetchone() or {}
        work_id = _norm(edition_row.get("work_id"))

        if work_id:
            cur.execute(
                """
                SELECT COUNT(*) AS author_count
                FROM work_contributors wc
                WHERE wc.tenant_id = %s
                  AND wc.work_id = %s
                  AND upper(trim(coalesce(wc.contributor_role, '')))
                      IN ('A01', 'AUTHOR')
                """,
                (tenant_id, work_id),
            )
            author_count = int((cur.fetchone() or {}).get("author_count") or 0)

            if author_count <= 1:
                texts = [
                    text
                    for text in texts
                    if _norm(text.get("text_type")) != "12"
                ]
    except Exception:
        # Do not break ONIX assembly if a legacy deployment differs. The
        # original text rows remain available and no database data is changed.
        pass

    for text in texts:
        text["content_dates"] = _fetch_rows(
            cur, "edition_text_content_dates",
            tenant_id=tenant_id,
            key_column="edition_text_id",
            key_value=text.get("id"),
        )
    return texts


def _build_cited_content(cur, tenant_id: str, edition_id: str) -> List[Dict[str, Any]]:
    rows = _fetch_rows(
        cur, "edition_cited_content",
        tenant_id=tenant_id, key_column="edition_id", key_value=edition_id,
    )
    for item in rows:
        item["content_dates"] = _fetch_rows(
            cur, "edition_cited_content_dates",
            tenant_id=tenant_id,
            key_column="cited_content_id",
            key_value=item.get("id"),
        )
    return rows


def _build_supporting_resources(cur, tenant_id: str, edition_id: str) -> List[Dict[str, Any]]:
    resources = _fetch_rows(
        cur, "edition_supporting_resources",
        tenant_id=tenant_id, key_column="edition_id", key_value=edition_id,
    )
    for resource in resources:
        resource_id = resource.get("id")
        resource["resource_features"] = _fetch_rows(
            cur, "edition_supporting_resource_parent_features",
            tenant_id=tenant_id, key_column="resource_id", key_value=resource_id,
        )
        versions = _fetch_rows(
            cur, "edition_supporting_resource_versions",
            tenant_id=tenant_id, key_column="resource_id", key_value=resource_id,
        )
        for version in versions:
            version["resource_version_features"] = _fetch_rows(
                cur, "edition_supporting_resource_features",
                tenant_id=tenant_id,
                key_column="resource_version_id",
                key_value=version.get("id"),
            )
        resource["versions"] = versions
    return resources


def _build_related_products(cur, tenant_id: str, edition_id: str) -> List[Dict[str, Any]]:
    """
    Build RelatedProduct composites from the current single-table schema.

    edition_related_products.identifiers JSONB is the authoritative store for
    repeatable ProductIdentifier composites. The scalar identifier columns are
    retained for fast lookup and backwards compatibility.
    """
    rows = _fetch_rows(
        cur, "edition_related_products",
        tenant_id=tenant_id, key_column="edition_id", key_value=edition_id,
    )

    for row in rows:
        identifiers: List[Dict[str, Any]] = []

        stored_identifiers = row.get("identifiers")
        if isinstance(stored_identifiers, list):
            identifiers.extend(stored_identifiers)

        scalar_type = _norm(row.get("related_product_id_type"))
        scalar_value = _norm(row.get("related_product_id_value"))
        if scalar_value:
            identifiers.append(
                {
                    "id_type": scalar_type,
                    "id_type_name": "",
                    "id_value": scalar_value,
                }
            )

        # If an older row has only related_isbn13, still expose the proper
        # ProductIdentifier type 15 composite.
        related_isbn13 = _norm(row.get("related_isbn13"))
        if related_isbn13:
            identifiers.append(
                {
                    "id_type": "15",
                    "id_type_name": "",
                    "id_value": related_isbn13,
                }
            )

        row["product_identifiers"] = _dedupe_identifiers(identifiers)

    return rows


def _build_supply(cur, tenant_id: str, edition_id: str) -> List[Dict[str, Any]]:
    rows = _fetch_rows(
        cur, "edition_supply_details",
        tenant_id=tenant_id, key_column="edition_id", key_value=edition_id,
    )
    for supply in rows:
        supply_id = supply.get("id")
        supply["supplier_identifiers"] = _fetch_rows(
            cur, "edition_supplier_identifiers",
            tenant_id=tenant_id,
            key_column="supply_detail_id",
            key_value=supply_id,
        )
        supply["prices"] = _fetch_rows(
            cur, "edition_prices",
            tenant_id=tenant_id,
            key_column="supply_detail_id",
            key_value=supply_id,
        )
        supply["supply_dates"] = _fetch_rows(
            cur, "edition_supply_dates",
            tenant_id=tenant_id,
            key_column="supply_detail_id",
            key_value=supply_id,
        )
    return rows


def _canonical_titles_from_db(
    work: Dict[str, Any],
    title_rows: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """
    Preserve normalized work_titles and expose the authoritative works.title
    as TitleText on the primary title row.

    ONIX mapping:
      works.title                       -> TitleText
      work_titles.title_prefix          -> TitlePrefix
      work_titles.title_without_prefix  -> TitleWithoutPrefix
      work_titles.subtitle              -> Subtitle

    No title metadata is invented here. All values come directly from the
    database.
    """
    rows = [dict(row) for row in title_rows]

    work_title = _norm(work.get("title"))
    work_subtitle = _norm(work.get("subtitle"))

    if rows:
        target_index = next(
            (
                idx
                for idx, row in enumerate(rows)
                if row.get("is_primary") is True
            ),
            0,
        )

        row = dict(rows[target_index])

        if work_title:
            row["title_text"] = work_title

        if work_subtitle and not _norm(row.get("subtitle")):
            row["subtitle"] = work_subtitle

        rows[target_index] = row

        # Short title is stored canonically on works.short_title rather than as a
        # physical work_titles row. Expose it as ONIX TitleType 05 without
        # duplicating it in the database.
        short_title = _norm(work.get("short_title"))
        if short_title and not any(_norm(r.get("title_type")) == "05" for r in rows):
            rows.append({
                "title_type": "05",
                "title_element_level": "01",
                "title_prefix": "",
                "title_without_prefix": short_title,
                "title_text": "",
                "subtitle": "",
                "part_number": "",
                "no_prefix": True,
                "language_code": _norm(work.get("language")),
                "is_primary": False,
                "item_order": max([int(r.get("item_order") or 0) for r in rows] + [1]) + 1,
            })
        return rows

    short_title = _norm(work.get("short_title"))
    if not (work_title or work_subtitle or short_title):
        return []

    result = []
    if work_title or work_subtitle:
        result.append(
            {
                "title_type": "01",
                "title_element_level": "01",
                "title_prefix": "",
                "title_without_prefix": "",
                "title_text": work_title,
                "subtitle": work_subtitle,
                "no_prefix": None,
                "is_primary": True,
                "item_order": 1,
            }
        )
    if short_title:
        result.append(
            {
                "title_type": "05",
                "title_element_level": "01",
                "title_prefix": "",
                "title_without_prefix": short_title,
                "title_text": "",
                "subtitle": "",
                "part_number": "",
                "no_prefix": True,
                "language_code": _norm(work.get("language")),
                "is_primary": False,
                "item_order": 2,
            }
        )
    return result

def _group_related_products(
    rows: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """
    Normalize semantically identical RelatedProduct rows into one composite
    with repeat ProductIdentifier children.

    This is required because RelatedProduct/ProductIdentifier is one-to-many
    in ONIX, while older InkSuite rows often stored one identifier per row.
    """
    grouped: Dict[tuple, Dict[str, Any]] = {}
    order: List[tuple] = []

    for row in rows:
        key = (
            _norm(row.get("relation_code")),
            _norm(row.get("related_product_form")),
            _norm(row.get("related_product_form_detail")),
            _norm(row.get("title")),
            _norm(row.get("subtitle")),
            _norm(row.get("publisher_name")),
            _norm(row.get("publication_date")),
            _norm(row.get("product_url")),
        )
        if key not in grouped:
            grouped[key] = dict(row)
            grouped[key]["product_identifiers"] = []
            order.append(key)

        grouped[key]["product_identifiers"].extend(
            row.get("product_identifiers") or []
        )

    output: List[Dict[str, Any]] = []
    for key in order:
        row = grouped[key]
        row["product_identifiers"] = _dedupe_identifiers(
            row.get("product_identifiers") or []
        )
        output.append(row)

    return output


def _build_one(cur, tenant_id: str, edition_id: str) -> Dict[str, Any]:
    cur.execute(
        """
        SELECT to_jsonb(e) AS edition, to_jsonb(w) AS work
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
    base = cur.fetchone()
    if not base:
        return {}

    edition = _clean_row(base.get("edition") or {})
    work = _clean_row(base.get("work") or {})
    work_id = _norm(work.get("id"))

    identifiers = _fetch_rows(
        cur, "edition_identifiers",
        tenant_id=tenant_id, key_column="edition_id", key_value=edition_id,
    )

    # Include legacy scalar identifiers only when not already represented by the
    # normalized identifier table. These are deterministic schema mappings, not
    # invented metadata values.
    scalar_identifiers = [
        {"id_type": "15", "id_type_name": "", "id_value": _norm(edition.get("isbn13"))},
        {"id_type": "03", "id_type_name": "", "id_value": _norm(edition.get("ean"))},
        {"id_type": "02", "id_type_name": "", "id_value": _norm(edition.get("isbn10"))},
        # Canonical LCCN is collected once at work level in works.loc_number.
        # ONIX ProductIDType 13 is the Library of Congress Control Number.
        {"id_type": "13", "id_type_name": "", "id_value": _norm(work.get("loc_number"))},
    ]
    identifiers = _dedupe_identifiers([*identifiers, *scalar_identifiers])

    product_form_details = _fetch_rows(
        cur, "edition_form_details",
        tenant_id=tenant_id, key_column="edition_id", key_value=edition_id,
    )
    content_types = _fetch_rows(
        cur, "edition_content_types",
        tenant_id=tenant_id, key_column="edition_id", key_value=edition_id,
    )

    titles = _fetch_rows(
        cur, "work_titles",
        tenant_id=tenant_id, key_column="work_id", key_value=work_id,
    )
    titles = _canonical_titles_from_db(work, titles)

    collections = _fetch_rows(
        cur, "work_collections",
        tenant_id=tenant_id, key_column="work_id", key_value=work_id,
    )

    # Optional collection identifiers if the normalized table exists.
    for collection in collections:
        if _table_exists(cur, "work_collection_identifiers"):
            collection["identifiers"] = _fetch_rows(
                cur, "work_collection_identifiers",
                tenant_id=tenant_id,
                key_column="collection_id",
                key_value=collection.get("id"),
            )
        else:
            collection["identifiers"] = []

    descriptive_detail = {
        "product_composition": _norm(edition.get("product_composition")),
        "product_form": _product_form_code(edition),
        "product_form_detail_scalar": _product_form_detail_code(edition),
        "product_form_details": product_form_details,
        "product_form_features": _fetch_rows(
            cur, "edition_product_form_features",
            tenant_id=tenant_id, key_column="edition_id", key_value=edition_id,
        ),
        "product_packaging": _norm(edition.get("product_packaging")),
        "primary_content_type": _first_nonempty(
            edition.get("primary_content_type"),
            edition.get("product_content_type"),
            *(
                row.get("content_type_code")
                or row.get("product_content_type")
                or row.get("content_type")
                for row in content_types
            ),
        ),
        "product_content_types": content_types,
        "trade_category": _norm(edition.get("trade_category")),
        "product_form_description": _norm(edition.get("product_form_description")),
        "country_of_manufacture": _norm(edition.get("country_of_manufacture")),
        "edition_number": _norm(edition.get("edition_number")),
        "edition_statement": _norm(edition.get("edition_statement")),
        "no_edition": not (
            _norm(edition.get("edition_number"))
            or _norm(edition.get("edition_statement"))
        ),
        "titles": titles,
        "collections": collections,
        "no_collection": len(collections) == 0,
        "contributors": _build_contributors(cur, tenant_id, work_id),
        "languages": [
            row for row in [
                {"language_role": "01", "language_code": _norm(work.get("language"))},
                {"language_role": "02", "language_code": _norm(work.get("original_language"))},
            ] if row["language_code"]
        ],
        "extents": _fetch_rows(
            cur, "edition_extents",
            tenant_id=tenant_id, key_column="edition_id", key_value=edition_id,
        ),
        "measurements": _fetch_rows(
            cur, "edition_measurements",
            tenant_id=tenant_id, key_column="edition_id", key_value=edition_id,
        ),
        "ancillary_content": _fetch_rows(
            cur, "edition_ancillary_content",
            tenant_id=tenant_id, key_column="edition_id", key_value=edition_id,
        ),
        "subjects": _fetch_rows(
            cur, "edition_subjects",
            tenant_id=tenant_id, key_column="edition_id", key_value=edition_id,
        ),
        "audiences": _fetch_rows(
            cur, "edition_audience",
            tenant_id=tenant_id, key_column="edition_id", key_value=edition_id,
        ),
        "usage_constraints": _fetch_rows(
            cur, "edition_usage_constraints",
            tenant_id=tenant_id, key_column="edition_id", key_value=edition_id,
        ),
        "audience_description": _norm(edition.get("audience_description")),
        "product_details_note": _norm(edition.get("product_details_note")),

        # Canonical Product Details scalar values. These are kept at the
        # DescriptiveDetail level so the serializer does not have to reach into
        # legacy compatibility buckets.
        "number_of_illustrations": edition.get("illustrations_number"),
        "illustrations_note": _norm(edition.get("illustrations_desc")),
        "color_content": _norm(edition.get("color_content")),
        "color_pages": edition.get("color_pages"),
        "number_of_pieces": edition.get("number_of_pieces"),
        "file_format": _norm(edition.get("file_format")),
        "epub_version": _norm(edition.get("epub_version")),
        "technical_protection": _norm(edition.get("technical_protection")),

        "digital": {
            "file_format": _norm(edition.get("file_format")),
            "epub_version": _norm(edition.get("epub_version")),
            "technical_protection": _norm(edition.get("technical_protection")),
            "digital_requirements": _norm(edition.get("digital_requirements")),
            "file_size": edition.get("file_size"),
            "file_size_unit": _norm(edition.get("file_size_unit")),
        },
        "audio": {
            "audio_type": _norm(edition.get("audio_type")),
            "narrator_statement": _norm(edition.get("narrator_statement")),
            "duration": edition.get("duration"),
            "duration_unit": _norm(edition.get("duration_unit")),
        },
        "legacy_descriptive": {
            "number_of_pages": edition.get("number_of_pages"),
            "illustrations_type": _norm(edition.get("illustrations_type")),
            "illustrations_desc": _norm(edition.get("illustrations_desc")),
            "illustrations_number": edition.get("illustrations_number"),
            "color_content": _norm(edition.get("color_content")),
            "color_pages": edition.get("color_pages"),
            "number_of_pieces": edition.get("number_of_pieces"),
        },
    }

    collateral_detail = {
        "texts": _build_texts(cur, tenant_id, edition_id),
        "cited_content": _build_cited_content(cur, tenant_id, edition_id),
        "supporting_resources": _build_supporting_resources(cur, tenant_id, edition_id),
        "prizes": _fetch_rows(
            cur, "edition_prizes",
            tenant_id=tenant_id, key_column="edition_id", key_value=edition_id,
        ),
    }

    rights_rows = _fetch_rows(
        cur, "edition_rights",
        tenant_id=tenant_id, key_column="edition_id", key_value=edition_id,
    )

    publisher_rows = _fetch_rows(
        cur,
        "edition_publishers",
        tenant_id=tenant_id,
        key_column="edition_id",
        key_value=edition_id,
    )
    for publisher in publisher_rows:
        publisher["identifiers"] = _fetch_rows(
            cur, "edition_publisher_identifiers",
            tenant_id=tenant_id, key_column="publisher_id", key_value=publisher.get("id"),
        )

    imprint_rows = _fetch_rows(
        cur, "edition_imprints",
        tenant_id=tenant_id, key_column="edition_id", key_value=edition_id,
    )
    for imprint in imprint_rows:
        imprint["identifiers"] = _fetch_rows(
            cur, "edition_imprint_identifiers",
            tenant_id=tenant_id, key_column="imprint_id", key_value=imprint.get("id"),
        )

    sales_rights_rows = _fetch_rows(
        cur, "edition_sales_rights",
        tenant_id=tenant_id, key_column="edition_id", key_value=edition_id,
    )

    publishing_detail = {
        "imprint_name": _first_nonempty(
            edition.get("imprint_name"),
            work.get("imprint_name"),
        ),
        "publisher_name": _first_nonempty(
            publisher_rows[0].get("publisher_name") if publisher_rows else "",
        ),
        "publishers": publisher_rows,
        "imprints": imprint_rows,
        "sales_rights": sales_rights_rows,
        "row_sales_rights_type": _first_nonempty(
            *(row.get("row_sales_rights_type") for row in rights_rows),
            edition.get("row_sales_rights_type"),
            edition.get("rowSalesRightsType"),
            # ONIX Best Practice requires ROWSalesRightsType. If InkSuite
            # does not store a separate ROW code, inherit the edition's
            # existing SalesRightsType. For A Dagger of Lightning this
            # correctly yields 01.
            *(row.get("sales_rights_type") for row in rights_rows),
            edition.get("sales_rights_type"),
            edition.get("salesRightsType"),
        ),
        "publishing_role": _first_nonempty(
            publisher_rows[0].get("publishing_role") if publisher_rows else "",
            edition.get("publishing_role"),
            edition.get("publisher_role"),
            work.get("publishing_role"),
            work.get("publisher_role"),
        ),
        "country_of_publication": _first_nonempty(
            edition.get("country_of_publication"),
            work.get("country_of_publication"),
        ),
        "city_of_publication": _first_nonempty(
            edition.get("city_of_publication"),
            work.get("city_of_publication"),
        ),
        "publisher_websites": _fetch_rows(
            cur, "edition_publisher_websites",
            tenant_id=tenant_id, key_column="edition_id", key_value=edition_id,
        ),
        "publishing_status": _norm(edition.get("publishing_status")),
        "publishing_dates": _fetch_rows(
            cur, "edition_publishing_dates",
            tenant_id=tenant_id, key_column="edition_id", key_value=edition_id,
        ),
        "copyright_year": edition.get("copyright_year"),
        "rights": rights_rows,
        "sales_restrictions": _fetch_rows(
            cur, "edition_sales_restrictions",
            tenant_id=tenant_id, key_column="edition_id", key_value=edition_id,
        ),
        "product_contacts": _fetch_rows(
            cur, "edition_product_contacts",
            tenant_id=tenant_id, key_column="edition_id", key_value=edition_id,
        ),
        "publisher_website_role": _norm(edition.get("publisher_website_role")),
        "publisher_website_url": _norm(edition.get("publisher_website_url")),
        "market_publishing_status": _norm(edition.get("market_publishing_status")),
        "market_date": {
            "market_date_role": _norm(edition.get("market_date_role")),
            "market_date_format": _norm(edition.get("market_date_format")),
            "market_date_text": _norm(edition.get("market_date_text")),
        },
        "promotion_contact": _norm(edition.get("promotion_contact")),
        "promotion_contact_text_format": _norm(edition.get("promotion_contact_text_format")),
        "initial_print_run": _norm(edition.get("initial_print_run")),
        "initial_print_run_text_format": _norm(edition.get("initial_print_run_text_format")),
        "promotion_campaign": _norm(edition.get("promotion_campaign")),
        "promotion_campaign_text_format": _norm(edition.get("promotion_campaign_text_format")),
    }

    related_material = {
        "related_works": _fetch_rows(
            cur, "edition_related_works",
            tenant_id=tenant_id, key_column="edition_id", key_value=edition_id,
        ),
        "related_products": _build_related_products(cur, tenant_id, edition_id),
    }

    product_supply = _build_supply(cur, tenant_id, edition_id)
    markets = _fetch_rows(
        cur, "edition_markets",
        tenant_id=tenant_id, key_column="edition_id", key_value=edition_id,
    )

    product = {
        "edition_id": edition_id,
        "work_id": work_id,
        "record_reference": _norm(edition.get("record_reference")),
        "notification_type": _norm(edition.get("notification_type")),
        "record_source_type": _norm(edition.get("record_source_type")),
        "record_source_name": _norm(edition.get("record_source_name")),
        "identifiers": identifiers,
        "barcode": {
            "barcode_type": _norm(edition.get("barcode_type")),
            "position_on_product": _norm(edition.get("barcode_position_on_product")),
            "barcode_value": _norm(edition.get("barcode")),
        },
        "descriptive_detail": descriptive_detail,
        "collateral_detail": collateral_detail,
        "publishing_detail": publishing_detail,
        "related_material": related_material,
        "product_supply": product_supply,
        "markets": markets,

        # Full source snapshots make the canonical API inspectable and ensure no
        # database field is silently discarded by assembly.
        "source_snapshot": {
            "edition": edition,
            "work": work,
        },
    }

    # Compatibility aliases for current router/UI/validation callers.
    primary_title = next(
        (row for row in titles if row.get("is_primary")),
        titles[0] if titles else {},
    )
    product.update(
        {
            "title": _first_nonempty(
                primary_title.get("title_without_prefix"),
                work.get("title"),
            ),
            "subtitle": _first_nonempty(
                primary_title.get("subtitle"),
                work.get("subtitle"),
            ),
            "series_title": _norm(work.get("series_title")),
            "series_number": work.get("series_number") or 0,
            "publisher_name": publishing_detail["publisher_name"],
            "language": _norm(work.get("language")),
            "product_form": descriptive_detail["product_form"],
            "product_form_detail": descriptive_detail["product_form_detail_scalar"],
            "publication_date": _publication_date_for_listing(publishing_detail["publishing_dates"]),
            "publishing_status": publishing_detail["publishing_status"],
            "contributors": [
                {
                    "role": c.get("role"),
                    "sequence_number": c.get("sequence_number"),
                    "name": _norm((c.get("party") or {}).get("display_name")),
                }
                for c in descriptive_detail["contributors"]
            ],
            "subjects": descriptive_detail["subjects"],
            "texts": collateral_detail["texts"],
            "supply_details": product_supply,
            "cover_image_link": _cover_for_listing(
                cur, tenant_id, edition_id, edition, work
            ),
            "number_of_pages": edition.get("number_of_pages"),
            "inventory_number": _norm(edition.get("inventory_number")),
        }
    )

    return product


def build_onix_product_payload(
    tenant_id: str,
    edition_id: str,
    cur=None,
) -> Dict[str, Any]:
    if cur is not None:
        return _build_one(cur, tenant_id, edition_id)

    with db_conn() as conn:
        with conn.cursor(row_factory=dict_row) as cur2:
            return _build_one(cur2, tenant_id, edition_id)


def get_exportable_product_by_isbn(
    tenant_slug: str,
    isbn13: str,
) -> Optional[Dict[str, Any]]:
    normalized = _norm_isbn(isbn13)
    if not normalized:
        return None

    with db_conn() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            tenant_id = _get_tenant_id_from_slug(cur, tenant_slug)
            cur.execute(
                """
                SELECT id
                FROM editions
                WHERE tenant_id = %s
                  AND regexp_replace(coalesce(isbn13,''), '[^0-9Xx]', '', 'g')
                      = regexp_replace(%s, '[^0-9Xx]', '', 'g')
                LIMIT 1
                """,
                (tenant_id, normalized),
            )
            row = cur.fetchone()
            if not row:
                return None
            return _build_one(cur, tenant_id, str(row["id"]))


def _build_message_header(cur, tenant_id: str) -> Optional[Dict[str, Any]]:
    """
    Build the tenant-level ONIX message Header from tenant_onix_headers.

    SentDateTime is intentionally not stored in the database. The serializer
    generates it at export time.
    """
    if not _table_exists(cur, "tenant_onix_headers"):
        return None

    row = _fetch_one_optional(
        cur,
        "tenant_onix_headers",
        tenant_id=tenant_id,
        key_column="tenant_id",
        key_value=tenant_id,
    )
    if not row:
        return None

    sender_name = _norm(row.get("sender_name"))
    contact_name = _norm(row.get("contact_name"))
    email_address = _norm(row.get("email_address"))

    if not (
        sender_name
        and contact_name
        and email_address
    ):
        return None

    return {
        "sender_name": sender_name,
        "sender_identifier_type": _norm(
            row.get("sender_identifier_type")
        ),
        "sender_identifier_value": _norm(
            row.get("sender_identifier_value")
        ),
        "contact_name": contact_name,
        "email_address": email_address,
    }



def build_onix_message_payload(
    tenant_id: str,
    edition_ids: List[str],
    cur=None,
) -> Dict[str, Any]:
    def build(cursor):
        products = [
            product
            for product in (
                _build_one(cursor, tenant_id, edition_id)
                for edition_id in edition_ids
            )
            if product
        ]
        return {
            "release": "3.0",
            "header": _build_message_header(cursor, tenant_id),
            "products": products,
        }

    if cur is not None:
        return build(cur)

    with db_conn() as conn:
        with conn.cursor(row_factory=dict_row) as cur2:
            return build(cur2)
