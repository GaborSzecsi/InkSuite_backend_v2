# routers/financials.py
from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Query
from psycopg.rows import dict_row

from app.core.db import db_conn

router = APIRouter(prefix="/financials", tags=["Financials"])


def _get_tenant_id(cur, tenant_slug: str = "marble-press") -> str:
    cur.execute(
        """
        SELECT id
        FROM public.tenants
        WHERE lower(slug) = lower(%s)
        LIMIT 1
        """,
        (tenant_slug,),
    )
    row = cur.fetchone()
    if not row:
        raise HTTPException(status_code=404, detail=f"Tenant not found: {tenant_slug}")
    return str(row["id"])


def _normalize_month(month: Any) -> Optional[str]:
    if month is None:
        return None
    try:
        mm = int(str(month).strip())
        if 1 <= mm <= 12:
            return f"{mm:02d}"
    except Exception:
        pass
    return None


def build_period_keys(mode: str, year: int, month: Optional[Any], season: Optional[str]) -> List[str]:
    mode_u = (mode or "MONTH").upper()

    if mode_u == "MONTH":
        mm = _normalize_month(month) or f"{datetime.utcnow().month:02d}"
        return [f"{year:04d}-{mm}"]

    s = (season or "").upper()
    if s not in ("SPRING", "FALL"):
        raise HTTPException(status_code=400, detail="For mode=SEASON, season must be SPRING or FALL")

    months = ["01", "02", "03", "04", "05", "06"] if s == "SPRING" else ["07", "08", "09", "10", "11", "12"]
    return [f"{year:04d}-{m}" for m in months]


def _friendly_label(fmt: str) -> str:
    return {
        "HC": "Hardcover",
        "PB": "Paperback",
        "BB": "Board Book",
        "EBK": "Ebook",
        "AUD": "Audiobook",
    }.get((fmt or "").upper(), fmt or "")


FMT_CASE = """
CASE
  WHEN lower(e.product_form) LIKE '%%hard%%' THEN 'HC'
  WHEN lower(e.product_form) LIKE '%%paper%%' THEN 'PB'
  WHEN lower(e.product_form) LIKE '%%board%%' THEN 'BB'
  WHEN lower(e.product_form) LIKE '%%ebook%%' OR lower(e.product_form) LIKE '%%e-book%%' THEN 'EBK'
  WHEN lower(e.product_form) LIKE '%%audio%%' THEN 'AUD'
  ELSE upper(e.product_form)
END
"""


@router.get("/health")
def financials_health():
    return {
        "ok": True,
        "source": "sql",
        "tables": [
            "works",
            "editions",
            "royalty_periods",
            "royalty_sales_lines",
            "inventory_movements",
            "fraser_ca_sales_lines",
        ],
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
    keys = build_period_keys(mode, year, month, season)
    latest_key = keys[-1]
    fmt_filter = (format or "ALL").upper()

    fmt_sql = ""
    if fmt_filter != "ALL":
        fmt_sql = f" AND {FMT_CASE} = %s "

    with db_conn() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            tenant_id = _get_tenant_id(cur)

            sales_params: List[Any] = [tenant_id, bookUid, bookUid, keys]
            if fmt_filter != "ALL":
                sales_params.append(fmt_filter)

            cur.execute(
                f"""
                SELECT
                  COALESCE(SUM(r.units_sold), 0) AS units_sold,
                  COALESCE(SUM(r.units_returned), 0) AS returns,
                  COALESCE(SUM(r.publisher_receipts), 0) AS publisher_receipts
                FROM public.royalty_sales_lines r
                JOIN public.royalty_periods rp ON rp.id = r.period_id
                JOIN public.editions e ON e.id = r.edition_id
                JOIN public.works w ON w.id = e.work_id
                WHERE r.tenant_id = %s
                  AND (w.id::text = %s OR COALESCE(w.uid::text, '') = %s)
                  AND rp.period_code = ANY(%s)
                  {fmt_sql}
                """,
                sales_params,
            )
            sales = cur.fetchone() or {}

            inv_period_params: List[Any] = [tenant_id, bookUid, bookUid, keys]
            if fmt_filter != "ALL":
                inv_period_params.append(fmt_filter)

            cur.execute(
                f"""
                SELECT
                  COALESCE(SUM(CASE WHEN m.movement_type = 'complimentary_shipment' THEN -m.quantity ELSE 0 END), 0) AS free_copies,
                  COALESCE(SUM(CASE WHEN m.movement_type = 'shipment_to_fraser_CA' THEN -m.quantity ELSE 0 END), 0) AS fraser_shipments
                FROM public.inventory_movements m
                JOIN public.editions e ON e.id = m.edition_id
                JOIN public.works w ON w.id = e.work_id
                WHERE m.tenant_id = %s
                  AND (w.id::text = %s OR COALESCE(w.uid::text, '') = %s)
                  AND to_char(m.movement_date, 'YYYY-MM') = ANY(%s)
                  {fmt_sql}
                """,
                inv_period_params,
            )
            inv_period = cur.fetchone() or {}

            fr_params: List[Any] = [tenant_id, bookUid, bookUid, keys]
            if fmt_filter != "ALL":
                fr_params.append(fmt_filter)

            cur.execute(
                f"""
                SELECT
                  COALESCE(SUM(f.units_sold), 0) AS fraser_units,
                  COALESCE(SUM(f.publisher_receipts), 0) AS fraser_dollars
                FROM public.fraser_ca_sales_lines f
                JOIN public.royalty_periods rp ON rp.id = f.period_id
                JOIN public.editions e ON e.id = f.edition_id
                JOIN public.works w ON w.id = e.work_id
                WHERE f.tenant_id = %s
                  AND (w.id::text = %s OR COALESCE(w.uid::text, '') = %s)
                  AND rp.period_code = ANY(%s)
                  {fmt_sql}
                """,
                fr_params,
            )
            fr = cur.fetchone() or {}

            inv_move_params: List[Any] = [tenant_id, bookUid, bookUid, latest_key]
            if fmt_filter != "ALL":
                inv_move_params.append(fmt_filter)

            sales_to_date_params: List[Any] = [tenant_id, bookUid, bookUid, latest_key]
            if fmt_filter != "ALL":
                sales_to_date_params.append(fmt_filter)

            cur.execute(
                f"""
                WITH movements AS (
                  SELECT
                    e.id AS edition_id,
                    COALESCE(SUM(CASE WHEN m.movement_type = 'receipt' THEN m.quantity ELSE 0 END), 0) AS received,
                    COALESCE(SUM(CASE WHEN m.movement_type = 'complimentary_shipment' THEN -m.quantity ELSE 0 END), 0) AS free_copies,
                    COALESCE(SUM(CASE WHEN m.movement_type = 'shipment_to_fraser_CA' THEN -m.quantity ELSE 0 END), 0) AS fraser_shipped
                  FROM public.inventory_movements m
                  JOIN public.editions e ON e.id = m.edition_id
                  JOIN public.works w ON w.id = e.work_id
                  WHERE m.tenant_id = %s
                    AND (w.id::text = %s OR COALESCE(w.uid::text, '') = %s)
                    AND to_char(m.movement_date, 'YYYY-MM') <= %s
                    {fmt_sql}
                  GROUP BY e.id
                ),
                sales_to_date AS (
                  SELECT
                    e.id AS edition_id,
                    COALESCE(SUM(r.units_sold - r.units_returned), 0) AS net_units
                  FROM public.royalty_sales_lines r
                  JOIN public.royalty_periods rp ON rp.id = r.period_id
                  JOIN public.editions e ON e.id = r.edition_id
                  JOIN public.works w ON w.id = e.work_id
                  WHERE r.tenant_id = %s
                    AND (w.id::text = %s OR COALESCE(w.uid::text, '') = %s)
                    AND rp.period_code <= %s
                    {fmt_sql}
                  GROUP BY e.id
                )
                SELECT
                  COALESCE(SUM(
                    COALESCE(m.received, 0)
                    - COALESCE(m.free_copies, 0)
                    - COALESCE(m.fraser_shipped, 0)
                    - COALESCE(s.net_units, 0)
                  ), 0) AS inventory_end
                FROM movements m
                LEFT JOIN sales_to_date s ON s.edition_id = m.edition_id
                """,
                inv_move_params + sales_to_date_params,
            )
            inv_end = cur.fetchone() or {}

            return {
                "unitsSold": float(sales.get("units_sold") or 0),
                "returns": float(sales.get("returns") or 0),
                "freeCopies": float(inv_period.get("free_copies") or 0),
                "inventoryEnd": float(inv_end.get("inventory_end") or 0),
                "fraserShipments": float(inv_period.get("fraser_shipments") or 0) + float(fr.get("fraser_units") or 0),
                "fraserDollars": round(float(fr.get("fraser_dollars") or 0), 2),
                "periods": keys,
                "_source": {"financials": "sql", "books": "sql"},
            }


@router.get("/book-format-stats")
def get_book_format_stats(
    bookUid: str = Query(...),
    year: int = Query(default_factory=lambda: datetime.utcnow().year),
):
    with db_conn() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            tenant_id = _get_tenant_id(cur)

            cur.execute(
                f"""
                WITH edition_base AS (
                  SELECT
                    e.id AS edition_id,
                    e.product_form,
                    {FMT_CASE} AS fmt
                  FROM public.editions e
                  JOIN public.works w ON w.id = e.work_id
                  WHERE e.tenant_id = %s
                    AND (w.id::text = %s OR COALESCE(w.uid::text, '') = %s)
                ),
                inv AS (
                  SELECT
                    eb.fmt,
                    COALESCE(SUM(CASE WHEN m.movement_type = 'receipt' THEN m.quantity ELSE 0 END), 0) AS total_printed,
                    COALESCE(SUM(CASE WHEN m.movement_type = 'complimentary_shipment' THEN -m.quantity ELSE 0 END), 0) AS free_copies,
                    COALESCE(SUM(CASE WHEN m.movement_type = 'shipment_to_fraser_CA' THEN -m.quantity ELSE 0 END), 0) AS fraser_shipped
                  FROM edition_base eb
                  LEFT JOIN public.inventory_movements m ON m.edition_id = eb.edition_id
                  GROUP BY eb.fmt
                ),
                sales AS (
                  SELECT
                    eb.fmt,
                    COALESCE(SUM(r.units_sold - r.units_returned), 0) AS lifetime_sold,
                    COALESCE(SUM(CASE WHEN rp.period_code LIKE %s THEN r.units_sold - r.units_returned ELSE 0 END), 0) AS ytd_sold
                  FROM edition_base eb
                  LEFT JOIN public.royalty_sales_lines r ON r.edition_id = eb.edition_id
                  LEFT JOIN public.royalty_periods rp ON rp.id = r.period_id
                  GROUP BY eb.fmt
                ),
                fraser AS (
                  SELECT
                    eb.fmt,
                    COALESCE(SUM(f.units_sold), 0) AS lifetime_fraser_sold,
                    COALESCE(SUM(CASE WHEN rp.period_code LIKE %s THEN f.units_sold ELSE 0 END), 0) AS ytd_fraser_sold
                  FROM edition_base eb
                  LEFT JOIN public.fraser_ca_sales_lines f ON f.edition_id = eb.edition_id
                  LEFT JOIN public.royalty_periods rp ON rp.id = f.period_id
                  GROUP BY eb.fmt
                )
                SELECT
                  eb.fmt,
                  MIN(eb.product_form) AS product_form,
                  COALESCE(inv.total_printed, 0) AS total_printed,
                  COALESCE(sales.lifetime_sold, 0) AS lifetime_sold,
                  COALESCE(sales.ytd_sold, 0) AS ytd_sold,
                  COALESCE(inv.fraser_shipped, 0) + COALESCE(fraser.lifetime_fraser_sold, 0) AS lifetime_fraser_shipments,
                  COALESCE(fraser.ytd_fraser_sold, 0) AS ytd_fraser_shipments,
                  (
                    COALESCE(inv.total_printed, 0)
                    - COALESCE(inv.free_copies, 0)
                    - COALESCE(inv.fraser_shipped, 0)
                    - COALESCE(sales.lifetime_sold, 0)
                  ) AS inventory_end
                FROM edition_base eb
                LEFT JOIN inv ON inv.fmt = eb.fmt
                LEFT JOIN sales ON sales.fmt = eb.fmt
                LEFT JOIN fraser ON fraser.fmt = eb.fmt
                GROUP BY
                  eb.fmt,
                  inv.total_printed,
                  inv.free_copies,
                  inv.fraser_shipped,
                  sales.lifetime_sold,
                  sales.ytd_sold,
                  fraser.lifetime_fraser_sold,
                  fraser.ytd_fraser_sold
                ORDER BY eb.fmt
                """,
                (tenant_id, bookUid, bookUid, f"{year}-%", f"{year}-%"),
            )
            rows = cur.fetchall() or []

            formats: Dict[str, Dict[str, Any]] = {}
            for r in rows:
                fmt = str(r.get("fmt") or "").upper()
                if not fmt:
                    continue
                formats[fmt] = {
                    "label": _friendly_label(fmt),
                    "totalPrinted": float(r.get("total_printed") or 0),
                    "lifetimeSold": float(r.get("lifetime_sold") or 0),
                    "ytdSold": float(r.get("ytd_sold") or 0),
                    "lifetimeFraserShipments": float(r.get("lifetime_fraser_shipments") or 0),
                    "ytdFraserShipments": float(r.get("ytd_fraser_shipments") or 0),
                    "inventoryEnd": float(r.get("inventory_end") or 0),
                }

            return {
                "bookUid": bookUid,
                "year": year,
                "asOf": None,
                "formats": formats,
                "_source": {"financials": "sql", "books": "sql"},
            }