# routers/financials.py
from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Query
from psycopg.rows import dict_row

from app.core.db import db_conn

router = APIRouter(prefix="/financials", tags=["Financials"])


def _get_tenant_id(cur, tenant_slug: str | None = None) -> str:
    from app.tenants.workspace import workspace
    selected = workspace()
    if tenant_slug and tenant_slug != selected.slug:
        raise HTTPException(403, "Conflicting publisher workspace")
    tenant_slug = selected.slug
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
                  COALESCE(SUM(r.units_sold + r.unit_adjustment), 0) AS units_sold,
                  COALESCE(SUM(r.units_returned), 0) AS returns,
                  COALESCE(SUM(CASE WHEN r.currency='USD' THEN r.publisher_receipts ELSE 0 END), 0) AS publisher_receipts
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
                  COALESCE(SUM(f.units_sold + f.unit_adjustment), 0) AS fraser_units,
                  COALESCE(SUM(CASE WHEN f.currency='USD' THEN f.publisher_receipts ELSE 0 END), 0) AS fraser_dollars
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
                    COALESCE(SUM(r.units_sold + r.unit_adjustment - r.units_returned), 0) AS net_units
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

            from app.stable.service import ready
            stable = {}
            if ready(cur):
                cur.execute(f"""WITH latest AS (
                    SELECT DISTINCT ON(s.edition_id) s.edition_id,s.available,s.as_of
                    FROM stable_inventory_snapshots s JOIN stable_imports i
                      ON i.tenant_id=s.tenant_id AND i.id=s.import_id AND i.status='posted'
                    WHERE s.tenant_id=%s ORDER BY s.edition_id,s.as_of DESC
                  ) SELECT sum(l.available) AS available,min(l.as_of) AS as_of,count(*) AS matched
                  FROM latest l JOIN editions e ON e.id=l.edition_id AND e.tenant_id=%s
                  JOIN works w ON w.id=e.work_id AND w.tenant_id=e.tenant_id
                  WHERE (w.id::text=%s OR COALESCE(w.uid::text,'')=%s) {fmt_sql}""",
                  [tenant_id,tenant_id,bookUid,bookUid]+([fmt_filter] if fmt_filter!='ALL' else []))
                stable=cur.fetchone() or {}
                cur.execute(f"""SELECT COALESCE(sum(a.amount),0) AS deductions
                    FROM stable_receipt_adjustments a JOIN stable_imports i ON i.id=a.import_id AND i.tenant_id=a.tenant_id AND i.status='posted'
                    JOIN editions e ON e.id=a.edition_id AND e.tenant_id=a.tenant_id
                    JOIN works w ON w.id=e.work_id AND w.tenant_id=e.tenant_id
                    JOIN royalty_periods p ON p.id=a.period_id AND p.tenant_id=a.tenant_id
                    WHERE a.tenant_id=%s AND (w.id::text=%s OR COALESCE(w.uid::text,'')=%s)
                    AND p.period_code=ANY(%s) AND a.currency='USD' {fmt_sql}""",
                    [tenant_id,bookUid,bookUid,keys]+([fmt_filter] if fmt_filter!='ALL' else []))
                stable['deductions']=cur.fetchone()['deductions']

            return {
                "unitsSold": float(sales.get("units_sold") or 0),
                "returns": float(sales.get("returns") or 0),
                "freeCopies": float(inv_period.get("free_copies") or 0),
                "inventoryEnd": float(stable['available']) if stable.get('matched') else float(inv_end.get("inventory_end") or 0),
                "inventoryAsOf": stable.get('as_of'),
                "inventorySource": "daily_report" if stable.get('matched') else "movement_estimate",
                "adjustedReceipts": float(sales.get('publisher_receipts') or 0)+float(fr.get('fraser_dollars') or 0)-float(stable.get('deductions') or 0),
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
                    COALESCE(SUM(r.units_sold + r.unit_adjustment - r.units_returned), 0) AS lifetime_sold,
                    COALESCE(SUM(CASE WHEN rp.period_code LIKE %s THEN r.units_sold + r.unit_adjustment - r.units_returned ELSE 0 END), 0) AS ytd_sold
                  FROM edition_base eb
                  LEFT JOIN public.royalty_sales_lines r ON r.edition_id = eb.edition_id
                  LEFT JOIN public.royalty_periods rp ON rp.id = r.period_id
                  GROUP BY eb.fmt
                ),
                fraser AS (
                  SELECT
                    eb.fmt,
                    COALESCE(SUM(f.units_sold + f.unit_adjustment), 0) AS lifetime_fraser_sold,
                    COALESCE(SUM(CASE WHEN rp.period_code LIKE %s THEN f.units_sold + f.unit_adjustment ELSE 0 END), 0) AS ytd_fraser_sold
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

            from app.stable.service import ready
            current_stock = {}
            if ready(cur):
                cur.execute(f"""WITH latest AS (
                    SELECT DISTINCT ON(s.edition_id) s.edition_id,s.available,s.as_of
                    FROM stable_inventory_snapshots s JOIN stable_imports i
                      ON i.tenant_id=s.tenant_id AND i.id=s.import_id AND i.status='posted'
                    WHERE s.tenant_id=%s ORDER BY s.edition_id,s.as_of DESC
                  ) SELECT {FMT_CASE} AS fmt,sum(l.available) AS available,min(l.as_of) AS as_of
                  FROM latest l JOIN editions e ON e.id=l.edition_id AND e.tenant_id=%s
                  JOIN works w ON w.id=e.work_id AND w.tenant_id=e.tenant_id
                  WHERE (w.id::text=%s OR COALESCE(w.uid::text,'')=%s) GROUP BY {FMT_CASE}""",
                  (tenant_id,tenant_id,bookUid,bookUid))
                current_stock={str(r['fmt']).upper():r for r in cur.fetchall()}

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
                    "inventoryEnd": float(current_stock[fmt]['available']) if fmt in current_stock else float(r.get("inventory_end") or 0),
                    "inventoryAsOf": current_stock.get(fmt,{}).get('as_of'),
                }

            return {
                "bookUid": bookUid,
                "year": year,
                "asOf": None,
                "formats": formats,
                "_source": {"financials": "sql", "books": "sql"},
            }
