# routers/salesdata.py
from __future__ import annotations

from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Query
from psycopg.rows import dict_row

from app.core.db import db_conn

router = APIRouter(prefix="/salesdata", tags=["Sales Data"])


def _get_tenant_id_from_slug(cur, tenant_slug: str) -> str:
    cur.execute(
        """
        SELECT id
        FROM public.tenants
        WHERE slug = %s
        LIMIT 1
        """,
        (tenant_slug,),
    )
    row = cur.fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Tenant not found")
    return str(row["id"])


def _year_filter_sql(alias: str, year: Optional[str], params: List[Any]) -> str:
    if not year or year == "all":
        return ""
    yy = year[-2:]
    params.append(f"{yy}%")
    return f" AND {alias}.source_file LIKE %s "


def _format_filter_sql(alias: str, product_form: Optional[str], params: List[Any]) -> str:
    if not product_form or product_form == "all":
        return ""
    params.append(product_form)
    return f" AND {alias}.product_form = %s "


def _search_filter_sql(title_alias: str, q: Optional[str], params: List[Any]) -> str:
    if not q:
        return ""
    like_q = f"%{q.strip()}%"
    params.extend([like_q, like_q, like_q])
    return f"""
      AND (
        {title_alias}.title ILIKE %s
        OR COALESCE({title_alias}.subtitle, '') ILIKE %s
        OR EXISTS (
          SELECT 1
          FROM public.editions e2
          WHERE e2.work_id = {title_alias}.id
            AND e2.isbn13 ILIKE %s
        )
      )
    """


@router.get("/overview")
def get_overview(
    tenant_slug: str = Query(...),
    year: Optional[str] = Query(None),
) -> Dict[str, Any]:
    with db_conn() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            tenant_id = _get_tenant_id_from_slug(cur, tenant_slug)

            us_params: List[Any] = [tenant_id]
            us_year_sql = _year_filter_sql("r", year, us_params)

            cur.execute(
                f"""
                SELECT
                  COALESCE(SUM(r.publisher_receipts), 0) AS us_revenue,
                  COALESCE(SUM(r.units_sold), 0) AS us_units_sold
                FROM public.royalty_sales_lines r
                WHERE r.tenant_id = %s
                {us_year_sql}
                """,
                us_params,
            )
            us_row = cur.fetchone() or {}

            fr_params: List[Any] = [tenant_id]
            fr_year_sql = _year_filter_sql("f", year, fr_params)

            cur.execute(
                f"""
                SELECT
                  COALESCE(SUM(f.publisher_receipts), 0) AS fraser_cash,
                  COALESCE(SUM(f.units_sold), 0) AS fraser_units_sold
                FROM public.fraser_ca_sales_lines f
                WHERE f.tenant_id = %s
                {fr_year_sql}
                """,
                fr_params,
            )
            fr_row = cur.fetchone() or {}

            inv_params: List[Any] = [tenant_id]
            inv_year_sql = _year_filter_sql("m", year, inv_params)

            cur.execute(
                f"""
                WITH inventory AS (
                  SELECT
                    m.edition_id,
                    SUM(CASE WHEN m.movement_type = 'receipt' THEN m.quantity ELSE 0 END) AS receipts,
                    SUM(CASE WHEN m.movement_type = 'complimentary_shipment' THEN -m.quantity ELSE 0 END) AS complimentary_units,
                    SUM(CASE WHEN m.movement_type = 'shipment_to_fraser_CA' THEN -m.quantity ELSE 0 END) AS fraser_shipped
                  FROM public.inventory_movements m
                  WHERE m.tenant_id = %s
                  {inv_year_sql}
                  GROUP BY m.edition_id
                ),
                us_sales AS (
                  SELECT
                    r.edition_id,
                    SUM(r.units_sold) AS us_units_sold
                  FROM public.royalty_sales_lines r
                  WHERE r.tenant_id = %s
                  {us_year_sql}
                  GROUP BY r.edition_id
                )
                SELECT
                  COALESCE(SUM(
                    COALESCE(i.receipts, 0)
                    - COALESCE(i.complimentary_units, 0)
                    - COALESCE(i.fraser_shipped, 0)
                    - COALESCE(u.us_units_sold, 0)
                  ), 0) AS estimated_inventory
                FROM inventory i
                LEFT JOIN us_sales u ON u.edition_id = i.edition_id
                """,
                inv_params + us_params,
            )
            inv_row = cur.fetchone() or {}

            total_revenue = float(us_row.get("us_revenue") or 0) + float(fr_row.get("fraser_cash") or 0)

            return {
                "net_revenue": total_revenue,
                "us_units_sold": float(us_row.get("us_units_sold") or 0),
                "fraser_cash": float(fr_row.get("fraser_cash") or 0),
                "fraser_units_sold": float(fr_row.get("fraser_units_sold") or 0),
                "estimated_inventory": float(inv_row.get("estimated_inventory") or 0),
            }


@router.get("/revenue-trend")
def get_revenue_trend(
    tenant_slug: str = Query(...),
    year: Optional[str] = Query(None),
) -> Dict[str, Any]:
    with db_conn() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            tenant_id = _get_tenant_id_from_slug(cur, tenant_slug)

            params: List[Any] = [tenant_id, tenant_id]
            year_sql_r = _year_filter_sql("r", year, params)
            year_sql_f = _year_filter_sql("f", year, params)

            cur.execute(
                f"""
                WITH us_rev AS (
                  SELECT
                    LEFT(r.source_file, 4) AS period_code,
                    COALESCE(SUM(r.publisher_receipts), 0) AS us_revenue
                  FROM public.royalty_sales_lines r
                  WHERE r.tenant_id = %s
                  {year_sql_r}
                  GROUP BY LEFT(r.source_file, 4)
                ),
                fr_rev AS (
                  SELECT
                    LEFT(f.source_file, 4) AS period_code,
                    COALESCE(SUM(f.publisher_receipts), 0) AS fraser_revenue
                  FROM public.fraser_ca_sales_lines f
                  WHERE f.tenant_id = %s
                  {year_sql_f}
                  GROUP BY LEFT(f.source_file, 4)
                ),
                periods AS (
                  SELECT period_code FROM us_rev
                  UNION
                  SELECT period_code FROM fr_rev
                )
                SELECT
                  p.period_code,
                  COALESCE(u.us_revenue, 0) AS us_revenue,
                  COALESCE(f.fraser_revenue, 0) AS fraser_revenue,
                  COALESCE(u.us_revenue, 0) + COALESCE(f.fraser_revenue, 0) AS total_revenue
                FROM periods p
                LEFT JOIN us_rev u ON u.period_code = p.period_code
                LEFT JOIN fr_rev f ON f.period_code = p.period_code
                ORDER BY p.period_code
                """,
                params,
            )

            rows = cur.fetchall() or []
            return {"items": rows}


@router.get("/canada-snapshot")
def get_canada_snapshot(
    tenant_slug: str = Query(...),
    year: Optional[str] = Query(None),
    format: Optional[str] = Query(None),
    q: Optional[str] = Query(None),
) -> Dict[str, Any]:
    with db_conn() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            tenant_id = _get_tenant_id_from_slug(cur, tenant_slug)

            params: List[Any] = [tenant_id, tenant_id]
            year_sql_inv = _year_filter_sql("m", year, params)
            year_sql_fr = _year_filter_sql("f", year, params)
            format_sql = _format_filter_sql("e", format, params)
            search_sql = _search_filter_sql("w", q, params)

            cur.execute(
                f"""
                WITH shipped AS (
                  SELECT
                    m.edition_id,
                    SUM(-m.quantity) AS fraser_shipped
                  FROM public.inventory_movements m
                  WHERE m.tenant_id = %s
                    AND m.movement_type = 'shipment_to_fraser_CA'
                  {year_sql_inv}
                  GROUP BY m.edition_id
                ),
                sold AS (
                  SELECT
                    f.edition_id,
                    SUM(f.units_sold) AS fraser_sold,
                    SUM(f.publisher_receipts) AS fraser_cash
                  FROM public.fraser_ca_sales_lines f
                  WHERE f.tenant_id = %s
                  {year_sql_fr}
                  GROUP BY f.edition_id
                )
                SELECT
                  e.isbn13,
                  w.title,
                  w.subtitle,
                  e.product_form AS format,
                  COALESCE(s.fraser_shipped, 0) AS fraser_shipped,
                  COALESCE(c.fraser_sold, 0) AS fraser_sold,
                  COALESCE(c.fraser_cash, 0) AS fraser_cash
                FROM public.editions e
                JOIN public.works w
                  ON w.id = e.work_id
                 AND w.tenant_id = e.tenant_id
                LEFT JOIN shipped s ON s.edition_id = e.id
                LEFT JOIN sold c ON c.edition_id = e.id
                WHERE e.tenant_id = %s
                {format_sql}
                {search_sql}
                  AND (s.edition_id IS NOT NULL OR c.edition_id IS NOT NULL)
                ORDER BY COALESCE(c.fraser_cash, 0) DESC, w.title, w.subtitle, e.isbn13
                """,
                params + [tenant_id],
            )

            rows = cur.fetchall() or []

            total_shipped = sum(float(r["fraser_shipped"] or 0) for r in rows)
            total_sold = sum(float(r["fraser_sold"] or 0) for r in rows)
            total_cash = sum(float(r["fraser_cash"] or 0) for r in rows)

            return {
                "totals": {
                    "fraser_shipped": total_shipped,
                    "fraser_sold": total_sold,
                    "sell_through_pct": (total_sold / total_shipped * 100) if total_shipped > 0 else 0,
                    "fraser_cash": total_cash,
                },
                "items": rows,
            }


@router.get("/inventory-intelligence")
def get_inventory_intelligence(
    tenant_slug: str = Query(...),
    year: Optional[str] = Query(None),
    format: Optional[str] = Query(None),
    q: Optional[str] = Query(None),
) -> Dict[str, Any]:
    with db_conn() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            tenant_id = _get_tenant_id_from_slug(cur, tenant_slug)

            params: List[Any] = [tenant_id, tenant_id, tenant_id]
            year_sql_inv = _year_filter_sql("m", year, params)
            year_sql_us = _year_filter_sql("r", year, params)
            year_sql_fr = _year_filter_sql("f", year, params)
            format_sql = _format_filter_sql("e", format, params)
            search_sql = _search_filter_sql("w", q, params)

            cur.execute(
                f"""
                WITH inventory AS (
                  SELECT
                    m.edition_id,
                    SUM(CASE WHEN m.movement_type = 'receipt' THEN m.quantity ELSE 0 END) AS receipts,
                    SUM(CASE WHEN m.movement_type = 'complimentary_shipment' THEN -m.quantity ELSE 0 END) AS complimentary_units,
                    SUM(CASE WHEN m.movement_type = 'shipment_to_fraser_CA' THEN -m.quantity ELSE 0 END) AS fraser_shipped
                  FROM public.inventory_movements m
                  WHERE m.tenant_id = %s
                  {year_sql_inv}
                  GROUP BY m.edition_id
                ),
                us_sales AS (
                  SELECT
                    r.edition_id,
                    SUM(r.units_sold) AS us_units_sold
                  FROM public.royalty_sales_lines r
                  WHERE r.tenant_id = %s
                  {year_sql_us}
                  GROUP BY r.edition_id
                ),
                fr_sales AS (
                  SELECT
                    f.edition_id,
                    SUM(f.units_sold) AS fraser_units_sold,
                    SUM(f.publisher_receipts) AS fraser_cash
                  FROM public.fraser_ca_sales_lines f
                  WHERE f.tenant_id = %s
                  {year_sql_fr}
                  GROUP BY f.edition_id
                )
                SELECT
                  e.isbn13,
                  w.title,
                  w.subtitle,
                  e.product_form AS format,
                  COALESCE(i.receipts, 0) AS receipts,
                  COALESCE(i.complimentary_units, 0) AS complimentary_units,
                  COALESCE(i.fraser_shipped, 0) AS fraser_shipped,
                  COALESCE(u.us_units_sold, 0) AS us_units_sold,
                  COALESCE(f.fraser_units_sold, 0) AS fraser_units_sold,
                  COALESCE(f.fraser_cash, 0) AS fraser_cash,
                  (
                    COALESCE(i.receipts, 0)
                    - COALESCE(i.complimentary_units, 0)
                    - COALESCE(u.us_units_sold, 0)
                    - COALESCE(i.fraser_shipped, 0)
                  ) AS est_us_inventory
                FROM public.editions e
                JOIN public.works w
                  ON w.id = e.work_id
                 AND w.tenant_id = e.tenant_id
                LEFT JOIN inventory i ON i.edition_id = e.id
                LEFT JOIN us_sales u ON u.edition_id = e.id
                LEFT JOIN fr_sales f ON f.edition_id = e.id
                WHERE e.tenant_id = %s
                {format_sql}
                {search_sql}
                  AND (
                    i.edition_id IS NOT NULL
                    OR u.edition_id IS NOT NULL
                    OR f.edition_id IS NOT NULL
                  )
                ORDER BY COALESCE(i.receipts, 0) DESC, w.title, w.subtitle, e.isbn13
                """,
                params + [tenant_id],
            )

            rows = cur.fetchall() or []

            return {
                "totals": {
                    "receipts": sum(float(r["receipts"] or 0) for r in rows),
                    "us_units_sold": sum(float(r["us_units_sold"] or 0) for r in rows),
                    "fraser_shipped": sum(float(r["fraser_shipped"] or 0) for r in rows),
                    "est_us_inventory": sum(float(r["est_us_inventory"] or 0) for r in rows),
                },
                "items": rows,
            }


@router.get("/royalty-input")
def get_royalty_input(
    tenant_slug: str = Query(...),
    work_id: str = Query(...),
    period_start: Optional[str] = Query(None),
    period_end: Optional[str] = Query(None),
) -> Dict[str, Any]:
    with db_conn() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            tenant_id = _get_tenant_id_from_slug(cur, tenant_slug)

            params: List[Any] = [tenant_id]
            period_sql = ""

            if period_start and period_end:
                start_code = period_start[2:4] + period_start[5:7]
                end_code = period_end[2:4] + period_end[5:7]
                params.extend([start_code, end_code])
                period_sql = " AND SUBSTRING(r.source_file FROM 1 FOR 4) BETWEEN %s AND %s "

            ebook_pattern = "%Ebook_Sales_By_Title.csv"

            params.extend([work_id, work_id, ebook_pattern, ebook_pattern])

            cur.execute(
                f"""
                SELECT
                  e.id AS edition_id,
                  e.isbn13,
                  e.product_form AS format,
                  w.id AS work_sql_id,
                  w.uid AS work_uid,
                  w.title,
                  w.subtitle,
                  COALESCE(SUM(r.units_sold), 0) AS us_sold,
                  COALESCE(SUM(r.units_returned), 0) AS us_returns,
                  COALESCE(SUM(r.units_sold - r.units_returned), 0) AS us_net_units,
                  COALESCE(SUM(r.publisher_receipts), 0) AS us_cash
                FROM public.royalty_sales_lines r
                JOIN public.editions e
                  ON e.id = r.edition_id
                JOIN public.works w
                  ON w.id = e.work_id
                 AND w.tenant_id = e.tenant_id
                WHERE r.tenant_id = %s
                  {period_sql}
                  AND (
                    w.id::text = %s
                    OR COALESCE(w.uid::text, '') = %s
                  )
                  AND (
                    (
                      e.product_form = 'E-Book'
                      AND r.source_file LIKE %s
                    )
                    OR
                    (
                      e.product_form <> 'E-Book'
                      AND r.source_file NOT LIKE %s
                    )
                  )
                GROUP BY
                  e.id, e.isbn13, e.product_form,
                  w.id, w.uid, w.title, w.subtitle
                ORDER BY e.product_form, e.isbn13
                """,
                params,
            )

            rows = cur.fetchall() or []

            return {
                "items": [
                    {
                        "edition_id": str(row["edition_id"]) if row.get("edition_id") else None,
                        "isbn13": row.get("isbn13"),
                        "workSqlId": str(row["work_sql_id"]) if row.get("work_sql_id") else None,
                        "workUid": str(row["work_uid"]) if row.get("work_uid") else None,
                        "title": row.get("title"),
                        "subtitle": row.get("subtitle"),
                        "format": row.get("format"),
                        "periodStart": period_start,
                        "periodEnd": period_end,
                        "usSold": float(row.get("us_sold") or 0),
                        "usReturns": float(row.get("us_returns") or 0),
                        "usNetUnits": float(row.get("us_net_units") or 0),
                        "usCash": float(row.get("us_cash") or 0),
                    }
                    for row in rows
                ]
            }


@router.get("/title-performance")
def get_title_performance(
    tenant_slug: str = Query(...),
    year: Optional[str] = Query(None),
    format: Optional[str] = Query(None),
    channel: Optional[str] = Query(None),
    q: Optional[str] = Query(None),
) -> Dict[str, Any]:
    with db_conn() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            tenant_id = _get_tenant_id_from_slug(cur, tenant_slug)

            inv_params: List[Any] = [tenant_id]
            us_params: List[Any] = [tenant_id]
            fr_params: List[Any] = [tenant_id]
            outer_params: List[Any] = [year if year and year != "all" else "all", tenant_id]

            year_sql_inv = _year_filter_sql("m", year, inv_params)
            year_sql_us = _year_filter_sql("r", year, us_params)
            year_sql_fr = _year_filter_sql("f", year, fr_params)

            filter_params: List[Any] = []
            format_sql = _format_filter_sql("e", format, filter_params)
            search_sql = _search_filter_sql("w", q, filter_params)

            channel_sql = ""
            if channel == "us":
                channel_sql = " AND COALESCE(u.us_units_sold, 0) > 0 "
            elif channel == "fraser":
                channel_sql = " AND (COALESCE(i.fraser_shipped, 0) > 0 OR COALESCE(f.fraser_units_sold, 0) > 0) "

            sql = f"""
                WITH inventory AS (
                  SELECT
                    m.edition_id,
                    SUM(CASE WHEN m.movement_type = 'receipt' THEN m.quantity ELSE 0 END) AS receipts,
                    SUM(CASE WHEN m.movement_type = 'shipment_to_fraser_CA' THEN -m.quantity ELSE 0 END) AS fraser_shipped
                  FROM public.inventory_movements m
                  WHERE m.tenant_id = %s
                  {year_sql_inv}
                  GROUP BY m.edition_id
                ),
                us_sales AS (
                  SELECT
                    r.edition_id,
                    SUM(r.units_sold) AS us_units_sold
                  FROM public.royalty_sales_lines r
                  WHERE r.tenant_id = %s
                  {year_sql_us}
                  GROUP BY r.edition_id
                ),
                fr_sales AS (
                  SELECT
                    f.edition_id,
                    SUM(f.units_sold) AS fraser_units_sold,
                    SUM(f.publisher_receipts) AS fraser_cash
                  FROM public.fraser_ca_sales_lines f
                  WHERE f.tenant_id = %s
                  {year_sql_fr}
                  GROUP BY f.edition_id
                )
                SELECT
                  e.isbn13,
                  w.title,
                  w.subtitle,
                  e.product_form AS format,
                  %s::text AS selected_year,
                  COALESCE(i.receipts, 0) AS receipts,
                  COALESCE(u.us_units_sold, 0) AS us_sold,
                  COALESCE(i.fraser_shipped, 0) AS fraser_shipped,
                  COALESCE(f.fraser_units_sold, 0) AS fraser_sold,
                  COALESCE(f.fraser_cash, 0) AS fraser_cash,
                  (
                    COALESCE(i.receipts, 0)
                    - COALESCE(u.us_units_sold, 0)
                    - COALESCE(i.fraser_shipped, 0)
                  ) AS est_us_inventory
                FROM public.editions e
                JOIN public.works w
                  ON w.id = e.work_id
                 AND w.tenant_id = e.tenant_id
                LEFT JOIN inventory i ON i.edition_id = e.id
                LEFT JOIN us_sales u ON u.edition_id = e.id
                LEFT JOIN fr_sales f ON f.edition_id = e.id
                WHERE e.tenant_id = %s
                {format_sql}
                {search_sql}
                {channel_sql}
                  AND (
                    i.edition_id IS NOT NULL
                    OR u.edition_id IS NOT NULL
                    OR f.edition_id IS NOT NULL
                  )
                ORDER BY COALESCE(f.fraser_cash, 0) DESC, w.title, w.subtitle, e.isbn13
            """

            params = inv_params + us_params + fr_params + outer_params + filter_params

            cur.execute(sql, params)
            rows = cur.fetchall() or []

            items = []
            for row in rows:
                year_value = row.get("selected_year") or "all"
                items.append(
                    {
                        "isbn13": row["isbn13"],
                        "title": row["title"],
                        "subtitle": row["subtitle"],
                        "format": row["format"],
                        "year": year_value,
                        "channel": "Fraser" if float(row["fraser_cash"] or 0) > 0 else "US",
                        "receipts": float(row["receipts"] or 0),
                        "usSold": float(row["us_sold"] or 0),
                        "fraserShipped": float(row["fraser_shipped"] or 0),
                        "fraserSold": float(row["fraser_sold"] or 0),
                        "fraserCash": float(row["fraser_cash"] or 0),
                        "estUsInventory": float(row["est_us_inventory"] or 0),
                    }
                )

            return {"items": items}