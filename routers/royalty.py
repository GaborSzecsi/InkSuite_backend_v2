# marble_app/routers/royalty.py

from __future__ import annotations

from typing import List, Dict, Any, Optional
from pathlib import Path
import logging
import json
import traceback
import base64
import uuid as uuid_lib
from datetime import datetime
from decimal import Decimal, InvalidOperation
import os, glob, shutil, subprocess, tempfile
from pydantic import BaseModel, Field
from psycopg.rows import dict_row
from fastapi import APIRouter, Request, HTTPException
from psycopg.rows import dict_row
from app.core.db import db_conn

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import HTMLResponse, Response

from models.royalty import (
    Book,
    RoyaltyStatementRequest,
)

from services.royalty_calculator import RoyaltyCalculator

logger = logging.getLogger(__name__)


# -----------------------------
# Utility: find Ghostscript
# -----------------------------
def find_gs_exe() -> str | None:
    for cand in ("gswin64c.exe", "gswin32c.exe", "gs"):
        p = shutil.which(cand)
        if p:
            return p
    for base in (r"C:\Program Files\gs", r"C:\Program Files (x86)\gs"):
        for exe in ("gswin64c.exe", "gswin32c.exe"):
            hits = sorted(glob.glob(os.path.join(base, "gs*", "bin", exe)), reverse=True)
            if hits:
                return hits[0]
    return None


router = APIRouter(prefix="/royalty", tags=["royalty"])
calculator = RoyaltyCalculator()

# -----------------------------
# Paths
# -----------------------------
LOGO_PATH = Path(os.getenv("MARBLE_LOGO_PATH", r"C:\Users\szecs\Documents\marble_app\assets\logo long2 NEW.png"))
UPLOADS_DIR = Path(os.getenv("MARBLE_UPLOADS_DIR", r"C:\Users\szecs\Documents\marble_app\data\uploads"))
ROYALTY_DATA_DIR = Path(os.getenv("MARBLE_ROYALTY_DATA_DIR", r"C:\Users\szecs\Documents\marble_app\book_data"))

# Global override (debug): force ReportLab instead of Playwright
FORCE_REPORTLAB_PDFS = False
FORCE_RASTERIZED_PDF = False
RASTER_DPI = 300
FORCE_WEASYPRINT = True


# =============================
#     Catalog work list / resolve (SQL)
# =============================
def _tenant_slug() -> str:
    return (os.getenv("NEXT_PUBLIC_TENANT_SLUG") or os.getenv("TENANT_SLUG") or "marble-press").strip()


def _import_catalog_mod():
    try:
        import routers.catalog as cat
        return cat
    except Exception:
        try:
            from routers import catalog as cat  # type: ignore
            return cat
        except Exception:
            return None


def _list_books_from_catalog(limit: int = 200) -> list[dict]:
    """Same item shape as GET /api/catalog/works (for royalty book picker)."""
    cat = _import_catalog_mod()
    if not cat:
        print("[royalty] catalog module unavailable for book list")
        return []
    try:
        from app.core.db import db_conn
        from psycopg.rows import dict_row
    except Exception as e:
        print("[royalty] DB import failed for book list:", e)
        return []
    try:
        with db_conn() as conn:
            with conn.cursor(row_factory=dict_row) as cur:
                tenant_id = cat._get_tenant_id_from_slug(cur, _tenant_slug())
                cur.execute(
                    """
                    SELECT w.*
                    FROM works w
                    WHERE w.tenant_id = %s
                    ORDER BY w.updated_at DESC NULLS LAST, w.created_at DESC
                    LIMIT %s
                    """,
                    (tenant_id, limit),
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
                            if cat._is_author_role(cat._safe_str(r.get("contributor_role"))):
                                preferred = r
                                break
                        if preferred is None and grp:
                            preferred = grp[0]
                        if preferred and preferred.get("author"):
                            author_by_work[wid] = cat._clean_display_name(preferred.get("author"))
                items: list[dict] = []
                for r in rows:
                    it = cat._work_row_to_list_item(r)
                    wid = str(r.get("id", ""))
                    if wid in author_by_work:
                        it["author"] = author_by_work[wid]
                    items.append(it)
                return items
    except Exception as e:
        print("[royalty] catalog book list failed:", e)
        traceback.print_exc()
        return []


def _catalog_agent_list_or_card_to_agent_dict(val: Any) -> Optional[dict]:
    """
    Book.author_agent / Illustrator.agent expect a single Agent-shaped dict.
    Catalog may use a list of agent rows (agent_name, role_label, …) or an agency card dict.
    Contact categories also flatten to top-level keys like author_agent = [rows].
    """
    if val is None:
        return None
    if isinstance(val, list):
        if not val:
            return None
        val = val[0]
    if not isinstance(val, dict):
        return None
    if val.get("agent_name") is not None or val.get("role_label") is not None:
        return {
            "name": str(val.get("agent_name") or val.get("name") or "").strip(),
            "agency": str(val.get("role_label") or val.get("agency") or "").strip(),
            "email": str(val.get("agent_email") or val.get("email") or "").strip(),
            "address": val.get("address"),
        }
    name = val.get("agent") or val.get("contact") or val.get("name") or ""
    return {
        "name": str(name).strip(),
        "agency": str(val.get("agency") or "").strip(),
        "email": str(val.get("email") or "").strip(),
        "address": val.get("address"),
    }


def _normalize_catalog_payload_for_royalty_book(payload: dict) -> dict:
    """Map catalog full-work JSON to shapes Book (Pydantic) accepts."""
    out = dict(payload)
    auth = out.get("author")
    if isinstance(auth, dict):
        nm = auth.get("name") or auth.get("display_name") or ""
        out["author"] = str(nm).strip() if nm else ""
    aa = out.get("author_agent")
    if aa is not None:
        out["author_agent"] = _catalog_agent_list_or_card_to_agent_dict(aa)
    elif isinstance(out.get("author_agency"), dict) and not _is_blank_agentish(out["author_agency"]):
        out["author_agent"] = _catalog_agent_list_or_card_to_agent_dict(out["author_agency"])

    ill = out.get("illustrator")
    if isinstance(ill, dict):
        ill = dict(ill)
        if not (ill.get("name") or "").strip() and ill.get("display_name"):
            ill["name"] = str(ill["display_name"]).strip()
        ill["agent"] = _catalog_agent_list_or_card_to_agent_dict(ill.get("agent"))
        out["illustrator"] = ill
    return out


def _is_blank_agentish(d: dict) -> bool:
    return not any(str(v or "").strip() for k, v in d.items() if k != "address")


def _fetch_book_dict_from_catalog(request: RoyaltyStatementRequest) -> Optional[dict]:
    """Load full work payload from PostgreSQL (same source as /api/catalog/works)."""
    cat = _import_catalog_mod()
    if not cat:
        print("[royalty] cannot import catalog module")
        return None
    slug = _tenant_slug()
    uid = (request.uid or "").strip()
    work_id = (request.work_id or "").strip()
    if not uid and not work_id:
        return None
    try:
        from app.core.db import db_conn
        from psycopg.rows import dict_row
    except Exception as e:
        print("[royalty] catalog DB import failed:", e)
        return None

    def _parse_uuid(val: str) -> Optional[uuid_lib.UUID]:
        if not val:
            return None
        try:
            return uuid_lib.UUID(str(val).strip())
        except Exception:
            return None

    try:
        with db_conn() as conn:
            with conn.cursor(row_factory=dict_row) as cur:
                try:
                    tenant_id = cat._get_tenant_id_from_slug(cur, slug)
                except HTTPException:
                    return None

                resolved: Optional[str] = None
                wid_u = _parse_uuid(work_id)
                if wid_u:
                    cur.execute(
                        "SELECT id FROM works WHERE tenant_id = %s AND id = %s LIMIT 1",
                        (tenant_id, wid_u),
                    )
                    row = cur.fetchone()
                    if row:
                        resolved = str(row["id"])
                if not resolved:
                    uid_u = _parse_uuid(uid)
                    if uid_u:
                        cur.execute(
                            "SELECT id FROM works WHERE tenant_id = %s AND uid = %s LIMIT 1",
                            (tenant_id, uid_u),
                        )
                        row = cur.fetchone()
                        if row:
                            resolved = str(row["id"])
                if not resolved and uid:
                    uid_as_id = _parse_uuid(uid)
                    if uid_as_id:
                        cur.execute(
                            "SELECT id FROM works WHERE tenant_id = %s AND id = %s LIMIT 1",
                            (tenant_id, uid_as_id),
                        )
                        row = cur.fetchone()
                        if row:
                            resolved = str(row["id"])
                if not resolved:
                    return None
                payload = cat._build_full_work_payload(cur, tenant_id, resolved)
    except Exception as e:
        print("[royalty] catalog book load failed:", e)
        traceback.print_exc()
        return None

    return _normalize_catalog_payload_for_royalty_book(payload)


def _resolve_book_dict(request: RoyaltyStatementRequest) -> Optional[dict]:
    return _fetch_book_dict_from_catalog(request)


def _calc_with_db_history(
    request: RoyaltyStatementRequest,
    book: Book,
    book_data: dict,
) -> Dict[str, Any]:
    """Run calculator with per-work statement history from royalty_statements when available."""
    bid = str(
        (book.uid or request.uid or request.work_id or book_data.get("uid") or book_data.get("id") or "")
    ).strip()
    work_pk = str(book_data.get("id") or "").strip()
    author_h: Optional[list] = None
    ill_h: Optional[list] = None
    if work_pk:
        try:
            from app.core.db import db_conn
            from psycopg.rows import dict_row
            from services import royalty_statement_db as rsdb

            cat = _import_catalog_mod()
            if cat:
                with db_conn() as conn:
                    with conn.cursor(row_factory=dict_row) as cur:
                        tenant_id = cat._get_tenant_id_from_slug(cur, _tenant_slug())
                        a, i = rsdb.load_work_statement_histories(cur, tenant_id, work_pk)
                author_h = a if a else None
                ill_h = i if i else None
        except Exception as e:
            print("[royalty] DB statement history unavailable, using JSON fallback:", e)
            traceback.print_exc()

    return calculator.calculate_royalties(
        request,
        book,
        book_id=bid,
        author_statement_history=author_h,
        illustrator_statement_history=ill_h,
    )


def _persist_statement_calculations_to_db(
    request: RoyaltyStatementRequest,
    book_data: dict,
    calcs: Dict[str, Any],
) -> None:
    work_pk = str(book_data.get("id") or "").strip()
    if not work_pk:
        return
    try:
        from app.core.db import db_conn
        from psycopg.rows import dict_row
        from services import royalty_statement_db as rsdb

        cat = _import_catalog_mod()
        if not cat:
            return
        with db_conn() as conn:
            with conn.cursor(row_factory=dict_row) as cur:
                tenant_id = cat._get_tenant_id_from_slug(cur, _tenant_slug())
                for party_key in ("author", "illustrator"):
                    block = calcs.get(party_key) or {}
                    rsdb.upsert_statement(
                        cur,
                        tenant_id,
                        work_pk,
                        party_key,
                        request.period_start,
                        request.period_end,
                        block,
                    )
    except Exception as e:
        print("[royalty] could not persist statement to DB:", e)
        traceback.print_exc()

# =============================
#     Subrights / Period APIs
# =============================

class SubrightsIncomeItem(BaseModel):
    period_id: str
    work_id: str
    royalty_set_id: str
    subrights_type_id: str
    income_date: str
    publisher_receipts: Decimal = Field(..., ge=0)


class SubrightsIncomeCreateBody(BaseModel):
    items: List[SubrightsIncomeItem]


def _get_tenant_id_for_royalty(cur) -> str:
    cat = _import_catalog_mod()
    if not cat:
        raise HTTPException(status_code=500, detail="Catalog module unavailable.")
    try:
        return str(cat._get_tenant_id_from_slug(cur, _tenant_slug()))
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Could not resolve tenant: {e}")


def _get_existing_columns(cur, table_name: str) -> set[str]:
    cur.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = %s
        """,
        (table_name,),
    )
    return {str(r["column_name"]) for r in (cur.fetchall() or [])}


def _resolve_active_royalty_set_id(cur, tenant_id: str, work_id: str) -> str:
    cur.execute(
        """
        SELECT id::text
        FROM royalty_sets
        WHERE tenant_id = %s::uuid
          AND work_id = %s::uuid
        ORDER BY is_active DESC, version DESC, created_at DESC, id DESC
        LIMIT 1
        """,
        (tenant_id, work_id),
    )
    row = cur.fetchone()
    if not row:
        raise HTTPException(
            status_code=404,
            detail=f"No royalty set found for work_id={work_id}",
        )
    return str(row["id"])


def _assert_period_exists(cur, tenant_id: str, period_id: str) -> None:
    cur.execute(
        """
        SELECT 1
        FROM royalty_periods
        WHERE tenant_id = %s::uuid
          AND id = %s::uuid
        LIMIT 1
        """,
        (tenant_id, period_id),
    )
    if not cur.fetchone():
        raise HTTPException(status_code=404, detail=f"Royalty period not found: {period_id}")


def _assert_work_exists(cur, tenant_id: str, work_id: str) -> None:
    cur.execute(
        """
        SELECT 1
        FROM works
        WHERE tenant_id = %s::uuid
          AND id = %s::uuid
        LIMIT 1
        """,
        (tenant_id, work_id),
    )
    if not cur.fetchone():
        raise HTTPException(status_code=404, detail=f"Work not found: {work_id}")


def _assert_royalty_set_belongs_to_work(cur, tenant_id: str, royalty_set_id: str, work_id: str) -> None:
    cur.execute(
        """
        SELECT 1
        FROM royalty_sets
        WHERE tenant_id = %s::uuid
          AND id = %s::uuid
          AND work_id = %s::uuid
        LIMIT 1
        """,
        (tenant_id, royalty_set_id, work_id),
    )
    if not cur.fetchone():
        raise HTTPException(
            status_code=400,
            detail=f"royalty_set_id {royalty_set_id} does not belong to work_id {work_id}",
        )


def _load_subrights_options_for_work(cur, tenant_id: str, work_id: str) -> List[Dict[str, Any]]:
    royalty_set_id = _resolve_active_royalty_set_id(cur, tenant_id, work_id)

    cur.execute(
        """
        SELECT
            rr.id::text AS rule_id,
            rr.party::text AS party,
            rr.mode::text AS mode,
            rr.base::text AS base,
            rr.percent,
            rr.flat_rate_percent,
            rr.subrights_type_id::text AS subrights_type_id,
            st.name
        FROM royalty_rules rr
        JOIN subrights_types st
          ON st.id = rr.subrights_type_id
        WHERE rr.tenant_id = %s::uuid
          AND rr.royalty_set_id = %s::uuid
          AND rr.rights_type = 'subrights'::roy_rights_type
          AND rr.subrights_type_id IS NOT NULL
        ORDER BY st.name ASC, rr.party ASC, rr.id ASC
        """,
        (tenant_id, royalty_set_id),
    )

    rows = cur.fetchall() or []
    grouped: Dict[str, Dict[str, Any]] = {}

    for r in rows:
        stid = str(r["subrights_type_id"])
        rec = grouped.setdefault(
            stid,
            {
                "rule_id": str(r["rule_id"]),
                "subrights_type_id": stid,
                "name": str(r.get("name") or ""),
                "base": str(r.get("base") or "net_receipts"),
                "author_percent": None,
                "illustrator_percent": None,
                "author_mode": None,
                "illustrator_mode": None,
            },
        )

        party = str(r.get("party") or "").lower()
        pct_val = r.get("percent")
        if pct_val is None:
            pct_val = r.get("flat_rate_percent")
        pct_num = float(pct_val) if pct_val is not None else None

        if party == "author":
            rec["author_percent"] = pct_num
            rec["author_mode"] = r.get("mode")
        elif party == "illustrator":
            rec["illustrator_percent"] = pct_num
            rec["illustrator_mode"] = r.get("mode")

        # keep whichever base is stored on the rule; subrights should be net_receipts
        if r.get("base") is not None:
            rec["base"] = str(r["base"])

    return list(grouped.values())


def _insert_subrights_income_row(cur, tenant_id: str, item: SubrightsIncomeItem) -> None:
    cols = _get_existing_columns(cur, "subrights_income_lines")

    insert_cols: List[str] = [
        "tenant_id",
        "period_id",
        "work_id",
        "subrights_type_id",
        "publisher_receipts",
    ]
    insert_vals: List[Any] = [
        tenant_id,
        item.period_id,
        item.work_id,
        item.subrights_type_id,
        item.publisher_receipts,
    ]

    if "income_date" in cols:
        insert_cols.append("income_date")
        insert_vals.append(item.income_date)
    elif "transaction_date" in cols:
        insert_cols.append("transaction_date")
        insert_vals.append(item.income_date)

    if "royalty_set_id" in cols:
        insert_cols.append("royalty_set_id")
        insert_vals.append(item.royalty_set_id)

    if "gross_amount" in cols:
        insert_cols.append("gross_amount")
        insert_vals.append(item.publisher_receipts)

    placeholders = ", ".join(["%s"] * len(insert_cols))
    sql = f"""
        INSERT INTO subrights_income_lines ({", ".join(insert_cols)})
        VALUES ({placeholders})
    """
    cur.execute(sql, insert_vals)


@router.get("/periods")
def get_royalty_periods() -> List[Dict[str, Any]]:
    try:
        from app.core.db import db_conn
        with db_conn() as conn:
            with conn.cursor(row_factory=dict_row) as cur:
                tenant_id = _get_tenant_id_for_royalty(cur)
                cur.execute(
                    """
                    SELECT
                        id::text AS id,
                        period_code,
                        period_start::text AS period_start,
                        period_end::text AS period_end,
                        COALESCE(is_closed, false) AS is_closed,

                        CASE
                            WHEN period_code LIKE '%%-H1'
                                THEN split_part(period_code, '-', 1) || ' H1 (Jan 1 – Jun 30)'
                            WHEN period_code LIKE '%%-H2'
                                THEN split_part(period_code, '-', 1) || ' H2 (Jul 1 – Dec 31)'
                            ELSE period_code
                        END AS display_label

                    FROM royalty_periods
                    WHERE tenant_id = %s::uuid
                      AND period_code ~ '^\d{4}-H[12]$'
                    ORDER BY period_start DESC, period_end DESC
                    """,
                    (tenant_id,),
                )
                rows = cur.fetchall() or []
                return [dict(r) for r in rows]
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Could not load royalty periods: {e}")


@router.get("/subrights/options")
def get_subrights_options(work_id: str = Query(...)) -> List[Dict[str, Any]]:
    """
    Returns subrights options for the active royalty set on a work.
    Output shape:
    [{
      rule_id,
      subrights_type_id,
      name,
      base,
      author_percent,
      illustrator_percent,
      author_mode,
      illustrator_mode
    }]
    """
    try:
        from app.core.db import db_conn
        with db_conn() as conn:
            with conn.cursor(row_factory=dict_row) as cur:
                tenant_id = _get_tenant_id_for_royalty(cur)
                _assert_work_exists(cur, tenant_id, work_id)
                return _load_subrights_options_for_work(cur, tenant_id, work_id)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Could not load subrights options: {e}")


@router.post("/subrights/income")
def create_subrights_income_rows(body: SubrightsIncomeCreateBody) -> Dict[str, Any]:
    """
    Saves manual subrights receipt rows for a royalty period.
    Input:
    {
      "items": [{
        "period_id",
        "work_id",
        "royalty_set_id",
        "subrights_type_id",
        "income_date",
        "publisher_receipts"
      }]
    }
    """
    if not body.items:
        raise HTTPException(status_code=400, detail="No subrights income rows were provided.")

    try:
        from app.core.db import db_conn
        with db_conn() as conn:
            prev_ac = conn.autocommit
            conn.autocommit = False
            try:
                with conn.cursor(row_factory=dict_row) as cur:
                    tenant_id = _get_tenant_id_for_royalty(cur)

                    for item in body.items:
                        _assert_period_exists(cur, tenant_id, item.period_id)
                        _assert_work_exists(cur, tenant_id, item.work_id)
                        _assert_royalty_set_belongs_to_work(cur, tenant_id, item.royalty_set_id, item.work_id)

                        cur.execute(
                            """
                            SELECT 1
                            FROM subrights_types
                            WHERE id = %s::uuid
                            LIMIT 1
                            """,
                            (item.subrights_type_id,),
                        )
                        if not cur.fetchone():
                            raise HTTPException(
                                status_code=404,
                                detail=f"Subrights type not found: {item.subrights_type_id}",
                            )

                        _insert_subrights_income_row(cur, tenant_id, item)

                conn.commit()
            except Exception:
                conn.rollback()
                raise
            finally:
                conn.autocommit = prev_ac

        return {"message": "Saved", "saved_count": len(body.items)}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Could not save subrights income rows: {e}")

def _require_tenant(request: Request) -> str:
    tenant_slug = request.headers.get("X-Tenant")
    if not tenant_slug:
        raise HTTPException(status_code=403, detail="X-Tenant header required for royalty")

    with db_conn() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                "SELECT id FROM tenants WHERE slug = %s LIMIT 1",
                (tenant_slug,)
            )
            row = cur.fetchone()

            if not row:
                raise HTTPException(status_code=404, detail="Tenant not found")

            return row["id"]

# =============================
#           ROUTES
# =============================

@router.get("/")
def info():
    books = _list_books_from_catalog()
    total = len(books)
    return {
        "message": "Royalty Calculator API",
        "endpoints": {
            "books": "/api/royalty/books",
            "save_book": "/api/royalty/books (POST)",
            "delete_book": "/api/royalty/books (DELETE)",
            "calculate": "/api/royalty/calculate (POST)",
            "statements": "/api/royalty/statements (POST)",
            "render": "/api/royalty/render (POST) - Generate HTML/PDF statement",
            "get_statements": "/api/royalty/statements/{person_type}/{person_name}",
            "delete_statement": "/api/royalty/statements/{person_type}/{person_name} (DELETE)",
            "categories": "/api/royalty/categories",
            "format_types": "/api/royalty/format-types",
            "periods": "/api/royalty/periods",
            "subrights_options": "/api/royalty/subrights/options?work_id=...",
            "subrights_income": "/api/royalty/subrights/income (POST)",
        },
        "total_books": total,
    }


@router.get("/books", response_model=List[Dict[str, Any]])
def get_books(request: Request):
    """Top-level JSON array of works from the catalog database, enriched with active royalty set id."""
    try:
        books = _list_books_from_catalog() or []

        if not books:
            return []

        tenant_id = _require_tenant(request)

        work_ids = []
        for b in books:
            work_id = b.get("id") or b.get("work_id")
            if work_id:
                work_ids.append(str(work_id))

        royalty_by_work: Dict[str, Dict[str, Any]] = {}

        if work_ids:
            with db_conn() as conn:
                with conn.cursor(row_factory=dict_row) as cur:
                    cur.execute(
                        """
                        SELECT DISTINCT ON (rs.work_id)
                            rs.work_id::text AS work_id,
                            rs.id::text AS active_royalty_set_id,
                            rs.version
                        FROM royalty_sets rs
                        WHERE rs.tenant_id = %s::uuid
                          AND rs.work_id::text = ANY(%s)
                        ORDER BY rs.work_id, rs.is_active DESC, rs.version DESC, rs.created_at DESC, rs.id DESC
                        """,
                        (tenant_id, work_ids),
                    )
                    for row in cur.fetchall() or []:
                        royalty_by_work[str(row["work_id"])] = {
                            "active_royalty_set_id": row["active_royalty_set_id"],
                            "active_royalty_set_version": row["version"],
                        }

        enriched: List[Dict[str, Any]] = []
        for b in books:
            item = dict(b)
            work_id = str(item.get("id") or item.get("work_id") or "")
            extra = royalty_by_work.get(work_id, {})
            item["active_royalty_set_id"] = extra.get("active_royalty_set_id")
            item["active_royalty_set_version"] = extra.get("active_royalty_set_version")
            enriched.append(item)

        return enriched

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Unexpected error: {e}")


@router.post("/books")
def save_book(payload: Dict[str, Any]):
    """Books live in SQL; use POST /api/catalog/works (or the catalog UI) to create or update works."""
    raise HTTPException(
        status_code=410,
        detail="Royalty JSON book storage is removed. Save works via POST /api/catalog/works.",
    )

@router.get("/periods")
def get_periods(request: Request):
    tenant_id = _require_tenant(request)

    with db_conn() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute("""
                SELECT
                    id,
                    period_code,
                    period_start,
                    period_end,
                    is_closed
                FROM royalty_periods
                WHERE tenant_id = %s
                ORDER BY period_start DESC
            """, (tenant_id,))

            rows = cur.fetchall() or []

    return rows

@router.get("/subrights/options")
def get_subrights_options(work_id: str, request: Request):
    tenant_id = _require_tenant(request)

    with db_conn() as conn:
        with conn.cursor(row_factory=dict_row) as cur:

            # get active royalty set
            cur.execute("""
                SELECT id
                FROM royalty_sets
                WHERE tenant_id = %s
                  AND work_id = %s
                  AND is_active = true
                LIMIT 1
            """, (tenant_id, work_id))

            rs = cur.fetchone()
            if not rs:
                return []

            royalty_set_id = rs["id"]

            # get subrights rules
            cur.execute("""
                SELECT
                    rr.id AS rule_id,
                    rr.subrights_type_id,
                    st.name,
                    rr.base,

                    -- author %
                    MAX(CASE WHEN rr.party = 'author' THEN rr.percent END) AS author_percent,

                    -- illustrator %
                    MAX(CASE WHEN rr.party = 'illustrator' THEN rr.percent END) AS illustrator_percent,

                    MAX(CASE WHEN rr.party = 'author' THEN rr.mode END) AS author_mode,
                    MAX(CASE WHEN rr.party = 'illustrator' THEN rr.mode END) AS illustrator_mode

                FROM royalty_rules rr
                JOIN subrights_types st
                  ON st.id = rr.subrights_type_id

                WHERE rr.tenant_id = %s
                  AND rr.royalty_set_id = %s
                  AND rr.rights_type = 'subrights'

                GROUP BY rr.id, rr.subrights_type_id, st.name, rr.base
                ORDER BY st.name
            """, (tenant_id, royalty_set_id))

            rows = cur.fetchall() or []

    return rows

from pydantic import BaseModel
from typing import List
from datetime import date


class SubrightsIncomeItem(BaseModel):
    period_id: str
    work_id: str
    royalty_set_id: str
    subrights_type_id: str
    income_date: date
    publisher_receipts: float


class SubrightsIncomePayload(BaseModel):
    items: List[SubrightsIncomeItem]


@router.post("/subrights/income")
def save_subrights_income(payload: SubrightsIncomePayload, request: Request):
    tenant_id = _require_tenant(request)

    if not payload.items:
        return {"ok": True, "inserted": 0}

    with db_conn() as conn:
        with conn.cursor() as cur:

            for item in payload.items:
                cur.execute("""
                    INSERT INTO subrights_income_lines (
                        tenant_id,
                        period_id,
                        work_id,
                        royalty_set_id,
                        subrights_type_id,
                        income_date,
                        publisher_receipts,
                        created_at,
                        updated_at
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, now(), now())
                """, (
                    tenant_id,
                    item.period_id,
                    item.work_id,
                    item.royalty_set_id,
                    item.subrights_type_id,
                    item.income_date,
                    item.publisher_receipts
                ))

        conn.commit()

    return {"ok": True, "inserted": len(payload.items)}


@router.delete("/books")
def delete_book(title: str, author: str):
    raise HTTPException(
        status_code=410,
        detail="Royalty JSON book storage is removed. Delete or archive works via the catalog API.",
    )


@router.post("/calculate")
def calculate_royalties(request: RoyaltyStatementRequest):
    """
    Finds the requested work by uid or work_id and returns the calculation dict (catalog / SQL only).
    """
    logger.info(
        "POST /api/royalty/calculate work_id=%s uid=%s period=%s..%s",
        request.work_id,
        request.uid,
        request.period_start,
        request.period_end,
    )
    book_data = _resolve_book_dict(request)
    if not book_data:
        raise HTTPException(
            status_code=404,
            detail=f"Book not found for uid/work_id: {request.uid or request.work_id}",
        )
    book = Book.model_validate(book_data)
    calcs = _calc_with_db_history(request, book, book_data)
    return {"message": "OK", "calculations": calcs}


@router.post("/statements")
def save_royalty_statement(request: RoyaltyStatementRequest):
    """
    Saves royalty statements and generates PDFs conditionally:
    - Author: if there is any sales/royalty row (same as before)
    - Illustrator: ONLY if there is at least one royalty % > 0
    Resolves the work from the catalog (SQL) only.
    """
    def has_party_rows(pdata: Dict | None) -> bool:
        if not pdata:
            return False
        cats = pdata.get("categories") or []
        return bool(isinstance(cats, list) and len(cats) > 0)

    book_data = _resolve_book_dict(request)
    if not book_data:
        raise HTTPException(
            status_code=404,
            detail=f"Book not found for uid/work_id: {request.uid or request.work_id}",
        )
    book = Book.model_validate(book_data)
    stmt_uid = str((book.uid or request.uid or book_data.get("uid") or request.work_id or "")).strip()
    if not stmt_uid:
        stmt_uid = str(book_data.get("id") or "")

    calcs = _calc_with_db_history(request, book, book_data)
    author_data = calcs.get("author") or {}
    illustrator_data = calcs.get("illustrator") or {}

    saved_parties: list[str] = []

    def write_party_json(party: str, party_data: Dict):
        party_file = ROYALTY_DATA_DIR / f"{party}_royalty.json"
        statement_data = {
            "uid": stmt_uid,
            "book_title": book.title,
            "book_author": book.author,
            "party": party,
            "period_start": request.period_start,
            "period_end": request.period_end,
            "generated_at": datetime.now().isoformat(),
            "sales_data": [
                sd.dict() if hasattr(sd, "dict") else dict(sd)
                for sd in request.sales_data
            ],
            "calculations": party_data,
        }
        try:
            existing: list = []
            if party_file.exists():
                maybe = json.loads(party_file.read_text(encoding="utf-8"))
                existing = maybe if isinstance(maybe, list) else []
            filtered = []
            for e in existing:
                e_uid = e.get("uid") or e.get("book_uid")
                if not (
                    e_uid == statement_data["uid"]
                    and e.get("period_start") == statement_data["period_start"]
                    and e.get("period_end") == statement_data["period_end"]
                ):
                    filtered.append(e)
            filtered.append(statement_data)
            ROYALTY_DATA_DIR.mkdir(parents=True, exist_ok=True)
            party_file.write_text(json.dumps(filtered, indent=2), encoding="utf-8")
        except Exception as e:
            print(f"[statements] Error saving to {party_file}: {e}")

    book_upload_dir = UPLOADS_DIR / stmt_uid
    book_upload_dir.mkdir(parents=True, exist_ok=True)

    if has_party_rows(author_data):
        try:
            write_party_json("author", author_data)
            author_pdf = generate_statement_pdf(book, request, author_data, "author")
            author_filename = f"royalty_statement_author_{request.period_start}_{request.period_end}.pdf"
            (book_upload_dir / author_filename).write_bytes(author_pdf)
            print(f"[save] Saved author PDF to {(book_upload_dir / author_filename)}")
            saved_parties.append("author")
        except Exception as e:
            print(f"[save] Error generating/saving author statement: {e}")
    else:
        print("[save] Skipping author statement (no author rows).")

    if has_party_rows(illustrator_data) and has_positive_royalty_percent(illustrator_data):
        try:
            write_party_json("illustrator", illustrator_data)
            illustrator_pdf = generate_statement_pdf(book, request, illustrator_data, "illustrator")
            illustrator_filename = f"royalty_statement_illustrator_{request.period_start}_{request.period_end}.pdf"
            (book_upload_dir / illustrator_filename).write_bytes(illustrator_pdf)
            print(f"[save] Saved illustrator PDF to {(book_upload_dir / illustrator_filename)}")
            saved_parties.append("illustrator")
        except Exception as e:
            print(f"[save] Error generating/saving illustrator statement: {e}")
    else:
        print("[save] Skipping illustrator statement (no royalty % > 0).")

    try:
        rebuild_author_index_from_log(ROYALTY_DATA_DIR)
    except Exception as reidx_err:
        print(f"[save] WARNING: could not rebuild author index: {reidx_err}")

    _persist_statement_calculations_to_db(request, book_data, calcs)

    if not saved_parties:
        return {"message": "No statements saved (author/illustrator conditions not met).", "saved": []}
    return {"message": "Saved", "saved": saved_parties}


@router.get("/statements/{person_type}/{person_name}")
def get_person_statements(person_type: str, person_name: str):
    if person_type not in ("author", "illustrator"):
        raise HTTPException(status_code=400, detail="person_type must be 'author' or 'illustrator'")
    return {"statements": calculator.get_person_statements(person_name, person_type)}


@router.delete("/statements/{person_type}/{person_name}")
def delete_statement(person_type: str, person_name: str, period_start: str, period_end: str):
    if person_type not in ("author", "illustrator"):
        raise HTTPException(status_code=400, detail="person_type must be 'author' or 'illustrator'")
    ok = calculator.delete_statement(person_type, person_name, period_start, period_end)
    if not ok:
        raise HTTPException(status_code=404, detail="Statement not found")
    return {"message": "Statement deleted"}


@router.get("/categories")
def get_categories():
    categories = [
        "Hardcover", "Paperback", "Board Book", "E-book", "Export", "Foreign Rights",
        "Canada-HC", "Canada-PB", "UK", "Large-type reprint",
        "Selections/Condensations", "Book club", "First serial",
        "Second serial", "Physical Audiobook",
    ]
    return {"categories": categories}


@router.get("/format-types")
def get_format_types():
    return {"formats": ["Hardcover", "Paperback", "Board Book", "E-book", "Audiobook", "Other"]}


@router.post("/render")
def render_royalty_statement(
    request: RoyaltyStatementRequest,
    format: str = Query("html", pattern="^(html|pdf)$"),
    party: str = Query("author", pattern="^(author|illustrator)$"),
):
    """
    Render a royalty statement as HTML or PDF for a specific party.
    Also saves statement data to JSON files for history.

    Rule:
      - If party == 'illustrator', only allow render/save when at least one
        row has a royalty % strictly greater than 0. If not, return a 200
        HTML placeholder (for html) or 204 (for pdf) instead of 400.
    """
    book_data = _resolve_book_dict(request)
    if not book_data:
        raise HTTPException(
            status_code=404,
            detail=f"Book not found for uid/work_id: {request.uid or request.work_id}",
        )
    book = Book.model_validate(book_data)
    stmt_uid = str((book.uid or request.uid or book_data.get("uid") or request.work_id or "")).strip()
    if not stmt_uid:
        stmt_uid = str(book_data.get("id") or "")

    # Calculate once
    calcs = _calc_with_db_history(request, book, book_data)
    party_data = calcs.get(party, {}) or {}
    has_rows = bool(party_data.get("categories"))

    # --- Illustrator gating: soft-return (no error) when not applicable
    not_applicable = (
        party == "illustrator"
        and (not has_rows or not has_positive_royalty_percent(party_data))
    )
    if not_applicable:
        if format == "html":
            placeholder = f"""
            <!DOCTYPE html>
            <html><head><meta charset="utf-8"><title>No Illustrator Statement</title>
            <style>body{{font-family:Segoe UI,Arial,sans-serif;padding:24px;color:#333}}
            .card{{border:1px solid #e5e7eb;border-radius:12px;padding:16px;background:#fafafa;max-width:720px}}
            h1{{font-size:18px;margin:0 0 8px 0}} p{{margin:0}}
            </style></head><body>
              <div class="card">
                <h1>No illustrator statement for this period</h1>
                <p>There are no illustrator royalties to report (royalty rate is 0%).</p>
              </div>
            </body></html>
            """
            return HTMLResponse(content=placeholder, headers={"X-Statement-Available": "false"})
        return Response(status_code=204, headers={"X-Statement-Available": "false"})

    if not has_rows:
        raise HTTPException(status_code=400, detail=f"No {party} data in calculations")

    statement_data = {
        "uid": stmt_uid,
        "book_title": book.title,
        "book_author": book.author,
        "party": party,
        "period_start": request.period_start,
        "period_end": request.period_end,
        "generated_at": datetime.now().isoformat(),
        "sales_data": [sd.dict() if hasattr(sd, "dict") else dict(sd) for sd in request.sales_data],
        "calculations": party_data,
    }

    party_file = ROYALTY_DATA_DIR / f"{party}_royalty.json"
    try:
        existing: list = []
        if party_file.exists():
            maybe = json.loads(party_file.read_text(encoding="utf-8"))
            existing = maybe if isinstance(maybe, list) else []

        book_uid_key = statement_data["uid"]
        ps_key = statement_data["period_start"]
        pe_key = statement_data["period_end"]

        filtered = []
        for e in existing:
            e_uid = e.get("uid") or e.get("book_uid")
            if not (e_uid == book_uid_key and e.get("period_start") == ps_key and e.get("period_end") == pe_key):
                filtered.append(e)

        filtered.append(statement_data)
        ROYALTY_DATA_DIR.mkdir(parents=True, exist_ok=True)
        party_file.write_text(json.dumps(filtered, indent=2), encoding="utf-8")

        try:
            rebuild_author_index_from_log(ROYALTY_DATA_DIR)
        except Exception as reidx_err:
            print(f"[render] WARNING: could not rebuild author index: {reidx_err}")

    except Exception as e:
        print(f"[render] Error saving to {party_file}: {e}")

    html = generate_statement_html(book, request, party_data, party)
    if format == "html":
        return HTMLResponse(content=html, headers={"X-Statement-Available": "true"})

    pdf_bytes = generate_statement_pdf(book, request, party_data, party)
    pdf_filename = f"royalty_statement_{party}_{request.period_start}_{request.period_end}.pdf"
    return Response(
        content=pdf_bytes,
        media_type="application/pdf",
        headers={
            "Content-Disposition": f"attachment; filename={pdf_filename}",
            "X-Statement-Available": "true",
        },
    )


# --- royalty % > 0 detector for a party's data -------------------------------
def _to_decimal(val) -> Decimal:
    if val is None:
        return Decimal(0)
    s = str(val).strip().replace("%", "").replace(",", "")
    if s == "":
        return Decimal(0)
    try:
        return Decimal(s)
    except InvalidOperation:
        return Decimal(0)


def has_positive_royalty_percent(party_data: dict | None) -> bool:
    """
    Returns True iff ANY row in party_data['categories'] contains a royalty %
    strictly greater than 0. Handles several possible field names.
    """
    if not party_data:
        return False
    rows = party_data.get("categories") or []
    if not isinstance(rows, list) or not rows:
        return False

    rate_keys = (
        "royalty_rate_percent",
        "Royalty Rate (%)",
        "royalty_rate",
        "rate",
        "royalty_percent",
    )
    for row in rows:
        if not isinstance(row, dict):
            continue
        for k in rate_keys:
            if k in row and _to_decimal(row.get(k)) > 0:
                return True
    return False


def rebuild_author_index_from_log(ROYALTY_DATA_DIR: Path):
    """
    Rebuild author_royalties.json (plural) from author_royalty.json (singular).
    De-dupes by (author/person, book_uid, period_start, period_end), keeping the newest generated_at.
    """
    from collections import defaultdict

    def parse_dt(s: str | None):
        if not s:
            return None
        try:
            return datetime.fromisoformat(s.replace("Z", "+00:00"))
        except Exception:
            return None

    log_file = ROYALTY_DATA_DIR / "author_royalty.json"
    idx_file = ROYALTY_DATA_DIR / "author_royalties.json"

    if not log_file.exists():
        idx_file.write_text(json.dumps({}, indent=2), encoding="utf-8")
        return

    try:
        data = json.loads(log_file.read_text(encoding="utf-8"))
        if not isinstance(data, list):
            for k in ("entries", "log", "data"):
                if isinstance(data.get(k), list):
                    data = data[k]
                    break
            else:
                data = []
    except Exception:
        data = []

    latest: dict[tuple, tuple] = {}
    for e in data:
        author = (
            e.get("person_name")
            or e.get("party_name")
            or e.get("party")
            or e.get("book_author")
            or e.get("author")
            or "author"
        )
        book_uid = e.get("book_uid") or e.get("book_id") or e.get("uid")
        ps = e.get("period_start") or e.get("periodStart")
        pe = e.get("period_end") or e.get("periodEnd")
        ga = e.get("generated_at") or e.get("generatedAt") or e.get("created_at") or e.get("timestamp")
        dt = parse_dt(ga)
        key = (author, book_uid, ps, pe)

        prev = latest.get(key)
        if prev is None:
            latest[key] = (dt, e)
        else:
            pdt, _ = prev
            if (dt and (not pdt or dt >= pdt)) or (pdt is None and dt is None):
                latest[key] = (dt, e)

    buckets: dict[str, list] = defaultdict(list)
    for (author, _book_uid, _ps, _pe), (_dt, entry) in latest.items():
        buckets[author].append(entry)

    def sort_key(e):
        return (
            e.get("period_end") or e.get("periodEnd") or "",
            e.get("period_start") or e.get("periodStart") or "",
            e.get("generated_at") or e.get("generatedAt") or "",
        )

    for author in buckets:
        buckets[author].sort(key=sort_key)

    idx_file.write_text(json.dumps(buckets, indent=2, ensure_ascii=False), encoding="utf-8")


# =============================
#     PDF GENERATION CORE
# =============================

def generate_statement_pdf(
    book: Book,
    request: RoyaltyStatementRequest,
    party_data: Dict,
    party: str,
    *,
    force_reportlab: bool = False,
) -> bytes:
    """
    Prefer WeasyPrint (when enabled), otherwise Playwright → optional Ghostscript normalize.
    Falls back to ReportLab only if HTML renderers fail.
    """
    if FORCE_REPORTLAB_PDFS or force_reportlab:
        return generate_statement_pdf_reportlab(book, request, party_data, party)

    html_content = generate_statement_html(book, request, party_data, party, target="pdf")

    # 1) WeasyPrint
    if FORCE_WEASYPRINT:
        try:
            return generate_statement_pdf_weasy(book, request, party_data, party)
        except Exception as e:
            print(f"[pdf] WeasyPrint failed: {e}")

    # 2) Playwright (Chromium)
    pdf_bytes = None
    try:
        from playwright.sync_api import sync_playwright
        with sync_playwright() as p:
            browser = p.chromium.launch()
            page = browser.new_page()
            page.emulate_media(media="print")
            page.set_content(html_content, wait_until="load")
            try:
                page.wait_for_function("document.fonts && document.fonts.status === 'loaded'", timeout=10000)
            except Exception:
                pass
            try:
                page.evaluate("document.documentElement.setAttribute('data-target','pdf')")
            except Exception:
                pass
            pdf_bytes = page.pdf(
                format="Letter",
                prefer_css_page_size=True,
                print_background=True,
                display_header_footer=False,
                scale=1.0,
            )
            browser.close()
            print(f"[pdf] Playwright PDF generated successfully, size: {len(pdf_bytes)} bytes")
    except Exception as e:
        print(f"[pdf] Playwright failed: {e}")

    if not pdf_bytes:
        return generate_statement_pdf_reportlab(book, request, party_data, party)

    # 3) Ghostscript normalize (optional)
    normalized_bytes = None
    try:
        gs = find_gs_exe()
        if gs:
            with tempfile.TemporaryDirectory() as td:
                in_pdf = os.path.join(td, "in.pdf")
                out_pdf = os.path.join(td, "out.pdf")
                Path(in_pdf).write_bytes(pdf_bytes)

                args = [
                    gs, "-dBATCH", "-dNOPAUSE", "-dSAFER",
                    "-sDEVICE=pdfwrite",
                    "-dCompatibilityLevel=1.4",
                    "-dPDFSETTINGS=/printer",
                    "-dDetectDuplicateImages=true",
                    "-dCompressFonts=true",
                    "-dSubsetFonts=true",
                    "-dEmbedAllFonts=true",
                    "-dAutoRotatePages=/None",
                    "-dUCRandBGInfo=/Remove",
                    f"-sOutputFile={out_pdf}",
                    in_pdf,
                ]
                subprocess.run(args, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=90)
                normalized_bytes = Path(out_pdf).read_bytes()
                print(f"[pdf] GS normalized PDF size: {len(normalized_bytes)}")
        else:
            print("[pdf] Ghostscript not found; skipping normalization.")
    except Exception as e:
        print(f"[pdf] GS normalization failed: {e}")
        normalized_bytes = None

    best_vector = normalized_bytes or pdf_bytes

    acrobat_safe = None
    try:
        acrobat_safe = distill_via_postscript(best_vector)
        if acrobat_safe:
            print(f"[pdf] Distilled via PostScript (Acrobat-safe), size: {len(acrobat_safe)}")
    except Exception as e:
        print(f"[pdf] distill_via_postscript failed: {e}")
        acrobat_safe = None

    base_for_output = acrobat_safe or best_vector

    if FORCE_RASTERIZED_PDF:
        print("[pdf] FORCE_RASTERIZED_PDF=True → raster fallback")
        ras = None
        try:
            ras = rasterize_pdf_pymupdf(base_for_output, dpi=RASTER_DPI)
        except Exception as e:
            print(f"[pdf] PyMuPDF raster failed: {e}")
        if not ras:
            ras = rasterize_pdf_to_images_and_wrap(base_for_output, dpi=RASTER_DPI)
        if ras:
            return ras
        return base_for_output

    return base_for_output


def generate_statement_pdf_reportlab(book: Book, request: RoyaltyStatementRequest, party_data: Dict, party: str) -> bytes:
    """Fallback PDF generation using ReportLab"""
    from reportlab.lib.pagesizes import letter
    from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle
    from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
    from reportlab.lib.units import inch
    from reportlab.lib import colors
    from reportlab.lib.enums import TA_CENTER
    from io import BytesIO

    def to_money(v: Any) -> Decimal:
        if isinstance(v, (int, float, Decimal)):
            return Decimal(str(v))
        if isinstance(v, str):
            s = v.replace(",", "").replace("$", "").strip()
            try:
                return Decimal(s)
            except InvalidOperation:
                return Decimal(0)
        return Decimal(0)

    categories = party_data.get("categories", []) or []
    party_name = (
        book.author if party == "author" else
        (book.illustrator.name if getattr(book, "illustrator", None) and hasattr(book.illustrator, "name")
         else str(getattr(book, "illustrator", None) or "Illustrator"))
    )

    full_title = (book.title or "")
    if getattr(book, "subtitle", None):
        full_title += f": {book.subtitle}"

    isbn_html = "N/A"
    if getattr(book, "formats", None):
        items = []
        for fmt in book.formats:
            fmt_dict = fmt.model_dump() if hasattr(fmt, "model_dump") else (fmt if isinstance(fmt, dict) else {})
            isbn = fmt_dict.get("isbn") or fmt_dict.get("ISBN")
            format_name = fmt_dict.get("format") or fmt_dict.get("Format") or "Unknown"
            if isbn and str(isbn).strip():
                items.append(f"{format_name}: {str(isbn).strip()}")
        if items:
            isbn_html = items[0]
            if len(items) > 1:
                isbn_html += "".join(f"<br/>{x}" for x in items[1:])

    advance_val = to_money(party_data.get("advance", 0))
    royalty_val = to_money(party_data.get("royalty_total", 0))
    last_bal = to_money(party_data.get("last_balance", 0))
    balance_val = to_money(party_data.get("balance", 0))
    payable_val = to_money(party_data.get("payable", 0))

    buffer = BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=letter, topMargin=0.75 * inch, bottomMargin=0.75 * inch)

    styles = getSampleStyleSheet()
    title_style = ParagraphStyle("CustomTitle", parent=styles["Heading1"], fontSize=18, spaceAfter=20, alignment=TA_CENTER)
    header_style = ParagraphStyle("Header", parent=styles["Normal"], fontSize=12, spaceAfter=6)

    story = []
    story.append(Paragraph("ROYALTY STATEMENT", title_style))
    story.append(Paragraph(f"Period: {request.period_start} to {request.period_end}", header_style))
    story.append(Spacer(1, 20))
    story.append(Paragraph(f"<b>Book Title:</b> {full_title}", styles["Normal"]))
    story.append(Spacer(1, 6))
    story.append(Paragraph(f"<b>ISBN(s):</b> {isbn_html}", styles["Normal"]))
    story.append(Spacer(1, 6))
    story.append(Paragraph(f"<b>Statement For:</b> {party_name}", styles["Normal"]))
    story.append(Spacer(1, 6))
    story.append(Paragraph(f"<b>Statement Date:</b> {datetime.now().strftime('%B %d, %Y')}", styles["Normal"]))
    story.append(Spacer(1, 20))

    story.append(Paragraph("<b>Sales Detail</b>", styles["Heading2"]))
    story.append(Spacer(1, 12))

    table_data = [[
        "Category", "Lifetime Qty", "RTD", "Units", "Returns", "Net Units",
        "Price", "Rate %", "Disc", "Net", "Value", "Royalty"
    ]]
    for cat in categories:
        row = [
            cat.get("category", cat.get("Category", "")),
            cat.get("lifetime_quantity", cat.get("Lifetime Quantity", "")),
            cat.get("returns_to_date", cat.get("Returns to Date", "")),
            cat.get("units", cat.get("Units", "")),
            cat.get("returns", cat.get("Returns", "")),
            cat.get("net_units", cat.get("Net Units", "")),
            cat.get("unit_price", cat.get("Unit Price", "")),
            cat.get("royalty_rate_percent", cat.get("Royalty Rate (%)", "")),
            cat.get("discount", cat.get("Discount", "")),
            cat.get("net_revenue", cat.get("Net Revenue", "")),
            cat.get("value", cat.get("Value", "")),
            cat.get("royalty", cat.get("Royalty", "")),
        ]
        table_data.append(row)

    table = Table(table_data, colWidths=[0.8*inch, 0.6*inch, 0.6*inch, 0.5*inch, 0.5*inch, 0.6*inch,
                                        0.6*inch, 0.5*inch, 0.45*inch, 0.6*inch, 0.6*inch, 0.6*inch])
    table.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), colors.darkblue),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.whitesmoke),
        ("ALIGN", (0, 0), (-1, -1), "CENTER"),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("FONTSIZE", (0, 0), (-1, 0), 8),
        ("BOTTOMPADDING", (0, 0), (-1, 0), 12),
        ("BACKGROUND", (0, 1), (-1, -1), colors.beige),
        ("FONTSIZE", (0, 1), (-1, -1), 7),
        ("GRID", (0, 0), (-1, -1), 1, colors.black),
    ]))
    story.append(table)
    story.append(Spacer(1, 20))

    story.append(Paragraph("<b>Financial Summary</b>", styles["Heading2"]))
    story.append(Spacer(1, 12))
    summary_data = [
        ["Advance Paid:", f"${advance_val:,.2f}"],
        ["Royalty for Period:", f"${royalty_val:,.2f}"],
        ["Last Period Balance:", f"${last_bal:,.2f}"],
        ["Current Balance:", f"${balance_val:,.2f}"],
        ["Amount Payable:", f"${payable_val:,.2f}"],
    ]
    summary_table = Table(summary_data, colWidths=[2*inch, 1.4*inch])
    summary_table.setStyle(TableStyle([
        ("ALIGN", (0, 0), (0, -1), "LEFT"),
        ("ALIGN", (1, 0), (1, -1), "RIGHT"),
        ("FONTNAME", (0, 0), (-1, -1), "Helvetica"),
        ("FONTSIZE", (0, 0), (-1, -1), 10),
        ("FONTNAME", (0, 4), (1, 4), "Helvetica-Bold"),
        ("FONTSIZE", (0, 4), (1, 4), 12),
        ("TEXTCOLOR", (1, 4), (1, 4), colors.green),
        ("LINEABOVE", (0, 4), (-1, 4), 2, colors.black),
        ("TOPPADDING", (0, 4), (-1, 4), 12),
    ]))
    story.append(summary_table)
    story.append(Spacer(1, 20))

    footer_style = ParagraphStyle("Footer", parent=styles["Normal"], fontSize=8, textColor=colors.grey, alignment=TA_CENTER)
    story.append(Paragraph("This statement is generated for informational purposes.", footer_style))
    story.append(Paragraph(f"Generated: {datetime.now().strftime('%B %d, %Y at %I:%M %p')}", footer_style))

    doc.build(story)
    pdf_bytes = buffer.getvalue()
    buffer.close()
    return pdf_bytes


def rasterize_pdf_to_images_and_wrap(pdf_bytes: bytes, dpi: int = 300) -> bytes | None:
    try:
        gs = find_gs_exe()
        if not gs:
            print("[pdf] rasterize: Ghostscript not found")
            return None

        from io import BytesIO
        from reportlab.pdfgen import canvas
        from reportlab.lib.pagesizes import letter
        from reportlab.lib.utils import ImageReader

        with tempfile.TemporaryDirectory() as td:
            in_pdf = os.path.join(td, "in.pdf")
            Path(in_pdf).write_bytes(pdf_bytes)

            png_pattern = os.path.join(td, "page-%04d.png")
            args = [
                gs, "-dBATCH", "-dNOPAUSE", "-dSAFER",
                "-sDEVICE=png16m",
                f"-r{dpi}",
                "-dUseCropBox",
                f"-sOutputFile={png_pattern}",
                in_pdf,
            ]
            subprocess.run(args, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

            png_files = sorted(Path(td).glob("page-*.png"))
            if not png_files:
                print("[pdf] rasterize: no PNGs were produced")
                return None

            buf = BytesIO()
            page_width, page_height = letter
            c = canvas.Canvas(buf, pagesize=letter)

            for p in png_files:
                img = ImageReader(p.open("rb"))
                iw, ih = img.getSize()

                img_width_pts = iw / dpi * 72.0
                img_height_pts = ih / dpi * 72.0

                scale = min(page_width / img_width_pts, page_height / img_height_pts)
                draw_w = img_width_pts * scale
                draw_h = img_height_pts * scale
                x = (page_width - draw_w) / 2.0
                y = (page_height - draw_h) / 2.0

                c.drawImage(img, x, y, width=draw_w, height=draw_h)
                c.showPage()

            c.save()
            out = buf.getvalue()
            buf.close()
            return out if out else None

    except Exception as e:
        print(f"[pdf] rasterize: failed with {e}")
        return None


def distill_via_postscript(pdf_bytes: bytes) -> bytes | None:
    try:
        gs = find_gs_exe()
        if not gs:
            print("[pdf] distill: Ghostscript not found")
            return None

        with tempfile.TemporaryDirectory() as td:
            in_pdf = os.path.join(td, "in.pdf")
            mid_ps = os.path.join(td, "mid.ps")
            out_pdf = os.path.join(td, "out.pdf")
            Path(in_pdf).write_bytes(pdf_bytes)

            args_ps = [
                gs, "-dBATCH", "-dNOPAUSE", "-dSAFER",
                "-sDEVICE=ps2write",
                "-dLanguageLevel=3",
                "-dColorConversionStrategy=/sRGB",
                "-dProcessColorModel=/DeviceRGB",
                "-sColorConversionStrategy=RGB",
                "-dOverprint=0",
                "-dAutoRotatePages=/None",
                f"-sOutputFile={mid_ps}",
                in_pdf,
            ]
            subprocess.run(args_ps, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=90)

            args_pdf = [
                gs, "-dBATCH", "-dNOPAUSE", "-dSAFER",
                "-sDEVICE=pdfwrite",
                "-dCompatibilityLevel=1.3",
                "-dColorConversionStrategy=/sRGB",
                "-dProcessColorModel=/DeviceRGB",
                "-sColorConversionStrategy=RGB",
                "-dDetectDuplicateImages=true",
                "-dCompressFonts=true",
                "-dSubsetFonts=true",
                "-dOverprint=0",
                "-dAutoRotatePages=/None",
                f"-sOutputFile={out_pdf}",
                mid_ps,
            ]
            subprocess.run(args_pdf, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=90)

            data = Path(out_pdf).read_bytes()
            return data if data else None

    except Exception as e:
        print(f"[pdf] distill: failed with {e}")
        return None


def rasterize_pdf_pymupdf(pdf_bytes: bytes, dpi: int = 300) -> bytes | None:
    try:
        import fitz  # PyMuPDF
        from io import BytesIO
        from reportlab.pdfgen import canvas
        from reportlab.lib.pagesizes import letter
        from reportlab.lib.utils import ImageReader

        doc = fitz.open(stream=pdf_bytes, filetype="pdf")
        if len(doc) == 0:
            return None

        png_bytes_list = []
        zoom = dpi / 72.0
        mat = fitz.Matrix(zoom, zoom)
        for page in doc:
            pix = page.get_pixmap(matrix=mat, alpha=False)
            png_bytes_list.append(pix.tobytes("png"))
        doc.close()

        buf = BytesIO()
        page_w, page_h = letter
        c = canvas.Canvas(buf, pagesize=letter)
        for png in png_bytes_list:
            img = ImageReader(BytesIO(png))
            iw, ih = img.getSize()
            w_pt = iw / dpi * 72.0
            h_pt = ih / dpi * 72.0
            scale = min(page_w / w_pt, page_h / h_pt)
            draw_w, draw_h = w_pt * scale, h_pt * scale
            x, y = (page_w - draw_w) / 2.0, (page_h - draw_h) / 2.0
            c.drawImage(img, x, y, width=draw_w, height=draw_h)
            c.showPage()
        c.save()
        out = buf.getvalue()
        buf.close()
        return out if out else None
    except Exception as e:
        print(f"[pdf] rasterize_pdf_pymupdf failed: {e}")
        return None


def generate_statement_pdf_weasy(book: Book, request: RoyaltyStatementRequest, party_data: Dict, party: str) -> bytes:
    from weasyprint import HTML, CSS

    html_content = generate_statement_html(book, request, party_data, party, target="pdf")

    css = CSS(string="""
        @page { size: Letter; margin: 0.7in; }
        html, body { background: #ffffff !important; -webkit-print-color-adjust: exact; print-color-adjust: exact; }
        * { background-clip: border-box; }
    """)

    base_url = str(Path.cwd())
    pdf = HTML(string=html_content, base_url=base_url).write_pdf(stylesheets=[css])
    return pdf


# =============================
#          HTML VIEW
# =============================

def generate_statement_html(book: Book, request: RoyaltyStatementRequest, party_data: Dict, party: str, *, target: str = "screen") -> str:
    def to_money(v: Any) -> Decimal:
        if isinstance(v, (int, float, Decimal)):
            return Decimal(str(v))
        if isinstance(v, str):
            s = v.replace(",", "").replace("$", "").strip()
            try:
                return Decimal(s)
            except InvalidOperation:
                return Decimal(0)
        return Decimal(0)

    def money_fmt(x: Decimal) -> str:
        return f"${x:,.2f}"

    logo_base64 = ""
    if LOGO_PATH.exists():
        try:
            logo_bytes = LOGO_PATH.read_bytes()
            logo_base64 = base64.b64encode(logo_bytes).decode("utf-8")
        except Exception as e:
            print(f"[render] Could not load logo: {e}")

    categories = party_data.get("categories", []) or []

    party_name = book.author if party == "author" else (
        book.illustrator.name if getattr(book, "illustrator", None) and hasattr(book.illustrator, "name")
        else str(getattr(book, "illustrator", None) or "Illustrator")
    )

    agency_html = ""
    if party == "author" and getattr(book, "author_agent", None):
        agent = book.author_agent
        agency_name = getattr(agent, "agency", "") or getattr(agent, "name", "")
        if agency_name:
            agency_html = f"<div style='margin-bottom:5px;font-weight:600;'>{agency_name}</div>"
        addr = getattr(agent, "address", None)
        if isinstance(addr, dict):
            street = addr.get("street", "") or ""
            city = addr.get("city", "") or ""
            state = addr.get("state", "") or ""
            zip_code = addr.get("zip", "") or ""
            if street:
                agency_html += f"<div>{street}</div>"
            if city or state or zip_code:
                line = " ".join(p for p in [city, state] if p)
                agency_html += f"<div>{line}{'&nbsp;&nbsp;' + zip_code if zip_code else ''}</div>"
        elif isinstance(addr, str) and addr:
            agency_html += f"<div>{addr}</div>"

    elif party == "illustrator" and getattr(book, "illustrator", None) and getattr(book.illustrator, "agent", None):
        agent = book.illustrator.agent
        agency_name = getattr(agent, "agency", "") or getattr(agent, "name", "")
        if agency_name:
            agency_html = f"<div style='margin-bottom:5px;font-weight:600;'>{agency_name}</div>"
        addr = getattr(agent, "address", None)
        if isinstance(addr, dict):
            street = addr.get("street", "")
            city = addr.get("city", "")
            state = addr.get("state", "")
            zip_code = addr.get("zip", "")
            if street:
                agency_html += f"<div>{street}</div>"
            if city or state or zip_code:
                agency_html += f"<div>{' '.join(filter(None, [city, state, zip_code]))}</div>"
        elif isinstance(addr, str) and addr:
            agency_html += f"<div>{addr}</div>"

    isbn_html = "N/A"
    if getattr(book, "formats", None):
        items = []
        for fmt in book.formats:
            fmt_dict = fmt.model_dump() if hasattr(fmt, "model_dump") else (fmt if isinstance(fmt, dict) else {})
            isbn = fmt_dict.get("isbn") or fmt_dict.get("ISBN")
            format_name = fmt_dict.get("format") or fmt_dict.get("Format") or "Unknown"
            if isbn and str(isbn).strip():
                items.append(f"{format_name}: {str(isbn).strip()}")
        if items:
            isbn_html = "".join(f"<div class='isbn-line'>{x}</div>" for x in items)

    full_title = (book.title or "")
    if getattr(book, "subtitle", None):
        full_title += f": {book.subtitle}"

    advance_val = to_money(party_data.get("advance", 0))
    royalty_val = to_money(party_data.get("royalty_total", 0))
    last_bal = to_money(party_data.get("last_balance", 0))
    balance_val = to_money(party_data.get("balance", 0))
    payable_val = to_money(party_data.get("payable", 0))

    agency_box_html = agency_html + (
        f"<div class='info-row' style='margin-top:10px'>"
        f"<span class='label'>Statement For: </span>"
        f"<span style='font-weight:600;'>{party_name}</span>"
        f"</div>"
    )

    html = f"""<!DOCTYPE html>
<html data-target="{ 'pdf' if target == 'pdf' else 'screen' }">
<head>
<meta charset="UTF-8">
<title>Royalty Statement - {party_name}</title>
<style>
    :root {{
    --font-base: 17px;
    --line: 1.5;
    --pad: 10px;
    --density: 1;
    }}
    html[data-target="pdf"] {{
    --font-base: 11pt;
    --line: 1.35;
    --pad: 6px;
    --density: .92;
    }}

    @page {{ size: Letter; margin: 0.7in; background: #ffffff; }}
    html, body {{ background: #ffffff !important; color: #000; }}
    body::before {{
        content: "";
        position: fixed; inset: 0;
        background: #ffffff;
        z-index: -1;
    }}

    body {{
        font-family: 'Segoe UI', Arial, sans-serif;
        font-size: var(--font-base);
        line-height: var(--line);
        color: #333;
        margin: 0;
    }}

    .header {{
        text-align: center;
        margin: 0 0 20px 0;
        padding: 10px 0 15px 0;
        border-bottom: 2px solid #333;
    }}
    .logo {{ max-width: 300px; height: auto; margin-bottom: 10px; }}
    .title {{ font-size: clamp(18px, 2.2vw, 22px); font-weight: bold; margin: 10px 0; }}

    .info-section {{
        display: grid;
        grid-template-columns: 1fr 260px;
        gap: 0px;
        align-items: start;
        margin: 20px 0;
    }}
    .agency-box {{ line-height: 1.5; }}
    .book-info {{ line-height: 1.6; margin-left: auto; padding-left: 0; }}

    .info-row {{
        display: flex;
        align-items: flex-start;
        gap: 3px;
        margin: 3px 0;
    }}
    .info-row .label {{
        flex: 0 0 80px;
        font-weight: 600;
        white-space: nowrap;
    }}
    .info-section .value {{
        flex: 1 1 auto;
        min-width: 0;
        white-space: normal;
        line-height: 1.45;
    }}
    .info-section .value br {{ line-height: 1.4; }}

    h3 {{ margin: 18px 0 8px; }}

    table {{
        width: 100%;
        border-collapse: collapse;
        margin: 20px 0;
        font-size: calc(0.85 * var(--font-base));
        font-variant-numeric: tabular-nums;
    }}
    thead th {{
        background: #2c3e50; color: #fff; padding: 8px 4px; text-align: left; font-weight: 600;
    }}
    td {{ padding: calc(6px * var(--density)) 4px; border-bottom: 1px solid #ddd; }}
    tbody tr:nth-child(even) {{ background: #f9f9f9; }}
    .text-right {{ text-align: right; }}
    .text-center {{ text-align: center; }}

    .summary {{
        width: 240px;
        margin-left: auto;
        max-width: 60vw;
        padding: 10px 8px 10px 12px;
        background: #e8f4f8;
        border-left: 4px solid #2c3e50;
        border-radius: 6px;
        margin-bottom: 24px;
    }}
    .summary h3 {{ margin: 0 0 6px 0; font-size: calc(0.95 * var(--font-base)); text-align: left; }}
    .summary-row {{ display: grid; grid-template-columns: minmax(0, 62%) 1fr; column-gap: 12px; align-items: baseline; font-size: calc(0.95 * var(--font-base)); line-height: 1.25; }}
    .summary-total {{ font-size: calc(1.1 * var(--font-base)); font-weight: 700; margin-top: 6px; padding-top: 6px; border-top: 1px solid #333; }}
    .summary-row .label {{ white-space: nowrap; }}
    .summary-row .value {{ white-space: nowrap; text-align: right; }}

    .page-content {{
        padding-bottom: 0.7in;
        display: flow-root;
    }}

    .footer {{
        clear: both;
        margin-top: 40px;
        padding-top: 6px;
        border-top: 1px solid #ccc;
        text-align: center;
        font-size: calc(0.6 * var(--font-base));
        color: #666;
        background: transparent;
        page-break-inside: avoid;
    }}

    thead {{ display: table-header-group; }}
    tfoot {{ display: table-footer-group; }}
    .avoid-break {{ page-break-inside: avoid; }}
</style>
</head>
<body>
    <div class="header">
        {"<img src='data:image/png;base64," + logo_base64 + "' class='logo' />" if logo_base64 else ""}
        <div class="title">ROYALTY STATEMENT</div>
        <div class="book-title">{full_title}</div>
        <div>Period: {request.period_start} to {request.period_end}</div>
    </div>

    <div class="page-content">
        <div class="info-section">
        <div class="agency-box">{agency_box_html}</div>
        <div class="info-row">
            <span class="label">ISBN(s):</span>
            <span class="value">{isbn_html}</span>
        </div>
        </div>

    <h3>Sales Detail</h3>
    <table>
        <thead>
            <tr>
            <th>Category</th>
            <th class="text-right">Lifetime Qty</th>
            <th class="text-right">RTD</th>
            <th class="text-right">Units</th>
            <th class="text-right">Returns</th>
            <th class="text-right">Net Units</th>
            <th class="text-right">Price</th>
            <th class="text-right">Royalty %</th>
            <th class="text-right">Disc.</th>
            <th class="text-center">Net</th>
            <th class="text-right">Value</th>
            <th class="text-right">Royalty</th>
            </tr>
        </thead>
        <tbody>
"""
    for cat in categories:
        html += f"""
        <tr>
          <td>{cat.get('category', cat.get('Category', ''))}</td>
          <td class="text-right">{cat.get('lifetime_quantity', cat.get('Lifetime Quantity', ''))}</td>
          <td class="text-right">{cat.get('returns_to_date', cat.get('Returns to Date', ''))}</td>
          <td class="text-right">{cat.get('units', cat.get('Units', ''))}</td>
          <td class="text-right">{cat.get('returns', cat.get('Returns', ''))}</td>
          <td class="text-right">{cat.get('net_units', cat.get('Net Units', ''))}</td>
          <td class="text-right">{cat.get('unit_price', cat.get('Unit Price', ''))}</td>
          <td class="text-right">{cat.get('royalty_rate_percent', cat.get('Royalty Rate (%)', ''))}</td>
          <td class="text-right">{cat.get('discount', cat.get('Discount', ''))}</td>
          <td class="text-center">{cat.get('net_revenue', cat.get('Net Revenue', ''))}</td>
          <td class="text-right">{cat.get('value', cat.get('Value', ''))}</td>
          <td class="text-right" style="font-weight:600;">{cat.get('royalty', cat.get('Royalty', ''))}</td>
        </tr>
"""
    html += f"""
      </tbody>
    </table>

    <div class="summary">
      <h3>Financial Summary</h3>
      <div class="summary-row"><span class="label">Advance Paid:</span><span class="value">{money_fmt(advance_val)}</span></div>
      <div class="summary-row"><span class="label">Royalty for Period:</span><span class="value">{money_fmt(royalty_val)}</span></div>
      <div class="summary-row"><span class="label">Last Period Balance:</span><span class="value">{money_fmt(last_bal)}</span></div>
      <div class="summary-row"><span class="label">Current Balance:</span><span class="value">{money_fmt(balance_val)}</span></div>
      <div class="summary-total summary-row"><span class="label">Amount Payable:</span><span class="value" style="color:#16a34a;">{money_fmt(payable_val)}</span></div>
    </div>
    </div>

    <div class="footer">
        <p>This statement is generated for informational purposes.</p>
        <p>Generated: {datetime.now().strftime('%B %d, %Y at %I:%M %p')}</p>
    </div>
</body>
</html>
"""
    return html
