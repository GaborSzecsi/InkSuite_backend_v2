# routers/deal_memo_drafts.py — Deal memos CRUD (PostgreSQL deal_memos table).
# Replaces TempDealMemo.json / S3. List, upsert, delete by tenant_id + uid.
from __future__ import annotations

import json
import secrets
import string
import time
from typing import Any, Dict, List

from fastapi import APIRouter, Body, HTTPException

from app.core.db import db_conn
from psycopg.rows import dict_row

router = APIRouter(prefix="/contracts", tags=["contracts"])

# Tenant: same as catalog fallback (slug from env or first tenant)
DEFAULT_TENANT_SLUG = "marble-press"


def _rand_uid(n: int = 7) -> str:
    alphabet = string.ascii_lowercase + string.digits
    return "".join(secrets.choice(alphabet) for _ in range(n))


def _now() -> float:
    return time.time()


def _tenant_id(cur) -> str:
    import os
    slug = (os.environ.get("DEFAULT_TENANT_SLUG") or os.environ.get("TENANT_SLUG") or DEFAULT_TENANT_SLUG).strip()
    cur.execute("SELECT id FROM tenants WHERE lower(slug) = lower(%s) LIMIT 1", (slug,))
    row = cur.fetchone()
    if not row:
        cur.execute("SELECT id FROM tenants ORDER BY id LIMIT 1")
        row = cur.fetchone()
    if not row:
        raise HTTPException(status_code=503, detail="No tenant configured (deal_memos)")
    return str(row["id"])


def _row_to_draft(row: dict) -> dict:
    """Build frontend draft from DB row: payload_json + uid, name, title, createdAt, updatedAt."""
    payload = row.get("payload_json") or {}
    if isinstance(payload, str):
        try:
            payload = json.loads(payload) if payload else {}
        except Exception:
            payload = {}
    uid = (row.get("uid") or "").strip()
    name = (row.get("name") or "").strip() or (payload.get("title") or "Untitled")
    title = (row.get("title") or "").strip() or name
    created_at = row.get("created_at")
    updated_at = row.get("updated_at")
    created_ms = int(created_at.timestamp() * 1000) if hasattr(created_at, "timestamp") else int(time.time() * 1000)
    updated_ms = int(updated_at.timestamp() * 1000) if hasattr(updated_at, "timestamp") else created_ms
    return {
        **payload,
        "uid": uid,
        "name": name,
        "title": title,
        "createdAt": created_ms,
        "updatedAt": updated_ms,
    }


@router.get("/dealmemos/_where")
def where_file() -> dict:
    return {"storage": "postgres", "table": "deal_memos"}


@router.post("/dealmemos/_touch")
def touch_file() -> dict:
    return {"ok": True, "storage": "postgres", "table": "deal_memos"}


@router.get("/dealmemos")
def list_deal_memos() -> List[dict]:
    with db_conn() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            tenant_id = _tenant_id(cur)
            cur.execute(
                """
                SELECT id, uid, name, title, payload_json, created_at, updated_at
                FROM deal_memos
                WHERE tenant_id = %s
                ORDER BY updated_at DESC
                """,
                (tenant_id,),
            )
            rows = cur.fetchall() or []
            return [_row_to_draft(dict(r)) for r in rows]


@router.get("/dealmemos/{uid}")
def get_deal_memo(uid: str) -> dict:
    uid = (uid or "").strip()
    if not uid:
        raise HTTPException(status_code=400, detail="uid required")
    with db_conn() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            tenant_id = _tenant_id(cur)
            cur.execute(
                """
                SELECT id, uid, name, title, payload_json, created_at, updated_at
                FROM deal_memos
                WHERE tenant_id = %s AND uid = %s
                LIMIT 1
                """,
                (tenant_id, uid),
            )
            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Draft not found")
            return _row_to_draft(dict(row))


@router.post("/dealmemos")
def upsert_deal_memo(body: Dict[str, Any] = Body(...)) -> dict:
    if not isinstance(body, dict):
        raise HTTPException(status_code=400, detail="Invalid JSON body")

    uid = (body.get("uid") or "").strip()
    if not uid:
        uid = _rand_uid()
        body["uid"] = uid

    name = (body.get("name") or "").strip()
    if not name:
        body["name"] = (body.get("title") or "").strip() or "Untitled"

    now_ts = _now()
    body["updatedAt"] = now_ts
    if not isinstance(body.get("createdAt"), (int, float)):
        body["createdAt"] = now_ts

    payload_json = json.dumps(body, ensure_ascii=False)
    title = (body.get("title") or "").strip() or name
    contributor_role = (body.get("contributor_role") or body.get("contributorRole") or "author").strip()
    if contributor_role not in ("author", "illustrator", "other"):
        contributor_role = "author"

    with db_conn() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            tenant_id = _tenant_id(cur)
            cur.execute(
                """
                INSERT INTO deal_memos (tenant_id, uid, name, title, contributor_role, payload_json, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s::jsonb, now())
                ON CONFLICT (tenant_id, uid) DO UPDATE SET
                    name = EXCLUDED.name,
                    title = EXCLUDED.title,
                    contributor_role = EXCLUDED.contributor_role,
                    payload_json = EXCLUDED.payload_json,
                    updated_at = now()
                """,
                (tenant_id, uid, name, title, contributor_role, payload_json),
            )
            cur.execute(
                "SELECT id, uid, name, title, payload_json, created_at, updated_at FROM deal_memos WHERE tenant_id = %s AND uid = %s LIMIT 1",
                (tenant_id, uid),
            )
            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=500, detail="Upsert failed")
            saved = _row_to_draft(dict(row))
            return {"ok": True, "draft": saved}


@router.put("/dealmemos/{uid}")
def update_deal_memo(uid: str, body: Dict[str, Any] = Body(...)) -> dict:
    uid = (uid or "").strip()
    if not uid:
        raise HTTPException(status_code=400, detail="uid required")
    body = dict(body or {})
    body["uid"] = uid
    name = (body.get("name") or "").strip()
    if not name:
        body["name"] = (body.get("title") or "").strip() or "Untitled"
    return upsert_deal_memo(body)


@router.delete("/dealmemos/{uid}")
def delete_draft(uid: str) -> dict:
    uid = (uid or "").strip()
    if not uid:
        raise HTTPException(status_code=400, detail="uid required")
    with db_conn() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            tenant_id = _tenant_id(cur)
            cur.execute("DELETE FROM deal_memos WHERE tenant_id = %s AND uid = %s", (tenant_id, uid))
            if cur.rowcount == 0:
                raise HTTPException(status_code=404, detail="Draft not found")
    return {"ok": True, "deleted": uid}
