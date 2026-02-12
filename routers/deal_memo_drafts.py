# marble_app/routers/deal_memo_drafts.py
from __future__ import annotations
from fastapi import APIRouter, HTTPException, Body
from typing import List, Any, Dict, Tuple
from pathlib import Path
import json, time, secrets, string

router = APIRouter(prefix="/contracts", tags=["contracts"])

HOME = Path.home()
BOOK_DATA_DIR = HOME / "Documents" / "marble_app" / "book_data"
BOOK_DATA_DIR.mkdir(parents=True, exist_ok=True)
TEMP_DRAFTS_PATH = BOOK_DATA_DIR / "TempDealMemo.json"

# ---------- helpers ----------
def _rand_uid(n: int = 7) -> str:
    alphabet = string.ascii_lowercase + string.digits
    return "".join(secrets.choice(alphabet) for _ in range(n))

def _now() -> float:
    return time.time()

def _read_raw() -> List[dict]:
    if not TEMP_DRAFTS_PATH.exists():
        return []
    try:
        with TEMP_DRAFTS_PATH.open("r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, list) else []
    except Exception:
        return []

def _write(items: List[dict]) -> None:
    tmp = TEMP_DRAFTS_PATH.with_suffix(".tmp")
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(items, f, ensure_ascii=False, indent=2)
    tmp.replace(TEMP_DRAFTS_PATH)

def _normalize_items(items: List[dict]) -> Tuple[List[dict], bool]:
    changed = False
    for it in items:
        uid = (it.get("uid") or "").strip()
        if not uid:
            uid = (it.get("id") or "").strip() or _rand_uid()
            it["uid"] = uid
            changed = True
        if not (isinstance(it.get("name"), str) and it["name"].strip()):
            title = (it.get("title") or "").strip()
            it["name"] = title or "Untitled"
            changed = True
        if not isinstance(it.get("createdAt"), (int, float)):
            it["createdAt"] = _now()
            changed = True
        if not isinstance(it.get("updatedAt"), (int, float)):
            it["updatedAt"] = it["createdAt"]
            changed = True
    return items, changed

def _read_drafts() -> List[dict]:
    items = _read_raw()
    items, changed = _normalize_items(items)
    if changed:
        _write(items)
    return items

def _find_index_by_uid(items: List[dict], uid: str) -> int:
    return next((i for i, d in enumerate(items) if (d.get("uid") or "").strip() == uid), -1)

# ---------- routes ----------
@router.get("/dealmemos")
def list_deal_memos() -> List[dict]:
    return _read_drafts()

@router.post("/dealmemos")
def upsert_deal_memo(body: Dict[str, Any] = Body(...)) -> dict:
    if not isinstance(body, dict):
        raise HTTPException(status_code=400, detail="Invalid JSON body")

    items = _read_drafts()
    now = _now()

    uid = (body.get("uid") or "").strip()
    if not uid:
        uid = _rand_uid()
        body["uid"] = uid

    name = (body.get("name") or "").strip()
    if not name:
         body["name"] = (body.get("title") or "").strip() or "Untitled"

    idx = _find_index_by_uid(items, uid)
    if idx >= 0:
        created = items[idx].get("createdAt", now)
        saved = { **items[idx], **body, "createdAt": created, "updatedAt": now }
        items[idx] = saved
    else:
        saved = { **body, "createdAt": now, "updatedAt": now }
        items.insert(0, saved)

    _write(items)
    return {"ok": True, "draft": saved}

@router.put("/dealmemos/{uid}")
def update_deal_memo(uid: str, body: Dict[str, Any] = Body(...)) -> dict:
    items = _read_drafts()
    i = _find_index_by_uid(items, uid.strip())
    if i < 0:
        raise HTTPException(status_code=404, detail="Draft not found")
    now = _now()
    # force the path UID to win
    body = dict(body or {})
    body["uid"] = uid.strip()
    if not (body.get("name") or "").strip():
        body["name"] = (body.get("title") or "").strip() or "Untitled"
    created = items[i].get("createdAt", now)
    saved = { **items[i], **body, "createdAt": created, "updatedAt": now }
    items[i] = saved
    _write(items)
    return {"ok": True, "draft": saved}

@router.delete("/dealmemos/{uid}")
def delete_draft(uid: str) -> dict:
    items = _read_drafts()
    new_items = [d for d in items if (d.get("uid") or "").strip() != uid.strip()]
    if len(new_items) == len(items):
        raise HTTPException(status_code=404, detail="Draft not found")
    _write(new_items)
    return {"ok": True, "deleted": uid}

@router.get("/dealmemos/_where")
def where_file():
    return { "path": str(TEMP_DRAFTS_PATH), "exists": TEMP_DRAFTS_PATH.exists() }

@router.post("/dealmemos/_touch")
def touch_file():
    if not TEMP_DRAFTS_PATH.exists():
        _write([])
    return {"ok": True, "path": str(TEMP_DRAFTS_PATH), "exists": TEMP_DRAFTS_PATH.exists()}
