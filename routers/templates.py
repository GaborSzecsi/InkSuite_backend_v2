# marble_app/routers/templates.py
from __future__ import annotations

import os
import json
import shutil
import hashlib
import mimetypes
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Literal, Optional, Tuple

from fastapi import (
    APIRouter,
    UploadFile,
    File,
    Form,
    HTTPException,
    Body,
    Request,
    Response,
)
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field

# deps:
#   pip install python-docx mammoth requests python-dotenv
from docx import Document
import mammoth
import requests

# -------------------- .env loading (robust for reloader/WD quirks) --------------------
try:
    from dotenv import load_dotenv  # type: ignore
    # Load ../.env (project root), even if uvicorn runs from a different CWD
    _ENV_PATH = Path(__file__).resolve().parents[1] / ".env"
    if _ENV_PATH.exists():
        load_dotenv(dotenv_path=_ENV_PATH, override=False)
except Exception:
    # Don't hard-fail if dotenv isn't available
    pass

# Use env or sane local defaults so you don't get 503s while developing
DOCSERVICE_URL = os.environ.get("ONLYOFFICE_DOCSERVICE_URL", "").strip() or "http://localhost:8082"
PUBLIC_BASE_URL = os.environ.get("PUBLIC_BASE_URL", "").strip() or "http://127.0.0.1:8000"

# -------------------- Routers --------------------
router = APIRouter(prefix="/contracts", tags=["contracts"])
onlyoffice_router = APIRouter(prefix="/onlyoffice", tags=["onlyoffice"])

# -------------------- Storage layout --------------------
# C:\Users\<you>\Documents\marble_app\data\Templates
BASE_DATA_DIR = Path.home() / "Documents" / "marble_app" / "data"
TEMPLATES_DIR = BASE_DATA_DIR / "Templates"
TEMPLATES_DIR.mkdir(parents=True, exist_ok=True)

INDEX_PATH = TEMPLATES_DIR / "templates_index.json"
VERSIONS_DIR = TEMPLATES_DIR / "Versions"
VERSIONS_DIR.mkdir(parents=True, exist_ok=True)

# -------------------- Models --------------------
TypeStr = Literal["author", "illustrator", "generic"]
DealTypeStr = Literal["single", "series", "other"]

class ContractTemplate(BaseModel):
    id: str
    name: str
    filename: str                 # stored filename on disk (tid_<original>.docx)
    type: TypeStr
    dealType: DealTypeStr
    uploadedAt: str
    placeholders: Optional[List[str]] = None
    previewUrl: Optional[str] = None  # absolute URL to /api/contracts/templates/{id}/file

# ---- Mapping payloads (kept if you still want mapper JSON) ----
class MappingPosition(BaseModel):
    x_pct: float = Field(ge=0, le=100)
    y_pct: float = Field(ge=0, le=100)

class MappingItem(BaseModel):
    field_key: str
    placeholder: str
    position: MappingPosition

class TemplateMapping(BaseModel):
    templateId: str
    mapping: List[MappingItem]
    _format: Optional[str] = None  # e.g., "docx-overlay"

# ---- Inline insert payload ----
class InsertTokenPayload(BaseModel):
    placeholder: str               # e.g. "{{workTitle}}"
    paragraph_index: int
    char_offset: int               # absolute character offset inside the paragraph text
    left_ctx: Optional[str] = None
    right_ctx: Optional[str] = None

# -------------------- Helpers --------------------
def _read_index() -> List[ContractTemplate]:
    if not INDEX_PATH.exists():
        return []
    try:
        data = json.loads(INDEX_PATH.read_text(encoding="utf-8"))
        if isinstance(data, list):
            return [ContractTemplate(**item) for item in data]
        return []
    except Exception:
        return []

def _write_index(items: List[ContractTemplate]) -> None:
    INDEX_PATH.write_text(
        json.dumps([i.model_dump() for i in items], indent=2, ensure_ascii=False),
        encoding="utf-8",
    )

def _file_path_for_id(tid: str) -> Path:
    """Resolve the physical file path by template id."""
    for pat in (f"{tid}_*", f"{tid}__*"):
        for p in TEMPLATES_DIR.glob(pat):
            if p.is_file():
                return p
    return TEMPLATES_DIR / f"{tid}_MISSING"

def _abs_url(request: Request, rel: str) -> str:
    # Prefer PUBLIC_BASE_URL if set; otherwise use request.base_url
    base = (PUBLIC_BASE_URL or str(request.base_url)).rstrip("/")
    if not rel.startswith("/"):
        rel = "/" + rel
    return f"{base}{rel}"

def _get_template_obj(tid: str) -> Optional[ContractTemplate]:
    for it in _read_index():
        if it.id == tid:
            return it
    return None

def _version_backup(path: Path, tid: str) -> None:
    """Save a timestamped backup of the current file if it exists."""
    if not path.exists():
        return
    ts = datetime.utcnow().strftime("%Y%m%d-%H%M%S")
    vdir = VERSIONS_DIR / tid
    vdir.mkdir(parents=True, exist_ok=True)
    backup_path = vdir / f"{ts}.docx"
    try:
        shutil.copyfile(path, backup_path)
    except Exception as e:
        print(f"[templates] version backup failed: {e}")

def _touch_index_after_save(tid: str, path: Path) -> None:
    items = _read_index()
    changed = False
    for i, it in enumerate(items):
        if it.id == tid:
            it.uploadedAt = datetime.utcnow().isoformat() + "Z"
            it.filename = path.name
            items[i] = it
            changed = True
            break
    if changed:
        _write_index(items)

def _safe_remove(p: Path) -> None:
    try:
        if p.is_dir():
            shutil.rmtree(p, ignore_errors=True)
        elif p.exists():
            p.unlink(missing_ok=True)
    except Exception:
        # swallow errors; we'll still update the index
        pass

# -------------------- /api/contracts/... --------------------
@router.get("/templates", response_model=List[ContractTemplate])
async def list_templates(request: Request):
    items = _read_index()
    base = str(request.base_url).rstrip("/")
    changed = False
    for it in items:
        want = f"{base}/api/contracts/templates/{it.id}/file"
        if it.previewUrl != want:
            it.previewUrl = want
            changed = True
    if changed:
        _write_index(items)
    return items

@router.post("/templates", response_model=ContractTemplate)
async def upload_template(
    request: Request,
    file: UploadFile = File(...),
    name: str = Form(...),
    type: TypeStr = Form(...),
    dealType: DealTypeStr = Form(...),
):
    if not file.filename.lower().endswith(".docx"):
        raise HTTPException(status_code=400, detail="Only .docx files are accepted.")

    tid = os.urandom(16).hex()
    original_name = file.filename.rsplit("\\", 1)[-1].rsplit("/", 1)[-1]
    stored_filename = f"{tid}_{original_name}"
    target_path = TEMPLATES_DIR / stored_filename

    try:
        with target_path.open("wb") as out:
            shutil.copyfileobj(file.file, out)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to save file: {e}")

    item = ContractTemplate(
        id=tid,
        name=name,
        filename=stored_filename,
        type=type,
        dealType=dealType,
        uploadedAt=datetime.utcnow().isoformat() + "Z",
        placeholders=None,
        previewUrl=f"{str(request.base_url).rstrip('/')}/api/contracts/templates/{tid}/file",
    )

    items = _read_index()
    items.append(item)
    _write_index(items)
    return item

@router.get("/templates/{tid}", response_model=ContractTemplate)
async def get_template(tid: str, request: Request):
    it = _get_template_obj(tid)
    if not it:
        raise HTTPException(status_code=404, detail="Template not found")

    want = f"{str(request.base_url).rstrip('/')}/api/contracts/templates/{tid}/file"
    if it.previewUrl != want:
        items = _read_index()
        for i, x in enumerate(items):
            if x.id == tid:
                items[i].previewUrl = want
                break
        _write_index(items)
        it.previewUrl = want
    return it

@router.get("/templates/{tid}/file")
async def get_template_file(tid: str):
    """Serve the stored .docx inline so the browser (and previewers) can fetch it."""
    path = _file_path_for_id(tid)
    if not path.exists():
        raise HTTPException(status_code=404, detail="File not found on disk")

    media_type = (
        mimetypes.guess_type(path.name)[0]
        or "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
    )
    headers = {
        "Content-Disposition": f'inline; filename="{path.name.split("_",1)[-1]}"',
        "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
        "Pragma": "no-cache",
        "Expires": "0",
        "ETag": str(int(path.stat().st_mtime)),
    }
    return FileResponse(path, media_type=media_type, filename=path.name.split("_", 1)[-1], headers=headers)

# ---- HTML preview (if you still use it anywhere)
@router.get("/templates/{tid}/html")
async def get_template_html(tid: str):
    path = _file_path_for_id(tid)
    if not path.exists():
        raise HTTPException(status_code=404, detail="Template file not on disk")

    with open(path, "rb") as f:
        result = mammoth.convert_to_html(f, style_map="")
    raw_html = result.value or ""

    parts = raw_html.split("<p")
    rebuilt = []
    p_index = -1
    for i, part in enumerate(parts):
        if i == 0:
            rebuilt.append(part)
            continue
        p_index += 1
        rebuilt.append(f'<p data-p-index="{p_index}"' + part)
    indexed_html = "".join(rebuilt)

    return {"ok": True, "html": indexed_html, "updated": datetime.utcnow().isoformat() + "Z"}

# ---- Insert token helpers (kept for mapper flow)
def _clone_run_format(src_run, dst_run):
    sf, df = src_run.font, dst_run.font
    df.name = sf.name
    df.size = sf.size
    df.bold = sf.bold
    df.italic = sf.italic
    df.underline = sf.underline
    df.color.rgb = getattr(sf.color, "rgb", None)
    df.all_caps = sf.all_caps
    df.small_caps = sf.small_caps

def _normalize_text(s: str) -> str:
    return (s or "").replace("\u00A0", " ").replace("\r", "").replace("\n", " ").replace("\t", " ").strip()

def _find_offset_with_context(full_text: str, raw_offset: int, left_ctx: Optional[str], right_ctx: Optional[str]) -> int:
    ntext = _normalize_text(full_text)
    if left_ctx:
        left_ctx = _normalize_text(left_ctx)
    if right_ctx:
        right_ctx = _normalize_text(right_ctx)
    if left_ctx and right_ctx:
        li = ntext.find(left_ctx)
        if li >= 0:
            start = li + len(left_ctx)
            if right_ctx == "" or ntext.find(right_ctx, start) >= 0:
                return min(max(start, 0), len(ntext))
    if left_ctx and not right_ctx:
        li = ntext.find(left_ctx)
        if li >= 0:
            return min(max(li + len(left_ctx), 0), len(ntext))
    if right_ctx and not left_ctx:
        ri = ntext.find(right_ctx)
        if ri >= 0:
            return min(max(ri, 0), len(ntext))
    return min(max(raw_offset, 0), len(ntext))

def _insert_text_at_paragraph_offset(p, offset: int, token: str) -> Tuple[bool, int]:
    runs = list(p.runs)
    positions = []
    total = 0
    for r in runs:
        t = r.text or ""
        start = total
        end = start + len(t)
        positions.append((r, t, start, end))
        total = end

    if offset < 0:
        offset = 0
    if offset > total:
        offset = total

    if not runs:
        new_r = p.add_run(token)
        return True, 0

    target_index = None
    for i, (_, _, start, end) in enumerate(positions):
        if start <= offset <= end:
            target_index = i
            break
    if target_index is None:
        r_last, t_last, *_ = positions[-1]
        r_last.text = (t_last or "") + token
        return True, total

    r, t, start, end = positions[target_index]
    inner = max(0, min(offset - start, len(t)))
    before = t[:inner]
    after = t[inner:]

    r.text = before
    new_run = p.add_run("")
    p._p.remove(new_run._r)
    r._r.addnext(new_run._r)
    new_run.text = token
    _clone_run_format(r, new_run)

    if after:
        tail_run = p.add_run("")
        p._p.remove(tail_run._r)
        new_run._r.addnext(tail_run._r)
        tail_run.text = after
        _clone_run_format(r, tail_run)

    return True, offset

@router.post("/templates/{tid}/insert-token")
async def insert_token(tid: str, body: InsertTokenPayload):
    path = _file_path_for_id(tid)
    if not path.exists():
        raise HTTPException(status_code=404, detail="Template file not on disk")
    token_text = (body.placeholder or "").strip()
    if not token_text:
        raise HTTPException(status_code=400, detail="placeholder cannot be empty")

    doc = Document(path)
    paragraphs = list(doc.paragraphs)
    if body.paragraph_index < 0 or body.paragraph_index >= len(paragraphs):
        raise HTTPException(status_code=400, detail="paragraph_index out of range")
    p = paragraphs[body.paragraph_index]

    para_text = p.text or ""
    effective_offset = _find_offset_with_context(para_text, body.char_offset, body.left_ctx, body.right_ctx)
    ok, eff = _insert_text_at_paragraph_offset(p, effective_offset, token_text)
    if not ok:
        raise HTTPException(status_code=500, detail="failed to insert token")

    doc.save(path)

    items = _read_index()
    changed = False
    for it in items:
        if it.id == tid:
            phs = set(it.placeholders or [])
            if token_text not in phs:
                phs.add(token_text)
                it.placeholders = sorted(phs)
                changed = True
            break
    if changed:
        _write_index(items)

    return {
        "ok": True,
        "templateId": tid,
        "paragraph_index": body.paragraph_index,
        "char_offset": eff,
        "inserted": token_text,
        "matched_by": "context" if (body.left_ctx or body.right_ctx) else "offset",
        "saved_at": datetime.utcnow().isoformat() + "Z",
    }

@router.post("/templates/{tid}/mapping")
async def save_template_mapping(tid: str, payload: TemplateMapping):
    if payload.templateId != tid:
        raise HTTPException(status_code=400, detail="templateId mismatch")

    path = _file_path_for_id(tid)
    if not path.exists():
        raise HTTPException(status_code=404, detail="Template file not found on disk")

    mapping_path = TEMPLATES_DIR / f"{tid}.mapping.json"
    mapping_obj: Dict[str, Any] = {
        "template_id": tid,
        "saved_at": datetime.utcnow().isoformat() + "Z",
        "mapping": [mi.model_dump() for mi in payload.mapping],
        "_format": payload._format or "docx-overlay",
    }
    mapping_path.write_text(json.dumps(mapping_obj, indent=2, ensure_ascii=False), encoding="utf-8")

    items = _read_index()
    ph = sorted({m.placeholder for m in payload.mapping if m.placeholder})
    changed = False
    for it in items:
        if it.id == tid:
            it.placeholders = ph
            changed = True
            break
    if changed:
        _write_index(items)

    return {
        "ok": True,
        "templateId": tid,
        "placeholders": ph,
        "mapping_file": str(mapping_path),
        "saved_at": datetime.utcnow().isoformat() + "Z",
    }

# -------------------- /api/contracts/templates/{tid}/save (backend persist) --------------------
@router.post("/templates/{tid}/save")
async def persist_updated_template(tid: str, request: Request):
    """
    Receive an updated DOCX (bytes) and overwrite it. No versioning.
    """
    path = _file_path_for_id(tid)
    if not path.exists():
        raise HTTPException(status_code=404, detail="Template not found on disk")

    try:
        body = await request.body()
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Failed to read body: {e}")

    if not body:
        raise HTTPException(status_code=400, detail="Empty body")

    try:
        tmp = path.with_suffix(".tmp.docx")
        with open(tmp, "wb") as f:
            f.write(body)
        tmp.replace(path)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to write docx: {e}")

    _touch_index_after_save(tid, path)
    return {"ok": True, "id": tid, "path": str(path)}

# -------------------- small no-op force-save (UI ping) --------------------
@router.post("/templates/{tid}/force-save")
async def force_save_noop(tid: str):
    """
    Frontend pings this to request a save. We rely on ONLYOFFICE autosave/callback,
    so this is a harmless ACK to keep the UI happy.
    """
    return {"ok": True, "id": tid}

# -------------------- DELETE template --------------------
@router.delete("/templates/{tid}", status_code=204)
async def delete_template(tid: str):
    """
    Permanently delete a template: the .docx, mapping JSON, version backups,
    and its index entry. Returns 204 on success.
    """
    items = _read_index()
    idx = next((i for i, it in enumerate(items) if it.id == tid), None)
    if idx is None:
        raise HTTPException(status_code=404, detail="Template not found")

    # likely artifacts
    docx_path = _file_path_for_id(tid)
    mapping_path = TEMPLATES_DIR / f"{tid}.mapping.json"
    versions_path = VERSIONS_DIR / tid

    _safe_remove(docx_path)
    _safe_remove(mapping_path)
    _safe_remove(versions_path)

    # remove from index
    del items[idx]
    _write_index(items)

    return Response(status_code=204)

# -------------------- /api/onlyoffice/... (Document Server integration) --------------------
def _doc_key_for(path: Path) -> str:
    """
    Build a deterministic key that changes when the file changes.
    ONLYOFFICE requires a unique key per document version (<= 128 chars).
    """
    stat = path.stat()
    raw = f"{path.name}:{int(stat.st_mtime_ns)}"
    h = hashlib.sha256(raw.encode("utf-8")).hexdigest()
    return h[:40]

@onlyoffice_router.get("/config/{tid}")
async def onlyoffice_config(tid: str, request: Request):
    """
    Return a minimal ONLYOFFICE editor config for the given template.
    The Document Server will load the file from our /api/contracts/templates/{tid}/file URL.
    """
    tpl = _get_template_obj(tid)
    if not tpl:
        raise HTTPException(status_code=404, detail="Template not found")

    path = _file_path_for_id(tid)
    if not path.exists():
        raise HTTPException(status_code=404, detail="Template file not found on disk")

    docservice = DOCSERVICE_URL.rstrip("/")
    # Absolute URL to .docx that DocumentServer can fetch
    doc_url = _abs_url(request, f"/api/contracts/templates/{tid}/file")
    callback_url = _abs_url(request, f"/api/onlyoffice/callback/{tid}")
    title = path.name.split("_", 1)[-1]
    key = _doc_key_for(path)

    config: Dict[str, Any] = {
        "document": {
            "fileType": "docx",
            "key": key,
            "title": title,
            "url": doc_url,
            "permissions": {
                "edit": True,
                "download": True,
                "print": True,
                "copy": True,
                "fillForms": True,
                "review": True,
                "comment": True,
            },
        },
        "editorConfig": {
            "mode": "edit",
            "callbackUrl": callback_url,
            "user": {
                # In real apps, pull from your auth session
                "id": "local-user",
                "name": "Template Editor",
            },
            # ⬇️ autosave must be here (not in customization)
            "autosave": True,
            "customization": {
                "toolbar": True,
                "forcesave": True,     # shows Save button -> status 6
                "hideRightMenu": True, # hide right panel
                "toolbarNoTabs": False,
                "chat": False,
            },
        },
    }

    return {"docServiceUrl": docservice, "config": config}

@onlyoffice_router.post("/callback/{tid}")
async def onlyoffice_callback(tid: str, payload: Dict[str, Any] = Body(default={})):
    """
    Handle save callbacks from ONLYOFFICE Document Server.
    On status 2 (autosave/close) or 6 (Force Save), download payload.url and overwrite the .docx.
    MUST return {"error":0}.
    """
    path = _file_path_for_id(tid)
    if not path.exists():
        # Acknowledge anyway so DS doesn't retry forever
        return {"error": 0}

    try:
        status = int(payload.get("status") or 0)
    except Exception:
        status = 0

    if status in (2, 6):
        download_url = payload.get("url")
        if not download_url:
            return {"error": 1}

        headers: Dict[str, str] = {}
        token = payload.get("token")
        if isinstance(token, str) and token:
            headers["Authorization"] = f"Bearer {token}"

        try:
            r = requests.get(download_url, headers=headers, timeout=60)
            r.raise_for_status()

            # 🔁 Overwrite in place (no versions)
            tmp = path.with_suffix(".tmp.docx")
            with tmp.open("wb") as f:
                f.write(r.content)
            tmp.replace(path)

            # update index timestamp (last modified)
            _touch_index_after_save(tid, path)
        except Exception as e:
            print(f"[onlyoffice_callback] save error: {e}")
            return {"error": 1}

    return {"error": 0}

# -------------------- Aliases under /contracts/templates/{tid}/onlyoffice-* --------------------
@router.get("/templates/{tid}/onlyoffice-config")
async def alias_onlyoffice_config(tid: str, request: Request):
    return await onlyoffice_config(tid, request)

@router.post("/templates/{tid}/onlyoffice-callback")
async def alias_onlyoffice_callback(tid: str, payload: Dict[str, Any] = Body(default={})):
    return await onlyoffice_callback(tid, payload)
