# marble_app/routers/ingest.py
from __future__ import annotations

import re
import uuid
from pathlib import Path

from fastapi import APIRouter, HTTPException, UploadFile, File, Form
from pydantic import BaseModel

from marble_app.services.camcat_ingest import ingest_onix

router = APIRouter(prefix="/ingest", tags=["ingest"])

# Set these to your real paths (same style you use elsewhere)
BOOKS_JSON = Path(r"C:\Users\szecs\Documents\marble_app\book_data\books.json")
UPLOADS_ROOT = Path(r"C:\Users\szecs\Documents\marble_app\data\uploads")

_ONIX_IMPORTS_DIRNAME = "_onix_imports"
_BAD_CHARS = re.compile(r"[^a-zA-Z0-9.\-_]+")


class OnixIngestRequest(BaseModel):
    uid: str                 # book UUID folder
    filename: str            # uploaded ONIX filename (stored under that uid folder)
    source_tag: str = "onix" # optional label: camcat/ingram/etc


def _safe_filename(name: str) -> str:
    name = (name or "onix.xml").strip()
    name = name.split("/")[-1].split("\\")[-1]
    name = _BAD_CHARS.sub("_", name)
    return name or "onix.xml"


def find_uploaded_file(uid: str, filename: str) -> Path:
    root = UPLOADS_ROOT / uid
    if not root.exists():
        raise HTTPException(404, f"Uploads folder not found for uid {uid}")
    # look in root and in subfolders (so you can store ONIX under /onix/ too)
    direct = root / filename
    if direct.exists():
        return direct
    hits = list(root.rglob(filename))
    if hits:
        return hits[0]
    raise HTTPException(404, f"ONIX file '{filename}' not found under uploads/{uid}")


@router.post("/onix")
def ingest_uploaded_onix(req: OnixIngestRequest):
    """
    Ingest an ONIX file that already exists somewhere under:
      data/uploads/<uid>/...

    This is useful if you upload ONIX into a book folder first.
    """
    onix_path = find_uploaded_file(req.uid, req.filename)

    result = ingest_onix(
        onix_xml_path=onix_path,
        books_json_path=BOOKS_JSON,
        uploads_root=UPLOADS_ROOT,
        covers_dir=None,
        source_tag=req.source_tag or "onix",
    )

    return {"status": "ok", "result": result, "onix_path": str(onix_path)}


@router.post("/onix-file")
async def ingest_onix_file(
    file: UploadFile = File(...),
    source_tag: str = Form("onix"),
):
    """
    Ingest an ONIX XML by uploading it directly (multipart/form-data).

    Saves to:
      data/uploads/_onix_imports/<uuid>/<original_filename>

    Then runs ingest.
    """
    if not file:
        raise HTTPException(400, "Missing file")

    safe_name = _safe_filename(file.filename or "onix.xml")
    batch_id = str(uuid.uuid4())
    dest_dir = UPLOADS_ROOT / _ONIX_IMPORTS_DIRNAME / batch_id
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest_path = dest_dir / safe_name

    try:
        content = await file.read()
        dest_path.write_bytes(content)
    except Exception as e:
        raise HTTPException(500, f"Failed to save uploaded file: {e}")

    try:
        result = ingest_onix(
            onix_xml_path=dest_path,
            books_json_path=BOOKS_JSON,
            uploads_root=UPLOADS_ROOT,
            covers_dir=None,
            source_tag=source_tag or "onix",
        )
    except Exception as e:
        raise HTTPException(500, f"ONIX ingest failed: {e}")

    return {
        "status": "ok",
        "source_tag": source_tag or "onix",
        "saved_to": str(dest_path),
        "result": result,
    }
