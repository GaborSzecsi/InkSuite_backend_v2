import os, io, time, pathlib
from typing import Literal
from fastapi import APIRouter, UploadFile, File, Form, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from PIL import Image

router = APIRouter()

DATA_UPLOAD_DIR = os.environ.get("DATA_UPLOAD_DIR", "./data/uploads")
pathlib.Path(DATA_UPLOAD_DIR).mkdir(parents=True, exist_ok=True)

UploadKind = Literal["author_contract","illustrator_contract","w9","book_cover","other"]

ALLOWED = {
    "author_contract": {"application/pdf"},
    "illustrator_contract": {"application/pdf"},
    "w9": {"application/pdf"},
    "book_cover": {"image/jpeg","image/png"},
    "other": {"application/pdf","image/jpeg","image/png"},
}

class UploadResponse(BaseModel):
    ok: bool
    url: str
    filename: str
    mime: str
    size: int
    width: int | None = None
    height: int | None = None
    dpi: tuple[int,int] | None = None

def _clean_name(name: str) -> str:
    return "".join(c for c in name if c.isalnum() or c in ("-","_",".")).strip("._")

@router.post("/", response_model=UploadResponse)
async def upload_file(
    file: UploadFile = File(...),
    kind: UploadKind = Form(...),
    book_key: str = Form("")
):
    allowed = ALLOWED.get(kind, set())
    mime = file.content_type or ""
    if allowed and mime not in allowed:
        raise HTTPException(status_code=400, detail=f"Unsupported type for {kind}: {mime}")

    data = await file.read()
    size = len(data)
    if size == 0:
        raise HTTPException(status_code=400, detail="Empty file")

    width = height = None
    dpi_tuple = None
    if mime.startswith("image/"):
        try:
            img = Image.open(io.BytesIO(data))
            width, height = img.size
            if "dpi" in img.info and isinstance(img.info["dpi"], tuple):
                dpi_tuple = tuple(int(round(x)) for x in img.info["dpi"])
            if kind == "book_cover":
                if not dpi_tuple or dpi_tuple[0] < 300 or dpi_tuple[1] < 300:
                    raise HTTPException(status_code=400, detail="Cover must be at least 300 DPI")
        except HTTPException:
            raise
        except Exception:
            raise HTTPException(status_code=400, detail="Invalid image file")

    safe_dir = os.path.join(DATA_UPLOAD_DIR, _clean_name(book_key) or "_")
    pathlib.Path(safe_dir).mkdir(parents=True, exist_ok=True)

    ext = pathlib.Path(file.filename or "upload.bin").suffix or ""
    safe_name = f"{int(time.time()*1000)}_{_clean_name(file.filename or 'file')}{ext}"
    out_path = os.path.join(safe_dir, safe_name)
    with open(out_path, "wb") as f:
        f.write(data)

    url = f"/static/uploads/{_clean_name(book_key) or '_'}/{safe_name}"

    return UploadResponse(
        ok=True, url=url, filename=file.filename or safe_name,
        mime=mime, size=size, width=width, height=height, dpi=dpi_tuple
    )
