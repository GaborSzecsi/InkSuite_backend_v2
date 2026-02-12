from fastapi import APIRouter, UploadFile, File, Form, HTTPException
from pathlib import Path
from typing import List
from datetime import datetime, timezone
import asyncio
import subprocess
import sys

router = APIRouter()

# Base folder to store uploaded financial CSVs
BASE_DIR = Path(__file__).resolve().parent.parent
UPLOAD_DIR = BASE_DIR / "data" / "financials"
UPLOAD_DIR.mkdir(parents=True, exist_ok=True)

# Where your ingest script lives (adjust if needed)
INGEST_SCRIPT = BASE_DIR / "routers" / "financials_ingest.py"


async def run_financials_ingest() -> dict:
    """
    Run the ingest script that rebuilds book_data/financials.json.
    Runs in a thread so we don't block the event loop.
    """
    if not INGEST_SCRIPT.exists():
        raise HTTPException(
            status_code=500,
            detail=f"financials_ingest.py not found at: {INGEST_SCRIPT}",
        )

    def _run():
        # Use the same interpreter running FastAPI
        proc = subprocess.run(
            [sys.executable, str(INGEST_SCRIPT)],
            cwd=str(BASE_DIR),
            capture_output=True,
            text=True,
        )
        return proc

    proc = await asyncio.to_thread(_run)

    # If ingest failed, surface the error immediately
    if proc.returncode != 0:
        raise HTTPException(
            status_code=500,
            detail={
                "message": "financials ingest failed",
                "returncode": proc.returncode,
                "stdout": (proc.stdout or "").strip()[-4000:],
                "stderr": (proc.stderr or "").strip()[-4000:],
            },
        )

    return {
        "ok": True,
        "returncode": proc.returncode,
        "stdout": (proc.stdout or "").strip()[-4000:],
        "stderr": (proc.stderr or "").strip()[-4000:],
    }


@router.post("/financials/upload")
async def upload_financial_file(
    file: UploadFile = File(...),
    periodCode: str = Form(...),
    year: int = Form(...),
    month: int = Form(...),
    reportKind: str = Form(...),
    rebuild: bool = Form(True),  # <- NEW: rebuild by default after every upload
):
    """
    Save a financial CSV file to disk under:
      data/financials/YYYY-MM/<reportKind>/<original_filename>

    Then (optionally) rebuild book_data/financials.json by running financials_ingest.py
    """

    original_name = file.filename
    if not original_name:
        raise HTTPException(status_code=400, detail="Missing filename")

    # Folder structure: data/financials/2025-10/Activity_Summary_Report/...
    dest_dir = UPLOAD_DIR / f"{year:04d}-{month:02d}" / reportKind
    dest_dir.mkdir(parents=True, exist_ok=True)

    dest_path = dest_dir / original_name

    content = await file.read()
    if not content:
        raise HTTPException(status_code=400, detail="Uploaded file is empty")

    dest_path.write_bytes(content)

    uploaded_at = datetime.now(timezone.utc).isoformat()

    ingest_result = None
    if rebuild:
        ingest_result = await run_financials_ingest()

    return {
        "ok": True,
        "filename": original_name,
        "saved_as": str(dest_path.relative_to(BASE_DIR)),
        "periodCode": periodCode,
        "year": year,
        "month": month,
        "reportKind": reportKind,
        "uploadedAt": uploaded_at,
        "rebuildRan": bool(rebuild),
        "ingest": ingest_result,
    }


@router.get("/financials/uploads")
async def list_financial_uploads() -> List[dict]:
    """
    Very simple index: scan data/financials and return what we find.
    This is enough to make the 'Uploaded List' tab work.
    """

    items: List[dict] = []

    if not UPLOAD_DIR.exists():
        return items

    for period_dir in UPLOAD_DIR.iterdir():
        if not period_dir.is_dir():
            continue

        # Try to parse YYYY-MM from folder name
        try:
            year_str, month_str = period_dir.name.split("-")
            year = int(year_str)
            month = int(month_str)
        except Exception:
            year = None
            month = None

        for report_dir in period_dir.iterdir():
            if not report_dir.is_dir():
                continue
            report_kind = report_dir.name

            for f in report_dir.iterdir():
                if not f.is_file():
                    continue

                stat = f.stat()
                uploaded_at = datetime.fromtimestamp(
                    stat.st_mtime, tz=timezone.utc
                ).isoformat()

                items.append(
                    {
                        "periodCode": "",  # not tracked yet
                        "year": year,
                        "month": month,
                        "reportKind": report_kind,
                        "filename": f.name,
                        "uploadedAt": uploaded_at,
                        "status": "Uploaded",
                    }
                )

    return items
