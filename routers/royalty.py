from typing import List, Dict, Any
from pathlib import Path
import json
import traceback
import base64
from datetime import datetime
from decimal import Decimal, InvalidOperation

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import HTMLResponse, Response

from marble_app.models.royalty import ( Book, SalesData, RoyaltyStatementRequest, RoyaltyCalculation, PaymentSummary )
from marble_app.services.royalty_calculator import RoyaltyCalculator
import os, glob, shutil, subprocess, tempfile

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

# Absolute paths (adjust if you move the app)
BOOKS_FILE = Path(r"C:\Users\szecs\Documents\marble_app\book_data\books.json")
LOGO_PATH = Path(r"C:\Users\szecs\Documents\marble_app\assets\logo long2 NEW.png")
UPLOADS_DIR = Path(r"C:\Users\szecs\Documents\marble_app\data\uploads")
ROYALTY_DATA_DIR = Path(r"C:\Users\szecs\Documents\marble_app\book_data")

# Global override (debug): force ReportLab instead of Playwright
FORCE_REPORTLAB_PDFS = False
FORCE_RASTERIZED_PDF = False
RASTER_DPI = 300
FORCE_WEASYPRINT = True

# =============================
#           ROUTES
# =============================

@router.get("/")
def info():
    try:
        books = calculator.get_books()
        total = len(books) if isinstance(books, list) else (len(books.get("books", [])) if isinstance(books, dict) else 0)
    except Exception:
        total = 0

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
        },
        "total_books": total,
    }

@router.get("/books", response_model=List[Dict[str, Any]])
def get_books():
    """
    Returns a top-level JSON array of books.
    Falls back to reading the known file directly if the calculator path is off.
    """
    try:
        data = calculator.get_books()
        if isinstance(data, dict) and "books" in data:
            data = data["books"]
        if data is None:
            data = []
        if not isinstance(data, list):
            raise HTTPException(status_code=500, detail=f"books.json must be a JSON array, got {type(data).__name__}")
        return data
    except Exception as e:
        try:
            print("[/api/royalty/books] calculator.get_books() failed:", e, "\n", traceback.format_exc())
            if not BOOKS_FILE.exists():
                raise HTTPException(status_code=500, detail=f"Data file not found: {BOOKS_FILE}")
            arr = json.loads(BOOKS_FILE.read_text(encoding="utf-8"))
            if isinstance(arr, dict) and "books" in arr:
                arr = arr["books"]
            if not isinstance(arr, list):
                raise HTTPException(status_code=500, detail=f"books.json must be a JSON array, got {type(arr).__name__}")
            return arr
        except HTTPException:
            raise
        except json.JSONDecodeError as je:
            raise HTTPException(status_code=500, detail=f"Invalid JSON in {BOOKS_FILE}: {je}")
        except Exception as ex:
            raise HTTPException(status_code=500, detail=f"Unexpected error: {ex}")

@router.post("/books")
def save_book(payload: Dict[str, Any]):
    """
    Save or update a book while preserving ALL fields provided by the frontend.
    """
    try:
        result = calculator.save_book_raw(payload)
        return {"message": "Book saved successfully", "book": result}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@router.delete("/books")
def delete_book(title: str, author: str):
    success = calculator.delete_book(title, author)
    if success:
        return {"message": "Book deleted successfully"}
    raise HTTPException(status_code=404, detail="Book not found")

@router.post("/calculate")
def calculate_royalties(request: RoyaltyStatementRequest):
    """
    Finds the requested book by uid and returns the calculation dict.
    """
    books = calculator.get_books() or []
    for b in books:
        if b.get('uid') == request.uid:
            book = Book.model_validate(b)
            calcs = calculator.calculate_royalties(request, book)  # dict
            return {"message": "OK", "calculations": calcs}
    raise HTTPException(status_code=404, detail=f"Book not found for uid: {request.uid}")

@router.post("/statements")
def save_royalty_statement(request: RoyaltyStatementRequest):
    """
    Saves royalty statements and generates PDFs conditionally:
    - Author: if there is any sales/royalty row (same as before)
    - Illustrator: ONLY if there is at least one royalty % > 0
    """
    def has_party_rows(pdata: Dict | None) -> bool:
        if not pdata:
            return False
        cats = pdata.get("categories") or []
        return bool(isinstance(cats, list) and len(cats) > 0)

    books = calculator.get_books() or []
    for b in books:
        if b.get("uid") == request.uid:
            book = Book.model_validate(b)

            # Calculate once
            calcs = calculator.calculate_royalties(request, book)
            author_data = calcs.get("author") or {}
            illustrator_data = calcs.get("illustrator") or {}

            saved_parties: list[str] = []

            def write_party_json(party: str, party_data: Dict):
                party_file = ROYALTY_DATA_DIR / f"{party}_royalty.json"
                statement_data = {
                    "uid": request.uid,
                    "book_title": book.title,
                    "book_author": book.author,
                    "party": party,
                    "period_start": request.period_start,
                    "period_end": request.period_end,
                    "generated_at": datetime.now().isoformat(),
                    "sales_data": [sd.dict() if hasattr(sd, "dict") else dict(sd) for sd in request.sales_data],
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

            book_upload_dir = UPLOADS_DIR / request.uid
            book_upload_dir.mkdir(parents=True, exist_ok=True)

            # --- AUTHOR (unchanged)
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

            # --- ILLUSTRATOR: require royalty % > 0
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

            # Rebuild index but don't fail the request
            try:
                rebuild_author_index_from_log(ROYALTY_DATA_DIR)
            except Exception as reidx_err:
                print(f"[save] WARNING: could not rebuild author index: {reidx_err}")

            if not saved_parties:
                return {"message": "No statements saved (author/illustrator conditions not met).", "saved": []}
            return {"message": "Saved", "saved": saved_parties}

    raise HTTPException(status_code=404, detail=f"Book not found for uid: {request.uid}")

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
    format: str = Query("html", regex="^(html|pdf)$"),
    party: str = Query("author", regex="^(author|illustrator)$")
):
    """
    Render a royalty statement as HTML or PDF for a specific party.
    Also saves statement data to JSON files for history.

    Rule:
      - If party == 'illustrator', only allow render/save when at least one
        row has a royalty % strictly greater than 0. If not, return a 200
        HTML placeholder (for html) or 204 (for pdf) instead of 400.
    """
    # Locate book
    books = calculator.get_books() or []
    book_data = next((b for b in books if b.get("uid") == request.uid), None)
    if not book_data:
        available_uids = [b.get('uid') for b in books if b.get('uid')][:5]
        raise HTTPException(
            status_code=404,
            detail=f"Book not found for uid: {request.uid}. Available UIDs (first 5): {available_uids}"
        )
    book = Book.model_validate(book_data)

    # Calculate once
    calcs = calculator.calculate_royalties(request, book)
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
            # Hint to the frontend that no statement exists:
            return HTMLResponse(content=placeholder, headers={"X-Statement-Available": "false"})
        else:
            # No PDF to return
            return Response(status_code=204, headers={"X-Statement-Available": "false"})

    # Still here? Party is applicable — continue as before
    if not has_rows:
        raise HTTPException(status_code=400, detail=f"No {party} data in calculations")

    # Build the statement record
    statement_data = {
        "uid": request.uid,
        "book_title": book.title,
        "book_author": book.author,
        "party": party,
        "period_start": request.period_start,
        "period_end": request.period_end,
        "generated_at": datetime.now().isoformat(),
        "sales_data": [sd.dict() if hasattr(sd, "dict") else dict(sd) for sd in request.sales_data],
        "calculations": party_data,
    }

    # Save to rolling JSON per party (history) — replace if same (uid, period)
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

    # ---------- RETURN THE VIEW ----------
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
            "X-Statement-Available": "true"
        }
    )

# --- royalty % > 0 detector for a party's data -------------------------------
def _to_decimal(val) -> Decimal:
    from marble_app.decimal import Decimal, InvalidOperation
    if val is None:
        return Decimal(0)
    s = str(val).strip().replace('%', '').replace(',', '')
    if s == '':
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
            if k in row:
                if _to_decimal(row.get(k)) > 0:
                    return True
    return False

def rebuild_author_index_from_log(ROYALTY_DATA_DIR):
    """
    Rebuild author_royalties.json (plural) from marble_app.author_royalty.json (singular).
    De-dupes by (author/person, book_uid, period_start, period_end), keeping the newest generated_at.
    """
    import json
    from marble_app.collections import defaultdict
    from marble_app.datetime import datetime

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
            # fallback for old shapes
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
            or e.get("party")           # often "author"
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

    from marble_app.collections import defaultdict as dd
    buckets: dict[str, list] = dd(list)
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
    force_reportlab: bool = False
) -> bytes:
    """
    Prefer WeasyPrint (when enabled), otherwise Playwright → optional Ghostscript normalize.
    Falls back to ReportLab only if HTML renderers fail.
    """
    if FORCE_REPORTLAB_PDFS or force_reportlab:
        return generate_statement_pdf_reportlab(book, request, party_data, party)

    html_content = generate_statement_html(book, request, party_data, party, target="pdf")

   # 1) WeasyPrint (best for Acrobat printing)
    if 'FORCE_WEASYPRINT' in globals() and FORCE_WEASYPRINT:
        try:
            return generate_statement_pdf_weasy(book, request, party_data, party)
        except Exception as e:
            print(f"[pdf] WeasyPrint failed: {e}")


    # 2) Playwright (Chromium)
    pdf_bytes = None
    try:
        from marble_app.playwright.sync_api import sync_playwright
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
        # Last resort vector fallback
        return generate_statement_pdf_reportlab(book, request, party_data, party)

    # 3) Optional: Ghostscript normalize (keeps vectors; subsets fonts)
    normalized_bytes = None
    try:
        gs = find_gs_exe()
        if gs:
            with tempfile.TemporaryDirectory() as td:
                in_pdf  = os.path.join(td, "in.pdf")
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

    # Decide the base to return (or rasterize from)
    base_for_output = acrobat_safe or best_vector

    # Optional: rasterize-only path (guaranteed-to-print fallback)
    if 'FORCE_RASTERIZED_PDF' in globals() and FORCE_RASTERIZED_PDF:
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
        # If rasterization failed, still return the best vector we have
        return base_for_output

    # Default: return the best vector (distilled if available)
    return base_for_output

def generate_statement_pdf_reportlab(book: Book, request: RoyaltyStatementRequest, party_data: Dict, party: str) -> bytes:
    """Fallback PDF generation using ReportLab"""
    from marble_app.reportlab.lib.pagesizes import letter
    from marble_app.reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle
    from marble_app.reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
    from marble_app.reportlab.lib.units import inch
    from marble_app.reportlab.lib import colors
    from marble_app.reportlab.lib.enums import TA_CENTER
    from marble_app.io import BytesIO

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

    categories = party_data.get('categories', []) or []
    party_name = (
        book.author if party == 'author' else
        (book.illustrator.name if getattr(book, "illustrator", None) and hasattr(book.illustrator, 'name')
         else str(getattr(book, "illustrator", None) or 'Illustrator'))
    )

    full_title = (book.title or '')
    if getattr(book, 'subtitle', None):
        full_title += f": {book.subtitle}"

    # Build ISBN list
    isbn_html = "N/A"
    if getattr(book, 'formats', None):
        items = []
        for fmt in book.formats:
            fmt_dict = (
                fmt.dict() if hasattr(fmt, 'dict')
                else (fmt.model_dump() if hasattr(fmt, 'model_dump') else (fmt if isinstance(fmt, dict) else {}))
            )
            isbn = fmt_dict.get('isbn') or fmt_dict.get('ISBN')
            format_name = fmt_dict.get('format') or fmt_dict.get('Format') or 'Unknown'
            if isbn and str(isbn).strip():
                items.append(f"{format_name}: {str(isbn).strip()}")
        if items:
            isbn_html = items[0]
            if len(items) > 1:
                isbn_html += "".join(f"<br/>{x}" for x in items[1:])

    advance_val = to_money(party_data.get('advance', 0))
    royalty_val = to_money(party_data.get('royalty_total', 0))
    last_bal = to_money(party_data.get('last_balance', 0))
    balance_val = to_money(party_data.get('balance', 0))
    payable_val = to_money(party_data.get('payable', 0))

    buffer = BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=letter, topMargin=0.75*inch, bottomMargin=0.75*inch)

    styles = getSampleStyleSheet()
    title_style = ParagraphStyle('CustomTitle', parent=styles['Heading1'], fontSize=18, spaceAfter=20, alignment=TA_CENTER)
    header_style = ParagraphStyle('Header', parent=styles['Normal'], fontSize=12, spaceAfter=6)

    story = []
    story.append(Paragraph("ROYALTY STATEMENT", title_style))
    story.append(Paragraph(f"Period: {request.period_start} to {request.period_end}", header_style))
    story.append(Spacer(1, 20))
    story.append(Paragraph(f"<b>Book Title:</b> {full_title}", styles['Normal']))
    story.append(Spacer(1, 6))
    story.append(Paragraph(f"<b>ISBN(s):</b> {isbn_html}", styles['Normal']))
    story.append(Spacer(1, 6))
    story.append(Paragraph(f"<b>Statement For:</b> {party_name}", styles['Normal']))
    story.append(Spacer(1, 6))
    story.append(Paragraph(f"<b>Statement Date:</b> {datetime.now().strftime('%B %d, %Y')}", styles['Normal']))
    story.append(Spacer(1, 20))

    story.append(Paragraph("<b>Sales Detail</b>", styles['Heading2']))
    story.append(Spacer(1, 12))

    table_data = [['Category', 'Lifetime Qty', 'RTD', 'Units', 'Returns', 'Net Units', 'Price', 'Rate %', 'Disc', 'Net', 'Value', 'Royalty' ]]
    for cat in categories:
        row = [
            cat.get('category', cat.get('Category', '')),
            cat.get('lifetime_quantity', cat.get('Lifetime Quantity', '')),
            cat.get('returns_to_date', cat.get('Returns to Date', '')),
            cat.get('units', cat.get('Units', '')),
            cat.get('returns', cat.get('Returns', '')),
            cat.get('net_units', cat.get('Net Units', '')),
            cat.get('unit_price', cat.get('Unit Price', '')),
            cat.get('royalty_rate_percent', cat.get('Royalty Rate (%)', '')),
            cat.get('discount', cat.get('Discount', '')),
            cat.get('net_revenue', cat.get('Net Revenue', '')),
            cat.get('value', cat.get('Value', '')),
            cat.get('royalty', cat.get('Royalty', ''))
        ]
        table_data.append(row)

    from marble_app.reportlab.platypus import Table, TableStyle
    from marble_app.reportlab.lib import colors
    table = Table(table_data, colWidths=[0.8*inch, 0.6*inch, 0.6*inch, 0.5*inch, 0.5*inch, 0.6*inch, 0.6*inch, 0.5*inch, 0.45*inch, 0.6*inch, 0.6*inch, 0.6*inch])
    table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), colors.darkblue),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
        ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, 0), 8),
        ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
        ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
        ('FONTSIZE', (0, 1), (-1, -1), 7),
        ('GRID', (0, 0), (-1, -1), 1, colors.black)
    ]))
    story.append(table)
    story.append(Spacer(1, 20))

    story.append(Paragraph("<b>Financial Summary</b>", styles['Heading2']))
    story.append(Spacer(1, 12))
    summary_data = [
        ['Advance Paid:', f"${advance_val:,.2f}"],
        ['Royalty for Period:', f"${royalty_val:,.2f}"],
        ['Last Period Balance:', f"${last_bal:,.2f}"],
        ['Current Balance:', f"${balance_val:,.2f}"],
        ['Amount Payable:', f"${payable_val:,.2f}"]
    ]
    summary_table = Table(summary_data, colWidths=[2*inch, 1.4*inch])
    summary_table.setStyle(TableStyle([
        ('ALIGN', (0, 0), (0, -1), 'LEFT'),
        ('ALIGN', (1, 0), (1, -1), 'RIGHT'),
        ('FONTNAME', (0, 0), (-1, -1), 'Helvetica'),
        ('FONTSIZE', (0, 0), (-1, -1), 10),
        ('FONTNAME', (0, 4), (1, 4), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 4), (1, 4), 12),
        ('TEXTCOLOR', (1, 4), (1, 4), colors.green),
        ('LINEABOVE', (0, 4), (-1, 4), 2, colors.black),
        ('TOPPADDING', (0, 4), (-1, 4), 12),
    ]))
    story.append(summary_table)
    story.append(Spacer(1, 20))

    footer_style = ParagraphStyle('Footer', parent=styles['Normal'], fontSize=8, textColor=colors.grey, alignment=TA_CENTER)
    story.append(Paragraph("This statement is generated for informational purposes.", footer_style))
    story.append(Paragraph(f"Generated: {datetime.now().strftime('%B %d, %Y at %I:%M %p')}", footer_style))

    doc.build(story)
    pdf_bytes = buffer.getvalue()
    buffer.close()
    return pdf_bytes

def rasterize_pdf_to_images_and_wrap(pdf_bytes: bytes, dpi: int = 300) -> bytes | None:
    """
    Convert each page to a PNG at the given DPI and rebuild a simple image-only PDF.
    This guarantees printer compatibility at the cost of larger files.
    """
    try:
        gs = find_gs_exe()
        if not gs:
            print("[pdf] rasterize: Ghostscript not found")
            return None

        from marble_app.io import BytesIO
        from marble_app.reportlab.pdfgen import canvas
        from marble_app.reportlab.lib.pagesizes import letter
        from marble_app.reportlab.lib.utils import ImageReader

        with tempfile.TemporaryDirectory() as td:
            in_pdf = os.path.join(td, "in.pdf")
            Path(in_pdf).write_bytes(pdf_bytes)

            # Render each page to PNG (RGB, no alpha)
            # page images: page-0001.png, page-0002.png, ...
            png_pattern = os.path.join(td, "page-%04d.png")
            args = [
                gs, "-dBATCH", "-dNOPAUSE", "-dSAFER",
                "-sDEVICE=png16m",           # 24-bit RGB PNG (no alpha)
                f"-r{dpi}",                  # DPI
                "-dUseCropBox",              # respect crop box for page bounds
                f"-sOutputFile={png_pattern}",
                in_pdf,
            ]
            subprocess.run(args, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

            # Collect generated images in order
            png_files = sorted(Path(td).glob("page-*.png"))
            if not png_files:
                print("[pdf] rasterize: no PNGs were produced")
                return None

            # Build a new PDF (Letter pages) with each PNG scaled to fit
            buf = BytesIO()
            page_width, page_height = letter  # 612 x 792 pts @ 72dpi
            c = canvas.Canvas(buf, pagesize=letter)

            for p in png_files:
                img = ImageReader(p.open("rb"))
                iw, ih = img.getSize()

                # Pixel dimensions from marble_app.Ghostscript; convert to points at 72 dpi:
                # If the PNG is at 'dpi', its size in points is (px / dpi * 72).
                img_width_pts  = iw / dpi * 72.0
                img_height_pts = ih / dpi * 72.0

                # Scale to "contain" within Letter while keeping aspect
                scale = min(page_width / img_width_pts, page_height / img_height_pts)
                draw_w = img_width_pts * scale
                draw_h = img_height_pts * scale
                x = (page_width  - draw_w) / 2.0
                y = (page_height - draw_h) / 2.0

                c.drawImage(img, x, y, width=draw_w, height=draw_h)
                c.showPage()

            c.save()
            out = buf.getvalue()
            buf.close()
            return out if out and len(out) > 0 else None

    except Exception as e:
        print(f"[pdf] rasterize: failed with {e}")
        return None
    
def distill_via_postscript(pdf_bytes: bytes) -> bytes | None:
    """
    Convert PDF -> PostScript (ps2write) -> PDF (pdfwrite).
    This flattens transparency/blends into simple operators that Acrobat printers handle.
    """
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

            # Step 1: PDF -> PostScript
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

            # Step 2: PostScript -> PDF
            args_pdf = [
                gs, "-dBATCH", "-dNOPAUSE", "-dSAFER",
                "-sDEVICE=pdfwrite",
                "-dCompatibilityLevel=1.3",  # transparency-free target
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
    """
    Rasterize each page via PyMuPDF (no Ghostscript). Wrap back into a simple PDF.
    pip install pymupdf
    """
    try:
        import fitz  # PyMuPDF
        from marble_app.io import BytesIO
        from marble_app.reportlab.pdfgen import canvas
        from marble_app.reportlab.lib.pagesizes import letter
        from marble_app.reportlab.lib.utils import ImageReader

        doc = fitz.open(stream=pdf_bytes, filetype="pdf")
        if len(doc) == 0:
            return None

        png_bytes_list = []
        zoom = dpi / 72.0
        mat = fitz.Matrix(zoom, zoom)
        for page in doc:
            pix = page.get_pixmap(matrix=mat, alpha=False)  # RGB
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
    """
    HTML -> PDF using WeasyPrint (Cairo/Pango). Very printer-friendly.
    """
    from marble_app.weasyprint import HTML, CSS

    html_content = generate_statement_html(book, request, party_data, party, target="pdf")

    # Ensure a plain white page background and explicit page size in case CSS misses it
    css = CSS(string="""
        @page { size: Letter; margin: 0.7in; }
        html, body { background: #ffffff !important; -webkit-print-color-adjust: exact; print-color-adjust: exact; }
        /* Optional: force fully opaque colors (avoid transparency artifacts) */
        * { background-clip: border-box; }
    """)

    # Base URL is important if your HTML ever references relative assets (fonts/images)
    # In your case you embed the logo as base64, so it's fine; still harmless to set:
    base_url = str(Path.cwd())

    pdf = HTML(string=html_content, base_url=base_url).write_pdf(stylesheets=[css])
    return pdf

# =============================
#          HTML VIEW
# =============================

def generate_statement_html(book: Book, request: RoyaltyStatementRequest, party_data: Dict, party: str, *, target: str = "screen") -> str:
    """
    Generate HTML for royalty statement.
    Uses CSS variables + data-target to present a comfy screen view and a compact PDF view.
    """
    # ---------- Helpers ----------
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

    # ---------- Logo ----------
    logo_base64 = ""
    if LOGO_PATH.exists():
        try:
            logo_bytes = LOGO_PATH.read_bytes()
            logo_base64 = base64.b64encode(logo_bytes).decode('utf-8')
        except Exception as e:
            print(f"[render] Could not load logo: {e}")

    # ---------- Data ----------
    categories = party_data.get('categories', []) or []

    # party display name
    party_name = book.author if party == 'author' else (
        book.illustrator.name if getattr(book, "illustrator", None) and hasattr(book.illustrator, 'name')
        else str(getattr(book, "illustrator", None) or 'Illustrator')
    )

    # Agency block (author agent or illustrator agent)
    agency_html = ""
    if party == 'author' and getattr(book, 'author_agent', None):
        agent = book.author_agent
        agency_name = getattr(agent, 'agency', '') or getattr(agent, 'name', '')
        if agency_name:
            agency_html = f"<div style='margin-bottom:5px;font-weight:600;'>{agency_name}</div>"
        addr = getattr(agent, 'address', None)
        if isinstance(addr, dict):
            street = addr.get('street', '') or ''
            city = addr.get('city', '') or ''
            state = addr.get('state', '') or ''
            zip_code = addr.get('zip', '') or ''
            if street:
                agency_html += f"<div>{street}</div>"
            if city or state or zip_code:
                line = " ".join(p for p in [city, state] if p)
                agency_html += f"<div>{line}{'&nbsp;&nbsp;' + zip_code if zip_code else ''}</div>"
        elif isinstance(addr, str) and addr:
            agency_html += f"<div>{addr}</div>"
    elif party == 'illustrator' and getattr(book, 'illustrator', None) and getattr(book.illustrator, 'agent', None):
        agent = book.illustrator.agent
        agency_name = getattr(agent, 'agency', '') or getattr(agent, 'name', '')
        if agency_name:
            agency_html = f"<div style='margin-bottom:5px;font-weight:600;'>{agency_name}</div>"
        addr = getattr(agent, 'address', None)
        if isinstance(addr, dict):
            street = addr.get('street', '')
            city = addr.get('city', '')
            state = addr.get('state', '')
            zip_code = addr.get('zip', '')
            if street:
                agency_html += f"<div>{street}</div>"
            if city or state or zip_code:
                agency_html += f"<div>{' '.join(filter(None, [city, state, zip_code]))}</div>"
        elif isinstance(addr, str) and addr:
            agency_html += f"<div>{addr}</div>"

    # ISBNs from marble_app.book.formats
    isbn_html = "N/A"
    if getattr(book, 'formats', None):
        items = []
        for fmt in book.formats:
            fmt_dict = (
                fmt.dict() if hasattr(fmt, 'dict')
                else (fmt.model_dump() if hasattr(fmt, 'model_dump') else (fmt if isinstance(fmt, dict) else {}))
            )
            isbn = fmt_dict.get('isbn') or fmt_dict.get('ISBN')
            format_name = fmt_dict.get('format') or fmt_dict.get('Format') or 'Unknown'
            if isbn and str(isbn).strip():
                items.append(f"{format_name}: {str(isbn).strip()}")
        if items:
            isbn_html = "".join(f"<div class='isbn-line'>{x}</div>" for x in items)

    # Title + subtitle
    full_title = (book.title or '')
    if getattr(book, 'subtitle', None):
        full_title += f": {book.subtitle}"

    # ---------- Extract Summary Values ----------
    advance_val = to_money(party_data.get('advance', 0))
    royalty_val = to_money(party_data.get('royalty_total', 0))
    last_bal = to_money(party_data.get('last_balance', 0))
    balance_val = to_money(party_data.get('balance', 0))
    payable_val = to_money(party_data.get('payable', 0))

    agency_box_html = agency_html + (
        f"<div class='info-row' style='margin-top:10px'>"
        f"<span class='label'>Statement For: </span>"
        f"<span style='font-weight:600;'>{party_name}</span>"
        f"</div>"
    )

    # ---------- HTML ----------
    html = f"""<!DOCTYPE html>
<html data-target="{ 'pdf' if target == 'pdf' else 'screen' }">
<head>
<meta charset="UTF-8">
<title>Royalty Statement - {party_name}</title>
<style>
    /* Base: screen vs pdf via data-target */
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
        padding-bottom: 0.7in;  /* keep clear of any footer */
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
    </div> <!-- /.page-content -->

    <div class="footer">
        <p>This statement is generated for informational purposes.</p>
        <p>Generated: {datetime.now().strftime('%B %d, %Y at %I:%M %p')}</p>
    </div>
</body>
</html>
"""
    return html
