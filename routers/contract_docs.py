# marble_app/routers/contract_docs.py
from __future__ import annotations
from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel
from typing import Dict, Any, Optional, List
from pathlib import Path
from datetime import datetime
import os, json, re, uuid, hashlib

try:
    from docx import Document  # pip install python-docx
    from docx.shared import RGBColor
except Exception as e:
    raise RuntimeError("python-docx not installed. pip install python-docx") from e

router = APIRouter(prefix="/contracts", tags=["contracts"])

# ================== PATHS (aligned with main.py; flat files only) ==================
BASE_DATA_DIR = Path.home() / "Documents" / "marble_app" / "data"
TEMPLATES_DIR = BASE_DATA_DIR / "Templates"           # template .docx files live directly here
DRAFTS_DIR    = BASE_DATA_DIR / "TempDraftContracts"  # generated DOCX drafts here
INDEX_PATH    = DRAFTS_DIR / "index.json"

TEMPLATES_DIR.mkdir(parents=True, exist_ok=True)
DRAFTS_DIR.mkdir(parents=True, exist_ok=True)
if not INDEX_PATH.exists():
    INDEX_PATH.write_text("[]", encoding="utf-8")

# {{Token}} pattern (e.g., {{Book_Title}})
TOKEN_RE = re.compile(r"\{\{([A-Za-z0-9_]+)\}\}")

class GenerateRequest(BaseModel):
    dealMemo: Dict[str, Any]
    templateId: str                    # id prefix OR a full filename ending with .docx
    mapping: Optional[Dict[str, str]] = None  # optional explicit token->memoKey mapping

# Subrights tokens we may want to delete whole paragraphs for if missing
SUBRIGHT_TOKENS = [
    "Sub_BookClub",
    "Sub_FirstSerial_Illustrated",
    "Sub_FirstSerial_Text",
    "Sub_SecondSerial",
    "Sub_Audio_Physical",
    "Sub_Audio_Digital",
    "Sub_UK",
    "Sub_Canada",
    "Sub_Export",
    "Sub_ForeignTranslation",
    "Sub_TV_Movie",
    "Sub_Animation_Digital",
    "Sub_MassMerch",
]

def _load_index() -> List[dict]:
    try:
        return json.loads(INDEX_PATH.read_text(encoding="utf-8"))
    except Exception:
        return []

def _save_index(items: List[dict]) -> None:
    INDEX_PATH.write_text(json.dumps(items, ensure_ascii=False, indent=2), encoding="utf-8")

def _find_template_path(template_id: str) -> Path:
    """
    Flat-file lookup ONLY:
      - If template_id ends with '.docx': use Templates/<template_id> directly.
      - Else try Templates/<template_id>_*.docx (first match).
      - Else try Templates/<template_id>.docx
    """
    if template_id.lower().endswith(".docx"):
        p = TEMPLATES_DIR / template_id
        if p.exists():
            return p
        raise HTTPException(status_code=404, detail=f"Template file not found: {p}")

    matches = sorted(TEMPLATES_DIR.glob(f"{template_id}_*.docx"))
    if matches:
        return matches[0]

    fallback = TEMPLATES_DIR / f"{template_id}.docx"
    if fallback.exists():
        return fallback

    raise HTTPException(
        status_code=404,
        detail=(
            "Template not found.\n"
            f"Looked for:\n"
            f" - {TEMPLATES_DIR}\\{template_id}_*.docx\n"
            f" - {TEMPLATES_DIR}\\{template_id}.docx"
        ),
    )

def _get_value_from_memo(memo: dict, dotted: str) -> str:
    cur = memo
    for part in dotted.split("."):
        if isinstance(cur, dict) and part in cur:
            cur = cur[part]
        else:
            return ""
    return "" if cur is None else str(cur)

def _default_mapping(role: str) -> dict[str, str]:
    """
    Build the default token -> memoKey mapping, switching between
    author vs illustrator depending on contributorRole.

    NOTE:
    - We always use Author_* tokens in the template, but they may be
      filled from author_* or illustrator_* fields based on role.
    """
    is_illustrator = (role == "illustrator")
    person_key = "author" if not is_illustrator else "illustrator.name"
    addr_prefix = "illustrator_address" if is_illustrator else "author_address"

    return {
        # PERSON + ADDRESS
        "Author_Name":           person_key,
        "Author_Street_Address": f"{addr_prefix}.street",
        "Author_City":           f"{addr_prefix}.city",
        "Author_State":          f"{addr_prefix}.state",
        "Author_Zip":            f"{addr_prefix}.zip",

        # Contact
        "Author_Email":          "author_email",
        "Author_Phone":          "author_phone_number",

        # BOOK / DEAL
        "Date":                  "effectiveDate",
        "Book_Title":            "title",
        "Book_Description":      "shortDescription",
        "Option_Clause":         "optionClause",
        "Projected_Publication": "projectedPublicationDate",
        "Territory":             "territoriesRights",
        "Advance_Installments":  "advanceSchedule",
    }

def _populate_royalty_tokens(memo: dict, values: dict[str, str]) -> bool:
    """
    Fill derived royalty tokens (Hardcover_1, Paperback_1, Boardbook_*, Ebook, etc.)
    into values from the nested royalties JSON, without changing the JSON structure.

    Returns:
        has_boardbook (bool): True if a Board Book royalty format exists.
    """
    def _fmt(v) -> str:
        return "" if v is None else str(v)

    royalties = (memo.get("royalties") or {}).get("author") or {}
    first_rights = royalties.get("first_rights") or []

    def _normalize_fmt_name(fmt: str) -> str:
        return fmt.lower().replace("-", "").replace(" ", "")

    def _find_format(fmt: str):
        target = _normalize_fmt_name(fmt)
        for r in first_rights:
            name = _normalize_fmt_name(str(r.get("format") or ""))
            if name == target:
                return r
        return None

    has_boardbook = False

    def _fill_format(prefix: str, fmt_key: str) -> bool:
        nonlocal has_boardbook

        fr = _find_format(fmt_key)
        if not fr:
            return False

        tiers = fr.get("tiers") or []
        if not tiers:
            return False

        # Tier 1: rate + copy limit
        t1 = tiers[0]
        values[f"{prefix}_1"] = _fmt(t1.get("rate_percent"))

        copy_limit = None
        for cond in t1.get("conditions") or []:
            if cond.get("kind") == "units" and cond.get("value") is not None:
                copy_limit = cond["value"]
                break
        if copy_limit is not None:
            values[f"{prefix}_Copy_Limit_1"] = _fmt(copy_limit)

        # Tier 2–4: subsequent tiers, if present
        if len(tiers) > 1:
            values[f"{prefix}_2"] = _fmt(tiers[1].get("rate_percent"))
        if len(tiers) > 2:
            values[f"{prefix}_3"] = _fmt(tiers[2].get("rate_percent"))
        if len(tiers) > 3:
            values[f"{prefix}_4"] = _fmt(tiers[3].get("rate_percent"))

        if prefix.lower().startswith("boardbook"):
            has_boardbook = True

        return True

    # Hardcover / Paperback / Board Book
    _fill_format("Hardcover", "Hardcover")
    _fill_format("Paperback", "Paperback")
    _fill_format("Boardbook", "Board Book")

    # Ebook: flat rate percent from the E-book entry
    ebook = _find_format("E-book") or _find_format("Ebook")
    if ebook:
        values["Ebook"] = _fmt(ebook.get("flat_rate_percent"))

    return has_boardbook

def _populate_subrights(memo: dict, values: dict[str, str]) -> None:
    """
    Fill Sub_* tokens from memo['royalties']['author']['subrights'].

    JSON shape (inside memo):
      "royalties": {
        "author": {
          ...
          "subrights": [
            { "name": "...", "percent": 25 },
            { "name": "...", "variants": { "text_only": 90, "text_and_art": 45 } },
            ...
          ]
        }
      }
    """
    royalties = (memo.get("royalties") or {}).get("author") or {}
    subrights = royalties.get("subrights") or []
    if not subrights:
        # Still enforce our Canada / Export default below.
        values["Sub_Canada"] = "2/3"
        values["Sub_Export"] = "2/3"
        return

    def norm(s: str) -> str:
        return re.sub(r"[^a-z0-9]+", " ", (s or "").lower()).strip()

    def find_sub(fragment: str):
        frag = fragment.lower()
        for item in subrights:
            if frag in norm(item.get("name", "")):
                return item
        return None

    def set_percent(token: str, item: dict | None, variant_key: str | None = None):
        if not item:
            return
        if variant_key:
            v = item.get("variants") or {}
            if variant_key in v and v[variant_key] is not None:
                values[token] = str(v[variant_key])
        else:
            p = item.get("percent")
            if p is not None:
                values[token] = str(p)

    # a. Hardcover, paperback, and large-type reprint editions
    set_percent("Sub_HC_PB_LargeType", find_sub("hardcover paperback"))

    # b. Anthologies / textbooks / digests
    set_percent("Sub_Anthologies", find_sub("anthologies"))

    # c. Book club publication
    set_percent("Sub_BookClub", find_sub("book club"))

    # d. First serial publication: text+art / text only
    first_serial = find_sub("first serial")
    set_percent("Sub_FirstSerial_Text", first_serial, variant_key="text_only")
    set_percent("Sub_FirstSerial_Illustrated", first_serial, variant_key="text_and_art")

    # e. Second serial publication – choose text_only as the single contract %.
    second_serial = find_sub("second serial")
    set_percent("Sub_SecondSerial", second_serial, variant_key="text_only")

    # f. Audiobooks: physical / digital
    audio = find_sub("audiobook")
    if not audio:
        audio = find_sub("audiobooks")
    set_percent("Sub_Audio_Physical", audio, variant_key="physical")
    set_percent("Sub_Audio_Digital", audio, variant_key="digital")

    # g. UK – from JSON
    set_percent("Sub_UK", find_sub("uk"))

    # h. Canada – ALWAYS 2/3 of U.S. royalty rate, ignore JSON percent
    values["Sub_Canada"] = "2/3"

    # i. Export rights – ALWAYS 2/3 of prevailing U.S. rate, ignore JSON percent
    values["Sub_Export"] = "2/3"

    # j. Foreign translation
    set_percent("Sub_ForeignTranslation", find_sub("foreign translation"))

    # k, l, m (TV/Movie, Animation, MassMerch) are not in your JSON yet.
    # We intentionally do NOT set:
    #   Sub_TV_Movie, Sub_Animation_Digital, Sub_MassMerch
    # so paragraphs that contain only those will be treated as "not in Deal Memo"
    # by the deletion logic and removed.

def _find_draft(item_id: str) -> dict | None:
    for it in _load_index():
        if str(it.get("id")) == str(item_id):
            return it
    return None

def _public_base_url() -> str:
    """
    Base URL that the OnlyOffice DocumentServer (running in Docker) can reach.
    If not set, default to http://host.docker.internal:8000 (works on Docker for Windows/Mac).
    """
    return (os.getenv("PUBLIC_BASE_URL") or "http://host.docker.internal:8000").rstrip("/")

# ------------------------ OnlyOffice config for drafts ------------------------
@router.get("/draft-contracts/{item_id}/onlyoffice-config")
def onlyoffice_config_for_draft(item_id: str, request: Request):
    it = _find_draft(item_id)
    if not it:
        raise HTTPException(status_code=404, detail="Draft not found")

    draft_path = Path(it.get("path", ""))
    if not draft_path.exists():
        raise HTTPException(status_code=404, detail="Draft file not found on disk")

    public_base = _public_base_url()  # e.g., http://host.docker.internal:8000
    file_url    = f"{public_base}/api/contracts/draft-contracts/{item_id}/file"
    callback_url = f"{public_base}/api/contracts/draft-contracts/{item_id}/callback"

    # "key" must change when file changes; use mtime+size
    stat = draft_path.stat()
    key_src = f"{draft_path.name}-{stat.st_mtime_ns}-{stat.st_size}"
    doc_key = hashlib.sha256(key_src.encode("utf-8")).hexdigest()[:32]

    cfg = {
        "documentType": "word",
        "type": "desktop",
        "document": {
            "title": it.get("filename") or draft_path.name,
            "fileType": "docx",
            "key": doc_key,
            "url": file_url,
            "permissions": {
                "edit": True,
                "download": False,
                "print": True,
                "copy": True,
            },
        },
        "editorConfig": {
            "lang": "en",
            "mode": "edit",                 # set to "view" for read-only
            "callbackUrl": callback_url,    # sink for save/forcesave events
            "customization": {
                # modern API (no deprecated 'goback')
                "close": { "visible": True, "label": "Close" },
                "autosave": True,
            },
        },
    }
    return JSONResponse(cfg)

@router.post("/draft-contracts/{item_id}/callback")
async def onlyoffice_callback_sink(item_id: str, request: Request):
    """
    Minimal callback sink. If you later want to implement ForceSave,
    parse OnlyOffice JSON body and persist the posted file.
    """
    # body = await request.json()
    # print("ONLYOFFICE CALLBACK:", body)
    return {"status": "ok"}

# ----------------------------- Generate a draft ------------------------------
@router.post("/generate")
def generate_contract(req: GenerateRequest):
    memo = req.dealMemo or {}
    role = str(memo.get("contributorRole") or "author").lower()

    mapping = _default_mapping("illustrator" if role == "illustrator" else "author")
    if isinstance(req.mapping, dict):
        mapping.update({k: v for k, v in req.mapping.items() if isinstance(k, str) and isinstance(v, str)})

    template_path = _find_template_path(req.templateId)
    doc = Document(str(template_path))

    values: dict[str, str] = {}
    for token, key in mapping.items():
        if key == "advanceSchedule" and isinstance(memo.get("advanceSchedule"), list):
            parts = []
            for row in memo["advanceSchedule"]:
                amt = row.get("value")
                typ = row.get("amountType")
                trig = row.get("trigger") or ""
                parts.append(
                    f"{amt}% {trig}".strip()
                    if typ == "percent"
                    else f"${amt} {trig}".strip()
                )
            values[token] = "; ".join(parts)
        else:
            values[token] = _get_value_from_memo(memo, key)

    # Add derived royalty tokens like Hardcover_1, Paperback_1, Boardbook_*, Ebook, etc.
    has_boardbook = _populate_royalty_tokens(memo, values)

    # Add Sub_* tokens (subrights) from royalties.author.subrights,
    # with Sub_Canada and Sub_Export forced to "2/3".
    _populate_subrights(memo, values)

    def replace_in_paragraph(p):
        if not p.runs:
            return

        # Merge runs to catch tokens that span runs
        original_text = "".join(r.text for r in p.runs)

        # 1) Board Book: if no Board Book royalties in JSON, and this paragraph
        #    contains any Boardbook_* token, delete the whole paragraph.
        if (not has_boardbook) and ("Boardbook_" in original_text):
            element = p._element
            parent = element.getparent()
            if parent is not None:
                parent.remove(element)
            return

        # 2) Subrights: if this paragraph contains any Sub_* token AND
        #    ANY of those tokens has no value in `values`, delete the whole paragraph
        if any(f"{{{{{tok}}}}}" in original_text for tok in SUBRIGHT_TOKENS):
            for tok in SUBRIGHT_TOKENS:
                if f"{{{{{tok}}}}}" in original_text:
                    val = values.get(tok)
                    # 0 or "0" is allowed; only delete if truly missing/empty
                    if val is None or str(val).strip() == "":
                        element = p._element
                        parent = element.getparent()
                        if parent is not None:
                            parent.remove(element)
                        return
            # if we didn't early-return, all tokens present here have values → continue

        had_token = False
        START = "\uE000"  # marker for start of inserted value
        END   = "\uE001"  # marker for end of inserted value

        def repl(m):
            nonlocal had_token
            had_token = True
            tok = m.group(1)  # token name without braces
            val = values.get(tok, "") or ""
            # Wrap the value in markers so we can style it later
            return f"{START}{val}{END}"

        # Replace {{Token}} → START + value + END
        new_text = TOKEN_RE.sub(repl, original_text)

        # No tokens? Leave paragraph unchanged
        if not had_token:
            return

        # Clear existing runs' text
        p.runs[0].text = ""
        for r in p.runs[1:]:
            r.text = ""

        # Rebuild runs: normal text = normal; marked text = red
        buf = []
        in_token = False

        def flush_normal():
            nonlocal buf
            if buf:
                p.add_run("".join(buf))
                buf = []

        def flush_token():
            nonlocal buf
            if buf:
                run = p.add_run("".join(buf))
                run.font.color.rgb = RGBColor(0xFF, 0x00, 0x00)
                buf = []

        for ch in new_text:
            if ch == START:
                flush_normal()
                in_token = True
            elif ch == END:
                flush_token()
                in_token = False
            else:
                buf.append(ch)

        # trailing text
        if buf:
            if in_token:
                flush_token()
            else:
                flush_normal()

    # Use list(...) so we can safely remove paragraphs during iteration
    for p in list(doc.paragraphs):
        replace_in_paragraph(p)
    for tbl in doc.tables:
        for row in tbl.rows:
            for cell in row.cells:
                for p in list(cell.paragraphs):
                    replace_in_paragraph(p)

    uid = str(memo.get("uid") or uuid.uuid4().hex[:12])
    safe_title = (str(memo.get("title") or memo.get("name") or "Contract").strip() or "Contract").replace(" ", "_")
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_name = f"{safe_title}_{uid}_{stamp}.docx"
    out_path = DRAFTS_DIR / out_name
    doc.save(str(out_path))

    items = _load_index()
    item = {
        "id": uuid.uuid4().hex[:12],
        "uid": uid,
        "title": safe_title.replace("_", " "),
        "filename": out_name,
        "path": str(out_path),
        "templateId": req.templateId,
        "createdAt": datetime.now().isoformat(),
    }
    items.insert(0, item)
    _save_index(items)

    return {"file": item}

# -------------------------- Drafts listing & file ----------------------------
@router.get("/draft-contracts")
def list_draft_contracts():
    return {"items": _load_index()}

@router.get("/draft-contracts/{item_id}/file")
def get_draft_contract_file(item_id: str):
    it = _find_draft(item_id)
    if not it:
        raise HTTPException(status_code=404, detail="Draft not found")

    p = Path(it.get("path", ""))
    if not p.exists():
        raise HTTPException(status_code=404, detail="Draft file not found on disk")

    return FileResponse(
        str(p),
        filename=p.name,
        media_type="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
    )
