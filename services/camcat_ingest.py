# marble_app/services/camcat_ingest.py
from __future__ import annotations

import json
import re
import shutil
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from xml.etree import ElementTree as ET

# -----------------------------
# Utilities
# -----------------------------
_WS = re.compile(r"\s+")
_ISBN = re.compile(r"^\d{10}(\d{3})?$")


def clean(s: Any) -> str:
    return _WS.sub(" ", (s or "").strip())


def norm_key(s: Any) -> str:
    return clean(s).lower()


def safe_float(v: Any) -> float:
    try:
        return float(v)
    except Exception:
        return 0.0


def safe_int(v: Any) -> int:
    try:
        return int(float(v))
    except Exception:
        return 0


def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def onix_ns(root: ET.Element) -> Dict[str, str]:
    # root.tag: "{namespace}ONIXMessage"
    if root.tag.startswith("{") and "}" in root.tag:
        ns = root.tag.split("}")[0].strip("{")
        return {"onix": ns}
    return {"onix": ""}


def findtext(el: Optional[ET.Element], path: str, ns: Dict[str, str], default: str = "") -> str:
    if el is None:
        return default
    t = el.findtext(path, default=default, namespaces=ns)
    return clean(t)


def first(el: Optional[ET.Element], path: str, ns: Dict[str, str]) -> Optional[ET.Element]:
    if el is None:
        return None
    return el.find(path, ns)


def all_(el: Optional[ET.Element], path: str, ns: Dict[str, str]) -> List[ET.Element]:
    if el is None:
        return []
    return el.findall(path, ns)


# -----------------------------
# ONIX extraction
# -----------------------------
def extract_isbn13(prod: ET.Element, ns: Dict[str, str]) -> str:
    # ProductIdentifier / ProductIDType=15 is ISBN-13
    for pid in all_(prod, ".//onix:ProductIdentifier", ns):
        pid_type = findtext(pid, "onix:ProductIDType", ns)
        val = findtext(pid, "onix:IDValue", ns)
        if pid_type == "15" and val and _ISBN.match(val):
            return val
    # fallback: any IDValue that looks like ISBN
    for pid in all_(prod, ".//onix:ProductIdentifier", ns):
        val = findtext(pid, "onix:IDValue", ns)
        if val and _ISBN.match(val):
            return val
    return ""


def extract_title(prod: ET.Element, ns: Dict[str, str]) -> str:
    t = findtext(prod, ".//onix:TitleWithoutPrefix", ns)
    if t:
        return t
    return findtext(prod, ".//onix:TitleText", ns)


def extract_author(prod: ET.Element, ns: Dict[str, str]) -> str:
    # contributor role A01 = Author
    for c in all_(prod, ".//onix:Contributor", ns):
        role = findtext(c, "onix:ContributorRole", ns)
        if role == "A01":
            name = findtext(c, "onix:PersonName", ns) or clean(
                findtext(c, "onix:NamesBeforeKey", ns) + " " + findtext(c, "onix:KeyNames", ns)
            )
            if name:
                return name
    # fallback: first contributor name
    c0 = first(prod, ".//onix:Contributor", ns)
    if c0 is not None:
        return findtext(c0, "onix:PersonName", ns) or clean(
            findtext(c0, "onix:NamesBeforeKey", ns) + " " + findtext(c0, "onix:KeyNames", ns)
        )
    return ""


def extract_publishing_year(prod: ET.Element, ns: Dict[str, str]) -> int:
    for pd in all_(prod, ".//onix:PublishingDate", ns):
        date = findtext(pd, "onix:Date", ns)
        if len(date) >= 4 and date[:4].isdigit():
            return int(date[:4])
    return 0


def extract_pub_date(prod: ET.Element, ns: Dict[str, str]) -> str:
    for pd in all_(prod, ".//onix:PublishingDate", ns):
        date = findtext(pd, "onix:Date", ns)
        if len(date) == 8 and date.isdigit():
            return f"{date[0:4]}-{date[4:6]}-{date[6:8]}"
        if len(date) >= 10 and date[0:4].isdigit():
            return date[:10]
    return ""


def extract_prices(prod: ET.Element, ns: Dict[str, str]) -> Tuple[float, float]:
    usd = 0.0
    cad = 0.0
    for pr in all_(prod, ".//onix:SupplyDetail//onix:Price", ns):
        amt = safe_float(findtext(pr, "onix:PriceAmount", ns))
        cur = findtext(pr, "onix:CurrencyCode", ns)
        if cur == "USD" and amt:
            usd = usd or amt
        elif cur == "CAD" and amt:
            cad = cad or amt
    return usd, cad


def extract_dimensions(prod: ET.Element, ns: Dict[str, str]) -> Dict[str, Any]:
    pages = 0
    for ep in all_(prod, ".//onix:Extent", ns):
        etype = findtext(ep, "onix:ExtentType", ns)
        if etype == "11":  # pages
            pages = safe_int(findtext(ep, "onix:ExtentValue", ns))
            break
    return {
        "pages": pages,
        "tall": 0,
        "wide": 0,
        "spine": 0,
        "weight": 0,
        "loc_number": "",
    }


def extract_format_name(prod: ET.Element, ns: Dict[str, str]) -> str:
    pf = findtext(prod, ".//onix:ProductForm", ns)

    if pf in {"BB", "BC", "BD"}:
        pfd = findtext(prod, ".//onix:ProductFormDetail", ns)
        if pfd in {"B204", "B206"}:
            return "Hardcover"
        return "Paperback"

    if pf in {"BH"}:
        return "Board Book"

    if pf in {"DG", "EA", "EB", "EC", "ED"}:
        return "E-book"

    if pf in {"AA", "AB"}:
        return "Audiobook"

    pdt = findtext(prod, ".//onix:ProductContentType", ns)
    if pdt in {"10", "11"}:
        return "E-book"

    return "Other"


def extract_cover_link(prod: ET.Element, ns: Dict[str, str]) -> str:
    for sr in all_(prod, ".//onix:SupportingResource", ns):
        rtype = findtext(sr, "onix:ResourceContentType", ns)
        mode = findtext(sr, "onix:ResourceMode", ns)
        link = findtext(sr, ".//onix:ResourceLink", ns)
        if rtype == "01" and mode == "03" and link:
            return link
    return ""


# -----------------------------
# Grouping helpers
# -----------------------------
def grouping_key(title: str, author: str) -> str:
    return f"{norm_key(title)}||{norm_key(author)}"


def pick_primary_isbn(formats: List[Dict[str, Any]]) -> str:
    def isbn_of(fmt: Dict[str, Any]) -> str:
        return clean(fmt.get("isbn", ""))

    pb = next((isbn_of(f) for f in formats if f.get("format") == "Paperback" and isbn_of(f)), "")
    if pb:
        return pb
    hc = next((isbn_of(f) for f in formats if f.get("format") == "Hardcover" and isbn_of(f)), "")
    if hc:
        return hc
    eb = next((isbn_of(f) for f in formats if f.get("format") == "E-book" and isbn_of(f)), "")
    if eb:
        return eb
    any_ = next((isbn_of(f) for f in formats if isbn_of(f)), "")
    return any_


def stable_format_uid(book_uid: str, fmt: Dict[str, Any]) -> str:
    name = f"{fmt.get('format','')}|{clean(fmt.get('isbn',''))}"
    return str(uuid.uuid5(uuid.UUID(book_uid), name))


def find_cover_file_for_isbn(covers_dir: Optional[Path], isbn: str) -> Optional[Path]:
    if not isbn:
        return None
    if not covers_dir:
        return None
    if not covers_dir.exists():
        return None
    for ext in (".jpg", ".jpeg", ".png", ".webp"):
        p = covers_dir / f"{isbn}{ext}"
        if p.exists():
            return p
    hits = list(covers_dir.glob(f"*{isbn}*.*"))
    return hits[0] if hits else None


def write_cover_to_uploads(
    uploads_root: Path,
    book_uid: str,
    covers_dir: Optional[Path],
    isbn_for_cover: str,
) -> Optional[str]:
    src = find_cover_file_for_isbn(covers_dir, isbn_for_cover)
    if not src:
        return None
    dest_dir = uploads_root / book_uid
    ensure_dir(dest_dir)
    dest = dest_dir / f"{book_uid}__cover.jpg"
    shutil.copyfile(src, dest)
    return str(dest)


# -----------------------------
# Public API (called by router)
# -----------------------------
def ingest_camcat(
    onix_xml_path: Path,
    covers_dir: Optional[Path],
    books_json_path: Path,
    uploads_root: Path,
) -> Dict[str, Any]:
    if not onix_xml_path.exists():
        raise FileNotFoundError(f"ONIX not found at: {onix_xml_path}")

    # Load existing books.json (we append)
    if books_json_path.exists():
        books: List[Dict[str, Any]] = json.loads(books_json_path.read_text(encoding="utf-8"))
    else:
        books = []

    # Parse ONIX
    tree = ET.parse(str(onix_xml_path))
    root = tree.getroot()
    ns = onix_ns(root)
    products = root.findall(".//onix:Product", ns)

    grouped: Dict[str, Dict[str, Any]] = {}

    for prod in products:
        isbn = extract_isbn13(prod, ns)
        title = extract_title(prod, ns)
        author = extract_author(prod, ns)
        if not title or not author or not isbn:
            continue

        fmt_name = extract_format_name(prod, ns)
        pub_year = extract_publishing_year(prod, ns)
        pub_date = extract_pub_date(prod, ns)
        price_us, price_can = extract_prices(prod, ns)
        dims = extract_dimensions(prod, ns)

        key = grouping_key(title, author)

        if key not in grouped:
            book_uid = str(uuid.uuid4())
            grouped[key] = {
                "uid": book_uid,
                "title": title,
                "publishing_year": pub_year or 0,
                "author": author,
                "formats": [],
                "camcat": {"cover_link": ""},
            }

        book = grouped[key]

        cover_link = extract_cover_link(prod, ns)
        if cover_link and not book.get("camcat", {}).get("cover_link"):
            book["camcat"]["cover_link"] = cover_link

        # de-dupe by ISBN inside formats
        existing = next((f for f in book["formats"] if clean(f.get("isbn")) == isbn), None)
        if existing:
            if not existing.get("pub_date") and pub_date:
                existing["pub_date"] = pub_date
            if not existing.get("price_us") and price_us:
                existing["price_us"] = price_us
            if not existing.get("price_can") and price_can:
                existing["price_can"] = price_can
            continue

        row: Dict[str, Any] = {
            "format": fmt_name,
            "isbn": isbn,
            "pub_date": pub_date,
            "price_us": price_us or 0,
            "price_can": price_can or 0,
            "loc_number": dims.get("loc_number", ""),
            "pages": dims.get("pages", 0),
            "tall": dims.get("tall", 0),
            "wide": dims.get("wide", 0),
            "spine": dims.get("spine", 0),
            "weight": dims.get("weight", 0),
        }
        row["uid"] = stable_format_uid(book["uid"], row)
        book["formats"].append(row)

        if not book.get("publishing_year") and pub_year:
            book["publishing_year"] = pub_year

    # finalize books (one cover + one uid folder per book)
    new_books: List[Dict[str, Any]] = []
    for book in grouped.values():
        if not book.get("formats"):
            continue

        primary_isbn = pick_primary_isbn(book["formats"])
        book["isbn"] = primary_isbn

        cover_written = write_cover_to_uploads(
            uploads_root=uploads_root,
            book_uid=book["uid"],
            covers_dir=covers_dir,
            isbn_for_cover=primary_isbn,
        )
        if cover_written:
            book["cover_image_url"] = cover_written.replace("\\", "/")

        new_books.append(book)

    before = len(books)
    books.extend(new_books)

    books_json_path.parent.mkdir(parents=True, exist_ok=True)
    books_json_path.write_text(json.dumps(books, ensure_ascii=False, indent=2), encoding="utf-8")

    return {
        "products_seen": len(products),
        "books_created": len(new_books),
        "books_total_before": before,
        "books_total_after": len(books),
    }


def ingest_onix(
    onix_xml_path: Path,
    books_json_path: Path,
    uploads_root: Path,
    covers_dir: Optional[Path] = None,
    source_tag: str = "onix",
) -> Dict[str, Any]:
    """
    Generic ONIX ingest wrapper.

    - Accepts any ONIX XML file path.
    - Updates books.json.
    - Optionally copies covers if a covers_dir is supplied.
    - source_tag is reserved for future (e.g., storing provenance).
    """
    # For now, we reuse ingest_camcat’s parsing logic; "source_tag" is kept for future.
    _ = source_tag
    return ingest_camcat(
        onix_xml_path=onix_xml_path,
        covers_dir=covers_dir,
        books_json_path=books_json_path,
        uploads_root=uploads_root,
    )
