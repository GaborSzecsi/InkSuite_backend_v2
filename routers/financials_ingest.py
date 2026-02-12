"""
Ingest BTPS CSV financial reports into a normalized JSON structure.

Usage (from C:\\Users\\szecs\\Documents):

    python -m marble_app.routers.financials_ingest

This will read:
  - C:\\Users\\szecs\\Documents\\marble_app\\book_data\\books.json
  - C:\\Users\\szecs\\Documents\\marble_app\\data\\financials\\*.csv

And write:
  - C:\\Users\\szecs\\Documents\\marble_app\\book_data\\financials.json

The financials.json structure is:

{
  "YYYY-MM": {
    "byBook": {
      "<bookUid>": {
        "formats": {
          "HC":  {
            # Core roll-up fields
            "unitsSold": ...,
            "returns": ...,
            "inventoryEnd": ...,
            "freeCopies": ...,   # *non-Fraser* free copies only

            # Optional inventory breakdown per print run
            "inventory": {
              "print1": ...,
              "print2": ...
            },

            # Fraser consignment shipments (from Publisher_Free_Book_Report)
            "fraserShipments": ...,

            # Dollar amount from Fraser (sell-through, net of returns)
            "fraserDollars": ...,

            # Optional detailed sales breakdown for UI / royalty:
            "sales": {
              "us": {
                "unitsGross": ...,
                "unitsReturns": ...,
                "unitsNet": ...
              },
              "fraser": {
                "unitsGross": ...,
                "unitsReturns": ...,
                "unitsNet": ...,
                "dollars": ...
              }
            },

            # Optional extra inventory signals for reconciliation:
            "beginInventory": ...,
            "printsTotal": ...,
            "shipQty": ...,
            "adjQty": ...,
            "reconDelta": ...     # (begin + receivedTotal) - (end + ship + adj)
          },
          "PB":  { ... },
          "BB":  { ... },
          "EBK": { ... },
          "AUD": { ... }
        }
      }
    }
  }
}
"""

from __future__ import annotations

import csv
import json
import re
from pathlib import Path
from typing import Dict, Tuple, Any


# ------------------ paths ------------------

# financials_ingest.py lives in marble_app/routers
# we want the project root: marble_app
BASE_DIR = Path(__file__).resolve().parents[1]
UPLOADS_DIR = BASE_DIR / "data" / "uploads"
BOOK_DATA_DIR = BASE_DIR / "book_data"
FINANCIALS_DIR = BASE_DIR / "data" / "financials"
FINANCIALS_JSON = BOOK_DATA_DIR / "financials.json"


# ------------------ helpers ------------------

def norm_isbn(s: str) -> str:
    """Normalize ISBN to digits/X only, so 978-123-456 becomes 978123456."""
    return re.sub(r"[^0-9Xx]", "", s or "")


FORMAT_MAP = {
    "hardcover": "HC",
    "hard back": "HC",
    "hard back case": "HC",
    "cased laminate": "HC",
    "paperback": "PB",
    "softcover": "PB",
    "soft cover": "PB",
    "board book": "BB",
    "boardbook": "BB",
    "ebook": "EBK",
    "e-book": "EBK",
    "e book": "EBK",
    "audio": "AUD",
    "audiobook": "AUD",
}


def format_code_from_name(name: str) -> str | None:
    key = (name or "").strip().lower()
    return FORMAT_MAP.get(key)


def period_key_from_filename(fname: str) -> str | None:
    """
    Expect names like '2506-Activity_Summary_Report.csv' where 25=year, 06=month.

    Returns '2025-06' for '2506-...'.
    """
    m = re.match(r"(\d{4})-", fname)
    if not m:
        return None
    code = m.group(1)
    yy = int(code[:2])
    mm = int(code[2:])
    year = 2000 + yy
    return f"{year:04d}-{mm:02d}"


def safe_int_from_number_like(value: Any) -> int:
    """
    Convert BTPS numeric strings like '0', '0.00', '1,234', '-22.00' to int.
    Returns 0 on failure/blank.
    """
    s = (str(value or "")).strip()
    if not s:
        return 0
    s = s.replace(",", "")
    try:
        return int(float(s))
    except Exception:
        return 0


def safe_float_from_number_like(value: Any) -> float:
    """
    Convert BTPS numeric strings like '0', '0.00', '1,234', '-22.00' to float.
    Returns 0.0 on failure/blank.
    """
    s = (str(value or "").strip())
    if not s:
        return 0.0
    s = s.replace(",", "")
    try:
        return float(s)
    except Exception:
        return 0.0


# ------------------ ISBN -> (uid, format) index ------------------

def build_isbn_index() -> Dict[str, Tuple[str, str]]:
    """
    Build a mapping:
        norm_isbn -> (bookUid, formatCode)
    using book_data/books.json
    """
    books_path = BOOK_DATA_DIR / "books.json"
    if not books_path.exists():
        raise FileNotFoundError(f"books.json not found at: {books_path}")

    with books_path.open("r", encoding="utf-8") as f:
        books = json.load(f)

    index: Dict[str, Tuple[str, str]] = {}
    missing_formats = set()

    if not isinstance(books, list):
        raise ValueError("books.json must be a JSON array of book objects")

    for b in books:
        uid = (
            b.get("uid")
            or b.get("id")
            or b.get("book_uid")
            or b.get("slug")
        )
        if not uid:
            continue

        for fmt in b.get("formats", []):
            isbn_raw = fmt.get("isbn")
            if not isbn_raw:
                continue
            fmt_name = str(fmt.get("format") or "")
            fmt_code = format_code_from_name(fmt_name)
            if not fmt_code:
                missing_formats.add(fmt_name.strip())
                continue

            isbn_norm = norm_isbn(str(isbn_raw))
            if not isbn_norm:
                continue

            index[isbn_norm] = (uid, fmt_code)

    if missing_formats:
        print(
            "[financials_ingest] Warning: some format names not mapped to codes:",
            sorted(missing_formats),
        )

    print(f"[financials_ingest] Built ISBN index with {len(index)} entries.")
    return index


# ------------------ CSV parsers ------------------

# We allow mixed types in the stats bucket (ints + list for inventory prints)
StatsBucket = Dict[str, Any]


def get_bucket(
    accum: Dict[Tuple[str, str, str], StatsBucket],
    period: str,
    uid: str,
    fmt_code: str,
) -> StatsBucket:
    key = (period, uid, fmt_code)
    if key not in accum:
        accum[key] = {
            # Ebook / generic units from Ebook_Sales_By_Title
            "unitsSold": 0,

            # Physical US metrics from Monthly_Sales_By_Title
            "usUnitsSold": 0,         # gross US units (positive lines only)
            "usReturnsUnits": 0,      # US returns (absolute units, negative lines)

            # Fraser-specific metrics from Monthly_Sales_By_Title
            "fraserUnitsSold": 0,     # gross Fraser units sold (positive qty)
            "fraserReturnUnits": 0,   # Fraser returns (absolute units, negative qty)
            "fraserDollars": 0.0,     # revenue from Fraser (can be negative with returns)

            # Fraser consignment shipments from Publisher_Free_Book_Report
            "fraserShipments": 0,

            # Roll-up fields
            "returns": 0,             # final returns figure (for compatibility)
            "inventoryEnd": 0,

            # Free copies (non-Fraser only)
            "freeCopies": 0,

            # Activity Summary inventory flow
            "receivedTotal": 0,       # TOT QTY RECV (returns + new prints)
            "inventoryPrints": [],    # list of per-print-run quantities from Inventory_Receipt_Report

            # Extra Activity Summary fields for reconciliation
            "inventoryBegin": 0,      # BEG QTY
            "shipQty": 0,             # SHIP QTY
            "adjQty": 0,              # ADJ QTY
        }
    return accum[key]


def parse_activity_summary(
    path: Path,
    isbn_index: Dict[str, Tuple[str, str]],
    accum: Dict[Tuple[str, str, str], StatsBucket],
) -> None:
    """
    Activity_Summary_Report.csv

    Columns (after skipping header lines):
    0: ISBN
    1: TITLE
    2: AUTHOR (BTPS often puts PROD TYPE here...)
    3: PROD TYPE
    4: WHS
    5: BILL PERIOD
    6: BEG QTY
    7: END QTY
    8: TOT QTY RECV
    9: SHIP QTY
    10: STRG DUE
    11: INSUR DUE
    12: ADJ QTY

    We use:
      - BEG QTY (col 6) as 'inventoryBegin' (sum across warehouses)
      - END QTY (col 7) as 'inventoryEnd' (sum across warehouses)
      - TOT QTY RECV (col 8) as 'receivedTotal' (returns + new prints).
        Later we estimate:
            returns_base = max(0, receivedTotal - sum(inventoryPrints))
      - SHIP QTY (col 9) and ADJ QTY (col 12) for reconciliation only.
    """
    period = period_key_from_filename(path.name)
    if not period:
        print(f"[financials_ingest] Skipping {path.name} – cannot parse period.")
        return

    with path.open("r", encoding="utf-8", errors="replace") as f:
        reader = csv.reader(f)
        # skip first 4 header lines
        for _ in range(4):
            next(reader, None)

        for row in reader:
            if not row or not row[0].strip():
                continue
            if row[0].startswith("TOT"):
                break

            isbn_raw = row[0].strip()
            isbn_norm = norm_isbn(isbn_raw)
            if not isbn_norm or isbn_norm not in isbn_index:
                continue

            uid, fmt_code = isbn_index[isbn_norm]

            begin_qty = safe_int_from_number_like(row[6] if len(row) > 6 else 0)
            end_qty = safe_int_from_number_like(row[7] if len(row) > 7 else 0)
            recv_qty = safe_int_from_number_like(row[8] if len(row) > 8 else 0)
            ship_qty = safe_int_from_number_like(row[9] if len(row) > 9 else 0)
            adj_qty = safe_int_from_number_like(row[12] if len(row) > 12 else 0)

            bucket = get_bucket(accum, period, uid, fmt_code)

            bucket["inventoryBegin"] += begin_qty
            bucket["inventoryEnd"] += end_qty
            bucket["receivedTotal"] += recv_qty
            bucket["shipQty"] += ship_qty
            bucket["adjQty"] += adj_qty


def parse_monthly_sales_by_title(
    path: Path,
    isbn_index: Dict[str, Tuple[str, str]],
    accum: Dict[Tuple[str, str, str], StatsBucket],
) -> None:
    """
    Monthly_Sales_By_Title.csv

    Structure (after 3 pre-header lines):
      0: ISBN #
      1: TITLE
      2: WAREHOUSE
      3: ORDER #
      4: PONO
      5: CUSTOMER
      6: UNITS SOLD
      7: DISC %
      8: AMOUNT

    We use:
      - UNITS SOLD as quantities (can be negative for returns)
      - CUSTOMER to separate FRASER DIRECT from US customers
      - AMOUNT as revenue, used only for Fraser (fraserDollars)

    Rules:
      - US units sold (non-Fraser, qty > 0):   -> stats['usUnitsSold']
      - US returns (non-Fraser, qty < 0):      -> stats['usReturnsUnits'] (absolute units)
      - Fraser sales (qty > 0):                -> stats['fraserUnitsSold'] (units),
                                                 stats['fraserDollars'] (+amount)
      - Fraser returns (qty < 0):              -> stats['fraserReturnUnits'] (absolute units),
                                                 stats['fraserDollars'] (+negative amount)
    """
    period = period_key_from_filename(path.name)
    if not period:
        print(f"[financials_ingest] Skipping {path.name} – cannot parse period.")
        return

    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.reader(f)

        # Skip pre-header lines until we hit the actual column header
        header = None
        for row in reader:
            if not row:
                continue
            if str(row[0]).strip().upper().startswith("ISBN"):
                header = row
                break

        if not header or len(header) < 7:
            print(f"[financials_ingest]  -> no usable header in {path.name}, skipping.")
            return

        for row in reader:
            if not row or not row[0].strip():
                continue

            # Skip subtotal / total rows
            first_cell = row[0].strip().upper()
            if first_cell.startswith("SUBTOTAL") or first_cell.startswith("TOTAL"):
                continue

            if len(row) < 9:
                continue

            isbn_norm = norm_isbn(row[0])
            if not isbn_norm or isbn_norm not in isbn_index:
                continue

            uid, fmt_code = isbn_index[isbn_norm]

            customer = (row[5] or "").strip().upper()
            qty = safe_int_from_number_like(row[6])
            amount = safe_float_from_number_like(row[8])

            if qty == 0 and amount == 0.0:
                continue

            bucket = get_bucket(accum, period, uid, fmt_code)
            is_fraser = "FRASER" in customer

            if qty > 0:
                if is_fraser:
                    # Fraser sell-through: units and money received from Canada
                    bucket["fraserUnitsSold"] = bucket.get("fraserUnitsSold", 0) + qty
                    bucket["fraserDollars"] = bucket.get("fraserDollars", 0.0) + amount
                else:
                    # US sales
                    bucket["usUnitsSold"] = bucket.get("usUnitsSold", 0) + qty
            elif qty < 0:
                abs_qty = -qty
                if is_fraser:
                    # Fraser returns
                    bucket["fraserReturnUnits"] = bucket.get("fraserReturnUnits", 0) + abs_qty
                    bucket["fraserDollars"] = bucket.get("fraserDollars", 0.0) + amount
                else:
                    # US returns
                    bucket["usReturnsUnits"] = bucket.get("usReturnsUnits", 0) + abs_qty


def parse_ebook_sales(
    path: Path,
    isbn_index: Dict[str, Tuple[str, str]],
    accum: Dict[Tuple[str, str, str], StatsBucket],
) -> None:
    """
    Ebook_Sales_By_Title.csv

    Approx structure:
      Header line 1
      Header line 2
      Header line 3
      Then rows:
        0: ORDER NBR
        1: DATE
        2: ISBN
        3: TITLE
        4: QUANTITY
        5: PRODUCT AMT
      With subtotals lines like:
        "Subtotal for: [ISBN] ..."
      And a 'Report Totals' line at the end.

    We treat QUANTITY as 'unitsSold' for EBK format.
    """
    period = period_key_from_filename(path.name)
    if not period:
        print(f"[financials_ingest] Skipping {path.name} – cannot parse period.")
        return

    with path.open("r", encoding="utf-8", errors="replace") as f:
        reader = csv.reader(f)
        # skip 3 header lines
        for _ in range(3):
            next(reader, None)

        for row in reader:
            if not row or not row[0].strip():
                continue
            if row[0].startswith("Subtotal for"):
                continue
            if row[0].startswith("Report Totals"):
                break
            if len(row) < 5:
                continue

            isbn_norm = norm_isbn(row[2])
            if not isbn_norm or isbn_norm not in isbn_index:
                continue

            uid, fmt_code = isbn_index[isbn_norm]

            qty = safe_int_from_number_like(row[4])

            bucket = get_bucket(accum, period, uid, fmt_code)
            bucket["unitsSold"] += qty
            # no explicit returns info in this report


def parse_free_book_report(
    path: Path,
    isbn_index: Dict[str, Tuple[str, str]],
    accum: Dict[Tuple[str, str, str], StatsBucket],
) -> None:
    """
    Parse Publisher_Free_Book_Report.csv files.

    We:
      - Derive period key from the filename (e.g. 2506-... -> 2025-06)
      - Find header row with 'ISBN', 'SHIP QTY', and 'SHIP TO'
      - For each data row, map ISBN -> (book_uid, format_code)
      - If SHIP TO contains 'FRASER', add SHIP QTY to stats['fraserShipments']
        (consignment sent to Canada)
      - Otherwise, add SHIP QTY to stats['freeCopies']

    IMPORTANT:
      freeCopies here are *only* true freebies (non-Fraser).
      Fraser shipments stay in 'fraserShipments' and do not inflate freeCopies.
    """
    print(f"[financials_ingest] Processing {path}...")
    if not path.exists():
        print(f"[financials_ingest]  -> file missing, skipping {path}")
        return

    period = period_key_from_filename(path.name)
    if not period:
        print(f"[financials_ingest]  -> cannot parse period from {path.name}, skipping.")
        return

    try:
        with path.open("r", encoding="utf-8-sig", newline="") as f:
            reader = csv.reader(f)

            isbn_col = None
            qty_col = None
            shipto_col = None

            # ---- find the header row ----
            for row in reader:
                cells = [c.strip() for c in row]
                if not any(cells):
                    continue

                upper = [c.upper() for c in cells]

                if "ISBN" in upper and any("SHIP" in u and "QTY" in u for u in upper):
                    isbn_col = upper.index("ISBN")
                    for i, u in enumerate(upper):
                        if "SHIP" in u and "QTY" in u:
                            qty_col = i
                        if "SHIP TO" in u:
                            shipto_col = i
                    break  # header found

            if isbn_col is None or qty_col is None:
                print(
                    f"[financials_ingest]  -> free book report missing ISBN or SHIP QTY column "
                    f"(isbn_col={isbn_col}, qty_col={qty_col}), skipping {path.name}."
                )
                return

            # ---- data rows ----
            for row in reader:
                if len(row) <= max(isbn_col, qty_col):
                    continue

                raw_isbn = row[isbn_col].strip()
                raw_qty = row[qty_col].strip()

                if not raw_isbn or not raw_qty:
                    continue

                isbn_norm = norm_isbn(raw_isbn)
                if not isbn_norm or isbn_norm not in isbn_index:
                    continue

                qty = safe_int_from_number_like(raw_qty)
                if qty == 0:
                    continue

                ship_to = ""
                if shipto_col is not None and len(row) > shipto_col:
                    ship_to = (row[shipto_col] or "").strip().upper()

                uid, fmt_code = isbn_index[isbn_norm]
                bucket = get_bucket(accum, period, uid, fmt_code)

                if "FRASER" in ship_to:
                    # Consignment shipped to Fraser (Canada)
                    bucket["fraserShipments"] = bucket.get("fraserShipments", 0) + qty
                else:
                    # Regular free copies (review copies, samples, etc.)
                    bucket["freeCopies"] = bucket.get("freeCopies", 0) + qty

    except Exception as e:
        print(f"[financials_ingest] ERROR parsing free-book report {path}: {e}")


def parse_inventory_receipt_report(
    path: Path,
    isbn_index: Dict[str, Tuple[str, str]],
    accum: Dict[Tuple[str, str, str], StatsBucket],
) -> None:
    """
    Parse Inventory_Receipt_Report.csv files and record new print runs.

    Typical header (after some preamble):
      TTRAN, PUB, RECEIPT DATE, ISBN, TITLE, WAREHOUSE, QTY RECEIVED, ...

    We:
      - Derive period key from filename (YYMM-Inventory_Receipt_Report.csv)
      - Find header row with 'ISBN' and a 'QTY RECEIVED' style column
      - For each data row, map ISBN -> (book_uid, format_code)
      - Append QTY RECEIVED as another print run in bucket['inventoryPrints']
    """
    print(f"[financials_ingest] Processing {path} (inventory receipt)...")
    if not path.exists():
        print(f"[financials_ingest]  -> file missing, skipping {path}")
        return

    period = period_key_from_filename(path.name)
    if not period:
        print(f"[financials_ingest]  -> cannot parse period from {path.name}, skipping.")
        return

    try:
        with path.open("r", encoding="utf-8-sig", newline="") as f:
            reader = csv.reader(f)

            isbn_col = None
            qty_col = None

            # ---- find the header row ----
            for row in reader:
                cells = [c.strip() for c in row]
                if not any(cells):
                    continue
                upper = [c.upper() for c in cells]

                # Look for header with 'ISBN' and something like 'QTY RECEIVED'
                if "ISBN" in upper and any("QTY" in u and "RECE" in u for u in upper):
                    isbn_col = upper.index("ISBN")
                    for i, u in enumerate(upper):
                        if "QTY" in u and "RECE" in u:
                            qty_col = i
                            break
                    break

            if isbn_col is None or qty_col is None:
                print(
                    f"[financials_ingest]  -> inventory receipt report missing ISBN or QTY RECEIVED "
                    f"(isbn_col={isbn_col}, qty_col={qty_col}), skipping {path.name}."
                )
                return

            # ---- data rows ----
            for row in reader:
                if len(row) <= max(isbn_col, qty_col):
                    continue

                raw_isbn = row[isbn_col].strip()
                raw_qty = row[qty_col].strip()

                if not raw_isbn or not raw_qty:
                    continue

                isbn_norm = norm_isbn(raw_isbn)
                if not isbn_norm or isbn_norm not in isbn_index:
                    continue

                qty = safe_int_from_number_like(raw_qty)
                if qty <= 0:
                    continue

                uid, fmt_code = isbn_index[isbn_norm]
                bucket = get_bucket(accum, period, uid, fmt_code)

                prints: list[int] = bucket.setdefault("inventoryPrints", [])
                prints.append(qty)

    except Exception as e:
        print(f"[financials_ingest] ERROR parsing inventory receipt report {path}: {e}")


# ------------------ rebuild main ------------------

def rebuild_financials() -> None:
    print("[financials_ingest] Starting rebuild...")
    print(f"[financials_ingest] BOOK_DATA_DIR:     {BOOK_DATA_DIR}")
    print(f"[financials_ingest] FINANCIALS_DIR:    {FINANCIALS_DIR}")
    print(f"[financials_ingest] OUTPUT JSON FILE:  {FINANCIALS_JSON}")

    isbn_index = build_isbn_index()

    if not FINANCIALS_DIR.exists():
        raise FileNotFoundError(f"Financials CSV directory not found: {FINANCIALS_DIR}")

    accum: Dict[Tuple[str, str, str], StatsBucket] = {}

    # Walk all CSVs in data/financials **recursively**
    scan_roots = [FINANCIALS_DIR]
    if UPLOADS_DIR.exists():
        scan_roots.append(UPLOADS_DIR)

    all_csvs = []
    for root in scan_roots:
        all_csvs.extend(root.rglob("*.csv"))

    for path in sorted(all_csvs):
        name_lower = path.name.lower()
        print(f"[financials_ingest] Processing {path}...")

        if "activity_summary_report" in name_lower:
            parse_activity_summary(path, isbn_index, accum)
        elif "monthly_sales_by_title" in name_lower:
            parse_monthly_sales_by_title(path, isbn_index, accum)
        elif "ebook_sales_by_title" in name_lower:
            parse_ebook_sales(path, isbn_index, accum)
        elif "publisher_free_book_report" in name_lower:
            parse_free_book_report(path, isbn_index, accum)
        elif "inventory_receipt_report" in name_lower:
            parse_inventory_receipt_report(path, isbn_index, accum)
        else:
            print(f"[financials_ingest]  -> skipping (unknown type)")

    # Build final nested structure
    out: Dict[str, Any] = {}

    for (period, uid, fmt_code), stats in accum.items():
        period_block = out.setdefault(period, {"byBook": {}})
        by_book = period_block["byBook"]
        book_block = by_book.setdefault(uid, {"formats": {}})

        # Raw Activity Summary inventory flow
        received_total = int(stats.get("receivedTotal", 0))
        inventory_prints = stats.get("inventoryPrints") or []
        total_prints = int(sum(int(p) for p in inventory_prints)) if inventory_prints else 0

        begin_inv = int(stats.get("inventoryBegin", 0))
        end_inv = int(stats.get("inventoryEnd", 0))
        ship_qty = int(stats.get("shipQty", 0))
        adj_qty = int(stats.get("adjQty", 0))

        # Base returns from Activity Summary:
        #   returns_base = max(0, receivedTotal - sum(inventoryPrints))
        returns_base = max(0, received_total - total_prints)

        # Remove Fraser-related returns from this base, if present
        fraser_returns_units = int(stats.get("fraserReturnUnits", 0))
        returns_after_fraser = max(0, returns_base - fraser_returns_units)

        # Prefer explicit US returns from Monthly_Sales_By_Title if present
        monthly_us_returns = int(stats.get("usReturnsUnits", 0))
        if monthly_us_returns > 0:
            returns_final = monthly_us_returns
        else:
            returns_final = returns_after_fraser

        # Build inventory dict { print1: qty, print2: qty, ... }
        inventory_dict: Dict[str, int] = {}
        for idx, qty in enumerate(inventory_prints, start=1):
            inventory_dict[f"print{idx}"] = int(qty)

        # Units sold:
        #   - Physical US units from Monthly_Sales_By_Title -> usUnitsSold
        #   - Ebook (and other non-physical) units from parse_ebook_sales -> unitsSold
        physical_us_units_gross = int(stats.get("usUnitsSold", 0))
        other_units = int(stats.get("unitsSold", 0))
        units_sold_final = physical_us_units_gross + other_units

        # Detailed sales breakdown
        us_units_returns = int(stats.get("usReturnsUnits", 0))
        fraser_units_gross = int(stats.get("fraserUnitsSold", 0))
        fraser_units_returns = int(stats.get("fraserReturnUnits", 0))
        fraser_dollars = float(stats.get("fraserDollars", 0.0) or 0.0)

        sales_block: Dict[str, Any] = {}

        if physical_us_units_gross or us_units_returns:
            sales_block["us"] = {
                "unitsGross": physical_us_units_gross,
                "unitsReturns": us_units_returns,
                "unitsNet": physical_us_units_gross - us_units_returns,
            }

        if fraser_units_gross or fraser_units_returns or abs(fraser_dollars) > 0.0001:
            sales_block["fraser"] = {
                "unitsGross": fraser_units_gross,
                "unitsReturns": fraser_units_returns,
                "unitsNet": fraser_units_gross - fraser_units_returns,
                "dollars": round(fraser_dollars, 2),
            }

        # Base format entry
        format_entry: Dict[str, Any] = {
            "unitsSold": units_sold_final,
            "returns": int(returns_final),
            "inventoryEnd": end_inv,
            # freeCopies are already "pure freebies" (non-Fraser) from free-book report
            "freeCopies": int(stats.get("freeCopies", 0)),
        }

        if inventory_dict:
            format_entry["inventory"] = inventory_dict

        # Fraser consignment shipments (copies sent to Fraser in the month)
        fraser_shipments = int(stats.get("fraserShipments", 0))
        if fraser_shipments:
            format_entry["fraserShipments"] = fraser_shipments

        # Dollar amount received from Fraser (net, may include negatives if returns)
        if abs(fraser_dollars) > 0.0001:
            format_entry["fraserDollars"] = round(fraser_dollars, 2)

        # Attach detailed sales breakdown if any
        if sales_block:
            format_entry["sales"] = sales_block

        # Extra reconciliation info for debugging / sanity checks
        if begin_inv or received_total or ship_qty or adj_qty or total_prints:
            # Warehouse-level reconciliation:
            #   begin + receivedTotal ≈ end + shipQty + adjQty
            recon_delta = (begin_inv + received_total) - (end_inv + ship_qty + adj_qty)
            format_entry["beginInventory"] = begin_inv
            if total_prints:
                format_entry["printsTotal"] = total_prints
            if ship_qty:
                format_entry["shipQty"] = ship_qty
            if adj_qty:
                format_entry["adjQty"] = adj_qty
            if recon_delta:
                format_entry["reconDelta"] = recon_delta

        book_block["formats"][fmt_code] = format_entry

    # Write JSON
    BOOK_DATA_DIR.mkdir(parents=True, exist_ok=True)
    with FINANCIALS_JSON.open("w", encoding="utf-8") as f:
        json.dump(out, f, indent=2, sort_keys=True)
        f.write("\n")

    periods = sorted(out.keys())
    total_rows = sum(
        len(book_block["formats"])
        for _period, block in out.items()
        for _uid, book_block in block["byBook"].items()
    )

    print(f"[financials_ingest] Done.")
    print(f"[financials_ingest] Periods:       {periods or 'none'}")
    print(f"[financials_ingest] Book-format rows aggregated: {total_rows}")
    print(f"[financials_ingest] Wrote: {FINANCIALS_JSON}")


if __name__ == "__main__":
    rebuild_financials()
