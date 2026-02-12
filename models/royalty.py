# royalties_api.py
from __future__ import annotations

import json
import os
from datetime import date
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

from fastapi import FastAPI, HTTPException, Body
from fastapi import Response, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Extra, Field
from fastapi.responses import HTMLResponse
from jinja2 import Template
import base64, pathlib, io

# =============================================================================
# Data models (uid is the single source of truth)
# =============================================================================

class BookFormat(BaseModel):
    format: Optional[str] = Field(default=None, alias="Format")
    isbn: Optional[str] = Field(default=None, alias="ISBN")
    pub_date: Optional[str] = Field(default=None, alias="PubDate")

    price: Optional[float] = Field(default=None, alias="Price")
    price_us: Optional[float] = Field(default=None, alias="Price US")
    price_can: Optional[float] = Field(default=None, alias="Price CAN")

    LOC: Optional[str] = None
    Pages: Optional[int] = None
    Tall: Optional[float] = None
    Wide: Optional[float] = None
    Spine: Optional[float] = None
    Weight: Optional[float] = None

    class Config:
        extra = Extra.allow
        allow_population_by_field_name = True


class Agent(BaseModel):
    name: Optional[str] = ""
    agency: Optional[str] = ""
    address: Optional[Any] = None
    email: Optional[str] = ""

    class Config:
        extra = Extra.allow


class RoyaltyRate(BaseModel):
    category: Optional[str] = None
    royalty_percent: float = Field(0.0, ge=0.0, le=100.0)
    net_revenue_based: bool = False

    class Config:
        extra = Extra.allow


class Illustrator(BaseModel):
    name: Optional[str] = ""
    email: Optional[str] = ""
    address: Optional[Any] = None
    agent: Optional[Agent] = None

    class Config:
        extra = Extra.allow


# ---------- Rich royalty schema (supports tiers & discount conditions) ----------

class TierCondition(BaseModel):
    kind: str  # "units" or "discount"
    comparator: str = "<"  # one of <, <=, >, >=, ==
    value: float


class RoyaltyTier(BaseModel):
    rate_percent: float
    conditions: List[TierCondition] = Field(default_factory=list)


class RightsBlock(BaseModel):
    format: str  # e.g., "Hardcover", "Paperback", "E-book"
    base: str = "list_price"  # "list_price" or "net_receipts"
    tiers: List[RoyaltyTier] = Field(default_factory=list)
    flat_rate_percent: Optional[float] = None  # fallback if tiers empty


class Subright(BaseModel):
    name: str
    mode: Optional[str] = None      # e.g., "fixed"
    percent: Optional[float] = None # percent split for net receipts, etc.
    base: Optional[str] = None      # "net_receipts", "list_price", ...


class PartyRights(BaseModel):
    first_rights: List[RightsBlock] = Field(default_factory=list)
    subrights: List[Subright] = Field(default_factory=list)


class RichRoyalties(BaseModel):
    author: PartyRights = Field(default_factory=PartyRights)
    illustrator: PartyRights = Field(default_factory=PartyRights)


class Book(BaseModel):
    title: Optional[str] = None
    subtitle: Optional[str] = None
    author: Optional[str] = None

    author_email: Optional[str] = ""
    author_address: Optional[Any] = None
    author_agent: Optional[Agent] = None

    author_royalty: List[RoyaltyRate] = Field(default_factory=list)  # legacy fallback
    author_advance: float = 0.0
    author_has_agency: bool = False
    author_split: int = Field(100, ge=0, le=100)
    agent_split: int = Field(0, ge=0, le=100)

    illustrator: Optional[Illustrator] = None
    illustrator_royalty: List[RoyaltyRate] = Field(default_factory=list)  # legacy fallback
    illustrator_advance: float = 0.0
    illustrator_has_agency: bool = False
    illustrator_split: int = Field(100, ge=0, le=100)
    illustrator_agent_split: int = Field(0, ge=0, le=100)

    royalties: Optional[RichRoyalties] = None

    publishing_year: Optional[int] = Field(default_factory=lambda: date.today().year)
    description: Optional[str] = None

    formats: List[BookFormat] = Field(default_factory=list)

    # IDs — uid is the ONLY source of truth
    uid: Optional[str] = None

    class Config:
        extra = Extra.allow
        allow_population_by_field_name = True


class SalesData(BaseModel):
    category: str
    units: int = 0
    returns: int = 0
    unit_price_or_net_revenue: float = 0.0
    discount: float = 0.0
    net_revenue: bool = False


class RoyaltyCalculation(BaseModel):
    category: str
    units: int
    returns: int
    net_units: int
    lifetime_quantity: str
    returns_to_date: str
    unit_price: str
    royalty_rate_percent: str
    discount: float
    net_revenue: str
    value: str
    royalty: str


class PaymentSummary(BaseModel):
    advance_paid: str
    royalty_for_period: str
    last_period_balance: str
    balance: str
    amount_payable: str


class RoyaltyStatementRequest(BaseModel):
    # REQUIRED uid from client
    uid: str
    period_start: str
    period_end: str
    sales_data: List[SalesData]

    # Simple rates (legacy) are used when Book.royalties is not present
    author_rates: Dict[str, float] = Field(default_factory=dict)
    illustrator_rates: Dict[str, float] = Field(default_factory=dict)

    author_advance: float = 0.0
    illustrator_advance: float = 0.0

    class Config:
        extra = Extra.allow
        allow_population_by_field_name = True


class RoyaltyStatement(BaseModel):
    # ALWAYS emit uid
    uid: str
    period_start: str
    period_end: str
    sales_data: List[SalesData]

    author: Dict[str, Any]
    illustrator: Dict[str, Any]

    lifetime_quantity_by_cat: Dict[str, int]
    returns_to_date_by_cat: Dict[str, int]

def _fmt_money(s: str) -> float:
    try:
        return float(str(s).replace("$","").replace(",","").replace("(","-").replace(")",""))
    except Exception:
        return 0.0

def _collect_isbns(book: Book) -> str:
    vals = []
    for f in (book.formats or []):
        if getattr(f, "isbn", None):
            vals.append(str(f.isbn).strip())
    out, seen = [], set()
    for v in vals:
        if v and v not in seen:
            out.append(v); seen.add(v)
    return ", ".join(out)

def _totals_for_rows(rows: List[Dict[str, Any]]) -> Dict[str, str]:
    u = sum(int(r.get("units", 0) or 0) for r in rows)
    rets = sum(int(r.get("returns", 0) or 0) for r in rows)
    net = sum(int(r.get("net_units", 0) or 0) for r in rows)
    lifetime = sum(int(str(r.get("lifetime_quantity","0")).replace(",","") or 0) for r in rows)
    rets_to_date = sum(int(str(r.get("returns_to_date","0")).replace(",","") or 0) for r in rows)
    val = "${:,.2f}".format(sum(_fmt_money(r.get("value","$0")) for r in rows))
    roy = "${:,.2f}".format(sum(_fmt_money(r.get("royalty","$0")) for r in rows))
    return {
        "units": f"{u:.1f}".rstrip('0').rstrip('.'),
        "returns": f"{rets:.1f}".rstrip('0').rstrip('.'),
        "net_units": f"{net:.1f}".rstrip('0').rstrip('.'),
        "lifetime": f"{lifetime:,.0f}",
        "returns_to_date": f"{rets_to_date:,.0f}",
        "value": val,
        "royalty": roy,
    }

# =============================================================================
# Tier-aware calculation helpers
# =============================================================================

Comparator: Dict[str, Callable[[float, float], bool]] = {
    "<":  lambda a, b: a < b,
    "<=": lambda a, b: a <= b,
    ">":  lambda a, b: a > b,
    ">=": lambda a, b: a >= b,
    "==": lambda a, b: a == b,
}


def category_to_format(cat: str) -> str:
    c = (cat or "").strip().lower()
    if "hc" in c and "can" in c:
        return "Hardcover"
    if "pb" in c and "can" in c:
        return "Paperback"
    if c.startswith("canada-"):
        tail = c.split("-", 1)[1]
        return "Hardcover" if tail.lower().startswith("hc") else "Paperback"
    if c in {"hardcover", "hc"}: return "Hardcover"
    if c in {"paperback", "pb"}: return "Paperback"
    if c in {"board book", "boardbook"}: return "Board Book"
    if "ebook" in c or c == "e-book": return "E-book"
    return cat or ""


def _find_rights_block(book: Book, party: str, fmt: str) -> Optional[RightsBlock]:
    if not book.royalties:
        return None
    party_rights: PartyRights = getattr(book.royalties, party, PartyRights())
    for block in (party_rights.first_rights or []):
        if (block.format or "").strip().lower() == (fmt or "").strip().lower():
            return block
    return None


def _pick_tiers(block: RightsBlock, discount: float) -> List[RoyaltyTier]:
    tiers = block.tiers or []
    any_discount = any(any(c.kind == "discount" for c in t.conditions) for t in tiers)
    chosen: List[RoyaltyTier] = []
    for t in tiers:
        ok = True
        for c in (t.conditions or []):
            if c.kind == "discount":
                comp = Comparator.get(c.comparator or "<")
                if (comp is None) or (not comp(discount, float(c.value))):
                    ok = False
                    break
        if ok:
            chosen.append(t)
    if not chosen and any_discount:
        return []
    return chosen or tiers


def _prorate_units_across_threshold(lifetime_before: float, add_units: float, threshold: float) -> Tuple[float, float]:
    if lifetime_before >= threshold:
        return 0.0, add_units
    remaining_low = max(0.0, threshold - lifetime_before)
    low_units = min(add_units, remaining_low)
    high_units = max(0.0, add_units - low_units)
    return low_units, high_units


def _compute_line(
    block: RightsBlock,
                  lifetime_before: float,
                  net_units_this_period: float,
                  unit_price: float,
                  discount: float,
    net_revenue_mode: bool
) -> List[Dict[str, float]]:
    """
    Return list of fragments with keys: units, rate_percent, value_amount, royalty_amount.
    If tiers exist, we use them; otherwise we fall back to flat_rate_percent.
    """
    results: List[Dict[str, float]] = []
    tiers = _pick_tiers(block, discount)

    if not tiers:
        rate = float(block.flat_rate_percent or 0.0)
        value_amount = unit_price * net_units_this_period
        results.append({
            "units": net_units_this_period,
            "rate_percent": rate,
            "value_amount": value_amount,
            "royalty_amount": unit_price * rate/100.0 * net_units_this_period,
        })
        return results

    unit_tiers = [t for t in tiers if any(c.kind == "units" for c in (t.conditions or []))]
    if not unit_tiers:
        t = tiers[0]
        rate = float(t.rate_percent)
        value_amount = unit_price * net_units_this_period
        results.append({
            "units": net_units_this_period,
            "rate_percent": rate,
            "value_amount": value_amount,
            "royalty_amount": unit_price * rate/100.0 * net_units_this_period,
        })
        return results

    threshold: Optional[float] = None
    low_rate: Optional[float] = None
    high_rate: Optional[float] = None
    for t in unit_tiers:
        r = float(t.rate_percent)
        for c in (t.conditions or []):
            if c.kind == "units":
                comp = c.comparator
                val = float(c.value)
                if comp in ("<=", "<"):
                    threshold = val
                    low_rate = r
                elif comp in (">", ">="):
                    high_rate = r

    if threshold is None:  # weird schema; treat as flat using first tier
        r = float(unit_tiers[0].rate_percent)
        value_amount = unit_price * net_units_this_period
        results.append({
            "units": net_units_this_period,
            "rate_percent": r,
            "value_amount": value_amount,
            "royalty_amount": unit_price * r/100.0 * net_units_this_period,
        })
        return results

    low_units, high_units = _prorate_units_across_threshold(lifetime_before, net_units_this_period, threshold)

    def add_fragment(units: float, rate: Optional[float]):
        if not units or units <= 0:
            return
        r = float(rate or 0.0)
        value_amount = unit_price * units
        results.append({
            "units": units,
            "rate_percent": r,
            "value_amount": value_amount,
            "royalty_amount": unit_price * r/100.0 * units,
        })

    add_fragment(low_units, low_rate)
    add_fragment(high_units, high_rate)
    return results


def calculate_statement(
    book: Book,
    req: RoyaltyStatementRequest,
    lifetime_quantity_by_cat: Dict[str, int],
    returns_to_date_by_cat: Dict[str, int],
    last_balance_author: float = 0.0,
    last_balance_illustrator: float = 0.0,
) -> RoyaltyStatement:

    def calc_party(party: str, simple_rates: Dict[str, float], advance: float, last_balance: float) -> Dict[str, Any]:
        total_royalty = 0.0
        rows: List[RoyaltyCalculation] = []

        for sd in (req.sales_data or []):
            units = int(sd.units or 0)
            rets = int(sd.returns or 0)
            net_units = units - rets
            lifetime_before = float(lifetime_quantity_by_cat.get(sd.category, 0))
            unit_price = float(sd.unit_price_or_net_revenue or 0.0)
            discount = float(sd.discount or 0.0)
            net_mode = bool(sd.net_revenue)

            if book.royalties:
                fmt = category_to_format(sd.category)
                block = _find_rights_block(book, party, fmt)
                if block:
                    fragments = _compute_line(block, lifetime_before, net_units, unit_price, discount, net_mode)
                    value_amount = sum(f["value_amount"] for f in fragments)
                    royalty_amount = sum(f["royalty_amount"] for f in fragments)
                    eff_rate = (royalty_amount / value_amount * 100.0) if value_amount > 0 else 0.0
                else:
                    value_amount = unit_price * max(net_units, 0)
                    royalty_amount = 0.0
                    eff_rate = 0.0
            else:
                rate = float(simple_rates.get(sd.category, 0.0))
                value_amount = unit_price * max(net_units, 0)
                royalty_amount = value_amount * rate / 100.0
                eff_rate = rate

            total_royalty += royalty_amount

            rows.append(RoyaltyCalculation(
                category=sd.category,
                units=units,
                returns=rets,
                net_units=net_units,
                lifetime_quantity=f"{lifetime_before:,.0f}",
                returns_to_date=f"{float(returns_to_date_by_cat.get(sd.category, 0)):,.0f}",
                unit_price=f"${unit_price:,.2f}",
                royalty_rate_percent=f"{eff_rate:.2f}",
                discount=discount,
                net_revenue=("■" if net_mode else ""),
                value=f"${value_amount:,.2f}",
                royalty=f"${royalty_amount:,.2f}",
            ))

            lifetime_quantity_by_cat[sd.category] = int(lifetime_before + net_units)
            returns_to_date_by_cat[sd.category] = int(float(returns_to_date_by_cat.get(sd.category, 0)) + rets)

        lb = float(last_balance)
        if lb == 0.0 and advance:
            lb = -float(advance)
        balance = lb + total_royalty
        payable = max(0.0, balance)

        return {
            "categories": [r.dict() for r in rows],
            "summary": PaymentSummary(
                advance_paid=f"${advance:,.2f}",
                royalty_for_period=f"${total_royalty:,.2f}",
                last_period_balance=f"${lb:,.2f}",
                balance=f"${balance:,.2f}",
                amount_payable=f"${payable:,.2f}",
            ).dict(),
            "raw_totals": {
                "advance": float(advance),
                "royalty_total": float(total_royalty),
                "last_balance": float(lb),
                "balance": float(balance),
                "payable": float(payable),
            }
        }

    author_out = calc_party("author", req.author_rates or {}, req.author_advance or 0.0, last_balance_author or 0.0)
    illustrator_out = calc_party("illustrator", req.illustrator_rates or {}, req.illustrator_advance or 0.0, last_balance_illustrator or 0.0)

    return RoyaltyStatement(
        uid=req.uid,
        period_start=req.period_start,
        period_end=req.period_end,
        sales_data=req.sales_data,
        author=author_out,
        illustrator=illustrator_out,
        lifetime_quantity_by_cat=lifetime_quantity_by_cat,
        returns_to_date_by_cat=returns_to_date_by_cat,
    )


# =============================================================================
# App + catalog
# =============================================================================

app = FastAPI(title="Royalties API", version="1.0.0")

# Allow your local UI to call this API
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # tighten for prod
    allow_methods=["*"],
    allow_headers=["*"],
)

BOOKS: List[Book] = []


def _load_books_from_disk() -> List[Dict[str, Any]]:
    path = os.environ.get("BOOKS_JSON", "./books.json")
    p = Path(path)
    if not p.exists():
        return []
    with p.open("r", encoding="utf-8") as fh:
        raw = json.load(fh)
    if isinstance(raw, dict) and "books" in raw and isinstance(raw["books"], list):
        return raw["books"]
    if isinstance(raw, list):
        return raw
    return []


def load_books() -> None:
    global BOOKS
    try:
        raw = _load_books_from_disk()
        BOOKS = [Book(**b) for b in raw if isinstance(b, dict)]
        # Keep only entries that have a uid (source of truth)
        BOOKS = [b for b in BOOKS if (b.uid or "").strip()]
        print(f"[royalties_api] Loaded {len(BOOKS)} books")
    except Exception as e:
        BOOKS = []
        print(f"[royalties_api] Failed to load books: {e}")


load_books()


def find_book_by_uid(uid: str) -> Optional[Book]:
    u = (uid or "").strip()
    if not u:
        return None
    for b in BOOKS:
        if (b.uid or "").strip() == u:
            return b
    return None


# =============================================================================
# Helpers to match the UI shape
# =============================================================================

def _rows_to_titlecase(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for r in rows or []:
        out.append({
            "Category": r.get("category", ""),
            "Units": r.get("units", 0),
            "Returns": r.get("returns", 0),
            "Net Units": r.get("net_units", 0),
            "Lifetime Quantity": r.get("lifetime_quantity", ""),
            "Returns to Date": r.get("returns_to_date", ""),
            "Unit Price": r.get("unit_price", ""),
            "Royalty Rate (%)": r.get("royalty_rate_percent", ""),
            "Discount": r.get("discount", 0),
            "Net Revenue": r.get("net_revenue", ""),
            "Value": r.get("value", ""),
            "Royalty": r.get("royalty", ""),
        })
    return out


def _calc_response_shape(stmt: RoyaltyStatement) -> Dict[str, Any]:
    a = stmt.author or {}
    i = stmt.illustrator or {}

    def totals(block: Dict[str, Any]) -> Dict[str, float]:
        rt = block.get("raw_totals", {}) or {}
        return {
            "advance": float(rt.get("advance", 0.0)),
            "royalty_total": float(rt.get("royalty_total", 0.0)),
            "last_balance": float(rt.get("last_balance", 0.0)),
            "balance": float(rt.get("balance", 0.0)),
            "payable": float(rt.get("payable", 0.0)),
        }

    at = totals(a)
    it = totals(i)

    return {
        "author": {
            **at,
            "categories": _rows_to_titlecase(a.get("categories", [])),
        },
        "illustrator": {
            **it,
            "categories": _rows_to_titlecase(i.get("categories", [])),
        },
    }


# =============================================================================
# Flexible request payload (prevents opaque 422s)
# =============================================================================

class _CalcPayloadFlexible(BaseModel):
    # accept uid or legacy id/book_id; we’ll normalize to uid
    uid: Optional[str] = None
    book_id: Optional[str] = None
    id: Optional[str] = None

    period_start: Optional[str] = None
    period_end: Optional[str] = None

    sales_data: Optional[List[Dict[str, Any]]] = None
    author_rates: Optional[Dict[str, float]] = None
    illustrator_rates: Optional[Dict[str, float]] = None
    author_advance: Optional[float] = 0.0
    illustrator_advance: Optional[float] = 0.0

    class Config:
        extra = Extra.allow


def _require_fields(cp: _CalcPayloadFlexible) -> None:
    missing = []
    if not (cp.uid or cp.book_id or cp.id):
        missing.append("uid (or legacy book_id)")
    if not cp.period_start:
        missing.append("period_start")
    if not cp.period_end:
        missing.append("period_end")
    if not cp.sales_data:
        missing.append("sales_data")
    if missing:
        raise HTTPException(status_code=422, detail=f"Field required: {', '.join(missing)}")


def _normalize_sales_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for row in rows or []:
        if not (row or {}).get("category"):
            continue
        out.append({
            "category": str(row.get("category", "")),
            "units": int(row.get("units", 0) or 0),
            "returns": int(row.get("returns", 0) or 0),
            "unit_price_or_net_revenue": float(row.get("unit_price_or_net_revenue", 0.0) or 0.0),
            "discount": float(row.get("discount", 0.0) or 0.0),
            "net_revenue": bool(row.get("net_revenue", False)),
        })
    return out

def _logo_data_uri() -> str:
    """
    Embed a logo via data URI.
    Set ROYALTY_LOGO_PATH to an image path (png/svg/jpg/webp).
    Fallback: ./static/logo.png next to this file.
    """
    p = os.getenv("ROYALTY_LOGO_PATH") or str((pathlib.Path(__file__).parent / "static" / "logo.png").resolve())
    try:
        with open(p, "rb") as f:
            b64 = base64.b64encode(f.read()).decode("ascii")
        ext = (p.split(".")[-1] or "png").lower()
        if ext not in ("png", "svg", "jpeg", "jpg", "webp"):
            ext = "png"
        return f"data:image/{ext};base64,{b64}"
    except Exception:
        return ""


STATEMENT_HTML = Template(r"""
<!doctype html>
<html>
<head>
  <meta charset="utf-8"/>
  <title>Royalty Statement - {{ party_name }}</title>
  <style>
    @page { size: letter; margin: 1in; }
    body { font-family: system-ui,-apple-system,Segoe UI,Roboto,Arial,sans-serif; color:#111; }
    h1,h2,h3 { margin: 0 0 6px; }
    .row { display:flex; align-items:center; justify-content:space-between; }
    .logo { height: 56px; }
    .muted { color:#555; }
    .tiny { font-size: 11px; color:#666; }
    .section { margin-top: 16px; }
    .box { padding: 12px; border: 1px solid #e5e7eb; border-radius: 8px; }
    .grid2 { display:grid; grid-template-columns: 1.25fr 1fr; gap: 18px; }
    .kvs, .meta { display:grid; grid-template-columns: 1fr 1fr; gap: 6px 12px; font-size: 14px; }
    .kvs div:nth-child(odd), .meta div:nth-child(odd){ color:#444; }
    .strong { font-weight: 600; }

    table { width:100%; border-collapse: collapse; font-size: 13px; margin-top: 6px; }
    th, td { border:1px solid #e5e7eb; padding: 6px 8px; text-align: right; }
    th:first-child, td:first-child { text-align: left; }
    thead th { background:#f8fafc; }
    tfoot td { font-weight:600; background:#fcfcfc; }

    .summary { display:grid; grid-template-columns: repeat(2, 1fr); gap: 8px 16px; }
    .summary div { display:flex; justify-content: space-between; }
  </style>
</head>
<body>
  <div class="row">
    <div>
      <h1>ROYALTY STATEMENT</h1>
      <div class="tiny">ROYALTY PERIOD: {{ period_start }} – {{ period_end }}</div>
    </div>
    {% if logo_data %}
      <img class="logo" src="{{ logo_data }}" alt="Logo"/>
    {% endif %}
  </div>

  <div class="section grid2">
    <!-- Left card: Publisher or Agency header (unchanged logic) -->
    <div class="box">
      {% if agency_name %}
        <h3>{{ agency_name }}</h3>
        {% if agency_addr %}<div class="tiny">{{ agency_addr }}</div>{% endif %}
      {% else %}
        <h3>{{ publisher_name }}</h3>
        {% if publisher_addr %}<div class="tiny">{{ publisher_addr }}</div>{% endif %}
      {% endif %}
    </div>

    <!-- Right card: concise meta (labels left, values right) -->
    <div class="box">
      <div class="meta">
        <div class="muted">Book:</div><div class="strong">{{ book_title }}</div>
        <div class="muted">Statement For:</div><div class="strong">{{ party_name }}</div>
        <div class="muted">Date:</div><div class="strong">{{ statement_date or period_end }}</div>
        {% if isbns %}<div class="muted">ISBN(s):</div><div>{{ isbns }}</div>{% endif %}
        {% if copyright %}<div class="muted">Copyright:</div><div>© {{ copyright }}</div>{% endif %}
      </div>
    </div>
  </div>

  <div class="section">
    <!-- Agency block ABOVE the royalty table, left aligned -->
    {% if agency_name %}
      <div class="tiny" style="margin-bottom:8px;">
        <span class="strong">{{ agency_name }}</span>
        {% if agency_addr %}<span> — {{ agency_addr }}</span>{% endif %}
      </div>
    {% endif %}

    <h3>Detail</h3>
    <table>
      <thead>
        <tr>
          <th>Category</th><th>Units</th><th>Returns</th><th>Net Units</th>
          <th>Lifetime Quantity</th><th>Returns to Date</th><th>Unit Price</th>
          <th>Royalty Rate (%)</th><th>Discount</th><th>Net</th><th>Value</th><th>Royalty</th>
        </tr>
      </thead>
      <tbody>
        {% for r in rows %}
        <tr>
          <td>{{ r.category }}</td>
          <td>{{ r.units }}</td>
          <td>{{ r.returns }}</td>
          <td>{{ r.net_units }}</td>
          <td>{{ r.lifetime_quantity }}</td>
          <td>{{ r.returns_to_date }}</td>
          <td>{{ r.unit_price }}</td>
          <td>{{ r.royalty_rate_percent }}</td>
          <td>{{ "{:.1f}".format(r.discount) }}</td>
          <td style="text-align:center">{{ r.net_revenue }}</td>
          <td>{{ r.value }}</td>
          <td>{{ r.royalty }}</td>
        </tr>
        {% endfor %}
      </tbody>
      <tfoot>
        <tr>
          <td>TOTAL</td>
          <td>{{ totals.units }}</td>
          <td>{{ totals.returns }}</td>
          <td>{{ totals.net_units }}</td>
          <td>{{ totals.lifetime }}</td>
          <td>{{ totals.returns_to_date }}</td>
          <td></td><td></td><td></td><td></td>
          <td>{{ totals.value }}</td>
          <td>{{ totals.royalty }}</td>
        </tr>
      </tfoot>
    </table>
  </div>

  <div class="section box">
    <div class="summary">
      <div><span>Advance Paid</span><span>{{ summary.advance_paid }}</span></div>
      <div><span>Royalty for the Period</span><span>{{ summary.royalty_for_period }}</span></div>
      <div><span>Last Period Balance</span><span>{{ summary.last_period_balance }}</span></div>
      <div><span>Balance</span><span class="strong">{{ summary.balance }}</span></div>
      <div><span>Amount Payable</span><span class="strong">{{ summary.amount_payable }}</span></div>
    </div>
  </div>
</body>
</html>
""")

# =============================================================================
# Endpoints (uid-first)
# =============================================================================

@app.get("/api/royalty/books")
def api_books():
    # return as plain dicts; the UI already knows how to read various keys
    return [b.dict() for b in BOOKS]


@app.get("/api/royalty/categories")
def api_categories():
    cats: set[str] = set()
    for b in BOOKS:
        # rich schema
        if b.royalties:
            for fr in (b.royalties.author.first_rights or []):
                if fr.format:
                    cats.add(fr.format)
            for fr in (b.royalties.illustrator.first_rights or []):
                if fr.format:
                    cats.add(fr.format)
        # legacy schema (handle both dict and model gracefully)
        for r in (b.author_royalty or []):
            cat = r.category if isinstance(r, RoyaltyRate) else (r or {}).get("category")
            if cat:
                cats.add(cat)
        for r in (b.illustrator_royalty or []):
            cat = r.category if isinstance(r, RoyaltyRate) else (r or {}).get("category")
            if cat:
                cats.add(cat)
        # formats (just in case)
        for f in (b.formats or []):
            fmt = f.format if isinstance(f, BookFormat) else (f or {}).get("format") or (f or {}).get("Format")
            if fmt:
                cats.add(fmt)
    return {"categories": sorted(cats)}


@app.post("/api/royalty/calculate")
def api_calculate(payload: _CalcPayloadFlexible = Body(...)):
    _require_fields(payload)

    uid = (payload.uid or payload.book_id or payload.id or "").strip()
    book = find_book_by_uid(uid)
    if not book:
        sample = ", ".join([b.uid for b in BOOKS[:8] if b.uid] or ["<no books loaded>"])
        raise HTTPException(status_code=404, detail=f"Book not found for uid '{uid}'. Known uids (sample): {sample}")

    req = RoyaltyStatementRequest(
        uid=uid,
        period_start=str(payload.period_start),
        period_end=str(payload.period_end),
        sales_data=[SalesData(**r) for r in _normalize_sales_rows(payload.sales_data or [])],
        author_rates=payload.author_rates or {},
        illustrator_rates=payload.illustrator_rates or {},
        author_advance=float(payload.author_advance or 0.0),
        illustrator_advance=float(payload.illustrator_advance or 0.0),
    )

    lifetime: Dict[str, int] = {}
    returns_to_date: Dict[str, int] = {}

    stmt = calculate_statement(book, req, lifetime, returns_to_date)
    return {"ok": True, "calculations": _calc_response_shape(stmt)}


@app.post("/api/royalty/statements")
def api_save_statement(payload: _CalcPayloadFlexible = Body(...)):
    # reuse logic from /calculate; pretend we saved successfully
    out = api_calculate(payload)
    return {
        "ok": True,
        "uid": (payload.uid or payload.book_id or payload.id or "").strip(),
        "calculations": out["calculations"],
    }
@app.post("/api/royalty/render")
def api_render_statement(
    payload: _CalcPayloadFlexible = Body(...),
    format: str = Query("html", pattern="^(html|pdf)$"),
    party: str = Query("author", pattern="^(author|illustrator)$"),
):
    # Reuse validation + normalization from /calculate for consistency
    _require_fields(payload)

    uid = (payload.uid or payload.book_id or payload.id or "").strip()
    book = find_book_by_uid(uid)
    if not book:
        raise HTTPException(status_code=404, detail=f"Book not found for uid '{uid}'.")

    req = RoyaltyStatementRequest(
        uid=uid,
        period_start=str(payload.period_start),
        period_end=str(payload.period_end),
        sales_data=[SalesData(**r) for r in _normalize_sales_rows(payload.sales_data or [])],
        author_rates=payload.author_rates or {},
        illustrator_rates=payload.illustrator_rates or {},
        author_advance=float(payload.author_advance or 0.0),
        illustrator_advance=float(payload.illustrator_advance or 0.0),
    )

    lifetime: Dict[str, int] = {}
    returns_to_date: Dict[str, int] = {}
    stmt = calculate_statement(book, req, lifetime, returns_to_date)

    side = (stmt.author if party == "author" else stmt.illustrator) or {}
    rows = side.get("categories", [])
    summary = side.get("summary", {})
    totals = _totals_for_rows(rows)

    html = STATEMENT_HTML.render(
        logo_data=_logo_data_uri(),
        publisher_name="Marble Press",   # customize if you store publisher name
        publisher_addr="",
        agency_name=(getattr(book, "author_agent", None).name if party=="author" and getattr(book, "author_agent", None) else ""),
        agency_addr=(getattr(book, "author_agent", None).address if party=="author" and getattr(book, "author_agent", None) else ""),
        party_name=(book.author if party=="author" else (getattr(book, "illustrator", None).name if getattr(book, "illustrator", None) else "")) or "Party",
        book_title=(book.title or "(Untitled)"),
        isbns=_collect_isbns(book),
        copyright=book.publishing_year or "",
        period_start=stmt.period_start,
        period_end=stmt.period_end,
        rows=rows,
        totals=totals,
        summary=summary,
    )

    if format == "html":
        return HTMLResponse(content=html, status_code=200)

    # PDF (WeasyPrint preferred; xhtml2pdf fallback; else return HTML)
    try:
        from weasyprint import HTML as WHTML
        pdf_bytes = WHTML(string=html).write_pdf()
        return Response(
            content=pdf_bytes,
            media_type="application/pdf",
            headers={"Content-Disposition": f'attachment; filename="royalty_statement_{uid}_{party}.pdf"'}
        )
    except Exception:
        try:
            from xhtml2pdf import pisa
            mem = io.BytesIO()
            pisa.CreatePDF(src=html, dest=mem)
            return Response(
                content=mem.getvalue(),
                media_type="application/pdf",
                headers={"Content-Disposition": f'attachment; filename="royalty_statement_{uid}_{party}.pdf"'}
            )
        except Exception:
            return HTMLResponse(content=html, status_code=200)

