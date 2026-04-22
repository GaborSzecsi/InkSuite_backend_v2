"""
First-rights royalty statement generation from royalty_sales_lines + royalty_rules/tiers.
Uses Decimal throughout. Persists frozen lines to royalty_statement_lines and header to royalty_statements.
"""
from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, ROUND_HALF_UP
from typing import Any, Dict, List, Optional, Sequence, Tuple
import uuid as uuid_lib

from routers.catalog import (
    _build_full_work_payload,
    _fetch_party_core,
    _fetch_party_address_lines,
)

import psycopg
from psycopg.rows import dict_row

MONEY_QUANT = Decimal("0.01")
RATE_QUANT = Decimal("0.000001")
TWO_THIRDS = Decimal("0.666666666667")


class StatementEngineError(Exception):
    """Base class for statement generation failures."""


class StatementValidationError(StatementEngineError):
    """User-fixable validation (missing price, rule, ambiguous tier, etc.)."""


@dataclass(frozen=True)
class PeriodRow:
    id: str
    period_code: str
    period_start: Any
    period_end: Any


@dataclass
class SalesBucket:
    edition_id: str
    category_label: str
    units_sold: Decimal
    units_returned: Decimal
    publisher_receipts: Decimal
    discount_weighted_num: Decimal
    discount_weight_den: Decimal
    royalty_stream: str = "first_rights"


@dataclass
class RuleRow:
    id: str
    format_label: str
    party: str
    rights_type: str
    mode: str
    base: str
    escalating: bool
    flat_rate_percent: Optional[Decimal]
    percent: Optional[Decimal]


@dataclass
class TierRow:
    id: str
    rule_id: str
    tier_order: int
    rate_percent: Decimal
    base: str


@dataclass
class TierConditionRow:
    tier_id: str
    kind: str
    comparator: str
    value: Decimal


def _d(val: Any) -> Decimal:
    if val is None:
        return Decimal("0")
    if isinstance(val, Decimal):
        return val
    return Decimal(str(val))


def _money(val: Decimal) -> Decimal:
    return val.quantize(MONEY_QUANT, rounding=ROUND_HALF_UP)


def _rate(val: Decimal) -> Decimal:
    return val.quantize(RATE_QUANT, rounding=ROUND_HALF_UP)


def _norm_key(s: str) -> str:
    return " ".join((s or "").strip().lower().split())


def _safe_str(v: Any) -> str:
    return str(v).strip() if v is not None else ""


def _pick_party_block(work_payload: Dict[str, Any], party: str) -> Dict[str, Any]:
    if party == "illustrator":
        blk = work_payload.get("illustrator")
        return blk if isinstance(blk, dict) else {}
    blk = work_payload.get("author")
    return blk if isinstance(blk, dict) else {}

def _load_prior_earned_to_date(
    cur,
    tenant_id: str,
    work_id: str,
    party: str,
    period_end,
) -> Decimal:
    cur.execute(
        """
        SELECT COALESCE(SUM(COALESCE(rs.earned_this_period, 0)), 0) AS total
        FROM royalty_statements rs
        JOIN royalty_periods rp
          ON rp.id = rs.period_id
        WHERE rs.tenant_id = %s::uuid
          AND rs.work_id = %s::uuid
          AND rs.party = %s
          AND rp.period_end < %s
        """,
        (tenant_id, work_id, party, period_end),
    )
    row = cur.fetchone()
    return _d((row or {}).get("total"))


def _collect_statement_isbns(work_payload: Dict[str, Any]) -> List[str]:
    seen = set()
    out: List[str] = []

    for fmt in (work_payload.get("formats") or []):
        if not isinstance(fmt, dict):
            continue
        isbn = _safe_str(fmt.get("isbn"))
        if isbn and isbn not in seen:
            seen.add(isbn)
            out.append(isbn)

    for ed in (work_payload.get("_editions") or []):
        if not isinstance(ed, dict):
            continue
        for key in ("isbn", "isbn13"):
            isbn = _safe_str(ed.get(key))
            if isbn and isbn not in seen:
                seen.add(isbn)
                out.append(isbn)

    return out


def _load_payment_instruction(
    cur,
    tenant_id: str,
    royalty_set_id: str,
    party: str,
) -> Dict[str, Any]:
    try:
        cur.execute(
            """
            SELECT
                payee_mode,
                contributor_party_id,
                agency_party_id,
                contributor_percent,
                agency_percent
            FROM royalty_payment_instructions
            WHERE tenant_id = %s::uuid
              AND royalty_set_id = %s::uuid
              AND party = %s::roy_party
            LIMIT 1
            """,
            (tenant_id, royalty_set_id, party),
        )
        row = cur.fetchone()
    except Exception:
        row = None

    if not row:
        return {}

    contributor_party_id = str(row["contributor_party_id"]) if row.get("contributor_party_id") else None
    agency_party_id = str(row["agency_party_id"]) if row.get("agency_party_id") else None

    contributor_core = (
        _fetch_party_core(cur, tenant_id, contributor_party_id)
        if contributor_party_id else {}
    )
    agency_core = (
        _fetch_party_core(cur, tenant_id, agency_party_id)
        if agency_party_id else {}
    )

    contributor_address_lines = (
        _fetch_party_address_lines(cur, tenant_id, contributor_party_id)
        if contributor_party_id else []
    )
    agency_address_lines = (
        _fetch_party_address_lines(cur, tenant_id, agency_party_id)
        if agency_party_id else []
    )

    return {
        "payee_mode": _safe_str(row.get("payee_mode")) or "contributor_only",
        "contributor_percent": str(row["contributor_percent"]) if row.get("contributor_percent") is not None else None,
        "agency_percent": str(row["agency_percent"]) if row.get("agency_percent") is not None else None,
        "contributor_party_id": contributor_party_id,
        "agency_party_id": agency_party_id,
        "contributor_name": _safe_str(contributor_core.get("display_name")),
        "agency_name": _safe_str(agency_core.get("display_name")),
        "contributor_address_lines": contributor_address_lines,
        "agency_address_lines": agency_address_lines,
    }


def load_period(cur, tenant_id: str, period_id: str) -> PeriodRow:
    cur.execute(
        """
        SELECT id, period_code, period_start, period_end
        FROM royalty_periods
        WHERE tenant_id = %s::uuid AND id = %s::uuid
        LIMIT 1
        """,
        (tenant_id, period_id),
    )
    r = cur.fetchone()
    if not r:
        raise StatementValidationError(f"Royalty period not found: {period_id}")
    return PeriodRow(
        id=str(r["id"]),
        period_code=str(r.get("period_code") or ""),
        period_start=r["period_start"],
        period_end=r["period_end"],
    )


def resolve_period_id_for_generate(
    cur,
    tenant_id: str,
    period_id: Optional[str],
    period_start: Optional[str],
    period_end: Optional[str],
) -> str:
    pid = (period_id or "").strip()
    if pid:
        load_period(cur, tenant_id, pid)
        return pid

    ps = (period_start or "").strip()
    pe = (period_end or "").strip()
    if not ps or not pe:
        raise StatementValidationError(
            "period_id or both period_start and period_end are required"
        )

    cur.execute(
        """
        SELECT id::text
        FROM royalty_periods
        WHERE tenant_id = %s::uuid
          AND period_start = %s::date
          AND period_end = %s::date
        LIMIT 1
        """,
        (tenant_id, ps, pe),
    )
    row = cur.fetchone()
    if not row:
        raise StatementValidationError(
            f"No royalty_periods row for period_start={ps!r} period_end={pe!r}"
        )
    return str(row["id"])


def assert_royalty_set_for_work(cur, tenant_id: str, royalty_set_id: str, work_id: str) -> None:
    cur.execute(
        """
        SELECT id, work_id
        FROM royalty_sets
        WHERE tenant_id = %s::uuid AND id = %s::uuid
        LIMIT 1
        """,
        (tenant_id, royalty_set_id),
    )
    r = cur.fetchone()
    if not r:
        raise StatementValidationError(f"Royalty set not found: {royalty_set_id}")
    if str(r["work_id"]) != str(work_id):
        raise StatementValidationError("royalty_set_id does not belong to the given work_id")


def assert_work(cur, tenant_id: str, work_id: str) -> None:
    cur.execute(
        "SELECT id FROM works WHERE tenant_id = %s::uuid AND id = %s::uuid LIMIT 1",
        (tenant_id, work_id),
    )
    if not cur.fetchone():
        raise StatementValidationError(f"Work not found: {work_id}")


def load_sales_rows_for_period(
    cur,
    tenant_id: str,
    work_id: str,
    period: PeriodRow,
) -> List[Dict[str, Any]]:
    params = (
        tenant_id,
        work_id,
        period.period_start,
        period.period_end,
        period.period_end,
        period.period_start,
        period.period_start,
        period.period_end,
    )

    sql = """
        SELECT
            r.id,
            r.edition_id,
            COALESCE(r.units_sold, 0)::text AS units_sold,
            COALESCE(r.units_returned, 0)::text AS units_returned,
            COALESCE(r.discount_percent, 0)::text AS discount_percent,
            COALESCE(r.publisher_receipts, 0)::text AS publisher_receipts,
            COALESCE(r.gross_sales, 0)::text AS gross_sales,
            COALESCE(r.royalty_stream, 'first_rights') AS royalty_stream,
            e.product_form,
            e.product_form_detail
        FROM royalty_sales_lines r
        JOIN editions e
          ON e.id = r.edition_id
         AND e.tenant_id = r.tenant_id
        LEFT JOIN royalty_periods rp
          ON rp.id = r.period_id
        WHERE r.tenant_id = %s::uuid
          AND e.work_id = %s::uuid
          AND (
                (
                  r.transaction_date IS NOT NULL
                  AND r.transaction_date::date BETWEEN %s AND %s
                )
                OR
                (
                  rp.id IS NOT NULL
                  AND rp.period_start <= %s
                  AND rp.period_end >= %s
                )
                OR
                (
                  r.transaction_date IS NULL
                  AND rp.id IS NULL
                  AND (r.created_at AT TIME ZONE 'UTC')::date BETWEEN %s AND %s
                )
              )
    """
    try:
        cur.execute(sql, params)
    except psycopg.errors.UndefinedColumn:
        cur.execute(
            """
            SELECT
                r.id,
                r.edition_id,
                COALESCE(r.units_sold, 0)::text AS units_sold,
                COALESCE(r.units_returned, 0)::text AS units_returned,
                COALESCE(r.discount_percent, 0)::text AS discount_percent,
                COALESCE(r.publisher_receipts, 0)::text AS publisher_receipts,
                COALESCE(r.gross_sales, 0)::text AS gross_sales,
                'first_rights' AS royalty_stream,
                e.product_form,
                e.product_form_detail
            FROM royalty_sales_lines r
            JOIN editions e
              ON e.id = r.edition_id
             AND e.tenant_id = r.tenant_id
            LEFT JOIN royalty_periods rp
              ON rp.id = r.period_id
            WHERE r.tenant_id = %s::uuid
              AND e.work_id = %s::uuid
              AND (
                    (
                      rp.id IS NOT NULL
                      AND rp.period_start <= %s
                      AND rp.period_end >= %s
                    )
                    OR
                    (
                      rp.id IS NULL
                      AND (r.created_at AT TIME ZONE 'UTC')::date BETWEEN %s AND %s
                    )
                  )
            """,
            (
                tenant_id,
                work_id,
                period.period_end,
                period.period_start,
                period.period_start,
                period.period_end,
            ),
        )

    return list(cur.fetchall() or [])


def edition_category_label(product_form: Any, product_form_detail: Any) -> str:
    d = str(product_form_detail or "").strip()
    if d:
        return d
    p = str(product_form or "").strip()
    if p.lower() in ("e-book", "ebook"):
        return "E-book"
    return p


def aggregate_sales_into_buckets(
    rows: Sequence[Dict[str, Any]],
    *,
    royalty_stream: str = "first_rights",
) -> List[SalesBucket]:
    buckets: Dict[Tuple[str, str, str], SalesBucket] = {}
    for r in rows:
        eid = str(r["edition_id"])
        cat = edition_category_label(r.get("product_form"), r.get("product_form_detail"))
        sold = _d(r.get("units_sold"))
        ret = _d(r.get("units_returned"))
        disc = _d(r.get("discount_percent"))
        receipts = _d(r.get("publisher_receipts"))
        row_stream = _safe_str(r.get("royalty_stream")) or royalty_stream
        key = (eid, cat, row_stream)

        if key not in buckets:
            buckets[key] = SalesBucket(
                edition_id=eid,
                category_label=cat,
                units_sold=Decimal("0"),
                units_returned=Decimal("0"),
                publisher_receipts=Decimal("0"),
                discount_weighted_num=Decimal("0"),
                discount_weight_den=Decimal("0"),
                royalty_stream=row_stream,
            )

        b = buckets[key]
        b.units_sold += sold
        b.units_returned += ret
        b.publisher_receipts += receipts
        if sold > 0:
            b.discount_weighted_num += sold * disc
            b.discount_weight_den += sold
    return list(buckets.values())


def fetch_edition_us_list_price(cur, tenant_id: str, edition_id: str) -> Decimal:
    cur.execute(
        """
        SELECT ep.price_amount
        FROM edition_supply_details sd
        JOIN edition_prices ep ON ep.supply_detail_id = sd.id
        WHERE sd.tenant_id = %s::uuid
          AND sd.edition_id = %s::uuid
          AND upper(COALESCE(ep.currency_code, '')) = 'USD'
        ORDER BY ep.id DESC
        LIMIT 1
        """,
        (tenant_id, edition_id),
    )
    row = cur.fetchone()
    if not row or row.get("price_amount") is None:
        raise StatementValidationError(
            f"Missing US list price for edition_id={edition_id} (required for list_price basis)"
        )
    return _d(row["price_amount"])


def load_first_rights_rules(cur, tenant_id: str, royalty_set_id: str, party: str) -> List[RuleRow]:
    cur.execute(
        """
        SELECT
            id,
            format_label,
            party::text AS party,
            rights_type::text AS rights_type,
            COALESCE(mode::text, '') AS mode,
            base::text AS base,
            COALESCE(escalating, false) AS escalating,
            flat_rate_percent,
            percent
        FROM royalty_rules
        WHERE tenant_id = %s::uuid
          AND royalty_set_id = %s::uuid
          AND party = %s::roy_party
          AND rights_type = 'first_rights'::roy_rights_type
        ORDER BY id ASC
        """,
        (tenant_id, royalty_set_id, party),
    )
    out: List[RuleRow] = []
    for r in cur.fetchall() or []:
        out.append(
            RuleRow(
                id=str(r["id"]),
                format_label=str(r.get("format_label") or ""),
                party=str(r.get("party") or ""),
                rights_type=str(r.get("rights_type") or ""),
                mode=str(r.get("mode") or ""),
                base=str(r.get("base") or "list_price"),
                escalating=bool(r.get("escalating")),
                flat_rate_percent=_d(r["flat_rate_percent"]) if r.get("flat_rate_percent") is not None else None,
                percent=_d(r["percent"]) if r.get("percent") is not None else None,
            )
        )
    return out


def load_subrights_rules(cur, tenant_id: str, royalty_set_id: str, party: str) -> List[RuleRow]:
    cur.execute(
        """
        SELECT
            rr.id,
            st.name AS format_label,
            rr.party::text AS party,
            rr.rights_type::text AS rights_type,
            COALESCE(rr.mode::text, '') AS mode,
            rr.base::text AS base,
            COALESCE(rr.escalating, false) AS escalating,
            rr.flat_rate_percent,
            rr.percent
        FROM royalty_rules rr
        JOIN subrights_types st
          ON st.id = rr.subrights_type_id
        WHERE rr.tenant_id = %s::uuid
          AND rr.royalty_set_id = %s::uuid
          AND rr.party = %s::roy_party
          AND rr.rights_type = 'subrights'::roy_rights_type
          AND rr.subrights_type_id IS NOT NULL
        ORDER BY st.name ASC, rr.id ASC
        """,
        (tenant_id, royalty_set_id, party),
    )
    out: List[RuleRow] = []
    for r in cur.fetchall() or []:
        out.append(
            RuleRow(
                id=str(r["id"]),
                format_label=str(r.get("format_label") or ""),
                party=str(r.get("party") or ""),
                rights_type=str(r.get("rights_type") or ""),
                mode=str(r.get("mode") or ""),
                base=str(r.get("base") or "net_receipts"),
                escalating=bool(r.get("escalating")),
                flat_rate_percent=_d(r["flat_rate_percent"]) if r.get("flat_rate_percent") is not None else None,
                percent=_d(r["percent"]) if r.get("percent") is not None else None,
            )
        )
    return out


def load_subrights_income_rows_for_period(
    cur,
    tenant_id: str,
    work_id: str,
    royalty_set_id: str,
    period: PeriodRow,
) -> List[Dict[str, Any]]:
    cur.execute(
        """
        SELECT
            sil.id,
            sil.subrights_type_id::text AS subrights_type_id,
            st.name AS subrights_name,
            COALESCE(sil.publisher_receipts, 0)::text AS publisher_receipts,
            COALESCE(sil.gross_amount, 0)::text AS gross_amount,
            sil.income_date,
            sil.created_at
        FROM subrights_income_lines sil
        JOIN subrights_types st
          ON st.id = sil.subrights_type_id
        WHERE sil.tenant_id = %s::uuid
          AND sil.work_id = %s::uuid
          AND sil.royalty_set_id = %s::uuid
          AND (
            sil.period_id = %s::uuid
            OR (
              sil.period_id IS NULL
              AND COALESCE(
                    sil.income_date,
                    (sil.created_at AT TIME ZONE 'UTC')::date
                  ) BETWEEN %s AND %s
            )
          )
        ORDER BY st.name ASC, sil.id ASC
        """,
        (tenant_id, work_id, royalty_set_id, period.id, period.period_start, period.period_end),
    )
    return list(cur.fetchall() or [])


def load_tiers_for_rules(
    cur,
    tenant_id: str,
    rule_ids: Sequence[str],
) -> Tuple[Dict[str, List[TierRow]], Dict[str, List[TierConditionRow]]]:
    if not rule_ids:
        return {}, {}

    cur.execute(
        """
        SELECT id, rule_id, tier_order, rate_percent, base::text AS base
        FROM royalty_tiers
        WHERE tenant_id = %s::uuid
          AND rule_id::text = ANY(%s)
        ORDER BY rule_id ASC, tier_order ASC, id ASC
        """,
        (tenant_id, list(rule_ids)),
    )
    tiers_by_rule: Dict[str, List[TierRow]] = {}
    for r in cur.fetchall() or []:
        rid = str(r["rule_id"])
        tiers_by_rule.setdefault(rid, []).append(
            TierRow(
                id=str(r["id"]),
                rule_id=rid,
                tier_order=int(r.get("tier_order") or 0),
                rate_percent=_d(r.get("rate_percent")),
                base=str(r.get("base") or "list_price"),
            )
        )

    all_tier_ids = [t.id for ts in tiers_by_rule.values() for t in ts]
    conds_by_tier: Dict[str, List[TierConditionRow]] = {}
    if all_tier_ids:
        cur.execute(
            """
            SELECT tier_id, kind::text AS kind, comparator::text AS comparator, value
            FROM royalty_tier_conditions
            WHERE tenant_id = %s::uuid
              AND tier_id::text = ANY(%s)
            ORDER BY tier_id ASC, id ASC
            """,
            (tenant_id, all_tier_ids),
        )
        for r in cur.fetchall() or []:
            tid = str(r["tier_id"])
            conds_by_tier.setdefault(tid, []).append(
                TierConditionRow(
                    tier_id=tid,
                    kind=str(r.get("kind") or "units"),
                    comparator=str(r.get("comparator") or "<"),
                    value=_d(r.get("value")),
                )
            )
    return tiers_by_rule, conds_by_tier


def pick_rule_for_category(rules: Sequence[RuleRow], party: str, category_label: str) -> RuleRow:
    ck = _norm_key(category_label)
    matches = [
        r for r in rules
        if _norm_key(r.format_label) == ck and _norm_key(r.party) == _norm_key(party)
    ]
    if not matches:
        raise StatementValidationError(
            f"No first_rights royalty rule for party={party!r} format_label matching {category_label!r}"
        )
    if len(matches) > 1:
        raise StatementValidationError(
            f"Ambiguous royalty rules for format {category_label!r}: {[m.id for m in matches]}"
        )
    return matches[0]


def pick_subrights_rule_for_name(rules: Sequence[RuleRow], party: str, subrights_name: str) -> RuleRow:
    ck = _norm_key(subrights_name)
    matches = [
        r for r in rules
        if _norm_key(r.format_label) == ck and _norm_key(r.party) == _norm_key(party)
    ]
    if not matches:
        raise StatementValidationError(
            f"No subrights royalty rule for party={party!r} subright matching {subrights_name!r}"
        )
    if len(matches) > 1:
        raise StatementValidationError(
            f"Ambiguous subrights rules for {subrights_name!r}: {[m.id for m in matches]}"
        )
    return matches[0]


def _cmp(left: Decimal, op: str, right: Decimal) -> bool:
    if op == "<":
        return left < right
    if op == "<=":
        return left <= right
    if op == ">":
        return left > right
    if op == ">=":
        return left >= right
    if op in ("=", "=="):
        return left == right
    if op in ("!=", "<>"):
        return left != right
    return False


def tier_matches(
    tier: TierRow,
    conditions: Sequence[TierConditionRow],
    net_units: Decimal,
    discount_avg: Decimal,
) -> bool:
    for c in conditions:
        if c.kind == "discount":
            lhs = discount_avg
        elif c.kind == "units":
            lhs = net_units
        else:
            raise StatementValidationError(f"Unknown tier condition kind: {c.kind!r}")
        if not _cmp(lhs, c.comparator, c.value):
            return False
    return True


def _format_condition_text(conditions: Sequence[TierConditionRow]) -> str:
    if not conditions:
        return ""
    return "; ".join(
        f"{c.kind} {c.comparator} {c.value.normalize() if c.value == c.value.to_integral() else c.value}"
        for c in conditions
    )


def select_applied_tier(
    rule: RuleRow,
    tiers: Sequence[TierRow],
    conds_by_tier: Dict[str, List[TierConditionRow]],
    net_units: Decimal,
    discount_avg: Decimal,
) -> Tuple[Optional[TierRow], Decimal, str]:
    ordered = sorted(tiers, key=lambda t: (t.tier_order, t.id))
    if not ordered:
        rate = rule.flat_rate_percent if rule.flat_rate_percent is not None else rule.percent
        if rate is None:
            raise StatementValidationError(
                f"Rule {rule.id} has no tiers and no flat_rate_percent/percent"
            )
        return None, _rate(rate), rule.base

    matching = []
    for t in ordered:
        conds = conds_by_tier.get(t.id, [])
        if tier_matches(t, conds, net_units, discount_avg):
            matching.append(t)

    if not matching:
        raise StatementValidationError(
            f"No royalty tier matched for rule {rule.id} (net_units={net_units}, discount_avg={discount_avg})"
        )
    if len(matching) > 1:
        raise StatementValidationError(
            f"Ambiguous tier selection for rule {rule.id}: tiers={[m.id for m in matching]}"
        )

    chosen = matching[0]
    return chosen, _rate(chosen.rate_percent), chosen.base


def total_recoupable_advances(cur, tenant_id: str, royalty_set_id: str, party: str) -> Decimal:
    cur.execute(
        """
        SELECT COALESCE(SUM(amount), 0)::text AS s
        FROM advances
        WHERE tenant_id = %s::uuid
          AND royalty_set_id = %s::uuid
          AND party = %s::roy_party
          AND COALESCE(recoupable, false) = true
        """,
        (tenant_id, royalty_set_id, party),
    )
    row = cur.fetchone()
    return _d(row.get("s") if row else 0)


def load_previous_closing_recoupment(
    cur,
    tenant_id: str,
    work_id: str,
    party: str,
    current_period_end: Any,
) -> Optional[Decimal]:
    cur.execute(
        """
        SELECT rs.closing_recoupment_balance::text
        FROM royalty_statements rs
        JOIN royalty_periods rp
          ON rp.id = rs.period_id
        WHERE rs.tenant_id = %s::uuid
          AND rs.work_id = %s::uuid
          AND rs.party = %s::roy_party
          AND rp.period_end < %s
        ORDER BY rp.period_end DESC, rs.created_at DESC
        LIMIT 1
        """,
        (tenant_id, work_id, party, current_period_end),
    )
    row = cur.fetchone()
    if not row or row.get("closing_recoupment_balance") is None:
        return None
    return _d(row["closing_recoupment_balance"])


def find_existing_statement(
    cur,
    tenant_id: str,
    work_id: str,
    royalty_set_id: str,
    party: str,
    period_id: str,
) -> Optional[Tuple[str, str]]:
    cur.execute(
        """
        SELECT id::text, status
        FROM royalty_statements
        WHERE tenant_id = %s::uuid
          AND work_id = %s::uuid
          AND party = %s
          AND period_id = %s::uuid
        LIMIT 1
        """,
        (tenant_id, work_id, party, period_id),
    )
    row = cur.fetchone()
    if not row:
        return None
    return str(row["id"]), str(row.get("status") or "draft")


def compute_header_amounts(
    opening_recoupment_balance: Decimal,
    earned_this_period: Decimal,
    adjustments_this_period: Decimal,
) -> Tuple[Decimal, Decimal, Decimal, Decimal]:
    avail = opening_recoupment_balance + earned_this_period + adjustments_this_period
    closing = avail if avail < 0 else Decimal("0")
    payable = avail if avail > 0 else Decimal("0")
    if opening_recoupment_balance < 0:
        recouped = min(
            earned_this_period + adjustments_this_period,
            abs(opening_recoupment_balance),
        )
    else:
        recouped = Decimal("0")
    return _money(closing), _money(payable), _money(recouped), _money(avail)


def _derive_canada_or_export_rate(
    *,
    rule: RuleRow,
    applied_tier: Optional[TierRow],
    rate_pct: Decimal,
    conds_by_tier: Dict[str, List[TierConditionRow]],
    stream: str,
) -> Tuple[Decimal, Optional[Decimal], Optional[str], Optional[str]]:
    derived_rate = _rate(rate_pct * TWO_THIRDS)
    source_us_rate_percent = _rate(rate_pct)

    source_us_condition_text: Optional[str] = None
    if applied_tier:
        conds = conds_by_tier.get(applied_tier.id, [])
        source_us_condition_text = _format_condition_text(conds) or None

    if stream == "export_derived":
        formula = f"2/3 × {source_us_rate_percent}%"
    else:
        formula = f"2/3 × {source_us_rate_percent}%"

    return derived_rate, source_us_rate_percent, source_us_condition_text, formula


def generate_statement(
    cur,
    *,
    tenant_id: str,
    work_id: str,
    royalty_set_id: str,
    party: str,
    period_id: Optional[str] = None,
    period_start: Optional[str] = None,
    period_end: Optional[str] = None,
    rebuild: bool = False,
    status: str = "draft",
    adjustments_this_period: Decimal = Decimal("0"),
) -> Dict[str, Any]:
    if party not in ("author", "illustrator"):
        raise StatementValidationError("party must be 'author' or 'illustrator'")

    assert_work(cur, tenant_id, work_id)
    assert_royalty_set_for_work(cur, tenant_id, royalty_set_id, work_id)
    resolved_period_id = resolve_period_id_for_generate(
        cur, tenant_id, period_id, period_start, period_end
    )
    period = load_period(cur, tenant_id, resolved_period_id)

    existing = find_existing_statement(
        cur, tenant_id, work_id, royalty_set_id, party, resolved_period_id
    )
    if existing:
        stmt_id, st = existing

        if st == "final":
            raise StatementValidationError(
                "Final statement already exists for this work, period, and party. "
                "Use the audit workflow to revise it."
            )
    else:
        stmt_id = str(uuid_lib.uuid4())

    first_rights_rules = load_first_rights_rules(cur, tenant_id, royalty_set_id, party)
    first_rule_ids = [r.id for r in first_rights_rules]
    tiers_by_rule, conds_by_tier = load_tiers_for_rules(cur, tenant_id, first_rule_ids)

    subrights_rules = load_subrights_rules(cur, tenant_id, royalty_set_id, party)

    raw_sales = load_sales_rows_for_period(cur, tenant_id, work_id, period)

    ordinary_sales = [
        r for r in raw_sales
        if (_safe_str(r.get("royalty_stream")) or "first_rights") == "first_rights"
    ]
    canada_sales = [
        r for r in raw_sales
        if _safe_str(r.get("royalty_stream")) == "canada_derived"
    ]
    export_sales = [
        r for r in raw_sales
        if _safe_str(r.get("royalty_stream")) == "export_derived"
    ]

    buckets = aggregate_sales_into_buckets(ordinary_sales, royalty_stream="first_rights")
    canada_buckets = aggregate_sales_into_buckets(canada_sales, royalty_stream="canada_derived")
    export_buckets = aggregate_sales_into_buckets(export_sales, royalty_stream="export_derived")

    subrights_income_rows = load_subrights_income_rows_for_period(
        cur, tenant_id, work_id, royalty_set_id, period
    )

    line_rows: List[Dict[str, Any]] = []
    earned_total = Decimal("0")

    # Ordinary first-rights
    for b in buckets:
        net_u = b.units_sold - b.units_returned
        disc_avg = (
            (b.discount_weighted_num / b.discount_weight_den)
            if b.discount_weight_den > 0
            else Decimal("0")
        )

        rule = pick_rule_for_category(first_rights_rules, party, b.category_label)
        tiers = tiers_by_rule.get(rule.id, [])
        applied_tier, rate_pct, applied_base = select_applied_tier(
            rule, tiers, conds_by_tier, net_u, disc_avg
        )

        if applied_base == "list_price":
            list_px = fetch_edition_us_list_price(cur, tenant_id, b.edition_id)
            basis = _money(net_u * list_px)
            frozen_px = _money(list_px)
        elif applied_base == "net_receipts":
            basis = _money(b.publisher_receipts)
            frozen_px = None
        else:
            raise StatementValidationError(f"Unknown applied base: {applied_base!r}")

        royalty_amt = _money(basis * rate_pct / Decimal("100"))
        earned_total += royalty_amt

        line_rows.append(
            {
                "line_type": "first_rights",
                "category_label": b.category_label,
                "edition_id": b.edition_id,
                "units_sold": b.units_sold,
                "units_returned": b.units_returned,
                "net_units": net_u,
                "basis_amount": basis,
                "royalty_rate": rate_pct,
                "royalty_amount": royalty_amt,
                "applied_rule_id": rule.id,
                "applied_tier_id": applied_tier.id if applied_tier else None,
                "frozen_list_price_usd": frozen_px,
                "display_section": "first_rights",
                "royalty_stream": "first_rights",
                "source_us_rate_percent": None,
                "source_us_condition_text": None,
                "derived_rate_formula": None,
            }
        )

    # Canada / Export derived rows
    for derived_bucket in [*canada_buckets, *export_buckets]:
        net_u = derived_bucket.units_sold - derived_bucket.units_returned
        disc_avg = (
            (derived_bucket.discount_weighted_num / derived_bucket.discount_weight_den)
            if derived_bucket.discount_weight_den > 0
            else Decimal("0")
        )

        rule = pick_rule_for_category(first_rights_rules, party, derived_bucket.category_label)
        tiers = tiers_by_rule.get(rule.id, [])
        applied_tier, us_rate_pct, _applied_base = select_applied_tier(
            rule, tiers, conds_by_tier, net_u, disc_avg
        )

        derived_rate, source_us_rate_percent, source_us_condition_text, formula = _derive_canada_or_export_rate(
            rule=rule,
            applied_tier=applied_tier,
            rate_pct=us_rate_pct,
            conds_by_tier=conds_by_tier,
            stream=derived_bucket.royalty_stream,
        )

        basis = _money(derived_bucket.publisher_receipts)
        royalty_amt = _money(basis * derived_rate / Decimal("100"))
        earned_total += royalty_amt

        display_label = (
            f"Export - {derived_bucket.category_label}"
            if derived_bucket.royalty_stream == "export_derived"
            else f"Canada - {derived_bucket.category_label}"
        )

        line_rows.append(
            {
                "line_type": "first_rights",
                "category_label": display_label,
                "edition_id": derived_bucket.edition_id,
                "units_sold": derived_bucket.units_sold,
                "units_returned": derived_bucket.units_returned,
                "net_units": net_u,
                "basis_amount": basis,
                "royalty_rate": derived_rate,
                "royalty_amount": royalty_amt,
                "applied_rule_id": rule.id,
                "applied_tier_id": applied_tier.id if applied_tier else None,
                "frozen_list_price_usd": None,
                "display_section": "canada_export",
                "royalty_stream": derived_bucket.royalty_stream,
                "source_us_rate_percent": source_us_rate_percent,
                "source_us_condition_text": source_us_condition_text,
                "derived_rate_formula": formula,
            }
        )

    # Ordinary subrights
    for sr in subrights_income_rows:
        subrights_name = str(sr.get("subrights_name") or "").strip()
        if not subrights_name:
            raise StatementValidationError(
                f"Subrights income row {sr.get('id')} is missing subrights name"
            )

        rule = pick_subrights_rule_for_name(subrights_rules, party, subrights_name)

        if rule.base != "net_receipts":
            raise StatementValidationError(
                f"Subrights rule {rule.id} for {subrights_name!r} must use net_receipts basis"
            )

        rate_pct = rule.flat_rate_percent if rule.flat_rate_percent is not None else rule.percent
        if rate_pct is None:
            raise StatementValidationError(
                f"Subrights rule {rule.id} for {subrights_name!r} has no percent"
            )
        rate_pct = _rate(rate_pct)

        basis = _money(_d(sr.get("publisher_receipts")))
        royalty_amt = _money(basis * rate_pct / Decimal("100"))
        earned_total += royalty_amt

        line_rows.append(
            {
                "line_type": "subrights",
                "category_label": subrights_name,
                "edition_id": None,
                "units_sold": Decimal("0"),
                "units_returned": Decimal("0"),
                "net_units": Decimal("0"),
                "basis_amount": basis,
                "royalty_rate": rate_pct,
                "royalty_amount": royalty_amt,
                "applied_rule_id": rule.id,
                "applied_tier_id": None,
                "frozen_list_price_usd": None,
                "display_section": "subrights",
                "royalty_stream": None,
                "source_us_rate_percent": None,
                "source_us_condition_text": None,
                "derived_rate_formula": None,
            }
        )

    prev_close = load_previous_closing_recoupment(
        cur, tenant_id, work_id, party, period.period_end
    )
    if prev_close is not None:
        opening = prev_close
    else:
        adv = total_recoupable_advances(cur, tenant_id, royalty_set_id, party)
        opening = -_money(adv) if adv > 0 else Decimal("0")
    
    advance_paid_original = _money(total_recoupable_advances(cur, tenant_id, royalty_set_id, party))

    closing, payable, recouped, available_after = compute_header_amounts(
        opening, _money(earned_total), adjustments_this_period
    )

    if existing:
        cur.execute(
            """
            DELETE FROM royalty_statement_lines
            WHERE statement_id = %s::uuid
            """,
            (stmt_id,),
        )

        cur.execute(
            """
            UPDATE royalty_statements
            SET
                period_id = %s::uuid,
                royalty_set_id = %s::uuid,
                opening_recoupment_balance = %s,
                earned_this_period = %s,
                adjustments_this_period = %s,
                closing_recoupment_balance = %s,
                recouped_this_period = %s,
                payable_this_period = %s,
                status = %s,
                updated_at = now()
            WHERE id = %s::uuid
            """,
            (
                resolved_period_id,
                royalty_set_id,
                str(opening),
                str(_money(earned_total)),
                str(adjustments_this_period),
                str(closing),
                str(recouped),
                str(payable),
                status,
                stmt_id,
            ),
        )
    else:
        cur.execute(
            """
            INSERT INTO royalty_statements (
                id,
                tenant_id,
                work_id,
                party,
                period_id,
                royalty_set_id,
                opening_recoupment_balance,
                earned_this_period,
                adjustments_this_period,
                closing_recoupment_balance,
                recouped_this_period,
                payable_this_period,
                status
            )
            VALUES (
                %s::uuid,
                %s::uuid,
                %s::uuid,
                %s,
                %s::uuid,
                %s::uuid,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s
            )
            """,
            (
                stmt_id,
                tenant_id,
                work_id,
                party,
                resolved_period_id,
                royalty_set_id,
                str(opening),
                str(_money(earned_total)),
                str(adjustments_this_period),
                str(closing),
                str(recouped),
                str(payable),
                status,
            ),
        )

    for ln in line_rows:
        cur.execute(
            """
            INSERT INTO royalty_statement_lines (
                tenant_id,
                statement_id,
                line_type,
                category_label,
                edition_id,
                units_sold,
                units_returned,
                net_units,
                basis_amount,
                royalty_rate,
                royalty_amount,
                applied_rule_id,
                applied_tier_id
            )
            VALUES (
                %s::uuid,
                %s::uuid,
                %s,
                %s,
                %s::uuid,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s::uuid,
                %s
            )
            """,
            (
                tenant_id,
                stmt_id,
                ln["line_type"],
                ln["category_label"],
                ln["edition_id"],
                str(ln["units_sold"]),
                str(ln["units_returned"]),
                str(ln["net_units"]),
                str(ln["basis_amount"]),
                str(ln["royalty_rate"]),
                str(ln["royalty_amount"]),
                ln["applied_rule_id"],
                ln["applied_tier_id"],
            ),
        )

    earned_this_period = _d(earned_total)

    prior_earned_to_date = _load_prior_earned_to_date(
        cur,
        tenant_id,
        work_id,
        party,
        period.period_end,
    )

    earned_to_date = prior_earned_to_date + earned_this_period

    return {
        "statement_id": stmt_id,
        "period_id": resolved_period_id,
        "work_id": work_id,
        "royalty_set_id": royalty_set_id,
        "party": party,
        "lines_written": len(line_rows),
        "header": {
            "opening_recoupment_balance": str(opening),
            "earned_this_period": str(_money(earned_total)),
            "earned_to_date": str(_money(earned_to_date)),
            "advance_paid_original": str(advance_paid_original),
            "adjustments_this_period": str(adjustments_this_period),
            "closing_recoupment_balance": str(closing),
            "recouped_this_period": str(recouped),
            "payable_this_period": str(payable),
            "running_balance": str(available_after),
            "status": status,
        },
    }


def fetch_statement_bundle(cur, statement_id: str) -> Dict[str, Any]:
    cur.execute(
        """
        SELECT *
        FROM royalty_statements
        WHERE id = %s::uuid
        LIMIT 1
        """,
        (statement_id,),
    )
    head = cur.fetchone()
    if not head:
        raise StatementValidationError(f"Statement not found: {statement_id}")

    hdr = dict(head)
    for k, v in list(hdr.items()):
        if hasattr(v, "isoformat"):
            hdr[k] = v.isoformat()
        elif isinstance(v, Decimal):
            hdr[k] = str(v)

    tenant_id = str(head["tenant_id"])
    work_id = str(head["work_id"])
    royalty_set_id = str(head["royalty_set_id"])
    party = str(head["party"])

    hdr["advance_paid_original"] = str(
        _money(total_recoupable_advances(cur, tenant_id, royalty_set_id, party))
    )

    period_meta: Dict[str, Any] = {}
    try:
        cur.execute(
            """
            SELECT id, period_code, period_start, period_end
            FROM royalty_periods
            WHERE id = %s::uuid
            LIMIT 1
            """,
            (head["period_id"],),
        )
        pr = cur.fetchone()
        if pr:
            period_meta = {
                "period_code": _safe_str(pr.get("period_code")),
                "period_start": pr["period_start"].isoformat() if pr.get("period_start") else None,
                "period_end": pr["period_end"].isoformat() if pr.get("period_end") else None,
            }
    except Exception:
        period_meta = {}

    current_period_end = period_meta.get("period_end")
    if current_period_end:
        prior_earned_to_date = _load_prior_earned_to_date(
            cur,
            tenant_id,
            work_id,
            party,
            current_period_end,
        )
        earned_this_period = _d(hdr.get("earned_this_period"))
        hdr["earned_to_date"] = str(_money(prior_earned_to_date + earned_this_period))
    else:
        hdr["earned_to_date"] = str(_money(_d(hdr.get("earned_this_period"))))

    try:
        work_payload = _build_full_work_payload(cur, tenant_id, work_id)
    except Exception:
        work_payload = {}

    contributor_block = _pick_party_block(work_payload, party)
    isbns = _collect_statement_isbns(work_payload)

    payment = _load_payment_instruction(cur, tenant_id, royalty_set_id, party)

    hdr["work_title"] = _safe_str(work_payload.get("title"))
    hdr["work_subtitle"] = _safe_str(work_payload.get("subtitle"))
    hdr["contributor_name"] = _safe_str(contributor_block.get("name"))
    hdr["contributor_address_lines"] = contributor_block.get("addressLines") or []
    hdr["agency_name"] = _safe_str((contributor_block.get("agency") or {}).get("agency")) or _safe_str(payment.get("agency_name"))
    hdr["agency_address_lines"] = (
        (contributor_block.get("agency") or {}).get("addressLines") or payment.get("agency_address_lines") or []
    )
    hdr["isbns"] = isbns

    hdr.update(period_meta)
    if period_meta.get("period_code") and period_meta.get("period_start") and period_meta.get("period_end"):
        hdr["period_label"] = (
            f"{period_meta['period_code']} "
            f"({period_meta['period_start']} – {period_meta['period_end']})"
        )

    hdr["payee_mode"] = payment.get("payee_mode")
    hdr["contributor_percent"] = payment.get("contributor_percent")
    hdr["agency_percent"] = payment.get("agency_percent")
    hdr["contributor_party_id"] = payment.get("contributor_party_id")
    hdr["agency_party_id"] = payment.get("agency_party_id")

    cur.execute(
        """
        SELECT *
        FROM royalty_statement_lines
        WHERE statement_id = %s::uuid
        ORDER BY category_label ASC, edition_id::text ASC
        """,
        (statement_id,),
    )
    lines = []
    raw_lines = cur.fetchall() or []

    rule_ids = []
    tier_ids = []
    for r in raw_lines:
        if r.get("applied_rule_id"):
            rule_ids.append(str(r["applied_rule_id"]))
        if r.get("applied_tier_id"):
            tier_ids.append(str(r["applied_tier_id"]))

    rule_map: Dict[str, Dict[str, Any]] = {}
    if rule_ids:
        cur.execute(
            """
            SELECT
                rr.id,
                rr.format_label,
                rr.rights_type::text AS rights_type,
                rr.base::text AS base,
                rr.mode::text AS mode,
                rr.notes
            FROM royalty_rules rr
            WHERE rr.id::text = ANY(%s)
            """,
            (rule_ids,),
        )
        for rr in cur.fetchall() or []:
            rule_map[str(rr["id"])] = {
                "format_label": _safe_str(rr.get("format_label")),
                "rights_type": _safe_str(rr.get("rights_type")),
                "base": _safe_str(rr.get("base")),
                "mode": _safe_str(rr.get("mode")),
                "notes": _safe_str(rr.get("notes")),
            }

    tier_condition_map: Dict[str, List[Dict[str, Any]]] = {}
    if tier_ids:
        cur.execute(
            """
            SELECT
                tier_id::text AS tier_id,
                kind::text AS kind,
                comparator::text AS comparator,
                value
            FROM royalty_tier_conditions
            WHERE tier_id::text = ANY(%s)
            ORDER BY tier_id ASC, id ASC
            """,
            (tier_ids,),
        )
        for tc in cur.fetchall() or []:
            tier_condition_map.setdefault(str(tc["tier_id"]), []).append({
                "kind": _safe_str(tc.get("kind")),
                "comparator": _safe_str(tc.get("comparator")),
                "value": str(tc["value"]) if tc.get("value") is not None else None,
            })

    for r in raw_lines:
        row = dict(r)
        for k, v in list(row.items()):
            if hasattr(v, "isoformat"):
                row[k] = v.isoformat()
            elif isinstance(v, Decimal):
                row[k] = str(v)

        applied_rule_id = str(row["applied_rule_id"]) if row.get("applied_rule_id") else None
        applied_tier_id = str(row["applied_tier_id"]) if row.get("applied_tier_id") else None

        if applied_rule_id and applied_rule_id in rule_map:
            row["basis_type"] = rule_map[applied_rule_id].get("base")
            row["rule_mode"] = rule_map[applied_rule_id].get("mode")
            row["rule_label"] = rule_map[applied_rule_id].get("notes") or rule_map[applied_rule_id].get("format_label")

        if applied_tier_id and applied_tier_id in tier_condition_map:
            conds = tier_condition_map[applied_tier_id]
            row["tier_conditions"] = conds
            row["rule_condition_text"] = "; ".join(
                f"{c['kind']} {c['comparator']} {c['value']}"
                for c in conds
                if c.get("kind") and c.get("comparator") and c.get("value") is not None
            )

        label_lower = _safe_str(row.get("category_label")).lower()
        if label_lower.startswith("canada -") or label_lower.startswith("export -"):
            row["display_section"] = "canada_export"
        elif _safe_str(row.get("line_type")).lower() == "subrights":
            row["display_section"] = "subrights"
        else:
            row["display_section"] = "first_rights"

        lines.append(row)

    return {
        "header": hdr,
        "lines": lines,
        "work": {
            "id": work_payload.get("id"),
            "uid": work_payload.get("uid"),
            "title": work_payload.get("title"),
            "subtitle": work_payload.get("subtitle"),
            "formats": work_payload.get("formats") or [],
        },
    }


def run_generate_statement(
    tenant_id: str,
    work_id: str,
    royalty_set_id: str,
    party: str,
    *,
    period_id: Optional[str] = None,
    period_start: Optional[str] = None,
    period_end: Optional[str] = None,
    rebuild: bool = False,
    status: str = "draft",
) -> Dict[str, Any]:
    from app.core.db import db_conn

    with db_conn() as conn:
        prev_ac = conn.autocommit
        conn.autocommit = False
        try:
            with conn.cursor(row_factory=dict_row) as cur:
                out = generate_statement(
                    cur,
                    tenant_id=tenant_id,
                    work_id=work_id,
                    royalty_set_id=royalty_set_id,
                    party=party,
                    period_id=period_id,
                    period_start=period_start,
                    period_end=period_end,
                    rebuild=rebuild,
                    status=status,
                )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.autocommit = prev_ac
    return out


def run_fetch_statement(statement_id: str) -> Dict[str, Any]:
    from app.core.db import db_conn

    with db_conn() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            bundle = fetch_statement_bundle(cur, statement_id)
    return bundle