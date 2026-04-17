# marble_app/services/royalty_statement_db.py
from __future__ import annotations

import json
from datetime import date
from typing import Any, Dict, List, Tuple

from psycopg.types.json import Json


def _parse_period_date(s: str) -> date:
    return date.fromisoformat(str(s).strip()[:10])


def load_work_statement_histories(
    cur,
    tenant_id: str,
    work_id: str,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Rows shaped like RoyaltyCalculator JSON history entries for this work only.
    """
    cur.execute(
        """
        SELECT
            party,
            period_start::text AS period_start,
            period_end::text AS period_end,
            running_balance,
            party_payload
        FROM royalty_statements
        WHERE tenant_id = %s::uuid
          AND work_id = %s::uuid
        ORDER BY party, period_end ASC, period_start ASC
        """,
        (tenant_id, work_id),
    )
    rows = cur.fetchall() or []
    author: List[Dict[str, Any]] = []
    illustrator: List[Dict[str, Any]] = []
    wid = str(work_id)
    for r in rows:
        payload = r.get("party_payload") or {}
        if isinstance(payload, str):
            try:
                payload = json.loads(payload)
            except Exception:
                payload = {}
        if not isinstance(payload, dict):
            payload = {}
        cats = payload.get("categories") or []
        rec = {
            "book_id": wid,
            "period_start": str(r.get("period_start") or ""),
            "period_end": str(r.get("period_end") or ""),
            "balance": float(r["running_balance"] if r.get("running_balance") is not None else 0.0),
            "categories": cats,
        }
        party = (r.get("party") or "").lower()
        if party == "author":
            author.append(rec)
        elif party == "illustrator":
            illustrator.append(rec)
    return author, illustrator


def upsert_statement(
    cur,
    tenant_id: str,
    work_id: str,
    party: str,
    period_start: str,
    period_end: str,
    party_calc: Dict[str, Any],
    *,
    status: str = "final",
) -> None:
    last_bal = float(party_calc.get("last_balance") or 0.0)
    earned = float(party_calc.get("royalty_total") or 0.0)
    balance = float(party_calc.get("balance") or 0.0)
    payable = float(party_calc.get("payable") or 0.0)

    closing_recoup = balance if balance < 0 else 0.0
    recouped = max(0.0, balance - last_bal)

    ps = _parse_period_date(period_start)
    pe = _parse_period_date(period_end)

    party_payload: Dict[str, Any] = {
        "categories": party_calc.get("categories") or [],
        "advance": party_calc.get("advance"),
        "royalty_total": party_calc.get("royalty_total"),
        "last_balance": party_calc.get("last_balance"),
        "balance": party_calc.get("balance"),
        "payable": party_calc.get("payable"),
    }

    cur.execute(
        """
        INSERT INTO royalty_statements (
            tenant_id, work_id, party, period_start, period_end,
            opening_recoupment_balance, earned_this_period, recouped_this_period,
            adjustments_this_period, closing_recoupment_balance, payable_this_period,
            running_balance, status, party_payload, updated_at
        )
        VALUES (
            %s::uuid, %s::uuid, %s, %s, %s,
            %s, %s, %s,
            %s, %s, %s,
            %s, %s, %s, now()
        )
        ON CONFLICT (tenant_id, work_id, party, period_start, period_end)
        DO UPDATE SET
            opening_recoupment_balance = EXCLUDED.opening_recoupment_balance,
            earned_this_period = EXCLUDED.earned_this_period,
            recouped_this_period = EXCLUDED.recouped_this_period,
            adjustments_this_period = EXCLUDED.adjustments_this_period,
            closing_recoupment_balance = EXCLUDED.closing_recoupment_balance,
            payable_this_period = EXCLUDED.payable_this_period,
            running_balance = EXCLUDED.running_balance,
            status = EXCLUDED.status,
            party_payload = EXCLUDED.party_payload,
            updated_at = now()
        """,
        (
            tenant_id,
            work_id,
            party,
            ps,
            pe,
            last_bal,
            earned,
            recouped,
            0.0,
            closing_recoup,
            payable,
            balance,
            status,
            Json(party_payload),
        ),
    )


