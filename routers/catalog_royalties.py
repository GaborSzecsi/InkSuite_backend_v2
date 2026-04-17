from __future__ import annotations

import uuid
from typing import Any, Dict, Optional

from .catalog_shared import (_safe_str,)


def _clone_royalty_graph_to_work(
    cur,
    tenant_id: str,
    work_id: str,
    source_royalty_set_id: str,
    author_party_id: Optional[str] = None,
    illustrator_party_id: Optional[str] = None,
) -> str:
    target_set_id = _get_royalty_set_for_write(cur, tenant_id, work_id)

    _clear_royalty_graph_for_set(cur, tenant_id, target_set_id)

    try:
        cur.execute(
            """
            DELETE FROM advances
            WHERE tenant_id = %s
              AND royalty_set_id = %s
            """,
            (tenant_id, target_set_id),
        )
    except Exception:
        pass

    cur.execute(
        """
        SELECT
            rr.party,
            rr.rights_type,
            rr.format_label,
            rr.mode,
            rr.base,
            rr.escalating,
            rr.flat_rate_percent,
            rr.percent,
            rr.notes,
            st.name AS subrights_name,
            rr.id AS source_rule_id
        FROM royalty_rules rr
        LEFT JOIN subrights_types st
          ON st.id = rr.subrights_type_id
        WHERE rr.tenant_id = %s
          AND rr.royalty_set_id = %s
        ORDER BY rr.party, rr.rights_type, rr.format_label, rr.id
        """,
        (tenant_id, source_royalty_set_id),
    )
    rule_rows = cur.fetchall() or []

    for rr in rule_rows:
        rule_obj = {
            "format": _safe_str(rr.get("format_label")),
            "format_label": _safe_str(rr.get("format_label")),
            "mode": _safe_str(rr.get("mode")),
            "base": _safe_str(rr.get("base")),
            "escalating": bool(rr.get("escalating") or False),
            "flat_rate_percent": rr.get("flat_rate_percent"),
            "percent": rr.get("percent"),
            "note": _safe_str(rr.get("notes")),
            "notes": _safe_str(rr.get("notes")),
            "tiers": [],
        }

        if _safe_str(rr.get("rights_type")) == "subrights":
            rule_obj["name"] = _safe_str(rr.get("subrights_name") or rr.get("format_label"))
            rule_obj["subrights_name"] = _safe_str(rr.get("subrights_name") or rr.get("format_label"))

        cur.execute(
            """
            SELECT id, tier_order, rate_percent, base, note
            FROM royalty_tiers
            WHERE tenant_id = %s
              AND rule_id = %s
            ORDER BY tier_order ASC, id ASC
            """,
            (tenant_id, rr["source_rule_id"]),
        )
        tier_rows = cur.fetchall() or []

        for tr in tier_rows:
            tier_obj = {
                "rate_percent": tr.get("rate_percent"),
                "base": _safe_str(tr.get("base")),
                "note": _safe_str(tr.get("note")),
                "conditions": [],
            }

            try:
                cur.execute(
                    """
                    SELECT kind, comparator, value, value_min, value_max
                    FROM royalty_tier_conditions
                    WHERE tenant_id = %s
                      AND tier_id = %s
                    ORDER BY id ASC
                    """,
                    (tenant_id, tr["id"]),
                )
                cond_rows = cur.fetchall() or []
                has_range = True
            except Exception:
                cur.execute(
                    """
                    SELECT kind, comparator, value
                    FROM royalty_tier_conditions
                    WHERE tenant_id = %s
                      AND tier_id = %s
                    ORDER BY id ASC
                    """,
                    (tenant_id, tr["id"]),
                )
                cond_rows = cur.fetchall() or []
                has_range = False

            for cr in cond_rows:
                cond = {
                    "kind": _safe_str(cr.get("kind")),
                    "comparator": _safe_str(cr.get("comparator")),
                }
                if cond["comparator"] == "between" and has_range:
                    cond["value"] = [cr.get("value_min"), cr.get("value_max")]
                    cond["value_min"] = cr.get("value_min")
                    cond["value_max"] = cr.get("value_max")
                else:
                    cond["value"] = cr.get("value")
                    if has_range and cr.get("value_min") is not None:
                        cond["value_min"] = cr.get("value_min")
                    if has_range and cr.get("value_max") is not None:
                        cond["value_max"] = cr.get("value_max")
                tier_obj["conditions"].append(cond)

            rule_obj["tiers"].append(tier_obj)

        _insert_royalty_rule(
            cur,
            tenant_id,
            target_set_id,
            _safe_str(rr.get("party")),
            _safe_str(rr.get("rights_type")),
            rule_obj,
        )

    try:
        cur.execute(
            """
            SELECT party, amount, currency, recoupable
            FROM advances
            WHERE tenant_id = %s
              AND royalty_set_id = %s
            ORDER BY created_at ASC, id ASC
            """,
            (tenant_id, source_royalty_set_id),
        )
        adv_rows = cur.fetchall() or []
    except Exception:
        adv_rows = []

    for ar in adv_rows:
        party = _safe_str(ar.get("party")).lower()
        party_id = None
        if party == "author":
            party_id = author_party_id
        elif party == "illustrator":
            party_id = illustrator_party_id

        cur.execute(
            """
            INSERT INTO advances (
                tenant_id,
                royalty_set_id,
                work_id,
                party,
                party_id,
                amount,
                currency,
                recoupable
            )
            VALUES (%s, %s, %s, %s::roy_party, %s, %s, %s, %s)
            """,
            (
                tenant_id,
                target_set_id,
                work_id,
                party,
                party_id,
                ar.get("amount"),
                ar.get("currency") or "USD",
                bool(ar.get("recoupable") if ar.get("recoupable") is not None else True),
            ),
        )

    return target_set_id


def _royalty_set_has_dependents(cur, tenant_id: str, royalty_set_id: str) -> bool:
    checks = [
        (
            "SELECT 1 FROM subrights_income_lines WHERE tenant_id = %s AND royalty_set_id = %s LIMIT 1",
            (tenant_id, royalty_set_id),
        ),
        (
            "SELECT 1 FROM royalty_statements WHERE tenant_id = %s AND royalty_set_id = %s LIMIT 1",
            (tenant_id, royalty_set_id),
        ),
        (
            """
            SELECT 1
            FROM royalty_statement_lines rsl
            WHERE EXISTS (
                SELECT 1
                FROM royalty_rules rr
                WHERE rr.id = rsl.applied_rule_id
                  AND rr.tenant_id = %s
                  AND rr.royalty_set_id = %s
            )
            LIMIT 1
            """,
            (tenant_id, royalty_set_id),
        ),
    ]

    for sql, params in checks:
        try:
            cur.execute(sql, params)
            if cur.fetchone():
                return True
        except Exception:
            continue
    return False


def _get_or_create_active_royalty_set(cur, tenant_id: str, work_id: str) -> str:
    cur.execute(
        """
        SELECT id
        FROM royalty_sets
        WHERE tenant_id = %s
          AND work_id = %s
        ORDER BY is_active DESC, version DESC, created_at DESC, id DESC
        LIMIT 1
        """,
        (tenant_id, work_id),
    )
    row = cur.fetchone()
    if row:
        return str(row["id"])

    set_id = str(uuid.uuid4())
    cur.execute(
        """
        INSERT INTO royalty_sets (id, tenant_id, work_id, version, is_active)
        VALUES (%s, %s, %s, 1, true)
        """,
        (set_id, tenant_id, work_id),
    )
    return set_id


def _next_royalty_set_version(cur, tenant_id: str, work_id: str) -> int:
    cur.execute(
        """
        SELECT COALESCE(MAX(version), 0) AS max_version
        FROM royalty_sets
        WHERE tenant_id = %s
          AND work_id = %s
        """,
        (tenant_id, work_id),
    )
    row = cur.fetchone()
    return int((row or {}).get("max_version") or 0) + 1


def _get_royalty_set_for_write(cur, tenant_id: str, work_id: str) -> str:
    set_id = _get_or_create_active_royalty_set(cur, tenant_id, work_id)

    if not _royalty_set_has_dependents(cur, tenant_id, set_id):
        return set_id

    next_version = _next_royalty_set_version(cur, tenant_id, work_id)

    cur.execute(
        """
        UPDATE royalty_sets
        SET is_active = false,
            updated_at = now()
        WHERE tenant_id = %s
          AND work_id = %s
          AND id = %s
        """,
        (tenant_id, work_id, set_id),
    )

    new_set_id = str(uuid.uuid4())
    cur.execute(
        """
        INSERT INTO royalty_sets (id, tenant_id, work_id, version, is_active)
        VALUES (%s, %s, %s, %s, true)
        """,
        (new_set_id, tenant_id, work_id, next_version),
    )
    return new_set_id


def _clear_royalty_graph_for_set(cur, tenant_id: str, royalty_set_id: str) -> None:
    try:
        cur.execute(
            """
            UPDATE royalty_statement_lines rsl
            SET applied_tier_id = NULL
            WHERE rsl.tenant_id = %s::uuid
              AND rsl.applied_tier_id IS NOT NULL
              AND rsl.applied_tier_id IN (
                  SELECT t.id
                  FROM royalty_tiers t
                  JOIN royalty_rules r ON r.id = t.rule_id
                  WHERE t.tenant_id = %s::uuid
                    AND r.tenant_id = %s::uuid
                    AND r.royalty_set_id = %s::uuid
              )
            """,
            (tenant_id, tenant_id, tenant_id, royalty_set_id),
        )
        cur.execute(
            """
            UPDATE royalty_statement_lines rsl
            SET applied_rule_id = NULL
            WHERE rsl.tenant_id = %s::uuid
              AND rsl.applied_rule_id IS NOT NULL
              AND rsl.applied_rule_id IN (
                  SELECT id
                  FROM royalty_rules
                  WHERE tenant_id = %s::uuid
                    AND royalty_set_id = %s::uuid
              )
            """,
            (tenant_id, tenant_id, royalty_set_id),
        )
    except Exception:
        pass

    cur.execute(
        """
        DELETE FROM royalty_tier_conditions
        WHERE tenant_id = %s
          AND tier_id IN (
              SELECT t.id
              FROM royalty_tiers t
              JOIN royalty_rules r ON r.id = t.rule_id
              WHERE t.tenant_id = %s
                AND r.tenant_id = %s
                AND r.royalty_set_id = %s
          )
        """,
        (tenant_id, tenant_id, tenant_id, royalty_set_id),
    )

    cur.execute(
        """
        DELETE FROM royalty_tiers
        WHERE tenant_id = %s
          AND rule_id IN (
              SELECT id
              FROM royalty_rules
              WHERE tenant_id = %s
                AND royalty_set_id = %s
          )
        """,
        (tenant_id, tenant_id, royalty_set_id),
    )

    cur.execute(
        """
        DELETE FROM royalty_rules
        WHERE tenant_id = %s
          AND royalty_set_id = %s
        """,
        (tenant_id, royalty_set_id),
    )


def _resolve_subrights_type_id(
    cur, tenant_id: str, rule_obj: Dict[str, Any]
) -> Optional[str]:
    raw_name = _safe_str(rule_obj.get("name") or rule_obj.get("subrights_name"))
    if not raw_name:
        return None

    cur.execute(
        """
        SELECT id
        FROM subrights_types
        WHERE lower(name) = lower(%s)
        LIMIT 1
        """,
        (raw_name,),
    )
    row = cur.fetchone()
    if row:
        return str(row["id"])

    new_id = str(uuid.uuid4())
    cur.execute(
        """
        INSERT INTO subrights_types (id, name, is_active)
        VALUES (%s, %s, true)
        """,
        (new_id, raw_name),
    )
    return new_id


def _insert_royalty_rule(
    cur,
    tenant_id: str,
    set_id: str,
    party: str,
    rights_type: str,
    rule_obj: Dict[str, Any],
) -> None:
    if not isinstance(rule_obj, dict):
        return

    mode = (_safe_str(rule_obj.get("mode")) or "fixed").lower().replace(" ", "_")
    if mode not in ("fixed", "tiered"):
        mode = "fixed"

    base = (_safe_str(rule_obj.get("base")) or "list_price").lower().replace(" ", "_")
    if base not in ("list_price", "net_receipts"):
        base = "list_price"

    format_label = _safe_str(rule_obj.get("format") or rule_obj.get("format_label"))
    escalating = bool(rule_obj.get("escalating") or False)

    flat_rate = rule_obj.get("flat_rate_percent")
    try:
        flat_rate = float(flat_rate) if flat_rate is not None else None
    except Exception:
        flat_rate = None

    percent = rule_obj.get("percent")
    try:
        percent = float(percent) if percent is not None else None
    except Exception:
        percent = None

    notes = _safe_str(rule_obj.get("note") or rule_obj.get("notes"))

    subrights_type_id: Optional[str] = None
    insert_format_label = format_label

    if rights_type == "subrights":
        subrights_type_id = _resolve_subrights_type_id(cur, tenant_id, rule_obj)
        if not subrights_type_id:
            return
        if not insert_format_label:
            cur.execute(
                "SELECT name FROM subrights_types WHERE id = %s",
                (subrights_type_id,),
            )
            st_row = cur.fetchone()
            insert_format_label = _safe_str(st_row.get("name")) if st_row else ""
        if base not in ("net_receipts", "list_price"):
            base = "net_receipts"

    cur.execute(
        """
        INSERT INTO royalty_rules (
            tenant_id,
            royalty_set_id,
            party,
            rights_type,
            format_label,
            subrights_type_id,
            mode,
            base,
            escalating,
            flat_rate_percent,
            percent,
            notes
        )
        VALUES (%s, %s, %s::roy_party, %s::roy_rights_type, %s, %s, %s::roy_mode, %s::roy_base, %s, %s, %s, %s)
        RETURNING id
        """,
        (
            tenant_id,
            set_id,
            party,
            rights_type,
            insert_format_label,
            subrights_type_id,
            mode,
            base,
            escalating,
            flat_rate,
            percent,
            notes,
        ),
    )
    rule_row = cur.fetchone()
    if not rule_row:
        return

    rule_id = rule_row["id"]

    for t_idx, tier in enumerate(rule_obj.get("tiers") or [], start=1):
        if not isinstance(tier, dict):
            continue

        try:
            rate = float(tier.get("rate_percent") or tier.get("percent") or 0)
        except Exception:
            rate = 0.0

        tier_base = (_safe_str(tier.get("base")) or base).lower().replace(" ", "_")
        if tier_base not in ("list_price", "net_receipts"):
            tier_base = base

        tier_note = _safe_str(tier.get("note"))

        cur.execute(
            """
            INSERT INTO royalty_tiers (tenant_id, rule_id, tier_order, rate_percent, base, note)
            VALUES (%s, %s, %s, %s, %s::roy_base, %s)
            RETURNING id
            """,
            (tenant_id, rule_id, t_idx, rate, tier_base, tier_note),
        )
        tier_row = cur.fetchone()
        if not tier_row:
            continue

        tier_id = tier_row["id"]

        for cond in (tier.get("conditions") or []):
            if not isinstance(cond, dict):
                continue

            kind = (_safe_str(cond.get("kind")) or "units").lower()
            if kind not in ("units", "discount"):
                kind = "units"

            comp = _safe_str(cond.get("comparator")) or "<"
            if comp not in ("<", "<=", ">", ">=", "=", "!=", "<>", "between"):
                comp = "<"

            raw_value = cond.get("value")
            value_min = cond.get("value_min")
            value_max = cond.get("value_max")
            scalar_value = None

            if comp == "between":
                if isinstance(raw_value, (list, tuple)) and len(raw_value) >= 2:
                    value_min = raw_value[0]
                    value_max = raw_value[1]
                elif isinstance(raw_value, dict):
                    value_min = raw_value.get("min", value_min)
                    value_max = raw_value.get("max", value_max)
                else:
                    if value_min is None and raw_value not in (None, ""):
                        value_min = raw_value
                    if value_max is None and raw_value not in (None, ""):
                        value_max = raw_value

                try:
                    value_min = float(value_min) if value_min not in (None, "") else None
                except Exception:
                    value_min = None

                try:
                    value_max = float(value_max) if value_max not in (None, "") else None
                except Exception:
                    value_max = None

                if value_min is None or value_max is None:
                    continue

                try:
                    cur.execute(
                        """
                        INSERT INTO royalty_tier_conditions
                            (tenant_id, tier_id, kind, comparator, value, value_min, value_max)
                        VALUES (%s, %s, %s::roy_condition_kind, %s::roy_comparator, %s, %s, %s)
                        """,
                        (tenant_id, tier_id, kind, comp, value_min, value_min, value_max),
                    )
                except Exception:
                    cur.execute(
                        """
                        INSERT INTO royalty_tier_conditions
                            (tenant_id, tier_id, kind, comparator, value)
                        VALUES (%s, %s, %s::roy_condition_kind, %s::roy_comparator, %s)
                        """,
                        (tenant_id, tier_id, kind, comp, value_min),
                    )
            else:
                if isinstance(raw_value, (list, tuple)):
                    raw_value = raw_value[0] if raw_value else None

                try:
                    scalar_value = float(raw_value) if raw_value not in (None, "") else None
                except Exception:
                    scalar_value = None

                try:
                    value_min = float(value_min) if value_min not in (None, "") else None
                except Exception:
                    value_min = None

                try:
                    value_max = float(value_max) if value_max not in (None, "") else None
                except Exception:
                    value_max = None

                if scalar_value is None:
                    if value_min is not None:
                        scalar_value = value_min
                    elif value_max is not None:
                        scalar_value = value_max
                    else:
                        scalar_value = 0.0

                cur.execute(
                    """
                    INSERT INTO royalty_tier_conditions (tenant_id, tier_id, kind, comparator, value)
                    VALUES (%s, %s, %s::roy_condition_kind, %s::roy_comparator, %s)
                    """,
                    (tenant_id, tier_id, kind, comp, scalar_value),
                )


def _fetch_royalties_graph(cur, tenant_id: str, work_id: str) -> Dict[str, Any]:
    cur.execute(
        """
        SELECT id, version
        FROM royalty_sets
        WHERE tenant_id = %s
          AND work_id = %s
        ORDER BY is_active DESC, version DESC, created_at DESC, id DESC
        LIMIT 1
        """,
        (tenant_id, work_id),
    )
    rs = cur.fetchone()
    if not rs:
        return {
            "royalty_set_id": None,
            "version": None,
            "author": {"first_rights": [], "subrights": [], "advance": None},
            "illustrator": {"first_rights": [], "subrights": [], "advance": None},
        }

    set_id = str(rs["id"])
    version = rs.get("version")

    cur.execute(
        """
        SELECT
            rr.id,
            rr.party,
            rr.rights_type,
            rr.format_label,
            rr.mode,
            rr.base,
            rr.escalating,
            rr.flat_rate_percent,
            rr.percent,
            rr.notes,
            st.name AS subrights_name
        FROM royalty_rules rr
        LEFT JOIN subrights_types st
          ON st.id = rr.subrights_type_id
        WHERE rr.tenant_id = %s
          AND rr.royalty_set_id = %s
        ORDER BY rr.party, rr.rights_type, rr.format_label, rr.id
        """,
        (tenant_id, set_id),
    )
    rule_rows = cur.fetchall() or []

    out: Dict[str, Any] = {
        "royalty_set_id": set_id,
        "version": version,
        "author": {"first_rights": [], "subrights": [], "advance": None},
        "illustrator": {"first_rights": [], "subrights": [], "advance": None},
    }

    try:
        cur.execute(
            """
            SELECT party, amount
            FROM advances
            WHERE tenant_id = %s
              AND royalty_set_id = %s
            ORDER BY created_at ASC, id ASC
            """,
            (tenant_id, set_id),
        )
        adv_rows = cur.fetchall() or []
    except Exception:
        try:
            cur.execute(
                """
                SELECT party, amount
                FROM advances
                WHERE tenant_id = %s
                  AND work_id = %s
                ORDER BY created_at ASC, id ASC
                """,
                (tenant_id, work_id),
            )
            adv_rows = cur.fetchall() or []
        except Exception:
            adv_rows = []

    advance_totals = {"author": 0.0, "illustrator": 0.0}
    seen_any_advance = {"author": False, "illustrator": False}

    for ar in adv_rows:
        party = str(ar.get("party") or "").strip().lower()
        if party not in ("author", "illustrator"):
            continue
        try:
            amt = float(ar.get("amount") or 0)
        except Exception:
            amt = 0.0
        advance_totals[party] += amt
        seen_any_advance[party] = True

    if seen_any_advance["author"]:
        out["author"]["advance"] = advance_totals["author"]
    if seen_any_advance["illustrator"]:
        out["illustrator"]["advance"] = advance_totals["illustrator"]

    for rr in rule_rows:
        rule_id = rr["id"]
        party = _safe_str(rr.get("party")) or "author"
        rights_type = _safe_str(rr.get("rights_type")) or "first_rights"

        rule_obj: Dict[str, Any] = {
            "id": str(rule_id),
            "format": _safe_str(rr.get("format_label")),
            "format_label": _safe_str(rr.get("format_label")),
            "mode": _safe_str(rr.get("mode")),
            "base": _safe_str(rr.get("base")),
            "escalating": bool(rr.get("escalating") or False),
            "flat_rate_percent": rr.get("flat_rate_percent"),
            "percent": rr.get("percent"),
            "note": _safe_str(rr.get("notes")),
            "notes": _safe_str(rr.get("notes")),
            "tiers": [],
        }

        if rights_type == "subrights":
            rule_obj["name"] = _safe_str(rr.get("subrights_name") or rr.get("format_label"))
            rule_obj["subrights_name"] = _safe_str(
                rr.get("subrights_name") or rr.get("format_label")
            )

        cur.execute(
            """
            SELECT id, tier_order, rate_percent, base, note
            FROM royalty_tiers
            WHERE tenant_id = %s
              AND rule_id = %s
            ORDER BY tier_order ASC, id ASC
            """,
            (tenant_id, rule_id),
        )
        tier_rows = cur.fetchall() or []

        for tr in tier_rows:
            tier_id = tr["id"]
            tier_obj: Dict[str, Any] = {
                "id": str(tier_id),
                "tier_order": tr.get("tier_order"),
                "rate_percent": tr.get("rate_percent"),
                "base": _safe_str(tr.get("base")),
                "note": _safe_str(tr.get("note")),
                "conditions": [],
            }

            try:
                cur.execute(
                    """
                    SELECT kind, comparator, value, value_min, value_max
                    FROM royalty_tier_conditions
                    WHERE tenant_id = %s
                      AND tier_id = %s
                    ORDER BY id ASC
                    """,
                    (tenant_id, tier_id),
                )
                cond_rows = cur.fetchall() or []
                has_range_columns = True
            except Exception:
                cur.execute(
                    """
                    SELECT kind, comparator, value
                    FROM royalty_tier_conditions
                    WHERE tenant_id = %s
                      AND tier_id = %s
                    ORDER BY id ASC
                    """,
                    (tenant_id, tier_id),
                )
                cond_rows = cur.fetchall() or []
                has_range_columns = False

            for cr in cond_rows:
                comparator = _safe_str(cr.get("comparator"))
                cond_obj: Dict[str, Any] = {
                    "kind": _safe_str(cr.get("kind")),
                    "comparator": comparator,
                }

                if comparator == "between" and has_range_columns:
                    vmin = cr.get("value_min")
                    vmax = cr.get("value_max")
                    if vmin is not None and vmax is not None:
                        cond_obj["value"] = [vmin, vmax]
                        cond_obj["value_min"] = vmin
                        cond_obj["value_max"] = vmax
                    else:
                        cond_obj["value"] = cr.get("value")
                else:
                    cond_obj["value"] = cr.get("value")
                    if has_range_columns:
                        if cr.get("value_min") is not None:
                            cond_obj["value_min"] = cr.get("value_min")
                        if cr.get("value_max") is not None:
                            cond_obj["value_max"] = cr.get("value_max")

                tier_obj["conditions"].append(cond_obj)

            rule_obj["tiers"].append(tier_obj)

        if party not in out:
            out[party] = {"first_rights": [], "subrights": [], "advance": None}
        if rights_type not in out[party]:
            out[party][rights_type] = []

        out[party][rights_type].append(rule_obj)

    return out