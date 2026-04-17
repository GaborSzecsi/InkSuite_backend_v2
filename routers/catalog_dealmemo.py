from __future__ import annotations

import uuid
from typing import Any, Dict, List, Optional

from fastapi import HTTPException

from .catalog_shared import _safe_str, _parse_date_or_none
from .catalog_write import (
    _get_or_create_party,
    _upsert_party_core,
)
from .catalog_royalties import (
    _get_royalty_set_for_write,
    _insert_royalty_rule,
)


def _get_deal_memo_draft(cur, tenant_id: str, uid: str) -> Dict[str, Any]:
    uid = _safe_str(uid)
    if not uid:
        raise HTTPException(status_code=400, detail="uid is required")

    candidate_queries = [
        (
            """
            SELECT *
            FROM deal_memo_drafts
            WHERE tenant_id = %s
              AND uid = %s
            ORDER BY updated_at DESC NULLS LAST, created_at DESC NULLS LAST
            LIMIT 1
            """,
            (tenant_id, uid),
        ),
        (
            """
            SELECT *
            FROM contract_deal_memo_drafts
            WHERE tenant_id = %s
              AND uid = %s
            ORDER BY updated_at DESC NULLS LAST, created_at DESC NULLS LAST
            LIMIT 1
            """,
            (tenant_id, uid),
        ),
        (
            """
            SELECT *
            FROM contracts_deal_memo_drafts
            WHERE tenant_id = %s
              AND uid = %s
            ORDER BY updated_at DESC NULLS LAST, created_at DESC NULLS LAST
            LIMIT 1
            """,
            (tenant_id, uid),
        ),
    ]

    for sql, params in candidate_queries:
        try:
            cur.execute(sql, params)
            row = cur.fetchone()
            if row:
                return dict(row)
        except Exception:
            continue

    raise HTTPException(status_code=404, detail=f"Deal memo draft not found for uid: {uid}")


def _normalize_scope(raw: Any) -> str:
    s = _safe_str(raw).strip().lower()
    if s in ("illustrator", "a12", "artist", "illustration"):
        return "illustrator"
    return "author"


def _draft_scope(draft: Dict[str, Any]) -> str:
    return _normalize_scope(
        draft.get("contributor_role")
        or draft.get("scope")
        or draft.get("party_scope")
        or draft.get("role")
        or "author"
    )


def _draft_title(draft: Dict[str, Any]) -> str:
    return _safe_str(
        draft.get("title")
        or draft.get("book_title")
        or draft.get("name")
        or draft.get("project_title")
    )


def _draft_subtitle(draft: Dict[str, Any]) -> str:
    return _safe_str(draft.get("subtitle") or draft.get("book_subtitle"))


def _draft_series(draft: Dict[str, Any]) -> str:
    raw = draft.get("series_title")
    if raw not in (None, ""):
        return _safe_str(raw)
    if isinstance(draft.get("series"), str):
        return _safe_str(draft.get("series"))
    return ""


def _draft_language(draft: Dict[str, Any]) -> str:
    return _safe_str(draft.get("language"))


def _draft_rights(draft: Dict[str, Any]) -> str:
    return _safe_str(
        draft.get("territories_rights")
        or draft.get("rights")
        or draft.get("territory")
    )


def _draft_publication_date(draft: Dict[str, Any]):
    return _parse_date_or_none(
        draft.get("publication_date")
        or draft.get("projected_publication_date")
        or draft.get("pub_date")
        or draft.get("projected_publication")
    )


def _draft_publishing_year(draft: Dict[str, Any]):
    raw = draft.get("publishing_year") or draft.get("publication_year")
    if raw in (None, ""):
        return None
    try:
        return int(str(raw).strip())
    except Exception:
        return None


def _draft_imprint_name(draft: Dict[str, Any]) -> str:
    return _safe_str(
        draft.get("publisher_or_imprint")
        or draft.get("imprint")
        or draft.get("imprint_name")
        or draft.get("publisher_name")
    )


def _build_party_payload_from_draft(draft: Dict[str, Any], scope: str) -> Dict[str, Any]:
    prefix = "illustrator" if scope == "illustrator" else "author"
    name_key = "illustrator_name" if scope == "illustrator" else "author"

    payload: Dict[str, Any] = {
        scope: {
            "name": _safe_str(draft.get(name_key) or draft.get(prefix)),
            "email": _safe_str(draft.get(f"{prefix}_email")),
            "website": _safe_str(draft.get(f"{prefix}_website")),
            "phone_country_code": _safe_str(draft.get(f"{prefix}_phone_country_code")),
            "phone_number": _safe_str(
                draft.get(f"{prefix}_phone_number") or draft.get(f"{prefix}_phone")
            ),
            "address": {
                "street": _safe_str(draft.get(f"{prefix}_street")),
                "city": _safe_str(draft.get(f"{prefix}_city")),
                "state": _safe_str(draft.get(f"{prefix}_state")),
                "zip": _safe_str(
                    draft.get(f"{prefix}_zip") or draft.get(f"{prefix}_postal_code")
                ),
                "country": _safe_str(draft.get(f"{prefix}_country")),
            },
        },
        f"{prefix}_birth_city": _safe_str(draft.get(f"{prefix}_birth_city")),
        f"{prefix}_birth_country": _safe_str(draft.get(f"{prefix}_birth_country")),
        f"{prefix}_birth_date": draft.get(f"{prefix}_birth_date"),
        f"{prefix}_citizenship": _safe_str(draft.get(f"{prefix}_citizenship")),
        f"{prefix}_bio": _safe_str(draft.get(f"{prefix}_bio")),
        f"{prefix}_book_bio": _safe_str(draft.get(f"{prefix}_book_bio")),
        f"{prefix}_website_bio": _safe_str(draft.get(f"{prefix}_website_bio")),
    }
    return payload


def _ensure_work_row_for_author(cur, tenant_id: str, draft: Dict[str, Any]) -> str:
    work_id = str(uuid.uuid4())
    work_uid = str(uuid.uuid4())

    title = _draft_title(draft)
    if not title:
        raise HTTPException(status_code=400, detail="Deal memo is missing a title")

    subtitle = _draft_subtitle(draft)
    series_title = _draft_series(draft)
    language = _draft_language(draft)
    rights = _draft_rights(draft)
    publication_date = _draft_publication_date(draft)
    publishing_year = _draft_publishing_year(draft)
    imprint_name = _draft_imprint_name(draft)

    insert_attempts = [
        (
            """
            INSERT INTO works (
                id,
                tenant_id,
                uid,
                title,
                subtitle,
                series_title,
                language,
                rights,
                publication_date,
                publishing_year,
                publisher_or_imprint
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                work_id,
                tenant_id,
                work_uid,
                title,
                subtitle,
                series_title,
                language,
                rights,
                publication_date,
                publishing_year,
                imprint_name,
            ),
        ),
        (
            """
            INSERT INTO works (
                id,
                tenant_id,
                uid,
                title,
                subtitle,
                series_title,
                language,
                rights,
                publication_date,
                publishing_year
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                work_id,
                tenant_id,
                work_uid,
                title,
                subtitle,
                series_title,
                language,
                rights,
                publication_date,
                publishing_year,
            ),
        ),
        (
            """
            INSERT INTO works (
                id,
                tenant_id,
                uid,
                title,
                subtitle,
                series_title,
                language,
                rights
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                work_id,
                tenant_id,
                work_uid,
                title,
                subtitle,
                series_title,
                language,
                rights,
            ),
        ),
        (
            """
            INSERT INTO works (id, tenant_id, uid, title)
            VALUES (%s, %s, %s, %s)
            """,
            (work_id, tenant_id, work_uid, title),
        ),
    ]

    last_error: Optional[Exception] = None
    for sql, params in insert_attempts:
        try:
            cur.execute(sql, params)
            break
        except Exception as exc:
            last_error = exc
    else:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to create work from deal memo: {last_error}",
        )

    return work_id


def _require_existing_work_for_illustrator(cur, tenant_id: str, draft: Dict[str, Any]) -> str:
    work_id = _safe_str(draft.get("work_id") or draft.get("catalog_work_id"))
    if not work_id:
        raise HTTPException(
            status_code=400,
            detail="Illustrator deal memo requires an existing work_id",
        )

    cur.execute(
        """
        SELECT id
        FROM works
        WHERE tenant_id = %s
          AND id = %s
        LIMIT 1
        """,
        (tenant_id, work_id),
    )
    row = cur.fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Existing work not found")
    return str(row["id"])


def _ensure_work_contributor(
    cur,
    tenant_id: str,
    work_id: str,
    party_id: str,
    contributor_role: str,
    sequence_number: int,
) -> None:
    cur.execute(
        """
        SELECT id
        FROM work_contributors
        WHERE tenant_id = %s
          AND work_id = %s
          AND party_id = %s
        LIMIT 1
        """,
        (tenant_id, work_id, party_id),
    )
    row = cur.fetchone()
    if row:
        cur.execute(
            """
            UPDATE work_contributors
            SET contributor_role = %s,
                sequence_number = %s
            WHERE tenant_id = %s
              AND id = %s
            """,
            (contributor_role, sequence_number, tenant_id, row["id"]),
        )
        return

    try:
        cur.execute(
            """
            INSERT INTO work_contributors (
                id, tenant_id, work_id, party_id, contributor_role, sequence_number
            )
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (str(uuid.uuid4()), tenant_id, work_id, party_id, contributor_role, sequence_number),
        )
    except Exception:
        cur.execute(
            """
            INSERT INTO work_contributors (
                tenant_id, work_id, party_id, contributor_role, sequence_number
            )
            VALUES (%s, %s, %s, %s, %s)
            """,
            (tenant_id, work_id, party_id, contributor_role, sequence_number),
        )


def _ensure_party_representation_from_ids(
    cur,
    tenant_id: str,
    represented_party_id: str,
    work_id: str,
    agency_party_id: Optional[str],
    agent_party_id: Optional[str],
) -> None:
    agency_party_id = _safe_str(agency_party_id) or None
    agent_party_id = _safe_str(agent_party_id) or None

    representation_target = agent_party_id or agency_party_id
    if not representation_target:
        return

    try:
        cur.execute(
            """
            DELETE FROM party_representations
            WHERE tenant_id = %s
              AND represented_party_id = %s
              AND (work_id = %s OR work_id IS NULL)
            """,
            (tenant_id, represented_party_id, work_id),
        )
    except Exception:
        pass

    try:
        cur.execute(
            """
            INSERT INTO party_representations (
                tenant_id, represented_party_id, agent_party_id, work_id
            )
            VALUES (%s, %s, %s, %s)
            """,
            (tenant_id, represented_party_id, representation_target, work_id),
        )
    except Exception:
        pass

    if agency_party_id and agent_party_id:
        try:
            cur.execute(
                """
                DELETE FROM agency_agent_links
                WHERE tenant_id = %s
                  AND agency_party_id = %s
                """,
                (tenant_id, agency_party_id),
            )
        except Exception:
            pass

        try:
            cur.execute(
                """
                INSERT INTO agency_agent_links (
                    tenant_id, agency_party_id, agent_party_id, is_primary, role_label
                )
                VALUES (%s, %s, %s, true, 'agent')
                """,
                (tenant_id, agency_party_id, agent_party_id),
            )
        except Exception:
            pass


def _resolve_subrights_type_id(cur, subrights_name: str) -> Optional[str]:
    name = _safe_str(subrights_name)
    if not name:
        return None

    cur.execute(
        """
        SELECT id
        FROM subrights_types
        WHERE lower(name) = lower(%s)
        LIMIT 1
        """,
        (name,),
    )
    row = cur.fetchone()
    if row:
        return str(row["id"])

    new_id = str(uuid.uuid4())
    try:
        cur.execute(
            """
            INSERT INTO subrights_types (
                id,
                name,
                is_active,
                created_at
            )
            VALUES (%s, %s, true, now())
            """,
            (new_id, name),
        )
    except Exception:
        cur.execute(
            """
            SELECT id
            FROM subrights_types
            WHERE lower(name) = lower(%s)
            LIMIT 1
            """,
            (name,),
        )
        row = cur.fetchone()
        if row:
            return str(row["id"])
        raise

    return new_id


def _normalize_deal_memo_conditions(cond_rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Convert deal memo condition rows into the normalized condition objects expected
    by _insert_royalty_rule().

    Important normalization:
    - a pair of >= and <= rows for the same kind becomes one "between" condition
      with value_min/value_max
    """
    grouped: Dict[str, Dict[str, Any]] = {}
    passthrough: List[Dict[str, Any]] = []

    for cr in cond_rows:
        kind = _safe_str(cr.get("kind"))
        comparator = _safe_str(cr.get("comparator"))
        value = cr.get("value")

        if not kind or not comparator:
            continue

        if comparator in (">=", "<="):
            bucket = grouped.setdefault(kind, {})
            bucket[comparator] = value
        else:
            passthrough.append(
                {
                    "kind": kind,
                    "comparator": comparator,
                    "value": value,
                }
            )

    out: List[Dict[str, Any]] = []

    for kind, bucket in grouped.items():
        low = bucket.get(">=")
        high = bucket.get("<=")

        if low is not None and high is not None:
            out.append(
                {
                    "kind": kind,
                    "comparator": "between",
                    "value": [low, high],
                    "value_min": low,
                    "value_max": high,
                }
            )
        else:
            if low is not None:
                out.append(
                    {
                        "kind": kind,
                        "comparator": ">=",
                        "value": low,
                    }
                )
            if high is not None:
                out.append(
                    {
                        "kind": kind,
                        "comparator": "<=",
                        "value": high,
                    }
                )

    out.extend(passthrough)
    return out


def _copy_deal_memo_royalties_to_work(
    cur,
    tenant_id: str,
    deal_memo_draft_id: str,
    work_id: str,
) -> None:
    royalty_set_id = _get_royalty_set_for_write(cur, tenant_id, work_id)

    cur.execute(
        """
        SELECT 1
        FROM royalty_rules
        WHERE tenant_id = %s
          AND royalty_set_id = %s
        LIMIT 1
        """,
        (tenant_id, royalty_set_id),
    )
    if cur.fetchone():
        return

    cur.execute(
        """
        SELECT
            id,
            party,
            rights_type,
            format_label,
            mode,
            base,
            escalating,
            flat_rate_percent,
            percent,
            notes,
            subrights_name
        FROM deal_memo_royalty_rules
        WHERE tenant_id = %s
          AND deal_memo_draft_id = %s
        ORDER BY party, rights_type, format_label, id
        """,
        (tenant_id, deal_memo_draft_id),
    )
    rule_rows = cur.fetchall() or []

    for rr in rule_rows:
        cur.execute(
            """
            SELECT
                id,
                tier_order,
                rate_percent,
                base,
                note
            FROM deal_memo_royalty_tiers
            WHERE tenant_id = %s
              AND rule_id = %s
            ORDER BY tier_order ASC, id ASC
            """,
            (tenant_id, rr["id"]),
        )
        tier_rows = cur.fetchall() or []

        tiers: List[Dict[str, Any]] = []
        for tr in tier_rows:
            cur.execute(
                """
                SELECT
                    kind,
                    comparator,
                    value
                FROM deal_memo_royalty_tier_conditions
                WHERE tenant_id = %s
                  AND tier_id = %s
                ORDER BY id ASC
                """,
                (tenant_id, tr["id"]),
            )
            cond_rows = cur.fetchall() or []

            conditions = _normalize_deal_memo_conditions(cond_rows)

            tiers.append(
                {
                    "rate_percent": tr.get("rate_percent"),
                    "base": _safe_str(tr.get("base")),
                    "note": _safe_str(tr.get("note")),
                    "conditions": conditions,
                }
            )

        rights_type = _safe_str(rr.get("rights_type"))
        subrights_name = _safe_str(rr.get("subrights_name"))
        format_label = _safe_str(rr.get("format_label"))

        rule_obj: Dict[str, Any] = {
            "format": format_label,
            "format_label": format_label,
            "mode": _safe_str(rr.get("mode")),
            "base": _safe_str(rr.get("base")),
            "escalating": bool(rr.get("escalating") or False),
            "flat_rate_percent": rr.get("flat_rate_percent"),
            "percent": rr.get("percent"),
            "note": _safe_str(rr.get("notes")),
            "notes": _safe_str(rr.get("notes")),
            "tiers": tiers,
        }

        if rights_type == "subrights":
            # Ensure the normalized side can resolve the subright by name.
            _resolve_subrights_type_id(cur, subrights_name)
            rule_obj["name"] = subrights_name
            rule_obj["subrights_name"] = subrights_name
            rule_obj["format"] = ""
            rule_obj["format_label"] = ""

        _insert_royalty_rule(
            cur,
            tenant_id,
            royalty_set_id,
            _safe_str(rr.get("party")),
            rights_type,
            rule_obj,
        )


def _ensure_single_party_advance(
    cur,
    tenant_id: str,
    work_id: str,
    party: str,
    party_id: Optional[str],
    amount: Any,
    note: str = "",
) -> None:
    if not party_id:
        return

    royalty_set_id = _get_royalty_set_for_write(cur, tenant_id, work_id)

    try:
        amt = float(amount) if amount not in (None, "") else None
    except Exception:
        amt = None

    if amt is None:
        return

    cur.execute(
        """
        DELETE FROM advances
        WHERE tenant_id = %s
          AND royalty_set_id = %s
          AND party = %s
        """,
        (tenant_id, royalty_set_id, party),
    )

    cur.execute(
        """
        INSERT INTO advances (
            id,
            tenant_id,
            royalty_set_id,
            party,
            amount,
            currency,
            recoupable,
            notes,
            party_id
        )
        VALUES (%s, %s, %s, %s, %s, 'USD', true, %s, %s)
        """,
        (
            str(uuid.uuid4()),
            tenant_id,
            royalty_set_id,
            party,
            amt,
            note,
            party_id,
        ),
    )


def _create_or_update_from_author_deal_memo(
    cur,
    tenant_id: str,
    draft: Dict[str, Any],
) -> str:
    work_id = _ensure_work_row_for_author(cur, tenant_id, draft)

    author_name = _safe_str(draft.get("author"))
    if not author_name:
        raise HTTPException(status_code=400, detail="Author name is required")

    author_email = _safe_str(draft.get("author_email"))
    author_party_id = _get_or_create_party(cur, tenant_id, author_name, author_email, "person")
    if not author_party_id:
        raise HTTPException(status_code=500, detail="Could not create author party")

    payload = _build_party_payload_from_draft(draft, "author")
    _upsert_party_core(cur, tenant_id, author_party_id, payload, "author")

    _ensure_work_contributor(
        cur,
        tenant_id,
        work_id,
        author_party_id,
        "A01",
        1,
    )

    _ensure_party_representation_from_ids(
        cur,
        tenant_id,
        represented_party_id=author_party_id,
        work_id=work_id,
        agency_party_id=draft.get("agency_party_id"),
        agent_party_id=draft.get("agent_party_id"),
    )

    _copy_deal_memo_royalties_to_work(
        cur,
        tenant_id,
        deal_memo_draft_id=str(draft["id"]),
        work_id=work_id,
    )

    _ensure_single_party_advance(
        cur,
        tenant_id,
        work_id,
        party="author",
        party_id=author_party_id,
        amount=draft.get("author_advance"),
        note=_draft_title(draft),
    )

    return work_id


def _apply_illustrator_deal_memo_to_existing_work(
    cur,
    tenant_id: str,
    draft: Dict[str, Any],
) -> str:
    work_id = _require_existing_work_for_illustrator(cur, tenant_id, draft)

    illustrator_name = _safe_str(draft.get("illustrator_name") or draft.get("illustrator"))
    if not illustrator_name:
        raise HTTPException(status_code=400, detail="Illustrator name is required")

    illustrator_email = _safe_str(draft.get("illustrator_email"))
    illustrator_party_id = _get_or_create_party(
        cur, tenant_id, illustrator_name, illustrator_email, "person"
    )
    if not illustrator_party_id:
        raise HTTPException(status_code=500, detail="Could not create illustrator party")

    payload = _build_party_payload_from_draft(draft, "illustrator")
    _upsert_party_core(cur, tenant_id, illustrator_party_id, payload, "illustrator")

    _ensure_work_contributor(
        cur,
        tenant_id,
        work_id,
        illustrator_party_id,
        "A12",
        2,
    )

    _ensure_party_representation_from_ids(
        cur,
        tenant_id,
        represented_party_id=illustrator_party_id,
        work_id=work_id,
        agency_party_id=draft.get("agency_party_id"),
        agent_party_id=draft.get("agent_party_id"),
    )

    _copy_deal_memo_royalties_to_work(
        cur,
        tenant_id,
        deal_memo_draft_id=str(draft["id"]),
        work_id=work_id,
    )

    _ensure_single_party_advance(
        cur,
        tenant_id,
        work_id,
        party="illustrator",
        party_id=illustrator_party_id,
        amount=draft.get("illustrator_advance"),
        note=_draft_title(draft),
    )

    return work_id


def _upsert_work_from_deal_memo(cur, tenant_id: str, uid: str) -> str:
    draft = _get_deal_memo_draft(cur, tenant_id, uid)
    scope = _draft_scope(draft)

    if scope == "illustrator":
        return _apply_illustrator_deal_memo_to_existing_work(cur, tenant_id, draft)

    return _create_or_update_from_author_deal_memo(cur, tenant_id, draft)