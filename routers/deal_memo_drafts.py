# routers/deal_memo_drafts.py
# Deal memo drafts CRUD + agency autocomplete/upsert for contract automation.
from __future__ import annotations

import secrets
import string
import time
from datetime import date, datetime
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Body, HTTPException, Query
from psycopg.rows import dict_row

from app.core.db import db_conn

router = APIRouter(prefix="/contracts", tags=["contracts"])

DEFAULT_TENANT_SLUG = "marble-press"
DEAL_MEMO_TABLE = "deal_memo_drafts"



def _rand_uid(n: int = 7) -> str:
    alphabet = string.ascii_lowercase + string.digits
    return "".join(secrets.choice(alphabet) for _ in range(n))


def _now_ms() -> int:
    return int(time.time() * 1000)


def _s(v: Any) -> str:
    return "" if v is None else str(v).strip()


def _jsonable(v: Any) -> Any:
    if isinstance(v, (datetime, date)):
        return v.isoformat()
    return v


def _person_name(v: Any) -> str:
    if isinstance(v, str):
        return v.strip()
    if isinstance(v, dict):
        return _s(v.get("name")) or " ".join(
            [x for x in (_s(v.get("first_name")), _s(v.get("last_name"))) if x]
        ).strip()
    return ""


def _person_email(v: Any, fallback: Any = "") -> str:
    if isinstance(v, dict):
        return _s(v.get("email")) or _s(fallback)
    return _s(fallback)


def _person_website(v: Any, fallback: Any = "") -> str:
    if isinstance(v, dict):
        return _s(v.get("website")) or _s(fallback)
    return _s(fallback)


def _person_phone_cc(v: Any, fallback: Any = "") -> str:
    if isinstance(v, dict):
        return _s(v.get("phone_country_code")) or _s(fallback)
    return _s(fallback)


def _person_phone_number(v: Any, fallback: Any = "") -> str:
    if isinstance(v, dict):
        return _s(v.get("phone_number")) or _s(fallback)
    return _s(fallback)


def _bool(v: Any) -> bool:
    if isinstance(v, bool):
        return v
    if v in (None, "", 0, "0", "false", "False", "FALSE", "no", "No"):
        return False
    return True


def _person_address(v: Any, fallback: Any = None) -> dict:
    fallback = fallback or {}
    if isinstance(v, dict) and isinstance(v.get("address"), dict):
        a = v["address"]
        return {
            "street": _s(a.get("street")) or _s(fallback.get("street")),
            "city": _s(a.get("city")) or _s(fallback.get("city")),
            "state": _s(a.get("state")) or _s(fallback.get("state")),
            "zip": _s(a.get("zip")) or _s(fallback.get("zip")),
            "country": _s(a.get("country")) or _s(fallback.get("country")),
            "nonUS": _bool(a.get("nonUS") if "nonUS" in a else fallback.get("nonUS")),
        }
    return {
        "street": _s(fallback.get("street")),
        "city": _s(fallback.get("city")),
        "state": _s(fallback.get("state")),
        "zip": _s(fallback.get("zip")),
        "country": _s(fallback.get("country")),
        "nonUS": _bool(fallback.get("nonUS")),
    }


def _date_or_none(v: Any) -> Optional[date]:
    if v in (None, "", "null"):
        return None
    if isinstance(v, date) and not isinstance(v, datetime):
        return v
    if isinstance(v, datetime):
        return v.date()
    try:
        return date.fromisoformat(str(v)[:10])
    except Exception:
        return None


def _int_or_none(v: Any) -> Optional[int]:
    if v in (None, "", "null"):
        return None
    try:
        return int(v)
    except Exception:
        return None


def _float_or_none(v: Any) -> Optional[float]:
    if v in (None, "", "null"):
        return None
    try:
        return float(v)
    except Exception:
        return None


def _tenant_id(cur, tenant_slug: str | None = None) -> str:
    import os

    slug = (
        tenant_slug
        or os.environ.get("DEFAULT_TENANT_SLUG")
        or os.environ.get("TENANT_SLUG")
        or DEFAULT_TENANT_SLUG
    ).strip()

    cur.execute("SELECT id FROM tenants WHERE lower(slug) = lower(%s) LIMIT 1", (slug,))
    row = cur.fetchone()
    if not row:
        cur.execute("SELECT id FROM tenants ORDER BY id LIMIT 1")
        row = cur.fetchone()
    if not row:
        raise HTTPException(status_code=503, detail="No tenant configured (deal_memo_drafts)")
    return str(row["id"])


def _require_tables(cur) -> None:
    required = [
        "deal_memo_drafts",
        "deal_memo_advance_installments",
        "deal_memo_royalty_rules",
        "deal_memo_royalty_tiers",
        "deal_memo_royalty_tier_conditions",
        "parties",
        "party_addresses",
        "agency_profiles",
        "agency_agent_links",
        "party_representations",
        "deal_memo_contributors",
        "deal_memo_contributor_addresses",
        "deal_memo_contributor_socials",
        "deal_memo_contributor_awards",
        "deal_memo_contributor_identifiers",
        "deal_memo_contributor_representations",
        "deal_memo_representation_addresses",
    ]
    cur.execute(
        """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public'
          AND table_name = ANY(%s)
        """,
        (required,),
    )
    found = {r["table_name"] for r in (cur.fetchall() or [])}
    missing = [t for t in required if t not in found]
    if missing:
        raise HTTPException(status_code=500, detail=f"Missing required tables: {', '.join(missing)}")


def _get_agency_agents(cur, tenant_id: str, agency_party_id: str) -> List[dict]:
    cur.execute(
        """
        SELECT
            ag.id AS agent_party_id,
            ag.display_name AS agent_name,
            ag.email AS agent_email,
            ag.phone_country_code AS agent_phone_country_code,
            ag.phone_number AS agent_phone_number,
            l.is_primary,
            l.role_label
        FROM agency_agent_links l
        JOIN parties ag
          ON ag.id = l.agent_party_id
        WHERE l.tenant_id = %s
          AND l.agency_party_id = %s
        ORDER BY l.is_primary DESC, ag.display_name ASC
        """,
        (tenant_id, agency_party_id),
    )
    return [dict(r) for r in (cur.fetchall() or [])]


def _get_agency_detail(cur, tenant_id: str, agency_party_id: str) -> dict:
    cur.execute(
        """
        SELECT
            p.id AS agency_party_id,
            p.display_name AS agency_name,
            p.email AS agency_email,
            p.website AS agency_website,
            p.phone_country_code AS agency_phone_country_code,
            p.phone_number AS agency_phone_number,
            pa.street AS agency_street,
            pa.city AS agency_city,
            pa.state AS agency_state,
            pa.zip AS agency_zip,
            pa.country AS agency_country,
            ap.agency_clause
        FROM parties p
        LEFT JOIN party_addresses pa
          ON pa.party_id = p.id
         AND pa.label = 'primary'
        LEFT JOIN agency_profiles ap
          ON ap.agency_party_id = p.id
        WHERE p.tenant_id = %s
          AND p.id = %s
          AND p.party_type = 'org'
        LIMIT 1
        """,
        (tenant_id, agency_party_id),
    )
    row = cur.fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Agency not found")

    agency = dict(row)
    agency["agents"] = _get_agency_agents(cur, tenant_id, agency_party_id)
    return agency


def _upsert_agency(cur, tenant_id: str, body: Dict[str, Any]) -> Dict[str, Any]:
    _require_tables(cur)

    agency_party_id = _s(body.get("agency_party_id"))

    # --- sanitize agency name ---
    raw_agency_name = body.get("agency_name")
    if isinstance(raw_agency_name, dict):
        agency_name = _s(raw_agency_name.get("name"))
    else:
        agency_name = _s(raw_agency_name)
    if agency_name == "[object Object]":
        agency_name = ""

    agency_email = _s(body.get("agency_email"))
    agency_website = _s(body.get("agency_website"))
    agency_clause = _s(body.get("agency_clause"))
    agency_street = _s(body.get("agency_street"))
    agency_city = _s(body.get("agency_city"))
    agency_state = _s(body.get("agency_state"))
    agency_zip = _s(body.get("agency_zip"))
    agency_country = _s(body.get("agency_country"))

    # --- sanitize agent ---
    agent_party_id = _s(body.get("agent_party_id"))

    raw_agent_name = body.get("agent_name")
    if isinstance(raw_agent_name, dict):
        agent_name = _s(raw_agent_name.get("name"))
    else:
        agent_name = _s(raw_agent_name)
    if agent_name == "[object Object]":
        agent_name = ""

    agent_email = _s(body.get("agent_email"))
    agent_phone_country_code = _s(body.get("agent_phone_country_code"))
    agent_phone_number = _s(body.get("agent_phone_number"))

    contributor_party_id = _s(
        body.get("contributor_party_id")
        or body.get("represented_party_id")
    )
    work_id = _s(body.get("work_id"))

    if not agency_name.strip() and not agent_name.strip():
        return {"agency_party_id": "", "agent_party_id": "", "agency": None, "agents": []}

    if agency_party_id:
        cur.execute(
            """
            UPDATE parties
            SET
                display_name = CASE WHEN %s <> '' THEN %s ELSE display_name END,
                email = %s,
                website = %s,
                updated_at = now()
            WHERE tenant_id = %s
              AND id = %s
              AND party_type = 'org'
            RETURNING id
            """,
            (agency_name, agency_name, agency_email, agency_website, tenant_id, agency_party_id),
        )
        row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Selected agency not found")
        agency_party_id = str(row["id"])
    else:
        cur.execute(
            """
            SELECT id
            FROM parties
            WHERE tenant_id = %s
              AND party_type = 'org'
              AND lower(display_name) = lower(%s)
            LIMIT 1
            """,
            (tenant_id, agency_name),
        )
        row = cur.fetchone()
        if row:
            agency_party_id = str(row["id"])
            cur.execute(
                """
                UPDATE parties
                SET
                    email = CASE WHEN %s <> '' THEN %s ELSE email END,
                    website = CASE WHEN %s <> '' THEN %s ELSE website END,
                    updated_at = now()
                WHERE id = %s
                """,
                (agency_email, agency_email, agency_website, agency_website, agency_party_id),
            )
        else:
            cur.execute(
                """
                INSERT INTO parties (
                    tenant_id, party_type, display_name, email, website
                )
                VALUES (%s, 'org', %s, %s, %s)
                RETURNING id
                """,
                (tenant_id, agency_name or "Unnamed Agency", agency_email, agency_website),
            )
            agency_party_id = str(cur.fetchone()["id"])

    cur.execute(
        """
        SELECT id
        FROM party_addresses
        WHERE party_id = %s
          AND label = 'primary'
        LIMIT 1
        """,
        (agency_party_id,),
    )
    addr_row = cur.fetchone()
    if addr_row:
        cur.execute(
            """
            UPDATE party_addresses
            SET
                street = %s,
                city = %s,
                state = %s,
                zip = %s,
                country = %s,
                is_non_us = %s
            WHERE id = %s
            """,
            (
                agency_street,
                agency_city,
                agency_state,
                agency_zip,
                agency_country,
                bool(agency_country and agency_country.lower() not in ("usa", "us", "united states", "")),
                addr_row["id"],
            ),
        )
    else:
        cur.execute(
            """
            INSERT INTO party_addresses (
                tenant_id, party_id, label, street, city, state, zip, country, is_non_us
            )
            VALUES (%s, %s, 'primary', %s, %s, %s, %s, %s, %s)
            """,
            (
                tenant_id,
                agency_party_id,
                agency_street,
                agency_city,
                agency_state,
                agency_zip,
                agency_country,
                bool(agency_country and agency_country.lower() not in ("usa", "us", "united states", "")),
            ),
        )

    cur.execute(
        """
        INSERT INTO agency_profiles (
            tenant_id, agency_party_id, agency_clause, notes
        )
        VALUES (%s, %s, %s, '')
        ON CONFLICT (agency_party_id) DO UPDATE SET
            agency_clause = EXCLUDED.agency_clause,
            updated_at = now()
        """,
        (tenant_id, agency_party_id, agency_clause),
    )

    resolved_agent_party_id = ""
    if agent_name.strip():
        if agent_party_id:
            cur.execute(
                """
                UPDATE parties
                SET
                    display_name = CASE WHEN %s <> '' THEN %s ELSE display_name END,
                    email = %s,
                    phone_country_code = %s,
                    phone_number = %s,
                    updated_at = now()
                WHERE tenant_id = %s
                  AND id = %s
                  AND party_type = 'person'
                RETURNING id
                """,
                (
                    agent_name,
                    agent_name,
                    agent_email,
                    agent_phone_country_code,
                    agent_phone_number,
                    tenant_id,
                    agent_party_id,
                ),
            )
            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Selected agent not found")
            resolved_agent_party_id = str(row["id"])
        else:
            if agent_email:
                cur.execute(
                    """
                    SELECT id
                    FROM parties
                    WHERE tenant_id = %s
                      AND party_type = 'person'
                      AND lower(display_name) = lower(%s)
                      AND lower(coalesce(email, '')) = lower(%s)
                    LIMIT 1
                    """,
                    (tenant_id, agent_name, agent_email),
                )
            else:
                cur.execute(
                    """
                    SELECT id
                    FROM parties
                    WHERE tenant_id = %s
                      AND party_type = 'person'
                      AND lower(display_name) = lower(%s)
                    LIMIT 1
                    """,
                    (tenant_id, agent_name),
                )
            row = cur.fetchone()
            if row:
                resolved_agent_party_id = str(row["id"])
                cur.execute(
                    """
                    UPDATE parties
                    SET
                        email = CASE WHEN %s <> '' THEN %s ELSE email END,
                        phone_country_code = CASE WHEN %s <> '' THEN %s ELSE phone_country_code END,
                        phone_number = CASE WHEN %s <> '' THEN %s ELSE phone_number END,
                        updated_at = now()
                    WHERE id = %s
                    """,
                    (
                        agent_email,
                        agent_email,
                        agent_phone_country_code,
                        agent_phone_country_code,
                        agent_phone_number,
                        agent_phone_number,
                        resolved_agent_party_id,
                    ),
                )
            else:
                cur.execute(
                    """
                    INSERT INTO parties (
                        tenant_id, party_type, display_name, email, phone_country_code, phone_number
                    )
                    VALUES (%s, 'person', %s, %s, %s, %s)
                    RETURNING id
                    """,
                    (tenant_id, agent_name, agent_email, agent_phone_country_code, agent_phone_number),
                )
                resolved_agent_party_id = str(cur.fetchone()["id"])
               

        cur.execute(
            """
            INSERT INTO agency_agent_links (
                tenant_id, agency_party_id, agent_party_id, is_primary, role_label
            )
            VALUES (%s, %s, %s, %s, 'agent')
            ON CONFLICT (agency_party_id, agent_party_id) DO UPDATE SET
                updated_at = now()
            """,
            (tenant_id, agency_party_id, resolved_agent_party_id, True),
        )

        if contributor_party_id and resolved_agent_party_id:
            if work_id:
                cur.execute(
                    """
                    DELETE FROM party_representations
                    WHERE tenant_id = %s
                      AND represented_party_id = %s
                      AND work_id = %s
                    """,
                    (tenant_id, contributor_party_id, work_id),
                )

                cur.execute(
                    """
                    INSERT INTO party_representations (
                        tenant_id, represented_party_id, agent_party_id, work_id,
                        is_primary, role_label, notes
                    )
                    VALUES (%s, %s, %s, %s, true, 'agent', '')
                    """,
                    (tenant_id, contributor_party_id, resolved_agent_party_id, work_id),
                )
            else:
                cur.execute(
                    """
                    DELETE FROM party_representations
                    WHERE tenant_id = %s
                      AND represented_party_id = %s
                      AND work_id IS NULL
                    """,
                    (tenant_id, contributor_party_id),
                )

                cur.execute(
                    """
                    INSERT INTO party_representations (
                        tenant_id, represented_party_id, agent_party_id, work_id,
                        is_primary, role_label, notes
                    )
                    VALUES (%s, %s, %s, NULL, true, 'agent', '')
                    """,
                    (tenant_id, contributor_party_id, resolved_agent_party_id),
                )

    agency = _get_agency_detail(cur, tenant_id, agency_party_id)
    return {
        "agency_party_id": agency_party_id,
        "agent_party_id": resolved_agent_party_id,
        "agency": agency,
        "agents": agency.get("agents", []),
    }


def _hydrate_advance_schedule(cur, draft_id: str) -> List[dict]:
    cur.execute(
        """
        SELECT installment_order, amount_type, value, trigger
        FROM deal_memo_advance_installments
        WHERE deal_memo_draft_id = %s
        ORDER BY installment_order ASC, id ASC
        """,
        (draft_id,),
    )
    rows = cur.fetchall() or []
    out: List[dict] = []
    for idx, r in enumerate(rows, start=1):
        out.append(
            {
                "id": f"inst-{idx}",
                "amountType": _s(r.get("amount_type")) or "percent",
                "value": float(r["value"]) if r.get("value") is not None else None,
                "trigger": _s(r.get("trigger")),
            }
        )
    return out


def _hydrate_royalties(cur, draft_id: str) -> dict:
    cur.execute(
        """
        SELECT *
        FROM deal_memo_royalty_rules
        WHERE deal_memo_draft_id = %s
        ORDER BY party ASC, rights_type ASC, created_at ASC, id ASC
        """,
        (draft_id,),
    )
    rules = cur.fetchall() or []

    cur.execute(
        """
        SELECT *
        FROM deal_memo_royalty_tiers
        WHERE rule_id IN (
          SELECT id FROM deal_memo_royalty_rules WHERE deal_memo_draft_id = %s
        )
        ORDER BY rule_id ASC, tier_order ASC, id ASC
        """,
        (draft_id,),
    )
    tier_rows = cur.fetchall() or []

    cur.execute(
        """
        SELECT c.*
        FROM deal_memo_royalty_tier_conditions c
        WHERE c.tier_id IN (
          SELECT t.id
          FROM deal_memo_royalty_tiers t
          WHERE t.rule_id IN (
            SELECT r.id FROM deal_memo_royalty_rules r WHERE r.deal_memo_draft_id = %s
          )
        )
        ORDER BY c.tier_id ASC, c.created_at ASC, c.id ASC
        """,
        (draft_id,),
    )
    cond_rows = cur.fetchall() or []

    conds_by_tier: Dict[str, List[dict]] = {}
    for c in cond_rows:
        conds_by_tier.setdefault(str(c["tier_id"]), []).append(
            {
                "kind": _s(c.get("kind")),
                "comparator": _s(c.get("comparator")),
                "value": float(c["value"]) if c.get("value") is not None else 0,
            }
        )

    def _merge_conditions(rows: List[dict]) -> List[dict]:
        out: List[dict] = []
        used = set()

        for i, row in enumerate(rows):
            if i in used:
                continue

            kind = _s(row.get("kind"))
            comp = _s(row.get("comparator"))
            val = row.get("value")

            if comp in (">=", ">"):
                for j, other in enumerate(rows):
                    if j == i or j in used:
                        continue
                    if _s(other.get("kind")) == kind and _s(other.get("comparator")) in ("<=", "<"):
                        low = float(val)
                        high = float(other.get("value"))
                        out.append({"kind": kind, "comparator": "between", "value": [low, high]})
                        used.add(i)
                        used.add(j)
                        break

            if i not in used:
                out.append(row)
                used.add(i)

        return out

    tiers_by_rule: Dict[str, List[dict]] = {}
    for t in tier_rows:
        tier_id = str(t["id"])
        tiers_by_rule.setdefault(str(t["rule_id"]), []).append(
            {
                "rate_percent": float(t["rate_percent"]) if t.get("rate_percent") is not None else 0,
                "base": _s(t.get("base")) or "list_price",
                "note": _s(t.get("note")),
                "conditions": _merge_conditions(conds_by_tier.get(tier_id, [])),
            }
        )

    out = {
        "author": {"first_rights": [], "subrights": []},
        "illustrator": {"first_rights": [], "subrights": []},
    }

    for r in rules:
        party = _s(r.get("party")).lower() or "author"
        rights_type = _s(r.get("rights_type")) or "first_rights"

        if party not in out:
            continue

        rule_obj = {
            "format": _s(r.get("format_label")),
            "name": _s(r.get("subrights_name")),
            "mode": _s(r.get("mode")) or ("tiered" if rights_type == "first_rights" else "fixed"),
            "base": _s(r.get("base")) or "list_price",
            "escalating": bool(r.get("escalating") or False),
            "flat_rate_percent": float(r["flat_rate_percent"]) if r.get("flat_rate_percent") is not None else None,
            "percent": float(r["percent"]) if r.get("percent") is not None else None,
            "note": _s(r.get("notes")),
            "tiers": tiers_by_rule.get(str(r["id"]), []),
        }
        out[party].setdefault(rights_type, []).append(rule_obj)

    out["author"].setdefault("subrights", [])
    out["illustrator"].setdefault("subrights", [])
    return out



def _list_of_dicts(value: Any) -> List[dict]:
    if not isinstance(value, list):
        return []
    return [dict(item) for item in value if isinstance(item, dict)]


def _first(value: dict, *keys: str, default: Any = "") -> Any:
    for key in keys:
        if key in value and value.get(key) not in (None, ""):
            return value.get(key)
    return default


def _normalized_role_code(raw: Any, fallback: str = "A01") -> str:
    value = _s(raw).upper()
    aliases = {
        "AUTHOR": "A01",
        "WRITER": "A01",
        "A01": "A01",
        "ILLUSTRATOR": "A12",
        "ARTIST": "A12",
        "ILLUSTRATION": "A12",
        "A12": "A12",
        "TRANSLATOR": "B06",
        "EDITOR": "B01",
        "NARRATOR": "E07",
        "PHOTOGRAPHER": "A13",
    }
    return aliases.get(value, value or fallback)


def _default_role_label(role_code: str) -> str:
    return {
        "A01": "Author",
        "A12": "Illustrator",
        "B01": "Editor",
        "B06": "Translator",
        "E07": "Narrator",
        "A13": "Photographer",
    }.get(role_code, role_code)


def _name_parts(contributor: dict) -> dict:
    nested = contributor.get("name") if isinstance(contributor.get("name"), dict) else {}
    return {
        "titles_before_names": _s(_first(contributor, "titles_before_names", "titlesBeforeNames", default=_first(nested, "titles_before_names", "titlesBeforeNames"))),
        "names_before_key": _s(_first(contributor, "names_before_key", "namesBeforeKey", "first_name", "firstName", default=_first(nested, "names_before_key", "namesBeforeKey", "first_name", "firstName"))),
        "prefix_to_key": _s(_first(contributor, "prefix_to_key", "prefixToKey", default=_first(nested, "prefix_to_key", "prefixToKey"))),
        "key_names": _s(_first(contributor, "key_names", "keyNames", "last_name", "lastName", default=_first(nested, "key_names", "keyNames", "last_name", "lastName"))),
        "suffix_to_key": _s(_first(contributor, "suffix_to_key", "suffixToKey", default=_first(nested, "suffix_to_key", "suffixToKey"))),
        "letters_after_names": _s(_first(contributor, "letters_after_names", "lettersAfterNames", default=_first(nested, "letters_after_names", "lettersAfterNames"))),
        "person_name_inverted": _s(_first(contributor, "person_name_inverted", "personNameInverted", default=_first(nested, "person_name_inverted", "personNameInverted"))),
    }


def _display_name(contributor: dict) -> str:
    raw_name = contributor.get("name")
    if isinstance(raw_name, str) and raw_name.strip():
        return raw_name.strip()
    return _s(
        _first(
            contributor,
            "display_name",
            "displayName",
            "full_name",
            "fullName",
            "contributor_name",
            "contributorName",
            "corporate_name",
            "corporateName",
        )
    ) or _person_name(raw_name)


def _address_rows(contributor: dict) -> List[dict]:
    rows = _list_of_dicts(_first(contributor, "addresses", "address_list", "addressList", default=[]))
    if rows:
        return rows
    address = contributor.get("address")
    return [dict(address)] if isinstance(address, dict) else []


def _representation_rows(contributor: dict) -> List[dict]:
    rows = _list_of_dicts(_first(contributor, "representations", "representation", "agents", default=[]))
    if rows:
        return rows
    agency = contributor.get("agency")
    if isinstance(agency, dict):
        return [dict(agency)]
    return []


def _contributors_from_body(body: Dict[str, Any]) -> List[dict]:
    """
    Return the richest structured contributor collection in the request.

    Some frontend versions send the same contributor records under more than
    one key. One collection can be stale while another contains the newly
    entered socials, awards, and identifiers. Prefer the collection carrying
    the most contributor detail instead of blindly accepting the first one.
    """
    candidate_sets: List[List[dict]] = []

    for key in ("contributors", "contributor_cards", "contributorCards"):
        rows = _list_of_dicts(body.get(key))
        if rows:
            candidate_sets.append(rows)

    if candidate_sets:
        def information_score(rows: List[dict]) -> int:
            score = 0

            for contributor in rows:
                socials = _list_of_dicts(
                    _first(
                        contributor,
                        "socials",
                        "social",
                        "social_profiles",
                        "socialProfiles",
                        "social_media_profiles",
                        "socialMediaProfiles",
                        default=[],
                    )
                )
                awards = _list_of_dicts(
                    _first(
                        contributor,
                        "awards",
                        "awards_honors",
                        "awards_and_honors",
                        "awardsHonors",
                        "honors",
                        default=[],
                    )
                )
                identifiers = _list_of_dicts(
                    _first(
                        contributor,
                        "identifiers",
                        "contributor_identifiers",
                        "contributorIdentifiers",
                        default=[],
                    )
                )

                # Strongly prefer the collection containing repeatable details.
                score += 100 * (len(socials) + len(awards) + len(identifiers))

                # Use ordinary populated fields as a tie-breaker.
                for key in (
                    "display_name",
                    "displayName",
                    "name",
                    "email",
                    "website",
                    "phone_number",
                    "phoneNumber",
                    "birth_date",
                    "birthDate",
                    "citizenship",
                ):
                    value = contributor.get(key)
                    if isinstance(value, str) and value.strip():
                        score += 1
                    elif value not in (None, "", [], {}):
                        score += 1

            return score

        return max(candidate_sets, key=information_score)

    rows: List[dict] = []

    author = body.get("author")
    if isinstance(author, dict):
        item = dict(author)
        item.setdefault("role_code", "A01")
        item.setdefault("role_label", "Author")
        item.setdefault("sequence_number", 1)
        item.setdefault("party_id", body.get("contributor_party_id"))
        rows.append(item)

    illustrator = body.get("illustrator")
    if isinstance(illustrator, dict):
        item = dict(illustrator)
        item.setdefault("role_code", "A12")
        item.setdefault("role_label", "Illustrator")
        item.setdefault("sequence_number", 1)
        item.setdefault(
            "party_id",
            body.get("contributor_party_id")
            if _s(body.get("contributor_role")).lower() == "illustrator"
            else None,
        )
        rows.append(item)

    # Keep legacy clients working. Only create the active legacy contributor
    # when no structured contributor object/list was supplied.
    if not rows:
        scope = _s(
            body.get("contributor_role")
            or body.get("contributorRole")
            or "author"
        ).lower()
        prefix = "illustrator" if scope == "illustrator" else "author"
        name_key = "illustrator_name" if prefix == "illustrator" else "author"
        name = _s(body.get(name_key))

        if name:
            rows.append(
                {
                    "party_id": body.get("contributor_party_id"),
                    "role_code": "A12" if prefix == "illustrator" else "A01",
                    "role_label": "Illustrator" if prefix == "illustrator" else "Author",
                    "sequence_number": 1,
                    "contributor_type": "person",
                    "display_name": name,
                    "email": body.get(f"{prefix}_email"),
                    "website": body.get(f"{prefix}_website"),
                    "phone_country_code": body.get(f"{prefix}_phone_country_code"),
                    "phone_number": body.get(f"{prefix}_phone_number"),
                    "birth_date": body.get(f"{prefix}_birth_date"),
                    "birth_city": body.get(f"{prefix}_birth_city"),
                    "birth_country": body.get(f"{prefix}_birth_country"),
                    "citizenship": body.get(f"{prefix}_citizenship"),
                    "address": {
                        "label": "primary",
                        "street": body.get(f"{prefix}_street"),
                        "city": body.get(f"{prefix}_city"),
                        "state": body.get(f"{prefix}_state"),
                        "zip": body.get(f"{prefix}_zip"),
                        "country": body.get(f"{prefix}_country"),
                        "is_non_us": body.get(f"{prefix}_non_us"),
                    },
                }
            )

    return rows


def _save_deal_memo_contributors(
    cur,
    tenant_id: str,
    draft_id: str,
    body: Dict[str, Any],
) -> None:
    contributors = _contributors_from_body(body)

    cur.execute(
        "DELETE FROM deal_memo_contributors WHERE tenant_id = %s AND deal_memo_draft_id = %s",
        (tenant_id, draft_id),
    )

    role_counts: Dict[str, int] = {}
    for contributor in contributors:
        role_code = _normalized_role_code(
            _first(contributor, "role_code", "roleCode", "contributor_role", "contributorRole", "role"),
            "A01",
        )
        role_counts[role_code] = role_counts.get(role_code, 0) + 1
        sequence_number = _int_or_none(
            _first(contributor, "sequence_number", "sequenceNumber", "sequence", default=role_counts[role_code])
        ) or role_counts[role_code]
        if sequence_number < 1:
            sequence_number = role_counts[role_code]

        contributor_type = _s(_first(contributor, "contributor_type", "contributorType", "type", default="person")).lower()
        if contributor_type in ("org", "corporate", "company"):
            contributor_type = "organization"
        if contributor_type not in ("person", "organization"):
            contributor_type = "person"

        parts = _name_parts(contributor)
        cur.execute(
            """
            INSERT INTO deal_memo_contributors (
                tenant_id, deal_memo_draft_id, party_id,
                role_code, role_label, sequence_number, contributor_type,
                display_name, titles_before_names, names_before_key,
                prefix_to_key, key_names, suffix_to_key, letters_after_names,
                person_name_inverted, pen_name, corporate_name,
                language_code, country_code, region_code,
                email, website, phone_country_code, phone_number,
                birth_date, death_date, birth_city, birth_country, citizenship
            )
            VALUES (
                %s, %s, %s,
                %s, %s, %s, %s,
                %s, %s, %s,
                %s, %s, %s, %s,
                %s, %s, %s,
                %s, %s, %s,
                %s, %s, %s, %s,
                %s, %s, %s, %s, %s
            )
            RETURNING id
            """,
            (
                tenant_id,
                draft_id,
                _s(_first(contributor, "party_id", "partyId", "contributor_party_id", "contributorPartyId")) or None,
                role_code,
                _s(_first(contributor, "role_label", "roleLabel")) or _default_role_label(role_code),
                sequence_number,
                contributor_type,
                _display_name(contributor),
                parts["titles_before_names"],
                parts["names_before_key"],
                parts["prefix_to_key"],
                parts["key_names"],
                parts["suffix_to_key"],
                parts["letters_after_names"],
                parts["person_name_inverted"],
                _s(_first(contributor, "pen_name", "penName")),
                _s(_first(contributor, "corporate_name", "corporateName")),
                _s(_first(contributor, "language_code", "languageCode", "language")),
                _s(_first(contributor, "country_code", "countryCode")),
                _s(_first(contributor, "region_code", "regionCode")),
                _s(contributor.get("email")),
                _s(contributor.get("website")),
                _s(_first(contributor, "phone_country_code", "phoneCountryCode")),
                _s(_first(contributor, "phone_number", "phoneNumber", "phone")),
                _date_or_none(_first(contributor, "birth_date", "birthDate")),
                _date_or_none(_first(contributor, "death_date", "deathDate")),
                _s(_first(contributor, "birth_city", "birthCity")),
                _s(_first(contributor, "birth_country", "birthCountry")),
                _s(contributor.get("citizenship")),
            ),
        )
        contributor_id = str(cur.fetchone()["id"])

        for item_order, address in enumerate(_address_rows(contributor)):
            cur.execute(
                """
                INSERT INTO deal_memo_contributor_addresses (
                    tenant_id, deal_memo_contributor_id, label,
                    street, city, state, zip, country, is_non_us, item_order
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    tenant_id,
                    contributor_id,
                    _s(address.get("label")) or ("primary" if item_order == 0 else "other"),
                    _s(address.get("street")),
                    _s(address.get("city")),
                    _s(address.get("state")),
                    _s(_first(address, "zip", "postal_code", "postalCode")),
                    _s(address.get("country")),
                    _bool(_first(address, "is_non_us", "isNonUs", "nonUS", "non_us", default=False)),
                    item_order,
                ),
            )

        socials = _list_of_dicts(
            _first(
                contributor,
                "socials",
                "social",
                "social_profiles",
                "socialProfiles",
                "social_media_profiles",
                "socialMediaProfiles",
                default=[],
            )
        )
        for item_order, social in enumerate(socials):
            platform = _s(_first(social, "platform", "type", "name"))
            url = _s(_first(social, "url", "value", "handle"))
            if not platform or not url:
                continue
            cur.execute(
                """
                INSERT INTO deal_memo_contributor_socials (
                    tenant_id, deal_memo_contributor_id, platform, url, item_order
                ) VALUES (%s, %s, %s, %s, %s)
                """,
                (tenant_id, contributor_id, platform, url, item_order),
            )

        awards = _list_of_dicts(
            _first(
                contributor,
                "awards",
                "awards_honors",
                "awards_and_honors",
                "awardsHonors",
                "honors",
                default=[],
            )
        )
        for item_order, award in enumerate(awards):
            award_name = _s(_first(award, "award_name", "awardName", "name"))
            if not award_name:
                continue
            cur.execute(
                """
                INSERT INTO deal_memo_contributor_awards (
                    tenant_id, deal_memo_contributor_id, award_name,
                    award_year, award_result, notes, item_order
                ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    tenant_id,
                    contributor_id,
                    award_name,
                    _s(_first(award, "award_year", "awardYear", "year")),
                    _s(_first(award, "award_result", "awardResult", "result")),
                    _s(award.get("notes")),
                    item_order,
                ),
            )

        identifiers = _list_of_dicts(_first(contributor, "identifiers", "contributor_identifiers", "contributorIdentifiers", default=[]))
        for item_order, identifier in enumerate(identifiers):
            identifier_type = _s(_first(identifier, "identifier_type", "identifierType", "type", "scheme"))
            identifier_value = _s(_first(identifier, "identifier_value", "identifierValue", "value", "id"))
            if not identifier_type or not identifier_value:
                continue
            cur.execute(
                """
                INSERT INTO deal_memo_contributor_identifiers (
                    tenant_id, deal_memo_contributor_id,
                    identifier_type, identifier_value, item_order
                ) VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (deal_memo_contributor_id, identifier_type, identifier_value)
                DO UPDATE SET item_order = EXCLUDED.item_order, updated_at = now()
                """,
                (tenant_id, contributor_id, identifier_type, identifier_value, item_order),
            )

        for item_order, representation in enumerate(_representation_rows(contributor)):
            cur.execute(
                """
                INSERT INTO deal_memo_contributor_representations (
                    tenant_id, deal_memo_contributor_id,
                    agency_party_id, agent_party_id,
                    agency_name, agent_name, email, website,
                    phone_country_code, phone_number,
                    is_primary, item_order
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id
                """,
                (
                    tenant_id,
                    contributor_id,
                    _s(_first(representation, "agency_party_id", "agencyPartyId")) or None,
                    _s(_first(representation, "agent_party_id", "agentPartyId")) or None,
                    _s(_first(representation, "agency_name", "agencyName", "name")),
                    _s(_first(representation, "agent_name", "agentName")),
                    _s(_first(representation, "email", "agent_email", "agentEmail", "agency_email", "agencyEmail")),
                    _s(_first(representation, "website", "agency_website", "agencyWebsite")),
                    _s(_first(representation, "phone_country_code", "phoneCountryCode", "agent_phone_country_code", "agentPhoneCountryCode")),
                    _s(_first(representation, "phone_number", "phoneNumber", "agent_phone_number", "agentPhoneNumber")),
                    _bool(_first(representation, "is_primary", "isPrimary", default=(item_order == 0))),
                    item_order,
                ),
            )
            representation_id = str(cur.fetchone()["id"])
            rep_addresses = _address_rows(representation)
            for address_order, address in enumerate(rep_addresses):
                cur.execute(
                    """
                    INSERT INTO deal_memo_representation_addresses (
                        tenant_id, representation_id, label,
                        street, city, state, zip, country, is_non_us, item_order
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        tenant_id,
                        representation_id,
                        _s(address.get("label")) or ("primary" if address_order == 0 else "other"),
                        _s(address.get("street")),
                        _s(address.get("city")),
                        _s(address.get("state")),
                        _s(_first(address, "zip", "postal_code", "postalCode")),
                        _s(address.get("country")),
                        _bool(_first(address, "is_non_us", "isNonUs", "nonUS", "non_us", default=False)),
                        address_order,
                    ),
                )


def _hydrate_deal_memo_contributors(cur, tenant_id: str, draft_id: str) -> List[dict]:
    cur.execute(
        """
        SELECT *
        FROM deal_memo_contributors
        WHERE tenant_id = %s AND deal_memo_draft_id = %s
        ORDER BY sequence_number ASC, created_at ASC, id ASC
        """,
        (tenant_id, draft_id),
    )
    contributors = [dict(row) for row in (cur.fetchall() or [])]
    out: List[dict] = []

    for row in contributors:
        contributor_id = str(row["id"])
        cur.execute(
            """
            SELECT label, street, city, state, zip, country, is_non_us, item_order
            FROM deal_memo_contributor_addresses
            WHERE tenant_id = %s AND deal_memo_contributor_id = %s
            ORDER BY item_order ASC, created_at ASC, id ASC
            """,
            (tenant_id, contributor_id),
        )
        addresses = [
            {
                "label": _s(a.get("label")),
                "street": _s(a.get("street")),
                "city": _s(a.get("city")),
                "state": _s(a.get("state")),
                "zip": _s(a.get("zip")),
                "country": _s(a.get("country")),
                "is_non_us": bool(a.get("is_non_us") or False),
                "isNonUs": bool(a.get("is_non_us") or False),
                "nonUS": bool(a.get("is_non_us") or False),
            }
            for a in (cur.fetchall() or [])
        ]

        cur.execute(
            """
            SELECT platform, url, item_order
            FROM deal_memo_contributor_socials
            WHERE tenant_id = %s AND deal_memo_contributor_id = %s
            ORDER BY item_order ASC, created_at ASC, id ASC
            """,
            (tenant_id, contributor_id),
        )
        socials = [
            {
                "platform": _s(item.get("platform")),
                "url": _s(item.get("url")),
                "handle": _s(item.get("url")),
                "item_order": int(item.get("item_order") or 0),
            }
            for item in (cur.fetchall() or [])
        ]

        cur.execute(
            """
            SELECT award_name, award_year, award_result, notes, item_order
            FROM deal_memo_contributor_awards
            WHERE tenant_id = %s AND deal_memo_contributor_id = %s
            ORDER BY item_order ASC, created_at ASC, id ASC
            """,
            (tenant_id, contributor_id),
        )
        awards = [
            {
                "award_name": _s(item.get("award_name")),
                "awardName": _s(item.get("award_name")),
                "name": _s(item.get("award_name")),
                "award_year": _s(item.get("award_year")),
                "awardYear": _s(item.get("award_year")),
                "year": _s(item.get("award_year")),
                "award_result": _s(item.get("award_result")),
                "awardResult": _s(item.get("award_result")),
                "result": _s(item.get("award_result")),
                "notes": _s(item.get("notes")),
                "item_order": int(item.get("item_order") or 0),
            }
            for item in (cur.fetchall() or [])
        ]

        cur.execute(
            """
            SELECT identifier_type, identifier_value, item_order
            FROM deal_memo_contributor_identifiers
            WHERE tenant_id = %s AND deal_memo_contributor_id = %s
            ORDER BY item_order ASC, created_at ASC, id ASC
            """,
            (tenant_id, contributor_id),
        )
        identifiers = [
            {
                "identifier_type": _s(item.get("identifier_type")),
                "identifierType": _s(item.get("identifier_type")),
                "type": _s(item.get("identifier_type")),
                "identifier_value": _s(item.get("identifier_value")),
                "identifierValue": _s(item.get("identifier_value")),
                "value": _s(item.get("identifier_value")),
                "item_order": int(item.get("item_order") or 0),
            }
            for item in (cur.fetchall() or [])
        ]

        cur.execute(
            """
            SELECT *
            FROM deal_memo_contributor_representations
            WHERE tenant_id = %s AND deal_memo_contributor_id = %s
            ORDER BY item_order ASC, created_at ASC, id ASC
            """,
            (tenant_id, contributor_id),
        )
        representations: List[dict] = []
        for rep_row in (cur.fetchall() or []):
            rep = dict(rep_row)
            rep_id = str(rep["id"])
            cur.execute(
                """
                SELECT label, street, city, state, zip, country, is_non_us, item_order
                FROM deal_memo_representation_addresses
                WHERE tenant_id = %s AND representation_id = %s
                ORDER BY item_order ASC, created_at ASC, id ASC
                """,
                (tenant_id, rep_id),
            )
            rep_addresses = [
                {
                    "label": _s(a.get("label")),
                    "street": _s(a.get("street")),
                    "city": _s(a.get("city")),
                    "state": _s(a.get("state")),
                    "zip": _s(a.get("zip")),
                    "country": _s(a.get("country")),
                    "is_non_us": bool(a.get("is_non_us") or False),
                    "isNonUs": bool(a.get("is_non_us") or False),
                    "nonUS": bool(a.get("is_non_us") or False),
                }
                for a in (cur.fetchall() or [])
            ]
            rep.pop("tenant_id", None)
            rep.pop("deal_memo_contributor_id", None)
            rep["id"] = rep_id
            rep["agencyPartyId"] = _s(rep.get("agency_party_id"))
            rep["agentPartyId"] = _s(rep.get("agent_party_id"))
            rep["agencyName"] = _s(rep.get("agency_name"))
            rep["agentName"] = _s(rep.get("agent_name"))
            rep["phoneCountryCode"] = _s(rep.get("phone_country_code"))
            rep["phoneNumber"] = _s(rep.get("phone_number"))
            rep["isPrimary"] = bool(rep.get("is_primary") or False)
            rep["addresses"] = rep_addresses
            rep["address"] = rep_addresses[0] if rep_addresses else {}
            representations.append(rep)

        item = {
            "id": contributor_id,
            "party_id": _s(row.get("party_id")),
            "partyId": _s(row.get("party_id")),
            "role_code": _s(row.get("role_code")),
            "roleCode": _s(row.get("role_code")),
            "role_label": _s(row.get("role_label")),
            "roleLabel": _s(row.get("role_label")),
            "sequence_number": int(row.get("sequence_number") or 1),
            "sequenceNumber": int(row.get("sequence_number") or 1),
            "contributor_type": _s(row.get("contributor_type")) or "person",
            "contributorType": _s(row.get("contributor_type")) or "person",
            "display_name": _s(row.get("display_name")),
            "displayName": _s(row.get("display_name")),
            "name": _s(row.get("display_name")),
            "titles_before_names": _s(row.get("titles_before_names")),
            "titlesBeforeNames": _s(row.get("titles_before_names")),
            "names_before_key": _s(row.get("names_before_key")),
            "namesBeforeKey": _s(row.get("names_before_key")),
            "prefix_to_key": _s(row.get("prefix_to_key")),
            "prefixToKey": _s(row.get("prefix_to_key")),
            "key_names": _s(row.get("key_names")),
            "keyNames": _s(row.get("key_names")),
            "suffix_to_key": _s(row.get("suffix_to_key")),
            "suffixToKey": _s(row.get("suffix_to_key")),
            "letters_after_names": _s(row.get("letters_after_names")),
            "lettersAfterNames": _s(row.get("letters_after_names")),
            "person_name_inverted": _s(row.get("person_name_inverted")),
            "personNameInverted": _s(row.get("person_name_inverted")),
            "pen_name": _s(row.get("pen_name")),
            "penName": _s(row.get("pen_name")),
            "corporate_name": _s(row.get("corporate_name")),
            "corporateName": _s(row.get("corporate_name")),
            "language_code": _s(row.get("language_code")),
            "languageCode": _s(row.get("language_code")),
            "country_code": _s(row.get("country_code")),
            "countryCode": _s(row.get("country_code")),
            "region_code": _s(row.get("region_code")),
            "regionCode": _s(row.get("region_code")),
            "email": _s(row.get("email")),
            "website": _s(row.get("website")),
            "phone_country_code": _s(row.get("phone_country_code")),
            "phoneCountryCode": _s(row.get("phone_country_code")),
            "phone_number": _s(row.get("phone_number")),
            "phoneNumber": _s(row.get("phone_number")),
            "birth_date": _jsonable(row.get("birth_date")),
            "birthDate": _jsonable(row.get("birth_date")),
            "death_date": _jsonable(row.get("death_date")),
            "deathDate": _jsonable(row.get("death_date")),
            "birth_city": _s(row.get("birth_city")),
            "birthCity": _s(row.get("birth_city")),
            "birth_country": _s(row.get("birth_country")),
            "birthCountry": _s(row.get("birth_country")),
            "citizenship": _s(row.get("citizenship")),
            "addresses": addresses,
            "address": addresses[0] if addresses else {},
            "socials": socials,
            "socialProfiles": socials,
            "awards": awards,
            "identifiers": identifiers,
            "contributorIdentifiers": identifiers,
            "representations": representations,
        }
        out.append(item)

    return out

def _row_to_draft(cur, tenant_id: str, row: dict) -> dict:
    created_at = row.get("created_at")
    updated_at = row.get("updated_at")
    created_ms = int(created_at.timestamp() * 1000) if hasattr(created_at, "timestamp") else _now_ms()
    updated_ms = int(updated_at.timestamp() * 1000) if hasattr(updated_at, "timestamp") else created_ms

    author_address = {
        "street": _s(row.get("author_street")),
        "city": _s(row.get("author_city")),
        "state": _s(row.get("author_state")),
        "zip": _s(row.get("author_zip")),
        "country": _s(row.get("author_country")),
        "nonUS": bool(row.get("author_non_us") or False),
    }

    illustrator_address = {
        "street": _s(row.get("illustrator_street")),
        "city": _s(row.get("illustrator_city")),
        "state": _s(row.get("illustrator_state")),
        "zip": _s(row.get("illustrator_zip")),
        "country": _s(row.get("illustrator_country")),
        "nonUS": bool(row.get("illustrator_non_us") or False),
    }

    out = {
        "uid": _s(row.get("uid")),
        "name": _s(row.get("name")) or _s(row.get("title")) or "Untitled Deal Memo",
        "title": _s(row.get("title")),
        "status": _s(row.get("status")) or "draft",
        "contributor_role": _s(row.get("contributor_role")) or "author",
        "selected_template_id": _s(row.get("selected_template_id")),
        "work_id": _s(row.get("work_id")),
        "contributor_party_id": _s(row.get("contributor_party_id")),
        "agency_party_id": _s(row.get("agency_party_id")),
        "agent_party_id": _s(row.get("agent_party_id")),

        "author": _s(row.get("author")),
        "author_email": _s(row.get("author_email")),
        "author_website": _s(row.get("author_website")),
        "author_phone_country_code": _s(row.get("author_phone_country_code")),
        "author_phone_number": _s(row.get("author_phone_number")),
        "author_address": author_address,
        "author_street": author_address["street"],
        "author_city": author_address["city"],
        "author_state": author_address["state"],
        "author_zip": author_address["zip"],
        "author_country": author_address["country"],
        "author_non_us": bool(row.get("author_non_us") or False),
        "author_birth_date": _jsonable(row.get("author_birth_date")),
        "author_birth_city": _s(row.get("author_birth_city")),
        "author_birth_country": _s(row.get("author_birth_country")),
        "author_citizenship": _s(row.get("author_citizenship")),
        "author_advance": float(row["author_advance"]) if row.get("author_advance") is not None else None,

        "illustrator_name": _s(row.get("illustrator_name")),
        "illustrator_email": _s(row.get("illustrator_email")),
        "illustrator_website": _s(row.get("illustrator_website")),
        "illustrator_phone_country_code": _s(row.get("illustrator_phone_country_code")),
        "illustrator_phone_number": _s(row.get("illustrator_phone_number")),
        "illustrator_address": illustrator_address,
        "illustrator_street": illustrator_address["street"],
        "illustrator_city": illustrator_address["city"],
        "illustrator_state": illustrator_address["state"],
        "illustrator_zip": illustrator_address["zip"],
        "illustrator_country": illustrator_address["country"],
        "illustrator_non_us": bool(row.get("illustrator_non_us") or False),
        "illustrator_birth_date": _jsonable(row.get("illustrator_birth_date")),
        "illustrator_birth_city": _s(row.get("illustrator_birth_city")),
        "illustrator_birth_country": _s(row.get("illustrator_birth_country")),
        "illustrator_citizenship": _s(row.get("illustrator_citizenship")),
        "illustrator_advance": float(row["illustrator_advance"]) if row.get("illustrator_advance") is not None else None,

        "effectiveDate": _jsonable(row.get("effective_date")),
        "series": bool(row.get("series") or False),
        "series_title": _s(row.get("series_title")),
        "number_of_books": int(row.get("number_of_books") or 0),
        "shortDescription": _s(row.get("short_description")),
        "projectedPublicationDate": _s(row.get("projected_publication_date")),
        "projectedRetailPrice": _s(row.get("projected_retail_price")),
        "territoriesRights": _s(row.get("territories_rights")),
        "optionDeleted": bool(row.get("option_deleted") or False),
        "optionClause": _s(row.get("option_clause")),
        "optionSupplement": _s(row.get("option_supplement")),
        "compCopiesContributor": row.get("comp_copies_contributor"),
        "compCopiesAgent": row.get("comp_copies_agent"),
        "deliveryMode": _s(row.get("delivery_mode")),
        "deliveryClause": _s(row.get("delivery_clause")),
        "deliveryDate": _jsonable(row.get("delivery_date")),
        "generated_contract_filename": _s(row.get("generated_contract_filename")),
        "generated_contract_s3_key": _s(row.get("generated_contract_s3_key")),
        "generated_at": _jsonable(row.get("generated_at")),

        "createdAt": created_ms,
        "updatedAt": updated_ms,
    }

    if row.get("agency_party_id"):
        try:
            agency = _get_agency_detail(cur, tenant_id, str(row["agency_party_id"]))
            out["agency"] = agency
            out["agents"] = agency.get("agents", [])

            out["agency_name"] = _s(agency.get("agency_name"))
            out["agency_email"] = _s(agency.get("agency_email"))
            out["agency_website"] = _s(agency.get("agency_website"))
            out["agency_street"] = _s(agency.get("agency_street"))
            out["agency_city"] = _s(agency.get("agency_city"))
            out["agency_state"] = _s(agency.get("agency_state"))
            out["agency_zip"] = _s(agency.get("agency_zip"))
            out["agency_country"] = _s(agency.get("agency_country"))
            out["agency_clause"] = _s(agency.get("agency_clause"))

            # legacy aliases used by AuthorAgencyCard
            out["author_agency_name"] = _s(agency.get("agency_name"))
            out["author_agency_email"] = _s(agency.get("agency_email"))
            out["author_agency_website"] = _s(agency.get("agency_website"))
            out["author_agency_street"] = _s(agency.get("agency_street"))
            out["author_agency_city"] = _s(agency.get("agency_city"))
            out["author_agency_state"] = _s(agency.get("agency_state"))
            out["author_agency_zip"] = _s(agency.get("agency_zip"))
            out["author_agency_country"] = _s(agency.get("agency_country"))
            out["author_agency_clause"] = _s(agency.get("agency_clause"))
            out["author_agency_address"] = {
                "street": _s(agency.get("agency_street")),
                "city": _s(agency.get("agency_city")),
                "state": _s(agency.get("agency_state")),
                "zip": _s(agency.get("agency_zip")),
                "country": _s(agency.get("agency_country")),
            }

            primary = next((a for a in agency.get("agents", []) if a.get("is_primary")), None)
            if primary:
                out["agent_name"] = _s(primary.get("agent_name"))
                out["agent_email"] = _s(primary.get("agent_email"))
                out["agent_phone_country_code"] = _s(primary.get("agent_phone_country_code"))
                out["agent_phone_number"] = _s(primary.get("agent_phone_number"))

                # legacy aliases used by AuthorAgencyCard
                out["author_agent_name"] = _s(primary.get("agent_name"))
                out["author_agent_email"] = _s(primary.get("agent_email"))
                out["author_agent_phone_country_code"] = _s(primary.get("agent_phone_country_code"))
                out["authors_agent_phone_country_code"] = _s(primary.get("agent_phone_country_code"))
                out["author_agent_phone_number"] = _s(primary.get("agent_phone_number"))
                out["authors_agent_phone_number"] = _s(primary.get("agent_phone_number"))

            out["authors_agent"] = True
            out["author_has_agency"] = True
        except Exception:
            pass

    draft_id = str(row["id"])
    out["advanceSchedule"] = _hydrate_advance_schedule(cur, draft_id)
    out["royalties"] = _hydrate_royalties(cur, draft_id)

    contributors = _hydrate_deal_memo_contributors(cur, tenant_id, draft_id)
    out["contributors"] = contributors
    out["contributor_cards"] = contributors
    out["contributorCards"] = contributors

    # Preserve legacy author/illustrator response fields while exposing every
    # structured field through the normalized contributor objects.
    author_contributor = next((c for c in contributors if c.get("role_code") == "A01"), None)
    illustrator_contributor = next((c for c in contributors if c.get("role_code") == "A12"), None)
    if author_contributor:
        out["authorContributor"] = author_contributor
        out["author_contributor"] = author_contributor
        out["author_card"] = author_contributor

        # Flat compatibility aliases used by older Deal Memo hydration paths.
        out["author_socials"] = author_contributor.get("socials") or []
        out["author_social_profiles"] = author_contributor.get("socials") or []
        out["author_social_media_profiles"] = author_contributor.get("socials") or []
        out["author_awards"] = author_contributor.get("awards") or []
        out["author_awards_honors"] = author_contributor.get("awards") or []
        out["author_awards_and_honors"] = author_contributor.get("awards") or []
        out["author_identifiers"] = author_contributor.get("identifiers") or []
        out["author_contributor_identifiers"] = author_contributor.get("identifiers") or []

    if illustrator_contributor:
        out["illustrator"] = illustrator_contributor
        out["illustratorContributor"] = illustrator_contributor
        out["illustrator_contributor"] = illustrator_contributor
        out["illustrator_card"] = illustrator_contributor

        out["illustrator_socials"] = illustrator_contributor.get("socials") or []
        out["illustrator_social_profiles"] = illustrator_contributor.get("socials") or []
        out["illustrator_social_media_profiles"] = illustrator_contributor.get("socials") or []
        out["illustrator_awards"] = illustrator_contributor.get("awards") or []
        out["illustrator_awards_honors"] = illustrator_contributor.get("awards") or []
        out["illustrator_awards_and_honors"] = illustrator_contributor.get("awards") or []
        out["illustrator_identifiers"] = illustrator_contributor.get("identifiers") or []
        out["illustrator_contributor_identifiers"] = illustrator_contributor.get("identifiers") or []

    return out


def _fetch_one_draft(cur, tenant_id: str, uid: str) -> dict | None:
    cur.execute(
        f"""
        SELECT *
        FROM {DEAL_MEMO_TABLE}
        WHERE tenant_id = %s
          AND uid = %s
        LIMIT 1
        """,
        (tenant_id, uid),
    )
    row = cur.fetchone()
    return dict(row) if row else None


def _clear_children(cur, draft_id: str) -> None:
    # Child address/social/award/identifier/representation rows cascade from
    # deal_memo_contributors.
    cur.execute(
        "DELETE FROM deal_memo_contributors WHERE deal_memo_draft_id = %s",
        (draft_id,),
    )

    cur.execute(
        """
        DELETE FROM deal_memo_advance_installments
        WHERE deal_memo_draft_id = %s
        """,
        (draft_id,),
    )
    cur.execute(
        """
        DELETE FROM deal_memo_royalty_tier_conditions
        WHERE tier_id IN (
            SELECT t.id
            FROM deal_memo_royalty_tiers t
            WHERE t.rule_id IN (
                SELECT r.id
                FROM deal_memo_royalty_rules r
                WHERE r.deal_memo_draft_id = %s
            )
        )
        """,
        (draft_id,),
    )
    cur.execute(
        """
        DELETE FROM deal_memo_royalty_tiers
        WHERE rule_id IN (
            SELECT r.id
            FROM deal_memo_royalty_rules r
            WHERE r.deal_memo_draft_id = %s
        )
        """,
        (draft_id,),
    )
    cur.execute(
        """
        DELETE FROM deal_memo_royalty_rules
        WHERE deal_memo_draft_id = %s
        """,
        (draft_id,),
    )


def _save_advance_schedule(cur, tenant_id: str, draft_id: str, body: Dict[str, Any]) -> None:
    rows = body.get("advanceSchedule") or body.get("advance_schedule") or []
    if not isinstance(rows, list):
        rows = []

    for idx, row in enumerate(rows, start=1):
        if not isinstance(row, dict):
            continue
        cur.execute(
            """
            INSERT INTO deal_memo_advance_installments (
                tenant_id, deal_memo_draft_id, installment_order, amount_type, value, trigger
            )
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (
                tenant_id,
                draft_id,
                idx,
                _s(row.get("amountType") or row.get("amount_type") or "percent"),
                _float_or_none(row.get("value")),
                _s(row.get("trigger")),
            ),
        )


def _insert_condition(cur, tenant_id: str, tier_id: str, cond: dict) -> None:
    kind = _s(cond.get("kind")) or "units"
    comparator = _s(cond.get("comparator")) or "<="
    value = cond.get("value")

    if comparator == "between" and isinstance(value, (list, tuple)) and len(value) == 2:
        low = _float_or_none(value[0])
        high = _float_or_none(value[1])
        if low is not None:
            cur.execute(
                """
                INSERT INTO deal_memo_royalty_tier_conditions (
                    tenant_id, tier_id, kind, comparator, value
                )
                VALUES (%s, %s, %s, %s, %s)
                """,
                (tenant_id, tier_id, kind, ">=", low),
            )
        if high is not None:
            cur.execute(
                """
                INSERT INTO deal_memo_royalty_tier_conditions (
                    tenant_id, tier_id, kind, comparator, value
                )
                VALUES (%s, %s, %s, %s, %s)
                """,
                (tenant_id, tier_id, kind, "<=", high),
            )
        return

    numeric_value = _float_or_none(value)
    if numeric_value is None:
        return

    cur.execute(
        """
        INSERT INTO deal_memo_royalty_tier_conditions (
            tenant_id, tier_id, kind, comparator, value
        )
        VALUES (%s, %s, %s, %s, %s)
        """,
        (tenant_id, tier_id, kind, comparator, numeric_value),
    )


def _save_royalties(cur, tenant_id: str, draft_id: str, body: Dict[str, Any]) -> None:
    royalties = body.get("royalties") or {}
    if not isinstance(royalties, dict):
        return

    for party in ("author", "illustrator"):
        party_block = royalties.get(party) or {}
        if not isinstance(party_block, dict):
            continue

        for rights_type in ("first_rights", "subrights"):
            rows = party_block.get(rights_type) or []
            if not isinstance(rows, list):
                continue

            for row in rows:
                if not isinstance(row, dict):
                    continue

                cur.execute(
                    """
                    INSERT INTO deal_memo_royalty_rules (
                        tenant_id, deal_memo_draft_id, party, rights_type,
                        format_label, subrights_name, mode, base, escalating,
                        flat_rate_percent, percent, notes
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    RETURNING id
                    """,
                    (
                        tenant_id,
                        draft_id,
                        party,
                        rights_type,
                        _s(row.get("format")),
                        _s(row.get("name")),
                        _s(row.get("mode") or ("tiered" if rights_type == "first_rights" else "fixed")),
                        _s(row.get("base") or "list_price"),
                        _bool(row.get("escalating")),
                        _float_or_none(row.get("flat_rate_percent")),
                        _float_or_none(row.get("percent")),
                        _s(row.get("note") or row.get("notes")),
                    ),
                )
                rule_id = str(cur.fetchone()["id"])

                tiers = row.get("tiers") or []
                if not isinstance(tiers, list):
                    tiers = []

                for idx, tier in enumerate(tiers, start=1):
                    if not isinstance(tier, dict):
                        continue

                    cur.execute(
                        """
                        INSERT INTO deal_memo_royalty_tiers (
                            tenant_id, rule_id, tier_order, rate_percent, base, note
                        )
                        VALUES (%s, %s, %s, %s, %s, %s)
                        RETURNING id
                        """,
                        (
                            tenant_id,
                            rule_id,
                            idx,
                            _float_or_none(tier.get("rate_percent")) or 0,
                            _s(tier.get("base") or "list_price"),
                            _s(tier.get("note")),
                        ),
                    )
                    tier_id = str(cur.fetchone()["id"])

                    conditions = tier.get("conditions") or []
                    if not isinstance(conditions, list):
                        conditions = []

                    for cond in conditions:
                        if isinstance(cond, dict):
                            _insert_condition(cur, tenant_id, tier_id, cond)


@router.get("/dealmemos/_where")
def where_file() -> dict:
    return {"storage": "postgres", "table": DEAL_MEMO_TABLE}


@router.post("/dealmemos/_touch")
def touch_file() -> dict:
    return {"ok": True, "storage": "postgres", "table": DEAL_MEMO_TABLE}


@router.get("/dealmemos")
def list_deal_memos(tenant_slug: str = Query(DEFAULT_TENANT_SLUG)) -> List[dict]:
    with db_conn() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            _require_tables(cur)
            tenant_id = _tenant_id(cur, tenant_slug)
            cur.execute(
                f"""
                SELECT *
                FROM {DEAL_MEMO_TABLE}
                WHERE tenant_id = %s
                ORDER BY updated_at DESC NULLS LAST, created_at DESC
                """,
                (tenant_id,),
            )
            rows = cur.fetchall() or []
            return [_row_to_draft(cur, tenant_id, dict(r)) for r in rows]


@router.get("/dealmemos/{uid}")
def get_deal_memo(uid: str, tenant_slug: str = Query(DEFAULT_TENANT_SLUG)) -> dict:
    uid = _s(uid)
    if not uid:
        raise HTTPException(status_code=400, detail="uid required")

    with db_conn() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            _require_tables(cur)
            tenant_id = _tenant_id(cur, tenant_slug)
            row = _fetch_one_draft(cur, tenant_id, uid)
            if not row:
                raise HTTPException(status_code=404, detail="Draft not found")
            return _row_to_draft(cur, tenant_id, row)


@router.post("/dealmemos")
def upsert_deal_memo(
    body: Dict[str, Any] = Body(...),
    tenant_slug: str = Query(DEFAULT_TENANT_SLUG),
) -> dict:
    if not isinstance(body, dict):
        raise HTTPException(status_code=400, detail="Invalid JSON body")

    uid = _s(body.get("uid")) or _rand_uid()
    title = _s(body.get("title"))
    name = _s(body.get("name")) or title or "Untitled Deal Memo"
    contributor_role = _s(body.get("contributor_role") or body.get("contributorRole") or "author").lower()
    if contributor_role not in ("author", "illustrator", "other"):
        contributor_role = "author"

    author_obj = body.get("author")
    illustrator_obj = body.get("illustrator")

    author_address = _person_address(
        author_obj,
        body.get("author_address") if isinstance(body.get("author_address"), dict) else {
            "street": body.get("author_street"),
            "city": body.get("author_city"),
            "state": body.get("author_state"),
            "zip": body.get("author_zip"),
            "country": body.get("author_country"),
            "nonUS": body.get("author_non_us"),
        },
    )

    illustrator_address = _person_address(
        illustrator_obj,
        body.get("illustrator_address") if isinstance(body.get("illustrator_address"), dict) else {
            "street": body.get("illustrator_street"),
            "city": body.get("illustrator_city"),
            "state": body.get("illustrator_state"),
            "zip": body.get("illustrator_zip"),
            "country": body.get("illustrator_country"),
            "nonUS": body.get("illustrator_non_us"),
        },
    )

    with db_conn() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            _require_tables(cur)
            tenant_id = _tenant_id(cur, tenant_slug)

            cur.execute(
                f"""
                INSERT INTO {DEAL_MEMO_TABLE} (
                    tenant_id, uid, name, title, status, contributor_role, selected_template_id,
                    work_id, contributor_party_id, agency_party_id, agent_party_id,
                    author, author_email, author_website, author_phone_country_code, author_phone_number,
                    author_street, author_city, author_state, author_zip, author_country, author_non_us,
                    author_birth_date, author_birth_city, author_birth_country, author_citizenship, author_advance,
                    illustrator_name, illustrator_email, illustrator_website,
                    illustrator_phone_country_code, illustrator_phone_number,
                    illustrator_street, illustrator_city, illustrator_state, illustrator_zip, illustrator_country, illustrator_non_us,
                    illustrator_birth_date, illustrator_birth_city, illustrator_birth_country, illustrator_citizenship, illustrator_advance,
                    effective_date, series, series_title, number_of_books,
                    short_description, projected_publication_date, projected_retail_price,
                    territories_rights, option_deleted, option_clause, option_supplement,
                    comp_copies_contributor, comp_copies_agent,
                    delivery_mode, delivery_clause, delivery_date,
                    generated_contract_filename, generated_contract_s3_key, generated_at
                )
                VALUES (
                    %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s,
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s,
                    %s, %s, %s,
                    %s, %s,
                    %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, %s,
                    %s, %s, %s,
                    %s, %s, %s, %s,
                    %s, %s,
                    %s, %s, %s,
                    %s, %s, %s
                )
                ON CONFLICT (tenant_id, uid) DO UPDATE SET
                    name = EXCLUDED.name,
                    title = EXCLUDED.title,
                    status = EXCLUDED.status,
                    contributor_role = EXCLUDED.contributor_role,
                    selected_template_id = EXCLUDED.selected_template_id,
                    work_id = EXCLUDED.work_id,
                    contributor_party_id = EXCLUDED.contributor_party_id,
                    agency_party_id = EXCLUDED.agency_party_id,
                    agent_party_id = EXCLUDED.agent_party_id,
                    author = EXCLUDED.author,
                    author_email = EXCLUDED.author_email,
                    author_website = EXCLUDED.author_website,
                    author_phone_country_code = EXCLUDED.author_phone_country_code,
                    author_phone_number = EXCLUDED.author_phone_number,
                    author_street = EXCLUDED.author_street,
                    author_city = EXCLUDED.author_city,
                    author_state = EXCLUDED.author_state,
                    author_zip = EXCLUDED.author_zip,
                    author_country = EXCLUDED.author_country,
                    author_non_us = EXCLUDED.author_non_us,
                    author_birth_date = EXCLUDED.author_birth_date,
                    author_birth_city = EXCLUDED.author_birth_city,
                    author_birth_country = EXCLUDED.author_birth_country,
                    author_citizenship = EXCLUDED.author_citizenship,
                    author_advance = EXCLUDED.author_advance,
                    illustrator_name = EXCLUDED.illustrator_name,
                    illustrator_email = EXCLUDED.illustrator_email,
                    illustrator_website = EXCLUDED.illustrator_website,
                    illustrator_phone_country_code = EXCLUDED.illustrator_phone_country_code,
                    illustrator_phone_number = EXCLUDED.illustrator_phone_number,
                    illustrator_street = EXCLUDED.illustrator_street,
                    illustrator_city = EXCLUDED.illustrator_city,
                    illustrator_state = EXCLUDED.illustrator_state,
                    illustrator_zip = EXCLUDED.illustrator_zip,
                    illustrator_country = EXCLUDED.illustrator_country,
                    illustrator_non_us = EXCLUDED.illustrator_non_us,
                    illustrator_birth_date = EXCLUDED.illustrator_birth_date,
                    illustrator_birth_city = EXCLUDED.illustrator_birth_city,
                    illustrator_birth_country = EXCLUDED.illustrator_birth_country,
                    illustrator_citizenship = EXCLUDED.illustrator_citizenship,
                    illustrator_advance = EXCLUDED.illustrator_advance,
                    effective_date = EXCLUDED.effective_date,
                    series = EXCLUDED.series,
                    series_title = EXCLUDED.series_title,
                    number_of_books = EXCLUDED.number_of_books,
                    short_description = EXCLUDED.short_description,
                    projected_publication_date = EXCLUDED.projected_publication_date,
                    projected_retail_price = EXCLUDED.projected_retail_price,
                    territories_rights = EXCLUDED.territories_rights,
                    option_deleted = EXCLUDED.option_deleted,
                    option_clause = EXCLUDED.option_clause,
                    option_supplement = EXCLUDED.option_supplement,
                    comp_copies_contributor = EXCLUDED.comp_copies_contributor,
                    comp_copies_agent = EXCLUDED.comp_copies_agent,
                    delivery_mode = EXCLUDED.delivery_mode,
                    delivery_clause = EXCLUDED.delivery_clause,
                    delivery_date = EXCLUDED.delivery_date,
                    generated_contract_filename = EXCLUDED.generated_contract_filename,
                    generated_contract_s3_key = EXCLUDED.generated_contract_s3_key,
                    generated_at = EXCLUDED.generated_at,
                    updated_at = now()
                RETURNING id
                """,
                (
                    tenant_id,
                    uid,
                    name,
                    title,
                    _s(body.get("status") or "draft"),
                    contributor_role,
                    _s(body.get("selected_template_id") or body.get("selectedTemplateId")),

                    _s(body.get("work_id")) or None,
                    _s(body.get("contributor_party_id")) or None,
                    _s(body.get("agency_party_id")) or None,
                    _s(body.get("agent_party_id")) or None,

                    _person_name(author_obj) or _s(body.get("author")),
                    _person_email(author_obj, body.get("author_email")),
                    _person_website(author_obj, body.get("author_website")),
                    _person_phone_cc(author_obj, body.get("author_phone_country_code")),
                    _person_phone_number(author_obj, body.get("author_phone_number")),
                    _s(author_address.get("street") or body.get("author_street")),
                    _s(author_address.get("city") or body.get("author_city")),
                    _s(author_address.get("state") or body.get("author_state")),
                    _s(author_address.get("zip") or body.get("author_zip")),
                    _s(author_address.get("country") or body.get("author_country")),
                    _bool(author_address.get("nonUS") if "nonUS" in author_address else body.get("author_non_us")),
                    _date_or_none(body.get("author_birth_date")),
                    _s(body.get("author_birth_city")),
                    _s(body.get("author_birth_country")),
                    _s(body.get("author_citizenship")),
                    _float_or_none(body.get("author_advance")),

                    _person_name(illustrator_obj) or _s(body.get("illustrator_name")),
                    _person_email(illustrator_obj, body.get("illustrator_email")),
                    _person_website(illustrator_obj, body.get("illustrator_website")),
                    _person_phone_cc(illustrator_obj, body.get("illustrator_phone_country_code")),
                    _person_phone_number(illustrator_obj, body.get("illustrator_phone_number")),
                    _s(illustrator_address.get("street") or body.get("illustrator_street")),
                    _s(illustrator_address.get("city") or body.get("illustrator_city")),
                    _s(illustrator_address.get("state") or body.get("illustrator_state")),
                    _s(illustrator_address.get("zip") or body.get("illustrator_zip")),
                    _s(illustrator_address.get("country") or body.get("illustrator_country")),
                    _bool(illustrator_address.get("nonUS") if "nonUS" in illustrator_address else body.get("illustrator_non_us")),
                    _date_or_none(body.get("illustrator_birth_date")),
                    _s(body.get("illustrator_birth_city")),
                    _s(body.get("illustrator_birth_country")),
                    _s(body.get("illustrator_citizenship")),
                    _float_or_none(body.get("illustrator_advance")),

                    _date_or_none(body.get("effectiveDate") or body.get("effective_date")),
                    _bool(body.get("series")),
                    _s(body.get("series_title")),
                    _int_or_none(body.get("number_of_books")) or 0,

                    _s(body.get("shortDescription") or body.get("short_description")),
                    _s(body.get("projectedPublicationDate") or body.get("projected_publication_date")),
                    _s(body.get("projectedRetailPrice") or body.get("projected_retail_price")),
                    _s(body.get("territoriesRights") or body.get("territories_rights")),
                    _bool(body.get("optionDeleted") if "optionDeleted" in body else body.get("option_deleted")),
                    _s(body.get("optionClause") or body.get("option_clause")),
                    _s(body.get("optionSupplement") or body.get("option_supplement")),
                    _int_or_none(body.get("compCopiesContributor") if "compCopiesContributor" in body else body.get("comp_copies_contributor")),
                    _int_or_none(body.get("compCopiesAgent") if "compCopiesAgent" in body else body.get("comp_copies_agent")),
                    _s(body.get("deliveryMode") or body.get("delivery_mode")),
                    _s(body.get("deliveryClause") or body.get("delivery_clause")),
                    _date_or_none(body.get("deliveryDate") or body.get("delivery_date")),
                    _s(body.get("generated_contract_filename")),
                    _s(body.get("generated_contract_s3_key")),
                    body.get("generated_at"),
                ),
            )

            draft_id = str(cur.fetchone()["id"])
            _clear_children(cur, draft_id)
            _save_advance_schedule(cur, tenant_id, draft_id, body)
            _save_royalties(cur, tenant_id, draft_id, body)
            _save_deal_memo_contributors(cur, tenant_id, draft_id, body)

            row = _fetch_one_draft(cur, tenant_id, uid)
            if not row:
                raise HTTPException(status_code=500, detail="Upsert failed")

            return {"ok": True, "draft": _row_to_draft(cur, tenant_id, row)}


@router.put("/dealmemos/{uid}")
def update_deal_memo(
    uid: str,
    body: Dict[str, Any] = Body(...),
    tenant_slug: str = Query(DEFAULT_TENANT_SLUG),
) -> dict:
    uid = _s(uid)
    if not uid:
        raise HTTPException(status_code=400, detail="uid required")
    body = dict(body or {})
    body["uid"] = uid
    if not _s(body.get("name")):
        body["name"] = _s(body.get("title")) or "Untitled Deal Memo"
    return upsert_deal_memo(body, tenant_slug=tenant_slug)


@router.delete("/dealmemos/{uid}")
def delete_draft(uid: str, tenant_slug: str = Query(DEFAULT_TENANT_SLUG)) -> dict:
    uid = _s(uid)
    if not uid:
        raise HTTPException(status_code=400, detail="uid required")

    with db_conn() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            _require_tables(cur)
            tenant_id = _tenant_id(cur, tenant_slug)

            row = _fetch_one_draft(cur, tenant_id, uid)
            if not row:
                raise HTTPException(status_code=404, detail="Draft not found")

            draft_id = str(row["id"])
            _clear_children(cur, draft_id)
            cur.execute(
                f"DELETE FROM {DEAL_MEMO_TABLE} WHERE tenant_id = %s AND uid = %s",
                (tenant_id, uid),
            )
    return {"ok": True, "deleted": uid}


@router.get("/dealmemo-drafts")
def list_deal_memo_drafts_alias(tenant_slug: str = Query(DEFAULT_TENANT_SLUG)) -> List[dict]:
    return list_deal_memos(tenant_slug=tenant_slug)


@router.get("/dealmemo-drafts/{uid}")
def get_deal_memo_draft_alias(uid: str, tenant_slug: str = Query(DEFAULT_TENANT_SLUG)) -> dict:
    return get_deal_memo(uid=uid, tenant_slug=tenant_slug)


@router.post("/dealmemo-drafts")
def upsert_deal_memo_draft_alias(
    body: Dict[str, Any] = Body(...),
    tenant_slug: str = Query(DEFAULT_TENANT_SLUG),
) -> dict:
    return upsert_deal_memo(body=body, tenant_slug=tenant_slug)


@router.delete("/dealmemo-drafts/{uid}")
def delete_deal_memo_draft_alias(uid: str, tenant_slug: str = Query(DEFAULT_TENANT_SLUG)) -> dict:
    return delete_draft(uid=uid, tenant_slug=tenant_slug)


@router.get("/agencies/search")
def search_agencies(q: str = Query("", min_length=0), tenant_slug: str = Query(DEFAULT_TENANT_SLUG)) -> dict:
    q = _s(q)
    with db_conn() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            _require_tables(cur)
            tenant_id = _tenant_id(cur, tenant_slug)

            if not q:
                return {"items": []}

            cur.execute(
                """
                SELECT
                    p.id AS agency_party_id,
                    p.display_name AS agency_name,
                    p.email AS agency_email,
                    p.website AS agency_website,
                    pa.street AS agency_street,
                    pa.city AS agency_city,
                    pa.state AS agency_state,
                    pa.zip AS agency_zip,
                    pa.country AS agency_country,
                    ap.agency_clause
                FROM parties p
                LEFT JOIN party_addresses pa
                  ON pa.party_id = p.id
                 AND pa.label = 'primary'
                LEFT JOIN agency_profiles ap
                  ON ap.agency_party_id = p.id
                WHERE p.tenant_id = %s
                  AND p.party_type = 'org'
                  AND (
                    lower(p.display_name) LIKE lower(%s)
                    OR lower(coalesce(p.website, '')) LIKE lower(%s)
                    OR lower(coalesce(p.email, '')) LIKE lower(%s)
                  )
                ORDER BY p.display_name ASC
                LIMIT 15
                """,
                (tenant_id, f"%{q}%", f"%{q}%", f"%{q}%"),
            )
            agencies = [dict(r) for r in (cur.fetchall() or [])]

            for agency in agencies:
                agency["agents"] = _get_agency_agents(cur, tenant_id, str(agency["agency_party_id"]))

            return {"items": agencies}


@router.get("/agencies/{agency_id}")
def get_agency(agency_id: str, tenant_slug: str = Query(DEFAULT_TENANT_SLUG)) -> dict:
    agency_id = _s(agency_id)
    if not agency_id:
        raise HTTPException(status_code=400, detail="agency_id required")

    with db_conn() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            _require_tables(cur)
            tenant_id = _tenant_id(cur, tenant_slug)
            agency = _get_agency_detail(cur, tenant_id, agency_id)
            return {"agency": agency, "agents": agency.get("agents", [])}

@router.delete("/agencies/{agency_id}/agents/{agent_id}")
def delete_agency_agent(
    agency_id: str,
    agent_id: str,
    tenant_slug: str = Query(DEFAULT_TENANT_SLUG),
) -> dict:
    agency_id = _s(agency_id)
    agent_id = _s(agent_id)

    if not agency_id or not agent_id:
        raise HTTPException(status_code=400, detail="agency_id and agent_id are required")

    with db_conn() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            _require_tables(cur)
            tenant_id = _tenant_id(cur, tenant_slug)

            # remove agency link
            cur.execute(
                """
                DELETE FROM agency_agent_links
                WHERE tenant_id = %s
                  AND agency_party_id = %s
                  AND agent_party_id = %s
                """,
                (tenant_id, agency_id, agent_id),
            )

            # remove rep rows if that matches your business rule
            cur.execute(
                """
                DELETE FROM party_representations
                WHERE tenant_id = %s
                  AND agent_party_id = %s
                """,
                (tenant_id, agent_id),
            )

            # see whether person is still referenced anywhere
            cur.execute(
                """
                SELECT
                  EXISTS (
                    SELECT 1
                    FROM agency_agent_links
                    WHERE tenant_id = %s
                      AND agent_party_id = %s
                  ) AS still_in_agency_links,
                  EXISTS (
                    SELECT 1
                    FROM party_representations
                    WHERE tenant_id = %s
                      AND agent_party_id = %s
                  ) AS still_in_representations,
                  EXISTS (
                    SELECT 1
                    FROM work_contributors
                    WHERE party_id = %s
                  ) AS still_in_work_contributors
                """,
                (tenant_id, agent_id, tenant_id, agent_id, agent_id),
            )
            refs = cur.fetchone() or {}

            if (
                not refs.get("still_in_agency_links")
                and not refs.get("still_in_representations")
                and not refs.get("still_in_work_contributors")
            ):
                cur.execute(
                    """
                    DELETE FROM parties
                    WHERE tenant_id = %s
                      AND id = %s
                      AND party_type = 'person'
                    """,
                    (tenant_id, agent_id),
                )

            agency = _get_agency_detail(cur, tenant_id, agency_id)
            return {"ok": True, "agency": agency, "agents": agency.get("agents", [])}
        
@router.post("/agencies/upsert")
def upsert_agency(
    body: Dict[str, Any] = Body(...),
    tenant_slug: str = Query(DEFAULT_TENANT_SLUG),
) -> dict:
    if not isinstance(body, dict):
        raise HTTPException(status_code=400, detail="Invalid JSON body")

    with db_conn() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            _require_tables(cur)
            tenant_id = _tenant_id(cur, tenant_slug)
            return _upsert_agency(cur, tenant_id, body)