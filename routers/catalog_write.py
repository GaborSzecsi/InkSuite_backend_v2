from __future__ import annotations

import re
import uuid
import json
from datetime import date, datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from .catalog_shared import (
    _safe_str,
    _safe_name,
    _parse_date_or_none,
    _to_int_or_none,
    _to_float_or_none,
    _normalize_isbn13,
    _contributor_input,
    _has_real_contributor,
    _agency_payload_from_input,
    _first_non_empty_dict_list,
)
from .catalog_royalties import (
    _get_royalty_set_for_write,
    _clear_royalty_graph_for_set,
    _insert_royalty_rule,
)


def _resolve_work_id(cur, tenant_id: str, body: Dict[str, Any]) -> Optional[str]:
    body_id = body.get("id")
    if body_id is not None and str(body_id).strip():
        cur.execute(
            "SELECT id FROM works WHERE tenant_id = %s AND id = %s LIMIT 1",
            (tenant_id, str(body_id).strip()),
        )
        row = cur.fetchone()
        if row:
            return str(row["id"])

    uid_raw = body.get("uid")
    if uid_raw is not None and str(uid_raw).strip():
        try:
            u = uuid.UUID(str(uid_raw).strip())
        except (ValueError, TypeError):
            pass
        else:
            cur.execute(
                "SELECT id FROM works WHERE tenant_id = %s AND uid = %s LIMIT 1",
                (tenant_id, u),
            )
            row = cur.fetchone()
            if row:
                return str(row["id"])
    return None


def _resolve_work_id_by_title_author(
    cur, tenant_id: str, title: str, author: str
) -> Optional[str]:
    title = _safe_str(title)
    author = _safe_str(author)
    if not title and not author:
        return None

    cur.execute(
        """
        SELECT w.id
        FROM works w
        WHERE w.tenant_id = %s
          AND trim(lower(w.title)) = trim(lower(%s))
          AND EXISTS (
              SELECT 1
              FROM work_contributors wc
              JOIN parties p
                ON p.id = wc.party_id
               AND p.tenant_id = w.tenant_id
              WHERE wc.work_id = w.id
                AND trim(lower(coalesce(p.display_name, ''))) = trim(lower(%s))
          )
        LIMIT 1
        """,
        (tenant_id, title, author),
    )
    row = cur.fetchone()
    return str(row["id"]) if row else None


def _upsert_party_minimal(
    cur,
    tenant_id: str,
    display_name: str,
    party_type: str = "person",
    email: str = "",
) -> Optional[str]:
    name = _safe_name(display_name)
    email = _safe_str(email).lower()

    if not name and not email:
        return None

    if email:
        cur.execute(
            """
            SELECT id
            FROM parties
            WHERE tenant_id = %s
              AND lower(coalesce(email, '')) = %s
            LIMIT 1
            """,
            (tenant_id, email),
        )
        row = cur.fetchone()
        if row:
            cur.execute(
                """
                UPDATE parties
                SET
                    display_name = CASE
                        WHEN coalesce(trim(display_name), '') = '' THEN %s
                        ELSE display_name
                    END,
                    updated_at = now()
                WHERE tenant_id = %s
                  AND id = %s
                """,
                (name, tenant_id, row["id"]),
            )
            return str(row["id"])

    if name:
        cur.execute(
            """
            SELECT id
            FROM parties
            WHERE tenant_id = %s
              AND display_name = %s
            LIMIT 1
            """,
            (tenant_id, name),
        )
        row = cur.fetchone()
        if row:
            return str(row["id"])

    pid = str(uuid.uuid4())
    cur.execute(
        """
        INSERT INTO parties (
            id, tenant_id, party_type, display_name,
            names_before_key, key_names, person_name_inverted, corporate_name, email
        )
        VALUES (%s, %s, %s, %s, '', '', '', '', %s)
        """,
        (pid, tenant_id, party_type, name or email or "Unknown", email),
    )
    return pid


def _get_or_create_party(
    cur,
    tenant_id: str,
    display_name: str,
    email: str = "",
    party_type: str = "person",
) -> str:
    party_id = _upsert_party_minimal(cur, tenant_id, display_name, party_type, email)
    return str(party_id or "")

def _replace_simple_contributor_rows(
    cur,
    tenant_id: str,
    party_id: str,
    table_name: str,
    rows: List[Dict[str, Any]],
    columns: List[Tuple[str, Tuple[str, ...]]],
) -> None:
    """Replace one contributor repeat table from a normalized payload."""
    cur.execute(
        f"DELETE FROM {table_name} WHERE tenant_id = %s AND party_id = %s",
        (tenant_id, party_id),
    )

    for idx, raw in enumerate(rows or [], start=1):
        if not isinstance(raw, dict):
            continue

        values: List[Any] = []
        insert_columns = ["tenant_id", "party_id"]

        for column_name, aliases in columns:
            value = ""
            for alias in aliases:
                if alias in raw and raw.get(alias) is not None:
                    value = _safe_str(raw.get(alias))
                    break
            insert_columns.append(column_name)
            values.append(value)

        if not any(values):
            continue

        insert_columns.append("item_order")
        placeholders = ", ".join(["%s"] * len(insert_columns))
        cur.execute(
            f"INSERT INTO {table_name} ({', '.join(insert_columns)}) VALUES ({placeholders})",
            tuple([tenant_id, party_id] + values + [idx]),
        )


def _replace_contributor_dates(
    cur,
    tenant_id: str,
    party_id: str,
    rows: List[Dict[str, Any]],
) -> None:
    cur.execute(
        "DELETE FROM party_contributor_dates WHERE tenant_id = %s AND party_id = %s",
        (tenant_id, party_id),
    )

    for idx, raw in enumerate(rows or [], start=1):
        if not isinstance(raw, dict):
            continue
        role = _safe_str(
            raw.get("contributor_date_role")
            or raw.get("contributorDateRole")
            or raw.get("date_role")
            or raw.get("dateRole")
        )
        date_value = _parse_date_or_none(
            raw.get("date_value")
            or raw.get("dateValue")
            or raw.get("date")
        )
        if not role or not date_value:
            continue
        cur.execute(
            """
            INSERT INTO party_contributor_dates (
                tenant_id, party_id, contributor_date_role, date_value, item_order
            )
            VALUES (%s, %s, %s, %s, %s)
            """,
            (tenant_id, party_id, role, date_value, idx),
        )


def add_work_contributor(
    cur,
    tenant_id: str,
    work_id: str,
    payload: Dict[str, Any],
) -> Dict[str, Any]:
    """Create or update one work contributor and its reusable ONIX party profile."""

    work_id = _safe_str(work_id)
    if not work_id:
        raise ValueError("work_id is required")

    cur.execute(
        """
        SELECT id, title
        FROM works
        WHERE tenant_id = %s
          AND id = %s
        LIMIT 1
        """,
        (tenant_id, work_id),
    )
    work_row = cur.fetchone()
    if not work_row:
        raise ValueError("Existing work not found")

    contributors = payload.get("contributors")
    if isinstance(contributors, list) and contributors and isinstance(contributors[0], dict):
        contributor = dict(contributors[0])
    elif isinstance(payload.get("contributor"), dict):
        contributor = dict(payload["contributor"])
    else:
        contributor = dict(payload)

    selected_party_id = _safe_str(contributor.get("party_id") or payload.get("party_id"))
    work_contributor_id = _safe_str(
        contributor.get("work_contributor_id")
        or contributor.get("workContributorId")
        or payload.get("work_contributor_id")
    )

    name = _safe_name(contributor.get("name") or contributor.get("display_name"))
    email = _safe_str(contributor.get("email"))
    phone_country_code = _safe_str(contributor.get("phone_country_code"))
    phone_number = _safe_str(contributor.get("phone_number") or contributor.get("phone"))
    party_type = _safe_str(
        contributor.get("party_type")
        or contributor.get("contributor_type")
        or contributor.get("contributorType")
    ) or "person"

    role_code = _safe_str(
        payload.get("contributor_role_code")
        or contributor.get("contributor_role_code")
        or payload.get("role_code")
        or contributor.get("role_code")
    ).upper()
    role_label = _safe_str(
        payload.get("contributor_role_label")
        or contributor.get("contributor_role_label")
        or payload.get("role_label")
        or contributor.get("role_label")
    )

    if not role_code:
        raise ValueError("Contributor role code is required")

    party_id = ""
    if selected_party_id:
        cur.execute(
            """
            SELECT id, display_name, email, phone_country_code, phone_number
            FROM parties
            WHERE tenant_id = %s AND id = %s
            LIMIT 1
            """,
            (tenant_id, selected_party_id),
        )
        selected_party = cur.fetchone()
        if not selected_party:
            raise ValueError("Selected contributor was not found")
        party_id = str(selected_party["id"])
        name = name or _safe_name(selected_party.get("display_name"))
        email = email or _safe_str(selected_party.get("email"))
        phone_country_code = phone_country_code or _safe_str(selected_party.get("phone_country_code"))
        phone_number = phone_number or _safe_str(selected_party.get("phone_number"))
    else:
        if not name:
            raise ValueError("Contributor name is required")
        party_id = _get_or_create_party(
            cur, tenant_id, name, email=email, party_type=party_type
        )

    if not party_id:
        raise ValueError("Could not create or resolve contributor party")

    # Persist every field exposed by the contributor editor. The previous POST path
    # only updated display name, email, website and phone, so most editor fields were lost.
    birth_date = None
    death_date = None
    for raw_date in (contributor.get("contributor_dates") or contributor.get("contributorDates") or []):
        if not isinstance(raw_date, dict):
            continue
        date_role = _safe_str(
            raw_date.get("contributor_date_role")
            or raw_date.get("contributorDateRole")
            or raw_date.get("date_role")
            or raw_date.get("dateRole")
        )
        parsed = _parse_date_or_none(
            raw_date.get("date_value") or raw_date.get("dateValue") or raw_date.get("date")
        )
        if date_role == "50":
            birth_date = parsed
        elif date_role == "51":
            death_date = parsed

    website = _safe_str(contributor.get("website"))
    short_bio = _safe_str(
        contributor.get("short_bio")
        or contributor.get("shortBio")
        or contributor.get("biographical_note")
        or contributor.get("biographicalNote")
    )
    long_bio = _safe_str(
        contributor.get("long_bio")
        or contributor.get("longBio")
    )

    # Metadata Contributor saves may contain only the ONIX fields visible on that
    # card. Preserve existing party values when an omitted field arrives blank.
    cur.execute(
        """
        UPDATE parties
        SET
            party_type = CASE WHEN %s <> '' THEN %s ELSE party_type END,
            display_name = CASE WHEN %s <> '' THEN %s ELSE display_name END,
            email = CASE WHEN %s <> '' THEN %s ELSE email END,
            website = CASE WHEN %s <> '' THEN %s ELSE website END,
            phone_country_code = CASE WHEN %s <> '' THEN %s ELSE phone_country_code END,
            phone_number = CASE WHEN %s <> '' THEN %s ELSE phone_number END,
            titles_before_names = CASE WHEN %s <> '' THEN %s ELSE titles_before_names END,
            names_before_key = CASE WHEN %s <> '' THEN %s ELSE names_before_key END,
            prefix_to_key = CASE WHEN %s <> '' THEN %s ELSE prefix_to_key END,
            key_names = CASE WHEN %s <> '' THEN %s ELSE key_names END,
            suffix_to_key = CASE WHEN %s <> '' THEN %s ELSE suffix_to_key END,
            letters_after_names = CASE WHEN %s <> '' THEN %s ELSE letters_after_names END,
            person_name_inverted = CASE WHEN %s <> '' THEN %s ELSE person_name_inverted END,
            corporate_name = CASE WHEN %s <> '' THEN %s ELSE corporate_name END,
            language_code = CASE WHEN %s <> '' THEN %s ELSE language_code END,
            short_bio = CASE WHEN %s <> '' THEN %s ELSE short_bio END,
            long_bio = CASE WHEN %s <> '' THEN %s ELSE long_bio END,
            notes = CASE WHEN %s <> '' THEN %s ELSE notes END,
            birth_date = COALESCE(%s, birth_date),
            death_date = COALESCE(%s, death_date),
            updated_at = now()
        WHERE tenant_id = %s AND id = %s
        """,
        (
            party_type, party_type,
            name, name,
            email, email,
            website, website,
            phone_country_code, phone_country_code,
            phone_number, phone_number,
            _safe_str(contributor.get("titles_before_names")), _safe_str(contributor.get("titles_before_names")),
            _safe_str(contributor.get("names_before_key")), _safe_str(contributor.get("names_before_key")),
            _safe_str(contributor.get("prefix_to_key")), _safe_str(contributor.get("prefix_to_key")),
            _safe_str(contributor.get("key_names")), _safe_str(contributor.get("key_names")),
            _safe_str(contributor.get("suffix_to_key")), _safe_str(contributor.get("suffix_to_key")),
            _safe_str(contributor.get("letters_after_names")), _safe_str(contributor.get("letters_after_names")),
            _safe_str(contributor.get("person_name_inverted")), _safe_str(contributor.get("person_name_inverted")),
            _safe_str(contributor.get("corporate_name")), _safe_str(contributor.get("corporate_name")),
            _safe_str(contributor.get("language_code") or contributor.get("internal_preferred_language")),
            _safe_str(contributor.get("language_code") or contributor.get("internal_preferred_language")),
            short_bio, short_bio,
            long_bio, long_bio,
            _safe_str(contributor.get("notes")), _safe_str(contributor.get("notes")),
            birth_date,
            death_date,
            tenant_id,
            party_id,
        ),
    )

    # Keep the questionnaire's overlapping bio fields synchronized for author /
    # illustrator profiles. parties.short_bio / long_bio remain canonical.
    profile_scope = (
        "author"
        if role_code in {"A01", "AUTHOR"}
        else "illustrator"
        if role_code in {"A12", "ILLUSTRATOR"}
        else ""
    )
    if profile_scope and (short_bio or long_bio):
        cur.execute(
            """
            INSERT INTO contributor_marketing_profiles (
                tenant_id, party_id, scope, book_bio, website_bio
            )
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (tenant_id, party_id, scope)
            DO UPDATE SET
                book_bio = CASE
                    WHEN EXCLUDED.book_bio <> '' THEN EXCLUDED.book_bio
                    ELSE contributor_marketing_profiles.book_bio
                END,
                website_bio = CASE
                    WHEN EXCLUDED.website_bio <> '' THEN EXCLUDED.website_bio
                    ELSE contributor_marketing_profiles.website_bio
                END,
                updated_at = now()
            """,
            (tenant_id, party_id, profile_scope, short_bio, long_bio),
        )

    address = contributor.get("address") if isinstance(contributor.get("address"), dict) else {}
    if any(_safe_str(address.get(k)) for k in ("street", "city", "state", "zip", "postal_code", "country")):
        cur.execute(
            "DELETE FROM party_addresses WHERE tenant_id = %s AND party_id = %s",
            (tenant_id, party_id),
        )
        _insert_party_address(cur, tenant_id, party_id, address)

    identifiers = contributor.get("name_identifiers") or contributor.get("nameIdentifiers") or contributor.get("identifiers") or []
    _replace_simple_contributor_rows(
        cur,
        tenant_id,
        party_id,
        "party_name_identifiers",
        identifiers if isinstance(identifiers, list) else [],
        [
            ("name_id_type", ("name_id_type", "nameIdType", "type")),
            ("id_type_name", ("id_type_name", "idTypeName")),
            ("id_value", ("id_value", "idValue", "value")),
        ],
    )

    alternative_names = contributor.get("alternative_names") or contributor.get("alternativeNames") or []
    _replace_simple_contributor_rows(
        cur,
        tenant_id,
        party_id,
        "party_alternative_names",
        alternative_names if isinstance(alternative_names, list) else [],
        [
            ("name_type", ("name_type", "nameType")),
            ("display_name", ("display_name", "displayName", "person_name", "personName")),
            ("person_name_inverted", ("person_name_inverted", "personNameInverted")),
            ("names_before_key", ("names_before_key", "namesBeforeKey")),
            ("key_names", ("key_names", "keyNames")),
            ("corporate_name", ("corporate_name", "corporateName")),
        ],
    )

    websites = contributor.get("websites") or contributor.get("contributor_websites") or []
    _replace_simple_contributor_rows(
        cur,
        tenant_id,
        party_id,
        "party_websites",
        websites if isinstance(websites, list) else [],
        [
            ("website_role", ("website_role", "websiteRole", "role_code", "roleCode")),
            ("website_description", ("website_description", "websiteDescription", "description")),
            ("website_link", ("website_link", "websiteLink", "url")),
        ],
    )

    # Keep legacy parties.website synchronized with the first contributor website.
    primary_website = ""
    for website_row in websites if isinstance(websites, list) else []:
        if isinstance(website_row, dict):
            url = _safe_str(website_row.get("website_link") or website_row.get("websiteLink") or website_row.get("url"))
            role = _safe_str(website_row.get("website_role") or website_row.get("websiteRole"))
            if url and (not primary_website or role == "06"):
                primary_website = url
                if role == "06":
                    break
    cur.execute(
        "UPDATE parties SET website = %s, updated_at = now() WHERE tenant_id = %s AND id = %s",
        (primary_website, tenant_id, party_id),
    )

    places = contributor.get("contributor_places") or contributor.get("contributorPlaces") or []
    _replace_simple_contributor_rows(
        cur,
        tenant_id,
        party_id,
        "party_contributor_places",
        places if isinstance(places, list) else [],
        [
            ("contributor_place_relator", ("contributor_place_relator", "contributorPlaceRelator", "relator")),
            ("country_code", ("country_code", "countryCode")),
            ("region_code", ("region_code", "regionCode")),
            ("location_name", ("location_name", "locationName")),
        ],
    )

    contributor_dates = contributor.get("contributor_dates") or contributor.get("contributorDates") or []
    _replace_contributor_dates(
        cur,
        tenant_id,
        party_id,
        contributor_dates if isinstance(contributor_dates, list) else [],
    )

    affiliations = contributor.get("professional_affiliations") or contributor.get("professionalAffiliations") or []
    _replace_simple_contributor_rows(
        cur,
        tenant_id,
        party_id,
        "party_professional_affiliations",
        affiliations if isinstance(affiliations, list) else [],
        [
            ("professional_position", ("professional_position", "professionalPosition")),
            ("affiliation", ("affiliation",)),
            ("affiliation_id_type", ("affiliation_id_type", "affiliationIdType")),
            ("affiliation_id_type_name", ("affiliation_id_type_name", "affiliationIdTypeName")),
            ("affiliation_id_value", ("affiliation_id_value", "affiliationIdValue")),
        ],
    )

    from_languages = [
        _safe_str(value)
        for value in (contributor.get("from_languages") or contributor.get("fromLanguages") or [])
        if _safe_str(value)
    ]
    to_languages = [
        _safe_str(value)
        for value in (contributor.get("to_languages") or contributor.get("toLanguages") or [])
        if _safe_str(value)
    ]
    contributor_description = _safe_str(
        contributor.get("contributor_description") or contributor.get("contributorDescription")
    )

    requested_sequence = 0
    try:
        requested_sequence = int(contributor.get("sequence") or contributor.get("sequence_number") or 0)
    except (TypeError, ValueError):
        requested_sequence = 0

    if work_contributor_id:
        cur.execute(
            """
            SELECT id, party_id
            FROM work_contributors
            WHERE tenant_id = %s AND work_id = %s AND id = %s
            LIMIT 1
            """,
            (tenant_id, work_id, work_contributor_id),
        )
        assignment = cur.fetchone()
        if not assignment:
            raise ValueError("Contributor assignment not found")

        cur.execute(
            """
            UPDATE work_contributors
            SET party_id = %s,
                contributor_role = %s,
                sequence_number = %s,
                from_language_codes = %s,
                to_language_codes = %s,
                contributor_description = %s
            WHERE tenant_id = %s AND work_id = %s AND id = %s
            """,
            (
                party_id,
                role_code,
                requested_sequence or 1,
                from_languages,
                to_languages,
                contributor_description,
                tenant_id,
                work_id,
                work_contributor_id,
            ),
        )
        assignment_id = work_contributor_id
        sequence_number = requested_sequence or 1
    else:
        cur.execute(
            """
            SELECT id, sequence_number
            FROM work_contributors
            WHERE tenant_id = %s
              AND work_id = %s
              AND party_id = %s
              AND upper(trim(coalesce(contributor_role, ''))) = %s
            LIMIT 1
            """,
            (tenant_id, work_id, party_id, role_code),
        )
        existing_assignment = cur.fetchone()

        if existing_assignment:
            assignment_id = str(existing_assignment["id"])
            sequence_number = int(existing_assignment.get("sequence_number") or requested_sequence or 1)
            cur.execute(
                """
                UPDATE work_contributors
                SET sequence_number = %s,
                    from_language_codes = %s,
                    to_language_codes = %s,
                    contributor_description = %s
                WHERE tenant_id = %s AND id = %s
                """,
                (
                    requested_sequence or sequence_number,
                    from_languages,
                    to_languages,
                    contributor_description,
                    tenant_id,
                    assignment_id,
                ),
            )
            sequence_number = requested_sequence or sequence_number
        else:
            cur.execute(
                """
                SELECT coalesce(max(sequence_number), 0) + 1 AS next_sequence
                FROM work_contributors
                WHERE tenant_id = %s AND work_id = %s
                """,
                (tenant_id, work_id),
            )
            next_sequence = int((cur.fetchone() or {}).get("next_sequence") or 1)
            sequence_number = requested_sequence or next_sequence
            cur.execute(
                """
                INSERT INTO work_contributors (
                    tenant_id, work_id, party_id, contributor_role, sequence_number,
                    from_language_codes, to_language_codes, contributor_description
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id
                """,
                (
                    tenant_id,
                    work_id,
                    party_id,
                    role_code,
                    sequence_number,
                    from_languages,
                    to_languages,
                    contributor_description,
                ),
            )
            assignment_id = str((cur.fetchone() or {}).get("id") or "")

    return {
        "ok": True,
        "work_id": work_id,
        "work_title": _safe_str(work_row.get("title")),
        "contributor": {
            "work_contributor_id": assignment_id,
            "party_id": party_id,
            "display_name": name,
            "email": email,
            "contributor_role_code": role_code,
            "contributor_role_label": role_label or role_code,
            "contributor_role": role_code,
            "sequence_number": sequence_number,
            "from_languages": from_languages,
            "to_languages": to_languages,
            "contributor_description": contributor_description,
            "reused_existing_party": bool(selected_party_id),
        },
    }


def unlink_work_contributor(

    cur,
    tenant_id: str,
    work_id: str,
    party_id: str,
    role_code: str = "",
) -> Dict[str, Any]:
    """Remove one contributor assignment while preserving the party profile."""

    work_id = _safe_str(work_id)
    party_id = _safe_str(party_id)
    role_code = _safe_str(role_code).upper()

    if not work_id or not party_id:
        raise ValueError("work_id and party_id are required")

    cur.execute(
        """
        SELECT
            wc.party_id,
            upper(trim(coalesce(wc.contributor_role, ''))) AS contributor_role,
            p.display_name
        FROM work_contributors wc
        JOIN parties p
          ON p.tenant_id = wc.tenant_id
         AND p.id = wc.party_id
        WHERE wc.tenant_id = %s
          AND wc.work_id = %s
          AND wc.party_id = %s
          AND (
              %s = ''
              OR upper(trim(coalesce(wc.contributor_role, ''))) = %s
          )
        ORDER BY wc.sequence_number NULLS LAST
        LIMIT 1
        """,
        (tenant_id, work_id, party_id, role_code, role_code),
    )
    assignment = cur.fetchone()

    if not assignment:
        raise ValueError("Contributor assignment not found")

    assignment_role = _safe_str(assignment.get("contributor_role")).upper()

    if assignment_role in ("AUTHOR", "A01"):
        cur.execute(
            """
            SELECT count(*) AS author_count
            FROM work_contributors
            WHERE tenant_id = %s
              AND work_id = %s
              AND upper(trim(coalesce(contributor_role, '')))
                    IN ('AUTHOR', 'A01', 'A02')
            """,
            (tenant_id, work_id),
        )
        count_row = cur.fetchone() or {}
        author_count = int(count_row.get("author_count") or 0)

        if author_count <= 1:
            raise ValueError(
                "The sole author cannot be unlinked. Delete the title instead."
            )

    cur.execute(
        """
        DELETE FROM work_contributors
        WHERE tenant_id = %s
          AND work_id = %s
          AND party_id = %s
          AND (
              %s = ''
              OR upper(trim(coalesce(contributor_role, ''))) = %s
          )
        """,
        (tenant_id, work_id, party_id, role_code, role_code),
    )

    if cur.rowcount <= 0:
        raise ValueError("Contributor assignment was not removed")

    return {
        "ok": True,
        "work_id": work_id,
        "party_id": party_id,
        "display_name": _safe_str(assignment.get("display_name")),
        "role_code": assignment_role,
        "unlinked": True,
        "party_preserved": True,
    }


def _table_has_columns(cur, table_name: str, columns: List[str]) -> bool:
    cur.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = %s
          AND column_name = ANY(%s)
        """,
        (table_name, columns),
    )
    found = {str(row["column_name"]) for row in (cur.fetchall() or [])}
    return all(column in found for column in columns)


def _count_reference(
    cur,
    tenant_id: str,
    table_name: str,
    party_column: str,
    party_id: str,
) -> int:
    if not _table_has_columns(cur, table_name, ["tenant_id", party_column]):
        return 0

    cur.execute(
        f"""
        SELECT count(*) AS row_count
        FROM {table_name}
        WHERE tenant_id = %s
          AND {party_column} = %s
        """,
        (tenant_id, party_id),
    )
    row = cur.fetchone() or {}
    return int(row.get("row_count") or 0)


def delete_contributor_party(
    cur,
    tenant_id: str,
    party_id: str,
) -> Dict[str, Any]:
    """
    Permanently delete a contributor profile.

    Title assignments are automatically removed first so the user does not
    need to unlink the contributor from every title manually.

    Deletion is still blocked while the party is referenced by agreements,
    contracts, agency relationships, representations, or other externally
    meaningful records.
    """

    party_id = _safe_str(party_id)
    if not party_id:
        raise ValueError("party_id is required")

    cur.execute(
        """
        SELECT id, display_name
        FROM parties
        WHERE tenant_id = %s
          AND id = %s
        LIMIT 1
        """,
        (tenant_id, party_id),
    )
    party = cur.fetchone()

    if not party:
        raise ValueError("Contributor not found")

    blocking_refs: List[Dict[str, Any]] = []

    # Work/title assignments are intentionally NOT blockers. They are deleted
    # automatically below. Other relationship types remain protected.
    reference_specs = [
        ("party_representations", "represented_party_id", "representations"),
        ("party_representations", "agent_party_id", "agent representations"),
        ("agency_agent_links", "agency_party_id", "agency relationships"),
        ("agency_agent_links", "agent_party_id", "agent relationships"),
        ("agreement_contributors", "party_id", "agreements"),
        ("contract_contributors", "party_id", "contracts"),
    ]

    for table_name, column_name, label in reference_specs:
        count = _count_reference(
            cur,
            tenant_id,
            table_name,
            column_name,
            party_id,
        )
        if count:
            blocking_refs.append(
                {
                    "table": table_name,
                    "label": label,
                    "count": count,
                }
            )

    if blocking_refs:
        summary = ", ".join(
            f"{item['count']} {item['label']}"
            for item in blocking_refs
        )
        raise ValueError(
            f"Contributor cannot be deleted because the profile is still linked to {summary}. "
            "Remove those protected relationships first."
        )

    # Count and remove every title assignment in one operation.
    cur.execute(
        """
        SELECT count(*) AS assignment_count
        FROM work_contributors
        WHERE tenant_id = %s
          AND party_id = %s
        """,
        (tenant_id, party_id),
    )
    assignment_row = cur.fetchone() or {}
    title_assignments_removed = int(
        assignment_row.get("assignment_count") or 0
    )

    cur.execute(
        """
        DELETE FROM work_contributors
        WHERE tenant_id = %s
          AND party_id = %s
        """,
        (tenant_id, party_id),
    )

    # Remove marketing/sales contacts and clean orphan contact_directory rows.
    if _table_has_columns(
        cur,
        "contributor_contact_links",
        ["tenant_id", "party_id", "contact_id"],
    ):
        cur.execute(
            """
            SELECT contact_id
            FROM contributor_contact_links
            WHERE tenant_id = %s
              AND party_id = %s
            """,
            (tenant_id, party_id),
        )
        contact_ids = [
            str(row["contact_id"])
            for row in (cur.fetchall() or [])
            if row.get("contact_id")
        ]

        cur.execute(
            """
            DELETE FROM contributor_contact_links
            WHERE tenant_id = %s
              AND party_id = %s
            """,
            (tenant_id, party_id),
        )

        if _table_has_columns(
            cur,
            "contact_directory",
            ["tenant_id", "id"],
        ):
            for contact_id in contact_ids:
                cur.execute(
                    """
                    DELETE FROM contact_directory cd
                    WHERE cd.tenant_id = %s
                      AND cd.id = %s
                      AND NOT EXISTS (
                          SELECT 1
                          FROM contributor_contact_links ccl
                          WHERE ccl.contact_id = cd.id
                      )
                      AND NOT EXISTS (
                          SELECT 1
                          FROM work_contact_links wcl
                          WHERE wcl.contact_id = cd.id
                      )
                    """,
                    (tenant_id, contact_id),
                )

    owned_tables = [
        "party_addresses",
        "party_socials",
        "party_identifiers",
        "party_websites",
        "party_dates",
        "party_places",
        "party_names",
        "party_awards",
        "party_honors",
        "contributor_awards",
        "contributor_marketing_profiles",
        "contributor_other_publications",
        "contributor_published_books",
        "contributor_media_appearances",
        "contributor_media_contacts",
        "contributor_previous_publicity",
        "contributor_niche_publicity_targets",
    ]

    for table_name in owned_tables:
        if not _table_has_columns(
            cur,
            table_name,
            ["tenant_id", "party_id"],
        ):
            continue

        cur.execute(
            f"""
            DELETE FROM {table_name}
            WHERE tenant_id = %s
              AND party_id = %s
            """,
            (tenant_id, party_id),
        )

    cur.execute(
        """
        DELETE FROM parties
        WHERE tenant_id = %s
          AND id = %s
        """,
        (tenant_id, party_id),
    )

    if cur.rowcount <= 0:
        raise ValueError("Contributor was not deleted")

    return {
        "ok": True,
        "party_id": party_id,
        "display_name": _safe_str(party.get("display_name")),
        "deleted": True,
        "title_assignments_removed": title_assignments_removed,
    }

def _insert_party_address(
    cur, tenant_id: str, party_id: str, address_obj: Dict[str, Any]
) -> None:
    if not any(
        _safe_str(address_obj.get(k))
        for k in ("street", "city", "state", "zip", "country")
    ):
        return

    try:
        cur.execute(
            """
            INSERT INTO party_addresses (
                tenant_id, party_id, street, city, state, postal_code, country, label
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, 'primary')
            """,
            (
                tenant_id,
                party_id,
                _safe_str(address_obj.get("street")),
                _safe_str(address_obj.get("city")),
                _safe_str(address_obj.get("state")),
                _safe_str(address_obj.get("zip")),
                _safe_str(address_obj.get("country")),
            ),
        )
    except Exception:
        cur.execute(
            """
            INSERT INTO party_addresses (
                tenant_id, party_id, street, city, state, zip, country, label
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, 'primary')
            """,
            (
                tenant_id,
                party_id,
                _safe_str(address_obj.get("street")),
                _safe_str(address_obj.get("city")),
                _safe_str(address_obj.get("state")),
                _safe_str(address_obj.get("zip")),
                _safe_str(address_obj.get("country")),
            ),
        )


def _upsert_party_core(
    cur, tenant_id: str, party_id: str, payload: Dict[str, Any], scope: str
) -> None:
    """Patch the shared party profile without erasing existing values with blanks.

    Normal book saves frequently submit empty strings for fields that were not loaded or
    edited. Empty strings therefore mean "preserve" here. Explicit clearing should be
    implemented later with a dedicated clear_fields contract.
    """
    info = _contributor_input(payload, scope)
    email = _safe_str(info.get("email"))
    website = _safe_str(info.get("website"))
    phone_country_code = _safe_str(info.get("phone_country_code"))
    phone_number = _safe_str(info.get("phone_number"))
    birth_city = _safe_str(payload.get(f"{scope}_birth_city"))
    birth_country = _safe_str(payload.get(f"{scope}_birth_country"))
    birth_date = _parse_date_or_none(payload.get(f"{scope}_birth_date"))
    citizenship = _safe_str(payload.get(f"{scope}_citizenship"))
    short_bio = _safe_str(payload.get(f"{scope}_book_bio") or payload.get(f"{scope}_bio"))
    long_bio = _safe_str(payload.get(f"{scope}_website_bio") or payload.get(f"{scope}_long_bio"))
    contributor_name = _safe_name(info.get("name"))

    cur.execute(
        """
        UPDATE parties
        SET
            display_name = CASE WHEN %s <> '' THEN %s ELSE display_name END,
            email = CASE WHEN %s <> '' THEN %s ELSE email END,
            website = CASE WHEN %s <> '' THEN %s ELSE website END,
            phone_country_code = CASE WHEN %s <> '' THEN %s ELSE phone_country_code END,
            phone_number = CASE WHEN %s <> '' THEN %s ELSE phone_number END,
            birth_city = CASE WHEN %s <> '' THEN %s ELSE birth_city END,
            birth_country = CASE WHEN %s <> '' THEN %s ELSE birth_country END,
            birth_date = COALESCE(%s, birth_date),
            citizenship = CASE WHEN %s <> '' THEN %s ELSE citizenship END,
            short_bio = CASE WHEN %s <> '' THEN %s ELSE short_bio END,
            long_bio = CASE WHEN %s <> '' THEN %s ELSE long_bio END,
            updated_at = now()
        WHERE tenant_id = %s AND id = %s
        """,
        (
            contributor_name, contributor_name,
            email, email,
            website, website,
            phone_country_code, phone_country_code,
            phone_number, phone_number,
            birth_city, birth_city,
            birth_country, birth_country,
            birth_date,
            citizenship, citizenship,
            short_bio, short_bio,
            long_bio, long_bio,
            tenant_id, party_id,
        ),
    )

    address_obj = info.get("address") if isinstance(info.get("address"), dict) else {}
    has_address = any(
        _safe_str(address_obj.get(k))
        for k in ("street", "city", "state", "zip", "postal_code", "country")
    )
    if has_address:
        cur.execute(
            "DELETE FROM party_addresses WHERE tenant_id = %s AND party_id = %s",
            (tenant_id, party_id),
        )
        _insert_party_address(cur, tenant_id, party_id, address_obj)

def _replace_party_socials(
    cur, tenant_id: str, party_id: str, payload: Dict[str, Any], scope: str
) -> None:
    socials = payload.get(f"{scope}_socials") or []
    cur.execute(
        "DELETE FROM party_socials WHERE tenant_id = %s AND party_id = %s",
        (tenant_id, party_id),
    )
    for s in socials:
        if not isinstance(s, dict):
            continue
        platform = _safe_str(s.get("platform"))
        url = _safe_str(s.get("url"))
        if not platform and not url:
            continue
        cur.execute(
            """
            INSERT INTO party_socials (tenant_id, party_id, platform, url)
            VALUES (%s, %s, %s, %s)
            """,
            (tenant_id, party_id, platform, url),
        )


def _payload_scoped_value(payload: Dict[str, Any], scope: str, *names: str) -> Tuple[bool, Any]:
    """Return whether a contributor-card value was supplied and its raw value.

    Accepts the nested ContributorCard shape (camelCase or snake_case) plus the
    legacy flat ``author_*`` / ``illustrator_*`` payload shape.
    """
    nested = payload.get(scope)
    if isinstance(nested, dict):
        for name in names:
            if name in nested:
                return True, nested.get(name)

    for name in names:
        flat_name = f"{scope}_{name}"
        if flat_name in payload:
            return True, payload.get(flat_name)

    return False, None


def _update_party_identity_extensions(
    cur, tenant_id: str, party_id: str, payload: Dict[str, Any], scope: str
) -> None:
    """Persist the ONIX-oriented identity fields added to ContributorCard."""
    _, inverted_raw = _payload_scoped_value(
        payload, scope, "personNameInverted", "person_name_inverted"
    )
    _, pen_name_raw = _payload_scoped_value(
        payload, scope, "penName", "pen_name", "pseudonym"
    )

    inverted = _safe_str(inverted_raw)
    pen_name = _safe_str(pen_name_raw)

    cur.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'parties'
          AND column_name = ANY(%s)
        """,
        (["person_name_inverted", "pen_name", "pseudonym"],),
    )
    columns = {str(row["column_name"]) for row in (cur.fetchall() or [])}

    assignments: List[str] = []
    values: List[Any] = []

    if inverted and "person_name_inverted" in columns:
        assignments.append("person_name_inverted = %s")
        values.append(inverted)

    pen_column = "pen_name" if "pen_name" in columns else (
        "pseudonym" if "pseudonym" in columns else ""
    )
    if pen_name and pen_column:
        assignments.append(f"{pen_column} = %s")
        values.append(pen_name)

    if not assignments:
        return

    assignments.append("updated_at = now()")
    values.extend([tenant_id, party_id])
    cur.execute(
        f"""
        UPDATE parties
        SET {', '.join(assignments)}
        WHERE tenant_id = %s AND id = %s
        """,
        tuple(values),
    )


def _replace_party_identifiers(
    cur, tenant_id: str, party_id: str, payload: Dict[str, Any], scope: str
) -> None:
    present, raw_rows = _payload_scoped_value(payload, scope, "identifiers")
    if not present:
        return

    rows = raw_rows if isinstance(raw_rows, list) else []
    cur.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = 'party_identifiers'
        """
    )
    columns = {str(row["column_name"]) for row in (cur.fetchall() or [])}
    if not {"tenant_id", "party_id"}.issubset(columns):
        return

    type_column = next(
        (c for c in ("identifier_type", "identifier_type_code", "type") if c in columns),
        "",
    )
    value_column = next(
        (c for c in ("identifier_value", "identifier", "value") if c in columns),
        "",
    )
    order_column = next(
        (c for c in ("item_order", "sequence_number") if c in columns),
        "",
    )
    if not type_column or not value_column:
        return

    cur.execute(
        "DELETE FROM party_identifiers WHERE tenant_id = %s AND party_id = %s",
        (tenant_id, party_id),
    )

    for idx, row in enumerate(rows, start=1):
        if not isinstance(row, dict):
            continue
        identifier_type = _safe_str(
            row.get("type") or row.get("identifierType") or row.get("identifier_type")
        )
        identifier_value = _safe_str(
            row.get("value") or row.get("identifierValue") or row.get("identifier_value")
        )
        if not identifier_type and not identifier_value:
            continue

        insert_columns = ["tenant_id", "party_id", type_column, value_column]
        insert_values: List[Any] = [tenant_id, party_id, identifier_type, identifier_value]
        if order_column:
            insert_columns.append(order_column)
            insert_values.append(idx)

        placeholders = ", ".join(["%s"] * len(insert_columns))
        cur.execute(
            f"INSERT INTO party_identifiers ({', '.join(insert_columns)}) VALUES ({placeholders})",
            tuple(insert_values),
        )


def _replace_party_awards(
    cur, tenant_id: str, party_id: str, payload: Dict[str, Any], scope: str
) -> None:
    present, raw_rows = _payload_scoped_value(payload, scope, "awards", "honors")
    if not present:
        return

    rows = raw_rows if isinstance(raw_rows, list) else []
    table_name = ""
    columns: set[str] = set()
    for candidate in ("party_awards", "party_honors", "contributor_awards"):
        cur.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = %s
            """,
            (candidate,),
        )
        candidate_columns = {
            str(row["column_name"]) for row in (cur.fetchall() or [])
        }
        if {"tenant_id", "party_id"}.issubset(candidate_columns):
            table_name = candidate
            columns = candidate_columns
            break

    if not table_name:
        return

    name_column = next(
        (c for c in ("award_name", "honor_name", "name", "title") if c in columns),
        "",
    )
    if not name_column:
        return

    year_column = next(
        (c for c in ("award_year", "year_text", "year") if c in columns), ""
    )
    result_column = next((c for c in ("result", "status") if c in columns), "")
    notes_column = "notes" if "notes" in columns else ""
    order_column = next(
        (c for c in ("item_order", "sequence_number") if c in columns), ""
    )

    cur.execute(
        f"DELETE FROM {table_name} WHERE tenant_id = %s AND party_id = %s",
        (tenant_id, party_id),
    )

    for idx, row in enumerate(rows, start=1):
        if not isinstance(row, dict):
            continue
        name = _safe_str(row.get("name") or row.get("award") or row.get("honor"))
        year = _safe_str(row.get("year"))
        result = _safe_str(row.get("result"))
        notes = _safe_str(row.get("notes"))
        if not any((name, year, result, notes)):
            continue

        insert_columns = ["tenant_id", "party_id", name_column]
        insert_values: List[Any] = [tenant_id, party_id, name]
        for column, value in (
            (year_column, year),
            (result_column, result),
            (notes_column, notes),
            (order_column, idx),
        ):
            if column:
                insert_columns.append(column)
                insert_values.append(value)

        placeholders = ", ".join(["%s"] * len(insert_columns))
        cur.execute(
            f"INSERT INTO {table_name} ({', '.join(insert_columns)}) VALUES ({placeholders})",
            tuple(insert_values),
        )


def _replace_contributor_other_publications(
    cur, tenant_id: str, party_id: str, rows: List[Dict[str, Any]], scope: str = "author"
) -> None:
    cur.execute(
        """
        DELETE FROM contributor_other_publications
        WHERE tenant_id = %s AND party_id = %s AND scope = %s
        """,
        (tenant_id, party_id, scope),
    )

    for idx, item in enumerate(rows or [], start=1):
        if not isinstance(item, dict):
            continue

        title = _safe_str(item.get("title"))
        publication = _safe_str(
            item.get("publication")
            or item.get("publication_name")
        )
        date_text = _safe_str(
            item.get("date_text")
            or item.get("date")
            or item.get("publication_date")
        )
        notes = _safe_str(item.get("notes"))

        if not any([title, publication, date_text, notes]):
            continue

        cur.execute(
            """
            INSERT INTO contributor_other_publications (
                tenant_id,
                party_id,
                scope,
                item_order,
                title,
                publication,
                date_text,
                notes
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                tenant_id,
                party_id,
                scope,
                idx,
                title,
                publication,
                date_text,
                notes,
            ),
        )


def _is_blank_contact_payload_row(row: Dict[str, Any]) -> bool:
    if not isinstance(row, dict):
        return True
    for k, v in row.items():
        if k in ("personal_contact", "personalContact", "link_type", "contact_type"):
            continue
        if isinstance(v, bool):
            if v:
                return False
            continue
        if v is None:
            continue
        if isinstance(v, (int, float)) and v != 0:
            return False
        if _safe_str(v):
            return False
    return True


def _insert_contributor_contact_link_row(
    cur,
    tenant_id: str,
    party_id: str,
    scope_lc: str,
    category: str,
    item_order: int,
    row: Dict[str, Any],
) -> None:
    if _is_blank_contact_payload_row(row):
        return

    cd_id = str(uuid.uuid4())
    link_id = str(uuid.uuid4())
    category_lc = _safe_str(category).lower()

    explicit_company = "company_or_outlet" in row
    explicit_name = "name" in row

    name = _safe_str(row.get("name"))
    company = _safe_str(row.get("company_or_outlet"))
    position = _safe_str(row.get("position"))
    email = _safe_str(row.get("email"))
    phone = _safe_str(row.get("phone"))
    website = _safe_str(row.get("website"))
    street = _safe_str(row.get("street"))
    city = _safe_str(row.get("city"))
    state = _safe_str(row.get("state"))
    postal = _safe_str(row.get("zip") or row.get("postal_code"))
    country = _safe_str(row.get("country"))
    social_handle = _safe_str(row.get("social_handle"))
    notes = _safe_str(row.get("notes"))
    rel_note = _safe_str(row.get("relationship_note"))
    personal = bool(row.get("personal_contact") or False)
    link_type = _safe_str(row.get("link_type"))
    contact_type = _safe_str(row.get("contact_type"))

    if not explicit_company:
        company = _safe_str(
            row.get("company")
            or row.get("outlet")
            or row.get("chain_name")
            or row.get("publication")
            or row.get("entity_name")
        )

    if not explicit_name:
        name = _safe_str(row.get("contact_person"))

    if not website:
        website = _safe_str(row.get("url"))

    if not rel_note:
        rel_note = _safe_str(row.get("relationship") or row.get("connection"))

    if "_marketing_" in category_lc and not explicit_company and not company and name:
        company, name = name, ""

    if "_sales_" in category_lc:
        if not link_type:
            if category_lc.endswith("_sales_local_bookstores"):
                link_type = "local_bookstore"
            elif category_lc.endswith("_sales_schools_libraries"):
                link_type = "school_library"
            elif category_lc.endswith("_sales_societies_orgs_conf"):
                link_type = "society_org_conf"
            elif category_lc.endswith("_sales_nontrade_outlets"):
                link_type = "nontrade_outlet"
            elif category_lc.endswith("_sales_museums_parks"):
                link_type = "museum_park"
        if not contact_type:
            contact_type = link_type

    if not any([
        name, company, position, email, phone, website, street, city, state,
        postal, country, social_handle, notes, rel_note
    ]):
        return

    cd_vals = (
        cd_id, tenant_id, contact_type, name, company, position, email, phone,
        website, street, city, state, postal, country, social_handle, notes
    )

    cd_ok = False
    for postal_col in ("postal_code", "zip"):
        try:
            cur.execute(
                f"""
                INSERT INTO contact_directory (
                    id, tenant_id, contact_type, name, company_or_outlet,
                    position, email, phone, website, street, city, state,
                    {postal_col}, country, social_handle, notes
                )
                VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s
                )
                """,
                cd_vals,
            )
            cd_ok = True
            break
        except Exception:
            continue

    if not cd_ok:
        return

    try:
        cur.execute(
            """
            INSERT INTO contributor_contact_links (
                id, tenant_id, party_id, scope, category, link_type,
                item_order, personal_contact, relationship_note, contact_id
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                link_id, tenant_id, party_id, scope_lc, category, link_type,
                item_order, personal, rel_note, cd_id
            ),
        )
    except Exception:
        try:
            cur.execute(
                "DELETE FROM contact_directory WHERE tenant_id = %s AND id = %s",
                (tenant_id, cd_id),
            )
        except Exception:
            pass


def _contact_category_specs_for_scope(scope: str) -> List[Tuple[str, List[str]]]:
    s = scope
    return [
        (f"{s}_marketing_endorsers", [f"{s}_marketing_endorsers", f"{s}_publicity_endorsers_blurbers"]),
        (f"{s}_marketing_big_mouth_list", [f"{s}_marketing_big_mouth_list", f"{s}_publicity_big_mouth_list"]),
        (f"{s}_marketing_review_copy_wishlist", [f"{s}_marketing_review_copy_wishlist", f"{s}_publicity_review_copy_wishlist"]),
        (f"{s}_marketing_local_media", [f"{s}_marketing_local_media", f"{s}_publicity_local_media"]),
        (f"{s}_marketing_alumni_org_publications", [f"{s}_marketing_alumni_org_publications", f"{s}_publicity_alumni_org_publications"]),
        (f"{s}_marketing_targeted_sites", [f"{s}_marketing_targeted_sites", f"{s}_publicity_target_sites"]),
        (f"{s}_marketing_bloggers", [f"{s}_marketing_bloggers", f"{s}_publicity_bloggers_genre"]),
        (f"{s}_sales_local_bookstores", [f"{s}_sales_local_bookstores"]),
        (f"{s}_sales_schools_libraries", [f"{s}_sales_schools_libraries"]),
        (f"{s}_sales_societies_orgs_conf", [f"{s}_sales_societies_orgs_conf"]),
        (f"{s}_sales_nontrade_outlets", [f"{s}_sales_nontrade_outlets"]),
        (f"{s}_sales_museums_parks", [f"{s}_sales_museums_parks"]),
    ]


def _replace_contributor_contact_categories(
    cur, tenant_id: str, party_id: str, scope: str, payload: Dict[str, Any]
) -> None:
    """Replace only contact categories explicitly supplied with non-empty rows.

    A Book Management section save must never erase unrelated marketing, publicity,
    or sales collections. Empty or absent arrays mean preserve existing data.
    """
    scope_lc = (scope or "author").lower()

    for category, alias_keys in _contact_category_specs_for_scope(scope_lc):
        rows: Optional[List[Dict[str, Any]]] = None
        for key in alias_keys:
            value = payload.get(key)
            if isinstance(value, list):
                cleaned = [row for row in value if isinstance(row, dict) and not _is_blank_contact_payload_row(row)]
                if cleaned:
                    rows = cleaned
                    break

        if rows is None:
            continue

        try:
            cur.execute(
                """
                SELECT contact_id
                FROM contributor_contact_links
                WHERE tenant_id = %s
                  AND party_id = %s
                  AND lower(scope) = %s
                  AND lower(category) = lower(%s)
                """,
                (tenant_id, party_id, scope_lc, category),
            )
            old_contact_ids = [str(r["contact_id"]) for r in (cur.fetchall() or []) if r.get("contact_id")]

            cur.execute(
                """
                DELETE FROM contributor_contact_links
                WHERE tenant_id = %s
                  AND party_id = %s
                  AND lower(scope) = %s
                  AND lower(category) = lower(%s)
                """,
                (tenant_id, party_id, scope_lc, category),
            )

            for old_contact_id in old_contact_ids:
                cur.execute(
                    """
                    DELETE FROM contact_directory cd
                    WHERE cd.tenant_id = %s
                      AND cd.id = %s
                      AND NOT EXISTS (
                          SELECT 1 FROM contributor_contact_links ccl
                          WHERE ccl.contact_id = cd.id
                      )
                      AND NOT EXISTS (
                          SELECT 1 FROM work_contact_links wcl
                          WHERE wcl.contact_id = cd.id
                      )
                    """,
                    (tenant_id, old_contact_id),
                )
        except Exception:
            continue

        for idx, row in enumerate(rows, start=1):
            try:
                _insert_contributor_contact_link_row(
                    cur, tenant_id, party_id, scope_lc, category, idx, row
                )
            except Exception:
                pass


def _replace_contributor_published_books(
    cur, tenant_id: str, party_id: str, payload: Dict[str, Any], scope: str
) -> None:
    rows = payload.get(f"{scope}_books_published") or payload.get(f"{scope}_published_books") or []
    cur.execute(
        """
        DELETE FROM contributor_published_books
        WHERE tenant_id = %s AND party_id = %s AND lower(scope) = %s
        """,
        (tenant_id, party_id, scope.lower()),
    )
    for idx, b in enumerate(rows, start=1):
        if not isinstance(b, dict):
            continue
        cur.execute(
            """
            INSERT INTO contributor_published_books (
                tenant_id, party_id, scope, item_order,
                title, isbn, publisher, publication_year, approx_sold
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                tenant_id,
                party_id,
                scope,
                idx,
                _safe_str(b.get("title")),
                _safe_str(b.get("isbn")),
                _safe_str(b.get("publisher")),
                _safe_str(b.get("year") or b.get("publication_year")),
                _safe_str(b.get("approx_sold")),
            ),
        )


def _replace_contributor_media_appearances(
    cur, tenant_id: str, party_id: str, payload: Dict[str, Any], scope: str
) -> None:
    rows = payload.get(f"{scope}_media_appearances") or []
    cur.execute(
        """
        DELETE FROM contributor_media_appearances
        WHERE tenant_id = %s AND party_id = %s AND lower(scope) = %s
        """,
        (tenant_id, party_id, scope.lower()),
    )
    for idx, m in enumerate(rows, start=1):
        if not isinstance(m, dict):
            continue
        cur.execute(
            """
            INSERT INTO contributor_media_appearances (
                tenant_id, party_id, scope, item_order,
                title, venue, date_text, link, notes
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                tenant_id,
                party_id,
                scope,
                idx,
                _safe_str(m.get("title")),
                _safe_str(m.get("venue")),
                _safe_str(m.get("appearance_date") or m.get("date")),
                _safe_str(m.get("link")),
                _safe_str(m.get("notes")),
            ),
        )


def _replace_contributor_media_contacts(
    cur, tenant_id: str, party_id: str, payload: Dict[str, Any], scope: str
) -> None:
    rows = payload.get(f"{scope}_media_contacts") or []
    cur.execute(
        """
        DELETE FROM contributor_media_contacts
        WHERE tenant_id = %s AND party_id = %s AND lower(scope) = %s
        """,
        (tenant_id, party_id, scope.lower()),
    )
    for idx, m in enumerate(rows, start=1):
        if not isinstance(m, dict):
            continue
        cur.execute(
            """
            INSERT INTO contributor_media_contacts (
                tenant_id, party_id, scope, item_order,
                company, name, position, phone, email
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                tenant_id,
                party_id,
                scope,
                idx,
                _safe_str(m.get("company")),
                _safe_str(m.get("name")),
                _safe_str(m.get("position")),
                _safe_str(m.get("phone")),
                _safe_str(m.get("email")),
            ),
        )


def _replace_contributor_previous_publicity(
    cur, tenant_id: str, party_id: str, scope: str, rows: List[Dict[str, Any]]
) -> None:
    cur.execute(
        """
        DELETE FROM contributor_previous_publicity
        WHERE tenant_id = %s AND party_id = %s AND lower(scope) = %s
        """,
        (tenant_id, party_id, scope.lower()),
    )
    for idx, item in enumerate(rows or [], start=1):
        if not isinstance(item, dict):
            continue
        try:
            cur.execute(
                """
                INSERT INTO contributor_previous_publicity (
                    tenant_id, party_id, scope, item_order,
                    outlet_or_title, contact, relationship_note, notes, source_category
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    tenant_id,
                    party_id,
                    scope,
                    idx,
                    _safe_str(
                        item.get("outlet_or_title")
                        or item.get("outlet")
                        or item.get("publicity_name")
                    ),
                    _safe_str(item.get("contact")),
                    _safe_str(item.get("relationship_note") or item.get("relationship")),
                    _safe_str(item.get("notes")),
                    _safe_str(item.get("source_category")),
                ),
            )
        except Exception:
            pass


def _replace_contributor_niche_targets(
    cur, tenant_id: str, party_id: str, scope: str, rows: List[Dict[str, Any]]
) -> None:
    scope_lc = (scope or "author").lower()
    cur.execute(
        """
        DELETE FROM contributor_niche_publicity_targets
        WHERE tenant_id = %s AND party_id = %s AND lower(coalesce(scope, '')) = %s
        """,
        (tenant_id, party_id, scope_lc),
    )

    cleaned_rows: List[Dict[str, Any]] = []
    for item in rows or []:
        if not isinstance(item, dict):
            continue

        area = _safe_str(
            item.get("area")
            or item.get("target_area")
            or item.get("target_name")
        )
        notes = _safe_str(item.get("notes"))

        if not area and not notes:
            continue

        cleaned_rows.append(
            {
                "area": area,
                "notes": notes,
                "source_category": "niche_publicity",
            }
        )

    for idx, item in enumerate(cleaned_rows, start=1):
        try:
            cur.execute(
                """
                INSERT INTO contributor_niche_publicity_targets (
                    tenant_id, party_id, scope, item_order,
                    target_area, notes, source_category
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    tenant_id,
                    party_id,
                    scope_lc,
                    idx,
                    item["area"],
                    item["notes"],
                    item["source_category"],
                ),
            )
        except Exception:
            pass




def _table_column_names(cur, table_name: str) -> set[str]:
    cur.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = %s
        """,
        (table_name,),
    )
    return {str(row["column_name"]) for row in (cur.fetchall() or [])}


def update_edition_product_identity(
    cur,
    tenant_id: str,
    work_id: str,
    edition_id: str,
    payload: Dict[str, Any],
) -> Dict[str, Any]:
    """Persist only Product Identity fields for one existing edition."""

    try:
        edition_uuid = str(uuid.UUID(str(edition_id)))
    except (ValueError, TypeError):
        raise ValueError("Invalid edition id")

    cur.execute(
        """
        SELECT id, isbn13
        FROM editions
        WHERE tenant_id = %s
          AND work_id = %s
          AND id = %s
        LIMIT 1
        """,
        (tenant_id, work_id, edition_uuid),
    )
    existing = cur.fetchone()
    if not existing:
        raise ValueError("Edition not found")

    raw_isbn = _safe_str(
        payload.get("isbn13")
        or payload.get("isbn")
        or payload.get("ISBN")
    )
    normalized_isbn = _normalize_isbn13(raw_isbn) if raw_isbn else ""

    if raw_isbn and not normalized_isbn:
        raise ValueError("ISBN-13 must contain 13 digits")

    if normalized_isbn:
        cur.execute(
            """
            SELECT id
            FROM editions
            WHERE tenant_id = %s
              AND isbn13 = %s
              AND id <> %s
            LIMIT 1
            """,
            (tenant_id, normalized_isbn, edition_uuid),
        )
        if cur.fetchone():
            raise ValueError("That ISBN-13 is already assigned to another edition")

    product_form = _safe_str(payload.get("product_form") or payload.get("format"))
    product_form_detail = _safe_str(
        payload.get("product_form_detail") or payload.get("format_label") or product_form
    )
    onix_product_form = _safe_str(payload.get("onix_product_form"))
    onix_product_form_detail = _safe_str(payload.get("onix_product_form_detail"))
    notification_type = _safe_str(payload.get("notification_type"))
    product_composition = _safe_str(payload.get("product_composition"))
    primary_content_type = _safe_str(
        payload.get("primary_content_type") or payload.get("product_content_type")
    )
    barcode_type = _safe_str(payload.get("barcode_type"))
    barcode_position_on_product = _safe_str(
        payload.get("barcode_position_on_product")
        or payload.get("barcodePositionOnProduct")
        or payload.get("position_on_product")
        or payload.get("positionOnProduct")
    )

    if barcode_type == "00":
        barcode_position_on_product = ""
    product_packaging = _safe_str(
        payload.get("product_packaging")
        or payload.get("productPackaging")
    )
    record_reference = _safe_str(
        payload.get("record_reference")
        or payload.get("recordReference")
    )
    record_source_type = _safe_str(
        payload.get("record_source_type")
        or payload.get("recordSourceType")
    )
    record_source_name = _safe_str(
        payload.get("record_source_name")
        or payload.get("recordSourceName")
    )
    publishing_status = _safe_str(payload.get("publishing_status"))

    cur.execute(
        """
        UPDATE editions
        SET
            isbn13 = CASE
                WHEN %s <> '' THEN %s
                ELSE isbn13
            END,
            product_form = CASE WHEN %s <> '' THEN %s ELSE product_form END,
            product_form_detail = CASE WHEN %s <> '' THEN %s ELSE product_form_detail END,
            onix_product_form = %s,
            onix_product_form_detail = %s,
            notification_type = %s,
            product_composition = %s,
            primary_content_type = %s,
            barcode_type = %s,
            barcode_position_on_product = %s,
            product_packaging = %s,
            record_reference = %s,
            record_source_type = %s,
            record_source_name = %s,
            publishing_status = %s,
            updated_at = now()
        WHERE tenant_id = %s
          AND work_id = %s
          AND id = %s
        """,
        (
            normalized_isbn,
            normalized_isbn,
            product_form,
            product_form,
            product_form_detail,
            product_form_detail,
            onix_product_form,
            onix_product_form_detail,
            notification_type,
            product_composition,
            primary_content_type,
            barcode_type,
            barcode_position_on_product,
            product_packaging,
            record_reference,
            record_source_type,
            record_source_name,
            publishing_status,
            tenant_id,
            work_id,
            edition_uuid,
        ),
    )

    # Replace non-ISBN identifiers for this edition. ISBN remains authoritative
    # in editions.isbn13 and the normalized id_type=15 row is synchronized below.
    if _table_has_columns(
        cur,
        "edition_identifiers",
        ["tenant_id", "edition_id", "id_type", "id_value"],
    ):
        cur.execute(
            """
            DELETE FROM edition_identifiers
            WHERE tenant_id = %s
              AND edition_id = %s
              AND id_type <> '15'
            """,
            (tenant_id, edition_uuid),
        )

        rows = payload.get("product_identifiers") or []
        if isinstance(rows, list):
            item_order = 2
            for row in rows:
                if not isinstance(row, dict):
                    continue

                id_type = _safe_str(
                    row.get("id_type")
                    or row.get("identifier_type")
                    or row.get("identifierType")
                )
                id_type_name = _safe_str(
                    row.get("id_type_name")
                    or row.get("identifier_type_name")
                    or row.get("identifierTypeName")
                )
                id_value = _safe_str(
                    row.get("id_value")
                    or row.get("identifier_value")
                    or row.get("identifierValue")
                    or row.get("value")
                )

                if not id_type or not id_value:
                    continue
                if id_type == "15":
                    continue

                cur.execute(
                    """
                    INSERT INTO edition_identifiers (
                        tenant_id,
                        edition_id,
                        id_type,
                        id_type_name,
                        id_value,
                        item_order
                    )
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (
                        tenant_id,
                        edition_id,
                        id_type,
                        id_value
                    ) DO UPDATE SET
                        id_type_name = EXCLUDED.id_type_name,
                        item_order = EXCLUDED.item_order
                    """,
                    (
                        tenant_id,
                        edition_uuid,
                        id_type,
                        id_type_name,
                        id_value,
                        item_order,
                    ),
                )
                item_order += 1

        # Synchronize ISBN identifier only after the ISBN is assigned.
        if normalized_isbn:
            cur.execute(
                """
                DELETE FROM edition_identifiers
                WHERE tenant_id = %s
                  AND edition_id = %s
                  AND id_type = '15'
                  AND id_value <> %s
                """,
                (tenant_id, edition_uuid, normalized_isbn),
            )
            cur.execute(
                """
                INSERT INTO edition_identifiers (
                    tenant_id,
                    edition_id,
                    id_type,
                    id_type_name,
                    id_value,
                    item_order
                )
                VALUES (%s, %s, '15', '', %s, 1)
                ON CONFLICT (
                    tenant_id,
                    edition_id,
                    id_type,
                    id_value
                ) DO UPDATE SET item_order = 1
                """,
                (tenant_id, edition_uuid, normalized_isbn),
            )

    # Publisher is edition-level ONIX metadata.
    # Role 01 is the canonical Publisher composite for this edition.
    publisher = _safe_str(
        payload.get("publisher")
        or payload.get("publisher_name")
        or payload.get("publisherName")
    )

    cur.execute(
        """
        DELETE FROM edition_publishers
        WHERE tenant_id = %s
          AND edition_id = %s
          AND publishing_role = '01'
        """,
        (tenant_id, edition_uuid),
    )

    if publisher:
        cur.execute(
            """
            INSERT INTO edition_publishers (
                id,
                tenant_id,
                edition_id,
                publishing_role,
                publisher_name,
                item_order
            )
            VALUES (%s, %s, %s, '01', %s, 1)
            """,
            (
                str(uuid.uuid4()),
                tenant_id,
                edition_uuid,
                publisher,
            ),
        )

    # Imprint is still temporarily work-level until its normalized ONIX
    # storage is migrated. Country/language fields are also left unchanged
    # by this publisher-only cleanup.
    work_columns = _table_column_names(cur, "works")
    updates: list[str] = []
    values: list[Any] = []

    imprint = _safe_str(payload.get("imprint"))
    country = _safe_str(payload.get("country_of_publication"))
    language = _safe_str(payload.get("language"))
    original_language = _safe_str(payload.get("original_language"))

    if "imprint_name" in work_columns:
        updates.append("imprint_name = %s")
        values.append(imprint)
    if "country_of_publication" in work_columns:
        updates.append("country_of_publication = %s")
        values.append(country)
    if "language" in work_columns:
        updates.append("language = %s")
        values.append(language)
    if "original_language" in work_columns:
        updates.append("original_language = %s")
        values.append(original_language)

    if updates:
        updates.append("updated_at = now()")
        values.extend([tenant_id, work_id])
        cur.execute(
            f"""
            UPDATE works
            SET {", ".join(updates)}
            WHERE tenant_id = %s
              AND id = %s
            """,
            tuple(values),
        )

    return {
        "ok": True,
        "work_id": work_id,
        "edition_id": edition_uuid,
    }



def _uuid_or_none(value: Any) -> Optional[str]:
    raw = _safe_str(value)
    if not raw:
        return None
    try:
        return str(uuid.UUID(raw))
    except (ValueError, TypeError):
        return None



def update_work_titles_collections(
    cur,
    tenant_id: str,
    work_id: str,
    payload: Dict[str, Any],
) -> Dict[str, Any]:
    """Persist the work-level Titles & Collections metadata card."""

    work_id = _safe_str(work_id)
    if not work_id:
        raise ValueError("work_id is required")

    cur.execute(
        """
        SELECT id FROM works
        WHERE tenant_id = %s AND id = %s
        LIMIT 1
        """,
        (tenant_id, work_id),
    )
    if not cur.fetchone():
        raise ValueError("Existing work not found")

    title = _safe_str(payload.get("title"))
    subtitle = _safe_str(payload.get("subtitle"))
    short_title = _safe_str(payload.get("short_title") or payload.get("shortTitle"))
    title_prefix = _safe_str(payload.get("title_prefix") or payload.get("titlePrefix"))
    title_without_prefix = _safe_str(
        payload.get("title_without_prefix") or payload.get("titleWithoutPrefix")
    )
    title_element_level = _safe_str(
        payload.get("title_element_level") or payload.get("titleElementLevel")
    ) or "01"
    no_prefix = bool(
        payload.get("no_prefix")
        if payload.get("no_prefix") is not None
        else payload.get("noPrefix", False)
    )
    title_part_number = _safe_str(
        payload.get("title_part_number")
        or payload.get("titlePartNumber")
        or payload.get("part_number")
        or payload.get("partNumber")
    )

    if not title:
        raise ValueError("Title is required")

    work_columns = _table_column_names(cur, "works")
    updates: List[str] = ["title = %s", "subtitle = %s"]
    values: List[Any] = [title, subtitle]

    if "short_title" in work_columns:
        updates.append("short_title = %s")
        values.append(short_title)

    collections = payload.get("collections") or payload.get("collection_memberships") or []
    if not isinstance(collections, list):
        collections = []

    primary_collection = next(
        (
            row
            for row in collections
            if isinstance(row, dict)
            and _safe_str(row.get("title") or row.get("collection_title"))
        ),
        None,
    )

    if primary_collection and "series_title" in work_columns:
        updates.append("series_title = %s")
        values.append(
            _safe_str(
                primary_collection.get("title")
                or primary_collection.get("collection_title")
            )
        )

    if primary_collection and "series_number" in work_columns:
        raw_series_number = _safe_str(
            primary_collection.get("collection_number")
            or primary_collection.get("collectionNumber")
            or primary_collection.get("volume_number")
            or primary_collection.get("volumeNumber")
        )
        try:
            series_number_value = int(raw_series_number) if raw_series_number else 0
        except ValueError:
            series_number_value = 0
        updates.append("series_number = %s")
        values.append(series_number_value)

    updates.append("updated_at = now()")
    values.extend([tenant_id, work_id])
    cur.execute(
        f"""
        UPDATE works
        SET {", ".join(updates)}
        WHERE tenant_id = %s AND id = %s
        """,
        tuple(values),
    )

    if _table_has_columns(
        cur,
        "work_titles",
        [
            "tenant_id", "work_id", "title_type", "title_element_level",
            "title_prefix", "title_without_prefix", "subtitle", "part_number",
            "no_prefix", "language_code", "is_primary", "item_order",
        ],
    ):
        cur.execute(
            """
            SELECT id FROM work_titles
            WHERE tenant_id = %s AND work_id = %s AND is_primary = true
            LIMIT 1
            """,
            (tenant_id, work_id),
        )
        primary_row = cur.fetchone()

        if primary_row:
            cur.execute(
                """
                UPDATE work_titles
                SET title_type = '01',
                    title_element_level = %s,
                    title_prefix = %s,
                    title_without_prefix = %s,
                    subtitle = %s,
                    part_number = %s,
                    no_prefix = %s,
                    item_order = 1,
                    updated_at = now()
                WHERE tenant_id = %s AND work_id = %s AND id = %s
                """,
                (
                    title_element_level, title_prefix, title_without_prefix,
                    subtitle, title_part_number, no_prefix,
                    tenant_id, work_id, primary_row["id"],
                ),
            )
        else:
            cur.execute(
                """
                INSERT INTO work_titles (
                    tenant_id, work_id, title_type, title_element_level,
                    title_prefix, title_without_prefix, subtitle, part_number,
                    no_prefix, language_code, is_primary, item_order
                )
                VALUES (%s, %s, '01', %s, %s, %s, %s, %s, %s, '', true, 1)
                """,
                (
                    tenant_id, work_id, title_element_level, title_prefix,
                    title_without_prefix, subtitle, title_part_number, no_prefix,
                ),
            )

        alternative_titles = payload.get("alternative_titles") or payload.get("alternativeTitles") or []
        if not isinstance(alternative_titles, list):
            alternative_titles = []

        keep_title_ids: List[str] = []
        for index, row in enumerate(alternative_titles, start=2):
            if not isinstance(row, dict):
                continue

            row_id = _uuid_or_none(row.get("id"))
            alt_type = _safe_str(row.get("title_type") or row.get("titleType")) or "06"
            alt_level = _safe_str(
                row.get("title_element_level") or row.get("titleElementLevel")
            ) or "01"
            alt_no_prefix = bool(
                row.get("no_prefix")
                if row.get("no_prefix") is not None
                else row.get("noPrefix", False)
            )
            alt_part_number = _safe_str(row.get("part_number") or row.get("partNumber"))
            alt_title_prefix = _safe_str(row.get("title_prefix") or row.get("titlePrefix"))
            alt_title = _safe_str(
                row.get("title_without_prefix")
                or row.get("titleWithoutPrefix")
                or row.get("title")
                or row.get("title_text")
                or row.get("titleText")
            )
            # A supplied prefix means this title is explicitly prefixed; otherwise
            # ONIX should emit NoPrefix + TitleWithoutPrefix.
            alt_no_prefix = not bool(alt_title_prefix)
            alt_subtitle = _safe_str(row.get("subtitle"))
            language_code = _safe_str(row.get("language_code") or row.get("languageCode"))

            if not (alt_title or alt_subtitle or language_code or alt_part_number):
                continue

            existing = None
            if row_id:
                cur.execute(
                    """
                    SELECT id FROM work_titles
                    WHERE tenant_id = %s AND work_id = %s AND id = %s
                      AND is_primary = false
                    LIMIT 1
                    """,
                    (tenant_id, work_id, row_id),
                )
                existing = cur.fetchone()

            if existing:
                cur.execute(
                    """
                    UPDATE work_titles
                    SET title_type = %s,
                        title_element_level = %s,
                        title_prefix = %s,
                        title_without_prefix = %s,
                        subtitle = %s,
                        part_number = %s,
                        no_prefix = %s,
                        language_code = %s,
                        item_order = %s,
                        updated_at = now()
                    WHERE tenant_id = %s AND work_id = %s AND id = %s
                    """,
                    (
                        alt_type, alt_level, alt_title_prefix, alt_title, alt_subtitle,
                        alt_part_number, alt_no_prefix, language_code, index,
                        tenant_id, work_id, row_id,
                    ),
                )
                saved_id = row_id
            else:
                cur.execute(
                    """
                    INSERT INTO work_titles (
                        tenant_id, work_id, title_type, title_element_level,
                        title_prefix, title_without_prefix, subtitle, part_number,
                        no_prefix, language_code, is_primary, item_order
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, false, %s)
                    RETURNING id
                    """,
                    (
                        tenant_id, work_id, alt_type, alt_level, alt_title_prefix, alt_title,
                        alt_subtitle, alt_part_number, alt_no_prefix,
                        language_code, index,
                    ),
                )
                saved_id = str((cur.fetchone() or {}).get("id") or "")

            if saved_id:
                keep_title_ids.append(saved_id)

        if keep_title_ids:
            cur.execute(
                """
                DELETE FROM work_titles
                WHERE tenant_id = %s AND work_id = %s
                  AND is_primary = false
                  AND NOT (id = ANY(%s::uuid[]))
                """,
                (tenant_id, work_id, keep_title_ids),
            )
        else:
            cur.execute(
                """
                DELETE FROM work_titles
                WHERE tenant_id = %s AND work_id = %s AND is_primary = false
                """,
                (tenant_id, work_id),
            )

    if _table_has_columns(
        cur,
        "work_collections",
        [
            "tenant_id", "work_id", "collection_type", "title_type",
            "title_element_level", "no_prefix", "collection_title",
            "collection_subtitle", "collection_number", "volume_number",
            "part_number", "is_primary", "item_order",
        ],
    ):
        keep_collection_ids: List[str] = []

        for index, row in enumerate(collections):
            if not isinstance(row, dict):
                continue

            row_id = _uuid_or_none(row.get("id"))
            collection_type = _safe_str(row.get("collection_type") or row.get("collectionType")) or "10"
            collection_title_type = _safe_str(row.get("title_type") or row.get("titleType")) or "01"
            collection_level = _safe_str(
                row.get("title_element_level") or row.get("titleElementLevel")
            ) or "02"
            collection_no_prefix = bool(
                row.get("no_prefix")
                if row.get("no_prefix") is not None
                else row.get("noPrefix", True)
            )
            collection_title = _safe_str(row.get("title") or row.get("collection_title") or row.get("collectionTitle"))
            collection_subtitle = _safe_str(row.get("subtitle") or row.get("collection_subtitle") or row.get("collectionSubtitle"))
            collection_number = _safe_str(row.get("collection_number") or row.get("collectionNumber"))
            volume_number = _safe_str(row.get("volume_number") or row.get("volumeNumber"))
            part_number = _safe_str(row.get("part_number") or row.get("partNumber"))

            if not (collection_title or collection_subtitle or collection_number or volume_number or part_number):
                continue

            existing = None

            # ONIX imports do not carry InkSuite's internal work_collections.id.
            # Resolve the existing row semantically so repeated imports update
            # the collection instead of attempting to create another primary row.
            if row_id:
                cur.execute(
                    """
                    SELECT id
                    FROM work_collections
                    WHERE tenant_id = %s
                      AND work_id = %s
                      AND id = %s
                    LIMIT 1
                    """,
                    (tenant_id, work_id, row_id),
                )
                existing = cur.fetchone()

            # The first incoming ONIX Collection is the primary collection.
            # The DB enforces one primary collection per work, so reuse it.
            if not existing and index == 0:
                cur.execute(
                    """
                    SELECT id
                    FROM work_collections
                    WHERE tenant_id = %s
                      AND work_id = %s
                      AND is_primary = true
                    ORDER BY item_order ASC NULLS LAST, created_at ASC, id ASC
                    LIMIT 1
                    """,
                    (tenant_id, work_id),
                )
                existing = cur.fetchone()
                if existing:
                    row_id = str(existing["id"])

            # For additional collections, match the existing row by its ONIX
            # identity rather than blindly inserting on every re-import.
            if not existing and index > 0:
                cur.execute(
                    """
                    SELECT id
                    FROM work_collections
                    WHERE tenant_id = %s
                      AND work_id = %s
                      AND is_primary = false
                      AND COALESCE(collection_type, '') = %s
                      AND COALESCE(collection_title, '') = %s
                      AND COALESCE(collection_subtitle, '') = %s
                      AND COALESCE(collection_number, '') = %s
                      AND COALESCE(volume_number, '') = %s
                      AND COALESCE(part_number, '') = %s
                    ORDER BY item_order ASC NULLS LAST, created_at ASC, id ASC
                    LIMIT 1
                    """,
                    (
                        tenant_id,
                        work_id,
                        collection_type,
                        collection_title,
                        collection_subtitle,
                        collection_number,
                        volume_number,
                        part_number,
                    ),
                )
                existing = cur.fetchone()
                if existing:
                    row_id = str(existing["id"])

            if existing:
                cur.execute(
                    """
                    UPDATE work_collections
                    SET collection_type = %s,
                        title_type = %s,
                        title_element_level = %s,
                        no_prefix = %s,
                        collection_title = %s,
                        collection_subtitle = %s,
                        collection_number = %s,
                        volume_number = %s,
                        part_number = %s,
                        is_primary = %s,
                        item_order = %s,
                        updated_at = now()
                    WHERE tenant_id = %s AND work_id = %s AND id = %s
                    """,
                    (
                        collection_type, collection_title_type, collection_level,
                        collection_no_prefix, collection_title, collection_subtitle,
                        collection_number, volume_number, part_number, index == 0,
                        index + 1, tenant_id, work_id, row_id,
                    ),
                )
                saved_id = row_id
            else:
                cur.execute(
                    """
                    INSERT INTO work_collections (
                        tenant_id, work_id, collection_type, title_type,
                        title_element_level, no_prefix, collection_title,
                        collection_subtitle, collection_number, volume_number,
                        part_number, is_primary, item_order
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    RETURNING id
                    """,
                    (
                        tenant_id, work_id, collection_type, collection_title_type,
                        collection_level, collection_no_prefix, collection_title,
                        collection_subtitle, collection_number, volume_number,
                        part_number, index == 0, index + 1,
                    ),
                )
                saved_id = str((cur.fetchone() or {}).get("id") or "")

            if saved_id:
                keep_collection_ids.append(saved_id)

        if keep_collection_ids:
            cur.execute(
                """
                DELETE FROM work_collections
                WHERE tenant_id = %s AND work_id = %s
                  AND NOT (id = ANY(%s::uuid[]))
                """,
                (tenant_id, work_id, keep_collection_ids),
            )
        else:
            cur.execute(
                "DELETE FROM work_collections WHERE tenant_id = %s AND work_id = %s",
                (tenant_id, work_id),
            )

    return {"ok": True, "work_id": work_id}


def update_edition_descriptive_content(
    cur,
    tenant_id: str,
    work_id: str,
    edition_id: str,
    payload: Dict[str, Any],
) -> Dict[str, Any]:
    """Persist Descriptive Content for one existing edition."""

    try:
        edition_uuid = str(uuid.UUID(str(edition_id)))
    except (ValueError, TypeError):
        raise ValueError("Invalid edition id")

    cur.execute(
        """
        SELECT id FROM editions
        WHERE tenant_id = %s AND work_id = %s AND id = %s
        LIMIT 1
        """,
        (tenant_id, work_id, edition_uuid),
    )
    if not cur.fetchone():
        raise ValueError("Edition not found")

    rows = payload.get("descriptive_texts") or payload.get("descriptiveTexts") or payload.get("texts") or []
    if not isinstance(rows, list):
        rows = []

    keep_ids: List[str] = []

    for index, row in enumerate(rows):
        if not isinstance(row, dict):
            continue

        row_id = _uuid_or_none(row.get("id"))
        text_type = _safe_str(row.get("text_type") or row.get("textType"))
        text_format = _safe_str(row.get("text_format") or row.get("textFormat")) or "06"
        text_value = _safe_str(row.get("text_value") or row.get("text") or row.get("text_content") or row.get("textContent"))
        source_corporate = _safe_str(row.get("source_corporate") or row.get("sourceCorporate") or row.get("source_name") or row.get("sourceName"))
        source_title = _safe_str(row.get("source_title") or row.get("sourceTitle"))
        source_title_type = _safe_str(row.get("source_title_type") or row.get("sourceTitleType"))
        source_url = _safe_str(row.get("source_url") or row.get("sourceUrl"))
        author = _safe_str(row.get("author") or row.get("text_author") or row.get("textAuthor"))
        content_audience = _safe_str(row.get("content_audience") or row.get("contentAudience"))
        language_code = _safe_str(row.get("language_code") or row.get("languageCode"))
        item_order = _to_int_or_none(row.get("item_order") or row.get("sequence_number") or row.get("sequenceNumber")) or index + 1

        if not (text_type or text_value or source_corporate or source_title or source_url or author):
            continue

        existing = None
        if row_id:
            cur.execute(
                """
                SELECT id FROM edition_texts
                WHERE tenant_id = %s AND edition_id = %s AND id = %s
                LIMIT 1
                """,
                (tenant_id, edition_uuid, row_id),
            )
            existing = cur.fetchone()

        if existing:
            cur.execute(
                """
                UPDATE edition_texts
                SET text_type = %s,
                    text_format = %s,
                    text_value = %s,
                    source_corporate = %s,
                    source_title = %s,
                    source_title_type = %s,
                    source_url = %s,
                    author = %s,
                    content_audience = %s,
                    language_code = %s,
                    item_order = %s,
                    updated_at = now()
                WHERE tenant_id = %s AND edition_id = %s AND id = %s
                """,
                (
                    text_type, text_format, text_value, source_corporate,
                    source_title, source_title_type, source_url, author, content_audience,
                    language_code, item_order, tenant_id, edition_uuid, row_id,
                ),
            )
            saved_id = row_id
        else:
            cur.execute(
                """
                INSERT INTO edition_texts (
                    tenant_id, edition_id, text_type, text_format, text_value,
                    source_corporate, source_title, source_title_type, source_url, author,
                    content_audience, language_code, item_order
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id
                """,
                (
                    tenant_id, edition_uuid, text_type, text_format, text_value,
                    source_corporate, source_title, source_title_type, source_url, author,
                    content_audience, language_code, item_order,
                ),
            )
            saved_id = str((cur.fetchone() or {}).get("id") or "")

        if not saved_id:
            continue

        keep_ids.append(saved_id)

        content_dates = row.get("content_dates") or row.get("contentDates") or []
        if not isinstance(content_dates, list):
            content_dates = []

        cur.execute(
            """
            DELETE FROM edition_text_content_dates
            WHERE tenant_id = %s AND edition_id = %s AND edition_text_id = %s
            """,
            (tenant_id, edition_uuid, saved_id),
        )

        for date_index, date_row in enumerate(content_dates):
            if not isinstance(date_row, dict):
                continue
            content_date_role = _safe_str(
                date_row.get("content_date_role")
                or date_row.get("contentDateRole")
                or date_row.get("date_role")
                or date_row.get("dateRole")
            )
            date_format = _safe_str(date_row.get("date_format") or date_row.get("dateFormat")) or "00"
            date_text = _safe_str(
                date_row.get("date_text")
                or date_row.get("dateText")
                or date_row.get("date_value")
                or date_row.get("dateValue")
                or date_row.get("date")
            )
            date_order = _to_int_or_none(
                date_row.get("item_order")
                or date_row.get("sequence_number")
                or date_row.get("sequenceNumber")
            ) or date_index + 1

            if not (content_date_role or date_text):
                continue

            cur.execute(
                """
                INSERT INTO edition_text_content_dates (
                    tenant_id, edition_id, edition_text_id, content_date_role,
                    date_format, date_text, item_order
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    tenant_id, edition_uuid, saved_id, content_date_role,
                    date_format, date_text, date_order,
                ),
            )

    if keep_ids:
        cur.execute(
            """
            DELETE FROM edition_text_content_dates
            WHERE tenant_id = %s AND edition_id = %s
              AND NOT (edition_text_id = ANY(%s::uuid[]))
            """,
            (tenant_id, edition_uuid, keep_ids),
        )
        cur.execute(
            """
            DELETE FROM edition_texts
            WHERE tenant_id = %s AND edition_id = %s
              AND NOT (id = ANY(%s::uuid[]))
            """,
            (tenant_id, edition_uuid, keep_ids),
        )
    else:
        cur.execute(
            "DELETE FROM edition_text_content_dates WHERE tenant_id = %s AND edition_id = %s",
            (tenant_id, edition_uuid),
        )
        cur.execute(
            "DELETE FROM edition_texts WHERE tenant_id = %s AND edition_id = %s",
            (tenant_id, edition_uuid),
        )

    return {"ok": True, "work_id": work_id, "edition_id": edition_uuid}

def _first_existing_column(
    columns: set[str],
    *candidates: str,
) -> Optional[str]:
    for candidate in candidates:
        if candidate in columns:
            return candidate
    return None


def _dynamic_insert(
    cur,
    table_name: str,
    values_by_column: Dict[str, Any],
) -> str:
    if not values_by_column:
        raise ValueError(
            f"No writable columns found for {table_name}"
        )

    columns = list(values_by_column.keys())
    placeholders = ", ".join(["%s"] * len(columns))

    cur.execute(
        f"""
        INSERT INTO {table_name}
            ({", ".join(columns)})
        VALUES
            ({placeholders})
        RETURNING id
        """,
        tuple(values_by_column[column] for column in columns),
    )

    row = cur.fetchone() or {}
    return str(row.get("id") or "")


def update_edition_subjects_audience(
    cur,
    tenant_id: str,
    work_id: str,
    edition_id: str,
    payload: Dict[str, Any],
) -> Dict[str, Any]:
    """Persist Subjects & Audience for one edition using the deployed schema."""

    try:
        edition_uuid = str(uuid.UUID(str(edition_id)))
    except (ValueError, TypeError):
        raise ValueError("Invalid edition id")

    cur.execute(
        """
        SELECT id
        FROM editions
        WHERE tenant_id = %s
          AND work_id = %s
          AND id = %s
        LIMIT 1
        """,
        (tenant_id, work_id, edition_uuid),
    )
    if not cur.fetchone():
        raise ValueError("Edition not found")

    # ---------------------------------------------------------------
    # Subjects
    # deployed columns:
    # scheme_id, subject_code, heading_text, region_code,
    # scheme_version, keywords, is_main, item_order
    # ---------------------------------------------------------------
    cur.execute(
        """
        DELETE FROM edition_subjects
        WHERE tenant_id = %s
          AND edition_id = %s
        """,
        (tenant_id, edition_uuid),
    )

    subjects = payload.get("subjects") or []
    if not isinstance(subjects, list):
        subjects = []

    main_already_written = False

    for index, row in enumerate(subjects):
        if not isinstance(row, dict):
            continue

        scheme_id = _safe_str(
            row.get("subject_scheme_identifier")
            or row.get("subjectSchemeIdentifier")
            or row.get("scheme_id")
            or row.get("schemeIdentifier")
        )

        if not scheme_id:
            continue

        scheme_name = _safe_str(
            row.get("subject_scheme_name")
            or row.get("subjectSchemeName")
            or row.get("scheme_name")
            or row.get("schemeName")
        )

        scheme_version = _safe_str(
            row.get("subject_scheme_version")
            or row.get("subjectSchemeVersion")
            or row.get("scheme_version")
            or row.get("schemeVersion")
        )

        subject_code = _safe_str(
            row.get("subject_code")
            or row.get("subjectCode")
            or row.get("code")
        )

        heading_text = _safe_str(
            row.get("subject_heading_text")
            or row.get("subjectHeadingText")
            or row.get("heading_text")
            or row.get("heading")
        )

        keywords = ""
        if scheme_id == "20":
            keywords = _safe_str(
                row.get("keywords")
                or row.get("subject_heading_text")
                or row.get("subjectHeadingText")
                or row.get("heading_text")
            )
            # Keywords belong in edition_subjects.keywords, not heading_text.
            subject_code = ""
            heading_text = ""

        requested_main = bool(
            row.get("main_subject")
            or row.get("mainSubject")
            or row.get("is_main")
        )
        is_main = requested_main and not main_already_written
        if is_main:
            main_already_written = True

        item_order = (
            _to_int_or_none(
                row.get("sequence_number")
                or row.get("sequenceNumber")
                or row.get("item_order")
            )
            or (index + 1)
        )

        cur.execute(
            """
            INSERT INTO edition_subjects (
                tenant_id,
                edition_id,
                scheme_id,
                scheme_name,
                subject_code,
                heading_text,
                region_code,
                scheme_version,
                keywords,
                is_main,
                item_order
            )
            VALUES (
                %s, %s, %s, %s, %s, %s, '', %s, %s, %s, %s
            )
            """,
            (
                tenant_id,
                edition_uuid,
                scheme_id,
                scheme_name,
                subject_code,
                heading_text,
                scheme_version,
                keywords,
                is_main,
                item_order,
            ),
        )

    # ---------------------------------------------------------------
    # Audience
    # deployed columns:
    # onix_audience_code, audience_range_qualifier,
    # range_precision_1, range_value_1,
    # range_precision_2, range_value_2, item_order
    # ---------------------------------------------------------------
    cur.execute(
        """
        DELETE FROM edition_audience
        WHERE tenant_id = %s
          AND edition_id = %s
        """,
        (tenant_id, edition_uuid),
    )

    audience_codes = (
        payload.get("audience_codes")
        or payload.get("audienceCodes")
        or []
    )
    if not isinstance(audience_codes, list):
        audience_codes = []

    item_order = 1

    for row in audience_codes:
        if isinstance(row, dict):
            code = _safe_str(row.get("audience_code") or row.get("audienceCode") or row.get("audience_code_value") or row.get("audienceCodeValue") or row.get("code"))
            audience_code_type = _safe_str(row.get("audience_code_type") or row.get("audienceCodeType")) or "01"
            audience_code_type_name = _safe_str(row.get("audience_code_type_name") or row.get("audienceCodeTypeName"))
        else:
            code = _safe_str(row)
            audience_code_type = "01"
            audience_code_type_name = ""

        if not code:
            continue

        cur.execute(
            """
            INSERT INTO edition_audience (
                tenant_id,
                edition_id,
                onix_audience_code,
                audience_code_type, audience_code_type_name,
                audience_range_qualifier,
                range_precision_1,
                range_value_1,
                range_precision_2,
                range_value_2,
                item_order
            )
            VALUES (
                %s, %s, %s, %s, %s, '', '', '', '', '', %s
            )
            """,
            (
                tenant_id,
                edition_uuid,
                code,
                audience_code_type, audience_code_type_name,
                item_order,
            ),
        )
        item_order += 1

    audience_ranges = (
        payload.get("audience_ranges")
        or payload.get("audienceRanges")
        or []
    )
    if not isinstance(audience_ranges, list):
        audience_ranges = []

    for row in audience_ranges:
        if not isinstance(row, dict):
            continue

        qualifier = _safe_str(
            row.get("audience_range_qualifier")
            or row.get("audienceRangeQualifier")
            or row.get("qualifier")
        )

        precision1 = _safe_str(
            row.get("audience_range_precision")
            or row.get("audienceRangePrecision")
            or row.get("precision")
        )

        value1 = _safe_str(
            row.get("audience_range_value")
            or row.get("audienceRangeValue")
            or row.get("value")
        )

        precision2 = _safe_str(
            row.get("audience_range_precision_2")
            or row.get("audienceRangePrecision2")
            or row.get("precision2")
        )

        value2 = _safe_str(
            row.get("audience_range_value_2")
            or row.get("audienceRangeValue2")
            or row.get("value2")
        )

        # AudienceRangeQualifier is the identity of an ONIX audience range.
        # Never persist a partial range without a qualifier. Besides producing an
        # invalid/incomplete range, a blank qualifier is also the sentinel used by
        # audience-code rows in edition_audience.
        if not qualifier:
            continue

        cur.execute(
            """
            INSERT INTO edition_audience (
                tenant_id,
                edition_id,
                onix_audience_code,
                audience_range_qualifier,
                range_precision_1,
                range_value_1,
                range_precision_2,
                range_value_2,
                item_order
            )
            VALUES (
                %s, %s, '', %s, %s, %s, %s, %s, %s
            )
            """,
            (
                tenant_id,
                edition_uuid,
                qualifier,
                precision1,
                value1,
                precision2,
                value2,
                item_order,
            ),
        )
        item_order += 1

    audience_description = _safe_str(
        payload.get("audience_description")
        or payload.get("audienceDescription")
    )

    cur.execute(
        """
        UPDATE editions
        SET
            audience_description = %s,
            updated_at = now()
        WHERE tenant_id = %s
          AND work_id = %s
          AND id = %s
        """,
        (
            audience_description,
            tenant_id,
            work_id,
            edition_uuid,
        ),
    )

    return {
        "ok": True,
        "work_id": work_id,
        "edition_id": edition_uuid,
    }





def update_edition_publishing_dates(
    cur,
    tenant_id: str,
    work_id: str,
    edition_id: str,
    payload: Dict[str, Any],
) -> Dict[str, Any]:
    """Persist ONIX publishing dates and preserve lexical precision."""

    try:
        edition_uuid = str(uuid.UUID(str(edition_id)))
    except (ValueError, TypeError):
        raise ValueError("Invalid edition id")

    cur.execute(
        """
        SELECT id FROM editions
        WHERE tenant_id = %s AND work_id = %s AND id = %s
        LIMIT 1
        """,
        (tenant_id, work_id, edition_uuid),
    )
    if not cur.fetchone():
        raise ValueError("Edition not found")

    rows = payload.get("publishing_dates") or payload.get("publishingDates") or []
    if not isinstance(rows, list):
        rows = []

    cur.execute(
        "DELETE FROM edition_publishing_dates WHERE tenant_id = %s AND edition_id = %s",
        (tenant_id, edition_uuid),
    )

    publication_date = None
    seen_dates = set()

    for index, row in enumerate(rows):
        if not isinstance(row, dict):
            continue

        date_role = _safe_str(
            row.get("date_role")
            or row.get("publishing_date_role")
            or row.get("dateRole")
            or row.get("publishingDateRole")
        )
        date_format = _safe_str(row.get("date_format") or row.get("dateFormat")) or "00"
        date_text = _safe_str(
            row.get("date_text")
            or row.get("dateText")
            or row.get("date_value")
            or row.get("dateValue")
            or row.get("display_date")
            or row.get("displayDate")
            or row.get("date")
        )
        compact = date_text.replace("-", "")
        date_value = _parse_date_or_none(compact) if re.fullmatch(r"\\d{8}", compact) else None
        note = _safe_str(row.get("note") or row.get("date_note") or row.get("dateNote"))
        item_order = _to_int_or_none(
            row.get("item_order") or row.get("sequence_number") or row.get("sequenceNumber")
        ) or index + 1

        if not (date_role or date_text or note):
            continue

        # The card can receive the same logical date through both the
        # normalized publishing_dates array and a compatibility publication
        # date mirror.  Enforce one logical row per role/date before INSERT so
        # the database uniqueness constraint is never hit by duplicate payload
        # rows.
        dedupe_key = (
            date_role,
            date_value.isoformat() if date_value is not None else "",
            date_text if date_value is None else "",
        )
        if dedupe_key in seen_dates:
            continue
        seen_dates.add(dedupe_key)

        cur.execute(
            """
            INSERT INTO edition_publishing_dates (
                tenant_id, edition_id, date_role, date_value, date_text,
                date_format, note, item_order
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                tenant_id, edition_uuid, date_role, date_value, date_text,
                date_format, note, item_order,
            ),
        )

        if date_role == "01" and date_value is not None:
            publication_date = date_value

    return {
        "ok": True,
        "work_id": work_id,
        "edition_id": edition_uuid,
        "publication_date": publication_date.isoformat() if publication_date else "",
    }

def _payload_first_present(row: Dict[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in row and row.get(key) is not None:
            return row.get(key)
    return None


def update_edition_product_details(
    cur,
    tenant_id: str,
    work_id: str,
    edition_id: str,
    payload: Dict[str, Any],
) -> Dict[str, Any]:
    """Persist edition-scoped Product Details and ONIX repeat data."""

    try:
        edition_uuid = str(uuid.UUID(str(edition_id)))
    except (ValueError, TypeError):
        raise ValueError("Invalid edition id")

    cur.execute(
        """
        SELECT id
        FROM editions
        WHERE tenant_id = %s
          AND work_id = %s
          AND id = %s
        LIMIT 1
        """,
        (tenant_id, work_id, edition_uuid),
    )
    if not cur.fetchone():
        raise ValueError("Edition not found")

    form_details = (
        payload.get("product_form_details")
        or payload.get("productFormDetails")
        or payload.get("form_details")
        or payload.get("formDetails")
        or []
    )
    if not isinstance(form_details, list):
        form_details = [form_details] if form_details else []
    form_details = [_safe_str(v) for v in form_details if _safe_str(v)]

    cur.execute(
        "DELETE FROM edition_form_details WHERE tenant_id = %s AND edition_id = %s",
        (tenant_id, edition_uuid),
    )
    for index, code in enumerate(form_details):
        cur.execute(
            """
            INSERT INTO edition_form_details
                (tenant_id, edition_id, form_detail_code, item_order)
            VALUES (%s, %s, %s, %s)
            """,
            (tenant_id, edition_uuid, code, index + 1),
        )

    content_types = (
        payload.get("product_content_types")
        or payload.get("productContentTypes")
        or payload.get("content_types")
        or payload.get("contentTypes")
        or []
    )
    if not isinstance(content_types, list):
        content_types = [content_types] if content_types else []
    content_types = [_safe_str(v) for v in content_types if _safe_str(v)]

    cur.execute(
        "DELETE FROM edition_content_types WHERE tenant_id = %s AND edition_id = %s",
        (tenant_id, edition_uuid),
    )
    for index, code in enumerate(content_types):
        cur.execute(
            """
            INSERT INTO edition_content_types
                (tenant_id, edition_id, content_type_code, is_primary, item_order)
            VALUES (%s, %s, %s, %s, %s)
            """,
            (tenant_id, edition_uuid, code, index == 0, index + 1),
        )

    product_form_features = (
        payload.get("product_form_features")
        or payload.get("productFormFeatures")
        or []
    )
    if not isinstance(product_form_features, list):
        product_form_features = []

    cur.execute(
        """
        DELETE FROM edition_product_form_features
        WHERE tenant_id = %s AND edition_id = %s
        """,
        (tenant_id, edition_uuid),
    )

    for index, row in enumerate(product_form_features):
        if not isinstance(row, dict):
            continue

        feature_type = _safe_str(
            row.get("feature_type")
            or row.get("featureType")
            or row.get("product_form_feature_type")
            or row.get("productFormFeatureType")
        )
        feature_value = _safe_str(
            row.get("feature_value")
            or row.get("featureValue")
            or row.get("product_form_feature_value")
            or row.get("productFormFeatureValue")
        )
        feature_description = _safe_str(
            row.get("feature_description")
            or row.get("featureDescription")
            or row.get("product_form_feature_description")
            or row.get("productFormFeatureDescription")
        )

        if not feature_type:
            continue

        cur.execute(
            """
            INSERT INTO edition_product_form_features (
                tenant_id,
                edition_id,
                feature_type,
                feature_value,
                feature_description,
                item_order
            )
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (
                tenant_id,
                edition_uuid,
                feature_type,
                feature_value,
                feature_description,
                index + 1,
            ),
        )

    ancillary_content = (
        payload.get("ancillary_content")
        or payload.get("ancillaryContent")
        or []
    )
    if not isinstance(ancillary_content, list):
        ancillary_content = []

    cur.execute(
        """
        DELETE FROM edition_ancillary_content
        WHERE tenant_id = %s AND edition_id = %s
        """,
        (tenant_id, edition_uuid),
    )

    for index, row in enumerate(ancillary_content):
        if not isinstance(row, dict):
            continue

        content_type = _safe_str(
            row.get("ancillary_content_type")
            or row.get("ancillaryContentType")
            or row.get("content_type")
            or row.get("contentType")
        )
        description = _safe_str(
            row.get("description") or row.get("ancillary_content_description") or row.get("ancillaryContentDescription")
        )
        description_text_format = _safe_str(row.get("description_text_format") or row.get("descriptionTextFormat")) or "05"
        number = _to_int_or_none(
            row.get("number")
            or row.get("ancillary_content_number")
            or row.get("ancillaryContentNumber")
        )

        if not content_type:
            continue

        cur.execute(
            """
            INSERT INTO edition_ancillary_content (
                tenant_id,
                edition_id,
                ancillary_content_type,
                description,
                description_text_format,
                number,
                item_order
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            (
                tenant_id,
                edition_uuid,
                content_type,
                description,
                description_text_format,
                number,
                index + 1,
            ),
        )

    measurements = (
        payload.get("measurements")
        or payload.get("product_measurements")
        or payload.get("productMeasurements")
        or []
    )
    if not isinstance(measurements, list):
        measurements = []

    cur.execute(
        "DELETE FROM edition_measurements WHERE tenant_id = %s AND edition_id = %s",
        (tenant_id, edition_uuid),
    )

    for index, row in enumerate(measurements):
        if not isinstance(row, dict):
            continue

        measure_type = _safe_str(
            row.get("measure_type")
            or row.get("measureType")
            or row.get("measurement_type")
            or row.get("measurementType")
            or row.get("type")
        )
        measurement = _to_float_or_none(
            _payload_first_present(
                row,
                "measurement",
                "measurement_value",
                "measurementValue",
                "value",
            )
        )
        unit_code = _safe_str(
            row.get("measure_unit_code")
            or row.get("measureUnitCode")
            or row.get("unit_code")
            or row.get("unitCode")
            or row.get("unit")
        )

        if not measure_type:
            continue

        cur.execute(
            """
            INSERT INTO edition_measurements
                (tenant_id, edition_id, measure_type, measurement, measure_unit_code, item_order)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (tenant_id, edition_uuid, measure_type, measurement, unit_code, index + 1),
        )

    extents = (
        payload.get("extents")
        or payload.get("product_extents")
        or payload.get("productExtents")
        or []
    )
    if not isinstance(extents, list):
        extents = []

    cur.execute(
        "DELETE FROM edition_extents WHERE tenant_id = %s AND edition_id = %s",
        (tenant_id, edition_uuid),
    )

    page_count = None
    duration = None
    duration_unit = ""
    file_size = None
    file_size_unit = ""

    for index, row in enumerate(extents):
        if not isinstance(row, dict):
            continue

        extent_type = _safe_str(
            row.get("extent_type")
            or row.get("extentType")
            or row.get("type")
        )
        extent_value = _to_float_or_none(
            _payload_first_present(
                row,
                "extent_value",
                "extentValue",
                "value",
            )
        )
        extent_unit = _safe_str(
            row.get("extent_unit")
            or row.get("extentUnit")
            or row.get("unit_code")
            or row.get("unitCode")
            or row.get("unit")
        )

        if not extent_type:
            continue

        cur.execute(
            """
            INSERT INTO edition_extents
                (tenant_id, edition_id, extent_type, extent_value, extent_unit, item_order)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (tenant_id, edition_uuid, extent_type, extent_value, extent_unit, index + 1),
        )

        if extent_type == "00" and extent_value is not None:
            page_count = int(extent_value)
        elif page_count is None and extent_type in {"02", "11"} and extent_unit == "03" and extent_value is not None:
            page_count = int(extent_value)
        elif extent_type == "09":
            duration, duration_unit = extent_value, extent_unit
        elif extent_type == "22":
            file_size, file_size_unit = extent_value, extent_unit

    product_form = _safe_str(
        payload.get("onix_product_form")
        or payload.get("onixProductForm")
        or payload.get("product_form")
        or payload.get("productForm")
        or payload.get("format_code")
        or payload.get("formatCode")
    )

    edition_number = _safe_str(payload.get("edition_number") or payload.get("editionNumber"))
    edition_statement = _safe_str(payload.get("edition_statement") or payload.get("editionStatement"))
    illustration_count = _to_int_or_none(
        payload.get("illustration_count")
        or payload.get("illustrationCount")
        or payload.get("number_of_illustrations")
        or payload.get("numberOfIllustrations")
    )
    illustration_note = _safe_str(
        payload.get("illustration_note")
        or payload.get("illustrationNote")
        or payload.get("illustrations_note")
        or payload.get("illustrationsNote")
    )
    color_content = _safe_str(
        payload.get("color_content")
        or payload.get("colorContent")
        or payload.get("colour_content")
        or payload.get("colourContent")
    )
    color_pages = _to_int_or_none(
        payload.get("color_pages")
        or payload.get("colorPages")
        or payload.get("colour_pages")
        or payload.get("colourPages")
    )
    number_of_pieces = _to_int_or_none(payload.get("number_of_pieces") or payload.get("numberOfPieces"))
    trade_category = _safe_str(payload.get("trade_category") or payload.get("tradeCategory"))
    country_of_manufacture = _safe_str(
        payload.get("country_of_manufacture")
        or payload.get("countryOfManufacture")
    )
    product_form_description = _safe_str(
        payload.get("product_form_description")
        or payload.get("productFormDescription")
    )
    technical_protection = _safe_str(
        payload.get("technical_protection")
        or payload.get("technicalProtection")
        or payload.get("drm_type")
        or payload.get("drmType")
    )
    epub_version = _safe_str(payload.get("epub_version") or payload.get("epubVersion"))
    file_format = _safe_str(payload.get("file_format") or payload.get("fileFormat"))
    product_details_note = _safe_str(
        payload.get("product_details_note")
        or payload.get("productDetailsNote")
        or payload.get("physical_description")
        or payload.get("physicalDescription")
    )

    cur.execute(
        """
        UPDATE editions
        SET
            onix_product_form = %s,
            onix_product_form_detail = %s,
            primary_content_type = %s,
            edition_number = %s,
            edition_statement = %s,
            duration = %s,
            duration_unit = %s,
            file_size = %s,
            file_size_unit = %s,
            illustrations_number = %s,
            illustrations_desc = %s,
            color_content = %s,
            color_pages = %s,
            number_of_pieces = %s,
            trade_category = %s,
            country_of_manufacture = %s,
            product_form_description = %s,
            technical_protection = %s,
            epub_version = %s,
            file_format = %s,
            product_details_note = %s,
            updated_at = now()
        WHERE tenant_id = %s
          AND work_id = %s
          AND id = %s
        """,
        (
            product_form,
            form_details[0] if form_details else "",
            content_types[0] if content_types else "",
            edition_number,
            edition_statement,
            duration,
            duration_unit,
            file_size,
            file_size_unit,
            illustration_count,
            illustration_note,
            color_content,
            color_pages,
            number_of_pieces,
            trade_category,
            country_of_manufacture,
            product_form_description,
            technical_protection,
            epub_version,
            file_format,
            product_details_note,
            tenant_id,
            work_id,
            edition_uuid,
        ),
    )

    return {"ok": True, "work_id": work_id, "edition_id": edition_uuid}



def update_edition_supply_pricing(
    cur,
    tenant_id: str,
    work_id: str,
    edition_id: str,
    payload: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Save Supply & Pricing using the EXISTING
    edition_supply_details -> edition_prices structure.

    Important:
    - Existing database rows are updated in place when their UUID is supplied.
    - Existing price IDs are preserved.
    - New prices are inserted into edition_prices.
    - Removed UI prices are deleted from edition_prices.
    - No parallel pricing table is created or used.
    - Newer ONIX fields are written only when the column exists.
    """

    try:
        edition_uuid = str(uuid.UUID(str(edition_id)))
    except (ValueError, TypeError):
        raise ValueError("Invalid edition id")

    cur.execute(
        """
        SELECT id
        FROM editions
        WHERE tenant_id = %s
          AND work_id = %s
          AND id = %s
        LIMIT 1
        """,
        (tenant_id, work_id, edition_uuid),
    )
    if not cur.fetchone():
        raise ValueError("Edition not found")

    # ProductSupply / MarketPublishingDetail lives at edition level.
    market_publishing_status = _safe_str(
        payload.get("market_publishing_status")
        or payload.get("marketPublishingStatus")
    )
    market_date_role = _safe_str(
        payload.get("market_date_role")
        or payload.get("marketDateRole")
    )
    market_date_format = _safe_str(
        payload.get("market_date_format")
        or payload.get("marketDateFormat")
    ) or "00"
    market_date_text = _safe_str(
        payload.get("market_date_text")
        or payload.get("marketDateText")
        or payload.get("market_date")
        or payload.get("marketDate")
    ).replace("-", "")
    promotion_contact = _safe_str(
        payload.get("promotion_contact")
        or payload.get("promotionContact")
    )
    promotion_contact_text_format = _safe_str(
        payload.get("promotion_contact_text_format")
        or payload.get("promotionContactTextFormat")
    ) or "05"
    initial_print_run = _safe_str(payload.get("initial_print_run") or payload.get("initialPrintRun"))
    initial_print_run_text_format = _safe_str(payload.get("initial_print_run_text_format") or payload.get("initialPrintRunTextFormat")) or "05"
    promotion_campaign = _safe_str(payload.get("promotion_campaign") or payload.get("promotionCampaign"))
    promotion_campaign_text_format = _safe_str(payload.get("promotion_campaign_text_format") or payload.get("promotionCampaignTextFormat")) or "05"

    edition_columns = _table_column_names(cur, "editions")
    market_values = {
        "market_publishing_status": market_publishing_status,
        "market_date_role": market_date_role,
        "market_date_format": market_date_format,
        "market_date_text": market_date_text,
        "promotion_contact": promotion_contact,
        "promotion_contact_text_format": promotion_contact_text_format,
        "initial_print_run": initial_print_run, "initial_print_run_text_format": initial_print_run_text_format,
        "promotion_campaign": promotion_campaign, "promotion_campaign_text_format": promotion_campaign_text_format,
    }
    usable_market_values = {
        key: value
        for key, value in market_values.items()
        if key in edition_columns
    }
    if usable_market_values:
        assignments = ", ".join(
            f"{column} = %s"
            for column in usable_market_values.keys()
        )
        cur.execute(
            f"""
            UPDATE editions
            SET {assignments},
                updated_at = now()
            WHERE tenant_id = %s
              AND work_id = %s
              AND id = %s
            """,
            tuple(usable_market_values.values())
            + (tenant_id, work_id, edition_uuid),
        )

    # Canonical ProductSupply/Market composites.
    market_columns = _table_column_names(cur, "edition_markets")
    if market_columns:
        cur.execute(
            "DELETE FROM edition_markets WHERE tenant_id = %s AND edition_id = %s",
            (tenant_id, edition_uuid),
        )
        markets = payload.get("markets") or []
        if isinstance(markets, list):
            for market_index, market in enumerate(markets, start=1):
                if not isinstance(market, dict):
                    continue
                values = {
                    "tenant_id": tenant_id,
                    "edition_id": edition_uuid,
                    "countries_included": _safe_str(market.get("countries_included") or market.get("countriesIncluded")),
                    "regions_included": _safe_str(market.get("regions_included") or market.get("regionsIncluded")),
                    "countries_excluded": _safe_str(market.get("countries_excluded") or market.get("countriesExcluded")),
                    "regions_excluded": _safe_str(market.get("regions_excluded") or market.get("regionsExcluded")),
                    "item_order": market_index,
                }
                usable = {k:v for k,v in values.items() if k in market_columns}
                if not any(_safe_str(usable.get(k)) for k in ("countries_included","regions_included","countries_excluded","regions_excluded")):
                    continue
                cols = list(usable.keys())
                cur.execute(
                    f"INSERT INTO edition_markets ({', '.join(cols)}) VALUES ({', '.join(['%s']*len(cols))})",
                    tuple(usable[c] for c in cols),
                )

    product_contacts = payload.get("product_contacts") or payload.get("productContacts") or []
    if not isinstance(product_contacts, list):
        product_contacts = []
    cur.execute(
        "DELETE FROM edition_product_contacts WHERE tenant_id = %s AND edition_id = %s",
        (tenant_id, edition_uuid),
    )
    for index, row in enumerate(product_contacts):
        if not isinstance(row, dict):
            continue
        role = _safe_str(row.get("product_contact_role") or row.get("productContactRole") or row.get("role"))
        product_name = _safe_str(row.get("product_contact_name") or row.get("productContactName"))
        contact_name = _safe_str(row.get("contact_name") or row.get("contactName"))
        email = _safe_str(row.get("email_address") or row.get("emailAddress") or row.get("email"))
        if not (role or product_name or contact_name or email):
            continue
        cur.execute(
            """
            INSERT INTO edition_product_contacts (
                tenant_id, edition_id, product_contact_role,
                product_contact_name, contact_name, email_address, item_order
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            (tenant_id, edition_uuid, role, product_name, contact_name, email, index + 1),
        )

    supply_columns = _table_column_names(
        cur,
        "edition_supply_details",
    )
    price_columns = _table_column_names(
        cur,
        "edition_prices",
    )
    supplier_identifier_columns = _table_column_names(
        cur,
        "edition_supplier_identifiers",
    )
    supply_date_columns = _table_column_names(cur, "edition_supply_dates")

    if not supply_columns:
        raise ValueError(
            "edition_supply_details table is missing"
        )
    if not price_columns:
        raise ValueError(
            "edition_prices table is missing"
        )

    def _uuid_or_blank(value: Any) -> str:
        raw = _safe_str(value)
        if not raw:
            return ""
        try:
            return str(uuid.UUID(raw))
        except (ValueError, TypeError):
            return ""

    def _update_row(
        table: str,
        row_id: str,
        values: Dict[str, Any],
    ) -> None:
        usable = {
            key: value
            for key, value in values.items()
            if key in (
                supply_columns
                if table == "edition_supply_details"
                else price_columns
            )
        }
        if (
            "updated_at"
            in (
                supply_columns
                if table == "edition_supply_details"
                else price_columns
            )
        ):
            usable["updated_at"] = datetime.now(
                timezone.utc
            )

        if not usable:
            return

        assignments = ", ".join(
            f"{column} = %s"
            for column in usable.keys()
        )
        params = list(usable.values()) + [
            tenant_id,
            row_id,
        ]

        cur.execute(
            f"""
            UPDATE {table}
            SET {assignments}
            WHERE tenant_id = %s
              AND id = %s
            """,
            tuple(params),
        )

    def _insert_row(
        table: str,
        values: Dict[str, Any],
    ) -> str:
        columns_for_table = (
            supply_columns
            if table == "edition_supply_details"
            else price_columns
        )

        usable = {
            key: value
            for key, value in values.items()
            if key in columns_for_table
        }

        if not usable:
            return ""

        columns = list(usable.keys())
        placeholders = ", ".join(
            ["%s"] * len(columns)
        )

        cur.execute(
            f"""
            INSERT INTO {table} (
                {", ".join(columns)}
            )
            VALUES ({placeholders})
            RETURNING id
            """,
            tuple(usable[column] for column in columns),
        )
        row = cur.fetchone()
        return str(row.get("id") or "") if row else ""

    supplies = (
        payload.get("supply_details")
        or payload.get("supplyDetails")
        or payload.get("supplies")
        or []
    )
    if not isinstance(supplies, list):
        supplies = []

    # Existing supply records for this edition.
    cur.execute(
        """
        SELECT id
        FROM edition_supply_details
        WHERE tenant_id = %s
          AND edition_id = %s
        """,
        (tenant_id, edition_uuid),
    )
    existing_supply_ids = {
        str(row["id"])
        for row in (cur.fetchall() or [])
    }

    kept_supply_ids: set[str] = set()

    for supply_index, supply in enumerate(
        supplies,
        start=1,
    ):
        if not isinstance(supply, dict):
            continue

        supplier = supply.get("supplier")
        if not isinstance(supplier, dict):
            supplier = {}

        supplier_name = _safe_str(
            supply.get("supplier_name")
            or supply.get("supplierName")
            or supplier.get("supplier_name")
            or supplier.get("supplierName")
            or supplier.get("name")
        )
        supplier_role = _safe_str(
            supply.get("supplier_role")
            or supply.get("supplierRole")
            or supplier.get("supplier_role")
            or supplier.get("supplierRole")
        )
        supplier_email = _safe_str(
            supply.get("supplier_email")
            or supply.get("supplierEmail")
            or supplier.get("email_address")
            or supplier.get("emailAddress")
            or supplier.get("email")
        )
        supplier_telephone = _safe_str(supply.get("supplier_telephone") or supply.get("supplierTelephone") or supplier.get("telephone_number") or supplier.get("telephoneNumber"))
        supplier_fax = _safe_str(supply.get("supplier_fax") or supply.get("supplierFax") or supplier.get("fax_number") or supplier.get("faxNumber"))
        availability = _safe_str(
            supply.get("product_availability")
            or supply.get("productAvailability")
            or supply.get("availability_code")
            or supply.get("availabilityCode")
            or supply.get("availability")
        )

        order_raw = (
            supply.get("order_time_days")
            if supply.get("order_time_days")
            is not None
            else supply.get("orderTimeDays")
        )
        order_time_days = _to_int_or_none(
            order_raw
        )

        returns_code_type = _safe_str(
            supply.get("returns_code_type")
            or supply.get("returnsCodeType")
        )
        returns_code = _safe_str(
            supply.get("returns_code")
            or supply.get("returnsCode")
            or supply.get("returns_type")
            or supply.get("returnsType")
        )
        returns_note = _safe_str(
            supply.get("returns_note")
            or supply.get("returnsNote")
        )

        pack_raw = (
            supply.get("pack_quantity")
            if supply.get("pack_quantity")
            is not None
            else supply.get("packQuantity")
        )
        pack_quantity = _to_int_or_none(
            pack_raw
        )

        carton_raw = (
            supply.get("carton_quantity")
            if supply.get("carton_quantity")
            is not None
            else supply.get("cartonQuantity")
        )
        carton_quantity = _to_int_or_none(
            carton_raw
        )

        stock_raw = (
            supply.get("stock_quantity")
            if supply.get("stock_quantity")
            is not None
            else (
                supply.get("stockQuantity")
                if supply.get("stockQuantity")
                is not None
                else supply.get(
                    "on_hand_quantity"
                )
            )
        )
        stock_quantity = _to_int_or_none(
            stock_raw
        )

        expected_ship_date = _parse_date_or_none(
            supply.get("expected_ship_date")
            or supply.get("expectedShipDate")
            or supply.get(
                "expected_availability_date"
            )
            or supply.get(
                "expectedAvailabilityDate"
            )
        )

        supply_note = _safe_str(
            supply.get("supply_note")
            or supply.get("supplyNote")
            or supply.get("note")
        )

        prices = (
            supply.get("prices")
            or supply.get("product_prices")
            or supply.get("productPrices")
            or []
        )
        if not isinstance(prices, list):
            prices = []

        identifiers = (
            supply.get("supplier_identifiers")
            or supply.get("supplierIdentifiers")
            or supplier.get(
                "supplier_identifiers"
            )
            or supplier.get(
                "supplierIdentifiers"
            )
            or []
        )
        if not isinstance(identifiers, list):
            identifiers = []

        if not (
            supplier_name
            or supplier_role
            or availability
            or returns_code
            or supply_note
            or prices
            or identifiers
        ):
            continue

        supply_values = {
            "tenant_id": tenant_id,
            "edition_id": edition_uuid,
            "supplier_name": supplier_name,
            "supplier_role": supplier_role,
            "supplier_email": supplier_email,
            "supplier_telephone": supplier_telephone, "supplier_fax": supplier_fax,
            "pack_quantity": pack_quantity,
            "product_availability": availability,
            "returns_code_type": returns_code_type,
            "returns_code": returns_code,
            "returns_note": returns_note,
            "stock_on_hand": stock_quantity,
            "order_time_days": order_time_days,
            "carton_quantity": carton_quantity,
            "expected_ship_date":
                expected_ship_date,
            "supply_note": supply_note,
        }

        incoming_supply_id = _uuid_or_blank(
            supply.get("id")
        )

        if (
            incoming_supply_id
            and incoming_supply_id
            in existing_supply_ids
        ):
            supply_detail_id = incoming_supply_id
            _update_row(
                "edition_supply_details",
                supply_detail_id,
                supply_values,
            )
        else:
            supply_detail_id = _insert_row(
                "edition_supply_details",
                supply_values,
            )

        if not supply_detail_id:
            continue

        kept_supply_ids.add(
            supply_detail_id
        )

        # Canonical repeatable SupplyDate composites.
        if supply_date_columns:
            cur.execute(
                "DELETE FROM edition_supply_dates WHERE tenant_id = %s AND supply_detail_id = %s",
                (tenant_id, supply_detail_id),
            )
            supply_dates = supply.get("supply_dates") or supply.get("supplyDates") or []
            if isinstance(supply_dates, list):
                for date_index, date_row in enumerate(supply_dates, start=1):
                    if not isinstance(date_row, dict):
                        continue
                    date_role = _safe_str(date_row.get("date_role") or date_row.get("dateRole") or date_row.get("supply_date_role") or date_row.get("supplyDateRole"))
                    date_format = _safe_str(date_row.get("date_format") or date_row.get("dateFormat")) or "00"
                    date_text = _safe_str(date_row.get("date_text") or date_row.get("dateText") or date_row.get("date"))
                    if not (date_role or date_text):
                        continue
                    date_value = _parse_date_or_none(date_text)
                    values = {
                        "tenant_id": tenant_id,
                        "supply_detail_id": supply_detail_id,
                        "date_role": date_role,
                        "date_format": date_format,
                        "date_text": date_text,
                        "date_value": date_value,
                        "item_order": date_index,
                    }
                    usable = {k:v for k,v in values.items() if k in supply_date_columns}
                    cols = list(usable.keys())
                    cur.execute(
                        f"INSERT INTO edition_supply_dates ({', '.join(cols)}) VALUES ({', '.join(['%s']*len(cols))})",
                        tuple(usable[c] for c in cols),
                    )

        # Keep useful scalar mirrors populated without losing the canonical rows.
        supply_dates = supply.get("supply_dates") or supply.get("supplyDates") or []
        if isinstance(supply_dates, list) and supply_dates:
            on_sale = next((_parse_date_or_none(d.get("date_text") or d.get("dateText") or d.get("date")) for d in supply_dates if isinstance(d, dict) and _safe_str(d.get("date_role") or d.get("dateRole")) == "02"), None)
            expected = next((_parse_date_or_none(d.get("date_text") or d.get("dateText") or d.get("date")) for d in supply_dates if isinstance(d, dict) and _safe_str(d.get("date_role") or d.get("dateRole")) == "08"), None)
            mirror_values = {}
            if on_sale is not None and "on_sale_date" in supply_columns:
                mirror_values["on_sale_date"] = on_sale
            if expected is not None and "expected_ship_date" in supply_columns:
                mirror_values["expected_ship_date"] = expected
            if mirror_values:
                _update_row("edition_supply_details", supply_detail_id, mirror_values)

        # ----------------------------------------
        # Existing price rows stay in edition_prices
        # ----------------------------------------
        cur.execute(
            """
            SELECT id
            FROM edition_prices
            WHERE tenant_id = %s
              AND supply_detail_id = %s
            """,
            (tenant_id, supply_detail_id),
        )
        existing_price_ids = {
            str(row["id"])
            for row in (cur.fetchall() or [])
        }
        kept_price_ids: set[str] = set()

        for price_index, price in enumerate(
            prices,
            start=1,
        ):
            if not isinstance(price, dict):
                continue

            price_type = _safe_str(
                price.get("price_type")
                or price.get("priceType")
                or price.get("type")
            )

            price_amount_raw = (
                price.get("price_amount")
                if price.get("price_amount")
                is not None
                else (
                    price.get("priceAmount")
                    if price.get("priceAmount")
                    is not None
                    else price.get("amount")
                )
            )
            price_amount = _to_float_or_none(
                price_amount_raw
            )

            currency = _safe_str(
                price.get("currency_code")
                or price.get("currencyCode")
                or price.get("currency")
            )

            # The UI calls this "Tax Type".
            # Existing InkSuite / ONIX schema stores
            # the code in edition_prices.tax_rate_code.
            tax_rate_code = _safe_str(
                price.get("tax_rate_code")
                or price.get("taxRateCode")
                or price.get("tax_type")
                or price.get("taxType")
                or price.get("tax")
            ).upper()

            # ONIX List 62 TaxRateCode values are letters, not numeric
            # "tax included / excluded" flags. Clear legacy invalid values.
            if tax_rate_code not in {"H", "P", "R", "S", "T", "Z"}:
                tax_rate_code = ""

            tax_rate_percent = _to_float_or_none(
                price.get("tax_rate_percent")
                if price.get("tax_rate_percent") is not None
                else price.get("taxRatePercent")
            )
            taxable_amount = _to_float_or_none(
                price.get("taxable_amount")
                if price.get("taxable_amount") is not None
                else price.get("taxableAmount")
            )
            tax_amount = _to_float_or_none(
                price.get("tax_amount")
                if price.get("tax_amount") is not None
                else price.get("taxAmount")
            )

            # Tax detail belongs only to tax-inclusive ONIX List 58 price types.
            tax_included_price_types = {
                "02", "04", "07", "09", "12", "14", "17", "22", "24", "27", "42"
            }
            if price_type not in tax_included_price_types:
                tax_rate_code = ""
                tax_rate_percent = None
                taxable_amount = None
                tax_amount = None

            country = _safe_str(
                price.get("country_code")
                or price.get("countryCode")
                or price.get("country")
            )

            territory = _safe_str(
                price.get("territory")
                or price.get("region_code")
                or price.get("regionCode")
            )

            price_status = _safe_str(
                price.get("price_status")
                or price.get("priceStatus")
                or price.get("status")
            )

            effective_from = _parse_date_or_none(
                price.get(
                    "price_effective_from"
                )
                or price.get(
                    "priceEffectiveFrom"
                )
                or price.get(
                    "effective_from"
                )
                or price.get(
                    "effectiveFrom"
                )
            )

            effective_until = _parse_date_or_none(
                price.get(
                    "price_effective_until"
                )
                or price.get(
                    "priceEffectiveUntil"
                )
                or price.get(
                    "effective_until"
                )
                or price.get(
                    "effectiveUntil"
                )
            )

            discount_code = _safe_str(
                price.get("discount_code")
                or price.get("discountCode")
            )

            minimum_order_raw = (
                price.get(
                    "minimum_order_quantity"
                )
                if price.get(
                    "minimum_order_quantity"
                )
                is not None
                else price.get(
                    "minimumOrderQuantity"
                )
            )
            minimum_order_quantity = (
                _to_int_or_none(
                    minimum_order_raw
                )
            )

            price_note = _safe_str(
                price.get("price_note")
                or price.get("priceNote")
                or price.get("note")
            )

            if not (
                price_type
                or price_amount is not None
                or currency
                or country
                or territory
                or price_note
            ):
                continue

            price_values = {
                "tenant_id": tenant_id,
                "supply_detail_id":
                    supply_detail_id,
                "price_type_code":
                    price_type,
                "price_amount":
                    price_amount,
                "currency_code":
                    currency,
                "discount_code":
                    discount_code,
                "tax_rate_code":
                    tax_rate_code,
                "tax_rate_percent":
                    tax_rate_percent,
                "taxable_amount":
                    taxable_amount,
                "tax_amount":
                    tax_amount,
                "territory_country_included":
                    country,
                "territory_region_included":
                    territory,
                "price_status":
                    price_status,
                "price_effective_from":
                    effective_from,
                "price_effective_until":
                    effective_until,
                "minimum_order_quantity":
                    minimum_order_quantity,
                "price_note":
                    price_note,
                "item_order":
                    price_index,
            }

            incoming_price_id = _uuid_or_blank(
                price.get("id")
            )

            if (
                incoming_price_id
                and incoming_price_id
                in existing_price_ids
            ):
                price_id = incoming_price_id
                _update_row(
                    "edition_prices",
                    price_id,
                    price_values,
                )
            else:
                price_id = _insert_row(
                    "edition_prices",
                    price_values,
                )

            if price_id:
                kept_price_ids.add(price_id)

        # Delete prices removed from this supplier in the UI.
        price_ids_to_delete = (
            existing_price_ids
            - kept_price_ids
        )
        if price_ids_to_delete:
            cur.execute(
                """
                DELETE FROM edition_prices
                WHERE tenant_id = %s
                  AND supply_detail_id = %s
                  AND id = ANY(%s::uuid[])
                """,
                (
                    tenant_id,
                    supply_detail_id,
                    list(price_ids_to_delete),
                ),
            )

        # Supplier identifiers are optional and only
        # persisted if this table already exists.
        if supplier_identifier_columns:
            cur.execute(
                """
                DELETE FROM edition_supplier_identifiers
                WHERE tenant_id = %s
                  AND supply_detail_id = %s
                """,
                (
                    tenant_id,
                    supply_detail_id,
                ),
            )

            for identifier_index, identifier in enumerate(
                identifiers,
                start=1,
            ):
                if not isinstance(
                    identifier,
                    dict,
                ):
                    continue

                id_type = _safe_str(
                    identifier.get(
                        "supplier_id_type"
                    )
                    or identifier.get(
                        "supplierIdType"
                    )
                    or identifier.get(
                        "identifier_type"
                    )
                    or identifier.get(
                        "identifierType"
                    )
                    or identifier.get("type")
                )
                id_type_name = _safe_str(
                    identifier.get(
                        "supplier_id_type_name"
                    )
                    or identifier.get(
                        "supplierIdTypeName"
                    )
                    or identifier.get(
                        "id_type_name"
                    )
                    or identifier.get(
                        "idTypeName"
                    )
                    or identifier.get(
                        "identifier_type_name"
                    )
                    or identifier.get(
                        "identifierTypeName"
                    )
                    or identifier.get("type_name")
                    or identifier.get("typeName")
                )
                # ONIX SupplierIDType 01 is proprietary and requires
                # the name of the proprietary identifier scheme.
                # Standard schemes must not carry a stale IDTypeName.
                if id_type != "01":
                    id_type_name = ""

                id_value = _safe_str(
                    identifier.get("id_value")
                    or identifier.get("idValue")
                    or identifier.get(
                        "identifier_value"
                    )
                    or identifier.get(
                        "identifierValue"
                    )
                    or identifier.get("value")
                )

                if not (
                    id_type
                    or id_value
                ):
                    continue

                values = {
                    "tenant_id": tenant_id,
                    "supply_detail_id":
                        supply_detail_id,
                    "supplier_id_type":
                        id_type,
                    "supplier_id_type_name":
                        id_type_name,
                    "id_value":
                        id_value,
                    "item_order":
                        identifier_index,
                }

                usable = {
                    key: value
                    for key, value
                    in values.items()
                    if key
                    in supplier_identifier_columns
                }

                if not usable:
                    continue

                columns = list(
                    usable.keys()
                )
                placeholders = ", ".join(
                    ["%s"] * len(columns)
                )
                cur.execute(
                    f"""
                    INSERT INTO edition_supplier_identifiers (
                        {", ".join(columns)}
                    )
                    VALUES ({placeholders})
                    """,
                    tuple(
                        usable[column]
                        for column in columns
                    ),
                )

    # Delete suppliers removed from the UI.
    supply_ids_to_delete = (
        existing_supply_ids
        - kept_supply_ids
    )

    for supply_id in supply_ids_to_delete:
        if supply_date_columns:
            cur.execute(
                "DELETE FROM edition_supply_dates WHERE tenant_id = %s AND supply_detail_id = %s",
                (tenant_id, supply_id),
            )
        # Be explicit: delete price children first.
        cur.execute(
            """
            DELETE FROM edition_prices
            WHERE tenant_id = %s
              AND supply_detail_id = %s
            """,
            (tenant_id, supply_id),
        )

        if supplier_identifier_columns:
            cur.execute(
                """
                DELETE FROM edition_supplier_identifiers
                WHERE tenant_id = %s
                  AND supply_detail_id = %s
                """,
                (tenant_id, supply_id),
            )

        cur.execute(
            """
            DELETE FROM edition_supply_details
            WHERE tenant_id = %s
              AND edition_id = %s
              AND id = %s
            """,
            (
                tenant_id,
                edition_uuid,
                supply_id,
            ),
        )

    return {
        "ok": True,
        "work_id": work_id,
        "edition_id": edition_uuid,
    }






def upsert_bookdev_task_assignment(
    cur,
    tenant_id: str,
    work_id: str,
    edition_id: str,
    payload: Dict[str, Any],
) -> Dict[str, Any]:
    """Persist one Project Management task assignment for an edition."""

    try:
        edition_uuid = str(uuid.UUID(str(edition_id)))
    except (ValueError, TypeError):
        raise ValueError("Invalid edition id")

    task_key = _safe_str(
        payload.get("task_key")
        or payload.get("taskKey")
    )
    responsible_person = _safe_str(
        payload.get("responsible_person")
        or payload.get("responsiblePerson")
    )
    deadline_raw = _safe_str(payload.get("deadline"))
    deadline = _parse_date_or_none(deadline_raw) if deadline_raw else None

    if not task_key:
        raise ValueError("task_key is required")

    cur.execute(
        """
        SELECT 1
        FROM editions
        WHERE tenant_id = %s
          AND work_id = %s
          AND id = %s
        LIMIT 1
        """,
        (tenant_id, work_id, edition_uuid),
    )
    if not cur.fetchone():
        raise ValueError("Edition not found")

    cur.execute(
        """
        INSERT INTO bookdev_task_assignments (
            tenant_id,
            work_id,
            edition_id,
            task_key,
            responsible_person,
            deadline,
            updated_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, now())
        ON CONFLICT (
            tenant_id,
            work_id,
            edition_id,
            task_key
        )
        DO UPDATE SET
            responsible_person = EXCLUDED.responsible_person,
            deadline = EXCLUDED.deadline,
            updated_at = now()
        RETURNING
            id,
            task_key,
            responsible_person,
            deadline,
            created_at,
            updated_at
        """,
        (
            tenant_id,
            work_id,
            edition_uuid,
            task_key,
            responsible_person,
            deadline,
        ),
    )
    row = cur.fetchone() or {}

    return {
        "ok": True,
        "work_id": work_id,
        "edition_id": edition_uuid,
        "assignment": {
            "id": str(row.get("id") or ""),
            "task_key": _safe_str(row.get("task_key")),
            "taskKey": _safe_str(row.get("task_key")),
            "responsible_person": _safe_str(row.get("responsible_person")),
            "responsiblePerson": _safe_str(row.get("responsible_person")),
            "deadline": row.get("deadline").isoformat() if row.get("deadline") else "",
            "created_at": row.get("created_at").isoformat() if row.get("created_at") else "",
            "updated_at": row.get("updated_at").isoformat() if row.get("updated_at") else "",
        },
    }


def update_edition_cited_content(cur, tenant_id: str, work_id: str, edition_id: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    try: edition_uuid = str(uuid.UUID(str(edition_id)))
    except (ValueError, TypeError): raise ValueError("Invalid edition id")
    cur.execute("SELECT id FROM editions WHERE tenant_id=%s AND work_id=%s AND id=%s LIMIT 1", (tenant_id,work_id,edition_uuid))
    if not cur.fetchone(): raise ValueError("Edition not found")
    rows=payload.get("cited_content") or payload.get("citedContent") or []
    if not isinstance(rows,list): rows=[]
    cur.execute("DELETE FROM edition_cited_content WHERE tenant_id=%s AND edition_id=%s",(tenant_id,edition_uuid))
    inserted=0
    for index,row in enumerate(rows):
        if not isinstance(row,dict): continue
        vals={
          "type":_safe_str(row.get("cited_content_type") or row.get("citedContentType")),
          "aud":_safe_str(row.get("content_audience") or row.get("contentAudience")),
          "source_type":_safe_str(row.get("source_type") or row.get("sourceType")),
          "source_title":_safe_str(row.get("source_title") or row.get("sourceTitle")),
          "note":_safe_str(row.get("citation_note") or row.get("citationNote")),
          "fmt":_safe_str(row.get("citation_note_text_format") or row.get("citationNoteTextFormat")) or "05",
          "link":_safe_str(row.get("resource_link") or row.get("resourceLink")),
          "list":_safe_str(row.get("list_name") or row.get("listName")),
          "pos":_safe_str(row.get("position_on_list") or row.get("positionOnList")),
        }
        if not any(vals.values()): continue
        cur.execute("""INSERT INTO edition_cited_content (tenant_id,edition_id,cited_content_type,content_audience,source_type,source_title,citation_note,citation_note_text_format,resource_link,list_name,position_on_list,item_order) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING id""",(tenant_id,edition_uuid,vals["type"],vals["aud"],vals["source_type"],vals["source_title"],vals["note"],vals["fmt"],vals["link"],vals["list"],vals["pos"],index+1))
        cid=(cur.fetchone() or {}).get("id")
        dates=row.get("content_dates") or row.get("contentDates") or []
        if isinstance(dates,list):
            for di,d in enumerate(dates):
                if not isinstance(d,dict): continue
                role=_safe_str(d.get("content_date_role") or d.get("contentDateRole") or d.get("date_role") or d.get("dateRole")); fmt=_safe_str(d.get("date_format") or d.get("dateFormat")) or "00"; dt=_safe_str(d.get("date_text") or d.get("dateText") or d.get("date_value") or d.get("dateValue") or d.get("date"))
                if role or dt: cur.execute("INSERT INTO edition_cited_content_dates (tenant_id,edition_id,cited_content_id,content_date_role,date_format,date_text,item_order) VALUES (%s,%s,%s,%s,%s,%s,%s)",(tenant_id,edition_uuid,cid,role,fmt,dt,di+1))
        inserted+=1
    return {"ok":True,"work_id":work_id,"edition_id":edition_uuid,"cited_content_saved":inserted}

def update_edition_awards(
    cur,
    tenant_id: str,
    work_id: str,
    edition_id: str,
    payload: Dict[str, Any],
) -> Dict[str, Any]:
    """Persist normalized award/prize records for one edition."""

    try:
        edition_uuid = str(uuid.UUID(str(edition_id)))
    except (ValueError, TypeError):
        raise ValueError("Invalid edition id")

    cur.execute(
        """
        SELECT id
        FROM editions
        WHERE tenant_id = %s
          AND work_id = %s
          AND id = %s
        LIMIT 1
        """,
        (tenant_id, work_id, edition_uuid),
    )
    if not cur.fetchone():
        raise ValueError("Edition not found")

    rows = (
        payload.get("awards")
        or payload.get("prizes")
        or payload.get("award_records")
        or payload.get("awardRecords")
        or []
    )
    if not isinstance(rows, list):
        rows = []

    cur.execute(
        """
        DELETE FROM edition_prizes
        WHERE tenant_id = %s
          AND edition_id = %s
        """,
        (tenant_id, edition_uuid),
    )

    inserted = 0

    for index, row in enumerate(rows):
        if not isinstance(row, dict):
            continue

        prize_name = _safe_str(
            row.get("award_name")
            or row.get("awardName")
            or row.get("prize_name")
            or row.get("prizeName")
            or row.get("name")
        )

        prize_year = _safe_str(
            row.get("award_year")
            or row.get("awardYear")
            or row.get("prize_year")
            or row.get("prizeYear")
            or row.get("year")
        )

        prize_country = _safe_str(
            row.get("country_code")
            or row.get("countryCode")
            or row.get("prize_country")
            or row.get("prizeCountry")
            or row.get("country")
        )

        prize_code = _safe_str(
            row.get("award_code")
            or row.get("awardCode")
            or row.get("prize_code")
            or row.get("prizeCode")
            or row.get("code")
        )

        prize_jury = _safe_str(
            row.get("awarding_body")
            or row.get("awardingBody")
            or row.get("award_organization")
            or row.get("awardOrganization")
            or row.get("organization")
            or row.get("prize_jury")
            or row.get("prizeJury")
        )

        award_type = _safe_str(
            row.get("award_type")
            or row.get("awardType")
            or row.get("type")
        )

        award_status = _safe_str(
            row.get("award_status")
            or row.get("awardStatus")
            or row.get("status")
        )

        award_date = _parse_date_or_none(
            row.get("award_date")
            or row.get("awardDate")
            or row.get("date")
        )

        award_category = _safe_str(
            row.get("award_category")
            or row.get("awardCategory")
            or row.get("category")
        )

        award_level = _safe_str(
            row.get("award_level")
            or row.get("awardLevel")
            or row.get("level")
        )

        language_code = _safe_str(
            row.get("language_code")
            or row.get("languageCode")
            or row.get("language")
        )

        recipient_name = _safe_str(
            row.get("recipient_name")
            or row.get("recipientName")
            or row.get("recipient")
        )

        recipient_role = _safe_str(
            row.get("recipient_role")
            or row.get("recipientRole")
        )

        award_position = _safe_str(
            row.get("award_position")
            or row.get("awardPosition")
            or row.get("position")
        )

        raw_sequence = (
            row.get("sequence_number")
            or row.get("sequenceNumber")
            or row.get("sequence")
            or index + 1
        )
        try:
            sequence_number = int(str(raw_sequence).strip())
        except (ValueError, TypeError):
            sequence_number = index + 1

        award_website = _safe_str(
            row.get("award_website")
            or row.get("awardWebsite")
            or row.get("website")
            or row.get("url")
        )

        award_note = _safe_str(
            row.get("award_note")
            or row.get("awardNote")
            or row.get("note")
        )

        if not (
            prize_name
            or prize_year
            or prize_country
            or prize_code
            or prize_jury
            or award_type
            or award_status
            or award_date
            or award_category
            or award_level
            or language_code
            or recipient_name
            or recipient_role
            or award_position
            or award_website
            or award_note
        ):
            continue

        cur.execute(
            """
            INSERT INTO edition_prizes (
                tenant_id,
                edition_id,
                prize_name,
                prize_year,
                prize_country,
                prize_code,
                prize_jury,
                award_type,
                award_status,
                award_date,
                award_category,
                award_level,
                language_code,
                recipient_name,
                recipient_role,
                award_position,
                sequence_number,
                award_website,
                award_note
            )
            VALUES (
                %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s
            )
            """,
            (
                tenant_id,
                edition_uuid,
                prize_name,
                prize_year,
                prize_country,
                prize_code,
                prize_jury,
                award_type,
                award_status,
                award_date,
                award_category,
                award_level,
                language_code,
                recipient_name,
                recipient_role,
                award_position,
                sequence_number,
                award_website,
                award_note,
            ),
        )
        inserted += 1

    return {
        "ok": True,
        "work_id": work_id,
        "edition_id": edition_uuid,
        "awards_saved": inserted,
    }

def update_edition_related_products(
    cur,
    tenant_id: str,
    work_id: str,
    edition_id: str,
    payload: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Persist ONIX RelatedWork and RelatedProduct composites.

    RelatedProduct is intentionally stored in the existing single normalized
    edition_related_products table. The primary identifier is stored in the
    scalar lookup columns, while the complete repeatable ProductIdentifier
    composite is preserved in identifiers JSONB.
    """

    try:
        edition_uuid = str(uuid.UUID(str(edition_id)))
    except (ValueError, TypeError):
        raise ValueError("Invalid edition id")

    cur.execute(
        """
        SELECT id
        FROM editions
        WHERE tenant_id = %s
          AND work_id = %s
          AND id = %s
        LIMIT 1
        """,
        (tenant_id, work_id, edition_uuid),
    )
    if not cur.fetchone():
        raise ValueError("Edition not found")

    rows = (
        payload.get("related_products")
        or payload.get("relatedProducts")
        or payload.get("related_items")
        or payload.get("relatedItems")
        or []
    )
    if not isinstance(rows, list):
        rows = []

    related_works = (
        payload.get("related_works")
        or payload.get("relatedWorks")
        or []
    )
    if not isinstance(related_works, list):
        related_works = []

    # ------------------------------------------------------------
    # RelatedWork
    # ------------------------------------------------------------
    cur.execute(
        """
        DELETE FROM edition_related_works
        WHERE tenant_id = %s
          AND edition_id = %s
        """,
        (tenant_id, edition_uuid),
    )

    related_works_inserted = 0

    for index, row in enumerate(related_works):
        if not isinstance(row, dict):
            continue

        work_relation_code = _safe_str(
            row.get("work_relation_code")
            or row.get("workRelationCode")
            or row.get("relation_code")
            or row.get("relationCode")
        )
        work_id_type = _safe_str(
            row.get("work_id_type")
            or row.get("workIdType")
            or row.get("identifier_type")
            or row.get("identifierType")
        )
        id_type_name = _safe_str(
            row.get("id_type_name")
            or row.get("idTypeName")
        )
        id_value = _safe_str(
            row.get("id_value")
            or row.get("idValue")
            or row.get("identifier_value")
            or row.get("identifierValue")
        )
        note = _safe_str(row.get("note"))

        if not (
            work_relation_code
            or work_id_type
            or id_value
            or note
        ):
            continue

        cur.execute(
            """
            INSERT INTO edition_related_works (
                tenant_id,
                edition_id,
                work_relation_code,
                work_id_type,
                id_type_name,
                id_value,
                note,
                item_order
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                tenant_id,
                edition_uuid,
                work_relation_code,
                work_id_type,
                id_type_name,
                id_value,
                note,
                index + 1,
            ),
        )
        related_works_inserted += 1

    # ------------------------------------------------------------
    # RelatedProduct
    # One row per RelatedProduct composite. Repeatable identifiers live in
    # edition_related_products.identifiers JSONB.
    # ------------------------------------------------------------
    cur.execute(
        """
        DELETE FROM edition_related_products
        WHERE tenant_id = %s
          AND edition_id = %s
        """,
        (tenant_id, edition_uuid),
    )

    inserted = 0

    for index, row in enumerate(rows):
        if not isinstance(row, dict):
            continue

        relation_code = _safe_str(
            row.get("product_relation_code")
            or row.get("productRelationCode")
            or row.get("relation_code")
            or row.get("relationCode")
            or row.get("relationship_type")
            or row.get("relationshipType")
        )

        related_isbn13 = _normalize_isbn13(
            row.get("related_isbn13")
            or row.get("isbn13")
            or row.get("isbn_13")
            or row.get("isbn")
        ) or ""

        product_form = _safe_str(
            row.get("related_product_form")
            or row.get("product_form")
            or row.get("productForm")
            or row.get("format_code")
            or row.get("formatCode")
        )

        product_form_detail = _safe_str(
            row.get("related_product_form_detail")
            or row.get("product_form_detail")
            or row.get("productFormDetail")
            or row.get("format_detail")
            or row.get("formatDetail")
        )

        title = _safe_str(
            row.get("title")
            or row.get("product_title")
            or row.get("productTitle")
        )

        subtitle = _safe_str(
            row.get("subtitle")
            or row.get("product_subtitle")
            or row.get("productSubtitle")
        )

        proprietary_id = _safe_str(
            row.get("proprietary_id")
            or row.get("proprietaryId")
        )

        publisher_name = _safe_str(
            row.get("publisher_name")
            or row.get("publisherName")
            or row.get("publisher")
        )

        publication_date = _parse_date_or_none(
            row.get("publication_date")
            or row.get("publicationDate")
        )

        product_url = _safe_str(
            row.get("product_url")
            or row.get("productUrl")
            or row.get("url")
            or row.get("link")
        )

        note = _safe_str(
            row.get("relationship_note")
            or row.get("relationshipNote")
            or row.get("note")
        )

        # --------------------------------------------------------
        # Normalize repeatable ProductIdentifier composites.
        # --------------------------------------------------------
        raw_identifiers = (
            row.get("product_identifiers")
            or row.get("productIdentifiers")
            or row.get("identifiers")
            or []
        )
        if not isinstance(raw_identifiers, list):
            raw_identifiers = []

        cleaned_identifiers: List[Dict[str, str]] = []
        seen_identifiers = set()

        for identifier in raw_identifiers:
            if not isinstance(identifier, dict):
                continue

            id_type = _safe_str(
                identifier.get("product_id_type")
                or identifier.get("productIdType")
                or identifier.get("identifier_type")
                or identifier.get("identifierType")
                or identifier.get("type")
            )

            id_type_name = _safe_str(
                identifier.get("id_type_name")
                or identifier.get("idTypeName")
            )

            id_value = _safe_str(
                identifier.get("id_value")
                or identifier.get("idValue")
                or identifier.get("identifier_value")
                or identifier.get("identifierValue")
                or identifier.get("value")
            )

            if not id_type or not id_value:
                continue

            key = (id_type, id_type_name, id_value)
            if key in seen_identifiers:
                continue

            seen_identifiers.add(key)
            cleaned_identifiers.append(
                {
                    "product_id_type": id_type,
                    "id_type_name": id_type_name,
                    "id_value": id_value,
                }
            )

        # Accept scalar primary identifier fields from the UI/importer.
        direct_id_type = _safe_str(
            row.get("related_product_id_type")
            or row.get("relatedProductIdType")
        )
        direct_id_value = _safe_str(
            row.get("related_product_id_value")
            or row.get("relatedProductIdValue")
        )

        if direct_id_type and direct_id_value:
            direct_key = (
                direct_id_type,
                "",
                direct_id_value,
            )
            if direct_key not in seen_identifiers:
                cleaned_identifiers.insert(
                    0,
                    {
                        "product_id_type": direct_id_type,
                        "id_type_name": "",
                        "id_value": direct_id_value,
                    },
                )
                seen_identifiers.add(direct_key)

        # related_isbn13 is itself ProductIdentifier type 15.
        if related_isbn13 and not any(
            identifier["product_id_type"] == "15"
            and identifier["id_value"] == related_isbn13
            for identifier in cleaned_identifiers
        ):
            cleaned_identifiers.insert(
                0,
                {
                    "product_id_type": "15",
                    "id_type_name": "",
                    "id_value": related_isbn13,
                },
            )

        if not (
            relation_code
            or related_isbn13
            or proprietary_id
            or title
            or cleaned_identifiers
        ):
            continue

        if not related_isbn13:
            isbn_identifier = next(
                (
                    identifier
                    for identifier in cleaned_identifiers
                    if identifier["product_id_type"] == "15"
                    and _normalize_isbn13(identifier["id_value"])
                ),
                None,
            )
            if isbn_identifier:
                related_isbn13 = (
                    _normalize_isbn13(isbn_identifier["id_value"]) or ""
                )

        # Existing scalar columns hold the primary identifier for fast lookup.
        # Prefer ISBN-13 (ProductIDType 15), otherwise the first identifier.
        primary_identifier = next(
            (
                identifier
                for identifier in cleaned_identifiers
                if identifier["product_id_type"] == "15"
            ),
            cleaned_identifiers[0]
            if cleaned_identifiers
            else None,
        )

        primary_id_type = (
            primary_identifier["product_id_type"]
            if primary_identifier
            else ""
        )
        primary_id_value = (
            primary_identifier["id_value"]
            if primary_identifier
            else ""
        )

        cur.execute(
            """
            INSERT INTO edition_related_products (
                tenant_id,
                edition_id,
                relation_code,
                related_isbn13,
                related_product_form,
                related_product_form_detail,
                related_product_id_type,
                related_product_id_value,
                title,
                subtitle,
                proprietary_id,
                publisher_name,
                publication_date,
                product_url,
                identifiers,
                note,
                item_order
            )
            VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s::jsonb, %s, %s
            )
            RETURNING id
            """,
            (
                tenant_id,
                edition_uuid,
                relation_code,
                related_isbn13,
                product_form,
                product_form_detail,
                primary_id_type,
                primary_id_value,
                title,
                subtitle,
                proprietary_id,
                publisher_name,
                publication_date,
                product_url,
                json.dumps(cleaned_identifiers),
                note,
                index + 1,
            ),
        )

        # RETURNING is retained so a failed parent insert is detected
        # immediately, but there is no separate identifier child table.
        parent_row = cur.fetchone() or {}
        if not _safe_str(parent_row.get("id")):
            raise ValueError(
                "Related Product insert did not return an id"
            )

        inserted += 1

    return {
        "ok": True,
        "work_id": work_id,
        "edition_id": edition_uuid,
        "related_products_saved": inserted,
        "related_works_saved": related_works_inserted,
    }

def update_edition_rights_restrictions(
    cur,
    tenant_id: str,
    work_id: str,
    edition_id: str,
    payload: Dict[str, Any],
) -> Dict[str, Any]:
    """Persist Rights & Restrictions against the existing InkSuite schema."""

    try:
        edition_uuid = str(uuid.UUID(str(edition_id)))
    except (ValueError, TypeError):
        raise ValueError("Invalid edition id")

    cur.execute(
        """
        SELECT id
        FROM editions
        WHERE tenant_id = %s
          AND work_id = %s
          AND id = %s
        LIMIT 1
        """,
        (tenant_id, work_id, edition_uuid),
    )
    if not cur.fetchone():
        raise ValueError("Edition not found")

    def _territory_from(value: Any) -> Dict[str, Any]:
        source = value if isinstance(value, dict) else {}

        included = (
            source.get("countries_included")
            or source.get("countriesIncluded")
            or []
        )
        excluded = (
            source.get("countries_excluded")
            or source.get("countriesExcluded")
            or []
        )

        if not isinstance(included, list):
            included = [
                item
                for item in re.split(r"[\s,;]+", _safe_str(included))
                if item
            ]
        if not isinstance(excluded, list):
            excluded = [
                item
                for item in re.split(r"[\s,;]+", _safe_str(excluded))
                if item
            ]

        return {
            "worldwide": bool(
                source.get("worldwide")
                or source.get("is_worldwide")
                or source.get("isWorldwide")
            ),
            "countries_included": [
                _safe_str(item)
                for item in included
                if _safe_str(item)
            ],
            "countries_excluded": [
                _safe_str(item)
                for item in excluded
                if _safe_str(item)
            ],
            "regions_included": _safe_str(
                source.get("regions_included")
                or source.get("regionsIncluded")
            ),
            "regions_excluded": _safe_str(
                source.get("regions_excluded")
                or source.get("regionsExcluded")
            ),
        }

    sales_rights_type = _safe_str(
        payload.get("sales_rights_type")
        or payload.get("salesRightsType")
    )
    row_sales_rights_type = _safe_str(
        payload.get("row_sales_rights_type")
        or payload.get("rowSalesRightsType")
        or payload.get("ROWSalesRightsType")
    )
    territory = _territory_from(
        payload.get("sales_rights_territory")
        or payload.get("salesRightsTerritory")
        or payload
    )

    # Existing edition_rights columns are TEXT, not arrays.
    countries_included_text = " ".join(
        territory["countries_included"]
    )
    countries_excluded_text = " ".join(
        territory["countries_excluded"]
    )

    # Preserve the card's Worldwide checkbox without adding a duplicate DB column.
    # WORLD is stored in the existing regions_included field.
    regions_included_text = (
        "WORLD"
        if territory["worldwide"]
        else territory["regions_included"]
    )
    regions_excluded_text = territory["regions_excluded"]

    copyright_type = _safe_str(
        payload.get("copyright_type")
        or payload.get("copyrightType")
    ) or "C"

    copyright_year = _to_int_or_none(
        payload.get("copyright_year")
        if payload.get("copyright_year") is not None
        else payload.get("copyrightYear")
    )
    copyright_holder = _safe_str(
        payload.get("copyright_owner")
        or payload.get("copyrightOwner")
        or payload.get("copyright_holder")
        or payload.get("copyrightHolder")
    )
    copyright_notice = _safe_str(
        payload.get("copyright_notice")
        or payload.get("copyrightNotice")
    )
    public_domain = bool(
        payload.get("public_domain")
        if payload.get("public_domain") is not None
        else payload.get("publicDomain")
    )
    notes = _safe_str(
        payload.get("rights_note")
        or payload.get("rightsNote")
        or payload.get("notes")
    )

    rights_columns = _table_column_names(
        cur,
        "edition_rights",
    )

    cur.execute(
        """
        SELECT id
        FROM edition_rights
        WHERE tenant_id = %s
          AND edition_id = %s
        ORDER BY item_order ASC NULLS LAST, id ASC
        LIMIT 1
        """,
        (tenant_id, edition_uuid),
    )
    rights_row = cur.fetchone()

    rights_values = {
        "sales_rights_type": sales_rights_type,
        "row_sales_rights_type": row_sales_rights_type,
        "countries_included": countries_included_text,
        "countries_excluded": countries_excluded_text,
        "regions_included": regions_included_text,
        "regions_excluded": regions_excluded_text,
        "copyright_type": copyright_type,
        "copyright_holder": copyright_holder,
        "copyright_notice": copyright_notice,
        "public_domain": public_domain,
        "notes": notes,
        "item_order": 1,

        # Keep legacy mirrors populated.
        "exclusive_rights_country": countries_included_text,
        "exclusive_rights_territory": regions_included_text,
    }

    if rights_row:
        usable = {
            key: value
            for key, value in rights_values.items()
            if key in rights_columns
        }
        if "updated_at" in rights_columns:
            usable["updated_at"] = datetime.now()

        assignments = ", ".join(
            f"{column} = %s"
            for column in usable.keys()
        )
        cur.execute(
            f"""
            UPDATE edition_rights
            SET {assignments}
            WHERE tenant_id = %s
              AND edition_id = %s
              AND id = %s
            """,
            tuple(usable.values())
            + (
                tenant_id,
                edition_uuid,
                rights_row["id"],
            ),
        )
    else:
        values = {
            "tenant_id": tenant_id,
            "edition_id": edition_uuid,
            **rights_values,
        }
        usable = {
            key: value
            for key, value in values.items()
            if key in rights_columns
        }
        columns = list(usable.keys())
        placeholders = ", ".join(["%s"] * len(columns))
        cur.execute(
            f"""
            INSERT INTO edition_rights (
                {", ".join(columns)}
            )
            VALUES ({placeholders})
            """,
            tuple(usable[column] for column in columns),
        )

    # Canonical repeatable SalesRights composites.
    sales_rights_rows = payload.get("sales_rights") or payload.get("salesRights") or []
    if not isinstance(sales_rights_rows, list):
        sales_rights_rows = []
    cur.execute(
        "DELETE FROM edition_sales_rights WHERE tenant_id = %s AND edition_id = %s",
        (tenant_id, edition_uuid),
    )
    for index, row in enumerate(sales_rights_rows, start=1):
        if not isinstance(row, dict):
            continue
        s_type = _safe_str(row.get("sales_rights_type") or row.get("salesRightsType"))
        c_in = _safe_str(row.get("countries_included") or row.get("countriesIncluded"))
        r_in = _safe_str(row.get("regions_included") or row.get("regionsIncluded"))
        c_ex = _safe_str(row.get("countries_excluded") or row.get("countriesExcluded"))
        r_ex = _safe_str(row.get("regions_excluded") or row.get("regionsExcluded"))
        if not (s_type or c_in or r_in or c_ex or r_ex):
            continue
        cur.execute(
            """
            INSERT INTO edition_sales_rights (
                tenant_id, edition_id, sales_rights_type,
                countries_included, regions_included,
                countries_excluded, regions_excluded, item_order
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
            """,
            (tenant_id, edition_uuid, s_type, c_in, r_in, c_ex, r_ex, index),
        )

    # Copyright year already belongs to editions.
    cur.execute(
        """
        UPDATE editions
        SET copyright_year = %s,
            updated_at = now()
        WHERE tenant_id = %s
          AND work_id = %s
          AND id = %s
        """,
        (
            copyright_year,
            tenant_id,
            work_id,
            edition_uuid,
        ),
    )

    # Existing edition_sales_restrictions table.
    restrictions = (
        payload.get("sales_restrictions")
        or payload.get("salesRestrictions")
        or []
    )
    if not isinstance(restrictions, list):
        restrictions = []

    cur.execute(
        """
        DELETE FROM edition_sales_restrictions
        WHERE tenant_id = %s
          AND edition_id = %s
        """,
        (tenant_id, edition_uuid),
    )

    for index, row in enumerate(restrictions, start=1):
        if not isinstance(row, dict):
            continue

        restriction_type = _safe_str(
            row.get("sales_restriction_type")
            or row.get("salesRestrictionType")
            or row.get("restriction_type")
            or row.get("restrictionType")
            or row.get("type")
        )
        restriction_detail = _safe_str(
            row.get("restriction_detail")
            or row.get("restrictionDetail")
        )
        rterritory = _territory_from(
            row.get("territory") or row
        )

        r_countries_included = " ".join(
            rterritory["countries_included"]
        )
        r_countries_excluded = " ".join(
            rterritory["countries_excluded"]
        )
        r_regions_included = (
            "WORLD"
            if rterritory["worldwide"]
            else rterritory["regions_included"]
        )
        r_regions_excluded = rterritory["regions_excluded"]

        start_date = _parse_date_or_none(
            row.get("start_date")
            or row.get("startDate")
        )
        end_date = _parse_date_or_none(
            row.get("end_date")
            or row.get("endDate")
        )
        note = _safe_str(
            row.get("restriction_note")
            or row.get("restrictionNote")
            or row.get("note")
        )

        if not (
            restriction_type
            or restriction_detail
            or r_countries_included
            or r_countries_excluded
            or r_regions_included
            or r_regions_excluded
            or note
        ):
            continue

        cur.execute(
            """
            INSERT INTO edition_sales_restrictions (
                tenant_id,
                edition_id,
                restriction_type,
                restriction_detail,
                countries_included,
                countries_excluded,
                regions_included,
                regions_excluded,
                start_date,
                end_date,
                note,
                item_order
            )
            VALUES (
                %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s
            )
            """,
            (
                tenant_id,
                edition_uuid,
                restriction_type,
                restriction_detail,
                r_countries_included,
                r_countries_excluded,
                r_regions_included,
                r_regions_excluded,
                start_date,
                end_date,
                note,
                index,
            ),
        )

    # Digital constraints table is the only genuinely missing persistence table.
    constraints = (
        payload.get("usage_constraints")
        or payload.get("usageConstraints")
        or payload.get("epub_usage_constraints")
        or payload.get("epubUsageConstraints")
        or []
    )
    if not isinstance(constraints, list):
        constraints = []

    usage_columns = _table_column_names(
        cur,
        "edition_usage_constraints",
    )

    if usage_columns:
        cur.execute(
            """
            DELETE FROM edition_usage_constraints
            WHERE tenant_id = %s
              AND edition_id = %s
            """,
            (tenant_id, edition_uuid),
        )

        for index, row in enumerate(constraints, start=1):
            if not isinstance(row, dict):
                continue

            usage_type = _safe_str(
                row.get("usage_type")
                or row.get("usageType")
                or row.get("constraint_type")
                or row.get("constraintType")
                or row.get("type")
            )
            usage_status = _safe_str(
                row.get("usage_status")
                or row.get("usageStatus")
                or row.get("status")
            )
            quantity = _to_float_or_none(
                row.get("quantity")
                if row.get("quantity") is not None
                else row.get("usage_quantity")
                if row.get("usage_quantity") is not None
                else row.get("usageQuantity")
            )
            unit_code = _safe_str(
                row.get("unit_code")
                or row.get("unitCode")
                or row.get("usage_unit")
                or row.get("usageUnit")
            )
            note = _safe_str(
                row.get("usage_note")
                or row.get("usageNote")
                or row.get("note")
            )

            if not (
                usage_type
                or usage_status
                or quantity is not None
                or unit_code
                or note
            ):
                continue

            cur.execute(
                """
                INSERT INTO edition_usage_constraints (
                    tenant_id,
                    edition_id,
                    usage_type,
                    usage_status,
                    quantity,
                    unit_code,
                    usage_note,
                    item_order
                )
                VALUES (
                    %s, %s, %s, %s,
                    %s, %s, %s, %s
                )
                """,
                (
                    tenant_id,
                    edition_uuid,
                    usage_type,
                    usage_status,
                    quantity,
                    unit_code,
                    note,
                    index,
                ),
            )

    return {
        "ok": True,
        "work_id": work_id,
        "edition_id": edition_uuid,
    }



def create_first_work_edition(
    cur,
    tenant_id: str,
    work_id: str,
    payload: Dict[str, Any],
) -> Dict[str, Any]:
    """Create the first development edition before an ISBN is assigned."""

    work_id = _safe_str(work_id)
    if not work_id:
        raise ValueError("work_id is required")

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
    if not cur.fetchone():
        raise ValueError("Existing work not found")

    cur.execute(
        """
        SELECT id
        FROM editions
        WHERE tenant_id = %s
          AND work_id = %s
        ORDER BY created_at ASC, id ASC
        LIMIT 1
        """,
        (tenant_id, work_id),
    )
    if cur.fetchone():
        raise ValueError("This work already has an edition")

    requested = _safe_str(
        payload.get("format")
        or payload.get("product_form")
        or payload.get("productForm")
    ).lower()

    if requested == "hardcover":
        product_form = "Hardcover"
        product_form_detail = "Hardcover"
        onix_product_form = "BB"
        onix_product_form_detail = ""
    elif requested == "paperback":
        product_form = "Paperback"
        product_form_detail = "Paperback"
        onix_product_form = "BC"
        onix_product_form_detail = "B102"
    else:
        raise ValueError("First edition must be Hardcover or Paperback")

    cur.execute(
        """
        INSERT INTO editions (
            tenant_id,
            work_id,
            isbn13,
            product_form,
            product_form_detail,
            onix_product_form,
            onix_product_form_detail,
            publishing_status,
            notification_type,
            product_composition
        )
        VALUES (%s, %s, NULL, %s, %s, %s, %s, '02', '02', '00')
        RETURNING id
        """,
        (
            tenant_id,
            work_id,
            product_form,
            product_form_detail,
            onix_product_form,
            onix_product_form_detail,
        ),
    )
    row = cur.fetchone() or {}

    return {
        "ok": True,
        "work_id": work_id,
        "edition_id": str(row.get("id") or ""),
        "product_form": product_form,
        "product_form_detail": product_form_detail,
        "onix_product_form": onix_product_form,
        "onix_product_form_detail": onix_product_form_detail,
        "publishing_status": "02",
        "notification_type": "02",
        "product_composition": "00",
    }


def _replace_editions(cur, tenant_id: str, work_id: str, payload: Dict[str, Any]) -> None:
    incoming_formats = payload.get("formats") or []
    if not isinstance(incoming_formats, list):
        incoming_formats = []

    seen_isbns: List[str] = []

    for fmt in incoming_formats:
        if not isinstance(fmt, dict):
            continue

        raw_edition_id = _safe_str(
            fmt.get("id")
            or fmt.get("edition_id")
            or fmt.get("editionId")
            or fmt.get("persistedId")
        )

        isbn13 = _normalize_isbn13(
            fmt.get("ISBN") or fmt.get("isbn") or fmt.get("isbn13")
        )
        isbn_value = isbn13 or None

        product_form = _safe_str(
            fmt.get("format")
            or fmt.get("product_form")
            or fmt.get("productForm")
            or fmt.get("product_form_detail")
            or fmt.get("productFormDetail")
        )
        product_form_detail = _safe_str(
            fmt.get("product_form_detail")
            or fmt.get("productFormDetail")
            or fmt.get("format")
            or product_form
        )
        onix_product_form = _safe_str(
            fmt.get("onix_product_form")
            or fmt.get("product_form_code")
            or fmt.get("productFormCode")
        )
        onix_product_form_detail = _safe_str(
            fmt.get("onix_product_form_detail")
            or fmt.get("product_form_detail_code")
            or fmt.get("productFormDetailCode")
        )
        publishing_status = _safe_str(
            fmt.get("publishing_status")
            or fmt.get("publishingStatus")
            or fmt.get("product_status")
            or fmt.get("productStatus")
        )
        notification_type = _safe_str(
            fmt.get("notification_type") or fmt.get("notificationType")
        )
        product_composition = _safe_str(
            fmt.get("product_composition") or fmt.get("productComposition")
        )

        publication_date = _parse_date_or_none(
            fmt.get("pub_date") or fmt.get("publication_date")
        )
        number_of_pages = _to_int_or_none(
            fmt.get("pages") if fmt.get("pages") is not None else fmt.get("number_of_pages")
        )
        height = _to_float_or_none(
            fmt.get("tall") if fmt.get("tall") is not None else fmt.get("height")
        )
        width = _to_float_or_none(
            fmt.get("wide") if fmt.get("wide") is not None else fmt.get("width")
        )
        thickness = _to_float_or_none(
            fmt.get("spine") if fmt.get("spine") is not None else fmt.get("thickness")
        )
        unit_weight = _to_float_or_none(
            fmt.get("weight") if fmt.get("weight") is not None else fmt.get("unit_weight")
        )

        price_us = _to_float_or_none(fmt.get("price_us"))
        price_can = _to_float_or_none(fmt.get("price_can"))

        existing = None

        if raw_edition_id:
            try:
                edition_uuid = str(uuid.UUID(raw_edition_id))
            except (ValueError, TypeError):
                edition_uuid = ""
            if edition_uuid:
                cur.execute(
                    """
                    SELECT id
                    FROM editions
                    WHERE tenant_id = %s
                      AND work_id = %s
                      AND id = %s
                    LIMIT 1
                    """,
                    (tenant_id, work_id, edition_uuid),
                )
                existing = cur.fetchone()

        if not existing and isbn13:
            cur.execute(
                """
                SELECT id
                FROM editions
                WHERE tenant_id = %s
                  AND work_id = %s
                  AND isbn13 = %s
                LIMIT 1
                """,
                (tenant_id, work_id, isbn13),
            )
            existing = cur.fetchone()

        # Blank ISBN is valid only for an already persisted development edition.
        if not existing and not isbn13:
            continue

        if isbn13:
            seen_isbns.append(isbn13)

        if existing:
            edition_id = existing["id"]
            cur.execute(
                """
                UPDATE editions
                SET
                    isbn13 = COALESCE(%s, isbn13),
                    product_form = CASE WHEN %s <> '' THEN %s ELSE product_form END,
                    product_form_detail = CASE WHEN %s <> '' THEN %s ELSE product_form_detail END,
                    onix_product_form = CASE WHEN %s <> '' THEN %s ELSE onix_product_form END,
                    onix_product_form_detail = CASE WHEN %s <> '' THEN %s ELSE onix_product_form_detail END,
                    publishing_status = CASE WHEN %s <> '' THEN %s ELSE publishing_status END,
                    notification_type = CASE WHEN %s <> '' THEN %s ELSE notification_type END,
                    product_composition = CASE WHEN %s <> '' THEN %s ELSE product_composition END,
                    updated_at = now()
                WHERE tenant_id = %s
                  AND work_id = %s
                  AND id = %s
                """,
                (
                    isbn_value,
                    product_form, product_form,
                    product_form_detail, product_form_detail,
                    onix_product_form, onix_product_form,
                    onix_product_form_detail, onix_product_form_detail,
                    publishing_status, publishing_status,
                    notification_type, notification_type,
                    product_composition, product_composition,
                    tenant_id,
                    work_id,
                    edition_id,
                ),
            )
        else:
            cur.execute(
                """
                INSERT INTO editions (
                    tenant_id, work_id, isbn13,
                    product_form, product_form_detail,
                    onix_product_form, onix_product_form_detail,
                    publishing_status, notification_type, product_composition
                )
                VALUES (
                    %s, %s, %s, %s, %s, %s, %s,
                    COALESCE(NULLIF(%s, ''), '02'),
                    COALESCE(NULLIF(%s, ''), '02'),
                    COALESCE(NULLIF(%s, ''), '00')
                )
                RETURNING id
                """,
                (
                    tenant_id, work_id, isbn13,
                    product_form, product_form_detail,
                    onix_product_form, onix_product_form_detail,
                    publishing_status, notification_type, product_composition,
                ),
            )
            edition_row = cur.fetchone()
            edition_id = edition_row and edition_row.get("id")

        # Legacy format-card compatibility: keep accepting pub_date/publication_date,
        # but store PublishingDateRole 01 only in the normalized ONIX table.
        if edition_id and publication_date is not None:
            cur.execute(
                """
                DELETE FROM edition_publishing_dates
                WHERE tenant_id = %s
                  AND edition_id = %s
                  AND date_role = '01'
                """,
                (tenant_id, edition_id),
            )
            cur.execute(
                """
                INSERT INTO edition_publishing_dates (
                    tenant_id, edition_id, date_role, date_value, date_text,
                    date_format, note, item_order
                )
                VALUES (%s, %s, '01', %s, %s, '00', '', 1)
                """,
                (
                    tenant_id,
                    edition_id,
                    publication_date,
                    publication_date.strftime("%Y%m%d"),
                ),
            )

        # Legacy format-card compatibility: persist pages and physical dimensions only
        # in the normalized ONIX tables. Do not write deprecated editions scalars.
        if edition_id:
            if number_of_pages is not None:
                cur.execute(
                    "DELETE FROM edition_extents WHERE tenant_id = %s AND edition_id = %s AND extent_type = '00'",
                    (tenant_id, edition_id),
                )
                if number_of_pages > 0:
                    cur.execute(
                        """
                        INSERT INTO edition_extents
                            (tenant_id, edition_id, extent_type, extent_value, extent_unit, item_order)
                        VALUES (%s, %s, '00', %s, '03', 1)
                        """,
                        (tenant_id, edition_id, number_of_pages),
                    )

            for measure_type, measurement, unit_code in (
                ('01', height, 'in'),
                ('02', width, 'in'),
                ('03', thickness, 'in'),
                ('08', unit_weight, 'lb'),
            ):
                if measurement is None:
                    continue
                cur.execute(
                    "DELETE FROM edition_measurements WHERE tenant_id = %s AND edition_id = %s AND measure_type = %s",
                    (tenant_id, edition_id, measure_type),
                )
                if measurement != 0:
                    cur.execute(
                        """
                        INSERT INTO edition_measurements
                            (tenant_id, edition_id, measure_type, measurement, measure_unit_code, item_order)
                        VALUES (%s, %s, %s, %s, %s, 1)
                        """,
                        (tenant_id, edition_id, measure_type, measurement, unit_code),
                    )

        if edition_id and (price_us is not None or price_can is not None):
            try:
                cur.execute(
                    """
                    SELECT id
                    FROM edition_supply_details
                    WHERE tenant_id = %s
                      AND edition_id = %s
                    ORDER BY id ASC
                    LIMIT 1
                    """,
                    (tenant_id, edition_id),
                )
                sd_row = cur.fetchone()

                if sd_row:
                    supply_detail_id = sd_row["id"]
                else:
                    cur.execute(
                        """
                        INSERT INTO edition_supply_details (tenant_id, edition_id)
                        VALUES (%s, %s)
                        RETURNING id
                        """,
                        (tenant_id, edition_id),
                    )
                    sd_created = cur.fetchone()
                    supply_detail_id = sd_created and sd_created.get("id")

                if supply_detail_id:
                    if price_us is not None:
                        cur.execute(
                            """
                            SELECT id
                            FROM edition_prices
                            WHERE tenant_id = %s
                              AND supply_detail_id = %s
                              AND upper(coalesce(currency_code, '')) = 'USD'
                            LIMIT 1
                            """,
                            (tenant_id, supply_detail_id),
                        )
                        row = cur.fetchone()
                        if row:
                            cur.execute(
                                """
                                UPDATE edition_prices
                                SET price_amount = %s
                                WHERE tenant_id = %s
                                  AND id = %s
                                """,
                                (price_us, tenant_id, row["id"]),
                            )
                        else:
                            cur.execute(
                                """
                                INSERT INTO edition_prices (
                                    tenant_id, supply_detail_id, price_amount, currency_code
                                )
                                VALUES (%s, %s, %s, 'USD')
                                """,
                                (tenant_id, supply_detail_id, price_us),
                            )

                    if price_can is not None:
                        cur.execute(
                            """
                            SELECT id
                            FROM edition_prices
                            WHERE tenant_id = %s
                              AND supply_detail_id = %s
                              AND upper(coalesce(currency_code, '')) IN ('CAD', 'CAN')
                            LIMIT 1
                            """,
                            (tenant_id, supply_detail_id),
                        )
                        row = cur.fetchone()
                        if row:
                            cur.execute(
                                """
                                UPDATE edition_prices
                                SET price_amount = %s,
                                    currency_code = 'CAD'
                                WHERE tenant_id = %s
                                  AND id = %s
                                """,
                                (price_can, tenant_id, row["id"]),
                            )
                        else:
                            cur.execute(
                                """
                                INSERT INTO edition_prices (
                                    tenant_id, supply_detail_id, price_amount, currency_code
                                )
                                VALUES (%s, %s, %s, 'CAD')
                                """,
                                (tenant_id, supply_detail_id, price_can),
                            )
            except Exception:
                pass


def _replace_party_representation(
    cur,
    tenant_id: str,
    represented_party_id: str,
    work_id: str,
    agency_payload: Dict[str, Any],
) -> None:
    if not agency_payload:
        return

    agency_name = _safe_str(agency_payload.get("agency"))
    agent_name = _safe_str(agency_payload.get("agent") or agency_payload.get("contact"))
    email = _safe_str(agency_payload.get("email"))
    website = _safe_str(agency_payload.get("website"))
    phone = _safe_str(agency_payload.get("phone"))

    agency_party_id: Optional[str] = None
    agent_party_id: Optional[str] = None

    try:
        if agency_name:
            agency_party_id = _upsert_party_minimal(
                cur, tenant_id, agency_name, party_type="organization", email=email
            )
            if agency_party_id:
                cur.execute(
                    """
                    UPDATE parties
                    SET
                        email = COALESCE(NULLIF(%s, ''), email),
                        website = COALESCE(NULLIF(%s, ''), website),
                        updated_at = now()
                    WHERE tenant_id = %s AND id = %s
                    """,
                    (email, website, tenant_id, agency_party_id),
                )

        if agent_name:
            agent_party_id = _upsert_party_minimal(
                cur, tenant_id, agent_name, party_type="person", email=email
            )
            if agent_party_id:
                cur.execute(
                    """
                    UPDATE parties
                    SET
                        email = COALESCE(NULLIF(%s, ''), email),
                        website = COALESCE(NULLIF(%s, ''), website),
                        phone_number = COALESCE(NULLIF(%s, ''), phone_number),
                        updated_at = now()
                    WHERE tenant_id = %s AND id = %s
                    """,
                    (email, website, phone, tenant_id, agent_party_id),
                )

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
                    WHERE tenant_id = %s AND agency_party_id = %s
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
    except Exception:
        pass


def _normalize_date_string(v: Any) -> Optional[str]:
    if v in (None, "", "null"):
        return None
    if isinstance(v, (datetime, date)):
        return v.isoformat()[:10]
    s = str(v).strip()
    if not s:
        return None
    try:
        return date.fromisoformat(s[:10]).isoformat()
    except Exception:
        return None


def _clean_foreign_rights_sold_rows(rows: Any) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for row in (rows or []):
        if not isinstance(row, dict):
            continue

        country = _safe_str(row.get("country"))
        agency = _safe_str(row.get("agency"))
        sold_date = _normalize_date_string(row.get("sold_date") or row.get("date"))
        expiration_date = _normalize_date_string(
            row.get("expiration_date") or row.get("expiration")
        )
        notes = _safe_str(row.get("notes"))

        if not any([country, agency, sold_date, expiration_date, notes]):
            continue

        out.append(
            {
                "country": country,
                "agency": agency,
                "sold_date": sold_date,
                "expiration_date": expiration_date,
                "notes": notes,
            }
        )
    return out


def _replace_foreign_rights_sold(
    cur, tenant_id: str, work_id: str, payload: Dict[str, Any]
) -> None:
    rows = _clean_foreign_rights_sold_rows(
        payload.get("foreign_rights_sold") or payload.get("foreignRightsSold") or []
    )

    cur.execute(
        "DELETE FROM work_foreign_rights_sold WHERE tenant_id = %s AND work_id = %s",
        (tenant_id, work_id),
    )

    for row in rows:
        cur.execute(
            """
            INSERT INTO work_foreign_rights_sold (
                tenant_id, work_id, country, agency, sold_date, expiration_date, notes
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            (
                tenant_id,
                work_id,
                row["country"],
                row["agency"],
                _parse_date_or_none(row["sold_date"]),
                _parse_date_or_none(row["expiration_date"]),
                row["notes"],
            ),
        )


def _upsert_contributor_profile(
    cur, tenant_id: str, party_id: str, scope: str, payload: Dict[str, Any]
) -> None:
    """Upsert contributor profile fields while preserving existing nonblank values."""
    values = {
        "website_bio": _safe_str(payload.get(f"{scope}_website_bio") or payload.get(f"{scope}_long_bio")),
        "book_bio": _safe_str(payload.get(f"{scope}_book_bio") or payload.get(f"{scope}_bio")),
        "contact_pref_rank1": _safe_str(payload.get(f"{scope}_contact_pref_rank1")),
        "contact_pref_rank2": _safe_str(payload.get(f"{scope}_contact_pref_rank2")),
        "media_best_times": _safe_str(payload.get(f"{scope}_media_best_times")),
        "us_travel_plans": _safe_str(payload.get(f"{scope}_us_travel_plans")),
        "travel_dates": _safe_str(payload.get(f"{scope}_travel_dates")),
        "additional_notes": _safe_str(
            payload.get(f"{scope}_additional_notes")
            or payload.get(f"{scope}_marketing_additional_notes")
            or payload.get(f"{scope}_publicity_additional_notes")
        ),
        "photo_credit": _safe_str(payload.get(f"{scope}_photo_credit")),
        "present_position": _safe_str(payload.get(f"{scope}_present_position")),
        "former_positions": _safe_str(payload.get(f"{scope}_former_positions")),
        "degrees_honors": _safe_str(payload.get(f"{scope}_degrees_honors")),
        "professional_honors": _safe_str(payload.get(f"{scope}_professional_honors")),
    }
    press_key = f"{scope}_media_press_share"
    press_present = press_key in payload and payload.get(press_key) is not None
    press_value = bool(payload.get(press_key)) if press_present else False

    cur.execute(
        """
        INSERT INTO contributor_marketing_profiles (
            tenant_id, party_id, scope, website_bio, book_bio,
            contact_pref_rank1, contact_pref_rank2, media_best_times,
            media_press_share, us_travel_plans, travel_dates, additional_notes,
            photo_credit, present_position, former_positions, degrees_honors,
            professional_honors
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (tenant_id, party_id, scope)
        DO UPDATE SET
            website_bio = COALESCE(NULLIF(EXCLUDED.website_bio, ''), contributor_marketing_profiles.website_bio),
            book_bio = COALESCE(NULLIF(EXCLUDED.book_bio, ''), contributor_marketing_profiles.book_bio),
            contact_pref_rank1 = COALESCE(NULLIF(EXCLUDED.contact_pref_rank1, ''), contributor_marketing_profiles.contact_pref_rank1),
            contact_pref_rank2 = COALESCE(NULLIF(EXCLUDED.contact_pref_rank2, ''), contributor_marketing_profiles.contact_pref_rank2),
            media_best_times = COALESCE(NULLIF(EXCLUDED.media_best_times, ''), contributor_marketing_profiles.media_best_times),
            media_press_share = CASE WHEN %s THEN EXCLUDED.media_press_share ELSE contributor_marketing_profiles.media_press_share END,
            us_travel_plans = COALESCE(NULLIF(EXCLUDED.us_travel_plans, ''), contributor_marketing_profiles.us_travel_plans),
            travel_dates = COALESCE(NULLIF(EXCLUDED.travel_dates, ''), contributor_marketing_profiles.travel_dates),
            additional_notes = COALESCE(NULLIF(EXCLUDED.additional_notes, ''), contributor_marketing_profiles.additional_notes),
            photo_credit = COALESCE(NULLIF(EXCLUDED.photo_credit, ''), contributor_marketing_profiles.photo_credit),
            present_position = COALESCE(NULLIF(EXCLUDED.present_position, ''), contributor_marketing_profiles.present_position),
            former_positions = COALESCE(NULLIF(EXCLUDED.former_positions, ''), contributor_marketing_profiles.former_positions),
            degrees_honors = COALESCE(NULLIF(EXCLUDED.degrees_honors, ''), contributor_marketing_profiles.degrees_honors),
            professional_honors = COALESCE(NULLIF(EXCLUDED.professional_honors, ''), contributor_marketing_profiles.professional_honors),
            updated_at = now()
        """,
        (
            tenant_id, party_id, scope,
            values["website_bio"], values["book_bio"],
            values["contact_pref_rank1"], values["contact_pref_rank2"],
            values["media_best_times"], press_value,
            values["us_travel_plans"], values["travel_dates"],
            values["additional_notes"], values["photo_credit"],
            values["present_position"], values["former_positions"],
            values["degrees_honors"], values["professional_honors"],
            press_present,
        ),
    )


    # Questionnaire / marketing profile submissions are another legitimate input
    # path for these two contributor fields. Synchronize them into the master
    # party record, but never erase a canonical bio with an empty questionnaire field.
    if values["book_bio"] or values["website_bio"]:
        cur.execute(
            """
            UPDATE parties
            SET
                short_bio = CASE
                    WHEN %s <> '' THEN %s
                    ELSE short_bio
                END,
                long_bio = CASE
                    WHEN %s <> '' THEN %s
                    ELSE long_bio
                END,
                updated_at = now()
            WHERE tenant_id = %s
              AND id = %s
            """,
            (
                values["book_bio"], values["book_bio"],
                values["website_bio"], values["website_bio"],
                tenant_id, party_id,
            ),
        )

def _replace_advances(
    cur,
    tenant_id: str,
    royalty_set_id: str,
    author_party_id: Optional[str],
    illustrator_party_id: Optional[str],
    payload: Dict[str, Any],
) -> None:
    subtitle_note = _safe_str(payload.get("subtitle"))
    default_currency = _safe_str(payload.get("currency")) or "USD"

    royalties = payload.get("royalties") or {}
    author_roy = royalties.get("author") or {}
    illustrator_roy = royalties.get("illustrator") or {}

    rows = [
        (
            "author",
            author_party_id,
            _to_float_or_none(
                payload.get("author_advance")
                if payload.get("author_advance") not in (None, "")
                else author_roy.get("advance")
            ),
        ),
        (
            "illustrator",
            illustrator_party_id,
            _to_float_or_none(
                payload.get("illustrator_advance")
                if payload.get("illustrator_advance") not in (None, "")
                else illustrator_roy.get("advance")
            ),
        ),
    ]

    for party, party_id, amount in rows:
        if not party_id:
            continue

        cur.execute(
            """
            DELETE FROM advances
            WHERE tenant_id = %s
              AND royalty_set_id = %s
              AND party = %s
            """,
            (tenant_id, royalty_set_id, party),
        )

        if amount is None:
            continue

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
            VALUES (%s, %s, %s, %s, %s, %s, true, %s, %s)
            """,
            (
                str(uuid.uuid4()),
                tenant_id,
                royalty_set_id,
                party,
                amount,
                default_currency,
                subtitle_note,
                party_id,
            ),
        )


def _payload_nonempty_list(payload: Dict[str, Any], *keys: str) -> Optional[List[Dict[str, Any]]]:
    """Return the first non-empty list among keys; otherwise None (preserve existing)."""
    for key in keys:
        value = payload.get(key)
        if isinstance(value, list) and value:
            return [row for row in value if isinstance(row, dict)]
    return None


def _payload_has_nonempty_contact_category(payload: Dict[str, Any], scope: str) -> bool:
    for _category, aliases in _contact_category_specs_for_scope(scope.lower()):
        for key in aliases:
            value = payload.get(key)
            if isinstance(value, list) and any(isinstance(row, dict) for row in value):
                return True
    return False


def _explicit_party_id(cur, tenant_id: str, payload: Dict[str, Any], scope: str) -> Optional[str]:
    raw = payload.get(f"{scope}_party_id")
    if not raw:
        nested = payload.get(scope)
        if isinstance(nested, dict):
            raw = nested.get("party_id") or nested.get("id")
    if not raw:
        return None
    try:
        candidate = str(uuid.UUID(str(raw)))
    except (ValueError, TypeError):
        return None
    cur.execute(
        "SELECT id FROM parties WHERE tenant_id = %s AND id = %s LIMIT 1",
        (tenant_id, candidate),
    )
    row = cur.fetchone()
    return str(row["id"]) if row else None


def _upsert_work_from_payload(conn, cur, tenant_id: str, body: Dict[str, Any]) -> str:
    work_id = _resolve_work_id(cur, tenant_id, body)
    payload = dict(body or {})

    uid_val = payload.get("uid")
    try:
        uid_uuid = uuid.UUID(str(uid_val)) if uid_val else uuid.uuid4()
    except (ValueError, TypeError):
        uid_uuid = uuid.uuid4()

    title = _safe_str(payload.get("title"))
    subtitle = _safe_str(payload.get("subtitle"))
    series = _safe_str(payload.get("series") or payload.get("series_title"))
    series_num = int(payload.get("volume_number") or payload.get("series_number") or 0)
    language = _safe_str(payload.get("language"))
    rights = _safe_str(payload.get("rights"))
    main_desc = _safe_str(
        payload.get("main_description")
        or payload.get("description")
        or payload.get("book_description")
    )
    editor_name = _safe_str(payload.get("editor_name"))
    art_director = _safe_str(payload.get("art_director_name"))
    ages = _safe_str(payload.get("ages"))
    us_grade = _safe_str(payload.get("us_grade"))
    loc_number = _safe_str(payload.get("loc_number"))

    about_summary = _safe_str(payload.get("about_summary"))
    about_bookstore_shelf = _safe_str(payload.get("about_bookstore_shelf"))
    about_audience = _safe_str(payload.get("about_audience"))
    about_dates_holidays = _safe_str(payload.get("about_dates_holidays"))
    about_promotable_points = payload.get("about_promotable_points") or []
    if not isinstance(about_promotable_points, list):
        about_promotable_points = []

    about_diff_competitors = payload.get("about_diff_competitors") or []
    if not isinstance(about_diff_competitors, list):
        about_diff_competitors = []

    about_extra = _safe_str(payload.get("about_extra"))

    about_promotable_point_1 = _safe_str(about_promotable_points[0]) if len(about_promotable_points) > 0 else ""
    about_promotable_point_2 = _safe_str(about_promotable_points[1]) if len(about_promotable_points) > 1 else ""
    about_promotable_point_3 = _safe_str(about_promotable_points[2]) if len(about_promotable_points) > 2 else ""

    about_diff_competitor_1 = _safe_str(about_diff_competitors[0]) if len(about_diff_competitors) > 0 else ""
    about_diff_competitor_2 = _safe_str(about_diff_competitors[1]) if len(about_diff_competitors) > 1 else ""
    about_diff_competitor_3 = _safe_str(about_diff_competitors[2]) if len(about_diff_competitors) > 2 else ""

    if work_id:
        cur.execute(
            """
            UPDATE works
            SET
                uid = %s,
                title = %s,
                subtitle = %s,
                series_title = %s,
                series_number = %s,
                language = %s,
                rights = %s,
                main_description = %s,
                editor_name = %s,
                art_director_name = %s,
                ages = %s,
                us_grade = %s,
                loc_number = %s,
                about_summary = %s,
                about_bookstore_shelf = %s,
                about_audience = %s,
                about_dates_holidays = %s,
                about_promotable_point_1 = %s,
                about_promotable_point_2 = %s,
                about_promotable_point_3 = %s,
                about_diff_competitor_1 = %s,
                about_diff_competitor_2 = %s,
                about_diff_competitor_3 = %s,
                about_extra = %s,
                updated_at = now()
            WHERE tenant_id = %s
              AND id = %s
            """,
            (
                uid_uuid,
                title,
                subtitle,
                series,
                series_num,
                language,
                rights,
                main_desc,
                editor_name,
                art_director,
                ages,
                us_grade,
                loc_number,
                about_summary,
                about_bookstore_shelf,
                about_audience,
                about_dates_holidays,
                about_promotable_point_1,
                about_promotable_point_2,
                about_promotable_point_3,
                about_diff_competitor_1,
                about_diff_competitor_2,
                about_diff_competitor_3,
                about_extra,
                tenant_id,
                work_id,
            ),
        )
        cur.execute(
            """
            SELECT
                about_promotable_point_1,
                about_promotable_point_2,
                about_promotable_point_3,
                about_diff_competitor_1,
                about_diff_competitor_2,
                about_diff_competitor_3,
                about_extra
            FROM works
            WHERE tenant_id = %s
              AND id = %s
            """,
            (tenant_id, work_id),
        )
        print("ABOUT DB AFTER UPDATE", dict(cur.fetchone() or {}))

    else:
        work_id = str(uuid.uuid4())
        cur.execute(
            """
            INSERT INTO works (
                id, tenant_id, uid, title, subtitle, series_title, series_number,
                language, rights, main_description,
                editor_name, art_director_name, ages, us_grade, loc_number,
                about_summary, about_bookstore_shelf, about_audience, about_dates_holidays, about_promotable_point_1, about_promotable_point_2, about_promotable_point_3,
                about_diff_competitor_1, about_diff_competitor_2, about_diff_competitor_3,
                about_extra
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                work_id,
                tenant_id,
                uid_uuid,
                title,
                subtitle,
                series,
                series_num,
                language,
                rights,
                main_desc,
                editor_name,
                art_director,
                ages,
                us_grade,
                loc_number,
                about_summary,
                about_bookstore_shelf,
                about_audience,
                about_dates_holidays,
                about_promotable_point_1,
                about_promotable_point_2,
                about_promotable_point_3,
                about_diff_competitor_1,
                about_diff_competitor_2,
                about_diff_competitor_3,
                about_extra,
            ),
        )
        cur.execute(
            """
            SELECT
                about_promotable_point_1,
                about_promotable_point_2,
                about_promotable_point_3,
                about_diff_competitor_1,
                about_diff_competitor_2,
                about_diff_competitor_3,
                about_extra
            FROM works
            WHERE tenant_id = %s
              AND id = %s
            """,
            (tenant_id, work_id),
        )
        print("ABOUT DB AFTER INSERT", dict(cur.fetchone() or {}))

    _replace_editions(cur, tenant_id, work_id, payload)
    _replace_foreign_rights_sold(cur, tenant_id, work_id, payload)

    author_info = _contributor_input(payload, "author")
    illustrator_info = _contributor_input(payload, "illustrator")

    author_party_id = _explicit_party_id(cur, tenant_id, payload, "author")
    if not author_party_id:
        author_party_id = _get_or_create_party(
            cur, tenant_id,
            _safe_str(author_info.get("name")) or "Unknown",
            email=_safe_str(author_info.get("email")),
            party_type="person",
        )

    illustrator_party_id: Optional[str] = _explicit_party_id(cur, tenant_id, payload, "illustrator")
    if not illustrator_party_id and _has_real_contributor(payload, "illustrator"):
        illustrator_party_id = _get_or_create_party(
            cur, tenant_id,
            _safe_str(illustrator_info.get("name")) or "Unknown",
            email=_safe_str(illustrator_info.get("email")),
            party_type="person",
        )

    # Replace only the legacy author/illustrator assignments. Preserve translators,
    # narrators, editors, and all future contributor roles.
    cur.execute(
        """
        DELETE FROM work_contributors
        WHERE tenant_id = %s AND work_id = %s
          AND upper(trim(contributor_role)) IN ('AUTHOR', 'A01', 'ILLUSTRATOR', 'A12')
        """,
        (tenant_id, work_id),
    )

    cur.execute(
        """
        INSERT INTO work_contributors (
            tenant_id, work_id, party_id, contributor_role, sequence_number
        )
        VALUES (%s, %s, %s, 'AUTHOR', 1)
        """,
        (tenant_id, work_id, author_party_id),
    )

    if illustrator_party_id:
        cur.execute(
            """
            INSERT INTO work_contributors (
                tenant_id, work_id, party_id, contributor_role, sequence_number
            )
            VALUES (%s, %s, %s, 'ILLUSTRATOR', 2)
            """,
            (tenant_id, work_id, illustrator_party_id),
        )

    contributor_targets: List[Tuple[str, str]] = [(author_party_id, "author")]
    if illustrator_party_id:
        contributor_targets.append((illustrator_party_id, "illustrator"))

    for party_id, scope in contributor_targets:
        _upsert_party_core(cur, tenant_id, party_id, payload, scope)
        _update_party_identity_extensions(cur, tenant_id, party_id, payload, scope)
        _replace_party_awards(cur, tenant_id, party_id, payload, scope)
        _replace_party_identifiers(cur, tenant_id, party_id, payload, scope)
        _upsert_contributor_profile(cur, tenant_id, party_id, scope, payload)

        socials = _payload_nonempty_list(payload, f"{scope}_socials")
        if socials is not None:
            _replace_party_socials(cur, tenant_id, party_id, {f"{scope}_socials": socials}, scope)

        if _payload_has_nonempty_contact_category(payload, scope):
            _replace_contributor_contact_categories(cur, tenant_id, party_id, scope, payload)

        published = _payload_nonempty_list(payload, f"{scope}_books_published", f"{scope}_published_books")
        if published is not None:
            _replace_contributor_published_books(
                cur, tenant_id, party_id, {f"{scope}_books_published": published}, scope
            )

        appearances = _payload_nonempty_list(payload, f"{scope}_media_appearances")
        if appearances is not None:
            _replace_contributor_media_appearances(
                cur, tenant_id, party_id, {f"{scope}_media_appearances": appearances}, scope
            )

        media_contacts = _payload_nonempty_list(payload, f"{scope}_media_contacts")
        if media_contacts is not None:
            _replace_contributor_media_contacts(
                cur, tenant_id, party_id, {f"{scope}_media_contacts": media_contacts}, scope
            )

        other_pubs = _payload_nonempty_list(payload, f"{scope}_other_publications")
        if other_pubs is not None:
            _replace_contributor_other_publications(
                cur, tenant_id, party_id, other_pubs, scope=scope
            )

        previous_publicity = _payload_nonempty_list(
            payload,
            f"{scope}_previous_publicity",
            f"{scope}_marketing_previous_book_publicity",
            f"{scope}_publicity_previous_book_publicity",
        )
        if previous_publicity is not None:
            _replace_contributor_previous_publicity(
                cur, tenant_id, party_id, scope, previous_publicity
            )

        # FIX: only trust the actual UI niche fields, never the stale legacy
        # *_niche_publicity_targets payload because it is polluted by old data.
        raw_niche_rows = payload.get(f"{scope}_marketing_niche_publicity")
        if not isinstance(raw_niche_rows, list):
            raw_niche_rows = payload.get(f"{scope}_publicity_niche_marketing")
        if not isinstance(raw_niche_rows, list):
            raw_niche_rows = []

        niche_rows: List[Dict[str, Any]] = []
        for item in raw_niche_rows:
            if not isinstance(item, dict):
                continue

            area = _safe_str(item.get("area"))
            notes = _safe_str(item.get("notes"))

            if not area and not notes:
                continue

            niche_rows.append(
                {
                    "area": area,
                    "notes": notes,
                    "source_category": "niche_publicity",
                }
            )

        if niche_rows:
            _replace_contributor_niche_targets(cur, tenant_id, party_id, scope, niche_rows)

        cur.execute(
            """
            INSERT INTO work_party_preferences (
                tenant_id, work_id, party_id,
                contact_pref_rank1, contact_pref_rank2,
                media_best_times, media_press_share,
                us_travel_plans, travel_dates
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (work_id, party_id)
            DO UPDATE SET
                contact_pref_rank1 = EXCLUDED.contact_pref_rank1,
                contact_pref_rank2 = EXCLUDED.contact_pref_rank2,
                media_best_times = EXCLUDED.media_best_times,
                media_press_share = EXCLUDED.media_press_share,
                us_travel_plans = EXCLUDED.us_travel_plans,
                travel_dates = EXCLUDED.travel_dates
            """,
            (
                tenant_id,
                work_id,
                party_id,
                _safe_str(payload.get(f"{scope}_contact_pref_rank1")),
                _safe_str(payload.get(f"{scope}_contact_pref_rank2")),
                _safe_str(payload.get(f"{scope}_media_best_times")),
                bool(payload.get(f"{scope}_media_press_share") or False),
                _safe_str(payload.get(f"{scope}_us_travel_plans")),
                _safe_str(payload.get(f"{scope}_travel_dates")),
            ),
        )

        agency_payload = _agency_payload_from_input(payload, scope)
        if agency_payload:
            _replace_party_representation(
                cur, tenant_id, party_id, work_id, agency_payload
            )

    royalties = payload.get("royalties") or {}
    if not isinstance(royalties, dict):
        royalties = {}

    author_roy = royalties.get("author") or {}
    illustrator_roy = royalties.get("illustrator") or {}

    if not isinstance(author_roy, dict):
        author_roy = {}
    if not isinstance(illustrator_roy, dict):
        illustrator_roy = {}

    if author_roy or illustrator_roy:
        set_id = _get_royalty_set_for_write(cur, tenant_id, work_id)
        _clear_royalty_graph_for_set(cur, tenant_id, set_id)

        for r in (author_roy.get("first_rights") or []):
            if isinstance(r, dict):
                _insert_royalty_rule(cur, tenant_id, set_id, "author", "first_rights", r)

        for r in (author_roy.get("subrights") or []):
            if isinstance(r, dict):
                _insert_royalty_rule(cur, tenant_id, set_id, "author", "subrights", r)

        for r in (illustrator_roy.get("first_rights") or []):
            if isinstance(r, dict):
                _insert_royalty_rule(
                    cur, tenant_id, set_id, "illustrator", "first_rights", r
                )

        for r in (illustrator_roy.get("subrights") or []):
            if isinstance(r, dict):
                _insert_royalty_rule(
                    cur, tenant_id, set_id, "illustrator", "subrights", r
                )
        _replace_advances(
            cur,
            tenant_id,
            set_id,
            author_party_id,
            illustrator_party_id,
            payload,
        )

    return work_id

def update_metadata_assistant_question_state(
    cur,
    tenant_id: str,
    work_id: str,
    edition_id: str,
    payload: Dict[str, Any],
) -> Dict[str, Any]:
    """Persist Metadata Completion Assistant workflow state.

    Actual metadata answers continue to be saved through their owning card
    endpoints. This table stores only assistant workflow decisions:
    - not_applicable
    - deferred
    """

    try:
        edition_uuid = str(uuid.UUID(str(edition_id)))
    except (ValueError, TypeError):
        raise ValueError("Invalid edition id")

    cur.execute(
        """
        SELECT 1
        FROM editions
        WHERE tenant_id = %s
          AND work_id = %s
          AND id = %s
        LIMIT 1
        """,
        (tenant_id, work_id, edition_uuid),
    )
    if not cur.fetchone():
        raise ValueError("Edition not found")

    clear_all = bool(
        payload.get("clear_all")
        or payload.get("clearAll")
    )
    clear_deferred = bool(
        payload.get("clear_deferred")
        or payload.get("clearDeferred")
    )

    if clear_all:
        cur.execute(
            """
            DELETE FROM metadata_assistant_question_state
            WHERE tenant_id = %s
              AND work_id = %s
              AND edition_id = %s
            """,
            (tenant_id, work_id, edition_uuid),
        )
        return {
            "ok": True,
            "cleared": "all",
        }

    if clear_deferred:
        cur.execute(
            """
            DELETE FROM metadata_assistant_question_state
            WHERE tenant_id = %s
              AND work_id = %s
              AND edition_id = %s
              AND status = 'deferred'
            """,
            (tenant_id, work_id, edition_uuid),
        )
        return {
            "ok": True,
            "cleared": "deferred",
        }

    question_key = _safe_str(
        payload.get("question_key")
        or payload.get("questionKey")
    )
    status = _safe_str(payload.get("status")).lower()

    if not question_key:
        raise ValueError("question_key is required")

    if status in ("", "clear", "answered"):
        cur.execute(
            """
            DELETE FROM metadata_assistant_question_state
            WHERE tenant_id = %s
              AND work_id = %s
              AND edition_id = %s
              AND question_key = %s
            """,
            (
                tenant_id,
                work_id,
                edition_uuid,
                question_key,
            ),
        )
        return {
            "ok": True,
            "question_key": question_key,
            "status": "",
        }

    if status not in (
        "not_applicable",
        "deferred",
    ):
        raise ValueError(
            "status must be 'not_applicable', 'deferred', or 'clear'"
        )

    cur.execute(
        """
        INSERT INTO metadata_assistant_question_state (
            tenant_id,
            work_id,
            edition_id,
            question_key,
            status,
            updated_at
        )
        VALUES (%s, %s, %s, %s, %s, now())
        ON CONFLICT (
            tenant_id,
            work_id,
            edition_id,
            question_key
        )
        DO UPDATE SET
            status = EXCLUDED.status,
            updated_at = now()
        RETURNING
            id,
            question_key,
            status,
            created_at,
            updated_at
        """,
        (
            tenant_id,
            work_id,
            edition_uuid,
            question_key,
            status,
        ),
    )

    row = cur.fetchone() or {}

    return {
        "ok": True,
        "question_key": _safe_str(
            row.get("question_key")
        ),
        "status": _safe_str(row.get("status")),
    }

