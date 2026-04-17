from __future__ import annotations

import uuid
from datetime import date, datetime
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
    long_bio = _safe_str(
        payload.get(f"{scope}_website_bio") or payload.get(f"{scope}_long_bio")
    )

    contributor_name = _safe_name(info.get("name"))

    cur.execute(
        """
        UPDATE parties
        SET
            display_name = CASE
                WHEN %s <> '' THEN %s
                ELSE display_name
            END,
            email = %s,
            website = %s,
            phone_country_code = %s,
            phone_number = %s,
            birth_city = %s,
            birth_country = %s,
            birth_date = %s,
            citizenship = %s,
            short_bio = %s,
            long_bio = %s,
            updated_at = now()
        WHERE tenant_id = %s
          AND id = %s
        """,
        (
            contributor_name,
            contributor_name,
            email,
            website,
            phone_country_code,
            phone_number,
            birth_city,
            birth_country,
            birth_date,
            citizenship,
            short_bio,
            long_bio,
            tenant_id,
            party_id,
        ),
    )

    address_obj = info.get("address") if isinstance(info.get("address"), dict) else {}
    if isinstance(address_obj, dict):
        try:
            cur.execute(
                "DELETE FROM party_addresses WHERE tenant_id = %s AND party_id = %s",
                (tenant_id, party_id),
            )
            _insert_party_address(cur, tenant_id, party_id, address_obj)
        except Exception:
            pass


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

    street = _safe_str(row.get("street"))
    city = _safe_str(row.get("city"))
    state = _safe_str(row.get("state"))
    postal = _safe_str(row.get("zip") or row.get("postal_code"))
    country = _safe_str(row.get("country"))
    social_handle = _safe_str(row.get("social_handle"))
    personal = bool(row.get("personal_contact") or False)
    link_type = _safe_str(row.get("link_type"))

    name = ""
    company = ""
    position = ""
    email = ""
    phone = ""
    website = ""
    notes = ""
    rel_note = ""

    if category_lc.endswith("_marketing_endorsers"):
        name = _safe_str(row.get("name"))
        raw_contact = _safe_str(row.get("contact"))

        company = ""
        position = ""
        email = _safe_str(row.get("email"))
        phone = _safe_str(row.get("phone"))
        website = _safe_str(row.get("url") or row.get("website"))
        notes = _safe_str(row.get("notes"))
        rel_note = _safe_str(
            row.get("relationship") or row.get("connection") or row.get("relationship_note")
        )

        # The UX has a single generic "Contact" field.
        # Preserve it intelligently instead of dropping it.
        if raw_contact:
            lc = raw_contact.lower()
            if "@" in raw_contact and not email:
                email = raw_contact
            elif raw_contact.startswith("http://") or raw_contact.startswith("https://") or raw_contact.startswith("www."):
                if not website:
                    website = raw_contact
            elif any(ch.isdigit() for ch in raw_contact) and not phone:
                phone = raw_contact
            else:
                # non-email / non-url / non-phone text like organization or affiliation
                company = raw_contact

    elif category_lc.endswith("_marketing_big_mouth_list"):
        name = _safe_str(row.get("name"))
        company = ""
        position = ""
        email = _safe_str(row.get("email"))
        phone = _safe_str(row.get("phone") or row.get("contact"))
        website = _safe_str(row.get("url") or row.get("website"))
        notes = _safe_str(row.get("notes"))
        rel_note = _safe_str(row.get("relationship"))

    elif category_lc.endswith("_marketing_review_copy_wishlist"):
        name = _safe_str(row.get("contact"))
        company = _safe_str(row.get("outlet"))
        position = ""
        email = _safe_str(row.get("email"))
        phone = _safe_str(row.get("phone"))
        website = _safe_str(row.get("url") or row.get("website"))
        notes = _safe_str(row.get("notes"))
        rel_note = _safe_str(row.get("connection"))

    elif category_lc.endswith("_marketing_local_media"):
        name = _safe_str(row.get("contact"))
        company = _safe_str(row.get("outlet") or row.get("company"))
        position = _safe_str(row.get("position"))
        email = _safe_str(row.get("email"))
        phone = _safe_str(row.get("phone"))
        website = _safe_str(row.get("url") or row.get("website"))
        notes = _safe_str(row.get("notes"))
        rel_note = _safe_str(
            row.get("relationship") or row.get("connection") or row.get("relationship_note")
        )

    elif category_lc.endswith("_marketing_alumni_org_publications"):
        name = _safe_str(row.get("contact"))
        company = _safe_str(row.get("outlet") or row.get("publication") or row.get("company"))
        position = ""
        email = _safe_str(row.get("email"))
        phone = _safe_str(row.get("phone"))
        website = _safe_str(row.get("url") or row.get("website"))
        notes = _safe_str(row.get("notes"))
        rel_note = ""

    elif category_lc.endswith("_marketing_targeted_sites"):
        name = _safe_str(row.get("name"))
        company = ""
        position = ""
        email = _safe_str(row.get("email"))
        phone = _safe_str(row.get("phone"))
        website = _safe_str(row.get("url") or row.get("website"))
        notes = _safe_str(row.get("notes"))
        rel_note = _safe_str(row.get("contact"))

    elif category_lc.endswith("_marketing_bloggers"):
        name = _safe_str(row.get("name"))
        company = ""
        position = ""
        email = _safe_str(row.get("email"))
        phone = _safe_str(row.get("phone"))
        website = _safe_str(row.get("url") or row.get("website"))
        notes = _safe_str(row.get("notes"))
        rel_note = _safe_str(row.get("contact"))

    elif category_lc.endswith("_sales_local_bookstores"):
        name = _safe_str(row.get("name"))
        company = _safe_str(row.get("chain_name"))
        position = _safe_str(row.get("kind"))
        email = _safe_str(row.get("email"))
        phone = _safe_str(row.get("phone"))
        website = _safe_str(row.get("url") or row.get("website"))
        notes = _safe_str(row.get("notes"))
        rel_note = _safe_str(row.get("contact"))

    elif category_lc.endswith("_sales_schools_libraries"):
        name = _safe_str(row.get("name"))
        company = ""
        position = _safe_str(row.get("kind"))
        email = _safe_str(row.get("email"))
        phone = _safe_str(row.get("phone"))
        website = _safe_str(row.get("url") or row.get("website"))
        notes = _safe_str(row.get("notes"))
        rel_note = _safe_str(row.get("contact"))
    
    elif category_lc.endswith("_sales_societies_orgs_conf"):
        name = _safe_str(row.get("name"))
        company = ""
        position = _safe_str(row.get("kind"))
        email = _safe_str(row.get("email"))
        phone = _safe_str(row.get("phone"))
        website = _safe_str(row.get("url") or row.get("website"))
        notes = _safe_str(row.get("notes"))
        rel_note = _safe_str(row.get("contact"))
    
    elif category_lc.endswith("_sales_nontrade_outlets"):
        name = _safe_str(row.get("name"))
        company = ""
        position = _safe_str(row.get("category"))
        email = _safe_str(row.get("email"))
        phone = _safe_str(row.get("phone"))
        website = _safe_str(row.get("url") or row.get("website"))
        notes = _safe_str(row.get("notes"))
        rel_note = _safe_str(row.get("contact"))
    
    elif category_lc.endswith("_sales_museums_parks"):
        name = _safe_str(row.get("name"))
        company = _safe_str(row.get("connection"))
        position = _safe_str(row.get("kind"))
        email = _safe_str(row.get("email"))
        phone = _safe_str(row.get("phone"))
        website = _safe_str(row.get("url") or row.get("website"))
        notes = _safe_str(row.get("notes"))
        rel_note = _safe_str(row.get("contact"))
    


    else:
        name = _safe_str(row.get("contact") or row.get("name"))
        company = _safe_str(row.get("name") or row.get("outlet") or row.get("company"))
        if not name and company:
            name, company = company, ""
        position = _safe_str(row.get("position"))
        email = _safe_str(row.get("email"))
        phone = _safe_str(row.get("phone"))
        website = _safe_str(row.get("url") or row.get("website"))
        notes = _safe_str(row.get("notes"))
        rel_note = _safe_str(
            row.get("relationship") or row.get("connection") or row.get("relationship_note")
        )


    if not any(
        [
            name,
            company,
            position,
            email,
            phone,
            website,
            street,
            city,
            state,
            postal,
            country,
            social_handle,
            notes,
            rel_note,
        ]
    ):
        return

    cd_vals = (
        cd_id,
        tenant_id,
        "",
        name,
        company,
        position,
        email,
        phone,
        website,
        street,
        city,
        state,
        postal,
        country,
        social_handle,
        notes,
    )

    cd_ok = False
    for postal_col in ("postal_code", "zip"):
        try:
            cur.execute(
                f"""
                INSERT INTO contact_directory (
                    id, tenant_id, contact_type, name, company_or_outlet, position, email, phone,
                    website, street, city, state, {postal_col}, country, social_handle, notes
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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
                id, tenant_id, party_id, scope, category, link_type, item_order,
                personal_contact, relationship_note, contact_id
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                link_id,
                tenant_id,
                party_id,
                scope_lc,
                category,
                link_type,
                item_order,
                personal,
                rel_note,
                cd_id,
            ),
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
    scope_lc = (scope or "author").lower()
    try:
        cur.execute(
            """
            DELETE FROM contributor_contact_links
            WHERE tenant_id = %s AND party_id = %s AND lower(scope) = %s
            """,
            (tenant_id, party_id, scope_lc),
        )
    except Exception:
        return

    for category, alias_keys in _contact_category_specs_for_scope(scope_lc):
        rows = _first_non_empty_dict_list(payload, alias_keys)
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


def _replace_editions(cur, tenant_id: str, work_id: str, payload: Dict[str, Any]) -> None:
    incoming_formats = payload.get("formats") or []
    if not isinstance(incoming_formats, list):
        incoming_formats = []

    seen_isbns: List[str] = []

    for fmt in incoming_formats:
        if not isinstance(fmt, dict):
            continue

        isbn13 = _normalize_isbn13(
            fmt.get("ISBN") or fmt.get("isbn") or fmt.get("isbn13")
        )
        if not isbn13:
            continue

        seen_isbns.append(isbn13)

        product_form = _safe_str(fmt.get("format"))
        publication_date = _parse_date_or_none(fmt.get("pub_date"))
        number_of_pages = _to_int_or_none(fmt.get("pages"))
        height = _to_float_or_none(fmt.get("tall"))
        width = _to_float_or_none(fmt.get("wide"))
        thickness = _to_float_or_none(fmt.get("spine"))
        unit_weight = _to_float_or_none(fmt.get("weight"))

        price_us = _to_float_or_none(fmt.get("price_us"))
        price_can = _to_float_or_none(fmt.get("price_can"))

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

        if existing:
            edition_id = existing["id"]
            cur.execute(
                """
                UPDATE editions
                SET
                    product_form = %s,
                    product_form_detail = %s,
                    publication_date = %s,
                    number_of_pages = %s,
                    height = %s,
                    height_unit = 'in',
                    width = %s,
                    width_unit = 'in',
                    thickness = %s,
                    thickness_unit = 'in',
                    unit_weight = %s,
                    unit_weight_unit = 'lb',
                    updated_at = now()
                WHERE tenant_id = %s
                  AND id = %s
                """,
                (
                    product_form,
                    product_form,
                    publication_date,
                    number_of_pages,
                    height,
                    width,
                    thickness,
                    unit_weight,
                    tenant_id,
                    edition_id,
                ),
            )
        else:
            cur.execute(
                """
                INSERT INTO editions (
                    tenant_id, work_id, isbn13, status, product_form, product_form_detail,
                    publication_date, number_of_pages,
                    height, height_unit,
                    width, width_unit,
                    thickness, thickness_unit,
                    unit_weight, unit_weight_unit
                )
                VALUES (%s, %s, %s, 'planned', %s, %s, %s, %s, %s, 'in', %s, 'in', %s, 'in', %s, 'lb')
                RETURNING id
                """,
                (
                    tenant_id,
                    work_id,
                    isbn13,
                    product_form,
                    product_form,
                    publication_date,
                    number_of_pages,
                    height,
                    width,
                    thickness,
                    unit_weight,
                ),
            )
            edition_row = cur.fetchone()
            edition_id = edition_row and edition_row.get("id")

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
    cur.execute(
        """
        INSERT INTO contributor_marketing_profiles (
            tenant_id, party_id, scope,
            website_bio, book_bio,
            contact_pref_rank1, contact_pref_rank2,
            media_best_times, media_press_share,
            us_travel_plans, travel_dates,
            additional_notes,
            photo_credit,
            present_position,
            former_positions,
            degrees_honors,
            professional_honors
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (tenant_id, party_id, scope)
        DO UPDATE SET
            website_bio = EXCLUDED.website_bio,
            book_bio = EXCLUDED.book_bio,
            contact_pref_rank1 = EXCLUDED.contact_pref_rank1,
            contact_pref_rank2 = EXCLUDED.contact_pref_rank2,
            media_best_times = EXCLUDED.media_best_times,
            media_press_share = EXCLUDED.media_press_share,
            us_travel_plans = EXCLUDED.us_travel_plans,
            travel_dates = EXCLUDED.travel_dates,
            additional_notes = EXCLUDED.additional_notes,
            photo_credit = EXCLUDED.photo_credit,
            present_position = EXCLUDED.present_position,
            former_positions = EXCLUDED.former_positions,
            degrees_honors = EXCLUDED.degrees_honors,
            professional_honors = EXCLUDED.professional_honors,
            updated_at = now()
        """,
        (
            tenant_id,
            party_id,
            scope,
            _safe_str(payload.get(f"{scope}_website_bio") or payload.get(f"{scope}_long_bio")),
            _safe_str(payload.get(f"{scope}_book_bio") or payload.get(f"{scope}_bio")),
            _safe_str(payload.get(f"{scope}_contact_pref_rank1")),
            _safe_str(payload.get(f"{scope}_contact_pref_rank2")),
            _safe_str(payload.get(f"{scope}_media_best_times")),
            bool(payload.get(f"{scope}_media_press_share") or False),
            _safe_str(payload.get(f"{scope}_us_travel_plans")),
            _safe_str(payload.get(f"{scope}_travel_dates")),
            _safe_str(
                payload.get(f"{scope}_additional_notes")
                or payload.get(f"{scope}_marketing_additional_notes")
                or payload.get(f"{scope}_publicity_additional_notes")
            ),
            _safe_str(payload.get(f"{scope}_photo_credit")),
            _safe_str(payload.get(f"{scope}_present_position")),
            _safe_str(payload.get(f"{scope}_former_positions")),
            _safe_str(payload.get(f"{scope}_degrees_honors")),
            _safe_str(payload.get(f"{scope}_professional_honors")),
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
    publisher_or_imprint = _safe_str(payload.get("publisher_or_imprint"))
    pub_year = _to_int_or_none(payload.get("publishing_year"))
    language = _safe_str(payload.get("language"))
    rights = _safe_str(payload.get("rights"))
    main_desc = _safe_str(
        payload.get("main_description")
        or payload.get("description")
        or payload.get("book_description")
    )
    cover_link = _safe_str(payload.get("cover_image_link"))
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
                publisher_or_imprint = %s,
                publishing_year = %s,
                language = %s,
                rights = %s,
                main_description = %s,
                cover_image_link = %s,
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
                publisher_or_imprint,
                pub_year,
                language,
                rights,
                main_desc,
                cover_link,
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
                publisher_or_imprint, publishing_year, language, rights, main_description,
                cover_image_link, editor_name, art_director_name, ages, us_grade, loc_number,
                about_summary, about_bookstore_shelf, about_audience, about_dates_holidays, about_promotable_point_1, about_promotable_point_2, about_promotable_point_3,
                about_diff_competitor_1, about_diff_competitor_2, about_diff_competitor_3,
                about_extra
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                work_id,
                tenant_id,
                uid_uuid,
                title,
                subtitle,
                series,
                series_num,
                publisher_or_imprint,
                pub_year,
                language,
                rights,
                main_desc,
                cover_link,
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

    author_party_id = _get_or_create_party(
        cur,
        tenant_id,
        _safe_str(author_info.get("name")) or "Unknown",
        email=_safe_str(author_info.get("email")),
        party_type="person",
    )

    illustrator_party_id: Optional[str] = None
    if _has_real_contributor(payload, "illustrator"):
        illustrator_party_id = _get_or_create_party(
            cur,
            tenant_id,
            _safe_str(illustrator_info.get("name")) or "Unknown",
            email=_safe_str(illustrator_info.get("email")),
            party_type="person",
        )

    cur.execute(
        "DELETE FROM work_contributors WHERE tenant_id = %s AND work_id = %s",
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
        _upsert_contributor_profile(cur, tenant_id, party_id, scope, payload)
        _replace_party_socials(cur, tenant_id, party_id, payload, scope)
        _replace_contributor_contact_categories(cur, tenant_id, party_id, scope, payload)
        _replace_contributor_published_books(cur, tenant_id, party_id, payload, scope)
        _replace_contributor_media_appearances(cur, tenant_id, party_id, payload, scope)
        _replace_contributor_media_contacts(cur, tenant_id, party_id, payload, scope)

        rows = payload.get(f"{scope}_other_publications") or []
        _replace_contributor_other_publications(
            cur,
            tenant_id,
            party_id,
            rows,
            scope=scope,
        )

        _replace_contributor_previous_publicity(
            cur,
            tenant_id,
            party_id,
            scope,
            payload.get(f"{scope}_previous_publicity")
            or payload.get(f"{scope}_marketing_previous_book_publicity")
            or payload.get(f"{scope}_publicity_previous_book_publicity")
            or [],
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