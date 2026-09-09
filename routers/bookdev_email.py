# app/routers/bookdev_email.py
from __future__ import annotations

import hashlib
import json
import os
import secrets
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional
from typing import List

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from pydantic import BaseModel, EmailStr, validator
from psycopg.rows import dict_row

from app.core.db import db_conn
from routers.contract_invites import (
    _load_smtp_secret,
    _load_tenant_email_settings_or_400,
    _send_email_smtp,
)
from routers.catalog import _fetch_party_address
from routers.catalog_write import (
    _get_or_create_party,
    _upsert_party_core,
    _replace_party_socials,
)

router = APIRouter(
    prefix="/project-management/book-development",
    tags=["Book Development Requests"],
)

SUPPORTED_REQUEST_TYPES = {
    "CONTRIBUTOR_INFO",
    "AUTHOR_PHOTO",
    "ILLUSTRATOR_PHOTO",
    "MEDIA_QUESTIONNAIRE",
    "MARKETING_PROFILE",
    "SALES_INFORMATION",
}

class ProjectTimelineItemIn(BaseModel):
    task_name: str
    deadline: str
    sort_order: int = 0

    @validator("task_name", pre=True)
    def clean_task_name(cls, value):
        return str(value or "").strip()

    @validator("deadline", pre=True)
    def validate_deadline(cls, value):
        raw = str(value or "").strip()
        try:
            datetime.strptime(raw, "%m/%d/%Y")
        except ValueError:
            raise ValueError("deadline must use MM/DD/YYYY format")
        return raw


class ProjectTimelineSaveIn(BaseModel):
    items: list[ProjectTimelineItemIn] = []


def _timeline_row_to_dict(row: Dict[str, Any]) -> Dict[str, Any]:
    deadline = row.get("deadline")
    if hasattr(deadline, "strftime"):
        deadline_text = deadline.strftime("%m/%d/%Y")
    else:
        deadline_text = _safe(deadline)

    return {
        "id": _safe(row.get("id")),
        "task_name": _safe(row.get("task_name")),
        "deadline": deadline_text,
        "sort_order": int(row.get("sort_order") or 0),
    }


class DashboardSaveItemIn(BaseModel):
    work_id: str
    pinned: bool = False
    sort_order: int = 0

class DashboardSaveIn(BaseModel):
    items: list[DashboardSaveItemIn] = []


class BookDevRequestIn(BaseModel):
    request_type: str = "CONTRIBUTOR_INFO"
    party: str = "author"
    recipient_name: str = ""
    recipient_email: EmailStr
    requester_email: Optional[EmailStr] = None
    message: str = ""

    # Exact contributor identity for CONTRIBUTOR_INFO requests.
    contributor_party_id: Optional[str] = None
    contributor_role_code: Optional[str] = None
    contributor_role_label: Optional[str] = None
    contributor_sequence_number: Optional[int] = None

    # The frontend also sends the same values in a nested contributor object.
    contributor: Optional[Dict[str, Any]] = None

    class Config:
        extra = "ignore"


class BookDevPhotoSubmitIn(BaseModel):
    kind: str = ""
    filename: str = ""
    url: str = ""
    key: str = ""
    mime: str = ""
    size: int = 0
    width: Optional[int] = None
    height: Optional[int] = None


class MediaQuestionnaireSubmitIn(BaseModel):
    media_press_share: Optional[bool] = None
    books_published: list[Dict[str, Any]] = []
    other_publications: list[Dict[str, Any]] = []
    media_appearances: list[Dict[str, Any]] = []
    media_contacts: list[Dict[str, Any]] = []
    book_bio: Optional[str] = None
    website_bio: Optional[str] = None
    contact_pref_rank1: Optional[str] = None
    contact_pref_rank2: Optional[str] = None
    media_best_times: Optional[str] = None
    us_travel_plans: Optional[str] = None
    travel_dates: Optional[str] = None
    additional_notes: Optional[str] = None
    photo_credit: Optional[str] = None
    present_position: Optional[str] = None
    former_positions: Optional[str] = None
    degrees_honors: Optional[str] = None
    professional_honors: Optional[str] = None

    class Config:
        extra = "ignore"

    @validator("*", pre=True)
    def empty_string_to_none(cls, v):
        if isinstance(v, str):
            v = v.strip()
            return None if v == "" else v
        return v



class ContributorInfoSubmitIn(BaseModel):
    # Public form field names currently sent by the frontend
    contributor_name: Optional[str] = None
    contributor_display_name: Optional[str] = None
    contributor_email: Optional[str] = None
    contributor_phone_country_code: Optional[str] = None
    contributor_phone_number: Optional[str] = None
    contributor_website: Optional[str] = None

    contributor_address_street: Optional[str] = None
    contributor_address_city: Optional[str] = None
    contributor_address_state: Optional[str] = None
    contributor_address_zip: Optional[str] = None
    contributor_address_country: Optional[str] = None

    contributor_citizenship: Optional[str] = None
    contributor_birth_date: Optional[str] = None
    contributor_birth_city: Optional[str] = None
    contributor_birth_country: Optional[str] = None

    social_media: Optional[Dict[str, Any]] = None
    short_bio: Optional[str] = None
    long_bio: Optional[str] = None

    has_agent: bool = False
    agency_name: Optional[str] = None
    agency_email: Optional[str] = None
    agency_website: Optional[str] = None
    agency_street: Optional[str] = None
    agency_city: Optional[str] = None
    agency_state: Optional[str] = None
    agency_zip: Optional[str] = None
    agency_country: Optional[str] = None

    agent_name: Optional[str] = None
    agent_email: Optional[str] = None
    agent_phone_country_code: Optional[str] = None
    agent_phone_number: Optional[str] = None

    # Older/backend names kept for compatibility with earlier snippets
    contributor_short_bio: Optional[str] = None
    contributor_long_bio: Optional[str] = None
    agency_phone_country_code: Optional[str] = None
    agency_phone_number: Optional[str] = None
    agency_address_line1: Optional[str] = None
    agency_address_line2: Optional[str] = None
    agency_postal_code: Optional[str] = None
    agent_display_name: Optional[str] = None
    notes: Optional[str] = None

    class Config:
        extra = "ignore"

    @validator("*", pre=True)
    def empty_string_to_none(cls, v):
        if isinstance(v, str):
            v = v.strip()
            if v == "":
                return None
        return v

    @validator(
        "contributor_email",
        "agency_email",
        "agent_email",
        pre=False,
        always=False,
    )
    def validate_email_if_present(cls, v):
        # Avoid FastAPI 422 on optional blank/null emails, but still reject obvious bad emails.
        if v in (None, ""):
            return None
        s = str(v).strip()
        if "@" not in s or s.startswith("@") or s.endswith("@"):
            raise ValueError("Invalid email address")
        return s


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _expires_at(days: int = 14) -> datetime:
    return _now_utc() + timedelta(days=days)


def _safe(v: Any) -> str:
    return str(v or "").strip()


def _token_hash(raw_token: str) -> str:
    return hashlib.sha256(raw_token.encode("utf-8")).hexdigest()


def _frontend_base_url() -> str:
    return (os.getenv("FRONTEND_BASE_URL") or "https://www.inksuite.io").rstrip("/")


def _token_from_request(request: Request) -> Optional[str]:
    auth = (request.headers.get("Authorization") or "").strip()
    if auth.lower().startswith("bearer "):
        return auth[7:].strip() or None

    cookie = request.headers.get("Cookie") or ""
    for part in cookie.split(";"):
        part = part.strip()
        if part.lower().startswith("access_token="):
            return part[13:].strip() or None

    return None


def _ctx_from_bearer(request: Request):
    claims = getattr(request.state, "user_claims", None)
    if claims:
        return claims

    token = _token_from_request(request)
    if not token:
        raise HTTPException(status_code=401, detail="Not authenticated")

    try:
        from app.auth.service import get_current_user_from_token

        claims = get_current_user_from_token(token)
    except Exception:
        claims = None

    if not claims:
        raise HTTPException(status_code=401, detail="Not authenticated")

    return claims


def _resolve_ctx_cognito_sub(ctx: Any) -> Optional[str]:
    if isinstance(ctx, dict):
        if ctx.get("sub"):
            return str(ctx["sub"])

        user = ctx.get("user") or {}
        if isinstance(user, dict):
            for key in ("sub", "cognito_sub", "id", "user_id"):
                if user.get(key):
                    return str(user[key])

    try:
        sub = getattr(ctx, "sub", None)
        return str(sub) if sub else None
    except Exception:
        return None


def _load_user_and_membership_or_403(*, tenant_slug: str, ctx: Any) -> Dict[str, Any]:
    sub = _resolve_ctx_cognito_sub(ctx)
    if not sub:
        raise HTTPException(status_code=401, detail="Not authenticated")

    with db_conn() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute("SELECT id FROM users WHERE cognito_sub = %s", (sub,))
            user = cur.fetchone()
            if not user:
                raise HTTPException(status_code=401, detail="User not found")

            user_id = str(user["id"])

            cur.execute("SELECT id FROM tenants WHERE slug = %s", (tenant_slug,))
            tenant = cur.fetchone()
            if not tenant:
                raise HTTPException(status_code=404, detail="Tenant not found")

            tenant_id = str(tenant["id"])

            cur.execute(
                """
                SELECT role, module_permissions
                FROM memberships
                WHERE tenant_id = %s::uuid
                  AND user_id = %s::uuid
                """,
                (tenant_id, user_id),
            )
            membership = cur.fetchone()
            if not membership:
                raise HTTPException(status_code=403, detail="Not a member of this tenant")

    return {
        "tenant_slug": tenant_slug,
        "tenant_id": tenant_id,
        "user_id": user_id,
        "membership_role": str(membership["role"] or ""),
        "module_permissions": membership["module_permissions"] or {},
    }


def _validate_party(
    party: str,
    *,
    request_type: Optional[str] = None,
) -> str:
    value = _safe(party)

    if not value:
        raise HTTPException(
            status_code=400,
            detail="Contributor role is required",
        )

    # Contributor Information requests support every ONIX contributor role,
    # including A01, A12, B01, B06, etc.
    if _safe(request_type).upper() == "CONTRIBUTOR_INFO":
        return value.upper()

    # Legacy author/illustrator workflows remain restricted.
    normalized = (value or "").strip().lower()

    if not normalized:
        raise HTTPException(
            status_code=400,
            detail="party is required",
        )

    return normalized


def _validate_request_type(request_type: str) -> str:
    rt = (request_type or "").strip().upper()
    if rt not in SUPPORTED_REQUEST_TYPES:
        raise HTTPException(status_code=400, detail=f"Unsupported request_type: {request_type}")
    return rt


def _optional_uuid(value: Any, field_name: str) -> str:
    raw = _safe(value)
    if not raw:
        return ""
    try:
        return str(uuid.UUID(raw))
    except (ValueError, TypeError, AttributeError):
        raise HTTPException(status_code=400, detail=f"{field_name} must be a valid UUID")


def _request_contributor_metadata(payload: BookDevRequestIn) -> Dict[str, Any]:
    nested = payload.contributor if isinstance(payload.contributor, dict) else {}

    party_id = _optional_uuid(
        payload.contributor_party_id or nested.get("partyId") or nested.get("party_id"),
        "contributor_party_id",
    )
    role_code = _safe(
        payload.contributor_role_code
        or nested.get("roleCode")
        or nested.get("role_code")
        or payload.party
    ).upper()
    role_label = _safe(
        payload.contributor_role_label
        or nested.get("roleLabel")
        or nested.get("role_label")
    )

    sequence_raw = (
        payload.contributor_sequence_number
        if payload.contributor_sequence_number is not None
        else nested.get("sequenceNumber", nested.get("sequence_number"))
    )
    try:
        sequence_number = int(sequence_raw) if sequence_raw not in (None, "") else None
    except (TypeError, ValueError):
        raise HTTPException(status_code=400, detail="contributor_sequence_number must be an integer")

    return {
        "contributor_party_id": party_id,
        "contributor_role_code": role_code,
        "contributor_role_label": role_label,
        "contributor_sequence_number": sequence_number,
    }


def _work_title(row: Dict[str, Any]) -> str:
    title = str(row.get("title") or "Untitled").strip()
    subtitle = str(row.get("subtitle") or "").strip()
    return f"{title}: {subtitle}" if subtitle else title


def _load_work_or_404(cur, tenant_id: str, work_id: str) -> Dict[str, Any]:
    cur.execute(
        """
        SELECT id::text AS id, uid, title, subtitle
        FROM works
        WHERE tenant_id = %s::uuid
          AND id = %s::uuid
        LIMIT 1
        """,
        (tenant_id, work_id),
    )
    row = cur.fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Work not found")
    return dict(row)


def _request_path_for_type(request_type: str, token: str) -> str:
    if request_type == "CONTRIBUTOR_INFO":
        return f"/email-requests/contributor-info/{token}"
    if request_type in {"AUTHOR_PHOTO", "ILLUSTRATOR_PHOTO"}:
        return f"/email-requests/contributor-photo/{token}"
    if request_type == "MEDIA_QUESTIONNAIRE":
        return f"/email-requests/media-questionnaire/{token}"
    if request_type == "MARKETING_PROFILE":
        return f"/email-requests/marketing-profile/{token}"
    if request_type == "SALES_INFORMATION":
        return f"/email-requests/sales-information/{token}"
    return f"/email-requests/request/{token}"


def _render_bookdev_request_email(
    *,
    request_type: str,
    recipient_name: str,
    title: str,
    form_link: str,
    expires_at: datetime,
    signature: str,
    custom_message: str = "",
) -> tuple[str, str]:
    expires_str = expires_at.strftime("%B %d, %Y")
    name = recipient_name or "Contributor"

    labels = {
        "CONTRIBUTOR_INFO": "Contributor Information Request",
        "AUTHOR_PHOTO": "Author Photo Request",
        "ILLUSTRATOR_PHOTO": "Illustrator Photo Request",
        "MEDIA_QUESTIONNAIRE": "Media & Publicity Questionnaire",
        "MARKETING_PROFILE": "Marketing & Publicity Profile",
        "SALES_INFORMATION": "Sales Information Request",
    }

    label = labels.get(request_type, "Book Development Request")
    subject = f"{label} – {title}"

    extra = f"\n\n{custom_message.strip()}\n" if custom_message.strip() else ""

    body = f"""Hello {name},

Please complete the following request for:

{title}

Request: {label}
{extra}
Use the secure link below:

{form_link}

This link expires on {expires_str}. Please do not forward it.

{signature}
"""

    return subject, body


def _render_completion_email(
    *,
    title: str,
    party: str,
    contributor_name: str,
    signature: str,
) -> tuple[str, str]:
    subject = f"Completed: Contributor Information – {title}"

    body = f"""Hello,

The contributor information request has been completed.

Title: {title}
Party: {party.title()}
Contributor: {contributor_name or "Contributor"}

You can now review the updated contributor information in InkSuite.

{signature}
"""

    return subject, body


def _insert_bookdev_request(
    *,
    tenant_slug: str,
    tenant_id: str,
    work_id: str,
    request_type: str,
    party: str,
    recipient_name: str,
    recipient_email: str,
    requester_email: Optional[str],
    token_hash: str,
    expires_at: datetime,
    created_by_user_id: Optional[str],
    payload_json: Dict[str, Any],
) -> str:
    with db_conn() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                """
                INSERT INTO bookdev_requests
                  (
                    tenant_slug,
                    tenant_id,
                    work_id,
                    request_type,
                    party,
                    recipient_name,
                    recipient_email,
                    requester_email,
                    token_hash,
                    status,
                    expires_at,
                    created_by_user_id,
                    last_sent_at,
                    payload_json
                  )
                VALUES
                  (
                    %s,
                    %s::uuid,
                    %s::uuid,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    'sent',
                    %s,
                    %s::uuid,
                    now(),
                    %s::jsonb
                  )
                RETURNING id::text AS id
                """,
                (
                    tenant_slug,
                    tenant_id,
                    work_id,
                    request_type,
                    party,
                    recipient_name,
                    recipient_email,
                    requester_email,
                    token_hash,
                    expires_at,
                    created_by_user_id,
                    json.dumps(payload_json),
                ),
            )
            row = cur.fetchone()
        conn.commit()

    return str(row["id"])


def _get_request_by_token_hash(token_hash: str) -> Optional[Dict[str, Any]]:
    with db_conn() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                """
                SELECT
                    r.id::text AS id,
                    r.tenant_slug,
                    r.tenant_id::text AS tenant_id,
                    r.work_id::text AS work_id,
                    r.request_type,
                    r.party,
                    r.recipient_name,
                    r.recipient_email,
                    r.requester_email,
                    r.status,
                    r.expires_at,
                    r.completed_at,
                    r.payload_json,
                    r.response_json,
                    w.title,
                    w.subtitle
                FROM bookdev_requests r
                JOIN works w
                  ON w.id = r.work_id
                 AND w.tenant_id = r.tenant_id
                WHERE r.token_hash = %s
                LIMIT 1
                """,
                (token_hash,),
            )
            row = cur.fetchone()

    return dict(row) if row else None


def _mark_expired_if_needed(request_id: str, expires_at: datetime, status: str) -> str:
    if status == "expired":
        return "expired"

    if expires_at <= _now_utc():
        with db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE bookdev_requests
                    SET status = 'expired', updated_at = now()
                    WHERE id = %s::uuid
                      AND status NOT IN ('revoked', 'expired', 'completed')
                    """,
                    (request_id,),
                )
            conn.commit()
        return "expired"

    return status


def _role_match_sql(party: str) -> tuple[str, tuple[str, ...]]:
    if party == "illustrator":
        return "illustrator", ("ILLUSTRATOR", "A12", "ARTIST", "ILLUSTRATION")
    return "author", ("AUTHOR", "A01", "WRITER", "PRIMARY AUTHOR")


def _fetch_socials(cur, tenant_id: str, party_id: str) -> Dict[str, str]:
    out = {
        "instagram": "",
        "facebook": "",
        "x": "",
        "tiktok": "",
        "linkedin": "",
        "website": "",
    }

    try:
        cur.execute(
            """
            SELECT platform, url
            FROM party_socials
            WHERE tenant_id = %s::uuid
              AND party_id = %s::uuid
            ORDER BY platform ASC, id ASC
            """,
            (tenant_id, party_id),
        )
        rows = cur.fetchall() or []
    except Exception:
        return out

    for r in rows:
        platform = _safe(r.get("platform")).lower()
        url = _safe(r.get("url"))

        if not platform and not url:
            continue

        if "instagram" in platform:
            out["instagram"] = url
        elif platform in {"x", "twitter"} or "twitter" in platform:
            out["x"] = url
        elif "facebook" in platform:
            out["facebook"] = url
        elif "tiktok" in platform:
            out["tiktok"] = url
        elif "linkedin" in platform:
            out["linkedin"] = url
        elif platform in {"website", "site"}:
            out["website"] = url
        else:
            out[platform or f"other_{len(out)}"] = url

    return out


def _fetch_agency_agent_prefill(
    cur,
    tenant_id: str,
    represented_party_id: str,
    work_id: str,
) -> Dict[str, Any]:
    empty = {
        "agency": {},
        "agent": {},
        "representation": {},
    }

    cur.execute(
        """
        SELECT
            pr.id::text AS representation_id,
            pr.is_primary,
            pr.role_label,
            pr.agent_party_id::text AS linked_party_id
        FROM party_representations pr
        WHERE pr.tenant_id = %s::uuid
          AND pr.represented_party_id = %s::uuid
          AND (pr.work_id = %s::uuid OR pr.work_id IS NULL)
        ORDER BY pr.is_primary DESC NULLS LAST,
                 (pr.work_id IS NOT NULL) DESC,
                 pr.created_at DESC NULLS LAST
        LIMIT 1
        """,
        (tenant_id, represented_party_id, work_id),
    )
    rep = cur.fetchone()
    if not rep or not rep.get("linked_party_id"):
        return empty

    linked_party_id = str(rep["linked_party_id"])

    cur.execute(
        """
        SELECT
            id::text AS id,
            party_type,
            display_name,
            email,
            website,
            phone_country_code,
            phone_number
        FROM parties
        WHERE tenant_id = %s::uuid
          AND id = %s::uuid
        LIMIT 1
        """,
        (tenant_id, linked_party_id),
    )
    linked = cur.fetchone()
    if not linked:
        return empty

    agency_party: Dict[str, Any] = {}
    agent_party: Dict[str, Any] = {}

    cur.execute(
        """
        SELECT
            l.agent_party_id::text AS agent_party_id,
            ag.display_name AS agent_name,
            ag.email AS agent_email,
            ag.website AS agent_website,
            ag.phone_country_code AS agent_phone_country_code,
            ag.phone_number AS agent_phone_number,
            l.is_primary,
            l.role_label
        FROM agency_agent_links l
        JOIN parties ag
          ON ag.id = l.agent_party_id
         AND ag.tenant_id = l.tenant_id
        WHERE l.tenant_id = %s::uuid
          AND l.agency_party_id = %s::uuid
        ORDER BY l.is_primary DESC NULLS LAST, ag.display_name ASC, l.id ASC
        LIMIT 1
        """,
        (tenant_id, linked_party_id),
    )
    row_as_agency = cur.fetchone()

    if row_as_agency:
        agency_party = dict(linked)
        agent_party = {
            "id": row_as_agency.get("agent_party_id"),
            "display_name": row_as_agency.get("agent_name"),
            "email": row_as_agency.get("agent_email"),
            "website": row_as_agency.get("agent_website"),
            "phone_country_code": row_as_agency.get("agent_phone_country_code"),
            "phone_number": row_as_agency.get("agent_phone_number"),
        }
    else:
        cur.execute(
            """
            SELECT
                l.agency_party_id::text AS agency_party_id,
                agcy.display_name AS agency_name,
                agcy.email AS agency_email,
                agcy.website AS agency_website,
                agcy.phone_country_code AS agency_phone_country_code,
                agcy.phone_number AS agency_phone_number,
                l.is_primary,
                l.role_label
            FROM agency_agent_links l
            JOIN parties agcy
              ON agcy.id = l.agency_party_id
             AND agcy.tenant_id = l.tenant_id
            WHERE l.tenant_id = %s::uuid
              AND l.agent_party_id = %s::uuid
            ORDER BY l.is_primary DESC NULLS LAST, agcy.display_name ASC, l.id ASC
            LIMIT 1
            """,
            (tenant_id, linked_party_id),
        )
        row_as_agent = cur.fetchone()

        if row_as_agent:
            agent_party = dict(linked)
            agency_party = {
                "id": row_as_agent.get("agency_party_id"),
                "display_name": row_as_agent.get("agency_name"),
                "email": row_as_agent.get("agency_email"),
                "website": row_as_agent.get("agency_website"),
                "phone_country_code": row_as_agent.get("agency_phone_country_code"),
                "phone_number": row_as_agent.get("agency_phone_number"),
            }
        else:
            party_type = _safe(linked.get("party_type")).lower()
            if party_type in {"organization", "agency", "company"}:
                agency_party = dict(linked)
            else:
                agent_party = dict(linked)

    agency_address = {}
    if agency_party.get("id"):
        agency_address = _fetch_party_address(cur, tenant_id, str(agency_party["id"])) or {}

    return {
        "representation": {
            "id": rep.get("representation_id") or "",
            "is_primary": bool(rep.get("is_primary")),
            "role_label": rep.get("role_label") or "",
        },
        "agency": {
            "party_id": agency_party.get("id") or "",
            "name": agency_party.get("display_name") or "",
            "email": agency_party.get("email") or "",
            "website": agency_party.get("website") or "",
            "address_street": agency_address.get("street") or "",
            "address_city": agency_address.get("city") or "",
            "address_state": agency_address.get("state") or "",
            "address_zip": agency_address.get("zip") or "",
            "address_country": agency_address.get("country") or "",
        },
        "agent": {
            "party_id": agent_party.get("id") or "",
            "name": agent_party.get("display_name") or "",
            "email": agent_party.get("email") or "",
            "phone_country_code": agent_party.get("phone_country_code") or "",
            "phone_number": agent_party.get("phone_number") or "",
        },
    }


def _load_contributor_prefill(
    *,
    tenant_id: str,
    work_id: str,
    contributor_party_id: str,
    contributor_role_code: str = "",
) -> Dict[str, Any]:
    empty = {
        "contributor": {},
        "agency": {},
        "agent": {},
        "representation": {},
    }

    contributor_party_id = _optional_uuid(contributor_party_id, "contributor_party_id")
    if not contributor_party_id:
        return empty

    with db_conn() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                """
                SELECT
                    p.id::text AS party_id,
                    p.display_name,
                    p.email,
                    p.phone_country_code,
                    p.phone_number,
                    p.website,
                    p.citizenship,
                    p.birth_date::text AS birth_date,
                    p.birth_city,
                    p.birth_country,
                    p.short_bio,
                    p.long_bio,
                    wc.contributor_role,
                    wc.sequence_number
                FROM work_contributors wc
                JOIN parties p
                  ON p.id = wc.party_id
                 AND p.tenant_id = wc.tenant_id
                WHERE wc.tenant_id = %s::uuid
                  AND wc.work_id = %s::uuid
                  AND wc.party_id = %s::uuid
                LIMIT 1
                """,
                (tenant_id, work_id, contributor_party_id),
            )
            contributor = cur.fetchone()

            if not contributor:
                return empty

            stored_role = _safe(contributor.get("contributor_role")).upper()
            expected_role = _safe(contributor_role_code).upper()
            if expected_role and stored_role and expected_role != stored_role:
                raise HTTPException(
                    status_code=409,
                    detail="Contributor role no longer matches this request",
                )

            contributor_address = _fetch_party_address(cur, tenant_id, contributor_party_id) or {}
            socials = _fetch_socials(cur, tenant_id, contributor_party_id)
            rep_data = _fetch_agency_agent_prefill(
                cur,
                tenant_id=tenant_id,
                represented_party_id=contributor_party_id,
                work_id=work_id,
            )

    return {
        "contributor": {
            "party_id": contributor_party_id,
            "name": contributor.get("display_name") or "",
            "email": contributor.get("email") or "",
            "phone_country_code": contributor.get("phone_country_code") or "",
            "phone_number": contributor.get("phone_number") or "",
            "website": contributor.get("website") or "",
            "address_street": contributor_address.get("street") or "",
            "address_city": contributor_address.get("city") or "",
            "address_state": contributor_address.get("state") or "",
            "address_zip": contributor_address.get("zip") or contributor_address.get("postal_code") or "",
            "address_country": contributor_address.get("country") or "",
            "citizenship": contributor.get("citizenship") or "",
            "birth_date": contributor.get("birth_date") or "",
            "birth_city": contributor.get("birth_city") or "",
            "birth_country": contributor.get("birth_country") or "",
            "short_bio": contributor.get("short_bio") or "",
            "long_bio": contributor.get("long_bio") or "",
            "social_media": socials,
            "role_code": stored_role,
            "sequence_number": contributor.get("sequence_number"),
        },
        "agency": rep_data.get("agency") or {},
        "agent": rep_data.get("agent") or {},
        "representation": rep_data.get("representation") or {},
    }


def _socials_to_catalog_list(raw: Dict[str, Any]) -> list[Dict[str, str]]:
    if not isinstance(raw, dict):
        return []

    out: list[Dict[str, str]] = []
    for platform, url in raw.items():
        platform_s = _safe(platform)
        url_s = _safe(url)
        if platform_s or url_s:
            out.append({"platform": platform_s, "url": url_s})
    return out


def _payload_for_catalog_helpers(payload: ContributorInfoSubmitIn, scope: str) -> Dict[str, Any]:
    contributor_name = _safe(
        payload.contributor_name
        or payload.contributor_display_name
    )

    short_bio = _safe(payload.short_bio or payload.contributor_short_bio)
    long_bio = _safe(payload.long_bio or payload.contributor_long_bio)

    return {
        scope: {
            "name": contributor_name,
            "email": _safe(payload.contributor_email),
            "website": _safe(payload.contributor_website),
            "phone_country_code": _safe(payload.contributor_phone_country_code),
            "phone_number": _safe(payload.contributor_phone_number),
            "address": {
                "street": _safe(payload.contributor_address_street),
                "city": _safe(payload.contributor_address_city),
                "state": _safe(payload.contributor_address_state),
                "zip": _safe(payload.contributor_address_zip),
                "country": _safe(payload.contributor_address_country),
            },
        },
        f"{scope}_birth_city": _safe(payload.contributor_birth_city),
        f"{scope}_birth_country": _safe(payload.contributor_birth_country),
        f"{scope}_birth_date": payload.contributor_birth_date,
        f"{scope}_citizenship": _safe(payload.contributor_citizenship),
        f"{scope}_book_bio": short_bio,
        f"{scope}_website_bio": long_bio,
        f"{scope}_socials": _socials_to_catalog_list(payload.social_media or {}),
    }


def _ensure_work_contributor(
    cur,
    tenant_id: str,
    work_id: str,
    party_id: str,
    contributor_role: str,
    sequence_number: Optional[int] = None,
) -> None:
    contributor_role = _safe(contributor_role).upper()
    if not contributor_role:
        raise HTTPException(status_code=400, detail="Contributor role code is required")

    cur.execute(
        """
        SELECT id, contributor_role, sequence_number
        FROM work_contributors
        WHERE tenant_id = %s::uuid
          AND work_id = %s::uuid
          AND party_id = %s::uuid
        LIMIT 1
        """,
        (tenant_id, work_id, party_id),
    )
    existing = cur.fetchone()

    if existing:
        cur.execute(
            """
            UPDATE work_contributors
            SET contributor_role = %s,
                sequence_number = COALESCE(%s, sequence_number)
            WHERE tenant_id = %s::uuid
              AND id = %s
            """,
            (contributor_role, sequence_number, tenant_id, existing["id"]),
        )
        return

    cur.execute(
        """
        INSERT INTO work_contributors
          (tenant_id, work_id, party_id, contributor_role, sequence_number)
        VALUES
          (%s::uuid, %s::uuid, %s::uuid, %s, COALESCE(%s, 1))
        """,
        (tenant_id, work_id, party_id, contributor_role, sequence_number),
    )


def _agency_payload_for_catalog(payload: ContributorInfoSubmitIn) -> Dict[str, Any]:
    if not bool(payload.has_agent):
        return {}

    agency_name = _safe(payload.agency_name)
    agent_name = _safe(payload.agent_name or payload.agent_display_name)

    if not agency_name and not agent_name:
        return {}

    phone = " ".join(
        x
        for x in (
            _safe(payload.agent_phone_country_code or payload.agency_phone_country_code),
            _safe(payload.agent_phone_number or payload.agency_phone_number),
        )
        if x
    )

    address_lines = [
        _safe(payload.agency_street or payload.agency_address_line1),
        _safe(payload.agency_address_line2),
        ", ".join(
            x
            for x in (
                _safe(payload.agency_city),
                _safe(payload.agency_state),
                _safe(payload.agency_zip or payload.agency_postal_code),
            )
            if x
        ),
        _safe(payload.agency_country),
    ]

    return {
        "agency": agency_name,
        "agent": agent_name,
        "contact": agent_name,
        "email": _safe(payload.agent_email or payload.agency_email),
        "website": _safe(payload.agency_website),
        "phone": phone,
        "addressLines": [x for x in address_lines if x],
    }


def _sp_name(prefix: str = "bd") -> str:
    return f"{prefix}_{secrets.token_hex(4)}"


def _execute_savepoint(cur, sql: str, params: tuple = ()) -> bool:
    """
    Run one optional DB statement without poisoning the outer transaction.
    psycopg marks the whole transaction aborted after an error unless we roll
    back to a savepoint. This is critical for optional agency writes.
    """
    sp = _sp_name()
    cur.execute(f"SAVEPOINT {sp}")
    try:
        cur.execute(sql, params)
        cur.execute(f"RELEASE SAVEPOINT {sp}")
        return True
    except Exception as exc:
        print("BOOKDEV optional DB statement skipped:", repr(exc))
        cur.execute(f"ROLLBACK TO SAVEPOINT {sp}")
        cur.execute(f"RELEASE SAVEPOINT {sp}")
        return False


def _fetch_one_savepoint(cur, sql: str, params: tuple = ()) -> Optional[Dict[str, Any]]:
    sp = _sp_name()
    cur.execute(f"SAVEPOINT {sp}")
    try:
        cur.execute(sql, params)
        row = cur.fetchone()
        cur.execute(f"RELEASE SAVEPOINT {sp}")
        return dict(row) if row else None
    except Exception as exc:
        print("BOOKDEV optional DB fetch skipped:", repr(exc))
        cur.execute(f"ROLLBACK TO SAVEPOINT {sp}")
        cur.execute(f"RELEASE SAVEPOINT {sp}")
        return None


def _bookdev_get_or_create_agency_party(
    cur,
    tenant_id: str,
    agency_name: str,
    agency_email: str = "",
    agency_website: str = "",
) -> Optional[str]:
    agency_name = _safe(agency_name)
    agency_email = _safe(agency_email)
    agency_website = _safe(agency_website)

    if not agency_name:
        return None

    existing = _fetch_one_savepoint(
        cur,
        """
        SELECT id::text AS id
        FROM parties
        WHERE tenant_id = %s::uuid
          AND lower(coalesce(display_name, '')) = lower(%s)
          AND lower(coalesce(party_type, '')) IN ('org', 'organization', 'agency', 'company')
        LIMIT 1
        """,
        (tenant_id, agency_name),
    )

    if existing and existing.get("id"):
        agency_party_id = str(existing["id"])
        _execute_savepoint(
            cur,
            """
            UPDATE parties
            SET email = COALESCE(NULLIF(%s, ''), email),
                website = COALESCE(NULLIF(%s, ''), website),
                updated_at = now()
            WHERE tenant_id = %s::uuid
              AND id = %s::uuid
            """,
            (agency_email, agency_website, tenant_id, agency_party_id),
        )
        return agency_party_id

    for party_type in ("org", "organization", "agency", "company"):
        agency_party_id = str(uuid.uuid4())
        ok = _execute_savepoint(
            cur,
            """
            INSERT INTO parties (id, tenant_id, party_type, display_name, email, website)
            VALUES (%s::uuid, %s::uuid, %s, %s, %s, %s)
            """,
            (agency_party_id, tenant_id, party_type, agency_name, agency_email, agency_website),
        )
        if ok:
            return agency_party_id

    return None


def _bookdev_replace_agency_address(
    cur,
    tenant_id: str,
    agency_party_id: str,
    payload: ContributorInfoSubmitIn,
) -> None:
    street = _safe(payload.agency_street or payload.agency_address_line1)
    city = _safe(payload.agency_city)
    state = _safe(payload.agency_state)
    postal = _safe(payload.agency_zip or payload.agency_postal_code)
    country = _safe(payload.agency_country)

    if not any([street, city, state, postal, country]):
        return

    _execute_savepoint(
        cur,
        "DELETE FROM party_addresses WHERE tenant_id = %s::uuid AND party_id = %s::uuid AND label = 'primary'",
        (tenant_id, agency_party_id),
    )

    inserted = _execute_savepoint(
        cur,
        """
        INSERT INTO party_addresses (tenant_id, party_id, label, street, city, state, zip, country, is_non_us)
        VALUES (%s::uuid, %s::uuid, 'primary', %s, %s, %s, %s, %s, %s)
        """,
        (
            tenant_id,
            agency_party_id,
            street,
            city,
            state,
            postal,
            country,
            bool(country and country.lower() not in ("us", "usa", "united states")),
        ),
    )

    if not inserted:
        _execute_savepoint(
            cur,
            """
            INSERT INTO party_addresses (tenant_id, party_id, label, street, city, state, postal_code, country, is_non_us)
            VALUES (%s::uuid, %s::uuid, 'primary', %s, %s, %s, %s, %s, %s)
            """,
            (
                tenant_id,
                agency_party_id,
                street,
                city,
                state,
                postal,
                country,
                bool(country and country.lower() not in ("us", "usa", "united states")),
            ),
        )


def _bookdev_replace_party_representation(
    cur,
    tenant_id: str,
    represented_party_id: str,
    work_id: str,
    payload: ContributorInfoSubmitIn,
) -> None:
    """
    Local replacement for catalog_write._replace_party_representation.

    The catalog helper swallows optional SQL errors. With psycopg, catching an
    error inside a transaction without rolling back to a savepoint leaves the
    transaction aborted, which caused:
    "current transaction is aborted, commands ignored until end of transaction block".
    """
    if not bool(payload.has_agent):
        return

    agency_name = _safe(payload.agency_name)
    agent_name = _safe(payload.agent_name or payload.agent_display_name)
    agency_email = _safe(payload.agency_email)
    agency_website = _safe(payload.agency_website)
    agent_email = _safe(payload.agent_email)
    agent_phone_country_code = _safe(payload.agent_phone_country_code or payload.agency_phone_country_code)
    agent_phone_number = _safe(payload.agent_phone_number or payload.agency_phone_number)

    if not agency_name and not agent_name:
        return

    agency_party_id: Optional[str] = None
    if agency_name:
        agency_party_id = _bookdev_get_or_create_agency_party(
            cur,
            tenant_id,
            agency_name,
            agency_email,
            agency_website,
        )
        if agency_party_id:
            _bookdev_replace_agency_address(cur, tenant_id, agency_party_id, payload)

    agent_party_id: Optional[str] = None
    if agent_name or agent_email:
        agent_party_id = _get_or_create_party(
            cur,
            tenant_id,
            agent_name or agent_email,
            agent_email,
            "person",
        )
        if agent_party_id:
            _execute_savepoint(
                cur,
                """
                UPDATE parties
                SET display_name = COALESCE(NULLIF(%s, ''), display_name),
                    email = COALESCE(NULLIF(%s, ''), email),
                    phone_country_code = COALESCE(NULLIF(%s, ''), phone_country_code),
                    phone_number = COALESCE(NULLIF(%s, ''), phone_number),
                    updated_at = now()
                WHERE tenant_id = %s::uuid
                  AND id = %s::uuid
                """,
                (
                    agent_name,
                    agent_email,
                    agent_phone_country_code,
                    agent_phone_number,
                    tenant_id,
                    agent_party_id,
                ),
            )

    representation_target = agent_party_id or agency_party_id
    if not representation_target:
        return

    _execute_savepoint(
        cur,
        """
        DELETE FROM party_representations
        WHERE tenant_id = %s::uuid
          AND represented_party_id = %s::uuid
          AND (work_id = %s::uuid OR work_id IS NULL)
        """,
        (tenant_id, represented_party_id, work_id),
    )

    inserted_rep = _execute_savepoint(
        cur,
        """
        INSERT INTO party_representations (
            tenant_id, represented_party_id, agent_party_id, work_id,
            is_primary, role_label, notes
        )
        VALUES (%s::uuid, %s::uuid, %s::uuid, %s::uuid, true, 'agent', '')
        """,
        (tenant_id, represented_party_id, representation_target, work_id),
    )

    if not inserted_rep:
        _execute_savepoint(
            cur,
            """
            INSERT INTO party_representations (
                tenant_id, represented_party_id, agent_party_id, work_id,
                is_primary, role, notes
            )
            VALUES (%s::uuid, %s::uuid, %s::uuid, %s::uuid, true, 'agent', '')
            """,
            (tenant_id, represented_party_id, representation_target, work_id),
        )

    if agency_party_id and agent_party_id:
        _execute_savepoint(
            cur,
            """
            INSERT INTO agency_agent_links (
                tenant_id, agency_party_id, agent_party_id, is_primary, role_label
            )
            VALUES (%s::uuid, %s::uuid, %s::uuid, true, 'agent')
            ON CONFLICT (agency_party_id, agent_party_id) DO UPDATE SET
                is_primary = EXCLUDED.is_primary,
                role_label = EXCLUDED.role_label,
                updated_at = now()
            """,
            (tenant_id, agency_party_id, agent_party_id),
        )


def _bookdev_update_contributor_core(
    cur,
    tenant_id: str,
    party_id: str,
    payload: ContributorInfoSubmitIn,
    contributor_name: str,
    contributor_email: str,
) -> None:
    """
    Required contributor update for the public contributor-info form.

    This intentionally avoids catalog_write._upsert_party_core and
    _replace_party_socials for this endpoint because those helpers can catch
    optional SQL errors inside the current transaction. In psycopg, once a SQL
    error is caught without rolling back to a savepoint, the transaction becomes
    aborted and the next command fails with:
      current transaction is aborted, commands ignored until end of transaction block
    """
    cur.execute(
        """
        UPDATE parties
        SET
            display_name = %s,
            email = %s,
            website = %s,
            phone_country_code = %s,
            phone_number = %s,
            birth_city = %s,
            birth_country = %s,
            birth_date = NULLIF(%s, '')::date,
            citizenship = %s,
            short_bio = %s,
            long_bio = %s,
            updated_at = now()
        WHERE tenant_id = %s::uuid
          AND id = %s::uuid
        """,
        (
            contributor_name,
            contributor_email,
            _safe(payload.contributor_website),
            _safe(payload.contributor_phone_country_code),
            _safe(payload.contributor_phone_number),
            _safe(payload.contributor_birth_city),
            _safe(payload.contributor_birth_country),
            _safe(payload.contributor_birth_date),
            _safe(payload.contributor_citizenship),
            _safe(payload.short_bio or payload.contributor_short_bio),
            _safe(payload.long_bio or payload.contributor_long_bio),
            tenant_id,
            party_id,
        ),
    )

    street = _safe(payload.contributor_address_street)
    city = _safe(payload.contributor_address_city)
    state = _safe(payload.contributor_address_state)
    postal = _safe(payload.contributor_address_zip)
    country = _safe(payload.contributor_address_country)

    if not any([street, city, state, postal, country]):
        return

    # Address is useful, but it must not poison the transaction if this database
    # happens to use zip vs postal_code or has a slightly different shape.
    _execute_savepoint(
        cur,
        "DELETE FROM party_addresses WHERE tenant_id = %s::uuid AND party_id = %s::uuid AND label = 'primary'",
        (tenant_id, party_id),
    )

    inserted = _execute_savepoint(
        cur,
        """
        INSERT INTO party_addresses (tenant_id, party_id, label, street, city, state, zip, country, is_non_us)
        VALUES (%s::uuid, %s::uuid, 'primary', %s, %s, %s, %s, %s, %s)
        """,
        (
            tenant_id,
            party_id,
            street,
            city,
            state,
            postal,
            country,
            bool(country and country.lower() not in ("us", "usa", "united states")),
        ),
    )

    if not inserted:
        _execute_savepoint(
            cur,
            """
            INSERT INTO party_addresses (tenant_id, party_id, label, street, city, state, postal_code, country, is_non_us)
            VALUES (%s::uuid, %s::uuid, 'primary', %s, %s, %s, %s, %s, %s)
            """,
            (
                tenant_id,
                party_id,
                street,
                city,
                state,
                postal,
                country,
                bool(country and country.lower() not in ("us", "usa", "united states")),
            ),
        )




def _load_media_questionnaire_prefill(
    *,
    tenant_id: str,
    work_id: str,
    party: str,
) -> Dict[str, Any]:
    scope, allowed_roles = _role_match_sql(party)
    empty = {
        "contributor": {},
        "media": {},
    }

    with db_conn() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                """
                SELECT
                    p.id::text AS party_id,
                    p.display_name,
                    p.email
                FROM work_contributors wc
                JOIN parties p
                  ON p.id = wc.party_id
                 AND p.tenant_id = wc.tenant_id
                WHERE wc.tenant_id = %s::uuid
                  AND wc.work_id = %s::uuid
                  AND upper(COALESCE(wc.contributor_role, '')) = ANY(%s)
                ORDER BY wc.sequence_number NULLS LAST, p.created_at NULLS LAST, p.display_name
                LIMIT 1
                """,
                (tenant_id, work_id, list(allowed_roles)),
            )
            contributor = cur.fetchone()
            if not contributor:
                return empty

            party_id = str(contributor["party_id"])

            profile: Dict[str, Any] = {}
            try:
                cur.execute(
                    """
                    SELECT *
                    FROM contributor_marketing_profiles
                    WHERE tenant_id = %s::uuid
                      AND party_id = %s::uuid
                      AND lower(scope) = %s
                    LIMIT 1
                    """,
                    (tenant_id, party_id, scope.lower()),
                )
                r = cur.fetchone()
                if r:
                    profile = dict(r)
            except Exception:
                profile = {}

            def fetch_rows(sql: str):
                try:
                    cur.execute(sql, (tenant_id, party_id, scope.lower()))
                    return cur.fetchall() or []
                except Exception:
                    return []

            published = [
                {
                    "title": _safe(r.get("title")),
                    "isbn": _safe(r.get("isbn")),
                    "publisher": _safe(r.get("publisher")),
                    "year": _safe(r.get("publication_year")),
                    "approx_sold": _safe(r.get("approx_sold")),
                }
                for r in fetch_rows(
                    """
                    SELECT * FROM contributor_published_books
                    WHERE tenant_id = %s::uuid AND party_id = %s::uuid AND lower(scope) = %s
                    ORDER BY item_order ASC, title ASC, id ASC
                    """
                )
            ]

            other_pubs = [
                {
                    "title": _safe(r.get("title")),
                    "publication": _safe(r.get("publication")),
                    "date": _safe(r.get("date_text")),
                    "notes": _safe(r.get("notes")),
                }
                for r in fetch_rows(
                    """
                    SELECT * FROM contributor_other_publications
                    WHERE tenant_id = %s::uuid AND party_id = %s::uuid AND lower(scope) = %s
                    ORDER BY item_order ASC, id ASC
                    """
                )
            ]

            media_appearances = [
                {
                    "title": _safe(r.get("title")),
                    "venue": _safe(r.get("venue")),
                    "date": _safe(r.get("date_text")),
                    "link": _safe(r.get("link")),
                    "notes": _safe(r.get("notes")),
                }
                for r in fetch_rows(
                    """
                    SELECT * FROM contributor_media_appearances
                    WHERE tenant_id = %s::uuid AND party_id = %s::uuid AND lower(scope) = %s
                    ORDER BY item_order ASC, id ASC
                    """
                )
            ]

            media_contacts = [
                {
                    "company": _safe(r.get("company")),
                    "name": _safe(r.get("name")),
                    "position": _safe(r.get("position")),
                    "phone": _safe(r.get("phone")),
                    "email": _safe(r.get("email")),
                }
                for r in fetch_rows(
                    """
                    SELECT * FROM contributor_media_contacts
                    WHERE tenant_id = %s::uuid AND party_id = %s::uuid AND lower(scope) = %s
                    ORDER BY item_order ASC, id ASC
                    """
                )
            ]

    return {
        "contributor": {
            "party_id": party_id,
            "name": contributor.get("display_name") or "",
            "email": contributor.get("email") or "",
        },
        "media": {
            "media_press_share": profile.get("media_press_share"),
            "books_published": published,
            "other_publications": other_pubs,
            "media_appearances": media_appearances,
            "media_contacts": media_contacts,
            "book_bio": _safe(profile.get("book_bio")),
            "website_bio": _safe(profile.get("website_bio")),
            "contact_pref_rank1": _safe(profile.get("contact_pref_rank1")),
            "contact_pref_rank2": _safe(profile.get("contact_pref_rank2")),
            "media_best_times": _safe(profile.get("media_best_times")),
            "us_travel_plans": _safe(profile.get("us_travel_plans")),
            "travel_dates": _safe(profile.get("travel_dates")),
            "additional_notes": _safe(profile.get("additional_notes")),
            "photo_credit": _safe(profile.get("photo_credit")),
            "present_position": _safe(profile.get("present_position")),
            "former_positions": _safe(profile.get("former_positions")),
            "degrees_honors": _safe(profile.get("degrees_honors")),
            "professional_honors": _safe(profile.get("professional_honors")),
        },
    }


def _find_contributor_party_id(cur, tenant_id: str, work_id: str, party: str) -> str:
    _, allowed_roles = _role_match_sql(party)
    cur.execute(
        """
        SELECT p.id::text AS party_id
        FROM work_contributors wc
        JOIN parties p
          ON p.id = wc.party_id
         AND p.tenant_id = wc.tenant_id
        WHERE wc.tenant_id = %s::uuid
          AND wc.work_id = %s::uuid
          AND upper(COALESCE(wc.contributor_role, '')) = ANY(%s)
        ORDER BY wc.sequence_number NULLS LAST, p.created_at NULLS LAST, p.display_name
        LIMIT 1
        """,
        (tenant_id, work_id, list(allowed_roles)),
    )
    row = cur.fetchone()
    if not row:
        raise HTTPException(status_code=400, detail=f"No {party} contributor found for this work")
    return str(row["party_id"])


def _clean_rows(rows: Any) -> list[Dict[str, Any]]:
    if not isinstance(rows, list):
        return []
    out: list[Dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        cleaned = {str(k): _safe(v) for k, v in row.items()}
        if any(cleaned.values()):
            out.append(cleaned)
    return out


def _save_media_questionnaire(
    cur,
    tenant_id: str,
    party_id: str,
    scope: str,
    payload: MediaQuestionnaireSubmitIn,
) -> None:
    data = payload.model_dump(mode="json") if hasattr(payload, "model_dump") else payload.dict()
    books = _clean_rows(data.get("books_published"))
    other_pubs = _clean_rows(data.get("other_publications"))
    appearances = _clean_rows(data.get("media_appearances"))
    contacts = _clean_rows(data.get("media_contacts"))

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
        VALUES (%s::uuid, %s::uuid, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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
            _safe(data.get("website_bio")),
            _safe(data.get("book_bio")),
            _safe(data.get("contact_pref_rank1")),
            _safe(data.get("contact_pref_rank2")),
            _safe(data.get("media_best_times")),
            bool(data.get("media_press_share") or False),
            _safe(data.get("us_travel_plans")),
            _safe(data.get("travel_dates")),
            _safe(data.get("additional_notes")),
            _safe(data.get("photo_credit")),
            _safe(data.get("present_position")),
            _safe(data.get("former_positions")),
            _safe(data.get("degrees_honors")),
            _safe(data.get("professional_honors")),
        ),
    )

    cur.execute(
        "DELETE FROM contributor_published_books WHERE tenant_id = %s::uuid AND party_id = %s::uuid AND lower(scope) = %s",
        (tenant_id, party_id, scope.lower()),
    )
    for idx, b in enumerate(books, start=1):
        cur.execute(
            """
            INSERT INTO contributor_published_books (
                tenant_id, party_id, scope, item_order, title, isbn, publisher, publication_year, approx_sold
            ) VALUES (%s::uuid, %s::uuid, %s, %s, %s, %s, %s, %s, %s)
            """,
            (tenant_id, party_id, scope, idx, _safe(b.get("title")), _safe(b.get("isbn")), _safe(b.get("publisher")), _safe(b.get("year") or b.get("publication_year")), _safe(b.get("approx_sold"))),
        )

    cur.execute(
        "DELETE FROM contributor_other_publications WHERE tenant_id = %s::uuid AND party_id = %s::uuid AND lower(scope) = %s",
        (tenant_id, party_id, scope.lower()),
    )
    for idx, r in enumerate(other_pubs, start=1):
        cur.execute(
            """
            INSERT INTO contributor_other_publications (
                tenant_id, party_id, scope, item_order, title, publication, date_text, notes
            ) VALUES (%s::uuid, %s::uuid, %s, %s, %s, %s, %s, %s)
            """,
            (tenant_id, party_id, scope, idx, _safe(r.get("title")), _safe(r.get("publication")), _safe(r.get("date") or r.get("date_text")), _safe(r.get("notes"))),
        )

    cur.execute(
        "DELETE FROM contributor_media_appearances WHERE tenant_id = %s::uuid AND party_id = %s::uuid AND lower(scope) = %s",
        (tenant_id, party_id, scope.lower()),
    )
    for idx, r in enumerate(appearances, start=1):
        cur.execute(
            """
            INSERT INTO contributor_media_appearances (
                tenant_id, party_id, scope, item_order, title, venue, date_text, link, notes
            ) VALUES (%s::uuid, %s::uuid, %s, %s, %s, %s, %s, %s, %s)
            """,
            (tenant_id, party_id, scope, idx, _safe(r.get("title")), _safe(r.get("venue")), _safe(r.get("date") or r.get("appearance_date")), _safe(r.get("link")), _safe(r.get("notes"))),
        )

    cur.execute(
        "DELETE FROM contributor_media_contacts WHERE tenant_id = %s::uuid AND party_id = %s::uuid AND lower(scope) = %s",
        (tenant_id, party_id, scope.lower()),
    )
    for idx, r in enumerate(contacts, start=1):
        cur.execute(
            """
            INSERT INTO contributor_media_contacts (
                tenant_id, party_id, scope, item_order, company, name, position, phone, email
            ) VALUES (%s::uuid, %s::uuid, %s, %s, %s, %s, %s, %s, %s)
            """,
            (tenant_id, party_id, scope, idx, _safe(r.get("company")), _safe(r.get("name")), _safe(r.get("position")), _safe(r.get("phone")), _safe(r.get("email"))),
        )

@router.post("/{work_id}/requests")
def send_bookdev_request(
    work_id: str,
    payload: BookDevRequestIn,
    request: Request,
    tenant_slug: str = Query(..., description="Tenant slug, e.g. marble-press"),
    ctx=Depends(_ctx_from_bearer),
) -> Dict[str, Any]:
    request_type = _validate_request_type(payload.request_type)
    contributor_meta = _request_contributor_metadata(payload)
    party = _validate_party(
        contributor_meta["contributor_role_code"] or payload.party,
        request_type=request_type,
    )
    mctx = _load_user_and_membership_or_403(tenant_slug=tenant_slug, ctx=ctx)

    with db_conn() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            work = _load_work_or_404(cur, mctx["tenant_id"], work_id)

    settings = _load_tenant_email_settings_or_400(tenant_slug)

    raw_token = secrets.token_urlsafe(32)
    token_hash = _token_hash(raw_token)
    exp = _expires_at(days=int(os.getenv("BOOKDEV_REQUEST_EXPIRES_DAYS") or "14"))

    form_url = f"{_frontend_base_url()}{_request_path_for_type(request_type, raw_token)}"

    request_id = _insert_bookdev_request(
        tenant_slug=tenant_slug,
        tenant_id=mctx["tenant_id"],
        work_id=work_id,
        request_type=request_type,
        party=party,
        recipient_name=(payload.recipient_name or "").strip(),
        recipient_email=str(payload.recipient_email).strip().lower(),
        requester_email=str(payload.requester_email or "").strip().lower() or None,
        token_hash=token_hash,
        expires_at=exp,
        created_by_user_id=mctx["user_id"],
        payload_json={
            "message": payload.message,
            "form_url": form_url,
            **contributor_meta,
        },
    )

    subject, body_text = _render_bookdev_request_email(
        request_type=request_type,
        recipient_name=(payload.recipient_name or "").strip() or "Contributor",
        title=_work_title(work),
        form_link=form_url,
        expires_at=exp,
        signature=settings["from_name"],
        custom_message=payload.message,
    )

    try:
        username, password = _load_smtp_secret(settings["smtp_secret_id"])
        print("SMTP username from secret:", username)
        print("SMTP username from table :", settings.get("smtp_username_hint"))
        print("SMTP password length    :", len(password or ""))

        _send_email_smtp(
            signature_context=(mctx["tenant_id"], mctx["user_id"]),
            smtp_host=settings["smtp_host"],
            smtp_port=settings["smtp_port"],
            tls_mode=settings["tls_mode"],
            username=username,
            password=password,
            from_email=settings["from_email"],
            from_name=settings["from_name"],
            to_email=str(payload.recipient_email).strip().lower(),
            to_name=(payload.recipient_name or "").strip(),
            subject=subject,
            body_text=body_text,
        )

        print("BOOKDEV EMAIL SENT SUCCESSFULLY")

    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        print("BOOKDEV EMAIL FAILED:", repr(e))
        raise HTTPException(status_code=500, detail=f"Email send failed: {e}")

    return {
        "ok": True,
        "email_sent": True,
        "request_id": request_id,
        "request_type": request_type,
        "tenant_slug": tenant_slug,
        "work_id": work_id,
        "party": party,
        "recipient_email": str(payload.recipient_email),
        "requester_email": str(payload.requester_email or ""),
        "form_url": form_url,
        "expires_at": exp,
    }


@router.get("/requests/{token}")
def resolve_bookdev_request_token(token: str) -> Dict[str, Any]:
    row = _get_request_by_token_hash(_token_hash(token))

    if not row:
        raise HTTPException(status_code=404, detail="Book development request not found")

    status = _mark_expired_if_needed(
        request_id=row["id"],
        expires_at=row["expires_at"],
        status=row["status"],
    )

    if status in {"revoked", "expired"}:
        raise HTTPException(status_code=410, detail=f"Book development request {status}")

    request_type = _validate_request_type(row["request_type"])
    party = _validate_party(row["party"], request_type=request_type)
    request_payload = row.get("payload_json") or {}

    prefill = {}
    if request_type == "CONTRIBUTOR_INFO":
        prefill = _load_contributor_prefill(
            tenant_id=row["tenant_id"],
            work_id=row["work_id"],
            contributor_party_id=_safe(request_payload.get("contributor_party_id")),
            contributor_role_code=_safe(request_payload.get("contributor_role_code") or party),
        )
    elif request_type in {"MEDIA_QUESTIONNAIRE", "MARKETING_PROFILE", "SALES_INFORMATION"}:
        prefill = _load_public_form_prefill(
            tenant_id=row["tenant_id"],
            work_id=row["work_id"],
            party=party,
        )

    return {
        "ok": True,
        "request_id": row["id"],
        "request_type": request_type,
        "tenant_slug": row["tenant_slug"],
        "work_id": row["work_id"],
        "party": party,
        "contributor_party_id": _safe(request_payload.get("contributor_party_id")),
        "contributor_role_code": _safe(request_payload.get("contributor_role_code") or party),
        "contributor_role_label": _safe(request_payload.get("contributor_role_label")),
        "contributor_sequence_number": request_payload.get("contributor_sequence_number"),
        "recipient_name": row["recipient_name"],
        "recipient_email": row["recipient_email"],
        "requester_email": row.get("requester_email") or "",
        "title": _work_title(row),
        "status": status,
        "expires_at": row["expires_at"],
        "payload_json": row.get("payload_json") or {},
        "response_json": row.get("response_json") or {},
        "prefill": prefill,
    }


@router.post("/requests/{token}/photo")
def submit_photo_request(
    token: str,
    payload: BookDevPhotoSubmitIn,
) -> Dict[str, Any]:
    row = _get_request_by_token_hash(_token_hash(token))

    if not row:
        raise HTTPException(status_code=404, detail="Book development request not found")

    status = _mark_expired_if_needed(
        request_id=row["id"],
        expires_at=row["expires_at"],
        status=row["status"],
    )

    if status in {"revoked", "expired"}:
        raise HTTPException(status_code=410, detail=f"Book development request {status}")

    if status == "completed":
        raise HTTPException(status_code=409, detail="Book development request already completed")

    request_type = _validate_request_type(row["request_type"])
    if request_type not in {"AUTHOR_PHOTO", "ILLUSTRATOR_PHOTO"}:
        raise HTTPException(status_code=400, detail="This endpoint only accepts photo requests")

    kind = _safe(payload.kind)
    expected_kind = "author_photo" if request_type == "AUTHOR_PHOTO" else "illustrator_photo"
    if kind and kind != expected_kind:
        raise HTTPException(status_code=400, detail=f"Expected upload kind {expected_kind}")

    response_body = payload.model_dump(mode="json") if hasattr(payload, "model_dump") else payload.dict()
    response_body["kind"] = expected_kind

    with db_conn() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                """
                UPDATE bookdev_requests
                SET status = 'completed',
                    completed_at = now(),
                    response_json = %s::jsonb,
                    updated_at = now()
                WHERE id = %s::uuid
                """,
                (json.dumps(response_body), row["id"]),
            )
        conn.commit()

    requester_email = str(row.get("requester_email") or "").strip().lower()
    if requester_email:
        try:
            settings = _load_tenant_email_settings_or_400(row["tenant_slug"])
            username, password = _load_smtp_secret(settings["smtp_secret_id"])
            label = "Author photo" if request_type == "AUTHOR_PHOTO" else "Illustrator photo"
            subject = f"Completed: {label} – {_work_title(row)}"
            body_text = f"""Hello,

The {label.lower()} request has been completed.

Title: {_work_title(row)}
Uploaded file: {payload.filename or payload.key or payload.url or 'Photo uploaded'}

You can now review the uploaded photo in InkSuite.

{settings['from_name']}
"""
            _send_email_smtp(
                smtp_host=settings["smtp_host"],
                smtp_port=settings["smtp_port"],
                tls_mode=settings["tls_mode"],
                username=username,
                password=password,
                from_email=settings["from_email"],
                from_name=settings["from_name"],
                to_email=requester_email,
                to_name="",
                subject=subject,
                body_text=body_text,
            )
        except Exception as e:
            print("BOOKDEV PHOTO COMPLETION EMAIL FAILED:", repr(e))

    return {
        "ok": True,
        "completed": True,
        "request_id": row["id"],
        "request_type": request_type,
        "work_id": row["work_id"],
        "party": row.get("party") or "",
    }




@router.post("/requests/{token}/media-questionnaire")
def submit_media_questionnaire(
    token: str,
    payload: MediaQuestionnaireSubmitIn,
) -> Dict[str, Any]:
    row = _get_request_by_token_hash(_token_hash(token))

    if not row:
        raise HTTPException(status_code=404, detail="Book development request not found")

    status = _mark_expired_if_needed(
        request_id=row["id"],
        expires_at=row["expires_at"],
        status=row["status"],
    )

    if status in {"revoked", "expired"}:
        raise HTTPException(status_code=410, detail=f"Book development request {status}")

    if status == "completed":
        raise HTTPException(status_code=409, detail="Book development request already completed")

    request_type = _validate_request_type(row["request_type"])
    if request_type != "MEDIA_QUESTIONNAIRE":
        raise HTTPException(status_code=400, detail="This endpoint only accepts MEDIA_QUESTIONNAIRE requests")

    party = _validate_party(row["party"])
    scope, _ = _role_match_sql(party)
    tenant_id = row["tenant_id"]
    work_id = row["work_id"]
    response_body = payload.model_dump(mode="json") if hasattr(payload, "model_dump") else payload.dict()

    with db_conn() as conn:
        prev_ac = conn.autocommit
        conn.autocommit = False
        try:
            with conn.cursor(row_factory=dict_row) as cur:
                party_id = _find_contributor_party_id(cur, tenant_id, work_id, party)
                _save_media_questionnaire(cur, tenant_id, party_id, scope, payload)

                cur.execute(
                    """
                    UPDATE bookdev_requests
                    SET status = 'completed',
                        completed_at = now(),
                        response_json = %s::jsonb,
                        updated_at = now()
                    WHERE id = %s::uuid
                    """,
                    (json.dumps(response_body), row["id"]),
                )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.autocommit = prev_ac

    requester_email = str(row.get("requester_email") or "").strip().lower()
    if requester_email:
        try:
            settings = _load_tenant_email_settings_or_400(row["tenant_slug"])
            username, password = _load_smtp_secret(settings["smtp_secret_id"])
            label = "Author media questionnaire" if party == "author" else "Illustrator media questionnaire"
            subject = f"Completed: {label} – {_work_title(row)}"
            body_text = f"""Hello,

The {label.lower()} has been completed.

Title: {_work_title(row)}
Party: {party.title()}

You can now review the updated media questionnaire in InkSuite.

{settings['from_name']}
"""
            _send_email_smtp(
                smtp_host=settings["smtp_host"],
                smtp_port=settings["smtp_port"],
                tls_mode=settings["tls_mode"],
                username=username,
                password=password,
                from_email=settings["from_email"],
                from_name=settings["from_name"],
                to_email=requester_email,
                to_name="",
                subject=subject,
                body_text=body_text,
            )
        except Exception as e:
            print("BOOKDEV MEDIA COMPLETION EMAIL FAILED:", repr(e))

    return {
        "ok": True,
        "completed": True,
        "request_id": row["id"],
        "request_type": request_type,
        "work_id": work_id,
        "party": party,
    }

@router.post("/requests/{token}/contributor-info")
def submit_contributor_info(
    token: str,
    payload: ContributorInfoSubmitIn,
) -> Dict[str, Any]:
    row = _get_request_by_token_hash(_token_hash(token))

    if not row:
        raise HTTPException(status_code=404, detail="Book development request not found")

    status = _mark_expired_if_needed(
        request_id=row["id"],
        expires_at=row["expires_at"],
        status=row["status"],
    )

    if status in {"revoked", "expired"}:
        raise HTTPException(status_code=410, detail=f"Book development request {status}")

    if status == "completed":
        raise HTTPException(status_code=409, detail="Book development request already completed")

    request_type = _validate_request_type(row["request_type"])
    if request_type != "CONTRIBUTOR_INFO":
        raise HTTPException(status_code=400, detail="This endpoint only accepts CONTRIBUTOR_INFO requests")

    party = _validate_party(row["party"], request_type=request_type)
    request_payload = row.get("payload_json") or {}
    contributor_party_id = _optional_uuid(
        request_payload.get("contributor_party_id"),
        "contributor_party_id",
    )
    contributor_role_code = _safe(
        request_payload.get("contributor_role_code") or party
    ).upper()
    contributor_role_label = _safe(request_payload.get("contributor_role_label"))
    contributor_sequence_number = request_payload.get("contributor_sequence_number")

    if not contributor_party_id:
        raise HTTPException(
            status_code=409,
            detail="This contributor request does not contain a contributor_party_id; send a new request.",
        )

    tenant_id = row["tenant_id"]
    work_id = row["work_id"]

    contributor_name = _safe(payload.contributor_name or payload.contributor_display_name or row["recipient_name"])
    contributor_email = _safe(payload.contributor_email or row["recipient_email"]).lower()

    if not contributor_name:
        raise HTTPException(status_code=400, detail="Contributor name is required")
    if not contributor_email:
        raise HTTPException(status_code=400, detail="Contributor email is required")

    response_body = payload.model_dump(mode="json") if hasattr(payload, "model_dump") else payload.dict()
    response_body.update({
        "contributor_party_id": contributor_party_id,
        "contributor_role_code": contributor_role_code,
        "contributor_role_label": contributor_role_label,
        "contributor_sequence_number": contributor_sequence_number,
    })

    with db_conn() as conn:
        prev_ac = conn.autocommit
        conn.autocommit = False

        try:
            with conn.cursor(row_factory=dict_row) as cur:
                cur.execute(
                    """
                    SELECT p.id::text AS party_id
                    FROM parties p
                    JOIN work_contributors wc
                      ON wc.party_id = p.id
                     AND wc.tenant_id = p.tenant_id
                    WHERE p.tenant_id = %s::uuid
                      AND p.id = %s::uuid
                      AND wc.work_id = %s::uuid
                    LIMIT 1
                    """,
                    (tenant_id, contributor_party_id, work_id),
                )
                targeted_contributor = cur.fetchone()
                if not targeted_contributor:
                    raise HTTPException(
                        status_code=409,
                        detail="The contributor attached to this request is no longer linked to this work",
                    )

                # Update the exact contributor selected when the request was sent.
                # Never locate or create a different party by name or email here.
                # that can swallow SQL errors and leave this transaction aborted.
                _bookdev_update_contributor_core(
                    cur,
                    tenant_id,
                    contributor_party_id,
                    payload,
                    contributor_name,
                    contributor_email,
                )

                _ensure_work_contributor(
                    cur,
                    tenant_id,
                    work_id,
                    contributor_party_id,
                    contributor_role_code,
                    contributor_sequence_number,
                )

                # If the public form includes agent/agency information, write the
                # same structure the Book Information page already reads:
                # party_representations links contributor -> agent for this work,
                # and agency_agent_links links agency -> agent.
                if bool(payload.has_agent):
                    _bookdev_replace_party_representation(
                        cur,
                        tenant_id,
                        contributor_party_id,
                        work_id,
                        payload,
                    )

                cur.execute(
                    """
                    UPDATE bookdev_requests
                    SET status = 'completed',
                        completed_at = now(),
                        response_json = %s::jsonb,
                        updated_at = now()
                    WHERE id = %s::uuid
                    """,
                    (
                        json.dumps(response_body),
                        row["id"],
                    ),
                )

            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.autocommit = prev_ac

    requester_email = str(row.get("requester_email") or "").strip().lower()
    if requester_email:
        try:
            settings = _load_tenant_email_settings_or_400(row["tenant_slug"])
            username, password = _load_smtp_secret(settings["smtp_secret_id"])

            subject, body_text = _render_completion_email(
                title=_work_title(row),
                party=contributor_role_label or contributor_role_code or party,
                contributor_name=contributor_name,
                signature=settings["from_name"],
            )

            _send_email_smtp(
                smtp_host=settings["smtp_host"],
                smtp_port=settings["smtp_port"],
                tls_mode=settings["tls_mode"],
                username=username,
                password=password,
                from_email=settings["from_email"],
                from_name=settings["from_name"],
                to_email=requester_email,
                to_name="",
                subject=subject,
                body_text=body_text,
            )
        except Exception as e:
            print("BOOKDEV COMPLETION EMAIL FAILED:", repr(e))

    return {
        "ok": True,
        "completed": True,
        "request_id": row["id"],
        "request_type": request_type,
        "work_id": work_id,
        "party": party,
    }
@router.post("/requests/{token}/marketing-profile")
def submit_marketing_profile(token: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    return _submit_questionnaire_like_request(
        token=token,
        payload=payload,
        expected_type="MARKETING_PROFILE",
        response_key="marketing_profile",
    )

@router.post("/requests/{token}/sales-information")
def submit_sales_information(token: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    return _submit_questionnaire_like_request(
        token=token,
        payload=payload,
        expected_type="SALES_INFORMATION",
        response_key="sales",
    )

def _submit_questionnaire_like_request(
    *,
    token: str,
    payload: Dict[str, Any],
    expected_type: str,
    response_key: str,
) -> Dict[str, Any]:
    row = _get_request_by_token_hash(_token_hash(token))
    if not row:
        raise HTTPException(status_code=404, detail="Book development request not found")

    status = _mark_expired_if_needed(
        request_id=row["id"],
        expires_at=row["expires_at"],
        status=row["status"],
    )
    if status in {"revoked", "expired"}:
        raise HTTPException(status_code=410, detail=f"Book development request {status}")
    if status == "completed":
        raise HTTPException(status_code=409, detail="Book development request already completed")

    request_type = _validate_request_type(row["request_type"])
    if request_type != expected_type:
        raise HTTPException(status_code=400, detail=f"This endpoint only accepts {expected_type} requests")

    party = _validate_party(row["party"])
    response_json = {
        response_key: payload or {},
        "party": party,
        "request_type": request_type,
        "submitted_at": _now_utc().isoformat(),
    }

    with db_conn() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                """
                UPDATE bookdev_requests
                SET status = 'completed',
                    completed_at = now(),
                    response_json = %s::jsonb,
                    updated_at = now()
                WHERE id = %s::uuid
                """,
                (json.dumps(response_json), row["id"]),
            )
        conn.commit()

    requester_email = str(row.get("requester_email") or "").strip().lower()
    if requester_email:
        try:
            settings = _load_tenant_email_settings_or_400(row["tenant_slug"])
            username, password = _load_smtp_secret(settings["smtp_secret_id"])
            title = _work_title(row)
            subject = f"Completed: {expected_type.replace('_', ' ').title()} – {title}"
            body_text = f"""Hello,

The {expected_type.replace('_', ' ').title()} request has been completed.

Title: {title}
Party: {party.title()}

You can now review the submitted information in InkSuite.

{settings["from_name"]}
"""
            _send_email_smtp(
                smtp_host=settings["smtp_host"],
                smtp_port=settings["smtp_port"],
                tls_mode=settings["tls_mode"],
                username=username,
                password=password,
                from_email=settings["from_email"],
                from_name=settings["from_name"],
                to_email=requester_email,
                to_name="",
                subject=subject,
                body_text=body_text,
            )
        except Exception as e:
            print("BOOKDEV QUESTIONNAIRE COMPLETION EMAIL FAILED:", repr(e))

    return {
        "ok": True,
        "completed": True,
        "request_id": row["id"],
        "request_type": request_type,
        "work_id": row["work_id"],
        "party": party,
    }

# -----------------------------------------------------------------------------
# Book Development workspace cleanup endpoints
# Team members + user-specific task state/dashboard flags
# -----------------------------------------------------------------------------

INTERNAL_TEAM_ROLE_KEYS = {
    "editor",
    "art_director",
    "copyeditor",
    "proofreader",
    "book_designer",
}


class BookDevTeamMemberIn(BaseModel):
    display_name: str = ""
    email: str = ""

    class Config:
        extra = "ignore"

    @validator("*", pre=True)
    def trim_strings(cls, v):
        if isinstance(v, str):
            return v.strip()
        return v


class BookDevTeamMembersIn(BaseModel):
    team_members: Dict[str, BookDevTeamMemberIn] = {}

    class Config:
        extra = "ignore"


class BookDevTaskStateIn(BaseModel):
    task_key: str
    is_complete: bool = False
    is_on_dashboard: bool = True

    class Config:
        extra = "ignore"

    @validator("task_key", pre=True)
    def trim_task_key(cls, v):
        return str(v or "").strip()


def _load_project_contributors_for_team(cur, tenant_id: str, work_id: str) -> Dict[str, Dict[str, str]]:
    out = {
        "author": {"display_name": "", "email": ""},
        "illustrator": {"display_name": "", "email": ""},
    }

    cur.execute(
        """
        SELECT
            upper(COALESCE(wc.contributor_role, '')) AS contributor_role,
            p.display_name,
            p.email,
            wc.sequence_number
        FROM work_contributors wc
        JOIN parties p
          ON p.id = wc.party_id
         AND p.tenant_id = wc.tenant_id
        WHERE wc.tenant_id = %s::uuid
          AND wc.work_id = %s::uuid
        ORDER BY wc.sequence_number NULLS LAST, p.created_at NULLS LAST, p.display_name
        """,
        (tenant_id, work_id),
    )
    rows = cur.fetchall() or []

    for r in rows:
        role = _safe(r.get("contributor_role")).upper()
        slot = ""
        if role in {"AUTHOR", "A01", "WRITER", "PRIMARY AUTHOR"}:
            slot = "author"
        elif role in {"ILLUSTRATOR", "A12", "ARTIST", "ILLUSTRATION"}:
            slot = "illustrator"

        if slot and not out[slot]["display_name"]:
            out[slot] = {
                "display_name": _safe(r.get("display_name")),
                "email": _safe(r.get("email")),
            }

    return out


def _empty_team_members() -> Dict[str, Dict[str, str]]:
    return {role: {"display_name": "", "email": ""} for role in sorted(INTERNAL_TEAM_ROLE_KEYS)}


def _load_project_team_members(cur, tenant_id: str, work_id: str) -> Dict[str, Dict[str, str]]:
    out = _empty_team_members()
    cur.execute(
        """
        SELECT role_key, display_name, email
        FROM bookdev_project_team_members
        WHERE tenant_id = %s::uuid
          AND work_id = %s::uuid
        ORDER BY role_key ASC
        """,
        (tenant_id, work_id),
    )
    for r in cur.fetchall() or []:
        role_key = _safe(r.get("role_key")).lower()
        if role_key in INTERNAL_TEAM_ROLE_KEYS:
            out[role_key] = {
                "display_name": _safe(r.get("display_name")),
                "email": _safe(r.get("email")),
            }
    return out


@router.get("/{work_id}/team")
def get_bookdev_team_members(
    work_id: str,
    request: Request,
    tenant_slug: str = Query(..., description="Tenant slug, e.g. marble-press"),
    ctx=Depends(_ctx_from_bearer),
) -> Dict[str, Any]:
    mctx = _load_user_and_membership_or_403(tenant_slug=tenant_slug, ctx=ctx)

    with db_conn() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            work = _load_work_or_404(cur, mctx["tenant_id"], work_id)
            contributors = _load_project_contributors_for_team(cur, mctx["tenant_id"], work_id)
            team_members = _load_project_team_members(cur, mctx["tenant_id"], work_id)

    return {
        "ok": True,
        "tenant_slug": tenant_slug,
        "work_id": work["id"],
        "title": _work_title(work),
        "contributors": contributors,
        "team_members": team_members,
    }


@router.post("/{work_id}/team")
def save_bookdev_team_members(
    work_id: str,
    payload: BookDevTeamMembersIn,
    request: Request,
    tenant_slug: str = Query(..., description="Tenant slug, e.g. marble-press"),
    ctx=Depends(_ctx_from_bearer),
) -> Dict[str, Any]:
    mctx = _load_user_and_membership_or_403(tenant_slug=tenant_slug, ctx=ctx)

    incoming = payload.team_members or {}

    with db_conn() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            work = _load_work_or_404(cur, mctx["tenant_id"], work_id)

            for role_key in sorted(INTERNAL_TEAM_ROLE_KEYS):
                member = incoming.get(role_key) or BookDevTeamMemberIn()
                display_name = _safe(getattr(member, "display_name", ""))
                email = _safe(getattr(member, "email", "")).lower()

                cur.execute(
                    """
                    INSERT INTO bookdev_project_team_members
                      (tenant_id, work_id, role_key, display_name, email)
                    VALUES
                      (%s::uuid, %s::uuid, %s, %s, %s)
                    ON CONFLICT (tenant_id, work_id, role_key)
                    DO UPDATE SET
                      display_name = EXCLUDED.display_name,
                      email = EXCLUDED.email,
                      updated_at = now()
                    """,
                    (mctx["tenant_id"], work_id, role_key, display_name, email),
                )

            contributors = _load_project_contributors_for_team(cur, mctx["tenant_id"], work_id)
            team_members = _load_project_team_members(cur, mctx["tenant_id"], work_id)
        conn.commit()

    return {
        "ok": True,
        "tenant_slug": tenant_slug,
        "work_id": work["id"],
        "title": _work_title(work),
        "contributors": contributors,
        "team_members": team_members,
    }


@router.get("/{work_id}/task-state")
def get_bookdev_task_state(
    work_id: str,
    request: Request,
    tenant_slug: str = Query(..., description="Tenant slug, e.g. marble-press"),
    ctx=Depends(_ctx_from_bearer),
) -> Dict[str, Any]:
    mctx = _load_user_and_membership_or_403(tenant_slug=tenant_slug, ctx=ctx)

    with db_conn() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            work = _load_work_or_404(cur, mctx["tenant_id"], work_id)
            cur.execute(
                """
                SELECT task_key, is_complete, is_on_dashboard, completed_at::text AS completed_at
                FROM bookdev_user_work_items
                WHERE tenant_id = %s::uuid
                  AND user_id = %s::uuid
                  AND work_id = %s::uuid
                ORDER BY task_key ASC
                """,
                (mctx["tenant_id"], mctx["user_id"], work_id),
            )
            rows = cur.fetchall() or []

    task_state: Dict[str, bool] = {}
    items = []
    is_on_dashboard = False

    for r in rows:
        task_key = _safe(r.get("task_key"))
        item = {
            "task_key": task_key,
            "is_complete": bool(r.get("is_complete")),
            "is_on_dashboard": bool(r.get("is_on_dashboard")),
            "completed_at": r.get("completed_at") or None,
        }
        items.append(item)
        if task_key == "__dashboard__":
            is_on_dashboard = bool(r.get("is_on_dashboard"))
        elif task_key:
            task_state[task_key] = bool(r.get("is_complete"))

    return {
        "ok": True,
        "tenant_slug": tenant_slug,
        "work_id": work["id"],
        "title": _work_title(work),
        "is_on_dashboard": is_on_dashboard,
        "task_state": task_state,
        "items": items,
    }


@router.post("/{work_id}/task-state")
def save_bookdev_task_state(
    work_id: str,
    payload: BookDevTaskStateIn,
    request: Request,
    tenant_slug: str = Query(..., description="Tenant slug, e.g. marble-press"),
    ctx=Depends(_ctx_from_bearer),
) -> Dict[str, Any]:
    mctx = _load_user_and_membership_or_403(tenant_slug=tenant_slug, ctx=ctx)
    task_key = _safe(payload.task_key)
    if not task_key:
        raise HTTPException(status_code=400, detail="task_key is required")

    with db_conn() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            work = _load_work_or_404(cur, mctx["tenant_id"], work_id)
            cur.execute(
                """
                INSERT INTO bookdev_user_work_items
                  (tenant_id, user_id, work_id, task_key, is_complete, completed_at, is_on_dashboard)
                VALUES
                  (
                    %s::uuid,
                    %s::uuid,
                    %s::uuid,
                    %s,
                    %s,
                    CASE WHEN %s THEN now() ELSE NULL END,
                    %s
                  )
                ON CONFLICT (tenant_id, user_id, work_id, task_key)
                DO UPDATE SET
                  is_complete = EXCLUDED.is_complete,
                  completed_at = CASE WHEN EXCLUDED.is_complete THEN COALESCE(bookdev_user_work_items.completed_at, now()) ELSE NULL END,
                  is_on_dashboard = EXCLUDED.is_on_dashboard,
                  updated_at = now()
                RETURNING task_key, is_complete, is_on_dashboard, completed_at::text AS completed_at
                """,
                (
                    mctx["tenant_id"],
                    mctx["user_id"],
                    work_id,
                    task_key,
                    bool(payload.is_complete),
                    bool(payload.is_complete),
                    bool(payload.is_on_dashboard),
                ),
            )
            saved = cur.fetchone()
        conn.commit()

    return {
        "ok": True,
        "tenant_slug": tenant_slug,
        "work_id": work["id"],
        "title": _work_title(work),
        "item": dict(saved) if saved else {},
    }


@router.post("/{work_id}/dashboard")
def save_bookdev_dashboard_membership(
    work_id: str,
    payload: Dict[str, Any],
    request: Request,
    tenant_slug: str = Query(..., description="Tenant slug, e.g. marble-press"),
    ctx=Depends(_ctx_from_bearer),
) -> Dict[str, Any]:
    mctx = _load_user_and_membership_or_403(tenant_slug=tenant_slug, ctx=ctx)
    is_on_dashboard = bool(payload.get("is_on_dashboard", True))

    with db_conn() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            work = _load_work_or_404(cur, mctx["tenant_id"], work_id)
            cur.execute(
                """
                INSERT INTO bookdev_user_work_items
                  (tenant_id, user_id, work_id, task_key, is_complete, is_on_dashboard)
                VALUES
                  (%s::uuid, %s::uuid, %s::uuid, '__dashboard__', false, %s)
                ON CONFLICT (tenant_id, user_id, work_id, task_key)
                DO UPDATE SET
                  is_on_dashboard = EXCLUDED.is_on_dashboard,
                  updated_at = now()
                RETURNING task_key, is_complete, is_on_dashboard, completed_at::text AS completed_at
                """,
                (mctx["tenant_id"], mctx["user_id"], work_id, is_on_dashboard),
            )
            saved = cur.fetchone()
        conn.commit()

    return {
        "ok": True,
        "tenant_slug": tenant_slug,
        "work_id": work["id"],
        "title": _work_title(work),
        "item": dict(saved) if saved else {},
    }


# Backfill helper expected by resolve_bookdev_request_token for public request pages.
# It keeps the public pages from failing if marketing/sales detail tables are added later.
def _load_public_form_prefill(
    *,
    tenant_id: str,
    work_id: str,
    party: str,
) -> Dict[str, Any]:
    base = _load_media_questionnaire_prefill(tenant_id=tenant_id, work_id=work_id, party=party)
    media = base.get("media") or {}

    marketing_profile = {
        "website_bio": media.get("website_bio") or "",
        "book_bio": media.get("book_bio") or "",
        "media_best_times": media.get("media_best_times") or "",
        "us_travel_plans": media.get("us_travel_plans") or "",
        "travel_dates": media.get("travel_dates") or "",
        "photo_credit": media.get("photo_credit") or "",
        "present_position": media.get("present_position") or "",
        "former_positions": media.get("former_positions") or "",
        "degrees_honors": media.get("degrees_honors") or "",
        "professional_honors": media.get("professional_honors") or "",
        "additional_notes": media.get("additional_notes") or "",
        "contact_pref_rank1": media.get("contact_pref_rank1") or "",
        "contact_pref_rank2": media.get("contact_pref_rank2") or "",
        "marketing_endorsers": [],
        "marketing_big_mouth_list": [],
        "marketing_review_copy_wishlist": [],
        "marketing_local_media": [],
        "marketing_alumni_org_publications": [],
        "marketing_targeted_sites": [],
        "marketing_bloggers": [],
    }

    sales = {
        "local_bookstores": [],
        "schools_libraries": [],
        "societies_orgs_conf": [],
        "nontrade_outlets": [],
        "museums_parks": [],
    }

    return {
        "contributor": base.get("contributor") or {},
        "media": media,
        "marketing_profile": marketing_profile,
        "sales": sales,
    }
@router.get("/dashboard")
def get_bookdev_dashboard(
    request: Request,
    tenant_slug: str = Query(...),
    ctx=Depends(_ctx_from_bearer),
) -> Dict[str, Any]:
    mctx = _load_user_and_membership_or_403(tenant_slug=tenant_slug, ctx=ctx)

    with db_conn() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                """
                SELECT
                    work_id::text AS work_id,
                    COALESCE(pinned, false) AS pinned,
                    COALESCE(sort_order, 0) AS sort_order,
                    created_at::text AS added_at
                FROM bookdev_user_work_items
                WHERE tenant_id = %s::uuid
                  AND user_id = %s::uuid
                  AND task_key = '__dashboard__'
                  AND is_on_dashboard = true
                ORDER BY COALESCE(pinned, false) DESC,
                         COALESCE(sort_order, 0) ASC,
                         created_at ASC
                """,
                (mctx["tenant_id"], mctx["user_id"]),
            )
            rows = cur.fetchall() or []

    return {
        "ok": True,
        "items": [
            {
                "work_id": r["work_id"],
                "pinned": bool(r.get("pinned")),
                "sort_order": int(r.get("sort_order") or 0),
                "added_at": r.get("added_at") or "",
            }
            for r in rows
        ],
    }

@router.post("/dashboard")
def save_bookdev_dashboard(
    payload: DashboardSaveIn,
    request: Request,
    tenant_slug: str = Query(...),
    ctx=Depends(_ctx_from_bearer),
) -> Dict[str, Any]:
    mctx = _load_user_and_membership_or_403(tenant_slug=tenant_slug, ctx=ctx)

    incoming = []
    seen = set()
    for idx, item in enumerate(payload.items or []):
        work_id = _safe(item.work_id)
        if not work_id or work_id in seen:
            continue
        seen.add(work_id)
        incoming.append({
            "work_id": work_id,
            "pinned": bool(item.pinned),
            "sort_order": int(item.sort_order if item.sort_order is not None else idx),
        })

    with db_conn() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                """
                UPDATE bookdev_user_work_items
                SET is_on_dashboard = false,
                    updated_at = now()
                WHERE tenant_id = %s::uuid
                  AND user_id = %s::uuid
                  AND task_key = '__dashboard__'
                """,
                (mctx["tenant_id"], mctx["user_id"]),
            )

            for idx, item in enumerate(incoming):
                cur.execute(
                    """
                    INSERT INTO bookdev_user_work_items (
                        tenant_id,
                        user_id,
                        work_id,
                        is_on_dashboard,
                        task_key,
                        is_complete,
                        pinned,
                        sort_order
                    )
                    VALUES (
                        %s::uuid,
                        %s::uuid,
                        %s::uuid,
                        true,
                        '__dashboard__',
                        false,
                        %s,
                        %s
                    )
                    ON CONFLICT (tenant_id, user_id, work_id, task_key)
                    DO UPDATE SET
                        is_on_dashboard = true,
                        pinned = EXCLUDED.pinned,
                        sort_order = EXCLUDED.sort_order,
                        updated_at = now()
                    """,
                    (
                        mctx["tenant_id"],
                        mctx["user_id"],
                        item["work_id"],
                        item["pinned"],
                        idx,
                    ),
                )

        conn.commit()

    return {"ok": True, "items": incoming}

@router.get("/{work_id}/timeline")
def get_project_timeline(
    work_id: str,
    request: Request,
    tenant_slug: str = Query(...),
    ctx=Depends(_ctx_from_bearer),
) -> Dict[str, Any]:
    mctx = _load_user_and_membership_or_403(
        tenant_slug=tenant_slug,
        ctx=ctx,
    )

    with db_conn() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            _load_work_or_404(cur, mctx["tenant_id"], work_id)

            cur.execute(
                """
                SELECT
                    id::text AS id,
                    task_name,
                    deadline,
                    sort_order
                FROM bookdev_project_timeline_items
                WHERE tenant_id = %s::uuid
                  AND work_id = %s::uuid
                ORDER BY sort_order ASC, deadline ASC, created_at ASC
                """,
                (mctx["tenant_id"], work_id),
            )
            rows = cur.fetchall() or []

    return {
        "ok": True,
        "work_id": work_id,
        "items": [_timeline_row_to_dict(dict(row)) for row in rows],
    }


@router.post("/{work_id}/timeline")
def save_project_timeline(
    work_id: str,
    payload: ProjectTimelineSaveIn,
    request: Request,
    tenant_slug: str = Query(...),
    ctx=Depends(_ctx_from_bearer),
) -> Dict[str, Any]:
    mctx = _load_user_and_membership_or_403(
        tenant_slug=tenant_slug,
        ctx=ctx,
    )

    cleaned: list[Dict[str, Any]] = []
    for index, item in enumerate(payload.items or []):
        task_name = _safe(item.task_name)
        if not task_name:
            raise HTTPException(
                status_code=400,
                detail="Every timeline item must have a task name",
            )

        try:
            deadline = datetime.strptime(item.deadline, "%m/%d/%Y").date()
        except ValueError:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid deadline for {task_name}. Use MM/DD/YYYY.",
            )

        cleaned.append(
            {
                "task_name": task_name,
                "deadline": deadline,
                "sort_order": index,
            }
        )

    with db_conn() as conn:
        prev_ac = conn.autocommit
        conn.autocommit = False

        try:
            with conn.cursor(row_factory=dict_row) as cur:
                _load_work_or_404(cur, mctx["tenant_id"], work_id)

                cur.execute(
                    """
                    DELETE FROM bookdev_project_timeline_items
                    WHERE tenant_id = %s::uuid
                      AND work_id = %s::uuid
                    """,
                    (mctx["tenant_id"], work_id),
                )

                for item in cleaned:
                    cur.execute(
                        """
                        INSERT INTO bookdev_project_timeline_items (
                            tenant_id,
                            work_id,
                            task_name,
                            deadline,
                            sort_order
                        )
                        VALUES (
                            %s::uuid,
                            %s::uuid,
                            %s,
                            %s,
                            %s
                        )
                        """,
                        (
                            mctx["tenant_id"],
                            work_id,
                            item["task_name"],
                            item["deadline"],
                            item["sort_order"],
                        ),
                    )

                cur.execute(
                    """
                    SELECT
                        id::text AS id,
                        task_name,
                        deadline,
                        sort_order
                    FROM bookdev_project_timeline_items
                    WHERE tenant_id = %s::uuid
                      AND work_id = %s::uuid
                    ORDER BY sort_order ASC, deadline ASC, created_at ASC
                    """,
                    (mctx["tenant_id"], work_id),
                )
                rows = cur.fetchall() or []

            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.autocommit = prev_ac

    return {
        "ok": True,
        "work_id": work_id,
        "items": [_timeline_row_to_dict(dict(row)) for row in rows],
    }
