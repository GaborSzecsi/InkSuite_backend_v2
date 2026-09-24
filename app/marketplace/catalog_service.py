"""Marketplace catalog service; transactions and mutation boundaries live here."""

from . import catalog_repository as _repository
from uuid import UUID
from fastapi import APIRouter, Depends, HTTPException, Query
from .core import (
    transaction,
    one,
    rows,
    required,
    current_user,
    organization_permission,
    safe_url,
    image_ref,
    public_actor,
)
from .schemas import Organization, Listing, LibraryItem

router = APIRouter()
BOOK_JOIN = """FROM marketplace_books b JOIN marketplace_organizations o ON o.id=b.publisher_organization_id
 JOIN works w ON w.id=b.work_id AND w.tenant_id=o.tenant_id"""
PUBLIC_BOOK = "b.marketplace_status='public' AND o.status='active' AND o.organization_type='publisher'"


def legacy_cover(publisher_slug, upload_uid, tenant_id, cur):
    # Resolve the tenant's storage identity from the catalog, never a request path.
    tenant = _repository.legacy_cover_query_1(cur, tenant_id)
    if not tenant:
        return ""
    return _legacy_cover_url(tenant["slug"], str(upload_uid))


from functools import lru_cache
from time import time


def _legacy_cover_url(tenant_slug, upload_uid):
    return _cached_legacy_cover(tenant_slug, upload_uid, int(time() // 300))


@lru_cache(maxsize=256)
def _cached_legacy_cover(tenant_slug, upload_uid, cache_window):
    from routers.uploads import _s3_client, S3_BUCKET
    from botocore.exceptions import BotoCoreError, ClientError

    # Public legacy cover files only; never expose private title assets.
    prefix = f"tenants/{tenant_slug}/data/uploads/{UUID(upload_uid)}/"
    try:
        client = _s3_client()
        candidates = []
        for page in client.get_paginator("list_objects_v2").paginate(
            Bucket=S3_BUCKET, Prefix=prefix
        ):
            for item in page.get("Contents", []):
                key = item["Key"]
                relative = key[len(prefix) :] if key.startswith(prefix) else ""
                name = relative.rsplit("/", 1)[-1].lower()
                is_public = relative.startswith("public/") or "/" not in relative
                is_cover = (
                    "retailcover" in name
                    or "__cover." in name
                    or name.startswith("cover.")
                )
                if (
                    is_public
                    and is_cover
                    and name.endswith((".jpg", ".jpeg", ".png", ".webp"))
                ):
                    candidates.append(key)
        if candidates:
            key = sorted(candidates, key=lambda k: ("/public/" not in k, k))[0]
            return client.generate_presigned_url(
                "get_object", Params={"Bucket": S3_BUCKET, "Key": key}, ExpiresIn=900
            )
    except (BotoCoreError, ClientError):
        pass
    return ""


def book_view(cur, book_id, detail=False):
    b = required(_repository.book_view_query_3(cur, PUBLIC_BOOK, BOOK_JOIN, book_id))
    contributors = _repository.book_view_query_1(cur, b)
    editions = _repository.book_view_query_2(cur, b)
    for e in editions:
        e["cover"] = safe_url(e.pop("cover_image_link"))
        # Explicit retail price types, with currency/territory shown; no wholesale data.
        e["prices"] = _repository.book_view_query_4(cur, e, b)
        e["subjects"] = _repository.book_view_query_5(cur, e, b)
    if detail:
        for e in editions:
            e["rights"] = _repository.book_view_query_7(cur, e, b)
            e["copyright"] = _repository.book_view_query_8(cur, e, b)
        publisher_actor = _repository.book_view_query_6(cur, b)
        b["publisher_actor_id"] = publisher_actor["id"] if publisher_actor else None
    b["contributors"] = contributors
    b["editions"] = editions
    b["cover"] = next((e["cover"] for e in editions if e["cover"]), "")
    if not b["cover"]:
        b["cover"] = legacy_cover(
            b["publisher_slug"],
            b.get("upload_uid") or b["work_id"],
            b["tenant_id"],
            cur,
        )
    b.pop("upload_uid", None)
    b["publication_date"] = editions[0]["publication_date"] if editions else None
    b.pop("tenant_id")
    b.pop("work_id")
    return b


def books(
    q: str = Query("", max_length=200),
    publisher: str = Query("", max_length=100),
    period: str = "",
    audience: str = "",
    category: str = "",
    subject: str = Query("", max_length=100),
    offset: int = Query(0, ge=0, le=10000),
    limit: int = Query(18, ge=1, le=40),
):
    with transaction() as cur:
        where = PUBLIC_BOOK + " AND b.discoverable"
        params = []
        if publisher:
            where += " AND o.slug=%s"
            params.append(publisher)
        if q:
            where += """ AND (w.title ILIKE %s OR o.name ILIKE %s OR EXISTS(SELECT 1 FROM work_contributors c
              JOIN parties p ON p.id=c.party_id AND p.tenant_id=c.tenant_id WHERE c.work_id=w.id AND c.tenant_id=w.tenant_id AND p.display_name ILIKE %s))"""
            params += ["%" + q + "%"] * 3
        edition_join = """FROM marketplace_book_editions be JOIN editions e ON e.id=be.edition_id AND e.work_id=w.id AND e.tenant_id=w.tenant_id"""
        audience_codes = {"adult": "01", "young-adult": "03", "children": "02"}
        if audience in audience_codes:
            where += (
                " AND EXISTS(SELECT 1 "
                + edition_join
                + " JOIN edition_audience a ON a.edition_id=e.id AND a.tenant_id=e.tenant_id WHERE be.marketplace_book_id=b.id AND a.onix_audience_code=%s)"
            )
            params.append(audience_codes[audience])
        category_terms = {
            "Sci-Fi & Fantasy": ["Science Fiction", "Fantasy"],
            "Mystery & Thrillers": ["Mystery", "Thriller", "Suspense"],
            "Romance": ["Romance"],
            "Historical": ["Historical"],
            "Adventure": ["Adventure"],
            "Paranormal": ["Paranormal"],
            "Horror": ["Horror"],
            "Literary": ["Literary"],
            "Picture Books": ["Picture Book"],
            "Middle Grade": ["Middle Grade"],
            "Graphic Novels": ["Graphic Novel", "Comics"],
            "Nonfiction": ["Nonfiction", "Non-fiction"],
            "Emotions": ["Emotion", "Feeling"],
            "Body Safety": ["Body Safety", "Personal Safety"],
        }
        if category in category_terms:
            terms = category_terms[category]
            where += (
                " AND EXISTS(SELECT 1 "
                + edition_join
                + " JOIN edition_subjects s ON s.edition_id=e.id AND s.tenant_id=e.tenant_id WHERE be.marketplace_book_id=b.id AND ("
                + " OR ".join(["s.heading_text ILIKE %s"] * len(terms))
                + "))"
            )
            params.extend(["%" + term + "%" for term in terms])
        if period in ("coming", "new"):
            where += (
                " AND EXISTS(SELECT 1 "
                + edition_join
                + """ JOIN edition_publishing_dates d ON d.edition_id=e.id AND d.tenant_id=e.tenant_id
              WHERE be.marketplace_book_id=b.id AND d.date_role='01' AND """
                + (
                    "d.date_value>current_date"
                    if period == "coming"
                    else "d.date_value BETWEEN current_date-interval '90 days' AND current_date"
                )
                + ")"
            )
        if period == "available":
            where += (
                " AND EXISTS(SELECT 1 "
                + edition_join
                + " JOIN edition_supply_details s ON s.edition_id=e.id AND s.tenant_id=e.tenant_id WHERE be.marketplace_book_id=b.id AND s.product_availability IN ('20','21','22'))"
            )
        if subject:
            where += (
                " AND EXISTS(SELECT 1 "
                + edition_join
                + " JOIN edition_subjects s ON s.edition_id=e.id AND s.tenant_id=e.tenant_id WHERE be.marketplace_book_id=b.id AND (s.subject_code=%s OR s.heading_text ILIKE %s))"
            )
            params += [subject, "%" + subject + "%"]
        ids = _repository.books_query_1(cur, where, BOOK_JOIN, params, offset, limit)
        return {
            "items": [book_view(cur, x["id"]) for x in ids[:limit]],
            "has_more": len(ids) > limit,
        }


def book(slug: str):
    with transaction() as cur:
        b = required(_repository.book_query_1(cur, slug))
        return book_view(cur, b["id"], detail=True)


def publishers(offset: int = Query(0, ge=0, le=10000)):
    with transaction() as cur:
        data = _repository.publishers_query_1(cur, offset)
        for x in data:
            x["image"] = image_ref(x.pop("logo_asset_ref"))
        return {"items": data[:30], "has_more": len(data) > 30}


def publisher(slug: str):
    with transaction() as cur:
        o = required(_repository.publisher_query_1(cur, slug))
        a = required(_repository.publisher_query_2(cur, o))
        o["actor"] = public_actor(cur, a["id"])
        o["website"] = safe_url(o["website"])
        o["logo"] = image_ref(o.pop("logo_asset_ref"))
        o["banner"] = image_ref(o.pop("banner_asset_ref"))
        o["followers"] = _repository.publisher_query_3(cur, a)["n"]
        return o


def initialize(tenant_id: UUID, user=Depends(current_user)):
    with transaction() as cur:
        tenant = required(_repository.initialize_query_4(cur, tenant_id))
        member = _repository.initialize_query_1(cur, tenant_id, user)
        if user.get("platform_role") != "superadmin" and (
            not member or member["role"] != "tenant_admin"
        ):
            raise HTTPException(
                403, "Only an organization administrator can initialize a storefront."
            )
        org = _repository.initialize_query_2(cur, tenant_id, tenant)
        _repository.initialize_query_3(cur, org)
        return org


def org_settings(org_id: UUID, user=Depends(current_user)):
    with transaction() as cur:
        o = organization_permission(cur, user, org_id, admin=True)
        return {
            k: o[k]
            for k in (
                "id",
                "name",
                "slug",
                "description",
                "website",
                "location_text",
                "status",
            )
        }


def save_org(org_id: UUID, body: Organization, user=Depends(current_user)):
    with transaction() as cur:
        organization_permission(cur, user, org_id, admin=True)
        if body.website and not safe_url(body.website):
            raise HTTPException(422, "Use an https website address.")
        return _repository.save_org_query_1(cur, org_id, body)


def manage_catalog(
    org_id: UUID,
    q: str = Query("", max_length=200),
    offset: int = Query(0, ge=0, le=10000),
    user=Depends(current_user),
):
    with transaction() as cur:
        o = organization_permission(cur, user, org_id, admin=True)
        data = _repository.manage_catalog_query_1(cur, org_id, offset, o, q)
        for w in data:
            w["editions"] = _repository.manage_catalog_query_2(cur, w, o)
        return {"items": data[:30], "has_more": len(data) > 30}


def save_listing(org_id: UUID, body: Listing, user=Depends(current_user)):
    with transaction() as cur:
        o = organization_permission(cur, user, org_id, admin=True)
        required(_repository.save_listing_query_3(cur, body, o))
        if body.marketplace_status == "public" and not body.edition_ids:
            raise HTTPException(422, "Choose at least one edition before publishing.")
        result = _repository.save_listing_query_1(cur, org_id, body)
        required(result)
        _repository.save_listing_query_2(cur, result)
        for i, edition in enumerate(dict.fromkeys(body.edition_ids)):
            _repository.save_listing_query_4(cur, edition, result, i)
        return result


def library(offset: int = Query(0, ge=0, le=10000), user=Depends(current_user)):
    with transaction() as cur:
        data = _repository.library_query_1(cur, PUBLIC_BOOK, offset, user)
        return {
            "items": [
                dict(
                    book_view(cur, x["marketplace_book_id"]), library_status=x["status"]
                )
                for x in data[:30]
            ],
            "has_more": len(data) > 30,
        }


def save_library(book_id: UUID, body: LibraryItem, user=Depends(current_user)):
    with transaction() as cur:
        book_view(cur, book_id)
        required(
            _repository.save_library_query_2(cur, user),
            "Create your Marketplace profile first.",
        )
        _repository.save_library_query_1(cur, book_id, user, body)
        return {"ok": True}


def remove_library(book_id: UUID, user=Depends(current_user)):
    with transaction() as cur:
        from . import arc_repository
        if arc_repository.ready(cur) and arc_repository.library_asset(cur, user["id"], book_id):
            raise HTTPException(409, "ARC access and reading history remain in your Library.")
        _repository.remove_library_query_1(cur, book_id, user)
        return {"ok": True}


def publisher_catalogs(slug: str):
    with transaction() as cur:
        org = required(_repository.publisher_catalogs_query_2(cur, slug))
        assets = _repository.publisher_catalogs_query_1(cur, org)
        return {
            "items": [
                {
                    "id": a["id"],
                    "name": a["asset_name"] or "Publisher catalog",
                    "url": safe_url(a["public_url"]),
                    "format": a["file_format"],
                }
                for a in assets
                if safe_url(a["public_url"])
            ]
        }
