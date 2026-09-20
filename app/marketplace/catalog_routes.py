from .observability import TimedRoute

"""HTTP adapters; service functions also support non-HTTP callers."""
from .catalog_service import *
from . import catalog_service as service

router = APIRouter(route_class=TimedRoute)


@router.get("/books")
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
    return service.books(
        q, publisher, period, audience, category, subject, offset, limit
    )


@router.get("/books/{slug}")
def book(slug: str):
    return service.book(slug)


@router.get("/publishers")
def publishers(offset: int = Query(0, ge=0, le=10000)):
    return service.publishers(offset)


@router.get("/publishers/{slug}")
def publisher(slug: str):
    return service.publisher(slug)


@router.post("/organizations/from-tenant/{tenant_id}")
def initialize(tenant_id: UUID, user=Depends(current_user)):
    return service.initialize(tenant_id, user)


@router.get("/organizations/{org_id}/settings")
def org_settings(org_id: UUID, user=Depends(current_user)):
    return service.org_settings(org_id, user)


@router.put("/organizations/{org_id}")
def save_org(org_id: UUID, body: Organization, user=Depends(current_user)):
    return service.save_org(org_id, body, user)


@router.get("/organizations/{org_id}/catalog")
def manage_catalog(
    org_id: UUID,
    q: str = Query("", max_length=200),
    offset: int = Query(0, ge=0, le=10000),
    user=Depends(current_user),
):
    return service.manage_catalog(org_id, q, offset, user)


@router.put("/organizations/{org_id}/listings")
def save_listing(org_id: UUID, body: Listing, user=Depends(current_user)):
    return service.save_listing(org_id, body, user)


@router.get("/library")
def library(offset: int = Query(0, ge=0, le=10000), user=Depends(current_user)):
    return service.library(offset, user)


@router.put("/library/{book_id}")
def save_library(book_id: UUID, body: LibraryItem, user=Depends(current_user)):
    return service.save_library(book_id, body, user)


@router.delete("/library/{book_id}")
def remove_library(book_id: UUID, user=Depends(current_user)):
    return service.remove_library(book_id, user)


@router.get("/publishers/{slug}/catalogs")
def publisher_catalogs(slug: str):
    return service.publisher_catalogs(slug)
