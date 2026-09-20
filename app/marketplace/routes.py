from fastapi import APIRouter
from . import identity, catalog, social, messages, media, invitations, notifications
from . import native_routes

router = APIRouter(prefix="/marketplace", tags=["Marketplace"])
for child in (
    identity.router,
    catalog.router,
    social.router,
    messages.router,
    media.router,
    invitations.router,
    notifications.router,
    native_routes.router,
):
    router.include_router(child)
