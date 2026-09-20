from .observability import TimedRoute

"""HTTP adapters; service functions also support non-HTTP callers."""
from .media_service import *
from . import media_service as service

router = APIRouter(route_class=TimedRoute)


@router.post("/images")
async def upload_image(
    actor_id: UUID, file: UploadFile = File(...), user=Depends(current_user)
):
    return await service.upload_image(actor_id, file, user)


@router.get("/images")
def personal_images(
    actor_id: UUID, offset: int = Query(0, ge=0, le=10000), user=Depends(current_user)
):
    return service.personal_images(actor_id, offset, user)


@router.delete("/images/{image_id}")
def delete_image(image_id: UUID, actor_id: UUID, user=Depends(current_user)):
    return service.delete_image(image_id, actor_id, user)


@router.put("/identity-image")
def identity_image(
    actor_id: UUID,
    purpose: str,
    asset_id: UUID | None = None,
    key: str | None = None,
    user=Depends(current_user),
):
    return service.identity_image(actor_id, purpose, asset_id, key, user)
