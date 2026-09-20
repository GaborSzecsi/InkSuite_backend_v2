from .observability import TimedRoute
from uuid import UUID
from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel, Field, ConfigDict
from .core import current_user
from . import native_service as service

router = APIRouter(route_class=TimedRoute)


class Upload(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)
    actor_id: UUID
    filename: str = Field(min_length=1, max_length=255)
    content_type: str = Field(max_length=100)
    file_size: int = Field(gt=0)


@router.get("/native-media/capabilities")
def capabilities():
    return service.capabilities()


@router.post("/media/uploads")
def initiate(body: Upload, user=Depends(current_user)):
    return service.initiate(body, user)


@router.post("/native-media/{media_id}/complete")
def complete(media_id: UUID, actor_id: UUID, user=Depends(current_user)):
    return service.complete(media_id, actor_id, user)


@router.get("/native-media")
def listing(
    actor_id: UUID, offset: int = Query(0, ge=0, le=10000), user=Depends(current_user)
):
    return service.list_media(actor_id, offset, user)


@router.get("/native-media/{media_id}")
def inspect(media_id: UUID, actor_id: UUID, user=Depends(current_user)):
    return service.inspect(media_id, actor_id, user)


@router.delete("/native-media/{media_id}")
def remove(media_id: UUID, actor_id: UUID, user=Depends(current_user)):
    return service.remove(media_id, actor_id, user)


@router.post("/native-media/{media_id}/retry")
def retry(media_id: UUID, actor_id: UUID, user=Depends(current_user)):
    return service.retry(media_id, actor_id, user)
