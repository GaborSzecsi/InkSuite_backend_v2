from datetime import datetime
from typing import Literal
from uuid import UUID
from fastapi import APIRouter, Depends, File, Form, UploadFile, HTTPException
from fastapi.responses import Response
from pydantic import BaseModel, Field
from .core import current_user
from . import arc_service as service

router = APIRouter()


class RequestARC(BaseModel):
    actor_id: UUID
    message: str = Field(default="", max_length=2000)


class Decision(BaseModel):
    status: Literal["approved", "rejected", "revoked"]
    reason: str = Field(default="", max_length=1000)


class Progress(BaseModel):
    revision: int = Field(ge=1)
    location: str = Field(min_length=1, max_length=2000, pattern=r"^epubcfi\(")
    percent: float = Field(ge=0, le=100)


class Bookmark(BaseModel):
    revision: int = Field(ge=1)
    location: str = Field(min_length=1, max_length=2000, pattern=r"^epubcfi\(")
    chapter: str = Field(default="", max_length=300)
    label: str = Field(default="", max_length=300)


@router.get("/arc/editions/{edition_id}")
def edition_state(edition_id: UUID, user=Depends(current_user)):
    return service.edition_state(edition_id, user)


@router.post("/arc/editions/{edition_id}")
def upload(
    edition_id: UUID,
    file: UploadFile = File(...),
    replace: bool = Form(False),
    request_enabled: bool = Form(True),
    available_from: datetime | None = Form(None),
    available_until: datetime | None = Form(None),
    user=Depends(current_user),
):
    data = file.file.read()
    return service.upload(
        edition_id,
        user,
        data,
        file.filename or "",
        file.content_type or "",
        replace,
        available_from,
        available_until,
        request_enabled,
    )


@router.get("/books/{book_id}/arc")
def options(book_id: UUID, user=Depends(current_user)):
    return service.options(book_id, user)


@router.post("/arc/{asset_id}/request")
def request(asset_id: UUID, body: RequestARC, user=Depends(current_user)):
    return service.request(asset_id, body, user)


@router.get("/arc-requests")
def queue(
    organization_id: UUID,
    status: Literal["pending", "approved", "rejected", "revoked", "all"] = "pending",
    user=Depends(current_user),
):
    return service.queue(organization_id, status, user)


@router.put("/arc-requests/{request_id}")
def decide(request_id: UUID, body: Decision, user=Depends(current_user)):
    return service.decide(request_id, body.status, body.reason, user)


@router.get("/arc-library")
def library(user=Depends(current_user)):
    return service.library(user)


@router.post("/library/{book_id}/reading-session")
def start(book_id: UUID, user=Depends(current_user)):
    return service.start(book_id, user)


@router.get("/reader/{session_id}/resource/{path:path}")
def resource(session_id: UUID, path: str, user=Depends(current_user)):
    data, mime = service.resource(session_id, path, user)
    return Response(
        data,
        media_type=mime,
        headers={
            "Cache-Control": "private, no-store, max-age=0",
            "X-Content-Type-Options": "nosniff",
            "Referrer-Policy": "no-referrer",
            "Cross-Origin-Resource-Policy": "same-origin",
            "Content-Security-Policy": "default-src 'none'; img-src 'self' blob:; style-src 'self' 'unsafe-inline'; font-src 'self'; script-src 'none'; connect-src 'none'; frame-src 'none'; object-src 'none'; base-uri 'none'; form-action 'none'",
        },
    )


@router.put("/library/{book_id}/progress")
def progress(book_id: UUID, body: Progress, user=Depends(current_user)):
    return service.progress(book_id, body, user)


@router.get("/library/{book_id}/bookmarks")
def bookmarks(book_id: UUID, user=Depends(current_user)):
    return service.bookmarks(book_id, user)


@router.post("/library/{book_id}/bookmarks")
def add_bookmark(book_id: UUID, body: Bookmark, user=Depends(current_user)):
    return service.bookmarks(book_id, user, body=body)


@router.delete("/library/{book_id}/bookmarks/{bookmark_id}")
def delete_bookmark(book_id: UUID, bookmark_id: UUID, user=Depends(current_user)):
    return service.bookmarks(book_id, user, delete_id=bookmark_id)


@router.post("/arc/{asset_id}/preview")
def preview(asset_id: UUID, user=Depends(current_user)):
    return service.preview(asset_id, user)


@router.get("/reader/{session_id}/status")
def session_status(session_id: UUID, user=Depends(current_user)):
    return service.session_status(session_id, user)
