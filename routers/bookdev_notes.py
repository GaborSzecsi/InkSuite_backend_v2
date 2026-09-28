"""Private notes, always scoped to authenticated tenant membership, user and work."""

from uuid import UUID
from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse
from psycopg.rows import dict_row
from pydantic import BaseModel, ConfigDict, Field
from app.core.db import db_conn
from routers.bookdev_email import _ctx_from_bearer, _load_user_and_membership_or_403

router = APIRouter(prefix="/project-management/book-development", tags=["My Notes"])


class NoteInput(BaseModel):
    model_config = ConfigDict(extra="forbid")
    content: str = Field(default="", max_length=1_000_000)


def owner(
    tenant_slug: str = Query(..., min_length=1, max_length=100),
    ctx=Depends(_ctx_from_bearer),
):
    return _load_user_and_membership_or_403(tenant_slug=tenant_slug, ctx=ctx)


def ready(cur):
    cur.execute("SELECT to_regclass('public.book_development_notes') AS ready")
    if not cur.fetchone()["ready"]:
        raise HTTPException(
            503,
            "My Notes setup is pending. Apply migration 015_book_development_notes.sql.",
        )


def scope(cur, user, work_id):
    cur.execute(
        "SELECT id FROM public.works WHERE id=%s AND tenant_id=%s",
        (work_id, user["tenant_id"]),
    )
    if not cur.fetchone():
        raise HTTPException(404, "Title not found.")
    ready(cur)
    return (user["tenant_id"], user["user_id"], work_id)


def reply(data):
    return JSONResponse(
        jsonable_encoder(data), headers={"Cache-Control": "private, no-store"}
    )


@router.get("/notes/counts")
def counts(user=Depends(owner)):
    with db_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
        ready(cur)
        cur.execute(
            """SELECT n.work_id, count(*) AS count FROM public.book_development_notes n
            JOIN public.works w ON w.id=n.work_id AND w.tenant_id=n.tenant_id
            WHERE n.tenant_id=%s AND n.user_id=%s GROUP BY n.work_id""",
            (user["tenant_id"], user["user_id"]),
        )
        return reply(
            {"counts": {str(row["work_id"]): row["count"] for row in cur.fetchall()}}
        )


@router.get("/{work_id}/notes")
def list_notes(work_id: UUID, user=Depends(owner)):
    with db_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
        args = scope(cur, user, work_id)
        cur.execute(
            """SELECT id, content, created_at, updated_at FROM public.book_development_notes
            WHERE tenant_id=%s AND user_id=%s AND work_id=%s ORDER BY updated_at DESC, id DESC""",
            args,
        )
        return reply({"notes": cur.fetchall()})


@router.post("/{work_id}/notes")
def create_note(work_id: UUID, payload: NoteInput, user=Depends(owner)):
    with db_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
        args = scope(cur, user, work_id)
        cur.execute(
            """INSERT INTO public.book_development_notes (tenant_id,user_id,work_id,content)
            VALUES (%s,%s,%s,%s) RETURNING id,content,created_at,updated_at""",
            (*args, payload.content),
        )
        return reply({"note": cur.fetchone()})


@router.put("/{work_id}/notes/{note_id}")
def update_note(work_id: UUID, note_id: UUID, payload: NoteInput, user=Depends(owner)):
    with db_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
        args = scope(cur, user, work_id)
        cur.execute(
            """UPDATE public.book_development_notes SET content=%s,updated_at=now()
            WHERE tenant_id=%s AND user_id=%s AND work_id=%s AND id=%s
            RETURNING id,content,created_at,updated_at""",
            (payload.content, *args, note_id),
        )
        note = cur.fetchone()
        if not note:
            raise HTTPException(404, "Note not found.")
        return reply({"note": note})


@router.delete("/{work_id}/notes/{note_id}")
def delete_note(work_id: UUID, note_id: UUID, user=Depends(owner)):
    with db_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
        args = scope(cur, user, work_id)
        cur.execute(
            """DELETE FROM public.book_development_notes
            WHERE tenant_id=%s AND user_id=%s AND work_id=%s AND id=%s RETURNING id""",
            (*args, note_id),
        )
        if not cur.fetchone():
            raise HTTPException(404, "Note not found.")
        return reply({"deleted": True})
