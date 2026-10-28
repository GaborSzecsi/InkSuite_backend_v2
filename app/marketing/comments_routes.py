from uuid import UUID
from typing import Literal
from fastapi import APIRouter, Depends, Response, Query, HTTPException
from pydantic import BaseModel, Field
from psycopg.rows import dict_row
from app.core.db import db_conn
from . import service as s, comments


def no_store(response: Response):
    response.headers['Cache-Control'] = 'no-store'


router = APIRouter(prefix='/social/targets', tags=['Marketing comments'], dependencies=[Depends(no_store)])


class CommentAction(BaseModel):
    handle: str = Field(min_length=1, max_length=16384)
    action: Literal['reply', 'hide', 'unhide', 'delete']
    message: str | None = Field(default=None, max_length=2000)


@router.get('/{target_id}/discussion')
def preview(target_id: UUID, ctx=Depends(s.access)):
    with db_conn() as conn, conn.transaction(), conn.cursor(row_factory=dict_row) as cur:
        return comments.preview(cur, ctx, target_id)


@router.get('/{target_id}/comments')
def listing(target_id: UUID, cursor: str | None = Query(default=None, max_length=16384),
            parent: str | None = Query(default=None, max_length=16384), ctx=Depends(s.access)):
    with db_conn() as conn, conn.transaction(), conn.cursor(row_factory=dict_row) as cur:
        result = comments.listing(cur, ctx, target_id, cursor, parent)
        # Existing clients must not turn a restricted empty edge into "no comments".
        if result.get('visibility') == 'not_returned' and not result['comments']:
            count = result['reported_count']
            raise HTTPException(409, f'Meta reports {count} comments, but is not providing their content to InkSuite. Comment access or platform visibility restrictions may apply.', headers={'Cache-Control': 'no-store'})
        return result


@router.post('/{target_id}/comments/actions')
def action(target_id: UUID, body: CommentAction, ctx=Depends(s.admin)):
    with db_conn() as conn, conn.transaction(), conn.cursor(row_factory=dict_row) as cur:
        return comments.act(cur, ctx, target_id, body.handle, body.action, body.message)
