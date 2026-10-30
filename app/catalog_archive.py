"""Reversible, publisher-scoped edition archiving. Publishing status is unchanged."""
from datetime import datetime, timezone
from uuid import UUID
from fastapi import HTTPException
from psycopg.types.json import Jsonb


def require_active(cur, tenant_id, edition_id, work_id=None):
    for value in (tenant_id, edition_id, *([work_id] if work_id else [])):
        try: UUID(str(value))
        except (ValueError, TypeError): raise HTTPException(422, 'Invalid edition identity.') from None
    # Keep the read lock until the metadata write commits. Legacy callers use
    # autocommit connections, so opt into their existing explicit commit.
    connection=getattr(cur,'connection',None)
    if connection is not None and connection.autocommit:
        connection.autocommit=False
    cur.execute('SELECT id,work_id,archived_at FROM editions WHERE tenant_id=%s AND id=%s FOR SHARE',(tenant_id,edition_id))
    row=cur.fetchone()
    if not row or work_id is not None and str(row['work_id'])!=str(work_id):
        raise HTTPException(404,'Edition not found.')
    if row['archived_at'] is not None:
        raise HTTPException(409,'This edition is archived. Restore it before making changes or publishing it.')
    return row


def change(cur, tenant_id, work_id, edition_id, actor, *, archive, reason=''):
    for value in (tenant_id,work_id,edition_id):
        try:UUID(str(value))
        except (ValueError,TypeError):raise HTTPException(422,'Invalid edition identity.') from None
    cur.execute('SELECT id,work_id,isbn13,archived_at,archive_reason FROM editions WHERE tenant_id=%s AND work_id=%s AND id=%s FOR UPDATE',(tenant_id,work_id,edition_id))
    row=cur.fetchone()
    if not row:raise HTTPException(404,'Edition not found.')
    if bool(row['archived_at'])==archive:
        return {'id':str(row['id']),'work_id':str(row['work_id']),'archived':archive,'changed':False}
    event={'action':'archived' if archive else 'restored','at':datetime.now(timezone.utc).isoformat(),'actor':str(actor) if actor else None,'reason':str(reason or '').strip()[:1000]}
    if archive:
        cur.execute('''UPDATE editions SET archived_at=now(),archived_by=%s,archive_reason=%s,
            archive_history=archive_history || %s::jsonb,updated_at=now()
            WHERE tenant_id=%s AND work_id=%s AND id=%s''',(actor,event['reason'],Jsonb([event]),tenant_id,work_id,edition_id))
    else:
        cur.execute('''UPDATE editions SET archived_at=NULL,archived_by=NULL,archive_reason='',
            archive_history=archive_history || %s::jsonb,updated_at=now()
            WHERE tenant_id=%s AND work_id=%s AND id=%s''',(Jsonb([event]),tenant_id,work_id,edition_id))
    return {'id':str(row['id']),'work_id':str(row['work_id']),'archived':archive,'changed':True}
