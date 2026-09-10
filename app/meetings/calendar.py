"""Authenticated calendar read model. Team responses contain busy intervals only."""
from datetime import datetime, timedelta, timezone
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field, model_validator
from psycopg.rows import dict_row

from app.core.db import db_conn
from app.tenants.dependencies import require_tenant_access
from . import service as s
from .providers import ProviderError

router = APIRouter(prefix='/tenants/{tenant_slug}/meetings/calendar', tags=['Meetings'])


def ready(cur):
    row = s.one(cur, "SELECT to_regclass('public.meeting_calendar_events') AS events, to_regclass('public.meeting_calendar_preferences') AS preferences")
    return bool(row['events'] and row['preferences'])


def require_ready(cur):
    if not ready(cur):
        raise HTTPException(409, 'Calendar event storage needs migration 006_meeting_calendar.sql. Your connected calendars and meetings are still available.')


def window(start, end):
    if start.tzinfo is None or end.tzinfo is None or not timedelta(0) < end-start <= timedelta(days=45):
        raise HTTPException(422, 'Choose a calendar range of up to 45 days with timezone offsets.')


class EventIn(BaseModel):
    title: str = Field(min_length=1, max_length=200)
    start: datetime
    end: datetime

    @model_validator(mode='after')
    def valid(self):
        self.title = self.title.strip()
        if not self.title:
            raise ValueError('Enter an event title.')
        window(self.start, self.end)
        return self


class SharingIn(BaseModel):
    share_busy: bool


def busy_view(intervals, name):
    # Deliberately discard provider IDs, subjects, locations, attendees and descriptions.
    return [{'id': f'team-{i}', 'title': 'Busy', 'source': 'team', 'person': name,
             'start': a, 'end': b, 'all_day': False} for i, (a, b) in enumerate(intervals)]


@router.get('/settings')
def settings(ctx=Depends(require_tenant_access)):
    tenant, user = ctx['tenant']['id'], ctx['user']['id']
    with db_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
        s.profile(cur, tenant, user)
        available = ready(cur)
        preference = s.one(cur, 'SELECT share_busy FROM meeting_calendar_preferences WHERE tenant_id=%s AND user_id=%s', (tenant,user)) if available else None
        people = s.all_rows(cur, """SELECT p.user_id AS id, COALESCE(NULLIF(p.display_name,''),u.email) AS name
            FROM meeting_profiles p JOIN users u ON u.id=p.user_id
            JOIN meeting_calendar_preferences cp ON cp.tenant_id=p.tenant_id AND cp.user_id=p.user_id
            WHERE p.tenant_id=%s AND p.user_id<>%s AND cp.share_busy
            AND EXISTS(SELECT 1 FROM memberships m WHERE m.tenant_id=p.tenant_id AND m.user_id=p.user_id)
            ORDER BY name""", (tenant,user)) if available else []
        return {'storage_ready': available, 'share_busy': bool(preference and preference['share_busy']), 'people': people}


@router.put('/settings')
def sharing(body: SharingIn, ctx=Depends(require_tenant_access)):
    tenant, user = ctx['tenant']['id'], ctx['user']['id']
    with db_conn() as conn, conn.transaction(), conn.cursor(row_factory=dict_row) as cur:
        require_ready(cur)
        s.profile(cur, tenant, user, lock=True)
        cur.execute('INSERT INTO meeting_calendar_preferences(tenant_id,user_id,share_busy) VALUES(%s,%s,%s) ON CONFLICT(tenant_id,user_id) DO UPDATE SET share_busy=EXCLUDED.share_busy', (tenant,user,body.share_busy))
    return {'share_busy': body.share_busy}


@router.get('/events')
def events(start: datetime, end: datetime, team_user: UUID | None = None, ctx=Depends(require_tenant_access)):
    window(start,end)
    tenant, user = ctx['tenant']['id'], ctx['user']['id']
    result, warnings = [], []
    with db_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
        available = ready(cur)
        if team_user:
            require_ready(cur)
            person = s.one(cur, """SELECT p.user_id, COALESCE(NULLIF(p.display_name,''),u.email) AS name
                FROM meeting_profiles p JOIN users u ON u.id=p.user_id
                JOIN meeting_calendar_preferences cp ON cp.tenant_id=p.tenant_id AND cp.user_id=p.user_id
                WHERE p.tenant_id=%s AND p.user_id=%s AND cp.share_busy
                AND EXISTS(SELECT 1 FROM memberships m WHERE m.tenant_id=p.tenant_id AND m.user_id=p.user_id)""", (tenant,team_user))
            if not person:
                raise HTTPException(404, 'Team availability is not shared.')
            user = team_user
        bookings = s.all_rows(cur, """SELECT id,start_at,end_at,snapshot,external_connection_id,external_calendar_id,external_event_id
            FROM meeting_bookings WHERE tenant_id=%s AND user_id=%s AND status IN ('pending','confirmed','sync_error')
            AND start_at<%s AND end_at>%s""", (tenant,user,end,start))
        local = s.all_rows(cur, 'SELECT id,title,start_at,end_at FROM meeting_calendar_events WHERE tenant_id=%s AND user_id=%s AND start_at<%s AND end_at>%s', (tenant,user,end,start)) if available else []
        connections = s.all_rows(cur, 'SELECT * FROM meeting_connections WHERE tenant_id=%s AND user_id=%s', (tenant,user))
        if team_user:
            intervals = [(r['start_at'],r['end_at']) for r in bookings+local]
        else:
            result = [{'id':str(b['id']), 'title':b['snapshot']['title'], 'start':b['start_at'], 'end':b['end_at'], 'source':'inksuite', 'booking':True, 'all_day':False} for b in bookings]
            result += [{'id':str(b['id']), 'title':b['title'], 'start':b['start_at'], 'end':b['end_at'], 'source':'inksuite', 'editable':True, 'all_day':False} for b in local]
        synced = {(str(b['external_connection_id']), b['external_calendar_id'], b['external_event_id']) for b in bookings}
        for c in connections:
            if c['status'] != 'connected':
                warnings.append('A calendar needs reconnection. Availability may be incomplete.' if team_user else f"Reconnect {c['email']} to load its events.")
                continue
            adapter = None
            for cal in c['calendars']:
                try:
                    if adapter is None:
                        adapter = s.adapter(c)
                    if team_user:
                        intervals.extend(adapter.busy([cal['id']],start,end))
                    else:
                        for e in adapter.calendar_events(cal['id'],start,end):
                            if (str(c['id']),cal['id'],e['id']) in synced:
                                continue
                            result.append({**e, 'id':f"{c['id']}:{cal['id']}:{e['id']}", 'source':c['provider'], 'calendar':cal['name']})
                except Exception:
                    # Never expose credential/provider payloads or another user's calendar identity.
                    warnings.append('A calendar could not load. Availability may be incomplete.' if team_user else f"Could not load {cal['name']} ({c['email']}). Try refreshing or reconnecting.")
        if team_user:
            from .availability import merge_busy
            result = busy_view(merge_busy(intervals),person['name'])
        return {'events':result, 'warnings':list(dict.fromkeys(warnings))}


@router.post('/events')
def add_event(body: EventIn, ctx=Depends(require_tenant_access)):
    tenant, user = ctx['tenant']['id'], ctx['user']['id']
    with db_conn() as conn, conn.transaction(), conn.cursor(row_factory=dict_row) as cur:
        require_ready(cur)
        s.profile(cur,tenant,user,lock=True)
        row = s.one(cur, 'INSERT INTO meeting_calendar_events(id,tenant_id,user_id,title,start_at,end_at) VALUES(%s,%s,%s,%s,%s,%s) RETURNING id', (s.uid(),tenant,user,body.title,body.start,body.end))
    return row


@router.delete('/events/{event_id}')
def delete_event(event_id: UUID, ctx=Depends(require_tenant_access)):
    tenant, user = ctx['tenant']['id'], ctx['user']['id']
    with db_conn() as conn, conn.transaction(), conn.cursor(row_factory=dict_row) as cur:
        require_ready(cur)
        s.profile(cur,tenant,user,lock=True)
        row = s.one(cur, 'DELETE FROM meeting_calendar_events WHERE id=%s AND tenant_id=%s AND user_id=%s RETURNING id', (event_id,tenant,user))
        if not row:
            raise HTTPException(404,'Event not found.')
    return {'deleted':True}
