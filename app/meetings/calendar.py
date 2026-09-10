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


def first_name(name):
    value = (name or '').strip().split('@', 1)[0]
    return value.replace('.', ' ').replace('_', ' ').split()[0].capitalize() if value else 'Teammate'


def team_people(cur, tenant, user=None):
    return s.all_rows(cur, """SELECT m.user_id AS id,
        COALESCE(NULLIF(to_jsonb(u)->>'first_name',''), NULLIF(to_jsonb(u)->>'given_name',''),
                 NULLIF(p.display_name,''), NULLIF(to_jsonb(u)->>'display_name',''), u.email) AS name
        FROM memberships m JOIN users u ON u.id=m.user_id
        LEFT JOIN meeting_profiles p ON p.tenant_id=m.tenant_id AND p.user_id=m.user_id
        WHERE m.tenant_id=%s AND (%s::uuid IS NULL OR m.user_id<>%s::uuid)
        ORDER BY name""", (tenant,user,user))


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
        people = team_people(cur, tenant, user)
        return {'storage_ready': available, 'people': [dict(p, name=first_name(p['name'])) for p in people]}


@router.get('/events')
def events(start: datetime, end: datetime, team_user: UUID | None = None, ctx=Depends(require_tenant_access)):
    window(start,end)
    tenant, user = ctx['tenant']['id'], ctx['user']['id']
    result, warnings = [], []
    with db_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
        available = ready(cur)
        if team_user:
            person = next((p for p in team_people(cur, tenant) if str(p['id']) == str(team_user)), None)
            if not person:
                raise HTTPException(404, 'Team member not found.')
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
                except Exception as exc:
                    print(
                        f"[meetings-calendar] failed provider={c['provider']} "
                        f"connection_id={c['id']} calendar_id={cal.get('id')} "
                        f"calendar_name={cal.get('name')} error={type(exc).__name__}: {exc}"
                    )
                    # Never expose credential/provider payloads or another user's calendar identity.
                    warnings.append(
                        'A calendar could not load. Availability may be incomplete.'
                        if team_user
                        else f"Could not load {cal['name']} ({c['email']}). Try refreshing or reconnecting."
                    )
        if team_user:
            from .availability import merge_busy
            result = busy_view(merge_busy(intervals),first_name(person['name']))
            for event in result:
                event['id'] = f"{team_user}:{event['id']}"
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
