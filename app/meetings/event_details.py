"""Owner-only event details and invitation updates; team busy references are never accepted."""
from typing import Literal
from uuid import UUID
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, EmailStr, Field
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb
from app.core.db import db_conn
from app.tenants.dependencies import require_tenant_access
from . import service as s

router=APIRouter()

class EventRef(BaseModel):
    kind: Literal['booking','local','external']
    id: str=Field(min_length=1,max_length=2048)
    connection: UUID | None=None
    calendar: str=Field(default='',max_length=2048)

class UpdateDetails(BaseModel):
    event: EventRef
    location: str=Field(default='',max_length=2000)
    invitees: list[EmailStr]=Field(default_factory=list,max_length=50)
    revision: str=Field(max_length=1000)

def storage(cur):
    row=s.one(cur,"SELECT to_regclass('public.meeting_event_details') AS details, to_regclass('public.meeting_event_mail') AS mail")
    return bool(row and row['details'] and row['mail'])

def require_storage(cur):
    if not storage(cur):raise HTTPException(409,'Apply 011_calendar_event_details.sql to enable InkSuite invitations and location updates.')

def resolve(cur, ref, tenant, user):
    if ref.kind=='external':
        if not ref.connection:raise HTTPException(422,'Calendar connection is required.')
        c=s.owned(cur,'meeting_connections',ref.connection,tenant,user)
        cal=next((v for v in c['calendars'] if v['id']==ref.calendar),None)
        if c['status']!='connected' or cal is None:raise HTTPException(409,'Reconnect this calendar.')
        return None,c,cal
    try:uid=UUID(ref.id)
    except ValueError:raise HTTPException(422,'Invalid event.')
    if ref.kind=='booking':
        b=s.owned(cur,'meeting_bookings',uid,tenant,user)
        if b.get('external_connection_id'):
            c=s.owned(cur,'meeting_connections',b['external_connection_id'],tenant,user)
            cal=next((v for v in c['calendars'] if v['id']==b['external_calendar_id']),None)
            if c['status']!='connected' or not cal or not b.get('external_event_id'):raise HTTPException(409,'Wait for this meeting to sync, or reconnect its calendar.')
            return b,c,cal
        return b,None,None
    b=s.one(cur,'SELECT * FROM meeting_calendar_events WHERE id=%s AND tenant_id=%s AND user_id=%s',(uid,tenant,user))
    if not b:raise HTTPException(404,'Event not found.')
    return b,None,None

def read_details(cur,ref,tenant,user):
    b,c,cal=resolve(cur,ref,tenant,user)
    if c:
        detail=s.adapter(c).event_details(cal['id'],b['external_event_id'] if b else ref.id)
        detail.pop('raw',None)
        detail['can_edit']=bool(detail['can_edit'] and cal.get('can_write') and (not b or b['status']=='confirmed'))
        return detail
    p=s.profile(cur,tenant,user)
    if ref.kind=='booking':
        detail={'location':b['snapshot'].get('location',''),'attendees':[b['guest']]+b['snapshot'].get('additional_attendees',[]),'revision':str(b['updated_at'])}
        can_edit=b['status']=='confirmed'
    else:
        row=s.one(cur,'SELECT * FROM meeting_event_details WHERE tenant_id=%s AND user_id=%s AND event_id=%s',(tenant,user,b['id'])) if storage(cur) else None
        detail={'location':row['location'] if row else '', 'attendees':row['attendees'] if row else [],'revision':str(row['version'] if row else 0)}
        can_edit=True
    detail.update(organizer=p['company_email'],can_edit=can_edit)
    detail['attendees']=[{'email':a['email'],'name':a.get('name',''),'status':'invited'} for a in detail['attendees']]
    if storage(cur):
        states=s.all_rows(cur,'SELECT status,count(*) AS count FROM meeting_event_mail WHERE tenant_id=%s AND user_id=%s AND event_id=%s GROUP BY status',(tenant,user,b['id']))
        detail['delivery']={r['status']:r['count'] for r in states}
    return detail

def queue_mail(cur,b,tenant,user,location,attendees,version,cancel=False,recipients=None):
    require_storage(cur)
    s.sender(cur,tenant,user)  # Validate a configured company sender before accepting invitations.
    payload={'title':b.get('title') or b['snapshot']['title'],'start':b['start_at'].isoformat(),'end':b['end_at'].isoformat(),
             'location':location,'attendees':attendees,'version':version,'cancelled':cancel}
    for a in (attendees if recipients is None else recipients):
        cur.execute("UPDATE meeting_event_mail SET status='cancelled' WHERE tenant_id=%s AND user_id=%s AND event_id=%s AND lower(recipient)=lower(%s) AND status IN ('pending','processing')",(tenant,user,b['id'],a['email']))
        cur.execute('INSERT INTO meeting_event_mail(id,tenant_id,user_id,event_id,recipient,payload) VALUES(%s,%s,%s,%s,%s,%s)',
                    (s.uid(),tenant,user,b['id'],a['email'],Jsonb(payload)))

@router.post('/details')
def details(ref:EventRef,ctx=Depends(require_tenant_access)):
    with db_conn() as conn,conn.cursor(row_factory=dict_row) as cur:
        return read_details(cur,ref,ctx['tenant']['id'],ctx['user']['id'])

@router.put('/details')
def update(body:UpdateDetails,ctx=Depends(require_tenant_access)):
    tenant,user=ctx['tenant']['id'],ctx['user']['id']
    with db_conn() as conn,conn.transaction(),conn.cursor(row_factory=dict_row) as cur:
        s.profile(cur,tenant,user,lock=True)
        b,c,cal=resolve(cur,body.event,tenant,user)
        if b and body.event.kind=='booking' and b['status']!='confirmed':raise HTTPException(409,'This meeting is not ready to edit.')
        if c:
            if not cal.get('can_write'):raise HTTPException(403,'This calendar is read-only.')
            d=s.adapter(c).update_details(cal['id'],b['external_event_id'] if b else body.event.id,body.location,[str(e) for e in body.invitees],body.revision)
            d.pop('raw',None)
            if b:
                snapshot=dict(b['snapshot'],location=body.location)
                cur.execute('UPDATE meeting_bookings SET snapshot=%s,updated_at=now() WHERE id=%s',(Jsonb(snapshot),b['id']))
            return {**d,'message':'Saved. Calendar invitations and updates were submitted.'}
        current=read_details(cur,body.event,tenant,user)
        if current['revision']!=body.revision:raise HTTPException(409,'This meeting changed. Close and reopen it before saving again.')
        attendees=list(current['attendees']);known={a['email'].lower() for a in attendees};known.add(current['organizer'].lower())
        added=[]
        for email in body.invitees:
            email=str(email)
            if email.lower() not in known:
                known.add(email.lower());a={'email':email,'name':'','status':'invited'};attendees.append(a);added.append(a)
        changed=body.location!=current['location']
        if not changed and not added:return {**current,'message':'No changes to save.'}
        if body.event.kind=='booking':
            s.sender(cur,tenant,user)
            snapshot=dict(b['snapshot'],location=body.location,additional_attendees=[a for a in attendees if a['email'].lower()!=b['guest']['email'].lower()])
            b=s.one(cur,'UPDATE meeting_bookings SET snapshot=%s,version=version+1,updated_at=now() WHERE id=%s RETURNING *',(Jsonb(snapshot),b['id']))
            s.schedule(cur,b,'updated')
            return {**read_details(cur,body.event,tenant,user),'message':'Saved. Invitations and updates are queued for email delivery.'}
        else:
            require_storage(cur)
            version=int(current['revision'])+1
            cur.execute('INSERT INTO meeting_event_details(tenant_id,user_id,event_id,location,attendees,version) VALUES(%s,%s,%s,%s,%s,%s) ON CONFLICT(tenant_id,user_id,event_id) DO UPDATE SET location=EXCLUDED.location,attendees=EXCLUDED.attendees,version=EXCLUDED.version',
                        (tenant,user,b['id'],body.location,Jsonb(attendees),version))
        recipients=attendees if changed else added
        if recipients:queue_mail(cur,b,tenant,user,body.location,attendees,version,recipients=recipients)
        return {**read_details(cur,body.event,tenant,user),'message':'Saved. Invitations and updates are queued for email delivery.' if recipients else 'Location saved.'}
