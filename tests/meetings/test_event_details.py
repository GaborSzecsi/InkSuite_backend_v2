from datetime import datetime,timezone
from unittest.mock import Mock
import pytest
from pydantic import ValidationError
from fastapi import HTTPException
from app.meetings.providers import CalendarProvider,ProviderError
from app.meetings import event_details as d,event_mail,worker,service

def google_event(**kw):
    return dict(id='event',etag='revision',organizer={'self':True,'email':'host@example.com'},location='Room A',
                attendees=[{'email':'first@example.com','responseStatus':'accepted'}],**kw)

def test_google_invites_preserve_existing_attendees_and_notify():
    p=CalendarProvider('google',{});raw=google_event()
    p.api=Mock(side_effect=[raw,{},raw])
    p.update_details('cal','event','Room B',['new@example.com','FIRST@example.com'],'revision')
    call=p.api.call_args_list[1]
    assert call.args[0]=='PATCH'
    assert call.kwargs['json']['attendees']==[raw['attendees'][0],{'email':'new@example.com'}]
    assert call.kwargs['json']['location']=='Room B'
    assert call.kwargs['params']=={'sendUpdates':'all'}
    assert call.kwargs['headers']=={'If-Match':'revision'}

def test_microsoft_preserves_optional_attendees():
    p=CalendarProvider('microsoft',{})
    raw={'@odata.etag':'rev','isOrganizer':True,'location':{'displayName':'Office'},'organizer':{'emailAddress':{'address':'host@example.com'}},
         'attendees':[{'emailAddress':{'address':'first@example.com'},'type':'optional','status':{'response':'accepted'}}]}
    p.api=Mock(side_effect=[raw,{},raw])
    p.update_details('cal','event','Office',['new@example.com'],'rev')
    body=p.api.call_args_list[1].kwargs['json']
    assert 'location' not in body
    assert body['attendees'][0]==raw['attendees'][0]
    assert body['attendees'][1]['emailAddress']['address']=='new@example.com'

@pytest.mark.parametrize('raw,revision',[(dict(google_event(),organizer={'self':False}),'revision'),(google_event(),'old')])
def test_nonorganizer_or_stale_revision_cannot_edit(raw,revision):
    p=CalendarProvider('google',{});p.api=Mock(return_value=raw)
    with pytest.raises(ProviderError):p.update_details('cal','event','New',[],revision)
    assert p.api.call_count==1

def test_duplicate_guest_does_not_send_another_invitation():
    p=CalendarProvider('google',{});p.api=Mock(return_value=google_event())
    p.update_details('cal','event','Room A',['FIRST@example.com'],'revision')
    assert all(call.args[0]=='GET' for call in p.api.call_args_list)

def test_team_busy_reference_is_rejected():
    with pytest.raises(ValidationError):d.EventRef(kind='team',id='someone')

def test_invalid_invitation_email_is_rejected():
    with pytest.raises(ValidationError):d.UpdateDetails(event={'kind':'local','id':'1'},revision='0',invitees=['not-an-email'])

def test_external_connection_lookup_is_owner_scoped(monkeypatch):
    lookup=Mock(side_effect=HTTPException(404,'Not found'));monkeypatch.setattr(d.s,'owned',lookup)
    ref=d.EventRef(kind='external',id='event',connection='11111111-1111-1111-1111-111111111111',calendar='calendar')
    with pytest.raises(HTTPException):d.resolve(Mock(),ref,'tenant','owner')
    assert lookup.call_args.args[-2:]==('tenant','owner')

def test_local_invitation_contains_real_calendar_request():
    j={'id':'job','event_id':'event','recipient':'new@example.com','payload':{'title':'Editorial meeting','start':'2026-10-01T12:00:00Z','end':'2026-10-01T13:00:00Z','location':'Room A','version':2,'attendees':[{'email':'new@example.com'}]}}
    msg=event_mail.message(j,{'display_name':'Host','company_email':'host@example.com'})
    part=list(msg.iter_attachments())[0]
    assert part.get_param('method')=='REQUEST'
    ics=part.get_payload(decode=True).decode()
    assert 'ATTENDEE;RSVP=TRUE:mailto:new@example.com' in ics
    assert 'LOCATION:Room A' in ics and 'UID:event@inksuite.io' in ics

def test_added_attendee_email_has_no_management_token(monkeypatch):
    secret=Mock(side_effect=AssertionError('Must not read booking management token'))
    monkeypatch.setattr(worker,'secret_read',secret)
    monkeypatch.setattr(service,'signature',Mock(return_value=('','')))
    b={'id':'book','tenant_id':'tenant','user_id':'owner','version':2,'status':'confirmed','guest':{'email':'original@example.com'},
       'snapshot':{'title':'Meeting','location':'Room B','additional_attendees':[{'email':'new@example.com'}]},
       'start_at':datetime(2026,10,1,12,tzinfo=timezone.utc),'end_at':datetime(2026,10,1,13,tzinfo=timezone.utc)}
    msg=worker.make_message(Mock(),b,{'id':'job','kind':'updated','recipient':'new@example.com'},{'company_email':'host@example.com','display_name':'Host','timezone':'UTC'})
    assert msg['To']=='new@example.com'
    assert '/meeting/manage/' not in msg.as_string()
    assert 'Room B' in msg.get_body(preferencelist=('plain',)).get_content()
