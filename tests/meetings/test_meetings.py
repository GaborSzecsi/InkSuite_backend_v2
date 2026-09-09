from datetime import datetime,timedelta,timezone
from unittest.mock import Mock
import pytest
from app.meetings.availability import slots,instant,merge_busy
from app.meetings.schemas import MeetingType,Guest
from app.meetings.providers import CalendarProvider,ProviderError
from app.meetings import service as s

NOW=instant('2026-09-07T00:00:00Z')
def config(**patch):return dict(timezone='UTC',duration=30,weekly={'0':[['09:00','12:00'],['13:00','17:00']]},exceptions={},notice=0,horizon=60,buffer_before=0,buffer_after=0,interval=15,**patch)

def test_multiple_windows():
    result=slots(config(),'2026-09-07',now=NOW)
    assert instant('2026-09-07T09:00Z') in result
    assert instant('2026-09-07T12:00Z') not in result
    assert instant('2026-09-07T16:45Z') not in result

@pytest.mark.parametrize('windows,count',[([],0),([['10:00','11:00']],3)])
def test_exceptions(windows,count):
    c=config();c['exceptions']={'2026-09-07':windows}
    assert len(slots(c,'2026-09-07',now=NOW))==count

def test_notice_horizon_and_buffers():
    c=config();c.update(notice=600,buffer_before=15,buffer_after=15,horizon=1)
    result=slots(c,'2026-09-07',now=NOW)
    assert result[0]==instant('2026-09-07T10:00Z')
    assert result[-1]==instant('2026-09-07T16:15Z')
    assert not slots(c,'2026-09-14',now=NOW)

def test_merged_busy_and_boundary():
    busy=[('2026-09-07T09:00Z','2026-09-07T10:00Z'),('2026-09-07T09:30Z','2026-09-07T10:30Z')]
    assert len(merge_busy(busy))==1
    result=slots(config(),'2026-09-07',busy,now=NOW)
    assert result[0]==instant('2026-09-07T10:30Z')

@pytest.mark.parametrize('day,expected',[('2026-03-08',False),('2026-11-01',True)])
def test_dst(day,expected):
    c=config();c.update(timezone='America/New_York',weekly={'6':[['01:00','04:00']]})
    result=slots(c,day,now=instant(day+'T00:00Z'))
    assert len(result)==len(set(result))
    local=[d.astimezone(__import__('zoneinfo').ZoneInfo(c['timezone'])).strftime('%H:%M') for d in result]
    if not expected:assert not any(t.startswith('02:') for t in local)
    else:assert local.count('01:00')==2

@pytest.mark.parametrize('patch',[{'duration':0},{'weekly':{'0':[['11:00','09:00']]}},{'weekly':{'9':[['09:00','10:00']]}},{'timezone':'not/a-zone'},{'location_type':'manual','location':'javascript:alert(1)'}])
def test_invalid_configs(patch):
    with pytest.raises(ValueError):MeetingType(title='Test',slug='meeting-testing',**patch)

def test_guest_requires_timezone():
    with pytest.raises(ValueError):Guest(name='Guest',email='guest@example.com',start='2026-09-07T12:00',idempotency_key='a'*20)

def test_google_busy_errors_are_not_free():
    p=CalendarProvider('google',{});p.api=Mock(return_value={'calendars':{'x':{'errors':[{'reason':'notFound'}]}}})
    with pytest.raises(ProviderError):p.busy(['x'],NOW,NOW+timedelta(days=1))

def test_google_multiple_calendars():
    p=CalendarProvider('google',{});p.api=Mock(return_value={'calendars':{'x':{'busy':[{'start':'2026-09-07T09:00Z','end':'2026-09-07T10:00Z'}]},'y':{'busy':[{'start':'2026-09-07T11:00Z','end':'2026-09-07T12:00Z'}]}}})
    assert len(p.busy(['x','y'],NOW,NOW+timedelta(days=1)))==2
    assert p.api.call_count==1

def test_microsoft_free_and_cancelled():
    p=CalendarProvider('microsoft',{});p.api=Mock(return_value={'value':[{'id':'a','showAs':'free'},{'id':'b','isCancelled':True},{'id':'c','showAs':'busy','start':{'dateTime':'2026-09-07T09:00:00'},'end':{'dateTime':'2026-09-07T10:00:00'}}]})
    assert len(p.busy(['x'],NOW,NOW+timedelta(days=1)))==1

def test_microsoft_reschedule_excludes_only_own_event():
    p=CalendarProvider('microsoft',{});p.api=Mock(return_value={'value':[{'id':id,'showAs':'busy','start':{'dateTime':'2026-09-07T09:00:00'},'end':{'dateTime':'2026-09-07T10:00:00'}} for id in ['mine','other']]})
    assert len(p.busy(['x'],NOW,NOW+timedelta(days=1),exclude_event=('x','mine')))==1

def test_signature_safe_and_plain_text(monkeypatch):
    monkeypatch.setattr(s,'profile',lambda *a:{'signature_text':'<script>bad</script>','signature_links':[{'type_id':'a','label':'Schedule a meeting <img>','style':'button'}]})
    monkeypatch.setattr(s,'owned',lambda *a:{'slug':'my-meeting','active':False})
    monkeypatch.setenv('MEETINGS_PUBLIC_BASE','https://www.inksuite.io')
    plain,markup=s.signature(Mock(),'tenant','user')
    assert '<script>' not in markup and '<img>' not in markup
    assert '<a href="https://www.inksuite.io/book/my-meeting"' in markup
    assert 'Schedule a meeting <img>: https://' in plain

def test_ownership_filters(monkeypatch):
    cursor=Mock();cursor.fetchone.return_value=None
    with pytest.raises(Exception):s.owned(cursor,'meeting_types','id','tenant-a','user-a')
    assert cursor.execute.call_args.args[1]==('id','tenant-a','user-a')

def test_profile_sender_is_current_not_snapshot(monkeypatch):
    monkeypatch.setattr(s,'profile',lambda *a:{'company_email':'new@marblepress.com','sender_connection_id':'c'})
    monkeypatch.setattr(s,'owned',lambda *a:{'status':'connected','email':'new@marblepress.com'})
    assert s.sender(Mock(),'t','u')[0]['company_email']=='new@marblepress.com'

@pytest.mark.parametrize('email,status',[('other@example.com','connected'),('host@marblepress.com','disconnected')])
def test_sender_rejects_mismatch(monkeypatch,email,status):
    monkeypatch.setattr(s,'profile',lambda *a:{'company_email':'host@marblepress.com','sender_connection_id':'c'})
    monkeypatch.setattr(s,'owned',lambda *a:{'status':status,'email':email})
    with pytest.raises(Exception):s.sender(Mock(),'t','u')

def test_missing_sender(monkeypatch):
    monkeypatch.setattr(s,'profile',lambda *a:{'company_email':''})
    with pytest.raises(Exception):s.sender(Mock(),'t','u')

def test_schedule_intervals_and_recipients(monkeypatch):
    jobs=[];monkeypatch.setattr(s,'job',lambda cur,b,kind,recipient,due:jobs.append((kind,recipient,due)))
    monkeypatch.setattr(s,'notify',lambda *a:None)
    start=datetime.now(timezone.utc)+timedelta(days=2)
    b={'id':'x','tenant_id':'t','user_id':'u','version':2,'start_at':start,'snapshot':{'title':'Call'},'external_calendar_id':'cal'}
    s.schedule(Mock(),b,'rescheduled')
    reminders=[j for j in jobs if j[0].startswith('reminder')]
    assert len(reminders)==9
    for minutes in [1440,60,30]:
        assert {r for k,r,d in reminders if k=='reminder_'+str(minutes)}=={'host','guest','internal'}
        assert all(d==start-timedelta(minutes=minutes) for k,r,d in reminders if k=='reminder_'+str(minutes))

def test_cancel_schedules_no_reminders(monkeypatch):
    jobs=[];monkeypatch.setattr(s,'job',lambda *a:jobs.append(a[2]));monkeypatch.setattr(s,'notify',lambda *a:None)
    s.schedule(Mock(),{'id':'x','version':2,'external_calendar_id':'c','snapshot':{'title':'Call'}},'cancelled')
    assert jobs==['calendar_cancel','cancelled','cancelled']

def test_booking_retry_does_not_create_second_booking(monkeypatch):
    from app.meetings.schemas import Guest
    from datetime import datetime
    calls=[]
    t={'id':'type','tenant_id':'tenant','user_id':'user','active':True}
    existing={'id':'booking','guest':{'email':'guest@example.com'}}
    monkeypatch.setattr(s,'profile',lambda *a,**kw:calls.append('lock'))
    monkeypatch.setattr(s,'one',lambda cur,sql,args: t if 'meeting_types' in sql else existing)
    monkeypatch.setattr(s,'available',lambda *a:pytest.fail('Idempotent retry must not book again'))
    result=s.book(Mock(),t,Guest(name='Guest',email='guest@example.com',start='2026-09-10T12:00Z',idempotency_key='unique-request-1234'))
    assert result is existing and calls==['lock']

def test_inactive_link_cannot_book():
    with pytest.raises(Exception):s.book(Mock(),{'active':False},None)

def test_unavailable_slot_does_not_write_booking(monkeypatch):
    t={'id':'type','tenant_id':'tenant','user_id':'user','active':True,'config':{'timezone':'UTC'}}
    monkeypatch.setattr(s,'profile',lambda *a,**kw:None)
    monkeypatch.setattr(s,'one',lambda cur,sql,args: t if 'meeting_types' in sql else None)
    monkeypatch.setattr(s,'sender',lambda *a:({'company_email':'host@marblepress.com'},{}))
    monkeypatch.setattr(s,'available',lambda *a:[])
    monkeypatch.setattr(s,'secret_write',lambda *a:pytest.fail('Unavailable slot must not allocate a management token'))
    with pytest.raises(Exception):s.book(Mock(),t,Guest(name='Guest',email='guest@example.com',start='2026-09-10T12:00Z',idempotency_key='unique-request-1234'))

def test_google_event_update_does_not_create(monkeypatch):
    p=CalendarProvider('google',{});p.api=Mock(return_value={})
    b={'id':'id','external_calendar_id':'cal','external_event_id':'existing','snapshot':{'title':'Call'},'guest':{'name':'Guest','email':'g@example.com'},'start_at':NOW,'end_at':NOW+timedelta(minutes=30)}
    assert p.event(b,'update')=='existing'
    assert p.api.call_args.args[0]=='PATCH' and p.api.call_args.args[1].endswith('/existing')

def test_microsoft_event_creation_has_idempotency_id():
    p=CalendarProvider('microsoft',{});p.api=Mock(return_value={'id':'external'})
    b={'id':'stable-uuid','external_calendar_id':'cal','snapshot':{'title':'Call'},'guest':{'name':'Guest','email':'g@example.com'},'start_at':NOW,'end_at':NOW+timedelta(minutes=30)}
    assert p.event(b,'create')=='external'
    assert p.api.call_args.kwargs['json']['transactionId']=='stable-uuid'
