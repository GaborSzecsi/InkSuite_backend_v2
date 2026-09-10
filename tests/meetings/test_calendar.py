from contextlib import contextmanager
from datetime import datetime, timezone
from uuid import uuid4
from unittest.mock import Mock

import pytest
from fastapi import HTTPException
from app.meetings import calendar as c
from app.meetings.providers import CalendarProvider

A = datetime(2026,9,1,tzinfo=timezone.utc)
B = datetime(2026,10,1,tzinfo=timezone.utc)


def test_google_expands_recurring_events_and_paginates():
    p=CalendarProvider('google',{})
    p.api=Mock(side_effect=[{'items':[{'id':'all-day','summary':'Holiday','start':{'date':'2026-09-10'},'end':{'date':'2026-09-11'}}], 'nextPageToken':'next'},
                            {'items':[{'id':'gone','status':'cancelled'},{'id':'meeting','start':{'dateTime':'2026-09-10T10:00:00-07:00'},'end':{'dateTime':'2026-09-10T11:00:00-07:00'}}]}])
    result=p.calendar_events('a@b.com',A,B)
    assert len(result)==2 and result[0]['all_day'] and result[0]['end']=='2026-09-11'
    assert result[1]['title']=='Busy'
    assert p.api.call_args.kwargs['params']['singleEvents']=='true'
    assert p.api.call_args.kwargs['params']['pageToken']=='next'


def test_microsoft_calendar_view_normalizes_utc_and_skips_cancelled():
    p=CalendarProvider('microsoft',{})
    p.api=Mock(return_value={'value':[{'id':'a','subject':'Test','start':{'dateTime':'2026-09-10T17:00:00'},'end':{'dateTime':'2026-09-10T18:00:00'}},{'id':'cancelled','isCancelled':True}]})
    result=p.calendar_events('id',A,B)
    assert result[0]['start'].endswith('Z') and len(result)==1
    assert '/calendarView?' in p.api.call_args.args[1]


@pytest.mark.parametrize('start,end',[(A,A),(B,A),(datetime(2026,9,1),B),(A,datetime(2027,1,1,tzinfo=timezone.utc))])
def test_rejects_invalid_or_unbounded_ranges(start,end):
    with pytest.raises(HTTPException):c.window(start,end)


def test_busy_projection_has_no_private_metadata():
    result=c.busy_view([(A,B)],'Colleague')
    assert set(result[0])=={'id','title','source','person','start','end','all_day'}
    assert result[0]['title']=='Busy'


@pytest.fixture
def db(monkeypatch):
    cur=Mock()
    @contextmanager
    def scope():yield cur
    conn=Mock();conn.cursor=lambda **kw:scope();conn.transaction=scope
    @contextmanager
    def connection():yield conn
    monkeypatch.setattr(c,'db_conn',connection)
    monkeypatch.setattr(c,'ready',lambda cur:True)
    monkeypatch.setattr(c.s,'profile',Mock())
    return cur


def test_team_requires_same_tenant_before_provider_access(monkeypatch,db):
    tenant,user,target=uuid4(),uuid4(),uuid4()
    query=Mock(return_value=[]);provider=Mock()
    monkeypatch.setattr(c.s,'all_rows',query);monkeypatch.setattr(c.s,'adapter',provider)
    with pytest.raises(HTTPException) as error:
        c.events(A,B,target,{'tenant':{'id':tenant},'user':{'id':user}})
    assert error.value.status_code==404
    assert query.call_args.args[2]==(tenant,None,None)
    assert 'share_busy' not in query.call_args.args[1]
    assert 'memberships' in query.call_args.args[1]
    provider.assert_not_called()


def test_team_queries_free_busy_never_event_details(monkeypatch,db):
    target=uuid4()
    monkeypatch.setattr(c,'team_people',Mock(return_value=[{'id':target,'name':'Susan Smith'}]))
    monkeypatch.setattr(c.s,'all_rows',Mock(side_effect=[[],[],[{'status':'connected','calendars':[{'id':'private','name':'Secret'}]}]]))
    provider=Mock();provider.busy.return_value=[(A,B)]
    monkeypatch.setattr(c.s,'adapter',Mock(return_value=provider))
    result=c.events(A,B,target,{'tenant':{'id':uuid4()},'user':{'id':uuid4()}})
    provider.calendar_events.assert_not_called()
    assert result['events'][0]['title']=='Busy'
    assert 'Secret' not in str(result)
    assert result['events'][0]['person']=='Susan'
    assert result['events'][0]['id'].startswith(str(target)+':')


def test_synced_booking_is_not_shown_twice(monkeypatch,db):
    booking={'id':'book','start_at':A,'end_at':B,'snapshot':{'title':'Meeting'},'external_connection_id':'conn','external_calendar_id':'cal','external_event_id':'external'}
    monkeypatch.setattr(c.s,'all_rows',Mock(side_effect=[[booking],[],[{'id':'conn','status':'connected','provider':'google','email':'a@b.com','calendars':[{'id':'cal','name':'Work'}]}]]))
    provider=Mock();provider.calendar_events.return_value=[{'id':'external'}]
    monkeypatch.setattr(c.s,'adapter',Mock(return_value=provider))
    result=c.events(A,B,None,{'tenant':{'id':uuid4()},'user':{'id':uuid4()}})
    assert len(result['events'])==1 and result['events'][0]['id']=='book'


def test_delete_is_scoped_to_owner_and_tenant(monkeypatch,db):
    query=Mock(return_value=None);monkeypatch.setattr(c.s,'one',query)
    tenant,user,event=uuid4(),uuid4(),uuid4()
    with pytest.raises(HTTPException):c.delete_event(event,{'tenant':{'id':tenant},'user':{'id':user}})
    assert query.call_args.args[2]==(event,tenant,user)


def test_added_events_block_booking_availability(monkeypatch):
    monkeypatch.setattr(c.s,'one',Mock(return_value={'present':True}))
    rows=Mock(side_effect=[[],[{'start_at':A,'end_at':B}]])
    monkeypatch.setattr(c.s,'all_rows',rows)
    slots=Mock(return_value=[])
    monkeypatch.setattr(c.s,'slots',slots)
    c.s.available(Mock(),{'tenant_id':'tenant','user_id':'owner','config':{'timezone':'UTC'}},'2026-09-10')
    assert slots.call_args.args[2]==[(A,B)]
    assert rows.call_args.args[2][:2]==('tenant','owner')


def test_team_roster_defaults_to_all_members_without_preferences(monkeypatch):
    rows=Mock(return_value=[])
    monkeypatch.setattr(c.s,'all_rows',rows)
    c.team_people(Mock(),'tenant','viewer')
    sql=rows.call_args.args[1]
    assert 'LEFT JOIN meeting_profiles' in sql
    assert 'share_busy' not in sql and 'meeting_calendar_preferences' not in sql
    assert rows.call_args.args[2]==('tenant','viewer','viewer')


@pytest.mark.parametrize('name,expected',[('Susan Smith','Susan'),('susan.smith@example.com','Susan'),('','Teammate')])
def test_first_name_labels(name,expected):
    assert c.first_name(name)==expected
