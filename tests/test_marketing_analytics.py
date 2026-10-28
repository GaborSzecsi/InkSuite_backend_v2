import json
import subprocess
from contextlib import contextmanager
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import Mock, patch
from uuid import uuid4
from psycopg.types.json import Jsonb

import pytest
from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient
from app.marketing import analytics as a, analytics_worker as worker, oauth
from app.marketing.analytics_providers import (Collector, Collection, AnalyticsError, numeric, scopes, VERSION)
from app.marketing.analytics_routes import router
from app.marketing import service


class Cursor:
    def __init__(self,process): self.process=process; self.buffer=[]
    def execute(self,sql,args=()):
        for i in range(len(args)): sql=sql.replace('%s',f'${i+1}',1)
        self.process.stdin.write(json.dumps({'sql':sql,'params':[json.dumps(x.obj) if isinstance(x,Jsonb) else x for x in args]},default=str)+'\n');self.process.stdin.flush()
        result=json.loads(self.process.stdout.readline())
        if 'error' in result: raise RuntimeError(result['code']+': '+result['error'])
        self.buffer=result['result']['rows']
        for row in self.buffer:
            for key,value in row.items():
                if isinstance(value,str) and (key.endswith('_at') or key=='observed_at'):
                    row[key]=datetime.fromisoformat(value.replace('Z','+00:00'))
                elif isinstance(value,str) and key in ('period_start','period_end','last_collected_through','backfill_before'):
                    row[key]=date.fromisoformat(value[:10])
        return self
    def fetchone(self): return self.buffer.pop(0) if self.buffer else None
    def fetchall(self): rows=self.buffer;self.buffer=[];return rows
    def __enter__(self): return self
    def __exit__(self,*args): pass


@pytest.fixture(scope='module')
def pg():
    path=Path(__file__).parent/'marketplace_pg'/'analytics_rpc.cjs'
    process=subprocess.Popen(['node',str(path)],stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True,encoding='utf-8')
    assert json.loads(process.stdout.readline())=={'ready':True}
    yield Cursor(process)
    process.stdin.close();process.wait(timeout=10)


@pytest.fixture
def data(pg):
    tenant,other,user,account,foreign,campaign,post,media=[str(uuid4()) for _ in range(8)]
    for value in (tenant,other): pg.execute('INSERT INTO tenants(id) VALUES(%s)',(value,))
    pg.execute('INSERT INTO users(id) VALUES(%s)',(user,))
    for owner,identifier in ((tenant,account),(other,foreign)):
        pg.execute("INSERT INTO social_accounts(id,tenant_id,provider,provider_account_id,display_name,account_type,access_token_encrypted,connected_by,scopes) VALUES(%s,%s,'instagram',%s,'Example','professional','encrypted',%s,'[\"instagram_business_basic\"]')",(identifier,owner,identifier,user))
    pg.execute("INSERT INTO marketing_campaigns(id,tenant_id,name,association_type,created_by) VALUES(%s,%s,'Launch','publisher',%s)",(campaign,tenant,user))
    pg.execute("INSERT INTO social_posts(id,tenant_id,campaign_id,created_by) VALUES(%s,%s,%s,%s)",(post,tenant,campaign,user))
    pg.execute("INSERT INTO social_post_targets(id,tenant_id,post_id,social_account_id,provider,provider_post_id,provider_status) VALUES(%s,%s,%s,%s,'instagram','123','published')",(str(uuid4()),tenant,post,account))
    return dict(tenant=tenant,other=other,user=user,account=account,foreign=foreign,campaign=campaign,post=post,media=media)


def result(day=None):
    day=day or datetime.now(timezone.utc).date()
    r=Collection(api_version='v25.0')
    r.media=[dict(external_id='123',media_type='IMAGE',title='Book',permalink='https://www.instagram.com/p/example/',thumbnail_url=None,published_at=datetime.combine(day,datetime.min.time(),timezone.utc))]
    for metric,value in [('reach',100),('views',150),('engagements',10),('likes',8),('comments',0),('shares',1),('saves',1)]: r.add(metric,value,day,scope='lifetime',subject='123')
    for metric,value in [('reach',80),('views',120),('engagements',9)]: r.add(metric,value,day)
    r.add('followers',25,day,scope='snapshot')
    return r


def save(pg,d,r=None,run=None,observed=None):
    account={'id':d['account'],'tenant_id':d['tenant'],'provider':'instagram'}
    worker.persist(pg,account,r or result(),run or uuid4(),observed or datetime.now(timezone.utc))


def test_schema_rejects_cross_tenant_account(pg,data):
    with pytest.raises(RuntimeError,match='23503'):
        pg.execute("INSERT INTO social_analytics_media(id,tenant_id,account_id,external_id,media_type) VALUES(%s,%s,%s,'x','image')",(uuid4(),data['other'],data['account']))


def test_idempotency_and_preserved_history(pg,data):
    run=uuid4();save(pg,data,run=run);save(pg,data,run=run)
    pg.execute('SELECT count(*) AS n FROM social_analytics_observations WHERE tenant_id=%s',(data['tenant'],))
    assert pg.fetchone()['n']==11
    save(pg,data)
    pg.execute('SELECT count(*) AS n FROM social_analytics_observations WHERE tenant_id=%s',(data['tenant'],))
    assert pg.fetchone()['n']==22


def test_filters_report_zero_campaign_and_history(pg,data):
    save(pg,data)
    report=a.report(pg,data['tenant'])
    assert len(report['accounts'])==1
    assert report['posts'][0]['metrics']['comments']['value']==0
    assert report['posts'][0]['metrics']['engagement_rate']['value']==10
    assert report['posts'][0]['source']=='inksuite'
    assert report['campaigns'][0]['platforms'][0]['metrics']['reach']['value']==100
    assert report['overview'][0]['metrics']['reach']['status']=='partial' # only one of 30 days
    detail=a.post_detail(pg,data['tenant'],report['posts'][0]['id'])
    assert len(detail['history'])==7
    assert a.report(pg,data['tenant'],campaign_id=data['campaign'])['posts'][0]['post_id']==data['post']
    assert a.report(pg,data['tenant'],provider='tiktok')['posts']==[]


def test_cross_tenant_reads_are_blocked(pg,data):
    save(pg,data)
    for operation in (lambda:a.report(pg,data['other'],account_id=data['account']),lambda:a.report(pg,data['other'],campaign_id=data['campaign']),lambda:a.refresh(pg,{'tenant':{'id':data['other']},'user':{'id':data['user']}},data['account'])):
        with pytest.raises(HTTPException) as err: operation()
        assert err.value.status_code==404
    assert a.report(pg,data['other'])['posts']==[]
    media=a.report(pg,data['tenant'])['posts'][0]['id']
    with pytest.raises(HTTPException): a.post_detail(pg,data['other'],media)


def test_refresh_throttled_and_disconnected(pg,data):
    ctx={'tenant':{'id':data['tenant']},'user':{'id':data['user']}}
    assert a.refresh(pg,ctx,data['account'])['status']=='pending'
    with pytest.raises(HTTPException) as err: a.refresh(pg,ctx,data['account'])
    assert err.value.status_code==429
    pg.execute("UPDATE social_accounts SET status='disconnected' WHERE id=%s",(data['account'],))
    with pytest.raises(HTTPException) as err: a.refresh(pg,ctx,data['account'])
    assert err.value.status_code==409


def test_numeric_and_scopes():
    assert numeric(0)==0
    for value in (None,True,'12',float('nan'),-1): assert numeric(value) is None
    assert scopes({'scopes':['pins:read user_accounts:read','video.list,user.info.stats']})=={'pins:read','user_accounts:read','video.list','user.info.stats'}


def test_tiktok_counts_and_no_invented_reach(monkeypatch):
    now=datetime.now(timezone.utc);collector=Collector({'provider':'tiktok','provider_account_id':'id','scopes':['video.list','user.info.stats']},'fake',now.date()-timedelta(days=1),now.date())
    responses=[{'data':{'user':{'follower_count':10}}},{'data':{'videos':[{'id':'123','create_time':int(now.timestamp()),'like_count':4,'comment_count':0,'share_count':2,'view_count':100}], 'has_more':False}}]
    monkeypatch.setattr(collector,'call',lambda *args,**kwargs:responses.pop(0))
    collection=collector.collect()
    assert len(collection.media)==1
    assert not any(r['metric']=='reach' for r in collection.observations)
    metrics=a.post_metrics([dict(r,account_id='id',definition_version=VERSION,observed_at=now) for r in collection.observations if r['subject']])
    a.rate(metrics,'tiktok')
    assert metrics['engagement_rate']['value']==6


def test_pinterest_daily_mapping_skips_processing_and_keeps_zero():
    day=date(2026,10,26);collector=Collector({},'fake',day,day)
    collector.pinterest_daily({'all':{'daily_metrics':[{'date':str(day),'data_status':'READY','metrics':{'IMPRESSION':20,'SAVE':0}},{'date':str(day),'data_status':'PROCESSING','metrics':{'IMPRESSION':999}}]}})
    assert [(r['metric'],r['value']) for r in collector.result.observations]==[('impressions',20),('saves',0)]


@pytest.mark.parametrize('status,code,expected,retry',[(401,'190','authorization_expired',False),(403,'200','permission_missing',False),(429,'x','rate_limited',True),(500,'x','temporary_failure',True),(400,'100','unsupported',False)])
def test_errors_redact_payload(monkeypatch,status,code,expected,retry,capsys):
    response=Mock(status_code=status);response.json.return_value={'error':{'code':code,'message':'access_token=DO_NOT_LOG'}}
    monkeypatch.setattr('app.marketing.analytics_providers.requests.request',lambda *args,**kwargs:response)
    with pytest.raises(AnalyticsError) as err: Collector({},'SECRET',date.today(),date.today()).call('GET','https://graph.instagram.com/me')
    assert err.value.category==expected and err.value.retryable==retry
    assert 'SECRET' not in str(err.value) and 'DO_NOT_LOG' not in capsys.readouterr().out


def test_meta_pagination_never_follows_external_url(monkeypatch):
    c=Collector({},'fake',date.today(),date.today());calls=[]
    def call(method,url,**kw):
        calls.append((url,kw))
        return {'data':[{'id':'1'}],'paging':{'next':'https://evil.example/?access_token=secret','cursors':{'after':'cursor'}}} if len(calls)==1 else {'data':[{'id':'2'}]}
    monkeypatch.setattr(c,'call',call)
    assert list(c.pages('https://graph.instagram.com/me/media',{}))==[{'id':'1'},{'id':'2'}]
    assert all(url=='https://graph.instagram.com/me/media' for url,_ in calls)


def test_unsupported_metric_does_not_stop_other_metrics(monkeypatch):
    c=Collector({},'fake',date.today(),date.today())
    def call(method,url,**kw):
        if kw['params']['metric']=='views':raise AnalyticsError('unsupported')
        return {'data':[{'name':'reach','values':[{'value':12}]}]}
    monkeypatch.setattr(c,'call',call)
    c.meta_insights('https://graph.instagram.com/123',{'views':'views','reach':'reach'},'123')
    assert c.result.observations[0]['availability']=='unsupported'
    assert c.result.observations[1]['value']==12


def test_opt_in_scopes_do_not_add_comments_or_messages(monkeypatch):
    monkeypatch.setattr(oauth,'config',lambda _:('client','secret','redirect'));monkeypatch.setattr(oauth,'cipher',Mock())
    from urllib.parse import urlparse,parse_qs
    ctx={'tenant':{'id':'tenant'},'user':{'id':'user'}}
    regular=parse_qs(urlparse(oauth.begin(Mock(),ctx,'instagram')['url']).query)['scope'][0]
    analytics=parse_qs(urlparse(oauth.begin(Mock(),ctx,'instagram',analytics=True)['url']).query)['scope'][0]
    assert set(regular.split(','))=={'instagram_business_basic','instagram_business_content_publish'}
    assert set(analytics.split(','))==set(regular.split(','))|{'instagram_business_manage_insights'}
    tiktok=parse_qs(urlparse(oauth.begin(Mock(),ctx,'tiktok',analytics=True)['url']).query)['scope'][0]
    assert {'video.list','user.info.stats'}.issubset(tiktok.split(','))


def test_api_requires_auth_and_admin():
    app=FastAPI();app.include_router(router)
    async def denied():raise HTTPException(403,'Denied')
    app.dependency_overrides[service.access]=denied;app.dependency_overrides[service.admin]=denied
    client=TestClient(app)
    assert client.get('/analytics/overview').status_code==403
    assert client.post('/analytics/sync/'+str(uuid4())).status_code==403


def test_window_and_incompatible_rate():
    with pytest.raises(HTTPException):a.window(date(2025,1,1),date(2026,1,1))
    with pytest.raises(HTTPException):a.window(date.today()+timedelta(days=1),date.today()+timedelta(days=1))
    metrics={'engagements':dict(value=10,status='available',basis='selected_period',coverage=1),'reach':dict(value=100,status='available',basis='lifetime',coverage=1)}
    a.rate(metrics,'instagram');assert metrics['engagement_rate']['value'] is None


class Connection:
    def __init__(self,cur):self.cur=cur
    def cursor(self,**kwargs):return self.cur
    @contextmanager
    def transaction(self):
        self.cur.execute('BEGIN')
        try:yield
        except Exception:self.cur.execute('ROLLBACK');raise
        else:self.cur.execute('COMMIT')


def test_worker_claim_persist_schedule_and_disconnect(pg,data,monkeypatch):
    pg.execute("UPDATE social_accounts SET status='disconnected' WHERE id<>%s",(data['account'],))
    @contextmanager
    def connection():yield Connection(pg)
    monkeypatch.setattr(worker,'db_conn',connection)
    monkeypatch.setattr(worker,'credentials',lambda *args,**kwargs:'fake')
    collector=Mock();collector.collect.return_value=result()
    monkeypatch.setattr(worker,'Collector',lambda *args,**kwargs:collector)
    assert worker.tick()
    pg.execute('SELECT * FROM social_analytics_sync WHERE account_id=%s',(data['account'],));state=pg.fetchone()
    assert state['status']=='success' and state['last_success_at'] is not None
    assert state['backfill_before'] is not None
    assert not worker.tick() # not due, cannot collect twice
    pg.execute("UPDATE social_accounts SET status='disconnected' WHERE id=%s",(data['account'],))
    pg.execute("UPDATE social_analytics_sync SET next_run_at=now()-interval '1 minute' WHERE account_id=%s",(data['account'],))
    assert not worker.tick()


def test_worker_permission_error_backoff_and_recovery(pg,data,monkeypatch):
    pg.execute("UPDATE social_accounts SET status='disconnected' WHERE id<>%s",(data['account'],))
    @contextmanager
    def connection():yield Connection(pg)
    monkeypatch.setattr(worker,'db_conn',connection)
    monkeypatch.setattr(worker,'credentials',lambda *args,**kwargs:'fake')
    collector=Mock();collector.collect.side_effect=AnalyticsError('permission_missing')
    monkeypatch.setattr(worker,'Collector',lambda *args,**kwargs:collector)
    assert worker.tick()
    pg.execute('SELECT * FROM social_analytics_sync WHERE account_id=%s',(data['account'],));state=pg.fetchone()
    assert state['status']=='permission_missing' and state['last_success_at'] is None
    assert state['failures']==1 and state['next_run_at']>datetime.now(timezone.utc)+timedelta(hours=5)
    collector.collect.side_effect=None;collector.collect.return_value=result()
    pg.execute("UPDATE social_analytics_sync SET next_run_at=now()-interval '1 minute' WHERE account_id=%s",(data['account'],))
    assert worker.tick()
    pg.execute('SELECT status,failures FROM social_analytics_sync WHERE account_id=%s',(data['account'],))
    assert pg.fetchone()=={'status':'success','failures':0}


def test_redacted_token_refresh_transport(monkeypatch,capsys):
    from app.marketing.providers import request,ProviderError
    response=Mock(status_code=401);response.json.return_value={'error':{'code':190,'message':'DO_NOT_LOG'}}
    monkeypatch.setattr('app.marketing.providers.requests.request',lambda *args,**kwargs:response)
    with pytest.raises(ProviderError):request('GET','https://provider.example',redact_errors=True)
    assert 'DO_NOT_LOG' not in capsys.readouterr().out


def test_full_period_comparison_and_snapshot_growth(pg,data):
    today=datetime.now(timezone.utc).date();prior=today-timedelta(days=1)
    before=result(prior);before.observations=[r for r in before.observations if not r['subject']]
    save(pg,data,before,observed=datetime.combine(prior,datetime.min.time(),timezone.utc))
    current=result(today);current.observations=[{**r,'value':50 if r['metric']=='followers' else 160 if r['metric']=='reach' else r['value']} for r in current.observations]
    save(pg,data,current)
    panel=a.report(pg,data['tenant'],start=today,end=today)['overview'][0]
    assert panel['metrics']['reach']['change_percent']==100
    assert panel['metrics']['follower_growth']['value']==25
    assert any(p['metric']=='engagement_rate' for p in a.report(pg,data['tenant'],start=today,end=today)['series'])


def test_disconnected_guard_prevents_network_calls(monkeypatch):
    active=True
    @contextmanager
    def guard():
        if not active:raise AnalyticsError('disconnected')
        yield
    response=Mock(status_code=200);response.json.return_value={'data':[]}
    transport=Mock(return_value=response)
    monkeypatch.setattr('app.marketing.analytics_providers.requests.request',transport)
    collector=Collector({},'secret',date.today(),date.today(),guard=guard)
    collector.call('GET','https://graph.instagram.com/me')
    active=False
    with pytest.raises(AnalyticsError,match='disconnected'):collector.call('GET','https://graph.instagram.com/me')
    assert transport.call_count==1


def test_pagination_checkpoint_resumes_current_endpoint(monkeypatch):
    day=date.today();checkpoint={'start':str(day),'end':str(day),'positions':{'media':'saved-cursor'}}
    collector=Collector({},'fake',day,day,checkpoint=checkpoint)
    call=Mock(return_value={'data':[{'id':'456'}]})
    monkeypatch.setattr(collector,'call',call)
    assert list(collector.pages('https://graph.instagram.com/me/media',{}))==[{'id':'456'}]
    assert call.call_args.kwargs['params']['after']=='saved-cursor'
    assert collector.result.cursors['positions']=={}


def test_http_report_serialization_and_no_cache(pg,data,monkeypatch):
    from app.marketing import analytics_routes
    save(pg,data)
    @contextmanager
    def connection():yield Connection(pg)
    monkeypatch.setattr(analytics_routes,'db_conn',connection)
    app=FastAPI();app.include_router(router)
    async def access():return {'tenant':{'id':data['tenant']},'user':{'id':data['user']}}
    app.dependency_overrides[service.access]=access
    client=TestClient(app)
    response=client.get('/analytics/overview')
    assert response.status_code==200 and response.headers['cache-control']=='no-store'
    assert response.json()['posts'][0]['metrics']['comments']['value']==0
    assert client.get('/analytics/overview?account_id='+data['foreign']).status_code==404


def test_same_external_account_and_post_are_isolated_per_tenant(pg,data):
    pg.execute('UPDATE social_accounts SET provider_account_id=%s WHERE id=%s',(data['account'],data['foreign']))
    save(pg,data)
    other=result()
    for row in other.observations:
        if row['metric']=='likes':row['value']=999
    worker.persist(pg,{'id':data['foreign'],'tenant_id':data['other'],'provider':'instagram'},other,uuid4(),datetime.now(timezone.utc))
    first=a.report(pg,data['tenant']);second=a.report(pg,data['other'])
    assert first['posts'][0]['external_id']==second['posts'][0]['external_id']=='123'
    assert first['posts'][0]['metrics']['likes']['value']==8
    assert second['posts'][0]['metrics']['likes']['value']==999
    assert first['posts'][0]['source']=='inksuite' and second['posts'][0]['source']=='external'
    assert second['posts'][0]['campaign_id'] is None
    assert all(str(x['id'])!=data['foreign'] for x in first['accounts'])
    with pytest.raises(HTTPException):a.post_detail(pg,data['other'],first['posts'][0]['id'])


def test_background_worker_services_multiple_tenants(pg,data,monkeypatch):
    pg.execute("UPDATE social_accounts SET status='disconnected' WHERE id<>%s AND id<>%s",(data['account'],data['foreign']))
    @contextmanager
    def connection():yield Connection(pg)
    monkeypatch.setattr(worker,'db_conn',connection)
    monkeypatch.setattr(worker,'credentials',lambda *args,**kwargs:'fake')
    collected=[]
    def collector(account,*args,**kwargs):
        collected.append(account['tenant_id'])
        instance=Mock();instance.collect.return_value=result();return instance
    monkeypatch.setattr(worker,'Collector',collector)
    assert worker.tick() and worker.tick()
    assert set(collected)=={data['tenant'],data['other']}
    for tenant in (data['tenant'],data['other']):
        pg.execute('SELECT count(*) AS n FROM social_analytics_observations WHERE tenant_id=%s',(tenant,))
        assert pg.fetchone()['n']==11


def test_retry_after_is_honored_without_exposing_provider_body(monkeypatch):
    response=Mock(status_code=429,headers={'Retry-After':'3600'});response.json.return_value={'error':{'code':'rate_limit_exceeded'}}
    monkeypatch.setattr('app.marketing.analytics_providers.requests.request',lambda *args,**kwargs:response)
    with pytest.raises(AnalyticsError) as error:Collector({},'fake',date.today(),date.today()).call('GET','https://provider.example')
    assert error.value.retry_after==3600


def test_missing_period_snapshot_does_not_invent_zero_growth(pg,data):
    today=datetime.now(timezone.utc).date();prior=today-timedelta(days=1)
    r=result(prior);r.observations=[row for row in r.observations if row['metric']=='followers']
    save(pg,data,r,observed=datetime.combine(prior,datetime.min.time(),timezone.utc))
    metric=a.report(pg,data['tenant'],start=today,end=today)['overview'][0]['metrics']['follower_growth']
    assert metric['value'] is None


def test_missing_insights_grant_does_not_repeat_denied_calls(monkeypatch):
    collector=Collector({},'fake',date.today(),date.today())
    call=Mock(side_effect=AnalyticsError('permission_missing'));monkeypatch.setattr(collector,'call',call)
    collector.meta_insights('https://graph.instagram.com/123',{'views':'views','reach':'reach'},'123')
    assert call.call_count==1
    assert [row['availability'] for row in collector.result.observations]==['permission_missing','permission_missing']
