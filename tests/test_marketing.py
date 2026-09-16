import io
from datetime import datetime,timezone
from uuid import uuid4
from unittest.mock import Mock
import pytest
from app.marketing.domain import association,aggregate,schedule_instant,public_key,upload_key
from app.marketing.providers import FacebookPublisher,InstagramPublisher,PinterestPublisher,TikTokPublisher,ProviderError,request

@pytest.mark.parametrize('kind,works',[('single_work',[1]),('multi_work',[1,2]),('publisher',[])])
def test_campaign_associations(kind,works): association(kind,works)
@pytest.mark.parametrize('kind,works',[('single_work',[]),('single_work',[1,2]),('multi_work',[1,1]),('publisher',[1])])
def test_invalid_associations(kind,works):
    with pytest.raises(ValueError): association(kind,works)
def test_dst_gap_rejected():
    with pytest.raises(ValueError,match='does not exist'): schedule_instant('2026-03-08T02:30','America/Los_Angeles')
def test_dst_fold_requires_choice():
    with pytest.raises(ValueError,match='occurs twice'): schedule_instant('2026-11-01T01:30','America/Los_Angeles')
    first=schedule_instant('2026-11-01T01:30','America/Los_Angeles',0)
    second=schedule_instant('2026-11-01T01:30','America/Los_Angeles',1)
    assert (second-first).total_seconds()==3600
def test_schedule_zone():
    assert schedule_instant('2026-09-15T10:00','America/Los_Angeles')==datetime(2026,9,15,17,tzinfo=timezone.utc)
@pytest.mark.parametrize('states,want',[([], 'draft'),(['published'],'published'),(['published','failed'],'partially_published'),(['failed','failed'],'failed'),(['submitted'],'scheduled'),(['cancelled'],'cancelled')])
def test_status(states,want): assert aggregate(states)==want
@pytest.mark.parametrize('key',['tenants/other/assets/marketing/public/library/a.jpg','tenants/test/data/uploads/uid/private/a.jpg','tenants/test/assets/marketing/public/../a.jpg','tenants/test/assets/marketing/public//a.jpg'])
def test_storage_rejects_private_and_other_tenant(key):
    with pytest.raises(ValueError): public_key('test',key)
def test_title_key_unchanged():
    key='tenants/test/data/uploads/uid/public/cover.jpg'
    assert public_key('test',key,title_uid='uid')==key
    with pytest.raises(ValueError): public_key('test',key,title_uid='different')
def test_upload_paths():
    assert '/library/general/' in upload_key('test','asset','photo.jpg')
    assert '/campaigns/campaign/original/' in upload_key('test','asset','photo.jpg','campaign')

@pytest.fixture
def post(): return {'title':'Cover reveal','content_text':'A new story','timezone':'UTC'}
@pytest.fixture
def target(): return {'provider_payload':{},'provider_state':{}}
@pytest.fixture
def media(): return [{'public_url':'https://media.example.com/test/a.jpg','media_type':'image','mime_type':'image/jpeg','width':1000,'height':1000,'filesize_bytes':10000}]
def test_instagram_container_not_published(monkeypatch,post,target,media):
    monkeypatch.setenv('MARKETING_META_API_VERSION','v25.0')
    monkeypatch.setattr('app.marketing.providers.request',lambda *a,**kw:{'id':'container-1'})
    result=InstagramPublisher().advance({'provider_account_id':'account'},'secret',post,target,media)
    assert result.status=='submitted' and result.state['container']=='container-1'
def test_instagram_waits_for_processing(monkeypatch,post,target,media):
    monkeypatch.setenv('MARKETING_META_API_VERSION','v25.0');target['provider_state']={'container':'container-1'}
    call=Mock(return_value={'status_code':'IN_PROGRESS'})
    monkeypatch.setattr('app.marketing.providers.request',call)
    assert InstagramPublisher().advance({'provider_account_id':'a'},'secret',post,target,media).status=='submitted'
    assert call.call_count==1 and call.call_args.args[0]=='GET'
def test_pinterest_missing_board(post,target,media):
    with pytest.raises(ProviderError): PinterestPublisher().validate_post(post,target,media)
def test_tiktok_final_status(monkeypatch,post,target,media):
    target['provider_state']={'publish_id':'submission'}
    monkeypatch.setattr('app.marketing.providers.request',lambda *a,**kw:{'data':{'status':'PUBLISH_COMPLETE','publicaly_available_post_id':['final-id']}})
    result=TikTokPublisher().advance({},'secret',post,target,media)
    assert result.status=='published' and result.post_id=='final-id'
def test_tiktok_requires_verified_url(monkeypatch,post,target,media):
    target['provider_payload']={'consent':True,'privacy_level':'SELF_ONLY'}
    monkeypatch.delenv('MARKETING_TIKTOK_VERIFIED_URL_PREFIXES',raising=False)
    with pytest.raises(ProviderError,match='verified'): TikTokPublisher().validate_post(post,target,media)
@pytest.mark.parametrize('status,retry,uncertain',[(429,True,False),(503,True,True),(401,False,False),(400,False,False)])
def test_error_classification_redacts_provider_message(monkeypatch,status,retry,uncertain):
    response=Mock(status_code=status);response.json.return_value={'error':{'code':'error','message':'access_token=secret'}}
    monkeypatch.setattr('app.marketing.providers.requests.request',lambda *a,**kw:response)
    with pytest.raises(ProviderError) as raised: request('POST','https://provider.example','secret',json={})
    assert raised.value.retryable==retry and raised.value.uncertain==uncertain
    assert 'secret' not in str(raised.value)
def test_credentials_bound_to_tenant(monkeypatch):
    from cryptography.fernet import Fernet
    from app.marketing.oauth import encrypt,decrypt
    monkeypatch.setenv('MARKETING_TOKEN_KEY',Fernet.generate_key().decode())
    ciphertext=encrypt('test-secret','tenant-a')
    assert 'test-secret' not in ciphertext
    assert decrypt(ciphertext,'tenant-a')=='test-secret'
    with pytest.raises(ProviderError): decrypt(ciphertext,'tenant-b')
def test_tenant_owned_lookup_never_falls_back():
    from app.marketing.service import owned
    from fastapi import HTTPException
    cur=Mock();cur.fetchone.return_value=None
    with pytest.raises(HTTPException) as raised: owned(cur,'works','work-a','tenant-b')
    assert raised.value.status_code==404
    assert cur.execute.call_args.args[1]==('work-a','tenant-b')
def test_unauthenticated_routes_rejected():
    from fastapi import FastAPI
    from fastapi.testclient import TestClient
    from app.marketing.routes import router
    app=FastAPI();app.include_router(router,prefix='/api')
    client=TestClient(app)
    assert client.get('/api/tenants/example/marketing/campaigns').status_code==401
    assert client.post('/api/tenants/example/marketing/social/connect/facebook').status_code==401

def test_title_asset_discovery_and_reference_never_copy(monkeypatch):
    from app.marketing import assets
    from PIL import Image
    wid=uuid4();tenant=uuid4();user=uuid4();folder=str(uuid4())
    key=f'tenants/test/data/uploads/{folder}/public/cover.jpg'
    ctx={'tenant':{'id':tenant,'slug':'test'},'user':{'id':user}}
    buf=io.BytesIO();Image.new('RGB',(1000,1400),'white').save(buf,'JPEG');raw=buf.getvalue()
    storage=Mock();storage.get_paginator.return_value.paginate.return_value=[{'Contents':[{'Key':key,'Size':len(raw)}]}]
    storage.get_object.return_value={'Body':io.BytesIO(raw)}
    monkeypatch.setattr(assets,'s3',lambda:storage)
    monkeypatch.setattr(assets.s,'owned',lambda *args:{'id':wid,'uid':folder})
    calls=[]
    def query(cur,sql,args):
        calls.append((sql,args))
        return None if sql.startswith('SELECT') else {'id':str(uuid4()),'s3_key':key}
    monkeypatch.setattr(assets.s,'one',query)
    from app.marketing.schemas import AssetReference
    result=assets.reference(Mock(),ctx,AssetReference(work_id=wid,s3_key=key))
    assert result['s3_key']==key
    storage.copy_object.assert_not_called();storage.put_object.assert_not_called();storage.put_object_acl.assert_not_called()
    assert storage.get_paginator.return_value.paginate.call_args.kwargs['Prefix']==f'tenants/test/data/uploads/{folder}/public/'
    assert 1000 in calls[-1][1] and 1400 in calls[-1][1]

def test_derivative_preserves_source(monkeypatch):
    from app.marketing import assets
    from app.marketing.schemas import Derivative
    from PIL import Image
    wid=uuid4();tenant=uuid4();campaign=uuid4();source=uuid4();folder=str(uuid4())
    key=f'tenants/test/data/uploads/{folder}/public/cover.jpg'
    buf=io.BytesIO();Image.new('RGB',(600,900),'red').save(buf,'JPEG')
    original={'media_type':'image','source_type':'title_public_asset','campaign_id':None,'work_id':wid,'s3_key':key,'s3_bucket':'bucket'}
    monkeypatch.setattr(assets.s,'owned',lambda cur,table,*args:original if table=='social_media_assets' else {'id':wid,'uid':folder})
    monkeypatch.setattr(assets.s,'one',lambda cur,sql,args:{'key':args[7],'source':args[4]})
    storage=Mock();storage.get_paginator.return_value.paginate.return_value=[];storage.get_object.return_value={'Body':io.BytesIO(buf.getvalue())};monkeypatch.setattr(assets,'s3',lambda:storage)
    ctx={'tenant':{'id':tenant,'slug':'test'},'user':{'id':uuid4()}}
    result=assets.derivative(Mock(),ctx,source,Derivative(campaign_id=campaign))
    uploaded=storage.put_object.call_args.kwargs
    assert f'/campaigns/{campaign}/generated/' in uploaded['Key']
    assert uploaded['Key']!=key and result['source']==source
    with Image.open(io.BytesIO(uploaded['Body'])) as image: assert image.size==(1080,1080)
    storage.delete_object.assert_not_called()

def test_facebook_success(monkeypatch,post,target):
    monkeypatch.setenv('MARKETING_META_API_VERSION','v25.0')
    monkeypatch.setattr('app.marketing.providers.request',lambda *a,**kw:{'id':'page_post'})
    result=FacebookPublisher().advance({'provider_account_id':'page'},'token',post,target,[])
    assert result.status=='published' and result.post_id=='page_post'

def test_tiktok_video_upload_fallback(monkeypatch,post,target):
    from cryptography.fernet import Fernet
    monkeypatch.setenv('MARKETING_TOKEN_KEY',Fernet.generate_key().decode())
    monkeypatch.delenv('MARKETING_TIKTOK_VERIFIED_URL_PREFIXES',raising=False)
    target['provider_payload']={'consent':True,'privacy_level':'SELF_ONLY'}
    video=[{'public_url':'https://s3.example/video.mp4','media_type':'video','mime_type':'video/mp4','duration_seconds':30,'width':1080,'height':1920,'filesize_bytes':25*1024*1024}]
    calls=[]
    def call(method,url,*args,**kwargs):
        calls.append(kwargs)
        return {'data':{'privacy_level_options':['SELF_ONLY'],'max_video_post_duration_sec':60}} if 'creator_info' in url else {'data':{'publish_id':'id','upload_url':'https://open-upload.tiktokapis.com/upload?token=upload-secret'}}
    monkeypatch.setattr('app.marketing.providers.request',call)
    adapter=TikTokPublisher();adapter.validate_post(post,target,video)
    result=adapter.advance({'tenant_id':'tenant'},'secret',post,target,video)
    assert calls[-1]['json']['source_info']['source']=='FILE_UPLOAD'
    assert result.state['total_chunk_count']==2
    assert 'upload-secret' not in str(result.state)

def test_expired_credentials_without_refresh(monkeypatch):
    from app.marketing.oauth import refresh
    account={'provider':'facebook','status':'connected','token_expires_at':datetime(2020,1,1,tzinfo=timezone.utc)}
    with pytest.raises(ProviderError,match='Reconnect'): refresh(account)

def test_oauth_replay_rejected_before_exchange(monkeypatch):
    from app.marketing import oauth
    from fastapi import HTTPException
    monkeypatch.setattr(oauth.s,'one',lambda *a:None)
    exchange=Mock();monkeypatch.setattr(oauth,'token_exchange',exchange)
    with pytest.raises(HTTPException): oauth.consume_state(Mock(),{'tenant':{'id':'tenant'},'user':{'id':'user'}},'facebook','replayed')
    exchange.assert_not_called()
