"""Provider calls are mocked; SQL tenant isolation uses the real PGlite test schema."""
from unittest.mock import Mock
from uuid import uuid4
from contextlib import nullcontext
from cryptography.fernet import Fernet
import pytest
from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient
from test_marketing_analytics import pg, data
from app.marketing import comments as c, comments_routes as routes, service
from app.marketing.providers import ProviderError


@pytest.fixture
def env(monkeypatch, pg, data):
    monkeypatch.setenv('MARKETING_TOKEN_KEY',Fernet.generate_key().decode())
    monkeypatch.setenv('MARKETING_META_API_VERSION','v25.0')
    monkeypatch.setattr(c,'credentials',lambda *a,**k:'not-a-real-token')
    ctx={'tenant':{'id':data['tenant']},'user':{'id':data['user']},'membership_role':'tenant_admin'}
    pg.execute('SELECT id FROM social_post_targets WHERE post_id=%s AND tenant_id=%s',(data['post'],data['tenant']))
    target=pg.fetchone()['id']
    return pg,data,ctx,target


def response(provider='instagram',**changes):
    return {'id':'456','text':'A real question','username':'reader','timestamp':'2026-10-28T00:00:00Z',
            'message':'A real question','from':{'name':'Reader'},'created_time':'2026-10-28T00:00:00Z',
            'can_comment':True,'can_hide':True,'can_remove':True,**changes}


def test_list_and_thread_cursors_are_bound_to_target(env,monkeypatch):
    pg,data,ctx,target_id=env
    calls=[]
    def request(method,url,token,**kwargs):
        calls.append((method,url,kwargs))
        return {'data':[response()], 'paging':{'next':'https://untrusted.invalid/?access_token=secret','cursors':{'after':'next-position'}}}
    monkeypatch.setattr(c,'request',request)
    page=c.listing(pg,ctx,target_id)
    assert page['can_manage'] is True
    assert page['comments'][0]['text']=='A real question'
    assert 'secret' not in page['next_cursor']
    assert calls[-1][1]=='https://graph.instagram.com/v25.0/123/comments'
    c.listing(pg,ctx,target_id,cursor=page['next_cursor'])
    assert calls[-1][2]['params']['after']=='next-position'
    parent=page['comments'][0]['handle']
    c.listing(pg,ctx,target_id,parent=parent)
    assert calls[-1][1].endswith('/456/replies')
    with pytest.raises(HTTPException) as err:c.listing(pg,ctx,target_id,parent=parent,cursor=page['next_cursor'])
    assert err.value.status_code==409
    foreign_ctx={**ctx,'tenant':{'id':data['other']}}
    before=len(calls)
    with pytest.raises(HTTPException) as err:c.listing(pg,foreign_ctx,target_id)
    assert err.value.status_code==404 and len(calls)==before
    owned_target,_=c.destination(pg,ctx,target_id)
    with pytest.raises(HTTPException):c.unseal(parent,{**owned_target,'id':uuid4()},data['tenant'],'comment')
    with pytest.raises(HTTPException):c.unseal(parent[:-5]+'AAAAA',owned_target,data['tenant'],'comment')
    with pytest.raises(HTTPException):c.unseal(page['next_cursor'],owned_target,data['tenant'],'comment')


@pytest.mark.parametrize('provider',['facebook','instagram'])
@pytest.mark.parametrize('action',['reply','hide','unhide','delete'])
def test_actions_provider_paths_and_audit_privacy(env,monkeypatch,provider,action):
    pg,data,ctx,target_id=env
    pg.execute('UPDATE social_accounts SET provider=%s WHERE id=%s AND tenant_id=%s',(provider,data['account'],data['tenant']))
    pg.execute('UPDATE social_post_targets SET provider=%s WHERE id=%s AND tenant_id=%s',(provider,target_id,data['tenant']))
    target,_=c.destination(pg,ctx,target_id)
    item=c.comment_view(response(provider),target,data['tenant'])
    calls=[]
    def request(method,url,token,**kwargs):
        calls.append((method,url,kwargs));return {'id':'789'} if action=='reply' else {'success':True}
    monkeypatch.setattr(c,'request',request)
    assert c.act(pg,ctx,target_id,item['handle'],action,'Private draft reply')['ok']
    method,url,kwargs=calls[0]
    if action=='reply':
        assert method=='POST' and url.endswith('/456/'+('replies' if provider=='instagram' else 'comments'))
        assert kwargs['data']=={'message':'Private draft reply'}
    elif action=='delete':assert method=='DELETE' and url.endswith('/456')
    else:assert kwargs['data']=={('hide' if provider=='instagram' else 'is_hidden'):('true' if action=='hide' else 'false')}
    assert kwargs['redact_errors'] is True
    pg.execute('SELECT detail FROM marketing_audit WHERE tenant_id=%s AND entity_id=%s',(data['tenant'],target_id))
    detail=pg.fetchone()['detail']
    assert detail=={'provider':provider,'comment_id':'456'}


def test_capabilities_disconnect_and_unpublished_guard(env,monkeypatch):
    pg,data,ctx,target_id=env
    pg.execute("UPDATE social_accounts SET provider='facebook' WHERE id=%s",(data['account'],))
    pg.execute("UPDATE social_post_targets SET provider='facebook' WHERE id=%s",(target_id,))
    target,_=c.destination(pg,ctx,target_id)
    item=c.comment_view(response(can_hide=False,can_remove=False,can_comment=False),target,data['tenant'])
    request=Mock();monkeypatch.setattr(c,'request',request)
    with pytest.raises(HTTPException) as err:c.act(pg,ctx,target_id,item['handle'],'hide')
    assert err.value.status_code==403
    pg.execute("UPDATE social_accounts SET status='disconnected' WHERE id=%s",(data['account'],))
    with pytest.raises(HTTPException) as err:c.listing(pg,ctx,target_id)
    assert err.value.status_code==409
    pg.execute("UPDATE social_accounts SET status='connected' WHERE id=%s",(data['account'],))
    pg.execute("UPDATE social_post_targets SET provider_status='failed' WHERE id=%s",(target_id,))
    with pytest.raises(HTTPException) as err:c.listing(pg,ctx,target_id)
    assert err.value.status_code==409
    request.assert_not_called()


def test_expiry_and_safe_uncertain_mutations(env,monkeypatch):
    pg,data,ctx,target_id=env
    target,_=c.destination(pg,ctx,target_id)
    handle=c.seal(target,data['tenant'],'comment',comment='456',capabilities={'reply':True})
    # An expired signed handle cannot be reused even if it authenticates correctly.
    monkeypatch.setattr(c,'cipher',lambda:Mock(decrypt=Mock(side_effect=__import__('cryptography.fernet',fromlist=['InvalidToken']).InvalidToken())))
    with pytest.raises(HTTPException):c.act(pg,ctx,target_id,handle,'reply','Text')
    error=ProviderError('network_error','SECRET raw provider body',True,True)
    with pytest.raises(HTTPException) as result:c.provider_error(error,mutation=True)
    assert result.value.status_code==409 and 'SECRET' not in result.value.detail
    assert 'may already have succeeded' in result.value.detail


def test_reply_validation_before_provider_call(env,monkeypatch):
    pg,data,ctx,target_id=env;target,_=c.destination(pg,ctx,target_id)
    handle=c.comment_view(response(),target,data['tenant'])['handle']
    request=Mock();monkeypatch.setattr(c,'request',request)
    for message in ('   ','x'*2001):
        with pytest.raises(HTTPException) as err:c.act(pg,ctx,target_id,handle,'reply',message)
        assert err.value.status_code==422
    request.assert_not_called()


def test_api_admin_required_no_store_and_input_validation(env,monkeypatch):
    pg,data,ctx,target_id=env
    app=FastAPI();app.include_router(routes.router)
    app.dependency_overrides[service.access]=lambda:ctx
    async def denied():raise HTTPException(403,'Administrator required')
    app.dependency_overrides[service.admin]=denied
    client=TestClient(app)
    assert client.post(f'/social/targets/{target_id}/comments/actions',json={'handle':'x','action':'delete'}).status_code==403
    class Conn:
        def transaction(self):return nullcontext()
        def cursor(self,**kwargs):return pg
    monkeypatch.setattr(routes,'db_conn',lambda:nullcontext(Conn()))
    monkeypatch.setattr(c,'request',lambda *a,**k:{'data':[]})
    result=client.get(f'/social/targets/{target_id}/comments')
    assert result.status_code==200 and result.headers['cache-control']=='no-store'
    assert client.get(f'/social/targets/{target_id}/comments?cursor='+('x'*16385)).status_code==422
    app.dependency_overrides[service.admin]=lambda:ctx
    assert client.post(f'/social/targets/{target_id}/comments/actions',json={'handle':'x','action':'unsupported'}).status_code==422


def test_instagram_preview_uses_published_copy_and_safe_media(env,monkeypatch):
    pg,data,ctx,target_id=env
    monkeypatch.setattr(c,'request',lambda *a,**k:{'id':'123','caption':'Published caption','media_type':'CAROUSEL_ALBUM','permalink':'https://www.instagram.com/p/example/','children':{'data':[{'media_url':'javascript:alert(1)'},{'media_url':'https://cdn.example/test.mp4','media_type':'VIDEO','thumbnail_url':'https://cdn.example/thumb.jpg'}]}})
    result=c.preview(pg,ctx,target_id)
    assert result['text']=='Published caption' and len(result['media'])==1
    assert result['media'][0]['kind']=='video'

def test_reply_to_reply_stays_in_owned_thread(env,monkeypatch):
    pg,data,ctx,target_id=env
    target,_=c.destination(pg,ctx,target_id)
    child=c.comment_view(response(id='777'),target,data['tenant'],reply_to='456')
    calls=[]
    monkeypatch.setattr(c,'request',lambda method,url,*args,**kwargs:calls.append(url) or {'id':'999'})
    c.act(pg,ctx,target_id,child['handle'],'reply','A thread reply')
    assert calls==['https://graph.instagram.com/v25.0/456/replies']


def test_real_expired_token_and_replaced_post_rejected(env,monkeypatch):
    import time,json
    pg,data,ctx,target_id=env
    target,_=c.destination(pg,ctx,target_id)
    handle=c.comment_view(response(),target,data['tenant'])['handle']
    payload=c.cipher().decrypt(handle.encode())
    expired=c.cipher().encrypt_at_time(payload,int(time.time())-1801).decode()
    with pytest.raises(HTTPException):c.unseal(expired,target,data['tenant'],'comment')
    with pytest.raises(HTTPException):c.unseal(handle,{**target,'provider_post_id':'789'},data['tenant'],'comment')


def test_non_admin_can_read_but_is_not_offered_management(env,monkeypatch):
    pg,data,ctx,target_id=env
    monkeypatch.setattr(c,'request',lambda *a,**k:{'data':[response()]})
    result=c.listing(pg,{**ctx,'membership_role':'member'},target_id)
    assert result['comments'] and result['can_manage'] is False
