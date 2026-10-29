import json
import time
from unittest.mock import Mock

import pytest
from cryptography.fernet import Fernet
from fastapi import HTTPException
from app.marketing import oauth
from test_marketing_analytics import pg, data


@pytest.fixture
def setup(monkeypatch):
    monkeypatch.setenv('MARKETING_TOKEN_KEY', Fernet.generate_key().decode())
    ctx = {'tenant': {'id': 'tenant-a'}, 'user': {'id': 'user-a'}}
    candidates = [({'id': str(i), 'name': name}, {'access_token': 'secret-page-'+str(i)}, str(i))
                  for i, name in [(1, 'Marble Press'), (2, 'Susan Szecsi ART'), (3, 'Susan Szecsi')]]
    return ctx, candidates


def test_callback_does_not_connect_pages(setup, monkeypatch):
    ctx, candidates = setup
    monkeypatch.setattr(oauth, 'config', lambda _: ('client', 'secret', 'redirect'))
    monkeypatch.setattr(oauth, 'token_exchange', lambda *a: {'access_token': 'user-token'})
    monkeypatch.setattr(oauth, 'graph', lambda: 'https://graph.facebook.com/v25.0')
    monkeypatch.setattr(oauth, 'request', lambda *a, **k: {'data': [dict(item, **tokens) for item, tokens, _ in candidates]})
    persist = Mock()
    monkeypatch.setattr(oauth, 'persist_accounts', persist)
    cur = Mock()
    result = oauth.finish(cur, ctx, 'facebook', 'code')
    assert result['selection_required']
    assert [a['id'] for a in result['accounts']] == ['1', '2', '3']
    assert 'secret-page' not in json.dumps(result)
    persist.assert_not_called()
    assert 'marketing_oauth_states' in cur.execute.call_args.args[0]


@pytest.mark.parametrize('chosen', [['1'], ['1', '2'], ['1', '2', '3']])
def test_only_selected_pages_persist(setup, monkeypatch, chosen):
    ctx, candidates = setup
    ticket = oauth.prepare_selection(Mock(), ctx, 'facebook', candidates)['selection_token']
    consume = Mock()
    persist = Mock(return_value={'connected': len(chosen)})
    monkeypatch.setattr(oauth, 'consume_state', consume)
    monkeypatch.setattr(oauth, 'persist_accounts', persist)
    assert oauth.confirm_selection(Mock(), ctx, 'facebook', ticket, chosen)['connected'] == len(chosen)
    assert [a[0]['id'] for a in persist.call_args.args[3]] == chosen
    consume.assert_called_once()


@pytest.mark.parametrize('chosen', [[], ['missing'], ['1', '1'], ['1', 'missing']])
def test_invalid_selection_writes_nothing(setup, monkeypatch, chosen):
    ctx, candidates = setup
    ticket = oauth.prepare_selection(Mock(), ctx, 'facebook', candidates)['selection_token']
    persist = Mock(); consume = Mock()
    monkeypatch.setattr(oauth, 'persist_accounts', persist)
    monkeypatch.setattr(oauth, 'consume_state', consume)
    with pytest.raises(HTTPException): oauth.confirm_selection(Mock(), ctx, 'facebook', ticket, chosen)
    persist.assert_not_called(); consume.assert_not_called()


@pytest.mark.parametrize('change', ['tenant', 'user', 'provider', 'expired', 'tampered'])
def test_ticket_binding_and_expiration(setup, monkeypatch, change):
    ctx, candidates = setup
    ticket = oauth.prepare_selection(Mock(), ctx, 'facebook', candidates)['selection_token']
    provider = 'facebook'
    if change in ('tenant', 'user'): ctx = {**ctx, change: {'id': 'other'}}
    if change == 'provider': provider = 'instagram'
    if change == 'expired':
        raw = oauth.cipher().decrypt(ticket.encode())
        ticket = oauth.cipher().encrypt_at_time(raw, int(time.time())-700).decode()
    if change == 'tampered': ticket = 'invalid'+ticket
    persist = Mock(); monkeypatch.setattr(oauth, 'persist_accounts', persist)
    with pytest.raises(HTTPException): oauth.confirm_selection(Mock(), ctx, provider, ticket, ['1'])
    persist.assert_not_called()


def test_used_nonce_cannot_persist_again(setup, monkeypatch):
    ctx, candidates = setup
    ticket = oauth.prepare_selection(Mock(), ctx, 'facebook', candidates)['selection_token']
    cur = Mock(); cur.fetchone.return_value = None
    persist = Mock(); monkeypatch.setattr(oauth, 'persist_accounts', persist)
    with pytest.raises(HTTPException): oauth.confirm_selection(cur, ctx, 'facebook', ticket, ['1'])
    assert 'used_at IS NULL' in cur.execute.call_args.args[0]
    persist.assert_not_called()


def test_facebook_callback_offers_linked_instagram_as_independent_choice(setup,monkeypatch):
    ctx,_=setup
    monkeypatch.setenv('MARKETING_META_API_VERSION','v25.0')
    monkeypatch.setattr(oauth,'config',lambda _:('client','secret','redirect'))
    monkeypatch.setattr(oauth,'token_exchange',lambda *a:{'access_token':'user-secret'})
    def request(method,url,*args,**kwargs):
        if url.endswith('/me/permissions'):
            return {'data':[{'permission':'instagram_basic','status':'granted'}]}
        return {'data':[{'id':'111','name':'A Publisher','access_token':'page-secret',
            'instagram_business_account':{'id':'222','username':'publisher_books'}}]}
    monkeypatch.setattr(oauth,'request',request)
    result=oauth.finish(Mock(),ctx,'facebook','code')
    assert [(a['id'],a['provider']) for a in result['accounts']]==[('111','facebook'),('instagram:222','instagram')]
    assert result['accounts'][1]['parent_display_name']=='A Publisher'
    assert 'page-secret' not in json.dumps(result) and 'user-secret' not in json.dumps(result)
    consume=Mock();persist=Mock(return_value={'connected':1})
    monkeypatch.setattr(oauth,'consume_state',consume);monkeypatch.setattr(oauth,'persist_accounts',persist)
    assert oauth.confirm_selection(Mock(),ctx,'facebook',result['selection_token'],['instagram:222'])=={'connected':1}
    assert persist.call_args.args[2]=='instagram'
    selected=persist.call_args.args[3][0]
    assert selected[0]['provider_metadata']['auth_method']=='facebook' and selected[2]=='111'


def test_mixed_choice_ids_do_not_collide_and_only_checked_destinations_persist(setup,monkeypatch):
    ctx,candidates=setup
    mixed=[candidates[0],({'id':'1','provider':'instagram','username':'publisher'}, {'access_token':'secret'},'1')]
    result=oauth.prepare_selection(Mock(),ctx,'facebook',mixed)
    assert [x['id'] for x in result['accounts']]==['1','instagram:1']
    monkeypatch.setattr(oauth,'consume_state',Mock())
    persist=Mock(return_value={'connected':1});monkeypatch.setattr(oauth,'persist_accounts',persist)
    assert oauth.confirm_selection(Mock(),ctx,'facebook',result['selection_token'],['1','instagram:1'])=={'connected':2}
    assert [call.args[2] for call in persist.call_args_list]==['facebook','instagram']


def test_verified_instagram_method_switch_preserves_row_history_and_tenant(pg,data,monkeypatch):
    monkeypatch.setenv('MARKETING_TOKEN_KEY',Fernet.generate_key().decode())
    monkeypatch.setenv('MARKETING_META_API_VERSION','v25.0')
    ctx={'tenant':{'id':data['tenant']},'user':{'id':data['user']}}
    monkeypatch.setattr(oauth,'request',lambda *a,**k:{'id':'123','owner':{'id':'999'}})
    result=[({'id':'999','provider':'instagram','name':'Publisher', 'username':'books',
        'provider_metadata':{'auth_method':'facebook','instagram_ids':['999']}},
        {'access_token':'new-page-token','scope':'instagram_basic,instagram_manage_insights'},'555')]
    assert oauth.persist_accounts(pg,ctx,'instagram',result)=={'connected':1}
    pg.execute('SELECT * FROM social_accounts WHERE id=%s AND tenant_id=%s',(data['account'],data['tenant']))
    account=pg.fetchone()
    assert account['provider_account_id']=='999' and account['provider_parent_account_id']=='555'
    assert account['provider_metadata']['auth_method']=='facebook'
    assert data['account'] in account['provider_metadata']['instagram_ids']
    assert oauth.decrypt(account['access_token_encrypted'],data['tenant'])=='new-page-token'
    pg.execute('SELECT social_account_id FROM social_post_targets WHERE post_id=%s AND tenant_id=%s',(data['post'],data['tenant']))
    assert str(pg.fetchone()['social_account_id'])==data['account']
    pg.execute('SELECT provider_account_id FROM social_accounts WHERE id=%s',(data['foreign'],))
    assert pg.fetchone()['provider_account_id']==data['foreign']
    direct=[({'id':'scoped-id','name':'Publisher','username':'books',
        'provider_metadata':{'auth_method':'instagram','instagram_ids':['scoped-id','999']}},
        {'access_token':'direct-token','scope':'instagram_business_basic'},None)]
    oauth.persist_accounts(pg,ctx,'instagram',direct)
    pg.execute('SELECT id,provider_account_id,provider_parent_account_id,provider_metadata FROM social_accounts WHERE tenant_id=%s',(data['tenant'],))
    accounts=pg.fetchall()
    assert len(accounts)==1 and str(accounts[0]['id'])==data['account']
    assert accounts[0]['provider_account_id']=='scoped-id' and accounts[0]['provider_parent_account_id'] is None
    assert accounts[0]['provider_metadata']['auth_method']=='instagram'


def test_same_username_without_identity_proof_is_not_merged(pg,data,monkeypatch):
    monkeypatch.setenv('MARKETING_META_API_VERSION','v25.0')
    ctx={'tenant':{'id':data['tenant']},'user':{'id':data['user']}}
    pg.execute("UPDATE social_accounts SET username='books' WHERE id=%s",(data['account'],))
    monkeypatch.setattr(oauth,'request',lambda *a,**k:{'id':'123','owner':{'id':'someone-else'}})
    with pytest.raises(HTTPException) as error:
        oauth.instagram_reconnection(pg,ctx,{'id':'999','username':'books','provider_metadata':{'auth_method':'facebook'}}, {'access_token':'fake'})
    assert error.value.status_code==409


def test_instagram_api_host_and_required_scopes_follow_authorization(monkeypatch):
    from app.marketing.providers import instagram_graph
    from app.marketing.analytics_providers import required_scopes
    monkeypatch.setenv('MARKETING_META_API_VERSION','v25.0')
    direct={'provider':'instagram','provider_metadata':{'auth_method':'instagram'}}
    linked={'provider':'instagram','provider_metadata':{'auth_method':'facebook'}}
    assert instagram_graph(direct)=='https://graph.instagram.com/v25.0'
    assert instagram_graph(linked)=='https://graph.facebook.com/v25.0'
    assert 'instagram_business_manage_insights' in required_scopes(direct)
    assert 'instagram_manage_insights' in required_scopes(linked)


@pytest.mark.parametrize('method,host',[('facebook','graph.facebook.com'),('instagram','graph.instagram.com')])
def test_instagram_publishing_uses_the_selected_connection_route(monkeypatch,method,host):
    from app.marketing import providers
    monkeypatch.setenv('MARKETING_META_API_VERSION','v25.0')
    calls=[]
    monkeypatch.setattr(providers,'request',lambda *a,**k:calls.append(a) or {'id':'container'})
    account={'provider_account_id':'111','provider_metadata':{'auth_method':method}}
    post={'title':'Example','content_text':'Caption'}
    target={'provider_state':{},'provider_payload':{}}
    media=[{'public_url':'https://media.example/image.jpg','media_type':'image'}]
    result=providers.InstagramPublisher().advance(account,'fake',post,target,media)
    assert result.state['container']=='container' and calls[0][1]=='https://'+host+'/v25.0/111/media'
