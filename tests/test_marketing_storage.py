from unittest.mock import Mock
import pytest
from fastapi import HTTPException
from app.marketing import storage,assets

@pytest.fixture
def ctx(): return {'tenant':{'id':'tenant-id','slug':'marble'},'user':{'id':'user-id'}}

def test_inventory_is_marketing_only(monkeypatch,ctx):
    client=Mock();client.get_paginator.return_value.paginate.return_value=[{'Contents':[{'Key':'tenants/marble/assets/marketing/public/a','Size':20}]}]
    monkeypatch.setattr(assets,'s3',lambda:client)
    assert storage.usage(ctx)['used_bytes']==20
    assert client.get_paginator.return_value.paginate.call_args.kwargs['Prefix']=='tenants/marble/assets/marketing/public/'

@pytest.mark.parametrize('used,size,allowed',[(0,1,True),(999999999,1,True),(999999999,2,False),(1000000001,1,False)])
def test_limit_and_serialization(monkeypatch,ctx,used,size,allowed):
    cur=Mock()
    def usage(context):
        assert 'FOR UPDATE' in cur.execute.call_args.args[0]
        return {'used_bytes':used}
    monkeypatch.setattr(storage,'usage',usage)
    if allowed: storage.require_space(cur,ctx,size)
    else:
        with pytest.raises(HTTPException) as e: storage.require_space(cur,ctx,size)
        assert e.value.status_code==413

@pytest.mark.parametrize('key',['tenants/other/assets/marketing/public/a','tenants/marble/data/uploads/title/public/a'])
def test_deletion_cannot_escape_marketing(monkeypatch,ctx,key):
    client=Mock();monkeypatch.setattr(assets,'s3',lambda:client)
    with pytest.raises(ValueError): storage.delete_key(Mock(),ctx,key)
    client.delete_object.assert_not_called()

def test_active_post_protects_file(monkeypatch,ctx):
    client=Mock();monkeypatch.setattr(assets,'s3',lambda:client)
    monkeypatch.setattr(storage.s,'one',Mock(side_effect=[{'id':'asset','source_type':'publisher_asset'},{'id':'active-post'}]))
    with pytest.raises(HTTPException) as e: storage.delete_key(Mock(),ctx,'tenants/marble/assets/marketing/public/a')
    assert e.value.status_code==409
    client.delete_object.assert_not_called()

def test_old_asset_deleted_without_deleting_derivatives(monkeypatch,ctx):
    client=Mock();cur=Mock();monkeypatch.setattr(assets,'s3',lambda:client)
    monkeypatch.setattr(storage.s,'one',Mock(side_effect=[{'id':'asset','source_type':'publisher_asset'},None]))
    key='tenants/marble/assets/marketing/public/a'
    storage.delete_key(cur,ctx,key)
    assert client.delete_object.call_args.kwargs['Key']==key
    assert any('source_asset_id=NULL' in call.args[0] for call in cur.execute.call_args_list)

def test_quota_failure_prevents_s3_upload(monkeypatch,ctx):
    client=Mock();monkeypatch.setattr(assets,'s3',lambda:client)
    monkeypatch.setattr(assets,'inspect_media',lambda *args:('image/jpeg','image',100,100,None))
    monkeypatch.setattr(storage,'usage',lambda _: {'used_bytes':storage.LIMIT_BYTES})
    with pytest.raises(HTTPException): assets.upload(Mock(),ctx,'file.jpg',b'data',None,'general')
    client.put_object.assert_not_called()
