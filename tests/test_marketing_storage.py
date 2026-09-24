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

def test_unlimited_upload_does_not_scan_storage(monkeypatch,ctx):
    cur=Mock()
    usage=Mock(side_effect=AssertionError("No quota inventory scan needed"))
    monkeypatch.setattr(storage,'usage',usage)
    storage.require_space(cur,ctx,2_000_000_000)
    assert 'FOR UPDATE' in cur.execute.call_args.args[0]
    usage.assert_not_called()

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

