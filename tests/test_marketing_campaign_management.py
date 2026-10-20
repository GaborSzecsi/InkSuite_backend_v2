from contextlib import nullcontext
from unittest.mock import Mock
from uuid import uuid4
import pytest
from fastapi import HTTPException
from app.marketing import routes


def database(monkeypatch):
    cur=Mock()
    conn=Mock()
    conn.cursor.return_value=nullcontext(cur)
    conn.transaction.return_value=nullcontext()
    monkeypatch.setattr(routes,'db_conn',lambda:nullcontext(conn))
    return cur


def test_delete_campaign_keeps_posts_and_media(monkeypatch):
    cur=database(monkeypatch)
    owned=Mock();audit=Mock()
    monkeypatch.setattr(routes.s,'owned',owned)
    monkeypatch.setattr(routes.s,'audit',audit)
    campaign=uuid4();tenant=uuid4();ctx={'tenant':{'id':tenant}}
    assert routes.delete_campaign(campaign,ctx)=={'deleted':str(campaign)}
    owned.assert_called_once_with(cur,'marketing_campaigns',campaign,tenant,True)
    statements=[call.args for call in cur.execute.call_args_list]
    assert len(statements)==3
    assert 'UPDATE social_posts SET campaign_id=NULL' in statements[0][0]
    assert 'UPDATE social_media_assets SET campaign_id=NULL' in statements[1][0]
    assert 'DELETE FROM marketing_campaigns' in statements[2][0]
    assert all(args==(campaign,tenant) for _,args in statements)
    audit.assert_called_once_with(cur,ctx,campaign,'campaign_deleted')


def test_delete_campaign_rejects_unowned_campaign_before_mutation(monkeypatch):
    cur=database(monkeypatch)
    monkeypatch.setattr(routes.s,'owned',Mock(side_effect=HTTPException(404,'Resource not found.')))
    with pytest.raises(HTTPException): routes.delete_campaign(uuid4(),{'tenant':{'id':uuid4()}})
    cur.execute.assert_not_called()


def test_book_search_matches_contributor_names_with_tenant_scope(monkeypatch):
    cur=database(monkeypatch);rows=Mock(return_value=[{'title':'Book','author':'Writer'}])
    monkeypatch.setattr(routes.s,'rows',rows)
    assert routes.works('Writer',{'tenant':{'id':'tenant'}})==rows.return_value
    query,args=rows.call_args.args[1:]
    assert 'w.title ILIKE %s OR names.author ILIKE %s' in query
    assert 'wc.tenant_id=w.tenant_id' in query and 'p.tenant_id=wc.tenant_id' in query
    assert args==('tenant','%Writer%','%Writer%')
