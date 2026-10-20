from datetime import datetime, timezone
from unittest.mock import Mock
from uuid import uuid4
import pytest
from fastapi import HTTPException
from app.marketing import asset_removal as removal, schemas, publishing


@pytest.fixture
def scenario(monkeypatch):
    tenant,asset_id,post_id,project_id=uuid4(),uuid4(),uuid4(),uuid4()
    ctx={'tenant':{'id':tenant,'slug':'tenant'},'user':{'id':uuid4()}}
    asset={'id':asset_id,'filename':'video.mp4','source_type':'generated_derivative','s3_bucket':'test','s3_key':'tenants/tenant/assets/marketing/public/video.mp4'}
    post={'id':post_id,'title':'Failed launch','status':'failed','updated_at':datetime.now(timezone.utc)}
    project={'id':project_id,'name':'My edit','revision':2}
    data={'posts':[post],'targets':[{'id':uuid4(),'provider_status':'failed'}],'publish_jobs':[{'id':uuid4(),'status':'failed'}],'projects':[project],'jobs':[{'id':uuid4(),'status':'completed'}]}
    def rows(cur,query,args):
        assert args[0]==tenant
        if 'FROM social_posts p' in query: return data['posts']
        if 'FROM social_post_targets' in query: return data['targets']
        if 'FROM social_publish_jobs' in query: return data['publish_jobs']
        if 'FROM video_projects' in query: return data['projects']
        if 'FROM video_render_jobs' in query: return data['jobs']
        raise AssertionError(query)
    monkeypatch.setattr(removal.s,'owned',Mock(return_value=asset));monkeypatch.setattr(removal.s,'rows',rows)
    monkeypatch.setattr(removal.assets,'bucket',lambda:'test');monkeypatch.setattr(removal.s,'audit',Mock())
    delete=Mock();monkeypatch.setattr(removal.storage,'delete_key',delete)
    return ctx,asset_id,data,delete


def test_unreviewed_delete_cannot_remove_linked_content(scenario):
    ctx,asset,data,delete=scenario;cur=Mock()
    with pytest.raises(HTTPException) as exc: removal.remove(cur,ctx,asset)
    assert exc.value.status_code==409
    delete.assert_not_called()
    assert not any(c.args[0].startswith('DELETE') for c in cur.execute.call_args_list)


def test_confirmed_delete_removes_local_posts_and_projects_before_file(scenario):
    ctx,asset,data,delete=scenario;cur=Mock()
    plan=removal.preview(cur,ctx,asset);result=removal.remove(cur,ctx,asset,plan['confirmation'])
    assert result['posts_deleted']==result['projects_deleted']==1
    queries=[c.args[0] for c in cur.execute.call_args_list]
    assert any('DELETE FROM social_publish_jobs' in q for q in queries)
    assert any('DELETE FROM video_render_jobs' in q for q in queries)
    assert any('DELETE FROM social_posts' in q for q in queries)
    assert any('DELETE FROM video_projects' in q for q in queries)
    delete.assert_called_once()


@pytest.mark.parametrize('status',['published','partially_published'])
def test_published_history_is_kept_without_the_deleted_media(scenario,status):
    ctx,asset,data,delete=scenario;data['posts'][0]['status']=status;cur=Mock()
    plan=removal.preview(cur,ctx,asset)
    assert plan['posts'][0]['action']=='keep_history'
    result=removal.remove(cur,ctx,asset,plan['confirmation'])
    assert result['posts_deleted']==0 and result['history_kept']==1
    deletes=[c for c in cur.execute.call_args_list if c.args[0].startswith('DELETE FROM social_posts')]
    assert deletes[0].args[1][1]==[]
    assert any('DELETE FROM social_post_assets' in c.args[0] for c in cur.execute.call_args_list)
    delete.assert_called_once()


@pytest.mark.parametrize('change',['post','project','render'])
def test_changed_dependencies_invalidate_confirmation(scenario,change):
    ctx,asset,data,delete=scenario;cur=Mock();plan=removal.preview(cur,ctx,asset)
    if change=='post': data['posts'][0]['status']='scheduled'
    elif change=='project': data['projects'][0]['revision']+=1
    else: data['jobs'].append({'id':uuid4(),'status':'queued'})
    with pytest.raises(HTTPException): removal.remove(cur,ctx,asset,plan['confirmation'])
    delete.assert_not_called()


@pytest.mark.parametrize('active',['processing','submitted','running'])
def test_inflight_publish_or_render_blocks_delete(scenario,active):
    ctx,asset,data,delete=scenario
    if active=='running': data['jobs'][0]['status']=active
    else: data['targets'][0]['provider_status']=active
    plan=removal.preview(Mock(),ctx,asset)
    assert plan['blocked']
    with pytest.raises(HTTPException): removal.remove(Mock(),ctx,asset,plan['confirmation'])
    delete.assert_not_called()


def test_new_posts_move_media_out_of_library_by_default():
    assert schemas.Post().keep_media_in_library is False
    assert schemas.Post(keep_media_in_library=True).keep_media_in_library is True


@pytest.mark.parametrize('status',['published','failed','scheduled'])
def test_only_successful_publication_can_move_library_media(monkeypatch,status):
    monkeypatch.setattr(publishing.s,'rows',lambda *args:[{'provider_status':status}]);cur=Mock()
    publishing.recalculate(cur,uuid4(),uuid4())
    moves=[c for c in cur.execute.call_args_list if 'library_visible=false' in c.args[0]]
    assert bool(moves)==(status=='published')
    if moves:
        assert 'NOT p.keep_media_in_library' in moves[0].args[0]
        assert 'other_p.keep_media_in_library' in moves[0].args[0]
