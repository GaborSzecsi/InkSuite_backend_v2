from contextlib import contextmanager
from uuid import uuid4
from unittest.mock import Mock
import pytest
from fastapi import HTTPException
from app.video import cleanup,routes
from app.video.schema import CleanupRequest

@pytest.fixture
def scenario(monkeypatch):
    tenant,project_id,render_id=uuid4(),uuid4(),uuid4()
    ctx={'tenant':{'id':tenant,'slug':'tenant'},'user':{'id':uuid4()}}
    def asset(name,**extra): return {'id':uuid4(),'filename':name,'source_type':'publisher_asset','s3_key':'tenants/tenant/assets/marketing/private/video/originals/'+name,'s3_bucket':'bucket','filesize_bytes':100,'work_id':None,'campaign_id':None,'project_id':None,'working_project_id':None,'music_license':None,**extra}
    owned=asset('editor.png',working_project_id=project_id);original=asset('original.mp4');book=asset('cover.png',work_id=uuid4());shared=asset('shared.png',working_project_id=project_id);music=asset('music.mp3',music_license={'approved':True});output=asset('finished.mp4',project_id=project_id)
    candidates=[owned,original,book,shared,music,output]
    document={'clips':[{'asset_id':str(a['id'])} for a in candidates]}
    p={'id':project_id,'revision':3,'document':document,'source_asset_id':original['id']}
    job={'status':'completed','job_kind':'composition','project_revision':3,'output_asset_id':output['id'],'document':document}
    monkeypatch.setattr(cleanup.v,'project',Mock(return_value=p));monkeypatch.setattr(cleanup.v,'validate_asset',Mock(side_effect=lambda cur,ctx,id:next(a for a in candidates if str(a['id'])==str(id))))
    monkeypatch.setattr(cleanup.s,'rows',Mock(return_value=candidates))
    def one(cur,query,args=()):
        if query.startswith('SELECT * FROM video_render_jobs'): return job
        if query.startswith('SELECT id FROM social_post_assets') and args[1]==shared['id']: return {'id':'post'}
        return None
    monkeypatch.setattr(cleanup.s,'one',Mock(side_effect=one))
    return {'ctx':ctx,'project_id':project_id,'render_id':render_id,'p':p,'job':job,'owned':owned,'original':original,'book':book,'shared':shared,'music':music,'output':output}


def test_cleanup_defaults_to_editor_imports_and_preserves_library(scenario):
    x=scenario;plan=cleanup.preview(Mock(),x['ctx'],x['project_id'],x['render_id'])
    assert [f['id'] for f in plan['files'] if f['delete']]==[str(x['owned']['id'])]
    assert str(x['output']['id']) not in [f['id'] for f in plan['files']]
    assert plan['delete_count']==1 and plan['delete_bytes']==100


def test_cleanup_original_opt_in_still_preserves_shared_book_and_music(scenario):
    x=scenario;plan=cleanup.preview(Mock(),x['ctx'],x['project_id'],x['render_id'],True)
    assert {f['id'] for f in plan['files'] if f['delete']}=={str(x['owned']['id']),str(x['original']['id'])}
    assert all(not f['delete'] for f in plan['files'] if f['id'] in {str(x[k]['id']) for k in ('book','shared','music')})


@pytest.mark.parametrize('change',[{'status':'failed'},{'job_kind':'normalize'},{'output_asset_id':None},{'project_revision':2}])
def test_cleanup_requires_successful_current_export(scenario,change):
    x=scenario;x['job'].update(change)
    with pytest.raises(HTTPException) as raised: cleanup.preview(Mock(),x['ctx'],x['project_id'],x['render_id'])
    assert raised.value.status_code==409


def test_cleanup_confirmation_changes_with_original_file_choice(scenario):
    x=scenario;a=cleanup.preview(Mock(),x['ctx'],x['project_id'],x['render_id']);b=cleanup.preview(Mock(),x['ctx'],x['project_id'],x['render_id'],True)
    assert a['confirmation']!=b['confirmation']


def test_cleanup_project_lookup_is_tenant_scoped(scenario):
    x=scenario;cur=Mock();cleanup.preview(cur,x['ctx'],x['project_id'],x['render_id'])
    cleanup.v.project.assert_called_once_with(cur,x['project_id'],x['ctx']['tenant']['id'],True)


def database(monkeypatch):
    cur=Mock();conn=Mock()
    @contextmanager
    def borrow():yield conn
    @contextmanager
    def cursor(**kwargs):yield cur
    @contextmanager
    def transaction():yield
    conn.cursor=cursor;conn.transaction=transaction
    monkeypatch.setattr(routes,'db_conn',borrow)
    return cur


def test_missing_export_object_keeps_project(scenario,monkeypatch):
    x=scenario;plan=cleanup.preview(Mock(),x['ctx'],x['project_id'],x['render_id']);cur=database(monkeypatch)
    monkeypatch.setattr(cleanup,'preview',Mock(return_value=plan));client=Mock();client.head_object.side_effect=RuntimeError('missing');monkeypatch.setattr(routes.assets,'s3',lambda:client)
    with pytest.raises(HTTPException): routes.cleanup_project(x['project_id'],CleanupRequest(render_id=x['render_id'],revision=3,confirmation=plan['confirmation']),x['ctx'])
    assert not any('DELETE' in call.args[0] for call in cur.execute.call_args_list)


def test_stale_confirmation_never_deletes_project(scenario,monkeypatch):
    x=scenario;cur=database(monkeypatch)
    with pytest.raises(HTTPException): routes.cleanup_project(x['project_id'],CleanupRequest(render_id=x['render_id'],revision=3,confirmation='0'*64),x['ctx'])
    assert not any('DELETE' in call.args[0] for call in cur.execute.call_args_list)


def test_cleanup_failure_reports_file_and_preserves_output(scenario,monkeypatch):
    from app.marketing import storage
    x=scenario;plan=cleanup.preview(Mock(),x['ctx'],x['project_id'],x['render_id']);cur=database(monkeypatch)
    monkeypatch.setattr(cleanup,'preview',Mock(return_value=plan));monkeypatch.setattr(cleanup,'protected_reason',Mock(return_value=None));monkeypatch.setattr(routes.s,'one',Mock(return_value=x['owned']));monkeypatch.setattr(routes.s,'audit',Mock())
    client=Mock();client.head_object.return_value={'ContentLength':200};monkeypatch.setattr(routes.assets,'s3',lambda:client)
    remove=Mock(side_effect=RuntimeError('S3 unavailable'));monkeypatch.setattr(storage,'delete_key',remove)
    result=routes.cleanup_project(x['project_id'],CleanupRequest(render_id=x['render_id'],revision=3,confirmation=plan['confirmation']),x['ctx'])
    assert result['project_deleted'] and len(result['failed'])==1 and not result['deleted']
    assert result['output_asset_id']==str(x['output']['id'])
    assert remove.call_args.args[-1]==x['owned']['s3_key']

def test_finished_export_remains_protected_after_project_is_removed(scenario):
    x=scenario;a={**x['output'],'project_id':None,'is_finished_video':True}
    assert cleanup.protected_reason(Mock(),x['ctx']['tenant']['id'],a,uuid4())=='Finished video'


def test_legacy_prepared_media_is_not_mistaken_for_disposable_work(scenario):
    x=scenario;a={**x['owned'],'source_type':'generated_derivative','working_project_id':None}
    assert cleanup.protected_reason(Mock(),x['ctx']['tenant']['id'],a,uuid4())=='Existing prepared media or finished video'
