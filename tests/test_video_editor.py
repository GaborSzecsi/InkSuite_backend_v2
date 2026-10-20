import subprocess
from pathlib import Path
from uuid import uuid4
from unittest.mock import Mock
import pytest
from PIL import Image
from pydantic import ValidationError
from fastapi import HTTPException
from app.video.schema import Document,Clip,ProjectSave
from app.video.composition import evaluate,dimensions
from app.video import service
from app.video.render import render,probe,Cancelled
from app.marketing.media_prepare import binary
from app.video.scratch import scratch

@pytest.fixture
def video_folder(monkeypatch):
    import os
    if os.name=="nt": monkeypatch.setenv("VIDEO_TEMP_DIR",str(Path.cwd()))
    with scratch() as folder: yield Path(folder)

ASSET=str(uuid4())
def document():
    return Document.model_validate({'tracks':[{'id':'visual','name':'Main','kind':'visual'},{'id':'text','name':'Text','kind':'text'},{'id':'audio','name':'Music','kind':'audio'}],'clips':[{'id':'image','kind':'image','asset_id':ASSET,'track_id':'visual','duration':1},{'id':'text','kind':'text','track_id':'text','duration':1,'text':'InkSuite test','enter':'fade'}]})

def test_project_is_generic_and_schema_is_platform_neutral():
    doc=document();assert doc.schema_version==1 and doc.work_ids==[] and doc.campaign_id is None
    assert Document.model_validate_json(doc.model_dump_json())==doc

@pytest.mark.parametrize('change',[{'duration':301},{'track_id':'audio'},{'start':float('nan')},{'transform':{'x':float('inf')}},{'asset_id':None}])
def test_reject_invalid_compositions(change):
    doc=document().model_dump();doc['clips'][0].update(change)
    with pytest.raises(ValidationError): Document.model_validate(doc)

def test_keyframes_match_browser_evaluator():
    c=Clip(id='text',kind='text',track_id='text',duration=4,keyframes=[{'time':0,'x':0,'y':0,'scale':1,'rotation':0,'opacity':1},{'time':4,'x':1,'y':1,'scale':2,'rotation':90,'opacity':.5}])
    state=evaluate(c,2);assert state['x']==.5 and state['scale']==1.5 and state['rotation']==45 and state['opacity']==.75
    c.enter='fade';assert evaluate(c,0)['opacity']==0
    assert dimensions('9:16',720)==(720,1280)

def test_reject_cross_tenant_asset(monkeypatch):
    owned=Mock(side_effect=HTTPException(404,'Resource not found.'));monkeypatch.setattr(service.s,'owned',owned)
    ctx={'tenant':{'id':'tenant-a','slug':'a'}}
    with pytest.raises(HTTPException): service.validate_document(Mock(),ctx,document())
    assert owned.call_args.args[-1]=='tenant-a'

def test_project_save_detects_revision_conflict(monkeypatch):
    monkeypatch.setattr(service,'validate_document',Mock());monkeypatch.setattr(service,'project',Mock(return_value={'revision':3}))
    cur=Mock()
    with pytest.raises(HTTPException) as e: service.save(cur,{'tenant':{'id':uuid4()}},ProjectSave(document=document(),revision=2),uuid4())
    assert e.value.status_code==409;cur.execute.assert_not_called()

@pytest.mark.parametrize('key',['tenants/other/assets/marketing/private/video/a.mp4','tenants/a/assets/marketing/private/video/../b','tenants/a/assets/marketing/public/a'])
def test_private_paths_cannot_escape_tenant(key):
    with pytest.raises(ValueError): service.private_key('a',key)

def test_render_h264_aac_with_video_text_and_music(video_folder):
    source=video_folder/'video.mp4';music=video_folder/'music.wav';output=video_folder/'mixed.mp4'
    subprocess.run([binary('ffmpeg'),'-v','error','-y','-f','lavfi','-i','testsrc2=s=320x240:r=30:d=2','-f','lavfi','-i','sine=frequency=440:duration=2','-c:v','libx264','-pix_fmt','yuv420p','-c:a','aac',str(source)],check=True)
    subprocess.run([binary('ffmpeg'),'-v','error','-y','-f','lavfi','-i','sine=frequency=220:duration=2',str(music)],check=True)
    a=str(uuid4());doc=document();doc.clips[0]=Clip(id='video',kind='video',asset_id=ASSET,track_id='visual',duration=1,trim=.2,speed=1.5,motion='zoom_in',volume=.3)
    doc.clips.append(Clip(id='music',kind='audio',asset_id=a,track_id='audio',duration=1,volume=.4,fade_in=.1,fade_out=.1))
    progress=[];render(doc,{ASSET:source,a:music},output,720,progress.append);info=probe(output)
    video=next(s for s in info['streams'] if s['codec_type']=='video');audio=next(s for s in info['streams'] if s['codec_type']=='audio')
    assert video['codec_name']=='h264' and audio['codec_name']=='aac'
    assert (video['width'],video['height'])==(720,1280) and video['r_frame_rate']=='30/1'
    assert float(info['format']['duration'])==pytest.approx(1,abs=.04) and progress[-1]==100

def test_images_only_and_export_cancellation(video_folder):
    source=video_folder/'image.png';Image.new('RGB',(180,260),'red').save(source)
    output=video_folder/'result.mp4';render(document(),{ASSET:source},output,720)
    assert probe(output)['streams'][0]['codec_name']=='h264'
    def stop(_): raise Cancelled()
    with pytest.raises(Cancelled): render(document(),{ASSET:source},video_folder/'cancelled.mp4',720,stop)

def test_privacy_guard_refuses_public_bucket_before_customer_upload(monkeypatch):
    from app.video import privacy
    client=Mock();monkeypatch.setattr(privacy.assets,'s3',lambda:client)
    monkeypatch.setattr(privacy.requests,'head',Mock(return_value=Mock(status_code=200)))
    with pytest.raises(ValueError,match='anonymous reads'): privacy.ensure_private_storage({'tenant':{'slug':'a'}})
    client.delete_object.assert_called_once()
    assert client.put_object.call_args.kwargs['Body']==b'InkSuite private media access check'


def test_privacy_guard_accepts_blocked_unsigned_reads(monkeypatch):
    from app.video import privacy
    client=Mock();monkeypatch.setattr(privacy.assets,'s3',lambda:client)
    monkeypatch.setattr(privacy.requests,'head',Mock(return_value=Mock(status_code=403)))
    privacy.ensure_private_storage({'tenant':{'slug':'a'}})
    client.delete_object.assert_called_once()

def test_normalization_preserves_dimensions_and_audio(video_folder):
    from app.video.render import normalize
    source=video_folder/'source.mov';output=video_folder/'copy.mp4'
    subprocess.run([binary('ffmpeg'),'-v','error','-y','-f','lavfi','-i','testsrc2=s=320x240:r=24:d=1','-f','lavfi','-i','sine=duration=1','-c:v','libx264','-c:a','aac',str(source)],check=True)
    normalize(source,output,'video',lambda p:None);info=probe(output)
    video=next(s for s in info['streams'] if s['codec_type']=='video')
    assert video['codec_name']=='h264' and (video['width'],video['height'])==(320,240) and video['r_frame_rate']=='30/1'
    assert any(s['codec_name']=='aac' for s in info['streams'])


def worker_database(monkeypatch,video_folder,rows):
    from contextlib import contextmanager
    from app.video import worker
    cur=Mock();conn=Mock()
    @contextmanager
    def borrow():yield conn
    @contextmanager
    def cursor(**kwargs):yield cur
    @contextmanager
    def transaction():yield
    @contextmanager
    def folder():yield str(video_folder)
    conn.cursor=cursor;conn.transaction=transaction
    monkeypatch.setattr(worker,'db_conn',borrow);monkeypatch.setattr(worker,'scratch',folder)
    monkeypatch.setattr(worker,'ensure_private_storage',Mock())
    monkeypatch.setattr(worker.s,'one',Mock(side_effect=rows))
    return worker,cur


def test_missing_media_marks_job_failed_without_registering_asset(monkeypatch,video_folder):
    job={'id':uuid4(),'tenant_id':uuid4(),'tenant_slug':'a','created_by':uuid4(),'document':document().model_dump(mode='json'),'resolution':720}
    worker,cur=worker_database(monkeypatch,video_folder,[job])
    monkeypatch.setattr(worker.v,'validate_document',Mock(side_effect=ValueError('Missing source media.')))
    assert worker.tick(job['id'])
    assert cur.execute.call_args.args[1][0]=='failed'
    assert not any('INSERT INTO social_media_assets' in call.args[0] for call in cur.execute.call_args_list)


def test_worker_cancellation_does_not_register_output(monkeypatch,video_folder):
    job={'id':uuid4(),'tenant_id':uuid4(),'tenant_slug':'a','created_by':uuid4(),'document':document().model_dump(mode='json'),'resolution':720}
    worker,cur=worker_database(monkeypatch,video_folder,[job,{'status':'running','cancel_requested':True}])
    monkeypatch.setattr(worker.v,'validate_document',Mock(return_value={}))
    monkeypatch.setattr(worker,'render',lambda doc,paths,output,resolution,progress:progress(0))
    assert worker.tick(job['id'])
    assert cur.execute.call_args.args[1][0]=='cancelled'
    assert not any('INSERT INTO social_media_assets' in call.args[0] for call in cur.execute.call_args_list)
