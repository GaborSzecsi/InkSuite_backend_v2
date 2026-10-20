from .scratch import scratch
"""Run separately: python -m app.video.worker. One bounded render at a time per process."""
import logging,os,time,tempfile
from pathlib import Path
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb
from app.core.db import db_conn
from app.marketing import service as s,assets
from app.marketing.storage import lock
from app.marketing.domain import tenant_root
from .schema import Document
from . import service as v
from .render import render,probe,normalize,Cancelled
from .privacy import ensure_private_storage
log=logging.getLogger(__name__)

def tick(job_id=None):
    with db_conn() as conn,conn.transaction(),conn.cursor(row_factory=dict_row) as cur:
        cur.execute("UPDATE video_render_jobs SET status=CASE WHEN cancel_requested THEN 'cancelled' ELSE 'failed' END,error='The render worker stopped. Retry this export.',completed_at=now() WHERE status='running' AND heartbeat_at<now()-interval '5 minutes' AND (%s::uuid IS NULL OR id=%s)",(job_id,job_id))
        job=s.one(cur,"""SELECT j.*,t.slug AS tenant_slug,p.name AS project_name FROM video_render_jobs j JOIN tenants t ON t.id=j.tenant_id LEFT JOIN video_projects p ON p.id=j.project_id AND p.tenant_id=j.tenant_id WHERE j.status='queued' AND (%s::uuid IS NULL OR j.id=%s) AND NOT j.cancel_requested AND NOT EXISTS(SELECT 1 FROM video_render_jobs active WHERE active.tenant_id=j.tenant_id AND active.status='running') ORDER BY j.created_at FOR UPDATE OF j SKIP LOCKED LIMIT 1""",(job_id,job_id))
        if not job: return False
        cur.execute("UPDATE video_render_jobs SET status='running',started_at=now(),heartbeat_at=now() WHERE id=%s",(job['id'],))
    ctx={'tenant':{'id':job['tenant_id'],'slug':job['tenant_slug']},'user':{'id':job['created_by']}}
    def progress(p):
        with db_conn() as conn,conn.cursor(row_factory=dict_row) as cur:
            current=s.one(cur,'SELECT cancel_requested,status FROM video_render_jobs WHERE id=%s AND tenant_id=%s',(job['id'],job['tenant_id']))
            if not current or current['cancel_requested'] or current['status']!='running': raise Cancelled()
            cur.execute('UPDATE video_render_jobs SET progress=%s,heartbeat_at=now() WHERE id=%s AND tenant_id=%s',(min(99,p),job['id'],job['tenant_id']))
    key=None
    try:
        ensure_private_storage(ctx)
        normalizing=job.get('job_kind')=='normalize'
        doc=None if normalizing else Document.model_validate(job['document'])
        with scratch() as folder:
            paths={};total=0;last_heartbeat=time.monotonic()
            with db_conn() as conn,conn.cursor(row_factory=dict_row) as cur:
                media={str(job['normalize_asset_id']):v.validate_asset(cur,ctx,job['normalize_asset_id'])} if normalizing else v.validate_document(cur,ctx,doc)
                if doc and doc.canvas.background_asset_id: media[str(doc.canvas.background_asset_id)]=v.validate_asset(cur,ctx,doc.canvas.background_asset_id)
                for identity,a in media.items():
                    progress(0);path=Path(folder)/(identity+'.media');stream=assets.s3().get_object(Bucket=a['s3_bucket'],Key=a['s3_key'])['Body']
                    try:
                        with path.open('wb') as output:
                            while True:
                                chunk=stream.read(1024*1024)
                                if not chunk: break
                                total+=len(chunk)
                                if total>2*1024**3 or path.stat().st_size+len(chunk)>500*1024**2: raise ValueError('Project media exceeds the 2 GB render budget or a source exceeds 500 MB.')
                                output.write(chunk)
                                if time.monotonic()-last_heartbeat>=1: progress(0);last_heartbeat=time.monotonic()
                    finally: stream.close()
                    paths[identity]=path
            kind=next(iter(media.values()))['media_type'] if normalizing else 'video'
            output=Path(folder)/('export.mp4' if kind=='video' else 'editing-copy.m4a')
            if normalizing: normalize(next(iter(paths.values())),output,kind,progress)
            else: render(doc,paths,output,job['resolution'],progress)
            info=probe(output);video=next((s for s in info['streams'] if s['codec_type']=='video'),{})
            if (kind=='video' and video.get('codec_name')!='h264') or output.stat().st_size>500*1024**2: raise ValueError('Export is invalid or exceeds 500 MB. Choose a shorter project.')
            progress(99);identity=s.uid();key=v.private_key(job['tenant_slug'],tenant_root(job['tenant_slug'])+f"assets/marketing/private/video/exports/{identity}.{'mp4' if kind=='video' else 'm4a'}")
            with output.open('rb') as stream: assets.s3().put_object(Bucket=assets.bucket(),Key=key,Body=stream,ContentType='video/mp4' if kind=='video' else 'audio/mp4',ServerSideEncryption='AES256')
            with db_conn() as conn,conn.transaction(),conn.cursor(row_factory=dict_row) as cur:
                lock(cur,ctx);current=s.one(cur,'SELECT cancel_requested,status FROM video_render_jobs WHERE id=%s AND tenant_id=%s FOR UPDATE',(job['id'],job['tenant_id']))
                if current['cancel_requested'] or current['status']!='running': raise Cancelled()
                export_name=(next(iter(media.values()))['filename']+' · Editing copy') if normalizing else job['project_name']+' · Export'
                asset=s.one(cur,'''INSERT INTO social_media_assets(id,tenant_id,source_type,s3_bucket,s3_key,public_url,filename,display_name,mime_type,media_type,width,height,duration_seconds,filesize_bytes,created_by) VALUES(%s,%s,'generated_derivative',%s,%s,'',%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING *''',(identity,job['tenant_id'],assets.bucket(),key,f"{identity}.{'mp4' if kind=='video' else 'm4a'}",export_name,'video/mp4' if kind=='video' else 'audio/mp4',kind,video.get('width'),video.get('height'),float(info['format']['duration']),output.stat().st_size,job['created_by']))
                cur.execute('INSERT INTO video_asset_metadata(asset_id,tenant_id,project_id,codec_info,working_project_id,is_finished_video) VALUES(%s,%s,%s,%s,%s,%s)',(identity,job['tenant_id'],None if normalizing else job['project_id'],Jsonb(info),job['project_id'] if normalizing else None,not normalizing))
                cur.execute("UPDATE video_render_jobs SET status='completed',progress=100,output_asset_id=%s,completed_at=now(),heartbeat_at=now() WHERE id=%s AND tenant_id=%s",(identity,job['id'],job['tenant_id']))
                s.audit(cur,ctx,job['project_id'] or job['normalize_asset_id'],'video_export_completed',{'asset_id':str(identity),'revision':job['project_revision']})
            key=None
    except Exception as error:
        if key:
            try: assets.s3().delete_object(Bucket=assets.bucket(),Key=key)
            except Exception: log.exception('Could not clean up failed video export object')
        message=str(error) if isinstance(error,ValueError) else 'Export failed. Check source media and the render worker, then retry.'
        with db_conn() as conn,conn.cursor(row_factory=dict_row) as cur: cur.execute('UPDATE video_render_jobs SET status=%s,error=%s,completed_at=now(),heartbeat_at=now() WHERE id=%s AND tenant_id=%s',('cancelled' if isinstance(error,Cancelled) else 'failed',None if isinstance(error,Cancelled) else message,job['id'],job['tenant_id']))
        if not isinstance(error,Cancelled): log.exception('Video render %s failed',job['id'])
    return True

def main():
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).resolve().parents[2]/'.env');logging.basicConfig(level=logging.INFO)
    while True:
        try: worked=tick()
        except Exception: log.exception('Video worker tick failed');worked=False
        if not worked: time.sleep(3)
if __name__=='__main__': main()
