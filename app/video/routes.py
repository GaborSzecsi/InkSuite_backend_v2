from .scratch import scratch
from pathlib import Path
from uuid import UUID
import json,tempfile
from fastapi import APIRouter,Depends,HTTPException,UploadFile,File,Form
from fastapi.responses import FileResponse
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb
from app.core.db import db_conn
from app.marketing import service as s,assets
from app.marketing.storage import lock
from app.marketing.domain import tenant_root
from . import service as v
from .schema import ProjectSave,Document,RenderRequest,MusicLicense
from .render import probe,FONT
from .privacy import ensure_private_storage

router=APIRouter(prefix='/tenants/{tenant_slug}/marketing/video',tags=['Video editor'])

@router.get('/font')
def font(ctx=Depends(s.access)):
    return FileResponse(FONT,media_type='font/ttf')

@router.get('/projects')
def projects(asset_id:UUID|None=None,ctx=Depends(s.access)):
    with db_conn() as conn,conn.cursor(row_factory=dict_row) as cur:
        return s.rows(cur,'''SELECT p.id,p.name,p.revision,p.source_asset_id,p.created_at,p.updated_at FROM video_projects p WHERE p.tenant_id=%s
        AND (%s::uuid IS NULL OR p.source_asset_id=%s OR EXISTS(SELECT 1 FROM video_asset_metadata m WHERE m.project_id=p.id AND m.tenant_id=p.tenant_id AND m.asset_id=%s)) ORDER BY p.updated_at DESC LIMIT 100''',(ctx['tenant']['id'],asset_id,asset_id,asset_id))

@router.post('/projects')
def create(body:ProjectSave,ctx=Depends(s.access)):
    with db_conn() as conn,conn.transaction(),conn.cursor(row_factory=dict_row) as cur: lock(cur,ctx);return v.save(cur,ctx,body)

@router.get('/projects/{identity}')
def get(identity:UUID,ctx=Depends(s.access)):
    with db_conn() as conn,conn.cursor(row_factory=dict_row) as cur: return v.project(cur,identity,ctx['tenant']['id'])

@router.put('/projects/{identity}')
def update(identity:UUID,body:ProjectSave,ctx=Depends(s.access)):
    with db_conn() as conn,conn.transaction(),conn.cursor(row_factory=dict_row) as cur: lock(cur,ctx);return v.save(cur,ctx,body,identity)

@router.post('/projects/{identity}/duplicate')
def duplicate(identity:UUID,ctx=Depends(s.access)):
    with db_conn() as conn,conn.transaction(),conn.cursor(row_factory=dict_row) as cur:
        lock(cur,ctx);old=v.project(cur,identity,ctx['tenant']['id'])
        return v.save(cur,ctx,ProjectSave(name=(old['name']+' copy')[:250],document=Document.model_validate(old['document'])))

@router.delete('/projects/{identity}')
def delete(identity:UUID,ctx=Depends(s.access)):
    with db_conn() as conn,conn.transaction(),conn.cursor(row_factory=dict_row) as cur:
        lock(cur,ctx);v.project(cur,identity,ctx['tenant']['id'],True)
        if s.one(cur,"SELECT id FROM video_render_jobs WHERE project_id=%s AND tenant_id=%s AND status IN ('queued','running')",(identity,ctx['tenant']['id'])): raise HTTPException(409,'Cancel or finish exports before deleting this project.')
        cur.execute('DELETE FROM video_render_jobs WHERE project_id=%s AND tenant_id=%s',(identity,ctx['tenant']['id']))
        cur.execute('DELETE FROM video_projects WHERE id=%s AND tenant_id=%s',(identity,ctx['tenant']['id']))
        s.audit(cur,ctx,identity,'video_project_deleted');return {'deleted':str(identity)}

@router.get('/media')
def media(ctx=Depends(s.access)):
    with db_conn() as conn,conn.cursor(row_factory=dict_row) as cur:
        items=s.rows(cur,'''SELECT a.*,m.music_license,m.project_id AS video_project_id FROM social_media_assets a LEFT JOIN video_asset_metadata m ON m.asset_id=a.id AND m.tenant_id=a.tenant_id WHERE a.tenant_id=%s AND a.media_type IN ('image','video','audio') ORDER BY a.created_at DESC LIMIT 500''',(ctx['tenant']['id'],))
        return [v.hydrate(a) for a in items]

@router.post('/media')
async def upload(file:UploadFile=File(...),ctx=Depends(s.access)):
    try: ensure_private_storage(ctx)
    except ValueError as error: raise HTTPException(503,str(error)) from None
    data=await file.read(500*1024*1024+1)
    if not data or len(data)>500*1024*1024: raise HTTPException(422,'Choose a file up to 500 MB.')
    filename=Path((file.filename or 'media').replace('\\','/')).name;info={}
    try: mime,kind,w,h,duration=assets.inspect_media(data,filename)
    except HTTPException:
        with scratch() as folder:
            path=Path(folder)/'source.media';path.write_bytes(data)
            try: info=probe(path)
            except Exception: raise HTTPException(422,'Unsupported or damaged media. Use JPEG, PNG, WebP, MP4, MOV, WebM, MP3, WAV, AAC or M4A.') from None
        video=next((x for x in info.get('streams',[]) if x['codec_type']=='video' and not x.get('disposition',{}).get('attached_pic')),None)
        audio=next((x for x in info.get('streams',[]) if x['codec_type']=='audio'),None)
        duration=float(info.get('format',{}).get('duration') or 0)
        if not 0<duration<=7200: raise HTTPException(422,'Use media with a duration of up to two hours.')
        if video:
            formats=info.get('format',{}).get('format_name','')
            if not any(x in formats for x in ('mov','mp4','webm','matroska')): raise HTTPException(422,'Unsupported video container.')
            mime,kind,w,h='video/mp4' if 'mov' in formats else 'video/webm','video',video['width'],video['height']
        elif audio and audio.get('codec_name') in ('mp3','aac','pcm_s16le','pcm_s24le','pcm_f32le','vorbis','opus'):
            mime,kind,w,h='audio/'+('mpeg' if audio['codec_name']=='mp3' else 'wav' if audio['codec_name'].startswith('pcm_') else 'mp4' if audio['codec_name']=='aac' else 'ogg'),'audio',None,None
        else: raise HTTPException(422,'No compatible audio or video stream found.')
    with db_conn() as conn,conn.transaction(),conn.cursor(row_factory=dict_row) as cur:
        lock(cur,ctx);identity=s.uid();key=v.private_key(ctx['tenant']['slug'],tenant_root(ctx['tenant']['slug'])+f'assets/marketing/private/video/originals/{identity}/{filename}')
        assets.s3().put_object(Bucket=assets.bucket(),Key=key,Body=data,ContentType=mime,ServerSideEncryption='AES256')
        try:
            a=s.one(cur,'''INSERT INTO social_media_assets(id,tenant_id,source_type,s3_bucket,s3_key,public_url,filename,mime_type,media_type,width,height,duration_seconds,filesize_bytes,created_by) VALUES(%s,%s,'publisher_asset',%s,%s,'',%s,%s,%s,%s,%s,%s,%s,%s) RETURNING *''',(identity,ctx['tenant']['id'],assets.bucket(),key,filename,mime,kind,w,h,duration,len(data),ctx['user']['id']))
            cur.execute('INSERT INTO video_asset_metadata(asset_id,tenant_id,codec_info) VALUES(%s,%s,%s)',(identity,ctx['tenant']['id'],Jsonb(info)))
            return v.hydrate(a)
        except Exception: assets.s3().delete_object(Bucket=assets.bucket(),Key=key);raise

@router.put('/media/{identity}/license')
def license(identity:UUID,body:MusicLicense,ctx=Depends(s.admin)):
    with db_conn() as conn,conn.transaction(),conn.cursor(row_factory=dict_row) as cur:
        a=v.validate_asset(cur,ctx,identity)
        if a['media_type']!='audio': raise HTTPException(422,'Select an audio asset.')
        cur.execute('INSERT INTO video_asset_metadata(asset_id,tenant_id,music_license) VALUES(%s,%s,%s) ON CONFLICT(asset_id) DO UPDATE SET music_license=EXCLUDED.music_license',(identity,ctx['tenant']['id'],Jsonb(body.model_dump())))
        return body

@router.post('/projects/{identity}/renders')
def export(identity:UUID,body:RenderRequest,ctx=Depends(s.access)):
    with db_conn() as conn,conn.transaction(),conn.cursor(row_factory=dict_row) as cur:
        lock(cur,ctx);p=v.project(cur,identity,ctx['tenant']['id'],True)
        if p['revision']!=body.revision: raise HTTPException(409,'Save the current revision before exporting.')
        doc=Document.model_validate(p['document']);v.validate_document(cur,ctx,doc)
        if not doc.clips: raise HTTPException(422,'Add a clip before exporting.')
        count=s.one(cur,"SELECT count(*) AS n FROM video_render_jobs WHERE tenant_id=%s AND status IN ('queued','running')",(ctx['tenant']['id'],))['n']
        if count>=2: raise HTTPException(409,'Two exports are already queued or running for this company.')
        return s.one(cur,'INSERT INTO video_render_jobs(id,tenant_id,project_id,project_revision,document,resolution,created_by) VALUES(%s,%s,%s,%s,%s,%s,%s) RETURNING *',(s.uid(),ctx['tenant']['id'],identity,p['revision'],Jsonb(p['document']),body.resolution,ctx['user']['id']))

@router.get('/renders/{identity}')
def status(identity:UUID,ctx=Depends(s.access)):
    with db_conn() as conn,conn.cursor(row_factory=dict_row) as cur:
        job=s.one(cur,'SELECT id,job_kind,project_id,project_revision,status,progress,error,output_asset_id,created_at,completed_at FROM video_render_jobs WHERE id=%s AND tenant_id=%s',(identity,ctx['tenant']['id']))
        if not job: raise HTTPException(404,'Export not found.')
        if job['output_asset_id']: job['asset']=v.hydrate(s.owned(cur,'social_media_assets',job['output_asset_id'],ctx['tenant']['id']))
        return job

@router.delete('/renders/{identity}')
def cancel(identity:UUID,ctx=Depends(s.access)):
    with db_conn() as conn,conn.transaction(),conn.cursor(row_factory=dict_row) as cur:
        job=s.one(cur,"UPDATE video_render_jobs SET cancel_requested=true,status=CASE WHEN status='queued' THEN 'cancelled' ELSE status END WHERE id=%s AND tenant_id=%s RETURNING id,status",(identity,ctx['tenant']['id']))
        if not job: raise HTTPException(404,'Export not found.')
        return job

@router.get('/projects/{identity}/renders')
def recent_renders(identity:UUID,ctx=Depends(s.access)):
    with db_conn() as conn,conn.cursor(row_factory=dict_row) as cur:
        v.project(cur,identity,ctx['tenant']['id'])
        jobs=s.rows(cur,'SELECT id,status,progress,error,output_asset_id FROM video_render_jobs WHERE project_id=%s AND tenant_id=%s ORDER BY created_at DESC LIMIT 10',(identity,ctx['tenant']['id']))
        for job in jobs:
            if job['output_asset_id']: job['asset']=v.hydrate(s.owned(cur,'social_media_assets',job['output_asset_id'],ctx['tenant']['id']))
        return jobs

@router.post('/media/{identity}/normalize')
def normalize(identity:UUID,ctx=Depends(s.access)):
    with db_conn() as conn,conn.transaction(),conn.cursor(row_factory=dict_row) as cur:
        lock(cur,ctx);a=v.validate_asset(cur,ctx,identity)
        if a['media_type'] not in ('video','audio'): raise HTTPException(422,'Select video or audio media.')
        if float(a.get('duration_seconds') or 0)>300: raise HTTPException(422,'Editing copies are limited to five minutes. Trim this longer source in a project before exporting it.')
        if s.one(cur,"SELECT count(*) AS n FROM video_render_jobs WHERE tenant_id=%s AND status IN ('queued','running')",(ctx['tenant']['id'],))['n']>=2: raise HTTPException(409,'Two exports are already queued or running.')
        return s.one(cur,"INSERT INTO video_render_jobs(id,tenant_id,project_revision,document,resolution,created_by,normalize_asset_id,job_kind) VALUES(%s,%s,0,'{}',1080,%s,%s,'normalize') RETURNING *",(s.uid(),ctx['tenant']['id'],ctx['user']['id'],identity))
