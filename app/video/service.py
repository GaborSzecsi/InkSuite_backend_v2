"""Tenant-owned project operations and asset adaptation; reusable by web and future mobile clients."""
import json
from uuid import UUID
from fastapi import HTTPException
from psycopg.types.json import Jsonb
from app.marketing import service as s,assets
from app.marketing.domain import tenant_root,public_key
from .schema import Document

def project(cur,identity,tenant,lock=False):
    result=s.one(cur,'SELECT * FROM video_projects WHERE id=%s AND tenant_id=%s'+(' FOR UPDATE' if lock else ''),(identity,tenant))
    if not result: raise HTTPException(404,'Video project not found.')
    return result

def private_key(slug,key):
    prefix=tenant_root(slug)+'assets/marketing/private/video/'
    if not key.startswith(prefix) or '\\' in key or any(p in ('','..','.') for p in key.split('/')): raise ValueError('Invalid private media path.')
    return key

def validate_asset(cur,ctx,identity):
    a=s.owned(cur,'social_media_assets',identity,ctx['tenant']['id'])
    if a['s3_bucket']!=assets.bucket(): raise HTTPException(422,'Media belongs to a different storage bucket.')
    if '/assets/marketing/private/video/' in a['s3_key']: private_key(ctx['tenant']['slug'],a['s3_key'])
    elif a['source_type']=='title_public_asset':
        w=s.owned(cur,'works',a['work_id'],ctx['tenant']['id']);public_key(ctx['tenant']['slug'],a['s3_key'],title_uid=str(w.get('uid') or w['id']))
    else: public_key(ctx['tenant']['slug'],a['s3_key'])
    return a

def hydrate(a):
    a=dict(a)
    if '/assets/marketing/private/video/' in a['s3_key']:
        a['public_url']=assets.s3().generate_presigned_url('get_object',Params={'Bucket':a['s3_bucket'],'Key':a['s3_key']},ExpiresIn=86400)
    return a

def validate_document(cur,ctx,doc:Document):
    tenant=ctx['tenant']['id'];media={}
    for c in doc.clips:
        if c.asset_id and str(c.asset_id) not in media: media[str(c.asset_id)]=validate_asset(cur,ctx,c.asset_id)
        if c.asset_id:
            a=media[str(c.asset_id)]
            if a['media_type']!=c.kind: raise HTTPException(422,'A clip source has an incompatible media type.')
            if c.kind in ('video','audio') and not c.loop and a.get('duration_seconds') and c.trim+c.duration*c.speed>float(a['duration_seconds'])+.1: raise HTTPException(422,'Clip timing exceeds its source. Trim it or enable looping.')
    if doc.canvas.background_asset_id:
        bg=validate_asset(cur,ctx,doc.canvas.background_asset_id)
        if bg['media_type']!='image': raise HTTPException(422,'Choose a photograph for the canvas background.')
    for work in doc.work_ids: s.owned(cur,'works',work,tenant)
    if doc.campaign_id: s.owned(cur,'marketing_campaigns',doc.campaign_id,tenant)
    # Keep memory/decoder use bounded even when many clips overlap.
    for point in sorted({c.start for c in doc.clips}):
        active=[c for c in doc.clips if c.start<=point<c.start+c.duration]
        if len(active)>16: raise HTTPException(422,'Use at most sixteen overlapping clips.')
    return media

def save(cur,ctx,body,identity=None):
    tenant=ctx['tenant']['id'];validate_document(cur,ctx,body.document)
    if body.source_asset_id: validate_asset(cur,ctx,body.source_asset_id)
    if identity:
        old=project(cur,identity,tenant,True)
        if old['revision']!=body.revision: raise HTTPException(409,'This project changed in another session. Reopen it before saving your changes.')
        row=s.one(cur,'UPDATE video_projects SET name=%s,document=%s,revision=revision+1,updated_at=now() WHERE id=%s AND tenant_id=%s RETURNING *',(body.name,Jsonb(body.document.model_dump(mode='json')),identity,tenant))
    else:
        row=s.one(cur,'INSERT INTO video_projects(id,tenant_id,name,document,source_asset_id,created_by) VALUES(%s,%s,%s,%s,%s,%s) RETURNING *',(s.uid(),tenant,body.name,Jsonb(body.document.model_dump(mode='json')),body.source_asset_id,ctx['user']['id']))
    s.audit(cur,ctx,row['id'],'video_project_saved',{'revision':row['revision']})
    return row
