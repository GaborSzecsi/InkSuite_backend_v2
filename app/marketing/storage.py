"""Tenant-scoped Marketing storage accounting and deletion."""
from fastapi import HTTPException
from psycopg.types.json import Jsonb
from . import service as s
from .domain import tenant_root, public_key



def lock(cur, ctx):
    # Serialize uploads, generated outputs, deletions and draft attachment changes.
    cur.execute('SELECT id FROM tenants WHERE id=%s FOR UPDATE', (ctx['tenant']['id'],))


def inventory(ctx):
    from .assets import s3, bucket
    prefixes=[tenant_root(ctx['tenant']['slug'])+'assets/marketing/'+folder for folder in ('public/','private/video/')]
    return [item for prefix in prefixes for page in s3().get_paginator('list_objects_v2').paginate(Bucket=bucket(),Prefix=prefix)
            for item in page.get('Contents',[]) if item['Key'].startswith(prefix) and not item['Key'].endswith('/')]


def usage(ctx):
    items=inventory(ctx);used=sum(int(i['Size']) for i in items)
    return {'used_bytes':used,'limit_bytes':None,'available_bytes':None,'file_count':len(items)}


def require_space(cur,ctx,size):
    lock(cur,ctx)


def delete_key(cur,ctx,key):
    from .assets import s3,bucket
    lock(cur,ctx)
    if '/assets/marketing/private/video/' in key:
        from app.video.service import private_key
        private_key(ctx['tenant']['slug'],key)
    else: public_key(ctx['tenant']['slug'],key)
    asset=s.one(cur,'SELECT * FROM social_media_assets WHERE tenant_id=%s AND s3_bucket=%s AND s3_key=%s FOR UPDATE',
        (ctx['tenant']['id'],bucket(),key))
    if asset:
        if asset['source_type']=='title_public_asset': raise HTTPException(403,'Title Assets are protected.')
        if s.one(cur,"""SELECT pa.id FROM social_post_assets pa JOIN social_posts p
            ON p.id=pa.post_id AND p.tenant_id=pa.tenant_id WHERE pa.asset_id=%s AND pa.tenant_id=%s
            AND p.status NOT IN ('published','cancelled') LIMIT 1""",(asset['id'],ctx['tenant']['id'])):
            raise HTTPException(409,'This file is used by a draft, scheduled or unfinished post. Remove it from the draft or cancel the post first.')
        if s.one(cur,"SELECT id FROM video_projects WHERE tenant_id=%s AND (source_asset_id=%s OR document->'clips' @> %s::jsonb OR document->'canvas'->>'background_asset_id'=%s) UNION ALL SELECT id FROM video_render_jobs WHERE tenant_id=%s AND status IN ('queued','running') AND (normalize_asset_id=%s OR document->'clips' @> %s::jsonb OR document->'canvas'->>'background_asset_id'=%s) LIMIT 1",(ctx['tenant']['id'],asset['id'],Jsonb([{'asset_id':str(asset['id'])}]),str(asset['id']),ctx['tenant']['id'],asset['id'],Jsonb([{'asset_id':str(asset['id'])}]),str(asset['id']))):
            raise HTTPException(409,'This file is used by an editable video project. Remove it from the project or delete the project first.')
    # Remove only this exact object. Never delete the tenant root or title folder.
    s3().delete_object(Bucket=bucket(),Key=key)
    if asset:
        cur.execute('DELETE FROM social_post_assets WHERE asset_id=%s AND tenant_id=%s',(asset['id'],ctx['tenant']['id']))
        cur.execute('UPDATE social_media_assets SET source_asset_id=NULL WHERE source_asset_id=%s AND tenant_id=%s',(asset['id'],ctx['tenant']['id']))
        cur.execute('DELETE FROM social_media_assets WHERE id=%s AND tenant_id=%s',(asset['id'],ctx['tenant']['id']))
        s.audit(cur,ctx,asset['id'],'asset_deleted')
    return {'ok':True}
