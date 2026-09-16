"""Tenant-scoped Marketing storage accounting and deletion."""
from fastapi import HTTPException
from . import service as s
from .domain import tenant_root, public_key

LIMIT_BYTES = 1_000_000_000


def lock(cur, ctx):
    # Serialize uploads, generated outputs, deletions and draft attachment changes.
    cur.execute('SELECT id FROM tenants WHERE id=%s FOR UPDATE', (ctx['tenant']['id'],))


def inventory(ctx):
    from .assets import s3, bucket
    prefix=tenant_root(ctx['tenant']['slug'])+'assets/marketing/public/'
    return [item for page in s3().get_paginator('list_objects_v2').paginate(Bucket=bucket(),Prefix=prefix)
            for item in page.get('Contents',[]) if not item['Key'].endswith('/')]


def usage(ctx):
    items=inventory(ctx);used=sum(int(i['Size']) for i in items)
    return {'used_bytes':used,'limit_bytes':LIMIT_BYTES,'available_bytes':max(0,LIMIT_BYTES-used),'file_count':len(items)}


def require_space(cur,ctx,size):
    lock(cur,ctx)
    if usage(ctx)['used_bytes']+size>LIMIT_BYTES:
        raise HTTPException(413,'Marketing storage is limited to 1 GB per company. Delete older Marketing assets before uploading or generating more media.')


def delete_key(cur,ctx,key):
    from .assets import s3,bucket
    lock(cur,ctx)
    public_key(ctx['tenant']['slug'],key)
    asset=s.one(cur,'SELECT * FROM social_media_assets WHERE tenant_id=%s AND s3_bucket=%s AND s3_key=%s FOR UPDATE',
        (ctx['tenant']['id'],bucket(),key))
    if asset:
        if asset['source_type']=='title_public_asset': raise HTTPException(403,'Title Assets are protected.')
        if s.one(cur,"""SELECT pa.id FROM social_post_assets pa JOIN social_posts p
            ON p.id=pa.post_id AND p.tenant_id=pa.tenant_id WHERE pa.asset_id=%s AND pa.tenant_id=%s
            AND p.status NOT IN ('published','cancelled') LIMIT 1""",(asset['id'],ctx['tenant']['id'])):
            raise HTTPException(409,'This file is used by a draft, scheduled or unfinished post. Remove it from the draft or cancel the post first.')
    # Remove only this exact object. Never delete the tenant root or title folder.
    s3().delete_object(Bucket=bucket(),Key=key)
    if asset:
        cur.execute('DELETE FROM social_post_assets WHERE asset_id=%s AND tenant_id=%s',(asset['id'],ctx['tenant']['id']))
        cur.execute('UPDATE social_media_assets SET source_asset_id=NULL WHERE source_asset_id=%s AND tenant_id=%s',(asset['id'],ctx['tenant']['id']))
        cur.execute('DELETE FROM social_media_assets WHERE id=%s AND tenant_id=%s',(asset['id'],ctx['tenant']['id']))
        s.audit(cur,ctx,asset['id'],'asset_deleted')
    return {'ok':True}
