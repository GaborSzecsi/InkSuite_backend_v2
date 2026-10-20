"""Review and remove an asset together with its local posts and editable projects."""
import hashlib
import json
from fastapi import HTTPException
from psycopg.types.json import Jsonb
from . import service as s, storage, assets


def preview(cur, ctx, asset_id):
    storage.lock(cur, ctx)
    tenant = ctx['tenant']['id']
    asset = s.owned(cur, 'social_media_assets', asset_id, tenant, True)
    if asset['source_type'] == 'title_public_asset':
        raise HTTPException(403, 'Manage title files from the title project.')
    if asset['s3_bucket'] != assets.bucket():
        raise HTTPException(409, 'This asset belongs to a different storage bucket.')
    needle = Jsonb([{'asset_id': str(asset_id)}])
    posts = s.rows(cur, '''SELECT p.id,p.title,p.status,p.updated_at FROM social_posts p
        WHERE p.tenant_id=%s AND EXISTS (SELECT 1 FROM social_post_assets pa
        WHERE pa.post_id=p.id AND pa.tenant_id=p.tenant_id AND pa.asset_id=%s)
        ORDER BY p.id FOR UPDATE''', (tenant, asset_id))
    post_ids = [p['id'] for p in posts]
    targets = s.rows(cur, '''SELECT id,provider_status FROM social_post_targets WHERE tenant_id=%s
        AND post_id=ANY(%s::uuid[]) ORDER BY id FOR UPDATE''', (tenant, post_ids))
    publish_jobs = s.rows(cur, '''SELECT id,status FROM social_publish_jobs WHERE tenant_id=%s
        AND post_target_id=ANY(%s::uuid[]) ORDER BY id FOR UPDATE''', (tenant, [t['id'] for t in targets]))
    projects = s.rows(cur, '''SELECT id,name,revision FROM video_projects WHERE tenant_id=%s
        AND (source_asset_id=%s OR document->'clips' @> %s::jsonb OR document->'canvas'->>'background_asset_id'=%s)
        ORDER BY id FOR UPDATE''', (tenant, asset_id, needle, str(asset_id)))
    jobs = s.rows(cur, '''SELECT id,status FROM video_render_jobs WHERE tenant_id=%s AND
        (project_id=ANY(%s::uuid[]) OR normalize_asset_id=%s OR document->'clips' @> %s::jsonb
        OR document->'canvas'->>'background_asset_id'=%s) ORDER BY id FOR UPDATE''',
        (tenant, [p['id'] for p in projects], asset_id, needle, str(asset_id)))
    blocked = None
    if any(t['provider_status'] in ('processing', 'submitted') for t in targets) or any(j['status'] in ('processing', 'submitted') for j in publish_jobs):
        blocked = 'A linked post is being published. Wait for delivery to finish before deleting its media.'
    if any(j['status'] == 'running' for j in jobs):
        blocked = 'A linked video is being exported. Finish or cancel that export before deleting its media.'
    plan = {'asset_id': str(asset_id), 'name': asset.get('display_name') or asset['filename'],
            'posts': [{**p, 'id': str(p['id']), 'updated_at': str(p['updated_at']),
                       'action': 'keep_history' if p['status'] in ('published','partially_published') else 'delete'} for p in posts],
            'projects': [{**p, 'id': str(p['id'])} for p in projects],
            'render_jobs': [{'id': str(j['id']), 'status': j['status']} for j in jobs],
            'blocked': blocked}
    plan['confirmation'] = hashlib.sha256(json.dumps(plan, sort_keys=True).encode()).hexdigest()
    return plan


def remove(cur, ctx, asset_id, confirmation=None):
    plan = preview(cur, ctx, asset_id)
    if plan['blocked']:
        raise HTTPException(409, plan['blocked'])
    if confirmation != plan['confirmation'] and (confirmation or plan['posts'] or plan['projects'] or plan['render_jobs']):
        raise HTTPException(409, 'Review the linked posts and projects before deleting this asset. The deletion list may have changed.')
    tenant = ctx['tenant']['id']
    posts = [p['id'] for p in plan['posts'] if p['action']=='delete']
    history = [p['id'] for p in plan['posts'] if p['action']=='keep_history']
    projects = [p['id'] for p in plan['projects']]
    cur.execute('''DELETE FROM social_publish_jobs WHERE tenant_id=%s AND post_target_id IN
        (SELECT id FROM social_post_targets WHERE tenant_id=%s AND post_id=ANY(%s::uuid[]))''', (tenant, tenant, posts))
    cur.execute('DELETE FROM social_posts WHERE tenant_id=%s AND id=ANY(%s::uuid[])', (tenant, posts))
    # Keep publication records and provider links; stop any remaining partial deliveries.
    cur.execute('''UPDATE social_publish_jobs SET status='cancelled',completed_at=now(),updated_at=now()
        WHERE tenant_id=%s AND status IN ('pending','retrying') AND post_target_id IN
        (SELECT id FROM social_post_targets WHERE tenant_id=%s AND post_id=ANY(%s::uuid[]))''',(tenant,tenant,history))
    cur.execute('''UPDATE social_post_targets SET provider_status=CASE WHEN provider_status IN ('pending','scheduled','retrying','failed')
        THEN 'cancelled' ELSE provider_status END,provider_payload=provider_payload-'asset_ids'-'cover_asset_id',updated_at=now()
        WHERE tenant_id=%s AND post_id=ANY(%s::uuid[])''',(tenant,history))
    cur.execute('DELETE FROM social_post_assets WHERE tenant_id=%s AND asset_id=%s AND post_id=ANY(%s::uuid[])',(tenant,asset_id,history))
    cur.execute('DELETE FROM video_render_jobs WHERE tenant_id=%s AND id=ANY(%s::uuid[])',
                (tenant, [j['id'] for j in plan['render_jobs']]))
    cur.execute('DELETE FROM video_projects WHERE tenant_id=%s AND id=ANY(%s::uuid[])', (tenant, projects))
    asset = s.owned(cur, 'social_media_assets', asset_id, tenant)
    storage.delete_key(cur, ctx, asset['s3_key'])
    s.audit(cur, ctx, asset_id, 'asset_and_local_content_deleted', {'posts': posts, 'history_kept':history, 'projects': projects})
    return {'deleted': str(asset_id), 'posts_deleted': len(posts), 'history_kept':len(history), 'projects_deleted': len(projects)}
