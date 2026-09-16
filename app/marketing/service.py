import uuid
from fastapi import Depends, HTTPException
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb
from app.core.db import db_conn
from app.tenants.dependencies import require_tenant_access

def uid(): return uuid.uuid4()
def one(cur, query, args=()):
    cur.execute(query, args)
    return cur.fetchone()
def rows(cur, query, args=()):
    cur.execute(query, args)
    return cur.fetchall()

async def access(ctx=Depends(require_tenant_access)):
    if ctx['membership_role'] in ('superadmin', 'tenant_admin'): return ctx
    with db_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
        m = one(cur, 'SELECT module_permissions FROM memberships WHERE tenant_id=%s AND user_id=%s', (ctx['tenant']['id'],ctx['user']['id']))
    if not m or not (m['module_permissions'] or {}).get('project_management'):
        raise HTTPException(403, 'Project Management permission is required.')
    return ctx

async def admin(ctx=Depends(access)):
    if ctx['membership_role'] not in ('superadmin','tenant_admin'):
        raise HTTPException(403, 'A tenant administrator must manage social accounts and publish posts.')
    return ctx

TABLES = {'marketing_campaigns','social_posts','social_accounts','social_media_assets','social_post_targets','works'}
def owned(cur, table, entity, tenant, lock=False):
    if table not in TABLES: raise ValueError('Invalid resource type')
    row = one(cur, f'SELECT * FROM {table} WHERE id=%s AND tenant_id=%s' + (' FOR UPDATE' if lock else ''), (entity,tenant))
    if not row: raise HTTPException(404, 'Resource not found.')
    return row

def audit(cur, ctx, entity, action, detail=None):
    cur.execute('INSERT INTO marketing_audit(id,tenant_id,actor_id,entity_id,action,detail) VALUES(%s,%s,%s,%s,%s,%s)', (uid(),ctx['tenant']['id'],ctx['user']['id'],entity,action,Jsonb(detail or {})))

def campaign_view(cur, campaign):
    campaign['works'] = rows(cur, '''SELECT w.id,w.uid,w.title FROM marketing_campaign_works cw JOIN works w ON w.id=cw.work_id AND w.tenant_id=cw.tenant_id WHERE cw.campaign_id=%s AND cw.tenant_id=%s ORDER BY w.title''', (campaign['id'],campaign['tenant_id']))
    campaign['counts'] = {r['status']: r['count'] for r in rows(cur,'SELECT status,count(*) FROM social_posts WHERE tenant_id=%s AND campaign_id=%s GROUP BY status',(campaign['tenant_id'],campaign['id']))}
    return campaign

def post_view(cur, post):
    args=(post['id'],post['tenant_id'])
    post['targets']=rows(cur, '''SELECT t.id,t.social_account_id,t.provider,t.provider_payload,t.provider_status,t.provider_post_url,t.last_error_code,t.last_error_message,t.attempt_count,a.display_name FROM social_post_targets t JOIN social_accounts a ON a.id=t.social_account_id AND a.tenant_id=t.tenant_id WHERE t.post_id=%s AND t.tenant_id=%s ORDER BY t.created_at''',args)
    post['works']=rows(cur,'''SELECT w.id,w.uid,w.title FROM social_post_works pw JOIN works w ON w.id=pw.work_id AND w.tenant_id=pw.tenant_id WHERE pw.post_id=%s AND pw.tenant_id=%s''',args)
    post['assets']=rows(cur,'''SELECT a.*,pa.role FROM social_post_assets pa JOIN social_media_assets a ON a.id=pa.asset_id AND a.tenant_id=pa.tenant_id WHERE pa.post_id=%s AND pa.tenant_id=%s ORDER BY pa.sort_order''',args)
    return post

def save_post(cur, body, ctx, post_id=None):
    from .storage import lock
    lock(cur,ctx)
    tenant=ctx['tenant']['id']; user=ctx['user']['id']
    if post_id:
        old=owned(cur,'social_posts',post_id,tenant,True)
        if old['status']!='draft': raise HTTPException(409,'Only drafts can be edited. Cancel a scheduled post before creating a revised draft.')
    else: post_id=uid()
    if body.campaign_id: owned(cur,'marketing_campaigns',body.campaign_id,tenant)
    for work in set(body.work_ids): owned(cur,'works',work,tenant)
    covers=set()
    for target in body.targets:
        value=target.provider_payload.get('cover_asset_id')
        if value:
            try: covers.add(uuid.UUID(str(value)))
            except ValueError: raise HTTPException(422,'Invalid Reel cover asset.')
    for asset in set(body.asset_ids) | covers:
        a=owned(cur,'social_media_assets',asset,tenant)
        if asset in covers and a['media_type']!='image': raise HTTPException(422,'Choose an image for the Reel cover.')
        if a['campaign_id'] and a['campaign_id']!=body.campaign_id: raise HTTPException(422,'Campaign media belongs to a different campaign.')
        if a['work_id'] and a['work_id'] not in body.work_ids: raise HTTPException(422,'Add the media’s title to featured books.')
    accounts={t.social_account_id:owned(cur,'social_accounts',t.social_account_id,tenant) for t in body.targets}
    if len(accounts)!=len(body.targets): raise HTTPException(422,'A social destination can only be selected once.')
    cur.execute('''INSERT INTO social_posts(id,tenant_id,campaign_id,title,content_text,timezone,created_by,updated_by) VALUES(%s,%s,%s,%s,%s,%s,%s,%s)
      ON CONFLICT(id) DO UPDATE SET campaign_id=EXCLUDED.campaign_id,title=EXCLUDED.title,content_text=EXCLUDED.content_text,timezone=EXCLUDED.timezone,updated_by=EXCLUDED.updated_by,updated_at=now()''',(post_id,tenant,body.campaign_id,body.title,body.content_text,body.timezone,user,user))
    for table in ('social_post_works','social_post_assets','social_post_targets'):
        cur.execute(f'DELETE FROM {table} WHERE post_id=%s AND tenant_id=%s',(post_id,tenant))
    for work in set(body.work_ids): cur.execute('INSERT INTO social_post_works(id,tenant_id,post_id,work_id) VALUES(%s,%s,%s,%s)',(uid(),tenant,post_id,work))
    for i,asset in enumerate(dict.fromkeys(body.asset_ids)): cur.execute('INSERT INTO social_post_assets(id,tenant_id,post_id,asset_id,sort_order) VALUES(%s,%s,%s,%s,%s)',(uid(),tenant,post_id,asset,i))
    for asset in covers-set(body.asset_ids):
        cur.execute("INSERT INTO social_post_assets(id,tenant_id,post_id,asset_id,role) VALUES(%s,%s,%s,%s,'cover')",(uid(),tenant,post_id,asset))
    for target in body.targets:
        cur.execute('INSERT INTO social_post_targets(id,tenant_id,post_id,social_account_id,provider,provider_payload) VALUES(%s,%s,%s,%s,%s,%s)',(uid(),tenant,post_id,target.social_account_id,accounts[target.social_account_id]['provider'],Jsonb(target.provider_payload)))
    audit(cur,ctx,post_id,'draft_saved')
    return post_view(cur,owned(cur,'social_posts',post_id,tenant))
