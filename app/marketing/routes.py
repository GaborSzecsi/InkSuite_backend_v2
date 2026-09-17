from datetime import datetime, timezone
from datetime import timedelta, date
from uuid import UUID
from fastapi import APIRouter, Depends, HTTPException, UploadFile, File, Form, Query
from psycopg.rows import dict_row
from app.core.db import db_conn
from . import service as s, schemas, assets
from . import oauth, publishing
from .providers import publisher, ProviderError, request as provider_request

router=APIRouter(prefix='/tenants/{tenant_slug}/marketing',tags=['Marketing'])

def work_facts(cur,tenant,work_id):
    from routers.catalog import _build_full_work_payload
    s.owned(cur,'works',work_id,tenant)
    document=_build_full_work_payload(cur,str(tenant),str(work_id))
    fields=('id','uid','title','subtitle','series','author','illustrator','publication_date','cover_image_link','description','publisher_name')
    result={key:document.get(key) for key in fields}
    cover=result.get('cover_image_link') or ''
    if cover.startswith('tenants/') and '/public/' in cover: result['cover_image_link']=assets.public_url(cover)
    return result

@router.get('/works/{work_id}')
def title_facts(work_id:UUID,ctx=Depends(s.access)):
    with db_conn() as conn,conn.cursor(row_factory=dict_row) as cur: return work_facts(cur,ctx['tenant']['id'],work_id)

@router.post('/campaigns/{campaign_id}/template')
def template(campaign_id:UUID,body:schemas.Template,ctx=Depends(s.access)):
    with db_conn() as conn,conn.transaction(),conn.cursor(row_factory=dict_row) as cur:
        c=s.campaign_view(cur,s.owned(cur,'marketing_campaigns',campaign_id,ctx['tenant']['id'],True))
        if len(c['works'])!=1: raise HTTPException(422,'A launch template needs one title.')
        if s.one(cur,"SELECT id FROM marketing_audit WHERE tenant_id=%s AND entity_id=%s AND action='launch_template_created'",(ctx['tenant']['id'],campaign_id)): raise HTTPException(409,'This campaign already has launch milestones.')
        facts=work_facts(cur,ctx['tenant']['id'],c['works'][0]['id'])
        try: publication=date.fromisoformat(str(facts.get('publication_date') or ''))
        except ValueError: raise HTTPException(422,'Set a full publication date in the title metadata first.') from None
        milestones=[(-90,'Cover Reveal'),(-60,'Meet the Author'),(-45,'Story Introduction'),(-30,'Preorder Reminder'),(-14,'Review Quote'),(-7,'Countdown'),(-1,'Tomorrow'),(0,'Available Today'),(7,'Reader Follow-up')]
        from .domain import schedule_instant
        for offset,title in milestones:
            draft=s.save_post(cur,schemas.Post(campaign_id=campaign_id,title=title,work_ids=[c['works'][0]['id']],timezone=body.timezone),ctx)
            instant=schedule_instant((publication+timedelta(days=offset)).isoformat()+'T10:00',body.timezone)
            cur.execute('UPDATE social_posts SET scheduled_at=%s WHERE id=%s AND tenant_id=%s',(instant,draft['id'],ctx['tenant']['id']))
        s.audit(cur,ctx,campaign_id,'launch_template_created')
        return {'drafts_created':len(milestones)}

@router.get('/summary')
def summary(zone:str='UTC',ctx=Depends(s.access)):
    from zoneinfo import ZoneInfo,ZoneInfoNotFoundError
    try: ZoneInfo(zone)
    except ZoneInfoNotFoundError: raise HTTPException(422,'Choose an IANA timezone.') from None
    with db_conn() as conn,conn.cursor(row_factory=dict_row) as cur:
        tenant=ctx['tenant']['id']
        return {'active_campaigns':s.one(cur,"SELECT count(*) AS n FROM marketing_campaigns WHERE tenant_id=%s AND status='active'",(tenant,))['n'],
          'post_counts':{r['status']:r['count'] for r in s.rows(cur,'SELECT status,count(*) FROM social_posts WHERE tenant_id=%s GROUP BY status',(tenant,))},
          'today':s.one(cur,"SELECT count(*) AS n FROM social_posts WHERE tenant_id=%s AND status='scheduled' AND (scheduled_at AT TIME ZONE %s)::date=(now() AT TIME ZONE %s)::date",(tenant,zone,zone))['n'],
          'failed':s.one(cur,"SELECT count(DISTINCT post_id) AS n FROM social_post_targets WHERE tenant_id=%s AND provider_status='failed'",(tenant,))['n']}

@router.get('/works')
def works(q:str='',ctx=Depends(s.access)):
    with db_conn() as conn,conn.cursor(row_factory=dict_row) as cur:
        return s.rows(cur,'SELECT id,uid,title FROM works WHERE tenant_id=%s AND title ILIKE %s ORDER BY title LIMIT 100',(ctx['tenant']['id'],'%'+q+'%'))

@router.get('/campaigns')
def campaigns(q:str='',work_id:UUID|None=None,ctx=Depends(s.access)):
    with db_conn() as conn,conn.cursor(row_factory=dict_row) as cur:
        result=s.rows(cur,'''SELECT c.* FROM marketing_campaigns c WHERE c.tenant_id=%s AND c.name ILIKE %s
        AND (%s::uuid IS NULL OR EXISTS(SELECT 1 FROM marketing_campaign_works cw WHERE cw.campaign_id=c.id AND cw.tenant_id=c.tenant_id AND cw.work_id=%s)) ORDER BY c.updated_at DESC''',(ctx['tenant']['id'],'%'+q+'%',work_id,work_id))
        return [s.campaign_view(cur,c) for c in result]

@router.post('/campaigns')
@router.put('/campaigns/{campaign_id}')
def save_campaign(body:schemas.Campaign,campaign_id:UUID|None=None,ctx=Depends(s.access)):
    tenant=ctx['tenant']['id']
    with db_conn() as conn,conn.transaction(),conn.cursor(row_factory=dict_row) as cur:
        if campaign_id: s.owned(cur,'marketing_campaigns',campaign_id,tenant,True)
        else: campaign_id=s.uid()
        for w in set(body.work_ids): s.owned(cur,'works',w,tenant)
        cur.execute('''INSERT INTO marketing_campaigns(id,tenant_id,name,description,association_type,start_date,end_date,status,created_by,archived_at)
        VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT(id) DO UPDATE SET name=EXCLUDED.name,description=EXCLUDED.description,association_type=EXCLUDED.association_type,start_date=EXCLUDED.start_date,end_date=EXCLUDED.end_date,status=EXCLUDED.status,archived_at=EXCLUDED.archived_at,updated_at=now()''',
        (campaign_id,tenant,body.name,body.description,body.association_type,body.start_date,body.end_date,body.status,ctx['user']['id'],datetime.now(timezone.utc) if body.status=='archived' else None))
        cur.execute('DELETE FROM marketing_campaign_works WHERE campaign_id=%s AND tenant_id=%s',(campaign_id,tenant))
        for w in set(body.work_ids): cur.execute('INSERT INTO marketing_campaign_works(id,tenant_id,campaign_id,work_id) VALUES(%s,%s,%s,%s)',(s.uid(),tenant,campaign_id,w))
        s.audit(cur,ctx,campaign_id,'campaign_saved')
        return s.campaign_view(cur,s.owned(cur,'marketing_campaigns',campaign_id,tenant))

@router.get('/campaigns/{campaign_id}')
def campaign(campaign_id:UUID,ctx=Depends(s.access)):
    with db_conn() as conn,conn.cursor(row_factory=dict_row) as cur:
        return s.campaign_view(cur,s.owned(cur,'marketing_campaigns',campaign_id,ctx['tenant']['id']))

@router.get('/social/posts')
@router.get('/calendar')
def posts(campaign_id:UUID|None=None,work_id:UUID|None=None,q:str='',status:str='',provider:str='',offset:int=Query(0,ge=0),limit:int=Query(500,ge=1,le=500),ctx=Depends(s.access)):
    with db_conn() as conn,conn.cursor(row_factory=dict_row) as cur:
        result=s.rows(cur,'''SELECT p.* FROM social_posts p WHERE p.tenant_id=%s
        AND (%s::uuid IS NULL OR p.campaign_id=%s) AND (%s='' OR p.status=%s)
        AND (p.title ILIKE %s OR p.content_text ILIKE %s)
        AND (%s::uuid IS NULL OR EXISTS(SELECT 1 FROM social_post_works pw WHERE pw.post_id=p.id AND pw.tenant_id=p.tenant_id AND pw.work_id=%s))
        AND (%s='' OR EXISTS(SELECT 1 FROM social_post_targets t WHERE t.post_id=p.id AND t.tenant_id=p.tenant_id AND t.provider=%s))
        ORDER BY p.created_at DESC,p.id LIMIT %s OFFSET %s''',(ctx['tenant']['id'],campaign_id,campaign_id,status,status,'%'+q+'%','%'+q+'%',work_id,work_id,provider,provider,limit,offset))
        return [s.post_view(cur,p) for p in result]

@router.post('/social/posts')
@router.put('/social/posts/{post_id}')
def save_post(body:schemas.Post,post_id:UUID|None=None,ctx=Depends(s.access)):
    with db_conn() as conn,conn.transaction(),conn.cursor(row_factory=dict_row) as cur:
        return s.save_post(cur,body,ctx,post_id)

@router.get('/social/posts/{post_id}')
def post(post_id:UUID,ctx=Depends(s.access)):
    with db_conn() as conn,conn.cursor(row_factory=dict_row) as cur:
        return s.post_view(cur,s.owned(cur,'social_posts',post_id,ctx['tenant']['id']))

@router.get('/assets/title/{work_id}')
def title_assets(work_id:UUID,campaign_id:UUID|None=None,ctx=Depends(s.access)):
    with db_conn() as conn,conn.cursor(row_factory=dict_row) as cur:
        originals=assets.discover(cur,ctx,work_id)
        if campaign_id: s.owned(cur,'marketing_campaigns',campaign_id,ctx['tenant']['id'])
        generated=s.rows(cur,"""SELECT * FROM social_media_assets WHERE tenant_id=%s
          AND work_id=%s AND source_type='generated_derivative'
          AND (campaign_id IS NULL OR campaign_id=%s) ORDER BY created_at DESC""",
          (ctx['tenant']['id'],work_id,campaign_id))
        return generated+originals

@router.post('/assets/reference')
def reference(body:schemas.AssetReference,ctx=Depends(s.access)):
    with db_conn() as conn,conn.transaction(),conn.cursor(row_factory=dict_row) as cur: return assets.reference(cur,ctx,body)

@router.get('/assets/publisher')
@router.get('/assets/campaign/{campaign_id}')
def list_assets(campaign_id:UUID|None=None,ctx=Depends(s.access)):
    with db_conn() as conn,conn.cursor(row_factory=dict_row) as cur:
        if campaign_id: s.owned(cur,'marketing_campaigns',campaign_id,ctx['tenant']['id'])
        return s.rows(cur,'''SELECT * FROM social_media_assets WHERE tenant_id=%s AND
        ((%s::uuid IS NULL AND campaign_id IS NULL AND source_type IN ('publisher_asset','generated_derivative')) OR campaign_id=%s) ORDER BY created_at DESC''',(ctx['tenant']['id'],campaign_id,campaign_id))

@router.post('/assets/upload')
async def upload(file:UploadFile=File(...),campaign_id:UUID|None=Form(None),category:str=Form('general'),ctx=Depends(s.access)):
    data=await file.read(500*1024*1024+1)
    with db_conn() as conn,conn.transaction(),conn.cursor(row_factory=dict_row) as cur:
        return assets.upload(cur,ctx,file.filename or 'image',data,campaign_id,category)

@router.patch('/assets/{asset_id}')
def metadata(asset_id:UUID,body:schemas.AssetMetadata,ctx=Depends(s.access)):
    with db_conn() as conn,conn.transaction(),conn.cursor(row_factory=dict_row) as cur:
        a=s.owned(cur,'social_media_assets',asset_id,ctx['tenant']['id'])
        if a['source_type']=='title_public_asset': raise HTTPException(403,'Manage title files from the title project.')
        return s.one(cur,'UPDATE social_media_assets SET display_name=%s,alt_text=%s WHERE id=%s AND tenant_id=%s RETURNING *',(body.display_name,body.alt_text,asset_id,ctx['tenant']['id']))

@router.post('/assets/{asset_id}/derivative')
def derivative(asset_id:UUID,body:schemas.Derivative,ctx=Depends(s.access)):
    with db_conn() as conn,conn.transaction(),conn.cursor(row_factory=dict_row) as cur: return assets.derivative(cur,ctx,asset_id,body)

@router.get('/storage')
def storage_usage(ctx=Depends(s.access)):
    from . import storage
    return storage.usage(ctx)

@router.delete('/storage')
def clear_storage(confirmation:str='',ctx=Depends(s.admin)):
    from . import storage
    if confirmation!='DELETE MARKETING ASSETS': raise HTTPException(422,'Confirm deletion of Marketing assets.')
    items=storage.inventory(ctx);deleted=protected=failed=0
    with db_conn() as conn:
        for item in items:
            try:
                with conn.transaction(),conn.cursor(row_factory=dict_row) as cur:
                    storage.delete_key(cur,ctx,item['Key'])
                deleted+=1
            except HTTPException as error:
                if error.status_code==409: protected+=1
                else: failed+=1
            except Exception: failed+=1
    return {'deleted':deleted,'protected':protected,'failed':failed,**storage.usage(ctx)}

@router.delete('/assets/{asset_id}')
def delete_asset(asset_id:UUID,ctx=Depends(s.admin)):
    from . import storage
    with db_conn() as conn,conn.transaction(),conn.cursor(row_factory=dict_row) as cur:
        storage.lock(cur,ctx)
        a=s.owned(cur,'social_media_assets',asset_id,ctx['tenant']['id'])
        if a['source_type']=='title_public_asset': raise HTTPException(403,'Title assets cannot be deleted from Marketing.')
        if a['s3_bucket']!=assets.bucket(): raise HTTPException(409,'This asset belongs to a different storage bucket.')
        return storage.delete_key(cur,ctx,a['s3_key'])

@router.get('/social/accounts')
def accounts(ctx=Depends(s.access)):
    with db_conn() as conn,conn.cursor(row_factory=dict_row) as cur:
        return s.rows(cur,'''SELECT id,provider,display_name,username,account_type,profile_image_url,status,token_expires_at,disconnected_at FROM social_accounts WHERE tenant_id=%s ORDER BY provider,display_name''',(ctx['tenant']['id'],))

@router.post('/social/connect/{provider}')
def connect(provider:str,ctx=Depends(s.admin)):
    try:
        publisher(provider)
        with db_conn() as conn,conn.transaction(),conn.cursor(row_factory=dict_row) as cur: return oauth.begin(cur,ctx,provider)
    except ProviderError as e: raise HTTPException(422,str(e)) from None

@router.post('/social/oauth/{provider}/callback')
def callback(provider:str,body:schemas.OAuthCallback,ctx=Depends(s.admin)):
    try:
        publisher(provider)
        with db_conn() as conn,conn.cursor(row_factory=dict_row) as cur:
            with conn.transaction(): oauth.consume_state(cur,ctx,provider,body.state)
            with conn.transaction(): return oauth.finish(cur,ctx,provider,body.code)
    except ProviderError as e: raise HTTPException(422,str(e)) from None

@router.post('/social/oauth/{provider}/select')
def select_accounts(provider:str,body:schemas.OAuthSelection,ctx=Depends(s.admin)):
    if provider not in ('facebook','instagram'):
        raise HTTPException(422,'Page selection is only available for Facebook and Instagram.')
    try:
        with db_conn() as conn,conn.transaction(),conn.cursor(row_factory=dict_row) as cur:
            return oauth.confirm_selection(cur,ctx,provider,body.selection_token,body.account_ids)
    except ProviderError as e: raise HTTPException(422,str(e)) from None

@router.delete('/social/accounts/{account_id}')
def disconnect(account_id:UUID,ctx=Depends(s.admin)):
    with db_conn() as conn,conn.transaction(),conn.cursor(row_factory=dict_row) as cur:
        a=s.owned(cur,'social_accounts',account_id,ctx['tenant']['id'],True)
        if s.one(cur,"SELECT id FROM social_post_targets WHERE social_account_id=%s AND tenant_id=%s AND provider_status='processing'",(account_id,ctx['tenant']['id'])):
            raise HTTPException(409,'A publication request is in progress. Try disconnecting again shortly.')
        cur.execute("UPDATE social_accounts SET status='disconnected',access_token_encrypted='',refresh_token_encrypted=NULL,disconnected_at=now(),updated_at=now() WHERE id=%s AND tenant_id=%s",(account_id,ctx['tenant']['id']))
        s.audit(cur,ctx,account_id,'social_account_disconnected');return {'ok':True}

@router.get('/social/pinterest/boards')
def boards(account_id:UUID,ctx=Depends(s.admin)):
    try:
        with db_conn() as conn,conn.transaction(),conn.cursor(row_factory=dict_row) as cur:
            a=s.owned(cur,'social_accounts',account_id,ctx['tenant']['id'],True)
            if a['provider']!='pinterest': raise HTTPException(422,'Select a Pinterest account.')
            token=publishing.credentials(cur,a);result=[];bookmark=None
            for _ in range(20):
                data=provider_request('GET','https://api-sandbox.pinterest.com/v5/boards',token,params={'page_size':100,**({'bookmark':bookmark} if bookmark else {})})
                result.extend({'id':b['id'],'name':b['name']} for b in data.get('items',[]));bookmark=data.get('bookmark')
                if not bookmark: break
            return result
    except ProviderError as e: raise HTTPException(422,str(e)) from None

@router.get('/social/tiktok/creator')
def creator(account_id:UUID,ctx=Depends(s.admin)):
    try:
        with db_conn() as conn,conn.transaction(),conn.cursor(row_factory=dict_row) as cur:
            a=s.owned(cur,'social_accounts',account_id,ctx['tenant']['id'],True)
            if a['provider']!='tiktok': raise HTTPException(422,'Select a TikTok account.')
            return publisher('tiktok').creator(publishing.credentials(cur,a))
    except ProviderError as e: raise HTTPException(422,str(e)) from None

@router.post('/social/posts/{post_id}/schedule')
def schedule(post_id:UUID,body:schemas.Schedule,ctx=Depends(s.admin)):
    with db_conn() as conn,conn.transaction(),conn.cursor(row_factory=dict_row) as cur: return publishing.schedule(cur,ctx,post_id,body)

@router.post('/social/posts/{post_id}/publish')
def publish(post_id:UUID,ctx=Depends(s.admin)):
    with db_conn() as conn,conn.transaction(),conn.cursor(row_factory=dict_row) as cur: return publishing.schedule(cur,ctx,post_id)

@router.post('/social/posts/{post_id}/cancel')
def cancel(post_id:UUID,ctx=Depends(s.admin)):
    with db_conn() as conn,conn.transaction(),conn.cursor(row_factory=dict_row) as cur: return publishing.cancel(cur,ctx,post_id)

@router.post('/social/targets/{target_id}/retry')
def retry(target_id:UUID,ctx=Depends(s.admin)):
    with db_conn() as conn,conn.transaction(),conn.cursor(row_factory=dict_row) as cur: return publishing.retry(cur,ctx,target_id)
