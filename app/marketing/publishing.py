from datetime import datetime,timedelta,timezone
from fastapi import HTTPException
from psycopg.types.json import Jsonb
from . import service as s
from .domain import schedule_instant,aggregate
from .providers import publisher,ProviderError,media_for_target
from .oauth import encrypt

def credentials(cur,account):
    token,data=publisher(account['provider']).refresh_credentials(account)
    if data:
        expiry=datetime.now(timezone.utc)+timedelta(seconds=int(data['expires_in']))
        refresh_expiry=datetime.now(timezone.utc)+timedelta(seconds=int(data['refresh_expires_in'])) if data.get('refresh_expires_in') else None
        cur.execute('''UPDATE social_accounts SET access_token_encrypted=%s,refresh_token_encrypted=COALESCE(%s,refresh_token_encrypted),token_expires_at=%s,refresh_token_expires_at=COALESCE(%s,refresh_token_expires_at),updated_at=now() WHERE id=%s AND tenant_id=%s''',
        (encrypt(token,account['tenant_id']),encrypt(data['refresh_token'],account['tenant_id']) if data.get('refresh_token') else None,expiry,refresh_expiry,account['id'],account['tenant_id']))
    return token

def recalculate(cur,post_id,tenant):
    targets=s.rows(cur,'SELECT provider_status FROM social_post_targets WHERE post_id=%s AND tenant_id=%s',(post_id,tenant))
    status=aggregate([t['provider_status'] for t in targets])
    cur.execute("UPDATE social_posts SET status=%s,published_at=CASE WHEN %s='published' THEN COALESCE(published_at,now()) ELSE published_at END,updated_at=now() WHERE id=%s AND tenant_id=%s",(status,status,post_id,tenant))

def schedule(cur,ctx,post_id,body=None):
    tenant=ctx['tenant']['id'];post=s.post_view(cur,s.owned(cur,'social_posts',post_id,tenant,True))
    if post['status'] not in ('draft','scheduled'): raise HTTPException(409,'Only drafts or unclaimed schedules can be scheduled.')
    if not post['targets']: raise HTTPException(422,'Select at least one connected social destination.')
    if any(t['provider_status'] not in ('pending','scheduled') for t in post['targets']): raise HTTPException(409,'Publication already started.')
    try:
        due=schedule_instant(body.local_datetime,body.timezone,body.fold) if body else datetime.now(timezone.utc)
        if body and due<=datetime.now(timezone.utc): raise ValueError('Schedule a future time.')
        for target in post['targets']:
            account=s.owned(cur,'social_accounts',target['social_account_id'],tenant)
            if account['status']!='connected': raise ValueError('Reconnect all selected destinations.')
            publisher(target['provider']).validate_post(post,target,media_for_target(post,target))
    except (ValueError,KeyError,ProviderError) as e: raise HTTPException(422,str(e)) from None
    zone=body.timezone if body else post['timezone']
    cur.execute("UPDATE social_posts SET status='scheduled',scheduled_at=%s,timezone=%s,publish_mode=%s,scheduled_by=%s,scheduling_timestamp=now(),updated_at=now() WHERE id=%s AND tenant_id=%s",(due,zone,'schedule' if body else 'now',ctx['user']['id'],post_id,tenant))
    for target in post['targets']:
        cur.execute("UPDATE social_post_targets SET provider_status='scheduled' WHERE id=%s AND tenant_id=%s",(target['id'],tenant))
        cur.execute('''INSERT INTO social_publish_jobs(id,tenant_id,post_target_id,scheduled_at) VALUES(%s,%s,%s,%s)
        ON CONFLICT(post_target_id) DO UPDATE SET scheduled_at=EXCLUDED.scheduled_at,status='pending',next_retry_at=NULL,updated_at=now()''',(s.uid(),tenant,target['id'],due))
    s.audit(cur,ctx,post_id,'post_scheduled' if body else 'publish_requested')
    return s.post_view(cur,s.owned(cur,'social_posts',post_id,tenant))

def cancel(cur,ctx,post_id):
    tenant=ctx['tenant']['id'];post=s.owned(cur,'social_posts',post_id,tenant,True)
    targets=s.rows(cur,'SELECT * FROM social_post_targets WHERE post_id=%s AND tenant_id=%s FOR UPDATE',(post_id,tenant))
    if any(t['provider_status'] in ('processing','submitted','published') for t in targets): raise HTTPException(409,'Publication has already started; this post cannot be cancelled.')
    cur.execute("UPDATE social_post_targets SET provider_status='cancelled',updated_at=now() WHERE post_id=%s AND tenant_id=%s",(post_id,tenant))
    cur.execute("UPDATE social_publish_jobs SET status='cancelled',completed_at=now() WHERE tenant_id=%s AND post_target_id IN(SELECT id FROM social_post_targets WHERE post_id=%s AND tenant_id=%s)",(tenant,post_id,tenant))
    cur.execute("UPDATE social_posts SET status='cancelled',cancelled_at=now(),updated_at=now() WHERE id=%s AND tenant_id=%s",(post_id,tenant))
    s.audit(cur,ctx,post_id,'post_cancelled');return {'ok':True}

def retry(cur,ctx,target_id):
    tenant=ctx['tenant']['id'];target=s.owned(cur,'social_post_targets',target_id,tenant,True)
    if target['provider_status']!='failed': raise HTTPException(409,'Only failed destinations can be retried.')
    if target['last_error_code']=='delivery_unknown': raise HTTPException(409,'Check the destination for this post before creating a new draft. Automatic retry is disabled because delivery is uncertain.')
    account=s.owned(cur,'social_accounts',target['social_account_id'],tenant)
    if account['status']!='connected': raise HTTPException(422,'Reconnect this social account first.')
    post=s.post_view(cur,s.owned(cur,'social_posts',target['post_id'],tenant))
    try: publisher(target['provider']).validate_post(post,target,media_for_target(post,target))
    except ProviderError as e: raise HTTPException(422,str(e)) from None
    cur.execute("UPDATE social_post_targets SET provider_status='retrying',last_error_code=NULL,last_error_message=NULL,updated_at=now() WHERE id=%s AND tenant_id=%s",(target_id,tenant))
    cur.execute("UPDATE social_publish_jobs SET status='retrying',scheduled_at=now(),next_retry_at=NULL,attempt_count=0,completed_at=NULL,updated_at=now() WHERE post_target_id=%s AND tenant_id=%s",(target_id,tenant))
    s.audit(cur,ctx,target_id,'target_retry_requested');recalculate(cur,target['post_id'],tenant);return {'ok':True}
