"""Run separately: python -m app.marketing.worker. Never started by the web API."""
import logging,os,socket,time
from datetime import datetime,timezone,timedelta
from pathlib import Path
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb
from app.core.db import db_conn
from . import service as s
from .providers import publisher,ProviderError,media_for_target
from .publishing import credentials,recalculate

log=logging.getLogger(__name__)
RETRIES=(300,900,3600)

def tick():
    with db_conn() as conn,conn.cursor(row_factory=dict_row) as cur:
        with conn.transaction():
            # Stale processing is never republished blindly. A provider might have accepted the request.
            stale=s.rows(cur,"SELECT * FROM social_publish_jobs WHERE status='processing' AND locked_at<now()-interval '30 minutes' FOR UPDATE SKIP LOCKED")
            for job in stale:
                cur.execute("UPDATE social_publish_jobs SET status='failed',last_error='delivery_unknown',completed_at=now() WHERE id=%s",(job['id'],))
                t=s.one(cur,"UPDATE social_post_targets SET provider_status='failed',last_error_code='delivery_unknown',last_error_message='Worker stopped during delivery. Check the destination before creating another post.' WHERE id=%s AND tenant_id=%s RETURNING post_id",(job['post_target_id'],job['tenant_id']))
                recalculate(cur,t['post_id'],job['tenant_id'])
            job=s.one(cur,"""SELECT j.* FROM social_publish_jobs j JOIN social_post_targets t ON t.id=j.post_target_id AND t.tenant_id=j.tenant_id
              JOIN social_posts p ON p.id=t.post_id AND p.tenant_id=t.tenant_id
              WHERE j.status IN ('pending','retrying','submitted') AND j.scheduled_at<=now() AND (j.next_retry_at IS NULL OR j.next_retry_at<=now())
              ORDER BY j.scheduled_at FOR UPDATE OF p,t,j SKIP LOCKED LIMIT 1""")
            if not job: return False
            cur.execute("UPDATE social_publish_jobs SET status='processing',locked_at=now(),locked_by=%s,updated_at=now() WHERE id=%s",(socket.gethostname()+':'+str(os.getpid()),job['id']))
            target=s.one(cur,"UPDATE social_post_targets SET provider_status='processing',last_attempt_at=now(),attempt_count=attempt_count+1 WHERE id=%s AND tenant_id=%s RETURNING *",(job['post_target_id'],job['tenant_id']))
            s.audit(cur,{'tenant':{'id':job['tenant_id']},'user':{'id':None}},target['id'],'target_attempt_started')
        try:
            with conn.transaction():
                account=s.owned(cur,'social_accounts',target['social_account_id'],job['tenant_id'],True)
                token=credentials(cur,account)
            post=s.post_view(cur,s.owned(cur,'social_posts',target['post_id'],job['tenant_id']))
            adapter=publisher(target['provider']);media=media_for_target(post,target);adapter.validate_post(post,target,media)
            result=adapter.advance(account,token,post,target,media)
            started=target['provider_state'].get('_started_at') or datetime.now(timezone.utc).isoformat()
            result.state['_started_at']=started
            if result.status=='submitted' and datetime.now(timezone.utc)-datetime.fromisoformat(started)>timedelta(days=1):
                raise ProviderError('processing_timeout','Provider processing needs review. Check the destination before retrying.',False,True)
            with conn.transaction():
                cur.execute("UPDATE social_post_targets SET provider_status=%s,provider_state=%s,provider_post_id=COALESCE(%s,provider_post_id),provider_post_url=COALESCE(%s,provider_post_url),published_at=CASE WHEN %s='published' THEN now() ELSE NULL END,last_error_code=NULL,last_error_message=NULL,updated_at=now() WHERE id=%s AND tenant_id=%s",(result.status,Jsonb(result.state),result.post_id,result.url,result.status,target['id'],job['tenant_id']))
                cur.execute("UPDATE social_publish_jobs SET status=%s,next_retry_at=now()+interval '30 seconds',locked_at=NULL,locked_by=NULL,completed_at=CASE WHEN %s='published' THEN now() ELSE NULL END,updated_at=now() WHERE id=%s",('completed' if result.status=='published' else 'submitted',result.status,job['id']))
                s.audit(cur,{'tenant':{'id':job['tenant_id']},'user':{'id':None}},target['id'],'target_'+result.status)
                recalculate(cur,target['post_id'],job['tenant_id'])
        except Exception as exc:
            error=exc if isinstance(exc,ProviderError) else ProviderError('internal_error','Publishing stopped unexpectedly; check the destination before retrying.',False,True)
            code='delivery_unknown' if error.uncertain else error.code
            attempt=job['attempt_count']+1;retry=error.retryable and not error.uncertain and attempt<=len(RETRIES)
            with conn.transaction():
                cur.execute('UPDATE social_publish_jobs SET status=%s,attempt_count=%s,next_retry_at=%s,last_error=%s,locked_at=NULL,locked_by=NULL,updated_at=now() WHERE id=%s',('retrying' if retry else 'failed',attempt,datetime.now(timezone.utc)+timedelta(seconds=RETRIES[attempt-1]) if retry else None,code,job['id']))
                cur.execute('UPDATE social_post_targets SET provider_status=%s,last_error_code=%s,last_error_message=%s,updated_at=now() WHERE id=%s AND tenant_id=%s',('retrying' if retry else 'failed',code,str(error),target['id'],job['tenant_id']))
                s.audit(cur,{'tenant':{'id':job['tenant_id']},'user':{'id':None}},target['id'],'target_retrying' if retry else 'target_failed',{'code':code,'attempt':attempt})
                recalculate(cur,target['post_id'],job['tenant_id'])
            log.warning('Marketing target %s failed (%s)',target['id'],code)
        return True

def main():
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).resolve().parents[2]/'.env')
    logging.basicConfig(level=logging.INFO)
    while True:
        try:
            if not tick(): time.sleep(5)
        except Exception:
            log.error('Marketing worker database unavailable; will retry.')
            time.sleep(15)
if __name__=='__main__':main()
