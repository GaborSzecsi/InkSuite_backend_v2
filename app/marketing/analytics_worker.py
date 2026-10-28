"""Explicit analytics worker: python -m app.marketing.analytics_worker. Never runs in API startup."""
import logging
import time
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb
from app.core.db import db_conn
from . import service as s
from .analytics_providers import Collector, AnalyticsError, PROVIDERS, METRICS, VERSION
from .publishing import credentials
from .providers import ProviderError

log = logging.getLogger(__name__)


def persist(cur, account, result, run_id, observed):
    for metric,(label,unit,aggregation,description) in METRICS.items():
        cur.execute('''INSERT INTO social_analytics_definitions(provider,metric,version,label,unit,aggregation,description)
            VALUES(%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING''',
            (account['provider'],metric,VERSION,label,unit,aggregation,description))
    for item in result.media:
        cur.execute('''INSERT INTO social_analytics_media(id,tenant_id,account_id,external_id,media_type,title,permalink,thumbnail_url,published_at)
            VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT(account_id,tenant_id,external_id) DO UPDATE SET
            media_type=COALESCE(NULLIF(EXCLUDED.media_type,'unknown'),social_analytics_media.media_type),
            title=COALESCE(NULLIF(EXCLUDED.title,''),social_analytics_media.title),
            permalink=COALESCE(EXCLUDED.permalink,social_analytics_media.permalink),
            thumbnail_url=COALESCE(EXCLUDED.thumbnail_url,social_analytics_media.thumbnail_url),
            published_at=COALESCE(EXCLUDED.published_at,social_analytics_media.published_at),updated_at=now()''',
            (s.uid(),account['tenant_id'],account['id'],item['external_id'],item['media_type'],item['title'],item['permalink'],item['thumbnail_url'],item['published_at']))
    for row in result.observations:
        cur.execute('''INSERT INTO social_analytics_observations
            (tenant_id,account_id,provider,subject,metric,definition_version,api_version,value,availability,scope,period_start,period_end,observed_at,run_id)
            VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING''',
            (account['tenant_id'],account['id'],account['provider'],row['subject'],row['metric'],VERSION,result.api_version,row['value'],
             row['availability'],row['scope'],row['period_start'],row['period_end'],observed,run_id))


def tick():
    now = datetime.now(timezone.utc)
    with db_conn() as conn,conn.cursor(row_factory=dict_row) as cur:
        with conn.transaction():
            cur.execute('''INSERT INTO social_analytics_sync(account_id,tenant_id)
                SELECT id,tenant_id FROM social_accounts WHERE status='connected' AND provider=ANY(%s)
                ON CONFLICT DO NOTHING''',(list(PROVIDERS),))
            # Account is locked before sync state to match manual-refresh lock order.
            account = s.one(cur,'''SELECT a.* FROM social_accounts a JOIN social_analytics_sync j
                ON j.account_id=a.id AND j.tenant_id=a.tenant_id WHERE a.status='connected'
                AND j.next_run_at<=now() AND (j.status<>'processing' OR j.locked_at<now()-interval '15 minutes')
                ORDER BY j.next_run_at FOR UPDATE OF a SKIP LOCKED LIMIT 1''')
            if not account:
                return False
            state = s.one(cur,'SELECT * FROM social_analytics_sync WHERE account_id=%s AND tenant_id=%s FOR UPDATE',(account['id'],account['tenant_id']))
            run_id = s.uid()
            cur.execute("UPDATE social_analytics_sync SET status='processing',run_id=%s,locked_at=now(),last_attempt_at=now() WHERE account_id=%s AND tenant_id=%s",(run_id,account['id'],account['tenant_id']))
        category = None
        result = None
        # Lock only around individual API calls, not the whole multi-minute sync.
        # Publishing can proceed between requests; completed disconnects stop new calls.
        try:
            with conn.transaction():
                account = s.owned(cur,'social_accounts',account['id'],account['tenant_id'],True)
                if account['status']!='connected':
                    raise AnalyticsError('disconnected')
                token = credentials(cur,account,redact_errors=True)
                # Register existing publications even if analytics access is not yet granted.
                # This does not create duplicate posts: campaign/title ownership stays in social_posts.
                cur.execute('''INSERT INTO social_analytics_media(id,tenant_id,account_id,external_id,media_type,permalink,published_at)
                    SELECT gen_random_uuid(),tenant_id,social_account_id,provider_post_id,'unknown',provider_post_url,published_at
                    FROM social_post_targets WHERE tenant_id=%s AND social_account_id=%s
                    AND provider_status='published' AND provider_post_id IS NOT NULL
                    AND published_at>=now()-interval '90 days' ON CONFLICT DO NOTHING''',(account['tenant_id'],account['id']))
                today = now.date()
                backfill_end = state['backfill_before']-timedelta(days=1) if state['backfill_before'] else today-timedelta(days=1)
                backfilling = not state['last_success_at'] or (state['backfill_before'] and state['backfill_before']>today-timedelta(days=90))
                end = backfill_end if backfilling else today-timedelta(days=1)
                start = max(today-timedelta(days=90),end-timedelta(days=6))
                # Refresh a rotating least-recently-observed cohort, including posts older
                # than the daily lookback. Existing target IDs are reused, not copied.
                known = s.rows(cur,'''SELECT external_id FROM (
                    SELECT t.provider_post_id AS external_id,COALESCE(m.updated_at,'epoch'::timestamptz) AS refreshed
                    FROM social_post_targets t LEFT JOIN social_analytics_media m
                      ON m.account_id=t.social_account_id AND m.tenant_id=t.tenant_id AND m.external_id=t.provider_post_id
                    WHERE t.social_account_id=%s AND t.tenant_id=%s AND t.provider_status='published'
                      AND t.provider_post_id IS NOT NULL AND t.published_at>=now()-interval '90 days'
                    UNION SELECT external_id,updated_at FROM social_analytics_media
                    WHERE account_id=%s AND tenant_id=%s AND published_at>=now()-interval '90 days'
                    ) known GROUP BY external_id ORDER BY min(refreshed),external_id LIMIT %s''',
                    (account['id'],account['tenant_id'],account['id'],account['tenant_id'],5 if account['provider']=='instagram' else 15))
                expected = s.one(cur,'SELECT access_token_encrypted FROM social_accounts WHERE id=%s AND tenant_id=%s',
                                 (account['id'],account['tenant_id']))['access_token_encrypted']
            @contextmanager
            def guard():
                with conn.transaction():
                    active = s.one(cur,'SELECT status,access_token_encrypted FROM social_accounts WHERE id=%s AND tenant_id=%s FOR SHARE',
                                   (account['id'],account['tenant_id']))
                    if not active or active['status']!='connected':
                        raise AnalyticsError('disconnected')
                    if active['access_token_encrypted']!=expected:
                        raise AnalyticsError('authorization_changed')
                    yield
            result = Collector(account,token,start,end,known_posts=known,checkpoint=state['discovery_cursors'],guard=guard).collect()
            with guard():
                current = s.one(cur,'SELECT run_id FROM social_analytics_sync WHERE account_id=%s AND tenant_id=%s FOR UPDATE',
                                (account['id'],account['tenant_id']))
                if str(current['run_id'])!=str(run_id):
                    return True
                persist(cur,account,result,run_id,datetime.now(timezone.utc))
                category = next((x for x in ('authorization_expired','permission_missing','rate_limited','temporary_failure','collection_limit','unsupported') if x in result.limitations),None)
                has_values = any(x['availability']=='available' for x in result.observations)
                if not has_values:
                    category = category or 'not_returned'
                advance = backfilling and category not in ('authorization_expired','permission_missing','rate_limited','temporary_failure','collection_limit')
                status = 'permission_missing' if category=='permission_missing' and not has_values else 'partial' if category else 'success'
                failures = min(8,state['failures']+1) if category in ('rate_limited','temporary_failure','collection_limit') else 0
                delay = min(21600,300*2**(failures-1)) if failures else 300 if advance and start>today-timedelta(days=90) else 21600
                delay = max(delay,result.retry_after)
                next_time = now + timedelta(seconds=delay)
                cur.execute('''UPDATE social_analytics_sync SET status=%s,error_category=%s,
                    last_success_at=CASE WHEN %s THEN now() ELSE last_success_at END,
                    next_run_at=%s,locked_at=NULL,failures=%s,last_collected_through=%s,
                    backfill_before=CASE WHEN %s THEN %s ELSE backfill_before END,discovery_cursors=%s
                    WHERE account_id=%s AND tenant_id=%s AND run_id=%s''',
                    (status,category,has_values,next_time,failures,end,advance,start,Jsonb(result.cursors),account['id'],account['tenant_id'],run_id))
                s.audit(cur,{'tenant':{'id':account['tenant_id']},'user':{'id':None}},account['id'],
                        'analytics_sync_completed',{'status':status,'category':category,'observations':len(result.observations),'calls':result.calls})
        except (AnalyticsError,ProviderError) as error:
            category = error.category if isinstance(error,AnalyticsError) else ('disconnected' if error.code=='disconnected' else 'authorization_expired' if error.code in ('expired','credentials','190') else 'provider_failure')
            failures = min(8,state['failures']+1)
            delay = min(21600,300*2**(failures-1)) if error.retryable else 21600
            delay = max(delay,getattr(error,'retry_after',0))
            if category=='authorization_changed':
                delay=0
            with conn.transaction():
                cur.execute('''UPDATE social_analytics_sync SET status=%s,error_category=%s,failures=%s,
                    next_run_at=%s,locked_at=NULL WHERE account_id=%s AND tenant_id=%s AND run_id=%s''',
                    ('pending' if category=='authorization_changed' else 'disconnected' if category=='disconnected' else 'permission_missing' if category=='permission_missing' else 'failed',
                     category,failures,now+timedelta(seconds=delay),account['id'],account['tenant_id'],run_id))
        return True


def main():
    logging.basicConfig(level=logging.INFO)
    while True:
        try:
            worked = tick()
        except Exception:
            # Database/provider exception messages may contain credentials; no raw exceptions.
            log.error('Analytics worker iteration failed; check database migration and configuration.')
            worked = False
        time.sleep(1 if worked else 30)


if __name__=='__main__':
    main()
