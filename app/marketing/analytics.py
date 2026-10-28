"""Tenant-scoped durable analytics reads and refresh requests; no provider calls in HTTP handlers."""
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from uuid import UUID

from fastapi import HTTPException
from . import service as s
from .analytics_providers import METRICS, PROVIDERS, REQUIRED, VERSION, SUPPORTED, scopes


def window(start, end):
    end = end or datetime.now(timezone.utc).date()
    start = start or end - timedelta(days=29)
    if end < start or (end-start).days > 89 or end > datetime.now(timezone.utc).date():
        raise HTTPException(422, 'Choose a date range of up to 90 days ending today or earlier.')
    return start, end


def filters(cur, tenant):
    accounts = s.rows(cur, '''SELECT a.id,a.provider,a.display_name,a.status,a.scopes,
        j.status AS sync_status,j.last_attempt_at,j.last_success_at,j.next_run_at,j.error_category,j.backfill_before
        FROM social_accounts a LEFT JOIN social_analytics_sync j ON j.account_id=a.id AND j.tenant_id=a.tenant_id
        WHERE a.tenant_id=%s AND a.provider=ANY(%s) ORDER BY a.provider,a.display_name''', (tenant,list(PROVIDERS)))
    for account in accounts:
        granted = scopes(account)
        account['analytics_scopes'] = REQUIRED[account['provider']]
        account['missing_scopes'] = sorted(set(REQUIRED[account['provider']])-granted)
        account['scope_information_known'] = bool(granted) and account['provider'] not in ('facebook','instagram')
        account.pop('scopes')
    return {'accounts': accounts, 'campaigns': s.rows(cur,
        'SELECT id,name,start_date,end_date FROM marketing_campaigns WHERE tenant_id=%s ORDER BY name',(tenant,)),
        'platforms': list(PROVIDERS), 'definitions': {key:dict(label=v[0],unit=v[1],aggregation=v[2],description=v[3]) for key,v in METRICS.items()}}


def refresh(cur, ctx, account_id):
    account = s.owned(cur, 'social_accounts', account_id, ctx['tenant']['id'], True)
    if account['status'] != 'connected':
        raise HTTPException(409, 'Reconnect this account before synchronizing analytics.')
    if account['provider'] not in PROVIDERS:
        raise HTTPException(422, 'Analytics is not supported for this platform.')
    row = s.one(cur, '''INSERT INTO social_analytics_sync(account_id,tenant_id,requested_at)
        VALUES(%s,%s,now()) ON CONFLICT(account_id) DO UPDATE SET
        next_run_at=now(),requested_at=now(),status='pending',failures=0,error_category=NULL
        WHERE social_analytics_sync.tenant_id=EXCLUDED.tenant_id
          AND social_analytics_sync.status<>'processing'
          AND (social_analytics_sync.requested_at IS NULL OR social_analytics_sync.requested_at<now()-interval '5 minutes')
          AND (social_analytics_sync.last_attempt_at IS NULL OR social_analytics_sync.last_attempt_at<now()-interval '5 minutes')
        RETURNING account_id,status''',(account_id,ctx['tenant']['id']))
    if not row:
        raise HTTPException(429, 'A refresh is running or was requested recently. Please wait five minutes.')
    s.audit(cur,ctx,account_id,'analytics_refresh_requested')
    return row


def latest(rows):
    result = {}
    for row in sorted(rows, key=lambda r:r['observed_at']):
        key = (str(row['account_id']),row['subject'],row['metric'],row['scope'],
               row['period_start'] if row['scope']=='day' else None, row['definition_version'])
        result[key] = row
    return list(result.values())


def cell(rows, metric, basis, expected=None):
    selected = [r for r in rows if r['metric']==metric]
    available = [r for r in selected if r['availability']=='available' and r['value'] is not None]
    if not available:
        statuses = {r['availability'] for r in selected}
        state = next((x for x in ('permission_missing','temporary_failure','unsupported','not_returned') if x in statuses), 'not_synced')
        return dict(value=None,status=state,basis=basis,coverage=0,change_percent=None)
    count = len(available)
    coverage = min(1.0,count/expected) if expected else count/len(selected)
    return dict(value=sum(float(r['value']) for r in available),status='available' if coverage==1 else 'partial',
                basis=basis,coverage=coverage,change_percent=None)


def post_metrics(rows,start=None,end=None,published=None):
    rows = latest(rows)
    result = {}
    for metric in METRICS:
        candidates = [r for r in rows if r['metric']==metric]
        daily = [r for r in candidates if r['scope']=='day']
        expected = max(1,(end-max(start,published.date())).days+1) if daily and start and end and published else None
        result[metric] = cell(daily or candidates,metric,'selected_period' if daily else 'lifetime',expected)
    return result


def missing_status(accounts):
    categories={a.get('error_category') for a in accounts}
    if 'authorization_expired' in categories:return 'authorization_expired'
    if 'permission_missing' in categories:return 'permission_missing'
    if categories & {'temporary_failure','rate_limited','provider_failure','collection_limit'}:return 'temporary_failure'
    return 'not_returned' if accounts and all(a.get('last_success_at') for a in accounts) else 'not_synced'


def rate(metrics, provider):
    for key,value in metrics.items():
        if key in METRICS and key not in SUPPORTED[provider] and value.get('value') is None:
            value['status']='unsupported'
    denominator = 'impressions' if provider=='pinterest' else 'views' if provider=='tiktok' else 'reach'
    numerator = metrics.get('engagements',{})
    if numerator.get('value') is None and provider in ('facebook','tiktok'):
        components = [metrics.get(key,{}) for key in ('likes','comments','shares')]
        if all(x.get('value') is not None and x.get('status')=='available' for x in components) and len({x['basis'] for x in components})==1:
            numerator = {**components[0],'value':sum(x['value'] for x in components)}
            metrics['engagements'] = numerator
    den = metrics.get(denominator,{})
    value = (100*numerator['value']/den['value'] if numerator.get('value') is not None and den.get('value')
             and numerator.get('basis')==den.get('basis') and numerator.get('status')==den.get('status')=='available' else None)
    metrics['engagement_rate'] = dict(value=value,status='available' if value is not None else 'not_returned',
        basis=numerator.get('basis','selected_period'),coverage=min(numerator.get('coverage',0),den.get('coverage',0)),
        denominator=denominator,change_percent=None)


def report(cur, tenant, start=None, end=None, provider=None, account_id=None, campaign_id=None):
    start,end = window(start,end)
    if provider and provider not in PROVIDERS:
        raise HTTPException(422,'Unsupported analytics platform.')
    if account_id:
        s.owned(cur,'social_accounts',account_id,tenant)
    if campaign_id:
        s.owned(cur,'marketing_campaigns',campaign_id,tenant)
    options = filters(cur,tenant)
    accounts = [a for a in options['accounts'] if (not provider or a['provider']==provider) and (not account_id or str(a['id'])==str(account_id))]
    ids = [a['id'] for a in accounts]
    previous_start = start-(end-start)-timedelta(days=1)
    completed_end = min(end,datetime.now(timezone.utc).date()-timedelta(days=1))
    completed_days = max(1,(completed_end-start).days+1)
    comparison_start = start-timedelta(days=completed_days)
    observations = s.rows(cur, '''SELECT DISTINCT ON(account_id,subject,metric,scope,
        CASE WHEN scope='day' THEN period_start ELSE (observed_at AT TIME ZONE 'UTC')::date END,definition_version) o.*
        FROM social_analytics_observations o
        WHERE o.tenant_id=%s AND o.account_id=ANY(%s::uuid[]) AND o.definition_version=%s
        AND ((o.scope='day' AND o.period_start BETWEEN %s AND %s)
          OR (o.scope<>'day' AND o.observed_at >= %s::date AND o.observed_at < (%s::date+1)::timestamptz))
        ORDER BY account_id,subject,metric,scope,
        CASE WHEN scope='day' THEN period_start ELSE (observed_at AT TIME ZONE 'UTC')::date END,definition_version,observed_at DESC''',
        (tenant,ids,VERSION,previous_start,end,previous_start,end))
    baselines = s.rows(cur,'''SELECT DISTINCT ON(account_id,metric) * FROM social_analytics_observations
        WHERE tenant_id=%s AND account_id=ANY(%s::uuid[]) AND definition_version=%s
        AND subject='' AND scope='snapshot' AND observed_at<%s::date
        ORDER BY account_id,metric,observed_at DESC''',(tenant,ids,VERSION,start))
    observations = sorted(baselines+observations,key=lambda row:row['observed_at'])
    media = s.rows(cur, '''SELECT m.*,a.provider,a.display_name,t.post_id,p.campaign_id,c.name AS campaign_name,
        COALESCE(NULLIF(m.title,''),p.title,'') AS title
        FROM social_analytics_media m JOIN social_accounts a ON a.id=m.account_id AND a.tenant_id=m.tenant_id
        LEFT JOIN LATERAL (SELECT post_id FROM social_post_targets t
          WHERE t.social_account_id=m.account_id AND t.tenant_id=m.tenant_id AND t.provider_post_id=m.external_id
          ORDER BY t.published_at DESC NULLS LAST,t.id LIMIT 1) t ON true
        LEFT JOIN social_posts p ON p.id=t.post_id AND p.tenant_id=m.tenant_id
        LEFT JOIN marketing_campaigns c ON c.id=p.campaign_id AND c.tenant_id=p.tenant_id
        WHERE m.tenant_id=%s AND m.account_id=ANY(%s::uuid[])
        AND m.published_at>=%s::date AND m.published_at<(%s::date+1)::timestamptz
        AND (%s::uuid IS NULL OR p.campaign_id=%s::uuid)
        ORDER BY m.published_at DESC''',(tenant,ids,start,end,campaign_id,campaign_id))
    # A provider post may be referenced by several historical InkSuite targets.
    media = list({str(m['id']):m for m in media}.values())
    latest_rows = latest(observations)
    by_media = defaultdict(list)
    for row in observations:
        if row['subject'] and (row['scope']!='day' or start<=row['period_start']<=end):
            by_media[(str(row['account_id']),row['subject'])].append(row)
    for item in media:
        item['source'] = 'inksuite' if item['post_id'] else 'external'
        item['metrics'] = post_metrics(by_media[(str(item['account_id']),item['external_id'])],start,completed_end,item['published_at'])
        rate(item['metrics'],item['provider'])
        account_state=[a for a in accounts if str(a['id'])==str(item['account_id'])]
        for metric in item['metrics'].values():
            if metric['value'] is None and metric['status']=='not_synced':metric['status']=missing_status(account_state)
    panels = []
    for platform in (PROVIDERS if not provider else [provider]):
        selected_accounts = [a for a in accounts if a['provider']==platform]
        if not selected_accounts:
            continue
        platform_posts = [m for m in media if m['provider']==platform]
        subject_keys = {(str(m['account_id']),m['external_id']) for m in platform_posts}
        flows = [r for r in latest_rows if r['provider']==platform and not r['subject'] and r['scope']=='day' and start<=r['period_start']<=end]
        snapshots = [r for r in latest_rows if r['provider']==platform and not r['subject'] and r['scope']=='snapshot']
        counters = [r for r in latest_rows if (str(r['account_id']),r['subject']) in subject_keys and r['scope']=='lifetime']
        post_days = [r for r in latest_rows if (str(r['account_id']),r['subject']) in subject_keys and r['scope']=='day' and start<=r['period_start']<=end]
        metrics = {}
        for key in METRICS:
            daily = [r for r in flows if r['metric']==key] if not campaign_id else [r for r in post_days if r['metric']==key]
            if key=='followers':
                metrics[key] = cell(snapshots if not campaign_id else [],key,'snapshot',len(selected_accounts))
            elif daily:
                expected = len(selected_accounts)*completed_days if not campaign_id else sum(max(0,(completed_end-max(start,m['published_at'].date())).days+1) for m in platform_posts)
                metrics[key] = cell(daily,key,'selected_period',expected or None)
            else:
                metrics[key] = cell(counters,key,'lifetime_post_cohort',len(platform_posts) or None)
            if key not in SUPPORTED[platform]:
                metrics[key] = dict(value=None,status='unsupported',basis='selected_period',coverage=0,change_percent=None)
            elif metrics[key]['value'] is None and metrics[key]['status']=='not_synced':
                metrics[key]['status']=missing_status(selected_accounts)
            if metrics[key]['basis']=='selected_period' and metrics[key]['status']=='available':
                before = [r for r in latest_rows if r['provider']==platform and not r['subject'] and r['scope']=='day' and comparison_start<=r['period_start']<start]
                previous = cell(before,key,'selected_period',len(selected_accounts)*completed_days)
                if not campaign_id and previous['status']=='available' and previous['value']:
                    metrics[key]['change_percent'] = 100*(metrics[key]['value']-previous['value'])/previous['value']
        # Snapshot delta needs a real observation for every selected account at both boundaries.
        first,last = {},{}
        for row in sorted(observations,key=lambda r:r['observed_at']):
            if row['provider']==platform and row['metric']=='followers' and row['availability']=='available' and not row['subject']:
                if row['observed_at'].date()<start:
                    first[str(row['account_id'])] = float(row['value'])
                else:
                    last[str(row['account_id'])] = float(row['value'])
        growth = sum(last[k]-first[k] for k in last) if not campaign_id and len(first)==len(last)==len(selected_accounts) and first else None
        metrics['follower_growth'] = dict(value=growth,status='available' if growth is not None else 'not_synced',basis='snapshot_delta',coverage=1 if growth is not None else 0,change_percent=None)
        rate(metrics,platform)
        panels.append(dict(provider=platform,metrics=metrics,post_count=len(platform_posts)))
    # Keep gaps explicit. Lifetime histories are observations, not activity on that date.
    series = defaultdict(dict)
    permitted_subjects = {(str(m['account_id']),m['external_id']) for m in media}
    for row in observations:
        if row['availability']!='available':
            continue
        if campaign_id and (str(row['account_id']),row['subject']) not in permitted_subjects:
            continue
        if not campaign_id and row['subject']:
            continue
        day = row['period_start'] if row['scope']=='day' else row['observed_at'].date()
        if start<=day<=end:
            key=(row['provider'],row['metric'],row['scope'],str(row['account_id']),row['subject'],day)
            series[key] = row
    grouped = defaultdict(float)
    for key,row in series.items():
        grouped[(key[0],key[1],key[2],key[-1])] += float(row['value'])
    chart = [dict(provider=p,metric=m,scope=sc,date=d,value=v) for (p,m,sc,d),v in sorted(grouped.items())]
    for platform in PROVIDERS:
        den = 'impressions' if platform=='pinterest' else 'views' if platform=='tiktok' else 'reach'
        for day in {key[3] for key in grouped if key[0]==platform and key[2]=='day'}:
            numerator=grouped.get((platform,'engagements','day',day))
            denominator=grouped.get((platform,den,'day',day))
            num_subjects={(key[3],key[4]) for key in series if key[0]==platform and key[1]=='engagements' and key[2]=='day' and key[-1]==day}
            den_subjects={(key[3],key[4]) for key in series if key[0]==platform and key[1]==den and key[2]=='day' and key[-1]==day}
            if numerator is not None and denominator and num_subjects==den_subjects:
                chart.append(dict(provider=platform,metric='engagement_rate',scope='day',date=day,value=100*numerator/denominator))
    campaigns = []
    for campaign in options['campaigns']:
        items = [m for m in media if m['campaign_id']==campaign['id']]
        if items:
            breakdown = []
            for platform in PROVIDERS:
                platform_items = [m for m in items if m['provider']==platform]
                if not platform_items:
                    continue
                totals = {}
                for key in METRICS:
                    cells = [m['metrics'][key] for m in platform_items]
                    valid = [c for c in cells if c['value'] is not None]
                    bases = {c['basis'] for c in valid}
                    totals[key] = dict(value=sum(c['value'] for c in valid) if valid and len(bases)==1 else None,
                        status='available' if len(valid)==len(cells) and len(bases)==1 else 'partial' if valid and len(bases)==1 else 'not_returned',
                        basis=next(iter(bases)) if len(bases)==1 else 'incompatible',coverage=len(valid)/len(cells),change_percent=None)
                rate(totals,platform)
                breakdown.append(dict(provider=platform,metrics=totals,post_count=len(platform_items)))
            campaigns.append(dict(**campaign,post_count=len(items),platforms=breakdown))
    return dict(start=start,end=end,accounts=accounts,overview=panels,series=chart,posts=media,campaigns=campaigns,
        definitions=options['definitions'],financials_available=False,
        notes=['Platforms are reported separately. Reach across posts or days is not deduplicated.',
               'Daily reporting uses completed UTC days. Today may not have provider data; comparisons use the same number of completed days.',
               'Lifetime post counters describe posts published in the date range, not interactions occurring only in that range.',
               'Missing days stay missing. Historical follower growth starts only after sufficient snapshots exist.'])


def post_detail(cur, tenant, media_id):
    media = s.one(cur,'SELECT id,account_id,external_id FROM social_analytics_media WHERE id=%s AND tenant_id=%s',(media_id,tenant))
    if not media:
        raise HTTPException(404,'Analytics post not found.')
    history = s.rows(cur,'''SELECT metric,value,availability,scope,period_start,period_end,observed_at,api_version
        FROM social_analytics_observations WHERE tenant_id=%s AND account_id=%s AND subject=%s
        ORDER BY observed_at DESC,metric LIMIT 5000''',(tenant,media['account_id'],media['external_id']))
    return dict(media_id=media_id,history=history)
