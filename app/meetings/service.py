import hashlib, html, json, os, secrets, uuid
from datetime import datetime,timedelta,timezone
from zoneinfo import ZoneInfo
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb
from fastapi import HTTPException
from app.core.db import db_conn
from .availability import slots,instant
from .providers import CalendarProvider, secret_read, secret_write, ProviderError
UTC=timezone.utc

def uid():return str(uuid.uuid4())
def digest(v):return hashlib.sha256(v.encode()).hexdigest()
def public_base():
    value=os.getenv('MEETINGS_PUBLIC_BASE','http://localhost:3000').rstrip('/')
    if not (value.startswith('https://') or value.startswith('http://localhost:')):raise RuntimeError('MEETINGS_PUBLIC_BASE must use HTTPS.')
    return value

def one(cur,sql,args=()):cur.execute(sql,args);return cur.fetchone()
def all_rows(cur,sql,args=()):cur.execute(sql,args);return cur.fetchall()
def owned(cur,table,id,tenant,user):
    if table not in ('meeting_types','meeting_bookings','meeting_connections','meeting_smtp_senders'):raise ValueError('Invalid resource')
    row=one(cur,f'SELECT * FROM {table} WHERE id=%s AND tenant_id=%s AND user_id=%s',(id,tenant,user))
    if not row:raise HTTPException(404,'Not found')
    return row

def profile(cur,tenant,user,lock=False):
    cur.execute('INSERT INTO meeting_profiles(tenant_id,user_id) VALUES(%s,%s) ON CONFLICT DO NOTHING',(tenant,user))
    return one(cur,'SELECT * FROM meeting_profiles WHERE tenant_id=%s AND user_id=%s'+(' FOR UPDATE' if lock else ''),(tenant,user))

def adapter(connection):
    sid=connection['secret_id']
    return CalendarProvider(connection['provider'],secret_read(sid),lambda value:secret_write(sid,value))

def sender(cur,tenant,user):
    p=profile(cur,tenant,user)
    if not p['company_email']:
        raise HTTPException(409,'Configure your company email in Meetings -> Calendar settings.')

    if p['sender_connection_id']:
        c=owned(cur,'meeting_connections',p['sender_connection_id'],tenant,user)
        if c['status']!='connected' or c['email'].lower()!=p['company_email'].lower():
            raise HTTPException(409,'Reconnect your authorized company mailbox.')
        return p,c

    if p.get('smtp_sender_id'):
        smtp_sender=owned(cur,'meeting_smtp_senders',p['smtp_sender_id'],tenant,user)

        if smtp_sender['status']!='connected' or smtp_sender['email'].lower()!=p['company_email'].lower():
            raise HTTPException(409,'Reconnect your authorized company SMTP mailbox.')

        settings=one(
            cur,
            'SELECT e.* FROM tenant_email_settings e JOIN tenants t ON t.slug=e.tenant_slug WHERE t.id=%s',
            (tenant,)
        )

        if not settings or not settings['is_enabled'] or not settings['smtp_host']:
            raise HTTPException(409,'Company SMTP settings are not configured.')

        return p,{
            'sender_type':'smtp',
            'email':smtp_sender['email'],
            'secret_id':smtp_sender['secret_id'],
            'smtp_host':settings['smtp_host'],
            'smtp_port':settings['smtp_port'],
            'tls_mode':settings['tls_mode'],
            'from_name':p['display_name'] or smtp_sender['email'],
        }

    s=one(
        cur,
        'SELECT e.* FROM tenant_email_settings e JOIN tenants t ON t.slug=e.tenant_slug WHERE t.id=%s',
        (tenant,)
    )

    if (
        not s
        or not s['is_enabled']
        or s['from_email'].lower()!=p['company_email'].lower()
        or not s['smtp_secret_id']
    ):
        raise HTTPException(
            409,
            'The company sender is not authorized. Connect its mailbox or ask your administrator.'
        )

    return p,s


def validate_calendars(cur,tenant,user,cfg):
    refs=list(cfg.get('conflicts',[]))+([cfg['destination']] if cfg.get('destination') else [])
    for ref in refs:
        if set(ref)!= {'connection','calendar'}:raise HTTPException(422,'Select a calendar and connection.')
        c=owned(cur,'meeting_connections',ref['connection'],tenant,user)
        calendar=next((x for x in c['calendars'] if x['id']==ref['calendar']),None)
        if not calendar or c['status']!='connected':raise HTTPException(409,'Reconnect and select an available calendar.')
        if ref==cfg.get('destination') and not calendar['can_write']:raise HTTPException(422,'Choose a writable destination calendar.')

def available(cur,t,day,exclude=None):
    cfg=t['config'];zone=ZoneInfo(cfg['timezone']);day=datetime.fromisoformat(day).date()
    start=datetime.combine(day,datetime.min.time(),zone).astimezone(UTC)-timedelta(days=1);end=start+timedelta(days=3)
    busy=[(r['busy_start'],r['busy_end']) for r in all_rows(cur,"SELECT busy_start,busy_end FROM meeting_bookings WHERE tenant_id=%s AND user_id=%s AND status IN ('pending','confirmed','sync_error') AND busy_start<%s AND busy_end>%s AND (%s::uuid IS NULL OR id<>%s::uuid)",(t['tenant_id'],t['user_id'],end,start,exclude,exclude))]
    if one(cur,"SELECT to_regclass('public.meeting_calendar_events') AS present")['present']:
        busy.extend((r['start_at'],r['end_at']) for r in all_rows(cur,
            'SELECT start_at,end_at FROM meeting_calendar_events WHERE tenant_id=%s AND user_id=%s AND start_at<%s AND end_at>%s',
            (t['tenant_id'],t['user_id'],end,start)))
    grouped={}
    refs=list(cfg.get('conflicts',[]))
    if cfg.get('destination') and cfg['destination'] not in refs:refs.append(cfg['destination'])
    for ref in refs:grouped.setdefault(ref['connection'],set()).add(ref['calendar'])
    for cid,cals in grouped.items():
        c=owned(cur,'meeting_connections',cid,t['tenant_id'],t['user_id'])
        if c['status']!='connected':raise ProviderError('The host calendar needs reconnection. Please try later.')
        # Rescheduling does not ignore external events, including the original event.
        original=one(cur,'SELECT external_calendar_id,external_event_id,external_connection_id FROM meeting_bookings WHERE id=%s AND tenant_id=%s AND user_id=%s',(exclude,t['tenant_id'],t['user_id'])) if exclude else None
        skip=(original['external_calendar_id'],original['external_event_id']) if original and str(original['external_connection_id'])==str(cid) else None
        busy.extend(adapter(c).busy(list(cals),start,end,exclude_event=skip))
    return slots(cfg,day,busy)

def notify(cur,b,key,kind,title,message):
    cur.execute('INSERT INTO user_notifications(id,tenant_id,user_id,dedupe_key,kind,title,message,booking_id) VALUES(%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT(tenant_id,user_id,dedupe_key) DO NOTHING',(uid(),b['tenant_id'],b['user_id'],key,kind,title,message,b['id']))

def job(cur,b,kind,recipient,due):
    cur.execute('INSERT INTO meeting_jobs(id,booking_id,version,kind,recipient,due_at) VALUES(%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING',(uid(),b['id'],b['version'],kind,recipient,due))

def schedule(cur,b,kind):
    now=datetime.now(UTC)
    cur.execute("UPDATE meeting_jobs SET status='cancelled',updated_at=now() WHERE booking_id=%s AND version<>%s AND status IN ('pending','retry','processing')",(b['id'],b['version']))
    if b['external_calendar_id']:job(cur,b,'calendar_'+('create' if kind=='booked' else 'cancel' if kind=='cancelled' else 'update'),'provider',now)
    for recipient in ('host','guest'):job(cur,b,kind,recipient,now)
    notify(cur,b,f"{b['id']}:{b['version']}:{kind}",kind,'Meeting '+kind,b['snapshot']['title'])
    if kind!='cancelled':
        for minutes in (1440,60,30):
            due=b['start_at']-timedelta(minutes=minutes)
            for recipient in ('host','guest','internal'):job(cur,b,'reminder_'+str(minutes),recipient,due)
            if due<=now:
                cur.execute("UPDATE meeting_jobs SET status='skipped' WHERE booking_id=%s AND version=%s AND kind=%s AND status='pending'",(b['id'],b['version'],'reminder_'+str(minutes)))
    cur.execute('INSERT INTO meeting_audit(booking_id,event) VALUES(%s,%s)',(b['id'],kind))

def book(cur,t,guest):
    if not t['active']:raise HTTPException(410,'This booking link is inactive.')
    profile(cur,t['tenant_id'],t['user_id'],lock=True)
    t=one(cur,'SELECT * FROM meeting_types WHERE id=%s',(t['id'],))
    if not t['active']:raise HTTPException(410,'This booking link is inactive.')
    existing=one(cur,'SELECT * FROM meeting_bookings WHERE type_id=%s AND idempotency_key=%s',(t['id'],guest.idempotency_key))
    if existing:
        if existing['guest']['email'].lower()!=str(guest.email).lower():raise HTTPException(409,'Request already used.')
        return existing
    p,_=sender(cur,t['tenant_id'],t['user_id']);start=instant(guest.start)
    cfg=t['config'];day=start.astimezone(ZoneInfo(cfg['timezone'])).date().isoformat()
    if cfg.get('destination'):
        destination=owned(cur,'meeting_connections',cfg['destination']['connection'],t['tenant_id'],t['user_id'])
        if destination['email'].lower()!=p['company_email'].lower():
            raise HTTPException(409,'Select a destination calendar connected to your company email. Personal calendars can still be checked for conflicts.')
    if start not in available(cur,t,day):raise HTTPException(409,'That time is no longer available. Choose another time.')
    for q in cfg.get('questions',[]):
        if q['required'] and not guest.answers.get(q['id'],'').strip():raise HTTPException(422,'Please answer: '+q['label'])
    id=uid();token=secrets.token_urlsafe(32);sid=secret_write('inksuite/meetings/manage/'+id,{'token':token})
    dest=cfg.get('destination') or {};end=start+timedelta(minutes=cfg['duration'])
    b=one(cur,"""INSERT INTO meeting_bookings(id,tenant_id,user_id,type_id,guest,start_at,end_at,busy_start,busy_end,host_timezone,status,snapshot,sender_email_snapshot,manage_hash,manage_secret_id,external_connection_id,external_calendar_id,idempotency_key)
      VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING *""",(id,t['tenant_id'],t['user_id'],t['id'],Jsonb(guest.model_dump(mode='json',exclude={'start','idempotency_key'})),start,end,start-timedelta(minutes=cfg['buffer_before']),end+timedelta(minutes=cfg['buffer_after']),cfg['timezone'],'pending' if dest else 'confirmed',Jsonb(cfg),p['company_email'],digest(token),sid,dest.get('connection'),dest.get('calendar'),guest.idempotency_key))
    schedule(cur,b,'booked');return b

def change(cur,b,action,start=None):
    profile(cur,b['tenant_id'],b['user_id'],lock=True)
    b=one(cur,'SELECT * FROM meeting_bookings WHERE id=%s FOR UPDATE',(b['id'],))
    if b['status']=='cancelled':
        if action=='cancel':return b
        raise HTTPException(409,'This meeting has been cancelled.')
    if b['status']!='confirmed':raise HTTPException(409,'Calendar synchronization is pending. Please try again after it finishes.')
    if b['start_at']<=datetime.now(UTC):raise HTTPException(409,'This meeting has already started.')
    if action=='cancel':
        b=one(cur,"UPDATE meeting_bookings SET status='cancelled',version=version+1,updated_at=now() WHERE id=%s RETURNING *",(b['id'],));schedule(cur,b,'cancelled');return b
    t=owned(cur,'meeting_types',b['type_id'],b['tenant_id'],b['user_id'])
    if not t['active']:raise HTTPException(410,'This booking link is inactive. Contact the host.')
    start=instant(start)
    if start==b['start_at']:return b
    day=start.astimezone(ZoneInfo(t['config']['timezone'])).date().isoformat()
    if start not in available(cur,t,day,b['id']):raise HTTPException(409,'That time is unavailable.')
    end=start+timedelta(minutes=t['config']['duration'])
    b=one(cur,"UPDATE meeting_bookings SET start_at=%s,end_at=%s,busy_start=%s,busy_end=%s,status=%s,version=version+1,updated_at=now() WHERE id=%s RETURNING *",(start,end,start-timedelta(minutes=t['config']['buffer_before']),end+timedelta(minutes=t['config']['buffer_after']),'pending' if b['external_calendar_id'] else 'confirmed',b['id']))
    schedule(cur,b,'rescheduled');return b

def signature(cur,tenant,user):
    p=profile(cur,tenant,user);text=p['signature_text'];markup='<br>'.join(html.escape(text).splitlines())
    for link in p['signature_links']:
        t=owned(cur,'meeting_types',link['type_id'],tenant,user);url=public_base()+'/book/'+t['slug'];label=link.get('label') or 'Schedule a meeting'
        style='color:#2563eb;text-decoration:underline;' if link.get('style')!='button' else 'display:inline-block;background:#2563eb;color:#fff;padding:10px 16px;border-radius:6px;text-decoration:none;'
        markup+='<br><a href="'+html.escape(url,quote=True)+'" style="'+style+'">'+html.escape(label)+'</a>'
        text+='\n'+label+': '+url
    return text,markup

