import secrets, uuid
import requests
from datetime import datetime,timedelta,timezone
from fastapi import APIRouter,Depends,HTTPException,Request
from fastapi.responses import RedirectResponse
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb
from pydantic import BaseModel,Field
from app.core.db import db_conn
from app.tenants.dependencies import require_tenant_access
from . import service as s
from .providers import authorize,exchange,CalendarProvider,secret_read,secret_write,secret_delete,ProviderError
from .schemas import MeetingType,Guest
from .availability import instant

router=APIRouter(tags=['Meetings'])
ROOT='/tenants/{tenant_slug}/meetings'

def context(ctx):return ctx['tenant']['id'],ctx['user']['id']
def serialize_type(t):return {**t['config'],'id':t['id'],'active':t['active'],'public_url':s.public_base()+'/book/'+t['slug']}
def booking_view(b):return {k:b[k] for k in ('id','type_id','guest','start_at','end_at','status','error','snapshot','host_timezone')}
def public_booking(b):return {k:b[k] for k in ('start_at','end_at','status','host_timezone')} | {'title':b['snapshot']['title'],'location':b['snapshot'].get('location',''),'message':b['snapshot'].get('confirmation','')}

def rate(cur,request,key):
    key=s.digest((request.client.host if request.client else 'unknown')+':'+key)
    row=s.one(cur,"""INSERT INTO meeting_rate_limits(key,window_at,hits) VALUES(%s,now(),1)
    ON CONFLICT(key) DO UPDATE SET hits=CASE WHEN meeting_rate_limits.window_at<now()-interval '1 minute' THEN 1 ELSE meeting_rate_limits.hits+1 END, window_at=CASE WHEN meeting_rate_limits.window_at<now()-interval '1 minute' THEN now() ELSE meeting_rate_limits.window_at END RETURNING hits""",(key,))
    if row['hits']>120:raise HTTPException(429,'Too many requests. Try again in a minute.')

def public_type(cur,slug):
    t=s.one(cur,'SELECT * FROM meeting_types WHERE slug=%s',(slug,))
    if not t:raise HTTPException(404,'Booking link not found.')
    if not t['active']:raise HTTPException(410,'This booking link is inactive. Please contact the host.')
    return t

def managed(cur,token):
    b=s.one(cur,'SELECT * FROM meeting_bookings WHERE manage_hash=%s',(s.digest(token),))
    if not b:raise HTTPException(404,'This meeting link is invalid or has been revoked.')
    return b

@router.get(ROOT+'/types')
def types(ctx=Depends(require_tenant_access)):
    tenant,user=context(ctx)
    with db_conn() as conn,conn.cursor(row_factory=dict_row) as cur:
        s.profile(cur,tenant,user)
        return [serialize_type(t) for t in s.all_rows(cur,'SELECT * FROM meeting_types WHERE tenant_id=%s AND user_id=%s ORDER BY created_at DESC',(tenant,user))]

@router.post(ROOT+'/types')
@router.put(ROOT+'/types/{type_id}')
def save_type(body:MeetingType,type_id:uuid.UUID|None=None,ctx=Depends(require_tenant_access)):
    tenant,user=context(ctx)
    with db_conn() as conn,conn.transaction(),conn.cursor(row_factory=dict_row) as cur:
        s.profile(cur,tenant,user,lock=True);s.validate_calendars(cur,tenant,user,body.model_dump())
        if type_id:s.owned(cur,'meeting_types',type_id,tenant,user)
        else:type_id=s.uid()
        if s.one(cur,'SELECT id FROM meeting_types WHERE slug=%s AND id<>%s',(body.slug,type_id)):raise HTTPException(409,'That public address is already in use.')
        t=s.one(cur,'INSERT INTO meeting_types(id,tenant_id,user_id,slug,config,active) VALUES(%s,%s,%s,%s,%s,%s) ON CONFLICT(id) DO UPDATE SET slug=EXCLUDED.slug,config=EXCLUDED.config,active=EXCLUDED.active,updated_at=now() RETURNING *',(type_id,tenant,user,body.slug,Jsonb(body.model_dump()),body.active))
        return serialize_type(t)

@router.get(ROOT+'/profile')
def get_profile(ctx=Depends(require_tenant_access)):
    tenant,user=context(ctx)
    with db_conn() as conn,conn.cursor(row_factory=dict_row) as cur:
        p=s.profile(cur,tenant,user);text,markup=s.signature(cur,tenant,user)
        return {k:p[k] for k in ('timezone','display_name','company_email','sender_connection_id','smtp_sender_id','signature_text','signature_links','availability')} | {'preview_text':text,'preview_html':markup}

@router.get(ROOT+'/smtp-senders')
def smtp_senders(ctx=Depends(require_tenant_access)):
    tenant,user=context(ctx)
    with db_conn() as conn,conn.cursor(row_factory=dict_row) as cur:
        s.profile(cur,tenant,user)
        return s.all_rows(
            cur,
            '''SELECT id,email,status,created_at,updated_at
               FROM meeting_smtp_senders
               WHERE tenant_id=%s AND user_id=%s
               ORDER BY created_at''',
            (tenant,user),
        )

class SmtpSenderIn(BaseModel):
    email:str=Field(min_length=3,max_length=320)
    password:str=Field(min_length=1,max_length=1000)

@router.post(ROOT+'/smtp-senders')
def save_smtp_sender(body:SmtpSenderIn,ctx=Depends(require_tenant_access)):
    import smtplib

    tenant,user=context(ctx)
    email=body.email.strip().lower()

    if '@' not in email or email.startswith('@') or email.endswith('@'):
        raise HTTPException(422,'Enter a valid company email address.')

    # Read the tenant SMTP server configuration. Individual mailbox
    # credentials are stored separately in Secrets Manager.
    with db_conn() as conn,conn.cursor(row_factory=dict_row) as cur:
        s.profile(cur,tenant,user)
        settings=s.one(
            cur,
            '''SELECT e.*
               FROM tenant_email_settings e
               JOIN tenants t ON t.slug=e.tenant_slug
               WHERE t.id=%s''',
            (tenant,),
        )

    if not settings or not settings['is_enabled'] or not settings['smtp_host']:
        raise HTTPException(409,'Company SMTP settings are not configured.')

    company_email=(settings['from_email'] or '').strip().lower()
    if '@' not in company_email:
        raise HTTPException(409,'Company email domain is not configured.')

    if email.rsplit('@',1)[-1] != company_email.rsplit('@',1)[-1]:
        raise HTTPException(422,'Use an email address on your company email domain.')

    host=settings['smtp_host']
    port=settings['smtp_port']
    tls_mode=settings['tls_mode']

    # Verify the credentials before saving them.
    try:
        if tls_mode=='ssl':
            client=smtplib.SMTP_SSL(host,port,timeout=20)
        else:
            client=smtplib.SMTP(host,port,timeout=20)
            client.ehlo()
            client.starttls()
            client.ehlo()

        with client:
            client.login(email,body.password)

    except smtplib.SMTPAuthenticationError:
        raise HTTPException(422,'The mailbox login was rejected. Check the email address and password.')
    except (smtplib.SMTPException,OSError) as exc:
        raise HTTPException(502,'Could not connect to the company mail server.') from exc

    sender_id=s.uid()
    secret_name=f'inksuite/meetings/smtp/{tenant}/{user}'

    try:
        secret_id=secret_write(
            secret_name,
            {'username':email,'password':body.password},
        )

        with db_conn() as conn,conn.transaction(),conn.cursor(row_factory=dict_row) as cur:
            s.profile(cur,tenant,user,lock=True)

            existing=s.one(
                cur,
                '''SELECT *
                   FROM meeting_smtp_senders
                   WHERE tenant_id=%s AND user_id=%s
                   ORDER BY created_at
                   LIMIT 1
                   FOR UPDATE''',
                (tenant,user),
            )

            if existing:
                sender_id=existing['id']
                cur.execute(
                    '''UPDATE meeting_smtp_senders
                       SET email=%s,secret_id=%s,status='connected',updated_at=now()
                       WHERE id=%s''',
                    (email,secret_id,sender_id),
                )
            else:
                cur.execute(
                    '''INSERT INTO meeting_smtp_senders
                       (id,tenant_id,user_id,email,secret_id,status)
                       VALUES(%s,%s,%s,%s,%s,'connected')''',
                    (sender_id,tenant,user,email,secret_id),
                )

            cur.execute(
                '''UPDATE meeting_profiles
                   SET company_email=%s,
                       sender_connection_id=NULL,
                       smtp_sender_id=%s,
                       updated_at=now()
                   WHERE tenant_id=%s AND user_id=%s''',
                (email,sender_id,tenant,user),
            )

    except HTTPException:
        raise
    except Exception:
        # Do not expose infrastructure/database details to the client.
        raise HTTPException(500,'The mailbox was verified but could not be saved.')

    return {
        'id':sender_id,
        'email':email,
        'status':'connected',
    }

@router.delete(ROOT+'/smtp-senders/{sender_id}')
def delete_smtp_sender(sender_id:uuid.UUID,ctx=Depends(require_tenant_access)):
    tenant,user=context(ctx)

    with db_conn() as conn,conn.transaction(),conn.cursor(row_factory=dict_row) as cur:
        sender=s.one(
            cur,
            '''SELECT *
               FROM meeting_smtp_senders
               WHERE id=%s AND tenant_id=%s AND user_id=%s
               FOR UPDATE''',
            (sender_id,tenant,user),
        )

        if not sender:
            raise HTTPException(404,'SMTP sender not found.')

        cur.execute(
            '''UPDATE meeting_profiles
               SET smtp_sender_id=NULL,updated_at=now()
               WHERE tenant_id=%s AND user_id=%s AND smtp_sender_id=%s''',
            (tenant,user,sender_id),
        )

        cur.execute(
            '''DELETE FROM meeting_smtp_senders
               WHERE id=%s AND tenant_id=%s AND user_id=%s''',
            (sender_id,tenant,user),
        )

        secret_id=sender['secret_id']

    # DB disconnect is authoritative. Remove the credential afterward.
    # A Secrets Manager failure must not reconnect the mailbox.
    try:
        secret_delete(secret_id)
    except Exception:
        pass

    return {'ok':True}


class ProfileIn(BaseModel):
    timezone:str='America/Los_Angeles'
    display_name:str=Field(default='',max_length=120)
    sender_connection_id:uuid.UUID|None=None
    smtp_sender_id:uuid.UUID|None=None
    signature_text:str=Field(default='',max_length=3000)
    signature_links:list[dict]=Field(default_factory=list,max_length=5)
    availability:dict=Field(default_factory=dict)

@router.put(ROOT+'/profile')
def save_profile(body:ProfileIn,ctx=Depends(require_tenant_access)):
    from zoneinfo import ZoneInfo
    try:ZoneInfo(body.timezone)
    except Exception:raise HTTPException(422,'Choose an IANA timezone.')
    tenant,user=context(ctx)
    with db_conn() as conn,conn.transaction(),conn.cursor(row_factory=dict_row) as cur:
        s.profile(cur,tenant,user,lock=True)
        email=''
        if body.sender_connection_id and body.smtp_sender_id:
            raise HTTPException(422,'Choose either an OAuth mailbox or an SMTP mailbox, not both.')

        if body.sender_connection_id:
            c=s.owned(cur,'meeting_connections',body.sender_connection_id,tenant,user)
            if c['status']!='connected':raise HTTPException(409,'Reconnect your company mailbox.')
            email=c['email']
            organization=s.one(cur,'SELECT e.from_email FROM tenant_email_settings e JOIN tenants t ON e.tenant_slug=t.slug WHERE t.id=%s',(tenant,))
            if organization and '@' in organization['from_email'] and email.rsplit('@',1)[-1].lower()!=organization['from_email'].rsplit('@',1)[-1].lower():
                raise HTTPException(422,'Select an authorized mailbox on your company email domain. Personal calendars can still be checked for conflicts.')

        elif body.smtp_sender_id:
            smtp_sender=s.owned(cur,'meeting_smtp_senders',body.smtp_sender_id,tenant,user)
            if smtp_sender['status']!='connected':
                raise HTTPException(409,'Reconnect your company SMTP mailbox.')
            email=smtp_sender['email']
            organization=s.one(cur,'SELECT e.from_email FROM tenant_email_settings e JOIN tenants t ON e.tenant_slug=t.slug WHERE t.id=%s',(tenant,))
            if organization and '@' in organization['from_email'] and email.rsplit('@',1)[-1].lower()!=organization['from_email'].rsplit('@',1)[-1].lower():
                raise HTTPException(422,'Select an authorized mailbox on your company email domain.')

        else:
            settings=s.one(cur,'SELECT e.from_email FROM tenant_email_settings e JOIN tenants t ON e.tenant_slug=t.slug WHERE t.id=%s AND e.is_enabled=true',(tenant,))
            if settings and settings['from_email'].lower()==ctx['user']['email'].lower():
                email=settings['from_email']
        for link in body.signature_links:
            if set(link)-{'type_id','label','style'}:raise HTTPException(422,'Invalid signature link.')
            try:uuid.UUID(str(link.get('type_id')))
            except ValueError:raise HTTPException(422,'Choose a booking link.')
            s.owned(cur,'meeting_types',link['type_id'],tenant,user)
            if len(str(link.get('label','')))>100 or link.get('style','text') not in ('text','button'):raise HTTPException(422,'Invalid signature display style.')
        # Reusable availability is validated with the same window schema as a booking link.
        MeetingType(title='Availability',slug='availability-profile',timezone=body.timezone,weekly=body.availability)
        cur.execute('UPDATE meeting_profiles SET timezone=%s,display_name=%s,company_email=%s,sender_connection_id=%s,smtp_sender_id=%s,signature_text=%s,signature_links=%s,availability=%s,updated_at=now() WHERE tenant_id=%s AND user_id=%s',(body.timezone,body.display_name,email,body.sender_connection_id,body.smtp_sender_id,body.signature_text,Jsonb(body.signature_links),Jsonb(body.availability),tenant,user))
    return get_profile(ctx)

@router.get(ROOT+'/bookings')
def bookings(past:bool=False,ctx=Depends(require_tenant_access)):
    tenant,user=context(ctx)
    with db_conn() as conn,conn.cursor(row_factory=dict_row) as cur:
        rows=s.all_rows(cur,'SELECT * FROM meeting_bookings WHERE tenant_id=%s AND user_id=%s AND (start_at<now())=%s ORDER BY start_at '+('DESC' if past else 'ASC')+' LIMIT 200',(tenant,user,past))
        return [booking_view(b) for b in rows]

@router.get(ROOT+'/header-summary')
def summary(day:str|None=None,ctx=Depends(require_tenant_access)):
    from zoneinfo import ZoneInfo
    tenant,user=context(ctx)
    with db_conn() as conn,conn.cursor(row_factory=dict_row) as cur:
        p=s.profile(cur,tenant,user);zone=ZoneInfo(p['timezone'])
        try:selected=datetime.fromisoformat(day).date() if day else datetime.now(zone).date()
        except ValueError:raise HTTPException(422,'Choose a date.')
        start=datetime.combine(selected,datetime.min.time(),zone);end=start+timedelta(days=1)
        rows=s.all_rows(cur,"SELECT * FROM meeting_bookings WHERE tenant_id=%s AND user_id=%s AND start_at>=now() AND status<>'cancelled' ORDER BY start_at LIMIT 10",(tenant,user))
        daily=s.all_rows(cur,"SELECT * FROM meeting_bookings WHERE tenant_id=%s AND user_id=%s AND start_at>=%s AND start_at<%s AND status<>'cancelled' ORDER BY start_at LIMIT 100",(tenant,user,start,end))
        return {'upcoming':[booking_view(b) for b in rows],'day_bookings':[booking_view(b) for b in daily],'timezone':p['timezone']}

@router.get(ROOT+'/bookings/{booking_id}')
def booking_detail(booking_id:uuid.UUID,ctx=Depends(require_tenant_access)):
    tenant,user=context(ctx)
    with db_conn() as conn,conn.cursor(row_factory=dict_row) as cur:
        return booking_view(s.owned(cur,'meeting_bookings',booking_id,tenant,user))

class ChangeIn(BaseModel):
    start:str|None=None
@router.post(ROOT+'/bookings/{booking_id}/{action}')
def host_change(booking_id:uuid.UUID,action:str,body:ChangeIn,ctx=Depends(require_tenant_access)):
    if action not in ('cancel','reschedule'):raise HTTPException(404)
    if action=='reschedule':
        try:instant(body.start)
        except (ValueError,TypeError):raise HTTPException(422,'Choose a valid time with a timezone.')
    tenant,user=context(ctx)
    with db_conn() as conn,conn.transaction(),conn.cursor(row_factory=dict_row) as cur:
        b=s.owned(cur,'meeting_bookings',booking_id,tenant,user)
        return booking_view(s.change(cur,b,action,body.start))

@router.get(ROOT+'/notifications')
def notifications(ctx=Depends(require_tenant_access)):
    tenant,user=context(ctx)
    with db_conn() as conn,conn.cursor(row_factory=dict_row) as cur:
        return s.all_rows(cur,'SELECT id,kind,title,message,booking_id,read_at,created_at FROM user_notifications WHERE tenant_id=%s AND user_id=%s ORDER BY created_at DESC LIMIT 50',(tenant,user))
@router.post(ROOT+'/notifications/{id}/read')
def read_notification(id:uuid.UUID,ctx=Depends(require_tenant_access)):
    tenant,user=context(ctx)
    with db_conn() as conn,conn.cursor() as cur:cur.execute('UPDATE user_notifications SET read_at=now() WHERE id=%s AND tenant_id=%s AND user_id=%s',(id,tenant,user))
    return {'ok':True}

@router.get(ROOT+'/connections')
def connections(ctx=Depends(require_tenant_access)):
    tenant,user=context(ctx)
    with db_conn() as conn,conn.cursor(row_factory=dict_row) as cur:
        return s.all_rows(cur,"SELECT id,provider,email,status,calendars FROM meeting_connections WHERE tenant_id=%s AND user_id=%s AND status='connected'",(tenant,user))

@router.post(ROOT+'/connections/{id}/refresh')
def refresh_calendars(id:uuid.UUID,ctx=Depends(require_tenant_access)):
    tenant,user=context(ctx)
    with db_conn() as conn,conn.cursor(row_factory=dict_row) as cur:
        c=s.owned(cur,'meeting_connections',id,tenant,user);data=s.adapter(c).calendars()
        cur.execute("UPDATE meeting_connections SET calendars=%s,status='connected' WHERE id=%s",(Jsonb(data),id))
    return {'ok':True}

@router.delete(ROOT+'/connections/{id}')
def disconnect(id:uuid.UUID,ctx=Depends(require_tenant_access)):
    tenant,user=context(ctx)

    # Read the OAuth credential before changing local state.
    with db_conn() as conn,conn.cursor(row_factory=dict_row) as cur:
        c=s.owned(cur,'meeting_connections',id,tenant,user)
        credentials=secret_read(c['secret_id'])

    if c['provider']=='google':
        token=credentials.get('refresh_token') or credentials.get('access_token')
        if token:
            try:
                response=requests.post(
                    'https://oauth2.googleapis.com/revoke',
                    data={'token':token},
                    timeout=20,
                )
            except requests.RequestException as exc:
                raise HTTPException(503,'Google could not revoke the calendar connection. Please try again.') from exc

            # Google can return 400 when the token is already invalid/revoked.
            if response.status_code not in (200,400):
                raise HTTPException(503,'Google could not revoke the calendar connection. Please try again.')

    elif c['provider']=='microsoft':
        # Do not present a local-only removal as a full provider disconnect.
        raise HTTPException(501,'Full Microsoft calendar revocation is not configured yet.')

    # Keep the connection row for historical booking references, but make it
    # inactive and detach it from the profile.
    with db_conn() as conn,conn.transaction(),conn.cursor(row_factory=dict_row) as cur:
        c=s.owned(cur,'meeting_connections',id,tenant,user)
        cur.execute(
            '''UPDATE meeting_profiles
               SET sender_connection_id=NULL,updated_at=now()
               WHERE tenant_id=%s AND user_id=%s AND sender_connection_id=%s''',
            (tenant,user,id),
        )
        cur.execute(
            "UPDATE meeting_connections SET status='disconnected',updated_at=now() WHERE id=%s",
            (id,),
        )

    try:
        secret_delete(c['secret_id'])
    except Exception:
        # Provider access is already revoked and the DB connection is inactive.
        pass

    return {'ok':True}

@router.post(ROOT+'/connect/{provider}')
def connect(provider:str,ctx=Depends(require_tenant_access)):
    tenant,user=context(ctx);state=secrets.token_urlsafe(32);verifier=secrets.token_urlsafe(48)
    url=authorize(provider,state,verifier)
    with db_conn() as conn,conn.cursor(row_factory=dict_row) as cur:
        s.profile(cur,tenant,user);sid=secret_write('inksuite/meetings/oauth/'+s.digest(state),{'verifier':verifier})
        cur.execute('INSERT INTO meeting_oauth_states(state_hash,tenant_id,user_id,provider,verifier_secret_id,expires_at) VALUES(%s,%s,%s,%s,%s,now()+interval \'10 minutes\')',(s.digest(state),tenant,user,provider,sid))
    return {'url':url}

@router.get('/meetings/oauth/{provider}/callback')
def callback(provider:str,state:str,code:str=''):
    with db_conn() as conn,conn.transaction(),conn.cursor(row_factory=dict_row) as cur:
        entry=s.one(cur,'DELETE FROM meeting_oauth_states WHERE state_hash=%s AND provider=%s AND expires_at>now() RETURNING *',(s.digest(state),provider))
        if not entry:raise HTTPException(400,'The connection request expired. Please start again.')
    try:
        credentials=exchange(provider,code,secret_read(entry['verifier_secret_id'])['verifier']);a=CalendarProvider(provider,credentials);identity=a.identity();calendars=a.calendars()
        with db_conn() as conn,conn.transaction(),conn.cursor(row_factory=dict_row) as cur:
            prior=s.one(cur,'SELECT id FROM meeting_connections WHERE tenant_id=%s AND user_id=%s AND provider=%s AND account_id=%s',(entry['tenant_id'],entry['user_id'],provider,identity['id']))
            id=str(prior['id']) if prior else s.uid();sid=secret_write('inksuite/meetings/calendar/'+id,credentials)
            cur.execute("INSERT INTO meeting_connections(id,tenant_id,user_id,provider,account_id,email,secret_id,calendars) VALUES(%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT(id) DO UPDATE SET email=EXCLUDED.email,secret_id=EXCLUDED.secret_id,calendars=EXCLUDED.calendars,status='connected'",(id,entry['tenant_id'],entry['user_id'],provider,identity['id'],identity['email'],sid,Jsonb(calendars)))
    finally:secret_delete(entry['verifier_secret_id'])
    return RedirectResponse(s.public_base()+'/app/meetings?view=settings',303)

@router.get('/public/meetings/{slug}')
def public_info(slug:str,request:Request):
    with db_conn() as conn,conn.cursor(row_factory=dict_row) as cur:
        rate(cur,request,'lookup');t=public_type(cur,slug);p=s.profile(cur,t['tenant_id'],t['user_id'])
        keys=('title','description','duration','timezone','location_type','questions','horizon')
        return {k:t['config'][k] for k in keys}|{'host':p['display_name']}

@router.get('/public/meetings/{slug}/slots')
def public_slots(slug:str,day:str,request:Request):
    with db_conn() as conn,conn.cursor(row_factory=dict_row) as cur:
        rate(cur,request,'slots');t=public_type(cur,slug)
        try:date=datetime.fromisoformat(day).date()
        except ValueError:raise HTTPException(422,'Choose a date.')
        now=datetime.now(timezone.utc).date()
        if abs((date-now).days)>366:raise HTTPException(422,'Date is outside the booking horizon.')
        return {'slots':s.available(cur,t,date.isoformat())}

@router.post('/public/meetings/{slug}/book')
def public_book(slug:str,body:Guest,request:Request):
    with db_conn() as conn,conn.cursor(row_factory=dict_row) as cur:rate(cur,request,'book')
    with db_conn() as conn,conn.transaction(),conn.cursor(row_factory=dict_row) as cur:
        b=s.book(cur,public_type(cur,slug),body)
        token=secret_read(b['manage_secret_id'])['token']
        return public_booking(b)|{'manage_url':s.public_base()+'/meeting/manage/'+token}

@router.get('/public/meeting-management/{token}')
def manage_info(token:str,request:Request):
    with db_conn() as conn,conn.cursor(row_factory=dict_row) as cur:
        rate(cur,request,'manage');b=managed(cur,token);t=s.one(cur,'SELECT slug FROM meeting_types WHERE id=%s',(b['type_id'],))
        return public_booking(b)|{'slug':t['slug']}

@router.post('/public/meeting-management/{token}/{action}')
def manage_change(token:str,action:str,body:ChangeIn,request:Request):
    if action not in ('cancel','reschedule'):raise HTTPException(404)
    if action=='reschedule':
        try:instant(body.start)
        except (ValueError,TypeError):raise HTTPException(422,'Choose a valid time with a timezone.')
    with db_conn() as conn,conn.cursor(row_factory=dict_row) as cur:rate(cur,request,'manage-change')
    with db_conn() as conn,conn.transaction(),conn.cursor(row_factory=dict_row) as cur:return public_booking(s.change(cur,managed(cur,token),action,body.start))
@router.get(ROOT+'/bookings/{booking_id}/delivery')
def delivery(booking_id:uuid.UUID,ctx=Depends(require_tenant_access)):
    tenant,user=context(ctx)
    with db_conn() as conn,conn.cursor(row_factory=dict_row) as cur:
        s.owned(cur,'meeting_bookings',booking_id,tenant,user)
        return s.all_rows(cur,'SELECT id,kind,recipient,due_at,status,sent_at,error,attempts FROM meeting_jobs WHERE booking_id=%s ORDER BY version DESC,due_at',(booking_id,))

class RetryIn(BaseModel):verified_not_delivered:bool=False
@router.post(ROOT+'/jobs/{job_id}/retry')
def retry_job(job_id:uuid.UUID,body:RetryIn,ctx=Depends(require_tenant_access)):
    tenant,user=context(ctx)
    with db_conn() as conn,conn.transaction(),conn.cursor(row_factory=dict_row) as cur:
        j=s.one(cur,'SELECT j.* FROM meeting_jobs j JOIN meeting_bookings b ON b.id=j.booking_id WHERE j.id=%s AND b.tenant_id=%s AND b.user_id=%s FOR UPDATE OF j',(job_id,tenant,user))
        if not j:raise HTTPException(404)
        if j['status'] not in ('failed','uncertain'):raise HTTPException(409,'This delivery is not awaiting retry.')
        if j['status']=='uncertain' and not body.verified_not_delivered:raise HTTPException(409,'Verify that the provider did not deliver this message before retrying.')
        cur.execute("UPDATE meeting_jobs SET status='retry',attempts=0,due_at=now(),updated_at=now() WHERE id=%s",(job_id,))
    return {'ok':True}
