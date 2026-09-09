"""Run with python -m app.meetings.worker. Durable DB jobs, no browser timers."""
import html, logging, time
from datetime import datetime,timedelta,timezone
from email.message import EmailMessage
from email.utils import formataddr
from psycopg.rows import dict_row
from app.core.db import db_conn
from . import service as s
from .providers import secret_read
from .availability import instant
from zoneinfo import ZoneInfo
log=logging.getLogger(__name__)

def make_message(cur,b,j,p):
    guest=b['guest'];host=j['recipient']=='host';to=p['company_email'] if host else guest['email'];zone=ZoneInfo(p['timezone'] if host else guest['timezone'])
    when=b['start_at'].astimezone(zone).strftime('%A, %B %d, %Y at %I:%M %p %Z')
    label={'booked':'Meeting confirmed','cancelled':'Meeting cancelled','rescheduled':'Meeting rescheduled'}.get(j['kind'],'Meeting reminder')
    if j['kind'].startswith('reminder_'):label+=' · '+{'1440':'24 hours','60':'1 hour','30':'30 minutes'}[j['kind'].split('_')[1]]
    token=secret_read(b['manage_secret_id'])['token'];manage=s.public_base()+'/meeting/manage/'+token
    text=f"{label}: {b['snapshot']['title']}\n\n{when}\nHost: {p['display_name']} <{p['company_email']}>\nLocation: {b['snapshot'].get('location') or 'To be determined'}\n\nManage or reschedule: {manage}"
    signature_text,signature_html=s.signature(cur,b['tenant_id'],b['user_id'])
    msg=EmailMessage();msg['From']=formataddr((p['display_name'],p['company_email']));msg['Reply-To']=p['company_email'];msg['To']=to;msg['Subject']=label+': '+b['snapshot']['title'];msg['Message-ID']='<'+str(j['id'])+'@meetings.inksuite.io>'
    msg.set_content(text+'\n\n'+signature_text);msg.add_alternative('<p>'+html.escape(text).replace('\n','<br>')+'</p><p>'+signature_html+'</p>',subtype='html')
    def esc(v):return str(v).replace('\\','\\\\').replace('\n','\\n').replace(';','\\;').replace(',','\\,').replace('\r','')
    stamp=lambda d:d.astimezone(timezone.utc).strftime('%Y%m%dT%H%M%SZ')
    ics='\r\n'.join(['BEGIN:VCALENDAR','VERSION:2.0','PRODID:-//InkSuite//Meetings//EN','METHOD:PUBLISH','BEGIN:VEVENT','UID:'+str(b['id'])+'@inksuite.io','SEQUENCE:'+str(b['version']),'DTSTAMP:'+stamp(datetime.now(timezone.utc)),'DTSTART:'+stamp(b['start_at']),'DTEND:'+stamp(b['end_at']),'SUMMARY:'+esc(b['snapshot']['title']),'ORGANIZER:mailto:'+p['company_email'],'LOCATION:'+esc(b['snapshot'].get('location','')),'STATUS:'+('CANCELLED' if b['status']=='cancelled' else 'CONFIRMED'),'END:VEVENT','END:VCALENDAR',''])
    msg.add_attachment(ics.encode(),maintype='text',subtype='calendar',filename='meeting.ics')
    return msg

def run_once():
    now=datetime.now(timezone.utc)
    with db_conn() as conn,conn.transaction(),conn.cursor(row_factory=dict_row) as cur:
        # A process crash during external email delivery has an ambiguous outcome.
        # Never blindly resend it; surface it for review. Calendar event writes are idempotent.
        stale=s.all_rows(cur,"UPDATE meeting_jobs SET status=CASE WHEN kind LIKE 'calendar_%%' THEN 'retry' ELSE 'uncertain' END,error='Worker stopped during delivery; verify provider delivery before retrying.',updated_at=now() WHERE status='processing' AND updated_at<now()-interval '5 minutes' RETURNING *")
        for j in stale:
            b=s.one(cur,'SELECT * FROM meeting_bookings WHERE id=%s',(j['booking_id'],))
            s.notify(cur,b,str(j['id'])+':uncertain','delivery_attention','Meeting delivery needs review','A delivery outcome could not be confirmed. Open Meetings to review.')
        j=s.one(cur,"SELECT * FROM meeting_jobs WHERE status IN ('pending','retry') AND due_at<=now() ORDER BY CASE WHEN kind LIKE 'calendar_%%' THEN 0 ELSE 1 END,due_at FOR UPDATE SKIP LOCKED LIMIT 1")
        if not j:return False
        cur.execute("UPDATE meeting_jobs SET status='processing',attempts=attempts+1,updated_at=now() WHERE id=%s",(j['id'],))
    sending=False
    try:
        with db_conn() as conn,conn.transaction(),conn.cursor(row_factory=dict_row) as cur:
            b=s.one(cur,'SELECT * FROM meeting_bookings WHERE id=%s',(j['booking_id'],))
            # Same lock ordering as booking/cancellation; cannot send after a concurrent cancellation commits.
            s.profile(cur,b['tenant_id'],b['user_id'],lock=True)
            b=s.one(cur,'SELECT * FROM meeting_bookings WHERE id=%s FOR UPDATE',(j['booking_id'],))
            if b['version']!=j['version'] or (j['kind'].startswith('reminder_') and (b['status']=='cancelled' or b['start_at']<=now or now-j['due_at']>timedelta(minutes=10))):
                cur.execute("UPDATE meeting_jobs SET status='skipped',updated_at=now() WHERE id=%s",(j['id'],));return True
            if j['kind'].startswith('calendar_'):
                c=s.owned(cur,'meeting_connections',b['external_connection_id'],b['tenant_id'],b['user_id'])
                if c['status']!='connected':raise RuntimeError('Reconnect the destination calendar.')
                eid=s.adapter(c).event(b,j['kind'].split('_')[1])
                cur.execute("UPDATE meeting_bookings SET external_event_id=%s,status=CASE WHEN status='cancelled' THEN status ELSE 'confirmed' END,error=NULL WHERE id=%s",(eid,b['id']))
            else:
                pending=s.one(cur,"SELECT id FROM meeting_jobs WHERE booking_id=%s AND version=%s AND kind LIKE 'calendar_%%' AND status<>'sent'",(b['id'],b['version']))
                if pending:
                    cur.execute("UPDATE meeting_jobs SET status='retry',due_at=now()+interval '1 minute',updated_at=now() WHERE id=%s",(j['id'],));return True
                if j['recipient']=='internal':
                    s.notify(cur,b,str(j['id']),'reminder','Meeting reminder',j['kind'].split('_')[1]+'-minute reminder: '+b['snapshot']['title'])
                else:
                    p,mailbox=s.sender(cur,b['tenant_id'],b['user_id']);message=make_message(cur,b,j,p)
                    # Record the actual current sender used, independent of the historical booking snapshot.
                    cur.execute('UPDATE meeting_jobs SET sender_email_snapshot=%s WHERE id=%s',(p['company_email'],j['id']))
                    sending=True
                    if p['sender_connection_id']:
                        s.adapter(mailbox).send(message)
                    else:
                        from routers.contract_invites import _load_smtp_secret
                        import smtplib

                        smtp_secret_id=mailbox.get('secret_id') or mailbox.get('smtp_secret_id')
                        if not smtp_secret_id:
                            raise RuntimeError('SMTP sender credentials are not configured.')

                        username,password=_load_smtp_secret(smtp_secret_id)

                        if mailbox['tls_mode']=='ssl':
                            client=smtplib.SMTP_SSL(
                                mailbox['smtp_host'],
                                mailbox['smtp_port'],
                                timeout=20
                            )
                        else:
                            client=smtplib.SMTP(
                                mailbox['smtp_host'],
                                mailbox['smtp_port'],
                                timeout=20
                            )
                            client.ehlo()
                            client.starttls()
                            client.ehlo()

                        with client:
                            client.login(username,password)
                            client.send_message(message)
            cur.execute("UPDATE meeting_jobs SET status='sent',sent_at=now(),updated_at=now(),error=NULL WHERE id=%s",(j['id'],))
            cur.execute('INSERT INTO meeting_audit(booking_id,event) VALUES(%s,%s)',(b['id'],j['kind']+':'+j['recipient']+':sent'))
    except Exception:
        # Do not persist exception messages containing provider responses or credentials.
        state='uncertain' if sending else 'failed' if j['attempts']>=4 else 'retry'
        with db_conn() as conn,conn.transaction(),conn.cursor(row_factory=dict_row) as cur:
            cur.execute("UPDATE meeting_jobs SET status=%s,error=%s,due_at=now()+interval '2 minutes',updated_at=now() WHERE id=%s",(state,'Delivery could not be confirmed; review calendar and company email settings.',j['id']))
            b=s.one(cur,'SELECT * FROM meeting_bookings WHERE id=%s',(j['booking_id'],))
            s.notify(cur,b,str(j['id'])+':failed','delivery_attention','Meeting delivery needs attention','Check Calendar settings and delivery status in Meetings.')
            if j['kind'].startswith('calendar_'):
                cur.execute("UPDATE meeting_bookings SET error='Calendar synchronization needs attention',status=CASE WHEN status='pending' THEN 'sync_error' ELSE status END WHERE id=%s",(b['id'],))
        log.warning('Meeting job %s requires attention',j['id'])
    return True

if __name__=='__main__':
    from pathlib import Path
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).resolve().parents[2]/'.env',override=False)
    logging.basicConfig(level=logging.INFO)
    while True:
        try:
            if not run_once():time.sleep(10)
        except Exception:
            log.warning('Meetings worker could not access its queue. Retrying.');time.sleep(10)
