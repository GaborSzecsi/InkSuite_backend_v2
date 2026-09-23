"""Durable delivery for invitations on InkSuite events without an external calendar."""
from datetime import datetime, timezone
from email.message import EmailMessage
from email.utils import formataddr
from psycopg.rows import dict_row
from app.core.db import db_conn
from . import service as s
from .availability import instant

def message(job,profile):
    event=job['payload'];msg=EmailMessage()
    msg['From']=formataddr((profile['display_name'],profile['company_email']))
    msg['To']=job['recipient'];msg['Reply-To']=profile['company_email']
    msg['Subject']=('Cancelled: ' if event.get('cancelled') else 'Meeting invitation / update: ')+event['title']
    msg['Message-ID']=f"<{job['id']}@meetings.inksuite.io>"
    msg.set_content(f"{event['title']}\n\nStarts: {event['start']}\nEnds: {event['end']}\nLocation: {event['location'] or 'To be determined'}\n\nPlease open the attached calendar invitation.")
    def escape(v):return str(v).replace('\\','\\\\').replace('\r','').replace('\n','\\n').replace(';','\\;').replace(',','\\,')
    def stamp(v):return instant(v).strftime('%Y%m%dT%H%M%SZ')
    method='CANCEL' if event.get('cancelled') else 'REQUEST'
    lines=['BEGIN:VCALENDAR','VERSION:2.0','PRODID:-//InkSuite//Meetings//EN','METHOD:'+method,'BEGIN:VEVENT',
           f"UID:{job['event_id']}@inksuite.io",'SEQUENCE:'+str(event['version']),
           'DTSTAMP:'+stamp(datetime.now(timezone.utc)),'DTSTART:'+stamp(event['start']),'DTEND:'+stamp(event['end']),
           'SUMMARY:'+escape(event['title']),'LOCATION:'+escape(event['location']),
           'ORGANIZER:mailto:'+profile['company_email'],
           'STATUS:'+('CANCELLED' if event.get('cancelled') else 'CONFIRMED')]
    lines += ['ATTENDEE;RSVP=TRUE:mailto:'+a['email'] for a in event['attendees']]
    lines += ['END:VEVENT','END:VCALENDAR','']
    # Fold long UTF-8 lines at 75 octets as required by iCalendar.
    folded=[]
    for line in lines:
        chunk=''
        for char in line:
            if len((chunk+char).encode())>75:folded.append(chunk);chunk=' '
            chunk+=char
        folded.append(chunk)
    msg.add_attachment(('\r\n'.join(folded)).encode(),maintype='text',subtype='calendar',filename='meeting.ics',params={'method':method,'charset':'UTF-8'})
    return msg

def send(cur,job):
    p,mailbox=s.sender(cur,job['tenant_id'],job['user_id'])
    msg=message(job,p)
    if p['sender_connection_id']:
        s.adapter(mailbox).send(msg)
        return
    import smtplib
    from routers.contract_invites import _load_smtp_secret
    username,password=_load_smtp_secret(mailbox.get('secret_id') or mailbox.get('smtp_secret_id'))
    client=(smtplib.SMTP_SSL if mailbox['tls_mode']=='ssl' else smtplib.SMTP)(mailbox['smtp_host'],mailbox['smtp_port'],timeout=20)
    with client:
        if mailbox['tls_mode']!='ssl':client.ehlo();client.starttls();client.ehlo()
        client.login(username,password);client.send_message(msg)

def run_once():
    with db_conn() as conn,conn.transaction(),conn.cursor(row_factory=dict_row) as cur:
        if not s.one(cur,"SELECT to_regclass('public.meeting_event_mail') AS present")['present']:return False
        cur.execute("UPDATE meeting_event_mail SET status='uncertain',error='Delivery interrupted. Verify sent mail before inviting again.' WHERE status='processing' AND updated_at<now()-interval '5 minutes'")
        job=s.one(cur,"SELECT * FROM meeting_event_mail WHERE status='pending' ORDER BY created_at FOR UPDATE SKIP LOCKED LIMIT 1")
        if not job:return False
        cur.execute("UPDATE meeting_event_mail SET status='processing',updated_at=now() WHERE id=%s",(job['id'],))
    try:
        with db_conn() as conn,conn.transaction(),conn.cursor(row_factory=dict_row) as cur:
            s.profile(cur,job['tenant_id'],job['user_id'],lock=True)
            state=s.one(cur,'SELECT status FROM meeting_event_mail WHERE id=%s FOR UPDATE',(job['id'],))
            if state['status']!='processing':return True
            send(cur,job)
            cur.execute("UPDATE meeting_event_mail SET status='sent',updated_at=now() WHERE id=%s",(job['id'],))
    except Exception:
        # Do not automatically retry a delivery with an ambiguous SMTP outcome.
        with db_conn() as conn,conn.cursor(row_factory=dict_row) as cur:
            cur.execute("UPDATE meeting_event_mail SET status='uncertain',error='Delivery could not be confirmed. Check sent mail and company email settings.',updated_at=now() WHERE id=%s AND status='processing'",(job['id'],))
        return True
    return True
