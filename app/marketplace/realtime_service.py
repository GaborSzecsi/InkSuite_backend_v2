"""Authenticated WebSockets using short-lived, single-use tickets and shared PostgreSQL LISTEN/NOTIFY.
Only invalidation metadata travels over the socket; authorized REST is the source of truth.
"""
import asyncio
import hashlib
import json
import os
import secrets
import time
from threading import Event
from uuid import UUID, uuid4
import anyio
import psycopg
from fastapi import APIRouter, Depends, HTTPException, WebSocket, WebSocketDisconnect, Query
from starlette.concurrency import run_in_threadpool
from app.db.db import _normalize_database_url
from .core import transaction, actor, one, rows, required, current_user, public_actor

router = APIRouter()


def ready(cur):
    if not one(cur,"SELECT to_regclass('public.marketplace_messaging_sessions') AS ready")['ready']:
        raise HTTPException(503,"Messaging is awaiting migration 035_marketplace_messaging.sql.")


def notify_presence(cur, actor_id, user_id):
    # Refresh only accepted peers of this actor and the session owner's personal identity.
    peers = rows(cur,"""SELECT DISTINCT CASE WHEN c.requester_actor_id=a.id THEN c.recipient_actor_id
      ELSE c.requester_actor_id END AS id FROM marketplace_actors a JOIN marketplace_connections c
      ON a.id IN (c.requester_actor_id,c.recipient_actor_id)
      WHERE (a.id=%s OR a.user_id=%s) AND c.status='accepted'""",(actor_id,user_id))
    if peers:
        cur.execute("SELECT pg_notify('marketplace_messaging',%s)",
                    (json.dumps({'type':'presence','actors':[str(p['id']) for p in peers]}),))


def page_heartbeat(actor_id: UUID, session_id: UUID, user):
    with transaction() as cur:
        actor(cur,user,actor_id,messaging=True)
        ready(cur)
        existing = one(cur,'SELECT actor_id,user_id FROM marketplace_messaging_sessions WHERE id=%s',(session_id,))
        if existing and (str(existing['actor_id']) != str(actor_id) or str(existing['user_id']) != str(user['id'])):
            raise HTTPException(403,'This session belongs to another identity.')
        was_online = one(cur,"SELECT 1 FROM marketplace_messaging_sessions WHERE user_id=%s AND connected AND heartbeat_at>now()-interval '90 seconds' LIMIT 1",(user['id'],))
        if not existing:
            cur.execute("DELETE FROM marketplace_messaging_sessions WHERE heartbeat_at<now()-interval '5 minutes'")
            count = one(cur,'SELECT count(*) AS n FROM marketplace_messaging_sessions WHERE user_id=%s',(user['id'],))['n']
            if count>=20:
                raise HTTPException(429,'Too many messaging sessions.')
        cur.execute("""INSERT INTO marketplace_messaging_sessions(id,actor_id,user_id,connected)
          VALUES(%s,%s,%s,true) ON CONFLICT(id) DO UPDATE SET connected=true,heartbeat_at=now()
          WHERE marketplace_messaging_sessions.actor_id=EXCLUDED.actor_id AND marketplace_messaging_sessions.user_id=EXCLUDED.user_id""",
          (session_id,actor_id,user['id']))
        if not was_online:
            notify_presence(cur,actor_id,user['id'])
        return {'ok':True}


def end_page_session(actor_id: UUID, session_id: UUID, user):
    with transaction() as cur:
        actor(cur,user,actor_id,messaging=True)
        ready(cur)
        cur.execute('DELETE FROM marketplace_messaging_sessions WHERE id=%s AND actor_id=%s AND user_id=%s',
                    (session_id,actor_id,user['id']))
        notify_presence(cur,actor_id,user['id'])
        return {'ok':True}


def session(actor_id: UUID, user=Depends(current_user)):
    ticket = secrets.token_urlsafe(32)
    with transaction() as cur:
        actor(cur,user,actor_id,messaging=True)
        ready(cur)
        # Prune ephemeral rows only, never conversation history.
        cur.execute("DELETE FROM marketplace_messaging_sessions WHERE heartbeat_at<now()-interval '5 minutes'")
        count = one(cur,"SELECT count(*) AS n FROM marketplace_messaging_sessions WHERE user_id=%s",(user['id'],))['n']
        if count >= 20:
            raise HTTPException(429,'Too many messaging sessions. Close unused tabs and retry.')
        cur.execute("INSERT INTO marketplace_messaging_sessions(actor_id,user_id,ticket_hash) VALUES(%s,%s,%s)",
                    (actor_id,user['id'],hashlib.sha256(ticket.encode()).hexdigest()))
    return {'ticket':ticket}


def consume(ticket):
    if not isinstance(ticket,str) or len(ticket)>128:
        raise HTTPException(401,'Invalid messaging ticket.')
    with transaction() as cur:
        s = one(cur,"""UPDATE marketplace_messaging_sessions SET ticket_hash=NULL,connected=true,heartbeat_at=now()
          WHERE ticket_hash=%s AND ticket_expires_at>now() AND NOT connected RETURNING id,actor_id,user_id""",
          (hashlib.sha256(ticket.encode()).hexdigest(),))
        if not s:
            raise HTTPException(401,'Expired messaging ticket.')
        user = one(cur,'SELECT id,platform_role FROM users WHERE id=%s',(s['user_id'],))
        actor(cur,user,s['actor_id'],messaging=True)
        notify_presence(cur,s['actor_id'],s['user_id'])
        s['user'] = user
        return s


def heartbeat(s, update=True):
    with transaction() as cur:
        user = required(one(cur,'SELECT id,platform_role FROM users WHERE id=%s',(s['user_id'],)))
        actor(cur,user,s['actor_id'],messaging=True)
        if not update:
            return
        cur.execute('UPDATE marketplace_messaging_sessions SET heartbeat_at=now() WHERE id=%s',(s['id'],))


def disconnect(s):
    with transaction() as cur:
        cur.execute('DELETE FROM marketplace_messaging_sessions WHERE id=%s',(s['id'],))
        notify_presence(cur,s['actor_id'],s['user_id'])


def connections(actor_id: UUID, q: str = Query('',max_length=200), offset: int = Query(0,ge=0,le=1000000), user=Depends(current_user)):
    with transaction() as cur:
        actor(cur,user,actor_id,messaging=True)
        ready(cur)
        pattern = '%' + q.replace('\\','\\\\').replace('%','\\%').replace('_','\\_') + '%'
        data = rows(cur,"""SELECT a.id,EXISTS(SELECT 1 FROM marketplace_messaging_sessions s WHERE (s.actor_id=a.id OR (a.user_id IS NOT NULL AND s.user_id=a.user_id))
          AND s.connected AND s.heartbeat_at>now()-interval '90 seconds') AS online,
          (SELECT max(c.last_message_at) FROM marketplace_conversations c WHERE
          (c.actor_low_id=%s AND c.actor_high_id=a.id) OR (c.actor_high_id=%s AND c.actor_low_id=a.id)) AS contacted
          FROM marketplace_connections cn JOIN marketplace_actors a ON a.id=CASE WHEN cn.requester_actor_id=%s
          THEN cn.recipient_actor_id ELSE cn.requester_actor_id END
          LEFT JOIN marketplace_profiles p ON p.user_id=a.user_id LEFT JOIN marketplace_organizations o ON o.id=a.organization_id
          WHERE cn.status='accepted' AND %s IN (cn.requester_actor_id,cn.recipient_actor_id) AND a.status='active'
          AND (a.organization_id IS NULL OR o.status='active')
          AND NOT EXISTS(SELECT 1 FROM marketplace_blocks b WHERE (b.blocker_actor_id=%s AND b.blocked_actor_id=a.id)
          OR (b.blocked_actor_id=%s AND b.blocker_actor_id=a.id))
          AND (COALESCE(p.display_name,o.name,'') ILIKE %s OR p.username ILIKE %s)
          ORDER BY online DESC,contacted DESC NULLS LAST,COALESCE(p.display_name,o.name,''),a.id LIMIT 16 OFFSET %s""",
          (actor_id,actor_id,actor_id,actor_id,actor_id,actor_id,pattern,pattern,offset))
        return {'items':[dict(public_actor(cur,d['id'],viewer=actor_id),online=d['online']) for d in data[:15]],'has_more':len(data)>15}


# One DB listener per process/event loop, rather than one per connected browser.
class Broker:
    def __init__(self):
        self.clients = {}
        self.task = None
        self.stop = Event()

    def join(self, actor_id):
        key = str(uuid4())
        queue = asyncio.Queue(maxsize=32)
        self.clients[key] = (str(actor_id),queue)
        if self.task is None or self.task.done():
            self.stop = Event()
            self.task = asyncio.create_task(self.listen())
        return key,queue

    def leave(self,key):
        self.clients.pop(key,None)
        if not self.clients and self.task:
            self.stop.set()
            self.task.cancel()
            self.task = None

    async def listen(self):
        # A blocking listener in one dedicated thread also supports Windows'
        # default Proactor loop, which psycopg AsyncConnection does not support.
        await asyncio.to_thread(self.listen_sync,asyncio.get_running_loop(),self.stop)

    def listen_sync(self,loop,stop):
        while not stop.is_set():
            try:
                dsn = _normalize_database_url(os.environ.get('DATABASE_URL',''))
                with psycopg.connect(dsn,autocommit=True,connect_timeout=5) as conn:
                    conn.execute('LISTEN marketplace_messaging')
                    loop.call_soon_threadsafe(self.publish,{'type':'refresh'})
                    while not stop.is_set():
                        for notification in conn.notifies(timeout=1):
                            loop.call_soon_threadsafe(self.publish,json.loads(notification.payload))
            except (psycopg.Error,ValueError):
                # No credentials, tickets or message bodies enter application logs.
                stop.wait(2)
            except RuntimeError:
                return

    def publish(self,event):
        recipients = event.get('actors')
        for actor_id,queue in list(self.clients.values()):
            if recipients and actor_id not in recipients:
                continue
            if queue.full():
                queue.get_nowait()
            queue.put_nowait({'type':event['type'],'conversation_id':event.get('conversation_id'),'incoming':event.get('incoming',False),'sender_actor_id':event.get('sender_actor_id')})

_brokers = {}

async def live(ws: WebSocket):
    # Browser Origin is explicit, fail closed in production.
    allowed = {o.strip().rstrip('/') for o in os.environ.get('ALLOW_ORIGINS','').split(',') if o.strip()}
    origin = ws.headers.get('origin','').rstrip('/')
    if origin not in allowed:
        await ws.close(code=1008)
        return
    await ws.accept()
    s = None
    broker = None
    key = None
    incoming = None
    outgoing = None
    try:
        frame = await asyncio.wait_for(ws.receive_text(),10)
        if len(frame)>1024:
            raise HTTPException(401,'Invalid messaging frame.')
        payload = json.loads(frame)
        if not isinstance(payload,dict):
            raise HTTPException(401,'Invalid messaging frame.')
        s = await run_in_threadpool(consume,payload.get('ticket'))
        loop = asyncio.get_running_loop()
        broker = _brokers.setdefault(loop,Broker())
        key,queue = broker.join(s['actor_id'])
        await ws.send_json({'type':'ready'})
        incoming = asyncio.create_task(ws.receive_text())
        outgoing = asyncio.create_task(queue.get())
        last_heartbeat = time.monotonic()
        while True:
            done,_ = await asyncio.wait([incoming,outgoing],timeout=30,return_when=asyncio.FIRST_COMPLETED)
            if not done:
                raise HTTPException(401,'Heartbeat expired.')
            if incoming in done:
                value = incoming.result()
                if len(value)>128 or value != 'ping' or time.monotonic()-last_heartbeat<5:
                    raise HTTPException(429,'Invalid heartbeat.')
                await run_in_threadpool(heartbeat,s)
                last_heartbeat=time.monotonic()
                await ws.send_json({'type':'pong'})
                incoming=asyncio.create_task(ws.receive_text())
            if outgoing in done:
                # Recheck actor permission on delivery; HTTP also checks every retrieval.
                await run_in_threadpool(heartbeat,s,False)
                await ws.send_json(outgoing.result())
                outgoing=asyncio.create_task(queue.get())
            if time.monotonic()-last_heartbeat>60:
                raise HTTPException(401,'Heartbeat expired.')
    except (WebSocketDisconnect,asyncio.TimeoutError,HTTPException,ValueError,TypeError):
        try:
            await ws.close(code=1008)
        except RuntimeError:
            pass
    finally:
        for task in [incoming,outgoing]:
            if task:
                task.cancel()
        if broker and key:
            broker.leave(key)
        if s:
            with anyio.CancelScope(shield=True):
                try:
                    await run_in_threadpool(disconnect,s)
                except HTTPException:
                    pass
