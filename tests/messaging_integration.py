"""Runs production route code/SQL against disposable PGlite. No AWS calls or DSN."""

import sys, json, subprocess, contextlib
from pathlib import Path
from uuid import uuid4

root = Path(__file__).parent
sys.path.insert(0, str(root.parent))
import psycopg
from psycopg.types.json import Jsonb
from fastapi import FastAPI
from fastapi.testclient import TestClient
from app.marketplace import (
    core,
    identity,
    social,
    notifications,
    catalog as catalog_module,
)

real_connection_email = notifications.email_connection_request
mail_calls = []
notifications.email_connection_request = lambda connection_id: mail_calls.append(
    connection_id
)
catalog_module._legacy_cover_url = lambda *args: ""
from app.marketplace.routes import router
from app.auth.dependencies import require_session

proc = subprocess.Popen(
    [
        "node",
        "--preserve-symlinks",
        "--preserve-symlinks-main",
        str(root / "marketplace_pg/rpc.cjs"),
    ],
    stdin=subprocess.PIPE,
    stdout=subprocess.PIPE,
    text=True,
    encoding="utf-8",
)
assert json.loads(proc.stdout.readline()).get("ready")


def rpc(sql, params=(), execute=False):
    params = [
        (
            json.dumps(v.obj)
            if isinstance(v, Jsonb)
            else str(v) if isinstance(v, __import__("uuid").UUID) else v
        )
        for v in params
    ]
    proc.stdin.write(
        json.dumps({"sql": sql, "params": params, "exec": execute}, default=str) + "\n"
    )
    proc.stdin.flush()
    r = json.loads(proc.stdout.readline())
    if "error" in r:
        print("SQL ERROR:", r["error"])
        raise psycopg.errors.lookup(r.get("code") or "XX000")(r["error"])
    return r["result"]


query_count = 0


class Cursor:
    def execute(self, sql, params=()):
        global query_count
        query_count += 1
        i = 0
        while "%s" in sql:
            i += 1
            sql = sql.replace("%s", f"${i}", 1)
        self.data = rpc(sql, params)["rows"]
        return self

    def fetchone(self):
        return self.data[0] if self.data else None

    def fetchall(self):
        return self.data

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass


class Connection:
    @contextlib.contextmanager
    def transaction(self):
        rpc("BEGIN", execute=True)
        try:
            yield
        except BaseException:
            rpc("ROLLBACK", execute=True)
            raise
        else:
            rpc("COMMIT", execute=True)

    def cursor(self, **kwargs):
        return Cursor()


@contextlib.contextmanager
def db():
    yield Connection()


core.db_conn = db
user = {"id": str(uuid4()), "platform_role": "user"}
admin = dict(user)
reader = {"id": str(uuid4()), "platform_role": "user"}
outsider = {"id": str(uuid4()), "platform_role": "user"}
claims = {"sub": "test"}
identity.auth.get_user_db_record_from_claims = lambda claims: user
social.get_user_db_record_from_claims = lambda claims: user
app = FastAPI()
app.include_router(router, prefix="/api")
app.dependency_overrides[core.current_user] = lambda: user
app.dependency_overrides[require_session] = lambda: claims
client = TestClient(app)
checks = 0


def call(method, path, body=None, status=200):
    global checks
    r = client.request(method, "/api/marketplace/" + path, json=body)
    assert r.status_code == status, (method, path, r.status_code, r.text)
    checks += 1
    return r.json()


def seed(sql, params=()):
    return rpc(sql, params)["rows"]


try:
    rpc((root.parent / "migrations/035_marketplace_messaging.sql").read_text(encoding="utf-8"), execute=True)
    owners=[]
    actors=[]
    for i in range(37):
        u={"id":str(uuid4()),"platform_role":"user"}; owners.append(u)
        seed("INSERT INTO users(id,cognito_sub,email) VALUES($1,$2,'fixture@example.invalid')",[u['id'],'messaging-'+str(i)])
        seed("INSERT INTO marketplace_profiles(user_id,username,display_name) VALUES($1,$2,$3)",[u['id'],'fixture_'+str(i),'Historic Participant '+str(i)])
        a=str(uuid4()); actors.append(a)
        seed("INSERT INTO marketplace_actors(id,user_id) VALUES($1,$2)",[a,u['id']])
    user=owners[0]; me=actors[0]
    call('POST','conversations',{'actor_id':me,'target_id':actors[1]},403)
    conversations=[]
    for i in range(1,37):
        seed("INSERT INTO marketplace_connections(requester_actor_id,recipient_actor_id,status,responded_at) VALUES($1,$2,'accepted',now())",[me,actors[i]])
        c=call('POST','conversations',{'actor_id':me,'target_id':actors[i]})['id'];conversations.append(c)
        seed("INSERT INTO marketplace_messages(conversation_id,sender_actor_id,created_by_user_id,body,created_at) VALUES($1,$2,$3,$4,now()-($5||' minutes')::interval)",[c,actors[i],owners[i]['id'],'Historical content '+str(i),str(40-i)])
    inbox=call('GET',f'conversations?actor_id={me}')
    assert len(inbox['items'])==15 and not inbox['has_more']
    assert inbox['items'][0]['id']==conversations[-1]
    assert len(call('GET',f'conversations?actor_id={me}&offset=15')['items'])==15
    old=conversations[0]
    assert old not in [c['id'] for c in inbox['items']]
    assert call('GET',f'conversations/search?actor_id={me}&q=Historical%20content%201')['items'][0]['id'] in conversations
    assert [c['id'] for c in call('GET',f'conversations/search?actor_id={me}&q=Participant%201')['items']][-1]==old
    assert call('GET',f'conversations/search?actor_id={me}&q=content%201')['has_more'] is False
    assert not call('GET',f'conversations/search?actor_id={me}&q=%25')['items']  # literal percent, not SQL wildcard
    unread=call('GET',f'conversations/search?actor_id={me}&unread_only=true')
    assert unread['has_more'] and len(unread['items'])==30
    assert old in [c['id'] for c in call('GET',f'conversations/search?actor_id={me}&unread_only=true&offset=30')['items']]
    assert call('GET',f'unread?actor_id={me}')['count']==36
    original=call('GET',f'conversations/{old}/messages?actor_id={me}')['items'][0]
    token=str(uuid4())
    msg=call('POST',f'conversations/{old}/messages',{'actor_id':me,'body':'<script>alert(1)</script>','client_message_id':token})
    assert call('POST',f'conversations/{old}/messages',{'actor_id':me,'body':'<script>alert(1)</script>','client_message_id':token})==msg
    call('POST',f'conversations/{old}/messages',{'actor_id':me,'body':'Different','client_message_id':token},409)
    assert call('GET',f'conversations?actor_id={me}')['items'][0]['id']==old
    history=call('GET',f'conversations/{old}/messages?actor_id={me}')['items']
    assert len(history)==2 and history[0]['id']==original['id'] and history[1]['body']=='<script>alert(1)</script>'
    call('PUT',f'conversations/{old}/read/{msg["id"]}?actor_id={me}')
    call('PUT',f'conversations/{old}/read/{original["id"]}?actor_id={me}')  # must not regress
    assert call('GET',f'unread?actor_id={me}')['count']==35
    # Exact equal timestamps: marking the lower UUID read leaves the higher unread.
    low='00000000-0000-0000-0000-000000000001'; high='ffffffff-ffff-ffff-ffff-ffffffffffff'
    seed("INSERT INTO marketplace_messages(id,conversation_id,sender_actor_id,created_by_user_id,body,created_at) VALUES($1,$3,$4,$5,'tie lower','2100-01-01'),($2,$3,$4,$5,'tie higher','2100-01-01')",[low,high,old,actors[1],owners[1]['id']])
    call('PUT',f'conversations/{old}/read/{low}?actor_id={me}')
    assert next(c for c in call('GET',f'conversations?actor_id={me}')['items'] if c['id']==old)['unread']==1
    # History pagination retrieves all saved messages.
    for i in range(55):
        seed("INSERT INTO marketplace_messages(conversation_id,sender_actor_id,created_by_user_id,body) VALUES($1,$2,$3,$4)",[old,actors[1],owners[1]['id'],'page '+str(i)])
    page=call('GET',f'conversations/{old}/messages?actor_id={me}');assert len(page['items'])==50 and page['has_more']
    previous=call('GET',f'conversations/{old}/messages?actor_id={me}&before={page["items"][0]["id"]}')
    assert len(previous['items'])==9 and not previous['has_more']
    # Accepted connections search scans beyond the initial 15.
    assert len(call('GET',f'messaging/connections?actor_id={me}')['items'])==15
    assert call('GET',f'messaging/connections?actor_id={me}&q=Participant%2036')['items'][0]['id']==actors[36]
    from fastapi import HTTPException
    from app.marketplace import realtime_routes as live
    ticket=call('POST',f'messaging/session?actor_id={me}')['ticket']
    session=live.consume(ticket)
    try: live.consume(ticket);assert False,'ticket replay accepted'
    except HTTPException as exc: assert exc.status_code==401
    second=live.consume(call('POST',f'messaging/session?actor_id={me}')['ticket'])
    live.disconnect(session)
    live.heartbeat(second)
    user=owners[1]
    assert call('GET',f'messaging/connections?actor_id={actors[1]}&q=Participant%200')['items'][0]['online']
    seed("UPDATE marketplace_messaging_sessions SET heartbeat_at=now()-interval '91 seconds' WHERE id=$1",[second['id']])
    assert not call('GET',f'messaging/connections?actor_id={actors[1]}&q=Participant%200')['items'][0]['online']
    call('GET',f'conversations/{conversations[1]}/messages?actor_id={actors[1]}',status=404)
    call('GET',f'conversations/search?actor_id={me}&q=Historical',status=403)
    user=owners[0]
    page_id=str(uuid4());other_page=str(uuid4())
    call('POST',f'messaging/heartbeat?actor_id={me}&session_id={page_id}')
    call('POST',f'messaging/heartbeat?actor_id={me}&session_id={other_page}')
    call('DELETE',f'messaging/session?actor_id={me}&session_id={page_id}')
    user=owners[1]
    assert call('GET',f'messaging/connections?actor_id={actors[1]}&q=Participant%200')['items'][0]['online']
    call('POST',f'messaging/heartbeat?actor_id={actors[1]}&session_id={other_page}',status=403)
    user=owners[0]
    call('DELETE',f'messaging/session?actor_id={me}&session_id={other_page}')
    user=owners[1]
    assert not call('GET',f'messaging/connections?actor_id={actors[1]}&q=Participant%200')['items'][0]['online']
    user=owners[0]
    call('PUT','block',{'actor_id':me,'target_id':actors[1]})
    call('POST',f'conversations/{old}/messages',{'actor_id':me,'body':'blocked'},403)
    assert not call('GET',f'messaging/connections?actor_id={me}&q=Participant%201')['items'][0]['id']==actors[1]
    assert call('GET',f'conversations/{old}/messages?actor_id={me}')['items']  # retained read-only history
    print(f'PASS: {checks} messaging REST/SQL checks. Inbox cap, historical/unread search, ordering, history pagination, idempotency, cursor ties, privacy, blocking, presence, multi-session and ticket replay verified. No live writes.')
finally:
    proc.stdin.close(); proc.wait(timeout=10)
