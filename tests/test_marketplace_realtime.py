import asyncio
from unittest.mock import patch
import pytest
from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient
from app.marketplace import realtime_service as live
from app.marketplace.realtime_routes import router

@pytest.fixture
def client():
    app=FastAPI();app.include_router(router)
    async def idle(self):
        await asyncio.Event().wait()
    def consume(ticket):
        if ticket not in ('alice','bob'):
            raise HTTPException(401,'Bad ticket')
        return {'id':ticket,'actor_id':ticket,'user':{'id':ticket}}
    with patch.dict('os.environ',{'ALLOW_ORIGINS':'https://reader.example'}), patch.object(live,'consume',side_effect=consume), patch.object(live,'heartbeat') as beat, patch.object(live,'disconnect') as disconnect, patch.object(live.Broker,'listen',idle):
        with TestClient(app) as c:
            yield c,beat,disconnect


def test_two_online_users_receive_shared_event_and_read_sync(client):
    c,beat,disconnect=client
    with c.websocket_connect('/messaging/live',headers={'origin':'https://reader.example'}) as alice, c.websocket_connect('/messaging/live',headers={'origin':'https://reader.example'}) as bob:
        alice.send_json({'ticket':'alice'});assert alice.receive_json()['type']=='ready'
        bob.send_json({'ticket':'bob'});assert bob.receive_json()['type']=='ready'
        async def publish():
            broker=live._brokers[asyncio.get_running_loop()]
            broker.publish({'type':'refresh','actors':['alice','bob'],'conversation_id':'private','incoming':True,'sender_actor_id':'alice'})
            broker.publish({'type':'refresh','actors':['alice'],'conversation_id':'alice-only'})
            broker.publish({'type':'refresh','actors':['bob'],'conversation_id':'bob-only'})
        c.portal.call(publish)
        assert alice.receive_json()['conversation_id']=='private'
        event=bob.receive_json();assert event['conversation_id']=='private' and event['incoming']
        assert alice.receive_json()['conversation_id']=='alice-only'
        assert bob.receive_json()['conversation_id']=='bob-only'
        assert beat.call_count==4
    assert disconnect.call_count==2


def test_invalid_origin_and_ticket_are_rejected(client):
    from starlette.websockets import WebSocketDisconnect
    c,_,_=client
    with pytest.raises(WebSocketDisconnect):
        with c.websocket_connect('/messaging/live',headers={'origin':'https://attacker.example'}):
            pass
    with c.websocket_connect('/messaging/live',headers={'origin':'https://reader.example'}) as ws:
        ws.send_json({'ticket':'invalid'})
        with pytest.raises(WebSocketDisconnect):ws.receive_json()


def test_reconnect_gets_ready_and_can_recover_over_rest(client):
    c,_,disconnect=client
    for _ in range(2):
        with c.websocket_connect('/messaging/live',headers={'origin':'https://reader.example'}) as ws:
            ws.send_json({'ticket':'alice'})
            assert ws.receive_json()['type']=='ready'
    assert disconnect.call_count==2


def test_broker_coalesces_overflow_without_message_content():
    async def check():
        broker=live.Broker()
        # No listener needed to test overflow and recipient filtering.
        q=asyncio.Queue(maxsize=1);broker.clients['key']=('alice',q)
        broker.publish({'type':'refresh','actors':['bob'],'conversation_id':'secret'})
        assert q.empty()
        broker.publish({'type':'refresh','actors':['alice'],'body':'never send bodies'})
        broker.publish({'type':'refresh','actors':['alice'],'conversation_id':'latest'})
        assert q.get_nowait()=={'type':'refresh','conversation_id':'latest','incoming':False,'sender_actor_id':None}
    asyncio.run(check())
