"""Thin HTTP/WebSocket adapters for the messaging service."""
from .realtime_service import *
from . import realtime_service as service
router = APIRouter()

@router.post('/messaging/session')
def session(actor_id: UUID,user=Depends(current_user)):
    return service.session(actor_id,user)

@router.get('/messaging/connections')
def connections(actor_id: UUID,q: str=Query('',max_length=200),offset: int=Query(0,ge=0,le=1000000),user=Depends(current_user)):
    return service.connections(actor_id,q,offset,user)

@router.websocket('/messaging/live')
async def live(ws: WebSocket):
    await service.live(ws)
