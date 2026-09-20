from .observability import TimedRoute

"""HTTP adapters; service functions also support non-HTTP callers."""
from .notifications_service import *
from . import notifications_service as service

router = APIRouter(route_class=TimedRoute)


@router.get("/connection-notifications")
def connection_notifications(user=Depends(current_user)):
    return service.connection_notifications(user)
