"""Compatibility alias for existing Marketplace imports."""

import sys
from . import notifications_service as service
from . import notifications_routes as http

service.router = http.router
sys.modules[__name__] = service
