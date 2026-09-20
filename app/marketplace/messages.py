"""Compatibility alias for existing Marketplace imports."""

import sys
from . import messages_service as service
from . import messages_routes as http

service.router = http.router
sys.modules[__name__] = service
