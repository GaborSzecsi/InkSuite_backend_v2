"""Compatibility alias for existing Marketplace imports."""

import sys
from . import invitations_service as service
from . import invitations_routes as http

service.router = http.router
sys.modules[__name__] = service
