"""Compatibility alias for existing Marketplace imports."""

import sys
from . import media_service as service
from . import media_routes as http

service.router = http.router
sys.modules[__name__] = service
