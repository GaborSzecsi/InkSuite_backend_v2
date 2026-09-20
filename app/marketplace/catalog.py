"""Compatibility alias for existing Marketplace imports."""

import sys
from . import catalog_service as service
from . import catalog_routes as http

service.router = http.router
sys.modules[__name__] = service
