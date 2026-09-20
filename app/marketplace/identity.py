"""Compatibility alias for existing Marketplace imports."""

import sys
from . import identity_service as service
from . import identity_routes as http

service.router = http.router
sys.modules[__name__] = service
