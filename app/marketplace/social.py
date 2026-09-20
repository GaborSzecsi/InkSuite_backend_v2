"""Compatibility alias for existing Marketplace imports."""

import sys
from . import social_service as service
from . import social_routes as http

service.router = http.router
sys.modules[__name__] = service
