"""Timing only: never log bodies, credentials, signed URLs or SQL parameters."""

import logging
import time
from fastapi.routing import APIRoute

log = logging.getLogger("inksuite.marketplace.performance")


class TimedRoute(APIRoute):
    def get_route_handler(self):
        original = super().get_route_handler()

        async def handler(request):
            start = time.monotonic()
            status = 500
            try:
                response = await original(request)
                status = response.status_code
                return response
            except Exception as exc:
                status = getattr(exc, "status_code", 500)
                raise
            finally:
                log.info(
                    "marketplace_http route=%s method=%s status=%s elapsed_ms=%s",
                    self.path,
                    request.method,
                    status,
                    round((time.monotonic() - start) * 1000),
                )

        return handler


class TimedCursor:
    def __init__(self, cursor):
        self.cursor = cursor
        self.count = 0
        self.ms = 0

    def __getattr__(self, name):
        return getattr(self.cursor, name)

    def execute(self, sql, params=()):
        start = time.monotonic()
        try:
            return self.cursor.execute(sql, params)
        finally:
            elapsed = (time.monotonic() - start) * 1000
            self.count += 1
            self.ms += elapsed
            if elapsed >= 200:
                import hashlib

                log.warning(
                    "marketplace_slow_query fingerprint=%s elapsed_ms=%s",
                    hashlib.sha256(str(sql).encode()).hexdigest()[:12],
                    round(elapsed),
                )
