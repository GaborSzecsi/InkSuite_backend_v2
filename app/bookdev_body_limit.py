"""Limit multipart bytes before Starlette spools a public upload to disk."""
from starlette.responses import JSONResponse
from starlette.requests import Request
from starlette.concurrency import run_in_threadpool
from fastapi import HTTPException
from app.bookdev_security import public_request_endpoint

MAX_REQUEST_BYTES = 10 * 1024 * 1024 + 64 * 1024


class BookdevBodyLimit:
    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        path = scope.get("path", "")
        if scope.get("type") != "http" or scope.get("method") != "POST" or not public_request_endpoint(path, "POST"):
            return await self.app(scope, receive, send)
        if path.rstrip("/").endswith("/photo-upload"):
            from routers.bookdev_email import _pending_request
            from app.bookdev_security import require_verified
            try:
                row = await run_in_threadpool(_pending_request, path.rstrip("/").split("/")[-2])
                await run_in_threadpool(require_verified, row, Request(scope))
            except HTTPException as error:
                return await JSONResponse({"detail": error.detail}, status_code=error.status_code)(scope, receive, send)
        limit = MAX_REQUEST_BYTES if path.rstrip("/").endswith("/photo-upload") else 1024 * 1024
        chunks = []
        size = 0
        while True:
            message = await receive()
            if message["type"] == "http.disconnect":
                return
            size += len(message.get("body", b""))
            if size > limit:
                return await JSONResponse({"detail": "Request is too large."}, status_code=413)(scope, receive, send)
            chunks.append(message.get("body", b""))
            if not message.get("more_body", False):
                break
        delivered = False
        async def bounded_receive():
            nonlocal delivered
            if not delivered:
                delivered = True
                return {"type": "http.request", "body": b"".join(chunks), "more_body": False}
            return await receive()
        return await self.app(scope, bounded_receive, send)
