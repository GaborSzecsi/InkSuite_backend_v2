from contextlib import contextmanager
from concurrent.futures import ThreadPoolExecutor
from io import BytesIO
from threading import Lock
import hashlib
import time
import pytest
from fastapi import HTTPException
from app.marketplace import arc_service as service


def test_concurrent_resources_share_download_and_cached_resources_check_access(monkeypatch):
    data = b"test archive"
    entry = {"storage_key": "test/book.epub", "sha256": hashlib.sha256(data).hexdigest(), "package": {"opf": "book.opf", "resources": {"chapter.xhtml": "application/xhtml+xml"}}}
    counts = {"download": 0, "process": 0, "access": 0}
    lock = Lock()
    revoked = False
    @contextmanager
    def transaction():
        yield None
    def access(*args):
        with lock:
            counts["access"] += 1
        if revoked:
            raise HTTPException(403, "Revoked")
        return entry
    class Storage:
        def get_object(self, **kwargs):
            with lock:
                counts["download"] += 1
            time.sleep(0.04)
            return {"Body": BytesIO(data)}
    def process(*args):
        with lock:
            counts["process"] += 1
        return b"chapter", "application/xhtml+xml"
    monkeypatch.setattr(service, "transaction", transaction)
    monkeypatch.setattr(service, "session_access", access)
    monkeypatch.setattr(service, "storage", lambda: (Storage(), "test"))
    monkeypatch.setattr(service.arc_epub, "resource", process)
    service._archive_cache.clear()
    service._resource_cache.clear()
    try:
        with ThreadPoolExecutor(max_workers=4) as pool:
            results = list(pool.map(lambda _: service.resource("session", "chapter.xhtml", {}), range(4)))
        assert all(result[0] == b"chapter" for result in results)
        assert counts["download"] == 1
        processed, accesses = counts["process"], counts["access"]
        assert service.resource("session", "chapter.xhtml", {})[0] == b"chapter"
        assert counts["process"] == processed
        assert counts["access"] == accesses + 1
        revoked = True
        with pytest.raises(HTTPException) as error:
            service.resource("session", "chapter.xhtml", {})
        assert error.value.status_code == 403
    finally:
        service._archive_cache.clear()
        service._resource_cache.clear()
