"""Runs production route code/SQL against disposable PGlite. No AWS calls or DSN."""

import sys, json, subprocess, contextlib
from pathlib import Path
from uuid import uuid4

root = Path(__file__).parent
sys.path.insert(0, str(root.parent))
import psycopg
from psycopg.types.json import Jsonb
from fastapi import FastAPI
from fastapi.testclient import TestClient
from app.marketplace import (
    core,
    identity,
    social,
    notifications,
    catalog as catalog_module,
)

real_connection_email = notifications.email_connection_request
mail_calls = []
notifications.email_connection_request = lambda connection_id: mail_calls.append(
    connection_id
)
catalog_module._legacy_cover_url = lambda *args: ""
from app.marketplace.routes import router
from app.auth.dependencies import require_session

proc = subprocess.Popen(
    [
        "node",
        "--preserve-symlinks",
        "--preserve-symlinks-main",
        str(root / "marketplace_pg/rpc.cjs"),
    ],
    stdin=subprocess.PIPE,
    stdout=subprocess.PIPE,
    text=True,
    encoding="utf-8",
)
assert json.loads(proc.stdout.readline()).get("ready")


def rpc(sql, params=(), execute=False):
    params = [
        (
            json.dumps(v.obj)
            if isinstance(v, Jsonb)
            else str(v) if isinstance(v, __import__("uuid").UUID) else v
        )
        for v in params
    ]
    proc.stdin.write(
        json.dumps({"sql": sql, "params": params, "exec": execute}, default=str) + "\n"
    )
    proc.stdin.flush()
    r = json.loads(proc.stdout.readline())
    if "error" in r:
        print("SQL ERROR:", r["error"])
        raise psycopg.errors.lookup(r.get("code") or "XX000")(r["error"])
    return r["result"]


query_count = 0


class Cursor:
    def execute(self, sql, params=()):
        global query_count
        query_count += 1
        i = 0
        while "%s" in sql:
            i += 1
            sql = sql.replace("%s", f"${i}", 1)
        self.data = rpc(sql, params)["rows"]
        return self

    def fetchone(self):
        return self.data[0] if self.data else None

    def fetchall(self):
        return self.data

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass


class Connection:
    @contextlib.contextmanager
    def transaction(self):
        rpc("BEGIN", execute=True)
        try:
            yield
        except BaseException:
            rpc("ROLLBACK", execute=True)
            raise
        else:
            rpc("COMMIT", execute=True)

    def cursor(self, **kwargs):
        return Cursor()


@contextlib.contextmanager
def db():
    yield Connection()


core.db_conn = db
user = {"id": str(uuid4()), "platform_role": "user"}
admin = dict(user)
reader = {"id": str(uuid4()), "platform_role": "user"}
outsider = {"id": str(uuid4()), "platform_role": "user"}
claims = {"sub": "test"}
identity.auth.get_user_db_record_from_claims = lambda claims: user
social.get_user_db_record_from_claims = lambda claims: user
app = FastAPI()
app.include_router(router, prefix="/api")
app.dependency_overrides[core.current_user] = lambda: user
app.dependency_overrides[require_session] = lambda: claims
client = TestClient(app)
checks = 0


def call(method, path, body=None, status=200):
    global checks
    r = client.request(method, "/api/marketplace/" + path, json=body)
    assert r.status_code == status, (method, path, r.status_code, r.text)
    checks += 1
    return r.json()


def seed(sql, params=()):
    return rpc(sql, params)["rows"]


def epub():
    import io, zipfile

    out = io.BytesIO()
    with zipfile.ZipFile(out, "w") as z:
        z.writestr("mimetype", "application/epub+zip")
        z.writestr(
            "META-INF/container.xml",
            '<container xmlns="urn:oasis:names:tc:opendocument:xmlns:container"><rootfiles><rootfile full-path="OPS/book.opf"/></rootfiles></container>',
        )
        z.writestr(
            "OPS/book.opf",
            '<package xmlns="http://www.idpf.org/2007/opf" version="3.0"><metadata xmlns:dc="http://purl.org/dc/elements/1.1/"><dc:title>Test</dc:title><dc:identifier>test</dc:identifier><dc:language>en</dc:language></metadata><manifest><item id="chapter" href="chapter.xhtml" media-type="application/xhtml+xml"/><item id="nav" href="nav.xhtml" media-type="application/xhtml+xml" properties="nav"/></manifest><spine><itemref idref="chapter"/></spine></package>',
        )
        z.writestr(
            "OPS/chapter.xhtml",
            '<html xmlns="http://www.w3.org/1999/xhtml"><head><title>Test</title></head><body><h1>Chapter one</h1><script>alert(1)</script><p onclick="bad()">Reading test</p><img src="https://attacker.invalid/pixel"/></body></html>',
        )
        z.writestr(
            "OPS/nav.xhtml",
            '<html xmlns="http://www.w3.org/1999/xhtml" xmlns:epub="http://www.idpf.org/2007/ops"><head><title>Contents</title></head><body><nav epub:type="toc"><ol><li><a href="chapter.xhtml">Chapter one</a></li></ol></nav></body></html>',
        )
    return out.getvalue()


try:
    from app.marketplace import arc_service
    import io

    objects = {}

    class S3:
        def put_object(self, **kw):
            objects[kw["Key"]] = kw["Body"]

        def get_object(self, **kw):
            return {"Body": io.BytesIO(objects[kw["Key"]])}

    arc_service.storage = lambda: (S3(), "private-test")
    rpc(
        (root.parent / "migrations/013_marketplace_arc.sql").read_text(
            encoding="utf-8"
        ),
        execute=True,
    )
    tid, tid2, work, work2, edition, edition2, org, org2, bid = [
        str(uuid4()) for _ in range(9)
    ]
    seed(
        "INSERT INTO tenants(id,slug,name,s3_prefix) VALUES($1,'test','Test','test/'),($2,'other','Other','other/')",
        [tid, tid2],
    )
    for i, u in enumerate([admin, reader, outsider]):
        seed(
            "INSERT INTO users(id,cognito_sub,email) VALUES($1,$2,$3)",
            [u["id"], "test" + str(i), str(i) + "@example.invalid"],
        )
        seed(
            "INSERT INTO marketplace_profiles(user_id,username,display_name) VALUES($1,$2,$3)",
            [u["id"], "user_" + str(i), "User " + str(i)],
        )
        seed("INSERT INTO marketplace_actors(user_id) VALUES($1)", [u["id"]])
    ra = seed("SELECT id FROM marketplace_actors WHERE user_id=$1", [reader["id"]])[0][
        "id"
    ]
    seed(
        "INSERT INTO memberships(tenant_id,user_id,role) VALUES($1,$2,'tenant_admin'),($3,$4,'tenant_admin')",
        [tid, admin["id"], tid2, outsider["id"]],
    )
    seed(
        "INSERT INTO works(id,tenant_id,uid,title) VALUES($1,$2,gen_random_uuid(),'Book'),($3,$4,gen_random_uuid(),'Other')",
        [work, tid, work2, tid2],
    )
    seed(
        "INSERT INTO editions(id,tenant_id,work_id) VALUES($1,$2,$3),($4,$5,$6)",
        [edition, tid, work, edition2, tid2, work2],
    )
    seed(
        "INSERT INTO marketplace_organizations(id,tenant_id,organization_type,name,slug,status) VALUES($1,$2,'publisher','Test','test','active'),($3,$4,'publisher','Other','other','active')",
        [org, tid, org2, tid2],
    )
    seed(
        "INSERT INTO marketplace_books(id,work_id,publisher_organization_id,slug,marketplace_status) VALUES($1,$2,$3,'book','public')",
        [bid, work, org],
    )
    seed(
        "INSERT INTO marketplace_book_editions(marketplace_book_id,edition_id) VALUES($1,$2)",
        [bid, edition],
    )
    data = epub()

    def upload(eid, expected=200, replace=False):
        global checks
        r = client.post(
            "/api/marketplace/arc/editions/" + eid,
            files={"file": ("test.epub", data, "application/epub+zip")},
            data={"replace": str(replace).lower()},
        )
        assert r.status_code == expected, r.text
        checks += 1
        return r.json()

    user = admin
    upload(edition2, 403)
    aid = upload(edition)["id"]
    upload(edition, 409)
    user = reader
    call("POST", f"library/{bid}/reading-session", status=404)
    request = call("POST", f"arc/{aid}/request", {"actor_id": ra, "message": "Please"})[
        "id"
    ]
    call("POST", f"arc/{aid}/request", {"actor_id": ra}, 409)
    user = outsider
    call("PUT", f"arc-requests/{request}", {"status": "approved"}, 403)
    call("GET", f"arc-requests?organization_id={org}", status=403)
    user = admin
    assert len(call("GET", f"arc-requests?organization_id={org}")["items"]) == 1
    assert any(
        x["kind"] == "arc" for x in call("GET", "connection-notifications")["items"]
    )
    real_grant = arc_service.repo.grant

    def fail_grant(*args):
        from fastapi import HTTPException

        raise HTTPException(409, "Simulated entitlement failure")

    arc_service.repo.grant = fail_grant
    call("PUT", f"arc-requests/{request}", {"status": "approved"}, 409)
    assert (
        seed("SELECT status FROM marketplace_arc_requests WHERE id=$1", [request])[0][
            "status"
        ]
        == "pending"
    )
    arc_service.repo.grant = real_grant
    call("PUT", f"arc-requests/{request}", {"status": "approved"})
    assert (
        seed(
            "SELECT arc_asset_id FROM marketplace_library_items WHERE user_id=$1",
            [reader["id"]],
        )[0]["arc_asset_id"]
        == aid
    )
    assert call("POST", f"arc/{aid}/preview")["book_id"] == bid
    user = reader
    assert any(
        x["kind"] == "arc" and x["href"].endswith("/read")
        for x in call("GET", "connection-notifications")["items"]
    )
    s = call("POST", f"library/{bid}/reading-session")
    endpoint = f"/api/marketplace/reader/{s['id']}/resource/OPS/chapter.xhtml"
    call("GET", f"reader/{s['id']}/status")
    r = client.get(endpoint)
    assert r.status_code == 200, r.text
    assert (
        "<script" not in r.text
        and "onclick" not in r.text
        and "attacker.invalid" not in r.text
    )
    assert r.headers["cache-control"].startswith("private, no-store")
    call(
        "PUT",
        f"library/{bid}/progress",
        {"revision": 1, "location": "epubcfi(/6/2!/4/2:0)", "percent": 30},
    )
    mark = call(
        "POST",
        f"library/{bid}/bookmarks",
        {"revision": 1, "location": "epubcfi(/6/2!/4/2:0)"},
    )
    assert len(call("GET", f"library/{bid}/bookmarks")["items"]) == 1
    user = outsider
    assert client.get(endpoint).status_code == 404
    call("GET", f"reader/{s['id']}/status", status=404)
    call("POST", f"library/{bid}/reading-session", status=404)
    call(
        "PUT",
        f"library/{bid}/progress",
        {"revision": 1, "location": "epubcfi(/6/2)", "percent": 10},
        404,
    )
    call("GET", f"library/{bid}/bookmarks", status=404)
    outsider_actor = seed(
        "SELECT id FROM marketplace_actors WHERE user_id=$1", [outsider["id"]]
    )[0]["id"]
    rejected = call("POST", f"arc/{aid}/request", {"actor_id": outsider_actor})["id"]
    user = admin
    call(
        "PUT",
        f"arc-requests/{rejected}",
        {"status": "rejected", "reason": "Review list is full."},
    )
    user = reader
    seed(
        "UPDATE marketplace_reader_sessions SET expires_at=now()-interval '1 second',issued_at=now()-interval '1 hour' WHERE id=$1",
        [s["id"]],
    )
    assert client.get(endpoint).status_code == 404
    seed(
        "UPDATE marketplace_library_items SET expires_at=now()-interval '1 second' WHERE user_id=$1",
        [reader["id"]],
    )
    call("POST", f"library/{bid}/reading-session", status=403)
    seed(
        "UPDATE marketplace_library_items SET expires_at=NULL WHERE user_id=$1",
        [reader["id"]],
    )
    s = call("POST", f"library/{bid}/reading-session")
    user = admin
    assert upload(edition, replace=True)["revision"] == 2
    user = reader
    assert (
        client.get(
            f"/api/marketplace/reader/{s['id']}/resource/OPS/chapter.xhtml"
        ).status_code
        == 409
    )
    s = call("POST", f"library/{bid}/reading-session")
    assert s["revision"] == 2 and s["location"] == ""
    assert not call("GET", f"library/{bid}/bookmarks")["items"]
    user = admin
    call("PUT", f"arc-requests/{request}", {"status": "revoked"})
    user = reader
    call("POST", f"library/{bid}/reading-session", status=403)
    assert (
        client.get(
            f"/api/marketplace/reader/{s['id']}/resource/OPS/chapter.xhtml"
        ).status_code
        == 403
    )
    assert len(call("GET", "arc-library")["items"]) == 1
    call("DELETE", f"library/{bid}", status=409)
    print(
        f"ARC integration passed: {checks} endpoint checks plus resource, ownership, migration, storage and history assertions"
    )
finally:
    proc.stdin.close()
    proc.wait(timeout=10)
