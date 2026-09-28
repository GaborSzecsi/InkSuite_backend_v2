"""Disposable PostgreSQL endpoint tests; never reads/writes the real database."""

import contextlib
import importlib
import json
import subprocess
import sys
from pathlib import Path
from uuid import uuid4
from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient

root = Path(__file__).resolve().parent
sys.path.insert(0, str(root.parent))
proc = subprocess.Popen(
    [
        "node",
        "--preserve-symlinks",
        "--preserve-symlinks-main",
        str(root / "banking_pg.cjs"),
    ],
    stdin=subprocess.PIPE,
    stdout=subprocess.PIPE,
    text=True,
    encoding="utf-8",
)
assert json.loads(proc.stdout.readline())["ready"]


def rpc(sql, params=(), execute=False):
    proc.stdin.write(
        json.dumps({"sql": sql, "params": params, "exec": execute}, default=str) + "\n"
    )
    proc.stdin.flush()
    result = json.loads(proc.stdout.readline())
    assert "error" not in result, result
    return result["result"]


class Cursor:
    def execute(self, sql, params=()):
        for n in range(sql.count("%s")):
            sql = sql.replace("%s", f"${n+1}", 1)
        self.rows = rpc(sql, params)["rows"]

    def fetchone(self):
        return self.rows[0] if self.rows else None

    def fetchall(self):
        return self.rows

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass


class Connection:
    def cursor(self, **kwargs):
        return Cursor()

    @contextlib.contextmanager
    def transaction(self):
        rpc("BEGIN", execute=True)
        try:
            yield
            rpc("COMMIT", execute=True)
        except BaseException:
            rpc("ROLLBACK", execute=True)
            raise


@contextlib.contextmanager
def db():
    yield Connection()


try:
    rpc(
        """CREATE TABLE users(id uuid PRIMARY KEY,cognito_sub text);
    CREATE TABLE tenants(id uuid PRIMARY KEY,slug text);
    CREATE TABLE memberships(user_id uuid,tenant_id uuid,role text,module_permissions jsonb);
    CREATE TABLE works(id uuid PRIMARY KEY,tenant_id uuid);""",
        execute=True,
    )
    migration = (root.parent / "migrations/015_book_development_notes.sql").read_text()
    rpc(migration, execute=True)
    rpc(migration, execute=True)
    tenant, other, user1, user2, user3, work, otherwork = [
        str(uuid4()) for _ in range(7)
    ]
    rpc(
        "INSERT INTO tenants VALUES ($1,'publisher-a'),($2,'publisher-b')",
        [tenant, other],
    )
    rpc(
        "INSERT INTO users VALUES ($1,'one'),($2,'two'),($3,'three')",
        [user1, user2, user3],
    )
    rpc(
        "INSERT INTO memberships VALUES ($1,$4,'tenant_admin','{}'),($2,$4,'tenant_user','{}'),($3,$5,'tenant_admin','{}')",
        [user1, user2, user3, tenant, other],
    )
    rpc("INSERT INTO works VALUES ($1,$2),($3,$4)", [work, tenant, otherwork, other])
    module = importlib.import_module("routers.bookdev_notes")
    helpers = importlib.import_module("routers.bookdev_email")
    module.db_conn = db
    helpers.db_conn = db
    app = FastAPI()
    app.include_router(module.router, prefix="/api")
    identity = ["one"]

    def claims():
        if identity[0] is None:
            raise HTTPException(401, "Not authenticated")
        return {"sub": identity[0]}

    app.dependency_overrides[helpers._ctx_from_bearer] = claims
    client = TestClient(app)
    baseurl = "/api/project-management/book-development"
    url = f"{baseurl}/{work}/notes"
    checks = [0]

    def request(method, path=url, payload=None, status=200, slug="publisher-a"):
        response = (
            client.request(method, path + "?tenant_slug=" + slug, json=payload)
            if payload is not None
            else client.request(method, path + "?tenant_slug=" + slug)
        )
        assert response.status_code == status, (response.status_code, response.text)
        checks[0] += 1
        if status == 200:
            assert "no-store" in response.headers["cache-control"]
        return response.json()

    assert request("GET")["notes"] == []
    assert request("GET", baseurl + "/notes/counts")["counts"] == {}
    text = "Email follow-up\n\n<script>plain text</script>\n" + "Long email. " * 5000
    first = request("POST", payload={"content": text})["note"]
    second = request("POST", payload={"content": ""})["note"]
    assert request("GET")["notes"][1]["content"] == text
    assert request("GET", baseurl + "/notes/counts")["counts"][work] == 2
    updated = request(
        "PUT", url + "/" + first["id"], {"content": "Revised\nKeep whitespace  "}
    )["note"]
    assert updated["content"] == "Revised\nKeep whitespace  "
    assert request("GET")["notes"][0]["id"] == first["id"]
    identity[0] = "two"
    assert request("GET")["notes"] == []
    assert request("GET", baseurl + "/notes/counts")["counts"] == {}
    request("PUT", url + "/" + first["id"], {"content": "not mine"}, 404)
    request("DELETE", url + "/" + first["id"], status=404)
    their = request("POST", payload={"content": "Second user private"})["note"]
    identity[0] = "one"
    request("DELETE", url + "/" + their["id"], status=404)
    request("POST", payload={"content": "spoof", "user_id": user2}, status=422)
    request("POST", payload={"content": "spoof", "tenant_id": other}, status=422)
    request("POST", payload={"content": "x" * 1000001}, status=422)
    for method in ["GET", "POST", "PUT", "DELETE"]:
        foreign = f"{baseurl}/{otherwork}/notes" + (
            "/" + first["id"] if method in ["PUT", "DELETE"] else ""
        )
        request(
            method,
            foreign,
            {"content": "cross tenant"} if method in ["POST", "PUT"] else None,
            404,
        )
    request("GET", slug="publisher-b", status=403)
    identity[0] = "three"
    assert request("GET", baseurl + "/notes/counts", slug="publisher-b")["counts"] == {}
    request("DELETE", url + "/" + first["id"], slug="publisher-b", status=404)
    identity[0] = None
    for method, path in [
        ("GET", url),
        ("POST", url),
        ("PUT", url + "/" + first["id"]),
        ("DELETE", url + "/" + first["id"]),
        ("GET", baseurl + "/notes/counts"),
    ]:
        request(
            method,
            path,
            {"content": "anonymous"} if method in ["POST", "PUT"] else None,
            401,
        )
    identity[0] = "one"
    request("DELETE", url + "/" + first["id"])
    assert request("GET", baseurl + "/notes/counts")["counts"][work] == 1
    request("DELETE", url + "/" + second["id"])
    assert request("GET", baseurl + "/notes/counts")["counts"] == {}
    print(
        f"PASS: {checks[0]} notes API checks, migration idempotence, CRUD/counts, long text, user/tenant isolation and anonymous denials."
    )
finally:
    proc.terminate()
    proc.wait(timeout=5)
