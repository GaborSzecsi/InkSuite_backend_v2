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
        """CREATE TYPE roy_party AS ENUM ('author','illustrator');
    CREATE TABLE users(id uuid PRIMARY KEY,cognito_sub text);
    CREATE TABLE memberships(user_id uuid,tenant_id uuid,role text,module_permissions jsonb);
    CREATE TABLE works(id uuid PRIMARY KEY,tenant_id uuid);
    CREATE TABLE parties(id uuid PRIMARY KEY,tenant_id uuid,party_type text);
    CREATE TABLE royalty_payment_instructions(id uuid PRIMARY KEY DEFAULT gen_random_uuid(),tenant_id uuid,work_id uuid REFERENCES works(id),party roy_party,payee_mode text CHECK(payee_mode IN ('contributor_only','agency_only','split')),contributor_party_id uuid REFERENCES parties(id),agency_party_id uuid REFERENCES parties(id),contributor_percent numeric(8,4),agency_percent numeric(8,4),effective_start date,effective_end date,notes text,created_at timestamptz DEFAULT now(),updated_at timestamptz DEFAULT now());""",
        execute=True,
    )
    tenant, other, user, work, otherwork, contributor, agency, foreignparty = [
        str(uuid4()) for _ in range(8)
    ]
    rpc("INSERT INTO users VALUES ($1,'test-sub')", [user])
    rpc("INSERT INTO memberships VALUES ($1,$2,'tenant_admin','{}')", [user, tenant])
    rpc("INSERT INTO works VALUES ($1,$2),($3,$4)", [work, tenant, otherwork, other])
    rpc(
        "INSERT INTO parties VALUES ($1,$2,'person'),($3,$2,'org'),($4,$5,'org')",
        [contributor, tenant, agency, foreignparty, other],
    )
    module = importlib.import_module("routers.payment_instructions")
    module.db_conn = db
    app = FastAPI()
    app.include_router(module.router, prefix="/api")
    auth = [True]

    def claims():
        if not auth[0]:
            raise HTTPException(401, "Not authenticated")
        return {"sub": "test-sub"}

    app.dependency_overrides[module.get_current_user] = claims
    client = TestClient(app)
    url = f"/api/payment-instructions/{work}/author"
    checks = [0]

    def request(method, route=url, payload=None, status=200):
        response = (
            client.request(method, route, json=payload)
            if payload is not None
            else client.request(method, route)
        )
        assert response.status_code == status, (response.status_code, response.text)
        checks[0] += 1
        return response.json()

    assert request("GET")["instruction"] is None
    assert (
        rpc("SELECT count(*) n FROM royalty_payment_instructions")["rows"][0]["n"] == 0
    )
    base = {
        "payee_mode": "contributor_only",
        "contributor_party_id": contributor,
        "agency_party_id": agency,
        "contributor_percent": 50,
        "agency_percent": 50,
        "effective_start": "2026-01-01",
        "effective_end": "2027-01-01",
        "notes": "Annual instruction",
    }
    direct = request("PUT", payload=base)["instruction"]
    assert (
        direct["agency_party_id"] is None
        and float(direct["contributor_percent"]) == 100
    )
    assert request("GET")["instruction"]["notes"] == "Annual instruction"
    agencydata = {**base, "payee_mode": "agency_only"}
    saved = request("PUT", payload=agencydata)["instruction"]
    assert (
        saved["id"] == direct["id"]
        and float(saved["agency_percent"]) == 100
        and saved["agency_party_id"] == agency
    )
    split = {
        **base,
        "payee_mode": "split",
        "contributor_percent": 85,
        "agency_percent": 15,
    }
    saved = request("PUT", payload=split)["instruction"]
    assert float(saved["contributor_percent"]) == 85
    assert float(request("GET")["instruction"]["agency_percent"]) == 15
    request("PUT", payload=split)
    assert (
        rpc("SELECT count(*) n FROM royalty_payment_instructions")["rows"][0]["n"] == 1
    )
    request("GET", route=f"/api/payment-instructions/{work}/illustrator")
    request("PUT", route=f"/api/payment-instructions/{work}/illustrator", payload=base)
    assert (
        rpc("SELECT count(*) n FROM royalty_payment_instructions")["rows"][0]["n"] == 2
    )
    for invalid in [
        dict(split, agency_party_id=None),
        dict(split, agency_percent=14),
        dict(split, contributor_percent=-1),
        dict(split, contributor_percent="NaN"),
        dict(split, agency_percent="15.00001"),
        dict(split, effective_end="2025-01-01"),
        dict(split, contributor_party_id=foreignparty),
        dict(split, agency_party_id=foreignparty),
        dict(split, agency_party_id=contributor),
        dict(split, tenant_id=other),
    ]:
        request("PUT", payload=invalid, status=422)
    request("GET", route=f"/api/payment-instructions/{otherwork}/author", status=404)
    request(
        "PUT",
        route=f"/api/payment-instructions/{otherwork}/author",
        payload=split,
        status=404,
    )
    request(
        "PUT",
        route=f"/api/payment-instructions/{work}/editor",
        payload=split,
        status=422,
    )
    rpc("UPDATE memberships SET role='reader'")
    request("GET", status=403)
    request("PUT", payload=split, status=403)
    rpc("UPDATE memberships SET role='tenant_user',module_permissions='{}'")
    request("GET", status=403)
    rpc("UPDATE memberships SET module_permissions='{\"project_management\":true}'")
    request("GET")
    auth[0] = False
    request("GET", status=401)
    request("PUT", payload=split, status=401)
    print(
        f"PASS: {checks[0]} API checks: save/reload/update, role separation, canonical percentages, validations, authentication and tenant isolation."
    )
finally:
    proc.terminate()
    proc.wait(timeout=5)
