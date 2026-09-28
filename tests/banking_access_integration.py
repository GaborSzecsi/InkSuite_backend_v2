"""Real endpoint SQL in disposable PostgreSQL; fake KMS only. Never accesses AWS."""

import base64
import contextlib
import hashlib
import hmac
import importlib
import json
import subprocess
import sys
import types
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

from cryptography.hazmat.primitives.ciphers.aead import AESGCM
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
    params = [
        {"__bytes": list(x)} if isinstance(x, (bytes, bytearray)) else x for x in params
    ]
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
        for row in self.rows:
            for key, value in list(row.items()):
                if (
                    key
                    in {
                        "secret_ciphertext",
                        "encrypted_payload",
                        "encrypted_data_key",
                        "nonce",
                    }
                    and value is not None
                ):
                    row[key] = (
                        bytes(value.values())
                        if isinstance(value, dict)
                        else bytes(value)
                    )
                if (
                    key in {"confirmed_at", "setup_expires_at", "locked_until"}
                    and value
                ):
                    row[key] = datetime.fromisoformat(value.replace("Z", "+00:00"))

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

    def commit(self):
        rpc("COMMIT", execute=True)


@contextlib.contextmanager
def db():
    rpc("BEGIN", execute=True)
    try:
        yield Connection()
        rpc("COMMIT", execute=True)
    except BaseException:
        rpc("ROLLBACK", execute=True)
        raise


class KMS:
    def encrypt(self, Plaintext, EncryptionContext, **kwargs):
        nonce = b"0" * 12
        return {
            "CiphertextBlob": nonce
            + AESGCM(b"1" * 32).encrypt(
                nonce, Plaintext, json.dumps(EncryptionContext, sort_keys=True).encode()
            )
        }

    def decrypt(self, CiphertextBlob, EncryptionContext):
        return {
            "Plaintext": AESGCM(b"1" * 32).decrypt(
                CiphertextBlob[:12],
                CiphertextBlob[12:],
                json.dumps(EncryptionContext, sort_keys=True).encode(),
            )
        }


clock = [1800000000]
stub = types.ModuleType("routers.banking")
stub.db = db
stub.kms = KMS()
stub.KMS_KEY_ID = "test-only"
stub.now = lambda: datetime.fromtimestamp(clock[0], timezone.utc)
stub.digest = lambda value: hmac.new(
    b"test-pepper", value.encode(), hashlib.sha256
).hexdigest()
stub.aad = lambda tenant, account: json.dumps(
    {"tenant_id": tenant, "payment_account_id": account, "purpose": "payment-account"},
    separators=(",", ":"),
    sort_keys=True,
).encode()
sys.modules["routers.banking"] = stub
module = importlib.import_module("routers.banking_access")
module.time = types.SimpleNamespace(time=lambda: clock[0])

schema = """
CREATE TABLE public.users(id uuid PRIMARY KEY,cognito_sub text,email text);
CREATE TABLE public.tenants(id uuid PRIMARY KEY,slug text UNIQUE);
CREATE TABLE public.memberships(user_id uuid,tenant_id uuid,role text,module_permissions jsonb);
CREATE SCHEMA secure_payments;
CREATE TABLE secure_payments.payment_profiles(id uuid PRIMARY KEY,tenant_id text,recipient_type text,recipient_id text,status text);
CREATE TABLE secure_payments.payment_accounts(id uuid PRIMARY KEY,payment_profile_id uuid,payment_method text,bank_name text,account_last4 text,is_active boolean,encrypted_payload bytea,encrypted_data_key bytea,nonce bytea);
CREATE TABLE secure_payments.payment_requests(tenant_id text,recipient_type text,recipient_id text,recipient_name text,recipient_email text,used_at timestamptz,expires_at timestamptz);
"""
rpc(schema, execute=True)
migration = (root.parent / "migrations/014_banking_access.sql").read_text()
rpc(migration, execute=True)
rpc(migration, execute=True)
uid, tid, outsider, aid, pid = [str(uuid4()) for _ in range(5)]
rpc(
    "INSERT INTO users VALUES($1,'admin','admin@example.test'),($2,'reader','reader@example.test')",
    (uid, outsider),
)
rpc("INSERT INTO tenants VALUES($1,'publisher')", (tid,))
rpc("INSERT INTO memberships VALUES($1,$2,'tenant_admin','{}')", (uid, tid))
rpc(
    "INSERT INTO secure_payments.payment_profiles VALUES($1,'publisher','CONTRIBUTOR','person','COMPLETE')",
    (pid,),
)
rpc(
    "INSERT INTO secure_payments.payment_requests VALUES('publisher','CONTRIBUTOR','person','Test Person','person@example.test',now(),now())"
)
payload = {
    "account_number": "1234567890",
    "routing_number": "021000021",
    "legal_account_holder_name": "Test Person",
}
nonce = b"2" * 12
key = b"3" * 32
encrypted_key = stub.kms.encrypt(
    Plaintext=key,
    EncryptionContext={
        "tenant_id": "publisher",
        "payment_account_id": aid,
        "purpose": "payment-account",
    },
)["CiphertextBlob"]
ciphertext = AESGCM(key).encrypt(
    nonce, json.dumps(payload).encode(), stub.aad("publisher", aid)
)
rpc(
    "INSERT INTO secure_payments.payment_accounts VALUES($1,$2,'ACH','Test Bank','7890',true,$3,$4,$5)",
    (aid, pid, ciphertext, encrypted_key, nonce),
)
claims = {"sub": "admin", "auth_time": clock[0]}
app = FastAPI()
app.include_router(module.router)


def identity():
    if not claims:
        raise HTTPException(401)
    return claims


app.dependency_overrides[module.get_current_user] = identity
client = TestClient(app)
checks = 0


def call(path, body=None, status=200, tenant="publisher"):
    global checks
    response = client.request(
        "POST" if body is not None else "GET",
        f"/financials/banking/{path}?tenant_slug={tenant}",
        json=body,
    )
    assert response.status_code == status, (path, response.status_code, response.text)
    assert (
        response.headers.get("cache-control") == "private, no-store"
        if status == 200
        else True
    )
    checks += 1
    return response.json()


try:
    listing = call("accounts")
    assert "1234567890" not in json.dumps(listing) and "021000021" not in json.dumps(
        listing
    )
    assert listing["items"][0]["account_last4"] == "7890"
    call(f"accounts/{aid}/reveal", {"code": "123456"}, 403)
    claims["sub"] = "reader"
    call("accounts", status=403)
    claims["sub"] = "admin"
    call("accounts", status=403, tenant="other")
    claims["auth_time"] -= 1000
    call("authenticator/setup", {}, 403)
    claims["auth_time"] = clock[0]
    setup = call("authenticator/setup", {})
    assert setup["qr"].startswith("data:image/svg+xml;base64,")
    secret = setup["secret"]
    # RFC 6238 SHA1 test vector at 59s (six-digit truncation).
    assert (
        module.totp(base64.b32encode(b"12345678901234567890").decode(), 1) == "287082"
    )
    confirmed = call(
        "authenticator/confirm", {"code": module.totp(secret, clock[0] // 30)}
    )
    recovery = confirmed["recovery_codes"][0]
    call("authenticator/setup", {}, 409)
    call(f"accounts/{aid}/reveal", {"code": module.totp(secret, clock[0] // 30)}, 400)
    clock[0] += 30
    revealed = call(
        f"accounts/{aid}/reveal", {"code": module.totp(secret, clock[0] // 30)}
    )
    assert revealed["fields"] == payload
    call(f"accounts/{aid}/reveal", {"code": module.totp(secret, clock[0] // 30)}, 400)
    for _ in range(4):
        call(f"accounts/{aid}/reveal", {"code": "bad-code"}, 400)
    call(f"accounts/{aid}/reveal", {"code": "bad-code"}, 429)
    clock[0] += 901
    claims["auth_time"] = clock[0]
    replacement = call("authenticator/recover", {"code": recovery})
    call("authenticator/recover", {"code": recovery}, 403)
    call(
        "authenticator/confirm",
        {"code": module.totp(replacement["secret"], clock[0] // 30)},
    )
    call("authenticator/recover", {"code": recovery}, 400)
    clock[0] += 30
    # Removal of finance permissions takes effect even after successful enrollment.
    rpc("UPDATE memberships SET role='member',module_permissions='{}'")
    call(
        f"accounts/{aid}/reveal",
        {"code": module.totp(replacement["secret"], clock[0] // 30)},
        403,
    )
    rpc("UPDATE memberships SET module_permissions='{\"banking\":true}'")
    call("accounts")
    call(f"accounts/{uuid4()}/reveal", {"code": "000000"}, 404)
    audit = rpc("SELECT action FROM secure_payments.banking_access_audit")["rows"]
    assert any(x["action"] == "BANKING_DETAILS_REVEALED" for x in audit)
    assert any(x["action"] == "VERIFICATION_FAILED" for x in audit)
    claims.clear()
    call("accounts", status=401)
    print(
        f"Banking integration passed: {checks} API checks; migration idempotence, masking, tenant isolation, TOTP replay/lockout, recovery and KMS context."
    )
finally:
    proc.stdin.close()
    proc.wait(timeout=15)
