"""Disposable PostgreSQL tests. No real DB, SMTP, S3, or production secrets are used."""
import contextlib
import ast
import asyncio
import io
import json
import os
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import uuid4

from fastapi import FastAPI, HTTPException, UploadFile
from fastapi.testclient import TestClient
from starlette.datastructures import Headers
from PIL import Image

root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(root))
os.environ["BOOKDEV_VERIFICATION_SECRET"] = "test-only-secret-" * 4
proc = subprocess.Popen(["node", "--preserve-symlinks", "--preserve-symlinks-main", str(root / "tests/banking_pg.cjs")], stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True, encoding="utf-8")
assert json.loads(proc.stdout.readline())["ready"]


def rpc(sql, params=(), execute=False):
    proc.stdin.write(json.dumps({"sql": sql, "params": params, "exec": execute}, default=str) + "\n")
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
            for name, value in row.items():
                if value is not None and name.endswith("_at"):
                    row[name] = datetime.fromisoformat(value.replace("Z", "+00:00"))
    def fetchone(self):
        return self.rows[0] if self.rows else None
    def fetchall(self):
        return self.rows
    def __enter__(self):
        return self
    def __exit__(self, *args):
        pass


class Connection:
    autocommit = True
    def cursor(self, **kwargs):
        return Cursor()
    def commit(self):
        pass
    def rollback(self):
        pass
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
    rpc("""CREATE TABLE works(id uuid PRIMARY KEY,tenant_id uuid,title text,subtitle text);
    CREATE TABLE bookdev_requests(id uuid PRIMARY KEY, tenant_slug text, tenant_id uuid, work_id uuid,
    request_type text,party text,recipient_name text,recipient_email text,requester_email text,
    token_hash text,status text,expires_at timestamptz,completed_at timestamptz,updated_at timestamptz,
    payload_json jsonb,response_json jsonb);
    CREATE TABLE work_contributors(tenant_id uuid,work_id uuid,party_id uuid);""", execute=True)
    migration = (root / "migrations/025_bookdev_request_verification.sql").read_text()
    rpc(migration, execute=True)
    rpc(migration, execute=True)  # deployment can safely retry this migration
    from routers import bookdev_email as routes
    from app import bookdev_security as security, bookdev_photos as photos
    from app.bookdev_body_limit import BookdevBodyLimit
    routes.db_conn = security.db_conn = db
    tenant, work, party_id = [str(uuid4()) for _ in range(3)]
    rpc("INSERT INTO works VALUES ($1,$2,'Private Title','')", (work, tenant))
    rpc("INSERT INTO work_contributors VALUES ($1,$2,$3)", (tenant, work, party_id))
    timestamp = [datetime.now(timezone.utc)]
    security.now = lambda: timestamp[0]
    emails = []
    routes._send_recipient_code = lambda row, code: emails.append((row["id"], row["recipient_email"], code))
    routes._load_public_form_prefill = lambda **kwargs: {"private_information": "Only after verification"}
    app = FastAPI()
    app.include_router(routes.router, prefix="/api")
    app.add_middleware(BookdevBodyLimit)
    client = TestClient(app)
    checks = [0]

    def make_request(kind="MARKETING_PROFILE", status="sent", expires=None, contributor=party_id):
        token = uuid4().hex + uuid4().hex
        request_id = str(uuid4())
        rpc("""INSERT INTO bookdev_requests(id,tenant_slug,tenant_id,work_id,request_type,party,
            recipient_name,recipient_email,token_hash,status,expires_at,payload_json,response_json)
            VALUES ($1,'publisher',$2,$3,$4,'author','Recipient','original@example.com',$5,$6,$7,$8,'{}')""",
            (request_id,tenant,work,kind,routes._token_hash(token),status,expires or timestamp[0]+timedelta(days=1),json.dumps({"contributor_party_id":contributor})))
        return request_id, f"/api/project-management/book-development/requests/{token}"

    def hit(method, path, *, status=200, session=None, **kwargs):
        headers = kwargs.pop("headers", {})
        if session:
            headers["x-bookdev-session"] = session
        response = client.request(method, path, headers=headers, **kwargs)
        assert response.status_code == status, (method, path, response.status_code, response.text)
        checks[0] += 1
        return response.json()

    def authenticate(path):
        hit("POST", path+"/verification-code", json={"recipient_email":"attacker@example.com"})
        assert emails[-1][1] == "original@example.com"
        code = emails[-1][2]
        verified = hit("POST", path+"/verify", json={"code":code})
        return verified["session_token"], code

    first_id, first = make_request()
    assert hit("GET", first) == {"verification_required": True}
    session, used_code = authenticate(first)
    hit("POST", first+"/verify", status=400, json={"code":used_code})  # one-time code
    assert "prefill" in hit("GET", first, session=session)
    assert hit("GET", first) == {"verification_required": True}  # forwarded URL lacks cookie proof
    _, another = make_request()
    assert hit("GET", another, session=session) == {"verification_required": True}
    for endpoint in ["contributor-info", "media-questionnaire", "marketing-profile", "sales-information", "photo"]:
        hit("POST", first+"/"+endpoint, status=403, json={})
    hit("POST", another+"/marketing-profile", session=session, status=403, json={})
    hit("POST", first+"/photo", session=session, status=400, json={"url":"https://attacker.invalid/photo.jpg"})
    hit("POST", first+"/marketing-profile", session=session, json={"notes":"Saved"})
    hit("POST", first+"/marketing-profile", session=session, status=410, json={})
    hit("GET", first, session=session, status=410)
    hit("POST", first+"/verification-code", status=410)

    sales_id, sales = make_request("SALES_INFORMATION")
    sales_session, _ = authenticate(sales)
    hit("POST", sales+"/sales-information", session=sales_session, json={"notes":"Sales"})
    assert rpc("SELECT status FROM bookdev_requests WHERE id=$1", [sales_id])["rows"][0]["status"] == "completed"

    bad_id, bad = make_request()
    hit("POST", bad+"/verification-code")
    hit("POST", bad+"/verification-code", status=429)
    wrong = "00000000" if emails[-1][2] != "00000000" else "11111111"
    for _ in range(5):
        hit("POST", bad+"/verify", status=400, json={"code":wrong})
    hit("POST", bad+"/verify", status=429, json={"code":emails[-1][2]})
    assert rpc("SELECT failed_attempts FROM bookdev_request_verification WHERE request_id=$1", [bad_id])["rows"][0]["failed_attempts"] == 5
    for _ in range(4):
        timestamp[0] += timedelta(seconds=61)
        hit("POST", bad+"/verification-code")
    timestamp[0] += timedelta(seconds=61)
    hit("POST", bad+"/verification-code", status=429)
    timestamp[0] += timedelta(minutes=11)
    hit("POST", bad+"/verify", status=400, json={"code":emails[-1][2]})

    _, expiring = make_request()
    proof, _ = authenticate(expiring)
    timestamp[0] += timedelta(hours=1,seconds=1)
    assert hit("GET", expiring, session=proof) == {"verification_required":True}
    hit("POST", expiring+"/marketing-profile", session=proof, status=403, json={})
    for status in ["completed", "revoked", "expired"]:
        _, path = make_request(status=status)
        hit("GET", path, status=410)
    _, expired = make_request(expires=timestamp[0]-timedelta(seconds=1))
    hit("POST", expired+"/verification-code", status=410)
    _, unbound = make_request(contributor="")
    hit("POST", unbound+"/verification-code", status=409)
    removed_id, removed = make_request(contributor=str(uuid4()))
    hit("POST", removed+"/verification-code", status=409)

    def photo(data, mime="image/png"):
        return UploadFile(filename="../../malicious.png", file=io.BytesIO(data), headers=Headers({"content-type":mime}))
    buffer = io.BytesIO()
    Image.new("RGB",(16,16),"red").save(buffer,format="PNG")
    image = buffer.getvalue()
    encoded,w,h = photos.normalize_photo(photo(image+b"<script>not copied</script>"))
    assert (w,h)==(16,16) and b"<script>" not in encoded and encoded[:2] == b"\xff\xd8"
    for data,mime,expected in [(b"<svg/>","image/png",400),(image,"image/jpeg",415),(image,"image/svg+xml",415),(b"x"*(photos.MAX_PHOTO_BYTES+1),"image/png",413),(b"","image/png",400)]:
        try:
            photos.normalize_photo(photo(data,mime))
            raise AssertionError("Invalid photo accepted")
        except HTTPException as error:
            assert error.status_code==expected
            checks[0]+=1
    original_cap = photos.MAX_PHOTO_PIXELS
    photos.MAX_PHOTO_PIXELS=100
    try:
        photos.normalize_photo(photo(image))
        raise AssertionError("Oversized pixel dimensions accepted")
    except HTTPException as error:
        assert error.status_code==400
    finally:
        photos.MAX_PHOTO_PIXELS=original_cap

    photo_id, photo_path = make_request("AUTHOR_PHOTO")
    hit("POST", photo_path+"/photo-upload", status=403, files={"file":("p.png",image,"image/png")})
    photo_session,_ = authenticate(photo_path)
    hit("POST", photo_path+"/photo-upload", session=photo_session, status=400, files={"file":("fake.png",b"not an image","image/png")})
    saved = []
    def save(row, target, data, width, height):
        assert row["id"]==photo_id and row["work_id"]==work and row["tenant_id"]==tenant and target==party_id
        saved.append(True)
        return {"key":"server-selected-key", "filename":"server-photo.jpg", "mime":"image/jpeg"}
    photos.save_request_photo=save
    hit("POST", photo_path+"/photo-upload", session=photo_session,
        files={"file":("photo.png",image,"image/png")}, data={"workId":str(uuid4()),"partyId":str(uuid4()),"key":"attacker-key"})
    assert saved == [True]
    hit("POST", photo_path+"/photo-upload", session=photo_session, status=410, files={"file":("p.png",image,"image/png")})
    _, large_photo = make_request("AUTHOR_PHOTO")
    large_session,_ = authenticate(large_photo)
    hit("POST", large_photo+"/photo-upload", session=large_session, status=413, content=b"x"*(11*1024*1024), headers={"content-type":"multipart/form-data; boundary=x"})
    hit("POST", another+"/photo-upload", status=403, content=b"x"*(11*1024*1024), headers={"content-type":"multipart/form-data; boundary=x"})

    # A stale pre-lock snapshot cannot bypass the atomic completion check.
    _, racing = make_request()
    racing_session,_ = authenticate(racing)
    stale = routes._get_request_by_token_hash(routes._token_hash(racing.split("/")[-1]))
    hit("POST", racing+"/marketing-profile", session=racing_session, json={"notes":"first"})
    real_lookup = routes._get_request_by_token_hash
    routes._get_request_by_token_hash = lambda token_hash: stale
    try:
        hit("POST", racing+"/marketing-profile", session=racing_session, status=410, json={"notes":"replay"})
    finally:
        routes._get_request_by_token_hash = real_lookup

    # Multiple contributors on a book must not expose the first author's data.
    rpc("ALTER TABLE work_contributors ADD COLUMN contributor_role text DEFAULT 'A01'; ALTER TABLE work_contributors ADD COLUMN sequence_number integer DEFAULT 1; CREATE TABLE parties(id uuid,tenant_id uuid,display_name text,email text,created_at timestamptz DEFAULT now());", execute=True)
    second_party = str(uuid4())
    rpc("INSERT INTO parties(id,tenant_id,display_name,email) VALUES ($1,$2,'First author','first@example.com'),($3,$2,'Selected author','selected@example.com')", [party_id,tenant,second_party])
    rpc("INSERT INTO work_contributors(tenant_id,work_id,party_id,sequence_number) VALUES ($1,$2,$3,2)", [tenant,work,second_party])
    prefill = routes._load_media_questionnaire_prefill(tenant_id=tenant,work_id=work,party='author',contributor_party_id=second_party)
    assert prefill['contributor']['party_id'] == second_party
    assert prefill['contributor']['email'] == 'selected@example.com'

    # Happy paths for the other two form writers retain the bound contributor.
    media_id,media_path = make_request('MEDIA_QUESTIONNAIRE', contributor=second_party)
    media_session,_ = authenticate(media_path)
    media_saved=[]
    routes._save_media_questionnaire = lambda cur,tenant_id,target,scope,payload: media_saved.append(target)
    hit('POST',media_path+'/media-questionnaire',session=media_session,json={})
    assert media_saved == [second_party]
    contributor_id,contributor_path = make_request('CONTRIBUTOR_INFO',contributor=second_party)
    contributor_session,_ = authenticate(contributor_path)
    contributor_saved=[]
    routes._bookdev_update_contributor_core = lambda cur,tenant_id,target,*args: contributor_saved.append(target)
    routes._ensure_work_contributor = lambda *args: None
    hit('POST',contributor_path+'/contributor-info',session=contributor_session,json={'contributor_name':'Selected author','contributor_email':'selected@example.com','contributor_party_id':party_id})
    assert contributor_saved == [second_party]

    real_save = __import__('importlib').reload(photos).save_request_photo
    from routers import uploads
    original_work,original_people,original_s3 = uploads._resolve_work,uploads._photo_contributors,uploads._s3_client
    photo_row = real_lookup(routes._token_hash(large_photo.split('/')[-1]))
    uploads._resolve_work=lambda candidate: {'id':work,'tenant_id':str(uuid4())}
    try:
        real_save(photo_row,party_id,encoded,w,h)
        raise AssertionError('Cross-tenant upload accepted')
    except HTTPException as error:
        assert error.status_code==403
    uploads._resolve_work=lambda candidate: {'id':work,'tenant_id':tenant}
    uploads._photo_contributors=lambda candidate: []
    try:
        real_save(photo_row,party_id,encoded,w,h)
        raise AssertionError('Unassigned contributor accepted')
    except HTTPException as error:
        assert error.status_code==409
    finally:
        uploads._resolve_work,uploads._photo_contributors,uploads._s3_client=original_work,original_people,original_s3

    for method,suffix in [("GET",""),("POST","/verify"),("POST","/photo-upload")]:
        assert security.public_request_endpoint(first+suffix,method)
    assert not security.public_request_endpoint(first+"/photo-upload","DELETE")
    assert not security.public_request_endpoint(first+"/photo-upload/extra","POST")
    assert not security.public_request_endpoint("/api/project-management/book-development/work/requests","POST")
    # Verification configuration fails closed rather than exposing an unprotected form.
    configured_key = os.environ.pop('BOOKDEV_VERIFICATION_SECRET')
    try:
        _, not_configured = make_request()
        hit('POST',not_configured+'/verification-code',status=503)
    finally:
        os.environ['BOOKDEV_VERIFICATION_SECRET'] = configured_key

    # Authenticated senders bind an exact saved contributor and cannot bind another book.
    rpc('ALTER TABLE works ADD COLUMN uid text DEFAULT \'book-uid\';',execute=True)
    captured=[]
    routes._load_user_and_membership_or_403=lambda **kwargs: {'tenant_id':tenant,'user_id':str(uuid4())}
    routes._load_tenant_email_settings_or_400=lambda slug: {'smtp_secret_id':'test','smtp_host':'mock','smtp_port':465,'tls_mode':'ssl','from_email':'publisher@example.com','from_name':'Publisher'}
    routes._load_smtp_secret=lambda secret: ('mock-user','mock-password')
    routes._send_email_smtp=lambda **kwargs: 'mock-message-id'
    routes._insert_bookdev_request=lambda **kwargs: (captured.append(kwargs) or str(uuid4()))
    app.dependency_overrides[routes._ctx_from_bearer]=lambda: {'sub':'publisher'}
    with contextlib.redirect_stdout(io.StringIO()):
        hit('POST',f'/api/project-management/book-development/{work}/requests?tenant_slug=publisher',json={'request_type':'AUTHOR_PHOTO','party':'author','recipient_email':'original@example.com','contributor_party_id':second_party})
        hit('POST',f'/api/project-management/book-development/{work}/requests?tenant_slug=publisher',status=409,json={'request_type':'AUTHOR_PHOTO','party':'author','recipient_email':'original@example.com','contributor_party_id':str(uuid4())})
    assert captured[-1]['payload_json']['contributor_party_id']==second_party
    assert 'form_url' not in captured[-1]['payload_json']

    # Exercise the production account-auth middleware without importing main.py
    # (which would initialize production services and print registered routes).
    from starlette.requests import Request
    from starlette.responses import JSONResponse
    parsed = ast.parse((root / 'main.py').read_text(encoding='utf-8'))
    auth_nodes = [node for node in parsed.body if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name in {'_bearer_token_from_header','_token_from_cookie','require_auth_middleware'}]
    for node in auth_nodes:
        node.decorator_list = []
    auth_globals = {'REQUIRE_AUTH':True,'JSONResponse':JSONResponse}
    exec(compile(ast.Module(body=auth_nodes,type_ignores=[]),'<production auth middleware>','exec'),auth_globals)
    async def downstream(request):
        return JSONResponse({'ok':True})
    def middleware_status(path,method='POST'):
        request=Request({'type':'http','method':method,'path':path,'scheme':'https','server':('testserver',443),'headers':[],'query_string':b''})
        return asyncio.run(auth_globals['require_auth_middleware'](request,downstream))
    assert middleware_status(large_photo+'/verify').status_code==200
    assert middleware_status(large_photo,'GET').headers['Cache-Control']=='private, no-store'
    assert middleware_status('/api/project-management/book-development/work-id/requests').status_code==401
    assert middleware_status(large_photo+'/unexpected').status_code==401
    auth_globals['REQUIRE_AUTH']=False
    for method in ['POST','PUT','DELETE']:
        assert middleware_status('/api/uploads',method).status_code==401
    checks[0]+=7

    print(f"PASS: {checks[0]} recipient-verification and photo security checks; disposable PostgreSQL only.")
finally:
    proc.stdin.close()
    proc.wait(timeout=30)
