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


try:
    from types import SimpleNamespace
    from unittest.mock import patch
    from app.marketplace import documents_service as docs
    # rpc.cjs loads the inspected live publishing_assets schema.
    tid,tid2,work,work2,eid,eid2,org,org2,bid=[str(uuid4()) for _ in range(9)]
    seed("INSERT INTO tenants(id,slug,name,s3_prefix) VALUES($1,'publisher','Publisher','publisher/'),($2,'other','Other','other/')",[tid,tid2])
    for i,u in enumerate([admin,outsider]):
        seed("INSERT INTO users(id,cognito_sub,email) VALUES($1,$2,'fixture@example.invalid')",[u['id'],'docs-'+str(i)])
    seed("INSERT INTO memberships(tenant_id,user_id,role) VALUES($1,$2,'tenant_admin'),($3,$4,'tenant_admin')",[tid,admin['id'],tid2,outsider['id']])
    seed("INSERT INTO works(id,tenant_id,uid,title) VALUES($1,$2,$1,'Public Book'),($3,$4,$3,'Other Title')",[work,tid,work2,tid2])
    seed("INSERT INTO editions(id,work_id,tenant_id,isbn13,product_form) VALUES($1,$2,$3,'9781234567890','BC'),($4,$5,$6,'9781234567891','BC')",[eid,work,tid,eid2,work2,tid2])
    seed("INSERT INTO marketplace_organizations(id,tenant_id,organization_type,name,slug,status) VALUES($1,$2,'publisher','Publisher','publisher','active'),($3,$4,'publisher','Other','other','active')",[org,tid,org2,tid2])
    seed("INSERT INTO marketplace_books(id,publisher_organization_id,work_id,slug,marketplace_status) VALUES($1,$2,$3,'public-book','public')",[bid,org,work])
    from io import BytesIO
    class Store:
        exceptions=SimpleNamespace(NoSuchKey=KeyError)
        def generate_presigned_post(self,**kwargs):
            assert kwargs["ExpiresIn"]==900
            return {"url":"https://storage.example.invalid","fields":{"key":kwargs["Key"],**kwargs["Fields"]}}
        def get_object(self,**kwargs):
            obj=self.objects[kwargs["Key"]]
            return {"Body":BytesIO(obj["Body"]),"ContentLength":len(obj["Body"]),"Metadata":obj["Metadata"]}
        def __init__(self): self.objects={};self.meta=SimpleNamespace(region_name='us-east-2');self.deleted=[]
        def put_object(self,**kwargs): self.objects[kwargs['Key']]=kwargs
        def delete_object(self,**kwargs):self.deleted.append(kwargs['Key']);self.objects.pop(kwargs['Key'],None)
        def generate_presigned_url(self,operation,Params,ExpiresIn):
            assert operation=='get_object' and ExpiresIn==900
            assert Params['ResponseContentType']=='application/pdf' and Params['ResponseContentDisposition'].startswith('attachment;')
            assert Params['Key'] in self.objects
            return 'https://files.example.invalid/download.pdf?signature=fixture'
    store=Store();docs.storage=lambda:(store,'fixture-bucket')
    pdf=b'%PDF-1.7\n1 0 obj << /Type /Catalog >> endobj\n%%EOF\n'
    def upload(path,name='Press kit.pdf',data=pdf,status=200):
        global checks
        r=client.post('/api/marketplace/'+path,files={'file':(name,data,'application/pdf')})
        assert r.status_code==status,(r.status_code,r.text);checks+=1
        return r.json()
    user=admin
    upload(f'organizations/{org}/catalogs',status=503)
    assert not store.objects
    rpc((root.parent/'migrations/036_marketplace_public_documents.sql').read_text(encoding='utf-8-sig'),execute=True)
    a=upload(f'editions/{eid}/marketing-materials')
    a2=upload(f'editions/{eid}/marketing-materials')
    c=upload(f'organizations/{org}/catalogs','Catalog.pdf')
    c2=upload(f'organizations/{org}/catalogs','Catalog.pdf')
    assert len(store.objects)==4
    assert all(f'tenants/publisher/data/uploads/{work}/public/marketing/' in k for k in list(store.objects)[:2])
    assert all('/assets/marketing/public/catalogs/' in k for k in list(store.objects)[2:])
    assert all(v['ContentType']=='application/pdf' for v in store.objects.values())
    assert len(call('GET',f'editions/{eid}/marketing-materials')['items'])==2
    assert len(call('GET',f'organizations/{org}/catalogs')['items'])==2
    assert len(call('GET','publishers/publisher/catalogs')['items'])==2
    assert call('GET','publishers/publisher/catalogs')['items'][0]['url'].startswith('/api/marketplace/documents/')
    # Documents reject renamed non-PDF files and non-PDF extensions.
    upload(f'editions/{eid}/marketing-materials','bad.pdf',b'<html>not pdf</html>',422)
    upload(f'editions/{eid}/marketing-materials','bad.html',pdf,422)
    user=outsider
    upload(f'editions/{eid}/marketing-materials',status=403)
    upload(f'organizations/{org}/catalogs',status=403)
    call('GET',f'editions/{eid}/marketing-materials',status=403)
    call('GET',f'organizations/{org}/catalogs',status=403)
    # Public lists and downloads work even if authentication would reject the visitor.
    from fastapi import HTTPException
    def no_auth(): raise HTTPException(401,'Sign in')
    app.dependency_overrides[core.current_user]=no_auth
    assert len(call('GET','books/public-book/marketing-materials')['items'])==2
    response=client.get('/api/marketplace'+a['url'].removeprefix('/api/marketplace'),follow_redirects=False)
    assert response.status_code==302 and response.headers['location'].startswith('https://files.example.invalid/')
    response=client.get(c['url'],follow_redirects=False);assert response.status_code==302
    seed("UPDATE marketplace_books SET marketplace_status='draft' WHERE id=$1",[bid])
    call('GET','books/public-book/marketing-materials',status=404)
    response=client.get(a['url'],follow_redirects=False);assert response.status_code==404
    seed("UPDATE marketplace_books SET marketplace_status='public' WHERE id=$1",[bid])
    seed("UPDATE publishing_assets SET is_public=false WHERE id=$1",[a['id']])
    assert len(call('GET','books/public-book/marketing-materials')['items'])==1
    assert client.get(a['url'],follow_redirects=False).status_code==404
    seed("UPDATE marketplace_organizations SET status='hidden' WHERE id=$1",[org])
    assert client.get(c['url'],follow_redirects=False).status_code==404
    seed("UPDATE marketplace_organizations SET status='active' WHERE id=$1",[org])
    app.dependency_overrides[core.current_user]=lambda:user
    user=admin
    # Failed SQL after storage write rolls back the asset and cleans only that object.
    before=set(store.objects)
    with patch.object(docs.repo,'save',side_effect=RuntimeError('fixture SQL failure')):
        try: docs.upload(eid,None,user,'Failure.pdf',pdf);assert False
        except RuntimeError: pass
    assert set(store.objects)==before and store.deleted
    seed("UPDATE publishing_assets SET storage_key='tenants/other/private/secret.pdf' WHERE id=$1",[c['id']])
    assert client.get(c['url'],follow_redirects=False).status_code==404
    assert seed("SELECT count(*) AS n FROM publishing_assets")[0]['n']==4
    # Neither title materials nor publisher catalogs silently stop after 15 files.
    for i in range(16):
        upload(f'editions/{eid}/marketing-materials',f'Kit {i}.pdf')
        upload(f'organizations/{org}/catalogs',f'Catalog {i}.pdf')
    assert len(call('GET','books/public-book/marketing-materials')['items'])==17
    assert len(call('GET','publishers/publisher/catalogs')['items'])==18
    assert len(call('GET',f'editions/{eid}/marketing-materials')['items'])==17
    assert len(call('GET',f'organizations/{org}/catalogs')['items'])==18
    with patch.object(docs,'MAX_PDF',10):
        upload(f'editions/{eid}/marketing-materials','Too large.pdf',pdf,413)

    # Large files travel directly to storage; only metadata reaches API routes.
    def stage(path,data=pdf):
        target=call('POST',path+'/uploads',{'filename':'Large.pdf','size':len(data)})
        fields=target['upload']['fields']
        assert '/public/' not in target['key'] and '/assets/' not in target['key']
        metadata={k.removeprefix('x-amz-meta-'):v for k,v in fields.items() if k.startswith('x-amz-meta-')}
        store.put_object(Key=target['key'],Body=data,Metadata=metadata,ContentType='application/pdf')
        return target['key']
    catalog_path=f'organizations/{org}/catalogs'
    material_path=f'editions/{eid}/marketing-materials'
    for path in [catalog_path,material_path]:
        key=stage(path,b'%PDF-1.7\n'+b' '* (8*1024*1024)+b'\n%%EOF')
        result=call('POST',path+'/uploads/complete',{'key':key,'filename':'Large.pdf'})
        assert key not in store.objects
        assert client.get(result['url'],follow_redirects=False).status_code==302
    key=stage(catalog_path,b'not a PDF')
    call('POST',catalog_path+'/uploads/complete',{'key':key,'filename':'Bad.pdf'},status=422)
    assert key not in store.objects
    key=stage(catalog_path)
    call('POST',material_path+'/uploads/complete',{'key':key,'filename':'Wrong.pdf'},status=403)
    assert key in store.objects
    store.objects[key]['Metadata']['upload-expires']='1'
    call('POST',catalog_path+'/uploads/complete',{'key':key,'filename':'Expired.pdf'},status=410)
    assert key not in store.objects
    user=outsider
    call('POST',catalog_path+'/uploads',{'filename':'No.pdf','size':len(pdf)},status=403)
    call('POST',material_path+'/uploads',{'filename':'No.pdf','size':len(pdf)},status=403)

    print(f'PASS: {checks} document REST/SQL requests plus anonymous download, public visibility, tenant isolation, PDF validation, unique filenames and storage rollback checks. No live database or S3 writes.')
finally:
    proc.stdin.close();proc.wait(timeout=10)
