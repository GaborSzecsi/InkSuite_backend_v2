"""Execute actual distribution SQL and services in disposable PostgreSQL WASM. No AWS DSN."""
import contextlib,json,subprocess,unittest
from pathlib import Path
from uuid import uuid4
from unittest.mock import patch
from fastapi import FastAPI,HTTPException
from fastapi.testclient import TestClient
from psycopg.types.json import Jsonb
from app.distribution import service as s
from app.distribution.models import OrderIn
from app.distribution.router import router,access,admin

proc=None

def rpc(sql,params=(),execute=False):
 params=[v.obj if isinstance(v,Jsonb) else str(v) if isinstance(v,__import__('uuid').UUID) else v for v in params]
 proc.stdin.write(json.dumps({'sql':sql,'params':params,'exec':execute})+'\n');proc.stdin.flush()
 result=json.loads(proc.stdout.readline())
 if 'error' in result:raise RuntimeError(result['error'])
 return result['result']

class Cursor:
 def execute(self,sql,params=()):
  for i in range(len(params)):sql=sql.replace('%s',f'${i+1}',1)
  self.data=rpc(sql,params)['rows'];return self
 def fetchone(self):return self.data[0] if self.data else None
 def fetchall(self):return self.data
 def __enter__(self):return self
 def __exit__(self,*args):pass
class Conn:
 @contextlib.contextmanager
 def transaction(self):
  rpc('BEGIN')
  try:yield
  except BaseException:rpc('ROLLBACK');raise
  else:rpc('COMMIT')
 def cursor(self,**kw):return Cursor()
@contextlib.contextmanager
def db():yield Conn()

class DistributionSQLTests(unittest.TestCase):
 @classmethod
 def setUpClass(cls):
  global proc
  proc=subprocess.Popen(['node','--preserve-symlinks','--preserve-symlinks-main',str(Path(__file__).with_name('distribution_pg.cjs'))],stdin=subprocess.PIPE,stdout=subprocess.PIPE,text=True,encoding='utf-8')
  ready=json.loads(proc.stdout.readline());assert ready.get('ready'),ready
  cls.patch=patch.object(s,'db_conn',db);cls.patch.start()
 @classmethod
 def tearDownClass(cls):
  cls.patch.stop();proc.stdin.close();proc.wait(timeout=20);proc.stdout.close()
 def setUp(self):
  self.tenant=str(uuid4());self.other=str(uuid4());self.work=str(uuid4());self.ed=str(uuid4());self.connection=str(uuid4());self.user=str(uuid4())
  rpc('INSERT INTO tenants VALUES($1),($2)',[self.tenant,self.other]);rpc('INSERT INTO users VALUES($1)',[self.user])
  rpc('INSERT INTO works VALUES($1,$2,$3)',[self.work,self.tenant,'Test Book'])
  rpc("INSERT INTO editions(id,tenant_id,work_id,isbn13,product_form,onix_product_form) VALUES($1,$2,$3,$4,'Hardcover',$5)",[self.ed,self.tenant,self.work,'9780000000002','BB'])
  rpc("INSERT INTO distribution_connections(id,tenant_id,display_name,adapter) VALUES($1,$2,'Test distributor','hachette_exporteo')",[self.connection,self.tenant])
  self.body=OrderIn(connection_id=self.connection,source='SHOPIFY',source_account='example.myshopify.com',external_order_id='100',reference='#100',recipient={'name':'Test','address_1':'1 Test Rd','city':'Test','postal_code':'10001','country':'US'},shipping_method='Ground',items=[{'edition_id':self.ed,'quantity':1}])
  self.ctx={'tenant':{'id':self.tenant},'user':{'id':self.user},'membership_role':'tenant_admin'}
 def create(self,body=None):
  with s.transaction() as cur:return s.create_order(cur,self.tenant,body or self.body,self.user)
 def live(self,order):
  rpc("UPDATE distribution_connections SET enabled=true,mode='live',safety_buffer=0 WHERE id=$1",[self.connection])
  rpc("INSERT INTO distribution_inventory(tenant_id,connection_id,edition_id,distributor_quantity,sync_status,last_synced_at,marketplace_enabled) VALUES($1,$2,$3,1,'OK',now(),true) ON CONFLICT DO NOTHING",[self.tenant,self.connection,self.ed])
  rpc("UPDATE distribution_orders SET mode='live' WHERE id=$1",[order['id']])
 def test_preview_idempotent(self):
  order=self.create();self.assertEqual(order['id'],self.create()['id']);self.assertEqual(order['mode'],'preview')
  with self.assertRaises(HTTPException):
   with s.transaction() as cur:s.reserve(cur,self.tenant,order['id'])
  self.assertEqual(rpc('SELECT COUNT(*) AS n FROM distribution_reservations WHERE tenant_id=$1',[self.tenant])['rows'][0]['n'],0)
 def test_changed_duplicate_conflict(self):
  self.create();new=self.body.model_copy(update={'reference':'changed'})
  with self.assertRaises(HTTPException) as err:self.create(new)
  self.assertEqual(err.exception.status_code,409)
 def test_tenant_isolation(self):
  with self.assertRaises(HTTPException):
   with s.transaction() as cur:s.create_order(cur,self.other,self.body,self.user)
  with self.assertRaises(RuntimeError):rpc('INSERT INTO distribution_inventory(tenant_id,connection_id,edition_id) VALUES($1,$2,$3)',[self.other,self.connection,self.ed])
 def test_last_unit_and_cancel(self):
  a=self.create();b=self.create(self.body.model_copy(update={'external_order_id':'101'}));self.live(a);self.live(b)
  with s.transaction() as cur:s.reserve(cur,self.tenant,a['id'])
  with self.assertRaises(HTTPException):
   with s.transaction() as cur:s.reserve(cur,self.tenant,b['id'])
  with s.transaction() as cur:s.cancel(cur,self.tenant,a['id'],self.user)
  with s.transaction() as cur:s.reserve(cur,self.tenant,b['id'])
  self.assertEqual(rpc('SELECT sellable_quantity FROM distribution_availability WHERE tenant_id=$1',[self.tenant])['rows'][0]['sellable_quantity'],0)
 def test_ambiguous_submission_no_release(self):
  a=self.create();self.live(a)
  with s.transaction() as cur:s.reserve(cur,self.tenant,a['id'])
  rpc("UPDATE distribution_orders SET status='SUBMITTING' WHERE id=$1",[a['id']])
  with self.assertRaises(HTTPException):
   with s.transaction() as cur:s.cancel(cur,self.tenant,a['id'],self.user)
  self.assertEqual(rpc('SELECT sellable_quantity FROM distribution_availability WHERE tenant_id=$1',[self.tenant])['rows'][0]['sellable_quantity'],0)
 def test_stale_snapshot_zero(self):
  a=self.create();self.live(a);rpc("UPDATE distribution_inventory SET last_synced_at=now()-interval '2 days' WHERE tenant_id=$1",[self.tenant])
  with self.assertRaises(HTTPException):
   with s.transaction() as cur:s.reserve(cur,self.tenant,a['id'])
 def test_router_read_and_listings(self):
  app=FastAPI();app.include_router(router,prefix='/api');app.dependency_overrides[access]=lambda:self.ctx;app.dependency_overrides[admin]=lambda:self.ctx
  client=TestClient(app);base='/api/tenants/test/distribution'
  self.assertEqual(client.get(base+'/overview').status_code,200)
  self.assertEqual(client.get(base+'/inventory').json()['items'][0]['isbn13'],'9780000000002')
  r=client.put(base+f'/connections/{self.connection}/editions/{self.ed}/marketplace',json={'enabled':True});self.assertEqual(r.status_code,200,r.text)
  self.assertFalse(r.json()['checkout_enabled'])
  self.assertTrue(client.get(base+'/inventory').json()['items'][0]['marketplace_enabled'])
  r=client.post(base+'/orders/preview',json=self.body.model_dump(mode='json'));self.assertEqual(r.status_code,201,r.text)
  self.assertEqual(client.get(base+'/orders/'+r.json()['id']).status_code,200)
  self.assertEqual(client.post(base+f'/connections/{self.connection}/sync',json={}).status_code,409)
 def test_webhook_hmac_and_duplicate_delivery(self):
  import base64,hashlib,hmac,os
  from app.distribution import shopify
  install=str(uuid4());shop='test-'+self.tenant+'.myshopify.com'
  rpc('INSERT INTO distribution_shopify_installations(id,tenant_id,shop,secret_reference) VALUES($1,$2,$3,$4)',[install,self.tenant,shop,'not-read-in-test'])
  app=FastAPI();app.include_router(shopify.public_router);client=TestClient(app)
  body=b'{"id":100,"email":"not-retained@example.test"}'
  headers={'x-shopify-shop-domain':shop,'x-shopify-topic':'orders/paid','x-shopify-webhook-id':'delivery-1','content-type':'application/json'}
  with patch.dict(os.environ,{'DISTRIBUTION_SHOPIFY_APP_SECRET':'test-only'}),patch.object(shopify,'app_credentials',return_value={'client_secret':'test-secret'}):
   self.assertEqual(client.post('/api/distribution/shopify/webhooks',content=body,headers=headers).status_code,401)
   headers['x-shopify-hmac-sha256']=base64.b64encode(hmac.new(b'test-secret',body,hashlib.sha256).digest()).decode()
   for _ in range(2):self.assertEqual(client.post('/api/distribution/shopify/webhooks',content=body,headers=headers).status_code,200)
  rows=rpc('SELECT payload FROM distribution_webhooks WHERE tenant_id=$1',[self.tenant])['rows']
  self.assertEqual(len(rows),1);self.assertEqual(rows[0]['payload'],{'id':100})
 def test_unmatched_event_retained(self):
  from app.distribution import shopify
  install=str(uuid4());shop='test-'+self.tenant+'.myshopify.com'
  rpc('INSERT INTO distribution_shopify_installations(id,tenant_id,shop,secret_reference,location_id) VALUES($1,$2,$3,$4,$5)',[install,self.tenant,shop,'not-read','gid://shopify/Location/1'])
  rpc("INSERT INTO distribution_webhooks(tenant_id,installation_id,delivery_id,topic,payload) VALUES($1,$2,'delivery','orders/paid',$3)",[self.tenant,install,{'id':100}])
  order={'id':'gid://shopify/Order/100','name':'#100','cancelledAt':None,'displayFinancialStatus':'PAID','fulfillmentOrders':{'pageInfo':{'hasNextPage':False},'nodes':[{'assignedLocation':{'location':{'id':'gid://shopify/Location/1'}},'lineItems':{'pageInfo':{'hasNextPage':False},'nodes':[{'remainingQuantity':1,'lineItem':{'id':'line','sku':'unmatched','title':'Test book','variant':{'id':'variant'}}}]}}]}}
  # Restrict queue selection to this test by settling old fixtures first.
  rpc("UPDATE distribution_webhooks SET status='DONE' WHERE tenant_id<>$1",[self.tenant])
  with patch.object(shopify,'graphql',return_value={'order':order}):self.assertTrue(shopify.process_inbox_once())
  row=rpc('SELECT status,error_code FROM distribution_webhooks WHERE tenant_id=$1',[self.tenant])['rows'][0]
  self.assertEqual(row['status'],'REVIEW_REQUIRED');self.assertEqual(row['error_code'],'UNMATCHED_PRODUCTS')
  self.assertEqual(len(rpc('SELECT * FROM distribution_shopify_variants WHERE tenant_id=$1',[self.tenant])['rows']),1)
 def test_multi_item_failure_rolls_back(self):
  second=str(uuid4())
  rpc("INSERT INTO editions(id,tenant_id,work_id,isbn13,product_form,onix_product_form) VALUES($1,$2,$3,'9780000000019','Paperback','BC')",[second,self.tenant,self.work])
  body=OrderIn(**{**self.body.model_dump(mode='json'),'items':[{'edition_id':self.ed,'quantity':1},{'edition_id':second,'quantity':1}]})
  order=self.create(body);self.live(order)
  with self.assertRaises(HTTPException):
   with s.transaction() as cur:s.reserve(cur,self.tenant,order['id'])
  self.assertEqual(rpc('SELECT COUNT(*) AS n FROM distribution_reservations WHERE tenant_id=$1',[self.tenant])['rows'][0]['n'],0)
 def test_job_enqueue_idempotent(self):
  with s.transaction() as cur:
   s.enqueue(cur,self.tenant,self.connection,'TEST');s.enqueue(cur,self.tenant,self.connection,'TEST')
  self.assertEqual(rpc('SELECT COUNT(*) AS n FROM distribution_jobs WHERE tenant_id=$1',[self.tenant])['rows'][0]['n'],1)
if __name__=='__main__':unittest.main()
