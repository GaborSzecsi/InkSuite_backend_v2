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
 params=[v.obj if isinstance(v,Jsonb) else str(v) if isinstance(v,__import__('uuid').UUID) else v.isoformat() if isinstance(v,__import__('datetime').datetime) else v for v in params]
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
 def publishing_fixture(self):
  from app.distribution import publishing
  install={'id':str(uuid4()),'tenant_id':self.tenant}
  rpc("INSERT INTO distribution_shopify_installations(id,tenant_id,shop,secret_reference) VALUES($1,$2,$3,'test')",[install['id'],self.tenant,str(uuid4())+'.myshopify.com'])
  item={'id':self.ed,'work_id':self.work,'status':'Ready to add','isbn':'9780306406157'}
  return publishing,install,item
 def test_publish_metadata_review_can_be_resubmitted_with_fresh_selection(self):
  publishing,install,item=self.publishing_fixture()
  for message in ('InkSuite metadata changed; review selection again','InkSuite artwork changed; review selection again'):
   rpc("DELETE FROM distribution_shopify_publish_jobs WHERE installation_id=$1",[install['id']])
   with patch.object(publishing,'catalog',return_value={'can_publish':True,'editions':[item]}):
    first=publishing.enqueue(install,[self.ed],self.connection)
    rpc("UPDATE distribution_shopify_publish_jobs SET status='REVIEW_REQUIRED',message=$1 WHERE id=$2",[message,first['jobs'][0]['id']])
    item['isbn']='9780140328721'
    second=publishing.enqueue(install,[self.ed],self.connection)
   self.assertNotEqual(first['jobs'][0]['id'],second['jobs'][0]['id'])
   rows=rpc('SELECT status,selection,message FROM distribution_shopify_publish_jobs WHERE installation_id=$1 ORDER BY created_at',[install['id']])['rows']
   self.assertEqual([r['status'] for r in rows],['SUPERSEDED','QUEUED'])
   self.assertEqual(rows[0]['message'],message)
   self.assertEqual(rows[1]['selection'][0]['isbn'],'9780140328721')
 def test_publish_pending_submission_returns_existing_job_without_duplicate(self):
  publishing,install,item=self.publishing_fixture()
  with patch.object(publishing,'catalog',return_value={'can_publish':True,'editions':[item]}):
   first=publishing.enqueue(install,[self.ed],self.connection)
   second=publishing.enqueue(install,[self.ed],self.connection)
   self.assertEqual(first['jobs'],second['jobs'])
   rpc("UPDATE distribution_shopify_publish_jobs SET status='RUNNING' WHERE id=$1",[first['jobs'][0]['id']])
   third=publishing.enqueue(install,[self.ed],self.connection)
   self.assertEqual(third['jobs'][0]['status'],'RUNNING')
  self.assertEqual(rpc('SELECT count(*) AS n FROM distribution_shopify_publish_jobs WHERE installation_id=$1',[install['id']])['rows'][0]['n'],1)
 def test_publish_uncertain_shopify_outcome_returns_error_not_fake_queued(self):
  publishing,install,item=self.publishing_fixture()
  with patch.object(publishing,'catalog',return_value={'can_publish':True,'editions':[item]}):
   first=publishing.enqueue(install,[self.ed],self.connection)
   rpc("UPDATE distribution_shopify_publish_jobs SET status='REVIEW_REQUIRED',message='Interrupted publication: inspect Shopify before retrying' WHERE id=$1",[first['jobs'][0]['id']])
   with self.assertRaises(HTTPException) as err:publishing.enqueue(install,[self.ed],self.connection)
   self.assertEqual(err.exception.status_code,409)
   self.assertIn('Interrupted publication',err.exception.detail)
  self.assertEqual(rpc('SELECT count(*) AS n FROM distribution_shopify_publish_jobs WHERE installation_id=$1',[install['id']])['rows'][0]['n'],1)
 def test_preview_idempotent(self):
  order=self.create();self.assertEqual(order['id'],self.create()['id']);self.assertEqual(order['mode'],'preview')
  with self.assertRaises(HTTPException):
   with s.transaction() as cur:s.reserve(cur,self.tenant,order['id'])
  self.assertEqual(rpc('SELECT COUNT(*) AS n FROM distribution_reservations WHERE tenant_id=$1',[self.tenant])['rows'][0]['n'],0)
 def test_changed_duplicate_conflict(self):
  self.create();new=self.body.model_copy(update={'shipping_method':'changed'})
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
  order={'createdAt':__import__('datetime').datetime.now(__import__('datetime').timezone.utc).isoformat(),'id':'gid://shopify/Order/100','name':'#100','cancelledAt':None,'displayFinancialStatus':'PAID','fulfillmentOrders':{'pageInfo':{'hasNextPage':False},'nodes':[{'assignedLocation':{'location':{'id':'gid://shopify/Location/1'}},'lineItems':{'pageInfo':{'hasNextPage':False},'nodes':[{'remainingQuantity':1,'lineItem':{'id':'line','sku':'unmatched','title':'Test book','variant':{'id':'variant'}}}]}}]}}
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

 def privacy_install(self):
  install=str(uuid4());shop=self.body.source_account+'-unused'
  shop='test-'+self.tenant+'.myshopify.com'
  rpc('INSERT INTO distribution_shopify_installations(id,tenant_id,shop,secret_reference) VALUES($1,$2,$3,$4)',[install,self.tenant,shop,f'inksuite/distribution/{self.tenant}/{install}'])
  self.body=self.body.model_copy(update={'source_account':shop})
  rpc("UPDATE distribution_privacy_requests SET status='DONE' WHERE tenant_id<>$1",[self.tenant])
  return {'id':install,'tenant_id':self.tenant,'shop':shop}
 def privacy_request(self,install,topic,ids=None):
  from app.distribution import privacy
  payload={'orders_requested':ids or [],'orders_to_redact':ids or [],'customer':{'email':'must-not-store@example.test','phone':'5555555555','id':23}}
  with s.transaction() as cur:privacy.receive(cur,install,topic,str(uuid4()),payload)
  return rpc('SELECT * FROM distribution_privacy_requests WHERE tenant_id=$1 ORDER BY created_at DESC',[self.tenant])['rows'][0]
 def test_embedded_link_code_is_single_use_and_scoped(self):
  from app.distribution import embedded as em
  rpc("INSERT INTO memberships VALUES($1,$2,'tenant_admin')",[self.tenant,self.user])
  code=em.link_code(self.ctx)['code'];auth={'shop':'embedded-'+self.tenant+'.myshopify.com','sub':'42'}
  with patch.object(em,'exchange',return_value={'access_token':'test'}),patch.object(em.sh.secrets,'save',return_value='test-reference'):
   self.assertTrue(em.link(em.LinkIn(code=code),auth)['linked'])
   with self.assertRaises(HTTPException):em.link(em.LinkIn(code=code),auth)
  install=em.linked(auth);self.assertEqual(install['tenant_id'],self.tenant)
  with self.assertRaises(HTTPException):em.linked({**auth,'sub':'43'})
  with self.assertRaises(HTTPException):em.linked({**auth,'shop':'other.myshopify.com'})
  rpc("UPDATE memberships SET role='tenant_editor' WHERE tenant_id=$1",[self.tenant])
  with self.assertRaises(HTTPException):em.linked(auth)
 def test_uninstall_releases_store_without_transferring_publisher_data(self):
  import base64,hashlib,hmac,os
  from app.distribution import embedded as em, shopify
  rpc("INSERT INTO memberships VALUES($1,$2,'tenant_admin'),($3,$2,'tenant_admin')",[self.tenant,self.user,self.other])
  auth={'shop':'reinstall-'+self.tenant+'.myshopify.com','sub':'42'}
  other_ctx={**self.ctx,'tenant':{'id':self.other}}
  app=FastAPI();app.include_router(shopify.public_router);client=TestClient(app)
  body=b'{}';headers={'x-shopify-shop-domain':auth['shop'],'x-shopify-topic':'app/uninstalled','x-shopify-webhook-id':'uninstall-1','content-type':'application/json','x-shopify-hmac-sha256':base64.b64encode(hmac.new(b'test-secret',body,hashlib.sha256).digest()).decode()}
  with patch.object(em,'exchange',return_value={'access_token':'test'}),patch.object(em.sh.secrets,'save',return_value='test-reference'),patch.dict(os.environ,{'DISTRIBUTION_SHOPIFY_APP_SECRET':'test'}),patch.object(shopify,'app_credentials',return_value={'client_secret':'test-secret'}):
   em.link(em.LinkIn(code=em.link_code(self.ctx)['code']),auth)
   old=em.linked(auth)
   rpc("UPDATE distribution_shopify_installations SET location_id='old-location',inventory_push_enabled=true WHERE id=$1",[old['id']])
   rpc("INSERT INTO distribution_shopify_variants(tenant_id,installation_id,variant_id,title) VALUES($1,$2,'old-variant','Private old title')",[self.tenant,old['id']])
   next_code=em.link_code(other_ctx)['code']
   with self.assertRaises(HTTPException) as error:em.link(em.LinkIn(code=next_code),auth)
   self.assertEqual(error.exception.status_code,409)
   self.assertEqual(client.post('/api/distribution/shopify/webhooks',content=body,headers=headers).status_code,200)
   with self.assertRaises(HTTPException):em.linked(auth)
   previous=rpc('SELECT active,inventory_push_enabled,location_id FROM distribution_shopify_installations WHERE id=$1',[old['id']])['rows'][0]
   self.assertEqual(previous,{'active':False,'inventory_push_enabled':False,'location_id':None})
   em.link(em.LinkIn(code=next_code),auth)
   new=em.linked(auth)
   self.assertNotEqual(new['id'],old['id']);self.assertEqual(new['tenant_id'],self.other)
   self.assertIsNone(new['location_id']);self.assertFalse(new['inventory_push_enabled'])
   self.assertEqual(em.workspace(new)['variants'],[])
   self.assertEqual(rpc('SELECT COUNT(*) AS n FROM distribution_shopify_users WHERE installation_id=$1',[old['id']])['rows'][0]['n'],0)
   # Duplicate uninstall and delayed events must not disable the new connection.
   self.assertEqual(client.post('/api/distribution/shopify/webhooks',content=body,headers=headers).status_code,200)
   delayed={**headers,'x-shopify-webhook-id':'delayed-uninstall','x-shopify-triggered-at':'2020-01-01T00:00:00Z'}
   self.assertEqual(client.post('/api/distribution/shopify/webhooks',content=body,headers=delayed).status_code,200)
   self.assertEqual(em.linked(auth)['id'],new['id'])
   # Privacy requests must still reach both publishers' retained installations.
   privacy_body=json.dumps({'shop_domain':auth['shop'],'orders_to_redact':[100]}).encode()
   privacy_headers={**headers,'x-shopify-topic':'customers/redact','x-shopify-webhook-id':'privacy-both','x-shopify-hmac-sha256':base64.b64encode(hmac.new(b'test-secret',privacy_body,hashlib.sha256).digest()).decode()}
   self.assertEqual(client.post('/api/distribution/shopify/webhooks',content=privacy_body,headers=privacy_headers).status_code,200)
   self.assertEqual(len(rpc("SELECT id FROM distribution_privacy_requests WHERE delivery_id='privacy-both' AND shop=$1",[auth['shop']])['rows']),2)
 def test_same_publisher_reinstall_uses_fresh_installation(self):
  from app.distribution import embedded as em
  rpc("INSERT INTO memberships VALUES($1,$2,'tenant_admin')",[self.tenant,self.user])
  auth={'shop':'same-'+self.tenant+'.myshopify.com','sub':'42'}
  with patch.object(em,'exchange',return_value={'access_token':'test'}),patch.object(em.sh.secrets,'save',return_value='test-reference'):
   em.link(em.LinkIn(code=em.link_code(self.ctx)['code']),auth);old=em.linked(auth)
   rpc('UPDATE distribution_shopify_installations SET active=false WHERE id=$1',[old['id']])
   em.link(em.LinkIn(code=em.link_code(self.ctx)['code']),auth)
   self.assertNotEqual(em.linked(auth)['id'],old['id'])
 def test_embedded_workspace_excludes_other_stores_and_marketplace(self):
  from app.distribution import embedded as em
  install=self.privacy_install();install['user_id']=self.user;install['location_id']=None
  own=self.create()
  self.create(self.body.model_copy(update={'external_order_id':'999','source_account':'other.myshopify.com'}))
  self.create(self.body.model_copy(update={'external_order_id':'888','source':'MARKETPLACE'}))
  data=em.workspace(install)
  self.assertEqual([o['id'] for o in data['orders']],[own['id']])
  self.assertNotIn('recipient',data['orders'][0])
 def test_shopify_never_persists_delivery_details(self):
  from app.distribution import shopify
  body=self.body.model_copy(update={'delivery_instructions':'Private gift note'})
  row=self.create(body)
  self.assertEqual(row['recipient'],{});self.assertEqual(row['delivery_instructions'],'')
  self.assertNotIn('Test Rd',json.dumps(row))
  with self.assertRaises(RuntimeError):rpc('UPDATE distribution_orders SET recipient=$1 WHERE id=$2',[{'name':'Forbidden'},row['id']])
  for field in ('shippingAddress','email','phone','note','customer {'):
   self.assertNotIn(field,shopify.ORDER_QUERY)
 def test_customer_redaction_scoped_and_blocks_reimport(self):
  from app.distribution import privacy
  install=self.privacy_install();row=self.create()
  unrelated=self.create(self.body.model_copy(update={'external_order_id':'200'}))
  self.privacy_request(install,'customers/redact',['100'])
  self.assertTrue(privacy.run_once())
  self.assertEqual(rpc('SELECT id FROM distribution_orders WHERE id=$1',[row['id']])['rows'],[])
  self.assertTrue(rpc('SELECT id FROM distribution_orders WHERE id=$1',[unrelated['id']])['rows'])
  with self.assertRaises(HTTPException):self.create()
  request=rpc('SELECT * FROM distribution_privacy_requests WHERE tenant_id=$1',[self.tenant])['rows'][0]
  self.assertEqual(request['status'],'DONE');self.assertEqual(request['payload'],{});self.assertIsNone(request['shop'])
 def test_redaction_removes_unimported_inbox_and_ignores_delayed_delivery(self):
  import base64,hashlib,hmac,os
  from app.distribution import privacy,shopify
  install=self.privacy_install()
  for order_id in ('100','200'):
   rpc("INSERT INTO distribution_webhooks(tenant_id,installation_id,delivery_id,topic,payload) VALUES($1,$2,$3,'orders/paid',$4)",[self.tenant,install['id'],order_id,{'id':order_id}])
  self.privacy_request(install,'customers/redact',['100']);privacy.run_once()
  self.assertEqual(rpc('SELECT payload FROM distribution_webhooks WHERE tenant_id=$1',[self.tenant])['rows'],[{'payload':{'id':'200'}}])
  app=FastAPI();app.include_router(shopify.public_router);client=TestClient(app)
  headers={'x-shopify-shop-domain':install['shop'],'x-shopify-topic':'orders/paid','x-shopify-webhook-id':'delayed','content-type':'application/json'}
  body=json.dumps({'id':100,'email':'never-store@example.test'}).encode()
  headers['x-shopify-hmac-sha256']=base64.b64encode(hmac.new(b'test-secret',body,hashlib.sha256).digest()).decode()
  with patch.dict(os.environ,{'DISTRIBUTION_SHOPIFY_APP_SECRET':'test-only'}),patch.object(shopify,'app_credentials',return_value={'client_secret':'test-secret'}):
   self.assertEqual(client.post('/api/distribution/shopify/webhooks',content=body,headers=headers).status_code,200)
  self.assertEqual(rpc('SELECT payload FROM distribution_webhooks WHERE tenant_id=$1',[self.tenant])['rows'],[{'payload':{'id':'200'}}])
 def test_order_webhook_rejects_non_identifier_payload(self):
  import base64,hashlib,hmac,os
  from app.distribution import shopify
  install=self.privacy_install();app=FastAPI();app.include_router(shopify.public_router);client=TestClient(app)
  headers={'x-shopify-shop-domain':install['shop'],'x-shopify-topic':'orders/paid','x-shopify-webhook-id':'malformed','content-type':'application/json'}
  with patch.dict(os.environ,{'DISTRIBUTION_SHOPIFY_APP_SECRET':'test-only'}),patch.object(shopify,'app_credentials',return_value={'client_secret':'test-secret'}):
   for payload in ({'id':'private@example.test'},{'id':100,'admin_graphql_api_id':'private@example.test'}):
    body=json.dumps(payload).encode();headers['x-shopify-hmac-sha256']=base64.b64encode(hmac.new(b'test-secret',body,hashlib.sha256).digest()).decode()
    self.assertEqual(client.post('/api/distribution/shopify/webhooks',content=body,headers=headers).status_code,400)
  self.assertEqual(rpc('SELECT payload FROM distribution_webhooks WHERE tenant_id=$1',[self.tenant])['rows'],[])
 def test_data_request_download_and_delivery(self):
  from app.distribution import privacy
  install=self.privacy_install();self.create();request=self.privacy_request(install,'customers/data_request',['100'])
  privacy.run_once()
  with self.assertRaises(HTTPException):privacy.delivered(request['id'],self.ctx)
  wrong={**self.ctx,'tenant':{'id':self.other}}
  with self.assertRaises(HTTPException):privacy.export(request['id'],wrong)
  result=privacy.export(request['id'],self.ctx)
  data=json.loads(result.body);self.assertEqual(len(data['orders']),1)
  self.assertNotIn('recipient',data['orders'][0]);self.assertEqual(result.headers['cache-control'],'no-store')
  self.assertEqual(privacy.delivered(request['id'],self.ctx),{'completed':True})
 def test_shop_redaction_removes_scoped_data_and_token(self):
  from app.distribution import privacy
  install=self.privacy_install();self.create()
  rpc('UPDATE distribution_shopify_installations SET active=false WHERE id=$1',[install['id']])
  self.privacy_request(install,'shop/redact')
  with patch.object(privacy.secrets,'remove') as remove:
   privacy.run_once();remove.assert_called_once()
  self.assertEqual(rpc('SELECT * FROM distribution_orders WHERE tenant_id=$1',[self.tenant])['rows'],[])
  self.assertEqual(rpc('SELECT * FROM distribution_shopify_installations WHERE tenant_id=$1',[self.tenant])['rows'],[])
  self.assertTrue(rpc('SELECT * FROM distribution_connections WHERE tenant_id=$1',[self.tenant])['rows'])
 def test_shop_redact_does_not_remove_reinstalled_store(self):
  from app.distribution import privacy
  install=self.privacy_install();self.create();self.privacy_request(install,'shop/redact')
  with patch.object(privacy.secrets,'remove') as remove:
   privacy.run_once();remove.assert_not_called()
  self.assertTrue(rpc('SELECT * FROM distribution_orders WHERE tenant_id=$1',[self.tenant])['rows'])
  self.assertEqual(rpc('SELECT error_code FROM distribution_privacy_requests WHERE tenant_id=$1',[self.tenant])['rows'][0]['error_code'],'STORE_STILL_ACTIVE')
 def test_retention_expires_preview_only(self):
  from app.distribution import privacy
  self.privacy_install();old=self.create();fresh=self.create(self.body.model_copy(update={'external_order_id':'200'}))
  live=self.create(self.body.model_copy(update={'external_order_id':'300'}))
  rpc("UPDATE distribution_orders SET created_at=now()-interval '31 days' WHERE id IN ($1,$2)",[old['id'],live['id']])
  rpc("UPDATE distribution_orders SET mode='live' WHERE id=$1",[live['id']])
  privacy.retention_once()
  rows=rpc('SELECT id FROM distribution_orders WHERE tenant_id=$1',[self.tenant])['rows']
  self.assertEqual({r['id'] for r in rows},{fresh['id'],live['id']})
 def test_retention_deletes_completed_live_orders_and_children(self):
  from app.distribution import privacy
  self.privacy_install()
  for number,status in [('801','SHIPPED'),('802','CANCELLED')]:
   row=self.create(self.body.model_copy(update={'external_order_id':number}))
   rpc("UPDATE distribution_orders SET mode='live',status=$2,created_at=now()-interval '40 days',updated_at=now()-interval '31 days' WHERE id=$1",[row['id'],status])
   rpc("INSERT INTO distribution_jobs(tenant_id,connection_id,order_id,kind,status) VALUES($1,$2,$3,'SUBMIT','DONE')",[self.tenant,self.connection,row['id']])
  privacy.retention_once()
  for table in ('distribution_orders','distribution_order_items','distribution_events','distribution_jobs'):
   self.assertEqual(rpc(f'SELECT count(*) AS n FROM {table} WHERE tenant_id=$1',[self.tenant])['rows'][0]['n'],0)
  with s.transaction() as cur:self.assertTrue(privacy.suppressed(cur,self.tenant,self.body.source_account,'801'))
 def test_retention_preserves_unfinished_recent_and_unreconciled_live_orders(self):
  from app.distribution import privacy
  self.privacy_install()
  kept=[]
  for index,status in enumerate(('RECEIVED','ACCEPTED','REVIEW_REQUIRED','FAILED','SHIPPED','CANCELLED','SHIPPED')):
   row=self.create(self.body.model_copy(update={'external_order_id':str(900+index)}));kept.append(row['id'])
   rpc("UPDATE distribution_orders SET mode='live',status=$2,created_at=now()-interval '40 days',updated_at=now()-interval '31 days' WHERE id=$1",[row['id'],status])
  rpc('UPDATE distribution_orders SET updated_at=now() WHERE id=$1',[kept[4]])
  rpc("INSERT INTO distribution_jobs(tenant_id,connection_id,order_id,kind,status) VALUES($1,$2,$3,'TRACKING','REVIEW_REQUIRED')",[self.tenant,self.connection,kept[5]])
  rpc('INSERT INTO distribution_inventory(tenant_id,connection_id,edition_id) VALUES($1,$2,$3)',[self.tenant,self.connection,self.ed])
  rpc("INSERT INTO distribution_reservations(tenant_id,connection_id,edition_id,order_id,quantity,status) VALUES($1,$2,$3,$4,1,'FULFILLED')",[self.tenant,self.connection,self.ed,kept[6]])
  privacy.retention_once()
  self.assertEqual({r['id'] for r in rpc('SELECT id FROM distribution_orders WHERE tenant_id=$1',[self.tenant])['rows']},set(kept))
 def test_retention_honors_unresolved_live_privacy_request(self):
  from app.distribution import privacy
  install=self.privacy_install();row=self.create()
  rpc("UPDATE distribution_orders SET mode='live',status='CANCELLED',created_at=now()-interval '60 days',updated_at=now()-interval '40 days' WHERE id=$1",[row['id']])
  request=self.privacy_request(install,'customers/data_request',['100'])
  rpc("UPDATE distribution_privacy_requests SET created_at=now()-interval '40 days' WHERE id=$1",[request['id']])
  privacy.retention_once()
  self.assertTrue(rpc('SELECT id FROM distribution_orders WHERE id=$1',[row['id']])['rows'])
  rpc("UPDATE distribution_privacy_requests SET status='DONE',completed_at=now() WHERE id=$1",[request['id']])
  privacy.retention_once()
  self.assertFalse(rpc('SELECT id FROM distribution_orders WHERE id=$1',[row['id']])['rows'])
 def test_missing_order_identifiers_requires_review(self):
  from app.distribution import privacy
  install=self.privacy_install();self.create();self.privacy_request(install,'customers/redact')
  privacy.run_once()
  row=rpc('SELECT status,payload FROM distribution_privacy_requests WHERE tenant_id=$1',[self.tenant])['rows'][0]
  self.assertEqual(row['status'],'REVIEW_REQUIRED');self.assertEqual(row['payload'],{'order_ids':[]})
 def test_token_deletion_failure_does_not_claim_completion(self):
  from app.distribution import privacy
  install=self.privacy_install();self.create()
  rpc('UPDATE distribution_shopify_installations SET active=false WHERE id=$1',[install['id']])
  self.privacy_request(install,'shop/redact')
  with patch.object(privacy.secrets,'remove',side_effect=RuntimeError('denied')):privacy.run_once()
  self.assertTrue(rpc('SELECT * FROM distribution_orders WHERE tenant_id=$1',[self.tenant])['rows'])
  self.assertEqual(rpc('SELECT error_code FROM distribution_privacy_requests WHERE tenant_id=$1',[self.tenant])['rows'][0]['error_code'],'TOKEN_DELETION_FAILED')
 def test_privacy_webhook_signed_minimal_duplicate(self):
  import base64,hashlib,hmac,os
  from app.distribution import shopify
  install=self.privacy_install()
  app=FastAPI();app.include_router(shopify.public_router);client=TestClient(app)
  body=json.dumps({'shop_domain':install['shop'],'orders_to_redact':[100],'customer':{'email':'private@example.test','id':11,'phone':'555'}}).encode()
  headers={'x-shopify-shop-domain':install['shop'],'x-shopify-topic':'customers/redact','x-shopify-webhook-id':'privacy-1','content-type':'application/json'}
  with patch.dict(os.environ,{'DISTRIBUTION_SHOPIFY_APP_SECRET':'test-only'}),patch.object(shopify,'app_credentials',return_value={'client_secret':'test-secret'}):
   self.assertEqual(client.post('/api/distribution/shopify/webhooks',content=body,headers=headers).status_code,401)
   headers['x-shopify-hmac-sha256']=base64.b64encode(hmac.new(b'test-secret',body,hashlib.sha256).digest()).decode()
   for _ in range(2):self.assertEqual(client.post('/api/distribution/shopify/webhooks',content=body,headers=headers).status_code,200)
  rows=rpc('SELECT payload FROM distribution_privacy_requests WHERE tenant_id=$1',[self.tenant])['rows']
  self.assertEqual(len(rows),1);self.assertEqual(rows[0]['payload'],{'order_ids':['100']})
 def test_legacy_pii_hidden_in_ui_and_file_preview(self):
  row=self.create()
  # Simulate a pre-migration legacy row, then restore the NOT VALID write guard.
  rpc('ALTER TABLE distribution_orders DROP CONSTRAINT distribution_shopify_no_contact_data')
  rpc('UPDATE distribution_orders SET recipient=$1,delivery_instructions=$2 WHERE id=$3',[{'name':'Hidden','email':'private@example.test'},'Secret note',row['id']])
  rpc("ALTER TABLE distribution_orders ADD CONSTRAINT distribution_shopify_no_contact_data CHECK(source <> 'SHOPIFY' OR (recipient = '{}'::jsonb AND delivery_instructions = '')) NOT VALID")
  app=FastAPI();app.include_router(router,prefix='/api');app.dependency_overrides[access]=lambda:self.ctx;app.dependency_overrides[admin]=lambda:self.ctx
  client=TestClient(app);base='/api/tenants/test/distribution'
  self.assertNotIn('Hidden',client.get(base+'/orders').text)
  detail=client.get(base+'/orders/'+row['id']);self.assertNotIn('private@example.test',detail.text)
  self.assertEqual(client.get(base+'/orders/'+row['id']+'/preview').status_code,409)

 def test_isbn_matching_variants_and_preserved_mapping(self):
  from app.distribution import shopify as sh
  install=self.privacy_install()
  rpc("UPDATE editions SET isbn13='9780306406157' WHERE id=$1",[self.ed])
  paperback=str(uuid4())
  rpc("INSERT INTO editions(id,tenant_id,work_id,isbn13,product_form) VALUES($1,$2,$3,'9780140328721','Paperback')",[paperback,self.tenant,self.work])
  def variant(id,title,barcode,sku=''):
   return {'id':'gid://shopify/ProductVariant/'+id,'title':title,'barcode':barcode,'sku':sku,'product':{'id':'gid://shopify/Product/1','title':'One book'},'inventoryItem':{'id':'gid://shopify/InventoryItem/'+id}}
  variants=[variant('1','Hardcover','978-0-306-40615-7'),variant('2','Paperback','','9780140328721'),variant('3','Conflict','9780306406157','9780140328721')]
  def run():
   sh.sync_products(install['id'],self.ctx,self.connection)
   with patch.object(sh,'graphql',return_value={'productVariants':{'nodes':variants,'pageInfo':{'hasNextPage':False,'endCursor':None}}}):self.assertTrue(sh.sync_catalog_once())
  run()
  rows=rpc('SELECT * FROM distribution_shopify_variants WHERE installation_id=$1 ORDER BY variant_id',[install['id']])['rows']
  self.assertEqual([r['edition_id'] for r in rows],[self.ed,paperback,None])
  self.assertEqual(rows[0]['connection_id'],self.connection)
  self.assertEqual(rows[2]['catalog_metadata']['match_status'],'CONFLICTING_ISBNS')
  variants[0]['barcode']='9780140328721'
  run()
  row=rpc('SELECT edition_id FROM distribution_shopify_variants WHERE installation_id=$1 AND variant_id=$2',[install['id'],variants[0]['id']])['rows'][0]
  self.assertEqual(row['edition_id'],self.ed)

 def test_isbn_validation_duplicate_and_tenant_scope(self):
  from app.distribution import shopify as sh
  self.assertEqual(sh.normalized_isbn('0-306-40615-2'),'9780306406157')
  for value in ('9780306406158','SKU9780306406157','1234567890123',''):
   self.assertIsNone(sh.normalized_isbn(value))
  rpc("UPDATE editions SET isbn13='9780306406157' WHERE id=$1",[self.ed])
  with s.transaction() as cur:
   self.assertEqual(sh.match_isbn(cur,self.other,{'barcode':'9780306406157'})[1],'ISBN_NOT_FOUND')
   self.assertEqual(sh.match_isbn(cur,self.tenant,{'barcode':'not-isbn','sku':'9780306406157'})[0],self.ed)
  rpc("INSERT INTO editions(id,tenant_id,work_id,isbn13) VALUES($1,$2,$3,'9780306406157')",[str(uuid4()),self.tenant,self.work])
  with s.transaction() as cur:self.assertEqual(sh.match_isbn(cur,self.tenant,{'barcode':'9780306406157'})[1],'DUPLICATE_ISBN')

 def test_sync_reconciles_deletion_only_after_last_page(self):
  from app.distribution import shopify as sh
  install=self.privacy_install()
  rpc("INSERT INTO distribution_shopify_variants(tenant_id,installation_id,variant_id,title) VALUES($1,$2,'old','Deleted book')",[self.tenant,install['id']])
  sh.sync_products(install['id'],self.ctx,self.connection)
  page={'productVariants':{'nodes':[],'pageInfo':{'hasNextPage':True,'endCursor':'next'}}}
  with patch.object(sh,'graphql',return_value=page):sh.sync_catalog_once()
  self.assertTrue(rpc("SELECT active FROM distribution_shopify_variants WHERE installation_id=$1",[install['id']])['rows'][0]['active'])
  with patch.object(sh,'graphql',side_effect=RuntimeError('network')):sh.sync_catalog_once()
  self.assertTrue(rpc("SELECT active FROM distribution_shopify_variants WHERE installation_id=$1",[install['id']])['rows'][0]['active'])
  sh.sync_products(install['id'],self.ctx,self.connection)
  page['productVariants']['pageInfo']={'hasNextPage':False,'endCursor':None}
  with patch.object(sh,'graphql',return_value=page):sh.sync_catalog_once()
  self.assertFalse(rpc("SELECT active FROM distribution_shopify_variants WHERE installation_id=$1",[install['id']])['rows'][0]['active'])

if __name__=='__main__':unittest.main()
