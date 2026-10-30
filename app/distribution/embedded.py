"""Shopify App Bridge authentication; no InkSuite cookies or caller-selected tenant."""
import hashlib
import logging
from botocore.exceptions import ClientError
import secrets as random
from uuid import uuid4, UUID
from urllib.parse import urlparse
from jose import jwt
from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel, Field
from . import shopify as sh, service as s, privacy
from .router import router, admin, ids
from .models import MappingIn, ConnectionIn

embedded = APIRouter(prefix='/api/distribution/shopify/embedded')


def ready(cur):
    if not s.one(cur, "SELECT to_regclass('public.distribution_shopify_users') AS ready")['ready']:
        raise HTTPException(503, 'Embedded Shopify setup awaits migration 027.')


def session(request: Request):
    credentials = sh.app_credentials()
    token = request.headers.get('authorization', '')
    try:
        if not token.startswith('Bearer '): raise ValueError()
        raw = token[7:]
        claims = jwt.decode(raw, credentials['client_secret'], algorithms=['HS256'], audience=credentials['client_id'],
                            options={'require_exp':True,'require_nbf':True,'require_iat':True,'require_aud':True,'require_iss':True,'require_sub':True})
        dest = claims['dest']; shop = sh.shop_domain(urlparse(dest).netloc)
        if dest != 'https://' + shop or claims['iss'] != dest + '/admin': raise ValueError()
        if not claims['sub'].isdigit(): raise ValueError()
        return {'shop':shop,'sub':claims['sub'],'token':raw,'credentials':credentials}
    except Exception:
        raise HTTPException(401, 'Open InkSuite from Shopify to authenticate.', headers={'X-Shopify-Retry-Invalid-Session-Request':'1'}) from None


def exchange(auth,offline=True):
    token = sh.token_request(auth['shop'], {
        'grant_type':'urn:ietf:params:oauth:grant-type:token-exchange',
        'subject_token':auth['token'], 'subject_token_type':'urn:ietf:params:oauth:token-type:id_token',
        'requested_token_type':'urn:shopify:params:oauth:token-type:offline-access-token' if offline else 'urn:shopify:params:oauth:token-type:online-access-token',
        **({'expiring':'1'} if offline else {}),
    }, auth['credentials'], offline=offline)
    if not sh.has_required_scopes(token.get('scope','')):
        raise HTTPException(403, 'Install the current InkSuite app version to grant its required permissions.')
    return token


def linked(auth=Depends(session)):
    with s.transaction() as cur:
        ready(cur)
        row = s.one(cur, '''SELECT i.*,g.user_id FROM distribution_shopify_installations i
            JOIN distribution_shopify_users g ON g.installation_id=i.id AND g.subject=%s
            JOIN memberships m ON m.tenant_id=i.tenant_id AND m.user_id=g.user_id
            WHERE i.shop=%s AND i.active AND m.role IN ('tenant_admin','superadmin')''', (auth['sub'],auth['shop']))
        if not row: raise HTTPException(403, 'Link this Shopify user to an InkSuite publisher administrator.')
        from .inventory_cleanup import clear_previous_installation_stock
        clear_previous_installation_stock(cur,row)
    return row


@router.post('/shopify/link-code')
def link_code(ctx=Depends(admin)):
    tenant,user=ids(ctx);code=random.token_urlsafe(24)
    with s.transaction() as cur:
        ready(cur)
        cur.execute('DELETE FROM distribution_shopify_link_codes WHERE expires_at<now() OR (tenant_id=%s AND user_id=%s)',(tenant,user))
        cur.execute("INSERT INTO distribution_shopify_link_codes(code_hash,tenant_id,user_id,expires_at) VALUES(%s,%s,%s,now()+interval '10 minutes')",(hashlib.sha256(code.encode()).hexdigest(),tenant,user))
    return {'code':code,'expires_in':600}


@embedded.post('/session')
def start(auth=Depends(session)):
    # Shopify authentication/token exchange precedes publisher account linking UI.
    exchange(auth,offline=False)
    try: row=linked(auth)
    except HTTPException as exc:
        if exc.status_code!=403: raise
        return {'linked':False,'shop':auth['shop']}
    return {'linked':True,'shop':auth['shop']}


class LinkIn(BaseModel):
    code:str=Field(min_length=20,max_length=128)


@embedded.post('/link')
def link(body:LinkIn,auth=Depends(session)):
    with s.transaction() as cur:
        ready(cur)
        cur.execute('SELECT pg_advisory_xact_lock(hashtextextended(%s,0))',('distribution-shop:'+auth['shop'],))
        code=s.one(cur,'SELECT * FROM distribution_shopify_link_codes WHERE code_hash=%s AND used_at IS NULL AND expires_at>now() FOR UPDATE',(hashlib.sha256(body.code.strip().encode()).hexdigest(),))
        if not code: raise HTTPException(400,'Link code expired or invalid. Generate another in InkSuite.')
        if not s.one(cur,"SELECT user_id FROM memberships WHERE tenant_id=%s AND user_id=%s AND role IN ('tenant_admin','superadmin')",(code['tenant_id'],code['user_id'])):
            raise HTTPException(403,'Publisher administrator access is required.')
        existing=s.one(cur,'SELECT * FROM distribution_shopify_installations WHERE shop=%s AND active FOR UPDATE',(auth['shop'],))
        if existing and existing['tenant_id']!=code['tenant_id']: raise HTTPException(409,'Store is linked to a different publisher.')
        token=exchange(auth);installation=existing['id'] if existing else uuid4()
        try:
            reference=sh.secrets.save(code['tenant_id'],installation,token)
        except ClientError as exc:
            aws_code=exc.response.get('Error',{}).get('Code','Unknown')
            logging.getLogger(__name__).error('Shopify installation token storage failed: %s',aws_code)
            raise HTTPException(503, 'Unable to securely save the Shopify connection. The server administrator must check installation-token Secrets Manager permissions.') from None
        cur.execute('INSERT INTO distribution_shopify_installations(id,tenant_id,shop,secret_reference) VALUES(%s,%s,%s,%s) ON CONFLICT(shop) WHERE active DO UPDATE SET active=true,secret_reference=excluded.secret_reference',(installation,code['tenant_id'],auth['shop'],reference))
        cur.execute('INSERT INTO distribution_shopify_users(installation_id,subject,user_id) VALUES(%s,%s,%s) ON CONFLICT(installation_id,subject) DO UPDATE SET user_id=excluded.user_id',(installation,auth['sub'],code['user_id']))
        cur.execute('UPDATE distribution_shopify_link_codes SET used_at=now() WHERE code_hash=%s',(code['code_hash'],))
        privacy.audit(cur,code['tenant_id'],str(code['user_id']),'SHOPIFY_USER_LINKED',installation)
        from .inventory_cleanup import clear_previous_installation_stock
        current=s.one(cur,'SELECT * FROM distribution_shopify_installations WHERE id=%s',(installation,))
        clear_previous_installation_stock(cur,current)
    return {'linked':True,'shop':auth['shop']}


@embedded.get('/workspace')
def workspace(install=Depends(linked)):
    with s.transaction() as cur:
        tenant=install['tenant_id']
        from .router import public_connection
        cur.execute('SELECT * FROM distribution_connections WHERE tenant_id=%s AND archived_at IS NULL AND (shopify_installation_id IS NULL OR shopify_installation_id=%s) ORDER BY display_name',(tenant,install['id']));connections=[public_connection(c) for c in cur.fetchall()]
        cur.execute("SELECT e.id,e.isbn13,w.title,COALESCE(NULLIF(e.onix_product_form,''),e.product_form) AS product_form FROM editions e JOIN works w ON w.id=e.work_id AND w.tenant_id=e.tenant_id WHERE e.tenant_id=%s AND e.archived_at IS NULL ORDER BY w.title LIMIT 1000",(tenant,));editions=cur.fetchall()
        cur.execute('SELECT variant_id,title,sku,edition_id,connection_id,catalog_metadata FROM distribution_shopify_variants WHERE tenant_id=%s AND installation_id=%s AND active ORDER BY title LIMIT 500',(tenant,install['id']));variants=cur.fetchall()
        cur.execute("SELECT o.id,o.external_order_id,o.status,o.created_at,o.connection_id,o.mode,COALESCE(sum(l.quantity),0) AS quantity FROM distribution_orders o LEFT JOIN distribution_order_items l ON l.order_id=o.id AND l.tenant_id=o.tenant_id WHERE o.tenant_id=%s AND o.source='SHOPIFY' AND o.source_account=%s GROUP BY o.id ORDER BY o.created_at DESC LIMIT 100",(tenant,install['shop']));orders=cur.fetchall()
        cur.execute('SELECT id,topic,status,error_code FROM distribution_webhooks WHERE tenant_id=%s AND installation_id=%s ORDER BY created_at DESC LIMIT 100',(tenant,install['id']));inbox=cur.fetchall()
        cur.execute('SELECT status,error_code,updated_at FROM distribution_shopify_jobs WHERE installation_id=%s ORDER BY created_at DESC LIMIT 1',(install['id'],));sync=cur.fetchone()
        cur.execute('SELECT id,work_id,status,message FROM distribution_shopify_publish_jobs WHERE installation_id=%s ORDER BY created_at DESC LIMIT 50',(install['id'],));publishing=cur.fetchall()
        privacy.audit(cur,tenant,str(install['user_id']),'EMBEDDED_WORKSPACE_READ',install['id'])
    return {'shop':install['shop'],'location_id':install['location_id'],'connections':connections,'editions':editions,'variants':variants,'orders':orders,'inbox':inbox,'sync':sync,'publishing':publishing}


def ctx(install):
    return {'tenant':{'id':install['tenant_id']},'user':{'id':install['user_id']},'membership_role':'tenant_admin'}


@embedded.get('/locations')
def locations(install=Depends(linked)):
    with s.transaction() as cur:
        row=s.one(cur,'SELECT * FROM distribution_shopify_installations WHERE id=%s FOR UPDATE',(install['id'],))
        data=sh.graphql(cur,row,'{locations(first:100){nodes{id name} pageInfo{hasNextPage}}}')
        return data['locations']


@embedded.put('/location')
def location(body:sh.LocationIn,install=Depends(linked)):
    return sh.set_location(install['id'],body,ctx(install))


class ProductImportIn(BaseModel):
    connection_id:UUID


@embedded.post('/products')
def products(body:ProductImportIn,install=Depends(linked)):
    return sh.sync_products(install['id'],ctx(install),body.connection_id)


class MapIn(MappingIn):
    variant_id:str=Field(pattern=r'^gid://shopify/ProductVariant/[0-9]+$')


@embedded.put('/mapping')
def mapping(body:MapIn,install=Depends(linked)):
    from .router import map_variant
    return map_variant(install['id'],body.variant_id,MappingIn(edition_id=body.edition_id,connection_id=body.connection_id),ctx(install))


@embedded.post('/connections')
def connection(body:ConnectionIn,install=Depends(linked)):
    from .router import add_connection
    return add_connection(body,{**ctx(install),'shopify_installation_id':install['id']})


class RetryIn(BaseModel):
    id:UUID


class RetryOrderIn(RetryIn):
    connection_id:UUID


@embedded.post('/retry-order')
def retry_order(body:RetryOrderIn,install=Depends(linked)):
    from .order_recovery import retry_simulated_order
    with s.transaction() as cur:
        return retry_simulated_order(cur,install,body.id,body.connection_id)


@embedded.post('/retry')
def retry(body:RetryIn,install=Depends(linked)):
    with s.transaction() as cur:
        row=s.one(cur,"UPDATE distribution_webhooks SET status='RECEIVED',error_code=NULL WHERE id=%s AND tenant_id=%s AND installation_id=%s AND status='REVIEW_REQUIRED' AND topic IN ('orders/paid','orders/updated','orders/cancelled') RETURNING id",(body.id,install['tenant_id'],install['id']))
        if not row: raise HTTPException(404,'Retryable event not found.')
    return {'queued':True}


@embedded.get('/catalog')
def publisher_catalog(install=Depends(linked)):
    from .publishing import catalog
    with s.transaction() as cur:
        return catalog(cur,install)

class PublishIn(BaseModel):
    edition_ids:list[UUID]=Field(min_length=1,max_length=100)
    connection_id:UUID

@embedded.post('/publish',status_code=202)
def publish_books(body:PublishIn,install=Depends(linked)):
    from .publishing import enqueue
    return enqueue(install,body.edition_ids,body.connection_id)


# Per-publisher execution controls, authenticated by the existing linked administrator.
from typing import Literal
from .models import ConfigurationIn, ExecutionIn, CredentialsIn

class DistributorAction(BaseModel):
    connection_id:UUID
    action:Literal['configure','credentials','test','sync','settings','retry','ship_simulation','simulate_order','remove','set_test_stock']
    configuration:ConfigurationIn|None=None
    settings:ExecutionIn|None=None
    credentials:CredentialsIn|None=None
    job_id:UUID|None=None
    order_id:UUID|None=None
    edition_id:UUID|None=None
    quantity:int=Field(default=1,ge=1,le=100000)
    test_quantity:int=Field(default=0,ge=0,le=100000,strict=True)
    safety_buffer:int=Field(default=5,ge=0,le=100000,strict=True)

@embedded.get('/distribution')
def distribution_status(install=Depends(linked)):
    from .execution import ready
    with s.transaction() as cur:
        if not ready(cur): return {'ready':False,'jobs':[],'inventory':[],'inventory_push_enabled':False}
        row=s.one(cur,'SELECT inventory_push_enabled FROM distribution_shopify_installations WHERE tenant_id=%s AND id=%s',(install['tenant_id'],install['id']))
        cur.execute("""SELECT j.id,j.connection_id,j.order_id,j.kind,j.status,j.error_code,j.attempts,j.created_at,
            o.status AS order_status,o.external_order_id,sim.status AS simulated_status
            FROM distribution_jobs j
            LEFT JOIN distribution_orders o ON o.tenant_id=j.tenant_id AND o.id=j.order_id
            LEFT JOIN distribution_simulated_orders sim ON sim.tenant_id=j.tenant_id AND sim.order_id=j.order_id AND sim.connection_id=j.connection_id
            WHERE j.tenant_id=%s ORDER BY j.created_at DESC LIMIT 100""",(install['tenant_id'],))
        jobs=cur.fetchall()
        cur.execute("""SELECT a.connection_id,e.isbn13,w.title,a.distributor_quantity,a.reserved_quantity,a.sellable_quantity,
            a.last_synced_at,a.sync_status FROM distribution_availability a
            JOIN editions e ON e.tenant_id=a.tenant_id AND e.id=a.edition_id
            JOIN works w ON w.tenant_id=e.tenant_id AND w.id=e.work_id WHERE a.tenant_id=%s
            ORDER BY w.title,e.isbn13 LIMIT 500""",(install['tenant_id'],))
        return {'ready':True,'jobs':jobs,'inventory':cur.fetchall(),'inventory_push_enabled':row['inventory_push_enabled']}

@embedded.post('/distribution')
def distribution_action(body:DistributorAction,install=Depends(linked)):
    from . import router as routes
    from .execution import ready
    from .adapters import adapter_for
    tenant=install['tenant_id']
    with s.transaction() as cur:
        if not ready(cur): raise HTTPException(503,'Execution awaits migration 030.')
        conn=s.connection(cur,tenant,body.connection_id)
    context=ctx(install)
    if body.action=='set_test_stock' and body.edition_id:
        from .simulated_inventory import set_test_stock
        return set_test_stock(install,body.connection_id,body.edition_id,body.test_quantity,body.safety_buffer)
    if body.action=='remove': return routes.remove_connection(body.connection_id,context)
    if body.action=='configure' and body.configuration:
        return routes.configure(body.connection_id,body.configuration,context)
    if body.action=='credentials' and body.credentials:
        return routes.credentials(body.connection_id,body.credentials,context)
    if body.action=='test': return routes.test(body.connection_id,context)
    if body.action=='sync': return routes.sync(body.connection_id,context)
    if body.action=='settings' and body.settings:
        return routes.configure_execution(body.connection_id,body.settings,context)
    if body.action=='simulate_order' and body.edition_id:
        from .models import OrderIn
        if conn['adapter']!='simulator': raise HTTPException(409,'Select a simulated distributor.')
        if not conn['enabled']: raise HTTPException(409,'Enable the simulated connection first.')
        key='simulation-'+str(uuid4())
        order=OrderIn(connection_id=body.connection_id,source='SHOPIFY',source_account=install['shop'],
            external_order_id=key,reference=key,shipping_method='SIMULATED',
            items=[{'edition_id':body.edition_id,'quantity':body.quantity}])
        with s.transaction() as cur:
            result=s.create_order(cur,tenant,order,install['user_id'],automated=True)
        return {'queued':True,'order_id':str(result['id'])}
    if body.action=='retry' and body.job_id:
        with s.transaction() as cur:
            job=s.one(cur,"SELECT * FROM distribution_jobs WHERE tenant_id=%s AND connection_id=%s AND id=%s AND status IN ('FAILED','REVIEW_REQUIRED') FOR UPDATE",(tenant,body.connection_id,body.job_id))
            if not job: raise HTTPException(404,'Retryable job not found.')
            if job['kind']=='SUBMIT' and not getattr(adapter_for(conn),'idempotent_submission',False):
                raise HTTPException(409,'Confirm the transmission outcome with the distributor; this adapter cannot safely replay an uncertain upload.')
            if job['kind']=='SUBMIT':
                order=s.one(cur,'SELECT status FROM distribution_orders WHERE tenant_id=%s AND id=%s',(tenant,job['order_id']))
                if not order or order['status'] not in ('QUEUED','SUBMITTING'):
                    raise HTTPException(409,'This order has already been acknowledged or rejected. Review it with the distributor instead of sending it again.')
            if s.one(cur,"SELECT id FROM distribution_jobs WHERE tenant_id=%s AND connection_id=%s AND kind=%s AND order_id IS NOT DISTINCT FROM %s AND status IN ('RUNNING','QUEUED')",(tenant,body.connection_id,job['kind'],job['order_id'])):
                raise HTTPException(409,'A matching job is already pending.')
            cur.execute("UPDATE distribution_jobs SET status='QUEUED',due_at=now(),error_code=NULL WHERE id=%s",(job['id'],))
            s.event(cur,tenant,body.connection_id,install['user_id'],'JOB_RETRY_REQUESTED',job['order_id'])
            return {'queued':True}
    if body.action=='ship_simulation' and body.order_id:
        if conn['adapter']!='simulator': raise HTTPException(409,'This operation is available only for simulated distributors.')
        with s.transaction() as cur:
            s.connection(cur,tenant,body.connection_id,lock=True)
            row=s.one(cur,"SELECT status FROM distribution_simulated_orders WHERE tenant_id=%s AND connection_id=%s AND order_id=%s FOR UPDATE",(tenant,body.connection_id,body.order_id))
            if not row: raise HTTPException(404,'Simulated accepted order not found.')
            if row['status']=='SHIPPED': return {'queued':False,'shipped':True}
            if row['status']!='ACCEPTED': raise HTTPException(409,'Only an accepted simulated order can be shipped.')
            cur.execute("UPDATE distribution_simulated_orders SET status='SHIPPED' WHERE tenant_id=%s AND connection_id=%s AND order_id=%s",(tenant,body.connection_id,body.order_id))
            s.enqueue(cur,tenant,body.connection_id,'TRACKING',body.order_id)
            s.event(cur,tenant,body.connection_id,install['user_id'],'SIMULATED_SHIPMENT',body.order_id)
            return {'queued':True}
    raise HTTPException(422,'Required action fields are missing.')

class InventoryPublicationIn(BaseModel):
    enabled:bool
    confirm_inventory_authority:bool=False

@embedded.put('/inventory-publication')
def inventory_publication(body:InventoryPublicationIn,install=Depends(linked),auth=Depends(session)):
    from .execution import ready
    if body.enabled and not body.confirm_inventory_authority:
        raise HTTPException(409,'Confirm this distributor is the inventory authority for the selected location.')
    with s.transaction() as cur:
        if not ready(cur): raise HTTPException(503,'Execution awaits migration 030.')
        row=s.one(cur,'SELECT * FROM distribution_shopify_installations WHERE tenant_id=%s AND id=%s FOR UPDATE',(install['tenant_id'],install['id']))
        if body.enabled:
            if not row['location_id']: raise HTTPException(409,'Select a fulfillment location first.')
            token=exchange(auth)
            if not sh.has_required_scopes(token.get('scope',''),'write_inventory'):
                raise HTTPException(403,'Approve the current app version with inventory write permission, then retry.')
            sh.secrets.save(row['tenant_id'],row['id'],token)
        cur.execute('UPDATE distribution_shopify_installations SET inventory_push_enabled=%s WHERE id=%s',(body.enabled,row['id']))
        privacy.audit(cur,install['tenant_id'],install['user_id'],'INVENTORY_PUBLICATION_ENABLED' if body.enabled else 'INVENTORY_PUBLICATION_DISABLED',install['id'])
    return {'inventory_push_enabled':body.enabled}

