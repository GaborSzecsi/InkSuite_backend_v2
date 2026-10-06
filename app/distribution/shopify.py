"""Standalone Shopify OAuth, expiring offline tokens, and verified webhook inbox.
This release requests read scopes only and never publishes inventory/fulfillments.
"""
import base64
import hashlib
import hmac
import json
import os
import re
import secrets as random
import time
from urllib.parse import urlencode
from uuid import uuid4, UUID
import requests
from fastapi import APIRouter,Depends,HTTPException,Request
from fastapi.responses import JSONResponse,RedirectResponse
from psycopg.types.json import Jsonb
from pydantic import BaseModel
from . import service as s
from . import secrets
from .router import router,admin,access,ids
from . import privacy

public_router=APIRouter(prefix='/api/distribution/shopify',tags=['Distribution Shopify'])
SCOPES='read_products,read_orders,read_inventory,read_locations,read_merchant_managed_fulfillment_orders'
API_VERSION='2026-07'
TOPICS={'orders/paid','orders/updated','orders/cancelled','app/uninstalled','customers/data_request','customers/redact','shop/redact'}


def shop_domain(value):
    value=(value or '').strip().lower()
    if not re.fullmatch(r'[a-z0-9][a-z0-9-]*\.myshopify\.com',value): raise HTTPException(422,'Enter the store’s myshopify.com domain.')
    return value


def app_credentials():
    name=os.getenv('DISTRIBUTION_SHOPIFY_APP_SECRET','')
    if not name: raise HTTPException(503,'Shopify app registration is pending. Configure its client ID and secret in Secrets Manager first.')
    try:
        data=json.loads(secrets.client().get_secret_value(SecretId=name)['SecretString'])
        if not data.get('client_id') or not data.get('client_secret'): raise ValueError()
        return data
    except Exception: raise HTTPException(503,'Shopify app credentials are unavailable.') from None


def verify_webhook(raw,signature,secret):
    digest=base64.b64encode(hmac.new(secret.encode(),raw,hashlib.sha256).digest()).decode()
    return bool(signature) and hmac.compare_digest(digest,signature)


def verify_callback(query,secret):
    pairs=list(query.multi_items())
    if len({k for k,v in pairs})!=len(pairs): return False
    supplied=query.get('hmac','')
    message='&'.join(f'{k}={v}' for k,v in sorted(pairs) if k not in ('hmac','signature'))
    expected=hmac.new(secret.encode(),message.encode(),hashlib.sha256).hexdigest()
    return bool(supplied) and hmac.compare_digest(expected,supplied)


def token_request(shop,values,credentials):
    try:
        res=requests.post(f'https://{shop_domain(shop)}/admin/oauth/access_token',data={**values,'client_id':credentials['client_id'],'client_secret':credentials['client_secret']},timeout=20,allow_redirects=False)
        if res.status_code!=200: raise ValueError()
        value=res.json()
        if not value.get('access_token') or not value.get('refresh_token'): raise ValueError()
        value['expires_at']=time.time()+int(value['expires_in'])
        return value
    except Exception: raise HTTPException(502,'Shopify authorization failed. Reconnect the store; no orders were transmitted.') from None


class ConnectIn(BaseModel): shop:str

@router.post('/shopify/connect')
def connect(body:ConnectIn,ctx=Depends(admin)):
    tenant,user=ids(ctx);shop=shop_domain(body.shop);credentials=app_credentials()
    base=os.getenv('DISTRIBUTION_PUBLIC_ORIGIN','').rstrip('/')
    if not base.startswith('https://'): raise HTTPException(503,'Set the public HTTPS callback origin before connecting Shopify.')
    state=random.token_urlsafe(32)
    with s.transaction() as cur:
        existing=s.one(cur,'SELECT tenant_id FROM distribution_shopify_installations WHERE shop=%s',(shop,))
        if existing and str(existing['tenant_id'])!=str(tenant): raise HTTPException(409,'This Shopify store is linked to another publisher.')
        cur.execute("INSERT INTO distribution_oauth_states(state_hash,tenant_id,user_id,shop,expires_at) VALUES(%s,%s,%s,%s,now()+interval '10 minutes')",(hashlib.sha256(state.encode()).hexdigest(),tenant,user,shop))
    url=f'https://{shop}/admin/oauth/authorize?'+urlencode({'client_id':credentials['client_id'],'scope':SCOPES,'redirect_uri':base+'/api/distribution/shopify/callback','state':state})
    response=JSONResponse({'authorization_url':url})
    response.set_cookie('distribution_oauth',state,max_age=600,httponly=True,secure=True,samesite='lax',path='/')
    return response


@public_router.get('/callback')
def callback(request:Request):
    credentials=app_credentials();q=request.query_params
    if not verify_callback(q,credentials['client_secret']): raise HTTPException(401,'Invalid Shopify callback signature.')
    state=q.get('state','');cookie=request.cookies.get('distribution_oauth','')
    if not state or not cookie or not hmac.compare_digest(state,cookie): raise HTTPException(401,'Shopify connection session expired. Start again.')
    shop=shop_domain(q.get('shop',''))
    try:
        if abs(time.time()-int(q.get('timestamp','0')))>600: raise ValueError()
    except ValueError: raise HTTPException(401,'Shopify callback expired.') from None
    with s.transaction() as cur:
        row=s.one(cur,'SELECT * FROM distribution_oauth_states WHERE state_hash=%s AND used_at IS NULL AND expires_at>now() FOR UPDATE',(hashlib.sha256(state.encode()).hexdigest(),))
        if not row or row['shop']!=shop: raise HTTPException(401,'Invalid or expired Shopify connection session.')
        # Serialize by shop across tenants before exchanging/storing credentials.
        cur.execute('SELECT pg_advisory_xact_lock(hashtextextended(%s,0))',('distribution-shop:'+shop,))
        existing=s.one(cur,'SELECT * FROM distribution_shopify_installations WHERE shop=%s',(shop,))
        if existing and str(existing['tenant_id'])!=str(row['tenant_id']): raise HTTPException(409,'Store belongs to another publisher.')
        token=token_request(shop,{'code':q.get('code',''),'expiring':'1'},credentials)
        if not set(SCOPES.split(',')).issubset(set(token.get('scope','').split(','))): raise HTTPException(403,'Required Shopify read permissions were not granted.')
        installation_id=existing['id'] if existing else uuid4()
        reference=secrets.save(row['tenant_id'],installation_id,token)
        cur.execute('INSERT INTO distribution_shopify_installations(id,tenant_id,shop,secret_reference) VALUES(%s,%s,%s,%s) ON CONFLICT(shop) DO UPDATE SET secret_reference=excluded.secret_reference,active=true',(installation_id,row['tenant_id'],shop,reference))
        cur.execute('UPDATE distribution_oauth_states SET used_at=now() WHERE state_hash=%s',(row['state_hash'],))
    response=RedirectResponse('/app/financials/inventory',status_code=303)
    response.delete_cookie('distribution_oauth',path='/')
    return response


def graphql(cur,installation,query,variables=None):
    # Caller holds an installation lock, preventing concurrent refresh-token rotation.
    credentials=secrets.read(installation['secret_reference'])
    if credentials.get('expires_at',0)<time.time()+120:
        credentials=token_request(installation['shop'],{'grant_type':'refresh_token','refresh_token':credentials['refresh_token']},app_credentials())
        secrets.save(installation['tenant_id'],installation['id'],credentials)
    try:
        res=requests.post(f'https://{shop_domain(installation["shop"])}/admin/api/{API_VERSION}/graphql.json',headers={'X-Shopify-Access-Token':credentials['access_token']},json={'query':query,'variables':variables or {}},timeout=20,allow_redirects=False)
        data=res.json()
        if res.status_code!=200 or data.get('errors'): raise ValueError()
        return data['data']
    except Exception: raise HTTPException(502,'Shopify could not complete the read request. Retry shortly or reconnect.') from None


@public_router.post('/webhooks')
async def webhook(request:Request):
    if not os.getenv('DISTRIBUTION_SHOPIFY_APP_SECRET'): raise HTTPException(503,'Shopify is not configured.')
    body=bytearray()
    async for chunk in request.stream():
        body.extend(chunk)
        if len(body)>2*1024*1024: raise HTTPException(413,'Webhook too large.')
    credentials=app_credentials()
    if not verify_webhook(bytes(body),request.headers.get('x-shopify-hmac-sha256',''),credentials['client_secret']): raise HTTPException(401,'Invalid webhook signature.')
    shop=shop_domain(request.headers.get('x-shopify-shop-domain',''));topic=request.headers.get('x-shopify-topic','')
    delivery=request.headers.get('x-shopify-webhook-id','')
    if topic not in TOPICS or not delivery or len(delivery)>200: raise HTTPException(400,'Invalid webhook metadata.')
    try: payload=json.loads(body)
    except ValueError: raise HTTPException(400,'Invalid webhook JSON.') from None
    if not isinstance(payload,dict): raise HTTPException(400,'Invalid webhook payload.')
    if topic in privacy.TOPICS and payload.get('shop_domain') != shop: raise HTTPException(400,'Privacy request store mismatch.')
    # Store only identifiers required to retrieve canonical GraphQL records. No customer payload archive.
    minimal={k:payload[k] for k in ('id','admin_graphql_api_id','shop_id') if k in payload}
    with s.transaction() as cur:
        install=s.one(cur,'SELECT * FROM distribution_shopify_installations WHERE shop=%s',(shop,))
        if not install:
            if topic in privacy.TOPICS: return {'received':True}
            raise HTTPException(404,'Store is not installed.')
        if topic in privacy.TOPICS:
            try: privacy.receive(cur,install,topic,delivery,payload)
            except ValueError: raise HTTPException(400,'Invalid privacy request identifiers.') from None
            return {'received':True}
        if not install['active'] and topic not in ('app/uninstalled','customers/redact','shop/redact'): return {'received':True}
        cur.execute('INSERT INTO distribution_webhooks(tenant_id,installation_id,delivery_id,topic,payload) VALUES(%s,%s,%s,%s,%s) ON CONFLICT(installation_id,delivery_id) DO NOTHING',(install['tenant_id'],install['id'],delivery,topic,Jsonb(minimal)))
        if topic=='app/uninstalled': cur.execute('UPDATE distribution_shopify_installations SET active=false WHERE id=%s',(install['id'],))
    return {'received':True}


@router.get('/shopify/inbox')
def inbox(ctx=Depends(access)):
    tenant,_=ids(ctx)
    with s.transaction() as cur:
        cur.execute('SELECT w.id,w.topic,w.status,w.error_code,w.created_at,i.shop FROM distribution_webhooks w JOIN distribution_shopify_installations i ON i.id=w.installation_id AND i.tenant_id=w.tenant_id WHERE w.tenant_id=%s ORDER BY w.created_at DESC LIMIT 100',(tenant,))
        return {'items':cur.fetchall()}

ORDER_QUERY='''query DistributionOrder($id: ID!) {
 order(id:$id) { id createdAt displayFinancialStatus cancelledAt
  shippingLines(first:10) { nodes { title } pageInfo { hasNextPage } }
  fulfillmentOrders(first:50) { pageInfo { hasNextPage } nodes {
   assignedLocation { location { id } }
   lineItems(first:100) { pageInfo { hasNextPage } nodes { remainingQuantity lineItem { id sku title variant { id } } } }
  } }
 }
}'''


def process_inbox_once():
    """Fetch canonical records asynchronously; explicitly hold incomplete or unmatched orders."""
    from .models import OrderIn
    with s.transaction() as cur:
        entry=s.one(cur,"SELECT * FROM distribution_webhooks WHERE status='RECEIVED' ORDER BY created_at,id FOR UPDATE SKIP LOCKED LIMIT 1")
        if not entry: return False
        install=s.one(cur,'SELECT * FROM distribution_shopify_installations WHERE tenant_id=%s AND id=%s FOR UPDATE',(entry['tenant_id'],entry['installation_id']))
        code=None;status='REVIEW_REQUIRED'
        if entry['topic']=='app/uninstalled': status='DONE'
        elif entry['topic'] in ('customers/data_request','customers/redact','shop/redact'): code='PRIVACY_REQUEST_REQUIRES_OPERATOR'
        elif not install['active']: code='SHOP_UNINSTALLED'
        else:
            try:
                order_id=entry['payload'].get('admin_graphql_api_id') or f'gid://shopify/Order/{entry["payload"].get("id","")}'
                order=graphql(cur,install,ORDER_QUERY,{'id':order_id})['order']
                if not order: raise ValueError('ORDER_NOT_AVAILABLE')
                from datetime import datetime, timezone, timedelta
                created = datetime.fromisoformat(order['createdAt'].replace('Z','+00:00'))
                if created < datetime.now(timezone.utc)-timedelta(days=30): raise ValueError('ORDER_OUTSIDE_PREVIEW_RETENTION')
                privacy.require_ready(cur)
                if privacy.suppressed(cur,install['tenant_id'],install['shop'],order['id']): raise ValueError('ORDER_REMOVED_FOR_PRIVACY')
                if order['cancelledAt']: raise ValueError('CANCELLED_SOURCE_ORDER_REQUIRES_REVIEW')
                if order['displayFinancialStatus']!='PAID': raise ValueError('SOURCE_ORDER_NOT_PAID')
                if not install['location_id']: raise ValueError('SELECT_SHOPIFY_FULFILLMENT_LOCATION')
                groups={};pending=False
                fulfillment=order['fulfillmentOrders']
                if fulfillment['pageInfo']['hasNextPage']: raise ValueError('ORDER_REQUIRES_PAGINATION_REVIEW')
                for fo in fulfillment['nodes']:
                    location=((fo.get('assignedLocation') or {}).get('location') or {}).get('id')
                    if location!=install['location_id']: continue
                    if fo['lineItems']['pageInfo']['hasNextPage']: raise ValueError('ORDER_REQUIRES_PAGINATION_REVIEW')
                    for item in fo['lineItems']['nodes']:
                        if item['remainingQuantity']<=0: continue
                        line=item['lineItem'];variant=(line.get('variant') or {}).get('id')
                        if not variant: pending=True;continue
                        mapping=s.one(cur,"INSERT INTO distribution_shopify_variants(tenant_id,installation_id,variant_id,sku,title) VALUES(%s,%s,%s,%s,%s) ON CONFLICT(tenant_id,installation_id,variant_id) DO UPDATE SET sku=excluded.sku,title=excluded.title RETURNING *",(install['tenant_id'],install['id'],variant,line.get('sku') or '',line['title']))
                        if not mapping['edition_id'] or not mapping['connection_id']: pending=True;continue
                        groups.setdefault(str(mapping['connection_id']),[]).append({'edition_id':str(mapping['edition_id']),'quantity':item['remainingQuantity']})
                if pending: raise ValueError('UNMATCHED_PRODUCTS')
                if not groups: raise ValueError('NO_ELIGIBLE_LINES_AT_SELECTED_LOCATION')
                shipping=order['shippingLines']
                if shipping['pageInfo']['hasNextPage'] or len(shipping['nodes'])!=1: raise ValueError('SHIPPING_METHOD_REQUIRES_REVIEW')
                # A savepoint rolls back ALL split previews if any group cannot be normalized.
                cur.execute('SAVEPOINT normalize_order')
                try:
                    for connection_id,items in groups.items():
                        body=OrderIn(connection_id=connection_id,source='SHOPIFY',source_account=install['shop'],external_order_id=order['id'],reference='Shopify '+order['id'].rsplit('/',1)[-1],shipping_method=shipping['nodes'][0]['title'],items=items)
                        s.create_order(cur,install['tenant_id'],body,'shopify-worker')
                except Exception:
                    cur.execute('ROLLBACK TO SAVEPOINT normalize_order')
                    raise ValueError('NORMALIZATION_REQUIRES_REVIEW') from None
                finally: cur.execute('RELEASE SAVEPOINT normalize_order')
                status='DONE'
            except ValueError as exc:
                allowed={'ORDER_OUTSIDE_PREVIEW_RETENTION','ORDER_REMOVED_FOR_PRIVACY','ORDER_NOT_AVAILABLE','CANCELLED_SOURCE_ORDER_REQUIRES_REVIEW','SOURCE_ORDER_NOT_PAID','SELECT_SHOPIFY_FULFILLMENT_LOCATION','ORDER_REQUIRES_PAGINATION_REVIEW','UNMATCHED_PRODUCTS','NO_ELIGIBLE_LINES_AT_SELECTED_LOCATION','SHIPPING_METHOD_REQUIRES_REVIEW','SHIPPING_ADDRESS_REQUIRED','NORMALIZATION_REQUIRES_REVIEW'}
                code=str(exc) if str(exc) in allowed else 'SHOPIFY_RESPONSE_REQUIRES_REVIEW'
            except Exception: code='SHOPIFY_READ_FAILED'
        cur.execute('UPDATE distribution_webhooks SET status=%s,error_code=%s WHERE id=%s',(status,code,entry['id']))
        return True


@router.get('/shopify/stores')
def stores(ctx=Depends(access)):
    tenant,_=ids(ctx)
    with s.transaction() as cur:
        cur.execute('SELECT id,shop,active,location_id FROM distribution_shopify_installations WHERE tenant_id=%s ORDER BY shop',(tenant,))
        return {'items':cur.fetchall()}

class LocationIn(BaseModel): location_id:str

@router.put('/shopify/{installation_id}/location')
def set_location(installation_id:UUID,body:LocationIn,ctx=Depends(admin)):
    tenant,_=ids(ctx)
    if not re.fullmatch(r'gid://shopify/Location/[0-9]+',body.location_id): raise HTTPException(422,'A Shopify location ID is required.')
    with s.transaction() as cur:
        installation=s.one(cur,'SELECT * FROM distribution_shopify_installations WHERE tenant_id=%s AND id=%s FOR UPDATE',(tenant,installation_id))
        if not installation: raise HTTPException(404,'Shopify installation not found.')
        data=graphql(cur,installation,'query($id:ID!){location(id:$id){id name}}',{'id':body.location_id})
        if not data.get('location'): raise HTTPException(422,'Location does not belong to this store.')
        cur.execute('UPDATE distribution_shopify_installations SET location_id=%s WHERE tenant_id=%s AND id=%s',(body.location_id,tenant,installation_id))
    return {'location_id':body.location_id,'name':data['location']['name']}


@router.post('/shopify/inbox/{entry_id}/retry')
def retry_webhook(entry_id:UUID,ctx=Depends(admin)):
    tenant,_=ids(ctx)
    with s.transaction() as cur:
        row=s.one(cur,"UPDATE distribution_webhooks SET status='RECEIVED',error_code=NULL WHERE tenant_id=%s AND id=%s AND status='REVIEW_REQUIRED' AND topic IN ('orders/paid','orders/updated','orders/cancelled') RETURNING id",(tenant,entry_id))
        if not row: raise HTTPException(404,'Retryable order event not found.')
    return {'queued':True}

CATALOG_QUERY='''query DistributionCatalog($after:String){productVariants(first:100,after:$after){nodes{id title sku barcode inventoryItem{id} product{title}} pageInfo{hasNextPage endCursor}}}'''

@router.post('/shopify/{installation_id}/sync-products',status_code=202)
def sync_products(installation_id:UUID,ctx=Depends(admin)):
    tenant,_=ids(ctx)
    with s.transaction() as cur:
        if not s.one(cur,'SELECT id FROM distribution_shopify_installations WHERE tenant_id=%s AND id=%s AND active',(tenant,installation_id)): raise HTTPException(404,'Active Shopify store not found.')
        cur.execute('INSERT INTO distribution_shopify_jobs(tenant_id,installation_id) VALUES(%s,%s) ON CONFLICT DO NOTHING',(tenant,installation_id))
    return {'queued':True}


def sync_catalog_once():
    with s.transaction() as cur:
        job=s.one(cur,"SELECT * FROM distribution_shopify_jobs WHERE status='QUEUED' ORDER BY created_at FOR UPDATE SKIP LOCKED LIMIT 1")
        if not job:return False
        installation=s.one(cur,'SELECT * FROM distribution_shopify_installations WHERE tenant_id=%s AND id=%s AND active FOR UPDATE',(job['tenant_id'],job['installation_id']))
        if not installation:
            cur.execute("UPDATE distribution_shopify_jobs SET status='FAILED',error_code='SHOP_UNINSTALLED' WHERE id=%s",(job['id'],));return True
        try:
            result=graphql(cur,installation,CATALOG_QUERY,{'after':job['cursor']})['productVariants']
        except Exception:
            cur.execute("UPDATE distribution_shopify_jobs SET status='FAILED',error_code='SHOPIFY_READ_FAILED',attempts=attempts+1,updated_at=now() WHERE id=%s",(job['id'],));return True
        for variant in result['nodes']:
            sku=variant.get('sku') or '';isbn=re.sub(r'[^0-9Xx]','',variant.get('barcode') or sku)
            edition=None
            if re.fullmatch(r'[0-9]{13}',isbn):
                cur.execute("SELECT id FROM editions WHERE tenant_id=%s AND regexp_replace(isbn13,'[^0-9]','','g')=%s",(installation['tenant_id'],isbn))
                matches=cur.fetchall()
                if len(matches)==1:edition=matches[0]['id']
            cur.execute('''INSERT INTO distribution_shopify_variants(tenant_id,installation_id,variant_id,sku,title,inventory_item_id,edition_id)
                VALUES(%s,%s,%s,%s,%s,%s,%s) ON CONFLICT(tenant_id,installation_id,variant_id)
                DO UPDATE SET sku=excluded.sku,title=excluded.title,inventory_item_id=excluded.inventory_item_id,
                edition_id=COALESCE(distribution_shopify_variants.edition_id,excluded.edition_id)''',
                (installation['tenant_id'],installation['id'],variant['id'],sku,variant['product']['title']+' / '+variant['title'],(variant.get('inventoryItem') or {}).get('id'),edition))
        page=result['pageInfo']
        cur.execute('UPDATE distribution_shopify_jobs SET status=%s,cursor=%s,error_code=NULL,updated_at=now() WHERE id=%s',('QUEUED' if page['hasNextPage'] else 'DONE',page['endCursor'],job['id']))
    return True
