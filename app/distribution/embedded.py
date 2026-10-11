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
    if not set(sh.SCOPES.split(',')).issubset(set(token.get('scope','').split(','))):
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
        existing=s.one(cur,'SELECT * FROM distribution_shopify_installations WHERE shop=%s FOR UPDATE',(auth['shop'],))
        if existing and existing['tenant_id']!=code['tenant_id']: raise HTTPException(409,'Store is linked to a different publisher.')
        token=exchange(auth);installation=existing['id'] if existing else uuid4()
        try:
            reference=sh.secrets.save(code['tenant_id'],installation,token)
        except ClientError as exc:
            aws_code=exc.response.get('Error',{}).get('Code','Unknown')
            logging.getLogger(__name__).error('Shopify installation token storage failed: %s',aws_code)
            raise HTTPException(503, 'Unable to securely save the Shopify connection. The server administrator must check installation-token Secrets Manager permissions.') from None
        cur.execute('INSERT INTO distribution_shopify_installations(id,tenant_id,shop,secret_reference) VALUES(%s,%s,%s,%s) ON CONFLICT(shop) DO UPDATE SET active=true,secret_reference=excluded.secret_reference',(installation,code['tenant_id'],auth['shop'],reference))
        cur.execute('INSERT INTO distribution_shopify_users(installation_id,subject,user_id) VALUES(%s,%s,%s) ON CONFLICT(installation_id,subject) DO UPDATE SET user_id=excluded.user_id',(installation,auth['sub'],code['user_id']))
        cur.execute('UPDATE distribution_shopify_link_codes SET used_at=now() WHERE code_hash=%s',(code['code_hash'],))
        privacy.audit(cur,code['tenant_id'],str(code['user_id']),'SHOPIFY_USER_LINKED',installation)
    return {'linked':True,'shop':auth['shop']}


@embedded.get('/workspace')
def workspace(install=Depends(linked)):
    with s.transaction() as cur:
        tenant=install['tenant_id']
        cur.execute('SELECT id,display_name FROM distribution_connections WHERE tenant_id=%s ORDER BY display_name',(tenant,));connections=cur.fetchall()
        cur.execute('SELECT e.id,e.isbn13,w.title FROM editions e JOIN works w ON w.id=e.work_id AND w.tenant_id=e.tenant_id WHERE e.tenant_id=%s ORDER BY w.title LIMIT 1000',(tenant,));editions=cur.fetchall()
        cur.execute('SELECT variant_id,title,sku,edition_id,connection_id,catalog_metadata FROM distribution_shopify_variants WHERE tenant_id=%s AND installation_id=%s ORDER BY title LIMIT 500',(tenant,install['id']));variants=cur.fetchall()
        cur.execute("SELECT o.id,o.external_order_id,o.status,o.created_at,COALESCE(sum(l.quantity),0) AS quantity FROM distribution_orders o LEFT JOIN distribution_order_items l ON l.order_id=o.id AND l.tenant_id=o.tenant_id WHERE o.tenant_id=%s AND o.source='SHOPIFY' AND o.source_account=%s GROUP BY o.id ORDER BY o.created_at DESC LIMIT 100",(tenant,install['shop']));orders=cur.fetchall()
        cur.execute('SELECT id,topic,status,error_code FROM distribution_webhooks WHERE tenant_id=%s AND installation_id=%s ORDER BY created_at DESC LIMIT 100',(tenant,install['id']));inbox=cur.fetchall()
        privacy.audit(cur,tenant,str(install['user_id']),'EMBEDDED_WORKSPACE_READ',install['id'])
    return {'shop':install['shop'],'location_id':install['location_id'],'connections':connections,'editions':editions,'variants':variants,'orders':orders,'inbox':inbox}


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
    return add_connection(body,ctx(install))


class RetryIn(BaseModel):
    id:UUID


@embedded.post('/retry')
def retry(body:RetryIn,install=Depends(linked)):
    with s.transaction() as cur:
        row=s.one(cur,"UPDATE distribution_webhooks SET status='RECEIVED',error_code=NULL WHERE id=%s AND tenant_id=%s AND installation_id=%s AND status='REVIEW_REQUIRED' AND topic IN ('orders/paid','orders/updated','orders/cancelled') RETURNING id",(body.id,install['tenant_id'],install['id']))
        if not row: raise HTTPException(404,'Retryable event not found.')
    return {'queued':True}
