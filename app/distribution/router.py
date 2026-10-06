from dataclasses import asdict
from uuid import UUID
from fastapi import APIRouter, Depends, HTTPException, Response
from app.tenants.dependencies import require_tenant_access
from . import service as s
from . import secrets
from .adapters import ADAPTERS, adapter_for, ConfigurationRequired
from .models import ConnectionIn, CredentialsIn, OrderIn, ListingIn, MappingIn

router=APIRouter(prefix='/tenants/{tenant_slug}/distribution',tags=['Distribution'])


def access(ctx=Depends(require_tenant_access)):
    if ctx['membership_role'] in ('tenant_admin','superadmin'): return ctx
    with s.db_conn() as conn,conn.cursor() as cur:
        cur.execute('SELECT module_permissions FROM memberships WHERE tenant_id=%s AND user_id=%s',(ctx['tenant']['id'],ctx['user']['id']))
        row=cur.fetchone()
    if not row or (row[0] or {}).get('financials') is not True: raise HTTPException(403,'Financials permission is required.')
    return ctx


def admin(ctx=Depends(access)):
    if ctx['membership_role'] not in ('tenant_admin','superadmin'): raise HTTPException(403,'A publisher administrator must configure distribution.')
    return ctx


def ids(ctx): return ctx['tenant']['id'],ctx['user']['id']


def schema_ready():
    with s.db_conn() as conn,conn.cursor(row_factory=s.dict_row) as cur:
        return bool(s.one(cur,"SELECT to_regclass('public.distribution_connections') AS ready")['ready'])


def public_connection(row):
    return {k:v for k,v in row.items() if k!='secret_reference'} | {
        'credentials_saved':bool(row.get('secret_reference')),
        'capabilities':asdict(adapter_for(row).capabilities),
    }


@router.get('/overview')
def overview(ctx=Depends(access)):
    tenant,_=ids(ctx)
    if not schema_ready():
        return {'mode':'preview','connections':[],'checkout_enabled':False,'schema_ready':False}
    with s.transaction() as cur:
        cur.execute('SELECT * FROM distribution_connections WHERE tenant_id=%s ORDER BY display_name,id',(tenant,))
        connections=[public_connection(row) for row in cur.fetchall()]
    return {'mode':'preview','connections':connections,'checkout_enabled':False,'schema_ready':True}


@router.post('/connections',status_code=201)
def add_connection(body:ConnectionIn,ctx=Depends(admin)):
    tenant,actor=ids(ctx)
    with s.transaction() as cur:
        row=s.one(cur,'INSERT INTO distribution_connections(tenant_id,display_name,adapter,safety_buffer) VALUES(%s,%s,%s,%s) RETURNING *',(tenant,body.display_name,body.adapter,body.safety_buffer))
        s.event(cur,tenant,row['id'],actor,'CONNECTION_CREATED')
        return public_connection(row)


@router.put('/connections/{connection_id}/credentials')
def credentials(connection_id:UUID,body:CredentialsIn,ctx=Depends(admin)):
    tenant,actor=ids(ctx)
    with s.transaction() as cur:
        s.connection(cur,tenant,connection_id,lock=True)
        try: name=secrets.save(tenant,connection_id,body.model_dump())
        except Exception: raise HTTPException(503,'Credentials could not be stored securely. Check the distribution Secrets Manager permission.') from None
        cur.execute('UPDATE distribution_connections SET secret_reference=%s,updated_at=now() WHERE tenant_id=%s AND id=%s',(name,tenant,connection_id))
        s.event(cur,tenant,connection_id,actor,'CREDENTIALS_UPDATED')
    return {'credentials_saved':True}


@router.post('/connections/{connection_id}/sync',status_code=202)
def sync(connection_id:UUID,ctx=Depends(access)):
    tenant,actor=ids(ctx)
    with s.transaction() as cur:
        conn=s.connection(cur,tenant,connection_id)
        if not adapter_for(conn).capabilities.inventory:
            raise HTTPException(409,'The distributor inventory specification is required before synchronization can be enabled.')
        result=s.enqueue(cur,tenant,connection_id,'INVENTORY')
        s.event(cur,tenant,connection_id,actor,'INVENTORY_SYNC_REQUESTED')
        return result


@router.post('/connections/{connection_id}/test',status_code=202)
def test(connection_id:UUID,ctx=Depends(admin)):
    tenant,actor=ids(ctx)
    with s.transaction() as cur:
        conn=s.connection(cur,tenant,connection_id)
        if not conn['secret_reference']: raise HTTPException(409,'Save connection credentials first.')
        result=s.enqueue(cur,tenant,connection_id,'TEST')
        s.event(cur,tenant,connection_id,actor,'CONNECTION_TEST_REQUESTED')
        return result


@router.get('/inventory')
def inventory(connection_id:UUID|None=None,q:str='',ctx=Depends(access)):
    tenant,_=ids(ctx)
    if not schema_ready():
        with s.db_conn() as conn,conn.cursor(row_factory=s.dict_row) as cur:
            cur.execute("""SELECT e.id AS edition_id,w.id AS work_id,w.title,e.isbn13,e.product_form,
                NULL AS connection_id,NULL AS distributor_quantity,NULL AS reserved_quantity,
                NULL AS sellable_quantity,NULL AS last_synced_at,'NOT_SYNCED' AS sync_status,false AS marketplace_enabled
                FROM editions e JOIN works w ON w.id=e.work_id AND w.tenant_id=e.tenant_id
                WHERE e.tenant_id=%s AND COALESCE(NULLIF(e.onix_product_form,''),e.product_form) IN ('BB','BC','BA','BH','BD','BE','BF','BG','BK')
                AND (w.title ILIKE %s OR e.isbn13 ILIKE %s) ORDER BY w.title,e.product_form,e.id LIMIT 500""",(tenant,'%'+q[:150]+'%','%'+q[:150]+'%'))
            return {'items':cur.fetchall()}

    with s.transaction() as cur:
        cur.execute("""SELECT e.id AS edition_id,w.id AS work_id,w.title,e.isbn13,e.product_form,
            a.connection_id,a.distributor_quantity,a.reserved_quantity,a.sellable_quantity,
            a.last_synced_at,a.sync_status,COALESCE(a.marketplace_enabled,false) AS marketplace_enabled
            FROM editions e JOIN works w ON w.id=e.work_id AND w.tenant_id=e.tenant_id
            LEFT JOIN distribution_availability a ON a.tenant_id=e.tenant_id AND a.edition_id=e.id
                AND (%s::uuid IS NULL OR a.connection_id=%s)
            WHERE e.tenant_id=%s AND COALESCE(NULLIF(e.onix_product_form,''),e.product_form) IN ('BB','BC','BA','BH','BD','BE','BF','BG','BK')
                AND (w.title ILIKE %s OR e.isbn13 ILIKE %s)
            ORDER BY w.title,e.product_form,e.id LIMIT 500""",(connection_id,connection_id,tenant,'%'+q[:150]+'%','%'+q[:150]+'%'))
        return {'items':cur.fetchall()}


@router.put('/connections/{connection_id}/editions/{edition_id}/marketplace')
def listing(connection_id:UUID,edition_id:UUID,body:ListingIn,ctx=Depends(admin)):
    tenant,actor=ids(ctx)
    with s.transaction() as cur:
        s.connection(cur,tenant,connection_id)
        ed=s.one(cur,"SELECT id FROM editions WHERE tenant_id=%s AND id=%s AND COALESCE(NULLIF(onix_product_form,''),product_form) IN ('BB','BC','BA','BH','BD','BE','BF','BG','BK')",(tenant,edition_id))
        if not ed: raise HTTPException(404,'Physical edition not found.')
        cur.execute('INSERT INTO distribution_inventory(tenant_id,connection_id,edition_id,marketplace_enabled) VALUES(%s,%s,%s,%s) ON CONFLICT(tenant_id,connection_id,edition_id) DO UPDATE SET marketplace_enabled=excluded.marketplace_enabled',(tenant,connection_id,edition_id,body.enabled))
        s.event(cur,tenant,connection_id,actor,'MARKETPLACE_LISTING_CHANGED',details={'edition_id':str(edition_id),'enabled':body.enabled})
    return {'enabled':body.enabled,'checkout_enabled':False}


@router.get('/orders')
def orders(status:str|None=None,ctx=Depends(access)):
    tenant,actor=ids(ctx)
    with s.transaction() as cur:
        from .privacy import audit
        audit(cur,tenant,actor,'ORDER_LIST_READ')
        cur.execute("""SELECT o.id,CASE WHEN o.source='SHOPIFY' THEN 'Shopify ' || regexp_replace(o.external_order_id,'^.*/','') ELSE o.reference END AS reference,o.source,o.status,o.mode,o.created_at,o.error_code,
            CASE WHEN o.source='SHOPIFY' THEN NULL ELSE o.recipient->>'name' END AS customer,c.display_name AS distributor,
            (SELECT SUM(quantity) FROM distribution_order_items i WHERE i.tenant_id=o.tenant_id AND i.order_id=o.id) AS items
            FROM distribution_orders o JOIN distribution_connections c ON c.tenant_id=o.tenant_id AND c.id=o.connection_id
            WHERE o.tenant_id=%s AND (%s::text IS NULL OR o.status=%s) ORDER BY o.created_at DESC LIMIT 200""",(tenant,status,status))
        return {'items':cur.fetchall()}


@router.post('/orders/preview',status_code=201)
def preview_order(body:OrderIn,ctx=Depends(admin)):
    tenant,actor=ids(ctx)
    with s.transaction() as cur: return s.public_order(s.create_order(cur,tenant,body,actor))


@router.get('/orders/{order_id}')
def order_detail(order_id:UUID,ctx=Depends(access)):
    tenant,actor=ids(ctx)
    with s.transaction() as cur:
        order=s.one(cur,'SELECT * FROM distribution_orders WHERE tenant_id=%s AND id=%s',(tenant,order_id))
        if not order: raise HTTPException(404,'Order not found.')
        cur.execute('SELECT i.*,w.title FROM distribution_order_items i JOIN editions e ON e.id=i.edition_id AND e.tenant_id=i.tenant_id JOIN works w ON w.id=e.work_id AND w.tenant_id=e.tenant_id WHERE i.tenant_id=%s AND i.order_id=%s ORDER BY i.edition_id',(tenant,order_id))
        order['items']=cur.fetchall()
        cur.execute('SELECT event,details,created_at FROM distribution_events WHERE tenant_id=%s AND order_id=%s ORDER BY created_at',(tenant,order_id))
        order['events']=cur.fetchall()
        from .privacy import audit
        audit(cur,tenant,actor,'ORDER_DETAIL_READ',order_id)
        return s.public_order(order)


@router.get('/orders/{order_id}/preview')
def order_file(order_id:UUID,ctx=Depends(access)):
    tenant,actor=ids(ctx)
    with s.transaction() as cur:
        order=s.one(cur,'SELECT * FROM distribution_orders WHERE tenant_id=%s AND id=%s',(tenant,order_id))
        if not order: raise HTTPException(404,'Order not found.')
        if order['source']=='SHOPIFY': raise HTTPException(409,'Customer delivery data is not stored in InkSuite. Shopify file previews are disabled; details will be retrieved only for an authorized distributor transmission.')
        from .privacy import audit
        audit(cur,tenant,actor,'ORDER_FILE_DOWNLOADED',order_id)
        conn=s.connection(cur,tenant,order['connection_id'])
        cur.execute('SELECT * FROM distribution_order_items WHERE tenant_id=%s AND order_id=%s ORDER BY edition_id',(tenant,order_id))
        try: data=adapter_for(conn).render_order(order,cur.fetchall())
        except ConfigurationRequired as exc: raise HTTPException(409,str(exc)) from None
    return Response(data,media_type='text/plain',headers={'Content-Disposition':f'attachment; filename="preview-{order_id}.tsv"','Cache-Control':'no-store'})


@router.post('/orders/{order_id}/cancel')
def cancel_order(order_id:UUID,ctx=Depends(admin)):
    tenant,actor=ids(ctx)
    with s.transaction() as cur: return s.public_order(s.cancel(cur,tenant,order_id,actor))


@router.get('/shopify/mappings')
def mappings(ctx=Depends(access)):
    tenant,_=ids(ctx)
    with s.transaction() as cur:
        cur.execute('SELECT v.*,i.shop FROM distribution_shopify_variants v JOIN distribution_shopify_installations i ON i.id=v.installation_id AND i.tenant_id=v.tenant_id WHERE v.tenant_id=%s ORDER BY v.title,v.variant_id LIMIT 500',(tenant,))
        return {'items':cur.fetchall()}


@router.put('/shopify/{installation_id}/mappings/{variant_id}')
def map_variant(installation_id:UUID,variant_id:str,body:MappingIn,ctx=Depends(admin)):
    tenant,actor=ids(ctx)
    with s.transaction() as cur:
        s.connection(cur,tenant,body.connection_id)
        if not s.one(cur,'SELECT id FROM editions WHERE tenant_id=%s AND id=%s',(tenant,body.edition_id)): raise HTTPException(404,'Edition not found.')
        row=s.one(cur,'UPDATE distribution_shopify_variants SET edition_id=%s,connection_id=%s WHERE tenant_id=%s AND installation_id=%s AND variant_id=%s RETURNING variant_id',(body.edition_id,body.connection_id,tenant,installation_id,variant_id))
        if not row: raise HTTPException(404,'Shopify variant not found.')
        s.event(cur,tenant,body.connection_id,actor,'SHOPIFY_VARIANT_MAPPED',details={'variant_id':variant_id,'edition_id':str(body.edition_id)})
        return row

from .models import ConfigurationIn
from psycopg.types.json import Jsonb

@router.put('/connections/{connection_id}/configuration')
def configure(connection_id:UUID,body:ConfigurationIn,ctx=Depends(admin)):
    tenant,actor=ids(ctx)
    for key,value in body.shipping_methods.items():
        if not key or not value or len(key)>100 or len(value)>50 or any(c in key+value for c in '\r\n\t'):
            raise HTTPException(422,'Shipping mappings need a channel label and a valid distributor code.')
    with s.transaction() as cur:
        s.connection(cur,tenant,connection_id,lock=True)
        config=body.model_dump(exclude={'safety_buffer'},exclude_none=True)
        row=s.one(cur,"UPDATE distribution_connections SET configuration=%s,safety_buffer=%s,status='NOT_CONFIGURED',updated_at=now() WHERE tenant_id=%s AND id=%s RETURNING *",(Jsonb(config),body.safety_buffer,tenant,connection_id))
        s.event(cur,tenant,connection_id,actor,'CONNECTION_CONFIGURATION_UPDATED')
        return public_connection(row)
