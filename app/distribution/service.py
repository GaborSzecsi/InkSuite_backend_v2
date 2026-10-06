"""Distribution transactions. Call these functions inside one database transaction."""
import hashlib
import json
from collections import Counter
from contextlib import contextmanager
from fastapi import HTTPException
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb
from app.core.db import db_conn


def one(cur, sql, args=()):
    cur.execute(sql,args)
    return cur.fetchone()


@contextmanager
def transaction():
    with db_conn() as conn, conn.transaction(), conn.cursor(row_factory=dict_row) as cur:
        if not one(cur,"SELECT to_regclass('public.distribution_connections') AS ready")['ready']:
            raise HTTPException(503,'Distribution is awaiting migration 020. No existing integrations have been changed.')
        yield cur


def connection(cur, tenant, connection_id, lock=False):
    row=one(cur,'SELECT * FROM distribution_connections WHERE tenant_id=%s AND id=%s'+(' FOR UPDATE' if lock else ''),(tenant,connection_id))
    if not row: raise HTTPException(404,'Distributor connection not found.')
    return row


def event(cur,tenant,connection_id,actor,action,order_id=None,details=None):
    cur.execute('INSERT INTO distribution_events(tenant_id,connection_id,actor,event,order_id,details) VALUES(%s,%s,%s,%s,%s,%s)',(tenant,connection_id,str(actor),action,order_id,Jsonb(details or {})))


def enqueue(cur,tenant,connection_id,kind,order_id=None):
    row=one(cur,"INSERT INTO distribution_jobs(tenant_id,connection_id,kind,order_id) VALUES(%s,%s,%s,%s) ON CONFLICT DO NOTHING RETURNING id",(tenant,connection_id,kind,order_id))
    return {'queued':True,'job_id':str(row['id']) if row else None}


def create_order(cur,tenant,body,actor):
    """Preview ingestion; live ingestion uses the same row locks and reserve function."""
    conn=connection(cur,tenant,body.connection_id,lock=True)
    data=body.model_dump(mode='json')
    if body.source == 'SHOPIFY':
        # Customer delivery details are fetched only at a future transmission boundary,
        # never archived or exposed by the order preview workflow.
        data['recipient'] = {}
        data['delivery_instructions'] = ''
        data['reference'] = 'Shopify ' + body.external_order_id.rsplit('/', 1)[-1]
        from .privacy import require_ready, suppressed
        require_ready(cur)
        if suppressed(cur, tenant, body.source_account, body.external_order_id):
            raise HTTPException(409, 'This order was removed under the privacy or retention policy.')
    digest=hashlib.sha256(json.dumps(data,sort_keys=True,separators=(',',':')).encode()).hexdigest()
    existing=one(cur,'SELECT * FROM distribution_orders WHERE tenant_id=%s AND source=%s AND source_account=%s AND external_order_id=%s AND connection_id=%s',
                 (tenant,body.source,body.source_account,body.external_order_id,body.connection_id))
    if existing:
        if existing['request_hash']!=digest: raise HTTPException(409,'This source order already exists with different contents. Review the original order.')
        return existing
    quantities=Counter()
    for item in body.items: quantities[str(item.edition_id)]+=item.quantity
    editions=[]
    for edition_id,qty in sorted(quantities.items()):
        ed=one(cur,"SELECT id,isbn13,COALESCE(NULLIF(onix_product_form,''),product_form) AS product_form FROM editions WHERE tenant_id=%s AND id=%s",(tenant,edition_id))
        if not ed: raise HTTPException(422,'An edition does not belong to this publisher.')
        if not ed['isbn13'] or ed['product_form'] not in ('BB','BC','BA','BH','BD','BE','BF','BG','BK'):
            raise HTTPException(422,'Only physical editions with an ISBN can be fulfilled.')
        editions.append((edition_id,qty,ed['isbn13']))
    row=one(cur,"""INSERT INTO distribution_orders(tenant_id,connection_id,source,source_account,external_order_id,reference,request_hash,recipient,shipping_method,delivery_instructions,mode)
        VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'preview') RETURNING *""",
        (tenant,body.connection_id,body.source,body.source_account,body.external_order_id,data['reference'],digest,Jsonb(data['recipient']),body.shipping_method,data['delivery_instructions']))
    for ed,qty,isbn in editions:
        cur.execute('INSERT INTO distribution_order_items(tenant_id,order_id,edition_id,isbn,quantity) VALUES(%s,%s,%s,%s,%s)',(tenant,row['id'],ed,isbn,qty))
    event(cur,tenant,conn['id'],actor,'ORDER_PREVIEW_CREATED',row['id'])
    return row


def reserve(cur,tenant,order_id):
    """Atomic all-or-nothing reservation. A preview order cannot consume live stock."""
    order=one(cur,'SELECT * FROM distribution_orders WHERE tenant_id=%s AND id=%s FOR UPDATE',(tenant,order_id))
    if not order: raise HTTPException(404,'Order not found.')
    if order['mode']!='live': raise HTTPException(409,'Preview orders do not reserve live stock.')
    if order['status'] in ('RESERVED','QUEUED','SUBMITTING','SUBMITTED','ACCEPTED','SHIPPED'): return order
    if order['status']!='RECEIVED': raise HTTPException(409,'This order cannot be reserved in its current state.')
    conn=connection(cur,tenant,order['connection_id'])
    if not conn['enabled'] or conn['mode']!='live': raise HTTPException(409,'This connection is not live.')
    cur.execute('SELECT * FROM distribution_order_items WHERE tenant_id=%s AND order_id=%s ORDER BY edition_id',(tenant,order_id))
    items=cur.fetchall()
    if not items: raise HTTPException(422,'Order has no items.')
    for item in items:
        stock=one(cur,'SELECT * FROM distribution_inventory WHERE tenant_id=%s AND connection_id=%s AND edition_id=%s FOR UPDATE',(tenant,conn['id'],item['edition_id']))
        if not stock: raise HTTPException(409,'Inventory has not been synchronized for this edition.')
        available=one(cur,'SELECT sellable_quantity,marketplace_enabled FROM distribution_availability WHERE tenant_id=%s AND connection_id=%s AND edition_id=%s',(tenant,conn['id'],item['edition_id']))
        if order['source']=='MARKETPLACE' and not available['marketplace_enabled']: raise HTTPException(409,'This edition is not enabled for Marketplace.')
        if available['sellable_quantity']<item['quantity']: raise HTTPException(409,'Insufficient available inventory. The order needs review.')
    for item in items:
        cur.execute("INSERT INTO distribution_reservations(tenant_id,connection_id,edition_id,order_id,quantity,status) VALUES(%s,%s,%s,%s,%s,'RESERVED')",(tenant,conn['id'],item['edition_id'],order_id,item['quantity']))
    return one(cur,"UPDATE distribution_orders SET status='RESERVED',updated_at=now() WHERE tenant_id=%s AND id=%s RETURNING *",(tenant,order_id))


def cancel(cur,tenant,order_id,actor):
    order=one(cur,'SELECT * FROM distribution_orders WHERE tenant_id=%s AND id=%s FOR UPDATE',(tenant,order_id))
    if not order: raise HTTPException(404,'Order not found.')
    if order['status']=='CANCELLED': return order
    if order['status'] not in ('RECEIVED','RESERVED','QUEUED','FAILED'):
        raise HTTPException(409,'Transmission may have started. Confirm cancellation with the distributor before releasing stock.')
    cur.execute("UPDATE distribution_reservations SET status='CANCELLED',updated_at=now() WHERE tenant_id=%s AND order_id=%s AND status='RESERVED'",(tenant,order_id))
    cur.execute("UPDATE distribution_jobs SET status='FAILED',error_code='ORDER_CANCELLED' WHERE tenant_id=%s AND order_id=%s AND status='QUEUED'",(tenant,order_id))
    result=one(cur,"UPDATE distribution_orders SET status='CANCELLED',updated_at=now() WHERE tenant_id=%s AND id=%s RETURNING *",(tenant,order_id))
    event(cur,tenant,order['connection_id'],actor,'ORDER_CANCELLED',order_id)
    return result


def public_order(row):
    if row.get('source') != 'SHOPIFY':
        return row
    # Defensive protection for legacy rows pending the explicit cleanup command.
    return {**row, 'reference': 'Shopify ' + row['external_order_id'].rsplit('/', 1)[-1],
            'recipient': {}, 'delivery_instructions': '', 'tracking': []}
