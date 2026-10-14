"""Distributor inventory and submission orchestration. All state is publisher scoped."""
from datetime import datetime, timezone, timedelta
from uuid import uuid4
from fastapi import HTTPException
from . import service as s
from .adapters import adapter_for, ConfigurationRequired
from .protocol import Snapshot, Receipt
from .transports import TransportError
from .secrets import read
from psycopg.errors import DeadlockDetected, SerializationFailure

def ready(cur):
    return bool(s.one(cur,"SELECT to_regclass('public.distribution_simulated_orders') AS ready")['ready'])

def active(conn, feature):
    return conn['enabled'] and conn.get(feature,False) and (conn['mode']=='live' or conn['adapter']=='simulator')

def adapter(cur, conn):
    credentials=read(conn['secret_reference']) if conn.get('secret_reference') else {}
    return adapter_for(conn,cur=cur,credentials=credentials)

def queue_order(cur, tenant, order_id):
    order=s.one(cur,'SELECT * FROM distribution_orders WHERE tenant_id=%s AND id=%s FOR UPDATE',(tenant,order_id))
    conn=s.connection(cur,tenant,order['connection_id'],lock=True)
    if not active(conn,'orders_enabled') or order['status']!='RECEIVED': return False
    if order['source']!='SHOPIFY':
        raise ConfigurationRequired('Marketplace checkout remains disabled.')
    # Preview records are never promoted by enabling a connection later.
    if order['mode']!='live' and conn['adapter']!='simulator': return False
    if not adapter_for(conn).capabilities.submit_order:
        raise ConfigurationRequired('Configure an order-capable distributor adapter.')
    if conn.get('inventory_enabled'):
        s.reserve(cur,tenant,order_id)
    cur.execute("UPDATE distribution_orders SET status='QUEUED',updated_at=now() WHERE tenant_id=%s AND id=%s",(tenant,order_id))
    s.enqueue(cur,tenant,conn['id'],'SUBMIT',order_id)
    return True

def reconcile(cur, conn, snapshot):
    snapshot=Snapshot.model_validate(snapshot)
    prior=conn.get('inventory_as_of')
    if isinstance(prior,str): prior=datetime.fromisoformat(prior.replace('Z','+00:00'))
    if prior and snapshot.as_of<=prior:
        raise ConfigurationRequired('Inventory snapshot must be newer than the last successful snapshot.')
    tenant=conn['tenant_id']; cid=conn['id']
    # Validate the entire response before touching inventory; partial or stale feeds cannot zero stock.
    resolved=[]
    for stock in snapshot.items:
        cur.execute("SELECT id FROM editions WHERE tenant_id=%s AND regexp_replace(isbn13,'[^0-9]','','g')=%s AND COALESCE(NULLIF(onix_product_form,''),product_form) IN ('BB','BC','BA','BH','BD','BE','BF','BG','BK')",(tenant,stock.isbn))
        matches=cur.fetchall()
        if len(matches)>1: raise ConfigurationRequired('Duplicate ISBN in publisher catalog.')
        if matches: resolved.append((matches[0]['id'],stock.on_hand))
    reflected=[]
    included={str(edition) for edition,_ in resolved}
    for key in snapshot.reflected_order_keys:
        row=s.one(cur,"SELECT id,status FROM distribution_orders WHERE tenant_id=%s AND connection_id=%s AND id=%s",(tenant,cid,key))
        # A privacy-erased historical key may still occur in the remote snapshot.
        # Ignore it; never look up or modify a different publisher's order.
        if not row: continue
        if row['status']!='SHIPPED': raise ConfigurationRequired('Inventory references an unacknowledged shipment.')
        if not snapshot.complete:
            cur.execute('SELECT edition_id FROM distribution_order_items WHERE tenant_id=%s AND order_id=%s',(tenant,key))
            if any(str(item['edition_id']) not in included for item in cur.fetchall()):
                raise ConfigurationRequired('A partial snapshot must include every edition in a reflected shipment.')
        reflected.append(key)
    # Only a declared complete snapshot makes omitted, previously known editions unavailable.
    if snapshot.complete:
        cur.execute("UPDATE distribution_inventory SET distributor_quantity=0,sync_status='OK',last_synced_at=%s WHERE tenant_id=%s AND connection_id=%s",(snapshot.as_of,tenant,cid))
    for edition,quantity in resolved:
        cur.execute("""INSERT INTO distribution_inventory(tenant_id,connection_id,edition_id,distributor_quantity,sync_status,last_synced_at)
            VALUES(%s,%s,%s,%s,'OK',%s) ON CONFLICT(tenant_id,connection_id,edition_id)
            DO UPDATE SET distributor_quantity=excluded.distributor_quantity,sync_status='OK',last_synced_at=excluded.last_synced_at""",(tenant,cid,edition,quantity,snapshot.as_of))
    # If inventory is enabled after order forwarding, already outstanding orders
    # still need a hold. A missing feed ISBN remains unavailable, never invented stock.
    cur.execute("""INSERT INTO distribution_inventory(tenant_id,connection_id,edition_id)
        SELECT DISTINCT o.tenant_id,o.connection_id,i.edition_id FROM distribution_orders o
        JOIN distribution_order_items i ON i.tenant_id=o.tenant_id AND i.order_id=o.id
        WHERE o.tenant_id=%s AND o.connection_id=%s AND (o.mode='live' OR %s)
        AND o.status IN ('QUEUED','SUBMITTING','SUBMITTED','ACCEPTED','SHIPPED') ON CONFLICT DO NOTHING""",
        (tenant,cid,conn['adapter']=='simulator'))
    cur.execute("""INSERT INTO distribution_reservations(tenant_id,connection_id,edition_id,order_id,quantity,status)
        SELECT o.tenant_id,o.connection_id,i.edition_id,o.id,i.quantity,
            CASE WHEN o.status='SHIPPED' THEN 'FULFILLED' WHEN o.status IN ('SUBMITTED','ACCEPTED') THEN 'SUBMITTED' ELSE 'RESERVED' END
        FROM distribution_orders o JOIN distribution_order_items i ON i.tenant_id=o.tenant_id AND i.order_id=o.id
        WHERE o.tenant_id=%s AND o.connection_id=%s AND (o.mode='live' OR %s)
        AND o.status IN ('QUEUED','SUBMITTING','SUBMITTED','ACCEPTED','SHIPPED') ON CONFLICT DO NOTHING""",
        (tenant,cid,conn['adapter']=='simulator'))
    for key in reflected:
        cur.execute("UPDATE distribution_reservations SET status='RELEASED',updated_at=now() WHERE tenant_id=%s AND connection_id=%s AND order_id=%s AND status IN ('SUBMITTED','FULFILLED')",(tenant,cid,key))
    cur.execute("UPDATE distribution_connections SET inventory_as_of=%s,last_inventory_sync_at=now(),updated_at=now() WHERE tenant_id=%s AND id=%s",(snapshot.as_of,tenant,cid))
    s.event(cur,tenant,cid,'distribution-worker','INVENTORY_RECONCILED',details={'matched_editions':len(resolved),'unmatched_isbns':len(snapshot.items)-len(resolved),'complete':snapshot.complete})
    return len(resolved)

def apply_receipt(cur,conn,order,receipt):
    receipt=Receipt.model_validate(receipt)
    if str(receipt.order_key)!=str(order['id']):
        raise ConfigurationRequired('Distributor receipt does not match the order key.')
    if order.get('distributor_order_id') and receipt.distributor_order_id!=order['distributor_order_id']:
        raise ConfigurationRequired('Distributor order identity changed.')
    tenant=conn['tenant_id']; oid=order['id']
    # Never regress an acknowledgement or release stock on an ambiguous result.
    rank={'SUBMITTING':0,'SUBMITTED':1,'ACCEPTED':2,'SHIPPED':3}
    if receipt.status=='REJECTED':
        if order['status'] not in ('SUBMITTING','SUBMITTED'):
            raise ConfigurationRequired('A previously accepted order requires reconciliation.')
        status='REVIEW_REQUIRED'
        cur.execute("UPDATE distribution_reservations SET status='RELEASED',updated_at=now() WHERE tenant_id=%s AND order_id=%s",(tenant,oid))
    else:
        status=max((order['status'],receipt.status),key=lambda x:rank.get(x,-1))
        cur.execute("UPDATE distribution_reservations SET status=%s,updated_at=now() WHERE tenant_id=%s AND order_id=%s AND status IN ('RESERVED','SUBMITTED')",('FULFILLED' if status=='SHIPPED' else 'SUBMITTED',tenant,oid))
    cur.execute("""UPDATE distribution_orders SET status=%s,distributor_order_id=%s,
        error_code=%s,submitted_at=COALESCE(submitted_at,now()),
        accepted_at=CASE WHEN %s IN ('ACCEPTED','SHIPPED') THEN COALESCE(accepted_at,now()) ELSE accepted_at END,
        shipped_at=CASE WHEN %s='SHIPPED' THEN COALESCE(shipped_at,now()) ELSE shipped_at END,updated_at=now()
        WHERE tenant_id=%s AND id=%s""",(status,receipt.distributor_order_id,'DISTRIBUTOR_REJECTED' if receipt.status=='REJECTED' else None,status,status,tenant,oid))
    if conn['adapter']=='simulator' and conn.get('inventory_enabled') and status!=order['status']:
        cur.execute('UPDATE distribution_connections SET next_sync_at=now() WHERE tenant_id=%s AND id=%s',(tenant,conn['id']))
    s.event(cur,tenant,conn['id'],'distribution-worker','ORDER_'+status,oid)
    return status

def schedule(cur):
    # A connection row lock serializes snapshots, simulator stock, and activation changes.
    cur.execute("""SELECT * FROM distribution_connections WHERE enabled AND next_sync_at<=now()
        AND (inventory_enabled OR orders_enabled) ORDER BY next_sync_at FOR NO KEY UPDATE SKIP LOCKED LIMIT 20""")
    for conn in cur.fetchall():
        if active(conn,'inventory_enabled'):
            s.enqueue(cur,conn['tenant_id'],conn['id'],'INVENTORY')
            # Publication also runs when the feed fails, so stale stock can be zeroed.
            if s.one(cur,"SELECT id FROM distribution_shopify_installations WHERE tenant_id=%s AND active AND (inventory_push_enabled OR simulated_inventory_connection_id=%s) LIMIT 1",(conn['tenant_id'],conn['id'])):
                s.enqueue(cur,conn['tenant_id'],conn['id'],'PUBLISH')
        if active(conn,'orders_enabled'):
            cur.execute("SELECT id FROM distribution_orders WHERE tenant_id=%s AND connection_id=%s AND status IN ('SUBMITTED','ACCEPTED') ORDER BY created_at LIMIT 100",(conn['tenant_id'],conn['id']))
            if adapter_for(conn).capabilities.order_status:
                for order in cur.fetchall(): s.enqueue(cur,conn['tenant_id'],conn['id'],'TRACKING',order['id'])
        cur.execute("UPDATE distribution_connections SET next_sync_at=now()+interval '5 minutes' WHERE id=%s",(conn['id'],))

def execute(cur,job):
    conn=s.connection(cur,job['tenant_id'],job['connection_id'],lock=True)
    driver=adapter_for(conn) if job['kind']=='PUBLISH' else adapter(cur,conn)
    if job['kind']=='PUBLISH':
        if not active(conn,'inventory_enabled'):
            raise ConfigurationRequired('Shopify stock publication requires an enabled inventory connection.')
        from .shopify_execution import push_inventory
        push_inventory(cur,conn)
        return
    if job['kind']=='TEST':
        driver.test_connection()
        return
    if job['kind']=='INVENTORY':
        if not active(conn,'inventory_enabled') or not driver.capabilities.inventory:
            raise ConfigurationRequired('Inventory synchronization requires an enabled, configured connection.')
        reconcile(cur,conn,driver.fetch_inventory())
        s.enqueue(cur,conn['tenant_id'],conn['id'],'PUBLISH')
        return
    if not active(conn,'orders_enabled'):
        raise ConfigurationRequired('Order forwarding requires an enabled distributor connection.')
    order=s.one(cur,'SELECT * FROM distribution_orders WHERE tenant_id=%s AND id=%s FOR UPDATE',(conn['tenant_id'],job['order_id']))
    if not order or order['status']=='CANCELLED': return
    if job['kind']=='TRACKING':
        receipt=driver.get_order_status(order['id'])
        if receipt is None: raise ConfigurationRequired('Distributor order could not be reconciled.')
        apply_receipt(cur,conn,order,receipt)
        return
    if order['status'] in ('SUBMITTED','ACCEPTED','SHIPPED'): return
    if order['status']!='SUBMITTING': raise ConfigurationRequired('Order is not ready for transmission.')
    cur.execute('SELECT * FROM distribution_order_items WHERE tenant_id=%s AND order_id=%s ORDER BY edition_id',(conn['tenant_id'],order['id']))
    items=cur.fetchall()
    if job['attempts']>1 and getattr(driver,'idempotent_submission',False):
        existing=driver.get_order_status(order['id'])
        if existing is not None:
            apply_receipt(cur,conn,order,existing)
            return
    if getattr(driver,'simulated',False):
        transient={**order,'recipient':{}}  # simulator never retrieves buyer information
    else:
        from .shopify_execution import delivery_order
        transient=delivery_order(cur,conn,order,items)
    try:
        apply_receipt(cur,conn,order,driver.submit_order(transient,items))
    finally:
        transient.clear()

def run_once():
    with s.transaction() as cur:
        if not ready(cur): return False
        # Interrupted requests require reconciliation. No blind replay after a process crash.
        cur.execute("""WITH expired AS (SELECT id FROM distribution_jobs WHERE status='RUNNING'
            AND started_at<now()-interval '10 minutes' FOR UPDATE SKIP LOCKED)
            UPDATE distribution_jobs SET status='REVIEW_REQUIRED',error_code='WORKER_INTERRUPTED',
            finished_at=now() WHERE id IN (SELECT id FROM expired)""")
        schedule(cur)
        job=s.one(cur,"SELECT * FROM distribution_jobs WHERE status='QUEUED' AND due_at<=now() ORDER BY due_at,id FOR UPDATE SKIP LOCKED LIMIT 1")
        if not job: return False
        lease=uuid4();job['attempts']+=1
        cur.execute("UPDATE distribution_jobs SET status='RUNNING',attempts=attempts+1,started_at=now(),lease_id=%s WHERE id=%s",(lease,job['id']))
        if job['kind']=='SUBMIT':
            cur.execute("UPDATE distribution_orders SET status='SUBMITTING',updated_at=now() WHERE tenant_id=%s AND id=%s AND status='QUEUED'",(job['tenant_id'],job['order_id']))
    code=None; retry=False
    try:
        with s.transaction() as cur:
            s.connection(cur,job['tenant_id'],job['connection_id'],lock=True)
            # Hold the lease row across execution so another worker cannot steal a slow request.
            current=s.one(cur,"SELECT id FROM distribution_jobs WHERE id=%s AND lease_id=%s AND status='RUNNING' FOR UPDATE",(job['id'],lease))
            if not current:return True
            execute(cur,job)
            order=s.one(cur,'SELECT status,error_code FROM distribution_orders WHERE tenant_id=%s AND id=%s',(job['tenant_id'],job['order_id'])) if job['order_id'] else None
            rejected=order and order['status']=='REVIEW_REQUIRED'
            cur.execute("UPDATE distribution_jobs SET status=%s,finished_at=now(),error_code=%s WHERE id=%s",('REVIEW_REQUIRED' if rejected else 'DONE',order['error_code'] if rejected else None,job['id']))
            cur.execute("UPDATE distribution_connections SET status='CONNECTED',last_error=NULL,last_successful_connection_at=now() WHERE tenant_id=%s AND id=%s",(job['tenant_id'],job['connection_id']))
        return True
    except ConfigurationRequired: code='CONFIGURATION_REQUIRED'
    except TransportError: code='TRANSPORT_FAILED';retry=True
    except (DeadlockDetected,SerializationFailure): code='CONCURRENT_UPDATE';retry=True
    except HTTPException as exc: code='SHOPIFY_REQUEST_FAILED';retry=exc.status_code>=500
    except Exception: code='RECONCILIATION_REQUIRED'
    with s.transaction() as cur:
        conn=s.connection(cur,job['tenant_id'],job['connection_id'])
        can_retry=retry and job['attempts']<5 and (job['kind']!='SUBMIT' or getattr(adapter_for(conn),'idempotent_submission',False))
        cur.execute("""UPDATE distribution_jobs SET status=%s,error_code=%s,
            due_at=now()+(%s * interval '1 second'),finished_at=now()
            WHERE id=%s AND lease_id=%s AND status='RUNNING'""",
            ('QUEUED' if can_retry else 'REVIEW_REQUIRED',code,min(900,30*2**job['attempts']),job['id'],lease))
        cur.execute("UPDATE distribution_connections SET last_error=%s WHERE tenant_id=%s AND id=%s",(code,job['tenant_id'],job['connection_id']))
        if job['kind']=='TEST':
            cur.execute("UPDATE distribution_connections SET status='ERROR',updated_at=now() WHERE tenant_id=%s AND id=%s",(job['tenant_id'],job['connection_id']))
        s.event(cur,job['tenant_id'],job['connection_id'],'distribution-worker',job['kind']+'_NEEDS_ATTENTION',job['order_id'],{'error_code':code,'retrying':can_retry})
    return True

