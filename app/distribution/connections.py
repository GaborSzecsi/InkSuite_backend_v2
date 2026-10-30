"""Remove distributor configuration; permanently purge disposable simulator data."""
from fastapi import HTTPException
from . import service as s


def purge_simulator(cur, tenant, cid):
    """Permanently remove a disposable simulator and its dependent test data."""
    row=s.one(cur,'SELECT * FROM distribution_connections WHERE tenant_id=%s AND id=%s FOR UPDATE',(tenant,cid))
    if not row or row['adapter']!='simulator':
        raise HTTPException(409,'Only simulated distributor data can be purged.')
    if row['secret_reference']:
        raise HTTPException(409,'Remove simulator credentials before purging.')
    if s.one(cur,"SELECT id FROM distribution_jobs WHERE tenant_id=%s AND connection_id=%s AND status='RUNNING' LIMIT 1",(tenant,cid)) or s.one(cur,"SELECT id FROM distribution_shopify_publish_jobs WHERE tenant_id=%s AND connection_id=%s AND status='RUNNING' LIMIT 1",(tenant,cid)):
        raise HTTPException(409,'Wait for running jobs to finish before deleting test data.')
    from .privacy import delete_order_webhooks, numeric_id
    cur.execute("SELECT source_account,external_order_id FROM distribution_orders WHERE tenant_id=%s AND connection_id=%s AND source='SHOPIFY'",(tenant,cid))
    for order in cur.fetchall():
        value=numeric_id(order['external_order_id'])
        if value: delete_order_webhooks(cur,{'tenant_id':tenant,'shop':order['source_account']},value)
    cur.execute('UPDATE distribution_shopify_variants SET connection_id=NULL WHERE tenant_id=%s AND connection_id=%s',(tenant,cid))
    cur.execute('UPDATE distribution_shopify_installations SET simulated_inventory_connection_id=NULL,inventory_push_enabled=false WHERE tenant_id=%s AND simulated_inventory_connection_id=%s',(tenant,cid))
    cur.execute('DELETE FROM distribution_shopify_jobs WHERE tenant_id=%s AND connection_id=%s',(tenant,cid))
    cur.execute('DELETE FROM distribution_shopify_publish_jobs WHERE tenant_id=%s AND connection_id=%s',(tenant,cid))
    cur.execute('DELETE FROM distribution_access_audit WHERE tenant_id=%s AND (resource_id=%s OR resource_id IN (SELECT id FROM distribution_orders WHERE tenant_id=%s AND connection_id=%s))',(tenant,cid,tenant,cid))
    for table in ('distribution_jobs','distribution_events','distribution_reservations','distribution_order_items','distribution_simulated_orders'):
        # Items have no connection_id; use their owning order for every table.
        cur.execute(f'DELETE FROM {table} WHERE tenant_id=%s AND order_id IN (SELECT id FROM distribution_orders WHERE tenant_id=%s AND connection_id=%s)',(tenant,tenant,cid))
    for table in ('distribution_jobs','distribution_events','distribution_inventory'):
        cur.execute(f'DELETE FROM {table} WHERE tenant_id=%s AND connection_id=%s',(tenant,cid))
    cur.execute('DELETE FROM distribution_orders WHERE tenant_id=%s AND connection_id=%s',(tenant,cid))
    cur.execute('DELETE FROM distribution_connections WHERE tenant_id=%s AND id=%s',(tenant,cid))
    return {'removed':True}


def remove_connection(cur, tenant, cid, actor):
    row=s.connection(cur,tenant,cid,lock=True)
    if row['adapter']=='simulator':
        return purge_simulator(cur,tenant,cid)
    if s.one(cur,"SELECT id FROM distribution_jobs WHERE tenant_id=%s AND connection_id=%s AND status='RUNNING' LIMIT 1",(tenant,cid)):
        raise HTTPException(409,'A distributor job is running. Wait for it to finish before removing the connection.')
    if s.one(cur,"SELECT id FROM distribution_shopify_publish_jobs WHERE tenant_id=%s AND connection_id=%s AND status='RUNNING' LIMIT 1",(tenant,cid)):
        raise HTTPException(409,'A book publication is running. Wait for it to finish before removing the connection.')
    if row['adapter']!='simulator' and s.one(cur,"""SELECT id FROM distribution_orders WHERE tenant_id=%s AND connection_id=%s
       AND (status IN ('RESERVED','QUEUED','SUBMITTING','SUBMITTED','ACCEPTED','REVIEW_REQUIRED')
       OR EXISTS (SELECT 1 FROM distribution_reservations r WHERE r.tenant_id=distribution_orders.tenant_id
          AND r.order_id=distribution_orders.id AND r.status IN ('RESERVED','SUBMITTED'))) LIMIT 1""",(tenant,cid)):
        raise HTTPException(409,'Resolve outstanding orders and reservations before removing this distributor.')
    cur.execute("""UPDATE distribution_connections SET archived_at=now(),enabled=false,orders_enabled=false,
        inventory_enabled=false,status='DISABLED',configuration='{}',updated_at=now() WHERE tenant_id=%s AND id=%s""",(tenant,cid))
    cur.execute("UPDATE distribution_jobs SET status='FAILED',error_code='CONNECTION_REMOVED',finished_at=now() WHERE tenant_id=%s AND connection_id=%s AND status IN ('QUEUED','REVIEW_REQUIRED')",(tenant,cid))
    cur.execute("UPDATE distribution_shopify_publish_jobs SET status='SUPERSEDED',message='Distributor connection removed',updated_at=now() WHERE tenant_id=%s AND connection_id=%s AND status IN ('QUEUED','REVIEW_REQUIRED')",(tenant,cid))
    cur.execute('UPDATE distribution_shopify_variants SET connection_id=NULL WHERE tenant_id=%s AND connection_id=%s',(tenant,cid))
    cur.execute('UPDATE distribution_inventory SET marketplace_enabled=false WHERE tenant_id=%s AND connection_id=%s',(tenant,cid))
    cur.execute('UPDATE distribution_shopify_installations SET simulated_inventory_connection_id=NULL WHERE tenant_id=%s AND simulated_inventory_connection_id=%s',(tenant,cid))
    if row['adapter']=='simulator':
        cur.execute("UPDATE distribution_reservations SET status='CANCELLED',updated_at=now() WHERE tenant_id=%s AND connection_id=%s AND status IN ('RESERVED','SUBMITTED')",(tenant,cid))
        cur.execute("UPDATE distribution_orders SET status='CANCELLED',updated_at=now() WHERE tenant_id=%s AND connection_id=%s AND status NOT IN ('SHIPPED','CANCELLED')",(tenant,cid))
    s.event(cur,tenant,cid,actor,'CONNECTION_REMOVED')
    return {'removed':True}


def uninstall_connections(cur,install):
    # Only configurations explicitly created by this installation belong to it.
    cur.execute('SELECT id FROM distribution_connections WHERE tenant_id=%s AND shopify_installation_id=%s AND archived_at IS NULL ORDER BY id',(install['tenant_id'],install['id']))
    ids=[r['id'] for r in cur.fetchall()]
    for cid in ids:
        shared=s.one(cur,"""SELECT v.variant_id FROM distribution_shopify_variants v
            JOIN distribution_shopify_installations i ON i.id=v.installation_id AND i.tenant_id=v.tenant_id
            WHERE v.tenant_id=%s AND v.connection_id=%s AND i.active AND i.id<>%s LIMIT 1""",(install['tenant_id'],cid,install['id']))
        if shared:
            cur.execute('UPDATE distribution_connections SET shopify_installation_id=NULL WHERE tenant_id=%s AND id=%s',(install['tenant_id'],cid))
            continue
        try:remove_connection(cur,install['tenant_id'],cid,'shopify-uninstall')
        except HTTPException as exc:
            if exc.status_code!=409:raise
            cur.execute("UPDATE distribution_connections SET archived_at=now() WHERE tenant_id=%s AND id=%s AND adapter='simulator'",(install['tenant_id'],cid))
            # Do not discard unfinished real fulfillment. Keep it visible in InkSuite.
            cur.execute('UPDATE distribution_connections SET enabled=false,orders_enabled=false,inventory_enabled=false WHERE tenant_id=%s AND id=%s',(install['tenant_id'],cid))


def cleanup_credentials():
    from . import secrets
    with s.transaction() as cur:
        pending=s.one(cur,"""SELECT c.id,c.tenant_id FROM distribution_connections c
            WHERE c.adapter='simulator' AND c.archived_at IS NOT NULL AND c.secret_reference IS NULL
            AND NOT EXISTS(SELECT 1 FROM distribution_jobs j WHERE j.tenant_id=c.tenant_id AND j.connection_id=c.id AND j.status='RUNNING')
            AND NOT EXISTS(SELECT 1 FROM distribution_shopify_publish_jobs j WHERE j.tenant_id=c.tenant_id AND j.connection_id=c.id AND j.status='RUNNING')
            ORDER BY c.archived_at LIMIT 1 FOR UPDATE SKIP LOCKED""")
        if pending:
            purge_simulator(cur,pending['tenant_id'],pending['id'])
            return True
        row=s.one(cur,"SELECT id,tenant_id,secret_reference FROM distribution_connections WHERE archived_at IS NOT NULL AND secret_reference IS NOT NULL ORDER BY archived_at LIMIT 1 FOR UPDATE SKIP LOCKED")
        if not row:return False
        secrets.remove(row['secret_reference'])
        cur.execute('UPDATE distribution_connections SET secret_reference=NULL WHERE tenant_id=%s AND id=%s',(row['tenant_id'],row['id']))
    return True
