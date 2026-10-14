"""Remove active configuration while preserving historical order references."""
from fastapi import HTTPException
from . import service as s


def remove_connection(cur, tenant, cid, actor):
    row=s.connection(cur,tenant,cid,lock=True)
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
            # Do not discard unfinished real fulfillment. Keep it visible in InkSuite.
            cur.execute('UPDATE distribution_connections SET enabled=false,orders_enabled=false,inventory_enabled=false WHERE tenant_id=%s AND id=%s',(install['tenant_id'],cid))


def cleanup_credentials():
    from . import secrets
    with s.transaction() as cur:
        row=s.one(cur,"SELECT id,tenant_id,secret_reference FROM distribution_connections WHERE archived_at IS NOT NULL AND secret_reference IS NOT NULL ORDER BY archived_at LIMIT 1 FOR UPDATE SKIP LOCKED")
        if not row:return False
        secrets.remove(row['secret_reference'])
        cur.execute('UPDATE distribution_connections SET secret_reference=NULL WHERE tenant_id=%s AND id=%s',(row['tenant_id'],row['id']))
    return True
