"""Explicit recovery of untouched Shopify previews into a test distributor."""
from fastapi import HTTPException
from . import service as s
from .execution import active
from .shopify_execution import require_development_store
from .adapters import ConfigurationRequired


def retry_simulated_order(cur, install, order_id, connection_id):
    tenant=install['tenant_id']
    target=s.connection(cur,tenant,connection_id,lock=True)
    if target['adapter']!='simulator' or not active(target,'orders_enabled'):
        raise HTTPException(409,'Select a simulated distributor with order processing enabled.')
    if target.get('shopify_installation_id') and str(target['shopify_installation_id'])!=str(install['id']):
        raise HTTPException(403,'This distributor belongs to another store.')
    try: require_development_store(cur,install)
    except ConfigurationRequired as exc: raise HTTPException(409,str(exc)) from None
    order=s.one(cur,"""SELECT * FROM distribution_orders WHERE tenant_id=%s AND id=%s
        AND source='SHOPIFY' AND source_account=%s FOR UPDATE""",(tenant,order_id,install['shop']))
    if not order: raise HTTPException(404,'Order not found in this store.')
    if order['status']!='RECEIVED' or order['mode']!='preview':
        raise HTTPException(409,'Only an unsubmitted preview can be retried this way.')
    for table in ('distribution_jobs','distribution_reservations','distribution_simulated_orders'):
        if s.one(cur,f'SELECT order_id FROM {table} WHERE tenant_id=%s AND order_id=%s LIMIT 1',(tenant,order_id)):
            raise HTTPException(409,'This order already has processing records; it cannot be rerouted.')
    external=order['external_order_id']
    if s.one(cur,"""SELECT id FROM distribution_orders WHERE tenant_id=%s AND source='SHOPIFY'
        AND source_account=%s AND external_order_id=%s AND id<>%s LIMIT 1""",(tenant,install['shop'],external,order_id)):
        raise HTTPException(409,'This order is split across distributors and requires separate review.')
    webhook=s.one(cur,"""SELECT id FROM distribution_webhooks WHERE tenant_id=%s AND installation_id=%s
        AND topic IN ('orders/paid','orders/updated')
        AND (payload->>'id'=%s OR payload->>'admin_graphql_api_id'=%s)
        ORDER BY created_at DESC LIMIT 1 FOR UPDATE""",(tenant,install['id'],external.rsplit('/',1)[-1],external))
    if not webhook: raise HTTPException(409,'No Shopify event is available to recheck this order.')
    cur.execute('SELECT edition_id FROM distribution_order_items WHERE tenant_id=%s AND order_id=%s',(tenant,order_id))
    editions=[r['edition_id'] for r in cur.fetchall()]
    if not editions: raise HTTPException(409,'This order has no matched editions.')
    for edition in editions:
        cur.execute("""UPDATE distribution_shopify_variants SET connection_id=%s
            WHERE tenant_id=%s AND installation_id=%s AND active AND edition_id=%s
            AND connection_id=%s RETURNING variant_id""",(connection_id,tenant,install['id'],edition,order['connection_id']))
        if not cur.fetchall(): raise HTTPException(409,'Product matches changed. Refresh and review the order before retrying.')
    # Re-ingest from Shopify so paid/cancelled status, location and quantities are
    # freshly checked. Never submit directly using a stale stored preview.
    for table in ('distribution_events','distribution_order_items'):
        cur.execute(f'DELETE FROM {table} WHERE tenant_id=%s AND order_id=%s',(tenant,order_id))
    cur.execute('DELETE FROM distribution_access_audit WHERE tenant_id=%s AND resource_id=%s',(tenant,order_id))
    cur.execute('DELETE FROM distribution_orders WHERE tenant_id=%s AND id=%s',(tenant,order_id))
    cur.execute("UPDATE distribution_webhooks SET status='RECEIVED',error_code=NULL WHERE id=%s",(webhook['id'],))
    return {'queued':True}
