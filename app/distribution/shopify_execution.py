"""Shopify boundaries: transient delivery retrieval and guarded inventory publication."""
from collections import Counter
from uuid import uuid4
from . import service as s, shopify as sh, privacy
from .adapters import ConfigurationRequired
from .models import ShippingAddress

# The preview/webhook query remains contact-free. This query runs only at transmission.
DELIVERY_QUERY=sh.ORDER_QUERY.replace(
    'id createdAt displayFinancialStatus cancelledAt',
    'id createdAt displayFinancialStatus cancelledAt shippingAddress { name company address1 address2 city provinceCode zip countryCodeV2 phone }')

def mapped_quantities(cur,install,source,connection_id):
    fulfillment=source['fulfillmentOrders']
    if fulfillment['pageInfo']['hasNextPage']:
        raise ConfigurationRequired('Fulfillment pagination requires review.')
    quantities=Counter()
    for group in fulfillment['nodes']:
        if ((group.get('assignedLocation') or {}).get('location') or {}).get('id')!=install['location_id']: continue
        if group['lineItems']['pageInfo']['hasNextPage']:
            raise ConfigurationRequired('Fulfillment pagination requires review.')
        for line in group['lineItems']['nodes']:
            variant=(line['lineItem'].get('variant') or {}).get('id')
            mapping=s.one(cur,"SELECT edition_id FROM distribution_shopify_variants WHERE tenant_id=%s AND installation_id=%s AND variant_id=%s AND connection_id=%s AND active",(install['tenant_id'],install['id'],variant,connection_id))
            if mapping and mapping['edition_id'] and line['remainingQuantity']>0:
                quantities[str(mapping['edition_id'])]+=line['remainingQuantity']
    return quantities

def delivery_order(cur,conn,order,items):
    if order['source']!='SHOPIFY' or order['mode']!='live':
        raise ConfigurationRequired('Only authorized live Shopify orders can be transmitted.')
    install=s.one(cur,'SELECT * FROM distribution_shopify_installations WHERE tenant_id=%s AND shop=%s AND active FOR UPDATE',(conn['tenant_id'],order['source_account']))
    if not install or not install['location_id']: raise ConfigurationRequired('Active store and fulfillment location required.')
    if privacy.delivery_restricted(cur,conn['tenant_id'],install['shop'],order['external_order_id']):
        raise ConfigurationRequired('Delivery is restricted pending privacy review or deletion.')
    # Fail closed if the durable audit cannot be committed.
    privacy.durable_delivery_audit(conn['tenant_id'],'DELIVERY_DETAILS_READ_ATTEMPT',order['id'])
    source=sh.graphql(cur,install,DELIVERY_QUERY,{'id':order['external_order_id']})['order']
    privacy.durable_delivery_audit(conn['tenant_id'],'DELIVERY_DETAILS_READ_FOR_TRANSMISSION',order['id'])
    if not source or source['cancelledAt'] or source['displayFinancialStatus']!='PAID':
        raise ConfigurationRequired('Order is no longer eligible for fulfillment.')
    expected=Counter({str(i['edition_id']):i['quantity'] for i in items})
    if mapped_quantities(cur,install,source,conn['id'])!=expected:
        raise ConfigurationRequired('Order quantities or fulfillment location changed.')
    lines=source['shippingLines']
    if lines['pageInfo']['hasNextPage'] or len(lines['nodes'])!=1 or lines['nodes'][0]['title']!=order['shipping_method']:
        raise ConfigurationRequired('Shipping method changed; review the order.')
    address=source.get('shippingAddress') or {}
    try:
        recipient=ShippingAddress(name=address.get('name') or '',company=address.get('company') or '',
            address_1=address.get('address1') or '',address_2=address.get('address2') or '',
            city=address.get('city') or '',state_region=address.get('provinceCode') or '',
            postal_code=address.get('zip') or '',country=address.get('countryCodeV2') or '',
            phone=address.get('phone') or '').model_dump()
    except Exception:
        raise ConfigurationRequired('A valid delivery address is required.') from None
    return {**order,'recipient':recipient}

LEVEL_QUERY='''query($item:ID!,$location:ID!){inventoryItem(id:$item){tracked inventoryLevel(locationId:$location){quantities(names:["on_hand","committed"]){name quantity}}}}'''
SET_QUERY='''mutation($input:InventorySetQuantitiesInput!,$key:String!){
 inventorySetQuantities(input:$input) @idempotent(key:$key){
  inventoryAdjustmentGroup {createdAt} userErrors {code}
 }}'''

DEVELOPMENT_STORE_QUERY='query { shop { plan { partnerDevelopment } } }'

def require_development_store(cur,install):
    data=sh.graphql(cur,install,DEVELOPMENT_STORE_QUERY)
    if ((data.get('shop') or {}).get('plan') or {}).get('partnerDevelopment') is not True:
        raise ConfigurationRequired('Simulated stock can only update a Shopify development store.')


def known_shopify_commitments(cur, conn, install):
    tenant=conn['tenant_id']
    # Use CURRENT remaining lines, not historical quantities, to avoid treating a new
    # sale as an old committed order and accidentally restoring sold stock.
    cur.execute("""SELECT id,external_order_id,status FROM distribution_orders o WHERE tenant_id=%s AND connection_id=%s
        AND source='SHOPIFY' AND source_account=%s AND status IN ('RESERVED','QUEUED','SUBMITTING','SUBMITTED','ACCEPTED','SHIPPED')
        AND (EXISTS (SELECT 1 FROM distribution_reservations r WHERE r.tenant_id=o.tenant_id AND r.order_id=o.id
                    AND r.status IN ('RESERVED','SUBMITTED','FULFILLED')) OR (status='SHIPPED' AND NOT shopify_fulfillment_reconciled))
        ORDER BY created_at LIMIT 1001""",(tenant,conn['id'],install['shop']))
    orders=cur.fetchall()
    if len(orders)>1000: raise ConfigurationRequired('Order reconciliation exceeds the current batch limit.')
    known=Counter()
    for row in orders:
        if conn['adapter']=='simulator' and row['external_order_id'].startswith('simulation-'): continue
        source=sh.graphql(cur,install,sh.ORDER_QUERY,{'id':row['external_order_id']})['order']
        if not source: raise ConfigurationRequired('Cannot reconcile a previously imported order.')
        remaining=mapped_quantities(cur,install,source,conn['id']) if not source['cancelledAt'] else Counter()
        if row['status']=='SHIPPED' and not remaining:
            cur.execute('UPDATE distribution_orders SET shopify_fulfillment_reconciled=true WHERE tenant_id=%s AND id=%s',(tenant,row['id']))
        if remaining:
            cur.execute("""SELECT r.edition_id,r.quantity FROM distribution_reservations r
                JOIN distribution_orders o ON o.tenant_id=r.tenant_id AND o.id=r.order_id
                WHERE o.tenant_id=%s AND o.connection_id=%s AND o.source_account=%s AND o.external_order_id=%s
                AND (r.status IN ('RESERVED','SUBMITTED','FULFILLED') OR (r.status='RELEASED' AND o.status='SHIPPED'))""",(tenant,conn['id'],install['shop'],row['external_order_id']))
            for reservation in cur.fetchall():
                edition=str(reservation['edition_id'])
                known[edition]+=min(reservation['quantity'],remaining[edition])
            # A distributor shipment is already deducted in its physical stock;
            # add back Shopify's still-unfulfilled commitment until Shopify catches up.
            # This also supports connections activated without local reservations.
            if row['status']=='SHIPPED':
                cur.execute("""SELECT i.edition_id,i.quantity FROM distribution_order_items i WHERE i.tenant_id=%s AND i.order_id=%s
                    AND NOT EXISTS (SELECT 1 FROM distribution_reservations r WHERE r.tenant_id=i.tenant_id AND r.order_id=i.order_id AND r.edition_id=i.edition_id)""",(tenant,row['id']))
                for item in cur.fetchall():
                    known[str(item['edition_id'])]+=min(item['quantity'],remaining[str(item['edition_id'])])
    return known


def push_inventory(cur,conn):
    """One stock pool per managed Shopify inventory item, guarded against racing sales."""
    tenant=conn['tenant_id']
    cur.execute("""SELECT DISTINCT i.* FROM distribution_shopify_installations i
        JOIN distribution_shopify_variants v ON v.tenant_id=i.tenant_id AND v.installation_id=i.id
        WHERE v.tenant_id=%s AND v.connection_id=%s AND v.active AND i.active AND (i.inventory_push_enabled OR i.simulated_inventory_connection_id=v.connection_id)""",(tenant,conn['id']))
    installations=cur.fetchall()
    for initial in installations:
        install=s.one(cur,'SELECT * FROM distribution_shopify_installations WHERE tenant_id=%s AND id=%s FOR UPDATE',(tenant,initial['id']))
        if not install['active']: continue
        if conn['adapter']!='simulator' and not install['inventory_push_enabled']: continue
        if conn['adapter']=='simulator':
            if str(install.get('simulated_inventory_connection_id'))!=str(conn['id']): continue
            # Recheck on every write batch: a development store can become a live store.
            require_development_store(cur,install)
        if not install['location_id']: raise ConfigurationRequired('Select the Shopify stock location.')
        known=known_shopify_commitments(cur,conn,install)
        cur.execute("""SELECT v.edition_id,v.inventory_item_id,COALESCE(a.sellable_quantity,0) AS quantity,
            a.sync_status,a.last_synced_at
            FROM distribution_shopify_variants v LEFT JOIN distribution_availability a
              ON a.tenant_id=v.tenant_id AND a.connection_id=v.connection_id AND a.edition_id=v.edition_id
            WHERE v.tenant_id=%s AND v.connection_id=%s AND v.installation_id=%s AND v.active AND v.edition_id IS NOT NULL""",
            (tenant,conn['id'],install['id']))
        variants=cur.fetchall()
        for variant in variants:
            count=s.one(cur,"""SELECT count(*) AS n FROM distribution_shopify_variants v
                JOIN distribution_shopify_installations i ON i.tenant_id=v.tenant_id AND i.id=v.installation_id
                WHERE v.tenant_id=%s AND v.connection_id=%s AND v.edition_id=%s AND v.active AND i.active AND (i.inventory_push_enabled OR i.simulated_inventory_connection_id=v.connection_id)""",
                (tenant,conn['id'],variant['edition_id']))['n']
            if count!=1: raise ConfigurationRequired('Allocate shared stock before publishing one edition to multiple variants or stores.')
            if not variant['inventory_item_id']: raise ConfigurationRequired('Import the Shopify inventory item first.')
            item=sh.graphql(cur,install,LEVEL_QUERY,{'item':variant['inventory_item_id'],'location':install['location_id']})['inventoryItem']
            if not item or not item['tracked'] or not item['inventoryLevel']:
                raise ConfigurationRequired('Enable inventory tracking and stock the variant at the selected location.')
            levels={q['name']:q['quantity'] for q in item['inventoryLevel']['quantities']}
            if 'on_hand' not in levels or 'committed' not in levels:
                raise ConfigurationRequired('Shopify did not return required inventory quantities.')
            # Shopify subtracts its own committed stock from on_hand, including new
            # orders not ingested yet. Add back only locally reserved units still
            # committed at Shopify; avoid deducting that same order twice.
            from datetime import datetime,timezone,timedelta
            fresh=variant['last_synced_at']
            if isinstance(fresh,str): fresh=datetime.fromisoformat(fresh.replace('Z','+00:00'))
            target=(int(variant['quantity'])+min(max(0,levels['committed']),known[str(variant['edition_id'])])
                    if variant['sync_status']=='OK' and fresh and fresh>datetime.now(timezone.utc)-timedelta(hours=24) else 0)
            # Keep only the IDs needed to withdraw stock even after uninstall
            # deletes the distributor and clears the installation location.
            from .inventory_cleanup import remember_stock
            remember_stock(cur,install,conn['id'],variant['inventory_item_id'],install['location_id'])
            if levels['on_hand']==target: continue
            result=sh.graphql(cur,install,SET_QUERY,{'key':str(uuid4()),'input':{
                'name':'on_hand','reason':'correction','referenceDocumentUri':'gid://inksuite/Distributor/'+str(conn['id']),
                'quantities':[{'inventoryItemId':variant['inventory_item_id'],'locationId':install['location_id'],
                               'quantity':target,'changeFromQuantity':levels['on_hand']}]}})
            if result['inventorySetQuantities']['userErrors']:
                raise ConfigurationRequired('Inventory changed or Shopify rejected the update; synchronize again.')
        s.event(cur,tenant,conn['id'],'distribution-worker','SHOPIFY_INVENTORY_PUBLISHED',details={'installation_id':str(install['id']),'variants':len(variants)})

