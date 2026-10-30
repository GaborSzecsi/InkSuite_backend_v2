"""Reflect explicitly simulated shipments in their owning development store only."""
from collections import Counter
from . import service as s, shopify as sh
from .adapters import ConfigurationRequired
from .shopify_execution import require_development_store

ORDER_QUERY='''query SimulatedFulfillment($id:ID!){order(id:$id){id cancelledAt displayFinancialStatus fulfillmentOrders(first:50){pageInfo{hasNextPage} nodes{id status assignedLocation{location{id}} lineItems(first:100){pageInfo{hasNextPage} nodes{id remainingQuantity totalQuantity lineItem{variant{id}}}}}}}}'''
CREATE_QUERY='''mutation SimulatedFulfillmentCreate($fulfillment:FulfillmentInput!){fulfillmentCreate(fulfillment:$fulfillment,message:"InkSuite development-store simulated shipment"){fulfillment{id status} userErrors{field message}}}'''


def fulfill_simulated_order(cur, conn, order):
    if conn['adapter']!='simulator' or order['source']!='SHOPIFY':
        raise ConfigurationRequired('Only simulated Shopify shipments are supported here.')
    if order['external_order_id'].startswith('simulation-'):
        return
    if not order['external_order_id'].startswith('gid://shopify/Order/'):
        raise ConfigurationRequired('A Shopify order identity is required.')
    install=s.one(cur,'SELECT * FROM distribution_shopify_installations WHERE tenant_id=%s AND shop=%s AND active FOR UPDATE',(conn['tenant_id'],order['source_account']))
    if not install or not install['location_id'] or str(conn.get('shopify_installation_id'))!=str(install['id']):
        raise ConfigurationRequired('The simulator must belong to this active store installation.')
    require_development_store(cur,install)
    sim=s.one(cur,'SELECT status FROM distribution_simulated_orders WHERE tenant_id=%s AND connection_id=%s AND order_id=%s',(conn['tenant_id'],conn['id'],order['id']))
    if not sim or sim['status']!='SHIPPED':
        raise ConfigurationRequired('Explicitly simulate the shipment before fulfilling it.')
    cur.execute('SELECT edition_id,quantity FROM distribution_order_items WHERE tenant_id=%s AND order_id=%s',(conn['tenant_id'],order['id']))
    expected=Counter({str(i['edition_id']):i['quantity'] for i in cur.fetchall()})
    source=sh.graphql(cur,install,ORDER_QUERY,{'id':order['external_order_id']}).get('order')
    if not source or source['cancelledAt'] or source['displayFinancialStatus']!='PAID':
        raise ConfigurationRequired('The Shopify order is no longer eligible for fulfillment.')
    groups=source['fulfillmentOrders']
    if groups['pageInfo']['hasNextPage']:
        raise ConfigurationRequired('Fulfillment pagination requires review.')
    totals=Counter(); remaining=Counter(); selected=[]
    for group in groups['nodes']:
        if ((group.get('assignedLocation') or {}).get('location') or {}).get('id')!=install['location_id']:
            continue
        if group['lineItems']['pageInfo']['hasNextPage']:
            raise ConfigurationRequired('Fulfillment pagination requires review.')
        lines=[]
        for line in group['lineItems']['nodes']:
            variant=(line['lineItem'].get('variant') or {}).get('id')
            mapping=s.one(cur,'SELECT edition_id FROM distribution_shopify_variants WHERE tenant_id=%s AND installation_id=%s AND connection_id=%s AND variant_id=%s AND active',(conn['tenant_id'],install['id'],conn['id'],variant))
            if not mapping or str(mapping['edition_id']) not in expected:
                continue
            if not line['remainingQuantity'] and group['status']!='CLOSED':
                raise ConfigurationRequired('Shopify has not confirmed these items fulfilled.')
            edition=str(mapping['edition_id'])
            totals[edition]+=line['totalQuantity']
            remaining[edition]+=line['remainingQuantity']
            if line['remainingQuantity']:
                if group['status'] not in ('OPEN','IN_PROGRESS'):
                    raise ConfigurationRequired('Shopify fulfillment is on hold or otherwise unavailable.')
                lines.append({'id':line['id'],'quantity':line['remainingQuantity']})
        if lines:
            selected.append({'fulfillmentOrderId':group['id'],'fulfillmentOrderLineItems':lines})
    if not expected or totals!=expected:
        raise ConfigurationRequired('Shopify quantities, edition matches or fulfillment location changed.')
    # A retry after a lost response reads Shopify first. Never blindly replay a
    # fulfillment or silently complete a partially fulfilled/edited order.
    if +remaining:
        if remaining!=expected:
            raise ConfigurationRequired('Partial fulfillment requires review.')
        result=sh.graphql(cur,install,CREATE_QUERY,{'fulfillment':{'notifyCustomer':False,'lineItemsByFulfillmentOrder':selected}})['fulfillmentCreate']
        if result['userErrors'] or not result.get('fulfillment') or result['fulfillment']['status']!='SUCCESS':
            raise ConfigurationRequired('Shopify rejected the simulated fulfillment. Verify fulfillment write permission and order status.')
    cur.execute('UPDATE distribution_orders SET shopify_fulfillment_reconciled=true WHERE tenant_id=%s AND id=%s',(conn['tenant_id'],order['id']))
