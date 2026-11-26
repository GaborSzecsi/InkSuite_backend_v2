"""Manually fulfill a verified complete shipment from the distributor feed."""
import hashlib

from fastapi import HTTPException

from . import service as db, shopify
from .shipment_shopify import search_term

ORDER = '''query ShipmentFulfillmentOrder($search: String!) {
    orders(first: 2, query: $search) {
        pageInfo { hasNextPage }
        nodes {
            id name cancelledAt displayFinancialStatus
            fulfillmentOrders(first: 5) {
                pageInfo { hasNextPage }
                nodes {
                    id status
                    supportedActions { action }
                    assignedLocation { location { id } }
                    lineItems(first: 100) {
                        pageInfo { hasNextPage }
                        nodes { id totalQuantity remainingQuantity }
                    }
                }
            }
        }
    }
}'''
SCOPES = '''query ShipmentWritePermission {
    currentAppInstallation { accessScopes { handle } }
}'''
CREATE = '''mutation PostShipmentFulfillment($fulfillment: FulfillmentInput!) {
    fulfillmentCreate(fulfillment: $fulfillment) {
        fulfillment { id status }
        userErrors { field message }
    }
}'''


def full_shipment_lines(order, location_id):
    groups = order['fulfillmentOrders']
    if order['cancelledAt'] or order['displayFinancialStatus'] != 'PAID' or groups['pageInfo']['hasNextPage']:
        raise HTTPException(409, 'Order eligibility or fulfillment assignments require review.')
    selected = []
    for group in groups['nodes']:
        location = (group['assignedLocation'] or {}).get('location') or {}
        if location.get('id') != location_id:
            continue
        lines = group['lineItems']
        if (lines['pageInfo']['hasNextPage'] or group['status'] != 'OPEN'
                or not any(action['action'] == 'CREATE_FULFILLMENT' for action in group['supportedActions'])):
            raise HTTPException(409, 'An assigned fulfillment order is unavailable or requires review.')
        items = []
        for line in lines['nodes']:
            remaining = line['remainingQuantity']
            if remaining <= 0 or remaining != line['totalQuantity']:
                raise HTTPException(409, 'An item is already fulfilled or partially fulfilled.')
            items.append({'id': line['id'], 'quantity': remaining})
        if not items:
            raise HTTPException(409, 'A fulfillment order has no eligible items.')
        selected.append({'fulfillmentOrderId': group['id'], 'fulfillmentOrderLineItems': items})
    if not selected:
        raise HTTPException(409, 'No fulfillment orders were assigned to the selected location.')
    return selected


def lock_install(cur, tenant, installation_id, location_id):
    install = db.one(cur, """SELECT * FROM distribution_shopify_installations
        WHERE tenant_id=%s AND id=%s AND active FOR UPDATE""", (tenant, installation_id))
    if not install or install['location_id'] != location_id:
        raise HTTPException(409, 'The Shopify store or fulfillment location has changed. Preview the file again.')
    return install


def resolve_order(cur, install, reference):
    connection = shopify.graphql(cur, install, ORDER, {'search': search_term(reference)})['orders']
    exact = [order for order in connection['nodes'] if order['name'] == reference]
    if connection['pageInfo']['hasNextPage'] or len(exact) != 1:
        raise HTTPException(409, 'Shopify order reference could not be matched uniquely.')
    return exact[0]


def post_full_shipment(tenant, actor, connection_id, installation_id, location_id, fields, check_configuration):
    with db.transaction() as cur:
        check_configuration(cur)
        if not db.one(cur, "SELECT to_regclass('public.distribution_shipment_fulfillments') AS ready")['ready']:
            raise HTTPException(503, 'Live fulfillment awaits migration 049.')
        install = lock_install(cur, tenant, installation_id, location_id)
        access = shopify.graphql(cur, install, SCOPES)['currentAppInstallation']['accessScopes']
        if not any(scope['handle'] == 'write_merchant_managed_fulfillment_orders' for scope in access):
            raise HTTPException(403, 'The Shopify installation lacks the merchant-managed fulfillment write scope.')
        order = resolve_order(cur, install, fields['order_reference'])
        key = hashlib.sha256(f"{installation_id}:{order['id']}:{location_id}".encode()).hexdigest()
        existing = db.one(cur, """SELECT status, shopify_fulfillment_id
            FROM distribution_shipment_fulfillments WHERE tenant_id=%s AND shipment_key=%s""", (tenant, key))
        if existing and existing['status'] == 'SUCCEEDED':
            return {'status': 'already_posted', 'fulfillment_id': existing['shopify_fulfillment_id']}
        if existing and existing['status'] == 'PENDING':
            raise HTTPException(409, 'A submission is in progress or its result is uncertain. Check the Shopify order before taking further action.')
        lines = full_shipment_lines(order, location_id)
        # Commit the reservation before sending any mutation so a lost response cannot cause a duplicate.
        claimed = db.one(cur, """INSERT INTO distribution_shipment_fulfillments AS ledger
            (tenant_id, shipment_key, connection_id, actor_id) VALUES (%s,%s,%s,%s)
            ON CONFLICT (tenant_id,shipment_key) DO UPDATE
            SET status='PENDING', actor_id=EXCLUDED.actor_id, updated_at=now()
            WHERE ledger.status='REJECTED' RETURNING shipment_key""", (tenant, key, connection_id, actor))
        if not claimed:
            raise HTTPException(409, 'This shipment is already being processed.')

    with db.transaction() as cur:
        check_configuration(cur)
        install = lock_install(cur, tenant, installation_id, location_id)
        current = resolve_order(cur, install, fields['order_reference'])
        if current['id'] != order['id'] or full_shipment_lines(current, location_id) != lines:
            raise HTTPException(409, 'Shopify fulfillment items changed. Review the shipment in Shopify.')
        try:
            result = shopify.graphql(cur, install, CREATE, {'fulfillment': {
                'lineItemsByFulfillmentOrder': lines,
                'trackingInfo': {'number': fields['tracking_number'], 'company': fields['carrier']},
                'notifyCustomer': False,
            }})['fulfillmentCreate']
        except (HTTPException, KeyError, TypeError):
            raise HTTPException(502, 'Shopify fulfillment outcome is uncertain. Check the order in Shopify before taking further action.') from None
        fulfillment = result.get('fulfillment')
        if fulfillment and fulfillment.get('status') == 'SUCCESS':
            status, fulfillment_id = 'SUCCEEDED', fulfillment['id']
        elif not fulfillment and result.get('userErrors'):
            status, fulfillment_id = 'REJECTED', None
        else:
            raise HTTPException(502, 'Shopify fulfillment outcome is uncertain. Check the order in Shopify before taking further action.')
        cur.execute("""UPDATE distribution_shipment_fulfillments
            SET status=%s, shopify_fulfillment_id=%s, updated_at=now()
            WHERE tenant_id=%s AND shipment_key=%s""", (status, fulfillment_id, tenant, key))
    if status == 'REJECTED':
        raise HTTPException(409, 'Shopify rejected this fulfillment. Check the order status, location, and fulfillment permissions.')
    return {'status': 'posted', 'fulfillment_id': fulfillment_id}
