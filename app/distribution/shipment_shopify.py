"""Read Shopify fulfillment assignments for a shipment preview."""
import re
from collections import defaultdict

from fastapi import HTTPException

from . import service as db, shopify

ORDERS = '''query ShipmentPreviewOrders($search: String!) {
    orders(first: 50, query: $search) {
        pageInfo { hasNextPage }
        nodes {
            name
            cancelledAt
            displayFulfillmentStatus
            fulfillmentOrders(first: 5) {
                pageInfo { hasNextPage }
                nodes { status assignedLocation { location { id name } } }
            }
        }
    }
}'''


def search_term(reference):
    # Shopify's name filter accepts the display number without its leading #.
    value = reference.removeprefix('#')
    escaped = re.sub(r'([\\":()])', r'\\\1', value)
    return 'name:"' + escaped + '"'


def annotate_preview(cur, tenant, installation_id, rows):
    candidates = [row for row in rows if row['status'] == 'parsed']
    if not candidates:
        return
    install = db.one(cur, '''SELECT * FROM distribution_shopify_installations
        WHERE tenant_id=%s AND id=%s AND active FOR UPDATE''',
        (tenant, installation_id))
    if not install or not install['location_id']:
        raise HTTPException(409, 'Select a Shopify store in Shipment Confirmation and configure its fulfillment location.')
    references = sorted({row['fields']['order_reference'] for row in candidates})
    if len(references) > 25:
        raise HTTPException(422, 'Shopify matching supports up to 25 different order names per preview. Use a smaller sample.')
    result = shopify.graphql(cur, install, ORDERS, {'search': ' OR '.join(map(search_term, references))})['orders']
    matches = defaultdict(list)
    for order in result['nodes']:
        matches[order['name']].append(order)
    for row in candidates:
        orders = matches[row['fields']['order_reference']]
        if result['pageInfo']['hasNextPage'] or len(orders) > 1:
            row['match_status'] = 'shopify_match_requires_review'
        elif not orders:
            row['match_status'] = 'unmatched_in_shopify'
        else:
            annotate_order(row, orders[0], install['location_id'])


def annotate_order(row, order, location_id):
    groups = order['fulfillmentOrders']
    assigned = [(group, ((group['assignedLocation'] or {}).get('location') or {}))
                for group in groups['nodes']]
    eligible = [(group, location) for group, location in assigned if location.get('id') == location_id]
    row['order_status'] = order['displayFulfillmentStatus']
    if order['cancelledAt']:
        row['match_status'] = 'cancelled_in_shopify'
    elif groups['pageInfo']['hasNextPage'] or not eligible:
        row['match_status'] = 'shopify_location_requires_review'
    else:
        row['configured_location_id'] = location_id
        row['shopify_location_name'] = eligible[0][1]['name']
        row['match_status'] = 'matched_in_shopify'
        if not any(group['status'] in ('OPEN', 'IN_PROGRESS') for group, _ in eligible):
            row['match_status'] = 'no_open_fulfillment_at_location'
