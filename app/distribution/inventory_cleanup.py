"""Withdraw distributor availability while the merchant still grants API access."""
from uuid import uuid4
from fastapi import HTTPException
from . import service as s, shopify as sh
from .shopify_execution import SET_QUERY

LEVEL_QUERY = '''query($item:ID!,$location:ID!){inventoryItem(id:$item){
 tracked inventoryLevel(locationId:$location){quantities(names:["available"]){name quantity}}
}}'''


def clear_connection_inventory(cur, connection):
    """Zero only this connection's managed availability, preserving commitments.

    Caller holds the connection lock, before deleting mappings. Never call from
    app/uninstalled: Shopify has already revoked the token at that point.
    A failed request must abort removal so the merchant can retry with mappings intact.
    """
    tenant, cid = connection['tenant_id'], connection['id']
    cur.execute('''SELECT DISTINCT i.id FROM distribution_shopify_installations i
        JOIN distribution_shopify_variants v ON v.installation_id=i.id AND v.tenant_id=i.tenant_id
        WHERE v.tenant_id=%s AND v.connection_id=%s AND v.active AND i.active
        AND (i.inventory_push_enabled OR i.simulated_inventory_connection_id=%s)
        ORDER BY i.id''', (tenant, cid, cid))
    installs = cur.fetchall()
    for entry in installs:
        install = s.one(cur, '''SELECT * FROM distribution_shopify_installations
            WHERE tenant_id=%s AND id=%s AND active FOR UPDATE''', (tenant, entry['id']))
        if not install:
            continue
        if not install['location_id']:
            raise HTTPException(409, 'Save the Shopify stock location before removing this distributor.')
        cur.execute('''SELECT DISTINCT v.inventory_item_id FROM distribution_shopify_variants v
            WHERE v.tenant_id=%s AND v.installation_id=%s AND v.connection_id=%s AND v.active''',
            (tenant, install['id'], cid))
        variants = cur.fetchall()
        for variant in variants:
            item_id = variant['inventory_item_id']
            if not item_id:
                raise HTTPException(409, 'Import Shopify products before removing this distributor so its stock can be cleared.')
            shared = s.one(cur, '''SELECT v.variant_id FROM distribution_shopify_variants v
                JOIN distribution_shopify_installations i ON i.tenant_id=v.tenant_id AND i.id=v.installation_id
                WHERE i.active AND i.shop=%s AND i.location_id=%s AND v.active
                AND v.inventory_item_id=%s AND v.connection_id IS DISTINCT FROM %s LIMIT 1''',
                (install['shop'], install['location_id'], item_id, cid))
            if shared:
                raise HTTPException(409, 'This stock location has another product mapping. Resolve shared stock before removing the distributor.')
            try:
                item = sh.graphql(cur, install, LEVEL_QUERY, {'item': item_id, 'location': install['location_id']})['inventoryItem']
                # Deleted products and deactivated levels have no availability to withdraw.
                if not item:
                    continue
                if not item['tracked']:
                    raise HTTPException(409, 'Enable Shopify inventory tracking before removing the distributor.')
                if not item['inventoryLevel']:
                    continue
                quantities = {q['name']: q['quantity'] for q in item['inventoryLevel']['quantities']}
                if 'available' not in quantities:
                    raise ValueError('Missing availability')
                if quantities['available'] == 0:
                    continue
                result = sh.graphql(cur, install, SET_QUERY, {'key': str(uuid4()), 'input': {
                    'name': 'available', 'reason': 'correction',
                    'referenceDocumentUri': 'gid://inksuite/DistributorRemoval/' + str(cid),
                    'quantities': [{'inventoryItemId': item_id, 'locationId': install['location_id'],
                                    'quantity': 0, 'changeFromQuantity': quantities['available']}]}})
                if result['inventorySetQuantities']['userErrors']:
                    raise ValueError('Shopify rejected inventory cleanup')
            except Exception as exc:
                raise HTTPException(409, 'Shopify stock could not be cleared. The distributor was not removed. Check inventory tracking and permissions, then retry removal.') from exc
