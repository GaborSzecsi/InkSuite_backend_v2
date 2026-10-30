"""Withdraw distributor availability while the merchant still grants API access."""
from uuid import uuid4
from fastapi import HTTPException
from . import service as s, shopify as sh
from .shopify_execution import SET_QUERY

LEVEL_QUERY = '''query($item:ID!,$location:ID!){inventoryItem(id:$item){
 tracked inventoryLevel(locationId:$location){quantities(names:["available"]){name quantity}}
}}'''


def remember_stock(cur, install, connection_id, item_id, location_id):
    cur.execute("""INSERT INTO distribution_shopify_managed_stock
        (tenant_id,installation_id,connection_id,shop,inventory_item_id,location_id)
        VALUES(%s,%s,%s,%s,%s,%s)
        ON CONFLICT(tenant_id,shop,inventory_item_id,location_id) DO UPDATE
        SET installation_id=excluded.installation_id,connection_id=excluded.connection_id""",
        (install['tenant_id'],install['id'],connection_id,install['shop'],item_id,location_id))


def withdraw_stock(cur, install, item_id, location_id, connection_id):
    # The location comes from the stock ownership record, not new setup defaults.
    shared = s.one(cur, """SELECT v.variant_id FROM distribution_shopify_variants v
        JOIN distribution_shopify_installations i ON i.tenant_id=v.tenant_id AND i.id=v.installation_id
        WHERE i.active AND i.shop=%s AND i.location_id=%s AND v.active
        AND v.inventory_item_id=%s AND v.connection_id IS DISTINCT FROM %s LIMIT 1""",
        (install['shop'],location_id,item_id,connection_id))
    if shared:
        raise HTTPException(409,'Resolve shared Shopify stock mappings before clearing distributor stock.')
    try:
        item = sh.graphql(cur,install,LEVEL_QUERY,{'item':item_id,'location':location_id})['inventoryItem']
        if not item:
            return  # Deleted product.
        if not item['tracked']:
            raise ValueError('Inventory tracking is disabled')
        if not item['inventoryLevel']:
            return  # No stock at the previously managed location.
        quantities = {q['name']:q['quantity'] for q in item['inventoryLevel']['quantities']}
        available = quantities['available']
        if available == 0:
            return
        result = sh.graphql(cur,install,SET_QUERY,{'key':str(uuid4()),'input':{
            'name':'available','reason':'correction',
            'referenceDocumentUri':'gid://inksuite/DistributorRemoval/'+str(connection_id),
            'quantities':[{'inventoryItemId':item_id,'locationId':location_id,
                           'quantity':0,'changeFromQuantity':available}]}})
        if result['inventorySetQuantities']['userErrors']:
            raise ValueError('Shopify rejected inventory cleanup')
        # Do not claim cleanup succeeded on a successful HTTP response alone.
        after = sh.graphql(cur,install,LEVEL_QUERY,{'item':item_id,'location':location_id})['inventoryItem']
        if after and after['inventoryLevel']:
            values = {q['name']:q['quantity'] for q in after['inventoryLevel']['quantities']}
            if values.get('available') != 0:
                raise ValueError('Shopify stock did not remain zero')
    except Exception as exc:
        raise HTTPException(409,'Shopify stock cleanup is incomplete. Check inventory tracking and permissions, then retry. Connection cleanup has not been completed.') from exc


def forget_stock(cur, row):
    cur.execute("""DELETE FROM distribution_shopify_managed_stock
        WHERE tenant_id=%s AND shop=%s AND inventory_item_id=%s AND location_id=%s""",
        (row['tenant_id'],row['shop'],row['inventory_item_id'],row['location_id']))


def clear_connection_inventory(cur, connection):
    """Withdraw mapped stock before removal; retained ownership survives uninstall."""
    tenant,cid = connection['tenant_id'],connection['id']
    # Backward-compatible discovery for mappings created before stock ownership tracking.
    cur.execute("""SELECT DISTINCT i.* FROM distribution_shopify_installations i
        JOIN distribution_shopify_variants v ON v.installation_id=i.id AND v.tenant_id=i.tenant_id
        WHERE v.tenant_id=%s AND v.connection_id=%s AND v.active AND i.active
        AND (i.inventory_push_enabled OR i.simulated_inventory_connection_id=%s)
        ORDER BY i.id""",(tenant,cid,cid))
    for entry in cur.fetchall():
        install=s.one(cur,'SELECT * FROM distribution_shopify_installations WHERE tenant_id=%s AND id=%s FOR UPDATE',(tenant,entry['id']))
        if not install['location_id']:
            raise HTTPException(409,'Save the Shopify stock location before removing this distributor.')
        cur.execute('SELECT DISTINCT inventory_item_id FROM distribution_shopify_variants WHERE tenant_id=%s AND installation_id=%s AND connection_id=%s AND active',(tenant,install['id'],cid))
        for variant in cur.fetchall():
            if not variant['inventory_item_id']:
                raise HTTPException(409,'Import Shopify products before removing this distributor so its stock can be cleared.')
            remember_stock(cur,install,cid,variant['inventory_item_id'],install['location_id'])
    cur.execute('SELECT * FROM distribution_shopify_managed_stock WHERE tenant_id=%s AND connection_id=%s ORDER BY shop,inventory_item_id,location_id FOR UPDATE',(tenant,cid))
    for row in cur.fetchall():
        install=s.one(cur,'SELECT * FROM distribution_shopify_installations WHERE tenant_id=%s AND shop=%s AND active FOR UPDATE',(tenant,row['shop']))
        if not install:
            raise HTTPException(409,'Reinstall and link InkSuite to clear its remaining Shopify stock.')
        withdraw_stock(cur,install,row['inventory_item_id'],row['location_id'],cid)
        forget_stock(cur,row)


def clear_previous_installation_stock(cur, install):
    """On authenticated relinking, withdraw old stock before new setup is usable.

    Pending IDs survive uninstall; successful cleanup deletes them. Never use an
    old token, infer a location, or touch another publisher's stock ownership.
    """
    cur.execute("""SELECT m.* FROM distribution_shopify_managed_stock m
        JOIN distribution_shopify_installations old ON old.id=m.installation_id
        WHERE m.tenant_id=%s AND m.shop=%s AND m.installation_id<>%s AND NOT old.active
        ORDER BY m.inventory_item_id,m.location_id FOR UPDATE OF m""",
        (install['tenant_id'],install['shop'],install['id']))
    rows=cur.fetchall()
    if not rows:
        return
    current=s.one(cur,'SELECT * FROM distribution_shopify_installations WHERE tenant_id=%s AND id=%s AND active FOR UPDATE',(install['tenant_id'],install['id']))
    if not current:
        raise HTTPException(409,'Relink the active Shopify installation before clearing old stock.')
    for row in rows:
        withdraw_stock(cur,current,row['inventory_item_id'],row['location_id'],row['connection_id'])
        forget_stock(cur,row)
