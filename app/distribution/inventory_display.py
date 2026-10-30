"""Read Shopify commitments without recreating deleted orders or reserving twice."""
from . import service as s, shopify as sh
from .shopify_execution import known_shopify_commitments
from .adapters import ConfigurationRequired

LEVEL_QUERY = '''query($item:ID!,$location:ID!){inventoryItem(id:$item){
 inventoryLevel(locationId:$location){quantities(names:["committed","available"]){name quantity}}
}}'''


def reconcile_display(cur, install, rows):
    if not rows or not install.get('location_id'):
        return rows
    current=s.one(cur,'SELECT * FROM distribution_shopify_installations WHERE tenant_id=%s AND id=%s AND active FOR UPDATE',
                  (install['tenant_id'],install['id']))
    if not current or not current['location_id']:
        return rows
    cur.execute('''SELECT edition_id,connection_id,inventory_item_id FROM distribution_shopify_variants
        WHERE tenant_id=%s AND installation_id=%s AND active AND connection_id IS NOT NULL''',
        (current['tenant_id'],current['id']))
    mappings={}
    for v in cur.fetchall():
        key=(str(v['connection_id']),str(v['edition_id']))
        mappings.setdefault(key,[]).append(v['inventory_item_id'])
    known={}
    for row in rows:
        cid=str(row['connection_id']);edition=str(row['edition_id'])
        items=mappings.get((cid,edition),[])
        if not items:
            continue
        if len(items)!=1 or not items[0]:
            raise ConfigurationRequired('Inventory requires one matched Shopify variant per edition.')
        if cid not in known:
            conn=s.connection(cur,current['tenant_id'],row['connection_id'])
            known[cid]=known_shopify_commitments(cur,conn,current)
        item=sh.graphql(cur,current,LEVEL_QUERY,{'item':items[0],'location':current['location_id']})['inventoryItem']
        if not item or not item['inventoryLevel']:
            raise ConfigurationRequired('Shopify inventory is unavailable at the selected location.')
        levels={q['name']:q['quantity'] for q in item['inventoryLevel']['quantities']}
        if 'committed' not in levels or 'available' not in levels:
            raise ConfigurationRequired('Shopify did not return its inventory commitments.')
        extra=max(0,levels['committed']-known[cid][edition])
        row['shopify_untracked_commitments']=extra
        row['reserved_quantity']+=extra
        row['sellable_quantity']=max(0,row['sellable_quantity']-extra)
        row['shopify_available_quantity']=levels['available']
    return rows
