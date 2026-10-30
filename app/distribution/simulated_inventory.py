"""Save simulator stock and queue publication for the authenticated development store."""
from fastapi import HTTPException
from psycopg.types.json import Jsonb
from . import service as s, execution as x
from .shopify_execution import require_development_store
from .adapters import ConfigurationRequired


def set_test_stock(install,connection_id,edition_id,quantity,safety_buffer):
    tenant=install['tenant_id']
    with s.transaction() as cur:
        conn=s.connection(cur,tenant,connection_id,lock=True)
        if conn['adapter']!='simulator':
            raise HTTPException(409,'Select a simulated distributor.')
        if conn.get('shopify_installation_id') and str(conn['shopify_installation_id'])!=str(install['id']):
            raise HTTPException(403,'This distributor belongs to a different store.')
        store=s.one(cur,'SELECT * FROM distribution_shopify_installations WHERE tenant_id=%s AND id=%s AND active FOR UPDATE',(tenant,install['id']))
        if not store or not store['location_id']:
            raise HTTPException(409,'Select and save a Shopify fulfillment location before setting test stock.')
        try: require_development_store(cur,store)
        except ConfigurationRequired as exc: raise HTTPException(409,str(exc)) from None
        edition=s.one(cur,"SELECT isbn13 FROM editions WHERE tenant_id=%s AND id=%s AND archived_at IS NULL AND COALESCE(NULLIF(onix_product_form,''),product_form) IN ('BB','BC','BA','BH','BD','BE','BF','BG','BK')",(tenant,edition_id))
        if not edition or not edition['isbn13']:
            raise HTTPException(404,'Select a print edition with an ISBN in this publisher.')
        isbn=''.join(c for c in edition['isbn13'] if c.isdigit())
        from .protocol import Stock
        try: Stock(isbn=isbn,on_hand=quantity)
        except ValueError: raise HTTPException(422,'Test stock requires a 13 digit ISBN and a nonnegative quantity.') from None
        cur.execute('''SELECT variant_id,connection_id,inventory_item_id FROM distribution_shopify_variants
            WHERE tenant_id=%s AND installation_id=%s AND edition_id=%s AND active FOR UPDATE''',
            (tenant,install['id'],edition_id))
        matches=cur.fetchall()
        if len(matches)!=1 or not matches[0]['inventory_item_id']:
            raise HTTPException(409,'Import and match this edition to exactly one Shopify variant before setting test stock.')
        assigned=matches[0]['connection_id']
        if assigned and str(assigned)!=str(connection_id):
            raise HTTPException(409,'This Shopify variant belongs to another distributor. Review its distributor mapping before setting test stock.')
        cur.execute('''UPDATE distribution_shopify_variants SET connection_id=%s
            WHERE tenant_id=%s AND installation_id=%s AND variant_id=%s''',
            (connection_id,tenant,install['id'],matches[0]['variant_id']))
        config={**conn['configuration'],'simulated_stock':{**conn['configuration'].get('simulated_stock',{}),isbn:quantity}}
        # Enable inventory only. A paused order exporter must not be resumed by this action.
        conn=s.one(cur,"""UPDATE distribution_connections SET configuration=%s,safety_buffer=%s,
            orders_enabled=orders_enabled AND enabled,enabled=true,inventory_enabled=true,
            status='CONNECTED',next_sync_at=now()+interval '5 minutes',updated_at=now()
            WHERE tenant_id=%s AND id=%s RETURNING *""",(Jsonb(config),safety_buffer,tenant,connection_id))
        cur.execute('UPDATE distribution_shopify_installations SET simulated_inventory_connection_id=%s WHERE tenant_id=%s AND id=%s',(connection_id,tenant,install['id']))
        x.reconcile(cur,conn,x.adapter(cur,conn).fetch_inventory())
        result=s.enqueue(cur,tenant,connection_id,'PUBLISH')
        s.event(cur,tenant,connection_id,install['user_id'],'SIMULATED_STOCK_SET',details={'edition_id':str(edition_id),'quantity':quantity,'installation_id':str(install['id'])})
        return result
