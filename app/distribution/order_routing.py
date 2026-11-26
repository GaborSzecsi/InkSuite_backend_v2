"""Connect Shopify variant stock assignments to an optional outbound order destination."""
from uuid import UUID

from fastapi import Depends, HTTPException

from . import service as db
from .models import StrictModel
from .router import admin, ids, router


def outbound_connection(installation, mapped_connection):
    inventory = installation.get('outbound_inventory_connection_id')
    outbound = installation.get('outbound_order_connection_id')
    if inventory and outbound and str(inventory) == str(mapped_connection):
        return outbound
    return mapped_connection


class OrderRouteIn(StrictModel):
    inventory_connection_id: UUID | None
    outbound_connection_id: UUID | None


@router.put('/shopify/{installation_id}/order-route')
def save_order_route(installation_id: UUID, body: OrderRouteIn, ctx=Depends(admin)):
    tenant, actor = ids(ctx)
    inventory_id, outbound_id = body.inventory_connection_id, body.outbound_connection_id
    if bool(inventory_id) != bool(outbound_id):
        raise HTTPException(422, 'Select both the inventory source and the outbound SFTP connection.')
    with db.transaction() as cur:
        install = db.one(cur, """SELECT id FROM distribution_shopify_installations
            WHERE tenant_id=%s AND id=%s AND active FOR UPDATE""", (tenant, installation_id))
        if not install:
            raise HTTPException(404, 'Active Shopify store not found.')
        if inventory_id:
            inventory = db.connection(cur, tenant, inventory_id)
            outbound = db.connection(cur, tenant, outbound_id)
            if inventory['adapter'] != 'stable_s3_inventory' or outbound['adapter'] != 'hachette_exporteo':
                raise HTTPException(422, 'Choose a Stable inventory source and a Hachette order export connection.')
            if outbound['shopify_installation_id'] and str(outbound['shopify_installation_id']) != str(installation_id):
                raise HTTPException(409, 'The outbound connection belongs to a different Shopify installation.')
        cur.execute("""UPDATE distribution_shopify_installations
            SET outbound_inventory_connection_id=%s,outbound_order_connection_id=%s
            WHERE tenant_id=%s AND id=%s""", (inventory_id, outbound_id, tenant, installation_id))
        if inventory_id:
            db.event(cur, tenant, inventory_id, actor, 'SHOPIFY_OUTBOUND_ROUTE_CONFIGURED',
                     details={'installation_id': str(installation_id), 'outbound_connection_id': str(outbound_id)})
    return {'inventory_connection_id': inventory_id, 'outbound_connection_id': outbound_id}
