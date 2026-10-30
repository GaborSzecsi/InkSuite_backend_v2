"""Explicitly reset test data for a named Shopify development store."""
import argparse
from app.distribution import service as s
from app.distribution.connections import purge_simulator


def reset(shop, apply=False):
    with s.transaction() as cur:
        cur.execute("""SELECT DISTINCT c.tenant_id,c.id,c.display_name FROM distribution_connections c
            WHERE c.adapter='simulator' AND (
              EXISTS(SELECT 1 FROM distribution_shopify_installations i WHERE i.id=c.shopify_installation_id AND i.tenant_id=c.tenant_id AND i.shop=%s)
              OR EXISTS(SELECT 1 FROM distribution_orders o WHERE o.connection_id=c.id AND o.tenant_id=c.tenant_id AND o.source='SHOPIFY' AND o.source_account=%s)
              OR EXISTS(SELECT 1 FROM distribution_shopify_variants v JOIN distribution_shopify_installations i ON i.id=v.installation_id AND i.tenant_id=v.tenant_id
                 WHERE v.connection_id=c.id AND v.tenant_id=c.tenant_id AND i.shop=%s))
            ORDER BY c.tenant_id,c.id""",(shop,shop,shop))
        rows=cur.fetchall()
        for row in rows:
            shared=s.one(cur,"""SELECT v.variant_id FROM distribution_shopify_variants v
                JOIN distribution_shopify_installations i ON i.id=v.installation_id AND i.tenant_id=v.tenant_id
                WHERE v.tenant_id=%s AND v.connection_id=%s AND i.active AND i.shop<>%s LIMIT 1""",(row['tenant_id'],row['id'],shop))
            if shared: raise RuntimeError('A simulator is shared with another store; reset aborted without changes.')
        if apply:
            for row in rows: purge_simulator(cur,row['tenant_id'],row['id'])
            cur.execute("""DELETE FROM distribution_webhooks w USING distribution_shopify_installations i
                WHERE w.installation_id=i.id AND w.tenant_id=i.tenant_id AND i.shop=%s
                AND w.topic IN ('orders/paid','orders/updated','orders/cancelled')""",(shop,))
        print(('Deleted' if apply else 'Would delete'),len(rows),'simulator connections and their test data.')
        return len(rows)


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--shop',required=True)
    parser.add_argument('--apply',action='store_true')
    args=parser.parse_args()
    reset(args.shop,args.apply)
