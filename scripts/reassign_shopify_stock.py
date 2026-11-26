"""Move matched Shopify print variants from a simulator to the Stable stock source."""
import argparse
from collections import Counter

from app.distribution import service as db, shopify

ITEMS_QUERY = """query StockReassignmentItems($items: [ID!]!, $location: ID!) {
    nodes(ids: $items) {
        ... on InventoryItem {
            id
            tracked
            inventoryLevel(locationId: $location) {
                quantities(names: ["on_hand", "committed"]) { name }
            }
        }
    }
}"""


def candidates_for_store(cur, tenant, installation_id, source_id, target_id):
    cur.execute("""SELECT v.variant_id, v.edition_id, v.sku, v.inventory_item_id,
        EXISTS (
            SELECT 1 FROM editions e JOIN distribution_inventory a
              ON a.tenant_id=e.tenant_id AND a.edition_id=e.id
            WHERE e.tenant_id=v.tenant_id AND e.id=v.edition_id AND e.archived_at IS NULL
              AND a.connection_id=%s AND a.sync_status='OK'
              AND a.last_synced_at>now()-interval '24 hours'
        ) AS fresh,
        EXISTS (
            SELECT 1 FROM distribution_shopify_variants other
            JOIN distribution_shopify_installations i
              ON i.tenant_id=other.tenant_id AND i.id=other.installation_id
            WHERE other.tenant_id=v.tenant_id AND other.edition_id=v.edition_id
              AND other.connection_id=%s AND other.active AND i.active
        ) AS existing_target
        FROM distribution_shopify_variants v
        WHERE v.tenant_id=%s AND v.installation_id=%s AND v.connection_id=%s AND v.active
        ORDER BY v.sku,v.variant_id FOR UPDATE OF v""",
        (target_id, target_id, tenant, installation_id, source_id))
    return cur.fetchall()


def select_candidates(rows):
    editions = Counter(row['edition_id'] for row in rows)
    items = Counter(row['inventory_item_id'] for row in rows)
    skipped = Counter()
    selected = []
    for row in rows:
        if not row['edition_id'] or not row['inventory_item_id']:
            skipped['missing edition or inventory item'] += 1
        elif not row['fresh']:
            skipped['no fresh Stable stock record'] += 1
        elif row['existing_target'] or editions[row['edition_id']] > 1 or items[row['inventory_item_id']] > 1:
            skipped['ambiguous or already assigned edition'] += 1
        else:
            selected.append(row)
    return selected, skipped


def verify_shopify_levels(cur, install, selected, skipped):
    eligible = []
    for start in range(0, len(selected), 50):
        batch = selected[start:start + 50]
        response = shopify.graphql(cur, install, ITEMS_QUERY, {
            'items': [row['inventory_item_id'] for row in batch], 'location': install['location_id'],
        })
        items = {item['id']: item for item in response['nodes'] if item and item.get('id')}
        for row in batch:
            item = items.get(row['inventory_item_id']) or {}
            level = item.get('inventoryLevel') or {}
            names = {quantity['name'] for quantity in level.get('quantities', [])}
            if item.get('tracked') and {'on_hand', 'committed'} <= names:
                eligible.append(row)
            else:
                skipped['untracked or not stocked at Shopify location'] += 1
    return eligible


def reassign(args):
    with db.transaction() as cur:
        tenant = db.one(cur, 'SELECT id FROM tenants WHERE slug=%s', (args.tenant,))
        if not tenant:
            raise SystemExit('Publisher not found.')
        tenant_id = tenant['id']
        cur.execute("""SELECT id,display_name,adapter,enabled,mode,inventory_enabled
            FROM distribution_connections WHERE tenant_id=%s AND archived_at IS NULL
            AND display_name=ANY(%s) ORDER BY id FOR NO KEY UPDATE""",
            (tenant_id, [args.source, args.target]))
        matches = cur.fetchall()
        if len(matches) != 2 or {row['display_name'] for row in matches} != {args.source, args.target}:
            raise SystemExit('Expected one source connection and one target connection.')
        source = next(row for row in matches if row['display_name'] == args.source)
        target = next(row for row in matches if row['display_name'] == args.target)
        if source['adapter'] != 'simulator' or target['adapter'] != 'stable_s3_inventory':
            raise SystemExit('The source must be a simulator and the target must be Stable inventory.')
        if not target['enabled'] or target['mode'] != 'live' or not target['inventory_enabled']:
            raise SystemExit('Enable live inventory processing on the Stable connection first.')
        install = db.one(cur, """SELECT * FROM distribution_shopify_installations
            WHERE tenant_id=%s AND shop=%s AND active FOR UPDATE""", (tenant_id, args.shop))
        if not install or not install['inventory_push_enabled'] or install['location_id'] != args.location:
            raise SystemExit('The store must have inventory publication enabled at the specified location.')

        rows = candidates_for_store(cur, tenant_id, install['id'], source['id'], target['id'])
        selected, skipped = select_candidates(rows)
        eligible = verify_shopify_levels(cur, install, selected, skipped)
        print(f'{len(rows)} active simulator variants; {len(eligible)} eligible.')
        for reason, count in sorted(skipped.items()):
            print(f'  {count} skipped: {reason}')
        for row in eligible:
            print(f"  {row['sku'] or '(no SKU)'} | {row['variant_id']}")
        if not args.apply or not eligible:
            print('No variant mappings changed.')
            return

        ids = [row['variant_id'] for row in eligible]
        cur.execute("""UPDATE distribution_shopify_variants SET connection_id=%s
            WHERE tenant_id=%s AND installation_id=%s AND connection_id=%s
              AND active AND variant_id=ANY(%s)""",
            (target['id'], tenant_id, install['id'], source['id'], ids))
        if cur.rowcount != len(ids):
            raise SystemExit('Mappings changed during this run. No changes were committed.')
        db.event(cur, tenant_id, target['id'], 'stock-reassignment',
                 'SHOPIFY_VARIANTS_REASSIGNED', details={
                     'installation_id': str(install['id']), 'source_connection_id': str(source['id']),
                     'variants': len(ids),
                 })
        job = db.enqueue(cur, tenant_id, target['id'], 'PUBLISH')
    print(f'Moved {len(ids)} variants; Stable publication job: {job["job_id"] or "already queued"}.')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--tenant', required=True)
    parser.add_argument('--shop', required=True)
    parser.add_argument('--source', required=True)
    parser.add_argument('--target', required=True)
    parser.add_argument('--location', required=True, help='Exact Shopify location GID')
    parser.add_argument('--apply', action='store_true', help='Commit mappings and queue stock publication')
    reassign(parser.parse_args())


if __name__ == '__main__':
    main()
