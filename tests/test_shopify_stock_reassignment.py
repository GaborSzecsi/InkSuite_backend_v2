from collections import Counter
from unittest.mock import Mock

from scripts import reassign_shopify_stock as stock


def test_select_candidates_keeps_only_fresh_unique_mapped_variants():
    def row(name, edition, *, fresh=True, existing_target=False):
        return {'sku': name, 'variant_id': name, 'edition_id': edition,
                'inventory_item_id': 'item-' + name, 'fresh': fresh, 'existing_target': existing_target}

    eligible, skipped = stock.select_candidates([
        row('ready', 'edition-1'),
        row('stale', 'edition-2', fresh=False),
        row('duplicate-a', 'edition-3'),
        row('duplicate-b', 'edition-3'),
        row('already-assigned', 'edition-4', existing_target=True),
        row('no-match', None),
    ])
    assert [item['sku'] for item in eligible] == ['ready']
    assert skipped == {'no fresh Stable stock record': 1, 'ambiguous or already assigned edition': 3,
                       'missing edition or inventory item': 1}


def test_shopify_check_skips_untracked_and_unstocked_items(monkeypatch):
    rows = [{'inventory_item_id': value} for value in ('ready', 'untracked', 'unstocked')]
    monkeypatch.setattr(stock.shopify, 'graphql', Mock(return_value={'nodes': [
        {'id': 'ready', 'tracked': True, 'inventoryLevel': {'quantities': [
            {'name': 'on_hand'}, {'name': 'committed'},
        ]}},
        {'id': 'untracked', 'tracked': False, 'inventoryLevel': None},
        {'id': 'unstocked', 'tracked': True, 'inventoryLevel': None},
    ]}))
    skipped = Counter()
    assert stock.verify_shopify_levels(Mock(), {'location_id': 'location'}, rows, skipped) == [rows[0]]
    assert skipped['untracked or not stocked at Shopify location'] == 2
