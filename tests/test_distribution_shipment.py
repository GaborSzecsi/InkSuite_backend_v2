import stat
from datetime import date
from types import SimpleNamespace
from unittest.mock import Mock
from uuid import uuid4

import pytest
from fastapi import HTTPException
from pydantic import ValidationError

from app.distribution.shipment_files import ShipmentFeed, matching_files, preview, read_file
from app.distribution.shipment_routes import local_matches, save_feed_credentials


def settings(**overrides):
    return ShipmentFeed.model_validate({
        'name': 'Example feed', 'host': 'sftp.example.com', 'directory': '/confirmations',
        'host_key': 'AAAA', 'filename_pattern': 'confirm_{{MM}}{{DD}}{{YYYY}}-{{*}}.txt',
        'mappings': {'order_reference': 0, 'tracking_number': 5},
        'default_carrier': 'UPS', 'filters': [{'column': 0, 'operator': 'starts_with', 'value': '#99'}],
        **overrides,
    })


def test_bom_tab_no_header_filter_invalid_and_duplicate():
    content = ('\ufeff#991\t\t\t\t\tTRACK\r\n#100\t\t\t\t\tOTHER\n'
               '#992\t\t\t\t\t\n#993\tshort\n\n#991\t\t\t\t\tTRACK\r\n').encode()
    result = preview(content, settings())
    assert result['columns'] == list(range(6))
    assert [row['status'] for row in result['rows']] == [
        'parsed', 'skipped', 'invalid', 'invalid', 'duplicate_in_file']
    assert result['rows'][0]['fields'] == {
        'order_reference': '#991', 'tracking_number': 'TRACK', 'carrier': 'UPS'}


def test_named_csv_with_quoted_fields_and_malformed_row():
    config = settings(header=True, column_mode='name', delimiter=',', filters=[],
                      mappings={'order_reference': 'Order', 'tracking_number': 'Tracking'})
    result = preview(b'Order,Tracking,Note\n"#5","T01","quoted,comma"\n"#6', config)
    assert result['columns'] == ['Order', 'Tracking', 'Note']
    assert [r['status'] for r in result['rows']] == ['parsed', 'invalid']


def test_named_mapping_requires_header():
    with pytest.raises(ValidationError):
        settings(column_mode='name')


def test_bounded_listing_ignores_symlinks_and_unsafe_names():
    client = Mock()
    entries = [('confirm_11252026-b.txt', stat.S_IFREG),
               ('confirm_11242026-a.txt', stat.S_IFREG),
               ('confirm_11232026-c.txt', stat.S_IFREG),
               ('confirm_11252026-link.txt', stat.S_IFLNK),
               ('confirm_11252026-../bad.txt', stat.S_IFREG)]
    client.listdir_iter.return_value = [SimpleNamespace(filename=name, st_mode=mode)
                                        for name, mode in entries]
    assert matching_files(client, settings(lookback_days=2), date(2026, 11, 25)) == [
        'confirm_11242026-a.txt', 'confirm_11252026-b.txt']
    with pytest.raises(ValueError):
        read_file(client, settings(), '../unsafe.txt')
    client.open.assert_not_called()


def test_order_matching_limits_tenant_connection_and_shop(monkeypatch):
    from app.distribution import shipment_routes
    tenant, connection_id, installation_id = uuid4(), uuid4(), uuid4()
    monkeypatch.setattr(shipment_routes.db, 'one', lambda *_: {'shop': 'example.myshopify.com', 'location_id': 'L'})
    cur = Mock()
    cur.fetchall.return_value = [{'reference': '#991', 'status': 'SUBMITTED'}]
    rows = preview(b'#991\t\t\t\t\tA\n#992\t\t\t\t\tB', settings())['rows']
    local_matches(cur, tenant, {'id': connection_id, 'shopify_installation_id': installation_id}, settings(), rows)
    query, values = cur.execute.call_args.args
    assert 'tenant_id=%s AND connection_id=%s AND source=%s' in query
    assert values[:5] == (tenant, connection_id, 'SHOPIFY', 'example.myshopify.com', 'example.myshopify.com')
    assert [row['match_status'] for row in rows] == ['matched', 'unmatched']


def test_invalid_credential_request_does_not_echo_password():
    async def stream():
        yield b'{"username":"","password":"test-secret"}'

    request = save_feed_credentials(uuid4(), SimpleNamespace(stream=stream), {})
    with pytest.raises(HTTPException) as result:
        request.send(None)
    assert result.value.status_code == 422
    assert 'test-secret' not in result.value.detail


def test_shopify_match_requires_exact_name_and_distributor_location(monkeypatch):
    from app.distribution import shipment_shopify

    tenant, installation_id = uuid4(), uuid4()
    install = {'id': installation_id, 'location_id': 'L'}
    monkeypatch.setattr(shipment_shopify.db, 'one', lambda *_: install)
    groups = {'pageInfo': {'hasNextPage': False}, 'nodes': [
        {'status': 'OPEN', 'assignedLocation': {'location': {'id': 'L', 'name': 'Warehouse'}}},
        {'status': 'OPEN', 'assignedLocation': {'location': {'id': 'OTHER', 'name': 'Other'}}},
    ]}
    query = Mock(return_value={'orders': {'pageInfo': {'hasNextPage': False}, 'nodes': [
        {'name': '#991', 'cancelledAt': None, 'displayFulfillmentStatus': 'UNFULFILLED', 'fulfillmentOrders': groups},
        {'name': '992', 'cancelledAt': None, 'displayFulfillmentStatus': 'UNFULFILLED', 'fulfillmentOrders': groups},
    ]}})
    monkeypatch.setattr(shipment_shopify.shopify, 'graphql', query)
    rows = preview(b'#991\t\t\t\t\tA\n#992\t\t\t\t\tB', settings())['rows']
    shipment_shopify.annotate_preview(Mock(), tenant, {'shopify_installation_id': installation_id}, rows)
    assert [row['match_status'] for row in rows] == ['matched_in_shopify', 'unmatched_in_shopify']
    assert rows[0]['configured_location_id'] == 'L'
    assert query.call_args.args[-1]['search'] == 'name:"991" OR name:"992"'
