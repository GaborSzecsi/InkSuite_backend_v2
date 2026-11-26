from contextlib import nullcontext
from hashlib import sha256
from unittest.mock import Mock
from uuid import uuid4

import pytest
from fastapi import HTTPException

from app.distribution import shipment_fulfillment as shipment
from app.distribution.shipment_files import ShipmentFeed
from app.distribution.shipment_routes import FulfillIn, confirmed_shipment

LOCATION = 'gid://shopify/Location/1'


def group(location=LOCATION, remaining=2):
    return {
        'id': 'fulfillment-order', 'status': 'OPEN',
        'supportedActions': [{'action': 'CREATE_FULFILLMENT'}],
        'assignedLocation': {'location': {'id': location}},
        'lineItems': {'pageInfo': {'hasNextPage': False}, 'nodes': [
            {'id': 'line-item', 'totalQuantity': 2, 'remainingQuantity': remaining},
        ]},
    }


def order(*groups):
    return {
        'id': 'gid://shopify/Order/1', 'name': '#991', 'cancelledAt': None,
        'displayFinancialStatus': 'PAID',
        'fulfillmentOrders': {'pageInfo': {'hasNextPage': False}, 'nodes': list(groups)},
    }


def test_complete_shipment_only_includes_selected_location():
    lines = shipment.full_shipment_lines(order(group(), group('elsewhere', remaining=1)), LOCATION)
    assert lines == [{'fulfillmentOrderId': 'fulfillment-order',
                      'fulfillmentOrderLineItems': [{'id': 'line-item', 'quantity': 2}]}]


def test_partial_shipment_requires_review():
    with pytest.raises(HTTPException, match='partially fulfilled'):
        shipment.full_shipment_lines(order(group(remaining=1)), LOCATION)


@pytest.mark.parametrize('content,expected', [
    (b'#991\tABC\n', None),
    (b'#991\tABC\n#991\tDEF\n', 'multiple shipment rows'),
    (b'#261\tABC\n', 'skipped'),
])
def test_live_uses_source_filters_and_blocks_multi_package(content, expected):
    config = ShipmentFeed(name='Feed', host='sftp.example.com', directory='/', filename_pattern='file.txt',
                          mappings={'order_reference': 0, 'tracking_number': 1}, default_carrier='UPS',
                          filters=[{'column': 0, 'operator': 'starts_with', 'value': '#99'}])
    payload = FulfillIn(filename='file.txt', file_hash=sha256(content).hexdigest(), row=1,
                        order_reference='#991', tracking_number='ABC', installation_id=uuid4(),
                        location_id=LOCATION, confirm_complete_shipment=True)
    if expected:
        with pytest.raises(HTTPException, match=expected):
            confirmed_shipment(content, config, payload)
    else:
        assert confirmed_shipment(content, config, payload)['tracking_number'] == 'ABC'
        with pytest.raises(HTTPException, match='file changed'):
            confirmed_shipment(content + b'\n', config, payload)


def run_case(monkeypatch, existing=None, create=None):
    cursor = Mock()
    install = {'location_id': LOCATION}
    monkeypatch.setattr(shipment.db, 'transaction', lambda: nullcontext(cursor))
    monkeypatch.setattr(shipment.db, 'one', Mock(side_effect=[
        {'ready': True}, install, existing, {'shipment_key': 'key'}, install,
    ]))
    monkeypatch.setattr(shipment, 'resolve_order', Mock(return_value=order(group())))
    graphql = Mock(side_effect=[
        {'currentAppInstallation': {'accessScopes': [{'handle': 'write_merchant_managed_fulfillment_orders'}]}},
        create,
    ])
    monkeypatch.setattr(shipment.shopify, 'graphql', graphql)
    fields = {'order_reference': '#991', 'tracking_number': 'ABC', 'carrier': 'UPS'}
    execute = lambda: shipment.post_full_shipment(
        uuid4(), uuid4(), uuid4(), uuid4(), LOCATION, fields, Mock())
    return execute, graphql, cursor


def test_single_shopify_mutation_records_success_without_notification(monkeypatch):
    execute, graphql, cursor = run_case(monkeypatch, create={
        'fulfillmentCreate': {'fulfillment': {'id': 'gid://shopify/Fulfillment/1', 'status': 'SUCCESS'}, 'userErrors': []}})
    assert execute()['status'] == 'posted'
    payload = graphql.call_args.args[3]['fulfillment']
    assert payload['trackingInfo'] == {'number': 'ABC', 'company': 'UPS'}
    assert payload['notifyCustomer'] is False
    assert cursor.execute.call_args.args[1][0] == 'SUCCEEDED'


def test_pending_shipment_never_replays_mutation(monkeypatch):
    execute, graphql, _ = run_case(monkeypatch, existing={'status': 'PENDING'})
    with pytest.raises(HTTPException, match='uncertain'):
        execute()
    assert graphql.call_count == 1


def test_uncertain_mutation_keeps_reservation(monkeypatch):
    execute, graphql, cursor = run_case(monkeypatch, create=HTTPException(502, 'timeout'))
    with pytest.raises(HTTPException, match='uncertain'):
        execute()
    assert graphql.call_count == 2
    cursor.execute.assert_not_called()


@pytest.mark.parametrize('fresh', [True, False])
def test_stable_manual_sync_uses_database_snapshot(monkeypatch, fresh):
    from datetime import datetime, timedelta, timezone
    from app.distribution import router

    current = datetime.now(timezone.utc)
    connection = {'adapter': 'stable_s3_inventory', 'enabled': True, 'mode': 'live',
                  'inventory_enabled': True, 'configuration': {},
                  'inventory_as_of': current - timedelta(hours=0 if fresh else 25)}
    cur = Mock()
    monkeypatch.setattr(router.s, 'transaction', lambda: nullcontext(cur))
    monkeypatch.setattr(router.s, 'connection', Mock(return_value=connection))
    enqueue = Mock(return_value={'queued': True})
    monkeypatch.setattr(router.s, 'enqueue', enqueue)
    monkeypatch.setattr(router.s, 'event', Mock())
    tenant, actor, connection_id = uuid4(), uuid4(), uuid4()
    ctx = {'tenant': {'id': tenant}, 'user': {'id': actor}}
    if fresh:
        assert router.sync(connection_id, ctx)['queued']
        enqueue.assert_called_once_with(cur, tenant, connection_id, 'PUBLISH')
    else:
        with pytest.raises(HTTPException, match='no fresh accepted snapshot'):
            router.sync(connection_id, ctx)
        enqueue.assert_not_called()
