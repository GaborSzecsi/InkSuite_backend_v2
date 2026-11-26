from contextlib import nullcontext
from datetime import datetime, timezone
from unittest.mock import Mock
from uuid import uuid4

from app.distribution import shipment_automation as automation
from app.distribution.adapters import HachetteExporteoAdapter
from app.distribution.order_routing import outbound_connection
from app.distribution.shipment_files import ShipmentFeed
from app.distribution.shopify_execution import mapped_quantities


def test_scheduled_feed_posts_only_matching_rows_then_records_checkpoint(monkeypatch):
    tenant, connection, actor, store, lease = (uuid4() for _ in range(5))
    location = 'gid://shopify/Location/1'
    config = ShipmentFeed(
        name='Shipment feed', host='sftp.example.com', directory='/', filename_pattern='*.txt',
        shopify_installation_id=store, default_carrier='UPS',
        mappings={'order_reference': 0, 'tracking_number': 1},
        filters=[{'column': 0, 'operator': 'starts_with', 'value': '#99'}],
    )
    feed = {'tenant_id': tenant, 'connection_id': connection, 'automation_actor': actor,
            'automation_location_id': location, 'automation_enabled': True, 'automation_lease': lease,
            'updated_at': datetime.now(timezone.utc), 'configuration': config.model_dump(mode='json')}
    cur = Mock()
    monkeypatch.setattr(automation.db, 'transaction', lambda: nullcontext(cur))
    monkeypatch.setattr(automation, 'claim', Mock(return_value=feed))
    monkeypatch.setattr(automation.db, 'one', Mock(side_effect=[{'location_id': location}, None]))
    monkeypatch.setattr(automation, 'load_feed', Mock(return_value=({}, feed)))
    monkeypatch.setattr(automation, 'remote_feed', Mock(side_effect=[
        ['shipment.txt'], b'#261\tOTHER\n#991\tABC\n',
    ]))
    post = Mock(return_value={'status': 'posted'})
    monkeypatch.setattr(automation, 'post_full_shipment', post)
    assert automation.run_once()
    post.assert_called_once()
    assert post.call_args.args[:5] == (tenant, actor, connection, store, location)
    assert post.call_args.args[5] == {'order_reference': '#991', 'tracking_number': 'ABC', 'carrier': 'UPS'}
    assert cur.execute.call_args_list[0].args[1][-1] is False
    summary = cur.execute.call_args_list[1].args[1][0].obj
    assert summary == {'files': 1, 'skipped': 1, 'attempted': 1, 'posted': 1}


def test_scheduled_feed_does_not_run_without_due_claim(monkeypatch):
    monkeypatch.setattr(automation.db, 'transaction', lambda: nullcontext(Mock()))
    monkeypatch.setattr(automation, 'claim', Mock(return_value=None))
    process = Mock()
    monkeypatch.setattr(automation, 'process_feed', process)
    assert not automation.run_once()
    process.assert_not_called()


def test_shopify_route_preserves_inventory_mapping_and_routes_outbound(monkeypatch):
    from app.distribution import shopify_execution as execution

    inventory, outbound, edition = uuid4(), uuid4(), uuid4()
    install = {'tenant_id': uuid4(), 'id': uuid4(), 'location_id': 'warehouse',
               'outbound_inventory_connection_id': inventory, 'outbound_order_connection_id': outbound}
    assert outbound_connection(install, inventory) == outbound
    other = uuid4()
    assert outbound_connection(install, other) == other
    row = {'connection_id': inventory, 'edition_id': edition}
    monkeypatch.setattr(execution.s, 'one', Mock(return_value=row))
    source = {'fulfillmentOrders': {'pageInfo': {'hasNextPage': False}, 'nodes': [
        {'assignedLocation': {'location': {'id': 'warehouse'}}, 'lineItems': {
            'pageInfo': {'hasNextPage': False}, 'nodes': [
                {'remainingQuantity': 2, 'lineItem': {'variant': {'id': 'variant'}, 'sku': 'BOOK-X'}},
            ],
        }},
    ]}}
    skus = {}
    assert mapped_quantities(Mock(), install, source, outbound, outbound=True, sku_by_edition=skus) == {str(edition): 2}
    assert mapped_quantities(Mock(), install, source, inventory) == {str(edition): 2}
    assert skus == {str(edition): 'BOOK-X'}


def test_outbound_sftp_file_contains_original_shopify_name_and_sku(monkeypatch):
    from app.distribution import transports

    client = Mock()
    upload = Mock()
    monkeypatch.setattr(transports, 'sftp', lambda *args: nullcontext(client))
    monkeypatch.setattr(transports, 'sftp_upload', upload)
    adapter = HachetteExporteoAdapter({
        'orders': {'transport': 'sftp', 'directory': '/queued', 'host': 'sftp.example.com'},
        'shipping_methods': {'Ground': 'PG'},
    }, {'username': 'test', 'password': 'fake'})
    order_id, edition_id = uuid4(), uuid4()
    order = {'id': order_id, 'reference': '#991234', 'shipping_method': 'Ground',
             'export_skus': {str(edition_id): 'BOOK-X'}, 'recipient': {
                 'name': 'Test Reader', 'address_1': 'Example', 'city': 'Boston',
                 'state_region': 'MA', 'postal_code': '02110', 'country': 'US', 'phone': '5550000000',
             }}
    assert adapter.submit_order(order, [{'edition_id': edition_id, 'isbn': '9780000000002', 'quantity': 2}]).status == 'SUBMITTED'
    assert upload.call_args.args[:3] == (client, '/queued', f'inksuite-{order_id}.txt')
    header, line, _ = upload.call_args.args[3].decode().split('\r\n')
    assert header.split('\t')[:3] == ['HDR', '#991234', '#991234']
    assert line == 'DTL\t#991234\t1\tBOOK-X\t2'
