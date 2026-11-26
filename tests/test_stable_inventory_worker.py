from contextlib import nullcontext
from datetime import datetime, timedelta, timezone
from unittest.mock import Mock

from app.stable import worker


def test_inventory_only_skips_statement_files_and_posts_only_inventory(monkeypatch):
    cur = Mock()
    cur.fetchone.return_value = {'locked': True}
    cur.fetchall.side_effect = [[], [{'id': 'inventory-import'}]]
    conn = Mock()
    conn.cursor.return_value = nullcontext(cur)
    conn.transaction.side_effect = nullcontext
    monkeypatch.setattr(worker, 'db_conn', lambda: nullcontext(conn))
    post = Mock()
    monkeypatch.setattr(worker, 'post', post)
    s3 = Mock()
    s3.get_paginator.return_value.paginate.return_value = [{'Contents': [{
        'Key': 'incoming/monthly_client_statement.xlsx', 'Size': 100,
        'LastModified': datetime.now(timezone.utc) - timedelta(hours=1),
    }]}]
    worker.scan(s3, {'tenant_id': 'tenant', 'bucket': 'bucket', 'key_prefix': 'incoming/'}, inventory_only=True)
    s3.get_object.assert_not_called()
    statements = [call.args for call in cur.execute.call_args_list]
    assert not any('Multiple statement revisions' in query for query, *_ in statements)
    posting_query, parameters = next((query, params) for query, params in statements if 'ORDER BY as_of' in query)
    assert "report_type='inventory'" in posting_query
    assert parameters == ('tenant', True)
    post.assert_called_once_with(cur, 'tenant', 'inventory-import')
