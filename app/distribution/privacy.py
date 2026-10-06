"""Shopify privacy workflow. Scoped to distribution data, never royalty/accounting data."""
import hashlib
import json
from datetime import datetime, timezone
from uuid import UUID
from fastapi import Depends, HTTPException, Response
from fastapi.encoders import jsonable_encoder
from psycopg.types.json import Jsonb
from . import service as s, secrets
from .router import router, admin, ids

TOPICS = {'customers/data_request', 'customers/redact', 'shop/redact'}


def ready(cur):
    return bool(s.one(cur, "SELECT to_regclass('public.distribution_privacy_requests') AS ready")['ready'])


def require_ready(cur):
    if not ready(cur):
        raise HTTPException(503, 'Privacy controls await migration 021.')


def audit(cur, tenant, actor, action, resource=None):
    require_ready(cur)
    cur.execute('INSERT INTO distribution_access_audit(tenant_id,actor,action,resource_id) VALUES(%s,%s,%s,%s)',
                (tenant, str(actor), action, resource))


def numeric_id(value):
    value = str(value or '').rsplit('/', 1)[-1]
    return value if value.isdigit() else ''


def fingerprint(shop, kind, value):
    return hashlib.sha256((shop + ':' + kind + ':' + str(value)).encode()).hexdigest()


def suppressed(cur, tenant, shop, order_id=''):
    for kind, value in [('order', numeric_id(order_id))]:
        if value and s.one(cur, 'SELECT fingerprint FROM distribution_privacy_suppressions WHERE tenant_id=%s AND fingerprint=%s AND expires_at>now()',
                           (tenant, fingerprint(shop, kind, value))):
            return True
    return False


def suppress(cur, tenant, shop, kind, value):
    if value:
        cur.execute("INSERT INTO distribution_privacy_suppressions(tenant_id,fingerprint) VALUES(%s,%s) ON CONFLICT(tenant_id,fingerprint) DO UPDATE SET expires_at=now()+interval '30 days'",
                    (tenant, fingerprint(shop, kind, value)))


def minimal_payload(topic, payload):
    key = 'orders_requested' if topic == 'customers/data_request' else 'orders_to_redact'
    values = payload.get(key) or []
    if not isinstance(values, list) or len(values) > 10000:
        raise ValueError('Invalid order identifiers')
    orders = [numeric_id(value) for value in values]
    if any(not value for value in orders):
        raise ValueError('Invalid order identifier')
    return {'order_ids': orders}



def receive(cur, install, topic, delivery, payload):
    require_ready(cur)
    minimal = {} if topic == 'shop/redact' else minimal_payload(topic, payload)
    cur.execute('INSERT INTO distribution_privacy_requests(tenant_id,installation_id,shop,delivery_id,topic,payload) VALUES(%s,%s,%s,%s,%s,%s) ON CONFLICT(installation_id,delivery_id) DO NOTHING',
                (install['tenant_id'], install['id'], install['shop'], delivery, topic, Jsonb(minimal)))


def matching_orders(cur, request):
    cur.execute("SELECT * FROM distribution_orders WHERE tenant_id=%s AND source='SHOPIFY' AND source_account=%s ORDER BY id FOR UPDATE",
                (request['tenant_id'], request['shop']))
    rows = cur.fetchall()
    if request['topic'] == 'shop/redact':
        return rows
    payload = request['payload']; order_ids = set(payload.get('order_ids') or [])
    return [row for row in rows if numeric_id(row['external_order_id']) in order_ids]



def delete_orders(cur, request, rows):
    for row in rows:
        suppress(cur, request['tenant_id'], request['shop'], 'order', numeric_id(row['external_order_id']))
        for table in ('distribution_jobs', 'distribution_events', 'distribution_reservations', 'distribution_order_items'):
            cur.execute(f'DELETE FROM {table} WHERE tenant_id=%s AND order_id=%s', (request['tenant_id'], row['id']))
        cur.execute('DELETE FROM distribution_orders WHERE tenant_id=%s AND id=%s', (request['tenant_id'], row['id']))
        cur.execute('''DELETE FROM distribution_webhooks w USING distribution_shopify_installations i
            WHERE w.tenant_id=%s AND i.tenant_id=w.tenant_id AND i.id=w.installation_id AND i.shop=%s
            AND (w.payload->>'id'=%s OR w.payload->>'admin_graphql_api_id'=%s)''',
            (request['tenant_id'], request['shop'], numeric_id(row['external_order_id']), 'gid://shopify/Order/'+numeric_id(row['external_order_id'])))


def complete(cur, request, status='DONE', code=None):
    cur.execute("UPDATE distribution_privacy_requests SET status=%s,error_code=%s,payload='{}',shop=NULL,completed_at=now() WHERE id=%s",
                (status, code, request['id']))
    audit(cur, request['tenant_id'], 'privacy-worker', 'PRIVACY_' + status, request['id'])


def run_once():
    with s.transaction() as cur:
        require_ready(cur)
        request = s.one(cur, "SELECT * FROM distribution_privacy_requests WHERE status='PENDING' ORDER BY due_at,id FOR UPDATE SKIP LOCKED LIMIT 1")
        if not request:
            return False
        install = s.one(cur, 'SELECT * FROM distribution_shopify_installations WHERE tenant_id=%s AND id=%s FOR UPDATE',
                        (request['tenant_id'], request['installation_id']))
        if request['topic'] != 'shop/redact' and not request['payload'].get('order_ids'):
            cur.execute("UPDATE distribution_privacy_requests SET status='REVIEW_REQUIRED',error_code='ORDER_IDENTIFIERS_REQUIRED' WHERE id=%s", (request['id'],))
            return True
        if request['topic'] == 'customers/data_request':
            cur.execute("UPDATE distribution_privacy_requests SET status='READY' WHERE id=%s", (request['id'],))
            return True
        if request['topic'] == 'shop/redact' and install and install['active']:
            cur.execute("UPDATE distribution_privacy_requests SET status='REVIEW_REQUIRED',error_code='STORE_STILL_ACTIVE' WHERE id=%s", (request['id'],))
            return True
        rows = matching_orders(cur, request)
        # No live fulfillment is enabled in this release. Do not invent legal holds or
        # cancel real shipments when encountering data from a later live deployment.
        if any(row['mode'] != 'preview' for row in rows):
            cur.execute("UPDATE distribution_privacy_requests SET status='REVIEW_REQUIRED',error_code='LIVE_ORDER_RETENTION_REVIEW' WHERE id=%s", (request['id'],))
            return True
        if request['topic'] == 'shop/redact' and install:
            try:
                secrets.remove(install['secret_reference'])
            except Exception:
                cur.execute("UPDATE distribution_privacy_requests SET status='REVIEW_REQUIRED',error_code='TOKEN_DELETION_FAILED' WHERE id=%s", (request['id'],))
                return True
        delete_orders(cur, request, rows)
        for order_id in request['payload'].get('order_ids', []):
            suppress(cur, request['tenant_id'], request['shop'], 'order', order_id)
        if request['topic'] == 'shop/redact':
            for table in ('distribution_shopify_jobs', 'distribution_shopify_variants', 'distribution_webhooks'):
                cur.execute(f'DELETE FROM {table} WHERE tenant_id=%s AND installation_id=%s', (request['tenant_id'], request['installation_id']))
            cur.execute('DELETE FROM distribution_shopify_installations WHERE tenant_id=%s AND id=%s', (request['tenant_id'], request['installation_id']))
            cur.execute('DELETE FROM distribution_oauth_states WHERE tenant_id=%s AND shop=%s', (request['tenant_id'], request['shop']))
            cur.execute("UPDATE distribution_privacy_requests SET payload='{}',shop=NULL,status='DONE',completed_at=now(),error_code=NULL WHERE tenant_id=%s AND installation_id=%s AND id<>%s",
                        (request['tenant_id'], request['installation_id'], request['id']))
        complete(cur, request)
        return True


def retention_once():
    """Bounded batch: preview customer records 30 days; non-payload audit 365 days."""
    with s.transaction() as cur:
        require_ready(cur)
        cur.execute("DELETE FROM distribution_oauth_states WHERE expires_at<now()-interval '1 day'")
        cur.execute('DELETE FROM distribution_privacy_suppressions WHERE expires_at<now()')
        cur.execute("DELETE FROM distribution_access_audit WHERE created_at<now()-interval '365 days'")
        cur.execute("DELETE FROM distribution_privacy_requests WHERE completed_at<now()-interval '365 days'")
        cur.execute("DELETE FROM distribution_webhooks WHERE created_at<now()-interval '30 days'")
        cur.execute("DELETE FROM distribution_shopify_jobs WHERE status IN ('DONE','FAILED') AND updated_at<now()-interval '30 days'")
        cur.execute("""SELECT o.* FROM distribution_orders o WHERE o.source='SHOPIFY' AND o.mode='preview'
            AND o.created_at<now()-interval '30 days' AND NOT EXISTS (
            SELECT 1 FROM distribution_privacy_requests p WHERE p.tenant_id=o.tenant_id AND p.shop=o.source_account
            AND p.created_at>now()-interval '30 days' AND p.topic='customers/data_request' AND p.status IN ('PENDING','READY','REVIEW_REQUIRED'))
            ORDER BY o.id LIMIT 100 FOR UPDATE SKIP LOCKED""")
        rows = cur.fetchall()
        for row in rows:
            request = {'tenant_id': row['tenant_id'], 'shop': row['source_account']}
            delete_orders(cur, request, [row])
            audit(cur, row['tenant_id'], 'retention-worker', 'PREVIEW_ORDER_EXPIRED')
        return len(rows)


@router.get('/shopify/privacy')
def requests(ctx=Depends(admin)):
    tenant, actor = ids(ctx)
    with s.transaction() as cur:
        require_ready(cur)
        cur.execute('SELECT id,topic,status,error_code,created_at,due_at,completed_at,shop FROM distribution_privacy_requests WHERE tenant_id=%s ORDER BY created_at DESC LIMIT 200', (tenant,))
        rows = cur.fetchall()
        audit(cur, tenant, actor, 'PRIVACY_QUEUE_READ')
    return {'items': rows}


@router.get('/shopify/privacy/{request_id}/export')
def export(request_id: UUID, ctx=Depends(admin)):
    tenant, actor = ids(ctx)
    with s.transaction() as cur:
        require_ready(cur)
        request = s.one(cur, "SELECT * FROM distribution_privacy_requests WHERE tenant_id=%s AND id=%s AND topic='customers/data_request' AND status='READY' FOR UPDATE", (tenant, request_id))
        if not request:
            raise HTTPException(404, 'A ready customer data request was not found.')
        rows = matching_orders(cur, request)
        data = []
        for row in rows:
            cur.execute('SELECT isbn,quantity FROM distribution_order_items WHERE tenant_id=%s AND order_id=%s', (tenant, row['id']))
            row = s.public_order(row)
            data.append({key: row[key] for key in ('reference','external_order_id','shipping_method','status','created_at')} | {'items': cur.fetchall()})
        # Stream only to authenticated merchant admin. Do not persist a second PII export.
        output = json.dumps(jsonable_encoder({'request_id': str(request_id), 'orders': data}))
        audit(cur, tenant, actor, 'CUSTOMER_DATA_EXPORT_DOWNLOADED', request_id)
        # Keep request ready until merchant explicitly confirms delivery to customer.
    return Response(output, media_type='application/json', headers={'Cache-Control':'no-store','Content-Disposition':f'attachment; filename="privacy-{request_id}.json"'})


@router.post('/shopify/privacy/{request_id}/delivered')
def delivered(request_id: UUID, ctx=Depends(admin)):
    tenant, actor = ids(ctx)
    with s.transaction() as cur:
        request = s.one(cur, "SELECT * FROM distribution_privacy_requests WHERE tenant_id=%s AND id=%s AND topic='customers/data_request' AND status='READY' FOR UPDATE", (tenant, request_id))
        if not request:
            raise HTTPException(404, 'Ready request not found.')
        if not s.one(cur, "SELECT id FROM distribution_access_audit WHERE tenant_id=%s AND resource_id=%s AND action='CUSTOMER_DATA_EXPORT_DOWNLOADED'", (tenant, request_id)):
            raise HTTPException(409, 'Download the customer data before confirming delivery.')
        complete(cur, request)
        audit(cur, tenant, actor, 'CUSTOMER_DATA_DELIVERY_CONFIRMED', request_id)
    return {'completed': True}


@router.post('/shopify/privacy/{request_id}/retry')
def retry(request_id: UUID, ctx=Depends(admin)):
    tenant, actor = ids(ctx)
    with s.transaction() as cur:
        row = s.one(cur, "UPDATE distribution_privacy_requests SET status='PENDING',error_code=NULL WHERE tenant_id=%s AND id=%s AND status='REVIEW_REQUIRED' RETURNING id", (tenant, request_id))
        if not row:
            raise HTTPException(404, 'Review request not found.')
        audit(cur, tenant, actor, 'PRIVACY_REQUEST_RETRIED', request_id)
    return {'queued': True}
