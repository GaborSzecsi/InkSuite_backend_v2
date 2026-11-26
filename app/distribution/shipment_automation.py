"""Scheduled import of complete shipments from a distributor's SFTP feed."""
import hashlib
from collections import Counter
from uuid import uuid4

from fastapi import HTTPException
from psycopg.types.json import Jsonb

from . import service as db
from .shipment_files import ShipmentFeed, preview, read_file
from .shipment_fulfillment import post_full_shipment
from .shipment_routes import file_names, load_feed, remote_feed

BATCH_SIZE = 10
SCAN_ROWS = 10000


def ready(cur):
    return bool(db.one(cur, "SELECT to_regclass('public.distribution_shipment_automation_rows') AS ready")['ready'])


def claim(cur):
    if not ready(cur):
        return None
    return db.one(cur, """WITH due AS (
        SELECT f.tenant_id, f.connection_id FROM distribution_shipment_feeds f
        JOIN distribution_connections c ON c.tenant_id=f.tenant_id AND c.id=f.connection_id
        WHERE f.automation_enabled AND f.automation_due_at<=now()
          AND c.archived_at IS NULL AND c.adapter<>'simulator'
        ORDER BY f.automation_due_at, f.connection_id FOR UPDATE OF f SKIP LOCKED LIMIT 1
    )
    UPDATE distribution_shipment_feeds f SET automation_lease=%s,
        automation_due_at=now()+interval '30 minutes'
    FROM due WHERE f.tenant_id=due.tenant_id AND f.connection_id=due.connection_id
    RETURNING f.*""", (uuid4(),))


def eligible_rows(result):
    if result['truncated'] or any(row['status'] == 'invalid' for row in result['rows']):
        raise HTTPException(422, 'Shipment file is incomplete or contains an invalid row.')
    rows = [row for row in result['rows'] if row['status'] == 'parsed']
    totals = Counter(row['fields']['order_reference'] for row in rows)
    for row in rows:
        fields = row['fields']
        valid = (totals[fields['order_reference']] == 1 and bool(fields['carrier'])
                 and not fields.get('sku') and not fields.get('quantity'))
        yield row, valid


def process_feed(feed, counts):
    tenant, connection_id = feed['tenant_id'], feed['connection_id']
    config = ShipmentFeed.model_validate(feed['configuration'])
    installation_id = config.shopify_installation_id
    if not installation_id or config.order_identifier != 'shopify_name':
        raise HTTPException(409, 'Select a Shopify store and use the Shopify order name.')
    with db.transaction() as cur:
        install = db.one(cur, """SELECT location_id FROM distribution_shopify_installations
            WHERE tenant_id=%s AND id=%s AND active""", (tenant, installation_id))
    if not install or install['location_id'] != feed['automation_location_id']:
        raise HTTPException(409, 'The Shopify store or fulfillment location changed after activation.')

    def check_configuration(cur):
        _, current = load_feed(cur, tenant, connection_id, lock=True)
        if (not current['automation_enabled'] or current['automation_lease'] != feed['automation_lease']
                or current['updated_at'] != feed['updated_at']):
            raise HTTPException(409, 'The shipment automation settings changed.')

    names = remote_feed(tenant, feed, file_names)
    counts['files'] = len(names)
    for filename in names:
        data = remote_feed(tenant, feed, lambda client, settings: read_file(client, settings, filename))
        result = preview(data, config, limit=SCAN_ROWS)
        counts['skipped'] += sum(row['status'] == 'skipped' for row in result['rows'])
        try:
            rows = list(eligible_rows(result))
        except HTTPException:
            counts['invalid_files'] += 1
            continue
        for row, valid in rows:
            key = hashlib.sha256(f"{feed['updated_at']}:{result['file_hash']}:{row['row']}".encode()).hexdigest()
            with db.transaction() as cur:
                prior = db.one(cur, """SELECT retry_at IS NULL OR retry_at>now() AS skip
                    FROM distribution_shipment_automation_rows
                    WHERE tenant_id=%s AND connection_id=%s AND row_key=%s""", (tenant, connection_id, key))
            if prior and prior['skip']:
                continue
            if counts['attempted'] == BATCH_SIZE:
                counts['more_rows'] = 1
                return
            counts['attempted'] += 1
            retry = False
            if valid:
                try:
                    outcome = post_full_shipment(
                        tenant, feed['automation_actor'], connection_id, installation_id,
                        install['location_id'], row['fields'], check_configuration,
                    )
                    counts[outcome['status']] += 1
                except HTTPException:
                    counts['review'] += 1
                    retry = True
            else:
                counts['review'] += 1
                retry = True
            with db.transaction() as cur:
                check_configuration(cur)
                cur.execute("""INSERT INTO distribution_shipment_automation_rows
                    (tenant_id, connection_id, row_key, retry_at)
                    VALUES (%s,%s,%s,CASE WHEN %s THEN now()+interval '1 hour' END)
                    ON CONFLICT (tenant_id, connection_id, row_key) DO UPDATE SET retry_at=EXCLUDED.retry_at""",
                    (tenant, connection_id, key, retry))


def run_once():
    with db.transaction() as cur:
        feed = claim(cur)
    if not feed:
        return False
    counts = Counter()
    try:
        process_feed(feed, counts)
    except HTTPException as error:
        counts['run_failed'] = 1
        counts['http_status'] = error.status_code
    except Exception:
        counts['run_failed'] = 1
        counts['internal_error'] = 1
    with db.transaction() as cur:
        cur.execute("""UPDATE distribution_shipment_feeds
            SET automation_lease=NULL, automation_last_run_at=now(), automation_result=%s,
                automation_due_at=now()+automation_interval_minutes*interval '1 minute'
            WHERE tenant_id=%s AND connection_id=%s AND automation_lease=%s""",
            (Jsonb(dict(counts)), feed['tenant_id'], feed['connection_id'], feed['automation_lease']))
    return True
