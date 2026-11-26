"""Publisher-admin shipment setup and previews. No fulfillment mutations."""
import base64
import binascii
from datetime import datetime, timezone
from typing import Literal
from uuid import UUID

from fastapi import Depends, HTTPException, Request
from pydantic import ConfigDict, Field, SecretStr, ValidationError
from psycopg.types.json import Jsonb

from . import secrets, service as db
from .models import CredentialsIn, StrictModel
from .router import admin, ids, router
from .shipment_files import ShipmentFeed, matching_files, preview, read_file
from .transports import MAX_BYTES, TransportError, sftp

FEED = '/connections/{connection_id}/shipment'
MAX_SAMPLE = 1024 * 1024


class ShipmentCredentials(CredentialsIn):
    model_config = ConfigDict(str_strip_whitespace=False)
    password: SecretStr = Field(min_length=1, max_length=4096)


class PreviewIn(StrictModel):
    lookup_shopify: bool = False
    filename: str | None = Field(default=None, min_length=1, max_length=255)
    sample_base64: str | None = Field(default=None, min_length=1, max_length=4 * ((MAX_SAMPLE + 2) // 3))


def load_feed(cur, tenant, connection_id, *, required=True, lock=False):
    connection = db.connection(cur, tenant, connection_id, lock=lock)
    if not db.one(cur, "SELECT to_regclass('public.distribution_shipment_feeds') AS ready")['ready']:
        raise HTTPException(503, 'Shipment confirmation awaits migration 048.')
    feed = db.one(cur, '''SELECT * FROM distribution_shipment_feeds
        WHERE tenant_id=%s AND connection_id=%s''', (tenant, connection_id))
    if required and not feed:
        raise HTTPException(409, 'Save the shipment feed configuration first.')
    return connection, feed


def public_feed(feed):
    return {'configuration': feed['configuration'] if feed else None,
            'credentials_saved': bool(feed and feed['credentials_saved']), 'mode': 'preview'}


@router.get(FEED)
def get_feed(connection_id: UUID, ctx=Depends(admin)):
    tenant, _ = ids(ctx)
    with db.transaction() as cur:
        _, feed = load_feed(cur, tenant, connection_id, required=False)
        return public_feed(feed)


@router.put(FEED)
def save_feed(connection_id: UUID, body: ShipmentFeed, ctx=Depends(admin)):
    tenant, actor = ids(ctx)
    configuration = body.model_dump(mode='json')
    with db.transaction() as cur:
        _, previous = load_feed(cur, tenant, connection_id, required=False, lock=True)
        if body.shopify_installation_id and not db.one(cur, """SELECT id FROM distribution_shopify_installations
            WHERE tenant_id=%s AND id=%s AND active""", (tenant, body.shopify_installation_id)):
            raise HTTPException(422, 'Select an active Shopify store linked to this publisher.')
        if previous and previous.get('automation_enabled') and previous['configuration'] != configuration:
            raise HTTPException(409, 'Pause shipment automation before changing the feed configuration.')
        identity = ('host', 'port', 'host_key')
        changed_server = previous and any(previous['configuration'][key] != configuration[key] for key in identity)
        feed = db.one(cur, '''INSERT INTO distribution_shipment_feeds AS feed
            (tenant_id, connection_id, configuration) VALUES (%s,%s,%s)
            ON CONFLICT (tenant_id, connection_id) DO UPDATE
            SET configuration=EXCLUDED.configuration, updated_at=now(),
                credentials_saved=feed.credentials_saved AND NOT %s,
                credential_id=CASE WHEN %s THEN gen_random_uuid() ELSE feed.credential_id END
            RETURNING *''', (tenant, connection_id, Jsonb(configuration), bool(changed_server), bool(changed_server)))
        db.event(cur, tenant, connection_id, actor, 'SHIPMENT_CONFIGURATION_CHANGED')
        return public_feed(feed)


@router.put(FEED + '/credentials')
async def save_feed_credentials(connection_id: UUID, request: Request, ctx=Depends(admin)):
    from starlette.concurrency import run_in_threadpool

    raw = bytearray()
    async for chunk in request.stream():
        if len(raw) + len(chunk) > 32768:
            raise HTTPException(413, 'Credential request exceeds the size limit.')
        raw.extend(chunk)
    try:
        body = ShipmentCredentials.model_validate_json(raw)
        if not body.username.strip():
            raise ValueError()
    except (ValidationError, ValueError):
        raise HTTPException(422, 'Enter a valid shipment username and password.') from None
    return await run_in_threadpool(store_credentials, connection_id, body, ctx)


def store_credentials(connection_id, body, ctx):
    tenant, actor = ids(ctx)
    with db.transaction() as cur:
        _, feed = load_feed(cur, tenant, connection_id, lock=True)
        if not feed['configuration']['host_key']:
            raise HTTPException(409, 'Save an independently verified SSH host key before storing credentials.')
        try:
            secrets.save(tenant, feed['credential_id'], {'username': body.username.strip(), 'password': body.password.get_secret_value()})
        except Exception:
            raise HTTPException(503, 'Shipment credentials could not be stored securely.') from None
        cur.execute('''UPDATE distribution_shipment_feeds SET credentials_saved=true, updated_at=now()
            WHERE tenant_id=%s AND connection_id=%s''', (tenant, connection_id))
        db.event(cur, tenant, connection_id, actor, 'SHIPMENT_CREDENTIALS_UPDATED')
    return {'credentials_saved': True}


def remote_feed(tenant, feed, operation):
    if not feed['credentials_saved']:
        raise HTTPException(409, 'Save credentials for this shipment server first.')
    config = ShipmentFeed.model_validate(feed['configuration'])
    try:
        auth = secrets.read(secrets.reference(tenant, feed['credential_id']))
    except Exception:
        raise HTTPException(503, 'Shipment credentials could not be read securely.') from None
    try:
        with sftp(config.model_dump(), auth) as client:
            return operation(client, config)
    except (TransportError, OSError, ValueError):
        raise HTTPException(502, 'Shipment SFTP access failed. Check the allowed host, verified host key, credentials, and directory.') from None


def file_names(client, config):
    return matching_files(client, config, datetime.now(timezone.utc).date())


@router.post(FEED + '/test')
def test_feed(connection_id: UUID, ctx=Depends(admin)):
    tenant, _ = ids(ctx)
    with db.transaction() as cur:
        _, feed = load_feed(cur, tenant, connection_id)

    def test_access(client, config):
        names = file_names(client, config)
        if names:
            read_file(client, config, names[-1])
        return {'directory_readable': True, 'file_read_verified': bool(names)}

    return remote_feed(tenant, feed, test_access)


@router.get(FEED + '/files')
def discover_files(connection_id: UUID, ctx=Depends(admin)):
    tenant, _ = ids(ctx)
    with db.transaction() as cur:
        _, feed = load_feed(cur, tenant, connection_id)
    return {'files': remote_feed(tenant, feed, file_names)}


def local_matches(cur, tenant, connection, config, rows):
    if config.order_identifier == 'marketplace_reference':
        source, account, field = 'MARKETPLACE', None, 'reference'
    else:
        install = db.one(cur, '''SELECT shop, location_id FROM distribution_shopify_installations
            WHERE tenant_id=%s AND id=%s AND active''', (tenant, config.shopify_installation_id or connection['shopify_installation_id']))
        source = 'SHOPIFY'
        account = install['shop'] if install else None
        field = 'external_order_id' if config.order_identifier == 'shopify_id' else 'reference'
        if not account:
            for row in rows:
                if row['status'] == 'parsed':
                    row['match_status'] = 'store_not_linked'
            return
    references = {row['fields']['order_reference'] for row in rows if row['status'] == 'parsed'}
    if not references:
        return
    cur.execute(f'''SELECT id, status, {field} AS reference FROM distribution_orders
        WHERE tenant_id=%s AND connection_id=%s AND source=%s
        AND (%s::text IS NULL OR source_account=%s)
        AND {field}=ANY(%s)''', (tenant, connection['id'], source, account, account, sorted(references)))
    matches = {}
    for order in cur.fetchall():
        matches.setdefault(order['reference'], []).append(order)
    for row in rows:
        if row['status'] != 'parsed':
            continue
        orders = matches.get(row['fields']['order_reference'], [])
        row['match_status'] = {0: 'unmatched', 1: 'matched'}.get(len(orders), 'ambiguous')
        if len(orders) == 1:
            row['order_status'] = orders[0]['status']
            row['configured_location_id'] = install['location_id'] if source == 'SHOPIFY' else None


def download_listed_file(tenant, feed, filename):
    def download(client, settings):
        if filename not in file_names(client, settings):
            return None
        return read_file(client, settings, filename)

    content = remote_feed(tenant, feed, download)
    if content is None:
        raise HTTPException(404, 'The file is outside the configured shipment listing.')
    return content

@router.post(FEED + '/preview')
def preview_feed(connection_id: UUID, body: PreviewIn, ctx=Depends(admin)):
    tenant, _ = ids(ctx)
    if (body.filename is None) == (body.sample_base64 is None):
        raise HTTPException(400, 'Select one remote file or upload one local sample.')
    with db.transaction() as cur:
        _, feed = load_feed(cur, tenant, connection_id)
    config = ShipmentFeed.model_validate(feed['configuration'])
    if body.sample_base64 is not None:
        try:
            content = base64.b64decode(body.sample_base64, validate=True)
        except (binascii.Error, ValueError):
            raise HTTPException(400, 'Invalid local sample encoding.') from None
        if len(content) > MAX_SAMPLE:
            raise HTTPException(413, 'Local samples must be 1 MiB or smaller.')
    else:
        content = download_listed_file(tenant, feed, body.filename)
    try:
        result = preview(content, config)
    except ValueError as error:
        raise HTTPException(422, str(error)) from None
    with db.transaction() as cur:
        connection = db.connection(cur, tenant, connection_id)
        if body.lookup_shopify:
            if config.order_identifier != 'shopify_name':
                raise HTTPException(422, 'Shopify lookup requires the Shopify order name identifier.')
            from .shipment_shopify import annotate_preview
            installation_id = config.shopify_installation_id or connection['shopify_installation_id']
            annotate_preview(cur, tenant, installation_id, result['rows'])
        else:
            local_matches(cur, tenant, connection, config, result['rows'])
    return result | {'remote_filename': body.filename}


@router.get(FEED + '/shopify-permissions')
def shopify_permissions(connection_id: UUID, ctx=Depends(admin)):
    from . import shopify

    tenant, _ = ids(ctx)
    with db.transaction() as cur:
        connection, feed = load_feed(cur, tenant, connection_id, required=False)
        selected = feed['configuration'].get('shopify_installation_id') if feed else None
        install = db.one(cur, """SELECT * FROM distribution_shopify_installations
            WHERE tenant_id=%s AND id=%s AND active FOR UPDATE""",
            (tenant, selected or connection['shopify_installation_id']))
        if not install:
            raise HTTPException(409, 'Select an active Shopify store in Shipment Confirmation and save the configuration first.')
        result = shopify.graphql(cur, install, '''query ShipmentAppPermissions {
            currentAppInstallation { accessScopes { handle } }
        }''')
    scopes = {entry['handle'] for entry in result['currentAppInstallation']['accessScopes']}
    required = 'write_merchant_managed_fulfillment_orders'
    return {'shop': install['shop'], 'scope': required, 'merchant_managed_write_granted': required in scopes}


class FulfillIn(StrictModel):
    filename: str = Field(min_length=1, max_length=255)
    file_hash: str = Field(pattern=r'^[a-f0-9]{64}$')
    row: int = Field(gt=0)
    order_reference: str = Field(min_length=1, max_length=500)
    tracking_number: str = Field(min_length=1, max_length=500)
    installation_id: UUID
    location_id: str = Field(pattern=r'^gid://shopify/Location/[0-9]+$')
    confirm_complete_shipment: Literal[True]


def confirmed_shipment(content, config, body):
    try:
        result = preview(content, config)
    except ValueError:
        raise HTTPException(422, 'The shipment file could not be parsed.') from None
    if result['file_hash'] != body.file_hash:
        raise HTTPException(409, 'The shipment file changed. Preview it again.')
    if result['truncated'] or any(row['status'] == 'invalid' for row in result['rows']):
        raise HTTPException(409, 'The shipment file is incomplete or contains invalid rows and requires review.')
    match = next((row for row in result['rows'] if row['row'] == body.row and row['status'] == 'parsed'), None)
    if not match:
        raise HTTPException(409, 'The shipment row is skipped, invalid, or duplicated.')
    fields = match['fields']
    if (fields['order_reference'], fields['tracking_number']) != (body.order_reference, body.tracking_number):
        raise HTTPException(409, 'The shipment mapping changed. Preview the file again.')
    if not fields['carrier'] or fields.get('sku') or fields.get('quantity'):
        raise HTTPException(409, 'Only complete location shipments with a carrier and no item-level rows are supported.')
    related = [row['fields'] for row in result['rows']
               if row['status'] == 'parsed' and row['fields']['order_reference'] == fields['order_reference']]
    if len(related) != 1:
        raise HTTPException(409, 'This order has multiple shipment rows and requires review.')
    return fields


@router.post(FEED + '/fulfill')
def fulfill_shipment(connection_id: UUID, body: FulfillIn, ctx=Depends(admin)):
    from .shipment_fulfillment import post_full_shipment

    tenant, actor = ids(ctx)
    with db.transaction() as cur:
        connection, feed = load_feed(cur, tenant, connection_id)
    if connection['adapter'] == 'simulator':
        raise HTTPException(409, 'Simulator connections cannot post live fulfillments.')
    config = ShipmentFeed.model_validate(feed['configuration'])
    if config.order_identifier != 'shopify_name' or config.shopify_installation_id != body.installation_id:
        raise HTTPException(409, 'Save an explicit Shopify store on this shipment feed and preview the file again.')
    fields = confirmed_shipment(download_listed_file(tenant, feed, body.filename), config, body)

    def check_configuration(cur):
        _, current = load_feed(cur, tenant, connection_id, lock=True)
        if current['updated_at'] != feed['updated_at']:
            raise HTTPException(409, 'The shipment configuration changed. Preview the file again.')

    return post_full_shipment(tenant, actor, connection_id, body.installation_id, body.location_id,
                              fields, check_configuration)


class AutomationIn(StrictModel):
    enabled: bool
    interval_minutes: int = Field(default=15, ge=5, le=1440)
    confirm_complete_shipments: bool = False


def automation_status(feed):
    return {
        'enabled': bool(feed['automation_enabled']),
        'interval_minutes': feed['automation_interval_minutes'],
        'last_run_at': feed['automation_last_run_at'],
        'result': feed['automation_result'],
    }


@router.get(FEED + '/automation')
def get_automation(connection_id: UUID, ctx=Depends(admin)):
    from .shipment_automation import ready
    tenant, _ = ids(ctx)
    with db.transaction() as cur:
        _, feed = load_feed(cur, tenant, connection_id, required=False)
        if not ready(cur):
            return {'available': False}
        return {'available': True, **(automation_status(feed) if feed else {'enabled': False, 'interval_minutes': 15})}


@router.put(FEED + '/automation')
def set_automation(connection_id: UUID, body: AutomationIn, ctx=Depends(admin)):
    from . import shopify
    from .shipment_automation import ready
    from .shipment_fulfillment import SCOPES

    tenant, actor = ids(ctx)
    with db.transaction() as cur:
        connection, feed = load_feed(cur, tenant, connection_id, lock=True)
        if not ready(cur):
            raise HTTPException(503, 'Shipment automation awaits migration 050.')
        location_id = None
        if body.enabled:
            if feed['automation_enabled']:
                raise HTTPException(409, 'Pause shipment automation before enabling it again.')
            if not body.confirm_complete_shipments:
                raise HTTPException(422, 'Confirm the feed identifies complete shipments for the configured warehouse.')
            config = ShipmentFeed.model_validate(feed['configuration'])
            if (connection['adapter'] == 'simulator' or not feed['credentials_saved']
                    or not config.shopify_installation_id or config.order_identifier != 'shopify_name'):
                raise HTTPException(409, 'Select a live distributor, save shipment credentials, and select the Shopify store and order name.')
            install = db.one(cur, """SELECT * FROM distribution_shopify_installations
                WHERE tenant_id=%s AND id=%s AND active FOR UPDATE""", (tenant, config.shopify_installation_id))
            if not install or not install['location_id']:
                raise HTTPException(409, 'Configure the active Shopify store’s fulfillment location first.')
            location_id = install['location_id']
            scopes = shopify.graphql(cur, install, SCOPES)['currentAppInstallation']['accessScopes']
            if not any(scope['handle'] == 'write_merchant_managed_fulfillment_orders' for scope in scopes):
                raise HTTPException(403, 'The Shopify store lacks merchant-managed fulfillment write access.')
        feed = db.one(cur, """UPDATE distribution_shipment_feeds
            SET automation_enabled=%s, automation_interval_minutes=%s, automation_actor=%s, automation_location_id=%s,
                automation_due_at=CASE WHEN %s THEN now() END, automation_lease=NULL
            WHERE tenant_id=%s AND connection_id=%s RETURNING *""",
            (body.enabled, body.interval_minutes, actor, location_id, body.enabled, tenant, connection_id))
        db.event(cur, tenant, connection_id, actor,
                 'SHIPMENT_AUTOMATION_ENABLED' if body.enabled else 'SHIPMENT_AUTOMATION_PAUSED')
    return {'available': True, **automation_status(feed)}
