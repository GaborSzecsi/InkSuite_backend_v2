"""Publisher-admin shipment setup and previews. No fulfillment mutations."""
import base64
import binascii
from datetime import datetime, timezone
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
            WHERE tenant_id=%s AND id=%s AND active''', (tenant, connection['shopify_installation_id']))
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
        def download(client, settings):
            if body.filename not in file_names(client, settings):
                return None
            return read_file(client, settings, body.filename)

        content = remote_feed(tenant, feed, download)
        if content is None:
            raise HTTPException(404, 'The file is outside the configured shipment listing.')
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
            annotate_preview(cur, tenant, connection, result['rows'])
        else:
            local_matches(cur, tenant, connection, config, result['rows'])
    return result


@router.get(FEED + '/shopify-permissions')
def shopify_permissions(connection_id: UUID, ctx=Depends(admin)):
    from . import shopify

    tenant, _ = ids(ctx)
    with db.transaction() as cur:
        connection = db.connection(cur, tenant, connection_id)
        install = db.one(cur, """SELECT * FROM distribution_shopify_installations
            WHERE tenant_id=%s AND id=%s AND active FOR UPDATE""",
            (tenant, connection['shopify_installation_id']))
        if not install:
            raise HTTPException(409, 'Link the distributor to an active Shopify store first.')
        result = shopify.graphql(cur, install, '''query ShipmentAppPermissions {
            currentAppInstallation { accessScopes { handle } }
        }''')
    scopes = {entry['handle'] for entry in result['currentAppInstallation']['accessScopes']}
    required = 'write_merchant_managed_fulfillment_orders'
    return {'shop': install['shop'], 'scope': required, 'merchant_managed_write_granted': required in scopes}
