"""Authenticated upload sessions; only file parts travel from the browser to S3."""
import logging
import mimetypes
import os
import tempfile
from datetime import datetime, timedelta, timezone
from functools import lru_cache

import boto3
from botocore.config import Config
from fastapi import HTTPException

from . import assets, service as s, storage
from .domain import tenant_root, upload_key

PART_SIZE = 8 * 1024 * 1024
log = logging.getLogger(__name__)


@lru_cache(maxsize=1)
def client():
    return boto3.client('s3', region_name=os.getenv('AWS_REGION') or 'us-east-2',
                        config=Config(signature_version='s3v4', retries={'mode': 'standard', 'max_attempts': 3}))


def initiate(cur, ctx, body):
    mime = mimetypes.guess_type(body.filename)[0] or ''
    if mime not in ('image/jpeg', 'image/png', 'image/webp', 'video/mp4', 'video/quicktime', 'video/webm'):
        raise HTTPException(422, 'Use a JPEG, PNG, WebP, MP4, MOV or WebM file.')
    if mime.startswith('image/') and body.filesize_bytes > 25 * 1024 * 1024:
        raise HTTPException(422, 'Image exceeds 25 MB.')
    if body.campaign_id:
        s.owned(cur, 'marketing_campaigns', body.campaign_id, ctx['tenant']['id'])
    storage.require_space(cur, ctx, body.filesize_bytes)
    identity, asset_id = s.uid(), s.uid()
    key = upload_key(ctx['tenant']['slug'], str(asset_id), body.filename,
                     str(body.campaign_id) if body.campaign_id else None, body.category)
    staging = 'marketing-upload-staging/' + tenant_root(ctx['tenant']['slug']) + str(identity)
    expires = datetime.now(timezone.utc) + timedelta(hours=2)
    b = assets.bucket()
    upload_id = client().create_multipart_upload(
        Bucket=b, Key=staging, ContentType='application/octet-stream',
        Metadata={'upload-session': str(identity)})['UploadId']
    try:
        cur.execute('''INSERT INTO marketing_upload_sessions
            (id,tenant_id,user_id,campaign_id,asset_id,filename,filesize_bytes,s3_bucket,
             staging_key,final_key,upload_id,part_size,expires_at)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)''',
            (identity, ctx['tenant']['id'], ctx['user']['id'], body.campaign_id, asset_id,
             body.filename, body.filesize_bytes, b, staging, key, upload_id, PART_SIZE, expires))
    except Exception:
        client().abort_multipart_upload(Bucket=b, Key=staging, UploadId=upload_id)
        raise
    return {'id': str(identity), 'part_size': PART_SIZE,
            'part_count': (body.filesize_bytes + PART_SIZE - 1) // PART_SIZE, 'expires_at': expires}


def session(cur, ctx, identity):
    row = s.one(cur, '''SELECT * FROM marketing_upload_sessions
        WHERE id=%s AND tenant_id=%s AND user_id=%s FOR UPDATE''',
        (identity, ctx['tenant']['id'], ctx['user']['id']))
    if not row:
        raise HTTPException(404, 'Upload session not found.')
    return row


def active(row):
    if row['status'] != 'pending':
        raise HTTPException(409, 'This upload session is no longer active.')
    if row['expires_at'] <= datetime.now(timezone.utc):
        raise HTTPException(410, 'Upload session expired. Choose the file again.')


def arguments(row):
    return {'Bucket': row['s3_bucket'], 'Key': row['staging_key'], 'UploadId': row['upload_id']}


def part_length(row, number):
    count = (row['filesize_bytes'] + row['part_size'] - 1) // row['part_size']
    if not 1 <= number <= count:
        raise HTTPException(422, 'Invalid upload part number.')
    return min(row['part_size'], row['filesize_bytes'] - (number - 1) * row['part_size'])


def sign_part(cur, ctx, identity, number):
    row = session(cur, ctx, identity)
    active(row)
    length = part_length(row, number)
    seconds = min(900, max(1, int((row['expires_at'] - datetime.now(timezone.utc)).total_seconds())))
    # Content-Length is signed; the browser supplies it automatically for the Blob.
    url = client().generate_presigned_url('upload_part', Params={**arguments(row),
        'PartNumber': number, 'ContentLength': length}, ExpiresIn=seconds)
    return {'url': url}


def object_head(row):
    try:
        return client().head_object(Bucket=row['s3_bucket'], Key=row['staging_key'])
    except client().exceptions.ClientError as error:
        # HeadObject uses a generic 404 rather than a modeled NoSuchKey exception.
        if error.response.get('ResponseMetadata', {}).get('HTTPStatusCode') != 404:
            raise
        return None


def assembled_object(row):
    # A previous request may have completed S3 but lost its DB transaction/response.
    head = object_head(row)
    if head is None:
        try:
            parts = [part for page in client().get_paginator('list_parts').paginate(**arguments(row))
                     for part in page.get('Parts', [])]
        except client().exceptions.NoSuchUpload:
            raise HTTPException(409, 'Upload is missing. Choose the file again.') from None
        expected = (row['filesize_bytes'] + row['part_size'] - 1) // row['part_size']
        if len(parts) != expected or any(
            part['PartNumber'] != number or part['Size'] != part_length(row, number)
            for number, part in enumerate(parts, 1)
        ):
            raise HTTPException(422, 'Upload is incomplete or its size does not match the selected file.')
        client().complete_multipart_upload(**arguments(row), MultipartUpload={
            'Parts': [{'PartNumber': p['PartNumber'], 'ETag': p['ETag']} for p in parts]})
        head = object_head(row)
    if not head or head['ContentLength'] != row['filesize_bytes'] or head.get('Metadata', {}).get('upload-session') != str(row['id']):
        raise HTTPException(422, 'Uploaded file does not match this session.')
    return head


def complete(cur, ctx, identity):
    # Match the lock order used by all Marketing writes and cleanup.
    storage.lock(cur, ctx)
    row = session(cur, ctx, identity)
    if row['status'] == 'completed':
        return s.owned(cur, 'social_media_assets', row['asset_id'], ctx['tenant']['id'])
    active(row)
    if row['campaign_id']:
        s.owned(cur, 'marketing_campaigns', row['campaign_id'], ctx['tenant']['id'])
    head = assembled_object(row)
    # Bound the download even if an external writer changes the staging object.
    with tempfile.TemporaryDirectory(prefix='inksuite-upload-') as folder:
        path = os.path.join(folder, 'source.media')
        response = client().get_object(Bucket=row['s3_bucket'], Key=row['staging_key'], IfMatch=head['ETag'])
        stream = response['Body']
        try:
            total = 0
            with open(path, 'wb') as output:
                while chunk := stream.read(1024 * 1024):
                    total += len(chunk)
                    if total > row['filesize_bytes']:
                        raise HTTPException(422, 'Uploaded file exceeds its declared size.')
                    output.write(chunk)
        finally:
            stream.close()
        if total != row['filesize_bytes']:
            raise HTTPException(422, 'Uploaded file is incomplete.')
        mime, media_type, width, height, duration = assets.inspect_media_file(path, row['filename'])
    storage.require_space(cur, ctx, total)
    # Publish only inspected media. The browser never receives a URL for this key.
    client().copy_object(Bucket=row['s3_bucket'], Key=row['final_key'],
        CopySource={'Bucket': row['s3_bucket'], 'Key': row['staging_key']},
        CopySourceIfMatch=head['ETag'], MetadataDirective='REPLACE', ContentType=mime,
        TaggingDirective='REPLACE', Tagging='')
    result = s.one(cur, '''INSERT INTO social_media_assets
        (id,tenant_id,campaign_id,source_type,s3_bucket,s3_key,public_url,filename,mime_type,
         media_type,width,height,duration_seconds,filesize_bytes,created_by)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING *''',
        (row['asset_id'], row['tenant_id'], row['campaign_id'],
         'campaign_asset' if row['campaign_id'] else 'publisher_asset', row['s3_bucket'],
         row['final_key'], assets.public_url(row['final_key']), row['filename'], mime,
         media_type, width, height, duration, total, ctx['user']['id']))
    cur.execute("UPDATE marketing_upload_sessions SET status='completed' WHERE id=%s", (row['id'],))
    # Keep staging until after commit: a lost transaction can be retried safely.
    # The cleanup command removes it after expiration, plus any unregistered copy.
    return result


def discard(row):
    try:
        client().abort_multipart_upload(**arguments(row))
    except client().exceptions.NoSuchUpload:
        pass  # It may already be assembled, aborted or expired by S3 lifecycle.
    client().delete_object(Bucket=row['s3_bucket'], Key=row['staging_key'])


def abort(cur, ctx, identity):
    storage.lock(cur, ctx)
    row = session(cur, ctx, identity)
    if row['status'] == 'completed':
        return {'ok': True}  # A late cancellation must never delete a registered asset.
    discard(row)
    client().delete_object(Bucket=row['s3_bucket'], Key=row['final_key'])
    cur.execute("UPDATE marketing_upload_sessions SET status='aborted' WHERE id=%s", (row['id'],))
    return {'ok': True}


def cleanup():
    """Run hourly with the backend environment: python -m app.marketing.uploads."""
    from app.core.db import db_conn
    from psycopg.rows import dict_row
    with db_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
        failures = 0
        expired = s.rows(cur, 'SELECT id,tenant_id FROM marketing_upload_sessions WHERE expires_at < now()')
        for item in expired:
            try:
                with conn.transaction():
                    cur.execute('SELECT id FROM tenants WHERE id=%s FOR UPDATE', (item['tenant_id'],))
                    row = s.one(cur, 'SELECT * FROM marketing_upload_sessions WHERE id=%s FOR UPDATE', (item['id'],))
                    if not row:
                        continue
                    discard(row)
                    if row['status'] != 'completed':
                        client().delete_object(Bucket=row['s3_bucket'], Key=row['final_key'])
                    cur.execute('DELETE FROM marketing_upload_sessions WHERE id=%s', (row['id'],))
            except Exception:
                failures += 1
                log.exception('Could not clean upload session %s', item['id'])
        if failures:
            raise RuntimeError(f'{failures} upload session(s) could not be cleaned.')


if __name__ == '__main__':
    cleanup()
