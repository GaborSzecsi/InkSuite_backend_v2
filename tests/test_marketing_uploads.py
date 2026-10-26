import io
from datetime import datetime, timedelta, timezone
from unittest.mock import Mock
from uuid import uuid4

import boto3
import pytest
from botocore.config import Config
from botocore.exceptions import ClientError
from fastapi import HTTPException
from PIL import Image
from pydantic import ValidationError

from app.marketing import assets, schemas, uploads as u
from scripts.configure_marketing_uploads import merge


@pytest.fixture
def setup(monkeypatch):
    ctx = {'tenant': {'id': uuid4(), 'slug': 'test-publisher'}, 'user': {'id': uuid4()}}
    row = {'id': uuid4(), 'asset_id': uuid4(), 'tenant_id': ctx['tenant']['id'],
           'user_id': ctx['user']['id'], 'status': 'pending', 'campaign_id': None,
           'expires_at': datetime.now(timezone.utc) + timedelta(hours=1),
           'filesize_bytes': 12, 'part_size': u.PART_SIZE, 'filename': 'image.png',
           's3_bucket': 'bucket', 'staging_key': 'marketing-upload-staging/tenants/test-publisher/stage',
           'final_key': 'tenants/test-publisher/assets/marketing/public/library/general/image.png',
           'upload_id': 'multipart-id'}
    client = Mock()
    client.create_multipart_upload.return_value = {'UploadId': 'multipart-id'}
    client.exceptions.NoSuchUpload = type('NoSuchUpload', (Exception,), {})
    client.exceptions.ClientError = ClientError
    monkeypatch.setattr(u, 'client', lambda: client)
    monkeypatch.setattr(u.s, 'one', Mock(return_value=row))
    return ctx, row, client, Mock()


def test_initiation_uses_private_generated_keys(setup):
    ctx, row, client, cur = setup
    result = u.initiate(cur, ctx, schemas.AssetUploadStart(filename='clip.mp4', filesize_bytes=13_000_000))
    assert result['part_count'] == 2
    create = client.create_multipart_upload.call_args.kwargs
    assert create['Key'].startswith('marketing-upload-staging/tenants/test-publisher/')
    assert '/public/' not in create['Key']
    assert create['Metadata']['upload-session'] == result['id']
    assert cur.execute.call_args.args[1][9].startswith('tenants/test-publisher/assets/marketing/public/')


@pytest.mark.parametrize('name,size', [('evil.html', 1), ('large.png', 26*1024*1024)])
def test_rejected_before_s3(setup, name, size):
    ctx, row, client, cur = setup
    with pytest.raises(HTTPException):
        u.initiate(cur, ctx, schemas.AssetUploadStart(filename=name, filesize_bytes=size))
    client.create_multipart_upload.assert_not_called()


@pytest.mark.parametrize('size', [0, -1, 500*1024*1024+1])
def test_declared_size_is_bounded(size):
    with pytest.raises(ValidationError):
        schemas.AssetUploadStart(filename='clip.mp4', filesize_bytes=size)


def test_session_query_binds_both_tenant_and_user(setup):
    ctx, row, client, cur = setup
    u.s.one.return_value = None
    with pytest.raises(HTTPException) as error:
        u.sign_part(cur, ctx, row['id'], 1)
    assert error.value.status_code == 404
    assert u.s.one.call_args.args[2] == (row['id'], ctx['tenant']['id'], ctx['user']['id'])
    client.generate_presigned_url.assert_not_called()


def test_part_signature_binds_exact_size_and_key(setup):
    ctx, row, client, cur = setup
    u.sign_part(cur, ctx, row['id'], 1)
    params = client.generate_presigned_url.call_args.kwargs['Params']
    assert params['ContentLength'] == 12
    assert params['Key'] == row['staging_key']
    with pytest.raises(HTTPException):
        u.sign_part(cur, ctx, row['id'], 2)


def test_sigv4_signs_content_length_without_network():
    client = boto3.client('s3', region_name='us-east-2', aws_access_key_id='test',
        aws_secret_access_key='test', config=Config(signature_version='s3v4'))
    url = client.generate_presigned_url('upload_part', Params={
        'Bucket': 'test-bucket', 'Key': 'stage', 'UploadId': 'upload', 'PartNumber': 1, 'ContentLength': 12})
    assert 'X-Amz-SignedHeaders=content-length%3Bhost' in url


def test_expired_session_cannot_sign(setup):
    ctx, row, client, cur = setup
    row['expires_at'] = datetime.now(timezone.utc) - timedelta(seconds=1)
    with pytest.raises(HTTPException) as error:
        u.sign_part(cur, ctx, row['id'], 1)
    assert error.value.status_code == 410


@pytest.mark.parametrize('parts', [[], [{'PartNumber': 1, 'Size': 13, 'ETag': 'x'}],
                                   [{'PartNumber': 2, 'Size': 12, 'ETag': 'x'}]])
def test_wrong_or_missing_parts_never_complete(setup, parts):
    ctx, row, client, cur = setup
    client.head_object.side_effect = ClientError({'ResponseMetadata': {'HTTPStatusCode': 404}}, 'HeadObject')
    client.get_paginator.return_value.paginate.return_value = [{'Parts': parts}]
    with pytest.raises(HTTPException) as error:
        u.assembled_object(row)
    assert error.value.status_code == 422
    client.complete_multipart_upload.assert_not_called()


@pytest.mark.parametrize('already_assembled', [False, True])
def test_complete_inspects_then_publishes_and_is_retryable(setup, monkeypatch, already_assembled):
    ctx, row, client, cur = setup
    data = io.BytesIO()
    Image.new('RGB', (30, 20)).save(data, 'PNG')
    row['filesize_bytes'] = len(data.getvalue())
    head = {'ContentLength': row['filesize_bytes'], 'ETag': 'etag',
        'Metadata': {'upload-session': str(row['id'])}}
    if already_assembled:
        client.head_object.return_value = head
    else:
        client.head_object.side_effect = [ClientError({'ResponseMetadata': {'HTTPStatusCode': 404}}, 'HeadObject'), head]
        client.get_paginator.return_value.paginate.return_value = [{'Parts': [{'PartNumber': 1, 'Size': row['filesize_bytes'], 'ETag': 'part-etag'}]}]
    stream = io.BytesIO(data.getvalue())
    client.get_object.return_value = {'Body': stream}
    result = {'id': row['asset_id']}
    u.s.one.side_effect = [row, result]
    assert u.complete(cur, ctx, row['id']) == result
    assert stream.closed
    copy = client.copy_object.call_args.kwargs
    assert copy['ContentType'] == 'image/png'
    assert copy['Key'] == row['final_key']
    assert copy['Tagging'] == ''
    assert client.complete_multipart_upload.call_count == (0 if already_assembled else 1)
    client.delete_object.assert_not_called()  # staging survives a later DB rollback
    row['status'] = 'completed'
    monkeypatch.setattr(u.s, 'owned', Mock(return_value=result))
    u.s.one.side_effect = None
    u.s.one.return_value = row
    assert u.complete(cur, ctx, row['id']) == result
    assert client.copy_object.call_count == 1


def test_invalid_media_never_published(setup):
    ctx, row, client, cur = setup
    client.head_object.return_value = {'ContentLength': 12, 'ETag': 'etag',
        'Metadata': {'upload-session': str(row['id'])}}
    stream = io.BytesIO(b'invalid file')
    client.get_object.return_value = {'Body': stream}
    with pytest.raises(HTTPException):
        u.complete(cur, ctx, row['id'])
    assert stream.closed
    client.copy_object.assert_not_called()


def test_cancel_does_not_delete_completed_asset(setup):
    ctx, row, client, cur = setup
    row['status'] = 'completed'
    u.abort(cur, ctx, row['id'])
    client.delete_object.assert_not_called()
    client.abort_multipart_upload.assert_not_called()


def test_abort_cleans_assembled_staging_and_unregistered_copy(setup):
    ctx, row, client, cur = setup
    client.abort_multipart_upload.side_effect = client.exceptions.NoSuchUpload()
    u.abort(cur, ctx, row['id'])
    assert {c.kwargs['Key'] for c in client.delete_object.call_args_list} == {row['staging_key'], row['final_key']}


def test_s3_rules_preserve_existing_rules_and_scope_cleanup():
    existing_cors = {'ID': 'ARC', 'AllowedOrigins': ['https://example.org'], 'AllowedMethods': ['POST']}
    existing_lifecycle = {'ID': 'Other', 'Status': 'Enabled', 'Filter': {'Prefix': 'other/'}}
    cors, lifecycle = merge([existing_cors], [existing_lifecycle], 'https://www.inksuite.io')
    assert cors['CORSRules'][0] == existing_cors
    assert lifecycle['Rules'][0] == existing_lifecycle
    assert lifecycle['Rules'][1]['Filter'] == {'Prefix': 'marketing-upload-staging/'}
    assert merge(cors['CORSRules'], lifecycle['Rules'], 'https://www.inksuite.io') == (cors, lifecycle)
