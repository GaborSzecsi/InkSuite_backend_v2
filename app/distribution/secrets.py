"""Credentials never enter ordinary DB rows, API responses or event details."""
import json
import os
import boto3

def client():
    return boto3.client('secretsmanager',region_name=os.getenv('AWS_REGION') or os.getenv('AWS_DEFAULT_REGION') or 'us-east-2')

def reference(tenant,connection):
    from uuid import UUID
    return f'inksuite/distribution/{UUID(str(tenant))}/{UUID(str(connection))}'

def save(tenant,connection,credentials):
    name=reference(tenant,connection);svc=client()
    try: svc.create_secret(Name=name,SecretString=json.dumps(credentials))
    except svc.exceptions.ResourceExistsException: svc.put_secret_value(SecretId=name,SecretString=json.dumps(credentials))
    return name

def read(name):
    if not name.startswith('inksuite/distribution/'): raise ValueError('Invalid distribution secret reference.')
    return json.loads(client().get_secret_value(SecretId=name)['SecretString'])


def remove(name):
    """Schedule only an installation token secret for deletion, with AWS recovery window."""
    import re
    if not re.fullmatch(r'inksuite/distribution/[0-9a-f-]{36}/[0-9a-f-]{36}', name or ''):
        raise ValueError('Invalid installation secret reference.')
    svc = client()
    try:
        detail = svc.describe_secret(SecretId=name)
        if not detail.get('DeletedDate'):
            svc.delete_secret(SecretId=name, RecoveryWindowInDays=7)
    except svc.exceptions.ResourceNotFoundException:
        pass
