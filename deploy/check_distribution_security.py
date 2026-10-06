"""Read-only checks. Run with the API service environment; never prints secrets."""
import json
import os
import boto3
from botocore.exceptions import ClientError
from app.core.db import db_conn

region = os.getenv('AWS_REGION') or os.getenv('AWS_DEFAULT_REGION') or 'us-east-2'
def check(label, function):
    try:
        print(label + ': ' + json.dumps(function(), default=str))
    except ClientError as error:
        print(label + ': ' + error.response['Error']['Code'])
    except Exception as error:
        print(label + ': ' + type(error).__name__)

def database():
    with db_conn() as conn, conn.cursor() as cur:
        cur.execute('SELECT ssl,version FROM pg_stat_ssl WHERE pid=pg_backend_pid()')
        tls = cur.fetchone()
        cur.execute("SELECT count(*) FROM distribution_orders WHERE source='SHOPIFY' AND (recipient<>'{}'::jsonb OR delivery_instructions<>'')")
        return {'connection_tls': tls, 'legacy_delivery_rows':cur.fetchone()[0]}

def rds():
    instances=boto3.client('rds',region_name=region).describe_db_instances()['DBInstances']
    return [{k:d.get(k) for k in ('DBInstanceIdentifier','StorageEncrypted','BackupRetentionPeriod','PubliclyAccessible','MultiAZ')} for d in instances if d.get('Endpoint',{}).get('Address')=='inksuite-postgres.c582wcyqm25j.us-east-2.rds.amazonaws.com']

def cognito():
    pool=os.getenv('COGNITO_USER_POOL_ID') or os.getenv('COGNITO_POOL_ID')
    if not pool: return {'verification':'pool environment name not found'}
    p=boto3.client('cognito-idp',region_name=region).describe_user_pool(UserPoolId=pool)['UserPool']
    return {'Policies':p.get('Policies'), 'MfaConfiguration':p.get('MfaConfiguration')}

check('Database',database)
check('RDS configuration',rds)
check('Cognito policy',cognito)
