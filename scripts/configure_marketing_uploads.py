"""Preview/merge S3 browser-upload settings without replacing unrelated rules."""
import argparse
import json

import boto3
from botocore.exceptions import ClientError


def configuration(client, bucket, operation, key, missing):
    try:
        return getattr(client, operation)(Bucket=bucket).get(key, [])
    except ClientError as error:
        if error.response.get('Error', {}).get('Code') != missing:
            raise
        return []


def merge(cors, lifecycle, origin):
    cors = [rule for rule in cors if rule.get('ID') != 'InkSuiteMarketingUploads']
    cors.append({'ID': 'InkSuiteMarketingUploads', 'AllowedOrigins': [origin],
                 'AllowedMethods': ['PUT'], 'AllowedHeaders': ['content-type'],
                 'MaxAgeSeconds': 3600})
    lifecycle = [rule for rule in lifecycle if rule.get('ID') != 'InkSuiteMarketingUploadCleanup']
    # This prefix is deliberately separate from all public media and video projects.
    # Hourly DB-aware cleanup also removes failed final copies; lifecycle is a backstop.
    lifecycle.append({'ID': 'InkSuiteMarketingUploadCleanup', 'Status': 'Enabled',
        'Filter': {'Prefix': 'marketing-upload-staging/'}, 'Expiration': {'Days': 1},
        'AbortIncompleteMultipartUpload': {'DaysAfterInitiation': 1}})
    return {'CORSRules': cors}, {'Rules': lifecycle}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--bucket', required=True)
    parser.add_argument('--region', default='us-east-2')
    parser.add_argument('--origin', default='https://www.inksuite.io')
    parser.add_argument('--apply', action='store_true')
    args = parser.parse_args()
    if not args.origin.startswith('https://') or '*' in args.origin or args.origin.endswith('/'):
        parser.error('Use an exact HTTPS website origin without a trailing slash.')
    client = boto3.client('s3', region_name=args.region)
    try:
        cors = configuration(client, args.bucket, 'get_bucket_cors', 'CORSRules', 'NoSuchCORSConfiguration')
        lifecycle = configuration(client, args.bucket, 'get_bucket_lifecycle_configuration', 'Rules', 'NoSuchLifecycleConfiguration')
        cors, lifecycle = merge(cors, lifecycle, args.origin)
        print(json.dumps({'CORSConfiguration': cors, 'LifecycleConfiguration': lifecycle}, indent=2))
        if args.apply:
            client.put_bucket_cors(Bucket=args.bucket, CORSConfiguration=cors)
            client.put_bucket_lifecycle_configuration(Bucket=args.bucket, LifecycleConfiguration=lifecycle)
            if client.get_bucket_cors(Bucket=args.bucket)['CORSRules'] != cors['CORSRules']:
                raise RuntimeError('CORS configuration readback did not match.')
            if client.get_bucket_lifecycle_configuration(Bucket=args.bucket)['Rules'] != lifecycle['Rules']:
                raise RuntimeError('Lifecycle configuration readback did not match.')
            print('S3 upload configuration applied and verified.')
        else:
            print('Preview only. Review these rules before rerunning with --apply.')
    except ClientError as error:
        raise SystemExit(str(error)) from None


if __name__ == '__main__':
    main()
