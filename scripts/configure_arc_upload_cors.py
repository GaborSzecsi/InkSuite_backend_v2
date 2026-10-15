"""Add the website origin for signed browser ARC POSTs; preserve other CORS rules.
Default is read-only. Pass --apply explicitly to update the existing bucket.
"""
import argparse
import json
from pathlib import Path
import sys
from urllib.parse import urlsplit


def merge_rules(rules, origin):
    for rule in rules:
        if origin in rule.get('AllowedOrigins',[]) and 'POST' in rule.get('AllowedMethods',[]):
            return rules
    return [*rules,{'AllowedOrigins':[origin],'AllowedMethods':['POST'],
                   'AllowedHeaders':['*'],'ExposeHeaders':['ETag'],'MaxAgeSeconds':3600}]


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--origin',default='https://www.inksuite.io')
    parser.add_argument('--apply',action='store_true')
    args=parser.parse_args()
    origin=args.origin.rstrip('/')
    parsed=urlsplit(origin)
    if parsed.scheme!='https' or not parsed.hostname or parsed.path or parsed.query or parsed.fragment or parsed.username or parsed.password:
        parser.error('--origin must be an exact HTTPS website origin, without a path.')
    root=Path(__file__).resolve().parents[1]
    sys.path.insert(0,str(root))
    from dotenv import load_dotenv
    load_dotenv('/etc/inksuite-api.env',override=False)
    load_dotenv(root/'.env',override=False)
    from app.marketplace.arc_service import storage
    from botocore.exceptions import ClientError
    client,bucket=storage()
    try:
        rules=client.get_bucket_cors(Bucket=bucket).get('CORSRules',[])
    except ClientError as error:
        if error.response.get('Error',{}).get('Code')!='NoSuchCORSConfiguration':
            raise
        rules=[]
    merged=merge_rules(rules,origin)
    if merged==rules:
        print('The website already has S3 POST CORS permission. No change needed.')
        return
    print(json.dumps({'CORSRules':merged},indent=2))
    if not args.apply:
        print('Read-only preview. Rerun with --apply to save this bucket CORS configuration.')
        return
    client.put_bucket_cors(Bucket=bucket,CORSConfiguration={'CORSRules':merged})
    verified=client.get_bucket_cors(Bucket=bucket).get('CORSRules',[])
    if not any(origin in r.get('AllowedOrigins',[]) and 'POST' in r.get('AllowedMethods',[]) for r in verified):
        raise RuntimeError('CORS update could not be verified.')
    print('Verified: signed browser uploads are allowed from '+origin)


if __name__=='__main__':
    main()
