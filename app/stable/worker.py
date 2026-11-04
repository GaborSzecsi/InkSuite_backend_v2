"""Bounded daily S3 scan. IAM credentials come from the standard SDK chain."""
from __future__ import annotations

import argparse
import hashlib
import json
import logging
import re
from datetime import datetime, timedelta, timezone
from io import BytesIO
from pathlib import Path
from zipfile import BadZipFile
from xml.etree.ElementTree import ParseError

import boto3
from botocore.config import Config
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from app.core.db import db_conn
from .parser import ReviewRequired, Report, VERSION, parse_book, read_workbook, statement_period, sections, report_territory
from .service import json_value, register, post

log = logging.getLogger(__name__)


def report_date(key):
    # Stable's reviewed MMDDYY daily-inventory naming convention. Pub Date is
    # never used as a stock report date. Other names require explicit review.
    match = re.match(r'(\d{6})-Marble Daily Inventory\b', Path(key).name, re.I)
    if match:
        return datetime.strptime(match[1], '%m%d%y').date().isoformat()
    return None


def scan(s3, config):
    tenant = config['tenant_id']
    now = datetime.now(timezone.utc)
    objects = []
    present_keys=set()
    for page in s3.get_paginator('list_objects_v2').paginate(Bucket=config['bucket'],Prefix=config['key_prefix']):
        for item in page.get('Contents', []):
            present_keys.add(item['Key'])
            name = Path(item['Key']).name.lower()
            if not name.endswith('.xlsx') or item['Size'] == 0:
                continue
            if name.startswith('~$') or name.endswith(('.tmp.xlsx','.part.xlsx')):
                continue
            if item['LastModified'] > now-timedelta(minutes=5):
                continue
            objects.append(item)
            if len(objects) > 10000:
                raise ReviewRequired('Source contains more than 10,000 candidate workbooks')
    # Companions are read only after a matching fiscal period has been established.
    companions = {}
    def download(item):
        if item['Size'] > 32*1024*1024:
            raise ReviewRequired('Source workbook exceeds 32 MB')
        response = s3.get_object(Bucket=config['bucket'],Key=item['Key'],IfMatch=item['ETag'])
        with response['Body'] as body:
            data = body.read(32*1024*1024+1)
        if len(data) > 32*1024*1024:
            raise ReviewRequired('Source workbook exceeds 32 MB')
        return data,response.get('VersionId')
    for item in objects:
        name = Path(item['Key']).name.lower()
        if 'draft' not in name:
            try:
                data, _ = download(item)
                book = read_workbook(data)
                if 'transactions' not in sections(book,'US'):
                    continue
                period = statement_period(book)
                if period in companions:
                    companions[period] = None  # Ambiguous revisions cannot supply evidence.
                else:
                    companions[period] = item
            except (ReviewRequired, ValueError, BadZipFile, ParseError, OSError):
                continue
    results = []
    with db_conn() as connection, connection.cursor(row_factory=dict_row) as cur:
        cur.execute('SELECT pg_try_advisory_lock(hashtextextended(%s,0)) AS locked', ('stable:'+str(tenant),))
        if not cur.fetchone()['locked']:
            return []
        try:
            with connection.transaction():
                reconcile_removed_inventory(cur,config,present_keys)
            for item in sorted(objects,key=lambda i:i['Key']):
                data, version = download(item)
                digest = hashlib.sha256(data).hexdigest()
                cur.execute('SELECT id,status,parser_version,report_type FROM stable_imports WHERE tenant_id=%s AND content_hash=%s AND as_of IS NOT DISTINCT FROM %s::date', (tenant,digest,report_date(item['Key'])))
                existing=cur.fetchone()
                if existing and not (existing['status']=='needs_review' and (existing['parser_version']!=VERSION or existing['report_type']=='inventory')):
                    continue
                try:
                    book = read_workbook(data)
                    companion = None
                    if report_territory(book)=='CA':
                        match = companions.get(statement_period(book))
                        if match:
                            companion = read_workbook(download(match)[0])
                    report = parse_book(book,digest,companion=companion,inventory_date=report_date(item['Key']))
                    if 'draft' in Path(item['Key']).name.lower():
                        report.issues.append('Draft statement cannot be automatically posted')
                    if report.currency_hint:
                        currency = config['ca_currency' if report.territory == 'CA' else 'us_currency']
                        if report.currency_hint != currency:
                            report.issues.append(f'Reported currency {report.currency_hint} conflicts with configured {currency}')
                except (ReviewRequired, ValueError, KeyError, BadZipFile, ParseError, OSError) as exc:
                    report = Report(digest,'unknown',issues=[str(exc)])
                with connection.transaction():
                    result = register(cur,tenant,config['bucket'],item['Key'],version,report)
                    if result:
                        results.append(dict(key=item['Key'],**result))
            # Quarantine every conflicting revision before posting any of them.
            with connection.transaction():
                cur.execute("""UPDATE stable_imports i SET status='needs_review',payload='{}',issues=i.issues || '["Multiple statement revisions require review"]'::jsonb
                  WHERE i.tenant_id=%s AND i.report_type='statement' AND i.status='validated'
                  AND EXISTS(SELECT 1 FROM stable_imports j WHERE j.tenant_id=i.tenant_id AND j.period_code=i.period_code
                    AND j.territory=i.territory AND j.id<>i.id AND j.status NOT IN ('failed','superseded'))""", (tenant,))
                cur.execute("""UPDATE stable_imports i SET status='needs_review',payload='{}',issues=i.issues || '["Conflicting inventory sources for the same report date"]'::jsonb
                  WHERE i.tenant_id=%s AND i.report_type='inventory' AND i.status='validated'
                  AND EXISTS(SELECT 1 FROM stable_imports j WHERE j.tenant_id=i.tenant_id AND j.report_type='inventory'
                    AND j.as_of=i.as_of AND j.content_hash<>i.content_hash AND j.status NOT IN ('failed','superseded'))""", (tenant,))
            cur.execute("SELECT id FROM stable_imports WHERE tenant_id=%s AND status='validated' ORDER BY as_of NULLS FIRST,period_code,created_at", (tenant,))
            pending = list(cur.fetchall())
            for item in pending:
                try:
                    with connection.transaction():
                        post(cur,tenant,item['id'])
                except ReviewRequired as exc:
                    cur.execute("UPDATE stable_imports SET status='needs_review',payload='{}',issues=issues || %s WHERE tenant_id=%s AND id=%s", (Jsonb([str(exc)]),tenant,item['id']))
                # Database/transport exceptions deliberately fail the job for retry.
        finally:
            cur.execute('SELECT pg_advisory_unlock(hashtextextended(%s,0))', ('stable:'+str(tenant),))
    return results


def reconcile_removed_inventory(cur,config,present_keys):
    """Retire only unposted inventory candidates removed from this exact S3 source."""
    cur.execute("""SELECT id,object_key FROM stable_imports WHERE tenant_id=%s AND bucket=%s
      AND report_type='inventory' AND status IN ('needs_review','validated')""",(config['tenant_id'],config['bucket']))
    for row in cur.fetchall():
        if row['object_key'].startswith(config['key_prefix']) and row['object_key'] not in present_keys:
            cur.execute("""UPDATE stable_imports SET status='superseded',payload='{}',
              issues=issues || '["Unposted inventory source removed from S3"]'::jsonb
              WHERE tenant_id=%s AND id=%s AND status IN ('needs_review','validated')""",(config['tenant_id'],row['id']))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--local',type=Path,help='Parse a local workbook without SQL or S3 writes')
    parser.add_argument('--companion',type=Path)
    args = parser.parse_args()
    if args.local:
        data = args.local.read_bytes()
        report = parse_book(read_workbook(data),hashlib.sha256(data).hexdigest(),
                  companion=read_workbook(args.companion.read_bytes()) if args.companion else None,
                  inventory_date=report_date(args.local.name))
        print(json.dumps(json_value(dict(payload=report.payload(),totals=report.totals(report.territory))),indent=2))
        return 2 if report.issues else 0
    s3 = boto3.client('s3',region_name='us-east-2',config=Config(connect_timeout=5,read_timeout=30,retries={'mode':'standard','total_max_attempts':3}))
    with db_conn() as connection, connection.cursor(row_factory=dict_row) as cur:
        cur.execute('SELECT * FROM stable_sources ORDER BY tenant_id')
        sources = list(cur.fetchall())
    for source in sources:
        log.info('Stable ingestion: %s',json.dumps(json_value(scan(s3,source))))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
