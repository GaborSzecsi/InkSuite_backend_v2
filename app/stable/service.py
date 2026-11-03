from __future__ import annotations

import calendar
import json
from datetime import date, datetime, time, timezone
from decimal import Decimal, ROUND_HALF_UP
from uuid import uuid4
from zoneinfo import ZoneInfo

from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from app.core.db import db_conn
from .parser import Report, ReviewRequired, VERSION


def json_value(value):
    return json.loads(json.dumps(value, default=str))


def cents(amount):
    return Decimal(str(amount)).quantize(Decimal('.01'), rounding=ROUND_HALF_UP)


def allocate_cents(values,target=None):
    """Deterministic cent allocation; aggregate cash must not drift per row."""
    rounded={key:cents(value) for key,value in values.items()}
    if not rounded:
        return rounded
    goal=cents(sum(values.values(),Decimal(0))) if target is None else cents(target)
    difference=goal-sum(rounded.values(),Decimal(0))
    direction=Decimal('.01') if difference>0 else Decimal('-.01')
    order=sorted(values,key=lambda key: (-(values[key]-rounded[key]) if difference>0 else values[key]-rounded[key],str(key)))
    for i in range(int(abs(difference)/Decimal('.01'))):
        rounded[order[i%len(order)]]+=direction
    return rounded


def ready(cur):
    cur.execute("SELECT to_regclass('public.stable_imports')")
    row = cur.fetchone()
    return bool(next(iter(row.values())) if isinstance(row, dict) else row[0])


def resolve(cur, tenant, codes):
    result = {};matches={}
    cur.execute("SELECT id,work_id,regexp_replace(isbn13,'[^0-9]','','g') AS code FROM editions WHERE tenant_id=%s AND regexp_replace(isbn13,'[^0-9]','','g')=ANY(%s::text[])", (tenant,sorted(codes)))
    for row in cur.fetchall():
        matches.setdefault(row['code'],[]).append(row)
    for code in sorted(codes):
        rows=matches.get(code,[])
        if len(rows) != 1:
            raise ReviewRequired(f'ISBN {code}: expected one publisher edition, found {len(rows)}')
        result[code] = rows[0]
    return result


def discount_works(cur,tenant):
    cur.execute("""SELECT DISTINCT rs.work_id FROM royalty_sets rs
        JOIN royalty_rules r ON r.tenant_id=rs.tenant_id AND r.royalty_set_id=rs.id
        JOIN royalty_tiers t ON t.tenant_id=r.tenant_id AND t.rule_id=r.id
        JOIN royalty_tier_conditions c ON c.tenant_id=t.tenant_id AND c.tier_id=t.id
        WHERE rs.tenant_id=%s AND rs.is_active AND c.kind::text='discount'""",(tenant,))
    return {str(row['work_id']) for row in cur.fetchall()}


def register(cur, tenant, bucket, key, version, report):
    codes = {s.isbn for s in report.sales} | {c['isbn'] for c in report.costs} | {i['isbn'] for i in report.inventory}
    editions={}
    try:
        editions=resolve(cur, tenant, codes)
    except ReviewRequired as exc:
        report.issues.append(str(exc))
    if report.kind == 'statement':
        cur.execute("SELECT id FROM stable_imports WHERE tenant_id=%s AND period_code=%s AND territory=%s AND content_hash<>%s AND status<>'failed'", (tenant, report.period, report.territory, report.digest))
        if cur.fetchone():
            report.issues.append('Another source revision exists for this period/territory; controlled replacement required')
        # Stable may overlap the previous distributor during the transition.
        cur.execute("SELECT 1 FROM royalty_sales_lines r JOIN royalty_periods p ON p.id=r.period_id WHERE r.tenant_id=%s AND p.period_code=%s AND COALESCE(r.source_system,'')<>'stable' LIMIT 1", (tenant,report.period))
        if cur.fetchone():
            report.issues.append('Prior-distributor activity exists in this period; review ISBN/source overlap before posting')
    status = 'needs_review' if report.issues else 'validated'
    payload=report.payload()
    needed=discount_works(cur,tenant) if report.sales else set()
    for row in payload['sales']:
        row.pop('sales_price',None)
        if str(editions.get(row['isbn'],{}).get('work_id')) not in needed:
            row.pop('discount_percent',None)
        elif row.get('discount_percent') is None and Decimal(row['units_sold']):
            report.issues.append(f"A royalty discount tier requires a verified discount for ISBN {row['isbn']}")
            status='needs_review'
    cur.execute("""INSERT INTO stable_imports(tenant_id,bucket,object_key,version_id,content_hash,parser_version,report_type,
        territory,period_code,as_of,status,issues,payload)
        VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT DO NOTHING RETURNING id,status""",
        (tenant,bucket,key,version,report.digest,VERSION,report.kind,report.territory,report.period,report.as_of,status,
         Jsonb(report.issues),Jsonb(json_value(payload if status=='validated' else {}))))
    return cur.fetchone()


def post(cur, tenant, import_id):
    """Atomic, tenant-bound posting. Caller must own a transaction and source lock."""
    cur.execute('SELECT * FROM stable_imports WHERE tenant_id=%s AND id=%s FOR UPDATE', (tenant,import_id))
    source = cur.fetchone()
    if not source or source['status'] != 'validated':
        return False
    cur.execute('SELECT * FROM stable_sources WHERE tenant_id=%s', (tenant,))
    config = cur.fetchone()
    if not config or not config['enabled']:
        return False
    payload = source['payload']
    codes = {s['isbn'] for s in payload['sales']} | {c['isbn'] for c in payload['costs']} | {i['isbn'] for i in payload['inventory']}
    editions = resolve(cur, tenant, codes)
    if source['report_type'] == 'inventory':
        return post_inventory(cur, source, payload, editions, config)
    period = source['period_code']
    start = date.fromisoformat(period+'-01')
    end = start.replace(day=calendar.monthrange(start.year,start.month)[1])
    cur.execute('SELECT id,is_closed FROM royalty_periods WHERE tenant_id=%s AND period_start<=%s AND period_end>=%s FOR UPDATE', (tenant,end,start))
    periods = cur.fetchall()
    if any(p['is_closed'] for p in periods):
        raise ReviewRequired('Statement overlaps a closed royalty period')
    cur.execute("SELECT 1 FROM royalty_statements s JOIN royalty_periods p ON p.id=s.period_id AND p.tenant_id=s.tenant_id WHERE s.tenant_id=%s AND p.period_start<=%s AND p.period_end>=%s AND lower(s.status)<>'draft' LIMIT 1", (tenant,end,start))
    if cur.fetchone():
        raise ReviewRequired('Statement overlaps an issued/frozen royalty statement')
    cur.execute('SELECT id FROM royalty_periods WHERE tenant_id=%s AND period_code=%s', (tenant,period))
    p = cur.fetchone()
    if p:
        period_id = p['id']
    else:
        cur.execute('INSERT INTO royalty_periods(tenant_id,period_code,period_start,period_end) VALUES(%s,%s,%s,%s) RETURNING id', (tenant,period,start,end))
        period_id = cur.fetchone()['id']
    territory = source['territory']
    currency = config['us_currency' if territory == 'US' else 'ca_currency']
    adjustments = list(payload['costs'])
    # Existing money columns have two decimals. Reconcile the final ISBN cash
    # and source gross totals before allocating cents to individual buyer rows.
    exact_net={code:Decimal(0) for code in codes}
    exact_gross={code:Decimal(0) for code in codes}
    for row in payload['sales']:
        exact_net[row['isbn']]+=Decimal(row['publisher_receipts'])
        exact_gross[row['isbn']]+=Decimal(row['gross_sales'])
    for cost in payload['costs']:
        exact_net[cost['isbn']]-=Decimal(cost['amount'])
    net_targets=allocate_cents(exact_net)
    gross_targets=allocate_cents(exact_gross)
    posted_receipts={code:Decimal(0) for code in codes}
    sale_rows = defaultdict_list(payload['sales'])
    for code, rows in sale_rows.items():
        gross_rows=allocate_cents({str(i):Decimal(row['gross_sales']) for i,row in enumerate(rows)},gross_targets[code])
        for i,row in enumerate(rows):
            receipt = Decimal(row['publisher_receipts'])
            if not Decimal(row['units_sold']) and not Decimal(row['units_returned']) and not Decimal(row['gross_sales']):
                if receipt:
                    adjustments.append(dict(isbn=code,kind='transfer_handling',amount=-receipt,sheet=row['sheet'],row=row['row']))
                continue
            edition = editions[code]
            table = 'royalty_sales_lines' if territory == 'US' else 'canada_sales_lines'
            sid = uuid4()
            posted_receipts[code]+=cents(receipt)
            cur.execute(f"""INSERT INTO {table}(id,tenant_id,work_id,edition_id,period_id,transaction_date,source_system,source_file,
              reference_number,customer_name,territory,transaction_type,units_sold,units_returned,gross_sales,publisher_receipts,currency,discount_percent)
              VALUES(%s,%s,%s,%s,%s,%s,'stable',%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
              (sid,tenant,edition['work_id'],edition['id'],period_id,end,source['object_key'],f"{source['id']}:{row['sheet']}:{row['row']}",
               row['buyer'],territory,'return' if Decimal(row['units_returned']) and not Decimal(row['units_sold']) else 'sale',
               row['units_sold'],row['units_returned'],gross_rows[str(i)],cents(receipt),currency,row.get('discount_percent')))
    grouped={}
    for cost in adjustments:
        key=(cost['isbn'],cost['kind'])
        if key not in grouped:
            grouped[key]=dict(cost,amount=Decimal(0))
        grouped[key]['amount']+=Decimal(cost['amount'])
    expenses={code:Decimal(0) for code in codes}
    for cost in grouped.values():
        expenses[cost['isbn']]+=cost['amount']
    for code in sorted(codes):
        correction=posted_receipts[code]-expenses[code]-net_targets[code]
        if correction:
            grouped[(code,'rounding')]=dict(isbn=code,kind='rounding',amount=correction)
    for cost in grouped.values():
        cur.execute('''INSERT INTO stable_receipt_adjustments(tenant_id,import_id,edition_id,territory,currency,period_id,kind,amount,billed_inventory)
          VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s)''', (tenant,source['id'],editions[cost['isbn']]['id'],territory,currency,period_id,cost['kind'],cost['amount'],cost.get('inventory')))
    cur.execute("UPDATE stable_imports SET status='posted',posted_at=now(),payload='{}' WHERE tenant_id=%s AND id=%s", (tenant,source['id']))
    return True


def defaultdict_list(rows):
    result = {}
    for row in rows:
        result.setdefault(row['isbn'], []).append(row)
    return result


def post_inventory(cur, source, payload, editions, config):
    tenant = source['tenant_id']
    day = source['as_of']
    closing = datetime.combine(day,time(23,59),ZoneInfo('America/Los_Angeles')).astimezone(timezone.utc)
    if closing>datetime.now(timezone.utc):
        return False  # Keep today's closing report queued until its Pacific day ends.
    cur.execute("SELECT max(as_of) AS latest FROM stable_imports WHERE tenant_id=%s AND status='posted' AND report_type='inventory'", (tenant,))
    latest = cur.fetchone()['latest']
    if latest and day <= latest:
        raise ReviewRequired('Inventory is older than or conflicts with the latest posted report')
    for item in payload['inventory']:
        cur.execute('INSERT INTO stable_inventory_snapshots(tenant_id,import_id,edition_id,as_of,available,ending) VALUES(%s,%s,%s,%s,%s,%s)',
                    (tenant,source['id'],editions[item['isbn']]['id'],day,item['available'],item['ending']))
    age = datetime.now(timezone.utc)-closing
    if config['inventory_connection_id'] and age.total_seconds() <= 24*3600:
        if not config['usable_inventory_confirmed']:
            raise ReviewRequired('Usable balance must be confirmed as available-to-sell before Shopify publication')
        from app.distribution.execution import reconcile
        cur.execute('SELECT * FROM distribution_connections WHERE tenant_id=%s AND id=%s FOR UPDATE', (tenant,config['inventory_connection_id']))
        connection = cur.fetchone()
        # Report's closing day is represented in Pacific time, never import time.
        as_of = closing
        reconcile(cur,connection,dict(version=1,as_of=as_of,complete=False,
                  items=[dict(isbn=i['isbn'],on_hand=i['available']) for i in payload['inventory']]))
        # The existing distribution scheduler publishes only when a Shopify
        # installation has explicitly enabled inventory sync for this publisher.
    cur.execute("UPDATE stable_imports SET status='posted',posted_at=now(),payload='{}' WHERE tenant_id=%s AND id=%s", (tenant,source['id']))
    return True


def royalty_adjustments(cur, tenant, work_id, start, end):
    """Synthetic accounting inputs have zero sales units and no buyer identity."""
    if not ready(cur):
        return []
    cur.execute('''SELECT a.id,a.edition_id,0 AS units_sold,0 AS units_returned,0 AS discount_percent,
       -a.amount AS publisher_receipts,0 AS gross_sales,
       CASE WHEN a.territory='CA' THEN 'canada_derived' ELSE 'first_rights' END AS royalty_stream,
       e.product_form,e.product_form_detail,a.currency
       FROM stable_receipt_adjustments a JOIN editions e ON e.tenant_id=a.tenant_id AND e.id=a.edition_id
       JOIN stable_imports i ON i.tenant_id=a.tenant_id AND i.id=a.import_id AND i.status='posted'
       JOIN royalty_periods p ON p.id=a.period_id
       WHERE a.tenant_id=%s AND e.work_id=%s AND p.period_end BETWEEN %s AND %s
       UNION ALL
       SELECT r.id,r.edition_id,r.units_sold,r.units_returned,COALESCE(r.discount_percent,0),r.publisher_receipts,r.gross_sales,
       'canada_derived',e.product_form,e.product_form_detail,r.currency
       FROM canada_sales_lines r JOIN editions e ON e.tenant_id=r.tenant_id AND e.id=r.edition_id
       JOIN stable_imports i ON i.tenant_id=r.tenant_id AND i.id::text=split_part(r.reference_number,':',1) AND i.territory='CA' AND i.status='posted'
       JOIN royalty_periods p ON p.id=r.period_id
       WHERE r.tenant_id=%s AND r.source_system='stable' AND e.work_id=%s AND p.period_end BETWEEN %s AND %s''',
       (tenant,work_id,start,end,tenant,work_id,start,end))
    rows = list(cur.fetchall())
    if any(row['currency'] != 'USD' for row in rows):
        raise ReviewRequired('Non-USD Stable receipts require an approved exchange-rate policy before USD royalty calculation')
    return rows
