from datetime import date, datetime
from decimal import Decimal
from zoneinfo import ZoneInfo

from fastapi import APIRouter, Depends, Query, HTTPException
from psycopg.rows import dict_row

from app.core.db import db_conn
from app.distribution.router import access
from .service import ready
from .parser import VERSION

router = APIRouter(prefix='/tenants/{tenant_slug}/stable',tags=['Stable analytics'])


@router.get('/status')
def sync_status(ctx=Depends(access)):
    with db_conn() as conn,conn.cursor(row_factory=dict_row) as cur:
        if not ready(cur):
            return dict(available=False)
        cur.execute("SELECT max(posted_at) AS last_sync,count(*) FILTER(WHERE status='needs_review') AS needs_review FROM stable_imports WHERE tenant_id=%s",(ctx['tenant']['id'],))
        row=cur.fetchone()
        return dict(available=True,lastSync=row['last_sync'],needsReview=row['needs_review'],schedule='00:05 America/Los_Angeles')


def ledger(cur,tenant,year,territory,currency,search,work_id):
    cur.execute('''WITH sales AS (
       SELECT r.id,r.tenant_id,r.edition_id,r.period_id,r.customer_name,r.units_sold,r.units_returned,r.unit_adjustment,
         r.gross_sales,r.publisher_receipts,r.currency,'US'::text AS territory FROM royalty_sales_lines r
       WHERE r.source_system='stable' AND EXISTS(SELECT 1 FROM stable_imports i WHERE i.tenant_id=r.tenant_id
         AND i.id::text=split_part(r.reference_number,':',1) AND i.status='posted' AND i.territory='US')
       UNION ALL
       SELECT r.id,r.tenant_id,r.edition_id,r.period_id,r.customer_name,r.units_sold,r.units_returned,r.unit_adjustment,
         r.gross_sales,r.publisher_receipts,r.currency,'CA'::text FROM canada_sales_lines r
       WHERE r.source_system='stable' AND EXISTS(SELECT 1 FROM stable_imports i WHERE i.tenant_id=r.tenant_id
         AND i.id::text=split_part(r.reference_number,':',1) AND i.status='posted' AND i.territory='CA')
       UNION ALL
       SELECT a.id,a.tenant_id,a.edition_id,a.period_id,NULL,0,0,0,0,-a.amount,a.currency,a.territory
       FROM stable_receipt_adjustments a JOIN stable_imports i ON i.tenant_id=a.tenant_id AND i.id=a.import_id AND i.status='posted'
     ) SELECT s.*,e.isbn13,e.product_form,e.work_id,w.uid AS work_uid,w.title,p.period_code
       FROM sales s JOIN editions e ON e.tenant_id=s.tenant_id AND e.id=s.edition_id
       JOIN works w ON w.tenant_id=e.tenant_id AND w.id=e.work_id
       JOIN royalty_periods p ON p.tenant_id=s.tenant_id AND p.id=s.period_id
       WHERE s.tenant_id=%s AND (%s='' OR left(p.period_code,4)=%s)
       AND (%s='ALL' OR s.territory=%s) AND s.currency=%s
       AND (%s='' OR w.title ILIKE %s OR e.isbn13 ILIKE %s OR s.customer_name ILIKE %s)
       AND (%s='' OR e.work_id::text=%s OR COALESCE(w.uid::text,'')=%s)''',
       (tenant,year,year,territory,territory,currency,search,'%'+search+'%','%'+search+'%','%'+search+'%',work_id,work_id,work_id))
    return list(cur.fetchall())


def bestseller_ranks(records):
    editions={}
    for row in records:
        item=editions.setdefault(str(row['edition_id']),dict(net=Decimal(0),gross=Decimal(0),title=row['title']))
        item['net']+=Decimal(str(row['units_sold'] or 0))-Decimal(str(row['units_returned'] or 0))+Decimal(str(row['unit_adjustment'] or 0))
        item['gross']+=Decimal(str(row['gross_sales'] or 0))
    ordered=sorted((key for key in editions if editions[key]['net']>0),
      key=lambda key:(-editions[key]['net'],-editions[key]['gross'],editions[key]['title'].casefold(),key))
    return {key:rank for rank,key in enumerate(ordered,1)}


def period_bounds(mode,year,month,today=None):
    today=today or datetime.now(ZoneInfo('America/Los_Angeles')).date()
    if mode not in ('all','ytd','year','month'):
        raise HTTPException(422,'Invalid period filter')
    if mode=='all':
        return None,f'{today:%Y-%m}'
    if not year.isdigit() or not 2000<=int(year)<=2099:
        raise HTTPException(422,'Select a valid year')
    if mode=='month':
        if not month.isdigit() or not 1<=int(month)<=12:
            raise HTTPException(422,'Select a valid month')
        code=f'{year}-{int(month):02d}'
        return code,code
    return f'{year}-01',f'{year}-{today.month:02d}' if mode=='ytd' else f'{year}-12'


def channel_stock(cur,tenant,cutoff,work_id):
    # A stock balance is cumulative from the start, never just selected-month transfers.
    cur.execute('''SELECT m.buyer,m.territory,sum(m.outbound-m.returned) AS transferred
      FROM stable_channel_movements m JOIN stable_imports i ON i.tenant_id=m.tenant_id AND i.id=m.import_id
      JOIN editions e ON e.tenant_id=m.tenant_id AND e.id=m.edition_id
      JOIN works w ON w.tenant_id=e.tenant_id AND w.id=e.work_id
      WHERE m.tenant_id=%s AND i.status='posted' AND i.period_code<=%s
      AND (%s='' OR w.id::text=%s OR COALESCE(w.uid::text,'')=%s)
      GROUP BY m.buyer,m.territory''',(tenant,cutoff,work_id,work_id,work_id))
    stock={(r['buyer'],r['territory']):Decimal(str(r['transferred'])) for r in cur.fetchall()}
    for row in ledger(cur,tenant,'','ALL','USD','',work_id):
        if row['period_code']<=cutoff and row['customer_name']=='Amazon (paid sales)':
            key=(row['customer_name'],row['territory'])
            stock[key]=stock.get(key,Decimal(0))-Decimal(str(row['units_sold'] or 0))-Decimal(str(row['unit_adjustment'] or 0))
    cur.execute("""SELECT count(*) AS gaps FROM stable_imports WHERE tenant_id=%s AND period_code<=%s
      AND report_type IN ('statement','unknown') AND (status NOT IN ('posted','superseded','failed')
      OR (status='posted' AND parser_version<>%s))""",(tenant,cutoff,VERSION))
    return stock,bool(cur.fetchone()['gaps'])


@router.get('/overview')
def overview(year: str=Query('',pattern=r'^(|20\d{2})$'),territory: str=Query('ALL',pattern='^(ALL|US|CA)$'),
             currency: str=Query('USD',pattern='^[A-Z]{3}$'),q: str=Query('',max_length=200),
             work_id: str=Query('',max_length=64), ctx=Depends(access), period: str='all', month: str=''):
    tenant = ctx['tenant']['id']
    lower,upper=period_bounds(period,year,month)
    with db_conn() as conn,conn.cursor(row_factory=dict_row) as cur:
        if not ready(cur):
            return dict(available=False,detail='Stable ingestion migration has not been applied.',imports=[],rows=[],months=[],buyers=[])
        records = ledger(cur,tenant,year,territory,currency,'','')
        if lower:
            records=[r for r in records if lower<=r['period_code']<=upper]
        ranks=bestseller_ranks(records)
        if work_id:
            records=[r for r in records if work_id in (str(r['work_id']),str(r['work_uid'] or ''))]
        if q:
            records=[r for r in records if any(q.casefold() in str(r.get(field) or '').casefold() for field in ('title','isbn13','customer_name'))]
        titles,months,buyers = {},{},{}
        zero = lambda: dict(grossUnits=Decimal(0),returns=Decimal(0),unitAdjustments=Decimal(0),grossSales=Decimal(0),netReceipts=Decimal(0))
        for row in records:
            title = titles.setdefault(str(row['edition_id']),dict(editionId=str(row['edition_id']),title=row['title'],isbn=row['isbn13'],format=row['product_form'],**zero()))
            month = months.setdefault(row['period_code'],dict(period=row['period_code'],**zero()))
            targets = [title,month]
            if row['customer_name'] and (row['units_sold'] or row['units_returned'] or row['gross_sales'] or row['unit_adjustment']):
                key = (row['customer_name'],row['territory'])
                targets.append(buyers.setdefault(key,dict(buyer=row['customer_name'],territory=row['territory'],**zero())))
            for target in targets:
                for key,source in [('grossUnits','units_sold'),('returns','units_returned'),('unitAdjustments','unit_adjustment'),('grossSales','gross_sales'),('netReceipts','publisher_receipts')]:
                    target[key] += Decimal(str(row[source] or 0))
        cur.execute("""SELECT s.edition_id,s.available,s.as_of
          FROM stable_inventory_snapshots s JOIN stable_imports i ON i.tenant_id=s.tenant_id AND i.id=s.import_id
          JOIN editions e ON e.tenant_id=s.tenant_id AND e.id=s.edition_id
          JOIN works w ON w.tenant_id=e.tenant_id AND w.id=e.work_id
          WHERE s.tenant_id=%s AND i.status='posted'
          AND s.as_of=(SELECT max(t.as_of) FROM stable_inventory_snapshots t JOIN stable_imports j
            ON j.tenant_id=t.tenant_id AND j.id=t.import_id WHERE t.tenant_id=%s AND j.status='posted')
          AND (%s='' OR w.id::text=%s OR COALESCE(w.uid::text,'')=%s)""",(tenant,tenant,work_id,work_id,work_id))
        stock = {str(r['edition_id']):r for r in cur.fetchall()}
        for row in titles.values():
            snapshot = stock.get(row['editionId'])
            row['inventory'] = snapshot['available'] if snapshot else None
            row['inventoryAsOf'] = snapshot['as_of'] if snapshot else None
            row['netUnits'] = row['grossUnits']-row['returns']+row['unitAdjustments']
            row['bestsellerRank']=ranks.get(row['editionId'])
        today=datetime.now(ZoneInfo('America/Los_Angeles')).date()
        implied,incomplete=channel_stock(cur,tenant,f'{today:%Y-%m}',work_id)
        ranked = sorted(buyers.values(),key=lambda b:(-b['grossUnits'],b['buyer'].casefold(),b['territory']))
        for rank,row in enumerate(ranked,1):
            balance=implied.get((row['buyer'],row['territory']))
            amazon=row['buyer']=='Amazon (paid sales)'
            row.update(rank=rank,netUnits=row['grossUnits']-row['returns']+row['unitAdjustments'],
              impliedStock=balance if amazon and balance is not None and not incomplete else None,
              impliedStockAsOf=today.isoformat() if amazon else None,
              impliedStockNote=('Statements awaiting review; stock unavailable' if incomplete else 'Estimated; zero opening stock assumed') if amazon else 'Not a stock-holding account')
        for row in months.values():
            row['netUnits']=row['grossUnits']-row['returns']+row['unitAdjustments']
        cur.execute('SELECT id,object_key,report_type,territory,period_code,as_of,status,issues,created_at,posted_at FROM stable_imports WHERE tenant_id=%s ORDER BY created_at DESC LIMIT 100',(tenant,))
        imports = list(cur.fetchall())
        if lower:
            imports=[i for i in imports if lower<=str(i['period_code'] or i['as_of'] or '')[:7]<=upper]
        if territory!='ALL':
            imports=[i for i in imports if i['territory'] in (None,territory)]
        cur.execute('SELECT max(posted_at) AS last_sync FROM stable_imports WHERE tenant_id=%s AND status=%s',(tenant,'posted'))
        last_sync = cur.fetchone()['last_sync']
        totals = zero()
        for row in titles.values():
            for key in totals:
                totals[key] += row[key]
        totals['netUnits'] = totals['grossUnits']-totals['returns']+totals['unitAdjustments']
        return dict(available=True,currency=currency,totals=totals,rows=list(titles.values()),
                    inventory=dict(available=sum(r['available'] for r in stock.values()) if stock else None,
                      asOf=max((r['as_of'] for r in stock.values()),default=None),
                      basis='Latest accepted distributor warehouse report; independent of sales period and territory'),
                    months=sorted(months.values(),key=lambda m:m['period']),buyers=ranked,
                    imports=imports,lastSync=last_sync,schedule='00:05 America/Los_Angeles',
                    scopeLabel='Accepted Stable statements only',
                    buyerReceiptBasis='Before monthly ISBN storage and technology allocations')
