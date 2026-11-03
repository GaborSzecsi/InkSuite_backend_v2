from datetime import date
from decimal import Decimal

from fastapi import APIRouter, Depends, Query
from psycopg.rows import dict_row

from app.core.db import db_conn
from app.distribution.router import access
from .service import ready

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
       SELECT r.id,r.tenant_id,r.edition_id,r.period_id,r.customer_name,r.units_sold,r.units_returned,
         r.gross_sales,r.publisher_receipts,r.currency,'US'::text AS territory FROM royalty_sales_lines r
       UNION ALL
       SELECT r.id,r.tenant_id,r.edition_id,r.period_id,r.customer_name,r.units_sold,r.units_returned,
         r.gross_sales,r.publisher_receipts,r.currency,'CA'::text FROM canada_sales_lines r
       UNION ALL
       SELECT a.id,a.tenant_id,a.edition_id,a.period_id,NULL,0,0,0,-a.amount,a.currency,a.territory
       FROM stable_receipt_adjustments a JOIN stable_imports i ON i.tenant_id=a.tenant_id AND i.id=a.import_id AND i.status='posted'
     ) SELECT s.*,e.isbn13,e.product_form,e.work_id,w.title,p.period_code
       FROM sales s JOIN editions e ON e.tenant_id=s.tenant_id AND e.id=s.edition_id
       JOIN works w ON w.tenant_id=e.tenant_id AND w.id=e.work_id
       JOIN royalty_periods p ON p.tenant_id=s.tenant_id AND p.id=s.period_id
       WHERE s.tenant_id=%s AND (%s='' OR left(p.period_code,4)=%s)
       AND (%s='ALL' OR s.territory=%s) AND s.currency=%s
       AND (%s='' OR w.title ILIKE %s OR e.isbn13 ILIKE %s OR s.customer_name ILIKE %s)
       AND (%s='' OR e.work_id::text=%s OR COALESCE(w.uid::text,'')=%s)''',
       (tenant,year,year,territory,territory,currency,search,'%'+search+'%','%'+search+'%','%'+search+'%',work_id,work_id,work_id))
    return list(cur.fetchall())


@router.get('/overview')
def overview(year: str=Query('',pattern=r'^(|20\d{2})$'),territory: str=Query('ALL',pattern='^(ALL|US|CA)$'),
             currency: str=Query('USD',pattern='^[A-Z]{3}$'),q: str=Query('',max_length=200),
             work_id: str=Query('',max_length=64), ctx=Depends(access)):
    tenant = ctx['tenant']['id']
    with db_conn() as conn,conn.cursor(row_factory=dict_row) as cur:
        if not ready(cur):
            return dict(available=False,detail='Stable ingestion migration has not been applied.',imports=[],rows=[],months=[],buyers=[])
        records = ledger(cur,tenant,year,territory,currency,q,work_id)
        titles,months,buyers = {},{},{}
        zero = lambda: dict(grossUnits=Decimal(0),returns=Decimal(0),grossSales=Decimal(0),netReceipts=Decimal(0))
        for row in records:
            title = titles.setdefault(str(row['edition_id']),dict(editionId=str(row['edition_id']),title=row['title'],isbn=row['isbn13'],format=row['product_form'],**zero()))
            month = months.setdefault(row['period_code'],dict(period=row['period_code'],**zero()))
            targets = [title,month]
            if row['customer_name'] and (row['units_sold'] or row['units_returned'] or row['gross_sales']):
                key = (row['customer_name'],row['territory'])
                targets.append(buyers.setdefault(key,dict(buyer=row['customer_name'],territory=row['territory'],**zero())))
            for target in targets:
                for key,source in [('grossUnits','units_sold'),('returns','units_returned'),('grossSales','gross_sales'),('netReceipts','publisher_receipts')]:
                    target[key] += Decimal(str(row[source] or 0))
        cur.execute("""SELECT DISTINCT ON(s.edition_id) s.edition_id,s.available,s.as_of
          FROM stable_inventory_snapshots s JOIN stable_imports i ON i.tenant_id=s.tenant_id AND i.id=s.import_id
          WHERE s.tenant_id=%s AND i.status='posted' ORDER BY s.edition_id,s.as_of DESC""",(tenant,))
        stock = {str(r['edition_id']):r for r in cur.fetchall()}
        for row in titles.values():
            snapshot = stock.get(row['editionId'])
            row['inventory'] = snapshot['available'] if snapshot else None
            row['inventoryAsOf'] = snapshot['as_of'] if snapshot else None
            row['netUnits'] = row['grossUnits']-row['returns']
        ranked = sorted(buyers.values(),key=lambda b:(-b['grossUnits'],b['buyer'].casefold(),b['territory']))
        for rank,row in enumerate(ranked,1):
            row.update(rank=rank,netUnits=row['grossUnits']-row['returns'])
        for row in months.values():
            row['netUnits']=row['grossUnits']-row['returns']
        cur.execute('SELECT id,object_key,report_type,territory,period_code,as_of,status,issues,created_at,posted_at FROM stable_imports WHERE tenant_id=%s ORDER BY created_at DESC LIMIT 100',(tenant,))
        imports = list(cur.fetchall())
        cur.execute('SELECT max(posted_at) AS last_sync FROM stable_imports WHERE tenant_id=%s AND status=%s',(tenant,'posted'))
        last_sync = cur.fetchone()['last_sync']
        totals = zero()
        for row in titles.values():
            for key in totals:
                totals[key] += row[key]
        totals['netUnits'] = totals['grossUnits']-totals['returns']
        return dict(available=True,currency=currency,totals=totals,rows=list(titles.values()),
                    months=sorted(months.values(),key=lambda m:m['period']),buyers=ranked,
                    imports=imports,lastSync=last_sync,schedule='00:05 America/Los_Angeles',
                    buyerReceiptBasis='Before monthly ISBN storage and technology allocations')
