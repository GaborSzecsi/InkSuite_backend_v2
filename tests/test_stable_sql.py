"""Exercise real PostgreSQL statements in disposable PGlite, never AWS."""
import contextlib
import json
import subprocess
import os
import unittest
from unittest.mock import patch
from datetime import date,datetime
from decimal import Decimal
from pathlib import Path
from uuid import UUID,uuid4

from psycopg.types.json import Jsonb
from app.stable import service
from app.stable.parser import ReviewRequired,parse_book,parse,read_workbook
from test_stable_parser import source

proc=None


def rpc(sql,params=(),execute=False):
    def value(v):
        if isinstance(v,Jsonb): return v.obj
        if isinstance(v,(date,datetime)): return v.isoformat()
        if isinstance(v,(UUID,Decimal)): return str(v)
        return v
    proc.stdin.write(json.dumps(dict(sql=sql,params=[value(v) for v in params],exec=execute))+'\n');proc.stdin.flush()
    result=json.loads(proc.stdout.readline())
    if 'error' in result: raise RuntimeError(result['error'])
    return result['result']


class Cursor:
    def execute(self,sql,params=()):
        for i in range(len(params)): sql=sql.replace('%s',f'${i+1}',1)
        self.rows=rpc(sql,params)['rows'];return self
    def fetchone(self): return self.rows[0] if self.rows else None
    def fetchall(self): return self.rows
    def __enter__(self): return self
    def __exit__(self,*args): pass


class Connection:
    def cursor(self,**kwargs): return Cursor()


@contextlib.contextmanager
def database(): yield Connection()


class StableSQLTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        global proc
        proc=subprocess.Popen(['node',str(Path(__file__).with_name('stable_pg.cjs'))],stdin=subprocess.PIPE,stdout=subprocess.PIPE,text=True,encoding='utf-8')
        status=json.loads(proc.stdout.readline());assert status.get('ready'),status
    @classmethod
    def tearDownClass(cls):
        proc.stdin.close();proc.wait(timeout=20);proc.stdout.close()
    def setUp(self):
        self.tenant=str(uuid4());self.work=str(uuid4());self.edition=str(uuid4());self.cur=Cursor()
        rpc('INSERT INTO tenants VALUES($1)',[self.tenant])
        rpc("INSERT INTO works VALUES($1,$2,'Example','test')",[self.work,self.tenant])
        rpc("INSERT INTO editions VALUES($1,$2,$3,'9780306406157','Hardcover','Hardcover')",[self.edition,self.tenant,self.work])
        rpc("INSERT INTO stable_sources(tenant_id,bucket,key_prefix,enabled,us_currency,ca_currency) VALUES($1,'bucket','tenant/',true,'USD','USD')",[self.tenant])
    def register(self):
        return service.register(self.cur,self.tenant,'bucket','tenant/march.xlsx','v1',parse_book(source(),str(uuid4())))
    def test_posting_reuses_sales_and_separates_once_only_costs(self):
        item=self.register();self.assertEqual(item['status'],'validated')
        self.assertTrue(service.post(self.cur,self.tenant,item['id']))
        self.assertFalse(service.post(self.cur,self.tenant,item['id']))
        sale=rpc('SELECT * FROM royalty_sales_lines WHERE tenant_id=$1',[self.tenant])['rows']
        self.assertEqual(len(sale),1);self.assertEqual(sale[0]['customer_name'],'Bookshop')
        self.assertIsNone(sale[0]['discount_percent'])
        costs=rpc('SELECT sum(amount) AS total FROM stable_receipt_adjustments WHERE tenant_id=$1',[self.tenant])['rows'][0]
        self.assertEqual(Decimal(costs['total']),Decimal('.57'))
        receipts=Decimal(sale[0]['publisher_receipts'])-Decimal(costs['total'])
        self.assertEqual(receipts,Decimal('12.62'))
        rows=service.royalty_adjustments(self.cur,self.tenant,self.work,date(2026,3,1),date(2026,3,31))
        self.assertEqual(sum(Decimal(r['publisher_receipts']) for r in rows),Decimal('-.57'))
    def test_content_idempotency_and_cross_tenant_isolation(self):
        report=parse_book(source(),'identical')
        a=service.register(self.cur,self.tenant,'bucket','one','1',report)
        self.assertIsNone(service.register(self.cur,self.tenant,'bucket','two','2',report))
        self.assertFalse(service.post(self.cur,str(uuid4()),a['id']))
    def test_revised_statement_requires_review(self):
        first=self.register();second=self.register()
        self.assertEqual(first['status'],'validated');self.assertEqual(second['status'],'needs_review')
    def test_unknown_isbn_cannot_post(self):
        book=source();book['b'][1][4]='9781958325391'
        item=service.register(self.cur,self.tenant,'bucket','unknown','1',parse_book(book,str(uuid4())))
        self.assertEqual(item['status'],'needs_review');self.assertFalse(service.post(self.cur,self.tenant,item['id']))
    def test_import_preserves_issued_statement_and_closed_period(self):
        item=self.register()
        rpc("INSERT INTO royalty_periods(tenant_id,period_code,period_start,period_end) VALUES($1,'2026-03','2026-03-01','2026-03-31')",[self.tenant])
        rpc("INSERT INTO royalty_statements(tenant_id,period_id,status) SELECT tenant_id,id,'final' FROM royalty_periods WHERE tenant_id=$1",[self.tenant])
        rpc('UPDATE royalty_periods SET is_closed=true WHERE tenant_id=$1',[self.tenant])
        before=rpc('SELECT * FROM royalty_statements WHERE tenant_id=$1',[self.tenant])['rows']
        self.assertTrue(service.post(self.cur,self.tenant,item['id']))
        self.assertEqual(rpc('SELECT * FROM royalty_statements WHERE tenant_id=$1',[self.tenant])['rows'],before)
        self.assertTrue(rpc('SELECT is_closed FROM royalty_periods WHERE tenant_id=$1',[self.tenant])['rows'][0]['is_closed'])
    def test_combined_workbook_has_independent_territory_imports(self):
        import copy
        report=parse_book(source(),'combined-source')
        ca=copy.deepcopy(report);ca.territory='CA'
        a=service.register(self.cur,self.tenant,'bucket','combined.xlsx','1',report)
        b=service.register(self.cur,self.tenant,'bucket','combined.xlsx','1',ca)
        self.assertNotEqual(a['id'],b['id'])
        self.assertEqual((a['status'],b['status']),('validated','validated'))

    def test_previous_distributor_activity_does_not_block_stable(self):
        item=self.register();service.post(self.cur,self.tenant,item['id'])
        # A second, independent tenant has historical activity in the same month.
        oldtenant,oldwork,oldedition=self.tenant,self.work,self.edition
        self.setUp()
        rpc("INSERT INTO royalty_periods(tenant_id,period_code,period_start,period_end) VALUES($1,'2026-03','2026-03-01','2026-03-31')",[self.tenant])
        rpc("INSERT INTO royalty_sales_lines(tenant_id,work_id,edition_id,period_id,source_system,units_sold,gross_sales,publisher_receipts,currency) SELECT $1,$2,$3,id,'previous_distributor',5,50,50,'USD' FROM royalty_periods WHERE tenant_id=$1",[self.tenant,self.work,self.edition])
        item=self.register();self.assertEqual(item['status'],'validated')
        self.assertTrue(service.post(self.cur,self.tenant,item['id']))
        self.assertEqual(len(rpc('SELECT id FROM royalty_sales_lines WHERE tenant_id=$1',[self.tenant])['rows']),2)

    def test_compatibility_view_preserves_canada_history(self):
        rpc("INSERT INTO fraser_ca_sales_lines(tenant_id,customer_name) VALUES($1,'Historical Canadian buyer')",[self.tenant])
        self.assertEqual(rpc('SELECT customer_name FROM canada_sales_lines WHERE tenant_id=$1',[self.tenant])['rows'][0]['customer_name'],'Historical Canadian buyer')
    def test_dashboard_uses_adjusted_receipts_and_buyer_ranking(self):
        from app.stable import router
        item=self.register();service.post(self.cur,self.tenant,item['id'])
        ctx={'tenant':{'id':self.tenant}}
        with patch.object(router,'db_conn',database):
            data=router.overview(year='2026',territory='ALL',currency='USD',q='',work_id='',ctx=ctx)
            self.assertEqual(data['totals']['netReceipts'],Decimal('12.62'))
            self.assertEqual(data['buyers'][0]['rank'],1)
            self.assertEqual(data['buyers'][0]['netReceipts'],Decimal('13.19'))
            self.assertEqual(data['months'][0]['netUnits'],2)
            other=router.overview(year='2026',territory='ALL',currency='CAD',q='',work_id='',ctx=ctx)
            self.assertEqual(other['rows'],[])
    def test_adjustments_stock_period_and_historical_exclusion(self):
        from app.stable import router
        book=source()
        book['c'][1]=['9780306406157','Example',5,-1,50,-10,40,.1,-4,36]
        report=parse_book(book,str(uuid4()))
        self.assertEqual(report.issues,[])
        report.movements=[dict(isbn='9780306406157',territory='US',buyer='Amazon (paid sales)',outbound=Decimal(20),returned=Decimal(2))]
        item=service.register(self.cur,self.tenant,'bucket','march','1',report)
        service.post(self.cur,self.tenant,item['id'])
        # Historical receipts and gross are not verified Stable financials.
        rpc("INSERT INTO royalty_sales_lines(tenant_id,work_id,edition_id,period_id,source_system,units_sold,gross_sales,publisher_receipts,currency) SELECT tenant_id,work_id,edition_id,period_id,'old',10000,10000,10000,'USD' FROM royalty_sales_lines WHERE tenant_id=$1 LIMIT 1",[self.tenant])
        ctx={'tenant':{'id':self.tenant}}
        with patch.object(router,'db_conn',database):
            data=router.overview(year='2026',territory='ALL',currency='USD',q='',work_id='',ctx=ctx,period='month',month='03')
            self.assertEqual(data['totals']['grossUnits'],7)
            self.assertEqual(data['totals']['unitAdjustments'],-1)
            self.assertEqual(data['totals']['returns'],0)
            self.assertEqual(data['totals']['netUnits'],6)
            self.assertEqual(data['totals']['netReceipts'],Decimal('48.62'))
            amazon=next(b for b in data['buyers'] if b['buyer']=='Amazon (paid sales)')
            self.assertEqual(amazon['impliedStock'],14)
            april={key:[[v.replace('202603','202604').replace('March','April') if isinstance(v,str) else v for v in row] for row in rows] for key,rows in source().items()}
            april['c'][1]=['9780306406157','Example',3,0,30,0,30,.1,-3,27]
            item=service.register(self.cur,self.tenant,'bucket','april','1',parse_book(april,str(uuid4())))
            service.post(self.cur,self.tenant,item['id'])
            rpc("INSERT INTO stable_inventory_snapshots(tenant_id,import_id,edition_id,as_of,available,ending) VALUES($1,$2,$3,'2026-04-30',7,9)",[self.tenant,item['id'],self.edition])
            data=router.overview(year='2026',territory='ALL',currency='USD',q='',work_id='',ctx=ctx,period='month',month='04')
            self.assertEqual(data['inventory']['available'],7)
            march=router.overview(year='2026',territory='ALL',currency='USD',q='',work_id='',ctx=ctx,period='month',month='03')
            self.assertEqual(march['inventory']['available'],7)
            amazon=next(b for b in data['buyers'] if b['buyer']=='Amazon (paid sales)')
            self.assertEqual(amazon['grossUnits'],3)
            self.assertEqual(amazon['impliedStock'],11)
            march_amazon=next(b for b in march['buyers'] if b['buyer']=='Amazon (paid sales)')
            self.assertEqual(march_amazon['impliedStock'],11)

    def test_selected_book_keeps_rank_against_all_books(self):
        from app.stable import router
        item=self.register();service.post(self.cur,self.tenant,item['id'])
        other_work,other_edition=str(uuid4()),str(uuid4())
        rpc("INSERT INTO works VALUES($1,$2,'Bestseller','bestseller')",[other_work,self.tenant])
        rpc("INSERT INTO editions VALUES($1,$2,$3,'9781958325391','Hardcover','Hardcover')",[other_edition,self.tenant,other_work])
        rpc("INSERT INTO royalty_sales_lines(tenant_id,work_id,edition_id,period_id,source_system,reference_number,customer_name,units_sold,gross_sales,publisher_receipts,currency) SELECT tenant_id,$2,$3,period_id,'stable',reference_number || ':other','Other buyer',100,1000,500,'USD' FROM royalty_sales_lines WHERE tenant_id=$1 LIMIT 1",[self.tenant,other_work,other_edition])
        with patch.object(router,'db_conn',database):
            data=router.overview(year='2026',territory='ALL',currency='USD',q='',work_id=self.work,ctx={'tenant':{'id':self.tenant}})
        self.assertEqual(len(data['rows']),1)
        self.assertEqual(data['rows'][0]['bestsellerRank'],2)
        self.assertEqual(data['totals']['grossUnits'],2)

    def test_deleted_inventory_duplicate_releases_remaining_file_only(self):
        from app.stable.worker import reconcile_removed_inventory
        data={'inventory':[['ISBN (Item Code)','Default Useable Balance Qty','Ending Balance Qty'],['9780306406157',8,10]]}
        a=service.register(self.cur,self.tenant,'bucket','tenant/removed.xlsx','1',parse_book(data,'removed',inventory_date='2026-03-01'))
        b=service.register(self.cur,self.tenant,'bucket','tenant/kept.xlsx','1',parse_book(data,'kept',inventory_date='2026-03-01'))
        rpc("UPDATE stable_imports SET status='needs_review' WHERE tenant_id=$1",[self.tenant])
        old=service.register(self.cur,self.tenant,'bucket','tenant/posted.xlsx','1',parse_book(data,'posted',inventory_date='2026-02-28'))
        rpc("UPDATE stable_imports SET status='posted' WHERE id=$1",[old['id']])
        reconcile_removed_inventory(self.cur,dict(tenant_id=self.tenant,bucket='bucket',key_prefix='tenant/'),{'tenant/kept.xlsx'})
        statuses={r['object_key']:r['status'] for r in rpc('SELECT object_key,status FROM stable_imports WHERE tenant_id=$1',[self.tenant])['rows']}
        self.assertEqual(statuses['tenant/removed.xlsx'],'superseded')
        self.assertEqual(statuses['tenant/posted.xlsx'],'posted')
        kept=service.register(self.cur,self.tenant,'bucket','tenant/kept.xlsx','1',parse_book(data,'kept',inventory_date='2026-03-01'))
        self.assertEqual(kept['status'],'validated')

    def test_historical_inventory_fills_gap_without_replacing_latest(self):
        data={'inventory':[['ISBN (Item Code)','Default Useable Balance Qty','Ending Balance Qty'],['9780306406157',8,10]]}
        newer=service.register(self.cur,self.tenant,'bucket','tenant/newer.xlsx','1',parse_book(data,'newer',inventory_date='2026-03-02'))
        older=service.register(self.cur,self.tenant,'bucket','tenant/older.xlsx','1',parse_book(data,'older',inventory_date='2026-03-01'))
        self.assertTrue(service.post(self.cur,self.tenant,newer['id']))
        self.assertTrue(service.post(self.cur,self.tenant,older['id']))
        result=rpc('SELECT max(as_of)::text AS latest,count(*) AS rows FROM stable_inventory_snapshots WHERE tenant_id=$1',[self.tenant])['rows'][0]
        self.assertEqual(result['latest'],'2026-03-02')
        self.assertEqual(result['rows'],2)

    def test_pacific_ytd_and_month_bounds(self):
        from app.stable.router import period_bounds
        self.assertEqual(period_bounds('ytd','2025','',date(2026,7,4)),('2025-01','2025-07'))
        self.assertEqual(period_bounds('month','2026','2'),('2026-02','2026-02'))

    def test_daily_identical_balance_on_different_dates_keeps_history(self):
        book={'inventory':[['ISBN (Item Code)','Default Useable Balance Qty','Ending Balance Qty'],['9780306406157',8,10]]}
        a=service.register(self.cur,self.tenant,'bucket','one','1',parse_book(book,'same-bytes',inventory_date='2026-03-01'))
        b=service.register(self.cur,self.tenant,'bucket','two','2',parse_book(book,'same-bytes',inventory_date='2026-03-02'))
        self.assertNotEqual(a['id'],b['id'])
    def test_currency_conversion_is_not_silently_assumed(self):
        item=self.register();service.post(self.cur,self.tenant,item['id'])
        rpc("UPDATE stable_receipt_adjustments SET currency='CAD' WHERE tenant_id=$1",[self.tenant])
        with self.assertRaises(ReviewRequired):service.royalty_adjustments(self.cur,self.tenant,self.work,date(2026,3,1),date(2026,3,31))
    def test_sql_keeps_no_raw_percentage_or_duplicate_posted_payload(self):
        item=self.register()
        payload=rpc('SELECT payload FROM stable_imports WHERE id=$1',[item['id']])['rows'][0]['payload']
        self.assertNotIn('controls',payload)
        self.assertNotIn('raw',payload['sales'][0])
        self.assertNotIn('discount_percent',payload['sales'][0])
        self.assertNotIn('sales_price',payload['sales'][0])
        service.post(self.cur,self.tenant,item['id'])
        row=rpc('SELECT payload FROM stable_imports WHERE id=$1',[item['id']])['rows'][0]
        self.assertEqual(row,dict(payload={}))
    def test_discount_is_retained_only_when_a_royalty_rule_needs_it(self):
        ids=[str(uuid4()) for _ in range(4)]
        rpc('INSERT INTO royalty_sets VALUES($1,$2,$3,true)',[ids[0],self.tenant,self.work])
        rpc('INSERT INTO royalty_rules VALUES($1,$2,$3)',[ids[1],self.tenant,ids[0]])
        rpc('INSERT INTO royalty_tiers VALUES($1,$2,$3)',[ids[2],self.tenant,ids[1]])
        rpc("INSERT INTO royalty_tier_conditions VALUES($1,$2,$3,'discount')",[ids[3],self.tenant,ids[2]])
        item=self.register();service.post(self.cur,self.tenant,item['id'])
        self.assertEqual(Decimal(rpc('SELECT discount_percent FROM royalty_sales_lines WHERE tenant_id=$1',[self.tenant])['rows'][0]['discount_percent']),50)
    def test_book_information_receives_adjusted_receipts_and_daily_stock(self):
        from routers import financials
        item=self.register();service.post(self.cur,self.tenant,item['id'])
        book={'inventory':[['ISBN (Item Code)','Default Useable Balance Qty','Ending Balance Qty'],['9780306406157',8,10]]}
        stock=service.register(self.cur,self.tenant,'bucket','daily','v1',parse_book(book,'inventory-check',inventory_date='2026-03-01'))
        rpc("UPDATE stable_imports SET status='posted',payload='{}' WHERE id=$1",[stock['id']])
        rpc("INSERT INTO stable_inventory_snapshots VALUES($1,$2,$3,'2026-03-01',8,10)",[self.tenant,stock['id'],self.edition])
        with patch.object(financials,'db_conn',database),patch.object(financials,'_get_tenant_id',return_value=self.tenant):
            kpis=financials.get_book_kpis(bookUid=self.work,mode='MONTH',year=2026,month='03',season=None,format='ALL')
            self.assertEqual(kpis['adjustedReceipts'],12.62)
            self.assertEqual(kpis['inventoryEnd'],8)
            self.assertEqual(kpis['inventorySource'],'daily_report')
            stats=financials.get_book_format_stats(bookUid=self.work,year=2026)
            self.assertEqual(stats['formats']['HC']['inventoryEnd'],8)

    def test_rounding_allocation_is_signed_and_preserves_control_total(self):
        for values in ({'a':Decimal('.005'),'b':Decimal('.005')},{'a':Decimal('-.005'),'b':Decimal('-.005')}):
            result=service.allocate_cents(values)
            self.assertEqual(sum(result.values()),service.cents(sum(values.values())))
            self.assertEqual(result,service.allocate_cents(values))

    @unittest.skipUnless(os.getenv('STABLE_ACCEPTANCE_DIR'),'Private originals supplied separately')
    def test_canadian_september_posts_net_receipts_with_companion_provenance(self):
        from app.stable.parser import parse_reports
        root=Path(os.environ['STABLE_ACCEPTANCE_DIR'])
        detail=root/'MARBLE PRESS_Hachette_Sales_Detail_202609.xlsx'
        if not detail.exists():self.skipTest('Original Hachette detail supplied separately')
        us=read_workbook((root/'MARBLE PRESS_Client_Statement_202603.xlsx').read_bytes())
        ca=read_workbook((root/'MARBLE PRESS_Client_Statement_Canada_202603.xlsx').read_bytes())
        reports=parse_reports(read_workbook((root/'MARBLE PRESS_Client_Statement_202609.xlsx').read_bytes()),
          transaction_detail={detail.name+' / '+k:v for k,v in read_workbook(detail.read_bytes()).items()},
          reserve_origins={'US':parse_book(us),'CA':parse_book(ca,companion=us)})
        report=reports[1];report.digest='canada-september-original'
        self.assertEqual(report.issues,[])
        for code in {s.isbn for s in report.sales}|{c['isbn'] for c in report.costs}:
            rpc("INSERT INTO editions VALUES($1,$2,$3,$4,'Hardcover','Hardcover')",[str(uuid4()),self.tenant,self.work,code])
        item=service.register(self.cur,self.tenant,'bucket','statement-september','v1',report)
        self.assertTrue(service.post(self.cur,self.tenant,item['id']))
        row=rpc('SELECT sum(units_sold) AS sold,sum(units_returned) AS returned,sum(unit_adjustment) AS adjusted,sum(publisher_receipts) AS cash FROM canada_sales_lines WHERE tenant_id=$1',[self.tenant])['rows'][0]
        costs=rpc('SELECT sum(amount) AS amount FROM stable_receipt_adjustments WHERE tenant_id=$1',[self.tenant])['rows'][0]['amount']
        self.assertEqual(Decimal(row['cash'])-Decimal(costs),Decimal('1565.00'))
        self.assertEqual(Decimal(row['sold']),360);self.assertEqual(Decimal(row['returned']),6)
        evidence=rpc("SELECT count(*) AS n FROM canada_sales_lines WHERE tenant_id=$1 AND reference_number LIKE '%Hachette_Sales_Detail_202609.xlsx%'",[self.tenant])['rows'][0]['n']
        self.assertEqual(evidence,116)

    def test_late_companion_revalidates_same_parser_without_rewriting_posted(self):
        report=parse_book(source(),'late-companion')
        report.issues.append('Missing matching companion')
        first=service.register(self.cur,self.tenant,'bucket','source','v1',report)
        self.assertEqual(first['status'],'needs_review')
        ready=service.register(self.cur,self.tenant,'bucket','source','v1',parse_book(source(),'late-companion'))
        self.assertEqual(ready['id'],first['id']);self.assertEqual(ready['status'],'validated')
        service.post(self.cur,self.tenant,ready['id'])
        self.assertIsNone(service.register(self.cur,self.tenant,'bucket','source','v1',report))
        self.assertEqual(rpc('SELECT status FROM stable_imports WHERE id=$1',[ready['id']])['rows'][0]['status'],'posted')

    def post_original(self,month,territory):
        root=Path(os.environ['STABLE_ACCEPTANCE_DIR'])
        us=root/('MARBLE PRESS_Client_Statement_'+month+('_v2' if month=='202607' else '')+'.xlsx')
        report=parse(us.read_bytes()) if territory=='US' else parse((root/f'MARBLE PRESS_Client_Statement_Canada_{month}.xlsx').read_bytes(),companion=read_workbook(us.read_bytes()))
        self.assertEqual(report.issues,[])
        codes={s.isbn for s in report.sales}|{c['isbn'] for c in report.costs}
        for code in codes:
            rpc("INSERT INTO editions VALUES($1,$2,$3,$4,'Hardcover','Hardcover')",[str(uuid4()),self.tenant,self.work,code])
        item=service.register(self.cur,self.tenant,'bucket','original-source','v1',report)
        self.assertTrue(service.post(self.cur,self.tenant,item['id']))
        table='royalty_sales_lines' if territory=='US' else 'canada_sales_lines'
        sale=rpc(f'SELECT sum(gross_sales) AS gross,sum(publisher_receipts) AS cash FROM {table} WHERE tenant_id=$1',[self.tenant])['rows'][0]
        costs=rpc('SELECT sum(amount) AS total FROM stable_receipt_adjustments WHERE tenant_id=$1',[self.tenant])['rows'][0]['total']
        return Decimal(sale['gross']),Decimal(sale['cash'])-Decimal(costs)

    @unittest.skipUnless(os.getenv('STABLE_ACCEPTANCE_DIR'),'Private originals supplied separately')
    def test_original_march_is_correct_after_actual_two_decimal_sql_posting(self):
        self.assertEqual(self.post_original('202603','US'),(Decimal('2309.86'),Decimal('1051.54')))

    @unittest.skipUnless(os.getenv('STABLE_ACCEPTANCE_DIR'),'Private originals supplied separately')
    def test_original_july_canada_conversion_does_not_lose_a_cent(self):
        self.assertEqual(self.post_original('202607','CA'),(Decimal('143.70'),Decimal('89.34')))


if __name__=='__main__':unittest.main()
