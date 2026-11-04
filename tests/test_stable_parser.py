import copy
import os
import unittest
from decimal import Decimal
from pathlib import Path

from app.stable.parser import ReviewRequired, isbn, parse_book, parse, read_workbook


def source():
    return {
      'renamed summary': [['Stable March 2026 Monthly Settlement Summary'],['Fiscal Period: 202603']],
      'a': [['Fiscal Period: 202603'],['Client_Code','Acct Name','Sale Type','Gross Units','Return Units','Comp Units','Gross Sales $','Actual Return $','Net Sales $','Distribution Fee %','Distribution Fee $','Returns Reserve %','Returns Reserve $','Fulfillment Fee $','Returns Fee $'],
            ['19','Bookshop','Domestic Regular',2,0,0,20,0,20,.15,-3,.15,-2.55,-1.26,0]],
      'b': [['Date','Account','Sale Type','Invoice #','ISBN','Title','Gross Qty','Return Qty','Comp Qty','Gross Sales $','Returns $','Net Sales $'],
            ['2026-03-03','Bookshop','Domestic Regular','1','9780306406157','Example',2,0,0,20,0,20]],
      'c': [['ISBN','Title','Units Sold','Adj Units','Gross Sales $','Adjustments $','Net Sales $','Distribution Fee %','Distribution Fee $','Net Due to Client'],['No transactions this period.']],
      'd': [['Source','Retailer / Customer','ISBN','Title','Qty','Gross Sales $','Distribution Fee %','Distribution Fee $','Net Due to Client'],['No transactions this period.']],
      'e': [['ISBN','Ending Balance Qty','Monthly Storage Charge','US Price'],['9780306406157',10,.07,20]],
    }


class StableParserTests(unittest.TestCase):
    def test_source_precision_and_once_per_isbn_costs(self):
        r = parse_book(source())
        self.assertEqual(r.issues, [])
        self.assertEqual(r.totals('US')['publisher_receipts'],Decimal('12.62'))
        self.assertEqual(r.sales[0].discount_percent,Decimal('50'))

    def test_shifted_reordered_columns_and_renamed_tabs(self):
        book = source()
        for key,rows in list(book.items()):
            if key!='renamed summary':
                book[key] = [[None],['Unrelated heading']]+[list(reversed(row))+[None] for row in rows]
        self.assertEqual(parse_book(book).totals('US'),parse_book(source()).totals('US'))

    def test_negative_gross_row_is_return_when_account_return_control_agrees(self):
        book=source()
        book['a'][2]=['19','Bookshop','Domestic Regular',2,-1,0,20,-10,10,.15,-1.5,.15,-1.275,-1.26,-.63]
        book['b'].append(['2026-03-04','Bookshop','Domestic Regular','2','9780306406157','Example',-1,0,0,-10,0,-10])
        report=parse_book(book)
        self.assertEqual(report.issues,[])
        self.assertEqual(report.totals('US')['units_sold'],2)
        self.assertEqual(report.totals('US')['units_returned'],1)
        self.assertEqual(report.totals('US')['gross_sales'],20)
        self.assertEqual(report.sales[1].discount_percent,Decimal(50))

    def test_negative_gross_row_remains_adjustment_when_no_return_is_reported(self):
        book=source()
        book['a'][2]=['19','Bookshop','Domestic Regular',2,0,0,10,0,10,.15,-1.5,.15,-1.275,-1.26,0]
        book['b'].append(['2026-03-04','Bookshop','Domestic Regular','2','9780306406157','Example',-1,0,0,-10,0,-10])
        report=parse_book(book)
        self.assertEqual(report.issues,[])
        self.assertEqual(report.sales[1].unit_adjustment,-1)
        self.assertEqual(report.totals('US')['units_returned'],0)
        self.assertEqual(report.totals('US')['units_sold'],2)
        self.assertEqual(report.sales[1].handling,0)

    def test_signed_amazon_money_adjustment_changes_gross_once(self):
        book=source();book['c'][1]=['9780306406157','Example',3,-1,30,-10,20,.1,-2,18]
        report=parse_book(book)
        self.assertEqual(report.issues,[])
        self.assertEqual(report.totals('US')['gross_sales'],40)
        self.assertEqual(report.totals('US')['publisher_receipts'],Decimal('30.62'))
        book['c'][1]=['9780306406157','Example',3,1,30,10,40,.1,-4,36]
        report=parse_book(book)
        self.assertEqual(report.issues,[])
        self.assertEqual(report.totals('US')['gross_sales'],60)
        self.assertEqual(report.totals('US')['publisher_receipts'],Decimal('48.62'))

    @unittest.skipUnless(os.getenv('STABLE_ACCEPTANCE_DIR'),'Private originals supplied separately')
    def test_april_gross_matches_summary_after_amazon_adjustments(self):
        p=Path(os.environ['STABLE_ACCEPTANCE_DIR'])/'MARBLE PRESS_Client_Statement_202604.xlsx'
        report=parse(p.read_bytes())
        self.assertEqual(report.issues,[])
        self.assertEqual(report.totals('US')['gross_sales'].quantize(Decimal('.01')),Decimal('11537.33'))
        self.assertEqual(report.totals('US')['publisher_receipts'].quantize(Decimal('.01')),Decimal('6643.32'))

    def test_identical_transactions_are_not_deduplicated(self):
        book=source();book['b'].append(copy.deepcopy(book['b'][1]))
        r=parse_book(book)
        self.assertEqual(len(r.sales),2)
        self.assertEqual(r.totals('US')['units_sold'],4)
        self.assertTrue(any('control mismatch' in i for i in r.issues))

    def test_missing_cache_is_not_zero(self):
        book=source();book['b'][1][-1]=None
        with self.assertRaises(ReviewRequired): parse_book(book)

    def test_known_control_arithmetic_can_reconstruct_cache(self):
        book=source()
        for i in (8,10,12,13,14): book['a'][2][i]=None
        self.assertEqual(parse_book(book).issues,[])

    def test_inconsistent_unused_percentage_does_not_replace_cash_amounts(self):
        book=source();book['a'][2][9]=.99;book['a'][2][11]=.77
        r=parse_book(book)
        self.assertEqual(r.issues,[])
        self.assertEqual(r.totals('US')['publisher_receipts'],Decimal('12.62'))

    def test_inventory_missing_is_not_zero_and_duplicate_rejected(self):
        rows=[['ISBN (Item Code)','Default Useable Balance Qty','Ending Balance Qty'],['9780306406157',8,10]]
        r=parse_book({'anything':rows},inventory_date='2026-03-01')
        self.assertEqual(r.inventory[0]['available'],8)
        rows.append(rows[1])
        with self.assertRaises(ReviewRequired): parse_book({'anything':rows})

    def test_isbn_ten_and_checksum(self):
        self.assertEqual(isbn('0306406152'),'9780306406157')
        with self.assertRaises(ReviewRequired): isbn('9780306406158')

    def test_stable_inventory_adapter_cannot_forward_orders(self):
        from app.distribution.adapters import adapter_for
        adapter=adapter_for(dict(adapter='stable_s3_inventory',configuration={}))
        self.assertTrue(adapter.capabilities.inventory)
        self.assertFalse(adapter.capabilities.submit_order)
        self.assertFalse(adapter.capabilities.order_status)

    @unittest.skipUnless(os.getenv('STABLE_ACCEPTANCE_DIR'),'Private original statements supplied separately')
    def test_canadian_september_detail_reconciles_cash_and_return_fee(self):
        from app.stable.parser import parse_reports, canadian_detail
        directory=Path(os.environ['STABLE_ACCEPTANCE_DIR'])
        detail_path=directory/'MARBLE PRESS_Hachette_Sales_Detail_202609.xlsx'
        if not detail_path.exists():self.skipTest('Original Hachette detail supplied separately')
        detail=read_workbook(detail_path.read_bytes())
        self.assertEqual(canadian_detail(detail),'2026-09')
        with self.assertRaises(ReviewRequired):canadian_detail(detail,'2026-08')
        us=read_workbook((directory/'MARBLE PRESS_Client_Statement_202603.xlsx').read_bytes())
        ca=read_workbook((directory/'MARBLE PRESS_Client_Statement_Canada_202603.xlsx').read_bytes())
        origins={'US':parse_book(us),'CA':parse_book(ca,companion=us)}
        a,b=parse_reports(read_workbook((directory/'MARBLE PRESS_Client_Statement_202609.xlsx').read_bytes()),
          transaction_detail=detail,reserve_origins=origins)
        self.assertEqual(a.issues,[]);self.assertEqual(b.issues,[])
        self.assertEqual(b.totals('CA')['units_sold'],360)
        self.assertEqual(b.totals('CA')['units_returned'],6)
        self.assertEqual(sum(s.unit_adjustment for s in b.sales),1)
        self.assertEqual(b.totals('CA')['publisher_receipts'].quantize(Decimal('.01')),Decimal('1565.00'))
        self.assertEqual(sum(s.handling for s in b.sales).quantize(Decimal('.01')),Decimal('203.49'))
        self.assertEqual(len([s for s in b.sales if s.activity=='Canadian Sale']),116)

    @unittest.skipUnless(os.getenv('STABLE_ACCEPTANCE_DIR'),'Private original statements supplied separately')
    def test_original_combined_september_uses_real_reserve_release(self):
        from app.stable.parser import parse_reports
        directory=Path(os.environ['STABLE_ACCEPTANCE_DIR'])
        march_us=read_workbook((directory/'MARBLE PRESS_Client_Statement_202603.xlsx').read_bytes())
        march_ca=read_workbook((directory/'MARBLE PRESS_Client_Statement_Canada_202603.xlsx').read_bytes())
        origins={'US':parse_book(march_us),'CA':parse_book(march_ca,companion=march_us)}
        a,b=parse_reports(read_workbook((directory/'MARBLE PRESS_Client_Statement_202609.xlsx').read_bytes()),reserve_origins=origins)
        self.assertEqual(a.territory,'US');self.assertEqual(a.issues,[])
        self.assertEqual(a.totals('US')['units_sold'],3993)
        self.assertEqual(a.totals('US')['units_returned'],136)
        self.assertEqual(sum(c['amount'] for c in a.costs if c['kind']=='reserve_release'),Decimal('-290.892525'))
        self.assertEqual(b.territory,'CA');self.assertEqual(len(b.issues),1)
        self.assertIn('no Canadian ISBN transaction detail',b.issues[0])

    @unittest.skipUnless(os.getenv('STABLE_ACCEPTANCE_DIR'),'Private original statements supplied separately')
    def test_original_march_us_and_canada(self):
        directory=Path(os.environ['STABLE_ACCEPTANCE_DIR'])
        us=directory/'MARBLE PRESS_Client_Statement_202603.xlsx'
        ca=directory/'MARBLE PRESS_Client_Statement_Canada_202603.xlsx'
        a=parse(us.read_bytes());b=parse(ca.read_bytes(),companion=read_workbook(us.read_bytes()))
        self.assertEqual(a.issues,[]);self.assertEqual(b.issues,[])
        self.assertEqual(a.totals('US')['publisher_receipts'],Decimal('1051.539975'))
        self.assertEqual(b.totals('CA')['publisher_receipts'],Decimal('24.511575'))
        self.assertEqual(a.totals('US')['units_sold'],274)
        self.assertEqual(sum(c['amount'] for c in a.costs if c['kind']=='technology'),Decimal('14.50'))


if __name__=='__main__': unittest.main()
