import unittest
from datetime import date, timedelta
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import MagicMock, patch
from uuid import uuid4
from fastapi import HTTPException
from pydantic import ValidationError
from services import royalty_accounts as accounts

class AccountTrackingTests(unittest.TestCase):
    def setUp(self):
        self.tenant = str(uuid4())
        self.work = str(uuid4())
        self.period = str(uuid4())
        self.cur = MagicMock()
        capacity_patch=patch('services.royalty_settlement.account_payment_capacity',return_value=None)
        capacity_patch.start();self.addCleanup(capacity_patch.stop)
        self.statement = {'id':str(uuid4()), 'tenant_id':self.tenant, 'work_id':self.work,
            'party':'author','period_id':self.period,'status':'final','currency':'USD',
            'payable_this_period':Decimal('125.88')}

    def change(self, **patches):
        values = dict(request_id=uuid4(),work_id=self.work,royalty_set_id=uuid4(),period_id=self.period,
            party='author',version=0,minimum_payout='100.00',reserve_percent='10',reserve_held='25.50',reason='Set opening reserve')
        values.update(patches)
        return accounts.AccountChange(**values)

    def payment(self, **patches):
        values = dict(request_id=uuid4(),statement_id=self.statement['id'],amount='25.88',
            payment_date=date.today(),reference_number='check-123',payment_method='check')
        values.update(patches)
        return accounts.PaymentRecord(**values)

    def test_invalid_money_and_percent(self):
        for field,value in [('minimum_payout','-1'),('reserve_percent','101'),('reserve_held','NaN'),('reserve_held','0.001')]:
            with self.assertRaises(ValidationError): self.change(**{field:value})
        for amount in ('0','-1','NaN','1.001'):
            with self.assertRaises(ValidationError): self.payment(amount=amount)

    @patch('services.royalty_accounts.require_tracking')
    @patch('services.royalty_accounts.replay', return_value=None)
    @patch('services.royalty_accounts.event')
    def test_record_partial_payment_exactly(self, event, replay, ready):
        self.cur.fetchone.side_effect=[self.statement,{'paid':Decimal('50.00')},None]
        result=accounts.record_payment(self.cur,self.tenant,'actor',self.payment())
        self.assertEqual(result['unpaid_balance'],'50.00')
        self.assertEqual(event.call_args.args[5],'payment')
        inserts=[c for c in self.cur.execute.call_args_list if 'INSERT INTO royalty_payments' in c.args[0]]
        self.assertEqual(len(inserts),1)
        self.assertIn(Decimal('25.88'),inserts[0].args[1])

    @patch('services.royalty_accounts.require_tracking')
    @patch('services.royalty_accounts.replay', return_value=None)
    def test_overpayment_has_no_insert(self, replay, ready):
        self.cur.fetchone.side_effect=[self.statement,{'paid':Decimal('100')}]
        with self.assertRaises(HTTPException) as error:
            accounts.record_payment(self.cur,self.tenant,'actor',self.payment(amount='26'))
        self.assertEqual(error.exception.status_code,422)
        self.assertFalse(any('INSERT' in c.args[0] for c in self.cur.execute.call_args_list))

    @patch('services.royalty_accounts.require_tracking')
    @patch('services.royalty_accounts.replay', return_value=None)
    def test_duplicate_reference_has_no_insert(self, replay, ready):
        self.cur.fetchone.side_effect=[self.statement,{'paid':Decimal('0')},{'id':'old'}]
        with self.assertRaises(HTTPException) as error:
            accounts.record_payment(self.cur,self.tenant,'actor',self.payment())
        self.assertEqual(error.exception.status_code,409)
        self.assertFalse(any('INSERT' in c.args[0] for c in self.cur.execute.call_args_list))

    @patch('services.royalty_accounts.require_tracking')
    @patch('services.royalty_accounts.replay', return_value={'payment_id':'existing'})
    def test_retry_does_not_record_twice(self, replay, ready):
        self.cur.fetchone.return_value=self.statement
        self.assertEqual(accounts.record_payment(self.cur,self.tenant,'actor',self.payment()),{'payment_id':'existing'})
        self.assertFalse(any('INSERT' in c.args[0] for c in self.cur.execute.call_args_list))

    @patch('services.royalty_accounts.require_tracking')
    @patch('services.royalty_accounts.replay', return_value=None)
    def test_future_payment_rejected(self, replay, ready):
        self.cur.fetchone.return_value=self.statement
        with self.assertRaises(HTTPException):
            accounts.record_payment(self.cur,self.tenant,'actor',self.payment(payment_date=date.today()+timedelta(days=1)))

    @patch('services.royalty_accounts.require_tracking')
    def test_draft_statement_rejected(self, ready):
        self.cur.fetchone.return_value={**self.statement,'status':'draft'}
        with self.assertRaises(HTTPException): accounts.record_payment(self.cur,self.tenant,'actor',self.payment())

    def test_request_id_cannot_cross_tenants(self):
        self.cur.fetchone.return_value={'tenant_id':'different','request_payload':{},'after_values':{}}
        with self.assertRaises(HTTPException) as error: accounts.replay(self.cur,self.tenant,uuid4(),{})
        self.assertEqual(error.exception.status_code,409)

    def test_settings_audit_and_concurrency(self):
        engine=SimpleNamespace(assert_work=MagicMock(),assert_royalty_set_for_work=MagicMock(),load_period=MagicMock())
        old={'version':0,'minimum_payout':Decimal('50'),'reserve_percent':Decimal('5'),'reserve_held':Decimal('10'),'currency':'USD'}
        with patch.dict('sys.modules',{'services.royalty_statement_engine':engine}), patch.object(accounts,'require_tracking'), patch.object(accounts,'replay',return_value=None), patch.object(accounts,'account_state',return_value=old), patch.object(accounts,'event') as event:
            result=accounts.save_settings(self.cur,self.tenant,'actor',self.change())
            self.assertEqual(result['reserve_change'],'15.50')
            self.assertEqual(result['version'],1)
            self.assertEqual(event.call_args.args[-2]['minimum_payout'],'50')
            self.assertEqual(event.call_args.args[-1]['minimum_payout'],'100.00')
            with self.assertRaises(HTTPException) as error:
                accounts.save_settings(self.cur,self.tenant,'actor',self.change(version=1))
            self.assertEqual(error.exception.status_code,409)

    def test_missing_schema_fails_safely(self):
        self.cur.fetchone.return_value={'ready':False}
        with self.assertRaises(HTTPException) as error: accounts.require_tracking(self.cur)
        self.assertEqual(error.exception.status_code,503)
        self.assertFalse(any('CREATE' in c.args[0] for c in self.cur.execute.call_args_list))

if __name__ == '__main__': unittest.main()
