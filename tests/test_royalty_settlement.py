import unittest
from services.royalty_settlement import calculate_settlement

class SettlementTests(unittest.TestCase):
    def test_below_minimum_accrues_until_exact_threshold(self):
        first=calculate_settlement(30,0,0,0,0,50)
        self.assertEqual((first['actual_payable'],first['accrued_carried_forward']),('0.00','30.00'))
        next=calculate_settlement(20,first['accrued_carried_forward'],0,0,0,50)
        self.assertEqual((next['actual_payable'],next['accrued_carried_forward']),('50.00','0.00'))

    def test_saved_custom_threshold_is_authoritative(self):
        self.assertEqual(calculate_settlement(75,0,0,0,0,100)['actual_payable'],'0.00')
        self.assertEqual(calculate_settlement(75,0,0,0,0,50)['actual_payable'],'75.00')

    def test_reserve_increase_and_decrease(self):
        increased=calculate_settlement(100,0,10,200,10,50)
        self.assertEqual((increased['reserve_held'],increased['reserve_change'],increased['actual_payable']),('20.00','10.00','90.00'))
        decreased=calculate_settlement(40,0,20,100,10,50)
        self.assertEqual((decreased['reserve_held'],decreased['reserve_change'],decreased['actual_payable']),('10.00','-10.00','50.00'))

    def test_reserve_cannot_withhold_unavailable_money(self):
        row=calculate_settlement(5,3,2,1000,10,50)
        self.assertEqual((row['reserve_held'],row['reserve_shortfall'],row['actual_payable']),('10.00','90.00','0.00'))

    def test_no_history_preserves_verified_opening_reserve(self):
        row=calculate_settlement(40,0,10,None,10,50)
        self.assertEqual((row['reserve_held'],row['reserve_change'],row['accrued_carried_forward']),('10.00','0.00','40.00'))

    def test_reserve_percent_precision_is_preserved(self):
        row=calculate_settlement(100,0,0,100,'12.3456',50)
        self.assertEqual((row['reserve_percent'],row['reserve_target']),('12.3456','12.35'))

    def test_pdf_shows_frozen_settlement_and_actual_payable(self):
        from routers.royalty_engine import _pdf_html
        policy=calculate_settlement(30,0,10,200,10,50)
        rendered=_pdf_html({'header':{'settlement':policy,'payable_this_period':policy['actual_payable']},'lines':[]})
        for label in ('Minimum payment','Accrued brought forward','Closing reserve held','Accrued carried forward','Current statement payable'):
            self.assertIn(label,rendered)
        self.assertIn('$50.00',rendered)
        self.assertIn('$0.00',rendered)

    def test_pdf_shows_prior_payments_and_total_due_without_duplicating_current_payable(self):
        from routers.royalty_engine import _pdf_html
        policy=calculate_settlement(30,0,0,0,0,50)
        policy.update(prior_unpaid_payable='60.00',total_payment_due='60.00',prior_payment_balances=[
            {'period_code':'2025-H2','statement_payable':'100.00','paid_amount':'40.00','outstanding_amount':'60.00'}])
        rendered=_pdf_html({'header':{'settlement':policy,'payable_this_period':'0.00'},'lines':[]})
        for phrase in ('2025-H2: payable / paid / unpaid','$100.00 / $40.00 / $60.00','Prior statements still unpaid','Total payment due (current plus prior unpaid)'):
            self.assertIn(phrase,rendered)
