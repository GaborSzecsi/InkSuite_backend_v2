import unittest
from contextlib import ExitStack
from datetime import date
from decimal import Decimal
from unittest.mock import MagicMock, patch
from fastapi import HTTPException
from starlette.requests import Request
from routers import royalty, royalty_engine as api
from services import royalty_statement_engine as engine


class RoyaltyWorkflowTests(unittest.TestCase):
    def setUp(self):
        self.cur = MagicMock()
        self.item = royalty.SubrightsIncomeItem(period_id='period', work_id='work',
            royalty_set_id='set', subrights_type_id='type', income_date='2026-06-30',
            publisher_receipts=Decimal('143.74'))
        self.cols = {'id','income_date','royalty_set_id','publisher_receipts','updated_at'}

    def test_only_one_income_post_registered(self):
        routes = [r for r in royalty.router.routes if r.path.endswith('/subrights/income') and 'POST' in r.methods]
        self.assertEqual(len(routes), 1)

    def test_insert_returns_database_identity(self):
        self.cur.fetchone.return_value = {'id': 'saved'}
        with patch.object(royalty, '_get_existing_columns', return_value=self.cols):
            self.assertEqual(royalty._save_subrights_income_row(self.cur, 'tenant', self.item), 'saved')
        sql, values = self.cur.execute.call_args.args
        self.assertIn('INSERT INTO', sql)
        self.assertIn('RETURNING id', sql)
        self.assertIn(Decimal('143.74'), values)

    def test_edit_and_repeat_save_do_not_insert(self):
        self.item.id = 'saved'
        self.cur.fetchone.return_value = {'id': 'saved'}
        with patch.object(royalty, '_get_existing_columns', return_value=self.cols):
            for _ in range(2):
                self.assertEqual(royalty._save_subrights_income_row(self.cur, 'tenant', self.item), 'saved')
        for call in self.cur.execute.call_args_list:
            sql, values = call.args
            self.assertIn('UPDATE subrights_income_lines', sql)
            self.assertIn('tenant_id = %s::uuid', sql)
            self.assertEqual(values[-4:], ['saved','tenant','period','work'])

    def test_missing_or_cross_tenant_income_is_not_inserted(self):
        self.item.id = 'other'
        self.cur.fetchone.return_value = None
        with patch.object(royalty, '_get_existing_columns', return_value=self.cols):
            with self.assertRaises(HTTPException) as error:
                royalty._save_subrights_income_row(self.cur, 'tenant', self.item)
        self.assertEqual(error.exception.status_code, 404)
        self.assertNotIn('INSERT', self.cur.execute.call_args.args[0])

    def test_wrong_work_set_rejected(self):
        self.cur.fetchone.return_value = None
        with self.assertRaises(HTTPException) as error:
            royalty._assert_royalty_set_belongs_to_work(self.cur, 'tenant', 'set', 'wrong-work')
        self.assertEqual(error.exception.status_code, 400)
        self.assertEqual(self.cur.execute.call_args.args[1], ('tenant', 'set', 'wrong-work'))

    def test_get_saved_income_is_scoped_and_repeatable(self):
        conn = MagicMock()
        conn.cursor.return_value.__enter__.return_value = self.cur
        self.cur.fetchall.return_value = [{'id':'saved','publisher_receipts':Decimal('143.74')}]
        with patch.object(royalty, '_require_tenant', return_value='tenant'), \
             patch.object(royalty, 'db_conn') as db, \
             patch.object(royalty, '_assert_period_exists'), \
             patch.object(royalty, '_assert_work_exists'), \
             patch.object(royalty, '_get_existing_columns', return_value=self.cols):
            db.return_value.__enter__.return_value = conn
            first = royalty.get_subrights_income(MagicMock(), 'period', 'work')
            self.assertEqual(first, royalty.get_subrights_income(MagicMock(), 'period', 'work'))
        sql, params = self.cur.execute.call_args.args
        self.assertEqual(params, ('tenant','period','work'))
        self.assertIn('ORDER BY i.income_date, i.id', sql)
        self.assertEqual(first[0]['id'], 'saved')

    def test_approval_changes_only_status(self):
        self.cur.fetchone.return_value = {'statement_id':'stmt','status':'draft'}
        self.assertEqual(api._approve_statement(self.cur, 'tenant','stmt')['status'], 'final')
        sql, params = self.cur.execute.call_args.args
        self.assertIn("status = 'final'", sql)
        self.assertNotIn('earned_this_period', sql)
        self.assertNotIn('royalty_statement_lines', sql)
        self.assertEqual(params, ('tenant','stmt'))
        self.assertIn('FOR UPDATE', self.cur.execute.call_args_list[0].args[0])

    def test_final_approval_idempotent(self):
        self.cur.fetchone.return_value = {'statement_id':'stmt','status':'final'}
        self.assertEqual(api._approve_statement(self.cur, 'tenant','stmt')['status'], 'final')
        self.assertEqual(self.cur.execute.call_count, 1)

    def test_cross_tenant_approval_rejected(self):
        self.cur.fetchone.return_value = None
        with self.assertRaises(HTTPException) as error:
            api._approve_statement(self.cur, 'tenant','other')
        self.assertEqual(error.exception.status_code,404)
        self.assertEqual(self.cur.execute.call_args.args[1], ('tenant','other'))

    def test_queue_states(self):
        self.cur.fetchone.return_value = {'exists':1}
        self.cur.fetchall.return_value = [
            {'statement_id':None,'status':None,'sent_at':None},
            {'statement_id':'draft','status':'draft','sent_at':None},
            {'statement_id':'final','status':'final','sent_at':None},
            {'statement_id':'sent','status':'final','sent_at':'2026-07-01'},
        ]
        rows = api._fetch_statement_queue(self.cur,'tenant','period')
        self.assertEqual([r['workflow_status'] for r in rows],
            ['Not generated','Draft','Approved — awaiting distribution','Complete'])
        self.assertTrue(rows[0]['needs_generation_review'])
        self.assertFalse(rows[2]['needs_generation_review'])
        self.assertTrue(rows[2]['awaiting_distribution'])
        self.assertTrue(rows[3]['complete'])
        self.assertIn('rr.party::text',self.cur.execute.call_args.args[0])

    def test_distribution_only_approved_unsent_and_period_scoped(self):
        self.cur.fetchall.return_value = []
        api._fetch_distribution_items(self.cur,'tenant','period')
        sql, params = self.cur.execute.call_args.args
        self.assertIn("rs.status = 'final'",sql)
        self.assertIn('rs.sent_at IS NULL',sql)
        self.assertEqual(params,['tenant','period'])

    def test_queue_hides_completed_items(self):
        conn = MagicMock()
        with patch.object(api, '_require_tenant_id', return_value='tenant'), \
             patch.object(api, 'db_conn') as db, \
             patch.object(api, '_fetch_statement_queue', return_value=[
                 {'statement_id':'active','complete':False},
                 {'statement_id':'sent','complete':True}]):
            db.return_value.__enter__.return_value = conn
            result = api.statement_queue_endpoint(MagicMock(), 'period')
        self.assertEqual(result['items'], [{'statement_id':'active','complete':False}])
        self.assertEqual(result['complete_count'], 1)
        self.assertEqual(result['period_id'], 'period')

    def test_draft_pdf_rejected_before_rendering(self):
        with patch.object(api, '_require_tenant_id', return_value='tenant'), \
             patch.object(api, 'db_conn'), \
             patch.object(api, '_statement_recipients'), \
             patch.object(api, 'run_fetch_statement', return_value={'header':{'status':'draft'}}), \
             patch.object(api, '_pdf_html') as render:
            with self.assertRaises(HTTPException) as error:
                api._statement_pdf_bytes('stmt', MagicMock())
        self.assertEqual(error.exception.status_code, 409)
        render.assert_not_called()

    def test_other_tenant_pdf_rejected_before_fetch(self):
        with patch.object(api, '_require_tenant_id', return_value='tenant'), \
             patch.object(api, 'db_conn'), \
             patch.object(api, '_statement_recipients', side_effect=HTTPException(404, 'Statement not found')), \
             patch.object(api, 'run_fetch_statement') as fetch:
            with self.assertRaises(HTTPException):
                api._statement_pdf_bytes('other', MagicMock())
        fetch.assert_not_called()

    def test_queue_unknown_period_rejected(self):
        self.cur.fetchone.return_value = None
        with self.assertRaises(HTTPException) as error:
            api._fetch_statement_queue(self.cur, 'tenant','other-period')
        self.assertEqual(error.exception.status_code, 404)
        self.assertEqual(self.cur.execute.call_count, 1)

    def generation_context(self, existing):
        stack = ExitStack()
        mocks = {'assert_work':None, 'assert_royalty_set_for_work':None,
            'resolve_period_id_for_generate':'period',
            'load_period':engine.PeriodRow('period','2026-H1',date(2026,1,1),date(2026,6,30)),
            'find_existing_statement':existing,'load_first_rights_rules':[],
            'load_tiers_for_rules':({},{}),'load_subrights_rules':[],
            'load_sales_rows_for_period':[],'load_previous_closing_recoupment':Decimal('0'),
            'total_recoupable_advances':Decimal('0'),'_load_prior_earned_to_date':Decimal('0'),
            'load_subrights_income_rows_for_period':[{'id':'receipt','subrights_name':'Digital audiobook rights','publisher_receipts':'143.74'}],
            'pick_subrights_rule_for_name':engine.RuleRow('rule','Digital audiobook rights','author',
                'subrights','flat','net_receipts',False,Decimal('50'),None)}
        for name,value in mocks.items():
            stack.enter_context(patch.object(engine,name,return_value=value))
        return stack

    def test_rebuild_same_draft_replaces_lines_and_includes_subrights(self):
        with self.generation_context(('same-draft','draft')):
            result=engine.generate_statement(self.cur,tenant_id='tenant',work_id='work',
                royalty_set_id='set',party='author',period_id='period',rebuild=True)
        self.assertEqual(result['statement_id'],'same-draft')
        self.assertEqual(result['header']['earned_this_period'],'71.87')
        calls=self.cur.execute.call_args_list
        deletes=[c for c in calls if 'DELETE FROM royalty_statement_lines' in c.args[0]]
        self.assertEqual(deletes[0].args[1],('same-draft',))
        inserts=[c for c in calls if 'INSERT INTO royalty_statement_lines' in c.args[0]]
        self.assertEqual(len(inserts),1)
        self.assertIn('subrights',inserts[0].args[1])
        self.assertFalse(any('INSERT INTO royalty_statements' in c.args[0] for c in calls))

    def test_final_cannot_rebuild(self):
        with self.generation_context(('final','final')):
            with self.assertRaises(engine.StatementValidationError):
                engine.generate_statement(self.cur,tenant_id='tenant',work_id='work',
                    royalty_set_id='set',party='author',period_id='period',rebuild=True)
        self.cur.execute.assert_not_called()


if __name__ == '__main__':
    unittest.main()
