import unittest
from decimal import Decimal
from unittest.mock import MagicMock, patch
from routers import catalog_royalties
from services import royalty_statement_engine as engine


class RoyaltyRuleResolutionTests(unittest.TestCase):
    def inserted_base(self, rights_type, **values):
        cur = MagicMock()
        cur.fetchone.return_value = {'id': 'rule'}
        rule = dict(format='Digital audiobook rights', percent=25, **values)
        with patch.object(catalog_royalties, '_resolve_subrights_type_id', return_value='type'):
            catalog_royalties._insert_royalty_rule(cur, 'tenant', 'set', 'author', rights_type, rule)
        self.assertEqual(cur.execute.call_count, 1)
        args = cur.execute.call_args.args[1]
        self.assertEqual(args[10], 25)
        return args[7]

    def test_missing_subrights_basis_defaults_to_receipts(self):
        self.assertEqual(self.inserted_base('subrights'), 'net_receipts')

    def test_missing_first_rights_basis_still_defaults_to_list_price(self):
        self.assertEqual(self.inserted_base('first_rights'), 'list_price')

    def test_explicit_basis_is_preserved(self):
        self.assertEqual(self.inserted_base('subrights', base='net_receipts'), 'net_receipts')
        self.assertEqual(self.inserted_base('subrights', base='list_price'), 'list_price')

    def rule(self, label, party='author'):
        return engine.RuleRow('rule-'+label, label, party, 'first_rights', 'fixed', 'net_receipts', False, Decimal('10'), None)

    def test_detail_falls_back_to_its_recorded_parent_format(self):
        hardcover=self.rule('Hardcover')
        self.assertIs(engine.pick_rule_for_category([hardcover], 'author', 'Paper over boards', 'Hardcover'), hardcover)

    def test_specific_binding_rule_takes_priority(self):
        detail=self.rule('Paper over boards')
        self.assertIs(engine.pick_rule_for_category([self.rule('Hardcover'), detail], 'author', 'Paper over boards', 'Hardcover'), detail)

    def test_fallback_never_guesses_or_crosses_parties(self):
        for rules,parent in [([self.rule('Hardcover')],'Paperback'), ([self.rule('Hardcover','illustrator')],'Hardcover')]:
            with self.assertRaises(engine.StatementValidationError):
                engine.pick_rule_for_category(rules, 'author', 'Paper over boards', parent)

    def test_ambiguous_parent_rules_still_fail(self):
        with self.assertRaises(engine.StatementValidationError):
            engine.pick_rule_for_category([self.rule('Hardcover'),self.rule('Hardcover')], 'author', 'Paper over boards','Hardcover')

    def test_sales_bucket_preserves_binding_label_and_parent(self):
        buckets=engine.aggregate_sales_into_buckets([{'edition_id':'edition','product_form':'Hardcover','product_form_detail':'Paper over boards',
            'units_sold':1,'publisher_receipts':'100'}])
        self.assertEqual(buckets[0].category_label,'Paper over boards')
        self.assertEqual(buckets[0].product_form,'Hardcover')


if __name__ == '__main__': unittest.main()
