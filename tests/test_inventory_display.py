from collections import Counter
from unittest.mock import patch
import unittest
import test_distribution_sql as base
from test_distribution_sql import rpc,s
from app.distribution import inventory_display as display


class InventoryDisplayTests(unittest.TestCase):
    setUpClass=classmethod(base.DistributionSQLTests.setUpClass.__func__)
    tearDownClass=classmethod(base.DistributionSQLTests.tearDownClass.__func__)

    def setUp(self):
        base.DistributionSQLTests.setUp(self)
        _,self.install,_=base.DistributionSQLTests.publishing_fixture(self)
        rpc("UPDATE distribution_shopify_installations SET location_id='location' WHERE id=$1",[self.install['id']])
        self.install['location_id']='location'
        rpc("INSERT INTO distribution_shopify_variants(tenant_id,installation_id,variant_id,edition_id,connection_id,inventory_item_id) VALUES($1,$2,'variant',$3,$4,'item')",[self.tenant,self.install['id'],self.ed,self.connection])

    def result(self,reserved,sellable,committed,known):
        rows=[{'connection_id':self.connection,'edition_id':self.ed,'distributor_quantity':20,'reserved_quantity':reserved,'sellable_quantity':sellable}]
        response={'inventoryItem':{'inventoryLevel':{'quantities':[{'name':'committed','quantity':committed},{'name':'available','quantity':12}]}}}
        with patch.object(display,'known_shopify_commitments',return_value=Counter({self.ed:known})),patch.object(display.sh,'graphql',return_value=response) as gql:
            with s.transaction() as cur:result=display.reconcile_display(cur,self.install,rows)[0]
        self.assertEqual(gql.call_args.args[3],{'item':'item','location':'location'})
        self.assertEqual(result['distributor_quantity'],20)
        return result

    def test_old_commitments_are_included(self):
        row=self.result(0,15,3,0)
        self.assertEqual((row['reserved_quantity'],row['sellable_quantity']),(3,12))

    def test_existing_reservation_is_not_deducted_twice(self):
        row=self.result(1,14,4,1)
        self.assertEqual((row['reserved_quantity'],row['sellable_quantity']),(4,11))

    def test_cancellation_removes_external_commitment(self):
        row=self.result(0,15,0,0)
        self.assertEqual((row['reserved_quantity'],row['sellable_quantity']),(0,15))

    def test_known_overlap_cannot_increase_stock(self):
        row=self.result(1,14,0,1)
        self.assertEqual((row['reserved_quantity'],row['sellable_quantity']),(1,14))

    def test_missing_location_never_queries_shopify(self):
        with patch.object(display.sh,'graphql') as gql:
            with s.transaction() as cur:
                self.assertEqual(display.reconcile_display(cur,{**self.install,'location_id':None},[{'test':1}]),[{'test':1}])
        gql.assert_not_called()
