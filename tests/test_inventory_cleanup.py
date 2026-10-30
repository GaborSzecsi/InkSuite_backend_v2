import unittest
from unittest.mock import patch
from fastapi import HTTPException
import test_distribution_sql as base
from test_distribution_sql import rpc, s
from app.distribution import inventory_cleanup as cleanup
from app.distribution.connections import remove_connection, uninstall_connections


class InventoryCleanupTests(unittest.TestCase):
    setUpClass = classmethod(base.DistributionSQLTests.setUpClass.__func__)
    tearDownClass = classmethod(base.DistributionSQLTests.tearDownClass.__func__)

    def setUp(self):
        base.DistributionSQLTests.setUp(self)
        _, self.install, _ = base.DistributionSQLTests.publishing_fixture(self)
        rpc("UPDATE distribution_connections SET adapter='simulator',shopify_installation_id=$1 WHERE id=$2", [self.install['id'], self.connection])
        rpc("UPDATE distribution_shopify_installations SET location_id='location-1',simulated_inventory_connection_id=$1 WHERE id=$2", [self.connection, self.install['id']])
        rpc("""INSERT INTO distribution_shopify_variants(tenant_id,installation_id,variant_id,inventory_item_id,connection_id,edition_id)
            VALUES($1,$2,'variant-1','item-1',$3,$4)""", [self.tenant,self.install['id'],self.connection,self.ed])
        self.available = 12
        self.committed = 3
        self.writes = []

    def api(self, cur, install, query, variables):
        self.assertEqual(str(install['id']), self.install['id'])
        if query == cleanup.LEVEL_QUERY:
            self.assertEqual(variables, {'item':'item-1','location':'location-1'})
            return {'inventoryItem': {'tracked':True, 'inventoryLevel': {
                'quantities':[{'name':'available','quantity':self.available}]}}}
        self.assertEqual(query, cleanup.SET_QUERY)
        body = variables['input']
        self.assertEqual(body['name'], 'available')
        self.assertEqual(body['quantities'], [{'inventoryItemId':'item-1','locationId':'location-1','quantity':0,'changeFromQuantity':12}])
        self.writes.append(body)
        self.available = 0
        return {'inventorySetQuantities': {'userErrors':[]}}

    def remove(self):
        with s.transaction() as cur:
            return remove_connection(cur,self.tenant,self.connection,self.user)

    def test_removal_zeros_available_not_committed_then_deletes(self):
        with patch.object(cleanup.sh,'graphql',side_effect=self.api):
            self.assertEqual(self.remove(), {'removed':True})
        self.assertEqual((self.available,self.committed),(0,3))
        self.assertEqual(len(self.writes),1)
        self.assertEqual(rpc('SELECT id FROM distribution_connections WHERE id=$1',[self.connection])['rows'],[])

    def test_rejection_preserves_connection_and_mapping(self):
        def fail(cur, install, query, variables):
            if query == cleanup.LEVEL_QUERY:
                return self.api(cur,install,query,variables)
            return {'inventorySetQuantities': {'userErrors':[{'code':'CHANGE_FROM_QUANTITY_STALE'}]}}
        with patch.object(cleanup.sh,'graphql',side_effect=fail), self.assertRaises(HTTPException):
            self.remove()
        self.assertTrue(rpc('SELECT id FROM distribution_connections WHERE id=$1',[self.connection])['rows'])
        self.assertEqual(rpc("SELECT connection_id FROM distribution_shopify_variants WHERE installation_id=$1",[self.install['id']])['rows'][0]['connection_id'],self.connection)

    def test_zero_stock_retry_does_not_write_again(self):
        self.available=0
        with patch.object(cleanup.sh,'graphql',side_effect=self.api):self.remove()
        self.assertEqual(self.writes,[])

    def test_unmanaged_stock_is_not_touched(self):
        rpc('UPDATE distribution_shopify_installations SET simulated_inventory_connection_id=NULL WHERE id=$1',[self.install['id']])
        with patch.object(cleanup.sh,'graphql') as api:self.remove()
        api.assert_not_called()

    def test_other_connection_and_location_are_not_touched(self):
        rpc("INSERT INTO distribution_shopify_variants(tenant_id,installation_id,variant_id,inventory_item_id) VALUES($1,$2,'other-variant','other-item')",[self.tenant,self.install['id']])
        with patch.object(cleanup.sh,'graphql',side_effect=self.api):self.remove()
        self.assertEqual(len(self.writes),1)

    def test_shared_item_blocks_removal(self):
        rpc("INSERT INTO distribution_shopify_variants(tenant_id,installation_id,variant_id,inventory_item_id) VALUES($1,$2,'other-variant','item-1')",[self.tenant,self.install['id']])
        with patch.object(cleanup.sh,'graphql') as api, self.assertRaises(HTTPException):self.remove()
        api.assert_not_called()

    def test_uninstall_never_uses_revoked_token(self):
        with patch.object(cleanup.sh,'graphql') as api:
            with s.transaction() as cur:uninstall_connections(cur,self.install)
        api.assert_not_called()

    def test_running_job_prevents_remote_changes(self):
        rpc("INSERT INTO distribution_jobs(tenant_id,connection_id,kind,status) VALUES($1,$2,'PUBLISH','RUNNING')",[self.tenant,self.connection])
        with patch.object(cleanup.sh,'graphql') as api, self.assertRaises(HTTPException):self.remove()
        api.assert_not_called()

    def test_deleted_item_needs_no_write(self):
        with patch.object(cleanup.sh,'graphql',return_value={'inventoryItem':None}) as api:self.remove()
        self.assertEqual(api.call_count,1)

    def test_real_distributor_removal_also_clears_stock(self):
        rpc("UPDATE distribution_connections SET adapter='standard_json' WHERE id=$1",[self.connection])
        rpc("UPDATE distribution_shopify_installations SET simulated_inventory_connection_id=NULL,inventory_push_enabled=true WHERE id=$1",[self.install['id']])
        with patch.object(cleanup.sh,'graphql',side_effect=self.api):self.remove()
        self.assertEqual(len(self.writes),1)
