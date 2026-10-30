import unittest
import test_distribution_sql as base
from test_distribution_sql import rpc, s
from app.distribution.connections import purge_simulator
from fastapi import HTTPException
from unittest.mock import patch


class SimulatorPurgeTests(unittest.TestCase):
    setUpClass = classmethod(base.DistributionSQLTests.setUpClass.__func__)
    tearDownClass = classmethod(base.DistributionSQLTests.tearDownClass.__func__)
    setUp = base.DistributionSQLTests.setUp

    def test_purge_deletes_order_dependents_and_keeps_catalog(self):
        rpc("UPDATE distribution_connections SET adapter='simulator' WHERE id=$1",[self.connection])
        order=rpc("""INSERT INTO distribution_orders(tenant_id,connection_id,source,source_account,external_order_id,request_hash,reference,recipient,shipping_method)
            VALUES($1,$2,'SHOPIFY','example.myshopify.com','simulation-test','hash','test','{}','Ground') RETURNING id""",[self.tenant,self.connection])['rows'][0]['id']
        rpc("INSERT INTO distribution_order_items(tenant_id,order_id,edition_id,isbn,quantity) VALUES($1,$2,$3,'9780000000002',1)",[self.tenant,order,self.ed])
        rpc("INSERT INTO distribution_simulated_orders(tenant_id,connection_id,order_id,items) VALUES($1,$2,$3,'[]')",[self.tenant,self.connection,order])
        rpc("INSERT INTO distribution_jobs(tenant_id,connection_id,order_id,kind,status) VALUES($1,$2,$3,'SUBMIT','DONE')",[self.tenant,self.connection,order])
        with s.transaction() as cur:purge_simulator(cur,self.tenant,self.connection)
        for table in ('distribution_orders','distribution_order_items','distribution_simulated_orders','distribution_jobs','distribution_connections'):
            self.assertEqual(rpc(f'SELECT count(*) AS n FROM {table} WHERE tenant_id=$1',[self.tenant])['rows'][0]['n'],0)
        self.assertEqual(rpc('SELECT count(*) AS n FROM editions WHERE tenant_id=$1',[self.tenant])['rows'][0]['n'],1)

    def test_purge_refuses_non_simulator_and_other_tenant(self):
        for tenant in (self.tenant,self.other):
            with self.assertRaises(HTTPException):
                with s.transaction() as cur:purge_simulator(cur,tenant,self.connection)
        self.assertEqual(len(rpc('SELECT id FROM distribution_connections WHERE id=$1',[self.connection])['rows']),1)

    def test_completed_checks_are_deleted(self):
        from app.distribution import execution
        rpc("UPDATE distribution_connections SET adapter='simulator' WHERE id=$1",[self.connection])
        with s.transaction() as cur:s.enqueue(cur,self.tenant,self.connection,'TEST')
        with patch.object(execution,'execute'),patch.object(execution,'schedule'):
            self.assertTrue(execution.run_once())
        self.assertEqual(rpc('SELECT count(*) AS n FROM distribution_jobs WHERE connection_id=$1',[self.connection])['rows'][0]['n'],0)
        self.assertEqual(rpc('SELECT status FROM distribution_connections WHERE id=$1',[self.connection])['rows'][0]['status'],'CONNECTED')

    def test_archived_simulator_is_eventually_deleted(self):
        from app.distribution.connections import cleanup_credentials
        rpc("UPDATE distribution_connections SET adapter='simulator',archived_at=now() WHERE id=$1",[self.connection])
        self.assertTrue(cleanup_credentials())
        self.assertEqual(rpc('SELECT id FROM distribution_connections WHERE id=$1',[self.connection])['rows'],[])

    def test_store_reset_dry_run_and_apply(self):
        from deploy.reset_shopify_simulation import reset
        rpc("UPDATE distribution_connections SET adapter='simulator' WHERE id=$1",[self.connection])
        rpc("""INSERT INTO distribution_orders(tenant_id,connection_id,source,source_account,external_order_id,request_hash,reference,recipient,shipping_method)
            VALUES($1,$2,'SHOPIFY','example.myshopify.com','simulation-reset','hash','test','{}','Ground')""",[self.tenant,self.connection])
        self.assertEqual(reset('unrelated.myshopify.com',True),0)
        self.assertEqual(reset('example.myshopify.com'),1)
        self.assertTrue(rpc('SELECT id FROM distribution_connections WHERE id=$1',[self.connection])['rows'])
        self.assertEqual(reset('example.myshopify.com',True),1)
        self.assertEqual(rpc('SELECT id FROM distribution_connections WHERE id=$1',[self.connection])['rows'],[])
