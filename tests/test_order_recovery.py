import unittest
from uuid import uuid4
from datetime import datetime, timezone
from unittest.mock import patch
import test_distribution_sql as base
from test_distribution_sql import rpc, s
from app.distribution import order_recovery as recovery, shopify, execution
from fastapi import HTTPException


class RecoveryTests(unittest.TestCase):
    setUpClass=classmethod(base.DistributionSQLTests.setUpClass.__func__)
    tearDownClass=classmethod(base.DistributionSQLTests.tearDownClass.__func__)

    def setUp(self):
        rpc("UPDATE distribution_webhooks SET status='DONE' WHERE status='RECEIVED'")
        rpc("UPDATE distribution_jobs SET status='FAILED' WHERE status='QUEUED'")
        base.DistributionSQLTests.setUp(self)
        self.install={'id':str(uuid4()),'tenant_id':self.tenant,'shop':str(uuid4())+'.myshopify.com','location_id':'location','user_id':self.user}
        self.body.source_account=self.install['shop']
        self.body.external_order_id='gid://shopify/Order/100'
        rpc("INSERT INTO distribution_shopify_installations(id,tenant_id,shop,secret_reference,location_id) VALUES($1,$2,$3,'test','location')",[self.install['id'],self.tenant,self.install['shop']])
        self.target=str(uuid4())
        rpc("""INSERT INTO distribution_connections(id,tenant_id,display_name,adapter,enabled,orders_enabled,configuration)
            VALUES($1,$2,'Demo','simulator',true,true,'{"simulated_stock":{"9780000000002":20}}')""",[self.target,self.tenant])
        rpc("INSERT INTO distribution_shopify_variants(tenant_id,installation_id,variant_id,edition_id,connection_id) VALUES($1,$2,'variant',$3,$4)",[self.tenant,self.install['id'],self.ed,self.connection])
        with s.transaction() as cur:self.order=s.create_order(cur,self.tenant,self.body,self.user,automated=True)
        self.webhook=rpc("""INSERT INTO distribution_webhooks(tenant_id,installation_id,delivery_id,topic,payload,status) VALUES($1,$2,'test','orders/paid','{"id":"100"}','DONE') RETURNING id""",[self.tenant,self.install['id']])['rows'][0]['id']

    def retry(self,install=None):
        with patch.object(recovery,'require_development_store'):
            with s.transaction() as cur:return recovery.retry_simulated_order(cur,install or self.install,self.order['id'],self.target)

    def source_order(self):
        return {'id':'gid://shopify/Order/100','createdAt':datetime.now(timezone.utc).isoformat(),'cancelledAt':None,'displayFinancialStatus':'PAID',
          'shippingLines':{'pageInfo':{'hasNextPage':False},'nodes':[{'title':'Ground'}]},
          'fulfillmentOrders':{'pageInfo':{'hasNextPage':False},'nodes':[{'assignedLocation':{'location':{'id':'location'}},
            'lineItems':{'pageInfo':{'hasNextPage':False},'nodes':[{'remainingQuantity':1,'lineItem':{'variant':{'id':'variant'},'title':'Book','sku':'9780000000002'}}]}}]}}

    def test_received_order_recovered_through_webhook_and_worker(self):
        self.assertEqual(self.order['status'],'RECEIVED')
        self.assertTrue(self.retry()['queued'])
        with patch.object(shopify,'graphql',return_value={'order':self.source_order()}):self.assertTrue(shopify.process_inbox_once())
        with patch.object(execution,'schedule'):self.assertTrue(execution.run_once())
        rows=rpc('SELECT status,connection_id FROM distribution_orders WHERE tenant_id=$1',[self.tenant])['rows']
        self.assertEqual(rows,[{'status':'ACCEPTED','connection_id':self.target}])
        self.assertEqual(rpc("SELECT status FROM distribution_jobs WHERE tenant_id=$1 AND kind='SUBMIT'",[self.tenant])['rows'],[{'status':'DONE'}])

    def test_rechecks_cancelled_source_without_submission(self):
        self.retry()
        order=self.source_order();order['cancelledAt']=order['createdAt']
        with patch.object(shopify,'graphql',return_value={'order':order}):shopify.process_inbox_once()
        self.assertEqual(rpc('SELECT id FROM distribution_jobs WHERE tenant_id=$1',[self.tenant])['rows'],[])

    def test_wrong_shop_and_processed_order_are_rejected(self):
        with self.assertRaises(HTTPException):self.retry({**self.install,'shop':'other.myshopify.com'})
        rpc("UPDATE distribution_orders SET status='ACCEPTED' WHERE id=$1",[self.order['id']])
        with self.assertRaises(HTTPException):self.retry()
        self.assertEqual(rpc('SELECT connection_id FROM distribution_orders WHERE id=$1',[self.order['id']])['rows'][0]['connection_id'],self.connection)

    def test_disabled_target_rolls_back(self):
        rpc('UPDATE distribution_connections SET orders_enabled=false WHERE id=$1',[self.target])
        with self.assertRaises(HTTPException):self.retry()
        self.assertEqual(rpc('SELECT status FROM distribution_webhooks WHERE id=$1',[self.webhook])['rows'][0]['status'],'DONE')

    def test_retry_accepts_already_corrected_product_mapping(self):
        rpc('UPDATE distribution_shopify_variants SET connection_id=$1 WHERE installation_id=$2',[self.target,self.install['id']])
        self.assertTrue(self.retry()['queued'])

    def test_shipment_updates_local_inventory_with_scheduled_sync_off(self):
        self.retry()
        with patch.object(shopify,'graphql',return_value={'order':self.source_order()}):shopify.process_inbox_once()
        with patch.object(execution,'schedule'):execution.run_once()
        order=rpc('SELECT * FROM distribution_orders WHERE tenant_id=$1',[self.tenant])['rows'][0]
        rpc("UPDATE distribution_simulated_orders SET status='SHIPPED' WHERE order_id=$1",[order['id']])
        rpc("UPDATE distribution_jobs SET status='DONE' WHERE tenant_id=$1 AND kind='PUBLISH'",[self.tenant])
        with s.transaction() as cur:
            s.enqueue(cur,self.tenant,self.target,'TRACKING',order['id'])
        with patch.object(execution,'schedule'), patch('app.distribution.simulated_fulfillment.fulfill_simulated_order'):
            execution.run_once()
        stock=rpc('SELECT distributor_quantity,reserved_quantity FROM distribution_availability WHERE connection_id=$1',[self.target])['rows'][0]
        self.assertEqual(stock,{'distributor_quantity':19,'reserved_quantity':0})
        self.assertEqual(rpc('SELECT status FROM distribution_orders WHERE id=$1',[order['id']])['rows'][0]['status'],'SHIPPED')

    def test_enabling_orders_cannot_disable_configured_simulator_stock(self):
        from app.distribution.router import configure_execution
        from app.distribution.models import ExecutionIn
        rpc("UPDATE distribution_connections SET status='CONNECTED' WHERE id=$1",[self.target])
        configure_execution(self.target,ExecutionIn(enabled=True,orders_enabled=True,inventory_enabled=False),self.ctx)
        row=rpc('SELECT enabled,orders_enabled,inventory_enabled FROM distribution_connections WHERE id=$1',[self.target])['rows'][0]
        self.assertEqual(row,{'enabled':True,'orders_enabled':True,'inventory_enabled':True})
