import unittest
from unittest.mock import Mock, patch
from app.distribution import simulated_fulfillment as f
from app.distribution.adapters import ConfigurationRequired

class FulfillmentTests(unittest.TestCase):
    def setUp(self):
        self.conn={'id':'connection','tenant_id':'tenant','adapter':'simulator','shopify_installation_id':'installation'}
        self.order={'id':'order','source':'SHOPIFY','external_order_id':'gid://shopify/Order/1','source_account':'test.myshopify.com'}
        self.install={'id':'installation','tenant_id':'tenant','location_id':'location'}
        self.cur=Mock();self.cur.fetchall.return_value=[{'edition_id':'edition','quantity':1}]
        self.line={'id':'line','totalQuantity':1,'remainingQuantity':1,'lineItem':{'variant':{'id':'variant'}}}
        self.group={'id':'group','status':'OPEN','assignedLocation':{'location':{'id':'location'}},'lineItems':{'nodes':[self.line],'pageInfo':{'hasNextPage':False}}}
        self.source={'id':self.order['external_order_id'],'cancelledAt':None,'displayFinancialStatus':'PAID','fulfillmentOrders':{'nodes':[self.group],'pageInfo':{'hasNextPage':False}}}
        self.one=patch.object(f.s,'one',side_effect=[self.install,{'status':'SHIPPED'},{'edition_id':'edition'}]).start()
        self.dev=patch.object(f,'require_development_store').start()
        self.graph=patch.object(f.sh,'graphql',side_effect=[{'order':self.source},{'fulfillmentCreate':{'fulfillment':{'id':'fulfilled','status':'SUCCESS'},'userErrors':[]}}]).start()
        self.addCleanup(patch.stopall)
    def test_exact_items_no_buyer_notification(self):
        f.fulfill_simulated_order(self.cur,self.conn,self.order)
        self.dev.assert_called_once_with(self.cur,self.install)
        payload=self.graph.call_args.args[3]['fulfillment']
        self.assertEqual(payload,{'notifyCustomer':False,'lineItemsByFulfillmentOrder':[{'fulfillmentOrderId':'group','fulfillmentOrderLineItems':[{'id':'line','quantity':1}]}]})
        self.assertIn('shopify_fulfillment_reconciled=true',self.cur.execute.call_args.args[0])
    def test_retry_after_success_does_not_create_again(self):
        self.line['remainingQuantity']=0;self.group['status']='CLOSED'
        f.fulfill_simulated_order(self.cur,self.conn,self.order)
        self.assertEqual(self.graph.call_count,1)
    def test_partial_fulfillment_is_not_replayed(self):
        self.line['totalQuantity']=2
        self.cur.fetchall.return_value=[{'edition_id':'edition','quantity':2}]
        with self.assertRaises(ConfigurationRequired):f.fulfill_simulated_order(self.cur,self.conn,self.order)
        self.assertEqual(self.graph.call_count,1)
    def test_wrong_installation_rejected(self):
        self.conn['shopify_installation_id']='old-installation'
        with self.assertRaises(ConfigurationRequired):f.fulfill_simulated_order(self.cur,self.conn,self.order)
        self.graph.assert_not_called()
    def test_live_store_rejected(self):
        self.dev.side_effect=ConfigurationRequired('Not development')
        with self.assertRaises(ConfigurationRequired):f.fulfill_simulated_order(self.cur,self.conn,self.order)
        self.graph.assert_not_called()
    def test_cancelled_order_rejected(self):
        self.source['cancelledAt']='2026-10-30'
        with self.assertRaises(ConfigurationRequired):f.fulfill_simulated_order(self.cur,self.conn,self.order)
        self.assertEqual(self.graph.call_count,1)
    def test_wrong_location_rejected(self):
        self.group['assignedLocation']['location']['id']='other'
        with self.assertRaises(ConfigurationRequired):f.fulfill_simulated_order(self.cur,self.conn,self.order)
        self.assertEqual(self.graph.call_count,1)
    def test_synthetic_order_does_not_touch_shopify(self):
        self.order['external_order_id']='simulation-123'
        f.fulfill_simulated_order(self.cur,self.conn,self.order)
        self.graph.assert_not_called()
    def test_shopify_error_does_not_record_success(self):
        self.graph.side_effect=[{'order':self.source},{'fulfillmentCreate':{'fulfillment':None,'userErrors':[{'message':'Access denied'}]}}]
        with self.assertRaises(ConfigurationRequired):f.fulfill_simulated_order(self.cur,self.conn,self.order)
        self.assertNotIn('shopify_fulfillment_reconciled',self.cur.execute.call_args.args[0])
