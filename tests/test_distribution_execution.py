"""Contract and database regression tests. Uses disposable PGlite, never DATABASE_URL."""
import json
import unittest
from datetime import datetime, timezone, timedelta
from uuid import uuid4
from unittest.mock import patch, MagicMock
from fastapi import HTTPException
from pydantic import ValidationError
import test_distribution_sql as support
from app.distribution import service as s, execution as x
from app.distribution import protocol as p, shopify_execution as sx
from app.distribution.models import ExecutionIn
from app.distribution.adapters import ConfigurationRequired
from app.distribution.router import configure_execution


class DistributorContractTests(unittest.TestCase):
    def snapshot(self, **changes):
        return {'version':1,'as_of':datetime.now(timezone.utc),'complete':True,
                'items':[{'isbn':'9780306406157','on_hand':20}],**changes}

    def test_rejects_bad_inventory_without_coercing_quantities(self):
        for quantity in (-1, True, 1.5, '20'):
            with self.subTest(quantity=quantity), self.assertRaises(ValidationError):
                p.Snapshot.model_validate(self.snapshot(items=[{'isbn':'9780306406157','on_hand':quantity}]))
        for changes in ({'as_of':datetime.now()}, {'as_of':datetime.now(timezone.utc)-timedelta(days=2)},
                        {'as_of':datetime.now(timezone.utc)+timedelta(hours=1)}, {'version':2},
                        {'items':[{'isbn':'9780306406157','on_hand':20}]*2}):
            with self.subTest(changes=changes), self.assertRaises(ValidationError):
                p.Snapshot.model_validate(self.snapshot(**changes))

    def test_gateway_contract_requires_idempotency(self):
        adapter=p.StandardJSONAdapter({'api_url':'https://gateway.example/v1'},{'username':'test','password':'secret'})
        client=MagicMock()
        with patch.object(adapter,'client',return_value=client):
            client.request.return_value=b'{"protocol":"inksuite-distributor","version":1,"idempotent_orders":false}'
            with self.assertRaises(ConfigurationRequired): adapter.test_connection()
            client.request.return_value=b'{"protocol":"inksuite-distributor","version":1,"idempotent_orders":true}'
            adapter.test_connection()

    def test_order_key_is_stable_and_only_delivery_fields_leave_the_boundary(self):
        key=str(uuid4()); adapter=p.StandardJSONAdapter(); client=MagicMock()
        client.request.return_value=json.dumps({'order_key':key,'distributor_order_id':'D-1','status':'ACCEPTED'}).encode()
        order={'id':key,'recipient':{'name':'Recipient','address_1':'Test address'},'shipping_method':'Ground',
               'source_account':'private-store','reference':'private-order-reference','delivery_instructions':'Do not forward'}
        with patch.object(adapter,'client',return_value=client):
            for _ in range(2): adapter.submit_order(order,[{'isbn':'9780306406157','quantity':1}])
        self.assertEqual(client.request.call_args_list[0],client.request.call_args_list[1])
        method,path,body=client.request.call_args.args
        self.assertEqual((method,path),('PUT','orders/'+key))
        payload=json.loads(body)
        self.assertEqual(payload['reference'],key)
        self.assertNotIn('source_account',payload); self.assertNotIn('delivery_instructions',payload)

    def test_status_absence_is_explicit_not_an_ambiguous_transport_failure(self):
        key=str(uuid4()); adapter=p.StandardJSONAdapter();client=MagicMock()
        with patch.object(adapter,'client',return_value=client):
            client.request.return_value=json.dumps({'order_key':key,'status':'NOT_FOUND'}).encode()
            self.assertIsNone(adapter.get_order_status(key))
            client.request.return_value=b'{}'
            with self.assertRaises(ValidationError): adapter.get_order_status(key)


class DistributorExecutionSQLTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls): support.DistributionSQLTests.setUpClass.__func__(cls)
    @classmethod
    def tearDownClass(cls): support.DistributionSQLTests.tearDownClass.__func__(cls)
    def setUp(self):
        support.DistributionSQLTests.setUp(self)
        self.body=self.body.model_copy(update={'source_account':'execution-'+self.tenant+'.myshopify.com'})
        support.rpc('UPDATE distribution_connections SET enabled=false')
        support.rpc("UPDATE distribution_jobs SET status='DONE'")
    create=support.DistributionSQLTests.create
    live=support.DistributionSQLTests.live

    def simulator(self, quantity=10):
        support.rpc("""UPDATE distribution_connections SET adapter='simulator',enabled=true,mode='preview',
            status='CONNECTED',orders_enabled=true,inventory_enabled=true,safety_buffer=0,
            next_sync_at=now()+interval '1 day',configuration=$1 WHERE id=$2""",
            [{'simulated_stock':{'9780000000002':quantity}},self.connection])
        self.synchronize()

    def synchronize(self):
        with s.transaction() as cur:
            conn=s.connection(cur,self.tenant,self.connection,lock=True)
            x.reconcile(cur,conn,x.adapter(cur,conn).fetch_inventory())

    def new_order(self, **changes):
        with s.transaction() as cur:
            return s.create_order(cur,self.tenant,self.body.model_copy(update=changes),self.user,automated=True)

    def quantities(self):
        return support.rpc('SELECT distributor_quantity,reserved_quantity,sellable_quantity FROM distribution_availability WHERE tenant_id=$1',[self.tenant])['rows'][0]

    def order(self, oid):
        return support.rpc('SELECT * FROM distribution_orders WHERE id=$1',[oid])['rows'][0]

    def test_complete_simulator_lifecycle_has_no_buyer_data_or_duplicate_order(self):
        self.simulator(10);order=self.new_order()
        self.assertEqual(self.new_order()['id'],order['id'])
        self.assertEqual(self.quantities()['sellable_quantity'],9)
        with patch.object(sx,'delivery_order',side_effect=AssertionError('Simulation must not fetch buyer details')):
            self.assertTrue(x.run_once())
        self.assertEqual(self.order(order['id'])['status'],'ACCEPTED')
        self.synchronize()
        self.assertEqual(self.quantities(),{'distributor_quantity':10,'reserved_quantity':1,'sellable_quantity':9})
        support.rpc("UPDATE distribution_simulated_orders SET status='SHIPPED' WHERE order_id=$1",[order['id']])
        with s.transaction() as cur: s.enqueue(cur,self.tenant,self.connection,'TRACKING',order['id'])
        self.assertTrue(x.run_once());self.synchronize()
        self.assertEqual(self.quantities(),{'distributor_quantity':9,'reserved_quantity':0,'sellable_quantity':9})
        self.assertEqual(self.order(order['id'])['status'],'SHIPPED')
        self.assertEqual(self.order(order['id'])['recipient'],{})
        self.assertEqual(support.rpc('SELECT count(*) AS n FROM distribution_simulated_orders WHERE tenant_id=$1',[self.tenant])['rows'][0]['n'],1)
        serialized=json.dumps(support.rpc('SELECT * FROM distribution_events WHERE tenant_id=$1',[self.tenant]))
        self.assertNotIn('Test Rd',serialized)

    def test_reservation_prevents_overselling_last_copy(self):
        self.simulator(1);self.new_order()
        with self.assertRaises(HTTPException): self.new_order(external_order_id='101')
        self.assertEqual(self.quantities()['sellable_quantity'],0)
        self.assertEqual(support.rpc('SELECT count(*) AS n FROM distribution_orders WHERE tenant_id=$1',[self.tenant])['rows'][0]['n'],1)

    def test_preview_is_not_promoted_and_old_live_orders_are_not_backfilled(self):
        original=self.create()
        support.rpc("UPDATE distribution_connections SET adapter='standard_json',enabled=true,mode='live',orders_enabled=true,orders_enabled_at=now() WHERE id=$1",[self.connection])
        self.assertEqual(self.new_order()['mode'],'preview')
        with s.transaction() as cur:
            old=s.create_order(cur,self.tenant,self.body.model_copy(update={'external_order_id':'old'}),self.user,
                               automated=True,source_created_at=datetime.now(timezone.utc)-timedelta(days=1))
            new=s.create_order(cur,self.tenant,self.body.model_copy(update={'external_order_id':'new'}),self.user,
                               automated=True,source_created_at=datetime.now(timezone.utc)+timedelta(seconds=1))
        self.assertEqual(old['mode'],'preview');self.assertEqual(new['mode'],'live')
        self.assertEqual(self.order(original['id'])['status'],'RECEIVED')
        self.assertEqual(self.order(new['id'])['status'],'QUEUED')

    def test_stale_or_out_of_order_snapshot_does_not_mutate_stock(self):
        self.simulator(10)
        with self.assertRaises(ConfigurationRequired):
            with s.transaction() as cur:
                conn=s.connection(cur,self.tenant,self.connection)
                x.reconcile(cur,conn,p.Snapshot(as_of=datetime.now(timezone.utc)-timedelta(hours=1),complete=True,items=[]))
        self.assertEqual(self.quantities()['distributor_quantity'],10)

    def test_partial_snapshot_cannot_release_unreported_shipment(self):
        self.simulator(10);order=self.new_order();x.run_once()
        support.rpc("UPDATE distribution_orders SET status='SHIPPED' WHERE id=$1",[order['id']])
        with self.assertRaises(ConfigurationRequired):
            with s.transaction() as cur:
                conn=s.connection(cur,self.tenant,self.connection)
                x.reconcile(cur,conn,p.Snapshot(as_of=datetime.now(timezone.utc),complete=False,items=[],reflected_order_keys=[order['id']]))
        self.assertEqual(self.quantities()['reserved_quantity'],1)

    def test_unknown_reflected_order_cannot_release_other_publisher_stock(self):
        self.simulator(10);self.new_order()
        with s.transaction() as cur:
            x.reconcile(cur,s.connection(cur,self.tenant,self.connection),p.Snapshot(as_of=datetime.now(timezone.utc),complete=False,items=[],reflected_order_keys=[uuid4()]))
        self.assertEqual(self.quantities()['distributor_quantity'],10)
        self.assertEqual(self.quantities()['reserved_quantity'],1)

    def test_complete_snapshot_zeroes_omissions_but_delta_does_not(self):
        self.simulator(10)
        for complete,expected in ((False,10),(True,0)):
            with s.transaction() as cur:
                x.reconcile(cur,s.connection(cur,self.tenant,self.connection),p.Snapshot(as_of=datetime.now(timezone.utc),complete=complete,items=[]))
            self.assertEqual(self.quantities()['distributor_quantity'],expected)

    def test_uncertain_retry_reconciles_original_order_without_resending(self):
        self.simulator(10);order=self.new_order()
        support.rpc("UPDATE distribution_orders SET status='SUBMITTING' WHERE id=$1",[order['id']])
        job=support.rpc('SELECT * FROM distribution_jobs WHERE order_id=$1',[order['id']])['rows'][0];job['attempts']=2
        driver=MagicMock(simulated=True,idempotent_submission=True)
        driver.get_order_status.return_value=p.Receipt(order_key=order['id'],distributor_order_id='existing-1',status='ACCEPTED')
        with patch.object(x,'adapter',return_value=driver):
            with s.transaction() as cur:x.execute(cur,job)
        driver.submit_order.assert_not_called()
        self.assertEqual(self.order(order['id'])['status'],'ACCEPTED')

    def test_receipt_identity_mismatch_cannot_acknowledge_order(self):
        self.simulator(10);order=self.new_order()
        with self.assertRaises(ConfigurationRequired):
            with s.transaction() as cur:
                x.apply_receipt(cur,s.connection(cur,self.tenant,self.connection),self.order(order['id']),
                    p.Receipt(order_key=uuid4(),distributor_order_id='wrong',status='ACCEPTED'))
        self.assertEqual(self.order(order['id'])['status'],'QUEUED')

    def test_enabling_live_orders_requires_test_and_exporter_confirmation(self):
        support.rpc("UPDATE distribution_connections SET adapter='standard_json' WHERE id=$1",[self.connection])
        body=ExecutionIn(enabled=True,orders_enabled=True)
        with self.assertRaises(HTTPException): configure_execution(self.connection,body,self.ctx)
        support.rpc("UPDATE distribution_connections SET status='CONNECTED' WHERE id=$1",[self.connection])
        with self.assertRaises(HTTPException): configure_execution(self.connection,body,self.ctx)
        row=configure_execution(self.connection,body.model_copy(update={'exclusive_fulfillment_confirmed':True}),self.ctx)
        self.assertTrue(row['enabled']);self.assertIsNotNone(row['orders_enabled_at'])
        with self.assertRaises(HTTPException): configure_execution(self.connection,body,{**self.ctx,'tenant':{'id':self.other}})

    def test_stale_stock_publication_is_scheduled_independently_of_feed_success(self):
        self.simulator(10)
        support.rpc("UPDATE distribution_connections SET adapter='standard_json',mode='live',next_sync_at=now() WHERE id=$1",[self.connection])
        support.rpc("INSERT INTO distribution_shopify_installations(tenant_id,shop,secret_reference,inventory_push_enabled) VALUES($1,$2,'test',true)",[self.tenant,self.tenant+'.myshopify.com'])
        with s.transaction() as cur:x.schedule(cur)
        kinds={r['kind'] for r in support.rpc('SELECT kind FROM distribution_jobs WHERE tenant_id=$1',[self.tenant])['rows']}
        self.assertEqual(kinds,{'INVENTORY','PUBLISH'})

    def stock_store(self):
        installation=str(uuid4())
        support.rpc("INSERT INTO distribution_shopify_installations(id,tenant_id,shop,secret_reference,location_id,inventory_push_enabled) VALUES($1,$2,$3,'test','gid://shopify/Location/1',true)",[installation,self.tenant,self.body.source_account])
        support.rpc("INSERT INTO distribution_shopify_variants(tenant_id,installation_id,variant_id,edition_id,connection_id,inventory_item_id) VALUES($1,$2,'gid://shopify/ProductVariant/1',$3,$4,'gid://shopify/InventoryItem/1')",[self.tenant,installation,self.ed,self.connection])
        return installation

    def test_inventory_publication_uses_compare_and_set_and_zeroes_stale_stock(self):
        self.simulator(10);self.stock_store()
        support.rpc("UPDATE distribution_connections SET adapter='standard_json',mode='live' WHERE id=$1",[self.connection])
        response={'inventoryItem':{'tracked':True,'inventoryLevel':{'quantities':[{'name':'on_hand','quantity':40},{'name':'committed','quantity':0}]}}}
        for stale,target in ((False,10),(True,0)):
            if stale:support.rpc("UPDATE distribution_inventory SET last_synced_at=now()-interval '2 days' WHERE tenant_id=$1",[self.tenant])
            with patch.object(sx.sh,'graphql',side_effect=[response,{'inventorySetQuantities':{'userErrors':[]}}]) as graphql:
                with s.transaction() as cur:sx.push_inventory(cur,s.connection(cur,self.tenant,self.connection))
            variables=graphql.call_args.args[3]
            self.assertEqual(variables['input']['quantities'][0]['quantity'],target)
            self.assertEqual(variables['input']['quantities'][0]['changeFromQuantity'],40)
            self.assertIn('@idempotent',graphql.call_args.args[2]);self.assertTrue(variables['key'])

    def test_shared_pool_is_not_duplicated_across_variants(self):
        self.simulator(10);install=self.stock_store()
        support.rpc("UPDATE distribution_connections SET adapter='standard_json',mode='live' WHERE id=$1",[self.connection])
        support.rpc("INSERT INTO distribution_shopify_variants(tenant_id,installation_id,variant_id,edition_id,connection_id,inventory_item_id) VALUES($1,$2,'gid://shopify/ProductVariant/2',$3,$4,'gid://shopify/InventoryItem/2')",[self.tenant,install,self.ed,self.connection])
        with patch.object(sx.sh,'graphql') as graphql:
            with self.assertRaises(ConfigurationRequired):
                with s.transaction() as cur:sx.push_inventory(cur,s.connection(cur,self.tenant,self.connection))
            graphql.assert_not_called()

    def demo_install(self):
        installation=self.stock_store()
        return {'id':installation,'tenant_id':self.tenant,'user_id':self.user}

    def test_set_stock_saves_and_queues_without_enabling_orders(self):
        from app.distribution.simulated_inventory import set_test_stock
        self.simulator(10);install=self.demo_install()
        support.rpc('UPDATE distribution_shopify_installations SET inventory_push_enabled=false WHERE id=$1',[install['id']])
        support.rpc('UPDATE distribution_connections SET enabled=false WHERE id=$1',[self.connection])
        with patch.object(sx.sh,'graphql',return_value={'shop':{'plan':{'partnerDevelopment':True}}}):
            self.assertTrue(set_test_stock(install,self.connection,self.ed,20,2)['queued'])
            self.assertTrue(set_test_stock(install,self.connection,self.ed,20,2)['queued'])
        self.assertEqual(self.quantities()['sellable_quantity'],18)
        store=support.rpc('SELECT inventory_push_enabled,simulated_inventory_connection_id FROM distribution_shopify_installations WHERE id=$1',[install['id']])['rows'][0]
        self.assertFalse(store['inventory_push_enabled']);self.assertEqual(store['simulated_inventory_connection_id'],self.connection)
        with s.transaction() as cur:
            conn=s.connection(cur,self.tenant,self.connection)
        self.assertTrue(conn['enabled']);self.assertTrue(conn['inventory_enabled']);self.assertFalse(conn['orders_enabled'])
        jobs=support.rpc("SELECT kind FROM distribution_jobs WHERE tenant_id=$1 AND status='QUEUED'",[self.tenant])['rows']
        self.assertEqual(jobs,[{'kind':'PUBLISH'}])

    def test_set_stock_assigns_unassigned_variant_after_reconnect(self):
        from app.distribution.simulated_inventory import set_test_stock
        self.simulator(10);install=self.demo_install()
        support.rpc('UPDATE distribution_shopify_variants SET connection_id=NULL WHERE installation_id=$1',[install['id']])
        with patch.object(sx.sh,'graphql',return_value={'shop':{'plan':{'partnerDevelopment':True}}}):
            set_test_stock(install,self.connection,self.ed,20,5)
        rows=support.rpc('SELECT connection_id FROM distribution_shopify_variants WHERE installation_id=$1',[install['id']])['rows']
        self.assertEqual(rows,[{'connection_id':self.connection}])
        self.assertEqual(self.quantities()['sellable_quantity'],15)

    def test_set_stock_rejects_missing_variant_without_saving_quantity(self):
        from app.distribution.simulated_inventory import set_test_stock
        self.simulator(10);install=self.demo_install()
        support.rpc('DELETE FROM distribution_shopify_variants WHERE installation_id=$1',[install['id']])
        with patch.object(sx.sh,'graphql',return_value={'shop':{'plan':{'partnerDevelopment':True}}}),self.assertRaises(HTTPException):
            set_test_stock(install,self.connection,self.ed,20,5)
        self.assertEqual(self.quantities()['distributor_quantity'],10)

    def test_set_stock_does_not_take_another_distributors_variant(self):
        from app.distribution.simulated_inventory import set_test_stock
        self.simulator(10);install=self.demo_install();other=str(uuid4())
        support.rpc("INSERT INTO distribution_connections(id,tenant_id,display_name,adapter) VALUES($1,$2,'Other','simulator')",[other,self.tenant])
        support.rpc('UPDATE distribution_shopify_variants SET connection_id=$1 WHERE installation_id=$2',[other,install['id']])
        with patch.object(sx.sh,'graphql',return_value={'shop':{'plan':{'partnerDevelopment':True}}}),self.assertRaises(HTTPException):
            set_test_stock(install,self.connection,self.ed,20,5)
        self.assertEqual(support.rpc('SELECT connection_id FROM distribution_shopify_variants WHERE installation_id=$1',[install['id']])['rows'][0]['connection_id'],other)

    def test_set_stock_rejects_live_store_missing_plan_and_cross_tenant(self):
        from app.distribution.simulated_inventory import set_test_stock
        self.simulator(10);install=self.demo_install()
        for response in ({'shop':{'plan':{'partnerDevelopment':False}}},{'shop':None},{}):
            with patch.object(sx.sh,'graphql',return_value=response),self.assertRaises(HTTPException):
                set_test_stock(install,self.connection,self.ed,99,0)
        self.assertEqual(self.quantities()['distributor_quantity'],10)
        with patch.object(sx.sh,'graphql') as gql,self.assertRaises(HTTPException):
            set_test_stock({**install,'tenant_id':self.other},self.connection,self.ed,99,0)
        gql.assert_not_called()

    def test_simulator_cannot_publish_without_set_stock_or_after_store_conversion(self):
        self.simulator(10);install=self.demo_install()
        with patch.object(sx.sh,'graphql') as gql:
            with s.transaction() as cur:sx.push_inventory(cur,s.connection(cur,self.tenant,self.connection))
        gql.assert_not_called()
        support.rpc('UPDATE distribution_shopify_installations SET simulated_inventory_connection_id=$1 WHERE id=$2',[self.connection,install['id']])
        with patch.object(sx.sh,'graphql',return_value={'shop':{'plan':{'partnerDevelopment':False}}}) as gql:
            with self.assertRaises(ConfigurationRequired):
                with s.transaction() as cur:sx.push_inventory(cur,s.connection(cur,self.tenant,self.connection))
        self.assertEqual(gql.call_count,1);self.assertEqual(gql.call_args.args[2],sx.DEVELOPMENT_STORE_QUERY)

    def test_simulated_reservations_reduce_shopify_stock_without_fake_order_fetch(self):
        from app.distribution.simulated_inventory import set_test_stock
        self.simulator(20);install=self.demo_install()
        support.rpc('UPDATE distribution_shopify_installations SET inventory_push_enabled=false WHERE id=$1',[install['id']])
        self.new_order(external_order_id='simulation-local')
        development={'shop':{'plan':{'partnerDevelopment':True}}}
        with patch.object(sx.sh,'graphql',return_value=development):
            set_test_stock(install,self.connection,self.ed,20,2)
        level={'inventoryItem':{'tracked':True,'inventoryLevel':{'quantities':[{'name':'on_hand','quantity':-2},{'name':'committed','quantity':0}]}}}
        with patch.object(sx.sh,'graphql',side_effect=[development,level,{'inventorySetQuantities':{'userErrors':[]}}]) as gql:
            with s.transaction() as cur:x.execute(cur,{'tenant_id':self.tenant,'connection_id':self.connection,'kind':'PUBLISH'})
        self.assertEqual(gql.call_args.args[3]['input']['quantities'][0]['quantity'],17)
        self.assertEqual(gql.call_args.args[3]['input']['quantities'][0]['changeFromQuantity'],-2)
        self.assertFalse(any(c.args[2]==sx.sh.ORDER_QUERY for c in gql.call_args_list))

    def test_test_stock_requires_saved_location_and_keeps_live_settings_unchanged(self):
        from app.distribution.simulated_inventory import set_test_stock
        self.simulator(10);install=self.demo_install()
        support.rpc('UPDATE distribution_shopify_installations SET location_id=NULL WHERE id=$1',[install['id']])
        with patch.object(sx.sh,'graphql') as gql,self.assertRaises(HTTPException):
            set_test_stock(install,self.connection,self.ed,20,0)
        gql.assert_not_called()
        self.assertEqual(self.quantities()['distributor_quantity'],10)

    def test_unchanged_simulator_receipt_does_not_schedule_busy_loop(self):
        self.simulator(10);order=self.new_order()
        x.run_once()
        support.rpc("UPDATE distribution_connections SET next_sync_at=now()+interval '1 day' WHERE id=$1",[self.connection])
        with s.transaction() as cur:
            conn=s.connection(cur,self.tenant,self.connection)
            before=conn['next_sync_at']
            current=self.order(order['id'])
            x.apply_receipt(cur,conn,current,p.Receipt(order_key=order['id'],distributor_order_id=current['distributor_order_id'],status='ACCEPTED'))
            self.assertEqual(s.connection(cur,self.tenant,self.connection)['next_sync_at'],before)


if __name__=='__main__': unittest.main()
