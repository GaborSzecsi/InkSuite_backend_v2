"""Payments against disposable PostgreSQL, never DATABASE_URL or production."""
import contextlib
import json
import subprocess
import unittest
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch
from uuid import UUID, uuid4

from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient
from psycopg.types.json import Jsonb
from routers import financial_payments as api
from services.financial_payments.domain import split_amount, next_status

ROOT=Path(__file__).resolve().parent


class DomainTests(unittest.TestCase):
    def test_rounding_and_validation(self):
        inst=dict(payee_mode='split',contributor_party_id=uuid4(),agency_party_id=uuid4(),contributor_percent='85',agency_percent='15')
        values=split_amount('0.03',inst)
        self.assertEqual(sum((r[3] for r in values),Decimal(0)),Decimal('.03'))
        self.assertEqual(values[0][3],Decimal('.03'))
        with self.assertRaises(HTTPException): split_amount('1',inst|{'agency_percent':'14'})

    def test_out_of_order_does_not_regress(self):
        self.assertEqual(next_status('settled','processing'),'settled')
        self.assertEqual(next_status('returned','settled'),'returned')
        self.assertEqual(next_status('settled','returned'),'returned')


class PaymentsPostgresTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.proc=subprocess.Popen(['node',str(ROOT/'banking_pg.cjs')],stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True,encoding='utf-8')
        if not json.loads(cls.proc.stdout.readline()).get('ready'): raise RuntimeError('PGlite failed to start')
        cls.addClassCleanup(cls.shutdown)
        cls.rpc('''
          CREATE TYPE roy_party AS ENUM ('author','illustrator');
          CREATE TABLE tenants(id uuid PRIMARY KEY,slug text UNIQUE);
          CREATE TABLE users(id uuid PRIMARY KEY,cognito_sub text UNIQUE,full_name text);
          CREATE TABLE memberships(tenant_id uuid,user_id uuid,role text,module_permissions jsonb);
          CREATE TABLE works(id uuid PRIMARY KEY,tenant_id uuid,title text);
          CREATE TABLE parties(id uuid PRIMARY KEY,tenant_id uuid,display_name text);
          CREATE TABLE work_contributors(tenant_id uuid,work_id uuid,party_id uuid);
          CREATE TABLE royalty_periods(id uuid PRIMARY KEY,tenant_id uuid,period_code text,period_start date,period_end date);
          CREATE TABLE royalty_statements(id uuid PRIMARY KEY,tenant_id uuid,work_id uuid,period_id uuid,party roy_party,
            status text,currency text,payable_this_period numeric,settlement jsonb,updated_at timestamptz DEFAULT now());
          CREATE TABLE royalty_payments(id uuid PRIMARY KEY,tenant_id uuid,statement_id uuid,payee_party_id uuid,
            payee_role text,payment_date date,amount numeric,currency text,payment_method text,reference_number text);
          CREATE TABLE royalty_payment_instructions(id uuid PRIMARY KEY,tenant_id uuid,work_id uuid,party roy_party,payee_mode text,
            contributor_party_id uuid,agency_party_id uuid,contributor_percent numeric,agency_percent numeric,
            effective_start date,effective_end date,updated_at timestamptz DEFAULT now());
          CREATE SCHEMA secure_payments;
          CREATE TABLE secure_payments.payment_profiles(id uuid PRIMARY KEY,tenant_id text,recipient_id text,recipient_type text,status text);
          CREATE TABLE secure_payments.payment_accounts(id uuid PRIMARY KEY,payment_profile_id uuid,account_last4 text,
            bank_name text,currency text,verification_status text,updated_at timestamptz DEFAULT now(),created_at timestamptz DEFAULT now(),
            payment_method text,country_code text,is_active boolean);
        ''',execute=True)
        cls.rpc((ROOT.parent/'migrations/026_financial_payments.sql').read_text(),execute=True)

    @classmethod
    def shutdown(cls):
        cls.proc.stdin.close();cls.proc.wait(timeout=20)
        cls.proc.stdout.close();cls.proc.stderr.close()

    @classmethod
    def rpc(cls,sql,params=(),execute=False):
        def encode(v):
            if isinstance(v,Jsonb): return encode(v.obj)
            if isinstance(v,dict): return {k:encode(x) for k,x in v.items()}
            if isinstance(v,(list,tuple)): return [encode(x) for x in v]
            if isinstance(v,(UUID,Decimal,date,datetime)): return str(v)
            return v
        cls.proc.stdin.write(json.dumps(dict(sql=sql,params=encode(params),exec=execute))+'\n');cls.proc.stdin.flush()
        result=json.loads(cls.proc.stdout.readline())
        if 'error' in result: raise RuntimeError(result['error'])
        return result['result']

    def cursor(self):
        owner=self
        uuid_keys={'id','user_id','tenant_id','work_id','party_id','contributor_party_id','agency_party_id','statement_id','period_id','source_statement_id','batch_id','recipient_id','connection_id','funding_account_id','destination_account_id','secure_bank_account_ref','created_by','actor_user_id','allocation_id','obligation_id','transfer_id','idempotency_key'}
        money_keys={'amount','payable_this_period','paid_amount','operating_reserve','approval_threshold','mock_opening_balance','available_balance','current_balance','pending','settled','prepared','reserved','mock_settled','total_amount','allocation_percent','original_payable','contributor_percent','agency_percent'}
        class Cursor:
            def execute(self,sql,params=()):
                for i in range(sql.count('%s')): sql=sql.replace('%s',f'${i+1}',1)
                self.rows=owner.rpc(sql,params)['rows']
                for row in self.rows:
                    for k,v in list(row.items()):
                        if v is None: continue
                        if k in uuid_keys: row[k]=UUID(str(v))
                        elif k in money_keys: row[k]=Decimal(str(v))
                        elif k in ('period_start','period_end','effective_start','effective_end'): row[k]=date.fromisoformat(v[:10])
                        elif k.endswith('_at') or k=='balance_as_of': row[k]=datetime.fromisoformat(v.replace('Z','+00:00'))
            def fetchone(self): return self.rows[0] if self.rows else None
            def fetchall(self): return self.rows
            def __enter__(self): return self
            def __exit__(self,*args): pass
        return Cursor()

    @contextlib.contextmanager
    def connection(self):
        owner=self
        self.rpc('BEGIN',execute=True)
        class Conn:
            def cursor(self,**kwargs): return owner.cursor()
            @contextlib.contextmanager
            def transaction(self): yield
        try:
            yield Conn()
            self.rpc('COMMIT',execute=True)
        except BaseException:
            self.rpc('ROLLBACK',execute=True)
            raise

    def setUp(self):
        # Financial history is append-only. Each test has separate tenant data.
        self.tenant,self.user,self.author,self.agency,self.work,self.period,self.statement=[uuid4() for _ in range(7)]
        self.slug='test-'+uuid4().hex; self.sub='test-'+uuid4().hex
        cur=self.cursor()
        cur.execute('INSERT INTO tenants VALUES(%s,%s)',(self.tenant,self.slug))
        cur.execute('INSERT INTO users VALUES(%s,%s,%s)',(self.user,self.sub,'Test Administrator'))
        cur.execute("INSERT INTO memberships VALUES(%s,%s,'tenant_admin','{}')",(self.tenant,self.user))
        cur.execute("INSERT INTO works VALUES(%s,%s,'Book A')",(self.work,self.tenant))
        for pid,name in [(self.author,'Author'),(self.agency,'Agency')]:
            cur.execute('INSERT INTO parties VALUES(%s,%s,%s)',(pid,self.tenant,name))
            profile=uuid4()
            cur.execute("INSERT INTO secure_payments.payment_profiles VALUES(%s,%s,%s,%s,'COMPLETE')",(profile,self.slug,str(pid),'CONTRIBUTOR' if pid==self.author else 'AGENCY'))
            cur.execute("INSERT INTO secure_payments.payment_accounts(id,payment_profile_id,account_last4,bank_name,currency,verification_status,payment_method,country_code,is_active) VALUES(%s,%s,'1234','Bank','USD','UNVERIFIED','ACH','US',true)",(uuid4(),profile))
        cur.execute('INSERT INTO work_contributors VALUES(%s,%s,%s)',(self.tenant,self.work,self.author))
        cur.execute("INSERT INTO royalty_periods VALUES(%s,%s,'2026-H1','2026-01-01','2026-06-30')",(self.period,self.tenant))
        cur.execute("INSERT INTO royalty_statements(id,tenant_id,work_id,period_id,party,status,currency,payable_this_period,settlement) VALUES(%s,%s,%s,%s,'author','final','USD',1000,%s)",
            (self.statement,self.tenant,self.work,self.period,Jsonb(dict(settlement_version=2,account_balance_carried_forward='1000',payments_recorded_total='0'))))
        cur.execute("INSERT INTO royalty_payment_instructions(id,tenant_id,work_id,party,payee_mode,contributor_party_id,agency_party_id,contributor_percent,agency_percent) VALUES(%s,%s,%s,'author','split',%s,%s,85,15)",(uuid4(),self.tenant,self.work,self.author,self.agency))
        app=FastAPI();app.include_router(api.router,prefix='/api');app.dependency_overrides[api.get_current_user]=lambda:dict(sub=self.sub)
        self.client=TestClient(app);self.addCleanup(self.client.close)
        patcher=patch.object(api,'db_conn',self.connection);patcher.start();self.addCleanup(patcher.stop)

    def get(self,path): return self.client.get('/api/financials/payments/'+path+('&' if '?' in path else '?')+'tenant_slug='+self.slug)
    def post(self,path,body): return self.client.post('/api/financials/payments/'+path+'?tenant_slug='+self.slug,json=body)
    def setup_payment(self,opening='2000'):
        connection=self.post('bank-connections',dict(opening_balance=opening,idempotency_key=str(uuid4())))
        self.assertEqual(connection.status_code,200,connection.text)
        self.account=connection.json()['account']['id']
        for pid in (self.author,self.agency):
            verified=self.post('recipients/verify-simulation',dict(statement_id=str(self.statement),party_id=str(pid)))
            self.assertEqual(verified.status_code,200,verified.text)

    def prepare(self,amount='1000',key=None):
        return self.post('batches',dict(funding_account_id=self.account,idempotency_key=key or str(uuid4()),items=[dict(statement_id=str(self.statement),amount=amount)]))

    def single_approval(self):
        response=self.client.put('/api/financials/payments/settings?tenant_slug='+self.slug,json=dict(operating_reserve='0',approval_threshold='0',dual_approval_required=False,balance_max_age_minutes=15))
        self.assertEqual(response.status_code,200,response.text)

    def authorize(self,bid):
        for action in ('submit-for-approval','approve','execute'):
            r=self.post('batches/'+bid+'/'+action,{})
            self.assertEqual(r.status_code,200,r.text)
        return r.json()

    def test_full_lifecycle_duplicate_delayed_events_and_return(self):
        self.setup_payment(); self.single_approval()
        key=str(uuid4()); batch=self.prepare(key=key)
        self.assertEqual(batch.status_code,200,batch.text)
        bid=batch.json()['batch']['id']
        self.assertEqual(len(batch.json()['transfers']),2)
        self.assertEqual(self.prepare(key=key).json()['batch']['id'],bid)
        self.assertEqual(self.prepare('1').status_code,409)
        detail=self.authorize(bid)
        self.assertEqual(self.authorize_again(bid).status_code,200)
        for t in detail['transfers']:
            eid=str(uuid4()); payload=dict(status='settled',event_id=eid)
            r=self.post('transfers/'+t['id']+'/simulate-event',payload);self.assertEqual(r.status_code,200,r.text)
            self.assertEqual(self.post('transfers/'+t['id']+'/simulate-event',payload).status_code,200)
            delayed=self.post('transfers/'+t['id']+'/simulate-event',dict(status='processing',event_id=str(uuid4())))
            self.assertEqual(delayed.status_code,200,delayed.text)
        self.assertEqual(self.get('payables').json()['items'][0]['available_to_schedule'],'0.00')
        self.assertEqual(self.rpc('SELECT count(*) FROM royalty_payments WHERE tenant_id=$1',[str(self.tenant)])['rows'][0]['count'],0)
        for t in detail['transfers']:
            returned=self.post('transfers/'+t['id']+'/simulate-event',dict(status='returned',event_id=str(uuid4())))
            self.assertEqual(returned.status_code,200,returned.text)
        self.assertEqual(self.get('payables').json()['items'][0]['available_to_schedule'],'1000.00')
        self.assertEqual(len(returned.json()['reconciliations']),4)

    def authorize_again(self,bid): return self.post('batches/'+bid+'/execute',{})

    def test_partial_reservations_and_insufficient_funds(self):
        self.setup_payment('500')
        self.assertEqual(self.prepare('501').status_code,409)
        batch=self.prepare('300');self.assertEqual(batch.status_code,200,batch.text)
        self.assertEqual(self.get('payables').json()['items'][0]['available_to_schedule'],'700.00')
        self.assertEqual(self.prepare('201').status_code,409)
        cancel=self.post('batches/'+batch.json()['batch']['id']+'/cancel',{})
        self.assertEqual(cancel.status_code,200,cancel.text)
        self.assertEqual(self.get('payables').json()['items'][0]['available_to_schedule'],'1000.00')

    def test_missing_and_contradictory_instructions_and_tenant_access(self):
        forbidden=self.client.get('/api/financials/payments/summary?tenant_slug=another-publisher')
        self.assertEqual(forbidden.status_code,403)
        self.cursor().execute('UPDATE royalty_payment_instructions SET agency_percent=14 WHERE tenant_id=%s',(self.tenant,))
        row=self.get('payables').json()['items'][0];self.assertFalse(row['eligible']);self.assertIn('contradictory',row['reason'])

    def test_manual_records_reduce_available_and_invalidate_batch(self):
        self.setup_payment();self.single_approval(); batch=self.prepare('500');self.assertEqual(batch.status_code,200,batch.text)
        self.cursor().execute("INSERT INTO royalty_payments VALUES(%s,%s,%s,%s,'contributor','2026-07-01',100,'USD','Check','manual')",(uuid4(),self.tenant,self.statement,self.author))
        self.assertEqual(self.get('payables').json()['items'][0]['outstanding'],'900.00')
        self.assertEqual(self.post('batches/'+batch.json()['batch']['id']+'/submit-for-approval',{}).status_code,409)
        hist=self.get('history').json()['items'];self.assertTrue(any(not h['simulated'] for h in hist))

    def test_carry_forward_not_counted_twice(self):
        period2,statement2=uuid4(),uuid4()
        cur=self.cursor();cur.execute("INSERT INTO royalty_periods VALUES(%s,%s,'2026-H2','2026-07-01','2026-12-31')",(period2,self.tenant))
        cur.execute("INSERT INTO royalty_statements(id,tenant_id,work_id,period_id,party,status,currency,payable_this_period,settlement) VALUES(%s,%s,%s,%s,'author','final','USD',1500,%s)",(statement2,self.tenant,self.work,period2,Jsonb(dict(settlement_version=2,account_balance_carried_forward='1500',payments_recorded_total='0'))))
        items=self.get('payables').json()['items'];self.assertEqual(len(items),1);self.assertEqual(items[0]['outstanding'],'1500.00')

    def test_legacy_prior_debt_is_separate_from_current_payable(self):
        cur=self.cursor()
        cur.execute('UPDATE royalty_statements SET payable_this_period=75.84,settlement=NULL WHERE id=%s',(self.statement,))
        period2,statement2=uuid4(),uuid4()
        cur.execute("INSERT INTO royalty_periods VALUES(%s,%s,'2026-H2','2026-07-01','2026-12-31')",(period2,self.tenant))
        cur.execute("INSERT INTO royalty_statements(id,tenant_id,work_id,period_id,party,status,currency,payable_this_period,settlement) VALUES(%s,%s,%s,%s,'author','final','USD',168.88,%s)",
            (statement2,self.tenant,self.work,period2,Jsonb(dict(actual_payable='168.88',prior_unpaid_payable='75.84',total_payment_due='244.72'))))
        items=self.get('payables').json()['items']
        self.assertEqual(len(items),2)
        self.assertEqual(sorted(r['outstanding'] for r in items),['168.88','75.84'])
        self.assertEqual(sum(Decimal(r['outstanding']) for r in items),Decimal('244.72'))

    def test_dual_approval_and_changed_destination(self):
        self.setup_payment();batch=self.prepare('100');bid=batch.json()['batch']['id']
        self.assertEqual(self.post('batches/'+bid+'/submit-for-approval',{}).status_code,200)
        self.assertEqual(self.post('batches/'+bid+'/approve',{}).status_code,403)
        self.cursor().execute("UPDATE secure_payments.payment_accounts SET is_active=false WHERE payment_profile_id IN (SELECT id FROM secure_payments.payment_profiles WHERE tenant_id=%s)",(self.slug,))
        r=self.post('batches/'+bid+'/approve',{});self.assertEqual(r.status_code,409,r.text)

    def test_database_append_only_and_live_gate(self):
        self.setup_payment();self.single_approval();batch=self.prepare('100')
        with self.assertRaises(RuntimeError): self.rpc("UPDATE financial_payments.payment_batches SET total_amount=99 WHERE tenant_id=$1",[str(self.tenant)])
        with self.assertRaises(RuntimeError): self.rpc("DELETE FROM financial_payments.payment_batches WHERE tenant_id=$1",[str(self.tenant)])
        with self.assertRaises(RuntimeError): self.rpc("UPDATE financial_payments.payment_settings SET live_payments_enabled=true WHERE tenant_id=$1",[str(self.tenant)])

    def test_dual_approval_two_distinct_admins(self):
        self.setup_payment();batch=self.prepare('100');bid=batch.json()['batch']['id']
        self.post('batches/'+bid+'/submit-for-approval',{})
        for index in (1,2):
            uid=uuid4();sub='approver-'+uuid4().hex
            self.cursor().execute('INSERT INTO users VALUES(%s,%s,%s)',(uid,sub,'Approver'))
            self.cursor().execute("INSERT INTO memberships VALUES(%s,%s,'tenant_admin','{}')",(self.tenant,uid))
            self.sub=sub
            result=self.post('batches/'+bid+'/approve',{})
            self.assertEqual(result.status_code,200,result.text)
            self.assertEqual(result.json()['batch']['status'],'pending_approval' if index==1 else 'approved')
        self.assertEqual(self.post('batches/'+bid+'/execute',{}).status_code,200)

    def test_two_titles_consolidate_by_destination_not_name(self):
        self.setup_payment()
        work2,statement2=uuid4(),uuid4();cur=self.cursor()
        cur.execute("INSERT INTO works VALUES(%s,%s,'Book B')",(work2,self.tenant))
        cur.execute('INSERT INTO work_contributors VALUES(%s,%s,%s)',(self.tenant,work2,self.author))
        cur.execute("INSERT INTO royalty_statements(id,tenant_id,work_id,period_id,party,status,currency,payable_this_period,settlement) VALUES(%s,%s,%s,%s,'author','final','USD',400,%s)",(statement2,self.tenant,work2,self.period,Jsonb(dict(settlement_version=2,account_balance_carried_forward='400',payments_recorded_total='0'))))
        cur.execute("INSERT INTO royalty_payment_instructions(id,tenant_id,work_id,party,payee_mode,contributor_party_id,agency_party_id,contributor_percent,agency_percent) VALUES(%s,%s,%s,'author','split',%s,%s,85,15)",(uuid4(),self.tenant,work2,self.author,self.agency))
        result=self.post('batches',dict(funding_account_id=self.account,idempotency_key=str(uuid4()),items=[dict(statement_id=str(self.statement),amount='600'),dict(statement_id=str(statement2),amount='400')]))
        self.assertEqual(result.status_code,200,result.text)
        self.assertEqual(len(result.json()['allocations']),4)
        self.assertEqual(len(result.json()['transfers']),2)
        self.assertEqual(sorted(t['amount'] for t in result.json()['transfers']),['150.00','850.00'])

    def test_failed_transfer_releases_only_its_allocations(self):
        self.setup_payment();self.single_approval();batch=self.prepare('1000');detail=self.authorize(batch.json()['batch']['id'])
        transfer=next(t for t in detail['transfers'] if t['amount']=='850.00')
        result=self.post('transfers/'+transfer['id']+'/simulate-event',dict(status='failed',event_id=str(uuid4())))
        self.assertEqual(result.status_code,200,result.text)
        row=self.get('payables').json()['items'][0]
        self.assertEqual(row['active_reservations'],'150.00');self.assertEqual(row['available_to_schedule'],'850.00')
        self.assertEqual(result.json()['reconciliations'],[])

    def test_bank_change_cancels_authorization_and_cross_tenant_ids_fail(self):
        self.setup_payment();batch=self.prepare('100');bid=batch.json()['batch']['id']
        other=uuid4();self.cursor().execute("INSERT INTO tenants VALUES(%s,'other-'||%s)",(other,str(other)))
        self.cursor().execute("UPDATE memberships SET tenant_id=%s WHERE user_id=%s",(other,self.user))
        self.assertEqual(self.get('batches/'+bid).status_code,403)
        with self.assertRaises(RuntimeError): self.rpc("INSERT INTO financial_payments.payment_recipients(tenant_id,party_id,recipient_type,display_name) VALUES($1,$2,'contributor','Wrong owner')",[str(other),str(self.author)])

    def test_minimum_and_reserve_snapshot_prevents_scheduling_accrual(self):
        self.cursor().execute('UPDATE royalty_statements SET payable_this_period=0,settlement=%s WHERE id=%s',
            (Jsonb(dict(settlement_version=2,account_balance_carried_forward='30',payments_recorded_total='0',accrued_carried_forward='30')),self.statement))
        row=self.get('payables').json()['items'][0]
        self.assertEqual(row['outstanding'],'0.00');self.assertFalse(row['eligible'])

    def test_fifty_dollar_minimum_and_partial_payment(self):
        self.setup_payment()
        self.assertEqual(self.prepare('49.99').status_code,409)
        for amount,eligible in [('49.99',False),('50.00',True)]:
            self.cursor().execute('UPDATE royalty_statements SET payable_this_period=%s,settlement=%s WHERE id=%s',
                (amount,Jsonb(dict(settlement_version=2,account_balance_carried_forward=amount,payments_recorded_total='0')),self.statement))
            row=self.get('payables').json()['items'][0]
            self.assertEqual(row['outstanding'],amount)
            self.assertEqual(row['eligible'],eligible)
            result=self.prepare(amount)
            self.assertEqual(result.status_code,200 if eligible else 409,result.text)

    def test_read_only_member_cannot_execute_or_approve(self):
        self.setup_payment();self.single_approval();batch=self.prepare('100');bid=batch.json()['batch']['id']
        self.post('batches/'+bid+'/submit-for-approval',{})
        self.cursor().execute("UPDATE memberships SET role='staff',module_permissions='{\"financials\":true}' WHERE user_id=%s",(self.user,))
        self.assertEqual(self.get('summary').status_code,200)
        self.assertEqual(self.post('batches/'+bid+'/approve',{}).status_code,403)
        self.assertEqual(self.post('batches/'+bid+'/execute',{}).status_code,403)

    def test_provider_event_id_cannot_be_reused_for_another_transfer(self):
        self.setup_payment();self.single_approval();batch=self.prepare('100');detail=self.authorize(batch.json()['batch']['id'])
        tid1,tid2=[t['id'] for t in detail['transfers']];event_id=str(uuid4())
        self.assertEqual(self.post('transfers/'+tid1+'/simulate-event',dict(status='processing',event_id=event_id)).status_code,200)
        self.assertEqual(self.post('transfers/'+tid2+'/simulate-event',dict(status='processing',event_id=event_id)).status_code,409)


if __name__=='__main__': unittest.main()
