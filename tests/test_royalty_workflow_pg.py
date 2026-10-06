"""Royalty workflow against disposable PostgreSQL (PGlite), never the configured database."""
import contextlib
import json
import subprocess
import unittest
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch
from uuid import uuid4
from fastapi import FastAPI
from fastapi.testclient import TestClient
from routers import royalty, royalty_engine
from services import royalty_statement_engine as engine

ROOT = Path(__file__).resolve().parent


class RoyaltyPostgresTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.proc = subprocess.Popen(['node', '--preserve-symlinks', '--preserve-symlinks-main', str(ROOT/'banking_pg.cjs')],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, encoding='utf-8')
        cls.addClassCleanup(cls.shutdown)
        ready = cls.proc.stdout.readline()
        if not ready or not json.loads(ready).get('ready'):
            cls.proc.terminate()
            raise RuntimeError('Disposable PostgreSQL failed to start.')
        cls.rpc('''
            CREATE TYPE roy_party AS ENUM ('author', 'illustrator');
            CREATE TYPE roy_rights_type AS ENUM ('first_rights', 'subrights');
            CREATE TABLE tenants(id uuid PRIMARY KEY, slug text);
            CREATE TABLE works(id uuid PRIMARY KEY, tenant_id uuid, title text, subtitle text);
            CREATE TABLE royalty_sets(id uuid PRIMARY KEY, tenant_id uuid, work_id uuid, is_active boolean DEFAULT true, version integer DEFAULT 1, created_at timestamptz DEFAULT now());
            CREATE TABLE royalty_periods(id uuid PRIMARY KEY, tenant_id uuid, period_code text, period_start date, period_end date, is_closed boolean DEFAULT false);
            CREATE TABLE subrights_types(id uuid PRIMARY KEY, name text);
            CREATE TABLE royalty_rules(id uuid PRIMARY KEY, tenant_id uuid, royalty_set_id uuid, party roy_party, rights_type roy_rights_type,
                format_label text, subrights_type_id uuid, base text, mode text, escalating boolean DEFAULT false, percent numeric, flat_rate_percent numeric);
            CREATE TABLE royalty_tiers(id uuid PRIMARY KEY, tenant_id uuid, rule_id uuid, tier_order integer, rate_percent numeric, base text, note text);
            CREATE TABLE subrights_income_lines(id uuid PRIMARY KEY DEFAULT gen_random_uuid(), tenant_id uuid, work_id uuid, period_id uuid, royalty_set_id uuid,
                subrights_type_id uuid, income_date date, publisher_receipts numeric, gross_amount numeric, created_at timestamptz DEFAULT now(), updated_at timestamptz DEFAULT now());
            CREATE TABLE editions(id uuid PRIMARY KEY, tenant_id uuid, work_id uuid, product_form text, product_form_detail text);
            CREATE TABLE royalty_sales_lines(id uuid PRIMARY KEY, tenant_id uuid, edition_id uuid, period_id uuid, units_sold numeric, units_returned numeric,
                discount_percent numeric, publisher_receipts numeric, gross_sales numeric, royalty_stream text, transaction_date date, created_at timestamptz DEFAULT now());
            CREATE TABLE advances(id uuid PRIMARY KEY, tenant_id uuid, royalty_set_id uuid, party roy_party, amount numeric, recoupable boolean);
            CREATE TABLE parties(id uuid PRIMARY KEY, display_name text, email text);
            CREATE TABLE work_contributors(work_id uuid, party_id uuid, contributor_role text, sequence_number integer);
            CREATE TABLE party_representations(id uuid PRIMARY KEY, represented_party_id uuid, agent_party_id uuid, work_id uuid,
                is_primary boolean, role_label text, created_at timestamptz);
        ''', execute=True)
        cls.rpc((ROOT.parent/'migrations/003_royalty_statements.sql').read_text(encoding='utf-8-sig'),execute=True)
        cls.rpc((ROOT.parent/'migrations/004_royalty_statement_engine.sql').read_text(encoding='utf-8-sig'),execute=True)
        cls.rpc((ROOT.parent/'migrations/019_royalty_account_tracking.sql').read_text(encoding='utf-8-sig'),execute=True)
        cls.rpc((ROOT.parent/'migrations/023_royalty_settlement.sql').read_text(encoding='utf-8-sig'),execute=True)
        cls.rpc((ROOT.parent/'migrations/024_royalty_payment_tracking.sql').read_text(encoding='utf-8-sig'),execute=True)
        cls.rpc('ALTER TABLE royalty_statements DROP CONSTRAINT royalty_statements_party_check; ALTER TABLE royalty_statements ALTER COLUMN party TYPE roy_party USING party::roy_party',execute=True)
        cls.rpc('ALTER TABLE royalty_statements ALTER COLUMN period_start DROP NOT NULL, ALTER COLUMN period_end DROP NOT NULL',execute=True)
        cls.rpc('''ALTER TABLE royalty_statements ADD COLUMN sent_at timestamptz, ADD COLUMN pdf_saved_at timestamptz,
            ADD COLUMN pdf_s3_key text, ADD COLUMN sent_to_contributor_email text, ADD COLUMN sent_to_agent_email text;''',execute=True)

    @classmethod
    def shutdown(cls):
        cls.proc.stdin.close()
        cls.proc.wait(timeout=15)
        cls.proc.stdout.close()
        cls.proc.stderr.close()

    @classmethod
    def rpc(cls, sql, params=(), execute=False):
        params=[getattr(value,'obj',value) for value in params]
        cls.proc.stdin.write(json.dumps({'sql':sql,'params':params,'exec':execute},default=str)+'\n')
        cls.proc.stdin.flush()
        result=json.loads(cls.proc.stdout.readline())
        if 'error' in result:
            raise AssertionError(f"{result['error']}\nSQL: {sql}")
        return result['result']

    def setUp(self):
        self.rpc('BEGIN',execute=True)
        self.addCleanup(lambda:self.rpc('ROLLBACK',execute=True))
        self.tenant, self.other, self.work, self.work2, self.period, self.set_id, self.set2, self.stype = [str(uuid4()) for _ in range(8)]
        cur=self.cursor()
        cur.execute('INSERT INTO tenants VALUES (%s::uuid, %s), (%s::uuid, %s)', (self.tenant,'marble-press',self.other,'other'))
        cur.execute('INSERT INTO works VALUES (%s::uuid,%s::uuid,%s,NULL),(%s::uuid,%s::uuid,%s,NULL)',
            (self.work,self.tenant,'Hotel Melikov',self.work2,self.tenant,'Snowbird'))
        cur.execute('INSERT INTO royalty_sets(id,tenant_id,work_id) VALUES (%s::uuid,%s::uuid,%s::uuid),(%s::uuid,%s::uuid,%s::uuid)',
            (self.set_id,self.tenant,self.work,self.set2,self.tenant,self.work2))
        cur.execute("INSERT INTO royalty_periods(id,tenant_id,period_code,period_start,period_end) VALUES (%s::uuid,%s::uuid,'2026-H1','2026-01-01','2026-06-30')",(self.period,self.tenant))
        cur.execute("INSERT INTO subrights_types VALUES (%s::uuid,'Digital audiobook rights')",(self.stype,))
        for work_set in (self.set_id,self.set2):
            cur.execute("""INSERT INTO royalty_rules(id,tenant_id,royalty_set_id,party,rights_type,subrights_type_id,base,mode,percent)
                VALUES (%s::uuid,%s::uuid,%s::uuid,'author','subrights',%s::uuid,'net_receipts','flat',50)""",
                (str(uuid4()),self.tenant,work_set,self.stype))
        app=FastAPI()
        @app.middleware('http')
        async def signed_in(request, call_next):
            request.state.user_claims={'sub':'test-accountant'}
            return await call_next(request)
        app.include_router(royalty.router,prefix='/api')
        app.include_router(royalty_engine.router,prefix='/api')
        self.client=TestClient(app)
        self.addCleanup(self.client.close)
        for target in ('routers.royalty.db_conn','routers.royalty_engine.db_conn','app.core.db.db_conn'):
            patcher=patch(target,self.connection);patcher.start();self.addCleanup(patcher.stop)

    def cursor(self):
        owner=self
        class Cursor:
            def execute(self,sql,params=()):
                for index in range(sql.count('%s')):sql=sql.replace('%s',f'${index+1}',1)
                self.rows=owner.rpc(sql,params)['rows']
                for row in self.rows:
                    for key,value in list(row.items()):
                        if value and key in ('period_start','period_end','income_date'):
                            row[key]=date.fromisoformat(str(value)[:10])
                        elif value and key == 'updated_at':
                            row[key]=datetime.fromisoformat(str(value).replace('Z','+00:00'))
            def fetchone(self):return self.rows[0] if self.rows else None
            def fetchall(self):return self.rows
            def __enter__(self):return self
            def __exit__(self,*args):pass
        return Cursor()

    @contextlib.contextmanager
    def connection(self):
        owner=self
        checkpoint='request_'+uuid4().hex
        owner.rpc('SAVEPOINT '+checkpoint,execute=True)
        class Connection:
            autocommit=False
            active=True
            def cursor(self,**kwargs):return owner.cursor()
            def commit(self):
                if self.active:
                    owner.rpc('RELEASE SAVEPOINT '+checkpoint,execute=True)
                    self.active=False
            def rollback(self):
                if self.active:
                    owner.rpc('ROLLBACK TO SAVEPOINT '+checkpoint,execute=True)
                    self.commit()
            @contextlib.contextmanager
            def transaction(self):
                owner.rpc('SAVEPOINT approve',execute=True)
                try:
                    yield
                    owner.rpc('RELEASE SAVEPOINT approve',execute=True)
                except BaseException:
                    owner.rpc('ROLLBACK TO SAVEPOINT approve',execute=True)
                    raise
        connection=Connection()
        try:yield connection
        finally:connection.commit()

    def post_income(self,**changes):
        item=dict(period_id=self.period,work_id=self.work,royalty_set_id=self.set_id,subrights_type_id=self.stype,
            income_date='2026-06-30',publisher_receipts='143.74',client_row_id=str(uuid4()))
        item.update(changes)
        return self.client.post('/api/royalty/subrights/income',headers={'X-Tenant':'marble-press'},json={'items':[item]})

    def generate(self):
        return engine.generate_statement(self.cursor(),tenant_id=self.tenant,work_id=self.work,
            royalty_set_id=self.set_id,party='author',period_id=self.period,rebuild=True)

    def queue(self):return royalty_engine._fetch_statement_queue(self.cursor(),self.tenant,self.period)

    def test_income_save_read_reopen_update_repeat_and_tenant_isolation(self):
        row_id=str(uuid4())
        saved=self.post_income(client_row_id=row_id)
        self.assertEqual(saved.status_code,200,saved.text)
        self.assertEqual(saved.json()['ids'],[row_id])
        url=f'/api/royalty/subrights/income?period_id={self.period}&work_id={self.work}'
        for _ in range(2):
            data=self.client.get(url,headers={'X-Tenant':'marble-press'})
            self.assertEqual(data.status_code,200,data.text)
            self.assertEqual(len(data.json()),1)
            self.assertEqual(Decimal(str(data.json()[0]['publisher_receipts'])),Decimal('143.74'))
        self.assertEqual(self.post_income(client_row_id=row_id).status_code,200)
        for _ in range(2):self.assertEqual(self.post_income(id=row_id,publisher_receipts='200').status_code,200)
        rows=self.client.get(url,headers={'X-Tenant':'marble-press'}).json()
        self.assertEqual(len(rows),1)
        self.assertEqual(Decimal(str(rows[0]['publisher_receipts'])),Decimal('200'))
        self.assertEqual(self.client.get(url,headers={'X-Tenant':'other'}).status_code,404)
        self.assertEqual(self.post_income(royalty_set_id=self.set2).status_code,400)
        self.assertEqual(self.client.get(url).status_code,403)
        periods=self.client.get('/api/royalty/periods',headers={'X-Tenant':'other'})
        self.assertEqual(periods.status_code,200,periods.text)
        self.assertEqual(periods.json(),[])
        options=self.client.get(f'/api/royalty/subrights/options?work_id={self.work}',headers={'X-Tenant':'other'})
        self.assertEqual(options.status_code,404,options.text)
        options=self.client.get(f'/api/royalty/subrights/options?work_id={self.work}',headers={'X-Tenant':'marble-press'})
        self.assertEqual(options.status_code,200,options.text)
        self.assertEqual(options.json()[0]['name'],'Digital audiobook rights')

    def test_draft_rebuild_approval_frozen_lines_queue_and_sent_completion(self):
        rows=self.queue()
        self.assertEqual(len(rows),2)
        self.assertTrue(all(row['workflow_status']=='Not generated' for row in rows))
        draft=self.generate()
        statement=draft['statement_id']
        self.assertEqual(draft['header']['earned_this_period'],'0.00')
        cur=self.cursor()
        cur.execute("INSERT INTO royalty_statement_lines(tenant_id,statement_id,category_label) VALUES (%s::uuid,%s::uuid,'old frozen line')",
            (self.tenant,statement))
        self.assertEqual(self.post_income().status_code,200)
        updated=self.generate()
        self.assertEqual(updated['statement_id'],statement)
        self.assertEqual(updated['header']['earned_this_period'],'71.87')
        cur.execute('SELECT * FROM royalty_statement_lines WHERE statement_id=%s::uuid',(statement,))
        lines=cur.fetchall()
        self.assertEqual(len(lines),1)
        self.assertEqual(lines[0]['line_type'],'subrights')
        self.assertEqual(Decimal(str(lines[0]['royalty_amount'])),Decimal('71.87'))
        self.assertEqual(next(row for row in self.queue() if row['work_id']==self.work)['workflow_status'],'Draft')
        cur.execute('SELECT updated_at FROM royalty_statements WHERE id=%s::uuid',(statement,))
        version=cur.fetchone()['updated_at'].isoformat()
        approval=f'/api/royalty/statements-engine/{statement}/approve'
        stale=self.client.post(approval,headers={'X-Tenant':'marble-press'},json={'expected_updated_at':'2020-01-01T00:00:00Z'})
        self.assertEqual(stale.status_code,409,stale.text)
        self.assertEqual(self.client.post(approval,headers={'X-Tenant':'other'}).status_code,404)
        for _ in range(2):
            approved=self.client.post(approval,headers={'X-Tenant':'marble-press'},json={'expected_updated_at':version})
            self.assertEqual(approved.status_code,200,approved.text)
        cur.execute('SELECT * FROM royalty_statement_lines WHERE statement_id=%s::uuid',(statement,))
        self.assertEqual(cur.fetchall(),lines)
        cur.execute('SELECT status,earned_this_period FROM royalty_statements WHERE id=%s::uuid',(statement,))
        head=cur.fetchone()
        self.assertEqual(head['status'],'final')
        self.assertEqual(Decimal(str(head['earned_this_period'])),Decimal('71.87'))
        with self.assertRaises(engine.StatementValidationError):self.generate()
        self.assertTrue(next(row for row in self.queue() if row['work_id']==self.work)['awaiting_distribution'])
        distribution=royalty_engine._fetch_distribution_items(self.cursor(),self.tenant,self.period)
        self.assertEqual([row['statement_id'] for row in distribution],[statement])
        # Model the existing successful send transition without delivering email.
        cur.execute('UPDATE royalty_statements SET sent_at=now() WHERE id=%s::uuid',(statement,))
        self.assertEqual(royalty_engine._fetch_distribution_items(self.cursor(),self.tenant,self.period),[])
        result=self.client.get(f'/api/royalty/statements-engine/queue?period_id={self.period}',headers={'X-Tenant':'marble-press'})
        self.assertEqual(result.status_code,200,result.text)
        self.assertEqual(result.json()['complete_count'],1)
        self.assertEqual([row['work_id'] for row in result.json()['items']],[self.work2])

    def test_binding_detail_uses_recorded_parent_rule_without_changing_rate(self):
        cur=self.cursor()
        edition=str(uuid4())
        rule=str(uuid4())
        cur.execute("""INSERT INTO royalty_rules(id,tenant_id,royalty_set_id,party,rights_type,format_label,base,mode,percent)
            VALUES (%s::uuid,%s::uuid,%s::uuid,'author','first_rights','Hardcover','net_receipts','fixed',10)""",
            (rule,self.tenant,self.set_id))
        cur.execute("INSERT INTO editions VALUES (%s::uuid,%s::uuid,%s::uuid,'Hardcover','Paper over boards')",(edition,self.tenant,self.work))
        cur.execute("""INSERT INTO royalty_sales_lines(id,tenant_id,edition_id,period_id,units_sold,publisher_receipts,transaction_date)
            VALUES (%s::uuid,%s::uuid,%s::uuid,%s::uuid,1,100,'2026-06-30')""",(str(uuid4()),self.tenant,edition,self.period))
        draft=self.generate()
        self.assertEqual(draft['header']['earned_this_period'],'10.00')
        cur.execute('SELECT * FROM royalty_statement_lines WHERE statement_id=%s::uuid',(draft['statement_id'],))
        line=cur.fetchone()
        self.assertEqual(line['category_label'],'Paper over boards')
        self.assertEqual(line['applied_rule_id'],rule)
        self.assertEqual(Decimal(str(line['royalty_rate'])),Decimal('10'))

    def test_income_batch_rolls_back_on_invalid_work_set(self):
        good=dict(period_id=self.period,work_id=self.work,royalty_set_id=self.set_id,
            subrights_type_id=self.stype,income_date='2026-06-30',publisher_receipts='143.74')
        bad={**good,'royalty_set_id':self.set2}
        result=self.client.post('/api/royalty/subrights/income',headers={'X-Tenant':'marble-press'},json={'items':[good,bad]})
        self.assertEqual(result.status_code,400,result.text)
        cur=self.cursor()
        cur.execute('SELECT count(*) AS n FROM subrights_income_lines WHERE tenant_id=%s::uuid',(self.tenant,))
        self.assertEqual(cur.fetchone()['n'],0)

    def test_illustrator_without_positive_rules_is_not_queued(self):
        cur=self.cursor()
        cur.execute("""INSERT INTO royalty_rules(id,tenant_id,royalty_set_id,party,rights_type,base,mode,percent)
            VALUES (%s::uuid,%s::uuid,%s::uuid,'illustrator','first_rights','net_receipts','flat',0)""",
            (str(uuid4()),self.tenant,self.set_id))
        self.assertTrue(all(row['party']=='author' for row in self.queue()))
        cur.execute("UPDATE royalty_rules SET percent=10 WHERE party='illustrator'")
        self.assertEqual(sum(row['party']=='illustrator' for row in self.queue()),1)

    def switch_active_set(self):
        cur = self.cursor()
        active = str(uuid4())
        cur.execute('UPDATE royalty_sets SET is_active=false WHERE id=%s::uuid', (self.set_id,))
        cur.execute("INSERT INTO royalty_sets(id,tenant_id,work_id,version) VALUES (%s::uuid,%s::uuid,%s::uuid,2)",
            (active,self.tenant,self.work))
        cur.execute("""INSERT INTO royalty_rules(id,tenant_id,royalty_set_id,party,rights_type,subrights_type_id,base,mode,percent)
            VALUES (%s::uuid,%s::uuid,%s::uuid,'author','subrights',%s::uuid,'net_receipts','fixed',37)""",
            (str(uuid4()),self.tenant,active,self.stype))
        return active

    def test_no_statement_queue_uses_only_current_active_set(self):
        active = self.switch_active_set()
        row = next(row for row in self.queue() if row['work_id']==self.work)
        self.assertEqual(row['royalty_set_id'], active)
        self.assertIsNone(row['statement_id'])
        self.assertFalse(row['statement_exists'])

    def test_stale_draft_queue_and_api_rebuild_preserve_identity_and_active_income(self):
        cur = self.cursor()
        cur.execute('DELETE FROM royalty_rules WHERE royalty_set_id=%s::uuid', (self.set_id,))
        for label, receipts, percent in [('E-Book','168.44',25),('Hardcover','392.67',10)]:
            edition = str(uuid4())
            cur.execute("""INSERT INTO royalty_rules(id,tenant_id,royalty_set_id,party,rights_type,format_label,base,mode,percent)
                VALUES (%s::uuid,%s::uuid,%s::uuid,'author','first_rights',%s,'net_receipts','fixed',%s)""",
                (str(uuid4()),self.tenant,self.set_id,label,percent))
            cur.execute('INSERT INTO editions VALUES (%s::uuid,%s::uuid,%s::uuid,%s,NULL)',(edition,self.tenant,self.work,label))
            cur.execute("""INSERT INTO royalty_sales_lines(id,tenant_id,edition_id,period_id,units_sold,publisher_receipts,transaction_date)
                VALUES (%s::uuid,%s::uuid,%s::uuid,%s::uuid,1,%s,'2026-06-30')""",
                (str(uuid4()),self.tenant,edition,self.period,receipts))
        original = self.generate()
        statement = original['statement_id']
        self.assertEqual(original['header']['earned_this_period'],'81.38')
        cur.execute('SELECT id::text FROM royalty_statement_lines WHERE statement_id=%s::uuid',(statement,))
        old_lines = {row['id'] for row in cur.fetchall()}
        active = self.switch_active_set()
        cur.execute("""INSERT INTO royalty_rules(id,tenant_id,royalty_set_id,party,rights_type,format_label,base,mode,percent)
            SELECT gen_random_uuid(),tenant_id,%s::uuid,party,rights_type,format_label,base,mode,percent
            FROM royalty_rules WHERE royalty_set_id=%s::uuid""",(active,self.set_id))
        self.assertEqual(self.post_income(royalty_set_id=active).status_code,200)
        cur.execute('SELECT * FROM subrights_income_lines WHERE work_id=%s::uuid',(self.work,))
        income_before = cur.fetchall()
        queue = [row for row in self.queue() if row['work_id']==self.work]
        self.assertEqual(len(queue),1)
        self.assertEqual((queue[0]['statement_id'],queue[0]['royalty_set_id'],queue[0]['status']), (statement,active,'draft'))
        result = self.client.post('/api/royalty/statements-engine/generate',headers={'X-Tenant':'marble-press'},
            json={'work_id':self.work,'royalty_set_id':self.set_id,'party':'author','period_id':self.period,'rebuild':True})
        self.assertEqual(result.status_code,200,result.text)
        self.assertEqual(result.json()['statement_id'],statement)
        cur.execute('SELECT royalty_set_id::text,status,earned_this_period FROM royalty_statements WHERE id=%s::uuid',(statement,))
        head=cur.fetchone()
        self.assertEqual((head['royalty_set_id'],head['status']),(active,'draft'))
        self.assertEqual(Decimal(str(head['earned_this_period'])),Decimal('134.56'))
        cur.execute('SELECT * FROM royalty_statement_lines WHERE statement_id=%s::uuid',(statement,))
        lines=cur.fetchall()
        self.assertEqual({row['category_label'] for row in lines},{'E-book','Hardcover','Digital audiobook rights'})
        self.assertFalse(old_lines.intersection(str(row['id']) for row in lines))
        audio=next(row for row in lines if row['line_type']=='subrights')
        self.assertEqual(Decimal(str(audio['basis_amount'])),Decimal('143.74'))
        self.assertEqual(Decimal(str(audio['royalty_rate'])),Decimal('37'))
        self.assertEqual(Decimal(str(audio['royalty_amount'])),Decimal('53.18'))
        cur.execute('SELECT * FROM subrights_income_lines WHERE work_id=%s::uuid',(self.work,))
        self.assertEqual(cur.fetchall(),income_before)

    def test_new_draft_stale_client_set_resolves_active_and_income_filter_stays_strict(self):
        self.assertEqual(self.post_income(publisher_receipts='999').status_code,200)
        active = self.switch_active_set()
        self.assertEqual(self.post_income(royalty_set_id=active).status_code,200)
        draft=self.generate()
        cur=self.cursor()
        cur.execute('SELECT royalty_set_id::text,earned_this_period FROM royalty_statements WHERE id=%s::uuid',(draft['statement_id'],))
        row=cur.fetchone()
        self.assertEqual(row['royalty_set_id'],active)
        self.assertEqual(Decimal(str(row['earned_this_period'])),Decimal('53.18'))

    def test_historical_final_never_migrates_and_sent_final_is_complete(self):
        self.post_income()
        statement=self.generate()['statement_id']
        cur=self.cursor()
        cur.execute("UPDATE royalty_statements SET status='final' WHERE id=%s::uuid",(statement,))
        cur.execute('SELECT * FROM royalty_statement_lines WHERE statement_id=%s::uuid',(statement,))
        frozen=cur.fetchall()
        active=self.switch_active_set()
        row=next(row for row in self.queue() if row['work_id']==self.work)
        self.assertEqual((row['statement_id'],row['royalty_set_id'],row['status']),(statement,self.set_id,'final'))
        for submitted in (self.set_id,active):
            result=self.client.post('/api/royalty/statements-engine/generate',headers={'X-Tenant':'marble-press'},
                json={'work_id':self.work,'royalty_set_id':submitted,'party':'author','period_id':self.period,'rebuild':True})
            self.assertEqual(result.status_code,400,result.text)
            self.assertIn('Final statement already exists',result.text)
        cur.execute('SELECT royalty_set_id::text,status FROM royalty_statements WHERE id=%s::uuid',(statement,))
        self.assertEqual(cur.fetchone(),{'royalty_set_id':self.set_id,'status':'final'})
        cur.execute('SELECT * FROM royalty_statement_lines WHERE statement_id=%s::uuid',(statement,))
        self.assertEqual(cur.fetchall(),frozen)
        cur.execute('UPDATE royalty_statements SET sent_at=now() WHERE id=%s::uuid',(statement,))
        result=self.client.get(f'/api/royalty/statements-engine/queue?period_id={self.period}',headers={'X-Tenant':'marble-press'})
        self.assertEqual(result.json()['complete_count'],1)
        self.assertTrue(all(row['work_id']!=self.work for row in result.json()['items']))

    def test_no_active_set_fails_without_modifying_existing_draft(self):
        statement=self.generate()['statement_id']
        cur=self.cursor()
        cur.execute('SELECT * FROM royalty_statements WHERE id=%s::uuid',(statement,))
        before=cur.fetchone()
        cur.execute('UPDATE royalty_sets SET is_active=false WHERE id=%s::uuid',(self.set_id,))
        with self.assertRaisesRegex(engine.StatementValidationError,'No active royalty set'):
            self.generate()
        cur.execute('SELECT * FROM royalty_statements WHERE id=%s::uuid',(statement,))
        self.assertEqual(cur.fetchone(),before)
        with self.assertRaises(royalty.HTTPException) as error:
            royalty._resolve_active_royalty_set_id(cur,self.tenant,self.work)
        self.assertIn('No active royalty set',error.exception.detail)
        result=self.client.post('/api/royalty/statements-engine/generate',headers={'X-Tenant':'marble-press'},
            json={'work_id':self.work2,'royalty_set_id':self.set2,'party':'author','period_id':self.period})
        self.assertEqual(result.status_code,200,result.text)
        cur.execute('UPDATE royalty_sets SET is_active=false WHERE id=%s::uuid',(self.set2,))
        cur.execute('DELETE FROM royalty_statements WHERE work_id=%s::uuid',(self.work2,))
        result=self.client.post('/api/royalty/statements-engine/generate',headers={'X-Tenant':'marble-press'},
            json={'work_id':self.work2,'royalty_set_id':self.set2,'party':'author','period_id':self.period})
        self.assertEqual(result.status_code,400,result.text)
        self.assertIn('No active royalty set',result.text)

    def test_subrights_basis_migration_repairs_only_bases_and_protects_future_writes(self):
        self.rpc("CREATE TYPE roy_base AS ENUM ('list_price','net_receipts'); ALTER TABLE royalty_rules ALTER COLUMN base TYPE roy_base USING base::roy_base; ALTER TABLE royalty_tiers ALTER COLUMN base TYPE roy_base USING base::roy_base;",execute=True)
        self.assertEqual(self.post_income().status_code,200)
        statement=self.generate()['statement_id']
        cur=self.cursor()
        cur.execute("UPDATE royalty_statements SET status='final' WHERE id=%s::uuid",(statement,))
        cur.execute('SELECT * FROM royalty_statements WHERE id=%s::uuid',(statement,))
        header_before=cur.fetchone()
        cur.execute('SELECT * FROM royalty_statement_lines WHERE statement_id=%s::uuid',(statement,))
        frozen_before=cur.fetchall()
        cur.execute('SELECT * FROM subrights_income_lines ORDER BY id')
        income_before=cur.fetchall()
        first_rule=str(uuid4())
        cur.execute("""INSERT INTO royalty_rules(id,tenant_id,royalty_set_id,party,rights_type,format_label,base,mode,percent)
            VALUES (%s::uuid,%s::uuid,%s::uuid,'author','first_rights','Hardcover','list_price','fixed',10)""",
            (first_rule,self.tenant,self.set_id))
        cur.execute("UPDATE royalty_rules SET base='list_price' WHERE rights_type='subrights'")
        cur.execute("""INSERT INTO royalty_tiers(id,tenant_id,rule_id,tier_order,rate_percent,base)
            SELECT gen_random_uuid(),tenant_id,id,1,43,'list_price' FROM royalty_rules""")
        cur.execute('SELECT id::text,percent,flat_rate_percent FROM royalty_rules ORDER BY id')
        rates_before=cur.fetchall()
        cur.execute('SELECT id::text,rate_percent FROM royalty_tiers ORDER BY id')
        tiers_before=cur.fetchall()
        migration=(ROOT.parent/'migrations/022_subrights_net_receipts.sql').read_text(encoding='utf-8')
        # Keep the migration inside the disposable test transaction for rollback isolation.
        migration=migration.replace('BEGIN;','',1).replace('COMMIT;','',1)
        for _ in range(2): self.rpc(migration,execute=True)
        cur.execute("SELECT base FROM royalty_rules WHERE rights_type='subrights'")
        self.assertTrue(all(row['base']=='net_receipts' for row in cur.fetchall()))
        cur.execute("SELECT t.base FROM royalty_tiers t JOIN royalty_rules r ON r.id=t.rule_id WHERE r.rights_type='subrights'")
        self.assertTrue(all(row['base']=='net_receipts' for row in cur.fetchall()))
        cur.execute('SELECT base FROM royalty_rules WHERE id=%s::uuid',(first_rule,))
        self.assertEqual(cur.fetchone()['base'],'list_price')
        cur.execute('SELECT base FROM royalty_tiers WHERE rule_id=%s::uuid',(first_rule,))
        self.assertEqual(cur.fetchone()['base'],'list_price')
        cur.execute('SELECT id::text,percent,flat_rate_percent FROM royalty_rules ORDER BY id')
        self.assertEqual(cur.fetchall(),rates_before)
        cur.execute('SELECT id::text,rate_percent FROM royalty_tiers ORDER BY id')
        self.assertEqual(cur.fetchall(),tiers_before)
        cur.execute('SELECT * FROM subrights_income_lines ORDER BY id')
        self.assertEqual(cur.fetchall(),income_before)
        cur.execute('SELECT * FROM royalty_statements WHERE id=%s::uuid',(statement,))
        self.assertEqual(cur.fetchone(),header_before)
        cur.execute('SELECT * FROM royalty_statement_lines WHERE statement_id=%s::uuid',(statement,))
        self.assertEqual(cur.fetchall(),frozen_before)
        for bad_basis in ('list_price',None):
            self.rpc('SAVEPOINT invalid_basis',execute=True)
            with self.assertRaisesRegex(AssertionError,'royalty_rules_subrights_net_receipts'):
                cur.execute("UPDATE royalty_rules SET base=%s WHERE royalty_set_id=%s::uuid AND rights_type='subrights'",(bad_basis,self.set_id))
            self.rpc('ROLLBACK TO SAVEPOINT invalid_basis',execute=True)
            self.rpc('RELEASE SAVEPOINT invalid_basis',execute=True)

    def settlement_period(self, code, start, end, receipts):
        period=str(uuid4())
        cur=self.cursor()
        cur.execute('INSERT INTO royalty_periods(id,tenant_id,period_code,period_start,period_end) VALUES (%s::uuid,%s::uuid,%s,%s,%s)',
            (period,self.tenant,code,start,end))
        self.assertEqual(self.post_income(period_id=period,income_date=end,publisher_receipts=receipts).status_code,200)
        return period

    def settlement_draft(self, period):
        return engine.generate_statement(self.cursor(),tenant_id=self.tenant,work_id=self.work,royalty_set_id=self.set_id,
            party='author',period_id=period,rebuild=True)

    def approve(self, statement):
        result=self.client.post(f'/api/royalty/statements-engine/{statement}/approve',headers={'X-Tenant':'marble-press'})
        self.assertEqual(result.status_code,200,result.text)
        return result

    def test_threshold_accrual_flows_across_final_statements_and_stays_separate_from_unpaid(self):
        self.post_income(publisher_receipts='60')
        first=self.generate()
        snapshot=first['header']['settlement']
        self.assertEqual((first['header']['payable_this_period'],snapshot['accrued_carried_forward']),('0.00','30.00'))
        # Rebuilding does not mutate the saved reserve/account or double the accrual.
        self.assertEqual(self.generate()['header']['settlement'],snapshot)
        self.approve(first['statement_id'])
        second_period=self.settlement_period('2026-H2','2026-07-01','2026-12-31','40')
        second=self.settlement_draft(second_period)
        self.assertEqual((second['header']['payable_this_period'],second['header']['settlement']['accrued_brought_forward']),('50.00','30.00'))
        self.approve(second['statement_id'])
        third_period=self.settlement_period('2027-H1','2027-01-01','2027-06-30','20')
        third=self.settlement_draft(third_period)
        self.assertEqual((third['header']['payable_this_period'],third['header']['settlement']['accrued_brought_forward'],third['header']['settlement']['accrued_carried_forward']),('0.00','0.00','10.00'))
        # The unpaid $50 belongs to the second statement and is not payable a second time.
        cur=self.cursor()
        cur.execute('SELECT payable_this_period FROM royalty_statements WHERE id=%s::uuid',(second['statement_id'],))
        self.assertEqual(Decimal(str(cur.fetchone()['payable_this_period'])),Decimal('50'))

    def test_reserve_average_changes_automatically_and_approval_is_idempotent(self):
        cur=self.cursor()
        cur.execute("""INSERT INTO royalty_account_settings(tenant_id,work_id,party,minimum_payout,reserve_percent,reserve_held,version)
            VALUES (%s::uuid,%s::uuid,'author',50,10,0,1)""",(self.tenant,self.work))
        for i,earned in enumerate((100,200,300)):
            period=self.settlement_period(f'202{i}-H1',f'202{i}-01-01',f'202{i}-06-30',str(earned*2))
            self.approve(self.settlement_draft(period)['statement_id'])
        self.post_income(publisher_receipts='200')
        draft=self.generate()
        policy=draft['header']['settlement']
        self.assertEqual((policy['history_count'],policy['reserve_average'],policy['reserve_target'],policy['opening_reserve'],policy['actual_payable']),
            (3,'200.00','20.00','15.00','95.00'))
        cur.execute('SELECT reserve_held FROM royalty_account_settings WHERE work_id=%s::uuid',(self.work,))
        self.assertEqual(Decimal(str(cur.fetchone()['reserve_held'])),Decimal('15'))
        self.approve(draft['statement_id']); self.approve(draft['statement_id'])
        cur.execute('SELECT reserve_held,version FROM royalty_account_settings WHERE work_id=%s::uuid',(self.work,))
        row=cur.fetchone()
        self.assertEqual((Decimal(str(row['reserve_held'])),row['version']),(Decimal('20'),5))
        cur.execute("SELECT count(*) AS n FROM royalty_account_events WHERE work_id=%s::uuid AND reason='Automatic reserve settlement on statement approval'",(self.work,))
        self.assertEqual(cur.fetchone()['n'],4)
        next_period=self.settlement_period('2026-H2','2026-07-01','2026-12-31','0')
        next_draft=self.settlement_draft(next_period)
        next_policy=next_draft['header']['settlement']
        self.assertEqual((next_policy['reserve_average'],next_policy['reserve_held'],next_policy['reserve_change'],next_policy['accrued_carried_forward']),
            ('200.00','20.00','0.00','0.00'))
        self.approve(next_draft['statement_id'])
        release_period=self.settlement_period('2027-H1','2027-01-01','2027-06-30','0')
        released=self.settlement_draft(release_period)['header']['settlement']
        self.assertEqual((released['reserve_average'],released['reserve_target'],released['reserve_change'],released['accrued_carried_forward']),
            ('133.33','13.33','-6.67','6.67'))

    def test_settings_change_blocks_stale_settlement_approval(self):
        self.post_income()
        draft=self.generate()
        cur=self.cursor()
        cur.execute("INSERT INTO royalty_account_settings(tenant_id,work_id,party,minimum_payout,version) VALUES (%s::uuid,%s::uuid,'author',100,1)",(self.tenant,self.work))
        result=self.client.post(f"/api/royalty/statements-engine/{draft['statement_id']}/approve",headers={'X-Tenant':'marble-press'})
        self.assertEqual(result.status_code,409,result.text)
        rebuilt=self.generate()
        self.assertEqual(rebuilt['header']['payable_this_period'],'0.00')
        self.assertEqual(rebuilt['header']['settlement']['minimum_payout'],'100.00')
        self.approve(rebuilt['statement_id'])

    def test_settlement_schema_is_additive_and_finals_stay_frozen(self):
        self.post_income()
        statement=self.generate()['statement_id']
        self.approve(statement)
        cur=self.cursor()
        cur.execute('SELECT * FROM royalty_statements WHERE id=%s::uuid',(statement,))
        frozen=cur.fetchone()
        cur.execute("UPDATE royalty_account_settings SET minimum_payout=100,reserve_percent=20,reserve_held=20,version=version+1 WHERE work_id=%s::uuid",(self.work,))
        migration=(ROOT.parent/'migrations/023_royalty_settlement.sql').read_text().replace('BEGIN;','',1).replace('COMMIT;','',1)
        for _ in range(2): self.rpc(migration,execute=True)
        self.approve(statement)
        cur.execute('SELECT * FROM royalty_statements WHERE id=%s::uuid',(statement,))
        self.assertEqual(cur.fetchone(),frozen)
        cur.execute('SELECT minimum_payout,reserve_held FROM royalty_account_settings WHERE work_id=%s::uuid',(self.work,))
        settings=cur.fetchone()
        self.assertEqual((Decimal(str(settings['minimum_payout'])),Decimal(str(settings['reserve_held']))),(Decimal('100'),Decimal('20')))
        with self.assertRaisesRegex(engine.StatementValidationError,'Final statement already exists'):
            self.generate()

    def test_later_approval_cannot_silently_rewrite_accrual_history(self):
        self.post_income(publisher_receipts='60')
        first=self.generate()
        later=self.settlement_period('2026-H2','2026-07-01','2026-12-31','40')
        later_draft=self.settlement_draft(later)
        self.approve(first['statement_id'])
        result=self.client.post(f"/api/royalty/statements-engine/{later_draft['statement_id']}/approve",headers={'X-Tenant':'marble-press'})
        self.assertEqual(result.status_code,409,result.text)
        rebuilt=self.settlement_draft(later)
        self.assertEqual(rebuilt['header']['payable_this_period'],'50.00')
        self.approve(rebuilt['statement_id'])
        with self.assertRaisesRegex(engine.StatementValidationError,'Generate a draft'):
            engine.generate_statement(self.cursor(),tenant_id=self.tenant,work_id=self.work2,royalty_set_id=self.set2,
                party='author',period_id=self.period,status='final')

    def manual_payment(self, statement, amount, reference):
        return self.client.post('/api/royalty/statements-engine/preparation/payments',headers={'X-Tenant':'marble-press'},json={
            'request_id':str(uuid4()),'statement_id':statement,'amount':amount,'payment_date':'2026-01-15',
            'reference_number':reference,'payment_method':'check','payee_role':'contributor'})

    def test_actual_payments_since_2025_h2_reduce_new_statement_prior_unpaid_without_double_counting(self):
        prior_period=self.settlement_period('2025-H2','2025-07-01','2025-12-31','200')
        prior=self.settlement_draft(prior_period)
        self.approve(prior['statement_id'])
        self.post_income(publisher_receipts='120')
        current=self.generate()
        policy=current['header']['settlement']
        self.assertEqual((policy['prior_unpaid_payable'],policy['actual_payable'],policy['total_payment_due']),('100.00','60.00','160.00'))
        result=self.manual_payment(prior['statement_id'],'40','partial-prior')
        self.assertEqual(result.status_code,200,result.text)
        stale=self.client.post(f"/api/royalty/statements-engine/{current['statement_id']}/approve",headers={'X-Tenant':'marble-press'})
        self.assertEqual(stale.status_code,409,stale.text)
        current=self.generate()
        policy=current['header']['settlement']
        balance=policy['prior_payment_balances'][0]
        self.assertEqual((balance['period_code'],balance['paid_amount'],balance['outstanding_amount'],balance['payment_status']),
            ('2025-H2','40.00','60.00','Partially paid'))
        self.assertEqual(policy['total_payment_due'],'120.00')
        self.approve(current['statement_id'])
        result=self.manual_payment(current['statement_id'],'60','paid-current')
        self.assertEqual(result.status_code,200,result.text)
        next_period=self.settlement_period('2026-H2','2026-07-01','2026-12-31','80')
        next_draft=self.settlement_draft(next_period)
        policy=next_draft['header']['settlement']
        self.assertEqual((policy['actual_payable'],policy['accrued_carried_forward'],policy['prior_unpaid_payable'],policy['total_payment_due']),
            ('0.00','40.00','60.00','60.00'))
        self.assertEqual(policy['prior_payment_balances'][1]['payment_status'],'Paid')
        self.approve(next_draft['statement_id'])
        self.assertEqual(self.manual_payment(prior['statement_id'],'60','paid-prior').status_code,200)
        future=self.settlement_period('2027-H1','2027-01-01','2027-06-30','20')
        policy=self.settlement_draft(future)['header']['settlement']
        self.assertEqual((policy['prior_unpaid_payable'],policy['actual_payable'],policy['total_payment_due']),('0.00','50.00','50.00'))
        cur=self.cursor()
        cur.execute('SELECT payable_this_period FROM royalty_statements WHERE id=%s::uuid',(prior['statement_id'],))
        self.assertEqual(Decimal(str(cur.fetchone()['payable_this_period'])),Decimal('100'))

    def test_payment_balance_start_date_tenant_party_and_current_period_selection(self):
        from services.royalty_settlement import payment_balances
        old=self.settlement_period('2025-H1','2025-01-01','2025-06-30','200')
        self.approve(self.settlement_draft(old)['statement_id'])
        start=self.settlement_period('2025-H2','2025-07-01','2025-12-31','200')
        first=self.settlement_draft(start); self.approve(first['statement_id'])
        self.post_income(publisher_receipts='143.74')
        second=self.generate(); self.approve(second['statement_id'])
        balances=payment_balances(self.cursor(),self.tenant,self.work,'author','2026-06-30',include_current=True)
        self.assertEqual([row['period_code'] for row in balances],['2025-H2','2026-H1'])
        self.assertEqual(payment_balances(self.cursor(),self.other,self.work,'author','2026-06-30',True),[])
        self.assertEqual(payment_balances(self.cursor(),self.tenant,self.work,'illustrator','2026-06-30',True),[])
        self.assertEqual(payment_balances(self.cursor(),self.tenant,self.work2,'author','2026-06-30',True),[])
        result=self.client.get(f'/api/royalty/statements-engine/preparation?work_id={self.work}&royalty_set_id={self.set_id}&period_id={self.period}',headers={'X-Tenant':'marble-press'})
        # Preparation only exposes parties with first-rights rules.
        cur=self.cursor()
        cur.execute("""INSERT INTO royalty_rules(id,tenant_id,royalty_set_id,party,rights_type,format_label,base,mode,percent)
            VALUES (%s::uuid,%s::uuid,%s::uuid,'author','first_rights','Hardcover','net_receipts','fixed',10)""",(str(uuid4()),self.tenant,self.set_id))
        result=self.client.get(f'/api/royalty/statements-engine/preparation?work_id={self.work}&royalty_set_id={self.set_id}&period_id={self.period}',headers={'X-Tenant':'marble-press'})
        self.assertEqual(result.status_code,200,result.text)
        account=result.json()['accounts'][0]
        self.assertEqual([row['statement_id'] for row in account['statement_payment_balances']],[first['statement_id'],second['statement_id']])
        self.assertEqual(account['total_unpaid_payable'],'171.87')

    def test_manual_fractional_payment_retry_and_migration_preserve_payment_records(self):
        self.post_income()
        statement=self.generate()['statement_id']; self.approve(statement)
        body={'request_id':str(uuid4()),'statement_id':statement,'amount':'25.88','payment_date':'2026-01-15',
            'reference_number':'actual-check-123','payment_method':'check','payee_role':'contributor'}
        for _ in range(2):
            result=self.client.post('/api/royalty/statements-engine/preparation/payments',headers={'X-Tenant':'marble-press'},json=body)
            self.assertEqual(result.status_code,200,result.text)
            self.assertEqual(result.json()['unpaid_balance'],'45.99')
        cur=self.cursor()
        cur.execute('SELECT * FROM royalty_payments ORDER BY id')
        records=cur.fetchall(); self.assertEqual(len(records),1)
        migration=(ROOT.parent/'migrations/024_royalty_payment_tracking.sql').read_text().replace('BEGIN;','',1).replace('COMMIT;','',1)
        for _ in range(2): self.rpc(migration,execute=True)
        cur.execute('SELECT * FROM royalty_payments ORDER BY id')
        self.assertEqual(cur.fetchall(),records)
        from services.royalty_settlement import payment_balances
        balance=payment_balances(cur,self.tenant,self.work,'author','2026-06-30',True)[0]
        self.assertEqual((balance['paid_amount'],balance['outstanding_amount']),('25.88','45.99'))
        rejected=self.manual_payment(statement,'46','too-much')
        self.assertEqual(rejected.status_code,422,rejected.text)


if __name__ == '__main__':unittest.main()
