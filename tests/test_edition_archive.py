import json,subprocess,unittest
from pathlib import Path
from uuid import uuid4
from psycopg.types.json import Jsonb
from fastapi import HTTPException
from app.catalog_archive import change,require_active
class Cursor:
 def __init__(self,p):self.p=p
 def execute(self,sql,args=()):
  for i in range(len(args)):sql=sql.replace('%s',f'${i+1}',1)
  self.p.stdin.write(json.dumps({'sql':sql,'params':[json.dumps(x.obj) if isinstance(x,Jsonb) else x for x in args]},default=str)+'\n');self.p.stdin.flush()
  data=json.loads(self.p.stdout.readline())
  if 'error' in data:raise RuntimeError(str(data))
  self.rows=data['result']['rows']
 def fetchone(self):return self.rows.pop(0) if self.rows else None
 def fetchall(self):return self.rows
class ArchiveTests(unittest.TestCase):
 @classmethod
 def setUpClass(cls):
  cls.p=subprocess.Popen(['node',str(Path(__file__).parent/'marketplace_pg/archive_rpc.cjs')],stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True,encoding='utf8');assert json.loads(cls.p.stdout.readline())=={'ready':True};cls.c=Cursor(cls.p)
 @classmethod
 def tearDownClass(cls):cls.p.stdin.close();cls.p.wait(timeout=20)
 def setUp(self):
  self.tenant,self.other,self.work,self.edition,self.regular,self.actor=map(str,[uuid4() for _ in range(6)])
  for eid in (self.edition,self.regular):self.c.execute('INSERT INTO editions(id,tenant_id,work_id,isbn13) VALUES(%s,%s,%s,%s)',(eid,self.tenant,self.work,eid))
  self.c.execute("INSERT INTO history VALUES(%s,'sale')",(self.edition,))
 def action(self,archive,tenant=None):return change(self.c,tenant or self.tenant,self.work,self.edition,self.actor,archive=archive,reason='Discontinued')
 def test_archive_restore_and_history(self):
  self.assertTrue(self.action(True)['changed']);self.assertFalse(self.action(True)['changed'])
  with self.assertRaises(HTTPException) as e:require_active(self.c,self.tenant,self.edition,self.work)
  self.assertEqual(e.exception.status_code,409)
  require_active(self.c,self.tenant,self.regular,self.work)
  self.assertTrue(self.action(False)['changed']);require_active(self.c,self.tenant,self.edition,self.work)
  self.c.execute('SELECT status,archived_at,archive_history FROM editions WHERE id=%s',(self.edition,));r=self.c.fetchone();self.assertEqual(r['status'],'active');self.assertIsNone(r['archived_at']);self.assertEqual([x['action'] for x in r['archive_history']],['archived','restored'])
  self.c.execute('SELECT count(*) AS n FROM history WHERE edition_id=%s',(self.edition,));self.assertEqual(self.c.fetchone()['n'],1)
 def test_wrong_tenant_cannot_archive_or_restore(self):
  for archive in (True,False):
   with self.assertRaises(HTTPException) as e:self.action(archive,self.other)
   self.assertEqual(e.exception.status_code,404)
  require_active(self.c,self.tenant,self.edition,self.work)
 def test_bad_ids_rejected(self):
  with self.assertRaises(HTTPException) as e:require_active(self.c,self.tenant,'bad',self.work)
  self.assertEqual(e.exception.status_code,422)
if __name__=='__main__':unittest.main()
