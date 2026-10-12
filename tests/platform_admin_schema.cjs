// Isolated PostgreSQL checks. No application environment or AWS connection is used.
const {PGlite}=require('./marketplace_pg/node_modules/@electric-sql/pglite');
const fs=require('node:fs/promises');
const path=require('node:path');
const assert=require('node:assert/strict');
const {randomUUID}=require('node:crypto');
async function verify(){
 const db=new PGlite();let checks=0;
 try{
  await db.exec(`CREATE TABLE tenants(id uuid PRIMARY KEY DEFAULT gen_random_uuid(),name text,slug text UNIQUE,created_at timestamptz DEFAULT now());
   CREATE TABLE users(id uuid PRIMARY KEY,email text,full_name text);
   CREATE TABLE memberships(tenant_id uuid REFERENCES tenants(id),user_id uuid REFERENCES users(id),role text,module_permissions jsonb DEFAULT '{}',PRIMARY KEY(tenant_id,user_id));
   CREATE TABLE test_books(id uuid PRIMARY KEY DEFAULT gen_random_uuid(),tenant_id uuid REFERENCES tenants(id) ON DELETE CASCADE);
   CREATE TABLE test_legacy(id uuid PRIMARY KEY DEFAULT gen_random_uuid(),publisher_id uuid REFERENCES tenants(id) ON DELETE CASCADE);`);
  const migration=await fs.readFile(path.join(__dirname,'../migrations/031_platform_admin.sql'),'utf8');
  await db.exec(migration);await db.exec(migration);checks++;
  const admin=randomUUID(),other=randomUUID(),tenant=randomUUID(),second=randomUUID();
  await db.query("INSERT INTO users VALUES($1,'admin@example.test','Admin'),($2,'other@example.test','Other')",[admin,other]);
  await db.query("INSERT INTO tenants(id,name,slug) VALUES($1,'Demo','demo'),($2,'Other','other')",[tenant,second]);
  await assert.rejects(db.query("INSERT INTO tenants(name,slug) VALUES('Duplicate','demo')"));checks++;
  await db.query("INSERT INTO platform_support_sessions(actor_id,tenant_id,tenant_slug,reason) VALUES($1,$2,'demo','Investigate test account')",[admin,tenant]);
  const active=await db.query('SELECT * FROM platform_support_sessions WHERE actor_id=$1 AND ended_at IS NULL AND expires_at>now()',[admin]);
  assert.equal(active.rows.length,1);assert.equal(new Date(active.rows[0].expires_at)-new Date(active.rows[0].started_at),1800000);checks++;
  assert.equal((await db.query('SELECT * FROM platform_support_sessions WHERE id=$1 AND actor_id=$2',[active.rows[0].id,other])).rows.length,0);checks++;
  await db.query("UPDATE platform_support_sessions SET expires_at=now()-interval '1 second' WHERE actor_id=$1",[admin]);
  assert.equal((await db.query('SELECT * FROM platform_support_sessions WHERE actor_id=$1 AND ended_at IS NULL AND expires_at>now()',[admin])).rows.length,0);checks++;
  await assert.rejects(db.query("INSERT INTO platform_support_sessions(actor_id,tenant_id,tenant_slug,reason) VALUES($1,$2,'demo','short')",[admin,tenant]));checks++;
  await db.query("INSERT INTO platform_activity(actor_id,tenant_id,tenant_slug,action,module) VALUES($1,$2,'demo','PAGE_VIEW','books'),($1,$2,'demo','PAGE_VIEW','contracts'),($3,$4,'other','PAGE_VIEW','books'),($1,NULL,NULL,'TENANT_CREATED','platform')",[admin,tenant,other,second]);
  const usage=(await db.query("SELECT count(*) FILTER(WHERE action='PAGE_VIEW') AS views,count(DISTINCT actor_id) FILTER(WHERE action='PAGE_VIEW') AS users FROM platform_activity")).rows[0];
  assert.equal(Number(usage.views),3);assert.equal(Number(usage.users),2);checks++;
  const scoped=(await db.query("SELECT count(*) AS views FROM platform_activity WHERE tenant_id=$1 AND action='PAGE_VIEW'",[tenant])).rows[0];assert.equal(Number(scoped.views),2);checks++;
  const source=await fs.readFile(path.join(__dirname,'../app/admin/platform.py'),'utf8');
  const inventorySql=source.match(/cur\.execute\("""(SELECT c\.table_name[\s\S]*?)"""\)/)[1];
  const columns=(await db.query(inventorySql)).rows;
  assert(columns.some(r=>r.table_name==='test_books'&&r.column_name==='tenant_id'));
  assert(columns.some(r=>r.table_name==='test_legacy'&&r.column_name==='publisher_id'));
  assert(!columns.some(r=>['memberships','platform_activity','platform_support_sessions'].includes(r.table_name)));checks++;
  await db.query('INSERT INTO test_legacy(publisher_id) VALUES($1)',[tenant]);
  assert.equal(Number((await db.query('SELECT count(*) AS n FROM test_legacy WHERE publisher_id::text=$1',[tenant])).rows[0].n),1);checks++;
  await db.exec('BEGIN');
  const empty=randomUUID();await db.query("INSERT INTO tenants(id,name,slug) VALUES($1,'Empty','empty')",[empty]);
  await db.query("INSERT INTO memberships(tenant_id,user_id,role) VALUES($1,$2,'tenant_admin')",[empty,admin]);
  await db.query("INSERT INTO platform_activity(actor_id,tenant_id,tenant_slug,action,module) VALUES($1,$2,'empty','TENANT_DELETED','platform')",[admin,empty]);
  await db.query('DELETE FROM memberships WHERE tenant_id=$1',[empty]);await db.query('DELETE FROM tenants WHERE id=$1',[empty]);await db.exec('COMMIT');
  assert.equal((await db.query('SELECT * FROM platform_activity WHERE tenant_id=$1',[empty])).rows.length,1);
  assert.equal((await db.query('SELECT * FROM users WHERE id=$1',[admin])).rows.length,1);checks++;
  return {passed:checks,database:'disposable PGlite',migration:'031_platform_admin.sql'};
 }finally{await db.close();}
}
module.exports={verify};
if(require.main===module)verify().then(console.log).catch(e=>{console.error(e);process.exitCode=1;});
