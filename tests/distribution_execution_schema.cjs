// Disposable PostgreSQL checks. No application credentials or live database connection.
const {PGlite}=require('./marketplace_pg/node_modules/@electric-sql/pglite');
const fs=require('node:fs/promises');
const path=require('node:path');
const assert=require('node:assert/strict');
const {randomUUID}=require('node:crypto');
async function verify(){
 const db=new PGlite();let checks=0;
 try{
  await db.exec(`CREATE TABLE tenants(id uuid PRIMARY KEY); CREATE TABLE users(id uuid PRIMARY KEY);
   CREATE TABLE memberships(tenant_id uuid REFERENCES tenants(id),user_id uuid REFERENCES users(id),role text,PRIMARY KEY(tenant_id,user_id));
   CREATE TABLE works(id uuid PRIMARY KEY,tenant_id uuid REFERENCES tenants(id),title text);
   CREATE TABLE editions(id uuid PRIMARY KEY,tenant_id uuid REFERENCES tenants(id),work_id uuid REFERENCES works(id),isbn13 text,product_form text,onix_product_form text);`);
  for(const migration of ['020_distribution.sql','021_distribution_privacy.sql','027_shopify_embedded.sql','028_shopify_variant_matching.sql','029_shopify_catalog_publishing.sql']){
   await db.exec((await fs.readFile(path.join(__dirname,'../migrations',migration),'utf8')).replace(/^\uFEFF/,''));
  }
  const tenant=randomUUID(),other=randomUUID(),connection=randomUUID(),work=randomUUID(),edition=randomUUID(),order=randomUUID();
  await db.query('INSERT INTO tenants VALUES($1),($2)',[tenant,other]);
  await db.query('INSERT INTO works VALUES($1,$2,$3)',[work,tenant,'Test title']);
  await db.query("INSERT INTO editions VALUES($1,$2,$3,'9780306406157','BB','BB')",[edition,tenant,work]);
  await db.query("INSERT INTO distribution_connections(id,tenant_id,display_name,adapter) VALUES($1,$2,'Existing connection','hachette_exporteo')",[connection,tenant]);
  await db.exec(await fs.readFile(path.join(__dirname,'../migrations/030_distribution_execution.sql'),'utf8'));
  const conn=(await db.query('SELECT * FROM distribution_connections WHERE id=$1',[connection])).rows[0];
  assert.equal(conn.enabled,false);assert.equal(conn.orders_enabled,false);assert.equal(conn.inventory_enabled,false);assert.equal(conn.mode,'preview');checks++;
  await db.query("UPDATE distribution_connections SET enabled=true,adapter='simulator',safety_buffer=2 WHERE id=$1",[connection]);
  await db.query("INSERT INTO distribution_orders(id,tenant_id,connection_id,source,source_account,external_order_id,reference,request_hash,recipient,shipping_method) VALUES($1,$2,$3,'SHOPIFY','test.myshopify.com','1','Shopify 1','hash','{}','Ground')",[order,tenant,connection]);
  await db.query("INSERT INTO distribution_order_items VALUES($1,$2,$3,'9780306406157',3)",[tenant,order,edition]);
  await db.query("INSERT INTO distribution_inventory(tenant_id,connection_id,edition_id,distributor_quantity,sync_status,last_synced_at) VALUES($1,$2,$3,10,'OK',now())",[tenant,connection,edition]);
  await db.query("INSERT INTO distribution_reservations(tenant_id,connection_id,edition_id,order_id,quantity,status) VALUES($1,$2,$3,$4,3,'RESERVED')",[tenant,connection,edition,order]);
  const quantity=async()=>Number((await db.query('SELECT sellable_quantity FROM distribution_availability WHERE tenant_id=$1',[tenant])).rows[0].sellable_quantity);
  assert.equal(await quantity(),5);checks++;
  await db.query("UPDATE distribution_reservations SET status='FULFILLED' WHERE order_id=$1",[order]);
  assert.equal(await quantity(),5);checks++;
  await db.exec('BEGIN');
  await db.query('UPDATE distribution_inventory SET distributor_quantity=7 WHERE tenant_id=$1',[tenant]);
  await db.query("UPDATE distribution_reservations SET status='RELEASED' WHERE order_id=$1",[order]);
  await db.exec('COMMIT');assert.equal(await quantity(),5);checks++;
  await db.query("UPDATE distribution_inventory SET last_synced_at=now()-interval '2 days' WHERE tenant_id=$1",[tenant]);
  assert.equal(await quantity(),0);checks++;
  await assert.rejects(db.query("UPDATE distribution_orders SET recipient='{"+'"name":"Forbidden"'+"}' WHERE id=$1",[order]));checks++;
  await assert.rejects(db.query("INSERT INTO distribution_simulated_orders VALUES($1,$2,$3,'[]','ACCEPTED',now())",[other,connection,order]));checks++;
  await db.query("INSERT INTO distribution_simulated_orders(tenant_id,connection_id,order_id,items) VALUES($1,$2,$3,$4)",[tenant,connection,order,[{isbn:'9780306406157',quantity:3}]]);
  await assert.rejects(db.query("INSERT INTO distribution_simulated_orders(tenant_id,connection_id,order_id,items) VALUES($1,$2,$3,'[]')",[tenant,connection,order]));checks++;
  const enqueue="INSERT INTO distribution_jobs(tenant_id,connection_id,kind) VALUES($1,$2,'PUBLISH') ON CONFLICT DO NOTHING";
  await db.query(enqueue,[tenant,connection]);await db.query(enqueue,[tenant,connection]);
  assert.equal(Number((await db.query('SELECT count(*) AS n FROM distribution_jobs WHERE tenant_id=$1',[tenant])).rows[0].n),1);checks++;
  return {passed:checks,database:'disposable PGlite',migration:'030_distribution_execution.sql'};
 }finally{await db.close();}
}
module.exports={verify};
if(require.main===module)verify().then(console.log).catch(e=>{console.error(e);process.exitCode=1;});
