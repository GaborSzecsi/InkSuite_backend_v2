const {PGlite}=require('./marketplace_pg/node_modules/@electric-sql/pglite');
const fs=require('fs'),path=require('path'),readline=require('readline');
(async()=>{
 const db=new PGlite();
 await db.exec(`CREATE TABLE tenants(id uuid PRIMARY KEY);
 CREATE TABLE works(id uuid PRIMARY KEY,tenant_id uuid,title text,uid text);
 CREATE TABLE editions(id uuid PRIMARY KEY,tenant_id uuid,work_id uuid,isbn13 text,product_form text,product_form_detail text,UNIQUE(tenant_id,id));
 CREATE TABLE distribution_connections(id uuid PRIMARY KEY,tenant_id uuid,UNIQUE(tenant_id,id));
 CREATE TABLE inventory_movements(id uuid PRIMARY KEY,tenant_id uuid,work_id uuid,edition_id uuid,movement_date date,movement_type text,quantity numeric);
 CREATE TABLE royalty_periods(id uuid PRIMARY KEY DEFAULT gen_random_uuid(),tenant_id uuid,period_code text,period_start date,period_end date,is_closed boolean DEFAULT false);
 CREATE TABLE royalty_statements(id uuid PRIMARY KEY DEFAULT gen_random_uuid(),tenant_id uuid,period_id uuid,status text);
 CREATE TABLE royalty_sets(id uuid PRIMARY KEY,tenant_id uuid,work_id uuid,is_active boolean);
 CREATE TABLE royalty_rules(id uuid PRIMARY KEY,tenant_id uuid,royalty_set_id uuid);
 CREATE TABLE royalty_tiers(id uuid PRIMARY KEY,tenant_id uuid,rule_id uuid);
 CREATE TABLE royalty_tier_conditions(id uuid PRIMARY KEY,tenant_id uuid,tier_id uuid,kind text);
 CREATE TABLE royalty_sales_lines(id uuid PRIMARY KEY DEFAULT gen_random_uuid(),tenant_id uuid,work_id uuid,edition_id uuid,period_id uuid,
 transaction_date date,source_system text,source_file text,reference_number text,customer_name text,territory text,transaction_type text CHECK(transaction_type IN ('sale','return')),
 units_sold numeric DEFAULT 0,units_returned numeric DEFAULT 0,gross_sales numeric(18,2),publisher_receipts numeric(18,2),currency text,discount_percent numeric,sales_price numeric);
 CREATE TABLE fraser_ca_sales_lines(LIKE royalty_sales_lines INCLUDING ALL);`);
 await db.exec(fs.readFileSync(path.join(__dirname,'../migrations/045_stable_ingestion.sql'),'utf8'));
 await db.exec(fs.readFileSync(path.join(__dirname,'../migrations/046_stable_channel_units.sql'),'utf8'));
 await db.exec(fs.readFileSync(path.join(__dirname,'../migrations/047_stable_combined_statements.sql'),'utf8'));
 console.log(JSON.stringify({ready:true}));
 for await(const line of readline.createInterface({input:process.stdin,crlfDelay:Infinity})){
  try{const c=JSON.parse(line);const result=c.exec?await db.exec(c.sql):await db.query(c.sql,c.params||[]);console.log(JSON.stringify({result}));}
  catch(e){console.log(JSON.stringify({error:e.message,code:e.code}));}
 }
 await db.close();
})().catch(e=>{console.log(JSON.stringify({error:e.message}));process.exit(1);});
