const {PGlite}=require('./marketplace_pg/node_modules/@electric-sql/pglite');
const fs=require('fs'),path=require('path'),readline=require('readline');
(async()=>{
 const db=new PGlite();
 await db.exec(`CREATE TABLE tenants(id uuid PRIMARY KEY); CREATE TABLE users(id uuid PRIMARY KEY);
 CREATE TABLE works(id uuid PRIMARY KEY,tenant_id uuid REFERENCES tenants(id),title text);
 CREATE TABLE editions(id uuid PRIMARY KEY,tenant_id uuid REFERENCES tenants(id),work_id uuid REFERENCES works(id),isbn13 text,product_form text,onix_product_form text);`);
 await db.exec(fs.readFileSync(path.join(__dirname,'../migrations/020_distribution.sql'),'utf8').replace(/^\uFEFF/,''));
 await db.exec(fs.readFileSync(path.join(__dirname,'../migrations/021_distribution_privacy.sql'),'utf8'));
 console.log(JSON.stringify({ready:true}));
 for await(const line of readline.createInterface({input:process.stdin,crlfDelay:Infinity})){
  try{const c=JSON.parse(line);const result=c.exec?await db.exec(c.sql):await db.query(c.sql,c.params||[]);console.log(JSON.stringify({result}));}
  catch(e){console.log(JSON.stringify({error:e.message,code:e.code}));}
 }
 await db.close();
})().catch(e=>{console.log(JSON.stringify({error:e.message}));process.exit(1);});
