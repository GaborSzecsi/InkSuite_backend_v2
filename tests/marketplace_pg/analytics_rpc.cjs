// Isolated PostgreSQL/WASM. Never connects to the production database.
const {PGlite}=require('@electric-sql/pglite');
const fs=require('fs'),path=require('path'),readline=require('readline');
(async()=>{
 const db=new PGlite();
 await db.exec('CREATE TABLE tenants(id uuid PRIMARY KEY); CREATE TABLE users(id uuid PRIMARY KEY); CREATE TABLE works(id uuid PRIMARY KEY,tenant_id uuid);');
 for(const file of ['006_marketing.sql','040_marketing_asset_library.sql','042_social_analytics.sql'])await db.exec(fs.readFileSync(path.resolve(__dirname,'../../migrations',file),'utf8'));
 console.log(JSON.stringify({ready:true}));
 for await(const line of readline.createInterface({input:process.stdin,crlfDelay:Infinity})){
  try{const cmd=JSON.parse(line);console.log(JSON.stringify({result:await db.query(cmd.sql,cmd.params||[])}));}
  catch(e){console.log(JSON.stringify({error:e.message,code:e.code}));}
 }
 await db.close();
})().catch(e=>{console.log(JSON.stringify({error:e.message}));process.exit(1);});
