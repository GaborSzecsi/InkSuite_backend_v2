const {PGlite}=require('@electric-sql/pglite');
const fs=require('fs'),path=require('path'),readline=require('readline');
(async()=>{
const db=new PGlite();
await db.exec(`CREATE TABLE editions(id uuid PRIMARY KEY,tenant_id uuid,work_id uuid,isbn13 text,status text default 'active',updated_at timestamptz default now()); CREATE TABLE history(edition_id uuid, details text);`);
const migration=fs.readFileSync(path.resolve(__dirname,'../../migrations/043_edition_archive.sql'),'utf8');await db.exec(migration);await db.exec(migration);
console.log(JSON.stringify({ready:true}));
for await(const line of readline.createInterface({input:process.stdin,crlfDelay:Infinity})){
try{const cmd=JSON.parse(line);console.log(JSON.stringify({result:await db.query(cmd.sql,cmd.params||[])}));}catch(e){console.log(JSON.stringify({error:e.message,code:e.code}));}
}await db.close();
})().catch(e=>{console.log(JSON.stringify({error:e.message}));process.exit(1);});
