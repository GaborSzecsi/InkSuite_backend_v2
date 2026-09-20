// In-memory engine for actual Marketplace endpoint SQL tests; no live DB access.
const {PGlite}=require('@electric-sql/pglite');
const fs=require('fs'),path=require('path'),readline=require('readline');
(async()=>{
 const db=new PGlite();
 const base=__dirname;
 const snapshot=JSON.parse(fs.readFileSync(path.join(base,'marketplace-schema-inspection.json'),'utf8'));
 const publicBook=JSON.parse(fs.readFileSync(path.join(base,'marketplace-public-book-schema.json'),'utf8'));
 snapshot.columns.push(...publicBook.columns); snapshot.constraints.push(...publicBook.constraints);
 const tables=[...new Set(snapshot.columns.map(c=>c.table_name))];
 for(const table of tables){
   const cols=snapshot.columns.filter(c=>c.table_name===table).map(c=>`"${c.column_name}" ${c.data_type==='ARRAY'?c.udt_name.slice(1)+'[]':c.data_type}${c.is_nullable==='NO'?' NOT NULL':''}${c.column_default?' DEFAULT '+c.column_default:''}`);
   await db.exec(`CREATE TABLE public.${table} (${cols.join(',')})`);
 }
 for(const type of ['p','u','c','f']) for(const c of snapshot.constraints.filter(c=>tables.includes(c.table_name)&&c.contype===type)){
   const ref=c.definition.match(/REFERENCES (\w+)/)?.[1];
   if(ref&&!tables.includes(ref))continue;
   await db.exec(`ALTER TABLE public.${c.table_name} ADD CONSTRAINT "${c.conname}" ${c.definition}`);
 }
 await db.exec(fs.readFileSync(path.resolve(__dirname,'../../migrations/007_marketplace.sql'),'utf8'));
 console.log(JSON.stringify({ready:true}));
 for await(const line of readline.createInterface({input:process.stdin,crlfDelay:Infinity})){
   try {const cmd=JSON.parse(line);const r=cmd.exec?await db.exec(cmd.sql):await db.query(cmd.sql,cmd.params||[]);console.log(JSON.stringify({result:r}));}
   catch(e){console.log(JSON.stringify({error:e.message,code:e.code}));}
 }
 await db.close();
})().catch(e=>{console.log(JSON.stringify({error:e.message}));process.exit(1);});
