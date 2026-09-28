const {PGlite}=require('./marketplace_pg/node_modules/@electric-sql/pglite');
const readline=require('readline');
(async()=>{const db=new PGlite();console.log(JSON.stringify({ready:true}));
for await(const line of readline.createInterface({input:process.stdin,crlfDelay:Infinity})){
 try{const c=JSON.parse(line);const result=c.exec?await db.exec(c.sql):await db.query(c.sql,(c.params||[]).map(x=>x&&x.__bytes?new Uint8Array(x.__bytes):x));console.log(JSON.stringify({result}));}
 catch(e){console.log(JSON.stringify({error:e.message}));}
}await db.close();})();
