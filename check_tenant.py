from dotenv import load_dotenv
load_dotenv('.env')

import os
import psycopg

dsn = os.environ['DATABASE_URL']
with psycopg.connect(dsn) as conn:
    with conn.cursor() as cur:
        cur.execute(\"select id::text, slug, name from tenants where lower(slug)='marble-press'\")
        print('tenant exists:', cur.fetchone())
