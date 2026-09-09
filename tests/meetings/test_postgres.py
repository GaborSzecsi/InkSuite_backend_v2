"""Real PostgreSQL race/migration check. Set MEETINGS_TEST_DATABASE_URL to a test DB."""
import os,uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from threading import Barrier
from datetime import datetime,timedelta,timezone
import pytest
import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb
from app.meetings import service as s
from app.meetings.schemas import Guest

@pytest.mark.skipif(not os.getenv('MEETINGS_TEST_DATABASE_URL'),reason='Dedicated PostgreSQL test database not configured')
def test_simultaneous_booking_and_migration(monkeypatch):
    dsn=os.environ['MEETINGS_TEST_DATABASE_URL'];schema='meetings_test_'+uuid.uuid4().hex
    tenant,user,type_id=[str(uuid.uuid4()) for _ in range(3)]
    with psycopg.connect(dsn,autocommit=True) as setup:
        setup.execute(psycopg.sql.SQL('CREATE SCHEMA {}').format(psycopg.sql.Identifier(schema)))
        try:
            setup.execute(psycopg.sql.SQL('SET search_path TO {}').format(psycopg.sql.Identifier(schema)))
            setup.execute('CREATE TABLE tenants(id uuid PRIMARY KEY); CREATE TABLE users(id uuid PRIMARY KEY)')
            setup.execute((Path(__file__).parents[2]/'migrations/005_meetings.sql').read_text(encoding='utf-8-sig'))
            setup.execute('INSERT INTO tenants VALUES(%s)',(tenant,));setup.execute('INSERT INTO users VALUES(%s)',(user,))
            setup.execute('INSERT INTO meeting_profiles(tenant_id,user_id,company_email) VALUES(%s,%s,%s)',(tenant,user,'host@example.com'))
            day=(datetime.now(timezone.utc)+timedelta(days=2)).date();start=datetime.combine(day,datetime.min.time(),timezone.utc)+timedelta(hours=10)
            cfg={'title':'Test','timezone':'UTC','duration':30,'weekly':{str(day.weekday()):[['09:00','17:00']]},'exceptions':{},'notice':0,'horizon':30,'buffer_before':0,'buffer_after':0,'destination':{},'questions':[]}
            setup.execute('INSERT INTO meeting_types(id,tenant_id,user_id,slug,config) VALUES(%s,%s,%s,%s,%s)',(type_id,tenant,user,'test-'+uuid.uuid4().hex,Jsonb(cfg)))
            monkeypatch.setattr(s,'sender',lambda *a:({'company_email':'host@example.com'},{}));monkeypatch.setattr(s,'secret_write',lambda *a:'test-secret')
            barrier=Barrier(2)
            def attempt(n):
                with psycopg.connect(dsn,autocommit=True,row_factory=dict_row) as conn:
                    conn.execute(psycopg.sql.SQL('SET search_path TO {}').format(psycopg.sql.Identifier(schema)))
                    with conn.transaction(),conn.cursor() as cur:
                        t=s.one(cur,'SELECT * FROM meeting_types WHERE id=%s',(type_id,));barrier.wait()
                        try:return s.book(cur,t,Guest(name='Guest',email='guest@example.com',start=start.isoformat(),idempotency_key='request-'+str(n)+'-'+'x'*20))['status']
                        except Exception as e:return getattr(e,'status_code',str(e))
            with ThreadPoolExecutor(2) as pool:results=list(pool.map(attempt,[1,2]))
            assert sorted(map(str,results))==['409','confirmed']
            assert setup.execute('SELECT count(*) FROM meeting_bookings').fetchone()[0]==1
            assert setup.execute('SELECT count(*) FROM meeting_jobs').fetchone()[0]==11
        finally:
            setup.execute(psycopg.sql.SQL('DROP SCHEMA {} CASCADE').format(psycopg.sql.Identifier(schema)))
