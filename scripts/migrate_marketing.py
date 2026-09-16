"""Explicit, additive migration. Default is read-only preflight; pass --apply to run."""
import argparse
from pathlib import Path
import sys
from dotenv import load_dotenv

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
load_dotenv(ROOT/'.env')
from app.core.db import db_conn

TABLES=('marketing_campaigns','marketing_campaign_works','social_accounts','social_posts','social_post_works','social_post_targets','social_media_assets','social_post_assets','social_publish_jobs','marketing_oauth_states','marketing_audit')

def main():
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--apply',action='store_true');args=parser.parse_args()
    with db_conn() as conn,conn.cursor() as cur:
        for table in ('tenants','users','works'):
            cur.execute('SELECT data_type FROM information_schema.columns WHERE table_schema=current_schema() AND table_name=%s AND column_name=\'id\'',(table,))
            if cur.fetchone()!=('uuid',): raise RuntimeError(f'{table}.id must be an existing UUID key.')
        cur.execute("SELECT data_type FROM information_schema.columns WHERE table_schema=current_schema() AND table_name='works' AND column_name='tenant_id'")
        if cur.fetchone()!=('uuid',): raise RuntimeError('works.tenant_id must be an existing UUID key.')
        cur.execute('SELECT table_name FROM information_schema.tables WHERE table_schema=current_schema() AND table_name=ANY(%s)',(list(TABLES),))
        print(f'Preflight passed. {len(cur.fetchall())}/{len(TABLES)} Marketing tables already exist.')
        if not args.apply:
            print('Read-only check complete. No migration applied.');return
        cur.execute("SET lock_timeout='5s'; SET statement_timeout='60s'")
        cur.execute("SELECT pg_advisory_lock(hashtext('inksuite:006-marketing'))")
        try:
            cur.execute((ROOT/'migrations'/'006_marketing.sql').read_text(encoding='utf-8'))
        finally:
            if conn.info.transaction_status.name=='INERROR': conn.rollback()
            cur.execute("SELECT pg_advisory_unlock(hashtext('inksuite:006-marketing'))")
        cur.execute('SELECT count(*) FROM information_schema.tables WHERE table_schema=current_schema() AND table_name=ANY(%s)',(list(TABLES),))
        assert cur.fetchone()[0]==len(TABLES)
        print('Marketing migration applied. All 11 tables verified. No worker started.')
if __name__=='__main__': main()
