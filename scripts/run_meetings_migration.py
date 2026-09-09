"""Apply Meetings migration explicitly using DATABASE_URL. Never runs at API startup."""
from pathlib import Path
from app.core.db import db_conn

def main():
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).resolve().parents[1]/".env",override=False)
    sql=(Path(__file__).resolve().parents[1]/'migrations/005_meetings.sql').read_text(encoding='utf-8-sig')
    with db_conn() as conn:
        conn.execute(sql)
    print('Meetings migration applied.')

if __name__=='__main__':main()
