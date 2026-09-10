"""Apply the calendar tables explicitly. Does not run at application startup."""
from pathlib import Path


def main():
    from dotenv import load_dotenv
    root = Path(__file__).resolve().parents[1]
    load_dotenv(root / '.env', override=False)
    from app.core.db import db_conn
    with db_conn() as conn:
        conn.execute((root / 'migrations/006_meeting_calendar.sql').read_text(encoding='utf-8'))
    print('Calendar migration applied.')


if __name__ == '__main__':
    main()
